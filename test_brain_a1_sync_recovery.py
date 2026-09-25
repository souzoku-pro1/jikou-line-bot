"""BRAIN-A1 T-C 同期の回復（v3 §10-3・§5）

固定する仕様:
- 同時刻更新の取りこぼしなし: 次回は「前回の走査上限時点 − 重複窓（10 分）」から
  取り直し、カーソルと同時刻に増えたレコードも拾う（重複は冪等キーで吸収）
- ページ途中失敗: 完了したページまでカーソルが進み、失敗ページで止まる。sync_run に
  incomplete_page が残り、confirmed_until（確認済み範囲）は進まない。次回は続きから
- DB commit 失敗: ページの台帳書込とカーソル前進が同一トランザクションで巻き戻る
- 再起動（新しい run）: 未完了範囲を同期済みにしない（案件の鮮度=incomplete）
- 定期照合: revision 一覧の差分で、窓の外の変更を検出して取り込む
- 走査上限・page_order は sync_run に固定・ページ上限で黙って完了扱いにしない
"""

import datetime
import unittest
from unittest.mock import patch

import sqlalchemy as sa

from brain_test_support import (LINE_A, BrainDbMixin, FakeKintone, FakeQueryError, app28,
                                app30, app40, run)
from hub import brain_ledger as ledger
from hub import brain_sync as sync
from hub.db import session_scope

_UTC = datetime.timezone.utc


class TestSyncRecovery(BrainDbMixin):
    def test_same_timestamp_update_is_not_missed(self):
        t = "2026-09-20T01:00:00Z"
        self.fake.data["40"] = [app40(1, 1, t), app40(2, 1, t)]

        async def body():
            r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual((r["status"], r["records"]), ("ok", 2))
            cur = await ledger.get_cursor("app40")
            self.assertEqual((cur["cursor_updated_at"], cur["cursor_record_id"]), (t, "2"))
            # カーソルと同時刻・番号が前のレコードが後から見えるようになった
            self.fake.data["40"].append(app40(0, 1, t, 顧客名="遅れて見えた"))
            r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(r["status"], "ok")
            self.assertEqual([f["value_text"] for f in await ledger.list_case_facts("40", "0")
                              if f["item_code"] == "app40.顧客名"], ["遅れて見えた"])
            # 発行 query は窓の始点（10 分前）から
            q = [q for a, q in self.fake.calls if a == "40" and "更新日時 >=" in q][-1]
            self.assertIn('更新日時 >= "2026-09-20T00:50:00Z"', q)
            # 重複取得分は冪等キーで吸収（1 と 2 は再挿入されない）
            self.assertEqual(r["inserted"], len(await ledger.list_case_facts("40", "0")))
        run(body())

    def test_page_failure_keeps_cursor_at_completed_page(self):
        t1, t2, t3 = ("2026-09-20T01:00:00Z", "2026-09-20T02:00:00Z", "2026-09-20T03:00:00Z")
        self.fake.data["40"] = [app40(1, 1, t1), app40(2, 1, t2), app40(3, 1, t3)]

        async def body():
            with patch.object(sync, "PAGE_SIZE", 1):
                self.fake.fail_at = {2}            # 2 ページ目の取得で失敗
                r = await sync.sync_target(sync.TARGET_APP40)
                self.assertEqual((r["status"], r["failure"]), ("failed", "kintone_fetch_failed"))
                cur = await ledger.get_cursor("app40")
                self.assertEqual((cur["cursor_updated_at"], cur["cursor_record_id"], cur["state"]),
                                 (t1, "1", "error"))
                self.assertEqual(cur["confirmed_until"], "")        # 確認済み範囲は進まない
                ov = await ledger.sync_overview()
                failed = [x for x in ov["runs"] if x["status"] == "failed"][0]
                self.assertEqual(failed["failure"], "kintone_fetch_failed")
                self.assertEqual(failed["incomplete_page"],
                                 {"after_updated_at": t1, "after_record_id": "1"})
                self.assertEqual(failed["pages_done"], 1)
                self.assertEqual(await ledger.case_freshness("40", "1", "app40"), "incomplete")
                self.assertEqual(await ledger.list_case_facts("40", "2"), [])
                # 続きから（窓は 1 の時刻 − 10 分）: 2・3 が取り込まれ、確認済み範囲が付く
                self.fake.fail_at = set()
                r = await sync.sync_target(sync.TARGET_APP40)
                self.assertEqual((r["status"], r["pages"]), ("ok", 3))   # 1 は重複窓で再取得
                cur = await ledger.get_cursor("app40")
                self.assertEqual((cur["cursor_record_id"], cur["state"]), ("3", "synced"))
                self.assertTrue(cur["confirmed_until"])
                self.assertEqual(await ledger.case_freshness("40", "3", "app40"), "synced")
                run_row = [x for x in (await ledger.sync_overview())["runs"]
                           if x["status"] == "ok"][0]
                self.assertEqual(run_row["pages_done"], 3)
        run(body())

    def test_db_commit_failure_rolls_back_page_and_cursor(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, "2026-09-20T02:00:00Z")]

        async def body():
            with patch.object(sync, "PAGE_SIZE", 1):
                real = ledger.advance_cursor_with_page
                calls = []

                async def flaky(*a, **kw):
                    calls.append(1)
                    if len(calls) == 2:
                        raise RuntimeError("commit failed")
                    return await real(*a, **kw)
                with patch.object(ledger, "advance_cursor_with_page", flaky):
                    r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual((r["status"], r["failure"]), ("failed", "db_write_failed"))
            cur = await ledger.get_cursor("app40")
            self.assertEqual((cur["cursor_record_id"], cur["state"]), ("1", "error"))
            self.assertEqual(await ledger.list_case_facts("40", "2"), [])
            self.assertTrue(await ledger.list_case_facts("40", "1"))
            # 実トランザクション内で失敗しても巻き戻る（fact もカーソルも残らない）
            src = ledger.SourceRef("40", "9", 1, "app40_record", "1")
            bad = ledger.FactIn("case", "app40.status", "choice", "x",
                                ledger.make_locator("status"), "high")
            good_then_bad = [(src, ("40", "9"), [bad], []),
                             (ledger.SourceRef("40", "9", 1, "app40_record", "1"),
                              ("40", "9"), [ledger.FactIn("case", "app40.status", "choice",
                                                          "y", ledger.make_locator("status"))], [])]
            with self.assertRaises(ledger.MismatchError):
                await ledger.advance_cursor_with_page(
                    "app40", await ledger.start_run("app40", "2026-09-22T00:00:00Z"),
                    ingests=good_then_bad, cursor_updated_at="2026-09-22T00:00:00Z",
                    cursor_record_id="9", pages_done=9, records_seen=9)
            self.assertEqual(await ledger.list_case_facts("40", "9"), [])
            self.assertEqual((await ledger.get_cursor("app40"))["cursor_record_id"], "1")
        run(body())

    def test_restart_does_not_mark_incomplete_range_synced(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, "2026-09-20T02:00:00Z")]

        async def body():
            with patch.object(sync, "PAGE_SIZE", 1):
                self.fake.fail_at = {2}
                await sync.sync_target(sync.TARGET_APP40)
            # 「再起動」= 新しい run。失敗直後の状態は未完了のまま
            self.assertEqual((await ledger.get_cursor("app40"))["state"], "error")
            self.assertEqual(await ledger.case_freshness("40", "2", "app40"), "incomplete")
            self.assertEqual(await ledger.case_freshness("40", "1", "app40"), "incomplete")
            # ページ上限に達した run は完了扱いにしない
            with patch.object(sync, "MAX_PAGES_PER_RUN", 1), patch.object(sync, "PAGE_SIZE", 1):
                self.fake.fail_at = set()
                r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual((r["status"], r["failure"]), ("failed", "page_limit_reached"))
            self.assertEqual((await ledger.get_cursor("app40"))["confirmed_until"], "")
            self.fake.fail_at = set()
            r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(r["status"], "ok")
            self.assertEqual(await ledger.case_freshness("40", "2", "app40"), "synced")
        run(body())

    def test_reconcile_detects_revision_change_outside_window(self):
        self.fake.data["40"] = [app40(1, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            # 更新日時が変わらないまま revision だけ進んだ（窓の外の反映）を模擬
            self.fake.data["40"] = [app40(1, 2, status="受理")]
            self.fake.data["40"][0]["更新日時"]["value"] = "2026-09-01T00:00:00Z"
            r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(r["inserted"], 0)                    # 窓では見えない
            rc = await sync.reconcile_target(sync.TARGET_APP40)
            self.assertEqual((rc["status"], rc["changed"]), ("ok", 1))
            st = [f for f in await ledger.list_case_facts("40", "1")
                  if f["item_code"] == "app40.status"]
            self.assertEqual([(f["value_text"], f["version"]) for f in st], [("受理", 2)])
            self.assertTrue((await ledger.get_cursor("app40"))["last_reconcile_at"])
            # 取得失敗は「値なし」に変換しない（fact は残る・状態は failed）
            self.fake.raise_all = True
            rc = await sync.reconcile_target(sync.TARGET_APP40)
            self.assertEqual(rc["status"], "failed")
            self.assertEqual(len(st), len([f for f in await ledger.list_case_facts("40", "1")
                                           if f["item_code"] == "app40.status"]))
        run(body())

    def test_run_records_scan_upper_bound_and_page_order(self):
        self.fake.data["40"] = [app40(1, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            ov = await ledger.sync_overview()
            self.assertEqual(ov["runs"][0]["status"], "ok")
            cur = await ledger.get_cursor("app40")
            self.assertRegex(cur["confirmed_until"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
            q = [q for a, q in self.fake.calls if a == "40"][0]
            self.assertTrue(q.endswith("order by 更新日時 asc, $id asc limit 100"))
        run(body())

    def test_job_runs_periodic_stages_once_per_day(self):
        self.fake.data["40"] = [app40(1, 1)]

        async def body():
            out = await sync.run_brain_sync_job()
            self.assertEqual(out["status"], "ok")
            cur = await ledger.get_cursor("app40")
            self.assertTrue(cur["last_reconcile_at"] and cur["last_recheck_at"])
            n = len(self.fake.calls)
            await sync.run_brain_sync_job()
            # 2 回目は同期 3 対象分の呼出しのみ（照合・再照合は 24h 未満で走らない）
            self.assertEqual(len(self.fake.calls) - n, 3)
        run(body())

    # ── fix1: BA-05（再照合の継続位置）/ BA-09（走査の再開）/ 補足 1 ──

    async def _seed_app30_sources(self, n: int) -> None:
        await ledger.ingest_source(
            ledger.SourceRef("40", "1", 1, *sync.CONVERTER[sync.TARGET_APP40]), ("40", "1"),
            [ledger.FactIn("case", "app40.status", "choice", "受任", ledger.make_locator("status"))])
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["30"] = [app30(i, 1) for i in range(1, n + 1)]
        for i in range(1, n + 1):
            await ledger.ingest_source(
                ledger.SourceRef("30", str(i), 1, *sync.CONVERTER[sync.TARGET_APP30]), ("40", "1"),
                [ledger.FactIn(f"shipping:{i}", "app30.件名", "text", "x", ledger.make_locator("件名"))])

    def test_recheck_cursor_walks_all_sources_in_batches(self):
        # BA-05: 出典 250 件・上限 100 → 3 回で全件に到達。「全件照合済み」は一巡完了後のみ
        async def body():
            await self._seed_app30_sources(250)
            seen = []
            for expect in ((100, False, "100"), (100, False, "200"), (50, True, "250")):
                n = len(self.fake.calls)
                r = await sync.recheck_target(sync.TARGET_APP30, batch=100)
                self.assertEqual((r["status"], r["checked"], r["complete"], r["position"]),
                                 ("ok",) + expect)
                for _a, q in self.fake.calls[n:]:
                    seen += [x.strip('"') for x in q.split("(")[1].split(")")[0].split(",")]
                rc = await ledger.get_cursor("app30", kind="recheck")
                if not expect[1]:
                    self.assertEqual((rc["state"], rc["cursor_record_id"], rc["recheck_completed_at"]),
                                     ("incomplete", expect[2], ""))
                    # 一巡の途中は 24h を待たず次の job で続きを回す
                    self.assertTrue(await sync._recheck_due(
                        "app30", await ledger.get_cursor("app30") or {}, sync._now()))
            self.assertEqual(sorted(int(x) for x in seen), list(range(1, 251)))
            rc = await ledger.get_cursor("app30", kind="recheck")
            self.assertEqual(rc["state"], "synced")
            self.assertTrue(rc["recheck_completed_at"] and rc["recheck_started_at"])
            self.assertFalse(await sync._recheck_due(
                "app30", await ledger.get_cursor("app30") or {}, sync._now()))
            self.assertTrue((await ledger.get_cursor("app30"))["last_recheck_at"])
            ov = await ledger.sync_overview()
            self.assertIn(("app30", "recheck"), {(c["target_app"], c["kind"]) for c in ov["cursors"]})
        run(body())

    def test_recheck_keeps_position_on_failure_and_continues(self):
        # BA-05: 途中失敗で継続位置が保持され、次回はそこから
        async def body():
            await self._seed_app30_sources(250)
            r1 = await sync.recheck_target(sync.TARGET_APP30, batch=100)
            self.assertEqual(r1["position"], "100")
            self.fake.fail_at = {len(self.fake.calls) + 2}       # 2 チャンク目（151〜200）で失敗
            r2 = await sync.recheck_target(sync.TARGET_APP30, batch=100)
            self.assertEqual((r2["status"], r2["checked"], r2["position"]), ("failed", 50, "150"))
            rc = await ledger.get_cursor("app30", kind="recheck")
            self.assertEqual((rc["state"], rc["cursor_record_id"], rc["recheck_completed_at"]),
                             ("error", "150", ""))
            self.fake.fail_at = set()
            r3 = await sync.recheck_target(sync.TARGET_APP30, batch=100)
            self.assertEqual((r3["checked"], r3["complete"], r3["position"]), (100, False, "250"))
            ids = [x.strip('"') for _a, q in self.fake.calls[-2:]
                   for x in q.split("(")[1].split(")")[0].split(",")]
            self.assertEqual((ids[0], ids[-1]), ("151", "250"))
            r4 = await sync.recheck_target(sync.TARGET_APP30, batch=100)
            self.assertEqual((r4["checked"], r4["complete"]), (0, True))
            self.assertEqual((await ledger.get_cursor("app30", kind="recheck"))["state"], "synced")
        run(body())

    def test_app28_is_included_in_reconcile_and_recheck(self):
        # BA-05: App 28 も定期照合・追跡再照合の対象
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1)]

        async def body():
            self.assertEqual((await sync.run_brain_sync_job())["status"], "ok")
            cur = await ledger.get_cursor("app28")
            self.assertTrue(cur["last_reconcile_at"] and cur["last_recheck_at"])
            rc = await ledger.get_cursor("app28", kind="recheck")
            self.assertTrue(rc and rc["recheck_completed_at"])
            self.assertTrue([q for a, q in self.fake.calls if a == "28" and "order by $id asc" in q])
            # 窓の外の revision 変化を照合が拾う（出来事は増えない・R9）
            self.fake.data["28"] = [app28(100, 2, "2026-09-01T00:00:00Z", message="編集後")]
            r = await sync.reconcile_target(sync.TARGET_APP28)
            self.assertEqual((r["status"], r["changed"]), ("ok", 1))
            self.assertEqual(await ledger.latest_known_revision("28", "100"), 2)
            self.assertEqual(len([e for e in await ledger.list_case_events("40", "1")
                                  if e["source_app_id"] == "28"]), 1)
        run(body())

    def test_incomplete_scan_resumes_from_position_with_same_upper_bound(self):
        # BA-09: 同時刻 3 件・上限 2 ページ → 2 回目は再開位置から同じ上限で 3 件目へ到達
        t = "2026-09-20T01:00:00Z"
        self.fake.data["40"] = [app40(1, 1, t), app40(2, 1, t), app40(3, 1, t)]
        now1 = datetime.datetime(2026, 9, 25, 0, 0, 0, tzinfo=_UTC)
        upper1 = "2026-09-25T00:00:00Z"

        async def body():
            with patch.object(sync, "PAGE_SIZE", 1), patch.object(sync, "MAX_PAGES_PER_RUN", 2):
                r1 = await sync.sync_target(sync.TARGET_APP40, now=now1)
                self.assertEqual((r1["status"], r1["failure"]), ("failed", "page_limit_reached"))
                cur = await ledger.get_cursor("app40")
                self.assertEqual((cur["state"], cur["scan_upper_bound"], cur["page_position"],
                                  cur["confirmed_until"]),
                                 ("incomplete", upper1,
                                  {"after_updated_at": t, "after_record_id": "2"}, ""))
                self.assertEqual(await ledger.list_case_facts("40", "3"), [])
                self.assertTrue(all(f'更新日時 <= "{upper1}"' in q for a, q in self.fake.calls))
                # 上限より後に更新されたレコードは再開走査に入らない
                self.fake.data["40"].append(app40(4, 1, "2026-09-25T00:00:30Z"))
                n = len(self.fake.calls)
                r2 = await sync.sync_target(sync.TARGET_APP40,
                                            now=now1 + datetime.timedelta(minutes=1))
                self.assertEqual((r2["status"], r2["resumed"], r2["pages"], r2["scan_upper_bound"]),
                                 ("ok", True, 1, upper1))
                self.assertTrue(await ledger.list_case_facts("40", "3"))
                self.assertEqual(await ledger.list_case_facts("40", "4"), [])
                cur = await ledger.get_cursor("app40")
                self.assertEqual((cur["state"], cur["confirmed_until"], cur["page_position"]),
                                 ("synced", upper1, None))
                q = self.fake.calls[n][1]                     # 再開走査の最初の query
                self.assertIn(f'更新日時 <= "{upper1}"', q)
                self.assertIn(f'(更新日時 > "{t}" or (更新日時 = "{t}" and $id > 2))', q)
                self.assertTrue(all("更新日時 >=" not in q_ for _a, q_ in self.fake.calls[n:]))
                ov = await ledger.sync_overview()
                self.assertEqual([x["status"] for x in ov["runs"][:2]], ["ok", "failed"])
            # 走査完了後は次の重複窓走査（前回位置 − 10 分・新しい上限）→ 4 件目に到達
            r3 = await sync.sync_target(sync.TARGET_APP40,
                                        now=now1 + datetime.timedelta(minutes=2))
            self.assertEqual((r3["status"], r3["resumed"]), ("ok", False))
            self.assertTrue(await ledger.list_case_facts("40", "4"))
            q = [q for a, q in self.fake.calls if a == "40"][-1]
            self.assertIn('更新日時 >= "2026-09-20T00:50:00Z"', q)
            self.assertIn('更新日時 <= "2026-09-25T00:02:00Z"', q)
            self.assertEqual((await ledger.get_cursor("app40"))["confirmed_until"],
                             "2026-09-25T00:02:00Z")
        run(body())

    def test_commit_failure_inside_page_transaction_rolls_back(self):
        # 補足 1: session.commit 自体の失敗（例外注入）でもページ全体が巻き戻る
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, "2026-09-20T02:00:00Z")]

        async def body():
            from sqlalchemy.ext.asyncio import AsyncSession
            real_advance = ledger.advance_cursor_with_page

            async def boom(_self):
                raise RuntimeError("commit failed")

            async def advance_with_commit_failure(*a, **kw):
                with patch.object(AsyncSession, "commit", boom):
                    return await real_advance(*a, **kw)
            with patch.object(sync, "PAGE_SIZE", 1), \
                    patch.object(ledger, "advance_cursor_with_page", advance_with_commit_failure):
                r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual((r["status"], r["failure"]), ("failed", "db_write_failed"))
            self.assertEqual(await ledger.list_case_facts("40", "1", current_only=False), [])
            self.assertIsNone(await ledger.latest_known_revision("40", "1"))
            async with session_scope() as session:
                n_events = (await session.execute(
                    sa.select(sa.func.count()).select_from(ledger.case_event))).scalar()
            self.assertEqual(int(n_events), 0)
            cur = await ledger.get_cursor("app40")
            self.assertEqual((cur["state"], cur["cursor_record_id"], cur["confirmed_until"]),
                             ("error", "", ""))
            failed = [x for x in (await ledger.sync_overview())["runs"] if x["status"] == "failed"][0]
            self.assertEqual(failed["incomplete_page"],
                             {"after_updated_at": None, "after_record_id": None})
            # 障害が解ければ通常どおり
            r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(r["status"], "ok")
            self.assertTrue(await ledger.list_case_facts("40", "1"))
            self.assertTrue(await ledger.list_case_facts("40", "2"))
        run(body())


class TestUndecidable(BrainDbMixin):
    """fix2 BA-12 / R12: 判定不能は「処理不要」と区別し pending_recheck として記録する。"""

    def test_undecidable_is_pending_recheck_not_ok(self):
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1)]

        async def fresh():
            return await ledger.case_freshness_detail("40", "1", "app40", sync.source_targets())

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP28)
            # 出典を確認不能にしてから復活させ、LINE 検索だけ失敗させる
            self.fake.data["28"] = []
            self.assertEqual((await sync.recheck_target(sync.TARGET_APP28))["unavailable"], 1)
            self.assertEqual([(h["source_record_id"], h["state"]) for h in await ledger.list_holds()],
                             [("100", "unavailable")])
            prev_done = (await ledger.get_cursor("app28", kind="recheck"))["recheck_completed_at"]
            self.fake.data["28"] = [app28(100, 1)]
            self.fake.fail_at = {len(self.fake.calls) + 2}         # 1=$id in・2=LINE 検索
            rc = await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual((rc["status"], rc["complete"], rc["pending_recheck"]),
                             ("partial", False, 1))
            holds = await ledger.list_holds()
            self.assertEqual([(h["source_record_id"], h["state"], h["pending_recheck"]) for h in holds],
                             [("100", "unavailable", True)])          # unavailable は解除されない
            self.assertEqual(await ledger.count_pending_recheck("28"), 1)
            fd = await fresh()
            self.assertEqual(fd["state"], "partial")
            self.assertEqual(fd["reasons"], ["app28:100:pending_recheck"])
            # 関連は保持（案件 1 の出来事は現在値のまま）
            self.assertEqual(await ledger.current_case_of_source("28", "100"), ("40", "1"))
            self.assertTrue([e for e in await ledger.list_case_events("40", "1")
                             if e["source_app_id"] == "28"])
            # 一巡完了にならない（完了時刻は前回のまま進まない）・次の job で続きが回る
            rcur = await ledger.get_cursor("app28", kind="recheck")
            self.assertEqual((rcur["state"], rcur["recheck_completed_at"]), ("incomplete", prev_done))
            self.assertTrue(await sync._recheck_due(
                "app28", await ledger.get_cursor("app28") or {}, sync._now()))
            self.assertEqual((await ledger.sync_overview())["pending_recheck"], 1)
            # 次回に検索成功で復旧: unavailable 解除・pending 解除・一巡完了・鮮度 synced
            self.fake.fail_at = set()
            rc = await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual((rc["status"], rc["complete"], rc["pending_recheck"], rc["pending_left"]),
                             ("ok", True, 0, 0))
            self.assertEqual(await ledger.list_holds(), [])
            self.assertEqual(await fresh(), {"state": "synced", "reasons": []})
            done = (await ledger.get_cursor("app28", kind="recheck"))["recheck_completed_at"]
            self.assertTrue(done and done > prev_done)                 # 一巡完了が進んだ
            # 通常同期でも判定不能は partial（新規行は行が無いので記録せず、窓・照合で再試行）
            self.fake.data["28"].append(app28(101, 1, "2026-09-21T01:00:00Z"))
            self.fake.fail_at = {len(self.fake.calls) + 3}         # 1=ページ・2=100 の検索・3=101 の検索
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual((r["status"], r["pending_recheck"]), ("partial", 1))
            self.assertIsNone(await ledger.latest_known_revision("28", "101"))
            self.assertEqual(await ledger.count_pending_recheck("28"), 0)
            self.fake.fail_at = set()
            self.assertEqual((await sync.sync_target(sync.TARGET_APP28))["status"], "ok")
            self.assertEqual(await ledger.latest_known_revision("28", "101"), 1)
            # App 30: 参照先の実在確認が失敗しても関連を外さない（pending のまま・moved 0）
            self.fake.data["40"] = [app40(1, 1), app40(2, 1)]     # No.2 は台帳未同期
            self.fake.data["30"] = [app30(5, 1, 案件レコードID="2")]
            await sync.sync_target(sync.TARGET_APP30)
            self.assertEqual(await ledger.current_case_of_source("30", "5"), ("40", "2"))
            self.fake.fail_at = {len(self.fake.calls) + 2}         # 1=$id in・2=App 40 実在確認
            rc = await sync.recheck_target(sync.TARGET_APP30)
            self.assertEqual((rc["status"], rc["moved"], rc["pending_recheck"]), ("partial", 0, 1))
            self.assertEqual(await ledger.current_case_of_source("30", "5"), ("40", "2"))
            self.assertTrue([f for f in await ledger.list_case_facts("40", "2")
                             if f["source_app_id"] == "30"])
            self.assertEqual([(h["source_app_id"], h["source_record_id"], h["pending_recheck"])
                              for h in await ledger.list_holds()], [("30", "5", True)])
            self.assertEqual(await ledger.count_pending_recheck("30"), 1)
            self.fake.fail_at = set()
            rc = await sync.recheck_target(sync.TARGET_APP30)
            self.assertEqual((rc["status"], rc["complete"], rc["pending_left"]), ("ok", True, 0))
        run(body())


class TestFix3Pending(BrainDbMixin):
    """fix3 BA-15（判定成功による不採用でも pending 解除）/ BA-17（初見の判定不能を永続化）。"""

    def test_decided_non_adopt_clears_pending(self):
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP28)
            # LINE 検索失敗で pending（検索失敗時は保持）
            self.fake.fail_at = {len(self.fake.calls) + 2}
            rc = await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual((rc["status"], rc["pending_recheck"], rc["complete"]), ("partial", 1, False))
            self.assertEqual(await ledger.count_pending_recheck("28"), 1)
            self.fake.raise_all = True
            rc = await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual(rc["status"], "failed")
            self.assertEqual(await ledger.count_pending_recheck("28"), 1)     # 失敗では保持
            self.fake.raise_all = False
            # 対象外 category の新版 → 判定成功による不採用（関連解除 R10）＋pending 解除（同一 tx）
            self.fake.data["28"] = [app28(100, 2, "2026-09-21T01:00:00Z", category="その他判断系")]
            rc = await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual((rc["status"], rc["pending_left"], rc["complete"]), ("ok", 0, True))
            self.assertEqual([(h["source_record_id"], h["state"], h["pending_recheck"])
                              for h in await ledger.list_holds()], [("100", "detached", False)])
            self.assertEqual((await ledger.latest_link("28", "100"))["reason"], "category_out_of_set")
            # detached のまま再び pending → 対象外のまま（変化なし）でも判定成功なら解除
            await ledger.set_pending_recheck("28", "100", True)
            rc = await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual((rc["pending_left"], rc["complete"]), (0, True))
            # 一意不成立・参照消去でも同じ（App 30: 参照消去）
            self.fake.data["30"] = [app30(5, 1, 案件レコードID="1")]
            await sync.sync_target(sync.TARGET_APP30)
            await ledger.set_pending_recheck("30", "5", True)
            self.fake.data["30"] = [app30(5, 2, "2026-09-21T01:00:00Z", 案件レコードID="")]
            rc = await sync.recheck_target(sync.TARGET_APP30)
            self.assertEqual((rc["pending_left"], rc["complete"]), (0, True))
            self.assertEqual([p["hold_reason"] for p in await ledger.list_pending_links()],
                             ["ref_not_digits"])
        run(body())

    def test_unregistered_undecidable_is_persisted_and_retried(self):
        # BA-17: 初見の App 28 行の LINE 検索失敗 → run ok にならない・cursor synced にならない・
        # overview に 1・再起動後（新しい run）も再試行・次回成功で解消
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            self.fake.fail_at = {len(self.fake.calls) + 2}          # 1=ページ・2=LINE 検索
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual((r["status"], r["pending_recheck"], r["pending_unregistered"]),
                             ("partial", 1, 1))
            self.assertIsNone(await ledger.latest_known_revision("28", "100"))   # 出典行は作らない
            run_row = (await ledger.sync_overview())["runs"][0]
            self.assertEqual((run_row["status"], run_row["failure"], run_row["pending_unregistered"]),
                             ("partial", "pending_unregistered", 1))
            cur = await ledger.get_cursor("app28")
            self.assertEqual((cur["state"], cur["confirmed_until"], cur["pending_unregistered"],
                              cur["cursor_updated_at"], cur["cursor_record_id"]),
                             ("incomplete", "", 1, "2026-09-21T00:00:00Z", "100"))
            self.assertEqual((await ledger.sync_overview())["pending_unregistered"], 1)
            self.assertEqual(await ledger.case_freshness("28", "100", "app28"), "incomplete")
            # 再起動（新しい run）でも再試行され、失敗が続く間は未完了表示が維持される
            self.fake.fail_at = {len(self.fake.calls) + 2}
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual((r["status"], r["pending_unregistered"]), ("partial", 1))
            cur = await ledger.get_cursor("app28")
            self.assertEqual((cur["state"], cur["confirmed_until"], cur["pending_unregistered"]),
                             ("incomplete", "", 1))
            self.assertEqual((await ledger.sync_overview())["pending_unregistered"], 1)
            # 次回成功で解消
            self.fake.fail_at = set()
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual((r["status"], r["pending_unregistered"]), ("ok", 0))
            self.assertEqual(await ledger.latest_known_revision("28", "100"), 1)
            cur = await ledger.get_cursor("app28")
            self.assertEqual((cur["state"], cur["pending_unregistered"]), ("synced", 0))
            self.assertTrue(cur["confirmed_until"])
            ov = await ledger.sync_overview()
            self.assertEqual((ov["pending_unregistered"], ov["runs"][0]["status"]), (0, "ok"))
        run(body())


class _App:
    def __init__(self, app_id):
        self._id = app_id

    def app_id(self):
        return self._id


class TestFakeKintoneContract(unittest.TestCase):
    """補足 2: 偽 kintone の厳格化（fields 尊重・未知構文は例外・limit/offset/order by）。"""

    def setUp(self):
        self.app = _App("40")
        self.fake = FakeKintone({"40": [app40(i, 1, f"2026-09-20T0{i}:00:00Z")
                                        for i in range(1, 6)]})

    def _ids(self, query, fields=None):
        return [r["$id"]["value"] for r in run(self.fake.search_records(self.app, query, fields))]

    def test_fields_are_respected(self):
        rows = run(self.fake.search_records(self.app, '$id = "3" limit 1', fields=["$id", "$revision"]))
        self.assertEqual(rows, [{"$id": {"value": "3"}, "$revision": {"value": "1"}}])
        full = run(self.fake.search_records(self.app, '$id = "3" limit 1'))
        self.assertIn("債権者一覧", full[0])
        with self.assertRaises(FakeQueryError):
            run(self.fake.search_records(self.app, '$id = "3" limit 1', fields=["存在しない欄"]))

    def test_unknown_syntax_and_operators_raise(self):
        for q in ('存在しない欄 = "x"', '更新日時 like "2026"', '顧客名 > "a"', '$id ~ 3',
                  'limit 501', 'offset 10001', '$id = "1" order by 顧客名 asc limit 1 foo',
                  '債権者一覧 = "x"', '$id in (1, 2', '$id = "1" limit 1 limit 1',
                  '$id = "1" order by 存在しない欄 asc'):
            with self.subTest(q=q), self.assertRaises(FakeQueryError):
                run(self.fake.search_records(self.app, q))

    def test_limit_offset_and_order_semantics(self):
        self.assertEqual(self._ids("order by $id asc limit 2 offset 2"), ["3", "4"])
        self.assertEqual(self._ids("limit 500"), ["5", "4", "3", "2", "1"])   # 既定は $id 降順
        self.assertEqual(self._ids("$id > 0"), ["5", "4", "3", "2", "1"])     # limit 既定 100
        self.assertEqual(self._ids("order by 更新日時 desc, $id asc limit 100"),
                         ["5", "4", "3", "2", "1"])
        self.assertEqual(self._ids(
            '更新日時 <= "2026-09-20T02:00:00Z" and (更新日時 > "2026-09-20T01:00:00Z" or '
            '(更新日時 = "2026-09-20T01:00:00Z" and $id > 0)) '
            'order by 更新日時 asc, $id asc limit 100'), ["1", "2"])
        self.assertEqual(self._ids('$id in ("2","4") limit 10'), ["4", "2"])
        self.assertEqual(self._ids(f'LINEユーザーID = "{LINE_A}" limit 2'), ["5", "4"])
        self.assertEqual(self._ids(f'LINEユーザーID = "{LINE_A}" limit 2', fields=["$id"]), ["5", "4"])
        self.assertEqual(self._ids('案件アプリID = "40"' if False else '$id != "3" limit 500'),
                         ["5", "4", "2", "1"])


if __name__ == "__main__":
    unittest.main()
