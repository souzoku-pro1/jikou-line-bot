"""BRAIN-A1 T-D 訂正・確認履歴（v3 §10-4・§3・§4-1・§4-2）

固定する仕様（すべて削除なし＝行数が減らない）:
- 値の変更 → 新 fact が旧 fact を supersede（旧は is_current=False・invalid_reason=superseded）
- 欄の消去 → 空値の新 fact（履歴として残る）
- 案件移動（App 30 の案件参照が別の App 40 へ）→ link_history に ref_changed、旧案件側の
  fact は link_moved で現在ビューから除外、新案件側へ積む
- 取得不能 → source_ingest=unavailable（fact は消さない・値なしにしない）。再出現で復帰
- 確認は fact の版に固定・新版へ継承しない。新版が来たら「再確認が必要」に出る
- 確認撤回は revoke 行の追加（元の行は残る）。撤回後は再確認一覧から消える
- 手動の紐付け訂正は履歴追加のみ。出典 revision が変わるまで自動判定より優先し、
  revision が変わって参照が変われば自動判定へ戻る
"""

import unittest

from brain_test_support import BrainDbMixin, app28, app30, app40, run, subrow
from hub import brain_ledger as ledger
from hub import brain_sync as sync
from hub.db import session_scope

import sqlalchemy as sa


async def _count(table):
    async with session_scope() as session:
        return int((await session.execute(sa.select(sa.func.count()).select_from(table))).scalar())


class TestCorrectionHistory(BrainDbMixin):
    def test_value_change_and_clear_are_history_not_delete(self):
        self.fake.data["40"] = [app40(1, 1, 事件番号="令和8年(家)第1号")]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            n1 = await _count(ledger.case_fact)
            self.fake.data["40"] = [app40(1, 2, "2026-09-20T02:00:00Z", status="受理", 事件番号="")]
            await sync.sync_target(sync.TARGET_APP40)
            self.assertGreater(await _count(ledger.case_fact), n1)
            allf = await ledger.list_case_facts("40", "1", current_only=False)
            st = sorted((f["version"], f["value_text"], f["is_current"], f["invalid_reason"])
                        for f in allf if f["item_code"] == "app40.status")
            self.assertEqual(st, [(1, "受任", False, "superseded"), (2, "受理", True, None)])
            jn = sorted((f["version"], f["value_text"], f["is_current"])
                        for f in allf if f["item_code"] == "app40.事件番号")
            self.assertEqual(jn, [(1, "令和8年(家)第1号", False), (2, "", True)])
            # supersedes の連鎖: 新→旧
            async with session_scope() as session:
                rows = (await session.execute(sa.select(ledger.case_fact).where(
                    ledger.case_fact.c.item_code == "app40.status")
                    .order_by(ledger.case_fact.c.source_revision))).fetchall()
            self.assertEqual(rows[1].supersedes_fact_id, rows[0].fact_id)
        run(body())

    def test_case_move_of_app30_reference(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1)]
        self.fake.data["30"] = [app30(5, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP30)
            self.assertTrue([f for f in await ledger.list_case_facts("40", "1")
                             if f["source_app_id"] == "30"])
            # 参照が 1 → 2 へ（新 revision）
            self.fake.data["30"] = [app30(5, 2, "2026-09-21T01:00:00Z", 案件レコードID="2")]
            r = await sync.sync_target(sync.TARGET_APP30)
            self.assertEqual(r["status"], "ok")
            self.assertEqual([f for f in await ledger.list_case_facts("40", "1")
                              if f["source_app_id"] == "30"], [])
            moved = [f for f in await ledger.list_case_facts("40", "2") if f["source_app_id"] == "30"]
            self.assertTrue(moved)
            old = [f for f in await ledger.list_case_facts("40", "1", current_only=False)
                   if f["source_app_id"] == "30"]
            self.assertTrue(old and all(f["invalid_reason"] == "link_moved" for f in old))
            last = await ledger.latest_link("30", "5")
            self.assertEqual((last["reason"], last["prev_case"], last["new_case"]),
                             ("ref_changed", ("40", "1"), ("40", "2")))
            self.assertEqual(await ledger.current_case_of_source("30", "5"), ("40", "2"))
            async with session_scope() as session:
                new_row = (await session.execute(sa.select(ledger.case_fact).where(
                    ledger.case_fact.c.source_app_id == "30",
                    ledger.case_fact.c.item_code == "app30.件名",
                    ledger.case_fact.c.is_current.is_(True)))).first()
            self.assertEqual((new_row.prev_case_app_id, new_row.prev_case_record_id), ("40", "1"))
        run(body())

    def test_unavailable_source_is_not_deleted_and_recovers(self):
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["30"] = [app30(5, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP30)
            fact = [f for f in await ledger.list_case_facts("40", "1")
                    if f["item_code"] == "app30.件名"][0]
            await ledger.record_confirmation(fact_id=fact["fact_id"], seen_version=fact["version"],
                                             decision="confirm", reason="", operation_id="op-u1",
                                             actor="owner")
            n = await _count(ledger.case_fact)
            self.fake.data["30"] = []                      # 消えた（権限・削除）
            rc = await sync.recheck_target(sync.TARGET_APP30)
            self.assertEqual(rc["unavailable"], 1)
            self.assertEqual(await _count(ledger.case_fact), n)          # 削除しない
            holds = await ledger.list_holds()
            self.assertEqual([(h["source_record_id"], h["state"]) for h in holds],
                             [("5", "unavailable")])
            # 値は「値なし」にならない
            cur = [f for f in await ledger.list_case_facts("40", "1") if f["item_code"] == "app30.件名"]
            self.assertEqual(cur[0]["value_text"], "受理通知送付状")
            # 確認は再確認対象（source_unavailable）
            rk = await ledger.list_recheck()
            self.assertEqual([x["reasons"] for x in rk], [["source_unavailable"]])
            self.assertEqual(await ledger.case_freshness("30", "5", "app30"), "error")
            # 出典確認不能は競合の相手にしない
            self.assertEqual(await ledger.list_conflicts(), [])
            # 再出現 → 復帰
            self.fake.data["30"] = [app30(5, 1)]
            rc = await sync.recheck_target(sync.TARGET_APP30)
            self.assertEqual(rc["unavailable"], 0)
            self.assertEqual(await ledger.list_holds(), [])
            self.assertEqual(await ledger.list_recheck(), [])
        run(body())

    def test_confirmation_pinned_to_version_and_revoke(self):
        self.fake.data["40"] = [app40(1, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            f1 = [f for f in await ledger.list_case_facts("40", "1") if f["item_code"] == "app40.status"][0]
            r = await ledger.record_confirmation(fact_id=f1["fact_id"], seen_version=1,
                                                 decision="confirm", reason="ok",
                                                 operation_id="op-1", actor="owner")
            cid = r["confirmation_id"]
            # 新版が来る → 確認は継承されず「再確認が必要」
            self.fake.data["40"] = [app40(1, 2, "2026-09-20T02:00:00Z", status="受理")]
            await sync.sync_target(sync.TARGET_APP40)
            rk = await ledger.list_recheck()
            self.assertEqual([(x["confirmation_id"], x["reasons"], x["current"]["version"])
                              for x in rk], [(cid, ["superseded"], 2)])
            f2 = [f for f in await ledger.list_case_facts("40", "1") if f["item_code"] == "app40.status"][0]
            self.assertEqual(await ledger.list_confirmations(f2["fact_id"]), [])   # 新版に確認なし
            # 古い版への確認操作は拒否（最新を返す）
            with self.assertRaises(ledger.VersionConflict) as cm:
                await ledger.record_confirmation(fact_id=f1["fact_id"], seen_version=1,
                                                 decision="confirm", reason="", operation_id="op-2",
                                                 actor="owner")
            self.assertEqual(cm.exception.current["version"], 2)
            self.assertEqual(cm.exception.current["fact_id"], f2["fact_id"])
            # 撤回（元の行は残る・撤回行が増える）→ 再確認一覧から消える
            n = await _count(ledger.case_confirmation)
            rv = await ledger.record_confirmation(fact_id=f1["fact_id"], seen_version=2,
                                                  decision="revoke", reason="古い",
                                                  operation_id="op-3", actor="owner",
                                                  revoked_of=cid)
            self.assertFalse(rv["duplicate"])
            self.assertEqual(await _count(ledger.case_confirmation), n + 1)
            confs = await ledger.list_confirmations(f1["fact_id"])
            self.assertEqual([(c["decision"], c["revoked_of"]) for c in confs],
                             [("confirm", None), ("revoke", cid)])
            self.assertEqual(await ledger.list_recheck(), [])
            # 撤回対象が別 fact の確認なら拒否
            with self.assertRaises(ledger.LedgerError):
                await ledger.record_confirmation(fact_id=f2["fact_id"], seen_version=2,
                                                 decision="revoke", reason="", operation_id="op-4",
                                                 actor="owner", revoked_of=cid)
            # 新版へ確認（現在版一致）
            ok = await ledger.record_confirmation(fact_id=f2["fact_id"], seen_version=2,
                                                  decision="confirm", reason="", operation_id="op-5",
                                                  actor="owner")
            self.assertFalse(ok["duplicate"])
        run(body())

    def test_manual_relink_history_and_pinning(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1)]
        self.fake.data["30"] = [app30(5, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP30)
            n_links = await _count(ledger.link_history)
            # BA-06: 画面が見ていた紐付け版・出典 revision を添える（期待値は不変）
            seen = dict(seen_link_version=await ledger.link_version("30", "5"),
                        seen_source_revision=1)
            r = await ledger.relink_source(source_app_id="30", source_record_id="5",
                                           new_case=("40", "2"), reason="誤紐付け",
                                           operation_id="op-r1", actor="owner", **seen)
            self.assertEqual((r["duplicate"], r["prev"]), (False, ("40", "1")))
            self.assertEqual(await _count(ledger.link_history), n_links + 1)
            # 同じ操作 ID は無視（履歴が増えない）
            r2 = await ledger.relink_source(source_app_id="30", source_record_id="5",
                                            new_case=("40", "2"), reason="誤紐付け",
                                            operation_id="op-r1", actor="owner", **seen)
            self.assertTrue(r2["duplicate"])
            self.assertEqual(await _count(ledger.link_history), n_links + 1)
            # 旧案件の現在ビューから除外・次回同期で新案件側へ積まれる（手動が優先）
            self.assertEqual([f for f in await ledger.list_case_facts("40", "1")
                              if f["source_app_id"] == "30"], [])
            await sync.sync_target(sync.TARGET_APP30)
            self.assertTrue([f for f in await ledger.list_case_facts("40", "2")
                             if f["source_app_id"] == "30"])
            self.assertEqual([f for f in await ledger.list_case_facts("40", "1")
                              if f["source_app_id"] == "30"], [])
            self.assertEqual((await ledger.latest_link("30", "5"))["actor"], "owner")
            # 出典の revision が進み、参照自体が 1 のまま → まだ手動が優先されるか？
            # 仕様: revision が変わったら自動判定へ戻る（正本の参照が正）
            self.fake.data["30"] = [app30(5, 2, "2026-09-21T01:00:00Z")]     # 参照は 1 のまま
            await sync.sync_target(sync.TARGET_APP30)
            self.assertTrue([f for f in await ledger.list_case_facts("40", "1")
                             if f["source_app_id"] == "30"])
            last = await ledger.latest_link("30", "5")
            self.assertEqual((last["actor"], last["reason"], last["new_case"]),
                             ("system", "ref_changed", ("40", "1")))
            # 履歴は全て残る
            self.assertGreaterEqual(await _count(ledger.link_history), n_links + 2)
        run(body())

    def test_conflict_between_valid_sources(self):
        async def body():
            a = ledger.SourceRef("40", "1", 1, "app40_record", "1")
            b = ledger.SourceRef("30", "7", 1, "app30_record", "1")
            await ledger.ingest_source(a, ("40", "1"), [ledger.FactIn(
                "case", "app40.顧客名", "text", "甲", ledger.make_locator("顧客名"))])
            await ledger.ingest_source(b, ("40", "1"), [ledger.FactIn(
                "case", "app40.顧客名", "text", "乙", ledger.make_locator("宛先名"))])
            c = await ledger.list_conflicts()
            self.assertEqual(len(c), 1)
            self.assertEqual({f["value_text"] for f in c[0]["facts"]}, {"甲", "乙"})
            # 正常な履歴の旧値は競合ではない
            await ledger.ingest_source(ledger.SourceRef("40", "1", 2, "app40_record", "1"),
                                       ("40", "1"), [ledger.FactIn(
                                           "case", "app40.顧客名", "text", "乙",
                                           ledger.make_locator("顧客名"))])
            self.assertEqual(await ledger.list_conflicts(), [])
        run(body())

    # ── fix1: BA-02（R10 関連喪失の共通処理）/ BA-03（行消去）/ BA-08（R9） ──

    async def _facts30(self, case=("40", "1"), current_only=True):
        return [f for f in await ledger.list_case_facts(*case, current_only=current_only)
                if f["source_app_id"] == "30"]

    def test_lost_reference_detaches_facts_and_flags_confirmations(self):
        # 参照が別アプリへ／消去／不正になったら: link_history（理由付き）→ 旧案件の fact/event を
        # 現在ビューから外す（link_lost）→ 確認は再確認一覧へ。通常同期と追跡再照合の両経路
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["30"] = [app30(5, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP30)
            fact = [f for f in await self._facts30() if f["item_code"] == "app30.件名"][0]
            await ledger.record_confirmation(fact_id=fact["fact_id"], seen_version=1,
                                             decision="confirm", reason="", operation_id="op-l1",
                                             actor="owner")
            n_facts, n_events = await _count(ledger.case_fact), await _count(ledger.case_event)
            # (a) App 40/No.1 → App 26 へ（追跡再照合の経路）
            self.fake.data["30"] = [app30(5, 2, "2026-09-21T01:00:00Z", 案件アプリID="26")]
            rc = await sync.recheck_target(sync.TARGET_APP30)
            self.assertEqual((rc["status"], rc["moved"]), ("ok", 1))
            self.assertEqual(await self._facts30(), [])
            old = await self._facts30(current_only=False)
            self.assertTrue(old and all(f["invalid_reason"] == "link_lost" for f in old))
            self.assertEqual([e for e in await ledger.list_case_events("40", "1")
                              if e["source_app_id"] == "30"], [])
            self.assertEqual({e["invalid_reason"] for e in
                              await ledger.list_case_events("40", "1", current_only=False)
                              if e["source_app_id"] == "30"}, {"link_lost"})
            self.assertEqual((await _count(ledger.case_fact), await _count(ledger.case_event)),
                             (n_facts, n_events))                                  # 削除なし
            self.assertEqual([x["reasons"] for x in await ledger.list_recheck()], [["link_lost"]])
            last = await ledger.latest_link("30", "5")
            self.assertEqual((last["trust_level"], last["reason"], last["prev_case"], last["new_case"]),
                             ("hold", "ref_other_app", ("40", "1"), None))
            self.assertIsNone(await ledger.current_case_of_source("30", "5"))
            self.assertEqual([(p["source_record_id"], p["hold_reason"], p["source_revision"])
                              for p in await ledger.list_pending_links()], [("5", "ref_other_app", 2)])
            self.assertEqual(await ledger.list_case_facts("26", "1"), [])
            # 参照が戻れば履歴を繋いで復帰（新 revision）。旧版への確認は再確認のまま
            self.fake.data["30"] = [app30(5, 3, "2026-09-21T02:00:00Z")]
            await sync.recheck_target(sync.TARGET_APP30)
            cur = [f for f in await self._facts30() if f["item_code"] == "app30.件名"]
            self.assertEqual([(f["value_text"], f["version"]) for f in cur], [("受理通知送付状", 3)])
            async with session_scope() as session:
                row = (await session.execute(sa.select(ledger.case_fact).where(
                    ledger.case_fact.c.fact_id == cur[0]["fact_id"]))).first()
            self.assertEqual(row.supersedes_fact_id, fact["fact_id"])
            self.assertEqual(await ledger.list_pending_links(), [])
            self.assertEqual(await ledger.current_case_of_source("30", "5"), ("40", "1"))
            self.assertEqual([x["reasons"] for x in await ledger.list_recheck()], [["link_lost"]])
            self.assertTrue([e for e in await ledger.list_case_events("40", "1")
                             if e["source_app_id"] == "30"])
            # (b) 参照消去（通常同期の経路）
            self.fake.data["30"] = [app30(5, 4, "2026-09-21T03:00:00Z", 案件レコードID="")]
            self.assertEqual((await sync.sync_target(sync.TARGET_APP30))["status"], "ok")
            self.assertEqual(await self._facts30(), [])
            self.assertEqual((await ledger.latest_link("30", "5"))["reason"], "ref_not_digits")
            self.assertEqual([p["hold_reason"] for p in await ledger.list_pending_links()],
                             ["ref_not_digits"])
            # (c) 不正参照（存在しない No.999・追跡再照合の経路）
            self.fake.data["30"] = [app30(5, 5, "2026-09-21T04:00:00Z")]
            await sync.recheck_target(sync.TARGET_APP30)
            self.assertTrue(await self._facts30())
            self.fake.data["30"] = [app30(5, 6, "2026-09-21T05:00:00Z", 案件レコードID="999")]
            await sync.recheck_target(sync.TARGET_APP30)
            self.assertEqual(await self._facts30(), [])
            last = await ledger.latest_link("30", "5")
            self.assertEqual((last["reason"], last["candidates"][0]["case_record_id"],
                              last["candidates"][0]["trust"]), ("ref_missing", "999", "candidate"))
            self.assertEqual(await ledger.list_case_facts("40", "999"), [])
            self.assertEqual((await _count(ledger.case_fact)) >= n_facts, True)
        run(body())

    def test_removed_subtable_rows_are_retired_only_on_full_fetch(self):
        # BA-03: 完全取得した新版でだけ行 ID 集合を比較し、消えた行を row_removed で利用停止。
        # 取得失敗・部分取得（欄なし）・遅着の旧 revision では判断しない
        self.fake.data["40"] = [app40(1, 1)]

        async def creditors(current_only=True):
            return {f["subject_id"] for f in await ledger.list_case_facts("40", "1", current_only)
                    if f["subject_id"].startswith("creditor:")}

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(await creditors(), {"creditor:11", "creditor:12"})
            n = await _count(ledger.case_fact)
            # 取得失敗 → 消去と判断しない
            self.fake.data["40"] = [app40(1, 2, "2026-09-20T02:00:00Z", 債権者一覧=[])]
            self.fake.fail_at = {len(self.fake.calls) + 1}
            self.assertEqual((await sync.sync_target(sync.TARGET_APP40))["status"], "failed")
            self.assertEqual(await creditors(), {"creditor:11", "creditor:12"})
            self.fake.fail_at = set()
            # 部分取得（サブテーブル欄が無い＝fields 絞り込み相当）→ 判断しない
            from brain_test_support import rec
            partial = rec(1, 2, "2026-09-20T02:00:00Z", status="受任", 顧客名="テスト太郎")
            self.assertEqual(sync.app40_subjects_seen(partial), {"creditor:": None, "document:": None})
            src, key, facts, events = sync.convert_app40(partial)
            self.assertFalse(any(f.item_code == "app40.LINEユーザーID" for f in facts))  # 欄なし≠消去
            await ledger.ingest_source(src, key, facts, events,
                                       extras={"subjects_seen": sync.app40_subjects_seen(partial)})
            self.assertEqual(await creditors(), {"creditor:11", "creditor:12"})
            self.assertTrue([f for f in await ledger.list_case_facts("40", "1")
                             if f["item_code"] == "app40.LINEユーザーID" and f["value_text"]])
            # 完全取得した新版（rev3）で一覧が空 → 行の fact を row_removed（削除なし）
            self.fake.data["40"] = [app40(1, 3, "2026-09-20T03:00:00Z", 債権者一覧=[])]
            self.assertEqual((await sync.sync_target(sync.TARGET_APP40))["status"], "ok")
            self.assertEqual(await creditors(), set())
            retired = [f for f in await ledger.list_case_facts("40", "1", current_only=False)
                       if f["subject_id"].startswith("creditor:")]
            self.assertTrue(retired and all(
                f["invalid_reason"] == "row_removed" and not f["is_current"] for f in retired))
            self.assertGreaterEqual(await _count(ledger.case_fact), n)
            # 遅着の旧 revision（rev2・行あり）は消去判定も復帰もしない（R8）
            late = app40(1, 2, "2026-09-20T02:00:00Z")
            src2, key2, facts2, ev2 = sync.convert_app40(late)
            await ledger.ingest_source(src2, key2, facts2, ev2,
                                       extras={"subjects_seen": sync.app40_subjects_seen(late)})
            self.assertEqual(await creditors(), set())
            # 行 11 が戻った rev4 → 履歴を繋いで復帰（12 は利用停止のまま）
            self.fake.data["40"] = [app40(1, 4, "2026-09-20T04:00:00Z",
                                          債権者一覧=[subrow(11, 債権者名="甲社", 通知要否="要")])]
            await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(await creditors(), {"creditor:11"})
            name = [f for f in await ledger.list_case_facts("40", "1")
                    if f["subject_id"] == "creditor:11"
                    and f["item_code"] == "app40.債権者一覧.債権者名"][0]
            async with session_scope() as session:
                row = (await session.execute(sa.select(ledger.case_fact).where(
                    ledger.case_fact.c.fact_id == name["fact_id"]))).first()
                prev = (await session.execute(sa.select(ledger.case_fact).where(
                    ledger.case_fact.c.fact_id == row.supersedes_fact_id))).first()
            self.assertEqual((prev.invalid_reason, int(prev.source_revision)), ("row_removed", 1))
        run(body())

    def test_chat_events_and_shipping_subjects_do_not_conflict(self):
        # BA-08/R9: 会話は case_event（競合判定の対象外・冪等キー=App 28 レコード番号）、
        # 発送は subject=shipping:{No}（同一発送の同一項目の別観測だけが競合）
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1, message="口座は A 銀行です"),
                                app28(101, 1, role="assistant", message="口座は B 銀行です")]
        self.fake.data["30"] = [app30(5, 1, 件名="受理通知送付状"), app30(6, 1, 件名="債権者通知")]

        async def chats():
            return [e for e in await ledger.list_case_events("40", "1") if e["source_app_id"] == "28"]

        async def body():
            for target in sync.TARGETS:
                await sync.sync_target(target)
            self.assertEqual(await ledger.list_conflicts(), [])
            self.assertEqual([(e["source_record_id"], e["kind"], e["summary"]) for e in await chats()],
                             [("100", "chat_message", "chat:user:hearing"),
                              ("101", "chat_message", "chat:assistant:hearing")])
            self.assertEqual([f for f in await ledger.list_case_facts("40", "1")
                              if f["source_app_id"] == "28"], [])          # 会話は fact にしない
            self.assertEqual({f["subject_id"] for f in await self._facts30()},
                             {"shipping:5", "shipping:6"})
            # 会話の版更新で出来事は増えない
            self.fake.data["28"][0] = app28(100, 2, "2026-09-21T01:00:00Z", message="訂正")
            await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual(len(await chats()), 2)
            self.assertEqual(await ledger.latest_known_revision("28", "100"), 2)
            # 同一発送の同一項目の別観測（変換器版違い）だけが競合
            other = ledger.SourceRef("30", "5", 1, "app30_record", "9")
            await ledger.ingest_source(other, ("40", "1"), [ledger.FactIn(
                "shipping:5", "app30.件名", "text", "別の件名", ledger.make_locator("件名"))])
            c = await ledger.list_conflicts()
            self.assertEqual([(x["subject_id"], x["item_code"]) for x in c],
                             [("shipping:5", "app30.件名")])
            self.assertEqual({f["value_text"] for f in c[0]["facts"]}, {"受理通知送付状", "別の件名"})
        run(body())


if __name__ == "__main__":
    unittest.main()
