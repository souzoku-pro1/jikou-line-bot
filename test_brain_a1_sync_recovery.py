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

import unittest
from unittest.mock import patch

from brain_test_support import BrainDbMixin, app40, run
from hub import brain_ledger as ledger
from hub import brain_sync as sync


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


if __name__ == "__main__":
    unittest.main()
