"""hub/return_deadline.py（返送期限監視ジョブ）のテスト（T1-4）

日付固定モック（2026-07-03）で検証:
- 超過1日 → 警報対象（超過日数付き）
- 期限当日 → 対象外（期限日いっぱいは待つ）
- 期限が未来 → 対象外
- 期限なし（未設定）→「期限未設定」として警報に含める
- 全件正常 → 警報なし
- 発送済→返送待ち遷移時の返送期限自動設定（compute_deadline・dispatch 結線）
"""

import os
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from hub import kintone, return_deadline
from hub import scheduler as hub_scheduler

TODAY = date(2026, 7, 3)


def rec(rid, deadline, subject="職務上請求（川口市）", tracking="1234-5678-9012"):
    r = {
        "$id": {"value": rid},
        "件名": {"value": subject},
        "チャネル": {"value": "職務上請求"},
        "顧客名表示用": {"value": "山田太郎"},
        "返送期限": {"value": deadline},
        "追跡番号": {"value": tracking},
    }
    return r


def run_check(records):
    """日付固定＋kintone/notify モックで return_deadline_check を実行"""
    notify_mock = AsyncMock()
    with patch("hub.return_deadline._today_jst", return_value=TODAY), \
         patch("hub.kintone.search_records", new=AsyncMock(return_value=records)) as search, \
         patch("hub.notify.notify_admin_line", new=notify_mock):
        import asyncio
        problems = asyncio.run(return_deadline.return_deadline_check())
    return problems, notify_mock, search


class TestDeadlineDetection(unittest.TestCase):
    def test_overdue_by_one_day_is_detected(self):
        problems, notify_mock, _ = run_check([rec("1", "2026-07-02")])
        self.assertEqual(len(problems), 1)
        self.assertIn("超過1日", problems[0])
        self.assertIn("No.1", problems[0])
        notify_mock.assert_awaited_once()
        text = notify_mock.await_args.args[0]
        self.assertIn("【返送期限超過】", text)
        self.assertIn("職務上請求（川口市）", text)
        self.assertIn("1234-5678-9012", text)

    def test_due_today_is_not_overdue(self):
        problems, notify_mock, _ = run_check([rec("1", "2026-07-03")])
        self.assertEqual(problems, [])
        notify_mock.assert_not_awaited()

    def test_future_deadline_is_not_overdue(self):
        problems, notify_mock, _ = run_check([rec("1", "2026-07-24")])
        self.assertEqual(problems, [])
        notify_mock.assert_not_awaited()

    def test_missing_deadline_is_reported(self):
        """返送期限なしの返送待ちは「期限未設定」として警報（永遠に警報されない事故の防止）"""
        problems, notify_mock, _ = run_check([rec("1", "")])
        self.assertEqual(len(problems), 1)
        self.assertIn("期限が未設定", problems[0])
        notify_mock.assert_awaited_once()

    def test_invalid_deadline_value_is_reported(self):
        problems, _, _ = run_check([rec("1", "そのうち")])
        self.assertEqual(len(problems), 1)
        self.assertIn("不正な値", problems[0])

    def test_multiple_problems_in_single_alert(self):
        records = [
            rec("1", "2026-07-01"),   # 超過2日
            rec("2", "2026-07-03"),   # 当日 → 対象外
            rec("3", ""),             # 未設定
            rec("4", "2026-08-01"),   # 未来 → 対象外
        ]
        problems, notify_mock, _ = run_check(records)
        self.assertEqual(len(problems), 2)
        notify_mock.assert_awaited_once()  # まとめて1通
        text = notify_mock.await_args.args[0]
        self.assertIn("超過2日", text)
        self.assertIn("期限が未設定", text)

    def test_no_records_no_alert(self):
        problems, notify_mock, _ = run_check([])
        self.assertEqual(problems, [])
        notify_mock.assert_not_awaited()

    def test_alert_is_throttled_key(self):
        _, notify_mock, _ = run_check([rec("1", "2026-07-01")])
        self.assertEqual(notify_mock.await_args.kwargs.get("throttle_key"),
                         "return_deadline_check")

    def test_query_targets_waiting_status_only(self):
        _, _, search = run_check([])
        query = search.await_args.args[1]
        self.assertIn("返送待ち", query)
        self.assertIn("発送ステータス", query)

    def test_fetch_error_alerts_and_returns_empty(self):
        notify_mock = AsyncMock()
        err = kintone.KintoneError(500, "X", "down")
        with patch("hub.return_deadline._today_jst", return_value=TODAY), \
             patch("hub.kintone.search_records", new=AsyncMock(side_effect=err)), \
             patch("hub.notify.notify_admin_line", new=notify_mock):
            import asyncio
            problems = asyncio.run(return_deadline.return_deadline_check())
        self.assertEqual(problems, [])
        self.assertIn("実行失敗", notify_mock.await_args.args[0])


class TestComputeDeadline(unittest.TestCase):
    def test_uses_unit_config_days(self):
        with patch("hub.return_deadline._today_jst", return_value=TODAY):
            self.assertEqual(return_deadline.compute_deadline("時効援用"), "2026-07-24")  # +21日

    def test_unknown_unit_falls_back_to_default(self):
        with patch("hub.return_deadline._today_jst", return_value=TODAY):
            self.assertEqual(return_deadline.compute_deadline("未知ユニット"), "2026-07-24")


class TestDispatchSetsDeadline(unittest.IsolatedAsyncioTestCase):
    """発送済→返送待ち遷移で返送期限が自動設定されること（dispatch 結線・T1-2 の保留分）"""

    async def test_needs_return_sets_deadline_field(self):
        from channels.base import ChannelAdapter, DispatchResult
        from hub import dispatch

        class Adapter(ChannelAdapter):
            channel_name = "職務上請求"
            needs_return = True

            async def dispatch(self, record):
                return DispatchResult()

        record = {
            "$id": {"value": "9"}, "$revision": {"value": "1"},
            "発送ステータス": {"value": "承認済"},
            "チャネル": {"value": "職務上請求"},
            "ユニット種別": {"value": "時効援用"},
            "件名": {"value": "x"}, "顧客名表示用": {"value": "y"},
            "実行済み": {"value": "no"},
        }
        updates = []

        async def fake_update(app, rid, fields, revision=None):
            updates.append(dict(fields))

        import channels
        with patch.dict(channels.CHANNEL_REGISTRY, {"職務上請求": Adapter()}, clear=True), \
             patch("hub.return_deadline._today_jst", return_value=TODAY), \
             patch("hub.kintone.update_record", new=fake_update), \
             patch("hub.notify.notify_admin_line", new=AsyncMock()):
            await dispatch._handle_dispatch(record)

        waiting = [u for u in updates if u.get("発送ステータス") == "返送待ち"]
        self.assertEqual(len(waiting), 1)
        self.assertEqual(waiting[0]["返送期限"], "2026-07-24")


class TestJobRegistration(unittest.TestCase):
    def setUp(self):
        hub_scheduler.stop_all()
        self.addCleanup(hub_scheduler.stop_all)

    def test_registers_daily_job_at_8_jst(self):
        import asyncio

        async def run():
            with patch.dict("os.environ", {"RETURN_DEADLINE_DISABLED": ""}):
                return_deadline.register_return_deadline_job()
            self.assertTrue(hub_scheduler.is_registered("RETURN_DEADLINE"))
            job = hub_scheduler._jobs["RETURN_DEADLINE"]
            self.assertEqual(job.kind, "daily")
            self.assertEqual(job.hour_jst, 8)

        asyncio.run(run())

    def test_disabled_by_env(self):
        import asyncio

        async def run():
            with patch.dict("os.environ", {"RETURN_DEADLINE_DISABLED": "1"}):
                return_deadline.register_return_deadline_job()
            self.assertFalse(hub_scheduler.is_registered("RETURN_DEADLINE"))

        asyncio.run(run())

    def test_double_registration_is_safe(self):
        import asyncio

        async def run():
            with patch.dict("os.environ", {"RETURN_DEADLINE_DISABLED": ""}):
                return_deadline.register_return_deadline_job()
                task1 = hub_scheduler._jobs["RETURN_DEADLINE"].task
                return_deadline.register_return_deadline_job()
                task2 = hub_scheduler._jobs["RETURN_DEADLINE"].task
            self.assertIs(task1, task2)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
