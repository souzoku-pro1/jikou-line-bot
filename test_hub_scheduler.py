"""hub/scheduler.py の単体テスト（T0-2）

検証: 時刻計算（daily_healthcheck から移設・ロジック不変）・ジョブ隔離
（1ジョブの失敗が他ジョブを止めない）・start_all の冪等性・重複登録の検出・
daily_healthcheck.start_healthcheck_scheduler の従前挙動。
"""

import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from hub import scheduler

_JST = timezone(timedelta(hours=9))


def jst(h, m=0, s=0):
    return datetime(2026, 7, 3, h, m, s, tzinfo=_JST)


class TestSecondsUntilNextRun(unittest.TestCase):
    """時刻計算（従来 daily_healthcheck._seconds_until_next_run と同一ロジック）"""

    def test_before_hour_waits_until_today(self):
        self.assertEqual(scheduler._seconds_until_next_run(7, now=jst(6, 0, 0)), 3600)

    def test_exactly_at_hour_waits_full_day(self):
        self.assertEqual(scheduler._seconds_until_next_run(7, now=jst(7, 0, 0)), 86400)

    def test_after_hour_waits_until_tomorrow(self):
        self.assertEqual(scheduler._seconds_until_next_run(7, now=jst(8, 0, 0)), 82800)

    def test_fractional_seconds(self):
        self.assertEqual(scheduler._seconds_until_next_run(7, now=jst(6, 59, 30)), 30)


class TestRegistry(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        scheduler.stop_all()
        self.addCleanup(scheduler.stop_all)

    async def test_duplicate_name_raises(self):
        scheduler.register_interval("job", 1, _noop)
        with self.assertRaises(ValueError):
            scheduler.register_interval("job", 1, _noop)
        with self.assertRaises(ValueError):
            scheduler.register_daily("job", 7, _noop)

    async def test_is_registered(self):
        self.assertFalse(scheduler.is_registered("job"))
        scheduler.register_daily("job", 7, _noop)
        self.assertTrue(scheduler.is_registered("job"))

    async def test_start_all_is_idempotent(self):
        scheduler.register_interval("job", 999, _noop)
        scheduler.start_all()
        task1 = scheduler._jobs["job"].task
        scheduler.start_all()
        task2 = scheduler._jobs["job"].task
        self.assertIs(task1, task2, "二重起動しない")

    async def test_job_isolation_failure_does_not_stop_others(self):
        """ジョブ隔離: 落ちるジョブが他ジョブを止めない・自分自身も次周期に走り続ける"""
        counter = {"ok": 0, "bad": 0}

        async def ok_job():
            counter["ok"] += 1

        async def bad_job():
            counter["bad"] += 1
            raise RuntimeError("boom")

        # 0.03秒間隔（minutes=0.0005）で両ジョブを回す
        scheduler.register_interval("ok", 0.0005, ok_job)
        scheduler.register_interval("bad", 0.0005, bad_job)
        scheduler.start_all()
        await asyncio.sleep(0.2)

        self.assertGreaterEqual(counter["bad"], 2, "失敗ジョブ自身も周期実行を継続する")
        self.assertGreaterEqual(counter["ok"], 2, "他ジョブは失敗ジョブの影響を受けない")
        for job in scheduler._jobs.values():
            self.assertFalse(job.task.done(), "例外でループタスクが死んでいない")

    async def test_run_job_once_swallows_exception(self):
        async def bad():
            raise RuntimeError("boom")

        job = scheduler._Job(name="x", kind="interval", coro_factory=bad, minutes=1)
        await scheduler._run_job_once(job)  # 例外が伝播しないこと


async def _noop():
    pass


class TestHealthcheckSchedulerCompat(unittest.IsolatedAsyncioTestCase):
    """daily_healthcheck.start_healthcheck_scheduler の従前挙動（T0-2 移行後）"""

    def setUp(self):
        import os
        os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
        scheduler.stop_all()
        self.addCleanup(scheduler.stop_all)

    async def test_disabled_by_env(self):
        import daily_healthcheck
        with patch.dict("os.environ", {"HEALTHCHECK_DISABLED": "1"}):
            daily_healthcheck.start_healthcheck_scheduler()
        self.assertFalse(scheduler.is_registered("HEALTHCHECK"))

    async def test_registers_and_starts_daily_job(self):
        import daily_healthcheck
        with patch.dict("os.environ", {"HEALTHCHECK_DISABLED": "", "HEALTHCHECK_HOUR_JST": "7"}):
            daily_healthcheck.start_healthcheck_scheduler()
        self.assertTrue(scheduler.is_registered("HEALTHCHECK"))
        job = scheduler._jobs["HEALTHCHECK"]
        self.assertEqual(job.kind, "daily")
        self.assertEqual(job.hour_jst, 7)
        self.assertIsNotNone(job.task)

    async def test_double_startup_is_safe(self):
        """startup が2回走っても登録・タスクは1つ（従来は二重ループになり得た点の改善）"""
        import daily_healthcheck
        with patch.dict("os.environ", {"HEALTHCHECK_DISABLED": ""}):
            daily_healthcheck.start_healthcheck_scheduler()
            task1 = scheduler._jobs["HEALTHCHECK"].task
            daily_healthcheck.start_healthcheck_scheduler()
            task2 = scheduler._jobs["HEALTHCHECK"].task
        self.assertIs(task1, task2)

    def test_seconds_until_next_run_reexported(self):
        """互換 re-export（daily_healthcheck._seconds_until_next_run）が生きていること"""
        import daily_healthcheck
        self.assertIs(daily_healthcheck._seconds_until_next_run,
                      scheduler._seconds_until_next_run)


if __name__ == "__main__":
    unittest.main()
