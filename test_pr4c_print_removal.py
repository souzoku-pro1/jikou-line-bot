"""RV-10 PR-4c: 凍結 print 2件の回収（移送先ロガー出力・二重出力の否定）。

- daily_healthcheck の OK ログは logger.info に一本化（従来の print による二重出力なし）。
- hub/scheduler の登録ログは app ロガーへ移送（print なし）。値は emit 契約経由。
"""

import asyncio
import io
import os
import sys
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

# daily_healthcheck は import 時に ANTHROPIC_API_KEY を要求。dummy 投入→import→復元。
_SAVED_ANTHROPIC = os.environ.get("ANTHROPIC_API_KEY")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")

import daily_healthcheck  # noqa: E402
import hub.scheduler as scheduler  # noqa: E402
from channels import soufu_annai  # noqa: E402

if _SAVED_ANTHROPIC is None:
    os.environ.pop("ANTHROPIC_API_KEY", None)
else:
    os.environ["ANTHROPIC_API_KEY"] = _SAVED_ANTHROPIC


class TestHealthcheckOkSinglePath(unittest.IsolatedAsyncioTestCase):
    async def test_ok_logs_via_logger_and_not_printed_twice(self):
        checks = [("check_models", True), ("check_kintone_schema", True),
                  ("check_templates", False), ("check_journal_backlog", True),
                  ("check_business_notify_liveness", True)]
        buf = io.StringIO()
        with ExitStack() as es:
            for name, isa in checks:
                es.enter_context(patch.object(
                    daily_healthcheck, name,
                    AsyncMock(return_value=[]) if isa else MagicMock(return_value=[])))
            es.enter_context(patch.object(soufu_annai, "check_block_sync",
                                          new_callable=AsyncMock, return_value=[]))
            es.enter_context(patch.object(sys, "stdout", buf))
            with self.assertLogs("daily_healthcheck", level="INFO") as cm:
                problems = await daily_healthcheck.run_healthcheck()
        self.assertEqual(problems, [])
        ok_logs = [l for l in cm.output if "healthcheck OK" in l]
        self.assertEqual(len(ok_logs), 1)                 # logger で1回のみ
        self.assertNotIn("healthcheck OK", buf.getvalue())  # print による二重出力なし


class TestSchedulerRegistrationSinglePath(unittest.IsolatedAsyncioTestCase):
    async def test_registration_logs_via_logger_and_not_printed(self):
        scheduler.stop_all()

        async def _noop():
            return None

        buf = io.StringIO()
        with patch.object(sys, "stdout", buf):
            with self.assertLogs("hub.scheduler", level="INFO") as cm:
                scheduler.register_daily("HEALTHCHECK", 7, _noop)
                scheduler.start_all()
                await asyncio.sleep(0.05)   # _job_loop 初回反復で登録ログを出す
        scheduler.stop_all()
        reg = [l for l in cm.output if "scheduler registered" in l]
        self.assertEqual(len(reg), 1)                          # logger で1回のみ
        self.assertIn("[HEALTHCHECK] scheduler registered", "\n".join(cm.output))
        self.assertNotIn("scheduler registered", buf.getvalue())  # print による出力なし


if __name__ == "__main__":
    unittest.main()
