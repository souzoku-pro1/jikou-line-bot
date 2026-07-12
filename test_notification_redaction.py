"""RV-10 P1-107a-fix3 / H02: 通知本文（notify_admin_line 等）に例外本文・vendor 応答本文が
混入しないことを固定する。例外はクラス名のみ可視・本文は載せない。"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")
_SAVED_KEY = os.environ.get("ANTHROPIC_API_KEY")

import claude_gateway  # noqa: E402
import hub.dispatch as hub_dispatch  # noqa: E402
import hub.return_deadline as hub_return_deadline  # noqa: E402

if _SAVED_KEY == "dummy_key_for_import_only":
    os.environ.pop("ANTHROPIC_API_KEY", None)

_SENTINEL = "VENDOR_SECRET_BODY_ZZZ_do_not_leak"


class TestNotificationExceptionBodyRedaction(unittest.IsolatedAsyncioTestCase):
    async def test_billing_error_notification_omits_exception_body(self):
        exc = RuntimeError(_SENTINEL)
        with patch.object(claude_gateway, "notify_admin_line",
                          new_callable=AsyncMock) as m:
            await claude_gateway._notify_billing_error("ctx", exc)
        body = m.call_args.args[0]
        self.assertNotIn(_SENTINEL, body)        # 例外本文は通知に載らない
        self.assertIn("RuntimeError", body)      # クラス名は可視

    async def test_return_deadline_fetch_error_omits_exception_body(self):
        # fetch は KintoneError を捕捉する。message に sentinel を仕込む。
        err = hub_return_deadline.kintone.KintoneError(500, "GAIA_X", _SENTINEL)
        with patch.object(hub_return_deadline.kintone, "search_records",
                          new_callable=AsyncMock, side_effect=err), \
             patch.object(hub_return_deadline.notify, "notify_admin_line",
                          new_callable=AsyncMock) as m:
            await hub_return_deadline.return_deadline_check()
        body = m.call_args.args[0]
        self.assertNotIn(_SENTINEL, body)        # 例外本文(message)は載らない
        self.assertIn("KintoneError", body)      # クラス名は可視


if __name__ == "__main__":
    unittest.main()
