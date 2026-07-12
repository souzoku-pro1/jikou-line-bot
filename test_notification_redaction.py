"""RV-10 P1-107a-fix3 / H02: 通知本文（notify_admin_line 等）に例外本文・vendor 応答本文が
混入しないことを固定する。例外はクラス名のみ可視・本文は載せない。"""

import logging
import os
import unittest
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import anthropic
import httpx

os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")
_SAVED_KEY = os.environ.get("ANTHROPIC_API_KEY")

import claude_gateway  # noqa: E402
import daily_healthcheck  # noqa: E402
import hub.dispatch as hub_dispatch  # noqa: E402
import hub.notify as hub_notify  # noqa: E402
import hub.return_deadline as hub_return_deadline  # noqa: E402
from channels import soufu_annai  # noqa: E402

if _SAVED_KEY == "dummy_key_for_import_only":
    os.environ.pop("ANTHROPIC_API_KEY", None)

_SENTINEL = "VENDOR_SECRET_BODY_ZZZ_do_not_leak"


def _anthropic_error(cls, message):
    req = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    resp = httpx.Response(400, request=req)
    return cls(message, response=resp, body={"error": {"message": message}})


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


class TestHealthcheckNotificationsNoLeak(unittest.IsolatedAsyncioTestCase):
    """M01(a): 各 check を sentinel 例外で失敗させ、通常業務通知と dead-man 代替通知の
    双方に例外本文が混入しないことを固定（parameterized）。"""

    _CHECKS = [
        ("check_models", True), ("check_kintone_schema", True),
        ("check_templates", False), ("check_block_sync", True),
        ("check_journal_backlog", True), ("check_business_notify_liveness", True),
    ]

    async def _run(self, failing, is_async):
        async def araise(*a, **k):
            raise RuntimeError(_SENTINEL)

        def sraise(*a, **k):
            raise RuntimeError(_SENTINEL)

        admin, alt = {}, {}

        async def fake_admin(text, throttle_key=""):
            admin["t"] = text
            return False  # 送信失敗 → dead-man 代替を誘発

        async def fake_business(to, text):
            alt["t"] = text
            return True

        # check_block_sync は run_healthcheck 内で channels.soufu_annai から
        # ローカル import されるため、そちらを patch する。他は daily_healthcheck 上。
        def _target(name):
            return soufu_annai if name == "check_block_sync" else daily_healthcheck

        with ExitStack() as es:
            for name, isa in self._CHECKS:
                if name == failing:
                    es.enter_context(patch.object(_target(name), name,
                                                  araise if isa else sraise))
                else:
                    es.enter_context(patch.object(
                        _target(name), name,
                        AsyncMock(return_value=[]) if isa else MagicMock(return_value=[])))
            es.enter_context(patch.object(daily_healthcheck, "notify_admin_line",
                                          fake_admin))
            es.enter_context(patch("hub.notify.notify_business", fake_business))
            es.enter_context(patch.dict(os.environ,
                                        {"ATTORNEY_LINE_USER_ID": "Uatt"}))
            await daily_healthcheck.run_healthcheck()
        return admin.get("t", ""), alt.get("t", "")

    async def test_failing_checks_do_not_leak_in_admin_or_deadman(self):
        for failing, is_async in self._CHECKS:
            with self.subTest(check=failing):
                admin_body, alt_body = await self._run(failing, is_async)
                self.assertTrue(admin_body)                    # 通常通知は出る
                self.assertTrue(alt_body,                      # dead-man 代替通知が実際に生成される
                                "dead-man 代替通知が生成されること")
                self.assertNotIn(_SENTINEL, admin_body)        # 例外本文なし
                self.assertNotIn(_SENTINEL, alt_body)          # dead-man 代替にもなし


class TestClaudeBothFailNoLeak(unittest.IsolatedAsyncioTestCase):
    """M01(b): PRIMARY/FALLBACK 両系失敗に別々の sentinel を入れ、通知本文にも
    送出される ClaudeUnavailableError にも双方が現れないことを検証。"""

    async def test_both_models_fail_omits_both_bodies(self):
        sent_p, sent_f = "PRIMARYBODY_AAA", "FALLBACKBODY_BBB"
        primary = _anthropic_error(anthropic.BadRequestError,
                                   f"The model `x` is deprecated. {sent_p}")
        fallback = RuntimeError(sent_f)

        class _Msgs:
            def __init__(self):
                self._seq = [primary, fallback]

            async def create(self, **k):
                raise self._seq.pop(0)

        class _Client:
            messages = _Msgs()

        bodies = []

        async def fake_admin(text, throttle_key=""):
            bodies.append(text)
            return True

        with patch.object(claude_gateway, "notify_admin_line", fake_admin):
            with self.assertRaises(claude_gateway.ClaudeUnavailableError) as ctx:
                await claude_gateway.create_message_with_fallback(
                    _Client(), context="test", messages=[], max_tokens=8)
        twofail = bodies[-1]
        # 両系失敗通知に例外本文が無く、クラス名のみ可視
        self.assertNotIn(sent_p, twofail)
        self.assertNotIn(sent_f, twofail)
        self.assertIn("BadRequestError", twofail)
        self.assertIn("RuntimeError", twofail)
        # 送出例外（呼び出し側が str(e) を弁護士通知へ流す）にも本文が無い
        self.assertNotIn(sent_p, str(ctx.exception))
        self.assertNotIn(sent_f, str(ctx.exception))


class TestThrottleLogNoIdLeak(unittest.IsolatedAsyncioTestCase):
    """M01(c): 同一 key で2回呼ぶと throttle INFO が出るが、key 内の ID
    （record_id/user_id）が出ず、種別のみ可視であることを検証。"""

    async def test_throttle_log_shows_kind_not_id(self):
        env = {"LINE_ADMIN_USER_ID": "Uadmin",
               "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "t"}
        id_sentinel = "REC_ID_SENTINEL_777"
        key = f"prepare_deferred:{id_sentinel}"
        with patch.dict(os.environ, env), \
             patch("hub.notify.push_line_message",
                   new_callable=AsyncMock, return_value=True):
            with self.assertLogs("hub.notify", level="INFO") as cm:
                await hub_notify.notify_admin_line("x", throttle_key=key)
                await hub_notify.notify_admin_line("x", throttle_key=key)
        out = "\n".join(cm.output)
        self.assertIn("throttled", out)
        self.assertIn("prepare_deferred", out)      # 種別は可視
        self.assertNotIn(id_sentinel, out)           # ID は出さない

    async def test_unknown_throttle_prefix_shows_only_unknown_kind(self):
        env = {"LINE_ADMIN_USER_ID": "Uadmin",
               "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "t"}
        id_sentinel = "UNKNOWN_ID_SENTINEL_555"
        key = f"totally_unknown_prefix:{id_sentinel}"
        with patch.dict(os.environ, env), \
             patch("hub.notify.push_line_message",
                   new_callable=AsyncMock, return_value=True):
            with self.assertLogs("hub.notify", level="INFO") as cm:
                await hub_notify.notify_admin_line("x", throttle_key=key)
                await hub_notify.notify_admin_line("x", throttle_key=key)
        out = "\n".join(cm.output)
        self.assertIn("unknown_kind", out)               # 未知頭は unknown_kind 固定文言
        self.assertNotIn(id_sentinel, out)               # ID は出さない
        self.assertNotIn("totally_unknown_prefix", out)  # 未知頭そのものも出さない


class TestSoufuCheckFixedClassification(unittest.IsolatedAsyncioTestCase):
    """M01(d): soufu_annai の healthcheck 失敗が「固定分類＋クラス名のみ」になること。"""

    async def test_block_sync_failure_is_fixed_classification(self):
        env = {"APP_ENCLOSURE": "32", "APP_SHIPPING": "30"}
        err = soufu_annai.kintone.KintoneError(500, "GAIA_X", _SENTINEL)
        with patch.dict(os.environ, env), \
             patch.object(soufu_annai.kintone, "get_form_fields",
                          new_callable=AsyncMock, side_effect=err):
            problems = await soufu_annai.check_block_sync()
        self.assertEqual(len(problems), 1)
        self.assertNotIn(_SENTINEL, problems[0])         # 例外本文なし
        self.assertIn("KintoneError", problems[0])       # クラス名（固定分類）


if __name__ == "__main__":
    unittest.main()
