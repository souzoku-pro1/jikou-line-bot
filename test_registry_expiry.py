"""REGISTRY-EXPIRY-WARN-1（QUEUE-2026-10-20-J J-1）: HMAC 鍵 registry の失効前警告（監視項目J）。

契約の pin:
  - 秘密値は戻り値・問題文に一切載らない（key_id・status・残日数のみ）
  - 「高」= 有効鍵 0 本 / 全有効鍵が N 日以内 → 毎日（曜日に依らず problems に載る）
  - 「中」= N 日以内の鍵があるが余裕鍵も残る → JST 月曜のみ
  - 未設定は署名基盤を使う設定（dual-accept ON / 署名必須 path）のときだけ「高」
  - JSON/構造不正は「高」
  - REGISTRY_EXPIRY_WARN_DAYS: 未設定・不正・0 以下は既定 14
  - run_healthcheck 統合: 問題文が通知本文に載る／監視の例外は分類名のみで報告
"""

import asyncio
import datetime
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import daily_healthcheck  # noqa: E402
from hub import registry_expiry as rx  # noqa: E402

SECRET = "ab" * 32            # 64 hex（テスト用・実鍵ではない）
NOW = datetime.datetime(2026, 10, 21, 3, 0, tzinfo=datetime.timezone.utc)   # JST 水曜 12:00
MONDAY = datetime.datetime(2026, 10, 19, 3, 0, tzinfo=datetime.timezone.utc)  # JST 月曜


def _entry(days_from_now: int, status: str = "active", now: datetime.datetime = NOW) -> dict:
    return {"secret": SECRET, "caller": "c", "allowed_methods": ["POST"],
            "allowed_paths": ["/koseki/ingest"], "status": status, "not_before": 0,
            "expires_at": int(now.timestamp()) + days_from_now * 86400 + 3600}


def _reg(**kids) -> str:
    return json.dumps(kids)


class _Env(unittest.TestCase):
    def env(self, **kw):
        base = {"SERVICE_HMAC_KEY_REGISTRY": "", "SERVICE_AUTH_DUAL_ACCEPT_ENABLED": "",
                "SERVICE_AUTH_SIGNED_REQUIRED_PATHS": "", "REGISTRY_EXPIRY_WARN_DAYS": ""}
        base.update(kw)
        p = patch.dict(os.environ, base)
        p.start()
        self.addCleanup(p.stop)


class TestWarnDays(_Env):
    def test_default_and_invalid(self):
        for raw in ("", "abc", "0", "-3"):
            with self.subTest(raw=raw):
                self.env(REGISTRY_EXPIRY_WARN_DAYS=raw)
                self.assertEqual(rx.warn_days(), 14)

    def test_custom(self):
        self.env(REGISTRY_EXPIRY_WARN_DAYS="30")
        self.assertEqual(rx.warn_days(), 30)


class TestInspect(_Env):
    def test_unconfigured(self):
        self.env()
        rep = rx.inspect(NOW)
        self.assertEqual(rep.state, rx.STATE_UNCONFIGURED)
        self.assertFalse(rep.registry_in_use)
        self.env(SERVICE_AUTH_DUAL_ACCEPT_ENABLED="1")
        self.assertTrue(rx.inspect(NOW).registry_in_use)

    def test_invalid(self):
        for raw in ("{not json", "[]", json.dumps({"k": {"caller": "c"}})):
            with self.subTest(raw=raw[:8]):
                self.env(SERVICE_HMAC_KEY_REGISTRY=raw)
                self.assertEqual(rx.inspect(NOW).state, rx.STATE_INVALID)

    def test_ok_and_days(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(60), b=_entry(400)))
        rep = rx.inspect(NOW)
        self.assertEqual(rep.state, rx.STATE_OK)
        self.assertEqual([(k.key_id, k.days_left, k.effective) for k in rep.keys],
                         [("a", 60, True), ("b", 400, True)])

    def test_warn_all_when_all_expired_or_near(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(-3), b=_entry(-20)))
        rep = rx.inspect(NOW)
        self.assertEqual(rep.state, rx.STATE_WARN_ALL)
        self.assertEqual([k.days_left for k in rep.keys], [-20, -3])
        self.assertFalse(any(k.effective for k in rep.keys))
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(5), b=_entry(14)))
        self.assertEqual(rx.inspect(NOW).state, rx.STATE_WARN_ALL)   # 14 日ちょうども「以内」

    def test_revoked_does_not_count_as_roomy(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(5), b=_entry(400, status="revoked")))
        rep = rx.inspect(NOW)
        self.assertEqual(rep.state, rx.STATE_WARN_ALL)
        self.assertEqual([(k.key_id, k.effective) for k in rep.keys], [("a", True), ("b", False)])

    def test_warn_some(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(5), b=_entry(400)))
        self.assertEqual(rx.inspect(NOW).state, rx.STATE_WARN_SOME)

    def test_report_carries_no_secret(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(5)))
        self.assertNotIn(SECRET, repr(rx.inspect(NOW)))


class TestHealthcheckProblems(_Env):
    def test_unconfigured_only_when_in_use(self):
        self.env()
        self.assertEqual(rx.healthcheck_problems(NOW), [])
        self.env(SERVICE_AUTH_DUAL_ACCEPT_ENABLED="1")
        p = rx.healthcheck_problems(NOW)
        self.assertEqual(len(p), 1)
        self.assertIn("【高】", p[0])
        self.assertIn("未設定", p[0])

    def test_invalid_is_high(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY="{broken")
        p = rx.healthcheck_problems(NOW)
        self.assertEqual(len(p), 1)
        self.assertIn("【高】", p[0])
        self.assertIn("不正", p[0])

    def test_all_near_is_high_every_day(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(**{"gas-ingest-2026-07a": _entry(3),
                                                   "pc-watcher-2026-08a": _entry(-2)}))
        for now in (NOW, MONDAY):
            with self.subTest(now=now.isoformat()):
                p = rx.healthcheck_problems(now)
                self.assertEqual(len(p), 1)
                self.assertIn("【高】", p[0])
                self.assertIn("gas-ingest-2026-07a 残3日", p[0])
                self.assertIn("pc-watcher-2026-08a 失効済み", p[0])
                self.assertNotIn(SECRET, p[0])

    def test_some_near_is_mid_on_monday_only(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(5), b=_entry(400)))
        self.assertEqual(rx.healthcheck_problems(NOW), [])          # 水曜: 載せない
        p = rx.healthcheck_problems(MONDAY)
        self.assertEqual(len(p), 1)
        self.assertIn("【中】", p[0])
        self.assertIn("a 残5日", p[0])
        self.assertNotIn("b ", p[0])

    def test_ok_is_silent(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(60), b=_entry(400)))
        self.assertEqual(rx.healthcheck_problems(NOW), [])
        self.assertEqual(rx.healthcheck_problems(MONDAY), [])

    def test_threshold_env(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(20)), REGISTRY_EXPIRY_WARN_DAYS="30")
        p = rx.healthcheck_problems(NOW)
        self.assertEqual(len(p), 1)
        self.assertIn("30日以内", p[0])


class TestRunHealthcheckIntegration(_Env):
    """監視項目J が run_healthcheck の problems → 通知本文に載る。他項目は空に固定。"""

    def _quiet_others(self):
        for name in ("check_models", "check_kintone_schema", "check_journal_backlog",
                     "check_unknown_providers", "check_app36_confirmed_decisions",
                     "check_business_notify_liveness"):
            p = patch.object(daily_healthcheck, name, AsyncMock(return_value=[]))
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(daily_healthcheck, "check_templates", lambda: [])
        p.start()
        self.addCleanup(p.stop)
        p = patch("channels.soufu_annai.check_block_sync", AsyncMock(return_value=[]))
        p.start()
        self.addCleanup(p.stop)
        self.notify = AsyncMock(return_value=True)
        p = patch.object(daily_healthcheck, "notify_admin_line", self.notify)
        p.start()
        self.addCleanup(p.stop)

    def test_high_problem_reaches_notification(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(-1)), INBOUND_EVENT_DURABLE_ENABLED="")
        self._quiet_others()
        problems = asyncio.run(daily_healthcheck.run_healthcheck())
        self.assertEqual(len(problems), 1)
        self.assertIn("【高】HMAC鍵registry", problems[0])
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[0]
        self.assertIn("【日次死活監視: 異常検知】", text)
        self.assertIn("a 失効済み", text)
        self.assertNotIn(SECRET, text)

    def test_ok_registry_adds_nothing(self):
        self.env(SERVICE_HMAC_KEY_REGISTRY=_reg(a=_entry(300)), INBOUND_EVENT_DURABLE_ENABLED="")
        self._quiet_others()
        self.assertEqual(asyncio.run(daily_healthcheck.run_healthcheck()), [])
        self.notify.assert_not_awaited()

    def test_check_failure_is_classified_only(self):
        self.env(INBOUND_EVENT_DURABLE_ENABLED="")
        self._quiet_others()
        with patch("hub.registry_expiry.healthcheck_problems",
                   side_effect=RuntimeError("secret-looking detail")):
            problems = asyncio.run(daily_healthcheck.run_healthcheck())
        self.assertEqual(problems, ["HMAC鍵registry監視の実行自体が失敗: RuntimeError"])
        self.assertNotIn("secret-looking", self.notify.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
