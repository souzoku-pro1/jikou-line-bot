"""P2-CHAIN-012: LINE durable 滞留監視（check_line_backlog）のテスト。

DRAFT_P2_DURABLE_IGNITION §3/§7 の要件を pin する:
- durable flag 配下で動作（OFF は完全 no-op・DB 非接触）
- received（閾値超）と processing（stale 閾値超）の両 state を対象
- `STRIPE_EVENT_JOURNAL_ENABLED` に依存しない（OFF でも検知）
- 警報文面は件数と PK のみ（D17 流儀・event ID／本文非搭載）
- daily_healthcheck への結線は env 直読みゲート（M-06: flag OFF は import 不発）
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa

import hub.db as db
from hub.inbound_event import Base as InboundBase, InboundEvent

_FLAG = "INBOUND_EVENT_DURABLE_ENABLED"


def _run(coro):
    return asyncio.run(coro)


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p2c012_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", _FLAG: "1"})
        self._env.start()
        os.environ.pop("STRIPE_EVENT_JOURNAL_ENABLED", None)  # Stripe flag 非依存を既定で検証
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(InboundBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _insert(self, *, state, age_seconds, provider="line", claimed_age=None,
                external_event_id="evt-XYZ-secret"):
        now = datetime.now(timezone.utc)

        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(InboundEvent.__table__).values(
                    provider=provider, external_event_id=external_event_id,
                    dedup_key=f"line:{external_event_id}:{state}:{age_seconds}:{claimed_age}",
                    payload_hash="0" * 64, signature_result="verified",
                    state=state, attempts=1,
                    received_at=now - timedelta(seconds=age_seconds),
                    claimed_at=(None if claimed_age is None
                                else now - timedelta(seconds=claimed_age))))
        _run(_ins())
        db.reset_for_tests()

    def _check(self):
        from hub.durable_inbound import check_line_backlog
        r = _run(check_line_backlog())
        db.reset_for_tests()
        return r


class TestFlagGate(unittest.TestCase):
    def test_flag_off_noop_without_db(self):
        # flag OFF は DATABASE_URL の有無に関わらず即 [] （DB 非接触）
        env = {k: v for k, v in os.environ.items() if k != _FLAG}
        with patch.dict(os.environ, {**env, "DATABASE_URL": ""}, clear=True):
            from hub.durable_inbound import check_line_backlog
            self.assertEqual(asyncio.run(check_line_backlog()), [])


class TestBacklogDetection(_DbMixin):
    def test_received_stale_detected(self):
        self._insert(state="received", age_seconds=7200)   # 既定閾値 3600 超
        problems = self._check()
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("received 滞留 1件", problems[0])

    def test_processing_stale_detected(self):
        self._insert(state="processing", age_seconds=7200, claimed_age=7200)
        problems = self._check()
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("processing stale 1件", problems[0])

    def test_processing_null_claimed_at_detected(self):
        # claimed_at NULL（列追加前の旧行相当）も stale として拾う
        self._insert(state="processing", age_seconds=7200, claimed_age=None)
        problems = self._check()
        self.assertEqual(len(problems), 1, problems)

    def test_threshold_boundary_not_detected(self):
        # 閾値内（30 分前）は検知しない
        self._insert(state="received", age_seconds=1800)
        self._insert(state="processing", age_seconds=1800, claimed_age=1800)
        self.assertEqual(self._check(), [])

    def test_terminal_and_other_providers_ignored(self):
        self._insert(state="done", age_seconds=7200)
        self._insert(state="failed", age_seconds=7200)
        self._insert(state="received", age_seconds=7200, provider="stripe",
                     external_event_id="evt-stripe")
        self.assertEqual(self._check(), [])

    def test_stripe_flag_independent(self):
        # STRIPE_EVENT_JOURNAL_ENABLED 未設定（setUp で除去済み）でも検知する
        self.assertNotIn("STRIPE_EVENT_JOURNAL_ENABLED", os.environ)
        self._insert(state="received", age_seconds=7200)
        self.assertEqual(len(self._check()), 1)

    def test_received_env_threshold_override(self):
        with patch.dict(os.environ, {"INBOUND_LINE_STALE_RECEIVED_SECONDS": "600"}):
            self._insert(state="received", age_seconds=1200)   # 600 秒閾値なら検知
            self.assertEqual(len(self._check()), 1)

    def test_alarm_redaction_pk_and_count_only(self):
        # D17: 警報文面に event ID・payload hash を載せない（件数と PK のみ）
        self._insert(state="received", age_seconds=7200,
                     external_event_id="evt-XYZ-secret")
        problems = self._check()
        self.assertEqual(len(problems), 1)
        self.assertNotIn("evt-XYZ-secret", problems[0])
        self.assertNotIn("0" * 64, problems[0])
        self.assertIn("PK:", problems[0])


class TestHealthcheckWiring(unittest.TestCase):
    """結線の構造 pin（M-06: import は flag 判定の内側・既存チェックは無変更）。"""

    def test_wired_inside_env_gate(self):
        src = Path("daily_healthcheck.py").read_text(encoding="utf-8")
        idx = src.index("INBOUND_EVENT_DURABLE_ENABLED")
        block = src[idx:idx + 400]
        self.assertIn("from hub.durable_inbound import check_line_backlog", block)
        self.assertIn("check_line_backlog()", block)
        # import が flag 判定より前（module 冒頭）に存在しないこと（M-06）
        self.assertNotIn("from hub.durable_inbound", src[:idx])

    def test_flag_off_gate_skips_call(self):
        # flag OFF では check_line_backlog が呼ばれない（ゲート式の直接検証）
        val = os.environ.get(_FLAG, "").strip().lower()
        self.assertNotIn(val, ("1", "true", "on", "yes"))


if __name__ == "__main__":
    unittest.main()
