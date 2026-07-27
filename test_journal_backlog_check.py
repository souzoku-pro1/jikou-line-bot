"""P1-005d: 日次死活監視の journal 滞留チェック（監視項目E）のテスト

固定する仕様:
  - state=processing で claimed_at 24時間超過（NULL含む）→ 警報1行
  - state=failed で received_at 24時間超過 → 警報1行
  - 閾値内・行なし → 問題なし
  - flag OFF / DATABASE_URL 未設定 / テーブル不在 → 静かにスキップ（[]）
  - 警報文面に event ID・dedup_key を含めない（件数とPKのみ・D17流儀）
  - 既存監視項目A〜Dのコードには触れない（本テストはEのみを対象）
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import timedelta
from unittest.mock import patch

os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "HEALTHCHECK_DISABLED": "1",
})

import sqlalchemy as sa  # noqa: E402

import hub.db as db  # noqa: E402
from daily_healthcheck import (  # noqa: E402
    check_journal_backlog,
    check_unknown_providers,
)
from hub.inbound_event import (  # noqa: E402
    Base,
    InboundEvent,
    _utcnow,
    record_stripe_event,
)

EVENT = {"id": "evt_backlog_001", "type": "checkout.session.completed"}
PAYLOAD = b'{"id": "evt_backlog_001"}'


def _run(coro):
    return asyncio.run(coro)


class _SqliteDbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="backlog_check_test_")
        self._url = f"sqlite+aiosqlite:///{self._dir}/test.db"
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": self._url,
            "STRIPE_EVENT_JOURNAL_ENABLED": "1",
        })
        self._env.start()
        db.reset_for_tests()

        async def _create():
            engine = db.get_async_engine()
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _set(self, **values):
        async def _u():
            async with db.session_scope() as session:
                await session.execute(sa.update(InboundEvent).values(**values))
        _run(_u())
        db.reset_for_tests()


class TestBacklogDetection(_SqliteDbMixin):
    def test_no_rows_is_ok(self):
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(problems, [])

    def test_recent_processing_is_ok(self):
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self._set(claimed_at=_utcnow() - timedelta(hours=1))
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(problems, [])

    def test_stuck_processing_over_24h_is_reported(self):
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self._set(claimed_at=_utcnow() - timedelta(hours=25))
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(len(problems), 1)
        self.assertIn("processing", problems[0])
        self.assertIn("1件", problems[0])
        self.assertIn("PK=", problems[0])

    def test_null_claimed_at_processing_is_reported(self):
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self._set(claimed_at=None)
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(len(problems), 1)

    def test_old_failed_is_reported_recent_failed_is_not(self):
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        # 25時間前に受信して failed のまま
        self._set(state="failed", claimed_at=_utcnow(),
                  received_at=_utcnow() - timedelta(hours=25))
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(len(problems), 1)
        self.assertIn("failed", problems[0])
        # 1時間前受信の failed はまだ警報しない（Stripe再送の自己回復待ち）
        self._set(received_at=_utcnow() - timedelta(hours=1))
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(problems, [])

    def test_alert_text_has_no_identifiers(self):
        """警報文面に event ID / dedup_key を含めない（件数とPKのみ）"""
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self._set(claimed_at=_utcnow() - timedelta(hours=25))
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        joined = " ".join(problems)
        self.assertNotIn("evt_", joined)
        self.assertNotIn("stripe:", joined)


class TestBacklogSkipConditions(unittest.TestCase):
    def test_flag_off_skips(self):
        env = {k: v for k, v in os.environ.items()
               if k != "STRIPE_EVENT_JOURNAL_ENABLED"}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(_run(check_journal_backlog()), [])

    def test_no_database_url_skips(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        env["STRIPE_EVENT_JOURNAL_ENABLED"] = "1"
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(_run(check_journal_backlog()), [])

    def test_missing_table_skips_quietly(self):
        """migration未適用（テーブル不在）のDBでは静かにスキップ"""
        tmp = tempfile.mkdtemp(prefix="backlog_no_table_")
        try:
            with patch.dict(os.environ, {
                    "DATABASE_URL": f"sqlite+aiosqlite:///{tmp}/empty.db",
                    "STRIPE_EVENT_JOURNAL_ENABLED": "1"}):
                db.reset_for_tests()
                self.assertEqual(_run(check_journal_backlog()), [])
        finally:
            db.reset_for_tests()
            shutil.rmtree(tmp, ignore_errors=True)


# ── RMC-M01（裁定）追加テスト: E系は provider='stripe' 限定 ──────────────────
class TestLineRowsNotCountedByE(_SqliteDbMixin):
    """LINE 行は G系（hub/durable_inbound.check_line_backlog）専任であり、
    24h 超で滞留していても E系（check_journal_backlog）には計上されない。"""

    def _insert_line_row(self, state: str):
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(InboundEvent).values(
                    provider="line", dedup_key=f"line:rmc-m01-{state}",
                    payload_hash="x" * 8, signature_result="valid",
                    state=state,
                    received_at=_utcnow() - timedelta(hours=30),
                    claimed_at=_utcnow() - timedelta(hours=30)))
        _run(_ins())
        db.reset_for_tests()

    def _set_stripe_only(self, **values):
        async def _u():
            async with db.session_scope() as session:
                await session.execute(
                    sa.update(InboundEvent)
                    .where(InboundEvent.provider == "stripe").values(**values))
        _run(_u())
        db.reset_for_tests()

    def test_line_rows_not_reported_by_journal_backlog(self):
        # processing 30h 超・failed 30h 超の LINE 行を置いても E系は無反応
        self._insert_line_row("processing")
        self._insert_line_row("failed")
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(problems, [], "LINE 行が E系に計上された（RMC-M01 違反）")
        # 対照: 同条件の stripe 行は従来どおり計上される
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self._set_stripe_only(claimed_at=_utcnow() - timedelta(hours=25))
        problems = _run(check_journal_backlog())
        db.reset_for_tests()
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("processing", problems[0])


# ── MAIN-CONS-fix2 M01（裁定済み）: 監視項目H＝未知 provider 検知 ────────────
class TestUnknownProviderDetection(_SqliteDbMixin):
    """既知集合 {stripe, line, kintone} 以外の provider 行を警報する。
    本文は provider 名と件数のみ（redaction 規律維持）。"""

    def _insert(self, provider: str, n: int = 1):
        async def _ins():
            async with db.session_scope() as s:
                for i in range(n):
                    await s.execute(sa.insert(InboundEvent).values(
                        provider=provider, dedup_key=f"{provider}:h-{i}",
                        payload_hash="x" * 8, signature_result="valid",
                        state="received", received_at=_utcnow()))
        _run(_ins())
        db.reset_for_tests()

    def test_unknown_provider_reported_with_name_and_count_only(self):
        self._insert("ghost", 2)      # 未知 provider fixture
        self._insert("line", 1)       # 既知（G系専任）は対象外
        problems = _run(check_unknown_providers())
        db.reset_for_tests()
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("ghost", problems[0])
        self.assertIn("2件", problems[0])
        self.assertNotIn("line", problems[0])       # 既知 provider を混ぜない
        self.assertNotIn("h-", problems[0])         # dedup_key/ID を載せない

    def test_known_providers_only_is_silent(self):
        for p in ("stripe", "line", "kintone"):
            self._insert(p, 1)
        problems = _run(check_unknown_providers())
        db.reset_for_tests()
        self.assertEqual(problems, [])

    def test_no_database_url_skips(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            self.assertEqual(_run(check_unknown_providers()), [])

    def test_missing_table_skips_quietly(self):
        tmp = tempfile.mkdtemp(prefix="unknown_provider_no_table_")
        try:
            with patch.dict(os.environ, {
                    "DATABASE_URL": f"sqlite+aiosqlite:///{tmp}/empty.db"}):
                db.reset_for_tests()
                self.assertEqual(_run(check_unknown_providers()), [])
        finally:
            db.reset_for_tests()
            shutil.rmtree(tmp, ignore_errors=True)


# ── MC3-L01: 監視項目H の run_healthcheck 結線（統合テスト） ─────────────────
class TestUnknownProviderWiring(unittest.IsolatedAsyncioTestCase):
    """H の problems が run_healthcheck() の最終 problems へ**ちょうど1回だけ**合流する。"""

    async def test_h_problems_merged_exactly_once(self):
        from contextlib import ExitStack
        from unittest.mock import AsyncMock, MagicMock

        import daily_healthcheck as hc
        from channels import soufu_annai
        sentinel = ("未知provider検知: ghost 9件"
                    "（provider 別滞留監視のいずれにも載らないため要確認）")
        checks = [("check_models", True), ("check_kintone_schema", True),
                  ("check_templates", False), ("check_journal_backlog", True),
                  ("check_business_notify_liveness", True)]
        with ExitStack() as es:
            for name, is_async in checks:
                es.enter_context(patch.object(
                    hc, name,
                    AsyncMock(return_value=[]) if is_async
                    else MagicMock(return_value=[])))
            es.enter_context(patch.object(soufu_annai, "check_block_sync",
                                          new_callable=AsyncMock, return_value=[]))
            es.enter_context(patch.object(hc, "check_unknown_providers",
                                          AsyncMock(return_value=[sentinel])))
            notify = es.enter_context(patch.object(hc, "notify_admin_line",
                                                   AsyncMock(return_value=True)))
            env = {k: v for k, v in os.environ.items()
                   if k not in ("INBOUND_EVENT_DURABLE_ENABLED",
                                "KINTONE_WEBHOOK_TOKEN_NEXT")}
            with patch.dict(os.environ, env, clear=True):
                problems = await hc.run_healthcheck()
        self.assertEqual(problems.count(sentinel), 1, problems)   # ちょうど1回
        self.assertEqual(len(problems), 1, problems)              # 他検査の混入なし
        notify.assert_awaited()   # 異常1件として通知経路にも乗る


if __name__ == "__main__":
    unittest.main()
