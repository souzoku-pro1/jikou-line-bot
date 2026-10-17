"""OPS-RUNBOOK-1（QUEUE-2026-10-16-R F-6 (2)）: 日次死活監視の追加点検 J〜M と既存挙動の不変

固定する仕様:
- J 送信台帳: unconfirmed が 24 時間超／started が 60 分超／直近 24 時間の失敗率 ≥ 20%（母数 ≥ 5）で警報。DB 未設定・表なしは静かにスキップ。
- K 対応待ち: hub.reply_pending が無い環境（main）はスキップ。あれば open の最古が 24 時間以上で警報。
- L 熟慮期間: App 40 未設定はスキップ。期限まで 7 日以内で未提出の件数を警報（0 件は何も出さない）。
- M 疎通: DB SELECT 1・LINE bot/info（顧客・業務の設定済みトークンだけ）。失敗は分類名のみ。
- 既存挙動の不変: 正常時は送信ゼロ／異常 1 項目で 1 通／同日 2 回目は台帳で skipped（_notify_once_per_day）。
- 呼出は監視項目F の 1 行置換のみ（run_healthcheck の行数不変・allowlist の番地 84/112/162/191/635/659 不変）。新規 sink なし。
"""

import asyncio
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}.items():
    os.environ.setdefault(_k, _v)

import daily_healthcheck as dh  # noqa: E402
from hub import db  # noqa: E402
from hub import houki_jukuryo as hj  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
UTC = timezone.utc
ADMIN = "U" + "c" * 32


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


class _Db(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="ops_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db",
                                            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz-token", "ATTORNEY_LINE_USER_ID": ADMIN})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.create_all)
        _run(_create())

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def op(self, state: str, created_ago_h: float, started_ago_h: float | None = None):
        now = datetime.now(UTC)

        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(sl.send_operation).values(
                    op_id=os.urandom(8).hex(), business="jikou", channel="jikou", actor="bot", purpose="reply", state=state,
                    attempt_no=1, owner_token="t", created_at=now - timedelta(hours=created_ago_h),
                    started_at=(now - timedelta(hours=started_ago_h)) if started_ago_h is not None else None,
                    conversation_version=0, inbound_seq=0, completed_after_human=False, first_reply_target=True,
                    text_version="v"))
        _run(_ins())


class TestSendLedgerHealth(_Db):
    def test_thresholds(self):
        self.assertEqual(_run(dh.check_send_ledger_health()), [])
        self.op("unconfirmed", 1)                                  # 24 時間未満: まだ
        self.assertEqual(_run(dh.check_send_ledger_health()), [])
        self.op("unconfirmed", 30)
        self.op("started", 2, started_ago_h=2)
        for _ in range(4):
            self.op("sent", 1)
        self.op("failed", 1)                                       # 失敗 1／5 = 20%
        out = _run(dh.check_send_ledger_health())
        self.assertEqual(len(out), 3, out)
        self.assertIn("結果未確認（unconfirmed）が 1 件", out[0])
        self.assertIn("started のまま 60 分超が 1 件", out[1])
        self.assertIn("送信失敗率 20%（失敗 1／5）", out[2])
        for p in out:
            self.assertNotIn("sqlite", p)

    def test_skip_without_db_or_table(self):
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            self.assertEqual(_run(dh.check_send_ledger_health()), [])

        async def _drop():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.drop_all)
        _run(_drop())
        self.assertEqual(_run(dh.check_send_ledger_health()), [])


class TestReplyPendingAndHouki(_Db):
    def test_reply_pending_skipped_without_module_and_counted_with_it(self):
        with patch.dict(sys.modules, {"hub.reply_pending": None}):
            self.assertEqual(_run(dh.check_reply_pending_oldest()), [])
        md = sa.MetaData()
        t = sa.Table("reply_pending", md, sa.Column("pending_id", sa.Integer, primary_key=True),
                     sa.Column("state", sa.Text), sa.Column("created_at", sa.DateTime(timezone=True)))
        fake = types.SimpleNamespace(reply_pending=t, STATE_OPEN="open")

        async def _seed():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(md.create_all)
            async with db.session_scope() as s:
                await s.execute(sa.insert(t).values(state="open", created_at=datetime.now(UTC) - timedelta(hours=30)))
                await s.execute(sa.insert(t).values(state="open", created_at=datetime.now(UTC) - timedelta(hours=1)))
                await s.execute(sa.insert(t).values(state="human_replied", created_at=datetime.now(UTC) - timedelta(hours=90)))
        _run(_seed())
        with patch.dict(sys.modules, {"hub.reply_pending": fake}):
            out = _run(dh.check_reply_pending_oldest())
        self.assertEqual(len(out), 1)
        self.assertIn("未処理 2 件・最古 約30 時間", out[0])

    def test_houki_deadline_soon(self):
        def rec(rid, start, submitted=""):
            return {"$id": {"value": rid}, "status": {"value": "受任"}, "申述提出日": {"value": submitted},
                    "起算日_確定": {"value": start}}
        today = datetime(2026, 10, 16).date()
        records = [rec("1", "2026-07-20"), rec("2", "2026-07-25"), rec("3", "2026-09-01"), rec("4", "")]
        # 期限: 1→10/19（残 3）・2→10/24（残 8・対象外）・3→11/30・4→未確定
        with patch.dict(os.environ, {"APP_HOUKI": "40", "TOKEN_HOUKI": "t"}), \
                patch.object(hj, "fetch_all_targets", AsyncMock(return_value=records)), patch.object(hj, "_today_jst", lambda: today):
            out = _run(dh.check_houki_deadline_soon())
        self.assertEqual(len(out), 1)
        self.assertIn("7 日以内で申述提出日が空の案件が 1 件", out[0])
        with patch.dict(os.environ, {"APP_HOUKI": "", "TOKEN_HOUKI": ""}):
            self.assertEqual(_run(dh.check_houki_deadline_soon()), [])


class TestConnectivity(_Db):
    def test_db_and_line(self):
        class _Resp:
            def __init__(self, code):
                self.status_code = code

        class _Client:
            codes = [200, 200]
            calls: list = []

            def __init__(self, **kw):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, url, headers=None):
                _Client.calls.append((url, headers.get("Authorization", "")))
                return _Resp(_Client.codes.pop(0))
        with patch.object(dh.httpx, "AsyncClient", _Client):
            self.assertEqual(_run(dh.check_connectivity()), [])
            self.assertEqual(len(_Client.calls), 2)
            self.assertTrue(all(u == dh._LINE_INFO_URL for u, _ in _Client.calls))
            _Client.codes = [401, 200]
            out = _run(dh.check_connectivity())
        self.assertEqual(len(out), 1)
        self.assertIn("LINE 疎通（顧客チャネル）: HTTP 401", out[0])
        self.assertNotIn("dummy_token", out[0])
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite+aiosqlite:///Z:/nonexistent/dir/x.db"}), \
                patch.object(dh.httpx, "AsyncClient", _Client):
            db.reset_for_tests()
            _Client.codes = [200, 200]
            out = _run(dh.check_connectivity())
        self.assertTrue(any(p.startswith("DB 疎通: 接続に失敗（") for p in out), out)
        self.assertFalse(any("nonexistent" in p for p in out))


class TestRunHealthcheckBehaviour(_Db):
    def _patches(self, ledger_problems):
        return [patch.object(dh, "check_models", AsyncMock(return_value=[])),
                patch.object(dh, "check_kintone_schema", AsyncMock(return_value=[])),
                patch.object(dh, "check_templates", lambda: []),
                patch("channels.soufu_annai.check_block_sync", AsyncMock(return_value=[])),
                patch.object(dh, "check_journal_backlog", AsyncMock(return_value=[])),
                patch.object(dh, "check_unknown_providers", AsyncMock(return_value=[])),
                patch.object(dh, "check_app36_confirmed_decisions", AsyncMock(return_value=[])),
                patch.object(dh, "check_business_notify_liveness", AsyncMock(return_value=[])),
                patch.object(dh, "check_send_ledger_health", AsyncMock(return_value=list(ledger_problems))),
                patch.object(dh, "check_reply_pending_oldest", AsyncMock(return_value=[])),
                patch.object(dh, "check_houki_deadline_soon", AsyncMock(return_value=[])),
                patch.object(dh, "check_connectivity", AsyncMock(return_value=[])),
                patch.object(dh, "check_next_token_residual", lambda: None)]

    def _run_with(self, ledger_problems, notify):
        ps = self._patches(ledger_problems) + [patch.object(dh, "notify_admin_line", notify)]
        for p in ps:
            p.start()
        try:
            return _run(dh.run_healthcheck())
        finally:
            for p in ps:
                p.stop()

    def test_normal_sends_nothing_one_anomaly_one_notice_same_day_skipped(self):
        notify = AsyncMock(return_value=True)
        self.assertEqual(self._run_with([], notify), [])
        self.assertEqual(notify.await_count, 0)                                 # 正常時: 送信ゼロ
        problems = self._run_with(["送信台帳: テスト異常"], notify)
        self.assertEqual(problems, ["送信台帳: テスト異常"])
        self.assertEqual(notify.await_count, 1)                                 # 異常 1 項目で 1 通
        self.assertIn("送信台帳: テスト異常", notify.await_args.args[0])
        problems = self._run_with(["送信台帳: テスト異常"], notify)
        self.assertEqual(problems, ["送信台帳: テスト異常"])
        self.assertEqual(notify.await_count, 1)                                 # 同日 2 回目は台帳で skipped
        ops = _run(self._ops())
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["channel"], ops[0]["state"]), ("healthcheck", "sent"))
        self.assertTrue(ops[0]["inbound_event_id"].startswith("healthcheck:"))

    async def _ops(self):
        async with db.session_scope() as s:
            return [dict(r._mapping) for r in (await s.execute(sa.select(sl.send_operation))).fetchall()]

    def test_item_failure_is_classified_and_others_continue(self):
        ps = self._patches([]) + [patch.object(dh, "check_connectivity", AsyncMock(side_effect=RuntimeError("postgres://u:p@h/db"))),
                                  patch.object(dh, "check_reply_pending_oldest", AsyncMock(return_value=["対応待ち: x"])),
                                  patch.object(dh, "notify_admin_line", AsyncMock(return_value=True))]
        for p in ps:
            p.start()
        try:
            problems = _run(dh.run_healthcheck())
        finally:
            for p in ps:
                p.stop()
        self.assertIn("疎通監視の実行自体が失敗: RuntimeError", problems)
        self.assertIn("対応待ち: x", problems)
        self.assertFalse(any("postgres://" in p for p in problems))


class TestPins(unittest.TestCase):
    def test_one_line_replacement_and_allowlist_addresses(self):
        src = (REPO / "daily_healthcheck.py").read_text(encoding="utf-8")
        lines = src.split("\n")
        self.assertEqual(src.count("problems += await _check_f_and_ops_runbook_items()"), 1)
        self.assertNotIn("problems += await check_business_notify_liveness()", src)
        self.assertIn('logger.error("healthcheck NG', lines[634])                   # 635 行目（allowlist 番地）不変
        self.assertIn('logger.info("healthcheck OK', lines[658])                    # 659 行目
        allow = json.loads((REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8"))
        known = {e for e in allow["entries"] if e.startswith("daily_healthcheck.py:")}
        self.assertEqual(set(scan_source(src, "daily_healthcheck.py")), known)       # 新規 sink なし（既知の番地のみ）
        self.assertEqual((dh.SEND_LEDGER_UNCONFIRMED_ALERT_HOURS, dh.SEND_LEDGER_STARTED_STUCK_MINUTES, dh.SEND_FAILURE_RATE_ALERT,
                          dh.SEND_FAILURE_RATE_MIN_TOTAL, dh.REPLY_PENDING_OPEN_ALERT_HOURS, dh.HOUKI_DEADLINE_SOON_DAYS),
                         (24, 60, 0.2, 5, 24, 7))
        self.assertIn("要大野確認", src)
        self.assertTrue((REPO / "test_daily_healthcheck_app36_audit.py").exists())


if __name__ == "__main__":
    unittest.main()
