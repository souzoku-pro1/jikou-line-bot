"""P1-005b: journal本番開通の前提解消のテスト（D11/D12/D13）

  D11(M02)     : kintone非2xxを業務失敗として例外化（doneに固定される経路ゼロ）。
                 flag OFF時にも適用（安全側一方向）
  D12(RCF-M06) : stale processing（claimed_at が STALE_PROCESSING_MINUTES 超過
                 or NULL）は再claimして再処理。15分以内はskipped_duplicate
  D13          : 再claimの二重処理リスクは受け入れ（未処理の闇損失を許さない）
  追加         : mark_done/failed の rowcount=0 警告・mark_failed への
                 例外本文渡し禁止のAST call-policy

テストDB: sqlite+aiosqlite（PostgreSQL固有型不使用のため全経路検証可）。
"""

import ast
import asyncio
import os
import shutil
import subprocess
import tempfile
import unittest
from datetime import timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

# ── main import 前に環境変数を差し込む（既存テストと同じ流儀） ────────────────
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "whsec_dummy",
})

import httpx  # noqa: E402
import sqlalchemy as sa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import hub.db as db  # noqa: E402
from hub.inbound_event import (  # noqa: E402
    Base,
    InboundEvent,
    _utcnow,
    mark_done,
    mark_failed,
    record_stripe_event,
    stale_processing_minutes,
)
import main  # noqa: E402

REPO = Path(__file__).parent

EVENT = {"id": "evt_hard_001", "type": "checkout.session.completed",
         "data": {"object": {"id": "cs_hard_001",
                             "customer_details": {"name": "太郎",
                                                  "email": "t@example.com"},
                             "amount_total": 44000}}}
PAYLOAD = b'{"id": "evt_hard_001"}'


def _run(coro):
    return asyncio.run(coro)


class _SqliteDbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="journal_hardening_test_")
        self._url = f"sqlite+aiosqlite:///{self._dir}/test.db"
        self._env = patch.dict(os.environ, {"DATABASE_URL": self._url})
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

    def fetch_only_row(self) -> dict:
        async def _fetch():
            async with db.session_scope() as session:
                rows = (await session.execute(
                    sa.select(InboundEvent))).scalars().all()
                assert len(rows) == 1, f"想定外の行数: {len(rows)}"
                r = rows[0]
                return {c.key: getattr(r, c.key)
                        for c in sa.inspect(InboundEvent).mapper.column_attrs}
        result = _run(_fetch())
        db.reset_for_tests()
        return result

    def age_claimed_at(self, minutes: int):
        """唯一の行の claimed_at を minutes 分だけ過去にずらす"""
        async def _age():
            async with db.session_scope() as session:
                await session.execute(
                    sa.update(InboundEvent).values(
                        claimed_at=_utcnow() - timedelta(minutes=minutes)))
        _run(_age())
        db.reset_for_tests()


class TestStaleProcessingReclaim(_SqliteDbMixin):
    """D12: processing 滞留の再claim（永久skipの穴を塞ぐ）"""

    def test_stale_processing_is_reclaimed(self):
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self.age_claimed_at(minutes=20)  # 既定15分を超過
        outcome, pk = _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self.assertEqual(outcome, "reprocess")
        self.assertIsNotNone(pk)
        row = self.fetch_only_row()
        self.assertEqual(row["attempts"], 2)
        self.assertEqual(row["state"], "processing")

    def test_recent_processing_is_skipped(self):
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self.age_claimed_at(minutes=5)  # 15分以内＝実行中とみなす
        outcome, pk = _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self.assertEqual((outcome, pk), ("skipped_duplicate", None))

    def test_null_claimed_at_is_treated_as_stale(self):
        """列追加前の行（claimed_at=NULL）は救済対象"""
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()

        async def _null():
            async with db.session_scope() as session:
                await session.execute(
                    sa.update(InboundEvent).values(claimed_at=None))
        _run(_null())
        db.reset_for_tests()
        outcome, _ = _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self.assertEqual(outcome, "reprocess")

    def test_reclaim_single_winner(self):
        """stale再claimは条件付きUPDATE＝勝者は1つ。直後の再送はskipになる"""
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        self.age_claimed_at(minutes=20)

        async def _two():
            first = await record_stripe_event(EVENT, PAYLOAD)
            second = await record_stripe_event(EVENT, PAYLOAD)
            return first, second
        (o1, _), (o2, pk2) = _run(_two())
        db.reset_for_tests()
        self.assertEqual(o1, "reprocess")           # 再claim成立（claimed_at更新）
        self.assertEqual((o2, pk2), ("skipped_duplicate", None))  # 2件目は敗者

    def test_stale_minutes_env(self):
        self.assertEqual(stale_processing_minutes(), 15)  # 既定
        with patch.dict(os.environ, {"STALE_PROCESSING_MINUTES": "3"}):
            self.assertEqual(stale_processing_minutes(), 3)
        with patch.dict(os.environ, {"STALE_PROCESSING_MINUTES": "abc"}):
            self.assertEqual(stale_processing_minutes(), 15)  # 不正値は既定へ
        with patch.dict(os.environ, {"STALE_PROCESSING_MINUTES": "0"}):
            self.assertEqual(stale_processing_minutes(), 15)  # 0/負値は既定へ

    def test_custom_stale_window_applies(self):
        with patch.dict(os.environ, {"STALE_PROCESSING_MINUTES": "1"}):
            _run(record_stripe_event(EVENT, PAYLOAD))
            db.reset_for_tests()
            self.age_claimed_at(minutes=2)
            outcome, _ = _run(record_stripe_event(EVENT, PAYLOAD))
            db.reset_for_tests()
        self.assertEqual(outcome, "reprocess")


class TestMarkRowcountWarning(_SqliteDbMixin):
    def test_mark_done_missing_row_warns(self):
        with self.assertLogs("hub.inbound_event", level="WARNING") as logs:
            _run(mark_done(99999))
        db.reset_for_tests()
        self.assertTrue(any("journal row missing" in m for m in logs.output))

    def test_mark_failed_missing_row_warns(self):
        with self.assertLogs("hub.inbound_event", level="WARNING") as logs:
            _run(mark_failed(99999, "RuntimeError"))
        db.reset_for_tests()
        self.assertTrue(any("journal row missing" in m for m in logs.output))


class _StatusClient:
    """main.httpx.AsyncClient 差し替え: 指定ステータスの kintone 応答を返す"""
    status_code = 200
    posts: list = []

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, json=None):
        _StatusClient.posts.append((url, json))
        resp = MagicMock()
        resp.status_code = _StatusClient.status_code
        if _StatusClient.status_code >= 400:
            resp.raise_for_status.side_effect = httpx.HTTPStatusError(
                f"{_StatusClient.status_code}", request=MagicMock(),
                response=resp)
        else:
            resp.raise_for_status.return_value = None
        return resp


class TestKintoneNon2xx(_SqliteDbMixin):
    """D11(M02): kintone非2xxがdoneに固定される経路ゼロ"""

    def setUp(self):
        super().setUp()
        _StatusClient.status_code = 200
        _StatusClient.posts = []
        self.client = TestClient(main.app, raise_server_exceptions=False)

    def _post(self):
        with patch.object(main.stripe.Webhook, "construct_event",
                          return_value=dict(EVENT)), \
             patch.object(main.httpx, "AsyncClient", _StatusClient):
            return self.client.post("/webhook/stripe", content=PAYLOAD,
                                    headers={"stripe-signature": "sig"})

    def test_non_2xx_marks_failed_and_returns_5xx(self):
        for code in (400, 401, 429, 500):
            with self.subTest(kintone_status=code):
                _StatusClient.status_code = code
                with patch.dict(os.environ,
                                {"STRIPE_EVENT_JOURNAL_ENABLED": "1"}):
                    r = self._post()
                self.assertEqual(r.status_code, 500)  # 成功ACKを出さない
                row = self.fetch_only_row()
                self.assertEqual(row["state"], "failed")  # done固定にならない
                self.assertEqual(row["last_error"], "HTTPStatusError")
                # 後片付け（次のsubTestのため行を消す）
                self._delete_all()

    def _delete_all(self):
        async def _d():
            async with db.session_scope() as session:
                await session.execute(sa.delete(InboundEvent))
        _run(_d())
        db.reset_for_tests()

    def test_2xx_marks_done(self):
        _StatusClient.status_code = 200
        with patch.dict(os.environ, {"STRIPE_EVENT_JOURNAL_ENABLED": "1"}):
            r = self._post()
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.fetch_only_row()["state"], "done")

    def test_flag_off_non_2xx_still_raises(self):
        """D11はflag OFF時にも適用（安全側一方向・journalには触れない）"""
        _StatusClient.status_code = 500
        env = {k: v for k, v in os.environ.items()
               if k != "STRIPE_EVENT_JOURNAL_ENABLED"}
        env.pop("DATABASE_URL", None)
        with patch.dict(os.environ, env, clear=True):
            r = self._post()
        self.assertEqual(r.status_code, 500)

    def test_flag_off_2xx_unchanged(self):
        _StatusClient.status_code = 200
        env = {k: v for k, v in os.environ.items()
               if k != "STRIPE_EVENT_JOURNAL_ENABLED"}
        env.pop("DATABASE_URL", None)
        with patch.dict(os.environ, env, clear=True):
            r = self._post()
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), {"status": "ok"})


class TestMarkFailedCallPolicy(unittest.TestCase):
    """mark_failed の第2引数に例外本文（str(e)・f-string・生の e）を渡さない。
    許可形: 文字列リテラル / …__name__ 属性（type(e).__name__ 等）"""

    def test_mark_failed_callers_pass_classification_only(self):
        out = subprocess.run(["git", "ls-files", "*.py"], capture_output=True,
                             text=True, check=True, cwd=REPO).stdout
        violations = []
        found = 0
        for line in out.splitlines():
            path = Path(line)
            if not line or path.name.startswith("test_"):
                continue
            tree = ast.parse((REPO / path).read_text(encoding="utf-8"),
                             filename=line)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                f = node.func
                name = f.id if isinstance(f, ast.Name) else \
                    f.attr if isinstance(f, ast.Attribute) else ""
                if name != "mark_failed" or len(node.args) < 2:
                    continue
                found += 1
                arg = node.args[1]
                ok = (isinstance(arg, ast.Constant)
                      and isinstance(arg.value, str)) or \
                     (isinstance(arg, ast.Attribute)
                      and arg.attr == "__name__")
                if not ok:
                    violations.append(f"{line}:{node.lineno}")
        self.assertGreaterEqual(found, 1, "mark_failed の呼び出しが見つからない")
        self.assertEqual(violations, [],
                         "mark_failed には分類のみ（リテラル or 型名 __name__）を"
                         "渡すこと（例外本文・PII禁止）")


if __name__ == "__main__":
    unittest.main()
