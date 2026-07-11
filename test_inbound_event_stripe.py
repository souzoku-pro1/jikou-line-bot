"""P1-005a: Stripe InboundEvent journal（durable dedup）のテスト

固定する設計判断:
  D7 : DB到達不能時は成功ACKを返さない（5xx→Stripe自動リトライ。memory fallback禁止）
  D8 : raw payload・PII をカラムに保存しない
  D9 : 同一 evt_id の再送は skipped_duplicate（業務処理は1回だけ）。
       failed の再送は reprocess（claimして再実行）
  D10: STRIPE_EVENT_JOURNAL_ENABLED 既定OFF＝完全に従来挙動

テストDB: sqlite+aiosqlite（ファイルベース・requirements-dev.txt）。
JSONB等のPostgreSQL固有型は不使用のため sqlite で全経路を検証できる。
注意: async エンジンはイベントループに紐づくため、ループを跨ぐ前に
reset_for_tests() でエンジンを破棄している（TestClient のループと分離）。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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

import sqlalchemy as sa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import hub.db as db  # noqa: E402
from hub.inbound_event import (  # noqa: E402
    Base,
    InboundEvent,
    mark_done,
    mark_failed,
    record_stripe_event,
    stripe_dedup_key,
)
import main  # noqa: E402

EVENT = {"id": "evt_test_001", "type": "checkout.session.completed",
         "data": {"object": {
             "id": "cs_test_001",
             "customer_details": {"name": "テスト太郎",
                                  "email": "taro@example.com"},
             "amount_total": 44000}}}
PAYLOAD = b'{"id": "evt_test_001", "email": "taro@example.com"}'


def _run(coro):
    return asyncio.run(coro)


class _SqliteDbMixin(unittest.TestCase):
    """テストごとに独立した sqlite ファイルDBを用意する"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="inbound_event_test_")
        self._url = f"sqlite+aiosqlite:///{self._dir}/test.db"
        self._env = patch.dict(os.environ, {"DATABASE_URL": self._url})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            engine = db.get_async_engine()
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        _run(_create())
        db.reset_for_tests()  # スキーマ作成に使ったループからエンジンを切り離す

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def fetch_rows(self) -> list[dict]:
        async def _fetch():
            async with db.session_scope() as session:
                rows = (await session.execute(sa.select(InboundEvent))).scalars()
                return [{c.key: getattr(r, c.key)
                         for c in sa.inspect(InboundEvent).mapper.column_attrs}
                        for r in rows]
        result = _run(_fetch())
        db.reset_for_tests()
        return result


class TestRecordStripeEvent(_SqliteDbMixin):
    def test_new_then_duplicate(self):
        """同一 evt_id の2回目: 実行中(processing)への重複は in_progress
        （P1-005c・D14で skipped_duplicate から変更＝503側へ倒す）"""
        async def _flow():
            first = await record_stripe_event(EVENT, PAYLOAD)
            second = await record_stripe_event(EVENT, PAYLOAD)
            return first, second
        (o1, pk1), (o2, pk2) = _run(_flow())
        db.reset_for_tests()
        self.assertEqual(o1, "new")
        self.assertIsNotNone(pk1)
        self.assertEqual((o2, pk2), ("in_progress", None))
        rows = self.fetch_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attempts"], 2)
        self.assertEqual(rows[0]["state"], "processing")

    def test_done_then_duplicate_is_skipped(self):
        async def _flow():
            _, pk = await record_stripe_event(EVENT, PAYLOAD)
            await mark_done(pk)
            return await record_stripe_event(EVENT, PAYLOAD)
        outcome, pk = _run(_flow())
        db.reset_for_tests()
        self.assertEqual((outcome, pk), ("skipped_duplicate", None))
        rows = self.fetch_rows()
        self.assertEqual(rows[0]["state"], "done")  # done は上書きしない

    def test_failed_then_redelivery_is_reprocessed(self):
        """D9: failed の再送は claim して再実行（D7 の 5xx リトライと対）"""
        async def _flow():
            _, pk = await record_stripe_event(EVENT, PAYLOAD)
            await mark_failed(pk, "RuntimeError")
            return pk, await record_stripe_event(EVENT, PAYLOAD)
        pk, (outcome, pk2) = _run(_flow())
        db.reset_for_tests()
        self.assertEqual(outcome, "reprocess")
        self.assertEqual(pk, pk2)
        rows = self.fetch_rows()
        self.assertEqual(rows[0]["state"], "processing")
        self.assertEqual(rows[0]["attempts"], 2)

    def test_no_pii_stored(self):
        """D8: 顧客名・メール等が journal のどのカラムにも入らない"""
        _run(record_stripe_event(EVENT, PAYLOAD))
        db.reset_for_tests()
        rows = self.fetch_rows()
        self.assertEqual(len(rows), 1)
        joined = " ".join(str(v) for v in rows[0].values())
        self.assertNotIn("taro@example.com", joined)
        self.assertNotIn("テスト太郎", joined)
        # カラム集合そのものも D8 で固定（PII用のカラムが増えたら検知）
        self.assertEqual(set(rows[0].keys()),
                         {"id", "provider", "external_event_id", "caller_id",
                          "dedup_key", "payload_hash", "event_type",
                          "signature_result", "received_at", "state",
                          "processed_at", "attempts", "last_error",
                          "claimed_at"})  # claimed_at は P1-005b（D12）で追加

    def test_dedup_key_without_event_id_falls_back_to_hash(self):
        key = stripe_dedup_key({"type": "x"}, b"body")
        self.assertTrue(key.startswith("stripe:sha256:"))

    def test_mark_failed_stores_class_only(self):
        async def _flow():
            _, pk = await record_stripe_event(EVENT, PAYLOAD)
            await mark_failed(pk, "OperationalError")
        _run(_flow())
        db.reset_for_tests()
        rows = self.fetch_rows()
        self.assertEqual(rows[0]["last_error"], "OperationalError")


class _FakeAsyncClient:
    """main.httpx.AsyncClient の差し替え（kintone POST/GET を記録するだけ）"""
    posts: list = []
    fail_next: bool = False

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, json=None):
        if _FakeAsyncClient.fail_next:
            _FakeAsyncClient.fail_next = False
            raise RuntimeError("kintone down (simulated)")
        _FakeAsyncClient.posts.append((url, json))
        resp = MagicMock(status_code=200)
        resp.raise_for_status.return_value = None
        return resp

    async def get(self, url, headers=None, params=None):
        # D15 reconciliation（P1-005c）: 既存レコードなしを返す
        resp = MagicMock(status_code=200)
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"records": []}
        return resp


class TestStripeWebhookHandler(_SqliteDbMixin):
    """ハンドラ結線（D9/D10/D7）。construct_event と kintone POST はモック"""

    def setUp(self):
        super().setUp()
        _FakeAsyncClient.posts = []
        _FakeAsyncClient.fail_next = False
        self.client = TestClient(main.app, raise_server_exceptions=False)

    def _post(self):
        with patch.object(main.stripe.Webhook, "construct_event",
                          return_value=dict(EVENT)), \
             patch.object(main.httpx, "AsyncClient", _FakeAsyncClient):
            return self.client.post("/webhook/stripe", content=PAYLOAD,
                                    headers={"stripe-signature": "sig"})

    def test_flag_on_duplicate_processes_business_once(self):
        with patch.dict(os.environ, {"STRIPE_EVENT_JOURNAL_ENABLED": "1"}):
            r1 = self._post()
            r2 = self._post()
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.json().get("journal"), "skipped_duplicate")
        self.assertEqual(len(_FakeAsyncClient.posts), 1)  # 業務処理は1回だけ
        rows = self.fetch_rows()
        self.assertEqual(rows[0]["state"], "done")
        self.assertEqual(rows[0]["attempts"], 2)

    def test_flag_on_business_failure_marks_failed_then_reprocess(self):
        with patch.dict(os.environ, {"STRIPE_EVENT_JOURNAL_ENABLED": "1"}):
            _FakeAsyncClient.fail_next = True
            r1 = self._post()
            self.assertEqual(r1.status_code, 500)  # 成功ACKを出さない
            rows = self.fetch_rows()
            self.assertEqual(rows[0]["state"], "failed")
            self.assertEqual(rows[0]["last_error"], "RuntimeError")
            r2 = self._post()  # Stripe再送を模擬 → reprocess
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(len(_FakeAsyncClient.posts), 1)
        rows = self.fetch_rows()
        self.assertEqual(rows[0]["state"], "done")

    def test_flag_off_is_exactly_current_behavior(self):
        """D10: OFF時は journal に触れず、従来どおり毎回処理される"""
        env = {k: v for k, v in os.environ.items()
               if k != "STRIPE_EVENT_JOURNAL_ENABLED"}
        env.pop("DATABASE_URL", None)  # OFF時はDBが無くても動くことも同時に検証
        with patch.dict(os.environ, env, clear=True):
            r1 = self._post()
            r2 = self._post()
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(len(_FakeAsyncClient.posts), 2)  # 従来挙動＝2回処理
        self.assertNotIn("journal", r1.json())

    def test_flag_on_db_unreachable_returns_5xx(self):
        """D7: DB不達なら成功ACKを返さず5xx（業務処理も走らない）"""
        with patch.dict(os.environ, {
                "STRIPE_EVENT_JOURNAL_ENABLED": "1",
                "DATABASE_URL":
                    "postgresql://u:p@127.0.0.1:1/db?connect_timeout=1"}):
            db.reset_for_tests()
            r = self._post()
        db.reset_for_tests()
        self.assertEqual(r.status_code, 500)
        self.assertEqual(_FakeAsyncClient.posts, [])


class TestShutdownContract(unittest.TestCase):
    """P1-004申し送り①: shutdown経路は await adispose_all() を使う"""

    def test_shutdown_awaits_adispose_all(self):
        with patch("hub.db.adispose_all", new_callable=AsyncMock) as spy:
            asyncio.run(main._on_shutdown())
        spy.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
