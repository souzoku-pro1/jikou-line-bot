"""HOUKI-SEND-LEDGER-1（QUEUE-2026-10-13-D D-8）: 相続放棄側の LINE 受信に touch_inbound・返信に purpose=other+接頭辞 houki_reply:

固定する仕様:
- 受け口（/webhook/souzoku-houki）のテキスト受信ごとに send_ledger.touch_inbound("souzoku-houki", userId, webhookEventId)
  （会話行が作られる・再配送は内部で無視・DB 未設定は None で会話を止めない）。
- ヒアリングの返信（reply_with_push_fallback）は送信台帳に business=souzoku-houki・purpose=other・
  inbound_event_id=houki_reply:{event_id} で記録される（event_id 無しは None）。
- 相談者から見える挙動は不変（送信回数・文面・順序は従来テストのまま＝test_houki_hearing が pin）。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from test_houki_hearing import _text_response  # noqa: E402


class _FakeResp:
    def __init__(self, status: int, text: str = ""):
        self.status_code = status
        self.is_success = 200 <= status < 300
        self.text = text


class _FakeClient:
    """hub/line_channel の httpx.AsyncClient の代替（LINE へは出ない・呼出を記録）。"""
    responses: list = []
    calls: list = []
    raise_exc: Exception | None = None

    def __init__(self, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeClient.calls.append((url, json))
        if _FakeClient.raise_exc is not None:
            raise _FakeClient.raise_exc
        if _FakeClient.responses:
            return _FakeClient.responses.pop(0)
        return _FakeResp(200)

from houki_bot import hearing, router  # noqa: E402
from hub import db, image_intake, line_channel  # noqa: E402
from hub import send_ledger as sl  # noqa: E402

UID = "U" + "d" * 32


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


class TestHoukiSendLedger(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="houki_sl_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db",
                                            "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki-token"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(sl.metadata.create_all)
        _run(_create())
        _FakeClient.responses, _FakeClient.calls, _FakeClient.raise_exc = [], [], None
        hearing.conversation_histories.pop(UID, None)
        self._p = [
            patch.object(line_channel.httpx, "AsyncClient", _FakeClient),
            patch.object(hearing, "call_hearing_model", AsyncMock(return_value=_text_response("ご返信ありがとうございます。亡くなられた方のお名前を教えてください。"))),
            patch.object(hearing, "save_to_chatlog", AsyncMock()),
            patch.object(hearing, "save_to_approval_queue", AsyncMock(return_value="q")),
            patch.object(hearing, "get_recent_chat_history", AsyncMock(return_value=[])),
            patch.object(hearing, "is_suppressed", AsyncMock(return_value=False)),
            patch.object(hearing, "autoreply_paused", lambda: False),
            patch.object(hearing.houki_case_store, "fetch_case", AsyncMock(return_value=None)),
            patch.object(image_intake, "heal_unreplied", AsyncMock(return_value=None)),
        ]
        for p in self._p:
            p.start()

    def tearDown(self):
        for p in self._p:
            p.stop()
        hearing.conversation_histories.pop(UID, None)
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _rows(self, table):
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(table))).fetchall()]
        return _run(_q())

    def test_reply_is_recorded_with_purpose_other_and_prefix(self):
        _run(hearing.handle_houki_hearing("rtok", UID, "母が亡くなりました", "ev-1"))
        self.assertEqual(len(_FakeClient.calls), 1)                        # 送信回数は従来どおり 1
        ops = self._rows(sl.send_operation)
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["business"], ops[0]["channel"], ops[0]["purpose"], ops[0]["actor"], ops[0]["state"]),
                         ("souzoku-houki", "souzoku-houki", "other", "bot", "sent"))
        self.assertEqual(ops[0]["inbound_event_id"], "houki_reply:ev-1")
        self.assertFalse(ops[0]["first_reply_target"] is True and ops[0]["purpose"] == "reply")
        # event_id 無しは受信イベント ID なし（従来どおり送る）
        _FakeClient.calls.clear()
        _run(hearing.handle_houki_hearing("rtok2", UID, "続きです"))
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertIsNone(self._rows(sl.send_operation)[-1]["inbound_event_id"])

    def test_webhook_touches_conversation_per_text_event(self):
        touch = AsyncMock(return_value={"conversation_id": 1})
        with patch.object(router, "verify_line_signature", lambda *a, **k: True), \
                patch.object(router, "houki_channel_disabled_reason", lambda: None), \
                patch.object(line_channel.send_ledger, "touch_inbound", touch), \
                patch.object(router, "handle_houki_hearing", AsyncMock()) as handler:
            from fastapi.testclient import TestClient
            import main
            client = TestClient(main.app)
            body = {"events": [{"type": "message", "replyToken": "rt", "webhookEventId": "wh-1",
                                "source": {"userId": UID}, "message": {"type": "text", "id": "m1", "text": "こんにちは"}}]}
            r = client.post("/webhook/souzoku-houki", json=body, headers={"X-Line-Signature": "x"})
            self.assertEqual(r.status_code, 200)
        touch.assert_awaited_once_with("souzoku-houki", UID, "wh-1")
        handler.assert_awaited_once()
        self.assertEqual(handler.await_args.args[3], "wh-1")                   # event_id が handler へ渡る

    def test_touch_inbound_creates_conversation_and_ignores_redelivery(self):
        _run(sl.touch_inbound("souzoku-houki", UID, "wh-9"))
        _run(sl.touch_inbound("souzoku-houki", UID, "wh-9"))
        convs = self._rows(sl.conversation)
        self.assertEqual(len(convs), 1)
        self.assertEqual(convs[0]["business"], "souzoku-houki")
        self.assertNotIn(UID, str(convs[0]))


if __name__ == "__main__":
    unittest.main()
