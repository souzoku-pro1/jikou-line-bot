"""D1: /webhook/dispatch-bot（LINE指示Bot入口）のテスト

- 署名検証（正/不正/secret未設定=deny-all）
- ホワイトリスト（許可→固定応答1回／拒否→沈黙＋管理者警報1回／env空=全拒否）
- 即200＋BackgroundTasks（TestClient はレスポンス後にバックグラウンドを同期実行）
- 顧客Bot（/webhook）の挙動不変・chat_responder 非依存
"""

import base64
import hashlib
import hmac
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

# main import 前に環境変数を差し込む（test_hub_dispatch と同じ流儀）
_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
os.environ.update({
    "LINE_CHANNEL_SECRET": "customer_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "customer_token",
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
    "APP_SHIPPING": "30",
    "TOKEN_SHIPPING": "dummy",
    "HUB_WEBHOOK_TOKEN": "hub_token",
    "ATTORNEY_LINE_USER_ID": "U_attorney",
    "HEALTHCHECK_DISABLED": "1",
    # 指示Bot（テスト既定値。deny-all テストでは patch.dict で消す）
    "DISPATCHBOT_CHANNEL_SECRET": "bot_secret",
    "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "bot_token",
    "DISPATCHBOT_ALLOWED_USER_IDS": "U_owner1, U_owner2",
})

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]  # skip ガードの誤解除防止（test_hub_dispatch 参照）

from dispatch_bot import router as bot  # noqa: E402

client = TestClient(main.app)
URL = "/webhook/dispatch-bot"


def _sign(body: bytes, secret: str = "bot_secret") -> str:
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def _event_body(user_id="U_owner1", text="テスト", event_type="message"):
    event = {"type": event_type, "replyToken": "rt1",
             "source": {"userId": user_id}}
    if event_type == "message":
        event["message"] = {"type": "text", "text": text}
    return json.dumps({"events": [event]}).encode()


def _post(body: bytes, signature: str | None = None, reply=None, alert=None):
    """署名付きPOST。reply/alert をモックし (response, reply_mock, alert_mock) を返す"""
    reply = reply or AsyncMock()
    alert = alert or AsyncMock()
    headers = {"X-Line-Signature": signature if signature is not None else _sign(body)}
    with patch.object(bot, "_send_reply", new=reply), \
         patch("hub.notify.notify_admin_line", new=alert):
        resp = client.post(URL, content=body, headers=headers)
    return resp, reply, alert


class TestSignature(unittest.TestCase):
    def test_valid_signature_returns_200(self):
        resp, _, _ = _post(_event_body())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"status": "ok"})

    def test_invalid_signature_returns_400(self):
        body = _event_body()
        resp, reply, alert = _post(body, signature=_sign(body, "wrong_secret"))
        self.assertEqual(resp.status_code, 400)
        reply.assert_not_awaited()
        alert.assert_not_awaited()

    def test_missing_signature_returns_400(self):
        resp, _, _ = _post(_event_body(), signature="")
        self.assertEqual(resp.status_code, 400)

    def test_secret_unset_denies_all(self):
        """DISPATCHBOT_CHANNEL_SECRET 未設定 = 正しい署名でも常に400（deny-all）"""
        body = _event_body()
        with patch.dict(os.environ, {"DISPATCHBOT_CHANNEL_SECRET": ""}):
            resp, reply, alert = _post(body)
        self.assertEqual(resp.status_code, 400)
        reply.assert_not_awaited()
        alert.assert_not_awaited()

    def test_customer_bot_secret_does_not_validate(self):
        """顧客Botのsecretで署名しても通らない（チャネル分離の検証）"""
        body = _event_body()
        resp, _, _ = _post(body, signature=_sign(body, "customer_secret"))
        self.assertEqual(resp.status_code, 400)


class TestWhitelist(unittest.TestCase):
    def test_allowed_user_gets_handled_reply(self):
        """許可内メッセージは handler の応答が返る（D2 で固定応答から解析応答に置換）"""
        with patch("dispatch_bot.handler.handle_message",
                   new=AsyncMock(return_value="解析応答テスト")):
            resp, reply, alert = _post(_event_body(user_id="U_owner1"))
        self.assertEqual(resp.status_code, 200)
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[2], "解析応答テスト")
        alert.assert_not_awaited()

    def test_second_allowed_user_and_space_tolerance(self):
        """カンマ区切りの2人目（空白付き）も許可される"""
        with patch("dispatch_bot.handler.handle_message",
                   new=AsyncMock(return_value="ok")):
            resp, reply, _ = _post(_event_body(user_id="U_owner2"))
        reply.assert_awaited_once()

    def test_unauthorized_user_is_silent_with_alert(self):
        """許可外: reply/pushゼロ＋管理者警報1回（スロットルkey付き）"""
        resp, reply, alert = _post(_event_body(user_id="U_attacker123", text="こんにちは"))
        self.assertEqual(resp.status_code, 200, "外形上は通常応答（存在を悟らせない）")
        reply.assert_not_awaited()
        alert.assert_awaited_once()
        text = alert.await_args.args[0]
        self.assertIn("許可外ユーザー", text)
        self.assertIn("U_attacker...", text)     # 先頭10文字＋省略
        self.assertNotIn("U_attacker123", text)  # 全文は載せない
        self.assertEqual(alert.await_args.kwargs["throttle_key"],
                         "dispatchbot_unauthorized:U_attacker123")

    def test_whitelist_unset_denies_all(self):
        """DISPATCHBOT_ALLOWED_USER_IDS 未設定・空 = 全拒否（deny-all）"""
        for empty in ("", "  ,  "):
            with self.subTest(env=empty):
                with patch.dict(os.environ, {"DISPATCHBOT_ALLOWED_USER_IDS": empty}):
                    resp, reply, alert = _post(_event_body(user_id="U_owner1"))
                self.assertEqual(resp.status_code, 200)
                reply.assert_not_awaited()
                alert.assert_awaited_once()

    def test_unauthorized_follow_event_alerts(self):
        """友だち追加もホワイトリスト外なら沈黙＋警報"""
        resp, reply, alert = _post(_event_body(user_id="U_stranger99", event_type="follow"))
        reply.assert_not_awaited()
        alert.assert_awaited_once()
        self.assertIn("友だち追加", alert.await_args.args[0])


class TestEventFiltering(unittest.TestCase):
    def test_non_text_message_is_ignored(self):
        body = json.dumps({"events": [{"type": "message", "replyToken": "rt",
                                       "source": {"userId": "U_owner1"},
                                       "message": {"type": "image"}}]}).encode()
        resp, reply, alert = _post(body)
        self.assertEqual(resp.status_code, 200)
        reply.assert_not_awaited()
        alert.assert_not_awaited()

    def test_empty_events_ok(self):
        resp, reply, alert = _post(json.dumps({"events": []}).encode())
        self.assertEqual(resp.status_code, 200)
        reply.assert_not_awaited()


class TestCustomerBotUnchanged(unittest.TestCase):
    """顧客Bot（/webhook）の挙動不変・分離の検証"""

    def test_customer_webhook_signature_still_customer_secret(self):
        """既存 /webhook は従来どおり顧客Bot secret で検証される（400/200 挙動不変）。
        ※main.LINE_CHANNEL_SECRET は最初に main を import したテストの env で固定される
        （モジュール定数）ため、実際に固定された値で署名する（スイート実行順に依存しない）"""
        body = json.dumps({"events": []}).encode()
        digest = hmac.new(main.LINE_CHANNEL_SECRET.encode(), body, hashlib.sha256).digest()
        sig = base64.b64encode(digest).decode()
        resp = client.post("/webhook", content=body,
                           headers={"X-Line-Signature": sig})
        self.assertEqual(resp.status_code, 200)
        self.assertNotEqual(main.LINE_CHANNEL_SECRET, "bot_secret",
                            "前提: 顧客Botと指示Botのsecretは別値")
        resp = client.post("/webhook", content=body,
                           headers={"X-Line-Signature": _sign(body)})  # 指示Bot secret では通らない
        self.assertEqual(resp.status_code, 400)

    def test_dispatch_bot_does_not_import_chat_responder(self):
        """chat_responder 非依存（確定判断9）をソースレベルで検査"""
        import pathlib
        pkg = pathlib.Path(bot.__file__).parent
        for py in pkg.glob("*.py"):
            src = py.read_text(encoding="utf-8")
            for stmt in ("import chat_responder", "from chat_responder"):
                self.assertNotIn(stmt, src,
                                 f"{py.name} が chat_responder を import している")

    def test_dispatch_bot_does_not_use_customer_line_token(self):
        """返信に顧客Botのトークン（LINE_CHANNEL_ACCESS_TOKEN）を使わない"""
        import pathlib
        src = (pathlib.Path(bot.__file__)).read_text(encoding="utf-8")
        self.assertNotIn('environ.get("LINE_CHANNEL_ACCESS_TOKEN', src)
        self.assertNotIn('environ["LINE_CHANNEL_ACCESS_TOKEN', src)

    def test_no_app28_chatlog_writes(self):
        """App 28（顧客チャットログ）に書かない（設計 02 §6・D1はRailwayログのみ）"""
        import pathlib
        src = (pathlib.Path(bot.__file__)).read_text(encoding="utf-8")
        for marker in ("APP_CHATLOG", "save_to_chatlog"):
            self.assertNotIn(marker, src)


if __name__ == "__main__":
    unittest.main()
