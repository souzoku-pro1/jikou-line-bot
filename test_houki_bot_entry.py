"""SOUZOKU-HOUKI-H1: /webhook/souzoku-houki（相続放棄 LINE 入口）のテスト。

固定する仕様:
- 署名検証は HOUKI_LINE_CHANNEL_SECRET（時効の LINE_CHANNEL_SECRET とは別）。
  **secret 未設定は 404**（受け口自体を無効＝存在しないフリ・fail-closed）。
  署名不正・欠落は 400。時効 secret で署名しても通らない（チャネル分離）。
- v1 は deny-all 既定: 受信の検証・記録+管理者通知（userId 単位 throttle）のみ。
  顧客への reply/push・kintone 書込は行わない。deny-all の構造的担保は
  fix1 [01] で AST checker（test_houki_bot_policy.py）へ格上げ（旧 source
  pin は本票由来で置換・削除）。
- fix1 [02]: HOUKI secret が時効側 secret と同値／HOUKI token が設定済みかつ
  時効側 token と同値の誤設定は受け口無効（404）・時効側は通常動作継続。
- LINE 送信のチャネル資格情報一般化（hub/line_channel）: 時効側
  （main._line_reply_with_fallback / chat_responder.send_line_push）は
  JIKOU_CHANNEL=従来 env への委譲となり、HTTP 呼び出し・fallback 順序・
  ログ文言は挙動不変（本テストで httpx レベルで pin）。
- 時効チャネル（/webhook）の署名検証は従来どおり（回帰なし）。
"""

import base64
import hashlib
import hmac
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

# main import 前に環境変数を差し込む。共有キーは **setdefault**（他 suite が
# 先に import 済みでも上書きしない＝suite 実行順に依存しない。main の
# module 定数は最初に import した suite の値で固定されるため、本 file は
# 以降すべて実行時の env / main 定数を参照する）
_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
# LINE_CHANNEL_* の既定は他 suite の多数派（dummy_secret/dummy_token）に
# 合わせる: 本 file が最初に main を import しても、リテラル署名で /webhook を
# 叩く既存 suite（test_autoreply_pause 等）の前提 module 定数を変えない
for _k, _v in {
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
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
    # 相続放棄チャネル（テスト既定値。fail-closed テストでは env から消す）
    "HOUKI_LINE_CHANNEL_SECRET": "houki_secret",
    "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki_token",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import chat_responder  # noqa: E402
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]  # skip ガードの誤解除防止（既存流儀）

from houki_bot import router as houki  # noqa: E402
from hub import line_channel  # noqa: E402

client = TestClient(main.app)
URL = "/webhook/souzoku-houki"


def _houki_secret() -> str:
    return os.environ["HOUKI_LINE_CHANNEL_SECRET"]


def _jikou_secret() -> str:
    return os.environ["LINE_CHANNEL_SECRET"]


def _sign(body: bytes, secret: str | None = None) -> str:
    secret = secret if secret is not None else _houki_secret()
    digest = hmac.new(secret.encode(), body, hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def _event_body(user_id="U_houki_customer_1", text="相談です",
                event_type="message", message_type="text"):
    event = {"type": event_type, "replyToken": "rt1",
             "source": {"userId": user_id}}
    if event_type == "message":
        event["message"] = {"type": message_type}
        if message_type == "text":
            event["message"]["text"] = text
    return json.dumps({"events": [event]}).encode()


def _post(body: bytes, signature: str | None = None, alert=None):
    """署名付き POST。管理者通知と送信ヘルパをモックし
    (response, alert, reply_helper, push_helper) を返す。
    H3: テキストのヒアリング回送先も遮断する（実 kintone/モデルに触れない。
    回送そのものの検証は test_text_message_dispatches_hearing が行う）。"""
    alert = alert or AsyncMock(return_value=True)
    reply_helper, push_helper = AsyncMock(), AsyncMock()
    headers = {"X-Line-Signature":
               signature if signature is not None else _sign(body)}
    with patch("hub.notify.notify_admin_line", new=alert), \
         patch.object(line_channel, "reply_with_push_fallback",
                      new=reply_helper), \
         patch.object(line_channel, "push_text", new=push_helper), \
         patch("houki_bot.router.handle_houki_hearing", new=AsyncMock()):
        resp = client.post(URL, content=body, headers=headers)
    return resp, alert, reply_helper, push_helper


class _FakeResponse:
    def __init__(self, status_code=200, text=""):
        self.status_code = status_code
        self.text = text

    @property
    def is_success(self):
        return 200 <= self.status_code < 300


class _FakeAsyncClient:
    """httpx.AsyncClient の記録用フェイク（POST を記録し所定の応答を返す）。"""
    calls: list = []
    responses: list = []

    def __init__(self, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeAsyncClient.calls.append(
            {"url": url, "headers": headers or {}, "json": json})
        if _FakeAsyncClient.responses:
            return _FakeAsyncClient.responses.pop(0)
        return _FakeResponse(200)

    @classmethod
    def reset(cls, responses=None):
        cls.calls = []
        cls.responses = list(responses or [])


class TestFailClosedAndSignature(unittest.TestCase):
    def test_secret_unset_returns_404(self):
        """HOUKI_LINE_CHANNEL_SECRET 未設定 = 受け口自体が無効（404・
        正しい形式の署名を付けても通らない）"""
        body = _event_body()
        saved = os.environ["HOUKI_LINE_CHANNEL_SECRET"]
        sig = _sign(body, saved)
        for empty in ("", None):
            with self.subTest(env=repr(empty)):
                try:
                    if empty is None:
                        os.environ.pop("HOUKI_LINE_CHANNEL_SECRET", None)
                    else:
                        os.environ["HOUKI_LINE_CHANNEL_SECRET"] = ""
                    resp, alert, reply, push = _post(body, signature=sig)
                finally:
                    os.environ["HOUKI_LINE_CHANNEL_SECRET"] = saved
                self.assertEqual(resp.status_code, 404)
                alert.assert_not_awaited()
                reply.assert_not_awaited()
                push.assert_not_awaited()

    def test_invalid_signature_returns_400(self):
        body = _event_body()
        resp, alert, reply, push = _post(body,
                                         signature=_sign(body, "wrong"))
        self.assertEqual(resp.status_code, 400)
        alert.assert_not_awaited()
        reply.assert_not_awaited()
        push.assert_not_awaited()

    def test_missing_signature_returns_400(self):
        resp, alert, _r, _p = _post(_event_body(), signature="")
        self.assertEqual(resp.status_code, 400)
        alert.assert_not_awaited()

    def test_jikou_secret_does_not_validate(self):
        """時効（顧客Bot）の secret で署名しても通らない（チャネル分離）"""
        self.assertNotEqual(_jikou_secret(), _houki_secret(),
                            "前提: 両チャネルの secret は別値")
        body = _event_body()
        resp, _a, _r, _p = _post(body,
                                 signature=_sign(body, _jikou_secret()))
        self.assertEqual(resp.status_code, 400)

    def test_valid_signature_returns_200(self):
        resp, _a, _r, _p = _post(_event_body())
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"status": "ok"})


class TestCredentialEqualityFailClosed(unittest.TestCase):
    """fix1 [02]: 時効側資格情報との同値=誤設定は受け口無効（404）。"""

    def _with_houki_env(self, secret=None, token=None):
        saved = {k: os.environ.get(k) for k in
                 ("HOUKI_LINE_CHANNEL_SECRET", "HOUKI_LINE_CHANNEL_ACCESS_TOKEN")}
        if secret is not None:
            os.environ["HOUKI_LINE_CHANNEL_SECRET"] = secret
        if token is not None:
            os.environ["HOUKI_LINE_CHANNEL_ACCESS_TOKEN"] = token
        return saved

    def _restore(self, saved):
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_same_secret_disables_endpoint(self):
        body = _event_body()
        saved = self._with_houki_env(secret=_jikou_secret())
        try:
            # 同値 secret による正しい署名でも 404（受け口自体が無効）
            resp, alert, reply, push = _post(
                body, signature=_sign(body, _jikou_secret()))
        finally:
            self._restore(saved)
        self.assertEqual(resp.status_code, 404)
        alert.assert_not_awaited()
        reply.assert_not_awaited()
        push.assert_not_awaited()

    def test_same_token_disables_endpoint(self):
        body = _event_body()
        saved = self._with_houki_env(
            token=os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
        try:
            resp, alert, _r, _p = _post(body)   # secret は正規・token のみ同値
        finally:
            self._restore(saved)
        self.assertEqual(resp.status_code, 404)
        alert.assert_not_awaited()

    def test_empty_token_stays_enabled(self):
        # token 未設定でも受け口は有効のまま（H3 票由来の書き換え: テキストは
        # ヒアリング回送になったため、通知経路が残る follow イベントで検証）
        body = _event_body(event_type="follow")
        saved = self._with_houki_env(token="")
        try:
            resp, alert, _r, _p = _post(body)
        finally:
            self._restore(saved)
        self.assertEqual(resp.status_code, 200)
        alert.assert_awaited_once()

    def test_jikou_webhook_unaffected_by_same_secret_misconfig(self):
        # 誤設定中も時効側 /webhook は通常動作を継続する
        saved = self._with_houki_env(secret=_jikou_secret())
        try:
            body = json.dumps({"events": []}).encode()
            digest = hmac.new(main.LINE_CHANNEL_SECRET.encode(), body,
                              hashlib.sha256).digest()
            resp = client.post(
                "/webhook", content=body,
                headers={"X-Line-Signature":
                         base64.b64encode(digest).decode()})
        finally:
            self._restore(saved)
        self.assertEqual(resp.status_code, 200)

    def test_disabled_reason_closed_set(self):
        self.assertIsNone(line_channel.houki_channel_disabled_reason())
        saved = self._with_houki_env(secret=_jikou_secret())
        try:
            self.assertEqual(line_channel.houki_channel_disabled_reason(),
                             "secret_equals_jikou")
        finally:
            self._restore(saved)
        saved = self._with_houki_env(
            token=os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
        try:
            self.assertEqual(line_channel.houki_channel_disabled_reason(),
                             "token_equals_jikou")
        finally:
            self._restore(saved)
        saved = dict(os.environ)
        os.environ.pop("HOUKI_LINE_CHANNEL_SECRET", None)
        try:
            self.assertEqual(line_channel.houki_channel_disabled_reason(),
                             "secret_unset")
        finally:
            os.environ["HOUKI_LINE_CHANNEL_SECRET"] = \
                saved["HOUKI_LINE_CHANNEL_SECRET"]


class TestDenyAllV1(unittest.TestCase):
    """v1: 検証・記録・管理者通知のみ。顧客への送信は一切ない。"""

    def _assert_notified(self, alert, kind: str, user_id: str):
        alert.assert_awaited_once()
        text = alert.await_args.args[0]
        self.assertIn("【相続放棄LINE】", text)
        self.assertIn(f"種別: {kind}", text)
        self.assertIn(user_id[:10] + "...", text)
        self.assertNotIn(user_id, text)          # userId 全文は載せない
        self.assertEqual(alert.await_args.kwargs["throttle_key"],
                         f"houki_inbound:{user_id}")

    def test_text_message_dispatches_hearing(self):
        # SOUZOKU-HOUKI-H3（票由来の書き換え）: テキストは deny-all 通知でなく
        # ヒアリング会話へ回送される（管理者通知なし・回送引数の逐語）
        uid = "U_houki_customer_1"
        hearing = AsyncMock()
        body = _event_body(user_id=uid, text="相談です")
        alert = AsyncMock(return_value=True)
        with patch("houki_bot.router.handle_houki_hearing", new=hearing), \
             patch("hub.notify.notify_admin_line", new=alert):
            resp = client.post(URL, content=body,
                               headers={"X-Line-Signature": _sign(body)})
        self.assertEqual(resp.status_code, 200)
        hearing.assert_awaited_once_with("rt1", uid, "相談です")
        alert.assert_not_awaited()

    def test_image_message_notifies(self):
        uid = "U_houki_customer_2"
        _resp, alert, reply, _p = _post(
            _event_body(user_id=uid, message_type="image"))
        self._assert_notified(alert, "画像", uid)
        reply.assert_not_awaited()

    def test_follow_event_notifies(self):
        uid = "U_houki_follower_9"
        _resp, alert, reply, _p = _post(
            _event_body(user_id=uid, event_type="follow"))
        self._assert_notified(alert, "友だち追加", uid)
        reply.assert_not_awaited()

    def test_other_event_types_ignored(self):
        for etype in ("unfollow", "postback"):
            with self.subTest(event_type=etype):
                resp, alert, _r, _p = _post(_event_body(event_type=etype))
                self.assertEqual(resp.status_code, 200)
                alert.assert_not_awaited()

    def test_empty_events_ok(self):
        resp, alert, _r, _p = _post(json.dumps({"events": []}).encode())
        self.assertEqual(resp.status_code, 200)
        alert.assert_not_awaited()

    # 旧 test_source_pins_no_send_no_jikou_creds_no_kintone（文字列包含の
    # source pin）は fix1 [01]（R-SOUZOKU-HOUKI-H1）で AST checker
    # （test_houki_bot_policy.py: import 閉集合・notify 許可属性閉集合・
    # 動的アクセス遮断+checker negative）へ格上げ・置換した（本票由来）。

    def test_throttle_kind_registered(self):
        """houki_inbound は notify の throttle 種別語彙に登録済み（ID 非露出）"""
        from hub import notify as hub_notify
        with self.assertLogs(hub_notify.logger, level="INFO") as cm:
            hub_notify._log_throttled("houki_inbound:U_secret_user")
        out = "\n".join(cm.output)
        self.assertIn("kind=houki_inbound", out)
        self.assertNotIn("unknown_kind", out)
        self.assertNotIn("U_secret_user", out)


class TestChannelConfig(unittest.TestCase):
    """チャネル資格情報の一般化（hub/line_channel）と env 名の pin。"""

    def test_jikou_channel_env_names_unchanged(self):
        self.assertEqual(line_channel.JIKOU_CHANNEL.secret_env,
                         "LINE_CHANNEL_SECRET")
        self.assertEqual(line_channel.JIKOU_CHANNEL.token_env,
                         "LINE_CHANNEL_ACCESS_TOKEN")

    def test_houki_channel_env_names(self):
        self.assertEqual(line_channel.HOUKI_CHANNEL.secret_env,
                         "HOUKI_LINE_CHANNEL_SECRET")
        self.assertEqual(line_channel.HOUKI_CHANNEL.token_env,
                         "HOUKI_LINE_CHANNEL_ACCESS_TOKEN")

    def test_verify_signature_channel_separation(self):
        # 実行時 env の実値で検証（suite 実行順・先行 import に依存しない）
        self.assertNotEqual(_jikou_secret(), _houki_secret())
        body = b'{"events": []}'
        ok = _sign(body, _houki_secret())
        jikou_sig = _sign(body, _jikou_secret())
        self.assertTrue(line_channel.verify_line_signature(
            line_channel.HOUKI_CHANNEL, body, ok))
        self.assertFalse(line_channel.verify_line_signature(
            line_channel.HOUKI_CHANNEL, body, jikou_sig))
        self.assertFalse(line_channel.verify_line_signature(
            line_channel.JIKOU_CHANNEL, body, ok))
        self.assertTrue(line_channel.verify_line_signature(
            line_channel.JIKOU_CHANNEL, body, jikou_sig))
        with patch.dict(os.environ, {"HOUKI_LINE_CHANNEL_SECRET": ""}):
            self.assertFalse(line_channel.verify_line_signature(
                line_channel.HOUKI_CHANNEL, body, ok))


class TestJikouSendUnchanged(unittest.IsolatedAsyncioTestCase):
    """時効側送信の挙動不変（呼び出し形の変更のみ）を httpx レベルで pin。"""

    async def test_reply_success_uses_jikou_token_single_call(self):
        _FakeAsyncClient.reset([_FakeResponse(200)])
        with patch.object(line_channel.httpx, "AsyncClient",
                          _FakeAsyncClient):
            await main._line_reply_with_fallback("rtok", "U123", "本文")
        calls = _FakeAsyncClient.calls
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["url"],
                         "https://api.line.me/v2/bot/message/reply")
        self.assertEqual(calls[0]["headers"]["Authorization"],
                         "Bearer " + os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
        self.assertEqual(calls[0]["json"],
                         {"replyToken": "rtok",
                          "messages": [{"type": "text", "text": "本文"}]})

    async def test_reply_failure_falls_back_to_push_same_token(self):
        _FakeAsyncClient.reset([_FakeResponse(400, "expired"),
                                _FakeResponse(200)])
        with patch.object(line_channel.httpx, "AsyncClient",
                          _FakeAsyncClient):
            await main._line_reply_with_fallback("rtok", "U123", "本文")
        calls = _FakeAsyncClient.calls
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["url"],
                         "https://api.line.me/v2/bot/message/push")
        self.assertEqual(calls[1]["headers"]["Authorization"],
                         "Bearer " + os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
        self.assertEqual(calls[1]["json"],
                         {"to": "U123",
                          "messages": [{"type": "text", "text": "本文"}]})

    async def test_send_line_push_uses_jikou_token(self):
        _FakeAsyncClient.reset([_FakeResponse(200)])
        with patch.object(line_channel.httpx, "AsyncClient",
                          _FakeAsyncClient):
            await chat_responder.send_line_push("U9", "承認済み返信")
        calls = _FakeAsyncClient.calls
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["url"],
                         "https://api.line.me/v2/bot/message/push")
        self.assertEqual(calls[0]["headers"]["Authorization"],
                         "Bearer " + os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
        self.assertEqual(calls[0]["headers"]["Content-Type"],
                         "application/json")

    async def test_houki_push_would_use_houki_token(self):
        """（前方互換の確認）HOUKI チャネル指定時は HOUKI token を使う。
        v1 では houki_bot から呼ばれない（source pin）——H-3 用の下回り検証"""
        _FakeAsyncClient.reset([_FakeResponse(200)])
        with patch.object(line_channel.httpx, "AsyncClient",
                          _FakeAsyncClient):
            await line_channel.push_text(line_channel.HOUKI_CHANNEL,
                                         "U9", "x")
        self.assertEqual(_FakeAsyncClient.calls[0]["headers"]["Authorization"],
                         "Bearer houki_token")

    async def test_delegation_passes_jikou_channel(self):
        """時効側 wrapper は JIKOU_CHANNEL を渡す（資格情報の取り違え防止 pin）"""
        with patch.object(line_channel, "reply_with_push_fallback",
                          new=AsyncMock()) as rw:
            await main._line_reply_with_fallback("rt", "U1", "t")
        self.assertIs(rw.await_args.args[0], line_channel.JIKOU_CHANNEL)
        with patch.object(line_channel, "push_text",
                          new=AsyncMock()) as pt:
            await chat_responder.send_line_push("U1", "t")
        self.assertIs(pt.await_args.args[0], line_channel.JIKOU_CHANNEL)


class TestJikouWebhookUnchanged(unittest.TestCase):
    """時効チャネル（/webhook）の署名検証は従来どおり（回帰なし）。"""

    def test_jikou_webhook_signature_separation(self):
        body = json.dumps({"events": []}).encode()
        digest = hmac.new(main.LINE_CHANNEL_SECRET.encode(), body,
                          hashlib.sha256).digest()
        sig = base64.b64encode(digest).decode()
        resp = client.post("/webhook", content=body,
                           headers={"X-Line-Signature": sig})
        self.assertEqual(resp.status_code, 200)
        self.assertNotEqual(main.LINE_CHANNEL_SECRET, _houki_secret(),
                            "前提: 時効と相続放棄の secret は別値")
        resp = client.post("/webhook", content=body,
                           headers={"X-Line-Signature": _sign(body)})
        self.assertEqual(resp.status_code, 400)   # houki secret では通らない


if __name__ == "__main__":
    unittest.main()
