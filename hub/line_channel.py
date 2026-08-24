"""LINE チャネル資格情報の一般化（SOUZOKU-HOUKI-H1）

正本 10-unit-02-souzoku-houki.md §10.1（G4: 署名検証・reply/push に channel
資格情報を渡す形へ一般化）の下回り。チャネル＝(署名 secret env, access token
env) の組で、値は**呼び出し時に env から読む**（hub/notify.push_line_message
と同じ流儀）。

- 時効（顧客 Bot）= JIKOU_CHANNEL: 既存 env（LINE_CHANNEL_SECRET /
  LINE_CHANNEL_ACCESS_TOKEN）のまま。main._line_reply_with_fallback と
  chat_responder.send_line_push は本 module へ委譲する薄い包みになるが、
  HTTP 呼び出し・ログ文言・fallback 順序は従来実装の逐語移設＝挙動変更ゼロ
  （test_houki_bot_entry が委譲と挙動を pin）。
- 相続放棄 = HOUKI_CHANNEL: 新 env（HOUKI_LINE_CHANNEL_SECRET /
  HOUKI_LINE_CHANNEL_ACCESS_TOKEN）。secret 未設定は verify が常に False
  （deny-all・dispatch_bot と同思想）。v1（H-1）では受信のみで送信経路は
  houki_bot から呼ばれない（H-3 で使用開始）。

業務通知（管理者・弁護士宛）は従来どおり hub/notify（DISPATCHBOT チャネル）
であり本 module の対象外。
"""

import base64
import hashlib
import hmac
import logging
import os
from dataclasses import dataclass

import httpx

from hub.redact import emit

logger = logging.getLogger("hub.line_channel")

_REPLY_URL = "https://api.line.me/v2/bot/message/reply"
_PUSH_URL = "https://api.line.me/v2/bot/message/push"


@dataclass(frozen=True)
class LineChannelConfig:
    """LINE Messaging API チャネル 1 本分の資格情報の名前（値は env 注入）。"""
    name: str          # ログ・識別用（固定語彙・PII なし）
    secret_env: str    # webhook 署名検証用 channel secret の env 名
    token_env: str     # reply/push 用 channel access token の env 名

    def secret(self) -> str:
        return os.environ.get(self.secret_env, "")

    def token(self) -> str:
        return os.environ.get(self.token_env, "")


# 時効援用（顧客 Bot・既存チャネル）: env 名は従来のまま（変更禁止・テスト pin）
JIKOU_CHANNEL = LineChannelConfig(
    "jikou", "LINE_CHANNEL_SECRET", "LINE_CHANNEL_ACCESS_TOKEN")

# 相続放棄（第2チャネル・正本 §6.4 の HOUKI_* 命名）
HOUKI_CHANNEL = LineChannelConfig(
    "souzoku-houki", "HOUKI_LINE_CHANNEL_SECRET",
    "HOUKI_LINE_CHANNEL_ACCESS_TOKEN")


def houki_channel_disabled_reason() -> str | None:
    """HOUKI 受け口の有効化判定（H1-fix1 [02]・fail-closed）。

    無効なら固定語彙の理由・有効なら None を返す（毎リクエスト判定。
    起動時 fail-fast は併用しない: 単一サービス同居のため誤設定 env で
    時効側まで起動不能にしない・dispatch_bot と同じ受け口判定方式）:
      - "secret_unset"        : HOUKI_LINE_CHANNEL_SECRET 未設定（点火前の既定状態）
      - "secret_equals_jikou" : 時効側 LINE_CHANNEL_SECRET と同値
                                （チャネル取り違えの誤設定＝分離が成立しない）
      - "token_equals_jikou"  : access token が**設定済みかつ**時効側と同値
                                （送信が時効チャネル名義になる誤設定。
                                token 空は v1（H-1）では正当のため対象外）
    """
    secret = HOUKI_CHANNEL.secret()
    if not secret:
        return "secret_unset"
    if secret == JIKOU_CHANNEL.secret():
        return "secret_equals_jikou"
    token = HOUKI_CHANNEL.token()
    if token and token == JIKOU_CHANNEL.token():
        return "token_equals_jikou"
    return None


def verify_line_signature(channel: LineChannelConfig,
                          body: bytes, signature: str) -> bool:
    """X-Line-Signature の HMAC-SHA256 検証（チャネル別 secret）。
    secret 未設定・signature 空は常に False（deny-all・dispatch_bot と同型）。"""
    secret = channel.secret()
    if not secret or not signature:
        return False
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    return hmac.compare_digest(
        base64.b64encode(digest).decode("utf-8"), signature)


async def reply_with_push_fallback(channel: LineChannelConfig,
                                   reply_token: str, to: str,
                                   text: str) -> None:
    """LINE Reply APIを試み、失敗（400等）したらPush APIにフォールバック。

    実装は旧 main._line_reply_with_fallback の逐語移設（ログ文言・順序とも
    不変）。トークンのみチャネル引数から解決する。"""
    token = channel.token()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _REPLY_URL,
            headers={"Authorization": f"Bearer {token}"},
            json={"replyToken": reply_token,
                  "messages": [{"type": "text", "text": text}]},
        )
    if resp.is_success:
        logger.info("[LINE] reply OK user_id=%s",
                    emit(to, "external_ref", "log", "operator"))
        return
    logger.warning("[LINE] reply failed %s %s, trying push",
                   emit(resp.status_code, "count", "log", "operator"),
                   emit(resp.text[:200], "vendor_raw", "log", "operator"))
    async with httpx.AsyncClient() as client:
        push_resp = await client.post(
            _PUSH_URL,
            headers={"Authorization": f"Bearer {token}"},
            json={"to": to, "messages": [{"type": "text", "text": text}]},
        )
    logger.info("[LINE] push fallback status=%s",
                emit(push_resp.status_code, "count", "log", "operator"))
    if not push_resp.is_success:
        logger.error("[LINE] push fallback error: %s",
                     emit(push_resp.text[:200], "vendor_raw", "log", "operator"))


async def push_text(channel: LineChannelConfig, to: str, text: str) -> None:
    """LINE Push API でメッセージを送信する。

    実装は旧 chat_responder.send_line_push の逐語移設（ログ文言不変）。
    トークンのみチャネル引数から解決する。"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _PUSH_URL,
            headers={
                "Authorization": f"Bearer {channel.token()}",
                "Content-Type": "application/json",
            },
            json={"to": to, "messages": [{"type": "text", "text": text}]},
        )
    logger.info("[LINE_PUSH] to=%s status=%s",
                emit(to, "external_ref", "log", "operator"),
                emit(resp.status_code, "count", "log", "operator"))
    if not resp.is_success:
        logger.error("[LINE_PUSH] ERROR: %s",
                     emit(resp.text, "vendor_raw", "log", "operator"))
