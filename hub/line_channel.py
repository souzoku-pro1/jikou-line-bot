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

from hub import send_ledger
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


async def _begin_with_policy(channel: LineChannelConfig, to: str, text: str):
    """JIKOU-REPLY-Q1c (a): V2 ON・顧客チャネル・主体 human 以外のときだけ停止条件（hub/send_policy）を
    束ねて begin() を呼ぶ。該当しなければ従来どおりの begin()（OFF は本関数の分岐に入らない）。"""
    from hub import send_policy
    cond = await send_policy.conditions_if_applicable(channel.name, to)
    if cond is None:
        return await send_ledger.begin(channel.name, channel.name, to, text)
    with send_ledger.policy(cond):
        op = await send_ledger.begin(channel.name, channel.name, to, text)
    if op is None:
        # Q1c-fix1 BC-01: 台帳（＝会話状態）が確認できない＝許可表を評価できない → 送らない（fail-closed・行なし・通知 1 日 1 回）。
        # Q1a の fail-open（begin None で送る）は V2 OFF・他業務・人の明示送信以外の経路に限る（cond is None の分岐）
        return send_ledger.PolicyStopped(await send_policy.ledger_unavailable_stop())
    if isinstance(op, send_ledger.PolicyStopped):
        send_policy.record_stop(op.reasons)
    elif op is send_ledger.FIRST_REPLY_ALREADY:
        send_policy.record_stop(("first_reply:already",))
    return op


def _policy_stopped(op) -> bool:
    return isinstance(op, send_ledger.PolicyStopped) or op is send_ledger.FIRST_REPLY_ALREADY


async def reply_with_push_fallback(channel: LineChannelConfig,
                                   reply_token: str, to: str,
                                   text: str) -> str:
    """LINE Reply APIを試み、失敗（400等）したらPush APIにフォールバック。

    実装は旧 main._line_reply_with_fallback の逐語移設（ログ文言・順序とも
    不変）。トークンのみチャネル引数から解決する。

    JIKOU-REPLY-Q1a: 送信操作記録と共通排他（hub/send_ledger）を通す——
    [pending で記録 → 排他の中で started → push → sent / unconfirmed / failed]。
    同じ受信イベント・用途の再配送は送らない。記録不能（DB 未設定・DB 例外）は素通り＝
    HTTP 呼び出し・ログ文言・順序は従来どおり。
    fix1 BQ-01: 戻り値は 3 値 send_ledger.SEND_SENT（送信成功、または送信済みの重複）／
    SEND_UNCONFIRMED（結果未確認の重複＝既存操作が pending/started/unconfirmed）／
    SEND_FAILED（最終的に非 2xx）。通信例外は従来どおり送出（記録は unconfirmed）。
    JIKOU-REPLY-Q1c (a): flag V2 ON の顧客チャネルでは送信前に停止条件を束ね、begin() の排他の中の許可表で
    止まれば送らず SEND_POLICY_STOPPED（J-0 の在庫検査で止まった場合も同じ）。OFF は従来どおり。"""
    op = await _begin_with_policy(channel, to, text)
    if op is send_ledger.DUPLICATE_SENT:
        logger.info("[LINE] duplicate send skipped (send_ledger: already sent)")
        return send_ledger.SEND_SENT
    if op is send_ledger.DUPLICATE_UNCONFIRMED:
        logger.info("[LINE] duplicate send skipped (send_ledger: unconfirmed)")
        return send_ledger.SEND_UNCONFIRMED
    if _policy_stopped(op):
        logger.info("[LINE] send skipped (send_policy v2: not allowed)")
        return send_ledger.SEND_POLICY_STOPPED
    try:
        # fix3 BQ-09: HTTP 待機中は heartbeat タスクが稼働中の印を打ち、全体期限（送信 timeout）
        # で asyncio.TimeoutError → 既存の例外経路（unconfirmed・再送出）。記録なし（op None）は素通し
        ok = await send_ledger.guarded_http(
            op, _reply_with_push_fallback_http(channel, reply_token, to, text))
    except BaseException:
        await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
        raise
    await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
    return send_ledger.SEND_SENT if ok else send_ledger.SEND_FAILED


async def _reply_with_push_fallback_http(channel: LineChannelConfig,
                                         reply_token: str, to: str,
                                         text: str) -> bool:
    """HTTP 部分（従来実装の逐語・戻り値 bool=最終的に 2xx を得たか、だけを追加）。"""
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
        return True
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
    return bool(push_resp.is_success)


async def push_text(channel: LineChannelConfig, to: str, text: str, *, require_ledger: bool = False) -> bool | str:
    """LINE Push API でメッセージを送信する。送信成功（2xx）で True。

    実装は旧 chat_responder.send_line_push の逐語移設（ログ文言不変）。
    トークンのみチャネル引数から解決する。
    IMAGE-INTAKE-1-fix1[03]: 戻り値 bool を追加（非 2xx=False）。既存 caller は
    戻り値を見ていないため挙動不変（非 2xx を例外化しない・通信例外は従来
    どおり送出のまま。test_image_intake が pin）。
    JIKOU-REPLY-Q1a: 送信操作記録と共通排他を通す（reply_with_push_fallback と同じ
    [begin → push → finish]）。fix1 BQ-01: 戻り値は 3 値——True（2xx、または送信済みの
    重複）／send_ledger.SEND_UNCONFIRMED（結果未確認の重複＝既存操作が
    pending/started/unconfirmed・成功として扱わない）／False（非 2xx）。既存 caller の
    `is True` 判定は SEND_UNCONFIRMED を失敗側へ倒す（None ではなく文字列にして、
    戻り値 None の mock・旧契約と混同しない）。
    JIKOU-REPLY-Q1c (a): V2 ON で許可表に落ちた送信は send_ledger.SEND_POLICY_STOPPED（文字列＝`is True`
    判定は失敗側）。承認 webhook はこの値で送信済み更新へ進まない。"""
    op = await _begin_with_policy(channel, to, text)
    if op is None and require_ledger:
        # PRE-REVIEW-1 #279-1（JIKOU-REPLY-Q4）: 承認済み下書きの送信は人の明示送信（push_text_explicit）と同じく台帳必須。
        # begin() が None（DB 未設定・記録失敗）なら送らない（bot 経路の fail-open は require_ledger=False のまま不変）
        logger.warning("[LINE_PUSH] ledger unavailable (not sent, fail-closed)")
        return send_ledger.SEND_LEDGER_UNAVAILABLE
    if op is send_ledger.DUPLICATE_SENT:
        logger.info("[LINE_PUSH] duplicate send skipped (send_ledger: already sent)")
        return True
    if op is send_ledger.DUPLICATE_UNCONFIRMED:
        logger.info("[LINE_PUSH] duplicate send skipped (send_ledger: unconfirmed)")
        return send_ledger.SEND_UNCONFIRMED
    if _policy_stopped(op):
        logger.info("[LINE_PUSH] send skipped (send_policy v2: not allowed)")
        return send_ledger.SEND_POLICY_STOPPED
    try:
        # fix3 BQ-09: heartbeat タスク + 全体期限（reply_with_push_fallback と同じ）
        ok = await send_ledger.guarded_http(op, _push_text_http(channel, to, text))
    except BaseException:
        await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
        raise
    await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
    return ok


async def _push_text_http(channel: LineChannelConfig, to: str, text: str) -> bool:
    """HTTP 部分（従来実装の逐語）。"""
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
    return bool(resp.is_success)


# ── JIKOU-REPLY-Q1b: 大野の明示送信（PWA 返信欄） ─────────────────────────────
PUSH_SENT = "sent"
PUSH_FAILED = "failed"
PUSH_UNCONFIRMED = "unconfirmed"
PUSH_DUPLICATE_SENT = "duplicate_sent"
PUSH_DUPLICATE_UNCONFIRMED = "duplicate_unconfirmed"
PUSH_LEDGER_UNAVAILABLE = "ledger_unavailable"
PUSH_OUTCOMES = (PUSH_SENT, PUSH_FAILED, PUSH_UNCONFIRMED, PUSH_DUPLICATE_SENT,
                 PUSH_DUPLICATE_UNCONFIRMED, PUSH_LEDGER_UNAVAILABLE)
LEDGER_APPLIED = "applied"
LEDGER_LATE = "late"
LEDGER_SKIPPED = "skipped"
LEDGER_NONE = "none"


@dataclass(frozen=True)
class ExplicitSendResult:
    """push_text_explicit の戻り値（閉集合）。outcome=送信の結果、ledger=finish の戻り値
    （applied / late / skipped・begin 不能や重複は none）、state=late のとき照合した台帳の現在状態。"""
    outcome: str
    ledger: str = LEDGER_NONE
    state: str | None = None


async def push_text_explicit(channel: LineChannelConfig, op_id: str, to: str,
                             text: str) -> ExplicitSendResult:
    """大野の明示送信: 操作 ID を固定して送信操作記録を通す（主体=human・用途=reply）。
    push_text と同じ [begin → guarded_http → finish] だが、**重複を成功と区別して返す**
    （画面側が App 28 記録・手本保存を二重に行わないため）。
    JIKOU-REPLY-Q1b-fix1（司令塔裁定: 人の明示送信は台帳必須＝fail-closed）:
    - BR-01: begin() が None（DB 未設定・記録失敗）なら **送らず** ledger_unavailable を返す
      （Q1a の bot 経路の fail-open は不変）。
    - BR-02: finish() の戻り値（applied / late / skipped）を ledger に載せる。late は現在の台帳状態を
      照合して state に載せる。呼び出し側は applied のときだけ成功処理を行う。
    - BR-03: 本文ハッシュ（text_version）を着手時に固定し finish で照合する（不一致は late）。
    - BR-05: human_reply 文脈（attend=True）＝begin() の同一 tx で会話を対応中にする。
    通信例外・期限到達は記録を unconfirmed にして outcome=unconfirmed を返す（例外は再送出しない。
    呼び出し側が「送信確認待ち」として扱う）。LINE には一度しか触れない（自動再送なし）。"""
    tv = send_ledger.text_version(text)
    async with send_ledger.human_reply(op_id):
        op = await send_ledger.begin(channel.name, channel.name, to, text)
    if op is None:
        logger.warning("[LINE_PUSH] explicit send refused (send_ledger unavailable, fail-closed)")
        return ExplicitSendResult(PUSH_LEDGER_UNAVAILABLE)
    if op is send_ledger.DUPLICATE_SENT:
        logger.info("[LINE_PUSH] explicit duplicate skipped (send_ledger: already sent)")
        return ExplicitSendResult(PUSH_DUPLICATE_SENT)
    if op is send_ledger.DUPLICATE_UNCONFIRMED:
        logger.info("[LINE_PUSH] explicit duplicate skipped (send_ledger: unconfirmed)")
        return ExplicitSendResult(PUSH_DUPLICATE_UNCONFIRMED)
    try:
        ok = await send_ledger.guarded_http(op, _push_text_http(channel, to, text))
    except Exception:
        led = await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED, text_version=tv)
        return ExplicitSendResult(PUSH_UNCONFIRMED, led)
    led = await send_ledger.finish(
        op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED, text_version=tv)
    state = None
    if led == LEDGER_LATE:
        try:
            state = await send_ledger.operation_state(op.op_id)
        except Exception:
            state = None
    return ExplicitSendResult(PUSH_SENT if ok else PUSH_FAILED, led, state)
