"""send_failclosed — JIKOU-SEND-FAILCLOSED-1: bot 送信経路の「台帳必須」切替（flag・既定 OFF）

正本: 時効LINEボット_返信規則_v1.4.md §10-5（送信操作記録）・JIKOU-REPLY-Q1a の fail-open 仮置き（DB 未設定・
DB 例外は記録を素通りして送る）。QUEUE-2026-10-13-E E-4。
- flag JIKOU_SEND_FAILCLOSED=1 のとき: hub/line_channel の 2 プリミティブ（reply_with_push_fallback / push_text）は
  送信台帳 begin() が None（DB 未設定・DB 例外＝台帳が使えない）なら **送らない**（reply は SEND_FAILED・push は False）。
  同時に業務 LINE へ障害通知を 1 通（固定文言・件数も PII も載せない）。通知の 1 回性は台帳 key
  send_ledger_down:{JST 日付}（business=channel 名・channel=send_ledger_down・purpose=other）で判定し、台帳が
  使えない間はプロセス内の日付メモで 1 日 1 回に抑える（台帳復旧後に同日の記録が無ければもう 1 通までは許容）。
- OFF（既定）: 本 module の関数は呼ばれても挙動を変えない（line_channel は flag OFF のとき従来の素通り経路のまま）。
- 人の明示送信（push_text_explicit・Q1b）は本票の対象外（既に fail-closed）。
- RV-10: ログ・通知は固定語彙のみ。
"""

import datetime
import logging
import os

from hub import notify, send_ledger
from hub.redact import emit

logger = logging.getLogger("hub.send_failclosed")

FLAG_ENV = "JIKOU_SEND_FAILCLOSED"
RECIPIENT_ENV = "ATTORNEY_LINE_USER_ID"
LEDGER_KIND = "send_ledger_down"
LEDGER_CHANNEL = "send_ledger_down"
NOTICE_HEADER = "【送信台帳 障害】"
NOTICE_BODY = "送信台帳（DB）に記録できないため、bot の自動送信を止めています。復旧後は自動的に再開します（未送信分の再送はしません）。"
NOTICE_FOOTER = "/app/send_ops と Railway のログを確認してください。"
_JST = datetime.timezone(datetime.timedelta(hours=9))

NOTIFY_SENT = "sent"
NOTIFY_THROTTLED = "throttled"
NOTIFY_FAILED = "failed"
NOTIFY_SKIPPED = "skipped"          # 宛先未設定

_memo: dict = {}                    # channel 名 → 最後に通知した JST 日付（台帳が使えない間の抑止）


def enabled() -> bool:
    return os.environ.get(FLAG_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def _today_jst() -> datetime.date:
    return datetime.datetime.now(_JST).date()


def ledger_key(today: datetime.date) -> str:
    return f"{LEDGER_KIND}:{today.isoformat()}"


def notice_text(channel_name: str, today: datetime.date) -> str:
    return f"{NOTICE_HEADER}{today.isoformat()}・チャネル: {channel_name}\n{NOTICE_BODY}\n{NOTICE_FOOTER}"


def reset_memo() -> None:
    _memo.clear()


async def _push(to: str, text: str) -> bool:
    return await notify.push_line_message(to, text, token_env=notify.business_token_env())


async def notify_ledger_down(channel_name: str, *, today: datetime.date | None = None) -> str:
    """台帳障害の業務 LINE 通知（1 日 1 回）。戻り値は閉集合 sent / throttled / failed / skipped。
    台帳 key で 1 回性を判定し、台帳が使えなければプロセス内メモで抑える。"""
    today = today or _today_jst()
    to = os.environ.get(RECIPIENT_ENV, "")
    if not to:
        logger.info("[SEND_FAILCLOSED] notice skipped (recipient not set)")
        return NOTIFY_SKIPPED
    text = notice_text(channel_name, today)
    token = send_ledger.bind_inbound(ledger_key(today))
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(channel_name, LEDGER_CHANNEL, to, text)
        if op is send_ledger.DUPLICATE_SENT or op is send_ledger.DUPLICATE_UNCONFIRMED:
            return NOTIFY_THROTTLED
        if isinstance(op, send_ledger.Operation):
            try:
                ok = await send_ledger.guarded_http(op, _push(to, text))
            except Exception:
                await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
                return NOTIFY_FAILED
            await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
            _memo[channel_name] = today
            return NOTIFY_SENT if ok else NOTIFY_FAILED
        # 台帳が使えない（想定される通常形）: プロセス内メモで 1 日 1 回
        if _memo.get(channel_name) == today:
            return NOTIFY_THROTTLED
        ok = await _push(to, text)
        if ok:
            _memo[channel_name] = today
        return NOTIFY_SENT if ok else NOTIFY_FAILED
    finally:
        send_ledger.unbind(token)


async def on_ledger_unavailable(channel_name: str) -> None:
    """line_channel から flag ON かつ begin()=None のときだけ呼ばれる: 送らない旨を固定語彙で記録し、障害通知。"""
    logger.warning("[SEND_FAILCLOSED] send stopped (ledger unavailable) channel=%s",
                   emit(channel_name, "freetext", "log", "operator"))
    try:
        res = await notify_ledger_down(channel_name)
    except Exception:
        logger.warning("[SEND_FAILCLOSED] notice failed (fixed reason)")
        return
    logger.info("[SEND_FAILCLOSED] notice=%s", emit(res, "freetext", "log", "operator"))
