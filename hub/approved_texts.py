"""approved_texts — 相談者向け文言の承認台帳と表示ゲート（TEXT-APPROVAL-GATE-1・QUEUE-2026-10-20-HI H-0）

背景: 文言表（hub/*_texts.py）の status=needs_approval は「印」であり、描画側が参照すれば未承認文がそのまま相談者に出る
（#307 feature/souzoku-2 の S-1〜S-3）。印ではなく**機械的に止める**:

- 承認台帳 ``APPROVED_SHA256``: key → 承認済み本文の SHA256（定数）。初期は安全文 1 件のみ。
- ``approved_text(key, text)``: 文言表の本文 SHA が台帳と一致するときだけ本文を返す。不一致・未登録は None。
- ``approved_or_safe(key, text)``: None のとき安全文 ``SAFE_TEXT`` を返し、参照された key を記録する（PII なし）。
- ``notify_unapproved_once()``: 記録された key を業務 LINE へ **1 日 1 回**（台帳 key ``text_unapproved:{JST 日付}``・
  台帳不可は直接送る fail-open・daily_healthcheck の CONCURRENCY-1 と同型）。
- Q4（人による承認）を通らない描画・送信経路は、本 module 経由でしか文言を取得できない形にする
  （機械検査: test_text_approval_gate.py・scripts/audit_static.py (f)）。

承認の運用（docs/text_approval_gate.md）: 大野の裁定を受けて司令塔の票で ``APPROVED_SHA256`` に key と SHA を追加する。
文言表側の本文を変えれば SHA が変わり、再承認まで自動的に安全文へ戻る（表の status 印とは独立）。
"""

from __future__ import annotations

import datetime
import hashlib
import logging

from hub.redact import emit

logger = logging.getLogger("hub.approved_texts")

SAFE_TEXT_KEY = "SAFE-1"
SAFE_TEXT = "現在準備中です。弁護士より改めてご連絡いたします。"


def sha256_text(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


# ── 承認台帳（key → 承認済み本文の SHA256）。追加は司令塔の票（大野の裁定）でのみ行う。 ──
APPROVED_SHA256: dict[str, str] = {
    SAFE_TEXT_KEY: sha256_text(SAFE_TEXT),
}

LEDGER_BUSINESS = "jikou"
LEDGER_CHANNEL = "text_approval"
_JST = datetime.timezone(datetime.timedelta(hours=9))

_pending_unapproved: set[str] = set()     # 参照された未承認 key（プロセス内・通知で消費）


def is_approved(key: str, text: str) -> bool:
    expected = APPROVED_SHA256.get(key or "")
    return bool(expected) and bool(text) and sha256_text(text) == expected


def approved_text(key: str, text: str) -> str | None:
    """承認済みなら本文、未承認・不一致・未登録なら None。"""
    return text if is_approved(key, text) else None


def _record_unapproved(key: str) -> None:
    _pending_unapproved.add(str(key or "")[:64])
    # 固定文言＋key のみ（record_id 値域・PII なし）
    logger.warning("unapproved text referenced; safe text shown key=%s",
                   emit(str(key or ""), "record_id", "log", "operator"))


def approved_or_safe(key: str, text: str) -> str:
    """描画・送信経路の唯一の文言取得口。未承認は安全文に置き換え、key を記録する。"""
    approved = approved_text(key, text)
    if approved is not None:
        return approved
    _record_unapproved(key)
    return SAFE_TEXT


def pending_unapproved_keys() -> tuple[str, ...]:
    return tuple(sorted(_pending_unapproved))


def reset_pending_for_tests() -> None:
    _pending_unapproved.clear()


def _day_key(now: datetime.datetime | None = None) -> str:
    now = now or datetime.datetime.now(_JST)
    return "text_unapproved:" + now.astimezone(_JST).strftime("%Y-%m-%d")


def notice_text(keys: tuple[str, ...]) -> str:
    return ("【未承認文言の参照】相談者向け文言の承認台帳に無い key が参照され、安全文に置き換えて表示しました。\n"
            "key=" + ", ".join(keys) + "\n"
            "承認は司令塔の票で hub/approved_texts.APPROVED_SHA256 に追加（docs/text_approval_gate.md）。")


async def notify_unapproved_once() -> bool:
    """記録済みの未承認 key を業務 LINE へ 1 日 1 回通知する（台帳 key text_unapproved:{JST 日付}）。
    記録が無ければ何もしない（True）。DUPLICATE は送らない・台帳不可は直接送る。送信後に記録を消費する。"""
    keys = pending_unapproved_keys()
    if not keys:
        return True
    from hub import send_ledger
    from hub.notify import get_admin_line_user_id, notify_admin_line
    text = notice_text(keys)
    token = send_ledger.bind_inbound(_day_key())
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(LEDGER_BUSINESS, LEDGER_CHANNEL, get_admin_line_user_id() or "", text)
    except Exception:
        op = None
    finally:
        send_ledger.unbind(token)
    if op is send_ledger.DUPLICATE_SENT or op is send_ledger.DUPLICATE_UNCONFIRMED:
        _pending_unapproved.clear()
        logger.info("unapproved text notice skipped (already sent today per ledger)")
        return True
    if not isinstance(op, send_ledger.Operation):
        ok = await notify_admin_line(text, throttle_key="text_unapproved:daily")
        if ok:
            _pending_unapproved.clear()
        return ok
    try:
        ok = await notify_admin_line(text, throttle_key="text_unapproved:daily")
    except Exception:
        await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
        raise
    await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
    if ok:
        _pending_unapproved.clear()
    return ok


async def gate_and_notify(key: str, text: str) -> str:
    """async の描画・送信経路向け: approved_or_safe ＋ 未承認なら通知（通知失敗で描画は止めない）。"""
    out = approved_or_safe(key, text)
    if out is SAFE_TEXT and pending_unapproved_keys():
        try:
            await notify_unapproved_once()
        except Exception:
            logger.error("unapproved text notice failed (request failed)")
    return out
