"""houki_overdue — HOUKI-JUKURYO-3: 期限超過×未提出の一覧と 1 日 1 回の件数通知

正本: docs/souzoku-houki/03-jukuryo-kikan.md §4（残日数 < 0 の警報）・HOUKI-JUKURYO-2（通知の正は送信台帳）。
QUEUE-2026-10-13-D D-7。
- 対象: hub/houki_jukuryo.fetch_all_targets（受任後 status × 申述提出日 空）のうち 起算日_確定 があり残日数 < 0 のレコード。
- 通知: 業務（管理者）LINE へ **件数のみ**（固定文言・レコード番号や氏名を載せない）を 1 日 1 回。送信台帳
  （business=souzoku-houki・channel=houki_overdue・purpose=other・受信イベント ID=houki_overdue:{JST 日付}）で
  1 回性を判定（HOUKI-JUKURYO-2 と同じ方式: begin → guarded_http → finish・DUPLICATE は送らない・台帳不可は送らない）。
  0 件の日は送らない。
- 一覧: /app/houki/overdue（認証 _gate・案件番号・期限・残日数のみ。氏名・住所・LINE userId は出さない）。
- ジョブ: hub/scheduler.register_daily（HOUKI_OVERDUE・9 時 JST）。env HOUKI_OVERDUE_DISABLED=1 で停止。
- RV-10: ログは件数・固定語彙のみ。
"""

import logging
import os
from dataclasses import dataclass
from datetime import date

from hub import houki_jukuryo as hj
from hub import kintone, send_ledger
from hub import scheduler as hub_scheduler
from hub.redact import emit

logger = logging.getLogger("hub.houki_overdue")

JOB_NAME = "HOUKI_OVERDUE"
RUN_HOUR_JST = 9
DISABLED_ENV = "HOUKI_OVERDUE_DISABLED"
LEDGER_BUSINESS = hj.LEDGER_BUSINESS
LEDGER_CHANNEL = "houki_overdue"
LEDGER_KIND = "houki_overdue"
NOTICE_HEADER = "【相続放棄 期限超過】"
NOTICE_FOOTER = "一覧は /app/houki/overdue（案件番号・期限・残日数のみ）で確認してください。"
SEND_SENT = hj.SEND_SENT
SEND_DUPLICATE_SENT = hj.SEND_DUPLICATE_SENT
SEND_DUPLICATE_UNCONFIRMED = hj.SEND_DUPLICATE_UNCONFIRMED
SEND_UNCONFIRMED = hj.SEND_UNCONFIRMED
SEND_FAILED = hj.SEND_FAILED
SEND_LEDGER_UNAVAILABLE = hj.SEND_LEDGER_UNAVAILABLE
SEND_NONE = "none"                                # 0 件（送らない）


@dataclass(frozen=True)
class OverdueRow:
    record_id: str
    deadline: date
    remaining: int


def disabled() -> bool:
    return os.environ.get(DISABLED_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def overdue_rows(records: list[dict], today: date) -> list[OverdueRow]:
    """受任後×未提出×起算日_確定あり×残日数 < 0（純関数・残日数の小さい順）。"""
    out: list[OverdueRow] = []
    for rec in records:
        if not hj.is_target(rec):
            continue
        comp = hj.compute(rec, today)
        if comp is None or comp.remaining >= 0:
            continue
        rid = hj._v(rec, "$id")
        if rid:
            out.append(OverdueRow(rid, comp.deadline, comp.remaining))
    return sorted(out, key=lambda r: (r.remaining, int(r.record_id) if r.record_id.isdigit() else 0))


def ledger_key(today: date) -> str:
    return f"{LEDGER_KIND}:{today.isoformat()}"


def notice_text(today: date, count: int) -> str:
    return (f"{NOTICE_HEADER}{today.isoformat()} 時点で、社内期限を超過し申述未提出の案件が {count} 件あります。\n"
            f"{NOTICE_FOOTER}")


async def send_daily_notice(today: date, count: int) -> str:
    """件数通知（1 日 1 回・台帳必須・自動再送なし）。戻り値は閉集合。"""
    if count <= 0:
        return SEND_NONE
    admin_id = hj.get_admin_line_user_id()
    if not admin_id:
        logger.info("[HOUKI_OVERDUE] notice skipped (admin LINE not set)")
        return SEND_FAILED
    text = notice_text(today, count)
    token = send_ledger.bind_inbound(ledger_key(today))
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(LEDGER_BUSINESS, LEDGER_CHANNEL, admin_id, text)
        if op is send_ledger.DUPLICATE_SENT:
            return SEND_DUPLICATE_SENT
        if op is send_ledger.DUPLICATE_UNCONFIRMED:
            return SEND_DUPLICATE_UNCONFIRMED
        if op is None or not isinstance(op, send_ledger.Operation):
            logger.warning("[HOUKI_OVERDUE] notice skipped (ledger unavailable)")
            return SEND_LEDGER_UNAVAILABLE
        try:
            ok = await send_ledger.guarded_http(op, hj._push_admin_http(admin_id, text))
        except Exception:
            await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
            return SEND_UNCONFIRMED
        if not ok:
            await send_ledger.finish(op, send_ledger.STATE_FAILED)
            return SEND_FAILED
        applied = await send_ledger.finish(op, send_ledger.STATE_SENT)
        return SEND_SENT if applied == "applied" else SEND_UNCONFIRMED
    finally:
        send_ledger.unbind(token)


async def list_overdue(*, today: date | None = None) -> list[OverdueRow]:
    """kintone から対象を取得して超過行を返す（取得失敗は例外）。"""
    today = today or hj._today_jst()
    records = await hj.fetch_all_targets()
    return overdue_rows(records, today)


async def overdue_daily_check() -> dict:
    """日次: 超過件数を数え、1 件以上なら件数だけを 1 日 1 回通知。"""
    res = {"targets": 0, "overdue": 0, "notice": SEND_NONE}
    if disabled():
        res["notice"] = "disabled"
        return res
    today = hj._today_jst()
    try:
        records = await hj.fetch_all_targets()
    except kintone.KintoneError as e:
        logger.error("[HOUKI_OVERDUE] fetch failed cls=%s", emit(type(e).__name__, "vendor_raw", "log", "operator"))
        return res
    rows = overdue_rows(records, today)
    res["targets"] = len(records)
    res["overdue"] = len(rows)
    res["notice"] = await send_daily_notice(today, len(rows))
    logger.info("[HOUKI_OVERDUE] overdue=%s notice=%s", emit(len(rows), "count", "log", "operator"),
                emit(res["notice"], "freetext", "log", "operator"))
    return res


def register_houki_overdue_job() -> None:
    """FastAPI startup から呼ぶ（houki_jukuryo と同方式・単一 worker 前提）。"""
    if not hub_scheduler.is_registered(JOB_NAME):
        hub_scheduler.register_daily(JOB_NAME, RUN_HOUR_JST, overdue_daily_check)
    hub_scheduler.start_all()
