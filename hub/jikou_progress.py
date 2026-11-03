"""jikou_progress — JIKOU-PROGRESS-1: 相談者向け進捗連絡の下書き積み（送信は Q4 の承認画面だけ）

QUEUE-2026-10-16-R F-2。feature/jikou-followup-1 の上。

3 イベントで、相談者向けの進捗連絡文（hub/jikou_reply_texts.PROGRESS_TEXTS・**要承認**・独自 sha pin）を Q4 の承認画面
（/app/drafts・hub/webapp_drafts_view）に「下書き」（reply_draft・state=proposed）として積む。**本 module は送信しない**
（hub/line_channel を import しない）。送信は Q4 の既存経路（承認→approved_draft 文脈→push_text→許可表 V2）だけ。

 1) 入金確認: App 21 の 入金状況=入金済み かつ LINEユーザーID あり（更新日時が PROGRESS_LOOKBACK_DAYS 以内）。走査ジョブで検出。
 2) 発送完了: hub/jikou_followup の表（jikou_case_result）に発送日がある案件（発送日が PROGRESS_LOOKBACK_DAYS 以内）。
    差し込みは 発送日・送付先社数（枠の数）。宛先は App 21 の LINEユーザーID。走査ジョブで検出。
 3) 債権者回答到着: F-1 の結果記録（/app/jikou/result の保存）時のフック（jikou_followup.register_result_hook）。

二重積み防止: 下書きの conversation_ref に `progress:{event}:{案件ID}` を置き、同じ ref の下書きが（状態を問わず）あれば積まない。
宛先の解決: Q4 の resolve_destination が `progress:` の ref を App 21 のレコード No. と解釈し LINEユーザーID を読む。
Q4 の送信は V2 ON の許可表（人対応中・対応中は停止）を通る。承認なしでは 1 通も送られない。

RV-10: 下書き本文は固定文言＋日付・社数のみ（氏名・金額・個別事情を入れない）。ログは件数と固定語彙。
定数: PROGRESS_LOOKBACK_DAYS・走査間隔は hub/jikou_constants（要大野確認）。
"""

import datetime
import logging
import os
import re

import sqlalchemy as sa

from hub import jikou_followup, jikou_reply_texts, kintone, reply_draft
from hub import scheduler as hub_scheduler
from hub.db import session_scope
from hub.jikou_constants import PROGRESS_LOOKBACK_DAYS, PROGRESS_SCAN_INTERVAL_MINUTES
from hub.redact import emit
from hub.send_ledger import line_user_hash

logger = logging.getLogger("hub.jikou_progress")

BUSINESS = "jikou"
CATEGORY = "進捗の事実回答"                      # reply_exemplar.CATEGORIES の 1 つ
REF_PREFIX = "progress:"
EVENT_PAID = "paid"
EVENT_SHIPPED = "shipped"
EVENT_ANSWERED = "answered"
EVENTS = (EVENT_PAID, EVENT_SHIPPED, EVENT_ANSWERED)
EVENT_LABELS = {EVENT_PAID: "入金確認", EVENT_SHIPPED: "発送完了", EVENT_ANSWERED: "債権者回答到着"}
TEXT_IDS = {EVENT_PAID: "P-1", EVENT_SHIPPED: "P-2", EVENT_ANSWERED: "P-3"}
JOB_NAME = "JIKOU_PROGRESS_SCAN"
DISABLED_ENV = "JIKOU_PROGRESS_DISABLED"
FIELD_PAYMENT = "入金状況"
PAYMENT_PAID = "入金済み"
FIELD_LINE_USER = "LINEユーザーID"
FIELD_UPDATED = "更新日時"
SEARCH_LIMIT = 500
_LINE_USER_RE = re.compile(r"^U[0-9a-f]{32}$")
_REF_RE = re.compile(r"^progress:(paid|shipped|answered):([0-9]{1,12})$")
_JST = datetime.timezone(datetime.timedelta(hours=9))

QUEUED = "queued"
DUPLICATE = "duplicate"
NO_USER = "no_user"
NO_TEXT = "no_text"
UNAVAILABLE = "unavailable"


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def disabled() -> bool:
    return os.environ.get(DISABLED_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def ref(event: str, record_id: str) -> str:
    return f"{REF_PREFIX}{event}:{record_id}"


def parse_ref(value: str | None) -> tuple[str, str] | None:
    m = _REF_RE.fullmatch(str(value or ""))
    return (m.group(1), m.group(2)) if m else None


def is_progress_ref(value: str | None) -> bool:
    return parse_ref(value) is not None


def fmt_date(d: datetime.date) -> str:
    return f"{d.year}年{d.month}月{d.day}日"


def render(event: str, slots: dict | None = None) -> str | None:
    """PROGRESS_TEXTS の定型に差し込み値（発送日・社数）を入れる。pin 不一致・未解決は None（積まない）。"""
    if event not in EVENTS or not jikou_reply_texts.verify_progress_pin():
        return None
    e = jikou_reply_texts.PROGRESS_TEXTS.get(TEXT_IDS[event]) or {}
    text = str(e.get("text") or "")
    for slot in e.get("slots", []):
        v = (slots or {}).get(slot)
        if v is None or v == "":
            return None
        text = text.replace(f"【{slot}】", str(v))
    if "【" in text:
        return None
    return text


def _valid_user(user_id: str | None) -> str:
    u = str(user_id or "")
    return u if _LINE_USER_RE.fullmatch(u) else ""


async def exists(ref_value: str) -> bool:
    async with session_scope() as s:
        t = reply_draft.reply_draft
        v = (await s.execute(sa.select(t.c.draft_id).where(t.c.business == BUSINESS,
                                                            t.c.conversation_ref == ref_value).limit(1))).scalar()
    return v is not None


async def enqueue(event: str, record_id: str, user_id: str, slots: dict | None = None, *, now=None) -> str:
    """下書きを 1 件積む（同じ案件・同じイベントは 1 回）。戻り値は閉集合（QUEUED／DUPLICATE／NO_USER／NO_TEXT／UNAVAILABLE）。"""
    uid = _valid_user(user_id)
    if not uid:
        return NO_USER
    text = render(event, slots)
    if text is None:
        return NO_TEXT
    r = ref(event, str(record_id))
    try:
        if await exists(r):
            return DUPLICATE
        did = await reply_draft.create(business=BUSINESS, pending_id=None, line_user_hash=line_user_hash(BUSINESS, uid),
                                       conversation_ref=r, category=CATEGORY, inbound_text=r, draft_text=text,
                                       guard_ok=True, guard_reasons=[], exemplar_ids=[], now=now)
    except Exception:
        logger.warning("[JIKOU_PROGRESS] enqueue failed (fixed reason)")
        return UNAVAILABLE
    if did is None:
        return UNAVAILABLE
    logger.info("[JIKOU_PROGRESS] draft queued event=%s case=%s", emit(event, "freetext", "log", "operator"),
                emit(record_id, "record_id", "log", "operator"))
    return QUEUED


# ── 宛先（App 21）─────────────────────────────────────────────────────────────
async def user_id_for_case(record_id: str) -> str:
    """App 21 のレコード No. → LINEユーザーID（形式が合わなければ ""）。"""
    try:
        rec = await kintone.get_record(jikou_followup.APP21, str(record_id))
    except Exception:
        return ""
    return _valid_user(jikou_followup._fv(rec, FIELD_LINE_USER))


# ── 走査（入金確認・発送完了）────────────────────────────────────────────────
def _paid_query(since: datetime.datetime, after_id: str | None = None) -> str:
    stamp = since.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    q = f'{FIELD_PAYMENT} in ("{PAYMENT_PAID}") and {FIELD_LINE_USER} != "" and {FIELD_UPDATED} > "{stamp}"'
    if after_id:
        q += f" and $id > {int(after_id)}"
    return q + f" order by $id asc limit {SEARCH_LIMIT}"


async def scan_paid(*, now=None) -> dict:
    now = now or _now()
    res = {"records": 0, QUEUED: 0, DUPLICATE: 0, "skipped": 0}
    since = now - datetime.timedelta(days=PROGRESS_LOOKBACK_DAYS)
    after = None
    while True:
        try:
            recs = await kintone.search_records(jikou_followup.APP21, _paid_query(since, after),
                                                ["$id", FIELD_LINE_USER, FIELD_PAYMENT])
        except Exception:
            logger.warning("[JIKOU_PROGRESS] paid scan read failed (fixed reason)")
            break
        if not recs:
            break
        for rec in recs:
            rid = jikou_followup._fv(rec, "$id")
            if not rid:
                continue
            res["records"] += 1
            out = await enqueue(EVENT_PAID, rid, jikou_followup._fv(rec, FIELD_LINE_USER), now=now)
            res[out if out in (QUEUED, DUPLICATE) else "skipped"] += 1
        if len(recs) < SEARCH_LIMIT:
            break
        after = jikou_followup._fv(recs[-1], "$id")
    return res


async def _shipped_cases(since: datetime.date) -> list[dict]:
    async with session_scope() as s:
        t = jikou_followup.jikou_case_result
        rows = (await s.execute(sa.select(t.c.case_record_id, t.c.shipped_on).where(
            t.c.business == BUSINESS, t.c.shipped_on.is_not(None), t.c.shipped_on >= since))).fetchall()
    agg: dict = {}
    for r in rows:
        c = agg.setdefault(str(r.case_record_id), {"shipped_on": r.shipped_on, "count": 0})
        c["count"] += 1
        if r.shipped_on < c["shipped_on"]:
            c["shipped_on"] = r.shipped_on
    return [{"case_record_id": k, **v} for k, v in sorted(agg.items())]


async def scan_shipped(*, now=None) -> dict:
    now = now or _now()
    res = {"records": 0, QUEUED: 0, DUPLICATE: 0, "skipped": 0}
    since = jikou_followup.today_jst(now) - datetime.timedelta(days=PROGRESS_LOOKBACK_DAYS)
    try:
        cases = await _shipped_cases(since)
    except Exception:
        logger.warning("[JIKOU_PROGRESS] shipped scan read failed (fixed reason)")
        return res
    for c in cases:
        res["records"] += 1
        rid = c["case_record_id"]
        if await exists(ref(EVENT_SHIPPED, rid)):
            res[DUPLICATE] += 1
            continue
        uid = await user_id_for_case(rid)
        out = await enqueue(EVENT_SHIPPED, rid, uid, {"発送日": fmt_date(c["shipped_on"]), "社数": c["count"]}, now=now)
        res[out if out in (QUEUED, DUPLICATE) else "skipped"] += 1
    return res


async def scan(*, now=None) -> dict:
    """走査ジョブ本体（入金確認＋発送完了）。停止 env・DB 未設定は何もしない。"""
    res = {"disabled": disabled(), "paid": None, "shipped": None}
    if res["disabled"] or not os.environ.get("DATABASE_URL"):
        return res
    res["paid"] = await scan_paid(now=now)
    res["shipped"] = await scan_shipped(now=now)
    logger.info("[JIKOU_PROGRESS] scan paid_queued=%s shipped_queued=%s",
                emit(res["paid"][QUEUED], "count", "log", "operator"), emit(res["shipped"][QUEUED], "count", "log", "operator"))
    return res


# ── F-1 のフック（債権者回答到着）─────────────────────────────────────────────
async def on_result_recorded(record_id: str, record: dict | None) -> str:
    """結果記録の保存時に呼ばれる（jikou_followup.register_result_hook）。宛先は渡された App 21 レコード（無ければ読む）。"""
    if disabled():
        return "disabled"
    uid = _valid_user(jikou_followup._fv(record, FIELD_LINE_USER)) if record else ""
    if not uid:
        uid = await user_id_for_case(record_id)
    return await enqueue(EVENT_ANSWERED, str(record_id), uid)


def register_progress_job() -> None:
    if not hub_scheduler.is_registered(JOB_NAME):
        hub_scheduler.register_interval(JOB_NAME, PROGRESS_SCAN_INTERVAL_MINUTES, scan)
    hub_scheduler.start_all()


def wire_all(app) -> None:
    """main.py 末尾の 1 行: F-1 の結果記録フック登録＋走査ジョブの startup 登録。画面は持たない（Q4 の /app/drafts を使う）。"""
    jikou_followup.register_result_hook(on_result_recorded)
    app.router.on_startup.append(register_progress_job)
