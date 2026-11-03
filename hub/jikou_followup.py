"""jikou_followup — JIKOU-FOLLOWUP-1: 時効援用通知 発送後のフォロー（業務 LINE 通知・結果記録・集計元）

QUEUE-2026-10-16-R F-1。feature/jikou-reply-q6 の上。

役割:
 1) 発送日から 14 日・28 日が経過しても債権者回答が未記録の案件を、業務 LINE に 1 通ずつ通知する
    （送信台帳 key `jikou_followup_14:{案件ID}:{発送日}`／`jikou_followup_28:…`・同じ key は 1 通＝重送なし・
    hub/send_policy.send_business_notice を経由＝business=jikou・channel=jikou_notice・purpose=other）。
 2) 結果記録: 成立／不成立／一部成立／回答なし の 4 値と回答日・回答要旨（自由記述）を、債権者ごと（App 21 の
    問い合わせ業者名／対象債権者2／対象債権者3 の枠）に **人が** /app/jikou/result/{案件ID}（hub/webapp_jikou_result_view・
    _gate）から入力する。保存先は本 module の表 jikou_case_result（＋履歴）。
 3) 集計（債権者別の成立率・回答までの日数の中央値）は hub/jikou_reply_metrics が本表を読む（E-1 の画面に「案件結果」区画）。

発送日の正:
 - kintone App 21 に DATE 欄 `発送日`（FIELD_SHIPPED）が **あれば** それを正とし、日次ジョブが本表へ写す（上書き）。
   欄の有無は form fields（1 時間キャッシュ）で判定する＝無い間は読まない（fail-closed・欄名は保留一覧）。
 - 欄が無い間は、結果記録画面で人が入力した発送日（本表 shipped_on）だけを使う。
 - 発送日が無い案件はフォロー対象外（通知しない）。

kintone への写し書込（結果 4 値・回答日・要旨）は、App 21 に対応欄（COPY_FIELDS: 時効援用結果_{枠}／債権者回答日_{枠}／
回答要旨_{枠}）が **全部そろっているときだけ** $revision CAS で書く。無ければ書かない（保留一覧に欄名案）。

禁止・規律:
 - 相談者への自動送信は一切ない（本 module は hub/line_channel を import しない・通知先は業務 LINE のみ）。
 - RV-10: 通知・ログに氏名・LINE userId・回答要旨を載せない（案件 No.・経過日数・社数・固定語彙のみ）。
   回答要旨は人が書く自由記述＝表にだけ置き、画面は _gate の内側でのみ表示する。
 - RV-08: 削除しない（結果の変更は上書き＋履歴）。
 - 新表は alembic CLI のみ（起動時に走らせない）。purpose は閉集合（other＋key 接頭辞）。
"""

import datetime
import logging
import os
import time

import sqlalchemy as sa

from hub import kintone, reply_pending, send_policy
from hub import scheduler as hub_scheduler
from hub.db import DatabaseNotConfigured, session_scope
from hub.redact import emit

logger = logging.getLogger("hub.jikou_followup")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JST = datetime.timezone(datetime.timedelta(hours=9))

BUSINESS = "jikou"
TABLE_NAME = "jikou_case_result"
HISTORY_TABLE_NAME = "jikou_case_result_history"
TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)

APP21 = kintone.KintoneApp("App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")
FIELD_SHIPPED = "発送日"                                  # DATE・CU 追加前提（無い間は読まない）
FIELD_NOTICE_STATUS = "契約書ステータス"
NOTICE_DONE = "時効援用通知作成済"
CREDITOR_FIELDS = ("問い合わせ業者名", "対象債権者2", "対象債権者3")      # notice_webhook と同じ枠（1〜3）
SLOTS = (1, 2, 3)
COPY_FIELDS = {                                            # kintone への写し（欄がそろっているときだけ）
    "result": "時効援用結果_{slot}",                        # DROP_DOWN: 成立／不成立／一部成立／回答なし
    "answered_on": "債権者回答日_{slot}",                    # DATE
    "summary": "回答要旨_{slot}",                           # MULTI_LINE_TEXT
}
FORM_CACHE_SECONDS = 3600

# 結果 4 値（閉集合・表には code を置く）
RESULT_ESTABLISHED = "established"
RESULT_NOT_ESTABLISHED = "not_established"
RESULT_PARTIAL = "partial"
RESULT_NO_ANSWER = "no_answer"
RESULTS = (RESULT_ESTABLISHED, RESULT_NOT_ESTABLISHED, RESULT_PARTIAL, RESULT_NO_ANSWER)
RESULT_LABELS = {RESULT_ESTABLISHED: "成立", RESULT_NOT_ESTABLISHED: "不成立", RESULT_PARTIAL: "一部成立",
                 RESULT_NO_ANSWER: "回答なし"}
ANSWERED_RESULTS = frozenset({RESULT_ESTABLISHED, RESULT_NOT_ESTABLISHED, RESULT_PARTIAL})   # 回答日を持つ結果

ACTOR_HUMAN = "human"
ACTOR_SYSTEM = "system"
MAX_SUMMARY_CHARS = 1000
CREDITOR_MAX = 60

# フォロー通知（閾値は定数 1 か所・日数の変更は大野判断）
FOLLOWUP_DAYS = (14, 28)
LEDGER_KINDS = {14: "jikou_followup_14", 28: "jikou_followup_28"}
JOB_NAME = "JIKOU_FOLLOWUP"
RUN_HOUR_JST = 9
DISABLED_ENV = "JIKOU_FOLLOWUP_DISABLED"
NOTICE_HEADER = "【時効 発送後フォロー】"
NOTICE_FOOTER = "/app/jikou/result/{record_id} から債権者回答の結果を記録してください（記録すると通知は止まります）。"
SEARCH_LIMIT = 500

SEND_NONE = "none"
SEND_DISABLED = "disabled"

jikou_case_result = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("result_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("case_record_id", sa.Text, nullable=False),          # App 21 レコード No.
    sa.Column("slot", sa.Integer, nullable=False),                  # 債権者の枠 1〜3
    sa.Column("creditor_name", sa.Text, nullable=True),             # 社名（記録時の App 21 の値・集計の軸）
    sa.Column("shipped_on", sa.Date, nullable=True),                # 発送日（App 21 の欄があれば写し・無ければ人の入力）
    sa.Column("shipped_source", sa.Text, nullable=True),            # kintone / human
    sa.Column("result", sa.Text, nullable=True),                    # RESULTS（NULL=未記録）
    sa.Column("answered_on", sa.Date, nullable=True),
    sa.Column("summary", sa.Text, nullable=True),                   # 回答要旨（人の自由記述・画面内のみ）
    sa.Column("recorded_by", sa.Text, nullable=True),
    sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("slot IN (1, 2, 3)", name="ck_jikou_case_result_slot"),
    sa.CheckConstraint("result IS NULL OR result IN ('established', 'not_established', 'partial', 'no_answer')",
                       name="ck_jikou_case_result_result"),
    sa.UniqueConstraint("business", "case_record_id", "slot", name="uq_jikou_case_result_case_slot"),
    sa.Index("ix_jikou_case_result_open", "business", "shipped_on", "result"),
)

jikou_case_result_history = sa.Table(
    HISTORY_TABLE_NAME, metadata,
    sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("result_id", _BIG, sa.ForeignKey("jikou_case_result.result_id"), nullable=False),
    sa.Column("field", sa.Text, nullable=False),                    # shipped_on / result / answered_on / summary
    sa.Column("from_value", sa.Text, nullable=True),
    sa.Column("to_value", sa.Text, nullable=True),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Index("ix_jikou_case_result_history_r", "result_id"),
)


# ── 純関数 ────────────────────────────────────────────────────────────────────
def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def today_jst(now: datetime.datetime | None = None) -> datetime.date:
    return (now or _now()).astimezone(_JST).date()


def disabled() -> bool:
    return os.environ.get(DISABLED_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def _db_configured() -> bool:
    return bool(os.environ.get("DATABASE_URL"))


def parse_date(text) -> datetime.date | None:
    """YYYY-MM-DD だけを受ける（kintone DATE と画面入力の共通形）。それ以外は None。"""
    s = str(text or "").strip()
    if len(s) != 10:
        return None
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        return None


def days_since(shipped_on: datetime.date, today: datetime.date) -> int:
    return (today - shipped_on).days


def due_thresholds(shipped_on: datetime.date, today: datetime.date) -> list[int]:
    """今日の時点で到来している閾値（14・28）。発送日が無効（未来）なら空。"""
    d = days_since(shipped_on, today)
    return [t for t in FOLLOWUP_DAYS if d >= t]


def ledger_key(threshold: int, case_record_id: str, shipped_on: datetime.date) -> str:
    return f"{LEDGER_KINDS[int(threshold)]}:{case_record_id}:{shipped_on.isoformat()}"


def notice_text(case_record_id: str, threshold: int, elapsed: int, unanswered: int) -> str:
    """固定文言＋案件 No.・閾値・経過日数・未記録の社数のみ（RV-10）。"""
    return (f"{NOTICE_HEADER}案件 No.{case_record_id}: 発送から {int(elapsed)} 日経過（{int(threshold)} 日の節目）。"
            f"債権者回答が未記録の社数: {int(unanswered)}。\n" + NOTICE_FOOTER.format(record_id=case_record_id))


def _fv(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def creditors_of(record: dict) -> list[tuple[int, str]]:
    """App 21 レコードの債権者枠（1〜3）のうち非空のもの（notice_webhook と同じ）。"""
    out = []
    for slot, code in zip(SLOTS, CREDITOR_FIELDS):
        v = " ".join(_fv(record, code).split())[:CREDITOR_MAX]
        if v:
            out.append((slot, v))
    return out


def normalize_summary(text) -> str | None:
    s = str(text or "").replace("\r\n", "\n").strip()
    return s[:MAX_SUMMARY_CHARS] if s else None


def validate_entry(result: str, answered_on: str, summary) -> tuple[bool, dict]:
    """画面入力 1 枠分の検査。result は閉集合（空=未記録に戻すことは不可・未入力はスキップ）。
    成立／不成立／一部成立 は回答日必須。回答なし は回答日を持たない。"""
    if result not in RESULTS:
        return False, {}
    d = parse_date(answered_on)
    if result in ANSWERED_RESULTS and d is None:
        return False, {}
    if result == RESULT_NO_ANSWER:
        d = None
    return True, {"result": result, "answered_on": d, "summary": normalize_summary(summary)}


# ── form fields（欄の有無）─────────────────────────────────────────────────────
_form_cache: dict = {}


def clear_form_cache() -> None:
    _form_cache.clear()


async def _form_fields() -> dict | None:
    hit = _form_cache.get("props")
    if hit and time.monotonic() - hit[0] < FORM_CACHE_SECONDS:
        return hit[1]
    try:
        props = await kintone.get_form_fields(APP21)
    except Exception:
        logger.warning("[JIKOU_FOLLOWUP] form fields unavailable (fixed reason)")
        return None
    _form_cache["props"] = (time.monotonic(), props or {})
    return props or {}


async def shipped_field_available() -> bool:
    props = await _form_fields()
    return bool(props) and str((props.get(FIELD_SHIPPED) or {}).get("type", "")) == "DATE"


async def copy_fields_available(slot: int) -> bool:
    props = await _form_fields()
    if not props:
        return False
    return all(tmpl.format(slot=int(slot)) in props for tmpl in COPY_FIELDS.values())


# ── 表の読み書き ───────────────────────────────────────────────────────────────
def _row(r) -> dict:
    d = dict(r._mapping)
    return d


async def _hist(s, result_id: int, field: str, from_value, to_value, actor: str, now) -> None:
    await s.execute(sa.insert(jikou_case_result_history).values(
        result_id=int(result_id), field=field, from_value=(None if from_value is None else str(from_value)),
        to_value=(None if to_value is None else str(to_value)), actor=actor, at=now))


async def rows_for_case(case_record_id: str) -> list[dict]:
    async with session_scope() as s:
        t = jikou_case_result
        rows = (await s.execute(sa.select(t).where(t.c.business == BUSINESS, t.c.case_record_id == str(case_record_id))
                                .order_by(t.c.slot.asc()))).fetchall()
    return [_row(r) for r in rows]


async def ensure_rows(case_record_id: str, creditors: list[tuple[int, str]], *, now=None) -> int:
    """債権者枠ごとの行を用意する（既存行は creditor_name だけ追随・無い枠は作る）。戻り値=作った行数。"""
    now = now or _now()
    created = 0
    async with session_scope() as s:
        t = jikou_case_result
        existing = {int(r.slot): r for r in (await s.execute(sa.select(t).where(
            t.c.business == BUSINESS, t.c.case_record_id == str(case_record_id)))).fetchall()}
        for slot, name in creditors:
            cur = existing.get(int(slot))
            if cur is None:
                await s.execute(sa.insert(t).values(business=BUSINESS, case_record_id=str(case_record_id), slot=int(slot),
                                                    creditor_name=name, created_at=now, updated_at=now))
                created += 1
            elif (cur.creditor_name or "") != name:
                await s.execute(sa.update(t).where(t.c.result_id == cur.result_id).values(creditor_name=name, updated_at=now))
    return created


async def set_shipped(case_record_id: str, shipped_on: datetime.date | None, *, source: str, actor: str, now=None,
                      only_if_empty: bool = False) -> int:
    """案件の全枠に発送日を置く（履歴あり）。only_if_empty=True は空の枠だけ。戻り値=変えた行数。"""
    now = now or _now()
    changed = 0
    async with session_scope() as s:
        t = jikou_case_result
        rows = (await s.execute(sa.select(t).where(t.c.business == BUSINESS,
                                                   t.c.case_record_id == str(case_record_id)))).fetchall()
        for r in rows:
            if r.shipped_on == shipped_on:
                continue
            if only_if_empty and r.shipped_on is not None:
                continue
            await s.execute(sa.update(t).where(t.c.result_id == r.result_id).values(
                shipped_on=shipped_on, shipped_source=(source if shipped_on else None), updated_at=now))
            await _hist(s, r.result_id, "shipped_on", r.shipped_on, shipped_on, actor, now)
            changed += 1
    return changed


async def record_result(case_record_id: str, slot: int, *, result: str, answered_on: datetime.date | None,
                        summary: str | None, actor: str = ACTOR_HUMAN, now=None) -> bool:
    """1 枠の結果を記録（上書き可・履歴あり）。枠の行が無ければ False（先に ensure_rows）。"""
    if result not in RESULTS:
        return False
    now = now or _now()
    async with session_scope() as s:
        t = jikou_case_result
        r = (await s.execute(sa.select(t).where(t.c.business == BUSINESS, t.c.case_record_id == str(case_record_id),
                                                t.c.slot == int(slot)))).first()
        if r is None:
            return False
        values = {"result": result, "answered_on": answered_on, "summary": summary, "recorded_by": actor,
                  "recorded_at": now, "updated_at": now}
        await s.execute(sa.update(t).where(t.c.result_id == r.result_id).values(**values))
        for field, old, new in (("result", r.result, result), ("answered_on", r.answered_on, answered_on),
                                ("summary", r.summary, summary)):
            if old != new:
                # 要旨の履歴は値を置かない（自由記述・本文は現在値のみ保持）
                if field == "summary":
                    await _hist(s, r.result_id, field, ("set" if old else None), ("set" if new else None), actor, now)
                else:
                    await _hist(s, r.result_id, field, old, new, actor, now)
    return True


async def open_cases(today: datetime.date) -> dict:
    """発送日があり結果が未記録の枠を案件ごとに束ねる: {案件ID: {"shipped_on": date, "unanswered": n}}。"""
    async with session_scope() as s:
        t = jikou_case_result
        rows = (await s.execute(sa.select(t.c.case_record_id, t.c.shipped_on, t.c.result).where(
            t.c.business == BUSINESS, t.c.shipped_on.is_not(None)))).fetchall()
    out: dict = {}
    for r in rows:
        if r.shipped_on is None or r.shipped_on > today:
            continue
        c = out.setdefault(str(r.case_record_id), {"shipped_on": r.shipped_on, "unanswered": 0, "total": 0})
        c["total"] += 1
        if r.result is None:
            c["unanswered"] += 1
        # 同一案件で発送日がずれていれば最も早い日を採る（枠ごとに違うことは想定しない）
        if r.shipped_on < c["shipped_on"]:
            c["shipped_on"] = r.shipped_on
    return {k: v for k, v in out.items() if v["unanswered"] > 0}


# ── kintone からの取込（欄があるときだけ）──────────────────────────────────────
def _search_query(after_id: str | None = None) -> str:
    base = f'{FIELD_NOTICE_STATUS} in ("{NOTICE_DONE}") and {FIELD_SHIPPED} != ""'
    if after_id:
        base += f" and $id > {int(after_id)}"
    return base + f" order by $id asc limit {SEARCH_LIMIT}"


async def import_shipped_from_kintone(*, now=None) -> dict:
    """App 21 の 発送日（欄があるとき）を本表へ写す。戻り値は件数のみ。欄が無い・読めないときは何もしない。"""
    res = {"available": False, "records": 0, "rows_created": 0, "shipped_set": 0}
    if not await shipped_field_available():
        return res
    res["available"] = True
    fields = ["$id", FIELD_SHIPPED, FIELD_NOTICE_STATUS, *CREDITOR_FIELDS]
    after = None
    while True:
        try:
            recs = await kintone.search_records(APP21, _search_query(after), fields)
        except Exception:
            logger.warning("[JIKOU_FOLLOWUP] import read failed (fixed reason)")
            break
        if not recs:
            break
        for rec in recs:
            rid = _fv(rec, "$id")
            shipped = parse_date(_fv(rec, FIELD_SHIPPED))
            creditors = creditors_of(rec)
            if not rid or shipped is None or not creditors:
                continue
            res["records"] += 1
            res["rows_created"] += await ensure_rows(rid, creditors, now=now)
            res["shipped_set"] += await set_shipped(rid, shipped, source="kintone", actor=ACTOR_SYSTEM, now=now)
        if len(recs) < SEARCH_LIMIT:
            break
        after = _fv(recs[-1], "$id")
    return res


# ── kintone への写し（欄がそろっているときだけ）───────────────────────────────
async def copy_result_to_kintone(case_record_id: str, slot: int, entry: dict) -> str:
    """結果の写しを App 21 に書く（$revision CAS）。欄が無い→skipped・409→cas_lost・失敗→failed。"""
    if not await copy_fields_available(slot):
        return "skipped"
    try:
        rec = await kintone.get_record(APP21, str(case_record_id))
        fields = {
            COPY_FIELDS["result"].format(slot=int(slot)): RESULT_LABELS[entry["result"]],
            COPY_FIELDS["answered_on"].format(slot=int(slot)): (entry["answered_on"].isoformat()
                                                                 if entry.get("answered_on") else ""),
            COPY_FIELDS["summary"].format(slot=int(slot)): entry.get("summary") or "",
        }
        await kintone.update_record(APP21, str(case_record_id), fields, revision=_fv(rec, "$revision") or None)
        return "written"
    except kintone.KintoneConflict:
        return "cas_lost"
    except Exception:
        logger.warning("[JIKOU_FOLLOWUP] copy to case app failed (fixed reason)")
        return "failed"


# ── 日次フォロー ───────────────────────────────────────────────────────────────
async def followup_scan(*, now=None) -> dict:
    """発送後 14／28 日の未記録案件に業務 LINE 通知（案件×閾値ごとに 1 通・台帳 key で重送なし）。"""
    now = now or _now()
    today = today_jst(now)
    res = {"notice": SEND_NONE, "cases": 0, "sent": 0, "throttled": 0, "failed": 0, "import": None}
    if disabled():
        res["notice"] = SEND_DISABLED
        return res
    if not _db_configured():
        return res
    try:
        res["import"] = await import_shipped_from_kintone(now=now)
        cases = await open_cases(today)
    except DatabaseNotConfigured:
        return res
    except Exception:
        logger.warning("[JIKOU_FOLLOWUP] scan read failed (fixed reason)")
        res["notice"] = reply_pending.NOTIFY_FAILED
        return res
    res["cases"] = len(cases)
    for rid in sorted(cases, key=lambda k: int(k) if str(k).isdigit() else 0):
        c = cases[rid]
        for th in due_thresholds(c["shipped_on"], today):
            text = notice_text(rid, th, days_since(c["shipped_on"], today), c["unanswered"])
            try:
                out = await send_policy.send_business_notice(ledger_key(th, rid, c["shipped_on"]), text)
            except Exception:
                out = reply_pending.NOTIFY_FAILED
            if out == reply_pending.NOTIFY_SENT:
                res["sent"] += 1
            elif out == reply_pending.NOTIFY_THROTTLED:
                res["throttled"] += 1
            else:
                res["failed"] += 1
    res["notice"] = reply_pending.NOTIFY_SENT if res["sent"] else (reply_pending.NOTIFY_THROTTLED if res["throttled"]
                                                                   else SEND_NONE)
    logger.info("[JIKOU_FOLLOWUP] scan cases=%s sent=%s throttled=%s failed=%s",
                emit(res["cases"], "count", "log", "operator"), emit(res["sent"], "count", "log", "operator"),
                emit(res["throttled"], "count", "log", "operator"), emit(res["failed"], "count", "log", "operator"))
    return res


def register_followup_job() -> None:
    """FastAPI startup から呼ぶ（Q6 と同方式・単一 worker 前提）。"""
    if not hub_scheduler.is_registered(JOB_NAME):
        hub_scheduler.register_daily(JOB_NAME, RUN_HOUR_JST, followup_scan)
    hub_scheduler.start_all()


# ── 集計（hub/jikou_reply_metrics が読む・読取のみ）────────────────────────────
async def result_rows(session) -> list[dict]:
    t = jikou_case_result
    rows = (await session.execute(sa.select(t.c.case_record_id, t.c.slot, t.c.creditor_name, t.c.shipped_on, t.c.result,
                                            t.c.answered_on).where(t.c.business == BUSINESS))).fetchall()
    return [{"case_record_id": r.case_record_id, "slot": int(r.slot), "creditor_name": r.creditor_name,
             "shipped_on": r.shipped_on, "result": r.result, "answered_on": r.answered_on} for r in rows]
