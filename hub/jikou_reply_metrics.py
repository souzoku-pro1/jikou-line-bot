"""jikou_reply_metrics — JIKOU-REPLY-Q6: 返信品質の運用計測（読取専用）と週次要約（業務 LINE・件数と率のみ）

正本: 時効LINEボット_返信規則_v1.4.md §10-9（集計単位=分類 ID・無修正承認の回数）・§10-10（RV-08: 履歴）。
QUEUE-2026-10-13-E E-1。

集計元（DB のみ・kintone は呼ばない・書込ゼロ）:
  1) 受信→弁護士返信の遅延: reply_pending（state=human_replied・resolved_by=human/approved_draft）の
     resolved_at − created_at。直近 7 日／30 日の 中央値・90 パーセンタイル（nearest-rank）・最長。
  2) AI 下書きの結果: reply_draft_approval（record_id が draft: の Q4 記録）の outcome（approved_asis／
     approved_edited／rejected）の件数と率（直近 30 日・全期間）。
  3) 凍結昇格した定型: jikou_reply_text（Q5）の text_id・昇格日・状態。
  4) 債権者別: Q4 記録を reply_draft の会話（conversation_ref）→ reply_exemplar.creditor_name（同じ会話の
     手本に残る社名・最新）で引き当てる。社名はそのまま、不明は「（不明）」。承認率 =（asis+edited）/ 結果あり。
  5) 未返信: reply_pending の open 件数と最古の経過時間。
  6) 案件結果（JIKOU-FOLLOWUP-1 (3)）: jikou_case_result（hub/jikou_followup）の債権者別 成立率と 発送→回答 日数の中央値・
     発送済みで未記録の枠数。表が無い（migration 未適用）ときは空＋注記（他の集計は落とさない）。
週次要約: ジョブ JIKOU_REPLY_METRICS_WEEKLY（register_daily 9 時 JST・月曜だけ送る＝HOUKI 系と同形式）。
送信台帳 key jikou_metrics_weekly:{JST 週開始日=月曜}・hub/send_policy.send_business_notice（channel jikou_notice・
purpose other・DUPLICATE は送らない）。全件数 0 の週は送らない。env JIKOU_REPLY_METRICS_DISABLED=1 で停止。
RV-10: 画面・通知・ログに相談者の氏名・LINE userId・line_user_hash・本文・電話番号を載せない（件数・率・定型 ID・
社名・日付のみ）。
"""

import datetime
import logging
import os
from dataclasses import dataclass, field

import sqlalchemy as sa

from hub import jikou_followup, jikou_reply_promote, reply_draft, reply_exemplar, reply_pending, send_policy
from hub import scheduler as hub_scheduler
from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.jikou_reply_metrics")

BUSINESS = "jikou"
JOB_NAME = "JIKOU_REPLY_METRICS_WEEKLY"
RUN_HOUR_JST = 9
DISABLED_ENV = "JIKOU_REPLY_METRICS_DISABLED"
LEDGER_KIND = "jikou_metrics_weekly"
NOTICE_HEADER = "【時効 返信計測 週次】"
NOTICE_FOOTER = "詳細は /app/reply/metrics（件数と率のみ）で確認してください。"
WINDOWS = (7, 30)
DRAFT_RECORD_PREFIX = "draft:"
UNKNOWN_CREDITOR = "（不明）"
CREDITOR_MAX = 40
_JST = datetime.timezone(datetime.timedelta(hours=9))
SEND_NONE = "none"
SEND_NOT_MONDAY = "not_monday"
SEND_DISABLED = "disabled"
_RESOLVERS = (reply_pending.RESOLVED_BY_HUMAN, reply_pending.RESOLVED_BY_APPROVED_DRAFT)


@dataclass(frozen=True)
class Latency:
    days: int
    count: int
    median_min: float | None
    p90_min: float | None
    max_min: float | None


@dataclass(frozen=True)
class DraftOutcome:
    days: int | None           # None = 全期間
    total: int
    asis: int
    edited: int
    rejected: int

    @property
    def asis_rate(self) -> float | None:
        return (self.asis / self.total) if self.total else None

    @property
    def edited_rate(self) -> float | None:
        return (self.edited / self.total) if self.total else None

    @property
    def rejected_rate(self) -> float | None:
        return (self.rejected / self.total) if self.total else None


@dataclass(frozen=True)
class PromotedText:
    text_id: str
    category: str
    promoted_on: str           # JST 日付
    status: str


@dataclass(frozen=True)
class CreditorRow:
    creditor: str
    drafts: int                # Q4 記録の件数
    decided: int               # 結果あり
    approved: int

    @property
    def approval_rate(self) -> float | None:
        return (self.approved / self.decided) if self.decided else None


@dataclass(frozen=True)
class CaseResultRow:
    """JIKOU-FOLLOWUP-1 (3): 案件結果（債権者別）。件数と率・日数のみ。"""
    creditor: str
    cases: int                 # 記録のある枠数（結果あり）
    established: int
    partial: int
    not_established: int
    no_answer: int
    median_days: float | None  # 発送日→回答日の中央値（回答日のある結果のみ）

    @property
    def established_rate(self) -> float | None:
        return ((self.established + self.partial) / self.cases) if self.cases else None


@dataclass(frozen=True)
class Metrics:
    as_of: datetime.datetime
    latency: tuple = ()
    drafts: tuple = ()
    promoted: tuple = ()
    creditors: tuple = ()
    open_count: int = 0
    oldest_open_min: float | None = None
    notes: tuple = field(default_factory=tuple)
    case_results: tuple = ()                 # JIKOU-FOLLOWUP-1 (3)
    case_unanswered: int = 0                 # 発送済み・結果未記録の枠数


# ── 純関数 ────────────────────────────────────────────────────────────────────
def _aware(v):
    if v is not None and v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return v


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def disabled() -> bool:
    return os.environ.get(DISABLED_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def percentile(values: list, pct: float) -> float | None:
    """nearest-rank 法（ceil(n*p/100) 番目）。空は None。"""
    if not values:
        return None
    xs = sorted(float(v) for v in values)
    k = max(1, int(-(-len(xs) * pct // 100)))        # ceil(n*p/100)
    return xs[min(k, len(xs)) - 1]


def median(values: list) -> float | None:
    if not values:
        return None
    xs = sorted(float(v) for v in values)
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def summarize_latency(minutes: list, days: int) -> Latency:
    return Latency(days=days, count=len(minutes), median_min=median(minutes), p90_min=percentile(minutes, 90),
                   max_min=(max(minutes) if minutes else None))


def week_start_jst(now: datetime.datetime | None = None) -> datetime.date:
    """JST の週開始日（月曜）。"""
    d = (now or _now()).astimezone(_JST).date()
    return d - datetime.timedelta(days=d.weekday())


def is_monday_jst(now: datetime.datetime | None = None) -> bool:
    return (now or _now()).astimezone(_JST).weekday() == 0


def ledger_key(week_start: datetime.date) -> str:
    return f"{LEDGER_KIND}:{week_start.isoformat()}"


def fmt_minutes(m: float | None) -> str:
    if m is None:
        return "—"
    m = int(round(m))
    if m < 60:
        return f"{m}分"
    h, r = divmod(m, 60)
    if h < 48:
        return f"{h}時間{r:02d}分"
    d, hh = divmod(h, 24)
    return f"{d}日{hh}時間"


def fmt_rate(r: float | None) -> str:
    return "—" if r is None else f"{round(r * 100)}%"


def _draft_id(record_id: str) -> int | None:
    s = str(record_id or "")
    if not s.startswith(DRAFT_RECORD_PREFIX):
        return None
    tail = s[len(DRAFT_RECORD_PREFIX):]
    return int(tail) if tail.isdigit() else None


def _clean_creditor(name) -> str:
    s = " ".join(str(name or "").split())
    return s[:CREDITOR_MAX] if s else UNKNOWN_CREDITOR


# ── 集計（読取のみ） ───────────────────────────────────────────────────────────
async def _latency_rows(s, since: datetime.datetime) -> list:
    t = reply_pending.reply_pending
    q = sa.select(t.c.created_at, t.c.resolved_at).where(
        t.c.business == BUSINESS, t.c.state == reply_pending.STATE_HUMAN_REPLIED,
        t.c.resolved_by.in_(list(_RESOLVERS)), t.c.resolved_at.is_not(None))
    rows = (await s.execute(q)).fetchall()
    out = []
    for r in rows:
        ra, ca = _aware(r.resolved_at), _aware(r.created_at)
        if ra is None or ca is None or ra < since:
            continue
        out.append(max(0.0, (ra - ca).total_seconds() / 60.0))
    return out


async def _draft_records(s) -> list[dict]:
    t = reply_exemplar.reply_draft_approval
    rows = (await s.execute(sa.select(t.c.record_id, t.c.created_at, t.c.outcome).where(
        t.c.business == BUSINESS, t.c.record_id.like(DRAFT_RECORD_PREFIX + "%")))).fetchall()
    out = []
    for r in rows:
        did = _draft_id(r.record_id)
        if did is None:
            continue
        out.append({"draft_id": did, "created_at": _aware(r.created_at), "outcome": r.outcome})
    return out


def _outcome_summary(records: list[dict], days: int | None, now: datetime.datetime) -> DraftOutcome:
    since = (now - datetime.timedelta(days=days)) if days else None
    asis = edited = rejected = 0
    for rec in records:
        if since is not None and (rec["created_at"] is None or rec["created_at"] < since):
            continue
        o = rec["outcome"]
        if o == reply_exemplar.OUTCOME_APPROVED_ASIS:
            asis += 1
        elif o == reply_exemplar.OUTCOME_APPROVED_EDITED:
            edited += 1
        elif o == reply_exemplar.OUTCOME_REJECTED:
            rejected += 1
    return DraftOutcome(days=days, total=asis + edited + rejected, asis=asis, edited=edited, rejected=rejected)


async def _creditor_by_draft(s, draft_ids: list) -> dict:
    """draft_id → 社名（reply_draft.conversation_ref → 同じ会話の手本の creditor_name・最新）。"""
    if not draft_ids:
        return {}
    d = reply_draft.reply_draft
    rows = (await s.execute(sa.select(d.c.draft_id, d.c.conversation_ref).where(
        d.c.draft_id.in_(list(draft_ids))))).fetchall()
    ref_by_draft = {int(r.draft_id): r.conversation_ref for r in rows}
    refs = sorted({v for v in ref_by_draft.values() if v})
    creditor_by_ref: dict = {}
    if refs:
        x = reply_exemplar.reply_exemplar
        xs = (await s.execute(sa.select(x.c.conversation_ref, x.c.creditor_name).where(
            x.c.business == BUSINESS, x.c.conversation_ref.in_(refs),
            x.c.creditor_name.is_not(None)).order_by(x.c.id.asc()))).fetchall()
        for r in xs:
            creditor_by_ref[r.conversation_ref] = r.creditor_name       # 後勝ち＝最新
    return {did: _clean_creditor(creditor_by_ref.get(ref)) for did, ref in ref_by_draft.items()}


def _creditor_rows(records: list[dict], creditor_by_draft: dict) -> list:
    agg: dict = {}
    for rec in records:
        name = creditor_by_draft.get(rec["draft_id"], UNKNOWN_CREDITOR)
        a = agg.setdefault(name, [0, 0, 0])
        a[0] += 1
        if rec["outcome"] in reply_exemplar.OUTCOMES:
            a[1] += 1
            if rec["outcome"] in (reply_exemplar.OUTCOME_APPROVED_ASIS, reply_exemplar.OUTCOME_APPROVED_EDITED):
                a[2] += 1
    rows = [CreditorRow(creditor=k, drafts=v[0], decided=v[1], approved=v[2]) for k, v in agg.items()]
    return sorted(rows, key=lambda r: (-r.drafts, r.creditor))


async def _promoted(s) -> list:
    t = jikou_reply_promote.jikou_reply_text
    rows = (await s.execute(sa.select(t.c.text_id, t.c.category, t.c.created_at, t.c.status).where(
        t.c.business == BUSINESS).order_by(t.c.created_at.asc(), t.c.text_id.asc()))).fetchall()
    out = []
    for r in rows:
        ca = _aware(r.created_at)
        out.append(PromotedText(text_id=r.text_id, category=r.category,
                                promoted_on=(ca.astimezone(_JST).date().isoformat() if ca else ""), status=r.status))
    return out


async def _open_summary(s, now: datetime.datetime) -> tuple:
    t = reply_pending.reply_pending
    rows = (await s.execute(sa.select(t.c.created_at).where(
        t.c.business == BUSINESS, t.c.state == reply_pending.STATE_OPEN))).fetchall()
    ages = [max(0.0, (now - _aware(r.created_at)).total_seconds() / 60.0) for r in rows if r.created_at is not None]
    return len(rows), (max(ages) if ages else None)


def _case_result_rows(rows: list[dict]) -> tuple:
    """JIKOU-FOLLOWUP-1 (3): 債権者別の 成立率（成立+一部成立／結果あり）と 発送→回答 日数の中央値。"""
    agg: dict = {}
    for r in rows:
        if r.get("result") is None:
            continue
        name = _clean_creditor(r.get("creditor_name"))
        a = agg.setdefault(name, {"cases": 0, "established": 0, "partial": 0, "not_established": 0, "no_answer": 0,
                                  "days": []})
        a["cases"] += 1
        a[r["result"]] = a.get(r["result"], 0) + 1
        if r.get("shipped_on") and r.get("answered_on"):
            a["days"].append(max(0, (r["answered_on"] - r["shipped_on"]).days))
    out = [CaseResultRow(creditor=k, cases=v["cases"], established=v["established"], partial=v["partial"],
                         not_established=v["not_established"], no_answer=v["no_answer"], median_days=median(v["days"]))
           for k, v in agg.items()]
    return tuple(sorted(out, key=lambda x: (-x.cases, x.creditor)))


async def _case_results_safe() -> tuple:
    """案件結果の表は別 migration（a9b1c3d5e7f0）。未適用・読取失敗は空＋注記（他の集計は落とさない・別セッション）。"""
    try:
        async with session_scope() as s:
            rows = await jikou_followup.result_rows(s)
    except Exception:
        return (), 0, ("case_results_unavailable",)
    unanswered = len([r for r in rows if r.get("shipped_on") and r.get("result") is None])
    return _case_result_rows(rows), unanswered, ()


async def collect(*, now: datetime.datetime | None = None) -> Metrics:
    """全指標を 1 回の読取で集める（例外は呼出側へ・書込ゼロ）。"""
    now = now or _now()
    async with session_scope() as s:
        latency_list = []
        for d in WINDOWS:
            latency_list.append(summarize_latency(await _latency_rows(s, now - datetime.timedelta(days=d)), d))
        latency = tuple(latency_list)
        records = await _draft_records(s)
        drafts = (_outcome_summary(records, 30, now), _outcome_summary(records, None, now))
        creditor_by_draft = await _creditor_by_draft(s, sorted({r["draft_id"] for r in records}))
        creditors = tuple(_creditor_rows(records, creditor_by_draft))
        promoted = tuple(await _promoted(s))
        open_count, oldest = await _open_summary(s, now)
    case_results, case_unanswered, notes = await _case_results_safe()
    return Metrics(as_of=now, latency=latency, drafts=drafts, promoted=promoted, creditors=creditors,
                   open_count=open_count, oldest_open_min=oldest, notes=notes, case_results=case_results,
                   case_unanswered=case_unanswered)


def has_any(m: Metrics) -> bool:
    return bool(m.open_count or m.promoted or any(x.count for x in m.latency) or any(d.total for d in m.drafts))


# ── 週次要約（件数と率のみ） ───────────────────────────────────────────────────
def notice_text(m: Metrics, week_start: datetime.date) -> str:
    lat = {x.days: x for x in m.latency}
    d30 = next((d for d in m.drafts if d.days == 30), None) or DraftOutcome(30, 0, 0, 0, 0)
    lines = [f"{NOTICE_HEADER}{week_start.isoformat()} 週（{m.as_of.astimezone(_JST).date().isoformat()} 集計）"]
    for days in WINDOWS:
        lx = lat.get(days)
        if lx:
            lines.append(f"返信遅延 {days}日: {lx.count}件 中央値 {fmt_minutes(lx.median_min)}／90% {fmt_minutes(lx.p90_min)}"
                         f"／最長 {fmt_minutes(lx.max_min)}")
    lines.append(f"AI下書き 30日: {d30.total}件 無修正 {fmt_rate(d30.asis_rate)}／修正 {fmt_rate(d30.edited_rate)}"
                 f"／却下 {fmt_rate(d30.rejected_rate)}")
    active = len([p for p in m.promoted if p.status == jikou_reply_promote.STATUS_ACTIVE])
    lines.append(f"昇格定型: {active}件（累計 {len(m.promoted)}件）")
    lines.append(f"未返信: {m.open_count}件 最古 {fmt_minutes(m.oldest_open_min)}")
    lines.append(NOTICE_FOOTER)
    return "\n".join(lines)


async def send_weekly_notice(*, now: datetime.datetime | None = None) -> str:
    """週次要約を 1 回だけ送る（台帳 key＝週開始日・全件数 0 は送らない）。戻り値は reply_pending.NOTIFY_* か SEND_NONE。"""
    now = now or _now()
    m = await collect(now=now)
    if not has_any(m):
        return SEND_NONE
    ws = week_start_jst(now)
    return await send_policy.send_business_notice(ledger_key(ws), notice_text(m, ws))


async def weekly_check() -> dict:
    """日次 9 時に呼ばれ、月曜だけ送る（再デプロイで同日 2 回走っても台帳で 1 通）。"""
    res = {"notice": SEND_NONE}
    if disabled():
        res["notice"] = SEND_DISABLED
        return res
    now = _now()
    if not is_monday_jst(now):
        res["notice"] = SEND_NOT_MONDAY
        return res
    try:
        res["notice"] = await send_weekly_notice(now=now)
    except Exception:
        logger.warning("[JIKOU_METRICS] weekly notice failed (fixed reason)")
        res["notice"] = reply_pending.NOTIFY_FAILED
    logger.info("[JIKOU_METRICS] weekly notice=%s", emit(res["notice"], "freetext", "log", "operator"))
    return res


def register_jikou_metrics_job() -> None:
    """FastAPI startup から呼ぶ（HOUKI 系と同方式・単一 worker 前提）。"""
    if not hub_scheduler.is_registered(JOB_NAME):
        hub_scheduler.register_daily(JOB_NAME, RUN_HOUR_JST, weekly_check)
    hub_scheduler.start_all()
