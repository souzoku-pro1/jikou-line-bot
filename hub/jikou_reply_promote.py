"""jikou_reply_promote — JIKOU-REPLY-Q5: 凍結定型の候補 → 大野の承認で定型表（データ表・DB）に追加する昇格経路

正本: 時効LINEボット_返信規則_v1.4.md R-J9（下書き→承認→閾値→大野の凍結操作→反映）・§10-9（集計単位=分類 ID・
「差分なしで承認」の回数が閾値 3 に達したら候補・見立て・推奨時期・判決確定後の方針の分類は昇格しても下書きのみ）・
§10-10（RV-08 履歴）。QUEUE-2026-10-13-D D-4。

設計（司令塔仮置き）:
- 候補: category の無修正承認回数（reply_exemplar.count_unmodified・Q3a/Q4 の記録）が PROMOTION_THRESHOLD 以上で、
  その category の承認済み（active）手本。下書きのみの分類（DRAFT_ONLY_CATEGORIES）は候補にしない。
- 昇格: 手本の匿名化済み返信文を、表 jikou_reply_text に新 ID（J-U1 から連番）で追加（category・source 手本 id・
  text_version=sha256）。**コードの定型表（hub/jikou_reply_texts・TABLE_SHA256_PIN）は触らない**。データ表の版
  （table_version=active 行の正規化 JSON の sha256）を履歴（note）に記録する＝「sha pin はデータ表の版として記録」。
- 選択: category → 定型 ID の対応は本表から読む（category_map）。hub/jikou_reply_flow は Q2 の決定表で該当なしのとき
  compose_reply の category に対応する active な昇格定型を選ぶ（1 件・新しい昇格順・差し込みなし）。連投禁止は履歴照合。
- 退役: status=retired（削除しない）。再承認は同画面で reactivate。履歴表 jikou_reply_text_history（promoted / retired /
  reactivated・actor・版）。
- RV-10: 表に LINE userId・原文は置かない（匿名化済み文のみ）。ログは件数・固定語彙のみ。
"""

import datetime
import hashlib
import json
import logging
import re

import sqlalchemy as sa

from hub import db, reply_exemplar
from hub.db import DatabaseNotConfigured, session_scope
from hub.redact import emit

logger = logging.getLogger("hub.jikou_reply_promote")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

BUSINESS = "jikou"
TABLE_NAME = "jikou_reply_text"
HISTORY_TABLE_NAME = "jikou_reply_text_history"
TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)
ID_PREFIX = "J-U"
_ID_RE = re.compile(r"^J-U(\d{1,6})$")
STATUS_ACTIVE = "active"
STATUS_RETIRED = "retired"
STATUSES = (STATUS_ACTIVE, STATUS_RETIRED)
ACTION_PROMOTED = "promoted"
ACTION_RETIRED = "retired"
ACTION_REACTIVATED = "reactivated"
ACTIONS = (ACTION_PROMOTED, ACTION_RETIRED, ACTION_REACTIVATED)
ACTOR_HUMAN = "human"
PROMOTION_THRESHOLD = reply_exemplar.PROMOTION_THRESHOLD
# 昇格しても下書きのみ（§10-9）: 見立て・法的判断（推奨時期・判決確定後の方針は compose の分類では「法的判断・見通し」に入る）
DRAFT_ONLY_CATEGORIES = frozenset({"時効見立て_条件付き", "法的判断・見通し", "緊急対応", "クレーム・不満",
                                   "解約・辞任関係", "本人確認不能・第三者", "費用交渉・減額相談"})
MAX_TEXT_CHARS = 2000

jikou_reply_text = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("text_id", sa.Text, primary_key=True),                # J-U1, J-U2, …
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("category", sa.Text, nullable=False),
    sa.Column("text", sa.Text, nullable=False),                     # 匿名化済み（手本の human_reply）
    sa.Column("text_version", sa.Text, nullable=False),             # sha256
    sa.Column("source_exemplar_id", _BIG, nullable=True),
    sa.Column("status", sa.Text, nullable=False, server_default=STATUS_ACTIVE),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("status IN ('active', 'retired')", name="ck_jikou_reply_text_status"),
    sa.Index("ix_jikou_reply_text_cat", "business", "category", "status"),
)

jikou_reply_text_history = sa.Table(
    HISTORY_TABLE_NAME, metadata,
    sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("text_id", sa.Text, sa.ForeignKey("jikou_reply_text.text_id"), nullable=False),
    sa.Column("action", sa.Text, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("table_version", sa.Text, nullable=False),            # 操作後のデータ表の版（sha256）
    sa.Column("note", sa.Text, nullable=True),
    sa.CheckConstraint("action IN ('promoted', 'retired', 'reactivated')", name="ck_jikou_reply_text_history_action"),
    sa.Index("ix_jikou_reply_text_history_t", "text_id"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _db_configured() -> bool:
    try:
        db.database_url()
        return True
    except DatabaseNotConfigured:
        return False


def _iso(v) -> str:
    return v.isoformat() if isinstance(v, datetime.datetime) else (str(v) if v else "")


def _row(r) -> dict:
    d = dict(r._mapping)
    for k in ("created_at", "updated_at"):
        d[k] = _iso(d.get(k))
    return d


def compute_version(rows: list[dict]) -> str:
    """データ表の版: active 行（text_id・category・text_version）の正規化 JSON の sha256（純関数）。"""
    body = sorted([{"id": r["text_id"], "category": r["category"], "v": r["text_version"]}
                   for r in rows if r.get("status") == STATUS_ACTIVE], key=lambda x: x["id"])
    return hashlib.sha256(json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def next_id(existing_ids) -> str:
    n = 0
    for i in existing_ids or ():
        m = _ID_RE.fullmatch(str(i))
        if m:
            n = max(n, int(m.group(1)))
    return f"{ID_PREFIX}{n + 1}"


async def _all_rows(s, business: str) -> list[dict]:
    rows = (await s.execute(sa.select(jikou_reply_text).where(jikou_reply_text.c.business == business)
                            .order_by(jikou_reply_text.c.created_at.asc(), jikou_reply_text.c.text_id.asc()))).fetchall()
    return [_row(r) for r in rows]


async def list_texts(business: str = BUSINESS, *, include_retired: bool = True) -> list[dict]:
    async with session_scope() as s:
        rows = await _all_rows(s, business)
    return [r for r in rows if include_retired or r["status"] == STATUS_ACTIVE]


async def table_version(business: str = BUSINESS) -> str:
    async with session_scope() as s:
        return compute_version(await _all_rows(s, business))


async def category_map(business: str = BUSINESS) -> dict:
    """category → active な昇格定型 ID の列（新しい昇格順）。DB 未設定・例外は {}（選択器は Q2 の表だけ）。"""
    if not _db_configured():
        return {}
    try:
        rows = await list_texts(business, include_retired=False)
    except Exception:
        logger.warning("[PROMOTE] category map unavailable (fixed reason)")
        return {}
    out: dict = {}
    for r in reversed(rows):
        out.setdefault(r["category"], []).append(r["text_id"])
    return out


async def active_text(text_id: str, business: str = BUSINESS) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(jikou_reply_text).where(
            jikou_reply_text.c.text_id == str(text_id), jikou_reply_text.c.business == business,
            jikou_reply_text.c.status == STATUS_ACTIVE))).first()
    return _row(r) if r else None


async def pick_for_category(category: str, business: str = BUSINESS) -> dict | None:
    """選択器の入口: category の active な昇格定型のうち最新 1 件（下書きのみの分類は None）。"""
    if not category or category in DRAFT_ONLY_CATEGORIES:
        return None
    ids = (await category_map(business)).get(category) or []
    if not ids:
        return None
    try:
        return await active_text(ids[0], business)
    except Exception:
        return None


# ── 候補 ────────────────────────────────────────────────────────────────────────
async def candidate_categories(business: str = BUSINESS) -> dict:
    """無修正承認回数が閾値以上の category → 回数（下書きのみの分類は除く）。"""
    if not _db_configured():
        return {}
    out: dict = {}
    try:
        async with session_scope() as s:
            rows = (await s.execute(
                sa.select(reply_exemplar.reply_draft_approval.c.category, sa.func.count())
                .where(reply_exemplar.reply_draft_approval.c.business == business,
                       reply_exemplar.reply_draft_approval.c.unmodified.is_(True))
                .group_by(reply_exemplar.reply_draft_approval.c.category))).fetchall()
    except Exception:
        logger.warning("[PROMOTE] candidate query failed (fixed reason)")
        return {}
    for cat, n in rows:
        if cat and cat not in DRAFT_ONLY_CATEGORIES and int(n or 0) >= PROMOTION_THRESHOLD:
            out[str(cat)] = int(n)
    return out


async def promoted_source_ids(business: str = BUSINESS) -> set:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(jikou_reply_text.c.source_exemplar_id).where(
            jikou_reply_text.c.business == business, jikou_reply_text.c.source_exemplar_id.isnot(None)))).fetchall()
    return {int(r[0]) for r in rows}


def is_candidate(exemplar: dict, candidates: dict, promoted_sources: set) -> bool:
    """手本が「定型に昇格」できる条件（純関数）: 承認済み（status=approved・active）・分類あり・閾値到達 category・未昇格。
    JIKOU-REPLY-Q1b-fix2 BR-10: 確認待ち（pending_review）の手本は人が匿名化を確認するまで候補にしない。"""
    if not exemplar or not exemplar.get("active"):
        return False
    if exemplar.get("status", reply_exemplar.STATUS_APPROVED) != reply_exemplar.STATUS_APPROVED:
        return False
    cat = exemplar.get("category_id")
    if not cat or cat in DRAFT_ONLY_CATEGORIES or cat not in (candidates or {}):
        return False
    return int(exemplar.get("id") or 0) not in (promoted_sources or set())


# ── 昇格・退役（人の操作・履歴あり） ──────────────────────────────────────────────
async def promote(exemplar_id: int, *, actor: str = ACTOR_HUMAN, business: str = BUSINESS, now=None) -> str | None:
    """候補の手本を定型表に追加（新 ID）。候補でなければ None。戻り値は新 ID。"""
    if not _db_configured():
        return None
    ex = await reply_exemplar.get_exemplar(int(exemplar_id))
    cands = await candidate_categories(business)
    done = await promoted_source_ids(business)
    if not is_candidate(ex, cands, done):
        return None
    text = str(ex.get("human_reply") or "").strip()[:MAX_TEXT_CHARS]
    if not text:
        return None
    now = now or _now()
    async with session_scope() as s:
        rows = await _all_rows(s, business)
        tid = next_id([r["text_id"] for r in rows])
        tv = hashlib.sha256(text.encode("utf-8")).hexdigest()
        await s.execute(sa.insert(jikou_reply_text).values(
            text_id=tid, business=business, category=str(ex["category_id"]), text=text, text_version=tv,
            source_exemplar_id=int(exemplar_id), status=STATUS_ACTIVE, created_at=now, updated_at=now))
        version = compute_version(rows + [{"text_id": tid, "category": str(ex["category_id"]), "text_version": tv,
                                           "status": STATUS_ACTIVE}])
        await s.execute(sa.insert(jikou_reply_text_history).values(
            text_id=tid, action=ACTION_PROMOTED, actor=actor, at=now, table_version=version,
            note=f"source_exemplar={int(exemplar_id)};count={int(cands.get(ex['category_id'], 0))}"))
    logger.info("[PROMOTE] promoted count_active=%s", emit(len([r for r in rows if r['status'] == STATUS_ACTIVE]) + 1,
                                                          "count", "log", "operator"))
    return tid


async def _transition(text_id: str, to_status: str, action: str, *, actor: str, business: str, now=None) -> bool:
    if not _ID_RE.fullmatch(str(text_id)):
        return False
    now = now or _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(jikou_reply_text).where(
            jikou_reply_text.c.text_id == str(text_id), jikou_reply_text.c.business == business,
            jikou_reply_text.c.status != to_status).values(status=to_status, updated_at=now))
        if int(r.rowcount or 0) != 1:
            return False
        version = compute_version(await _all_rows(s, business))
        await s.execute(sa.insert(jikou_reply_text_history).values(
            text_id=str(text_id), action=action, actor=actor, at=now, table_version=version))
    return True


async def retire(text_id: str, *, actor: str = ACTOR_HUMAN, business: str = BUSINESS) -> bool:
    return await _transition(text_id, STATUS_RETIRED, ACTION_RETIRED, actor=actor, business=business)


async def reactivate(text_id: str, *, actor: str = ACTOR_HUMAN, business: str = BUSINESS) -> bool:
    return await _transition(text_id, STATUS_ACTIVE, ACTION_REACTIVATED, actor=actor, business=business)


async def history(text_id: str) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(jikou_reply_text_history).where(
            jikou_reply_text_history.c.text_id == str(text_id)).order_by(jikou_reply_text_history.c.history_id))).fetchall()
    return [{"action": r.action, "actor": r.actor, "at": _iso(r.at), "table_version": r.table_version, "note": r.note}
            for r in rows]
