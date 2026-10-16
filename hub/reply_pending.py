"""reply_pending — JIKOU-REPLY-Q1c (d): 対応待ち（§10-6・独立状態）の表と遷移

正本: 時効LINEボット_返信規則_v1.4.md R-J1（無言+対応待ち+通知）・R-J11（未処理の対応待ちは会話の区切りを
またいで引き継ぐ）・R-J14（独立した状態・一つの成功で他を閉じない）・§10-6（対応待ち表: キー=業務・チャネル・
受信イベント ID／対応状況／通知／顧客送信の各状態と更新時刻・試行回数）・§10-10（RV-08: 履歴を残す）。
QUEUE-2026-10-12-C C-1 (d)。

本票の範囲（Q3a で表を完成させる前提の最小構成）:
- 表 reply_pending（対応待ち）: 1 行=無言にした 1 受信。キー= 業務・チャネル・受信イベント ID（受信イベント
  ID が無い経路〔非 durable〕は NULL＝一意制約の対象外）。対応状況 state は閉集合
  open（未着手）／human_replied（大野対応済）／auto_replied（自動回答済）／not_needed（不要）。
  通知 notify_state（none／sent／throttled／failed／unconfirmed／unavailable）と顧客送信 customer_send
  （none／j0_sent＝初回 1 通のみ）を独立に持つ。下書き生成・App 29 起票の状態列は Q3a（本票は App 29 の
  レコード No. を approval_record_id に持つだけ）。
- 表 reply_pending_history: 状態遷移の履歴（RV-08・削除しない）。
- 会話（conversation）表は変更しない（e5f8a1b4c7d9 適用済みの表を ALTER しない＝Q1b-fix1 と同じ流儀）。
  conversation_id と line_user_hash で会話に紐づける。
- 解除: 大野の明示送信（/app/reply・Q1b）の成功で、同じ相談者（line_user_hash）の open をすべて
  human_replied にする（§7「既定は未解決の最新」＝本票は同一相談者の未解決すべて）。承認済み下書きの送信
  成功も human_replied（resolved_by=approved_draft）。bot の自動送信は対応待ちを閉じない（R-J14）。
- 画面（/app/pending・hub/webapp_pending_view）から「不要」へ遷移できる（人の操作・理由固定）。
- RV-10: 本文・氏名・LINE userId を表に置かない（line_user_hash は一方向ハッシュ・category と理由は閉集合）。
  ログは固定語彙＋件数のみ。
"""

import datetime
import logging

import sqlalchemy as sa

from hub import db
from hub.db import DatabaseNotConfigured, session_scope
from hub.redact import emit

logger = logging.getLogger("hub.reply_pending")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

TABLE_NAME = "reply_pending"
HISTORY_TABLE_NAME = "reply_pending_history"
TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)

STATE_OPEN = "open"
STATE_HUMAN_REPLIED = "human_replied"
STATE_AUTO_REPLIED = "auto_replied"
STATE_NOT_NEEDED = "not_needed"
STATES = (STATE_OPEN, STATE_HUMAN_REPLIED, STATE_AUTO_REPLIED, STATE_NOT_NEEDED)

ROUTE_CUSTOMER = "customer"
ROUTE_HEARING = "hearing"
ROUTE_INBOUND = "inbound"          # 受信の入口で止めた（許可表・判定不能）
ROUTES = (ROUTE_CUSTOMER, ROUTE_HEARING, ROUTE_INBOUND)

REASON_DEMOTED = "demoted"                   # ガードで承認降格（下書きは App 29）
REASON_EMPTY_REPLY = "empty_reply"           # 空返事の検出（(b)）
REASON_POLICY_STOPPED = "policy_stopped"     # 許可表 V2 で bot 停止（対応中・判定不能）
REASON_OUTAGE = "claude_outage"              # Claude 障害・例外
REASONS = (REASON_DEMOTED, REASON_EMPTY_REPLY, REASON_POLICY_STOPPED, REASON_OUTAGE)

NOTIFY_NONE = "none"
NOTIFY_SENT = "sent"
NOTIFY_THROTTLED = "throttled"
NOTIFY_FAILED = "failed"
NOTIFY_UNCONFIRMED = "unconfirmed"
NOTIFY_UNAVAILABLE = "unavailable"
NOTIFY_STATES = (NOTIFY_NONE, NOTIFY_SENT, NOTIFY_THROTTLED, NOTIFY_FAILED, NOTIFY_UNCONFIRMED,
                 NOTIFY_UNAVAILABLE)

CUSTOMER_NONE = "none"
CUSTOMER_J0_SENT = "j0_sent"
CUSTOMER_STATES = (CUSTOMER_NONE, CUSTOMER_J0_SENT)

RESOLVED_BY_HUMAN = "human"
RESOLVED_BY_APPROVED_DRAFT = "approved_draft"
RESOLVED_BY_BOT = "bot"
RESOLVERS = (RESOLVED_BY_HUMAN, RESOLVED_BY_APPROVED_DRAFT, RESOLVED_BY_BOT)

LIST_LIMIT = 100

reply_pending = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("pending_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("channel", sa.Text, nullable=False),
    sa.Column("inbound_event_id", sa.Text, nullable=True),
    sa.Column("conversation_id", _BIG, nullable=True),
    sa.Column("line_user_hash", sa.Text, nullable=False),
    sa.Column("route", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("category", sa.Text, nullable=True),
    sa.Column("approval_record_id", sa.Text, nullable=True),
    sa.Column("state", sa.Text, nullable=False, server_default=STATE_OPEN),
    sa.Column("notify_state", sa.Text, nullable=False, server_default=NOTIFY_NONE),
    sa.Column("customer_send", sa.Text, nullable=False, server_default=CUSTOMER_NONE),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("resolved_by", sa.Text, nullable=True),
    sa.CheckConstraint("state IN ('open', 'human_replied', 'auto_replied', 'not_needed')",
                       name="ck_reply_pending_state"),
    sa.CheckConstraint("route IN ('customer', 'hearing', 'inbound')", name="ck_reply_pending_route"),
    sa.CheckConstraint("reason IN ('demoted', 'empty_reply', 'policy_stopped', 'claude_outage')",
                       name="ck_reply_pending_reason"),
    sa.UniqueConstraint("business", "channel", "inbound_event_id", name="uq_reply_pending_inbound"),
    sa.Index("ix_reply_pending_state", "state", "created_at"),
    sa.Index("ix_reply_pending_user", "business", "line_user_hash", "state"),
)

reply_pending_history = sa.Table(
    HISTORY_TABLE_NAME, metadata,
    sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("pending_id", _BIG, sa.ForeignKey("reply_pending.pending_id"), nullable=False),
    sa.Column("field", sa.Text, nullable=False),          # state / notify_state / customer_send / approval
    sa.Column("from_value", sa.Text, nullable=True),
    sa.Column("to_value", sa.Text, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Index("ix_reply_pending_history_p", "pending_id"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


def _iso(v) -> str:
    if v is None:
        return ""
    if v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return v.isoformat()


async def _hist(s, pending_id: int, field: str, from_value, to_value: str, actor: str, now) -> None:
    await s.execute(sa.insert(reply_pending_history).values(
        pending_id=int(pending_id), field=field, from_value=from_value, to_value=to_value,
        actor=actor, at=now))


# ── 登録 ─────────────────────────────────────────────────────────────────────
async def register(*, business: str, channel: str, line_user_hash: str, route: str, reason: str,
                   inbound_event_id: str | None = None, conversation_id: int | None = None,
                   category: str | None = None, approval_record_id: str | None = None,
                   now=None) -> int | None:
    """対応待ちを 1 行登録して pending_id を返す。同じ受信イベント ID の行が既にあればその pending_id
    （再配送の冪等）。DB 未設定・書込失敗は None（呼出側は顧客へ送らない＝無言のまま・固定語彙ログ）。
    route / reason は閉集合外なら None（登録しない）。"""
    if route not in ROUTES or reason not in REASONS:
        return None
    if not _db_configured():
        return None
    now = now or _now()
    try:
        async with session_scope() as s:
            if inbound_event_id:
                existing = (await s.execute(sa.select(reply_pending.c.pending_id).where(
                    reply_pending.c.business == business, reply_pending.c.channel == channel,
                    reply_pending.c.inbound_event_id == str(inbound_event_id)))).scalar()
                if existing is not None:
                    return int(existing)
            res = await s.execute(sa.insert(reply_pending).values(
                business=business, channel=channel,
                inbound_event_id=(str(inbound_event_id) if inbound_event_id else None),
                conversation_id=conversation_id, line_user_hash=line_user_hash, route=route,
                reason=reason, category=(category or None),
                approval_record_id=(str(approval_record_id) if approval_record_id else None),
                state=STATE_OPEN, notify_state=NOTIFY_NONE, customer_send=CUSTOMER_NONE,
                attempts=0, created_at=now, updated_at=now))
            pid = int(res.inserted_primary_key[0])
            await _hist(s, pid, "state", None, STATE_OPEN, RESOLVED_BY_BOT, now)
        return pid
    except Exception:
        logger.error("[REPLY_PENDING] register failed (customer stays silent) count=%s",
                     emit(1, "count", "log", "operator"))
        return None


async def _set(pending_id: int, field: str, to_value: str, actor: str, *, where_from=None,
               extra: dict | None = None, now=None) -> bool:
    now = now or _now()
    col = reply_pending.c[field]
    async with session_scope() as s:
        cur = (await s.execute(sa.select(col).where(
            reply_pending.c.pending_id == int(pending_id)))).scalar()
        if cur is None and field == "state":
            return False
        cond = [reply_pending.c.pending_id == int(pending_id)]
        if where_from is not None:
            cond.append(col.in_(tuple(where_from)))
        values = {field: to_value, "updated_at": now}
        values.update(extra or {})
        r = await s.execute(sa.update(reply_pending).where(*cond).values(**values))
        if int(r.rowcount or 0) != 1:
            return False
        await _hist(s, int(pending_id), field, cur, to_value, actor, now)
    return True


async def set_notify_state(pending_id: int, value: str, *, now=None) -> bool:
    if value not in NOTIFY_STATES:
        return False
    try:
        return await _set(pending_id, "notify_state", value, RESOLVED_BY_BOT, now=now)
    except Exception:
        logger.error("[REPLY_PENDING] notify_state update failed (fixed reason)")
        return False


async def set_customer_send(pending_id: int, value: str, *, now=None) -> bool:
    if value not in CUSTOMER_STATES:
        return False
    try:
        return await _set(pending_id, "customer_send", value, RESOLVED_BY_BOT, now=now)
    except Exception:
        logger.error("[REPLY_PENDING] customer_send update failed (fixed reason)")
        return False


async def attach_approval(pending_id: int, approval_record_id: str, *, now=None) -> bool:
    """後から判明した App 29 レコード No. を付ける（既に付いていれば触らない）。"""
    if not approval_record_id:
        return False
    now = now or _now()
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(reply_pending).where(
                reply_pending.c.pending_id == int(pending_id),
                reply_pending.c.approval_record_id.is_(None)).values(
                approval_record_id=str(approval_record_id), updated_at=now))
            if int(r.rowcount or 0) != 1:
                return False
            await _hist(s, int(pending_id), "approval", None, str(approval_record_id),
                        RESOLVED_BY_BOT, now)
        return True
    except Exception:
        logger.error("[REPLY_PENDING] attach_approval failed (fixed reason)")
        return False


# ── 解除 ─────────────────────────────────────────────────────────────────────
async def resolve_for_user(business: str, line_user_hash: str, *, resolved_by: str,
                           now=None) -> int:
    """同じ相談者の open をすべて閉じる。human（明示送信）・approved_draft → human_replied。
    戻り値は閉じた件数（DB 未設定・失敗は 0・固定語彙ログ）。"""
    if resolved_by not in (RESOLVED_BY_HUMAN, RESOLVED_BY_APPROVED_DRAFT):
        return 0
    if not _db_configured():
        return 0
    now = now or _now()
    try:
        async with session_scope() as s:
            rows = (await s.execute(sa.select(reply_pending.c.pending_id).where(
                reply_pending.c.business == business,
                reply_pending.c.line_user_hash == line_user_hash,
                reply_pending.c.state == STATE_OPEN))).scalars().all()
            n = 0
            for pid in rows:
                r = await s.execute(sa.update(reply_pending).where(
                    reply_pending.c.pending_id == int(pid),
                    reply_pending.c.state == STATE_OPEN).values(
                    state=STATE_HUMAN_REPLIED, resolved_at=now, resolved_by=resolved_by,
                    updated_at=now))
                if int(r.rowcount or 0) == 1:
                    await _hist(s, int(pid), "state", STATE_OPEN, STATE_HUMAN_REPLIED, resolved_by, now)
                    n += 1
        if n:
            logger.info("[REPLY_PENDING] resolved by human count=%s", emit(n, "count", "log", "operator"))
        return n
    except Exception:
        logger.error("[REPLY_PENDING] resolve failed (fixed reason)")
        return 0


async def mark_not_needed(pending_id: int, *, now=None) -> str:
    """人の操作（画面）: open → not_needed。戻り値 ok / not_open / not_found / db。"""
    now = now or _now()
    try:
        async with session_scope() as s:
            cur = (await s.execute(sa.select(reply_pending.c.state).where(
                reply_pending.c.pending_id == int(pending_id)))).scalar()
            if cur is None:
                return "not_found"
            r = await s.execute(sa.update(reply_pending).where(
                reply_pending.c.pending_id == int(pending_id),
                reply_pending.c.state == STATE_OPEN).values(
                state=STATE_NOT_NEEDED, resolved_at=now, resolved_by=RESOLVED_BY_HUMAN, updated_at=now))
            if int(r.rowcount or 0) != 1:
                return "not_open"
            await _hist(s, int(pending_id), "state", STATE_OPEN, STATE_NOT_NEEDED, RESOLVED_BY_HUMAN, now)
        return "ok"
    except Exception:
        logger.error("[REPLY_PENDING] mark_not_needed failed (fixed reason)")
        return "db"


# ── 読取 ─────────────────────────────────────────────────────────────────────
def _row(r) -> dict:
    return {"pending_id": int(r.pending_id), "business": r.business, "channel": r.channel,
            "conversation_id": (int(r.conversation_id) if r.conversation_id is not None else None),
            "line_user_hash": r.line_user_hash, "route": r.route, "reason": r.reason,
            "category": r.category or "", "approval_record_id": r.approval_record_id or "",
            "state": r.state, "notify_state": r.notify_state, "customer_send": r.customer_send,
            "attempts": int(r.attempts), "created_at": _iso(r.created_at),
            "updated_at": _iso(r.updated_at), "resolved_at": _iso(r.resolved_at),
            "resolved_by": r.resolved_by or "", "has_inbound_event": bool(r.inbound_event_id)}


async def get_pending(pending_id: int) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(reply_pending).where(
            reply_pending.c.pending_id == int(pending_id)))).first()
    return _row(r) if r is not None else None


async def list_open(limit: int = LIST_LIMIT) -> list[dict]:
    """未処理（open）の一覧（古い順）。本文・氏名・LINE userId なし。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_pending).where(
            reply_pending.c.state == STATE_OPEN)
            .order_by(reply_pending.c.created_at.asc()).limit(int(limit)))).fetchall()
    return [_row(r) for r in rows]


async def count_open() -> int:
    async with session_scope() as s:
        return int((await s.execute(sa.select(sa.func.count()).select_from(reply_pending)
                                    .where(reply_pending.c.state == STATE_OPEN))).scalar() or 0)


async def open_for_user(business: str, line_user_hash: str) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_pending).where(
            reply_pending.c.business == business, reply_pending.c.line_user_hash == line_user_hash,
            reply_pending.c.state == STATE_OPEN).order_by(reply_pending.c.created_at.asc()))).fetchall()
    return [_row(r) for r in rows]


async def history(pending_id: int) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_pending_history).where(
            reply_pending_history.c.pending_id == int(pending_id))
            .order_by(reply_pending_history.c.history_id))).fetchall()
    return [{"field": r.field, "from": r.from_value, "to": r.to_value, "actor": r.actor,
             "at": _iso(r.at)} for r in rows]
