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
CUSTOMER_J0_SENT = "j0_sent"                  # Q1c-fix1 BC-05: 台帳 finish(sent)=applied の後にだけ立つ
CUSTOMER_J0_FAILED = "j0_failed"              # LINE 非 2xx（台帳 failed・送っていない）
CUSTOMER_J0_UNCONFIRMED = "j0_unconfirmed"    # 結果不明（通信例外・late／skipped・戻り値なし）
CUSTOMER_J0_STOPPED = "j0_stopped"            # 許可表・J-0 在庫検査・台帳不可で送らなかった（LINE write 0）
CUSTOMER_STATES = (CUSTOMER_NONE, CUSTOMER_J0_SENT, CUSTOMER_J0_FAILED, CUSTOMER_J0_UNCONFIRMED,
                   CUSTOMER_J0_STOPPED)

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
    # Q1c-fix2 BC-07（migration b7d9f1a3c5e7）: 通知キーの永続化（初回に 1 度だけ算出）と通知の claim（条件付き更新）
    sa.Column("notice_key", sa.Text, nullable=True),
    sa.Column("notify_owner", sa.Text, nullable=True),
    sa.Column("notify_claimed_at", sa.DateTime(timezone=True), nullable=True),
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

# ── JIKOU-REPLY-Q1c-fix1（Codex R-Q1c BC-03）: 対応待ちの保存失敗を受信イベント ID で永続キューに残す ──
#    register() が失敗した受信（route／reason／ハッシュ／No. の閉集合値だけ・本文なし）を別セッションで 1 行置き、
#    回収ジョブ（send_ledger の recover hook）が次回以降に register → 通知 を再処理する。鍵は 業務・チャネル・受信
#    イベント ID（再配送は冪等）。DB 全停止中はこの表にも書けない（固定語彙ログ＋障害通知 1 日 1 回・App 28 の受信
#    記録で人が回収）。migration d4f6a8b0c2e4（a1c3 の次）。処理済み行は消さない（RV-08・state=done）。
RETRY_TABLE_NAME = "reply_pending_retry"
RETRY_TABLE_NAMES = (RETRY_TABLE_NAME,)
RETRY_QUEUED = "queued"
RETRY_DONE = "done"
RETRY_ABANDONED = "abandoned"
RETRY_STATES = (RETRY_QUEUED, RETRY_DONE, RETRY_ABANDONED)
RETRY_MAX_ATTEMPTS = 20          # register の再試行上限（5 分間隔の回収ジョブ × 20 ≒ 100 分）
NOTIFY_RETRY_MAX = 3             # 通知の再試行上限（台帳 key は同じ＝送れても 1 通）
RETRY_ERROR_REGISTER = "register_failed"

reply_pending_retry = sa.Table(
    RETRY_TABLE_NAME, metadata,
    sa.Column("retry_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("channel", sa.Text, nullable=False),
    sa.Column("inbound_event_id", sa.Text, nullable=False),
    sa.Column("conversation_id", _BIG, nullable=True),
    sa.Column("line_user_hash", sa.Text, nullable=False),
    sa.Column("route", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("category", sa.Text, nullable=True),
    sa.Column("approval_record_id", sa.Text, nullable=True),
    sa.Column("state", sa.Text, nullable=False, server_default=RETRY_QUEUED),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_error", sa.Text, nullable=True),
    sa.Column("pending_id", _BIG, nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("notice_key", sa.Text, nullable=True),      # Q1c-fix2 BC-07: キュー → 対応待ち登録 で引き継ぐ通知キー
    sa.CheckConstraint("state IN ('queued', 'done', 'abandoned')", name="ck_reply_pending_retry_state"),
    sa.UniqueConstraint("business", "channel", "inbound_event_id", name="uq_reply_pending_retry_inbound"),
    sa.Index("ix_reply_pending_retry_state", "state", "created_at"),
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
                   now=None, notice_key: str | None = None) -> int | None:
    """対応待ちを 1 行登録して pending_id を返す。同じ受信イベント ID の行が既にあればその pending_id
    （再配送の冪等・保存済みの notice_key はそのまま）。DB 未設定・書込失敗は None（呼出側は顧客へ送らない＝無言のまま・固定語彙ログ）。
    route / reason は閉集合外なら None（登録しない）。fix2 BC-07: notice_key（業務 LINE 通知の台帳 key）は呼出側が初回に算出して渡す。"""
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
                attempts=0, created_at=now, updated_at=now, notice_key=(str(notice_key) if notice_key else None)))
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
    """通知結果の記録。fix2 BC-07: 記録と同時に claim（notify_owner／notify_claimed_at）を解放する。"""
    if value not in NOTIFY_STATES:
        return False
    try:
        return await _set(pending_id, "notify_state", value, RESOLVED_BY_BOT, now=now,
                          extra={"notify_owner": None, "notify_claimed_at": None})
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
            "resolved_by": r.resolved_by or "", "has_inbound_event": bool(r.inbound_event_id),
            "notice_key": r.notice_key or ""}


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


# ══════════════════════════════════════════════════════════════
# JIKOU-REPLY-Q1c-fix1（Codex R-Q1c BC-03〜05）: 永続キュー・通知再試行・固定集合の解除・経過統計
#   - 末尾追記。本文・氏名・LINE userId は表に置かない（閉集合値と件数のみ）。ログは固定語彙＋件数。
# ══════════════════════════════════════════════════════════════
async def enqueue_retry(*, business: str, channel: str, inbound_event_id: str | None, line_user_hash: str,
                        route: str, reason: str, conversation_id: int | None = None, category: str | None = None,
                        approval_record_id: str | None = None, now=None, notice_key: str | None = None) -> bool:
    """register() 失敗時の永続キュー（BC-03）。受信イベント ID が無い経路（非 durable）は置けない（False）。
    既に同じ受信イベント ID の行があれば何もしない（True）。書込失敗は固定語彙ログ・False。
    fix2 BC-07: notice_key（初回に算出した通知キー）を一緒に置き、回収ジョブの register へ引き継ぐ。"""
    if not inbound_event_id or route not in ROUTES or reason not in REASONS:
        return False
    if not _db_configured():
        return False
    now = now or _now()
    try:
        async with session_scope() as s:
            existing = (await s.execute(sa.select(reply_pending_retry.c.retry_id).where(
                reply_pending_retry.c.business == business, reply_pending_retry.c.channel == channel,
                reply_pending_retry.c.inbound_event_id == str(inbound_event_id)))).scalar()
            if existing is not None:
                return True
            await s.execute(sa.insert(reply_pending_retry).values(
                business=business, channel=channel, inbound_event_id=str(inbound_event_id),
                conversation_id=conversation_id, line_user_hash=line_user_hash, route=route, reason=reason,
                category=(category or None),
                approval_record_id=(str(approval_record_id) if approval_record_id else None),
                state=RETRY_QUEUED, attempts=0, last_error=RETRY_ERROR_REGISTER, created_at=now, updated_at=now,
                notice_key=(str(notice_key) if notice_key else None)))
        logger.warning("[REPLY_PENDING] register failed; queued for retry count=%s",
                       emit(1, "count", "log", "operator"))
        return True
    except Exception:
        logger.error("[REPLY_PENDING] retry enqueue failed (receipt not recovered by job) count=%s",
                     emit(1, "count", "log", "operator"))
        return False


def _retry_row(r) -> dict:
    return {"retry_id": int(r.retry_id), "business": r.business, "channel": r.channel,
            "inbound_event_id": r.inbound_event_id,
            "conversation_id": (int(r.conversation_id) if r.conversation_id is not None else None),
            "line_user_hash": r.line_user_hash, "route": r.route, "reason": r.reason,
            "category": r.category or "", "approval_record_id": r.approval_record_id or "",
            "state": r.state, "attempts": int(r.attempts), "last_error": r.last_error or "",
            "pending_id": (int(r.pending_id) if r.pending_id is not None else None),
            "created_at": r.created_at, "updated_at": r.updated_at, "processed_at": r.processed_at,
            "notice_key": r.notice_key or ""}


async def list_retry_queue(limit: int = 50) -> list[dict]:
    """再処理待ち（queued・試行上限未満）を古い順に。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_pending_retry).where(
            reply_pending_retry.c.state == RETRY_QUEUED,
            reply_pending_retry.c.attempts < RETRY_MAX_ATTEMPTS)
            .order_by(reply_pending_retry.c.created_at.asc()).limit(int(limit)))).fetchall()
    return [_retry_row(r) for r in rows]


async def count_retry_queue() -> int:
    async with session_scope() as s:
        return int((await s.execute(sa.select(sa.func.count()).select_from(reply_pending_retry)
                                    .where(reply_pending_retry.c.state == RETRY_QUEUED))).scalar() or 0)


async def retry_done(retry_id: int, pending_id: int, *, now=None) -> bool:
    """再処理成功: queued → done（pending_id を記録・行は残す）。"""
    now = now or _now()
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(reply_pending_retry).where(
                reply_pending_retry.c.retry_id == int(retry_id),
                reply_pending_retry.c.state == RETRY_QUEUED).values(
                state=RETRY_DONE, pending_id=int(pending_id), processed_at=now, updated_at=now))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.error("[REPLY_PENDING] retry_done failed (fixed reason)")
        return False


async def retry_bump(retry_id: int, *, now=None) -> bool:
    """再処理失敗: attempts+1（上限に達したら abandoned・行は残す・固定理由）。"""
    now = now or _now()
    try:
        async with session_scope() as s:
            cur = (await s.execute(sa.select(reply_pending_retry.c.attempts).where(
                reply_pending_retry.c.retry_id == int(retry_id)))).scalar()
            if cur is None:
                return False
            nxt = int(cur) + 1
            values = {"attempts": nxt, "updated_at": now, "last_error": RETRY_ERROR_REGISTER}
            if nxt >= RETRY_MAX_ATTEMPTS:
                values["state"] = RETRY_ABANDONED
                logger.error("[REPLY_PENDING] retry abandoned (max attempts) count=%s",
                             emit(1, "count", "log", "operator"))
            r = await s.execute(sa.update(reply_pending_retry).where(
                reply_pending_retry.c.retry_id == int(retry_id)).values(**values))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.error("[REPLY_PENDING] retry_bump failed (fixed reason)")
        return False


async def list_notify_retry(limit: int = 50, *, max_attempts: int = NOTIFY_RETRY_MAX) -> list[dict]:
    """通知の再試行対象: open かつ notify_state が failed／unavailable で attempts が上限未満（古い順）。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_pending).where(
            reply_pending.c.state == STATE_OPEN,
            reply_pending.c.notify_state.in_((NOTIFY_FAILED, NOTIFY_UNAVAILABLE)),
            reply_pending.c.attempts < int(max_attempts))
            .order_by(reply_pending.c.created_at.asc()).limit(int(limit)))).fetchall()
    return [_row(r) for r in rows]


async def bump_attempts(pending_id: int, *, now=None) -> bool:
    now = now or _now()
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(reply_pending).where(
                reply_pending.c.pending_id == int(pending_id)).values(
                attempts=reply_pending.c.attempts + 1, updated_at=now))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.error("[REPLY_PENDING] bump_attempts failed (fixed reason)")
        return False


async def resolve_ids(pending_ids, *, resolved_by: str, business: str | None = None,
                      line_user_hash: str | None = None, now=None) -> int:
    """BC-04: 送信着手時点で固定した pending_id 集合だけを human_replied にする（open のものだけ・他は触らない）。
    line_user_hash を渡したときはその相談者の行に限る（別人の ID が混じっても閉じない）。戻り値は閉じた件数。"""
    if resolved_by not in (RESOLVED_BY_HUMAN, RESOLVED_BY_APPROVED_DRAFT):
        return 0
    ids = sorted({int(x) for x in (pending_ids or ()) if str(x).isdigit() or isinstance(x, int)})
    if not ids or not _db_configured():
        return 0
    now = now or _now()
    try:
        async with session_scope() as s:
            n = 0
            for pid in ids:
                cond = [reply_pending.c.pending_id == pid, reply_pending.c.state == STATE_OPEN]
                if business is not None:
                    cond.append(reply_pending.c.business == business)
                if line_user_hash is not None:
                    cond.append(reply_pending.c.line_user_hash == line_user_hash)
                r = await s.execute(sa.update(reply_pending).where(*cond).values(
                    state=STATE_HUMAN_REPLIED, resolved_at=now, resolved_by=resolved_by, updated_at=now))
                if int(r.rowcount or 0) == 1:
                    await _hist(s, pid, "state", STATE_OPEN, STATE_HUMAN_REPLIED, resolved_by, now)
                    n += 1
        if n:
            logger.info("[REPLY_PENDING] resolved by human (fixed set) count=%s", emit(n, "count", "log", "operator"))
        return n
    except Exception:
        logger.error("[REPLY_PENDING] resolve (fixed set) failed (fixed reason)")
        return 0


async def open_ids_for_user(business: str, line_user_hash: str) -> list[int]:
    """BC-04: 送信着手時点の返信対象（open の pending_id・古い順）。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_pending.c.pending_id).where(
            reply_pending.c.business == business, reply_pending.c.line_user_hash == line_user_hash,
            reply_pending.c.state == STATE_OPEN).order_by(reply_pending.c.created_at.asc()))).scalars().all()
    return [int(x) for x in rows]


async def age_stats(*, threshold_minutes: int, now=None) -> dict:
    """BC-03 (2): open の件数・閾値（分）超の件数・最長経過（分）。本文・ID は返さない。"""
    now = now or _now()
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_pending.c.created_at).where(
            reply_pending.c.state == STATE_OPEN))).scalars().all()
    ages = []
    for v in rows:
        if v is None:
            continue
        if v.tzinfo is None:
            v = v.replace(tzinfo=datetime.timezone.utc)
        ages.append(max(0, int((now - v).total_seconds() // 60)))
    over = [a for a in ages if a >= int(threshold_minutes)]
    return {"open_count": len(ages), "over_count": len(over), "oldest_minutes": (max(ages) if ages else 0)}


# ══════════════════════════════════════════════════════════════
# JIKOU-REPLY-Q1c-fix2（Codex R-Q1c-fix1 BC-07）／fix3（Codex R-Q1c-fix2 BC-13）: 通知キーの永続化と通知の claim（末尾追記・migration b7d9f1a3c5e7）
#   - notice_key は登録時（初回）に 1 度だけ算出して保存し、再試行・再配送・キュー経由の登録はすべて保存値で通知する。
#   - 通知処理は claim_notify（条件付き更新）を取れた 1 者だけが進む。リース超過（処理中に落ちた）は再取得できる。
#   - fix3 BC-13: 永続キーは **claim と同一トランザクション** で読み、その値だけで送る。読取・更新の例外は送出する（"" に潰さない＝
#     rollback で claim も残らず、呼出側は送らずに回収へ戻す）。列追加前の旧行（NULL）の埋めも同一 tx。
#   - 本文・PII は持たない（key は不透明 ref／ハッシュ先頭 16 文字と JST 日付のみ）。
# ══════════════════════════════════════════════════════════════
NOTIFY_CLAIM_LEASE_SECONDS = 300          # 通知 1 回の処理上限（送信 timeout 240 秒より長く・回収間隔 5 分と同じ）
CLAIM_OK = "ok"                           # 取得できた（この呼出だけが通知を進める）
CLAIM_BUSY = "busy"                       # 他者がリース内で処理中
CLAIM_DONE = "done"                       # 通知は既に sent／throttled／unconfirmed（再通知しない）
CLAIM_NOT_FOUND = "not_found"
CLAIM_RESULTS = (CLAIM_OK, CLAIM_BUSY, CLAIM_DONE, CLAIM_NOT_FOUND)
_NOTIFY_RETRYABLE = (NOTIFY_NONE, NOTIFY_FAILED, NOTIFY_UNAVAILABLE)


async def _notice_key_in_tx(s, pending_id: int):
    """claim と同一トランザクションで永続キーを読む（NULL は None・例外は送出）。"""
    return (await s.execute(sa.select(reply_pending.c.notice_key).where(
        reply_pending.c.pending_id == int(pending_id)))).scalar()


async def notice_key_of(pending_id: int) -> str:
    """保存済みの通知キー（NULL は ""）。fix3 BC-13: DB 例外は **送出** する（"" に潰さない）。送信経路は本関数を使わず、
    claim_notify が同一 tx で返す値だけを使う（表示・テスト用の読取）。"""
    async with session_scope() as s:
        v = await _notice_key_in_tx(s, pending_id)
    return str(v or "")


async def claim_notify(pending_id: int, owner: str, *, lease_seconds: int = NOTIFY_CLAIM_LEASE_SECONDS,
                       now=None, computed_key: str | None = None) -> tuple[str, str]:
    """通知処理の claim（条件付き更新）。notify_state が none／failed／unavailable で、未 claim かリース超過のときだけ
    owner を書けた 1 者が CLAIM_OK。戻り値は (result, key)。
    fix3 BC-13: CLAIM_OK のとき key は **同一トランザクションで読んだ永続キー**。列追加前の旧行（NULL）は computed_key を同一 tx で
    埋めてから読む（既に値があれば上書きしない＝初回値が正）。読取・更新の例外は送出する（rollback＝claim も残らない）。
    取れなかったときの key は ""。"""
    now = now or _now()
    cut = now - datetime.timedelta(seconds=int(lease_seconds))
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_pending).where(
            reply_pending.c.pending_id == int(pending_id),
            reply_pending.c.notify_state.in_(_NOTIFY_RETRYABLE),
            sa.or_(reply_pending.c.notify_owner.is_(None),
                   reply_pending.c.notify_claimed_at.is_(None),
                   reply_pending.c.notify_claimed_at < cut)).values(
            notify_owner=str(owner), notify_claimed_at=now))
        if int(r.rowcount or 0) != 1:
            state = (await s.execute(sa.select(reply_pending.c.notify_state).where(
                reply_pending.c.pending_id == int(pending_id)))).scalar()
            if state is None:
                return CLAIM_NOT_FOUND, ""
            return (CLAIM_BUSY if state in _NOTIFY_RETRYABLE else CLAIM_DONE), ""
        key = await _notice_key_in_tx(s, pending_id)
        if not key and computed_key:
            await s.execute(sa.update(reply_pending).where(
                reply_pending.c.pending_id == int(pending_id),
                reply_pending.c.notice_key.is_(None),
                reply_pending.c.notify_owner == str(owner)).values(notice_key=str(computed_key), updated_at=now))
            key = await _notice_key_in_tx(s, pending_id)
        return CLAIM_OK, str(key or "")


async def release_notify_claim(pending_id: int, owner: str, *, now=None) -> bool:
    """自分の claim だけを解放する（送らずに回収へ戻すとき）。他者の claim は触らない。"""
    now = now or _now()
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(reply_pending).where(
                reply_pending.c.pending_id == int(pending_id),
                reply_pending.c.notify_owner == str(owner)).values(
                notify_owner=None, notify_claimed_at=None, updated_at=now))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.error("[REPLY_PENDING] claim release failed (fixed reason)")
        return False
