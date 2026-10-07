"""send_ledger — JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他（§10-5・§12 Q1a）

正本: Desktop/claude/時効LINEボット_返信規則_v1.4.md §10-1・§10-5・§10-6（表定義のみ）・
§10-10・§12 Q1a。本票は「既存の許可判断を維持した接続」——相談者から見える挙動は不変で、
既存の全送信経路（承認 webhook・画像受領・画像読取・即時定型と PENDING_REPLY・ヒアリング・
follow・受付番号）に **記録と排他だけ** を足す。

構成:
- 表: conversation（会話）・send_operation（送信操作記録）・send_operation_history（状態遷移
  履歴・RV-08: 削除せず追加）。migration は alembic の明示コマンドのみ（起動時に走らせない）。
- 送信フック: hub/line_channel の 2 プリミティブ（reply_with_push_fallback / push_text）が
  `begin()` → [LINE push] → `finish()` を呼ぶ。各送信は
  [pending で記録 → 共通排他の中で started → push → sent / unconfirmed / failed] の順。
- 文脈: 用途・主体・受信イベント ID は ContextVar で束ねる（`bind_inbound` / `purpose` /
  `approved_draft`）。束ねが無い送信は 用途=reply・主体=bot・受信イベント ID=NULL。
- 共通排他（§10-5）: 業務・line_user_hash・チャネル単位。Postgres は
  `pg_advisory_xact_lock`（トランザクション終了で解放）、sqlite はテスト用の process 内
  asyncio.Lock。会話の作成（30 日超の無受信で新会話・受付時刻で判定・同一 userId で同時に
  2 つ作らない）と送信着手の確定（started）はこの排他の中で行い、LINE push の待機中は
  保持しない（着手確定＝commit で解放）。
- 再配送: 同じ受信イベント ID＋用途＋連番の送信操作が started/sent/unconfirmed/pending で
  既にあれば二重送信しない（DUPLICATE）。inbound_event（durable lane）は「受信配送の受付と
  処理所有権」、本記録は「送信段階の二重送信遮断」＝分担（両方が効いても二重にならない）。
- 送信確認待ち（unconfirmed）: push の例外（結果不明）で置く。自動再送はしない。人が
  `confirm_by_human()` で sent / failed に確定し、理由を履歴に残す（PWA: hub/webapp_send_ops_view）。
- 対応中（§10-1・R-J11）: 表と遷移関数（set_attending / end_attending）のみ。既存経路は参照
  しない（参照は Q1c・画面は Q1b）。
- 停止判定不能の分離（§2・§10-2）: `decide_send()` を純関数で実装するが既存経路には接続しない
  （flag JIKOU_SEND_POLICY_V2=0 既定・本票は `policy_v2_enabled()` の読取のみ）。

fail-open の裁定（本票の仮置き・Q3a で見直し）: DB 未設定（DATABASE_URL なし）は記録を素通り、
DB 例外時は送信を止めず記録失敗を固定語彙でログに出す＝相談者から見える挙動を不変に保つ。

RV-10: 本表・ログ・例外に本文・氏名・LINE userId を載せない。line_user_hash は一方向
（sha256）。文面は hash（text_version）だけを保存する。
"""

import asyncio
import contextlib
import contextvars
import datetime
import hashlib
import logging
import os
import uuid
import weakref
from dataclasses import dataclass, field, replace

import sqlalchemy as sa

from hub import db
from hub.db import DatabaseNotConfigured, session_scope

logger = logging.getLogger("hub.send_ledger")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

# ── 閉集合 ──────────────────────────────────────────────────────────────────
ACTOR_BOT = "bot"
ACTOR_HUMAN = "human"
ACTOR_APPROVED_DRAFT = "approved_draft"
ACTORS = (ACTOR_BOT, ACTOR_HUMAN, ACTOR_APPROVED_DRAFT)

PURPOSE_REPLY = "reply"
PURPOSE_FIRST_REPLY = "first_reply"
PURPOSE_URGENT = "urgent"
PURPOSE_IMAGE_RECEIPT = "image_receipt"
PURPOSE_IMAGE_RESULT = "image_result"
PURPOSE_FOLLOW = "follow"
PURPOSE_RECEIPT_NUMBER = "receipt_number"
PURPOSE_OTHER = "other"
PURPOSES = (PURPOSE_REPLY, PURPOSE_FIRST_REPLY, PURPOSE_URGENT, PURPOSE_IMAGE_RECEIPT,
            PURPOSE_IMAGE_RESULT, PURPOSE_FOLLOW, PURPOSE_RECEIPT_NUMBER, PURPOSE_OTHER)
# §8(e): follow あいさつは「初回返信判定の対象」外
FIRST_REPLY_EXCLUDED = frozenset({PURPOSE_FOLLOW})

STATE_PENDING = "pending"
STATE_STARTED = "started"
STATE_SENT = "sent"
STATE_UNCONFIRMED = "unconfirmed"
STATE_FAILED = "failed"
STATES = (STATE_PENDING, STATE_STARTED, STATE_SENT, STATE_UNCONFIRMED, STATE_FAILED)
# 同じ受信イベント・用途・連番の既存操作がこの状態なら二重送信しない
_BLOCKING_STATES = frozenset({STATE_PENDING, STATE_STARTED, STATE_SENT, STATE_UNCONFIRMED})

REASON_CREATED = "created"
REASON_STARTED = "started"
REASON_SENT = "sent"
REASON_FAILED = "failed"
REASON_UNCONFIRMED = "unconfirmed"
REASON_RETRY_AFTER_FAILED = "retry_after_failed"
# 人の確定理由（PWA の閉集合）
HUMAN_REASONS = ("line_delivered", "line_not_delivered", "customer_confirmed", "other")
HISTORY_REASONS = (REASON_CREATED, REASON_STARTED, REASON_SENT, REASON_FAILED,
                   REASON_UNCONFIRMED, REASON_RETRY_AFTER_FAILED) + tuple(
                       "human:" + r for r in HUMAN_REASONS)

CONVERSATION_GAP = datetime.timedelta(days=30)
POLICY_V2_ENV = "JIKOU_SEND_POLICY_V2"

# ── 表 ──────────────────────────────────────────────────────────────────────
conversation = sa.Table(
    "conversation", metadata,
    sa.Column("conversation_id", _BIG, primary_key=True, autoincrement=True),
    # 不透明な参照 ID（PWA・通知用。LINE userId・氏名を載せない）
    sa.Column("ref", sa.Text, nullable=False, unique=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("line_user_hash", sa.Text, nullable=False),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("version", _BIG, nullable=False, server_default="1"),
    sa.Column("attending", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Column("attending_since", sa.DateTime(timezone=True), nullable=True),
    sa.Column("attending_until", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_inbound_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Index("ix_conversation_user", "business", "line_user_hash", "started_at"),
)

send_operation = sa.Table(
    "send_operation", metadata,
    sa.Column("op_id", sa.Text, primary_key=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("channel", sa.Text, nullable=False),
    sa.Column("conversation_id", _BIG, sa.ForeignKey("conversation.conversation_id"),
              nullable=False),
    sa.Column("conversation_version", _BIG, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("purpose", sa.Text, nullable=False),
    sa.Column("first_reply_target", sa.Boolean, nullable=False),
    # 文面の版（本文は保存しない）: 定型 ID 列+差し込み値のハッシュ、または自由文のハッシュ
    sa.Column("text_version", sa.Text, nullable=False),
    sa.Column("inbound_event_id", sa.Text, nullable=True),
    sa.Column("inbound_seq", sa.Integer, nullable=False, server_default="1"),
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("completed_after_human", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="1"),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("confirmed_by", sa.Text, nullable=True),
    sa.CheckConstraint("actor IN ('bot', 'human', 'approved_draft')",
                       name="ck_send_operation_actor"),
    sa.CheckConstraint(
        "purpose IN ('reply', 'first_reply', 'urgent', 'image_receipt', 'image_result', "
        "'follow', 'receipt_number', 'other')", name="ck_send_operation_purpose"),
    sa.CheckConstraint("state IN ('pending', 'started', 'sent', 'unconfirmed', 'failed')",
                       name="ck_send_operation_state"),
    # 同じ受信イベント・用途・連番は 1 操作（再配送の二重送信遮断。NULL は対象外）
    sa.UniqueConstraint("business", "channel", "inbound_event_id", "purpose", "inbound_seq",
                        name="uq_send_operation_inbound"),
    sa.Index("ix_send_operation_conversation", "conversation_id"),
    sa.Index("ix_send_operation_state", "state"),
)

send_operation_history = sa.Table(
    "send_operation_history", metadata,
    sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("op_id", sa.Text, sa.ForeignKey("send_operation.op_id"), nullable=False),
    sa.Column("from_state", sa.Text, nullable=True),
    sa.Column("to_state", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Index("ix_send_operation_history_op", "op_id"),
)

TABLE_NAMES = ("conversation", "send_operation", "send_operation_history")


# ── 文脈（ContextVar） ─────────────────────────────────────────────────────────
@dataclass
class SendContext:
    inbound_event_id: str | None = None
    received_at: datetime.datetime | None = None
    purpose: str | None = None
    actor: str = ACTOR_BOT
    op_id: str | None = None
    seq: list = field(default_factory=lambda: [0])   # 受信イベント内の送信連番（共有）


_ctx: contextvars.ContextVar = contextvars.ContextVar("send_ledger_ctx", default=None)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def bind_inbound(event_id: str | None, received_at: datetime.datetime | None = None):
    """受信イベントの文脈を束ねる（durable lane の event id・受付時刻）。戻り値は
    `unbind()` に渡す token。event_id が空なら受信イベント ID なしの文脈。"""
    ctx = SendContext(inbound_event_id=(str(event_id) if event_id else None),
                      received_at=received_at or _now())
    return _ctx.set(ctx)


def unbind(token) -> None:
    _ctx.reset(token)


def current_context() -> SendContext | None:
    return _ctx.get()


@contextlib.contextmanager
def purpose(name: str):
    """送信の用途を束ねる（閉集合外は other）。同期 context manager。"""
    base = _ctx.get() or SendContext(received_at=None)
    token = _ctx.set(replace(base, purpose=(name if name in PURPOSES else PURPOSE_OTHER)))
    try:
        yield
    finally:
        _ctx.reset(token)


async def with_purpose(name: str, fn, *args, **kwargs):
    """`await with_purpose("receipt_number", send_fn, a, b)` の形で 1 行接続する。"""
    with purpose(name):
        return await fn(*args, **kwargs)


@contextlib.asynccontextmanager
async def approved_draft(op_id: str):
    """承認済み下書きの送信（主体=approved_draft・操作 ID 固定・用途=reply）。"""
    base = _ctx.get() or SendContext(received_at=None)
    token = _ctx.set(replace(base, actor=ACTOR_APPROVED_DRAFT, purpose=PURPOSE_REPLY,
                             op_id=str(op_id), inbound_event_id=None))
    try:
        yield
    finally:
        _ctx.reset(token)


# ── 補助 ────────────────────────────────────────────────────────────────────
def line_user_hash(business: str, user_id: str) -> str:
    """一方向ハッシュ（RV-10: userId を表に置かない）。"""
    return hashlib.sha256(f"{business}:{user_id}".encode("utf-8")).hexdigest()


def text_version(text: str) -> str:
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def _lock_key(business: str, user_hash: str, channel: str) -> int:
    """advisory lock のキー（64bit 符号付き・決定的）。"""
    digest = hashlib.sha256(f"{business}:{user_hash}:{channel}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


def _dialect() -> str:
    return db.get_async_engine().dialect.name


# sqlite 用の process 内排他（イベントループごとに持つ＝テストの asyncio.run 跨ぎでも
# 「別ループに束縛された Lock」にならない。本番は Postgres の advisory lock）
_local_locks: "weakref.WeakKeyDictionary" = weakref.WeakKeyDictionary()


def _local_lock(key: int) -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    per_loop = _local_locks.get(loop)
    if per_loop is None:
        per_loop = {}
        _local_locks[loop] = per_loop
    lock = per_loop.get(key)
    if lock is None:
        lock = asyncio.Lock()
        per_loop[key] = lock
    return lock


@contextlib.asynccontextmanager
async def _exclusive(business: str, user_hash: str, channel: str):
    """共通排他: Postgres は tx 内 advisory lock・それ以外（sqlite）は process 内 Lock。
    yield する session はこの排他の中のトランザクション。commit で解放。"""
    key = _lock_key(business, user_hash, channel)
    if _dialect() == "postgresql":
        async with session_scope() as s:
            await s.execute(sa.text("SELECT pg_advisory_xact_lock(:k)"), {"k": key})
            yield s
        return
    async with _local_lock(key):
        async with session_scope() as s:
            yield s


async def _history(s, op_id: str, from_state, to_state: str, reason: str, now) -> None:
    await s.execute(sa.insert(send_operation_history).values(
        op_id=op_id, from_state=from_state, to_state=to_state, reason=reason, at=now))


async def _get_or_create_conversation(s, business: str, user_hash: str,
                                      received_at, now) -> tuple:
    """排他の中で呼ぶ。戻り値 (conversation_id, version, created)。
    新会話の条件: 無し／直近受信（重複除外済み）から 30 日超（受付時刻で判定）。
    遅着（受付時刻が直近受信より古い）は旧会話を再生成せず現会話に属する。"""
    row = (await s.execute(sa.select(conversation).where(
        conversation.c.business == business, conversation.c.line_user_hash == user_hash)
        .order_by(conversation.c.started_at.desc(), conversation.c.conversation_id.desc())
        .limit(1))).first()
    at = received_at or now
    if row is not None:
        last = row.last_inbound_at
        if last is not None and last.tzinfo is None:
            last = last.replace(tzinfo=datetime.timezone.utc)
        stale = last is not None and at > last + CONVERSATION_GAP
        if not stale:
            if received_at is not None and (last is None or at > last):
                await s.execute(sa.update(conversation).where(
                    conversation.c.conversation_id == row.conversation_id).values(
                    last_inbound_at=at))
            return int(row.conversation_id), int(row.version), False
    res = await s.execute(sa.insert(conversation).values(
        ref=uuid.uuid4().hex, business=business, line_user_hash=user_hash,
        started_at=at, version=1, attending=False,
        last_inbound_at=(at if received_at is not None else None), created_at=now))
    return int(res.inserted_primary_key[0]), 1, True


# ── 送信操作（begin / finish） ────────────────────────────────────────────────
class Duplicate:
    """同じ受信イベント（または同じ操作 ID）の送信操作が既にある＝二重送信しない番兵。"""


DUPLICATE = Duplicate()


@dataclass
class Operation:
    op_id: str
    conversation_id: int
    conversation_version: int
    business: str
    channel: str
    purpose: str
    actor: str


def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


async def begin(business: str, channel: str, user_id: str, text: str):
    """送信着手: pending で記録 → 排他の中で [会話の取得/作成 → 再配送の検出 → started]。
    戻り値: Operation（push へ進む）／DUPLICATE（送らない）／None（記録なし＝素通り:
    DB 未設定・記録失敗の fail-open）。"""
    if not _db_configured():
        return None
    ctx = _ctx.get()
    purpose_ = (ctx.purpose if ctx and ctx.purpose in PURPOSES else PURPOSE_REPLY)
    actor = (ctx.actor if ctx and ctx.actor in ACTORS else ACTOR_BOT)
    event_id = ctx.inbound_event_id if ctx else None
    received_at = ctx.received_at if ctx else None
    seq = 1
    if ctx is not None and event_id:
        ctx.seq[0] += 1
        seq = ctx.seq[0]
    preset_op_id = ctx.op_id if ctx else None
    user_hash = line_user_hash(business, user_id)
    now = _now()
    try:
        async with _exclusive(business, user_hash, channel) as s:
            conv_id, version, _created = await _get_or_create_conversation(
                s, business, user_hash, received_at, now)
            existing = None
            if preset_op_id:
                existing = (await s.execute(sa.select(send_operation).where(
                    send_operation.c.op_id == preset_op_id))).first()
            elif event_id:
                existing = (await s.execute(sa.select(send_operation).where(
                    send_operation.c.business == business,
                    send_operation.c.channel == channel,
                    send_operation.c.inbound_event_id == event_id,
                    send_operation.c.purpose == purpose_,
                    send_operation.c.inbound_seq == seq))).first()
            if existing is not None and existing.state in _BLOCKING_STATES:
                return DUPLICATE
            new_version = version + 1
            await s.execute(sa.update(conversation).where(
                conversation.c.conversation_id == conv_id).values(version=new_version))
            if existing is not None:                      # failed の再試行（削除しない）
                op_id = existing.op_id
                await s.execute(sa.update(send_operation).where(
                    send_operation.c.op_id == op_id).values(
                    state=STATE_STARTED, attempts=int(existing.attempts) + 1,
                    conversation_version=new_version, started_at=now, finished_at=None,
                    text_version=text_version(text)))
                await _history(s, op_id, STATE_FAILED, STATE_STARTED,
                               REASON_RETRY_AFTER_FAILED, now)
            else:
                op_id = preset_op_id or uuid.uuid4().hex
                await s.execute(sa.insert(send_operation).values(
                    op_id=op_id, business=business, channel=channel,
                    conversation_id=conv_id, conversation_version=new_version,
                    actor=actor, purpose=purpose_,
                    first_reply_target=(purpose_ not in FIRST_REPLY_EXCLUDED),
                    text_version=text_version(text), inbound_event_id=event_id,
                    inbound_seq=seq, state=STATE_PENDING, attempts=1, created_at=now))
                await _history(s, op_id, None, STATE_PENDING, REASON_CREATED, now)
                await s.execute(sa.update(send_operation).where(
                    send_operation.c.op_id == op_id).values(
                    state=STATE_STARTED, started_at=now))
                await _history(s, op_id, STATE_PENDING, STATE_STARTED, REASON_STARTED, now)
        return Operation(op_id, conv_id, new_version, business, channel, purpose_, actor)
    except Exception:
        # fail-open（本票の裁定）: 記録できなくても送信は従来どおり。固定語彙のみ
        logger.warning("[SEND_LEDGER] begin failed (record skipped, send continues)")
        return None


async def finish(op: Operation, outcome: str) -> None:
    """送信結果の確定: sent / failed / unconfirmed（push の例外＝結果不明）。
    着手後に会話版が進んでいれば「人の操作後に完了」の印を付ける。"""
    if op is None:
        return
    if outcome not in (STATE_SENT, STATE_FAILED, STATE_UNCONFIRMED):
        outcome = STATE_UNCONFIRMED
    now = _now()
    try:
        async with session_scope() as s:
            ver = (await s.execute(sa.select(conversation.c.version).where(
                conversation.c.conversation_id == op.conversation_id))).scalar()
            after_human = ver is not None and int(ver) != int(op.conversation_version)
            await s.execute(sa.update(send_operation).where(
                send_operation.c.op_id == op.op_id,
                send_operation.c.state == STATE_STARTED).values(
                state=outcome, finished_at=now, completed_after_human=after_human))
            await _history(s, op.op_id, STATE_STARTED, outcome, outcome, now)
    except Exception:
        logger.warning("[SEND_LEDGER] finish failed (outcome not recorded)")


# ── 送信確認待ちの表示と人の確定 ──────────────────────────────────────────────
def _iso(v) -> str:
    if v is None:
        return ""
    if v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return v.isoformat()


async def list_unconfirmed(limit: int = 50) -> list[dict]:
    """unconfirmed の一覧（本文・氏名・LINE userId なし。会話は不透明参照 ID）。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(
            send_operation.c.op_id, send_operation.c.business, send_operation.c.channel,
            send_operation.c.purpose, send_operation.c.actor, send_operation.c.started_at,
            send_operation.c.attempts, conversation.c.ref).join(
            conversation, conversation.c.conversation_id == send_operation.c.conversation_id)
            .where(send_operation.c.state == STATE_UNCONFIRMED)
            .order_by(send_operation.c.started_at.asc()).limit(int(limit)))).fetchall()
    return [{"op_id": r.op_id, "business": r.business, "channel": r.channel,
             "purpose": r.purpose, "actor": r.actor, "started_at": _iso(r.started_at),
             "attempts": int(r.attempts), "conversation_ref": r.ref} for r in rows]


async def count_unconfirmed() -> int:
    async with session_scope() as s:
        return int((await s.execute(sa.select(sa.func.count()).select_from(send_operation)
                                    .where(send_operation.c.state == STATE_UNCONFIRMED))).scalar() or 0)


async def confirm_by_human(op_id: str, outcome: str, reason: str) -> str:
    """unconfirmed → sent / failed（人の確定・理由は閉集合・履歴に残す・自動再送なし）。
    戻り値: "ok" / "not_found" / "not_unconfirmed" / "bad_input"（固定語彙）。"""
    if outcome not in (STATE_SENT, STATE_FAILED) or reason not in HUMAN_REASONS:
        return "bad_input"
    now = _now()
    async with session_scope() as s:
        row = (await s.execute(sa.select(send_operation.c.state).where(
            send_operation.c.op_id == str(op_id)))).first()
        if row is None:
            return "not_found"
        if row.state != STATE_UNCONFIRMED:
            return "not_unconfirmed"
        await s.execute(sa.update(send_operation).where(
            send_operation.c.op_id == str(op_id),
            send_operation.c.state == STATE_UNCONFIRMED).values(
            state=outcome, finished_at=now, confirmed_by=ACTOR_HUMAN))
        await _history(s, str(op_id), STATE_UNCONFIRMED, outcome, "human:" + reason, now)
    return "ok"


async def operation_history(op_id: str) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(send_operation_history).where(
            send_operation_history.c.op_id == str(op_id))
            .order_by(send_operation_history.c.history_id))).fetchall()
    return [{"from": r.from_state, "to": r.to_state, "reason": r.reason, "at": _iso(r.at)}
            for r in rows]


# ── 対応中（表と遷移のみ・Q1b が画面と切替を載せる） ──────────────────────────
async def set_attending(business: str, user_id: str, *, channel: str | None = None,
                        now=None) -> dict:
    """会話を対応中にする（共通排他の中・会話版を進める・会話が無ければ作る）。"""
    return await _attending(business, user_id, True, channel=channel, now=now)


async def end_attending(business: str, user_id: str, *, channel: str | None = None,
                        now=None) -> dict:
    """対応終了（bot 再開）。"""
    return await _attending(business, user_id, False, channel=channel, now=now)


async def _attending(business, user_id, on: bool, *, channel, now) -> dict:
    now = now or _now()
    user_hash = line_user_hash(business, user_id)
    async with _exclusive(business, user_hash, channel or business) as s:
        conv_id, version, _c = await _get_or_create_conversation(s, business, user_hash,
                                                                 None, now)
        values = {"version": version + 1, "attending": on}
        if on:
            values.update(attending_since=now, attending_until=None)
        else:
            values.update(attending_until=now)
        await s.execute(sa.update(conversation).where(
            conversation.c.conversation_id == conv_id).values(**values))
    return {"conversation_id": conv_id, "version": version + 1, "attending": on}


async def conversation_state(business: str, user_id: str) -> dict | None:
    user_hash = line_user_hash(business, user_id)
    async with session_scope() as s:
        row = (await s.execute(sa.select(conversation).where(
            conversation.c.business == business, conversation.c.line_user_hash == user_hash)
            .order_by(conversation.c.started_at.desc(), conversation.c.conversation_id.desc())
            .limit(1))).first()
    if row is None:
        return None
    return {"conversation_id": int(row.conversation_id), "ref": row.ref,
            "version": int(row.version), "attending": bool(row.attending),
            "started_at": _iso(row.started_at), "last_inbound_at": _iso(row.last_inbound_at),
            "attending_since": _iso(row.attending_since),
            "attending_until": _iso(row.attending_until)}


# ── 停止判定不能の分離（§2・§10-2）: 判定関数のみ・既存経路には未接続 ───────────
STOPPED = "stopped"
CLEAR = "clear"
UNKNOWN = "unknown"
CONDITION_VALUES = (STOPPED, CLEAR, UNKNOWN)


def policy_v2_enabled() -> bool:
    """flag JIKOU_SEND_POLICY_V2（既定 OFF）。本票では読取のみ（適用は Q1c）。"""
    return os.environ.get(POLICY_V2_ENV, "").strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Decision:
    allow: bool
    reasons: tuple          # 固定語彙（停止条件名:値）
    stage_independent_only: bool = False


def decide_send(actor: str, *, paused: str = CLEAR, stoplisted: str = CLEAR,
                human_mode: str = CLEAR, attending: bool = False, stage_known: bool = True,
                explicit_confirm: bool = False, reapproved_after_pause: bool = False,
                versions_match: bool = True) -> Decision:
    """§10-2 の許可表（3 主体×8 行・各セル単独評価・一つでも停止なら停止）。
    paused / stoplisted / human_mode は stopped / clear / unknown の 3 値。"""
    if actor not in ACTORS:
        return Decision(False, ("actor:unknown",))
    for name, v in (("paused", paused), ("stoplisted", stoplisted), ("human_mode", human_mode)):
        if v not in CONDITION_VALUES:
            return Decision(False, (f"{name}:invalid",))
    reasons: list = []
    if actor == ACTOR_BOT:
        for name, v in (("paused", paused), ("stoplisted", stoplisted), ("human_mode", human_mode)):
            if v != CLEAR:
                reasons.append(f"{name}:{v}")
        if attending:
            reasons.append("attending:stopped")
        if reasons:
            return Decision(False, tuple(reasons))
        return Decision(True, (), stage_independent_only=not stage_known)
    if actor == ACTOR_HUMAN:
        # 大野の明示送信: 停止中・確認不能は「表示+明示確認」を経た場合のみ可
        for name, v in (("paused", paused), ("stoplisted", stoplisted), ("human_mode", human_mode)):
            if v == UNKNOWN or (v == STOPPED and name == "stoplisted"):
                if not explicit_confirm:
                    reasons.append(f"{name}:{v}:explicit_confirm_required")
        return Decision(not reasons, tuple(reasons))
    # approved_draft
    if paused == STOPPED and not reapproved_after_pause:
        reasons.append("paused:stopped:reapproval_required")
    if paused == UNKNOWN and not explicit_confirm:
        reasons.append("paused:unknown:explicit_operation_required")
    if stoplisted != CLEAR:
        reasons.append(f"stoplisted:{stoplisted}")
    if human_mode == UNKNOWN:
        reasons.append("human_mode:unknown")
    if human_mode == STOPPED and not versions_match:
        reasons.append("human_mode:stopped:version_mismatch")
    if attending and not versions_match:
        reasons.append("attending:version_mismatch")
    return Decision(not reasons, tuple(reasons))
