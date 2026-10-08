"""send_ledger — JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他（§10-5・§12 Q1a）

正本: Desktop/claude/時効LINEボット_返信規則_v1.4.md §10-1・§10-5・§10-6（表定義のみ）・
§10-10・§12 Q1a。本票は「既存の許可判断を維持した接続」——相談者から見える挙動は不変で、
既存の全送信経路（承認 webhook・画像受領・画像読取・即時定型と PENDING_REPLY・ヒアリング・
follow・受付番号）に **記録と排他だけ** を足す。

構成:
- 表: conversation（会話）・send_operation（送信操作記録）・send_operation_history（状態遷移
  履歴・RV-08: 削除せず追加）。migration は alembic の明示コマンドのみ（起動時に走らせない）。
- 受信側（fix1 BQ-03）: 会話の取得/作成と直近受信時刻の更新は `touch_inbound()`——受信の重複
  判定を通過した受信イベントの受付時刻（inbound_event に保存済みの時刻・無ければ受付時の
  現在時刻）で行い、送信の有無と独立。再配送（同じ受信イベント ID）では更新しない。30 日超で
  新会話を作るときは前会話の attending（対応中）を引き継ぐ。
- 送信側: hub/line_channel の 2 プリミティブ（reply_with_push_fallback / push_text）が
  `begin()` → [LINE push] → `finish()` を呼ぶ。各送信は
  [pending で記録 → 共通排他の中で started → push → sent / unconfirmed / failed] の順。
  送信時には会話を作らない（確定済みの直近会話を参照・無ければ会話 ID NULL で記録）。
- 文脈: 用途・主体・受信イベント ID は ContextVar で束ねる（`bind_inbound` / `purpose` /
  `approved_draft`）。束ねが無い送信は 用途=reply・主体=bot・受信イベント ID=NULL。
- 共通排他（§10-5）: 業務・line_user_hash・チャネル単位。Postgres は
  `pg_advisory_xact_lock`（トランザクション終了で解放）。非 Postgres の asyncio.Lock 代替は
  **DATABASE_URL が sqlite のときだけ**許可し、それ以外は例外（本番で誤って使われない担保・
  fix1）。LINE push の待機中は保持しない（着手確定＝commit で解放）。
- 再配送（fix1 BQ-01）: 同じ受信イベント ID＋用途＋連番（または同じ操作 ID）の既存操作が
  sent なら DUPLICATE_SENT（送らない・送信済みとして扱う）、pending/started/unconfirmed なら
  DUPLICATE_UNCONFIRMED（送らない・結果未確認として扱う＝呼び出し元は成功側へ進まない）、
  failed なら同じ操作を attempts+1 で再試行。inbound_event（durable lane）は「受信配送の受付と
  処理所有権」、本記録は「送信段階の二重送信遮断」＝分担。
- 送信確認待ち（unconfirmed）: push の例外（結果不明）で置く。started のまま一定時間
  （SEND_LEDGER_STALE_MINUTES・既定 10 分）更新が無い操作は回収ジョブ（fix1 BQ-02・既存
  scheduler の interval）が unconfirmed に移す（履歴 stale_started）。自動再送はしない。人が
  `confirm_by_human()` で sent / failed に確定し、条件付き UPDATE の更新件数が 1 のときだけ
  履歴を追加して ok（fix1 BQ-05・競合側は履歴なし）。
- 対応中（§10-1・R-J11）: 表と遷移関数（set_attending / end_attending）のみ。人の操作は
  human_version（fix1 BQ-04）を進め、completed_after_human は着手時と完了時の human_version
  の差で判定する（bot 送信同士では立たない）。既存経路は参照しない（参照は Q1c・画面は Q1b）。
- 停止判定不能の分離（§2・§10-2）: `decide_send()` を純関数で実装するが既存経路には接続しない
  （flag JIKOU_SEND_POLICY_V2=0 既定・本票は `policy_v2_enabled()` の読取のみ）。

fix2（Codex BQ-06〜08）:
- BQ-06 試行の所有: send_operation に attempt_no（試行番号）と owner_token（着手ごとに新規）を持ち、
  finish() は「操作 ID かつ started かつ attempt_no・owner_token 一致」の条件付き UPDATE が 1 件の
  ときだけ状態遷移と履歴を書く。一致しない遅着結果は状態を変えず履歴 late_result:{outcome}
  （観測状態付き）だけ残す。滞留回収は started_at の経過に加え、処理全体の期限（deadline_at＝
  着手 + SEND_LEDGER_DEADLINE_MINUTES〔既定 5 分〕）とハートビート（heartbeat()）で稼働中を除く。
  新しい試行の着手で前試行の owner_token は無効になる。
- BQ-07 受信の反映済み判定: 新表 inbound_touch（業務・チャネル・受信イベント ID・初回受付時刻・
  一意）。同じ受信イベント ID は 2 回目以降無視（直前 1 件との比較をやめる）。初回受付時刻は
  inbound_event.received_at（durable）があればそれ、無ければ受付時の現在時刻を**保存して**使う。
  画像・text・follow を同じ経路で統一。
- BQ-08 履歴の遷移元: 人の確定は同一 tx で現在状態を取得（FOR UPDATE・sqlite は rowcount 条件で
  同等）し、started からは「started → unconfirmed（human_direct）→ 確定状態」の 2 段を履歴に残す。

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
# 既存操作がこの状態なら「結果未確認の重複」（fix1 BQ-01）
_UNCONFIRMED_LIKE = frozenset({STATE_PENDING, STATE_STARTED, STATE_UNCONFIRMED})

# 送信フックの 3 値戻り値（reply_with_push_fallback）。push_text は True / None / False
SEND_SENT = "sent"
SEND_UNCONFIRMED = "unconfirmed"
SEND_FAILED = "failed"

REASON_CREATED = "created"
REASON_STARTED = "started"
REASON_SENT = "sent"
REASON_FAILED = "failed"
REASON_UNCONFIRMED = "unconfirmed"
REASON_RETRY_AFTER_FAILED = "retry_after_failed"
REASON_STALE_STARTED = "stale_started"
REASON_HUMAN_DIRECT = "human_direct"            # BQ-08: started からの人の確定の 1 段目
REASON_LATE_RESULT_PREFIX = "late_result:"      # BQ-06: 遅着結果の観測記録（状態は変えない）
# 人の確定理由（PWA の閉集合）
HUMAN_REASONS = ("line_delivered", "line_not_delivered", "customer_confirmed", "other")
HISTORY_REASONS = (REASON_CREATED, REASON_STARTED, REASON_SENT, REASON_FAILED,
                   REASON_UNCONFIRMED, REASON_RETRY_AFTER_FAILED, REASON_STALE_STARTED,
                   REASON_HUMAN_DIRECT) + tuple("human:" + r for r in HUMAN_REASONS) + tuple(
                       REASON_LATE_RESULT_PREFIX + o for o in (STATE_SENT, STATE_FAILED,
                                                              STATE_UNCONFIRMED))

CONVERSATION_GAP = datetime.timedelta(days=30)
POLICY_V2_ENV = "JIKOU_SEND_POLICY_V2"
STALE_MINUTES_ENV = "SEND_LEDGER_STALE_MINUTES"
STALE_STARTED_MINUTES_DEFAULT = 10
DEADLINE_MINUTES_ENV = "SEND_LEDGER_DEADLINE_MINUTES"
DEADLINE_MINUTES_DEFAULT = 5                     # 処理全体の期限（回収閾値より短い）
# fix3 BQ-09: 送信経路の稼働中の印と実効的な全体期限（heartbeat 間隔 < 送信 timeout < deadline < 回収閾値）
HEARTBEAT_SECONDS_ENV = "SEND_LEDGER_HEARTBEAT_SECONDS"
HEARTBEAT_SECONDS_DEFAULT = 60.0
SEND_TIMEOUT_SECONDS_ENV = "SEND_LEDGER_SEND_TIMEOUT_SECONDS"
SEND_TIMEOUT_SECONDS_DEFAULT = 240.0
RECOVER_JOB_NAME = "SEND_LEDGER_RECOVER"
RECOVER_INTERVAL_MINUTES = 5.0

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
    # fix1 BQ-04: 人の操作でのみ進む版（対応中への切替・対応終了）
    sa.Column("human_version", _BIG, nullable=False, server_default="1"),
    sa.Column("attending", sa.Boolean, nullable=False, server_default=sa.false()),
    sa.Column("attending_since", sa.DateTime(timezone=True), nullable=True),
    sa.Column("attending_until", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_inbound_at", sa.DateTime(timezone=True), nullable=True),
    # fix1 BQ-03: 直近に反映した受信イベント ID（再配送で直近受信時刻を更新しない）
    sa.Column("last_inbound_event_id", sa.Text, nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Index("ix_conversation_user", "business", "line_user_hash", "started_at"),
)

send_operation = sa.Table(
    "send_operation", metadata,
    sa.Column("op_id", sa.Text, primary_key=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("channel", sa.Text, nullable=False),
    # fix1 BQ-03: 受信を伴わない送信で会話が無ければ NULL（送信時には会話を作らない）
    sa.Column("conversation_id", _BIG, sa.ForeignKey("conversation.conversation_id"),
              nullable=True),
    sa.Column("conversation_version", _BIG, nullable=True),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("purpose", sa.Text, nullable=False),
    sa.Column("first_reply_target", sa.Boolean, nullable=False),
    # 文面の版（本文は保存しない）: 定型 ID 列+差し込み値のハッシュ、または自由文のハッシュ
    sa.Column("text_version", sa.Text, nullable=False),
    sa.Column("inbound_event_id", sa.Text, nullable=True),
    sa.Column("inbound_seq", sa.Integer, nullable=False, server_default="1"),
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("completed_after_human", sa.Boolean, nullable=False, server_default=sa.false()),
    # fix2 BQ-06: 試行番号・所有者トークン（着手ごとに新規）・処理全体の期限・ハートビート
    sa.Column("attempt_no", sa.Integer, nullable=False, server_default="1"),
    sa.Column("owner_token", sa.Text, nullable=True),
    sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
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

# fix2 BQ-07: 反映済み受信イベント（受信の重複判定・初回受付時刻の正本）
inbound_touch = sa.Table(
    "inbound_touch", metadata,
    sa.Column("touch_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("channel", sa.Text, nullable=False),
    sa.Column("inbound_event_id", sa.Text, nullable=False),
    sa.Column("first_received_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint("business", "channel", "inbound_event_id", name="uq_inbound_touch_event"),
)

TABLE_NAMES = ("conversation", "send_operation", "send_operation_history", "inbound_touch")


# ── 文脈（ContextVar） ─────────────────────────────────────────────────────────
@dataclass
class SendContext:
    inbound_event_id: str | None = None
    purpose: str | None = None
    actor: str = ACTOR_BOT
    op_id: str | None = None
    seq: list = field(default_factory=lambda: [0])   # 受信イベント内の送信連番（共有）


_ctx: contextvars.ContextVar = contextvars.ContextVar("send_ledger_ctx", default=None)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def bind_inbound(event_id: str | None):
    """受信イベントの文脈を束ねる（durable lane の event id）。戻り値は `unbind()` に渡す
    token。event_id が空なら受信イベント ID なしの文脈。会話の更新は `touch_inbound()`。"""
    ctx = SendContext(inbound_event_id=(str(event_id) if event_id else None))
    return _ctx.set(ctx)


def unbind(token) -> None:
    _ctx.reset(token)


def current_context() -> SendContext | None:
    return _ctx.get()


@contextlib.contextmanager
def purpose(name: str):
    """送信の用途を束ねる（閉集合外は other）。同期 context manager。"""
    base = _ctx.get() or SendContext()
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
    base = _ctx.get() or SendContext()
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


# ── 時間設定（fix3 BQ-09・fix4 BQ-11: 生の環境変数の読取と検証済み設定を分離） ────────
def _raw_minutes(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw.isdigit() and int(raw) > 0 else default


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        v = float(raw)
    except ValueError:
        return default
    return v if v > 0 else default


_TIMING_DEFAULTS = {"heartbeat": HEARTBEAT_SECONDS_DEFAULT, "timeout": SEND_TIMEOUT_SECONDS_DEFAULT,
                    "deadline": DEADLINE_MINUTES_DEFAULT * 60.0,
                    "stale": STALE_STARTED_MINUTES_DEFAULT * 60.0}
_timing_warned_for: tuple | None = None     # 1 回だけ警告（同じ不整合設定の間は重複ログを出さない）


def timing_config() -> dict:
    """検証済みの時間設定（秒）。整合（heartbeat < timeout < deadline < 回収閾値）を満たさない
    ときは fail-closed にせず、固定文言の警告（値は出さない・同じ設定の間は 1 回だけ）を出して
    既定値（60 / 240 / 300 / 600 秒）に戻す。**deadline_minutes() / stale_started_minutes() /
    heartbeat_seconds() / send_timeout_seconds() はすべて本関数の補正済み値を返す**ため、
    begin() の deadline_at 保存・回収境界（_stale_cut / _stale_started_where）・heartbeat_task・
    guarded_http が同一の設定を使う（fix4 BQ-11）。"""
    global _timing_warned_for
    hb = _float_env(HEARTBEAT_SECONDS_ENV, HEARTBEAT_SECONDS_DEFAULT)
    to = _float_env(SEND_TIMEOUT_SECONDS_ENV, SEND_TIMEOUT_SECONDS_DEFAULT)
    dl = _raw_minutes(DEADLINE_MINUTES_ENV, DEADLINE_MINUTES_DEFAULT) * 60.0
    st = _raw_minutes(STALE_MINUTES_ENV, STALE_STARTED_MINUTES_DEFAULT) * 60.0
    if hb < to < dl < st:
        _timing_warned_for = None
        return {"heartbeat": hb, "timeout": to, "deadline": dl, "stale": st, "defaulted": False}
    key = (hb, to, dl, st)
    if _timing_warned_for != key:
        logger.warning("[SEND_LEDGER] timing config inconsistent (heartbeat<timeout<deadline<stale "
                       "required); defaults applied")
        _timing_warned_for = key
    return dict(_TIMING_DEFAULTS, defaulted=True)


def stale_started_minutes() -> int:
    """回収閾値（分・補正済み）。"""
    return int(timing_config()["stale"] // 60)


def deadline_minutes() -> int:
    """処理全体の期限（分・補正済み）。begin() が deadline_at に保存する値の元。
    保存済みの deadline_at（設定変更前に着手した行）は再計算せず保存値を尊重する。"""
    return int(timing_config()["deadline"] // 60)


def heartbeat_seconds() -> float:
    return timing_config()["heartbeat"]


def send_timeout_seconds() -> float:
    return timing_config()["timeout"]


def check_timing_config() -> bool:
    """起動時の整合検査（main の末尾から呼ぶ）。不整合は警告して既定値（例外にしない）。
    戻り値=整合していたか。"""
    return not timing_config()["defaulted"]


SUPPORTED_DIALECTS = ("postgresql", "sqlite")


class UnsupportedDialect(RuntimeError):
    """fix1: 非 Postgres の排他代替は sqlite（テスト）だけ。他方言は使わせない。"""


def _dialect() -> str:
    name = db.get_async_engine().dialect.name
    if name not in SUPPORTED_DIALECTS:
        raise UnsupportedDialect("send_ledger_unsupported_dialect")
    return name


def check_dialect_at_startup() -> None:
    """起動時の担保（main の末尾から呼ぶ）: DATABASE_URL が設定されていて方言が
    postgresql / sqlite 以外なら例外（URL の値は出さない）。未設定は何もしない。"""
    try:
        url = db.database_url()
    except DatabaseNotConfigured:
        return
    scheme = url.split("://", 1)[0].split("+", 1)[0]
    if scheme not in SUPPORTED_DIALECTS:
        raise UnsupportedDialect("send_ledger_unsupported_dialect")


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
    """共通排他: Postgres は tx 内 advisory lock・sqlite は process 内 Lock（他方言は例外）。
    yield する session はこの排他の中のトランザクション。commit で解放。"""
    key = _lock_key(business, user_hash, channel)
    dialect = _dialect()
    if dialect == "postgresql":
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


def _aware(v):
    if v is not None and v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return v


async def _latest_conversation(s, business: str, user_hash: str):
    return (await s.execute(sa.select(conversation).where(
        conversation.c.business == business, conversation.c.line_user_hash == user_hash)
        .order_by(conversation.c.started_at.desc(), conversation.c.conversation_id.desc())
        .limit(1))).first()


def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


# ── 受信側: 会話の取得/作成と直近受信時刻（fix1 BQ-03・fix2 BQ-07） ───────────
async def _inbound_received_at(s, event_id: str):
    """inbound_event に保存済みの受付時刻（durable lane）。無ければ None。"""
    try:
        from hub.durable_inbound import line_dedup_key
        from hub.inbound_event import InboundEvent
        row = (await s.execute(sa.select(InboundEvent.received_at).where(
            InboundEvent.dedup_key == line_dedup_key(event_id)))).first()
    except Exception:
        return None
    return _aware(row[0]) if row is not None and row[0] is not None else None


async def _record_touch(s, business: str, channel: str, event_id: str, received_at, now):
    """inbound_touch に反映済み記録を作る。戻り値 (first_received_at, created)。
    既にあれば（再配送）保存済みの初回受付時刻を返し created=False。"""
    row = (await s.execute(sa.select(inbound_touch.c.first_received_at).where(
        inbound_touch.c.business == business, inbound_touch.c.channel == channel,
        inbound_touch.c.inbound_event_id == str(event_id)))).first()
    if row is not None:
        return _aware(row[0]), False
    at = _aware(received_at) or await _inbound_received_at(s, str(event_id)) or now
    await s.execute(sa.insert(inbound_touch).values(
        business=business, channel=channel, inbound_event_id=str(event_id),
        first_received_at=at, created_at=now))
    return at, True


async def touch_inbound(business: str, user_id: str, event_id: str, *,
                        received_at: datetime.datetime | None = None,
                        channel: str | None = None) -> dict | None:
    """受信の重複判定を通過した受信イベントごとに呼ぶ（送信の有無と独立・text/画像/follow 共通）。
    共通排他の中で: inbound_touch に同じ受信イベント ID が既にあれば何もしない（再配送・
    直前 1 件との比較ではなく一意記録で判定）／初回受付時刻は inbound_event.received_at
    （durable）があればそれ、無ければ受付時の現在時刻を保存して使う／直近受信から 30 日超なら
    新会話（前会話の attending を引き継ぐ）／直近受信時刻を進める。
    戻り値は会話の要約（fail-open: DB 未設定・例外は None）。"""
    if not event_id or not _db_configured():
        return None
    user_hash = line_user_hash(business, user_id)
    chan = channel or business
    now = _now()
    try:
        async with _exclusive(business, user_hash, chan) as s:
            at, created_touch = await _record_touch(s, business, chan, str(event_id),
                                                    received_at, now)
            row = await _latest_conversation(s, business, user_hash)
            if not created_touch:
                return {"conversation_id": (int(row.conversation_id) if row is not None else None),
                        "created": False, "updated": False}
            if row is not None:
                last = _aware(row.last_inbound_at)
                if last is None or at <= last + CONVERSATION_GAP:
                    values = {"last_inbound_event_id": str(event_id)}
                    if last is None or at > last:
                        values["last_inbound_at"] = at
                    await s.execute(sa.update(conversation).where(
                        conversation.c.conversation_id == row.conversation_id).values(**values))
                    return {"conversation_id": int(row.conversation_id), "created": False,
                            "updated": True}
            # 新会話（初回、または 30 日超）。前会話の対応中は引き継ぐ
            attending = bool(row.attending) if row is not None else False
            res = await s.execute(sa.insert(conversation).values(
                ref=uuid.uuid4().hex, business=business, line_user_hash=user_hash,
                started_at=at, version=1, human_version=1, attending=attending,
                attending_since=(row.attending_since if (row is not None and attending) else None),
                last_inbound_at=at, last_inbound_event_id=str(event_id), created_at=now))
            return {"conversation_id": int(res.inserted_primary_key[0]), "created": True,
                    "updated": True}
    except UnsupportedDialect:
        raise
    except Exception:
        logger.warning("[SEND_LEDGER] touch_inbound failed (conversation not updated)")
        return None


# ── 送信操作（begin / heartbeat / finish） ─────────────────────────────────────
class _Duplicate:
    """既存操作があるため送らない番兵。"""

    def __init__(self, kind: str):
        self.kind = kind

    def __repr__(self) -> str:
        return f"<Duplicate {self.kind}>"


DUPLICATE_SENT = _Duplicate("sent")                 # 送信済みの重複（成功として扱う）
DUPLICATE_UNCONFIRMED = _Duplicate("unconfirmed")   # 結果未確認の重複（成功として扱わない）


@dataclass
class Operation:
    op_id: str
    conversation_id: int | None
    conversation_version: int | None
    human_version: int | None
    business: str
    channel: str
    purpose: str
    actor: str
    attempt_no: int = 1
    owner_token: str = ""


def _owner_match(op):
    """BQ-06: この試行の所有者だけが状態を進められる条件。"""
    return sa.and_(send_operation.c.op_id == op.op_id,
                   send_operation.c.state == STATE_STARTED,
                   send_operation.c.attempt_no == int(op.attempt_no),
                   send_operation.c.owner_token == op.owner_token)


async def begin(business: str, channel: str, user_id: str, text: str):
    """送信着手: pending で記録 → 排他の中で [直近会話の参照 → 重複の検出 → started]。
    戻り値: Operation（push へ進む）／DUPLICATE_SENT／DUPLICATE_UNCONFIRMED（送らない）／
    None（記録なし＝素通り: DB 未設定・記録失敗の fail-open）。会話は作らない（BQ-03）。
    fix2 BQ-06: 着手ごとに owner_token を新規発行し、failed の再試行は attempt_no+1
    （前試行の所有者トークンは無効になる）。処理全体の期限 deadline_at を置く。"""
    if not _db_configured():
        return None
    ctx = _ctx.get()
    purpose_ = (ctx.purpose if ctx and ctx.purpose in PURPOSES else PURPOSE_REPLY)
    actor = (ctx.actor if ctx and ctx.actor in ACTORS else ACTOR_BOT)
    event_id = ctx.inbound_event_id if ctx else None
    seq = 1
    if ctx is not None and event_id:
        ctx.seq[0] += 1
        seq = ctx.seq[0]
    preset_op_id = ctx.op_id if ctx else None
    user_hash = line_user_hash(business, user_id)
    now = _now()
    owner = uuid.uuid4().hex
    deadline = now + datetime.timedelta(minutes=deadline_minutes())
    try:
        async with _exclusive(business, user_hash, channel) as s:
            row = await _latest_conversation(s, business, user_hash)
            conv_id = int(row.conversation_id) if row is not None else None
            human_version = int(row.human_version) if row is not None else None
            new_version = (int(row.version) + 1) if row is not None else None
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
            if existing is not None and existing.state == STATE_SENT:
                return DUPLICATE_SENT
            if existing is not None and existing.state in _UNCONFIRMED_LIKE:
                return DUPLICATE_UNCONFIRMED
            if row is not None:
                await s.execute(sa.update(conversation).where(
                    conversation.c.conversation_id == conv_id).values(version=new_version))
            if existing is not None:                      # failed の再試行（削除しない）
                op_id = existing.op_id
                attempt_no = int(existing.attempt_no) + 1
                await s.execute(sa.update(send_operation).where(
                    send_operation.c.op_id == op_id).values(
                    state=STATE_STARTED, attempt_no=attempt_no, owner_token=owner,
                    deadline_at=deadline, last_heartbeat_at=now,
                    conversation_id=conv_id, conversation_version=new_version,
                    started_at=now, finished_at=None, text_version=text_version(text)))
                await _history(s, op_id, STATE_FAILED, STATE_STARTED,
                               REASON_RETRY_AFTER_FAILED, now)
            else:
                op_id = preset_op_id or uuid.uuid4().hex
                attempt_no = 1
                await s.execute(sa.insert(send_operation).values(
                    op_id=op_id, business=business, channel=channel,
                    conversation_id=conv_id, conversation_version=new_version,
                    actor=actor, purpose=purpose_,
                    first_reply_target=(purpose_ not in FIRST_REPLY_EXCLUDED),
                    text_version=text_version(text), inbound_event_id=event_id,
                    inbound_seq=seq, state=STATE_PENDING, attempt_no=1, created_at=now))
                await _history(s, op_id, None, STATE_PENDING, REASON_CREATED, now)
                await s.execute(sa.update(send_operation).where(
                    send_operation.c.op_id == op_id).values(
                    state=STATE_STARTED, started_at=now, owner_token=owner,
                    deadline_at=deadline, last_heartbeat_at=now))
                await _history(s, op_id, STATE_PENDING, STATE_STARTED, REASON_STARTED, now)
        return Operation(op_id, conv_id, new_version, human_version, business, channel,
                         purpose_, actor, attempt_no, owner)
    except UnsupportedDialect:
        raise
    except Exception:
        # fail-open（本票の裁定）: 記録できなくても送信は従来どおり。固定語彙のみ
        logger.warning("[SEND_LEDGER] begin failed (record skipped, send continues)")
        return None


async def heartbeat(op, *, now=None) -> bool:
    """稼働中の印（所有者のみ）。戻り値=更新できたか（所有者でなければ False）。"""
    if op is None:
        return False
    now = now or _now()
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(send_operation).where(_owner_match(op)).values(
                last_heartbeat_at=now))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.warning("[SEND_LEDGER] heartbeat failed")
        return False


@contextlib.asynccontextmanager
async def heartbeat_task(op):
    """fix3 BQ-09: HTTP 呼出を await している間、別タスクが heartbeat 間隔ごとに
    heartbeat(op) を呼ぶ。終了時（成功・失敗・例外のいずれでも）に必ず cancel して待つ。
    op が None（記録なし）のときは何もしない。ログは出さない（op ID も出さない・RV-10）。"""
    if op is None:
        yield
        return
    interval = heartbeat_seconds()

    async def _beat():
        try:
            while True:
                await asyncio.sleep(interval)
                await heartbeat(op)
        except asyncio.CancelledError:
            pass

    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


async def guarded_http(op, coro):
    """fix3 BQ-09: 送信プリミティブの HTTP 部分を [heartbeat タスク + 全体期限] で囲む。
    op が None（記録なし＝DB 未設定・fail-open）のときは従来どおり素通し（期限も付けない）。
    期限到達は asyncio.TimeoutError を送出（呼び出し側の既存の例外経路＝unconfirmed）。"""
    if op is None:
        return await coro
    async with heartbeat_task(op):
        return await asyncio.wait_for(coro, timeout=send_timeout_seconds())


async def finish(op, outcome: str) -> str:
    """送信結果の確定: sent / failed / unconfirmed（push の例外＝結果不明）。
    fix2 BQ-06: 所有者（attempt_no・owner_token）が一致し state=started のときだけ遷移と
    履歴を書く（"applied"）。一致しない遅着結果は状態を変えず late_result:{outcome} の
    観測記録（観測状態つき）だけ残す（"late"）。着手後に人の操作（human_version）が入って
    いれば「人の操作後に完了」の印を付ける。戻り値は固定語彙（applied / late / skipped）。"""
    if op is None:
        return "skipped"
    if outcome not in (STATE_SENT, STATE_FAILED, STATE_UNCONFIRMED):
        outcome = STATE_UNCONFIRMED
    now = _now()
    try:
        async with session_scope() as s:
            after_human = False
            if op.conversation_id is not None:
                hv = (await s.execute(sa.select(conversation.c.human_version).where(
                    conversation.c.conversation_id == op.conversation_id))).scalar()
                after_human = (hv is not None and op.human_version is not None
                               and int(hv) != int(op.human_version))
            r = await s.execute(sa.update(send_operation).where(_owner_match(op)).values(
                state=outcome, finished_at=now, completed_after_human=after_human))
            if int(r.rowcount or 0) == 1:
                await _history(s, op.op_id, STATE_STARTED, outcome, outcome, now)
                return "applied"
            observed = (await s.execute(sa.select(send_operation.c.state).where(
                send_operation.c.op_id == op.op_id))).scalar()
            if observed is not None:
                await _history(s, op.op_id, observed, observed,
                               REASON_LATE_RESULT_PREFIX + outcome, now)
            return "late"
    except Exception:
        logger.warning("[SEND_LEDGER] finish failed (outcome not recorded)")
        return "skipped"


# ── 滞留した started の回収（fix1 BQ-02・fix2 BQ-06: 稼働中を除く・自動再送なし） ────
def _stale_cut(now) -> datetime.datetime:
    return now - datetime.timedelta(minutes=stale_started_minutes())


def _stale_started_where(now):
    """滞留 started: 着手から閾値超、かつ処理全体の期限切れ、かつハートビートも途絶。"""
    hb_cut = now - datetime.timedelta(minutes=deadline_minutes())
    return sa.and_(send_operation.c.state == STATE_STARTED,
                   send_operation.c.started_at < _stale_cut(now),
                   sa.or_(send_operation.c.deadline_at.is_(None),
                          send_operation.c.deadline_at < now),
                   sa.or_(send_operation.c.last_heartbeat_at.is_(None),
                          send_operation.c.last_heartbeat_at < hb_cut))


async def recover_stale_started(*, now=None) -> int:
    """滞留 started を unconfirmed に移す（履歴 stale_started）。戻り値=移した件数。
    稼働中（期限内・ハートビート継続）の started は触らない。"""
    now = now or _now()
    moved = 0
    async with session_scope() as s:
        rows = (await s.execute(sa.select(send_operation.c.op_id).where(
            _stale_started_where(now)))).fetchall()
        for (op_id,) in rows:
            r = await s.execute(sa.update(send_operation).where(
                send_operation.c.op_id == op_id, _stale_started_where(now)).values(
                state=STATE_UNCONFIRMED))
            if int(r.rowcount or 0) == 1:
                await _history(s, op_id, STATE_STARTED, STATE_UNCONFIRMED,
                               REASON_STALE_STARTED, now)
                moved += 1
    return moved


async def run_recover_job() -> None:
    """scheduler の interval job 本体（DB 未設定は何もしない・例外は固定語彙で握る）。"""
    if not _db_configured():
        return
    try:
        await recover_stale_started()
    except Exception:
        logger.warning("[SEND_LEDGER] recover job failed (fixed reason)")


def register_recover_job() -> None:
    """既存 scheduler へ interval 登録（flag 不要・main の末尾から呼ぶ・冪等）。"""
    from hub import scheduler as hub_scheduler
    if not hub_scheduler.is_registered(RECOVER_JOB_NAME):
        hub_scheduler.register_interval(RECOVER_JOB_NAME, RECOVER_INTERVAL_MINUTES,
                                        run_recover_job)


# ── 送信確認待ちの表示と人の確定 ──────────────────────────────────────────────
def _iso(v) -> str:
    if v is None:
        return ""
    return _aware(v).isoformat()


def _confirmable_where(now):
    """人の確定を受け付ける操作: unconfirmed、または滞留した started（稼働中は除く）。"""
    return sa.or_(send_operation.c.state == STATE_UNCONFIRMED, _stale_started_where(now))


async def list_unconfirmed(limit: int = 50, *, now=None) -> list[dict]:
    """unconfirmed と滞留 started の一覧（本文・氏名・LINE userId なし。会話は不透明参照 ID）。"""
    now = now or _now()
    async with session_scope() as s:
        rows = (await s.execute(sa.select(
            send_operation.c.op_id, send_operation.c.business, send_operation.c.channel,
            send_operation.c.purpose, send_operation.c.actor, send_operation.c.state,
            send_operation.c.started_at, send_operation.c.attempt_no, conversation.c.ref)
            .select_from(send_operation.outerjoin(
                conversation, conversation.c.conversation_id == send_operation.c.conversation_id))
            .where(_confirmable_where(now))
            .order_by(send_operation.c.started_at.asc()).limit(int(limit)))).fetchall()
    return [{"op_id": r.op_id, "business": r.business, "channel": r.channel,
             "purpose": r.purpose, "actor": r.actor, "state": r.state,
             "stale": r.state == STATE_STARTED, "started_at": _iso(r.started_at),
             "attempt_no": int(r.attempt_no), "conversation_ref": r.ref or ""} for r in rows]


async def count_unconfirmed(*, now=None) -> int:
    now = now or _now()
    async with session_scope() as s:
        return int((await s.execute(sa.select(sa.func.count()).select_from(send_operation)
                                    .where(_confirmable_where(now)))).scalar() or 0)


async def confirm_by_human(op_id: str, outcome: str, reason: str, *, now=None) -> str:
    """unconfirmed（または滞留 started）→ sent / failed（人の確定・自動再送なし）。
    fix2 BQ-08: 同一 tx で現在状態を取得（FOR UPDATE・sqlite は rowcount 条件で同等）し、
    started からは「started → unconfirmed（human_direct）→ 確定状態」の 2 段を履歴に残す。
    BQ-05: 各段とも条件付き UPDATE の更新件数が 1 のときだけ履歴を書き、0 件は
    "already_confirmed"（履歴なし）。戻り値: ok / not_found / already_confirmed / bad_input。"""
    if outcome not in (STATE_SENT, STATE_FAILED) or reason not in HUMAN_REASONS:
        return "bad_input"
    now = now or _now()
    async with session_scope() as s:
        q = sa.select(send_operation.c.state).where(send_operation.c.op_id == str(op_id))
        if _dialect() == "postgresql":
            q = q.with_for_update()
        observed = (await s.execute(q)).scalar()
        if observed is None:
            return "not_found"
        if observed == STATE_STARTED:
            r = await s.execute(sa.update(send_operation).where(
                send_operation.c.op_id == str(op_id), _stale_started_where(now)).values(
                state=STATE_UNCONFIRMED))
            if int(r.rowcount or 0) != 1:
                return "already_confirmed"           # 稼働中、または他者が先に動かした
            await _history(s, str(op_id), STATE_STARTED, STATE_UNCONFIRMED,
                           REASON_HUMAN_DIRECT, now)
        r = await s.execute(sa.update(send_operation).where(
            send_operation.c.op_id == str(op_id),
            send_operation.c.state == STATE_UNCONFIRMED).values(
            state=outcome, finished_at=now, confirmed_by=ACTOR_HUMAN))
        if int(r.rowcount or 0) != 1:
            return "already_confirmed"
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
    """会話を対応中にする（共通排他の中・会話版と human_version を進める）。"""
    return await _attending(business, user_id, True, channel=channel, now=now)


async def end_attending(business: str, user_id: str, *, channel: str | None = None,
                        now=None) -> dict:
    """対応終了（bot 再開）。"""
    return await _attending(business, user_id, False, channel=channel, now=now)


async def _attending(business, user_id, on: bool, *, channel, now) -> dict:
    now = now or _now()
    user_hash = line_user_hash(business, user_id)
    async with _exclusive(business, user_hash, channel or business) as s:
        row = await _latest_conversation(s, business, user_hash)
        if row is None:
            # 人の操作は受信が無くても会話を起こす（Q1b の明示送信・切替の前提）
            res = await s.execute(sa.insert(conversation).values(
                ref=uuid.uuid4().hex, business=business, line_user_hash=user_hash,
                started_at=now, version=1, human_version=1, attending=False, created_at=now))
            conv_id, version, hv = int(res.inserted_primary_key[0]), 1, 1
        else:
            conv_id, version, hv = int(row.conversation_id), int(row.version), int(row.human_version)
        values = {"version": version + 1, "human_version": hv + 1, "attending": on}
        if on:
            values.update(attending_since=now, attending_until=None)
        else:
            values.update(attending_until=now)
        await s.execute(sa.update(conversation).where(
            conversation.c.conversation_id == conv_id).values(**values))
    return {"conversation_id": conv_id, "version": version + 1, "human_version": hv + 1,
            "attending": on}


async def conversation_state(business: str, user_id: str) -> dict | None:
    user_hash = line_user_hash(business, user_id)
    async with session_scope() as s:
        row = await _latest_conversation(s, business, user_hash)
    if row is None:
        return None
    return {"conversation_id": int(row.conversation_id), "ref": row.ref,
            "version": int(row.version), "human_version": int(row.human_version),
            "attending": bool(row.attending),
            "started_at": _iso(row.started_at), "last_inbound_at": _iso(row.last_inbound_at),
            "last_inbound_event_id": row.last_inbound_event_id,
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
