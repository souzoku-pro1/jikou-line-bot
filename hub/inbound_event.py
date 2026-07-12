"""InboundEvent — inbound webhook の durable journal（P1-005a・§9.17/§17.2）

第1弾は Stripe（provider="stripe"）。設計判断（P1-005a票）:
- D8: **raw payload 本体は保存しない**（O-06/O-32 の PII/retention 裁定まで。
  保存するのは external_event_id・payload の SHA-256・event_type 等の
  最小抽出フィールドのみ。顧客名・メール等の PII をカラムに持たない）
- dedup_key = "<provider>:<external_event_id>"。external id を持たない
  provider は "<provider>:sha256:<payload hash>"（UNIQUE 制約が重複検知の実体）
- 状態機械: processing → done / failed。
  再送（dedup_key 衝突）の扱い:
    - 既存 state=done      → skipped_duplicate（業務処理を走らせない・attempts+1）
    - 既存 state=failed    → reprocess（claim して再実行。D7 の 5xx→Stripe再送と対）
    - 既存 state=processing:
        claimed_at が STALE_PROCESSING_MINUTES（env・既定15分）以内 →
          **in_progress**（呼び出し側は 503 を返し Stripe の再送を維持する。
          P1-005c・D14: 200 で飲むと「INSERT後クラッシュ→再送停止→永久未処理」
          の経路ができるため。真に処理中なら完了後の再送が done→200 skip になり、
          クラッシュ済みなら再送が続いて 15 分経過後の配送が stale 再claimで回収する
          ——「回収を起動する主体 = Stripe 再送」が構造的に保たれる。
          Stripe の再送は指数バックオフで最大3日間継続するため 15 分窓を確実に跨ぐ）
        claimed_at が超過 or NULL（列追加前の行）→ **stale とみなし再claim**
          （P1-005b・RCF-M06 解消）
  claim は全て条件付き UPDATE + RETURNING（並行競合で勝者は1つ）
- D13 裁定（P1-005b票で確定）: 「未処理の闇損失」を「まれな二重処理」より
  重く見る。stale 再claim による業務処理の二重実行は「クラッシュ後の再送」に
  限定され（15分窓＋条件付きUPDATE）、kintone 側で同一決済IDの二重起票が
  起きた場合も App 21 の Stripe決済ID 検索で人が気付ける。この方向にのみ倒す
- DB 到達不能時は例外をそのまま送出（D7: 呼び出し側が 5xx を返し
  Stripe の自動リトライに委ねる。memory fallback 禁止）

テーブル追加は必ず migration 経由（alembic/versions/…_inbound_event.py）。
"""

import hashlib
import logging
import os
from datetime import datetime, timedelta, timezone

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from hub.db import session_scope
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式）

logger = logging.getLogger("hub.inbound_event")


class Base(DeclarativeBase):
    """アプリのORM metadata の起点（alembic autogenerate の target_metadata）"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class InboundEvent(Base):
    __tablename__ = "inbound_event"

    # sqlite（テスト）では INTEGER PK でないと autoincrement しないため variant
    id: Mapped[int] = mapped_column(
        sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
        primary_key=True, autoincrement=True)
    provider: Mapped[str] = mapped_column(sa.Text, nullable=False)
    external_event_id: Mapped[str | None] = mapped_column(sa.Text)
    caller_id: Mapped[str | None] = mapped_column(sa.Text)
    dedup_key: Mapped[str] = mapped_column(sa.Text, nullable=False, unique=True)
    payload_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    event_type: Mapped[str | None] = mapped_column(sa.Text)
    signature_result: Mapped[str] = mapped_column(sa.Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, default=_utcnow)
    state: Mapped[str] = mapped_column(sa.Text, nullable=False)
    processed_at: Mapped[datetime | None] = mapped_column(
        sa.DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=1)
    # 失敗分類のみ（例外クラス名等）。本文・PII・vendor生値は入れない（RCF-M05流儀）
    last_error: Mapped[str | None] = mapped_column(sa.Text)
    # 処理権を取った時刻（INSERT時・claim時に更新）。stale判定の基準（P1-005b・D12）
    claimed_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))


def payload_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def stripe_dedup_key(event: dict, payload: bytes) -> str:
    """Stripe は event id（evt_…）がグローバル一意。無い場合は hash に縮退"""
    event_id = str(event.get("id") or "")
    if event_id:
        return f"stripe:{event_id}"
    return f"stripe:sha256:{payload_sha256(payload)}"


def stale_processing_minutes() -> int:
    """processing を stale とみなす閾値（env STALE_PROCESSING_MINUTES・既定15分）"""
    raw = os.environ.get("STALE_PROCESSING_MINUTES", "")
    try:
        minutes = int(raw)
    except ValueError:
        return 15
    return minutes if minutes > 0 else 15


async def record_stripe_event(event: dict, payload: bytes) -> tuple[str, int | None]:
    """Stripe event を journal に記録し、処理可否を返す。

    Returns:
        ("new", pk)                — 新規。業務処理へ進む
        ("reprocess", pk)          — failed の再送 or stale processing の再claim。
                                      業務処理へ進む（呼び出し側は D15 の
                                      reconciliation を先に行うこと）
        ("skipped_duplicate", None) — done 済みの重複。業務処理を走らせない（200）
        ("in_progress", None)       — 実行中（15分以内の processing）の重複。
                                      呼び出し側は 503 で Stripe 再送を維持（D14）
    DB 到達不能・未設定は例外送出（D7: 上位で 5xx にする。ここで飲まない）
    """
    dedup_key = stripe_dedup_key(event, payload)
    now = _utcnow()
    row = InboundEvent(
        provider="stripe",
        external_event_id=str(event.get("id") or "") or None,
        caller_id=None,
        dedup_key=dedup_key,
        payload_hash=payload_sha256(payload),
        event_type=str(event.get("type") or "") or None,
        signature_result="verified",  # 呼び出し側は construct_event 成功後のみ来る
        state="processing",
        claimed_at=now,
    )
    try:
        async with session_scope() as session:
            session.add(row)
            await session.flush()
            pk = row.id
        return "new", pk
    except IntegrityError:
        pass  # dedup_key 衝突 = 再送/重複。以降で状態に応じて分岐

    stale_cutoff = now - timedelta(minutes=stale_processing_minutes())
    async with session_scope() as session:
        # (1) failed の行を claim（条件付きUPDATEで競合安全に再処理権を取る）
        claimed = await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.dedup_key == dedup_key,
                   InboundEvent.state == "failed")
            .values(state="processing",
                    attempts=InboundEvent.attempts + 1,
                    claimed_at=now)
            .returning(InboundEvent.id))
        claimed_id = claimed.scalar_one_or_none()
        if claimed_id is not None:
            return "reprocess", claimed_id

        # (2) stale processing の再claim（D12/RCF-M06: クラッシュ滞留の救済。
        #     claimed_at NULL は列追加前の行＝救済対象。15分以内は実行中とみなす）
        claimed = await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.dedup_key == dedup_key,
                   InboundEvent.state == "processing",
                   sa.or_(InboundEvent.claimed_at.is_(None),
                          InboundEvent.claimed_at < stale_cutoff))
            .values(attempts=InboundEvent.attempts + 1,
                    claimed_at=now)
            .returning(InboundEvent.id))
        claimed_id = claimed.scalar_one_or_none()
        if claimed_id is not None:
            # D17: ログは PK のみ（dedup_key・event ID を出さない）
            logger.warning(
                "stale processing row reclaimed pk=%s (RCF-M06)",
                emit(claimed_id, "record_id", "log", "operator"))
            return "reprocess", claimed_id

        # (3) done / 実行中(15分以内)の processing → 再送回数を記録し状態で分岐
        bumped = await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.dedup_key == dedup_key)
            .values(attempts=InboundEvent.attempts + 1)
            .returning(InboundEvent.state))
        state = bumped.scalar_one_or_none()
    if state == "done":
        return "skipped_duplicate", None
    # processing(15分以内) / 競合で状態が動いた直後 / 行消失 —— いずれも
    # 503 側に倒す（D14: 200 で飲まない。Stripe 再送が次の判定機会を作る）
    return "in_progress", None


class JournalRowMissing(RuntimeError):
    """journal 行の消失（起きてはならない異常・fail closed。P1-005c・D16）"""


async def mark_done(event_pk: int) -> None:
    """rowcount=0 は JournalRowMissing（D16: fail closed→上位が5xx）"""
    async with session_scope() as session:
        result = await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.id == event_pk)
            .values(state="done", processed_at=_utcnow(), last_error=None))
        if result.rowcount == 0:
            logger.warning("mark_done: journal row missing pk=%s",
                           emit(event_pk, "record_id", "log", "operator"))
            raise JournalRowMissing(f"mark_done: pk={event_pk}")


async def mark_failed(event_pk: int, error_class: str) -> None:
    """error_class は分類のみ（例外クラス名等・100字で切る）。
    例外本文 str(e) を渡さないこと（call-policy を AST テストで機械強制）。
    rowcount=0 は JournalRowMissing（D16: fail closed→上位が5xx）"""
    async with session_scope() as session:
        result = await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.id == event_pk)
            .values(state="failed", processed_at=_utcnow(),
                    last_error=(error_class or "unknown")[:100]))
        if result.rowcount == 0:
            logger.warning("mark_failed: journal row missing pk=%s",
                           emit(event_pk, "record_id", "log", "operator"))
            raise JournalRowMissing(f"mark_failed: pk={event_pk}")
