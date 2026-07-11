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
    - 既存 state=processing → skipped_duplicate（同時二重配送の二重処理防止を優先。
      プロセスクラッシュで processing のまま残った行の救済（stale claim）は
      本弾では未実装＝既知の限界。DEFER台帳 RCF-M06 として登録）
- DB 到達不能時は例外をそのまま送出（D7: 呼び出し側が 5xx を返し
  Stripe の自動リトライに委ねる。memory fallback 禁止）

テーブル追加は必ず migration 経由（alembic/versions/…_inbound_event.py）。
"""

import hashlib
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from hub.db import session_scope


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


def payload_sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def stripe_dedup_key(event: dict, payload: bytes) -> str:
    """Stripe は event id（evt_…）がグローバル一意。無い場合は hash に縮退"""
    event_id = str(event.get("id") or "")
    if event_id:
        return f"stripe:{event_id}"
    return f"stripe:sha256:{payload_sha256(payload)}"


async def record_stripe_event(event: dict, payload: bytes) -> tuple[str, int | None]:
    """Stripe event を journal に記録し、処理可否を返す。

    Returns:
        ("new", pk)                — 新規。業務処理へ進む
        ("reprocess", pk)          — 前回 failed の再送。業務処理へ進む
        ("skipped_duplicate", None) — 処理済み/処理中の重複。業務処理を走らせない
    DB 到達不能・未設定は例外送出（D7: 上位で 5xx にする。ここで飲まない）
    """
    dedup_key = stripe_dedup_key(event, payload)
    row = InboundEvent(
        provider="stripe",
        external_event_id=str(event.get("id") or "") or None,
        caller_id=None,
        dedup_key=dedup_key,
        payload_hash=payload_sha256(payload),
        event_type=str(event.get("type") or "") or None,
        signature_result="verified",  # 呼び出し側は construct_event 成功後のみ来る
        state="processing",
    )
    try:
        async with session_scope() as session:
            session.add(row)
            await session.flush()
            pk = row.id
        return "new", pk
    except IntegrityError:
        pass  # dedup_key 衝突 = 再送/重複。以降で状態に応じて分岐

    async with session_scope() as session:
        # failed の行だけを claim（条件付きUPDATEで競合安全に再処理権を取る）
        claimed = await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.dedup_key == dedup_key,
                   InboundEvent.state == "failed")
            .values(state="processing",
                    attempts=InboundEvent.attempts + 1)
            .returning(InboundEvent.id))
        claimed_id = claimed.scalar_one_or_none()
        if claimed_id is not None:
            return "reprocess", claimed_id
        # done / processing → 重複 skip（再送回数だけ記録に残す）
        await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.dedup_key == dedup_key)
            .values(attempts=InboundEvent.attempts + 1))
    return "skipped_duplicate", None


async def mark_done(event_pk: int) -> None:
    async with session_scope() as session:
        await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.id == event_pk)
            .values(state="done", processed_at=_utcnow(), last_error=None))


async def mark_failed(event_pk: int, error_class: str) -> None:
    """error_class は分類のみ（例外クラス名等・100字で切る）。本文を渡さない"""
    async with session_scope() as session:
        await session.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.id == event_pk)
            .values(state="failed", processed_at=_utcnow(),
                    last_error=(error_class or "unknown")[:100]))
