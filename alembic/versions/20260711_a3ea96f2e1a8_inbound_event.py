"""inbound_event（P1-005a・D8）

inbound webhook の durable journal。第1弾は Stripe が書き込む。
raw payload 本体は保存しない（O-06/O-32 裁定まで・hub/inbound_event.py 参照）。

Revision ID: a3ea96f2e1a8
Revises: e0b2858b3ab0
Create Date: 2026-07-11 21:50:18.104708

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a3ea96f2e1a8'
down_revision: Union[str, Sequence[str], None] = 'e0b2858b3ab0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "inbound_event",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
                  primary_key=True, autoincrement=True),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("external_event_id", sa.Text(), nullable=True),
        sa.Column("caller_id", sa.Text(), nullable=True),
        sa.Column("dedup_key", sa.Text(), nullable=False),
        sa.Column("payload_hash", sa.Text(), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=True),
        sa.Column("signature_result", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        # dedup_key の UNIQUE 制約が重複検知の実体（D9）。
        # ALTER型のcreate_unique_constraintはsqlite方言（offline煙テスト）が
        # 非対応のため、create_table内のインライン制約にする
        sa.UniqueConstraint("dedup_key", name="uq_inbound_event_dedup_key"),
    )
    # 運用照会用（provider×state・受信時刻）
    op.create_index("ix_inbound_event_provider_state",
                    "inbound_event", ["provider", "state"])
    op.create_index("ix_inbound_event_received_at",
                    "inbound_event", ["received_at"])


def downgrade() -> None:
    op.drop_index("ix_inbound_event_received_at", table_name="inbound_event")
    op.drop_index("ix_inbound_event_provider_state", table_name="inbound_event")
    op.drop_table("inbound_event")  # インライン制約はtableごと落ちる
