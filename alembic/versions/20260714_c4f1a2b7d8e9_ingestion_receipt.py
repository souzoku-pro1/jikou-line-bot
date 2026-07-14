"""ingestion_receipt + processing_attempt (RV-05-13: durable file-ingest ledger)

Revision ID: c4f1a2b7d8e9
Revises: b7d3e1a9c2f4
Create Date: 2026-07-14

DRAFT_RV05_DURABLE_INBOUND.md §2.2/§2.3。既存表 ALTER なし・新規のみ。
- ingestion_receipt: 冪等/可視化/fencing 台帳（epoch fence・last_heartbeat_at lease）
- processing_attempt: 監査専用（FK(receipt_id) ON DELETE CASCADE・UNIQUE(receipt_id,epoch)）
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c4f1a2b7d8e9'
down_revision: Union[str, Sequence[str], None] = 'b7d3e1a9c2f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ingestion_receipt",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source_file_id", sa.Text(), nullable=False),
        sa.Column("source_sha256", sa.Text(), nullable=False),
        sa.Column("ingest_type", sa.Text(), nullable=False),
        sa.Column("caller_id", sa.Text(), nullable=False),
        sa.Column("case_hint", sa.Text()),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_outcome", sa.Text(), nullable=False),
        sa.Column("downstream_refs", sa.Text()),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("epoch", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("idempotency_key", name="uq_ingestion_receipt_idem"),
    )
    op.create_index("ix_ingestion_receipt_last_outcome", "ingestion_receipt",
                    ["last_outcome"])
    op.create_table(
        "processing_attempt",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("receipt_id", sa.BigInteger(), nullable=False),
        sa.Column("epoch", sa.Integer(), nullable=False),
        sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("phase", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text()),
        sa.ForeignKeyConstraint(["receipt_id"], ["ingestion_receipt.id"],
                                ondelete="CASCADE",
                                name="fk_processing_attempt_receipt"),
        sa.UniqueConstraint("receipt_id", "epoch",
                            name="uq_processing_attempt_receipt_epoch"),
    )


def downgrade() -> None:
    op.drop_table("processing_attempt")
    op.drop_index("ix_ingestion_receipt_last_outcome", table_name="ingestion_receipt")
    op.drop_table("ingestion_receipt")
