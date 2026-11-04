"""JIKOU-RECEIPT-1: 領収書 jikou_receipt・採番 receipt_sequence（DDL のみ）

QUEUE-2026-10-20-HI I-2。表定義は hub/jikou_receipt.py の metadata と同一。アプリ起動時には走らせない（alembic CLI のみ）。
down_revision は JIKOU-CERT-1 の b3c5d7e9f1a2。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（発行済み領収書・採番は消さない＝RV-08）。

Revision ID: c4d6e8f0a2b4
Revises: b3c5d7e9f1a2
Create Date: 2026-10-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'c4d6e8f0a2b4'
down_revision: Union[str, Sequence[str], None] = 'b3c5d7e9f1a2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES = ("jikou_receipt", "receipt_sequence")


def upgrade() -> None:
    op.create_table(
        "receipt_sequence",
        sa.Column("business", sa.Text, primary_key=True),
        sa.Column("year", sa.Integer, primary_key=True),
        sa.Column("seq", sa.Integer, primary_key=True),
        sa.Column("allocated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "jikou_receipt",
        sa.Column("receipt_id", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("number", sa.Text, nullable=False),
        sa.Column("year", sa.Integer, nullable=False),
        sa.Column("seq", sa.Integer, nullable=False),
        sa.Column("issued_on", sa.Date, nullable=False),
        sa.Column("addressee", sa.Text, nullable=False),
        sa.Column("creditor_names", sa.Text, nullable=False),
        sa.Column("count_total", sa.Integer, nullable=False),
        sa.Column("count_billed", sa.Integer, nullable=False),
        sa.Column("qty1", sa.Integer, nullable=False), sa.Column("unit1", sa.Integer, nullable=False), sa.Column("amt1", sa.Integer, nullable=False),
        sa.Column("qty2", sa.Integer, nullable=False), sa.Column("unit2", sa.Integer, nullable=False), sa.Column("amt2", sa.Integer, nullable=False),
        sa.Column("subtotal", sa.Integer, nullable=False), sa.Column("tax", sa.Integer, nullable=False), sa.Column("total", sa.Integer, nullable=False),
        sa.Column("reissue", sa.Boolean, nullable=False),
        sa.Column("reissue_of", sa.Text, nullable=True),
        sa.Column("xlsx", sa.LargeBinary, nullable=False),
        sa.Column("sha256", sa.Text, nullable=False),
        sa.Column("texts_version", sa.Text, nullable=False),
        sa.Column("template_sha256", sa.Text, nullable=False),
        sa.Column("created_by", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("count_billed >= 1", name="ck_jikou_receipt_billed"),
    )
    op.create_index("ix_jikou_receipt_case", "jikou_receipt", ["business", "case_record_id"])
    op.create_index("ix_jikou_receipt_number", "jikou_receipt", ["business", "number"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    for name in TABLES:
        n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {name}")).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("jikou_receipt_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_jikou_receipt_number", table_name="jikou_receipt")
    op.drop_index("ix_jikou_receipt_case", table_name="jikou_receipt")
    op.drop_table("jikou_receipt")
    op.drop_table("receipt_sequence")
