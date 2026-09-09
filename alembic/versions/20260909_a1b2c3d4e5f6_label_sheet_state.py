"""LABEL-PRINT-1: label_sheet_state（ラベルシート残量）と label_print_job（冪等キー）

正本: LABEL-PRINT-1 票（裁定 D2）。残量は既存の durable DB に持つ（kintone 新アプリなし）。
機械は次の空き面を提案するだけで、面を消費するのは人の「印刷済」のみ。
既存テーブルには一切触れない。

Revision ID: a1b2c3d4e5f6
Revises: e7a9c4d1f6b3
Create Date: 2026-09-09
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'e7a9c4d1f6b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_Json = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "label_sheet_state",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("sheet_id", sa.Text, nullable=False),
        sa.Column("layout", sa.Text, nullable=False),
        sa.Column("used_faces", _Json, nullable=False),
        sa.Column("pending_faces", _Json, nullable=False),
        sa.Column("history", _Json, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_by", sa.Text, nullable=False, server_default=""),
    )
    op.create_table(
        "label_print_job",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("idempotency_key", sa.Text, nullable=False, unique=True),
        sa.Column("sheet_id", sa.Text, nullable=False),
        sa.Column("face", sa.Integer, nullable=False),
        sa.Column("filename", sa.Text, nullable=False),
        sa.Column("file_key", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("created_by", sa.Text, nullable=False, server_default=""),
    )


def downgrade() -> None:
    op.drop_table("label_print_job")
    op.drop_table("label_sheet_state")
