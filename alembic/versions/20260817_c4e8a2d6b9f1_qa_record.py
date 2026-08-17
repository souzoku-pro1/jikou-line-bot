"""Q-BATCH-1: qa_record（Q&A 台帳・PWA 質問応答の保存層）

正本: ③項目10＋12.2-1＋大野裁定 2026-08-17（Q 機能は PWA 搭載）。
業務データと分離した専用テーブル（immutable trigger なし=票の指定）。
質問・回答・出典配列・注記配列・モデル・トークン・コスト概算・所要を保存する。

Revision ID: c4e8a2d6b9f1
Revises: f3d8c1a4e9b2
Create Date: 2026-08-17
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'c4e8a2d6b9f1'
down_revision: Union[str, Sequence[str], None] = 'f3d8c1a4e9b2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "qa_record",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("user_id", sa.Text, nullable=False),
        sa.Column("question", sa.Text, nullable=False),
        sa.Column("answer", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("sources", _JSON, nullable=False),
        sa.Column("notes", _JSON, nullable=False),
        sa.Column("model", sa.Text, nullable=False),
        sa.Column("input_tokens", sa.Integer, nullable=False,
                  server_default="0"),
        sa.Column("output_tokens", sa.Integer, nullable=False,
                  server_default="0"),
        sa.Column("cache_read_tokens", sa.Integer, nullable=False,
                  server_default="0"),
        sa.Column("cost_usd", sa.Text, nullable=False),
        sa.Column("elapsed_ms", sa.Integer, nullable=False,
                  server_default="0"),
        sa.CheckConstraint("status IN ('ok', 'no_source', 'error')",
                           name="ck_qa_record_status"),
    )
    op.create_index("ix_qa_record_created", "qa_record", ["created_at"])


def downgrade() -> None:
    op.drop_index("ix_qa_record_created", table_name="qa_record")
    op.drop_table("qa_record")
