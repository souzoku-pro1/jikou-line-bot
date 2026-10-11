"""JIKOU-REPLY-Q1b: 手本集 reply_exemplar（DDL のみ・可逆）

正本: 時効LINEボット_返信規則_v1.4.md §10-7（手本は匿名化済み文だけ）・§10-10（新設 DB 表・
起動時に実行しない）・§12 Q1b。QUEUE-2026-10-11-B Q-3 (d)。
表定義は hub/reply_exemplar.py の metadata と同一。アプリ起動時には走らせない（D2: alembic CLI
のみ）。downgrade は本表だけを落とす（送信操作記録・会話には触れない）。

Revision ID: f7b2c9d4e1a6
Revises: e5f8a1b4c7d9
Create Date: 2026-10-11
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'f7b2c9d4e1a6'
down_revision: Union[str, Sequence[str], None] = 'e5f8a1b4c7d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def upgrade() -> None:
    op.create_table(
        "reply_exemplar",
        sa.Column("id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("conversation_ref", sa.Text, nullable=True),
        sa.Column("inbound_text", sa.Text, nullable=False),
        sa.Column("human_reply", sa.Text, nullable=False),
        sa.Column("creditor_name", sa.Text, nullable=True),
        sa.Column("stage", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("source_op_id", sa.Text, nullable=True),
        sa.CheckConstraint("stage IN ('相談', '受任後')", name="ck_reply_exemplar_stage"),
    )
    op.create_index("ix_reply_exemplar_lookup", "reply_exemplar",
                    ["business", "stage", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_reply_exemplar_lookup", table_name="reply_exemplar")
    op.drop_table("reply_exemplar")
