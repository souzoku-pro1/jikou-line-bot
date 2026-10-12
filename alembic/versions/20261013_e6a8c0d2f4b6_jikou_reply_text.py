"""JIKOU-REPLY-Q5: 昇格定型 jikou_reply_text・履歴 jikou_reply_text_history（DDL のみ）

正本: 時効LINEボット_返信規則_v1.4.md R-J9・§10-9（凍結昇格）・§10-10。QUEUE-2026-10-13-D D-4 (a)(c)。
表定義は hub/jikou_reply_promote.py の metadata と同一。コードの定型表（hub/jikou_reply_texts）は触らない。
アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（jikou_reply_text に 1 行でもあれば例外で拒否＝RV-08）。

Revision ID: e6a8c0d2f4b6
Revises: d4f6b8c0e2a4
Create Date: 2026-10-13
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'e6a8c0d2f4b6'
down_revision: Union[str, Sequence[str], None] = 'd4f6b8c0e2a4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def upgrade() -> None:
    op.create_table(
        "jikou_reply_text",
        sa.Column("text_id", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("category", sa.Text, nullable=False),
        sa.Column("text", sa.Text, nullable=False),
        sa.Column("text_version", sa.Text, nullable=False),
        sa.Column("source_exemplar_id", _BIG, nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('active', 'retired')", name="ck_jikou_reply_text_status"),
    )
    op.create_index("ix_jikou_reply_text_cat", "jikou_reply_text", ["business", "category", "status"])
    op.create_table(
        "jikou_reply_text_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("text_id", sa.Text, sa.ForeignKey("jikou_reply_text.text_id"), nullable=False),
        sa.Column("action", sa.Text, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("table_version", sa.Text, nullable=False),
        sa.Column("note", sa.Text, nullable=True),
        sa.CheckConstraint("action IN ('promoted', 'retired', 'reactivated')", name="ck_jikou_reply_text_history_action"),
    )
    op.create_index("ix_jikou_reply_text_history_t", "jikou_reply_text_history", ["text_id"])


def _refuse_if_data_present() -> None:
    n = op.get_bind().execute(sa.text("SELECT COUNT(*) FROM jikou_reply_text")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("jikou_reply_text_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_jikou_reply_text_history_t", table_name="jikou_reply_text_history")
    op.drop_table("jikou_reply_text_history")
    op.drop_index("ix_jikou_reply_text_cat", table_name="jikou_reply_text")
    op.drop_table("jikou_reply_text")
