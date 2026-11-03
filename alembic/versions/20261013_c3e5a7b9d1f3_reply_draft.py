"""JIKOU-REPLY-Q3b: AI 下書き reply_draft・履歴 reply_draft_history（DDL のみ）

正本: 時効LINEボット_返信規則_v1.4.md R-J1・R-J9・§10-7・§12 Q3b。QUEUE-2026-10-13-D D-2 (a)。
表定義は hub/reply_draft.py の metadata と同一。アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（reply_draft に 1 行でもあれば例外で拒否＝RV-08）。

Revision ID: c3e5a7b9d1f3
Revises: b2d4f6a8c0e1
Create Date: 2026-10-13
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'c3e5a7b9d1f3'
down_revision: Union[str, Sequence[str], None] = 'b2d4f6a8c0e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def upgrade() -> None:
    op.create_table(
        "reply_draft",
        sa.Column("draft_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("pending_id", _BIG, nullable=True),
        sa.Column("line_user_hash", sa.Text, nullable=False),
        sa.Column("conversation_ref", sa.Text, nullable=True),
        sa.Column("category", sa.Text, nullable=True),
        sa.Column("inbound_text_version", sa.Text, nullable=False),
        sa.Column("draft_text", sa.Text, nullable=False),
        sa.Column("guard_ok", sa.Boolean, nullable=False),
        sa.Column("guard_reasons", sa.Text, nullable=True),
        sa.Column("exemplar_ids", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False, server_default="proposed"),
        sa.Column("approval_kind", sa.Text, nullable=True),
        sa.Column("final_text", sa.Text, nullable=True),
        sa.Column("op_id", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("state IN ('proposed', 'approved', 'rejected', 'sent', 'failed')", name="ck_reply_draft_state"),
        sa.CheckConstraint("approval_kind IS NULL OR approval_kind IN ('asis', 'edited')", name="ck_reply_draft_kind"),
    )
    op.create_index("ix_reply_draft_state", "reply_draft", ["business", "state", "created_at"])
    op.create_index("ix_reply_draft_pending", "reply_draft", ["pending_id"])
    op.create_table(
        "reply_draft_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("draft_id", _BIG, sa.ForeignKey("reply_draft.draft_id"), nullable=False),
        sa.Column("action", sa.Text, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("note", sa.Text, nullable=True),
        sa.CheckConstraint("action IN ('created', 'approved', 'rejected', 'sent', 'failed')",
                           name="ck_reply_draft_history_action"),
    )
    op.create_index("ix_reply_draft_history_d", "reply_draft_history", ["draft_id"])


def _refuse_if_data_present() -> None:
    n = op.get_bind().execute(sa.text("SELECT COUNT(*) FROM reply_draft")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("reply_draft_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_reply_draft_history_d", table_name="reply_draft_history")
    op.drop_table("reply_draft_history")
    op.drop_index("ix_reply_draft_pending", table_name="reply_draft")
    op.drop_index("ix_reply_draft_state", table_name="reply_draft")
    op.drop_table("reply_draft")
