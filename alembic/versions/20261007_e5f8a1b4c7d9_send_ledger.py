"""JIKOU-REPLY-Q1a-SEND-BASE: 会話・送信操作記録・状態遷移履歴（DDL のみ・可逆）

正本: 時効LINEボット_返信規則_v1.4.md §10-5（送信操作記録・会話・共通排他）・§10-10
（新設 DB 表・RV-08 の履歴）・§12 Q1a（新設書込: 送信操作記録・会話）。
表定義は hub/send_ledger.py の metadata と同一。アプリ起動時には走らせない（D2: alembic CLI
のみ）。downgrade は 3 表を落とす（記録は検証・運用の台帳であり、業務データの正本は App 28）。

Revision ID: e5f8a1b4c7d9
Revises: d1e4f7a0b3c6
Create Date: 2026-10-07
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'e5f8a1b4c7d9'
down_revision: Union[str, Sequence[str], None] = 'd1e4f7a0b3c6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def upgrade() -> None:
    op.create_table(
        "conversation",
        sa.Column("conversation_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("ref", sa.Text, nullable=False, unique=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("line_user_hash", sa.Text, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("version", _BIG, nullable=False, server_default="1"),
        sa.Column("attending", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("attending_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attending_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_inbound_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    )
    op.create_index("ix_conversation_user", "conversation",
                    ["business", "line_user_hash", "started_at"])
    op.create_table(
        "send_operation",
        sa.Column("op_id", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("channel", sa.Text, nullable=False),
        sa.Column("conversation_id", _BIG, sa.ForeignKey("conversation.conversation_id"),
                  nullable=False),
        sa.Column("conversation_version", _BIG, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("purpose", sa.Text, nullable=False),
        sa.Column("first_reply_target", sa.Boolean, nullable=False),
        sa.Column("text_version", sa.Text, nullable=False),
        sa.Column("inbound_event_id", sa.Text, nullable=True),
        sa.Column("inbound_seq", sa.Integer, nullable=False, server_default="1"),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("completed_after_human", sa.Boolean, nullable=False,
                  server_default=sa.false()),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("confirmed_by", sa.Text, nullable=True),
        sa.CheckConstraint("actor IN ('bot', 'human', 'approved_draft')",
                           name="ck_send_operation_actor"),
        sa.CheckConstraint(
            "purpose IN ('reply', 'first_reply', 'urgent', 'image_receipt', 'image_result', "
            "'follow', 'receipt_number', 'other')", name="ck_send_operation_purpose"),
        sa.CheckConstraint("state IN ('pending', 'started', 'sent', 'unconfirmed', 'failed')",
                           name="ck_send_operation_state"),
        sa.UniqueConstraint("business", "channel", "inbound_event_id", "purpose",
                            "inbound_seq", name="uq_send_operation_inbound"),
    )
    op.create_index("ix_send_operation_conversation", "send_operation", ["conversation_id"])
    op.create_index("ix_send_operation_state", "send_operation", ["state"])
    op.create_table(
        "send_operation_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("op_id", sa.Text, sa.ForeignKey("send_operation.op_id"), nullable=False),
        sa.Column("from_state", sa.Text, nullable=True),
        sa.Column("to_state", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_send_operation_history_op", "send_operation_history", ["op_id"])


def downgrade() -> None:
    op.drop_index("ix_send_operation_history_op", table_name="send_operation_history")
    op.drop_table("send_operation_history")
    op.drop_index("ix_send_operation_state", table_name="send_operation")
    op.drop_index("ix_send_operation_conversation", table_name="send_operation")
    op.drop_table("send_operation")
    op.drop_index("ix_conversation_user", table_name="conversation")
    op.drop_table("conversation")
