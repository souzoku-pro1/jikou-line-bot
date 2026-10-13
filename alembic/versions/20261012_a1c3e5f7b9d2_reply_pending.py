"""JIKOU-REPLY-Q1c: 対応待ち reply_pending（＋履歴）（DDL のみ）

正本: 時効LINEボット_返信規則_v1.4.md §10-6（対応待ち・独立状態）・§10-10（新設 DB 表・履歴を残す・起動時に
実行しない）・§12 Q1c。QUEUE-2026-10-12-C C-1 (d)。
表定義は hub/reply_pending.py の metadata と同一。conversation（e5f8a1b4c7d9）は ALTER しない（会話との紐付けは
conversation_id・line_user_hash の列で持つ）。アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（いずれかの表に 1 行でもあれば例外で拒否＝RV-08。
戻す必要があるときは先に大野が内容を確認し、行を退避してから）。

Revision ID: a1c3e5f7b9d2
Revises: f7b2c9d4e1a6
Create Date: 2026-10-12
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a1c3e5f7b9d2'
down_revision: Union[str, Sequence[str], None] = 'f7b2c9d4e1a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLES = ("reply_pending_history", "reply_pending")   # drop 順（FK の子が先）


def upgrade() -> None:
    op.create_table(
        "reply_pending",
        sa.Column("pending_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("channel", sa.Text, nullable=False),
        sa.Column("inbound_event_id", sa.Text, nullable=True),
        sa.Column("conversation_id", _BIG, nullable=True),
        sa.Column("line_user_hash", sa.Text, nullable=False),
        sa.Column("route", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("category", sa.Text, nullable=True),
        sa.Column("approval_record_id", sa.Text, nullable=True),
        sa.Column("state", sa.Text, nullable=False, server_default="open"),
        sa.Column("notify_state", sa.Text, nullable=False, server_default="none"),
        sa.Column("customer_send", sa.Text, nullable=False, server_default="none"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", sa.Text, nullable=True),
        sa.CheckConstraint("state IN ('open', 'human_replied', 'auto_replied', 'not_needed')",
                           name="ck_reply_pending_state"),
        sa.CheckConstraint("route IN ('customer', 'hearing', 'inbound')", name="ck_reply_pending_route"),
        sa.CheckConstraint("reason IN ('demoted', 'empty_reply', 'policy_stopped', 'claude_outage')",
                           name="ck_reply_pending_reason"),
        sa.UniqueConstraint("business", "channel", "inbound_event_id", name="uq_reply_pending_inbound"),
    )
    op.create_index("ix_reply_pending_state", "reply_pending", ["state", "created_at"])
    op.create_index("ix_reply_pending_user", "reply_pending", ["business", "line_user_hash", "state"])
    op.create_table(
        "reply_pending_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("pending_id", _BIG, sa.ForeignKey("reply_pending.pending_id"), nullable=False),
        sa.Column("field", sa.Text, nullable=False),
        sa.Column("from_value", sa.Text, nullable=True),
        sa.Column("to_value", sa.Text, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_reply_pending_history_p", "reply_pending_history", ["pending_id"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    for name in TABLES:
        n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {name}")).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("reply_pending_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_reply_pending_history_p", table_name="reply_pending_history")
    op.drop_table("reply_pending_history")
    op.drop_index("ix_reply_pending_user", table_name="reply_pending")
    op.drop_index("ix_reply_pending_state", table_name="reply_pending")
    op.drop_table("reply_pending")
