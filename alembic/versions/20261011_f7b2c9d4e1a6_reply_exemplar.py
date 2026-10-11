"""JIKOU-REPLY-Q1b: 手本集 reply_exemplar（＋履歴・App 28 記録未了の付帯表）（DDL のみ）

正本: 時効LINEボット_返信規則_v1.4.md §10-7（手本は匿名化済み文だけ）・§10-10（新設 DB 表・
操作・状態遷移の履歴を残す・起動時に実行しない）・§12 Q1b。QUEUE-2026-10-11-B Q-3 (d)。
fix1（Codex R-JIKOU-REPLY-Q1b-1 BR-08・BR-09）: 本 revision は本番未適用のため **同一 revision を更新**
（PR #267 と同じ扱い。merge 前に本番 current が f7b2c9d4e1a6 でないことを確認する）。
- reply_exemplar に category_id・source_text_version・approved_by・approved_at・active を追加
- reply_exemplar_history（created / used / deactivated）
- reply_chatlog_followup（送信操作記録の付帯情報＝App 28 human 行の記録未了フラグ・op_id 単位）
表定義は hub/reply_exemplar.py・hub/reply_followup.py の metadata と同一。アプリ起動時には走らせない
（D2: alembic CLI のみ）。

downgrade: 3 表を落とす（送信操作記録・会話には触れない）。**データがあれば downgrade 禁止**——
いずれかの表に 1 行でもあれば例外で拒否する（手本・履歴・記録未了は削除で失われる＝RV-08）。
戻す必要があるときは先に大野が内容を確認し、行を退避してから（手順書: PR 本文 §5・pgv/q1b_fix1_report.md）。

Revision ID: f7b2c9d4e1a6
Revises: e5f8a1b4c7d9
Create Date: 2026-10-11（fix1 2026-10-12）
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'f7b2c9d4e1a6'
down_revision: Union[str, Sequence[str], None] = 'e5f8a1b4c7d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLES = ("reply_exemplar_history", "reply_chatlog_followup", "reply_exemplar")   # drop 順（FK の子が先）


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
        sa.Column("category_id", sa.Text, nullable=True),
        sa.Column("source_text_version", sa.Text, nullable=True),
        sa.Column("approved_by", sa.Text, nullable=False, server_default="human"),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("active", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.CheckConstraint("stage IN ('相談', '受任後')", name="ck_reply_exemplar_stage"),
    )
    op.create_index("ix_reply_exemplar_lookup", "reply_exemplar",
                    ["business", "stage", "created_at"])
    op.create_index("ix_reply_exemplar_active", "reply_exemplar", ["business", "active"])
    op.create_table(
        "reply_exemplar_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("exemplar_id", _BIG, sa.ForeignKey("reply_exemplar.id"), nullable=False),
        sa.Column("action", sa.Text, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("note", sa.Text, nullable=True),
        sa.CheckConstraint("action IN ('created', 'used', 'deactivated')",
                           name="ck_reply_exemplar_history_action"),
    )
    op.create_index("ix_reply_exemplar_history_ex", "reply_exemplar_history", ["exemplar_id"])
    op.create_table(
        "reply_chatlog_followup",
        sa.Column("op_id", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("approval_record_id", sa.Text, nullable=False),
        sa.Column("line_user_hash", sa.Text, nullable=False),
        sa.Column("text", sa.Text, nullable=True),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_result", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("state IN ('pending', 'done', 'exhausted')", name="ck_reply_followup_state"),
    )
    op.create_index("ix_reply_followup_state", "reply_chatlog_followup", ["state", "created_at"])


def _refuse_if_data_present() -> None:
    """データがあれば downgrade 禁止（削除で失われる・RV-08）。値は出さない。"""
    bind = op.get_bind()
    for name in TABLES:
        n = bind.execute(sa.text(f'SELECT COUNT(*) FROM "{name}"')).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("reply_exemplar_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_reply_followup_state", table_name="reply_chatlog_followup")
    op.drop_table("reply_chatlog_followup")
    op.drop_index("ix_reply_exemplar_history_ex", table_name="reply_exemplar_history")
    op.drop_table("reply_exemplar_history")
    op.drop_index("ix_reply_exemplar_active", table_name="reply_exemplar")
    op.drop_index("ix_reply_exemplar_lookup", table_name="reply_exemplar")
    op.drop_table("reply_exemplar")
