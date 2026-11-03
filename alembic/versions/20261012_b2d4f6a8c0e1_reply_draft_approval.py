"""JIKOU-REPLY-Q3a: 承認キューの下書き照合 reply_draft_approval＋候補通知の確定表 exemplar_promotion_notice（DDL のみ）

正本: 時効LINEボット_返信規則_v1.4.md §10-9（凍結昇格: 「差分なしで承認」の回数が閾値に達したら候補）・§10-10。
QUEUE-2026-10-12-C C-2 (c)。表定義は hub/reply_exemplar.py の metadata（reply_draft_approval）と同一。
本文は持たない（起票時・送信時の sha256 のみ）。アプリ起動時には走らせない（D2: alembic CLI のみ）。

fix1 BQ-05（2026-11-03・本番未適用のため同一 revision に追加）: exemplar_promotion_notice（business, category, threshold の固定キーで
閾値到達を 1 行だけ確定・state pending/sent・attempts・notified_at）。本文・ID は無い。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（どちらかに 1 行でもあれば例外で拒否＝RV-08）。

Revision ID: b2d4f6a8c0e1
Revises: b7d9f1a3c5e7（Q1c-fix1 で a1c3 → d4f6、FORWARD-MERGE-2 で d4f6 → b7d9 に付け替え）
Create Date: 2026-10-12
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'b2d4f6a8c0e1'
down_revision: Union[str, Sequence[str], None] = 'b7d9f1a3c5e7'   # Q1c-fix1 で a1c3 → d4f6、FORWARD-MERGE-2（main ce92973）で d4f6 → b7d9（通知キー列）に付け替え
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "reply_draft_approval",
        sa.Column("record_id", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("category", sa.Text, nullable=True),
        sa.Column("draft_text_version", sa.Text, nullable=False),
        sa.Column("sent_text_version", sa.Text, nullable=True),
        sa.Column("unmodified", sa.Boolean, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_reply_draft_approval_cat", "reply_draft_approval", ["business", "category", "unmodified"])
    op.create_table(                                     # fix1 BQ-05
        "exemplar_promotion_notice",
        sa.Column("business", sa.Text, primary_key=True),
        sa.Column("category", sa.Text, primary_key=True),
        sa.Column("threshold", sa.Integer, primary_key=True),
        sa.Column("state", sa.Text, nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
    )


def _refuse_if_data_present() -> None:
    n = op.get_bind().execute(sa.text("SELECT COUNT(*) FROM reply_draft_approval")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("reply_draft_approval_downgrade_refused_data_present")
    m = op.get_bind().execute(sa.text("SELECT COUNT(*) FROM exemplar_promotion_notice")).scalar()
    if int(m or 0) > 0:
        raise RuntimeError("exemplar_promotion_notice_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_table("exemplar_promotion_notice")
    op.drop_index("ix_reply_draft_approval_cat", table_name="reply_draft_approval")
    op.drop_table("reply_draft_approval")
