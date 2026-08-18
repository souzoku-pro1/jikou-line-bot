"""Q-CHAT-1: qa_topic_reset（話題リセット境界・会話文脈の参照範囲を区切る）

正本: 大野裁定（Q 機能の会話化＋「新しい話題」操作）。境界はリセット時点の
qa_record 最大 id（last_qa_id）——会話文脈は境界より後（id >）の行のみ参照。
qa_record 本体・status CHECK 閉集合には一切触れない。

Revision ID: e7a9c4d1f6b3
Revises: c4e8a2d6b9f1
Create Date: 2026-08-18
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'e7a9c4d1f6b3'
down_revision: Union[str, Sequence[str], None] = 'c4e8a2d6b9f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def upgrade() -> None:
    op.create_table(
        "qa_topic_reset",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("user_id", sa.Text, nullable=False),
        sa.Column("last_qa_id", _BigIntPK, nullable=False,
                  server_default="0"),
    )


def downgrade() -> None:
    op.drop_table("qa_topic_reset")
