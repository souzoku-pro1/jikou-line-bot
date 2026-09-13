"""SHINDAN-LINE-LINK-1: shindan_link（友だち追加時に送る本人専用の診断フォームリンク）

token（不透明値・主キー）・line_user_id・created_at・expires_at（30 日）・used_at。
down_revision は origin/main の head（e7a9c4d1f6b3）。

Revision ID: a7d3f1c9e2b4
Revises: e7a9c4d1f6b3
Create Date: 2026-09-14
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a7d3f1c9e2b4'
down_revision: Union[str, Sequence[str], None] = 'e7a9c4d1f6b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "shindan_link",
        sa.Column("token", sa.Text, primary_key=True),
        sa.Column("line_user_id", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("shindan_link")
