"""JIKOU-STAGE-APP-1: 完了報告書の本人専用 URL jikou_report_link（DDL のみ）

QUEUE-2026-10-20-HI I-4'。表定義は hub/jikou_report_link.py の metadata と同一。アプリ起動時には走らせない（alembic CLI のみ）。
down_revision は JIKOU-RECEIPT-1 の c4d6e8f0a2b4。

downgrade: 表を落とす。**データがあれば downgrade 禁止**（発行済み URL の履歴は消さない＝RV-08）。

Revision ID: d5e7f9a1b3c5
Revises: c4d6e8f0a2b4
Create Date: 2026-10-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd5e7f9a1b3c5'
down_revision: Union[str, Sequence[str], None] = 'c4d6e8f0a2b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "jikou_report_link"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("token", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("doc_id", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_jikou_report_link_doc", TABLE, ["business", "doc_id"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("jikou_report_link_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_jikou_report_link_doc", table_name=TABLE)
    op.drop_table(TABLE)
