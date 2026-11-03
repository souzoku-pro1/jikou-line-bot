"""JIKOU-FOLLOWUP-1: 案件結果 jikou_case_result・履歴 jikou_case_result_history（DDL のみ）

QUEUE-2026-10-16-R F-1。表定義は hub/jikou_followup.py の metadata と同一。アプリ起動時には走らせない（alembic CLI のみ）。
down_revision は本 branch（feature/jikou-reply-q6）の head e6a8c0d2f4b6。JIKOU-SEND-PURPOSE-2（f8b0c2d4e6a8・別 branch）と
並列になるため、merge 順で後に入る側の down_revision を付け替える（保留一覧・大野判断）。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（いずれかの表に 1 行でもあれば例外で拒否＝RV-08）。

Revision ID: a9b1c3d5e7f0
Revises: e6a8c0d2f4b6
Create Date: 2026-10-16
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a9b1c3d5e7f0'
down_revision: Union[str, Sequence[str], None] = 'e6a8c0d2f4b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLES = ("jikou_case_result_history", "jikou_case_result")   # drop 順（FK の子が先）


def upgrade() -> None:
    op.create_table(
        "jikou_case_result",
        sa.Column("result_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("slot", sa.Integer, nullable=False),
        sa.Column("creditor_name", sa.Text, nullable=True),
        sa.Column("shipped_on", sa.Date, nullable=True),
        sa.Column("shipped_source", sa.Text, nullable=True),
        sa.Column("result", sa.Text, nullable=True),
        sa.Column("answered_on", sa.Date, nullable=True),
        sa.Column("summary", sa.Text, nullable=True),
        sa.Column("recorded_by", sa.Text, nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("slot IN (1, 2, 3)", name="ck_jikou_case_result_slot"),
        sa.CheckConstraint("result IS NULL OR result IN ('established', 'not_established', 'partial', 'no_answer')",
                           name="ck_jikou_case_result_result"),
        sa.UniqueConstraint("business", "case_record_id", "slot", name="uq_jikou_case_result_case_slot"),
    )
    op.create_index("ix_jikou_case_result_open", "jikou_case_result", ["business", "shipped_on", "result"])
    op.create_table(
        "jikou_case_result_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("result_id", _BIG, sa.ForeignKey("jikou_case_result.result_id"), nullable=False),
        sa.Column("field", sa.Text, nullable=False),
        sa.Column("from_value", sa.Text, nullable=True),
        sa.Column("to_value", sa.Text, nullable=True),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_jikou_case_result_history_r", "jikou_case_result_history", ["result_id"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    for name in TABLES:
        n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {name}")).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("jikou_case_result_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_jikou_case_result_history_r", table_name="jikou_case_result_history")
    op.drop_table("jikou_case_result_history")
    op.drop_index("ix_jikou_case_result_open", table_name="jikou_case_result")
    op.drop_table("jikou_case_result")
