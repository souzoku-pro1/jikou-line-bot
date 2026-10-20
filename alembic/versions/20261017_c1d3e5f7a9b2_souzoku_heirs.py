"""SOUZOKU-3: 相続人の確定 souzoku_heirs・履歴 souzoku_heirs_history（DDL のみ・作成のみ・未適用）

QUEUE-2026-10-17-H H-4。表定義は hub/souzoku_heirs.py の metadata と同一。アプリ起動時には走らせない（alembic CLI のみ）。
down_revision は SOUZOKU-2 の b0c1d2e3f4a5（b0c1 自体が e5f8 の子で Q1b の f7b2 と並列ヘッド＝merge 順で付け替える・
docs/pgcheck_runbook.md §4）。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（RV-08）。

Revision ID: c1d3e5f7a9b2
Revises: b0c1d2e3f4a5
Create Date: 2026-10-17
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'c1d3e5f7a9b2'
down_revision: Union[str, Sequence[str], None] = 'b0c1d2e3f4a5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLES = ("souzoku_heirs_history", "souzoku_heirs")


def upgrade() -> None:
    op.create_table(
        "souzoku_heirs",
        sa.Column("heirs_id", sa.Text, primary_key=True),
        sa.Column("intake_id", sa.Text, nullable=False),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("input_json", sa.Text, nullable=False),
        sa.Column("result_json", sa.Text, nullable=False),
        sa.Column("result", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default="draft"),
        sa.Column("law_table_version", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confirmed_by", sa.Text, nullable=True),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('draft', 'confirmed')", name="ck_souzoku_heirs_status"),
        sa.CheckConstraint("result IN ('determined', 'needs_attorney', 'no_heirs')", name="ck_souzoku_heirs_result"),
        sa.UniqueConstraint("intake_id", "version", name="uq_souzoku_heirs_version"),
    )
    op.create_index("ix_souzoku_heirs_intake", "souzoku_heirs", ["intake_id", "created_at"])
    op.create_table(
        "souzoku_heirs_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("heirs_id", sa.Text, sa.ForeignKey("souzoku_heirs.heirs_id"), nullable=False),
        sa.Column("action", sa.Text, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("action IN ('created', 'confirmed')", name="ck_souzoku_heirs_history_action"),
    )
    op.create_index("ix_souzoku_heirs_history_h", "souzoku_heirs_history", ["heirs_id"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    for t in TABLES:
        n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {t}")).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("souzoku_heirs_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_souzoku_heirs_history_h", table_name="souzoku_heirs_history")
    op.drop_table("souzoku_heirs_history")
    op.drop_index("ix_souzoku_heirs_intake", table_name="souzoku_heirs")
    op.drop_table("souzoku_heirs")
