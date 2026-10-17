"""SOUZOKU-4: 財産目録 souzoku_assets・協議書（案）保管 souzoku_kyogisho（DDL のみ・作成のみ・未適用）

QUEUE-2026-10-17-H H-5。表定義は hub/souzoku_assets.py の metadata と同一。アプリ起動時には走らせない（alembic CLI のみ）。
down_revision は SOUZOKU-3 の c1d3e5f7a9b2。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（RV-08）。

Revision ID: d2e4f6a8b0c3
Revises: c1d3e5f7a9b2
Create Date: 2026-10-17
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd2e4f6a8b0c3'
down_revision: Union[str, Sequence[str], None] = 'c1d3e5f7a9b2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLES = ("souzoku_kyogisho", "souzoku_assets")


def upgrade() -> None:
    op.create_table(
        "souzoku_assets",
        sa.Column("asset_id", sa.Text, primary_key=True),
        sa.Column("intake_id", sa.Text, nullable=False),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("detail_json", sa.Text, nullable=False),
        sa.Column("value_yen", _BIG, nullable=True),
        sa.Column("acquirer_label", sa.Text, nullable=True),
        sa.Column("compensation_json", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("voided_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("kind IN ('real_estate', 'deposit', 'securities', 'other')", name="ck_souzoku_assets_kind"),
        sa.CheckConstraint("status IN ('active', 'void')", name="ck_souzoku_assets_status"),
    )
    op.create_index("ix_souzoku_assets_intake", "souzoku_assets", ["intake_id", "created_at"])
    op.create_table(
        "souzoku_kyogisho",
        sa.Column("doc_id", sa.Text, primary_key=True),
        sa.Column("intake_id", sa.Text, nullable=False),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("sha256", sa.Text, nullable=False),
        sa.Column("size_bytes", sa.Integer, nullable=False),
        sa.Column("content", sa.LargeBinary, nullable=False),
        sa.Column("texts_version", sa.Text, nullable=False),
        sa.Column("template_sha256", sa.Text, nullable=False),
        sa.Column("created_by", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("intake_id", "version", name="uq_souzoku_kyogisho_version"),
    )
    op.create_index("ix_souzoku_kyogisho_intake", "souzoku_kyogisho", ["intake_id", "created_at"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    for t in TABLES:
        n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {t}")).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("souzoku_assets_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_souzoku_kyogisho_intake", table_name="souzoku_kyogisho")
    op.drop_table("souzoku_kyogisho")
    op.drop_index("ix_souzoku_assets_intake", table_name="souzoku_assets")
    op.drop_table("souzoku_assets")
