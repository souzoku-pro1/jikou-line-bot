"""SOUZOKU-2: 相続相談受付 souzoku_intake・履歴 souzoku_intake_history（DDL のみ・作成のみ・未適用）

QUEUE-2026-10-16-R G-5。表定義は hub/souzoku_intake.py の metadata と同一。アプリ起動時には走らせない（alembic CLI のみ）。
down_revision は main の head e5f8a1b4c7d9（JIKOU-REPLY-Q1a 送信台帳）。未 merge の Q1b（f7b2c9d4e1a6）も e5f8 の次のため、
merge 順で後に入る側の down_revision を付け替える（保留一覧・大野判断）。

downgrade: 2 表を落とす。**データがあれば downgrade 禁止**（RV-08）。

Revision ID: b0c1d2e3f4a5
Revises: e5f8a1b4c7d9
Create Date: 2026-10-16
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'b0c1d2e3f4a5'
down_revision: Union[str, Sequence[str], None] = 'e5f8a1b4c7d9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLES = ("souzoku_intake_history", "souzoku_intake")


def upgrade() -> None:
    op.create_table(
        "souzoku_intake",
        sa.Column("intake_id", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("source", sa.Text, nullable=False),
        sa.Column("line_user_hash", sa.Text, nullable=True),
        sa.Column("deceased_name", sa.Text, nullable=False),
        sa.Column("deceased_kana", sa.Text, nullable=False),
        sa.Column("death_on", sa.Date, nullable=False),
        sa.Column("heirs_json", sa.Text, nullable=False),
        sa.Column("heirs_total", sa.Integer, nullable=False),
        sa.Column("will_status", sa.Text, nullable=False),
        sa.Column("property_types", sa.Text, nullable=False),
        sa.Column("dispute", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("notify_state", sa.Text, nullable=False, server_default="none"),
        sa.Column("inbound_event_id", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("status IN ('received', 'out_of_scope')", name="ck_souzoku_intake_status"),
        sa.CheckConstraint("notify_state IN ('none', 'sent', 'throttled', 'failed', 'unconfirmed', 'unavailable')",
                           name="ck_souzoku_intake_notify"),
        sa.UniqueConstraint("inbound_event_id", name="uq_souzoku_intake_event"),
    )
    op.create_index("ix_souzoku_intake_created", "souzoku_intake", ["business", "created_at"])
    op.create_table(
        "souzoku_intake_history",
        sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("intake_id", sa.Text, sa.ForeignKey("souzoku_intake.intake_id"), nullable=False),
        sa.Column("field", sa.Text, nullable=False),
        sa.Column("from_value", sa.Text, nullable=True),
        sa.Column("to_value", sa.Text, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_souzoku_intake_history_i", "souzoku_intake_history", ["intake_id"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    for name in TABLES:
        n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {name}")).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("souzoku_intake_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_souzoku_intake_history_i", table_name="souzoku_intake_history")
    op.drop_table("souzoku_intake_history")
    op.drop_index("ix_souzoku_intake_created", table_name="souzoku_intake")
    op.drop_table("souzoku_intake")
