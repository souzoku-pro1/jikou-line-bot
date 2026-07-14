"""signature_nonce (RV-04a: NM01 v1 nonce store・案B)

Revision ID: b7d3e1a9c2f4
Revises: 3e59f8270aa8
Create Date: 2026-07-14

DRAFT_RV04_HMAC_MIGRATION.md §2.4（案B=専用表・司令塔裁定 2026-07-14）。
UNIQUE(nonce) が replay 検知の実体。expires_at は timestamp+SKEW（受理し得る最遅時刻）で、
超過行は検証時 lazy 削除される。
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7d3e1a9c2f4'
down_revision: Union[str, Sequence[str], None] = '3e59f8270aa8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "signature_nonce",
        sa.Column("nonce", sa.Text(), primary_key=True),   # UNIQUE = replay 検知
        sa.Column("key_id", sa.Text(), nullable=False),
        sa.Column("caller", sa.Text(), nullable=False),
        sa.Column("seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        # H-02（RV-04a-fix）: 128bit hex（32 文字）固定を DB でも保証
        sa.CheckConstraint("length(nonce) = 32", name="ck_signature_nonce_len"),
    )
    # lazy cleanup（expires_at < now の一括削除）の走査を支える
    op.create_index("ix_signature_nonce_expires_at", "signature_nonce", ["expires_at"])


def downgrade() -> None:
    op.drop_index("ix_signature_nonce_expires_at", table_name="signature_nonce")
    op.drop_table("signature_nonce")
