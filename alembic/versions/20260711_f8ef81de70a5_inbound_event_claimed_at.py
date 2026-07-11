"""inbound_event.claimed_at 追加（P1-005b・D12/RCF-M06）

処理権を取った時刻。stale processing（クラッシュ滞留）の再claim判定に使う。
既存行の backfill は不要（本番はflag未投入でテーブル空・NULLは救済対象として扱う）。

Revision ID: f8ef81de70a5
Revises: a3ea96f2e1a8
Create Date: 2026-07-11 22:21:54.838205

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f8ef81de70a5'
down_revision: Union[str, Sequence[str], None] = 'a3ea96f2e1a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("inbound_event",
                  sa.Column("claimed_at", sa.DateTime(timezone=True),
                            nullable=True))


def downgrade() -> None:
    op.drop_column("inbound_event", "claimed_at")
