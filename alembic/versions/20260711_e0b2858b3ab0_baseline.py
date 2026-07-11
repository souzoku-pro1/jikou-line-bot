"""baseline（P1-004 設計判断 D5）

空のbaseline。適用すると alembic_version テーブルだけが成立する
（業務テーブルは P1-005: InboundEvent / IngestionReceipt から migration で追加）。

Revision ID: e0b2858b3ab0
Revises:
Create Date: 2026-07-11 20:29:02.836820

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e0b2858b3ab0'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
