"""notify_heartbeat

Revision ID: 3e59f8270aa8
Revises: f8ef81de70a5
Create Date: 2026-07-12 17:24:32.793375

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '3e59f8270aa8'
down_revision: Union[str, Sequence[str], None] = 'f8ef81de70a5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "notify_heartbeat",
        sa.Column("channel", sa.Text(), primary_key=True),
        sa.Column("last_success_at", sa.DateTime(timezone=True),
                  nullable=False),
    )


def downgrade() -> None:
    op.drop_table("notify_heartbeat")
