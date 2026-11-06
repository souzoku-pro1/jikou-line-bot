"""UX-PENDING-1-fix4: merge revision — Q3a（b2d4）と UX-PENDING-1（d2f4）の 2 本の head を 1 本にする（DDL なし）

経緯: main に JIKOU-REPLY-Q3a（b7d9 → b2d4・PR #276）が入り、feature/ux-pending-1（b7d9 → c9e1 → d2f4）と head が分岐した。
本 revision は両方を down_revision に持つ **空の merge revision**。表・列・索引の変更は無い。

本番手順: `alembic upgrade head` 一発（適用済みの b2d4 に続けて c9e1 → d2f4 → 本 revision の順で alembic が解決する）。
downgrade も空（1 段戻すと再び 2 本の head になるだけ・データには触れない）。

Revision ID: e3f5a7b9c1d3
Revises: b2d4f6a8c0e1, d2f4a6b8c0e2
Create Date: 2026-11-07
"""
from typing import Sequence, Union

revision: str = 'e3f5a7b9c1d3'
down_revision: Union[str, Sequence[str], None] = ('b2d4f6a8c0e1', 'd2f4a6b8c0e2')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass        # merge revision（DDL なし）


def downgrade() -> None:
    pass        # merge revision（DDL なし）
