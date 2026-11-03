"""JIKOU-REPLY-Q4: reply_draft_approval に種別 outcome（無修正承認／修正承認／却下）を追加（DDL のみ）

正本: 時効LINEボット_返信規則_v1.4.md §10-1（このまま送る／直して送る／送らない）・§10-7（差分の大小を保存）・
§10-9。QUEUE-2026-10-13-D D-3 (b)。表定義は hub/reply_exemplar.py の metadata（reply_draft_approval）と同一。
アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 列を落とす。**outcome が入った行があれば downgrade 禁止**（RV-08）。
SQLite は batch_alter_table で列削除（本番 PostgreSQL は ALTER TABLE DROP COLUMN）。

Revision ID: d4f6b8c0e2a4
Revises: c3e5a7b9d1f3
Create Date: 2026-10-13
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd4f6b8c0e2a4'
down_revision: Union[str, Sequence[str], None] = 'c3e5a7b9d1f3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    with op.batch_alter_table("reply_draft_approval") as b:
        b.add_column(sa.Column("outcome", sa.Text, nullable=True))


def _refuse_if_data_present() -> None:
    n = op.get_bind().execute(sa.text("SELECT COUNT(*) FROM reply_draft_approval WHERE outcome IS NOT NULL")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("reply_draft_approval_outcome_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    with op.batch_alter_table("reply_draft_approval") as b:
        b.drop_column("outcome")
