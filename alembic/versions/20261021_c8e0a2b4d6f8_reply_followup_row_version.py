"""JIKOU-REPLY-Q1b-fix6（Codex R-Q1b-fix5 BR-18）: reply_chatlog_followup に世代番号 row_version を追加

- 古い観測（猶予超過 prepared＋台帳なし／台帳 failed）に基づく close が、その間に再準備・送信された行を not_sent／ledger_failed で閉じて
  記録を失わせる穴（BR-18）。内容を変える更新のたびに row_version を +1 し、終了更新（close／complete／所有者の release）は
  「読んだ時点の世代と一致」を AND で付けた条件付き UPDATE にする（hub/reply_followup）。
- 既存行は 0。a7c9e1f3b5d7（fix5）の次。Q1c の a1c3e5f7b9d2 は本 revision の後ろへ付け替える（前方 merge 時に適合）。

downgrade: row_version 列を落とす（世代番号は業務データではなく、落としても記録・本文は失われない）。

Revision ID: c8e0a2b4d6f8
Revises: a7c9e1f3b5d7
Create Date: 2026-10-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'c8e0a2b4d6f8'
down_revision: Union[str, Sequence[str], None] = 'a7c9e1f3b5d7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "reply_chatlog_followup"


def upgrade() -> None:
    op.add_column(TABLE, sa.Column("row_version", sa.Integer, nullable=False, server_default="0"))


def downgrade() -> None:
    op.drop_column(TABLE, "row_version")
