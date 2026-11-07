"""UX-PENDING-1-fix1 BU-04（Codex R-UX1）: 対応終了の「解除未完了」を永続化する attending_release（新表のみ・既存表は触らない）

- 「対応終了」の閉じ記録（reply_pending.closed_external／approval_close.done_external）と同一トランザクションで 1 行置き、
  対応中解除（conversation.attending）が失敗しても回収ジョブが再試行できるようにする。解除対象の会話 ID・human_version を固定する。
- 本文・氏名・LINE userId は置かない（ハッシュ・ID・状態・回数・時刻のみ）。c9e1b3d5f7a9（UX-PENDING-1）の次。
  アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 表を落とす。**1 行でもあれば downgrade 禁止**（RV-08・値は出さない）。

Revision ID: d2f4a6b8c0e2
Revises: c9e1b3d5f7a9
Create Date: 2026-11-04
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd2f4a6b8c0e2'
down_revision: Union[str, Sequence[str], None] = 'c9e1b3d5f7a9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "attending_release"
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("release_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("source_kind", sa.Text, nullable=False),
        sa.Column("source_id", sa.Text, nullable=False),
        sa.Column("approval_record_id", sa.Text, nullable=True),
        sa.Column("line_user_hash", sa.Text, nullable=True),
        sa.Column("conversation_id", _BIG, nullable=True),
        sa.Column("human_version", _BIG, nullable=True),
        sa.Column("state", sa.Text, nullable=False, server_default="pending"),
        sa.Column("result", sa.Text, nullable=True),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_attending_release_state", TABLE, ["business", "state"])
    op.create_index("ix_attending_release_source", TABLE, ["source_kind", "source_id"])


def downgrade() -> None:
    bind = op.get_bind()
    n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("attending_release_downgrade_refused_data_present")
    op.drop_index("ix_attending_release_source", table_name=TABLE)
    op.drop_index("ix_attending_release_state", table_name=TABLE)
    op.drop_table(TABLE)
