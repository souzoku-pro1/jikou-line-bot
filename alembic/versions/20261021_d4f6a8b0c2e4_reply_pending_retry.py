"""JIKOU-REPLY-Q1c-fix1（Codex R-Q1c BC-03）: 対応待ちの保存失敗の永続キュー reply_pending_retry（DDL のみ）

- 無言にした受信の対応待ち（reply_pending）の register が失敗したとき、受信イベント ID と閉集合値（route／reason／
  ハッシュ／カテゴリ／App 29 No.）だけを 1 行置き、回収ジョブ（send_ledger の recover hook・hub/send_policy
  run_pending_retry_job）が次回以降に register → 業務 LINE 通知 を再処理する。本文・氏名・LINE userId は置かない。
- 鍵は 業務・チャネル・受信イベント ID（再配送は冪等）。処理済み行は state=done で残す（RV-08）。
- 表定義は hub/reply_pending.py の metadata（reply_pending_retry）と同一。a1c3e5f7b9d2（Q1c）の次。
  アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 表を落とす。**データがあれば downgrade 禁止**（1 行でもあれば例外で拒否＝RV-08・値は出さない）。

Revision ID: d4f6a8b0c2e4
Revises: a1c3e5f7b9d2
Create Date: 2026-10-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd4f6a8b0c2e4'
down_revision: Union[str, Sequence[str], None] = 'a1c3e5f7b9d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
TABLE = "reply_pending_retry"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("retry_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("channel", sa.Text, nullable=False),
        sa.Column("inbound_event_id", sa.Text, nullable=False),
        sa.Column("conversation_id", _BIG, nullable=True),
        sa.Column("line_user_hash", sa.Text, nullable=False),
        sa.Column("route", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("category", sa.Text, nullable=True),
        sa.Column("approval_record_id", sa.Text, nullable=True),
        sa.Column("state", sa.Text, nullable=False, server_default="queued"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text, nullable=True),
        sa.Column("pending_id", _BIG, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("state IN ('queued', 'done', 'abandoned')", name="ck_reply_pending_retry_state"),
        sa.UniqueConstraint("business", "channel", "inbound_event_id", name="uq_reply_pending_retry_inbound"),
    )
    op.create_index("ix_reply_pending_retry_state", TABLE, ["state", "created_at"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("reply_pending_retry_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_reply_pending_retry_state", table_name=TABLE)
    op.drop_table(TABLE)
