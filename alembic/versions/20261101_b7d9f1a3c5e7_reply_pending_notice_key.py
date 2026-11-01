"""JIKOU-REPLY-Q1c-fix2（Codex R-Q1c-fix1 BC-07）: 対応待ち通知の通知キーの永続化と claim 列（列追加のみ）

- reply_pending.notice_key: 業務 LINE 通知の台帳 key（`pending_notice:{会話 ref|ハッシュ先頭 16}:{JST 日付}`）を **初回に 1 度だけ**
  算出して保存する。再試行・再配送はこの保存値で通知する（会話参照の取得障害でハッシュへ落ちる／日付をまたぐ、で key が変わり
  2 通目が出ることを防ぐ）。
- reply_pending.notify_owner / notify_claimed_at: 通知処理の claim（条件付き更新）。取れた 1 者だけが送る（並行する回収ジョブ・
  再配送の二重通知を防ぐ）。リース（NOTIFY_CLAIM_LEASE_SECONDS）超過で再取得可。
- reply_pending_retry.notice_key: 永続キュー → 対応待ち登録 のとき同じ key を引き継ぐ。
- 本文・氏名・LINE userId は置かない（key は不透明 ref／ハッシュ先頭 16 文字と日付のみ）。d4f6a8b0c2e4（fix1）の次。
  アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 列を落とす。**notice_key にデータがあれば downgrade 禁止**（1 行でも非 NULL なら例外で拒否＝RV-08・値は出さない）。

Revision ID: b7d9f1a3c5e7
Revises: d4f6a8b0c2e4
Create Date: 2026-11-01
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'b7d9f1a3c5e7'
down_revision: Union[str, Sequence[str], None] = 'd4f6a8b0c2e4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

PENDING = "reply_pending"
RETRY = "reply_pending_retry"


def upgrade() -> None:
    with op.batch_alter_table(PENDING) as b:
        b.add_column(sa.Column("notice_key", sa.Text, nullable=True))
        b.add_column(sa.Column("notify_owner", sa.Text, nullable=True))
        b.add_column(sa.Column("notify_claimed_at", sa.DateTime(timezone=True), nullable=True))
    with op.batch_alter_table(RETRY) as b:
        b.add_column(sa.Column("notice_key", sa.Text, nullable=True))


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    for table in (PENDING, RETRY):
        n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {table} WHERE notice_key IS NOT NULL")).scalar()
        if int(n or 0) > 0:
            raise RuntimeError("reply_pending_notice_key_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    with op.batch_alter_table(RETRY) as b:
        b.drop_column("notice_key")
    with op.batch_alter_table(PENDING) as b:
        b.drop_column("notify_claimed_at")
        b.drop_column("notify_owner")
        b.drop_column("notice_key")
