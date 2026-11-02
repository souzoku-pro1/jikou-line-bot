"""JIKOU-REPLY-Q1b-fix5（Codex R-Q1b-fix4 BR-17）: reply_chatlog_followup に「外部作成の確定状態」列 create_state を追加

- 従来は last_result 1 列に「外部作成の確定状態（creating／unknown）」と「直近の診断理由（destination_mismatch・lookup_unavailable 等）」が
  同居し、宛先再取得の失敗が unknown を上書きして消す穴があった（BR-17）。
- create_state（閉集合 none／creating／unknown／created・NOT NULL・既定 none）を分離し、last_result は **診断理由だけ** を持つ。
  遷移は単調（none→creating→unknown／created、unknown→created のみ）＝hub/reply_followup の条件付き UPDATE が担う。
- 既存行の移行: last_result から導出（unknown→unknown・creating→creating・written／recovered→created・それ以外→none）。
- CHECK 制約は postgresql のみ DB に置く（sqlite は表の作り直しが要り offline --sql で表定義が必要になるため置かない。
  値の閉集合はアプリ側の定数と metadata の CheckConstraint が担う）。
- 本 revision は f7b2c9d4e1a6 の次（Q1c の a1c3e5f7b9d2 は本 revision の後ろへ付け替える＝前方 merge 時に適合）。

downgrade: create_state 列を落とす。**none 以外の値を持つ行が 1 行でもあれば拒否**（確定状態が失われる＝RV-08）。

Revision ID: a7c9e1f3b5d7
Revises: f7b2c9d4e1a6
Create Date: 2026-10-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'a7c9e1f3b5d7'
down_revision: Union[str, Sequence[str], None] = 'f7b2c9d4e1a6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "reply_chatlog_followup"
CK = "ck_reply_followup_create_state"


def _is_pg() -> bool:
    return op.get_context().dialect.name == "postgresql"


def upgrade() -> None:
    op.add_column(TABLE, sa.Column("create_state", sa.Text, nullable=False, server_default="none"))
    if _is_pg():
        op.create_check_constraint(CK, TABLE, "create_state IN ('none', 'creating', 'unknown', 'created')")
    # 既存行の移行（last_result から導出・値は出さない）
    op.execute(sa.text(
        f"UPDATE \"{TABLE}\" SET create_state = CASE last_result "
        "WHEN 'unknown' THEN 'unknown' WHEN 'creating' THEN 'creating' "
        "WHEN 'written' THEN 'created' WHEN 'recovered' THEN 'created' ELSE 'none' END"))


def _refuse_if_state_present() -> None:
    bind = op.get_bind()
    n = bind.execute(sa.text(f"SELECT COUNT(*) FROM \"{TABLE}\" WHERE create_state <> 'none'")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("reply_followup_create_state_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_state_present()
    if _is_pg():
        op.drop_constraint(CK, TABLE, type_="check")
    op.drop_column(TABLE, "create_state")
