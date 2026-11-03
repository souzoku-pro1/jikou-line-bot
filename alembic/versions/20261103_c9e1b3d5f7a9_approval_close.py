"""UX-PENDING-1 (2)(9): 承認キューだけにある件を統合一覧で閉じた記録 approval_close（追記のみ）＋ reply_pending.state に closed_external

- V2 以前の承認キュー項目（reply_pending に行が無い App 29 レコード）を「対応終了（別経路で返信済み）」「不要」で閉じた記録。
  最新行の outcome がその件の状態（done_external / not_needed / reopened）。kintone は書かない。
- 本文・氏名・LINE userId は置かない（レコード ID・結果・操作者・時刻のみ）。b7d9f1a3c5e7（Q1c-fix2）の次。
  アプリ起動時には走らせない（D2: alembic CLI のみ）。

- reply_pending.state の CHECK 制約 ck_reply_pending_state に 'closed_external'（対応終了＝別経路で返信済み）を加える（列は増えない）。
  sqlite は表の作り直し（batch）になるため、b7d9 時点の表定義を **copy_from で明示**（反射に頼らない＝`alembic upgrade --sql` の offline
  モードでも通る・索引 2 本と一意制約も作り直しで維持）。PostgreSQL は ALTER TABLE DROP/ADD CONSTRAINT に落ちる。

downgrade: approval_close を落とし、CHECK 制約を 4 値に戻す。**approval_close に 1 行でもあれば／state=closed_external の行が 1 行でもあれば
downgrade 禁止**（RV-08・値は出さない）。

Revision ID: c9e1b3d5f7a9
Revises: b7d9f1a3c5e7
Create Date: 2026-11-03
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'c9e1b3d5f7a9'
down_revision: Union[str, Sequence[str], None] = 'b7d9f1a3c5e7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "approval_close"
PENDING = "reply_pending"
CK_STATE = "ck_reply_pending_state"
STATES_OLD = "state IN ('open', 'human_replied', 'auto_replied', 'not_needed')"
STATES_NEW = "state IN ('open', 'human_replied', 'auto_replied', 'not_needed', 'closed_external')"
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def _pending_table(state_expr: str) -> sa.Table:
    """b7d9 時点の reply_pending（列・制約・索引）。batch の copy_from 用（hub.reply_pending の metadata には依存しない）。"""
    return sa.Table(
        PENDING, sa.MetaData(),
        sa.Column("pending_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("channel", sa.Text, nullable=False),
        sa.Column("inbound_event_id", sa.Text, nullable=True),
        sa.Column("conversation_id", _BIG, nullable=True),
        sa.Column("line_user_hash", sa.Text, nullable=False),
        sa.Column("route", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("category", sa.Text, nullable=True),
        sa.Column("approval_record_id", sa.Text, nullable=True),
        sa.Column("state", sa.Text, nullable=False, server_default="open"),
        sa.Column("notify_state", sa.Text, nullable=False, server_default="none"),
        sa.Column("customer_send", sa.Text, nullable=False, server_default="none"),
        sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by", sa.Text, nullable=True),
        sa.Column("notice_key", sa.Text, nullable=True),
        sa.Column("notify_owner", sa.Text, nullable=True),
        sa.Column("notify_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(state_expr, name=CK_STATE),
        sa.CheckConstraint("route IN ('customer', 'hearing', 'inbound')", name="ck_reply_pending_route"),
        sa.CheckConstraint("reason IN ('demoted', 'empty_reply', 'policy_stopped', 'claude_outage')",
                           name="ck_reply_pending_reason"),
        sa.UniqueConstraint("business", "channel", "inbound_event_id", name="uq_reply_pending_inbound"),
        sa.Index("ix_reply_pending_state", "state", "created_at"),
        sa.Index("ix_reply_pending_user", "business", "line_user_hash", "state"),
    )


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("close_id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True,
                  autoincrement=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("approval_record_id", sa.Text, nullable=False),
        sa.Column("outcome", sa.Text, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_approval_close_record", TABLE, ["business", "approval_record_id"])
    with op.batch_alter_table(PENDING, copy_from=_pending_table(STATES_OLD)) as b:
        b.drop_constraint(CK_STATE, type_="check")
        b.create_check_constraint(CK_STATE, STATES_NEW)


def downgrade() -> None:
    bind = op.get_bind()
    n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("approval_close_downgrade_refused_data_present")
    m = bind.execute(sa.text(f"SELECT COUNT(*) FROM {PENDING} WHERE state = 'closed_external'")).scalar()
    if int(m or 0) > 0:
        raise RuntimeError("reply_pending_closed_external_downgrade_refused_data_present")
    with op.batch_alter_table(PENDING, copy_from=_pending_table(STATES_NEW)) as b:
        b.drop_constraint(CK_STATE, type_="check")
        b.create_check_constraint(CK_STATE, STATES_OLD)
    op.drop_index("ix_approval_close_record", table_name=TABLE)
    op.drop_table(TABLE)
