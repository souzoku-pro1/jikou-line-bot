"""LABEL-PRINT-1-fix2: ジョブの所有者・期限／イベント履歴／シート管理行／消費バッチ（Codex LPF1-01〜04）

- label_print_job: owner_token・lease_expires_at を追加、status に 'failed' を追加（CHECK 更新）
- label_event: event_id 一意・kind ∈ {ok, printed, undo, new_sheet}・job_id。label_sheet_op の行は
  label_event に移して label_sheet_op を drop（kind はそのまま）
- label_sheet_control: layout 主キー・current_sheet_id。既定レイアウト A4_2x5_aone31514 を seed
  （既存の最新シート行があればそれを current にする）
- label_sheet_state: sheet_no を追加し sheet_id（S-n）から埋め、(layout, sheet_no) の一意インデックス

前リビジョン（b2c3d4e5f6a7）が適用済み／未適用のどちらでも安全に通す: online では inspector で
存在確認して create/alter・seed は無ければ挿入。offline（--sql）は接続が無いので無条件 DDL を出す。

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-09-10
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_BigInt = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_Json = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

DEFAULT_LAYOUT = "A4_2x5_aone31514"
STATUS_CHECK_OLD = "status IN ('reserved', 'attached', 'printed', 'undone', 'cancelled')"
STATUS_CHECK_NEW = "status IN ('reserved', 'attached', 'printed', 'undone', 'cancelled', 'failed')"
UQ_SHEET_NO = "uq_label_sheet_state_layout_sheet_no"


def _event_columns():
    return [
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.Text, nullable=False, unique=True),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("job_id", _BigInt, nullable=True),
        sa.Column("sheet_id", sa.Text, nullable=False),
        sa.Column("payload", _Json, nullable=False),
        sa.Column("result", _Json, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("created_by", sa.Text, nullable=False, server_default=""),
        sa.CheckConstraint("kind IN ('ok', 'printed', 'undo', 'new_sheet')",
                           name="ck_label_event_kind"),
    ]


def _control_columns():
    return [
        sa.Column("layout", sa.Text, primary_key=True),
        sa.Column("current_sheet_id", sa.Text, nullable=False, server_default=""),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
    ]


def _replace_status_check(dialect: str) -> None:
    """CHECK の差し替え。PostgreSQL は drop/create、sqlite は batch（テーブル再作成）"""
    if dialect == "sqlite":
        with op.batch_alter_table("label_print_job", recreate="always") as batch:
            batch.drop_constraint("ck_label_print_job_status", type_="check")
            batch.create_check_constraint("ck_label_print_job_status", STATUS_CHECK_NEW)
        return
    op.drop_constraint("ck_label_print_job_status", "label_print_job", type_="check")
    op.create_check_constraint("ck_label_print_job_status", "label_print_job", STATUS_CHECK_NEW)


def _copy_ops_to_events() -> None:
    op.execute(sa.text(
        "INSERT INTO label_event (event_id, kind, job_id, sheet_id, payload, result, created_at, created_by) "
        "SELECT event_id, kind, NULL, sheet_id, payload, result, created_at, created_by FROM label_sheet_op"))


def _backfill_sheet_no() -> None:
    # sheet_id は 'S-<n>'（3 文字目以降が番号）。substr/CAST は PostgreSQL・sqlite 共通
    op.execute(sa.text(
        "UPDATE label_sheet_state SET sheet_no = CAST(substr(sheet_id, 3) AS INTEGER) "
        "WHERE sheet_no = 0 AND substr(sheet_id, 1, 2) = 'S-'"))


def upgrade() -> None:
    if context.is_offline_mode():
        _upgrade_offline()
        return
    bind = op.get_bind()
    insp = sa.inspect(bind)
    dialect = bind.dialect.name
    tables = set(insp.get_table_names())

    # ── A. label_print_job: owner_token / lease_expires_at / status 'failed' ──
    job_cols = {c["name"] for c in insp.get_columns("label_print_job")} if "label_print_job" in tables else set()
    if "label_print_job" in tables:
        if "owner_token" not in job_cols:
            op.add_column("label_print_job",
                          sa.Column("owner_token", sa.Text, nullable=False, server_default=""))
        if "lease_expires_at" not in job_cols:
            op.add_column("label_print_job",
                          sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
        checks = {c["name"]: c.get("sqltext", "") for c in insp.get_check_constraints("label_print_job")}
        if "ck_label_print_job_status" in checks and "failed" not in (checks["ck_label_print_job_status"] or ""):
            _replace_status_check(dialect)

    # ── B. label_event（label_sheet_op を統合して drop） ──
    if "label_event" not in tables:
        op.create_table("label_event", *_event_columns())
    if "label_sheet_op" in tables:
        _copy_ops_to_events()
        op.drop_table("label_sheet_op")

    # ── C. label_sheet_control（seed）・label_sheet_state.sheet_no 一意 ──
    if "label_sheet_control" not in tables:
        op.create_table("label_sheet_control", *_control_columns())
    if "label_sheet_state" in tables:
        state_cols = {c["name"] for c in insp.get_columns("label_sheet_state")}
        if "sheet_no" not in state_cols:
            op.add_column("label_sheet_state",
                          sa.Column("sheet_no", sa.Integer, nullable=False, server_default="0"))
        _backfill_sheet_no()
        indexes = {i["name"] for i in insp.get_indexes("label_sheet_state")}
        if UQ_SHEET_NO not in indexes:
            op.create_index(UQ_SHEET_NO, "label_sheet_state", ["layout", "sheet_no"], unique=True)
        latest = bind.execute(sa.text(
            "SELECT sheet_id FROM label_sheet_state WHERE layout = :layout "
            "ORDER BY sheet_no DESC, id DESC LIMIT 1"), {"layout": DEFAULT_LAYOUT}).first()
        current = latest[0] if latest else ""
    else:
        current = ""
    exists = bind.execute(sa.text(
        "SELECT 1 FROM label_sheet_control WHERE layout = :layout"), {"layout": DEFAULT_LAYOUT}).first()
    if not exists:
        op.execute(sa.text(
            "INSERT INTO label_sheet_control (layout, current_sheet_id, updated_at) "
            "VALUES (:layout, :current, CURRENT_TIMESTAMP)").bindparams(layout=DEFAULT_LAYOUT, current=current))


def _upgrade_offline() -> None:
    """--sql モード（接続なし＝inspector 不可）: 前リビジョン適用済みの形を前提に無条件 DDL を出す"""
    dialect = context.get_context().dialect.name
    op.add_column("label_print_job", sa.Column("owner_token", sa.Text, nullable=False, server_default=""))
    op.add_column("label_print_job", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    if dialect != "sqlite":          # sqlite の batch 再作成は接続が要る（offline では CHECK 差し替えを出さない）
        _replace_status_check(dialect)
    op.create_table("label_event", *_event_columns())
    _copy_ops_to_events()
    op.drop_table("label_sheet_op")
    op.create_table("label_sheet_control", *_control_columns())
    op.add_column("label_sheet_state", sa.Column("sheet_no", sa.Integer, nullable=False, server_default="0"))
    _backfill_sheet_no()
    op.create_index(UQ_SHEET_NO, "label_sheet_state", ["layout", "sheet_no"], unique=True)
    op.execute(sa.text(
        "INSERT INTO label_sheet_control (layout, current_sheet_id, updated_at) "
        "VALUES (:layout, '', CURRENT_TIMESTAMP)").bindparams(layout=DEFAULT_LAYOUT))


def downgrade() -> None:
    dialect = context.get_context().dialect.name
    op.drop_index(UQ_SHEET_NO, table_name="label_sheet_state")
    op.drop_column("label_sheet_state", "sheet_no")
    op.drop_table("label_sheet_control")
    op.create_table(
        "label_sheet_op",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.Text, nullable=False, unique=True),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("payload", _Json, nullable=False),
        sa.Column("result", _Json, nullable=False),
        sa.Column("sheet_id", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("created_by", sa.Text, nullable=False, server_default=""),
    )
    op.execute(sa.text(
        "INSERT INTO label_sheet_op (event_id, kind, payload, result, sheet_id, created_at, created_by) "
        "SELECT event_id, kind, payload, result, sheet_id, created_at, created_by FROM label_event "
        "WHERE kind IN ('printed', 'undo', 'new_sheet')"))
    op.drop_table("label_event")
    # failed を持つ行は CHECK を戻せないので reserved へ（回収対象のまま）
    op.execute(sa.text("UPDATE label_print_job SET status = 'reserved' WHERE status = 'failed'"))
    if dialect == "sqlite":
        with op.batch_alter_table("label_print_job", recreate="always") as batch:
            batch.drop_constraint("ck_label_print_job_status", type_="check")
            batch.create_check_constraint("ck_label_print_job_status", STATUS_CHECK_OLD)
            batch.drop_column("lease_expires_at")
            batch.drop_column("owner_token")
        return
    op.drop_constraint("ck_label_print_job_status", "label_print_job", type_="check")
    op.create_check_constraint("ck_label_print_job_status", "label_print_job", STATUS_CHECK_OLD)
    op.drop_column("label_print_job", "lease_expires_at")
    op.drop_column("label_print_job", "owner_token")
