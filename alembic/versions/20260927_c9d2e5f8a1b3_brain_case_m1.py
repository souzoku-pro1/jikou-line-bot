"""BRAIN-ID-1a M1: 案件本体 case・識別子 case_identity・統合履歴の表と case_id 列（DDL のみ・可逆）

正本: 案件脳_設計_v4.3.md §3-1（移行 (1)）・§14-2（裁定 R28: 3 段構成の 1 段目）・
§14-3（R29: 案件識別子の重なり禁止は Postgres の EXCLUDE USING gist〔btree_gist〕・
sqlite では作らない＝dialect 条件付き DDL）・§14-4（R30: source_ingest.line_user_id）。
表定義は hub/brain_ledger.py の metadata と同一（M2 適用後に列集合・制約名を突合）。

- 新表: case（"case" は予約語のため常に引用符つき）・case_identity・merge_history・
  subject_merge_history
- case_fact / case_event / case_derivation / source_ingest に case_id（NULL 可・FK は M2）・
  case_fact に prev_case_id（NULL 可）・link_history に prev_case_id / new_case_id（NULL 可）・
  source_ingest に line_user_id（NULL 可）・索引
- sync_run.status に stopped_stale（R28）
- この revision だけを適用した状態（新形式データゼロ）では挙動は A1 と同じ
  （test_brain_id1a_migration が pin）。downgrade は列・表を落として A1 形へ戻す
  （stopped_stale の run は stopped に写す）。
アプリ起動時には走らせない（D2: alembic CLI のみ）。

Revision ID: c9d2e5f8a1b3
Revises: b8c1d4e7f2a5
Create Date: 2026-09-27
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

revision: str = 'c9d2e5f8a1b3'
down_revision: Union[str, Sequence[str], None] = 'b8c1d4e7f2a5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

RUN_STATUS_A1 = "status IN ('running', 'ok', 'failed', 'stopped', 'partial')"
RUN_STATUS_M1 = "status IN ('running', 'ok', 'failed', 'stopped', 'partial', 'stopped_stale')"
# R29: 案件識別子（kintone_record / receipt_number / drive_folder）の有効期間の重なり禁止
EXCLUDE_NAME = "ex_case_identity_active"
EXCLUDE_DDL = (
    "ALTER TABLE case_identity ADD CONSTRAINT ex_case_identity_active "
    "EXCLUDE USING gist (namespace WITH =, kind WITH =, value WITH =, "
    "tstzrange(valid_from, valid_to, '[)') WITH &&) "
    "WHERE (kind IN ('kintone_record', 'receipt_number', 'drive_folder'))")


def _is_postgres() -> bool:
    return op.get_bind().dialect.name == "postgresql"


def _sync_run_a1() -> sa.Table:
    """offline（--sql）モードの sqlite は batch の反射ができないため、A1 の sync_run の形を
    copy_from に渡す（online では反射を使う＝この定義は使わない）。"""
    return sa.Table(
        "sync_run", sa.MetaData(),
        sa.Column("run_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("target_app", sa.Text, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("scan_upper_bound", sa.Text, nullable=True),
        sa.Column("page_order", sa.Text, nullable=False),
        sa.Column("incomplete_page", _JSON, nullable=True),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("failure", sa.Text, nullable=True),
        sa.Column("pages_done", sa.Integer, nullable=False, server_default='0'),
        sa.Column("records_seen", sa.Integer, nullable=False, server_default='0'),
        sa.Column("confirmed_range", _JSON, nullable=True),
        sa.Column("pending_unregistered", sa.Integer, nullable=False, server_default='0'),
        sa.Column("first_unresolved_position", _JSON, nullable=True),
        sa.CheckConstraint(RUN_STATUS_A1, name="ck_sync_run_status"),
    )


def _batch(table: str, copy_from=None):
    """online は反射・offline は copy_from（sqlite の --sql 生成用）。"""
    if context.is_offline_mode() and copy_from is not None:
        return op.batch_alter_table(table, copy_from=copy_from)
    return op.batch_alter_table(table)


def upgrade() -> None:
    op.create_table(
        "case",
        sa.Column("case_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("kind", sa.Text, nullable=False, server_default='souzoku_houki'),
        sa.Column("registration", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default='active'),
        sa.Column("merged_into_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=True),
        sa.Column("created_via", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("version", _BIG, nullable=False, server_default='1'),
        sa.CheckConstraint("registration IN ('registered', 'unregistered')",
                           name="ck_case_registration"),
        sa.CheckConstraint("status IN ('active', 'merged')", name="ck_case_status"),
        sa.CheckConstraint("created_via IN ('sync', 'memo', 'manual', 'backfill')",
                           name="ck_case_created_via"),
    )
    op.create_table(
        "case_identity",
        sa.Column("identity_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
        sa.Column("namespace", sa.Text, nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("value", sa.Text, nullable=False),
        sa.Column("valid_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_to", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason", sa.Text, nullable=False, server_default=''),
        sa.Column("actor", sa.Text, nullable=False, server_default='system'),
        sa.Column("operation_id", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "kind IN ('kintone_record', 'receipt_number', 'drive_folder', 'line_user')",
            name="ck_case_identity_kind"),
        sa.CheckConstraint("value <> ''", name="ck_case_identity_value_nonempty"),
        sa.CheckConstraint("valid_to IS NULL OR valid_to >= valid_from",
                           name="ck_case_identity_period"),
    )
    op.create_index("ix_case_identity_key", "case_identity", ["namespace", "kind", "value"])
    op.create_index("ix_case_identity_case", "case_identity", ["case_id"])
    if _is_postgres():
        # R29: DB 制約は Postgres のみ（btree_gist が必要）。sqlite ではアプリ側検査のみ
        op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
        op.execute(EXCLUDE_DDL)
    op.create_table(
        "merge_history",
        sa.Column("merge_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("from_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
        sa.Column("into_case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default='active'),
        sa.Column("actor", sa.Text, nullable=False, server_default='system'),
        sa.Column("reason", sa.Text, nullable=False, server_default=''),
        sa.Column("operation_id", sa.Text, nullable=True, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("reverted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revert_operation_id", sa.Text, nullable=True, unique=True),
        sa.CheckConstraint("status IN ('active', 'reverted')", name="ck_merge_history_status"),
        sa.CheckConstraint("from_case_id <> into_case_id", name="ck_merge_history_distinct"),
    )
    op.create_table(
        "subject_merge_history",
        sa.Column("subject_merge_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("case_id", _BIG, sa.ForeignKey("case.case_id"), nullable=False),
        sa.Column("from_subject_id", sa.Text, nullable=False),
        sa.Column("into_subject_id", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False, server_default='active'),
        sa.Column("actor", sa.Text, nullable=False, server_default='system'),
        sa.Column("reason", sa.Text, nullable=False, server_default=''),
        sa.Column("operation_id", sa.Text, nullable=True, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('active', 'reverted')",
                           name="ck_subject_merge_history_status"),
    )
    # case_id 列（NULL 可・FK は M2 で付ける）と索引（ADD COLUMN は sqlite でも batch 不要）
    op.add_column("case_fact", sa.Column("case_id", _BIG, nullable=True))
    op.add_column("case_fact", sa.Column("prev_case_id", _BIG, nullable=True))
    op.create_index("ix_case_fact_case_id", "case_fact", ["case_id"])
    op.add_column("case_event", sa.Column("case_id", _BIG, nullable=True))
    op.create_index("ix_case_event_case_id", "case_event", ["case_id"])
    op.add_column("case_derivation", sa.Column("case_id", _BIG, nullable=True))
    op.create_index("ix_case_derivation_case_id", "case_derivation", ["case_id"])
    op.add_column("source_ingest", sa.Column("case_id", _BIG, nullable=True))
    op.add_column("source_ingest", sa.Column("line_user_id", sa.Text, nullable=True))
    op.create_index("ix_source_ingest_case_id", "source_ingest", ["case_id"])
    op.create_index("ix_source_ingest_line_user", "source_ingest", ["line_user_id"])
    op.add_column("link_history", sa.Column("prev_case_id", _BIG, nullable=True))
    op.add_column("link_history", sa.Column("new_case_id", _BIG, nullable=True))
    # R28: stopped_stale
    with _batch("sync_run", _sync_run_a1()) as b:
        b.drop_constraint("ck_sync_run_status", type_="check")
        b.create_check_constraint("ck_sync_run_status", RUN_STATUS_M1)


def downgrade() -> None:
    sync_run = sa.table("sync_run", sa.column("status", sa.Text))
    op.execute(sync_run.update().where(sync_run.c.status == "stopped_stale")
               .values(status="stopped"))
    with op.batch_alter_table("sync_run") as b:
        b.drop_constraint("ck_sync_run_status", type_="check")
        b.create_check_constraint("ck_sync_run_status", RUN_STATUS_A1)
    with op.batch_alter_table("link_history") as b:
        b.drop_column("new_case_id")
        b.drop_column("prev_case_id")
    op.drop_index("ix_source_ingest_line_user", table_name="source_ingest")
    op.drop_index("ix_source_ingest_case_id", table_name="source_ingest")
    with op.batch_alter_table("source_ingest") as b:
        b.drop_column("line_user_id")
        b.drop_column("case_id")
    op.drop_index("ix_case_derivation_case_id", table_name="case_derivation")
    with op.batch_alter_table("case_derivation") as b:
        b.drop_column("case_id")
    op.drop_index("ix_case_event_case_id", table_name="case_event")
    with op.batch_alter_table("case_event") as b:
        b.drop_column("case_id")
    op.drop_index("ix_case_fact_case_id", table_name="case_fact")
    with op.batch_alter_table("case_fact") as b:
        b.drop_column("prev_case_id")
        b.drop_column("case_id")
    op.drop_table("subject_merge_history")
    op.drop_table("merge_history")
    if _is_postgres():
        op.execute("ALTER TABLE case_identity DROP CONSTRAINT IF EXISTS ex_case_identity_active")
    op.drop_index("ix_case_identity_case", table_name="case_identity")
    op.drop_index("ix_case_identity_key", table_name="case_identity")
    op.drop_table("case_identity")
    op.drop_table("case")
