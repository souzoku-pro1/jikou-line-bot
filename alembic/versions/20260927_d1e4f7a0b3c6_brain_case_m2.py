"""BRAIN-ID-1a M2: case_id への切替（検算つき・downgrade は新形式データの検出で拒否）

正本: 案件脳_設計_v4.3.md §3-1（移行 (3)(4)）・§14-2（裁定 R28: 3 段構成の 3 段目）。
前提: M1（c9d2e5f8a1b3）適用済み・scripts/brain_case_backfill.py --apply → --verify 済み。

upgrade():
  1. 冒頭で hub.brain_migration.verify（backfill --verify と**同じ関数**）を再実行し、
     失敗なら RuntimeError で中止（DDL に入らない・固定語彙の問題名のみ）
  2. 必須表（case_fact / case_event / case_derivation）の case_id を NOT NULL + FK(case)、
     NULL 可の表（source_ingest・link_history の prev/new・case_fact.prev_case_id）にも FK
  3. uq_case_fact_key を (case_id, subject_id, item_code, 出典 6 列) に置換
  4. 旧案件キー列（case_app_id / case_record_id）を NULL 許容
downgrade():
  §14-2 の拒否条件（未登録案件・統合済み・同一 (名前空間,種別,値) の識別子 2 行以上・
  merge_history / subject_merge_history 1 行以上・案件キー NULL かつ case_id ありの行）の
  いずれか 1 件で RuntimeError。ゼロなら idem_key を A1 形式へ戻し、制約・NULL 許容を
  M1 の形へ戻す（sqlite 往復のため batch_alter_table）。
アプリ起動時には走らせない（D2: alembic CLI のみ）。

Revision ID: d1e4f7a0b3c6
Revises: c9d2e5f8a1b3
Create Date: 2026-09-27
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import context, op
from sqlalchemy.dialects import postgresql

from hub import brain_migration

revision: str = 'd1e4f7a0b3c6'
down_revision: Union[str, Sequence[str], None] = 'c9d2e5f8a1b3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

UQ_A1 = ("case_app_id", "case_record_id", "subject_id", "item_code", "source_app_id",
         "source_record_id", "source_revision", "locator", "converter_name",
         "converter_version")
UQ_M2 = ("case_id", "subject_id", "item_code", "source_app_id", "source_record_id",
         "source_revision", "locator", "converter_name", "converter_version")


_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


class BrainMigrationRefused(RuntimeError):
    """検算失敗／downgrade 拒否（本文は固定語彙の問題名のみ・PII なし）。"""


def _m1_tables() -> dict:
    """offline（--sql）モードの sqlite は batch の反射ができないため、M1 の形（A1 の表＋
    M1 の NULL 可の列）を copy_from に渡す。online では反射を使う（この定義は使わない）。
    --sql の出力は生成スクリプトの煙テスト用であり、本番（Postgres）は online で適用する。"""
    md = sa.MetaData()
    case_fact = sa.Table(
        "case_fact", md,
        sa.Column("fact_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("case_app_id", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("subject_id", sa.Text, nullable=False),
        sa.Column("item_code", sa.Text, nullable=False),
        sa.Column("value_type", sa.Text, nullable=False),
        sa.Column("value_text", sa.Text, nullable=False, server_default=''),
        sa.Column("value_json", _JSON, nullable=True),
        sa.Column("source_kind", sa.Text, nullable=False),
        sa.Column("source_app_id", sa.Text, nullable=False),
        sa.Column("source_record_id", sa.Text, nullable=False),
        sa.Column("source_revision", _BIG, nullable=False),
        sa.Column("locator", sa.Text, nullable=False, server_default='-'),
        sa.Column("converter_name", sa.Text, nullable=False),
        sa.Column("converter_version", sa.Text, nullable=False),
        sa.Column("observation_id", sa.Text, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("confidence", sa.Text, nullable=False),
        sa.Column("is_current", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("supersedes_fact_id", _BIG, sa.ForeignKey("case_fact.fact_id"), nullable=True, unique=True),
        sa.Column("invalid_reason", sa.Text, nullable=True),
        sa.Column("invalidated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("prev_case_app_id", sa.Text, nullable=True),
        sa.Column("prev_case_record_id", sa.Text, nullable=True),
        sa.Column("case_id", _BIG, nullable=True),
        sa.Column("prev_case_id", _BIG, nullable=True),
        sa.CheckConstraint("confidence IN ('high', 'medium', 'low')", name="ck_case_fact_confidence"),
        sa.UniqueConstraint(*UQ_A1, name="uq_case_fact_key"),
        sa.CheckConstraint("locator <> ''", name="ck_case_fact_locator_nonempty"),
        sa.CheckConstraint('supersedes_fact_id IS NULL OR supersedes_fact_id <> fact_id',
                           name="ck_case_fact_no_self_supersede"),
        sa.Index("ix_case_fact_case", "case_app_id", "case_record_id"),
        sa.Index("ix_case_fact_item_value", "item_code", "value_text"),
        sa.Index("ix_case_fact_source", "source_app_id", "source_record_id"),
        sa.Index("ix_case_fact_case_id", "case_id"),
    )
    case_event = sa.Table(
        "case_event", md,
        sa.Column("event_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("idem_key", sa.Text, nullable=False, unique=True),
        sa.Column("case_app_id", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_app_id", sa.Text, nullable=False),
        sa.Column("source_record_id", sa.Text, nullable=False),
        sa.Column("source_revision", _BIG, nullable=False),
        sa.Column("locator", sa.Text, nullable=False, server_default='-'),
        sa.Column("summary", sa.Text, nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("is_current", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("invalid_reason", sa.Text, nullable=True),
        sa.Column("invalidated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("case_id", _BIG, nullable=True),
        sa.Index("ix_case_event_case", "case_app_id", "case_record_id"),
        sa.Index("ix_case_event_source", "source_app_id", "source_record_id"),
        sa.Index("ix_case_event_case_id", "case_id"),
    )
    case_derivation = sa.Table(
        "case_derivation", md,
        sa.Column("derivation_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("case_app_id", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("kind", sa.Text, nullable=False),
        sa.Column("input_fact_versions", _JSON, nullable=False),
        sa.Column("calculator_name", sa.Text, nullable=False),
        sa.Column("calculator_version", sa.Text, nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result", _JSON, nullable=True),
        sa.Column("needs_recalc", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("case_id", _BIG, nullable=True),
        sa.Index("ix_case_derivation_case_id", "case_id"),
    )
    source_ingest = sa.Table(
        "source_ingest", md,
        sa.Column("ingest_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("source_kind", sa.Text, nullable=False),
        sa.Column("source_app_id", sa.Text, nullable=False),
        sa.Column("source_record_id", sa.Text, nullable=False),
        sa.Column("source_revision", _BIG, nullable=False),
        sa.Column("locator", sa.Text, nullable=False, server_default='-'),
        sa.Column("converter_name", sa.Text, nullable=False),
        sa.Column("converter_version", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("case_app_id", sa.Text, nullable=True),
        sa.Column("case_record_id", sa.Text, nullable=True),
        sa.Column("hold_reason", sa.Text, nullable=True),
        sa.Column("source_updated_at", sa.Text, nullable=True),
        sa.Column("latest_seen_revision", _BIG, nullable=True),
        sa.Column("pending_recheck", sa.Boolean, nullable=False, server_default=sa.false()),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("case_id", _BIG, nullable=True),
        sa.Column("line_user_id", sa.Text, nullable=True),
        sa.Column("pending_reason", sa.Text, nullable=True),
        sa.CheckConstraint("locator <> ''", name="ck_source_ingest_locator_nonempty"),
        sa.UniqueConstraint("source_app_id", "source_record_id", "source_revision", "locator",
                            "converter_name", "converter_version", name="uq_source_ingest_key"),
        sa.CheckConstraint("state IN ('ingested', 'mismatch_hold', 'unavailable', 'held', 'detached')",
                           name="ck_source_ingest_state"),
        sa.Index("ix_source_ingest_source", "source_app_id", "source_record_id"),
        sa.Index("ix_source_ingest_case", "case_app_id", "case_record_id"),
        sa.Index("ix_source_ingest_case_id", "case_id"),
        sa.Index("ix_source_ingest_line_user", "line_user_id"),
    )
    link_history = sa.Table(
        "link_history", md,
        sa.Column("link_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("source_app_id", sa.Text, nullable=False),
        sa.Column("source_record_id", sa.Text, nullable=False),
        sa.Column("prev_case_app_id", sa.Text, nullable=True),
        sa.Column("prev_case_record_id", sa.Text, nullable=True),
        sa.Column("new_case_app_id", sa.Text, nullable=True),
        sa.Column("new_case_record_id", sa.Text, nullable=True),
        sa.Column("trust_level", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("candidates", _JSON, nullable=True),
        sa.Column("operation_id", sa.Text, nullable=True, unique=True),
        sa.Column("actor", sa.Text, nullable=False, server_default='system'),
        sa.Column("source_revision", _BIG, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("prev_case_id", _BIG, nullable=True),
        sa.Column("new_case_id", _BIG, nullable=True),
        sa.CheckConstraint("trust_level IN ('auto', 'candidate', 'hold')", name="ck_link_history_trust"),
        sa.Index("ix_link_history_source", "source_app_id", "source_record_id"),
    )
    return {"case_fact": case_fact, "case_event": case_event, "case_derivation": case_derivation,
            "source_ingest": source_ingest, "link_history": link_history}


def _batch(table: str):
    """online は反射・offline は M1 の形を copy_from に（sqlite の --sql 生成用）。"""
    if context.is_offline_mode():
        return op.batch_alter_table(table, copy_from=_m1_tables()[table])
    return op.batch_alter_table(table)


def upgrade() -> None:
    if not context.is_offline_mode():
        # 検算は接続が要る（--sql の生成スクリプトでは行えない＝本番は online で適用する）
        result = brain_migration.verify(op.get_bind())
        if not result["ok"]:
            raise BrainMigrationRefused("brain_case_verify_failed: " + ",".join(result["problems"]))
    with _batch("case_fact") as b:
        b.alter_column("case_id", existing_type=_BIG, nullable=False)
        b.create_foreign_key("fk_case_fact_case", "case", ["case_id"], ["case_id"])
        b.create_foreign_key("fk_case_fact_prev_case", "case", ["prev_case_id"], ["case_id"])
        b.drop_constraint("uq_case_fact_key", type_="unique")
        b.create_unique_constraint("uq_case_fact_key", list(UQ_M2))
        b.alter_column("case_app_id", existing_type=sa.Text, nullable=True)
        b.alter_column("case_record_id", existing_type=sa.Text, nullable=True)
    with _batch("case_event") as b:
        b.alter_column("case_id", existing_type=_BIG, nullable=False)
        b.create_foreign_key("fk_case_event_case", "case", ["case_id"], ["case_id"])
        b.alter_column("case_app_id", existing_type=sa.Text, nullable=True)
        b.alter_column("case_record_id", existing_type=sa.Text, nullable=True)
    with _batch("case_derivation") as b:
        b.alter_column("case_id", existing_type=_BIG, nullable=False)
        b.create_foreign_key("fk_case_derivation_case", "case", ["case_id"], ["case_id"])
        b.alter_column("case_app_id", existing_type=sa.Text, nullable=True)
        b.alter_column("case_record_id", existing_type=sa.Text, nullable=True)
    with _batch("source_ingest") as b:
        b.create_foreign_key("fk_source_ingest_case", "case", ["case_id"], ["case_id"])
    with _batch("link_history") as b:
        b.create_foreign_key("fk_link_history_prev_case", "case", ["prev_case_id"], ["case_id"])
        b.create_foreign_key("fk_link_history_new_case", "case", ["new_case_id"], ["case_id"])


def downgrade() -> None:
    if context.is_offline_mode():
        raise BrainMigrationRefused("brain_case_downgrade_requires_online")   # 拒否条件の検査に接続が要る
    conn = op.get_bind()
    refusals = brain_migration.downgrade_refusals(conn)
    if refusals:
        raise BrainMigrationRefused("brain_case_downgrade_refused: " + ",".join(refusals))
    brain_migration.rewrite_event_keys_legacy(conn)
    with _batch("link_history") as b:
        b.drop_constraint("fk_link_history_new_case", type_="foreignkey")
        b.drop_constraint("fk_link_history_prev_case", type_="foreignkey")
    with _batch("source_ingest") as b:
        b.drop_constraint("fk_source_ingest_case", type_="foreignkey")
    with _batch("case_derivation") as b:
        b.drop_constraint("fk_case_derivation_case", type_="foreignkey")
        b.alter_column("case_record_id", existing_type=sa.Text, nullable=False)
        b.alter_column("case_app_id", existing_type=sa.Text, nullable=False)
        b.alter_column("case_id", existing_type=_BIG, nullable=True)
    with _batch("case_event") as b:
        b.drop_constraint("fk_case_event_case", type_="foreignkey")
        b.alter_column("case_record_id", existing_type=sa.Text, nullable=False)
        b.alter_column("case_app_id", existing_type=sa.Text, nullable=False)
        b.alter_column("case_id", existing_type=_BIG, nullable=True)
    with _batch("case_fact") as b:
        b.drop_constraint("fk_case_fact_prev_case", type_="foreignkey")
        b.drop_constraint("fk_case_fact_case", type_="foreignkey")
        b.drop_constraint("uq_case_fact_key", type_="unique")
        b.create_unique_constraint("uq_case_fact_key", list(UQ_A1))
        b.alter_column("case_record_id", existing_type=sa.Text, nullable=False)
        b.alter_column("case_app_id", existing_type=sa.Text, nullable=False)
        b.alter_column("case_id", existing_type=_BIG, nullable=True)
