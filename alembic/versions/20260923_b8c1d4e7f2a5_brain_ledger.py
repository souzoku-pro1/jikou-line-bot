"""BRAIN-A1-LEDGER-1: 案件脳の台帳（case_fact / case_confirmation / case_event /
case_derivation / case_usage / source_ingest / link_history / sync_run / sync_cursor）

正本: 案件脳_設計_v3.md §4-1〜4-6（＋司令塔裁定 v3.1 R6: 現 head a7d3f1c9e2b4 に接続）。
表定義は hub/brain_ledger.py の metadata と同一（テストで列集合・制約名を突合）。
アプリ起動時には走らせない（D2: alembic CLI のみ）。

Revision ID: b8c1d4e7f2a5
Revises: a7d3f1c9e2b4
Create Date: 2026-09-23
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'b8c1d4e7f2a5'
down_revision: Union[str, Sequence[str], None] = 'a7d3f1c9e2b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "case_fact",
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
        sa.CheckConstraint("confidence IN ('high', 'medium', 'low')",
                           name="ck_case_fact_confidence"),
        sa.UniqueConstraint("case_app_id", "case_record_id", "subject_id", "item_code", "source_app_id", "source_record_id", "source_revision", "locator", "converter_name", "converter_version",
                            name="uq_case_fact_key"),
        sa.CheckConstraint("locator <> ''",
                           name="ck_case_fact_locator_nonempty"),
        sa.CheckConstraint('supersedes_fact_id IS NULL OR supersedes_fact_id <> fact_id',
                           name="ck_case_fact_no_self_supersede"),
    )
    op.create_index("ix_case_fact_case", "case_fact", ["case_app_id", "case_record_id"])
    op.create_index("ix_case_fact_item_value", "case_fact", ["item_code", "value_text"])
    op.create_index("ix_case_fact_source", "case_fact", ["source_app_id", "source_record_id"])
    op.create_table(
        "case_confirmation",
        sa.Column("confirmation_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("fact_id", _BIG, sa.ForeignKey("case_fact.fact_id"), nullable=False),
        sa.Column("fact_version", _BIG, nullable=False),
        sa.Column("actor", sa.Text, nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("decision", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False, server_default=''),
        sa.Column("revoked_of", _BIG, sa.ForeignKey("case_confirmation.confirmation_id"), nullable=True),
        sa.Column("operation_id", sa.Text, nullable=False, unique=True),
        sa.Column("seen_version", _BIG, nullable=False),
        sa.CheckConstraint("decision IN ('confirm', 'reject', 'revoke')",
                           name="ck_case_confirmation_decision"),
    )
    op.create_index("ix_case_confirmation_fact", "case_confirmation", ["fact_id"])
    op.create_table(
        "case_event",
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
    )
    op.create_index("ix_case_event_case", "case_event", ["case_app_id", "case_record_id"])
    op.create_table(
        "case_derivation",
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
    )
    op.create_table(
        "case_usage",
        sa.Column("usage_id", _BIG, primary_key=True, autoincrement=True),
        sa.Column("usage_kind", sa.Text, nullable=False),
        sa.Column("usage_ref", sa.Text, nullable=False),
        sa.Column("fact_id", _BIG, sa.ForeignKey("case_fact.fact_id"), nullable=False),
        sa.Column("fact_version", _BIG, nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_table(
        "source_ingest",
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
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("locator <> ''",
                           name="ck_source_ingest_locator_nonempty"),
        sa.UniqueConstraint("source_app_id", "source_record_id", "source_revision", "locator", "converter_name", "converter_version",
                            name="uq_source_ingest_key"),
        sa.CheckConstraint("state IN ('ingested', 'mismatch_hold', 'unavailable', 'held')",
                           name="ck_source_ingest_state"),
    )
    op.create_index("ix_source_ingest_source", "source_ingest", ["source_app_id", "source_record_id"])
    op.create_table(
        "link_history",
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
        sa.CheckConstraint("trust_level IN ('auto', 'candidate', 'hold')",
                           name="ck_link_history_trust"),
    )
    op.create_index("ix_link_history_source", "link_history", ["source_app_id", "source_record_id"])
    op.create_table(
        "sync_run",
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
        sa.CheckConstraint("status IN ('running', 'ok', 'failed', 'stopped')",
                           name="ck_sync_run_status"),
    )
    op.create_table(
        "sync_cursor",
        sa.Column("target_app", sa.Text, primary_key=True),
        sa.Column("cursor_updated_at", sa.Text, nullable=True),
        sa.Column("cursor_record_id", sa.Text, nullable=True),
        sa.Column("confirmed_until", sa.Text, nullable=True),
        sa.Column("last_run_id", _BIG, nullable=True),
        sa.Column("last_ok_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reconcile_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_recheck_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("state IN ('synced', 'incomplete', 'error', 'stopped')",
                           name="ck_sync_cursor_state"),
    )


def downgrade() -> None:
    op.drop_table("sync_cursor")
    op.drop_table("sync_run")
    op.drop_table("link_history")
    op.drop_table("source_ingest")
    op.drop_table("case_usage")
    op.drop_table("case_derivation")
    op.drop_table("case_event")
    op.drop_table("case_confirmation")
    op.drop_table("case_fact")
