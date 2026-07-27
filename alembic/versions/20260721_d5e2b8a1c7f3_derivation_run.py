"""P3-001: derivation_run + heir_confirmation_decision（App36 導出台帳・NH01 分離）

正本: DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md §2。
immutable 強制は DB trigger（BEFORE UPDATE/DELETE 拒否）を両 dialect に付与。

Revision ID: d5e2b8a1c7f3
Revises: c4f1a2b7d8e9
Create Date: 2026-07-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'd5e2b8a1c7f3'
down_revision: Union[str, Sequence[str], None] = 'c4f1a2b7d8e9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "derivation_run",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("case_app_id", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("decedent_person_id", sa.Text, nullable=False),
        sa.Column("at_date", sa.Text, nullable=False),
        sa.Column("frozen_case_version", sa.Text, nullable=False),
        sa.Column("input_person_revisions", _JSON, nullable=False),
        sa.Column("input_person_ids", _JSON, nullable=False),
        sa.Column("input_hash", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("rank", sa.Integer, nullable=False),
        sa.Column("result_payload", _JSON, nullable=False),
        sa.Column("result_hash", sa.Text, nullable=False),
        sa.Column("lawyer_flags", _JSON, nullable=True),
        sa.Column("provisional", sa.Boolean, nullable=False),
        sa.Column("supersedes_run_id", sa.BigInteger,
                  sa.ForeignKey("derivation_run.id"), nullable=True),
        sa.Column("engine_version", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.CheckConstraint("status IN ('derived', 'held', 'error')",
                           name="ck_derivation_run_status"),
        sa.CheckConstraint("rank IN (0, 1, 2, 3)", name="ck_derivation_run_rank"),
        sa.CheckConstraint("supersedes_run_id IS NULL OR supersedes_run_id != id",
                           name="ck_derivation_run_no_self_supersede"),
        sa.UniqueConstraint("supersedes_run_id", name="uq_derivation_run_supersedes"),
    )
    # fix2 H03: 同一 case の root は 1 行のみ（部分ユニーク・head 一意性の DB 担保）
    op.create_index("uq_derivation_run_single_root", "derivation_run",
                    ["case_record_id"], unique=True,
                    sqlite_where=sa.text("supersedes_run_id IS NULL"),
                    postgresql_where=sa.text("supersedes_run_id IS NULL"))
    op.create_table(
        "heir_confirmation_decision",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("derivation_run_id", sa.BigInteger,
                  sa.ForeignKey("derivation_run.id"), nullable=False),
        sa.Column("decision", sa.Text, nullable=False),
        sa.Column("decided_by", sa.Text, nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amendments", _JSON, nullable=True),   # 正本 §2.2「修正内容」
        sa.Column("supersedes_decision_id", sa.BigInteger,
                  sa.ForeignKey("heir_confirmation_decision.id"), nullable=True),
        sa.CheckConstraint("decision IN ('confirmed', 'held', 'rejected')",
                           name="ck_heir_decision_decision"),
        sa.CheckConstraint(
            "supersedes_decision_id IS NULL OR supersedes_decision_id != id",
            name="ck_heir_decision_no_self_supersede"),   # fix2 H04
        sa.UniqueConstraint("supersedes_decision_id",
                            name="uq_heir_decision_supersedes"),
    )
    # fix3 H04: 同一 run の root decision は 1 行のみ（部分ユニーク）
    op.create_index("uq_heir_decision_single_root", "heir_confirmation_decision",
                    ["derivation_run_id"], unique=True,
                    sqlite_where=sa.text("supersedes_decision_id IS NULL"),
                    postgresql_where=sa.text("supersedes_decision_id IS NULL"))
    # immutable trigger（両 dialect・モジュール定義と単一ソース共用）
    from hub.derivation_models import immutable_trigger_ddl
    dialect = op.get_bind().dialect.name
    for table in ("derivation_run", "heir_confirmation_decision"):
        for stmt in immutable_trigger_ddl(table).get(dialect, []):
            op.execute(stmt)


def downgrade() -> None:
    dialect = op.get_bind().dialect.name
    for table in ("heir_confirmation_decision", "derivation_run"):
        if dialect == "sqlite":
            op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_update")
            op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_delete")
        elif dialect == "postgresql":
            op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_mutation ON {table}")
            op.execute(f"DROP FUNCTION IF EXISTS {table}_immutable()")
        if table == "derivation_run":
            op.drop_index("uq_derivation_run_single_root", table_name="derivation_run")
        else:
            op.drop_index("uq_heir_decision_single_root",
                          table_name="heir_confirmation_decision")
        op.drop_table(table)
