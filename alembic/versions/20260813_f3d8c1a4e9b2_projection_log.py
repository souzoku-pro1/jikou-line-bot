"""P3-003C-CANCEL: projection_log（confirmed projection の write-set 台帳・裁定⑤）

正本: DRAFT_P3_003C_CANCEL.md §4.1a／§6 裁定⑤=(B)。
confirmed handler が App36 へ実際に書いた行（record ID・insert/update 区別・
書込み field 集合・書込み前 preimage）を immutable 追記する。取消関所の
postimage 完全一致照合の根拠台帳（P3-001 流儀・PII 非保持）。
immutable 強制は DB trigger（BEFORE UPDATE/DELETE 拒否）を両 dialect に付与。

Revision ID: f3d8c1a4e9b2
Revises: b9c4e7f2a6d1
Create Date: 2026-08-13
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'f3d8c1a4e9b2'
down_revision: Union[str, Sequence[str], None] = 'b9c4e7f2a6d1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "projection_log",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("derivation_run_id", sa.BigInteger,
                  sa.ForeignKey("derivation_run.id"), nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("app36_record_id", sa.Text, nullable=False),
        sa.Column("op", sa.Text, nullable=False),
        sa.Column("stage", sa.Text, nullable=False),
        sa.Column("fields_written", _JSON, nullable=False),
        sa.Column("preimage", _JSON, nullable=False),
        sa.Column("schema_version", sa.Integer, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.CheckConstraint("op IN ('insert', 'update')",
                           name="ck_projection_log_op"),
        sa.CheckConstraint("stage IN ('pending', 'completed')",
                           name="ck_projection_log_stage"),
    )
    op.create_index("ix_projection_log_run", "projection_log",
                    ["derivation_run_id"])
    # immutable trigger（両 dialect・モジュール定義と単一ソース共用）
    from hub.derivation_models import immutable_trigger_ddl
    dialect = op.get_bind().dialect.name
    for stmt in immutable_trigger_ddl("projection_log").get(dialect, []):
        op.execute(stmt)


def downgrade() -> None:
    dialect = op.get_bind().dialect.name
    table = "projection_log"
    if dialect == "sqlite":
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_update")
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_delete")
    elif dialect == "postgresql":
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_mutation ON {table}")
        op.execute(f"DROP FUNCTION IF EXISTS {table}_immutable()")
    op.drop_index("ix_projection_log_run", table_name=table)
    op.drop_table(table)
