"""RV-08: person_merge_operation（名寄せ統合の immutable 操作台帳・裁定⑦(B)）

正本: DRAFT_RV08_SOFT_MERGE.md §3.2a。
payload は fingerprint/record ID のみ（PII 非保持・RV10 準拠）。
immutable 強制は DB trigger（BEFORE UPDATE/DELETE 拒否）を両 dialect に付与。

Revision ID: b9c4e7f2a6d1
Revises: e7a3c9d2b5f1
Create Date: 2026-08-12
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'b9c4e7f2a6d1'
down_revision: Union[str, Sequence[str], None] = 'e7a3c9d2b5f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "person_merge_operation",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("operation_id", sa.Text, nullable=False),
        sa.Column("pair_key", sa.Text, nullable=False),
        sa.Column("envelope_record_id", sa.Text, nullable=False),
        sa.Column("winner_id", sa.Text, nullable=False),
        sa.Column("loser_id", sa.Text, nullable=False),
        sa.Column("stage", sa.Text, nullable=False),
        sa.Column("payload", _JSON, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.CheckConstraint("stage IN ('preimage', 'postimage', 'restore')",
                           name="ck_person_merge_operation_stage"),
        sa.UniqueConstraint("operation_id", "stage",
                            name="uq_person_merge_operation_stage"),
    )
    op.create_index("ix_person_merge_operation_envelope",
                    "person_merge_operation",
                    ["envelope_record_id", "pair_key"])
    # immutable trigger（両 dialect・モジュール定義と単一ソース共用）
    from hub.derivation_models import immutable_trigger_ddl
    dialect = op.get_bind().dialect.name
    for stmt in immutable_trigger_ddl("person_merge_operation").get(dialect, []):
        op.execute(stmt)


def downgrade() -> None:
    dialect = op.get_bind().dialect.name
    table = "person_merge_operation"
    if dialect == "sqlite":
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_update")
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_delete")
    elif dialect == "postgresql":
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_no_mutation ON {table}")
        op.execute(f"DROP FUNCTION IF EXISTS {table}_immutable()")
    op.drop_index("ix_person_merge_operation_envelope", table_name=table)
    op.drop_table(table)
