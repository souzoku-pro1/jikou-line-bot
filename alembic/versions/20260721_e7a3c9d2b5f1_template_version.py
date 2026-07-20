"""P3-002: template_version（TemplateVersion registry・§9.23 全 field・単一 active）

正本: DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md §5。
内容列 immutable trigger＋部分ユニーク（single active）を両 dialect に付与。

Revision ID: e7a3c9d2b5f1
Revises: d5e2b8a1c7f3
Create Date: 2026-07-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'e7a3c9d2b5f1'
down_revision: Union[str, Sequence[str], None] = 'd5e2b8a1c7f3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.create_table(
        "template_version",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("template_key", sa.Text, nullable=False),
        sa.Column("version", sa.Text, nullable=False),
        sa.Column("artifact_type", sa.Text, nullable=False),
        sa.Column("unit_type", sa.Text, nullable=False),
        sa.Column("purpose", sa.Text, nullable=True),
        sa.Column("file_ref", sa.Text, nullable=False),
        sa.Column("content_hash", sa.Text, nullable=False),
        sa.Column("content_bytes_ref", sa.Text, nullable=False),
        sa.Column("placeholders", _JSON, nullable=False),
        sa.Column("mapping_version", sa.Text, nullable=False),
        sa.Column("clause_library_version", sa.Text, nullable=False),
        sa.Column("generator_version", sa.Text, nullable=False),   # fix1 M02（§5.2）
        sa.Column("created_by", sa.Text, nullable=False),
        sa.Column("approved_by", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('draft', 'active', 'retired')",
                           name="ck_template_version_status"),
        sa.UniqueConstraint("template_key", "version",
                            name="uq_template_version_key_version"),
    )
    # §5.3 単一 active（部分ユニークインデックス・両 dialect）
    op.create_index("uq_template_version_single_active", "template_version",
                    ["template_key"], unique=True,
                    sqlite_where=sa.text("status = 'active'"),
                    postgresql_where=sa.text("status = 'active'"))
    # 内容列 immutable trigger（モジュール定義と単一ソース共用）
    from hub.template_registry import template_trigger_ddl
    dialect = op.get_bind().dialect.name
    for stmt in template_trigger_ddl().get(dialect, []):
        op.execute(stmt)


def downgrade() -> None:
    dialect = op.get_bind().dialect.name
    if dialect == "sqlite":
        for trg in ("frozen", "no_delete", "draft_only", "status_flow", "approved_once"):
            op.execute(f"DROP TRIGGER IF EXISTS trg_template_version_{trg}")
    elif dialect == "postgresql":
        for trg in ("frozen", "no_delete", "draft_only"):
            op.execute(f"DROP TRIGGER IF EXISTS trg_template_version_{trg} "
                       "ON template_version")
        op.execute("DROP FUNCTION IF EXISTS template_version_frozen()")
        op.execute("DROP FUNCTION IF EXISTS template_version_no_delete()")
        op.execute("DROP FUNCTION IF EXISTS template_version_draft_only()")
    op.drop_index("uq_template_version_single_active", table_name="template_version")
    op.drop_table("template_version")
