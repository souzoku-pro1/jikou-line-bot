"""LABEL-PRINT-1-fix1: ラベル印字のジョブ状態機械（Codex LP-01〜06）

- label_print_job を event_id 一意・status（reserved/attached/printed/undone/cancelled）・
  宛先キー（app/record/role）を持つ形に作り直す（面の予約＝ジョブ行。旧列 idempotency_key は廃止）。
  旧テーブルは本リビジョンの直前（a1b2c3d4e5f6）で作られ、運用データは無い前提で drop/create。
- label_sheet_state.pending_faces を廃止（占有中の面はジョブから導く）。
- label_sheet_op（印刷済／戻す／新しいシートの event_id 冪等記録）を追加。

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-09-10
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_Json = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


def upgrade() -> None:
    op.drop_column("label_sheet_state", "pending_faces")
    op.drop_table("label_print_job")
    op.create_table(
        "label_print_job",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("event_id", sa.Text, nullable=False, unique=True),
        sa.Column("app", sa.Text, nullable=False),
        sa.Column("record", sa.Text, nullable=False),
        sa.Column("role", sa.Text, nullable=False),
        sa.Column("sheet_id", sa.Text, nullable=False),
        sa.Column("face", sa.Integer, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("file_key", sa.Text, nullable=False, server_default=""),
        sa.Column("filename", sa.Text, nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("created_by", sa.Text, nullable=False, server_default=""),
        sa.CheckConstraint(
            "status IN ('reserved', 'attached', 'printed', 'undone', 'cancelled')",
            name="ck_label_print_job_status"),
    )
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


def downgrade() -> None:
    op.drop_table("label_sheet_op")
    op.drop_table("label_print_job")
    op.create_table(
        "label_print_job",
        sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
        sa.Column("idempotency_key", sa.Text, nullable=False, unique=True),
        sa.Column("sheet_id", sa.Text, nullable=False),
        sa.Column("face", sa.Integer, nullable=False),
        sa.Column("filename", sa.Text, nullable=False),
        sa.Column("file_key", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("created_by", sa.Text, nullable=False, server_default=""),
    )
    op.add_column("label_sheet_state",
                  sa.Column("pending_faces", _Json, nullable=False, server_default="[]"))
