"""JIKOU-CERT-1: 時効援用完了報告書の保管 jikou_cert_doc（DDL のみ）

QUEUE-2026-10-20-HI I-1。表定義は hub/jikou_cert.py の metadata と同一。アプリ起動時には走らせない（alembic CLI のみ）。
down_revision は本 lineage（feature/jikou-progress-1）の head a9b1c3d5e7f0（JIKOU-FOLLOWUP-1）。

downgrade: 表を落とす。**データがあれば downgrade 禁止**（1 行でもあれば例外で拒否＝RV-08・生成済み報告書は消さない）。

Revision ID: b3c5d7e9f1a2
Revises: a9b1c3d5e7f0
Create Date: 2026-10-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'b3c5d7e9f1a2'
down_revision: Union[str, Sequence[str], None] = 'a9b1c3d5e7f0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "jikou_cert_doc"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("doc_id", sa.Text, primary_key=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("case_record_id", sa.Text, nullable=False),
        sa.Column("slot", sa.Integer, nullable=False),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("creditor_name", sa.Text, nullable=True),
        sa.Column("docx", sa.LargeBinary, nullable=False),
        sa.Column("pdf", sa.LargeBinary, nullable=False),
        sa.Column("sha256_docx", sa.Text, nullable=False),
        sa.Column("sha256_pdf", sa.Text, nullable=False),
        sa.Column("texts_version", sa.Text, nullable=False),
        sa.Column("template_sha256", sa.Text, nullable=False),
        sa.Column("created_by", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("slot IN (1, 2, 3)", name="ck_jikou_cert_doc_slot"),
        sa.CheckConstraint("version >= 1", name="ck_jikou_cert_doc_version"),
        sa.UniqueConstraint("business", "case_record_id", "slot", "version", name="uq_jikou_cert_doc_version"),
    )
    op.create_index("ix_jikou_cert_doc_case", TABLE, ["business", "case_record_id"])


def _refuse_if_data_present() -> None:
    bind = op.get_bind()
    n = bind.execute(sa.text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("jikou_cert_doc_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_jikou_cert_doc_case", table_name=TABLE)
    op.drop_table(TABLE)
