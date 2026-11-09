"""K-1（docs/k_design.md §1-2・§6）: K 後追い下書きの記録表 reply_k_draft（新表のみ・既存表の列変更なし）

- App 29 No.（record_id）につき 1 行（UNIQUE）。下書き ID・元 pending ID・モデル名・規則版（v1.5）と sha・状態・固定理由・
  本文（generated のときだけ）・札・手本参照の ID（K-2 まで空）・入出力トークン数・概算費用（円）・所要 ms・作成／更新時刻。
- 本文は大野が返信画面で手直しして送る前提の下書き。LINE userId・例外本文は置かない。相談者の本人名は設計 §1-5 により本文に含み得る（認証内の返信画面でのみ表示）。
- e3f5a7b9c1d3（UX-PENDING-1-fix4 の merge revision＝本番適用済み）の次。アプリ起動時には走らせない（D2: alembic CLI のみ）。

downgrade: 表を落とす。**1 行でもあれば downgrade 禁止**（RV-08・値は出さない）。

Revision ID: f6a8c0e2d4b6
Revises: e3f5a7b9c1d3
Create Date: 2026-11-09
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'f6a8c0e2d4b6'
down_revision: Union[str, Sequence[str], None] = 'e3f5a7b9c1d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLE = "reply_k_draft"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
        sa.Column("record_id", sa.Text, nullable=False),
        sa.Column("pending_id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=True),
        sa.Column("business", sa.Text, nullable=False),
        sa.Column("category", sa.Text, nullable=True),
        sa.Column("model", sa.Text, nullable=False),
        sa.Column("rules_version", sa.Text, nullable=False),
        sa.Column("rules_sha", sa.Text, nullable=False),
        sa.Column("state", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=True),
        sa.Column("text", sa.Text, nullable=True),
        sa.Column("text_sha", sa.Text, nullable=True),
        sa.Column("flags", sa.Text, nullable=True),
        sa.Column("exemplar_ids", sa.Text, nullable=True),
        sa.Column("input_tokens", sa.Integer, nullable=True),
        sa.Column("output_tokens", sa.Integer, nullable=True),
        sa.Column("cost_yen", sa.Float, nullable=True),
        sa.Column("elapsed_ms", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("record_id", name="uq_reply_k_draft_record"),
    )
    op.create_index("ix_reply_k_draft_state", TABLE, ["business", "state"])
    op.create_index("ix_reply_k_draft_created", TABLE, ["business", "created_at"])


def _refuse_if_data_present() -> None:
    n = op.get_bind().execute(sa.text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
    if int(n or 0) > 0:
        raise RuntimeError("reply_k_draft_downgrade_refused_data_present")


def downgrade() -> None:
    _refuse_if_data_present()
    op.drop_index("ix_reply_k_draft_created", table_name=TABLE)
    op.drop_index("ix_reply_k_draft_state", table_name=TABLE)
    op.drop_table(TABLE)
