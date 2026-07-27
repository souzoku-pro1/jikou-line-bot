"""derivation_models — App36 導出台帳（DerivationRun／HeirConfirmationDecision）

正本: `docs/design-drafts/DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md` §2（P3-001）。
- **NH01（純粋 immutable 分離）**: DerivationRun は機械の導出事実のみを持つ
  純粋 immutable レコード。**human_state／decided_by／decided_at は持たない**。
  人の確定は HeirConfirmationDecision（追記のみ）へ分離し、§9.21 の human_state 等は
  「run＋最新 decision の join projection」として読む（テーブルに冗長保持しない）。
- **【裁定済み・2026-07-12 司令塔】専用モジュールの別 metadata**
  （inbound_event.Base 相乗りせず・L03 準拠）。alembic target_metadata へ統合。
- **UPDATE/DELETE 拒否**は二重で強制する:
  (i) ORM 層: before_update/before_delete listener が ImmutableRecordError を送出
  (ii) DB 層: BEFORE UPDATE/DELETE trigger（metadata.create_all にも migration にも付与・
      Core 文/外部クライアントからの変更も拒否）
  訂正・再導出は新行＋supersedes_*_id の連鎖のみ（連鎖の一意性は UNIQUE で担保）。
"""

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


class ImmutableRecordError(RuntimeError):
    """immutable 台帳（derivation_run／heir_confirmation_decision）への UPDATE/DELETE。"""


class DerivationBase(DeclarativeBase):
    """app-state DB 専用 metadata（L03: inbound_event.Base と相乗りしない）。"""


class DerivationRun(DerivationBase):
    """§2.1: 機械の導出事実のみ・純粋 immutable。"""

    __tablename__ = "derivation_run"
    __table_args__ = (
        sa.CheckConstraint("status IN ('derived', 'held', 'error')",
                           name="ck_derivation_run_status"),
        # 再導出連鎖の一意性: 1 つの旧 run を置き換えられる新 run は 1 つだけ
        sa.UniqueConstraint("supersedes_run_id", name="uq_derivation_run_supersedes"),
    )

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    case_app_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    case_record_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    decedent_person_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    at_date: Mapped[str] = mapped_column(sa.Text, nullable=False)   # 相続開始日（確定西暦）
    frozen_case_version: Mapped[str] = mapped_column(sa.Text, nullable=False)
    input_person_revisions: Mapped[dict] = mapped_column(_JSON, nullable=False)
    input_person_ids: Mapped[list] = mapped_column(_JSON, nullable=False)
    input_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False)    # derived/held/error
    rank: Mapped[int] = mapped_column(sa.Integer, nullable=False)   # 1/2/3/0
    result_payload: Mapped[dict] = mapped_column(_JSON, nullable=False)  # person_id のみ（§4）
    result_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    lawyer_flags: Mapped[dict | None] = mapped_column(_JSON)
    provisional: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    supersedes_run_id: Mapped[int | None] = mapped_column(
        sa.BigInteger, sa.ForeignKey("derivation_run.id"))
    engine_version: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


class HeirConfirmationDecision(DerivationBase):
    """§2.2: 人の確定・追記のみ（訂正は新 decision＋supersedes_decision_id）。"""

    __tablename__ = "heir_confirmation_decision"
    __table_args__ = (
        sa.CheckConstraint("decision IN ('confirmed', 'held', 'rejected')",
                           name="ck_heir_decision_decision"),
        sa.UniqueConstraint("supersedes_decision_id", name="uq_heir_decision_supersedes"),
    )

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    derivation_run_id: Mapped[int] = mapped_column(
        sa.BigInteger, sa.ForeignKey("derivation_run.id"), nullable=False)
    decision: Mapped[str] = mapped_column(sa.Text, nullable=False)
    decided_by: Mapped[str] = mapped_column(sa.Text, nullable=False)
    decided_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False)
    amendments: Mapped[dict | None] = mapped_column(_JSON)   # 正本 §2.2「修正内容」（監査用）
    supersedes_decision_id: Mapped[int | None] = mapped_column(
        sa.BigInteger, sa.ForeignKey("heir_confirmation_decision.id"))


# ── (i) ORM 層の immutable 強制 ──────────────────────────────────────────────

def _reject_mutation(mapper, connection, target):  # noqa: ARG001
    raise ImmutableRecordError(
        f"{target.__tablename__} is append-only (UPDATE/DELETE 禁止・"
        "訂正は新行＋supersedes 連鎖で行う)")


for _cls in (DerivationRun, HeirConfirmationDecision):
    sa.event.listen(_cls, "before_update", _reject_mutation)
    sa.event.listen(_cls, "before_delete", _reject_mutation)


# ── (ii) DB 層の immutable 強制（trigger・create_all/migration 双方に付与） ──

def immutable_trigger_ddl(table: str) -> dict[str, list[str]]:
    """dialect 別の BEFORE UPDATE/DELETE 拒否 trigger DDL（migration からも共用）。"""
    return {
        "sqlite": [
            f"CREATE TRIGGER trg_{table}_no_update BEFORE UPDATE ON {table} "
            f"BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END",
            f"CREATE TRIGGER trg_{table}_no_delete BEFORE DELETE ON {table} "
            f"BEGIN SELECT RAISE(ABORT, '{table} is immutable'); END",
        ],
        "postgresql": [
            f"CREATE OR REPLACE FUNCTION {table}_immutable() RETURNS trigger AS $$ "
            f"BEGIN RAISE EXCEPTION '{table} is immutable'; END; $$ LANGUAGE plpgsql",
            f"CREATE TRIGGER trg_{table}_no_mutation BEFORE UPDATE OR DELETE ON {table} "
            f"FOR EACH ROW EXECUTE FUNCTION {table}_immutable()",
        ],
    }


for _table in ("derivation_run", "heir_confirmation_decision"):
    _ddl = immutable_trigger_ddl(_table)
    _tbl = DerivationBase.metadata.tables[_table]
    for _stmt in _ddl["sqlite"]:
        sa.event.listen(_tbl, "after_create", sa.DDL(_stmt).execute_if(dialect="sqlite"))
    for _stmt in _ddl["postgresql"]:
        sa.event.listen(_tbl, "after_create",
                        sa.DDL(_stmt).execute_if(dialect="postgresql"))
