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

import re
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")


class ImmutableRecordError(RuntimeError):
    """immutable 台帳（derivation_run／heir_confirmation_decision）への UPDATE/DELETE。"""


class PayloadPolicyError(ValueError):
    """result_payload/lawyer_flags の schema allowlist・PII 防御違反（§3.5・fix1 H02）。
    immutable 台帳への誤保存は訂正不能のため、保存**前**の入口で拒否する。"""


class ChainIntegrityError(ValueError):
    """supersedes 連鎖の健全性違反（自己参照・cross-case・多重 head 等・fix1 H01）。"""


class DerivationBase(DeclarativeBase):
    """app-state DB 専用 metadata（L03: inbound_event.Base と相乗りしない）。"""


class DerivationRun(DerivationBase):
    """§2.1: 機械の導出事実のみ・純粋 immutable。"""

    __tablename__ = "derivation_run"
    __table_args__ = (
        sa.CheckConstraint("status IN ('derived', 'held', 'error')",
                           name="ck_derivation_run_status"),
        # fix1 M01: rank の語彙固定（1/2/3 順位・0=該当なし）
        sa.CheckConstraint("rank IN (0, 1, 2, 3)", name="ck_derivation_run_rank"),
        # fix1 H01: 自己参照の DB レベル拒否
        sa.CheckConstraint("supersedes_run_id IS NULL OR supersedes_run_id != id",
                           name="ck_derivation_run_no_self_supersede"),
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


# ══════════════════════════════════════════════════════════════
# fix1 H02: result_payload / lawyer_flags の schema allowlist（§3.5・氏名非保持）
# ══════════════════════════════════════════════════════════════
# 正本 §3.5: result_payload は person_id のみ（氏名・住所・生年月日を保持しない）。
# facts は導出根拠の最小限（条文キー等）。許可キー以外は保存拒否（PII 混入を構造で防ぐ）。
# immutable 台帳のため誤保存は訂正不能 → ORM 層＋リポジトリ関数の**入口**で拒否する。
# 許可集合の拡張は正本 §3.5 の改定と同時にのみ行う。

_RESULT_TOP_KEYS = frozenset({"heirs", "facts"})
_RESULT_HEIR_KEYS = frozenset({"person_id", "share", "relation_key"})
_LAWYER_FLAGS_KEYS = frozenset({"flags"})
# PII 様検出（D17 流儀）: 値は person_id／share（分数）／条文キー等の ASCII コード体系のみを
# 許容し、CJK（氏名・住所が必ず含む）や改行を含む文字列は PII 様として拒否する。
_VALUE_RE = re.compile(r"^[\x20-\x7e]{1,200}$")


def _check_str(value, where: str) -> None:
    if not isinstance(value, str) or not _VALUE_RE.fullmatch(value):
        raise PayloadPolicyError(
            f"{where}: 非 ASCII/長大/改行含み文字列は保存不可（PII 様・§3.5 氏名非保持）")


def validate_result_payload(payload) -> None:
    """§3.5 allowlist。違反は PayloadPolicyError（保存前・入口ガード）。"""
    if not isinstance(payload, dict):
        raise PayloadPolicyError("result_payload は dict であること")
    extra = set(payload) - _RESULT_TOP_KEYS
    if extra:
        raise PayloadPolicyError(f"result_payload: 許可キー外 {sorted(extra)}（§3.5）")
    heirs = payload.get("heirs", [])
    if not isinstance(heirs, list):
        raise PayloadPolicyError("result_payload.heirs は list であること")
    for h in heirs:
        if not isinstance(h, dict):
            raise PayloadPolicyError("result_payload.heirs[*] は dict であること")
        extra = set(h) - _RESULT_HEIR_KEYS
        if extra:
            raise PayloadPolicyError(
                f"result_payload.heirs[*]: 許可キー外 {sorted(extra)}（person_id 系のみ）")
        for k, v in h.items():
            _check_str(v, f"result_payload.heirs[*].{k}")
    for f in payload.get("facts", []):
        _check_str(f, "result_payload.facts[*]")


def validate_lawyer_flags(flags) -> None:
    if flags is None:
        return
    if not isinstance(flags, dict):
        raise PayloadPolicyError("lawyer_flags は dict/None であること")
    extra = set(flags) - _LAWYER_FLAGS_KEYS
    if extra:
        raise PayloadPolicyError(f"lawyer_flags: 許可キー外 {sorted(extra)}")
    values = flags.get("flags", [])
    if not isinstance(values, list):
        raise PayloadPolicyError("lawyer_flags.flags は list であること")
    for v in values:
        _check_str(v, "lawyer_flags.flags[*]")


def _validate_run_payloads_orm(mapper, connection, target):  # noqa: ARG001
    validate_result_payload(target.result_payload)
    validate_lawyer_flags(target.lawyer_flags)


sa.event.listen(DerivationRun, "before_insert", _validate_run_payloads_orm)


# ══════════════════════════════════════════════════════════════
# fix1 H01: リポジトリ関数（supersedes 連鎖の健全性＋入口ガードの正規経路）
# ══════════════════════════════════════════════════════════════

async def create_derivation_run(**fields) -> int:
    """DerivationRun 作成の正規経路（アプリ層 guard・fix1）。

    強制する健全性（DB 制約と重畳）:
    - schema allowlist／PII 防御（validate_result_payload／validate_lawyer_flags）
    - supersedes_run_id: 実在・**同一 case**（cross-case 参照拒否）・
      **未 supersede の head のみ**（既に置換済みの run を再置換＝分岐/循環の芽を拒否。
      自己参照は insert 前に id 未確定のため構造的に不可＋DB CHECK でも拒否）
    - **同一 case の head 一意性**: supersedes 無しの新規 run は、その case に
      run が 1 件も無い場合のみ許可（2 本目以降は必ず現 head を supersede する）
    """
    from hub.db import session_scope

    validate_result_payload(fields.get("result_payload"))
    validate_lawyer_flags(fields.get("lawyer_flags"))
    case_record_id = fields.get("case_record_id")
    sup = fields.get("supersedes_run_id")
    t = DerivationRun.__table__
    async with session_scope() as s:
        if sup is not None:
            old = (await s.execute(
                sa.select(t.c.id, t.c.case_record_id)
                .where(t.c.id == sup))).one_or_none()
            if old is None:
                raise ChainIntegrityError(f"supersedes_run_id={sup} は存在しない")
            if old.case_record_id != case_record_id:
                raise ChainIntegrityError(
                    "cross-case の supersede は禁止（同一 case の連鎖のみ）")
            already = (await s.execute(
                sa.select(t.c.id).where(t.c.supersedes_run_id == sup))).first()
            if already is not None:
                raise ChainIntegrityError(
                    f"run {sup} は既に supersede 済み（head のみ置換可・分岐/循環禁止）")
        else:
            exists = (await s.execute(
                sa.select(t.c.id)
                .where(t.c.case_record_id == case_record_id).limit(1))).first()
            if exists is not None:
                raise ChainIntegrityError(
                    "同一 case に run が既に存在（head 一意性・新 run は現 head を "
                    "supersedes_run_id で指すこと)")
        r = await s.execute(sa.insert(t).values(**fields))
        return r.inserted_primary_key[0]
