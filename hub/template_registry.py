"""template_registry — TemplateVersion registry（App37/成果物テンプレの版管理・P3-002）

正本: `DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md` §5（§9.23 全 field・
bytes 再現・単一 active）＋ P3 inventory P3-002 票案。

- **metadata**: 裁定 2026-07-12「DerivationRun/HCD/TemplateVersion は専用モジュールの
  別 metadata」に従い、app-state 群の `DerivationBase.metadata` に相乗りする
  （inbound_event.Base とは分離のまま・alembic 統合済み）。
- **immutable 版管理**: 版の内容列（template_key/version/実体参照/hash/生成 rule 版/
  placeholders/created_by）は**登録後変更不可**（DB trigger が内容列の UPDATE を拒否・
  DELETE は全面拒否）。可変なのは**ライフサイクル列のみ**
  （status／activated_at／approved_by／approved_at／retired_at）。訂正は新版の追加で行う。
- **単一 active（§5.3）**: `UNIQUE(template_key) WHERE status='active'` の
  **部分ユニークインデックス**で DB レベル強制（sqlite/postgresql 両対応）＋
  `activate()` は「旧 active を retired にする条件付き遷移＋同一トランザクション」。
- **bytes 再現 contract（§5.2）の DB 担保分**: content_hash・content_bytes_ref・
  mapping_version・clause_library_version を NOT NULL で強制（golden bytes 再現テスト
  自体は Phase 5 生成器側・本票対象外）。
- kintone App37 実結線は含まない（P3-002 スコープ）。
"""

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, mapped_column

from hub.db import session_scope
from hub.derivation_models import DerivationBase, ImmutableRecordError

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

# 登録後変更不可の内容列（trigger と ORM guard の単一ソース）
_FROZEN_COLUMNS = (
    "template_key", "version", "artifact_type", "unit_type", "purpose",
    "file_ref", "content_hash", "content_bytes_ref", "placeholders",
    "mapping_version", "clause_library_version", "created_by",
)


class TemplateVersion(DerivationBase):
    """§5.1 テーブル案の全 field。"""

    __tablename__ = "template_version"
    __table_args__ = (
        sa.CheckConstraint("status IN ('draft', 'active', 'retired')",
                           name="ck_template_version_status"),
        sa.UniqueConstraint("template_key", "version",
                            name="uq_template_version_key_version"),
        # §5.3 単一 active: 部分ユニークインデックス（両 dialect 対応）
        sa.Index("uq_template_version_single_active", "template_key",
                 unique=True,
                 sqlite_where=sa.text("status = 'active'"),
                 postgresql_where=sa.text("status = 'active'")),
    )

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    template_key: Mapped[str] = mapped_column(sa.Text, nullable=False)
    version: Mapped[str] = mapped_column(sa.Text, nullable=False)
    artifact_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    unit_type: Mapped[str] = mapped_column(sa.Text, nullable=False)   # §8.15 非混在強制
    purpose: Mapped[str | None] = mapped_column(sa.Text)
    file_ref: Mapped[str] = mapped_column(sa.Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(sa.Text, nullable=False)
    content_bytes_ref: Mapped[str] = mapped_column(sa.Text, nullable=False)
    placeholders: Mapped[list] = mapped_column(_JSON, nullable=False)
    mapping_version: Mapped[str] = mapped_column(sa.Text, nullable=False)
    clause_library_version: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_by: Mapped[str] = mapped_column(sa.Text, nullable=False)
    approved_by: Mapped[str | None] = mapped_column(sa.Text)
    status: Mapped[str] = mapped_column(sa.Text, nullable=False, default="draft")
    activated_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    approved_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(sa.DateTime(timezone=True))


# ── ORM 層 guard: 内容列の変更・delete を拒否（ライフサイクル列のみ可変） ──────

def _reject_frozen_update(mapper, connection, target):  # noqa: ARG001
    insp = sa.inspect(target)
    for col in _FROZEN_COLUMNS:
        hist = insp.attrs[col].history
        if hist.has_changes():
            raise ImmutableRecordError(
                f"template_version.{col} は登録後変更不可（訂正は新版の追加で行う）")


def _reject_delete(mapper, connection, target):  # noqa: ARG001
    raise ImmutableRecordError("template_version は削除不可（retired へ遷移させる）")


sa.event.listen(TemplateVersion, "before_update", _reject_frozen_update)
sa.event.listen(TemplateVersion, "before_delete", _reject_delete)


# ── DB 層 trigger（create_all／migration 共用の単一ソース） ──────────────────

def template_trigger_ddl() -> dict[str, list[str]]:
    """内容列 UPDATE 拒否＋DELETE 全面拒否の dialect 別 DDL。"""
    sqlite_when = " OR ".join(
        f"IFNULL(OLD.{c}, '') IS NOT IFNULL(NEW.{c}, '')" for c in _FROZEN_COLUMNS)
    pg_when = " OR ".join(
        f"OLD.{c} IS DISTINCT FROM NEW.{c}" for c in _FROZEN_COLUMNS)
    return {
        "sqlite": [
            "CREATE TRIGGER trg_template_version_frozen BEFORE UPDATE ON template_version "
            f"FOR EACH ROW WHEN {sqlite_when} "
            "BEGIN SELECT RAISE(ABORT, 'template_version content is immutable'); END",
            "CREATE TRIGGER trg_template_version_no_delete BEFORE DELETE ON template_version "
            "BEGIN SELECT RAISE(ABORT, 'template_version is append-only'); END",
        ],
        "postgresql": [
            "CREATE OR REPLACE FUNCTION template_version_frozen() RETURNS trigger AS $$ "
            f"BEGIN IF {pg_when} THEN "
            "RAISE EXCEPTION 'template_version content is immutable'; END IF; "
            "RETURN NEW; END; $$ LANGUAGE plpgsql",
            "CREATE TRIGGER trg_template_version_frozen BEFORE UPDATE ON template_version "
            "FOR EACH ROW EXECUTE FUNCTION template_version_frozen()",
            "CREATE OR REPLACE FUNCTION template_version_no_delete() RETURNS trigger AS $$ "
            "BEGIN RAISE EXCEPTION 'template_version is append-only'; END; "
            "$$ LANGUAGE plpgsql",
            "CREATE TRIGGER trg_template_version_no_delete BEFORE DELETE ON template_version "
            "FOR EACH ROW EXECUTE FUNCTION template_version_no_delete()",
        ],
    }


_tv_table = DerivationBase.metadata.tables["template_version"]
for _dialect, _stmts in template_trigger_ddl().items():
    for _stmt in _stmts:
        sa.event.listen(_tv_table, "after_create",
                        sa.DDL(_stmt).execute_if(dialect=_dialect))


# ── CRUD 最小（作成・参照・版固定＝activate） ────────────────────────────────

async def create_template_version(**fields) -> int:
    """新版を draft で登録（内容列は登録時のみ与えられる）。戻り値=id。"""
    fields.setdefault("status", "draft")
    async with session_scope() as s:
        r = await s.execute(sa.insert(TemplateVersion.__table__).values(**fields))
        return r.inserted_primary_key[0]


async def get_active(template_key: str):
    """template_key の現 active 版（Row）を返す。無ければ None。"""
    async with session_scope() as s:
        return (await s.execute(
            sa.select(TemplateVersion.__table__)
            .where(TemplateVersion.__table__.c.template_key == template_key,
                   TemplateVersion.__table__.c.status == "active"))).one_or_none()


async def activate(version_id: int, approved_by: str) -> None:
    """版固定: 同一 transaction で旧 active→retired ＋ 対象 draft→active（§5.3）。
    対象が draft でない場合は ValueError（再 activate・retired 復活は不可）。"""
    async with session_scope() as s:
        target = (await s.execute(
            sa.select(TemplateVersion.__table__)
            .where(TemplateVersion.__table__.c.id == version_id))).one_or_none()
        if target is None or target.status != "draft":
            raise ValueError("activate 対象は draft の版のみ")
        await s.execute(
            sa.update(TemplateVersion.__table__)
            .where(TemplateVersion.__table__.c.template_key == target.template_key,
                   TemplateVersion.__table__.c.status == "active")
            .values(status="retired", retired_at=sa.func.now()))
        await s.execute(
            sa.update(TemplateVersion.__table__)
            .where(TemplateVersion.__table__.c.id == version_id,
                   TemplateVersion.__table__.c.status == "draft")
            .values(status="active", activated_at=sa.func.now(),
                    approved_by=approved_by, approved_at=sa.func.now()))
