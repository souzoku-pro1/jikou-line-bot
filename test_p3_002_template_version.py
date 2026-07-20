"""P3-002: TemplateVersion registry のテスト。

正本 DRAFT_APP36 §5 の契約を pin する:
- immutable 版管理: 内容列は登録後変更不可（ORM guard＋DB trigger）・DELETE 全面拒否・
  ライフサイクル列（status/activated_at/approved_*/retired_at）のみ可変
- 単一 active（§5.3）: 部分ユニーク＋activate() の条件付き遷移（同一 transaction）
- bytes 再現 contract の DB 担保分: hash/bytes_ref/生成 rule 版の NOT NULL
- (template_key, version) UNIQUE・status 語彙 CHECK
- migration up-down 構造・別 metadata 非干渉
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError, OperationalError

import hub.db as db
from hub.derivation_models import DerivationBase, ImmutableRecordError
from hub.template_registry import (TemplateVersion, activate,
                                   create_template_version, get_active)


def _run(coro):
    return asyncio.run(coro)


def _fields(**over):
    f = dict(template_key="zaisan_mokuroku", version="1.0.0",
             artifact_type="財産目録", unit_type="相続一般",
             file_ref="templates/zaisan.docx", content_hash="ch" * 8,
             content_bytes_ref="drive:bytes-1", placeholders=["氏名", "作成日"],
             mapping_version="map-1", clause_library_version="clause-1",
             generator_version="gen-1", created_by="pc-a")
    f.update(over)
    return f


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3002_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/a.db"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(DerivationBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _create(self, **over) -> int:
        pk = _run(create_template_version(**_fields(**over)))
        db.reset_for_tests()
        return pk

    def _activate(self, pk, approved_by="attorney-1"):
        _run(activate(pk, approved_by))
        db.reset_for_tests()

    def _get_active(self, key="zaisan_mokuroku"):
        r = _run(get_active(key))
        db.reset_for_tests()
        return r


class TestCrudAndSingleActive(_DbMixin):
    def test_create_draft_then_activate(self):
        pk = self._create()
        self.assertIsNone(self._get_active())          # draft のうちは active なし
        self._activate(pk)
        row = self._get_active()
        self.assertIsNotNone(row)
        self.assertEqual(row.version, "1.0.0")
        self.assertEqual(row.approved_by, "attorney-1")
        self.assertIsNotNone(row.activated_at)

    def test_activate_supersedes_old_active(self):
        pk1 = self._create(version="1.0.0")
        pk2 = self._create(version="1.1.0")
        self._activate(pk1)
        self._activate(pk2)                            # 旧 active は retired へ
        row = self._get_active()
        self.assertEqual(row.version, "1.1.0")

        async def _old():
            async with db.session_scope() as s:
                return (await s.execute(
                    sa.select(TemplateVersion.__table__.c.status,
                              TemplateVersion.__table__.c.retired_at)
                    .where(TemplateVersion.__table__.c.id == pk1))).one()
        old = _run(_old())
        db.reset_for_tests()
        self.assertEqual(old.status, "retired")
        self.assertIsNotNone(old.retired_at)

    def test_partial_unique_blocks_second_active_direct_sql(self):
        pk1 = self._create(version="1.0.0")
        self._create(version="1.1.0")
        self._activate(pk1)

        async def _force_second_active():
            async with db.session_scope() as s:
                await s.execute(sa.update(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.version == "1.1.0")
                                .values(status="active"))
        with self.assertRaises(IntegrityError):        # 部分ユニークが DB レベルで遮断
            _run(_force_second_active())
        db.reset_for_tests()

    def test_activate_requires_draft(self):
        pk = self._create()
        self._activate(pk)
        with self.assertRaises(ValueError):            # 再 activate 不可
            _run(activate(pk, "attorney-2"))
        db.reset_for_tests()


class TestImmutability(_DbMixin):
    def test_content_column_update_rejected_core(self):
        pk = self._create()

        async def _upd():
            async with db.session_scope() as s:
                await s.execute(sa.update(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.id == pk)
                                .values(content_hash="tampered"))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_upd())
        db.reset_for_tests()

    def test_content_column_update_rejected_orm(self):
        pk = self._create()

        async def _upd():
            async with db.session_scope() as s:
                obj = (await s.execute(sa.select(TemplateVersion)
                                       .where(TemplateVersion.id == pk))).scalar_one()
                obj.mapping_version = "map-2"
        with self.assertRaises(ImmutableRecordError):
            _run(_upd())
        db.reset_for_tests()

    def test_delete_rejected(self):
        pk = self._create()

        async def _del():
            async with db.session_scope() as s:
                await s.execute(sa.delete(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.id == pk))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_del())
        db.reset_for_tests()

    def test_lifecycle_columns_mutable(self):
        # activate が status/activated_at/approved_* を更新できている＝可変列の証明は
        # TestCrudAndSingleActive で担保。ここでは retired_at 単独更新も通ることを確認。
        pk = self._create()
        self._activate(pk)

        async def _retire():
            async with db.session_scope() as s:
                await s.execute(sa.update(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.id == pk)
                                .values(status="retired", retired_at=sa.func.now()))
        _run(_retire())
        db.reset_for_tests()
        self.assertIsNone(self._get_active())


class TestConstraints(_DbMixin):
    def test_key_version_unique(self):
        self._create(version="1.0.0")
        with self.assertRaises(IntegrityError):
            _run(create_template_version(**_fields(version="1.0.0")))
        db.reset_for_tests()

    def test_status_vocabulary_check(self):
        # fix1 H01 で create_template_version は status を受けないため、
        # CHECK 制約自体は直接 SQL（Core insert）で検査する
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(TemplateVersion.__table__)
                                .values(**_fields(status="published")))
        with self.assertRaises(IntegrityError):
            _run(_ins())
        db.reset_for_tests()

    def test_bytes_contract_columns_not_null(self):
        for col in ("content_hash", "content_bytes_ref", "mapping_version",
                    "clause_library_version"):
            with self.subTest(col=col):
                with self.assertRaises(IntegrityError):
                    _run(create_template_version(**_fields(**{col: None})))
                db.reset_for_tests()


class TestStructure(unittest.TestCase):
    def test_metadata_and_noninterference(self):
        from hub.inbound_event import Base as InboundBase
        self.assertIn("template_version", DerivationBase.metadata.tables)
        self.assertNotIn("template_version", InboundBase.metadata.tables)

    def test_migration_chain_and_updown(self):
        mig = Path("alembic/versions/20260721_e7a3c9d2b5f1_template_version.py") \
            .read_text(encoding="utf-8")
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'd5e2b8a1c7f3'",
                      mig)
        self.assertEqual(mig.count("op.create_table"), 1)
        self.assertEqual(mig.count("op.drop_table"), 1)
        self.assertIn("uq_template_version_single_active", mig)


# ── fix1(P3002-H01/H02/M01/M02) 追加テスト ───────────────────────────────────
class TestDraftOnlyCreation(_DbMixin):
    """H01: 作成は常に draft・active 直接作成は repo/DB の両層で拒否。"""

    def test_repo_rejects_status_kwarg(self):
        with self.assertRaises(ValueError):
            _run(create_template_version(**_fields(status="active")))
        db.reset_for_tests()

    def test_direct_sql_active_insert_rejected_by_trigger(self):
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(TemplateVersion.__table__)
                                .values(**_fields(status="active")))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_ins())
        db.reset_for_tests()

    def test_status_flow_trigger_blocks_retired_to_active(self):
        pk = self._create()
        self._activate(pk)

        async def _retire():
            async with db.session_scope() as s:
                await s.execute(sa.update(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.id == pk)
                                .values(status="retired", retired_at=sa.func.now()))
        _run(_retire())
        db.reset_for_tests()

        async def _revive():
            async with db.session_scope() as s:
                await s.execute(sa.update(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.id == pk)
                                .values(status="active"))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_revive())
        db.reset_for_tests()


class TestActivateConflictSafety(_DbMixin):
    """H02: 並行 activate でも常に active ≤1 件・rowcount=0 は tx 全体 rollback。"""

    def _count_active(self):
        async def _q():
            async with db.session_scope() as s:
                return (await s.execute(
                    sa.select(sa.func.count()).select_from(TemplateVersion.__table__)
                    .where(TemplateVersion.__table__.c.status == "active"))).scalar_one()
        n = _run(_q())
        db.reset_for_tests()
        return n

    def test_sequential_activates_keep_single_active(self):
        pks = [self._create(version=f"1.{i}.0") for i in range(3)]
        for pk in pks:
            self._activate(pk)
            self.assertEqual(self._count_active(), 1)   # 常に 1 件

    def test_double_activate_same_id_rejected(self):
        pk = self._create()
        self._activate(pk)
        with self.assertRaises(ValueError):
            _run(activate(pk, "attorney-2"))
        db.reset_for_tests()
        self.assertEqual(self._count_active(), 1)

    def test_rowcount_zero_rolls_back_whole_tx(self):
        # TOCTOU 再現: 事前参照 seam を差し替え「実際は active な行」を draft と誤認させる
        import hub.template_registry as tr
        pk1 = self._create(version="1.0.0")
        self._activate(pk1)                       # 現 active = pk1

        real = tr._get_version_row

        async def _lying(s, version_id):
            row = await real(s, version_id)
            fake = dict(row._mapping)
            fake["status"] = "draft"              # 誤認: 実際は active
            import types
            return types.SimpleNamespace(**fake)

        with patch.object(tr, "_get_version_row", _lying):
            with self.assertRaises(tr.ActivationConflictError):
                _run(activate(pk1, "attorney-2"))
        db.reset_for_tests()
        # rollback により旧 active の retire が巻き戻り「active 0 件」にならない
        self.assertEqual(self._count_active(), 1)
        self.assertEqual(self._get_active().id, pk1)


class TestApprovedWriteOnce(_DbMixin):
    """M01: approved_by/approved_at は draft→active 遷移時に一度だけ設定可。"""

    def test_core_update_of_approved_rejected(self):
        pk = self._create()
        self._activate(pk)

        async def _upd():
            async with db.session_scope() as s:
                await s.execute(sa.update(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.id == pk)
                                .values(approved_by="someone-else"))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_upd())
        db.reset_for_tests()

    def test_orm_update_of_approved_rejected(self):
        from hub.derivation_models import ImmutableRecordError
        pk = self._create()
        self._activate(pk)

        async def _upd():
            async with db.session_scope() as s:
                obj = (await s.execute(sa.select(TemplateVersion)
                                       .where(TemplateVersion.id == pk))).scalar_one()
                obj.approved_by = "someone-else"
        with self.assertRaises(ImmutableRecordError):
            _run(_upd())
        db.reset_for_tests()


class TestGeneratorVersion(_DbMixin):
    """M02: generator_version は NOT NULL・frozen（bytes 再現要素・§5.2）。"""

    def test_not_null(self):
        with self.assertRaises(IntegrityError):
            _run(create_template_version(**_fields(generator_version=None)))
        db.reset_for_tests()

    def test_frozen(self):
        pk = self._create()

        async def _upd():
            async with db.session_scope() as s:
                await s.execute(sa.update(TemplateVersion.__table__)
                                .where(TemplateVersion.__table__.c.id == pk)
                                .values(generator_version="gen-2"))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_upd())
        db.reset_for_tests()


if __name__ == "__main__":
    unittest.main()
