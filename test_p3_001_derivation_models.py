"""P3-001: DerivationRun／HeirConfirmationDecision（App36 導出台帳）のテスト。

正本 DRAFT_APP36 §2 の契約を pin する:
- NH01 分離契約: DerivationRun は人の確定列（human_state/decided_by/decided_at）を持たない
- immutable: UPDATE/DELETE は ORM 層（listener）と DB 層（trigger）の両方で拒否
- 追記のみの訂正連鎖: supersedes_*_id は UNIQUE（1 つの旧行を置き換えるのは 1 行だけ）
- 状態語彙: status ∈ {derived, held, error}／decision ∈ {confirmed, held, rejected}
- 別 metadata（L03）: inbound_event.Base と相乗りしない・alembic 統合済み
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError, OperationalError

import hub.db as db
from hub.derivation_models import (DerivationBase, DerivationRun,
                                   HeirConfirmationDecision,
                                   ImmutableRecordError)


def _run(coro):
    return asyncio.run(coro)


def _run_row(**over):
    row = dict(case_app_id="26", case_record_id="R-1", decedent_person_id="P-0",
               at_date="2026-01-01", frozen_case_version="v1",
               input_person_revisions={"P-1": 3}, input_person_ids=["P-0", "P-1"],
               input_hash="ih" * 8, status="derived", rank=1,
               result_payload={"heirs": [{"person_id": "P-1", "share": "1/1"}]},
               result_hash="rh" * 8, provisional=False, engine_version="hd-1")
    row.update(over)
    return row


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3001_")
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

    def _insert_run(self, **over) -> int:
        async def _ins():
            async with db.session_scope() as s:
                r = await s.execute(sa.insert(DerivationRun.__table__)
                                    .values(**_run_row(**over)))
                return r.inserted_primary_key[0]
        pk = _run(_ins())
        db.reset_for_tests()
        return pk


class TestHappyPath(_DbMixin):
    def test_run_and_decision_append(self):
        run_id = self._insert_run()

        async def _decide():
            async with db.session_scope() as s:
                await s.execute(sa.insert(HeirConfirmationDecision.__table__).values(
                    derivation_run_id=run_id, decision="confirmed",
                    decided_by="attorney-1",
                    decided_at=datetime.now(timezone.utc)))
                rows = (await s.execute(
                    sa.select(HeirConfirmationDecision.decision))).scalars().all()
                return rows
        rows = _run(_decide())
        db.reset_for_tests()
        self.assertEqual(rows, ["confirmed"])

    def test_supersedes_chain_unique(self):
        old = self._insert_run()
        self._insert_run(supersedes_run_id=old, input_hash="x" * 16)

        async def _dup():
            async with db.session_scope() as s:
                await s.execute(sa.insert(DerivationRun.__table__).values(
                    **_run_row(supersedes_run_id=old, input_hash="y" * 16)))
        with self.assertRaises(IntegrityError):
            _run(_dup())
        db.reset_for_tests()


class TestImmutableDbLayer(_DbMixin):
    """DB trigger: Core 文（ORM を介さない変更）も拒否されること。"""

    def test_core_update_rejected(self):
        pk = self._insert_run()

        async def _upd():
            async with db.session_scope() as s:
                await s.execute(sa.update(DerivationRun.__table__)
                                .where(DerivationRun.__table__.c.id == pk)
                                .values(status="held"))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_upd())
        db.reset_for_tests()

    def test_core_delete_rejected(self):
        pk = self._insert_run()

        async def _del():
            async with db.session_scope() as s:
                await s.execute(sa.delete(DerivationRun.__table__)
                                .where(DerivationRun.__table__.c.id == pk))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_del())
        db.reset_for_tests()

    def test_decision_update_delete_rejected(self):
        run_id = self._insert_run()

        async def _ins():
            async with db.session_scope() as s:
                r = await s.execute(sa.insert(HeirConfirmationDecision.__table__).values(
                    derivation_run_id=run_id, decision="held", decided_by="a",
                    decided_at=datetime.now(timezone.utc)))
                return r.inserted_primary_key[0]
        pk = _run(_ins())
        db.reset_for_tests()

        async def _upd():
            async with db.session_scope() as s:
                await s.execute(sa.update(HeirConfirmationDecision.__table__)
                                .where(HeirConfirmationDecision.__table__.c.id == pk)
                                .values(decision="confirmed"))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_upd())
        db.reset_for_tests()

        async def _del():
            async with db.session_scope() as s:
                await s.execute(sa.delete(HeirConfirmationDecision.__table__)
                                .where(HeirConfirmationDecision.__table__.c.id == pk))
        with self.assertRaises((IntegrityError, OperationalError)):
            _run(_del())
        db.reset_for_tests()


class TestImmutableOrmLayer(_DbMixin):
    """ORM listener: session 経由の update/delete は flush 前に ImmutableRecordError。"""

    def test_orm_update_rejected(self):
        pk = self._insert_run()

        async def _upd():
            async with db.session_scope() as s:
                obj = (await s.execute(sa.select(DerivationRun)
                                       .where(DerivationRun.id == pk))).scalar_one()
                obj.status = "held"
        with self.assertRaises(ImmutableRecordError):
            _run(_upd())
        db.reset_for_tests()

    def test_orm_delete_rejected(self):
        pk = self._insert_run()

        async def _del():
            async with db.session_scope() as s:
                obj = (await s.execute(sa.select(DerivationRun)
                                       .where(DerivationRun.id == pk))).scalar_one()
                await s.delete(obj)
        with self.assertRaises(ImmutableRecordError):
            _run(_del())
        db.reset_for_tests()


class TestVocabularyConstraints(_DbMixin):
    def test_invalid_status_rejected(self):
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(DerivationRun.__table__)
                                .values(**_run_row(status="confirmed")))  # 人の語彙は不可
        with self.assertRaises(IntegrityError):
            _run(_ins())
        db.reset_for_tests()

    def test_invalid_decision_rejected(self):
        run_id = self._insert_run()

        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(HeirConfirmationDecision.__table__).values(
                    derivation_run_id=run_id, decision="derived",  # 機械の語彙は不可
                    decided_by="a", decided_at=datetime.now(timezone.utc)))
        with self.assertRaises(IntegrityError):
            _run(_ins())
        db.reset_for_tests()


class TestSeparationContract(unittest.TestCase):
    """NH01: 分離契約と別 metadata（DB 不要の構造検査）。"""

    def test_run_has_no_human_columns(self):
        cols = set(DerivationRun.__table__.c.keys())
        for banned in ("human_state", "decided_by", "decided_at"):
            self.assertNotIn(banned, cols)

    def test_decision_carries_human_fields(self):
        cols = set(HeirConfirmationDecision.__table__.c.keys())
        self.assertLessEqual({"decision", "decided_by", "decided_at",
                              "supersedes_decision_id"}, cols)

    def test_separate_metadata_from_inbound(self):
        from hub.inbound_event import Base as InboundBase
        self.assertIsNot(DerivationBase.metadata, InboundBase.metadata)
        self.assertNotIn("derivation_run", InboundBase.metadata.tables)
        self.assertNotIn("inbound_event", DerivationBase.metadata.tables)

    def test_alembic_integration(self):
        env = Path("alembic/env.py").read_text(encoding="utf-8")
        self.assertIn("DerivationBase.metadata", env)
        mig = Path("alembic/versions/20260721_d5e2b8a1c7f3_derivation_run.py") \
            .read_text(encoding="utf-8")
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'c4f1a2b7d8e9'",
                      mig)
        self.assertEqual(mig.count("op.create_table"), 2)
        self.assertEqual(mig.count("op.drop_table"), 1)   # loop で両表を drop


if __name__ == "__main__":
    unittest.main()
