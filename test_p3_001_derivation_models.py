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


# ── fix1(P3001-H01/H02/M01) 追加テスト ───────────────────────────────────────
class TestPayloadAllowlist(_DbMixin):
    """H02: schema allowlist＋PII 様防御（immutable への誤保存を入口で遮断）。"""

    def _create(self, **over):
        from hub.derivation_models import create_derivation_run
        pk = _run(create_derivation_run(**_run_row(**over)))
        db.reset_for_tests()
        return pk

    def test_valid_payload_accepted(self):
        self.assertIsInstance(self._create(), int)

    def test_pii_like_name_rejected(self):
        from hub.derivation_models import PayloadPolicyError
        for bad in (
            {"heirs": [{"person_id": "P-1", "share": "1/2"}], "氏名": "山田太郎"},
            {"heirs": [{"person_id": "P-1", "name": "山田太郎"}]},
            {"heirs": [{"person_id": "山田太郎"}]},
            {"heirs": [{"person_id": "P-1"}], "facts": ["住所: 東京都..."]},
        ):
            with self.subTest(bad=str(bad)[:40]):
                with self.assertRaises(PayloadPolicyError):
                    self._create(result_payload=bad)
                db.reset_for_tests()

    def test_lawyer_flags_allowlist(self):
        from hub.derivation_models import PayloadPolicyError, create_derivation_run
        self._create(lawyer_flags={"flags": ["renounce_review"]},
                     input_hash="lf-ok")
        with self.assertRaises(PayloadPolicyError):
            _run(create_derivation_run(**_run_row(
                lawyer_flags={"memo": "自由記述は不可"}, input_hash="lf-ng")))
        db.reset_for_tests()

    def test_orm_insert_also_guarded(self):
        from hub.derivation_models import PayloadPolicyError

        async def _ins():
            async with db.session_scope() as s:
                s.add(DerivationRun(**_run_row(
                    result_payload={"heirs": [{"person_id": "P-1",
                                               "full_name": "許可キー外"}]})))
                await s.flush()
        with self.assertRaises(PayloadPolicyError):
            _run(_ins())
        db.reset_for_tests()


class TestChainIntegrity(_DbMixin):
    """H01: supersedes 連鎖の健全性（自己参照/2-node/cross-case/多重 head）。"""

    def _create(self, **over):
        from hub.derivation_models import create_derivation_run
        pk = _run(create_derivation_run(**_run_row(**over)))
        db.reset_for_tests()
        return pk

    def test_self_loop_rejected_by_db_check(self):
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(DerivationRun.__table__).values(
                    id=777, supersedes_run_id=777, **_run_row()))
        with self.assertRaises(IntegrityError):
            _run(_ins())
        db.reset_for_tests()

    def test_two_node_cycle_impossible(self):
        # A→B の後、A を再度 supersede（分岐＝循環の芽）は拒否。
        # A.supersedes=B への書換は immutable のため物理的に不可（既存テストで実測済み）。
        from hub.derivation_models import ChainIntegrityError
        a = self._create(input_hash="a")
        self._create(supersedes_run_id=a, input_hash="b")
        with self.assertRaises(ChainIntegrityError):
            self._create(supersedes_run_id=a, input_hash="c")
        db.reset_for_tests()

    def test_cross_case_supersede_rejected(self):
        from hub.derivation_models import ChainIntegrityError
        a = self._create(case_record_id="R-1", input_hash="x1")
        with self.assertRaises(ChainIntegrityError):
            self._create(case_record_id="R-2", supersedes_run_id=a, input_hash="x2")
        db.reset_for_tests()

    def test_second_head_without_supersede_rejected(self):
        from hub.derivation_models import ChainIntegrityError
        self._create(input_hash="h1")
        with self.assertRaises(ChainIntegrityError):
            self._create(input_hash="h2")   # supersedes 無しの 2 本目＝多重 head
        db.reset_for_tests()

    def test_nonexistent_supersede_rejected(self):
        from hub.derivation_models import ChainIntegrityError
        with self.assertRaises(ChainIntegrityError):
            self._create(supersedes_run_id=99999, input_hash="nx")
        db.reset_for_tests()


class TestRankCheck(_DbMixin):
    """M01: rank IN (0,1,2,3) の総当たり（DB CHECK 実測）。"""

    def test_rank_values(self):
        for ok in (0, 1, 2, 3):
            with self.subTest(rank=ok):
                self._insert_run(rank=ok, input_hash=f"r{ok}")
        for ng in (-1, 4, 9):
            with self.subTest(rank=ng):
                async def _ins(ng=ng):
                    async with db.session_scope() as s:
                        await s.execute(sa.insert(DerivationRun.__table__)
                                        .values(**_run_row(rank=ng,
                                                           input_hash=f"n{ng}")))
                with self.assertRaises(IntegrityError):
                    _run(_ins())
                db.reset_for_tests()


if __name__ == "__main__":
    unittest.main()
