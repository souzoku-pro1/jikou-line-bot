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
    # person_id は App34 kintone `$id`＝数字列（fix2 grammar・heir_derivation.py:122）
    row = dict(case_app_id="26", case_record_id="R-1", decedent_person_id="10",
               at_date="2026-01-01", frozen_case_version="v1",
               input_person_revisions={"11": 3}, input_person_ids=["10", "11"],
               input_hash="ih" * 8, status="derived", rank=1,
               result_payload={"heirs": [{"person_id": "11", "share": "1/1"}]},
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
            {"heirs": [{"person_id": "11", "share": "1/2"}], "氏名": "山田太郎"},
            {"heirs": [{"person_id": "11", "name": "山田太郎"}]},
            {"heirs": [{"person_id": "山田太郎"}]},
            {"heirs": [{"person_id": "11"}], "facts": ["住所: 東京都..."]},
        ):
            with self.subTest(bad=str(bad)[:40]):
                with self.assertRaises(PayloadPolicyError):
                    self._create(result_payload=bad)
                db.reset_for_tests()

    def test_lawyer_flags_allowlist(self):
        from hub.derivation_models import PayloadPolicyError, create_derivation_run
        self._create(lawyer_flags={"flags": ["F1", "simultaneous_death"]},
                     input_hash="lf-ok")   # fix3: 実導出由来の enum（F1=放棄・同時死亡推定）
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
        # fix2 H03 の single-root 部分ユニークと干渉しないよう case を分ける
        for ok in (0, 1, 2, 3):
            with self.subTest(rank=ok):
                self._insert_run(rank=ok, input_hash=f"r{ok}",
                                 case_record_id=f"R-rank-{ok}")
        for ng in (-1, 4, 9):
            with self.subTest(rank=ng):
                async def _ins(ng=ng):
                    async with db.session_scope() as s:
                        await s.execute(sa.insert(DerivationRun.__table__)
                                        .values(**_run_row(rank=ng,
                                                           case_record_id=f"R-ng{ng}",
                                                           input_hash=f"n{ng}")))
                with self.assertRaises(IntegrityError):
                    _run(_ins())
                db.reset_for_tests()


# ── fix2(P3001-2-H01〜H04) 追加テスト ────────────────────────────────────────
class TestFieldGrammar(_DbMixin):
    """H01: field 別 grammar/enum（ASCII でも自由文字列は保存不可）。"""

    def _create(self, **over):
        from hub.derivation_models import create_derivation_run
        pk = _run(create_derivation_run(**_run_row(**over)))
        db.reset_for_tests()
        return pk

    def test_ascii_pii_rejected(self):
        # ASCII の氏名（ローマ字）/メール/住所/電話も grammar で拒否される
        from hub.derivation_models import PayloadPolicyError
        cases = {
            "romaji_name": {"heirs": [{"person_id": "Yamada Taro"}]},
            "email": {"heirs": [{"person_id": "11"}],
                      "facts": ["taro@example.com"]},
            "address": {"heirs": [{"person_id": "1-2-3 Chiyoda Tokyo"}]},
            "phone": {"heirs": [{"person_id": "090-1234-5678"}]},
        }
        for label, bad in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(PayloadPolicyError):
                    self._create(result_payload=bad)
                db.reset_for_tests()

    def test_share_and_relation_grammar(self):
        from hub.derivation_models import PayloadPolicyError
        self._create(result_payload={
            "heirs": [{"person_id": "11", "share": "1/2",
                       "relation_key": "spouse"}],
            "facts": ["minpo_890", "minpo_900_1"]})
        for bad_heir in ({"person_id": "11", "share": "0.5"},
                         {"person_id": "11", "share": "half"},
                         {"person_id": "11", "relation_key": "配偶者"},
                         {"person_id": "11", "relation_key": "friend"}):
            with self.subTest(bad=str(bad_heir)):
                with self.assertRaises(PayloadPolicyError):
                    self._create(result_payload={"heirs": [bad_heir]},
                                 case_record_id="R-g2")
                db.reset_for_tests()

    def test_facts_type_and_enum_rejected(self):
        from hub.derivation_models import PayloadPolicyError
        for bad_facts in ([123], [{"key": "minpo_890"}], [None],
                          ["民法890条"], ["minpo_999"], "minpo_890"):
            with self.subTest(bad=str(bad_facts)[:30]):
                with self.assertRaises(PayloadPolicyError):
                    self._create(result_payload={"heirs": [], "facts": bad_facts})
                db.reset_for_tests()


# fix3 改定裁定: 旧 TestCoreBypassPin（迂回成功の pin）は「脆弱性目録になる」判定により
# 削除。Core 迂回の防御は test_p3_core_ast_policy（AST 機械検査）＋正規 module 内ガードの
# 二段へ置換（hub/derivation_models.py の fix3 節参照）。


class TestSingleRootDbLevel(_DbMixin):
    """H03: 並行初回作成の DB 遮断（部分ユニーク: case の root は 1 行）。"""

    def test_second_root_core_insert_rejected(self):
        self._insert_run(input_hash="root1")
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(DerivationRun.__table__)
                                .values(**_run_row(input_hash="root2")))
        with self.assertRaises(IntegrityError):   # repo 層を迂回しても DB が遮断
            _run(_ins())
        db.reset_for_tests()


class TestHcdChainGuards(_DbMixin):
    """H04: HeirConfirmationDecision への連鎖 guard 横展開。"""

    def _decide(self, **fields):
        from hub.derivation_models import create_heir_decision
        base = dict(decision="held", decided_by="attorney-1",
                    decided_at=datetime.now(timezone.utc))
        base.update(fields)
        pk = _run(create_heir_decision(**base))
        db.reset_for_tests()
        return pk

    def test_happy_append_and_supersede(self):
        run_id = self._insert_run()
        d1 = self._decide(derivation_run_id=run_id)
        d2 = self._decide(derivation_run_id=run_id, decision="confirmed",
                          supersedes_decision_id=d1)
        self.assertGreater(d2, d1)

    def test_self_reference_rejected_by_db_check(self):
        run_id = self._insert_run()
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(HeirConfirmationDecision.__table__).values(
                    id=555, derivation_run_id=run_id, decision="held",
                    decided_by="a", decided_at=datetime.now(timezone.utc),
                    supersedes_decision_id=555))
        with self.assertRaises(IntegrityError):
            _run(_ins())
        db.reset_for_tests()

    def test_cross_run_supersede_rejected(self):
        from hub.derivation_models import ChainIntegrityError
        run1 = self._insert_run(case_record_id="R-a")
        run2 = self._insert_run(case_record_id="R-b", input_hash="ih-b")
        d1 = self._decide(derivation_run_id=run1)
        with self.assertRaises(ChainIntegrityError):
            self._decide(derivation_run_id=run2, supersedes_decision_id=d1)

    def test_nonexistent_refs_rejected(self):
        from hub.derivation_models import ChainIntegrityError
        with self.assertRaises(ChainIntegrityError):
            self._decide(derivation_run_id=99999)
        run_id = self._insert_run()
        with self.assertRaises(ChainIntegrityError):
            self._decide(derivation_run_id=run_id, supersedes_decision_id=88888)

    def test_second_root_decision_rejected(self):
        # fix3 H04: 同一 run の root decision 一意性（repo 検査＋DB 部分ユニーク）
        from hub.derivation_models import ChainIntegrityError
        run_id = self._insert_run()
        self._decide(derivation_run_id=run_id)
        with self.assertRaises(ChainIntegrityError):     # repo 層
            self._decide(derivation_run_id=run_id)

        async def _core_second_root():                   # repo 迂回でも DB が遮断
            async with db.session_scope() as s:
                await s.execute(sa.insert(HeirConfirmationDecision.__table__).values(
                    derivation_run_id=run_id, decision="held", decided_by="b",
                    decided_at=datetime.now(timezone.utc)))
        with self.assertRaises(IntegrityError):
            _run(_core_second_root())
        db.reset_for_tests()


# ── fix4(P30014-H02) 胎児合成 ID（司令塔裁定の収載検証） ────────────────────
class TestFetusSyntheticId(unittest.TestCase):
    """H02 裁定: 胎児 ID は役割語の自由文字列を保存せず、build_run_payload が
    `胎児:F{n}`（run 内出現順連番）へ写像する。grammar は合成 ID のみ許可。"""

    def test_build_run_payload_maps_labels_to_sequential_synthetic_ids(self):
        import json

        from heir_derivation import Declarations, HeirPerson, derive_heirs
        from hub.derivation_models import build_run_payload, validate_result_payload
        d = derive_heirs(
            [HeirPerson(record_id="10", name="被", alive="死亡",
                        death_date="2025-04-13", is_decedent=True),
             HeirPerson(record_id="11", name="長男", alive="生存",
                        father_id="10")],
            Declarations(fetuses=["妻", "第2子"]))   # 役割語の自由入力ラベル 2 件
        payload, _ = build_run_payload(d)
        fetus_ids = [h["person_id"] for h in payload["heirs"]
                     if h["person_id"].startswith("胎児:")]
        self.assertEqual(fetus_ids, ["胎児:F1", "胎児:F2"], payload)   # 出現順連番
        s = json.dumps(payload, ensure_ascii=False)
        self.assertNotIn("胎児:妻", s)      # 元ラベルとの対応も保存しない（裁定）
        self.assertNotIn("第2子", s)
        validate_result_payload(payload)    # 変換後は grammar を通過する

    def test_grammar_rejects_free_label_fetus_id(self):
        from hub.derivation_models import PayloadPolicyError, validate_result_payload
        for bad in ("胎児:妻", "胎児:", "胎児:F", "胎児:Fx", "胎児:F1a"):
            with self.subTest(pid=bad):
                with self.assertRaises(PayloadPolicyError):
                    validate_result_payload(
                        {"heirs": [{"person_id": bad}], "facts": []})
        validate_result_payload(   # 合成 ID は許可
            {"heirs": [{"person_id": "胎児:F1"}], "facts": []})


# ── fix5(P30015-M01) 胎児連番契約（validate 時の契約強制） ──────────────────
class TestFetusSequenceContract(unittest.TestCase):
    """M01 裁定: run 内の胎児 ID 集合は {F1..Fn}（F1 起点・正整数・連続・重複なし）。
    F0・先頭ゼロ・欠番・重複を拒否。採番は出現ごと（同一ラベルでも別番号）。"""

    def test_f0_and_leading_zero_rejected(self):
        from hub.derivation_models import PayloadPolicyError, validate_result_payload
        for bad in ("胎児:F0", "胎児:F01", "胎児:F00", "胎児:F010"):
            with self.subTest(pid=bad):
                with self.assertRaises(PayloadPolicyError):
                    validate_result_payload(
                        {"heirs": [{"person_id": bad}], "facts": []})

    def test_duplicate_gap_and_nonstart_rejected(self):
        from hub.derivation_models import PayloadPolicyError, validate_result_payload
        cases = {
            "duplicate": ["胎児:F1", "胎児:F1"],          # 重複
            "gap": ["胎児:F1", "胎児:F3"],                # 欠番（F2 なし）
            "non_f1_start": ["胎児:F2"],                  # F1 起点でない
            "non_f1_start_pair": ["胎児:F2", "胎児:F3"],  # 連続だが F1 起点でない
        }
        for label, pids in cases.items():
            with self.subTest(case=label):
                with self.assertRaises(PayloadPolicyError):
                    validate_result_payload(
                        {"heirs": [{"person_id": p} for p in pids], "facts": []})
        # 対照: {F1..Fn} 完全一致は通る（並び順は問わない）
        validate_result_payload(
            {"heirs": [{"person_id": "胎児:F2"}, {"person_id": "胎児:F1"}],
             "facts": []})

    def test_same_label_twice_gets_distinct_numbers(self):
        # 裁定: 同一ラベル2回出現でも別番号（辞書写像の同一化を解消）。
        # 導出器（凍結）を介さず build_run_payload の変換契約として直接検証する。
        from fractions import Fraction
        from types import SimpleNamespace

        from hub.derivation_models import build_run_payload, validate_result_payload
        heirs = [SimpleNamespace(person_id="胎児:妻", zokugara="胎児",
                                 share=Fraction(1, 4), basis=["民法886条"]),
                 SimpleNamespace(person_id="胎児:妻", zokugara="胎児",
                                 share=Fraction(1, 4), basis=["民法886条"])]
        payload, _ = build_run_payload(SimpleNamespace(heirs=heirs, flags=[]))
        pids = [h["person_id"] for h in payload["heirs"]]
        self.assertEqual(pids, ["胎児:F1", "胎児:F2"], payload)   # 同一化しない
        validate_result_payload(payload)                          # 連番契約も通過


# ── P3-001 改定票（P3-003B_DESIGN 裁定1・2026-07-30）: 続柄区分コード ────────
class TestZokugaraCodeEnum(unittest.TestCase):
    """裁定1: heirs 行の zokugara_code は固定9値 ASCII enum の閉集合。
    値は DRAFT_P3_003B_DESIGN §3.1 表/§3.2 の凍結9値と逐語一致（独自値名なし）。"""

    # §3.2 の凍結9値（設計書と逐語一致・この定数がテスト側の対照凍結コピー）
    FROZEN_9 = frozenset({
        "spouse", "child", "lineal_ascendant", "sibling", "nephew_niece_rep",
        "grandchild_rep", "further_rep", "fetus", "successive"})

    def test_nine_codes_verbatim_closed_set(self):
        from hub.derivation_models import ZOKUGARA_CODES
        self.assertEqual(ZOKUGARA_CODES, self.FROZEN_9)
        self.assertIsInstance(ZOKUGARA_CODES, frozenset)   # 公開定数は不変型

    def test_mapping_total_over_engine_labels(self):
        # §3.1 表: heir_derivation の生成 9 区分すべてにコードが定まる（total 写像）
        from hub.derivation_models import (relation_key_of, zokugara_code_of)
        expected = {
            "配偶者": "spouse", "子": "child", "胎児": "fetus",
            "孫（代襲）": "grandchild_rep", "甥姪（代襲）": "nephew_niece_rep",
            "再代襲（曾孫等）": "further_rep",
            "直系尊属": "lineal_ascendant", "兄弟姉妹": "sibling",
            "数次承継（No.5 花子 の 子）": "successive",   # 前方一致
        }
        collapse = {"grandchild_rep": "representative",
                    "nephew_niece_rep": "representative",
                    "further_rep": "representative"}
        for zoku, code in expected.items():
            with self.subTest(zokugara=zoku):
                self.assertEqual(zokugara_code_of(zoku), code)
                # relation_key との整合（collapse 方向・§3.1 表と1対1）
                self.assertEqual(collapse.get(code, code), relation_key_of(zoku))

    def test_unknown_zokugara_rejected(self):
        from hub.derivation_models import PayloadPolicyError, zokugara_code_of
        for bad in ("親戚", "spouse", "", None, 1):   # 日本語ラベル以外は写像に無い
            with self.subTest(zokugara=bad):
                with self.assertRaises(PayloadPolicyError):
                    zokugara_code_of(bad)

    def test_enum_closed_set_in_validate(self):
        from hub.derivation_models import (PayloadPolicyError,
                                           validate_result_payload)
        for code in sorted(self.FROZEN_9):   # 9値全数が受理される
            with self.subTest(ok=code):
                validate_result_payload(
                    {"heirs": [{"person_id": "11", "zokugara_code": code}],
                     "facts": []})
        for bad in ("配偶者", "grandchild", "SPOUSE", "", 123, None,
                    ["spouse"], "representative"):   # relation_key の語彙も enum 外
            with self.subTest(bad=str(bad)[:20]):
                with self.assertRaises(PayloadPolicyError):
                    validate_result_payload(
                        {"heirs": [{"person_id": "11", "zokugara_code": bad}],
                         "facts": []})

    def test_code_relation_key_consistency_enforced(self):
        from hub.derivation_models import (PayloadPolicyError,
                                           validate_result_payload)
        validate_result_payload(   # §3.1 表どおりの併存は受理
            {"heirs": [{"person_id": "11", "relation_key": "representative",
                        "zokugara_code": "grandchild_rep"}], "facts": []})
        for code, rel in (("grandchild_rep", "child"),
                          ("spouse", "sibling"),
                          ("successive", "representative")):
            with self.subTest(code=code, rel=rel):
                with self.assertRaises(PayloadPolicyError):
                    validate_result_payload(
                        {"heirs": [{"person_id": "11", "relation_key": rel,
                                    "zokugara_code": code}], "facts": []})

    def test_build_run_payload_carries_code_for_every_heir(self):
        from fractions import Fraction
        from types import SimpleNamespace

        from hub.derivation_models import (build_run_payload,
                                           payload_has_zokugara_codes,
                                           validate_result_payload)
        heirs = [SimpleNamespace(person_id="11", zokugara="配偶者",
                                 share=Fraction(1, 2), basis=["民法890条"]),
                 SimpleNamespace(person_id="12", zokugara="孫（代襲）",
                                 share=Fraction(1, 4), basis=["民法887条2項"]),
                 SimpleNamespace(person_id="13",
                                 zokugara="数次承継（No.9 二郎 の 子）",
                                 share=Fraction(1, 4), basis=["民法896条"])]
        payload, _ = build_run_payload(SimpleNamespace(heirs=heirs, flags=[]))
        self.assertEqual([h["zokugara_code"] for h in payload["heirs"]],
                         ["spouse", "grandchild_rep", "successive"], payload)
        validate_result_payload(payload)                    # 閉集合・整合を通過
        self.assertTrue(payload_has_zokugara_codes(payload))   # 改定後 payload と判別


class TestZokugaraCodeNonExposure(unittest.TestCase):
    """§3.2 M03 非露出: person_id と結合した続柄区分の値は例外文言へ出さない
    （既存 sink 検査と同型の sentinel 方式。PII 断定はしない=最小化対象として扱う）。"""

    def _assert_sentinels_absent(self, exc, *sentinels):
        surfaces = [str(exc), repr(exc), repr(exc.args)]
        for s in surfaces:
            for sent in sentinels:
                self.assertNotIn(sent, s, surfaces)

    def test_enum_violation_exposes_neither_code_nor_person_id(self):
        from hub.derivation_models import (PayloadPolicyError,
                                           validate_result_payload)
        pid_sentinel = "7777777707"
        code_sentinel = "SECRET_ZOKUGARA_SENTINEL_X"
        with self.assertRaises(PayloadPolicyError) as ctx:
            validate_result_payload(
                {"heirs": [{"person_id": pid_sentinel,
                            "zokugara_code": code_sentinel}], "facts": []})
        self._assert_sentinels_absent(ctx.exception, pid_sentinel, code_sentinel)

    def test_consistency_violation_exposes_no_values(self):
        from hub.derivation_models import (PayloadPolicyError,
                                           validate_result_payload)
        pid_sentinel = "7777777708"
        with self.assertRaises(PayloadPolicyError) as ctx:
            validate_result_payload(
                {"heirs": [{"person_id": pid_sentinel, "relation_key": "child",
                            "zokugara_code": "grandchild_rep"}], "facts": []})
        # 正当な enum 値どうしの矛盾でも、値と person_id を文言に載せない
        self._assert_sentinels_absent(ctx.exception, pid_sentinel,
                                      "grandchild_rep", "child")

    def test_mapping_failure_exposes_no_zokugara_value(self):
        from hub.derivation_models import PayloadPolicyError, zokugara_code_of
        label_sentinel = "続柄SENTINEL孫X"
        with self.assertRaises(PayloadPolicyError) as ctx:
            zokugara_code_of(label_sentinel)
        self._assert_sentinels_absent(ctx.exception, label_sentinel)


class TestZokugaraCodeHashMaterial(_DbMixin):
    """CMD 裁定5/§4B との整合: zokugara_code は result_payload 内＝result_hash
    （canonical(result_payload)）の材料に入る。input_hash 材料列（input_*／
    engine_version／frozen_case_version）は本改定で不変（列追加・DDL なし）。"""

    def test_code_difference_changes_result_hash_material(self):
        import hashlib
        import json

        from hub.derivation_models import validate_result_payload

        def canon_sha(p):   # §1.1/§4B と同型の決定的直列化（テスト内対照実装）
            return hashlib.sha256(json.dumps(
                p, ensure_ascii=False, sort_keys=True,
                separators=(",", ":")).encode("utf-8")).hexdigest()

        base = {"heirs": [{"person_id": "11", "relation_key": "representative",
                           "zokugara_code": "grandchild_rep"}], "facts": []}
        other = {"heirs": [{"person_id": "11", "relation_key": "representative",
                            "zokugara_code": "nephew_niece_rep"}], "facts": []}
        validate_result_payload(base)
        validate_result_payload(other)
        self.assertNotEqual(canon_sha(base), canon_sha(other))   # 材料差＝別 hash
        legacy = {"heirs": [{"person_id": "11",
                             "relation_key": "representative"}], "facts": []}
        validate_result_payload(legacy)
        self.assertNotEqual(canon_sha(base), canon_sha(legacy))  # 旧形とも別 hash

    def test_input_material_columns_unchanged(self):
        # 本票は payload 内キー追加のみ。derivation_run の列集合（input_hash 材料
        # 供給列を含む）に増減が無いことを pin（DDL/migration 無変更の構造証明）
        self.assertEqual(set(DerivationRun.__table__.c.keys()), {
            "id", "case_app_id", "case_record_id", "decedent_person_id",
            "at_date", "frozen_case_version", "input_person_revisions",
            "input_person_ids", "input_hash", "status", "rank",
            "result_payload", "result_hash", "lawyer_flags", "provisional",
            "supersedes_run_id", "engine_version", "created_at"})

    def test_new_payload_persists_via_regular_path(self):
        # 改定後 payload（コード入り）が正規経路（create_derivation_run）で保存到達
        from hub.derivation_models import create_derivation_run
        pk = _run(create_derivation_run(**_run_row(result_payload={
            "heirs": [{"person_id": "11", "share": "1/2",
                       "relation_key": "spouse", "zokugara_code": "spouse"}],
            "facts": ["minpo_890"]})))
        db.reset_for_tests()
        self.assertIsInstance(pk, int)


class TestZokugaraCodeVersionDiscrimination(_DbMixin):
    """§3.2「旧 run」: 改定前 payload（コード欠落）は保存有効なまま共存し、
    payload_has_zokugara_codes で False（精密 projection 不可＝要確認）と判別できる。
    App36 既存レコード 0 件のため data migration は無い（判別のみで足りる）。"""

    def test_discrimination_over_payload_forms(self):
        from hub.derivation_models import payload_has_zokugara_codes
        new = {"heirs": [{"person_id": "11", "zokugara_code": "spouse"},
                         {"person_id": "12", "zokugara_code": "child"}],
               "facts": []}
        legacy = {"heirs": [{"person_id": "11", "relation_key": "spouse"}],
                  "facts": []}
        mixed = {"heirs": [{"person_id": "11", "zokugara_code": "spouse"},
                           {"person_id": "12"}], "facts": []}
        self.assertTrue(payload_has_zokugara_codes(new))
        self.assertFalse(payload_has_zokugara_codes(legacy))    # 旧 run＝要確認側
        self.assertFalse(payload_has_zokugara_codes(mixed))     # 1行でも欠落なら旧扱い
        self.assertTrue(payload_has_zokugara_codes({"heirs": [], "facts": []}))
        self.assertFalse(payload_has_zokugara_codes(None))      # 構造外は安全側
        self.assertFalse(payload_has_zokugara_codes({"facts": []}))

    def test_legacy_payload_still_persists(self):
        # 旧形 payload の保存契約は不変（既存 run と同形の新規保存も拒否しない＝
        # 期待値の緩和ではなく現行契約の pin。判別は projection 側の責務）
        from hub.derivation_models import create_derivation_run
        pk = _run(create_derivation_run(**_run_row()))   # _run_row はコード無し旧形
        db.reset_for_tests()
        self.assertIsInstance(pk, int)


if __name__ == "__main__":
    unittest.main()
