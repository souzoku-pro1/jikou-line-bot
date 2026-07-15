"""RV-05-13: ingestion_receipt の epoch fencing 状態機械テスト（DRAFT §8 条件表準拠）。

各テストは naive（現行 process-memory）が FAIL する形。DB は sqlite（file）。
"""
import asyncio
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

os.environ.setdefault("ANTHROPIC_API_KEY", "dummy")  # 一部 import 経路の保険

import sqlalchemy as sa  # noqa: E402

import hub.db as db  # noqa: E402
from hub import ingestion_receipt as ir  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="ir_test_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(ir.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _new_receipt(self, file_id="F1", sha="S1", case="C1"):
        rid = _run(ir.upsert_receipt(ingest_type="sortation", caller_id="gas",
                                     source_file_id=file_id, source_sha256=sha, case_hint=case))
        db.reset_for_tests()
        return rid

    def _state(self, rid):
        async def _q():
            async with db.session_scope() as s:
                return (await s.execute(sa.select(ir.ingestion_receipt.c.last_outcome,
                        ir.ingestion_receipt.c.epoch)
                        .where(ir.ingestion_receipt.c.id == rid))).one()
        r = _run(_q())
        db.reset_for_tests()
        return r.last_outcome, r.epoch

    def _make_stale(self, rid):
        # DB clock（H-05）は秒解像度のため、テストは heartbeat を NULL にして stale を強制
        async def _u():
            async with db.session_scope() as s:
                await s.execute(sa.update(ir.ingestion_receipt)
                                .where(ir.ingestion_receipt.c.id == rid)
                                .values(last_heartbeat_at=None))
        _run(_u()); db.reset_for_tests()

    def _attempt_count(self, rid):
        async def _q():
            async with db.session_scope() as s:
                return (await s.execute(sa.select(sa.func.count(ir.processing_attempt.c.id))
                        .where(ir.processing_attempt.c.receipt_id == rid))).scalar_one()
        r = _run(_q()); db.reset_for_tests(); return r


class TestIdempotencyKey(unittest.TestCase):
    def test_length_prefix_no_collision(self):
        # 要素内 ":" があっても衝突しない（naive の ":" 連結は衝突する）
        k1 = ir.build_idempotency_key("a", "b:c", "d")
        k2 = ir.build_idempotency_key("a", "b", "c:d")
        self.assertNotEqual(k1, k2)

    def test_null_empty_rejected(self):
        for bad in [("a", "", "c"), ("a", None, "c")]:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    ir.build_idempotency_key(*bad)


class TestFencing(_DbMixin):
    def test_claim_then_terminal(self):
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()
        self.assertIsNotNone(ep)
        self.assertEqual(self._state(rid)[0], ir.ST_PROCESSING)
        ok = _run(ir.mark_terminal(rid, ep, ir.ST_COMPLETED)); db.reset_for_tests()
        self.assertTrue(ok)
        self.assertEqual(self._state(rid)[0], ir.ST_COMPLETED)

    def test_condition3_concurrent_claim_one_wins(self):
        # DRAFT §8 #3: 並行 claim の敗者は guard 不成立で None（concurrent_reject）
        rid = self._new_receipt()
        e1 = _run(ir.claim(rid)); db.reset_for_tests()
        e2 = _run(ir.claim(rid)); db.reset_for_tests()   # 既に processing → guard 外
        self.assertIsNotNone(e1)
        self.assertIsNone(e2)   # 敗者

    def test_condition4_stale_epoch_terminal_abort(self):
        # DRAFT §8 #4: 旧 epoch の terminal は 0 行で abort（False）
        rid = self._new_receipt()
        old = _run(ir.claim(rid)); db.reset_for_tests()
        # reconciliation 相当で epoch が進む（新 claim）
        _run(ir.mark_pending_retry(rid, old)); db.reset_for_tests()   # epoch++
        newep = _run(ir.claim(rid)); db.reset_for_tests()             # 再claim epoch++
        # 旧 epoch で terminal → abort
        aborted = _run(ir.mark_terminal(rid, old, ir.ST_COMPLETED)); db.reset_for_tests()
        self.assertFalse(aborted)
        # 新 epoch なら成功
        ok = _run(ir.mark_terminal(rid, newep, ir.ST_COMPLETED)); db.reset_for_tests()
        self.assertTrue(ok)

    def test_heartbeat_fence(self):
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()
        self.assertTrue(_run(ir.heartbeat(rid, ep))); db.reset_for_tests()
        # 再claim（別 request）で epoch 進む → 旧 heartbeat は False
        _run(ir.mark_pending_retry(rid, ep)); db.reset_for_tests()
        _run(ir.claim(rid)); db.reset_for_tests()
        self.assertFalse(_run(ir.heartbeat(rid, ep))); db.reset_for_tests()

    def test_condition2_sending_stale_to_unknown(self):
        # DRAFT §8 #2: SENDING stale → reconciliation で UNKNOWN（自動再送しない）
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()
        _run(ir.mark_phase(rid, ep, ir.ST_SENDING)); db.reset_for_tests()
        self._make_stale(rid)
        stats = _run(ir.reconcile_stale(stale_seconds=600)); db.reset_for_tests()
        self.assertEqual(stats["to_unknown"], 1)
        self.assertEqual(self._state(rid)[0], ir.ST_UNKNOWN)

    def test_condition1_vendor_pre_stale_reconcile_pending_retry(self):
        # DRAFT §8 #1: vendor_pre stale → reconciliation は PENDING_RETRY 可視化のみ（再処理せず）
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()
        _run(ir.mark_phase(rid, ep, ir.ST_VENDOR_PRE)); db.reset_for_tests()
        self._make_stale(rid)
        stats = _run(ir.reconcile_stale(stale_seconds=600)); db.reset_for_tests()
        self.assertEqual(stats["to_pending_retry"], 1)
        self.assertEqual(self._state(rid)[0], ir.ST_PENDING_RETRY)
        # PENDING_RETRY は再 claim 可能（GAS 再送）
        ep2 = _run(ir.claim(rid)); db.reset_for_tests()
        self.assertIsNotNone(ep2)

    def test_m01_reconciliation_writes_audit_row(self):
        # M-01: reconciliation も epoch++ ＋ 同一 tx で監査行を残す
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()          # attempt#1
        _run(ir.mark_phase(rid, ep, ir.ST_SENDING)); db.reset_for_tests()   # attempt#2
        before = self._attempt_count(rid)
        self._make_stale(rid)
        _run(ir.reconcile_stale(stale_seconds=600)); db.reset_for_tests()   # attempt#3(unknown)
        self.assertEqual(self._attempt_count(rid), before + 1)  # 監査行が増える

    def test_m04_reconcile_invalidates_inflight_epoch(self):
        # M-04 barrier: in-flight（my_epoch）中に reconciliation が走ると、in-flight の
        # terminal/heartbeat は fence 喪失（False）＝処理中断すべき
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()
        _run(ir.mark_phase(rid, ep, ir.ST_VENDOR_PRE)); db.reset_for_tests()
        cur = self._state(rid)[1]   # in-flight の最新 epoch
        self._make_stale(rid)
        _run(ir.reconcile_stale(stale_seconds=600)); db.reset_for_tests()   # epoch++（PENDING_RETRY）
        # in-flight の cur epoch では heartbeat も terminal も 0 行（fence 喪失）
        self.assertFalse(_run(ir.heartbeat(rid, cur))); db.reset_for_tests()
        self.assertFalse(_run(ir.mark_terminal(rid, cur, ir.ST_COMPLETED))); db.reset_for_tests()

    def test_condition7_duplicate_suspect(self):
        # DRAFT §8 #7: 同 file_id/sha・case_hint 相違 → duplicate_suspect（held・人手）
        rid = self._new_receipt(file_id="F", sha="S", case="caseA")
        _run(ir.claim(rid)); db.reset_for_tests()
        # 同一 file_id/sha だが case_hint 相違 → 衝突
        with self.assertRaises(ir.ReceiptConflict):
            _run(ir.upsert_receipt(ingest_type="sortation", caller_id="gas",
                                   source_file_id="F", source_sha256="S", case_hint="caseB"))
        db.reset_for_tests()
        self.assertEqual(self._state(rid)[0], ir.ST_DUPLICATE_SUSPECT)

    def test_dedup_same_elements_skips(self):
        rid = self._new_receipt(file_id="F", sha="S", case="C")
        rid2 = _run(ir.upsert_receipt(ingest_type="sortation", caller_id="gas",
                                      source_file_id="F", source_sha256="S", case_hint="C"))
        db.reset_for_tests()
        self.assertEqual(rid, rid2)   # 冪等（同一 receipt）

    def test_manual_reset(self):
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()
        _run(ir.mark_phase(rid, ep, ir.ST_SENDING)); db.reset_for_tests()
        self._make_stale(rid)
        _run(ir.reconcile_stale(600)); db.reset_for_tests()   # → UNKNOWN
        self.assertTrue(_run(ir.manual_reset(rid))); db.reset_for_tests()
        self.assertEqual(self._state(rid)[0], ir.ST_RECEIVED)

    def test_md501_convergence_no_double_count(self):
        # M-D5-01: UNKNOWN→reset→completed で二重計上しない（distinct 集計）
        rid = self._new_receipt()
        ep = _run(ir.claim(rid)); db.reset_for_tests()
        _run(ir.mark_phase(rid, ep, ir.ST_SENDING)); db.reset_for_tests()
        self._make_stale(rid)
        _run(ir.reconcile_stale(600)); db.reset_for_tests()       # UNKNOWN
        _run(ir.manual_reset(rid)); db.reset_for_tests()           # received
        ep2 = _run(ir.claim(rid)); db.reset_for_tests()
        _run(ir.mark_terminal(rid, ep2, ir.ST_COMPLETED)); db.reset_for_tests()
        stats = _run(ir.convergence_stats()); db.reset_for_tests()
        self.assertEqual(stats["distinct_total"], 1)   # 二重計上なし
        self.assertEqual(stats["converged"], 1)
        self.assertEqual(stats["convergence_rate"], 1.0)


if __name__ == "__main__":
    unittest.main()
