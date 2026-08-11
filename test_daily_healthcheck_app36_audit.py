"""監視項目I: App36「戸籍確認済=yes」の decision 監査のテスト（P3-003C-H11a）。

正本 DRAFT_APP36 §3.4 H11 検知側（司令塔裁定=案(a)）の契約を pin する:
  - 検知する形: yes×decision なし / yes×held のみ / yes×rejected のみ /
    yes×run 結線なし（current 空）/ yes×confirmed だが decided_by 空
  - 検知しない形: yes×confirmed 有効 leaf あり（held→confirmed supersede 含む）/
    no×decision なし（サーバ側フィルタで判定対象外）/ env 未設定スキップ /
    DATABASE_URL 未設定スキップ
kintone は全て mock（実機・ネットワーク非依存）。DB は sqlite（P3-001 流儀）。
警報文面は件数・recordID のみ（PII 非掲載・RV10 準拠）。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import sqlalchemy as sa

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import hub.db as db  # noqa: E402
import daily_healthcheck  # noqa: E402
from hub import kintone  # noqa: E402
from hub.derivation_models import (DerivationBase,  # noqa: E402
                                   HeirConfirmationDecision,
                                   create_derivation_run, create_heir_decision)

_ENV = {
    "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "t36",
}


def _run(coro):
    return asyncio.run(coro)


def _run_row(**over):
    row = dict(case_app_id="26", case_record_id="R-1", decedent_person_id="10",
               at_date="2026-01-01", frozen_case_version="v1",
               input_person_revisions={"11": 3}, input_person_ids=["10", "11"],
               input_hash="ih" * 8, status="derived", rank=1,
               result_payload={"heirs": [{"person_id": "11", "share": "1/1"}]},
               result_hash="rh" * 8, provisional=False, engine_version="hd-1")
    row.update(over)
    return row


class _AuditBase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="h11a_")
        env = dict(_ENV)
        env["DATABASE_URL"] = f"sqlite+aiosqlite:///{self._dir}/a.db"
        self._env = patch.dict(os.environ, env)
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(DerivationBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

        # App36 の全レコード（監査は 戸籍確認済=yes のみをサーバ側フィルタで
        # 取得する——fake もその契約を模擬し、no のレコードは返さない）
        self.app36_records: list[dict] = []
        self.search_queries: list[str] = []

        async def fake_search(app, query, fields=None):
            self.search_queries.append(query)
            assert app.label == "App 36 (相続人)"
            assert '戸籍確認済 in ("yes")' in query, "サーバ側 yes フィルタ必須"
            import re as _re
            m = _re.search(r"\$id > ([0-9]+)", query)
            cursor = int(m.group(1)) if m else 0
            lim = _re.search(r"limit ([0-9]+)", query)
            limit = int(lim.group(1)) if lim else 100
            hits = sorted(
                (r for r in self.app36_records
                 if r["戸籍確認済"]["value"] == "yes"
                 and int(r["$id"]["value"]) > cursor),
                key=lambda r: int(r["$id"]["value"]))
            return [dict(r) for r in hits[:limit]]

        self._search = patch.object(kintone, "search_records", fake_search)
        self._search.start()

    def tearDown(self):
        self._search.stop()
        self._env.stop()
        db.reset_for_tests()
        shutil.rmtree(self._dir, ignore_errors=True)

    # ── helpers ──────────────────────────────────────────────────────────────
    def add_app36(self, rid: str, current: str, kakunin: str = "yes"):
        self.app36_records.append({
            "$id": {"value": rid},
            "current_derivation_run_id": {"value": current},
            "戸籍確認済": {"value": kakunin},
        })

    def mk_run(self, case: str) -> int:
        pk = _run(create_derivation_run(**_run_row(case_record_id=case)))
        db.reset_for_tests()
        return pk

    def decide(self, run_id: int, decision: str, decided_by: str = "ATT1",
               supersedes: int | None = None) -> int:
        pk = _run(create_heir_decision(
            derivation_run_id=run_id, decision=decision, decided_by=decided_by,
            decided_at=datetime.now(timezone.utc),
            supersedes_decision_id=supersedes))
        db.reset_for_tests()
        return pk

    def audit(self) -> list[str]:
        result = _run(daily_healthcheck.check_app36_confirmed_decisions())
        db.reset_for_tests()
        return result


class TestDetects(_AuditBase):
    """検知する形（yes を正当化する confirmed 有効 leaf が無い）。"""

    def test_yes_without_any_decision_detected(self):
        run_id = self.mk_run("R-1")
        self.add_app36("101", str(run_id))
        problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("1件", problems[0])
        self.assertIn("101", problems[0])

    def test_yes_with_held_only_detected(self):
        """held は yes を正当化しない（confirmed のみが正当化・裁定=案(a)）。"""
        run_id = self.mk_run("R-1")
        self.decide(run_id, "held")
        self.add_app36("102", str(run_id))
        problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("102", problems[0])

    def test_yes_with_rejected_only_detected(self):
        """rejected も yes を正当化しない。"""
        run_id = self.mk_run("R-1")
        self.decide(run_id, "rejected")
        self.add_app36("103", str(run_id))
        problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("103", problems[0])

    def test_yes_with_empty_current_detected(self):
        """current 空＝run 結線なし（kintone 手作成レコードの yes はここに落ちる）。"""
        self.add_app36("104", "")
        problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("104", problems[0])

    def test_yes_with_confirmed_but_empty_decided_by_detected(self):
        """正本文言「decided_by あり」: confirmed でも decided_by 空は未正当化
        （正規経路では起き得ない＝直接 DB 操作の検知。raw INSERT で再現）。"""
        run_id = self.mk_run("R-1")

        async def _raw():
            async with db.session_scope() as s:
                await s.execute(sa.insert(HeirConfirmationDecision.__table__)
                                .values(derivation_run_id=run_id,
                                        decision="confirmed", decided_by="",
                                        decided_at=datetime.now(timezone.utc)))
        _run(_raw())
        db.reset_for_tests()
        self.add_app36("105", str(run_id))
        problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("105", problems[0])

    def test_message_is_count_and_record_ids_only(self):
        """警報文面は件数＋recordID のみ（氏名等の PII 非掲載・RV10）。"""
        r1 = self.mk_run("R-1")
        r2 = self.mk_run("R-2")
        self.decide(r1, "held")
        self.add_app36("110", str(r1))
        self.add_app36("111", str(r2))
        problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("2件", problems[0])
        self.assertIn("110", problems[0])
        self.assertIn("111", problems[0])

    def test_pagination_walks_all_pages(self):
        """$id カーソルのページングで全件走査する（silent cap なし）。"""
        run_id = self.mk_run("R-1")
        for rid in ("201", "202", "203"):
            self.add_app36(rid, str(run_id))
        with patch.object(daily_healthcheck, "_APP36_AUDIT_PAGE", 2):
            problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("3件", problems[0])
        self.assertGreaterEqual(len(self.search_queries), 2)


class TestDoesNotDetect(_AuditBase):
    """検知しない形（正当な yes・対象外・スキップ）。"""

    def test_yes_with_confirmed_leaf_ok(self):
        run_id = self.mk_run("R-1")
        self.decide(run_id, "confirmed")
        self.add_app36("301", str(run_id))
        self.assertEqual(self.audit(), [])

    def test_yes_with_held_then_confirmed_leaf_ok(self):
        """held→confirmed の supersede 後は有効 leaf=confirmed＝正当（leaf 判定の
        単一の正 get_leaf_decision を経由することの検証）。"""
        run_id = self.mk_run("R-1")
        d1 = self.decide(run_id, "held")
        self.decide(run_id, "confirmed", supersedes=d1)
        self.add_app36("302", str(run_id))
        self.assertEqual(self.audit(), [])

    def test_no_record_without_decision_ok(self):
        """戸籍確認済=no は decision が無くても対象外（yes 遷移の監査であり、
        サーバ側 yes フィルタで取得もしない）。"""
        run_id = self.mk_run("R-1")
        self.add_app36("303", str(run_id), kakunin="no")
        self.assertEqual(self.audit(), [])
        self.assertTrue(all('戸籍確認済 in ("yes")' in q
                            for q in self.search_queries))

    def test_env_unset_skips_silently(self):
        """APP_SOUZOKUNIN 未設定＝静かにスキップ（optional 方式・App36 未点火）。"""
        os.environ.pop("APP_SOUZOKUNIN", None)
        self.add_app36("304", "")
        self.assertEqual(self.audit(), [])
        self.assertEqual(self.search_queries, [])   # kintone へも行かない

    def test_db_unset_skips_silently(self):
        """DATABASE_URL 未設定＝静かにスキップ（判定不能で誤警報しない・lazy 原則）。"""
        os.environ.pop("DATABASE_URL", None)
        db.reset_for_tests()
        self.add_app36("305", "")
        self.assertEqual(self.audit(), [])
        self.assertEqual(self.search_queries, [])

    def test_mixed_only_unjustified_reported(self):
        """confirmed 済みの行は混在しても数えない（件数は未正当化のみ）。"""
        ok_run = self.mk_run("R-1")
        self.decide(ok_run, "confirmed")
        ng_run = self.mk_run("R-2")
        self.decide(ng_run, "held")
        self.add_app36("306", str(ok_run))
        self.add_app36("307", str(ng_run))
        problems = self.audit()
        self.assertEqual(len(problems), 1)
        self.assertIn("1件", problems[0])
        self.assertIn("307", problems[0])
        self.assertNotIn("306", problems[0])


if __name__ == "__main__":
    unittest.main()
