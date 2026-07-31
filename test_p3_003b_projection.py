"""P3-003b: App36 関所＋projection（hub/heir_projection）のテスト。

正本 DRAFT_P3_003B_DESIGN（§2 grammar M02・§2A 胎児停止・§3 写像・§4A/§4B 書込み表・
§5 冪等キー検索と1件一致状態表・重複収束 fix3 M01）＋ENVELOPE_FLOW §3.2〜3.4
（3 phase・ATTORNEY_ALLOWLIST・stale・human_state 保護）の契約を pin する。
kintone は全て mock（実機・ネットワーク非依存）。DB は sqlite（P3-001 流儀）。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import hub.db as db  # noqa: E402
from config import EXPECTED_KINTONE_SCHEMA  # noqa: E402
from hub import heir_projection as hp  # noqa: E402
from hub.derivation_models import (DerivationBase,  # noqa: E402
                                   HeirConfirmationDecision, ZOKUGARA_CODES,
                                   create_derivation_run)
from review_resolve import RESOLVERS, ReviewGroup, ReviewItem, resolve_group  # noqa: E402

_ENV = {
    "ATTORNEY_ALLOWLIST": "ATT1,ATT2",
    "SOUZOKU_KINTONE_APP_ID": "26", "KINTONE_APP_ID": "21",
    "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "t36",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
}

SENTINEL_NAME = "機密相続PII花子SENTINEL"
APP36_ZOKUGARA_VALUES = tuple(hp.ZOKUGARA_CODE_TO_APP36.values())


def _run(coro):
    return asyncio.run(coro)


def _payload(*heirs):
    return {"heirs": list(heirs), "facts": ["minpo_890"]}


def _heir(pid, code, share="1/2", relation=None):
    h = {"person_id": pid, "zokugara_code": code}
    if share is not None:
        h["share"] = share
    if relation is not None:
        h["relation_key"] = relation
    return h


def _item(run_id, record_id="70"):
    return ReviewItem(record_id=record_id, subject="相続人導出の確認",
                      detail={"derivation_run_id": run_id,
                              "case_record_id": "9", "冪等キー": "k"})


def _group(run_id, record_id="70"):
    return ReviewGroup(source="heir_derivation", idempotency_key="k",
                       items=[_item(run_id, record_id)])


class _ProjBase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3b_")
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

        # kintone mock: 封筒 get / App36 検索（pid → rows）/ create / update
        self.envelope = {"発送ステータス": {"value": "要確認"},
                         "実行済み": {"value": "no"}}
        self.app36_rows: dict[str, list] = {}
        self.created = []          # (app_label, fields)
        self.updated = []          # (app_label, record_id, fields)

        async def fake_get(app, rid):
            return dict(self.envelope)

        async def fake_search(app, query, fields=None):
            import re as _re
            m = _re.search(r'導出元人物ID = "([0-9]+)"', query)
            return list(self.app36_rows.get(m.group(1) if m else "", []))

        async def fake_create(app, fields):
            self.created.append((app.label, dict(fields)))
            return str(900 + len(self.created))

        async def fake_update(app, rid, fields):
            self.updated.append((app.label, str(rid), dict(fields)))

        self.get = AsyncMock(side_effect=fake_get)
        self.search = AsyncMock(side_effect=fake_search)
        self.create = AsyncMock(side_effect=fake_create)
        self.update = AsyncMock(side_effect=fake_update)
        self.alert = AsyncMock(return_value=True)
        for target, mock in [("get_record", self.get),
                             ("search_records", self.search),
                             ("create_record", self.create),
                             ("update_record", self.update)]:
            p = patch(f"hub.kintone.{target}", new=mock)
            p.start()
            self.addCleanup(p.stop)
        p = patch("hub.notify.notify_admin_line", new=self.alert)
        p.start()
        self.addCleanup(p.stop)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _mk_run(self, payload, *, case="9", supersedes=None, status="derived",
                ih=None):
        pk = _run(create_derivation_run(
            case_app_id="26", case_record_id=case, decedent_person_id="10",
            at_date="2026-01-01", frozen_case_version="v0.1",
            input_person_revisions={}, input_person_ids=[],
            input_hash=ih or f"ih-{os.urandom(8).hex()}", status=status, rank=1,
            result_payload=payload, result_hash="rh" * 32,
            lawyer_flags=None, provisional=True,
            supersedes_run_id=supersedes, engine_version="e1"))
        db.reset_for_tests()
        return pk

    def _resolve(self, group, decided_by="ATT1"):
        r = _run(hp._resolve_heir_derivation(group, "9", decided_by=decided_by))
        db.reset_for_tests()
        return r

    def _decisions(self):
        async def _q():
            async with db.session_scope() as s:
                return (await s.execute(
                    sa.select(HeirConfirmationDecision.__table__))).all()
        rows = _run(_q())
        db.reset_for_tests()
        return rows

    def assert_write_zero(self):
        self.create.assert_not_awaited()
        self.update.assert_not_awaited()
        self.assertEqual(self._decisions(), [])


# ── 写像・grammar・重複分類（DB 不要の純関数契約）────────────────────────────
class TestMappingAndGrammar(unittest.TestCase):
    def test_mapping_closed_against_frozen_enum(self):
        # 単一の正 ZOKUGARA_CODES との整合（fetus のみ写像対象外＝停止）
        self.assertEqual(set(hp.ZOKUGARA_CODE_TO_APP36) | {"fetus"},
                         set(ZOKUGARA_CODES))
        self.assertNotIn("fetus", hp.ZOKUGARA_CODE_TO_APP36)

    def test_mapping_targets_within_dropdown_10_values(self):
        # 実機 dropdown 10値（config 追随）に写像先が全て存在し、
        # 受遺者（相続人外）・その他は機械写像の対象外（§3.1 整合・発注 b）
        options = EXPECTED_KINTONE_SCHEMA["App 36 (相続人)"]["fields"][
            "続柄"]["required_options"]
        self.assertEqual(len(options), 10)
        for v in hp.ZOKUGARA_CODE_TO_APP36.values():
            self.assertIn(v, options)
        self.assertNotIn("受遺者（相続人外）", hp.ZOKUGARA_CODE_TO_APP36.values())
        self.assertNotIn("その他", hp.ZOKUGARA_CODE_TO_APP36.values())
        for added in ("孫（代襲）", "再代襲（曾孫等）", "数次承継"):
            self.assertIn(added, options)          # 裁定2 の3値（config pin）
        fields = EXPECTED_KINTONE_SCHEMA["App 36 (相続人)"]["fields"]
        self.assertIn("current_derivation_run_id", fields)   # §2 新設2 field
        self.assertIn("導出元人物ID", fields)

    def test_share_display(self):
        self.assertEqual(hp.share_to_display("1/2"), "2分の1")
        self.assertEqual(hp.share_to_display("1/6"), "6分の1")
        self.assertEqual(hp.share_to_display("1/1"), "1分の1")   # 裁定4・分岐なし
        self.assertEqual(hp.share_to_display("2/4"), "4分の2")   # 再約分しない
        self.assertEqual(hp.share_to_display("0/2"), "2分の0")   # 保存層が正（0分子）
        for bad in ("0.5", "half", "1/0", "", None, "1分の2"):
            with self.subTest(bad=bad):
                with self.assertRaises(hp.ProjectionPolicyError):
                    hp.share_to_display(bad)

    def test_run_id_two_stage_grammar(self):
        self.assertEqual(hp.validate_run_id_str("1"), "1")
        self.assertEqual(hp.validate_run_id_str("9223372036854775807"),
                         "9223372036854775807")            # int64 上限ちょうど
        for bad in ("0", "01", "", "9223372036854775808",   # 上限+1（regex は通る）
                    "9999999999999999999",                  # 19桁だが int64 超
                    "12345678901234567890", "x", None, 5):
            with self.subTest(bad=bad):
                with self.assertRaises(hp.ProjectionPolicyError):
                    hp.validate_run_id_str(bad)

    def test_duplicate_classification(self):
        def row(rid, cur):
            return {"$id": {"value": rid},
                    "current_derivation_run_id": {"value": cur}}
        # 同一 head 確認済み → $id 最小の tiebreak（提示のみ・削除は人手）
        cls = hp.classify_duplicate_rows([row("9", "5"), row("4", "5")], "5")
        self.assertEqual(cls, {"action": "tiebreak", "keep": "4",
                               "manual_delete_candidates": ["9"]})
        # 比較不能系（current 不一致・空・head 不一致）→ hold・削除ゼロ
        for rows, head in ([[row("4", "5"), row("9", "6")], "5"],
                           [[row("4", ""), row("9", "5")], "5"],
                           [[row("4", "5"), row("9", "5")], "7"]):
            with self.subTest(head=head):
                self.assertEqual(
                    hp.classify_duplicate_rows(rows, head)["action"], "hold")


# ── 一本経路（write 0）のソース検査（正本 §3.2/§4B と逐語整合の固定）─────────
class TestSingleWritePathSource(unittest.TestCase):
    def test_machine_rederivation_has_no_app36_reference(self):
        # 機械再導出（heir_derive_task）は App36 へ write 0＝参照自体ゼロ（fix3 H01）
        from pathlib import Path
        src = Path("dispatch_bot/heir_derive_task.py").read_text(encoding="utf-8")
        self.assertNotIn("APP_SOUZOKUNIN", src)
        self.assertNotIn("App 36", src)
        self.assertNotIn("heir_projection", src)

    def test_app36_writes_only_inside_confirmed_handler(self):
        # hub/heir_projection の kintone write（create/update）は確定関所
        # _resolve_heir_derivation の配下のみ（一本経路の機械検査）
        import ast
        from pathlib import Path
        tree = ast.parse(Path("hub/heir_projection.py").read_text(encoding="utf-8"))
        owners = []
        for top in tree.body:
            if isinstance(top, (ast.AsyncFunctionDef, ast.FunctionDef)):
                for node in ast.walk(top):
                    if isinstance(node, ast.Call) \
                            and isinstance(node.func, ast.Attribute) \
                            and node.func.attr in ("create_record",
                                                   "update_record"):
                        owners.append(top.name)
        self.assertTrue(owners)
        self.assertEqual(set(owners), {"_resolve_heir_derivation"})


# ── 関所ゲート（phase 1・すべて write 0 で中止）─────────────────────────────
class TestGatePhase(_ProjBase):
    def test_attorney_allowlist_rejects_outsider(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        r = self._resolve(_group(rid), decided_by="OUTSIDER")
        self.assertEqual(r["status"], "aborted")
        self.assertIn("確定権限がありません", r["reason"])
        self.assert_write_zero()

    def test_double_confirmation_guard(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        from hub.derivation_models import create_heir_decision
        _run(create_heir_decision(derivation_run_id=rid, decision="confirmed",
                                  decided_by="ATT1",
                                  decided_at=datetime.now(timezone.utc)))
        db.reset_for_tests()
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("確定済み", r["reason"])
        self.create.assert_not_awaited()
        self.update.assert_not_awaited()

    def test_stale_run_rejected(self):
        a = self._mk_run(_payload(_heir("11", "spouse")), ih="a" * 64)
        b = self._mk_run(_payload(_heir("11", "spouse")), supersedes=a,
                         ih="b" * 64)
        self.assertGreater(b, a)
        r = self._resolve(_group(a))                 # 旧 run の封筒から確定
        self.assertEqual(r["status"], "aborted")
        self.assertIn("新しい導出", r["reason"])
        self.assert_write_zero()

    def test_legacy_payload_without_codes_rejected(self):
        rid = self._mk_run(_payload(
            {"person_id": "11", "share": "1/2", "relation_key": "spouse"}))
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("旧形式", r["reason"])
        self.assert_write_zero()

    def test_fetus_stops_whole_case(self):
        # 裁定3/§2A（[人]明示承認済み）: 1行でも胎児を含めば全体停止・部分反映なし
        rid = self._mk_run(_payload(
            _heir("11", "spouse"),
            _heir("胎児:F1", "fetus", share=None, relation="fetus")))
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("胎児", r["reason"])
        self.assertIn("胎児行 1 件", r["reason"])
        self.assert_write_zero()                       # decision も App36 も 0
        self.alert.assert_awaited()                    # 業務警報（件数のみ）
        alert_text = str(self.alert.await_args)
        self.assertIn("胎児行 1 件", alert_text)

    def test_envelope_no_longer_pending_aborts(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        self.envelope = {"発送ステータス": {"value": "完了"},
                         "実行済み": {"value": "yes"}}
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("要確認ではなくなっています", r["reason"])
        self.assert_write_zero()

    def test_bad_run_id_and_case_mismatch(self):
        r = self._resolve(_group("012"))               # 前ゼロ（grammar 外）
        self.assertEqual(r["status"], "aborted")
        self.assertIn("derivation_run_id が不正", r["reason"])
        rid = self._mk_run(_payload(_heir("11", "spouse")), case="8")
        r2 = self._resolve(_group(rid))                # 案件不一致
        self.assertEqual(r2["status"], "aborted")
        self.assertIn("確定対象ではありません", r2["reason"])
        self.assert_write_zero()

    def test_db_unreachable_during_ancestry_check(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        with patch.object(hp, "_ancestor_ids",
                          new=AsyncMock(side_effect=RuntimeError("db down"))):
            r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("判定不能", r["reason"])
        self.assert_write_zero()


# ── projection E2E（phase 2/3・§4A 書込み表・§5 1件一致状態表）───────────────
class TestProjectionE2E(_ProjBase):
    def test_insert_path_full_fields(self):
        rid = self._mk_run(_payload(
        _heir("11", "spouse", share="1/2", relation="spouse"),
            _heir("12", "grandchild_rep", share="1/4",
                  relation="representative")))
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(r["items"][0]["app36_inserted"], 2)
        # decision=confirmed が 1 行（decided_by=ATT1）
        rows = self._decisions()
        self.assertEqual([(x.decision, x.decided_by) for x in rows],
                         [("confirmed", "ATT1")])
        # App36 insert fields（§4A 表）
        by_pid = {f["導出元人物ID"]: f for label, f in self.created
                  if label == "App 36 (相続人)"}
        f11 = by_pid["11"]
        self.assertEqual(f11["続柄"], "配偶者")
        self.assertEqual(f11["法定相続分"], "2分の1")
        self.assertEqual(f11["データ源"], "戸籍読解")
        self.assertEqual(f11["current_derivation_run_id"], str(rid))
        self.assertEqual(f11["戸籍確認済"], "yes")     # confirmed handler は設定可
        self.assertEqual(f11["案件レコードID"], "9")
        self.assertEqual(f11["ユニット種別"], "相続一般")
        self.assertEqual(by_pid["12"]["続柄"], "孫（代襲）")   # 裁定2 追加値へ写像
        # human_state・PII 列は書かない（§4A 保護）
        for f in by_pid.values():
            for banned in ("状態", "氏名", "住所", "生年月日", "本籍",
                           "連絡先", "印鑑証明"):
                self.assertNotIn(banned, f)
        # 封筒クローズ
        self.assertIn(("App 30 (発送管理)", "70",
                       {"発送ステータス": "完了", "実行済み": "yes"}), self.updated)

    def test_share_none_writes_no_share_field(self):
        rid = self._mk_run(_payload(_heir("11", "spouse", share=None)))
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        _label, fields = self.created[0]
        self.assertNotIn("法定相続分", fields)          # 空欄＝機械は書かない

    def test_same_run_idempotent_update_and_yes_protection(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        self.app36_rows["11"] = [{
            "$id": {"value": "301"},
            "current_derivation_run_id": {"value": str(rid)},
            "戸籍確認済": {"value": "yes"}}]
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        self.create.assert_not_awaited()               # insert しない（冪等ヒット）
        app36_updates = [(rid_, f) for label, rid_, f in self.updated
                         if label == "App 36 (相続人)"]
        self.assertEqual(len(app36_updates), 1)
        rid_, fields = app36_updates[0]
        self.assertEqual(rid_, "301")
        self.assertNotIn("戸籍確認済", fields)          # yes→no を起こさない（含めない）
        self.assertEqual(fields["current_derivation_run_id"], str(rid))

    def test_no_to_yes_only_by_confirmed_handler(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        self.app36_rows["11"] = [{
            "$id": {"value": "301"},
            "current_derivation_run_id": {"value": str(rid)},
            "戸籍確認済": {"value": "no"}}]
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        _rid, fields = [(x, f) for label, x, f in self.updated
                        if label == "App 36 (相続人)"][0]
        self.assertEqual(fields["戸籍確認済"], "yes")   # no→yes（§3.4/§4A）

    def test_ancestor_current_advances_h10(self):
        a = self._mk_run(_payload(_heir("11", "spouse")), ih="a" * 64)
        b = self._mk_run(_payload(_heir("11", "child", relation="child")),
                         supersedes=a, ih="b" * 64)
        self.app36_rows["11"] = [{
            "$id": {"value": "301"},
            "current_derivation_run_id": {"value": str(a)},   # 祖先を指す
            "戸籍確認済": {"value": "no"}}]
        r = self._resolve(_group(b))
        self.assertEqual(r["status"], "resolved")
        _rid, fields = [(x, f) for label, x, f in self.updated
                        if label == "App 36 (相続人)"][0]
        self.assertEqual(fields["current_derivation_run_id"], str(b))  # 前進
        self.assertEqual(fields["続柄"], "子")

    def test_current_empty_or_invalid_aborts(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        for cur in ("", "abc", "0", "9999999999999999999"):
            with self.subTest(cur=cur):
                self.app36_rows["11"] = [{
                    "$id": {"value": "301"},
                    "current_derivation_run_id": {"value": cur},
                    "戸籍確認済": {"value": "no"}}]
                self.created.clear()
                self.updated.clear()
                r = self._resolve(_group(rid))
                self.assertEqual(r["status"], "aborted")
                self.assertIn("current が空または不正", r["reason"])
                self.create.assert_not_awaited()
                self.assertEqual(
                    [u for u in self.updated if u[0] == "App 36 (相続人)"], [])
                # 二重確定ガード検査用: decision も書かれていない
                self.assertEqual(self._decisions(), [])

    def test_unrelated_run_aborts(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        self.app36_rows["11"] = [{
            "$id": {"value": "301"},
            "current_derivation_run_id": {"value": "999999"},   # 別系列
            "戸籍確認済": {"value": "no"}}]
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("別系列", r["reason"])
        self.assert_write_zero()
        self.alert.assert_awaited()

    def test_duplicate_rows_abort_with_classification(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        self.app36_rows["11"] = [
            {"$id": {"value": "305"},
             "current_derivation_run_id": {"value": str(rid)},
             "戸籍確認済": {"value": "no"}},
            {"$id": {"value": "302"},
             "current_derivation_run_id": {"value": str(rid)},
             "戸籍確認済": {"value": "no"}}]
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("重複行", r["reason"])
        self.assertIn("収束分類=tiebreak", r["reason"])   # 同一 head 限定 tiebreak
        self.assert_write_zero()                          # 機械は削除も書込みもゼロ
        alert_text = str(self.alert.await_args)
        self.assertIn("冪等キー重複", alert_text)

    def test_multi_row_anomaly_prevents_partial_projection(self):
        # 1行目は正常・2行目が別系列 → 全体中止（部分反映しない・phase 1 で検証）
        rid = self._mk_run(_payload(
            _heir("11", "spouse"),
            _heir("12", "child", relation="child")))
        self.app36_rows["12"] = [{
            "$id": {"value": "310"},
            "current_derivation_run_id": {"value": "999999"},
            "戸籍確認済": {"value": "no"}}]
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "aborted")
        self.assert_write_zero()                          # 11 の insert も起きない


# ── PII・続柄値の非露出（要件5・sentinel 方式）───────────────────────────────
class TestNonExposure(_ProjBase):
    def test_reason_logs_alerts_carry_no_zokugara_values_or_names(self):
        rid = self._mk_run(_payload(
            _heir("11", "grandchild_rep", relation="representative"),
            _heir("胎児:F1", "fetus", share=None, relation="fetus")))
        item = ReviewItem(record_id="70", subject=SENTINEL_NAME,
                          detail={"derivation_run_id": rid,
                                  "case_record_id": "9", "冪等キー": "k"})
        group = ReviewGroup(source="heir_derivation", idempotency_key="k",
                            items=[item])
        with self.assertLogs("hub.heir_projection", level="INFO") as cap:
            import logging
            logging.getLogger("hub.heir_projection").info("arm capture")
            r = self._resolve(group)
        surfaces = [str(r), "\n".join(cap.output),
                    "\n".join(str(c) for c in self.alert.await_args_list)]
        for s in surfaces:
            self.assertNotIn(SENTINEL_NAME, s)
            for zoku in APP36_ZOKUGARA_VALUES:
                self.assertNotIn(zoku, s)                 # 続柄値の非反射
            self.assertNotIn("胎児:F1", s)                # 合成 ID も出さない

    def test_resolved_result_carries_counts_and_ids_only(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        r = self._resolve(_group(rid))
        s = str(r)
        for zoku in APP36_ZOKUGARA_VALUES:
            self.assertNotIn(zoku, s)


# ── RESOLVERS 結線・decided_by 伝搬（既存互換）───────────────────────────────
class TestResolverWiring(_ProjBase):
    def test_registered_with_required_apps(self):
        handler, apps = RESOLVERS["heir_derivation"]
        self.assertIs(handler, hp._resolve_heir_derivation)
        self.assertIn(hp.APP_SOUZOKUNIN, apps)

    def test_resolve_group_passes_decided_by_capability_based(self):
        calls = {}

        async def with_db(group, case_record_id, decided_by=""):
            calls["with"] = decided_by
            return {"status": "resolved"}

        async def without_db(group, case_record_id):
            calls["without"] = True
            return {"status": "resolved"}

        g = ReviewGroup(source="_t_with", idempotency_key="k", items=[])
        RESOLVERS["_t_with"] = (with_db, ())
        RESOLVERS["_t_without"] = (without_db, ())
        try:
            _run(resolve_group(g, "9", decided_by="ATT1"))
            g2 = ReviewGroup(source="_t_without", idempotency_key="k", items=[])
            _run(resolve_group(g2, "9", decided_by="ATT1"))
        finally:
            del RESOLVERS["_t_with"]
            del RESOLVERS["_t_without"]
        self.assertEqual(calls, {"with": "ATT1", "without": True})

    def test_unknown_key_still_unsupported(self):
        g = ReviewGroup(source="ghost", idempotency_key="k", items=[])
        r = _run(resolve_group(g, "9", decided_by="ATT1"))
        self.assertEqual(r["status"], "unsupported")

    def test_task_layer_passes_pending_user(self):
        from types import SimpleNamespace

        from dispatch_bot import review_resolve_task as rt
        captured = {}

        async def fake_resolve(group, case_id, decided_by=""):
            captured["decided_by"] = decided_by
            return {"status": "aborted", "reason": "x"}

        pending = SimpleNamespace(
            user_id="ATT1",
            parsed={"task_params": {"group_source": "heir_derivation",
                                    "group_idem": "k", "group_items": [],
                                    "case_record_id": "9"}})
        with patch.object(rt, "resolve_group", new=fake_resolve):
            _run(rt.execute(pending))
        self.assertEqual(captured["decided_by"], "ATT1")


if __name__ == "__main__":
    unittest.main()
