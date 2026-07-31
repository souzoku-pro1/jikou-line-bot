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
            # fix2: App36 を stateful に模擬（再開経路の phase1/3 再検索が
            # 直前 write を観測できるようにする）
            self.created.append((app.label, dict(fields)))
            rid = str(900 + len(self.created))
            if app.label == "App 36 (相続人)":
                self.app36_rows.setdefault(fields["導出元人物ID"], []).append({
                    "$id": {"value": rid},
                    "current_derivation_run_id":
                        {"value": fields.get("current_derivation_run_id", "")},
                    "戸籍確認済": {"value": fields.get("戸籍確認済", "no")},
                    "$revision": {"value": "1"}})
            return rid

        async def fake_update(app, rid, fields, revision=None):
            self.updated.append((app.label, str(rid), dict(fields)))
            if app.label == "App 36 (相続人)":
                for rows in self.app36_rows.values():
                    for row in rows:
                        if row.get("$id", {}).get("value") == str(rid):
                            for k, v in fields.items():
                                if k in ("current_derivation_run_id",
                                         "戸籍確認済"):
                                    row[k] = {"value": v}
                            rev = row.setdefault("$revision", {"value": "0"})
                            rev["value"] = str(int(rev["value"] or "0") + 1)

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
        # _resolve_heir_derivation とその行ヘルパ _project_row の配下のみ
        # （fix1: 直前再検証の導入で行 write は _project_row へ分離。呼出し元は
        # _resolve_heir_derivation のみであることも同時に pin＝一本経路は不変）
        import ast
        from pathlib import Path
        tree = ast.parse(Path("hub/heir_projection.py").read_text(encoding="utf-8"))
        owners = []
        project_row_callers = []
        for top in tree.body:
            if isinstance(top, (ast.AsyncFunctionDef, ast.FunctionDef)):
                for node in ast.walk(top):
                    if isinstance(node, ast.Call) \
                            and isinstance(node.func, ast.Attribute) \
                            and node.func.attr in ("create_record",
                                                   "update_record"):
                        owners.append(top.name)
                    if isinstance(node, ast.Name) and node.id == "_project_row" \
                            and top.name != "_project_row":
                        project_row_callers.append(top.name)
        self.assertTrue(owners)
        self.assertEqual(set(owners),
                         {"_resolve_heir_derivation", "_project_row"})
        self.assertEqual(set(project_row_callers), {"_resolve_heir_derivation"})

    def test_repo_wide_app36_write_allowed_contexts(self):
        """L01（R-P3-003B-IMPL-1）: 許可文脈閉集合——repo 全 tracked .py を走査し、
        APP_SOUZOKUNIN（別名束縛・attribute 参照・KintoneApp("App 36…") 再構築を
        含む）を引数に取る kintone write 呼出しが、hub/heir_projection の確定関所
        配下（_resolve_heir_derivation／_project_row）以外に存在しないことを pin。
        test_*.py は走査除外（mock 束縛のみ・sink AST policy の scan_repo と同じ
        除外基準）。動的構築（env 名の文字列を実行時に組む経路）は静的検査の
        原理的限界＝対象外（ast_policy_helpers と同じ明記方針）。"""
        import ast
        import subprocess
        from pathlib import Path

        write_attrs = {"create_record", "update_record", "delete_record",
                       "create_records"}
        allowed = {("hub/heir_projection.py", "_resolve_heir_derivation"),
                   ("hub/heir_projection.py", "_project_row")}
        files = subprocess.run(["git", "ls-files", "*.py"],
                               capture_output=True, text=True,
                               check=True).stdout.splitlines()
        violations = []
        for f in files:
            name = Path(f).name
            if name.startswith("test_"):
                continue
            tree = ast.parse(Path(f).read_text(encoding="utf-8"), filename=f)
            # ── 束縛収集: APP_SOUZOKUNIN の別名（import asname／代入）＋
            #    KintoneApp 再構築（"APP_SOUZOKUNIN"/"App 36" リテラル）──
            bound = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    for a in node.names:
                        if a.name == "APP_SOUZOKUNIN":
                            bound.add(a.asname or a.name)
                elif isinstance(node, ast.Assign):
                    v = node.value
                    src = (
                        (isinstance(v, ast.Name)
                         and (v.id == "APP_SOUZOKUNIN" or v.id in bound))
                        or (isinstance(v, ast.Attribute)
                            and v.attr == "APP_SOUZOKUNIN")
                        or (isinstance(v, ast.Call) and any(
                            isinstance(a, ast.Constant)
                            and isinstance(a.value, str)
                            and ("APP_SOUZOKUNIN" in a.value
                                 or "App 36" in a.value)
                            for a in v.args)))
                    if src:
                        for t in node.targets:
                            if isinstance(t, ast.Name):
                                bound.add(t.id)
            bound.add("APP_SOUZOKUNIN")   # 素の名前・attribute 形は常時対象

            def _is_app36_arg(arg) -> bool:
                if isinstance(arg, ast.Name) and arg.id in bound:
                    return True
                return (isinstance(arg, ast.Attribute)
                        and arg.attr == "APP_SOUZOKUNIN")

            for top in tree.body:
                in_func = isinstance(top, (ast.FunctionDef,
                                           ast.AsyncFunctionDef))
                owner = top.name if in_func else "<module>"
                for node in ast.walk(top):
                    if not isinstance(node, ast.Call) or not node.args:
                        continue
                    fn = node.func
                    attr = fn.attr if isinstance(fn, ast.Attribute) else (
                        fn.id if isinstance(fn, ast.Name) else "")
                    if attr in write_attrs and _is_app36_arg(node.args[0]):
                        if (f, owner) not in allowed:
                            violations.append(f"{f}:{node.lineno}:{owner}")
        self.assertEqual(violations, [],
                         "APP_SOUZOKUNIN への write が確定関所配下以外に存在:\n"
                         + "\n".join(violations))


# ── 関所ゲート（phase 1・すべて write 0 で中止）─────────────────────────────
class TestGatePhase(_ProjBase):
    def test_attorney_allowlist_rejects_outsider(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        r = self._resolve(_group(rid), decided_by="OUTSIDER")
        self.assertEqual(r["status"], "aborted")
        self.assertIn("確定権限がありません", r["reason"])
        self.assert_write_zero()

    def test_existing_decision_resumes_projection(self):
        """fix2 M02（設計改定 §9-v2・発注要件2に伴う期待値の再定義＝緩和ではない）:
        初版の「root decision 既存＝二重確定で中止」を撤回し、run が head かつ
        封筒未クローズなら decision 作成をスキップして projection のみ再実行する
        正規の再開経路とする。新しい root decision は重複作成しない。"""
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        from hub.derivation_models import create_heir_decision
        _run(create_heir_decision(derivation_run_id=rid, decision="confirmed",
                                  decided_by="ATT1",
                                  decided_at=datetime.now(timezone.utc)))
        db.reset_for_tests()
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")      # 中止しない（再開）
        self.assertEqual(len(self._decisions()), 1)    # decision 重複作成なし
        self.assertEqual(r["items"][0]["app36_inserted"], 1)   # 残り行を反映
        self.assertIn(("App 30 (発送管理)", "70",
                       {"発送ステータス": "完了", "実行済み": "yes"}),
                      self.updated)                    # held 0 → クローズ

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


# ── fix1 H01: CAS・書込み直前再検証（R-P3-003B-IMPL-1）─────────────────────
class TestCasAndPreWriteReverification(_ProjBase):
    def test_supersede_between_phase1_and_decision_aborts_all(self):
        """(a) phase 1 通過後に head が supersede された場合、CAS（同一 txn の
        head 再検証）が検出し decision 含む write 0 で全体中止。"""
        a = self._mk_run(_payload(_heir("11", "spouse")), ih="a" * 64)
        b = self._mk_run(_payload(_heir("11", "spouse")), supersedes=a,
                         ih="b" * 64)
        # 注入: phase 1 の stale ガードには「a が head」と見せる（凍結窓の再現）。
        # CAS は実 DB を見る（head=b）ため不一致を検出する
        stale_head = type("H", (), {"id": a})()
        with patch.object(hp, "get_current_head",
                          new=AsyncMock(return_value=stale_head)):
            r = self._resolve(_group(a))
        self.assertEqual(r["status"], "aborted")
        self.assertIn("前提が変化", r["reason"])
        self.assert_write_zero()                   # decision も App36 も 0

    def test_prewrite_recheck_prevents_blind_insert(self):
        """(b) phase 1 で 0 件だった冪等キーが write 直前再検索で 1 件出現
        → 盲目 insert しない（当該行 held）。fix2 M02（設計改定 §9-v2 への追随・
        緩和ではない）: held>0 は封筒をクローズせず要確認のまま残し、detail へ
        保留人物 record ID を追記する。"""
        import json as _json
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        race_row = {"$id": {"value": "301"},
                    "current_derivation_run_id": {"value": "999999"},
                    "戸籍確認済": {"value": "no"},
                    "$revision": {"value": "5"}}
        calls = {"n": 0}

        async def racing_search(app, query, fields=None):
            import re as _re
            if _re.search(r'導出元人物ID = "11"', query):
                calls["n"] += 1
                return [] if calls["n"] == 1 else [race_row]   # 2回目=出現
            return []
        self.search.side_effect = racing_search
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(r["items"][0]["app36_inserted"], 0)
        self.assertEqual(r["items"][0]["app36_held"], 1)
        self.assertIs(r["items"][0]["envelope_closed"], False)
        self.create.assert_not_awaited()           # 盲目 insert なし
        self.alert.assert_awaited()
        # 封筒はクローズしない（fix2 M02: 耐久可視性は App30 キュー）
        closes = [u for u in self.updated
                  if u[0] == "App 30 (発送管理)" and u[2].get("実行済み") == "yes"]
        self.assertEqual(closes, [])
        # detail へ保留人物 ID（数字のみ）を追記
        detail_writes = [u for u in self.updated
                         if u[0] == "App 30 (発送管理)"
                         and "チャネル固有データ" in u[2]]
        self.assertEqual(len(detail_writes), 1)
        data = _json.loads(detail_writes[0][2]["チャネル固有データ"])
        self.assertEqual(data["heir_derivation"]["保留人物ID"], ["11"])

    def test_h10_update_revision_conflict_is_held(self):
        """(c) H10 update の revision 楽観ロック競合 → 当該行 held。
        fix2 M02（設計改定 §9-v2 への追随・緩和ではない）: 封筒はクローズしない。"""
        from hub.kintone import KintoneConflict
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        self.app36_rows["11"] = [{
            "$id": {"value": "301"},
            "current_derivation_run_id": {"value": str(rid)},
            "戸籍確認済": {"value": "no"},
            "$revision": {"value": "7"}}]

        async def conflicting_update(app, rid_, fields, revision=None):
            if app.label == "App 36 (相続人)":
                self.assertEqual(revision, "7")    # 楽観ロックが実伝搬している
                raise KintoneConflict(409, "GAIA_CO02", "conflict")
            self.updated.append((app.label, str(rid_), dict(fields)))
        self.update.side_effect = conflicting_update
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(r["items"][0]["app36_updated"], 0)
        self.assertEqual(r["items"][0]["app36_held"], 1)
        self.assertIs(r["items"][0]["envelope_closed"], False)
        closes = [u for u in self.updated
                  if u[0] == "App 30 (発送管理)" and u[2].get("実行済み") == "yes"]
        self.assertEqual(closes, [])               # クローズしない（fix2 M02）
        self.alert.assert_awaited()

    def test_happy_update_passes_revision(self):
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        self.app36_rows["11"] = [{
            "$id": {"value": "301"},
            "current_derivation_run_id": {"value": str(rid)},
            "戸籍確認済": {"value": "no"},
            "$revision": {"value": "9"}}]
        captured = {}

        async def capturing_update(app, rid_, fields, revision=None):
            if app.label == "App 36 (相続人)":
                captured["revision"] = revision
            self.updated.append((app.label, str(rid_), dict(fields)))
        self.update.side_effect = capturing_update
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(captured["revision"], "9")   # 再読 revision を使用


# ── fix2 H01-R2/M02: グループ原子化・再開可能 projection（R-P3-003B-IMPL-2）──
class TestGroupAtomicityAndResume(_ProjBase):
    def test_multi_item_same_run_single_decision(self):
        """(a) 複数 item が同一 run を参照 → decision は 1 件に重複排除・
        原子的成功・全封筒が同一結果（クローズ）で扱われる。"""
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        g = ReviewGroup(source="heir_derivation", idempotency_key="k",
                        items=[_item(rid, "70"), _item(rid, "71")])
        r = self._resolve(g)
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(len(self._decisions()), 1)     # 重複排除
        closes = {u[1] for u in self.updated
                  if u[0] == "App 30 (発送管理)" and u[2].get("実行済み") == "yes"}
        self.assertEqual(closes, {"70", "71"})          # 両封筒クローズ
        # item1 は insert・item2 は same-run ヒットの冪等 update（新規行を作らない）
        self.assertEqual(r["items"][0]["app36_inserted"], 1)
        self.assertEqual(r["items"][1]["app36_inserted"], 0)
        self.assertEqual(r["items"][1]["app36_updated"], 1)
        self.assertEqual(len(self.created), 1)

    def test_group_cas_failure_rolls_back_all_decisions(self):
        """(b) グループ途中の CAS 失敗 → 単一 txn 全体 rollback＝decision 0 件。
        （head である b の INSERT 後に a の CAS が失敗する順で直接検証）"""
        from hub.derivation_models import (ChainIntegrityError,
                                           create_confirmed_decisions_for_heads)
        a = self._mk_run(_payload(_heir("11", "spouse")), ih="a" * 64)
        b = self._mk_run(_payload(_heir("11", "spouse")), supersedes=a,
                         ih="b" * 64)
        with self.assertRaises(ChainIntegrityError):
            _run(create_confirmed_decisions_for_heads(
                "9", [b, a], decided_by="ATT1",
                decided_at=datetime.now(timezone.utc)))
        db.reset_for_tests()
        self.assertEqual(self._decisions(), [])         # b の分も rollback 済み

    def test_held_then_manual_fix_then_reconfirm_projects_rest(self):
        """(c) held 発生 → 封筒 open 維持＋保留人物ID 追記 → 人手収束を模擬 →
        同一封筒の再確定（再開経路）→ 残り行反映 → クローズ。"""
        import json as _json
        rid = self._mk_run(_payload(
            _heir("11", "spouse"),
            _heir("12", "child", relation="child")))
        calls = {"12": 0}

        async def racing(app, query, fields=None):
            import re as _re
            m = _re.search(r'導出元人物ID = "([0-9]+)"', query)
            pid = m.group(1) if m else ""
            if pid == "12":
                calls["12"] += 1
                if calls["12"] == 2:   # 1回目確定の write 直前に競合行が出現
                    self.app36_rows.setdefault("12", []).append({
                        "$id": {"value": "500"},
                        "current_derivation_run_id": {"value": "999999"},
                        "戸籍確認済": {"value": "no"},
                        "$revision": {"value": "3"}})
            return list(self.app36_rows.get(pid, []))
        self.search.side_effect = racing

        r1 = self._resolve(_group(rid))
        self.assertEqual(r1["status"], "resolved")
        self.assertEqual(r1["items"][0]["app36_inserted"], 1)   # 11 は反映
        self.assertEqual(r1["items"][0]["app36_held"], 1)       # 12 は保留
        self.assertIs(r1["items"][0]["envelope_closed"], False)
        detail_writes = [u for u in self.updated
                         if u[0] == "App 30 (発送管理)"
                         and "チャネル固有データ" in u[2]]
        data = _json.loads(detail_writes[0][2]["チャネル固有データ"])
        self.assertEqual(data["heir_derivation"]["保留人物ID"], ["12"])
        self.assertEqual(len(self._decisions()), 1)

        # 人手収束を模擬: 競合行の current を head run へ揃える（App36 行修正）
        self.app36_rows["12"][0]["current_derivation_run_id"]["value"] = str(rid)
        self.updated.clear()
        r2 = self._resolve(_group(rid))                 # 同一封筒を再確定
        self.assertEqual(r2["status"], "resolved")
        self.assertEqual(r2["items"][0]["app36_held"], 0)
        self.assertIs(r2["items"][0]["envelope_closed"], True)
        self.assertEqual(len(self._decisions()), 1)     # decision は増えない
        app36_updates = {u[1] for u in self.updated
                         if u[0] == "App 36 (相続人)"}
        self.assertIn("500", app36_updates)             # 残り行（12）が反映された
        self.assertIn(("App 30 (発送管理)", "70",
                       {"発送ステータス": "完了", "実行済み": "yes"}), self.updated)

    def test_duplicate_envelope_reconfirm_converges(self):
        """(d) 同一 run の重複封筒を後から確定 → 再開経路で冪等に走り
        （same-run ヒット・新規行を作らない）クローズされて収束。"""
        rid = self._mk_run(_payload(_heir("11", "spouse")))
        r1 = self._resolve(_group(rid, "70"))
        self.assertIs(r1["items"][0]["envelope_closed"], True)
        self.updated.clear()
        r2 = self._resolve(_group(rid, "71"))           # TOCTOU 由来の重複封筒
        self.assertEqual(r2["status"], "resolved")
        self.assertEqual(r2["items"][0]["app36_inserted"], 0)   # 新規行なし
        self.assertEqual(r2["items"][0]["app36_held"], 0)
        self.assertIs(r2["items"][0]["envelope_closed"], True)  # 収束
        self.assertEqual(len(self._decisions()), 1)
        self.assertEqual(len(self.created), 1)          # insert は初回の 1 件のみ

    def test_resume_rejects_stale_after_supersede(self):
        """(e) 再開経路も stale run（head 交代後）は拒否する。"""
        a = self._mk_run(_payload(_heir("11", "spouse")), ih="a" * 64)
        r1 = self._resolve(_group(a, "70"))             # a を確定（decision 記録）
        self.assertEqual(r1["status"], "resolved")
        b = self._mk_run(_payload(_heir("11", "spouse")), supersedes=a,
                         ih="b" * 64)
        self.assertGreater(b, a)
        r2 = self._resolve(_group(a, "71"))             # 旧 run の封筒を再確定
        self.assertEqual(r2["status"], "aborted")
        self.assertIn("新しい導出", r2["reason"])
        self.assertEqual(len(self._decisions()), 1)     # 追加 decision なし


# ── fix1 M01: heir 成功応答の整形（review_resolve_task E2E）─────────────────
class TestHeirResultFormatting(unittest.TestCase):
    def test_success_reply_shows_app36_counts_not_zaisan(self):
        from types import SimpleNamespace

        from dispatch_bot import review_resolve_task as rt

        async def fake_resolve(group, case_id, decided_by=""):
            return {"status": "resolved", "case_record_id": "9",
                    "items": [{"review_record_id": "70",
                               "derivation_run_id": 31,
                               "app36_inserted": 2, "app36_updated": 1,
                               "app36_held": 0}]}

        pending = SimpleNamespace(
            user_id="ATT1",
            parsed={"task_params": {"group_source": "heir_derivation",
                                    "group_idem": "k", "group_items": [],
                                    "case_record_id": "9",
                                    "folder_name": "F9"}})
        with patch.object(rt, "resolve_group", new=fake_resolve):
            msg, _rid, _url = asyncio.run(rt.execute(pending))
        self.assertIn("相続人反映(App36) 新規2件・更新1件（run #31）", msg)
        self.assertNotIn("財産行", msg)                # 誤フォールバック解消
        self.assertNotIn("No.None", msg)

    def test_success_reply_shows_held_count(self):
        from types import SimpleNamespace

        from dispatch_bot import review_resolve_task as rt

        async def fake_resolve(group, case_id, decided_by=""):
            return {"status": "resolved", "case_record_id": "9",
                    "items": [{"review_record_id": "70",
                               "derivation_run_id": 31,
                               "app36_inserted": 1, "app36_updated": 0,
                               "app36_held": 2}]}

        pending = SimpleNamespace(
            user_id="ATT1",
            parsed={"task_params": {"group_source": "heir_derivation",
                                    "group_idem": "k", "group_items": [],
                                    "case_record_id": "9"}})
        with patch.object(rt, "resolve_group", new=fake_resolve):
            msg, _rid, _url = asyncio.run(rt.execute(pending))
        # fix2 M02（設計改定 §9-v2 への追随・緩和ではない）: 再開経路の案内文言
        self.assertIn("要確認2件。収束後に同じ封筒を再確定すると残り行を再反映します",
                      msg)


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
