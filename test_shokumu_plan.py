"""SHOKUMU-PLAN 実装票のテスト（正本 DRAFT_SHOKUMU_PLAN.md FROZEN・§6 対照 1〜49）。

各テストの docstring 冒頭に対応する §6 系統番号を明記する（§6→テストの写像は
完了報告の対応表が正・1 テストが複数系統を覆う場合は全番号を列挙）。
kintone は全て mock（実機・ネットワーク非依存）・DB は sqlite（P3 系流儀）。
"""

import ast
import asyncio
import json
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import hub.db as db  # noqa: E402
import hub.shokumu_plan as sp  # noqa: E402
from hub.derivation_models import (DerivationBase,  # noqa: E402
                                   create_decisions_for_heads,
                                   create_derivation_run)
from review_resolve import RESOLVERS, ReviewGroup, ReviewItem  # noqa: E402

_ENV = {
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "t26",
    "KINTONE_APP_ID": "21",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
    "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
    "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33",
    "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "t36",
    "APP_CITY_MASTER": "31", "TOKEN_CITY_MASTER": "t31",
    "SHOKUMU_PLAN_ENABLED": "1",
}


def _run(coro):
    return asyncio.run(coro)


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


def _person(rid, name, *, addr="", honseki="", death="", decedent="no",
            father="", mother="", birth=""):
    events = []
    if birth:
        events = [{"value": {"事項種別": {"value": "出生"},
                             "年月日": {"value": birth}}}]
    return {"$id": {"value": rid}, "氏名": {"value": name},
            "住所最新": {"value": addr}, "本籍最新": {"value": honseki},
            "死亡日": {"value": death}, "被相続人フラグ": {"value": decedent},
            "父人物ID": {"value": father}, "母人物ID": {"value": mother},
            "身分事項": {"value": events}}


class _Base(unittest.TestCase):
    """sqlite 実 DB＋App30/31/33/34/36/案件の stateful kintone mock。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="spl_")
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

        # 標準ケース: 被相続人10（死亡）＋子11＝rank1
        self.persons = [
            _person("10", "被相続人太郎", addr="埼玉県川口市大字X 1-2",
                    honseki="埼玉県川口市大字Y 3", death="2026-01-01",
                    decedent="yes", birth="昭和20年1月1日"),
            _person("11", "長男一郎", father="10", birth="昭和45年2月2日"),
        ]
        self.kosekis: list[dict] = []
        self.city_master = {"川口市": "310"}       # 名称→App31 record id
        self.app36: list[dict] = []
        self.app30: list[dict] = []                # 起票済みレコード（stateful）
        self.created = []                          # (label, fields)
        self.updated = []                          # (label, rid, fields)
        self._next_id = 700

        async def fake_search(app, query, fields=None):
            label = app.label
            if label == sp.APP_KOSEKI_PERSON.label:
                return list(self.persons)
            if label == sp.APP_KOSEKI_BOOK.label:
                return list(self.kosekis)
            if label == "App 36 (相続人)":
                return list(self.app36)
            if "市区町村マスタ" in label or "App 31" in label:
                m = re.search(r'市区町村名 = "([^"]+)"', query)
                name = m.group(1) if m else ""
                rid = self.city_master.get(name)
                return ([{"$id": {"value": rid},
                          "市区町村名": {"value": name}}] if rid else [])
            if label == sp.APP_SHIPPING.label:
                if "チャネル固有データ like" in query:
                    m = re.search(r'like "([^"]+)"', query)
                    frag = m.group(1) if m else ""
                    return [r for r in self.app30
                            if frag in str((r.get("チャネル固有データ") or {})
                                           .get("value") or "")]
                if 'チャネル in ("職務上請求")' in query:
                    return [r for r in self.app30
                            if (r.get("チャネル") or {}).get("value")
                            == "職務上請求"]
                return list(self.app30)
            raise AssertionError(f"unexpected search: {label}")

        async def fake_get(app, rid):
            if app.label == sp.APP_SHIPPING.label:
                for r in self.app30:
                    if (r.get("$id") or {}).get("value") == str(rid):
                        return r
                raise AssertionError(f"no app30 record {rid}")
            if "相談カード" in app.label:
                return _rec(顧客名="山田太郎")
            raise AssertionError(f"unexpected get: {app.label}")

        async def fake_create(app, fields):
            self.created.append((app.label, dict(fields)))
            self._next_id += 1
            rid = str(self._next_id)
            if app.label == sp.APP_SHIPPING.label:
                rec = {k: {"value": v} for k, v in fields.items()}
                rec["$id"] = {"value": rid}
                self.app30.append(rec)
            return rid

        async def fake_update(app, rid, fields, revision=None):
            self.updated.append((app.label, str(rid), dict(fields)))
            for r in self.app30:
                if (r.get("$id") or {}).get("value") == str(rid):
                    for k, v in fields.items():
                        r[k] = {"value": v}

        for target, side in [("search_records", fake_search),
                             ("get_record", fake_get),
                             ("create_record", fake_create),
                             ("update_record", fake_update)]:
            p = patch(f"hub.kintone.{target}", new=AsyncMock(side_effect=side))
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    # ── helpers ──────────────────────────────────────────────────────────────
    def _mk_confirmed_run(self, *, rank=1, heirs=None, case="9"):
        payload = {"heirs": heirs or [
            {"person_id": "11", "zokugara_code": "child", "share": "1/1"}],
            "facts": ["minpo_890"]}
        rid = _run(create_derivation_run(
            case_app_id="26", case_record_id=case, decedent_person_id="10",
            at_date="2026-01-01", frozen_case_version="v0.1",
            input_person_revisions={}, input_person_ids=[],
            input_hash=f"ih-{os.urandom(8).hex()}", status="derived",
            rank=rank, result_payload=payload, result_hash="rh" * 32,
            lawyer_flags=None, provisional=True, supersedes_run_id=None,
            engine_version="e1"))
        db.reset_for_tests()
        from datetime import datetime, timezone
        _run(create_decisions_for_heads(
            case, [rid], decision="confirmed", decided_by="ATT1",
            decided_at=datetime.now(timezone.utc)))
        db.reset_for_tests()
        from hub.heir_projection import ZOKUGARA_CODE_TO_APP36
        self.app36 = [
            {"$id": {"value": str(900 + i)}, "$revision": {"value": "1"},
             "current_derivation_run_id": {"value": str(rid)},
             "導出元人物ID": {"value": str(h["person_id"])},
             "戸籍確認済": {"value": "yes"},
             "続柄": {"value": ZOKUGARA_CODE_TO_APP36.get(
                 h.get("zokugara_code"), "")}}
            for i, h in enumerate(payload["heirs"])]
        return rid

    def _plan(self, case="9"):
        m = _run(sp.build_plan(case))
        db.reset_for_tests()
        return m

    def _file(self, materials):
        r = _run(sp.file_plan_envelope(materials))
        db.reset_for_tests()
        return r

    def _is_plan_envelope(self, rec):
        try:
            data = json.loads((rec.get("チャネル固有データ") or {})
                              .get("value") or "")
        except (ValueError, TypeError):
            return False
        return isinstance(data, dict) and "shokumu_plan" in data

    def _envelope_item(self):
        rec = next(r for r in self.app30
                   if self._is_plan_envelope(r)
                   and (r.get("発送ステータス") or {}).get("value") == "要確認")
        detail = json.loads((rec["チャネル固有データ"])["value"])["shokumu_plan"]
        return ReviewItem(record_id=(rec["$id"])["value"],
                          subject="請求案", detail=detail)

    def _resolve(self, item, case="9"):
        group = ReviewGroup(source="shokumu_plan", idempotency_key="k",
                            items=[item])
        r = _run(sp._resolve_shokumu_plan(group, case))
        db.reset_for_tests()
        return r


# ══════════════════════════════════════════════════════════════
class TestPlanGeneration(_Base):
    def test_common_phase_matrix_and_order(self):
        """§6-1/§6-2/§6-9: 共通 2 行（除票=先頭 propose・附票=input_required）・
        第二段未充足は common 縮退＋条件列挙の道案内＋F5 注記の写像。"""
        m = self._plan()
        self.assertEqual(m.phase, "common")
        self.assertEqual([c["line_type"] for c in m.candidates],
                         ["joh_removed", "fuhyo"])
        self.assertEqual(m.candidates[0]["status"], "propose")
        self.assertEqual(m.candidates[0]["municipality"], "川口市")
        self.assertEqual(m.candidates[1]["status"], "input_required")
        self.assertTrue(any("条件未充足" in g for g in m.guidance))
        self.assertTrue(any("収集見込み" in g for g in m.guidance))

    def test_full_phase_rank1_set(self):
        """§6-1/§6-13/§6-23: rank1（子）セット＝decedent_joseki＋
        applicant_current（欠落行の補完込み・マトリクス 1:1）。"""
        self._mk_confirmed_run(rank=1)
        m = self._plan()
        self.assertEqual(m.phase, "full")
        self.assertEqual([c["line_type"] for c in m.candidates],
                         ["joh_removed", "fuhyo", "decedent_joseki",
                          "applicant_current"])
        app = m.candidates[-1]
        self.assertIsNone(app["person_id"])          # 裁定⑦=(C) 機械的固定
        self.assertEqual(app["status"], "input_required")
        self.assertEqual(app["municipality"], sp.INPUT_REQUIRED)

    def test_chain_missing_is_input_required(self):
        """§6-1/§6-22: F5 未収集→chain_missing 行・request_type=要入力＝
        input_required（M1 起票対象にならない・§2A.3）。"""
        self.persons.append(_person("12", "母花子", decedent="no"))
        self._mk_confirmed_run(rank=2, heirs=[
            {"person_id": "12", "zokugara_code": "lineal_ascendant",
             "share": "1/1"}])
        self.persons[0]["父人物ID"] = {"value": ""}
        self.city_master["足立区"] = "330"
        self.kosekis = [{"$id": {"value": "70"}, "読解JSON": {"value": json.dumps(
            {"戸籍": {"本籍": "埼玉県川口市大字Y 3", "筆頭者": "被相続人太郎",
                      "従前戸籍": {"本籍": "東京都足立区Z 9",
                                   "筆頭者": "先代"}}}, ensure_ascii=False)}}]
        m = self._plan()
        chain = [c for c in m.candidates if c["line_type"] == "chain_missing"]
        self.assertEqual(len(chain), 1)
        self.assertEqual(chain[0]["request_type"], sp.INPUT_REQUIRED)
        self.assertEqual(chain[0]["status"], "input_required")
        self.assertEqual(chain[0]["municipality"], "足立区")

    def test_senjun_hoki_no_expansion(self):
        """§6-1: 先順位放棄フラグ→マトリクス展開なし＝共通行のみ＋個別確定警報。"""
        self._mk_confirmed_run(rank=1)
        with patch.object(sp, "_senjun_hoki_flagged", return_value=True):
            m = self._plan()
        self.assertEqual([c["line_type"] for c in m.candidates],
                         ["joh_removed", "fuhyo"])
        self.assertTrue(any("個別確定" in g for g in m.guidance))

    def test_indeterminate_falls_to_problems(self):
        """§6-1/§6-11: 判定不能（被相続人 0/2 行・App36 不一致系）→ problems
        列挙＝全体要確認（write 0）。"""
        self.persons[0]["被相続人フラグ"] = {"value": "no"}
        m = self._plan()
        self.assertTrue(m.problems)
        self.assertEqual(self.created, [])
        # App36 不一致（§2B-3〜6 の否定形）→ common 縮退＋条件列挙
        self.persons[0]["被相続人フラグ"] = {"value": "yes"}
        self._mk_confirmed_run(rank=1)
        self.app36[0]["戸籍確認済"] = {"value": "no"}
        m2 = self._plan()
        self.assertEqual(m2.phase, "common")
        self.assertTrue(any("§2B-4" in g for g in m2.guidance))

    def test_sibling_death_four_branches(self):
        """§6-32: sibling_death 4 分岐——(a) 共有親一意→確定・(b) 共有なし・
        (c) 両方該当・(d) 親 ID 欠損→いずれも要確認（problems・推測ゼロ）。"""
        base = [
            _person("10", "被相続人", honseki="埼玉県川口市A", death="2026-01-01",
                    decedent="yes", father="1", mother="2",
                    birth="昭和20年1月1日"),
            _person("1", "祖父"), _person("2", "祖母"),
            _person("20", "兄", father="1", mother="2",
                    honseki="埼玉県川口市B"),
            _person("21", "甥", father="20", mother="30"),
            _person("30", "義姉"),
        ]
        heirs = [{"person_id": "21", "zokugara_code": "nephew_niece_rep",
                  "share": "1/1"}]
        # (a) 一意特定
        self.persons = [dict(p) for p in base]
        self._mk_confirmed_run(rank=3, heirs=heirs)
        self.app36[0]["導出元人物ID"] = {"value": "21"}
        self.app36[0]["続柄"] = {"value": "甥姪（代襲）"}
        m = self._plan()
        sib = [c for c in m.candidates if c["line_type"] == "sibling_death"]
        self.assertEqual([c["person_id"] for c in sib], ["20"])
        # (b) 共有なし (c) 両方該当 (d) 欠損
        for mutate in (
                lambda ps: ps[3].update({"父人物ID": {"value": "8"},
                                         "母人物ID": {"value": "9"}}),
                lambda ps: ps[5].update({"父人物ID": {"value": "1"},
                                         "母人物ID": {"value": "2"}}),
                lambda ps: ps[4].update({"父人物ID": {"value": ""},
                                         "母人物ID": {"value": ""}})):
            with self.subTest(mutate=mutate):
                self.persons = [dict(p) for p in base]
                for i, p in enumerate(self.persons):
                    self.persons[i] = json.loads(json.dumps(p))
                mutate(self.persons)
                m2 = self._plan()
                self.assertTrue(any("兄弟姉妹" in p for p in m2.problems))

    def test_app31_fallback_order_and_input_required(self):
        """§6-19/§6-43: 政令市は区→市の照合順・両方不在は要入力（空文字は ""
        統一・null 非使用）。"""
        self.city_master = {"さいたま市大宮区": "320"}
        self.persons[0]["住所最新"] = {"value": "埼玉県さいたま市大宮区X 1"}
        m = self._plan()
        row = next(r for r in m.app31_snapshot
                   if r["line_type"] == "joh_removed")
        self.assertEqual(row["fallback"], "ward")
        self.city_master = {"さいたま市": "321"}
        m2 = self._plan()
        row2 = next(r for r in m2.app31_snapshot
                    if r["line_type"] == "joh_removed")
        self.assertEqual(row2["fallback"], "city")
        self.city_master = {}
        m3 = self._plan()
        row3 = next(r for r in m3.app31_snapshot
                    if r["line_type"] == "joh_removed")
        self.assertEqual(m3.candidates[0]["municipality"], sp.INPUT_REQUIRED)
        self.assertEqual((row3["app31_record_id"], row3["有効"],
                          row3["fallback"]), ("", "", ""))


class TestHashesAndDetail(_Base):
    def test_envelope_idempotent_open_only(self):
        """§6-3/§6-27: 同一材料＋open 封筒→already_filed／terminal のみ→新規
        起票（却下=非抑止）／材料変化→新 plan_hash・新封筒。"""
        m = self._plan()
        r1 = self._file(m)
        self.assertEqual(r1["status"], "filed")
        r2 = self._file(self._plan())
        self.assertEqual(r2["status"], "already_filed")
        self.assertEqual(r2["record_id"], r1["record_id"])
        # 却下（terminal 化）→ 新規起票
        for rec in self.app30:
            rec["発送ステータス"] = {"value": "完了"}
        r3 = self._file(self._plan())
        self.assertEqual(r3["status"], "filed")
        self.assertNotEqual(r3["record_id"], r1["record_id"])
        # 材料変化（App33 追加）→ 別 plan_hash
        h1 = self._plan().plan_hash()
        self.kosekis = [{"$id": {"value": "70"},
                         "読解JSON": {"value": "{}"}}]
        self.assertNotEqual(self._plan().plan_hash(), h1)

    def test_stale_materials_change_plan_hash(self):
        """§6-12/§6-20/§6-21/§6-29: 使用 field 変更→plan_hash 変化・非使用 field
        変更→不変（両方向）・App31 更新→変化・被相続人フラグ付替え→問題化・
        §2C 状態（App30 起票状態）だけの変化→plan_hash 不変。"""
        h0 = self._plan().plan_hash()
        self.persons[0]["氏名"] = {"value": "被相続人改名"}
        h1 = self._plan().plan_hash()
        self.assertNotEqual(h0, h1)                       # 使用 field
        self.persons[0]["名寄せキー"] = {"value": "無関係"}
        self.assertEqual(self._plan().plan_hash(), h1)    # 非使用 field
        self.city_master["川口市"] = "999"                 # App31 引当て替え
        h2 = self._plan().plan_hash()
        self.assertNotEqual(h1, h2)
        # §2C 状態のみの変化（App30 に M1 起票が現れる）→ plan_hash 不変
        self.app30.append({"$id": {"value": "600"},
                           "チャネル": {"value": "職務上請求"},
                           "発送ステータス": {"value": "下書き"},
                           "チャネル固有データ": {"value": "{}"}})
        self.assertEqual(self._plan().plan_hash(), h2)
        # 被相続人フラグ付替え（full 時は run 不一致の問題化）
        self._mk_confirmed_run(rank=1)
        self.persons[1]["被相続人フラグ"] = {"value": "yes"}
        self.persons[0]["被相続人フラグ"] = {"value": "no"}
        m = self._plan()
        self.assertTrue(m.problems)

    def test_detail_validation_negatives(self):
        """§6-14/§6-26/§6-33: 閉集合外キー・grammar 外・相関制約違反・
        candidates 順序違反・plan_lines 系の enum 外が保存境界で拒否。"""
        m = self._plan()
        good = m.detail()
        sp.validate_detail(good)                          # 正常形は通る
        cases = []
        d = json.loads(json.dumps(good)); d["余分"] = 1; cases.append(d)
        d = json.loads(json.dumps(good)); d["plan_hash"] = "xyz"; cases.append(d)
        d = json.loads(json.dumps(good)); d["phase"] = "both"; cases.append(d)
        d = json.loads(json.dumps(good))
        d["candidates"][0]["municipality"] = sp.INPUT_REQUIRED  # propose のまま
        cases.append(d)
        d = json.loads(json.dumps(good))
        d["candidates"][0]["line_type"] = "applicant_current"   # pid 非 null
        cases.append(d)
        d = json.loads(json.dumps(good))
        d["candidates"] = list(reversed(d["candidates"]))       # 順序違反
        cases.append(d)
        for i, bad in enumerate(cases):
            with self.subTest(case=i):
                with self.assertRaises(sp.PlanPolicyError):
                    sp.validate_detail(bad)
        with self.assertRaises(Exception):
            sp.m1_fingerprint({"x": 1}, "", ["not_a_line_type"], "u", "form1")

    def test_candidate_total_order_and_merge(self):
        """§6-45/§6-46: candidates/snapshot の完全順序 sort・全鍵一致併合・
        入力順のみ相違→同値（plan_hash 決定性）。"""
        c = {"line_type": "parents_death", "request_type": "除籍謄本",
             "count": 1, "person_id": "2", "municipality": "川口市",
             "status": "propose"}
        c2 = dict(c, person_id="1")
        self.assertEqual(sp.sort_candidates([c, c2, dict(c)]),
                         sp.sort_candidates([dict(c), c2, c]))
        self.assertEqual(len(sp.sort_candidates([c, dict(c)])), 1)
        rows = [{"line_type": "joh_removed", "person_id": "10",
                 "市区町村名": "川口市", "app31_record_id": "310",
                 "有効": "yes", "fallback": "city"},
                {"line_type": "joh_removed", "person_id": "10",
                 "市区町村名": "川口市", "app31_record_id": "310",
                 "有効": "no", "fallback": "city"}]
        s1 = sp.sort_app31_snapshot(rows)
        s2 = sp.sort_app31_snapshot(list(reversed(rows)))
        self.assertEqual(s1, s2)
        self.assertEqual([r["有効"] for r in s1], ["no", "yes"])   # cp 昇順
        self.assertEqual(len(sp.sort_app31_snapshot([rows[0], dict(rows[0])])),
                         1)                                        # 併合

    def test_snapshot_uses_own_keys_only(self):
        """§6-47: snapshot の順序・同一性判定が自身の 6 キーのみを参照
        （candidates 側 status/count 等への参照ゼロ・AST pin）。"""
        src = Path("hub/shokumu_plan.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree)
                  if isinstance(n, ast.FunctionDef)
                  and n.name == "sort_app31_snapshot")
        consts = {n.value for n in ast.walk(fn)
                  if isinstance(n, ast.Constant) and isinstance(n.value, str)}
        self.assertFalse(consts & {"status", "count", "request_type"})


class TestFingerprint(_Base):
    def _bundle_fp(self, **override):
        base = {"request_items": [{"type": "住民票の除票", "count": 1}],
                "municipality": "川口市",
                "target": {"対象者": "被相続人太郎",
                           "生年月日": "昭和20年1月1日",
                           "本籍": "埼玉県川口市大字Y 3",
                           "住所": "埼玉県川口市大字X 1-2"},
                "purpose": "受任事件（相続放棄申述）の申述に必要な戸籍等の取得のため"}
        base.update(override.pop("channel", {}))
        return sp.m1_fingerprint(base, override.pop("app31", "310"),
                                 override.pop("lines", ["joh_removed"]),
                                 override.pop("unit", "相続放棄"),
                                 override.pop("form", "form2"))

    def test_fingerprint_material_sensitivity(self):
        """§6-35〜41: 一致=同値／purpose のみ変更・App31 id のみ変更・target
        4 field 個別変更・count 変化→各不一致／並び順のみ相違→同値。"""
        base = self._bundle_fp()
        self.assertEqual(base, self._bundle_fp())                    # 35
        self.assertNotEqual(base, self._bundle_fp(
            channel={"purpose": "別目的"}))                          # 39
        self.assertNotEqual(base, self._bundle_fp(app31="999"))      # 40
        for f, v in [("対象者", "別人"), ("生年月日", "平成1年1月1日"),
                     ("本籍", "別本籍"), ("住所", "別住所")]:         # 41/36
            with self.subTest(field=f):
                t = {"対象者": "被相続人太郎", "生年月日": "昭和20年1月1日",
                     "本籍": "埼玉県川口市大字Y 3",
                     "住所": "埼玉県川口市大字X 1-2", f: v}
                self.assertNotEqual(base, self._bundle_fp(
                    channel={"target": t}))
        self.assertNotEqual(base, self._bundle_fp(
            channel={"request_items": [{"type": "住民票の除票",
                                        "count": 2}]}))              # 37
        # 並び順のみ相違→同値（38・plan_lines の順・request_items は canonical）
        self.assertEqual(base, self._bundle_fp(lines=["joh_removed",
                                                      "joh_removed"]))

    def test_target_closed_set_structure(self):
        """§6-42: plan 経路の target は 4 キー閉集合（フリガナ・世帯主・筆頭者は
        非搭載）——実出力＋ソース検査の両面。"""
        m = self._plan()
        target = sp._build_target(m, "10")
        self.assertEqual(set(target), {"対象者", "生年月日", "本籍", "住所"})
        src = Path("hub/shokumu_plan.py").read_text(encoding="utf-8")
        for banned in ("フリガナ", "世帯主", "筆頭者"):
            self.assertNotIn(f'"{banned}"', src)


class TestResolveAndM1(_Base):
    def _confirmed_envelope(self):
        self._mk_confirmed_run(rank=1)
        m = self._plan()
        self._file(m)
        return self._envelope_item()

    def test_resolver_wiring_and_m1_fields(self):
        """§6-5/§6-6/§6-15/§6-44/§6-48: RESOLVERS 結線・M1 App30 fields が
        _fields_shokumu_seikyu と同一集合＋共通部・purpose=PURPOSE_BY_UNIT
        byte 一致・監査メタ 6 キー（単一定数）・A層と create 渡しの byte 一致。"""
        self.assertIn("shokumu_plan", RESOLVERS)
        item = self._confirmed_envelope()
        r = self._resolve(item)
        self.assertEqual(r["status"], "resolved")
        out = r["items"][0]
        self.assertEqual(out["issued"], 2)      # 除票 + decedent_joseki
        self.assertTrue(out["envelope_closed"])
        m1 = [f for label, f in self.created
              if f.get("チャネル") == "職務上請求"]
        self.assertEqual(len(m1), 2)
        from dispatch_bot.shokumu import PURPOSE_BY_UNIT
        for fields in m1:
            self.assertEqual(fields["発送ステータス"], "下書き")
            self.assertEqual(
                set(fields),
                {"発送ステータス", "ユニット種別", "顧客名表示用",
                 "案件アプリID", "案件レコードID", "実行済み", "チャネル",
                 "件名", "宛先名", "宛先郵便番号", "宛先住所",
                 "チャネル固有データ"})
            data = json.loads(fields["チャネル固有データ"])
            self.assertEqual(data["purpose"], PURPOSE_BY_UNIT["相続放棄"])
            for k in sp.PLAN_AUDIT_META_KEYS:
                self.assertIn(k, data)
            # A層 byte 一致（§4B fix7）: 保存 channel_json から監査メタを除いた
            # ものが fingerprint A層と一致
            channel = {k: v for k, v in data.items()
                       if k not in sp.PLAN_AUDIT_META_KEYS}
            fp = sp.m1_fingerprint(
                channel, "310", data["plan_lines"], sp.PLAN_UNIT,
                data["plan_idem"].rsplit(":", 1)[1])
            self.assertEqual(fp, data["m1_fingerprint"])

    def test_m1_input_passes_existing_parser(self):
        """§6-6/§6-44(d): 併合済み channel_json が既存 M1 の parse_channel_data
        を通過し、様式1 の count 合算が 1 エントリで表現される（様式 PDF 生成
        自体は M1 既存テスト test_shokumu_form が担保）。"""
        from channels.shokumu_seikyu import parse_channel_data
        item = self._confirmed_envelope()
        self._resolve(item)
        for _label, fields in self.created:
            if fields.get("チャネル") != "職務上請求":
                continue
            parsed = parse_channel_data(
                {"チャネル固有データ": {"value": fields["チャネル固有データ"]}})
            types = [i["type"] for i in parsed["request_items"]]
            self.assertEqual(len(types), len(set(types)))   # 同一 type 併合済み

    def test_plan_idem_hit_fingerprint_paths(self):
        """§6-24/§6-25/§6-34/§6-35/§6-49: plan_idem HIT の fingerprint 一致=skip
        回収（ACK 不明後の回収を含む）／不一致・欠落=要確認／壊れ JSON=全体要確認。"""
        item = self._confirmed_envelope()
        r1 = self._resolve(item)
        self.assertEqual(r1["items"][0]["issued"], 2)
        # 封筒を open に戻して再確定 → 全件 recovered（skip 回収・二重起票ゼロ）
        for rec in self.app30:
            if self._is_plan_envelope(rec):
                rec["発送ステータス"] = {"value": "要確認"}
                rec["実行済み"] = {"value": "no"}
        n_created = len(self.created)
        r2 = self._resolve(self._envelope_item())
        # 共通行（除票・附票の 2 行）は §2C 前段フィルタで skip・
        # 残り（form1 束ね）は §4B の plan_idem/fingerprint 一致で回収
        self.assertEqual(r2["items"][0]["recovered"], 1)
        self.assertEqual(r2["items"][0]["skipped"], 2)
        self.assertEqual(r2["items"][0]["issued"], 0)
        self.assertEqual(
            len([1 for label, f in self.created[n_created:]
                 if f.get("チャネル") == "職務上請求"]), 0)
        # fingerprint 欠落 → 要確認（§6-49）。r2 で封筒が再クローズされるため
        # 再度 open へ戻す（テスト都合の状態操作）
        for rec in self.app30:
            if self._is_plan_envelope(rec):
                rec["発送ステータス"] = {"value": "要確認"}
                rec["実行済み"] = {"value": "no"}
        for rec in self.app30:
            if (rec.get("チャネル") or {}).get("value") == "職務上請求":
                data = json.loads(rec["チャネル固有データ"]["value"])
                data.pop("m1_fingerprint", None)
                rec["チャネル固有データ"] = {"value": json.dumps(
                    data, ensure_ascii=False)}
        r3 = self._resolve(self._envelope_item())
        self.assertGreater(r3["items"][0]["held"], 0)
        self.assertFalse(r3["items"][0]["envelope_closed"])
        # 壊れ JSON → 全体要確認（未起票扱いにしない・§6-24）
        self.app30.append({"$id": {"value": "650"},
                           "チャネル": {"value": "職務上請求"},
                           "発送ステータス": {"value": "下書き"},
                           "チャネル固有データ": {"value": "{{broken"}})
        r4 = self._resolve(self._envelope_item())
        self.assertEqual(r4["items"][0]["issued"], 0)
        self.assertIn("解釈不能", r4["items"][0]["reason"])

    def test_partial_failure_open_and_reconcile(self):
        """§6-17/§6-25/§6-30: k 件目 create 失敗→例外伝播・封筒 open 維持・
        作成済み残存→**同一封筒の再確定（新関所往復）**で既存回収＋残り起票→
        全件でクローズ。"""
        item = self._confirmed_envelope()
        calls = {"n": 0}
        orig_create = None
        import hub.kintone as hk
        orig_create = hk.create_record

        async def failing_create(app, fields):
            if fields.get("チャネル") == "職務上請求":
                calls["n"] += 1
                if calls["n"] >= 2:
                    raise RuntimeError("kintone down")
            return await orig_create(app, fields)

        with patch("hub.kintone.create_record",
                   new=AsyncMock(side_effect=failing_create)):
            with self.assertRaises(RuntimeError):
                self._resolve(item)
        env = next(r for r in self.app30 if self._is_plan_envelope(r))
        self.assertEqual((env["発送ステータス"])["value"], "要確認")  # open 維持
        r2 = self._resolve(self._envelope_item())                     # 再確定
        self.assertEqual(r2["items"][0]["recovered"], 1)
        self.assertEqual(r2["items"][0]["issued"], 1)
        self.assertTrue(r2["items"][0]["envelope_closed"])

    def test_stale_on_confirm_aborts(self):
        """§6-4/§6-12: 起票後に使用 field を変更 → 確定時の plan_hash 再計算
        不一致 → aborted・write 0。"""
        item = self._confirmed_envelope()
        self.persons[0]["住所最新"] = {"value": "東京都千代田区1-1"}
        n = len(self.created)
        r = self._resolve(item)
        self.assertEqual(r["status"], "aborted")
        self.assertIn("前提が変わっています", r["reason"])
        self.assertEqual(len(self.created), n)

    def test_inversion_recovery(self):
        """§6-31: common 封筒却下（terminal 化）→ full 確定で共通行が復元起票
        される（canonical 全候補保存＋実行時フィルタの成立）。"""
        m = self._plan()                       # common phase
        self._file(m)
        for rec in self.app30:
            rec["発送ステータス"] = {"value": "完了"}   # 却下
        item = self._confirmed_envelope()      # full 封筒
        r = self._resolve(item)
        self.assertEqual(r["status"], "resolved")
        m1_types = [json.loads(f["チャネル固有データ"])["request_items"][0]["type"]
                    for _l, f in self.created
                    if f.get("チャネル") == "職務上請求"]
        self.assertIn("住民票の除票", m1_types)       # 共通行が復元起票

    def test_bundle_normalization(self):
        """§6-28: 同一自治体×異 person_id／異様式は別 M1・同一鍵のみ束ね。"""
        cands = [
            {"line_type": "decedent_joseki", "request_type": "除籍謄本",
             "count": 1, "person_id": "10", "municipality": "川口市",
             "status": "propose"},
            {"line_type": "parents_death", "request_type": "除籍謄本",
             "count": 1, "person_id": "1", "municipality": "川口市",
             "status": "propose"},
            {"line_type": "joh_removed", "request_type": "住民票の除票",
             "count": 1, "person_id": "10", "municipality": "川口市",
             "status": "propose"},
            {"line_type": "chain_missing", "request_type": sp.INPUT_REQUIRED,
             "count": 1, "person_id": "10", "municipality": "川口市",
             "status": "input_required"},
        ]
        bundles = sp._bundle_candidates(cands)
        self.assertEqual(len(bundles), 3)      # (10,form1)(1,form1)(10,form2)
        self.assertNotIn(("川口市", "10", "form1"),
                         [k for k in bundles if "要入力" in str(k)])

    def test_rejected_decision_vocab_unsupported(self):
        """§6-5: 保留/否認語彙は shokumu_plan 封筒では unsupported（decision
        能力ベースの既存規律・確定のみ対応）。"""
        from review_resolve import resolve_group
        item = self._confirmed_envelope()
        group = ReviewGroup(source="shokumu_plan", idempotency_key="k",
                            items=[item])
        r = _run(resolve_group(group, "9", decided_by="U1", decision="held"))
        db.reset_for_tests()
        self.assertEqual(r["status"], "unsupported")


class TestGuardsAndFlag(_Base):
    def test_flag_off_zero_io(self):
        """§6-8/§6-18（IMPL-fix1 IMPL-04 で強化）: flag OFF→固定文言辞退・
        build_plan/file_plan_envelope 未呼出し・kintone search/get/create の
        call count==0 を mock で直接検査・語彙一覧非掲載。"""
        from types import SimpleNamespace
        from dispatch_bot import shokumu_plan_task as task
        from dispatch_bot.registry import catalog_for_prompt
        import hub.kintone as hk
        with patch.dict(os.environ, {"SHOKUMU_PLAN_ENABLED": ""}), \
             patch("dispatch_bot.shokumu_plan_task.build_plan") as bp, \
             patch("dispatch_bot.shokumu_plan_task.file_plan_envelope") as fe:
            self.assertNotIn("shokumu_plan", catalog_for_prompt())
            with patch("dispatch_bot.confirm.invalidate"):
                msg, _rid, _url = _run(task.execute(SimpleNamespace(
                    user_id="U1", case=SimpleNamespace(record_id="9"))))
            self.assertEqual(msg, task.MSG_DISABLED)
            bp.assert_not_called()
            fe.assert_not_called()
        self.assertEqual(self.created, [])
        self.assertEqual(hk.search_records.await_count, 0)
        self.assertEqual(hk.get_record.await_count, 0)
        self.assertEqual(hk.create_record.await_count, 0)

    def test_no_approval_transition_in_source(self):
        """§6-7/§6-16: 承認済みへのサーバ遷移コードの不存在（ソース検査）＋
        全 write の 発送ステータス 値が閉集合（下書き/要確認/完了）のみ。"""
        for name in ("hub/shokumu_plan.py", "dispatch_bot/shokumu_plan_task.py"):
            src = Path(name).read_text(encoding="utf-8")
            self.assertNotIn('"承認済"', src)
            self.assertNotIn("承認待ち", src.replace("承認フロー", ""))
        item = None
        self._mk_confirmed_run(rank=1)
        self._file(self._plan())
        item = self._envelope_item()
        self._resolve(item)
        for _label, fields in self.created + [(l, f) for l, r, f in
                                              self.updated]:
            v = fields.get("発送ステータス")
            if v is not None:
                self.assertIn(v, ("下書き", "要確認", "完了"))

    def test_no_fee_or_form_logic(self):
        """§6-6: 料金計算（compute_kogawase）・様式生成の呼出しゼロ（AST・
        M1 委譲＝複製禁止）。"""
        tree = ast.parse(Path("hub/shokumu_plan.py").read_text(encoding="utf-8"))
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        for banned in ("compute_kogawase", "build_request_form_pdfs",
                       "find_municipality", "resolved_purpose"):
            self.assertNotIn(banned, names | attrs)

    def test_f5_note_in_envelope_response(self):
        """§6-2/§6-10: F5「収集見込み」注記が応答へ写像・第一段不存在時は
        第二段に共通行が含まれる（漏れゼロ・§2C）。"""
        from types import SimpleNamespace
        from dispatch_bot import shokumu_plan_task as task
        self._mk_confirmed_run(rank=1)
        with patch("dispatch_bot.confirm.invalidate"):
            msg, rid, _url = _run(task.execute(SimpleNamespace(
                user_id="U1", case=SimpleNamespace(record_id="9"))))
        db.reset_for_tests()
        self.assertIn("収集見込み", msg)
        self.assertIn("提案です", msg)
        detail = self._envelope_item().detail
        self.assertIn("joh_removed",
                      [c["line_type"] for c in detail["candidates"]])


class TestImplFix1(_Base):
    """R-SHOKUMU-PLAN-IMPL-1 対応の対照（IMPL-01/02/03/05）。"""

    def test_form1_missing_birth_is_input_required(self):
        """IMPL-01: 様式1 系の対象 person の出生行年月日が空 → input_required
        （被相続人・父・母・sibling の各形・空のまま propose へ進まない）。"""
        self.persons[0]["身分事項"] = {"value": []}
        self._mk_confirmed_run(rank=1)
        m = self._plan()
        dj = next(c for c in m.candidates
                  if c["line_type"] == "decedent_joseki")
        self.assertEqual(dj["status"], "input_required")
        joh = next(c for c in m.candidates if c["line_type"] == "joh_removed")
        self.assertEqual(joh["status"], "propose")   # form2 は影響なし
        base = [
            _person("10", "被相続人", honseki="埼玉県川口市A",
                    death="2026-01-01", decedent="yes", father="1", mother="2",
                    birth="昭和20年1月1日"),
            _person("1", "祖父", honseki="埼玉県川口市B", birth="大正10年1月1日"),
            _person("2", "祖母", honseki="埼玉県川口市C", birth="大正12年2月2日"),
            _person("20", "兄", father="1", mother="2",
                    honseki="埼玉県川口市D", birth="昭和18年3月3日"),
            _person("21", "甥", father="20", mother="30",
                    birth="昭和50年4月4日"),
            _person("30", "義姉", birth="昭和22年5月5日"),
        ]
        heirs = [{"person_id": "21", "zokugara_code": "nephew_niece_rep",
                  "share": "1/1"}]
        for i, missing_pid in enumerate(("1", "2", "20")):
            with self.subTest(missing=missing_pid):
                case = f"3{i}"          # 案件を分ける（single-root 制約）
                self.persons = json.loads(json.dumps(base))
                for rec in self.persons:
                    if rec["$id"]["value"] == missing_pid:
                        rec["身分事項"] = {"value": []}
                self._mk_confirmed_run(rank=3, heirs=heirs, case=case)
                m2 = self._plan(case)
                target_line = ("sibling_death" if missing_pid == "20"
                               else "parents_death")
                rows = [c for c in m2.candidates
                        if c["line_type"] == target_line
                        and c["person_id"] == missing_pid]
                self.assertEqual(len(rows), 1, m2.problems)
                self.assertEqual(rows[0]["status"], "input_required")

    def test_sibling_one_side_missing_parent_id(self):
        """IMPL-03（司令塔裁定）: 甥の父母人物 ID の**片側欠損×他方一致**でも
        自動確定せず要入力（欠損側が別の兄弟姉妹である可能性を排除できず
        半血判定=民法 900④但書を誤り得るため安全側）。"""
        self.persons = [
            _person("10", "被相続人", honseki="埼玉県川口市A",
                    death="2026-01-01", decedent="yes", father="1", mother="2",
                    birth="昭和20年1月1日"),
            _person("1", "祖父"), _person("2", "祖母"),
            _person("20", "兄", father="1", mother="2",
                    honseki="埼玉県川口市D", birth="昭和18年3月3日"),
            _person("21", "甥", father="20", mother="",
                    birth="昭和50年4月4日"),
        ]
        self._mk_confirmed_run(rank=3, heirs=[
            {"person_id": "21", "zokugara_code": "nephew_niece_rep",
             "share": "1/1"}])
        m = self._plan()
        self.assertTrue(any("兄弟姉妹" in x for x in m.problems))

    def test_audit_meta_validation_negatives_on_hit(self):
        """IMPL-02: 既存 M1 の保存データを改変して HIT 再照合——plan_lines
        欠落/非 list/重複/順序違反/enum 外・余分キー・不足キー(plan_idem 以外)の
        各形が**すべて要確認・create 0**（比較不能を一致扱いにしない）。"""
        self._mk_confirmed_run(rank=1)
        self._file(self._plan())
        r1 = self._resolve(self._envelope_item())
        self.assertEqual(r1["items"][0]["issued"], 2)

        def drop_lines(d):
            d.pop("plan_lines")

        def non_list(d):
            d["plan_lines"] = "joh_removed"

        def dup(d):
            d["plan_lines"] = d["plan_lines"] + d["plan_lines"]

        def unsorted(d):
            d["plan_lines"] = ["decedent_joseki", "joh_removed"]

        def enum_out(d):
            d["plan_lines"] = ["not_a_type"]

        def extra_key(d):
            d["余分"] = 1

        def drop_hash(d):
            d.pop("plan_hash")

        for mutate in (drop_lines, non_list, dup, unsorted, enum_out,
                       extra_key, drop_hash):
            with self.subTest(mutate=mutate.__name__):
                for rec in self.app30:
                    if self._is_plan_envelope(rec):
                        rec["発送ステータス"] = {"value": "要確認"}
                        rec["実行済み"] = {"value": "no"}
                saved = []
                for rec in self.app30:
                    if (rec.get("チャネル") or {}).get("value") != "職務上請求":
                        continue
                    raw = rec["チャネル固有データ"]["value"]
                    saved.append((rec, raw))
                    data = json.loads(raw)
                    if "plan_idem" in data:
                        mutate(data)
                        rec["チャネル固有データ"] = {"value": json.dumps(
                            data, ensure_ascii=False)}
                n = len([1 for _l, f in self.created
                         if f.get("チャネル") == "職務上請求"])
                r = self._resolve(self._envelope_item())
                self.assertGreater(r["items"][0]["held"], 0, mutate.__name__)
                self.assertFalse(r["items"][0]["envelope_closed"])
                self.assertEqual(
                    len([1 for _l, f in self.created
                         if f.get("チャネル") == "職務上請求"]), n,
                    "create 0 が破れた")
                for rec, raw in saved:
                    rec["チャネル固有データ"] = {"value": raw}

    def test_fields_parity_with_existing_builder(self):
        """IMPL-05: M1 App30 fields を既存 _fields_shokumu_seikyu の実出力と
        直接照合（キー集合＝builder 出力∪file_from_pending 共通部・件名/宛先系/
        channel_json 4 キーの値一致）。"""
        from types import SimpleNamespace
        from dispatch_bot.app30_filer import _fields_shokumu_seikyu
        self._mk_confirmed_run(rank=1)
        self._file(self._plan())
        self._resolve(self._envelope_item())
        ours = next(f for _l, f in self.created
                    if f.get("チャネル") == "職務上請求")
        data = json.loads(ours["チャネル固有データ"])
        pending = SimpleNamespace(
            command_id="cmd-x", instruction_text="請求案の確定",
            user_id="U1",
            parsed={"task_type": "shokumu_seikyu",
                    "task_params": {
                        "request_items": data["request_items"],
                        "municipality": data["municipality"],
                        "target": data["target"], "unit": sp.PLAN_UNIT,
                        "purpose": data["purpose"]}},
            case=SimpleNamespace(record_id="9", unit="相続"))
        builder = _fields_shokumu_seikyu(pending, {}, "山田太郎")
        common = {"発送ステータス", "ユニット種別", "顧客名表示用",
                  "案件アプリID", "案件レコードID", "実行済み"}
        self.assertEqual(set(ours), set(builder) | common)
        for k in ("チャネル", "件名", "宛先名", "宛先郵便番号", "宛先住所"):
            self.assertEqual(ours[k], builder[k], k)
        b_json = json.loads(builder["チャネル固有データ"])
        for k in ("request_items", "municipality", "target", "purpose"):
            self.assertEqual(data[k], b_json[k], k)

    def test_form1_pdf_end_to_end_with_merged_items(self):
        """IMPL-05/§6-44(d): 新規経路の併合済み channel_json（除籍謄本×2 の
        1 エントリ）を既存 build_request_form_pdfs へ実際に通し、様式1 PDF が
        **1 枚**生成されること（count 合計値の実 end-to-end）。"""
        from channels.shokumu_seikyu import build_request_form_pdfs
        office_env = {
            "OFFICE_NAME": "大野法律事務所", "OFFICE_ZIP": "332-0012",
            "OFFICE_ADDRESS": "埼玉県川口市本町4-1-6",
            "OFFICE_TEL": "048-000-0000", "OFFICE_ATTORNEY": "大野太郎",
            "OFFICE_ATTORNEY_REG": "12345",
            "OFFICE_BAR_ASSOCIATION": "埼玉弁護士会"}
        data = {"request_items": [{"type": "除籍謄本", "count": 2}],
                "municipality": "川口市",
                "target": {"対象者": "被相続人太郎",
                           "生年月日": "昭和20年1月1日",
                           "本籍": "埼玉県川口市大字Y 3",
                           "住所": "埼玉県川口市大字X 1-2"},
                "purpose": "受任事件（相続放棄申述）の申述に必要な戸籍等の"
                           "取得のため"}
        record = {"$id": {"value": "9"},
                  "顧客名表示用": {"value": "山田太郎"},
                  "宛先名": {"value": ""},
                  "件名": {"value": "職務上請求（川口市）"}}
        muni = {"市区町村名": {"value": "川口市"}}
        with patch.dict(os.environ, office_env):
            pdfs = build_request_form_pdfs(record, data, muni)
        self.assertEqual(len(pdfs), 1)               # 様式1 PDF 1 枚
        self.assertTrue(pdfs[0][1].startswith(b"%PDF"))


if __name__ == "__main__":
    unittest.main()
