"""KOSEKI-CHECK-1: 戸籍不足チェック（決定的検査＋Q tool 11 本目）。

固定する仕様:
- 切れ目規則: 編製日昇順の被覆前線方式。前線+1日超の編製日で between gap・
  消除日なし（現行）以降は無限被覆＝gap なし・生年月日_西暦があれば
  birth_to_first を判定。
- 判定不能の閉集合: no_kosekis/decedent_unknown/decedent_birth_unknown/
  unparseable_dates/shubetsu_unset/heirs_unregistered——「不足」と断定しない。
- 相続人: App36 行なしは insufficient（fail-closed の明示）。現行戸籍の
  人物名一致で has_current_koseki を判定。
- 出力 grammar: 閉集合 status・ocr_text 非搭載・作業キー除去。
- Q 統合: tool 閉集合 10→11・出典=検査に使った実レコード（App33+App36・
  pdf_url つき）・koseki_coverage_estimate の定型注記が必ず付く。
- read-only: P4 系 checker を koseki_coverage にも適用。
"""

import asyncio
import json
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from ast_policy_helpers import (
    _FORBIDDEN_IMPORTS,
    _binding_violations,
    _readonly_violations,
)
from test_q_batch1 import (
    _ENV,
    _auth_headers,
    _resp,
    _run,
    _submit,
    _tool_use,
)

import koseki_coverage
import hub.webapp_q as wq


def _koseki(rid, shubetsu, hensei, shojo, names, births=None, drive=""):
    persons = [{"氏名": n, "生年月日_西暦": (births or {}).get(n)}
               for n in names]
    reading = {"戸籍": {"筆頭者": names[0] if names else ""}, "人物": persons}
    return {"$id": {"value": rid}, "戸籍種別": {"value": shubetsu},
            "編製日": {"value": hensei or ""}, "消除日": {"value": shojo or ""},
            "読解JSON": {"value": json.dumps(reading, ensure_ascii=False)},
            "Drive_fileId": {"value": drive}}


def _person(rid, name, decedent=False):
    return {"$id": {"value": rid}, "氏名": {"value": name},
            "被相続人フラグ": {"value": "yes" if decedent else "no"}}


def _heir(rid, name):
    return {"$id": {"value": rid}, "氏名": {"value": name}}


def _check(kosekis, persons, heirs, case="3"):
    with patch.object(koseki_coverage.kintone, "search_records",
                      AsyncMock(return_value=kosekis)), \
         patch.object(koseki_coverage.souzoku_dash, "_load_persons",
                      AsyncMock(return_value={"records": persons,
                                              "excluded_merged_count": 0})), \
         patch.object(koseki_coverage.souzoku_dash, "_load_heirs",
                      AsyncMock(return_value={"records": heirs,
                                              "excluded_cancelled_count": 0})):
        return asyncio.run(koseki_coverage.check_coverage(case))


DEC = [_person("1", "熊澤太郎", decedent=True)]


class TestChainGaps(unittest.TestCase):
    def test_between_and_birth_gaps_found(self):
        result = _check(
            [_koseki("31", "改製原（昭和）", "1950-01-01", "1970-01-01",
                     ["熊澤太郎"], births={"熊澤太郎": "1940-05-05"}),
             _koseki("32", "現行", "1975-01-01", "", ["熊澤太郎"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertEqual(result["chain"]["status"], "gaps_found")
        self.assertEqual(result["chain"]["gaps"], [
            {"kind": "birth_to_first", "from": "1940-05-05",
             "to": "1950-01-01"},
            {"kind": "between", "from": "1970-01-01", "to": "1975-01-01"}])
        self.assertEqual(result["decedent"],
                         {"registered": True, "birth_seireki": "1940-05-05"})

    def test_adjacent_intervals_no_gap(self):
        # 消除翌日の編製（差 1 日）は切れ目としない
        result = _check(
            [_koseki("31", "除籍", "1950-01-01", "1970-01-01", ["熊澤太郎"]),
             _koseki("32", "現行", "1970-01-02", "", ["熊澤太郎"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertEqual(result["chain"]["gaps"], [])
        self.assertEqual(result["chain"]["status"], "ok")
        self.assertIn("decedent_birth_unknown",
                      result["chain"]["insufficient_reasons"])

    def test_open_interval_covers_rest(self):
        # 消除日なし（現在に続く）以降は無限被覆＝後続の編製日で gap を出さない
        result = _check(
            [_koseki("31", "現行", "1950-01-01", "", ["熊澤太郎"]),
             _koseki("32", "除籍", "1980-01-01", "1990-01-01", ["熊澤太郎"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertEqual(result["chain"]["gaps"], [])
        self.assertEqual(result["chain"]["status"], "ok")

    def test_decedent_kosekis_only(self):
        # 被相続人が現れない戸籍（相続人の現在戸籍等）は連続性判定に混ぜない
        result = _check(
            [_koseki("31", "除籍", "1950-01-01", "1970-01-01", ["熊澤太郎"]),
             _koseki("33", "現行", "1999-01-01", "", ["熊澤花子"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertEqual(result["chain"]["gaps"], [])   # 1999 は太郎の鎖でない
        rows = {r["record_id"]: r["belongs_to_decedent"]
                for r in result["chain"]["kosekis"]}
        self.assertEqual(rows, {"31": True, "33": False})


class TestInsufficiency(unittest.TestCase):
    def test_decedent_unknown(self):
        result = _check(
            [_koseki("31", "除籍", "1950-01-01", "1970-01-01", ["熊澤太郎"])],
            [_person("1", "熊澤太郎", decedent=False)],
            [_heir("201", "熊澤花子")])
        self.assertEqual(result["chain"]["status"], "insufficient")
        self.assertIn("decedent_unknown",
                      result["chain"]["insufficient_reasons"])
        self.assertFalse(result["decedent"]["registered"])

    def test_undated_koseki_listed_not_asserted_as_gap(self):
        # 西暦変換不能（編製日空）は「不足」と断定せず判定不能として列挙
        result = _check(
            [_koseki("31", "改製原（昭和）", "", "", ["熊澤太郎"]),
             _koseki("32", "現行", "1975-01-01", "", ["熊澤太郎"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertIn("unparseable_dates",
                      result["chain"]["insufficient_reasons"])
        self.assertEqual(result["chain"]["undated_koseki_ids"], ["31"])
        self.assertEqual(result["chain"]["gaps"], [])

    def test_no_kosekis_for_decedent(self):
        result = _check(
            [_koseki("33", "現行", "1999-01-01", "", ["熊澤花子"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertEqual(result["chain"]["status"], "insufficient")
        self.assertIn("no_kosekis", result["chain"]["insufficient_reasons"])

    def test_shubetsu_unset_reason(self):
        result = _check(
            [_koseki("31", "", "1950-01-01", "", ["熊澤太郎"])],
            DEC, [])
        self.assertIn("shubetsu_unset",
                      result["chain"]["insufficient_reasons"])


class TestHeirs(unittest.TestCase):
    def test_current_koseki_matched_and_missing(self):
        result = _check(
            [_koseki("31", "除籍", "1950-01-01", "1970-01-01", ["熊澤太郎"]),
             _koseki("33", "現行", "1999-01-01", "", ["熊澤花子"])],
            DEC, [_heir("201", "熊澤花子"), _heir("202", "熊澤次郎")])
        self.assertEqual(result["heirs"]["status"], "missing_found")
        rows = {r["record_id"]: r["has_current_koseki"]
                for r in result["heirs"]["rows"]}
        self.assertEqual(rows, {"201": True, "202": False})

    def test_all_heirs_covered_ok(self):
        result = _check(
            [_koseki("33", "現行", "1999-01-01", "", ["熊澤花子"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertEqual(result["heirs"]["status"], "ok")

    def test_no_heir_rows_is_explicit_insufficient(self):
        # App36 行なし=「相続人未登録のため判定不能」を fail-closed で明示
        result = _check(
            [_koseki("33", "現行", "1999-01-01", "", ["熊澤花子"])],
            DEC, [])
        self.assertEqual(result["heirs"]["status"], "insufficient")
        self.assertEqual(result["heirs"]["insufficient_reasons"],
                         ["heirs_unregistered"])


class TestOutputGrammar(unittest.TestCase):
    def test_closed_sets_and_no_ocr_text(self):
        result = _check(
            [_koseki("31", "除籍", "1950-01-01", "1970-01-01", ["熊澤太郎"])],
            DEC, [_heir("201", "熊澤花子")])
        self.assertIn(result["chain"]["status"],
                      koseki_coverage.CHAIN_STATUSES)
        self.assertIn(result["heirs"]["status"],
                      koseki_coverage.HEIRS_STATUSES)
        for reason in (result["chain"]["insufficient_reasons"]
                       + result["heirs"]["insufficient_reasons"]):
            self.assertIn(reason, koseki_coverage.INSUFFICIENT_REASONS)
        dumped = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("ocr_text", dumped)
        self.assertNotIn("_names", dumped)          # 作業キー除去
        self.assertTrue(result["estimate"])
        # ocr_text を含む field を fetch していない（subset pin）
        self.assertNotIn("ocr_text", koseki_coverage._KOSEKI_FIELDS)

    def test_readonly_checker_applies(self):
        import ast as _ast
        tree = _ast.parse(
            Path("koseki_coverage.py").read_text(encoding="utf-8"))
        self.assertEqual(_readonly_violations(tree), [])
        self.assertEqual(_binding_violations(tree), [])
        imported = set()
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Import):
                imported |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, _ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(imported & _FORBIDDEN_IMPORTS)


# ── Q 統合（tool 11 本目・出典・注記） ───────────────────────────────────────
class TestQIntegration(unittest.TestCase):
    def setUp(self):
        wq._ask_times.clear()

    def _fixture_result(self):
        return {
            "case_record_id": "3", "estimate": True,
            "decedent": {"registered": True, "birth_seireki": None},
            "chain": {"status": "ok",
                      "kosekis": [{"record_id": "31", "shubetsu": "現行",
                                   "hensei": "2002-01-01", "shojo": None,
                                   "belongs_to_decedent": True,
                                   "drive_file_id":
                                       "1AbC-dEfG_hIjKlMnOpQrStUv"}],
                      "gaps": [], "undated_koseki_ids": [],
                      "insufficient_reasons": ["decedent_birth_unknown"]},
            "heirs": {"status": "insufficient", "rows": [
                {"record_id": "201", "name": "熊澤花子",
                 "has_current_koseki": False}],
                "insufficient_reasons": []},
        }

    def test_tool_registered_as_11th(self):
        self.assertIn("check_koseki_coverage",
                      [t["name"] for t in wq._TOOLS])
        self.assertEqual(len(wq._TOOLS), 11)
        self.assertIn("check_koseki_coverage", wq._DISPATCH)

    def test_dispatch_records_sources_and_flags(self):
        ctx = {"sources": [], "source_keys": set(), "flags": set()}
        env = {**_ENV, "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "d"}
        with patch.dict(os.environ, env), \
             patch("koseki_coverage.check_coverage",
                   AsyncMock(return_value=self._fixture_result())):
            content, is_error = _run(wq._dispatch(
                "check_koseki_coverage", {"case_record_id": "3"}, ctx))
        self.assertFalse(is_error)
        keys = [(s["app"], s["record_id"]) for s in ctx["sources"]]
        self.assertEqual(keys, [("App33(戸籍読解)", "31"),
                                ("App36(相続人)", "201")])
        # App33 出典は原本 PDF リンクつき（grammar 検証済み Drive id のみ）
        self.assertEqual(
            ctx["sources"][0]["pdf_url"],
            "https://drive.google.com/file/d/1AbC-dEfG_hIjKlMnOpQrStUv/view")
        self.assertIn("koseki_coverage_estimate", ctx["flags"])
        self.assertIn("koseki_reading", ctx["flags"])
        payload = json.loads(content)
        self.assertEqual(payload["chain"]["status"], "ok")
        self.assertEqual(payload["_citation_keys"][0]["app"], "App33(戸籍読解)")

    def test_dispatch_rejects_bad_case_id(self):
        ctx = {"sources": [], "source_keys": set(), "flags": set()}
        with patch.dict(os.environ, _ENV):
            content, is_error = _run(wq._dispatch(
                "check_koseki_coverage", {"case_record_id": "3; drop"}, ctx))
        self.assertTrue(is_error)
        self.assertEqual(ctx["sources"], [])

    def test_e2e_answer_carries_estimate_note(self):
        env = {**_ENV, "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "d"}
        calls = []

        async def fake_create(**kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                return _resp("tool_use", [_tool_use(
                    "check_koseki_coverage", {"case_record_id": "3"})])
            tool_result = kwargs["messages"][-1]["content"][0]["content"]
            keys = json.loads(tool_result)["_citation_keys"]
            return _resp("tool_use", [_submit(
                "戸籍の切れ目は見つかりませんでした（参考見立て）。", keys)])

        from types import SimpleNamespace
        stub = SimpleNamespace(messages=SimpleNamespace(
            create=AsyncMock(side_effect=fake_create)))
        with patch.dict(os.environ, env), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch("koseki_coverage.check_coverage",
                   AsyncMock(return_value=self._fixture_result())):
            result = _run(wq._answer_question("熊澤案件の戸籍は足りてる？"))
        self.assertEqual(result["status"], "ok")
        self.assertIn(wq.FLAG_NOTES["koseki_coverage_estimate"],
                      result["notes"])
        self.assertIn(wq.FLAG_NOTES["koseki_reading"], result["notes"])
        apps = {s["app"] for s in result["sources"]}
        self.assertEqual(apps, {"App33(戸籍読解)", "App36(相続人)"})


if __name__ == "__main__":
    unittest.main()
