"""P3-001 fix3→fix4 接続テスト: 凍結 47 ケース全数を「導出→payload 変換→validate→保存」まで通す。

- 凍結 fixture は test_heir_derivation.py（正解仕様 09-heir-test-cases.md v0.1・
  弁護士承認・凍結・47 ケース）を**そのまま実行**し、derive_heirs の全戻り値を spy で
  捕捉する（fixture の複製を作らない＝凍結の単一ソース維持）。
- fix4 M01: 捕捉は**凍結テストID単位**で行い、テストID集合の完全一致を assert する
  （>=47 の件数条件を廃止）。fixture の記号 ID（App34 `$id` の stand-in）は
  **入力側の明示 adapter** で数字 ID へ写像してから導出器を再実行する。
  導出器出力に予期しない記号 ID が現れた場合は grammar（validate）が FAIL させる。
- fix4 H02（裁定）: 胎児 ID は build_run_payload が `胎児:F{n}`（run 内出現順連番）へ
  写像する。凍結の胎児ケースが変換後も保存到達し、役割語ラベルが保存されないことを検証。
- fix4 L01: flag リテラルの機械抽出は regex ではなく **AST literal 抽出**で行う。
"""

import ast
import asyncio
import copy
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import hub.db as db
from hub.derivation_models import (DerivationBase, LAWYER_FLAG_KEYS,
                                   build_run_payload, create_derivation_run,
                                   flag_key, validate_lawyer_flags,
                                   validate_result_payload)

# 凍結 47 行の正解仕様クラス（09-heir-test-cases.md v0.1 の A13+B8+C5+D5+E6+F6+G4）。
# TestNegatives／TestZ1GateScope は「追加検証」（凍結行の外・docstring 記載）。
_FROZEN_CLASS_PREFIXES = ("TestA_", "TestB_", "TestC_", "TestD_",
                          "TestE_", "TestF_", "TestG_")
_FROZEN_METHOD_RE = re.compile(r"^test_[A-G][0-9]+_")


def _run(coro):
    return asyncio.run(coro)


def _iter_tests(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from _iter_tests(item)
        else:
            yield item


def _adapt_inputs(args: tuple, kwargs: dict) -> tuple[list, dict]:
    """fix4 M01: 凍結 fixture の記号 ID → 数字 ID の**入力側**明示 adapter。

    - 写像対象: HeirPerson.record_id／father_id／mother_id／adoptive_*_id・
      Declarations.renounced／disqualified／adoption_kinds キー・decedent_id 引数
    - 数字 ID・導出器生成の `胎児:` ID は写像しない（実形式のまま）
    - 入力側で写像するため、**導出器出力に記号 ID が残っていれば grammar
      （validate_result_payload）が FAIL させる**（出力側の黙変換をしない向き）
    """
    a = [copy.deepcopy(x) for x in args]
    k = {key: copy.deepcopy(v) for key, v in kwargs.items()}
    idmap: dict[str, str] = {}

    def m(v: str) -> str:
        if not v or v.isdigit() or v.startswith("胎児:"):
            return v
        # 既存の数字 ID（G1 実データ形の "6"/"7" 等）と衝突しない帯へ写像
        return idmap.setdefault(v, str(9000000 + len(idmap) + 1))

    persons = a[0]
    for p_ in persons:
        p_.record_id = m(p_.record_id)
        p_.father_id = m(p_.father_id)
        p_.mother_id = m(p_.mother_id)
        p_.adoptive_father_id = m(p_.adoptive_father_id)
        p_.adoptive_mother_id = m(p_.adoptive_mother_id)
    decl = a[1] if len(a) > 1 else k.get("declarations")
    if decl is not None:
        decl.renounced = {m(x) for x in decl.renounced}
        decl.disqualified = {m(x) for x in decl.disqualified}
        decl.adoption_kinds = {m(kk): vv for kk, vv in decl.adoption_kinds.items()}
    if len(a) > 3:
        a[3] = m(a[3])
    if k.get("decedent_id"):
        k["decedent_id"] = m(k["decedent_id"])
    return a, k


class TestFrozenCasesEndToEnd(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3frozen_")
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

    def test_all_frozen_cases_convert_validate_and_persist(self):
        import heir_derivation
        import test_heir_derivation as frozen

        tests = list(_iter_tests(unittest.TestLoader().loadTestsFromModule(frozen)))
        frozen_ids = {
            t._testMethodName for t in tests
            if type(t).__name__.startswith(_FROZEN_CLASS_PREFIXES)
            and _FROZEN_METHOD_RE.match(t._testMethodName)}
        self.assertEqual(len(frozen_ids), 47,
                         f"凍結 47 行（A13+B8+C5+D5+E6+F6+G4）と不一致: {sorted(frozen_ids)}")

        # fix4 M01: 凍結テストID単位で spy 捕捉（テスト⇔Derivation の対応表を作る）
        real = heir_derivation.derive_heirs

        def _make_spy(calls: list):
            def spy(*a, **k):
                r = real(*a, **k)
                calls.append((a, k))
                return r
            return spy

        by_test: dict[str, list] = {}
        for t in tests:
            calls: list = []
            spy = _make_spy(calls)
            result = unittest.TestResult()
            with patch.object(heir_derivation, "derive_heirs", spy), \
                 patch.object(frozen, "derive_heirs", spy):
                t.run(result)
            self.assertTrue(result.wasSuccessful(),
                            f"凍結ケース自体が FAIL: {t.id()} "
                            f"{result.failures[:1]}{result.errors[:1]}")
            by_test.setdefault(t._testMethodName, []).extend(calls)

        # 対応表の完全一致: 全凍結テストが導出を実行し捕捉されている（件数条件でなく集合）
        captured_frozen = {tid for tid in frozen_ids if by_test.get(tid)}
        self.assertEqual(captured_frozen, frozen_ids,
                         "凍結テストID⇔捕捉Derivation の対応が欠落: "
                         f"{sorted(frozen_ids - captured_frozen)}")

        # 入力側 adapter → 導出器（凍結・無改変）再実行 → 変換 → validate → 保存
        fetus_ids_seen: set[str] = set()
        for tid in sorted(frozen_ids):
            for j, (a, k) in enumerate(by_test[tid]):
                aa, kk = _adapt_inputs(a, k)
                deriv = real(*aa, **kk)
                payload, flags = build_run_payload(deriv)   # 氏名・胎児ラベルはここで落ちる
                validate_result_payload(payload)   # 記号ID残存＝予期しない出力→ここで FAIL
                validate_lawyer_flags(flags)
                for h in payload["heirs"]:
                    pid = h["person_id"]
                    if pid.startswith("胎児:"):
                        fetus_ids_seen.add(pid)
                        # fix4 H02: 合成 ID 形式のみ（役割語の自由文字列を保存しない）
                        self.assertRegex(pid, r"^胎児:F[0-9]+$", (tid, pid))
                pk = _run(create_derivation_run(
                    case_app_id="26", case_record_id=f"R-{tid}-{j}",
                    decedent_person_id="0", at_date="2026-01-01",
                    frozen_case_version="v0.1",
                    input_person_revisions={}, input_person_ids=[],
                    input_hash=f"{tid}-{j}", status=deriv.status,
                    rank=deriv.rank, result_payload=payload,
                    result_hash=f"rh-{tid}-{j}", lawyer_flags=flags,
                    provisional=deriv.provisional, engine_version="hd-frozen"))
                db.reset_for_tests()
                self.assertIsInstance(pk, int)

        # fix4 H02: 胎児ケース（F3 系）が変換後も保存到達し、連番の起点が F1 であること
        self.assertTrue(fetus_ids_seen, "胎児ケース（F3）が接続経路を通っていない")
        self.assertIn("胎児:F1", fetus_ids_seen)

    def test_flag_mapping_covers_all_engine_flags(self):
        # fix4 L01: heir_derivation ソースの flag リテラルを AST から機械抽出
        # （regex 置換）。形式は 2 系統:
        #   (i) {"flag": <定数>} の dict リテラル（`suji_hold.flag or "数次"` の
        #       BoolOp fallback 定数を含む）
        #   (ii) _Hold(reason, "X9") の第 2 引数（保留フラグ）
        literals = _engine_flag_literals()
        self.assertGreaterEqual(len(literals), 10, literals)
        mapped = {flag_key(fl) for fl in literals}   # 未写像なら PayloadPolicyError
        self.assertLessEqual(mapped, LAWYER_FLAG_KEYS,
                             "写像結果が enum 外（enum と変換関数がずれている）")


def _engine_flag_literals() -> set[str]:
    """fix4 L01: heir_derivation.py の AST から flag リテラル全数を抽出。"""
    tree = ast.parse(Path(__file__).with_name("heir_derivation.py")
                     .read_text(encoding="utf-8"))
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            for key, val in zip(node.keys, node.values):
                if not (isinstance(key, ast.Constant) and key.value == "flag"):
                    continue
                if isinstance(val, ast.Constant) and isinstance(val.value, str) \
                        and val.value:
                    out.add(val.value)
                elif isinstance(val, ast.BoolOp):   # 例: suji_hold.flag or "数次"
                    for opnd in val.values:
                        if isinstance(opnd, ast.Constant) \
                                and isinstance(opnd.value, str) and opnd.value:
                            out.add(opnd.value)
        elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
              and node.func.id == "_Hold" and len(node.args) >= 2):
            flag_arg = node.args[1]
            if isinstance(flag_arg, ast.Constant) \
                    and isinstance(flag_arg.value, str) and flag_arg.value:
                out.add(flag_arg.value)
    return out


if __name__ == "__main__":
    unittest.main()
