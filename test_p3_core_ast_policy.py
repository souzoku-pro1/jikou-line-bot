"""P3 fix3（改定裁定）: app-state immutable 台帳への Core 文の AST 機械検査。

対象 table: derivation_run / heir_confirmation_decision / template_version。
`sa.insert / sa.update / sa.delete`（および from-import 形）でこれらの台帳を参照する
Core 文を、**正規 module（hub/derivation_models.py・hub/template_registry.py）・
alembic migration・当該テストファイル以外**で機械禁止する。

背景（改定裁定）: JSON payload 検査は DB trigger 化が実用不能・SQLAlchemy に table
レベル Core insert event も無いため、Core 迂回の防御は本 AST 検査＋正規 module 内
ガードの二段とする（旧「迂回成功 pin テスト」は脆弱性目録になるため削除）。
走査型は test_sink_ast_policy を踏襲（git 追跡 *.py 全域・AST）。
"""

import ast
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).parent

_TARGET_TOKENS = ("DerivationRun", "HeirConfirmationDecision", "TemplateVersion",
                  "derivation_run", "heir_confirmation_decision", "template_version")
_DML_NAMES = ("insert", "update", "delete")
# 正規 module・当該テスト（Core 文を検査対象 table に使ってよい場所）
_ALLOWED = {
    "hub/derivation_models.py",
    "hub/template_registry.py",
    "test_p3_001_derivation_models.py",
    "test_p3_001_frozen_cases.py",
    "test_p3_002_template_version.py",
    "test_p3_core_ast_policy.py",
}
_ALLOWED_PREFIXES = ("alembic/versions/",)


def _dml_name(call: ast.Call) -> str | None:
    f = call.func
    if isinstance(f, ast.Attribute) and f.attr in _DML_NAMES:
        base = f.value
        if isinstance(base, ast.Name) and base.id in ("sa", "sqlalchemy"):
            return f.attr
        return None   # s.delete(obj) 等の ORM/他オブジェクトは対象外（ORM listener が担保）
    if isinstance(f, ast.Name) and f.id in _DML_NAMES:
        return f.id   # from sqlalchemy import insert 形
    return None


def scan_source(src: str, path: str) -> list[str]:
    """対象 table への Core DML を列挙（'path:lineno:core_dml:<name>' 形式）。"""
    out = []
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _dml_name(node) and node.args):
            continue
        arg_txt = ast.unparse(node.args[0])
        if any(tok in arg_txt for tok in _TARGET_TOKENS):
            out.append(f"{path}:{node.lineno}:core_dml:{_dml_name(node)}")
    return out


def _tracked_py():
    out = subprocess.run(["git", "ls-files", "*.py"], capture_output=True,
                         text=True, check=True, cwd=REPO).stdout
    for line in out.splitlines():
        if not line:
            continue
        if line in _ALLOWED or line.startswith(_ALLOWED_PREFIXES):
            continue
        yield line


def scan_repo():
    violations, errors = [], []
    for path in _tracked_py():
        try:
            src = (REPO / path).read_text(encoding="utf-8")
            violations.extend(scan_source(src, path))
        except Exception as e:
            errors.append(f"{path}: {type(e).__name__}")
    return sorted(violations), sorted(errors)


class TestScannerFixtures(unittest.TestCase):
    """意図的違反 fixture での FAIL（検出）実測。"""

    def test_deliberate_violations_detected(self):
        cases = {
            "sa_insert_table": "import sqlalchemy as sa\n"
                               "sa.insert(DerivationRun.__table__)\n",
            "sqlalchemy_update": "import sqlalchemy\n"
                                 "sqlalchemy.update(HeirConfirmationDecision)\n",
            "from_import_delete": "from sqlalchemy import delete\n"
                                  "delete(TemplateVersion.__table__)\n",
            "metadata_tables_ref": "import sqlalchemy as sa\n"
                                   "sa.insert(meta.tables['template_version'])\n",
        }
        for label, src in cases.items():
            with self.subTest(case=label):
                found = scan_source(src, "fx.py")
                self.assertEqual(len(found), 1, (label, found))
                self.assertIn("core_dml", found[0])

    def test_safe_patterns_not_flagged(self):
        cases = {
            "other_table": "import sqlalchemy as sa\n"
                           "sa.insert(InboundEvent.__table__)\n",
            "select_is_ok": "import sqlalchemy as sa\n"
                            "sa.select(DerivationRun)\n",
            "orm_session_delete": "await s.delete(obj)\n",
            "unrelated_update": "d.update({'k': 1})\n",
        }
        for label, src in cases.items():
            with self.subTest(case=label):
                self.assertEqual(scan_source(src, "fx.py"), [], label)


class TestRepoWide(unittest.TestCase):
    def test_no_core_dml_outside_canonical_modules(self):
        violations, errors = scan_repo()
        self.assertEqual(errors, [], "AST 走査で解析失敗:\n" + "\n".join(errors))
        self.assertEqual(
            violations, [],
            "app-state 台帳への Core DML は正規 module（derivation_models/"
            "template_registry）・migration・当該テスト以外で禁止（fix3 改定裁定）:\n"
            + "\n".join(violations))

    def test_scan_reaches_enough_files(self):
        self.assertGreater(len(list(_tracked_py())), 10)


if __name__ == "__main__":
    unittest.main()
