"""P3 fix3（改定裁定）→fix4 H01: app-state immutable 台帳への Core 文の AST 機械検査。

対象 table: derivation_run / heir_confirmation_decision / template_version。
これらの台帳へ到達しうる DML を、**正規 module（hub/derivation_models.py・
hub/template_registry.py）・alembic migration・当該テストファイル以外**で機械禁止する。

検出経路（fix4 H01 で 4 経路を拡張・名前文字列比較を廃止）:
  (1) module 形:  sa.insert(...) / sqlalchemy.update(...) — **import 表で実体解決**
      （`import sqlalchemy as X` の任意 alias X を追跡）
  (2) from-import 形: `from sqlalchemy import insert [as Y]` — alias Y も実体解決
      （sqlalchemy 由来でない同名関数は対象外＝実体名判定）
  (3) Table メソッド形: `<対象>.__table__.insert()/update()/delete()`・および
      `t = <対象>.__table__` の変数経由（代入追跡）
  (4) raw SQL 形: `sa.text()/text()`（import 解決済み）の文字列リテラル内に
      INSERT/UPDATE/DELETE＋対象 table 名が同時に現れるもの

背景（改定裁定）: JSON payload 検査は DB trigger 化が実用不能・SQLAlchemy に table
レベル Core insert event も無いため、Core 迂回の防御は本 AST 検査＋正規 module 内
ガードの二段とする（旧「迂回成功 pin テスト」は脆弱性目録になるため削除）。
走査型は test_sink_ast_policy を踏襲（git 追跡 *.py 全域・AST）。
"""

import ast
import re
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).parent

_TARGET_TOKENS = ("DerivationRun", "HeirConfirmationDecision", "TemplateVersion",
                  "derivation_run", "heir_confirmation_decision", "template_version")
_DML_NAMES = ("insert", "update", "delete")
_RAW_DML_RE = re.compile(r"\b(INSERT|UPDATE|DELETE)\b", re.I)
_RAW_TABLE_RE = re.compile(
    r"\b(derivation_run|heir_confirmation_decision|template_version)\b")
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


def _import_table(tree: ast.AST) -> tuple[set, dict, set]:
    """import 表の構築（fix4 H01(a)・実体名で判定するための alias 解決）。

    Returns: (sqlalchemy module の alias 集合,
              from-import された DML 関数 {ローカル名: 実体名},
              from-import された text のローカル名集合)
    """
    mods: set[str] = set()
    dml_funcs: dict[str, str] = {}
    text_funcs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for al in node.names:
                if al.name == "sqlalchemy" or al.name.startswith("sqlalchemy."):
                    mods.add(al.asname or al.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == "sqlalchemy" or mod.startswith("sqlalchemy."):
                for al in node.names:
                    local = al.asname or al.name
                    if al.name in _DML_NAMES:
                        dml_funcs[local] = al.name
                    elif al.name == "text":
                        text_funcs.add(local)
    return mods, dml_funcs, text_funcs


def _table_vars(tree: ast.AST) -> set[str]:
    """fix4 H01(b): 対象 table の `__table__` を変数へ束縛する代入を追跡。"""
    out: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        try:
            val_txt = ast.unparse(node.value)
        except Exception:
            continue
        if "__table__" in val_txt and any(tok in val_txt for tok in _TARGET_TOKENS):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    out.add(tgt.id)
    return out


def _string_literal_parts(node: ast.AST) -> str:
    """文字列定数（f-string は定数部のみ）を連結して返す。非文字列は空。"""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value for v in node.values
                       if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return ""


def scan_source(src: str, path: str) -> list[str]:
    """対象 table への Core DML 到達経路を列挙（'path:lineno:<種別>:<name>' 形式）。"""
    out = []
    tree = ast.parse(src)
    mods, dml_funcs, text_funcs = _import_table(tree)
    tvars = _table_vars(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        # (1)(2) module 形／from-import 形（import 表で実体解決・fix4 H01(a)）
        dml = None
        if (isinstance(f, ast.Attribute) and f.attr in _DML_NAMES
                and isinstance(f.value, ast.Name) and f.value.id in mods):
            dml = f.attr
        elif isinstance(f, ast.Name) and f.id in dml_funcs:
            dml = dml_funcs[f.id]
        if dml and node.args:
            arg_txt = ast.unparse(node.args[0])
            if any(tok in arg_txt for tok in _TARGET_TOKENS):
                out.append(f"{path}:{node.lineno}:core_dml:{dml}")
                continue
        # (3) Table メソッド形（fix4 H01(b)・__table__ 直付け or 変数経由）
        if isinstance(f, ast.Attribute) and f.attr in _DML_NAMES:
            try:
                base_txt = ast.unparse(f.value)
            except Exception:
                base_txt = ""
            direct = ("__table__" in base_txt
                      and any(tok in base_txt for tok in _TARGET_TOKENS))
            via_var = isinstance(f.value, ast.Name) and f.value.id in tvars
            if direct or via_var:
                out.append(f"{path}:{node.lineno}:table_method:{f.attr}")
                continue
        # (4) raw SQL 形（fix4 H01(c)・text() 内の DML＋対象 table 名）
        is_text = ((isinstance(f, ast.Attribute) and f.attr == "text"
                    and isinstance(f.value, ast.Name) and f.value.id in mods)
                   or (isinstance(f, ast.Name) and f.id in text_funcs))
        if is_text and node.args:
            lit = _string_literal_parts(node.args[0])
            if lit and _RAW_DML_RE.search(lit) and _RAW_TABLE_RE.search(lit):
                out.append(f"{path}:{node.lineno}:raw_sql:text")
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
    """意図的違反 fixture での FAIL（検出）実測。fix4 H01(d): 4→8 種。"""

    def test_deliberate_violations_detected(self):
        cases = {
            # fix3 からの 4 種
            "sa_insert_table": ("import sqlalchemy as sa\n"
                                "sa.insert(DerivationRun.__table__)\n", "core_dml"),
            "sqlalchemy_update": ("import sqlalchemy\n"
                                  "sqlalchemy.update(HeirConfirmationDecision)\n",
                                  "core_dml"),
            "from_import_delete": ("from sqlalchemy import delete\n"
                                   "delete(TemplateVersion.__table__)\n", "core_dml"),
            "metadata_tables_ref": ("import sqlalchemy as sa\n"
                                    "sa.insert(meta.tables['template_version'])\n",
                                    "core_dml"),
            # fix4 H01 の 4 種（残余経路）
            "alias_module_insert": ("import sqlalchemy as db\n"
                                    "db.insert(DerivationRun.__table__)\n",
                                    "core_dml"),
            "from_import_alias_update": ("from sqlalchemy import update as up\n"
                                         "up(HeirConfirmationDecision)\n", "core_dml"),
            "table_method_direct": ("DerivationRun.__table__.insert()\n",
                                    "table_method"),
            "raw_text_delete": ("import sqlalchemy as sa\n"
                                "sa.text('DELETE FROM template_version "
                                "WHERE id=1')\n", "raw_sql"),
        }
        for label, (src, kind) in cases.items():
            with self.subTest(case=label):
                found = scan_source(src, "fx.py")
                self.assertEqual(len(found), 1, (label, found))
                self.assertIn(kind, found[0], label)

    def test_table_var_indirection_detected(self):
        """(b) 変数経由: t = <対象>.__table__ → t.delete() も検出。"""
        src = ("t = TemplateVersion.__table__\n"
               "t.delete()\n")
        found = scan_source(src, "fx.py")
        self.assertEqual(len(found), 1, found)
        self.assertIn("table_method:delete", found[0])

    def test_safe_patterns_not_flagged(self):
        cases = {
            # fix3 からの safe 4 種
            "other_table": "import sqlalchemy as sa\n"
                           "sa.insert(InboundEvent.__table__)\n",
            "select_is_ok": "import sqlalchemy as sa\n"
                            "sa.select(DerivationRun)\n",
            "orm_session_delete": "await s.delete(obj)\n",
            "unrelated_update": "d.update({'k': 1})\n",
            # fix4 safe 拡張
            "bare_insert_not_sqlalchemy": (   # 実体名判定: sqlalchemy 由来でない insert
                "def insert(x):\n    return x\n"
                "insert(DerivationRun)\n"),
            "alias_select": "import sqlalchemy as db\n"
                            "db.select(DerivationRun)\n",
            "text_other_table": "import sqlalchemy as sa\n"
                                "sa.text('DELETE FROM inbound_event')\n",
            "text_select_only": "from sqlalchemy import text\n"
                                "text('SELECT * FROM derivation_run')\n",
            "dict_update_token_arg": "cfg.update({'template_version': 2})\n",
            "other_table_var_method": "t = InboundEvent.__table__\nt.insert()\n",
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
            "template_registry）・migration・当該テスト以外で禁止（fix3 改定裁定・"
            "fix4 H01 経路拡張）:\n" + "\n".join(violations))

    def test_scan_reaches_enough_files(self):
        self.assertGreater(len(list(_tracked_py())), 10)


if __name__ == "__main__":
    unittest.main()
