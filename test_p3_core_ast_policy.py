"""P3 fix3（改定裁定）→fix4 H01→fix5 P30015-H01: app-state immutable 台帳への
Core 文の AST 機械検査。

対象 table: derivation_run / heir_confirmation_decision / template_version。
これらの台帳へ到達しうる DML を、**正規 module（hub/derivation_models.py・
hub/template_registry.py）・alembic migration・当該テストファイル以外**で機械禁止する。

検出対象（fix5 で Codex 列挙①〜⑦へ拡張・全て静的検出可能な通常 SQLAlchemy 記法）:
  (1)(2) module 形／from-import 形（import 表で実体解決・任意 alias）
  (3) Table メソッド形: `<対象>.__table__.insert()`・`metadata.tables["<対象>"]` の
      添字アクセス（②）・変数経由は**代入グラフの推移閉包**（③ 二段以上の alias）で追跡
  (4) raw SQL 形: `sa.text()/text()`・**`conn.exec_driver_sql()`（⑥・メソッド名検出）**の
      文字列引数に INSERT/UPDATE/DELETE＋対象 table 名。文字列は **BinOp(+) の定数
      畳み込み（④）＋文字列定数変数の代入追跡（⑤）＋JoinedStr（f-string）の
      リテラル部分連結（fix6 P30016-M01）**まで解決する。f-string の式部分
      （`{...}`）は**不明として扱い、リテラル部分のみで判定**する（リテラル部分に
      DML＋対象 table 名が揃えば検出・式部分にしか table 名が無い場合は検出しない）
  (5) 関数 alias（①）: `ins = sa.insert`／`from sqlalchemy import insert as X` の
      変数代入形（関数オブジェクトの代入追跡・推移閉包）
  (6) `getattr(sa, "insert")(...)`（⑦・第2引数が文字列リテラルの getattr を解決）

## 受容一覧（fix5 司令塔裁定・検出対象外の明示列挙＝規律・レビュー委任）
以下の**真に動的な経路**は静的検出の対象外とし、コーディング規律と Codex/[人]
レビューに委任する（本 scanner は検出しない）:
  - exec / eval による実行時コード生成
  - __import__ / importlib による動的 import
  - getattr(obj, 変数) — 属性名が文字列リテラルでない getattr
  - 実行時文字列組立で**定数伝播が追えない**もの（関数戻り値・引数・外部入力に
    依存する SQL 文字列 等）。※f-string は fix6 で**検出対象へ移動**——リテラル
    部分を連結して検査し、式部分のみを不明として扱う（上記 (4)）

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


def _fold_str(node: ast.AST, str_consts: dict) -> str | None:
    """文字列定数の畳み込み（fix5 ④⑤・fix6 M01）: Constant／BinOp(+) 連結／
    定数文字列変数の参照／**JoinedStr（f-string）はリテラル部分のみ連結**
    （式部分は不明として無視・リテラル部分だけで DML＋table 名判定に掛ける）。
    BinOp で畳めない部分を含む連結は None（＝受容一覧の動的経路）。"""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return str_consts.get(node.id)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _fold_str(node.left, str_consts)
        right = _fold_str(node.right, str_consts)
        if left is not None and right is not None:
            return left + right
        return None
    if isinstance(node, ast.JoinedStr):
        return "".join(v.value for v in node.values
                       if isinstance(v, ast.Constant) and isinstance(v.value, str))
    return None


def _resolve_dml_func(node: ast.AST, mods: set, dml_funcs: dict,
                      func_vars: dict) -> str | None:
    """式が sqlalchemy の DML 関数オブジェクトに解決できるか（fix5 ①⑦）。"""
    if (isinstance(node, ast.Attribute) and node.attr in _DML_NAMES
            and isinstance(node.value, ast.Name) and node.value.id in mods):
        return node.attr                      # sa.insert（関数オブジェクト参照）
    if isinstance(node, ast.Name):
        if node.id in dml_funcs:
            return dml_funcs[node.id]         # from-import 名
        if node.id in func_vars:
            return func_vars[node.id]         # 既知の関数 alias（推移閉包）
    if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
            and node.func.id == "getattr" and len(node.args) >= 2
            and isinstance(node.args[0], ast.Name) and node.args[0].id in mods
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in _DML_NAMES):
        return node.args[1].value             # getattr(sa, "insert")（⑦・literal のみ）
    return None


def _is_table_expr(node: ast.AST, table_vars: set) -> bool:
    """式が対象 table オブジェクトに解決できるか（fix5 ②③）。
    `__table__` 属性・`metadata.tables[...]` 添字＋対象 token、または既知 table 変数。"""
    if isinstance(node, ast.Name):
        return node.id in table_vars          # 推移閉包（u = t）
    try:
        txt = ast.unparse(node)
    except Exception:
        return False
    return (("__table__" in txt or ".tables[" in txt)
            and any(tok in txt for tok in _TARGET_TOKENS))


def _assignment_analysis(tree: ast.AST, mods: set, dml_funcs: dict
                         ) -> tuple[set, dict, dict]:
    """代入グラフの推移閉包（fix5 ①②③⑤）: 対象 table 変数・DML 関数 alias 変数・
    文字列定数変数を fixpoint まで伝播する。

    追跡対象は**モジュール内で 1 回だけ代入される変数**に限定する（再代入・
    自己参照蓄積 `x = x + ...` は「実行時組立」＝受容一覧の動的経路として追跡
    しない）。単一代入なら各変数は未解決→解決の一方向にしか動かないため、
    fixpoint は代入数に比例して必ず停止する。"""
    table_vars: set[str] = set()
    func_vars: dict[str, str] = {}
    str_consts: dict[str, str] = {}
    assigns = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)]
    counts: dict[str, int] = {}
    for node in assigns:
        for t in node.targets:
            if isinstance(t, ast.Name):
                counts[t.id] = counts.get(t.id, 0) + 1
    changed = True
    while changed:
        changed = False
        for node in assigns:
            targets = [t.id for t in node.targets
                       if isinstance(t, ast.Name) and counts[t.id] == 1]
            targets = [t for t in targets
                       if t not in str_consts and t not in func_vars
                       and t not in table_vars]
            if not targets:
                continue
            s = _fold_str(node.value, str_consts)
            if s is not None:
                for t in targets:
                    str_consts[t] = s
                changed = True
                continue
            fn = _resolve_dml_func(node.value, mods, dml_funcs, func_vars)
            if fn is not None:
                for t in targets:
                    func_vars[t] = fn
                changed = True
                continue
            if _is_table_expr(node.value, table_vars):
                table_vars.update(targets)
                changed = True
    return table_vars, func_vars, str_consts


def scan_source(src: str, path: str) -> list[str]:
    """対象 table への Core DML 到達経路を列挙（'path:lineno:<種別>:<name>' 形式）。"""
    out = []
    tree = ast.parse(src)
    mods, dml_funcs, text_funcs = _import_table(tree)
    table_vars, func_vars, str_consts = _assignment_analysis(tree, mods, dml_funcs)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        # (1)(2)(5)(6): module 形／from-import 形／関数 alias 変数／getattr 解決
        dml = None
        if (isinstance(f, ast.Attribute) and f.attr in _DML_NAMES
                and isinstance(f.value, ast.Name) and f.value.id in mods):
            dml = f.attr
        elif isinstance(f, ast.Name) and f.id in dml_funcs:
            dml = dml_funcs[f.id]
        elif isinstance(f, ast.Name) and f.id in func_vars:
            dml = func_vars[f.id]
        elif isinstance(f, ast.Call):
            dml = _resolve_dml_func(f, mods, dml_funcs, func_vars)
        if dml and node.args:
            arg = node.args[0]
            arg_txt = ast.unparse(arg)
            arg_is_table = (any(tok in arg_txt for tok in _TARGET_TOKENS)
                            or (isinstance(arg, ast.Name) and arg.id in table_vars))
            if arg_is_table:
                out.append(f"{path}:{node.lineno}:core_dml:{dml}")
                continue
        # (3): Table メソッド形（__table__ 直付け／metadata.tables 添字／変数経由）
        if isinstance(f, ast.Attribute) and f.attr in _DML_NAMES:
            if _is_table_expr(f.value, table_vars):
                out.append(f"{path}:{node.lineno}:table_method:{f.attr}")
                continue
        # (4): raw SQL 形 — text()（畳み込み④・定数変数⑤）＋ exec_driver_sql（⑥）
        is_text = ((isinstance(f, ast.Attribute) and f.attr == "text"
                    and isinstance(f.value, ast.Name) and f.value.id in mods)
                   or (isinstance(f, ast.Name) and f.id in text_funcs))
        is_driver = isinstance(f, ast.Attribute) and f.attr == "exec_driver_sql"
        if (is_text or is_driver) and node.args:
            lit = _fold_str(node.args[0], str_consts)
            if lit and _RAW_DML_RE.search(lit) and _RAW_TABLE_RE.search(lit):
                kind = "exec_driver_sql" if is_driver else "text"
                out.append(f"{path}:{node.lineno}:raw_sql:{kind}")
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
    """意図的違反 fixture での FAIL（検出）実測。fix4: 4→8 種・fix5 ①〜⑦: 8→15 種。"""

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
            # fix4 H01 の 4 種
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
            # fix5 P30015-H01 ①〜⑦ の 7 種
            "func_alias_assign": ("import sqlalchemy as sa\n"                # ①
                                  "ins = sa.insert\n"
                                  "ins(DerivationRun.__table__)\n", "core_dml"),
            "from_import_func_var": ("from sqlalchemy import insert as X\n"  # ①'
                                     "y = X\n"
                                     "y(HeirConfirmationDecision)\n", "core_dml"),
            "metadata_tables_var": ("t = meta.tables['derivation_run']\n"    # ②
                                    "t.insert()\n", "table_method"),
            "two_step_table_alias": ("t = DerivationRun.__table__\n"         # ③
                                     "u = t\n"
                                     "u.insert()\n", "table_method"),
            "text_concat": ("import sqlalchemy as sa\n"                      # ④
                            "sa.text('DELETE FROM ' + 'derivation_run')\n",
                            "raw_sql"),
            "const_sql_var": ("from sqlalchemy import text\n"                # ⑤
                              "SQL = 'DELETE FROM derivation_run'\n"
                              "text(SQL)\n", "raw_sql"),
            "exec_driver_sql": ("conn.exec_driver_sql("                      # ⑥
                                "'DELETE FROM template_version')\n",
                                "raw_sql:exec_driver_sql"),
            "getattr_literal": ("import sqlalchemy as sa\n"                  # ⑦
                                "getattr(sa, 'insert')"
                                "(DerivationRun.__table__)\n", "core_dml"),
            # fix6 P30016-M01: f-string のリテラル部分連結（式部分は不明扱い）
            "exec_driver_fstring": (
                "conn.exec_driver_sql("
                "f'DELETE FROM derivation_run WHERE id={x}')\n",
                "raw_sql:exec_driver_sql"),
            "text_fstring": ("import sqlalchemy as sa\n"
                             "sa.text(f'UPDATE template_version "
                             "SET status={s}')\n", "raw_sql:text"),
        }
        self.assertGreaterEqual(len(cases), 15)   # fix5: 8→15 種以上を維持
        for label, (src, kind) in cases.items():
            with self.subTest(case=label):
                found = scan_source(src, "fx.py")
                self.assertEqual(len(found), 1, (label, found))
                self.assertIn(kind, found[0], label)

    def test_table_var_indirection_detected(self):
        """fix4(b) 変数経由: t = <対象>.__table__ → t.delete() も検出。"""
        src = ("t = TemplateVersion.__table__\n"
               "t.delete()\n")
        found = scan_source(src, "fx.py")
        self.assertEqual(len(found), 1, found)
        self.assertIn("table_method:delete", found[0])

    def test_func_var_with_table_var_arg_detected(self):
        """fix5 ①×③ 複合: 関数 alias に推移閉包の table 変数を渡す形も検出。"""
        src = ("import sqlalchemy as sa\n"
               "ins = sa.insert\n"
               "t = DerivationRun.__table__\n"
               "u = t\n"
               "ins(u)\n")
        found = scan_source(src, "fx.py")
        self.assertEqual(len(found), 1, found)
        self.assertIn("core_dml:insert", found[0])

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
            "bare_insert_not_sqlalchemy": (
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
            # fix5 safe 拡張（受容一覧の動的経路・他 table・非 DML）
            "getattr_dynamic_name": (      # 受容: getattr(・, 変数) は規律委任
                "import sqlalchemy as sa\n"
                "n = get_name()\n"
                "getattr(sa, n)(DerivationRun.__table__)\n"),
            "func_alias_select": "import sqlalchemy as sa\n"
                                 "sel = sa.select\n"
                                 "sel(DerivationRun)\n",
            "const_sql_other_table": "from sqlalchemy import text\n"
                                     "SQL = 'DELETE FROM inbound_event'\n"
                                     "text(SQL)\n",
            "exec_driver_sql_select": "conn.exec_driver_sql("
                                      "'SELECT count(*) FROM derivation_run')\n",
            "two_step_other_table": "t = InboundEvent.__table__\n"
                                    "u = t\nu.insert()\n",
            "runtime_built_sql": (         # 受容: 定数伝播が追えない実行時組立
                "from sqlalchemy import text\n"
                "text('DELETE FROM ' + table_name())\n"),
            "fstring_table_in_expr_only": (   # fix6: table 名が式部分のみ＝不明扱い
                "conn.exec_driver_sql(f'DELETE FROM {table}')\n"),
            "fstring_no_dml_literal": (       # fix6: リテラル部分に DML が無い
                "import sqlalchemy as sa\n"
                "sa.text(f'{op} FROM derivation_run')\n"),
        }
        for label, src in cases.items():
            with self.subTest(case=label):
                self.assertEqual(scan_source(src, "fx.py"), [], label)


class TestAcceptedDynamicPathsDocumented(unittest.TestCase):
    """fix5: 受容一覧（検出対象外の動的経路）が scanner docstring に固定されていること。"""

    def test_docstring_lists_accepted_dynamic_paths(self):
        import test_p3_core_ast_policy as scanner
        doc = scanner.__doc__ or ""
        self.assertIn("受容一覧", doc)
        for item in ("exec", "eval", "__import__", "getattr(obj, 変数)",
                     "定数伝播が追えない", "規律", "レビュー"):
            self.assertIn(item, doc, f"受容一覧に {item!r} の明示がない")


class TestRepoWide(unittest.TestCase):
    def test_no_core_dml_outside_canonical_modules(self):
        violations, errors = scan_repo()
        self.assertEqual(errors, [], "AST 走査で解析失敗:\n" + "\n".join(errors))
        self.assertEqual(
            violations, [],
            "app-state 台帳への Core DML は正規 module（derivation_models/"
            "template_registry）・migration・当該テスト以外で禁止（fix3 改定裁定・"
            "fix4/fix5 経路拡張）:\n" + "\n".join(violations))

    def test_scan_reaches_enough_files(self):
        self.assertGreater(len(list(_tracked_py())), 10)


if __name__ == "__main__":
    unittest.main()
