"""TRACKING-PREP fix1/fix2: tracking_pg_harness の接続遮断・出力遮断の対照テスト。

固定する仕様（R-TRACKING-PREP-1/-2 対応）:
- fix1 H01: URL は検証済み要素からの再構築。query/fragment（host/hostaddr/service
  等の接続先上書きパラメータ）は全面拒否・拒否文言に URL 値非表示・非ローカル拒否
- fix1 H02: migrate サブコマンド（唯一の alembic 適用経路）——不正 URL では rc 2 で
  alembic 未到達・DATABASE_URL 残置なし
- fix2 H01: 子プロセス出力の構造化 sanitizer——既知 secret（URL 全体・password の
  encoded/decoded/再 quote 全形）の完全一致置換＋DSN 様文字列の汎用置換
  （password マスク済み URL でも username/host/dbname を残さない）
- fix2 M01: Alembic 起動形の AST pin——_run_migrate 内の1回のみ・argv 固定・
  shell 不使用・固定 cwd・検証済み子 env（同ファイル内への起動追加も検出）
- fix1 M01: --rounds は正整数のみ（0/負数は固定文言+rc 2）
- fix1 M02: check ごとの単調時計計時（#N elapsed=..s を出力）
"""

import ast
import io
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch  # subprocess.run は patch 経由でのみ触れる

import tools.tracking_pg_harness as harness
from tools.tracking_pg_harness import (
    HarnessConfigError,
    _run_migrate,
    _sanitize_output,
    _validated_local_url,
    main,
)


class TestUrlReconstruction(unittest.TestCase):
    """fix1 H01: 検証済み要素からの再構築・上書きパラメータの迂回拒否。"""

    def test_rebuilt_from_validated_components(self):
        self.assertEqual(
            _validated_local_url("postgresql://postgres:trk@127.0.0.1:5433/tracking_check"),
            "postgresql://postgres:trk@127.0.0.1:5433/tracking_check")
        # driver 明示・IPv6・user のみ・port なしも再構築で正規形へ
        self.assertEqual(
            _validated_local_url("postgresql+psycopg://u@localhost/db"),
            "postgresql://u@localhost/db")
        self.assertEqual(
            _validated_local_url("postgresql://postgres@[::1]:5433/db"),
            "postgresql://postgres@[::1]:5433/db")

    def test_credentials_requoted_not_passed_verbatim(self):
        rebuilt = _validated_local_url("postgresql://a%40b:p%3Aw@localhost/db")
        self.assertEqual(rebuilt, "postgresql://a%40b:p%3Aw@localhost/db")

    def test_override_params_rejected_without_reflection(self):
        cases = (
            "postgresql://u@localhost/db?host=evil.example.com",
            "postgresql://u@localhost/db?hostaddr=203.0.113.9",
            "postgresql://u@localhost/db?service=prod-railway",
            "postgresql://u@localhost/db?options=-csearch_path%3Dpublic",
            "postgresql://u@localhost/db?sslmode=require",   # 閉集合を置かず全面拒否
            "postgresql://u@localhost/db#fragvalue9",
        )
        for url in cases:
            with self.subTest(url=url.split("?")[-1]):
                with self.assertRaises(HarnessConfigError) as ctx:
                    _validated_local_url(url)
                msg = str(ctx.exception)
                for sentinel in ("evil.example.com", "203.0.113.9",
                                 "prod-railway", "search_path", "fragvalue9"):
                    self.assertNotIn(sentinel, msg)          # URL 値の非表示

    def test_non_local_and_malformed_rejected(self):
        for url in ("postgresql://u@db.railway.internal/db",
                    "postgresql://u@10.0.0.5/db",
                    "postgresql://u@localhost:notaport/db",
                    "postgresql://u@localhost/",             # dbname 空
                    "postgresql://u@localhost/db;evil",      # dbname grammar 外
                    "mysql://u@localhost/db",
                    ""):
            with self.subTest(url=url[:30]):
                with self.assertRaises(HarnessConfigError) as ctx:
                    _validated_local_url(url)
                self.assertNotIn("railway.internal", str(ctx.exception))


class TestCliValidation(unittest.TestCase):
    def test_rounds_must_be_positive(self):
        # fix1 M01: 0/負数は固定文言+rc 2（DB・alembic に未到達）
        for rounds in ("0", "-3"):
            with self.subTest(rounds=rounds):
                buf = io.StringIO()
                rc = main(["--rounds", rounds, "--sqlite-selftest"], out=buf)
                self.assertEqual(rc, 2)
                self.assertIn("--rounds は 1 以上", buf.getvalue())

    def test_run_rejects_bad_env_url_with_rc2(self):
        buf = io.StringIO()
        with patch.dict(os.environ, {"TRACKING_PG_URL":
                                     "postgresql://u@db.railway.app/prod"}):
            rc = main([], out=buf)
        self.assertEqual(rc, 2)
        self.assertIn("config error", buf.getvalue())
        self.assertNotIn("railway.app", buf.getvalue())      # 値の非反射

    def test_migrate_rejects_bad_env_url_and_leaves_no_database_url(self):
        # fix1 H02: 不正 URL では alembic 未到達・env 残置なし
        buf = io.StringIO()
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        env["TRACKING_PG_URL"] = "postgresql://u@localhost/db?host=evil"
        with patch.dict(os.environ, env, clear=True):
            rc = main(["migrate"], out=buf)
            self.assertEqual(rc, 2)
            self.assertNotIn("DATABASE_URL", os.environ)     # 残置なし
        self.assertNotIn("evil", buf.getvalue())


class TestMigrateOutputSanitizer(unittest.TestCase):
    """fix2 H01: 子プロセス出力の構造化 sanitizer の対照。"""

    _URL_RAW = "postgresql://trkuser:p%40ssw0rd@127.0.0.1:5433/tracking_check"

    def test_sanitize_unit_all_secret_forms(self):
        url = _validated_local_url(self._URL_RAW)
        dirty = "\n".join([
            f"conn: {url}",                                    # 完全 URL
            "enc: p%40ssw0rd",                                 # encoded password
            "dec: p@ssw0rd",                                   # decoded password
            "masked: postgresql://trkuser:***@127.0.0.1:5433/tracking_check",
            "other: postgresql+psycopg://foo@localhost/x",     # 別 DSN 様文字列
        ])
        clean = _sanitize_output(dirty, url)
        for sentinel in ("p%40ssw0rd", "p@ssw0rd", "trkuser",
                         "tracking_check", "127.0.0.1:5433"):
            self.assertNotIn(sentinel, clean)
        self.assertIn("<DSN>", clean)                          # 汎用置換の証跡

    def test_migrate_stdout_stderr_both_sanitized(self):
        url = _validated_local_url(self._URL_RAW)
        payload = (f"{url}\np%40ssw0rd p@ssw0rd\n"
                   "postgresql://trkuser:***@127.0.0.1:5433/tracking_check")
        fake = SimpleNamespace(returncode=0, stdout=payload, stderr=payload)
        buf = io.StringIO()
        with patch("subprocess.run", return_value=fake) as mock_run:
            rc = _run_migrate(url, buf)
        self.assertEqual(rc, 0)
        mock_run.assert_called_once()
        text = buf.getvalue()
        for sentinel in ("p%40ssw0rd", "p@ssw0rd", "trkuser", "tracking_check"):
            self.assertNotIn(sentinel, text)
        # 失敗経路も同じ sanitizer を通る
        fake_fail = SimpleNamespace(returncode=3, stdout="", stderr=payload)
        buf2 = io.StringIO()
        with patch("subprocess.run", return_value=fake_fail):
            rc2 = _run_migrate(url, buf2)
        self.assertEqual(rc2, 1)
        self.assertNotIn("p@ssw0rd", buf2.getvalue())
        self.assertIn("exit 3", buf2.getvalue())


# ── process 起動検出（fix3 M01 で走査範囲を完全化・関数化）──────────────────────

_SUBPROC_FUNCS = {"run", "Popen", "call", "check_call", "check_output", "system"}
_LAUNCH_MODULES = {"subprocess", "os"}


def _find_launches_legacy(tree):
    """fix2 時点の検出（**対照用の凍結コピー・変更しない**）。
    FunctionDef 配下限定＋module 名直書き（subprocess./os.）のみ検出する版。"""
    launches = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(func):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)):
                continue
            mod, attr = node.func.value.id, node.func.attr
            if (mod == "subprocess" and attr in _SUBPROC_FUNCS) \
                    or (mod == "os" and attr == "system"):
                launches.append((func.name, node))
    return launches


def _find_process_launches(tree):
    """fix3 M01 の新検出: module 全体を 1 度の walk で走査（module-level 含む）。

    alias 追跡（p4-002 fix2 と同型）: subprocess/os の import alias
    （import subprocess as sp）・from-import（from subprocess import run
    [as r]）・代入 alias（x = subprocess／f = subprocess.run）を固定点まで
    収集し、それら経由の起動呼出しも全て検出する。`from subprocess import *`
    は追跡不能のため検出（違反）として返す。
    Returns: list of (enclosing scope name or "<module>", ast.Call | ast.ImportFrom)
    """
    module_alias = set()
    func_alias = set()
    star_imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.split(".")[0] in _LAUNCH_MODULES:
                    module_alias.add(a.asname or a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] in _LAUNCH_MODULES:
                for a in node.names:
                    if a.name == "*":
                        star_imports.append(node)
                    elif a.name in _SUBPROC_FUNCS:
                        func_alias.add(a.asname or a.name)
    changed = True
    while changed:                       # 代入 alias の固定点反復
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
                continue
            value = getattr(node, "value", None)
            targets = node.targets if isinstance(node, ast.Assign) \
                else [node.target]
            names = [n.id for t in targets for n in ast.walk(t)
                     if isinstance(n, ast.Name)]
            if isinstance(value, ast.Name):
                if value.id in module_alias and not module_alias >= set(names):
                    module_alias.update(names)
                    changed = True
                if value.id in func_alias and not func_alias >= set(names):
                    func_alias.update(names)
                    changed = True
            if isinstance(value, ast.Attribute) \
                    and isinstance(value.value, ast.Name) \
                    and value.value.id in module_alias \
                    and value.attr in _SUBPROC_FUNCS \
                    and not func_alias >= set(names):
                func_alias.update(names)
                changed = True

    launches = [("<module>", node) for node in star_imports]

    def _visit(node, scope):
        for child in ast.iter_child_nodes(node):
            child_scope = child.name if isinstance(
                child, (ast.FunctionDef, ast.AsyncFunctionDef)) else scope
            if isinstance(child, ast.Call):
                f = child.func
                if isinstance(f, ast.Name) and f.id in func_alias:
                    launches.append((scope, child))
                elif isinstance(f, ast.Attribute) \
                        and isinstance(f.value, ast.Name) \
                        and f.value.id in module_alias \
                        and f.attr in _SUBPROC_FUNCS:
                    launches.append((scope, child))
            _visit(child, child_scope)

    _visit(tree, "<module>")
    return launches


class TestMigrateLaunchShape(unittest.TestCase):
    """fix2 M01-(ii)→fix3 M01: Alembic 起動形の AST 構造 pin（走査範囲完全化）。

    許可する起動は「_run_migrate 内の位置＋argv 構造」で一意特定した 1 件のみ。
    それ以外に検出された process 起動（module-level・alias 経由含む）は全て違反。
    """

    def _module_tree(self):
        return ast.parse(Path(harness.__file__).read_text(encoding="utf-8"))

    def _assert_pinned_shape(self, call):
        # argv 固定: [sys.executable, "-m", "alembic", "upgrade", "head"]
        argv = call.args[0]
        self.assertIsInstance(argv, ast.List)
        first = argv.elts[0]
        self.assertIsInstance(first, ast.Attribute)
        self.assertEqual((first.value.id, first.attr), ("sys", "executable"))
        self.assertEqual([e.value for e in argv.elts[1:]],
                         ["-m", "alembic", "upgrade", "head"])
        # keyword 形: shell 不使用・固定 cwd・検証済み子 env
        keywords = {k.arg: k.value for k in call.keywords}
        self.assertNotIn("shell", keywords)
        self.assertIsInstance(keywords["cwd"], ast.Name)
        self.assertEqual(keywords["cwd"].id, "_REPO_ROOT")
        self.assertIsInstance(keywords["env"], ast.Name)
        self.assertEqual(keywords["env"].id, "child_env")

    def test_single_launch_uniquely_identified_all_others_violate(self):
        launches = _find_process_launches(self._module_tree())
        allowed = [(scope, node) for scope, node in launches
                   if scope == "_run_migrate" and isinstance(node, ast.Call)
                   and node.args and isinstance(node.args[0], ast.List)]
        self.assertEqual(len(allowed), 1, "許可起動（_run_migrate 内）は 1 件のみ")
        self._assert_pinned_shape(allowed[0][1])
        others = [x for x in launches if x not in allowed]
        self.assertEqual(others, [], "許可外の process 起動は全て違反")

    def test_meta_bypass_fixtures_old_pass_new_fail(self):
        """fix3 M01-4: 迂回 fixture 3種が「旧検出 PASS・新検出 FAIL」の三段対照。"""
        fixtures = {
            "module_level_run": "import subprocess\nsubprocess.run(['x'])\n",
            "import_alias": ("import subprocess as sp\n"
                             "def f():\n    sp.run(['x'])\n"),
            "from_import_run": ("from subprocess import run\n"
                                "def f():\n    run(['x'])\n"),
        }
        for label, src in fixtures.items():
            with self.subTest(fixture=label):
                tree = ast.parse(src)
                self.assertEqual(_find_launches_legacy(tree), [],
                                 "旧検出は素通り（迂回が実在した証明）")
                self.assertNotEqual(_find_process_launches(tree), [],
                                    "新検出は検出すること")

    def test_d2_exclusion_is_full_path_only(self):
        # fix2 M01-(i): D2 検査の除外が完全 path 限定であること（同名別ファイル遮断）
        import test_db_foundation_hardening as hard
        cls = hard.TestNoDynamicAlembicInvocation
        self.assertIn("tools/tracking_pg_harness.py", cls.EXCLUDED_PATHS)
        self.assertNotIn("tracking_pg_harness.py", cls.EXCLUDED_FILES)


class TestSelftestSmoke(unittest.TestCase):
    def test_selftest_outputs_elapsed_per_check(self):
        # fix1 M02: check ごとの単調時計計時が出力に含まれる（selftest で確認）
        buf = io.StringIO()
        rc = main(["--sqlite-selftest", "--rounds", "1"], out=buf)
        text = buf.getvalue()
        self.assertEqual(rc, 0, text)
        self.assertRegex(text, r"#1 elapsed=\d+\.\ds")
        self.assertRegex(text, r"#2 elapsed=\d+\.\ds")
        self.assertIn("SELFTEST", text)


if __name__ == "__main__":
    unittest.main()
