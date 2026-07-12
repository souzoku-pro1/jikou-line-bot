"""sink 出力の AST 方針検査（P1-101 / P1-101a・DRAFT §5 の土台）

検査対象（アプリ本体 *.py・test_/legacy/alembic 除外）:
- 直接出力の sink 境界（H01）: `print`（別名代入・別名 import 経由含む）/
  `sys.stdout.write`・`sys.stderr.write` / module 直呼びの `logging.<level>(...)`。
- raw error/PII 迂回（H02）: `print` / logger 系（`logger.<level>` / `logger.log`）/
  `HTTPException`（`from fastapi import HTTPException as X` の別名・`fastapi.HTTPException`
  の module 修飾を含む）の **引数内** に、`str()` / `repr()` / `.format()` / `"%s" % x` /
  f-string の変数埋め込み / `.text` / `.content` / `.json()` を検出。
  過検知は絞らず全検出し、現状分は allowlist に凍結（台帳が増えるのは現状債務の可視化）。

移行期は **許可リスト方式**（redaction_sink_allowlist.json）:
- entries に無い **新規違反はゼロ**（新規追加を阻止）。
- 総数は baseline_count を **上限**（単調減少のみ許可・S1〜S4 で減らす）。
- H03: parse/read 失敗は**黙殺せずテスト失敗**にする。

この許可リストが S1〜S4 切替の作業台帳そのものである。
"""

import ast
import json
import subprocess
import unittest
from pathlib import Path

REPO = Path(__file__).parent
ALLOWLIST_PATH = REPO / "redaction_sink_allowlist.json"

EXCLUDE_PREFIXES = ("legacy/", "alembic/")
EXCLUDE_NAMES = {"conftest.py", "redact.py", "test_redact.py",
                 "test_sink_ast_policy.py"}

_LOGGER_LEVELS = {"debug", "info", "warning", "error", "exception", "critical"}


# ══════════════════════════════════════════════════════════════
# スキャナ本体（source → 違反リスト）。fixture テストのため関数化。
# ══════════════════════════════════════════════════════════════

def _print_aliases_and_httpexc(tree: ast.AST):
    """ファイル内の print 別名・HTTPException 別名を収集する"""
    print_aliases = set()
    httpexc_names = {"HTTPException"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                local = a.asname or a.name
                if a.name == "print":            # from builtins import print as p
                    print_aliases.add(local)
                if a.name == "HTTPException":     # from fastapi import HTTPException as X
                    httpexc_names.add(local)
        elif isinstance(node, ast.Assign):
            # p = print
            if isinstance(node.value, ast.Name) and node.value.id == "print":
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        print_aliases.add(tgt.id)
    return print_aliases, httpexc_names


def _is_stdio_write(func: ast.AST) -> bool:
    """sys.stdout.write / sys.stderr.write を検出（stdout/stderr.write でも可）"""
    if not (isinstance(func, ast.Attribute) and func.attr == "write"):
        return False
    v = func.value
    return isinstance(v, ast.Attribute) and v.attr in ("stdout", "stderr")


def _is_logging_module_level(func: ast.AST) -> bool:
    """logging.<level>(...) の module 直呼び（logger インスタンスでない）"""
    return (isinstance(func, ast.Attribute) and func.attr in _LOGGER_LEVELS
            and isinstance(func.value, ast.Name) and func.value.id == "logging")


def _arg_has_raw(node: ast.AST) -> bool:
    """引数 subtree に raw error/PII 迂回パターンが含まれるか"""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Call):
            f = sub.func
            if isinstance(f, ast.Name) and f.id in ("str", "repr"):
                return True
            if isinstance(f, ast.Attribute) and f.attr in ("format", "json"):
                return True
        elif isinstance(sub, ast.Attribute) and sub.attr in ("text", "content"):
            return True
        elif isinstance(sub, ast.BinOp) and isinstance(sub.op, ast.Mod) \
                and isinstance(sub.left, ast.Constant) \
                and isinstance(sub.left.value, str):
            return True  # "%s" % x
        elif isinstance(sub, ast.JoinedStr):
            # f-string の変数埋め込み（定数のみでない）
            if any(isinstance(v, ast.FormattedValue) for v in sub.values):
                return True
    return False


def scan_source(src: str, name: str) -> list[str]:
    """source 文字列を解析し違反リストを返す（parse 失敗は例外送出＝H03）"""
    tree = ast.parse(src, filename=name)   # 失敗は SyntaxError を送出（黙殺しない）
    print_aliases, httpexc_names = _print_aliases_and_httpexc(tree)
    violations = []

    for node in ast.walk(tree):
        # print 別名の生成（p = print / import as）を境界違反として記録
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                if a.name == "print":
                    violations.append(f"{name}:{node.lineno}:print_alias")
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name) \
                and node.value.id == "print":
            violations.append(f"{name}:{node.lineno}:print_alias")

        if not isinstance(node, ast.Call):
            continue
        f = node.func

        # 直接出力 sink（無条件違反）
        if isinstance(f, ast.Name) and (f.id == "print" or f.id in print_aliases):
            violations.append(f"{name}:{node.lineno}:print")
            continue
        if _is_stdio_write(f):
            violations.append(f"{name}:{node.lineno}:stdio_write")
            continue
        if _is_logging_module_level(f):
            violations.append(f"{name}:{node.lineno}:logging_module")
            continue

        # raw error を運ぶ可能性のある sink（引数に raw があれば違反）
        sink = None
        if isinstance(f, ast.Name) and f.id in httpexc_names:
            sink = "httpexception"
        elif isinstance(f, ast.Attribute):
            if f.attr == "HTTPException":            # fastapi.HTTPException(...)
                sink = "httpexception"
            elif f.attr in _LOGGER_LEVELS:
                sink = "logger"
            elif f.attr == "log":                    # logger.log(level, ...)
                sink = "logger_log"
        if sink is not None:
            args = list(node.args) + [kw.value for kw in node.keywords]
            if any(_arg_has_raw(a) for a in args):
                violations.append(f"{name}:{node.lineno}:raw_error:{sink}")

    return sorted(set(violations))


# ══════════════════════════════════════════════════════════════
# リポジトリ全体の走査
# ══════════════════════════════════════════════════════════════

def _tracked_py():
    out = subprocess.run(["git", "ls-files", "*.py"], capture_output=True,
                         text=True, check=True, cwd=REPO).stdout
    for line in out.splitlines():
        if not line:
            continue
        p = Path(line)
        if line.startswith(EXCLUDE_PREFIXES):
            continue
        if p.name.startswith("test_") or p.name in EXCLUDE_NAMES:
            continue
        yield p


def scan_repo():
    """全対象ファイルを走査。(violations, parse_errors) を返す（H03: 失敗を可視化）"""
    violations, errors = [], []
    for path in _tracked_py():
        posix = path.as_posix()
        try:
            src = (REPO / path).read_text(encoding="utf-8")
            violations.extend(scan_source(src, posix))
        except Exception as e:
            errors.append(f"{posix}: {type(e).__name__}")
    return sorted(set(violations)), sorted(errors)


# ══════════════════════════════════════════════════════════════
# スキャナ自身の unit test（fixture・table-driven）
# ══════════════════════════════════════════════════════════════

class TestScannerDetection(unittest.TestCase):
    CASES = [
        # (説明, source, 期待 rule 集合)
        ("print", "print('x')\n", {"print"}),
        ("print_alias_assign", "p = print\np('x')\n", {"print_alias", "print"}),
        ("print_alias_import",
         "from builtins import print as pp\npp('x')\n", {"print_alias", "print"}),
        ("stdout_write", "import sys\nsys.stdout.write('x')\n", {"stdio_write"}),
        ("stderr_write", "import sys\nsys.stderr.write('x')\n", {"stdio_write"}),
        ("logging_module", "import logging\nlogging.info('x')\n",
         {"logging_module"}),
        ("logger_str", "logger.info('a %s', str(e))\n", {"raw_error:logger"}),
        ("logger_fstring", "logger.error(f'boom {e}')\n", {"raw_error:logger"}),
        ("logger_percent", "logger.warning('x %s' % e)\n", {"raw_error:logger"}),
        ("logger_format", "logger.info('{}'.format(e))\n", {"raw_error:logger"}),
        ("logger_repr", "logger.info(repr(e))\n", {"raw_error:logger"}),
        ("logger_text", "logger.error(resp.text)\n", {"raw_error:logger"}),
        ("logger_content", "logger.error(resp.content)\n", {"raw_error:logger"}),
        ("logger_json", "logger.error(resp.json())\n", {"raw_error:logger"}),
        ("logger_log", "logger.log(level, str(e))\n", {"raw_error:logger_log"}),
        ("httpexc_plain",
         "raise HTTPException(status_code=500, detail=str(e))\n",
         {"raw_error:httpexception"}),
        ("httpexc_alias",
         "from fastapi import HTTPException as H\n"
         "raise H(detail=f'{e}')\n", {"raw_error:httpexception"}),
        ("httpexc_module",
         "import fastapi\nraise fastapi.HTTPException(detail=resp.text)\n",
         {"raw_error:httpexception"}),
        ("safe_logger_no_raw", "logger.info('static message')\n", set()),
        ("safe_logger_constant_fstring", "logger.info(f'no vars here')\n", set()),
    ]

    def test_each_pattern(self):
        for desc, src, expected_rules in self.CASES:
            with self.subTest(case=desc):
                found = scan_source(src, "fx.py")
                # entry = "name:lineno:rule[:extra]" → rule 部分（index 2 以降）
                rules = {":".join(v.split(":")[2:]) for v in found}
                self.assertEqual(rules, expected_rules,
                                 f"{desc}: got {found}")

    def test_parse_error_raises(self):
        """H03: parse 失敗は黙殺せず例外（scan_source は握りつぶさない）"""
        with self.assertRaises(SyntaxError):
            scan_source("def bad(:\n", "fx.py")


# ══════════════════════════════════════════════════════════════
# 許可リスト（台帳）検査
# ══════════════════════════════════════════════════════════════

class TestSinkAllowlist(unittest.TestCase):
    def setUp(self):
        data = json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8"))
        self.baseline = int(data["baseline_count"])
        self.allow = set(data["entries"])
        self.current, self.errors = scan_repo()

    def test_no_parse_errors(self):
        """H03: 走査中の parse/read 失敗を黙殺しない"""
        self.assertEqual(self.errors, [],
                         "AST 走査で解析失敗が発生した:\n" + "\n".join(self.errors))

    def test_scan_reaches_enough_files(self):
        self.assertGreater(len(self.current), 10)

    def test_no_new_violations(self):
        new = sorted(set(self.current) - self.allow)
        self.assertEqual(new, [],
                         "redaction 方針に反する新規 sink 直書きが追加された。"
                         "emit() 経由にするか、正当なら allowlist に追記して理由を"
                         "レビューで明示すること:\n" + "\n".join(new))

    def test_monotonic_decrease_only(self):
        """総数は baseline_count を上限（単調減少のみ許可・S1〜S4 で減らす）"""
        self.assertLessEqual(len(self.current), self.baseline,
                             f"違反総数が基準 {self.baseline} を超えた"
                             f"（現在 {len(self.current)}）。新規 sink 直書きの疑い。")

    def test_allowlist_entries_are_well_formed(self):
        rules = {"print", "print_alias", "stdio_write", "logging_module",
                 "raw_error"}
        for e in self.allow:
            parts = e.split(":")
            self.assertGreaterEqual(len(parts), 3, e)
            self.assertIn(parts[2], rules, e)


if __name__ == "__main__":
    unittest.main()
