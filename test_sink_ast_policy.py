"""sink 出力の AST 方針検査（P1-101 / P1-101a / P1-101b・DRAFT §5 の土台）

**ホワイトリスト方式（P1-101b・根治）**: sink 呼び出し（print/別名・logger 系
〔logger.<level> / logger.log / logging.<level>〕・HTTPException〔別名・module 修飾含む〕・
sys.stdout/stderr.write）の **全引数（位置・keyword 双方。exc_info 等も対象）** が
次のみで構成される場合に限り合格:
  (a) 定数（ast.Constant）
  (b) 変数埋込のない定数 f-string
  (c) `emit(...)`（hub.redact.emit）呼び出し
  (d) (a)〜(c) のみからなる結合（`+` 連結・タプル/リスト）
それ以外の要素（変数参照・属性参照・emit 以外の関数呼び出し・`%`/`.format()` 等の演算）を
1つでも含めば違反。logger の %スタイル `logger.info("msg %s", emit(...))` は
第1引数が定数・以降が全て(a)〜(c)なので合格。

移行期は許可リスト方式（redaction_sink_allowlist.json）。3 検査を併存（目的が別）:
- **no_new**: entries に無い新規違反はゼロ。
- **stale（M02）**: entries にあるが現存しない違反はゼロ（解消時の削除漏れを検知）。
- **monotonic**: 総数は baseline_count を上限（単調減少のみ許可）。
- **H03**: parse/read 失敗は黙殺せずテスト失敗。

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
# ホワイトリスト判定
# ══════════════════════════════════════════════════════════════

def _is_emit_call(node: ast.Call) -> bool:
    f = node.func
    if isinstance(f, ast.Name):
        return f.id == "emit"
    if isinstance(f, ast.Attribute):
        return f.attr == "emit"
    return False


def _arg_is_safe(node: ast.AST) -> bool:
    """引数が (a) 定数 / (b) 定数 f-string / (c) emit(...) / (d) それらの結合 のみか"""
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.JoinedStr):
        # 変数埋込（FormattedValue）が無い定数 f-string のみ合格
        return all(isinstance(v, ast.Constant) for v in node.values)
    if isinstance(node, ast.Call):
        return _is_emit_call(node)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _arg_is_safe(node.left) and _arg_is_safe(node.right)
    if isinstance(node, (ast.Tuple, ast.List)):
        return all(_arg_is_safe(e) for e in node.elts)
    return False


# ══════════════════════════════════════════════════════════════
# sink 識別 + 走査
# ══════════════════════════════════════════════════════════════

def _print_aliases_and_httpexc(tree: ast.AST):
    print_aliases = set()
    httpexc_names = {"HTTPException"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for a in node.names:
                local = a.asname or a.name
                if a.name == "print":
                    print_aliases.add(local)
                if a.name == "HTTPException":
                    httpexc_names.add(local)
        elif isinstance(node, ast.Assign):
            if isinstance(node.value, ast.Name) and node.value.id == "print":
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        print_aliases.add(tgt.id)
    return print_aliases, httpexc_names


def _sink_kind(node: ast.Call, print_aliases, httpexc_names):
    f = node.func
    if isinstance(f, ast.Name):
        if f.id == "print" or f.id in print_aliases:
            return "print"
        if f.id in httpexc_names:
            return "httpexception"
    if isinstance(f, ast.Attribute):
        if f.attr == "HTTPException":
            return "httpexception"
        if f.attr in _LOGGER_LEVELS:
            if isinstance(f.value, ast.Name) and f.value.id == "logging":
                return "logging_module"
            return "logger"
        if f.attr == "log":
            return "logger_log"
        if f.attr == "write" and isinstance(f.value, ast.Attribute) \
                and f.value.attr in ("stdout", "stderr"):
            return "stdio_write"
    return None


def scan_source(src: str, name: str) -> list[str]:
    """source 文字列を解析し違反リストを返す（parse 失敗は例外送出＝H03）"""
    tree = ast.parse(src, filename=name)
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
        kind = _sink_kind(node, print_aliases, httpexc_names)
        if kind is None:
            continue
        args = list(node.args) + [kw.value for kw in node.keywords]
        if not all(_arg_is_safe(a) for a in args):
            violations.append(f"{name}:{node.lineno}:sink:{kind}")

    return sorted(set(violations))


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
    # 違反になるべき（ホワイトリスト外の要素を含む）
    VIOLATION_CASES = [
        ("print_var", "print(x)\n", {"sink:print"}),
        ("print_alias_assign", "p = print\np(x)\n",
         {"print_alias", "sink:print"}),
        ("print_alias_import",
         "from builtins import print as pp\npp(x)\n",
         {"print_alias", "sink:print"}),
        ("stdout_write_var", "import sys\nsys.stdout.write(x)\n",
         {"sink:stdio_write"}),
        ("logging_module_var", "import logging\nlogging.info(x)\n",
         {"sink:logging_module"}),
        # Codex 提案分
        ("logger_customer_name", "logger.info(customer_name)\n", {"sink:logger"}),
        ("logger_error", "logger.error(error)\n", {"sink:logger"}),
        ("httpexc_detail_var",
         "raise HTTPException(detail=error_detail)\n",
         {"sink:httpexception"}),
        ("logger_exc_info", "logger.exception('x', exc_info=e)\n",
         {"sink:logger"}),
        ("logger_traceback", "logger.error(traceback.format_exc())\n",
         {"sink:logger"}),
        ("logger_eargs", "logger.error(e.args)\n", {"sink:logger"}),
        # 迂回パターン（従来 raw_error 相当）
        ("logger_str", "logger.info(str(e))\n", {"sink:logger"}),
        ("logger_fstring_var", "logger.error(f'boom {e}')\n", {"sink:logger"}),
        ("logger_percent_inline", "logger.warning('x %s' % e)\n",
         {"sink:logger"}),
        ("logger_format", "logger.info('{}'.format(e))\n", {"sink:logger"}),
        ("logger_repr", "logger.info(repr(e))\n", {"sink:logger"}),
        ("logger_text", "logger.error(resp.text)\n", {"sink:logger"}),
        ("logger_json", "logger.error(resp.json())\n", {"sink:logger"}),
        ("logger_log_var", "logger.log(level, str(e))\n", {"sink:logger_log"}),
        ("httpexc_str",
         "raise HTTPException(detail=str(e))\n", {"sink:httpexception"}),
        ("httpexc_alias",
         "from fastapi import HTTPException as H\nraise H(detail=f'{e}')\n",
         {"sink:httpexception"}),
        ("httpexc_module",
         "import fastapi\nraise fastapi.HTTPException(detail=resp.text)\n",
         {"sink:httpexception"}),
    ]
    # 合格すべき（全引数が定数 / 定数 f-string / emit / それらの結合）
    SAFE_CASES = [
        ("print_static", "print('x')\n"),
        ("stdout_write_static", "import sys\nsys.stdout.write('x')\n"),
        ("logging_module_static", "import logging\nlogging.info('x')\n"),
        ("logger_static", "logger.info('static message')\n"),
        ("logger_const_fstring", "logger.info(f'no vars here')\n"),
        ("logger_emit",
         "logger.info(emit(x, 'name', 'log', 'operator'))\n"),
        ("logger_percent_emit",
         "logger.info('case %s', emit(cid, 'record_id', 'log', 'operator'))\n"),
        ("httpexc_const",
         "raise HTTPException(status_code=500, detail='not found')\n"),
        ("logger_exc_info_true", "logger.error('x', exc_info=True)\n"),
        ("logger_concat_const_emit",
         "logger.info('p:' + emit(x, 'name', 'log', 'operator'))\n"),
    ]

    def test_violation_patterns(self):
        for desc, src, expected in self.VIOLATION_CASES:
            with self.subTest(case=desc):
                found = scan_source(src, "fx.py")
                rules = {":".join(v.split(":")[2:]) for v in found}
                self.assertEqual(rules, expected, f"{desc}: got {found}")

    def test_safe_patterns(self):
        for desc, src in self.SAFE_CASES:
            with self.subTest(case=desc):
                found = scan_source(src, "fx.py")
                # print_alias 生成が無い純合格ケースなので空
                self.assertEqual(found, [], f"{desc}: got {found}")

    def test_parse_error_raises(self):
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

    def test_no_stale_entries(self):
        """M02: 解消済み（現存しない）違反が allowlist に残っていないこと"""
        stale = sorted(self.allow - set(self.current))
        self.assertEqual(stale, [],
                         "解消済みの違反が allowlist に残っている（削除漏れ）。"
                         "S1〜S4 で直したら当該行を allowlist から削除すること:\n"
                         + "\n".join(stale))

    def test_monotonic_decrease_only(self):
        self.assertLessEqual(len(self.current), self.baseline,
                             f"違反総数が基準 {self.baseline} を超えた"
                             f"（現在 {len(self.current)}）。新規 sink 直書きの疑い。")

    def test_allowlist_entries_are_well_formed(self):
        ok_rules = {"print_alias", "sink:print", "sink:logger",
                    "sink:logger_log", "sink:logging_module",
                    "sink:httpexception", "sink:stdio_write"}
        for e in self.allow:
            rule = ":".join(e.split(":")[2:])
            self.assertIn(rule, ok_rules, e)


if __name__ == "__main__":
    unittest.main()
