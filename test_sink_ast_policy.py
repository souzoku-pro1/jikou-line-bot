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

# 例外情報を出力する keyword（値が True でも例外本文が出るため安全扱いしない・H03）
_UNSAFE_KEYWORDS = {"exc_info", "stack_info"}

# 外側 sink → emit に必須の (sink, audience) policy（H02）
_SINK_REQUIRED_POLICY = {
    "print": ("log", "operator"),
    "logger": ("log", "operator"),
    "logger_log": ("log", "operator"),
    "logging_module": ("log", "operator"),
    "stdio_write": ("log", "operator"),
    "httpexception": ("exception_detail", "caller"),
    # 将来の LINE sink 関数が対象になった際の拡張ポイント:
    #   "line_customer_fn": ("line_customer", "customer"),
    #   "line_business_fn": ("line_business", "attorney"),
}


# ══════════════════════════════════════════════════════════════
# ファイルごとの束縛収集（emit / print別名 / HTTPException別名）
# ══════════════════════════════════════════════════════════════

class _Bindings:
    """H01: 信頼できる emit 束縛のみを収集する。

    - `from hub.redact import emit [as X]` → local 名（emit / X）を emit_names へ
    - `import hub.redact` → hub.redact.emit を許可（emit_module_attrs に ('hub.redact',)）
    - `from hub import redact` / `import hub.redact as r` → <alias>.emit を許可
    束縛外の同名 emit（ローカル def・別 module の .emit）は信頼しない。
    """

    def __init__(self):
        self.emit_names = set()          # 直接 emit(...) を許すローカル名
        self.emit_module_aliases = set()  # <alias>.emit(...) を許す module 別名
        self.print_aliases = set()
        self.httpexc_names = {"HTTPException"}


def _dotted(node: ast.AST) -> str | None:
    """a.b.c の attribute/name チェーンを 'a.b.c' に（それ以外は None）"""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


def collect_bindings(tree: ast.AST) -> _Bindings:
    b = _Bindings()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            for a in node.names:
                local = a.asname or a.name
                if a.name == "print":
                    b.print_aliases.add(local)
                if a.name == "HTTPException":
                    b.httpexc_names.add(local)
                # from hub.redact import emit [as X]
                if mod == "hub.redact" and a.name == "emit":
                    b.emit_names.add(local)
                # from hub import redact [as r] → r.emit
                if mod == "hub" and a.name == "redact":
                    b.emit_module_aliases.add(local)
        elif isinstance(node, ast.Import):
            for a in node.names:
                # import hub.redact [as r]
                if a.name == "hub.redact":
                    b.emit_module_aliases.add(a.asname or "hub.redact")
        elif isinstance(node, ast.Assign):
            if isinstance(node.value, ast.Name) and node.value.id == "print":
                for tgt in node.targets:
                    if isinstance(tgt, ast.Name):
                        b.print_aliases.add(tgt.id)
    return b


def _is_trusted_emit(node: ast.Call, b: _Bindings) -> bool:
    """H01: hub.redact の emit 束縛に一致する呼び出しだけを emit と認める"""
    f = node.func
    if isinstance(f, ast.Name):
        return f.id in b.emit_names
    if isinstance(f, ast.Attribute) and f.attr == "emit":
        base = _dotted(f.value)
        return base in b.emit_module_aliases  # <alias>.emit / hub.redact.emit
    return False


def _const_str(node: ast.AST):
    """定数文字列なら値を返す（そうでなければ None）"""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _emit_policy_ok(call: ast.Call, required) -> bool:
    """H02: emit(value, kind, sink, audience) の sink/audience が定数かつ
    外側 sink の必須 policy と一致するか。位置引数・keyword 双方に対応。"""
    req_sink, req_aud = required
    sink_node = call.args[2] if len(call.args) > 2 else None
    aud_node = call.args[3] if len(call.args) > 3 else None
    for kw in call.keywords:
        if kw.arg == "sink":
            sink_node = kw.value
        elif kw.arg == "audience":
            aud_node = kw.value
    return _const_str(sink_node) == req_sink and _const_str(aud_node) == req_aud


def _arg_is_safe(node: ast.AST, b: _Bindings, required) -> bool:
    """引数が (a) 定数 / (b) 定数 f-string / (c) 信頼 emit で policy 一致 /
    (d) それらの結合 のみか（外側 sink の required policy 文脈で判定）"""
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.JoinedStr):
        return all(isinstance(v, ast.Constant) for v in node.values)
    if isinstance(node, ast.Call):
        return _is_trusted_emit(node, b) and _emit_policy_ok(node, required)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return (_arg_is_safe(node.left, b, required)
                and _arg_is_safe(node.right, b, required))
    if isinstance(node, (ast.Tuple, ast.List)):
        return all(_arg_is_safe(e, b, required) for e in node.elts)
    return False


def _sink_kind(node: ast.Call, b: _Bindings):
    f = node.func
    if isinstance(f, ast.Name):
        if f.id == "print" or f.id in b.print_aliases:
            return "print"
        if f.id in b.httpexc_names:
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
    b = collect_bindings(tree)
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
        kind = _sink_kind(node, b)
        if kind is None:
            continue
        required = _SINK_REQUIRED_POLICY[kind]
        # H03: exc_info / stack_info keyword は値が True でも違反（例外本文が出る）
        bad_kw = any(kw.arg in _UNSAFE_KEYWORDS for kw in node.keywords)
        args = list(node.args) + [kw.value for kw in node.keywords
                                  if kw.arg not in _UNSAFE_KEYWORDS]
        if bad_kw or not all(_arg_is_safe(a, b, required) for a in args):
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
        # H01: 束縛外 emit（偽 emit・ローカル def）は信頼しない
        ("fake_module_emit",
         "logger.info(fake.emit(customer_name))\n", {"sink:logger"}),
        ("local_def_emit",
         "def emit(v):\n    return v\nlogger.info(emit(customer_name))\n",
         {"sink:logger"}),
        ("bare_emit_no_import",
         "logger.info(emit(customer_name))\n", {"sink:logger"}),
        # H02: 正規 emit でも policy 不一致（logger に line_customer 指定）は違反
        ("policy_mismatch_logger",
         "from hub.redact import emit\n"
         "logger.info(emit(name, 'name', 'line_customer', 'customer'))\n",
         {"sink:logger"}),
        ("policy_mismatch_httpexc",
         "from hub.redact import emit\n"
         "raise HTTPException(detail=emit(x, 'name', 'log', 'operator'))\n",
         {"sink:httpexception"}),
        ("emit_nonconst_policy",
         "from hub.redact import emit\n"
         "logger.info(emit(x, 'name', sink_var, 'operator'))\n",
         {"sink:logger"}),
        # H03: exc_info / stack_info は True でも違反
        ("exc_info_true", "logger.error('x', exc_info=True)\n", {"sink:logger"}),
        ("stack_info_true", "logger.error('x', stack_info=True)\n",
         {"sink:logger"}),
    ]
    # 合格すべき（全引数が定数 / 定数 f-string / 信頼 emit で policy 一致 / 結合）
    SAFE_CASES = [
        ("print_static", "print('x')\n"),
        ("stdout_write_static", "import sys\nsys.stdout.write('x')\n"),
        ("logging_module_static", "import logging\nlogging.info('x')\n"),
        ("logger_static", "logger.info('static message')\n"),
        ("logger_const_fstring", "logger.info(f'no vars here')\n"),
        ("httpexc_const",
         "raise HTTPException(status_code=500, detail='not found')\n"),
        # H01: 3 束縛それぞれの正規 emit（policy 一致）は合格
        ("emit_from_import",
         "from hub.redact import emit\n"
         "logger.info(emit(x, 'name', 'log', 'operator'))\n"),
        ("emit_from_import_as",
         "from hub.redact import emit as R\n"
         "logger.info(R(x, 'name', 'log', 'operator'))\n"),
        ("emit_import_module",
         "import hub.redact\n"
         "logger.info(hub.redact.emit(x, 'name', 'log', 'operator'))\n"),
        ("emit_from_hub_import_redact",
         "from hub import redact\n"
         "logger.info(redact.emit(x, 'name', 'log', 'operator'))\n"),
        # H02: httpexception には exception_detail/caller policy の emit なら合格
        ("emit_httpexc_policy",
         "from hub.redact import emit\n"
         "raise HTTPException(detail=emit(x, 'name', 'exception_detail', "
         "'caller'))\n"),
        # keyword での sink/audience 指定・%スタイル・結合
        ("emit_kwargs_policy",
         "from hub.redact import emit\n"
         "logger.info(emit(x, 'name', sink='log', audience='operator'))\n"),
        ("logger_percent_emit",
         "from hub.redact import emit\n"
         "logger.info('case %s', emit(cid, 'record_id', 'log', 'operator'))\n"),
        ("logger_concat_const_emit",
         "from hub.redact import emit\n"
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
