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
    """H01/NH01: 信頼できる emit 束縛を **1 形式のみ** に限定 + shadow 全面禁止。

    信頼する唯一の形式:
        from hub.redact import emit        # alias 無し・**module-level のみ**（P1-113）

    - module 修飾・別名（`emit as X` / `hub.redact.emit` / `redact.emit`）は信頼しない。
    - **shadow 全面禁止**: ファイル内に `emit` という名前の他束縛が1つでもあれば、
      その束縛を `emit_shadow` 違反として記録し、そのファイルの全 emit 呼び出しを
      信頼しない（poison・trusted=False）。検出する束縛（P1-101d/e）:
        def/class/async def・代入/annotated/walrus/aug・関数/lambda param・
        for/with as/except as/comprehension/global/nonlocal・別 import/import as emit・
        **match capture（MatchAs/MatchStar/MatchMapping.rest・P1-101e H01）**・
        **信頼 emit と同居する star import `from x import *`（P1-101e H02）**・
        **del emit（P1-101e M01）**・
        **関数スコープ内の `from hub.redact import emit`（P1-113・HOTFIX-01 型の
        UnboundLocalError 時限爆弾＝module-level と関数内を区別し関数内は信頼しない）**。
    - **dynamic_name_op（P1-101e・別規則）**: 信頼 emit import を持つファイル内の
      `exec(...)` 呼び出し・`globals()[...]=` / `locals()[...]=` 代入を検出・poison。

    **静的解析の原理的限界（明記）**: 完全に動的な名前操作
    （`builtins.emit=...` 経由の fallback、`__import__`/属性経由の間接束縛、
    文字列連結して `exec` に渡す等）は AST から静的に確定できず、本スキャナの
    対象外。上記 `exec`/`globals()`/`locals()` の直接形のみを別規則で塞ぐ。
    """

    def __init__(self):
        self.trusted = False          # 信頼 emit 呼び出しを許すか
        self.shadow_lines = set()     # emit_shadow 束縛の行番号
        self.dynamic_lines = set()    # dynamic_name_op（exec/globals/locals）の行
        self.print_aliases = set()
        self.httpexc_names = {"HTTPException"}


def _iter_arg_nodes(arguments: ast.arguments):
    for a in (list(arguments.posonlyargs) + list(arguments.args)
              + list(arguments.kwonlyargs)):
        yield a
    if arguments.vararg:
        yield arguments.vararg
    if arguments.kwarg:
        yield arguments.kwarg


def collect_bindings(tree: ast.AST) -> _Bindings:
    b = _Bindings()
    trusted_import = False
    star_lines = set()      # 信頼 emit がある場合のみ poison する star import 行
    dynamic_cand = set()    # 同上・exec/globals/locals の候補行
    # P1-113: 関数スコープ内の import 文を先に収集し、module-level と区別する。
    # 関数内の `from hub.redact import emit`（信頼形式と同形）は emit を**関数全体で
    # ローカル変数化**し、「import 位置より前の emit 参照」を実行時 UnboundLocalError の
    # 時限爆弾にする（HOTFIX-01 の真因型）。信頼形式とは認めず emit_shadow として
    # poison する（module-level の alias 無し import のみが唯一の信頼形式）。
    nested_import_ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for sub in ast.walk(node):
                if isinstance(sub, (ast.Import, ast.ImportFrom)):
                    nested_import_ids.add(id(sub))
    for node in ast.walk(tree):
        # ── import 群 ──
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            for a in node.names:
                local = a.asname or a.name
                if a.name == "*":                     # from x import *（H02）
                    star_lines.add(node.lineno)
                    continue
                if a.name == "print":
                    b.print_aliases.add(local)
                if a.name == "HTTPException":
                    b.httpexc_names.add(local)
                # 唯一の信頼形式: **module-level の** from hub.redact import emit（alias 無し）
                if mod == "hub.redact" and a.name == "emit" and a.asname is None:
                    if id(node) in nested_import_ids:
                        # P1-113: 関数内は信頼せず shadow（HOTFIX-01 型の時限爆弾）
                        b.shadow_lines.add(node.lineno)
                    else:
                        trusted_import = True
                # 上記以外で 'emit' を束縛する import は shadow
                elif a.asname == "emit" or (a.asname is None and a.name == "emit"):
                    b.shadow_lines.add(node.lineno)
        elif isinstance(node, ast.Import):
            for a in node.names:
                if a.name == "emit" or a.asname == "emit":
                    b.shadow_lines.add(node.lineno)
        # ── print 別名の代入 ──
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Name) \
                and node.value.id == "print":
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    b.print_aliases.add(tgt.id)
        # ── shadow: def / class / async def emit ──
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)) and node.name == "emit":
            b.shadow_lines.add(node.lineno)
        # ── shadow: 関数/lambda の parameter 名 emit ──
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            for arg in _iter_arg_nodes(node.args):
                if arg.arg == "emit":
                    b.shadow_lines.add(arg.lineno)
        # ── shadow: except as emit ──
        if isinstance(node, ast.ExceptHandler) and node.name == "emit":
            b.shadow_lines.add(node.lineno)
        # ── shadow: global / nonlocal emit ──
        if isinstance(node, (ast.Global, ast.Nonlocal)) and "emit" in node.names:
            b.shadow_lines.add(node.lineno)
        # ── shadow: match capture（H01）──
        if isinstance(node, ast.MatchAs) and node.name == "emit":
            b.shadow_lines.add(node.lineno)
        if isinstance(node, ast.MatchStar) and node.name == "emit":
            b.shadow_lines.add(node.lineno)
        if isinstance(node, ast.MatchMapping) and node.rest == "emit":
            b.shadow_lines.add(node.lineno)
        # ── shadow: Store / Del される Name 'emit'（代入・for・with as・
        #    comprehension・walrus・aug 代入・del emit〔M01〕を一括カバー）──
        if isinstance(node, ast.Name) and node.id == "emit" \
                and isinstance(node.ctx, (ast.Store, ast.Del)):
            b.shadow_lines.add(node.lineno)
        # ── dynamic_name_op 候補: exec(...) ──
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id == "exec":
            dynamic_cand.add(node.lineno)
        # ── dynamic_name_op 候補: globals()[...]= / locals()[...]= ──
        if isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Store) \
                and isinstance(node.value, ast.Call) \
                and isinstance(node.value.func, ast.Name) \
                and node.value.func.id in ("globals", "locals"):
            dynamic_cand.add(node.lineno)

    # star import / dynamic は「信頼 emit import を持つファイル」に限り poison
    if trusted_import:
        b.shadow_lines |= star_lines
        b.dynamic_lines |= dynamic_cand
    b.trusted = trusted_import and not b.shadow_lines and not b.dynamic_lines
    return b


def _is_trusted_emit(node: ast.Call, b: _Bindings) -> bool:
    """H01/NH01: 信頼形式（alias 無し import）由来の `emit(...)` 直接呼び出しのみ。
    shadow / dynamic があるファイルでは b.trusted=False なので全て untrusted。"""
    f = node.func
    return isinstance(f, ast.Name) and f.id == "emit" and b.trusted


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

    # emit shadow 束縛（NH01・全面禁止）を記録
    for ln in b.shadow_lines:
        violations.append(f"{name}:{ln}:emit_shadow")
    # 動的名前操作（P1-101e・別規則）を記録
    for ln in b.dynamic_lines:
        violations.append(f"{name}:{ln}:dynamic_name_op")

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
        # L01: logger.exception / logging.exception は exc_info=True 相当で
        # 例外本文（PII を含みうる）を無条件に出力する。引数が定数でも危険なので
        # sink 種別として恒久禁止し、logger.error＋固定分類メッセージへ移行させる。
        f = node.func
        if kind in ("logger", "logging_module") \
                and isinstance(f, ast.Attribute) and f.attr == "exception":
            violations.append(f"{name}:{node.lineno}:logger_exception")
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
        # L01: logger.exception は引数に関わらず logger_exception 違反（exc_info 相当）
        ("logger_exc_info", "logger.exception('x', exc_info=e)\n",
         {"logger_exception"}),
        ("logger_exception_const", "logger.exception('constant only')\n",
         {"logger_exception"}),
        ("logger_exception_var", "logger.exception('m %s', v)\n",
         {"logger_exception"}),
        ("logging_module_exception",
         "import logging\nlogging.exception('x')\n", {"logger_exception"}),
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
        # H01: 束縛外 emit（偽 module.emit・無 import）は信頼しない
        ("fake_module_emit",
         "logger.info(fake.emit(customer_name))\n", {"sink:logger"}),
        ("bare_emit_no_import",
         "logger.info(emit(customer_name))\n", {"sink:logger"}),
        # H01: module 修飾形は信頼廃止（1 形式のみ）
        ("module_qualified_emit",
         "import hub.redact\n"
         "logger.info(hub.redact.emit(x, 'name', 'log', 'operator'))\n",
         {"sink:logger"}),
        # H02: 正規 emit でも policy 不一致は違反
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
        # NH01: shadow（正規 import があっても poison）——束縛=emit_shadow・呼出=distrust
        ("shadow_def_emit",
         "from hub.redact import emit\ndef emit(v):\n    return v\n"
         "logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_assign_fake",
         "from hub.redact import emit\nemit = fake_emit\n"
         "logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_param",
         "from hub.redact import emit\ndef f(emit, x):\n"
         "    logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_evil_import_then_valid",
         "from evil import emit\nfrom hub.redact import emit\n"
         "logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_comprehension",
         "from hub.redact import emit\nys = [emit for emit in xs]\n"
         "logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_except_as",
         "from hub.redact import emit\ntry:\n    pass\n"
         "except Exception as emit:\n"
         "    logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_walrus",
         "from hub.redact import emit\nif (emit := f()):\n"
         "    logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_import_as",
         "from hub.redact import emit\nimport json as emit\n"
         "logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_only_no_call",
         "def emit(v):\n    return v\n", {"emit_shadow"}),
        # P1-101e H01: match capture
        ("shadow_match_capture",
         "from hub.redact import emit\nmatch x:\n    case emit:\n"
         "        logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_match_as",
         "from hub.redact import emit\nmatch x:\n    case [y] as emit:\n"
         "        logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_match_star",
         "from hub.redact import emit\nmatch x:\n    case [*emit]:\n"
         "        logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("shadow_match_mapping_rest",
         "from hub.redact import emit\nmatch x:\n    case {'k': _, **emit}:\n"
         "        logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        # P1-101e H02: star import（信頼 emit と同居）
        ("shadow_star_import",
         "from hub.redact import emit\nfrom evil import *\n"
         "logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        # P1-101e M01: del emit
        ("shadow_del_emit",
         "from hub.redact import emit\ndel emit\n"
         "logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        # P1-113: 関数内の信頼形式 import は emit_shadow（HOTFIX-01 型・名前の関数ローカル化）
        ("nested_trusted_import_in_function",
         "def f(x):\n    from hub.redact import emit\n"
         "    logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("nested_import_with_module_level_trusted",
         "from hub.redact import emit\n"
         "def f(x):\n    from hub.redact import emit\n"
         "    logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        ("nested_trusted_import_async_function",
         "async def f(x):\n    from hub.redact import emit\n"
         "    logger.info(emit(x, 'name', 'log', 'operator'))\n",
         {"emit_shadow", "sink:logger"}),
        # P1-101e 別規則: dynamic_name_op
        ("dynamic_globals_assign",
         "from hub.redact import emit\nglobals()['emit'] = fake\n"
         "logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"dynamic_name_op", "sink:logger"}),
        ("dynamic_exec",
         "from hub.redact import emit\nexec('emit = fake')\n"
         "logger.info(emit(v, 'name', 'log', 'operator'))\n",
         {"dynamic_name_op", "sink:logger"}),
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
        # H01/NH01: 唯一の信頼形式（alias 無し import・shadow 無し）だけ合格
        ("emit_from_import",
         "from hub.redact import emit\n"
         "logger.info(emit(x, 'name', 'log', 'operator'))\n"),
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
        # 信頼 emit を持たないファイルの star import / exec / globals は台帳ノイズにしない
        ("star_import_no_trusted_emit",
         "from evil import *\nlogger.info('static')\n"),
        ("exec_no_trusted_emit",
         "exec('x = 1')\nlogger.info('static')\n"),
        ("globals_assign_no_trusted_emit",
         "globals()['x'] = 1\nlogger.info('static')\n"),
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

def _file_rule(entry: str) -> str:
    """entry 'file:line:rule...' から (file, rule) 結合キー 'file:rule...' を得る。"""
    parts = entry.split(":")
    return parts[0] + ":" + ":".join(parts[2:])


class TestSinkAllowlist(unittest.TestCase):
    def setUp(self):
        data = json.loads(ALLOWLIST_PATH.read_text(encoding="utf-8"))
        self.baseline = int(data["baseline_count"])
        self.allow = set(data["entries"])
        self.manifest_reasons = data.get("manifest", {})
        self.manifest = set(self.manifest_reasons.keys())
        self.current, self.errors = scan_repo()

    def test_manifest_reasons_are_non_empty(self):
        """L01: manifest の全キーは非空文字列の理由を持たねばならない
        （新規 sink debt は必ず理由付きで登録させる）。"""
        blank = sorted(k for k, v in self.manifest_reasons.items()
                       if not (isinstance(v, str) and v.strip()))
        self.assertEqual(blank, [],
                         "理由が空の manifest キー（理由を記載すること）:\n"
                         + "\n".join(blank))

    def test_every_entry_is_manifested(self):
        """RP1107B-M01: 台帳の各エントリの (file,rule) は変換 manifest に登録されて
        いなければならない。manifest なしの新規 (file,rule) 追加は FAIL（silent な
        新規 sink debt の混入を防ぐ）。"""
        unmanifested = sorted({_file_rule(e) for e in self.allow}
                              - self.manifest)
        self.assertEqual(unmanifested, [],
                         "manifest 未登録の (file,rule) が台帳にある。理由を添えて "
                         "redaction_sink_allowlist.json の manifest に登録すること:\n"
                         + "\n".join(unmanifested))

    def test_manifest_has_no_stale_keys(self):
        """manifest にあるが台帳に対応エントリが無いキーは削除漏れとして FAIL。"""
        live = {_file_rule(e) for e in self.allow}
        stale = sorted(self.manifest - live)
        self.assertEqual(stale, [],
                         "台帳に対応の無い manifest キー（削除漏れ）:\n"
                         + "\n".join(stale))

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
        ok_rules = {"print_alias", "emit_shadow", "dynamic_name_op",
                    "logger_exception",
                    "sink:print", "sink:logger", "sink:logger_log",
                    "sink:logging_module", "sink:httpexception",
                    "sink:stdio_write"}
        for e in self.allow:
            rule = ":".join(e.split(":")[2:])
            self.assertIn(rule, ok_rules, e)


if __name__ == "__main__":
    unittest.main()
