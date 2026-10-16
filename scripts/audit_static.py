"""audit_static — AUDIT-1: リポジトリ全体の静的監査（報告のみ・コードは触らない）

QUEUE-2026-10-16-R F-5。実行: `python scripts/audit_static.py --repo <root> --out <md> [--json <path>]`

検出（a）〜（d）＋未使用候補。すべて AST／正規表現の静的検査で、アプリを import しない・実行しない・外部 I/O なし。
  (a) PII になりうる識別子（name／phone／line_user_id／address／本文 等）が sink（print・logger・例外文字列・URL）へ流れる箇所。
      hub.redact.emit(...) で包まれた引数は対象外（redaction 契約の入口）。既存の債務台帳 redaction_sink_allowlist.json に
      載っている行は「既知（台帳）」として区別する。
  (b) 送信関数（LINE push・kintone 書込・FAX adapter）の呼出のうち、呼出元の関数／module に承認文脈の印
      （_gate・approved_draft・human_reply・ticket・confirm・approval・承認）が見当たらない経路。
  (c) DELETE 文・物理削除 API（sa.delete・delete_record・DELETE FROM・drop_table・os.remove／unlink・shutil.rmtree）。
  (d) 秘密値らしき文字列リテラル（鍵の接頭辞・長い英数列・password／token／secret の直書き）。
  (e) 未使用の関数・モジュール候補（定義名が他所で一度も参照されない）。
分類は「要修正」「誤検知」「判断保留」の 3 値。本スクリプトは **機械規則で仮分類**し（CLASSIFY_RULES）、最終分類は人が報告で確定する。
sink 方針: print は定数のみ（結果はファイルへ書く）。
"""

import argparse
import ast
import json
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

EXCLUDE_DIRS = ("legacy", "__pycache__", ".git", "node_modules", "sbom", "alembic")
EXCLUDE_FILES = {"test_sink_ast_policy.py", "redact.py", "audit_static.py"}
TEST_PREFIX = "test_"

PII_WORDS = ("name", "phone", "tel", "line_user", "user_id", "userid", "address", "addr", "email", "mail", "birth",
             "message", "text", "body", "customer", "zip", "postal", "account", "顧客", "氏名", "住所", "電話", "本文", "メール")
PII_SAFE_WORDS = ("filename", "file_name", "basename", "field_name", "fieldname", "app_name", "job_name", "name_of", "table_name",
                  "col_name", "column_name", "channel_name", "module_name", "func_name", "class_name", "sink_name", "env_name",
                  "key_name", "route_name", "text_version", "textversion", "test_", "format", "message_id", "messageid",
                  "body_sha", "text_sha", "hostname", "host_name", "display_name_hash", "purpose", "state_name", "doc_name",
                  "template", "mime", "content_type", "content-type", "addr_label")
SINK_FUNCS = {"print"}
LOGGER_LEVELS = {"debug", "info", "warning", "error", "exception", "critical", "log"}
EXC_NAMES = {"Exception", "RuntimeError", "ValueError", "HTTPException", "KintoneError", "TypeError", "KeyError"}
URL_HINT = re.compile(r"(url|uri|endpoint|href)", re.I)

SEND_FUNCS = {"push_text", "push_text_explicit", "reply_with_push_fallback", "notify_admin_line", "notify_business",
              "notify_admin_line_result", "send_business_notice", "update_record", "create_record", "create_records",
              "upload_file", "enqueue_fax", "send_fax", "fax_send"}
FAX_HINT = re.compile(r"fax", re.I)
APPROVAL_MARKS = ("_gate", "approved_draft", "human_reply", "human_explicit", "ticket", "confirm", "approval", "approve",
                  "承認", "明示確認", "revision=", "cas", "webapp_", "approval_queue")
DELETE_PATTERNS = (
    (re.compile(r"\bsa\.delete\("), "sa.delete"),
    (re.compile(r"\bdelete_record\("), "kintone.delete_record"),
    (re.compile(r"\bDELETE\s+FROM\b", re.I), "sql DELETE FROM"),
    (re.compile(r"\bdrop_table\("), "op.drop_table"),
    (re.compile(r"\bos\.(remove|unlink)\("), "os.remove/unlink"),
    (re.compile(r"\bshutil\.rmtree\("), "shutil.rmtree"),
    (re.compile(r"\.delete\(\)"), ".delete()"),
    (re.compile(r"\bTRUNCATE\b", re.I), "sql TRUNCATE"),
)
SECRET_PATTERNS = (
    (re.compile(r"^sk-[A-Za-z0-9_-]{16,}$"), "api key prefix sk-"),
    (re.compile(r"^(sk_live|sk_test|rk_live|whsec)_[A-Za-z0-9]{8,}$"), "stripe key"),
    (re.compile(r"^AKIA[0-9A-Z]{16}$"), "aws access key"),
    (re.compile(r"^xox[abp]-[A-Za-z0-9-]{10,}$"), "slack token"),
    (re.compile(r"^gh[pous]_[A-Za-z0-9]{20,}$"), "github token"),
    (re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"), "private key"),
    (re.compile(r"^[0-9a-fA-F]{48,}$"), "long hex literal"),
    (re.compile(r"^[A-Za-z0-9+/]{40,}={0,2}$"), "long base64-like literal"),
)
SECRET_ASSIGN_RE = re.compile(r"(password|passwd|secret|token|api_key|apikey|access_key)\s*[:=]\s*[\"'][^\"']{6,}[\"']", re.I)
SECRET_SAFE_LITERALS = re.compile(r"(dummy|test|example|placeholder|xxx|sample|fixture|fake|your-|\{\{|\}\}|^0+$|^a+$|^s+$)", re.I)

CATEGORY_A = "a_pii_sink"
CATEGORY_B = "b_send_without_approval_mark"
CATEGORY_C = "c_delete"
CATEGORY_D = "d_secret_literal"
CATEGORY_E_FUNC = "e_unused_function"
CATEGORY_E_MOD = "e_unused_module"

CLASS_FIX = "要修正"
CLASS_FALSE = "誤検知"
CLASS_HOLD = "判断保留"


@dataclass
class Finding:
    category: str
    file: str
    line: int
    detail: str
    classification: str = CLASS_HOLD
    reason: str = ""
    known_allowlist: bool = False
    extra: dict = field(default_factory=dict)


# ── 補助 ─────────────────────────────────────────────────────────────────────
def iter_py_files(repo: Path, include_tests: bool) -> list[Path]:
    out = []
    for p in sorted(repo.rglob("*.py")):
        rel = p.relative_to(repo).as_posix()
        parts = rel.split("/")
        if any(part in EXCLUDE_DIRS for part in parts[:-1]):
            continue
        if p.name in EXCLUDE_FILES:
            continue
        if not include_tests and (p.name.startswith(TEST_PREFIX) or parts[0] == "tests"):
            continue
        out.append(p)
    return out


def _name_of(node) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _dotted(node) -> str:
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else node.attr
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Call):
        return _dotted(node.func)
    return ""


def _is_emit_call(node) -> bool:
    return isinstance(node, ast.Call) and _name_of(node.func) == "emit"


def _pii_like(ident: str) -> bool:
    low = ident.lower()
    if any(s in low for s in PII_SAFE_WORDS):
        return False
    return any(w in low for w in PII_WORDS)


def _pii_idents(node) -> list[str]:
    """引数の木から PII らしき識別子を集める（emit(...) の内側は見ない）。"""
    found: list[str] = []

    def walk(n):
        if _is_emit_call(n):
            return
        if isinstance(n, (ast.Name, ast.Attribute)):
            nm = _name_of(n)
            if nm and _pii_like(nm):
                found.append(nm)
        for ch in ast.iter_child_nodes(n):
            walk(ch)
    walk(node)
    return sorted(set(found))


def _has_dynamic(node) -> bool:
    """定数だけで構成されていない（変数埋込・% / format）か。"""
    if isinstance(node, ast.Constant):
        return False
    if isinstance(node, ast.JoinedStr):
        return any(isinstance(v, ast.FormattedValue) for v in node.values)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        return True
    if isinstance(node, ast.Call) and _name_of(node.func) == "format":
        return True
    return True


def _enclosing_functions(tree) -> list[tuple[ast.AST, str]]:
    out = []
    for n in ast.walk(tree):
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out.append((n, n.name))
    return out


def _func_at(funcs, lineno: int):
    best = None
    for n, name in funcs:
        if n.lineno <= lineno <= getattr(n, "end_lineno", n.lineno):
            if best is None or n.lineno > best[0].lineno:
                best = (n, name)
    return best


def _func_source(src_lines: list[str], node) -> str:
    return "\n".join(src_lines[node.lineno - 1: getattr(node, "end_lineno", node.lineno)])


def load_allowlist(repo: Path) -> set:
    p = repo / "redaction_sink_allowlist.json"
    if not p.exists():
        return set()
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return set()
    out = set()
    for e in d.get("entries", []):
        parts = str(e).split(":")
        if len(parts) >= 2 and parts[1].isdigit():
            out.add((parts[0], int(parts[1])))
    return out


# ── (a) PII → sink ──────────────────────────────────────────────────────────
def audit_pii_sinks(rel: str, tree, src_lines: list[str], allow: set) -> list[Finding]:
    out: list[Finding] = []
    for n in ast.walk(tree):
        sink = ""
        args: list = []
        if isinstance(n, ast.Call):
            fn = n.func
            if isinstance(fn, ast.Name) and fn.id in SINK_FUNCS:
                sink, args = "print", list(n.args) + [k.value for k in n.keywords]
            elif isinstance(fn, ast.Attribute) and fn.attr in LOGGER_LEVELS and _name_of(fn.value) in ("logger", "logging", "log", "_log"):
                sink, args = f"logger.{fn.attr}", list(n.args) + [k.value for k in n.keywords]
            elif isinstance(fn, (ast.Name, ast.Attribute)) and _name_of(fn) in EXC_NAMES:
                sink, args = f"exception:{_name_of(fn)}", list(n.args) + [k.value for k in n.keywords]
            elif isinstance(fn, ast.Attribute) and fn.attr in ("get", "post", "put", "delete", "request") and n.args:
                first = n.args[0]
                if _has_dynamic(first) and _pii_idents(first):
                    sink, args = "url", [first]
        elif isinstance(n, ast.Assign):
            tgt = " ".join(_name_of(t) for t in n.targets)
            if URL_HINT.search(tgt) and _has_dynamic(n.value) and _pii_idents(n.value):
                sink, args = "url", [n.value]
        if not sink:
            continue
        idents = sorted({i for a in args for i in _pii_idents(a)})
        if not idents:
            continue
        if sink.startswith("exception") and all(isinstance(a, ast.Constant) for a in args):
            continue
        known = (rel, n.lineno) in allow
        f = Finding(CATEGORY_A, rel, n.lineno, f"{sink} ← {', '.join(idents)}", known_allowlist=known)
        out.append(f)
    return out


# ── (b) 送信関数の承認文脈 ───────────────────────────────────────────────────
def audit_send_paths(rel: str, tree, src_lines: list[str], module_src: str) -> list[Finding]:
    out: list[Finding] = []
    funcs = _enclosing_functions(tree)
    module_marked = any(m in module_src for m in ("_gate", "approved_draft(", "human_reply(", "approval"))
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        fname = _name_of(n.func)
        dotted = _dotted(n.func)
        is_send = fname in SEND_FUNCS or (FAX_HINT.search(dotted or "") and fname in ("send", "enqueue"))
        if not is_send:
            continue
        enc = _func_at(funcs, n.lineno)
        enc_name = enc[1] if enc else "<module>"
        enc_src = _func_source(src_lines, enc[0]) if enc else ""
        marks = [m for m in APPROVAL_MARKS if m in enc_src or m in enc_name]
        detail = f"{dotted or fname}() in {enc_name}()"
        f = Finding(CATEGORY_B, rel, n.lineno, detail,
                    extra={"marks_in_function": marks, "module_has_approval_marks": module_marked, "callee": fname})
        out.append(f)
    return out


# ── (c) 削除 ────────────────────────────────────────────────────────────────
def _is_disabled_delete_def(src_lines: list[str], i: int) -> bool:
    """AUDIT-FIX-1: `def delete_record` が HTTP DELETE を送らず例外にするだけの無効化スタブなら物理削除ではない
    （本文 12 行以内に "DELETE" の HTTP 動詞が無く raise がある）。関数名での除外ではなく本文で判定する。"""
    body = "\n".join(src_lines[i: i + 12])
    return ('"DELETE"' not in body and "'DELETE'" not in body) and "raise " in body


def audit_deletes(rel: str, src_lines: list[str]) -> list[Finding]:
    out = []
    for i, line in enumerate(src_lines, start=1):
        s = line.strip()
        if s.startswith("#"):
            continue
        for pat, label in DELETE_PATTERNS:
            if pat.search(line):
                if label == "kintone.delete_record" and s.startswith(("def ", "async def ")) and _is_disabled_delete_def(src_lines, i):
                    break                                   # 無効化スタブ（物理削除の器ではない）
                out.append(Finding(CATEGORY_C, rel, i, f"{label}: {s[:120]}"))
                break
    return out


# ── (d) 秘密値 ───────────────────────────────────────────────────────────────
def audit_secrets(rel: str, tree, src_lines: list[str]) -> list[Finding]:
    out = []
    for n in ast.walk(tree):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            v = n.value.strip()
            if len(v) < 16 or SECRET_SAFE_LITERALS.search(v):
                continue
            for pat, label in SECRET_PATTERNS:
                if pat.search(v):
                    out.append(Finding(CATEGORY_D, rel, n.lineno, f"{label}: {v[:6]}…（{len(v)} 文字）"))
                    break
    for i, line in enumerate(src_lines, start=1):
        m = SECRET_ASSIGN_RE.search(line)
        if m and not SECRET_SAFE_LITERALS.search(line) and "os.environ" not in line and "getenv" not in line:
            out.append(Finding(CATEGORY_D, rel, i, f"assignment: {line.strip()[:100]}"))
    return out


# ── (e) 未使用候補 ───────────────────────────────────────────────────────────
def _token_counts(text: str) -> dict:
    out: dict = {}
    for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text):
        out[tok] = out.get(tok, 0) + 1
    return out


def audit_unused(repo: Path, files: list[Path], all_files: list[Path]) -> list[Finding]:
    """参照 0 の関数・import されない module（識別子トークンを 1 回数えてから照合する＝O(files)）。"""
    out: list[Finding] = []
    corpus = {p: p.read_text(encoding="utf-8", errors="replace") for p in all_files}
    counts = {p: _token_counts(t) for p, t in corpus.items()}
    defs: list[tuple[str, str, int]] = []
    for p in files:
        rel = p.relative_to(repo).as_posix()
        try:
            tree = ast.parse(corpus[p])
        except SyntaxError:
            continue
        for n in tree.body:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if n.name.startswith("__") or n.name in ("main", "upgrade", "downgrade", "lifespan"):
                    continue
                if n.decorator_list:
                    continue              # route／decorator 登録は参照に現れないことがある
                defs.append((rel, n.name, n.lineno))
    for rel, name, line in defs:
        refs = 0
        for p, c in counts.items():
            k = c.get(name, 0)
            if p.relative_to(repo).as_posix() == rel:
                k -= 1                     # 自身の def 行
            refs += max(0, k)
        if refs == 0:
            out.append(Finding(CATEGORY_E_FUNC, rel, line, f"def {name}: 参照 0（テストを含む全ファイル）"))
    # modules: import 文だけを集めて照合
    import_lines = {p: [ln for ln in t.splitlines() if ln.lstrip().startswith(("import ", "from "))] for p, t in corpus.items()}
    for p in files:
        rel = p.relative_to(repo).as_posix()
        if rel in ("main.py", "daily_healthcheck.py", "conftest.py") or rel.startswith(("scripts/", "tools/")):
            continue
        stem = rel[:-3].replace("/", ".")
        short = stem.split(".")[-1]
        if short == "__init__":
            continue
        parent = ".".join(stem.split(".")[:-1])
        pats = [re.compile(r"\bimport\s+" + re.escape(stem) + r"\b"), re.compile(r"\bfrom\s+" + re.escape(stem) + r"\s+import")]
        if parent:
            pats.append(re.compile(r"\bfrom\s+" + re.escape(parent) + r"\s+import\s+[^\n]*\b" + re.escape(short) + r"\b"))
        else:
            pats.append(re.compile(r"\bimport\s+" + re.escape(short) + r"\b"))
        found = False
        for q, lines in import_lines.items():
            if q.relative_to(repo).as_posix() == rel:
                continue
            if any(pt.search(ln) for ln in lines for pt in pats):
                found = True
                break
        if not found:
            out.append(Finding(CATEGORY_E_MOD, rel, 1, f"module {stem}: import 参照 0"))
    return out


# ── 仮分類 ───────────────────────────────────────────────────────────────────
# (b) 業務 LINE（事務所側）への通知関数＝相談者宛ではない（承認は設計上不要）
BUSINESS_NOTIFY_FUNCS = {"notify_admin_line", "notify_business", "notify_admin_line_result", "send_business_notice",
                         "_push_business_http", "_push_admin_http"}
# (b) bot の自動返信（許可表 V2・サーバーガード・凍結定型・停止リストで統制された設計上の自動送信経路）
BOT_REPLY_MODULES = ("main.py", "chat_responder.py", "houki_bot/", "hub/send_policy.py", "hub/jikou_reply_flow.py",
                     "hub/image_intake.py", "hub/image_analysis.py", "hub/form_link.py", "hub/shindan_link.py")
# (c) 一時領域の掃除（テスト支援・ローカル変換ツール）
TEMP_CLEANUP_FILES = ("brain_test_support.py", "tools/", "scripts/")
# (e) 手動実行の収載スクリプト・単体ツール（import されないのが正常）
STANDALONE_MODULE_PREFIXES = ("make_", "import_", "registry_to_kintone", "person_restore_cli", "koseki_backfill")


def classify(f: Finding) -> None:
    """機械規則による仮分類（理由付き）。ファイル丸ごとの除外はしない＝各規則は行の性質で判定する。"""
    if f.category == CATEGORY_A:
        sink = f.detail.split(" ")[0]
        if f.known_allowlist:
            f.classification, f.reason = CLASS_HOLD, "既存の債務台帳（redaction_sink_allowlist.json）に登録済み・S 切替の作業台帳で管理"
        elif sink == "url":
            f.classification, f.reason = CLASS_FALSE, "外部 API（kintone／LINE）への問い合わせ条件・宛先パラメータ（送信先の契約・ログ／画面には出ない）"
        elif sink.startswith("exception"):
            idents = f.detail.split("← ")[-1]
            if idents in ("name",) and "scheduler" in f.file:
                f.classification, f.reason = CLASS_FALSE, "ジョブ名（固定識別子）で個人情報ではない"
            else:
                f.classification, f.reason = CLASS_HOLD, "例外文字列に PII らしき識別子（API 応答本文・読取テキスト等・到達範囲を要目視）"
        else:
            f.classification, f.reason = CLASS_FIX, "sink に PII らしき識別子が emit() なしで渡る（台帳外）"
    elif f.category == CATEGORY_B:
        marks = f.extra.get("marks_in_function", [])
        callee = f.extra.get("callee", "")
        if callee in BUSINESS_NOTIFY_FUNCS:
            f.classification, f.reason = CLASS_FALSE, "業務（事務所）LINE への固定文言通知＝相談者宛の送信ではない"
        elif callee in ("update_record", "create_record", "create_records", "upload_file"):
            if marks:
                f.classification, f.reason = CLASS_FALSE, f"kintone 書込・承認／CAS 文脈あり（{', '.join(marks[:3])}）"
            else:
                f.classification, f.reason = CLASS_HOLD, "kintone 書込（票で裁定された自動欄・空欄のみ・候補行か要目視。承認なし書込ゼロの原則との照合）"
        elif marks:
            f.classification, f.reason = CLASS_FALSE, f"関数内に承認文脈の印あり（{', '.join(marks[:4])}）"
        elif f.file.startswith(BOT_REPLY_MODULES):
            f.classification, f.reason = CLASS_HOLD, "bot の自動返信経路（許可表 V2・サーバーガード・凍結定型で統制＝設計上の自動送信。新設ではないか要目視）"
        else:
            f.classification, f.reason = CLASS_FIX, "承認文脈の印が無い相談者宛の送信が bot 経路以外にある（経路の正当性を要確認）"
    elif f.category == CATEGORY_C:
        if "drop_table" in f.detail:
            f.classification, f.reason = CLASS_FALSE, "alembic downgrade（データがあれば拒否する規約）"
        elif ("rmtree" in f.detail or "os.remove" in f.detail) and f.file.startswith(TEMP_CLEANUP_FILES):
            f.classification, f.reason = CLASS_FALSE, "一時ディレクトリの掃除（テスト支援・ローカルツールの staging）"
        elif "rmtree" in f.detail or "os.remove" in f.detail:
            f.classification, f.reason = CLASS_HOLD, "ファイル削除（対象が一時領域か要目視）"
        elif "delete_record" in f.detail and f.detail.startswith("kintone.delete_record: async def"):
            f.classification, f.reason = CLASS_FIX, "kintone の物理削除 API の器（呼出 0・RV-08: 論理削除の原則＝器を残さない）"
        elif "delete_record" in f.detail:
            f.classification, f.reason = CLASS_FIX, "kintone の物理削除 API の呼出（RV-08: 論理削除に置き換える）"
        elif "signature_nonce" in f.detail or "expires_at" in f.detail:
            f.classification, f.reason = CLASS_FALSE, "期限切れ nonce（署名の再送防止の一時値）の掃除・監査対象データではない"
        else:
            f.classification, f.reason = CLASS_HOLD, "DELETE（対象表と理由を要目視）"
    elif f.category == CATEGORY_D:
        if f.detail.startswith("long hex literal") and "64 文字" in f.detail:
            f.classification, f.reason = CLASS_FALSE, "SHA-256 pin（雛形・凍結文の同一性検証用。秘密ではない）"
        elif "health_deps" in f.file:
            f.classification, f.reason = CLASS_FALSE, "画像ヘッダ（PNG magic）等の固定バイト列の hex 表現・秘密ではない"
        elif f.detail.startswith("assignment"):
            f.classification, f.reason = CLASS_HOLD, "password／token の直書きらしき行（docstring の例示か要目視）"
        else:
            f.classification, f.reason = CLASS_HOLD, "秘密値らしき文字列（固定 pin／テスト値の可能性・要目視）"
    elif f.category == CATEGORY_E_FUNC:
        f.classification, f.reason = CLASS_HOLD, "未使用候補（動的参照・外部入口の可能性・削除は大野判断）"
    elif f.category == CATEGORY_E_MOD:
        name = f.file.split("/")[-1]
        if name.startswith(STANDALONE_MODULE_PREFIXES):
            f.classification, f.reason = CLASS_FALSE, "手動実行の収載スクリプト／単体ツール（import されないのが正常）"
        else:
            f.classification, f.reason = CLASS_HOLD, "import 参照 0 の module（動的 import・外部実行の可能性）"


# ── 実行 ─────────────────────────────────────────────────────────────────────
def run(repo: Path) -> list[Finding]:
    repo = repo.resolve()
    files = iter_py_files(repo, include_tests=False)
    all_files = iter_py_files(repo, include_tests=True)
    allow = load_allowlist(repo)
    findings: list[Finding] = []
    for p in files:
        rel = p.relative_to(repo).as_posix()
        src = p.read_text(encoding="utf-8", errors="replace")
        lines = src.split("\n")
        try:
            tree = ast.parse(src, filename=rel)
        except SyntaxError:
            findings.append(Finding(CATEGORY_A, rel, 1, "parse error", classification=CLASS_HOLD, reason="構文解析失敗"))
            continue
        findings += audit_pii_sinks(rel, tree, lines, allow)
        findings += audit_send_paths(rel, tree, lines, src)
        findings += audit_deletes(rel, lines)
        findings += audit_secrets(rel, tree, lines)
    findings += audit_unused(repo, files, all_files)
    for f in findings:
        if not f.reason:
            classify(f)
    return findings


def summarize(findings: list[Finding]) -> dict:
    out: dict = {}
    for f in findings:
        c = out.setdefault(f.category, {CLASS_FIX: 0, CLASS_FALSE: 0, CLASS_HOLD: 0, "total": 0})
        c[f.classification] += 1
        c["total"] += 1
    return out


def to_markdown(findings: list[Finding], title: str) -> str:
    summ = summarize(findings)
    lines = [f"# {title}", ""]
    lines.append("| 区分 | 件数 | 要修正 | 誤検知 | 判断保留 |")
    lines.append("|---|---|---|---|---|")
    for cat in (CATEGORY_A, CATEGORY_B, CATEGORY_C, CATEGORY_D, CATEGORY_E_FUNC, CATEGORY_E_MOD):
        c = summ.get(cat, {CLASS_FIX: 0, CLASS_FALSE: 0, CLASS_HOLD: 0, "total": 0})
        lines.append(f"| {cat} | {c['total']} | {c[CLASS_FIX]} | {c[CLASS_FALSE]} | {c[CLASS_HOLD]} |")
    lines.append("")
    for cat in (CATEGORY_A, CATEGORY_B, CATEGORY_C, CATEGORY_D, CATEGORY_E_FUNC, CATEGORY_E_MOD):
        rows = [f for f in findings if f.category == cat]
        lines.append(f"## {cat}（{len(rows)} 件）")
        lines.append("")
        if not rows:
            lines.append("なし")
            lines.append("")
            continue
        lines.append("| # | ファイル:行 | 内容 | 仮分類 | 理由 |")
        lines.append("|---|---|---|---|---|")
        for i, f in enumerate(sorted(rows, key=lambda x: (x.file, x.line)), start=1):
            detail = f.detail.replace("|", "｜")
            lines.append(f"| {i} | {f.file}:{f.line} | {detail} | {f.classification} | {f.reason} |")
        lines.append("")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--out", required=True)
    ap.add_argument("--json", default="")
    ap.add_argument("--title", default="audit_static 結果")
    a = ap.parse_args(argv)
    findings = run(Path(a.repo))
    md = to_markdown(findings, a.title)
    Path(a.out).write_text(md, encoding="utf-8", newline="\n")
    if a.json:
        Path(a.json).write_text(json.dumps([asdict(f) for f in findings], ensure_ascii=False, indent=1), encoding="utf-8")
    print("audit_static: wrote report")          # sink 方針: 定数のみ
    return 0


if __name__ == "__main__":
    sys.exit(main())
