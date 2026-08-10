"""MAINT-3: (A) PWA fetch 閉集合ラッパー・(B) App33 取得済み戸籍の最小表示。

- A（R-P4-004-2 L01 の発火条件充足）: 生 fetch は webapp/app.js の app_fetch
  ラッパー内の 1 箇所のみ。全ページ（cases/case/approvals/kinship）は
  app_fetch 経由・引数は "/app/api/" 固定 prefix のリテラル連結のみ。
  他のネットワーク API（XHR/WebSocket 等）は webapp 全域で不使用。挙動変更なし
  （ラッパーは従来の fetch(path, {redirect:"follow"}) と同値）。
- B（P4-005 申し送り・正本 §2 の App33 言及の範囲）: 取得済み戸籍の最小
  read-only 一覧（本籍・筆頭者・従前戸籍の本籍のみ・chain 判定なし＝参考判定の
  提示は SHOKUMU-PLAN 票の領分）。env 未設定は空リスト縮退。
"""

import asyncio
import os
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
from hub.webapp_auth import (  # noqa: E402
    MIN_ITERATIONS,
    hash_password,
    issue_session,
)
from kinship_graph import (  # noqa: E402
    KinshipGraph,
    PersonNode,
    load_koseki_summaries_for_case,
)

_client = TestClient(main.app)
_ENV = {
    "WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
    "WEBAPP_SESSION_SECRET": "s" * 32,
}
_WEBAPP = Path("webapp")
_DATA_PAGES = ("cases.html", "case.html", "approvals.html", "kinship.html")


def _run(coro):
    return asyncio.run(coro)


def _auth_headers():
    return {"Cookie": f"webapp_session={issue_session()}"}


# ── fix2 M01: JS 字句状態機械（正規表現ベースのコメント除去を撤廃・両時点の
#    偽陰性/偽陽性を解消）────────────────────────────────────────────────────
# 除算の直前に来得る有意文字（これ以外の直後の '/' は正規表現リテラル開始と
# 判定＝標準ヒューリスティック。'}' はブロック終端直後の文再開があり得るため
# リテラル開始側に倒す——本 webapp のコード形に「} / 除算」となる式は無い）
_DIV_PREV = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$)]")


def _js_executable_text(src: str) -> tuple[str, bool]:
    """JS ソースから**コメントのみ**を空白化したテキストを返す（fix2 M01）。

    文字単位の状態機械で '…'／"…"／`テンプレート`（\\ エスケープ・${} 内の
    式コードの入れ子を含む）／// 行コメント／/* */ ブロックコメント／正規表現
    リテラル（文字クラス [ ] 内の / を終端と誤認しない）を区別する。
    - **コメント内のみ除外**（空白化・行構造は保持）
    - **文字列・テンプレート内の fetch は検出対象のまま残す**（"fetch" 文字列
      アクセス迂回の禁止対象）
    - 実行コード・正規表現リテラル本文はそのまま残す（保守側＝除外はしない）
    戻り値: (テキスト, EOF が clean か＝文字列/コメント/正規表現/テンプレートの
    途中で終わっていないか)。
    """
    out: list[str] = []
    state = "code"
    tpl_stack: list[int] = []    # `${` の brace 入れ子（code→tpl 復帰管理）
    prev_sig = ""
    escape = False
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        nxt = src[i + 1] if i + 1 < n else ""
        if state == "code":
            if c == "/" and nxt == "/":
                state = "line"; out.append("  "); i += 2; continue
            if c == "/" and nxt == "*":
                state = "block"; out.append("  "); i += 2; continue
            if c == "/" and prev_sig not in _DIV_PREV:
                state = "regex"; out.append(c); i += 1; continue
            if c == "'":
                state = "sq"
            elif c == '"':
                state = "dq"
            elif c == "`":
                state = "tpl"
            elif tpl_stack and c == "{":
                tpl_stack[-1] += 1
            elif tpl_stack and c == "}":
                tpl_stack[-1] -= 1
                if tpl_stack[-1] == 0:
                    tpl_stack.pop()
                    state = "tpl"
            if not c.isspace():
                prev_sig = c
            out.append(c); i += 1; continue
        if state in ("sq", "dq", "tpl"):
            if escape:
                escape = False; out.append(c); i += 1; continue
            if c == "\\":
                escape = True; out.append(c); i += 1; continue
            if state == "sq" and c == "'":
                state = "code"; prev_sig = "'"
            elif state == "dq" and c == '"':
                state = "code"; prev_sig = '"'
            elif state == "tpl":
                if c == "`":
                    state = "code"; prev_sig = "`"
                elif c == "$" and nxt == "{":
                    out.append("${"); i += 2
                    tpl_stack.append(1); state = "code"; prev_sig = "{"
                    continue
            out.append(c); i += 1; continue
        if state == "line":
            if c == "\n":
                state = "code"; out.append(c)
            else:
                out.append(" ")
            i += 1; continue
        if state == "block":
            if c == "*" and nxt == "/":
                state = "code"; out.append("  "); i += 2; continue
            out.append(c if c == "\n" else " "); i += 1; continue
        if state == "regex":
            if escape:
                escape = False; out.append(c); i += 1; continue
            if c == "\\":
                escape = True; out.append(c); i += 1; continue
            if c == "[":
                state = "regex_class"
            elif c == "/":
                state = "code"; prev_sig = "/"
            out.append(c); i += 1; continue
        if state == "regex_class":
            if escape:
                escape = False; out.append(c); i += 1; continue
            if c == "\\":
                escape = True; out.append(c); i += 1; continue
            if c == "]":
                state = "regex"
            out.append(c); i += 1; continue
    # fix3: EOF が行コメント中（state="line"）は clean（JS 仕様上、行コメントは
    # 行末または EOF で正しく終端する——前巡の偽陽性解消）
    clean = (state in ("code", "line") and not tpl_stack and not escape)
    return "".join(out), clean


_SCRIPT_RE = re.compile(r"<script[^>]*>(.*?)</script>", re.S)


def _scan_units(name: str, src: str) -> list[tuple[str, str, bool]]:
    """検査単位への分解: (単位名, テキスト, JS lexer 適用有無)。

    - .js: 全体を JS として lexer 適用
    - .html: <script> 本文は lexer 適用・**script 外の残余は生のまま走査**
      （HTML コメントも除外しない保守側＝属性ハンドラ等の迂回も token で検出）
    - その他（.json 等）: 生のまま走査
    """
    if name.endswith(".js"):
        return [(name, src, True)]
    if name.endswith(".html"):
        units = [(f"{name}#script{i}", m.group(1), True)
                 for i, m in enumerate(_SCRIPT_RE.finditer(src))]
        units.append((f"{name}#html", _SCRIPT_RE.sub("", src), False))
        return units
    return [(name, src, False)]


# 生 fetch への到達手段の網羅（fix1 M02a）: 識別子 token としての fetch を
# 前後境界つきで検出——fetch( / fetch (空白) / fetch?.( / (0, fetch) /
# "fetch"・'fetch' の文字列リテラル（["fetch"] 動的アクセス）をすべて含む
_FETCH_TOKEN = re.compile(r"(?<![A-Za-z0-9_$])fetch(?![A-Za-z0-9_$])")


# ── fix3 M01（司令塔裁定=Codex 第3案）: 構築規律の閉集合禁止 ─────────────────
# 検出の高度化でなく「API 名を実行時に組み立てる手段」自体を webapp 全域で禁止
# する。JS 検査は lexer 適用後テキスト（コメントのみ除外・文字列は対象）に行う。
_JS_BANNED = [
    ("globalThis",
     re.compile(r"(?<![A-Za-z0-9_$.])globalThis(?![A-Za-z0-9_$])")),
    ('文字列リテラル computed access（x["…"]/x[\'…\']）',
     None),   # 判定は _has_string_computed_access（keyword 直前の配列リテラル除外）
    ("\\x エスケープ", re.compile(r"\\x[0-9a-fA-F]")),
    ("\\u エスケープ", re.compile(r"\\u[0-9a-fA-F{]")),
    ("eval", re.compile(r"(?<![A-Za-z0-9_$.])eval(?![A-Za-z0-9_$])")),
    ("Function コンストラクタ",
     re.compile(r"(?<![A-Za-z0-9_$.])(?:new\s+)?Function\s*\(")),
    ("文字列引数の setTimeout/setInterval",
     re.compile(r"(?<![A-Za-z0-9_$])set(?:Timeout|Interval)\s*\(\s*[\"'`]")),
    ("動的 import()", re.compile(r"(?<![A-Za-z0-9_$.])import\s*\(")),
    # PC-A 追加の防壁（裁定閉集合への上乗せ・完了報告に明記）: グローバル
    # オブジェクト token 自体を禁止——「変数へ組み立てた文字列で window[v] を
    # 引く」残余経路を入口（グローバル参照）側で塞ぐ。self は Service Worker
    # （sw.js）のみ許可
    ("グローバルオブジェクト参照（window/top/parent/frames）",
     re.compile(r"(?<![A-Za-z0-9_$.])(?:window|top|parent|frames)"
                r"(?![A-Za-z0-9_$])")),
]
_JS_BANNED_SELF = (
    "グローバルオブジェクト参照（self・sw.js 以外で禁止）",
    re.compile(r"(?<![A-Za-z0-9_$.])self(?![A-Za-z0-9_$])"))
_HTML_BANNED = [
    ("インラインイベント属性（on*=）", re.compile(r"\son[a-zA-Z]+\s*=")),
    ("数値文字参照（&#…;）", re.compile(r"&#")),
]


# 直前語が JS キーワードなら [ は配列リテラル（computed access でない）
_JS_KEYWORDS_BEFORE_ARRAY = frozenset(
    {"of", "in", "return", "typeof", "case", "do", "else", "void", "delete",
     "new", "yield", "await", "instanceof"})
_COMPUTED_RE = re.compile(
    r"([A-Za-z_$][A-Za-z0-9_$]*|\)|\])\s*\[\s*[\"']")


def _has_string_computed_access(text: str) -> bool:
    """識別子/閉じ括弧の直後の ["…"]/['…']（文字列リテラル computed access）。
    keyword（for-of 等）直後の配列リテラルは除外（fix3 の偽陽性対策）。"""
    for m in _COMPUTED_RE.finditer(text):
        if m.group(1) not in _JS_KEYWORDS_BEFORE_ARRAY:
            return True
    return False


def _discipline_violations(unit: str, text: str, is_js: bool,
                           allow_self: bool = False) -> list[str]:
    """構築規律違反の列挙（fix3 M01）。JS は lexer 適用済みテキストを渡すこと
    （コメントのみ除外・文字列/テンプレート/正規表現は検査対象のまま）。"""
    rules = list(_JS_BANNED) if is_js else list(_HTML_BANNED)
    if is_js and not allow_self:
        rules.append(_JS_BANNED_SELF)
    out = []
    for label, pat in rules:
        if pat is None:
            if _has_string_computed_access(text):
                out.append(f"{unit}: {label}")
        elif pat.search(text):
            out.append(f"{unit}: {label}")
    return out


class TestFetchWrapperClosedSet(unittest.TestCase):
    """A: fetch ラッパー閉集合化の静的 pin（fix1 M02→fix2 M01→fix3 M01）。

    本検査群の保証（fix3 で境界を固定）:
    - **fetch token の物理集約**——識別子/文字列 token としての fetch は app.js
      の app_fetch ブロック内 1 箇所のみ（コメントのみ除外の字句走査）。
    - **迂回に必要な構文の全面禁止**（TestConstructionDiscipline）——globalThis・
      文字列リテラル computed access・\\x/\\u エスケープ・eval/Function・文字列
      引数タイマー・動的 import()・グローバルオブジェクト token（self は sw.js
      のみ）・インラインイベント属性・数値文字参照。
    残る理論的経路は**上記の禁止構文を将来解除した場合のみ**——解除は司令塔裁定
    事項（規律変更として本テスト群の改定を伴う）。実行水準の確認は実機スモーク。
    """

    def _all_sources(self):
        return {p.name: p.read_text(encoding="utf-8")
                for p in sorted(_WEBAPP.iterdir()) if p.is_file()}

    @staticmethod
    def _wrapper_block(app_js_src: str) -> str:
        m = re.search(r"async function app_fetch\(path\) \{(.*?)\n\}",
                      app_js_src, flags=re.S)
        assert m, "app_fetch 関数ブロックが見つからない"
        return m.group(1)

    def test_raw_fetch_token_unreachable_outside_wrapper(self):
        # fix2 M01: 字句状態機械（コメントのみ除外・文字列/実行コードは走査）で
        # 全 webapp ソースを検査単位に分解し、fetch token（呼出し・空白付き・
        # fetch?.・(0, fetch)・"fetch" リテラルを包含）が app.js の app_fetch
        # ブロック内の 1 箇所のみであることを pin。各 JS 単位に EOF clean の
        # 健全性 assert を適用
        for name, src in self._all_sources().items():
            for unit, text, is_js in _scan_units(name, src):
                if is_js:
                    text, clean = _js_executable_text(text)
                    self.assertTrue(clean, f"{unit}: lexer が EOF で非 clean")
                hits = _FETCH_TOKEN.findall(text)
                if name != "app.js":
                    self.assertEqual(
                        hits, [], f"{unit}: ラッパー外に fetch 到達手段がある")
        app_js_text, clean = _js_executable_text(
            (_WEBAPP / "app.js").read_text(encoding="utf-8"))
        self.assertTrue(clean)
        self.assertEqual(len(_FETCH_TOKEN.findall(app_js_text)), 1)
        block_text, _ = _js_executable_text(self._wrapper_block(
            (_WEBAPP / "app.js").read_text(encoding="utf-8")))
        self.assertEqual(len(_FETCH_TOKEN.findall(block_text)), 1,
                         "生 fetch が app_fetch ブロック外にある")

    def test_wrapper_structure_prefix_throw_fetch_order(self):
        # fix1 M02b: 文字列存在でなく**構造**を検証——app_fetch ブロック内で
        # 「prefix 検査（if）→ throw → fetch 呼出し」がこの行順で存在し、
        # throw が if ブロック内（fetch より前に閉じる）にあること。
        # コメント行は除去してから照合（コメント化による迂回を検出）
        src = (_WEBAPP / "app.js").read_text(encoding="utf-8")
        self.assertIn('const APP_API_PREFIX = "/app/api/";', src)
        block, _ = _js_executable_text(self._wrapper_block(src))
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        idx_if = next((i for i, ln in enumerate(lines)
                       if ln.startswith("if (")
                       and "path.startsWith(APP_API_PREFIX)" in ln
                       and 'typeof path !== "string"' in ln), None)
        idx_throw = next((i for i, ln in enumerate(lines)
                          if ln.startswith("throw new Error")), None)
        idx_close = next((i for i, ln in enumerate(lines) if ln == "}"), None)
        idx_fetch = next((i for i, ln in enumerate(lines)
                          if ln == 'return fetch(path, {redirect: "follow"});'),
                         None)
        self.assertIsNotNone(idx_if, "prefix 検査の if が無い")
        self.assertIsNotNone(idx_throw, "throw が無い（コメント化を含む）")
        self.assertIsNotNone(idx_close)
        self.assertIsNotNone(idx_fetch, "固定オプションの fetch 呼出しが無い")
        # 順序: if → throw → if 閉じ → fetch（throw は if ブロック内）
        self.assertLess(idx_if, idx_throw)
        self.assertLess(idx_throw, idx_close)
        self.assertLess(idx_close, idx_fetch)

    def test_pages_use_wrapper_and_include_appjs(self):
        for name in _DATA_PAGES:
            with self.subTest(page=name):
                src = (_WEBAPP / name).read_text(encoding="utf-8")
                self.assertIn("app_fetch(", src)
                self.assertIn('<script src="/app/app.js"></script>', src)

    def test_appfetch_call_sites_use_api_path_literals(self):
        # 全呼出しの第1引数が "/app/api/" 固定 prefix のリテラル連結で始まる
        found = 0
        for name in _DATA_PAGES:
            src = (_WEBAPP / name).read_text(encoding="utf-8")
            for m in re.finditer(r'app_fetch\(\s*"([^"]*)"', src):
                found += 1
                self.assertTrue(m.group(1).startswith("/app/api/"),
                                f"{name}: {m.group(1)}")
            # リテラル以外の第1引数（変数直渡し等）が無いこと
            for m in re.finditer(r"app_fetch\(\s*([^\s\")])", src):
                self.fail(f"{name}: app_fetch の第1引数がリテラルでない: "
                          f"{m.group(1)}")
        self.assertEqual(found, 4)

    def test_throw_reachability_by_brace_depth(self):
        """fix2 L01（(b) 採用・node 不在のため静的解析）: throw が prefix 検査
        if の**直下（brace 深度 +1）**にあり、fetch 呼出しが if ブロックの外
        （深度 0）で throw より後にあることを brace 深度で pin。

        限界（明記）: 静的解析であり「実行時に throw が実際に投げられ fetch が
        呼ばれない」ことの実測ではない（node 実行環境がローカルに無いため
        (a) 実行テストは採らない）。if(false) ラップ等の**制御条件の無効化**は
        本検査では検出できず、深度・順序の改変（ブロック外移動・コメント化）
        のみを検出する。実行水準の確認は実機スモーク（[人]）事項。
        """
        src = (_WEBAPP / "app.js").read_text(encoding="utf-8")
        block, _ = _js_executable_text(self._wrapper_block(src))
        depth = 0
        if_depth = throw_depth = fetch_depth = None
        if_pos = throw_pos = fetch_pos = None
        for m in re.finditer(r"\{|\}|if \(typeof path|throw new Error"
                             r"|return fetch\(path", block):
            tok = m.group(0)
            if tok == "{":
                depth += 1
            elif tok == "}":
                depth -= 1
            elif tok.startswith("if ("):
                if_depth, if_pos = depth, m.start()
            elif tok.startswith("throw"):
                throw_depth, throw_pos = depth, m.start()
            else:
                fetch_depth, fetch_pos = depth, m.start()
        self.assertIsNotNone(if_pos, "prefix 検査 if が無い")
        self.assertIsNotNone(throw_pos, "throw が無い")
        self.assertIsNotNone(fetch_pos, "fetch 呼出しが無い")
        self.assertEqual(if_depth, 0)
        self.assertEqual(throw_depth, 1,
                         "throw が prefix 検査 if の直下（深度+1）にない")
        self.assertEqual(fetch_depth, 0, "fetch が if ブロック内にある")
        self.assertLess(if_pos, throw_pos)
        self.assertLess(throw_pos, fetch_pos)

    def test_no_other_network_apis_anywhere(self):
        for name, src in self._all_sources().items():
            for banned in ("XMLHttpRequest", "WebSocket", "EventSource",
                           "sendBeacon", "importScripts"):
                self.assertNotIn(banned, src, name)
        # sw.js は fetch handler を登録しない（P4-001 裁定の維持）
        sw = (_WEBAPP / "sw.js").read_text(encoding="utf-8")
        self.assertNotIn('addEventListener("fetch"', sw)


class TestConstructionDiscipline(unittest.TestCase):
    """fix3 M01: webapp 全域の構築規律 pin（禁止構文の不在・実査済み=現行ゼロ）。"""

    def test_webapp_has_no_banned_constructs(self):
        for p in sorted(_WEBAPP.iterdir()):
            if not p.is_file():
                continue
            src = p.read_text(encoding="utf-8")
            for unit, text, is_js in _scan_units(p.name, src):
                if is_js:
                    text, clean = _js_executable_text(text)
                    self.assertTrue(clean, f"{unit}: lexer が EOF で非 clean")
                violations = _discipline_violations(
                    unit, text, is_js, allow_self=(p.name == "sw.js"))
                self.assertEqual(violations, [])

    def test_discipline_detects_codex_bypass_forms(self):
        # Codex 例示の迂回 3 形＋派生形が規律違反として検出されること（unit 対照）
        cases = [
            ('globalThis["fe" + "tch"]("/x");',
             ["globalThis", "computed access"]),
            ('const f = "\\x66etch";', ["\\x エスケープ"]),
            ('const f = "\\u0066etch";', ["\\u エスケープ"]),
            ("const \\u0066etch = 1;", ["\\u エスケープ"]),   # エスケープ識別子
            ('const w = window; w[v]("/x");',
             ["グローバルオブジェクト参照"]),
            ('eval("fe" + "tch");', ["eval"]),
            ('new Function("return this")();', ["Function"]),
            ('setTimeout("code()", 1);', ["setTimeout"]),
            ('import("./m.js");', ["import"]),
        ]
        for snippet, expect_frags in cases:
            with self.subTest(snippet=snippet):
                text, clean = _js_executable_text(snippet)
                self.assertTrue(clean)
                violations = _discipline_violations("u", text, is_js=True)
                for frag in expect_frags:
                    self.assertTrue(
                        any(frag in v for v in violations),
                        f"{frag} が検出されない: {violations}")

    def test_discipline_detects_html_entity_event_attr(self):
        html = '<img src=x onerror="&#102;etch(\'/x\')">'
        violations = _discipline_violations("u", html, is_js=False)
        self.assertTrue(any("インラインイベント属性" in v for v in violations))
        self.assertTrue(any("数値文字参照" in v for v in violations))

    def test_string_concat_use_requires_banned_syntax(self):
        # "fe"+"tch" の連結**単体**は禁止対象でない（無害なデータ）が、これを
        # API 名として**使う**には computed access / globalThis / eval 等の
        # 禁止構文が必ず要る——使用形が全て検出されることの対照
        harmless = 'const s = "fe" + "tch";'
        text, _ = _js_executable_text(harmless)
        self.assertEqual(_discipline_violations("u", text, is_js=True), [])
        for use in ('globalThis[s]("/x");', 'const w = window; w[s]("/x");',
                    'eval(s + "(\'/x\')");'):
            with self.subTest(use=use):
                text, _ = _js_executable_text(harmless + use)
                self.assertTrue(
                    _discipline_violations("u", text, is_js=True))


class TestJsLexer(unittest.TestCase):
    """fix2 M01: 字句状態機械の対照テスト（前巡の偽陰性2形・偽陽性1形を含む）。"""

    @staticmethod
    def _hits(snippet: str) -> list[str]:
        text, clean = _js_executable_text(snippet)
        assert clean, "対照 snippet は clean EOF 前提"
        return _FETCH_TOKEN.findall(text)

    def test_false_negative_1_line_comment_marker_in_string(self):
        # 前巡偽陰性1: "//" を含む文字列の後の実行コード fetch を見逃さない
        self.assertEqual(
            self._hits('const marker = "//"; fetch("/x");'), ["fetch"])

    def test_false_negative_2_block_comment_marker_in_string(self):
        # 前巡偽陰性2: "/*" を含む文字列で以降が丸ごと消えない
        self.assertEqual(
            self._hits('const begin = "/*"; fetch("/x"); const end = "*/";'),
            ["fetch"])

    def test_false_positive_label_line_comment(self):
        # 前巡偽陽性: `:` 直後の // 行コメント（(?<!:) lookbehind の取り零し）
        # ——コメント内の fetch は検出しない
        self.assertEqual(
            self._hits("switch (x) { default: // fetch(\n }"), [])

    def test_string_fetch_still_detected(self):
        # 文字列内 fetch は検出対象のまま（"fetch" 動的アクセス迂回の禁止）
        self.assertEqual(self._hits('const f = window["fetch"];'), ["fetch"])

    def test_template_literal_with_expression_code(self):
        # テンプレート ${} 内の式コードも走査対象（迂回不可）
        self.assertEqual(self._hits("const t = `x${fetch(\"/y\")}z`;"),
                         ["fetch"])
        self.assertEqual(self._hits("const t = `// not comment`;"), [])

    def test_regex_literal_slashes_do_not_open_comment(self):
        # 正規表現リテラル内の // をコメントと誤認しない（後続コードを走査）
        self.assertEqual(
            self._hits('const re = /https:\\/\\//; fetch("/x");'), ["fetch"])
        # 文字クラス内の / で終端を誤認しない
        self.assertEqual(self._hits('const re = /[/]/; fetch("/x");'),
                         ["fetch"])

    def test_division_not_treated_as_regex(self):
        # 除算の / を正規表現開始と誤認して後続を飲み込まない
        self.assertEqual(self._hits('const a = b / c; fetch("/x");'),
                         ["fetch"])

    def test_comments_do_not_hide_and_are_excluded(self):
        self.assertEqual(self._hits("// fetch(\n/* fetch( */ const a = 1;"),
                         [])

    def test_eof_unclean_detected(self):
        for snippet in ('const s = "unterminated', "/* open comment",
                        "const t = `open template"):
            with self.subTest(snippet=snippet):
                _text, clean = _js_executable_text(snippet)
                self.assertFalse(clean)

    def test_eof_in_line_comment_is_clean(self):
        # fix3: 行コメントで EOF は clean（JS 仕様準拠・前巡の偽陽性解消）
        text, clean = _js_executable_text("const a = 1; // trailing comment")
        self.assertTrue(clean)
        self.assertNotIn("trailing", text)     # コメント本文は除外されている

    def test_nested_template_expression(self):
        # 入れ子 template（`${`…${…}…`}`）の ${} 入れ子復帰と走査対象化
        snippet = "const t = `a${`b${fetch(\"/x\")}c`}d`;"
        text, clean = _js_executable_text(snippet)
        self.assertTrue(clean)
        self.assertEqual(_FETCH_TOKEN.findall(text), ["fetch"])


class TestKosekiSummaries(unittest.TestCase):
    """B: App33 最小一覧の loader（純関数部）と API 同梱。"""

    def test_loader_env_unset_returns_empty_without_search(self):
        search = AsyncMock()
        with patch.dict(os.environ, {"APP_KOSEKI_BOOK": "",
                                     "TOKEN_KOSEKI_BOOK": ""}), \
             patch("hub.kintone.search_records", new=search):
            self.assertEqual(_run(load_koseki_summaries_for_case("9")), [])
        search.assert_not_awaited()          # 縮退＝kintone 未到達

    def test_loader_parses_reading_and_tolerates_broken_json(self):
        records = [
            {"$id": {"value": "70"},
             "読解JSON": {"value": '{"戸籍": {"本籍": "川口市大字X", '
                                   '"筆頭者": "山田太郎", '
                                   '"従前戸籍": {"本籍": "足立区Y"}}}'}},
            {"$id": {"value": "71"}, "読解JSON": {"value": "{{broken"}},
        ]
        with patch.dict(os.environ, {"APP_KOSEKI_BOOK": "33",
                                     "TOKEN_KOSEKI_BOOK": "t33"}), \
             patch("hub.kintone.search_records",
                   new=AsyncMock(return_value=records)):
            out = _run(load_koseki_summaries_for_case("9"))
        self.assertEqual(out, [
            {"record_id": "70", "honseki": "川口市大字X",
             "hittousha": "山田太郎", "juzen_honseki": "足立区Y"},
            {"record_id": "71", "honseki": "", "hittousha": "",
             "juzen_honseki": ""},          # 解釈不能は空欄（行は落とさない）
        ])

    def test_loader_defends_all_type_breakage_shapes(self):
        # fix1 M01: 各階層の型崩れ（レビュー例示 2 形＋追加形）でも例外を漏らさず
        # 「当該項目を空欄・行は維持」
        records = [
            {"$id": {"value": "80"},
             "読解JSON": {"value": '{"戸籍": "読解不能"}'}},           # 例示1
            {"$id": {"value": "81"},
             "読解JSON": {"value":
                          '{"戸籍": {"従前戸籍": "読解不能", '
                          '"本籍": "川口市"}}'}},                      # 例示2
            {"$id": {"value": "82"}, "読解JSON": {"value": "[1, 2]"}},  # 非dict
            {"$id": {"value": "83"},
             "読解JSON": {"value": '{"戸籍": {"本籍": 123, '
                                   '"筆頭者": ["山"], '
                                   '"従前戸籍": {"本籍": {"x": 1}}}}'}},
            {"$id": {"value": "84"}, "読解JSON": {"value": 42}},       # 値が数値
            {"$id": "broken", "読解JSON": []},                          # 階層崩れ
        ]
        with patch.dict(os.environ, {"APP_KOSEKI_BOOK": "33",
                                     "TOKEN_KOSEKI_BOOK": "t33"}), \
             patch("hub.kintone.search_records",
                   new=AsyncMock(return_value=records)):
            out = _run(load_koseki_summaries_for_case("9"))
        self.assertEqual(len(out), 6)                 # 行はすべて維持
        by_id = {row["record_id"]: row for row in out}
        blank = {"honseki": "", "hittousha": "", "juzen_honseki": ""}
        self.assertEqual({k: by_id["80"][k] for k in blank}, blank)
        self.assertEqual(by_id["81"],
                         {"record_id": "81", "honseki": "川口市",
                          "hittousha": "", "juzen_honseki": ""})
        for rid in ("82", "83", "84", ""):
            self.assertEqual({k: by_id[rid][k] for k in blank}, blank)

    def test_api_stays_closed_set_200_with_broken_readings(self):
        # fix1 M01: 型崩れデータで実 loader を通しても API は閉集合 status の
        # 200（500 の根絶を API 面で pin）
        graph = KinshipGraph(nodes=[PersonNode(
            record_id="10", name="被相続人", alive="死亡",
            death_date="2026-01-01", is_decedent=True,
            meyose="確定", kakunin="確認済")])
        records = [
            {"$id": {"value": "80"},
             "読解JSON": {"value": '{"戸籍": "読解不能"}'}},
            {"$id": {"value": "81"},
             "読解JSON": {"value": '{"戸籍": {"従前戸籍": "読解不能"}}'}},
        ]
        env = dict(_ENV)
        env.update({"APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33"})
        with patch.dict(os.environ, env), \
             patch("kinship_graph.load_graph_for_case",
                   new=AsyncMock(return_value=graph)), \
             patch("hub.kintone.search_records",
                   new=AsyncMock(return_value=records)), \
             patch("hub.webapp_kinship_view._overlay_for_case",
                   new=AsyncMock(return_value=None)), \
             patch("kinship_renderer.render_kinship",
                   new=MagicMock(return_value=b"<svg xmlns='x'></svg>")):
            r = _client.get("/app/api/kinship?case=9",
                            headers=_auth_headers(), follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data["status"], "ok")        # 閉集合 status のまま
        self.assertEqual(len(data["kosekis"]), 2)     # 行は空欄付きで残る
        self.assertEqual(data["kosekis"][0]["honseki"], "")

    def test_api_includes_kosekis_in_ok_and_not_renderable(self):
        from kinship_renderer import KinshipValidationRejected
        graph = KinshipGraph(nodes=[PersonNode(
            record_id="10", name="被相続人", alive="死亡",
            death_date="2026-01-01", is_decedent=True,
            meyose="確定", kakunin="確認済")])
        summaries = [{"record_id": "70", "honseki": "川口市",
                      "hittousha": "山田", "juzen_honseki": ""}]
        env = dict(_ENV)
        render = MagicMock(return_value=b"<svg xmlns='x'></svg>")
        with patch.dict(os.environ, env), \
             patch("kinship_graph.load_graph_for_case",
                   new=AsyncMock(return_value=graph)), \
             patch("kinship_graph.load_koseki_summaries_for_case",
                   new=AsyncMock(return_value=summaries)), \
             patch("hub.webapp_kinship_view._overlay_for_case",
                   new=AsyncMock(return_value=None)), \
             patch("kinship_renderer.render_kinship", new=render):
            r = _client.get("/app/api/kinship?case=9",
                            headers=_auth_headers(), follow_redirects=False)
            self.assertEqual(r.json()["kosekis"], summaries)
            self.assertEqual(r.json()["status"], "ok")
            render.side_effect = KinshipValidationRejected(["No.10: x"])
            r2 = _client.get("/app/api/kinship?case=9",
                             headers=_auth_headers(), follow_redirects=False)
            self.assertEqual(r2.json()["status"], "not_renderable")
            self.assertEqual(r2.json()["kosekis"], summaries)

    def test_page_renders_koseki_table_safely(self):
        src = (_WEBAPP / "kinship.html").read_text(encoding="utf-8")
        self.assertIn("renderKosekis", src)
        self.assertIn("取得済み戸籍（App33・", src)
        self.assertIn("data.kosekis", src)
        for banned in ("innerHTML", "outerHTML", "insertAdjacentHTML",
                       "document.write"):
            self.assertNotIn(banned, src)


if __name__ == "__main__":
    unittest.main()
