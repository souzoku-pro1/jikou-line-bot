"""SOUZOKU-HOUKI-H1-fix1 [01]: houki_bot の deny-all を AST checker で構造固定。

R-SOUZOKU-HOUKI-H1 [01]（HIGH）: 文字列包含の source pin を廃止し、P4 系
read-only checker と同様の「許可文脈の閉集合」方式へ格上げする。

方式（houki_bot/*.py 全ファイルに適用・AST 走査）:
1. import 閉集合（外部作用面の遮断・alias/相対 import 禁止）:
   - plain import: json / logging のみ
   - from-import: fastapi{APIRouter, BackgroundTasks, HTTPException, Request}
     / hub{notify} / hub.line_channel{HOUKI_CHANNEL, verify_line_signature,
     houki_channel_disabled_reason} / hub.redact{emit} のみ
   → httpx 等 HTTP クライアント・hub.kintone/DB・chat_responder・
     line_channel の送信関数（push_text / reply_with_push_fallback）は
     module ごと import 不能（許可文脈ゼロ）
2. notify の許可属性閉集合: notify.<attr> は notify_admin_line のみ
   （push_line_message / notify_business / notify_attorney_approval は禁止）
3. 送信・書込系識別子の全面禁止（Name / 属性名の両文脈・alias 迂回の残余遮断）
4. 動的アクセスの入口遮断: getattr/eval/exec/__import__/importlib/globals/
   locals/vars 等の識別子は許可文脈ゼロ（入口遮断優先の確立パターン）

checker 自体の negative（Codex 指定: notify.push_line_message(...) 追加形が
red になることを直接固定）を fixture で併置する。
"""

import ast
import unittest
from pathlib import Path

REPO = Path(__file__).parent
HOUKI_PKG = REPO / "houki_bot"

# ── 1. import 閉集合 ─────────────────────────────────────────────────────────
ALLOWED_PLAIN_IMPORTS = frozenset({"json", "logging"})
ALLOWED_FROM_IMPORTS = {
    "fastapi": frozenset({"APIRouter", "BackgroundTasks", "HTTPException",
                          "Request"}),
    "hub": frozenset({"notify"}),
    "hub.line_channel": frozenset({"HOUKI_CHANNEL", "verify_line_signature",
                                   "houki_channel_disabled_reason"}),
    "hub.redact": frozenset({"emit"}),
}

# ── 2. notify の許可属性（閉集合・これ以外の属性アクセスは違反） ─────────────────
NOTIFY_ALLOWED_ATTRS = frozenset({"notify_admin_line"})

# ── 3. 送信・書込系の禁止識別子（Name / Attribute.attr の両文脈で許可文脈ゼロ） ──
BANNED_EFFECT_NAMES = frozenset({
    # LINE 送信（hub/line_channel・hub/notify・chat_responder・raw HTTP）
    "push_text", "reply_with_push_fallback", "push_line_message",
    "notify_business", "notify_attorney_approval", "send_line_push",
    "httpx", "requests", "urllib", "aiohttp", "socket",
    # kintone / DB 書込
    "KintoneApp", "create_record", "update_record", "update_record_cas",
    "upload_file", "save_to_chatlog", "save_to_approval_queue",
    "get_engine", "get_session", "execute", "commit",
})

# ── 4. 動的アクセスの入口遮断 ───────────────────────────────────────────────
BANNED_DYNAMIC_NAMES = frozenset({
    "getattr", "setattr", "delattr", "eval", "exec", "compile",
    "__import__", "importlib", "globals", "locals", "vars", "open",
})


def policy_violations(src: str, filename: str = "<src>") -> list[str]:
    """houki_bot 外部作用ポリシーの違反一覧（空 = 適合）。"""
    tree = ast.parse(src, filename=filename)
    violations: list[str] = []

    def viol(node: ast.AST, msg: str) -> None:
        violations.append(f"{filename}:{getattr(node, 'lineno', 0)}: {msg}")

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname is not None:
                    viol(node, f"import alias 禁止: {alias.name} as {alias.asname}")
                if alias.name not in ALLOWED_PLAIN_IMPORTS:
                    viol(node, f"許可外 import: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            if node.level != 0:
                viol(node, "相対 import 禁止")
                continue
            allowed = ALLOWED_FROM_IMPORTS.get(node.module or "")
            if allowed is None:
                viol(node, f"許可外 from-import module: {node.module}")
                continue
            for alias in node.names:
                if alias.asname is not None:
                    viol(node, f"import alias 禁止: {alias.name} as {alias.asname}")
                if alias.name not in allowed:
                    viol(node, f"許可外 from-import 名: "
                               f"{node.module}.{alias.name}")
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id == "notify" \
                    and node.attr not in NOTIFY_ALLOWED_ATTRS:
                viol(node, f"notify の許可外属性: notify.{node.attr}")
            if node.attr in BANNED_EFFECT_NAMES:
                viol(node, f"禁止属性名: .{node.attr}")
            if node.attr in BANNED_DYNAMIC_NAMES:
                viol(node, f"禁止動的属性名: .{node.attr}")
        elif isinstance(node, ast.Name):
            if node.id in BANNED_EFFECT_NAMES:
                viol(node, f"禁止識別子: {node.id}")
            if node.id in BANNED_DYNAMIC_NAMES:
                viol(node, f"禁止動的識別子: {node.id}")

    return violations


def _pkg_sources() -> dict[str, str]:
    files = sorted(HOUKI_PKG.glob("*.py"))
    assert files, "houki_bot/*.py が見つからない"
    return {p.name: p.read_text(encoding="utf-8") for p in files}


class TestHoukiBotPolicyPasses(unittest.TestCase):
    """現物の houki_bot/*.py はポリシー適合（違反 0）。"""

    def test_all_package_files_clean(self):
        for name, src in _pkg_sources().items():
            with self.subTest(file=name):
                self.assertEqual(policy_violations(src, name), [])

    def test_router_is_scanned(self):
        # 走査対象に router.py が実在する（空 glob での空振り防止）
        self.assertIn("router.py", _pkg_sources())


class TestCheckerNegatives(unittest.TestCase):
    """checker 自体の negative: 迂回形が red になることを fixture で直接固定。"""

    def _router_plus(self, appended: str) -> list[str]:
        src = (HOUKI_PKG / "router.py").read_text(encoding="utf-8")
        return policy_violations(src + "\n" + appended, "router.py+fixture")

    def test_notify_push_line_message_is_red(self):
        # Codex 指定: notify.push_line_message(...) を追加した形が red
        v = self._router_plus(
            "async def _bad(uid):\n"
            '    await notify.push_line_message(uid, "x")\n')
        self.assertTrue(any("notify の許可外属性: notify.push_line_message"
                            in x for x in v), v)

    def test_notify_business_attr_is_red(self):
        v = self._router_plus(
            "async def _bad(uid):\n"
            '    await notify.notify_business(uid, "x")\n')
        self.assertTrue(any("notify.notify_business" in x for x in v), v)

    def test_httpx_direct_import_is_red(self):
        for stmt in ("import httpx",
                     "import httpx as hx",
                     "from httpx import AsyncClient"):
            with self.subTest(stmt=stmt):
                v = self._router_plus(stmt + "\n")
                self.assertTrue(v, stmt)

    def test_line_channel_sender_import_is_red(self):
        for stmt in ("from hub.line_channel import push_text",
                     "from hub.line_channel import push_text as pt",
                     "from hub.line_channel import reply_with_push_fallback",
                     "from hub import line_channel",
                     "import hub.line_channel",
                     "import hub.line_channel as lc"):
            with self.subTest(stmt=stmt):
                v = self._router_plus(stmt + "\n")
                self.assertTrue(v, stmt)

    def test_chat_responder_and_kintone_are_red(self):
        for stmt in ("from chat_responder import send_line_push",
                     "from hub import kintone",
                     "from hub.kintone import create_record",
                     "import chat_responder"):
            with self.subTest(stmt=stmt):
                v = self._router_plus(stmt + "\n")
                self.assertTrue(v, stmt)

    def test_dynamic_access_is_red(self):
        for snippet in ('_f = getattr(notify, "push_line" + "_message")\n',
                        '__import__("httpx")\n',
                        'eval("notify.push_line_message")\n',
                        'globals()["notify"]\n'):
            with self.subTest(snippet=snippet.strip()):
                v = self._router_plus(snippet)
                self.assertTrue(v, snippet)

    def test_alias_and_relative_import_are_red(self):
        for stmt in ("from hub import notify as n",
                     "import json as j",
                     "from . import router"):
            with self.subTest(stmt=stmt):
                v = self._router_plus(stmt + "\n")
                self.assertTrue(v, stmt)

    def test_banned_name_in_call_position_is_red(self):
        v = self._router_plus(
            "async def _bad(uid, text):\n"
            "    await send_line_push(uid, text)\n")
        self.assertTrue(any("send_line_push" in x for x in v), v)


class TestClosedSetsPinned(unittest.TestCase):
    """閉集合そのものを pin（拡張は票由来で行う・無断の緩和を検知）。"""

    def test_allowlists_pinned(self):
        self.assertEqual(ALLOWED_PLAIN_IMPORTS, frozenset({"json", "logging"}))
        self.assertEqual(set(ALLOWED_FROM_IMPORTS),
                         {"fastapi", "hub", "hub.line_channel", "hub.redact"})
        self.assertEqual(ALLOWED_FROM_IMPORTS["hub"], frozenset({"notify"}))
        self.assertEqual(ALLOWED_FROM_IMPORTS["hub.line_channel"],
                         frozenset({"HOUKI_CHANNEL", "verify_line_signature",
                                    "houki_channel_disabled_reason"}))
        self.assertEqual(NOTIFY_ALLOWED_ATTRS, frozenset({"notify_admin_line"}))

    def test_banned_sets_contain_review_targets(self):
        # レビュー指摘の対象（送信・HTTP・kintone/DB・動的アクセス）が
        # 禁止集合に実在することを個別に固定
        for name in ("push_text", "reply_with_push_fallback",
                     "push_line_message", "send_line_push", "httpx",
                     "KintoneApp", "create_record", "update_record",
                     "save_to_chatlog", "save_to_approval_queue"):
            self.assertIn(name, BANNED_EFFECT_NAMES)
        for name in ("getattr", "eval", "exec", "__import__", "importlib"):
            self.assertIn(name, BANNED_DYNAMIC_NAMES)


if __name__ == "__main__":
    unittest.main()
