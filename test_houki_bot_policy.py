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

SOUZOKU-HOUKI-H3（票由来の閉集合更新）: ヒアリング実装で必要になった分だけ
許可を拡張する——
- from-import に hub{houki_case_store, reply_sanitizer}・
  hub.autoreply_stoplist{is_suppressed}・hub.houki_profile{名前閉集合 11}・
  hub.line_channel へ reply_with_push_fallback・houki_bot.hearing
  {handle_houki_hearing} を追加
- BANNED から reply_with_push_fallback / save_to_chatlog /
  save_to_approval_queue を外す（import 閉集合+新規則で統制へ移行）
- 新規則: reply_with_push_fallback の呼び出しは**第1引数が HOUKI_CHANNEL の
  Name である場合のみ許可**（時効チャネルでの送信は構造的に不可能のまま）
- push_text・push_line_message・kintone/DB 直接アクセス・chat_responder
  import・動的アクセスの禁止は不変
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
    # H3: houki_case_store（App 40 アクセス層・高位関数のみ）・reply_sanitizer
    # （第 2 世代ガード機構）を追加
    # H4（票由来）: houki_phone_triage（電話推奨度判定の高位入口
    # run_phone_triage / triage_pending のみ使用。kintone/LINE への作用は
    # hub 側 module 内に閉じる）を追加
    # IMAGE-INTAKE-1（票由来）: image_intake（画像受領の束ね返信。送信は
    # hub 側 module 内の push_text に閉じる——houki_bot 内には httpx/送信名を
    # 置かない）を追加
    "hub": frozenset({"notify", "houki_case_store", "reply_sanitizer",
                      "houki_phone_triage", "image_intake"}),
    # H3: 送信は reply_with_push_fallback のみ解禁（呼び出し規則で
    # HOUKI_CHANNEL 限定・push_text は禁止のまま）
    "hub.line_channel": frozenset({"HOUKI_CHANNEL", "verify_line_signature",
                                   "houki_channel_disabled_reason",
                                   "reply_with_push_fallback"}),
    "hub.redact": frozenset({"emit"}),
    # H3: 停止リスト共用（照会のみ）
    "hub.autoreply_stoplist": frozenset({"is_suppressed"}),
    # H3: 相続放棄プロファイル/ヒアリング定義+機構 re-export の名前閉集合
    "hub.houki_profile": frozenset({
        "HEARING_TEMPLATE_BLOCKS_HOUKI", "HOUKI_HEARING_CATEGORY",
        "HOUKI_HEARING_PROMPT", "HOUKI_PROFILE", "ClaudeUnavailableError",
        "autoreply_paused", "call_hearing_model", "get_recent_chat_history",
        "save_to_approval_queue", "save_to_chatlog",
        "style_guard_violations"}),
    # H3: パッケージ内（router → hearing）
    "houki_bot.hearing": frozenset({"handle_houki_hearing"}),
}

# ── 2. notify の許可属性（閉集合・これ以外の属性アクセスは違反） ─────────────────
NOTIFY_ALLOWED_ATTRS = frozenset({"notify_admin_line"})

# ── 3. 送信・書込系の禁止識別子（Name / Attribute.attr の両文脈で許可文脈ゼロ） ──
# H3: reply_with_push_fallback / save_to_chatlog / save_to_approval_queue は
# import 閉集合+呼び出し規則（HOUKI_CHANNEL 限定）での統制へ移行し本集合から
# 除外（票由来）。それ以外は不変
BANNED_EFFECT_NAMES = frozenset({
    # LINE 送信（hub/line_channel・hub/notify・chat_responder・raw HTTP）
    "push_text", "push_line_message",
    "notify_business", "notify_attorney_approval", "send_line_push",
    "httpx", "requests", "urllib", "aiohttp", "socket",
    # kintone / DB 書込
    "KintoneApp", "create_record", "update_record", "update_record_cas",
    "upload_file",
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
        elif isinstance(node, ast.Call):
            # H3 新規則: 送信は HOUKI チャネル限定（第1引数が Name
            # "HOUKI_CHANNEL" のときだけ許可。式・別名・時効チャネルは違反）
            func = node.func
            fname = func.id if isinstance(func, ast.Name) else (
                func.attr if isinstance(func, ast.Attribute) else "")
            if fname == "reply_with_push_fallback":
                first = node.args[0] if node.args else None
                if not (isinstance(first, ast.Name)
                        and first.id == "HOUKI_CHANNEL"):
                    viol(node, "reply_with_push_fallback の第1引数は "
                               "HOUKI_CHANNEL の Name に限る")

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
        # H3: reply_with_push_fallback の**名前 import は許可**へ移行（呼び出し
        # 規則で HOUKI_CHANNEL 限定）。push_text・module import・alias は禁止のまま
        for stmt in ("from hub.line_channel import push_text",
                     "from hub.line_channel import push_text as pt",
                     "from hub.line_channel import reply_with_push_fallback as r",
                     "from hub import line_channel",
                     "import hub.line_channel",
                     "import hub.line_channel as lc"):
            with self.subTest(stmt=stmt):
                v = self._router_plus(stmt + "\n")
                self.assertTrue(v, stmt)

    def test_jikou_channel_send_is_red(self):
        # H3 新規則の negative: 時効チャネルでの送信は構造的に不可能
        # （JIKOU_CHANNEL の import 自体が名前閉集合外・呼び出し第1引数も検査）
        v = self._router_plus("from hub.line_channel import JIKOU_CHANNEL\n")
        self.assertTrue(any("JIKOU_CHANNEL" in x for x in v), v)
        v = self._router_plus(
            "async def _bad(rt, uid, t):\n"
            "    await reply_with_push_fallback(JIKOU_CHANNEL, rt, uid, t)\n")
        self.assertTrue(any("第1引数は" in x for x in v), v)
        v = self._router_plus(
            "async def _bad(rt, uid, t):\n"
            "    ch = HOUKI_CHANNEL\n"
            "    await reply_with_push_fallback(ch, rt, uid, t)\n")
        self.assertTrue(any("第1引数は" in x for x in v), v)   # 別名経由も red
        # 正形（HOUKI_CHANNEL の Name 直渡し）は green
        v = self._router_plus(
            "async def _ok(rt, uid, t):\n"
            "    await reply_with_push_fallback(HOUKI_CHANNEL, rt, uid, t)\n")
        self.assertEqual([x for x in v if "第1引数" in x], [])

    def test_houki_phone_triage_bypass_forms_are_red(self):
        # H4 の閉集合更新の negative: 許可は `from hub import
        # houki_phone_triage`（module 属性経由）のみ。名前 import・alias は red
        for stmt in ("from hub.houki_phone_triage import run_phone_triage",
                     "from hub import houki_phone_triage as t",
                     "import hub.houki_phone_triage"):
            with self.subTest(stmt=stmt):
                v = self._router_plus(stmt + "\n")
                self.assertTrue(v, stmt)

    def test_image_intake_bypass_forms_are_red(self):
        # IMAGE-INTAKE-1 の閉集合更新の negative: module 属性経由のみ許可
        for stmt in ("from hub.image_intake import handle_houki_image",
                     "from hub import image_intake as im",
                     "import hub.image_intake"):
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
        # H3 で更新した閉集合の pin（拡張は票由来のみ）
        self.assertEqual(ALLOWED_PLAIN_IMPORTS, frozenset({"json", "logging"}))
        self.assertEqual(set(ALLOWED_FROM_IMPORTS),
                         {"fastapi", "hub", "hub.line_channel", "hub.redact",
                          "hub.autoreply_stoplist", "hub.houki_profile",
                          "houki_bot.hearing"})
        self.assertEqual(ALLOWED_FROM_IMPORTS["hub"],
                         frozenset({"notify", "houki_case_store",
                                    "reply_sanitizer", "houki_phone_triage",
                                    "image_intake"}))   # H4/IMAGE-INTAKE-1 票由来
        self.assertEqual(ALLOWED_FROM_IMPORTS["hub.line_channel"],
                         frozenset({"HOUKI_CHANNEL", "verify_line_signature",
                                    "houki_channel_disabled_reason",
                                    "reply_with_push_fallback"}))
        self.assertEqual(ALLOWED_FROM_IMPORTS["hub.autoreply_stoplist"],
                         frozenset({"is_suppressed"}))
        self.assertEqual(len(ALLOWED_FROM_IMPORTS["hub.houki_profile"]), 11)
        self.assertEqual(ALLOWED_FROM_IMPORTS["houki_bot.hearing"],
                         frozenset({"handle_houki_hearing"}))
        self.assertEqual(NOTIFY_ALLOWED_ATTRS, frozenset({"notify_admin_line"}))

    def test_banned_sets_contain_review_targets(self):
        # レビュー指摘の対象（送信・HTTP・kintone/DB・動的アクセス）が
        # 禁止集合に実在することを個別に固定。H3 で統制方式を移行した 3 名は
        # **意図的に除外**（import 閉集合+HOUKI_CHANNEL 限定規則で統制）
        for name in ("push_text",
                     "push_line_message", "send_line_push", "httpx",
                     "KintoneApp", "create_record", "update_record"):
            self.assertIn(name, BANNED_EFFECT_NAMES)
        for name in ("reply_with_push_fallback", "save_to_chatlog",
                     "save_to_approval_queue"):
            self.assertNotIn(name, BANNED_EFFECT_NAMES)   # H3 移行（票由来）
        for name in ("getattr", "eval", "exec", "__import__", "importlib"):
            self.assertIn(name, BANNED_DYNAMIC_NAMES)


if __name__ == "__main__":
    unittest.main()
