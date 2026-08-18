"""P4-004: hub/webapp_approval_view（承認キュー参照・read-only）のテスト。

固定する仕様（DRAFT_P4 §2/§5＋裁定 2026-07-28）:
- 全 route が P4-001 の関所（_gate）必須・公開例外なし（機械検査）
- 参照のみ: 本 module に POST/PUT/DELETE route ゼロ・kintone 書込み API ゼロ
  （AST 機械検査）——承認経路は既存 webhook が単一の正のまま
- 絞込既定=送信済み no のみ／all=1 で全件（閉集合・他値は固定 400）
- AI 下書き本文（顧客往復含む）の素通し表示可（裁定 pin）
- ページング上限50/既定20・不正は固定 400 非反射・kintone 未到達
"""

import ast
import os
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import hub.kintone as hub_kintone  # noqa: E402
import hub.webapp_approval_view as av  # noqa: E402
from hub.webapp_auth import (  # noqa: E402
    MIN_ITERATIONS,
    PUBLIC_ROUTES,
    hash_password,
    issue_session,
)

_client = TestClient(main.app)
_ENV = {
    "WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
    "WEBAPP_SESSION_SECRET": "s" * 32,
}


# ── read-only AST 機械検査（P4-002 で確立した最終形を同水準で移植・fix6。
# 出現許可文脈の閉集合＋束縛元検証〔7分類＋PEP695 type parameter＋star import
# 禁止〕＋歴代 alias/代入遮断の重畳。AST-CONSOL 票で ast_policy_helpers へ移設
# =挙動同一の移設のみ・byte 同一並置の解消）──
from ast_policy_helpers import (_ALLOWED_KINTONE_ATTRS, _FORBIDDEN_IMPORTS,
                                _binding_violations, _readonly_violations)

def _auth_headers():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


class TestAuthAndReadOnly(unittest.TestCase):
    def test_unauthenticated_rejected(self):
        with patch.dict(os.environ, _ENV):
            for path in ("/app/api/approvals", "/app/approvals"):
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    self.assertEqual(r.status_code, 303)
                    self.assertEqual(r.headers["location"], "/app/login")

    def test_all_routes_gated_get_only(self):
        routes = [r for r in av.router.routes if hasattr(r, "endpoint")]
        self.assertGreaterEqual(len(routes), 2)
        for route in routes:
            self.assertEqual(route.methods, {"GET"})     # 参照のみ=GET 以外なし
            for method in route.methods:
                self.assertNotIn((route.path, method), PUBLIC_ROUTES)
                self.assertTrue(
                    getattr(route.endpoint, "__webapp_gate__", False),
                    f"{method} {route.path} に認証関所（_gate）がない")

    def test_only_read_apis_of_kintone_used(self):
        src = Path(av.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        used = {n.attr for n in ast.walk(tree)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name) and n.value.id == "kintone"}
        self.assertTrue(used)
        self.assertLessEqual(used, _ALLOWED_KINTONE_ATTRS,
                             f"書込み系 API の使用: {used - _ALLOWED_KINTONE_ATTRS}")
        for banned in ("create_record", "update_record", "delete_record",
                       "upload_file", "_write", "router.post", "router.put",
                       "router.delete"):
            self.assertNotIn(banned, src)

    def test_readonly_final_form_checker_passes(self):
        # P4-002 で確立した最終形（出現閉集合＋束縛検証＋star/type param 禁止）を
        # 同水準で適用し、本 module が違反ゼロであることを機械検査
        tree = ast.parse(Path(av.__file__).read_text(encoding="utf-8"))
        self.assertEqual(_readonly_violations(tree), [])
        self.assertEqual(_binding_violations(tree), [])

    def test_no_forbidden_or_process_launch_imports(self):
        imported = set()
        tree = ast.parse(Path(av.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(imported & _FORBIDDEN_IMPORTS, imported)

    def test_final_form_detects_bypass_fixtures(self):
        # 移植した最終形が代表的迂回（star import・type parameter・別名・
        # 動的アクセス）を検出することの健全性確認（p4-002 と同水準）
        fixtures = (
            "from hub import kintone\nfrom evil import *\n",
            "from hub import kintone\ndef f[kintone](): pass\n",
            "from hub import kintone\nk = kintone\nk.update_record(1)\n",
            "from hub import kintone\nga = getattr\nga(kintone, 'x')\n",
        )
        for src in fixtures:
            with self.subTest(src=src[:40]):
                self.assertNotEqual(_readonly_violations(ast.parse(src)), [])


class TestApprovalsApi(unittest.TestCase):
    def _get(self, url, mock_search):
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", mock_search):
            return _client.get(url, headers=_auth_headers(),
                               follow_redirects=False)

    def test_default_filters_unsent_only(self):
        rec = _rec(**{"$id": "5", "顧客名": "山田太郎",
                      "顧客メッセージ": "時効について教えてください",
                      "AI下書き": "拝見しました。時効援用の要件は…",
                      "送信済み": "no"})
        mock = AsyncMock(return_value=[rec])
        r = self._get("/app/api/approvals", mock)
        self.assertEqual(r.status_code, 200)
        app, query = mock.call_args.args[:2]
        self.assertIs(app, av.APP_APPROVAL)
        self.assertEqual(query, '送信済み in ("no") order by 更新日時 desc '
                                "limit 20 offset 0")
        body = r.json()
        self.assertFalse(body["all"])
        # 裁定 pin: AI 下書き本文（顧客往復含む）を素通しで返してよい
        self.assertEqual(body["records"][0]["AI下書き"]["value"],
                         "拝見しました。時効援用の要件は…")
        self.assertEqual(body["records"][0]["顧客メッセージ"]["value"],
                         "時効について教えてください")

    def test_all_switch(self):
        mock = AsyncMock(return_value=[])
        r = self._get("/app/api/approvals?all=1&limit=50&offset=20", mock)
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["all"])
        self.assertEqual(mock.call_args.args[1],
                         "order by 更新日時 desc limit 50 offset 20")

    def test_fields_exclude_line_user_id(self):
        # fix1 M01: 生 external ID をブラウザへ送らない（fields から除外）
        self.assertNotIn("line_user_id", av._FIELDS)
        mock = AsyncMock(return_value=[])
        self._get("/app/api/approvals", mock)
        self.assertEqual(mock.call_args.kwargs.get("fields"), av._FIELDS)
        self.assertNotIn("line_user_id", mock.call_args.kwargs.get("fields"))

    def test_invalid_inputs_fixed_400_no_call(self):
        for qs in ("all=0", "all=yes", "limit=51", "limit=0", "limit=abc",
                   "offset=-1", "offset=x"):
            with self.subTest(qs=qs):
                mock = AsyncMock(return_value=[])
                r = self._get(f"/app/api/approvals?{qs}", mock)
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.content, b"")     # 固定・非反射
                mock.assert_not_called()


class TestPage(unittest.TestCase):
    def test_page_served_when_authed(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/approvals", headers=_auth_headers(),
                            follow_redirects=False)
            self.assertEqual(r.status_code, 200)
            # UI-POLISH-1 票由来: 画面名を内部用語「承認キュー参照」から
            # 「返信の承認」へ改称（確認専用の明示を追加 pin・操作不在 assert
            # は不変）
            self.assertIn("返信の承認", r.text)
            self.assertIn("確認専用", r.text)
            # 参照のみ: 操作系の UI 要素・送信 API 呼出しが存在しない
            self.assertNotIn("承認する", r.text)
            self.assertNotIn("method=\"post\"", r.text.lower())

    def test_dashboard_links_to_approvals(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app", headers=_auth_headers(),
                            follow_redirects=False)
            self.assertIn('href="/app/approvals"', r.text)


class TestCacheControlNoStore(unittest.TestCase):
    """fix1 H01: PWA 保護領域の共通契約 Cache-Control: no-store, private。
    _gate 経由の全応答（成功/400/redirect/画面）＋ merge 済み P4-002 API へ遡及。"""

    _EXPECTED = "no-store, private"

    def _hdr(self, r):
        return r.headers.get("cache-control", "")

    def test_p4004_api_success_400_and_redirect(self):
        # 成功
        mock = AsyncMock(return_value=[])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", mock):
            ok = _client.get("/app/api/approvals", headers=_auth_headers(),
                             follow_redirects=False)
        self.assertEqual(self._hdr(ok), self._EXPECTED)
        # 400（固定応答）
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", AsyncMock()):
            bad = _client.get("/app/api/approvals?all=yes",
                              headers=_auth_headers(), follow_redirects=False)
        self.assertEqual(bad.status_code, 400)
        self.assertEqual(self._hdr(bad), self._EXPECTED)
        # 未認証 303 redirect
        with patch.dict(os.environ, _ENV):
            red = _client.get("/app/api/approvals", follow_redirects=False)
        self.assertEqual(red.status_code, 303)
        self.assertEqual(self._hdr(red), self._EXPECTED)
        # 画面
        with patch.dict(os.environ, _ENV):
            page = _client.get("/app/approvals", headers=_auth_headers(),
                               follow_redirects=False)
        self.assertEqual(self._hdr(page), self._EXPECTED)

    def test_p4002_case_api_retroactively_covered(self):
        # merge 済み P4-002 案件 API（顧客 PII 含む）へ遡及適用される
        import hub.webapp_case_views as cv
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", AsyncMock(return_value=[])):
            r = _client.get("/app/api/cases", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self._hdr(r), self._EXPECTED)
        self.assertTrue(cv.router.routes)          # 参照の健全性
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record",
                          AsyncMock(return_value={"顧客名": {"value": "x"}})), \
             patch.object(hub_kintone, "search_records", AsyncMock(return_value=[])):
            d = _client.get("/app/api/cases/1", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(self._hdr(d), self._EXPECTED)

    def test_login_public_responses_no_store(self):
        with patch.dict(os.environ, _ENV):
            page = _client.get("/app/login", follow_redirects=False)
            self.assertEqual(self._hdr(page), self._EXPECTED)
            fail = _client.post("/app/login", data={"password": "x"},
                                follow_redirects=False)
            self.assertEqual(fail.status_code, 303)
            self.assertEqual(self._hdr(fail), self._EXPECTED)


class TestReferenceOnlyUiClosedSet(unittest.TestCase):
    """fix1 M02: 参照専用 UI の閉集合機械検査（approvals.html の HTML/JS 走査）。"""

    def setUp(self):
        import hub.webapp_approval_view as _av
        self.src = (Path(_av.__file__).resolve().parent.parent
                    / "webapp" / "approvals.html").read_text(encoding="utf-8")
        self.lower = self.src.lower()

    def test_buttons_are_prev_next_only(self):
        ids = set(re.findall(r'<button[^>]*\bid="([^"]+)"', self.src))
        self.assertEqual(ids, {"prev", "next"})
        # id なし button（操作ボタンの混入）も不在
        self.assertEqual(self.src.count("<button"), 2)

    def test_inputs_are_all_checkbox_only(self):
        inputs = re.findall(r"<input\b[^>]*>", self.src)
        self.assertEqual(len(inputs), 1)
        self.assertIn('id="all"', inputs[0])
        self.assertIn('type="checkbox"', inputs[0])

    def test_fetch_targets_are_read_api_get_only(self):
        # fetch は参照 API のみ・書込み動詞や form はゼロ
        fetches = re.findall(r"fetch\(([^)]*)\)", self.src)
        self.assertTrue(fetches)
        for f in fetches:
            self.assertIn("/app/api/approvals", f)
        # fetch options に method 指定がない＝既定 GET
        self.assertNotIn("method:", self.lower.replace(" ", ""))
        for verb in ('"post"', "'post'", '"put"', '"patch"', '"delete"',
                     "<form", "action="):
            self.assertNotIn(verb, self.lower)

    def test_no_line_user_id_in_ui(self):
        self.assertNotIn("line_user_id", self.src)


if __name__ == "__main__":
    unittest.main()
