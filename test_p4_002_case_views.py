"""P4-002: hub/webapp_case_views（案件一覧＋詳細・read-only proxy）のテスト。

固定する仕様（DRAFT_P4 §2＋裁定/司令塔既定 2026-07-28）:
- 全 route が P4-001 の関所（_gate）必須・公開例外なし（機械検査）
- read-only: kintone 書込み API の呼出しゼロ（AST 機械検査）
- 一覧=App21 検索（status 絞込は schema 実選択肢の閉集合・更新順・
  ページング上限50/既定20・不正は固定 400 非反射）
- 詳細=App21 単票＋App30 案件絞込＋App28 件数のみ（本文非取得・fields=$id 限定・
  line_user_id は grammar 検証済みのみ query へ埋める）
- catch-all（webapp_auth）より先の結線（authed でページが 200 になることで pin）
"""

import ast
import os
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
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "d",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import hub.kintone as hub_kintone  # noqa: E402
import hub.webapp_case_views as cv  # noqa: E402
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
_LUID = "U" + "a" * 32

_ROUTES = ("/app/api/cases", "/app/api/cases/1", "/app/cases", "/app/case")


def _auth_headers():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


class TestAuthBoundary(unittest.TestCase):
    def test_unauthenticated_all_rejected(self):
        with patch.dict(os.environ, _ENV):
            for path in _ROUTES:
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    self.assertEqual(r.status_code, 303)
                    self.assertEqual(r.headers["location"], "/app/login")

    def test_all_routes_gated_no_public_exception(self):
        routes = [r for r in cv.router.routes if hasattr(r, "endpoint")]
        self.assertGreaterEqual(len(routes), 4)
        for route in routes:
            for method in route.methods:
                with self.subTest(path=route.path, method=method):
                    self.assertNotIn((route.path, method), PUBLIC_ROUTES)
                    self.assertTrue(
                        getattr(route.endpoint, "__webapp_gate__", False),
                        f"{method} {route.path} に認証関所（_gate）がない")


class TestReadOnlyMachineCheck(unittest.TestCase):
    """read-only の AST 機械検査（fix1 M01 で迂回経路を強化）。

    検査範囲: (i) kintone 系 module/属性の alias binding 追跡（`x = kintone`／
    `f = kintone.create_record`／import alias） (ii) getattr 等の動的属性アクセス
    の全面違反化 (iii) importlib/__import__/eval/exec の違反化 (iv) httpx 等の
    直接 HTTP client import と POST/PUT/DELETE/PATCH 系呼出しの違反化。

    静的解析の限界（明記）: 文字列組立てから到達する完全動的経路
    （例: globals() 辞書経由・C 拡張内部の呼出し）は本検査では検出できない。
    その残余は sink AST policy・関所テスト・レビューで重畳的に防御する
    （lineq G0 の動的 import 検査で確立した型の再利用）。
    """

    def setUp(self):
        self.src = Path(cv.__file__).read_text(encoding="utf-8")
        self.tree = ast.parse(self.src)

    def test_only_read_apis_of_kintone_used(self):
        allowed = {"KintoneApp", "search_records", "get_record"}
        used = {n.attr for n in ast.walk(self.tree)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name) and n.value.id == "kintone"}
        self.assertTrue(used)
        self.assertLessEqual(used, allowed, f"書込み系 API の使用: {used - allowed}")
        for banned in ("create_record", "update_record", "delete_record",
                       "upload_file", "_write"):
            self.assertNotIn(banned, self.src)

    def test_no_alias_binding_of_kintone(self):
        # fix1 M01-(i): module/属性の別名 binding を全面違反化
        # （検査が追跡できない参照名を作らせない）
        violations = []
        for node in ast.walk(self.tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                mod = getattr(node, "module", "") or ""
                for a in node.names:
                    if ("kintone" in (a.name or "") or "kintone" in mod) \
                            and a.asname is not None:
                        violations.append(f"import alias: {a.asname}")
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
                value = node.value
                if isinstance(value, ast.Name) and value.id == "kintone":
                    violations.append("module alias 代入")
                if isinstance(value, ast.Attribute) \
                        and isinstance(value.value, ast.Name) \
                        and value.value.id == "kintone":
                    violations.append(f"属性 alias 代入: {value.attr}")
        self.assertEqual(violations, [])

    def test_no_dynamic_access_or_import(self):
        # fix1 M01-(ii)(iii): getattr/setattr/__import__/importlib/eval/exec
        banned_calls = {"getattr", "setattr", "__import__", "eval", "exec",
                        "import_module"}
        found = []
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Call):
                f = node.func
                name = f.id if isinstance(f, ast.Name) else (
                    f.attr if isinstance(f, ast.Attribute) else "")
                if name in banned_calls:
                    found.append(name)
        self.assertEqual(found, [])
        self.assertNotIn("importlib", self.src)

    def test_no_direct_http_client_or_write_verbs(self):
        # fix1 M01-(iv): 直接 HTTP client の import・書込み動詞呼出しの違反化
        imported = set()
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(imported & {"httpx", "requests", "urllib", "aiohttp",
                                     "http"}, imported)
        write_verbs = {"post", "put", "delete", "patch", "request"}
        found = [n.func.attr for n in ast.walk(self.tree)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and n.func.attr in write_verbs]
        self.assertEqual(found, [])


class TestCasesApi(unittest.TestCase):
    def _get(self, url, mock_search):
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", mock_search):
            return _client.get(url, headers=_auth_headers(),
                               follow_redirects=False)

    def test_default_query_and_passthrough(self):
        mock = AsyncMock(return_value=[_rec(**{"$id": "1", "顧客名": "山田太郎",
                                               "status": "受任"})])
        r = self._get("/app/api/cases", mock)
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["records"][0]["顧客名"]["value"], "山田太郎")
        self.assertEqual(body["limit"], 20)
        app, query = mock.call_args.args[:2]
        self.assertIs(app, cv.APP_CASES)
        self.assertEqual(query, "order by 更新日時 desc limit 20 offset 0")

    def test_status_filter_closed_set(self):
        mock = AsyncMock(return_value=[])
        r = self._get("/app/api/cases?status=受任&limit=50&offset=20", mock)
        self.assertEqual(r.status_code, 200)
        query = mock.call_args.args[1]
        self.assertEqual(query,
                         'status in ("受任") order by 更新日時 desc '
                         "limit 50 offset 20")
        self.assertEqual(r.json()["status_options"], list(cv.STATUS_OPTIONS))

    def test_invalid_inputs_fixed_400_no_reflection_no_call(self):
        cases = ("status=怪しい値", "limit=51", "limit=0", "limit=abc",
                 "offset=-1", "offset=x")
        for qs in cases:
            with self.subTest(qs=qs):
                mock = AsyncMock(return_value=[])
                r = self._get(f"/app/api/cases?{qs}", mock)
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.content, b"")     # 固定・非反射
                mock.assert_not_called()             # kintone 未到達


class TestCaseDetailApi(unittest.TestCase):
    def _detail(self, record_id, case_rec, search_results):
        get_mock = AsyncMock(return_value=case_rec)
        search_mock = AsyncMock(side_effect=search_results)
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record", get_mock), \
             patch.object(hub_kintone, "search_records", search_mock):
            r = _client.get(f"/app/api/cases/{record_id}",
                            headers=_auth_headers(), follow_redirects=False)
        return r, get_mock, search_mock

    def test_detail_with_chat_count_only(self):
        case = _rec(**{"顧客名": "山田太郎", "LINEユーザーID": _LUID})
        shipping = [_rec(**{"$id": "9", "件名": "封筒"})]
        chats = [_rec(**{"$id": str(i)}) for i in range(3)]
        r, get_mock, search_mock = self._detail("12", case, [shipping, chats])
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["case"]["顧客名"]["value"], "山田太郎")
        self.assertEqual(body["shipping"][0]["件名"]["value"], "封筒")
        self.assertEqual(body["chat_count"], 3)
        self.assertFalse(body["chat_count_capped"])
        ship_call, chat_call = search_mock.call_args_list
        self.assertIn('案件レコードID = "12"', ship_call.args[1])
        self.assertIn(f'line_user_id = "{_LUID}"', chat_call.args[1])
        self.assertEqual(chat_call.kwargs.get("fields"), ["$id"])  # 本文非取得

    def test_invalid_line_user_id_skips_chat_query(self):
        bad = 'x" or role = "user'
        case = _rec(**{"顧客名": "山田太郎", "LINEユーザーID": bad})
        r, _, search_mock = self._detail("12", case, [[]])
        self.assertEqual(r.status_code, 200)
        self.assertIsNone(r.json()["chat_count"])
        self.assertEqual(search_mock.call_count, 1)      # App30 のみ・注入経路なし
        self.assertNotIn(bad, search_mock.call_args.args[1])

    def test_bad_record_id_fixed_404_no_kintone_call(self):
        for bad in ("abc", "1e3", "12345678901", "1;drop"):
            with self.subTest(rid=bad):
                r, get_mock, search_mock = self._detail(bad, {}, [])
                self.assertEqual(r.status_code, 404)
                self.assertEqual(r.content, b"")
                get_mock.assert_not_called()
                search_mock.assert_not_called()


class TestBoundaryPins(unittest.TestCase):
    """fix1 L01: 境界・fields 集合の完全一致 pin。"""

    def test_chat_count_exactly_at_cap_sets_capped(self):
        case = _rec(**{"顧客名": "山田太郎", "LINEユーザーID": _LUID})
        chats = [_rec(**{"$id": str(i)}) for i in range(500)]     # ちょうど 500
        get_mock = AsyncMock(return_value=case)
        search_mock = AsyncMock(side_effect=[[], chats])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record", get_mock), \
             patch.object(hub_kintone, "search_records", search_mock):
            r = _client.get("/app/api/cases/12", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.json()["chat_count"], 500)
        self.assertTrue(r.json()["chat_count_capped"])

    def test_fields_sets_pinned_exactly(self):
        # App21 一覧 / App30 絞込の fields 集合の完全一致（黙った拡張の防波堤）
        self.assertEqual(cv._LIST_FIELDS,
                         ["$id", "status", "顧客名", "問い合わせ業者名", "更新日時"])
        self.assertEqual(cv._SHIPPING_FIELDS,
                         ["$id", "件名", "チャネル", "方向", "発送ステータス",
                          "発送日時", "追跡番号", "送達結果", "更新日時"])
        mock = AsyncMock(return_value=[])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", mock):
            _client.get("/app/api/cases", headers=_auth_headers(),
                        follow_redirects=False)
        self.assertEqual(mock.call_args.kwargs.get("fields"), cv._LIST_FIELDS)
        get_mock = AsyncMock(return_value=_rec(**{"顧客名": "x"}))
        ship_mock = AsyncMock(side_effect=[[]])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record", get_mock), \
             patch.object(hub_kintone, "search_records", ship_mock):
            _client.get("/app/api/cases/12", headers=_auth_headers(),
                        follow_redirects=False)
        self.assertEqual(ship_mock.call_args.kwargs.get("fields"),
                         cv._SHIPPING_FIELDS)


class TestWebappDomSafety(unittest.TestCase):
    """fix1 H01: HTML 文字列補間 API の全画面不在（将来画面の防波堤）。"""

    _FORBIDDEN = ("innerHTML", "insertAdjacentHTML", "document.write")

    def test_no_html_string_apis_in_any_webapp_page(self):
        pages = sorted((Path(cv.__file__).resolve().parent.parent / "webapp")
                       .glob("*.html"))
        self.assertGreaterEqual(len(pages), 4)           # 走査対象が空でないこと
        for page in pages:
            src = page.read_text(encoding="utf-8")
            for banned in self._FORBIDDEN:
                with self.subTest(page=page.name, banned=banned):
                    self.assertNotIn(banned, src)

    def test_cases_status_options_built_as_dom_strings(self):
        # H01 pin: option は createElement+textContent/value 代入で構築され、
        # HTML 特殊文字を含む選択肢値も DOM 文字列として扱われる（補間文字列なし）
        src = (Path(cv.__file__).resolve().parent.parent
               / "webapp" / "cases.html").read_text(encoding="utf-8")
        self.assertIn('document.createElement("option")', src)
        self.assertIn("opt.textContent = o", src)
        self.assertIn("opt.value = o", src)
        self.assertNotIn("<option", src.split("<script>")[1])   # JS 内に option HTML なし


class TestPages(unittest.TestCase):
    def test_pages_served_when_authed(self):
        # catch-all（webapp_auth）より先の結線 pin: catch-all に食われると 404 になる
        with patch.dict(os.environ, _ENV):
            for path, needle in (("/app/cases", "案件一覧"),
                                 ("/app/case", "案件詳細")):
                with self.subTest(path=path):
                    r = _client.get(path, headers=_auth_headers(),
                                    follow_redirects=False)
                    self.assertEqual(r.status_code, 200)
                    self.assertIn(needle, r.text)

    def test_dashboard_links_to_cases(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app", headers=_auth_headers(),
                            follow_redirects=False)
            self.assertIn('href="/app/cases"', r.text)


if __name__ == "__main__":
    unittest.main()
