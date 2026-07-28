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
    def test_only_read_apis_of_kintone_used(self):
        # read-only の機械検査: kintone module への属性参照は読取系のみ
        src = Path(cv.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        allowed = {"KintoneApp", "search_records", "get_record"}
        used = {n.attr for n in ast.walk(tree)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name) and n.value.id == "kintone"}
        self.assertTrue(used)
        self.assertLessEqual(used, allowed, f"書込み系 API の使用: {used - allowed}")
        for banned in ("create_record", "update_record", "delete_record",
                       "upload_file", "_write"):
            self.assertNotIn(banned, src)


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
