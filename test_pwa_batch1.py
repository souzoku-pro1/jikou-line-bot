"""PWA-BATCH-1: PWA 骨格＋相続案件ダッシュボード（read-only）のテスト。

固定する仕様（docs/plan/2026-08_pwa-product-design_v2.4.md 該当章＋本票）:
- 全新規 route が P4-001 の関所（_gate）必須・公開例外なし（機械検査）。
  未認証は既存関所の確立挙動どおり一律 303→/app/login（内容非提供・
  API も JSON を返さない）。
- read-only: kintone 書込み API 呼出しゼロ（AST 機械検査・P4-002 最終形
  checker 共用）。ダッシュボード router に GET 以外の route が存在しない
  （「機械は確定しない」——確定・承認・編集の経路が構造的に無い）。
- App34 読取は filter_active_persons・App36 読取は filter_active_heir_rows
  経由（除外件数のみ注記用に返す）。manifest 閉包検査への登録は
  test_rv08_soft_merge / test_p3_003c_cancel 側。
- PII 非出力: module は logging を import しない（構造）＋sentinel 実測
  （業務データがログへ流れない・応答へは流れる）。
- 業務データ応答は Cache-Control: no-store, private（P4-004 共通契約の適用実測）。
- Service Worker: 業務データ非キャッシュの実測（fetch handler・Cache Storage の
  不在＝キャッシュ経路が構造的に存在しない。P4-001 fix1 H01 裁定の維持）。
- manifest/アイコン/shell/logout の配信と認証境界。
"""

import ast
import logging
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
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
    "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "d",
    "APP_ZAISAN": "35", "TOKEN_ZAISAN": "d",
    "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "d",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import hub.kintone as hub_kintone  # noqa: E402
import hub.derivation_models as derivation_models  # noqa: E402
import hub.webapp_souzoku_dashboard as sd  # noqa: E402
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
    "KINTONE_SUBDOMAIN": "testsub",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "APP_KOSEKI_PERSON": "34", "APP_ZAISAN": "35",
    "APP_SOUZOKUNIN": "36", "APP_SHIPPING": "30",
}

_ROUTES = ("/app/api/souzoku/cases", "/app/api/souzoku/cases/1",
           "/app/souzoku", "/app/souzoku/case")


def _auth_headers():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


def _head(run_id=7, status="confirmed", provisional=False):
    return SimpleNamespace(id=run_id, status=status, provisional=provisional)


# ── 認証境界（未認証 negative 全 route・関所の機械検査） ─────────────────────
class TestAuthBoundary(unittest.TestCase):
    def test_unauthenticated_all_rejected_no_content(self):
        with patch.dict(os.environ, _ENV):
            for path in _ROUTES:
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    # 既存関所（P4-001 _gate）の確立挙動: 一律 303→login。
                    # session なし API 呼び出しが JSON/業務データを返さないこと
                    self.assertEqual(r.status_code, 303)
                    self.assertEqual(r.headers["location"], "/app/login")
                    self.assertNotIn("json",
                                     r.headers.get("content-type", ""))
            r = _client.post("/app/logout", follow_redirects=False)
            self.assertEqual(r.status_code, 303)
            self.assertEqual(r.headers["location"], "/app/login")
            for asset in ("/app/shell.js", "/app/icons/icon-192.png",
                          "/app/icons/icon-512.png"):
                with self.subTest(path=asset):
                    r = _client.get(asset, follow_redirects=False)
                    self.assertEqual(r.status_code, 303)

    def test_all_routes_gated_no_public_exception(self):
        routes = [r for r in sd.router.routes if hasattr(r, "endpoint")]
        self.assertGreaterEqual(len(routes), 4)
        for route in routes:
            for method in route.methods:
                with self.subTest(path=route.path, method=method):
                    self.assertNotIn((route.path, method), PUBLIC_ROUTES)
                    self.assertTrue(
                        getattr(route.endpoint, "__webapp_gate__", False),
                        f"{method} {route.path} に認証関所（_gate）がない")

    def test_dashboard_router_is_get_only(self):
        # 「機械は確定しない」: 確定・承認・編集へ通じる POST/PUT 等の route が
        # 構造的に存在しない（read-only の router 不変量）
        for route in sd.router.routes:
            if hasattr(route, "methods"):
                self.assertEqual(route.methods, {"GET"}, route.path)


# ── read-only AST 機械検査（P4-002 最終形 checker 共用）＋PII 構造 pin ────────
from ast_policy_helpers import (_ALLOWED_KINTONE_ATTRS,  # noqa: E402
                                _FORBIDDEN_IMPORTS, _binding_violations,
                                _readonly_violations)


class TestReadOnlyMachineCheck(unittest.TestCase):
    def setUp(self):
        self.src = Path(sd.__file__).read_text(encoding="utf-8")
        self.tree = ast.parse(self.src)

    def test_only_read_apis_of_kintone_used(self):
        used = {n.attr for n in ast.walk(self.tree)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name) and n.value.id == "kintone"}
        self.assertTrue(used)
        self.assertLessEqual(used, _ALLOWED_KINTONE_ATTRS,
                             f"書込み系 API の使用: {used - _ALLOWED_KINTONE_ATTRS}")
        for banned in ("create_record", "update_record", "delete_record",
                       "upload_file", "_write"):
            self.assertNotIn(banned, self.src)

    def test_module_passes_strengthened_checker(self):
        self.assertEqual(_readonly_violations(self.tree), [])
        self.assertEqual(_binding_violations(self.tree), [])

    def test_no_forbidden_imports_and_no_logging(self):
        imported = set()
        for node in ast.walk(self.tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(imported & _FORBIDDEN_IMPORTS, imported)
        # PII 非出力の構造 pin: logging を一切 import しない（webapp_auth と同流儀）
        self.assertNotIn("logging", imported)


# ── 案件一覧 API ─────────────────────────────────────────────────────────────
class TestSouzokuCasesApi(unittest.TestCase):
    def _get(self, url, mock_search):
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", mock_search):
            return _client.get(url, headers=_auth_headers(),
                               follow_redirects=False)

    def test_default_query_fields_and_passthrough(self):
        mock = AsyncMock(return_value=[_rec(**{"$id": "1", "氏名": "山田太郎"})])
        r = self._get("/app/api/souzoku/cases", mock)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["records"][0]["氏名"]["value"], "山田太郎")
        app, query = mock.call_args.args[:2]
        self.assertIs(app, sd.APP_SOUZOKU_CASES)
        self.assertEqual(query, "order by 更新日時 desc limit 20 offset 0")
        self.assertEqual(mock.call_args.kwargs.get("fields"),
                         sd._CASE_LIST_FIELDS)
        self.assertEqual(sd._CASE_LIST_FIELDS,
                         ["$id", "氏名", "被相続人名", "書類ステータス",
                          "登録日時", "更新日時"])

    def test_invalid_paging_fixed_400_no_reflection_no_call(self):
        for qs in ("limit=51", "limit=0", "limit=abc", "offset=-1", "offset=x"):
            with self.subTest(qs=qs):
                mock = AsyncMock(return_value=[])
                r = self._get(f"/app/api/souzoku/cases?{qs}", mock)
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.content, b"")     # 固定・非反射
                mock.assert_not_called()             # kintone 未到達


# ── ダッシュボード API（filter 経由・状態注記・リンク・PARTIAL） ──────────────
class TestDashboardApi(unittest.TestCase):
    _CASE = {"$id": {"value": "12"},
             "氏名": {"value": "山田太郎"},
             "被相続人名": {"value": "山田花子"},
             "書類ステータス": {"value": "送付状作成済"}}

    def _persons_raw(self):
        return [_rec(**{"$id": "101", "氏名": "山田一郎", "名寄せ確定": "確定"}),
                _rec(**{"$id": "102", "氏名": "山田二郎",
                        "統合状態": "統合済み無効"})]

    def _heirs_raw(self):
        return [_rec(**{"$id": "201", "氏名": "山田一郎", "続柄": "子",
                        "戸籍確認済": "yes"}),
                _rec(**{"$id": "202", "氏名": "山田二郎", "続柄": "子",
                        "取消済み": "yes"})]

    def _assets(self):
        return [_rec(**{"$id": "301", "財産種別": "預貯金", "評価額": "1000000",
                        "評価確定": "no", "有効": "yes"})]

    def _docs(self):
        return [_rec(**{"$id": "401", "件名": "職務上請求書",
                        "発送ステータス": "下書き"})]

    def _call(self, record_id="12", search_side=None, head=None,
              get_mock=None):
        search_mock = AsyncMock(
            side_effect=search_side if search_side is not None else
            [self._persons_raw(), self._heirs_raw(), self._assets(),
             self._docs()])
        get_mock = get_mock or AsyncMock(return_value=self._CASE)
        head_mock = AsyncMock(return_value=head)
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "get_record", get_mock), \
             patch.object(hub_kintone, "search_records", search_mock), \
             patch.object(derivation_models, "get_current_head", head_mock):
            r = _client.get(f"/app/api/souzoku/cases/{record_id}",
                            headers=_auth_headers(), follow_redirects=False)
        return r, get_mock, search_mock

    def test_filters_and_exclusion_counts(self):
        r, _get, search = self._call(head=_head())
        self.assertEqual(r.status_code, 200)
        body = r.json()
        # App34: 統合済み無効は filter 済み・除外件数のみ注記用
        self.assertEqual(
            [p["$id"]["value"] for p in body["persons"]["records"]], ["101"])
        self.assertEqual(body["persons"]["excluded_merged_count"], 1)
        # App36: 取消済みは filter 済み・除外件数のみ注記用
        self.assertEqual(
            [h["$id"]["value"] for h in body["heirs"]["records"]], ["201"])
        self.assertEqual(body["heirs"]["excluded_cancelled_count"], 1)
        self.assertEqual(body["assets"]["records"][0]["$id"]["value"], "301")
        self.assertEqual(body["documents"]["records"][0]["$id"]["value"], "401")
        self.assertEqual(body["derivation"]["head"],
                         {"run_id": 7, "run_status": "confirmed",
                          "provisional": False})
        self.assertEqual(body["notice"], sd.NOTICE_READONLY)

    def test_queries_and_fields_pinned(self):
        _r, _get, search = self._call(head=None)
        calls = search.call_args_list
        apps = [c.args[0] for c in calls]
        self.assertEqual(apps, [sd.APP_KOSEKI_PERSON, sd.APP_SOUZOKUNIN,
                                sd.APP_ZAISAN, sd.APP_SHIPPING])
        self.assertEqual(calls[0].args[1],
                         '案件レコードID = "12" order by $id asc limit 200')
        self.assertEqual(calls[0].kwargs.get("fields"), sd._PERSON_FIELDS)
        self.assertEqual(calls[1].args[1],
                         '案件レコードID = "12" order by $id asc limit 200')
        self.assertEqual(calls[1].kwargs.get("fields"), sd._HEIR_FIELDS)
        self.assertEqual(calls[2].kwargs.get("fields"), sd._ASSET_FIELDS)
        # App30 は案件アプリID＋案件レコードID の両絞込（時効/相続の同居 app）
        self.assertEqual(calls[3].args[1],
                         '案件アプリID = "26" and 案件レコードID = "12" '
                         "order by 更新日時 desc limit 20")
        self.assertEqual(calls[3].kwargs.get("fields"), sd._DOC_FIELDS)
        # filter が状態 field を読めることの pin（黙った縮小の防波堤）
        self.assertIn("統合状態", sd._PERSON_FIELDS)
        self.assertIn("取消済み", sd._HEIR_FIELDS)

    def test_links_material_from_validated_env_only(self):
        r, *_ = self._call(head=None)
        links = r.json()["links"]
        self.assertEqual(links["base"], "https://testsub.cybozu.com/k")
        self.assertEqual(links["apps"],
                         {"case": "26", "person": "34", "heir": "36",
                          "asset": "35", "shipping": "30"})

    def test_bad_record_id_fixed_404_no_kintone_call(self):
        for bad in ("abc", "1e3", "12345678901", "1;drop"):
            with self.subTest(rid=bad):
                r, get_mock, search_mock = self._call(record_id=bad)
                self.assertEqual(r.status_code, 404)
                self.assertEqual(r.content, b"")
                get_mock.assert_not_called()
                search_mock.assert_not_called()

    def test_partial_degradation_section_flag_no_detail(self):
        # PARTIAL（①§6）: section 取得失敗は ok=false の固定 flag のみ・
        # 他 section は表示継続・例外詳細は応答へ非搭載
        r, *_ = self._call(search_side=[
            RuntimeError("boom-SENTINEL"), self._heirs_raw(),
            self._assets(), self._docs()], head=None)
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["persons"], {"ok": False})
        self.assertTrue(body["heirs"]["ok"])
        self.assertNotIn("boom-SENTINEL", r.text)

    def test_business_data_response_is_no_store(self):
        # 業務データ応答の保存禁止（P4-004 共通契約の適用実測・①§12.5）
        r, *_ = self._call(head=None)
        self.assertEqual(r.headers.get("cache-control"), "no-store, private")


# ── PII sentinel（業務データがログへ流れない実測） ────────────────────────────
class TestPiiSentinel(unittest.TestCase):
    def test_sentinel_pii_reaches_response_but_never_logs(self):
        sent_name = "SENTINEL-氏名-73AF"
        case = _rec(**{"$id": "12", "氏名": sent_name})
        persons = [_rec(**{"$id": "101", "氏名": sent_name})]
        records = []

        class _Cap(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        cap = _Cap()
        root = logging.getLogger()
        root.addHandler(cap)
        old_level = root.level
        root.setLevel(logging.DEBUG)
        try:
            with patch.dict(os.environ, _ENV), \
                 patch.object(hub_kintone, "get_record",
                              AsyncMock(return_value=case)), \
                 patch.object(hub_kintone, "search_records",
                              AsyncMock(side_effect=[persons, [], [], []])), \
                 patch.object(derivation_models, "get_current_head",
                              AsyncMock(return_value=None)):
                r = _client.get("/app/api/souzoku/cases/12",
                                headers=_auth_headers(),
                                follow_redirects=False)
        finally:
            root.setLevel(old_level)
            root.removeHandler(cap)
        self.assertEqual(r.status_code, 200)
        self.assertIn(sent_name, r.text)                 # 表示へは流れる
        self.assertNotIn(sent_name, "\n".join(records))  # ログへは流れない


# ── PWA 骨格（manifest・アイコン・shell・logout・SW） ─────────────────────────
class TestPwaShellAssets(unittest.TestCase):
    def test_manifest_standalone_with_icons(self):
        import json
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/manifest.json", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        m = json.loads(r.text)
        self.assertEqual(m["display"], "standalone")
        self.assertEqual(m["scope"], "/app")
        self.assertEqual(m["start_url"], "/app")
        srcs = [i["src"] for i in m["icons"]]
        self.assertEqual(srcs, ["/app/icons/icon-192.png",
                                "/app/icons/icon-512.png"])

    def test_icons_served_png_when_authed(self):
        with patch.dict(os.environ, _ENV):
            for path in ("/app/icons/icon-192.png", "/app/icons/icon-512.png"):
                with self.subTest(path=path):
                    r = _client.get(path, headers=_auth_headers(),
                                    follow_redirects=False)
                    self.assertEqual(r.status_code, 200)
                    self.assertEqual(r.headers["content-type"], "image/png")
                    self.assertEqual(r.content[:8],
                                     b"\x89PNG\r\n\x1a\n")   # PNG magic

    def test_shell_js_served_and_has_logout_form(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/shell.js", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn("/app/logout", r.text)
        self.assertNotIn("innerHTML", r.text)

    def test_logout_clears_cookie_and_redirects(self):
        with patch.dict(os.environ, _ENV):
            r = _client.post("/app/logout", headers=_auth_headers(),
                             follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/login")
        set_cookie = r.headers.get("set-cookie", "")
        self.assertIn("webapp_session=", set_cookie)
        self.assertIn('webapp_session=""', set_cookie)   # 値の破棄（削除）
        self.assertEqual(r.headers.get("cache-control"), "no-store, private")

    def test_sw_has_no_cache_and_no_fetch_handler(self):
        # C: Service Worker に業務データを載せない実測——キャッシュ経路
        # （Cache Storage・fetch handler）が構造的に存在しない（P4-001 fix1
        # H01 裁定の維持。全 request はブラウザ既定の network fetch）
        from hub.webapp_auth import WEBAPP_ROOT
        sw = (WEBAPP_ROOT / "sw.js").read_text(encoding="utf-8")
        for forbidden in ("caches.", "CacheStorage",
                          'addEventListener("fetch"', "respondWith",
                          "addAll(", "indexedDB", "localStorage"):
            self.assertNotIn(forbidden, sw, forbidden)


# ── 画面配信と結線 ───────────────────────────────────────────────────────────
class TestPages(unittest.TestCase):
    def test_pages_served_when_authed(self):
        with patch.dict(os.environ, _ENV):
            for path, needle in (("/app/souzoku", "相続案件"),
                                 ("/app/souzoku/case", "相続案件ダッシュボード")):
                with self.subTest(path=path):
                    r = _client.get(path, headers=_auth_headers(),
                                    follow_redirects=False)
                    self.assertEqual(r.status_code, 200)
                    self.assertIn(needle, r.text)
                    self.assertIn('src="/app/shell.js"', r.text)   # 共通画面枠

    def test_index_links_souzoku_and_shell(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertIn('href="/app/souzoku"', r.text)
        self.assertIn('src="/app/shell.js"', r.text)
        self.assertIn('rel="apple-touch-icon"', r.text)

    def test_no_write_ui_in_dashboard_pages(self):
        # 「機械は確定しない」: ダッシュボード画面に form/submit 経路がない
        # （唯一の form は共通画面枠の logout で shell.js 側・業務操作ではない）
        from hub.webapp_auth import WEBAPP_ROOT
        for name in ("souzoku.html", "souzoku_case.html"):
            src = (WEBAPP_ROOT / name).read_text(encoding="utf-8")
            self.assertNotIn("<form", src, name)
            self.assertNotIn('type="submit"', src, name)


if __name__ == "__main__":
    unittest.main()
