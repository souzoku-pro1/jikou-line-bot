"""PWA-BATCH-1: PWA 骨格＋相続案件ダッシュボード（read-only）のテスト。

固定する仕様（docs/plan/2026-08_pwa-product-design_v2.4.md 該当章＋本票＋
R-PWA-1 fix1）:
- 全新規 route が P4-001 の関所（_gate）必須・公開例外なし（機械検査）。
  未認証は既存関所の確立挙動どおり一律 303→/app/login（内容非提供・
  API も JSON を返さない）。
- read-only: kintone 書込み API 呼出しゼロ（AST 機械検査・P4-002 最終形
  checker 共用）。ダッシュボード router に GET 以外の route が存在しない
  （「機械は確定しない」——確定・承認・編集の経路が構造的に無い）。
- PWA-01: field 閉集合（取得=UI 表示・API 使用と 1:1・完全一致 pin）＋応答投影
  （未許可 field は fixture に現れても応答へ出ない）＋全 field 列挙ループの
  不在（構造 pin）。
- PWA-02: App34/36/35 は $id 厳密単調増加カーソルで全件取得（複数 page・
  後続 page の無効行除外・後続 page の合計算入・途中失敗 PARTIAL・カーソル
  単調増加/重複欠落なし）。
- PWA-03: 評価額集計はサーバ側 Python int（任意精度）・厳密 grammar
  ^[0-9]+$ のみ加算・不正は「集計不能」（0 円へ落とさない）・空値と 0 円の
  区別・有効=no 非算入・2^53 超の桁落ちなし・JSON は文字列。
- App34 読取は filter_active_persons・App36 読取は filter_active_heir_rows
  経由（除外件数のみ注記用に返す）。manifest 閉包検査への登録は
  test_rv08_soft_merge / test_p3_003c_cancel 側。
- PII 非出力: module は logging を import しない（構造）＋sentinel 実測。
- 業務データ応答は Cache-Control: no-store, private（P4-004 共通契約）。
- Service Worker: 業務データ非キャッシュの実測（P4-001 fix1 H01 裁定の維持）。
"""

import ast
import asyncio
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


def _run(coro):
    return asyncio.run(coro)


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


# ── PWA-01: field 閉集合（完全一致 pin・応答投影・構造 pin） ──────────────────
class TestFieldClosedSets(unittest.TestCase):
    def test_fields_sets_pinned_exactly(self):
        self.assertEqual(sd._CASE_LIST_FIELDS,
                         ["$id", "氏名", "被相続人名", "書類ステータス",
                          "登録日時", "更新日時"])
        self.assertEqual(sd._CASE_FIELDS,
                         ["$id", "氏名", "被相続人名", "続柄", "書類ステータス",
                          "登録日時", "更新日時"])
        self.assertEqual(sd._PERSON_FETCH_FIELDS,
                         ["$id", "氏名", "続柄メモ", "生死区分", "被相続人フラグ",
                          "名寄せ確定", "相続人候補", "相続資格", "確認状態",
                          "統合状態"])
        self.assertEqual(sd._PERSON_VIEW_FIELDS,
                         ["$id", "氏名", "続柄メモ", "生死区分", "被相続人フラグ",
                          "名寄せ確定", "相続人候補", "相続資格", "確認状態"])
        self.assertEqual(sd._HEIR_FETCH_FIELDS,
                         ["$id", "氏名", "続柄", "法定相続分", "状態",
                          "戸籍確認済", "印鑑証明", "データ源", "取消済み"])
        self.assertEqual(sd._HEIR_VIEW_FIELDS,
                         ["$id", "氏名", "続柄", "法定相続分", "状態",
                          "戸籍確認済", "印鑑証明", "データ源"])
        self.assertEqual(sd._ASSET_FIELDS,
                         ["$id", "財産種別", "名義", "評価額", "評価方法",
                          "評価基準日", "評価確定", "データ源", "有効"])
        self.assertEqual(sd._DOC_FIELDS,
                         ["$id", "件名", "チャネル", "方向", "発送ステータス",
                          "発送日時", "成果物"])
        # 「取得するが表示しない」PII の不在 pin（PWA-01 指定の代表）
        for banned in ("生年月日", "死亡日", "住所最新", "本籍最新", "導出元人物ID"):
            self.assertNotIn(banned, sd._PERSON_FETCH_FIELDS, banned)
        self.assertNotIn("住所", sd._HEIR_FETCH_FIELDS)
        self.assertNotIn("本籍", sd._HEIR_FETCH_FIELDS)
        self.assertNotIn("連絡先", sd._HEIR_FETCH_FIELDS)

    def test_projection_drops_unlisted_fields(self):
        # 未許可 PII field を fixture へ追加しても応答へ出ない（二重の防御の
        # 投影側。取得 fields 指定の側は query pin テストで固定）
        rows = [_rec(**{"$id": "1", "氏名": "山田太郎",
                        "住所": "SENTINEL-住所-11AA",
                        "電話番号": "SENTINEL-TEL-22BB"})]
        out = sd._project(rows, sd._CASE_LIST_FIELDS)
        self.assertEqual(set(out[0]), {"$id", "氏名"})

    def test_dashboard_response_never_carries_unlisted_fields(self):
        sent = "SENTINEL-未許可-33CC"
        case = [_rec(**{"$id": "12", "氏名": "山田太郎", "住所": sent})]
        persons = [_rec(**{"$id": "101", "氏名": "山田一郎", "生年月日": sent,
                           "住所最新": sent})]
        heirs = [_rec(**{"$id": "201", "氏名": "山田一郎", "住所": sent,
                         "取消済み": "no"})]
        assets = [_rec(**{"$id": "301", "財産種別": "預貯金", "特定情報": sent})]
        docs = [_rec(**{"$id": "401", "件名": "職務上請求書", "宛先住所": sent})]
        search = AsyncMock(side_effect=[case, persons, heirs, assets, docs])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", search), \
             patch.object(derivation_models, "get_current_head",
                          AsyncMock(return_value=None)):
            r = _client.get("/app/api/souzoku/cases/12",
                            headers=_auth_headers(), follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(sent, r.text)
        body = r.json()
        # filter 専用 field（統合状態・取消済み）も応答（VIEW）には出ない
        self.assertNotIn("取消済み", body["heirs"]["records"][0])
        self.assertNotIn("統合状態", body["persons"]["records"][0])

    def test_all_searches_specify_fields_and_no_entries_loop(self):
        # 構造 pin: field 追加が黙って公開範囲を広げない——
        # (i) module 内の全 search_records 呼出しが fields= を指定
        tree = ast.parse(Path(sd.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr == "search_records":
                kw = {k.arg for k in node.keywords}
                self.assertIn("fields", kw,
                              f"行{node.lineno}: fields 指定のない search_records")
        # (ii) 全 field 取得の get_record 呼出しが存在しない（AST 検査）
        called = {node.func.attr for node in ast.walk(tree)
                  if isinstance(node, ast.Call)
                  and isinstance(node.func, ast.Attribute)}
        self.assertNotIn("get_record", called)
        # (iii) 画面側の全 field 列挙ループの不在（閉集合の明示描画のみ）
        from hub.webapp_auth import WEBAPP_ROOT
        page = (WEBAPP_ROOT / "souzoku_case.html").read_text(encoding="utf-8")
        self.assertNotIn("Object.entries", page)


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

    def test_list_response_projected_to_closed_set(self):
        mock = AsyncMock(return_value=[_rec(**{"$id": "1", "氏名": "山田太郎",
                                               "電話番号": "090-SENT"})])
        r = self._get("/app/api/souzoku/cases", mock)
        self.assertNotIn("090-SENT", r.text)
        self.assertNotIn("電話番号", r.json()["records"][0])

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
    def _case_row(self):
        return _rec(**{"$id": "12", "氏名": "山田太郎", "被相続人名": "山田花子",
                       "書類ステータス": "送付状作成済"})

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

    def _call(self, record_id="12", search_side=None, head=None):
        search_mock = AsyncMock(
            side_effect=search_side if search_side is not None else
            [[self._case_row()], self._persons_raw(), self._heirs_raw(),
             self._assets(), self._docs()])
        head_mock = AsyncMock(return_value=head)
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", search_mock), \
             patch.object(derivation_models, "get_current_head", head_mock):
            r = _client.get(f"/app/api/souzoku/cases/{record_id}",
                            headers=_auth_headers(), follow_redirects=False)
        return r, search_mock

    def test_filters_and_exclusion_counts(self):
        r, _search = self._call(head=_head())
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
        self.assertEqual(body["assets"]["total"]["amount"], "1000000")
        self.assertEqual(body["documents"]["records"][0]["$id"]["value"], "401")
        self.assertEqual(body["derivation"]["head"],
                         {"run_id": 7, "run_status": "confirmed",
                          "provisional": False})
        self.assertEqual(body["notice"], sd.NOTICE_READONLY)

    def test_queries_and_fields_pinned(self):
        _r, search = self._call(head=None)
        calls = search.call_args_list
        apps = [c.args[0] for c in calls]
        self.assertEqual(apps, [sd.APP_SOUZOKU_CASES, sd.APP_KOSEKI_PERSON,
                                sd.APP_SOUZOKUNIN, sd.APP_ZAISAN,
                                sd.APP_SHIPPING])
        self.assertEqual(calls[0].args[1], '$id = "12" limit 1')
        self.assertEqual(calls[0].kwargs.get("fields"), sd._CASE_FIELDS)
        self.assertEqual(calls[1].args[1],
                         '案件レコードID = "12" and $id > 0 '
                         "order by $id asc limit 100")
        self.assertEqual(calls[1].kwargs.get("fields"),
                         sd._PERSON_FETCH_FIELDS)
        self.assertEqual(calls[2].args[1],
                         '案件レコードID = "12" and $id > 0 '
                         "order by $id asc limit 100")
        self.assertEqual(calls[2].kwargs.get("fields"), sd._HEIR_FETCH_FIELDS)
        self.assertEqual(calls[3].kwargs.get("fields"), sd._ASSET_FIELDS)
        # App30 は案件アプリID＋案件レコードID の両絞込（時効/相続の同居 app）
        self.assertEqual(calls[4].args[1],
                         '案件アプリID = "26" and 案件レコードID = "12" '
                         "order by 更新日時 desc limit 20")
        self.assertEqual(calls[4].kwargs.get("fields"), sd._DOC_FIELDS)
        # filter が状態 field を読めることの pin（黙った縮小の防波堤）
        self.assertIn("統合状態", sd._PERSON_FETCH_FIELDS)
        self.assertIn("取消済み", sd._HEIR_FETCH_FIELDS)

    def test_links_material_from_validated_env_only(self):
        r, _ = self._call(head=None)
        links = r.json()["links"]
        self.assertEqual(links["base"], "https://testsub.cybozu.com/k")
        self.assertEqual(links["apps"],
                         {"case": "26", "person": "34", "heir": "36",
                          "asset": "35", "shipping": "30"})

    def test_bad_record_id_fixed_404_no_kintone_call(self):
        for bad in ("abc", "1e3", "12345678901", "1;drop"):
            with self.subTest(rid=bad):
                r, search_mock = self._call(record_id=bad)
                self.assertEqual(r.status_code, 404)
                self.assertEqual(r.content, b"")
                search_mock.assert_not_called()

    def test_nonexistent_case_fixed_404(self):
        r, search = self._call(search_side=[[]])
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.content, b"")
        self.assertEqual(search.call_count, 1)   # 単票検索のみ・後続 section 無し

    def test_partial_degradation_section_flag_no_detail(self):
        # PARTIAL（①§6）: section 取得失敗は ok=false の固定 flag のみ・
        # 他 section は表示継続・例外詳細は応答へ非搭載
        r, _ = self._call(search_side=[
            [self._case_row()], RuntimeError("boom-SENTINEL"),
            self._heirs_raw(), self._assets(), self._docs()], head=None)
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["persons"], {"ok": False})
        self.assertTrue(body["heirs"]["ok"])
        self.assertNotIn("boom-SENTINEL", r.text)

    def test_business_data_response_is_no_store(self):
        # 業務データ応答の保存禁止（P4-004 共通契約の適用実測・①§12.5）
        r, _ = self._call(head=None)
        self.assertEqual(r.headers.get("cache-control"), "no-store, private")


# ── PWA-02: $id カーソル全件ページング（レビュー指定 5 本） ───────────────────
def _p36(i, cancelled="", **extra):
    f = {"$id": str(i), "氏名": f"相続人{i}", "続柄": "子"}
    if cancelled:
        f["取消済み"] = cancelled
    f.update(extra)
    return _rec(**f)


def _p34(i, state="", **extra):
    f = {"$id": str(i), "氏名": f"人物{i}"}
    if state:
        f["統合状態"] = state
    f.update(extra)
    return _rec(**f)


def _p35(i, amount, valid="yes", fixed="yes"):
    return _rec(**{"$id": str(i), "財産種別": "預貯金", "評価額": amount,
                   "評価確定": fixed, "有効": valid})


class TestCursorPaging(unittest.TestCase):
    def _patch(self, side):
        return patch.object(hub_kintone, "search_records",
                            AsyncMock(side_effect=side))

    def test_multi_page_full_fetch_and_cursor_queries(self):
        # (1) 複数 page 全件取得＋(5) カーソル単調増加・重複欠落なし
        page1 = [_p36(i) for i in range(1, 101)]          # 100 件=満 page
        page2 = [_p36(101), _p36(102)]                    # 端数 page で終端
        with self._patch([page1, page2]) as mock:
            out = _run(sd._load_heirs("12"))
        ids = [r["$id"]["value"] for r in out["records"]]
        self.assertEqual(ids, [str(i) for i in range(1, 103)])   # 重複・欠落なし
        self.assertEqual(out["excluded_cancelled_count"], 0)
        q1 = mock.call_args_list[0].args[1]
        q2 = mock.call_args_list[1].args[1]
        self.assertIn("$id > 0 ", q1)
        self.assertIn("$id > 100 ", q2)                   # 前 page 末尾がカーソル
        self.assertEqual(mock.call_count, 2)

    def test_later_page_invalid_rows_excluded(self):
        # (2) 後続 page の無効行（取消済み/統合済み無効）も除外される
        page1 = [_p36(i) for i in range(1, 101)]
        page2 = [_p36(101, cancelled="yes"), _p36(102)]
        with self._patch([page1, page2]):
            heirs = _run(sd._load_heirs("12"))
        self.assertEqual(len(heirs["records"]), 101)
        self.assertEqual(heirs["excluded_cancelled_count"], 1)
        self.assertNotIn("101",
                         [r["$id"]["value"] for r in heirs["records"]])
        p1 = [_p34(i) for i in range(1, 101)]
        p2 = [_p34(101, state="統合済み無効"), _p34(102)]
        with self._patch([p1, p2]):
            persons = _run(sd._load_persons("12"))
        self.assertEqual(len(persons["records"]), 101)
        self.assertEqual(persons["excluded_merged_count"], 1)

    def test_later_page_assets_counted_into_total(self):
        # (3) 後続 page の財産も合計へ算入（全件基準の合計）
        page1 = [_p35(i, "1") for i in range(1, 101)]
        page2 = [_p35(101, "5")]
        with self._patch([page1, page2]):
            out = _run(sd._load_assets("12"))
        self.assertEqual(len(out["records"]), 101)
        self.assertEqual(out["total"]["amount"], "105")
        self.assertEqual(out["total"]["counted"], 101)

    def test_mid_page_failure_raises_to_partial(self):
        # (4) 途中 page 失敗＝section 全体を PARTIAL（部分結果を完全値として
        # 返さない）。API 層では _guarded が ok=False へ写像する
        page1 = [_p35(i, "1") for i in range(1, 101)]
        with self._patch([page1, RuntimeError("boom")]):
            with self.assertRaises(RuntimeError):
                _run(sd._load_assets("12"))
        with self._patch([page1, RuntimeError("boom")]):
            self.assertEqual(_run(sd._guarded(sd._load_assets("12"))),
                             {"ok": False})   # _guarded が PARTIAL へ写像

    def test_cursor_monotonicity_guard(self):
        # (5) 補: カーソルの重複・逆行・非数字は例外（無限 loop・重複計上の遮断）
        dup = [_p36(5), _p36(5)]
        with self._patch([dup]):
            with self.assertRaises(ValueError):
                _run(sd._load_heirs("12"))
        backward = [_p36(5), _p36(3)]
        with self._patch([backward]):
            with self.assertRaises(ValueError):
                _run(sd._load_heirs("12"))
        nonnum = [_rec(**{"$id": "x5", "氏名": "n"})]
        with self._patch([nonnum]):
            with self.assertRaises(ValueError):
                _run(sd._load_heirs("12"))

    def test_runaway_page_limit_guard(self):
        # 満 page が続く限り取得し続けるが、上限超過は例外＝PARTIAL（暴走防御）
        pages = ([[_p36(i) for i in range(k * 100 + 1, k * 100 + 101)]
                  for k in range(0, 101)])
        with self._patch(list(pages)):
            with self.assertRaises(RuntimeError):
                _run(sd._load_heirs("12"))


# ── PWA-03: 金額の整数集計（レビュー指定 6 本） ───────────────────────────────
class TestAmountAggregation(unittest.TestCase):
    def test_1_boundary_2pow53_exact(self):
        # 2^53=9007199254740992 近傍の加算が正確（float なら丸まる値）
        rows = [_p35(1, "9007199254740993"), _p35(2, "9007199254740993")]
        t = sd._sum_assets(rows)
        self.assertTrue(t["computable"])
        self.assertEqual(t["amount"], "18014398509481986")

    def test_2_invalid_strings_not_silently_zero(self):
        for bad in ("1.5", "1e3", "-3", "1,000", "１００", " 1", "0x10"):
            with self.subTest(value=bad):
                t = sd._sum_assets([_p35(1, bad), _p35(2, "100")])
                self.assertEqual(t, {"computable": False})   # 部分合計も返さない

    def test_3_blank_distinct_from_zero(self):
        t = sd._sum_assets([_p35(1, ""), _p35(2, "0")])
        self.assertTrue(t["computable"])
        self.assertEqual(t["amount"], "0")
        self.assertEqual(t["counted"], 1)        # "0" は 0 円として算入
        self.assertEqual(t["blank_count"], 1)    # 空値は 0 円でなく対象外

    def test_4_unconfirmed_note_maintained(self):
        t = sd._sum_assets([_p35(1, "100", fixed="no"), _p35(2, "50")])
        self.assertEqual(t["amount"], "150")
        self.assertEqual(t["unconfirmed_count"], 1)

    def test_5_invalid_flag_no_rows_excluded(self):
        # 有効=no は金額があっても非算入（grammar 検査の対象にもしない）
        t = sd._sum_assets([_p35(1, "999", valid="no"), _p35(2, "1")])
        self.assertEqual(t["amount"], "1")
        self.assertEqual(t["counted"], 1)
        t2 = sd._sum_assets([_p35(1, "bad-value", valid="no"), _p35(2, "1")])
        self.assertTrue(t2["computable"])        # 無効行の不正値は集計に無関係
        self.assertEqual(t2["amount"], "1")

    def test_6_beyond_safe_integer_no_truncation(self):
        rows = [_p35(1, "90071992547409934567"), _p35(2, "1")]
        t = sd._sum_assets(rows)
        self.assertEqual(t["amount"], "90071992547409934568")   # 任意精度・桁落ちなし

    def test_api_returns_uncomputable_flag_and_string_amount(self):
        case = [_rec(**{"$id": "12", "氏名": "山田太郎"})]
        assets = [_p35(301, "1.5")]
        search = AsyncMock(side_effect=[case, [], [], assets, []])
        with patch.dict(os.environ, _ENV), \
             patch.object(hub_kintone, "search_records", search), \
             patch.object(derivation_models, "get_current_head",
                          AsyncMock(return_value=None)):
            r = _client.get("/app/api/souzoku/cases/12",
                            headers=_auth_headers(), follow_redirects=False)
        body = r.json()
        self.assertTrue(body["assets"]["ok"])    # 行表示は継続
        self.assertEqual(body["assets"]["total"], {"computable": False})

    def test_page_uses_string_formatting_not_number(self):
        # 構造 pin: 画面側に数値化 API（Number/parse/toLocaleString）が無い
        # ＝金額は文字列のまま固定 format（2^53 超でも桁落ちしない）
        from hub.webapp_auth import WEBAPP_ROOT
        page = (WEBAPP_ROOT / "souzoku_case.html").read_text(encoding="utf-8")
        for banned in ("Number(", "parseInt", "parseFloat", "toLocaleString"):
            self.assertNotIn(banned, page, banned)
        self.assertIn("集計不能", page)          # 集計不能注記の描画分岐が存在


# ── PII sentinel（業務データがログへ流れない実測） ────────────────────────────
class TestPiiSentinel(unittest.TestCase):
    def test_sentinel_pii_reaches_response_but_never_logs(self):
        sent_name = "SENTINEL-氏名-73AF"
        case = [_rec(**{"$id": "12", "氏名": sent_name})]
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
                 patch.object(hub_kintone, "search_records",
                              AsyncMock(side_effect=[case, persons, [], [],
                                                     []])), \
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
