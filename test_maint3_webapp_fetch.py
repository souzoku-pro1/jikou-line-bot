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


def _strip_js_comments(src: str) -> str:
    """// 行末コメントと /* */ ブロックコメントを除去（fix1 M02: コメント化に
    よる pin 迂回を照合前に排除。`://`（URL）は行コメント扱いしない）。"""
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"(?<!:)//[^\n]*", "", src)


# 生 fetch への到達手段の網羅（fix1 M02a）: 識別子 token としての fetch を
# 前後境界つきで検出——fetch( / fetch (空白) / fetch?.( / (0, fetch) /
# "fetch"・'fetch' の文字列リテラル（["fetch"] 動的アクセス）をすべて含む
_FETCH_TOKEN = re.compile(r"(?<![A-Za-z0-9_$])fetch(?![A-Za-z0-9_$])")


class TestFetchWrapperClosedSet(unittest.TestCase):
    """A: fetch ラッパー閉集合化の静的 pin（fix1 M02 で実効化）。"""

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
        # fix1 M02a: コメント除去後の全 webapp ソースで、fetch token
        # （呼出し・空白付き・fetch?.・(0, fetch)・"fetch" リテラルを包含）は
        # app.js の app_fetch ブロック内の 1 箇所のみ
        for name, src in self._all_sources().items():
            stripped = _strip_js_comments(src)
            hits = _FETCH_TOKEN.findall(stripped)
            if name != "app.js":
                self.assertEqual(
                    hits, [], f"{name}: ラッパー外に fetch 到達手段がある")
        app_js = _strip_js_comments((_WEBAPP / "app.js")
                                    .read_text(encoding="utf-8"))
        self.assertEqual(len(_FETCH_TOKEN.findall(app_js)), 1)
        block = _strip_js_comments(self._wrapper_block(
            (_WEBAPP / "app.js").read_text(encoding="utf-8")))
        self.assertEqual(len(_FETCH_TOKEN.findall(block)), 1,
                         "生 fetch が app_fetch ブロック外にある")

    def test_wrapper_structure_prefix_throw_fetch_order(self):
        # fix1 M02b: 文字列存在でなく**構造**を検証——app_fetch ブロック内で
        # 「prefix 検査（if）→ throw → fetch 呼出し」がこの行順で存在し、
        # throw が if ブロック内（fetch より前に閉じる）にあること。
        # コメント行は除去してから照合（コメント化による迂回を検出）
        src = (_WEBAPP / "app.js").read_text(encoding="utf-8")
        self.assertIn('const APP_API_PREFIX = "/app/api/";', src)
        block = _strip_js_comments(self._wrapper_block(src))
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

    def test_no_other_network_apis_anywhere(self):
        for name, src in self._all_sources().items():
            for banned in ("XMLHttpRequest", "WebSocket", "EventSource",
                           "sendBeacon", "importScripts"):
                self.assertNotIn(banned, src, name)
        # sw.js は fetch handler を登録しない（P4-001 裁定の維持）
        sw = (_WEBAPP / "sw.js").read_text(encoding="utf-8")
        self.assertNotIn('addEventListener("fetch"', sw)


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
