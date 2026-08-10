"""P4-005: hub/webapp_kinship_view（相続人関係図ビュー・read-only）のテスト。

固定する仕様（DRAFT_P4 §2/§5＋P4 系先例）:
- 全 route が P4-001 の関所（_gate）必須・公開例外なし・GET のみ（機械検査）＋
  全応答に Cache-Control: no-store, private（P4-004 先例の一元付与）
- 参照のみ: kintone 書込み API ゼロ・本 module は kintone を直接呼ばない
  （AST 機械検査＝P4-002 最終形 checker・ast_policy_helpers）
- 描画は Z1/Z2 流用（heir_scope=True 呼出しの pin・エンジン再実装なし）
- 導出結果の重畳: DerivationRun head 有り=凡例＋**未確定注記の常時同梱**・
  head 無し=overlay None（図のみ正常）
- Z1「拒否は道案内」: problems をそのまま返す（白画面/不明エラーにしない）
- case は数字列 grammar のみ・不正は固定 400（非反射・下流未到達）
- 画面（kinship.html）: HTML 文字列 API 不使用・img data URI（SVG の script
  実行文脈を持たない埋め込み）の静的 pin
"""

import ast
import asyncio
import os
import shutil
import tempfile
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
    "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "d",
    "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402
import hub.db as db  # noqa: E402
import hub.webapp_kinship_view as kv  # noqa: E402
from ast_policy_helpers import (  # noqa: E402
    _ALLOWED_KINTONE_ATTRS,
    _FORBIDDEN_IMPORTS,
    _binding_violations,
    _readonly_violations,
)
from hub.derivation_models import DerivationBase, create_derivation_run  # noqa: E402
from hub.webapp_auth import (  # noqa: E402
    MIN_ITERATIONS,
    PUBLIC_ROUTES,
    hash_password,
    issue_session,
)
from kinship_graph import KinshipGraph, PersonNode  # noqa: E402
from kinship_renderer import (  # noqa: E402
    GraphvizUnavailable,
    KinshipValidationRejected,
)

_client = TestClient(main.app)
_ENV = {
    "WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
    "WEBAPP_SESSION_SECRET": "s" * 32,
}


def _run(coro):
    return asyncio.run(coro)


def _auth_headers():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _node(rid, name, **kw):
    defaults = dict(gender="男", alive="生存", meyose="確定", kakunin="確認済")
    defaults.update(kw)
    return PersonNode(record_id=rid, name=name, **defaults)


def _graph(*nodes):
    return KinshipGraph(nodes=list(nodes), edges=[],
                        warnings=["No.11 長男: 婚姻の相手方「X」に一致する人物が"
                                  "いません（エッジ未作成）"])


SVG = b'<svg xmlns="http://www.w3.org/2000/svg"><text>&#22826;&#37070;</text></svg>'


class TestAuthAndReadOnly(unittest.TestCase):
    def test_unauthenticated_rejected(self):
        with patch.dict(os.environ, _ENV):
            for path in ("/app/api/kinship?case=9", "/app/kinship"):
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    self.assertEqual(r.status_code, 303)
                    self.assertEqual(r.headers["location"], "/app/login")
                    self.assertIn("no-store", r.headers.get("Cache-Control", ""))

    def test_all_routes_gated_get_only(self):
        routes = [r for r in kv.router.routes if hasattr(r, "endpoint")]
        self.assertGreaterEqual(len(routes), 2)
        for route in routes:
            self.assertEqual(route.methods, {"GET"})     # 参照のみ=GET 以外なし
            for method in route.methods:
                self.assertNotIn((route.path, method), PUBLIC_ROUTES)
                self.assertTrue(
                    getattr(route.endpoint, "__webapp_gate__", False),
                    f"{method} {route.path} に認証関所（_gate）がない")

    def test_no_direct_kintone_and_no_write_apis(self):
        src = Path(kv.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        used = {n.attr for n in ast.walk(tree)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name) and n.value.id == "kintone"}
        # 本 module は kintone を直接呼ばない（App34 読取は Z1 経由）
        self.assertEqual(used, set())
        self.assertLessEqual(used, _ALLOWED_KINTONE_ATTRS)
        for banned in ("create_record", "update_record", "delete_record",
                       "upload_file", "attach_kinship_to_case", "_write",
                       "router.post", "router.put", "router.delete"):
            self.assertNotIn(banned, src)

    def test_readonly_final_form_checker_passes(self):
        tree = ast.parse(Path(kv.__file__).read_text(encoding="utf-8"))
        self.assertEqual(_readonly_violations(tree), [])
        self.assertEqual(_binding_violations(tree), [])

    def test_no_forbidden_or_process_launch_imports(self):
        imported = set()
        tree = ast.parse(Path(kv.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(imported & _FORBIDDEN_IMPORTS, imported)


class _ApiBase(unittest.TestCase):
    """sqlite 実 DB（overlay 用）＋Z1/Z2 mock（dot バイナリ非依存）。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p4005_")
        env = dict(_ENV)
        env["DATABASE_URL"] = f"sqlite+aiosqlite:///{self._dir}/a.db"
        self._env = patch.dict(os.environ, env)
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(DerivationBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

        self.graph = _graph(_node("10", "被相続人 太郎", alive="死亡",
                                  death_date="2026-01-01", is_decedent=True),
                            _node("11", "長男 一郎"))
        self.load = AsyncMock(side_effect=lambda case: self.graph)
        self.render = unittest.mock.MagicMock(return_value=SVG)
        for target, mock in [("kinship_graph.load_graph_for_case", self.load),
                             ("kinship_renderer.render_kinship", self.render)]:
            p = patch(target, new=mock)
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _mk_run(self, *, case="9", payload=None, status="derived",
                provisional=True):
        pk = _run(create_derivation_run(
            case_app_id="26", case_record_id=case, decedent_person_id="10",
            at_date="2026-01-01", frozen_case_version="v0.1",
            input_person_revisions={}, input_person_ids=[],
            input_hash=f"ih-{os.urandom(8).hex()}", status=status, rank=1,
            result_payload=payload or {
                "heirs": [{"person_id": "11", "zokugara_code": "child",
                           "share": "1/1"}],
                "facts": ["minpo_890"]},
            result_hash="rh" * 32, lawyer_flags=None, provisional=provisional,
            supersedes_run_id=None, engine_version="e1"))
        db.reset_for_tests()
        return pk

    def _get(self, case="9"):
        r = _client.get(f"/app/api/kinship?case={case}",
                        headers=_auth_headers(), follow_redirects=False)
        db.reset_for_tests()
        return r


class TestApiBehavior(_ApiBase):
    def test_bad_case_is_fixed_400_without_downstream(self):
        for bad in ("", "abc", "9x", "0" * 11, "9%20or%201"):
            with self.subTest(case=bad):
                r = self._get(bad)
                self.assertEqual(r.status_code, 400)
                self.assertEqual(r.content, b"")     # 固定応答（非反射）
        self.load.assert_not_awaited()               # 下流未到達

    def test_ok_without_run_has_no_overlay(self):
        r = self._get()
        self.assertEqual(r.status_code, 200)
        self.assertIn("no-store", r.headers.get("Cache-Control", ""))
        data = r.json()
        self.assertEqual(data["status"], "ok")
        self.assertIn("<svg", data["svg"])
        self.assertIsNone(data["overlay"])           # run 無し=図のみ・正常
        self.assertEqual(data["names"]["10"], "被相続人 太郎")
        self.assertTrue(data["warnings"])            # Z1 warnings の素通し
        # Z2 呼出しは heir_scope=True（既存思想の維持を pin）
        _args, kwargs = self.render.call_args
        self.assertTrue(kwargs.get("heir_scope"))

    def test_ok_with_run_overlays_legend_and_notice(self):
        rid = self._mk_run()
        r = self._get()
        data = r.json()
        self.assertEqual(data["status"], "ok")
        ov = data["overlay"]
        self.assertEqual(ov["run_id"], rid)
        self.assertEqual(ov["run_status"], "derived")
        self.assertTrue(ov["provisional"])
        # 未確定注記の常時同梱（要件2: 必ず表示——API 契約側の pin）
        self.assertEqual(ov["notice"], kv.NOTICE_UNCONFIRMED)
        self.assertIn("機械は相続人を確定しません", ov["notice"])
        self.assertEqual(ov["heirs"], [{"person_id": "11", "zokugara": "子",
                                        "share_display": "1分の1"}])

    def test_old_run_without_zokugara_code_shows_blank(self):
        self._mk_run(payload={"heirs": [{"person_id": "11", "share": "1/2"}],
                              "facts": ["minpo_890"]})
        data = self._get().json()
        self.assertEqual(data["overlay"]["heirs"],
                         [{"person_id": "11", "zokugara": "",
                           "share_display": "2分の1"}])

    def test_not_renderable_passes_z1_guidance_through(self):
        problems = ["No.11 長男 一郎: 確認状態が「未確認」（確認済のみ描画可）",
                    "被相続人が特定されていません"
                    "（被相続人フラグ=yes の人物がいません）"]
        self.render.side_effect = KinshipValidationRejected(problems)
        rid = self._mk_run()
        data = self._get().json()
        self.assertEqual(data["status"], "not_renderable")
        self.assertEqual(data["problems"], problems)    # 道案内をそのまま写像
        self.assertEqual(data["overlay"]["run_id"], rid)

    def test_unavailable_is_explicit_degradation(self):
        self.render.side_effect = GraphvizUnavailable("dot がありません")
        data = self._get().json()
        self.assertEqual(data["status"], "unavailable")
        self.assertIn("graphviz", data["message"])

    def test_empty_case_is_explicit(self):
        self.graph = KinshipGraph()
        data = self._get().json()
        self.assertEqual(data["status"], "empty")
        self.assertIn("人物レコードがありません", data["message"])
        self.render.assert_not_called()


class TestPageStatics(unittest.TestCase):
    def test_page_served_under_gate(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/kinship", headers=_auth_headers())
            self.assertEqual(r.status_code, 200)
            self.assertIn("text/html", r.headers["content-type"])
            self.assertIn("no-store", r.headers.get("Cache-Control", ""))

    def test_html_uses_safe_dom_and_img_data_uri_only(self):
        src = (Path("webapp") / "kinship.html").read_text(encoding="utf-8")
        # HTML 文字列 API の全面禁止（P4-002 先例）
        for banned in ("innerHTML", "outerHTML", "insertAdjacentHTML",
                       "document.write", "DOMParser", "srcdoc"):
            self.assertNotIn(banned, src)
        # SVG は img の data URI（script 実行文脈を持たない埋め込み）のみ
        self.assertIn("data:image/svg+xml;charset=utf-8,", src)
        self.assertNotIn("<object", src)
        self.assertNotIn("<iframe", src)
        self.assertNotIn("<embed", src)
        self.assertIn("createElement", src)
        self.assertIn("textContent", src)
        # 未確定注記と道案内の描画部が存在（overlay.notice / problems 列挙）
        self.assertIn("ov.notice", src)
        self.assertIn("data.problems", src)

    def test_dashboard_links_kinship(self):
        src = (Path("webapp") / "index.html").read_text(encoding="utf-8")
        self.assertIn('href="/app/kinship"', src)


if __name__ == "__main__":
    unittest.main()
