"""Q-BATCH-1: 案件質問応答（PWA 搭載・read-only tool use）のテスト。

固定する仕様（③項目10 要件2〜7＋12.2-1＋大野裁定 2026-08-17=PWA 搭載）:
- tool は読み取り専用 kintone 検索の閉集合のみ（_TOOLS と _DISPATCH の一致・
  書込み tool の不存在・AST で kintone 書込み API 呼出しゼロ）
- 出典必須: 出典はサーバ実測記録。出典ゼロの断定回答は返さない（fail-closed
  固定文言へ）。
- 未確定注記・信頼度格付け（読解JSON=手書き低確信度・OCR=原本確認推奨）は
  サーバ機械判定の固定文言。
- Q&A 台帳（qa_record・業務データと分離）への保存（質問・回答・出典・
  コスト概算・所要・モデル・user_id）。
- PII 規律: 質問・回答がログに出ない（sentinel 実測）。質問は form POST
  （GET query に載せない）。
- 認証 negative（全 route _gate・未認証 303）・レート制限・エラー時
  fail-closed（推測で埋めた回答を返さない）。
"""

import asyncio
import logging
import os
import shutil
import tempfile
import time
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
    # NB: APP_KOSEKI_BOOK はここで恒久注入しない（p4_005 が「env 未設定=
    # 縮退」を前提にするため。必要なテストは _ENV の patch.dict で注入）
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import config  # noqa: E402
import main  # noqa: E402
import hub.db as db  # noqa: E402
import hub.qa_store as qa_store  # noqa: E402
import hub.webapp_q as wq  # noqa: E402
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
    "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "d",
}


def _auth_headers():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _run(coro):
    return asyncio.run(coro)


def _resp(stop, content, in_tok=1000, out_tok=100):
    return SimpleNamespace(
        stop_reason=stop, content=content,
        usage=SimpleNamespace(input_tokens=in_tok, output_tokens=out_tok,
                              cache_read_input_tokens=0,
                              cache_creation_input_tokens=0))


def _tool_use(name, args, tid="tu1"):
    return SimpleNamespace(type="tool_use", name=name, input=args, id=tid)


def _submit(answer, refs, tid="sub1"):
    return _tool_use("submit_answer",
                     {"answer": answer, "source_refs": refs}, tid)


def _text(t):
    return SimpleNamespace(type="text", text=t)


def _stub_client(side_effect):
    return SimpleNamespace(messages=SimpleNamespace(
        create=AsyncMock(side_effect=side_effect)))


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


class _DbMixin(unittest.TestCase):
    """qa_record 用の file sqlite（rv04b の流儀）。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="qb1_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/q.db"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(qa_store.metadata.create_all)
        asyncio.run(_create())
        db.reset_for_tests()
        wq._ask_times.clear()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)
        wq._ask_times.clear()


# ── 認証境界 ─────────────────────────────────────────────────────────────────
class TestAuthBoundary(unittest.TestCase):
    def test_unauthenticated_all_rejected(self):
        with patch.dict(os.environ, _ENV):
            for path in ("/app/q", "/app/api/q/history"):
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    self.assertEqual(r.status_code, 303)
                    self.assertEqual(r.headers["location"], "/app/login")
            r = _client.post("/app/q/ask", data={"question": "x"},
                             follow_redirects=False)
            self.assertEqual(r.status_code, 303)
            self.assertEqual(r.headers["location"], "/app/login")

    def test_all_routes_gated_no_public_exception(self):
        routes = [r for r in wq.router.routes if hasattr(r, "endpoint")]
        self.assertGreaterEqual(len(routes), 3)
        for route in routes:
            for method in route.methods:
                with self.subTest(path=route.path, method=method):
                    self.assertNotIn((route.path, method), PUBLIC_ROUTES)
                    self.assertTrue(
                        getattr(route.endpoint, "__webapp_gate__", False),
                        f"{method} {route.path} に認証関所（_gate）がない")

    def test_only_post_route_is_ask(self):
        # 「機械は確定しない」: 書き込み系 route は質問受付（Q&A 台帳への
        # 追記のみ・業務正本に触れない）の 1 本だけ
        posts = {r.path for r in wq.router.routes
                 if hasattr(r, "methods") and "POST" in r.methods}
        self.assertEqual(posts, {"/app/q/ask"})


# ── tool 閉集合と read-only 構造 pin ─────────────────────────────────────────
_EXPECTED_TOOLS = [
    "list_souzoku_cases", "get_souzoku_case", "list_case_persons",
    "list_case_heirs", "list_case_assets", "list_case_documents",
    "list_case_kosekis", "list_jikou_cases", "get_jikou_case",
    "list_case_chats",
]


class TestToolClosedSet(unittest.TestCase):
    def test_tool_names_pinned_exactly(self):
        self.assertEqual([t["name"] for t in wq._TOOLS], _EXPECTED_TOOLS)

    def test_dispatch_matches_tools(self):
        self.assertEqual(set(wq._DISPATCH), set(_EXPECTED_TOOLS))

    def test_submit_tool_separated_and_structured(self):
        # Q-02(ii): 最終回答は submit_answer の構造化出力のみ。読み取り閉集合
        # （_TOOLS/_DISPATCH）には含めない（kintone へ到達しない）
        self.assertEqual(wq.SUBMIT_TOOL_NAME, "submit_answer")
        self.assertNotIn(wq.SUBMIT_TOOL_NAME, wq._DISPATCH)
        self.assertNotIn(wq.SUBMIT_TOOL_NAME,
                         [t["name"] for t in wq._TOOLS])
        schema = wq._SUBMIT_TOOL["input_schema"]
        self.assertEqual(schema["required"], ["answer", "source_refs"])
        self.assertFalse(schema["additionalProperties"])
        self.assertTrue(wq._SUBMIT_TOOL["strict"])

    def test_no_write_semantics_in_tool_names(self):
        for banned in ("create", "update", "delete", "upload", "write",
                       "send", "post", "confirm", "approve"):
            for name in _EXPECTED_TOOLS:
                self.assertNotIn(banned, name, name)

    def test_unknown_tool_is_error_fixed(self):
        ctx = {"sources": [], "source_keys": set(), "flags": set()}
        content, is_error = _run(wq._dispatch("delete_record", {}, ctx))
        self.assertTrue(is_error)
        self.assertIn("読み取り専用の閉集合", content)
        self.assertEqual(ctx["sources"], [])

    def test_bad_record_id_no_kintone_call(self):
        ctx = {"sources": [], "source_keys": set(), "flags": set()}
        with patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock()) as mock:
            content, is_error = _run(wq._dispatch(
                "list_case_heirs", {"case_record_id": "9; drop"}, ctx))
        self.assertTrue(is_error)
        mock.assert_not_called()

    def test_handler_exception_fixed_error(self):
        ctx = {"sources": [], "source_keys": set(), "flags": set()}
        with patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(side_effect=RuntimeError("boom-SENT"))):
            content, is_error = _run(wq._dispatch(
                "list_case_heirs", {"case_record_id": "9"}, ctx))
        self.assertTrue(is_error)
        self.assertNotIn("boom-SENT", content)   # 例外詳細は LLM へ流さない


from ast_policy_helpers import (_FORBIDDEN_IMPORTS,  # noqa: E402
                                _binding_violations, _readonly_violations)
import ast  # noqa: E402


class TestReadOnlyStructure(unittest.TestCase):
    def _imports(self, tree):
        out = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                out.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                out.add((node.module or "").split(".")[0])
        return out

    def test_webapp_q_readonly_and_no_logging(self):
        src = Path(wq.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        used = {n.attr for n in ast.walk(tree)
                if isinstance(n, ast.Attribute)
                and isinstance(n.value, ast.Name) and n.value.id == "kintone"}
        self.assertTrue(used)
        self.assertLessEqual(used, {"search_records"},
                             f"許可外の kintone API: {used}")
        self.assertEqual(_readonly_violations(tree), [])
        self.assertEqual(_binding_violations(tree), [])
        imported = self._imports(tree)
        self.assertFalse(imported & _FORBIDDEN_IMPORTS, imported)
        self.assertNotIn("logging", imported)     # PII 反射経路なし（構造）
        for banned in ("create_record", "update_record", "delete_record",
                       "upload_file"):
            self.assertNotIn(banned, src)

    def test_qa_store_separated_from_kintone(self):
        src = Path(qa_store.__file__).read_text(encoding="utf-8")
        tree = ast.parse(src)
        imported = self._imports(tree)
        # 業務データと分離（構造）: kintone を import しない・kintone 名の
        # 束縛が一切ない（docstring の言及は対象外＝AST 基準）
        full_imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                full_imports.update(a.name for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                full_imports.add(node.module or "")
                full_imports.update(a.name for a in node.names)
        self.assertFalse({m for m in full_imports if "kintone" in m},
                         full_imports)
        self.assertNotIn("logging", imported)


# ── ask フロー（mock Claude・実 DB=sqlite） ──────────────────────────────────
class TestAskFlow(_DbMixin):
    def _heirs(self):
        return {"records": [_rec(**{"$id": "201", "氏名": "山田一郎",
                                    "続柄": "子", "戸籍確認済": "no"})],
                "excluded_cancelled_count": 1}

    def _post(self, question, stub):
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=self._heirs())):
            return _client.post("/app/q/ask", data={"question": question},
                                headers=_auth_headers(),
                                follow_redirects=False)

    def _history(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/api/q/history", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        return r.json()

    def test_happy_path_with_sources_and_notes(self):
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_heirs", {"case_record_id": "12"})]),
            _resp("tool_use",
                  [_submit("案件 No.12 の相続人のうち戸籍未確認は山田一郎です。",
                           [{"app": "App36(相続人)", "record_id": "201"}])]),
        ])
        r = self._post("案件12の戸籍未確認は？", stub)
        self.assertEqual(r.status_code, 303)
        self.assertTrue(r.headers["location"].startswith("/app/q?done="))
        data = self._history()
        self.assertTrue(data["available"])
        rec = data["records"][0]
        self.assertEqual(rec["status"], "ok")
        self.assertIn("山田一郎", rec["answer"])
        self.assertIn(wq.DISCLAIMER, rec["answer"])       # 定型文を全回答に付す
        # 出典=サーバ実測記録（kintone リンク付き）
        self.assertEqual(rec["sources"][0]["app"], "App36(相続人)")
        self.assertEqual(rec["sources"][0]["record_id"], "201")
        self.assertEqual(rec["sources"][0]["url"],
                         "https://testsub.cybozu.com/k/36/show#record=201")
        # 未確定注記（サーバ機械判定の固定文言）
        self.assertIn(wq.FLAG_NOTES["koseki_unconfirmed"], rec["notes"])
        self.assertIn(wq.FLAG_NOTES["cancelled_excluded"], rec["notes"])
        # コスト・所要・モデルの台帳記録
        self.assertEqual(rec["model"], config.PRIMARY_MODEL)
        self.assertEqual(rec["input_tokens"], 2000)
        self.assertEqual(rec["output_tokens"], 200)
        self.assertNotEqual(rec["cost_usd"], "")
        self.assertGreaterEqual(rec["elapsed_ms"], 0)

    def test_no_source_assertion_fails_closed(self):
        # 要件3: tool を一度も呼ばず断定した回答は返さない（submit を経ない
        # 本文回答も採用しない=Q-02）
        stub = _stub_client([
            _resp("end_turn", [_text("相続人は山田一郎で確定です。")]),
        ])
        r = self._post("相続人は誰？", stub)
        self.assertEqual(r.status_code, 303)
        rec = self._history()["records"][0]
        self.assertEqual(rec["status"], "no_source")
        self.assertIn(wq.NO_SOURCE_ANSWER, rec["answer"])
        self.assertNotIn("山田一郎", rec["answer"])   # 断定文は破棄
        self.assertEqual(rec["sources"], [])

    def test_api_error_fails_closed(self):
        stub = _stub_client([RuntimeError("api down")])
        r = self._post("質問", stub)
        self.assertEqual(r.status_code, 303)
        rec = self._history()["records"][0]
        self.assertEqual(rec["status"], "error")
        self.assertIn(wq.ERROR_ANSWER, rec["answer"])

    def test_rate_limit_fixed_redirect_no_api_call(self):
        now = time.time()
        wq._ask_times.extend([now] * wq.RATE_LIMIT)
        stub = _stub_client([])
        r = self._post("質問", stub)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/q?e=rate")
        stub.messages.create.assert_not_called()

    def test_empty_question_rejected(self):
        stub = _stub_client([])
        r = self._post("   ", stub)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/q?e=input")
        stub.messages.create.assert_not_called()


# ── 信頼度格付け（要件7）とコスト ────────────────────────────────────────────
class TestGradingAndCost(unittest.TestCase):
    def setUp(self):
        wq._ask_times.clear()

    def test_koseki_reading_low_confidence_note_with_pdf(self):
        import kinship_graph
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_kosekis", {"case_record_id": "9"})]),
            _resp("tool_use",
                  [_submit("戸籍は取得済みです。",
                           [{"app": "App33(戸籍読解)", "record_id": "70"}])]),
        ])
        pdf = "https://drive.google.com/file/d/1AbC-dEfG_hIjKlMnOpQrStUv/view"
        rows = [{"record_id": "70", "honseki": "川口市", "hittousha": "山田",
                 "juzen_honseki": "", "pdf_url": pdf}]
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(kinship_graph, "load_koseki_summaries_for_case",
                          AsyncMock(return_value=rows)):
            result = _run(wq._answer_question("戸籍の取得状況は？"))
        self.assertEqual(result["status"], "ok")
        self.assertIn(wq.FLAG_NOTES["koseki_reading"], result["notes"])
        src = result["sources"][0]
        self.assertEqual(src["app"], "App33(戸籍読解)")
        self.assertEqual(src["pdf_url"], pdf)     # 原本リンク必須（要件7）

    def test_ocr_numbers_note(self):
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_assets", {"case_record_id": "9"})]),
            _resp("tool_use",
                  [_submit("評価額合計は100万円です。",
                           [{"app": "App35(財産)", "record_id": "301"}])]),
        ])
        assets = {"records": [_rec(**{"$id": "301", "財産種別": "不動産_土地",
                                      "評価額": "1000000",
                                      "データ源": "OCR_課税明細"})],
                  "total": {"computable": True, "amount": "1000000",
                            "counted": 1, "unconfirmed_count": 1,
                            "blank_count": 0}}
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_assets",
                          AsyncMock(return_value=assets)):
            result = _run(wq._answer_question("財産の評価は？"))
        self.assertIn(wq.FLAG_NOTES["ocr_numbers"], result["notes"])
        self.assertIn(wq.FLAG_NOTES["valuation_unconfirmed"], result["notes"])

    def test_cost_estimate_decimal_string(self):
        self.assertEqual(
            wq._estimate_cost("claude-sonnet-4-6", 1_000_000, 0, 0, 0),
            "3.000000")
        self.assertEqual(
            wq._estimate_cost("claude-sonnet-4-6", 0, 1_000_000, 0, 0),
            "15.000000")
        self.assertEqual(
            wq._estimate_cost("unknown-model", 100, 100, 0, 0), "unknown")


# ── PII sentinel（質問・回答がログへ流れない実測） ────────────────────────────
class TestPiiSentinel(_DbMixin):
    def test_question_and_answer_never_log(self):
        sent_q = "SENTINEL-質問-91AB は何ですか"
        sent_a = "SENTINEL-回答-77CD"
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_heirs", {"case_record_id": "12"})]),
            _resp("tool_use",
                  [_submit(sent_a,
                           [{"app": "App36(相続人)", "record_id": "201"}])]),
        ])
        heirs = {"records": [_rec(**{"$id": "201", "氏名": "n",
                                     "戸籍確認済": "yes"})],
                 "excluded_cancelled_count": 0}
        records = []

        class _Cap(logging.Handler):
            def emit(self, record):
                records.append(record.getMessage())

        cap = _Cap()
        root = logging.getLogger()
        root.addHandler(cap)
        old = root.level
        # 本番の実効レベル=INFO 基準（DEBUG は aiosqlite 等が SQL 引数を内部
        # ログに含み得るが、本番では emit されない。アプリ自身のログ経路に
        # 質問・回答が載らないことを INFO 以上で実測する）
        root.setLevel(logging.INFO)
        try:
            with patch.dict(os.environ, _ENV), \
                 patch.object(wq, "_anthropic_client", lambda: stub), \
                 patch.object(wq.souzoku_dash, "_load_heirs",
                              AsyncMock(return_value=heirs)):
                r = _client.post("/app/q/ask", data={"question": sent_q},
                                 headers=_auth_headers(),
                                 follow_redirects=False)
            with patch.dict(os.environ, _ENV):
                h = _client.get("/app/api/q/history", headers=_auth_headers(),
                                follow_redirects=False)
        finally:
            root.setLevel(old)
            root.removeHandler(cap)
        self.assertEqual(r.status_code, 303)
        logs = "\n".join(records)
        self.assertNotIn(sent_q, logs)
        self.assertNotIn(sent_a, logs)
        rec = h.json()["records"][0]
        self.assertEqual(rec["question"], sent_q)     # 台帳へは保存される
        self.assertIn(sent_a, rec["answer"])
        self.assertEqual(h.headers.get("cache-control"), "no-store, private")


# ── history API・画面 ────────────────────────────────────────────────────────
class TestHistoryAndPages(unittest.TestCase):
    def test_history_invalid_paging_400(self):
        with patch.dict(os.environ, _ENV):
            for qs in ("limit=51", "limit=0", "limit=x", "offset=-1"):
                with self.subTest(qs=qs):
                    r = _client.get(f"/app/api/q/history?{qs}",
                                    headers=_auth_headers(),
                                    follow_redirects=False)
                    self.assertEqual(r.status_code, 400)
                    self.assertEqual(r.content, b"")

    def test_history_db_unavailable_flagged(self):
        with patch.dict(os.environ, _ENV), \
             patch.object(wq.qa_store, "list_qa",
                          AsyncMock(side_effect=RuntimeError("db down"))):
            r = _client.get("/app/api/q/history", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), {"records": [], "available": False})

    def test_q_page_served_with_form_post(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/q", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        self.assertIn("質問", r.text)
        # 質問は form POST（access log に載る GET query を使わない）
        self.assertIn('method="post" action="/app/q/ask"', r.text)
        self.assertIn('app_fetch("/app/api/q/history', r.text)
        self.assertIn('src="/app/shell.js"', r.text)

    def test_nav_links_q(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app", headers=_auth_headers(),
                            follow_redirects=False)
            s = _client.get("/app/shell.js", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertIn('href="/app/q"', r.text)
        self.assertIn("/app/q", s.text)


# ── qa_store（保存層） ───────────────────────────────────────────────────────
class TestQaStore(_DbMixin):
    def test_save_and_list_roundtrip(self):
        qa_id = _run(qa_store.save_qa(
            user_id="owner", question="q", answer="a", status="ok",
            sources=[{"app": "App36(相続人)", "record_id": "1", "url": None}],
            notes=["n"], model="claude-sonnet-4-6", input_tokens=10,
            output_tokens=5, cache_read_tokens=0, cost_usd="0.000105",
            elapsed_ms=1200))
        self.assertGreaterEqual(qa_id, 1)
        rows = _run(qa_store.list_qa(limit=10, offset=0))
        self.assertEqual(rows[0]["question"], "q")
        self.assertEqual(rows[0]["sources"][0]["app"], "App36(相続人)")
        self.assertEqual(rows[0]["cost_usd"], "0.000105")

    def test_status_closed_set_pinned(self):
        self.assertEqual(qa_store.STATUS_VALUES, ("ok", "no_source", "error"))


# ── Q-01: 消費量の上限（4 境界の negative） ───────────────────────────────────
def _small_heirs():
    return {"records": [_rec(**{"$id": "201", "氏名": "n",
                                "戸籍確認済": "yes"})],
            "excluded_cancelled_count": 0}


class TestConsumptionLimits(unittest.TestCase):
    def setUp(self):
        wq._ask_times.clear()

    def _answer(self, stub, heirs=None):
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=heirs if heirs is not None
                                    else _small_heirs())):
            return _run(wq._answer_question("質問"))

    def test_1_too_many_tool_use_in_one_response(self):
        # (iii) 1 response 内の tool_use 数上限（超過=ERROR・dispatch 非到達）
        blocks = [_tool_use("list_case_heirs", {"case_record_id": str(i)},
                            tid=f"tu{i}") for i in range(1, 7)]   # 6 > 5
        stub = _stub_client([_resp("tool_use", blocks)])
        load = AsyncMock(return_value=_small_heirs())
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs", load):
            result = _run(wq._answer_question("質問"))
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["answer"], wq.ERROR_ANSWER)
        load.assert_not_called()

    def test_2_total_tool_calls_capped(self):
        # (ii) 全 turn 合計 20 を超えたら fail-closed（5×5=25 で 5turn 目に超過）
        def turn(k):
            return _resp("tool_use", [
                _tool_use("list_case_heirs", {"case_record_id": str(k * 10 + i)},
                          tid=f"t{k}-{i}") for i in range(5)])
        stub = _stub_client([turn(k) for k in range(5)])
        result = self._answer(stub)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["answer"], wq.ERROR_ANSWER)
        self.assertEqual(stub.messages.create.await_count, 5)

    def test_3_huge_tool_result_discarded_with_guidance(self):
        # (iv) 巨大 loader 結果は「黙って切り捨てず」固定文言で再質問誘導・
        # 出典にも数えない（切捨て領域参照は Q-02 側で拒否される）
        huge = {"records": [_rec(**{"$id": str(200 + i),
                                    "氏名": "山" * 200})
                            for i in range(300)],
                "excluded_cancelled_count": 0}
        ctx = {"sources": [], "source_keys": set(), "flags": set()}
        with patch.dict(os.environ, _ENV), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=huge)):
            content, is_error = _run(wq._dispatch(
                "list_case_heirs", {"case_record_id": "12"}, ctx))
        self.assertTrue(is_error)
        self.assertEqual(content, wq.TOO_LARGE_RESULT)
        self.assertEqual(ctx["sources"], [])       # 出典へ統合しない
        # end-to-end: 破棄領域を引用した submit は no_source へ fail-closed
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_heirs", {"case_record_id": "12"})]),
            _resp("tool_use",
                  [_submit("大量データに基づく回答",
                           [{"app": "App36(相続人)", "record_id": "205"}])]),
        ])
        result = self._answer(stub, heirs=huge)
        self.assertEqual(result["status"], "no_source")
        self.assertEqual(result["answer"], wq.NO_SOURCE_ANSWER)
        self.assertEqual(result["sources"], [])

    def test_4_wall_clock_timeout(self):
        # (i) 全体 wall-clock timeout（API が沈黙しても固定文言へ）
        async def slow(*args, **kwargs):
            await asyncio.sleep(0.5)
        stub = SimpleNamespace(messages=SimpleNamespace(
            create=AsyncMock(side_effect=slow)))
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq, "TOTAL_TIMEOUT_SECONDS", 0.05):
            result = _run(wq._answer_question("質問"))
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["answer"], wq.ERROR_ANSWER)


# ── Q-02: 出典と回答の対応保証（negative） ────────────────────────────────────
class TestSourceCorrespondence(unittest.TestCase):
    def setUp(self):
        wq._ask_times.clear()

    def _ctx(self):
        return {"sources": [], "source_keys": set(), "flags": set()}

    def test_invalid_ids_not_recorded_as_sources(self):
        # (i) 空・grammar 外の app_id/record_id は出典に数えない
        ctx = self._ctx()
        wq._record_source(ctx, "App36(相続人)", "36", "")        # 空 ID
        wq._record_source(ctx, "App36(相続人)", "36", "abc")     # 非数字
        wq._record_source(ctx, "App36(相続人)", "", "201")       # app 空
        wq._record_source(ctx, "App36(相続人)", "x36", "201")    # app 非数字
        self.assertEqual(ctx["sources"], [])
        wq._record_source(ctx, "App36(相続人)", "36", "201")     # 正常形
        self.assertEqual(len(ctx["sources"]), 1)

    def _answer(self, stub):
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=_small_heirs())):
            return _run(wq._answer_question("質問"))

    def test_assertion_after_unrelated_tool_rejected(self):
        # (iv) 無関係 tool 1 回後の断定（refs 空）は通らない
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_heirs", {"case_record_id": "12"})]),
            _resp("tool_use", [_submit("断定回答", [])]),
        ])
        result = self._answer(stub)
        self.assertEqual(result["status"], "no_source")
        self.assertEqual(result["answer"], wq.NO_SOURCE_ANSWER)

    def test_unrecorded_source_reference_rejected(self):
        # (iv) 実測集合に無い source 参照は拒否
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_heirs", {"case_record_id": "12"})]),
            _resp("tool_use",
                  [_submit("読んでいない記録を引用",
                           [{"app": "App36(相続人)", "record_id": "999"}])]),
        ])
        result = self._answer(stub)
        self.assertEqual(result["status"], "no_source")
        self.assertEqual(result["sources"], [])

    def test_sources_narrowed_to_cited_subset(self):
        # (ii) 出典=回答で使用した参照の閉集合（読んだが引用しない記録は
        # 出典に載せない）
        heirs = {"records": [
            _rec(**{"$id": "201", "氏名": "a", "戸籍確認済": "yes"}),
            _rec(**{"$id": "202", "氏名": "b", "戸籍確認済": "yes"})],
            "excluded_cancelled_count": 0}
        stub = _stub_client([
            _resp("tool_use",
                  [_tool_use("list_case_heirs", {"case_record_id": "12"})]),
            _resp("tool_use",
                  [_submit("No.201 についての回答",
                           [{"app": "App36(相続人)", "record_id": "201"}])]),
        ])
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=heirs)):
            result = _run(wq._answer_question("質問"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual([s["record_id"] for s in result["sources"]], ["201"])

    def test_malformed_refs_rejected(self):
        for bad in ("not-a-list", [{"app": "App36(相続人)"}], ["x"], None):
            with self.subTest(refs=repr(bad)[:20]):
                out = wq._validated_submission(
                    {"answer": "a", "source_refs": bad},
                    {"sources": [{"app": "App36(相続人)", "record_id": "201",
                                  "url": None}],
                     "source_keys": set(), "flags": set()})
                self.assertIsNone(out)


# ── レビュー留保: SQLAlchemy echo/debug の無効 pin ────────────────────────────
class TestDbEchoDisabled(_DbMixin):
    def test_engine_echo_disabled(self):
        # SQL bind（質問・回答が INSERT パラメータとして流れる）が engine echo
        # で emit されない構造の pin（本番=echo 無効・db.py に echo 指定なし）
        eng = db.get_async_engine()
        self.assertFalse(bool(eng.sync_engine.echo))
        src = Path(db.__file__).read_text(encoding="utf-8")
        self.assertNotIn("echo=True", src)
        self.assertNotIn('echo="debug"', src)


if __name__ == "__main__":
    unittest.main()
