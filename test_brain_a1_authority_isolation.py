"""BRAIN-A1 T-E 権限・障害分離（v3 §10-5）

固定する仕様:
- 確認ビューの全 route は認証関所（_gate）必須・未認証は 303→/app/login（GET/POST とも）
- 操作は 操作 ID（UUID）で冪等（再送は 303 done=dup・行は増えない）、画面が見ていた版と
  現在の版の不一致は 409 で最新を返す、不正入力は固定 400（反射なし）
- ログ/通知に PII ゼロ: brain 系 4 module は sink AST policy に違反ゼロ（allowlist 追加ゼロ）、
  同期の実ログに氏名・LINE ID・値が出ない。brain_ledger/brain_link/webapp_brain_view は
  logging を import しない
- DB 障害で LINE webhook・承認参照・発送（dispatch 入口）へ影響しない（brain 内で握る）
- kintone 書込 API・通知・外部送信は一切呼ばれない（sync・照合・操作のすべてで）
- BRAIN_SYNC_ENABLED 未設定なら同期は走らない（kintone 未到達・DB 未到達）。
  scheduler には常時登録（5 分間隔）・config.py には名前の定数のみ（R7）
- webapp_brain_view は read-only AST 検査（kintone 非使用・禁止 import なし）に合格
- main.py の変更は末尾追記のみ（sink allowlist の番地に触れない）
"""

import ast
import os
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from brain_test_support import (BRAIN_ENV, LINE_A, BrainDbMixin, app28, app30, app40,
                                run)

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    **BRAIN_ENV,
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402
import main  # noqa: E402
import config  # noqa: E402
import hub.kintone as hub_kintone  # noqa: E402
import hub.notify as hub_notify  # noqa: E402
from hub import brain_ledger as ledger  # noqa: E402
from hub import brain_sync as sync  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
from hub import webapp_brain_view as bv  # noqa: E402
from hub.webapp_auth import (MIN_ITERATIONS, PUBLIC_ROUTES, hash_password,  # noqa: E402
                             issue_session)
from ast_policy_helpers import (_FORBIDDEN_IMPORTS,  # noqa: E402
                                _context_allowlist_violations)
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
_ENV = {"WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
        "WEBAPP_SESSION_SECRET": "s" * 32}
BRAIN_FILES = ("hub/brain_ledger.py", "hub/brain_link.py", "hub/brain_sync.py",
               "hub/webapp_brain_view.py")
OP1 = "11111111-2222-4333-8444-555555555555"
OP2 = "11111111-2222-4333-8444-666666666666"
OP3 = "11111111-2222-4333-8444-777777777777"
OP4 = "11111111-2222-4333-8444-888888888888"


def _auth():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _routes():
    return [r for r in bv.router.routes if hasattr(r, "endpoint")]


class TestAuthGate(unittest.TestCase):
    def test_all_routes_gated_and_not_public(self):
        routes = _routes()
        self.assertGreaterEqual(len(routes), 9)
        for route in routes:
            for method in route.methods:
                with self.subTest(path=route.path, method=method):
                    self.assertNotIn((route.path, method), PUBLIC_ROUTES)
                    self.assertTrue(getattr(route.endpoint, "__webapp_gate__", False))

    def test_unauthenticated_rejected_get_and_post(self):
        with patch.dict(os.environ, _ENV):
            for path in ("/app/brain", "/app/api/brain/overview", "/app/api/brain/pending",
                         "/app/api/brain/holds", "/app/api/brain/conflicts",
                         "/app/api/brain/recheck", "/app/api/brain/facts?app=40&record=1"):
                r = _client.get(path, follow_redirects=False)
                self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/login"), path)
            for path in ("/app/brain/confirm", "/app/brain/relink"):
                r = _client.post(path, data={"operation_id": OP1}, follow_redirects=False)
                self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/login"), path)

    def test_page_served_and_nav_linked_when_authed(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/brain", headers=_auth(), follow_redirects=False)
            self.assertEqual(r.status_code, 200)
            self.assertIn('<script src="/app/app.js"></script>', r.text)
            self.assertEqual(r.headers.get("cache-control"), "no-store, private")
            s = _client.get("/app/shell.js", headers=_auth(), follow_redirects=False)
            self.assertIn('"/app/brain"', s.text)
        # /app/brain は catch-all より前に結線されている（404 にならない）
        self.assertEqual(r.status_code, 200)


class TestOperations(BrainDbMixin):
    def _seed(self):
        self.fake.data["40"] = [app40(1, 1)]
        run(sync.sync_target(sync.TARGET_APP40))
        facts = run(ledger.list_case_facts("40", "1"))
        return [f for f in facts if f["item_code"] == "app40.status"][0]

    def test_confirm_idempotent_and_version_conflict(self):
        f = self._seed()
        with patch.dict(os.environ, _ENV):
            body = {"operation_id": OP1, "fact_id": str(f["fact_id"]),
                    "seen_version": str(f["version"]), "decision": "confirm", "reason": "ok"}
            r = _client.post("/app/brain/confirm", data=body, headers=_auth(),
                             follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/brain?done=confirm"))
            r = _client.post("/app/brain/confirm", data=body, headers=_auth(),
                             follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/brain?done=dup"))
            self.assertEqual(len(run(ledger.list_confirmations(f["fact_id"]))), 1)
            # 新版が来た後、古い画面（seen_version=1）の操作 → 409 で最新
            self.fake.data["40"] = [app40(1, 2, "2026-09-20T02:00:00Z", status="受理")]
            run(sync.sync_target(sync.TARGET_APP40))
            body["operation_id"] = OP2
            r = _client.post("/app/brain/confirm", data=body, headers=_auth(),
                             follow_redirects=False)
            self.assertEqual(r.status_code, 409)
            cur = r.json()["current"]
            self.assertEqual((r.json()["error"], cur["version"], cur["is_current"]),
                             ("version_conflict", 2, True))
            self.assertEqual(len(run(ledger.list_confirmations(f["fact_id"]))), 1)
            # API は再確認一覧に出す
            rk = _client.get("/app/api/brain/recheck", headers=_auth(), follow_redirects=False)
            self.assertEqual(rk.status_code, 200)
            self.assertEqual([x["reasons"] for x in rk.json()["records"]], [["superseded"]])
            facts = _client.get("/app/api/brain/facts?app=40&record=1", headers=_auth(),
                                follow_redirects=False).json()
            self.assertEqual(facts["freshness"], "synced")
            self.assertTrue(any(x["item_code"] == "app40.status" and x["version"] == 2
                                for x in facts["facts"]))

    def test_invalid_inputs_fixed_400(self):
        f = self._seed()
        with patch.dict(os.environ, _ENV):
            good = {"operation_id": OP1, "fact_id": str(f["fact_id"]),
                    "seen_version": str(f["version"]), "decision": "confirm", "reason": ""}
            for bad in ({"operation_id": "not-a-uuid"}, {"fact_id": "x"}, {"seen_version": ""},
                        {"decision": "approve"}, {"reason": "x" * 501},
                        {"revoked_of": "1"}, {"decision": "revoke"}):
                r = _client.post("/app/brain/confirm", data={**good, **bad}, headers=_auth(),
                                 follow_redirects=False)
                self.assertEqual(r.status_code, 400, bad)
                self.assertEqual(r.text, "")                       # 反射なし
            r = _client.post("/app/brain/relink", data={"operation_id": OP1, "source_app_id": "30",
                                                        "source_record_id": "5", "case_app_id": "40",
                                                        "case_record_id": "a"},
                             headers=_auth(), follow_redirects=False)
            self.assertEqual(r.status_code, 400)
            for path in ("/app/api/brain/pending?limit=0", "/app/api/brain/holds?limit=x",
                         "/app/api/brain/facts?app=40&record=%3Cb%3E"):
                r = _client.get(path, headers=_auth(), follow_redirects=False)
                self.assertEqual(r.status_code, 400, path)
            self.assertEqual(len(run(ledger.list_confirmations(f["fact_id"]))), 0)

    def test_relink_via_api_is_history_only(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1)]
        self.fake.data["30"] = [app30(5, 1)]
        run(sync.sync_target(sync.TARGET_APP40))
        run(sync.sync_target(sync.TARGET_APP30))
        with patch.dict(os.environ, _ENV):
            # BA-06: 画面が見ていた紐付け版・出典 revision を添える（期待値は不変）
            body = {"operation_id": OP1, "source_app_id": "30", "source_record_id": "5",
                    "case_app_id": "40", "case_record_id": "2", "reason": "誤り",
                    "seen_link_version": str(run(ledger.link_version("30", "5"))),
                    "seen_source_revision": "1"}
            r = _client.post("/app/brain/relink", data=body, headers=_auth(), follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/brain?done=relink"))
            r = _client.post("/app/brain/relink", data=body, headers=_auth(), follow_redirects=False)
            self.assertEqual(r.headers["location"], "/app/brain?done=dup")
        self.assertEqual(run(ledger.current_case_of_source("30", "5")), ("40", "2"))
        self.assertEqual(run(ledger.latest_link("30", "5"))["actor"], "owner")

    # ── fix1: BA-06（紐付け訂正の版照合・訂正先検査）/ BA-07（鮮度の集約・flag） ──

    def test_relink_versioning_and_target_validation(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1)]
        self.fake.data["30"] = [app30(61, 1, 案件レコードID="999")]      # 参照先不在 → 紐付け待ち
        run(sync.sync_target(sync.TARGET_APP40))
        run(sync.sync_target(sync.TARGET_APP30))

        def post(body):
            return _client.post("/app/brain/relink", data=body, headers=_auth(), follow_redirects=False)
        with patch.dict(os.environ, _ENV):
            pending = _client.get("/app/api/brain/pending", headers=_auth(),
                                  follow_redirects=False).json()["records"]
            self.assertEqual((pending[0]["source_record_id"], pending[0]["source_revision"]), ("61", 1))
            seen = {"seen_link_version": str(pending[0]["link_version"]),
                    "seen_source_revision": str(pending[0]["source_revision"])}
            base = {"source_app_id": "30", "source_record_id": "61", "case_app_id": "40",
                    "case_record_id": "2", "reason": "手動"}
            # 版の欠落は 400
            self.assertEqual(post({**base, "operation_id": OP1}).status_code, 400)
            # 出典が rev2 に進んだ後の古い画面の訂正 → 409 で最新
            self.fake.data["30"] = [app30(61, 2, "2026-09-21T01:00:00Z", 案件レコードID="999")]
            run(sync.sync_target(sync.TARGET_APP30))
            r = post({**base, **seen, "operation_id": OP1})
            self.assertEqual(r.status_code, 409)
            self.assertEqual(r.json(), {"error": "version_conflict",
                                        "current": {"link_version": int(seen["seen_link_version"]),
                                                    "source_revision": 2}})
            self.assertIsNone(run(ledger.current_case_of_source("30", "61")))
            seen["seen_source_revision"] = "2"
            # App 26/No.999 → 400・App 40/No.999（台帳に無い）→ 400
            for bad in ({"case_app_id": "26", "case_record_id": "999"}, {"case_record_id": "999"}):
                r = post({**base, **seen, **bad, "operation_id": OP2})
                self.assertEqual((r.status_code, r.text), (400, ""), bad)
            # 台帳にはあるが正本に無い → 400・正本の確認不能 → 503（訂正を通さない）
            saved = self.fake.data["40"]
            self.fake.data["40"] = [app40(1, 1)]
            self.assertEqual(post({**base, **seen, "operation_id": OP2}).status_code, 400)
            self.fake.data["40"] = saved
            self.fake.raise_all = True
            r = post({**base, **seen, "operation_id": OP2})
            self.assertEqual((r.status_code, r.json()), (503, {"error": "source_unavailable"}))
            self.fake.raise_all = False
            self.assertEqual(len(run(ledger.list_pending_links())), 1)     # 何も変わっていない
            # 正しい版で成功 → 303・履歴のみ・紐付け待ちから消え、次回同期で新案件側へ
            r = post({**base, **seen, "operation_id": OP2})
            self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/brain?done=relink"))
            self.assertEqual(run(ledger.current_case_of_source("30", "61")), ("40", "2"))
            self.assertEqual(run(ledger.list_pending_links()), [])
            run(sync.sync_target(sync.TARGET_APP30))
            self.assertTrue([f for f in run(ledger.list_case_facts("40", "2"))
                             if f["source_app_id"] == "30"])
            # 台帳レベル: 紐付け版の不一致も同一トランザクションの照合で 409
            with self.assertRaises(ledger.VersionConflict) as cm:
                run(ledger.relink_source(source_app_id="30", source_record_id="61",
                                         new_case=("40", "1"), reason="", operation_id=OP3,
                                         actor="owner", seen_link_version=0, seen_source_revision=2))
            self.assertEqual(cm.exception.current["source_revision"], 2)

    def test_freshness_aggregates_linked_sources_and_flags_unavailable(self):
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["30"] = [app30(5, 1)]
        run(sync.sync_target(sync.TARGET_APP40))
        run(sync.sync_target(sync.TARGET_APP30))

        def facts():
            return _client.get("/app/api/brain/facts?app=40&record=1", headers=_auth(),
                               follow_redirects=False).json()

        def confirm(op, f, decision, **extra):
            return _client.post("/app/brain/confirm", headers=_auth(), follow_redirects=False,
                                data={"operation_id": op, "fact_id": str(f["fact_id"]),
                                      "seen_version": str(f["version"]), "decision": decision, **extra})
        with patch.dict(os.environ, _ENV):
            d = facts()
            self.assertEqual((d["freshness"], d["freshness_reasons"]), ("synced", []))
            ship = [f for f in d["facts"] if f["source_app_id"] == "30"
                    and f["item_code"] == "app30.件名"][0]
            self.assertIsNone(ship["flag"])
            self.assertEqual(confirm(OP1, ship, "confirm").status_code, 303)
            # App 30 の出典が確認不能に → 案件は synced でなく partial（理由付き）
            self.fake.data["30"] = []
            run(sync.recheck_target(sync.TARGET_APP30))
            d = facts()
            self.assertEqual((d["freshness"], d["freshness_reasons"]),
                             ("partial", ["app30:5:source_unavailable"]))
            self.assertTrue(all(f["flag"] == "source_unavailable" for f in d["facts"]
                                if f["source_app_id"] == "30"))
            self.assertTrue(all(f["flag"] is None for f in d["facts"] if f["source_app_id"] == "40"))
            # 確認/却下の対象外（409・行は増えない）・撤回は可
            for op, decision in ((OP2, "confirm"), (OP3, "reject")):
                r = confirm(op, ship, decision)
                self.assertEqual((r.status_code, r.json()), (409, {"error": "source_unavailable"}))
            confs = run(ledger.list_confirmations(ship["fact_id"]))
            self.assertEqual(len(confs), 1)
            r = confirm(OP4, ship, "revoke", revoked_of=str(confs[0]["confirmation_id"]))
            self.assertEqual(r.status_code, 303)
            self.assertEqual(run(ledger.list_conflicts()), [])
            self.assertEqual(run(ledger.case_freshness("40", "1", "app40", brain_sync_targets())),
                             "partial")
            # 案件自身のカーソルが error なら旧 confirmed_until で synced を返さない
            run(ledger.set_cursor_state("app40", "error"))
            d = facts()
            self.assertEqual((d["freshness"], d["freshness_reasons"]), ("incomplete", ["cursor_error"]))
            # 復帰
            run(ledger.set_cursor_state("app40", "synced"))
            self.fake.data["30"] = [app30(5, 1)]
            run(sync.recheck_target(sync.TARGET_APP30))
            d = facts()
            self.assertEqual((d["freshness"], d["freshness_reasons"]), ("synced", []))
            self.assertTrue(all(f["flag"] is None for f in d["facts"]))


def brain_sync_targets():
    return sync.source_targets()


class TestPiiAndSinkPolicy(unittest.TestCase):
    def test_brain_modules_have_zero_sink_violations_and_no_allowlist_entries(self):
        for name in BRAIN_FILES:
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        self.assertNotIn("brain", allow)

    def test_ledger_link_view_do_not_import_logging(self):
        for name in ("hub/brain_ledger.py", "hub/brain_link.py", "hub/webapp_brain_view.py"):
            tree = ast.parse((REPO / name).read_text(encoding="utf-8"))
            imported = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(a.name.split(".")[0] for a in node.names)
                elif isinstance(node, ast.ImportFrom):
                    imported.add((node.module or "").split(".")[0])
            self.assertNotIn("logging", imported, name)
            self.assertNotIn("httpx", imported, name)

    def test_view_module_readonly_ast_and_forbidden_imports(self):
        # 本 module は kintone を一切使わない（読取も台帳経由）: 名前の出現ゼロ・
        # 禁止名（getattr/setattr/eval/exec/import_module/attrgetter）ゼロ。
        # （_readonly_violations は「kintone 束縛ちょうど 1 回」を要求するため、
        #  kintone 非使用の本 module には文脈検査のみを適用する）
        tree = ast.parse(Path(bv.__file__).read_text(encoding="utf-8"))
        self.assertEqual(_context_allowlist_violations(tree), [])
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        self.assertNotIn("kintone", names)
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        self.assertFalse(imported & _FORBIDDEN_IMPORTS, imported)
        self.assertNotIn("kintone", imported)

    def test_sync_logs_contain_no_pii(self):
        # 実ログ: 氏名・LINE ID・値・locator の中身が出ない（件数と固定語彙のみ）
        class _Case(BrainDbMixin):
            def runTest(self):
                pass
        t = _Case()
        t.setUp()
        try:
            t.fake.data["40"] = [app40(1, 1, 顧客名="機密太郎")]
            t.fake.data["30"] = [app30(5, 1, 宛先名="機密社")]
            t.fake.data["28"] = [app28(100, 1, message="機密メッセージ")]
            with self.assertLogs("hub.brain_sync", level="INFO") as cm:
                run(sync.run_brain_sync_job())
                t.fake.raise_all = True
                run(sync.sync_target(sync.TARGET_APP40))
            text = "\n".join(cm.output)
            for secret in ("機密太郎", "機密社", "機密メッセージ", LINE_A, "status/-/-/-/-"):
                self.assertNotIn(secret, text)
            self.assertIn("[BRAIN_SYNC] app40 run ok", text)
            self.assertIn("[BRAIN_SYNC] run failed (kintone_fetch_failed)", text)
        finally:
            t.tearDown()


class TestIsolationAndZeroWrites(unittest.TestCase):
    def test_disabled_flag_skips_everything(self):
        with patch.dict(os.environ, {"BRAIN_SYNC_ENABLED": ""}), \
                patch.object(hub_kintone, "search_records", AsyncMock()) as sr, \
                patch.object(ledger, "get_cursor", AsyncMock()) as gc:
            sync._disabled_logged.clear()
            with self.assertLogs("hub.brain_sync", level="INFO") as cm:
                out = run(sync.run_brain_sync_job())
            self.assertEqual(out, {"status": "disabled"})
            self.assertEqual(cm.output, ["INFO:hub.brain_sync:[BRAIN_SYNC] disabled "
                                         "(BRAIN_SYNC_ENABLED off)"])
            sr.assert_not_awaited()
            gc.assert_not_awaited()
        for v in ("1", "true", "on", "yes"):
            with patch.dict(os.environ, {"BRAIN_SYNC_ENABLED": v}):
                self.assertTrue(sync.brain_sync_enabled())
        with patch.dict(os.environ, {"BRAIN_SYNC_ENABLED": "0"}):
            self.assertFalse(sync.brain_sync_enabled())
        self.assertEqual(config.BRAIN_SYNC_ENABLED_ENV, "BRAIN_SYNC_ENABLED")
        self.assertNotIn("os.environ", Path("config.py").read_text(encoding="utf-8")
                         .split("BRAIN_SYNC_ENABLED_ENV")[1][:200])

    def test_job_registered_interval_5min(self):
        self.assertTrue(hub_scheduler.is_registered(sync.JOB_NAME))
        job = hub_scheduler._jobs[sync.JOB_NAME]
        self.assertEqual((job.kind, job.minutes), ("interval", 5.0))

    def test_db_failure_is_contained_and_business_routes_unaffected(self):
        env = {**BRAIN_ENV, "BRAIN_SYNC_ENABLED": "1",
               "DATABASE_URL": "sqlite+aiosqlite:////nonexistent_dir_brain/x/y.db", **_ENV}
        with patch.dict(os.environ, env):
            from hub import db
            db.reset_for_tests()
            try:
                with patch.object(hub_kintone, "search_records",
                                  AsyncMock(return_value=[app40(1, 1)])):
                    out = run(sync.run_brain_sync_job())          # 例外を外へ出さない
                self.assertEqual(out["status"], "degraded")
                # 確認ビューは固定 503（PII なし）・既存 route は生きている
                r = _client.get("/app/api/brain/overview", headers=_auth(), follow_redirects=False)
                self.assertEqual((r.status_code, r.json()), (503, {"error": "ledger_unavailable"}))
                r = _client.post("/app/brain/confirm", headers=_auth(), follow_redirects=False,
                                 data={"operation_id": OP1, "fact_id": "1", "seen_version": "1",
                                       "decision": "confirm"})
                self.assertEqual(r.status_code, 503)
                # LINE webhook（署名不正は既存どおりの拒否・DB 障害由来の 500 ではない）
                r = _client.post("/webhook", content=b"{}", headers={"X-Line-Signature": "bad"})
                self.assertIn(r.status_code, (400, 401, 403))
                # 承認参照（kintone mock）は通常どおり
                with patch.object(hub_kintone, "search_records", AsyncMock(return_value=[])):
                    r = _client.get("/app/api/approvals", headers=_auth(), follow_redirects=False)
                    self.assertEqual(r.status_code, 200)
                # 発送の入口（hub/dispatch の webhook）は認証段階で応答し DB に依存しない
                r = _client.post("/hub/dispatch", json={})
                self.assertIn(r.status_code, (400, 401, 403, 404, 422))
            finally:
                db.reset_for_tests()

    def test_no_kintone_writes_or_notifications_during_sync_and_ops(self):
        class _Case(BrainDbMixin):
            def runTest(self):
                pass
        t = _Case()
        t.setUp()
        try:
            t.fake.data["40"] = [app40(1, 1), app40(2, 1)]
            t.fake.data["30"] = [app30(5, 1), app30(6, 1, ユニット種別="時効援用")]
            t.fake.data["28"] = [app28(100, 1)]
            writes = {name: AsyncMock() for name in (
                "create_record", "create_records", "update_record", "delete_record",
                "upload_file")}
            notifies = {name: AsyncMock() for name in (
                "notify_admin_line", "notify_business", "notify_admin_line_result")}
            with patch.multiple(hub_kintone, **writes), patch.multiple(hub_notify, **notifies):
                run(sync.run_brain_sync_job())
                t.fake.data["40"] = [app40(1, 2, "2026-09-20T02:00:00Z", status="受理"), app40(2, 1)]
                run(sync.run_brain_sync_job())
                run(sync.reconcile_target(sync.TARGET_APP40))
                run(sync.recheck_target(sync.TARGET_APP30))
                f = [x for x in run(ledger.list_case_facts("40", "1"))
                     if x["item_code"] == "app40.status"][0]
                with patch.dict(os.environ, _ENV):
                    _client.post("/app/brain/confirm", headers=_auth(), follow_redirects=False,
                                 data={"operation_id": OP1, "fact_id": str(f["fact_id"]),
                                       "seen_version": str(f["version"]), "decision": "confirm"})
                    r = _client.post("/app/brain/relink", headers=_auth(), follow_redirects=False,
                                     data={"operation_id": OP2, "source_app_id": "30",
                                           "source_record_id": "5", "case_app_id": "40",
                                           "case_record_id": "2",
                                           "seen_link_version": str(run(ledger.link_version("30", "5"))),
                                           "seen_source_revision": "1"})
                    self.assertEqual(r.status_code, 303)      # 訂正は実行された（読取のみ）
            for name, m in {**writes, **notifies}.items():
                m.assert_not_awaited()
            # 偽 kintone は読取 API しか持たない（書込は AttributeError で落ちる構造）
            self.assertFalse(hasattr(t.fake, "update_record"))
        finally:
            t.tearDown()

    def test_main_py_change_is_tail_append_only(self):
        src = (REPO / "main.py").read_text(encoding="utf-8")
        tail = src[src.index("# ── BRAIN-A1-LEDGER-1"):]
        self.assertIn("include_before_catch_all(app, brain_view_router)", tail)
        self.assertIn("register_brain_sync_job()", tail)
        self.assertEqual(src.count("brain_"), tail.count("brain_"))   # 本文中の他所に無い
        self.assertNotIn("brain", src[:src.index("# ── BRAIN-A1-LEDGER-1")])

    def test_brain_html_uses_closed_paths_and_no_raw_network(self):
        src = (REPO / "webapp" / "brain.html").read_text(encoding="utf-8")
        actions = set(re.findall(r'"(/app/brain/[a-z]+)"', src))
        self.assertEqual(actions, {"/app/brain/confirm", "/app/brain/relink"})
        for m in re.finditer(r'app_fetch\(\s*"([^"]*)"', src):
            self.assertTrue(m.group(1).startswith("/app/api/brain/"), m.group(1))
        for banned in ("innerHTML", "XMLHttpRequest", "WebSocket", "eval("):
            self.assertNotIn(banned, src)
        self.assertIn("crypto.randomUUID()", src)          # 操作 ID はクライアント生成 UUID


if __name__ == "__main__":
    unittest.main()
