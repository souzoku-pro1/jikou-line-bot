"""HOUKI-KOSEKI-CHECKLIST-1（QUEUE-2026-10-12-C C-4）: 受任時の必要戸籍チェックリスト初期行と連続戸籍充足の候補通知

固定する仕様:
- 写像表: 続柄（App 40 実測値）→ 共通行 4（除票・委任状・印紙・郵券）+ 続柄別の行（10 §2）。順位 2/3・代襲は
  「その他」+対象者欄。先順位放棄フラグ・続柄が閉集合外は共通行のみ+「個別確定が必要」通知。
- 冪等: 既存行が 1 行でもあれば追加しない。受任系以外の status は書かない。CAS 409 は再取得して空なら 1 回再試行。
- スキーマ検査: 列・選択肢が想定と異なれば書かず通知（fail-closed）。書込は 書類チェック 1 欄のみ。
- 候補通知: 戸籍系の行がすべて受領（1 行以上受領）かつ 連続戸籍充足≠yes のとき 1 回（送信台帳キー・2 回目は duplicate）。
  連続戸籍充足 は書かない（弁護士のみ）。
- 受け口 /souzoku-houki/koseki-checklist/{secret}: 認証は申述書と同じ（404/403・app 一致）。
- 走査ジョブ HOUKI_KOSEKI_CHECKLIST（interval・HOUKI_KOSEKI_CHECKLIST_DISABLED=1 で停止）。
"""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "doc-secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d", "APP_HOUKI": "40", "TOKEN_HOUKI": "d",
    "HOUKI_LINE_CHANNEL_SECRET": "houki_secret", "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki_token",
    "HOUKI_WEBHOOK_TOKEN": "houki-hook",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import houki_koseki_webhook as kw  # noqa: E402
import main  # noqa: E402
from hub import db  # noqa: E402
from hub import houki_koseki_checklist as kc  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub.kintone import KintoneConflict, KintoneError  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
_URL = "/souzoku-houki/koseki-checklist/houki-hook"
ADMIN = "U" + "d" * 32


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


def _rec(**fields):
    out = {k: {"value": v} for k, v in fields.items()}
    out.setdefault("$id", {"value": "12"})
    out.setdefault("$revision", {"value": "5"})
    out.setdefault("status", {"value": "受任"})
    return out


def _row(name, target="", state="未"):
    return {"value": {"書類名": {"value": name}, "対象者": {"value": target},
                      "取得方法": {"value": "職務上請求"}, "書類状態": {"value": state}}}


def _props(**over):
    def opts(vals):
        return {v: {"label": v, "index": str(i)} for i, v in enumerate(vals)}
    fields = {"書類名": {"type": "DROP_DOWN", "options": opts(kc.DOC_OPTIONS)},
              "対象者": {"type": "SINGLE_LINE_TEXT"},
              "取得方法": {"type": "DROP_DOWN", "options": opts(kc.METHOD_OPTIONS)},
              "書類状態": {"type": "DROP_DOWN", "options": opts(kc.STATE_OPTIONS)}}
    fields.update(over)
    return {"書類チェック": {"type": "SUBTABLE", "fields": fields}}


# ── 1. 写像表（純関数） ───────────────────────────────────────────────────────
class TestMatrix(unittest.TestCase):
    def test_common_rows_first_and_per_kankei(self):
        for kankei, extra in (("子", 2), ("配偶者", 2), ("孫", 3), ("直系尊属（父母・祖父母）", 2),
                              ("兄弟姉妹", 3), ("おいめい", 4)):
            with self.subTest(kankei=kankei):
                rows, individual = kc.plan_rows(_rec(続柄=kankei))
                self.assertFalse(individual)
                self.assertEqual(rows[:4], list(kc.COMMON_ROWS))
                self.assertEqual(len(rows), 4 + extra)
                self.assertEqual(rows[0][0], kc.DOC_JOHYO)                       # 除票が最優先
                for name, _t, method in rows:
                    self.assertIn(name, kc.DOC_OPTIONS)
                    self.assertIn(method, kc.METHOD_OPTIONS)
        rows, _ = kc.plan_rows(_rec(続柄="兄弟姉妹"))
        others = [t for n, t, _m in rows if n == kc.DOC_OTHER]
        self.assertEqual(others, [kc.TARGET_CONTINUOUS, kc.TARGET_PARENTS_DEATH])   # 順位 3 は「その他」+対象者
        rows, _ = kc.plan_rows(_rec(続柄="おいめい"))
        self.assertIn(kc.TARGET_SIBLING_DEATH, [t for n, t, _m in rows])             # 代襲

    def test_individual_confirmation_cases(self):
        for rec in (_rec(続柄="その他"), _rec(続柄=""), _rec(続柄="不明"),
                    _rec(続柄="子", 複雑性フラグ=["先順位放棄"]), _rec(続柄="子", 複雑性フラグ="先順位放棄")):
            with self.subTest(rec=rec.get("続柄")):
                rows, individual = kc.plan_rows(rec)
                self.assertTrue(individual)
                self.assertEqual(rows, list(kc.COMMON_ROWS))

    def test_subtable_shape_and_initial_state(self):
        rows = kc.to_subtable(kc.plan_rows(_rec(続柄="子"))[0])
        self.assertEqual(set(rows[0]["value"]), {"書類名", "対象者", "取得方法", "書類状態"})
        self.assertTrue(all(r["value"]["書類状態"]["value"] == "未" for r in rows))

    def test_schema_check(self):
        self.assertTrue(kc.schema_ok(_props()))
        self.assertFalse(kc.schema_ok({}))
        self.assertFalse(kc.schema_ok({"書類チェック": {"type": "SINGLE_LINE_TEXT"}}))
        bad = _props()
        del bad["書類チェック"]["fields"]["対象者"]
        self.assertFalse(kc.schema_ok(bad))
        bad = _props(書類名={"type": "DROP_DOWN", "options": {"その他": {}}})
        self.assertFalse(kc.schema_ok(bad))

    def test_candidate_truth_table(self):
        rows_all = [_row(kc.DOC_JOHYO, state="受領"), _row(kc.DOC_DEATH_KOSEKI, state="受領"),
                    _row(kc.DOC_APPLICANT_KOSEKI, state="受領"), _row(kc.DOC_POA, state="未")]
        self.assertTrue(kc.continuous_candidate(_rec(書類チェック=rows_all, 連続戸籍充足="no")))
        self.assertFalse(kc.continuous_candidate(_rec(書類チェック=rows_all, 連続戸籍充足="yes")))
        rows_partial = rows_all[:2] + [_row(kc.DOC_APPLICANT_KOSEKI, state="請求中")]
        self.assertFalse(kc.continuous_candidate(_rec(書類チェック=rows_partial)))
        rows_other = [_row(kc.DOC_OTHER, kc.TARGET_CONTINUOUS, "受領"), _row(kc.DOC_APPLICANT_KOSEKI, state="不要")]
        self.assertTrue(kc.continuous_candidate(_rec(書類チェック=rows_other)))
        self.assertFalse(kc.continuous_candidate(_rec(書類チェック=[_row(kc.DOC_POA, state="受領")])))
        self.assertFalse(kc.continuous_candidate(_rec(書類チェック=[])))
        self.assertFalse(kc.continuous_candidate(_rec(書類チェック=[_row(kc.DOC_APPLICANT_KOSEKI, state="不要")])))


# ── 2. 初期化（冪等・CAS・スキーマ fail-closed・保護欄） ───────────────────────
class _KintoneMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="koseki_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db",
                                            "LINE_ADMIN_USER_ID": ADMIN,
                                            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz"})
        self._env.start()
        os.environ.pop(kc.DISABLE_ENV, None)
        db.reset_for_tests()
        kc.reset_schema_cache()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(sl.metadata.create_all)
        _run(_create())
        self.get = AsyncMock(return_value=_rec(続柄="子"))
        self.update = AsyncMock()
        self.fields = AsyncMock(return_value=_props())
        self.search = AsyncMock(return_value=[])
        self.push = AsyncMock(return_value=True)
        self.admin = AsyncMock(return_value=True)
        for p in (patch.object(kc.kintone, "get_record", self.get),
                  patch.object(kc.kintone, "update_record", self.update),
                  patch.object(kc.kintone, "get_form_fields", self.fields),
                  patch.object(kc.kintone, "search_records", self.search),
                  patch.object(kc, "_push_admin_http", self.push),
                  patch("hub.notify.notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)


class TestInit(_KintoneMixin):
    def test_creates_once_with_only_subtable_written(self):
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_CREATED)
        self.update.assert_awaited_once()
        args = self.update.await_args
        self.assertEqual(set(args.args[2]), {"書類チェック"})
        self.assertEqual(len(args.args[2]["書類チェック"]), 6)
        self.assertEqual(args.kwargs.get("revision"), "5")
        self.push.assert_not_awaited()
        # 既存行があれば追加しない
        self.get.return_value = _rec(続柄="子", 書類チェック=[_row(kc.DOC_JOHYO)])
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_EXISTS)
        self.update.assert_awaited_once()

    def test_not_engaged_and_disabled_do_not_write(self):
        self.get.return_value = _rec(続柄="子", status="問い合わせ")
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_NOT_ENGAGED)
        self.update.assert_not_awaited()
        with patch.dict(os.environ, {kc.DISABLE_ENV: "1"}):
            self.assertEqual(_run(kc.scan_once()), {"skipped": "disabled"})

    def test_individual_case_writes_common_rows_and_notifies_once(self):
        self.get.return_value = _rec(続柄="子", 複雑性フラグ=["先順位放棄"])
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_INDIVIDUAL)
        self.assertEqual(len(self.update.await_args.args[2]["書類チェック"]), 4)
        self.assertEqual(self.push.await_count, 1)
        self.assertIn("No.12", self.push.await_args.args[1])
        self.assertEqual(_run(kc.send_notice("koseki_individual:12", "x")), "duplicate")   # 同じキーは 1 通

    def test_schema_mismatch_is_fail_closed_with_one_admin_notice(self):
        self.fields.return_value = {"書類チェック": {"type": "SUBTABLE", "fields": {}}}
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_SCHEMA_MISMATCH)
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_SCHEMA_MISMATCH)
        self.update.assert_not_awaited()
        self.assertEqual(self.fields.await_count, 1)               # 検査結果は再利用
        self.assertEqual(self.admin.await_count, 1)
        self.fields.side_effect = RuntimeError("down")
        kc.reset_schema_cache()
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_SCHEMA_MISMATCH)

    def test_cas_conflict_refetches_and_respects_existing_rows(self):
        self.update.side_effect = [KintoneConflict(409, "GAIA_CO02", "conflict"), None]
        self.get.side_effect = [_rec(続柄="子"), _rec(続柄="子", **{"$revision": "6"})]
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_CREATED)
        self.assertEqual(self.update.await_count, 2)
        self.assertEqual(self.update.await_args.kwargs.get("revision"), "6")
        self.update.reset_mock()
        self.update.side_effect = [KintoneConflict(409, "GAIA_CO02", "conflict")]
        self.get.side_effect = [_rec(続柄="子"), _rec(続柄="子", 書類チェック=[_row(kc.DOC_JOHYO)])]
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_EXISTS)
        self.update.side_effect = KintoneConflict(409, "GAIA_CO02", "conflict")
        self.get.side_effect = None
        self.get.return_value = _rec(続柄="子")
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_CAS_LOST)

    def test_write_failure_is_reported_not_raised(self):
        self.update.side_effect = KintoneError(500, "GAIA_XX", "down")
        self.assertEqual(_run(kc.init_checklist("12")), kc.RESULT_FAILED)


class TestCandidateAndScan(_KintoneMixin):
    def test_candidate_notified_once_without_writing_declaration(self):
        rec = _rec(続柄="子", 書類チェック=[_row(kc.DOC_JOHYO, state="受領"), _row(kc.DOC_DEATH_KOSEKI, state="受領"),
                                        _row(kc.DOC_APPLICANT_KOSEKI, state="受領")], 連続戸籍充足="no")
        self.assertEqual(_run(kc.check_candidate("12", rec)), "sent")
        self.assertEqual(_run(kc.check_candidate("12", rec)), "duplicate")
        self.assertEqual(self.push.await_count, 1)
        text = self.push.await_args.args[1]
        self.assertIn("連続戸籍充足の候補", text)
        self.assertIn("No.12", text)
        self.update.assert_not_awaited()

    def test_scan_counts_and_process_record_paths(self):
        self.search.return_value = [
            _rec(続柄="子", **{"$id": "1"}),
            _rec(続柄="", **{"$id": "2"}),
            _rec(続柄="子", **{"$id": "3"}, 書類チェック=[_row(kc.DOC_DEATH_KOSEKI, state="受領"),
                                                  _row(kc.DOC_APPLICANT_KOSEKI, state="受領")]),
            _rec(続柄="子", **{"$id": "4"}, 書類チェック=[_row(kc.DOC_DEATH_KOSEKI, state="未")]),
        ]
        counts = _run(kc.scan_once())
        self.assertEqual(counts, {"records": 4, "created": 1, "individual": 1, "candidate": 1})
        self.assertEqual(self.update.await_count, 2)
        self.assertEqual(self.push.await_count, 2)              # 個別確定 1 + 候補 1
        q = self.search.await_args.args[1]
        self.assertIn('status in ("受任", "書類収集中", "申述書作成")', q)
        counts = _run(kc.scan_once())
        self.assertEqual(counts["candidate"], 0)                 # 2 周目は duplicate

    def test_scan_pages_past_the_first_100_records(self):
        """PRE-REVIEW-1 #284-1: 受任系が 100 件を超えても $id > 最後の id で継続取得し、101 件目以降も走査する。"""
        page1 = [_rec(続柄="子", **{"$id": str(i)}, 書類チェック=[_row(kc.DOC_DEATH_KOSEKI, state="未")])
                 for i in range(1, kc.SCAN_LIMIT + 1)]
        page2 = [_rec(続柄="", **{"$id": str(kc.SCAN_LIMIT + 1)})]      # 101 件目: 続柄なし → 個別確定
        self.search.side_effect = [page1, page2]
        counts = _run(kc.scan_once())
        self.assertEqual(counts["records"], kc.SCAN_LIMIT + 1)
        self.assertEqual(counts["individual"], 1)
        self.assertEqual(self.search.await_count, 2)
        q1, q2 = (c.args[1] for c in self.search.await_args_list)
        self.assertNotIn("$id >", q1)
        self.assertIn(f"$id > {kc.SCAN_LIMIT}", q2)
        self.assertIn(f"limit {kc.SCAN_LIMIT}", q2)
        # id が進まない応答では打ち切る（無限ループしない）
        self.search.side_effect = [page1, page1]
        counts = _run(kc.scan_once())
        self.assertEqual(counts["records"], kc.SCAN_LIMIT * 2)
        self.assertEqual(self.search.await_count, 4)

    def test_scan_unconfigured_or_fetch_failure_is_silent(self):
        with patch.dict(os.environ, {"APP_HOUKI": "", "TOKEN_HOUKI": ""}):
            self.assertEqual(_run(kc.scan_once()), {"skipped": "unconfigured"})
        self.search.side_effect = RuntimeError("down")
        self.assertEqual(_run(kc.scan_once()), {"skipped": "fetch_failed"})

    def test_job_registered_once_as_interval(self):
        hub_scheduler._jobs.pop(kc.SCAN_JOB_NAME, None)
        kc.register_houki_koseki_job()
        kc.register_houki_koseki_job()
        job = hub_scheduler._jobs[kc.SCAN_JOB_NAME]
        self.assertEqual((job.kind, job.minutes), ("interval", kc.SCAN_INTERVAL_MINUTES))


# ── 3. 受け口 ──────────────────────────────────────────────────────────────────
class TestWebhook(_KintoneMixin):
    def test_auth_and_shape(self):
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": ""}):
            self.assertEqual(_client.post(_URL, json={"record_id": "12"}).status_code, 404)
        with patch.dict(os.environ, {"HOUKI_WEBHOOK_TOKEN": os.environ["KINTONE_WEBHOOK_TOKEN"]}):
            self.assertEqual(_client.post("/souzoku-houki/koseki-checklist/kintone-token",
                                          json={"record_id": "12"}).status_code, 404)
        self.assertEqual(_client.post("/souzoku-houki/koseki-checklist/wrong", json={"record_id": "12"}).status_code, 403)
        r = _client.post(_URL, json={"app": {"id": "21"}, "record": {"$id": {"value": "12"}}})
        self.assertEqual(r.json().get("skip"), "app_mismatch")
        r = _client.post(_URL, json={"foo": "bar"})
        self.assertEqual(r.json().get("skip"), "no_record_id")
        self.get.assert_not_awaited()
        r = _client.post(_URL, json={"app": {"id": "40"}, "record": {"$id": {"value": "12"}}})
        self.assertEqual((r.status_code, r.json().get("init")), (200, kc.RESULT_CREATED))
        r = _client.post(_URL, json={"record_id": "12"})
        self.assertEqual(r.json().get("init"), kc.RESULT_CREATED)   # 2 回目は fake の record が空のまま=再生成（冪等は実レコードの既存行で担保）

    def test_disabled_flag_skips(self):
        with patch.dict(os.environ, {kc.DISABLE_ENV: "1"}):
            r = _client.post(_URL, json={"record_id": "12"})
        self.assertEqual(r.json().get("skip"), "disabled")
        self.update.assert_not_awaited()


# ── 4. pin ────────────────────────────────────────────────────────────────────
class TestPins(unittest.TestCase):
    def test_closed_sets(self):
        self.assertEqual(kc.DOC_OPTIONS, ("被相続人死亡記載戸籍", "被相続人住民票除票", "申述人戸籍", "委任状",
                                          "収入印紙", "切手", "その他"))
        self.assertEqual(kc.METHOD_OPTIONS, ("依頼者郵送", "職務上請求", "事務所手配"))
        self.assertEqual(kc.STATE_OPTIONS, ("未", "依頼中", "発送済", "請求中", "受領", "不要"))
        self.assertEqual(kc.ENGAGED_STATUSES, ("受任", "書類収集中", "申述書作成"))
        from hub import houki_shinjutsu
        self.assertTrue(set(kc.KOSEKI_MATRIX) <= set(houki_shinjutsu.KANKEI_MAP))   # 続柄の語彙は申述書と同じ

    def test_wiring_and_sink(self):
        for name in ("hub/houki_koseki_checklist.py", "houki_koseki_webhook.py"):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        self.assertNotIn("houki_koseki", allow)
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        # HOUKI-E2E-1（統合）: 後続の末尾追記が並ぶため、窓を末尾追記ブロック全体（check_timing_config 以降）へ広げる（結線の存在の pin は不変）
        self.assertIn("app.include_router(houki_koseki_router)", "\n".join(src[src.index("send_ledger.check_timing_config()"):]))
        self.assertIn("register_houki_koseki_job()", "\n".join(src[src.index("send_ledger.check_timing_config()"):]))   # 同上
        # 弁護士専権欄・status は書かない
        msrc = (REPO / "hub" / "houki_koseki_checklist.py").read_text(encoding="utf-8")
        self.assertEqual(msrc.count("update_record("), 1)
        self.assertNotIn('"連続戸籍充足": {"value"', msrc)


if __name__ == "__main__":
    unittest.main()
