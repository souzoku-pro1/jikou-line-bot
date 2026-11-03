"""JIKOU-FOLLOWUP-1（QUEUE-2026-10-16-R F-1）: 時効援用通知 発送後のフォロー

固定する仕様:
- 1) 発送日から 14／28 日経過で債権者回答が未記録の案件を業務 LINE に 1 通ずつ（台帳 key jikou_followup_14/28:{案件ID}:{発送日}・
  同じ key は 1 通＝重送なし・結果を記録すると止まる・未来の発送日は対象外・env で停止・DB 未設定は何もしない）。
- 発送日の正: App 21 の DATE 欄「発送日」があるときだけ日次で本表へ写す（欄が無い間は読まない）。無い間は人の入力。
- 2) /app/jikou/result/{案件ID}（_gate・human のみ）: 枠ごとに 4 値＋回答日＋要旨。明示確認必須。成立／不成立／一部成立は回答日必須。
  kintone への写しは対応欄がそろっているときだけ（無ければ skipped・書かない）。GET は書込ゼロ。
- 3) /app/reply/metrics に「案件結果」（債権者別の成立率・発送→回答日数の中央値）。表が無ければ注記のみ（他の集計は落ちない）。
- 禁止: 相談者への自動送信なし（hub/jikou_followup は hub/line_channel を import しない・LINE push 0 回）。
- pin: 新 module の sink 違反ゼロ・allowlist 追加ゼロ・main は末尾結線のみ・migration は e6a8 の次（単一チェーン）。
"""

import asyncio
import datetime
import os
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from test_jikou_reply_q1c import ATTORNEY, USER, _DbMixin, _FakeClient, _auth, _client, _rec, _run  # noqa: E402

import main  # noqa: E402
from hub import db  # noqa: E402
from hub import jikou_followup as F  # noqa: E402
from hub import jikou_reply_metrics as M  # noqa: E402
from hub import jikou_reply_promote as pm  # noqa: E402
from hub import kintone  # noqa: E402
from hub import reply_draft as rd  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_jikou_result_view as V  # noqa: E402
from hub import webapp_reply_metrics_view as MV  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 10, 16, 3, 0, tzinfo=UTC)             # 2026-10-16 12:00 JST
TODAY = datetime.date(2026, 10, 16)
CASE = "1234"
CUSTOMER = "山田太郎"
CRED1, CRED2 = "アコム", "レイク"
RECORD = _rec(**{"$id": CASE, "$revision": "7", "顧客名": CUSTOMER, "LINEユーザーID": USER, "電話番号": "09012345678",
                 "問い合わせ業者名": CRED1, "対象債権者2": CRED2, "対象債権者3": "",
                 "契約書ステータス": F.NOTICE_DONE, F.FIELD_SHIPPED: ""})


def _days_ago(n: int) -> datetime.date:
    return TODAY - datetime.timedelta(days=n)


class TestPure(unittest.TestCase):
    def test_dates_and_thresholds(self):
        self.assertEqual(F.parse_date("2026-10-01"), datetime.date(2026, 10, 1))
        self.assertIsNone(F.parse_date("2026/10/01"))
        self.assertIsNone(F.parse_date(""))
        self.assertIsNone(F.parse_date("2026-13-01"))
        self.assertEqual(F.due_thresholds(_days_ago(13), TODAY), [])
        self.assertEqual(F.due_thresholds(_days_ago(14), TODAY), [14])
        self.assertEqual(F.due_thresholds(_days_ago(27), TODAY), [14])
        self.assertEqual(F.due_thresholds(_days_ago(28), TODAY), [14, 28])
        self.assertEqual(F.due_thresholds(TODAY + datetime.timedelta(days=1), TODAY), [])
        self.assertEqual(F.FOLLOWUP_DAYS, (14, 28))
        self.assertEqual(F.ledger_key(14, CASE, _days_ago(14)), "jikou_followup_14:1234:2026-10-02")
        self.assertEqual(F.ledger_key(28, CASE, _days_ago(28)), "jikou_followup_28:1234:2026-09-18")
        self.assertEqual(F.today_jst(NOW), TODAY)

    def test_notice_text_counts_only(self):
        t = F.notice_text(CASE, 14, 15, 2)
        self.assertTrue(t.startswith(F.NOTICE_HEADER))
        for ok in ("案件 No.1234", "15 日経過", "14 日の節目", "未記録の社数: 2", "/app/jikou/result/1234"):
            self.assertIn(ok, t)
        for bad in (CUSTOMER, CRED1, USER):
            self.assertNotIn(bad, t)

    def test_entry_validation_and_creditors(self):
        ok, e = F.validate_entry(F.RESULT_ESTABLISHED, "2026-10-10", " 成立の回答あり \r\n")
        self.assertTrue(ok)
        self.assertEqual((e["result"], e["answered_on"], e["summary"]),
                         (F.RESULT_ESTABLISHED, datetime.date(2026, 10, 10), "成立の回答あり"))
        self.assertFalse(F.validate_entry(F.RESULT_ESTABLISHED, "", "")[0])          # 回答日必須
        self.assertFalse(F.validate_entry(F.RESULT_PARTIAL, "x", "")[0])
        ok, e = F.validate_entry(F.RESULT_NO_ANSWER, "2026-10-10", "")
        self.assertTrue(ok)
        self.assertIsNone(e["answered_on"])                                           # 回答なしは回答日を持たない
        self.assertIsNone(e["summary"])
        self.assertFalse(F.validate_entry("", "", "")[0])
        self.assertFalse(F.validate_entry("成立", "2026-10-10", "")[0])                # code 以外は拒否
        self.assertEqual(F.creditors_of(RECORD), [(1, CRED1), (2, CRED2)])
        self.assertEqual(F.creditors_of({}), [])
        self.assertEqual(F.normalize_summary("a" * 2000), "a" * F.MAX_SUMMARY_CHARS)
        self.assertEqual(sorted(F.RESULT_LABELS.values()), sorted(["成立", "不成立", "一部成立", "回答なし"]))


class _Mixin(_DbMixin):
    def setUp(self):
        super().setUp()

        async def _create():
            async with db.get_async_engine().begin() as c:
                for md in (F.metadata, rd.metadata, pm.metadata):
                    await c.run_sync(md.create_all)
        _run(_create())
        self.env({"ATTORNEY_LINE_USER_ID": ATTORNEY, "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz_token"})
        F.clear_form_cache()
        self.form = AsyncMock(side_effect=RuntimeError("no form"))          # 既定: form fields を読めない＝欄なし扱い
        self.search = AsyncMock(return_value=[])
        self.get_record = AsyncMock(return_value=RECORD)
        self.update = AsyncMock(return_value=None)
        self._kp = [patch.object(kintone, "get_form_fields", self.form), patch.object(kintone, "search_records", self.search),
                    patch.object(kintone, "get_record", self.get_record), patch.object(kintone, "update_record", self.update)]
        for p in self._kp:
            p.start()

    def tearDown(self):
        for p in self._kp:
            p.stop()
        F.clear_form_cache()
        super().tearDown()

    def ops(self) -> list:
        return self._rows(sl.send_operation)

    def rows(self) -> list:
        return _run(F.rows_for_case(CASE))

    def seed_case(self, shipped: datetime.date | None, creditors=((1, CRED1), (2, CRED2))):
        _run(F.ensure_rows(CASE, list(creditors), now=NOW))
        if shipped is not None:
            _run(F.set_shipped(CASE, shipped, source="human", actor=F.ACTOR_HUMAN, now=NOW))

    def scan(self, now=NOW) -> dict:
        return _run(F.followup_scan(now=now))


class TestScan(_Mixin):
    def test_14_then_28_once_each_and_stop_after_record(self):
        self.seed_case(_days_ago(15))
        r = self.scan()
        self.assertEqual((r["cases"], r["sent"], r["throttled"], r["notice"]), (1, 1, 0, rp.NOTIFY_SENT))
        self.assertEqual(self.biz.call_count, 1)
        to, text = self.biz.call_args.args[0], self.biz.call_args.args[1]
        self.assertEqual(to, ATTORNEY)
        self.assertIn("未記録の社数: 2", text)
        for bad in (CUSTOMER, USER, CRED1, CRED2):
            self.assertNotIn(bad, text)
        r = self.scan()                                                    # 同日再実行: 重送なし
        self.assertEqual((r["sent"], r["throttled"]), (0, 1))
        self.assertEqual(self.biz.call_count, 1)
        r = self.scan(now=NOW + datetime.timedelta(days=10))               # 25 日: まだ 28 未到来
        self.assertEqual((r["sent"], r["throttled"]), (0, 1))
        r = self.scan(now=NOW + datetime.timedelta(days=14))               # 29 日: 28 の通知が 1 通
        self.assertEqual((r["sent"], r["throttled"]), (1, 1))
        self.assertEqual(self.biz.call_count, 2)
        keys = sorted(o["inbound_event_id"] for o in self.ops())
        self.assertEqual(keys, ["jikou_followup_14:1234:2026-10-01", "jikou_followup_28:1234:2026-10-01"])
        self.assertTrue(all((o["business"], o["channel"], o["purpose"], o["state"]) ==
                            ("jikou", "jikou_notice", sl.PURPOSE_OTHER, sl.STATE_SENT) for o in self.ops()))
        # 全枠に結果を記録すると以後は送らない（案件自体が対象外）
        for slot in (1, 2):
            self.assertTrue(_run(F.record_result(CASE, slot, result=F.RESULT_NO_ANSWER, answered_on=None, summary=None)))
        r = self.scan(now=NOW + datetime.timedelta(days=40))
        self.assertEqual((r["cases"], r["sent"], r["throttled"]), (0, 0, 0))
        self.assertEqual(self.biz.call_count, 2)
        self.assertEqual(_FakeClient.calls, [])                             # 相談者チャネルへの push はゼロ

    def test_partial_record_counts_remaining(self):
        self.seed_case(_days_ago(20))
        self.assertTrue(_run(F.record_result(CASE, 1, result=F.RESULT_ESTABLISHED, answered_on=_days_ago(5),
                                             summary="成立")))
        self.scan()
        self.assertIn("未記録の社数: 1", self.biz.call_args.args[1])

    def test_not_due_future_disabled_and_no_db(self):
        self.seed_case(_days_ago(13))
        self.assertEqual(self.scan()["sent"], 0)
        self.seed_case(TODAY + datetime.timedelta(days=3))
        self.assertEqual(self.scan()["sent"], 0)
        self.seed_case(_days_ago(20))
        with patch.dict(os.environ, {F.DISABLED_ENV: "1"}):
            self.assertEqual(self.scan()["notice"], F.SEND_DISABLED)
        self.assertEqual(self.biz.call_count, 0)
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            r = _run(F.followup_scan(now=NOW))
        self.assertEqual((r["cases"], r["notice"]), (0, F.SEND_NONE))
        self.assertEqual(self.biz.call_count, 0)

    def test_no_shipped_date_is_not_followed(self):
        self.seed_case(None)
        r = self.scan(now=NOW + datetime.timedelta(days=60))
        self.assertEqual((r["cases"], r["sent"]), (0, 0))

    def test_import_from_app21_only_when_field_exists(self):
        self.seed_case(None)
        # 欄が無い（form fields に 発送日 なし）: 読まない
        self.form.side_effect = None
        self.form.return_value = {"顧客名": {"type": "SINGLE_LINE_TEXT"}}
        r = self.scan()
        self.assertFalse(r["import"]["available"])
        self.assertEqual(self.search.call_count, 0)
        # 欄がある: 作成済みの案件を写し、以後の判定に使う
        F.clear_form_cache()
        self.form.return_value = {F.FIELD_SHIPPED: {"type": "DATE"}}
        rec = dict(RECORD)
        rec[F.FIELD_SHIPPED] = {"value": _days_ago(16).isoformat()}
        self.search.return_value = [rec]
        r = self.scan()
        self.assertEqual((r["import"]["available"], r["import"]["records"], r["import"]["shipped_set"]), (True, 1, 2))
        self.assertEqual(r["sent"], 1)
        rows = self.rows()
        self.assertEqual([(x["slot"], x["creditor_name"], x["shipped_on"], x["shipped_source"]) for x in rows],
                         [(1, CRED1, _days_ago(16), "kintone"), (2, CRED2, _days_ago(16), "kintone")])
        q = self.search.call_args.args[1]
        self.assertIn(F.NOTICE_DONE, q)
        self.assertIn(f'{F.FIELD_SHIPPED} != ""', q)
        self.assertEqual(self.update.call_count, 0)                        # 取込は読むだけ

    def test_scan_read_failure_is_failed_not_exception(self):
        with patch.object(F, "open_cases", AsyncMock(side_effect=RuntimeError("x"))):
            self.assertEqual(self.scan()["notice"], rp.NOTIFY_FAILED)
        self.assertEqual(self.biz.call_count, 0)

    def test_job_registration(self):
        async def _reg():
            F.register_followup_job()
            F.register_followup_job()
            return hub_scheduler.is_registered(F.JOB_NAME), hub_scheduler._jobs[F.JOB_NAME]
        try:
            ok, job = asyncio.run(_reg())
            self.assertTrue(ok)
            self.assertEqual((job.kind, job.hour_jst), ("daily", 9))
        finally:
            mine = hub_scheduler._jobs.pop(F.JOB_NAME, None)
            if mine is not None and mine.task is not None and not mine.task.done():
                mine.task.cancel()


class TestPage(_Mixin):
    def _post(self, data: dict, rid: str = CASE):
        return _client.post(f"{V.PAGE}/{rid}/save", data=data, headers=_auth(), follow_redirects=False)

    def test_requires_auth_and_id_format(self):
        self.assertEqual(_client.get(f"{V.PAGE}/{CASE}", follow_redirects=False).status_code, 303)
        self.assertEqual(_client.post(f"{V.PAGE}/{CASE}/save", data={}, follow_redirects=False).status_code, 303)
        self.assertEqual(_client.get(f"{V.API}/{CASE}", follow_redirects=False).status_code, 303)
        self.assertEqual(_client.get(f"{V.PAGE}/abc", headers=_auth()).status_code, 400)
        self.assertEqual(self._post({}, rid="x").status_code, 400)

    def test_get_shows_creditors_without_pii_and_writes_nothing(self):
        r = _client.get(f"{V.PAGE}/{CASE}", headers=_auth())
        self.assertEqual(r.status_code, 200)
        for ok in (f"No.{CASE}", CRED1, CRED2, "未記録", "記録する", 'name="shipped_on"'):
            self.assertIn(ok, r.text)
        for bad in (CUSTOMER, USER, "09012345678"):
            self.assertNotIn(bad, r.text)
        self.assertEqual(self.rows(), [])                                  # GET は書かない
        self.assertEqual(self.update.call_count, 0)

    def test_confirm_required_then_save_with_copy_skipped(self):
        r = self._post({"result_1": F.RESULT_ESTABLISHED, "answered_on_1": "2026-10-10", "summary_1": "成立",
                        "shipped_on": "2026-10-01"})
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"{V.PAGE}/{CASE}?done=confirm_required"))
        self.assertEqual(self.rows(), [])
        r = self._post({"result_1": F.RESULT_ESTABLISHED, "answered_on_1": "2026-10-10", "summary_1": "成立の回答",
                        "shipped_on": "2026-10-01", "confirm": "1"})
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=saved_copy_skipped")
        rows = self.rows()
        self.assertEqual([(x["slot"], x["creditor_name"], x["shipped_on"], x["shipped_source"], x["result"], x["answered_on"],
                           x["summary"], x["recorded_by"]) for x in rows],
                         [(1, CRED1, datetime.date(2026, 10, 1), "human", F.RESULT_ESTABLISHED, datetime.date(2026, 10, 10),
                           "成立の回答", "human"),
                          (2, CRED2, datetime.date(2026, 10, 1), "human", None, None, None, None)])
        hist = self._rows(F.jikou_case_result_history)
        self.assertEqual(sorted(h["field"] for h in hist), ["answered_on", "result", "shipped_on", "shipped_on", "summary"])
        self.assertNotIn("成立の回答", [h["to_value"] for h in hist])        # 要旨の本文は履歴に置かない
        self.assertEqual(self.update.call_count, 0)                         # 欄が無い: 写しは書かない
        # 画面に記録が出る・要旨は _gate の内側のみ
        page = _client.get(f"{V.PAGE}/{CASE}", headers=_auth()).text
        self.assertIn("成立の回答", page)
        self.assertIn("回答日 2026-10-10", page)
        api = _client.get(f"{V.API}/{CASE}", headers=_auth()).json()
        self.assertEqual(api["rows"][0]["result"], F.RESULT_ESTABLISHED)
        self.assertTrue(api["rows"][0]["has_summary"])
        self.assertNotIn("成立の回答", str(api))
        # 変更なし（結果未選択・発送日なし）
        r = self._post({"confirm": "1"})
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=nothing")
        self.assertEqual(_FakeClient.calls, [])

    def test_copy_to_app21_only_when_fields_exist(self):
        self.form.side_effect = None
        self.form.return_value = {t.format(slot=1): {"type": "X"} for t in F.COPY_FIELDS.values()}
        r = self._post({"result_1": F.RESULT_NO_ANSWER, "confirm": "1"})
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=saved_copied")
        self.assertEqual(self.update.call_count, 1)
        app, rid, fields = self.update.call_args.args[0], self.update.call_args.args[1], self.update.call_args.args[2]
        self.assertEqual((app, rid), (F.APP21, CASE))
        self.assertEqual(fields, {"時効援用結果_1": "回答なし", "債権者回答日_1": "", "回答要旨_1": ""})
        self.assertEqual(self.update.call_args.kwargs.get("revision"), "7")
        # 枠 2 の欄は無い → その枠は skipped（混在は failed 扱いの固定文言）
        r = self._post({"result_2": F.RESULT_NOT_ESTABLISHED, "answered_on_2": "2026-10-12", "confirm": "1"})
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=saved_copy_skipped")
        self.assertEqual(self.update.call_count, 1)

    def test_bad_input_and_unknown_slot(self):
        r = self._post({"result_1": F.RESULT_ESTABLISHED, "confirm": "1"})              # 回答日なし
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=bad_input")
        r = self._post({"result_3": F.RESULT_NO_ANSWER, "confirm": "1"})                # 枠 3 の債権者は無い
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=bad_input")
        r = self._post({"shipped_on": "10/01", "confirm": "1"})
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=bad_input")
        self.assertEqual(self.rows(), [])

    def test_shipped_from_app21_is_readonly(self):
        self.form.side_effect = None
        self.form.return_value = {F.FIELD_SHIPPED: {"type": "DATE"}}
        rec = dict(RECORD)
        rec[F.FIELD_SHIPPED] = {"value": "2026-09-20"}
        self.get_record.return_value = rec
        page = _client.get(f"{V.PAGE}/{CASE}", headers=_auth()).text
        self.assertIn("2026-09-20", page)
        self.assertNotIn('name="shipped_on"', page)
        r = self._post({"result_1": F.RESULT_NO_ANSWER, "shipped_on": "2026-10-01", "confirm": "1"})
        self.assertEqual(r.status_code, 303)
        self.assertEqual([x["shipped_on"] for x in self.rows()], [None, None])      # 人の入力で上書きしない（正は App 21）

    def test_app21_unreachable_falls_back_to_rows(self):
        self.get_record.side_effect = RuntimeError("down")
        page = _client.get(f"{V.PAGE}/{CASE}", headers=_auth()).text
        self.assertIn(V._NOTICES["kintone"], page)
        r = self._post({"result_1": F.RESULT_NO_ANSWER, "confirm": "1"})
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=not_found")
        self.seed_case(_days_ago(1))
        r = self._post({"result_1": F.RESULT_NO_ANSWER, "confirm": "1"})
        self.assertEqual(r.headers["location"], f"{V.PAGE}/{CASE}?done=saved_copy_skipped")
        self.assertEqual(self.rows()[0]["result"], F.RESULT_NO_ANSWER)


class TestMetrics(_Mixin):
    def test_case_results_section(self):
        self.seed_case(_days_ago(30))
        _run(F.record_result(CASE, 1, result=F.RESULT_ESTABLISHED, answered_on=_days_ago(20), summary="s"))
        _run(F.ensure_rows("2222", [(1, CRED1)], now=NOW))
        _run(F.set_shipped("2222", _days_ago(40), source="human", actor="human", now=NOW))
        _run(F.record_result("2222", 1, result=F.RESULT_NOT_ESTABLISHED, answered_on=_days_ago(10), summary=None))
        _run(F.ensure_rows("3333", [(1, CRED1)], now=NOW))
        _run(F.set_shipped("3333", _days_ago(50), source="human", actor="human", now=NOW))
        _run(F.record_result("3333", 1, result=F.RESULT_PARTIAL, answered_on=_days_ago(30), summary=None))
        m = _run(M.collect(now=NOW))
        self.assertEqual(m.notes, ())
        self.assertEqual(m.case_unanswered, 1)                                 # CASE の枠 2
        self.assertEqual(len(m.case_results), 1)
        c = m.case_results[0]
        self.assertEqual((c.creditor, c.cases, c.established, c.partial, c.not_established, c.no_answer),
                         (CRED1, 3, 1, 1, 1, 0))
        self.assertEqual(c.median_days, 20)                                    # 10・20・30 の中央値
        self.assertAlmostEqual(c.established_rate, 2 / 3)
        with patch.object(M, "_now", lambda: NOW):
            page = _client.get(MV.PAGE, headers=_auth()).text
            j = _client.get(MV.API, headers=_auth()).json()
        for ok in ("案件結果", CRED1, "67%", "20日", "結果未記録の枠: 1"):
            self.assertIn(ok, page)
        for bad in (CUSTOMER, USER, "s</"):
            self.assertNotIn(bad, page)
        self.assertEqual(j["case_results"][0]["cases"], 3)
        self.assertEqual(j["case_unanswered"], 1)
        self.assertEqual(j["notes"], [])

    def test_missing_table_is_note_only(self):
        async def _drop():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(F.metadata.drop_all)
        _run(_drop())
        m = _run(M.collect(now=NOW))
        self.assertEqual((m.case_results, m.case_unanswered, m.notes), ((), 0, ("case_results_unavailable",)))
        page = _client.get(MV.PAGE, headers=_auth()).text
        self.assertIn("案件結果の表を読めませんでした", page)
        self.assertIn("時効 返信計測", page)                                   # 他の集計は表示される


class TestPins(unittest.TestCase):
    def test_sink_allowlist_wiring_and_no_customer_send(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/jikou_followup.py", "hub/webapp_jikou_result_view.py", "hub/jikou_reply_metrics.py",
                     "hub/webapp_reply_metrics_view.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("jikou_followup", allow)
        self.assertNotIn("webapp_jikou_result_view", allow)
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        # 末尾追記（3 行）。JIKOU-PROGRESS-1（F-2）が同じ main.py 末尾へ追記したため、窓を「F-1 の見出しコメント以降 3 行」へ移した。
        _h = next(i for i, line in enumerate(src) if line.startswith("# ── JIKOU-FOLLOWUP-1（QUEUE-2026-10-16-R F-1）"))
        self.assertIn("wire_jikou_result(app)", "\n".join(src[_h:_h + 3]))
        fsrc = (REPO / "hub" / "jikou_followup.py").read_text(encoding="utf-8")
        body = fsrc.split('"""', 2)[2]
        for bad in ("line_channel", "push_text", "reply_with_push_fallback", "sa.delete", "delete_record"):
            self.assertNotIn(bad, body)
        vsrc = (REPO / "hub" / "webapp_jikou_result_view.py").read_text(encoding="utf-8").split('"""', 2)[2]
        for bad in ("line_channel", "push_text", "delete_record"):
            self.assertNotIn(bad, vsrc)
        self.assertIn(F.register_followup_job, main.app.router.on_startup)
        self.assertEqual((F.JOB_NAME, F.RUN_HOUR_JST, F.FOLLOWUP_DAYS), ("JIKOU_FOLLOWUP", 9, (14, 28)))
        self.assertEqual(F.LEDGER_KINDS, {14: "jikou_followup_14", 28: "jikou_followup_28"})
        # 画面の全 route は _gate
        for r in main.app.router.routes:
            if getattr(r, "path", "").startswith((V.PAGE, V.API)):
                self.assertTrue(getattr(r.endpoint, "__webapp_gate__", False), r.path)

    def test_migration_is_next_in_single_chain(self):
        p = REPO / "alembic" / "versions" / "20261016_a9b1c3d5e7f0_jikou_case_result.py"
        src = p.read_text(encoding="utf-8")
        self.assertEqual(re.search(r"^revision: str = '([0-9a-f]+)'", src, re.M).group(1), "a9b1c3d5e7f0")
        self.assertEqual(re.search(r"^down_revision: .*? = '([0-9a-f]+)'", src, re.M).group(1), "e6a8c0d2f4b6")
        for col in ("case_record_id", "slot", "shipped_on", "result", "answered_on", "summary"):
            self.assertIn(col, src)
        self.assertIn("downgrade_refused_data_present", src)
        env = (REPO / "alembic" / "env.py").read_text(encoding="utf-8")
        self.assertIn("jikou_followup_metadata", env)


if __name__ == "__main__":
    unittest.main()
