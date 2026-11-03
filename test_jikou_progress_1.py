"""JIKOU-PROGRESS-1（QUEUE-2026-10-16-R F-2）: 相談者向け進捗連絡の下書き積み（送信は Q4 の承認画面だけ）

固定する仕様:
- 文言 3 種（P-1 入金確認／P-2 発送完了／P-3 債権者回答到着）は hub/jikou_reply_texts.PROGRESS_TEXTS（要承認・auto=False・
  独自 sha pin）。既存の TEXTS／TABLE_SHA256_PIN は不変。本文に数値を書かない（差し込みは 発送日・社数 のみ）。
- イベント発生で下書きが 1 件積まれる（reply_draft・proposed・category 進捗の事実回答・conversation_ref progress:{event}:{案件ID}）。
  同一案件・同一イベントは二重に積まない。宛先（LINEユーザーID）が無ければ積まない。
- 承認なしでは送信ゼロ。承認は Q4 の既存経路（/app/drafts/{id}/act）＝新しい送信経路を作らない。進捗連絡は AI 下書きの結果統計
  （reply_draft_approval）・手本候補に入れない。
- 入金確認・発送完了は走査ジョブ（JIKOU_PROGRESS_SCAN・interval）、債権者回答到着は F-1 の結果記録フック。
- pin: 新 module の sink 違反ゼロ・allowlist 追加ゼロ・main は末尾結線のみ・hub/jikou_progress は hub/line_channel を import しない。
"""

import datetime
import os
import re
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import USER, USER2, _FakeClient, _auth, _client, _rec, _run, _texts  # noqa: E402
from test_jikou_reply_q4 import _DraftMixin  # noqa: E402

import main  # noqa: E402
from hub import db  # noqa: E402
from hub import jikou_constants as C  # noqa: E402
from hub import jikou_followup as F  # noqa: E402
from hub import jikou_progress as P  # noqa: E402
from hub import jikou_reply_texts as T  # noqa: E402
from hub import kintone  # noqa: E402
from hub import reply_draft as rd  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_drafts_view as dv  # noqa: E402
from hub import webapp_jikou_result_view as RV  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 10, 16, 3, 0, tzinfo=UTC)
TODAY = datetime.date(2026, 10, 16)
CASE = "1234"
CASE2 = "2222"
CRED1, CRED2 = "アコム", "レイク"
APP21_REC = _rec(**{"$id": CASE, "$revision": "3", "顧客名": "山田太郎", "LINEユーザーID": USER, "入金状況": "入金済み",
                    "問い合わせ業者名": CRED1, "対象債権者2": CRED2, "対象債権者3": "", "契約書ステータス": F.NOTICE_DONE})


class TestTexts(unittest.TestCase):
    def test_table_and_pins(self):
        self.assertEqual(T.PROGRESS_TEXT_IDS, ("P-1", "P-2", "P-3"))
        for i, e in T.PROGRESS_TEXTS.items():
            self.assertEqual(e["status"], T.STATUS_NEEDS_APPROVAL, i)          # 要承認
            self.assertFalse(e["auto"], i)                                      # 自動送信の選択器は選ばない
            body = re.sub(r"【[^】]+】", "", e["text"])
            self.assertFalse(re.search(r"[0-9０-９]", body), i)                 # 数値を本文に書かない
            self.assertNotIn("円", body)
        self.assertTrue(T.verify_progress_pin())
        self.assertEqual(len(T.PROGRESS_SHA256_PIN), 64)
        self.assertEqual(T.TABLE_SHA256_PIN, "c8c45c0bc8fafb997dc30bd67430d35909194cdd9ff35eef8497aafeb3e3aacf")   # Q2 の凍結 pin 不変
        self.assertTrue(T.verify_pin())
        self.assertNotIn("P-1", T.TEXTS)
        self.assertEqual(len(T.progress_approval_table()), 3)

    def test_render(self):
        self.assertEqual(P.render(P.EVENT_PAID), T.PROGRESS_TEXTS["P-1"]["text"])
        t = P.render(P.EVENT_SHIPPED, {"発送日": "2026年10月1日", "社数": 2})
        self.assertIn("2026年10月1日に、時効援用通知書を対象の債権者 2 社へ", t)
        self.assertNotIn("【", t)
        self.assertIsNone(P.render(P.EVENT_SHIPPED, {"発送日": "2026年10月1日"}))       # 差し込み不足は積まない
        self.assertIsNone(P.render("other"))
        with patch.object(T, "PROGRESS_SHA256_PIN", "0" * 64):
            self.assertIsNone(P.render(P.EVENT_PAID))                           # pin 不一致は積まない
        self.assertEqual(P.fmt_date(datetime.date(2026, 10, 1)), "2026年10月1日")
        self.assertEqual(P.parse_ref("progress:paid:12"), ("paid", "12"))
        self.assertIsNone(P.parse_ref("progress:other:12"))
        self.assertIsNone(P.parse_ref("c1"))
        self.assertTrue(P.is_progress_ref(P.ref(P.EVENT_ANSWERED, CASE)))


class _Mixin(_DraftMixin):
    def setUp(self):
        super().setUp()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(F.metadata.create_all)
        _run(_create())
        F.clear_form_cache()
        self.get_record = AsyncMock(return_value=APP21_REC)
        self.search = AsyncMock(return_value=[])
        self.form = AsyncMock(side_effect=RuntimeError("no form"))
        self._kp = [patch.object(kintone, "get_record", self.get_record), patch.object(kintone, "search_records", self.search),
                    patch.object(kintone, "get_form_fields", self.form), patch.object(kintone, "update_record", AsyncMock())]
        for p in self._kp:
            p.start()

    def tearDown(self):
        for p in self._kp:
            p.stop()
        F.clear_form_cache()
        super().tearDown()

    def progress_drafts(self) -> list:
        return [d for d in self.drafts() if P.is_progress_ref(d.get("conversation_ref"))]


class TestEnqueue(_Mixin):
    def test_one_draft_per_event_and_case(self):
        self.assertEqual(_run(P.enqueue(P.EVENT_PAID, CASE, USER, now=NOW)), P.QUEUED)
        self.assertEqual(_run(P.enqueue(P.EVENT_PAID, CASE, USER, now=NOW)), P.DUPLICATE)
        self.assertEqual(_run(P.enqueue(P.EVENT_PAID, CASE2, USER, now=NOW)), P.QUEUED)       # 別案件は別
        self.assertEqual(_run(P.enqueue(P.EVENT_ANSWERED, CASE, USER, now=NOW)), P.QUEUED)    # 別イベントは別
        rows = self.progress_drafts()
        self.assertEqual(len(rows), 3)
        d = rows[0]
        self.assertEqual((d["state"], d["category"], d["conversation_ref"], d["pending_id"], d["guard_ok"]),
                         (rd.STATE_PROPOSED, P.CATEGORY, "progress:paid:1234", None, True))
        self.assertEqual(d["line_user_hash"], sl.line_user_hash("jikou", USER))
        self.assertEqual(d["draft_text"], T.PROGRESS_TEXTS["P-1"]["text"])
        self.assertEqual(_FakeClient.calls, [])                                              # 送信ゼロ
        self.assertEqual(self.biz.call_count, 0)

    def test_no_user_or_text_or_db(self):
        self.assertEqual(_run(P.enqueue(P.EVENT_PAID, CASE, "", now=NOW)), P.NO_USER)
        self.assertEqual(_run(P.enqueue(P.EVENT_PAID, CASE, "not-a-line-id", now=NOW)), P.NO_USER)
        self.assertEqual(_run(P.enqueue(P.EVENT_SHIPPED, CASE, USER, {}, now=NOW)), P.NO_TEXT)
        self.assertEqual(self.progress_drafts(), [])
        with patch.object(rd, "create", AsyncMock(return_value=None)):
            self.assertEqual(_run(P.enqueue(P.EVENT_PAID, CASE, USER, now=NOW)), P.UNAVAILABLE)


class TestScan(_Mixin):
    def test_paid_scan(self):
        rec_no_user = _rec(**{"$id": "9", "LINEユーザーID": "", "入金状況": "入金済み"})
        self.search.return_value = [APP21_REC, rec_no_user]
        r = _run(P.scan(now=NOW))
        self.assertEqual((r["paid"]["records"], r["paid"][P.QUEUED], r["paid"]["skipped"]), (2, 1, 1))
        q = self.search.call_args.args[1]
        self.assertIn('入金状況 in ("入金済み")', q)
        self.assertIn('LINEユーザーID != ""', q)
        self.assertIn('更新日時 > "2026-10-02T03:00:00Z"', q)                      # PROGRESS_LOOKBACK_DAYS=14
        r = _run(P.scan(now=NOW))
        self.assertEqual((r["paid"][P.QUEUED], r["paid"][P.DUPLICATE]), (0, 1))
        self.assertEqual(len(self.progress_drafts()), 1)
        self.assertEqual(_FakeClient.calls, [])

    def test_shipped_scan_uses_f1_rows_and_lookback(self):
        _run(F.ensure_rows(CASE, [(1, CRED1), (2, CRED2)], now=NOW))
        _run(F.set_shipped(CASE, TODAY - datetime.timedelta(days=3), source="human", actor="human", now=NOW))
        _run(F.ensure_rows(CASE2, [(1, CRED1)], now=NOW))
        _run(F.set_shipped(CASE2, TODAY - datetime.timedelta(days=20), source="human", actor="human", now=NOW))   # 期間外
        r = _run(P.scan(now=NOW))
        self.assertEqual((r["shipped"]["records"], r["shipped"][P.QUEUED]), (1, 1))
        rows = self.progress_drafts()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["conversation_ref"], "progress:shipped:1234")
        self.assertIn("2026年10月13日に、時効援用通知書を対象の債権者 2 社へ", rows[0]["draft_text"])
        self.assertEqual(self.get_record.call_args.args[1], CASE)
        r = _run(P.scan(now=NOW))
        self.assertEqual((r["shipped"][P.QUEUED], r["shipped"][P.DUPLICATE]), (0, 1))
        self.assertEqual(self.get_record.call_count, 1)                                     # 二重積みの判定は DB だけで済ませる

    def test_disabled_and_no_db(self):
        self.search.return_value = [APP21_REC]
        with patch.dict(os.environ, {P.DISABLED_ENV: "1"}):
            r = _run(P.scan(now=NOW))
        self.assertTrue(r["disabled"])
        self.assertEqual(self.search.call_count, 0)
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            r = _run(P.scan(now=NOW))
        self.assertIsNone(r["paid"])
        self.assertEqual(self.progress_drafts(), [])

    def test_job_registration(self):
        import asyncio

        async def _reg():
            P.register_progress_job()
            P.register_progress_job()
            return hub_scheduler.is_registered(P.JOB_NAME), hub_scheduler._jobs[P.JOB_NAME]
        try:
            ok, job = asyncio.run(_reg())
            self.assertTrue(ok)
            self.assertEqual((job.kind, job.minutes), ("interval", C.PROGRESS_SCAN_INTERVAL_MINUTES))
        finally:
            mine = hub_scheduler._jobs.pop(P.JOB_NAME, None)
            if mine is not None and mine.task is not None and not mine.task.done():
                mine.task.cancel()


class TestHook(_Mixin):
    def test_result_record_queues_answered_once(self):
        self.assertIn(P.on_result_recorded, F._result_hooks)
        r = _client.post(f"{RV.PAGE}/{CASE}/save", data={"result_1": F.RESULT_ESTABLISHED, "answered_on_1": "2026-10-10",
                                                          "confirm": "1"}, headers=_auth(), follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        rows = self.progress_drafts()
        self.assertEqual([d["conversation_ref"] for d in rows], ["progress:answered:1234"])
        self.assertEqual(rows[0]["draft_text"], T.PROGRESS_TEXTS["P-3"]["text"])
        r = _client.post(f"{RV.PAGE}/{CASE}/save", data={"result_2": F.RESULT_NO_ANSWER, "confirm": "1"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(len(self.progress_drafts()), 1)                                    # 同一案件は 1 回
        self.assertEqual(_FakeClient.calls, [])

    def test_hook_failure_does_not_break_record(self):
        with patch.object(P, "enqueue", AsyncMock(side_effect=RuntimeError("x"))):
            r = _client.post(f"{RV.PAGE}/{CASE}/save", data={"result_1": F.RESULT_NO_ANSWER, "confirm": "1"},
                             headers=_auth(), follow_redirects=False)
        self.assertIn("done=saved", r.headers["location"])
        self.assertEqual(_run(F.rows_for_case(CASE))[0]["result"], F.RESULT_NO_ANSWER)


class TestApproveViaQ4(_Mixin):
    def test_no_send_without_approval_then_q4_sends_once(self):
        _run(rp.register(business="jikou", channel="jikou", line_user_hash=sl.line_user_hash("jikou", USER),
                         route="inbound", reason="demoted", inbound_event_id="ev-open", conversation_id=None,
                         category=None, approval_record_id=None))               # 同じ相談者の open 対応待ち
        self.assertEqual(_run(P.enqueue(P.EVENT_SHIPPED, CASE, USER, {"発送日": "2026年10月1日", "社数": 2}, now=NOW)), P.QUEUED)
        did = int(self.progress_drafts()[0]["draft_id"])
        self.assertEqual(_texts(), [])                                                       # 積んだだけでは送らない
        lst = _client.get(dv.PAGE, headers=_auth()).text
        self.assertIn("進捗連絡: 発送完了（案件 No.1234）", lst)
        r = _client.get(f"{dv.PAGE}/{did}", headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertIn("2026年10月1日に、時効援用通知書", r.text)
        self.assertIn("承認して送信", r.text)
        self.assertNotIn(USER, r.text)
        f = self.open_form(did)
        text = self.progress_drafts()[0]["draft_text"]
        r = self.act(did, dict(f, action="send", text=text, confirm="1"))
        self.assertIn("done=sent", r.headers["location"])
        self.assertEqual(_texts(), [text])                                                   # 既存 Q4 経路で 1 通
        ops = [o for o in self.ops() if o["actor"] == sl.ACTOR_APPROVED_DRAFT]
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["purpose"], ops[0]["state"]), ("reply", "sent"))
        d = _run(rd.get_draft(did))
        self.assertEqual((d["state"], d["approval_kind"]), (rd.STATE_SENT, rd.KIND_ASIS))
        self.assertEqual(self.approvals(), [])                                               # AI 下書きの統計に入れない
        # PRE-REVIEW-1 #301-1: 進捗連絡の承認送信は同じ相談者の対応待ちを閉じない
        self.assertEqual([p["state"] for p in self.pending()], [rp.STATE_OPEN])
        self.assertEqual(self.chatlog.call_args_list[-1].args[1:3], ("assistant", text))
        r2 = self.act(did, dict(f, action="send", text=text, confirm="1"))
        self.assertIn("done=already", r2.headers["location"])
        self.assertEqual(len(_texts()), 1)

    def test_edited_send_and_reject_do_not_touch_exemplars_or_outcomes(self):
        _run(P.enqueue(P.EVENT_PAID, CASE, USER, now=NOW))
        _run(P.enqueue(P.EVENT_ANSWERED, CASE, USER, now=NOW))
        d1, d2 = [int(d["draft_id"]) for d in self.progress_drafts()]
        f = self.open_form(d1)
        r = self.act(d1, dict(f, action="send", text="ご入金を確認いたしました。ありがとうございます。", confirm="1"))
        self.assertIn("done=sent_edited", r.headers["location"])
        self.assertEqual(len(_texts()), 1)
        self.assertEqual(self.exemplars(), [])                                               # 手本候補にしない
        f2 = self.open_form(d2)
        r = self.act(d2, dict(f2, action="reject", op_id=f2["op_id"]))
        self.assertIn("done=rejected", r.headers["location"])
        self.assertEqual(self.approvals(), [])
        self.assertEqual(len(_texts()), 1)

    def test_unresolvable_destination_allows_reject_only(self):
        _run(P.enqueue(P.EVENT_PAID, CASE, USER, now=NOW))
        did = int(self.progress_drafts()[0]["draft_id"])
        self.get_record.return_value = _rec(**{"$id": CASE, "LINEユーザーID": ""})
        r = _client.get(f"{dv.PAGE}/{did}", headers=_auth())
        self.assertIn("却下のみ", r.text)
        self.assertNotIn('name="ticket" value="' + "0", r.text)
        self.assertEqual(_texts(), [])


class TestPins(unittest.TestCase):
    def test_sink_allowlist_wiring_and_no_send_path(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/jikou_progress.py", "hub/jikou_constants.py", "hub/jikou_reply_texts.py", "hub/webapp_drafts_view.py",
                     "hub/jikou_followup.py", "hub/webapp_jikou_result_view.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        for mod in ("jikou_progress", "jikou_constants"):
            self.assertNotIn(mod, allow)
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        self.assertIn("wire_jikou_progress(app)", "\n".join(src[-3:]))
        body = (REPO / "hub" / "jikou_progress.py").read_text(encoding="utf-8").split('"""', 2)[2]
        for bad in ("line_channel", "push_text", "reply_with_push_fallback", "httpx", "sa.delete", "update_record"):
            self.assertNotIn(bad, body)
        self.assertIn(P.register_progress_job, main.app.router.on_startup)
        self.assertIn(P.on_result_recorded, F._result_hooks)
        self.assertEqual((P.JOB_NAME, P.CATEGORY, P.EVENTS), ("JIKOU_PROGRESS_SCAN", "進捗の事実回答", ("paid", "shipped", "answered")))
        self.assertIs(F.FOLLOWUP_DAYS, C.FOLLOWUP_DAYS)
        self.assertEqual((C.FOLLOWUP_DAYS, C.PROGRESS_LOOKBACK_DAYS, C.PROGRESS_SCAN_INTERVAL_MINUTES), ((14, 28), 14, 30))
        self.assertIn("要大野確認", (REPO / "hub" / "jikou_constants.py").read_text(encoding="utf-8"))
        # 進捗連絡は Q4 の画面を使う（新しい画面・route を持たない）
        self.assertFalse([r.path for r in main.app.router.routes if "progress" in getattr(r, "path", "")])


if __name__ == "__main__":
    unittest.main()
