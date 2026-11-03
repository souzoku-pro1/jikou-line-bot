"""JIKOU-REPLY-Q6（QUEUE-2026-10-13-E E-1）: 返信品質の運用計測（/app/reply/metrics・読取専用）と週次要約

固定する仕様:
- 集計元は DB のみ（reply_pending・reply_draft_approval・reply_draft・reply_exemplar・jikou_reply_text）。kintone 呼出ゼロ・書込ゼロ。
- 1) 遅延 = human_replied（human／approved_draft）の resolved_at − created_at。7 日／30 日の 中央値・90%（nearest-rank）・最長。
- 2) 下書き結果 = Q4 記録（record_id draft:）の outcome の件数と率。3) 昇格定型 = jikou_reply_text の ID・昇格日・状態。
- 4) 債権者別 = 下書きの会話 → 手本の creditor_name（社名はそのまま・不明は「（不明）」）。5) 未返信 = open 件数と最古の経過。
- 画面・API は _gate（人のみ）。氏名・LINE userId・line_user_hash・本文を出さない。
- 週次: ジョブ JIKOU_REPLY_METRICS_WEEKLY（daily 9 時・月曜のみ送る）・台帳 key jikou_metrics_weekly:{週開始日}・
  同じ週は 1 通（DUPLICATE → throttled）・全件数 0 は送らない・env で停止。通知は件数と率のみ。
- pin: 新 module の sink 違反ゼロ・allowlist 追加ゼロ・main は末尾結線のみ。
"""

import asyncio
import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa

from test_jikou_reply_q1c import ATTORNEY, USER, USER2, _DbMixin, _auth, _client, _run  # noqa: E402

import main  # noqa: E402
from hub import db  # noqa: E402
from hub import jikou_reply_metrics as M  # noqa: E402
from hub import jikou_reply_promote as pm  # noqa: E402
from hub import reply_draft as rd  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_reply_metrics_view as V  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 10, 13, 3, 0, tzinfo=UTC)          # 火曜 12:00 JST
MONDAY = datetime.datetime(2026, 10, 12, 0, 0, tzinfo=UTC)       # 月曜 09:00 JST
CAT = "手続きの一般的な流れ"
DRAFT_TEXT = "ご相談の手続きは 2〜4 週間程度です。"
CREDITOR = "アコム"


def _h(user: str) -> str:
    return sl.line_user_hash("jikou", user)


def _minutes(n: int) -> datetime.timedelta:
    return datetime.timedelta(minutes=n)


class TestPure(unittest.TestCase):
    def test_stats_and_formats(self):
        self.assertIsNone(M.median([]))
        self.assertIsNone(M.percentile([], 90))
        self.assertEqual(M.median([5, 1, 3]), 3)
        self.assertEqual(M.median([4, 1, 3, 2]), 2.5)
        self.assertEqual(M.percentile(list(range(1, 11)), 90), 9)        # ceil(10*0.9)=9 番目
        self.assertEqual(M.percentile([7], 90), 7)
        lx = M.summarize_latency([10, 20, 30], 7)
        self.assertEqual((lx.days, lx.count, lx.median_min, lx.p90_min, lx.max_min), (7, 3, 20, 30, 30))
        self.assertEqual(M.fmt_minutes(None), "—")
        self.assertEqual(M.fmt_minutes(45), "45分")
        self.assertEqual(M.fmt_minutes(125), "2時間05分")
        self.assertEqual(M.fmt_minutes(60 * 50), "2日2時間")
        self.assertEqual(M.fmt_rate(None), "—")
        self.assertEqual(M.fmt_rate(2 / 3), "67%")
        self.assertEqual(M.week_start_jst(NOW), datetime.date(2026, 10, 12))
        self.assertEqual(M.week_start_jst(MONDAY), datetime.date(2026, 10, 12))
        self.assertTrue(M.is_monday_jst(MONDAY))
        self.assertFalse(M.is_monday_jst(NOW))
        self.assertEqual(M.ledger_key(datetime.date(2026, 10, 12)), "jikou_metrics_weekly:2026-10-12")
        self.assertEqual(M._draft_id("draft:12"), 12)
        self.assertIsNone(M._draft_id("500"))
        self.assertIsNone(M._draft_id("draft:x"))
        self.assertEqual(M._clean_creditor("  レイク  "), "レイク")
        self.assertEqual(M._clean_creditor(None), M.UNKNOWN_CREDITOR)
        with patch.dict(os.environ, {M.DISABLED_ENV: "1"}):
            self.assertTrue(M.disabled())


class _Mixin(_DbMixin):
    def setUp(self):
        super().setUp()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(rd.metadata.create_all)
                await c.run_sync(pm.metadata.create_all)
        _run(_create())
        self.env({"ATTORNEY_LINE_USER_ID": ATTORNEY, "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz_token"})

    # ── seed ───────────────────────────────────────────────────────────────────
    def replied(self, user: str, created: datetime.datetime, delay_min: int, by: str = rp.RESOLVED_BY_HUMAN,
                event: str = "") -> int:
        pid = _run(rp.register(business="jikou", channel="line", line_user_hash=_h(user), route=rp.ROUTE_CUSTOMER,
                               reason=rp.REASON_DEMOTED, inbound_event_id=(event or None), now=created))
        self.assertIsNotNone(pid)
        n = _run(rp.resolve_for_user("jikou", _h(user), resolved_by=by, now=created + _minutes(delay_min)))
        self.assertEqual(n, 1)
        return pid

    def open_pending(self, user: str, created: datetime.datetime, event: str) -> int:
        pid = _run(rp.register(business="jikou", channel="line", line_user_hash=_h(user), route=rp.ROUTE_CUSTOMER,
                               reason=rp.REASON_DEMOTED, inbound_event_id=event, now=created))
        self.assertIsNotNone(pid)
        return pid

    def draft(self, conv_ref: str, outcome: str | None, created: datetime.datetime) -> int:
        did = _run(rd.create(business="jikou", pending_id=None, line_user_hash=_h(USER), conversation_ref=conv_ref,
                             category=CAT, inbound_text="質問です", draft_text=DRAFT_TEXT, guard_ok=True,
                             guard_reasons=[], exemplar_ids=[], now=created))
        self.assertIsNotNone(did)
        with patch.object(rx, "_now", lambda: created):
            self.assertTrue(_run(rx.record_draft(f"draft:{did}", "jikou", CAT, DRAFT_TEXT)))
        if outcome:
            self.assertTrue(_run(rx.set_outcome(f"draft:{did}", outcome)))
        return did

    def exemplar(self, conv_ref: str, creditor: str | None):
        res = _run(rx.save_exemplar(business="jikou", inbound_text="どのくらいかかりますか", human_reply=DRAFT_TEXT,
                                    creditor_name=creditor, stage=rx.STAGE_ENGAGED, conversation_ref=conv_ref,
                                    category_id=CAT))
        self.assertEqual(res, "saved")

    def promoted(self, text_id: str, created: datetime.datetime, status: str = pm.STATUS_ACTIVE):
        async def _ins():
            async with db.session_scope() as s:
                await s.execute(sa.insert(pm.jikou_reply_text).values(
                    text_id=text_id, business="jikou", category=CAT, text=DRAFT_TEXT, text_version="v",
                    source_exemplar_id=None, status=status, created_at=created, updated_at=created))
        _run(_ins())

    def seed(self):
        # 1) 遅延: 7 日内 3 件（30・60・600 分）・30 日内にさらに 1 件（120 分）・31 日超 1 件（対象外）
        self.replied(USER, NOW - datetime.timedelta(days=1), 30, event="e1")
        self.replied(USER, NOW - datetime.timedelta(days=2), 60, by=rp.RESOLVED_BY_APPROVED_DRAFT, event="e2")
        self.replied(USER, NOW - datetime.timedelta(days=3), 600, event="e3")
        self.replied(USER, NOW - datetime.timedelta(days=20), 120, event="e4")
        self.replied(USER, NOW - datetime.timedelta(days=40), 9999, event="e5")
        # 5) 未返信: 2 件（最古 3 日）
        self.open_pending(USER2, NOW - datetime.timedelta(days=3), "o1")
        self.open_pending(USER2, NOW - datetime.timedelta(hours=1), "o2")
        # 2)/4) 下書き: 会話 c1（アコム）asis・edited・rejected・未決、会話 c2（不明）asis、40 日前 rejected（全期間のみ）
        self.exemplar("c1", CREDITOR)
        self.d1 = self.draft("c1", rx.OUTCOME_APPROVED_ASIS, NOW - datetime.timedelta(days=1))
        self.draft("c1", rx.OUTCOME_APPROVED_EDITED, NOW - datetime.timedelta(days=2))
        self.draft("c1", rx.OUTCOME_REJECTED, NOW - datetime.timedelta(days=3))
        self.draft("c1", None, NOW - datetime.timedelta(days=4))
        self.draft("c2", rx.OUTCOME_APPROVED_ASIS, NOW - datetime.timedelta(days=5))
        self.draft("c1", rx.OUTCOME_REJECTED, NOW - datetime.timedelta(days=40))
        # 3) 昇格定型 2 件（1 件退役）
        self.promoted("J-U1", NOW - datetime.timedelta(days=10))
        self.promoted("J-U2", NOW - datetime.timedelta(days=5), status=pm.STATUS_RETIRED)


class TestCollect(_Mixin):
    def test_collect_values(self):
        self.seed()
        m = _run(M.collect(now=NOW))
        l7, l30 = m.latency
        self.assertEqual((l7.days, l7.count, l7.median_min, l7.p90_min, l7.max_min), (7, 3, 60.0, 600.0, 600.0))
        self.assertEqual((l30.days, l30.count, l30.median_min, l30.max_min), (30, 4, 90.0, 600.0))
        d30, dall = m.drafts
        self.assertEqual((d30.days, d30.total, d30.asis, d30.edited, d30.rejected), (30, 4, 2, 1, 1))
        self.assertEqual((dall.days, dall.total, dall.asis, dall.edited, dall.rejected), (None, 5, 2, 1, 2))
        self.assertEqual(M.fmt_rate(d30.asis_rate), "50%")
        self.assertEqual([(p.text_id, p.promoted_on, p.status) for p in m.promoted],
                         [("J-U1", "2026-10-03", "active"), ("J-U2", "2026-10-08", "retired")])
        self.assertEqual([(c.creditor, c.drafts, c.decided, c.approved) for c in m.creditors],
                         [(CREDITOR, 5, 4, 2), (M.UNKNOWN_CREDITOR, 1, 1, 1)])
        self.assertEqual(m.creditors[0].approval_rate, 0.5)
        self.assertEqual(m.open_count, 2)
        self.assertEqual(m.oldest_open_min, 3 * 24 * 60.0)
        self.assertTrue(M.has_any(m))
        self.assertEqual(self.app21.call_count, 0)                     # kintone 呼出ゼロ

    def test_empty_db(self):
        m = _run(M.collect(now=NOW))
        self.assertEqual([x.count for x in m.latency], [0, 0])
        self.assertEqual([d.total for d in m.drafts], [0, 0])
        self.assertEqual((m.promoted, m.creditors, m.open_count, m.oldest_open_min), ((), (), 0, None))
        self.assertFalse(M.has_any(m))

    def test_notice_text_has_counts_only(self):
        self.seed()
        m = _run(M.collect(now=NOW))
        t = M.notice_text(m, M.week_start_jst(NOW))
        self.assertTrue(t.startswith(M.NOTICE_HEADER + "2026-10-12 週"))
        self.assertIn("返信遅延 7日: 3件 中央値 1時間00分／90% 10時間00分／最長 10時間00分", t)
        self.assertIn("AI下書き 30日: 4件 無修正 50%／修正 25%／却下 25%", t)
        self.assertIn("昇格定型: 1件（累計 2件）", t)
        self.assertIn("未返信: 2件 最古 3日0時間", t)
        for bad in (USER, USER2, _h(USER), _h(USER2), DRAFT_TEXT, "J-U1", CREDITOR):
            self.assertNotIn(bad, t)


class TestPageAndApi(_Mixin):
    def test_requires_auth(self):
        r = _client.get(V.PAGE, follow_redirects=False)
        self.assertIn(r.status_code, (302, 303, 401))
        r = _client.get(V.API, follow_redirects=False)
        self.assertIn(r.status_code, (302, 303, 401))

    def test_page_and_api_values_without_pii(self):
        self.seed()
        with patch.object(M, "_now", lambda: NOW):
            r = _client.get(V.PAGE, headers=_auth())
            j = _client.get(V.API, headers=_auth()).json()
        self.assertEqual(r.status_code, 200)
        body = r.text
        for ok in ("時効 返信計測", "J-U1", "J-U2", "退役", CREDITOR, M.UNKNOWN_CREDITOR, "2026-10-03", "直近 7 日", "全期間"):
            self.assertIn(ok, body)
        for bad in (USER, USER2, _h(USER), _h(USER2), DRAFT_TEXT, "どのくらいかかりますか"):
            self.assertNotIn(bad, body)
        self.assertTrue(j["ok"])
        self.assertEqual(j["open"], {"count": 2, "oldest_minutes": 3 * 24 * 60.0})
        self.assertEqual(j["latency"][0]["count"], 3)
        self.assertEqual(j["drafts"][0]["total"], 4)
        self.assertEqual([p["text_id"] for p in j["promoted"]], ["J-U1", "J-U2"])
        self.assertEqual(j["creditors"][0]["creditor"], CREDITOR)
        self.assertEqual(self.app21.call_count, 0)
        self.assertEqual(self.chatlog.call_count, 0)
        self.assertEqual(self.biz.call_count, 0)                      # 画面表示で通知しない

    def test_page_db_failure_is_fixed_notice(self):
        with patch.object(M, "collect", side_effect=RuntimeError("x")):
            r = _client.get(V.PAGE, headers=_auth())
            j = _client.get(V.API, headers=_auth()).json()
        self.assertEqual(r.status_code, 200)
        self.assertIn(V._NOTICE_DB, r.text)
        self.assertNotIn("RuntimeError", r.text)
        self.assertEqual(j, {"ok": False, "reason": "db"})


class TestWeekly(_Mixin):
    def _ops(self):
        return self._rows(sl.send_operation)

    def test_monday_sends_once_per_week_counts_only(self):
        self.seed()
        with patch.object(M, "_now", lambda: MONDAY):
            self.assertEqual(_run(M.weekly_check()), {"notice": rp.NOTIFY_SENT})
            self.assertEqual(_run(M.weekly_check()), {"notice": rp.NOTIFY_THROTTLED})     # 同じ週 2 回目は送らない
        self.assertEqual(self.biz.call_count, 1)
        to, text = self.biz.call_args.args[0], self.biz.call_args.args[1]
        self.assertEqual(to, ATTORNEY)
        self.assertTrue(text.startswith(M.NOTICE_HEADER + "2026-10-12 週"))
        for bad in (USER, USER2, _h(USER), DRAFT_TEXT, CREDITOR):
            self.assertNotIn(bad, text)
        ops = self._ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["business"], ops[0]["channel"], ops[0]["purpose"], ops[0]["inbound_event_id"],
                          ops[0]["state"]), ("jikou", "jikou_notice", sl.PURPOSE_OTHER, "jikou_metrics_weekly:2026-10-12",
                                              sl.STATE_SENT))
        self.assertEqual(self.app21.call_count, 0)

    def test_not_monday_disabled_and_empty(self):
        self.seed()
        with patch.object(M, "_now", lambda: NOW):
            self.assertEqual(_run(M.weekly_check()), {"notice": M.SEND_NOT_MONDAY})
        with patch.object(M, "_now", lambda: MONDAY), patch.dict(os.environ, {M.DISABLED_ENV: "1"}):
            self.assertEqual(_run(M.weekly_check()), {"notice": M.SEND_DISABLED})
        self.assertEqual(self.biz.call_count, 0)
        self.assertEqual(self._ops(), [])

    def test_all_zero_week_is_not_sent(self):
        with patch.object(M, "_now", lambda: MONDAY):
            self.assertEqual(_run(M.weekly_check()), {"notice": M.SEND_NONE})
        self.assertEqual(self.biz.call_count, 0)
        self.assertEqual(self._ops(), [])

    def test_collect_failure_is_failed_without_exception(self):
        with patch.object(M, "_now", lambda: MONDAY), patch.object(M, "collect", side_effect=RuntimeError("x")):
            self.assertEqual(_run(M.weekly_check()), {"notice": rp.NOTIFY_FAILED})
        self.assertEqual(self.biz.call_count, 0)

    def test_job_registration(self):
        async def _reg():
            M.register_jikou_metrics_job()
            M.register_jikou_metrics_job()                  # 冪等
            return hub_scheduler.is_registered(M.JOB_NAME), hub_scheduler._jobs[M.JOB_NAME]
        try:
            ok, job = asyncio.run(_reg())
            self.assertTrue(ok)
            self.assertEqual((job.kind, job.hour_jst), ("daily", 9))
        finally:
            # 自分の登録だけ外す（stop_all は他票の登録〔SEND_LEDGER_RECOVER 等〕まで消し、同一 worker の後続テストを壊す）
            mine = hub_scheduler._jobs.pop(M.JOB_NAME, None)
            if mine is not None and mine.task is not None and not mine.task.done():
                mine.task.cancel()


class TestPins(unittest.TestCase):
    def test_sink_allowlist_and_wiring(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/jikou_reply_metrics.py", "hub/webapp_reply_metrics_view.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("jikou_reply_metrics", allow)
        self.assertNotIn("webapp_reply_metrics_view", allow)
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        # 末尾追記（3 行）。JIKOU-FOLLOWUP-1（QUEUE-2026-10-16-R F-1）が同じ main.py 末尾へ追記したため、窓を「Q6 の見出し
        # コメント以降 3 行」へ移した（検査の意図＝Q6 の結線は見出し直後の 2 行だけで本文中に置かない、は不変）。
        q6 = next(i for i, line in enumerate(src) if line.startswith("# ── JIKOU-REPLY-Q6（QUEUE-2026-10-13-E E-1）"))
        self.assertIn("wire_reply_metrics(app)", "\n".join(src[q6:q6 + 3]))
        vsrc = (REPO / "hub" / "webapp_reply_metrics_view.py").read_text(encoding="utf-8")
        self.assertIn("app.router.on_startup.append(M.register_jikou_metrics_job)", vsrc)
        self.assertIn(M.register_jikou_metrics_job, main.app.router.on_startup)
        self.assertEqual(M.JOB_NAME, "JIKOU_REPLY_METRICS_WEEKLY")
        self.assertEqual(M.RUN_HOUR_JST, 9)
        self.assertEqual(M.WINDOWS, (7, 30))
        msrc = (REPO / "hub" / "jikou_reply_metrics.py").read_text(encoding="utf-8")
        self.assertNotIn("kintone", msrc.split('"""', 2)[2])          # 本文に kintone 呼出なし
        self.assertNotIn("sa.insert", msrc)
        self.assertNotIn("sa.update", msrc)
        self.assertNotIn("sa.delete", msrc)


if __name__ == "__main__":
    unittest.main()
