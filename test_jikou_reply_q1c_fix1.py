"""JIKOU-REPLY-Q1c-fix1（Codex R-Q1c BC-01〜05・司令塔裁定）: fail-closed・migration 単一 head・無言受信の回収・固定集合の解除・J-0 の記録

固定する仕様:
- BC-01 V2 ON の bot／承認済み下書きは、会話状態または送信台帳が確認できない（begin()=None・会話行の読取失敗）ときは送らない。
  J-0 も送らない。業務 LINE へ障害通知 1 日 1 回（台帳不可なら固定ログのみ）。V2 OFF は従来どおり fail-open。
- BC-02 migration は 1 本の鎖（… → c8e0 → a1c3 → d4f6）・head は d4f6 のみ・データがあれば downgrade 拒否。
- BC-03 対応待ちの保存失敗は永続キュー（受信イベント ID）→ 回収ジョブで register → 通知。通知失敗は台帳 key で再試行（1 通のみ）。
  未返信の経過通知は 1 日 2 回・件数と経過分のみ。
- BC-04 送信着手時点で固定した pending_id 集合だけを解除（送信中に届いた受信は open）。
- BC-05 J-0 の送信済み記録（j0_sent）は台帳 finish=applied の後だけ。失敗・結果不明・停止は別状態。App 28 への写しも確定した文だけ。
  main の送信ラッパーは戻り値を返す。
"""

import asyncio
import datetime
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import (_DbMixin, _FakeClient, _FakeResp, _client, _compose, _rec, _run, _texts,  # noqa: E402
                                  ATTORNEY, JIKOU, T0, USER, USER2)

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
NOW = datetime.datetime(2026, 10, 21, 3, 0, tzinfo=datetime.timezone.utc)      # 12:00 JST
RETRY = "d4f6a8b0c2e4"
PENDING = "a1c3e5f7b9d2"


def _begin_none_for(channels):
    """send_ledger.begin を包み、指定チャネルだけ None（台帳不可）を返す（他は本物）。"""
    real = sl.begin

    async def fake(business, channel, user_id, text):
        if channel in channels:
            return None
        return await real(business, channel, user_id, text)
    return fake


class _Fix1Mixin(_DbMixin):
    def setUp(self):
        super().setUp()
        sp.clear_outage_notice_cache()

    def tearDown(self):
        sp.clear_outage_notice_cache()
        super().tearDown()

    def retry_rows(self) -> list:
        return self._rows(rp.reply_pending_retry)

    def reg(self, user=USER, **kw):
        base = dict(business="jikou", channel="jikou", line_user_hash=sl.line_user_hash("jikou", user),
                    route="customer", reason="demoted", category="費用交渉・減額相談", approval_record_id="500")
        base.update(kw)
        return _run(rp.register(**base))

    def assistant_logs(self) -> list:
        return [c.args[2] for c in self.chatlog.await_args_list if c.args[1] == "assistant"]


# ── 1. BC-01: 会話状態・台帳が確認できないときは送らない（V2 ON）／OFF は従来 ──────────────────
class TestFailClosedWhenStateUnavailable(_Fix1Mixin):
    def test_on_bot_does_not_send_while_attending_and_ledger_unavailable_notice_once_a_day(self):
        self.on()
        self.touch("e1")
        _run(sl.set_attending("jikou", USER))
        with patch.object(sl, "begin", _begin_none_for({"jikou"})):
            self.assertEqual(self.run_r(line_channel.push_text(JIKOU, USER, "x")),
                             (sl.SEND_POLICY_STOPPED, (sp.REASON_LEDGER_UNAVAILABLE,)))
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "x")), sl.SEND_POLICY_STOPPED)
            with sl.purpose(sl.PURPOSE_FIRST_REPLY):                    # J-0 も送らない
                self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, sp.J0_TEXT)),
                                 sl.SEND_POLICY_STOPPED)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.biz.await_count, 1)                       # 障害通知は 1 日 1 回（3 回止めても 1 通）
        text = self.biz.await_args.args[1]
        self.assertIn(sp.OUTAGE_NOTICE_HEADER, text)
        self.assertNotIn(USER, text)
        self.assertEqual([o["channel"] for o in self.ops()], ["jikou_notice"])   # 顧客チャネルの台帳行は作らない
        self.assertEqual(sp.outage_key(datetime.date(2026, 10, 21)), "policy_outage:2026-10-21")

    def test_on_whole_db_failure_is_silent_and_inbound_is_stopped_with_fixed_log_only(self):
        self.on()
        self.touch("e1")
        _run(sl.set_attending("jikou", USER))
        with patch.object(sl, "_latest_conversation", AsyncMock(side_effect=RuntimeError("db down"))):
            self.assertEqual(self.run_r(line_channel.push_text(JIKOU, USER, "x")),
                             (sl.SEND_POLICY_STOPPED, (sp.REASON_LEDGER_UNAVAILABLE,)))
            # 受信入口: 会話状態（対応中）が読めない＝停止（attending:unknown）
            self.assertEqual(_run(main._inbound_stop_kind(USER, _rec(response_mode="自動"))), main.STOP_POLICY)
            self.assertIn(sp.REASON_ATTENDING_UNKNOWN, _run(sp.bot_stop_reasons(USER)))
        self.assertEqual(_FakeClient.calls, [])
        self.biz.assert_not_awaited()                                   # 台帳も不可 → 通知は固定ログのみ
        # 復旧後の最初の停止で通知 1 回
        with patch.object(sl, "begin", _begin_none_for({"jikou"})):
            self.assertEqual(_run(line_channel.push_text(JIKOU, USER, "x")), sl.SEND_POLICY_STOPPED)
        self.assertEqual(self.biz.await_count, 1)

    def test_on_approved_draft_is_not_sent_when_ledger_unavailable(self):
        self.on()

        async def _draft():
            async with sl.approved_draft("approval:9:1"):
                return await line_channel.push_text(JIKOU, USER, "draft")
        with patch.object(sl, "begin", _begin_none_for({"jikou"})):
            self.assertEqual(self.run_r(_draft()), (sl.SEND_POLICY_STOPPED, (sp.REASON_LEDGER_UNAVAILABLE,)))
        self.assertEqual(_FakeClient.calls, [])

    def test_on_webhook_keeps_sent_no_and_notifies_when_ledger_unavailable(self):
        self.on()
        self.env({"KINTONE_WEBHOOK_TOKEN": "kintone-token"})
        os.environ.pop("KINTONE_EVENT_DEDUP_ENABLED", None)
        record = {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}, "line_user_id": {"value": USER},
                  "AI下書き": {"value": "下書き"}, "カテゴリ": {"value": "c"}, "$revision": {"value": "3"}}
        body = {"record": {"$id": {"value": "7"}, "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}}}
        mark = AsyncMock()
        with patch.object(sl, "begin", _begin_none_for({"jikou"})), \
             patch.object(main, "mark_approval_sent", mark), \
             patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=record)):
            r = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual((r.status_code, r.json().get("skip")), (200, "policy_stopped"))
        mark.assert_not_awaited()
        self.assertEqual([j["to"] for u, j in _FakeClient.calls if "push" in u], [])
        texts = [c.args[1] for c in self.biz.await_args_list]
        self.assertEqual(len(texts), 2)                                 # 障害通知 1 ＋ 承認済み下書き未送信 1（1 承認版 1 通）
        self.assertTrue(any(sp.REASON_LEDGER_UNAVAILABLE in t for t in texts))

    def test_off_keeps_fail_open_when_ledger_unavailable(self):
        _run(sl.set_attending("jikou", USER))
        with patch.object(sl, "begin", AsyncMock(return_value=None)):
            self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "x")), sl.SEND_SENT)
        self.assertEqual(len(_FakeClient.calls), 2)
        self.biz.assert_not_awaited()
        self.assertEqual(_run(main._inbound_stop_kind(USER, _rec(response_mode="自動"))), "")

    def test_on_human_explicit_send_keeps_q1b_fail_closed_without_outage_notice(self):
        self.on()
        with patch.object(sl, "begin", AsyncMock(return_value=None)):
            res = _run(line_channel.push_text_explicit(JIKOU, "f" * 32, USER, "人の返信"))
        self.assertEqual(res.outcome, line_channel.PUSH_LEDGER_UNAVAILABLE)
        self.assertEqual(_FakeClient.calls, [])
        self.biz.assert_not_awaited()


# ── 2. BC-02: migration の単一 head・鎖・往復拒否 ─────────────────────────────────────
class TestMigrationChain(unittest.TestCase):
    def test_single_head_and_linear_chain(self):
        """単一 head・鎖は 1 本（alembic CLI で読む＝アプリ側テストから alembic を import しない方針 D2 を守る）。"""
        import re as _re
        from test_db_foundation import TestAlembicScaffold
        d = tempfile.mkdtemp(prefix="reply_pending_retry_heads_")
        alembic, _tables, _u, _n = TestAlembicScaffold._brain_mig_tools(f"{d}/mig.db")
        try:
            heads = alembic("heads")
            self.assertEqual(heads.returncode, 0, heads.stderr[-800:])
            ids = _re.findall(r"^([0-9a-f]{12})", heads.stdout, _re.M)
            self.assertEqual(len(ids), 1, heads.stdout)                 # 単一 head（後続票の migration は d4f6 の後ろに並ぶ）
            hist = alembic("history")
            self.assertEqual(hist.returncode, 0, hist.stderr[-800:])
            down_of = {m.group(2): m.group(1) for m in _re.finditer(r"^([0-9a-f]{12}|<base>) -> ([0-9a-f]{12})", hist.stdout, _re.M)}
            chain, rev = [], ids[0]
            while rev and rev != "<base>":
                chain.append(rev)
                rev = down_of.get(rev)
            i = chain.index(RETRY)
            self.assertEqual(chain[i:i + 6], [RETRY, PENDING, "c8e0a2b4d6f8", "a7c9e1f3b5d7", "f7b2c9d4e1a6", "e5f8a1b4c7d9"])
        finally:
            shutil.rmtree(d, ignore_errors=True)
        src = (REPO / "alembic" / "versions" / "20261012_a1c3e5f7b9d2_reply_pending.py").read_text(encoding="utf-8")
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'c8e0a2b4d6f8'", src)

    def test_round_trip_and_downgrade_refused_when_queue_has_data(self):
        from test_db_foundation import TestAlembicScaffold
        d = tempfile.mkdtemp(prefix="reply_pending_retry_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, _unique_cols, nullable = TestAlembicScaffold._brain_mig_tools(dbfile)
        try:
            up = alembic("upgrade", RETRY)                        # 後続票の head に依存しない（d4f6 まで）
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(RETRY, alembic("current").stdout)
            self.assertIn(rp.RETRY_TABLE_NAME, tables())
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute(f'PRAGMA table_info("{rp.RETRY_TABLE_NAME}")')}
                self.assertEqual(cols, {c.name for c in rp.reply_pending_retry.columns})
                nn = nullable(con, rp.RETRY_TABLE_NAME)
                self.assertFalse(nn["inbound_event_id"])
                self.assertTrue(nn["pending_id"])
                con.execute("INSERT INTO reply_pending_retry (business, channel, inbound_event_id, line_user_hash, route, reason, "
                            "state, attempts, created_at, updated_at) VALUES ('jikou', 'jikou', 'ev-secret', 'h', 'customer', "
                            "'demoted', 'queued', 0, '2026-10-21 00:00:00', '2026-10-21 00:00:00')")
                con.commit()
            finally:
                con.close()
            down = alembic("downgrade", PENDING)
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("reply_pending_retry_downgrade_refused_data_present", down.stderr)
            self.assertNotIn("ev-secret", down.stderr)
            self.assertIn(RETRY, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                con.execute("DELETE FROM reply_pending_retry")
                con.commit()
            finally:
                con.close()
            down = alembic("downgrade", PENDING)
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
            self.assertNotIn(rp.RETRY_TABLE_NAME, tables())
            self.assertTrue(set(rp.TABLE_NAMES) <= tables())            # 対応待ちの表は残る
            self.assertIn(PENDING, alembic("current").stdout)
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
        finally:
            shutil.rmtree(d, ignore_errors=True)


# ── 3. BC-03: 永続キュー・通知の再試行・経過通知 ─────────────────────────────────────────
class TestRetryQueueAndNotices(_Fix1Mixin):
    def customer_bound(self, event_id: str, result: dict):
        """受信イベント ID を束ねた顧客対応経路（durable lane と同じ文脈）。"""
        async def _w():
            tok = sl.bind_inbound(event_id)
            try:
                with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=result)):
                    await cr.handle_customer_message(user_id=USER, user_message="質問です", reply_token="rt",
                                                     app21_record=self.app21.return_value, reply_func=self.reply_func())
            finally:
                sl.unbind(tok)
        _run(_w())

    def test_register_failure_is_queued_and_reprocessed_by_job(self):
        self.on()
        self.touch("e-q1")
        with patch.object(rp, "register", AsyncMock(return_value=None)):
            self.customer_bound("e-q1", _compose("x", category="法的判断・見通し", auto_send=False))
        self.assertEqual(self.pending(), [])
        self.assertEqual(_texts(), [])                                  # J-0 も送らない
        self.biz.assert_not_awaited()
        q = self.retry_rows()
        self.assertEqual(len(q), 1)
        self.assertEqual((q[0]["inbound_event_id"], q[0]["state"], q[0]["route"], q[0]["reason"],
                          q[0]["approval_record_id"], q[0]["category"], q[0]["attempts"]),
                         ("e-q1", "queued", "customer", "demoted", "500", "法的判断・見通し", 0))
        self.assertEqual(q[0]["line_user_hash"], sl.line_user_hash("jikou", USER))
        self.assertNotIn(USER, json.dumps(q, default=str))
        # 回収ジョブ: register → 通知
        res = _run(sp.run_pending_retry_job())
        self.assertEqual((res["requeued"], res["registered"], res["register_failed"]), (1, 1, 0))
        rows = self.pending()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["route"], rows[0]["reason"], rows[0]["approval_record_id"], rows[0]["category"],
                          rows[0]["state"], rows[0]["notify_state"], rows[0]["customer_send"], rows[0]["inbound_event_id"]),
                         ("customer", "demoted", "500", "法的判断・見通し", "open", "sent", "none", "e-q1"))
        self.assertEqual(self.biz.await_count, 1)
        self.assertIn("No.500", self.biz.await_args.args[1])
        q = self.retry_rows()
        self.assertEqual((q[0]["state"], q[0]["pending_id"]), ("done", rows[0]["pending_id"]))
        # 2 回目のジョブ: 対象なし・通知も増えない
        res = _run(sp.run_pending_retry_job())
        self.assertEqual((res["requeued"], res["notify_retried"]), (0, 0))
        self.assertEqual(len(self.pending()), 1)
        self.assertEqual(self.biz.await_count, 1)
        # 再配送（同じ受信イベント ID）の enqueue は冪等
        self.assertTrue(_run(rp.enqueue_retry(business="jikou", channel="jikou", inbound_event_id="e-q1",
                                              line_user_hash="h", route="customer", reason="demoted")))
        self.assertEqual(len(self.retry_rows()), 1)

    def test_register_failure_without_event_id_is_not_queued(self):
        self.on()
        with patch.object(rp, "register", AsyncMock(return_value=None)):
            self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        self.assertEqual(self.retry_rows(), [])
        self.assertEqual(_texts(), [])
        self.assertFalse(_run(rp.enqueue_retry(business="jikou", channel="jikou", inbound_event_id=None,
                                               line_user_hash="h", route="customer", reason="demoted")))

    def test_queue_is_abandoned_after_max_attempts(self):
        self.assertTrue(_run(rp.enqueue_retry(business="jikou", channel="jikou", inbound_event_id="e-a",
                                              line_user_hash=sl.line_user_hash("jikou", USER), route="inbound",
                                              reason="policy_stopped")))
        with patch.object(rp, "register", AsyncMock(return_value=None)):
            for _ in range(rp.RETRY_MAX_ATTEMPTS):
                _run(sp.run_pending_retry_job())
        q = self.retry_rows()
        self.assertEqual((q[0]["state"], q[0]["attempts"], q[0]["last_error"]),
                         ("abandoned", rp.RETRY_MAX_ATTEMPTS, rp.RETRY_ERROR_REGISTER))
        self.assertEqual(_run(sp.run_pending_retry_job())["requeued"], 0)
        self.assertEqual(_run(rp.count_retry_queue()), 0)

    def test_notify_failure_is_retried_once_via_ledger_key(self):
        self.on()
        self.touch("e1")
        self.biz.return_value = False
        self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        rows = self.pending()
        self.assertEqual((rows[0]["notify_state"], rows[0]["customer_send"], rows[0]["attempts"]), ("failed", "j0_sent", 0))
        self.biz.return_value = True
        res = _run(sp.run_pending_retry_job())
        self.assertEqual(res["notify_retried"], 1)
        rows = self.pending()
        self.assertEqual((rows[0]["notify_state"], rows[0]["attempts"]), ("sent", 1))
        self.assertEqual(self.biz.await_count, 2)
        # 同じ台帳 key（登録日）で再開した 1 操作＝attempt_no 2 の sent（新しい操作は作らない）
        self.assertEqual([(o["state"], o["attempt_no"]) for o in self.ops() if o["channel"] == "jikou_notice"],
                         [("sent", 2)])
        # もう一度: 対象なし
        self.assertEqual(_run(sp.run_pending_retry_job())["notify_retried"], 0)
        self.assertEqual(self.biz.await_count, 2)

    def test_notify_unavailable_is_retried_and_capped(self):
        self.on()
        self.touch("e1")
        with patch.object(sl, "begin", _begin_none_for({"jikou_notice"})):
            self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        rows = self.pending()
        self.assertEqual((rows[0]["notify_state"], rows[0]["customer_send"]), ("unavailable", "j0_sent"))
        self.biz.assert_not_awaited()
        self.assertEqual(_run(sp.run_pending_retry_job())["notify_retried"], 1)
        self.assertEqual((self.pending()[0]["notify_state"], self.biz.await_count), ("sent", 1))
        # 上限: attempts が NOTIFY_RETRY_MAX に達した行は対象外（通知は増えない）
        pid = self.reg(user=USER2, inbound_event_id="e-cap")
        _run(rp.set_notify_state(pid, rp.NOTIFY_FAILED))
        for _ in range(rp.NOTIFY_RETRY_MAX):
            _run(rp.bump_attempts(pid))
        self.assertEqual(_run(sp.run_pending_retry_job())["notify_retried"], 0)
        self.assertEqual(self.biz.await_count, 1)

    def test_age_notice_twice_a_day_with_counts_only(self):
        self.on()
        a = self.reg(inbound_event_id="e-old1", now=NOW - datetime.timedelta(minutes=90))
        b = self.reg(inbound_event_id="e-old2", now=NOW - datetime.timedelta(minutes=61))
        c = self.reg(user=USER2, inbound_event_id="e-new", now=NOW - datetime.timedelta(minutes=5))
        res = _run(sp.run_age_notice_job(9, now=NOW))
        self.assertEqual((res["over_count"], res["queued"], res["notice"]), (2, 0, rp.NOTIFY_SENT))
        self.assertEqual(self.biz.await_count, 1)
        text = self.biz.await_args.args[1]
        self.assertIn(sp.AGE_NOTICE_HEADER, text)
        self.assertIn("対応待ち 3 件", text)
        self.assertIn("60 分超 2 件", text)
        self.assertIn("最長 90 分", text)
        self.assertNotIn(USER, text)
        self.assertNotIn(sl.line_user_hash("jikou", USER)[:16], text)
        # 同じ枠は 1 通（台帳 key）・別の枠は送る＝1 日 2 回
        self.assertEqual(_run(sp.run_age_notice_job(9, now=NOW))["notice"], rp.NOTIFY_THROTTLED)
        self.assertEqual(self.biz.await_count, 1)
        self.assertEqual(_run(sp.run_age_notice_job(15, now=NOW))["notice"], rp.NOTIFY_SENT)
        self.assertEqual(self.biz.await_count, 2)
        # 対象 0 件（閾値超なし・再処理待ちなし）なら送らない
        for pid in (a, b, c):
            _run(rp.mark_not_needed(pid))
        nxt = NOW + datetime.timedelta(days=1)
        self.assertEqual(_run(sp.run_age_notice_job(9, now=nxt)), {"over_count": 0, "queued": 0, "notice": ""})
        self.assertEqual(self.biz.await_count, 2)
        # 閾値は env で上書き（要大野確認・既定 60）: 5 分前の受信は既定では対象外・3 分なら対象
        self.reg(user=USER2, inbound_event_id="e-new2", now=nxt - datetime.timedelta(minutes=5))
        self.assertEqual(sp.age_minutes(), 60)
        self.assertEqual(_run(sp.run_age_notice_job(9, now=nxt))["over_count"], 0)
        with patch.dict(os.environ, {sp.REPLY_PENDING_AGE_MIN_ENV: "3"}):
            self.assertEqual(sp.age_minutes(), 3)
            self.assertEqual(_run(sp.run_age_notice_job(9, now=nxt))["over_count"], 1)
        self.assertEqual(self.biz.await_count, 3)

    def test_jobs_are_registered_from_main_tail_idempotently(self):
        self.assertEqual(sl._recover_hooks.count(sp.run_pending_retry_job), 1)
        for h in sp.AGE_NOTICE_HOURS_JST:
            self.assertTrue(hub_scheduler.is_registered(sp.age_job_name(h)), h)
        sp.register_jobs()
        self.assertEqual(sl._recover_hooks.count(sp.run_pending_retry_job), 1)
        self.assertEqual(sp.AGE_NOTICE_HOURS_JST, (9, 15))
        self.assertEqual(sp.age_job_name(9), "JIKOU_REPLY_PENDING_AGE_09")


# ── 4. BC-04: 送信着手時点で固定した集合だけを解除 ─────────────────────────────────────
class TestFixedTargets(_Fix1Mixin):
    def _post_approval(self, push):
        record = {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}, "line_user_id": {"value": USER},
                  "AI下書き": {"value": "下書き"}, "カテゴリ": {"value": "c"}, "$revision": {"value": "3"}}
        body = {"record": {"$id": {"value": "7"}, "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}}}
        with patch.object(main, "send_line_push", push), \
             patch.object(main, "mark_approval_sent", AsyncMock()), \
             patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=record)):
            return _client.post("/webhook/kintone/approval?token=kintone-token", json=body)

    def test_approved_draft_resolves_only_targets_fixed_at_send_start(self):
        self.on()
        self.env({"KINTONE_WEBHOOK_TOKEN": "kintone-token"})
        os.environ.pop("KINTONE_EVENT_DEDUP_ENABLED", None)
        a = self.reg(inbound_event_id="ev-a")
        arrived: dict = {}

        async def push_while_b_arrives(user_id, text):               # 返信 A の送信中に受信 B が対応待ちに入る
            arrived["b"] = await rp.register(business="jikou", channel="jikou",
                                             line_user_hash=sl.line_user_hash("jikou", USER), route="customer",
                                             reason="demoted", inbound_event_id="ev-b", approval_record_id="7")
            return sl.SEND_SENT
        r = self._post_approval(push_while_b_arrives)
        self.assertEqual(r.json().get("record_id"), "7")
        states = {x["pending_id"]: (x["state"], x["resolved_by"]) for x in self.pending()}
        self.assertEqual(states[a], ("human_replied", "approved_draft"))
        self.assertEqual(states[arrived["b"]], ("open", None))         # B は open のまま
        # 次の送信（着手時点で B が open）で B が閉じる
        r = self._post_approval(AsyncMock(return_value=sl.SEND_SENT))
        self.assertEqual(r.json().get("record_id"), "7")
        self.assertEqual(self.pending()[1]["state"], "human_replied")

    def test_human_reply_resolves_only_fixed_targets_and_only_same_user(self):
        a = self.reg(inbound_event_id="ev-a")
        targets = _run(sp.pending_targets(USER))
        self.assertEqual(targets, (a,))
        b = self.reg(inbound_event_id="ev-b")                          # 送信中に到着
        self.assertEqual(_run(sp.human_replied(USER, targets)), 1)
        states = {x["pending_id"]: (x["state"], x["resolved_by"]) for x in self.pending()}
        self.assertEqual((states[a], states[b]), (("human_replied", "human"), ("open", None)))
        c = self.reg(user=USER2, inbound_event_id="ev-c")
        self.assertEqual(_run(sp.human_replied(USER, (b, c))), 1)      # 別人の ID が混じっても閉じない
        self.assertEqual({x["pending_id"]: x["state"] for x in self.pending()}[c], "open")
        self.assertEqual(_run(sp.human_replied(USER, ())), 0)
        with patch.object(rp, "open_ids_for_user", AsyncMock(side_effect=RuntimeError("x"))):
            self.assertEqual(_run(sp.pending_targets(USER)), ())        # 読取失敗は空＝何も閉じない
        self.assertEqual([h["to"] for h in _run(rp.history(a))], ["open", "human_replied"])

    def test_reply_view_fixes_targets_before_explicit_send(self):
        src = (REPO / "hub" / "webapp_reply_view.py").read_text(encoding="utf-8")
        self.assertLess(src.index("pd_targets = await send_policy.pending_targets(user_id)"),
                        src.index("result = await line_channel.push_text_explicit("))
        self.assertEqual(src.count("send_policy.human_replied(user_id, pd_targets)"), 1)
        self.assertNotIn("resolve_for_user", (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8"))


# ── 5. BC-05: J-0 の送信済み記録は台帳 finish=applied の後だけ ──────────────────────────
class TestJ0Record(_Fix1Mixin):
    def test_j0_failed_is_not_recorded_as_sent_and_not_copied_to_chatlog(self):
        self.on()
        self.touch("e1")
        _FakeClient.responses = [_FakeResp(500, "err"), _FakeResp(500, "err")]   # reply 失敗 → push fallback も失敗
        self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        rows = self.pending()
        self.assertEqual((rows[0]["customer_send"], rows[0]["state"], rows[0]["notify_state"]), ("j0_failed", "open", "sent"))
        self.assertEqual(self.assistant_logs(), [])                     # 届いていない文は App 28 に写さない
        self.assertEqual([(o["purpose"], o["state"]) for o in self.ops() if o["channel"] == "jikou"],
                         [("first_reply", "failed")])
        # 次の受信: failed は在庫検査の対象外（届いていない）＝J-0 を改めて 1 通・今度は sent
        _FakeClient.calls.clear()
        self.customer(_compose("y", category="法的判断・見通し", auto_send=False))
        self.assertEqual(_texts(), [sp.J0_TEXT])
        self.assertEqual(self.pending()[1]["customer_send"], "j0_sent")
        self.assertEqual(self.assistant_logs(), [sp.J0_TEXT])

    def test_j0_unconfirmed_on_exception_is_recorded_as_unconfirmed(self):
        self.on()
        self.touch("e1")
        _FakeClient.raise_exc = RuntimeError("boom")
        with self.assertRaises(RuntimeError):
            self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        rows = self.pending()
        self.assertEqual(rows[0]["customer_send"], "j0_unconfirmed")
        self.assertEqual(self.assistant_logs(), [])
        self.assertEqual([(o["purpose"], o["state"]) for o in self.ops() if o["channel"] == "jikou"],
                         [("first_reply", "unconfirmed")])

    def test_immediate_notice_is_still_recorded_as_before(self):
        self.on()
        self.customer(_compose("x", category="緊急対応", auto_send=False, notice="court_doc_request"),
                      text="裁判所から書類が届きました")
        self.assertEqual(_texts(), [cr.COURT_DOC_REQUEST_REPLY])
        self.assertEqual(self.assistant_logs(), [cr.COURT_DOC_REQUEST_REPLY])

    def test_hearing_and_outage_paths_record_result_states(self):
        self.on()
        self.touch("e1")
        for res, state, text in ((sl.SEND_FAILED, "j0_failed", ""), (sl.SEND_POLICY_STOPPED, "j0_stopped", ""),
                                 (sl.SEND_UNCONFIRMED, "j0_unconfirmed", ""), (None, "j0_unconfirmed", ""),
                                 (sl.SEND_SENT, "j0_sent", sp.J0_TEXT)):
            send = AsyncMock(return_value=res)
            self.assertEqual(_run(sp.demoted_ack(USER, "rt", "500", "その他判断系", send, cr.PENDING_REPLY)), text, res)
            self.assertEqual(self.pending()[-1]["customer_send"], state, res)
        send = AsyncMock(side_effect=RuntimeError("x"))
        with self.assertRaises(RuntimeError):
            _run(sp.demoted_ack(USER, "rt", "500", "その他判断系", send, cr.PENDING_REPLY))
        self.assertEqual(self.pending()[-1]["customer_send"], "j0_unconfirmed")
        # 障害経路: LINE 失敗 → 写さない・j0_failed
        from claude_gateway import ClaudeUnavailableError
        _FakeClient.responses = [_FakeResp(500, "err"), _FakeResp(500, "err")]
        self.customer(_compose("x"), exc=ClaudeUnavailableError("down"))
        self.assertEqual((self.pending()[-1]["reason"], self.pending()[-1]["customer_send"]), ("claude_outage", "j0_failed"))
        self.assertEqual(self.assistant_logs(), [])

    def test_j0_state_closed_set_and_main_wrapper_returns_result(self):
        self.assertEqual([sp.j0_state_for(x) for x in (sl.SEND_SENT, sl.SEND_FAILED, sl.SEND_UNCONFIRMED,
                                                       sl.SEND_POLICY_STOPPED, None, True)],
                         ["j0_sent", "j0_failed", "j0_unconfirmed", "j0_stopped", "j0_unconfirmed", "j0_unconfirmed"])
        self.assertEqual(rp.CUSTOMER_STATES, ("none", "j0_sent", "j0_failed", "j0_unconfirmed", "j0_stopped"))
        with patch.object(line_channel, "reply_with_push_fallback", AsyncMock(return_value=sl.SEND_FAILED)):
            self.assertEqual(_run(main._line_reply_with_fallback("rt", USER, "x")), sl.SEND_FAILED)
        self.assertEqual(_run(main._line_reply_with_fallback("rt", USER, "x")), sl.SEND_SENT)
        self.assertEqual(len(_FakeClient.calls), 1)


# ── 6. pin（結線・sink・docs・閉集合・凍結の限定解除） ──────────────────────────────────
class TestPinsFix1(unittest.TestCase):
    def test_wiring_is_one_line_replacements_and_tail_only(self):
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        self.assertIn("send_policy.fixed_targets(user_id)", src[1599])
        self.assertIn("_sl_res = await send_line_push(user_id, ai_draft)", src[1599])
        self.assertEqual(sum("return await hub_line_channel.reply_with_push_fallback(" in l for l in src), 1)
        self.assertIn("send_policy.register_jobs()", "\n".join(src[-12:]))
        csrc = (REPO / "chat_responder.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("send_policy.send_ack(" in l for l in csrc), 1)
        self.assertEqual(sum("send_policy.ack_recorded(" in l for l in csrc), 1)
        self.assertEqual(sum("send_policy.pending_ack(" in l for l in csrc), 1)
        self.assertEqual(sum("send_policy.ack_purpose(" in l for l in csrc), 1)
        lc = (REPO / "hub" / "line_channel.py").read_text(encoding="utf-8")
        self.assertEqual(lc.count("send_policy.ledger_unavailable_stop()"), 1)
        for name in ("hub/line_channel.py", "main.py", "chat_responder.py", "hub/image_intake.py"):
            s = (REPO / name).read_text(encoding="utf-8")
            for banned in ("set_attending", "conversation_state", "decide_send"):
                self.assertNotIn(banned, s, name)
        allow = json.loads((REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8"))["entries"]
        self.assertEqual(sorted(int(e.split(":")[1]) for e in allow if e.startswith("main.py:")), [663, 685, 1320, 1620, 1917])
        self.assertEqual(sorted(int(e.split(":")[1]) for e in allow if e.startswith("chat_responder.py:")), [1660, 1672])

    def test_sink_policy_zero_violations_and_no_allowlist_entries(self):
        for name in ("hub/send_policy.py", "hub/reply_pending.py", "hub/line_channel.py", "hub/webapp_reply_view.py"):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for key in ("send_policy", "reply_pending", "line_channel", "webapp_reply_view"):
            self.assertNotIn(key, allow)

    def test_docs_contract_and_closed_sets(self):
        doc = (REPO / "docs" / "jikou_reply_q1c_contract.md").read_text(encoding="utf-8")
        for phrase in ("fail-closed", "reply_pending_retry", "REPLY_PENDING_AGE_MIN", "LINE アプリ", "限定的な凍結解除",
                       "j0_sent", "d4f6", "V2 ON の条件", "pending_targets"):
            self.assertIn(phrase, doc, phrase)
        self.assertEqual(rp.RETRY_STATES, ("queued", "done", "abandoned"))
        self.assertEqual((sp.REASON_LEDGER_UNAVAILABLE, sp.REASON_ATTENDING_UNKNOWN), ("ledger:unavailable", "attending:unknown"))
        self.assertEqual(rp.RETRY_TABLE_NAMES, ("reply_pending_retry",))
        self.assertEqual(rp.NOTIFY_RETRY_MAX, 3)

    def test_prompt_frozen_release_is_limited_to_one_line_and_pin(self):
        base = cr._SYSTEM_PROMPT_BASE
        self.assertEqual(base.count("は reply を空文字にする"), 1)
        style = (REPO / "test_autoreply_style1.py").read_text(encoding="utf-8")
        self.assertIn("a0835fe327c392aee9111b2fbf75cbf08a08a07c2578b2ad3c2c601637f39263", style)   # customer_prompt_base（限定解除後の pin）
        self.assertIn("52a8e0fefb8920f4189908aa9c62a068c0f4357148a8d8c2bbcb391a6be9c9a6", style)   # hearing_prompt_body は不変
        self.assertEqual(cr.PENDING_REPLY, "ありがとうございます。内容を確認の上、改めてご連絡いたします。\n"
                                            "少々お時間をいただく場合がございますが、何卒よろしくお願いいたします。")


if __name__ == "__main__":
    unittest.main()
