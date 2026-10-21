"""CONCURRENCY-1（QUEUE-2026-10-14-G G-3）: 並行・再送耐性（既存テストは不変）

1) LINE webhook の同一イベント再配送（同 webhookEventId）を 1〜50 回並行で受けても、相談者への返信・業務 LINE 通知・
   kintone 書込が各 1 回以下。durable flag ON（本番設定: INBOUND_EVENT_DURABLE_ENABLED=1・2026-08-11 点火）。
   asyncio 並行（ASGITransport・1 ループ・決定的）とスレッド並行（TestClient・スレッドごとのループ＝プロセス相当・
   sqlite で可能な範囲）。flag OFF は webhook 段の重複遮断が無い（設計どおり・報告に注記）。
2) ジョブを同時刻に 2 プロセスで起動しても通知 1 回: 熟慮期間（houki_jukuryo・台帳 key）・フォロー（reply_followup・
   条件付き UPDATE）・ヘルスチェック（daily_healthcheck・台帳 key healthcheck:{JST 日付}・台帳不可は従来どおり送る）。
   「2 プロセス」は (a) 同一ループ 2 タスク（sqlite の process 内 Lock＝Postgres advisory lock の代替）と
   (b) Lock を共有しない 2 タスク（advisory lock 不在＝UNIQUE 制約だけが守る下限）の両方で検査。
3) 承認画面（Q4）の二重クリック（同時 2 POST）・戻る→再送信で送信 1 回。
Postgres の advisory lock（プロセス間排他）は sqlite では検証できない → pgverify/check6_concurrency_pg.py
（要 Postgres-ak5B 実走・保留一覧）。
"""

import asyncio
import base64
import hashlib
import hmac
import json
import multiprocessing as mp
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch

import httpx
import sqlalchemy as sa
from fastapi.testclient import TestClient

from test_houki_jukuryo import ADMIN, _Base as _JukuryoBase, _rec as _houki_rec  # noqa: E402
from test_jikou_reply_q1c import USER, _DbMixin, _FakeClient, _auth, _compose, _run, _texts  # noqa: E402
from test_jikou_reply_q4 import _DraftMixin  # noqa: E402

import chat_responder as cr  # noqa: E402
import daily_healthcheck as dh  # noqa: E402
import main  # noqa: E402
from hub import db  # noqa: E402
from hub import houki_jukuryo as hj  # noqa: E402
from hub import reply_draft as rd  # noqa: E402
from hub import reply_followup as rf  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_drafts_view as dv  # noqa: E402
from hub.inbound_event import InboundEvent  # noqa: E402

DURABLE = "INBOUND_EVENT_DURABLE_ENABLED"
REPLY = "こんにちは。ご連絡ありがとうございます。"
_RealAsyncClient = httpx.AsyncClient          # fixture が line_channel.httpx.AsyncClient をフェイクに差し替える前に確保


def _line_body(event_id: str, text: str = "こんにちは", user: str = USER) -> tuple[bytes, dict]:
    body = json.dumps({"events": [{"type": "message", "webhookEventId": event_id, "replyToken": "rt",
                                   "source": {"userId": user}, "message": {"type": "text", "text": text}}]}).encode()
    sig = base64.b64encode(hmac.new(b"dummy_secret", body, hashlib.sha256).digest()).decode()
    return body, {"X-Line-Signature": sig}


async def _asgi_burst(path: str, n: int, *, content: bytes | None = None, data: dict | None = None,
                      headers: dict | None = None) -> list[int]:
    transport = httpx.ASGITransport(app=main.app)
    async with _RealAsyncClient(transport=transport, base_url="http://t") as c:
        rs = await asyncio.gather(*[c.post(path, content=content, data=data, headers=headers) for _ in range(n)])
    return [r.status_code for r in rs], rs


def _child_post(env: dict, body: bytes, headers: dict, barrier, q) -> None:
    """別プロセス（spawn）の本体: 同じ sqlite を共有し、フェイク（LINE／kintone／業務通知）を子側で当ててから
    Barrier で揃えて同一イベントを 1 回 POST。件数だけを親へ返す（本文・ID は返さない）。"""
    os.environ.update(env)
    from unittest.mock import AsyncMock as _AM, patch as _patch
    import chat_responder as cr_
    import main as main_
    from hub import autoreply_stoplist as st_
    from hub import line_channel as lc_
    from hub import send_policy as sp_
    from test_jikou_reply_q1c import _FakeClient as FC, _rec
    FC.calls, FC.responses, FC.raise_exc = [], [], None
    chatlog, approval = _AM(return_value="800"), _AM(return_value="500")
    biz, notify = _AM(return_value=True), _AM()
    app21 = _AM(return_value=_rec(status="受任", 問い合わせ業者名="アコム", response_mode="自動"))
    with _patch.object(lc_.httpx, "AsyncClient", FC), _patch.object(sp_, "_push_business_http", biz), \
            _patch.object(st_, "check", _AM(return_value=st_.OUTCOME_CLEAR)), \
            _patch.object(st_, "is_suppressed", _AM(return_value=False)), \
            _patch.object(cr_, "get_app21_record", app21), _patch.object(main_, "get_app21_record", app21), \
            _patch.object(cr_, "save_to_chatlog", chatlog), _patch.object(main_, "save_to_chatlog", chatlog), \
            _patch.object(cr_, "save_to_approval_queue", approval), \
            _patch.object(cr_, "_notify_attorney", notify), \
            _patch.object(cr_, "get_recent_chat_history", _AM(return_value=cr_.ChatHistory())), \
            _patch.object(cr_, "_call_compose_reply", _AM(return_value=_compose(REPLY, category="挨拶・雑談"))):
        barrier.wait(timeout=180)
        try:
            status = TestClient(main_.app, raise_server_exceptions=False).post("/webhook", content=body, headers=headers).status_code
        except Exception as e:                                          # sqlite のロック競合などは受理失敗として数える
            status = type(e).__name__
    roles = [c.args[1] for c in chatlog.await_args_list]
    q.put({"status": status, "reply": len(FC.calls), "chatlog_user": roles.count("user"),
           "chatlog_assistant": roles.count("assistant"), "approval": approval.await_count,
           "biz_notice": biz.await_count + notify.await_count})


# ── 1) LINE webhook の同一イベント再配送 ─────────────────────────────────────────
class TestWebhookRedelivery(_DbMixin):
    def setUp(self):
        super().setUp()
        self.env({DURABLE: "1", "IMAGE_HEAL_DISABLED": "1"})
        self.compose = AsyncMock(return_value=_compose(REPLY, category="挨拶・雑談"))
        # main は get_app21_record／save_to_chatlog を名前で import しているため main 側も同じフェイクに差し替える
        self._p = [patch.object(cr, "_call_compose_reply", self.compose),
                   patch.object(main, "get_app21_record", self.app21),
                   patch.object(main, "save_to_chatlog", self.chatlog)]
        for p in self._p:
            p.start()
        main.conversation_histories.pop(USER, None)

    def tearDown(self):
        for p in reversed(self._p):
            p.stop()
        super().tearDown()

    def _counts(self) -> dict:
        roles = [c.args[1] for c in self.chatlog.await_args_list]
        return {"reply": len(_FakeClient.calls), "chatlog_user": roles.count("user"),
                "chatlog_assistant": roles.count("assistant"), "approval": self.approval.await_count,
                "biz_notice": self.biz.await_count + self.notify_attorney.await_count,
                "compose": self.compose.await_count}

    def _inbound(self) -> list[tuple]:
        async def _q():
            async with db.session_scope() as s:
                return [(r.external_event_id, r.state, r.attempts)
                        for r in (await s.execute(sa.select(InboundEvent))).scalars().all()]
        return _run(_q())

    def _assert_at_most_once(self, counts: dict, statuses: list[int], n: int):
        for k, v in counts.items():
            self.assertLessEqual(v, 1, (k, counts, n))
        if all(s == 200 for s in statuses):
            self.assertEqual(counts["reply"], 1, (counts, n))           # 全受理なら必ず 1 回は返す
            self.assertEqual(_texts(), [REPLY])

    def test_asyncio_redelivery_1_to_50_replies_at_most_once(self):
        for n in (1, 2, 5, 10, 25, 50):
            _FakeClient.calls.clear()
            self.chatlog.reset_mock(); self.approval.reset_mock(); self.biz.reset_mock()
            self.notify_attorney.reset_mock(); self.compose.reset_mock()
            body, headers = _line_body(f"ev-burst-{n}")
            statuses, _ = _run(_asgi_burst("/webhook", n, content=body, headers=headers))
            self.assertTrue(set(statuses) <= {200, 503}, statuses)
            self._assert_at_most_once(self._counts(), statuses, n)
            rows = [r for r in self._inbound() if r[0] == f"ev-burst-{n}"]
            self.assertEqual(len(rows), 1, rows)                        # UNIQUE(dedup_key)
            self.assertEqual(rows[0][1], "done")

    def test_process_redelivery_replies_at_most_once(self):
        """別プロセス（spawn・同じ sqlite ファイル）から同一イベントを同時刻に POST（Barrier で揃える）。
        スレッド並行は hub.db の async engine がループ跨ぎで使えない（プロセスごとに 1 engine の設計）ため、
        プロセス並行で代替する。advisory lock の実効は Postgres でのみ検証（check6_concurrency_pg.py）。"""
        n = 3
        body, headers = _line_body("ev-procs")
        env = {k: os.environ[k] for k in ("DATABASE_URL", "WEBAPP_PASSWORD_HASH", "WEBAPP_SESSION_SECRET",
                                          "APP_CHATLOG", "TOKEN_CHATLOG", "APP_AUTOREPLY_STOP", "TOKEN_AUTOREPLY_STOP")}
        env.update({DURABLE: "1", "IMAGE_HEAL_DISABLED": "1", sl.POLICY_V2_ENV: ""})
        ctx = mp.get_context("spawn")
        barrier, q = ctx.Barrier(n), ctx.Queue()
        ps = [ctx.Process(target=_child_post, args=(env, body, headers, barrier, q)) for _ in range(n)]
        for p in ps:
            p.start()
        results = [q.get(timeout=240) for _ in ps]
        for p in ps:
            p.join(timeout=60)
        total = {k: sum(r[k] for r in results) for k in ("reply", "chatlog_user", "chatlog_assistant", "approval", "biz_notice")}
        statuses = [r["status"] for r in results]
        for k, v in total.items():
            self.assertLessEqual(v, 1, (k, total, statuses))
        self.assertLessEqual(len([r for r in self._inbound() if r[0] == "ev-procs"]), 1)
        if all(s == 200 for s in statuses):
            self.assertEqual(total["reply"], 1, (total, statuses))

    def test_sequential_redelivery_after_done_is_skipped(self):
        body, headers = _line_body("ev-seq")
        for _ in range(5):
            statuses, _ = _run(_asgi_burst("/webhook", 1, content=body, headers=headers))
            self.assertEqual(statuses, [200])
        self.assertEqual(self._counts()["reply"], 1)
        rows = [r for r in self._inbound() if r[0] == "ev-seq"]
        self.assertEqual((rows[0][1], rows[0][2]), ("done", 1))        # 終端後の再配送は attempts も動かない


# ── 2) ジョブの 2 プロセス同時起動 ───────────────────────────────────────────────
def _no_shared_lock(_key):
    """advisory lock 不在（別プロセス）の模擬: 呼出ごとに新しい Lock＝排他しない。"""
    return asyncio.Lock()


class TestJukuryoTwoProcesses(_JukuryoBase):
    def _two(self, shared_lock: bool):
        self.seed(_houki_rec(rid="1", start=""), _houki_rec(rid="2", start=""))   # 起算日未確定の件数通知のみ

        async def slow_push(admin, text):
            await asyncio.sleep(0.05)                                   # 送信中にもう 1 プロセスが台帳を見る
            return True
        self.push.side_effect = slow_push

        async def both():
            if shared_lock:
                return await asyncio.gather(hj.jukuryo_daily_check(8), hj.jukuryo_daily_check(8))
            with patch.object(sl, "_local_lock", _no_shared_lock):
                return await asyncio.gather(hj.jukuryo_daily_check(8), hj.jukuryo_daily_check(8))
        from test_houki_jukuryo import _run as _run_hj
        return _run_hj(both())

    def test_shared_exclusion_sends_once(self):
        r1, r2 = self._two(shared_lock=True)
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(sorted([r1["unset_result"], r2["unset_result"]]),
                         sorted([hj.SEND_SENT, hj.SEND_DUPLICATE_UNCONFIRMED]))
        self.assertEqual([o["state"] for o in self.ops()], ["sent"])

    def test_without_shared_exclusion_unique_constraint_still_sends_at_most_once(self):
        r1, r2 = self._two(shared_lock=False)
        self.assertLessEqual(self.push.await_count, 1)
        results = sorted([r1["unset_result"], r2["unset_result"]])
        self.assertLessEqual(results.count(hj.SEND_SENT), 1)
        self.assertTrue(set(results) <= {hj.SEND_SENT, hj.SEND_DUPLICATE_UNCONFIRMED, hj.SEND_DUPLICATE_SENT,
                                         hj.SEND_LEDGER_UNAVAILABLE}, results)
        self.assertLessEqual(len([o for o in self.ops() if o["state"] == "sent"]), 1)

    def test_second_process_after_first_is_duplicate(self):
        self.seed(_houki_rec(rid="1", start=""))
        from test_houki_jukuryo import _run as _run_hj
        r1 = _run_hj(hj.jukuryo_daily_check(8))
        r2 = _run_hj(hj.jukuryo_daily_check(8))
        self.assertEqual((r1["unset_result"], r2["unset_result"]), (hj.SEND_SENT, hj.SEND_DUPLICATE_SENT))
        self.assertEqual(self.push.await_count, 1)


class TestFollowupTwoProcesses(_DbMixin):
    OP = "f" * 32

    def _pending(self):
        return _run(rf.mark_pending(self.OP, business="jikou", approval_record_id="500",
                                    line_user_hash=sl.line_user_hash("jikou", USER), text="t", last_result="failed"))

    def _rows(self):
        return super()._rows(rf.reply_chatlog_followup)

    def test_sequential_two_runs_write_once(self):
        self.assertEqual(self._pending(), "pending")
        write = AsyncMock(return_value=rf.RESULT_WRITTEN)
        resolve = AsyncMock(return_value=USER)
        c1 = _run(rf.run_followup(resolve_user_id=resolve, write_chatlog=write))
        c2 = _run(rf.run_followup(resolve_user_id=resolve, write_chatlog=write))
        self.assertEqual((c1, c2), ({rf.RESULT_WRITTEN: 1}, {}))
        self.assertEqual(write.await_count, 1)
        self.assertEqual(self._rows()[0]["state"], rf.STATE_DONE)

    def test_concurrent_two_runs_mark_done_once(self):
        """同時起動: done への遷移は条件付き UPDATE で 1 回（2 件目は not_pending）。
        注記: 両者が pending を読んでから書く窓では App 28 の書込自体は 2 回になり得る（報告の反例一覧・判断待ち）。"""
        self.assertEqual(self._pending(), "pending")
        gate = asyncio.Event()

        async def write(user_id, text, confirm_owner=None):             # Q1b-fix4 BR-14: 第 3 引数（作成直前の owner 再確認）を受ける
            await gate.wait()                                           # 両者が pending を読み終えるまで待つ
            return rf.RESULT_WRITTEN
        resolve = AsyncMock(return_value=USER)

        async def both():
            a = asyncio.create_task(rf.run_followup(resolve_user_id=resolve, write_chatlog=write))
            b = asyncio.create_task(rf.run_followup(resolve_user_id=resolve, write_chatlog=write))
            await asyncio.sleep(0.05)
            gate.set()
            return await asyncio.gather(a, b)
        c1, c2 = _run(both())
        merged = {k: c1.get(k, 0) + c2.get(k, 0) for k in set(c1) | set(c2)}
        self.assertEqual(merged.get(rf.RESULT_WRITTEN, 0), 1)
        self.assertLessEqual(merged.get(rf.RESULT_NOT_PENDING, 0), 1)
        self.assertEqual(self._rows()[0]["state"], rf.STATE_DONE)


class TestHealthcheckTwoProcesses(_DbMixin):
    def _hc_patches(self, notify):
        return [patch.object(dh, "check_models", AsyncMock(return_value=["モデル監視: 異常"])),
                patch.object(dh, "check_kintone_schema", AsyncMock(return_value=[])),
                patch.object(dh, "check_templates", Mock(return_value=[])),
                patch("channels.soufu_annai.check_block_sync", AsyncMock(return_value=[])),
                patch.object(dh, "check_journal_backlog", AsyncMock(return_value=[])),
                patch.object(dh, "check_unknown_providers", AsyncMock(return_value=[])),
                patch.object(dh, "check_app36_confirmed_decisions", AsyncMock(return_value=[])),
                patch.object(dh, "check_business_notify_liveness", AsyncMock(return_value=[])),
                # PRE-REVIEW-1 #298 中: concurrency-fix-1（F-6）の監視項目 J〜M を取り込んだため、既存 3 テストと同じく
                # 個別 check を無害化する（M の LINE bot/info は test env の dummy トークンで実 HTTP 401 になり問題件数が変わる）
                patch.object(dh, "check_send_ledger_health", AsyncMock(return_value=[])),
                patch.object(dh, "check_reply_pending_oldest", AsyncMock(return_value=[])),
                patch.object(dh, "check_houki_deadline_soon", AsyncMock(return_value=[])),
                patch.object(dh, "check_connectivity", AsyncMock(return_value=[])),
                patch.object(dh, "notify_admin_line", notify),
                patch.dict(os.environ, {DURABLE: "", "KINTONE_WEBHOOK_TOKEN_NEXT": "", "LINE_ADMIN_USER_ID": ADMIN})]

    def _two(self, notify):
        async def both():
            return await asyncio.gather(dh.run_healthcheck(), dh.run_healthcheck())
        ps = self._hc_patches(notify)
        for p in ps:
            p.start()
        try:
            return _run(both())
        finally:
            for p in reversed(ps):
                p.stop()

    def test_two_processes_notify_once_via_ledger(self):
        async def notify(text, throttle_key="", **_kw):
            await asyncio.sleep(0.05)
            return True
        spy = AsyncMock(side_effect=notify)
        p1, p2 = self._two(spy)
        self.assertEqual((len(p1), len(p2)), (1, 1))                   # 監視結果（問題の検出）は両プロセスで同じ
        self.assertEqual(spy.await_count, 1)
        ops = [o for o in self.ops() if str(o["inbound_event_id"]).startswith("healthcheck:")]
        self.assertEqual([o["state"] for o in ops], ["sent"])
        self.assertNotIn("モデル監視", str(ops))                       # 台帳に本文は入らない（text_version のみ）

    def test_same_day_restart_does_not_resend_but_new_day_does(self):
        spy = AsyncMock(return_value=True)
        self._two(spy)
        self.assertEqual(spy.await_count, 1)
        ps = self._hc_patches(spy)
        for p in ps:
            p.start()
        try:
            _run(dh.run_healthcheck())                                  # 同日の再起動相当
            self.assertEqual(spy.await_count, 1)
            with patch.object(dh, "_healthcheck_day_key", lambda: "healthcheck:2099-01-01"):
                _run(dh.run_healthcheck())                              # 翌日
            self.assertEqual(spy.await_count, 2)
        finally:
            for p in reversed(ps):
                p.stop()

    def test_without_ledger_falls_back_to_direct_send(self):
        """台帳が使えない（DB 未設定）ときは従来どおり送る（監視通知を DB に依存させない＝fail-open）。"""
        spy = AsyncMock(return_value=True)
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            db.reset_for_tests()
            ps = self._hc_patches(spy)
            for p in ps:
                p.start()
            try:
                problems = _run(dh.run_healthcheck())
            finally:
                for p in reversed(ps):
                    p.stop()
        self.assertEqual(len(problems), 1)
        self.assertEqual(spy.await_count, 1)

    def test_failed_notice_is_retried_by_next_process(self):
        first = AsyncMock(return_value=False)
        ps = self._hc_patches(first)
        for p in ps:
            p.start()
        try:
            _run(dh.run_healthcheck())
        finally:
            for p in reversed(ps):
                p.stop()
        second = AsyncMock(return_value=True)
        ps = self._hc_patches(second)
        for p in ps:
            p.start()
        try:
            _run(dh.run_healthcheck())
        finally:
            for p in reversed(ps):
                p.stop()
        self.assertEqual((first.await_count, second.await_count), (1, 1))
        ops = [o for o in self.ops() if str(o["inbound_event_id"]).startswith("healthcheck:")]
        self.assertEqual([(o["state"], o["attempt_no"]) for o in ops], [("sent", 2)])


# ── 3) 承認画面の二重クリック・戻る→再送信 ──────────────────────────────────────
class TestApprovalDoubleClick(_DraftMixin):
    def test_concurrent_double_click_sends_once(self):
        did = self.make_draft()
        f = self.open_form(did)
        data = dict(f, action="send", text=self.gen.return_value, confirm="1")
        statuses, rs = _run(_asgi_burst(f"{dv.PAGE}/{did}/act", 2, data=data, headers=_auth()))
        self.assertEqual(statuses, [303, 303])
        dones = sorted(r.headers["location"].split("done=")[1] for r in rs)
        self.assertEqual(dones, ["already", "sent"])
        self.assertEqual(_texts(), [self.gen.return_value])
        ops = [o for o in self.ops() if o["actor"] == sl.ACTOR_APPROVED_DRAFT]
        self.assertEqual([o["state"] for o in ops], ["sent"])
        self.assertEqual({int(r["draft_id"]): r["state"] for r in self.drafts()}[did], rd.STATE_SENT)
        # 戻る→再送信（同じ op_id・ticket を逐次に再 POST）
        r = self.act(did, data)
        self.assertIn("done=already", r.headers["location"])
        self.assertEqual(len(_texts()), 1)

    def test_concurrent_five_clicks_send_once(self):
        did = self.make_draft()
        f = self.open_form(did)
        data = dict(f, action="send", text=self.gen.return_value, confirm="1")
        statuses, rs = _run(_asgi_burst(f"{dv.PAGE}/{did}/act", 5, data=data, headers=_auth()))
        self.assertEqual(statuses, [303] * 5)
        dones = [r.headers["location"].split("done=")[1] for r in rs]
        self.assertEqual(dones.count("sent"), 1)
        self.assertEqual(dones.count("already"), 4)
        self.assertEqual(len(_texts()), 1)


if __name__ == "__main__":
    unittest.main()
