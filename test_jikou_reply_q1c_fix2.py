"""JIKOU-REPLY-Q1c-fix2（Codex R-Q1c-fix1 REJECT 対応・BC-06〜BC-12・司令塔裁定）

固定する仕様:
- BC-06 main の実コールバック（顧客対応・障害応答）は送信結果を返す。reply_with_push_fallback の SEND_SENT は台帳 finish=applied
  （または sent 確定済みの重複）だけ。HTTP 成功でも台帳確定が late／skipped なら SEND_UNCONFIRMED＝j0_sent にしない・App 28 に写さない。
- BC-07 通知キーは初回に永続化（reply_pending.notice_key・キュー→pending に引き継ぐ）。通知は claim（条件付き更新）を取れた 1 者だけ。
- BC-08 経過通知の本文は承認台帳（hub/approved_texts）の定型 ops_age_notice_v1 だけ・数値差し込みのみ・未承認は送らない。
- BC-09 pending_targets は読取失敗を None で返す（1 回再試行）。None なら送信を進めない（webhook・返信欄とも）。
- BC-10 空白（半角・全角）・句読点・記号のみは空返事（V2 ON: 顧客送信ゼロ・対応待ち）。
- BC-11 J-0 は 業務 × LINE 利用者 単位で 1 通（30 日無受信後の新会話でも再送しない）。
- BC-12 は test_jikou_reply_q1b 側（実ルート表の一致順）。
"""

import asyncio
import datetime
import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import (_DbMixin, _FakeClient, _FakeResp, _client, _compose, _rec, _run, _texts,  # noqa: E402
                                  ATTORNEY, JIKOU, T0, USER, USER2, _auth)
from test_jikou_reply_q1c_fix1 import _Fix1Mixin  # noqa: E402

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
from claude_gateway import ClaudeUnavailableError  # noqa: E402
from hub import empty_reply_guard as eg  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from pathlib import Path  # noqa: E402

REPO = Path(__file__).parent


def _finish_for(channels, outcome: str):
    """send_ledger.finish を包み、指定チャネルの操作だけ outcome（"late"／"skipped"）を返す（他は本物）。
    HTTP は成功しているが台帳で確定できなかった状況の注入。"""
    real = sl.finish

    async def fake(op, result, *, text_version=None):
        if op is not None and getattr(op, "channel", "") in channels:
            return outcome
        return await real(op, result, text_version=text_version)
    return fake


class _Fix2Mixin(_Fix1Mixin):
    """main._process_line_event を実コールバックのまま走らせるための patch（LINE は _FakeClient・kintone/Claude はフェイク）。"""

    def setUp(self):
        super().setUp()
        main.conversation_histories.pop(USER, None)
        main.hearing_completed.discard(USER)
        self.main_chatlog = self.chatlog                         # main は自分の名前空間の save_to_chatlog を呼ぶ
        self._main_patches = [
            patch.object(main, "get_app21_record", self.app21),
            patch.object(main, "save_to_chatlog", self.chatlog),
            patch.object(main, "get_recent_chat_history", AsyncMock(return_value=[])),
            patch.object(main.image_intake, "close_held_markers", AsyncMock(return_value=0)),
            patch.object(main.image_intake, "heal_unreplied", AsyncMock(return_value=0)),
            patch.object(main, "save_to_approval_queue", self.approval),
        ]
        for p in self._main_patches:
            p.start()

    def tearDown(self):
        for p in self._main_patches:
            p.stop()
        main.conversation_histories.pop(USER, None)
        main.hearing_completed.discard(USER)
        super().tearDown()

    def inbound(self, text: str = "質問です"):
        _run(main._process_line_event("rt", USER, text))


# ── 1. BC-06: 実コールバック経由の J-0 記録（送信済みは台帳 applied のときだけ） ─────────────────
class TestRealCallbackSendResult(_Fix2Mixin):
    def test_customer_route_real_callback_records_j0_sent_and_chatlog(self):
        self.on()
        self.touch("e1")
        with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("x", category="法的判断・見通し", auto_send=False))):
            self.inbound()
        rows = self.pending()
        self.assertEqual((rows[0]["route"], rows[0]["customer_send"], rows[0]["state"]), ("customer", "j0_sent", "open"))
        self.assertEqual(_texts(), [sp.J0_TEXT])                                   # 顧客へは J-0 だけ
        self.assertEqual(self.assistant_logs(), [sp.J0_TEXT])                      # App 28 の写し（確定文だけ）
        self.assertEqual([(o["purpose"], o["state"]) for o in self.ops() if o["channel"] == "jikou"],
                         [("first_reply", "sent")])

    def test_customer_route_http_ok_but_ledger_not_applied_is_not_sent(self):
        self.on()
        # 相談者ごとに 1 回（確定できなかった first_reply 操作は started のまま在庫に残り、同じ相談者の次の J-0 は台帳が止める＝正しい）
        for user, event, outcome in ((USER, "e1", "skipped"), (USER2, "e2", "late")):
            self.touch(event, user=user)
            _FakeClient.calls.clear()
            self.chatlog.reset_mock()
            with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("x", category="法的判断・見通し", auto_send=False))), \
                 patch.object(sl, "finish", _finish_for({"jikou"}, outcome)):
                _run(main._process_line_event("rt", user, "質問です"))
            self.assertEqual(_texts(), [sp.J0_TEXT], outcome)                      # HTTP は 1 回成功している
            self.assertEqual(self.pending()[-1]["customer_send"], "j0_unconfirmed", outcome)
            self.assertEqual(self.assistant_logs(), [], outcome)                   # 送信済みとして写さない
            self.assertEqual([o["state"] for o in self.ops() if o["channel"] == "jikou" and o["purpose"] == "first_reply"][-1],
                             "started", outcome)                                  # 台帳は未確定のまま（回収ジョブ→unconfirmed→人の確定）

    def test_outage_route_real_callback_records_result(self):
        self.on()
        self.touch("e1")
        self.app21.return_value = None                                             # App 21 なし＝ヒアリング経路 → Claude 障害
        with patch.object(main, "ask_claude", AsyncMock(side_effect=ClaudeUnavailableError("down"))):
            self.inbound()
        rows = self.pending()
        self.assertEqual((rows[0]["route"], rows[0]["reason"], rows[0]["customer_send"]), ("customer", "claude_outage", "j0_sent"))
        self.assertEqual(_texts(), [sp.J0_TEXT])
        self.assertEqual(self.assistant_logs(), [sp.J0_TEXT])
        # 台帳確定が失敗した障害応答: 送信済みにしない・写さない
        self.touch("e2", user=USER2)
        _FakeClient.calls.clear()
        self.chatlog.reset_mock()
        with patch.object(main, "ask_claude", AsyncMock(side_effect=ClaudeUnavailableError("down"))), \
             patch.object(sl, "finish", _finish_for({"jikou"}, "skipped")):
            _run(main._process_line_event("rt", USER2, "質問です"))
        self.assertEqual(self.pending()[-1]["customer_send"], "j0_unconfirmed")
        self.assertEqual(self.assistant_logs(), [])

    def test_reply_with_push_fallback_return_values(self):
        self.on()
        self.touch("e1")
        # 台帳 applied → SENT／late・skipped → UNCONFIRMED／HTTP 失敗 → FAILED／重複 sent → SENT（HTTP なし）
        self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "a")), sl.SEND_SENT)
        with patch.object(sl, "finish", _finish_for({"jikou"}, "late")):
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "b")), sl.SEND_UNCONFIRMED)
        with patch.object(sl, "finish", _finish_for({"jikou"}, "skipped")):
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "c")), sl.SEND_UNCONFIRMED)
        _FakeClient.responses = [_FakeResp(500), _FakeResp(500)]
        self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "d")), sl.SEND_FAILED)
        with patch.object(sl, "begin", AsyncMock(return_value=sl.DUPLICATE_SENT)):
            n = len(_FakeClient.calls)
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "e")), sl.SEND_SENT)
            self.assertEqual(len(_FakeClient.calls), n)
        # 記録なし（OFF・DB 未設定の fail-open）は従来どおり SENT
        os.environ.pop(sl.POLICY_V2_ENV, None)
        with patch.object(sl, "begin", AsyncMock(return_value=None)):
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "f")), sl.SEND_SENT)

    def test_main_callbacks_return_result_in_source(self):
        src = (REPO / "main.py").read_text(encoding="utf-8")
        self.assertEqual(src.count("async def _reply_func(token: str, text: str) -> str:"), 2)
        self.assertEqual(src.count("return await _line_reply_with_fallback(token, user_id, text)"), 2)
        self.assertNotIn("async def _reply_func(token: str, text: str) -> None:", src)


# ── 2. BC-07: 通知キーの永続化・引き継ぎ・claim ─────────────────────────────────────────
class TestNoticeKeyPersistence(_Fix2Mixin):
    def customer_bound(self, event_id: str, result: dict, user: str = USER):
        async def _w():
            tok = sl.bind_inbound(event_id)
            try:
                with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=result)):
                    await cr.handle_customer_message(user_id=user, user_message="質問です", reply_token="rt",
                                                     app21_record=self.app21.return_value, reply_func=self.reply_func())
            finally:
                sl.unbind(tok)
        _run(_w())

    def notice_ops(self) -> list:
        return [(o["inbound_event_id"], o["state"], o["attempt_no"]) for o in self.ops() if o["channel"] == "jikou_notice"]

    def test_key_persisted_at_registration_and_reused_by_concurrent_retries_even_if_lookup_fails(self):
        self.on()
        self.touch("e1")
        self.biz.return_value = False
        self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        row = self.pending()[0]
        conv = _run(sl.conversation_state("jikou", USER))
        h = sl.line_user_hash("jikou", USER)
        self.assertEqual(row["notice_key"], sp.notice_key(conv["ref"], h, today=sp._jst_date()))   # 初回に算出して保存
        self.assertEqual((row["notify_state"], row["notify_owner"]), ("failed", None))              # claim は解放済み
        self.biz.return_value = True

        async def _both():
            # 回収ジョブ 2 つが並行し、会話参照の取得が落ちている（従来はハッシュへフォールバック＝別 key で 2 通目）
            with patch.object(sp, "_conversation_ref", AsyncMock(side_effect=RuntimeError("x"))):
                return await asyncio.gather(sp.run_pending_retry_job(), sp.run_pending_retry_job())
        _run(_both())
        self.assertEqual(self.biz.await_count, 2)                                                    # 初回（failed）+ 再試行 1 回＝2 通目は出ない
        self.assertEqual(self.notice_ops(), [(row["notice_key"], "sent", 2)])                        # 同じ key の 1 操作を再試行
        rows = self.pending()
        self.assertEqual((rows[0]["notify_state"], rows[0]["notify_owner"], rows[0]["attempts"]), ("sent", None, 1))

    def test_hash_fallback_key_at_registration_is_kept_on_retry(self):
        self.on()
        self.touch("e1")
        self.biz.return_value = False
        with patch.object(sp, "_conversation", AsyncMock(return_value=None)):                        # 登録時に会話参照が取れない
            self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        row = self.pending()[0]
        h = sl.line_user_hash("jikou", USER)
        self.assertEqual(row["notice_key"], sp.notice_key("", h, today=sp._jst_date()))              # ハッシュ形の key が初回値
        self.biz.return_value = True
        _run(sp.run_pending_retry_job())                                                             # 今は会話参照が取れるが、保存値を使う
        self.assertEqual(self.notice_ops(), [(row["notice_key"], "sent", 2)])
        self.assertEqual(self.biz.await_count, 2)                                                    # 初回（failed）+ 再試行 1 回

    def test_queue_key_is_carried_to_pending_across_jst_date_boundary(self):
        self.on()
        self.touch("e-q")
        d0 = datetime.date(2026, 10, 21)
        h = sl.line_user_hash("jikou", USER)
        with patch.object(sp, "_jst_date", side_effect=lambda v=None: d0), \
             patch.object(rp, "register", AsyncMock(return_value=None)):
            self.customer_bound("e-q", _compose("x", category="法的判断・見通し", auto_send=False))
        q = self.retry_rows()[0]
        conv = _run(sl.conversation_state("jikou", USER))
        self.assertEqual(q["notice_key"], sp.notice_key(conv["ref"], h, today=d0))                   # キューに初回値を保存
        self.assertEqual(self.biz.await_count, 0)
        d1 = d0 + datetime.timedelta(days=1)
        with patch.object(sp, "_jst_date", side_effect=lambda v=None: d1):                           # 翌日（JST）に回収
            res = _run(sp.run_pending_retry_job())
            self.assertEqual((res["registered"], self.biz.await_count), (1, 1))
            row = self.pending()[0]
            self.assertEqual((row["notice_key"], row["notify_state"]), (q["notice_key"], "sent"))    # 引き継いだ key で通知
            # 同じ受信イベントの再配送（翌日）: 既存行の保存 key・通知済みなので claim は done＝2 通目なし
            self.customer_bound("e-q", _compose("x", category="法的判断・見通し", auto_send=False))
        self.assertEqual((self.biz.await_count, len(self.pending())), (1, 1))
        self.assertEqual(self.notice_ops(), [(q["notice_key"], "sent", 1)])
        self.assertNotEqual(sp.notice_key(conv["ref"], h, today=d1), q["notice_key"])               # 再算出していれば別 key＝2 通目が出ていた

    def test_claim_is_conditional_update_released_by_state_write_and_direct_double_notify_sends_once(self):
        pid = self.reg(inbound_event_id="e-c")
        now = datetime.datetime.now(datetime.timezone.utc)
        self.assertEqual(_run(rp.claim_notify(pid, "A", now=now)), rp.CLAIM_OK)
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=now)), rp.CLAIM_BUSY)                    # リース内は他者不可
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=now + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1))),
                         rp.CLAIM_OK)                                                                # リース超過で再取得
        _run(rp.set_notify_state(pid, rp.NOTIFY_SENT))
        self.assertEqual(_run(rp.claim_notify(pid, "C")), rp.CLAIM_DONE)                             # 通知済みは再通知しない
        self.assertEqual(_run(rp.claim_notify(10 ** 9, "C")), rp.CLAIM_NOT_FOUND)
        self.assertEqual(rp.CLAIM_RESULTS, ("ok", "busy", "done", "not_found", "error"))
        self.assertEqual(rp.NOTIFY_CLAIM_LEASE_SECONDS, 300)
        # 直接の二重通知（同じ pending を 2 者が同時に通知）: claim を取れた 1 者だけが送る
        self.on()
        pid2 = self.reg(user=USER2, inbound_event_id="e-d", notice_key="pending_notice:refx:2026-10-21")

        async def _both():
            return await asyncio.gather(sp._notify_pending(pid2, "k", "500", "c"), sp._notify_pending(pid2, "k", "500", "c"))
        res = _run(_both())
        self.assertEqual(sorted(res), ["", rp.NOTIFY_SENT])
        self.assertEqual(self.biz.await_count, 1)
        self.assertEqual(self.notice_ops(), [("pending_notice:refx:2026-10-21", "sent", 1)])       # 保存値の key で送った
        # 旧行（notice_key NULL）: 渡された算出値を保存してから使う
        pid3 = self.reg(user=USER2, inbound_event_id="e-e")
        self.assertEqual(_run(rp.notice_key_of(pid3)), "")
        self.assertEqual(_run(sp._notify_pending(pid3, "pending_notice:legacy:2026-10-21", None, None)), rp.NOTIFY_SENT)
        self.assertEqual(_run(rp.notice_key_of(pid3)), "pending_notice:legacy:2026-10-21")
        self.assertFalse(_run(rp.set_notice_key_if_missing(pid3, "other")))                          # 初回値は上書きしない

    def test_migration_adds_columns_and_refuses_downgrade_with_notice_key_data(self):
        from test_db_foundation import TestAlembicScaffold
        NOTICE, RETRY = "b7d9f1a3c5e7", "d4f6a8b0c2e4"
        d = tempfile.mkdtemp(prefix="reply_pending_notice_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, _u, nullable = TestAlembicScaffold._brain_mig_tools(dbfile)
        try:
            up = alembic("upgrade", NOTICE)
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            con = sqlite3.connect(dbfile)
            try:
                for name, table in ((rp.TABLE_NAME, rp.reply_pending), (rp.RETRY_TABLE_NAME, rp.reply_pending_retry)):
                    cols = {r[1] for r in con.execute(f'PRAGMA table_info("{name}")')}
                    self.assertEqual(cols, {c.name for c in table.columns}, name)
                nn = nullable(con, rp.TABLE_NAME)
                self.assertTrue(nn["notice_key"] and nn["notify_owner"] and nn["notify_claimed_at"])
                con.execute("INSERT INTO reply_pending (business, channel, line_user_hash, route, reason, state, notify_state, "
                            "customer_send, attempts, created_at, updated_at, notice_key) VALUES ('jikou', 'jikou', 'h', 'customer', "
                            "'demoted', 'open', 'none', 'none', 0, '2026-11-01 00:00:00', '2026-11-01 00:00:00', 'pending_notice:secret-ref:2026-11-01')")
                con.commit()
            finally:
                con.close()
            down = alembic("downgrade", RETRY)
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("reply_pending_notice_key_downgrade_refused_data_present", down.stderr)
            self.assertNotIn("secret-ref", down.stderr)
            self.assertIn(NOTICE, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                con.execute("UPDATE reply_pending SET notice_key = NULL")
                con.commit()
            finally:
                con.close()
            down = alembic("downgrade", RETRY)
            self.assertEqual(down.returncode, 0, f"stderr={down.stderr[-800:]}")
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute(f'PRAGMA table_info("{rp.TABLE_NAME}")')}
                self.assertFalse({"notice_key", "notify_owner", "notify_claimed_at"} & cols)
                self.assertNotIn("notice_key", {r[1] for r in con.execute(f'PRAGMA table_info("{rp.RETRY_TABLE_NAME}")')})
                self.assertEqual(con.execute("SELECT COUNT(*) FROM reply_pending").fetchone()[0], 1)   # 行は残る
            finally:
                con.close()
            self.assertEqual(alembic("upgrade", "head").returncode, 0)
            self.assertIn(NOTICE, alembic("current").stdout)
        finally:
            shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
