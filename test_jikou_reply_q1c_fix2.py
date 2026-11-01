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
from hub import approved_texts as AT  # noqa: E402
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
        # fix3 BC-13: claim_notify は (result, 同一 tx で読んだ永続キー) を返す
        self.assertEqual(_run(rp.claim_notify(pid, "A", now=now))[0], rp.CLAIM_OK)
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=now)), (rp.CLAIM_BUSY, ""))             # リース内は他者不可
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=now + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)))[0],
                         rp.CLAIM_OK)                                                                # リース超過で再取得
        self.assertTrue(_run(rp.set_notify_state(pid, rp.NOTIFY_SENT, owner="B")))                  # Q1c-fix4 BC-17: 完了書込は所有者 B が行う（owner 必須契約への追随）
        self.assertEqual(_run(rp.claim_notify(pid, "C")), (rp.CLAIM_DONE, ""))                      # 通知済みは再通知しない
        self.assertEqual(_run(rp.claim_notify(10 ** 9, "C")), (rp.CLAIM_NOT_FOUND, ""))
        self.assertEqual(rp.CLAIM_RESULTS, ("ok", "busy", "done", "not_found", "closed"))   # fix4 BC-19: closed を追加
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
        # 旧行（notice_key NULL）: 渡された算出値を claim と同一 tx で保存してから使う
        pid3 = self.reg(user=USER2, inbound_event_id="e-e")
        self.assertEqual(_run(rp.notice_key_of(pid3)), "")
        self.assertEqual(_run(sp._notify_pending(pid3, "pending_notice:legacy:2026-10-21", None, None)), rp.NOTIFY_SENT)
        self.assertEqual(_run(rp.notice_key_of(pid3)), "pending_notice:legacy:2026-10-21")
        pid4 = self.reg(user=USER2, inbound_event_id="e-f")
        self.assertEqual(_run(rp.claim_notify(pid4, "Z1", computed_key="pending_notice:k1:2026-10-21")), (rp.CLAIM_OK, "pending_notice:k1:2026-10-21"))
        self.assertTrue(_run(rp.release_notify_claim(pid4, "Z1")))
        self.assertEqual(_run(rp.claim_notify(pid4, "Z2", computed_key="pending_notice:k2:2026-10-21")), (rp.CLAIM_OK, "pending_notice:k1:2026-10-21"))   # 初回値は上書きしない

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


# ── 3. BC-08: 経過通知は承認台帳の定型だけ（数値差し込みのみ・未承認は送らない） ─────────────────────
class TestAgeNoticeApprovedText(_Fix2Mixin):
    NOW = datetime.datetime(2026, 11, 1, 3, 0, tzinfo=datetime.timezone.utc)

    def setUp(self):
        super().setUp()
        AT.reset_pending_for_tests()
        self.addCleanup(AT.reset_pending_for_tests)

    def _seed(self):
        self.reg(inbound_event_id="e-old1", now=self.NOW - datetime.timedelta(minutes=90))
        self.reg(inbound_event_id="e-old2", now=self.NOW - datetime.timedelta(minutes=61))
        self.reg(user=USER2, inbound_event_id="e-new", now=self.NOW - datetime.timedelta(minutes=5))

    def test_unapproved_template_sends_nothing_and_marks_needs_approval(self):
        self.on()
        self._seed()
        self.assertNotIn(sp.AGE_NOTICE_TEXT_KEY, AT.APPROVED_SHA256)                                   # 大野承認待ち（台帳に無い）
        self.assertEqual(sp.age_notice_text({"open_count": 3, "over_count": 2, "oldest_minutes": 90}, 0, 60), "")
        admin = AsyncMock(return_value=True)
        with patch("hub.notify.notify_admin_line", admin), self.assertLogs("hub.send_policy", level="WARNING") as cm:
            res = _run(sp.run_age_notice_job(9, now=self.NOW))
        self.assertEqual((res["over_count"], res["queued"], res["notice"]), (2, 0, sp.AGE_NOTICE_NEEDS_APPROVAL))
        self.biz.assert_not_awaited()                                                                  # 業務 LINE へ経過通知は送らない
        self.assertEqual([o for o in self.ops() if o["channel"] == "jikou_notice"], [])                 # 台帳行も作らない
        self.assertIn("needs approval", "\n".join(cm.output))
        # 未承認 key は台帳の仕組みで記録・通知（文面は key のみ・件数も本文も載せない）
        self.assertEqual(AT.pending_unapproved_keys(), ())                                             # 通知後に消費済み
        admin.assert_awaited_once()
        self.assertIn("key=" + sp.AGE_NOTICE_TEXT_KEY, admin.await_args.args[0])
        self.assertNotIn("対応待ち 3 件", admin.await_args.args[0])

    def test_approved_template_substitutes_numbers_only(self):
        self.on()
        self._seed()
        with patch.dict(AT.APPROVED_SHA256, {sp.AGE_NOTICE_TEXT_KEY: AT.sha256_text(sp.AGE_NOTICE_TEMPLATE)}):
            text = sp.age_notice_text({"open_count": 3, "over_count": 2, "oldest_minutes": 90}, 1, 60)
            self.assertEqual(text, "【対応待ち・未返信】\n未返信の対応待ち 3 件（60 分超 2 件・最長 90 分経過）・再処理待ち 1 件\n"
                                   "/app/pending からご確認ください。")
            res = _run(sp.run_age_notice_job(9, now=self.NOW))
        self.assertEqual(res["notice"], rp.NOTIFY_SENT)
        sent = self.biz.await_args.args[1]
        self.assertEqual(sent, "【対応待ち・未返信】\n未返信の対応待ち 3 件（60 分超 2 件・最長 90 分経過）・再処理待ち 0 件\n"
                               "/app/pending からご確認ください。")
        self.assertNotIn("{{", sent)
        # 本文を 1 文字でも変えると SHA が変わり再承認まで送らない
        with patch.dict(AT.APPROVED_SHA256, {sp.AGE_NOTICE_TEXT_KEY: AT.sha256_text(sp.AGE_NOTICE_TEMPLATE + "。")}):
            self.assertEqual(sp.age_notice_text({"open_count": 3, "over_count": 2, "oldest_minutes": 90}, 1, 60), "")

    def test_template_is_fixed_and_slots_are_numeric_only(self):
        # 定型の SHA（pgv/q1c_fix2_texts_for_approval.md に記載・承認票はこの値を台帳へ追加する）
        self.assertEqual(AT.sha256_text(sp.AGE_NOTICE_TEMPLATE), "59da41fcf2545b9aaa19175ea18315388e5b1e6df1c65bdbccaccacfc879652d")
        self.assertEqual(sp.AGE_NOTICE_SLOTS, ("{{open}}", "{{threshold}}", "{{over}}", "{{oldest}}", "{{queued}}"))
        import re as _re
        self.assertEqual(set(_re.findall(r"\{\{[a-z_]+\}\}", sp.AGE_NOTICE_TEMPLATE)), set(sp.AGE_NOTICE_SLOTS))
        src = (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8")
        self.assertNotIn("f\"{AGE_NOTICE_HEADER}\\n未返信の対応待ち {stats", src)                        # 旧い文字列組立は残っていない
        self.assertEqual(sp.AGE_NOTICE_TEXT_KEY, "ops_age_notice_v1")
        # hub/approved_texts.py は TEXT-APPROVAL-GATE-1（0e80ea6）と同一内容（別 PR の merge で衝突しない）
        self.assertEqual(set(AT.APPROVED_SHA256), {AT.SAFE_TEXT_KEY})


# ── 4. BC-09: 対応待ちの読取失敗（None）は送信を進めない（返信欄・承認 webhook） ─────────────────────
class TestPendingTargetsFailClosed(_Fix2Mixin):
    def setUp(self):
        super().setUp()
        from test_jikou_reply_q1b import _FIELDS_WITH_HUMAN, _approval
        from hub import kintone
        self._kp = [patch.object(kintone, "get_record", AsyncMock(return_value=_approval())),
                    patch.object(kintone, "search_records", AsyncMock(return_value=[])),
                    patch.object(kintone, "get_form_fields", AsyncMock(return_value=_FIELDS_WITH_HUMAN)),
                    patch.object(kintone, "create_record", AsyncMock(return_value="900"))]
        for p in self._kp:
            p.start()

    def tearDown(self):
        for p in self._kp:
            p.stop()
        super().tearDown()

    def _flaky_then_real(self, fail_times: int):
        real = rp.open_ids_for_user
        calls = {"n": 0}

        async def f(business, user_hash):
            calls["n"] += 1
            if calls["n"] <= fail_times:
                raise RuntimeError("db read failed")
            return await real(business, user_hash)
        return f, calls

    def _push_registering_b(self, arrived: dict):
        async def push(channel, to, text):                                   # 送信中に受信 B が対応待ちに入る
            arrived["b"] = await rp.register(business="jikou", channel="jikou", line_user_hash=sl.line_user_hash("jikou", USER),
                                             route="customer", reason="demoted", inbound_event_id="ev-b", approval_record_id="7")
            return True
        return push

    def test_pending_targets_distinguishes_zero_from_failure_and_retries_once(self):
        self.assertEqual(_run(sp.pending_targets(USER)), ())                                    # 0 件
        f, calls = self._flaky_then_real(1)
        a = self.reg(inbound_event_id="ev-a")
        with patch.object(rp, "open_ids_for_user", f):
            self.assertEqual(_run(sp.pending_targets(USER)), (a,))                              # 1 回失敗 → 再試行で読めた
        self.assertEqual(calls["n"], 2)
        f, calls = self._flaky_then_real(2)
        with patch.object(rp, "open_ids_for_user", f):
            self.assertIsNone(_run(sp.pending_targets(USER)))                                   # 2 回とも失敗 → None
        self.assertEqual(calls["n"], 2)
        # fixed_targets: V2 ON で None なら印が立ち、抜けると消える／OFF は読まない
        self.on()

        async def _inside():
            f2, _c = self._flaky_then_real(2)
            with patch.object(rp, "open_ids_for_user", f2):
                async with sp.fixed_targets(USER):
                    flag = sp.targets_unavailable()
            return flag, sp.targets_unavailable()
        self.assertEqual(_run(_inside()), (True, False))
        os.environ.pop(sl.POLICY_V2_ENV, None)
        spy = AsyncMock(side_effect=RuntimeError("never"))

        async def _off():
            with patch.object(rp, "open_ids_for_user", spy):
                async with sp.fixed_targets(USER):
                    return sp.targets_unavailable()
        self.assertFalse(_run(_off()))
        spy.assert_not_awaited()
        self.assertEqual(sp.REASON_TARGETS_UNAVAILABLE, "pending_targets:unavailable")

    def test_reply_view_does_not_send_when_targets_unavailable_then_closes_only_fixed_set(self):
        from test_jikou_reply_q1b import OP, REC_ID, _form
        from hub import webapp_reply_view as rv
        a = self.reg(inbound_event_id="ev-a")
        f, calls = self._flaky_then_real(2)
        with patch.object(rp, "open_ids_for_user", f):
            r = _client.post(f"/app/reply/{REC_ID}/send", headers=_auth(), data=_form(), follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"/app/reply/{REC_ID}?done=pending_unavailable"))
        self.assertEqual(_FakeClient.calls, [])                                                 # 送っていない
        self.assertEqual(self.pending()[0]["state"], "open")                                    # A は未解決のまま（送っていないので正しい）
        from hub import reply_followup as rf
        fu = [x for x in self._rows(rf.reply_chatlog_followup) if x["op_id"] == OP]
        self.assertEqual([(x["state"], x["last_result"]) for x in fu], [("prepared", "pending_unavailable")])   # 同じ操作で再開できる
        self.assertIn("pending_unavailable", (REPO / "hub" / "webapp_reply_view.py").read_text(encoding="utf-8"))
        # 再 POST（同じ op_id・同じ本文）: 1 回失敗 → 再試行で読めた → 送信。着手時点の A だけ閉じ、送信中に届いた B は open
        arrived: dict = {}
        f, calls = self._flaky_then_real(1)
        with patch.object(rp, "open_ids_for_user", f), \
             patch.object(line_channel, "_push_text_http", self._push_registering_b(arrived)):
            r = _client.post(f"/app/reply/{REC_ID}/send", headers=_auth(), data=_form(), follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertTrue(r.headers["location"].startswith(f"/app/reply/{REC_ID}?done=sent"), r.headers["location"])
        states = {x["pending_id"]: (x["state"], x["resolved_by"]) for x in self.pending()}
        self.assertEqual(states[a], ("human_replied", "human"))
        self.assertEqual(states[arrived["b"]], ("open", None))
        self.assertEqual(calls["n"], 2)

    def test_approval_webhook_stops_when_targets_unavailable_then_closes_only_fixed_set(self):
        self.on()
        self.env({"KINTONE_WEBHOOK_TOKEN": "kintone-token"})
        os.environ.pop("KINTONE_EVENT_DEDUP_ENABLED", None)
        record = {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}, "line_user_id": {"value": USER},
                  "AI下書き": {"value": "下書き"}, "カテゴリ": {"value": "c"}, "$revision": {"value": "3"}}
        body = {"record": {"$id": {"value": "7"}, "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}}}
        a = self.reg(inbound_event_id="ev-a")
        mark_sent = AsyncMock()
        f, calls = self._flaky_then_real(2)
        with patch.object(main, "mark_approval_sent", mark_sent), patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=record)), \
             patch.object(rp, "open_ids_for_user", f):
            r = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual((r.status_code, r.json().get("skip")), (200, "policy_stopped"))
        self.assertEqual(_FakeClient.calls, [])                                                 # LINE write 0
        mark_sent.assert_not_awaited()                                                          # 送信済み=no のまま（再承認で送れる）
        self.assertEqual(self.pending()[0]["state"], "open")                                    # A は open のまま
        self.assertEqual([o for o in self.ops() if o["channel"] == "jikou"], [])                 # 台帳行なし
        self.assertIn("pending_targets:unavailable", self.biz.await_args.args[1])              # 業務 LINE へ理由つき固定文言
        self.assertEqual(calls["n"], 2)
        # 再試行で読めた: 送信し、着手時点の A だけ閉じる。送信中に届いた B は open
        arrived: dict = {}
        f, calls = self._flaky_then_real(1)
        with patch.object(main, "mark_approval_sent", mark_sent), patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=record)), \
             patch.object(rp, "open_ids_for_user", f), \
             patch.object(line_channel, "_push_text_http", self._push_registering_b(arrived)):
            r = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual(r.json().get("record_id"), "7")
        mark_sent.assert_awaited_once()
        states = {x["pending_id"]: (x["state"], x["resolved_by"]) for x in self.pending()}
        self.assertEqual(states[a], ("human_replied", "approved_draft"))
        self.assertEqual(states[arrived["b"]], ("open", None))


# ── 5. BC-10: 空白・句読点・記号のみは空返事（V2 ON: 顧客送信ゼロ・対応待ち） ─────────────────────
class TestSymbolsOnlyReply(_Fix2Mixin):
    CODEX_CASES = ("。。。", "！？")                                                           # Codex の反例をそのまま
    SYMBOL_CASES = ("…", "、", "。 、 …", "！?!", "!?", "・・・", "（）", "〜", "―", "　。　", " … ")
    SPACE_CASES = ("   ", "　　", " 　 \n")

    def test_classification_is_pure_and_closed_vocabulary(self):
        for t in self.CODEX_CASES + self.SYMBOL_CASES:
            with self.subTest(t=t):
                self.assertEqual(eg.empty_reply_labels(t), [eg.LABEL_SYMBOLS_ONLY])
                self.assertTrue(eg.symbols_only(t))
        for t in self.SPACE_CASES:
            with self.subTest(t=repr(t)):
                self.assertEqual(eg.empty_reply_labels(t), [eg.LABEL_EMPTY])                   # 空白（半角・全角）だけは従来どおり「空の返信」
        for t in ("はい。", "3", "ok", "受任から完了まで2〜4週間程度です。", "！はい！", "…はい…"):
            with self.subTest(t=t):
                self.assertFalse(eg.symbols_only(t))
                self.assertNotIn(eg.LABEL_SYMBOLS_ONLY, eg.empty_reply_labels(t))
        self.assertEqual(eg.LABEL_SYMBOLS_ONLY, "記号・空白のみ")

    def test_off_keeps_symbol_replies_unchanged_and_on_demotes(self):
        for t in self.CODEX_CASES:
            g = sp.apply_v2(cr.apply_server_guards(_compose(t), [], "q"), t)
            self.assertTrue(g.can_auto_send, t)                                                 # V2 OFF: 従来の挙動（語彙表を適用しない）
            self.assertEqual(sp.empty_reply_violations(t), [], t)
        with patch.dict(os.environ, {sl.POLICY_V2_ENV: "1"}):
            for t in self.CODEX_CASES + self.SYMBOL_CASES:
                g = sp.apply_v2(cr.apply_server_guards(_compose(t), [], "q"), t)
                self.assertFalse(g.can_auto_send, t)
                self.assertEqual(g.demotion_reasons[-1], sp.EMPTY_REPLY_REASON_PREFIX + eg.LABEL_SYMBOLS_ONLY, t)
                self.assertEqual(g.immediate_notice, "none", t)
                self.assertEqual(sp.empty_reply_violations(t), [sp.EMPTY_REPLY_REASON_PREFIX + eg.LABEL_SYMBOLS_ONLY], t)

    def test_customer_route_symbol_reply_sends_nothing_and_queues(self):
        self.on()
        for i, t in enumerate(self.CODEX_CASES):
            with self.subTest(t=t):
                _FakeClient.calls.clear()
                # 会話なし（J-0 の対象外）: 顧客送信ゼロ・対応待ち reason=empty_reply
                user = "U" + ("%032x" % (0x100 + i))
                with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose(t))):
                    _run(cr.handle_customer_message(user_id=user, user_message="質問です", reply_token="rt",
                                                    app21_record=self.app21.return_value, reply_func=self.reply_func()))
                self.assertEqual(_FakeClient.calls, [])
                row = self.pending()[-1]
                self.assertEqual((row["route"], row["reason"], row["state"], row["customer_send"]),
                                 ("customer", "empty_reply", "open", "none"))
                self.assertNotIn(t, [c.args[2] for c in self.chatlog.await_args_list if c.args[1] == "assistant"])
        # 会話あり（J-0 例外）: 送られるのは J-0 だけ・記号の返事は送らない（実コールバック経由）
        self.touch("e1")
        _FakeClient.calls.clear()
        with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("！？"))):
            self.inbound()
        self.assertEqual(_texts(), [sp.J0_TEXT])
        self.assertEqual((self.pending()[-1]["reason"], self.pending()[-1]["customer_send"]), ("empty_reply", "j0_sent"))

    def test_hearing_route_symbol_reply_sends_nothing_and_queues(self):
        self.on()
        self.app21.return_value = None                                                          # App 21 なし＝ヒアリング経路
        for i, t in enumerate(self.CODEX_CASES):
            with self.subTest(t=t):
                user = "U" + ("%032x" % (0x200 + i))
                main.conversation_histories.pop(user, None)
                _FakeClient.calls.clear()
                with patch.object(main, "ask_claude", AsyncMock(return_value=t)):               # Claude が記号だけを返した
                    _run(main._process_line_event("rt", user, "質問です"))
                self.assertEqual(_FakeClient.calls, [])                                         # 顧客送信ゼロ（会話なし＝J-0 も無し）
                row = self.pending()[-1]
                self.assertEqual((row["route"], row["reason"], row["state"]), ("hearing", "demoted", "open"))
                self.assertEqual(self.approval.await_count, i + 1)                              # 承認キューへ（降格理由に空返事）
                self.assertIn(eg.LABEL_SYMBOLS_ONLY, self.approval.await_args.kwargs["reason"])
                self.assertNotIn(t, [c.args[2] for c in self.chatlog.await_args_list if c.args[1] == "assistant"])
                main.conversation_histories.pop(user, None)
        # 会話あり: J-0 だけ（実コールバック）
        self.touch("e1")
        _FakeClient.calls.clear()
        with patch.object(main, "ask_claude", AsyncMock(return_value="。。。")):
            self.inbound()
        self.assertEqual(_texts(), [sp.J0_TEXT])
        self.assertEqual((self.pending()[-1]["route"], self.pending()[-1]["customer_send"]), ("hearing", "j0_sent"))

# ── 6. BC-11: J-0 は 業務 × LINE 利用者 単位で 1 通（30 日無受信後の新会話でも再送しない） ─────────────────────
class TestJ0PerUser(_Fix2Mixin):
    def _conversations(self, user=USER) -> list:
        h = sl.line_user_hash("jikou", user)
        return [c for c in self._rows(sl.conversation) if c["line_user_hash"] == h]

    def test_j0_not_resent_in_new_conversation_after_30_days_customer_route(self):
        self.on()
        self.touch("e1")
        with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("x", category="法的判断・見通し", auto_send=False))):
            self.inbound()
        self.assertEqual(_texts(), [sp.J0_TEXT])
        self.assertEqual(self.pending()[0]["customer_send"], "j0_sent")
        # 31 日後の受信 → 新しい会話（conversation 2 行目）。J-0 は利用者単位で送信済みなので再送しない（無言・対応待ちのみ）
        self.touch("e2", at=T0 + datetime.timedelta(days=31))
        self.assertEqual(len(self._conversations()), 2)
        self.assertFalse(_run(sp.first_reply_due(USER)))
        _FakeClient.calls.clear()
        with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("y", category="法的判断・見通し", auto_send=False))):
            self.inbound()
        self.assertEqual(_FakeClient.calls, [])
        rows = self.pending()
        self.assertEqual((len(rows), rows[1]["customer_send"], rows[1]["state"]), (2, "none", "open"))
        self.assertEqual(self.assistant_logs(), [sp.J0_TEXT])                                   # 写しも 1 回だけ
        # 台帳の在庫検査（排他の中）も利用者単位: purpose=first_reply の直接送信は止まる
        with sl.purpose(sl.PURPOSE_FIRST_REPLY):
            res, reasons = self.run_r(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, sp.J0_TEXT))
        self.assertEqual((res, reasons), (sl.SEND_POLICY_STOPPED, ("first_reply:already",)))
        self.assertEqual(_run(sl.first_reply_count("jikou", USER)), 1)
        # 別の利用者には影響しない（利用者単位）
        self.touch("e3", user=USER2, at=T0 + datetime.timedelta(days=31))
        self.assertTrue(_run(sp.first_reply_due(USER2)))

    def test_ledger_count_joins_conversation_and_ignores_unattached_ops(self):
        self.on()
        self.touch("e1")
        _run(line_channel.push_text(JIKOU, USER, "通常の返信"))                                   # 会話 1 の reply（対象）
        self.touch("e2", at=T0 + datetime.timedelta(days=40))                                   # 会話 2
        self.assertEqual(len(self._conversations()), 2)
        self.assertEqual(_run(sl.first_reply_count("jikou", USER)), 1)                          # 会話をまたいで数える
        self.assertFalse(_run(sp.first_reply_due(USER)))
        # follow は対象外（従来どおり）・別利用者は数えない
        _run(sl.with_purpose("follow", line_channel.push_text, JIKOU, USER, "あいさつ"))
        self.assertEqual(_run(sl.first_reply_count("jikou", USER)), 1)
        self.assertIsNone(_run(sl.first_reply_count("jikou", USER2)))                           # 会話なし＝None（J-0 は送らない）
        src = (REPO / "hub" / "send_ledger.py").read_text(encoding="utf-8")
        self.assertIn("send_operation.join(conversation", src)
        self.assertNotIn("send_operation.c.conversation_id == int(conv_id),\n        send_operation.c.first_reply_target", src)

    def test_docs_state_per_user_unit_and_j0_approval_as_v2_condition(self):
        doc = (REPO / "docs" / "jikou_reply_q1c_contract.md").read_text(encoding="utf-8")
        self.assertIn("業務 × LINE 利用者", doc)
        self.assertIn("V2 有効化前の条件", doc)
        self.assertNotIn("J-0（初回受付・1 会話 1 回", (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8"))

if __name__ == "__main__":
    unittest.main()
