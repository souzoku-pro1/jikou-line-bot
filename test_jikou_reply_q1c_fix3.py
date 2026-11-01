"""JIKOU-REPLY-Q1c-fix3（Codex R-Q1c-fix2 REJECT 対応・BC-13〜BC-16・司令塔裁定）

固定する仕様:
- BC-13 業務 LINE 通知に使う通知キーは **claim と同一トランザクションで読んだ永続キーだけ**。読取例外は "" に潰さず送らない（claim も残らない）。
  旧行（NULL）の埋めも同一 tx。計算値が送信に渡ることはない。
- BC-14 結果・試行数・claim 解放は pending_id AND notify_owner = 自分 の条件付き更新。0 件なら遅着として無変更。
- BC-15 回収対象に open + notify_state=none かつ未所有またはリース切れの行を加える（retry_done 直後の停止・初回 claim の DB エラー）。
  保存キーで送信台帳と照合してから送る（送信済みなら 2 通目を出さない）。回収は業務 LINE 通知のみ＝顧客送信・J-0 には触れない（裁定）。
- BC-16 空返事判定は基底文字（Letter／Number）の有無で行う。異体字セレクタ・単独結合記号だけでは内容ありとしない。
"""

import asyncio
import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import (_FakeClient, _compose, _run, _texts, JIKOU, T0, USER, USER2)  # noqa: E402
from test_jikou_reply_q1c_fix2 import _Fix2Mixin  # noqa: E402

import chat_responder as cr  # noqa: E402
from hub import empty_reply_guard as eg  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402

REPO = Path(__file__).parent
KEY_A = "pending_notice:keyA:2026-11-01"
KEY_B = "pending_notice:keyB:2026-11-01"


# ── 1. BC-13: 永続キーは claim と同一 tx で読み、その値だけで送る ─────────────────────────────────
class TestClaimKeySameTransaction(_Fix2Mixin):
    def test_key_read_failure_never_sends_computed_value_and_pending_stays_recoverable(self):
        self.on()
        pid = self.reg(inbound_event_id="e-a", notice_key=KEY_A)
        spy = AsyncMock(return_value=rp.NOTIFY_SENT)
        with patch.object(rp, "_notice_key_in_tx", AsyncMock(side_effect=RuntimeError("read failed"))), \
             patch.object(sp, "send_business_notice", spy):
            for _ in range(2):                                                                  # 読取 2 回失敗
                self.assertEqual(_run(sp._notify_pending(pid, KEY_B, "500", "c")), "")
        spy.assert_not_awaited()                                                                # 計算値 B は送信関数に渡らない
        row = self.pending()[0]
        self.assertEqual((row["state"], row["notify_state"], row["notify_owner"], row["notice_key"]),
                         ("open", "none", None, KEY_A))                                         # rollback＝claim も残らず回収可能な状態
        # 読めるようになれば保存キー A だけで送る
        with patch.object(sp, "send_business_notice", spy):
            self.assertEqual(_run(sp._notify_pending(pid, KEY_B, "500", "c")), rp.NOTIFY_SENT)
        spy.assert_awaited_once()
        self.assertEqual(spy.await_args.args[0], KEY_A)
        self.assertEqual((self.pending()[0]["notify_state"], self.pending()[0]["notify_owner"]), ("sent", None))

    def test_legacy_null_key_is_filled_in_same_transaction_and_read_failure_sends_nothing(self):
        self.on()
        pid = self.reg(inbound_event_id="e-l")                                                  # notice_key NULL（列追加前の行）
        spy = AsyncMock(return_value=rp.NOTIFY_SENT)
        with patch.object(rp, "_notice_key_in_tx", AsyncMock(side_effect=RuntimeError("read failed"))), \
             patch.object(sp, "send_business_notice", spy):
            self.assertEqual(_run(sp._notify_pending(pid, KEY_B, None, None)), "")
        spy.assert_not_awaited()
        self.assertEqual((self.pending()[0]["notice_key"], self.pending()[0]["notify_owner"]), (None, None))   # 埋めも巻き戻る
        with patch.object(sp, "send_business_notice", spy):
            self.assertEqual(_run(sp._notify_pending(pid, KEY_B, None, None)), rp.NOTIFY_SENT)
        self.assertEqual((spy.await_args.args[0], self.pending()[0]["notice_key"]), (KEY_B, KEY_B))            # 埋め値＝送信値

    def test_notice_key_of_raises_instead_of_empty_on_db_error_and_claim_releases_when_no_key(self):
        pid = self.reg(inbound_event_id="e-r", notice_key=KEY_A)
        self.assertEqual(_run(rp.notice_key_of(pid)), KEY_A)
        with patch.object(rp, "_notice_key_in_tx", AsyncMock(side_effect=RuntimeError("x"))):
            with self.assertRaises(RuntimeError):
                _run(rp.notice_key_of(pid))
        # 確定値が取れない（NULL かつ computed_key なし）: claim は取れるが key は ""＝送らず解放
        pid2 = self.reg(inbound_event_id="e-n")
        spy = AsyncMock(return_value=rp.NOTIFY_SENT)
        self.on()
        with patch.object(sp, "send_business_notice", spy):
            self.assertEqual(_run(sp._notify_pending(pid2, "", None, None)), "")
        spy.assert_not_awaited()
        self.assertEqual((self.pending()[1]["notify_owner"], self.pending()[1]["notify_state"]), (None, "none"))
        self.assertNotIn("set_notice_key_if_missing", (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8"))
        self.assertNotIn("notice_key_of(", (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8"))   # 送信経路は同一 tx の値だけ


# ── 2. BC-14: 結果・試行数・claim 解放は owner 条件付き（遅着は無変更） ─────────────────────────
class TestOwnerConditionedWrites(_Fix2Mixin):
    def test_late_result_from_expired_owner_does_not_overwrite_new_owner(self):
        pid = self.reg(inbound_event_id="e-o", notice_key=KEY_A)
        t0 = datetime.datetime(2026, 11, 1, 3, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(_run(rp.claim_notify(pid, "A", now=t0))[0], rp.CLAIM_OK)
        t1 = t0 + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=t1))[0], rp.CLAIM_OK)                # A の期限切れ後に B が claim
        self.assertFalse(_run(rp.bump_attempts(pid, owner="A")))                                 # 遅れて戻った A: 無変更
        self.assertFalse(_run(rp.set_notify_state(pid, rp.NOTIFY_SENT, owner="A")))
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["notify_owner"], row["attempts"]), ("none", "B", 0))   # B の状態は上書きされない
        self.assertEqual([h["field"] for h in _run(rp.history(pid))], ["state"])                 # 履歴も書かない
        self.assertTrue(_run(rp.bump_attempts(pid, owner="B")))
        self.assertTrue(_run(rp.set_notify_state(pid, rp.NOTIFY_FAILED, owner="B")))             # B の完了は書けて claim を解放
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["notify_owner"], row["notify_claimed_at"], row["attempts"]), ("failed", None, None, 1))
        self.assertFalse(_run(rp.set_notify_state(pid, rp.NOTIFY_SENT, owner="B")))             # 解放後はもう書けない

    def test_notify_pending_late_return_leaves_state_of_new_owner(self):
        self.on()
        pid = self.reg(inbound_event_id="e-p", notice_key=KEY_A)
        taken: dict = {}

        async def slow_send(key, text):                                                           # A の送信中にリースが切れ B が claim して完了した
            taken["claim"] = await rp.claim_notify(pid, "B", now=datetime.datetime.now(datetime.timezone.utc)
                                                   + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1))
            self.assertTrue(await rp.set_notify_state(pid, rp.NOTIFY_THROTTLED, owner="B"))
            return rp.NOTIFY_SENT
        with patch.object(sp, "send_business_notice", slow_send):
            self.assertEqual(_run(sp._notify_pending(pid, KEY_B, None, None)), rp.NOTIFY_SENT)   # A の戻り値は返るが…
        self.assertEqual(taken["claim"][0], rp.CLAIM_OK)
        self.assertEqual((self.pending()[0]["notify_state"], self.pending()[0]["notify_owner"]), ("throttled", None))   # …B の状態を上書きしない

# ── 3. BC-15: notify_state=none の対応待ちも回収（業務 LINE 通知のみ・顧客送信には触れない） ─────────────────────
class TestRecoverUnnotifiedPending(_Fix2Mixin):
    def _enqueue_via_register_failure(self, event_id: str = "e-q"):
        async def _w():
            tok = sl.bind_inbound(event_id)
            try:
                with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("x", category="法的判断・見通し", auto_send=False))), \
                     patch.object(rp, "register", AsyncMock(return_value=None)):
                    await cr.handle_customer_message(user_id=USER, user_message="質問です", reply_token="rt",
                                                     app21_record=self.app21.return_value, reply_func=self.reply_func())
            finally:
                sl.unbind(tok)
        _run(_w())

    def notice_ops(self) -> list:
        return [(o["inbound_event_id"], o["state"], o["attempt_no"]) for o in self.ops() if o["channel"] == "jikou_notice"]

    def test_stop_right_after_retry_done_is_recovered_with_exactly_one_notice(self):
        self.on()
        self.touch("e-q")
        self._enqueue_via_register_failure("e-q")
        self.assertEqual((len(self.retry_rows()), self.pending()), (1, []))
        # 回収ジョブ: register → retry_done の直後に停止（初回通知に至らない）
        with patch.object(sp, "_notify_pending", AsyncMock(side_effect=RuntimeError("process stopped"))):
            with self.assertRaises(RuntimeError):
                _run(sp.run_pending_retry_job())
        rows = self.pending()
        self.assertEqual((len(rows), rows[0]["notify_state"], rows[0]["notify_owner"], self.retry_rows()[0]["state"]),
                         (1, "none", None, "done"))                                               # キューは done・通知は未着手
        self.biz.assert_not_awaited()
        self.assertIn(rows[0]["pending_id"], [r["pending_id"] for r in _run(rp.list_notify_retry())])   # 回収対象に載る
        # 再起動後の回収ジョブ: 業務通知はちょうど 1 通
        res = _run(sp.run_pending_retry_job())
        self.assertEqual((res["requeued"], res["notify_retried"], res["notify_recovered"]), (0, 1, 1))
        self.assertEqual(self.biz.await_count, 1)
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["notify_owner"], row["attempts"], row["notice_key"]),
                         ("sent", None, 1, self.retry_rows()[0]["notice_key"]))                   # 引き継いだ保存キーで送った
        self.assertEqual(self.notice_ops(), [(row["notice_key"], "sent", 1)])
        # もう一度: 対象なし・通知は増えない
        self.assertEqual(_run(sp.run_pending_retry_job())["notify_retried"], 0)
        self.assertEqual(self.biz.await_count, 1)

    def test_initial_claim_db_error_leaves_none_row_that_job_recovers_once(self):
        self.on()
        self.touch("e1")
        with patch.object(rp, "claim_notify", AsyncMock(side_effect=RuntimeError("db error at first claim"))):
            self.customer(_compose("x", category="法的判断・見通し", auto_send=False))         # 登録はされ、初回 claim で DB エラー
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["notify_owner"]), ("none", None))
        self.biz.assert_not_awaited()
        self.assertEqual(_texts(), [sp.J0_TEXT])                                                  # 顧客向け J-0 は通知の失敗と独立（従来どおり 1 通）
        res = _run(sp.run_pending_retry_job())
        self.assertEqual((res["notify_recovered"], self.biz.await_count, self.pending()[0]["notify_state"]), (1, 1, "sent"))

    def test_crash_after_send_is_reconciled_with_ledger_not_resent(self):
        self.on()
        pid = self.reg(inbound_event_id="e-c", notice_key=KEY_A)
        with patch.object(rp, "set_notify_state", AsyncMock(side_effect=RuntimeError("crash after send"))):
            with self.assertRaises(RuntimeError):
                _run(sp._notify_pending(pid, KEY_B, "500", "c"))                                  # HTTP は成功・結果の記録前に停止
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], self.biz.await_count), ("none", 1))
        self.assertIsNotNone(row["notify_owner"])                                                 # claim はリース内
        self.assertEqual(_run(sp.run_pending_retry_job())["notify_retried"], 0)                   # リース内は拾わない
        later = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)
        res = _run(sp.run_pending_retry_job(now=later))                                           # リース切れ後に回収
        self.assertEqual((res["notify_recovered"], self.biz.await_count), (1, 1))                 # 台帳照合: 送信済み＝2 通目なし
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["notify_owner"]), ("throttled", None))
        self.assertEqual(self.notice_ops(), [(KEY_A, "sent", 1)])
        self.assertEqual(_run(rp.list_notify_retry(now=later)), [])

    def test_recovery_never_touches_customer_sends_or_j0(self):
        self.on()
        self.touch("e1")
        for i in range(3):                                                                        # none の対応待ちを複数（うち 1 件は旧行＝key NULL）
            self.reg(inbound_event_id=f"e-n{i}", notice_key=(KEY_A if i else None))
        reply_spy = AsyncMock(side_effect=AssertionError("customer reply must not be sent by recovery"))
        push_spy = AsyncMock(side_effect=AssertionError("customer push must not be sent by recovery"))
        with patch.object(line_channel, "reply_with_push_fallback", reply_spy), patch.object(line_channel, "push_text", push_spy):
            res = _run(sp.run_pending_retry_job())
        self.assertEqual(res["notify_recovered"], 3)
        reply_spy.assert_not_awaited()
        push_spy.assert_not_awaited()
        self.assertEqual(_FakeClient.calls, [])                                                   # 顧客 LINE への HTTP なし
        self.assertEqual({o["channel"] for o in self.ops()}, {"jikou_notice"})                     # 台帳の操作は業務通知だけ
        self.assertEqual({r["customer_send"] for r in self.pending()}, {"none"})                  # J-0 の記録に触れない
        self.assertTrue(_run(sp.first_reply_due(USER)))                                           # J-0 の在庫も不変
        # 同じ保存キー（同じ相談者・同日）の 2 件目は台帳照合で throttled（1 会話 1 日 1 回）＝業務通知は key ごとに 1 通
        self.assertEqual(sorted(r["notify_state"] for r in self.pending()), ["sent", "sent", "throttled"])
        self.assertEqual(self.biz.await_count, 2)
        src = (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8")
        job = src[src.index("async def run_pending_retry_job"):src.index("def age_notice_text")]
        for banned in ("J0_TEXT", "reply_with_push_fallback", "push_text", "_maybe_j0", "set_customer_send", "PURPOSE_FIRST_REPLY"):
            self.assertNotIn(banned, job, banned)

if __name__ == "__main__":
    unittest.main()
