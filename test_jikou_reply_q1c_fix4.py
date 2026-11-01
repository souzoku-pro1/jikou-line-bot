"""JIKOU-REPLY-Q1c-fix4（Codex R-Q1c-fix3 REJECT 対応・BC-17〜BC-19・司令塔裁定）

固定する仕様（いずれも業務 LINE 通知の回収に閉じる・顧客送信・J-0 には触れない）:
- BC-17 通知結果（set_notify_state）・試行数（bump_attempts）を変える関数は owner 必須（省略・None は TypeError）。未所有行の管理操作は
  *_unowned（notify_owner IS NULL 条件）に分ける。B 所有中に A の完了・加算は 0 件で無変更。
- BC-18 試行数の加算は送信前に行わず、owner 条件付きの通知結果確定・claim 解放と同一トランザクションで行う。送信後に停止した場合は
  保存キーで送信台帳と照合して確定する（attempts=2 の行が加算点直後の停止で回収対象から外れない）。
- BC-19 回収用 claim の UPDATE は state=open を条件に含める（一覧取得後に human_replied になった行には業務通知を出さない）。
"""

import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import (_FakeClient, _run, USER, USER2)  # noqa: E402
from test_jikou_reply_q1c_fix2 import _Fix2Mixin  # noqa: E402

from hub import line_channel  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402

REPO = Path(__file__).parent
KEY_A = "pending_notice:keyA:2026-11-01"
T0 = datetime.datetime(2026, 11, 1, 3, 0, tzinfo=datetime.timezone.utc)


# ── 1. BC-17: owner 必須・未所有行は別関数 ─────────────────────────────────────────────
class TestOwnerRequired(_Fix2Mixin):
    def test_owner_omitted_or_none_is_rejected(self):
        pid = self.reg(inbound_event_id="e-o", notice_key=KEY_A)
        with self.assertRaises(TypeError):
            _run(rp.set_notify_state(pid, rp.NOTIFY_SENT))                                        # 省略
        with self.assertRaises(TypeError):
            _run(rp.set_notify_state(pid, rp.NOTIFY_SENT, owner=None))                            # None
        with self.assertRaises(TypeError):
            _run(rp.bump_attempts(pid))
        with self.assertRaises(TypeError):
            _run(rp.bump_attempts(pid, owner=""))
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["attempts"], row["notify_owner"]), ("none", 0, None))   # 何も変わらない

    def test_completion_without_ownership_never_changes_row_owned_by_b(self):
        pid = self.reg(inbound_event_id="e-b", notice_key=KEY_A)
        self.assertEqual(_run(rp.claim_notify(pid, "A", now=T0))[0], rp.CLAIM_OK)
        later = T0 + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=later))[0], rp.CLAIM_OK)             # A 期限切れ後に B が claim
        self.assertFalse(_run(rp.set_notify_state(pid, rp.NOTIFY_SENT, owner="A")))              # A の完了: 0 件
        self.assertFalse(_run(rp.bump_attempts(pid, owner="A")))                                 # A の加算: 0 件
        self.assertFalse(_run(rp.set_notify_state_unowned(pid, rp.NOTIFY_SENT)))                 # 管理操作も所有中は触らない
        self.assertFalse(_run(rp.bump_attempts_unowned(pid)))
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["notify_owner"], row["attempts"]), ("none", "B", 0))
        self.assertEqual([h["field"] for h in _run(rp.history(pid))], ["state"])                 # 履歴なし
        # B が完了すると解放され、未所有の管理操作だけが通る
        self.assertTrue(_run(rp.set_notify_state(pid, rp.NOTIFY_FAILED, owner="B")))
        self.assertTrue(_run(rp.bump_attempts_unowned(pid)))
        self.assertTrue(_run(rp.set_notify_state_unowned(pid, rp.NOTIFY_UNAVAILABLE)))
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["notify_owner"], row["attempts"]), ("unavailable", None, 1))
        # 製品コードの通知経路に owner 省略の呼出しは無い
        src = (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8")
        import re as _re
        for m in _re.finditer(r"reply_pending\.(set_notify_state|bump_attempts)\(([^)]*)\)", src):
            self.assertIn("owner=", m.group(2), m.group(0))
        self.assertNotIn("_unowned(", src)


# ── 2. BC-18: 試行数の加算は送信前に行わず、確定・解放と同一トランザクション ─────────────────────────
class TestAttemptsBumpedAtCompletion(_Fix2Mixin):
    def _failed_row_with_attempts(self, n: int, event: str = "e-x"):
        pid = self.reg(inbound_event_id=event, notice_key=KEY_A)
        self.assertTrue(_run(rp.set_notify_state_unowned(pid, rp.NOTIFY_FAILED)))
        for _ in range(n):
            self.assertTrue(_run(rp.bump_attempts_unowned(pid)))
        self.assertEqual(self.pending()[-1]["attempts"], n)
        return pid

    def test_stop_before_send_keeps_attempts_and_row_recoverable_then_exactly_one_send(self):
        self.on()
        pid = self._failed_row_with_attempts(rp.NOTIFY_RETRY_MAX - 1)                             # attempts=2
        # 旧い加算点（claim 直後・台帳 begin 前）で停止を注入: 送信関数に入る前に落ちる
        with patch.object(sp, "send_business_notice", AsyncMock(side_effect=RuntimeError("stopped before ledger begin"))):
            with self.assertRaises(RuntimeError):
                _run(sp.run_pending_retry_job(now=T0))
        row = self.pending()[0]
        self.assertEqual((row["attempts"], row["notify_state"]), (2, "failed"))                   # 加算されていない（未送信のまま上限に達しない）
        self.assertIsNotNone(row["notify_owner"])                                                 # claim はリース内
        self.biz.assert_not_awaited()
        later = T0 + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)
        self.assertEqual([r["pending_id"] for r in _run(rp.list_notify_retry(now=later))], [pid])   # リース切れ後も回収対象
        res = _run(sp.run_pending_retry_job(now=later))                                           # 復旧後: 送信はちょうど 1 回
        self.assertEqual((res["notify_retried"], self.biz.await_count), (1, 1))
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["attempts"], row["notify_owner"]), ("sent", 3, None))   # 確定と同時に attempts=3
        self.assertEqual(_run(rp.list_notify_retry(now=later + datetime.timedelta(seconds=1))), [])     # 上限到達＝対象外
        self.assertEqual(self.biz.await_count, 1)

    def test_stop_after_send_is_settled_by_ledger_reconciliation_with_single_bump(self):
        self.on()
        pid = self._failed_row_with_attempts(rp.NOTIFY_RETRY_MAX - 1)                             # attempts=2
        with patch.object(rp, "set_notify_state", AsyncMock(side_effect=RuntimeError("stopped after send"))):
            with self.assertRaises(RuntimeError):
                _run(sp.run_pending_retry_job(now=T0))
        row = self.pending()[0]
        self.assertEqual((row["attempts"], row["notify_state"], self.biz.await_count), (2, "failed", 1))   # 送ったが確定前に停止
        later = T0 + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)
        res = _run(sp.run_pending_retry_job(now=later))                                           # 保存キーで台帳照合 → 送信済み
        self.assertEqual((res["notify_retried"], self.biz.await_count), (1, 1))                   # 2 通目なし
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["attempts"], row["notify_owner"]), ("throttled", 3, None))

    def test_completion_bumps_in_same_conditional_update(self):
        pid = self.reg(inbound_event_id="e-s", notice_key=KEY_A)
        self.assertEqual(_run(rp.claim_notify(pid, "A", now=T0))[0], rp.CLAIM_OK)
        later = T0 + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=later))[0], rp.CLAIM_OK)
        self.assertFalse(_run(rp.set_notify_state(pid, rp.NOTIFY_SENT, owner="A", bump=True)))   # 遅着: 結果も attempts も無変更
        self.assertEqual(self.pending()[0]["attempts"], 0)
        self.assertTrue(_run(rp.set_notify_state(pid, rp.NOTIFY_SENT, owner="B", bump=True)))
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["attempts"], row["notify_owner"]), ("sent", 1, None))
        src = (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8")
        self.assertNotIn("reply_pending.bump_attempts(", src)                                     # 送信前の加算は無い
        self.assertEqual(src.count("set_notify_state(pid, state, owner=owner, bump=bump_attempts)"), 1)

# ── 3. BC-19: 回収用 claim は state=open を再確認（一覧取得後に解決した行には送らない） ─────────────────────
class TestClaimRechecksOpenState(_Fix2Mixin):
    def test_row_resolved_after_listing_gets_no_business_push(self):
        self.on()
        pid = self.reg(inbound_event_id="e-h", notice_key=KEY_A)                                  # open・notify_state=none（回収対象）
        listed = _run(rp.list_notify_retry(now=T0))
        self.assertEqual([r["pending_id"] for r in listed], [pid])                               # 1) 一覧取得
        self.assertEqual(_run(rp.resolve_ids((pid,), resolved_by=rp.RESOLVED_BY_HUMAN, business="jikou",
                                             line_user_hash=sl.line_user_hash("jikou", USER))), 1)   # 2) 人の返信で human_replied
        self.assertEqual(self.pending()[0]["state"], "human_replied")
        r = listed[0]                                                                             # 3) 回収再開（取得済みの行で通知を試みる）
        self.assertEqual(_run(sp._notify_pending(r["pending_id"], r["notice_key"], None, None, bump_attempts=True, now=T0)), "")
        self.biz.assert_not_awaited()                                                             # 業務 push 0 回
        self.assertEqual(_run(rp.claim_notify(pid, "X", now=T0)), (rp.CLAIM_CLOSED, ""))        # claim 不成立（closed）
        row = self.pending()[0]
        self.assertEqual((row["state"], row["notify_state"], row["notify_owner"], row["attempts"]), ("human_replied", "none", None, 0))
        self.assertEqual([o for o in self.ops() if o["channel"] == "jikou_notice"], [])          # 台帳行も作らない
        self.assertEqual(rp.CLAIM_RESULTS, ("ok", "busy", "done", "not_found", "closed"))
        # not_needed（画面の操作）でも同じ
        pid2 = self.reg(user=USER2, inbound_event_id="e-i", notice_key=KEY_A)
        self.assertEqual(_run(rp.mark_not_needed(pid2)), "ok")
        self.assertEqual(_run(rp.claim_notify(pid2, "Y", now=T0)), (rp.CLAIM_CLOSED, ""))
        self.biz.assert_not_awaited()

    def test_recovery_still_touches_only_business_notices(self):
        # 裁定（前票と同じ）: fix4 後も回収は業務 LINE 通知に閉じる
        self.on()
        self.touch("e1")
        for i in range(2):
            self.reg(inbound_event_id=f"e-n{i}", notice_key=KEY_A)
        reply_spy = AsyncMock(side_effect=AssertionError("customer reply must not be sent by recovery"))
        push_spy = AsyncMock(side_effect=AssertionError("customer push must not be sent by recovery"))
        with patch.object(line_channel, "reply_with_push_fallback", reply_spy), patch.object(line_channel, "push_text", push_spy):
            res = _run(sp.run_pending_retry_job(now=T0))
        self.assertEqual(res["notify_recovered"], 2)
        reply_spy.assert_not_awaited()
        push_spy.assert_not_awaited()
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual({o["channel"] for o in self.ops()}, {"jikou_notice"})
        self.assertEqual({r["customer_send"] for r in self.pending()}, {"none"})
        self.assertTrue(_run(sp.first_reply_due(USER)))
        self.assertEqual(sorted(r["notify_state"] for r in self.pending()), ["sent", "throttled"])
        self.assertEqual([r["attempts"] for r in self.pending()], [1, 1])                        # 確定時に 1 回だけ加算

if __name__ == "__main__":
    unittest.main()
