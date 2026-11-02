"""JIKOU-REPLY-Q1c-fix5（Codex R-Q1c-fix4 REJECT 対応・BC-20・司令塔裁定）

固定する仕様（業務 LINE 通知の回収に閉じる・顧客送信・J-0 には触れない）:
- BC-20 回収用 claim（claim_notify）の UPDATE は attempts < NOTIFY_RETRY_MAX を条件に含める。failed／attempts=2 の同じ行を A・B が
  一覧取得し、A が 3 回目を失敗確定（attempts=3）した後に B が古い一覧で再開しても、B の claim は不成立（CLAIM_EXHAUSTED）＝送信関数を
  呼ばずに終了する（送信関数の呼出し 1 回・最終 attempts=3・送信 2 回／attempts=4 にならない）。
"""

import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import (_FakeClient, _run, USER)  # noqa: E402
from test_jikou_reply_q1c_fix2 import _Fix2Mixin  # noqa: E402

from hub import line_channel  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_policy as sp  # noqa: E402

REPO = Path(__file__).parent
KEY_A = "pending_notice:keyA:2026-11-01"
T0 = datetime.datetime(2026, 11, 1, 3, 0, tzinfo=datetime.timezone.utc)


# ── BC-20: 回収用 claim は attempts < 上限 を再確認（一覧取得後に他者が上限まで確定した行には送らない） ─────────
class TestClaimRechecksAttempts(_Fix2Mixin):
    def _failed_row_with_attempts(self, n: int, event: str = "e-x"):
        pid = self.reg(inbound_event_id=event, notice_key=KEY_A)
        self.assertTrue(_run(rp.set_notify_state_unowned(pid, rp.NOTIFY_FAILED)))
        for _ in range(n):
            self.assertTrue(_run(rp.bump_attempts_unowned(pid)))
        self.assertEqual(self.pending()[-1]["attempts"], n)
        return pid

    def test_stale_listing_after_third_failure_gets_no_second_send(self):
        self.on()
        self.touch("e1")
        pid = self._failed_row_with_attempts(rp.NOTIFY_RETRY_MAX - 1)                             # failed・attempts=2
        listed_a = _run(rp.list_notify_retry(now=T0))
        listed_b = _run(rp.list_notify_retry(now=T0))                                             # 1) A・B がともに attempts=2 を読む
        self.assertEqual([(r["pending_id"], r["attempts"]) for r in listed_a], [(pid, 2)])
        self.assertEqual([(r["pending_id"], r["attempts"]) for r in listed_b], [(pid, 2)])
        reply_spy = AsyncMock(side_effect=AssertionError("customer reply must not be sent by recovery"))
        push_spy = AsyncMock(side_effect=AssertionError("customer push must not be sent by recovery"))
        with patch.object(line_channel, "reply_with_push_fallback", reply_spy), patch.object(line_channel, "push_text", push_spy):
            self.biz.return_value = False                                                         # 2) A の 3 回目は失敗
            ra = listed_a[0]
            self.assertEqual(_run(sp._notify_pending(ra["pending_id"], ra["notice_key"], None, None, bump_attempts=True, now=T0)),
                             "failed")
            row = self.pending()[0]
            self.assertEqual((row["notify_state"], row["attempts"], row["notify_owner"]), ("failed", 3, None))   # 3 に確定・claim 解放
            self.assertEqual(self.biz.await_count, 1)
            self.biz.return_value = True
            rb = listed_b[0]                                                                      # 3) B が古い一覧（attempts=2）で再開
            self.assertEqual(_run(sp._notify_pending(rb["pending_id"], rb["notice_key"], None, None, bump_attempts=True, now=T0)), "")
        self.assertEqual(self.biz.await_count, 1)                                                 # 送信関数の呼出しは 1 回のまま
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["attempts"], row["notify_owner"], row["notify_claimed_at"]),
                         ("failed", 3, None, None))                                               # 最終 attempts=3・B の claim 痕跡なし
        self.assertEqual(_run(rp.claim_notify(pid, "B", now=T0)), (rp.CLAIM_EXHAUSTED, ""))      # B の claim 不成立（exhausted）
        self.assertEqual(self.pending()[0]["notify_owner"], None)
        self.assertEqual(len([o for o in self.ops() if o["channel"] == "jikou_notice"]), 1)       # 台帳は A の 1 回分だけ
        # 回収ジョブでも同じ（上限到達＝対象外・送らない）
        later = T0 + datetime.timedelta(seconds=rp.NOTIFY_CLAIM_LEASE_SECONDS + 1)
        self.assertEqual(_run(rp.list_notify_retry(now=later)), [])
        res = _run(sp.run_pending_retry_job(now=later))
        self.assertEqual((res["notify_retried"], self.biz.await_count), (0, 1))
        # 裁定（前票と同じ）: 回収は業務 LINE 通知に閉じる（顧客送信プリミティブ未呼出・顧客 HTTP なし・台帳は jikou_notice のみ・J-0 在庫不変）
        reply_spy.assert_not_awaited()
        push_spy.assert_not_awaited()
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual({o["channel"] for o in self.ops()}, {"jikou_notice"})
        self.assertEqual({r["customer_send"] for r in self.pending()}, {"none"})
        self.assertTrue(_run(sp.first_reply_due(USER)))

    def test_claim_requires_attempts_below_max(self):
        pid = self._failed_row_with_attempts(rp.NOTIFY_RETRY_MAX)                                 # attempts=3（上限）
        self.assertEqual(_run(rp.claim_notify(pid, "A", now=T0)), (rp.CLAIM_EXHAUSTED, ""))
        row = self.pending()[0]
        self.assertEqual((row["notify_owner"], row["notify_claimed_at"], row["attempts"], row["notify_state"]),
                         (None, None, 3, "failed"))                                               # 無変更
        pid2 = self._failed_row_with_attempts(rp.NOTIFY_RETRY_MAX - 1, event="e-y")               # attempts=2 は従来どおり取れる
        self.assertEqual(_run(rp.claim_notify(pid2, "A", now=T0)), (rp.CLAIM_OK, KEY_A))
        self.assertEqual(_run(rp.claim_notify(pid2, "B", now=T0)), (rp.CLAIM_BUSY, ""))          # リース内は busy（exhausted ではない）
        self.assertTrue(_run(rp.set_notify_state(pid2, rp.NOTIFY_FAILED, owner="A", bump=True)))  # A が 3 回目を失敗確定
        self.assertEqual(_run(rp.claim_notify(pid2, "B", now=T0)), (rp.CLAIM_EXHAUSTED, ""))     # 解放後でも上限＝取れない
        self.assertEqual(rp.CLAIM_RESULTS, ("ok", "busy", "done", "not_found", "closed", "exhausted"))
        src = (REPO / "hub" / "reply_pending.py").read_text(encoding="utf-8")
        self.assertEqual(src.count("reply_pending.c.attempts < int(NOTIFY_RETRY_MAX)"), 1)       # claim の UPDATE 条件（list は max_attempts 引数）


if __name__ == "__main__":
    unittest.main()
