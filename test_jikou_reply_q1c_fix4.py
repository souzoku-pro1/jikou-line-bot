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


if __name__ == "__main__":
    unittest.main()
