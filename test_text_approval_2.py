"""TEXT-APPROVAL-2（Q1c 文言 2 件の承認反映・2026-11-02・大野承認・Fable 経由）

固定する仕様:
- 承認台帳 hub/approved_texts.APPROVED_SHA256 への追加は ops_age_notice_v1 の 1 行だけ（SHA 59da41fc…＝send_policy.AGE_NOTICE_TEMPLATE
  の本文そのまま）。他キー（SAFE-1）は不変。
- J-0 文言（send_policy.J0_TEXT）は本文そのまま承認＝J0_TEXT と sha pin（0f1c3ca2…）は不変。
- 承認後、V2 ON なら run_age_notice_job は needs_approval ではなく送信（台帳を patch せずに確認）。未承認通知は出ない。
- V2 OFF では対応待ちが登録されず対象 0 件＝経路が動かないことは不変。
- fix1（Codex R-TA2 項目 3）: V2 OFF では run_age_notice_job／run_pending_retry_job が入口で停止（status=disabled）。承認済み台帳＋期限超過の
  対応待ちがあっても送信 0 回・台帳行 0。
"""

import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import (_run, USER2)  # noqa: E402
from test_jikou_reply_q1c_fix2 import _Fix2Mixin  # noqa: E402

from hub import approved_texts as AT  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_policy as sp  # noqa: E402

SHA_AGE = "59da41fcf2545b9aaa19175ea18315388e5b1e6df1c65bdbccaccacfc879652d"
SHA_J0 = "0f1c3ca29956f4000aface287c8dc9aa7c6907e93c9b7f1267858a6c2212b03d"
J0 = "ご連絡ありがとうございます。内容を確認のうえ、弁護士からこの LINE でご返信いたします。"
AGE_SENT = ("【対応待ち・未返信】\n未返信の対応待ち 3 件（60 分超 2 件・最長 90 分経過）・再処理待ち 0 件\n"
            "/app/pending からご確認ください。")


class TestTextApproval2(_Fix2Mixin):
    NOW = datetime.datetime(2026, 11, 2, 3, 0, tzinfo=datetime.timezone.utc)

    def setUp(self):
        super().setUp()
        AT.reset_pending_for_tests()
        self.addCleanup(AT.reset_pending_for_tests)
        # TEXT-APPROVAL-3（2026-11-04）追随: v2 承認後は公開ホストがあると v2（完全 URL）になるため、v1 の期待は「公開ホスト無し」を明示する
        _host = patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": ""})
        _host.start()
        self.addCleanup(_host.stop)

    def test_ledger_has_exactly_the_approved_line_and_j0_pin_is_unchanged(self):
        self.assertEqual(AT.APPROVED_SHA256["ops_age_notice_v1"], SHA_AGE)                        # 承認した SHA（全桁）
        self.assertEqual(AT.sha256_text(sp.AGE_NOTICE_TEMPLATE), SHA_AGE)                          # 本文・テンプレートは変えていない
        self.assertEqual(set(AT.APPROVED_SHA256), {AT.SAFE_TEXT_KEY, sp.AGE_NOTICE_TEXT_KEY,
                                                   sp.AGE_NOTICE_TEXT_KEY_V2})                     # TA2 の追加は v1 の 1 行・v2 は TEXT-APPROVAL-3（2026-11-04）
        self.assertEqual(AT.APPROVED_SHA256[AT.SAFE_TEXT_KEY], AT.sha256_text(AT.SAFE_TEXT))       # 他キー不変
        self.assertTrue(AT.is_approved(sp.AGE_NOTICE_TEXT_KEY, sp.AGE_NOTICE_TEMPLATE))
        self.assertIsNone(AT.approved_text(sp.AGE_NOTICE_TEXT_KEY, sp.AGE_NOTICE_TEMPLATE + "。"))   # 1 文字違えば不一致＝再承認
        self.assertEqual((sp.J0_TEXT, sp.J0_TEXT_SHA256), (J0, SHA_J0))                            # J-0 は本文そのまま承認（pin 不変）

    def test_age_notice_is_sent_after_approval_without_patching_the_ledger(self):
        self.on()                                                                                  # V2 ON
        self.reg(inbound_event_id="e-old1", now=self.NOW - datetime.timedelta(minutes=90))
        self.reg(inbound_event_id="e-old2", now=self.NOW - datetime.timedelta(minutes=61))
        self.reg(user=USER2, inbound_event_id="e-new", now=self.NOW - datetime.timedelta(minutes=5))
        self.assertEqual(sp.age_notice_text({"open_count": 3, "over_count": 2, "oldest_minutes": 90}, 0, 60), AGE_SENT)
        admin = AsyncMock(return_value=True)
        with patch("hub.notify.notify_admin_line", admin):
            res = _run(sp.run_age_notice_job(9, now=self.NOW))
        self.assertEqual((res["over_count"], res["queued"], res["notice"]), (2, 0, rp.NOTIFY_SENT))   # needs_approval ではなく送信
        self.assertEqual(self.biz.await_count, 1)
        self.assertEqual(self.biz.await_args.args[1], AGE_SENT)                                    # 数値差し込みのみ・スロット残なし
        admin.assert_not_awaited()                                                                 # 未承認通知は出ない
        self.assertEqual(AT.pending_unapproved_keys(), ())
        self.assertEqual(len([o for o in self.ops() if o["channel"] == "jikou_notice"]), 1)        # 台帳 1 行
        self.assertEqual(_run(sp.run_age_notice_job(9, now=self.NOW))["notice"], rp.NOTIFY_THROTTLED)   # 同じ枠は 1 通
        self.assertEqual(self.biz.await_count, 1)

    def test_v2_off_path_is_unchanged(self):
        self.assertFalse(sp.enabled())                                                             # V2 OFF（既定）
        res = _run(sp.run_age_notice_job(9, now=self.NOW))
        self.assertEqual(res, {"over_count": 0, "queued": 0, "notice": "", "status": "disabled"})  # fix1: 入口で停止
        self.biz.assert_not_awaited()
        self.assertEqual(self.pending(), [])

    # ── TEXT-APPROVAL-2-fix1（Codex R-TA2 項目 3）: V2 OFF では承認済みでも送らない（入口ゲート） ──────────────
    def test_v2_off_gate_blocks_age_notice_even_when_approved_and_overdue(self):
        self.assertFalse(sp.enabled())                                                             # V2 OFF
        self.assertTrue(AT.is_approved(sp.AGE_NOTICE_TEXT_KEY, sp.AGE_NOTICE_TEMPLATE))              # 承認済み台帳
        pid = self.reg(inbound_event_id="e-overdue", now=self.NOW - datetime.timedelta(minutes=90))  # 期限超過の対応待ち 1 件
        self.assertEqual(_run(rp.age_stats(threshold_minutes=60, now=self.NOW))["over_count"], 1)
        admin = AsyncMock(return_value=True)
        with patch("hub.notify.notify_admin_line", admin):
            res = _run(sp.run_age_notice_job(9, now=self.NOW))
        self.assertEqual(res, {"over_count": 0, "queued": 0, "notice": "", "status": "disabled"})  # 入口で停止
        self.biz.assert_not_awaited()                                                              # 送信 0 回
        admin.assert_not_awaited()
        self.assertEqual([o for o in self.ops() if o["channel"] == "jikou_notice"], [])             # 台帳行 0
        self.assertEqual(self.pending()[0]["pending_id"], pid)                                      # 行は触らない
        # 同じ系統の回収ジョブも入口で停止（failed の行があっても再試行しない）
        self.assertTrue(_run(rp.set_notify_state_unowned(pid, rp.NOTIFY_FAILED)))
        res2 = _run(sp.run_pending_retry_job(now=self.NOW))
        self.assertEqual(res2["status"], "disabled")
        self.assertEqual((res2["requeued"], res2["notify_retried"], res2["notify_recovered"]), (0, 0, 0))
        self.biz.assert_not_awaited()
        row = self.pending()[0]
        self.assertEqual((row["notify_state"], row["attempts"], row["notify_owner"]), ("failed", 0, None))   # 無変更
        # V2 ON にすると同じ状態から従来どおり送る（違いはゲートだけ）
        self.on()
        res3 = _run(sp.run_age_notice_job(9, now=self.NOW))
        self.assertEqual((res3["over_count"], res3["notice"], "status" in res3), (1, rp.NOTIFY_SENT, False))
        self.assertEqual(self.biz.await_count, 1)
        self.assertNotIn("{{", self.biz.await_args.args[1])
        # 入口ゲートの source pin（両ジョブとも enabled() を先頭で見る）
        src = (Path(__file__).parent / "hub" / "send_policy.py").read_text(encoding="utf-8")
        for fn in ("async def run_pending_retry_job", "async def run_age_notice_job"):
            body = src[src.index(fn):]
            self.assertLess(body.index("if not enabled():"), body.index("_db_configured()"), fn)


if __name__ == "__main__":
    unittest.main()
