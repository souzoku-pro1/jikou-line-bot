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


if __name__ == "__main__":
    unittest.main()
