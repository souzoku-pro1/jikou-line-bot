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


if __name__ == "__main__":
    unittest.main()
