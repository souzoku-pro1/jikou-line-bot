"""AUTOREPLY-PAUSE-IMPL ①全体停止 flag（env AUTOREPLY_PAUSED）のテスト。

固定する契約:
- flag 判定はあらゆる外部作用より前（App21 参照・Claude・LINE 送信・
  承認キュー投入のいずれにも到達しない）
- ON: App21 の有無・ヒアリングセッション中かを問わず自動返信 0 件。
  受信記録（App28）と管理者通知（業務チャネル・人対応と同型）は継続。
  Claude API 呼び出しもスキップ
- App21 レコードなしの新規顧客は PII 配慮の匿名ID（userId 生値を通知に
  載せない）
- OFF（未設定/他値）: 従来挙動と完全一致（ヒアリングフローが Claude→返信へ
  進む回帰を pin）
- durable wrapper（_process_line_event_durable）経由でも
  processing→completed の観測記録は継続（耐久化は無影響）
kintone・LINE・Claude は全て mock。
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "t", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}
_SAVED = {k: os.environ.get(k) for k in _ENV}
os.environ.update(_ENV)

import main  # noqa: E402

for _k, _o in _SAVED.items():
    if _o is None:
        os.environ.pop(_k, None)
    else:
        os.environ[_k] = _o

USER = "U-pause-test-0001"


def run(coro):
    return asyncio.run(coro)


def app21_record(name="山田太郎", mode="自動", status="受任"):
    return {"顧客名": {"value": name},
            "response_mode": {"value": mode},
            "status": {"value": status}}


class _Base(unittest.TestCase):
    """外部作用を全て mock し、呼出し回数で契約を固定する共通ハーネス"""

    def setUp(self):
        self.reply = AsyncMock()
        self.claude = AsyncMock(return_value="こんにちは")
        self.chatlog = AsyncMock()
        self.notify = AsyncMock(return_value=True)
        self.customer = AsyncMock()
        self.app21 = AsyncMock(return_value=None)
        self.idem_check = AsyncMock(return_value=False)   # 既定=未保存
        for p in [
            patch.object(main, "_line_reply_with_fallback", new=self.reply),
            patch.object(main, "ask_claude", new=self.claude),
            patch.object(main, "save_to_chatlog", new=self.chatlog),
            patch.object(main, "handle_customer_message", new=self.customer),
            patch.object(main, "get_app21_record", new=self.app21),
            patch.object(main, "_paused_chatlog_already_saved",
                         new=self.idem_check),
            patch.object(main, "ATTORNEY_LINE_USER_ID", "U-attorney"),
            patch.object(main, "emit",
                         new=lambda v, *a, **k: str(v)),   # 内容検証用に素通し
            patch("hub.notify.notify_business", new=self.notify),
        ]:
            p.start()
            self.addCleanup(p.stop)
        # ヒアリングセッション状態の掃除（プロセス共有 dict のため）
        main.conversation_histories.pop(USER, None)
        main.hearing_completed.discard(USER)
        self.addCleanup(lambda: (main.conversation_histories.pop(USER, None),
                                 main.hearing_completed.discard(USER)))

    def _assert_no_autoreply_side_effects(self):
        self.reply.assert_not_awaited()
        self.claude.assert_not_awaited()
        self.customer.assert_not_awaited()


class TestPausedOn(_Base):
    def test_new_customer_without_app21_recorded_and_notified(self):
        """ON×App21なし（新規・ヒアリング前）: 返信/Claude 0・記録と通知あり・
        通知は匿名ID（userId 生値を載せない）——pause 中の negative"""
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "初めまして"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once_with(USER, "user", "初めまして",
                                              "", "no")
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("【自動返信停止中】", text)
        self.assertIn("匿名ID:", text)
        self.assertNotIn(USER, text, "userId 生値を通知に載せない（PII 配慮）")
        self.assertIn("初めまして", text, "本文は人対応と同型（emit 経由）")

    def test_existing_customer_uses_app21_name(self):
        """ON×App21あり: 顧客名で通知（人対応と同型）・返信/Claude 0"""
        self.app21.return_value = app21_record(name="山田太郎")
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "進捗どうですか"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("山田太郎", text)
        self.assertNotIn("匿名ID:", text)

    def test_mid_hearing_session_is_also_paused(self):
        """ON×ヒアリングセッション中: セッション有無を問わず全停止
        （flag 判定はルーティング判定より前）"""
        main.conversation_histories[USER] = [{"role": "user", "content": "x"}]
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "続きです"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once()
        self.notify.assert_awaited_once()

    def test_app21_lookup_failure_falls_back_to_anonymous(self):
        """App21 参照失敗は best-effort（記録・通知は匿名IDで継続）"""
        self.app21.side_effect = RuntimeError("kintone down")
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "こんにちは"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once()
        self.assertIn("匿名ID:", self.notify.await_args.args[1])

    def test_durable_wrapper_still_observes(self):
        """durable wrapper 経由でも processing→completed の観測は継続
        （inbound_event 耐久化は pause の影響を受けない）"""
        from hub import durable_inbound as di
        marks = {"processing": AsyncMock(), "completed": AsyncMock(),
                 "failed": AsyncMock()}
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                patch.object(di, "mark_line_processing",
                             new=marks["processing"]), \
                patch.object(di, "mark_line_completed",
                             new=marks["completed"]), \
                patch.object(di, "mark_line_failed", new=marks["failed"]):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-1"))
        marks["processing"].assert_awaited_once_with("evt-1")
        marks["completed"].assert_awaited_once_with("evt-1")
        marks["failed"].assert_not_awaited()
        self.reply.assert_not_awaited()

    def test_attorney_unset_still_records(self):
        """通知先未設定でも受信記録は行う（記録が主・通知は従）"""
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                patch.object(main, "ATTORNEY_LINE_USER_ID", ""):
            run(main._process_line_event("rt", USER, "こんにちは"))
        self.chatlog.assert_awaited_once()
        self.notify.assert_not_awaited()
        self._assert_no_autoreply_side_effects()


class TestFailClosed(_Base):
    """AUTOREPLY-01: 記録/通知の独立試行・失敗の伝播・retry 冪等性"""

    def _durable_marks(self):
        from hub import durable_inbound as di
        marks = {"processing": AsyncMock(), "completed": AsyncMock(),
                 "failed": AsyncMock()}
        for name, mock in (("mark_line_processing", marks["processing"]),
                           ("mark_line_completed", marks["completed"]),
                           ("mark_line_failed", marks["failed"])):
            p = patch.object(di, name, new=mock)
            p.start()
            self.addCleanup(p.stop)
        return marks

    def test_save_failure_notify_still_sent_and_durable_failed(self):
        """(i)(ii) save 失敗 → 通知は届く（失敗分類つき=人が知れる）＋
        durable は completed でなく failed（retry 可能）"""
        self.chatlog.side_effect = RuntimeError("kintone down")
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-1"))
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("受信記録に失敗しています", text)
        self.assertIn("RuntimeError", text, "失敗分類を通知に含める")
        marks["failed"].assert_awaited_once()
        self.assertEqual(marks["failed"].await_args.args,
                         ("evt-1", "PausedInboundError"))
        marks["completed"].assert_not_awaited()
        self.reply.assert_not_awaited()

    def test_notify_failure_record_kept_and_durable_failed(self):
        """(i)(ii) notify 失敗 → 記録は残る＋durable failed"""
        self.notify.side_effect = RuntimeError("push down")
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-2"))
        self.chatlog.assert_awaited_once()
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_not_awaited()

    def test_notify_declined_is_also_failure(self):
        """notify_business の False（allowlist 外等の未送信）も失敗として伝播"""
        self.notify.return_value = False
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-3"))
        self.chatlog.assert_awaited_once()
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_not_awaited()

    def test_both_failures_attempted_independently(self):
        """(i) 両方失敗 → 双方とも試行されている（片方の失敗で他方を
        スキップしない）＋durable failed"""
        self.chatlog.side_effect = RuntimeError("kintone down")
        self.notify.side_effect = RuntimeError("push down")
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-4"))
        self.chatlog.assert_awaited_once()
        self.notify.assert_awaited_once()
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_not_awaited()

    def test_retry_is_idempotent_for_chatlog_but_renotifies(self):
        """(iii) retry で App28 増分 0（冪等キーの既存確認）・通知は再送・
        2 回目は completed へ収束"""
        self.notify.side_effect = [RuntimeError("push down"), True]
        self.idem_check.side_effect = [False, True]   # 1回目=未保存/2回目=保存済み
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-5"))
            run(main._process_line_event_durable("rt", USER, "本文", "evt-5"))
        self.assertEqual(self.chatlog.await_count, 1, "App28 増分 0（冪等）")
        self.assertEqual(self.chatlog.await_args.args,
                         (USER, "user", "本文", "paused:evt-5", "no"),
                         "冪等キーを category へ保存（inbound event 識別子）")
        self.assertEqual(self.notify.await_count, 2, "通知は再送（沈黙より安全側）")
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_awaited_once()

    def test_success_path_still_completes(self):
        """成功経路の completed 回帰（fail-closed 化による退行なし）"""
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-6"))
        marks["completed"].assert_awaited_once()
        marks["failed"].assert_not_awaited()

    def test_non_durable_failure_logs_and_raises(self):
        """(ii) 非 durable 経路: 分類付き ERROR ログ（PII 非搭載）＋送出"""
        self.chatlog.side_effect = RuntimeError("kintone down")
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                self.assertLogs("main", level="ERROR") as logs, \
                self.assertRaises(main.PausedInboundError) as ctx:
            run(main._process_line_event("rt", USER, "秘密の本文"))
        joined = "\n".join(logs.output)
        self.assertIn("chatlog save failed", joined)
        self.assertIn("cls=RuntimeError", joined)
        self.assertNotIn("秘密の本文", joined, "ERROR ログに本文を載せない")
        self.assertNotIn("秘密の本文", str(ctx.exception),
                         "例外メッセージは分類のみ（PII 非搭載）")


class TestPausedOff(_Base):
    def _run_and_assert_normal_hearing(self):
        run(main._process_line_event("rt", USER, "こんにちは"))
        self.claude.assert_awaited_once()
        self.reply.assert_awaited_once()
        self.notify.assert_not_awaited()
        self.chatlog.assert_not_awaited()

    def test_unset_behaves_as_before(self):
        """OFF（未設定）: 従来どおりヒアリングフロー（Claude→返信）へ進む"""
        os.environ.pop("AUTOREPLY_PAUSED", None)
        self._run_and_assert_normal_hearing()

    def test_zero_value_behaves_as_before(self):
        """OFF（=0 等の他値）: ON は "1" のみ（既定=従来動作と完全一致）"""
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "0"}):
            self._run_and_assert_normal_hearing()

    def test_off_human_mode_unchanged(self):
        """OFF×人対応顧客: 既存「人対応」経路が従来どおり動く（回帰 pin）"""
        self.app21.return_value = app21_record(name="佐藤花子", mode="人対応")
        os.environ.pop("AUTOREPLY_PAUSED", None)
        run(main._process_line_event("rt", USER, "相談です"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once_with(USER, "user", "相談です",
                                              "", "no")
        self.notify.assert_awaited_once()
        self.assertIn("【人対応中】", self.notify.await_args.args[1])


if __name__ == "__main__":
    unittest.main()
