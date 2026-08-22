"""AUTOREPLY-GEN2: 自動返信の第2世代改修。

固定する仕様:
- 要件1: 送信直前サニタイズ——markdown 記号（**強調**・`コード`・#見出し・
  ---水平線・行頭箇条書き）の平文化・許可外絵文字の除去。和文記法
  （・①━━━ 等）は不変。プレースホルダ/内部マーカー残存は fatal=送信せず
  承認降格。
- 要件2: 上限文字数（既定300・env AUTOREPLY_MAX_CHARS）と質問数上限（2）を
  サーバ側検証。超過は自動送信せず承認降格（切り詰めない）。ヒアリング定型
  テンプレブロック（━━━━）は長さ免除。
- 要件3: 顧客対応・ヒアリング両経路に会話履歴+収集済み項目台帳
  （App21 の正+画像受領マーカー・fail-open）を注入し既知の再質問を禁止。
  ヒアリングはメモリ消失時に App28 から履歴復元+App28 へ永続化。
- 要件4: 画像メッセージ=固定受領定型+弁護士通知のみ（AI に画像判断させ
  ない）。pause→停止リスト→人対応のゲート順はテキストと同一。自由文での
  写真依頼は禁止語（弁護士確定の定型文言のみ許可）。
- 要件5: PENDING の文脈化はカテゴリ名ベース閉集合文言・大野の文言確定
  （PENDING_CONTEXT_ENABLED=1）まで現行 PENDING_REPLY。
- 要件6: 法テラス質問には HOTERASU_STANDARD_REPLY をサーバー側で決定的に
  到達させる（逐語で含まない返信は承認降格+即時定型 hoterasu で送信・
  二度送りはマーカーで抑止）。
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token",
    "DOCUMENT_WEBHOOK_SECRET": "doc-secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
from hub import reply_sanitizer as rs  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


class TestSanitizer(unittest.TestCase):
    def test_markdown_flattened(self):
        text = ("# 見出し\n**重要**な`点`です\n---\n- 項目1\n* 項目2\n"
                "1. 番号つき")
        out, issues, fatal = rs.sanitize_reply(text)
        self.assertFalse(fatal)
        self.assertNotIn("**", out)
        self.assertNotIn("#", out)
        self.assertNotIn("`", out)
        self.assertNotIn("---", out)
        self.assertIn("重要な点です", out)
        self.assertIn("・項目1", out)
        self.assertIn("・項目2", out)
        self.assertIn("1. 番号つき", out)   # 和文の番号書きは対象外
        self.assertTrue(issues)

    def test_japanese_notation_untouched(self):
        text = ("━━━━━━━\n①債権者名\n・訴状が届いた\n※番号でお答えください\n"
                "〒333-0844")
        out, issues, fatal = rs.sanitize_reply(text)
        self.assertEqual(out, text)
        self.assertEqual(issues, [])
        self.assertFalse(fatal)

    def test_emoji_removed_unless_allowed(self):
        out, issues, _f = rs.sanitize_reply("了解です😊✨")
        self.assertEqual(out, "了解です")
        self.assertIn("許可外の絵文字を除去", issues)
        out2, _i, _f = rs.sanitize_reply("了解です😊", frozenset("😊"))
        self.assertEqual(out2, "了解です😊")

    def test_placeholder_residue_is_fatal(self):
        for text in ("次を添えます「<<HOTERASU_REPLY>>」",
                     "{{顧客名}}様", "[KINTONE_RECORD]{}[/KINTONE_RECORD]"):
            with self.subTest(text=text[:12]):
                _o, issues, fatal = rs.sanitize_reply(text)
                self.assertTrue(fatal)
                self.assertIn("プレースホルダ/内部マーカー残存", issues)

    def test_structure_limits(self):
        self.assertEqual(rs.structure_violations("あ" * 300), [])
        v = rs.structure_violations("あ" * 301)
        self.assertTrue(v and "文字数超過" in v[0])
        # ヒアリング定型テンプレブロックは長さ免除
        self.assertEqual(
            rs.structure_violations("━━━━━━━\n" + "あ" * 400), [])
        v = rs.structure_violations("A？B？C？")
        self.assertTrue(any("質問数超過" in x for x in v))
        self.assertEqual(rs.structure_violations("A？B？"), [])

    def test_max_chars_env_adjustable(self):
        with patch.dict(os.environ, {"AUTOREPLY_MAX_CHARS": "100"}):
            self.assertTrue(rs.structure_violations("あ" * 150))
            self.assertEqual(rs.structure_violations("あ" * 100), [])


class TestPhotoForbiddenWords(unittest.TestCase):
    def test_free_form_photo_request_forbidden(self):
        for text in ("よろしければ書類の写真を送ってください",
                     "画像を送っていただけますか",
                     "お写真をお送りください"):
            with self.subTest(text=text):
                self.assertTrue(cr.find_forbidden_words(text))

    def test_canonical_photo_phrases_allowed(self):
        for text in (cr.URGENT_SEIZURE_PANIC_REPLY,
                     cr.COURT_DOC_REQUEST_REPLY,
                     "お手元にある場合は" + cr.HEARING_PHOTO_GUIDE_PHRASE,
                     "状況を確認いたしますので、"
                     "差押えに関する書類の写真をこのLINEにお送りいただけますか"):
            with self.subTest(text=text[:15]):
                self.assertEqual(
                    [h for h in cr.find_forbidden_words(text)
                     if "写真依頼" in h], [])


def _result(reply, category="手続きの一般的な流れ", auto=True):
    return {"reply": reply, "category": category, "auto_send": auto}


class TestGuardsGen2(unittest.TestCase):
    def test_sanitize_fatal_demotes(self):
        g = cr.apply_server_guards(_result("こんにちは"), [], "こんにちは",
                                   sanitize_fatal=True)
        self.assertFalse(g.can_auto_send)
        self.assertIn("プレースホルダ/内部マーカー残存", g.demotion_reasons)

    def test_length_demotes_without_truncation(self):
        g = cr.apply_server_guards(_result("あ" * 301), [], "教えて")
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("文字数超過" in r for r in g.demotion_reasons))

    def test_question_count_demotes(self):
        g = cr.apply_server_guards(
            _result("いつですか？どこですか？なぜですか？"), [], "教えて")
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("質問数超過" in r for r in g.demotion_reasons))

    def test_hoterasu_enforced_deterministically(self):
        # 標準回答を逐語で含まない返信は降格+即時定型 hoterasu
        g = cr.apply_server_guards(
            _result("はい、ご利用いただけます"), [],
            "法テラス使えますか？")
        self.assertFalse(g.can_auto_send)
        self.assertIn("法テラス標準回答の不使用", g.demotion_reasons)
        self.assertEqual(g.immediate_notice, "hoterasu")
        self.assertEqual(g.immediate_notice_text, cr.HOTERASU_STANDARD_REPLY)

    def test_hoterasu_verbatim_passes(self):
        g = cr.apply_server_guards(
            _result(cr.HOTERASU_STANDARD_REPLY), [], "法テラス使えますか？")
        self.assertTrue(g.can_auto_send)

    def test_hoterasu_dedup_marker(self):
        history = [{"role": "assistant",
                    "content": cr.HOTERASU_STANDARD_REPLY}]
        g = cr.apply_server_guards(
            _result("はい、ご利用いただけます"), history,
            "法テラスは？")
        self.assertFalse(g.can_auto_send)
        self.assertEqual(g.immediate_notice, "none")  # 二度送り抑止


class TestPendingContext(unittest.TestCase):
    def test_disabled_by_default(self):
        self.assertEqual(cr.pending_reply_for("費用の定型案内"),
                         cr.PENDING_REPLY)

    def test_enabled_by_env_flag(self):
        with patch.dict(os.environ, {"PENDING_CONTEXT_ENABLED": "1"}):
            self.assertEqual(cr.pending_reply_for("費用の定型案内"),
                             cr.PENDING_BY_CATEGORY["費用の定型案内"])
            self.assertEqual(cr.pending_reply_for("未知カテゴリ"),
                             cr.PENDING_REPLY)

    def test_proposal_texts_pass_all_guards(self):
        # 文言案は自らのサニタイズ/構成/禁止語検査に適合する（閉集合）
        for category, text in cr.PENDING_BY_CATEGORY.items():
            with self.subTest(category=category):
                out, issues, fatal = rs.sanitize_reply(text)
                self.assertEqual(out, text)
                self.assertEqual(issues, [])
                self.assertFalse(fatal)
                self.assertEqual(rs.structure_violations(text), [])
                self.assertEqual(cr.find_forbidden_words(text), [])


class TestKnownItems(unittest.TestCase):
    def test_ledger_from_app21_and_history(self):
        record = {"問い合わせ業者名": {"value": "アコム"},
                  "借入時期_テキスト": {"value": "2010年頃"},
                  "最終返済日_テキスト": {"value": ""},
                  "裁判所書類": {"value": "何も届いていない"}}
        history = [{"role": "assistant", "content": cr.IMAGE_RECEIPT_REPLY}]
        items = cr.build_known_items(record, history)
        self.assertEqual(items["債権者名"], "アコム")
        self.assertEqual(items["借入時期"], "2010年頃")
        self.assertNotIn("最終返済日", items)     # 空=未知（fail-open）
        self.assertEqual(items["書類写真"], "受領済み")

    def test_ledger_fail_open(self):
        self.assertEqual(cr.build_known_items(None, []), {})

    def test_prompt_injection(self):
        p = cr.build_system_prompt(status="決済完了",
                                   known_items={"債権者名": "アコム"})
        self.assertIn("収集済み項目（既知・再質問禁止）", p)
        self.assertIn("- 債権者名: アコム", p)
        p2 = cr.build_system_prompt(status="決済完了")
        self.assertNotIn("収集済み項目（既知・再質問禁止）", p2)

    def test_ask_claude_injects_known_items(self):
        captured = {}

        async def fake_create(client, **kw):
            captured.update(kw)
            return MagicMock()

        with patch.object(main, "create_message_with_fallback", fake_create), \
             patch.object(main, "extract_text", lambda _r: "了解です"):
            _run(main.ask_claude("Uask-gen2", "こんにちは",
                                 known_items={"債権者名": "アコム"}))
        main.conversation_histories.pop("Uask-gen2", None)
        self.assertIn("収集済み項目（既知・再質問禁止）", captured["system"])
        self.assertIn("- 債権者名: アコム", captured["system"])
        self.assertTrue(captured["system"].startswith(main.SYSTEM_PROMPT))


class _AsyncBase(unittest.TestCase):
    def setUp(self):
        self.user = f"Utest-{self.id().rsplit('.', 1)[-1]}"

    def tearDown(self):
        main.conversation_histories.pop(self.user, None)


class TestImageEvent(_AsyncBase):
    def _patches(self, record=None):
        return (
            patch.object(main.autoreply_stoplist, "is_suppressed",
                         AsyncMock(return_value=False)),
            patch.object(main, "get_app21_record",
                         AsyncMock(return_value=record)),
            patch.object(main, "_line_reply_with_fallback", AsyncMock()),
            patch.object(main, "save_to_chatlog", AsyncMock()),
            patch("hub.notify.notify_business", AsyncMock(return_value=True)),
            patch.object(main, "ATTORNEY_LINE_USER_ID", "Uattorney"),
        )

    def test_receipt_reply_and_notify(self):
        p = self._patches()
        with p[0], p[1], p[2] as reply, p[3] as log, p[4] as notify, p[5]:
            _run(main._process_line_image_event("tok", self.user))
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[2], cr.IMAGE_RECEIPT_REPLY)
        self.assertEqual(log.await_count, 2)      # 受信マーカー+受領応答
        self.assertEqual(log.await_args_list[0].args[2],
                         cr.IMAGE_INBOUND_MARKER)
        notify.assert_awaited_once()
        self.assertIn("書類写真受領", notify.await_args.args[1])

    def test_hearing_history_gets_receipt_marker(self):
        main.conversation_histories[self.user] = [
            {"role": "user", "content": "こんにちは"},
            {"role": "assistant", "content": "テンプレ"}]
        p = self._patches()
        with p[0], p[1], p[2], p[3], p[4], p[5]:
            _run(main._process_line_image_event("tok", self.user))
        history = main.conversation_histories[self.user]
        self.assertEqual(history[-1]["content"], cr.IMAGE_RECEIPT_REPLY)
        items = cr.build_known_items(None, history)
        self.assertEqual(items.get("書類写真"), "受領済み")

    def test_human_mode_silent(self):
        record = {"response_mode": {"value": "人対応"},
                  "顧客名": {"value": "試験太郎"}}
        p = self._patches(record)
        with p[0], p[1], p[2] as reply, p[3] as log, p[4] as notify, p[5]:
            _run(main._process_line_image_event("tok", self.user))
        reply.assert_not_awaited()               # 顧客へは完全無言
        self.assertEqual(log.await_count, 1)
        notify.assert_awaited_once()

    def test_pause_gate(self):
        with patch.object(main, "_autoreply_paused", lambda: True), \
             patch.object(main, "_handle_paused_inbound",
                          AsyncMock()) as paused, \
             patch.object(main, "_line_reply_with_fallback",
                          AsyncMock()) as reply:
            _run(main._process_line_image_event("tok", self.user))
        paused.assert_awaited_once()
        self.assertEqual(paused.await_args.args[1], cr.IMAGE_INBOUND_MARKER)
        reply.assert_not_awaited()

    def test_stoplist_gate(self):
        with patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=True)), \
             patch.object(main, "_handle_suppressed_inbound",
                          AsyncMock()) as sup, \
             patch.object(main, "_line_reply_with_fallback",
                          AsyncMock()) as reply:
            _run(main._process_line_image_event("tok", self.user))
        sup.assert_awaited_once()
        reply.assert_not_awaited()


class TestHearingSendGate(_AsyncBase):
    def _run_event(self, model_reply, *, seeded_history=None):
        reply = AsyncMock()
        log = AsyncMock()
        queue = AsyncMock(return_value="q-1")
        with patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=False)), \
             patch.object(main, "get_app21_record",
                          AsyncMock(return_value=None)), \
             patch.object(main, "get_recent_chat_history",
                          AsyncMock(return_value=seeded_history or [])), \
             patch.object(main, "ask_claude",
                          AsyncMock(return_value=model_reply)), \
             patch.object(main, "_line_reply_with_fallback", reply), \
             patch.object(main, "save_to_chatlog", log), \
             patch.object(main, "save_to_approval_queue", queue):
            _run(main._process_line_event("tok", self.user, "こんにちは"))
        return reply, log, queue

    def test_markdown_reply_sanitized_before_send(self):
        reply, _log, queue = self._run_event("**ご案内**です😊")
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[2], "ご案内です")
        queue.assert_not_awaited()

    def test_long_free_reply_demoted_not_truncated(self):
        reply, log, queue = self._run_event("あ" * 400)
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[2], cr.PENDING_REPLY)
        queue.assert_awaited_once()
        self.assertIn("文字数超過",
                      queue.await_args.kwargs["reason"])
        self.assertIn("ヒアリング送信ゲートで降格",
                      queue.await_args.kwargs["reason"])

    def test_canonical_template_block_passes(self):
        template = ("━━━━━━━\n①債権者名\n②おおよその借入時期\n"
                    "（2）過去5年以内に返済しましたか？\n"
                    "④10年以内に裁判所から書類は届きましたか？\n"
                    "━━━━━━━\n" + "説明" * 120)
        reply, _log, queue = self._run_event(template)
        self.assertEqual(reply.await_args.args[2], template)
        queue.assert_not_awaited()

    def test_placeholder_residue_demoted(self):
        reply, _log, queue = self._run_event("こちらです<<SEIZURE_SCOPE>>")
        self.assertEqual(reply.await_args.args[2], cr.PENDING_REPLY)
        queue.assert_awaited_once()
        self.assertIn("プレースホルダ",
                      queue.await_args.kwargs["reason"])

    def test_hearing_turns_logged_to_chatlog(self):
        _reply, log, _q = self._run_event("こんにちは。ご用件をどうぞ。")
        cats = [c.args[3] for c in log.await_args_list]
        self.assertEqual(cats, ["ヒアリング", "ヒアリング"])
        roles = [c.args[1] for c in log.await_args_list]
        self.assertEqual(roles, ["user", "assistant"])

    def test_history_seeded_from_chatlog_when_memory_empty(self):
        seeded = [{"role": "assistant", "content": "先頭は落ちる"},
                  {"role": "user", "content": "前回の質問"},
                  {"role": "assistant", "content": "前回の回答"}]
        self.assertNotIn(self.user, main.conversation_histories)
        self._run_event("承知しました。", seeded_history=seeded)
        history = main.conversation_histories[self.user]
        self.assertEqual(history[0],
                         {"role": "user", "content": "前回の質問"})

    def test_normalize_history_contract(self):
        rows = [{"role": "assistant", "content": "a"},
                {"role": "user", "content": "u1"},
                {"role": "user", "content": "u2"},
                {"role": "assistant", "content": "b"}]
        out = main._normalize_history(rows)
        self.assertEqual(out, [{"role": "user", "content": "u1\nu2"},
                               {"role": "assistant", "content": "b"}])


class TestCustomerFlowIntegration(_AsyncBase):
    def _handle(self, compose_result, user_message):
        reply = AsyncMock()
        queue = AsyncMock(return_value="q-1")
        with patch.object(cr, "_call_compose_reply",
                          AsyncMock(return_value=compose_result)), \
             patch.object(cr, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(cr, "save_to_chatlog", AsyncMock()), \
             patch.object(cr, "save_to_approval_queue", queue), \
             patch.object(cr, "_notify_attorney", AsyncMock()):
            _run(cr.handle_customer_message(
                user_id=self.user, user_message=user_message,
                reply_token="tok",
                app21_record={"status": {"value": "決済完了"}},
                reply_func=reply))
        return reply, queue

    def test_sanitized_before_auto_send(self):
        reply, queue = self._handle(
            {"reply": "**了解**です😊", "category": "挨拶・雑談",
             "auto_send": True}, "こんにちは")
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[1], "了解です")
        queue.assert_not_awaited()

    def test_hoterasu_reachable_end_to_end(self):
        reply, queue = self._handle(
            {"reply": "確認して折り返します", "category": "その他判断系",
             "auto_send": False}, "法テラスは使えますか？")
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[1],
                         cr.HOTERASU_STANDARD_REPLY)
        queue.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
