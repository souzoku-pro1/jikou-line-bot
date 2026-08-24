"""AUTOREPLY-STYLE-1: 自動返信への「大野文体」の移植（文体規範+few-shot 見本）。

固定する仕様:
- 文体規範（7 項目）と見本 4 件を**両経路**（ヒアリング=main.SYSTEM_PROMPT・
  顧客対応=chat_responder.build_system_prompt）の prompt に収載する。単一の正
  （chat_responder.STYLE_SECTION）を両経路が共有する。
- 真似るのは文体のみ。見本中の数値・固有名詞・名乗り（「弁護士の大野と
  申します」）を Bot が引用しない旨を prompt に明記する。
- 見本は第 2 世代ガード（サニタイズ・300 字・質問数 2・禁止語・サーバ側
  ガード・ヒアリング送信ゲート）に適合する（票由来・改変禁止の逐語 pin）。
- 凍結文言（法テラス・PENDING 11 種・画像受領文言・ヒアリング定型ブロック・
  費用定型・即時定型・FAQ3 確定文言・承認済み定型指示）と、従来の prompt
  本文（ヒアリング=_HEARING_PROMPT_FROZEN・顧客対応=_SYSTEM_PROMPT_BASE から
  差し込みマーカーを除いた本文）は改変しない（main 時点の sha256 で pin）。
"""

import hashlib
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


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _run(coro):
    import asyncio
    return asyncio.run(coro)


# ── 票由来の見本（テスト側の凍結コピー・改変禁止の逐語対照） ─────────────────────
EXEMPLARS_FROZEN = {
    "見立てと提案": (
        "○○様 お問合せありがとうございます。弁護士の大野と申します。"
        "信用情報等ご確認いたしました。△△については○○様のおっしゃられるように"
        "譲渡済となっておりますね。そうすると譲渡先を開示していただき、"
        "その譲渡先に対して時効援用通知をご送付する流れとなりそうです。"
        "当事務所でもお手伝いさせていただくことはできますので、"
        "ご検討のほどよろしくお願いいたします。"),
    "不利益の正直な開示": (
        "信用情報については、延滞の文字は消去されるものの、"
        "遅いと時効援用通知の送付から5年程度は住宅ローンを組むことができない"
        "場合もございますので、ご了承いただきたく存じます。"),
    "質問への具体的回答": (
        "時効援用の手続きに際しまして、信用情報機関へのご連絡要求は必ず行って"
        "おります。もっとも、債権者側でもいつの時点で信用情報が完全に抹消される"
        "かは知りえません。そのため、送付から5年程度は完全に抹消されないものとして"
        "ご認識いただけると間違いございません。"),
    "進捗報告": (
        "お世話になっております。本日◯社に対して、時効援用通知を送付致します。"
        "今後は1ヶ月程度のお時間をいただき手続きを進めさせていただきます。"
        "少々お時間をいただきますが、引き続きよろしくお願い致します。"),
}

# 文体規範 7 項目（票の文言・各項目を識別する要語）
STYLE_RULE_PHRASES = (
    "結論を最初の1〜2文で言い切り",
    "金額・期間・次の行動を具体的に示す",
    "「確認します」で終わる文を避ける",
    "不利な情報も先回りで正直に開示する",
    "記号・箇条書き・罫線・絵文字なしのプレーン文",
    "「〜となります」「〜いただけますと」「よろしくお願い致します」",
    "相手の言葉を引き取ってから答える",
    "「○○様のおっしゃるように」",
    "宛名で始め「よろしくお願い致します」で結ぶ",
)

# 「文体のみ」裁定の明記（数値・固有名詞・名乗りの不引用と内容ルールの優先）
NO_QUOTE_PHRASES = (
    "見本の中の数値・期間・固有名詞",
    "法的見立ての中身は見本から引用しない",
    "弁護士確定定型・FAQ・顧客情報を出典とする",
    "「弁護士の大野と申します」）も引用しない",
    "弁護士本人として名乗らない",
    "文体規範が内容のルールに優先することはない",
)

# ── main 時点（4b61b41）の凍結文言 sha256 ────────────────────────────────────
FROZEN_SHA = {
    "hearing_prompt_body":
        "d1b1ef4743a814d80726434206fc2dbf73ac507327271c8bb9f24cbafd831883",
    "customer_prompt_base":
        "8e4f0625fbf998fb9472afa5ffa54eecd2940f8424c23509f5f7906b3ad3822c",
    "hearing_template_blocks":
        "ebd2dea63cb3e81b4f4e95235d49c78ac2933ea6f932ffcd19bff632fc78ab2e",
    "hoterasu":
        "93bd619e1270a61ef38af3dffb5ed6ea247a1838fe79c803f8ea324e016dd631",
    "pending_reply":
        "72f4332c206d683e55822b935680febaa4622dca6997d9abdb80c120a24f895c",
    "pending_by_category":
        "92527c6b3c9942092ff8e76ea2e073631a3217c510e473d70a55c33e85ca21ff",
    "image_receipt":
        "4cb4eedf022afb9ca08b6040d83a88a5f89417d1d2c1f1b1e24907cbf9450b59",
    "image_marker":
        "e8e3a1a6b89bae7bb1eb369843281d5af333d8cb4f0953ed83c99062230d03b0",
    "immediate_notice_texts":
        "b565c9fc4157eb584777735eaa422d46945b40798366e0adaae5afe4bc419c52",
    "faq3_canonical":
        "74035d95b0fda4efb5af025a628dadd7e7f625e0182a5b96d68bd632974f2587",
    "fee_guide":
        "ab4b02e474f234b3deaa2f47dad0a048578cd0dc7a77f682c383d89b73356e2f",
    "phone_instruction":
        "9b5003ff979dc6ccac5448359559dc6d52f4fecf39d97583555c48fde02c6ef6",
    "dunning_instruction":
        "69e6c211d46b2e3d695fe65d0a701855c3d7e68df4f641f219319e37d2724aef",
    "branching_example":
        "49a7dba8ba954ed6bcf5ab9b487a368fc6834f393370152b28249f8f2fea3125",
}

_CUSTOMER_STYLE_MARKER = "<<STYLE_SECTION>>"
_HEARING_KINTONE_HEADING = "【kintone登録について】"


def _customer_prompts() -> dict[str, str]:
    """顧客対応経路の組み立て済み prompt（受任前/受任後・既知項目あり）。"""
    return {
        "受任前": cr.build_system_prompt(status="決済完了",
                                         customer_name="試験太郎"),
        "受任後": cr.build_system_prompt(status="受任", customer_name="試験太郎"),
        "受任前_既知項目": cr.build_system_prompt(
            status="決済完了", known_items={"債権者名": "アコム"}),
    }


class TestExemplarsVerbatim(unittest.TestCase):
    """見本 4 件は票由来の逐語（改変禁止）。"""

    def test_exemplars_match_frozen_copy(self):
        self.assertEqual([lbl for lbl, _t in cr.STYLE_EXEMPLARS],
                         list(EXEMPLARS_FROZEN))
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                self.assertEqual(text, EXEMPLARS_FROZEN[label])

    def test_exemplars_contain_no_template_markers(self):
        # 置換マーカー（<<...>>）・format プレースホルダ（{}）・内部マーカーを
        # 含まない＝差し込み順序に依存せず、送信時 fatal にも該当しない
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                for bad in ("<<", ">>", "{", "}", "[KINTONE_", "━"):
                    self.assertNotIn(bad, text)
        for bad in ("<<", ">>", "{", "}", "[KINTONE_", "━"):
            self.assertNotIn(bad, cr.STYLE_SECTION)


class TestExemplarsPassGen2Guards(unittest.TestCase):
    """見本は第 2 世代ガードに適合する（見本が自らのガードで降格されない）。"""

    def test_sanitizer_leaves_exemplars_untouched(self):
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                for allowed in (frozenset(), cr.ALLOWED_CANONICAL_EMOJI):
                    out, issues, fatal = rs.sanitize_reply(text, allowed)
                    self.assertEqual(out, text)
                    self.assertEqual(issues, [])
                    self.assertFalse(fatal)

    def test_length_and_question_limits(self):
        # 既定 300 字・質問数 2（env による調整なしの既定値で検査）
        with patch.dict(os.environ, {"AUTOREPLY_MAX_CHARS": ""}):
            for label, text in cr.STYLE_EXEMPLARS:
                with self.subTest(label=label):
                    self.assertLessEqual(len(text), 300)
                    self.assertEqual(rs.structure_violations(text), [])
                    self.assertEqual(rs.structure_violations(
                        text, exempt_blocks=main.HEARING_TEMPLATE_BLOCKS), [])
                    self.assertEqual(len(rs._QUESTION_RE.findall(text)), 0)

    def test_no_forbidden_words(self):
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                self.assertEqual(cr.find_forbidden_words(text), [])

    def test_customer_server_guards_pass(self):
        # 自動送信可カテゴリで見本文をそのまま返しても降格されない
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                g = cr.apply_server_guards(
                    {"reply": text, "category": "手続きの一般的な流れ",
                     "auto_send": True}, [], "教えてください")
                self.assertTrue(g.can_auto_send, g.demotion_reasons)
                self.assertEqual(g.demotion_reasons, [])

    def test_hearing_send_gate_passes(self):
        # ヒアリング送信ゲート（サニタイズ+構成検証）で見本文がそのまま送信
        # される（承認キューへ降格されない）
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                user = f"Ustyle1-{hash(label) & 0xffff:x}"
                reply, queue = AsyncMock(), AsyncMock(return_value="q-1")
                with patch.object(main.autoreply_stoplist, "is_suppressed",
                                  AsyncMock(return_value=False)), \
                     patch.object(main, "get_app21_record",
                                  AsyncMock(return_value=None)), \
                     patch.object(main, "get_recent_chat_history",
                                  AsyncMock(return_value=[])), \
                     patch.object(main, "ask_claude",
                                  AsyncMock(return_value=text)), \
                     patch.object(main, "_line_reply_with_fallback", reply), \
                     patch.object(main, "save_to_chatlog", AsyncMock()), \
                     patch.object(main, "save_to_approval_queue", queue):
                    _run(main._process_line_event("tok", user, "こんにちは"))
                main.conversation_histories.pop(user, None)
                reply.assert_awaited_once()
                self.assertEqual(reply.await_args.args[2], text)
                queue.assert_not_awaited()


class TestStyleInBothPrompts(unittest.TestCase):
    """文体規範+見本が両経路の prompt に収載され、同一の正を共有する。"""

    def _all_prompts(self) -> dict[str, str]:
        return {"ヒアリング": main.SYSTEM_PROMPT, **_customer_prompts()}

    def test_style_section_present_once_in_each_route(self):
        for route, prompt in self._all_prompts().items():
            with self.subTest(route=route):
                self.assertEqual(prompt.count(cr.STYLE_SECTION), 1)
                self.assertEqual(prompt.count("【文体規範"), 1)
                self.assertEqual(prompt.count("【文体見本"), 1)

    def test_all_seven_rules_present(self):
        for route, prompt in self._all_prompts().items():
            for phrase in STYLE_RULE_PHRASES:
                with self.subTest(route=route, phrase=phrase):
                    self.assertIn(phrase, prompt)

    def test_all_four_exemplars_present_verbatim(self):
        for route, prompt in self._all_prompts().items():
            for i, (label, text) in enumerate(cr.STYLE_EXEMPLARS, start=1):
                with self.subTest(route=route, label=label):
                    self.assertIn(f"見本{i}（{label}）:\n{text}", prompt)

    def test_style_only_ruling_is_explicit(self):
        # 裁定: 真似るのは文体のみ。数値・固有名詞・名乗りの不引用と、内容
        # ルール（定型・承認制）の優先を両経路で明記
        for route, prompt in self._all_prompts().items():
            for phrase in NO_QUOTE_PHRASES:
                with self.subTest(route=route, phrase=phrase):
                    self.assertIn(phrase, prompt)

    def test_hearing_placement_and_supplement(self):
        p = main.SYSTEM_PROMPT
        self.assertEqual(p.count(_HEARING_KINTONE_HEADING), 1)
        self.assertLess(p.index("【文体規範"), p.index(_HEARING_KINTONE_HEADING))
        # 定型ブロックの後（初回テンプレより後）に置く
        self.assertLess(p.rindex("━━━━━"), p.index("【文体規範"))
        self.assertIn(main.HEARING_STYLE_SECTION, p)
        self.assertIn("文体規範は自由文の部分にのみ適用する", p)
        self.assertIn("顧客名が未回答の間は宛名を省略する", p)
        # 既存契約: 既知項目追記は SYSTEM_PROMPT の後ろに付く（gen2 と同じ）
        self.assertTrue(p.startswith("【友達追加・最初のメッセージへの自動返信】"))

    def test_customer_placement_and_no_residue(self):
        for route, prompt in _customer_prompts().items():
            with self.subTest(route=route):
                self.assertNotIn("<<", prompt)     # 置換マーカー残存なし
                self.assertNotIn(">>", prompt)
                self.assertLess(prompt.index("【会話スタイル（最重要）】"),
                                prompt.index("【文体規範"))
                self.assertLess(prompt.index("【文体規範"),
                                prompt.index("【カテゴリ選択肢】"))
        self.assertEqual(cr._SYSTEM_PROMPT_BASE.count(_CUSTOMER_STYLE_MARKER), 1)
        self.assertNotIn(_CUSTOMER_STYLE_MARKER, cr._SYSTEM_PROMPT_TMPL)

    def test_known_items_still_appended_after_style(self):
        p = cr.build_system_prompt(status="決済完了",
                                   known_items={"債権者名": "アコム"})
        self.assertLess(p.index("【文体見本"),
                        p.index("【収集済み項目（既知・再質問禁止）】"))
        captured = {}

        async def fake_create(client, **kw):
            captured.update(kw)
            return MagicMock()

        with patch.object(main, "create_message_with_fallback", fake_create), \
             patch.object(main, "extract_text", lambda _r: "了解です"):
            _run(main.ask_claude("Ustyle1-ask", "こんにちは",
                                 known_items={"債権者名": "アコム"}))
        main.conversation_histories.pop("Ustyle1-ask", None)
        self.assertTrue(captured["system"].startswith(main.SYSTEM_PROMPT))
        self.assertIn(cr.STYLE_SECTION, captured["system"])


class TestFrozenTextsUnchanged(unittest.TestCase):
    """凍結文言と従来 prompt 本文は main（4b61b41）時点から改変なし。"""

    def test_hearing_prompt_body_frozen(self):
        self.assertEqual(_sha(main._HEARING_PROMPT_FROZEN),
                         FROZEN_SHA["hearing_prompt_body"])
        # SYSTEM_PROMPT = 本文 + 差し込み（差し込みを外せば本文と逐語一致）
        restored = main.SYSTEM_PROMPT.replace(
            main.HEARING_STYLE_SECTION + "\n\n" + _HEARING_KINTONE_HEADING,
            _HEARING_KINTONE_HEADING, 1)
        self.assertEqual(restored, main._HEARING_PROMPT_FROZEN)
        # 定型ブロック抽出は差し込みの影響を受けない（免除集合不変）
        self.assertEqual(main.HEARING_TEMPLATE_BLOCKS,
                         main._extract_template_blocks(
                             main._HEARING_PROMPT_FROZEN))
        self.assertEqual(_sha("\n".join(main.HEARING_TEMPLATE_BLOCKS)),
                         FROZEN_SHA["hearing_template_blocks"])
        self.assertEqual(len(main.HEARING_TEMPLATE_BLOCKS), 2)

    def test_customer_prompt_base_frozen(self):
        restored = cr._SYSTEM_PROMPT_BASE.replace(
            _CUSTOMER_STYLE_MARKER + "\n\n", "", 1)
        self.assertEqual(_sha(restored), FROZEN_SHA["customer_prompt_base"])

    def test_canonical_texts_frozen(self):
        cases = {
            "hoterasu": cr.HOTERASU_STANDARD_REPLY,
            "pending_reply": cr.PENDING_REPLY,
            "pending_by_category": "\n".join(
                f"{k}\t{v}" for k, v in cr.PENDING_BY_CATEGORY.items()),
            "image_receipt": cr.IMAGE_RECEIPT_REPLY,
            "image_marker": cr.IMAGE_INBOUND_MARKER,
            "immediate_notice_texts": "\n".join(
                f"{k}\t{v}" for k, v in cr.IMMEDIATE_NOTICE_TEXTS.items()),
            "faq3_canonical": "\n".join(cr.FAQ3_CANONICAL_TEXTS),
            "fee_guide": cr.FEE_GUIDE_TEXT,
            "phone_instruction": cr.APPROVED_PHONE_INSTRUCTION,
            "dunning_instruction": cr.APPROVED_DUNNING_INSTRUCTION,
            "branching_example": cr.BRANCHING_GUIDANCE_EXAMPLE,
        }
        self.assertEqual(set(cases), set(FROZEN_SHA) - {
            "hearing_prompt_body", "customer_prompt_base",
            "hearing_template_blocks"})
        for key, text in cases.items():
            with self.subTest(key=key):
                self.assertEqual(_sha(text), FROZEN_SHA[key])
        self.assertEqual(len(cr.PENDING_BY_CATEGORY), 11)
        self.assertEqual(len(cr.IMMEDIATE_NOTICE_TEXTS), 6)

    def test_allowed_emoji_set_unchanged(self):
        # 見本は許可絵文字の収集元に加えない（許可集合は従来どおり空）
        self.assertEqual(cr.ALLOWED_CANONICAL_EMOJI, frozenset())


if __name__ == "__main__":
    unittest.main()
