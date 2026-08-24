"""AUTOREPLY-STYLE-1: 自動返信への「大野文体」の移植（文体規範+few-shot 見本）。

固定する仕様:
- 文体規範（7 項目）と見本 4 件を**両経路**（ヒアリング=main.SYSTEM_PROMPT・
  顧客対応=chat_responder.build_system_prompt）の prompt に収載する。単一の正
  （chat_responder.STYLE_SECTION）を両経路が共有する。
- 真似るのは文体のみ。見本の匿名化記号・事案内容を Bot が引用しない旨と、
  弁護士本人として名乗らない旨を prompt に明記する。
- 凍結文言（法テラス・PENDING 11 種・画像受領文言・ヒアリング定型ブロック・
  費用定型・即時定型・FAQ3 確定文言・承認済み定型指示）と、従来の prompt
  本文（ヒアリング=_HEARING_PROMPT_FROZEN・顧客対応=_SYSTEM_PROMPT_BASE から
  差し込みマーカーを除いた本文）は改変しない（main 時点の sha256 で pin）。

fix1（R-AUTOREPLY-STYLE-1 STYLE-01 HIGH・見本の無害化+サーバ側防壁の二層）:
- [A] 見本1 から名乗り文を除去。見本2〜4 の案件固有の数値・期間・法的内容を
  文体を保った内容なしの言い回しへ置換（匿名化記号は ○○/△△ の 2 種）。
- [B] サーバ側防壁（両経路・style_guard_violations）: 弁護士本人の名乗り
  （NFKC+空白除去のうえ閉集合）・見本の匿名化記号の残存・旧見本由来の
  無根拠表現（確定定型/FAQ に根拠のない旧見本固有の言い回し）を承認降格。
  FAQ に根拠のある語（5年程度・1ヶ月程度・ローン等）は従来どおり許容。
- [C] テストの向き: 修正後見本=**形式検査**（サニタイズ・300 字・質問数・
  禁止語）に適合し、匿名化記号を実名に埋めた形は全ガード通過。見本逐語
  （記号残存）・旧見本の引用・名乗り各形=**拒否**の negative。

fix2（大野確定による見本文言の差し替え・見本1〜3・見本4 は fix1 のまま）:
- 見本2・3 に戻した具体値（5年程度・住宅ローン）は FAQ 確定文言に根拠がある
  語＝fix1 防壁の「FAQ 根拠あり・許容」分類と整合（見本逐語は形式検査と
  [B] 防壁の双方を通る）。見本注記は「根拠なき引用の禁止」の趣旨へ微調整。
- 無根拠語の降格閉集合・名乗り検知 4 形・記号残存検知は不変。
- 本票由来の期待値更新: EXEMPLARS_FROZEN（見本1〜3）・見本に無い語の集合・
  NO_QUOTE_PHRASES（注記の調整分）。
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


# ── fix2（大野確定）後の見本（テスト側の凍結コピー・逐語対照） ──────────────────
EXEMPLARS_FROZEN = {
    "見立てと提案": (
        "○○様 お問合せありがとうございます。"
        "信用情報等ご確認いたしました。△△については○○様のおっしゃられるように"
        "譲渡済となっておりますね。そうすると、当事務所から原債権者に譲渡先を"
        "確認し、その譲渡先に対して時効援用通知をご送付する流れとなりそうです。"
        "当事務所でもお手伝いさせていただくことはできますので、"
        "ご検討のほどよろしくお願いいたします。"),
    "不利益の正直な開示": (
        "信用情報については、お手続きにより解消される部分はあるものの、"
        "時効援用通知の送付から5年程度は住宅ローンを組むことができない"
        "場合もございますので、あらかじめご了承いただきたく存じます。"),
    "質問への具体的回答": (
        "ご質問の点につきましては、当事務所で対応しております。"
        "もっとも、その結果がいつの時点で反映されるかは債権者にも分かりません。"
        "そのため、送付から5年程度は反映されないものとしてご認識いただけると"
        "間違いございません。"),
    "進捗報告": (
        "お世話になっております。本日△△に対して、時効援用通知を送付致します。"
        "今後は結果の確認までお時間をいただき、手続きを進めさせていただきます。"
        "少々お時間をいただきますが、引き続きよろしくお願い致します。"),
}

# ── 旧見本（AUTOREPLY-STYLE-1 e595ae6 時点・fix1 で prompt から除去済み） ─────────
# モデルが旧見本の内容を引用した出力=自動送信されないことを固定する negative 用
OLD_EXEMPLARS = {
    "旧1_名乗り": (
        "○○様 お問合せありがとうございます。弁護士の大野と申します。"
        "信用情報等ご確認いたしました。△△については○○様のおっしゃられるように"
        "譲渡済となっておりますね。そうすると譲渡先を開示していただき、"
        "その譲渡先に対して時効援用通知をご送付する流れとなりそうです。"
        "当事務所でもお手伝いさせていただくことはできますので、"
        "ご検討のほどよろしくお願いいたします。"),
    "旧2_住宅ローン": (
        "信用情報については、延滞の文字は消去されるものの、"
        "遅いと時効援用通知の送付から5年程度は住宅ローンを組むことができない"
        "場合もございますので、ご了承いただきたく存じます。"),
    "旧3_抹消": (
        "時効援用の手続きに際しまして、信用情報機関へのご連絡要求は必ず行って"
        "おります。もっとも、債権者側でもいつの時点で信用情報が完全に抹消される"
        "かは知りえません。そのため、送付から5年程度は完全に抹消されないものとして"
        "ご認識いただけると間違いございません。"),
    "旧4_◯社": (
        "お世話になっております。本日◯社に対して、時効援用通知を送付致します。"
        "今後は1ヶ月程度のお時間をいただき手続きを進めさせていただきます。"
        "少々お時間をいただきますが、引き続きよろしくお願い致します。"),
}
# 旧見本ごとに期待する降格理由の分類（[B] 3 種の閉集合）
OLD_EXEMPLAR_EXPECTED_REASON = {
    "旧1_名乗り": "弁護士本人の名乗り検出",
    "旧2_住宅ローン": "旧見本由来の無根拠表現",
    "旧3_抹消": "旧見本由来の無根拠表現",
    "旧4_◯社": "見本の匿名化記号の残存",
}

# 名乗り各形（Codex 指定: 空白挟み・全角空白含む）
SELF_INTRO_FORMS = (
    "弁護士の大野と申します。",
    "弁護士の 大野と申します。",
    "弁護士の　大野　と申します。",
    "大野　です。",
    "大野です。よろしくお願い致します。",
    "私は大野です。",
    "わたくし、弁護士の大野でございます。",
    "当職大野が対応いたします。",
    "大野弁護士と申します。",
    "弁護士本人としてお答えします。",
    "弁護士の大野と申し上げます。",
)
# 名乗りではない正当な表現（事務所名・第三者としての弁護士言及）
NOT_SELF_INTRO_FORMS = (
    "大野法律事務所　時効援用専門窓口です。",
    "大野法律事務所です。",
    "弁護士が確認のうえご連絡いたします。",
    "担当弁護士が内容を確認のうえ、改めてご連絡いたします。",
    "Googleの口コミは「大野法律事務所 川口」で検索いただけます。",
    "弁護士の責任のもとで最初から最後まで対応いたします。",
)

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

# 「文体のみ」裁定の明記（fix1: 匿名化記号の不残存・事案内容の不引用・
# 弁護士本人として名乗らない・内容ルールの優先）
NO_QUOTE_PHRASES = (
    "匿名化の空欄であり",
    "○○・△△の記号を残さない",
    "根拠がない限り引用しない（根拠なき引用の禁止）",
    "弁護士確定定型・FAQ・顧客情報を出典とする",
    "弁護士本人として名乗らない",
    "サーバ側で承認降格",
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


def _concretize(text: str) -> str:
    """見本の匿名化記号を実名に埋めた形（モデルが正しく穴埋めした出力）。"""
    return text.replace("○○", "山田").replace("△△", "アコム")


def _frozen_texts() -> dict[str, str]:
    return {
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


def _customer_prompts() -> dict[str, str]:
    """顧客対応経路の組み立て済み prompt（受任前/受任後・既知項目あり）。"""
    return {
        "受任前": cr.build_system_prompt(status="決済完了",
                                         customer_name="試験太郎"),
        "受任後": cr.build_system_prompt(status="受任", customer_name="試験太郎"),
        "受任前_既知項目": cr.build_system_prompt(
            status="決済完了", known_items={"債権者名": "アコム"}),
    }


def _customer_guards(text: str, user_message: str = "教えてください"):
    return cr.apply_server_guards(
        {"reply": text, "category": "手続きの一般的な流れ",
         "auto_send": True}, [], user_message)


def _hearing_gate(text: str, tag: str):
    """ヒアリング送信ゲートに model 返信 text を通す。(reply, queue) を返す。"""
    user = f"Ustyle1-{abs(hash(tag)) & 0xffffff:x}"
    reply, queue = AsyncMock(), AsyncMock(return_value="q-1")
    with patch.object(main.autoreply_stoplist, "is_suppressed",
                      AsyncMock(return_value=False)), \
         patch.object(main, "get_app21_record",
                      AsyncMock(return_value=None)), \
         patch.object(main, "get_recent_chat_history",
                      AsyncMock(return_value=[])), \
         patch.object(main, "ask_claude", AsyncMock(return_value=text)), \
         patch.object(main, "_line_reply_with_fallback", reply), \
         patch.object(main, "save_to_chatlog", AsyncMock()), \
         patch.object(main, "save_to_approval_queue", queue):
        _run(main._process_line_event("tok", user, "こんにちは"))
    main.conversation_histories.pop(user, None)
    return reply, queue


class TestExemplarsVerbatim(unittest.TestCase):
    """見本 4 件は fix1 [A] 後の逐語（改変禁止）。"""

    def test_exemplars_match_frozen_copy(self):
        self.assertEqual([lbl for lbl, _t in cr.STYLE_EXEMPLARS],
                         list(EXEMPLARS_FROZEN))
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                self.assertEqual(text, EXEMPLARS_FROZEN[label])

    def test_exemplars_are_sanitized_of_case_content(self):
        # fix1 [A]: 名乗り・無根拠の案件固有表現・旧記号「◯社」は見本に残って
        # いない（見本本文でも同様）。fix2: 大野確定で戻した 5年程度・住宅ローン
        # は FAQ 根拠あり＝禁止集合から外す（1ヶ月程度・信用情報機関は不使用のまま）
        banned = ("大野と申します", "弁護士の大野", "1ヶ月",
                  "延滞の文字", "完全に抹消", "ご連絡要求", "信用情報機関", "◯社")
        for label, text in cr.STYLE_EXEMPLARS:
            for token in banned:
                with self.subTest(label=label, token=token):
                    self.assertNotIn(token, text)
        for token in banned:
            with self.subTest(section=True, token=token):
                self.assertNotIn(token, cr.STYLE_EXEMPLARS_TEXT.replace(
                    cr.STYLE_EXEMPLARS_NOTE, ""))
        # 匿名化記号は ○○/△△ の 2 種のみ
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                self.assertFalse(set("◯□") & set(text))

    def test_exemplars_contain_no_template_markers(self):
        # 置換マーカー（<<...>>）・format プレースホルダ（{}）・内部マーカーを
        # 含まない＝差し込み順序に依存せず、送信時 fatal にも該当しない
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                for bad in ("<<", ">>", "{", "}", "[KINTONE_", "━"):
                    self.assertNotIn(bad, text)
        for bad in ("<<", ">>", "{", "}", "[KINTONE_", "━"):
            self.assertNotIn(bad, cr.STYLE_SECTION)


class TestExemplarsFormalChecks(unittest.TestCase):
    """[C] 修正後見本=形式検査（サニタイズ・300 字・質問数・禁止語）に適合。"""

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

    def test_no_forbidden_words_and_no_self_intro(self):
        for label, text in cr.STYLE_EXEMPLARS:
            with self.subTest(label=label):
                self.assertEqual(cr.find_forbidden_words(text), [])
                self.assertEqual(cr.find_attorney_self_intro(text), [])
                # 匿名化記号の残存以外の [B] 違反はない
                other = [v for v in cr.style_guard_violations(text)
                         if not v.startswith("見本の匿名化記号の残存")]
                self.assertEqual(other, [])

    def test_concretized_exemplars_pass_all_guards(self):
        # 記号を実名に埋めた形（正しい穴埋め）は両経路の全ガードを通過する
        # ＝見本の文体そのものはガードと整合している
        for label, text in cr.STYLE_EXEMPLARS:
            filled = _concretize(text)
            with self.subTest(route="顧客対応", label=label):
                g = _customer_guards(filled)
                self.assertTrue(g.can_auto_send, g.demotion_reasons)
                self.assertEqual(g.demotion_reasons, [])
            with self.subTest(route="ヒアリング", label=label):
                reply, queue = _hearing_gate(filled, "filled-" + label)
                reply.assert_awaited_once()
                self.assertEqual(reply.await_args.args[2], filled)
                queue.assert_not_awaited()


class TestExemplarVerbatimRejected(unittest.TestCase):
    """[C] 見本の丸写し（匿名化記号の残存）=拒否。旧見本の引用=拒否。"""

    def test_faq_backed_exemplars_pass_verbatim(self):
        # fix2: 見本2・3（記号なし・FAQ 根拠語 5年程度/住宅ローンを含む）は
        # 逐語でも [B] 防壁にかからず両経路で通過する（根拠あり=許容の整合）
        for label in ("不利益の正直な開示", "質問への具体的回答"):
            text = EXEMPLARS_FROZEN[label]
            self.assertIn("5年程度", text)
            with self.subTest(label=label):
                self.assertEqual(cr.style_guard_violations(text), [])
                self.assertTrue(_customer_guards(text).can_auto_send)
                reply, queue = _hearing_gate(text, "faqbacked-" + label)
                self.assertEqual(reply.await_args.args[2], text)
                queue.assert_not_awaited()

    def test_placeholder_residue_demoted_both_routes(self):
        for label, text in cr.STYLE_EXEMPLARS:
            if not cr._EXEMPLAR_PLACEHOLDER_RE.search(text):
                continue    # 見本2・3 は記号を含まない（形式検査のみの対象）
            with self.subTest(route="顧客対応", label=label):
                g = _customer_guards(text)
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any(r.startswith("見本の匿名化記号の残存")
                                    for r in g.demotion_reasons))
            with self.subTest(route="ヒアリング", label=label):
                reply, queue = _hearing_gate(text, "verbatim-" + label)
                self.assertEqual(reply.await_args.args[2], cr.PENDING_REPLY)
                queue.assert_awaited_once()
                self.assertIn("見本の匿名化記号の残存",
                              queue.await_args.kwargs["reason"])
        self.assertTrue(cr._EXEMPLAR_PLACEHOLDER_RE.search(
            EXEMPLARS_FROZEN["見立てと提案"]))    # 少なくとも 1 件は検査対象

    def test_old_exemplars_rejected_both_routes(self):
        # Codex 指定 negative: 旧見本1 の逐語全文（名乗り入り）をモデル出力
        # として返しても自動送信されない。旧2〜4 も各分類で降格
        for label, text in OLD_EXEMPLARS.items():
            expected = OLD_EXEMPLAR_EXPECTED_REASON[label]
            with self.subTest(route="顧客対応", label=label):
                g = _customer_guards(text)
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any(r.startswith(expected)
                                    for r in g.demotion_reasons),
                                g.demotion_reasons)
            with self.subTest(route="ヒアリング", label=label):
                reply, queue = _hearing_gate(text, "old-" + label)
                reply.assert_awaited_once()
                self.assertEqual(reply.await_args.args[2], cr.PENDING_REPLY)
                queue.assert_awaited_once()
                self.assertIn(expected, queue.await_args.kwargs["reason"])
                self.assertIn("ヒアリング送信ゲートで降格",
                              queue.await_args.kwargs["reason"])

    def test_old_exemplar1_self_intro_is_the_trigger(self):
        # 旧見本1 は名乗りだけで降格される（記号を埋めても名乗りが残れば拒否）
        filled = _concretize(OLD_EXEMPLARS["旧1_名乗り"])
        g = _customer_guards(filled)
        self.assertFalse(g.can_auto_send)
        self.assertEqual(
            [r for r in g.demotion_reasons if "名乗り" in r],
            ["弁護士本人の名乗り検出: 弁護士の大野「弁護士の大野」、"
             "大野と申します/です「大野と申します」"])


class TestAttorneySelfIntroGuard(unittest.TestCase):
    """[B] 弁護士本人の名乗り検知（NFKC+空白除去・閉集合）・両経路で降格。"""

    def test_each_form_detected(self):
        for form in SELF_INTRO_FORMS:
            with self.subTest(form=form):
                self.assertTrue(cr.find_attorney_self_intro(form), form)
                # 前後に通常の文があっても検知する
                text = "ご連絡ありがとうございます。" + form + "よろしくお願い致します。"
                self.assertTrue(cr.find_attorney_self_intro(text))

    def test_each_form_demoted_both_routes(self):
        for form in SELF_INTRO_FORMS:
            text = "ご連絡ありがとうございます。" + form
            with self.subTest(route="顧客対応", form=form):
                g = _customer_guards(text)
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any(r.startswith("弁護士本人の名乗り検出")
                                    for r in g.demotion_reasons))
            with self.subTest(route="ヒアリング", form=form):
                reply, queue = _hearing_gate(text, "intro-" + form)
                self.assertEqual(reply.await_args.args[2], cr.PENDING_REPLY)
                queue.assert_awaited_once()
                self.assertIn("弁護士本人の名乗り検出",
                              queue.await_args.kwargs["reason"])

    def test_office_name_and_third_person_not_detected(self):
        for form in NOT_SELF_INTRO_FORMS:
            with self.subTest(form=form):
                self.assertEqual(cr.find_attorney_self_intro(form), [])
                self.assertEqual(cr.style_guard_violations(form), [])
                self.assertTrue(_customer_guards(form).can_auto_send)

    def test_frozen_texts_and_template_blocks_pass(self):
        # 凍結文言（事務所名を含むヒアリング定型ブロックを含む）は [B] 防壁に
        # 一切かからない（false positive なし）
        for key, text in _frozen_texts().items():
            with self.subTest(key=key):
                self.assertEqual(cr.style_guard_violations(text), [])
        for i, block in enumerate(main.HEARING_TEMPLATE_BLOCKS):
            with self.subTest(block=i):
                self.assertEqual(cr.style_guard_violations(block), [])
        self.assertEqual(cr.style_guard_violations(
            "はじめまして。\n大野法律事務所　時効援用専門窓口です。"), [])

    def test_vocabulary_closed_set_pinned(self):
        # 検知語彙の閉集合（設計の可視化・追加は票由来で行う）
        self.assertEqual([lbl for lbl, _p in cr._SELF_INTRO_PATTERNS],
                         ["弁護士の大野", "大野と申します/です", "私は大野",
                          "弁護士本人です"])
        self.assertEqual(cr._EXEMPLAR_PLACEHOLDER_RE.pattern,
                         "○○|◯◯|△△|□□|◯社")
        self.assertEqual(cr.LEGACY_EXEMPLAR_NO_BASIS_PHRASES,
                         ("延滞の文字", "完全に抹消", "ご連絡要求"))


class TestLegacyTokenPolicy(unittest.TestCase):
    """[B] 旧見本トークンの線引き: FAQ に根拠のある語は許容・根拠なしは降格。"""

    def test_faq_backed_tokens_allowed(self):
        # FAQ（信用情報 5年程度・ローン・督促の 1ヶ月程度・信用情報機関への
        # 報告）に同内容の根拠がある語は新ガードで降格しない
        texts = (
            "山田様のおっしゃるように、信用情報の削除まで長いと5年程度かかる"
            "ことがございます。よろしくお願い致します。",
            "手続きから5年程度はカード作成やローンは組めない前提でいた方が"
            "よいです。住宅ローンも同様となります。",
            "ご依頼から1ヶ月程度経過後に届いた場合は必ずご連絡ください。",
            "信用情報機関へ早急に報告するよう業者には伝えます。",
        )
        for text in texts:
            with self.subTest(text=text[:16]):
                self.assertEqual(cr.style_guard_violations(text), [])
                self.assertTrue(_customer_guards(text).can_auto_send)

    def test_no_basis_phrases_demoted(self):
        for phrase in cr.LEGACY_EXEMPLAR_NO_BASIS_PHRASES:
            text = f"山田様、{phrase}について申し上げます。"
            with self.subTest(phrase=phrase):
                v = cr.style_guard_violations(text)
                self.assertEqual(
                    v, [f"旧見本由来の無根拠表現: {phrase}"])
                g = _customer_guards(text)
                self.assertFalse(g.can_auto_send)
                reply, queue = _hearing_gate(text, "legacy-" + phrase)
                self.assertEqual(reply.await_args.args[2], cr.PENDING_REPLY)
                self.assertIn("旧見本由来の無根拠表現",
                              queue.await_args.kwargs["reason"])


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
        # 裁定: 真似るのは文体のみ。記号の不残存・事案内容の不引用・名乗り
        # 禁止（サーバ側降格の明記）・内容ルール（定型・承認制）の優先を両経路で明記
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
        cases = _frozen_texts()
        self.assertEqual(set(cases), set(FROZEN_SHA) - {
            "hearing_prompt_body", "customer_prompt_base",
            "hearing_template_blocks"})
        for key, text in cases.items():
            with self.subTest(key=key):
                self.assertEqual(_sha(text), FROZEN_SHA[key])
        self.assertEqual(len(cr.PENDING_BY_CATEGORY), 11)
        self.assertEqual(len(cr.IMMEDIATE_NOTICE_TEXTS), 6)

    def test_existing_guard_closed_set_unchanged(self):
        # 既存ガード閉集合は不変: 許可絵文字=空集合・テンプレ免除 2 件・
        # 上限 300 字・質問数 2・カテゴリ閉集合
        self.assertEqual(cr.ALLOWED_CANONICAL_EMOJI, frozenset())
        self.assertEqual(len(main.HEARING_TEMPLATE_BLOCKS), 2)
        with patch.dict(os.environ, {"AUTOREPLY_MAX_CHARS": ""}):
            self.assertEqual(rs.max_auto_chars(), 300)
        self.assertEqual(rs.MAX_QUESTIONS, 2)
        self.assertEqual(cr.AUTO_SEND_CATEGORIES, {
            "挨拶・雑談", "手続きの一般的な流れ", "必要書類の案内",
            "費用の定型案内", "進捗の事実回答", "営業案内・アクセス",
            "時効見立て_条件付き"})


if __name__ == "__main__":
    unittest.main()
