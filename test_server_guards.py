"""
サーバー側ガード（自動送信前の二重チェック）の単体テスト

応答方針v2で追加した chat_responder.apply_server_guards() と
関連ヘルパーの回帰テスト。Claude API を呼ばないためオフラインで実行できる:

  python -m pytest test_server_guards.py -v

検証対象:
  a) 禁止語照合（断定語・行動指示語）と許可リスト（受任後電話対応の定型指示）
  b) カテゴリ「費用の定型案内」の必須文言チェック
  c) カテゴリ「時効見立て_条件付き」の留保文言・時効更新事由フラグ
  - 承認キュー行き時の即時定型文（裁判所書類・諦め離脱・対象外債権）の解決
  - 裁判所書類第一報のサーバー側検知（否定回答の除外・二度送り防止）
"""

import os
import unittest

from chat_responder import (
    _creditor_count_mentioned,
    _FORBIDDEN_PATTERNS,
    _SYSTEM_PROMPT_BASE,
    ALLOWED_CANONICAL_EMOJI,
    COUNT_UNKNOWN,
    FEE_COUNT_UNKNOWN_REASON,
    JIKOU_PROFILE,
    APPROVED_DUNNING_INSTRUCTION,
    APPROVED_PHONE_INSTRUCTION,
    AUTO_SEND_CATEGORIES,
    BRANCHING_GUIDANCE_EXAMPLE,
    CHURN_NEUTRAL_REPLY,
    COURT_DOC_REQUEST_REPLY,
    CRISIS_SUPPORT_REPLY,
    FAQ3_CANONICAL_TEXTS,
    FEE_GUIDE_TEXT,
    FEE_MULTI_REQUIRED_PHRASES,
    FEE_REQUIRED_PHRASES,
    HOTERASU_STANDARD_REPLY,
    IMMEDIATE_NOTICE_TEXTS,
    OUT_OF_SCOPE_DEBT_REPLY,
    URGENT_NOTICE_KINDS,
    URGENT_SEIZURE_PANIC_REPLY,
    apply_server_guards,
    build_attorney_notification,
    fee_total_yen,
    find_forbidden_words,
    looks_like_court_doc_report,
)
from hub import reply_sanitizer


def _result(
    reply="ありがとうございます。",
    category="挨拶・雑談",
    auto_send=True,
    jikou_update_flag=False,
    immediate_notice="none",
    reason="テスト",
):
    return {
        "reply": reply,
        "category": category,
        "auto_send": auto_send,
        "jikou_update_flag": jikou_update_flag,
        "immediate_notice": immediate_notice,
        "reason": reason,
    }


# 個別見立て（B型）の留保文言を満たす返信例
_VALID_MITATE_REPLY = (
    "お伺いした内容が正確であれば、時効援用できる可能性が高いです。"
    "最終的に時効が成立しているかは、当事務所から業者へ時効援用通知を送り、"
    "その後の業者への確認をもって確定します。"
    "ご希望でしたら、このままLINEでお手続きのご案内も可能です。"
)

# 一般論（A型）のただし書きを満たす返信例
_VALID_GENERAL_REPLY = (
    "その前提がすべて満たされていれば、時効援用により支払義務は消滅します。"
    "なお、債務の承認は本人が気づかず該当していることがあります"
    "（電話で支払うと言った、少額を入金した等）。"
)


class TestBasicRouting(unittest.TestCase):
    """従来からの二重チェック（auto_send × カテゴリ許可リスト）"""

    def test_auto_send_allowed_category(self):
        g = apply_server_guards(_result(), [], "こんにちは")
        self.assertTrue(g.can_auto_send)
        self.assertEqual(g.immediate_notice, "none")

    def test_model_auto_send_false_is_queued(self):
        g = apply_server_guards(_result(auto_send=False), [], "こんにちは")
        self.assertFalse(g.can_auto_send)

    def test_queue_category_never_auto_sends(self):
        g = apply_server_guards(
            _result(category="法的判断・見通し", auto_send=True), [], "質問です"
        )
        self.assertFalse(g.can_auto_send)

    def test_new_category_in_allowlist(self):
        self.assertIn("時効見立て_条件付き", AUTO_SEND_CATEGORIES)


class TestForbiddenWords(unittest.TestCase):
    """a) 禁止語照合と許可リスト"""

    def test_assertive_word_demotes(self):
        for reply in [
            "確実に時効になりますのでご安心ください。",
            "絶対に大丈夫です。",
            "間違いなく消滅します。",
            "必ず消滅しますのでご安心ください。",
        ]:
            with self.subTest(reply=reply):
                g = apply_server_guards(
                    _result(reply=reply, category="時効見立て_条件付き"), [], "大丈夫ですか"
                )
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any("禁止語" in r for r in g.demotion_reasons))

    def test_directive_word_demotes(self):
        for reply in [
            "業者にはもう払わないでください。",
            "督促は無視して大丈夫です。",
            "業者には連絡しないでください。",
            "その通知は放置して問題ありません。",
            "業者からの電話には出ないでください。",
        ]:
            with self.subTest(reply=reply):
                g = apply_server_guards(
                    _result(reply=reply, category="手続きの一般的な流れ"), [], "どうすれば"
                )
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any("禁止語" in r for r in g.demotion_reasons))

    def test_approved_phone_instruction_is_allowlisted(self):
        """弁護士確認済みの電話対応定型指示は許可リストで通る"""
        reply = f"ご不安でしたね。{APPROVED_PHONE_INSTRUCTION}"
        self.assertEqual(find_forbidden_words(reply), [])
        g = apply_server_guards(
            _result(reply=reply, category="手続きの一般的な流れ"), [], "業者から電話が来ます"
        )
        self.assertTrue(g.can_auto_send)

    def test_negated_directive_is_not_flagged(self):
        """「無視してはいけません」「無視してよいわけではありません」のような
        打ち消しは禁止語にしない（断定語の否定形除外と同じ原則）"""
        for reply in [
            "裁判所からの書類は無視してはいけません。放置してはいけない書類です。",
            "だからといって無視してよいわけではありません。",
            "督促を放置していいわけではありませんので、ご相談ください。",
        ]:
            with self.subTest(reply=reply):
                self.assertEqual(find_forbidden_words(reply), [])

    def test_affirmative_directive_still_demotes(self):
        """打ち消しを伴わない「無視して/放置して」は引き続き降格される"""
        for reply in [
            "督促は無視して大丈夫です。",
            "その通知は放置してください。",
        ]:
            with self.subTest(reply=reply):
                self.assertTrue(find_forbidden_words(reply), f"検出されるべき: {reply}")

    def test_negated_kanarazu_forms_are_allowed(self):
        """「必ず消滅するとは保証できない」等の否定形は許可される
        （法律知識ブロックの必須文言。2026-07-03 実測での誤検出を受けた弁護士承認済みの緩和）"""
        for reply in [
            "支払督促の場合、業者により見解が分かれるため、必ず消滅するとは保証できません。",
            "「必ず消滅する」とは言い切れないのが正直なところです。",
            "必ず消滅するとは限らない点にご注意ください。",
            "必ず時効になるとは限りません。",
            "必ず成立するとは言えませんが、可能性は十分あります。",
            "必ず消滅するとは断言できません。",
        ]:
            with self.subTest(reply=reply):
                self.assertEqual(find_forbidden_words(reply), [])
                g = apply_server_guards(
                    _result(reply=reply, category="手続きの一般的な流れ"), [], "一般論を教えてください"
                )
                self.assertTrue(g.can_auto_send)

    def test_dunning_instruction_full_text_is_allowlisted(self):
        """受任後の督促状定型指示（但し書き込みの全文）は許可リストで通る"""
        reply = f"ご安心ください。{APPROVED_DUNNING_INSTRUCTION}"
        self.assertEqual(find_forbidden_words(reply), [])
        g = apply_server_guards(
            _result(reply=reply, category="手続きの一般的な流れ"), [], "督促状は無視していいですか"
        )
        self.assertTrue(g.can_auto_send)

    def test_dunning_instruction_without_court_proviso_demotes(self):
        """裁判所書類の但し書きを省略した部分利用は「無視して」が残り降格される"""
        reply = "手続き中、業者からの督促状は無視していただいて問題ありません。"
        hits = find_forbidden_words(reply)
        self.assertTrue(hits, "但し書きなしの部分利用は禁止語として検出されるべき")
        g = apply_server_guards(
            _result(reply=reply, category="手続きの一般的な流れ"), [], "督促状は無視していいですか"
        )
        self.assertFalse(g.can_auto_send)

    def test_branching_guidance_contains_no_directives(self):
        """受任前向けの判断分岐提示型の標準文面に行動指示語・断定語が含まれない
        （許可リストに依存せず、生パターン照合でも検出ゼロであることを固定）"""
        for label, pattern in _FORBIDDEN_PATTERNS:
            self.assertIsNone(
                pattern.search(BRANCHING_GUIDANCE_EXAMPLE),
                f"{label} が判断分岐提示型の文面に含まれています",
            )
        g = apply_server_guards(
            _result(reply=BRANCHING_GUIDANCE_EXAMPLE, category="手続きの一般的な流れ"),
            [],
            "督促を無視してもいいですか",
        )
        self.assertTrue(g.can_auto_send)

    def test_negated_assertive_words_are_allowed(self):
        """「絶対に大丈夫とは言えません」等の留保付き応答は許可される
        （断定要求への留保付き自動送信の緩和・2026-07-03 弁護士指示）"""
        for reply in [
            "可能性は高いですが、絶対に大丈夫とは言えません。",
            "確実に消滅するとは言い切れませんが、可能性は高い状況です。",
            "間違いなく成立するとは断言できませんが、前向きに進められます。",
        ]:
            with self.subTest(reply=reply):
                self.assertEqual(find_forbidden_words(reply), [])

    def test_affirmative_assertive_words_still_demote(self):
        """否定を伴わない断定語は引き続き降格される"""
        for reply in [
            "絶対に大丈夫です。",
            "確実に時効になります。",
            "間違いなく成立します。",
        ]:
            with self.subTest(reply=reply):
                self.assertTrue(find_forbidden_words(reply), f"検出されるべき: {reply}")

    def test_jikou_madjika_is_forbidden(self):
        """「時効間近」は全応答で使用禁止（FAQ第2弾・2026-07-03 弁護士指示）"""
        for reply in [
            "この通知は時効間近のサインである可能性があります。",
            "時効間近ですので、お早めにご依頼ください。",
        ]:
            with self.subTest(reply=reply):
                hits = find_forbidden_words(reply)
                self.assertTrue(any("時効間近" in h for h in hits))
                g = apply_server_guards(
                    _result(reply=reply, category="手続きの一般的な流れ"), [], "減額通知が来ました"
                )
                self.assertFalse(g.can_auto_send)

    def test_gengaku_notice_standard_reply_is_clean(self):
        """減額通知の標準回答（弁護士確定の言い回し）は禁止語に触れない"""
        reply = (
            "一般的に、減額のご案内は時効にかかっている可能性が高くなる傾向は"
            "ありますが、時効にかかっていなくても届くことがあります。"
            "この通知だけで時効の成否を判断することはできません。"
        )
        self.assertEqual(find_forbidden_words(reply), [])
        g = apply_server_guards(
            _result(reply=reply, category="手続きの一般的な流れ"), [], "減額通知が届きました"
        )
        self.assertTrue(g.can_auto_send)

    def test_sashiosae_general_reply_is_clean_and_flag_neutral(self):
        """差押え中の一般論+資料収集の返信は自動送信可（フラグは見立てカテゴリのみ降格）"""
        reply = (
            "差押えを受けている場合、時効が更新されているため時効援用はできません。"
            "状況を確認いたしますので、差押えに関する書類の写真をこのLINEにお送りいただけますか。"
        )
        self.assertEqual(find_forbidden_words(reply), [])
        g = apply_server_guards(
            _result(reply=reply, category="手続きの一般的な流れ", jikou_update_flag=True),
            [],
            "給料を差し押さえられています。時効援用できますか？",
        )
        self.assertTrue(g.can_auto_send)

    def test_affirmative_kanarazu_forms_still_demote(self):
        """「必ず消滅します」等の肯定断定形は引き続き降格される"""
        for reply in [
            "必ず消滅しますのでご安心ください。",
            "5年経過していれば必ず時効になります。",
            "この場合は必ず成立します。",
            "時効援用すれば必ず消滅するのでご安心ください。",
        ]:
            with self.subTest(reply=reply):
                self.assertTrue(find_forbidden_words(reply), f"検出されるべき: {reply}")
                g = apply_server_guards(
                    _result(reply=reply, category="時効見立て_条件付き"), [], "大丈夫ですか"
                )
                self.assertFalse(g.can_auto_send)


class TestFeeRequiredPhrases(unittest.TestCase):
    """b) 費用の定型案内の必須文言チェック"""

    def test_fee_guide_text_satisfies_required_phrases(self):
        """固定文自体が必須文言をすべて含む（自己整合性）"""
        for phrase in FEE_REQUIRED_PHRASES:
            self.assertIn(phrase, FEE_GUIDE_TEXT)

    def test_full_template_auto_sends(self):
        reply = f"ご質問ありがとうございます。\n{FEE_GUIDE_TEXT}"
        g = apply_server_guards(
            _result(reply=reply, category="費用の定型案内"), [], "費用はいくらですか"
        )
        self.assertTrue(g.can_auto_send)

    def test_missing_required_phrase_demotes(self):
        """前払い・分割不可・不成立時費用の言及が欠けたら降格（固定文未送付の顧客）"""
        reply = "費用は1社あたり44,000円です。お支払いは銀行振込またはカード決済です。"
        g = apply_server_guards(
            _result(reply=reply, category="費用の定型案内"), [], "費用はいくらですか"
        )
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("必須文言" in r for r in g.demotion_reasons))

    def test_followup_after_fee_guide_is_allowed(self):
        """固定文を送付済みの顧客への続き質問には簡潔な回答を許容
        （会話単位の必須文言チェック・2026-07-03 弁護士承認済みの緩和）"""
        history = [
            {"role": "user", "content": "費用はいくらですか？"},
            {"role": "assistant", "content": f"ご案内いたします。\n{FEE_GUIDE_TEXT}"},
        ]
        # JIKOU-FEE-RULE-1（大野裁定 2026-10-11）: 3社=88,000+22,000=110,000円
        # （旧例文「44,000円×3社=132,000円」は新規則では誤り）
        reply = "3社ですと、2社分88,000円（税込）に3社目22,000円（税込）を加えた110,000円（税込）となります。"
        g = apply_server_guards(
            _result(reply=reply, category="費用の定型案内"), history, "三社だといくらですか？"
        )
        self.assertTrue(g.can_auto_send)

    def test_followup_without_prior_fee_guide_still_demotes(self):
        """固定文を一度も送っていない顧客への簡潔回答は従来どおり降格"""
        history = [
            {"role": "user", "content": "こんにちは"},
            {"role": "assistant", "content": "こんにちは。ご連絡ありがとうございます。"},
        ]
        reply = "3社ですと、2社分88,000円（税込）に3社目22,000円（税込）を加えた110,000円（税込）となります。"
        g = apply_server_guards(
            _result(reply=reply, category="費用の定型案内"), history, "三社だといくらですか？"
        )
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("必須文言" in r for r in g.demotion_reasons))

    def test_other_categories_not_checked_for_fee_phrases(self):
        """費用カテゴリ以外には費用必須文言を要求しない"""
        g = apply_server_guards(
            _result(reply="こんにちは。ご連絡ありがとうございます。"), [], "こんにちは"
        )
        self.assertTrue(g.can_auto_send)


class TestFeeRuleMultiCreditor(unittest.TestCase):
    """JIKOU-FEE-RULE-1（大野裁定 2026-10-11）: 1社 44,000円・2社 88,000円・
    3社目以降 1社 22,000円。3社以上の文脈でのみ「22,000円」を追加必須語とし、
    1〜2社の回答は従来どおり（単純追加で降格しない）。"""

    _SENT = [
        {"role": "user", "content": "費用はいくらですか？"},
        {"role": "assistant", "content": f"ご案内いたします。\n{FEE_GUIDE_TEXT}"},
    ]

    def test_a_fee_totals_and_guide_examples(self):
        """(a) 1〜4 社の合計と、固定文の例示・必須語の自己整合"""
        self.assertEqual([fee_total_yen(n) for n in (1, 2, 3, 4)],
                         [44000, 88000, 110000, 132000])
        # EDGE-1（QUEUE-2026-10-14-G G-2 反例 #1）で pin 更新: 0 以下・型外は 0 円を返さず例外（承認降格に倒す）
        for bad in (0, -1, 1000):
            with self.assertRaises(ValueError):
                fee_total_yen(bad)
        for bad in ("3", 3.0, True, None):
            with self.assertRaises(TypeError):
                fee_total_yen(bad)
        self.assertIn("3社目からは1社あたり22,000円（税込）", FEE_GUIDE_TEXT)
        self.assertIn("3社 110,000円・4社 132,000円", FEE_GUIDE_TEXT)
        self.assertIn("2社までは 44,000円（税込）× 社数", FEE_GUIDE_TEXT)
        for phrase in FEE_REQUIRED_PHRASES + FEE_MULTI_REQUIRED_PHRASES:
            self.assertIn(phrase, FEE_GUIDE_TEXT)
        self.assertEqual(FEE_MULTI_REQUIRED_PHRASES, ["22,000円"])
        # 固定文自体は 4 社の例を含む=3社以上の文脈でも自己整合で自動送信可
        for n, q in ((1, "1社です"), (2, "2社あります"), (3, "3社です"), (4, "四社です")):
            with self.subTest(n=n):
                g = apply_server_guards(
                    _result(reply=f"ご案内します。\n{FEE_GUIDE_TEXT}",
                            category="費用の定型案内"), [], q)
                self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_a_correct_totals_auto_send(self):
        """(a) 固定文送付済みの続き質問で、正しい合計の回答は自動送信可"""
        cases = (
            ("1社だといくらですか", "1社ですと44,000円（税込）です。"),
            ("2社だといくらですか", "2社ですと44,000円（税込）×2社で88,000円（税込）です。"),
            ("3社だといくらですか", "3社ですと88,000円に3社目の22,000円（税込）を加えた110,000円（税込）です。"),
            ("4社だといくらですか", "4社ですと88,000円に3社目・4社目の各22,000円（税込）を加えた132,000円（税込）です。"),
        )
        for q, reply in cases:
            with self.subTest(q=q):
                g = apply_server_guards(
                    _result(reply=reply, category="費用の定型案内"), self._SENT, q)
                self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_b_one_or_two_creditors_without_22000_not_demoted(self):
        """(b) 1〜2 社の文脈では 22,000円 が無くても降格しない（必須語の単純追加ではない）"""
        for q, reply in (("1社だといくらですか", "1社ですと44,000円（税込）です。"),
                         ("二社だといくら", "2社ですと88,000円（税込）です。"),
                         ("費用を教えて", "合計は44,000円（税込）です。")):
            with self.subTest(q=q):
                g = apply_server_guards(
                    _result(reply=reply, category="費用の定型案内"), self._SENT, q)
                self.assertTrue(g.can_auto_send, g.demotion_reasons)
                self.assertFalse(any("22,000円" in r for r in g.demotion_reasons))

    def test_c_three_or_more_without_22000_demoted(self):
        """(c) 3 社以上の文脈で 22,000円 が無ければ降格（送付済み緩和の対象外・
        質問側/返信側どちらに社数が現れても検知）"""
        wrong = "3社ですと、44,000円（税込）× 3社 = 132,000円（税込）となります。"
        for history in ([], self._SENT):
            for q, reply in (("三社だといくらですか？", wrong),
                             ("3社あります", "合計は132,000円（税込）です。"),
                             ("４社です", "4社ですと176,000円（税込）です。"),
                             ("費用はいくらですか", "3社ですと132,000円（税込）です。")):
                with self.subTest(sent=bool(history), q=q):
                    g = apply_server_guards(
                        _result(reply=reply, category="費用の定型案内"), history, q)
                    self.assertFalse(g.can_auto_send)
                    self.assertTrue(any("22,000円" in r for r in g.demotion_reasons),
                                    g.demotion_reasons)

    def test_c_full_guide_in_reply_satisfies_three_or_more(self):
        """固定文を返信に含めれば 3 社の文脈でも必須語を満たす（固定文未送付の顧客）"""
        g = apply_server_guards(
            _result(reply=f"ご案内します。\n{FEE_GUIDE_TEXT}", category="費用の定型案内"),
            [], "3社あるのですが費用は？")
        self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_d_additional_request_uses_cumulative_count(self):
        """(d) 追加依頼は通算社数（前回2社+今回1社=通算3社 → 22,000円）。
        通算の算定は prompt（FAQ 第2弾）の責務で、ガードは「返信が通算3社目と述べながら
        44,000円 のみで答える」誤りを「3社」の語で検知して止める（質問文の「2社」+「1社」
        の加算は行わない=「2社のうち1社だけ」と区別できないため）。モデル実測は
        test_triage_classification の fee-rule-1 ケース。前回1社+今回1社=通算2社は
        44,000円 のまま（22,000円 不要）"""
        q3 = "前回2社お願いしました。別の1社も追加でお願いしたいのですが費用は？"
        g = apply_server_guards(
            _result(reply="通算3社目となりますので、追加の1社は22,000円（税込）です。",
                    category="費用の定型案内"), self._SENT, q3)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)
        g = apply_server_guards(
            _result(reply="通算3社目となりますが、追加の1社も44,000円（税込）です。",
                    category="費用の定型案内"), self._SENT, q3)
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("22,000円" in r for r in g.demotion_reasons), g.demotion_reasons)
        q2 = "前回1社お願いしました。別の1社も追加でお願いしたいのですが費用は？"
        g = apply_server_guards(
            _result(reply="通算2社ですので、追加の1社は44,000円（税込）です。",
                    category="費用の定型案内"), self._SENT, q2)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)
        # prompt の FAQ（追加依頼=通算社数）と固定文の整合ルール
        self.assertIn("通算の社数で算定", _SYSTEM_PROMPT_BASE)
        self.assertIn("3社目以降は 1社あたり 22,000円（税込）を加算", _SYSTEM_PROMPT_BASE)
        self.assertNotIn("44,000円（税込）× 社数 と整合", _SYSTEM_PROMPT_BASE)

    def test_creditor_count_detector(self):
        """「N社」の読取（NFKC・漢数字）。数を持たない表現のうち「各社」は 0。
        fix1 BF-01（Codex R-JIKOU-FEE-RULE-1）: 数字列全体を読む・途中からの部分一致禁止
        EDGE-1（QUEUE-2026-10-14-G G-2 反例 #2）で pin 更新: 「複数社」「数社」は 0（1〜2 社扱い）でなく
        判定不能（None＝承認降格）。3 社以上の可能性を 1〜2 社に倒さない（安全側）"""
        for text, n in (("三社", 3), ("３社", 3), ("3社", 3), ("10社", 10),
                        ("1社あたり", 1), ("複数社", None), ("数社", None), ("各社", 0),
                        ("十数社あります", None), ("複数の業者から", None),
                        ("", 0), ("会社です", 0),
                        # BF-01 回帰: 複合漢数字（旧実装は末尾 1 文字だけ読み 1/2/1 になった）
                        ("十一社", 11), ("十二社", 12), ("二十一社", 21), ("十社", 10),
                        ("百社", 100), ("二百三十社", 230), ("3社目", 3), ("3 社", 3),
                        ("３社です", 3), ("三社分", 3), ("三社以上", 3), ("三社合計", 3),
                        ("2026社", 2026),
                        # 対象外のまま
                        ("3件", 0), ("三件", 0),
                        # fix2 BF-02（司令塔裁定）: 社名との区別はしない=安全側で社数として数える
                        # （誤検知で承認に回るコストは許容・見逃しは許容しない）
                        ("三社電機の 1 社です", 3), ("三社電機", 3), ("五社協定です", 5),
                        ("3社電機の1社です", 3), ("３社電機", 3), ("3社電機", 3),
                        # fix2 回帰: 社数らしい表現は文脈語に関わらず検出
                        ("三社借入があります。費用はいくらですか", 3),
                        ("十一社依頼した場合の費用は？", 11)):
            with self.subTest(text=text):
                self.assertEqual(_creditor_count_mentioned(text), n)

    def test_creditor_count_unknown(self):
        """fix1 BF-01: 解釈できない数表現は 1〜2 社に倒さず判定不能（COUNT_UNKNOWN）"""
        for text in ("十十社", "一十社", "二三社", "十百社", "1十社", "三1社",
                     "２社と十十社"):                      # 解釈可能な表現が混在しても不能
            with self.subTest(text=text):
                self.assertIs(_creditor_count_mentioned(text), COUNT_UNKNOWN)
        self.assertIsNone(COUNT_UNKNOWN)

    def test_c_compound_kanji_three_or_more_demoted(self):
        """fix1 BF-01: 「十一社」「二十一社」も 3 社以上の文脈として 22,000円 を要求"""
        for q in ("十一社あります", "二十一社です", "十社だといくら"):
            with self.subTest(q=q):
                g = apply_server_guards(
                    _result(reply="合計は484,000円（税込）です。", category="費用の定型案内"),
                    self._SENT, q)
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any("22,000円" in r for r in g.demotion_reasons),
                                g.demotion_reasons)
                g = apply_server_guards(
                    _result(reply="2社分88,000円に3社目以降は1社あたり22,000円（税込）を加算します。",
                            category="費用の定型案内"), self._SENT, q)
                self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_unknown_count_demotes_even_after_guide_sent(self):
        """fix1 BF-01: 判定不能は送付済み緩和の対象外で承認へ（質問側・返信側とも）。
        降格理由は固定文言。必須語欠落と併記される場合は「／」区切り"""
        for q, reply in (("十十社あります", "合計は44,000円（税込）です。"),
                         ("費用は？", "一十社ですと44,000円（税込）です。")):
            with self.subTest(q=q):
                g = apply_server_guards(
                    _result(reply=reply, category="費用の定型案内"), self._SENT, q)
                self.assertFalse(g.can_auto_send)
                self.assertIn(FEE_COUNT_UNKNOWN_REASON, g.demotion_reasons)
        g = apply_server_guards(
            _result(reply="合計は44,000円です。", category="費用の定型案内"), [], "十十社あります")
        self.assertFalse(g.can_auto_send)
        self.assertEqual(len(g.demotion_reasons), 1)
        self.assertTrue(g.demotion_reasons[0].startswith("費用定型の必須文言欠落: "))
        self.assertTrue(g.demotion_reasons[0].endswith("／" + FEE_COUNT_UNKNOWN_REASON))
        # 1〜2 社は従来どおり自動送信可（判定不能にならない）
        for q in ("二社です", "2社あります"):
            with self.subTest(q=q):
                g = apply_server_guards(
                    _result(reply="2社ですと88,000円（税込）です。", category="費用の定型案内"),
                    self._SENT, q)
                self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_fix2_company_name_counted_as_safe_side(self):
        """fix2 BF-02（司令塔裁定）: 社名（「三社電機」）は除外せず 3 社として扱う＝22,000円 が
        無ければ承認へ降格（誤検知のコストは許容）。固定文を含む返信なら自動送信可"""
        q = "三社電機の 1 社です。費用はいくらですか"
        g = apply_server_guards(
            _result(reply="1社ですと44,000円（税込）です。", category="費用の定型案内"), self._SENT, q)
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("22,000円" in r for r in g.demotion_reasons), g.demotion_reasons)
        g = apply_server_guards(
            _result(reply=f"ご案内します。\n{FEE_GUIDE_TEXT}", category="費用の定型案内"), [], q)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)
        self.assertEqual(_creditor_count_mentioned("3社電機"), 3)

    def test_fix2_question_side_count_applies_when_reply_omits_count(self):
        """fix2: 返信側が社数を省略しても質問側の社数で検査が働く
        （「十一社」質問+「合計は 132,000 円」返信 → 22,000円 欠落で降格）"""
        for q in ("十一社依頼した場合の費用は？", "三社借入があります。費用はいくらですか"):
            with self.subTest(q=q):
                g = apply_server_guards(
                    _result(reply="合計は 132,000 円（税込）です。", category="費用の定型案内"),
                    self._SENT, q)
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any("22,000円" in r for r in g.demotion_reasons),
                                g.demotion_reasons)
                g = apply_server_guards(
                    _result(reply="2社分88,000円に3社目以降1社あたり22,000円（税込）を加算した額です。",
                            category="費用の定型案内"), self._SENT, q)
                self.assertTrue(g.can_auto_send, g.demotion_reasons)
        # 費用カテゴリ以外は社数を見ない
        g = apply_server_guards(_result(reply="承知しました。"), [], "十十社あります")
        self.assertTrue(g.can_auto_send)

    def test_hoterasu_reply_aligned(self):
        """法テラス標準回答も同じ規則（44,000円 維持+22,000円 追記）"""
        self.assertIn("1社あたり44,000円（税込）", HOTERASU_STANDARD_REPLY)
        self.assertIn("3社目からは1社あたり22,000円（税込）", HOTERASU_STANDARD_REPLY)
        self.assertIn("前払い", HOTERASU_STANDARD_REPLY)


class TestFeeGuideLengthExemption(unittest.TestCase):
    """JIKOU-FEE-RULE-1-fix3（司令塔裁定 2026-10-11）: 費用の固定文 FEE_GUIDE_TEXT は顧客対応
    経路でも長さ上限（既定 300 字）の免除ブロック。逐語一致した 1 回分のみ免除し、自由文
    部分には従来の上限を適用する（reply_sanitizer.structure_violations の exempt_blocks 機構・
    ヒアリング経路と同じ）。実 API 実測で「固定文 248 字+宛名・前置き 88 字=336 字」が降格した
    対処。AUTOREPLY_MAX_CHARS の引き上げ・固定文の短縮はしない。"""

    PREAMBLE_88 = ("テスト太郎様 お問い合わせありがとうございます。2社のご依頼ですと、"
                   "以下のご案内となります。また、ご不明点があればいつでもお知らせください。"
                   "お手続きの流れもあわせてご案内いたします。")          # 88 字以上

    def _guard(self, reply, history=None, q="費用はいくらですか？2社あります。"):
        return apply_server_guards(
            _result(reply=reply, category="費用の定型案内"), history or [], q)

    def test_a_fixed_text_with_88_char_preamble_auto_sends(self):
        """(a) 固定文+宛名+前置き 88 字（合計 336 字超）の返信が自動送信可"""
        self.assertGreaterEqual(len(self.PREAMBLE_88), 88)
        reply = self.PREAMBLE_88 + "\n\n" + FEE_GUIDE_TEXT
        self.assertGreater(len(reply), 300)
        g = self._guard(reply)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)
        self.assertFalse(any("文字数超過" in r for r in g.demotion_reasons))
        # 本番経路と同じく sanitize_reply を通しても固定文は逐語のまま（免除が成立）
        out, _issues, fatal = reply_sanitizer.sanitize_reply(
            reply, allowed_emoji=ALLOWED_CANONICAL_EMOJI)
        self.assertFalse(fatal)
        self.assertIn(FEE_GUIDE_TEXT, out)
        self.assertTrue(self._guard(out).can_auto_send)

    def test_b_fixed_text_plus_301_free_chars_demoted(self):
        """(b) 固定文+自由文 301 字は降格（自由文には従来の 300 字上限）・300 字は可"""
        g = self._guard("あ" * 301 + "\n" + FEE_GUIDE_TEXT)
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any(r.startswith("文字数超過（自由文301字") for r in g.demotion_reasons),
                        g.demotion_reasons)
        g = self._guard("あ" * 300 + "\n" + FEE_GUIDE_TEXT)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_c_altered_fixed_text_not_exempted(self):
        """(c) 固定文を改変した返信は免除されず降格（逐語一致規則）。改変例: 金額の 1 字・
        読点の削除・行の省略。必須語が残る改変でも文字数超過で止まる"""
        altered = (FEE_GUIDE_TEXT.replace("132,000円", "132,000 円"),      # 1 字挿入
                   FEE_GUIDE_TEXT.replace("・お支払い方法: ", "・支払方法: "),  # 語の改変
                   FEE_GUIDE_TEXT.replace("\n・お支払い: 前払いのみ（分割払いは承っておりません）", ""))
        for text in altered:
            with self.subTest(text=text[:40]):
                self.assertNotIn(FEE_GUIDE_TEXT, text)
                g = self._guard(self.PREAMBLE_88 + "\n\n" + text)
                self.assertFalse(g.can_auto_send)
                self.assertTrue(any("文字数超過" in r for r in g.demotion_reasons),
                                g.demotion_reasons)

    def test_d_fixed_text_twice_exempted_once(self):
        """(d) 固定文 2 回は免除 1 回分のみ（2 回目は自由文として数える→降格）。1 回なら可"""
        pre = "ご案内します。" * 8                                   # 56 字
        once = pre + "\n" + FEE_GUIDE_TEXT
        twice = pre + "\n" + FEE_GUIDE_TEXT + "\n" + FEE_GUIDE_TEXT
        self.assertTrue(self._guard(once).can_auto_send)
        g = self._guard(twice)
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any(r.startswith("文字数超過（自由文") for r in g.demotion_reasons),
                        g.demotion_reasons)
        # 免除は費用固定文だけ: 他の凍結文（法テラス標準回答）は自由文として数える
        g = self._guard("あ" * 250 + "\n" + HOTERASU_STANDARD_REPLY + "\n" + FEE_GUIDE_TEXT,
                        q="法テラスは使えますか？2社あります")
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("文字数超過" in r for r in g.demotion_reasons), g.demotion_reasons)

    def test_e_hearing_route_exemption_unchanged(self):
        """(e) ヒアリング経路の既存 exempt 挙動（main.HEARING_TEMPLATE_BLOCKS）は不変:
        定型ブロックは免除・自由文に 300 字上限。顧客対応経路の免除は費用固定文だけで、
        ヒアリング定型ブロックは顧客対応経路では免除されない（従来どおり）"""
        for k, v in {"ANTHROPIC_API_KEY": "dummy",
                     "LINE_CHANNEL_SECRET": "dummy_secret", "LINE_CHANNEL_ACCESS_TOKEN": "t",
                     "KINTONE_SUBDOMAIN": "testsub", "KINTONE_APP_ID": "21",
                     "KINTONE_API_TOKEN": "dummy", "SOUZOKU_KINTONE_APP_ID": "26",
                     "SOUZOKU_KINTONE_API_TOKEN": "dummy", "CLOUDSIGN_CLIENT_ID": "c",
                     "CLOUDSIGN_WEBHOOK_SECRET": "cs", "KINTONE_WEBHOOK_TOKEN": "k",
                     "DOCUMENT_WEBHOOK_SECRET": "d", "APP_APPROVAL": "29",
                     "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
                     "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "v",
                     "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d"}.items():
            os.environ.setdefault(k, v)
        import main
        blocks = main.HEARING_TEMPLATE_BLOCKS
        self.assertEqual(len(blocks), 2)
        text = "あ" * 300 + "\n" + blocks[0]
        self.assertEqual(reply_sanitizer.structure_violations(text, exempt_blocks=blocks), [])
        self.assertTrue(any("文字数超過" in v for v in
                            reply_sanitizer.structure_violations("あ" * 301 + "\n" + blocks[0],
                                                                 exempt_blocks=blocks)))
        # 顧客対応経路ではヒアリング定型ブロックは免除されない（費用固定文のみ）
        g = apply_server_guards(
            _result(reply=text, category="手続きの一般的な流れ"), [], "教えてください")
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("文字数超過" in r for r in g.demotion_reasons), g.demotion_reasons)
        # 免除ブロックは profile 側の値（時効=FEE_GUIDE_TEXT・既定 ""=免除なし）
        self.assertEqual(JIKOU_PROFILE.fee_guide_text, FEE_GUIDE_TEXT)


class TestMitateReservation(unittest.TestCase):
    """c) 時効見立て_条件付きの留保文言・時効更新事由フラグ"""

    def test_individual_mitate_with_reservation_auto_sends(self):
        g = apply_server_guards(
            _result(reply=_VALID_MITATE_REPLY, category="時効見立て_条件付き"),
            [],
            "時効援用できそうでしょうか",
        )
        self.assertTrue(g.can_auto_send)

    def test_general_mitate_with_proviso_auto_sends(self):
        g = apply_server_guards(
            _result(reply=_VALID_GENERAL_REPLY, category="時効見立て_条件付き"),
            [],
            "5年経過で裁判なしなら消滅しますか",
        )
        self.assertTrue(g.can_auto_send)

    def test_mitate_without_reservation_demotes(self):
        reply = "お伺いした内容ですと、時効援用できる可能性が高いです。"
        g = apply_server_guards(
            _result(reply=reply, category="時効見立て_条件付き"),
            [],
            "時効援用できそうでしょうか",
        )
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("留保文言" in r for r in g.demotion_reasons))

    def test_update_flag_demotes_mitate(self):
        """時効更新事由の疑いフラグが立ったら留保付きでも承認制"""
        g = apply_server_guards(
            _result(
                reply=_VALID_MITATE_REPLY,
                category="時効見立て_条件付き",
                jikou_update_flag=True,
            ),
            [],
            "まだ時効援用できますか",
        )
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("更新事由" in r for r in g.demotion_reasons))

    def test_update_flag_does_not_affect_other_categories(self):
        """更新事由フラグは時効見立て以外の自動送信（挨拶等）を妨げない"""
        g = apply_server_guards(
            _result(jikou_update_flag=True), [], "ありがとうございます"
        )
        self.assertTrue(g.can_auto_send)


class TestImmediateNotice(unittest.TestCase):
    """承認キュー行き時の即時定型文の解決"""

    def test_model_selected_court_doc_notice(self):
        g = apply_server_guards(
            _result(
                category="緊急対応", auto_send=False,
                immediate_notice="court_doc_request",
            ),
            [],
            "裁判所から訴状が届きました",
        )
        self.assertFalse(g.can_auto_send)
        self.assertEqual(g.immediate_notice_text, COURT_DOC_REQUEST_REPLY)

    def test_server_backstop_detects_court_doc_first_report(self):
        """モデルが notice を選ばなくてもサーバー側検知で資料収集文面を送る"""
        g = apply_server_guards(
            _result(category="緊急対応", auto_send=False, immediate_notice="none"),
            [],
            "昨日、裁判所から支払督促という書類が届きました。",
        )
        self.assertEqual(g.immediate_notice, "court_doc_request")

    def test_court_doc_template_not_sent_twice(self):
        """過去に資料収集文面を送信済みなら通常の定型文に戻す"""
        history = [
            {"role": "user", "content": "裁判所から訴状が届きました"},
            {"role": "assistant", "content": COURT_DOC_REQUEST_REPLY},
        ]
        g = apply_server_guards(
            _result(
                category="緊急対応", auto_send=False,
                immediate_notice="court_doc_request",
            ),
            history,
            "写真はこれで大丈夫ですか",
        )
        self.assertEqual(g.immediate_notice, "none")
        self.assertIsNone(g.immediate_notice_text)

    def test_churn_neutral_notice(self):
        g = apply_server_guards(
            _result(category="その他判断系", auto_send=False, immediate_notice="churn_neutral"),
            [],
            "じゃあもういいです",
        )
        self.assertEqual(g.immediate_notice_text, CHURN_NEUTRAL_REPLY)

    def test_out_of_scope_debt_notice(self):
        g = apply_server_guards(
            _result(category="その他判断系", auto_send=False, immediate_notice="out_of_scope_debt"),
            [],
            "住民税の滞納も時効になりますか",
        )
        self.assertEqual(g.immediate_notice_text, OUT_OF_SCOPE_DEBT_REPLY)

    def test_notice_ignored_when_auto_sending(self):
        """自動送信できる場合は即時定型文を使わない"""
        g = apply_server_guards(
            _result(immediate_notice="churn_neutral"), [], "こんにちは"
        )
        self.assertTrue(g.can_auto_send)
        self.assertEqual(g.immediate_notice, "none")


class TestCrisisAndUrgentNotices(unittest.TestCase):
    """FAQ第3弾: 危機対応の専用即時文面と【緊急・要即時対応】通知（2026-07-03）"""

    def test_crisis_support_notice(self):
        g = apply_server_guards(
            _result(category="緊急対応", auto_send=False, immediate_notice="crisis_support"),
            [],
            "借金のことで頭がいっぱいで、正直自殺も考えています。",
        )
        self.assertFalse(g.can_auto_send)
        self.assertEqual(g.immediate_notice_text, CRISIS_SUPPORT_REPLY)

    def test_urgent_seizure_panic_notice(self):
        g = apply_server_guards(
            _result(category="緊急対応", auto_send=False, immediate_notice="urgent_seizure_panic"),
            [],
            "明日にも給料を差し押さえられるかもしれません。",
        )
        self.assertEqual(g.immediate_notice_text, URGENT_SEIZURE_PANIC_REPLY)

    def test_crisis_notices_are_not_deduped(self):
        """危機対応の文面は過去に送信済みでも再送する（汎用文に落とさない）"""
        history = [
            {"role": "user", "content": "死にたいです"},
            {"role": "assistant", "content": CRISIS_SUPPORT_REPLY},
        ]
        g = apply_server_guards(
            _result(category="緊急対応", auto_send=False, immediate_notice="crisis_support"),
            history,
            "やっぱりもう死ぬしかないのかなと思っています。",
        )
        self.assertEqual(g.immediate_notice, "crisis_support")

    def test_urgent_notification_format(self):
        """希死念慮・差押え切迫は【緊急・要即時対応】フォーマットで通知される"""
        for key, kind in URGENT_NOTICE_KINDS.items():
            with self.subTest(key=key):
                msg = build_attorney_notification(
                    "U123", "テスト太郎", "42", "緊急対応",
                    urgent_kind=kind, customer_message="明日差し押さえられるかもしれません",
                )
                self.assertIn("【緊急・要即時対応】", msg)
                self.assertIn(kind, msg)
                # P1-102（RV-10 S1）: 顧客氏名・相談本文は redact される（record No で参照）
                self.assertNotIn("テスト太郎", msg)
                self.assertNotIn("明日差し押さえられる", msg)
                self.assertIn("42", msg)
                self.assertNotIn("【承認依頼】", msg)

    def test_normal_notification_format_unchanged(self):
        """通常の承認依頼フォーマットは従来どおり"""
        msg = build_attorney_notification("U123", "テスト太郎", "42", "法的判断・見通し")
        self.assertIn("【承認依頼】", msg)
        self.assertIn("承認キューレコードNo: 42", msg)
        self.assertNotIn("緊急", msg)

    def test_all_notice_templates_are_forbidden_word_free(self):
        """即時定型文すべてに禁止語が含まれない"""
        for key, text in IMMEDIATE_NOTICE_TEXTS.items():
            with self.subTest(key=key):
                self.assertEqual(find_forbidden_words(text), [])

    def test_faq3_canonical_texts_are_forbidden_word_free(self):
        """FAQ第3弾の確定文言（時効年数・アンケート返送・自宅来訪・差押え範囲等）に
        「時効間近」等の既存禁止語が混入しないこと"""
        for text in FAQ3_CANONICAL_TEXTS:
            with self.subTest(text=text[:20]):
                self.assertEqual(find_forbidden_words(text), [])


class TestCourtDocDetection(unittest.TestCase):
    """裁判所書類第一報のサーバー側検知"""

    def test_positive_reports(self):
        for msg in [
            "昨日、裁判所から訴状が届きました。どうすればいいですか？",
            "裁判所から支払督促という書類が届きました。開けてみたら期限が今週です。",
            "給料を差し押さえると書かれた通知が届きました。",
            "裁判所からの封筒が家に来ていました。",
        ]:
            with self.subTest(msg=msg):
                self.assertTrue(looks_like_court_doc_report(msg))

    def test_negative_answers_are_excluded(self):
        """「届いていない」等の否定回答は第一報として扱わない"""
        for msg in [
            "裁判所からの書類は届いていません。",
            "支払督促は来ていないと思います。",
            "訴状が届いたことはありません。",
            "10年以内に裁判所から何も届いていません。",
        ]:
            with self.subTest(msg=msg):
                self.assertFalse(looks_like_court_doc_report(msg))

    def test_unrelated_message_not_detected(self):
        self.assertFalse(looks_like_court_doc_report("費用はいくらですか？"))


if __name__ == "__main__":
    unittest.main()
