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

import unittest

from chat_responder import (
    APPROVED_PHONE_INSTRUCTION,
    AUTO_SEND_CATEGORIES,
    CHURN_NEUTRAL_REPLY,
    COURT_DOC_REQUEST_REPLY,
    FEE_GUIDE_TEXT,
    FEE_REQUIRED_PHRASES,
    OUT_OF_SCOPE_DEBT_REPLY,
    apply_server_guards,
    find_forbidden_words,
    looks_like_court_doc_report,
)


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
        """「無視してはいけません」のような打ち消しは禁止語にしない"""
        reply = "裁判所からの書類は無視してはいけません。放置してはいけない書類です。"
        self.assertEqual(find_forbidden_words(reply), [])

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
        """前払い・分割不可・不成立時費用の言及が欠けたら降格"""
        reply = "費用は1社あたり44,000円です。お支払いは銀行振込またはカード決済です。"
        g = apply_server_guards(
            _result(reply=reply, category="費用の定型案内"), [], "費用はいくらですか"
        )
        self.assertFalse(g.can_auto_send)
        self.assertTrue(any("必須文言" in r for r in g.demotion_reasons))

    def test_other_categories_not_checked_for_fee_phrases(self):
        """費用カテゴリ以外には費用必須文言を要求しない"""
        g = apply_server_guards(
            _result(reply="こんにちは。ご連絡ありがとうございます。"), [], "こんにちは"
        )
        self.assertTrue(g.can_auto_send)


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
