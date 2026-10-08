"""JIKOU-FURIGANA-1（大野裁定 2026-10-08）: 時効ヒアリングで「お名前とふりがな」を聞き、
App 21 の furigana 欄（ラベル ふりがな・SINGLE_LINE_TEXT・任意）へ空欄のみ CAS で書く。

票の固定点:
(a) 凍結台本は「①お名前」→「①お名前とふりがな」の 1 問と KINTONE_UPDATE の ふりがな キー
    ＋分離指示 1 行のみ変更。②〜⑤・定型ブロック数・第 1 段階・SYSTEM_PROMPT の他部分は不変
(b) KINTONE_UPDATE に ふりがな が来たら furigana へ空欄のみ・$revision CAS で書く
(c) カタカナ（全角・半角）→ ひらがな。空白は全角 1 個へ統一
(d) 漢字混在・空は書かない（失敗にしない・件数ログのみ・対象外キーには数えない）
(e) ふりがなの値はログ・例外文字列・通知・冪等キー・戻り値に出ない（RV-10）
(f) furigana に既に値があるレコードは上書きしない（preexisting）
"""

import logging
import re
import unittest
from unittest.mock import patch

# 環境変数の既定・App 21 fake・patch 基盤は HOTFIX-1 のテストと同一の正を使う
from test_jikou_hearing_hotfix import (  # noqa: E402
    ADDR, BIRTH, MAIL, NAME, PHONE, REPLY_HEAD, USER, VALUES,
    _FlowBase, _LogCapture, _ModuleBase, _run, hu, main,
)

KANA = "やまだたろう"
KANA_KATA = "ヤマダタロウ"
KANA_HALF = "ﾔﾏﾀﾞ ﾀﾛｳ"
KANA_MIXED = "山田たろう"
KANA_SPACED = "やまだ　たろう"

FIELDS6 = {"顧客名": NAME, "ふりがな": KANA, "住所": ADDR, "生年月日": BIRTH,
           "電話番号": PHONE, "メールアドレス": MAIL}


# ── (a) 台本 ──────────────────────────────────────────────────────────────────
class TestScript(unittest.TestCase):
    def test_name_question_asks_furigana_and_others_unchanged(self):
        body = main._HEARING_PROMPT_FROZEN
        self.assertIn("①お名前とふりがな\n②ご住所\n③生年月日\n④電話番号\n⑤メールアドレス\n", body)
        self.assertNotIn("①お名前\n", body)                 # 旧 1 問は残らない
        self.assertEqual(body.count("①お名前とふりがな"), 1)

    def test_update_marker_has_furigana_key_and_split_instruction(self):
        body = main._HEARING_PROMPT_FROZEN
        start = body.index("[KINTONE_UPDATE]")
        block = body[start:body.index("[/KINTONE_UPDATE]")]
        keys = re.findall(r'^\s*"([^"]+)":', block, re.M)
        self.assertEqual(keys, ["顧客名", "ふりがな", "住所", "生年月日", "電話番号",
                                "メールアドレス"])
        self.assertIn("「ふりがな」には氏名の読み（ひらがな）のみ", body)
        # 第 1 段階（KINTONE_RECORD）は不変
        rec = body[body.index("[KINTONE_RECORD]"):body.index("[/KINTONE_RECORD]")]
        self.assertEqual(re.findall(r'^\s*"([^"]+)":', rec, re.M),
                         ["問い合わせ業者名", "借入時期_テキスト", "最終返済日_テキスト",
                          "裁判所書類", "信用情報確認"])
        self.assertNotIn("ふりがな", rec)

    def test_template_blocks_still_two_and_contain_new_question(self):
        self.assertEqual(len(main.HEARING_TEMPLATE_BLOCKS), 2)
        self.assertIn("①お名前とふりがな", main.HEARING_TEMPLATE_BLOCKS[1])
        self.assertEqual(main.HEARING_TEMPLATE_BLOCKS,
                         main._extract_template_blocks(main._HEARING_PROMPT_FROZEN))
        # SYSTEM_PROMPT は本文＋文体差し込みのみ（差し込みを外せば本文と逐語一致）
        restored = main.SYSTEM_PROMPT.replace(
            main.HEARING_STYLE_SECTION + "\n\n" + main._HEARING_KINTONE_HEADING,
            main._HEARING_KINTONE_HEADING, 1)
        self.assertEqual(restored, main._HEARING_PROMPT_FROZEN)


# ── (b)(c)(d)(f) 書込 ─────────────────────────────────────────────────────────
class TestFuriganaWrite(_ModuleBase):
    def setUp(self):
        super().setUp()
        self.cap = _LogCapture()
        root = logging.getLogger()
        self._old_level = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(self.cap)
        self.addCleanup(root.removeHandler, self.cap)
        self.addCleanup(root.setLevel, self._old_level)

    def test_b_furigana_written_only_when_empty_with_revision(self):
        self.fake.add("10", revision="5")
        r = _run(hu.apply_update("10", FIELDS6))
        self.assertEqual(r["outcome"], hu.OUTCOME_UPDATED)
        self.assertIn("furigana", r["written"])
        self.assertEqual(len(self.fake.update_calls), 1)
        rid, fields, rev = self.fake.update_calls[0]
        self.assertEqual((rid, rev), ("10", "5"))
        self.assertEqual(fields["furigana"], KANA)
        self.assertNotIn("ふりがな", fields)                 # PUT は欄コードのみ
        self.assertEqual(self.fake.values("10", "furigana"), KANA)
        self.assertEqual(self.fake.values("10", "顧客名"), NAME)

    def test_b_field_code_key_also_accepted(self):
        self.fake.add("10")
        r = _run(hu.apply_update("10", {"furigana": KANA}))
        self.assertEqual(r["written"], ["furigana"])
        self.assertEqual(r["dropped_count"], 0)

    def test_b_notice_lists_furigana_label_in_fixed_order(self):
        self.fake.add("10")
        _run(hu.handle_update(USER, None, FIELDS6))
        text = self.notice()
        self.assertIn("登録した欄: 氏名, ふりがな, 住所, 生年月日, 電話番号, メールアドレス", text)
        self.assertNotIn(KANA, text)

    def test_c_katakana_converted_to_hiragana(self):
        self.fake.add("10")
        _run(hu.apply_update("10", {"ふりがな": KANA_KATA}))
        self.assertEqual(self.fake.values("10", "furigana"), KANA)

    def test_c_halfwidth_katakana_and_space_unified(self):
        self.fake.add("10")
        _run(hu.apply_update("10", {"ふりがな": KANA_HALF}))
        self.assertEqual(self.fake.values("10", "furigana"), KANA_SPACED)   # 全角統一

    def test_c_normalize_function_cases(self):
        n = hu.normalize_furigana
        self.assertEqual(n("  やまだ　たろう  "), KANA_SPACED)
        self.assertEqual(n("やまだ  たろう"), KANA_SPACED)          # 半角空白 2 個 → 全角 1 個
        self.assertEqual(n("ヤマダ　タロー"), "やまだ　たろー")      # 長音は保持
        self.assertEqual(n("ｶﾞ"), "が")                             # 半角濁点の合成
        self.assertEqual(n("ヴ"), "ゔ")
        for bad in ("山田たろう", "やまだ taro", "ヤマダ1", "", "   ", None, "ヷ"):
            with self.subTest(bad=bad):
                self.assertEqual(n(bad), "")

    def test_d_kanji_mixed_not_written_count_log_only(self):
        self.fake.add("10")
        r = _run(hu.apply_update("10", {"顧客名": NAME, "ふりがな": KANA_MIXED}))
        self.assertEqual(r["outcome"], hu.OUTCOME_UPDATED)        # 他欄は書ける（失敗にしない）
        self.assertEqual(r["written"], ["顧客名"])
        self.assertEqual(r["dropped_count"], 0)                    # 対象外キーには数えない
        self.assertEqual(self.fake.update_calls[0][1], {"顧客名": NAME})
        self.assertEqual(self.fake.values("10", "furigana"), "")
        logs = self.cap.text()
        self.assertIn("furigana not writable count=1", logs)
        self.assertNotIn(KANA_MIXED, logs)

    def test_d_empty_furigana_not_written_and_not_counted(self):
        self.fake.add("10")
        r = _run(hu.apply_update("10", {"顧客名": NAME, "ふりがな": ""}))
        self.assertEqual(r["written"], ["顧客名"])
        self.assertEqual(r["dropped_count"], 0)
        self.assertNotIn("furigana not writable", self.cap.text())  # 空は値なし＝件数ログもなし

    def test_d_only_invalid_furigana_is_noop_not_failure(self):
        self.fake.add("10")
        r = _run(hu.apply_update("10", {"ふりがな": KANA_MIXED}))
        self.assertEqual(r["outcome"], hu.OUTCOME_NOOP)
        self.assertEqual(self.fake.update_calls, [])

    def test_d_invalid_furigana_notice_has_no_foreign_count_line(self):
        self.fake.add("10")
        _run(hu.handle_update(USER, None, {"顧客名": NAME, "ふりがな": KANA_MIXED}))
        text = self.notice()
        self.assertIn("登録した欄: 氏名", text)
        self.assertNotIn("対象外", text)
        self.assertNotIn(KANA_MIXED, text)

    def test_f_preexisting_furigana_not_overwritten(self):
        self.fake.add("10", furigana="てにゅうりょく")
        r = _run(hu.handle_update(USER, None, FIELDS6))
        self.assertEqual(r, ("10", hu.METHOD_SEARCH))
        self.assertEqual(self.fake.values("10", "furigana"), "てにゅうりょく")
        self.assertNotIn("furigana", self.fake.update_calls[0][1])
        text = self.notice()
        self.assertIn("既に値があり登録しなかった欄: ふりがな", text)
        self.assertNotIn("てにゅうりょく", text)

    def test_f_refetch_after_409_respects_newly_entered_furigana(self):
        self.fake.add("10", revision="5")
        self.fake.conflict_next = 1
        real_update = self.fake.update

        async def _update(app, rid, fields, revision=None):
            if self.fake.conflict_next:
                self.fake.rows[rid]["furigana"] = {"value": "ひとがいれた"}
            return await real_update(app, rid, fields, revision)
        with patch.object(hu.hub_kintone, "update_record", _update):
            r = _run(hu.apply_update("10", FIELDS6))
        self.assertEqual(r["outcome"], hu.OUTCOME_UPDATED)
        self.assertEqual(r["preexisting"], ["furigana"])
        self.assertEqual(self.fake.values("10", "furigana"), "ひとがいれた")


# ── (e) RV-10: 値はどこにも出ない ────────────────────────────────────────────
class TestFuriganaRedaction(_ModuleBase):
    def setUp(self):
        super().setUp()
        self.cap = _LogCapture()
        root = logging.getLogger()
        self._old_level = root.level
        root.setLevel(logging.DEBUG)
        root.addHandler(self.cap)
        self.addCleanup(root.removeHandler, self.cap)
        self.addCleanup(root.setLevel, self._old_level)

    def _assert_no_leak(self, ret, extra=()):
        text = self.notify.await_args.args[0]
        key = self.notify.await_args.kwargs["throttle_key"]
        for leak in (KANA, KANA_KATA, KANA_MIXED) + tuple(extra):
            self.assertNotIn(leak, text)                   # 通知本文
            self.assertNotIn(leak, key)                    # 冪等キー
            self.assertNotIn(leak, self.cap.text())        # 全ログ
            self.assertNotIn(leak, repr(ret))              # 戻り値（復唱）
        return text

    def test_e_success_path_no_value_anywhere(self):
        self.fake.add("10")
        ret = _run(hu.handle_update(USER, None, {**FIELDS6, "ふりがな": KANA_KATA}))
        self._assert_no_leak(ret, VALUES)

    def test_e_kintone_error_path_no_value_in_logs(self):
        self.fake.add("10")

        async def _update(app, rid, fields, revision=None):
            raise hu.hub_kintone.KintoneError(400, "CB_VA01", "bad " + fields.get("furigana", ""))
        with patch.object(hu.hub_kintone, "update_record", _update):
            ret = _run(hu.handle_update(USER, None, FIELDS6))
        self.assertEqual(ret, ("10", hu.METHOD_SEARCH))
        text = self._assert_no_leak(ret)
        self.assertIn("失敗", text)

    def test_e_unexpected_exception_path_fixed_reason_only(self):
        self.fake.add("10")

        async def _boom(app, record_id):
            raise RuntimeError("boom " + KANA)
        with patch.object(hu.hub_kintone, "get_record", _boom):
            ret = _run(hu.handle_update(USER, None, FIELDS6))
        self.assertEqual(ret, ("", hu.METHOD_ERROR))
        self._assert_no_leak(ret)
        errs = [r.getMessage() for r in self.cap.errors()]
        self.assertEqual(len(errs), 1)
        self.assertIn("unexpected failure (fixed reason)", errs[0])

    def test_e_redact_kind_name_is_pii(self):
        from hub import redact
        self.assertIn("name", redact.KINDS_PII)


# ── 受信イベント経由（main → hearing_update → App 21） ──────────────────────
class TestFlowWithFurigana(_FlowBase):
    MARKER = (REPLY_HEAD + "\n[KINTONE_UPDATE]\n"
              '{"顧客名": "' + NAME + '", "ふりがな": "' + KANA_KATA + '", "住所": "' + ADDR
              + '", "生年月日": "' + BIRTH + '", "電話番号": "' + PHONE
              + '", "メールアドレス": "' + MAIL + '"}\n[/KINTONE_UPDATE]')

    def test_marker_with_furigana_writes_hiragana_and_sends_clean(self):
        self.fake.add("10", revision="5")
        self.ask.return_value = self.MARKER
        self.run_event()
        self.assert_clean_send()
        rid, fields, rev = self.fake.update_calls[0]
        self.assertEqual((rid, rev), ("10", "5"))
        self.assertEqual(fields, {"顧客名": NAME, "furigana": KANA, "住所": ADDR,
                                  "生年月日": BIRTH, "電話番号": PHONE, "メールアドレス": MAIL})
        self.assertIn(USER, main.hearing_completed)
        text = self.notify.await_args.args[0]
        self.assertIn("ふりがな", text)
        for v in VALUES + (KANA, KANA_KATA):
            self.assertNotIn(v, text)
            self.assertNotIn(v, self.sent_text())

    def test_marker_with_empty_furigana_still_writes_other_fields(self):
        self.fake.add("10")
        self.ask.return_value = self.MARKER.replace(KANA_KATA, "")
        self.run_event()
        self.assert_clean_send()
        fields = self.fake.update_calls[0][1]
        self.assertNotIn("furigana", fields)
        self.assertEqual(fields["顧客名"], NAME)
        self.assertEqual(self.fake.values("10", "furigana"), "")


if __name__ == "__main__":
    unittest.main()
