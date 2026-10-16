"""WORDING-1（QUEUE-2026-10-14-G G-4）: 相談者向け文言の安全点検スクリプトのテスト

- 語リスト（hub/wording_rules.py・要大野確認）の各観点について「検出すべき例」と「検出してはいけない例」を固定する。
- scripts/check_reply_wording.py は AST 抽出のみ（アプリの import・env・外部 I/O なし）で、同じソースなら同じ報告（冪等）。
- pin: 新 module は sink 違反ゼロ・allowlist 追加ゼロ・文言ファイル（chat_responder／shindan_link／main）は不変（本票は報告のみ）。
"""

import importlib.util
import pathlib
import tempfile
import unittest

from hub import wording_rules as W
from test_sink_ast_policy import scan_source

REPO = pathlib.Path(__file__).parent
_spec = importlib.util.spec_from_file_location("check_reply_wording", REPO / "scripts" / "check_reply_wording.py")
C = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(C)


def _rules(text: str) -> set:
    return {h["rule"] for h in W.check_text(text)}


class TestRules(unittest.TestCase):
    def test_assertive_detects_and_negation_is_exempt(self):
        for t in ("時効は必ず成立します。", "確実に消えます", "100%大丈夫です", "絶対に止まります", "間違いなく受任できます",
                  "成立を保証します", "確実な時期としては 2027年3月末 頃をおすすめいたします"):
            self.assertIn(W.RULE_ASSERTIVE, _rules(t), t)
        for t in ("絶対に大丈夫とは言えません", "必ず消えるとは限りません", "確実とは申し上げられません", "保証はできません",
                  "必ずしも成立するものではありません", "お支払いは前払いのみです"):
            self.assertNotIn(W.RULE_ASSERTIVE, _rules(t), t)

    def test_amount_format_canonical_vs_variants(self):
        for t in ("1社あたり44,000円（税込）", "例: 3社 110,000円・4社 132,000円（税込）", "500円（税込）"):
            self.assertNotIn(W.RULE_AMOUNT_FORMAT, _rules(t), t)
        for t in ("44000円（税込）", "4万4千円（税込）", "４４，０００円（税込）", "44,000 円（税込）", "4.4万円（税込）"):
            self.assertIn(W.RULE_AMOUNT_FORMAT, _rules(t), t)

    def test_tax_missing_by_sentence(self):
        self.assertNotIn(W.RULE_TAX_MISSING, _rules("費用は1社あたり44,000円（税込）の前払いです。"))
        self.assertNotIn(W.RULE_TAX_MISSING, _rules("・3社目からは1社あたり22,000円（税込）です。"))
        # 文（。／改行）単位で見る: 例示の文に税込が無ければ検出（FEE_GUIDE_TEXT の「例: 3社 110,000円」行も同じ扱い＝報告のみ）
        self.assertIn(W.RULE_TAX_MISSING, _rules("・3社目からは1社あたり22,000円（税込）。例: 3社 110,000円・4社 132,000円"))
        self.assertIn(W.RULE_TAX_MISSING, _rules("費用は44,000円です。"))
        self.assertIn(W.RULE_TAX_MISSING, _rules("費用は44,000円です。\n税込です。"))      # 別の文の税込は数えない
        self.assertNotIn(W.RULE_TAX_MISSING, _rules("受任から完了まで2〜4週間程度。"))     # 金額なし

    def test_vague_deadline(self):
        for t in ("すぐに止まります", "近日中にご連絡します", "早急に対応します", "なるべく早くお送りください", "追ってご連絡します"):
            self.assertIn(W.RULE_VAGUE_DEADLINE, _rules(t), t)
        for t in ("契約後24時間以内に発送します", "基本的に10日以内に止まります", "受任から完了まで2〜4週間程度です"):
            self.assertNotIn(W.RULE_VAGUE_DEADLINE, _rules(t), t)

    def test_phone_invite_vs_mention(self):
        for t in ("お電話ください", "電話でご連絡ください", "お電話をお待ちしております", "電話番号をお知らせください",
                  "TELにてお問い合わせください"):
            self.assertIn(W.RULE_PHONE, _rules(t), t)
        for t in ("業者からの電話には出ないでください", "ご自宅への電話や郵送は行いません", "電話対応は不要です",
                  "LINEでご連絡ください"):
            self.assertNotIn(W.RULE_PHONE, _rules(t), t)

    def test_check_text_never_raises_and_snippet_masks_phone_like(self):
        for t in ("", None, "🙂" * 50, "\x00\x1f", "あ" * 50_000):
            self.assertIsInstance(W.check_text(t), list)
        s = W.snippet("お電話ください 048-123-4567 まで", 0, width=40)
        self.assertNotIn("123-4567", s)


class TestScript(unittest.TestCase):
    def test_sources_are_collected_from_ast_only(self):
        src = C.collect_sources(REPO)
        self.assertGreaterEqual(len(src[C.GROUP_FIXED]), 10)
        self.assertGreaterEqual(len(src[C.GROUP_FAQ]), 20)
        self.assertEqual([i for i, _s, _t in src[C.GROUP_GREETING]], ["FROZEN_GREETING"])
        self.assertEqual([i for i, _s, _t in src[C.GROUP_HEARING]], ["_HEARING_PROMPT_FROZEN"])
        ids = {i for i, _s, _t in src[C.GROUP_FIXED]}
        self.assertTrue({"FEE_GUIDE_TEXT", "HOTERASU_STANDARD_REPLY", "PENDING_REPLY"} <= ids, ids)
        self.assertNotIn("STYLE_RULES_TEXT", ids)
        if (REPO / "hub" / "jikou_reply_texts.py").exists():
            q2 = src[C.GROUP_Q2]
            self.assertGreaterEqual(len(q2), 20)
            self.assertIn(("J-7", "frozen_v14"), {(i, s) for i, s, _t in q2})
        else:
            self.assertEqual(src[C.GROUP_Q2], [])

    def test_faq_items_join_continuation_lines(self):
        rows = {i: t for i, _s, t in C.collect_faq(REPO)}
        phone = [t for i, t in rows.items() if t.startswith("業者からの電話")]
        self.assertEqual(len(phone), 1)
        self.assertGreater(len(phone[0].split("\n")), 1)

    def test_report_is_deterministic_and_marks_for_review(self):
        with tempfile.TemporaryDirectory() as d:
            out = pathlib.Path(d) / "r.md"
            self.assertEqual(C.main(["--out", str(out), "--repo", str(REPO)]), 0)
            first = out.read_text(encoding="utf-8")
            C.main(["--out", str(out), "--repo", str(REPO)])
            self.assertEqual(first, out.read_text(encoding="utf-8"))
        self.assertIn("要大野確認", first)
        self.assertIn("文言は未修正", first)
        for g in C.GROUPS:
            self.assertIn(g, first)
        self.assertNotRegex(first, r"\bU[0-9a-f]{32}\b")

    def test_pins_sink_allowlist_and_no_wording_changes(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/wording_rules.py", "scripts/check_reply_wording.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
            self.assertNotIn(name, allow)
        self.assertIn("要大野確認", (REPO / "hub" / "wording_rules.py").read_text(encoding="utf-8"))
        self.assertEqual(W.RULES, (W.RULE_ASSERTIVE, W.RULE_AMOUNT_FORMAT, W.RULE_TAX_MISSING, W.RULE_VAGUE_DEADLINE, W.RULE_PHONE))


if __name__ == "__main__":
    unittest.main()
