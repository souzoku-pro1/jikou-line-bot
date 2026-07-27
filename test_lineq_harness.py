"""LINE-Q-001: lineq 評価ハーネス（軸定義・judge プロンプト・合成スレッド・機械計数）。

固定する仕様（DRAFT_LINE_QUALITY §3.2）:
- 評価軸は 7 本・各軸に 1/3/5 の係留例・測り方の区分を持つ
- judge プロンプトは決定的に生成され、全軸・係留例・JSON 出力契約・transcript を含む
  （API 呼出しは存在しない=モデル選定[人]ゲート）
- 合成スレッドは 20〜30 本・主要3群＋希少3層（各3本以上）・PII 様パターンなし・
  delivery は allowlist の 5 値 enum 内
- 機械計数は決定的（質問数違反・専門用語密度・不安→降格 ack・反復検出）
- G0 規律: lineq は本番応答モジュール（chat_responder/main）を import しない
"""

import re
import unittest
from pathlib import Path

from lineq.axes import AXES, AXIS_IDS
from lineq.judge_prompt import build_judge_prompt, render_transcript
from lineq.metrics import compute_corpus_metrics, compute_thread_metrics
from lineq.synthetic_threads import MAIN_LAYERS, RARE_LAYERS, THREADS

_ALLOWED_DELIVERY = {"auto", "approved", "demoted", "immediate", "silent"}
_CATEGORIES = {
    "挨拶・雑談", "手続きの一般的な流れ", "必要書類の案内", "費用の定型案内",
    "進捗の事実回答", "営業案内・アクセス", "時効見立て_条件付き",
    "法的判断・見通し", "費用交渉・減額相談", "クレーム・不満", "解約・辞任関係",
    "緊急対応", "本人確認不能・第三者", "その他判断系",
}
_PII_RES = [
    re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}"),       # 電話番号様
    re.compile(r"\d{3}-\d{4}"),                     # 郵便番号様
    re.compile(r"\d{7,}"),                          # 長数字列（口座様）
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+"),  # メール様
]


class TestAxes(unittest.TestCase):
    def test_seven_axes_with_anchors(self):
        self.assertEqual(len(AXES), 7)
        self.assertEqual(len(set(AXIS_IDS)), 7)
        for a in AXES:
            with self.subTest(axis=a.axis_id):
                self.assertIn(a.method, ("judge", "machine", "both"))
                self.assertEqual(set(a.anchors), {1, 3, 5})   # 係留例 1/3/5
                self.assertTrue(a.definition)


class TestJudgePrompt(unittest.TestCase):
    def test_prompt_contains_axes_anchors_contract_and_transcript(self):
        thread = THREADS[0]
        p = build_judge_prompt(thread)
        for a in AXES:
            self.assertIn(a.axis_id, p)
            self.assertIn(a.name, p)
            self.assertIn(a.anchors[5], p)
        self.assertIn("JSON", p)
        self.assertIn("worst_exchange", p)
        self.assertIn(thread["turns"][0]["text"], p)   # transcript を含む
        self.assertEqual(p, build_judge_prompt(thread))  # 決定的

    def test_silent_thread_renders_explicitly(self):
        silent = next(t for t in THREADS if t["layer"] == "rare:silent")
        self.assertIn("無言スレッド", render_transcript({"turns": []}))
        self.assertIn(silent["turns"][0]["text"], render_transcript(silent))


class TestSyntheticThreads(unittest.TestCase):
    def test_count_and_unique_ids(self):
        self.assertGreaterEqual(len(THREADS), 20)
        self.assertLessEqual(len(THREADS), 30)
        ids = [t["thread_id"] for t in THREADS]
        self.assertEqual(len(set(ids)), len(ids))

    def test_schema_and_enums(self):
        for t in THREADS:
            with self.subTest(thread=t["thread_id"]):
                self.assertIn(t["layer"], MAIN_LAYERS + RARE_LAYERS)
                for turn in t["turns"]:
                    self.assertIn(turn["role"], ("user", "assistant"))
                    self.assertTrue(turn["text"])
                    if turn["role"] == "assistant":
                        self.assertIn(turn["delivery"], _ALLOWED_DELIVERY)
                        self.assertIn(turn["category"], _CATEGORIES)

    def test_layer_coverage(self):
        by_layer: dict[str, int] = {}
        for t in THREADS:
            by_layer[t["layer"]] = by_layer.get(t["layer"], 0) + 1
        for layer in RARE_LAYERS:
            self.assertGreaterEqual(by_layer.get(layer, 0), 3, by_layer)  # 各層3本以上
        for layer in MAIN_LAYERS:
            self.assertGreaterEqual(by_layer.get(layer, 0), 3, by_layer)

    def test_silent_threads_have_no_assistant_turn(self):
        for t in THREADS:
            if t["layer"] == "rare:silent":
                self.assertFalse(
                    [x for x in t["turns"] if x["role"] == "assistant"],
                    t["thread_id"])

    def test_no_pii_like_patterns(self):
        for t in THREADS:
            for turn in t["turns"]:
                for rx in _PII_RES:
                    # 費用定型の「44,000円」は桁区切り付き・長数字列に該当しない
                    self.assertIsNone(rx.search(turn["text"].replace("44,000", "")),
                                      (t["thread_id"], rx.pattern))


class TestMetrics(unittest.TestCase):
    def test_multi_question_violation_detected(self):
        th = {"turns": [
            {"role": "assistant", "text": "いつですか？どこですか？",
             "category": "その他判断系", "delivery": "auto"}]}
        self.assertEqual(compute_thread_metrics(th)["multi_question_turns"], 1)

    def test_jargon_density_counts_terms(self):
        th = {"turns": [
            {"role": "assistant", "text": "消滅時効の援用を行います。" + "あ" * 90,
             "category": "その他判断系", "delivery": "auto"}]}
        self.assertGreater(compute_thread_metrics(th)["jargon_per_100_chars"], 0)

    def test_anxiety_followed_by_demoted_ack_detected(self):
        th = next(t for t in THREADS if t["thread_id"] == "S012")
        m = compute_thread_metrics(th)
        self.assertEqual(m["anxiety_events"], 1)
        self.assertEqual(m["demoted_after_anxiety"], 1)

    def test_duplicate_assistant_text_detected(self):
        th = next(t for t in THREADS if t["thread_id"] == "S015")
        self.assertEqual(compute_thread_metrics(th)["duplicate_assistant_texts"], 1)

    def test_corpus_metrics_over_all_synthetic_threads(self):
        agg = compute_corpus_metrics(THREADS)
        self.assertEqual(agg["threads"], len(THREADS))
        self.assertGreaterEqual(agg["anxiety_demoted_rate"], 0.0)
        self.assertEqual(len(agg["per_thread"]), len(THREADS))


class TestG0Separation(unittest.TestCase):
    def test_lineq_does_not_import_production_response_modules(self):
        # G0 規律: 相1 ハーネスは本番応答経路（§2.1 の変更対象外）を import しない
        import ast
        banned = {"chat_responder", "main", "claude_gateway", "hub"}
        for f in Path("lineq").glob("*.py"):
            tree = ast.parse(f.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for al in node.names:
                        self.assertNotIn(al.name.split(".")[0], banned,
                                         (f.name, al.name))
                elif isinstance(node, ast.ImportFrom):
                    mod = (node.module or "").split(".")[0]
                    self.assertNotIn(mod, banned, (f.name, node.module))


if __name__ == "__main__":
    unittest.main()
