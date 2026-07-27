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
    # fix1 M01: 金額一般（実額）。丸め帯「◯万円台」「数万円」は数字が円に直結
    # しないため非該当（許可・allowlist の meta.amount_band と整合）
    re.compile(r"[¥￥]\s?\d"),                      # 通貨記号+数字
    re.compile(r"\d[\d,]{2,}円"),                   # 実額の円表記（44,000円 等）
    # fix1 M01: 住所・口座 sentinel（代表形）
    re.compile(r"\d+(丁目|番地|号室)"),
    re.compile(r"口座番号|普通預金\s?\d+"),
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

    def test_no_pii_like_patterns_machine_layer(self):
        """機械層の PII 検査（fix1 H02: 特例除外なしの全文検査）。

        保証範囲の契約（fix1 M01・二層）: 本テストが保証するのは**パターンで
        機械検出できる類型**（電話/郵便/長数字列/メール/実額金額/住所・口座
        sentinel）のみ。氏名等のパターン化できない類型は**固定24本の目視
        レビュー**（Codex レビュー＋[人]）が第二層として担う。"""
        for t in THREADS:
            for turn in t["turns"]:
                for rx in _PII_RES:
                    self.assertIsNone(rx.search(turn["text"]),
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


_G0_BANNED = {"chat_responder", "main", "claude_gateway", "hub"}


def _g0_scan_static_imports(src: str) -> list[str]:
    """旧検査（fix1 前）: 静的 Import/ImportFrom のみを見る（メタテスト用に保存）。"""
    import ast
    out = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Import):
            out += [al.name for al in node.names
                    if al.name.split(".")[0] in _G0_BANNED]
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] in _G0_BANNED:
                out.append(node.module or "")
    return out


def _g0_scan(src: str) -> list[str]:
    """G0 検査（fix1 H01 強化版）。

    検出対象: (1) 静的 Import/ImportFrom（禁止 module）
    (2) __import__(...) 呼出し (3) importlib.import_module(...) 呼出し
    （from importlib import import_module の別名含む） (4) exec(...) 呼出し。
    (2)-(4) は引数によらず**呼出し自体を違反**とする（相1 ハーネスに動的
    import/実行時コード実行の正当用途は無い）。

    残余（静的解析の限界・規律とレビューに委任）: getattr(builtins, 変数)・
    eval で組み立てた import 等の**完全動的経路**は検出できない。"""
    import ast
    out = list(_g0_scan_static_imports(src))
    tree = ast.parse(src)
    import_module_aliases = {"import_module"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "importlib":
            for al in node.names:
                if al.name == "import_module":
                    import_module_aliases.add(al.asname or al.name)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Name) and f.id in ({"__import__", "exec"}
                                                | import_module_aliases):
            out.append(f"dynamic:{f.id}")
        elif (isinstance(f, ast.Attribute) and f.attr == "import_module"
              and isinstance(f.value, ast.Name) and f.value.id == "importlib"):
            out.append("dynamic:importlib.import_module")
    return out


class TestG0Separation(unittest.TestCase):
    def test_lineq_does_not_import_production_response_modules(self):
        # G0 規律: 相1 ハーネスは本番応答経路（§2.1 の変更対象外）へ静的にも
        # 動的にも到達しない（検出対象と残余は _g0_scan docstring）
        for f in Path("lineq").glob("*.py"):
            self.assertEqual(_g0_scan(f.read_text(encoding="utf-8")), [], f.name)

    def test_meta_dynamic_import_caught_by_new_scan_only(self):
        """fix1 H01 メタテスト: 旧検査（静的 import のみ）では通過し、
        新検査では FAIL する違反 fixture を固定（検査強化の実効性を pin）。"""
        fixtures = {
            "dunder_import": "x = __import__('chat_responder')\n",
            "importlib_module": ("import importlib\n"
                                 "m = importlib.import_module('chat_responder')\n"),
            "importlib_from": ("from importlib import import_module as im\n"
                               "m = im('main')\n"),
            "exec_call": "exec('import chat_responder')\n",
        }
        for label, src in fixtures.items():
            with self.subTest(case=label):
                self.assertEqual(_g0_scan_static_imports(src), [], label)  # 旧: 通過
                self.assertTrue(_g0_scan(src), label)                      # 新: 検出

    def test_meta_static_import_still_caught(self):
        src = "import chat_responder\n"
        self.assertTrue(_g0_scan_static_imports(src))
        self.assertTrue(_g0_scan(src))


if __name__ == "__main__":
    unittest.main()
