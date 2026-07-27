"""LINE-LOG-1 準備票: tools/line_log_anonymize の合成データ試験（実ログ非接触）。

固定する仕様（DRAFT_LINE_QUALITY §4.1＋手順書ドラフト）:
- スレッド境界=72h 無応答分割・同一 user 最新1本の dedup（再現可能）
- 層割当: 希少優先（silent＞immediate）→主要3群（降格＞承認＞自動）・単一層のみ
- redaction 写像（電話/郵便/メール/token/長数字列→トークン）と残存検査
- fail-closed: 400字超・残存ヒット・enum 外 category のスレッドは出力ゼロ
- 出力は allowlist フィールドのみ・summary は件数のみ
- 定型文言の複製が本番定数と drift していないこと（同期テスト）
"""

import unittest
from datetime import datetime, timedelta

from tools.line_log_anonymize import (
    IMMEDIATE_TEXT_MARKERS,
    PENDING_REPLY_TEXT,
    classify_delivery,
    convert,
    redact_text,
    residue_hits,
    split_threads,
)

_T0 = datetime(2026, 7, 1, 10, 0)


def _row(user, role, text, dt, category="その他判断系"):
    return {"line_user_id": user, "role": role, "message": text,
            "category": category, "auto_sent": "yes", "_dt": dt}


def _pair(user, utext, atext, dt, category="挨拶・雑談"):
    return [_row(user, "user", utext, dt),
            _row(user, "assistant", atext, dt + timedelta(minutes=1), category)]


class TestThreadSplitAndDedup(unittest.TestCase):
    def test_72h_gap_splits_and_latest_thread_only(self):
        rows = (_pair("U1", "こんにちは", "こんにちは。", _T0)
                + _pair("U1", "その後どうですか", "確認します。", _T0 + timedelta(hours=100)))
        threads = split_threads(rows)
        self.assertEqual(len(threads["U1"]), 2)          # 72h 超で分割
        out = convert(rows, set())
        self.assertEqual(out["summary"]["picked"], 1)    # 最新スレッド1本のみ
        self.assertIn("その後どうですか",
                      out["threads"][0]["turns"][0]["text"])

    def test_within_72h_stays_one_thread(self):
        rows = (_pair("U1", "質問です", "はい。", _T0)
                + _pair("U1", "続きです", "どうぞ。", _T0 + timedelta(hours=71)))
        self.assertEqual(len(split_threads(rows)["U1"]), 1)


class TestLayerAssignment(unittest.TestCase):
    def test_priority_silent_over_all(self):
        rows = [_row("U1", "user", "先生と話したい", _T0)]   # assistant なし
        out = convert(rows, set())
        self.assertEqual(out["threads"][0]["layer"], "rare:silent")

    def test_priority_immediate_over_demoted(self):
        rows = (_pair("U2", "裁判所から封筒が", "裁判所からの書類は放置すると"
                      "不利益が大きい場合があります。", _T0, "緊急対応")
                + _pair("U2", "不安です", PENDING_REPLY_TEXT,
                        _T0 + timedelta(hours=1), "その他判断系"))
        out = convert(rows, set())
        self.assertEqual(out["threads"][0]["layer"], "rare:immediate")  # 単一層のみ

    def test_main_priority_demoted_over_approved_over_auto(self):
        approved = {"先生確認済みの回答です。"}
        rows = (_pair("U3", "a", "通常回答です。", _T0)
                + _pair("U3", "b", "先生確認済みの回答です。",
                        _T0 + timedelta(hours=1), "法的判断・見通し")
                + _pair("U3", "c", PENDING_REPLY_TEXT,
                        _T0 + timedelta(hours=2), "その他判断系"))
        out = convert(rows, approved)
        self.assertEqual(out["threads"][0]["layer"], "main:demoted")
        rows2 = (_pair("U4", "a", "通常回答です。", _T0)
                 + _pair("U4", "b", "先生確認済みの回答です。",
                         _T0 + timedelta(hours=1), "法的判断・見通し"))
        self.assertEqual(convert(rows2, approved)["threads"][0]["layer"],
                         "main:approved")

    def test_delivery_classification(self):
        approved = {"承認済み文面"}
        self.assertEqual(classify_delivery(PENDING_REPLY_TEXT, approved), "demoted")
        self.assertEqual(classify_delivery("承認済み文面", approved), "approved")
        self.assertEqual(
            classify_delivery("至急、弁護士が内容を確認してご連絡します。", approved),
            "immediate")
        self.assertEqual(classify_delivery("通常の回答", approved), "auto")


class TestRedactionAndFailClosed(unittest.TestCase):
    def test_redaction_patterns(self):
        red = redact_text("連絡先は 090-1234-5678、〒123-4567、a@b.co.jp、"
                          "口座 12345678 です")
        for token in ("<電話番号>", "<郵便番号>", "<メール>", "<数字列>"):
            self.assertIn(token, red)
        self.assertEqual(residue_hits(red), [])          # 変換後は残存ゼロ

    def test_over_400_chars_fail_closed(self):
        rows = _pair("U1", "経緯を話します", "了解です。", _T0)
        rows[0]["message"] = "あ" * 401
        out = convert(rows, set())
        self.assertEqual(out["summary"]["picked"], 0)    # 出力ゼロ（部分出力なし）
        self.assertEqual(out["summary"]["excluded_fail_closed"]["text_over_400"], 1)
        self.assertEqual(out["threads"], [])

    def test_category_out_of_enum_fail_closed(self):
        rows = _pair("U1", "q", "a", _T0, category="未知カテゴリ")
        out = convert(rows, set())
        self.assertEqual(out["summary"]["picked"], 0)
        self.assertIn("category_out_of_enum",
                      out["summary"]["excluded_fail_closed"])

    def test_output_is_allowlist_fields_only(self):
        rows = _pair("U1", "こんにちは", "こんにちは。", _T0)
        out = convert(rows, set())
        th = out["threads"][0]
        self.assertEqual(set(th), {"thread_id", "layer", "turns"})
        self.assertEqual(set(th["turns"][0]), {"role", "text"})          # user
        self.assertEqual(set(th["turns"][1]),
                         {"role", "text", "category", "delivery"})       # assistant
        self.assertNotIn("line_user_id", str(out["threads"]))            # 原 ID 非出力
        self.assertTrue(th["thread_id"].startswith("C"))


class TestSummaryAndChecklist(unittest.TestCase):
    def test_summary_counts_and_synthetic_needed(self):
        rows = [_row("U1", "user", "先生と話したい", _T0)]
        out = convert(rows, set())
        s = out["summary"]
        self.assertEqual(s["rare_counts"]["rare:silent"], 1)
        self.assertEqual(s["synthetic_needed"]["rare:silent"], 2)   # 3-1（別集計）
        self.assertEqual(s["synthetic_needed"]["rare:immediate"], 3)
        self.assertEqual(len(out["checklist"]), 1)
        self.assertIn("残存PII目視確認", out["checklist"][0])


class TestFixedTextSyncWithProduction(unittest.TestCase):
    """定型文言の複製 drift 検知（スクリプトは本番モジュール非 import のため、
    同期の担保はこのテストが担う）。"""

    def test_pending_reply_matches_chat_responder(self):
        import chat_responder
        self.assertEqual(PENDING_REPLY_TEXT, chat_responder.PENDING_REPLY)

    def test_immediate_markers_hit_each_production_text(self):
        import chat_responder
        texts = list(chat_responder.IMMEDIATE_NOTICE_TEXTS.values())
        # marker→実定型・実定型→marker の相互被覆（5 定型すべてを識別できる）
        for marker in IMMEDIATE_TEXT_MARKERS:
            self.assertTrue(any(marker in t for t in texts), marker)
        for t in texts:
            self.assertTrue(any(m in t for m in IMMEDIATE_TEXT_MARKERS), t[:30])


if __name__ == "__main__":
    unittest.main()
