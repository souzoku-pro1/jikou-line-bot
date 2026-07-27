"""LINE-LOG-1 準備票: tools/line_log_anonymize の合成データ試験（fix1・実ログ非接触）。

固定する仕様（DRAFT_LINE_QUALITY §4.1＋手順書）:
- 一方向工程（H04）: convert=中間＋summary 別ファイル（checklist なし）→
  reverify=全件再検査・PASS 時のみ最終成果物＋checklist（FAIL は非0・生成なし）
- role enum 検証（H01）: user/assistant 以外はスレッド出力ゼロ＋固定理由計数
- delivery は正規化後全文一致が一次判定（M01・marker 部分一致廃止）＋本番定型との
  同期・一意性・negative
- fallback 補正は構造化入力で a>b>c 再適用・二重計上なし（H03）
- main_shortfall の数値出力（M02）
"""

import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from tools.line_log_anonymize import (
    IMMEDIATE_TEXTS,
    MAIN_CAP,
    PENDING_REPLY_TEXT,
    apply_fallback_corrections,
    classify_delivery,
    convert,
    main,
    normalize_text,
    redact_text,
    residue_hits,
    reverify,
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
                + _pair("U1", "その後どうですか", "確認します。",
                        _T0 + timedelta(hours=100)))
        self.assertEqual(len(split_threads(rows)["U1"]), 2)
        out = convert(rows, set())
        self.assertEqual(out["summary"]["picked"], 1)
        self.assertIn("その後どうですか", out["threads"][0]["turns"][0]["text"])

    def test_within_72h_stays_one_thread(self):
        rows = (_pair("U1", "質問です", "はい。", _T0)
                + _pair("U1", "続きです", "どうぞ。", _T0 + timedelta(hours=71)))
        self.assertEqual(len(split_threads(rows)["U1"]), 1)


class TestRoleEnumFailClosed(unittest.TestCase):
    """fix1 H01: role の enum 検証（user/assistant 以外は fail-closed）。"""

    def test_bad_user_side_role_excluded(self):
        rows = _pair("U1", "こんにちは", "こんにちは。", _T0)
        rows[0]["role"] = "customer"             # user 側の異常形
        out = convert(rows, set())
        self.assertEqual(out["summary"]["picked"], 0)
        self.assertEqual(out["summary"]["excluded_fail_closed"]["role_out_of_enum"], 1)
        self.assertEqual(out["threads"], [])

    def test_bad_assistant_side_role_excluded(self):
        rows = _pair("U1", "こんにちは", "こんにちは。", _T0)
        rows[1]["role"] = "bot"                  # assistant 側の異常形
        out = convert(rows, set())
        self.assertEqual(out["summary"]["picked"], 0)
        self.assertEqual(out["summary"]["excluded_fail_closed"]["role_out_of_enum"], 1)


class TestDeliveryFullMatch(unittest.TestCase):
    """fix1 M01: 正規化後全文一致が一次判定・本番定型との同期・negative。"""

    def test_full_match_primary(self):
        approved = {normalize_text("承認済み文面")}
        self.assertEqual(classify_delivery(PENDING_REPLY_TEXT, approved), "demoted")
        # 改行→空白の揺れは正規化で吸収（全文一致は維持）
        self.assertEqual(
            classify_delivery(PENDING_REPLY_TEXT.replace("\n", " "), approved),
            "demoted")
        for text in IMMEDIATE_TEXTS.values():
            self.assertEqual(classify_delivery(text, approved), "immediate")
        self.assertEqual(classify_delivery("承認済み文面", approved), "approved")
        self.assertEqual(classify_delivery("通常の回答", approved), "auto")

    def test_negative_marker_phrase_inside_normal_reply_is_auto(self):
        # 定型の特徴句を含む「通常回答」は immediate にならない（全文一致のみ）
        text = ("ご質問ありがとうございます。至急、弁護士が内容を確認して"
                "ご連絡しますが、その前に1点だけ教えてください。")
        self.assertEqual(classify_delivery(text, set()), "auto")
        text2 = "裁判所からの書類は放置すると不利益が大きい場合があります。"
        self.assertEqual(classify_delivery(text2, set()), "auto")   # 部分文のみ

    def test_sync_with_production_texts(self):
        import chat_responder
        self.assertEqual(PENDING_REPLY_TEXT, chat_responder.PENDING_REPLY)
        self.assertEqual(IMMEDIATE_TEXTS, chat_responder.IMMEDIATE_NOTICE_TEXTS)

    def test_immediate_texts_unique_after_normalize(self):
        norms = [normalize_text(v) for v in IMMEDIATE_TEXTS.values()]
        self.assertEqual(len(set(norms)), len(norms))   # 各定型ちょうど1件に一致


class TestFallbackCorrections(unittest.TestCase):
    """fix1 H03→fix2 H01: 補正対象の機械強制（main:demoted＋PENDING 縮退発話のみ）。"""

    def _mk(self, tid, layer, demoted_turn=False):
        turns = [{"role": "user", "text": "x"}]
        if demoted_turn:
            turns.append({"role": "assistant", "text": "y",
                          "category": "その他判断系", "delivery": "demoted"})
        return {"thread_id": tid, "layer": layer, "turns": turns}

    def test_demoted_reassigned_and_counts_recomputed(self):
        threads = [self._mk("C001", "main:demoted", demoted_turn=True),
                   self._mk("C002", "main:demoted", demoted_turn=True),
                   self._mk("C003", "rare:silent")]
        errors = apply_fallback_corrections(threads, {"C001"})
        self.assertEqual(errors, [])
        from tools.line_log_anonymize import compute_summary
        s = compute_summary(threads, {})
        self.assertEqual(s["rare_counts"]["rare:fallback"], 1)   # 実例数の数値出力
        self.assertEqual(s["main_counts"]["main:demoted"], 1)    # 二重計上なし
        self.assertEqual(s["synthetic_needed"]["rare:fallback"], 2)

    def test_non_demoted_targets_rejected(self):
        # fix2 H01: main:auto／main:approved／希少層への指定はエラー（FAIL 化）
        threads = [self._mk("C001", "main:auto"),
                   self._mk("C002", "main:approved"),
                   self._mk("C003", "rare:silent"),
                   self._mk("C004", "main:demoted")]   # demoted 発話なしも対象外
        for fid in ("C001", "C002", "C003", "C004"):
            with self.subTest(target=fid):
                errors = apply_fallback_corrections(
                    [dict(t, turns=list(t["turns"])) for t in threads], {fid})
                self.assertEqual(len(errors), 1, errors)
                self.assertIn("補正対象外", errors[0])

    def test_unknown_id_reported(self):
        threads = [self._mk("C001", "main:auto")]
        errors = apply_fallback_corrections(threads, {"C999"})
        self.assertEqual(len(errors), 1)
        self.assertIn("corpus に無い", errors[0])

    def test_malformed_correction_id_not_echoed(self):
        # grammar 不一致の補正 ID は値を echo しない（固定 reason のみ）
        threads = [self._mk("C001", "main:auto")]
        errors = apply_fallback_corrections(threads, {"090-1234-5678"})
        self.assertEqual(len(errors), 1)
        self.assertNotIn("090", errors[0])
        self.assertIn("grammar 不一致", errors[0])


class TestRedactionAndFailClosed(unittest.TestCase):
    def test_redaction_patterns(self):
        red = redact_text("連絡先は 090-1234-5678、〒123-4567、a@b.co.jp、"
                          "口座 12345678 です")
        for token in ("<電話番号>", "<郵便番号>", "<メール>", "<数字列>"):
            self.assertIn(token, red)
        self.assertEqual(residue_hits(red), [])

    def test_over_400_chars_fail_closed(self):
        rows = _pair("U1", "経緯を話します", "了解です。", _T0)
        rows[0]["message"] = "あ" * 401
        out = convert(rows, set())
        self.assertEqual(out["summary"]["picked"], 0)
        self.assertEqual(out["summary"]["excluded_fail_closed"]["text_over_400"], 1)

    def test_category_out_of_enum_fail_closed(self):
        rows = _pair("U1", "q", "a", _T0, category="未知カテゴリ")
        out = convert(rows, set())
        self.assertIn("category_out_of_enum", out["summary"]["excluded_fail_closed"])


class TestSummaryShape(unittest.TestCase):
    def test_main_shortfall_and_synthetic_needed(self):
        rows = (_pair("U1", "a", "回答1。", _T0)
                + [_row("U2", "user", "先生と話したい", _T0)])
        out = convert(rows, set())
        s = out["summary"]
        self.assertEqual(s["main_counts"]["main:auto"], 1)
        self.assertEqual(s["main_shortfall"]["main:auto"], MAIN_CAP - 1)   # M02
        self.assertEqual(s["main_shortfall"]["main:demoted"], MAIN_CAP)
        self.assertEqual(s["rare_counts"]["rare:fallback"], 0)   # 数値（文字列でない）
        self.assertEqual(s["synthetic_needed"]["rare:fallback"], 3)


class TestOneWayPipeline(unittest.TestCase):
    """fix1 H04/H02: convert=中間＋summary 別ファイル・checklist なし →
    reverify=全件再検査・PASS 時のみ最終＋checklist。"""

    def _tmp(self):
        import tempfile
        return tempfile.mkdtemp(prefix="linelog_")

    def _write_csv(self, path):
        import csv
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=[
                "作成日時", "line_user_id", "role", "message",
                "category", "auto_sent"])
            w.writeheader()
            w.writerow({"作成日時": "2026-07-01 10:00", "line_user_id": "U1",
                        "role": "user", "message": "こんにちは",
                        "category": "", "auto_sent": ""})
            w.writerow({"作成日時": "2026-07-01 10:01", "line_user_id": "U1",
                        "role": "assistant", "message": "こんにちは。",
                        "category": "挨拶・雑談", "auto_sent": "yes"})

    def test_convert_outputs_corpus_and_separate_summary_no_checklist(self):
        d = self._tmp()
        self._write_csv(f"{d}/a28.csv")
        rc = main(["convert", "--app28-csv", f"{d}/a28.csv",
                   "--out", f"{d}/mid.json", "--summary-out", f"{d}/sum.json"])
        self.assertEqual(rc, 0)
        corpus = json.loads(Path(f"{d}/mid.json").read_text(encoding="utf-8"))
        self.assertEqual(set(corpus), {"threads"})       # H02: allowlist のみ
        summary = json.loads(Path(f"{d}/sum.json").read_text(encoding="utf-8"))
        self.assertIn("main_shortfall", summary)          # 運用メタは別ファイル
        self.assertFalse(list(Path(d).glob("*checklist*")))   # H04: 中間では生成しない

    def test_reverify_pass_emits_final_and_checklist(self):
        d = self._tmp()
        self._write_csv(f"{d}/a28.csv")
        main(["convert", "--app28-csv", f"{d}/a28.csv",
              "--out", f"{d}/mid.json", "--summary-out", f"{d}/s1.json"])
        rc = main(["reverify", "--in", f"{d}/mid.json",
                   "--out", f"{d}/final.json", "--summary-out", f"{d}/s2.json",
                   "--checklist-out", f"{d}/check.csv"])
        self.assertEqual(rc, 0)
        self.assertTrue(Path(f"{d}/final.json").exists())
        self.assertTrue(Path(f"{d}/check.csv").exists())

    def test_reverify_fail_closed_on_tampered_intermediate(self):
        d = self._tmp()
        cases = {
            "extra_field": {"threads": [{"thread_id": "C001", "layer": "main:auto",
                                         "extra": 1, "turns": []}]},
            "over_400": {"threads": [{"thread_id": "C001", "layer": "main:auto",
                                      "turns": [{"role": "user",
                                                 "text": "あ" * 401}]}]},
            "bad_delivery": {"threads": [{"thread_id": "C001", "layer": "main:auto",
                                          "turns": [{"role": "assistant",
                                                     "text": "x",
                                                     "category": "挨拶・雑談",
                                                     "delivery": "fallback"}]}]},
            "residue": {"threads": [{"thread_id": "C001", "layer": "main:auto",
                                     "turns": [{"role": "user",
                                                "text": "090-1234-5678"}]}]},
            "bad_role": {"threads": [{"thread_id": "C001", "layer": "main:auto",
                                      "turns": [{"role": "operator",
                                                 "text": "x"}]}]},
        }
        for label, doc in cases.items():
            with self.subTest(case=label):
                p = f"{d}/{label}.json"
                Path(p).write_text(json.dumps(doc, ensure_ascii=False),
                                   encoding="utf-8")
                rc = main(["reverify", "--in", p,
                           "--out", f"{d}/{label}_final.json",
                           "--summary-out", f"{d}/{label}_s.json",
                           "--checklist-out", f"{d}/{label}_c.csv"])
                self.assertEqual(rc, 1)                          # 非0終了
                self.assertFalse(Path(f"{d}/{label}_c.csv").exists())  # 生成不可
                self.assertFalse(Path(f"{d}/{label}_final.json").exists())

    def test_reverify_applies_fallback_ids(self):
        d = self._tmp()
        doc = {"threads": [
            {"thread_id": "C001", "layer": "main:demoted",
             "turns": [{"role": "user", "text": "不安です"},
                       {"role": "assistant", "text": "確認します。",
                        "category": "その他判断系", "delivery": "demoted"}]}]}
        Path(f"{d}/mid.json").write_text(json.dumps(doc, ensure_ascii=False),
                                         encoding="utf-8")
        Path(f"{d}/fb.json").write_text(json.dumps(["C001"]), encoding="utf-8")
        rc = main(["reverify", "--in", f"{d}/mid.json",
                   "--fallback-ids", f"{d}/fb.json",
                   "--out", f"{d}/final.json", "--summary-out", f"{d}/s.json",
                   "--checklist-out", f"{d}/c.csv"])
        self.assertEqual(rc, 0)
        s = json.loads(Path(f"{d}/s.json").read_text(encoding="utf-8"))
        self.assertEqual(s["rare_counts"]["rare:fallback"], 1)
        self.assertEqual(s["main_counts"]["main:demoted"], 0)    # 二重計上なし

    def test_reverify_unknown_fallback_id_fails(self):
        _errors, final = reverify({"threads": []}, {"C999"})
        self.assertIsNone(final)

    def test_reverify_thread_id_tampering_not_reflected(self):
        """fix2 H02: thread_id 改竄4種は index+固定 reason のみ・値を出力へ非反射。"""
        import io
        d = self._tmp()
        cases = {
            "pii_id": "090-1234-5678",
            "too_long": "C123456789",
            "bad_form": "X001",
        }
        for label, bad in cases.items():
            with self.subTest(case=label):
                doc = {"threads": [{"thread_id": bad, "layer": "main:auto",
                                    "turns": [{"role": "user", "text": "x"}]}]}
                p = f"{d}/{label}.json"
                Path(p).write_text(json.dumps(doc, ensure_ascii=False),
                                   encoding="utf-8")
                buf = io.StringIO()
                rc = main(["reverify", "--in", p,
                           "--out", f"{d}/{label}_f.json",
                           "--summary-out", f"{d}/{label}_s.json",
                           "--checklist-out", f"{d}/{label}_c.csv"], out=buf)
                self.assertEqual(rc, 1)
                self.assertNotIn(bad, buf.getvalue())            # 値の非反射
                self.assertIn("threads[0]", buf.getvalue())      # index+固定 reason
                self.assertFalse(Path(f"{d}/{label}_c.csv").exists())
        with self.subTest(case="non_string"):
            doc = {"threads": [{"thread_id": 12345, "layer": "main:auto",
                                "turns": [{"role": "user", "text": "x"}]}]}
            p = f"{d}/nonstr.json"
            Path(p).write_text(json.dumps(doc), encoding="utf-8")
            buf = io.StringIO()
            rc = main(["reverify", "--in", p, "--out", f"{d}/n_f.json",
                       "--summary-out", f"{d}/n_s.json",
                       "--checklist-out", f"{d}/n_c.csv"], out=buf)
            self.assertEqual(rc, 1)
            self.assertNotIn("12345", buf.getvalue())
            self.assertFalse(Path(f"{d}/n_c.csv").exists())

    def test_reverify_refuses_existing_outputs(self):
        """fix2 H03(i): 既存成果物あり→開始前拒否・旧成果物は不変（PASS 外観なし）。"""
        import io
        d = self._tmp()
        self._write_csv(f"{d}/a28.csv")
        main(["convert", "--app28-csv", f"{d}/a28.csv",
              "--out", f"{d}/mid.json", "--summary-out", f"{d}/s1.json"])
        # 1回目: 正常に最終成果物を生成
        main(["reverify", "--in", f"{d}/mid.json", "--out", f"{d}/final.json",
              "--summary-out", f"{d}/s2.json", "--checklist-out", f"{d}/c.csv"])
        old_final = Path(f"{d}/final.json").read_text(encoding="utf-8")
        # 2回目: 検証 FAIL する入力でも、既存成果物があるため開始前拒否
        bad = {"threads": [{"thread_id": "C001", "layer": "main:auto",
                            "turns": [{"role": "user", "text": "あ" * 401}]}]}
        Path(f"{d}/bad.json").write_text(json.dumps(bad, ensure_ascii=False),
                                         encoding="utf-8")
        buf = io.StringIO()
        rc = main(["reverify", "--in", f"{d}/bad.json", "--out", f"{d}/final.json",
                   "--summary-out", f"{d}/s2.json",
                   "--checklist-out", f"{d}/c.csv"], out=buf)
        self.assertEqual(rc, 1)
        self.assertIn("既存の成果物", buf.getvalue())
        self.assertEqual(Path(f"{d}/final.json").read_text(encoding="utf-8"),
                         old_final)                              # 旧成果物は不変

    def test_reverify_checklist_write_failure_leaves_no_partials(self):
        """fix2 H03(ii): checklist 書込み失敗時に部分成果物・一時ファイルを残さない。"""
        import io
        import os
        d = self._tmp()
        self._write_csv(f"{d}/a28.csv")
        main(["convert", "--app28-csv", f"{d}/a28.csv",
              "--out", f"{d}/mid.json", "--summary-out", f"{d}/s1.json"])
        os.makedirs(f"{d}/c.csv.tmp")            # checklist の一時書込み先を潰す
        buf = io.StringIO()
        rc = main(["reverify", "--in", f"{d}/mid.json", "--out", f"{d}/final.json",
                   "--summary-out", f"{d}/s2.json",
                   "--checklist-out", f"{d}/c.csv"], out=buf)
        self.assertEqual(rc, 1)
        self.assertIn("書込みに失敗", buf.getvalue())
        self.assertFalse(Path(f"{d}/final.json").exists())       # 部分成果物なし
        self.assertFalse(Path(f"{d}/s2.json").exists())
        self.assertFalse([x for x in Path(d).glob("*.tmp") if x.is_file()])  # 一時ファイルなし（fixture の dir は除く）


if __name__ == "__main__":
    unittest.main()
