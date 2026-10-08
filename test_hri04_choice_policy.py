"""HRI-04｜選択肢外値の扱い（裁定 B・大野決定 2026-09-22）

裁定 B: AI 応答の項目値は、サーバ側で項目別の選択肢に対して検証する。検証前に、選択肢と
同義の表記（例: 続柄「息子」→「子」）を正規化表に基づき正規化する。正規化表に無い選択肢外の
値は、スキーマ逸脱としてその応答全体を AI 失敗として停止する。項目単位の部分採用はしない。
正規化表の追加は大野の裁定事項とする。

実装: hub/intake_choice_synonyms.json（コードから分離・初期案=要承認）→ apply_choice_policy
（AI 応答 → 正規化 → cfg.choices で検証 → 逸脱があれば応答全体を ai_failed=HRI-02 の解放）。
逸脱した項目と値・正規化の前後は App 28 に記録（ログには出さない）。選択肢を持たない
自由記述欄は対象外。AI に渡す選択肢（tool スキーマの enum）と検証の選択肢は同じ cfg.choices。
"""
import json
import logging
import unittest
from unittest.mock import patch

from test_human_reply_intake import (  # noqa: E402
    EVT, NAME, USER, _Base, _ai, _run)

import chat_responder  # noqa: E402
from hub import houki_case_store as store  # noqa: E402
from hub import human_reply_intake as hri  # noqa: E402

RESERVE = f"返答取込:houki:{EVT}"
RELEASE = f"返答取込解放:houki:{EVT}"
DONE = f"返答取込済:houki:{EVT}"
DEVIATION = f"返答取込逸脱:houki:{EVT}"
NORMALIZED = f"返答取込正規化:houki:{EVT}"


def _ai_for(codes, **given):
    items = {c: (False, None, "high") for c in codes}
    for c, v in given.items():
        items[c] = (True, v, "high")
    return _ai(items)


class _ChoiceBase(_Base):
    def setUp(self):
        super().setUp()
        self.fake.add_houki()
        self.codes = list(hri.HOUKI.fields)

    def rows(self, category):
        return [r for r in self.fake.rows["APP_CHATLOG"].values()
                if r["category"]["value"] == category]

    def go(self, event_id=EVT, **given):
        # AI 応答のキー集合は取込が要求する「空欄の欄」と一致させる（レコード 50 の実値）
        rec = self.fake.rows["APP_HOUKI"]["50"]
        codes = [c for c in hri.HOUKI.fields if c in rec and not self.fake.val("APP_HOUKI", "50", c)]
        self.ai.return_value = _ai_for(codes, **given)
        return _run(hri.run_houki(USER, "…", event_id))

    def idem(self):
        """HRI-02 の冪等行（予約/解放/完了）だけ（逸脱・正規化の記録行は含めない）。"""
        return [m for m in self.fake.markers()
                if m.split(":", 1)[0] in ("返答取込", "返答取込解放", "返答取込済")]


class TestChoicePolicy(_ChoiceBase):
    # 受入 1: 「続柄=息子」→ 正規化で「子」となり、応答全体が正常に書き込まれる
    def test_synonym_is_normalized_and_whole_response_written(self):
        self.assertEqual(self.go(続柄="息子", 顧客名=NAME, 被相続人氏名="山田一郎"), "written")
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "続柄"), "子")
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "顧客名"), NAME)
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "被相続人氏名"), "山田一郎")
        (row,) = self.rows(NORMALIZED)                       # 正規化の記録（表を育てる材料）
        self.assertEqual(row["message"]["value"], "（返答取込・正規化）続柄: 息子 → 子")
        self.assertEqual((row["role"]["value"], row["auto_sent"]["value"]), ("user", "no"))
        self.assertEqual(self.rows(DEVIATION), [])
        self.assertEqual(self.idem(), [RESERVE, DONE])
        self.assertIn("登録した欄: ", self.notice())

    # 受入 2: 「続柄=東京都」→ 応答全体が停止し、他項目も書かない。App 28 に逸脱項目と値
    def test_out_of_set_stops_whole_response_and_records_deviation(self):
        self.assertEqual(self.go(続柄="東京都", 顧客名=NAME, 被相続人氏名="山田一郎"), "ai_failed")
        self.assertEqual(self.fake.update_calls, [])
        for code in ("続柄", "顧客名", "被相続人氏名"):
            self.assertEqual(self.fake.val("APP_HOUKI", "50", code), "")
        (row,) = self.rows(DEVIATION)
        self.assertEqual(row["message"]["value"], "（返答取込・逸脱）続柄=東京都")
        text = self.notice()
        self.assertIn("選択肢にない値", text)
        self.assertIn("続柄", text)
        self.assertNotIn("東京都", text)                     # 通知に値は載せない
        self.assertEqual(self.idem(), [RESERVE, RELEASE])   # HRI-02: 解放

    # 受入 3: 正規化表に無い言い換え → 停止（表の未整備は逸脱として扱う）
    def test_unknown_paraphrase_is_deviation(self):
        self.assertEqual(self.go(続柄="嫁の父", 顧客名=NAME), "ai_failed")
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual([r["message"]["value"] for r in self.rows(DEVIATION)],
                         ["（返答取込・逸脱）続柄=嫁の父"])

    def test_multiple_deviations_recorded_together(self):
        self.assertEqual(self.go(続柄="東京都", 相続順位="第4順位", 顧客名=NAME), "ai_failed")
        (row,) = self.rows(DEVIATION)
        self.assertEqual(row["message"]["value"], "（返答取込・逸脱）相続順位=第4順位／続柄=東京都")
        self.assertIn("（App 28 の逸脱行を確認してください）: 相続順位, 続柄", self.notice())

    def test_low_confidence_out_of_set_is_still_deviation(self):
        # 確信度に関わらず、選択肢外はスキーマ逸脱（順序: 正規化→検証→停止）
        self.ai.return_value = _ai({**{c: (False, None, "high") for c in self.codes},
                                    "続柄": (True, "東京都", "low"), "顧客名": (True, NAME, "high")})
        self.assertEqual(_run(hri.run_houki(USER, "…", EVT)), "ai_failed")
        self.assertEqual(self.fake.update_calls, [])

    # 受入 4: 選択肢を持たない自由記述項目は本裁定の対象外で従来どおり
    def test_free_text_fields_are_unaffected(self):
        self.assertEqual(self.go(顧客名=NAME, 知った経緯="市役所からの通知で"), "written")
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "知った経緯"), "市役所からの通知で")
        self.assertEqual(self.rows(DEVIATION), [])
        self.assertEqual(self.rows(NORMALIZED), [])
        # 自由記述の形式不正（URL）は従来どおり欄単位の却下（応答全体は止めない）
        self.notify.reset_mock()
        self.assertEqual(self.go(event_id=EVT + "b", 他の相続人="http://x"), "rejected_only")
        self.assertIn("形式不正のため登録しなかった欄: 他の相続人", self.notice())
        self.assertEqual(self.rows(DEVIATION), [])           # 自由記述の形式不正は逸脱ではない

    def test_canonical_value_passes_without_normalization_row(self):
        self.assertEqual(self.go(続柄="子", 顧客名=NAME), "written")
        self.assertEqual(self.rows(NORMALIZED), [])

    def test_whitespace_is_stripped_before_lookup(self):
        self.assertEqual(self.go(続柄=" 息子 ", 顧客名=NAME), "written")
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "続柄"), "子")

    # 受入 5: 停止後の再配送が HRI-02 の分岐と矛盾しない（解放→再処理・完了なら duplicate）
    def test_redelivery_after_stop_reprocesses(self):
        self.assertEqual(self.go(続柄="東京都", 顧客名=NAME), "ai_failed")
        self.assertEqual(self.go(続柄="息子", 顧客名=NAME), "written")   # 再配送で AI が正しく返した
        self.assertEqual(self.idem(), [RESERVE, RELEASE, RESERVE, DONE])
        self.assertEqual(self.go(続柄="息子", 顧客名=NAME), "duplicate")
        self.assertEqual(len(self.rows(DEVIATION)), 1)
        self.assertEqual(len(self.rows(NORMALIZED)), 1)

    def test_deviation_row_save_failure_still_stops_and_no_value_in_log(self):
        self.fake.fail_categories = {"返答取込逸脱:"}
        with self.assertLogs("hub.human_reply_intake", level="INFO") as cm:
            self.assertEqual(self.go(続柄="東京都", 顧客名=NAME), "ai_failed")
        self.assertEqual(self.fake.update_calls, [])
        joined = "\n".join(cm.output)
        self.assertNotIn("東京都", joined)                   # sink 規律: 値はログに出さない
        self.assertNotIn("続柄", joined)
        self.assertIn("internal row save failed", joined)


class TestRulingB1(_ChoiceBase):
    """裁定 B-1（2026-09-22）: 承認範囲の表と、続柄×相続順位の法定対応の検査。"""

    def test_approved_rows_normalize(self):
        cases = {"続柄": [("長女", "子"), ("奥様", "配偶者"), ("お母様", "直系尊属（父母・祖父母）"),
                          ("両親", "直系尊属（父母・祖父母）"), ("妹", "兄弟姉妹")],
                 "本人区分": [("本人です", "本人"), ("親族（本人が依頼予定）", "親族（本人依頼予定）")],
                 "相続順位": [("第一順位", "子"), ("１", "子"), ("第2順位", "直系尊属"),
                              ("三", "兄弟姉妹")],
                 "同時申述希望": [("希望します", "あり"), ("お願いします", "あり"),
                                  ("不要です", "なし"), ("いいえ", "なし")],
                 "財産処分有無": [("有", "あり"), ("無", "なし")],
                 "訴訟督促有無": [("有り", "あり"), ("無し", "なし")]}
        for code, pairs in cases.items():
            for raw, expected in pairs:
                with self.subTest(code=code, raw=raw):
                    self.assertEqual(hri.normalize_choice(hri.HOUKI, code, raw), expected)

    def test_disapproved_rows_are_absent_and_stop(self):
        # 不承認: 孫・甥・姪・叔父・叔母・いとこ・継子・配偶者の親・その他への寄せ／曖昧な本人区分／
        # 相続順位の続柄からの推測／事実からの推測（処分・督促）
        absent = {"続柄": ["孫娘", "孫息子", "甥", "姪", "甥姪", "叔父", "叔母", "いとこ", "継子",
                          "義父", "義母", "配偶者の親", "祖父", "祖母", "祖父母"],
                  "本人区分": ["家族", "親族", "代理", "代理人"],
                  "相続順位": ["夫", "妻", "息子", "娘", "父", "母", "甥姪", "わからない"],
                  "同時申述希望": ["希望"],
                  "財産処分有無": ["した", "していない", "使った", "解約した", "わからない"],
                  "訴訟督促有無": ["届いた", "届いていない", "催告書が来た", "通知が来た", "わからない"]}
        for code, values in absent.items():
            for raw in values:
                with self.subTest(code=code, raw=raw):
                    self.assertNotIn(raw, hri.CHOICE_SYNONYMS[code])
                    self.assertNotIn(hri.normalize_choice(hri.HOUKI, code, raw), hri.HOUKI.choices[code])
        # 実行時: 不承認の言い換えは停止（人が見る）
        self.assertEqual(self.go(続柄="孫娘", 顧客名=NAME), "ai_failed")
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual([r["message"]["value"] for r in self.rows(DEVIATION)],
                         ["（返答取込・逸脱）続柄=孫娘"])

    def test_rank_mismatch_stops_whole_response(self):
        self.assertEqual(self.go(続柄="息子", 相続順位="第2順位", 顧客名=NAME), "ai_failed")
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(self.fake.val("APP_HOUKI", "50", "顧客名"), "")
        (row,) = self.rows(DEVIATION)
        self.assertEqual(row["message"]["value"],
                         "（返答取込・逸脱）続柄／相続順位=子／直系尊属（法定の対応と不一致）")
        # 正規化は起きた（息子→子・第2順位→直系尊属）ので、その記録は残る
        self.assertEqual(sorted(r["message"]["value"] for r in self.rows(NORMALIZED)),
                         ["（返答取込・正規化）相続順位: 第2順位 → 直系尊属",
                          "（返答取込・正規化）続柄: 息子 → 子"])
        text = self.notice()
        self.assertIn("続柄／相続順位", text)
        self.assertNotIn("直系尊属", text)                   # 通知に値は載せない
        self.assertEqual(self.idem(), [RESERVE, RELEASE])   # 解放=再配送で再処理

    def test_rank_consistent_pairs_pass(self):
        pairs = [("子", "子"), ("父", "直系尊属"), ("兄弟姉妹", "第3順位"), ("妻", "配偶者")]
        for i, (rel, rank) in enumerate(pairs):
            with self.subTest(rel=rel, rank=rank):
                self.setUp()
                self.assertEqual(self.go(event_id=EVT + str(i), 続柄=rel, 相続順位=rank), "written")
                self.assertEqual(self.rows(DEVIATION), [])

    def test_rank_check_skipped_when_only_one_side(self):
        self.assertEqual(self.go(続柄="息子", 顧客名=NAME), "written")
        self.assertEqual(self.rows(DEVIATION), [])
        self.setUp()
        self.assertEqual(self.go(相続順位="第3順位", 顧客名=NAME), "written")
        self.assertEqual(self.rows(DEVIATION), [])
        # 既存レコードに続柄があり、応答には相続順位だけ → 応答内で両方でないので検査しない
        self.setUp()
        self.fake.rows["APP_HOUKI"]["50"]["続柄"] = {"value": "子"}
        self.assertEqual(self.go(相続順位="第3順位", 顧客名=NAME), "written")
        self.assertEqual(self.rows(DEVIATION), [])

    def test_rank_check_skipped_for_unmapped_values(self):
        # 続柄「その他」「孫」や相続順位「不明」「甥姪（代襲）」は法定対応の表に無い=検査しない
        self.assertEqual(self.go(続柄="その他", 相続順位="不明"), "written")
        self.setUp()
        self.assertEqual(self.go(続柄="孫", 相続順位="子"), "written")
        self.setUp()
        self.assertEqual(self.go(続柄="子", 相続順位="不明"), "written")
        self.assertEqual(self.rows(DEVIATION), [])

    def test_rank_mismatch_unit(self):
        f = hri._rank_mismatch
        mk = lambda rel, rank, a=True: {"続柄": {"answered": a, "value": rel},
                                        "相続順位": {"answered": a, "value": rank}}
        self.assertIsNone(f(mk("子", "子")))
        self.assertEqual(f(mk("配偶者", "子")), ("配偶者", "子"))
        self.assertEqual(f(mk("子", "配偶者")), ("子", "配偶者"))
        self.assertIsNone(f(mk("子", "子", a=False)))
        self.assertIsNone(f({"続柄": {"answered": True, "value": "子"}}))
        self.assertEqual(hri.RANK_BY_RELATION, {"子": "子", "直系尊属（父母・祖父母）": "直系尊属",
                                                "兄弟姉妹": "兄弟姉妹", "配偶者": "配偶者"})


class TestSingleSourceOfChoices(unittest.TestCase):
    def test_choice_fields_enumerated_from_one_definition(self):
        # AI に渡す選択肢（tool スキーマの enum）と検証の選択肢は同じ cfg.choices
        expected = {c: v for c, v in store.HEARING_CHOICE_FIELDS.items()
                    if c in hri.HOUKI.fields}
        self.assertEqual(hri.HOUKI.choices, expected)
        self.assertEqual(sorted(hri.HOUKI.choices),
                         ["同時申述希望", "本人区分", "相続順位", "続柄", "訴訟督促有無", "財産処分有無"])
        tool = hri.build_tool(hri.HOUKI, list(hri.HOUKI.fields))
        props = tool["input_schema"]["properties"]["items"]["properties"]
        for code, choices in hri.HOUKI.choices.items():
            self.assertEqual(props[code]["properties"]["value"]["enum"], list(choices) + [None])
        self.assertEqual(hri.JIKOU.choices, {})               # 時効の取込欄はすべて自由記述

    def test_synonym_table_is_consistent_with_choices(self):
        with open(hri.SYNONYMS_PATH, encoding="utf-8") as f:
            raw = json.load(f)
        self.assertEqual(raw["_meta"]["status"], "承認済み（裁定B-1・2026-09-22）")
        table = hri.CHOICE_SYNONYMS
        self.assertEqual(set(table), set(hri.HOUKI.choices))  # 欄=選択肢を持つ 6 欄と一致
        for code, syn in table.items():
            choices = set(hri.HOUKI.choices[code])
            for k, v in syn.items():
                self.assertIn(v, choices, (code, k, v))       # 変換先は選択肢の値
                self.assertNotIn(k, choices, (code, k))       # 選択肢の値そのものは書かない
                self.assertEqual(k, k.strip())

    def test_apply_choice_policy_unit(self):
        codes = ["続柄", "顧客名"]
        report = {"items": {"続柄": {"answered": True, "value": "娘", "confidence": "high"},
                            "顧客名": {"answered": True, "value": "娘", "confidence": "high"}},
                  "mixed_persons": False}
        out, normalized, deviations = hri.apply_choice_policy(hri.HOUKI, report, codes)
        self.assertEqual(out["items"]["続柄"]["value"], "子")
        self.assertEqual(out["items"]["顧客名"]["value"], "娘")   # 自由記述は触らない
        self.assertEqual(normalized, [("続柄", "娘", "子")])
        self.assertEqual(deviations, [])
        report["items"]["続柄"]["value"] = "東京都"
        _out, normalized, deviations = hri.apply_choice_policy(hri.HOUKI, report, codes)
        self.assertEqual(deviations, [("続柄", "東京都")])
        report["items"]["続柄"] = {"answered": False, "value": None, "confidence": "high"}
        _out, normalized, deviations = hri.apply_choice_policy(hri.HOUKI, report, codes)
        self.assertEqual((normalized, deviations), ([], []))

    def test_malformed_table_is_rejected(self):
        import os
        import tempfile
        d = tempfile.mkdtemp()
        p = os.path.join(d, "bad.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"続柄": {"息子": 1}}, f)
        with self.assertRaises(ValueError):
            hri.load_choice_synonyms(p)

    def test_internal_rows_hidden_from_history(self):
        records = [
            {"role": {"value": "user"}, "message": {"value": "（返答取込・逸脱）続柄=東京都"}},
            {"role": {"value": "user"}, "message": {"value": "（返答取込・正規化）続柄: 息子 → 子"}},
            {"role": {"value": "user"}, "message": {"value": hri.INTAKE_MARKER}},
            {"role": {"value": "assistant"}, "message": {"value": "返信"}},
            {"role": {"value": "user"}, "message": {"value": "こんにちは"}},
        ]

        class _Resp:
            is_success = True

            def json(self):
                return {"records": records}

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, *a, **k):
                return _Resp()
        with patch.object(chat_responder.httpx, "AsyncClient", lambda *a, **k: _Client()), \
             patch.object(chat_responder, "APP_CHATLOG", "28"), \
             patch.object(chat_responder, "TOKEN_CHATLOG", "d"):
            history = _run(chat_responder.get_recent_chat_history(USER))
        self.assertEqual([h["content"] for h in history], ["こんにちは", "返信"])
        self.assertTrue(hri.is_internal_row(hri.INTAKE_MARKER))
        self.assertFalse(hri.is_internal_row("（返答取込）と書きました"))


if __name__ == "__main__":
    unittest.main()
