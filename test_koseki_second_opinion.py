"""koseki_second_opinion.py（R5-1 セカンドオピニオン層）のテスト

検証:
発動条件（全体確信度/フィールド単位・閾値 env・高確信度の非発動・実施済みの
再発動なし=1書類1回）・フラグ無効の完全不発・比較用カノニカライズ（大字和暦⇔
算用の一致・小書き仮名は別文字のまま）・一致時の確信度引き上げ（値は不変・
max(現値,0.95)）・不一致時の両論保持と要目視マーク（一次値の保持・App 33
既存フィールド不変=読解JSON内ブロックのみ）・日付の形式所見の注記・
人物対応付け（正規化氏名・対応なし人物の記録）・process_record への結線
（発動/非発動・原本PDF取得・再読解失敗の縮退）・既存R3経路の無影響。
kintone / Claude は全てモック。
"""

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")

import koseki_reader  # noqa: E402
import koseki_second_opinion as so  # noqa: E402
from koseki_second_opinion import (  # noqa: E402
    _canon, merge_second_opinion, needs_second_opinion,
)

if os.environ.get("ANTHROPIC_API_KEY") == "dummy_key_for_import_only":
    del os.environ["ANTHROPIC_API_KEY"]

_ENV = {"SECOND_OPINION_ENABLED": "1",
        "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33",
        "KOSEKI_READER_DISABLED": "0"}


def run(coro):
    return asyncio.run(coro)


def reading(*, form_conf=0.9, honseki_conf=0.9, name_conf=0.9,
            birth="昭和拾參年參月弐拾弐日", name="鈴木縫次郎",
            aite="内田チヨ子"):
    return {
        "様式": "改製原（昭和）", "様式confidence": form_conf,
        "戸籍": {"本籍": "東京都足立区鹿浜三丁目", "筆頭者": name,
                 "編製日": "", "従前戸籍": {"本籍": "北鹿浜町千二百六十一番地",
                                            "筆頭者": "鈴木金太郎"},
                 "confidence": {"本籍": honseki_conf, "筆頭者": 0.9}},
        "人物": [{"氏名": name, "続柄": "夫", "生年月日": birth,
                  "身分事項": [{"種別": "婚姻",
                                "日付": "昭和参拾五年拾壱月弐拾弐日",
                                "相手方": aite, "confidence": 0.8}],
                  "confidence": {"氏名": name_conf, "生年月日": 0.8}}],
    }


def second(*, name="鈴木金次", birth="昭和13年3月22日", aite="内田チョ子"):
    return {
        "様式": "改製原（昭和）", "様式confidence": 0.9,
        "戸籍": {"本籍": "東京都足立区鹿浜三丁目", "筆頭者": name,
                 "従前戸籍": {"本籍": "北鹿浜町1261番地",
                              "筆頭者": "鈴木金太郎"},
                 "confidence": {"本籍": 0.9}},
        "人物": [{"氏名": name, "続柄": "夫", "生年月日": birth,
                  "身分事項": [{"種別": "婚姻", "日付": "昭和35年11月22日",
                                "相手方": aite, "confidence": 0.9}],
                  "confidence": {"氏名": 0.9}}],
    }


class TestCanon(unittest.TestCase):
    def test_daiji_wareki_equals_arabic(self):
        self.assertEqual(_canon("昭和拾參年參月弐拾弐日"), "昭和13年3月22日")
        self.assertEqual(_canon("昭和参拾五年拾壱月弐拾弐日"), "昭和35年11月22日")
        self.assertEqual(_canon("北鹿浜町千二百六十一番地"), "北鹿浜町1261番地")
        self.assertEqual(_canon("昭和13年3月22日"), "昭和13年3月22日")

    def test_small_kana_stays_distinct(self):
        """小書き仮名は変換しない（チョ子/チヨ子は不一致として人に見せる）"""
        self.assertNotEqual(_canon("内田チョ子"), _canon("内田チヨ子"))

    def test_space_and_width_normalized(self):
        self.assertEqual(_canon("鈴木　金次"), _canon("鈴木 金次"))


class TestTrigger(unittest.TestCase):
    def test_low_overall_triggers(self):
        with patch.dict(os.environ, _ENV):
            self.assertTrue(needs_second_opinion(
                reading(form_conf=0.5, honseki_conf=0.5, name_conf=0.5)))

    def test_low_single_field_triggers_even_with_high_overall(self):
        """フィールド単位: 1つでも閾値未満なら発動（全体が高くても）"""
        r = reading(form_conf=0.99, honseki_conf=0.99, name_conf=0.4)
        r["人物"][0]["身分事項"][0]["confidence"] = 0.99
        r["人物"][0]["confidence"]["生年月日"] = 0.99
        with patch.dict(os.environ, _ENV):
            self.assertTrue(needs_second_opinion(r))

    def test_high_confidence_does_not_trigger(self):
        r = reading(form_conf=0.95, honseki_conf=0.95, name_conf=0.95)
        r["人物"][0]["身分事項"][0]["confidence"] = 0.95
        r["人物"][0]["confidence"] = {"氏名": 0.95, "生年月日": 0.95}
        r["戸籍"]["confidence"] = {"本籍": 0.95, "筆頭者": 0.95}
        with patch.dict(os.environ, _ENV):
            self.assertFalse(needs_second_opinion(r))

    def test_threshold_env_override(self):
        r = reading(form_conf=0.8, honseki_conf=0.8, name_conf=0.8)
        r["人物"][0]["身分事項"][0]["confidence"] = 0.8
        r["人物"][0]["confidence"] = {"氏名": 0.8, "生年月日": 0.8}
        r["戸籍"]["confidence"] = {"本籍": 0.8, "筆頭者": 0.8}
        with patch.dict(os.environ, {**_ENV, "SECOND_OPINION_THRESHOLD": "0.7"}):
            self.assertFalse(needs_second_opinion(r))
        with patch.dict(os.environ, _ENV):  # 既定 0.85
            self.assertTrue(needs_second_opinion(r))

    def test_already_done_never_retriggers(self):
        """1書類1回: セカンドオピニオン実施済みの読解には再発動しない"""
        r = reading(form_conf=0.1)
        r[so.SO_KEY] = {"実施": True}
        with patch.dict(os.environ, _ENV):
            self.assertFalse(needs_second_opinion(r))


class TestMerge(unittest.TestCase):
    def test_agreement_raises_confidence_value_unchanged(self):
        """一致（大字⇔算用の表記差込み）→ 確信度 max(現値,0.95)・値は原文のまま"""
        first = reading(name="鈴木金次")
        merged = merge_second_opinion(first, second())
        # 本籍一致 → 0.9 -> 0.95
        self.assertEqual(merged["戸籍"]["confidence"]["本籍"], 0.95)
        # 生年月日: 大字 vs 算用でも canon 一致 → 引き上げ・原文は大字のまま
        self.assertEqual(merged["人物"][0]["confidence"]["生年月日"], 0.95)
        self.assertEqual(merged["人物"][0]["生年月日"], "昭和拾參年參月弐拾弐日")
        # 様式一致
        self.assertEqual(merged["様式confidence"], 0.95)

    def test_confidence_not_lowered_when_already_higher(self):
        first = reading(name="鈴木金次")
        first["戸籍"]["confidence"]["本籍"] = 0.99
        merged = merge_second_opinion(first, second())
        self.assertEqual(merged["戸籍"]["confidence"]["本籍"], 0.99,
                         "max(現値, 0.95)＝下げない")

    def test_mismatch_keeps_first_and_marks_for_review(self):
        """不一致（縫次郎/金次・チョ子/チヨ子）→ 一次値保持＋両論記録＋要目視"""
        merged = merge_second_opinion(reading(), second())
        block = merged[so.SO_KEY]
        self.assertTrue(block["実施"])
        self.assertTrue(block["要目視"])
        targets = {m["対象"]: m for m in block["不一致"]}
        self.assertIn("戸籍.筆頭者", targets)
        self.assertEqual(targets["戸籍.筆頭者"]["一次"], "鈴木縫次郎")
        self.assertEqual(targets["戸籍.筆頭者"]["再読解"], "鈴木金次")
        # 一次読解の値は変わらない（両論併記・機械はどちらも正としない）
        self.assertEqual(merged["戸籍"]["筆頭者"], "鈴木縫次郎")
        # 人物は氏名不一致で対応が取れない → その旨を記録
        self.assertIn("人物[鈴木縫次郎]", targets)
        self.assertIn("対応する人物がいません", targets["人物[鈴木縫次郎]"]["所見"])

    def test_small_kana_mismatch_recorded(self):
        """チョ子/チヨ子: canon では同一化されず不一致として記録される"""
        merged = merge_second_opinion(reading(name="鈴木金次"),
                                      second(aite="内田チョ子"))
        targets = [m["対象"] for m in merged[so.SO_KEY]["不一致"]]
        self.assertIn("人物[鈴木金次].身分事項[婚姻#1].相手方", targets)
        # 相手方不一致の事項は confidence を引き上げない
        self.assertEqual(merged["人物"][0]["身分事項"][0]["confidence"], 0.8)

    def test_event_agreement_raises_event_confidence(self):
        merged = merge_second_opinion(reading(aite="内田チョ子"),
                                      second(name="鈴木縫次郎"))
        self.assertEqual(merged["人物"][0]["身分事項"][0]["confidence"], 0.95,
                         "日付（大字⇔算用）・相手方とも一致 → 引き上げ")

    def test_date_format_annotation(self):
        """片方のみ日付として形式適正な不一致は形式所見を注記（値は自動採用しない）"""
        first = reading(name="鈴木金次", birth="昭和拾六年拾弐月四")  # 「日」欠け=不正
        merged = merge_second_opinion(first, second())
        entry = next(m for m in merged[so.SO_KEY]["不一致"]
                     if m["対象"] == "人物[鈴木金次].生年月日")
        self.assertEqual(entry["形式所見"], "一次=不正/再読解=適正")
        self.assertEqual(merged["人物"][0]["生年月日"], "昭和拾六年拾弐月四",
                         "形式不正でも一次値を自動置換しない")

    def test_extra_person_in_second_recorded(self):
        s = second()
        s["人物"].append({"氏名": "鈴木 誠", "続柄": "長男"})
        merged = merge_second_opinion(reading(name="鈴木金次"), s)
        targets = {m["対象"]: m for m in merged[so.SO_KEY]["不一致"]}
        self.assertIn("人物[鈴木 誠]", targets)
        self.assertIn("再読解のみ", targets["人物[鈴木 誠]"]["所見"])


class _Response:
    def __init__(self, payload):
        class _Block:
            type = "tool_use"
            name = "save_koseki_reading"
            input = payload
        self.content = [_Block()]
        self.stop_reason = "tool_use"


class TestProcessRecordWiring(unittest.IsolatedAsyncioTestCase):
    """process_record への結線（発動・非発動・縮退・既存経路無影響）"""

    def _record(self):
        return {"読解状態": {"value": "未読解"},
                "原本PDF": {"value": [{"fileKey": "fk-koseki", "name": "k.pdf"}]},
                "読解JSON": {"value": json.dumps({"ocr_text": "戸籍OCR..."},
                                                 ensure_ascii=False)}}

    def arm(self, *, env=None, first=None, second_reading=None,
            so_fail=False):
        self.saved = {}

        async def get_record(app, rid):
            return self._record()

        async def update_record(app, rid, fields, revision=None):
            self.saved = fields

        self.download = AsyncMock(return_value=b"%PDF-1.4 fake")
        self.first_read = AsyncMock(
            return_value=first if first is not None
            else reading(form_conf=0.5, honseki_conf=0.5, name_conf=0.5))
        if so_fail:
            self.second_read = AsyncMock(side_effect=RuntimeError("vision boom"))
        else:
            self.second_read = AsyncMock(
                return_value=second_reading if second_reading is not None
                else second(name="鈴木縫次郎", aite="内田チヨ子"))
        patchers = [
            patch.dict(os.environ, env if env is not None else _ENV),
            patch("hub.kintone.get_record", new=get_record),
            patch("hub.kintone.update_record", new=update_record),
            patch("hub.kintone.download_file", new=self.download),
            patch.object(koseki_reader, "_read_with_claude",
                         new=self.first_read),
            patch.object(so, "read_pdf_with_claude", new=self.second_read),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    async def test_low_confidence_triggers_second_opinion_once(self):
        self.arm()
        result = await koseki_reader.process_record("1")
        self.second_read.assert_awaited_once()
        self.download.assert_awaited_once_with(
            koseki_reader.APP_KOSEKI_BOOK, "fk-koseki")
        saved_json = json.loads(self.saved["読解JSON"])
        self.assertTrue(saved_json[so.SO_KEY]["実施"])
        self.assertIn(result["status"], ("ai_done", "needs_reread"))

    async def test_agreement_raises_overall_confidence(self):
        """全一致なら確信度引き上げが全体確信度に反映される"""
        first = reading(form_conf=0.6, honseki_conf=0.6, name_conf=0.6,
                        name="鈴木金次", aite="内田チョ子")
        self.arm(first=first,
                 second_reading=second(name="鈴木 金次", aite="内田チョ子"))
        await koseki_reader.process_record("1")
        saved_json = json.loads(self.saved["読解JSON"])
        self.assertEqual(saved_json[so.SO_KEY]["不一致"], [])
        self.assertGreaterEqual(float(self.saved["全体確信度"]), 0.9,
                                "一致フィールドの引き上げが全体に反映")

    async def test_flag_off_is_completely_inert(self):
        """フラグ無効: 視覚読解もPDF取得も呼ばれず、保存JSONにブロックなし"""
        self.arm(env={**_ENV, "SECOND_OPINION_ENABLED": ""})
        await koseki_reader.process_record("1")
        self.second_read.assert_not_awaited()
        self.download.assert_not_awaited()
        self.assertNotIn(so.SO_KEY, json.loads(self.saved["読解JSON"]))

    async def test_high_confidence_does_not_trigger(self):
        first = reading(form_conf=0.95, honseki_conf=0.95, name_conf=0.95)
        first["人物"][0]["身分事項"][0]["confidence"] = 0.95
        first["人物"][0]["confidence"] = {"氏名": 0.95, "生年月日": 0.95}
        first["戸籍"]["confidence"] = {"本籍": 0.95, "筆頭者": 0.95}
        self.arm(first=first)
        await koseki_reader.process_record("1")
        self.second_read.assert_not_awaited()

    async def test_second_opinion_failure_degrades_gracefully(self):
        """視覚再読解の失敗 → 一次読解のまま保存（エラー記録つき・読解は成立）"""
        self.arm(so_fail=True)
        result = await koseki_reader.process_record("1")
        saved_json = json.loads(self.saved["読解JSON"])
        self.assertFalse(saved_json[so.SO_KEY]["実施"])
        self.assertIn("vision boom", saved_json[so.SO_KEY]["エラー"])
        self.assertEqual(saved_json["戸籍"]["筆頭者"], "鈴木縫次郎",
                         "一次読解は保存される")
        self.assertIn(result["status"], ("ai_done", "needs_reread"))


class TestVisionCallShape(unittest.IsolatedAsyncioTestCase):
    """視覚再読解の呼び出し形（documentブロック・既存toolスキーマ・プロンプト要件）"""

    async def test_document_block_and_prompt(self):
        captured = {}

        async def fake_create(client, **kwargs):
            captured.update(kwargs)
            return _Response({"form": "現行", "form_confidence": 0.9,
                              "koseki": {"honseki": "x", "hittousha": "y"},
                              "persons": []})

        with patch.dict(os.environ, _ENV), \
                patch("claude_gateway.create_message_with_fallback",
                      new=fake_create):
            result = await so.read_pdf_with_claude(b"%PDF-1.4")
        content = captured["messages"][0]["content"]
        self.assertEqual(content[0]["type"], "document")
        self.assertEqual(content[0]["source"]["media_type"], "application/pdf")
        prompt = content[1]["text"]
        for phrase in ("算用数字", "小書き仮名", "推測で埋めない",
                       "縫次郎/金次", "チョ子/チヨ子", "雅寿→次",
                       "生年月日の行の混入", "改製原"):
            self.assertIn(phrase, prompt)
        self.assertEqual(captured["tools"][0]["name"], "save_koseki_reading",
                         "既存R3のtoolスキーマを流用")
        # 写像層を通って日本語キーで返る
        self.assertEqual(result["戸籍"]["本籍"], "x")


if __name__ == "__main__":
    unittest.main()
