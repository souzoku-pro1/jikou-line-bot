"""valuation_reader.py（S4-M1 評価証明・課税明細の構造化読解）のテスト

検証:
tool スキーマのキー制約（直接検査。AST 静的検査は test_koseki_tool_schema が
自動で対象化）・写像層の全項目往復（英語→日本語・confidence キー翻訳・恒等・
未知キー素通し・欠落キー非補完）・validate の各逸脱・確信度3層（平均・env 閾値）・
サンプル OCR 回帰（課税明細の複数物件ケース・Claude はモック・tool use 強制の
呼び出し形まで固定）。既存 /ocr/fixed-asset・zaisan_sync は不変（本部品は未結線）。
"""

import asyncio
import os
import re
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import valuation_reader  # noqa: E402
from valuation_reader import (  # noqa: E402
    DOC_KINDS,
    PROPERTY_KINDS,
    VALUATION_READING_TOOL,
    ValuationReaderError,
    overall_confidence,
    read_valuation,
    reread_threshold,
    to_japanese_valuation,
    validate_reading,
)

_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")


def run(coro):
    return asyncio.run(coro)


def english_reading() -> dict:
    """VALUATION_READING_TOOL（英語キー）どおりの出力サンプル
    （課税明細・複数物件=土地＋家屋）"""
    return {
        "doc_kind": "課税明細",
        "year": 2026,
        "owner_name": "熊澤正広",
        "properties": [
            {"kind": "土地", "location": "入間市東藤沢七丁目",
             "lot_number": "153番26", "assessed_value": 12345678,
             "confidence": {"location": 0.95, "assessed_value": 0.9}},
            {"kind": "家屋", "location": "入間市東藤沢七丁目153番地26",
             "building_number": "153番26", "assessed_value": 3456789,
             "confidence": {"building_number": 0.85}},
        ],
        "confidence": {"doc_kind": 0.95, "year": 0.9, "owner_name": 0.8},
    }


class TestToolSchemaKeys(unittest.TestCase):
    def test_all_property_keys_are_ascii(self):
        def walk(schema, where="root"):
            for key, sub in schema.get("properties", {}).items():
                self.assertRegex(key, _KEY_PATTERN, f"{where}.{key}")
                walk(sub, f"{where}.{key}")
            if isinstance(schema.get("items"), dict):
                walk(schema["items"], f"{where}[]")
        walk(VALUATION_READING_TOOL["input_schema"])

    def test_enums(self):
        props = VALUATION_READING_TOOL["input_schema"]["properties"]
        self.assertEqual(props["doc_kind"]["enum"], DOC_KINDS)
        self.assertEqual(props["properties"]["items"]["properties"]["kind"]["enum"],
                         PROPERTY_KINDS)
        self.assertEqual(DOC_KINDS, ["評価証明", "課税明細", "不明"])
        self.assertEqual(PROPERTY_KINDS, ["土地", "家屋", "不明"])


class TestToJapaneseValuation(unittest.TestCase):
    def test_full_mapping(self):
        mapped = to_japanese_valuation(english_reading())
        self.assertEqual(mapped["書類種別"], "課税明細")
        self.assertEqual(mapped["年度"], 2026)
        self.assertEqual(mapped["所有者名"], "熊澤正広")
        land, building = mapped["物件"]
        self.assertEqual(land["種別"], "土地")
        self.assertEqual(land["所在"], "入間市東藤沢七丁目")
        self.assertEqual(land["地番"], "153番26")
        self.assertEqual(land["評価額"], 12345678)
        self.assertEqual(building["種別"], "家屋")
        self.assertEqual(building["家屋番号"], "153番26")
        self.assertEqual(building["評価額"], 3456789)
        # 写像後は検証に適合し、英語キーが残らない
        self.assertEqual(validate_reading(mapped), [])
        flat = str(mapped)
        for en in ("'doc_kind'", "'year'", "'owner_name'", "'properties'",
                   "'kind'", "'location'", "'assessed_value'"):
            self.assertNotIn(en, flat)

    def test_confidence_map_keys_translated(self):
        mapped = to_japanese_valuation(english_reading())
        self.assertEqual(mapped["confidence"],
                         {"書類種別": 0.95, "年度": 0.9, "所有者名": 0.8})
        self.assertEqual(mapped["物件"][0]["confidence"],
                         {"所在": 0.95, "評価額": 0.9})
        self.assertEqual(mapped["物件"][1]["confidence"], {"家屋番号": 0.85})

    def test_japanese_input_is_identity(self):
        japanese = to_japanese_valuation(english_reading())
        self.assertEqual(to_japanese_valuation(japanese), japanese)

    def test_unknown_keys_pass_through(self):
        mapped = to_japanese_valuation(
            {"doc_kind": "評価証明", "future": 1,
             "properties": [{"kind": "土地", "location": "x", "extra": "z"}]})
        self.assertEqual(mapped["future"], 1)
        self.assertEqual(mapped["物件"][0]["extra"], "z")

    def test_missing_keys_stay_missing(self):
        mapped = to_japanese_valuation({"doc_kind": "評価証明",
                                        "properties": [{"kind": "土地"}]})
        self.assertEqual(mapped, {"書類種別": "評価証明", "物件": [{"種別": "土地"}]})
        self.assertNotIn("年度", mapped)


class TestValidateReading(unittest.TestCase):
    def test_valid_reading_passes(self):
        self.assertEqual(
            validate_reading(to_japanese_valuation(english_reading())), [])

    def test_violations_detected(self):
        cases = [
            ({"書類種別": "登記", "物件": [{"種別": "土地", "所在": "x"}]},
             "書類種別 が許容値外"),
            ({"書類種別": "課税明細", "物件": []}, "物件 が空でない配列でない"),
            ({"書類種別": "課税明細", "年度": "令和6",
              "物件": [{"種別": "土地", "所在": "x"}]}, "年度 が整数でも null でもない"),
            ({"書類種別": "課税明細",
              "物件": [{"種別": "建物", "所在": "x"}]}, "種別 が許容値外"),
            ({"書類種別": "課税明細",
              "物件": [{"種別": "土地", "所在": ""}]}, "所在 が空でない文字列でない"),
            ({"書類種別": "課税明細",
              "物件": [{"種別": "土地", "所在": "x", "評価額": "12,345円"}]},
             "評価額 が整数でも null でもない"),
        ]
        for reading, expected in cases:
            with self.subTest(expected=expected):
                errors = validate_reading(reading)
                self.assertTrue(any(expected in e for e in errors), errors)


class TestConfidence(unittest.TestCase):
    def test_overall_is_mean_of_all_confidences(self):
        mapped = to_japanese_valuation(english_reading())
        # top: 0.95,0.9,0.8 / 土地: 0.95,0.9 / 家屋: 0.85
        self.assertEqual(overall_confidence(mapped),
                         round((0.95 + 0.9 + 0.8 + 0.95 + 0.9 + 0.85) / 6, 3))

    def test_no_confidence_values_is_zero(self):
        self.assertEqual(overall_confidence(
            {"書類種別": "課税明細", "物件": [{"種別": "土地", "所在": "x"}]}), 0.0)

    def test_threshold_default_and_env_override(self):
        self.assertEqual(reread_threshold(), 0.5)
        with patch.dict(os.environ, {"VALUATION_REREAD_THRESHOLD": "0.7"}):
            self.assertEqual(reread_threshold(), 0.7)


class _Block:
    type = "tool_use"
    name = "save_valuation_reading"

    def __init__(self, data):
        self.input = data


SAMPLE_OCR = (
    "令和8年度 固定資産税・都市計画税 課税明細書\n納税義務者 熊澤正広\n"
    "所在地 入間市東藤沢七丁目 153番26 宅地 評価額 12,345,678円\n"
    "所在地 入間市東藤沢七丁目153番地26 家屋番号 153番26 居宅 評価額 3,456,789円"
)


class TestReadValuation(unittest.IsolatedAsyncioTestCase):
    async def test_sample_ocr_regression_multi_property(self):
        """課税明細の複数物件サンプル → tool use 強制 → 日本語キー評価JSON（回帰固定）"""
        response = MagicMock(content=[_Block(english_reading())])
        gateway = AsyncMock(return_value=response)
        with patch.object(valuation_reader, "create_message_with_fallback",
                          new=gateway), \
                patch.object(valuation_reader, "_get_client", new=MagicMock()):
            reading = await read_valuation(SAMPLE_OCR)

        kwargs = gateway.await_args.kwargs
        self.assertEqual(kwargs["context"], "評価証明読解")
        self.assertEqual(kwargs["tool_choice"],
                         {"type": "tool", "name": "save_valuation_reading"})
        self.assertIn(SAMPLE_OCR, kwargs["messages"][0]["content"])
        self.assertIn("すべての物件", kwargs["messages"][0]["content"],
                      "複数物件の全抽出指示がプロンプトに載る")

        self.assertEqual(len(reading["物件"]), 2, "複数物件が配列で返る")
        self.assertEqual(reading["物件"][0]["評価額"], 12345678)
        self.assertEqual(reading["書類種別"], "課税明細")
        self.assertEqual(validate_reading(reading), [])
        self.assertGreater(overall_confidence(reading), reread_threshold())

    async def test_no_tool_use_raises(self):
        response = MagicMock(content=[], stop_reason="end_turn")
        with patch.object(valuation_reader, "create_message_with_fallback",
                          new=AsyncMock(return_value=response)), \
                patch.object(valuation_reader, "_get_client", new=MagicMock()):
            with self.assertRaises(ValuationReaderError):
                await read_valuation(SAMPLE_OCR)


if __name__ == "__main__":
    unittest.main()
