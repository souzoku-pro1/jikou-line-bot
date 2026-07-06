"""registry_reader.py（S5-1 登記事項証明の構造化読解）のテスト

検証:
tool スキーマのキー制約（^[a-zA-Z0-9_.-]{1,64}$・直接検査。AST 静的検査は
test_koseki_tool_schema が自動で対象化）・写像層の全項目往復（英語→日本語・
恒等・未知キー素通し・欠落キー非補完・confidence キー翻訳）・validate の各逸脱・
確信度3層（収集・平均・env 閾値）・サンプル OCR テキストの読解回帰
（Claude はモック・tool use 強制の呼び出し形まで固定）。
"""

import asyncio
import os
import re
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import registry_reader  # noqa: E402
from registry_reader import (  # noqa: E402
    KINDS,
    REGISTRY_READING_TOOL,
    RegistryReaderError,
    overall_confidence,
    read_registry,
    reread_threshold,
    to_japanese_registry,
    validate_reading,
)

_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")


def run(coro):
    return asyncio.run(coro)


def english_reading() -> dict:
    """REGISTRY_READING_TOOL（英語キー）どおりの Claude 出力サンプル（全項目）"""
    return {
        "properties": [
            {
                "kind": "土地",
                "location": "埼玉県川口市青木一丁目",
                "lot_number": "123番4",
                "land_category": "宅地",
                "land_area": "123.45㎡",
                "kouku": {
                    "owners": [
                        {"name": "山田太郎", "address": "川口市青木一丁目1番1号",
                         "share": "2分の1", "confidence": {"name": 0.98, "share": 0.9}},
                        {"name": "山田花子", "address": "川口市青木一丁目1番1号",
                         "share": "2分の1"},
                    ],
                    "receipt_date": "平成14年3月1日",
                    "receipt_date_seireki": "2002-03-01",
                    "cause": "売買",
                    "cause_date": "平成14年2月15日",
                    "cause_date_seireki": None,
                    "confidence": {"receipt_date": 0.85, "cause": 0.8},
                },
                "otsuku": {"has_active_rights": True,
                           "detail": "抵当権（株式会社〇〇銀行・債権額 金3,600万円）",
                           "confidence": 0.75},
                "confidence": {"location": 0.95, "lot_number": 0.9},
            },
            {
                "kind": "建物",
                "location": "埼玉県川口市青木一丁目 123番地4",
                "building_number": "123番4",
                "building_type": "居宅",
                "structure": "木造かわらぶき2階建",
                "floor_area": "1階 58.50㎡ 2階 62.60㎡",
                "kouku": {"owners": [{"name": "山田太郎", "share": ""}]},
                "otsuku": {"has_active_rights": False, "detail": ""},
            },
        ],
    }


class TestToolSchemaKeys(unittest.TestCase):
    def test_all_property_keys_are_ascii(self):
        """API 制約 ^[a-zA-Z0-9_.-]{1,64}$ の直接検査（AST 走査の取りこぼし保険）"""
        def walk(schema, where="root"):
            for key, sub in schema.get("properties", {}).items():
                self.assertRegex(key, _KEY_PATTERN, f"{where}.{key}")
                walk(sub, f"{where}.{key}")
            if isinstance(schema.get("items"), dict):
                walk(schema["items"], f"{where}[]")
        walk(REGISTRY_READING_TOOL["input_schema"])

    def test_kind_enum_matches_kinds(self):
        item = REGISTRY_READING_TOOL["input_schema"]["properties"]["properties"]["items"]
        self.assertEqual(item["properties"]["kind"]["enum"], KINDS)


class TestToJapaneseRegistry(unittest.TestCase):
    def test_full_mapping(self):
        """英語キー入力→日本語キーの全項目写像（表題部・甲区・乙区・日付規約）"""
        mapped = to_japanese_registry(english_reading())
        land, building = mapped["物件"]
        self.assertEqual(land["種別"], "土地")
        self.assertEqual(land["所在"], "埼玉県川口市青木一丁目")
        self.assertEqual(land["地番"], "123番4")
        self.assertEqual(land["地目"], "宅地")
        self.assertEqual(land["地積"], "123.45㎡")
        kouku = land["甲区"]
        self.assertEqual(kouku["受付日"], "平成14年3月1日")
        self.assertEqual(kouku["受付日_西暦"], "2002-03-01")
        self.assertEqual(kouku["原因"], "売買")
        self.assertEqual(kouku["原因日付"], "平成14年2月15日")
        self.assertIsNone(kouku["原因日付_西暦"])
        o1, o2 = kouku["所有者"]
        self.assertEqual((o1["氏名"], o1["持分"]), ("山田太郎", "2分の1"))
        self.assertEqual((o2["氏名"], o2["持分"]), ("山田花子", "2分の1"))
        self.assertEqual(land["乙区"],
                         {"有効権利あり": True,
                          "内容": "抵当権（株式会社〇〇銀行・債権額 金3,600万円）",
                          "confidence": 0.75})
        self.assertEqual(building["家屋番号"], "123番4")
        self.assertEqual(building["種類"], "居宅")
        self.assertEqual(building["構造"], "木造かわらぶき2階建")
        self.assertEqual(building["床面積"], "1階 58.50㎡ 2階 62.60㎡")
        # 写像後は検証に適合し、英語キーが残らない
        self.assertEqual(validate_reading(mapped), [])
        flat = str(mapped)
        for en in ("'properties'", "'kind'", "'location'", "'owners'",
                   "'kouku'", "'otsuku'", "'name'"):
            self.assertNotIn(en, flat)

    def test_confidence_map_keys_translated(self):
        mapped = to_japanese_registry(english_reading())
        land = mapped["物件"][0]
        self.assertEqual(land["confidence"], {"所在": 0.95, "地番": 0.9})
        self.assertEqual(land["甲区"]["confidence"], {"受付日": 0.85, "原因": 0.8})
        self.assertEqual(land["甲区"]["所有者"][0]["confidence"],
                         {"氏名": 0.98, "持分": 0.9})

    def test_japanese_input_is_identity(self):
        japanese = to_japanese_registry(english_reading())
        self.assertEqual(to_japanese_registry(japanese), japanese)

    def test_unknown_keys_pass_through(self):
        mapped = to_japanese_registry(
            {"properties": [{"kind": "土地", "location": "x", "future": 1}],
             "extra_top": "y"})
        self.assertEqual(mapped["extra_top"], "y")
        self.assertEqual(mapped["物件"][0]["future"], 1)

    def test_missing_keys_stay_missing(self):
        mapped = to_japanese_registry({"properties": [{"kind": "土地"}]})
        self.assertEqual(mapped, {"物件": [{"種別": "土地"}]})
        self.assertNotIn("所在", mapped["物件"][0])


class TestValidateReading(unittest.TestCase):
    def test_valid_reading_passes(self):
        self.assertEqual(validate_reading(to_japanese_registry(english_reading())), [])

    def test_violations_detected(self):
        cases = [
            ({}, "物件 が空でない配列でない"),
            ({"物件": []}, "物件 が空でない配列でない"),
            ({"物件": [{"種別": "農地", "所在": "x"}]}, "種別 が許容値外"),
            ({"物件": [{"種別": "土地", "所在": ""}]}, "所在 が空でない文字列でない"),
            ({"物件": [{"種別": "土地", "所在": "x",
                        "甲区": {"所有者": [{"氏名": ""}]}}]}, "氏名 が文字列でない"),
            ({"物件": [{"種別": "土地", "所在": "x",
                        "乙区": {"有効権利あり": "yes"}}]}, "有効権利あり が真偽値でない"),
            ({"物件": [{"種別": "土地", "所在": "x", "甲区": "text"}]},
             "甲区 がオブジェクトでない"),
        ]
        for reading, expected in cases:
            with self.subTest(expected=expected):
                errors = validate_reading(reading)
                self.assertTrue(any(expected in e for e in errors), errors)


class TestConfidence(unittest.TestCase):
    def test_overall_is_mean_of_all_confidences(self):
        mapped = to_japanese_registry(english_reading())
        # 0.98,0.9（所有者1）/ 0.85,0.8（甲区）/ 0.75（乙区）/ 0.95,0.9（物件）
        self.assertEqual(overall_confidence(mapped), round(
            (0.98 + 0.9 + 0.85 + 0.8 + 0.75 + 0.95 + 0.9) / 7, 3))

    def test_no_confidence_values_is_zero(self):
        self.assertEqual(overall_confidence({"物件": [{"種別": "土地", "所在": "x"}]}),
                         0.0)

    def test_threshold_default_and_env_override(self):
        self.assertEqual(reread_threshold(), 0.5)
        with patch.dict(os.environ, {"REGISTRY_REREAD_THRESHOLD": "0.7"}):
            self.assertEqual(reread_threshold(), 0.7)


class _Block:
    type = "tool_use"
    name = "save_registry_reading"

    def __init__(self, data):
        self.input = data


SAMPLE_OCR = (
    "全部事項証明書（土地）\n所在 埼玉県川口市青木一丁目\n地番 123番4\n"
    "地目 宅地\n地積 123.45㎡\n権利部（甲区）\n所有者 山田太郎 持分2分の1\n"
    "平成14年3月1日受付 原因 売買\n権利部（乙区）\n抵当権設定 株式会社〇〇銀行"
)


class TestReadRegistry(unittest.IsolatedAsyncioTestCase):
    async def test_sample_ocr_regression(self):
        """サンプル OCR → tool use 強制呼び出し → 日本語キー登記JSON（回帰固定）"""
        response = MagicMock(content=[_Block(english_reading())])
        gateway = AsyncMock(return_value=response)
        with patch.object(registry_reader, "create_message_with_fallback",
                          new=gateway), \
                patch.object(registry_reader, "_get_client", new=MagicMock()):
            reading = await read_registry(SAMPLE_OCR)

        kwargs = gateway.await_args.kwargs
        self.assertEqual(kwargs["context"], "登記読解")
        self.assertEqual(kwargs["tool_choice"],
                         {"type": "tool", "name": "save_registry_reading"})
        self.assertEqual(kwargs["tools"][0]["name"], "save_registry_reading")
        self.assertIn(SAMPLE_OCR, kwargs["messages"][0]["content"])

        self.assertEqual(len(reading["物件"]), 2)
        self.assertEqual(reading["物件"][0]["地番"], "123番4")
        self.assertEqual(reading["物件"][0]["甲区"]["所有者"][0]["持分"], "2分の1")
        self.assertEqual(validate_reading(reading), [])
        self.assertGreater(overall_confidence(reading), reread_threshold())

    async def test_no_tool_use_raises(self):
        response = MagicMock(content=[], stop_reason="end_turn")
        with patch.object(registry_reader, "create_message_with_fallback",
                          new=AsyncMock(return_value=response)), \
                patch.object(registry_reader, "_get_client", new=MagicMock()):
            with self.assertRaises(RegistryReaderError):
                await read_registry(SAMPLE_OCR)


if __name__ == "__main__":
    unittest.main()
