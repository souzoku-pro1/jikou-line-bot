"""tool スキーマのキー制約（静的検査）と読解キー写像層のテスト

背景（2026-07-06 実機で判明）: Anthropic API は tool の input_schema の
プロパティキー名に ^[a-zA-Z0-9_.-]{1,64}$ を強制し、日本語キーは 400 で即時拒否
される。ユニットテストは Claude をモックするためこの制約に届かず、実機初回で
KOSEKI_READING_TOOL（旧・日本語キー）が全滅した。

検証:
1. リポジトリ内の全 tool スキーマのプロパティキーがパターンに適合すること
   （git 管理下の全 *.py を AST 走査——今後の新スキーマも自動で網にかかる）
2. to_japanese_reading の写像（英語キー→02 §3 日本語キーの全項目・
   confidence マップのキー翻訳・欠落キーは欠落のまま・日本語キー入力には恒等）
"""

import ast
import re
import subprocess
import unittest
from pathlib import Path

from koseki_reader import KOSEKI_READING_TOOL, to_japanese_reading, validate_reading

_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")


def _tracked_python_files() -> list[Path]:
    out = subprocess.run(["git", "ls-files", "*.py"],
                         capture_output=True, text=True, check=True).stdout
    return [Path(line) for line in out.splitlines() if line]


def _literal_property_keys(tree: ast.AST):
    """ソース中のリテラル dict のうち "properties" 直下のキー文字列を列挙する。
    （JSON Schema の properties はリテラル定義される前提。実行時に組む場合は
    このテストでは検出できないため、その際は個別テストを足すこと）"""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values):
            if (isinstance(key, ast.Constant) and key.value == "properties"
                    and isinstance(value, ast.Dict)):
                for prop_key in value.keys:
                    if isinstance(prop_key, ast.Constant) and \
                            isinstance(prop_key.value, str):
                        yield prop_key.value


class TestToolSchemaKeyPattern(unittest.TestCase):
    def test_all_literal_schema_property_keys_are_ascii(self):
        """git 管理下の全 *.py の properties キーがパターン適合（再帰・将来分も対象）"""
        violations = []
        scanned_files = 0
        found_keys = 0
        for path in _tracked_python_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            scanned_files += 1
            for key in _literal_property_keys(tree):
                found_keys += 1
                if not _KEY_PATTERN.match(key):
                    violations.append(f"{path}: {key!r}")
        self.assertGreater(scanned_files, 10, "走査対象が少なすぎる（git ls-files 失敗?）")
        self.assertGreater(found_keys, 20, "properties キーが検出できていない（走査壊れ?）")
        self.assertEqual(violations, [],
                         "input_schema のプロパティキーは ^[a-zA-Z0-9_.-]{1,64}$ 必須"
                         "（Anthropic API 制約・2026-07-06 実機 400 の再発防止）")

    def test_koseki_reading_tool_keys_directly(self):
        """KOSEKI_READING_TOOL 本体も直接検査（AST 走査の取りこぼし保険）"""
        def walk(schema, where="root"):
            props = schema.get("properties", {})
            for key, sub in props.items():
                self.assertRegex(key, _KEY_PATTERN, f"{where}.{key}")
                walk(sub, f"{where}.{key}")
            if isinstance(schema.get("items"), dict):
                walk(schema["items"], f"{where}[]")
        walk(KOSEKI_READING_TOOL["input_schema"])


def english_reading() -> dict:
    """KOSEKI_READING_TOOL（英語キー）どおりの Claude 出力サンプル（全項目）"""
    return {
        "form": "改製原（平成）",
        "form_confidence": 0.9,
        "koseki": {
            "honseki": "川口市青木一丁目1番地",
            "hittousha": "山田太郎",
            "hensei_date": "昭和32年4月1日",
            "hensei_date_seireki": "1957-04-01",
            "shojo_date": "平成14年3月1日",
            "shojo_date_seireki": None,
            "hensei_reason": "転籍",
            "juzen_koseki": {"honseki": "さいたま市浦和区高砂三丁目",
                             "hittousha": "山田先代"},
            "shin_koseki_honseki": "川口市青木二丁目",
            "confidence": {"honseki": 0.95, "hensei_date": 0.7},
        },
        "persons": [
            {"name": "山田太郎", "zokugara": "長男", "birth_date": "昭和10年1月2日",
             "removed": False, "removed_reason": "",
             "identity_events": [
                 {"type": "婚姻", "date": "昭和32年4月1日", "aite": "山田花子",
                  "biko": "", "confidence": 0.8}],
             "confidence": {"name": 0.99, "birth_date": 0.6}},
            {"name": "山田花子", "removed": True, "removed_reason": "死亡",
             "identity_events": [{"type": "死亡", "date": "令和2年5月3日"}]},
        ],
    }


class TestToJapaneseReading(unittest.TestCase):
    def test_full_mapping_to_02s3_contract(self):
        """英語キー入力→02 §3 日本語キー JSON の全項目写像"""
        mapped = to_japanese_reading(english_reading())
        self.assertEqual(mapped["様式"], "改製原（平成）")
        self.assertEqual(mapped["様式confidence"], 0.9)
        koseki = mapped["戸籍"]
        self.assertEqual(koseki["本籍"], "川口市青木一丁目1番地")
        self.assertEqual(koseki["筆頭者"], "山田太郎")
        self.assertEqual(koseki["編製日"], "昭和32年4月1日")
        self.assertEqual(koseki["編製日_西暦"], "1957-04-01")
        self.assertEqual(koseki["消除日"], "平成14年3月1日")
        self.assertIsNone(koseki["消除日_西暦"])
        self.assertEqual(koseki["編製事由"], "転籍")
        self.assertEqual(koseki["従前戸籍"],
                         {"本籍": "さいたま市浦和区高砂三丁目", "筆頭者": "山田先代"})
        self.assertEqual(koseki["新戸籍_本籍"], "川口市青木二丁目")
        p1, p2 = mapped["人物"]
        self.assertEqual(p1["氏名"], "山田太郎")
        self.assertEqual(p1["続柄"], "長男")
        self.assertEqual(p1["生年月日"], "昭和10年1月2日")
        self.assertIs(p1["除籍済み"], False)
        self.assertEqual(p1["身分事項"],
                         [{"種別": "婚姻", "日付": "昭和32年4月1日",
                           "相手方": "山田花子", "備考": "", "confidence": 0.8}])
        self.assertIs(p2["除籍済み"], True)
        self.assertEqual(p2["除籍事由"], "死亡")
        self.assertEqual(p2["身分事項"], [{"種別": "死亡", "日付": "令和2年5月3日"}])
        # 英語キーが残っていないこと（全キー翻訳の裏取り）
        self.assertEqual(validate_reading(mapped), [], "写像後は 02 §3 適合")
        flat = str(mapped)
        for en in ("'form'", "'koseki'", "'persons'", "'name'", "'honseki'",
                   "'identity_events'"):
            self.assertNotIn(en, flat)

    def test_confidence_map_keys_are_translated(self):
        """confidence マップの中のフィールド名キーも同じ対応表で翻訳される"""
        mapped = to_japanese_reading(english_reading())
        self.assertEqual(mapped["戸籍"]["confidence"],
                         {"本籍": 0.95, "編製日": 0.7})
        self.assertEqual(mapped["人物"][0]["confidence"],
                         {"氏名": 0.99, "生年月日": 0.6})

    def test_missing_keys_stay_missing(self):
        """欠落キーは補完しない（必須欠落の検知は validate_reading の責務のまま）"""
        mapped = to_japanese_reading(
            {"form": "現行", "koseki": {"honseki": "川口市"}})
        self.assertEqual(mapped, {"様式": "現行", "戸籍": {"本籍": "川口市"}})
        self.assertNotIn("様式confidence", mapped)
        errors = validate_reading(mapped)
        self.assertTrue(any("様式confidence" in e for e in errors))
        self.assertTrue(any("筆頭者" in e for e in errors))

    def test_japanese_input_is_identity(self):
        """日本語キー入力には恒等（既存 JSON の再処理・既存テスト流儀でも安全）"""
        japanese = to_japanese_reading(english_reading())
        self.assertEqual(to_japanese_reading(japanese), japanese)

    def test_unknown_keys_pass_through(self):
        """対応表にない未知キーは素通し（黙って落とさない）"""
        mapped = to_japanese_reading(
            {"form": "現行", "future_key": 1,
             "koseki": {"honseki": "x", "hittousha": "y", "extra": "z"}})
        self.assertEqual(mapped["future_key"], 1)
        self.assertEqual(mapped["戸籍"]["extra"], "z")

    def test_non_dict_values_pass_through(self):
        """型崩れ（戸籍が dict でない等）は写像せず素通し→validate が検知する構造"""
        broken = {"form": "現行", "koseki": "not-a-dict", "persons": "not-a-list"}
        mapped = to_japanese_reading(broken)
        self.assertEqual(mapped["戸籍"], "not-a-dict")
        self.assertTrue(validate_reading(mapped))


if __name__ == "__main__":
    unittest.main()
