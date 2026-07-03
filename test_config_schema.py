"""EXPECTED_KINTONE_SCHEMA の定義自体の検証（T1-1 完了条件）

App 30（発送管理）のスキーマ定義が設計（docs/architecture/02 §2.1）の
フィールド数・状態機械の選択肢・冪等フラグを正しく写像していることを保証する。
（実環境との突合は daily_healthcheck の仕事。ここでは定義の内部整合を見る）
"""

import os
import unittest

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from config import EXPECTED_KINTONE_SCHEMA

APP30 = EXPECTED_KINTONE_SCHEMA["App 30 (発送管理)"]

VALID_TYPES = {
    "SINGLE_LINE_TEXT", "MULTI_LINE_TEXT", "NUMBER", "DROP_DOWN",
    "RADIO_BUTTON", "CHECK_BOX", "DATE", "DATETIME", "FILE",
}


class TestApp30Schema(unittest.TestCase):
    def test_env_names(self):
        self.assertEqual(APP30["app_id_env"], "APP_SHIPPING")
        self.assertEqual(APP30["token_env"], "TOKEN_SHIPPING")

    def test_field_count_is_27(self):
        self.assertEqual(len(APP30["fields"]), 27)

    def test_all_types_are_valid_kintone_types(self):
        for code, spec in APP30["fields"].items():
            self.assertIn(spec["type"], VALID_TYPES, f"{code} の型が不正")

    def test_state_machine_options_complete(self):
        """発送ステータスの選択肢が状態機械（ハブ 01 §4）の10状態と完全一致"""
        expected = {"下書き", "承認待ち", "承認済", "発送処理中", "発送済",
                    "返送待ち", "完了", "エラー", "却下", "要確認"}
        self.assertEqual(set(APP30["fields"]["発送ステータス"]["required_options"]), expected)

    def test_channel_options_complete(self):
        expected = {"職務上請求", "e内容証明", "FAX", "送付案内", "スキャン受領"}
        self.assertEqual(set(APP30["fields"]["チャネル"]["required_options"]), expected)

    def test_idempotency_flag(self):
        """冪等ガード（実行済み）が no/yes のラジオボタンであること"""
        f = APP30["fields"]["実行済み"]
        self.assertEqual(f["type"], "RADIO_BUTTON")
        self.assertEqual(f["required_options"], ["no", "yes"])

    def test_delivery_result_options(self):
        expected = {"未確認", "送達済", "不達", "返送受領"}
        self.assertEqual(set(APP30["fields"]["送達結果"]["required_options"]), expected)

    def test_enclosure_checkbox_has_no_pinned_options(self):
        """同封物選択は仮選択肢（未設定）を required に固定しない
        （T2-1 で実選択肢に差し替えるため・固定すると差し替え時に誤警報）"""
        self.assertNotIn("required_options", APP30["fields"]["同封物選択"])

    def test_direction_and_unit(self):
        self.assertEqual(set(APP30["fields"]["方向"]["required_options"]), {"発送", "受領"})
        self.assertIn("時効援用", APP30["fields"]["ユニット種別"]["required_options"])


class TestApp31Schema(unittest.TestCase):
    """App 31（市区町村マスタ）のスキーマ定義検証（docs/architecture/02 §3）"""

    def setUp(self):
        self.app31 = EXPECTED_KINTONE_SCHEMA["App 31 (市区町村マスタ)"]

    def test_env_names(self):
        self.assertEqual(self.app31["app_id_env"], "APP_CITY_MASTER")
        self.assertEqual(self.app31["token_env"], "TOKEN_CITY_MASTER")

    def test_field_count_is_14(self):
        self.assertEqual(len(self.app31["fields"]), 14)

    def test_fee_fields_are_number(self):
        for code in ("手数料_戸籍謄本", "手数料_除籍改製原", "手数料_附票", "手数料_住民票"):
            self.assertEqual(self.app31["fields"][code]["type"], "NUMBER", code)

    def test_active_flag_options(self):
        f = self.app31["fields"]["有効"]
        self.assertEqual(f["type"], "RADIO_BUTTON")
        self.assertEqual(f["required_options"], ["yes", "no"])

    def test_all_types_valid(self):
        for code, spec in self.app31["fields"].items():
            self.assertIn(spec["type"], VALID_TYPES, f"{code} の型が不正")


class TestApp32Schema(unittest.TestCase):
    """App 32（同封物ブロックマスタ）のスキーマ定義検証（docs/architecture/02 §4.1）"""

    def setUp(self):
        self.app32 = EXPECTED_KINTONE_SCHEMA["App 32 (同封物ブロックマスタ)"]

    def test_env_names(self):
        self.assertEqual(self.app32["app_id_env"], "APP_ENCLOSURE")
        self.assertEqual(self.app32["token_env"], "TOKEN_ENCLOSURE")

    def test_field_count_is_7(self):
        self.assertEqual(len(self.app32["fields"]), 7)

    def test_return_flag_options(self):
        f = self.app32["fields"]["返送要否"]
        self.assertEqual(f["type"], "RADIO_BUTTON")
        self.assertEqual(set(f["required_options"]), {"要", "不要"})

    def test_unit_checkbox_requires_jikou(self):
        f = self.app32["fields"]["対象ユニット"]
        self.assertEqual(f["type"], "CHECK_BOX")
        self.assertIn("時効援用", f["required_options"])

    def test_all_types_valid(self):
        for code, spec in self.app32["fields"].items():
            self.assertIn(spec["type"], VALID_TYPES, f"{code} の型が不正")


if __name__ == "__main__":
    unittest.main()
