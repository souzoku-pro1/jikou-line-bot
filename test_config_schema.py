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


class TestApp33Schema(unittest.TestCase):
    """App 33（戸籍読解）のスキーマ定義検証
    （docs/koseki-ocr/02 §1・2026-07-05 実機フォーム設計APIと突合済み）"""

    def setUp(self):
        self.app33 = EXPECTED_KINTONE_SCHEMA["App 33 (戸籍読解)"]

    def test_env_names(self):
        self.assertEqual(self.app33["app_id_env"], "APP_KOSEKI_BOOK")
        self.assertEqual(self.app33["token_env"], "TOKEN_KOSEKI_BOOK")

    def test_field_count_is_22(self):
        self.assertEqual(len(self.app33["fields"]), 22)

    def test_is_optional(self):
        """env 未設定の環境では監視スキップ（通帳と同じ optional 方式）"""
        self.assertIs(self.app33.get("optional"), True)

    def test_dates_are_text_not_date(self):
        """編製日・消除日は和暦原文保持のため SINGLE_LINE_TEXT
        （DATE型にしない・2026-07-05 検収裁定・02 §1）"""
        for code in ("編製日", "消除日"):
            self.assertEqual(self.app33["fields"][code]["type"],
                             "SINGLE_LINE_TEXT", code)

    def test_reading_status_options(self):
        """読解状態は koseki_ingest/koseki_reader が書く3値＋人手確認の「確認済」"""
        f = self.app33["fields"]["読解状態"]
        self.assertEqual(f["type"], "DROP_DOWN")
        self.assertEqual(set(f["required_options"]),
                         {"未読解", "AI読解済", "確認済", "要再読解"})

    def test_koseki_type_options(self):
        f = self.app33["fields"]["戸籍種別"]
        self.assertEqual(set(f["required_options"]),
                         {"現行", "改製原（平成）", "改製原（昭和）", "除籍", "不明"})

    def test_reader_written_fields_present(self):
        """koseki_ingest / koseki_reader が読み書きするフィールドが監視に含まれること"""
        for code in ("原本PDF", "ページ画像", "Drive_fileId", "読解JSON",
                     "読解状態", "案件アプリID", "案件レコードID",
                     "様式確信度", "全体確信度"):
            self.assertIn(code, self.app33["fields"])

    def test_all_types_valid(self):
        for code, spec in self.app33["fields"].items():
            self.assertIn(spec["type"], VALID_TYPES, f"{code} の型が不正")


class TestHealthcheckOptionalSkip(unittest.TestCase):
    """check_kintone_schema の env 未設定時の挙動
    （optional=スキップ・警報なし / 非optional=警報。既存挙動の回帰込み）"""

    def _run(self, schema):
        import asyncio
        from unittest.mock import patch

        import daily_healthcheck

        with patch.dict(os.environ, {"KINTONE_SUBDOMAIN": "testsub"}, clear=True), \
                patch.object(daily_healthcheck, "EXPECTED_KINTONE_SCHEMA", schema):
            return asyncio.run(daily_healthcheck.check_kintone_schema())

    def test_app33_env_unset_is_silently_skipped(self):
        """App 33 の env 未設定は警報ゼロ（該当アプリの検査スキップ）"""
        schema = {"App 33 (戸籍読解)": EXPECTED_KINTONE_SCHEMA["App 33 (戸籍読解)"]}
        self.assertEqual(self._run(schema), [])

    def test_non_optional_env_unset_still_alarms(self):
        """既存回帰: optional でないアプリの env 未設定は従来どおり警報"""
        schema = {"App 21 (案件)": EXPECTED_KINTONE_SCHEMA["App 21 (案件)"]}
        problems = self._run(schema)
        self.assertEqual(len(problems), 1)
        self.assertIn("KINTONE_APP_ID", problems[0])

    def test_optional_flag_generic(self):
        """optional 方式の一般挙動: optional のみスキップ・非optional のみ警報"""
        schema = {
            "opt": {"app_id_env": "X_APP", "token_env": "X_TOKEN",
                    "optional": True, "fields": {}},
            "req": {"app_id_env": "Y_APP", "token_env": "Y_TOKEN", "fields": {}},
        }
        problems = self._run(schema)
        self.assertEqual(len(problems), 1)
        self.assertIn("Y_APP", problems[0])


if __name__ == "__main__":
    unittest.main()
