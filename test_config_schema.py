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
    "SUBTABLE",  # App 34（人物）の身分事項・登場戸籍（2026-07-05 追加）
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


class TestApp34Schema(unittest.TestCase):
    """App 34（人物）のスキーマ定義検証
    （docs/koseki-ocr/02 §2 改訂版・2026-07-05 実機フォーム設計APIと完全形35一致を確認済み）"""

    def setUp(self):
        self.app34 = EXPECTED_KINTONE_SCHEMA["App 34 (人物)"]

    def test_env_names(self):
        """env名の正は APP_KOSEKI_PERSON（APP_JINBUTSU は使わない・2026-07-05 裁定）"""
        self.assertEqual(self.app34["app_id_env"], "APP_KOSEKI_PERSON")
        self.assertEqual(self.app34["token_env"], "TOKEN_KOSEKI_PERSON")

    def test_field_count_is_34(self):
        """完全形35 = トップレベル34（サブテーブル2本含む）＋登場戸籍の続柄原文（テーブル内）"""
        self.assertEqual(len(self.app34["fields"]), 34)

    def test_is_optional(self):
        self.assertIs(self.app34.get("optional"), True)

    def test_parent_edges_present(self):
        """親子エッジ4（関係図・相続人導出の骨格）が監視対象に含まれること"""
        for code in ("父人物ID", "母人物ID", "養父人物ID", "養母人物ID"):
            self.assertEqual(self.app34["fields"][code]["type"],
                             "SINGLE_LINE_TEXT", code)

    def test_dropdown_options_match_live(self):
        """選択肢は実機実出力どおり（2026-07-05 フォーム設計取得API）"""
        expect = {
            "名寄せ確定": {"未確定", "自動候補", "確定"},
            "相続人候補": {"候補", "非該当", "未判定"},
            "相続資格": {"未判定", "法定相続人", "代襲相続人",
                         "数次相続人", "相続放棄済", "資格なし"},
            "生死区分": {"生存", "死亡", "不明"},
            "確認状態": {"未確認", "確認済", "要再確認"},
        }
        for code, options in expect.items():
            f = self.app34["fields"][code]
            self.assertEqual(f["type"], "DROP_DOWN", code)
            self.assertEqual(set(f["required_options"]), options, code)

    def test_subtables_registered_as_type_only(self):
        """身分事項・登場戸籍は SUBTABLE 型のみ検査（内部列は対象外）"""
        for code in ("身分事項", "登場戸籍"):
            self.assertEqual(self.app34["fields"][code], {"type": "SUBTABLE"}, code)

    def test_all_types_valid(self):
        for code, spec in self.app34["fields"].items():
            self.assertIn(spec["type"], VALID_TYPES, f"{code} の型が不正")


class TestApp35Schema(unittest.TestCase):
    """App 35（財産）のスキーマ定義検証
    （docs/souzoku-shorui/01 §1.1・2026-07-06 実機フォーム設計APIと19フィールド全一致を確認済み）"""

    def setUp(self):
        self.app35 = EXPECTED_KINTONE_SCHEMA["App 35 (財産)"]

    def test_env_names(self):
        self.assertEqual(self.app35["app_id_env"], "APP_ZAISAN")
        self.assertEqual(self.app35["token_env"], "TOKEN_ZAISAN")

    def test_field_count_is_19(self):
        self.assertEqual(len(self.app35["fields"]), 19)

    def test_is_optional(self):
        self.assertIs(self.app35.get("optional"), True)

    def test_case_reference_fields(self):
        """案件参照4点（ハブ共通方式）が監視対象に含まれること"""
        f = self.app35["fields"]
        self.assertEqual(set(f["ユニット種別"]["required_options"]),
                         {"時効援用", "相続放棄", "相続一般", "補助金"})
        for code in ("案件アプリID", "案件レコードID", "被相続人名表示用"):
            self.assertEqual(f[code]["type"], "SINGLE_LINE_TEXT", code)

    def test_zaisan_type_options_13(self):
        """財産種別13値（実機実出力どおり・債務/葬儀費用を含む）"""
        f = self.app35["fields"]["財産種別"]
        self.assertEqual(f["type"], "DROP_DOWN")
        self.assertEqual(len(f["required_options"]), 13)
        self.assertEqual(set(f["required_options"]),
                         {"不動産_土地", "不動産_建物", "不動産_区分建物", "預貯金",
                          "有価証券", "生命保険", "出資金", "自動車", "動産",
                          "債権", "債務", "葬儀費用", "その他"})

    def test_valuation_fields(self):
        """評価まわり: 評価方法7値・評価基準日=DATE・評価確定=RADIO（yes/no）"""
        f = self.app35["fields"]
        self.assertEqual(set(f["評価方法"]["required_options"]),
                         {"固定資産税評価額", "相続税評価額", "残高証明",
                          "解約返戻金相当額", "時価査定", "額面", "その他"})
        self.assertEqual(f["評価基準日"]["type"], "DATE")
        self.assertEqual(f["評価確定"]["type"], "RADIO_BUTTON")
        self.assertEqual(set(f["評価確定"]["required_options"]), {"no", "yes"})

    def test_traceability_fields(self):
        """データ源5値（OCR3経路＋手入力＋ヒアリング）・原本=FILE・冪等キー・有効フラグ"""
        f = self.app35["fields"]
        self.assertEqual(set(f["データ源"]["required_options"]),
                         {"OCR_課税明細", "OCR_残高証明", "OCR_登記事項証明",
                          "手入力", "ヒアリング"})
        self.assertEqual(f["原本"]["type"], "FILE")
        self.assertEqual(f["冪等キー"]["type"], "SINGLE_LINE_TEXT")
        self.assertEqual(set(f["有効"]["required_options"]), {"yes", "no"})

    def test_all_types_valid(self):
        for code, spec in self.app35["fields"].items():
            self.assertIn(spec["type"], VALID_TYPES, f"{code} の型が不正")


class TestApp36Schema(unittest.TestCase):
    """App 36（相続人）のスキーマ定義検証
    （docs/souzoku-shorui/01 §2・2026-07-06 実機フォーム設計APIと16フィールド全一致を確認済み）"""

    def setUp(self):
        self.app36 = EXPECTED_KINTONE_SCHEMA["App 36 (相続人)"]

    def test_env_names(self):
        self.assertEqual(self.app36["app_id_env"], "APP_SOUZOKUNIN")
        self.assertEqual(self.app36["token_env"], "TOKEN_SOUZOKUNIN")

    def test_field_count_is_16(self):
        self.assertEqual(len(self.app36["fields"]), 16)

    def test_is_optional(self):
        self.assertIs(self.app36.get("optional"), True)

    def test_birth_date_is_text_not_date(self):
        """生年月日は SINGLE_LINE_TEXT（協議書の当事者表示に和暦等を
        そのまま差し込むため DATE 型にしない・設計01 §2）"""
        self.assertEqual(self.app36["fields"]["生年月日"]["type"],
                         "SINGLE_LINE_TEXT")

    def test_zokugara_options_with_zenkaku_parens(self):
        """続柄7値・括弧は全角（実機実出力どおり）"""
        f = self.app36["fields"]["続柄"]
        self.assertEqual(f["type"], "DROP_DOWN")
        self.assertEqual(set(f["required_options"]),
                         {"配偶者", "子", "直系尊属", "兄弟姉妹",
                          "甥姪（代襲）", "受遺者（相続人外）", "その他"})

    def test_status_options_6(self):
        """状態6値（未成年の括弧も全角）"""
        f = self.app36["fields"]["状態"]
        self.assertEqual(set(f["required_options"]),
                         {"通常", "放棄済み", "代襲", "相続分譲渡",
                          "未成年（特別代理人要）", "成年被後見人"})

    def test_document_generation_gate_fields(self):
        """書類生成の前提: 戸籍確認済=RADIO（yes/no）・印鑑証明3値・データ源3値"""
        f = self.app36["fields"]
        self.assertEqual(f["戸籍確認済"]["type"], "RADIO_BUTTON")
        self.assertEqual(set(f["戸籍確認済"]["required_options"]), {"no", "yes"})
        self.assertEqual(set(f["印鑑証明"]["required_options"]),
                         {"未", "依頼中", "受領"})
        self.assertEqual(set(f["データ源"]["required_options"]),
                         {"ヒアリング", "戸籍読解", "手入力"})

    def test_no_yuukou_field(self):
        """App 36 に「有効」フィールドは無い（App 35 との差異・実機どおり）"""
        self.assertNotIn("有効", self.app36["fields"])

    def test_all_types_valid(self):
        for code, spec in self.app36["fields"].items():
            self.assertIn(spec["type"], VALID_TYPES, f"{code} の型が不正")


class TestApp37Schema(unittest.TestCase):
    """App 37（割付）のスキーマ定義検証
    （docs/souzoku-shorui/01 §3・2026-07-06 実機フォーム設計APIと11フィールド全一致を確認済み）"""

    def setUp(self):
        self.app37 = EXPECTED_KINTONE_SCHEMA["App 37 (割付)"]

    def test_env_names(self):
        self.assertEqual(self.app37["app_id_env"], "APP_WARITSUKE")
        self.assertEqual(self.app37["token_env"], "TOKEN_WARITSUKE")

    def test_field_count_is_11(self):
        self.assertEqual(len(self.app37["fields"]), 11)

    def test_is_optional(self):
        self.assertIs(self.app37.get("optional"), True)

    def test_allocation_edge_fields(self):
        """割付の両端（App 35 財産・App 36 相続人へのレコードID参照）が監視対象に含まれること"""
        for code in ("財産レコードID", "相続人レコードID"):
            self.assertEqual(self.app37["fields"][code]["type"],
                             "SINGLE_LINE_TEXT", code)

    def test_acquisition_type_options_with_zenkaku_parens(self):
        """取得区分6値（保険金受取の括弧は全角・実機実出力どおり）"""
        f = self.app37["fields"]["取得区分"]
        self.assertEqual(f["type"], "DROP_DOWN")
        self.assertEqual(set(f["required_options"]),
                         {"単独取得", "共有取得", "換価分割", "代償取得",
                          "債務引受", "保険金受取（みなし）"})

    def test_compensation_and_memo(self):
        """代償金額=NUMBER・条件メモ=複数行・持分=文字列（分数表記等をそのまま持つ）"""
        f = self.app37["fields"]
        self.assertEqual(f["代償金額"]["type"], "NUMBER")
        self.assertEqual(f["条件メモ"]["type"], "MULTI_LINE_TEXT")
        self.assertEqual(f["持分"]["type"], "SINGLE_LINE_TEXT")

    def test_yuukou_flag(self):
        f = self.app37["fields"]["有効"]
        self.assertEqual(f["type"], "RADIO_BUTTON")
        self.assertEqual(set(f["required_options"]), {"yes", "no"})

    def test_all_types_valid(self):
        for code, spec in self.app37["fields"].items():
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

    def test_app34_env_unset_is_silently_skipped(self):
        """App 34 の env 未設定は警報ゼロ（optional 方式・App 33 と同じ）"""
        schema = {"App 34 (人物)": EXPECTED_KINTONE_SCHEMA["App 34 (人物)"]}
        self.assertEqual(self._run(schema), [])

    def test_app35_env_unset_is_silently_skipped(self):
        """App 35 の env 未設定は警報ゼロ（optional 方式・App 33/34 と同じ）"""
        schema = {"App 35 (財産)": EXPECTED_KINTONE_SCHEMA["App 35 (財産)"]}
        self.assertEqual(self._run(schema), [])

    def test_app36_env_unset_is_silently_skipped(self):
        """App 36 の env 未設定は警報ゼロ（optional 方式・App 33/34 と同じ）"""
        schema = {"App 36 (相続人)": EXPECTED_KINTONE_SCHEMA["App 36 (相続人)"]}
        self.assertEqual(self._run(schema), [])

    def test_app37_env_unset_is_silently_skipped(self):
        """App 37 の env 未設定は警報ゼロ（optional 方式・App 33〜36 と同じ）"""
        schema = {"App 37 (割付)": EXPECTED_KINTONE_SCHEMA["App 37 (割付)"]}
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
