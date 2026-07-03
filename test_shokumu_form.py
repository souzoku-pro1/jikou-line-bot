"""T3-2: 統一用紙重ね打ち PDF・キャリブレーション・レターパック往復ラベルのテスト

- 座標表（FORM_COORDS）全項目の配置（キャリブレーション PDF に全キー）
- サンプルデータでの記入欄配置（座標一致・チェック印・通数・空欄スキップ）
- グリッドモードの PDF 生成・決定性・オフセット補正
- レターパック往復ラベル（2ページ・事務所未設定→ValueError）
- prepare 成果物への統合（チェックリスト＋重ね打ち＋往復ラベル / 縮退時は宛名のみ）
"""

import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from channels import shokumu_seikyu as sk
from hub import address_label

OFFICE_ENV = {
    "OFFICE_NAME": "大野法律事務所",
    "OFFICE_ZIP": "332-0012",
    "OFFICE_ADDRESS": "埼玉県川口市本町4-1-6",
    "OFFICE_TEL": "048-000-0000",
    "OFFICE_ATTORNEY": "大野太郎",
    "OFFICE_ATTORNEY_REG": "12345",
}


def sample_record():
    return {"$id": {"value": "9"}, "顧客名表示用": {"value": "山田太郎"},
            "宛先名": {"value": ""}, "件名": {"value": "職務上請求（川口市）"}}


def sample_data(**over):
    data = {
        "request_items": [{"type": "戸籍謄本", "count": 2},
                          {"type": "住民票の除票", "count": 1}],
        "target": {"本籍": "埼玉県川口市青木2-1-1", "筆頭者": "山田一郎",
                   "対象者": "山田花子", "生年月日": "昭和25年3月15日"},
        "purpose": "受任事件（消滅時効援用）の通知書送付先調査のため" * 2,
    }
    data.update(over)
    return data


def items_by_coord(items):
    return {(i.x_mm, i.y_mm): i for i in items}


class TestFormCoords(unittest.TestCase):
    """座標表全項目の配置（完了条件1）"""

    def test_all_types_have_check_and_count_coords(self):
        for t in sk.FEE_FIELD_BY_TYPE:
            self.assertIn(f"請求種別チェック_{t}", sk.FORM_COORDS)
            self.assertIn(f"通数_{t}", sk.FORM_COORDS)

    def test_coords_within_page(self):
        w, h = sk.FORM_SIZE_MM
        for key, (x, y) in sk.FORM_COORDS.items():
            self.assertTrue(0 <= x <= w and 0 <= y <= h, f"{key} がページ外: {(x, y)}")

    def test_no_duplicate_coords(self):
        coords = list(sk.FORM_COORDS.values())
        self.assertEqual(len(coords), len(set(coords)), "座標の重複（重ね印字）")

    def test_calibration_pdf_places_every_key(self):
        """キャリブレーション PDF は全キーをキー名付きで配置する"""
        items = [sk.TextAt(x, y, f"└{k}", font_size=7)
                 for k, (x, y) in sk.FORM_COORDS.items()]
        placed = {i.text[1:] for i in items}
        self.assertEqual(placed, set(sk.FORM_COORDS))
        pdf = sk.build_calibration_pdf()
        self.assertTrue(pdf.startswith(b"%PDF"))


class TestBuildFormItems(unittest.TestCase):
    def setUp(self):
        patcher = patch.dict(os.environ, OFFICE_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_items_match_coordinate_table(self):
        items = sk.build_form_items(sample_record(), sample_data())
        by_coord = items_by_coord(items)
        expect = {
            "事務所名": "大野法律事務所",
            "弁護士氏名": "大野太郎",
            "弁護士登録番号": "12345",
            "対象者本籍": "埼玉県川口市青木2-1-1",
            "対象者筆頭者": "山田一郎",
            "対象者氏名": "山田花子",
            "対象者生年月日": "昭和25年3月15日",
            "依頼者氏名": "山田太郎",
        }
        for key, text in expect.items():
            item = by_coord.get(sk.FORM_COORDS[key])
            self.assertIsNotNone(item, f"{key} が配置されていない")
            self.assertEqual(item.text, text, key)

    def test_check_marks_and_counts_for_selected_types_only(self):
        items = sk.build_form_items(sample_record(), sample_data())
        by_coord = items_by_coord(items)
        # 選択した2種別: チェック印＋通数
        for t, count in (("戸籍謄本", "2"), ("住民票の除票", "1")):
            self.assertEqual(by_coord[sk.FORM_COORDS[f"請求種別チェック_{t}"]].text,
                             sk.CHECK_MARK)
            self.assertEqual(by_coord[sk.FORM_COORDS[f"通数_{t}"]].text, count)
        # 選択していない種別は印字なし
        for t in ("除籍謄本", "改製原戸籍", "戸籍の附票", "住民票"):
            self.assertNotIn(sk.FORM_COORDS[f"請求種別チェック_{t}"], by_coord)

    def test_lawyer_qualification_always_checked(self):
        items = sk.build_form_items(sample_record(), sample_data())
        by_coord = items_by_coord(items)
        self.assertEqual(by_coord[sk.FORM_COORDS["請求者資格チェック_弁護士"]].text,
                         sk.CHECK_MARK)

    def test_purpose_wraps_to_two_lines(self):
        purpose = "あ" * (sk._PURPOSE_WRAP + 5)
        items = sk.build_form_items(sample_record(), sample_data(purpose=purpose))
        by_coord = items_by_coord(items)
        self.assertEqual(by_coord[sk.FORM_COORDS["利用目的_1行目"]].text,
                         "あ" * sk._PURPOSE_WRAP)
        self.assertEqual(by_coord[sk.FORM_COORDS["利用目的_2行目"]].text, "あ" * 5)

    def test_blank_values_are_skipped(self):
        data = sample_data(request_date="")
        data["target"] = {"対象者": "山田花子"}  # 本籍・筆頭者・生年月日なし
        items = sk.build_form_items(sample_record(), data)
        by_coord = items_by_coord(items)
        for key in ("請求日", "対象者本籍", "対象者筆頭者", "対象者生年月日"):
            self.assertNotIn(sk.FORM_COORDS[key], by_coord, f"{key} は空欄のはず")

    def test_honseki_falls_back_to_address(self):
        data = sample_data()
        data["target"] = {"対象者": "山田花子", "住所": "東京都北区1-2-3"}
        items = sk.build_form_items(sample_record(), data)
        by_coord = items_by_coord(items)
        self.assertEqual(by_coord[sk.FORM_COORDS["対象者本籍"]].text, "東京都北区1-2-3")


class TestFormPdf(unittest.TestCase):
    def setUp(self):
        patcher = patch.dict(os.environ, OFFICE_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_pdf_generation_and_determinism(self):
        a = sk.build_request_form_pdf(sample_record(), sample_data())
        b = sk.build_request_form_pdf(sample_record(), sample_data())
        self.assertTrue(a.startswith(b"%PDF"))
        self.assertEqual(a, b, "同一入力→同一バイト列（invariant）")

    def test_grid_mode_differs(self):
        """グリッドモードの PDF 生成（完了条件2）"""
        plain = sk.build_request_form_pdf(sample_record(), sample_data())
        grid = sk.build_request_form_pdf(sample_record(), sample_data(), grid=True)
        self.assertTrue(grid.startswith(b"%PDF"))
        self.assertNotEqual(plain, grid)
        self.assertGreater(len(grid), len(plain), "方眼のぶん大きくなる")

    def test_print_offset_env_shifts_output(self):
        base = sk.build_request_form_pdf(sample_record(), sample_data())
        with patch.dict(os.environ, {"PRINT_OFFSET_X_MM": "1.5", "PRINT_OFFSET_Y_MM": "-2"}):
            shifted = sk.build_request_form_pdf(sample_record(), sample_data())
        self.assertNotEqual(base, shifted, "オフセット補正が印字位置に反映される")


class TestLetterpackRoundtrip(unittest.TestCase):
    def test_two_pages_with_office(self):
        with patch.dict(os.environ, OFFICE_ENV):
            pdf = address_label.render_letterpack_roundtrip(
                "川口市　市民課", "332-8601", "埼玉県川口市青木2-1-1", honorific="御中")
        self.assertTrue(pdf.startswith(b"%PDF"))
        self.assertIn(b"/Count 2", pdf, "宛先面＋返信面の2ページ")

    def test_missing_office_raises(self):
        with patch.dict(os.environ, {"OFFICE_NAME": "", "OFFICE_ZIP": "", "OFFICE_ADDRESS": ""}):
            with self.assertRaises(ValueError):
                address_label.render_letterpack_roundtrip("川口市", "332-8601", "住所")

    def test_deterministic(self):
        with patch.dict(os.environ, OFFICE_ENV):
            a = address_label.render_letterpack_roundtrip("川口市", "332-8601", "住所X")
            b = address_label.render_letterpack_roundtrip("川口市", "332-8601", "住所X")
        self.assertEqual(a, b)


def _muni():
    rec = {"市区町村名": {"value": "川口市"}, "都道府県": {"value": "埼玉県"},
           "担当部署": {"value": "市民課"}, "郵便番号": {"value": "332-8601"},
           "住所": {"value": "埼玉県川口市青木2-1-1"}, "備考": {"value": ""},
           "手数料_戸籍謄本": {"value": "450"}, "手数料_除籍改製原": {"value": "750"},
           "手数料_附票": {"value": "300"}, "手数料_住民票": {"value": "300"}}
    return rec


def _shipping_record():
    data = {"request_items": [{"type": "戸籍謄本", "count": 1}],
            "municipality": "川口市",
            "target": {"対象者": "山田花子", "本籍": "埼玉県川口市…"},
            "purpose": "送付先調査のため"}
    return {"$id": {"value": "9"}, "発送ステータス": {"value": "下書き"},
            "チャネル": {"value": "職務上請求"}, "件名": {"value": "職務上請求"},
            "顧客名表示用": {"value": "山田太郎"}, "宛先名": {"value": ""},
            "チャネル固有データ": {"value": json.dumps(data, ensure_ascii=False)}}


class TestPrepareIntegration(unittest.IsolatedAsyncioTestCase):
    """T3-1 の prepare 成果物への統合（完了条件: ④）"""

    async def test_artifacts_with_office(self):
        with patch.dict(os.environ, OFFICE_ENV), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=[_muni()])):
            result = await sk.ShokumuSeikyuAdapter().prepare(_shipping_record())
        names = [a.filename for a in result.artifacts]
        self.assertEqual(names, ["発送準備チェックリスト.pdf",
                                 "職務上請求書_重ね打ち.pdf",
                                 "レターパック往復ラベル.pdf"])
        for a in result.artifacts:
            self.assertTrue(a.content.startswith(b"%PDF"), a.filename)
        self.assertIn(b"/Count 2", result.artifacts[2].content)

    async def test_artifacts_degrade_without_office(self):
        """事務所情報未設定: 返信面なしの宛名ラベルに縮退（prepare は止めない）"""
        env = {k: "" for k in OFFICE_ENV}
        with patch.dict(os.environ, env), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=[_muni()])):
            result = await sk.ShokumuSeikyuAdapter().prepare(_shipping_record())
        names = [a.filename for a in result.artifacts]
        self.assertEqual(names, ["発送準備チェックリスト.pdf",
                                 "職務上請求書_重ね打ち.pdf",
                                 "レターパック宛名ラベル.pdf"])
        self.assertNotIn(b"/Count 2", result.artifacts[2].content)


if __name__ == "__main__":
    unittest.main()
