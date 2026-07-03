"""T3-2（2様式対応版）: 統一用紙重ね打ち PDF・キャリブレーション・レターパック往復ラベル

- 様式判定（戸籍系→様式1 / 住民票系→様式2・混在は2枚生成）
- 様式別の配置テスト（座標一致・丸/チェック・請求者欄の既定ON/OFF と上書き）
- 生年月日分岐（様式1欠損→エラー / 様式2欠損→正常生成）・元号丸（和暦/ISO両対応）
- 様式別キャリブレーション PDF・グリッドモード・決定性・オフセット補正
- prepare 成果物への統合（チェックリスト＋様式別PDF＋往復ラベル）
"""

import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from channels import shokumu_seikyu as sk
from channels.shokumu_seikyu import FORM1, FORM2, ShokumuSeikyuError
from hub import address_label

OFFICE_ENV = {
    "OFFICE_NAME": "大野法律事務所",
    "OFFICE_ZIP": "332-0012",
    "OFFICE_ADDRESS": "埼玉県川口市本町4-1-6",
    "OFFICE_TEL": "048-000-0000",
    "OFFICE_ATTORNEY": "大野太郎",
    "OFFICE_ATTORNEY_REG": "12345",
    "OFFICE_BAR_ASSOCIATION": "埼玉弁護士会",
}

MUNI_STUB = {"市区町村名": {"value": "川口市"}}


def sample_record():
    return {"$id": {"value": "9"}, "顧客名表示用": {"value": "山田太郎"},
            "宛先名": {"value": ""}, "件名": {"value": "職務上請求（川口市）"}}


def sample_data(**over):
    data = {
        "request_items": [{"type": "戸籍謄本", "count": 2}],
        "target": {"本籍": "埼玉県川口市青木2-1-1", "住所": "埼玉県川口市青木2-1-1",
                   "筆頭者": "山田一郎", "世帯主": "山田一郎",
                   "対象者": "山田花子", "フリガナ": "ヤマダ　ハナコ",
                   "生年月日": "昭和25年3月15日"},
        "purpose": "受任事件（消滅時効援用）の通知書送付先調査のため" * 2,
    }
    data.update(over)
    return data


def by_coord(items):
    return {(i.x_mm, i.y_mm): i for i in items}


class TestFormAssignment(unittest.TestCase):
    """様式の自動判定"""

    def test_all_types_assigned_to_a_form(self):
        self.assertEqual(set(sk.FORM_BY_TYPE), set(sk.FEE_FIELD_BY_TYPE))

    def test_koseki_types_are_form1(self):
        for t in ("戸籍謄本", "除籍謄本", "改製原戸籍"):
            self.assertEqual(sk.FORM_BY_TYPE[t], FORM1, t)
            self.assertIn(t, sk.FORM1_TYPE_CIRCLE)

    def test_juminhyo_types_are_form2(self):
        for t in ("住民票", "住民票の除票", "戸籍の附票"):
            self.assertEqual(sk.FORM_BY_TYPE[t], FORM2, t)
            self.assertIn(t, sk.FORM2_TYPE_KEYS)

    def test_fee_fields_consistent_with_forms(self):
        """手数料との整合: 附票300円（手数料_附票）は様式2側"""
        self.assertEqual(sk.FORM_BY_TYPE["戸籍の附票"], FORM2)
        self.assertEqual(sk.FEE_FIELD_BY_TYPE["戸籍の附票"], "手数料_附票")
        for t, form in sk.FORM_BY_TYPE.items():
            if form == FORM1:
                self.assertIn(sk.FEE_FIELD_BY_TYPE[t],
                              ("手数料_戸籍謄本", "手数料_除籍改製原"), t)
            else:
                self.assertIn(sk.FEE_FIELD_BY_TYPE[t],
                              ("手数料_住民票", "手数料_附票"), t)


class TestCoordTables(unittest.TestCase):
    def test_coords_within_page_and_unique(self):
        w, h = sk.FORM_SIZE_MM
        for form, coords in sk.FORM_COORDS_BY_FORM.items():
            values = list(coords.values())
            self.assertEqual(len(values), len(set(values)), f"{form}: 座標の重複")
            for key, (x, y) in coords.items():
                self.assertTrue(0 <= x <= w and 0 <= y <= h, f"{form}/{key} がページ外")

    def test_form_specific_keys(self):
        self.assertIn("種別丸_原戸籍", sk.FORM1_COORDS)
        self.assertIn("該当号チェック_1号", sk.FORM1_COORDS)
        self.assertNotIn("種別丸_原戸籍", sk.FORM2_COORDS)
        self.assertIn("基礎証明チェック_本籍国籍", sk.FORM2_COORDS)
        self.assertIn("種別丸_附票の写し", sk.FORM2_COORDS)
        self.assertNotIn("基礎証明チェック_本籍国籍", sk.FORM1_COORDS)

    def test_calibration_pdf_per_form(self):
        """様式別キャリブレーション PDF（完了条件: グリッド2枚）"""
        pdf1 = sk.build_calibration_pdf(FORM1)
        pdf2 = sk.build_calibration_pdf(FORM2)
        self.assertTrue(pdf1.startswith(b"%PDF"))
        self.assertTrue(pdf2.startswith(b"%PDF"))
        self.assertNotEqual(pdf1, pdf2, "様式ごとに別の座標キーが印字される")


class TestForm1Items(unittest.TestCase):
    """様式第1号（戸籍謄本等）の配置"""

    def setUp(self):
        patcher = patch.dict(os.environ, OFFICE_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)

    def items(self, data=None, req=None):
        data = data or sample_data()
        req = req or data["request_items"][0]
        return sk.build_form1_items(sample_record(), data, MUNI_STUB, req)

    def test_type_circle_and_count(self):
        bc = by_coord(self.items())
        self.assertEqual(bc[sk.FORM1_COORDS["種別丸_戸籍"]].text, sk.CIRCLE_MARK)
        self.assertEqual(bc[sk.FORM1_COORDS["種別丸_謄本"]].text, sk.CIRCLE_MARK)
        self.assertEqual(bc[sk.FORM1_COORDS["通数"]].text, "2")
        self.assertNotIn(sk.FORM1_COORDS["種別丸_除籍"], bc)
        self.assertNotIn(sk.FORM1_COORDS["種別丸_抄本"], bc)

    def test_addressee_and_target_fields(self):
        bc = by_coord(self.items())
        expect = {
            "宛先自治体名": "川口市",
            "本籍": "埼玉県川口市青木2-1-1",
            "筆頭者氏名": "山田一郎",
            "請求に係る者_フリガナ": "ヤマダ　ハナコ",
            "請求に係る者_氏名": "山田花子",
            "生年月日_年月日": "25年3月15日",
            "依頼者氏名": "山田太郎",
        }
        for key, text in expect.items():
            self.assertEqual(bc[sk.FORM1_COORDS[key]].text, text, key)

    def test_era_circle_wareki(self):
        bc = by_coord(self.items())
        self.assertEqual(bc[sk.FORM1_COORDS["元号丸_昭和"]].text, sk.CIRCLE_MARK)
        for era in ("明治", "大正", "平成", "令和"):
            self.assertNotIn(sk.FORM1_COORDS[f"元号丸_{era}"], bc)

    def test_era_circle_iso_date(self):
        data = sample_data()
        data["target"]["生年月日"] = "1950-03-15"
        bc = by_coord(self.items(data))
        self.assertEqual(bc[sk.FORM1_COORDS["元号丸_昭和"]].text, sk.CIRCLE_MARK)
        self.assertEqual(bc[sk.FORM1_COORDS["生年月日_年月日"]].text, "25年3月15日")

    def test_purpose_kind3_block(self):
        bc = by_coord(self.items())
        self.assertEqual(bc[sk.FORM1_COORDS["利用目的丸_3"]].text, sk.CIRCLE_MARK)
        self.assertEqual(bc[sk.FORM1_COORDS["業務の種類"]].text, "受任事件の処理")
        self.assertEqual(bc[sk.FORM1_COORDS["該当号チェック_1号"]].text, sk.CHECK_MARK)
        self.assertIn(sk.FORM1_COORDS["具体的事由_1行目"], bc)
        self.assertIn(sk.FORM1_COORDS["具体的事由_2行目"], bc)

    def test_requester_off_by_default(self):
        """様式1は請求者欄印字済み在庫のため既定OFF"""
        bc = by_coord(self.items())
        for key in ("請求者_事務所名", "請求者_氏名", "請求者_弁護士会"):
            self.assertNotIn(sk.FORM1_COORDS[key], bc, key)

    def test_requester_override_on(self):
        data = sample_data(print_requester={"form1": True})
        bc = by_coord(self.items(data))
        self.assertEqual(bc[sk.FORM1_COORDS["請求者_事務所名"]].text, "大野法律事務所")
        self.assertEqual(bc[sk.FORM1_COORDS["請求者_弁護士会"]].text, "埼玉弁護士会")
        self.assertEqual(bc[sk.FORM1_COORDS["請求者_登録番号"]].text, "12345")

    def test_messenger_field_never_printed(self):
        data = sample_data(print_requester={"form1": True})
        bc = by_coord(self.items(data))
        self.assertNotIn(sk.FORM1_COORDS["使者_氏名"], bc)


class TestForm2Items(unittest.TestCase):
    """様式第2号（住民票の写し等）の配置"""

    def setUp(self):
        patcher = patch.dict(os.environ, OFFICE_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)

    def items(self, data=None):
        data = data or sample_data(request_items=[
            {"type": "住民票", "count": 1}, {"type": "戸籍の附票", "count": 3}])
        reqs = [r for r in data["request_items"] if sk.FORM_BY_TYPE[r["type"]] == FORM2]
        return sk.build_form2_items(sample_record(), data, MUNI_STUB, reqs)

    def test_type_circles_and_counts(self):
        bc = by_coord(self.items())
        self.assertEqual(bc[sk.FORM2_COORDS["種別丸_住民票の写し"]].text, sk.CIRCLE_MARK)
        self.assertEqual(bc[sk.FORM2_COORDS["通数_住民票"]].text, "1")
        self.assertEqual(bc[sk.FORM2_COORDS["種別丸_附票の写し"]].text, sk.CIRCLE_MARK)
        self.assertEqual(bc[sk.FORM2_COORDS["通数_附票"]].text, "3")
        self.assertNotIn(sk.FORM2_COORDS["種別丸_除票の写し"], bc)

    def test_address_honseki_and_setainushi(self):
        bc = by_coord(self.items())
        self.assertEqual(bc[sk.FORM2_COORDS["住所"]].text, "埼玉県川口市青木2-1-1")
        self.assertEqual(bc[sk.FORM2_COORDS["本籍"]].text, "埼玉県川口市青木2-1-1")
        self.assertEqual(bc[sk.FORM2_COORDS["世帯主筆頭者氏名"]].text, "山田一郎")

    def test_extra_items_checks(self):
        data = sample_data(
            request_items=[{"type": "住民票", "count": 1}],
            extra_items=["本籍又は国籍・地域", "世帯主の氏名及び続柄"])
        bc = by_coord(self.items(data))
        self.assertEqual(bc[sk.FORM2_COORDS["基礎証明チェック_本籍国籍"]].text, sk.CHECK_MARK)
        self.assertEqual(bc[sk.FORM2_COORDS["基礎証明チェック_世帯主氏名続柄"]].text, sk.CHECK_MARK)
        self.assertNotIn(sk.FORM2_COORDS["基礎証明チェック_世帯主の旨"], bc)

    def test_purpose_check_and_content(self):
        bc = by_coord(self.items())
        self.assertEqual(bc[sk.FORM2_COORDS["利用目的チェック_3"]].text, sk.CHECK_MARK)
        self.assertIn(sk.FORM2_COORDS["利用目的の内容_1行目"], bc)
        self.assertEqual(bc[sk.FORM2_COORDS["業務の種類"]].text, "受任事件の処理")

    def test_requester_on_by_default(self):
        bc = by_coord(self.items())
        self.assertEqual(bc[sk.FORM2_COORDS["請求者_事務所名"]].text, "大野法律事務所")

    def test_requester_override_off(self):
        data = sample_data(request_items=[{"type": "住民票", "count": 1}],
                           print_requester={"form2": False})
        bc = by_coord(self.items(data))
        self.assertNotIn(sk.FORM2_COORDS["請求者_事務所名"], bc)

    def test_missing_birthdate_is_ok(self):
        """様式2: 生年月日欠損→印字しないだけで正常生成（完了条件）"""
        data = sample_data(request_items=[{"type": "住民票", "count": 1}])
        del data["target"]["生年月日"]
        bc = by_coord(self.items(data))
        for era in ("明治", "大正", "昭和", "平成", "令和"):
            self.assertNotIn(sk.FORM2_COORDS[f"元号丸_{era}"], bc)
        self.assertNotIn(sk.FORM2_COORDS["生年月日_年月日"], bc)


class TestBirthdateBranch(unittest.TestCase):
    """生年月日の様式別バリデーション（完了条件の中核）"""

    def test_form1_missing_birthdate_raises(self):
        data = sample_data()
        data["target"]["生年月日"] = ""
        with self.assertRaises(ShokumuSeikyuError) as ctx:
            sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        self.assertIn("生年月日が必要です", str(ctx.exception))
        self.assertIn("様式第1号", str(ctx.exception))

    def test_form2_only_missing_birthdate_generates(self):
        data = sample_data(request_items=[{"type": "住民票の除票", "count": 1}])
        del data["target"]["生年月日"]
        with patch.dict(os.environ, OFFICE_ENV):
            pdfs = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        self.assertEqual([n for n, _ in pdfs], ["職務上請求書_様式2_住民票等.pdf"])
        self.assertTrue(pdfs[0][1].startswith(b"%PDF"))

    def test_mixed_request_missing_birthdate_raises(self):
        """様式1を1点でも含めば生年月日必須"""
        data = sample_data(request_items=[{"type": "住民票", "count": 1},
                                          {"type": "除籍謄本", "count": 1}])
        data["target"].pop("生年月日")
        with self.assertRaises(ShokumuSeikyuError) as ctx:
            sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        self.assertIn("除籍謄本", str(ctx.exception))


class TestFormPdfs(unittest.TestCase):
    def setUp(self):
        patcher = patch.dict(os.environ, OFFICE_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_form1_one_sheet_per_type(self):
        """様式1は「いずれかに○」のため種別ごとに1枚"""
        data = sample_data(request_items=[{"type": "戸籍謄本", "count": 1},
                                          {"type": "改製原戸籍", "count": 2}])
        pdfs = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        self.assertEqual([n for n, _ in pdfs],
                         ["職務上請求書_様式1_戸籍謄本.pdf",
                          "職務上請求書_様式1_改製原戸籍.pdf"])

    def test_mixed_forms_generate_both_sheets(self):
        """戸籍＋附票のような混在は様式1と様式2の2枚"""
        data = sample_data(request_items=[{"type": "戸籍謄本", "count": 1},
                                          {"type": "戸籍の附票", "count": 1}])
        pdfs = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        self.assertEqual([n for n, _ in pdfs],
                         ["職務上請求書_様式1_戸籍謄本.pdf",
                          "職務上請求書_様式2_住民票等.pdf"])
        for _, pdf in pdfs:
            self.assertTrue(pdf.startswith(b"%PDF"))

    def test_determinism_and_grid(self):
        data = sample_data()
        a = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        b = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        self.assertEqual(a, b, "同一入力→同一バイト列（invariant）")
        grid = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB, grid=True)
        self.assertNotEqual(a[0][1], grid[0][1])
        self.assertGreater(len(grid[0][1]), len(a[0][1]), "方眼のぶん大きくなる")

    def test_print_offset_env_shifts_output(self):
        data = sample_data()
        base = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        with patch.dict(os.environ, {"PRINT_OFFSET_X_MM": "1.5", "PRINT_OFFSET_Y_MM": "-2"}):
            shifted = sk.build_request_form_pdfs(sample_record(), data, MUNI_STUB)
        self.assertNotEqual(base[0][1], shifted[0][1])


class TestBirthdateParsing(unittest.TestCase):
    def test_split_variants(self):
        cases = {
            "昭和25年3月15日": ("昭和", "25年3月15日"),
            "令和元年5月1日": ("令和", "元年5月1日"),
            "1950-03-15": ("昭和", "25年3月15日"),
            "2019-05-01": ("令和", "元年5月1日"),
            "1989-01-07": ("昭和", "64年1月7日"),
            "1989-01-08": ("平成", "元年1月8日"),
            "S25.3.15": ("", "S25.3.15"),  # 判定不能は原文のまま（丸なし）
            "": ("", ""),
        }
        for raw, expected in cases.items():
            self.assertEqual(sk._split_birthdate(raw), expected, raw)


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


def _muni_record():
    return {"市区町村名": {"value": "川口市"}, "都道府県": {"value": "埼玉県"},
            "担当部署": {"value": "市民課"}, "郵便番号": {"value": "332-8601"},
            "住所": {"value": "埼玉県川口市青木2-1-1"}, "備考": {"value": ""},
            "手数料_戸籍謄本": {"value": "450"}, "手数料_除籍改製原": {"value": "750"},
            "手数料_附票": {"value": "300"}, "手数料_住民票": {"value": "300"}}


def _shipping_record(items):
    data = {"request_items": items, "municipality": "川口市",
            "target": {"対象者": "山田花子", "本籍": "埼玉県川口市…",
                       "生年月日": "昭和25年3月15日"},
            "purpose": "送付先調査のため"}
    return {"$id": {"value": "9"}, "発送ステータス": {"value": "下書き"},
            "チャネル": {"value": "職務上請求"}, "件名": {"value": "職務上請求"},
            "顧客名表示用": {"value": "山田太郎"}, "宛先名": {"value": ""},
            "チャネル固有データ": {"value": json.dumps(data, ensure_ascii=False)}}


class TestPrepareIntegration(unittest.IsolatedAsyncioTestCase):
    """prepare 成果物への統合"""

    async def test_artifacts_mixed_forms(self):
        items = [{"type": "戸籍謄本", "count": 1}, {"type": "住民票の除票", "count": 1}]
        with patch.dict(os.environ, OFFICE_ENV), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=[_muni_record()])):
            result = await sk.ShokumuSeikyuAdapter().prepare(_shipping_record(items))
        names = [a.filename for a in result.artifacts]
        self.assertEqual(names, ["発送準備チェックリスト.pdf",
                                 "職務上請求書_様式1_戸籍謄本.pdf",
                                 "職務上請求書_様式2_住民票等.pdf",
                                 "レターパック往復ラベル.pdf"])
        for a in result.artifacts:
            self.assertTrue(a.content.startswith(b"%PDF"), a.filename)
        self.assertIn(b"/Count 2", result.artifacts[-1].content)

    async def test_form1_missing_birthdate_raises_from_prepare(self):
        """様式1で生年月日欠損 → prepare がエラー（ディスパッチャがエラー遷移＋警報）"""
        rec = _shipping_record([{"type": "戸籍謄本", "count": 1}])
        data = json.loads(rec["チャネル固有データ"]["value"])
        del data["target"]["生年月日"]
        rec["チャネル固有データ"]["value"] = json.dumps(data, ensure_ascii=False)
        with patch.dict(os.environ, OFFICE_ENV), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=[_muni_record()])):
            with self.assertRaises(ShokumuSeikyuError) as ctx:
                await sk.ShokumuSeikyuAdapter().prepare(rec)
        self.assertIn("生年月日が必要です", str(ctx.exception))

    async def test_artifacts_degrade_without_office(self):
        """事務所情報未設定: 返信面なしの宛名ラベルに縮退（prepare は止めない）"""
        env = {k: "" for k in OFFICE_ENV}
        items = [{"type": "住民票", "count": 1}]
        with patch.dict(os.environ, env), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=[_muni_record()])):
            result = await sk.ShokumuSeikyuAdapter().prepare(_shipping_record(items))
        names = [a.filename for a in result.artifacts]
        self.assertEqual(names, ["発送準備チェックリスト.pdf",
                                 "職務上請求書_様式2_住民票等.pdf",
                                 "レターパック宛名ラベル.pdf"])
        self.assertNotIn(b"/Count 2", result.artifacts[-1].content)

    async def test_kogawase_breakdown_tags_form(self):
        """手数料明細に様式が付記される（手数料計算との整合）"""
        total, lines = sk.compute_kogawase(
            [{"type": "戸籍の附票", "count": 1}, {"type": "戸籍謄本", "count": 1}],
            _muni_record())
        self.assertEqual(total, 750)
        self.assertIn("戸籍の附票 1通 × 300円 = 300円（様式第2号）", lines[0])
        self.assertIn("戸籍謄本 1通 × 450円 = 450円（様式第1号）", lines[1])


if __name__ == "__main__":
    unittest.main()
