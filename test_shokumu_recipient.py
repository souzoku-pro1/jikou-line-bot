"""職務上請求の宛先表示: 役所名の接尾辞（2026-07-04 修正タスク)

- 市区町村名→施設名の導出（市/区→役所・町/村→役場・該当なしはそのまま）
- 適用範囲: App 30 宛先名の自動入力・チェックリスト表示（＝レターパックラベルは
  宛先名フィールドを使うため自動的に施設名になる）
- 統一用紙の「○○長 殿」（宛先自治体名）は変更しない
- 手入力の宛先名が優先される既存仕様の維持
"""

import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from channels import shokumu_seikyu as sk
from channels.shokumu_seikyu import municipality_office_name

OFFICE_ENV = {
    "OFFICE_NAME": "大野法律事務所", "OFFICE_ZIP": "332-0012",
    "OFFICE_ADDRESS": "埼玉県川口市本町4-1-6", "OFFICE_TEL": "048-000-0000",
    "OFFICE_ATTORNEY": "大野太郎",
}


def muni(name="川口市", dept="市民課"):
    return {"市区町村名": {"value": name}, "都道府県": {"value": "埼玉県"},
            "担当部署": {"value": dept}, "郵便番号": {"value": "332-8601"},
            "住所": {"value": "埼玉県川口市青木2-1-1"}, "備考": {"value": ""},
            "手数料_戸籍謄本": {"value": "450"}, "手数料_除籍改製原": {"value": "750"},
            "手数料_附票": {"value": "300"}, "手数料_住民票": {"value": "300"}}


def record(recipient=""):
    data = {"request_items": [{"type": "戸籍謄本", "count": 1}],
            "municipality": "川口市",
            "target": {"対象者": "山田花子", "本籍": "埼玉県川口市…",
                       "生年月日": "昭和25年3月15日"},
            "purpose": "送付先調査のため"}
    return {"$id": {"value": "9"}, "発送ステータス": {"value": "下書き"},
            "チャネル": {"value": "職務上請求"}, "件名": {"value": "職務上請求"},
            "顧客名表示用": {"value": "山田太郎"}, "宛先名": {"value": recipient},
            "チャネル固有データ": {"value": json.dumps(data, ensure_ascii=False)}}


class TestOfficeName(unittest.TestCase):
    """市/区/町/村の4パターン＋該当なし"""

    def test_patterns(self):
        cases = {
            "川口市": "川口市役所",
            "蕨市": "蕨市役所",
            "千代田区": "千代田区役所",
            "伊奈町": "伊奈町役場",
            "檜原村": "檜原村役場",
        }
        for name, expected in cases.items():
            self.assertEqual(municipality_office_name(name), expected, name)

    def test_no_match_returns_as_is(self):
        for name in ("東京都", "色丹郡色丹", ""):
            self.assertEqual(municipality_office_name(name), name)

    def test_whitespace_stripped(self):
        self.assertEqual(municipality_office_name(" 川口市 "), "川口市役所")


class TestPrepareRecipient(unittest.IsolatedAsyncioTestCase):
    async def _prepare(self, rec, muni_rec):
        with patch.dict(os.environ, OFFICE_ENV), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=[muni_rec])):
            return await sk.ShokumuSeikyuAdapter().prepare(rec)

    async def test_auto_recipient_uses_office_name(self):
        """App 30 宛先名の自動入力が施設名になる（→ラベルも同じ値を使う）"""
        result = await self._prepare(record(), muni())
        self.assertEqual(result.fields["宛先名"], "川口市役所　市民課")

    async def test_auto_recipient_without_dept(self):
        result = await self._prepare(record(), muni(dept=""))
        self.assertEqual(result.fields["宛先名"], "川口市役所")

    async def test_town_suffix(self):
        result = await self._prepare(record(), muni(name="伊奈町", dept="住民課"))
        self.assertEqual(result.fields["宛先名"], "伊奈町役場　住民課")

    async def test_manual_recipient_takes_precedence(self):
        """手入力の宛先名はそのまま（既存仕様の維持）"""
        result = await self._prepare(record(recipient="川口市　市民課戸籍係"), muni())
        self.assertEqual(result.fields["宛先名"], "川口市　市民課戸籍係")


class TestChecklistDisplay(unittest.TestCase):
    def test_checklist_addressee_uses_office_name(self):
        data = {"target": {"対象者": "山田花子"}, "purpose": "調査"}
        lines = sk._checklist_lines(record(), muni(), data, 450,
                                    ["戸籍謄本 1通 × 450円 = 450円"])
        self.assertIn("宛先: 川口市役所 市民課", lines)
        self.assertFalse(any(l.startswith("宛先: 川口市 ") for l in lines),
                         "旧表記（施設名なし）が残っていない")


class TestFormUnchanged(unittest.TestCase):
    """統一用紙の重ね打ち（○○長 殿）は自治体名のまま（変更しない）"""

    def setUp(self):
        patcher = patch.dict(os.environ, OFFICE_ENV)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _data(self, items):
        return {"request_items": items,
                "target": {"対象者": "山田花子", "本籍": "…", "住所": "…",
                           "生年月日": "昭和25年3月15日"},
                "purpose": "調査"}

    def test_form1_addressee_is_municipality_name(self):
        items = sk.build_form1_items(record(), self._data([{"type": "戸籍謄本", "count": 1}]),
                                     muni(), {"type": "戸籍謄本", "count": 1})
        by_coord = {(i.x_mm, i.y_mm): i for i in items}
        self.assertEqual(by_coord[sk.FORM1_COORDS["宛先自治体名"]].text, "川口市",
                         "「川口市長 殿」のままが正しい（川口市役所長にしない）")

    def test_form2_addressee_is_municipality_name(self):
        reqs = [{"type": "住民票", "count": 1}]
        items = sk.build_form2_items(record(), self._data(reqs), muni(), reqs)
        by_coord = {(i.x_mm, i.y_mm): i for i in items}
        self.assertEqual(by_coord[sk.FORM2_COORDS["宛先自治体名"]].text, "川口市")


if __name__ == "__main__":
    unittest.main()
