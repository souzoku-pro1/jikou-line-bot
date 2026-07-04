"""弁護士判断（2026-07-04）の固定: 職務上請求の対象者特定要件

判断: 対象者の特定に必須なのは氏名＋生年月日（様式1）。本籍は「あれば書く」情報で
必須ではない（不明・空欄でも自治体側で検索・交付されるのが通常運用。附票請求は
本籍不明だからこそ行うケースもある）。住所（様式2）も同様。

固定内容:
- 本籍・住所は聞き返しの必須項目に含まれない（本籍なしで復唱→起票まで通る）
- 指示文に本籍・住所があれば従来どおり抽出して起票JSONに入る（要求だけしない）
- 復唱では本籍が空のとき行を省略（「未記入」等の表示もしない）
"""

import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")
os.environ.setdefault("APP_SHIPPING", "30")
os.environ.setdefault("TOKEN_SHIPPING", "dummy")

from channels.shokumu_seikyu import parse_channel_data  # noqa: E402
from dispatch_bot import case_search, handler, parser, shokumu  # noqa: E402

MUNI_RECORD = {"市区町村名": {"value": "川口市"}, "担当部署": {"value": "市民課"},
               "郵便番号": {"value": "332-8601"}, "住所": {"value": "埼玉県川口市青木2-1-1"},
               "手数料_戸籍謄本": {"value": "450"}, "手数料_除籍改製原": {"value": "750"},
               "手数料_附票": {"value": "300"}, "手数料_住民票": {"value": "300"},
               "備考": {"value": ""}}


def parsed(**over):
    base = {"intent": "task", "task_type": "shokumu_seikyu",
            "customer_name": "佐藤", "task_params": {},
            "confidence": "high", "missing_fields": [], "clarification": None}
    base.update(over)
    return base


def hit():
    return case_search.CaseHit(record_id="45", customer_name="佐藤花子", status="受任")


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def patches(self, parse_results):
        seq = parse_results if isinstance(parse_results, list) else [parse_results]
        self.create_mock = AsyncMock(return_value="401")

        async def fake_search(app, query, fields=None):
            return [MUNI_RECORD] if "市区町村名" in query else []

        return [
            patch.object(parser, "parse_instruction", new=AsyncMock(side_effect=seq)),
            patch.object(case_search, "search_cases", new=AsyncMock(return_value=[hit()])),
            patch("hub.kintone.search_records", new=AsyncMock(side_effect=fake_search)),
            patch("hub.kintone.get_record",
                  new=AsyncMock(return_value={"顧客名": {"value": "佐藤花子"},
                                              "住所": {"value": "X"},
                                              "郵便番号": {"value": ""}})),
            patch("hub.kintone.create_record", new=self.create_mock),
        ]


class TestHonsekiNotRequired(Base):
    def test_first_missing_never_asks_honseki_or_address(self):
        """氏名＋生年月日がそろえば本籍・住所なしで不足なし（様式1）"""
        params = shokumu.normalize_params(
            {"request_items": [{"type": "戸籍謄本", "count": 1},
                               {"type": "戸籍の附票", "count": 1}],
             "municipality": "川口市",
             "target": {"対象者": "佐藤太郎", "生年月日": "昭和25年3月15日"}})
        params["unit"] = "時効援用"
        self.assertIsNone(shokumu.first_missing({"task_params": params}),
                          "本籍なしでも聞き返しは発生しない")

    def test_form2_address_not_required(self):
        """様式2（住民票）も住所なしで不足なし"""
        params = shokumu.normalize_params(
            {"request_items": [{"type": "住民票", "count": 1}],
             "municipality": "川口市", "target": {"対象者": "佐藤太郎"}})
        params["unit"] = "時効援用"
        self.assertIsNone(shokumu.first_missing({"task_params": params}))

    async def test_full_cycle_without_honseki(self):
        """本籍なしで復唱→OK→起票まで通り、復唱に本籍の行が出ない"""
        params = {"request_items": [{"type": "戸籍謄本", "count": 2}],
                  "municipality": "川口市",
                  "target": {"対象者": "佐藤太郎", "生年月日": "昭和25年3月15日"}}
        ps = self.patches([parsed(task_params=params), parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            r1 = await handler.handle_message("U1", "佐藤さんの戸籍謄本2通を川口市に")
            self.assertIn("【確認】以下で起票します", r1)
            self.assertNotIn("本籍", r1, "本籍が空なら行ごと省略（未記入表示もしない）")
            r2 = await handler.handle_message("U1", "OK")
        self.assertIn("起票しました", r2)
        fields = self.create_mock.await_args.args[1]
        data = parse_channel_data({"チャネル固有データ":
                                   {"value": fields["チャネル固有データ"]}})
        self.assertNotIn("本籍", data["target"], "空の本籍キーを作らない")

    async def test_honseki_in_instruction_is_extracted_and_filed(self):
        """指示文に本籍があれば従来どおり抽出→起票JSONに入る（要求だけしない）"""
        params = {"request_items": [{"type": "戸籍謄本", "count": 1}],
                  "municipality": "川口市",
                  "target": {"対象者": "佐藤太郎", "生年月日": "昭和25年3月15日",
                             "本籍": "埼玉県川口市青木9丁目99番"}}
        ps = self.patches([parsed(task_params=params), parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            await handler.handle_message(
                "U1", "佐藤さんの戸籍謄本1通を川口市に（本籍: 埼玉県川口市青木9丁目99番）")
            await handler.handle_message("U1", "OK")
        fields = self.create_mock.await_args.args[1]
        data = json.loads(fields["チャネル固有データ"])
        self.assertEqual(data["target"]["本籍"], "埼玉県川口市青木9丁目99番")

    def test_question_texts_do_not_mention_honseki(self):
        """聞き返し文言のどれも本籍を要求しない"""
        for key, q in shokumu.QUESTIONS.items():
            self.assertNotIn("本籍", q, f"QUESTIONS[{key}] が本籍を要求している")


if __name__ == "__main__":
    unittest.main()
