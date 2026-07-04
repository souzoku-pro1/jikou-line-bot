"""D4（第1.5弾）: 職務上請求のLINE対応テスト

- 聞き返し多段（1論点1往復×必要項目・同一論点の再質問1回まで・全体8往復打ち切り）
- 一括抽出スキップ／様式2のみで生年月日を聞かない／App 31未登録の選択分岐
- 復唱フルテンプレ（対象者・種別と通数・宛先自治体・小為替概算・kintone承認注記）
- 起票JSONが channels/shokumu_seikyu.parse_channel_data を実際に通ること
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
os.environ.setdefault("APP_CITY_MASTER", "31")
os.environ.setdefault("TOKEN_CITY_MASTER", "dummy")

from channels.shokumu_seikyu import parse_channel_data  # noqa: E402
from dispatch_bot import case_search, handler, parser, registry, shokumu  # noqa: E402

MUNI_RECORD = {"市区町村名": {"value": "川口市"}, "担当部署": {"value": "市民課"},
               "郵便番号": {"value": "332-8601"}, "住所": {"value": "埼玉県川口市青木2-1-1"},
               "手数料_戸籍謄本": {"value": "450"}, "手数料_除籍改製原": {"value": "750"},
               "手数料_附票": {"value": "300"}, "手数料_住民票": {"value": "300"},
               "備考": {"value": ""}}

CASE_RECORD = {"顧客名": {"value": "佐藤花子"},
               "住所": {"value": "埼玉県川口市本町1-2-3"},
               "郵便番号": {"value": "332-0012"}}

FULL_PARAMS = {"request_items": [{"type": "戸籍謄本", "count": 2},
                                 {"type": "戸籍の附票", "count": 1}],
               "municipality": "川口市",
               "target": {"対象者": "佐藤太郎", "生年月日": "昭和25年3月15日"}}


def parsed(**over):
    base = {"intent": "task", "task_type": "shokumu_seikyu",
            "customer_name": "佐藤", "task_params": {},
            "confidence": "high", "missing_fields": [], "clarification": None}
    base.update(over)
    return base


def hit(rid="45", name="佐藤花子", status="受任"):
    return case_search.CaseHit(record_id=rid, customer_name=name, status=status)


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def patches(self, parse_results, muni=MUNI_RECORD, hits=None):
        seq = parse_results if isinstance(parse_results, list) else [parse_results]
        self.create_mock = AsyncMock(return_value="201")

        async def fake_search(app, query, fields=None):
            if "市区町村名" in query:           # App 31 照合
                return [muni] if muni else []
            return []                            # App 30 重複ガード

        self.search_mock = AsyncMock(side_effect=fake_search)
        return [
            patch.object(parser, "parse_instruction", new=AsyncMock(side_effect=seq)),
            patch.object(case_search, "search_cases",
                         new=AsyncMock(return_value=hits or [hit()])),
            patch("hub.kintone.get_record", new=AsyncMock(return_value=CASE_RECORD)),
            patch("hub.kintone.search_records", new=self.search_mock),
            patch("hub.kintone.create_record", new=self.create_mock),
        ]

    async def send(self, text, user="U1"):
        return await handler.handle_message(user, text)


class TestParamAudit(unittest.TestCase):
    """第一工程: 洗い出し結果がレジストリ・判定関数に反映されていること"""

    def test_registry_entry(self):
        spec = registry.get_task("shokumu_seikyu")
        self.assertEqual(spec.risk, "中")
        self.assertEqual(spec.destination, "app30")
        self.assertEqual(spec.max_clarify, 8)
        for key in ("request_items", "municipality", "target_name", "birth_date"):
            self.assertIn(key, spec.field_questions)

    def test_missing_order_and_form_branch(self):
        """聞く順: 種別通数→自治体→対象者→生年月日（様式1のみ）"""
        p = lambda tp: {"task_params": shokumu.normalize_params(tp)}
        self.assertEqual(shokumu.first_missing(p({})), "request_items")
        self.assertEqual(shokumu.first_missing(
            p({"request_items": [{"type": "戸籍謄本", "count": 1}]})), "municipality")
        self.assertEqual(shokumu.first_missing(
            p({"request_items": [{"type": "戸籍謄本", "count": 1}],
               "municipality": "川口市"})), "target_name")
        # 様式1（戸籍系）を含む → 生年月日必須
        self.assertEqual(shokumu.first_missing(
            p({"request_items": [{"type": "戸籍謄本", "count": 1}],
               "municipality": "川口市", "target": {"対象者": "佐藤太郎"}})), "birth_date")
        # 様式2のみ（住民票）→ 生年月日は聞かない
        self.assertIsNone(shokumu.first_missing(
            p({"request_items": [{"type": "住民票", "count": 1}],
               "municipality": "川口市", "target": {"対象者": "佐藤太郎"}})))

    def test_normalize_drops_invalid_items(self):
        out = shokumu.normalize_params(
            {"request_items": [{"type": "パスポート", "count": 1},
                               {"type": "戸籍謄本", "count": 0},
                               {"type": "戸籍謄本", "count": "2"}]})
        self.assertEqual(out["request_items"], [{"type": "戸籍謄本", "count": 2}],
                         "未対応種別・0通は落ちる・文字列通数は整数化")


class TestFullAskPath(Base):
    async def test_ask_all_items_then_confirm_then_filing(self):
        """全項目を1論点ずつ聞く経路→復唱フル→OK→起票→parse_channel_data通過"""
        seq = [
            parsed(),                                                # Q1 種別通数
            parsed(task_params={"request_items": FULL_PARAMS["request_items"]}),   # Q2 自治体
            parsed(task_params={"request_items": FULL_PARAMS["request_items"],
                                "municipality": "川口市"}),          # Q3 対象者
            parsed(task_params={"request_items": FULL_PARAMS["request_items"],
                                "municipality": "川口市",
                                "target": {"対象者": "佐藤太郎"}}),   # Q4 生年月日
            parsed(task_params=dict(FULL_PARAMS)),                   # 完了→照合→復唱
            parsed(intent="confirm"),
        ]
        ps = self.patches(seq)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            r1 = await self.send("佐藤さんの職務上請求")
            self.assertIn("種別と通数", r1)
            r2 = await self.send("戸籍謄本2通と附票1通")
            self.assertIn("市区町村名", r2)
            r3 = await self.send("川口市")
            self.assertIn("対象者", r3)
            r4 = await self.send("佐藤太郎")
            self.assertIn("生年月日", r4, "様式1（戸籍系）を含むため必須として聞く")
            r5 = await self.send("昭和25年3月15日")
            # 復唱フルテンプレ（⑥）
            self.assertIn("【確認】以下で起票します", r5)
            self.assertIn("案件: No.45 佐藤花子", r5)
            self.assertIn("対象者: 佐藤太郎（昭和25年3月15日生）", r5)
            self.assertIn("請求: 戸籍謄本2通・戸籍の附票1通", r5)
            self.assertIn("宛先自治体: 川口市", r5)
            self.assertIn("小為替概算: 1,200円", r5)
            self.assertIn("発送には kintone での承認が別途必要です", r5)
            self.assertIn("リスク区分: 中", r5)
            r6 = await self.send("OK")

        self.assertIn("起票しました。App 30 No.201", r6)
        fields = self.create_mock.await_args.args[1]
        self.assertEqual(fields["チャネル"], "職務上請求")
        self.assertEqual(fields["発送ステータス"], "下書き")
        self.assertEqual(fields["件名"], "職務上請求（佐藤花子・川口市）")
        self.assertEqual(fields["宛先名"], "", "宛先は prepare が App 31 から解決")
        # 起票JSONが実物 parse_channel_data を通ること（完了条件）
        rec = {"チャネル固有データ": {"value": fields["チャネル固有データ"]}}
        data = parse_channel_data(rec)
        self.assertEqual(data["municipality"], "川口市")
        self.assertEqual(data["purpose"], shokumu.DEFAULT_PURPOSE)
        self.assertIn("dispatch_bot", data, "監査メタ併記")

    async def test_bulk_extraction_skips_questions(self):
        """③一括抽出:「佐藤花子さんの戸籍謄本2通と附票1通を川口市に職務上請求」"""
        seq = [parsed(task_params=dict(FULL_PARAMS)), parsed(intent="confirm")]
        ps = self.patches(seq)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            r1 = await self.send("佐藤花子さんの戸籍謄本2通と附票1通を川口市に職務上請求")
            self.assertIn("【確認】以下で起票します", r1, "聞き返しなしで復唱へ直行")
            r2 = await self.send("OK")
        self.assertIn("起票しました", r2)

    async def test_form2_only_skips_birthdate(self):
        """④様式2のみ（住民票）: 生年月日を聞かずに復唱へ"""
        params = {"request_items": [{"type": "住民票", "count": 1}],
                  "municipality": "川口市", "target": {"対象者": "佐藤太郎"}}
        ps = self.patches([parsed(task_params=params)])
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            reply = await self.send("佐藤さんの住民票1通を川口市に職務上請求")
        self.assertIn("【確認】", reply)
        self.assertNotIn("生年月日を教えてください", reply)


class TestClarifyLimits(Base):
    async def test_same_topic_reasked_once_then_give_up(self):
        """同一論点の再質問は1回まで（3回目が必要になったら打ち切り）"""
        ps = self.patches([parsed()] * 3)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            r1 = await self.send("佐藤さんの職務上請求")
            r2 = await self.send("よくわからない")
            r3 = await self.send("うーん")
        self.assertIn("種別と通数", r1)
        self.assertIn("種別と通数", r2, "再質問1回目は許容")
        self.assertEqual(r3, handler.MSG_GIVE_UP)

    async def test_total_eight_rounds_cap(self):
        """全体8往復で打ち切り（論点が変わり続けても9問目は出さない）"""
        items = FULL_PARAMS["request_items"]
        # 論点を交互に空けて8往復消費させる解析シーケンス
        seq = [
            parsed(),                                            # 1 request_items
            parsed(task_params={"request_items": items}),        # 2 municipality
            parsed(),                                            # 3 request_items(再)
            parsed(task_params={"request_items": items}),        # 4 municipality(再)
            parsed(task_params={"request_items": items,
                                "municipality": "川口市"}),      # 5 target_name
            parsed(task_params={"request_items": items}),        # 6 municipality→topic変化
            parsed(task_params={"request_items": items,
                                "municipality": "川口市"}),      # 7 target_name
            parsed(task_params={"request_items": items}),        # 8 municipality
            parsed(task_params={"request_items": items,
                                "municipality": "川口市"}),      # 9往復目 → 打ち切り
        ]
        ps = self.patches(seq)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            replies = []
            for text in ("佐藤さんの職務上請求", "a", "b", "c", "d", "e", "f", "g", "h"):
                replies.append(await self.send(text))
        self.assertNotEqual(replies[7], handler.MSG_GIVE_UP, "8往復目までは許容")
        self.assertEqual(replies[8], handler.MSG_GIVE_UP, "9往復目は打ち切り")


class TestMunicipalityBranch(Base):
    async def test_unregistered_offers_choice_then_file_anyway(self):
        """⑤App 31未登録→選択肢。「2」でそのまま起票（概算不可の注記付き）"""
        seq = [parsed(task_params=dict(FULL_PARAMS)), parsed(intent="confirm")]
        ps = self.patches(seq, muni=None)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            r1 = await self.send("佐藤さんの戸籍謄本2通を蕨市…")
            self.assertIn("App 31（市区町村マスタ）に未登録です", r1)
            self.assertIn("1. 登録後に再指示する", r1)
            self.assertIn("2. このまま起票する", r1)
            r2 = await self.send("2")
            self.assertIn("【確認】", r2)
            self.assertIn("App 31 未登録・起票後に登録依頼警報", r2)
            self.assertIn("概算不可（自治体未登録）", r2)
            r3 = await self.send("OK")
        self.assertIn("起票しました", r3)

    async def test_unregistered_choice_abort(self):
        """「1」で中止（起票なし）"""
        ps = self.patches([parsed(task_params=dict(FULL_PARAMS))], muni=None)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            await self.send("佐藤さんの戸籍謄本2通を蕨市に")
            reply = await self.send("1")
        self.assertEqual(reply, shokumu.MSG_ABORTED)
        self.create_mock.assert_not_awaited()

    async def test_invalid_choice_reprompts(self):
        ps = self.patches([parsed(task_params=dict(FULL_PARAMS))], muni=None)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            await self.send("佐藤さんの戸籍謄本2通を蕨市に")
            reply = await self.send("3")
        self.assertEqual(reply, "1 か 2 の番号で選んでください")

    async def test_missing_fee_estimate_note(self):
        """登録済みだが手数料未登録 → 概算不能の注記（起票は可能）"""
        muni = dict(MUNI_RECORD, 手数料_附票={"value": ""})
        ps = self.patches([parsed(task_params=dict(FULL_PARAMS))], muni=muni)
        with ps[0], ps[1], ps[2], ps[3], ps[4]:
            reply = await self.send("佐藤さんの戸籍謄本2通と附票1通を川口市に")
        self.assertIn("概算不能（App 31 の手数料未登録", reply)


if __name__ == "__main__":
    unittest.main()
