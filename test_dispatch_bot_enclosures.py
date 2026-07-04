"""同封物選択（2026-07-04 実機エラー対応）のテスト

事象: D3 は同封物選択を空のまま起票し prepare が「同封物が選択されていません」で
エラー遷移した。修正の固定内容:
- 同封物は必須。聞き返しは App 32 の有効ブロックから動的な番号選択式（ハードコードなし）
- 指示文に書類名が含まれ App 32 と照合できればスキップ
- 復唱に同封物（表示名）を含め、起票時は 同封物選択 にブロックキーを設定
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")
os.environ.setdefault("APP_SHIPPING", "30")
os.environ.setdefault("TOKEN_SHIPPING", "dummy")

from dispatch_bot import case_search, enclosures, handler, parser  # noqa: E402
from dispatch_bot.enclosures import EnclosureOption  # noqa: E402

OPTIONS = [EnclosureOption(key="委任契約書", label="委任契約書"),
           EnclosureOption(key="返信用封筒", label="返信用封筒")]


def parsed(**over):
    base = {"intent": "task", "task_type": "soufu_annai",
            "customer_name": "鈴木", "task_params": {},
            "confidence": "high", "missing_fields": [], "clarification": None}
    base.update(over)
    return base


def hit(rid="45", name="鈴木一郎", status="受任"):
    return case_search.CaseHit(record_id=rid, customer_name=name, status=status)


CASE_RECORD = {"顧客名": {"value": "鈴木一郎"},
               "住所": {"value": "埼玉県川口市本町1-2-3"},
               "郵便番号": {"value": "332-0012"}}


class Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def patches(self, parse_results, options=None, hits=None):
        seq = parse_results if isinstance(parse_results, list) else [parse_results]
        self.create_mock = AsyncMock(return_value="101")
        self.options_mock = AsyncMock(
            return_value=list(OPTIONS) if options is None else options)
        return [
            patch.object(parser, "parse_instruction", new=AsyncMock(side_effect=seq)),
            patch.object(enclosures, "list_options", new=self.options_mock),
            patch.object(case_search, "search_cases",
                         new=AsyncMock(return_value=hits or [hit()])),
            patch("hub.kintone.get_record", new=AsyncMock(return_value=CASE_RECORD)),
            patch("hub.kintone.search_records", new=AsyncMock(return_value=[])),
            patch("hub.kintone.create_record", new=self.create_mock),
        ]

    async def send(self, text, user="U1"):
        return await handler.handle_message(user, text)


class TestAskFlow(Base):
    async def test_ask_numbered_then_answer_then_filing(self):
        """一巡: 同封物聞き返し（動的番号選択）→番号回答→復唱→OK→起票"""
        ps = self.patches([parsed(), parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            r1 = await self.send("鈴木さんに送付案内を作って")
            self.assertEqual(
                r1,
                "同封する書類を番号で選んでください（複数可・カンマ区切り）\n"
                "1. 委任契約書\n2. 返信用封筒",
                "選択肢は App 32 から動的に組み立て（ハードコードなし）")
            r2 = await self.send("1,2")
            self.assertIn("に送付案内（委任契約書・返信用封筒）を起票します。", r2)
            self.assertIn("OK / キャンセル", r2)
            r3 = await self.send("OK")
        self.assertIn("起票しました", r3)
        fields = self.create_mock.await_args.args[1]
        self.assertEqual(fields["同封物選択"], ["委任契約書", "返信用封筒"],
                         "同封物選択にはブロックキーを設定")

    async def test_full_width_comma_and_single_number(self):
        ps = self.patches([parsed()])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            await self.send("鈴木さんに送付案内")
            r = await self.send("2")
        self.assertIn("送付案内（返信用封筒）を起票します。", r)

    async def test_invalid_number_reprompts(self):
        """無効な番号は再案内（選択肢は維持・言い直しにならない）"""
        ps = self.patches([parsed()])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            await self.send("鈴木さんに送付案内")
            r = await self.send("9")
            self.assertEqual(r, "1〜2 の番号で選んでください（複数はカンマ区切り）")
            r2 = await self.send("1")
        self.assertIn("送付案内（委任契約書）を起票します。", r2)

    async def test_no_options_guides_to_kintone(self):
        """App 32 に有効ブロックが0件 → 案内して終了"""
        ps = self.patches([parsed()], options=[])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            reply = await self.send("鈴木さんに送付案内")
        self.assertEqual(reply, enclosures.MSG_NO_OPTIONS)
        self.create_mock.assert_not_awaited()


class TestSkipWhenSpecified(Base):
    async def test_instruction_with_enclosure_skips_question(self):
        """指示文に同封物が含まれ App 32 と照合できれば聞き返しスキップ"""
        ps = self.patches([parsed(task_params={"enclosures": ["委任契約書"]}),
                           parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            r1 = await self.send("鈴木さんに委任契約書を送って")
            self.assertIn("送付案内（委任契約書）を起票します。", r1,
                          "聞き返しなしで復唱に進む")
            r2 = await self.send("OK")
        fields = self.create_mock.await_args.args[1]
        self.assertEqual(fields["同封物選択"], ["委任契約書"])

    async def test_unmatched_creative_names_fall_back_to_question(self):
        """App 32 と照合できない書類名（モデル創作等）は採用せず聞き返しへ"""
        ps = self.patches([parsed(task_params={"enclosures": ["請求書", "送付日"]})])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            reply = await self.send("鈴木さんに請求書を送って")
        self.assertIn("同封する書類を番号で選んでください", reply)
        self.assertNotIn("請求書", reply, "創作された名前が選択肢に混入しない")

    async def test_label_key_mapping(self):
        """表示名≠キーの場合: 選択肢と復唱は表示名・起票はキー"""
        opts = [EnclosureOption(key="委任契約書", label="委任契約書一式")]
        ps = self.patches([parsed(task_params={"enclosures": ["委任契約書一式"]}),
                           parsed(intent="confirm")], options=opts)
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5]:
            r1 = await self.send("鈴木さんに委任契約書一式を送って")
            self.assertIn("送付案内（委任契約書一式）を起票します。", r1, "復唱は表示名")
            await self.send("OK")
        fields = self.create_mock.await_args.args[1]
        self.assertEqual(fields["同封物選択"], ["委任契約書"], "起票はブロックキー")


class TestMatchNames(unittest.TestCase):
    def test_match_by_label_and_key_dedupe(self):
        opts = [EnclosureOption(key="k1", label="委任契約書"),
                EnclosureOption(key="k2", label="返信用封筒")]
        matched = enclosures.match_names(["委任契約書", "k1", "架空の書類"], opts)
        self.assertEqual([o.key for o in matched], ["k1"], "表示名/キー両対応・重複除去・創作除外")

    def test_registry_requires_enclosures(self):
        from dispatch_bot import registry
        spec = registry.get_task("soufu_annai")
        self.assertIn("enclosures", spec.required_fields)


if __name__ == "__main__":
    unittest.main()
