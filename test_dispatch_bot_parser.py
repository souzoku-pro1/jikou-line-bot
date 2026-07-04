"""D2: 自然言語解析・案件検索・応答組み立てのテスト

- 解析スキーマ検証（tool use 経由・正規化・text応答は不採用）
- 案件検索（1件/複数選択肢/0件と再検索/完了案件⚠）
- 未対応タスク→「第1弾では送付案内のみ」・照会→第2弾案内
- 聞き返し（1論点・回答結合再解析・2往復で打ち切り）
- chat_responder 非import・claude_gateway 経由（context="指示Bot解析"）
"""

import asyncio
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

from claude_gateway import ClaudeUnavailableError  # noqa: E402
from dispatch_bot import case_search, handler, parser, registry  # noqa: E402


def tool_response(**input_data):
    """anthropic レスポンスの代役（tool_use ブロック1つ）"""
    block = SimpleNamespace(type="tool_use", name="parse_instruction", input=input_data)
    return SimpleNamespace(content=[block], stop_reason="tool_use")


def parsed(**over):
    base = {"intent": "task", "task_type": "soufu_annai",
            "customer_name": "鈴木", "task_params": {},
            "confidence": "high", "missing_fields": [], "clarification": None}
    base.update(over)
    return base


def hit(rid="45", name="鈴木一郎", status="受任"):
    return case_search.CaseHit(record_id=rid, customer_name=name, status=status)


def run(coro):
    return asyncio.run(coro)


class TestRegistry(unittest.TestCase):
    def test_only_soufu_annai_registered(self):
        self.assertEqual(set(registry.TASK_REGISTRY), {"soufu_annai"})
        spec = registry.get_task("soufu_annai")
        self.assertFalse(spec.answer_only)
        self.assertEqual(spec.destination, "app30")
        self.assertIn("customer_name", spec.required_fields)

    def test_catalog_feeds_prompt(self):
        catalog = registry.catalog_for_prompt()
        self.assertIn("soufu_annai", catalog)
        self.assertIn("送付案内の作成", catalog)
        self.assertIn(catalog.splitlines()[0], parser.build_system_prompt())

    def test_unknown_task_type_returns_none(self):
        self.assertIsNone(registry.get_task("shokumu_seikyu"))
        self.assertIsNone(registry.get_task(None))


class TestParseInstruction(unittest.IsolatedAsyncioTestCase):
    async def test_tool_use_extracted_and_normalized(self):
        mock = AsyncMock(return_value=tool_response(
            intent="task", task_type="soufu_annai", customer_name=" 鈴木 ",
            confidence="high"))
        with patch.object(parser, "create_message_with_fallback", new=mock):
            result = await parser.parse_instruction("鈴木さんに送付案内を作って")
        self.assertEqual(result["intent"], "task")
        self.assertEqual(result["customer_name"], "鈴木")
        self.assertEqual(result["task_params"], {})
        self.assertEqual(result["missing_fields"], [])
        # claude_gateway 経由・context 指定（確定判断9）
        self.assertEqual(mock.await_args.kwargs["context"], "指示Bot解析")
        self.assertEqual(mock.await_args.kwargs["tool_choice"],
                         {"type": "tool", "name": "parse_instruction"})

    async def test_invalid_enum_falls_back_safely(self):
        mock = AsyncMock(return_value=tool_response(intent="destroy", confidence="huge"))
        with patch.object(parser, "create_message_with_fallback", new=mock):
            result = await parser.parse_instruction("x")
        self.assertEqual(result["intent"], "unknown")
        self.assertEqual(result["confidence"], "low")

    async def test_text_only_response_raises(self):
        """text応答からのJSON切り出しはしない（tool_useが無ければエラー）"""
        resp = SimpleNamespace(
            content=[SimpleNamespace(type="text", text='{"intent":"task"}')],
            stop_reason="end_turn")
        with patch.object(parser, "create_message_with_fallback",
                          new=AsyncMock(return_value=resp)):
            with self.assertRaises(ValueError):
                await parser.parse_instruction("x")


class TestCaseSearch(unittest.IsolatedAsyncioTestCase):
    async def test_single_hit(self):
        rec = {"$id": {"value": "45"}, "顧客名": {"value": "鈴木一郎"},
               "status": {"value": "受任"}}
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[rec])) as s:
            hits = await case_search.search_cases("鈴木")
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].label(), "鈴木一郎（No.45・受任・時効援用）")
        self.assertIn('顧客名 like "鈴木"', s.await_args.args[1])

    async def test_zero_hits_retries_once_with_compact_name(self):
        mock = AsyncMock(side_effect=[[], []])
        with patch("hub.kintone.search_records", new=mock):
            hits = await case_search.search_cases("鈴木　一郎")
        self.assertEqual(hits, [])
        self.assertEqual(mock.await_count, 2, "再検索は1回だけ")
        self.assertIn("鈴木一郎", mock.await_args_list[1].args[1])

    async def test_completed_case_gets_warning_label(self):
        h = hit(status="完了")
        self.assertTrue(h.warn)
        self.assertTrue(h.label().startswith("⚠ "))

    async def test_quote_escaped_in_query(self):
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[])) as s:
            await case_search.search_cases('鈴"木')
        self.assertIn('like "鈴\\"木"', s.await_args.args[1])


class TestHandler(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def _patch(self, parse_results, hits=None):
        """parse_instruction と search_cases を差し替える"""
        seq = parse_results if isinstance(parse_results, list) else [parse_results]
        return (patch.object(parser, "parse_instruction", new=AsyncMock(side_effect=seq)),
                patch.object(case_search, "search_cases",
                             new=AsyncMock(return_value=hits or [])))

    async def test_task_single_hit_presents_interpretation(self):
        p, s = self._patch(parsed(), hits=[hit()])
        with p, s:
            reply = await handler.handle_message("U1", "鈴木さんに送付案内を作って")
        self.assertIn("【解釈結果】", reply)
        self.assertIn("送付案内の作成", reply)
        self.assertIn("No.45 鈴木一郎（受任・時効援用）", reply)
        self.assertNotIn("⚠", reply)

    async def test_completed_case_shows_warning(self):
        p, s = self._patch(parsed(), hits=[hit(status="完了")])
        with p, s:
            reply = await handler.handle_message("U1", "鈴木さんに送付案内")
        self.assertIn("⚠ この案件は status=完了 です", reply)

    async def test_multiple_hits_then_number_selection(self):
        hits = [hit("45", "鈴木一郎", "受任"), hit("52", "鈴木花子", "手続き中"),
                hit("12", "鈴木一郎", "完了")]
        p, s = self._patch(parsed(), hits=hits)
        with p, s:
            reply = await handler.handle_message("U1", "鈴木さんに送付案内")
        self.assertIn("3件あります", reply)
        self.assertIn("1. 鈴木一郎（No.45・受任・時効援用）", reply)
        self.assertIn("3. ⚠ 鈴木一郎（No.12・完了・時効援用）", reply)
        # 番号選択（同姓同名は No で区別）
        reply2 = await handler.handle_message("U1", "3")
        self.assertIn("No.12 鈴木一郎", reply2)
        self.assertIn("【解釈結果】", reply2)

    async def test_out_of_range_number(self):
        p, s = self._patch(parsed(), hits=[hit(), hit("52", "鈴木花子", "受任")])
        with p, s:
            await handler.handle_message("U1", "鈴木さんに送付案内")
            reply = await handler.handle_message("U1", "9")
        self.assertIn("1〜2 の番号で", reply)

    async def test_zero_hits_guides(self):
        p, s = self._patch(parsed(), hits=[])
        with p, s:
            reply = await handler.handle_message("U1", "鈴木さんに送付案内")
        self.assertEqual(reply, case_search.NOT_FOUND_MESSAGE)

    async def test_unsupported_task_type(self):
        p, s = self._patch(parsed(task_type="shokumu_seikyu"))
        with p, s:
            reply = await handler.handle_message("U1", "佐藤さんの職務上請求を川口市宛で")
        self.assertEqual(reply, handler.MSG_UNSUPPORTED)

    async def test_query_intent_returns_phase2_notice(self):
        p, s = self._patch(parsed(intent="query", task_type=None))
        with p, s:
            reply = await handler.handle_message("U1", "今日の要対応一覧を出して")
        self.assertEqual(reply, handler.MSG_QUERY_LATER)

    async def test_confirm_without_pending(self):
        p, s = self._patch(parsed(intent="confirm"))
        with p, s:
            reply = await handler.handle_message("U1", "OK")
        self.assertEqual(reply, handler.MSG_NO_PENDING)

    async def test_cancel_clears_session(self):
        p, s = self._patch([parsed(customer_name=None,
                                   clarification="どの顧客への指示ですか？"),
                            parsed(intent="cancel")])
        with p, s:
            await handler.handle_message("U1", "送付案内を作って")
            reply = await handler.handle_message("U1", "キャンセル")
        self.assertEqual(reply, handler.MSG_CANCELLED)

    async def test_clarification_combines_and_reparses(self):
        """聞き返し→回答が元指示と結合されて再解析される（03 §7）"""
        parse_mock = AsyncMock(side_effect=[
            parsed(customer_name=None),   # 1回目: 顧客名なし→聞き返し
            parsed(),                     # 2回目: 結合テキストで確定
        ])
        with patch.object(parser, "parse_instruction", new=parse_mock), \
             patch.object(case_search, "search_cases",
                          new=AsyncMock(return_value=[hit()])):
            reply1 = await handler.handle_message("U1", "送付案内を作って")
            self.assertIn("氏名を教えてください", reply1)
            reply2 = await handler.handle_message("U1", "鈴木さん")
        self.assertIn("【解釈結果】", reply2)
        combined = parse_mock.await_args_list[1].args[0]
        self.assertIn("送付案内を作って", combined)
        self.assertIn("（追加回答）鈴木さん", combined)

    async def test_clarification_cut_off_after_two_rounds(self):
        """聞き返しは2往復まで。3回目が必要になったら打ち切り（03 §7）"""
        p, s = self._patch([parsed(customer_name=None)] * 3)
        with p, s:
            r1 = await handler.handle_message("U1", "送付案内")
            r2 = await handler.handle_message("U1", "えっと")
            r3 = await handler.handle_message("U1", "うーん")
        self.assertIn("氏名を教えてください", r1)
        self.assertIn("氏名を教えてください", r2)
        self.assertEqual(r3, handler.MSG_GIVE_UP)

    async def test_low_confidence_asks_clarification(self):
        p, s = self._patch(parsed(confidence="low",
                                  clarification="どの書類の送付案内ですか？"))
        with p, s:
            reply = await handler.handle_message("U1", "あれやっといて")
        self.assertEqual(reply, "どの書類の送付案内ですか？")

    async def test_claude_unavailable_returns_fixed_message(self):
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(side_effect=ClaudeUnavailableError("down"))):
            reply = await handler.handle_message("U1", "鈴木さんに送付案内")
        self.assertEqual(reply, handler.MSG_AI_DOWN)

    async def test_sessions_are_per_user(self):
        p, s = self._patch([parsed(customer_name=None), parsed(intent="confirm")])
        with p, s:
            await handler.handle_message("U_owner1", "送付案内")   # U_owner1 に聞き返し
            reply = await handler.handle_message("U_other", "OK")  # 別ユーザーは独立
        self.assertEqual(reply, handler.MSG_NO_PENDING)


class TestIsolation(unittest.TestCase):
    def test_no_chat_responder_import(self):
        """chat_responder 非依存（確定判断9・パッケージ全ファイル）"""
        import pathlib
        pkg = pathlib.Path(handler.__file__).parent
        for py in pkg.glob("*.py"):
            src = py.read_text(encoding="utf-8")
            for stmt in ("import chat_responder", "from chat_responder"):
                self.assertNotIn(stmt, src, f"{py.name} が chat_responder を import")

    def test_parser_goes_through_claude_gateway(self):
        """直接 anthropic messages.create せず claude_gateway を経由する"""
        import pathlib
        src = pathlib.Path(parser.__file__).read_text(encoding="utf-8")
        self.assertIn("create_message_with_fallback", src)
        self.assertNotIn("messages.create", src)


if __name__ == "__main__":
    unittest.main()
