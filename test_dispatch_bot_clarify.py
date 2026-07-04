"""D2 実機不具合の回帰テスト（2026-07-04）: 聞き返しのレジストリ駆動化

事象: 「送付案内作って」に対し聞き返しが「1. 宛先の顧客名 2. 送付する書類名
3. 送付日」と3項目を一度に要求した（1メッセージ1論点違反＋レジストリに存在しない
項目のモデル創作）。

修正の固定内容:
- 聞き返し文面はレジストリ（required_fields / field_questions）からコード側で組み立てる
- 不足項目のうち最初の1つだけを質問（1論点）
- モデルの missing_fields / clarification に何が入っていても、存在しない項目を聞かない
- soufu_annai の必須入力は顧客名のみ（設計05 §3.1）
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

from dispatch_bot import case_search, enclosures, handler, parser, registry  # noqa: E402

# 実機で観測されたモデル出力の再現（存在しない項目の創作＋複数列挙）
BUGGY_PARSE = {
    "intent": "task", "task_type": "soufu_annai", "customer_name": None,
    "task_params": {},
    "confidence": "medium",
    "missing_fields": ["宛先の顧客名", "送付する書類名", "送付日"],
    "clarification": "次を教えてください: 1. 宛先の顧客名 2. 送付する書類名 3. 送付日",
}

FORBIDDEN_WORDS = ("書類名", "送付日", "1.", "2.", "3.")


def hit(rid="45", name="鈴木一郎", status="受任"):
    return case_search.CaseHit(record_id=rid, customer_name=name, status=status)


class TestClarifyIsRegistryDriven(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    async def test_buggy_model_output_yields_single_registry_question(self):
        """実機再現: 創作された3項目の missing_fields でも、聞き返しは
        レジストリ定義の1論点（顧客名のみ）になる"""
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(BUGGY_PARSE))), \
             patch.object(case_search, "search_cases", new=AsyncMock(return_value=[])):
            reply = await handler.handle_message("U1", "送付案内作って")
        self.assertEqual(reply, "どの顧客（案件）への指示ですか？氏名を教えてください",
                         "レジストリ field_questions の文面そのもの")
        for word in FORBIDDEN_WORDS:
            self.assertNotIn(word, reply, f"存在しない項目/複数列挙が混入: {word}")

    async def test_invented_missing_fields_ignored_when_customer_present(self):
        """顧客名・同封物が取れていれば、創作 missing_fields があっても聞き返さず検索に進む"""
        data = dict(BUGGY_PARSE, customer_name="鈴木",
                    task_params={"enclosures": ["委任契約書"]},
                    missing_fields=["送付する書類名", "送付日"])
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=data)), \
             patch.object(enclosures, "list_options",
                          new=AsyncMock(return_value=[enclosures.EnclosureOption(
                              key="委任契約書", label="委任契約書")])), \
             patch.object(case_search, "search_cases",
                          new=AsyncMock(return_value=[hit()])) as search:
            reply = await handler.handle_message("U1", "鈴木さんに送付案内作って")
        search.assert_awaited_once()
        self.assertIn("を起票します。", reply)

    async def test_model_clarification_never_reaches_user_for_tasks(self):
        """タスク特定済みの聞き返し経路すべてで、モデルの clarification 文が出ない"""
        cases = [
            dict(BUGGY_PARSE),                             # 顧客名不足
            dict(BUGGY_PARSE, confidence="low",
                 customer_name="鈴木"),                    # 低確信度
        ]
        for data in cases:
            handler.reset_sessions()
            with self.subTest(confidence=data["confidence"]):
                # 2026-07-04 低確信度分岐の限定後は low でもフローが進むため、
                # 同封物選択肢（App 32）もモックする
                with patch.object(parser, "parse_instruction",
                                  new=AsyncMock(return_value=data)), \
                     patch.object(enclosures, "list_options",
                                  new=AsyncMock(return_value=[enclosures.EnclosureOption(
                                      key="委任契約書", label="委任契約書")])), \
                     patch.object(case_search, "search_cases",
                                  new=AsyncMock(return_value=[hit()])):
                    reply = await handler.handle_message("U1", "送付案内作って")
                self.assertNotIn("送付する書類名", reply)
                self.assertNotIn("送付日", reply)


class TestRegistryDefinition(unittest.TestCase):
    def test_soufu_annai_required_fields(self):
        """必須は顧客名＋同封物（2026-07-04: 同封物空起票のprepareエラーを受け必須化。
        宛先は案件から解決・「送付日」等は存在しない項目のまま）"""
        spec = registry.get_task("soufu_annai")
        self.assertEqual(spec.required_fields, ["customer_name", "enclosures"])
        self.assertIn("customer_name", spec.field_questions)
        self.assertNotIn("enclosures", spec.field_questions,
                         "同封物は動的選択肢（App 32）で聞くため静的質問文を持たない")

    def test_first_missing_question_is_single_topic(self):
        """_first_missing_question は最初の不足1つだけを返す（1論点）"""
        spec = registry.get_task("soufu_annai")
        q = handler._first_missing_question(
            spec, {"customer_name": None, "task_params": {}})
        self.assertEqual(q, "どの顧客（案件）への指示ですか？氏名を教えてください")
        q2 = handler._first_missing_question(
            spec, {"customer_name": "鈴木", "task_params": {}})
        self.assertIsNone(q2, "不足なしなら聞き返さない")

    def test_prompt_forbids_field_invention(self):
        """解析プロンプトに項目創作禁止と必須項目一覧が明記されている"""
        prompt = parser.build_system_prompt()
        self.assertIn("創作して要求しない", prompt)
        self.assertIn("1メッセージ1論点", prompt)
        self.assertIn("必須入力項目: customer_name", prompt)
        self.assertIn("これ以外の入力項目は存在しない", prompt)


if __name__ == "__main__":
    unittest.main()
