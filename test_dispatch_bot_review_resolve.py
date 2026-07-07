"""Bot語彙: 要確認の確定（S5-2.5 T2・review_resolve タスク）のテスト

検証:
レジストリ登録（フック3点）・要確認0件即答・1グループ→案件指定→復唱・
複数グループの番号選択・同姓複数の番号選択・No.直指定（task_params／回答テキスト
両経路・不明Noの聞き返し）・OK で resolve_group 起動と結果報告（成功／ガード中止／
env縮退／未知キーの素通し報告）・キャンセル語のフォールスルー・
既存タスク（送付案内）非干渉。kintone / T1コア / parser は全てモック。
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

from customer_directory import Candidate  # noqa: E402
from dispatch_bot import handler, parser, registry, review_resolve_task  # noqa: E402
from review_resolve import ReviewGroup, ReviewItem  # noqa: E402

_ENV = {"APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
        "APP_ZAISAN": "35", "TOKEN_ZAISAN": "t35",
        "KINTONE_FUDOSAN_APP_ID": "25", "KINTONE_FUDOSAN_API_TOKEN": "t25"}

PARSE_RR = {"intent": "task", "task_type": "review_resolve",
            "customer_name": "熊澤", "task_params": {}, "confidence": "high",
            "missing_fields": [], "clarification": None}
PARSE_CONFIRM = {"intent": "confirm", "task_type": None, "customer_name": None,
                 "task_params": {}, "confidence": "high", "missing_fields": [],
                 "clarification": None}
PARSE_CANCEL = {"intent": "cancel", "task_type": None, "customer_name": None,
                "task_params": {}, "confidence": "high", "missing_fields": [],
                "clarification": None}


def item(rid, fudosan_id, idem="fid-1"):
    return ReviewItem(record_id=str(rid),
                      subject="登記事項証明の読解転記: 案件紐付け不能",
                      detail={"不動産レコードID": str(fudosan_id), "冪等キー": idem},
                      file_keys=[f"pdf-{rid}"], file_name="touki.pdf")


def group(idem="fid-1", items=None):
    return ReviewGroup(source="registry_ingest", idempotency_key=idem,
                       items=items or [item(7, 97, idem), item(8, 98, idem)])


def cand(rid, name, status=""):
    return Candidate(record_id=str(rid), app_id="26", source="相談カード (相続)",
                     customer_name=name, status=status)


class _Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def arm(self, *, parse=PARSE_RR, groups=(), cands=(), kinds=("土地", "建物"),
            resolve=None, env=_ENV):
        self.resolve = AsyncMock(return_value=resolve if resolve is not None else
                                 {"status": "resolved", "case_record_id": "12",
                                  "items": [
                                      {"review_record_id": "7", "zaisan": "created",
                                       "zaisan_record_id": "351"},
                                      {"review_record_id": "8", "zaisan": "updated",
                                       "zaisan_record_id": "88"}]})
        patchers = [
            patch.object(parser, "parse_instruction",
                         new=AsyncMock(return_value=dict(parse))),
            patch.object(review_resolve_task, "list_pending_reviews",
                         new=AsyncMock(return_value=list(groups))),
            patch.object(review_resolve_task, "list_candidates",
                         new=AsyncMock(return_value=list(cands))),
            patch.object(review_resolve_task, "_group_kinds",
                         new=AsyncMock(return_value=list(kinds))),
            patch.object(review_resolve_task, "resolve_group", new=self.resolve),
            patch.dict(os.environ, env),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)


class TestRegistryEntry(unittest.TestCase):
    def test_registered_with_hooks(self):
        spec = registry.get_task("review_resolve")
        self.assertIsNotNone(spec)
        self.assertIs(spec.flow_fn, review_resolve_task.flow)
        self.assertIs(spec.flow_reply_fn, review_resolve_task.flow_reply)
        self.assertIs(spec.execute_fn, review_resolve_task.execute)

    def test_catalog_includes_review_resolve(self):
        self.assertIn("review_resolve", registry.catalog_for_prompt())


class TestFlowBranches(_Base):
    async def test_zero_reviews(self):
        self.arm(groups=[])
        reply = await handler.handle_message("U1", "要確認を処理して")
        self.assertEqual(reply, review_resolve_task.MSG_NO_PENDING_REVIEWS)

    async def test_single_group_single_customer_confirmation_text(self):
        self.arm(groups=[group()], cands=[cand(12, "熊澤正広")])
        reply = await handler.handle_message("U1", "熊澤さんの要確認を確定して")
        self.assertEqual(reply,
                         "要確認2件（土地・建物）を No12_熊澤正広 の案件に確定します。\n"
                         "OK / キャンセル（30分有効）")

    async def test_single_group_without_kinds_omits_parens(self):
        self.arm(groups=[group()], cands=[cand(12, "熊澤正広")], kinds=[])
        reply = await handler.handle_message("U1", "熊澤さんの要確認を確定して")
        self.assertIn("要確認2件を No12_熊澤正広 の案件に確定します", reply)

    async def test_multiple_groups_numbered_selection(self):
        g2 = group(idem="fid-2", items=[item(9, 99, "fid-2")])
        self.arm(groups=[group(), g2], cands=[cand(12, "熊澤正広")])
        reply = await handler.handle_message("U1", "熊澤さんの要確認を確定して")
        self.assertIn("要確認が2グループあります", reply)
        self.assertIn("1. 登記事項証明の読解転記: 案件紐付け不能（No.7・No.8）", reply)
        self.assertIn("（No.9）", reply)
        reply2 = await handler.handle_message("U1", "2")
        self.assertIn("要確認1件（土地・建物）を No12_熊澤正広", reply2)

    async def test_same_family_name_numbered_selection(self):
        self.arm(groups=[group()],
                 cands=[cand(12, "熊澤正広"), cand(30, "熊澤花子")])
        reply = await handler.handle_message("U1", "熊澤さんの要確認を確定して")
        self.assertIn("「熊澤」の候補顧客が2件あります", reply)
        reply2 = await handler.handle_message("U1", "2")
        self.assertIn("No30_熊澤花子 の案件に確定します", reply2)

    async def test_direct_case_no_in_task_params(self):
        parse = dict(PARSE_RR, customer_name=None,
                     task_params={"case_record_id": "12"})
        self.arm(parse=parse, groups=[group()], cands=[cand(12, "熊澤正広")])
        reply = await handler.handle_message("U1", "要確認をNo.12の案件へ")
        self.assertIn("No12_熊澤正広 の案件に確定します", reply)

    async def test_direct_case_no_as_answer_text(self):
        """氏名質問への「No.12」回答も直指定として受ける"""
        parse = dict(PARSE_RR, customer_name=None)
        self.arm(parse=parse, groups=[group()], cands=[cand(12, "熊澤正広")])
        reply = await handler.handle_message("U1", "要確認を確定して")
        self.assertEqual(reply, review_resolve_task.QUESTION_CUSTOMER)
        reply2 = await handler.handle_message("U1", "No.12")
        self.assertIn("No12_熊澤正広 の案件に確定します", reply2)

    async def test_unknown_case_no_reasks(self):
        parse = dict(PARSE_RR, customer_name=None,
                     task_params={"case_record_id": "999"})
        self.arm(parse=parse, groups=[group()], cands=[cand(12, "熊澤正広")])
        reply = await handler.handle_message("U1", "要確認をNo.999へ")
        self.assertIn("No.999 は候補顧客に見つかりません", reply)

    async def test_cancel_word_falls_through_during_name_stage(self):
        parse = dict(PARSE_RR, customer_name=None)
        self.arm(parse=parse, groups=[group()], cands=[cand(12, "熊澤正広")])
        await handler.handle_message("U1", "要確認を確定して")
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CANCEL))):
            reply = await handler.handle_message("U1", "キャンセル")
        self.assertEqual(reply, handler.MSG_CANCELLED)


class TestExecute(_Base):
    async def _to_pending(self):
        self.arm(groups=[group()], cands=[cand(12, "熊澤正広")])
        return await handler.handle_message("U1", "熊澤さんの要確認を確定して")

    async def test_ok_runs_resolve_group_and_reports(self):
        await self._to_pending()
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            reply = await handler.handle_message("U1", "OK")
        # resolve_group が復元グループ＋案件IDで1回呼ばれる
        (called_group, called_case), _ = self.resolve.await_args
        self.assertEqual(called_case, "12")
        self.assertEqual(called_group.source, "registry_ingest")
        self.assertEqual([i.record_id for i in called_group.items], ["7", "8"])
        self.assertIn("要確認2件を No12_熊澤正広 の案件に確定しました", reply)
        self.assertIn("・要確認 No.7 → 財産行 No.351（新規）", reply)
        self.assertIn("・要確認 No.8 → 財産行 No.88（追記）", reply)
        self.assertIn("対外送信なし", reply)
        self.assertIn("/k/35/show#record=351", reply)

    async def test_aborted_guard_is_reported_verbatim(self):
        await self._to_pending()
        self.resolve.return_value = {
            "status": "aborted",
            "reason": "No.8 が要確認ではなくなっています（発送ステータス=完了・実行済み=yes）。"
                      "グループ全体を中止しました（書き込みなし）"}
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            reply = await handler.handle_message("U1", "OK")
        self.assertIn("確定を中止しました: No.8 が要確認ではなくなっています", reply)

    async def test_unavailable_env_is_reported(self):
        await self._to_pending()
        self.resolve.return_value = {"status": "unavailable",
                                     "reason": "App 財産 の env（APP_ZAISAN）が未設定です"}
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            reply = await handler.handle_message("U1", "OK")
        self.assertIn("確定できません（環境未設定）: App 財産", reply)

    async def test_unsupported_source_is_reported(self):
        await self._to_pending()
        self.resolve.return_value = {"status": "unsupported",
                                     "reason": "対応する確定処理がありません"
                                               "（チャネル固有データのキー=zaisan_sync）"}
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            reply = await handler.handle_message("U1", "OK")
        self.assertIn("対応する確定処理がありません", reply)


class TestExistingTasksUnaffected(_Base):
    async def test_soufu_annai_pipeline_untouched(self):
        parse = {"intent": "task", "task_type": "soufu_annai", "customer_name": None,
                 "task_params": {}, "confidence": "high", "missing_fields": [],
                 "clarification": None}
        self.arm(parse=parse)
        reply = await handler.handle_message("U1", "送付案内作って")
        self.assertEqual(reply, "どの顧客（案件）への指示ですか？氏名を教えてください")
        review_resolve_task.list_pending_reviews.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
