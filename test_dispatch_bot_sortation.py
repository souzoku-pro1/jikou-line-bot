"""Bot仕分け指示（書類仕分け第2段②・sortation_assign）のテスト

検証:
レジストリ登録（フック3点）・App 38 env 未設定の即答・照会中0件/1件/複数件の分岐・
顧客突合（一意/同姓複数の番号選択/該当なし再質問）・氏名不足の聞き返し→回答継続・
復唱文言・OK で 照会中→確定 の更新（記入フィールド・対外効果ゼロ）・
処理済みレコードの安全側・二重OK・キャンセル語のフォールスルー・
更新失敗時のエラー返信・既存タスク（送付案内）への非干渉。
kintone / customer_directory / parser は全てモック。
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

from customer_directory import Candidate  # noqa: E402
from dispatch_bot import handler, parser, registry, sortation_assign  # noqa: E402
from hub import kintone  # noqa: E402

_ENV = {"APP_SORTATION_LOG": "38", "TOKEN_SORTATION_LOG": "t38"}

PARSE_SORT = {
    "intent": "task", "task_type": "sortation_assign", "customer_name": "山田",
    "task_params": {}, "confidence": "high", "missing_fields": [],
    "clarification": None,
}
PARSE_CONFIRM = {"intent": "confirm", "task_type": None, "customer_name": None,
                 "task_params": {}, "confidence": "high", "missing_fields": [],
                 "clarification": None}
PARSE_CANCEL = {"intent": "cancel", "task_type": None, "customer_name": None,
                "task_params": {}, "confidence": "high", "missing_fields": [],
                "clarification": None}


def log_rec(rid, fname, dtype="戸籍"):
    return {"$id": {"value": str(rid)}, "ファイル名": {"value": fname},
            "書類種類": {"value": dtype}}


def cand(rid, name, status=""):
    return Candidate(record_id=str(rid), app_id="26", source="相談カード (相続)",
                     customer_name=name, status=status)


class _Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def arm(self, *, parse=PARSE_SORT, logs=(), cands=(), record_state="照会中",
            env=_ENV, update_error=None):
        """parser / kintone / customer_directory をモックする"""
        self.search = AsyncMock(return_value=[log_rec(*a) if isinstance(a, tuple)
                                              else a for a in logs])
        self.get = AsyncMock(return_value={"状態": {"value": record_state}})
        self.update = AsyncMock(side_effect=update_error)
        patchers = [
            patch.object(parser, "parse_instruction",
                         new=AsyncMock(return_value=dict(parse))),
            patch.object(kintone, "search_records", new=self.search),
            patch.object(kintone, "get_record", new=self.get),
            patch.object(kintone, "update_record", new=self.update),
            patch.object(sortation_assign, "list_candidates",
                         new=AsyncMock(return_value=list(cands))),
            patch.dict(os.environ, env),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)


class TestRegistryEntry(unittest.TestCase):
    def test_registered_with_hooks(self):
        spec = registry.get_task("sortation_assign")
        self.assertIsNotNone(spec)
        self.assertFalse(spec.answer_only)
        self.assertEqual(spec.destination, "sortation_log")
        self.assertIs(spec.flow_fn, sortation_assign.flow)
        self.assertIs(spec.flow_reply_fn, sortation_assign.flow_reply)
        self.assertIs(spec.execute_fn, sortation_assign.execute)

    def test_catalog_includes_sortation(self):
        self.assertIn("sortation_assign", registry.catalog_for_prompt())


class TestFlowBranches(_Base):
    async def test_app_env_unset_message(self):
        self.arm(env={"APP_SORTATION_LOG": "", "TOKEN_SORTATION_LOG": ""})
        reply = await handler.handle_message("U1", "山田さんの書類を仕分けして")
        self.assertEqual(reply, sortation_assign.MSG_APP_UNSET)

    async def test_zero_pending_docs(self):
        self.arm(logs=[])
        reply = await handler.handle_message("U1", "山田さんの書類を仕分けして")
        self.assertEqual(reply, sortation_assign.MSG_NO_PENDING_DOCS)

    async def test_single_doc_single_customer_goes_to_confirmation(self):
        """照会中1件×顧客一意 → 指定の復唱文言"""
        self.arm(logs=[(5, "scan001.pdf", "評価証明・課税明細")],
                 cands=[cand(12, "山田太郎")])
        reply = await handler.handle_message("U1", "山田さんの書類を仕分けして")
        self.assertEqual(reply,
                         "scan001.pdfを No12_山田太郎 のフォルダに仕分けします。\n"
                         "OK / キャンセル（30分有効）")

    async def test_multiple_docs_numbered_list_then_select(self):
        """照会中複数 → 番号付き一覧（ファイル名・書類種類）→ 番号で対象確定"""
        self.arm(logs=[(5, "a.pdf", "戸籍"), (6, "b.pdf", "通帳")],
                 cands=[cand(12, "山田太郎")])
        reply = await handler.handle_message("U1", "山田さんの書類を仕分けして")
        self.assertIn("照会中の書類が2件あります", reply)
        self.assertIn("1. a.pdf（戸籍）", reply)
        self.assertIn("2. b.pdf（通帳）", reply)
        reply2 = await handler.handle_message("U1", "2")
        self.assertIn("b.pdfを No12_山田太郎 のフォルダに仕分けします", reply2)

    async def test_doc_select_out_of_range(self):
        self.arm(logs=[(5, "a.pdf"), (6, "b.pdf")], cands=[cand(12, "山田太郎")])
        await handler.handle_message("U1", "山田さんの書類を仕分けして")
        reply = await handler.handle_message("U1", "9")
        self.assertEqual(reply, "1〜2 の番号で選んでください")

    async def test_same_family_name_numbered_selection(self):
        """同姓複数 → D4方式の番号選択 → 復唱"""
        self.arm(logs=[(5, "a.pdf")],
                 cands=[cand(12, "山田太郎"), cand(30, "山田花子")])
        reply = await handler.handle_message("U1", "山田さんの書類を仕分けして")
        self.assertIn("「山田」の候補顧客が2件あります", reply)
        reply2 = await handler.handle_message("U1", "2")
        self.assertIn("a.pdfを No30_山田花子 のフォルダに仕分けします", reply2)

    async def test_customer_name_missing_asks_then_answer_continues(self):
        """氏名不足 → 聞き返し → 回答テキストで突合継続"""
        self.arm(parse=dict(PARSE_SORT, customer_name=None),
                 logs=[(5, "a.pdf")], cands=[cand(12, "山田太郎")])
        reply = await handler.handle_message("U1", "書類を仕分けして")
        self.assertEqual(reply, sortation_assign.QUESTION_CUSTOMER)
        reply2 = await handler.handle_message("U1", "山田太郎")
        self.assertIn("a.pdfを No12_山田太郎 のフォルダに仕分けします", reply2)

    async def test_no_customer_match_reasks(self):
        self.arm(logs=[(5, "a.pdf")], cands=[cand(12, "佐藤花子")])
        reply = await handler.handle_message("U1", "山田さんの書類を仕分けして")
        self.assertEqual(reply, sortation_assign.MSG_NO_CUSTOMER_MATCH)


class TestConfirmAndExecute(_Base):
    async def _to_pending(self):
        self.arm(logs=[(5, "scan001.pdf")], cands=[cand(12, "山田太郎", "送付状作成済")])
        return await handler.handle_message("U1", "山田さんの書類を仕分けして")

    async def test_ok_updates_log_to_confirmed(self):
        """OK → 照会中→確定・仕分け先4項目記入。App 30 起票・Drive・LINE顧客側は動かない"""
        await self._to_pending()
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            reply = await handler.handle_message("U1", "OK")
        self.assertIn("仕分けを確定しました: scan001.pdf → No12_山田太郎", reply)
        self.assertIn("GAS", reply)
        app, rid, fields = self.update.await_args.args
        self.assertEqual(app.app_id_env, "APP_SORTATION_LOG")
        self.assertEqual(rid, "5")
        self.assertEqual(fields["状態"], "確定")
        self.assertEqual(fields["仕分け先レコードID"], "12")
        self.assertEqual(fields["仕分け先氏名"], "山田太郎")
        self.assertEqual(fields["仕分け先フォルダ名"], "No12_山田太郎")
        self.assertRegex(fields["確定日時"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    async def test_double_ok_reshows_without_second_update(self):
        await self._to_pending()
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            await handler.handle_message("U1", "OK")
            reply = await handler.handle_message("U1", "OK")
        self.assertIn("実行済みです", reply)
        self.assertEqual(self.update.await_count, 1, "更新は1回だけ（単回消込）")

    async def test_already_processed_record_is_not_updated(self):
        """選択後に他経路で処理済みになっていたら更新しない（安全側）"""
        await self._to_pending()
        self.get.return_value = {"状態": {"value": "確定"}}
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            reply = await handler.handle_message("U1", "OK")
        self.assertIn("既に処理済みです（状態=確定", reply)
        self.assertEqual(self.update.await_count, 0)

    async def test_update_failure_returns_error_message(self):
        await self._to_pending()
        self.update.side_effect = kintone.KintoneError(500)
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))), \
                patch.object(handler.notify, "notify_admin_line", new=AsyncMock()):
            reply = await handler.handle_message("U1", "OK")
        self.assertEqual(reply, handler.MSG_FILE_FAILED)

    async def test_cancel_word_falls_through_during_name_stage(self):
        """氏名待ち中の「キャンセル」はフローが消費せず intent=cancel に落ちる"""
        self.arm(parse=dict(PARSE_SORT, customer_name=None), logs=[(5, "a.pdf")],
                 cands=[cand(12, "山田太郎")])
        await handler.handle_message("U1", "書類を仕分けして")
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CANCEL))):
            reply = await handler.handle_message("U1", "キャンセル")
        self.assertEqual(reply, handler.MSG_CANCELLED)


class TestExistingTasksUnaffected(_Base):
    async def test_soufu_annai_pipeline_untouched(self):
        """既存タスク（flow_fn なし）は従来の標準パイプラインのまま
        （案件検索の聞き返し文言が変わらないことで固定）"""
        parse = {"intent": "task", "task_type": "soufu_annai", "customer_name": None,
                 "task_params": {}, "confidence": "high", "missing_fields": [],
                 "clarification": None}
        self.arm(parse=parse)
        reply = await handler.handle_message("U1", "送付案内作って")
        self.assertEqual(reply, "どの顧客（案件）への指示ですか？氏名を教えてください")
        self.search.assert_not_awaited()  # App 38 は一切触らない


if __name__ == "__main__":
    unittest.main()
