"""Bot語彙: 名寄せ候補の確定（R4-2b T2・person_merge タスク）のテスト

検証:
レジストリ登録（フック3点・handler 固有分岐ゼロ）・フラグ無効の完全不発・
候補0件即答・一覧提示の1メッセージ集約（吹き出し分割しない・保留の別）・
番号指定の解釈（単数・複数・全部・範囲外・別人）・「全部」から保留の除外・
二段確認（復唱なしのOKは実行拒否・復唱段階では実行が走らない）・
OK で execute_merge / reject_pair 起動と結果の素通し報告（部分失敗含む）・
キャンセル語のフォールスルー・既存関所語彙への無影響。
T1コア / parser は全てモック。
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

from dispatch_bot import handler, parser, registry, person_merge_task  # noqa: E402
from person_merge_exec import MergeCandidate  # noqa: E402

_ENV = {"PERSON_MERGE_ENABLED": "1",
        "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
        "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30"}

PARSE_PM = {"intent": "task", "task_type": "person_merge",
            "customer_name": None, "task_params": {}, "confidence": "high",
            "missing_fields": [], "clarification": None}
PARSE_CONFIRM = {"intent": "confirm", "task_type": None, "customer_name": None,
                 "task_params": {}, "confidence": "high", "missing_fields": [],
                 "clarification": None}
PARSE_CANCEL = {"intent": "cancel", "task_type": None, "customer_name": None,
                "task_params": {}, "confidence": "high", "missing_fields": [],
                "clarification": None}


def cand(review, winner, loser, wname, lname, *, pending=False,
         signals=("①正規化氏名一致", "③生年月日一致")):
    return MergeCandidate(
        review_record_id=str(review),
        pair_key=f"person_merge:{winner}-{loser}",
        winner_id=str(winner), loser_id=str(loser),
        winner_name=wname, loser_name=lname, signals=list(signals),
        pending_case=pending,
        pending_reason="案件参照が相違" if pending else "")


CANDS = [cand(90, 6, 9, "鈴木 誠", "鈴木 誠"),
         cand(91, 7, 10, "香奈", "長谷川 香奈", pending=True,
              signals=("④婚姻相互リンク",)),
         cand(92, 13, 18, "鈴木 チヨ子", "鈴木 チョ子")]


class _Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def arm(self, *, parse=PARSE_PM, cands=CANDS, env=_ENV,
            merge_results=None, reject_results=None):
        self.merge = AsyncMock(
            side_effect=merge_results,
            return_value={"status": "merged", "winner_id": "6",
                          "loser_id": "9", "repointed": [],
                          "review_record_id": "90"})
        self.reject = AsyncMock(
            side_effect=reject_results,
            return_value={"status": "rejected",
                          "pair_key": "person_merge:7-10",
                          "review_record_id": "91"})
        patchers = [
            patch.object(parser, "parse_instruction",
                         new=AsyncMock(return_value=dict(parse))),
            patch.object(person_merge_task, "list_merge_candidates",
                         new=AsyncMock(return_value=list(cands))),
            patch.object(person_merge_task, "execute_merge", new=self.merge),
            patch.object(person_merge_task, "reject_pair", new=self.reject),
            patch.dict(os.environ, env),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    async def confirm_ok(self, user="U1"):
        """OK 送信（parser を confirm に差し替えて1メッセージ処理）"""
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            return await handler.handle_message(user, "OK")


class TestRegistryEntry(unittest.TestCase):
    def test_registered_with_hooks(self):
        spec = registry.get_task("person_merge")
        self.assertIsNotNone(spec)
        self.assertIs(spec.flow_fn, person_merge_task.flow)
        self.assertIs(spec.flow_reply_fn, person_merge_task.flow_reply)
        self.assertIs(spec.execute_fn, person_merge_task.execute)
        self.assertEqual(spec.required_fields, [], "顧客名は不要（番号指定で操作）")

    def test_catalog_includes_person_merge(self):
        self.assertIn("person_merge", registry.catalog_for_prompt())


class TestFlow(_Base):
    async def test_flag_off_is_inert(self):
        """フラグ無効: 一覧提示も不発（候補取得を呼ばない）"""
        self.arm(env={**_ENV, "PERSON_MERGE_ENABLED": ""})
        reply = await handler.handle_message("U1", "名寄せ候補を見せて")
        self.assertEqual(reply, person_merge_task.MSG_DISABLED)
        person_merge_task.list_merge_candidates.assert_not_awaited()

    async def test_zero_candidates(self):
        self.arm(cands=[])
        reply = await handler.handle_message("U1", "名寄せ候補を見せて")
        self.assertEqual(reply, person_merge_task.MSG_NO_CANDIDATES)

    async def test_list_aggregated_into_one_message(self):
        """一覧提示: 複数候補を1メッセージに集約（番号・氏名・レコード番号・
        シグナル・保留の別）"""
        self.arm()
        reply = await handler.handle_message("U1", "名寄せ候補を見せて")
        self.assertIn("名寄せ候補が3件あります", reply)
        self.assertIn("1. No.6 鈴木 誠 ⇔ No.9 鈴木 誠", reply)
        self.assertIn("2. No.7 香奈 ⇔ No.10 長谷川 香奈【保留: 案件相違】",
                      reply)
        self.assertIn("3. No.13 鈴木 チヨ子 ⇔ No.18 鈴木 チョ子", reply)
        self.assertIn("シグナル: ④婚姻相互リンク", reply)
        self.assertIn("「1と3を統合して」／「2は別人」／「全部統合して」", reply)


class TestNumberCommands(_Base):
    async def _open_list(self, user="U1"):
        await handler.handle_message(user, "名寄せ候補を見せて")

    async def test_multi_merge_confirmation(self):
        """「1と3を統合して」: 復唱に統合される氏名・レコード番号・無効化される
        番号を明示（RV-08: 削除ではなく無効化）。復唱段階では実行が走らない
        （二段確認）"""
        self.arm()
        await self._open_list()
        reply = await handler.handle_message("U1", "1と3を統合して")
        self.assertIn("以下の2件を統合します", reply)
        self.assertIn("No.9 鈴木 誠 を No.6 鈴木 誠 に統合"
                      "（No.9 のレコードは無効化されます）", reply)
        self.assertIn("No.18 鈴木 チョ子 を No.13 鈴木 チヨ子 に統合"
                      "（No.18 のレコードは無効化されます）", reply)
        self.assertIn("統合済み無効", reply)
        self.assertNotIn("物理削除", reply, "RV-08: 削除を予告しない")
        self.assertIn("OK / キャンセル", reply)
        self.merge.assert_not_awaited()
        self.reject.assert_not_awaited()

    async def test_single_merge_then_ok_executes(self):
        self.arm()
        await self._open_list()
        await handler.handle_message("U1", "1を統合して")
        self.merge.assert_not_awaited()
        reply = await self.confirm_ok()
        self.merge.assert_awaited_once()
        target = self.merge.await_args.args[0]
        self.assertEqual((target.winner_id, target.loser_id), ("6", "9"))
        self.assertIn("No.9 鈴木 誠 を No.6 鈴木 誠 に統合しました", reply)
        self.assertIn("監査JSONを封筒 No.90 に添付", reply)
        self.assertIn("kintone内部のみ・対外送信なし", reply)

    async def test_merge_all_excludes_pending(self):
        """「全部統合して」: 保留つきは対象外と復唱・対象は保留なしのみ"""
        self.arm()
        await self._open_list()
        reply = await handler.handle_message("U1", "全部統合して")
        self.assertIn("以下の2件を統合します", reply)
        self.assertNotIn("長谷川 香奈", reply.split("※")[0],
                         "保留つきは統合対象に載らない")
        self.assertIn("※ 保留（案件相違）つきの1件は対象外です", reply)
        await self.confirm_ok()
        merged = [c.args[0].pair_key for c in self.merge.await_args_list]
        self.assertEqual(merged, ["person_merge:6-9", "person_merge:13-18"])

    async def test_merge_all_with_only_pending(self):
        self.arm(cands=[cand(91, 7, 10, "香奈", "長谷川 香奈", pending=True)])
        await self._open_list()
        reply = await handler.handle_message("U1", "全部統合して")
        self.assertEqual(reply, person_merge_task.MSG_NO_MERGEABLE)
        self.merge.assert_not_awaited()

    async def test_pending_pair_explicit_merge_warns(self):
        """保留つきの明示番号指定は復唱に保留警告を出す"""
        self.arm()
        await self._open_list()
        reply = await handler.handle_message("U1", "2を統合して")
        self.assertIn("⚠ 保留つき: 案件参照が相違", reply)

    async def test_reject_flow(self):
        """「2は別人」→ 復唱 → OK でクローズ＋再起票の恒久抑止を報告"""
        self.arm()
        await self._open_list()
        reply = await handler.handle_message("U1", "2は別人")
        self.assertIn("【別人】として棄却します", reply)
        self.assertIn("今後自動起票されません", reply)
        self.reject.assert_not_awaited()
        result = await self.confirm_ok()
        self.reject.assert_awaited_once()
        self.assertEqual(self.reject.await_args.args[0].pair_key,
                         "person_merge:7-10")
        self.assertIn("別人として棄却しました", result)
        self.merge.assert_not_awaited()

    async def test_out_of_range_number(self):
        self.arm()
        await self._open_list()
        self.assertIn("1〜3 の番号",
                      await handler.handle_message("U1", "9は別人"))
        self.assertIn("1〜3 の番号",
                      await handler.handle_message("U1", "1と9を統合して"))
        self.merge.assert_not_awaited()

    async def test_partial_failure_reported_as_is(self):
        """候補ごとに独立実行・中止理由はそのまま報告（意訳しない）"""
        self.arm(merge_results=[
            {"status": "merged", "winner_id": "6", "loser_id": "9",
             "repointed": [{"person_record_id": "12", "fields": ["父人物ID"]}],
             "review_record_id": "90"},
            {"status": "aborted",
             "reason": "封筒 No.92 が要確認ではなくなっています"}])
        await self._open_list()
        await handler.handle_message("U1", "1と3を統合して")
        reply = await self.confirm_ok()
        self.assertIn("統合しました", reply)
        self.assertIn("親エッジ付け替え: No.12", reply)
        self.assertIn("封筒 No.92 が要確認ではなくなっています", reply)

    async def test_ok_without_confirmation_refused(self):
        """復唱なしの OK は実行拒否（二段確認の固定）"""
        self.arm()
        reply = await self.confirm_ok()
        self.assertEqual(reply, handler.MSG_NO_PENDING)
        self.merge.assert_not_awaited()

    async def test_cancel_word_falls_through(self):
        """一覧セッション中のキャンセル語は通常解析（cancel）へフォールスルー"""
        self.arm()
        await self._open_list()
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CANCEL))):
            reply = await handler.handle_message("U1", "キャンセル")
        self.assertEqual(reply, handler.MSG_CANCELLED)

    async def test_unrelated_text_not_consumed(self):
        """統合・別人・全部のどれでもない入力は消費しない（別指示に落ちる）"""
        self.arm()
        await self._open_list()
        session = handler._get_session("U1")
        handled, _ = await person_merge_task.flow_reply(
            "U1", "熊澤さんに送付案内を作って", session)
        self.assertFalse(handled)


if __name__ == "__main__":
    unittest.main()
