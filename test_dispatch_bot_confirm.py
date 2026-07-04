"""D3: 復唱確認・pending_command_id・App 30 起票のテスト

- 一巡: 指示→復唱（低リスク簡潔版）→OK→App 30 下書き起票＋返信（No・URL）
- pending: 二重OK・期限切れ・pendingなしOK・キャンセル・割込み無効化・単回消込
- 起票フィールドの内容検証（チャネル/状態/宛先=App 21由来/メタJSON）
- 二重実行防止の第2層（起票直前の pending_command_id 既存チェック）
- 起票失敗→ユーザー通知＋管理者警報
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

from dispatch_bot import app30_filer, case_search, confirm, enclosures, handler, parser  # noqa: E402
from hub import kintone  # noqa: E402


ENC_OPTIONS = [enclosures.EnclosureOption(key="委任契約書", label="委任契約書")]


def parsed(**over):
    # 同封物必須化（2026-07-04）に伴い、既定で指示文由来の同封物を含める
    base = {"intent": "task", "task_type": "soufu_annai",
            "customer_name": "鈴木", "task_params": {"enclosures": ["委任契約書"]},
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

    def patches(self, parse_results, hits=None, existing=None, create_id="101"):
        seq = parse_results if isinstance(parse_results, list) else [parse_results]
        # 起票は単票API（create_record・record.json）。一括API（create_records）は
        # kintone 仕様で Webhook が発射されないため使用禁止（2026-07-04 修正）。
        # bulk_mock はその「呼ばれないこと」の検証用
        self.create_mock = AsyncMock(return_value=create_id)
        self.bulk_mock = AsyncMock()
        self.search30_mock = AsyncMock(
            return_value=[{"$id": {"value": existing}}] if existing else [])
        return [
            patch.object(parser, "parse_instruction", new=AsyncMock(side_effect=seq)),
            patch.object(case_search, "search_cases",
                         new=AsyncMock(return_value=hits or [hit()])),
            patch("hub.kintone.get_record", new=AsyncMock(return_value=CASE_RECORD)),
            patch("hub.kintone.search_records", new=self.search30_mock),
            patch("hub.kintone.create_record", new=self.create_mock),
            patch("hub.kintone.create_records", new=self.bulk_mock),
            patch.object(enclosures, "list_options",
                         new=AsyncMock(return_value=list(ENC_OPTIONS))),
        ]

    async def send(self, text, user="U1"):
        return await handler.handle_message(user, text)


class TestFullCycle(Base):
    async def test_instruction_to_filing(self):
        """一巡: 指示→復唱（簡潔版2行）→OK→起票＋返信"""
        ps = self.patches([parsed(), parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            reply1 = await self.send("鈴木さんに送付案内を作って")
            # 復唱: 低リスク簡潔版（06 §2.1）
            self.assertEqual(
                reply1,
                "鈴木一郎さん（No.45・受任）に送付案内（委任契約書）を起票します。\n"
                "OK / キャンセル（30分有効）")
            reply2 = await self.send("OK")

        # 返信文言（③）
        self.assertIn("起票しました。App 30 No.101。", reply2)
        self.assertIn("この後の生成・承認はkintone側で行われます", reply2)
        self.assertIn("https://testsub.cybozu.com/k/30/show#record=101", reply2)

        # 起票フィールド検証
        self.create_mock.assert_awaited_once()
        fields = self.create_mock.await_args.args[1]
        self.assertEqual(fields["発送ステータス"], "下書き")
        self.assertEqual(fields["チャネル"], "送付案内")
        self.assertEqual(fields["ユニット種別"], "時効援用")
        self.assertEqual(fields["件名"], "送付案内（鈴木一郎）")
        self.assertEqual(fields["顧客名表示用"], "鈴木一郎")
        self.assertEqual(fields["宛先名"], "鈴木一郎")
        self.assertEqual(fields["宛先住所"], "埼玉県川口市本町1-2-3")
        self.assertEqual(fields["宛先郵便番号"], "332-0012")
        self.assertEqual(fields["案件アプリID"], "21")
        self.assertEqual(fields["案件レコードID"], "45")
        self.assertEqual(fields["実行済み"], "no")
        self.assertEqual(fields["同封物選択"], ["委任契約書"],
                         "同封物選択はブロックキーで設定（2026-07-04 修正）")
        meta = json.loads(fields["チャネル固有データ"])["dispatch_bot"]
        self.assertIn("鈴木さんに送付案内を作って", meta["指示原文"])
        self.assertEqual(meta["userId"], "U1")
        self.assertTrue(meta["pending_command_id"])
        # 一括API（Webhookが発射されない）を呼ばないこと（2026-07-04 修正の固定）
        self.bulk_mock.assert_not_awaited()

    async def test_completed_case_warning_in_confirmation(self):
        ps = self.patches(parsed(), hits=[hit(status="完了")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            reply = await self.send("鈴木さんに送付案内")
        self.assertIn("⚠ この案件は status=完了 です", reply)
        self.assertIn("起票します。", reply)

    async def test_number_selection_leads_to_confirmation(self):
        """複数候補→番号選択→復唱→OK→起票（番号選択は現対話への応答）"""
        hits = [hit("45"), hit("52", "鈴木花子", "手続き中")]
        ps = self.patches([parsed(), parsed(intent="confirm")], hits=hits)
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            await self.send("鈴木さんに送付案内")
            reply = await self.send("2")
            self.assertIn("鈴木花子さん（No.52・手続き中）に送付案内（委任契約書）を起票します。", reply)
            reply2 = await self.send("OK")
        self.assertIn("起票しました", reply2)
        fields = self.create_mock.await_args.args[1]
        self.assertEqual(fields["案件レコードID"], "52")


class TestPendingSafety(Base):
    async def test_double_ok_files_once(self):
        """二重OK: 起票は1回・2回目はリンク再掲（06 §3.1 単回消込）"""
        ps = self.patches([parsed(), parsed(intent="confirm"),
                           parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            await self.send("鈴木さんに送付案内")
            r1 = await self.send("OK")
            r2 = await self.send("OK")
        self.create_mock.assert_awaited_once()
        self.assertIn("起票しました", r1)
        self.assertIn("実行済みです（App 30 No.101）", r2)
        self.assertIn("show#record=101", r2)

    async def test_expired_ok(self):
        """30分経過後のOKは期限切れ（何も起票しない）"""
        ps = self.patches([parsed(), parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            await self.send("鈴木さんに送付案内")
            # 期限を強制超過
            p = confirm._pending["U1"]
            p.created_at -= confirm.PENDING_TTL_SEC + 1
            reply = await self.send("OK")
        self.assertEqual(reply, handler.MSG_EXPIRED)
        self.create_mock.assert_not_awaited()

    async def test_ok_without_pending(self):
        ps = self.patches(parsed(intent="confirm"))
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            reply = await self.send("OK")
        self.assertEqual(reply, handler.MSG_NO_PENDING)
        self.create_mock.assert_not_awaited()

    async def test_cancel_pending(self):
        """キャンセル→④の文言。その後のOKは pending なし扱い"""
        ps = self.patches([parsed(), parsed(intent="cancel"),
                           parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            await self.send("鈴木さんに送付案内")
            r1 = await self.send("キャンセル")
            r2 = await self.send("OK")
        self.assertEqual(r1, handler.MSG_CANCELLED_PENDING)
        self.assertEqual(r2, handler.MSG_NO_PENDING)
        self.create_mock.assert_not_awaited()

    async def test_interruption_invalidates_pending(self):
        """pending 有効中の別指示→旧 pending 無効化＋注記＋新しい復唱。
        その後のOKは新しい方を起票する（06 §3.1 割込み無効化）"""
        hits2 = [hit("77", "田中太郎", "受任")]
        parse_seq = [parsed(), parsed(customer_name="田中"), parsed(intent="confirm")]
        self.create_mock = AsyncMock(return_value="102")
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(side_effect=parse_seq)), \
             patch.object(case_search, "search_cases",
                          new=AsyncMock(side_effect=[[hit()], hits2])), \
             patch("hub.kintone.get_record",
                   new=AsyncMock(return_value={"顧客名": {"value": "田中太郎"},
                                               "住所": {"value": "X"},
                                               "郵便番号": {"value": ""}})), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=[])), \
             patch.object(enclosures, "list_options",
                          new=AsyncMock(return_value=list(ENC_OPTIONS))),              patch("hub.kintone.create_record", new=self.create_mock):
            await self.send("鈴木さんに送付案内")           # pending A
            r2 = await self.send("田中さんに送付案内")       # 割込み → pending B
            self.assertTrue(r2.startswith(handler.MSG_INTERRUPTED))
            self.assertIn("田中太郎さん（No.77・受任）", r2)
            r3 = await self.send("OK")
        self.assertIn("起票しました", r3)
        fields = self.create_mock.await_args.args[1]
        self.assertEqual(fields["案件レコードID"], "77", "OKは新しい pending に効く")

    async def test_pending_is_per_user(self):
        ps = self.patches([parsed(), parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            await self.send("鈴木さんに送付案内", user="U_owner1")
            reply = await self.send("OK", user="U_other")
        self.assertEqual(reply, handler.MSG_NO_PENDING)
        self.create_mock.assert_not_awaited()


class TestFilingGuardsAndFailure(Base):
    async def test_duplicate_guard_blocks_second_create(self):
        """第2層: 同一 pending_command_id のレコードが既にあれば作成しない（06 §3.3）"""
        ps = self.patches([parsed(), parsed(intent="confirm")], existing="99")
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            await self.send("鈴木さんに送付案内")
            reply = await self.send("OK")
        self.create_mock.assert_not_awaited()
        self.assertIn("起票済みです（App 30 No.99・二重実行を防止しました）", reply)
        # 検索クエリに command_id が含まれる
        query = self.search30_mock.await_args.args[1]
        self.assertIn("チャネル固有データ like", query)

    async def test_filing_failure_notifies_user_and_admin(self):
        """⑥ kintone APIエラー → 失敗返信＋管理者警報。pending は消込（再指示可能）"""
        ps = self.patches([parsed(), parsed(intent="confirm"),
                           parsed(intent="confirm")])
        alert = AsyncMock()
        self.create_mock.side_effect = kintone.KintoneError(500, "GAIA_XX", "boom")
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6], \
             patch("hub.notify.notify_admin_line", new=alert):
            await self.send("鈴木さんに送付案内")
            r1 = await self.send("OK")
            r2 = await self.send("OK")
        self.assertEqual(r1, handler.MSG_FILE_FAILED)
        alert.assert_awaited_once()
        self.assertIn("起票失敗", alert.await_args.args[0])
        self.assertEqual(r2, handler.MSG_NO_PENDING, "失敗後は pending なし（再指示でやり直し）")


class TestSingleRecordApi(Base):
    """2026-07-04 実機不具合の回帰: 起票は単票API（record.json）を使うこと。
    一括API（records.json）は kintone 仕様で「レコード追加」Webhook が発射されず、
    /hub/dispatch → prepare が走らない（レコードが下書きのまま止まる）"""

    async def test_filing_uses_single_record_api_only(self):
        ps = self.patches([parsed(), parsed(intent="confirm")])
        with ps[0], ps[1], ps[2], ps[3], ps[4], ps[5], ps[6]:
            await self.send("鈴木さんに送付案内")
            await self.send("OK")
        self.create_mock.assert_awaited_once()   # 単票 create_record が1回
        self.bulk_mock.assert_not_awaited()      # 一括 create_records はゼロ

    def test_filer_source_never_uses_bulk_api(self):
        """ソースレベルでも固定（create_records の再混入防止）"""
        import pathlib
        src = pathlib.Path(app30_filer.__file__).read_text(encoding="utf-8")
        self.assertIn("create_record(", src.replace("create_records(", ""))
        self.assertNotIn("await kintone.create_records(", src)


class TestApprovalPrinciple(unittest.TestCase):
    def test_filer_never_writes_beyond_draft(self):
        """起票は「下書き」のみ。発送ステータスを先に進めるコード・承認済への
        遷移コードが存在しないことをソースレベルで検査（絶対制約）"""
        import pathlib
        src = pathlib.Path(app30_filer.__file__).read_text(encoding="utf-8")
        self.assertIn('"発送ステータス": "下書き"', src)
        for word in ("承認待ち", "承認済", "発送処理中", "transition"):
            self.assertNotIn(f'"{word}"', src.replace('"発送ステータス": "下書き"', ""),
                             f"filer が {word} を書いている")


if __name__ == "__main__":
    unittest.main()
