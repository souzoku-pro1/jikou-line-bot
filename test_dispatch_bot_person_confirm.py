"""Bot語彙: 人物の確認（R4-2e T2・person_confirm タスク）のテスト

検証:
レジストリ登録（フック3点・既存語彙無改変）・フラグ無効の完全不発・
一覧集約（1メッセージ・現在値5種＋推定材料の添付）・指定解釈（単数・複数・全部・
複合1メッセージ・範囲外・解釈不能行・フォールスルー）・死亡日形式ガード
（再入力案内・pending 発行なし）・生死矛盾の拒否・二段確認（復唱の前→後全件明示・
復唱なしOKの拒否・復唱段階で書き込みゼロ）・人物ごと独立実行の結果報告・
案件指定（No.直指定・顧客名・同姓複数の番号選択）。T1コア / parser は全てモック。
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

from customer_directory import Candidate  # noqa: E402
from dispatch_bot import handler, parser, registry, person_confirm_task  # noqa: E402
from person_confirm import PersonRow  # noqa: E402
from dispatch_bot.person_confirm_task import parse_directives  # noqa: E402

_ENV = {"PERSON_MERGE_ENABLED": "1",
        "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
        "OFFICE_ATTORNEY": "大野太郎"}

PARSE_PC = {"intent": "task", "task_type": "person_confirm",
            "customer_name": "鈴木", "task_params": {}, "confidence": "high",
            "missing_fields": [], "clarification": None}
PARSE_CONFIRM = {"intent": "confirm", "task_type": None, "customer_name": None,
                 "task_params": {}, "confidence": "high", "missing_fields": [],
                 "clarification": None}

ROWS = [
    PersonRow(record_id="6", name="鈴木誠", meyose="確定", kakunin="未確認",
              alive="死亡", death_date="", decedent="no",
              hints=["死亡記載: 令和7年4月13日"]),
    PersonRow(record_id="7", name="鈴木香奈", meyose="確定", kakunin="未確認",
              alive="", death_date="", decedent="no"),
    PersonRow(record_id="8", name="香音", meyose="確定", kakunin="未確認",
              alive="", death_date="", decedent="no"),
]


def cand(rid="4", name="鈴木", status="相談カード"):
    return Candidate(record_id=str(rid), app_id="26", source="相談カード (相続)",
                     customer_name=name, status=status)


class TestParseDirectives(unittest.TestCase):
    """指定メッセージの解釈（純関数）"""

    def test_single_death_with_date(self):
        r = parse_directives("1は死亡2025-04-13", 3)
        self.assertTrue(r["ok"])
        self.assertEqual(r["changes"],
                         {1: {"生死区分": "死亡", "死亡日": "2025-04-13"}})

    def test_death_without_date_allowed(self):
        r = parse_directives("1は死亡", 3)
        self.assertEqual(r["changes"], {1: {"生死区分": "死亡"}})

    def test_multi_alive(self):
        r = parse_directives("2と3は生存", 3)
        self.assertEqual(r["changes"], {2: {"生死区分": "生存"},
                                        3: {"生死区分": "生存"}})

    def test_decedent(self):
        r = parse_directives("1を被相続人に", 3)
        self.assertEqual(r["changes"], {1: {"被相続人フラグ": "yes"}})

    def test_all_confirm(self):
        """「全部確認済みに」= 確認状態のみ全員一括"""
        r = parse_directives("全部確認済みに", 3)
        self.assertEqual(r["changes"], {1: {"確認状態": "確認済"},
                                        2: {"確認状態": "確認済"},
                                        3: {"確認状態": "確認済"}})

    def test_meyose_confirm(self):
        r = parse_directives("1と2の名寄せを確定", 3)
        self.assertEqual(r["changes"], {1: {"名寄せ確定": "確定"},
                                        2: {"名寄せ確定": "確定"}})

    def test_compound_multiline_message(self):
        """複数行・複数指定の1メッセージ一括（渋滞対策）"""
        text = "1は死亡2025-04-13\n1を被相続人に\n2と3は生存\n全部確認済みに"
        r = parse_directives(text, 3)
        self.assertTrue(r["ok"])
        self.assertEqual(r["changes"][1],
                         {"生死区分": "死亡", "死亡日": "2025-04-13",
                          "被相続人フラグ": "yes", "確認状態": "確認済"})
        self.assertEqual(r["changes"][2],
                         {"生死区分": "生存", "確認状態": "確認済"})
        self.assertEqual(r["changes"][3],
                         {"生死区分": "生存", "確認状態": "確認済"})

    def test_wareki_date_gets_guidance(self):
        """大字・和暦はエラーではなく再入力案内"""
        r = parse_directives("1は死亡令和7年4月13日", 3)
        self.assertFalse(r["ok"])
        self.assertEqual(r["reason"], "error")
        self.assertIn("YYYY-MM-DD", r["message"])

    def test_alive_dead_contradiction_rejected(self):
        r = parse_directives("1は死亡2025-04-13\n1は生存", 3)
        self.assertFalse(r["ok"])
        self.assertIn("矛盾", r["message"])

    def test_out_of_range(self):
        r = parse_directives("9は生存", 3)
        self.assertFalse(r["ok"])
        self.assertIn("1〜3", r["message"])

    def test_unmatched_falls_through(self):
        r = parse_directives("熊澤さんに送付案内を作って", 3)
        self.assertEqual(r["reason"], "unmatched")

    def test_partial_unparsable_line_reports(self):
        r = parse_directives("1は生存\nこれは謎の行", 3)
        self.assertFalse(r["ok"])
        self.assertIn("解釈できない行", r["message"])
        self.assertIn("謎の行", r["message"])


class _Base(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        handler.reset_sessions()

    def arm(self, *, parse=PARSE_PC, cands=None, rows=None, env=_ENV,
            apply_results=None):
        self.apply = AsyncMock(return_value=apply_results if apply_results
                               is not None else [
                                   {"record_id": "6", "name": "鈴木誠",
                                    "status": "updated",
                                    "fields": {"生死区分": "死亡",
                                               "死亡日": "2025-04-13"}}])
        patchers = [
            patch.object(parser, "parse_instruction",
                         new=AsyncMock(return_value=dict(parse))),
            patch.object(person_confirm_task, "list_candidates",
                         new=AsyncMock(return_value=list(
                             cands if cands is not None else [cand()]))),
            patch.object(person_confirm_task, "list_case_persons",
                         new=AsyncMock(return_value=list(
                             rows if rows is not None else ROWS))),
            patch.object(person_confirm_task, "apply_confirmations",
                         new=self.apply),
            patch.dict(os.environ, env),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    async def confirm_ok(self, user="U1"):
        with patch.object(parser, "parse_instruction",
                          new=AsyncMock(return_value=dict(PARSE_CONFIRM))):
            return await handler.handle_message(user, "OK")


class TestRegistryEntry(unittest.TestCase):
    def test_registered_with_hooks(self):
        spec = registry.get_task("person_confirm")
        self.assertIsNotNone(spec)
        self.assertIs(spec.flow_fn, person_confirm_task.flow)
        self.assertIs(spec.flow_reply_fn, person_confirm_task.flow_reply)
        self.assertIs(spec.execute_fn, person_confirm_task.execute)

    def test_catalog_includes_person_confirm(self):
        self.assertIn("person_confirm", registry.catalog_for_prompt())


class TestFlow(_Base):
    async def test_flag_off_is_inert(self):
        self.arm(env={**_ENV, "PERSON_MERGE_ENABLED": ""})
        reply = await handler.handle_message("U1", "鈴木さんの人物を確認して")
        self.assertEqual(reply, person_confirm_task.MSG_DISABLED)
        person_confirm_task.list_case_persons.assert_not_awaited()

    async def test_list_aggregated_with_hints(self):
        """一覧集約: 1メッセージに全員の現在値5種＋推定材料"""
        self.arm()
        reply = await handler.handle_message("U1", "鈴木さんの人物を確認して")
        self.assertIn("No.4 鈴木 の人物 3名:", reply)
        self.assertIn("1. No.6 鈴木誠", reply)
        self.assertIn("名寄せ=確定／確認=未確認／生死=死亡／死亡日=なし／被相続人=no",
                      reply)
        self.assertIn("💡死亡記載: 令和7年4月13日", reply, "推定材料の添付")
        self.assertIn("2. No.7 鈴木香奈", reply)
        self.assertIn("3. No.8 香音", reply)
        self.assertIn("受理形式", reply)

    async def test_no_persons(self):
        self.arm(rows=[])
        reply = await handler.handle_message("U1", "鈴木さんの人物を確認して")
        self.assertEqual(reply, person_confirm_task.MSG_NO_PERSONS)

    async def test_direct_case_number(self):
        self.arm(parse={**PARSE_PC, "customer_name": None,
                        "task_params": {"case_record_id": "4"}})
        reply = await handler.handle_message("U1", "No.4の人物を確認して")
        self.assertIn("No.4 鈴木 の人物 3名:", reply)

    async def test_ambiguous_customer_numbered_selection(self):
        self.arm(cands=[cand("4", "鈴木一郎"), cand("9", "鈴木二郎")])
        reply = await handler.handle_message("U1", "鈴木さんの人物を確認して")
        self.assertIn("候補顧客が2件あります", reply)
        reply2 = await handler.handle_message("U1", "2")
        self.assertIn("No.9 鈴木二郎 の人物 3名:", reply2)


class TestConfirmAndExecute(_Base):
    async def _open_list(self, user="U1"):
        await handler.handle_message(user, "鈴木さんの人物を確認して")

    async def test_two_step_confirmation_with_before_after(self):
        """復唱: レコードNo・氏名・変更前→後を全件明示。復唱段階で書き込みゼロ"""
        self.arm()
        await self._open_list()
        reply = await handler.handle_message(
            "U1", "1は死亡2025-04-13\n1を被相続人に\n2と3は生存")
        self.assertIn("以下の3名の確認内容を書き込みます", reply)
        self.assertIn("No.6 鈴木誠", reply)
        self.assertIn("生死区分「死亡」→「死亡」", reply)
        self.assertIn("死亡日「未設定」→「2025-04-13」", reply)
        self.assertIn("被相続人フラグ「no」→「yes」", reply)
        self.assertIn("No.7 鈴木香奈: 生死区分「未設定」→「生存」", reply)
        self.assertIn("OK / キャンセル", reply)
        self.apply.assert_not_awaited()

    async def test_ok_executes_and_reports(self):
        self.arm()
        await self._open_list()
        await handler.handle_message("U1", "1は死亡2025-04-13")
        reply = await self.confirm_ok()
        self.apply.assert_awaited_once()
        changes = self.apply.await_args.args[0]
        self.assertEqual(changes[0]["record_id"], "6")
        self.assertEqual(changes[0]["fields"],
                         {"生死区分": "死亡", "死亡日": "2025-04-13"})
        self.assertIn("No.6 鈴木誠: 生死区分=死亡、死亡日=2025-04-13 を"
                      "書き込みました", reply)
        self.assertIn("kintone内部のみ・対外送信なし", reply)

    async def test_partial_failure_reported_per_person(self):
        """人物ごと独立実行の結果報告（1件の失敗が他を止めない）"""
        self.arm(apply_results=[
            {"record_id": "6", "name": "鈴木誠", "status": "updated",
             "fields": {"確認状態": "確認済", "確認者": "大野太郎",
                        "確認日時": "2026-07-07T12:00:00Z"}},
            {"record_id": "7", "name": "鈴木香奈", "status": "error",
             "reason": "boom"}])
        await self._open_list()
        await handler.handle_message("U1", "全部確認済みに")
        reply = await self.confirm_ok()
        self.assertIn("No.6 鈴木誠: 確認状態=確認済 を書き込みました", reply)
        self.assertIn("No.7 鈴木香奈: boom", reply)

    async def test_wareki_date_guidance_no_pending(self):
        """和暦死亡日: 再入力案内のみ・pending 発行なし（OKしても実行されない）"""
        self.arm()
        await self._open_list()
        reply = await handler.handle_message("U1", "1は死亡令和7年4月13日")
        self.assertIn("YYYY-MM-DD", reply)
        reply2 = await self.confirm_ok()
        self.assertEqual(reply2, handler.MSG_NO_PENDING)
        self.apply.assert_not_awaited()

    async def test_contradiction_rejected(self):
        self.arm()
        await self._open_list()
        reply = await handler.handle_message("U1", "1は死亡2025-04-13\n1は生存")
        self.assertIn("矛盾", reply)
        self.apply.assert_not_awaited()

    async def test_ok_without_confirmation_refused(self):
        self.arm()
        reply = await self.confirm_ok()
        self.assertEqual(reply, handler.MSG_NO_PENDING)
        self.apply.assert_not_awaited()

    async def test_unrelated_text_not_consumed(self):
        self.arm()
        await self._open_list()
        session = handler._get_session("U1")
        handled, _ = await person_confirm_task.flow_reply(
            "U1", "熊澤さんに送付案内を作って", session)
        self.assertFalse(handled)


if __name__ == "__main__":
    unittest.main()
