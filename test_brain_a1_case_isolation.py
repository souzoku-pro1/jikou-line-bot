"""BRAIN-A1 T-A 案件分離（v3 §10-1）

固定する仕様:
- アプリ間の同番号（App 26 の 3 と App 40 の 3）: App 30 の同期は 案件アプリID=APP_HOUKI
  の行だけを取得し、別アプリ参照の行は保留（ref_other_app）にも入らない＝混入ゼロ
- 同一 LINE ID の複数案件: App 28 は App 40 の LINEユーザーID の現在値に**ちょうど 1 件**
  一致するときのみ取り込む（App 26 側の同 ID は台帳に無いので影響しない・複数一致は
  取り込まない＝R5）
- 本文中の他案件番号は無視（紐付けは LINE ID の一致のみ・本文は参照にしない）
- 矛盾参照（案件アプリID=App 40 だが ユニット種別≠相続放棄）・参照先不在 → 保留に入り
  業務台帳へは入らない（候補は自動確定しない）
"""

import unittest

from brain_test_support import (LINE_A, LINE_B, BrainDbMixin, app28, app30, app40,
                                run)
from hub import brain_ledger as ledger
from hub import brain_link
from hub import brain_sync as sync


class TestCaseIsolation(BrainDbMixin):
    def test_same_record_number_across_apps_does_not_mix(self):
        # App 40 の 3 と App 26 の 3。App 30 には両方を指す行がある
        self.fake.data["40"] = [app40(3, 1)]
        self.fake.data["30"] = [app30(50, 1, 案件レコードID="3"),
                                app30(51, 1, 案件アプリID="26", 案件レコードID="3")]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP30)
            facts = await ledger.list_case_facts("40", "3")
            srcs = {(f["source_app_id"], f["source_record_id"]) for f in facts}
            self.assertIn(("30", "50"), srcs)
            self.assertNotIn(("30", "51"), srcs)
            # App 26 の 3 を案件キーに持つ事実はゼロ（別アプリ参照は取得もしない）
            self.assertEqual(await ledger.list_case_facts("26", "3"), [])
            self.assertEqual(await ledger.list_pending_links(), [])
            # 発行 query は 案件アプリID=40 の絞込を必ず含む
            q30 = [q for a, q in self.fake.calls if a == "30" and "order by 更新日時" in q]
            self.assertTrue(q30 and all('案件アプリID = "40"' in q for q in q30))
        run(body())

    def test_other_app_reference_is_hold_even_if_returned(self):
        # 万一 query 絞込を素通りしても、判定は別アプリ参照を保留にする（純粋関数）
        d = brain_link.decide_app30_reference(app30(1, 1, 案件アプリID="26"), "40", True)
        self.assertEqual((d.trust, d.case_key, d.reason), ("hold", None, "ref_other_app"))

    def test_same_line_id_multiple_cases_not_ingested(self):
        # App 40 に同一 LINE ID が 2 件（一意制約が無い環境を模擬）→ App 28 は取り込まない
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, LINEユーザーID=LINE_A)]
        self.fake.data["28"] = [app28(100, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual(r["status"], "ok")
            self.assertEqual([f for f in await ledger.list_case_facts("40", "1")
                              if f["source_app_id"] == "28"], [])
            self.assertEqual([f for f in await ledger.list_case_facts("40", "2")
                              if f["source_app_id"] == "28"], [])
            for case in (("40", "1"), ("40", "2")):
                self.assertEqual(await ledger.list_case_events(*case, current_only=False), [
                    e for e in await ledger.list_case_events(*case, current_only=False)
                    if e["source_app_id"] != "28"])
            self.assertEqual(await ledger.latest_known_revision("28", "100"), None)
            self.assertEqual(await ledger.list_pending_links(), [])   # 保留にもしない
            self.assertEqual(await ledger.list_holds(), [])           # detached にもしない
        run(body())

    def test_chat_links_by_line_id_only_and_ignores_numbers_in_body(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, LINEユーザーID=LINE_B)]
        self.fake.data["28"] = [
            app28(100, 1, message="No.2 の件ですが案件 2 について"),      # 本文の他案件番号
            app28(101, 1, line_user_id=LINE_B, message="こちらは 2"),
            app28(102, 1, category="その他判断系"),                       # 時効カテゴリ
            app28(103, 1, line_user_id="U" + "9" * 32),                   # 一致なし
            app28(104, 1, category="画像解析:houki:evt1", message="読解結果"),
            app28(105, 1, category="画像解析:jikou:evt2", message="時効側"),
        ]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP28)
            # R9: App 28 の行は fact ではなく case_event（種別=会話）
            c1 = {e["source_record_id"] for e in await ledger.list_case_events("40", "1")
                  if e["source_app_id"] == "28"}
            c2 = {e["source_record_id"] for e in await ledger.list_case_events("40", "2")
                  if e["source_app_id"] == "28"}
            self.assertEqual(c1, {"100", "104"})
            self.assertEqual(c2, {"101"})
            for case in (("40", "1"), ("40", "2")):
                self.assertEqual([f for f in await ledger.list_case_facts(*case)
                                  if f["source_app_id"] == "28"], [])
            for rid in ("102", "103", "105"):
                self.assertIsNone(await ledger.latest_known_revision("28", rid))
            # 一意判定は正本（App 40 を LINEユーザーID で limit 2 検索）で行う（BA-04）
            q = [q for a, q in self.fake.calls if a == "40" and "LINEユーザーID" in q]
            self.assertTrue(q and all(q_.endswith("limit 2") for q_ in q))
        run(body())

    def test_contradictory_and_missing_references_are_held(self):
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["30"] = [
            app30(60, 1),                                        # 正常
            app30(61, 1, ユニット種別="時効援用"),                 # 矛盾参照
            app30(62, 1, 案件レコードID="999"),                   # 参照先不在
            app30(63, 1, 案件レコードID="abc"),                   # 数字でない
        ]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            r = await sync.sync_target(sync.TARGET_APP30)
            self.assertEqual(r["held"], 3)
            srcs = {f["source_record_id"] for f in await ledger.list_case_facts("40", "1")
                    if f["source_app_id"] == "30"}
            self.assertEqual(srcs, {"60"})
            pending = {p["source_record_id"]: p for p in await ledger.list_pending_links()}
            self.assertEqual(set(pending), {"61", "62", "63"})
            self.assertEqual(pending["61"]["hold_reason"], "unit_mismatch")
            self.assertEqual(pending["62"]["hold_reason"], "ref_missing")
            self.assertEqual(pending["63"]["hold_reason"], "ref_not_digits")
            # 候補は列挙されるが自動確定しない（案件 999 の事実は作られない）
            self.assertEqual(pending["61"]["candidates"][0]["trust"], "candidate")
            self.assertEqual(await ledger.list_case_facts("40", "999"), [])
            # 台帳の状態は held（業務台帳に入らない）
            st = await ledger.ingest_status(
                ledger.SourceRef("30", "61", 1, *sync.CONVERTER[sync.TARGET_APP30]), None)
            self.assertEqual((st["known"], st["state"], st["has_current_facts"]),
                             (True, "held", False))
            # 保留の繰り返し同期で履歴が増えない
            await sync.sync_target(sync.TARGET_APP30)
            self.assertEqual(len(await ledger.list_pending_links()), 3)
            last = await ledger.latest_link("30", "61")
            self.assertEqual(last["reason"], "unit_mismatch")
        run(body())

    def test_app40_exists_check_uses_ledger_then_kintone(self):
        # 参照先が台帳に無ければ kintone で実在確認（取得失敗は None=保留）
        async def body():
            self.fake.data["40"] = [app40(7, 1)]
            self.assertIs(await sync._app40_exists("7"), True)
            self.assertIs(await sync._app40_exists("8"), False)
            self.fake.raise_all = True
            self.assertIsNone(await sync._app40_exists("7"))
            self.assertEqual(brain_link.decide_app30_reference(
                app30(1, 1, 案件レコードID="7"), "40", None).reason, "ref_missing")
        run(body())

    def test_case_key_requires_app_and_record(self):
        # レコード番号単独では識別しない: 同番号の別案件キーは別の事実集合
        async def body():
            src_a = ledger.SourceRef("30", "1", 1, "app30_record", "1")
            f = [ledger.FactIn("case", "app30.件名", "text", "x", ledger.make_locator("件名"))]
            await ledger.ingest_source(src_a, ("40", "3"), f)
            self.assertEqual(len(await ledger.list_case_facts("40", "3")), 1)
            self.assertEqual(await ledger.list_case_facts("26", "3"), [])
            self.assertEqual(await ledger.list_case_facts("40", "30"), [])
        run(body())

    # ── fix1: BA-04（R5 の一意判定は正本・fail-closed）/ R10（App 28 の関連喪失） ──

    def test_line_id_uniqueness_is_decided_by_source_of_truth(self):
        # BA-04: 台帳には App 40 No.1 だけ・正本には同じ LINE ID の No.2 もある・
        # App 40 の同期は失敗中（台帳が正本に追いつけない）→ App 28 の採用は 0
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1)]

        async def chats(case):
            return [e for e in await ledger.list_case_events(*case) if e["source_app_id"] == "28"]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            self.fake.data["40"].append(app40(2, 1, "2026-09-20T02:00:00Z"))
            self.fake.fail_at = {len(self.fake.calls) + 1}
            self.assertEqual((await sync.sync_target(sync.TARGET_APP40))["status"], "failed")
            # 台帳だけを見れば 1 件（＝台帳基準なら誤って採用してしまう状態）
            self.assertEqual(await ledger.find_case_by_item_value(sync.APP40_LINE_ID_ITEM, LINE_A),
                             [("40", "1")])
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual(r["status"], "ok")
            self.assertEqual(await chats(("40", "1")), [])
            self.assertEqual(await chats(("40", "2")), [])
            self.assertIsNone(await ledger.latest_known_revision("28", "100"))
            self.assertEqual(await ledger.list_pending_links(), [])
            self.assertEqual(await ledger.list_holds(), [])
            q = [q for a, q in self.fake.calls if a == "40" and "LINEユーザーID" in q][-1]
            self.assertEqual(q, f'LINEユーザーID = "{LINE_A}" limit 2')
            # 検索失敗も採用しない（fail-closed）。R12: 処理不要と区別し partial・pending_recheck
            self.fake.data["40"] = [app40(1, 1)]
            self.fake.fail_at = {len(self.fake.calls) + 2}       # 1 回目=App 28 ページ・2 回目=LINE 検索
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual((r["status"], r["pending_recheck"]), ("partial", 1))
            self.assertIsNone(await ledger.latest_known_revision("28", "100"))
            # 正本でちょうど 1 件なら採用（fact ではなく会話の出来事・R9）
            self.fake.fail_at = set()
            await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual([e["source_record_id"] for e in await chats(("40", "1"))], ["100"])
        run(body())

    def test_lost_uniqueness_or_category_detaches_ingested_chat(self):
        # BA-04/R10: 取込済み App 28 行が再照合で一意不成立／閉集合外になったら利用停止
        # （return None で放置しない・保留にもしない＝detached）
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1), app28(101, 1)]

        async def chats(current_only=True):
            return [e for e in await ledger.list_case_events("40", "1", current_only=current_only)
                    if e["source_app_id"] == "28"]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual(len(await chats()), 2)
            n_events = len(await chats(current_only=False))
            # 正本に同じ LINE ID の 2 件目（台帳は未同期）→ 再照合で一意不成立
            self.fake.data["40"].append(app40(2, 1, "2026-09-20T02:00:00Z"))
            rc = await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual((rc["status"], rc["moved"]), ("ok", 2))
            self.assertEqual(await chats(), [])
            self.assertEqual({e["invalid_reason"] for e in await chats(current_only=False)},
                             {"link_lost"})
            self.assertEqual(len(await chats(current_only=False)), n_events)     # 削除なし
            last = await ledger.latest_link("28", "100")
            self.assertEqual((last["trust_level"], last["reason"], last["prev_case"], last["new_case"]),
                             ("hold", "line_id_not_unique", ("40", "1"), None))
            self.assertEqual(await ledger.list_pending_links(), [])          # R5: 保留にしない
            self.assertEqual({(h["source_record_id"], h["state"], h["hold_reason"])
                              for h in await ledger.list_holds()},
                             {("100", "detached", "line_id_not_unique"),
                              ("101", "detached", "line_id_not_unique")})
            self.assertIsNone(await ledger.current_case_of_source("28", "100"))
            # 一意に戻れば同じ出来事が同じ案件へ復帰（行は増えない）
            self.fake.data["40"] = [app40(1, 1)]
            await sync.recheck_target(sync.TARGET_APP28)
            self.assertEqual(len(await chats()), 2)
            self.assertEqual(len(await chats(current_only=False)), n_events)
            self.assertEqual(await ledger.list_holds(), [])
            # category が閉集合外に編集された新 revision → 通常同期の経路でも利用停止
            self.fake.data["28"][0] = app28(100, 2, "2026-09-21T01:00:00Z", category="その他判断系")
            self.assertEqual((await sync.sync_target(sync.TARGET_APP28))["status"], "ok")
            self.assertEqual({e["source_record_id"] for e in await chats()}, {"101"})
            self.assertEqual((await ledger.latest_link("28", "100"))["reason"], "category_out_of_set")
            # 検索失敗の再照合は既存の関連を変えない
            self.fake.raise_all = True
            self.assertEqual((await sync.recheck_target(sync.TARGET_APP28))["status"], "failed")
            self.assertEqual({e["source_record_id"] for e in await chats()}, {"101"})
        run(body())


class TestDetachedLatestSeen(BrainDbMixin):
    """fix5 BA-24（R11・R13 の適用漏れ）: 既知出典の「処理不要」経路（判定 None かつ関連解除済み）
    でも照合した最新 revision を latest_seen_revision に同一 tx で前進させる。解除は維持し、
    遅着の旧 revision は履歴のみ（会話イベントは無効のまま・latest は進まない）。"""

    async def _chats(self, current_only=True):
        return [e for e in await ledger.list_case_events("40", "1", current_only=current_only)
                if e["source_app_id"] == "28"]

    async def _hist(self):
        from hub.db import session_scope
        import sqlalchemy as sa
        async with session_scope() as session:
            return int((await session.execute(
                sa.select(sa.func.count()).select_from(ledger.link_history))).scalar())

    async def _run_path(self, path):
        async def step(record):
            self.fake.data["28"] = [record]
            if path == "sync":
                return await sync.sync_target(sync.TARGET_APP28)
            return await sync.recheck_target(sync.TARGET_APP28)
        self.fake.data["40"] = [app40(1, 1)]
        await sync.sync_target(sync.TARGET_APP40)
        # rev1 対象 category → 案件 1 に関連（初回は同期でしか起きない）
        self.fake.data["28"] = [app28(100, 1, "2026-09-21T00:00:00Z")]
        await sync.sync_target(sync.TARGET_APP28)
        self.assertEqual(await ledger.current_case_of_source("28", "100"), ("40", "1"))
        self.assertEqual(len(await self._chats()), 1)
        self.assertEqual(await ledger.latest_known_revision("28", "100"), 1)
        h0 = await self._hist()
        # rev2 対象外 → 解除・latest 2
        await step(app28(100, 2, "2026-09-21T01:00:00Z", category="その他判断系"))
        self.assertIsNone(await ledger.current_case_of_source("28", "100"))
        self.assertEqual(await self._chats(), [])
        self.assertEqual({e["invalid_reason"] for e in await self._chats(current_only=False)},
                         {"link_lost"})
        self.assertEqual(await ledger.latest_known_revision("28", "100"), 2)
        self.assertEqual(await self._hist(), h0 + 1)
        # rev4 対象外 → 解除維持・latest 4（処理不要経路でも同一 tx で前進・復活させない）
        r = await step(app28(100, 4, "2026-09-21T02:00:00Z", category="その他判断系"))
        self.assertEqual(r["status"], "ok")
        self.assertIsNone(await ledger.current_case_of_source("28", "100"))
        self.assertEqual(await self._chats(), [])
        self.assertEqual(await ledger.latest_known_revision("28", "100"), 4)
        self.assertEqual(await self._hist(), h0 + 1)
        self.assertEqual([(h["source_record_id"], h["state"]) for h in await ledger.list_holds()],
                         [("100", "detached")])
        # 遅着 rev3 対象 category → 履歴のみ・関連は解除のまま・会話は無効のまま・latest 4 のまま
        r = await step(app28(100, 3, "2026-09-21T03:00:00Z"))
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r.get("inserted", 0), 0)
        self.assertIsNone(await ledger.current_case_of_source("28", "100"))
        self.assertEqual(await self._chats(), [])
        self.assertEqual({e["invalid_reason"] for e in await self._chats(current_only=False)},
                         {"link_lost"})
        self.assertEqual(await ledger.latest_known_revision("28", "100"), 4)
        self.assertEqual(await self._hist(), h0 + 1)
        st = await ledger.ingest_status(
            ledger.SourceRef("28", "100", 3, *sync.CONVERTER[sync.TARGET_APP28]), None)
        self.assertEqual((st["known"], st["state"]), (True, "detached"))   # 履歴として保存
        self.assertEqual([(h["source_record_id"], h["state"]) for h in await ledger.list_holds()],
                         [("100", "detached")])

    def test_detached_source_advances_latest_seen_without_reattach_sync(self):
        run(self._run_path("sync"))

    def test_detached_source_advances_latest_seen_without_reattach_recheck(self):
        run(self._run_path("recheck"))


if __name__ == "__main__":
    unittest.main()
