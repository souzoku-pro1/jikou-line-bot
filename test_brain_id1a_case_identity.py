"""BRAIN-ID-1a 案件本体・識別子・case_id 基準（案件脳_設計_v4.3.md §3-1・§3-2・§14）

固定する仕様:
- 案件識別子（kintone_record 等）の重なり禁止はアプリ側検査（同一 tx・IdentityOverlap）で
  効く（Postgres の EXCLUDE 制約は tools/tracking_pg_harness.py で実走・R29）。有効終了後の
  再付与は可。関係者識別子（line_user）は複数案件可
- App 40 の同期が識別子（kintone_record・line_user）を作成・更新する（LINE ID の変更で有効終了）
- App 28 の判定は identity のみ（kintone を検索しない・R29）。App 28 の取込・再照合で
  source_ingest.line_user_id が埋まり、API の応答（PWA）には出ない（RV-10）
- case_event.idem_key は case_id 内包（R28）
- count_running_runs / stop_stale_runs（older_than=15 分・stopped_stale・R28）
- 未登録案件: 鮮度 unregistered／紐付く出典が synced なら synced（R31）。relink の訂正先に
  未登録案件を指定できる（R15 の範囲内）
- PWA: 案件は ?case={case_id}（識別子検索 /app/api/brain/case で案件キー→case_id）
"""

import datetime
import os
import re
import unittest
from unittest.mock import patch

import sqlalchemy as sa

from brain_test_support import (LINE_A, LINE_B, LINE_C, BrainDbMixin, app28, app30, app40,
                                cid, run)
from hub import brain_ledger as ledger
from hub import brain_link
from hub import brain_sync as sync
from hub.db import session_scope

_UTC = datetime.timezone.utc


async def _new_unregistered_case(created_via="memo"):
    """1b で作られる未登録案件（本票では台帳の内部関数で用意する）。"""
    async with session_scope() as session:
        return await ledger._new_case_tx(session, registration="unregistered",
                                         created_via=created_via, now=ledger._now())


class TestCaseIdentity(BrainDbMixin):
    def test_vocabulary_and_tables_pinned(self):
        self.assertEqual(ledger.CASE_IDENTITY_KINDS, ("kintone_record", "receipt_number", "drive_folder"))
        self.assertEqual(ledger.RELATION_IDENTITY_KINDS, ("line_user",))
        self.assertEqual(ledger.REGISTRATION_STATES, ("registered", "unregistered"))
        self.assertEqual(ledger.CASE_STATES, ("active", "merged"))
        self.assertEqual(ledger.CASE_CREATED_VIA, ("sync", "memo", "manual", "backfill"))
        self.assertEqual(ledger.FRESHNESS_STATES,
                         ("synced", "partial", "incomplete", "error", "stopped", "unregistered"))
        self.assertEqual(ledger.RUN_STATES[-1], "stopped_stale")
        self.assertEqual(ledger.STALE_RUN_MINUTES, 15)
        self.assertEqual(ledger.kintone_namespace("sub", "40"), "kintone:sub:40")
        self.assertEqual(ledger.line_namespace("ch"), "line:ch")
        self.assertEqual(ledger.app_id_of_namespace("kintone:sub:40"), "40")
        self.assertIsNone(ledger.app_id_of_namespace("line:ch"))
        self.assertEqual(sync.kintone_namespace("40"), "kintone:testsub:40")
        self.assertEqual(sync.line_namespace(), "line:souzoku-houki")
        for name in ("case", "case_identity", "merge_history", "subject_merge_history"):
            self.assertIn(name, ledger.metadata.tables)
        self.assertIn("line_user_id", ledger.source_ingest.c)
        self.assertIn("case_id", ledger.source_ingest.c)
        self.assertTrue(ledger.source_ingest.c.case_id.nullable)
        for t in (ledger.case_fact, ledger.case_event, ledger.case_derivation):
            self.assertFalse(t.c.case_id.nullable)
        self.assertIn("prev_case_id", ledger.link_history.c)
        self.assertIn("new_case_id", ledger.link_history.c)
        # R28: idem_key は case_id 内包・A1 形式との往復は決定的
        key = ledger.event_idem_key(7, "28", "100", "-", "chat_message", "message/-/-/-/-")
        self.assertEqual(key, "c7|28|100|-|chat_message|message/-/-/-/-")
        self.assertEqual(ledger.event_revision_part(key), "-")
        legacy = ledger.legacy_event_idem_key("40", "1", "30", "5", "3", "shipping_status_observed",
                                              "発送ステータス/-/-/-/-")
        self.assertEqual(legacy.split("|")[:2], ["40", "1"])
        self.assertEqual(ledger.event_revision_part(legacy), "3")

    def test_case_identity_overlap_is_rejected_in_app_check(self):
        async def body():
            a = await ledger.ensure_case_for_record("kintone:testsub:40", "40", "1")
            b = await ledger.ensure_case_for_record("kintone:testsub:40", "40", "2")
            self.assertEqual(await ledger.ensure_case_for_record("kintone:testsub:40", "40", "1"), a)
            self.assertNotEqual(a, b)
            # 同一 (名前空間, 種別, 値) の有効な案件識別子は別案件へ付けられない
            with self.assertRaises(ledger.IdentityOverlap) as cm:
                await ledger.add_case_identity(b, "kintone:testsub:40", "kintone_record", "1")
            self.assertEqual(cm.exception.reason, "case_identity_overlap")
            with self.assertRaises(ledger.IdentityOverlap):
                await ledger.add_case_identity(a, "kintone:testsub:40", "kintone_record", "1")
            # 名前空間が違えば別の識別子・受付番号／drive_folder も案件識別子
            await ledger.add_case_identity(b, "kintone:other:40", "kintone_record", "1")
            await ledger.add_case_identity(a, "receipt:office", "receipt_number", "R-1")
            with self.assertRaises(ledger.IdentityOverlap):
                await ledger.add_case_identity(b, "receipt:office", "receipt_number", "R-1")
            await ledger.add_case_identity(a, "drive", "drive_folder", "F-1")
            # 関係者識別子は複数案件可
            await ledger.add_case_identity(a, "line:souzoku-houki", "line_user", LINE_A)
            await ledger.add_case_identity(b, "line:souzoku-houki", "line_user", LINE_A)
            self.assertEqual(await ledger.active_cases_for_relation(
                "line:souzoku-houki", "line_user", LINE_A), sorted([a, b]))
            # 有効終了後は再付与できる（削除しない・履歴が残る）
            async with session_scope() as session:
                await session.execute(sa.update(ledger.case_identity).where(
                    ledger.case_identity.c.case_id == a,
                    ledger.case_identity.c.kind == "receipt_number").values(valid_to=ledger._now()))
            await ledger.add_case_identity(b, "receipt:office", "receipt_number", "R-1")
            idents = await ledger.list_case_identities(a)
            self.assertEqual({(i["kind"], i["value"], bool(i["valid_to"])) for i in idents},
                             {("kintone_record", "1", False), ("receipt_number", "R-1", True),
                              ("drive_folder", "F-1", False), ("line_user", None, False)})   # RV-10
            # 閉集合外・空値・不明案件は拒否
            for bad in (dict(kind="passport"), dict(value=""), dict(namespace="")):
                args = {**dict(namespace="ns", kind="drive_folder", value="x"), **bad}
                with self.assertRaises(ledger.LedgerError):
                    await ledger.add_case_identity(a, args["namespace"], args["kind"], args["value"])
            with self.assertRaises(ledger.LedgerError):
                await ledger.add_case_identity(999, "ns", "drive_folder", "x")
            # 同じ tx 内の検査（FOR UPDATE）: _assert_case_identity_free は有効行があれば拒否
            async with session_scope() as session:
                with self.assertRaises(ledger.IdentityOverlap):
                    await ledger._assert_case_identity_free(session, "kintone:testsub:40",
                                                            "kintone_record", "2")
                await ledger._assert_case_identity_free(session, "kintone:testsub:40",
                                                        "kintone_record", "3")
        run(body())

    def test_app40_sync_creates_and_updates_identities(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, "2026-09-20T02:00:00Z", LINEユーザーID=LINE_B)]

        async def rel(value):
            return await ledger.active_cases_for_relation(sync.line_namespace(), "line_user", value)

        async def body():
            self.assertIsNone(await ledger.resolve_case(("40", "1")))
            r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(r["status"], "ok")
            c1, c2 = await cid("40", "1"), await cid("40", "2")
            self.assertTrue(c1 and c2 and c1 != c2)
            info = await ledger.case_info(c1)
            self.assertEqual((info["registration"], info["status"], info["created_via"],
                              info["case_app_id"], info["case_record_id"]),
                             ("registered", "active", "sync", "40", "1"))
            self.assertEqual((await rel(LINE_A), await rel(LINE_B)), ([c1], [c2]))
            # LINE ID の変更: 旧値は有効終了（削除しない）・新値を追加。形式外は「無し」
            self.fake.data["40"] = [app40(1, 2, "2026-09-20T03:00:00Z", LINEユーザーID=LINE_C),
                                    app40(2, 2, "2026-09-20T03:00:00Z", LINEユーザーID="bad")]
            self.assertEqual((await sync.sync_target(sync.TARGET_APP40))["status"], "ok")
            self.assertEqual((await rel(LINE_A), await rel(LINE_B), await rel(LINE_C)), ([], [], [c1]))
            idents = await ledger.list_case_identities(c1)
            self.assertEqual([(i["kind"], bool(i["valid_to"]), i["reason"]) for i in idents],
                             [("kintone_record", False, "case_created"),
                              ("line_user", True, "app40_field_changed"),
                              ("line_user", False, "app40_sync")])
            # 欄が無い（部分取得）新版は識別子を変えない
            self.fake.data["40"] = [app40(1, 3, "2026-09-20T04:00:00Z")]
            del self.fake.data["40"][0]["LINEユーザーID"]
            self.assertEqual((await sync.sync_target(sync.TARGET_APP40))["status"], "ok")
            self.assertEqual(await rel(LINE_C), [c1])
            # 同期は App 40 の新レコードを見つけたら必ず新しい case を起こす（氏名で結合しない）
            self.fake.data["40"] = [app40(1, 3, "2026-09-20T04:00:00Z", LINEユーザーID=LINE_C),
                                    app40(3, 1, "2026-09-20T05:00:00Z", LINEユーザーID=LINE_C)]
            self.assertEqual((await sync.sync_target(sync.TARGET_APP40))["status"], "ok")
            c3 = await cid("40", "3")
            self.assertTrue(c3 and c3 not in (c1, c2))
            self.assertEqual(await rel(LINE_C), sorted([c1, c3]))
            ov = await ledger.sync_overview()
            self.assertEqual(ov["cases"], {"registered": 3})
            self.assertEqual(ov["running_runs"], 0)
            # R29 再開順序: revision の変わらない既知レコードでも、追跡再照合の一巡が identity を
            # 正本の現在値で揃える（切替直後に line_user 識別子が無い案件を埋める）
            async with session_scope() as session:
                await session.execute(sa.delete(ledger.case_identity).where(
                    ledger.case_identity.c.kind == ledger.IDENTITY_LINE_USER))
            self.assertEqual(await rel(LINE_C), [])
            rc = await sync.recheck_target(sync.TARGET_APP40)
            self.assertEqual((rc["status"], rc["complete"], rc["moved"]), ("ok", True, 0))
            self.assertEqual(await rel(LINE_C), sorted([c1, c3]))
            n_before = len(await ledger.list_case_identities(c1))
            await sync.recheck_target(sync.TARGET_APP40)                  # 冪等（行は増えない）
            self.assertEqual(len(await ledger.list_case_identities(c1)), n_before)
        run(body())

    def test_app28_decision_is_identity_only_and_line_user_id_stays_in_db(self):
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["28"] = [app28(100, 1), app28(101, 1, line_user_id=LINE_B),
                                app28(102, 1, category="その他判断系")]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            n = len(self.fake.calls)
            r = await sync.sync_target(sync.TARGET_APP28)
            self.assertEqual(r["status"], "ok")
            # kintone への呼び出しは App 28 のページ取得だけ（App 40 の LINE 検索は無い）
            self.assertEqual([a for a, _q in self.fake.calls[n:]], ["28"])
            c1 = await cid("40", "1")
            self.assertEqual(await ledger.current_case_of_source("28", "100"), c1)
            self.assertIsNone(await ledger.current_case_of_source("28", "101"))   # 識別子なし＝不採用
            self.assertIsNone(await ledger.latest_known_revision("28", "102"))
            # R30: 取込行に line_user_id（DB 内のみ）。不採用（識別子なし）でも行は作らない
            async with session_scope() as session:
                rows = (await session.execute(sa.select(
                    ledger.source_ingest.c.source_record_id, ledger.source_ingest.c.line_user_id).where(
                    ledger.source_ingest.c.source_app_id == "28"))).fetchall()
            self.assertEqual({(r[0], r[1]) for r in rows}, {("100", LINE_A)})
            # 判定の直接呼び出し: 0 件・2 件以上は不採用・1 件は採用（kintone に触れない）
            self.fake.raise_all = True
            self.assertEqual((await sync._decide_app28(app28(200, 1, line_user_id=LINE_B)))[:2],
                             (None, "line_id_not_unique"))
            d, reason, ok = await sync._decide_app28(app28(200, 1))
            self.assertEqual((d.case_id, reason, ok), (c1, "ref_confirmed", True))
            c9 = await ledger.ensure_case_for_record("kintone:testsub:40", "40", "9")
            await ledger.add_case_identity(c9, sync.line_namespace(), "line_user", LINE_A)
            self.assertEqual((await sync._decide_app28(app28(200, 1)))[:2], (None, "line_id_not_unique"))
            self.fake.raise_all = False
            # API の応答に LINE ID が出ない（RV-10）
            for payload in (await ledger.list_holds(), await ledger.list_pending_links(),
                            await ledger.list_case_events(c1), await ledger.sync_overview(),
                            await ledger.list_case_identities(c1)):
                self.assertNotIn(LINE_A, ledger.dumps(payload))
            self.assertEqual(brain_link.decide_app28_row({}, [c1]).case_id, c1)
            self.assertIsNone(brain_link.decide_app28_row({}, [c1, c9]))
        run(body())

    def test_event_idem_key_contains_case_id_and_relink_recomputes(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1, "2026-09-20T02:00:00Z", LINEユーザーID=LINE_B)]
        self.fake.data["28"] = [app28(100, 1)]

        async def keys():
            async with session_scope() as session:
                return sorted((r[0], int(r[1]), bool(r[2])) for r in (await session.execute(
                    sa.select(ledger.case_event.c.idem_key, ledger.case_event.c.case_id,
                              ledger.case_event.c.is_current).where(
                        ledger.case_event.c.source_app_id == "28"))).fetchall())

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP28)
            c1, c2 = await cid("40", "1"), await cid("40", "2")
            self.assertEqual(await keys(), [(f"c{c1}|28|100|-|chat_message|message/-/-/-/-", c1, True)])
            r = await ledger.relink_source(source_app_id="28", source_record_id="100", new_case=c2,
                                           reason="訂正", operation_id="op-1", actor="owner",
                                           seen_link_version=0, seen_source_revision=1)
            self.assertEqual((r["new_case_id"], r["prev"], r["prev_case"], r["reprojected_events"]),
                             (c2, c1, ("40", "1"), 1))
            self.assertEqual(await keys(), [(f"c{c1}|28|100|-|chat_message|message/-/-/-/-", c1, False),
                                            (f"c{c2}|28|100|-|chat_message|message/-/-/-/-", c2, True)])
            last = await ledger.latest_link("28", "100")
            self.assertEqual((last["prev_case_id"], last["new_case_id"], last["prev_case"], last["new_case"]),
                             (c1, c2, ("40", "1"), ("40", "2")))
            # 旧案件側の fact/event は prev_case_id で出所を残す（A1 の BA-16 のまま）
            ev = [e for e in await ledger.list_case_events(c2) if e["source_app_id"] == "28"]
            self.assertEqual([e["case_id"] for e in ev], [c2])
        run(body())

    def test_running_runs_and_stale_run_recovery(self):
        async def body():
            now = datetime.datetime(2026, 9, 27, 12, 0, tzinfo=_UTC)
            old = await ledger.start_run("app40", "2026-09-27T11:00:00Z", now=now - datetime.timedelta(minutes=20))
            fresh = await ledger.start_run("app30", "2026-09-27T11:59:00Z", now=now - datetime.timedelta(minutes=5))
            done = await ledger.start_run("app28", "2026-09-27T10:00:00Z", now=now - datetime.timedelta(hours=1))
            await ledger.finish_run(done, "ok", now=now)
            self.assertEqual(await ledger.count_running_runs(), 2)
            self.assertEqual(await ledger.stop_stale_runs(15, now=now), 1)     # 20 分前の run だけ
            self.assertEqual(await ledger.count_running_runs(), 1)
            runs = {r["run_id"]: r for r in (await ledger.sync_overview())["runs"]}
            self.assertEqual((runs[old]["status"], runs[old]["failure"]), ("stopped_stale", "stale_run"))
            self.assertEqual(runs[fresh]["status"], "running")
            self.assertEqual(await ledger.stop_stale_runs(15, now=now + datetime.timedelta(minutes=15)), 1)
            self.assertEqual(await ledger.count_running_runs(), 0)
            self.assertEqual(await ledger.stop_stale_runs(15, now=now + datetime.timedelta(hours=1)), 0)
            with self.assertRaises(ledger.LedgerError):
                await ledger.finish_run(old, "stale")
        run(body())

    def test_unregistered_case_freshness_and_relink_target(self):
        self.fake.data["40"] = [app40(1, 1)]
        self.fake.data["30"] = [app30(5, 1), app30(6, 1, 案件レコードID="999")]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP30)
            u = await _new_unregistered_case()
            info = await ledger.case_info(u)
            self.assertEqual((info["registration"], info["case_app_id"], info["case_record_id"]),
                             ("unregistered", None, None))
            self.assertTrue(await ledger.case_exists(u))
            self.assertFalse(await ledger.case_exists(u + 100))
            # 出典が無い未登録案件は unregistered（no_source ではない）
            self.assertEqual(await ledger.case_freshness_detail(u, "app40", sync.source_targets()),
                             {"state": "unregistered", "reasons": ["no_linked_source"]})
            # 紐付け待ちの App 30 No.6 を未登録案件へ訂正（R15 の範囲内・正本確認なしで可）
            r = await ledger.relink_source(source_app_id="30", source_record_id="6", new_case=u,
                                           reason="未登録案件へ", operation_id="op-u", actor="owner",
                                           seen_link_version=await ledger.link_version("30", "6"),
                                           seen_source_revision=1)
            self.assertEqual((r["new_case_id"], r["prev"], r["reprojected_facts"]), (u, None, 0))
            self.assertEqual(await ledger.current_case_of_source("30", "6"), u)
            self.assertEqual(await ledger.list_pending_links(), [])
            # 積み直しは次回同期（relink_pending → 同期後 synced）
            self.assertEqual(await ledger.case_freshness(u, "app40", sync.source_targets()), "partial")
            self.assertEqual((await sync.sync_target(sync.TARGET_APP30))["status"], "ok")
            facts = await ledger.list_case_facts(u)
            self.assertTrue(facts and all(f["case_id"] == u and f["case_app_id"] is None for f in facts))
            self.assertEqual(await ledger.case_freshness_detail(u, "app40", sync.source_targets()),
                             {"state": "synced", "reasons": []})
            self.assertEqual((await ledger.latest_link("30", "6"))["new_case"], None)   # 互換キー無し
            self.assertEqual((await ledger.latest_link("30", "6"))["new_case_id"], u)
            # 登録済み案件の互換キー: 自身の出典が無い（未同期）なら no_source（R31 の限定）
            reg = await ledger.ensure_case_for_record("kintone:testsub:40", "40", "7")
            self.assertEqual((await ledger.case_freshness_detail(reg, "app40"))["reasons"], ["no_source"])
            # 統合済み・不明な案件へは訂正できない
            async with session_scope() as session:
                await session.execute(sa.update(ledger.case).where(ledger.case.c.case_id == reg).values(
                    status="merged", merged_into_case_id=u))
            with self.assertRaises(ledger.LedgerError):
                await ledger.relink_source(source_app_id="30", source_record_id="6", new_case=reg,
                                           reason="", operation_id="op-m", actor="owner",
                                           seen_link_version=await ledger.link_version("30", "6"),
                                           seen_source_revision=1)
            with self.assertRaises(ledger.LedgerError):
                await ledger.relink_source(source_app_id="30", source_record_id="6", new_case=u + 100,
                                           reason="", operation_id="op-x", actor="owner",
                                           seen_link_version=await ledger.link_version("30", "6"),
                                           seen_source_revision=1)
            self.assertEqual(await ledger.active_cases_for_relation("line:x", "line_user", LINE_A), [])
        run(body())

    def test_legacy_case_key_inputs_resolve_through_identity(self):
        async def body():
            src = ledger.SourceRef("30", "1", 1, "app30_record", "1")
            f = [ledger.FactIn("shipping:1", "app30.件名", "text", "x", ledger.make_locator("件名"))]
            s = await ledger.ingest_source(src, ("40", "3"), f)       # 互換の案件キー → case を起こす
            self.assertEqual(s["state"], "ingested")
            c3 = await cid("40", "3")
            self.assertIsNotNone(c3)
            self.assertEqual(await ledger.list_case_facts(c3), await ledger.list_case_facts(("40", "3")))
            self.assertEqual(await ledger.list_case_facts(("26", "3")), [])
            self.assertEqual(await ledger.current_case_of_source("30", "1"), c3)
            self.assertEqual(await ledger.current_case_key_of_source("30", "1"), ("40", "3"))
            self.assertTrue(await ledger.case_exists(("40", "3")))
            self.assertFalse(await ledger.case_exists(("40", "4")))
            self.assertTrue(await ledger.case_exists(c3))
            self.assertEqual(await ledger.resolve_case(ledger.KintoneCaseKey("kintone:testsub:40", "40", "3")), c3)
            self.assertIsNone(await ledger.resolve_case(ledger.KintoneCaseKey("kintone:other:40", "40", "3")))
            with self.assertRaises(ledger.LedgerError):
                await ledger.list_case_facts(("40",))
            self.assertEqual(await ledger.list_case_facts(c3 + 100), [])     # 不明な案件の読取は空
            self.assertIsNone(await ledger.resolve_case(c3 + 100))
            with self.assertRaises(ledger.LedgerError):                         # 書込は拒否
                await ledger.ingest_source(ledger.SourceRef("30", "2", 1, "app30_record", "1"),
                                           c3 + 100, f)
            self.assertEqual(await ledger.list_case_facts(None), [])
        run(body())


class TestBrainViewCaseId(unittest.TestCase):
    """PWA の案件指定は ?case=（R31）。識別子検索 /app/api/brain/case・relink の case_id。"""

    def setUp(self):
        from test_brain_a1_authority_isolation import _ENV, _auth, _client
        self._env = _ENV
        self._auth = _auth
        self._client = _client
        self.t = BrainDbMixin("setUp")
        self.t.setUp()
        self.t.fake.data["40"] = [app40(1, 1), app40(2, 1, "2026-09-20T02:00:00Z", LINEユーザーID=LINE_B)]
        self.t.fake.data["30"] = [app30(5, 1), app30(6, 1, 案件レコードID="999")]
        run(sync.sync_target(sync.TARGET_APP40))
        run(sync.sync_target(sync.TARGET_APP30))

    def tearDown(self):
        self.t.tearDown()

    def get(self, path):
        return self._client.get(path, headers=self._auth(), follow_redirects=False)

    def test_case_lookup_and_facts_by_case_id(self):
        with patch.dict(os.environ, self._env):
            c1 = run(cid("40", "1"))
            r = self.get("/app/api/brain/case?app=40&record=1")
            self.assertEqual(r.status_code, 200)
            self.assertEqual(r.json(), {"case_id": c1, "registration": "registered", "status": "active",
                                        "case_app_id": "40", "case_record_id": "1"})
            self.assertEqual(self.get("/app/api/brain/case?app=40&record=999").status_code, 404)
            self.assertEqual(self.get("/app/api/brain/case?app=40").status_code, 400)
            by_id = self.get(f"/app/api/brain/facts?case={c1}").json()
            by_key = self.get("/app/api/brain/facts?app=40&record=1").json()
            self.assertEqual(by_id, by_key)
            self.assertEqual((by_id["case_id"], by_id["registration"], by_id["case_app_id"],
                              by_id["case_record_id"], by_id["freshness"]),
                             (c1, "registered", "40", "1", "synced"))
            self.assertTrue(all(f["case_id"] == c1 for f in by_id["facts"]))
            self.assertEqual(self.get("/app/api/brain/facts?case=999999").status_code, 404)
            self.assertEqual(self.get("/app/api/brain/facts?case=1&app=40&record=1").status_code, 400)
            self.assertEqual(self.get("/app/api/brain/facts?app=40&record=999").json()["facts"], [])
            u = run(_new_unregistered_case())
            d = self.get(f"/app/api/brain/facts?case={u}").json()
            self.assertEqual((d["case_id"], d["registration"], d["case_app_id"], d["freshness"]),
                             (u, "unregistered", None, "unregistered"))
            # 一覧系の応答に source_ingest.line_user_id を出さない（RV-10・App 40 の欄の fact は別）
            for path in ("/app/api/brain/holds", "/app/api/brain/pending", "/app/api/brain/overview"):
                self.assertNotIn(LINE_B, self.get(path).text)

    def test_relink_form_takes_case_id_and_allows_unregistered_target(self):
        with patch.dict(os.environ, self._env):
            u = run(_new_unregistered_case())
            pending = self.get("/app/api/brain/pending").json()["records"]
            self.assertEqual([p["source_record_id"] for p in pending], ["6"])
            body = {"operation_id": "11111111-2222-4333-8444-999999999999", "source_app_id": "30",
                    "source_record_id": "6", "case_id": str(u), "reason": "未登録案件へ",
                    "seen_link_version": str(pending[0]["link_version"]),
                    "seen_source_revision": str(pending[0]["source_revision"])}
            n = len(self.t.fake.calls)
            r = self._client.post("/app/brain/relink", data=body, headers=self._auth(), follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/brain?done=relink"))
            self.assertEqual(len(self.t.fake.calls), n)          # 未登録案件は正本確認をしない
            self.assertEqual(run(ledger.current_case_of_source("30", "6")), u)
            # 訂正先の欠落・不明・案件本体（App 40 の出典）は従来どおり
            bad = dict(body, operation_id="11111111-2222-4333-8444-999999999998")
            del bad["case_id"]
            self.assertEqual(self._client.post("/app/brain/relink", data=bad, headers=self._auth(),
                                               follow_redirects=False).status_code, 400)
            bad = dict(body, operation_id="11111111-2222-4333-8444-999999999997", case_id="999999")
            self.assertEqual(self._client.post("/app/brain/relink", data=bad, headers=self._auth(),
                                               follow_redirects=False).status_code, 400)
            bad = dict(body, operation_id="11111111-2222-4333-8444-999999999996", source_app_id="40",
                       source_record_id="1")
            self.assertEqual(self._client.post("/app/brain/relink", data=bad, headers=self._auth(),
                                               follow_redirects=False).status_code, 409)
        html = (sync.__file__.rsplit("hub", 1)[0] + "webapp/brain.html")
        src = open(html, encoding="utf-8").read()
        self.assertIn('id="case_id"', src)
        self.assertIn("/app/api/brain/facts?case=", src)
        self.assertIn('"/app/api/brain/case?app="', src)
        self.assertNotIn('name = "case_app_id"', src)
        self.assertTrue(re.search(r'a\.name = "case_id"', src))


if __name__ == "__main__":
    unittest.main()
