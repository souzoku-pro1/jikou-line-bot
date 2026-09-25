"""BRAIN-A1 T-B 冪等・版管理（v3 §10-2）＋ migration 往復・表と制約の pin

固定する仕様:
- locator は NOT NULL・不明部分は "-"（5 部固定）・空文字は DB CHECK で拒否
- 同一入力の再実行で fact が増えない（取込の冪等キー・revision 既知は再処理しない）
- 複数項目・サブテーブルの行入れ替えで subject（行 ID）が変わらず重複しない
- 旧 revision の遅着は現在値にならない（独立した観測として残る）
- 同一一意キーで値違い → MismatchError／source_ingest=mismatch_hold（黙って捨てない）
- supersedes: 自己参照は DB CHECK、分岐は UNIQUE、循環はアプリ検査で拒否
- 項目コードは閉集合（前提確認 §2 の表 + R2/R4）・確からしさは閉集合
- alembic b8c1d4e7f2a5（Revises a7d3f1c9e2b4）の接続と表・制約の pin（往復の実行は
  test_db_foundation.py::test_brain_ledger_migration_round_trip）
"""

import unittest
from pathlib import Path

import sqlalchemy as sa

from brain_test_support import BrainDbMixin, app30, app40, run, subrow
from hub import brain_ledger as ledger
from hub import brain_sync as sync
from hub.db import session_scope

REPO = Path(__file__).parent


def _src(rid="1", rev=1):
    return ledger.SourceRef("40", rid, rev, "app40_record", "1")


def _fact(code="app40.status", value="受任", subject="case", loc=None, conf="high"):
    return ledger.FactIn(subject, code, ledger.ITEM_CODES[code], value,
                         loc or ledger.make_locator(code.split(".", 1)[1]), conf)


class TestLocatorAndClosedSets(unittest.TestCase):
    def test_locator_normalization(self):
        self.assertEqual(ledger.make_locator(), "-/-/-/-/-")
        self.assertEqual(ledger.make_locator("status"), "status/-/-/-/-")
        self.assertEqual(ledger.make_locator("債権者一覧.債権者名", "11"),
                         "債権者一覧.債権者名/11/-/-/-")
        self.assertEqual(ledger.make_locator("a/b", "", None, "v2", " "),
                         "a_b/-/-/v2/-")
        self.assertEqual(ledger.LOCATOR_PARTS, 5)

    def test_fact_in_rejects_malformed_locator_and_unknown_code(self):
        with self.assertRaises(ledger.LedgerError):
            ledger.FactIn("case", "app40.status", "choice", "x", "")
        with self.assertRaises(ledger.LedgerError):
            ledger.FactIn("case", "app40.status", "choice", "x", "status")
        with self.assertRaises(ledger.LedgerError):
            ledger.FactIn("case", "app40.存在しない欄", "text", "x", ledger.make_locator("x"))
        with self.assertRaises(ledger.LedgerError):
            ledger.FactIn("case", "app40.status", "choice", "x",
                          ledger.make_locator("status"), confidence="certain")

    def test_item_codes_closed_set_matches_premise_table(self):
        codes = ledger.ITEM_CODE_SET
        # 前提確認 §2「載せる」+ R2（自由記述）+ R4（FILE）: 代表を pin
        for c in ("app40.status", "app40.顧客名", "app40.生年月日", "app40.他の相続人",
                  "app40.委任契約書", "app40.債権者一覧.債権者名", "app40.書類チェック.書類名",
                  "app30.発送ステータス", "app30.案件レコードID", "app28.message"):
            self.assertIn(c, codes)
        # 載せない（前提確認 §2）: システム欄・派生値・外部 ID・旧欄
        for c in ("app40.作成者", "app40.残日数", "app40.通知済み閾値", "app40.熟慮期間通知履歴",
                  "app40.cloudsign_document_id", "app40.Stripe決済ID", "app40.文字列__1行_",
                  "app40.宛名ラベル", "app40.職業", "app30.顧客名表示用", "app30.Drive_fileId",
                  "app30.エラー詳細", "app30.リトライ回数"):
            self.assertNotIn(c, codes)
        self.assertEqual(len(ledger.app40_fields()), 90)   # 105 − 載せない 12 − 職業(後回し) − SUBTABLE 2
        self.assertEqual(len(ledger.app30_fields()), 25)
        self.assertEqual(ledger.CONFIDENCE_VALUES, ("high", "medium", "low"))
        # R1: 申述人=案件キー由来の固定 ID・被相続人も固定 ID
        self.assertEqual(ledger.app40_field_subject("顧客名"), "applicant")
        self.assertEqual(ledger.app40_field_subject("被相続人氏名"), "decedent")
        self.assertEqual(ledger.app40_field_subject("他の相続人"), "case")   # R2
        self.assertEqual(ledger.ITEM_CODES["app40.生年月日"], "text")        # R3 原文
        self.assertEqual(ledger.ITEM_CODES["app40.委任契約書"], "file")      # R4

    def test_canonical_value_file_and_multi(self):
        t, j = ledger.canonical_value("file", [{"fileKey": "k2", "name": "n", "size": "1",
                                                "contentType": "x", "extra": "no"}])
        self.assertEqual(t, "k2")
        self.assertEqual(j, [{"fileKey": "k2", "name": "n", "size": "1", "contentType": "x"}])
        t, j = ledger.canonical_value("multi", ["b", "a"])
        self.assertEqual((t, j), ("a|b", ["a", "b"]))
        self.assertEqual(ledger.canonical_value("text", None), ("", None))


class TestIdempotency(BrainDbMixin):
    def test_rerun_same_input_adds_nothing(self):
        self.fake.data["40"] = [app40(1, 1), app40(2, 1)]
        self.fake.data["30"] = [app30(5, 1)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            await sync.sync_target(sync.TARGET_APP30)
            n1 = len(await ledger.list_case_facts("40", "1", current_only=False))
            for _ in range(3):
                r = await sync.sync_target(sync.TARGET_APP40)
                self.assertEqual(r["inserted"], 0)
                r = await sync.sync_target(sync.TARGET_APP30)
                self.assertEqual(r["inserted"], 0)
            self.assertEqual(len(await ledger.list_case_facts("40", "1", current_only=False)), n1)
            # 同じ SourceRef を直接再投入しても増えない
            src, key, facts, events = sync.convert_app40(app40(1, 1))
            s = await ledger.ingest_source(src, key, facts, events)
            self.assertEqual((s["inserted"], s["events"]), (0, 0))
        run(body())

    def test_multiple_items_and_subtable_reorder_keep_subjects(self):
        rows = [subrow(11, 債権者名="甲社", 通知要否="要"), subrow(12, 債権者名="乙社", 通知要否="不要")]
        self.fake.data["40"] = [app40(1, 1, 債権者一覧=rows)]

        async def body():
            await sync.sync_target(sync.TARGET_APP40)
            before = {(f["subject_id"], f["item_code"]): f["value_text"]
                      for f in await ledger.list_case_facts("40", "1")}
            self.assertEqual(before[("creditor:11", "app40.債権者一覧.債権者名")], "甲社")
            self.assertEqual(before[("creditor:12", "app40.債権者一覧.債権者名")], "乙社")
            self.assertEqual(before[("applicant", "app40.顧客名")], "テスト太郎")
            self.assertEqual(before[("decedent", "app40.被相続人氏名")], "テスト花子")
            # 行の順番だけ入れ替えた新 revision → subject 不変・fact 増えず
            self.fake.data["40"] = [app40(1, 2, "2026-09-20T02:00:00Z",
                                          債権者一覧=[rows[1], rows[0]])]
            r = await sync.sync_target(sync.TARGET_APP40)
            self.assertEqual(r["inserted"], 2)          # 更新日時・作成日時の 2 事実のみ
            after = {(f["subject_id"], f["item_code"]): f["value_text"]
                     for f in await ledger.list_case_facts("40", "1")}
            changed = {k for k in after if after[k] != before.get(k)}
            self.assertEqual(changed, {("case", "app40.更新日時"), ("case", "app40.作成日時")})
            self.assertEqual(set(before), set(after))   # subject×項目の集合は不変
            # 行 ID の無い行は subject を作らない（順番で識別しない）
            self.fake.data["40"] = [app40(1, 3, "2026-09-20T03:00:00Z",
                                          債権者一覧=[{"value": {"債権者名": {"value": "丙社"}}}])]
            await sync.sync_target(sync.TARGET_APP40)
            subs = {f["subject_id"] for f in await ledger.list_case_facts("40", "1")}
            self.assertFalse(any(s.startswith("creditor:-") or s == "creditor:" for s in subs))
        run(body())

    def test_late_old_revision_does_not_become_current(self):
        async def body():
            await ledger.ingest_source(_src(rev=5), ("40", "1"), [_fact(value="受理")])
            s = await ledger.ingest_source(_src(rev=4), ("40", "1"), [_fact(value="受任")])
            self.assertEqual(s["inserted"], 1)
            facts = await ledger.list_case_facts("40", "1", current_only=False)
            by_rev = {f["version"]: f for f in facts if f["item_code"] == "app40.status"}
            self.assertTrue(by_rev[5]["is_current"])
            self.assertFalse(by_rev[4]["is_current"])
            self.assertEqual([f["value_text"] for f in await ledger.list_case_facts("40", "1")],
                             ["受理"])
            # 同じ旧 revision を再投入しても増えない
            s = await ledger.ingest_source(_src(rev=4), ("40", "1"), [_fact(value="受任")])
            self.assertEqual(s["inserted"], 0)
        run(body())

    def test_same_key_different_value_is_mismatch_hold(self):
        async def body():
            await ledger.ingest_source(_src(rev=1), ("40", "1"), [_fact(value="受任")])
            s = await ledger.ingest_source(_src(rev=1), ("40", "1"), [_fact(value="受理")])
            self.assertEqual(s["state"], "mismatch_hold")
            self.assertEqual([f["value_text"] for f in await ledger.list_case_facts("40", "1")],
                             ["受任"])                       # 元の値は残り、上書きされない
            holds = await ledger.list_holds()
            self.assertEqual([(h["source_record_id"], h["state"]) for h in holds],
                             [("1", "mismatch_hold")])
            # ページ経路でも同じ（ページは巻き戻り・保留だけ記録・例外は上へ）
            with self.assertRaises(ledger.MismatchError):
                await ledger.advance_cursor_with_page(
                    "app40", await ledger.start_run("app40", "2026-09-22T00:00:00Z"),
                    ingests=[(_src(rev=1), ("40", "1"), [_fact(value="辞任")], [])],
                    cursor_updated_at="2026-09-22T00:00:00Z", cursor_record_id="1",
                    pages_done=1, records_seen=1)
            self.assertIsNone(await ledger.get_cursor("app40"))   # カーソルは進まない
        run(body())

    def test_supersedes_constraints(self):
        async def body():
            await ledger.ingest_source(_src(rev=1), ("40", "1"), [_fact(value="受任")])
            await ledger.ingest_source(_src(rev=2), ("40", "1"), [_fact(value="受理")])
            facts = {f["version"]: f for f in await ledger.list_case_facts("40", "1", False)}
            old, new = facts[1]["fact_id"], facts[2]["fact_id"]
            async with session_scope() as session:
                row = (await session.execute(sa.select(ledger.case_fact).where(
                    ledger.case_fact.c.fact_id == new))).first()
                self.assertEqual(row.supersedes_fact_id, old)
                # 自己参照は CHECK で拒否
                with self.assertRaises(Exception):
                    await session.execute(sa.update(ledger.case_fact).where(
                        ledger.case_fact.c.fact_id == old).values(supersedes_fact_id=old))
            # 分岐（同じ旧 fact を 2 行が supersede）は UNIQUE で拒否
            with self.assertRaises(Exception):
                async with session_scope() as session:
                    await session.execute(sa.insert(ledger.case_fact).values(
                        case_app_id="40", case_record_id="1", subject_id="case",
                        item_code="app40.status", value_type="choice", value_text="x",
                        source_kind="kintone", source_app_id="40", source_record_id="1",
                        source_revision=9, locator="status/-/-/-/-",
                        converter_name="app40_record", converter_version="1",
                        observation_id="o", observed_at=ledger._now(), confidence="high",
                        supersedes_fact_id=old))
            # 循環はアプリ検査で拒否（new→old が既にあるとき old→new を張る）
            async with session_scope() as session:
                with self.assertRaises(ledger.LedgerError):
                    await ledger._assert_no_cycle(session, old, new)
                with self.assertRaises(ledger.LedgerError):
                    await ledger._assert_no_cycle(session, new, new)
                await ledger._assert_no_cycle(session, None, new)   # 新規行→head は可
        run(body())

    def test_empty_value_without_history_is_not_stored_but_clearing_is(self):
        async def body():
            s = await ledger.ingest_source(_src(rev=1), ("40", "1"),
                                           [_fact("app40.事件番号", value="")])
            self.assertEqual(s["inserted"], 0)
            await ledger.ingest_source(_src(rev=2), ("40", "1"),
                                       [_fact("app40.事件番号", value="令和8年(家)第1号")])
            s = await ledger.ingest_source(_src(rev=3), ("40", "1"),
                                           [_fact("app40.事件番号", value="")])
            self.assertEqual(s["inserted"], 1)
            cur = [f for f in await ledger.list_case_facts("40", "1")
                   if f["item_code"] == "app40.事件番号"]
            self.assertEqual([(f["value_text"], f["version"]) for f in cur], [("", 3)])
        run(body())

    def test_event_idempotency_key(self):
        async def body():
            ev = [ledger.EventIn("case_status_observed", "status:受任",
                                 ledger.make_locator("status"))]
            s1 = await ledger.ingest_source(_src(rev=1), ("40", "1"), [_fact()], ev)
            s2 = await ledger.ingest_source(_src(rev=1), ("40", "1"), [_fact()], ev)
            self.assertEqual((s1["events"], s2["events"]), (1, 0))
            self.assertEqual(len(await ledger.list_case_events("40", "1")), 1)
        run(body())

    # ── fix1: BA-01 / R8（系列の latest_seen_revision） ──

    def test_latest_seen_revision_blocks_late_older_revision(self):
        # rev1=受任 → rev3=受任（同値: fact を増やさず latest_seen だけ進む）→ 遅着 rev2=受理
        async def body():
            await ledger.ingest_source(_src(rev=1), ("40", "1"), [_fact(value="受任")])
            s3 = await ledger.ingest_source(_src(rev=3), ("40", "1"), [_fact(value="受任")])
            self.assertEqual(s3["inserted"], 0)
            self.assertEqual(await ledger.latest_known_revision("40", "1"), 3)
            async with session_scope() as session:
                rows = (await session.execute(sa.select(
                    ledger.source_ingest.c.source_revision,
                    ledger.source_ingest.c.latest_seen_revision).where(
                    ledger.source_ingest.c.source_record_id == "1"))).fetchall()
            self.assertEqual(sorted((int(a), int(b)) for a, b in rows), [(1, 3), (3, 3)])
            s2 = await ledger.ingest_source(_src(rev=2), ("40", "1"), [_fact(value="受理")])
            self.assertEqual(s2["inserted"], 1)                          # 保存はする
            cur = [(f["value_text"], f["version"]) for f in await ledger.list_case_facts("40", "1")
                   if f["item_code"] == "app40.status"]
            self.assertEqual(cur, [("受任", 1)])                          # 現在値は受任のまま
            late = [f for f in await ledger.list_case_facts("40", "1", current_only=False)
                    if f["version"] == 2][0]
            self.assertEqual((late["is_current"], late["invalid_reason"]), (False, "stale_revision"))
            self.assertEqual(await ledger.latest_known_revision("40", "1"), 3)
            # 同期経路でも同じ（App 40 が rev1 → rev3 → rev2 の順に見えた）
            self.fake.data["40"] = [app40(7, 1)]
            await sync.sync_target(sync.TARGET_APP40)
            self.fake.data["40"] = [app40(7, 3, "2026-09-20T03:00:00Z")]
            await sync.sync_target(sync.TARGET_APP40)
            self.fake.data["40"] = [app40(7, 2, "2026-09-20T04:00:00Z", status="受理")]
            self.assertEqual((await sync.sync_target(sync.TARGET_APP40))["status"], "ok")
            st = [(f["value_text"], f["version"]) for f in await ledger.list_case_facts("40", "7")
                  if f["item_code"] == "app40.status"]
            self.assertEqual(st, [("受任", 1)])
            self.assertEqual(await ledger.latest_known_revision("40", "7"), 3)
        run(body())

    def test_fix1_columns_and_constraints_pinned(self):
        self.assertIn("latest_seen_revision", ledger.source_ingest.c)            # R8
        self.assertEqual(ledger.INGEST_STATES,
                         ("ingested", "mismatch_hold", "unavailable", "held", "detached"))
        for c in ("is_current", "invalid_reason", "invalidated_at"):               # R10
            self.assertIn(c, ledger.case_event.c)
        self.assertEqual([c.name for c in ledger.sync_cursor.primary_key.columns],
                         ["target_app", "kind"])                                  # BA-05
        for c in ("scan_upper_bound", "page_position",                            # BA-09
                  "recheck_started_at", "recheck_completed_at"):                  # BA-05
            self.assertIn(c, ledger.sync_cursor.c)
        self.assertEqual(ledger.CURSOR_KINDS, ("sync", "recheck"))
        self.assertEqual(ledger.RETIRED_REASONS, ("link_moved", "link_lost", "row_removed"))
        self.assertEqual(ledger.DETACH_REASONS, ("category_out_of_set", "line_id_not_unique"))
        self.assertEqual(ledger.SUBJECT_SHIPPING_PREFIX, "shipping:")               # R9
        src = (REPO / "alembic" / "versions" / "20260923_b8c1d4e7f2a5_brain_ledger.py"
               ).read_text(encoding="utf-8")
        for needle in ("latest_seen_revision", "'detached'", '"kind"', "ck_sync_cursor_kind",
                       "scan_upper_bound", "page_position", "recheck_completed_at",
                       "ix_case_event_source", "ix_source_ingest_case"):
            self.assertIn(needle, src)
        self.assertEqual(src.count("Revises: a7d3f1c9e2b4"), 1)                    # 単一線形チェーン


class TestMigrationRoundTrip(unittest.TestCase):
    """alembic b8c1d4e7f2a5 の revision 接続と表・制約の pin。up→down 往復の実行は
    alembic 起動が許可された test_db_foundation.py（test_brain_ledger_migration_round_trip）。"""

    REV = "b8c1d4e7f2a5"
    DOWN = "a7d3f1c9e2b4"

    def test_revision_file_chain(self):
        src = (REPO / "alembic" / "versions" / "20260923_b8c1d4e7f2a5_brain_ledger.py"
               ).read_text(encoding="utf-8")
        self.assertIn(f"revision: str = '{self.REV}'", src)
        self.assertIn(f"down_revision: Union[str, Sequence[str], None] = '{self.DOWN}'", src)
        env = (REPO / "alembic" / "env.py").read_text(encoding="utf-8")
        self.assertIn("brain_ledger_metadata", env)

    def test_tables_and_constraints_pinned(self):
        self.assertEqual(ledger.TABLE_NAMES, (
            "case_fact", "case_confirmation", "case_event", "case_derivation",
            "case_usage", "source_ingest", "link_history", "sync_run", "sync_cursor"))
        names = {c.name for c in ledger.case_fact.constraints if c.name}
        self.assertTrue({"uq_case_fact_key", "ck_case_fact_no_self_supersede",
                         "ck_case_fact_locator_nonempty", "ck_case_fact_confidence"} <= names)
        uq = [c for c in ledger.case_fact.constraints
              if isinstance(c, sa.UniqueConstraint) and c.name == "uq_case_fact_key"][0]
        self.assertEqual([c.name for c in uq.columns], [
            "case_app_id", "case_record_id", "subject_id", "item_code", "source_app_id",
            "source_record_id", "source_revision", "locator", "converter_name",
            "converter_version"])
        self.assertFalse(ledger.case_fact.c.locator.nullable)
        self.assertTrue(ledger.case_fact.c.supersedes_fact_id.unique)
        self.assertTrue(ledger.case_confirmation.c.operation_id.unique)
        self.assertTrue(ledger.case_event.c.idem_key.unique)
        uq2 = [c for c in ledger.source_ingest.constraints
               if isinstance(c, sa.UniqueConstraint) and c.name == "uq_source_ingest_key"][0]
        self.assertEqual([c.name for c in uq2.columns], [
            "source_app_id", "source_record_id", "source_revision", "locator",
            "converter_name", "converter_version"])


if __name__ == "__main__":
    unittest.main()
