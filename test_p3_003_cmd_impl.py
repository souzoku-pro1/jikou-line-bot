"""P3-003-CMD 実装票: 相続人導出コマンド経路のテスト（DRAFT_P3_003_CMD §7 の 20 系統）。

mock 境界は kintone（hub.kintone.search_records / create_record）と通知のみ。
導出エンジン・payload 変換・validate・run 保存（sqlite）・封筒結線は実物を通す。
§7-18（例外ラップ・stage 閉集合）は契約 pin の同時更新として
test_p3_003a_heir_envelope.TestFailureBehaviorContract 側に実装（裁定7）。
"""

import asyncio
import dataclasses
import os
import shutil
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
os.environ.setdefault("KINTONE_APP_ID", "21")
os.environ.setdefault("KINTONE_API_TOKEN", "dummy")

import hub.db as db  # noqa: E402
from dispatch_bot import confirm, heir_derive_task as ht, registry  # noqa: E402
from heir_derivation import (ENGINE_VERSION, FROZEN_CASE_VERSION,  # noqa: E402
                             Declarations, HeirPerson, LifeEvent)
from hub import heir_envelope as he  # noqa: E402
from hub import derivation_models as dm  # noqa: E402
from hub.derivation_models import (DerivationBase, DerivationRun,  # noqa: E402
                                   PayloadPolicyError, compute_input_hash)
from hub.kintone import KintoneError  # noqa: E402

_ENV = {
    "HEIR_DERIVATION_ENABLED": "1",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "APP_KOSEKI_PERSON": "34", "TOKEN_KOSEKI_PERSON": "t34",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
}

SENTINEL_NAME = "機密山田PII太郎SENTINEL"


def _run(coro):
    return asyncio.run(coro)


def _rec(rid, name, *, alive="生存", death="", decedent="no", rev="3",
         case="9", father="", mother="", events=None):
    r = {"$id": {"value": rid}, "$revision": {"value": rev},
         "案件アプリID": {"value": "26"}, "案件レコードID": {"value": case},
         "氏名": {"value": name}, "生死区分": {"value": alive},
         "死亡日": {"value": death}, "被相続人フラグ": {"value": decedent},
         "父人物ID": {"value": father}, "母人物ID": {"value": mother},
         "養父人物ID": {"value": ""}, "養母人物ID": {"value": ""},
         "身分事項": {"value": events or []}}
    return r


def _family(case="9", rev_child="3"):
    """被相続人（死亡・日付確定）＋子2名＝rank1 derived の標準ケース。"""
    return [
        _rec("10", SENTINEL_NAME, alive="死亡", death="2026-01-01",
             decedent="yes", case=case),
        _rec("11", "長男氏名X", father="10", case=case),
        _rec("12", "二男氏名Y", father="10", case=case, rev=rev_child),
    ]


def _pending(user="U1", case_record_id="9"):
    return SimpleNamespace(
        user_id=user,
        case=SimpleNamespace(record_id=case_record_id, customer_name="被",
                             status="進行中", unit="相続"),
        parsed={"task_type": "heir_derivation"},
        instruction_text="相続人を導出して")


class _CmdBase(unittest.TestCase):
    """sqlite 実 DB＋kintone/通知 mock の共通土台（mock 境界=kintone のみ）。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3cmd_")
        env = dict(_ENV)
        env["DATABASE_URL"] = f"sqlite+aiosqlite:///{self._dir}/a.db"
        self._env = patch.dict(os.environ, env)
        self._env.start()
        db.reset_for_tests()
        confirm.reset()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(DerivationBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

        self.app34_records = _family()
        self.envelope_hits = []          # find_existing が返す App30 レコード
        self.search_calls = []
        self.created_envelopes = []

        async def fake_search(app, query, fields=None):
            self.search_calls.append((app.label, query))
            if app.label == ht.APP_KOSEKI_PERSON.label:
                return list(self.app34_records)
            return list(self.envelope_hits)

        async def fake_create(app, fields):
            self.created_envelopes.append(fields)
            return str(700 + len(self.created_envelopes))

        self.search = AsyncMock(side_effect=fake_search)
        self.create = AsyncMock(side_effect=fake_create)
        self.alert = AsyncMock(return_value=True)
        patchers = [
            patch("hub.kintone.search_records", new=self.search),
            patch("hub.kintone.create_record", new=self.create),
            patch("hub.notify.notify_admin_line", new=self.alert),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _execute(self, pending=None):
        with self.assertLogs("dispatch_bot.heir_derive_task", level="INFO") as cap:
            msg, rid, url = _run(ht.execute(pending or _pending()))
        db.reset_for_tests()
        return msg, rid, url, "\n".join(cap.output)

    def _runs(self):
        async def _q():
            async with db.session_scope() as s:
                rows = (await s.execute(
                    sa.select(DerivationRun.__table__)
                    .order_by(DerivationRun.__table__.c.id))).all()
                return rows
        rows = _run(_q())
        db.reset_for_tests()
        return rows


# ── §7-1: registry / 語彙（flag 連動の catalog 掲載）────────────────────────
class TestRegistryAndVocabulary(unittest.TestCase):
    def test_registered_entry(self):
        spec = registry.get_task("heir_derivation")
        self.assertIsNotNone(spec)
        self.assertIs(spec.execute_fn, ht.execute)
        self.assertEqual(spec.required_fields, ["customer_name"])
        self.assertEqual(spec.search_apps, ["SOUZOKU_KINTONE_APP_ID"])
        # 正例の語彙条件: 「相続人」「導出」の両語を含む明示指示のみ（§2）
        self.assertIn("相続人", spec.hint_for_parser)
        self.assertIn("導出", spec.hint_for_parser)
        self.assertIn("明示指示のみ", spec.hint_for_parser)

    def test_catalog_hidden_while_flag_off(self):
        # §2: OFF の間は語彙一覧に載せない（公開は flag 点火と同時・実機確認⑤）
        env = {k: v for k, v in os.environ.items()
               if k != "HEIR_DERIVATION_ENABLED"}
        with patch.dict(os.environ, env, clear=True):
            catalog = registry.catalog_for_prompt()
        self.assertNotIn("heir_derivation", catalog)
        # 既存経路の完全無変更（要件5）: 既存6タスクは OFF でも掲載されたまま
        for existing in ("soufu_annai", "shokumu_seikyu", "sortation_assign",
                         "person_merge", "person_confirm", "review_resolve"):
            self.assertIn(existing, catalog)
        with patch.dict(os.environ, {"HEIR_DERIVATION_ENABLED": "1"}):
            self.assertIn("heir_derivation", registry.catalog_for_prompt())

    def test_existing_specs_unconditionally_visible(self):
        for t, spec in registry.TASK_REGISTRY.items():
            if t not in ("heir_derivation", "shokumu_plan", "heir_cancel"):
                # flag 連動タスク（heir/SHOKUMU-PLAN・時点ピンの追随）以外は
                # 無条件掲載＝既存タスク無変更の pin
                self.assertIsNone(spec.visible_fn, t)


# ── §7-2/§7-15/§7-16: flag ゲート境界 ────────────────────────────────────────
class TestFlagGate(_CmdBase):
    def test_off_is_inert_even_on_direct_call(self):
        env = {k: v for k, v in os.environ.items()
               if k != "HEIR_DERIVATION_ENABLED"}
        confirm.create("U1", {"task_type": "heir_derivation"},
                       _pending().case, "相続人を導出して")
        with patch.dict(os.environ, env, clear=True):
            msg, rid, url = _run(ht.execute(_pending()))
        db.reset_for_tests()
        self.assertEqual(msg, ht.MSG_DISABLED)
        self.search.assert_not_awaited()     # I/O ゼロ（kintone 読取なし）
        self.create.assert_not_awaited()
        self.assertEqual(self._runs(), [])   # DB write ゼロ
        self.assertFalse(confirm.has_active("U1"))   # 終端＝pending 無効化

    def test_mid_run_off_saves_run_and_reports_disabled(self):
        # §2/§7-16: 実行途中 OFF（run 保存後）→ envelope=disabled 応答・再指示で回収
        with patch.object(he, "heir_derivation_enabled", return_value=False):
            msg, rid, url, log = self._execute()
        self.assertIn("封筒は未起票です（機能停止中）", msg)
        self.assertEqual(len(self._runs()), 1)          # run は保存済み
        self.assertIn("run=created envelope=disabled", log)
        # 再開後の再指示で回収（同一入力 → no_change・封筒 filed）
        msg2, _rid2, _u, log2 = self._execute()
        self.assertIn("run=no_change envelope=filed", log2)
        self.assertEqual(len(self._runs()), 1)          # run 追加ゼロ


# ── §7-3/§7-14: 正常系 E2E と §4A 写像 ───────────────────────────────────────
class TestHappyPathE2E(_CmdBase):
    def test_derive_save_file_reply(self):
        msg, rid, url, log = self._execute()
        rows = self._runs()
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(rid, str(row.id))
        # §4A 写像の充足
        self.assertEqual(row.case_app_id, "26")
        self.assertEqual(row.case_record_id, "9")
        self.assertEqual(row.decedent_person_id, "10")
        self.assertEqual(row.at_date, "2026-01-01")
        self.assertEqual(row.frozen_case_version, FROZEN_CASE_VERSION)
        self.assertEqual(row.engine_version, ENGINE_VERSION)
        self.assertEqual(row.status, "derived")
        self.assertEqual(row.rank, 1)
        self.assertTrue(row.provisional)                 # 裁定1: OR True 強制
        self.assertEqual(row.input_person_ids, ["10", "11", "12"])  # int 昇順
        self.assertEqual(row.input_person_revisions,
                         {"10": "3", "11": "3", "12": "3"})
        self.assertEqual(row.result_hash,
                         dm.compute_result_hash(row.result_payload))
        # zokugara_code 入り payload（P3-001 改定を使用・要件1）
        self.assertTrue(dm.payload_has_zokugara_codes(row.result_payload))
        # 封筒 filed（実 file_heir_envelope・mock は kintone のみ）
        self.assertEqual(len(self.created_envelopes), 1)
        self.assertIn(f"run #{row.id}", self.created_envelopes[0]["件名"])
        # 応答: run id・件数・封筒 No・provisional 固定表示（PII なし）
        self.assertIn(f"run #{row.id}", msg)
        self.assertIn("候補 2 名", msg)
        self.assertIn("要確認封筒 No.701", msg)
        self.assertIn(ht.MSG_PROVISIONAL, msg)
        self.assertIn("run=created envelope=filed", log)

    def test_rank3_downgraded_to_held_and_provisional_forced(self):
        # 裁定2: kosekis=None の間 rank=3 は常に held（要件3）／裁定1: provisional
        self.app34_records = [
            _rec("10", "被相続人A", alive="死亡", death="2026-01-01",
                 decedent="yes"),
            _rec("13", "弟B", father="20", mother="21"),
            _rec("20", "父", alive="死亡", death="2000-01-01"),
            _rec("21", "母", alive="死亡", death="2001-01-01"),
        ]
        # 被相続人にも同じ親エッジ（兄弟姉妹＝親エッジ共有）
        self.app34_records[0]["父人物ID"]["value"] = "20"
        self.app34_records[0]["母人物ID"]["value"] = "21"
        msg, rid, url, log = self._execute()
        rows = self._runs()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].rank, 3)
        self.assertEqual(rows[0].status, "held")         # 格下げ保存
        self.assertTrue(rows[0].provisional)
        self.assertIn("保留として保存しました", msg)


# ── §7-4/§7-5: 冪等・再導出 ──────────────────────────────────────────────────
class TestIdempotencyAndRederivation(_CmdBase):
    def test_same_input_creates_no_second_run(self):
        msg1, rid1, _u, log1 = self._execute()
        self.assertIn("run=created envelope=filed", log1)
        # 2回目: 封筒検索が既存封筒を返す（1回目で filed 済み）
        self.envelope_hits = [
            {"$id": {"value": "701"},
             "チャネル固有データ":
                 {"value": self.created_envelopes[0]["チャネル固有データ"]}}]
        msg2, rid2, _u, log2 = self._execute()
        self.assertEqual(rid1, rid2)
        self.assertEqual(len(self._runs()), 1)           # run 追加ゼロ（裁定5）
        self.assertIn("入力に変化はありません", msg2)
        self.assertIn("run=no_change envelope=already_filed", log2)
        self.assertEqual(len(self.created_envelopes), 1)  # 二重起票なし

    def test_changed_input_supersedes_head(self):
        _m, rid1, _u, _l = self._execute()
        self.app34_records = _family(rev_child="4")      # revision 変化＝別 hash
        msg2, rid2, _u, log2 = self._execute()
        rows = self._runs()
        self.assertEqual(len(rows), 2)
        self.assertEqual(str(rows[1].id), rid2)
        self.assertEqual(rows[1].supersedes_run_id, int(rid1))  # head を supersede
        self.assertIn("run=created", log2)
        self.assertEqual(len(self.created_envelopes), 2)  # 新封筒


# ── §7-6/§7-11/§7-12: 失敗経路・App34 異常系・例外分類 ───────────────────────
class TestFailurePaths(_CmdBase):
    def test_kintone_read_failure(self):
        self.search.side_effect = KintoneError(503, "y", "down " + SENTINEL_NAME)
        msg, rid, _u, log = self._execute()
        self.assertIn("読取に失敗しました（KintoneError）", msg)
        self.assertIn("run=failed:kintone_read envelope=skipped", log)
        self.assertEqual(self._runs(), [])
        self.assertNotIn(SENTINEL_NAME, msg)
        self.assertNotIn(SENTINEL_NAME, log)

    def test_derive_error_is_not_saved(self):
        # 被相続人 0名（対象 0 件を含む枠・裁定6改定: run 非保存・DB 行ゼロ）
        self.app34_records = [_rec("11", "子だけ")]
        msg, rid, _u, log = self._execute()
        self.assertIn("導出エラー: 保留理由 1 件（保存はしていません）", msg)
        self.assertIn("run=not_saved_error envelope=skipped", log)
        self.assertEqual(self._runs(), [])
        self.create.assert_not_awaited()

    def test_zero_records_and_multiple_decedents_not_saved(self):
        for records in ([],                                  # 対象 0 件
                        [_rec("10", "被A", alive="死亡", death="2026-01-01",
                              decedent="yes"),
                         _rec("11", "被B", alive="死亡", death="2026-01-02",
                              decedent="yes")]):             # 被相続人 複数名
            with self.subTest(n=len(records)):
                self.app34_records = records
                msg, rid, _u, log = self._execute()
                self.assertIn("導出エラー", msg)
                self.assertIn("run=not_saved_error", log)
                self.assertEqual(self._runs(), [])

    def test_missing_revision_aborts_canonicalization(self):
        # §7-11: $revision 欠落 → canonical 化中止（payload_policy 枠・警報）
        broken = _family()
        broken[1]["$revision"]["value"] = ""
        self.app34_records = broken
        msg, rid, _u, log = self._execute()
        self.assertIn("保存規格に不適合のため中止しました", msg)
        self.assertIn("run=failed:payload_policy envelope=skipped", log)
        self.assertEqual(self._runs(), [])
        self.alert.assert_awaited()                       # 業務チャネル警報

    def test_case_mixup_detected_and_aborted(self):
        # fix2 M03: 別案件人物の混入＝中止（write 0）
        mixed = _family()
        mixed[2]["案件レコードID"]["value"] = "8"
        self.app34_records = mixed
        msg, rid, _u, log = self._execute()
        self.assertIn("別案件の人物が混入", msg)
        self.assertIn("run=failed:kintone_read envelope=skipped", log)
        self.assertEqual(self._runs(), [])
        self.assertNotIn(SENTINEL_NAME, msg)

    def test_validate_failure_alerts_and_saves_nothing(self):
        bad = {"heirs": [{"person_id": "11", "山田": "x"}], "facts": []}
        with patch.object(ht, "build_run_payload", return_value=(bad, None)):
            msg, rid, _u, log = self._execute()
        self.assertIn("保存規格に不適合のため中止しました", msg)
        self.assertEqual(self._runs(), [])
        self.alert.assert_awaited()

    def test_immutable_error_classified_with_alert(self):
        with patch.object(ht, "create_derivation_run",
                          side_effect=dm.ImmutableRecordError("x")):
            msg, rid, _u, log = self._execute()
        self.assertIn("内部整合性エラー（ImmutableRecordError）", msg)
        self.assertIn("run=failed:immutable envelope=skipped", log)
        self.alert.assert_awaited()

    def test_envelope_search_failure_keeps_run(self):
        async def fake_search(app, query, fields=None):
            if app.label == ht.APP_KOSEKI_PERSON.label:
                return list(self.app34_records)
            raise KintoneError(503, "z", "search down")
        self.search.side_effect = fake_search
        msg, rid, _u, log = self._execute()
        self.assertEqual(len(self._runs()), 1)            # run 残存
        self.assertIn(f"run #{rid} は保存済み・封筒起票のみ失敗", msg)
        self.assertIn("run=created envelope=failed:search", log)
        self.create.assert_not_awaited()                  # 封筒 write 0

    def test_envelope_ack_unknown_then_reconciled(self):
        self.create.side_effect = KintoneError(0, "", "timeout")
        msg, rid, _u, log = self._execute()
        self.assertEqual(len(self._runs()), 1)
        self.assertIn("封筒は結果不明です", msg)
        self.assertIn("run=created envelope=ack_unknown", log)
        # 再指示: 実は作成済みだった封筒を完全一致検索で回収（§3B・二重起票なし）
        key = he.idempotency_key("9", self._runs()[0].input_hash)
        self.envelope_hits = [
            {"$id": {"value": "88"},
             "チャネル固有データ":
                 {"value": '{"heir_derivation": {"冪等キー": "%s"}}' % key}}]
        self.create.side_effect = None
        self.create.reset_mock()
        msg2, _r, _u, log2 = self._execute()
        self.assertIn("run=no_change envelope=already_filed", log2)
        self.create.assert_not_awaited()

    def test_envelope_policy_failure_classified(self):
        # ユニット解決不能（SOUZOKU_KINTONE_APP_ID を封筒側写像から外す）
        # → run 保存済み・failed:policy
        async def broken_unit(case_app_id):
            raise he.EnvelopePolicyError("ユニット解決不能")
        with patch.object(he, "_unit_for_case",
                          side_effect=he.EnvelopePolicyError("x")):
            msg, rid, _u, log = self._execute()
        self.assertEqual(len(self._runs()), 1)
        self.assertIn("封筒の前提検証で中止", msg)
        self.assertIn("run=created envelope=failed:policy", log)


# ── §7-7: 並行3種 ────────────────────────────────────────────────────────────
class TestConcurrency(_CmdBase):
    def test_parallel_first_run_conflict(self):
        # 並行初回: 双方が head なしを観測→片方が先に root 保存→他方は DB の
        # single-root 部分ユニークで IntegrityError（DB 遮断自体は
        # test_p3_001 TestSingleRootDbLevel が pin 済み）。ここでは task が
        # IntegrityError を run_conflict へ分類し write を増やさないことを検査
        from sqlalchemy.exc import IntegrityError
        with patch.object(ht, "create_derivation_run",
                          side_effect=IntegrityError("x", None, Exception("dup"))):
            msg, rid, _u, log = self._execute()
        self.assertIn("並行実行と競合しました（run_conflict）", msg)
        self.assertIn("run=run_conflict envelope=skipped", log)
        self.assertEqual(self._runs(), [])                # 他方のみ成立（本側 0）
        self.create.assert_not_awaited()                  # 封筒段に未到達

    def test_supersede_conflict_chain_integrity(self):
        _m, rid1, _u, _l = self._execute()
        self.app34_records = _family(rev_child="6")
        _m2, rid2, _u, _l2 = self._execute()              # head は rid2 へ前進
        stale = SimpleNamespace(id=int(rid1), input_hash="0" * 64)
        self.app34_records = _family(rev_child="7")
        with patch.object(ht, "get_current_head",
                          new=AsyncMock(return_value=stale)):  # 古い head を観測
            msg, rid, _u, log = self._execute()
        self.assertIn("保存の前提が変化しました（ChainIntegrityError）", msg)
        self.assertIn("run=failed:chain_integrity envelope=skipped", log)
        self.assertEqual(len(self._runs()), 2)

    def test_envelope_toctou_duplicate_tolerated(self):
        # 封筒側は検索型 best-effort 冪等（§3・稀な二重は許容＝pin）
        _m, _r, _u, _l = self._execute()
        # 再指示だが検索が空を返し続ける（TOCTOU 窓）→ 2 通目の封筒を許容
        self.envelope_hits = []
        msg2, _r2, _u2, log2 = self._execute()
        self.assertIn("run=no_change envelope=filed", log2)
        self.assertEqual(len(self.created_envelopes), 2)


# ── §7-8/§7-19: PII 非漏れ・canonical blob 非残存 ────────────────────────────
class TestNonExposure(_CmdBase):
    def test_name_sentinel_never_reflected(self):
        with self.assertLogs("hub.heir_envelope", level="INFO") as env_log:
            msg, rid, _u, log = self._execute()
        for surface in (msg, log, "\n".join(env_log.output)):
            self.assertNotIn(SENTINEL_NAME, surface)
        for call in self.alert.await_args_list:
            self.assertNotIn(SENTINEL_NAME, str(call))
        # 保存された run（payload・全列）にも氏名は残らない
        for row in self._runs():
            self.assertNotIn(SENTINEL_NAME, str(row))

    def test_hold_reason_names_not_in_reply(self):
        # 保留理由の原文（氏名入り）は応答へ出さない（件数のみ）
        self.app34_records = [
            _rec("10", SENTINEL_NAME, alive="死亡", death="2026-01-01",
                 decedent="yes"),
            _rec("11", SENTINEL_NAME + "子", father="10", alive="不明"),
        ]
        msg, rid, _u, log = self._execute()
        self.assertIn("保留として保存しました", msg)
        self.assertIn("保留 1 件", msg)
        self.assertNotIn(SENTINEL_NAME, msg)
        self.assertNotIn(SENTINEL_NAME, log)

    def test_canonical_blob_not_retained(self):
        # §7-19: 直列化 blob を module 変数・関数属性・キャッシュに保持しない
        persons = [HeirPerson(record_id="10", name=SENTINEL_NAME, alive="死亡",
                              death_date="2026-01-01", is_decedent=True)]
        h = compute_input_hash(
            case_app_id="26", case_record_id="9", at_date="2026-01-01",
            persons=persons, person_revisions={"10": "3"},
            declarations=Declarations(), kosekis=None,
            engine_version="e", frozen_case_version="f")
        self.assertRegex(h, r"^[0-9a-f]{64}$")            # 戻り値は hash 値のみ
        self.assertNotIn(SENTINEL_NAME, h)
        self.assertFalse(vars(compute_input_hash))        # 関数属性キャッシュなし
        for name, value in vars(dm).items():              # module 変数に非残存
            self.assertNotIn(SENTINEL_NAME, repr(value), name)


# ── §7-9/§7-15: canonical hash の性質・入力型 ────────────────────────────────
class TestCanonicalHash(unittest.TestCase):
    BASE = dict(case_app_id="26", case_record_id="9", at_date="2026-01-01",
                person_revisions={"10": "3", "11": "4"},
                declarations=Declarations(), kosekis=None,
                engine_version="e1", frozen_case_version="f1")

    def _persons(self, name11="花子"):
        return [
            HeirPerson(record_id="10", name="被", alive="死亡",
                       death_date="2026-01-01", is_decedent=True,
                       events=[LifeEvent(kind="死亡", date="令和8年1月1日")]),
            HeirPerson(record_id="11", name=name11, father_id="10"),
        ]

    def _hash(self, persons=None, **over):
        args = {**self.BASE, "persons": persons or self._persons(), **over}
        return compute_input_hash(**args)

    def test_determinism_and_order_independence(self):
        self.assertEqual(self._hash(), self._hash())      # 決定性
        shuffled = list(reversed(self._persons()))
        self.assertEqual(self._hash(), self._hash(persons=shuffled))  # 整列固定

    def test_semantic_change_changes_hash(self):
        base = self._hash()
        self.assertNotEqual(base, self._hash(at_date="2026-01-02"))
        self.assertNotEqual(base, self._hash(engine_version="e2"))   # 裁定5
        self.assertNotEqual(base, self._hash(frozen_case_version="f2"))
        self.assertNotEqual(base, self._hash(
            declarations=Declarations(renounced={"11"})))

    def test_person_content_change_without_revision_changes_hash(self):
        # fix2 M01: revision 不変でも persons 内容 field の変更＝別 hash（併存材料）
        self.assertNotEqual(self._hash(), self._hash(self._persons(name11="花O")))

    def test_type_and_character_grammar(self):
        with self.assertRaises(PayloadPolicyError):       # int revision 拒否
            self._hash(person_revisions={"10": 3, "11": "4"})
        with self.assertRaises(PayloadPolicyError):       # C0 制御文字 拒否
            self._hash(self._persons(name11="花\x00子"))
        with self.assertRaises(PayloadPolicyError):       # kosekis は None 固定
            self._hash(kosekis=[])
        with self.assertRaises(PayloadPolicyError):       # 型不正（None 名）
            self._hash([HeirPerson(record_id="10", name=None, alive="死亡",
                                   death_date="2026-01-01", is_decedent=True),
                        self._persons()[1]])
        with self.assertRaises(PayloadPolicyError):       # revision 欠落（集合不一致）
            self._hash(person_revisions={"10": "3"})

    def test_nfc_equivalence_and_empty_vs_value(self):
        composed = self._persons(name11="ガ")             # U+30AC
        decomposed = self._persons(name11="ガ")     # カ＋濁点
        self.assertEqual(self._hash(composed), self._hash(decomposed))
        # 空文字は "" のまま保持（値ありと区別される）
        with_empty = self._persons()
        with_empty[1] = HeirPerson(record_id="11", name="花子", father_id="10",
                                   death_wareki="")
        with_value = self._persons()
        with_value[1] = HeirPerson(record_id="11", name="花子", father_id="10",
                                   death_wareki="令和")
        self.assertNotEqual(self._hash(with_empty), self._hash(with_value))


# ── §7-10/§7-13: confirm フロー境界・pending 3系統 ───────────────────────────
class TestConfirmAndPending(_CmdBase):
    def test_confirmation_message_and_cancel(self):
        from dispatch_bot.case_search import CaseHit
        spec = registry.get_task("heir_derivation")
        p = _pending()
        hit = CaseHit(record_id="9", customer_name="被",
                      status="進行中", unit="相続")
        text = confirm.confirmation_message(spec, hit, p.parsed)
        self.assertIn("相続人の導出", text)               # 復唱内容
        self.assertIn("OK / キャンセル", text)
        confirm.create("U1", p.parsed, p.case, "相続人を導出して")
        self.assertTrue(confirm.has_active("U1"))
        confirm.invalidate("U1")                          # 「いいえ」中止
        self.assertFalse(confirm.has_active("U1"))
        self.search.assert_not_awaited()                  # write/read 0

    def test_expired_and_interrupt_pending(self):
        p = _pending()
        pend = confirm.create("U1", p.parsed, p.case, "相続人を導出して")
        pend.created_at -= confirm.PENDING_TTL_SEC + 1    # 期限切れ
        self.assertEqual(confirm.peek("U1")[0], "expired")
        pend2 = confirm.create("U1", p.parsed, p.case, "again")   # 割込み=上書き
        state, got = confirm.peek("U1")
        self.assertEqual((state, got.command_id), ("active", pend2.command_id))

    def test_pending_invalidated_on_all_three_terminals(self):
        # §7-13: 成功／分類済み失敗／想定外例外——すべて finally で invalidate
        p = _pending()
        confirm.create("U1", p.parsed, p.case, "x")
        self._execute(p)                                  # 成功
        self.assertFalse(confirm.has_active("U1"))

        confirm.create("U1", p.parsed, p.case, "x")
        self.search.side_effect = KintoneError(500, "a", "b")
        self._execute(p)                                  # 分類済み失敗
        self.assertFalse(confirm.has_active("U1"))

        confirm.create("U1", p.parsed, p.case, "x")

        async def boom(app, query, fields=None):
            raise RuntimeError("unexpected")
        self.search.side_effect = boom
        with self.assertRaises(RuntimeError), \
             self.assertLogs("dispatch_bot.heir_derive_task", level="INFO"):
            _run(ht.execute(p))                           # 想定外＝伝播
        db.reset_for_tests()
        self.assertFalse(confirm.has_active("U1"))
        # 二重 OK 相当: pending が消えている＝再実行は「確認待ちなし」側に落ちる


# ── fix1 H01: ログ処理が死んでも pending invalidate へ必ず到達（二重 finally）──
class TestLogFailureStillInvalidates(_CmdBase):
    """R-P3-003-CMD-IMPL-1 H01: finally 内のログ処理（logger.info／
    build_heir_cmd_log／emit）がいかなる例外を送出しても、内側 finally の
    confirm.invalidate に必ず到達することを pending 消滅の実測で pin。"""

    def _arm_pending(self, user="U1"):
        p = _pending(user)
        confirm.create(user, p.parsed, p.case, "相続人を導出して")
        self.assertTrue(confirm.has_active(user))
        return p

    def test_logger_info_runtime_error_still_invalidates(self):
        p = self._arm_pending()
        with patch.object(ht.logger, "info",
                          side_effect=RuntimeError("logging backend down")):
            msg, rid, _u = _run(ht.execute(p))
        db.reset_for_tests()
        self.assertIn("相続人導出を保存しました", msg)   # 本処理は完了している
        self.assertFalse(confirm.has_active("U1"))       # invalidate 到達

    def test_build_log_valueerror_still_invalidates(self):
        # 表外組合せ相当（build_heir_cmd_log が ValueError）でも invalidate
        p = self._arm_pending()
        with patch.object(ht, "build_heir_cmd_log",
                          side_effect=ValueError("illegal combination")), \
             self.assertLogs("dispatch_bot.heir_derive_task",
                             level="ERROR") as cap:
            msg, rid, _u = _run(ht.execute(p))
        db.reset_for_tests()
        self.assertFalse(confirm.has_active("U1"))
        # 失敗時の logger.error は固定文言のみ（例外本文・値は非露出）
        out = "\n".join(cap.output)
        self.assertIn("log emission failed (fixed classification only)", out)
        self.assertNotIn("illegal combination", out)

    def test_emit_unexpected_error_still_invalidates(self):
        p = self._arm_pending()
        with patch.object(ht, "emit",
                          side_effect=RuntimeError("emit exploded")), \
             self.assertLogs("dispatch_bot.heir_derive_task",
                             level="ERROR") as cap:
            msg, rid, _u = _run(ht.execute(p))
        db.reset_for_tests()
        self.assertFalse(confirm.has_active("U1"))
        out = "\n".join(cap.output)
        self.assertIn("log emission failed (fixed classification only)", out)
        self.assertNotIn("emit exploded", out)


# ── fix1 L01: §7-1 誤爆 negative の分類契約 pin ──────────────────────────────
class TestVocabularyNegativeContract(unittest.TestCase):
    """§7-1 誤爆 negative。parser の分類は LLM（parse_instruction・実 API）で、
    既存 suite に分類実測の先例は無い（全 dispatch テストが parse_instruction を
    mock）。実装可能な最も近い形として、分類を規定する**契約面**を機械 pin する:
    (a) catalog の heir_derivation 行が「両語を含む明示指示のみ」を宣言
    (b) parser system prompt が「該当なしは task_type=null」を指示
    (c) 片語フレーズが正しく他タスク/null に分類された場合、handler が
        heir_derivation へ横流ししない（keyword 迂回経路が存在しない）"""

    NEGATIVE_PHRASES = ("相続人を確認して", "導出資料を表示して")

    def test_catalog_declares_both_word_requirement(self):
        with patch.dict(os.environ, {"HEIR_DERIVATION_ENABLED": "1"}):
            catalog = registry.catalog_for_prompt()
        line = next(ln for ln in catalog.splitlines()
                    if "heir_derivation" in ln)
        self.assertIn("「相続人」「導出」の両語を含む明示指示のみ", line)

    def test_prompt_requires_null_when_no_match(self):
        from dispatch_bot import parser
        with patch.dict(os.environ, {"HEIR_DERIVATION_ENABLED": "1"}):
            prompt = parser.build_system_prompt()
        self.assertIn("該当するタスク種別がなければ task_type=null", prompt)

    def test_handler_does_not_reroute_single_word_phrases(self):
        # 片語フレーズが person_confirm / null に分類されたとき、heir execute が
        # 呼ばれないこと（handler・registry に keyword 迂回が無いことの回帰 pin）
        from dispatch_bot import handler, parser

        async def _case(phrase, task_type):
            handler.reset_sessions()
            confirm.reset()
            parse = {"intent": "task", "task_type": task_type,
                     "customer_name": None, "task_params": {},
                     "confidence": "low", "missing_fields": [],
                     "clarification": "どの案件ですか"}
            heir_exec = AsyncMock()
            with patch.object(parser, "parse_instruction",
                              new=AsyncMock(return_value=parse)), \
                 patch.object(ht, "execute", new=heir_exec), \
                 patch.dict(os.environ, {"HEIR_DERIVATION_ENABLED": "1"}):
                await handler.handle_message("U9", phrase)
            heir_exec.assert_not_awaited()

        for phrase in self.NEGATIVE_PHRASES:
            for task_type in (None, "person_confirm"):
                with self.subTest(phrase=phrase, task_type=task_type):
                    asyncio.run(_case(phrase, task_type))


# ── §5A/§7-12: 想定外例外の段階分離（fix4 H01）───────────────────────────────
class TestUnexpectedStageSeparation(_CmdBase):
    def test_before_save_unexpected(self):
        async def boom(app, query, fields=None):
            raise RuntimeError("boom-before")
        self.search.side_effect = boom
        with self.assertRaises(RuntimeError), \
             self.assertLogs("dispatch_bot.heir_derive_task", level="INFO") as cap:
            _run(ht.execute(_pending()))
        db.reset_for_tests()
        self.assertIn("run=failed:unexpected envelope=skipped",
                      "\n".join(cap.output))

    def test_after_save_unexpected(self):
        with patch.object(ht, "file_heir_envelope",
                          new=AsyncMock(side_effect=RuntimeError("boom-after"))):
            with self.assertRaises(RuntimeError), \
                 self.assertLogs("dispatch_bot.heir_derive_task",
                                 level="INFO") as cap:
                _run(ht.execute(_pending()))
        db.reset_for_tests()
        self.assertIn("run=created envelope=failed:unexpected",
                      "\n".join(cap.output))
        self.assertEqual(len(self._runs()), 1)            # run は残存＝再指示で回収


# ── §7-17: 2軸 enum の table test（合法組合せ表との全対一致）─────────────────
class TestTwoAxisLogTable(unittest.TestCase):
    # §6 の合法組合せ表（テスト側凍結コピー・実装定数との全対一致を検査）
    TABLE = frozenset(
        {(r, e) for r in ("created", "no_change")
         for e in ("filed", "already_filed", "disabled", "failed:policy",
                   "failed:search", "failed:unexpected", "ack_unknown")}
        | {(r, "skipped") for r in ("not_saved_error", "run_conflict",
                                    "failed:chain_integrity",
                                    "failed:payload_policy",
                                    "failed:kintone_read", "failed:immutable",
                                    "failed:unexpected")})
    # §5A の各例外行 → 2軸値の写像（表の部分集合であること）
    SEC5A = [
        ("failed:chain_integrity", "skipped"),
        ("run_conflict", "skipped"),
        ("failed:payload_policy", "skipped"),
        ("created", "failed:policy"), ("no_change", "failed:policy"),
        ("created", "failed:search"), ("no_change", "failed:search"),
        ("created", "ack_unknown"), ("no_change", "ack_unknown"),
        ("failed:kintone_read", "skipped"),
        ("failed:immutable", "skipped"),
        ("failed:unexpected", "skipped"),                  # run 保存前
        ("created", "failed:unexpected"),                  # run 保存後
        ("no_change", "failed:unexpected"),
    ]

    def test_full_bidirectional_match(self):
        self.assertEqual(ht.LEGAL_COMBINATIONS, self.TABLE)  # 全対一致
        for r in sorted(ht.RUN_RESULTS):
            for e in sorted(ht.ENVELOPE_RESULTS):
                with self.subTest(run=r, env=e):
                    if (r, e) in self.TABLE:               # 表にある＝全て受理
                        line = ht.build_heir_cmd_log(r, e, "9", 1, "70")
                        self.assertIn(f"run={r} envelope={e}", line)
                    else:                                  # 表にない＝全て拒否
                        with self.assertRaises(ValueError):
                            ht.build_heir_cmd_log(r, e, "9", 1, "70")

    def test_sec5a_mapping_is_subset(self):
        for pair in self.SEC5A:
            with self.subTest(pair=pair):
                self.assertIn(pair, ht.LEGAL_COMBINATIONS)
        # 負系: (failed:unexpected, failed:*) は定義外（run 段で死んだら封筒未到達）
        for e in ("failed:policy", "failed:search", "failed:unexpected",
                  "ack_unknown", "filed"):
            with self.subTest(env=e):
                self.assertNotIn(("failed:unexpected", e), ht.LEGAL_COMBINATIONS)


# ── §7-20: field 集合の構造試験（canonical schema とエンジンの完全一致）──────
class TestCanonicalFieldSets(unittest.TestCase):
    def test_person_and_event_fields_match_engine(self):
        self.assertEqual(
            [f.name for f in dataclasses.fields(HeirPerson)],
            list(dm.CANONICAL_PERSON_FIELDS))
        self.assertEqual(
            [f.name for f in dataclasses.fields(LifeEvent)],
            list(dm.CANONICAL_EVENT_FIELDS))
        self.assertEqual(dm.CANONICAL_SCHEMA_VERSION, 2)


if __name__ == "__main__":
    unittest.main()
