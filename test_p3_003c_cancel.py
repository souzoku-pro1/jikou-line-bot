"""P3-003C-CANCEL 実装テスト（正本: DRAFT_P3_003C_CANCEL.md・FROZEN）。

固定する契約:
- flag HEIR_CANCEL_ENABLED の 4 象限（OFF/ON × 語彙可視性/実行可否・OFF は
  全経路不発＝kintone/DB write 0）
- 取消関所の一本経路（弁護士の明示操作のみ・ALLOWLIST 検証・裁定④）
- decision 鎖: supersede rejected 型（裁定①=(A)）・head のみ取消可（裁定⑥）・
  一般経路の already_confirmed 中止セルは不変・二重取消抑止
- App36 巻き戻し（裁定②/CANCEL-06）: update 行=preimage 復元（postimage 完全
  一致時のみ・不一致=write 0 要確認）／insert 行=無効化（戸籍確認済=no＋
  取消済み=yes の postimage 閉集合）・行削除なし・human_state 非接触
- 台帳（裁定⑤=projection_log）: write-set 捕捉（insert/update・preimage）・
  immutable・phase 順序（台帳追記→巻き戻し）・部分失敗=封筒 open＋resumed 回収・
  legacy=取消台帳のみ追記（App36 write 0・裁定⑧）
- consumer 除外: 取消済み=yes 行の読み飛ばし（共通 filter・単一の正）＋
  App36 reader manifest 閉包検査（RV08 の型）
- H11a 変更不要の設計帰結（no 化により監査対象外・最終網は維持）
kintone・通知は mock、DB は sqlite 実体。
"""

import asyncio
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import hub.db as db  # noqa: E402
from hub import kintone  # noqa: E402
from hub.app36_validity import (CANCELLED_FIELD, CANCELLED_VALUES,  # noqa: E402
                                filter_active_heir_rows, is_active_heir_row)
from hub.derivation_models import (DecisionBlockedError,  # noqa: E402
                                   DerivationBase, HeirConfirmationDecision,
                                   ProjectionLog, WRITESET_SCHEMA_VERSION,
                                   append_projection_log,
                                   create_decisions_for_heads,
                                   create_derivation_run, get_leaf_decision,
                                   load_write_set)
from hub import heir_cancel as hc  # noqa: E402

_ENV = {
    "ATTORNEY_ALLOWLIST": "ATT1,ATT2",
    "SOUZOKU_KINTONE_APP_ID": "26", "KINTONE_APP_ID": "21",
    "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "t36",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
    "HEIR_CANCEL_ENABLED": "1",
    # 他テストの env 残置に依存しない（App34 有効性ガードは対象外に固定）
    "APP_KOSEKI_PERSON": "", "TOKEN_KOSEKI_PERSON": "",
}

_NOW = datetime(2026, 8, 13, 3, 0, tzinfo=timezone.utc)


def _run(coro):
    return asyncio.run(coro)


def _payload(*heirs):
    return {"heirs": list(heirs), "facts": ["minpo_890"]}


def _heir(pid, code, share="1/2"):
    return {"person_id": pid, "zokugara_code": code, "share": share}


class _Base(unittest.TestCase):
    """sqlite 実 DB＋kintone/通知 mock（test_p3_003c_impl と同じ土台）。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3cc_")
        env = dict(_ENV)
        env["DATABASE_URL"] = f"sqlite+aiosqlite:///{self._dir}/a.db"
        self._env = patch.dict(os.environ, env)
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(DerivationBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

        # App36 実体 store（rid → record dict）と App30 封筒 store
        self.app36: dict[str, dict] = {}
        self.envelopes: list[dict] = []
        self.updated: list[tuple] = []
        self.created: list[tuple] = []

        async def fake_get(app, rid):
            if app.label.startswith("App 36"):
                rec = self.app36.get(str(rid))
                if rec is None:
                    raise kintone.KintoneError(404, "GAIA_RE01", "")
                return dict(rec)
            raise kintone.KintoneError(404, "GAIA_RE01", "")

        async def fake_search(app, query, fields=None):
            if app.label.startswith("App 30"):
                return [dict(e) for e in self.envelopes]
            if app.label.startswith("App 36"):
                return [dict(r) for r in self.app36.values()]
            return []

        async def fake_create(app, fields):
            self.created.append((app.label, dict(fields)))
            rid = str(700 + len(self.created))
            if app.label.startswith("App 30"):
                self.envelopes.append({
                    "$id": {"value": rid},
                    "チャネル固有データ":
                        {"value": fields.get("チャネル固有データ", "")},
                    "発送ステータス": {"value": fields.get("発送ステータス", "")},
                    "実行済み": {"value": fields.get("実行済み", "no")}})
            return rid

        async def fake_update(app, rid, fields, revision=None):
            self.updated.append((app.label, str(rid), dict(fields), revision))
            if app.label.startswith("App 36") and str(rid) in self.app36:
                for k, v in fields.items():
                    self.app36[str(rid)][k] = {"value": v}
            if app.label.startswith("App 30"):
                for e in self.envelopes:
                    if e["$id"]["value"] == str(rid):
                        for k, v in fields.items():
                            e[k] = {"value": v}

        self.get = AsyncMock(side_effect=fake_get)
        self.search = AsyncMock(side_effect=fake_search)
        self.create = AsyncMock(side_effect=fake_create)
        self.update = AsyncMock(side_effect=fake_update)
        self.delete = AsyncMock()
        self.alert = AsyncMock(return_value=True)
        for target, mock in [("get_record", self.get),
                             ("search_records", self.search),
                             ("create_record", self.create),
                             ("update_record", self.update),
                             ("delete_record", self.delete)]:
            p = patch(f"hub.kintone.{target}", new=mock)
            p.start()
            self.addCleanup(p.stop)
        p = patch("hub.notify.notify_admin_line", new=self.alert)
        p.start()
        self.addCleanup(p.stop)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    # ── seed helpers ──────────────────────────────────────────────────────

    def _mk_run(self, case="9", status="derived"):
        pk = _run(create_derivation_run(
            case_app_id="26", case_record_id=case, decedent_person_id="10",
            at_date="2026-01-01", frozen_case_version="v0.1",
            input_person_revisions={}, input_person_ids=[],
            input_hash=f"ih-{os.urandom(8).hex()}", status=status, rank=1,
            result_payload=_payload(_heir("11", "child")),
            result_hash="rh" * 32, lawyer_flags=None, provisional=True,
            supersedes_run_id=None, engine_version="e1"))
        db.reset_for_tests()
        return pk

    def _confirm_run(self, case, run_id):
        _run(create_decisions_for_heads(
            case, [run_id], decision="confirmed", decided_by="ATT1",
            decided_at=_NOW))
        db.reset_for_tests()

    def _app36_row(self, rid, **values):
        rec = {"$id": {"value": str(rid)}, "$revision": {"value": "3"}}
        for k, v in values.items():
            rec[k] = {"value": v}
        self.app36[str(rid)] = rec

    def _log(self, run_id, case, rid, op, written, pre):
        _run(append_projection_log(
            derivation_run_id=run_id, case_record_id=case,
            app36_record_id=str(rid), op=op, fields_written=written,
            preimage=pre))
        db.reset_for_tests()

    def _seed_confirmed_with_writeset(self, case="9"):
        """confirmed 済み run＋write-set＋App36 現物（postimage 一致状態）。
        行 801=insert（戸籍確認済 yes）・行 802=update（続柄を書換え済み）。"""
        run_id = self._mk_run(case)
        self._confirm_run(case, run_id)
        ins_written = {"続柄": "子", "データ源": "戸籍読解",
                       "current_derivation_run_id": str(run_id),
                       "導出元人物ID": "11", "案件アプリID": "26",
                       "案件レコードID": case, "ユニット種別": "相続一般",
                       "戸籍確認済": "yes"}
        upd_written = {"続柄": "配偶者", "データ源": "戸籍読解",
                       "current_derivation_run_id": str(run_id),
                       "導出元人物ID": "12"}
        upd_pre = {"続柄": "子", "データ源": "手入力",
                   "current_derivation_run_id": "1", "導出元人物ID": "12"}
        self._log(run_id, case, "801", "insert", ins_written, {})
        self._log(run_id, case, "802", "update", upd_written, upd_pre)
        self._app36_row("801", **ins_written)
        self._app36_row("802", **upd_written, 氏名="山田花子")
        return run_id

    def _cancel(self, case="9", by="ATT1"):
        r = _run(hc.execute_cancel(case, by, _NOW))
        db.reset_for_tests()
        return r

    def _plan(self, case="9", by="ATT1"):
        r = _run(hc.plan_cancel(case, by))
        db.reset_for_tests()
        return r

    def _leaf(self, run_id):
        leaf = _run(get_leaf_decision(run_id))
        db.reset_for_tests()
        return leaf

    def _app36_writes(self):
        return [u for u in self.updated if u[0].startswith("App 36")]


class TestFlagQuadrants(_Base):
    """flag HEIR_CANCEL_ENABLED の 4 象限（OFF は全経路不発）"""

    def test_off_vocabulary_hidden(self):
        from dispatch_bot import registry
        env = {k: v for k, v in os.environ.items()
               if k != "HEIR_CANCEL_ENABLED"}
        with patch.dict(os.environ, env, clear=True):
            self.assertNotIn("heir_cancel", registry.catalog_for_prompt())

    def test_on_vocabulary_visible(self):
        from dispatch_bot import registry
        self.assertIn("heir_cancel", registry.catalog_for_prompt())

    def test_off_all_paths_refuse_with_zero_io(self):
        run_id = self._seed_confirmed_with_writeset()
        self.get.reset_mock(); self.search.reset_mock()
        self.create.reset_mock(); self.update.reset_mock()
        with patch.dict(os.environ, {"HEIR_CANCEL_ENABLED": ""}):
            plan = _run(hc.plan_cancel("9", "ATT1"))
            db.reset_for_tests()
            result = _run(hc.execute_cancel("9", "ATT1", _NOW))
            db.reset_for_tests()
        self.assertEqual(plan["status"], "disabled")
        self.assertEqual(result["status"], "disabled")
        for m in (self.get, self.search, self.create, self.update):
            m.assert_not_awaited()
        self.assertEqual(self._leaf(run_id).decision, "confirmed",
                         "OFF では台帳へも書かない")

    def test_on_executes(self):
        run_id = self._seed_confirmed_with_writeset()
        result = self._cancel()
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(self._leaf(run_id).decision, "rejected")


class TestCancelGate(_Base):
    """一本経路の関所検証（ALLOWLIST・grammar・取消可能条件・裁定④⑥）"""

    def test_allowlist_rejects_outsider(self):
        self._seed_confirmed_with_writeset()
        plan = self._plan(by="OUTSIDER")
        self.assertEqual(plan["status"], "aborted")
        self.assertIn("取消権限がありません", plan["reason"])
        self.assertNotIn("OUTSIDER", plan["reason"], "識別子の値は非表示")

    def test_case_grammar(self):
        plan = self._plan(case="9; drop")
        self.assertEqual(plan["status"], "aborted")

    def test_no_run_aborts(self):
        plan = self._plan(case="9")
        self.assertIn("導出 run がありません", plan["reason"])

    def test_unconfirmed_leaf_aborts(self):
        run_id = self._mk_run("9")
        plan = self._plan()
        self.assertIn("確定済みではありません", plan["reason"])
        _run(create_decisions_for_heads("9", [run_id], decision="held",
                                        decided_by="ATT1", decided_at=_NOW))
        db.reset_for_tests()
        plan = self._plan()
        self.assertIn("確定済みではありません", plan["reason"])
        self.assertEqual(self._app36_writes(), [])

    def test_normally_rejected_leaf_aborts(self):
        """通常否認（rejected が held/root を supersede）は取消対象外"""
        run_id = self._mk_run("9")
        _run(create_decisions_for_heads("9", [run_id], decision="held",
                                        decided_by="ATT1", decided_at=_NOW))
        db.reset_for_tests()
        _run(create_decisions_for_heads("9", [run_id], decision="rejected",
                                        decided_by="ATT1", decided_at=_NOW))
        db.reset_for_tests()
        plan = self._plan()
        self.assertEqual(plan["status"], "aborted")
        self.assertIn("否認済み", plan["reason"])

    def test_general_path_confirmed_to_rejected_still_blocked(self):
        """一般経路（確定/保留/否認コマンド）の already_confirmed 中止セルは
        不変＝取消語彙の解禁は create_cancel_decision の一本経路のみ"""
        run_id = self._mk_run("9")
        self._confirm_run("9", run_id)
        with self.assertRaises(DecisionBlockedError) as ctx:
            _run(create_decisions_for_heads(
                "9", [run_id], decision="rejected", decided_by="ATT1",
                decided_at=_NOW))
        db.reset_for_tests()
        self.assertEqual(ctx.exception.code, "already_confirmed")


class TestCancelChain(_Base):
    """decision 鎖の取消表現（裁定①=(A) supersede rejected 型）と二重取消抑止"""

    def test_cancel_supersedes_confirmed_with_rejected(self):
        run_id = self._seed_confirmed_with_writeset()
        result = self._cancel()
        self.assertEqual(result["status"], "cancelled")
        leaf = self._leaf(run_id)
        self.assertEqual(leaf.decision, "rejected")

        async def _chain():
            t = HeirConfirmationDecision.__table__
            async with db.session_scope() as s:
                rows = (await s.execute(
                    sa.select(t.c.id, t.c.decision, t.c.supersedes_decision_id)
                    .order_by(t.c.id.asc()))).all()
            return rows
        rows = _run(_chain())
        db.reset_for_tests()
        self.assertEqual([r.decision for r in rows], ["confirmed", "rejected"])
        self.assertEqual(rows[1].supersedes_decision_id, rows[0].id,
                         "追記のみ（confirmed は不改変・supersede 連鎖）")

    def test_double_cancel_blocked(self):
        self._seed_confirmed_with_writeset()
        first = self._cancel()
        self.assertTrue(first["envelope_closed"])
        n_updates = len(self._app36_writes())
        second = self._cancel()
        self.assertEqual(second["status"], "aborted")
        self.assertIn("取消済み", second["reason"])
        self.assertEqual(len(self._app36_writes()), n_updates,
                         "二重取消で App36 write なし")
        self.assertEqual(
            len([c for c in self.created if c[0].startswith("App 30")]), 1,
            "取消封筒の二重起票なし（冪等キー一意化）")


class TestRollback(_Base):
    """App36 巻き戻し（裁定②・CANCEL-06 postimage 閉集合・照合規律）"""

    def test_insert_row_invalidated_closed_set(self):
        self._seed_confirmed_with_writeset()
        result = self._cancel()
        self.assertEqual(result["rolled_back"], 2)
        w = [u for u in self._app36_writes() if u[1] == "801"]
        self.assertEqual(len(w), 1)
        self.assertEqual(w[0][2], {"戸籍確認済": "no", CANCELLED_FIELD: "yes"},
                         "insert 行の無効化 postimage は閉集合（これ以外書かない"
                         "＝human_state 非接触・行削除しない）")
        self.assertEqual(w[0][3], "3", "revision 楽観ロックつき")
        self.delete.assert_not_awaited()

    def test_update_row_restored_to_preimage(self):
        self._seed_confirmed_with_writeset()
        self._cancel()
        w = [u for u in self._app36_writes() if u[1] == "802"]
        self.assertEqual(len(w), 1)
        self.assertEqual(w[0][2],
                         {"続柄": "子", "データ源": "手入力",
                          "current_derivation_run_id": "1",
                          "導出元人物ID": "12"},
                         "update 行は write-set の preimage を復元（書込み field "
                         "のみ・氏名等の human field 非接触）")
        self.assertNotIn(CANCELLED_FIELD, w[0][2],
                         "update 行は無効化しない（preimage 復元のみ）")

    def test_postimage_mismatch_writes_nothing_and_keeps_envelope_open(self):
        run_id = self._seed_confirmed_with_writeset()
        # projection 後に人手編集（続柄を変更）＝postimage 不一致
        self.app36["802"]["続柄"] = {"value": "兄弟姉妹"}
        result = self._cancel()
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(result["rolled_back"], 1, "一致行(801)のみ巻き戻し")
        self.assertEqual(result["held"], 1)
        self.assertFalse(result["envelope_closed"], "部分失敗＝封筒 open 維持")
        self.assertEqual([u for u in self._app36_writes() if u[1] == "802"],
                         [], "不一致行は write 0（機械は上書きしない）")
        self.assertEqual(self._leaf(run_id).decision, "rejected",
                         "台帳追記（phase 2）は完了している")

    def test_phase_order_ledger_before_app36(self):
        """phase 順序（§4.4）: 台帳追記の失敗時は App36 へ一切書かない"""
        self._seed_confirmed_with_writeset()
        with patch.object(hc, "create_cancel_decision",
                          new=AsyncMock(side_effect=hc.ChainIntegrityError("x"))):
            result = self._cancel()
        self.assertEqual(result["status"], "aborted")
        self.assertEqual(self._app36_writes(), [],
                         "phase 2 失敗＝App36 write 0（phase 2 より前に書かない）")

    def test_legacy_confirmed_ledger_only(self):
        """legacy（write-set なし・裁定⑧）: 取消台帳のみ追記・App36 write 0"""
        run_id = self._mk_run("9")
        self._confirm_run("9", run_id)
        self._app36_row("801", 戸籍確認済="yes",
                        current_derivation_run_id=str(run_id))
        result = self._cancel()
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(result["mode"], "legacy")
        self.assertEqual(self._app36_writes(), [], "App36 write 0")
        self.assertEqual(self._leaf(run_id).decision, "rejected",
                         "取消台帳（decision 鎖）は追記される")
        env_update = [u for u in self.updated if u[0].startswith("App 30")]
        self.assertTrue(any("人手調査" in str(u[2]) for u in env_update),
                        "封筒 detail に人手調査の注記")

    def test_legacy_old_schema_version(self):
        """schema version 不一致（旧 version の write-set）も legacy へ倒す
        （CANCEL-05: 解釈できない write-set で盲目適用しない）"""
        run_id = self._mk_run("9")
        self._confirm_run("9", run_id)
        self._log(run_id, "9", "801", "insert", {"戸籍確認済": "yes"}, {})
        with patch.object(hc, "WRITESET_SCHEMA_VERSION", 999):
            plan = self._plan()
        self.assertEqual(plan["mode"], "legacy")


class TestResumedRecovery(_Base):
    """ACK 喪失の回収（§4.4・phase 2 済み×巻き戻し未了→phase 3 のみ再実行）"""

    def test_resume_after_phase2_crash(self):
        run_id = self._seed_confirmed_with_writeset()
        # phase 2 完了直後のクラッシュを模擬: 封筒 open＋取消 decision 済み・
        # App36 未巻き戻し
        _run(hc._file_cancel_envelope({
            "idem_key": hc.cancel_idempotency_key("9", run_id),
            "case_record_id": "9", "run_id": run_id, "mode": "auto"}))
        db.reset_for_tests()
        _run(hc.create_cancel_decision("9", run_id, decided_by="ATT1",
                                       decided_at=_NOW))
        db.reset_for_tests()

        plan = self._plan()
        self.assertEqual(plan["mode"], "resumed", "取消記録済み・未了を検出")
        result = self._cancel()
        self.assertEqual(result["status"], "cancelled")
        self.assertEqual(result["decision_outcome"], "resumed",
                         "decision の二重 INSERT なし")
        self.assertEqual(result["rolled_back"], 2)
        self.assertTrue(result["envelope_closed"])
        self.assertEqual(
            len([c for c in self.created if c[0].startswith("App 30")]), 1,
            "既存 open 封筒を再利用（再起票しない）")

    def test_resume_skips_already_invalidated_rows(self):
        """resumed 再実行は巻き戻し済み行（取消済み=yes）を再書込みしない"""
        run_id = self._seed_confirmed_with_writeset()
        first = self._cancel()
        self.assertTrue(first["envelope_closed"])
        # クローズ済みのため二重取消は抑止される（上の TestCancelChain）——
        # ここでは封筒を open に戻した異常系でも 801 を再書込みしないことを固定
        for e in self.envelopes:
            e["実行済み"] = {"value": "no"}
        n = len([u for u in self._app36_writes() if u[1] == "801"])
        self._cancel()
        self.assertEqual(
            len([u for u in self._app36_writes() if u[1] == "801"]), n,
            "無効化済み行は完了扱い（盲目再適用しない）")


class TestWriteSetCapture(_Base):
    """projection の write-set 捕捉（裁定⑤・_project_row 発）と immutable"""

    def test_project_row_insert_captures_writeset(self):
        from types import SimpleNamespace
        run_id = self._mk_run("9")
        run = SimpleNamespace(id=run_id, case_app_id="26")
        outcome = _run(hp_project_row(run, "9", "相続一般", "11", "子", None))
        db.reset_for_tests()
        self.assertEqual(outcome, "inserted")
        ws = _run(load_write_set(run_id))
        db.reset_for_tests()
        self.assertEqual(len(ws), 1)
        self.assertEqual(ws[0]["op"], "insert")
        self.assertEqual(ws[0]["preimage"], {})
        self.assertEqual(ws[0]["fields_written"]["戸籍確認済"], "yes")
        self.assertEqual(ws[0]["schema_version"], WRITESET_SCHEMA_VERSION)

    def test_project_row_update_captures_preimage(self):
        from types import SimpleNamespace
        run_id = self._mk_run("9")
        self._app36_row("805", 導出元人物ID="11", 続柄="配偶者",
                        current_derivation_run_id=str(run_id),
                        戸籍確認済="yes", データ源="手入力", 法定相続分="")
        run = SimpleNamespace(id=run_id, case_app_id="26")
        outcome = _run(hp_project_row(run, "9", "相続一般", "11", "子", None))
        db.reset_for_tests()
        self.assertEqual(outcome, "updated")
        ws = _run(load_write_set(run_id))
        db.reset_for_tests()
        self.assertEqual(ws[0]["op"], "update")
        self.assertEqual(ws[0]["preimage"]["続柄"], "配偶者",
                         "書込み前の値を preimage として保存")
        self.assertEqual(set(ws[0]["preimage"]) , set(ws[0]["fields_written"]),
                         "preimage は書込み field と同じ code 集合")

    def test_projection_log_is_immutable(self):
        run_id = self._mk_run("9")
        self._log(run_id, "9", "801", "insert", {"a": "1"}, {})

        async def _mutate():
            t = ProjectionLog.__table__
            async with db.session_scope() as s:
                await s.execute(sa.update(t).values(op="update"))
        with self.assertRaises(Exception):   # DB trigger（immutable）
            _run(_mutate())
        db.reset_for_tests()


class TestConsumerExclusion(_Base):
    """consumer 除外（取消済み=yes の読み飛ばし・共通 filter・単一の正）"""

    def test_filter_semantics_match_rv08_pattern(self):
        self.assertTrue(is_active_heir_row({}))                       # CU 前互換
        self.assertTrue(is_active_heir_row({CANCELLED_FIELD: {"value": ""}}))
        self.assertTrue(is_active_heir_row({CANCELLED_FIELD: {"value": "no"}}))
        self.assertFalse(is_active_heir_row({CANCELLED_FIELD: {"value": "yes"}}))
        self.assertFalse(is_active_heir_row({CANCELLED_FIELD: {"value": "怪"}}),
                         "閉集合外は無効扱い（安全側・person_validity と同規約）")

    def test_closed_set_matches_config(self):
        import config
        f = config.EXPECTED_KINTONE_SCHEMA["App 36 (相続人)"]["fields"]
        self.assertEqual(tuple(f[CANCELLED_FIELD]["required_options"]),
                         CANCELLED_VALUES)
        self.assertEqual(f[CANCELLED_FIELD]["type"], "RADIO_BUTTON")

    def test_projection_skips_cancelled_row_and_inserts_fresh(self):
        """取消済み行は冪等検索で読み飛ばし＝再導出の projection は新規行を
        insert する（取消済み行の資源化・復活をしない）"""
        from types import SimpleNamespace
        run_id = self._mk_run("9")
        self._app36_row("801", 導出元人物ID="11", 続柄="子",
                        current_derivation_run_id="1",
                        戸籍確認済="no", データ源="戸籍読解",
                        法定相続分="", **{CANCELLED_FIELD: "yes"})
        run = SimpleNamespace(id=run_id, case_app_id="26")
        outcome = _run(hp_project_row(run, "9", "相続一般", "11", "子", None))
        db.reset_for_tests()
        self.assertEqual(outcome, "inserted",
                         "取消済み行は update 対象にならず新規 insert")


class TestReaderManifestClosure(unittest.TestCase):
    """App36 reader manifest 閉包検査（RV08 test_rv08_soft_merge の型を適用）。

    manifest は (module, function) → 規律。search=filter は関数内で
    filter_active_heir_rows を通すこと・exempt は理由必須。閉包は両方向
    （未登録 reader=FAIL・実体の無い entry=FAIL）。App36 参照は
    APP_SOUZOKUNIN 名参照に加え KintoneApp("App 36…") 再構築も検出する
    （shokumu_plan の inline 構築を取りこぼさない）。"""

    MANIFEST = {
        ("hub/heir_projection.py", "_resolve_heir_derivation"):
            {"search": "filter"},
        ("hub/heir_projection.py", "_project_row"): {"search": "filter"},
        ("hub/shokumu_plan.py", "_second_stage_conditions"):
            {"search": "filter"},
        ("hub/shokumu_plan.py", "_app36_rows_hash"): {"search": "filter"},
        ("daily_healthcheck.py", "check_app36_confirmed_decisions"): {
            "search": "exempt",
            "reason": "H11a は最終網（凍結票 §4.2/§4.4: H11a 変更不要・取消済み"
                      "行が人手で yes 化された場合も検知対象に載せる設計）"},
        ("hub/heir_cancel.py", "_verify_rows"): {
            "get": "exempt",
            "reason": "取消関所の postimage 照合（write-set 記載行の直接 get・"
                      "取消済み行も読んで完了判定する必要がある・§4.1a）"},
    }

    @classmethod
    def setUpClass(cls):
        import ast
        import subprocess
        from pathlib import Path
        repo = Path(__file__).parent
        files = subprocess.run(["git", "ls-files", "*.py"], cwd=repo,
                               capture_output=True, text=True,
                               check=True).stdout.splitlines()
        cls.readers = {}
        cls.sources = {}
        for f in files:
            name = Path(f).name
            if (name.startswith("test_") or name == "conftest.py"
                    or f.startswith("legacy/") or f.startswith("alembic/")):
                continue
            src = (repo / f).read_text(encoding="utf-8")
            tree = ast.parse(src)
            for top in ast.walk(tree):
                if not isinstance(top, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                refs_app36 = False
                reads = []
                filters = False
                for node in ast.walk(top):
                    if isinstance(node, ast.Name) and node.id == "APP_SOUZOKUNIN":
                        refs_app36 = True
                    if isinstance(node, ast.Attribute) \
                            and node.attr == "APP_SOUZOKUNIN":
                        refs_app36 = True
                    if isinstance(node, ast.Call):
                        fn = node.func
                        attr = fn.attr if isinstance(fn, ast.Attribute) else (
                            fn.id if isinstance(fn, ast.Name) else "")
                        if attr == "KintoneApp" and any(
                                isinstance(a, ast.Constant)
                                and isinstance(a.value, str)
                                and ("App 36" in a.value
                                     or a.value == "APP_SOUZOKUNIN")
                                for a in node.args):
                            refs_app36 = True
                        if attr in ("search_records", "get_record"):
                            reads.append(attr)
                        if attr == "filter_active_heir_rows":
                            filters = True
                if refs_app36 and reads:
                    cls.readers[(f, top.name)] = {"reads": set(reads),
                                                  "filters": filters}

    def test_manifest_is_exact_closure(self):
        found = set(self.readers)
        listed = set(self.MANIFEST)
        self.assertEqual(sorted(found - listed), [],
                         "manifest 未登録の App36 reader 関数（filter を通すか "
                         "exempt 理由を書いて登録すること）")
        self.assertEqual(sorted(listed - found), [],
                         "実体の無い manifest エントリ（削除漏れ）")

    def test_filter_disciplines_hold(self):
        for key, decl in self.MANIFEST.items():
            if decl.get("search") == "filter":
                self.assertTrue(self.readers[key]["filters"],
                                f"{key}: search=filter 宣言だが "
                                "filter_active_heir_rows を通していない")

    def test_exempt_entries_have_reasons(self):
        for key, decl in self.MANIFEST.items():
            if "exempt" in decl.values():
                self.assertTrue(str(decl.get("reason") or "").strip(),
                                f"{key}: exempt には理由必須")

    def test_h11a_source_has_no_cancel_filter(self):
        """H11a 変更不要の構造固定: 監査は共通 filter を通さない（最終網）"""
        from pathlib import Path
        src = (Path(__file__).parent / "daily_healthcheck.py").read_text(
            encoding="utf-8")
        self.assertNotIn("filter_active_heir_rows", src)
        self.assertNotIn("app36_validity", src)


class TestH11aUnchanged(_Base):
    """H11a 変更不要の設計帰結（票6・凍結票 §4.2: no 化により監査対象外）"""

    def _audit(self, yes_rows):
        import daily_healthcheck as dh

        async def fake_search(app, query, fields=None):
            # 実機の server-side filter を模擬: query は yes 行のみ返す
            self.assertIn('戸籍確認済 in ("yes")', query,
                          "H11a は yes 行のみ走査（server-side・変更なし）")
            return yes_rows
        with patch("hub.kintone.search_records",
                   new=AsyncMock(side_effect=fake_search)):
            problems = _run(dh.check_app36_confirmed_decisions())
        db.reset_for_tests()
        return problems

    def test_cancelled_insert_row_is_out_of_audit_scope(self):
        """取消済み insert 行は 戸籍確認済=no＝yes 走査に載らない→検知ゼロ
        （H11a 側の変更なしで取消済み行が監査を汚さない）"""
        run_id = self._seed_confirmed_with_writeset()
        result = self._cancel()
        self.assertTrue(result["envelope_closed"])
        self.assertEqual(self.app36["801"]["戸籍確認済"]["value"], "no")
        # 実機 query（yes のみ）が返す集合＝残った yes 行のみ（801 は no）
        yes_rows = [dict(r) for r in self.app36.values()
                    if r.get("戸籍確認済", {}).get("value") == "yes"]
        self.assertEqual(self._audit(yes_rows), [])

    def test_manually_reyessed_cancelled_row_is_detected(self):
        """最終網: 取消後に人手で yes 化された行は leaf=rejected のため検知
        される（H11a が filter を通さない設計の帰結・§4.4）"""
        run_id = self._seed_confirmed_with_writeset()
        self._cancel()
        row = {"$id": {"value": "801"},
               "current_derivation_run_id": {"value": str(run_id)}}
        problems = self._audit([row])
        self.assertEqual(len(problems), 1)
        self.assertIn("confirmed decision", problems[0])


class TestCommandLayer(_Base):
    """dispatch_bot 語彙（復唱対象 R1(iv)・flag 辞退・invalidate）"""

    def _cand(self):
        from types import SimpleNamespace
        return SimpleNamespace(record_id="9", customer_name="山田太郎",
                               status="受任")

    def test_readback_lists_required_items(self):
        from dispatch_bot import heir_cancel_task as ct
        run_id = self._seed_confirmed_with_writeset()
        created = []
        with patch("dispatch_bot.confirm.create",
                   new=lambda *a, **k: created.append(a)), \
                patch.dict("dispatch_bot.handler._sessions", {}, clear=True):
            msg = _run(ct._confirm("ATT1", {"task_params": {}}, "取消して",
                                   self._cand()))
        db.reset_for_tests()
        self.assertIn("No.9", msg)                      # 案件レコードID
        self.assertIn(f"run #{run_id}", msg)            # 対象 run id
        self.assertIn("No.801", msg)                    # App36 record ID 集合
        self.assertIn("No.802", msg)
        self.assertIn("無効化", msg)                     # 巻き戻し内容の要約
        self.assertIn("復元", msg)
        self.assertIn("OK / キャンセル", msg)
        self.assertNotIn("山田花子", msg, "PII（App36 の氏名値）は載せない")
        self.assertEqual(len(created), 1, "pending 発行")

    def test_flow_refuses_when_flag_off(self):
        from dispatch_bot import heir_cancel_task as ct
        with patch.dict(os.environ, {"HEIR_CANCEL_ENABLED": ""}), \
                patch.dict("dispatch_bot.handler._sessions", {}, clear=True):
            msg = _run(ct.flow("U1", {"task_params": {}}, "取消して", None))
        self.assertEqual(msg, hc.MSG_CANCEL_DISABLED)

    def test_execute_invalidates_pending_on_all_paths(self):
        from types import SimpleNamespace

        from dispatch_bot import heir_cancel_task as ct
        self._seed_confirmed_with_writeset()
        pending = SimpleNamespace(
            user_id="ATT1",
            parsed={"task_params": {"case_record_id": "9"}})
        with patch("dispatch_bot.confirm.invalidate") as inv:
            msg, rid, _url = _run(ct.execute(pending))
        db.reset_for_tests()
        self.assertIn("取消を完了しました", msg)
        inv.assert_called_once_with("ATT1")


def hp_project_row(run, case, unit, pid, zoku, share):
    """heir_projection._project_row の呼出しヘルパ（ancestors 空集合）。"""
    from hub import heir_projection as hp
    return hp._project_row(run, case, unit, pid, zoku, share, set())


if __name__ == "__main__":
    unittest.main()
