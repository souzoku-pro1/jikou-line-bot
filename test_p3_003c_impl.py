"""P3-003c: held/rejected 語彙（判断保留・否認の decision 経路）のテスト。

正本 DRAFT_P3_003C_HELD_REJECTED（FROZEN・R-P3-003C-D3 PASS）の §7 テスト計画
20 系統のうち、既存 suite の維持（§7-1）と既存 pin 流用分（§7-2 の制約系）を除く
新規分を pin する。kintone は全て mock（実機・ネットワーク非依存）・DB は sqlite。

対応表（§7 → 本ファイル）:
- §7-2/§7-13: TestDecisionChainTransitions（12 遷移閉集合・rejected 後の新 run 経路）
- §7-3: TestRejectedLeafBlocksConfirm（App36 呼出しゼロ＋aborted）
- §7-4/§7-14: TestApp36ZeroAccess（異常行存在下でも held/rejected は App36 無照会）
- §7-5/§7-20: TestEnvelopeSideEffects（held=open+注記／rejected=単一 update で
  完了+yes+注記・既存 detail キー全保持）
- §7-6: TestVocabularyAndReadback（閉集合判定・曖昧聞き返し・復唱の種別+帰結）
- §7-7/§7-16: TestAllowlistSymmetric（3 decision 対称拒否）
- §7-8: TestDecisionUnsupportedSource（decision 語彙 × 非 heir グループ）
- §7-9/§7-11/§7-15: TestHeldConfirmChainAndResume（held→confirmed→row-held→
  再確定 resume・注記終端・confirmed→confirmed 3 分岐）
- §7-10: TestRejectedRejected（decision 追加なし・封筒クローズ後は gate 固定応答）
- §7-12: TestDeriveNoChangeAfterRejected（否認済み・入力未変更の全面 no-op 明示）
- §7-17: TestSideEffectResume（App30 失敗→再指示で冪等再適用・単一 call 完全
  field 集合・decided_at=leaf 保存値の時刻不変）
- §7-18: TestIntegrityNormalization（ChainIntegrityError 正規化・本文非露出）
- §7-19: TestLeafFailClosed（0/1/複数の 3 分類・一本鎖破損）
- 構造 pin（発注 3）: TestBranchStructureSource（held/rejected 経路の App36 無参照）
"""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

import hub.db as db  # noqa: E402
from hub import heir_projection as hp  # noqa: E402
from hub.derivation_models import (ChainIntegrityError,  # noqa: E402
                                   DecisionBlockedError, DerivationBase,
                                   HeirConfirmationDecision,
                                   create_confirmed_decisions_for_heads,
                                   create_decisions_for_heads,
                                   create_derivation_run, get_leaf_decision)
from hub.heir_envelope import (DETAIL_DECISION_KEY,  # noqa: E402
                               DETAIL_HELD_PERSONS_KEY)
from review_resolve import (MSG_DECISION_UNSUPPORTED, ReviewGroup,  # noqa: E402
                            ReviewItem, resolve_group)

_ENV = {
    "ATTORNEY_ALLOWLIST": "ATT1,ATT2",
    "SOUZOKU_KINTONE_APP_ID": "26", "KINTONE_APP_ID": "21",
    "APP_SOUZOKUNIN": "36", "TOKEN_SOUZOKUNIN": "t36",
    "APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
}

DB_SENTINEL = "uq_heir_decision_supersedes_SENTINEL_DB本文"


def _run(coro):
    return asyncio.run(coro)


def _payload(*heirs):
    return {"heirs": list(heirs), "facts": ["minpo_890"]}


def _heir(pid, code, share="1/2"):
    return {"person_id": pid, "zokugara_code": code, "share": share}


def _item(run_id, record_id="70", extra_detail=None):
    detail = {"derivation_run_id": run_id, "case_record_id": "9", "冪等キー": "k"}
    if extra_detail:
        detail.update(extra_detail)
    return ReviewItem(record_id=record_id, subject="相続人導出の確認",
                      detail=detail)


def _group(run_id, record_id="70", extra_detail=None):
    return ReviewGroup(source="heir_derivation", idempotency_key="k",
                       items=[_item(run_id, record_id, extra_detail)])


class _Base(unittest.TestCase):
    """sqlite 実 DB＋kintone/通知 mock（test_p3_003b_projection と同じ土台）。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="p3c_")
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

        self.envelope = {"発送ステータス": {"value": "要確認"},
                         "実行済み": {"value": "no"}}
        self.app36_rows: dict[str, list] = {}
        self.created = []
        self.updated = []

        async def fake_get(app, rid):
            return dict(self.envelope)

        async def fake_search(app, query, fields=None):
            import re as _re
            m = _re.search(r'導出元人物ID = "([0-9]+)"', query)
            return list(self.app36_rows.get(m.group(1) if m else "", []))

        async def fake_create(app, fields):
            self.created.append((app.label, dict(fields)))
            rid = str(900 + len(self.created))
            if app.label == "App 36 (相続人)":
                self.app36_rows.setdefault(fields["導出元人物ID"], []).append({
                    "$id": {"value": rid},
                    "current_derivation_run_id":
                        {"value": fields.get("current_derivation_run_id", "")},
                    "戸籍確認済": {"value": fields.get("戸籍確認済", "no")},
                    "$revision": {"value": "1"}})
            return rid

        async def fake_update(app, rid, fields, revision=None):
            self.updated.append((app.label, str(rid), dict(fields)))

        self.get = AsyncMock(side_effect=fake_get)
        self.search = AsyncMock(side_effect=fake_search)
        self.create = AsyncMock(side_effect=fake_create)
        self.update = AsyncMock(side_effect=fake_update)
        self.alert = AsyncMock(return_value=True)
        for target, mock in [("get_record", self.get),
                             ("search_records", self.search),
                             ("create_record", self.create),
                             ("update_record", self.update)]:
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

    def _mk_run(self, payload=None, *, case="9", supersedes=None,
                status="derived", ih=None):
        pk = _run(create_derivation_run(
            case_app_id="26", case_record_id=case, decedent_person_id="10",
            at_date="2026-01-01", frozen_case_version="v0.1",
            input_person_revisions={}, input_person_ids=[],
            input_hash=ih or f"ih-{os.urandom(8).hex()}",
            status=status, rank=1,
            result_payload=payload or _payload(_heir("11", "child")),
            result_hash="rh" * 32, lawyer_flags=None, provisional=True,
            supersedes_run_id=supersedes, engine_version="e1"))
        db.reset_for_tests()
        return pk

    def _resolve(self, group, decided_by="ATT1", decision="confirmed"):
        r = _run(hp._resolve_heir_derivation(
            group, "9", decided_by=decided_by, decision=decision))
        db.reset_for_tests()
        return r

    def _decide(self, run_ids, decision, decided_by="ATT1", decided_at=None):
        r = _run(create_decisions_for_heads(
            "9", run_ids, decision=decision, decided_by=decided_by,
            decided_at=decided_at or datetime.now(timezone.utc)))
        db.reset_for_tests()
        return r

    def _decisions(self):
        async def _q():
            async with db.session_scope() as s:
                t = HeirConfirmationDecision.__table__
                return (await s.execute(sa.select(t).order_by(t.c.id))).all()
        rows = _run(_q())
        db.reset_for_tests()
        return rows

    def assert_no_app36_io(self):
        """App36（相続人）への search/create/update が一切ないこと。"""
        for call in self.search.await_args_list:
            self.assertNotEqual(call.args[0].label, "App 36 (相続人)")
        self.assertEqual(
            [c for c in self.created if c[0] == "App 36 (相続人)"], [])
        self.assertEqual(
            [c for c in self.updated if c[0] == "App 36 (相続人)"], [])


# ── §7-2/§7-13: 12 遷移閉集合（§3.2-v2 表と 1:1）─────────────────────────────
class TestDecisionChainTransitions(_Base):
    def test_no_leaf_creates_root_for_each_decision(self):
        for i, decision in enumerate(("confirmed", "held", "rejected")):
            with self.subTest(decision=decision):
                case = f"3{i}"                  # 各 subTest で独立 case（単独 root）
                rid = self._mk_run(case=case)
                res = _run(create_decisions_for_heads(
                    case, [rid], decision=decision, decided_by="ATT1",
                    decided_at=datetime.now(timezone.utc)))
                db.reset_for_tests()
                self.assertEqual(res[rid]["outcome"], "created")
                leaf = _run(get_leaf_decision(rid))
                db.reset_for_tests()
                self.assertEqual(leaf.decision, decision)

    def test_leaf_confirmed_transitions(self):
        rid = self._mk_run()
        self._decide([rid], "confirmed")
        # ×確定 = resumed（INSERT なし）
        res = self._decide([rid], "confirmed")
        self.assertEqual(res[rid]["outcome"], "resumed")
        self.assertEqual(len(self._decisions()), 1)
        # ×保留/否認 = already_confirmed 中止（INSERT なし）
        for decision in ("held", "rejected"):
            with self.subTest(decision=decision):
                with self.assertRaises(DecisionBlockedError) as cm:
                    self._decide([rid], decision)
                self.assertEqual(cm.exception.code, "already_confirmed")
        self.assertEqual(len(self._decisions()), 1)

    def test_leaf_held_transitions(self):
        # ×保留 = noop（decided_at は leaf 保存値）
        rid = self._mk_run()
        t0 = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
        self._decide([rid], "held", decided_at=t0)
        res = self._decide([rid], "held")
        self.assertEqual(res[rid]["outcome"], "noop")
        # sqlite は naive（UTC）で復元されるため UTC を付与して比較
        self.assertEqual(
            res[rid]["decided_at"].replace(tzinfo=timezone.utc), t0)
        self.assertEqual(len(self._decisions()), 1)
        # ×確定 = confirmed が supersede
        res = self._decide([rid], "confirmed")
        self.assertEqual(res[rid]["outcome"], "created")
        rows = self._decisions()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1].decision, "confirmed")
        self.assertEqual(rows[1].supersedes_decision_id, rows[0].id)
        # ×否認 = rejected が supersede（別 run で）
        rid2 = self._mk_run(case="8")
        _run(create_decisions_for_heads(
            "8", [rid2], decision="held", decided_by="ATT1",
            decided_at=datetime.now(timezone.utc)))
        db.reset_for_tests()
        res2 = _run(create_decisions_for_heads(
            "8", [rid2], decision="rejected", decided_by="ATT1",
            decided_at=datetime.now(timezone.utc)))
        db.reset_for_tests()
        self.assertEqual(res2[rid2]["outcome"], "created")
        leaf = _run(get_leaf_decision(rid2))
        db.reset_for_tests()
        self.assertEqual(leaf.decision, "rejected")

    def test_leaf_rejected_transitions(self):
        rid = self._mk_run()
        t0 = datetime(2026, 8, 2, 9, 0, 0, tzinfo=timezone.utc)
        self._decide([rid], "rejected", decided_at=t0)
        # ×確定/保留 = already_rejected 中止
        for decision in ("confirmed", "held"):
            with self.subTest(decision=decision):
                with self.assertRaises(DecisionBlockedError) as cm:
                    self._decide([rid], decision)
                self.assertEqual(cm.exception.code, "already_rejected")
        # ×否認（再）= noop・decision 追加なし・decided_at は leaf 保存値
        res = self._decide([rid], "rejected")
        self.assertEqual(res[rid]["outcome"], "noop")
        self.assertEqual(
            res[rid]["decided_at"].replace(tzinfo=timezone.utc), t0)
        self.assertEqual(len(self._decisions()), 1)

    def test_rejected_then_new_run_path(self):
        # §7-13: rejected 後の入力変更 → 新 run（supersede）→ 旧 run は stale・
        # 新 run へは confirmed root が入る
        rid1 = self._mk_run()
        self._decide([rid1], "rejected")
        rid2 = self._mk_run(supersedes=rid1)
        with self.assertRaises(ChainIntegrityError):
            self._decide([rid1], "confirmed")      # 旧 run = head でない（stale）
        res = self._decide([rid2], "confirmed")
        self.assertEqual(res[rid2]["outcome"], "created")


# ── §7-19: leaf 検索の 3 分類 fail-closed ─────────────────────────────────────
class TestLeafFailClosed(_Base):
    def test_zero_leaves_no_decisions_is_root_insert(self):
        rid = self._mk_run()
        self.assertIsNone(_run(get_leaf_decision(rid)))
        db.reset_for_tests()
        res = self._decide([rid], "held")
        self.assertEqual(res[rid]["outcome"], "created")

    def test_multiple_leaves_is_chain_corruption(self):
        # 一本鎖破損（leaf 複数・cross-run supersede 由来の状態）を leaf 検索の
        # 差し替えで模擬する（正規 module 外の core DML を作らない・検査対象は
        # 「複数 leaf を fail-closed で中止する判定」そのもの）
        from hub.derivation_models import DecisionChainCorruptionError
        rid = self._mk_run()
        self._decide([rid], "held")
        now = datetime.now(timezone.utc)
        fake = [SimpleNamespace(id=1, decision="held", decided_at=now),
                SimpleNamespace(id=2, decision="confirmed", decided_at=now)]
        with patch("hub.derivation_models._select_leaves",
                   new=AsyncMock(return_value=fake)):
            with self.assertRaises(DecisionChainCorruptionError) as cm:
                _run(get_leaf_decision(rid))
            db.reset_for_tests()
            # 破損は race と別型（fix1 M02）・件数と run_id のみを属性で保持
            self.assertEqual(cm.exception.count, 2)
            self.assertEqual(cm.exception.run_id, rid)
            with self.assertRaises(DecisionChainCorruptionError):
                _run(create_decisions_for_heads(
                    "9", [rid], decision="held", decided_by="ATT1",
                    decided_at=now))
            db.reset_for_tests()
        # 破損検出は write 0（行数不変=1）
        self.assertEqual(len(self._decisions()), 1)

    def test_corruption_via_handler_emits_business_alert(self):
        # fix1 M02: 破損検出は handler 経由で業務警報（固定分類・値非搭載）＋
        # aborted。App30/App36 write 0
        rid = self._mk_run()
        self._decide([rid], "held")
        now = datetime.now(timezone.utc)
        fake = [SimpleNamespace(id=1, decision="held", decided_at=now),
                SimpleNamespace(id=2, decision="confirmed", decided_at=now)]
        with patch("hub.derivation_models._select_leaves",
                   new=AsyncMock(return_value=fake)):
            r = self._resolve(_group(rid), decision="held")
        self.assertEqual(r["status"], "aborted")
        self.assertIn("破損検出", r["reason"])
        self.assertTrue(self.alert.await_count >= 1)
        alert_text = self.alert.await_args_list[0].args[0]
        self.assertIn("decision 鎖の破損検出", alert_text)
        self.assertIn(f"run #{rid}", alert_text)
        self.assertIn("有効 leaf 2 件", alert_text)
        self.assertEqual(self.updated, [])
        self.assert_no_app36_io()


# ── §7-18: 並行 supersede 競合の正規化＋DB 例外本文の非露出 ──────────────────
class TestIntegrityNormalization(_Base):
    def test_integrity_error_normalized_with_no_db_text(self):
        rid = self._mk_run()
        self._decide([rid], "held")

        from sqlalchemy.exc import IntegrityError

        async def stale_leaves(s, run_id):
            return []      # 並行 race の後着を模擬（leaf を見ずに root INSERT へ）

        with patch("hub.derivation_models._select_leaves", new=stale_leaves):
            with self.assertRaises(ChainIntegrityError) as cm:
                _run(create_decisions_for_heads(
                    "9", [rid], decision="held", decided_by="ATT1",
                    decided_at=datetime.now(timezone.utc)))
        db.reset_for_tests()
        exc = cm.exception
        self.assertNotIsInstance(exc, DecisionBlockedError)
        # 固定文言のみ・DB 例外本文/constraint 名の非露出（sentinel 方式）
        self.assertIn("並行実行と競合", str(exc))
        for banned in ("UNIQUE constraint", "uq_heir", "sqlite3", "SQL:"):
            self.assertNotIn(banned, str(exc))
        # except 外 raise（CMD 裁定9 の構造・連鎖に DB 例外を残さない）
        self.assertIsNone(exc.__context__)
        self.assertIsNone(exc.__cause__)
        # 全体 rollback（decision 追加なし）
        self.assertEqual(len(self._decisions()), 1)

    def test_handler_maps_normalized_error_to_aborted(self):
        rid = self._mk_run()
        with patch("hub.heir_projection.create_decisions_for_heads",
                   side_effect=ChainIntegrityError("固定")):
            r = self._resolve(_group(rid), decision="held")
        self.assertEqual(r["status"], "aborted")
        self.assertNotIn("固有", r["reason"])   # 固定応答（既存文言へ合流）
        self.assert_no_app36_io()
        # fix1 M02: 正常系の並行 race（正規化 ChainIntegrityError）では
        # 業務警報を出さない（破損のみ警報＝区別を pin）
        self.alert.assert_not_awaited()


# ── §7-3/§7-4/§7-14: App36 無照会（異常行存在下でも）＋ rejected leaf 遮断 ──
class TestApp36ZeroAccess(_Base):
    def _poison_app36(self):
        # App36 に冪等キー重複行・current 不正行が存在する状態
        self.app36_rows["11"] = [
            {"$id": {"value": "901"},
             "current_derivation_run_id": {"value": "broken"},
             "戸籍確認済": {"value": "no"}, "$revision": {"value": "1"}},
            {"$id": {"value": "902"},
             "current_derivation_run_id": {"value": ""},
             "戸籍確認済": {"value": "no"}, "$revision": {"value": "1"}},
        ]

    def test_held_rejected_never_touch_app36(self):
        for decision in ("held", "rejected"):
            with self.subTest(decision=decision):
                rid = self._mk_run(case="9" if decision == "held" else "8")
                case = "9" if decision == "held" else "8"
                self._poison_app36()
                g = _group(rid)
                r = _run(hp._resolve_heir_derivation(
                    g, case, decided_by="ATT1", decision=decision))
                db.reset_for_tests()
                self.assertEqual(r["status"], "resolved")
                self.assertEqual(r["decision"], decision)
                self.assert_no_app36_io()
                # App30 封筒 update は行われる（decision 記録の side effect）
                self.assertTrue(
                    [c for c in self.updated if c[0] == hp.APP_SHIPPING.label])

    def test_confirm_on_rejected_leaf_aborts_with_zero_app36(self):
        rid = self._mk_run()
        self._decide([rid], "rejected")
        self.updated.clear()
        r = self._resolve(_group(rid), decision="confirmed")
        self.assertEqual(r["status"], "aborted")
        self.assertIn("否認済み", r["reason"])
        self.assert_no_app36_io()
        self.assertEqual(self.updated, [])      # App30 も write 0（全体中止）


# ── §7-5/§7-20: 封筒 side effect（単一 update・既存キー保持）─────────────────
class TestEnvelopeSideEffects(_Base):
    def test_held_single_update_open_kept_and_annotation(self):
        rid = self._mk_run()
        r = self._resolve(_group(rid, extra_detail={"独自キー": "v"}),
                          decision="held")
        self.assertEqual(r["status"], "resolved")
        shipping = [c for c in self.updated if c[0] == hp.APP_SHIPPING.label]
        self.assertEqual(len(shipping), 1)      # 単一 update（§12 M01）
        _label, _rid, fields = shipping[0]
        # held は封筒 open 維持（クローズ系 field を含めない）
        self.assertNotIn("発送ステータス", fields)
        self.assertNotIn("実行済み", fields)
        detail = json.loads(fields["チャネル固有データ"])["heir_derivation"]
        self.assertEqual(detail[DETAIL_DECISION_KEY]["decision"], "held")
        self.assertTrue(detail[DETAIL_DECISION_KEY]["decided_at"])
        # 既存キー全保持（§7-20）
        for key in ("derivation_run_id", "case_record_id", "冪等キー", "独自キー"):
            self.assertIn(key, detail)
        # decided_by（LINE user ID）は注記に載せない
        self.assertNotIn("ATT1", fields["チャネル固有データ"])

    def test_rejected_single_update_closes_with_full_fields(self):
        rid = self._mk_run()
        r = self._resolve(_group(rid), decision="rejected")
        self.assertEqual(r["status"], "resolved")
        shipping = [c for c in self.updated if c[0] == hp.APP_SHIPPING.label]
        self.assertEqual(len(shipping), 1)      # クローズ＋注記も 1 呼出しに一括
        _label, _rid, fields = shipping[0]
        self.assertEqual(fields["発送ステータス"], "完了")
        self.assertEqual(fields["実行済み"], "yes")
        detail = json.loads(fields["チャネル固有データ"])["heir_derivation"]
        self.assertEqual(detail[DETAIL_DECISION_KEY]["decision"], "rejected")


# ── fix1 H01: detail 再構築の基底=App30 再読の現在値（3 経路の対照）─────────
class TestLiveDetailMerge(_Base):
    """pending 作成後に App30 detail へキーが追加された状況（再読が追加キー入りを
    返す）で、当該 decision の書くキーのみが上書きされ他キーが保持されることを
    3 経路で pin（§7-20 の実 App30 状態基準への強化）。"""

    def _set_live_detail(self, detail: dict):
        self.envelope["チャネル固有データ"] = {"value": json.dumps(
            {"heir_derivation": detail}, ensure_ascii=False)}

    def _last_shipping_detail(self):
        fields = [c for c in self.updated
                  if c[0] == hp.APP_SHIPPING.label][-1][2]
        return json.loads(fields["チャネル固有データ"])["heir_derivation"]

    def test_held_rejected_preserve_keys_added_after_pending(self):
        for decision in ("held", "rejected"):
            with self.subTest(decision=decision):
                case = "9" if decision == "held" else "8"
                rid = self._mk_run(case=case)
                # snapshot（pending 時）には無いキーが App30 側に後追いで存在
                self._set_live_detail({"derivation_run_id": rid,
                                       "case_record_id": case, "冪等キー": "k",
                                       "後追いキー": "added-after-pending"})
                g = _group(rid)     # item.detail（snapshot）は後追いキーを持たない
                r = _run(hp._resolve_heir_derivation(
                    g, case, decided_by="ATT1", decision=decision))
                db.reset_for_tests()
                self.assertEqual(r["status"], "resolved")
                detail = self._last_shipping_detail()
                self.assertEqual(detail["後追いキー"], "added-after-pending")
                self.assertEqual(detail[DETAIL_DECISION_KEY]["decision"],
                                 decision)
                self.assertEqual(detail["冪等キー"], "k")

    def test_confirmed_annotation_update_preserves_live_keys(self):
        # (b) confirmed 注記更新: live detail に 判断=held＋後追いキー。
        # snapshot は後追いキーを持たない → クローズ update で両方保持・注記のみ
        # confirmed へ更新
        rid = self._mk_run()
        self._decide([rid], "held")
        live = {"derivation_run_id": rid, "case_record_id": "9", "冪等キー": "k",
                DETAIL_DECISION_KEY: {"decision": "held", "decided_at": "t"},
                "後追いキー": "x"}
        self._set_live_detail(live)
        r = self._resolve(_group(rid))          # 確定（held を supersede）
        self.assertEqual(r["status"], "resolved")
        self.assertTrue(r["items"][0]["envelope_closed"])
        detail = self._last_shipping_detail()
        self.assertEqual(detail["後追いキー"], "x")
        self.assertEqual(detail[DETAIL_DECISION_KEY]["decision"], "confirmed")

    def test_rowheld_append_preserves_live_keys(self):
        # (c) row-held 追記: 直前再検証で行 held → 保留人物ID 追記 update が
        # live detail の後追いキーを保持
        rid = self._mk_run(_payload(_heir("11", "child", "1/2")))
        self._set_live_detail({"derivation_run_id": rid, "case_record_id": "9",
                               "冪等キー": "k", "後追いキー": "y"})
        calls = {"n": 0}

        async def racy_search(app, query, fields=None):
            calls["n"] += 1
            if calls["n"] > 1:      # phase 1 は 0 件・_project_row 再検索で重複出現
                return [{"$id": {"value": "955"},
                         "current_derivation_run_id": {"value": "99999"},
                         "戸籍確認済": {"value": "no"},
                         "$revision": {"value": "1"}},
                        {"$id": {"value": "956"},
                         "current_derivation_run_id": {"value": "99999"},
                         "戸籍確認済": {"value": "no"},
                         "$revision": {"value": "1"}}]
            return []

        self.search.side_effect = racy_search
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(r["items"][0]["app36_held"], 1)
        self.assertFalse(r["items"][0]["envelope_closed"])
        detail = self._last_shipping_detail()
        self.assertEqual(detail["後追いキー"], "y")
        self.assertEqual(detail[DETAIL_HELD_PERSONS_KEY], ["11"])


# ── §7-17: side effect の再開（App30 失敗→再指示・時刻不変）──────────────────
class TestSideEffectResume(_Base):
    def test_app30_failure_then_reissue_reapplies_idempotently(self):
        for decision in ("held", "rejected"):
            with self.subTest(decision=decision):
                case = "9" if decision == "held" else "8"
                rid = self._mk_run(case=case)
                g = _group(rid)
                self.update.side_effect = RuntimeError("kintone down")
                with self.assertRaises(RuntimeError):
                    _run(hp._resolve_heir_derivation(
                        g, case, decided_by="ATT1", decision=decision))
                db.reset_for_tests()
                n_decisions = len([d for d in self._decisions()
                                   if d.derivation_run_id == rid])
                self.assertEqual(n_decisions, 1)   # decision は commit 済み
                first_fields = None
                self.updated.clear()

                async def ok_update(app, rid_, fields, revision=None):
                    self.updated.append((app.label, str(rid_), dict(fields)))
                self.update.side_effect = ok_update
                # 同一指示の再発行 → decision 追加なし・side effect 冪等再適用
                r = _run(hp._resolve_heir_derivation(
                    g, case, decided_by="ATT1", decision=decision))
                db.reset_for_tests()
                self.assertEqual(r["status"], "resolved")
                self.assertEqual(r["items"][0]["decision_outcome"], "noop")
                self.assertEqual(
                    len([d for d in self._decisions()
                         if d.derivation_run_id == rid]), 1)
                shipping = [c for c in self.updated
                            if c[0] == hp.APP_SHIPPING.label]
                self.assertEqual(len(shipping), 1)   # 単一 call
                fields = shipping[0][2]
                detail = json.loads(
                    fields["チャネル固有データ"])["heir_derivation"]
                note = detail[DETAIL_DECISION_KEY]
                self.assertEqual(note["decision"], decision)
                # decided_at = leaf の保存値（§12 M01: 再適用で時刻を上書きしない・
                # 注記は UTC 正準化 ISO）
                leaf = _run(get_leaf_decision(rid))
                db.reset_for_tests()
                self.assertEqual(
                    note["decided_at"],
                    leaf.decided_at.replace(tzinfo=timezone.utc).isoformat())
                # 完全 field 集合（rejected はクローズ込み・held は注記のみ）
                if decision == "rejected":
                    self.assertEqual(
                        set(fields),
                        {"チャネル固有データ", "発送ステータス", "実行済み"})
                else:
                    self.assertEqual(set(fields), {"チャネル固有データ"})
                first_fields = fields
                # さらに再指示 → 同値の再適用（内容が不変＝冪等）
                self.updated.clear()
                r2 = _run(hp._resolve_heir_derivation(
                    g, case, decided_by="ATT1", decision=decision))
                db.reset_for_tests()
                self.assertEqual(r2["status"], "resolved")
                shipping2 = [c for c in self.updated
                             if c[0] == hp.APP_SHIPPING.label]
                self.assertEqual(shipping2[0][2], first_fields)


# ── §7-10: rejected→rejected（クローズ後は gate 固定応答・decision 追加なし）──
class TestRejectedRejected(_Base):
    def test_after_close_reissue_hits_gate_with_no_new_decision(self):
        rid = self._mk_run()
        r = self._resolve(_group(rid), decision="rejected")
        self.assertEqual(r["status"], "resolved")
        # クローズ済み封筒を模擬（§4.1(c): open でなければ従来どおり固定応答）
        self.envelope = {"発送ステータス": {"value": "完了"},
                         "実行済み": {"value": "yes"}}
        self.updated.clear()
        r2 = self._resolve(_group(rid), decision="rejected")
        self.assertEqual(r2["status"], "aborted")
        self.assertIn("要確認ではなくなっています", r2["reason"])
        self.assertEqual(len(self._decisions()), 1)   # decision 追加なし
        self.assertEqual(self.updated, [])


# ── §7-9/§7-11/§7-15: held→confirmed 全連鎖・resume 3 分岐・注記終端 ─────────
class TestHeldConfirmChainAndResume(_Base):
    def test_full_chain_held_confirm_rowheld_reconfirm(self):
        rid = self._mk_run(_payload(_heir("11", "child", "1/2"),
                                    _heir("12", "child", "1/2")))
        g = _group(rid)
        # 1) 保留
        r = self._resolve(g, decision="held")
        self.assertEqual(r["status"], "resolved")
        held_detail = json.loads(
            self.updated[-1][2]["チャネル固有データ"])["heir_derivation"]
        # 2) 確定（held を supersede）——person 12 の行だけ直前再検証で
        #    行状態変化 → row-held（phase1 は 0 件・_project_row は別系列行を返す）
        item = ReviewItem(record_id="70", subject="相続人導出の確認",
                          detail=held_detail)
        g2 = ReviewGroup(source="heir_derivation", idempotency_key="k",
                         items=[item])
        phase1_done = {"n": 0}
        orig_rows = {"12": [{"$id": {"value": "955"},
                             "current_derivation_run_id": {"value": "99999"},
                             "戸籍確認済": {"value": "no"},
                             "$revision": {"value": "1"}}]}

        async def racy_search(app, query, fields=None):
            import re as _re
            m = _re.search(r'導出元人物ID = "([0-9]+)"', query)
            pid = m.group(1) if m else ""
            phase1_done["n"] += 1
            if pid == "12" and phase1_done["n"] > 2:
                return list(orig_rows["12"])      # 直前再検証でのみ出現
            return list(self.app36_rows.get(pid, []))

        self.search.side_effect = racy_search
        r2 = _run(hp._resolve_heir_derivation(
            g2, "9", decided_by="ATT1", decision="confirmed"))
        db.reset_for_tests()
        self.assertEqual(r2["status"], "resolved")
        self.assertEqual(r2["items"][0]["app36_inserted"], 1)
        self.assertEqual(r2["items"][0]["app36_held"], 1)
        self.assertFalse(r2["items"][0]["envelope_closed"])
        rows = self._decisions()
        self.assertEqual([d.decision for d in rows], ["held", "confirmed"])
        self.assertEqual(rows[1].supersedes_decision_id, rows[0].id)
        # 封筒 open 維持＋判断注記は confirmed へ更新（除去しない・M03）＋
        # 保留人物ID と別キー併存（§7-15）
        fields = self.updated[-1][2]
        self.assertNotIn("発送ステータス", fields)
        detail = json.loads(fields["チャネル固有データ"])["heir_derivation"]
        self.assertEqual(detail[DETAIL_DECISION_KEY]["decision"], "confirmed")
        self.assertEqual(detail[DETAIL_HELD_PERSONS_KEY], ["12"])
        confirmed_at = detail[DETAIL_DECISION_KEY]["decided_at"]
        # 3) 収束（異常行が消えた）→ 同じ封筒を再確定 = resume（decision 追加なし）
        self.search.side_effect = None

        async def clean_search(app, query, fields=None):
            import re as _re
            m = _re.search(r'導出元人物ID = "([0-9]+)"', query)
            return list(self.app36_rows.get(m.group(1) if m else "", []))
        self.search.side_effect = clean_search
        item3 = ReviewItem(record_id="70", subject="相続人導出の確認",
                           detail=detail)
        g3 = ReviewGroup(source="heir_derivation", idempotency_key="k",
                         items=[item3])
        r3 = _run(hp._resolve_heir_derivation(
            g3, "9", decided_by="ATT1", decision="confirmed"))
        db.reset_for_tests()
        self.assertEqual(r3["status"], "resolved")
        self.assertTrue(r3["items"][0]["envelope_closed"])
        self.assertEqual(len(self._decisions()), 2)     # resume＝追加なし
        # クローズ update に注記が confirmed のまま残り decided_at 不変（終端一致）
        close_fields = self.updated[-1][2]
        self.assertEqual(close_fields["発送ステータス"], "完了")
        detail3 = json.loads(
            close_fields["チャネル固有データ"])["heir_derivation"]
        self.assertEqual(detail3[DETAIL_DECISION_KEY]["decision"], "confirmed")
        self.assertEqual(detail3[DETAIL_DECISION_KEY]["decided_at"],
                         confirmed_at)

    def test_confirmed_confirmed_three_branches(self):
        # (a) open+head = resume（decision 追加なし・projection 再実行）
        rid = self._mk_run()
        self._resolve(_group(rid))
        self.assertEqual(len(self._decisions()), 1)
        r = self._resolve(_group(rid))
        self.assertEqual(r["status"], "resolved")
        self.assertEqual(len(self._decisions()), 1)
        # (b) 封筒クローズ済み → 封筒再読 aborted
        self.envelope = {"発送ステータス": {"value": "完了"},
                         "実行済み": {"value": "yes"}}
        r2 = self._resolve(_group(rid))
        self.assertEqual(r2["status"], "aborted")
        self.assertIn("要確認ではなくなっています", r2["reason"])
        # (c) head でない → stale aborted
        self.envelope = {"発送ステータス": {"value": "要確認"},
                         "実行済み": {"value": "no"}}
        self._mk_run(supersedes=rid)
        r3 = self._resolve(_group(rid))
        self.assertEqual(r3["status"], "aborted")
        self.assertIn("最新ではありません", r3["reason"])


# ── §7-7/§7-16: allowlist 3 値対称拒否 ───────────────────────────────────────
class TestAllowlistSymmetric(_Base):
    def test_allowlist_rejects_all_three_decisions(self):
        rid = self._mk_run()
        for decision in ("confirmed", "held", "rejected"):
            with self.subTest(decision=decision):
                r = self._resolve(_group(rid), decided_by="OUTSIDER",
                                  decision=decision)
                self.assertEqual(r["status"], "aborted")
                self.assertIn("ATTORNEY_ALLOWLIST", r["reason"])
        self.assertEqual(self._decisions(), [])
        self.assertEqual(self.updated, [])
        self.assert_no_app36_io()

    def test_unknown_decision_aborts(self):
        rid = self._mk_run()
        r = self._resolve(_group(rid), decision="invalid")
        self.assertEqual(r["status"], "aborted")
        self.assertEqual(self._decisions(), [])


# ── §7-8: decision 語彙 × 非 heir グループ = unsupported 明示応答 ────────────
class TestDecisionUnsupportedSource(_Base):
    def test_resolve_group_rejects_decision_for_non_heir_source(self):
        group = ReviewGroup(source="koseki_ingest", idempotency_key="k",
                            items=[ReviewItem(record_id="70", subject="s",
                                              detail={})])
        env = {"APP_SHIPPING": "30", "TOKEN_SHIPPING": "t30",
               "APP_KOSEKI_BOOK": "33", "TOKEN_KOSEKI_BOOK": "t33"}
        for decision in ("held", "rejected"):
            with self.subTest(decision=decision):
                with patch.dict(os.environ, env):
                    r = _run(resolve_group(group, "9", decided_by="ATT1",
                                           decision=decision))
                self.assertEqual(r["status"], "unsupported")
                self.assertEqual(r["reason"], MSG_DECISION_UNSUPPORTED)
        self.assertEqual(self.updated, [])      # 黙って確定に倒さない・write 0


# ── §7-6: 語彙の閉集合判定・曖昧聞き返し・復唱文言 ───────────────────────────
class TestVocabularyAndReadback(unittest.TestCase):
    def test_detect_decision_closed_sets(self):
        from dispatch_bot.review_resolve_task import _detect_decision
        for text, expected in [
                ("要確認を確定して", "confirmed"),
                ("要確認を処理して", "confirmed"),
                ("要確認を保留して", "held"),
                ("要確認をペンディングにして", "held"),
                ("要確認を否認して", "rejected"),
                ("要確認を差し戻して", "rejected"),
                ("要確認を差戻しして", "rejected"),
                ("要確認を却下して", "rejected")]:
            with self.subTest(text=text):
                decision, msg = _detect_decision(text)
                self.assertEqual(decision, expected)
                self.assertEqual(msg, "")

    def test_ambiguous_asks_back_not_confirmed(self):
        from dispatch_bot.review_resolve_task import (_detect_decision,
                                                      MSG_DECISION_AMBIGUOUS)
        decision, msg = _detect_decision("要確認を保留か否認にして")
        self.assertIsNone(decision)              # confirmed に倒さない（§2.1）
        self.assertEqual(msg, MSG_DECISION_AMBIGUOUS)

    def test_readback_mentions_decision_and_consequence(self):
        # 復唱に decision 種別と帰結（rejected は不可逆性）が含まれること（§2.2）
        from dispatch_bot import review_resolve_task as rt
        from customer_directory import Candidate
        cand = Candidate(record_id="9", app_id="26",
                         source="相談カード (相続)", customer_name="山田",
                         status="進行中")
        group = _group(1)
        created = []
        with patch("dispatch_bot.confirm.create",
                   side_effect=lambda *a, **k: created.append(a)), \
             patch.dict("sys.modules"), \
             patch("dispatch_bot.handler._sessions", new={}):
            for decision, must_contain in [
                    ("held", ("保留", "要確認のまま残ります")),
                    ("rejected", ("否認", "クローズ", "再導出"))]:
                with self.subTest(decision=decision):
                    parsed = {"task_params": {"decision": decision}}
                    msg = _run(rt._confirm("U1", parsed, "t", group, cand))
                    for frag in must_contain:
                        self.assertIn(frag, msg)
                    self.assertIn("OK / キャンセル", msg)
        # pending へ decision が固定されている（execute への伝搬材料）
        self.assertEqual(
            [a[1]["task_params"]["decision"] for a in created],
            ["held", "rejected"])


# ── §7-12: rejected 後・同一 input 再導出の全面 no-op 明示 ───────────────────
class TestDeriveNoChangeAfterRejected(_Base):
    def test_no_change_response_mentions_rejected(self):
        rid = self._mk_run(ih="X" * 64)
        self._decide([rid], "rejected")
        from dispatch_bot import heir_derive_task as ht
        state = {"run": "failed:unexpected", "env": "skipped",
                 "run_id": None, "env_no": None}
        head = SimpleNamespace(
            id=rid, case_app_id="26", case_record_id="9",
            input_hash="X" * 64, result_hash="rh" * 32,
            status="derived", provisional=True, lawyer_flags=None)
        with patch.object(ht, "compute_input_hash", return_value="X" * 64), \
             patch.object(ht, "get_current_head",
                          new=AsyncMock(return_value=head)), \
             patch.object(ht, "persons_from_records") as pfr, \
             patch.object(ht, "file_heir_envelope",
                          new=AsyncMock(return_value={
                              "status": "already_filed", "record_id": "70"})), \
             patch.object(ht.kintone, "search_records",
                          new=AsyncMock(return_value=[
                              {"$id": {"value": "10"},
                               "$revision": {"value": "3"},
                               "案件レコードID": {"value": "9"}}])):
            pfr.return_value = [SimpleNamespace(
                record_id="10", is_decedent=True, death_date="2026-01-01")]
            msg = _run(ht._pipeline(state, "26", "9"))
        db.reset_for_tests()
        self.assertEqual(state["run"], "no_change")
        self.assertIn("入力に変化はありません", msg)
        self.assertIn("否認済み", msg)                    # §5 の明示
        self.assertIn("入力を修正してから再導出", msg)
        self.assertEqual(len(self._decisions()), 1)      # 全面 no-op

    def test_no_change_without_rejection_has_no_mention(self):
        rid = self._mk_run(ih="Y" * 64)
        from dispatch_bot import heir_derive_task as ht
        state = {"run": "failed:unexpected", "env": "skipped",
                 "run_id": None, "env_no": None}
        head = SimpleNamespace(
            id=rid, case_app_id="26", case_record_id="9",
            input_hash="Y" * 64, result_hash="rh" * 32,
            status="derived", provisional=True, lawyer_flags=None)
        with patch.object(ht, "compute_input_hash", return_value="Y" * 64), \
             patch.object(ht, "get_current_head",
                          new=AsyncMock(return_value=head)), \
             patch.object(ht, "persons_from_records") as pfr, \
             patch.object(ht, "file_heir_envelope",
                          new=AsyncMock(return_value={
                              "status": "already_filed", "record_id": "70"})), \
             patch.object(ht.kintone, "search_records",
                          new=AsyncMock(return_value=[
                              {"$id": {"value": "10"},
                               "$revision": {"value": "3"},
                               "案件レコードID": {"value": "9"}}])):
            pfr.return_value = [SimpleNamespace(
                record_id="10", is_decedent=True, death_date="2026-01-01")]
            msg = _run(ht._pipeline(state, "26", "9"))
        db.reset_for_tests()
        self.assertNotIn("否認済み", msg)


# ── 発注 3: 分岐位置の構造保証（ソース検査）──────────────────────────────────
class TestBranchStructureSource(unittest.TestCase):
    @staticmethod
    def _parse_fn():
        import ast
        from pathlib import Path
        tree = ast.parse(
            Path("hub/heir_projection.py").read_text(encoding="utf-8"))
        fn = next(n for n in tree.body
                  if isinstance(n, ast.AsyncFunctionDef)
                  and n.name == "_resolve_heir_derivation")
        return tree, fn

    def test_decision_branch_precedes_app36_row_plan(self):
        """fix1 M01: **分岐実体**（`decision != "confirmed"` を test に持ち body に
        Continue を含む If＝phase 1 ループ内の早期分岐）を特定し、その行が
        App36 検索（search_records）の最初の呼出しより構文上前にあることを pin。

        - 冒頭の閉集合検査（`decision not in DECISIONS`＝NotIn）や leaf 先行検査の
          比較（Return のみ・Continue なし）は **NotEq＋Continue の複合条件で除外**
          される＝最初に現れる比較を拾う旧方式（比較位置だけで真になり得る）を撤回。
        - 変異確認（報告事項）: 分岐実体を App36 検索の後へ移すと branch_line >
          search_line となり本テストは FAIL する（continue が search 後では
          「App36 照会ゼロ」が破れる改変を検出できる）。
        """
        import ast
        _tree, fn = self._parse_fn()
        early_branches = []          # (lineno) — NotEq('confirmed') かつ Continue
        search_line = None
        for node in ast.walk(fn):
            if isinstance(node, ast.If):
                has_ne_confirmed = any(
                    isinstance(c, ast.Compare)
                    and isinstance(c.left, ast.Name) and c.left.id == "decision"
                    and len(c.ops) == 1 and isinstance(c.ops[0], ast.NotEq)
                    and len(c.comparators) == 1
                    and isinstance(c.comparators[0], ast.Constant)
                    and c.comparators[0].value == "confirmed"
                    for c in ast.walk(node.test))
                has_continue = any(isinstance(b, ast.Continue)
                                   for b in ast.walk(node))
                if has_ne_confirmed and has_continue:
                    early_branches.append(node.lineno)
            if (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "search_records"
                    and search_line is None):
                search_line = node.lineno
        # 分岐実体は正確に 1 箇所（曖昧一致を許さない）
        self.assertEqual(len(early_branches), 1)
        self.assertIsNotNone(search_line)
        self.assertLess(early_branches[0], search_line)

    def test_side_effect_helper_has_no_app36_reference(self):
        import ast
        tree, _fn = self._parse_fn()
        helper = next(n for n in tree.body
                      if isinstance(n, ast.FunctionDef)
                      and n.name == "_decision_side_effect_fields")
        for node in ast.walk(helper):
            if isinstance(node, ast.Name):
                self.assertNotEqual(node.id, "APP_SOUZOKUNIN")
            if isinstance(node, ast.Attribute):
                self.assertNotIn(node.attr, ("search_records", "create_record",
                                             "update_record"))


if __name__ == "__main__":
    unittest.main()
