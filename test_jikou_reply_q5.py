"""JIKOU-REPLY-Q5（QUEUE-2026-10-13-D D-4）: 凍結定型の候補 → 大野の昇格 → データ表の定型として選択器が使う

固定する仕様:
- 候補の条件: active な手本・分類あり・その category の無修正承認回数が 3 以上・下書きのみの分類ではない・未昇格。
- 昇格: jikou_reply_text に J-U1 から連番で追加（匿名化済み本文・text_version）・履歴 promoted にデータ表の版（sha256）。
  コードの定型表（hub/jikou_reply_texts.TABLE_SHA256_PIN）は不変。
- 選択器への反映: V2 ON で Q2 の表に該当が無い問いは、compose の category に対応する active な昇格定型（最新 1 件）を
  そのまま送る。退役は選ばない。履歴に同文があれば送らない。下書きのみの分類は選ばない。
- 退役／再承認は /app/exemplars の同画面（履歴 retired / reactivated・版を記録）。
- migration e6a8c0d2f4b6（d4f6 の後ろ）。pin: 新 module の sink 違反ゼロ・allowlist 追加ゼロ・PII なし。
"""

import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import USER, _auth, _client, _compose, _run, _texts  # noqa: E402
from test_jikou_reply_q3b import CAT, _Mixin, _exemplar  # noqa: E402

from hub import db  # noqa: E402
from hub import jikou_reply_promote as pm  # noqa: E402
from hub import jikou_reply_texts as T  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
REPLY = "お手続きの期間は、ご依頼から 2〜4 週間程度が目安です。"


class _PromoteMixin(_Mixin):
    def setUp(self):
        super().setUp()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(pm.metadata.create_all)
        _run(_create())
        self.env({"WEBAPP_PASSWORD_HASH": __import__("os").environ["WEBAPP_PASSWORD_HASH"]})

    _seq = 0

    def approvals(self, n: int, category: str = CAT):
        """無修正承認 n 回（Q3a の記録 API を直接・record_id は通算連番）。"""
        for _ in range(n):
            type(self)._seq += 1
            rid = f"draft:{category}:{type(self)._seq}"
            _run(rx.record_draft(rid, "jikou", category, f"t{rid}"))
            _run(rx.record_sent(rid, f"t{rid}"))

    def texts(self) -> list:
        return self._rows(pm.jikou_reply_text)


class TestCandidateAndPromote(_PromoteMixin):
    def test_candidate_conditions(self):
        xid = _exemplar(reply=REPLY)
        ex = _run(rx.get_exemplar(xid))
        self.assertEqual(_run(pm.candidate_categories()), {})
        self.assertFalse(pm.is_candidate(ex, {}, set()))
        self.approvals(2)
        self.assertEqual(_run(pm.candidate_categories()), {})
        self.approvals(1)                                       # 3 回目
        self.assertEqual(_run(pm.candidate_categories()), {CAT: 3})
        self.assertTrue(pm.is_candidate(ex, {CAT: 3}, set()))
        self.assertFalse(pm.is_candidate(ex, {CAT: 3}, {xid}))                   # 昇格済み
        self.assertFalse(pm.is_candidate(dict(ex, active=False), {CAT: 3}, set()))
        self.assertFalse(pm.is_candidate(dict(ex, category_id=None), {CAT: 3}, set()))
        # 下書きのみの分類は閾値に達しても候補にしない
        self.approvals(3, category="法的判断・見通し")
        self.assertNotIn("法的判断・見通し", _run(pm.candidate_categories()))
        self.assertIsNone(_run(pm.promote(xid + 100)))

    def test_promote_assigns_sequential_ids_and_records_version(self):
        x1 = _exemplar(reply=REPLY)
        x2 = _exemplar(reply="二つ目の手本の返信です。")
        self.assertIsNone(_run(pm.promote(x1)))                 # 閾値未達
        self.approvals(3)
        t1 = _run(pm.promote(x1))
        self.assertEqual(t1, "J-U1")
        self.assertIsNone(_run(pm.promote(x1)))                 # 同じ手本は 2 度昇格しない
        t2 = _run(pm.promote(x2))
        self.assertEqual(t2, "J-U2")
        rows = self.texts()
        self.assertEqual([r["text_id"] for r in rows], ["J-U1", "J-U2"])
        self.assertEqual(rows[0]["text"], REPLY)
        self.assertEqual(rows[0]["category"], CAT)
        self.assertEqual(rows[0]["source_exemplar_id"], x1)
        h = _run(pm.history("J-U1"))
        self.assertEqual([x["action"] for x in h], [pm.ACTION_PROMOTED])
        self.assertEqual(len(h[0]["table_version"]), 64)
        self.assertEqual(_run(pm.table_version()), _run(pm.history("J-U2"))[0]["table_version"])
        self.assertTrue(T.verify_pin())                         # コードの pin は不変
        self.assertEqual(_run(pm.category_map()), {CAT: ["J-U2", "J-U1"]})
        self.assertEqual(_run(pm.pick_for_category(CAT))["text_id"], "J-U2")
        self.assertIsNone(_run(pm.pick_for_category("費用の定型案内")))
        self.assertIsNone(_run(pm.pick_for_category("法的判断・見通し")))
        self.assertEqual(pm.next_id([]), "J-U1")
        self.assertEqual(pm.next_id(["J-U7", "J-U12", "x"]), "J-U13")

    def test_retire_and_reactivate_with_history(self):
        x1 = _exemplar(reply=REPLY)
        self.approvals(3)
        _run(pm.promote(x1))
        v0 = _run(pm.table_version())
        self.assertTrue(_run(pm.retire("J-U1")))
        self.assertFalse(_run(pm.retire("J-U1")))
        self.assertFalse(_run(pm.retire("bogus")))
        self.assertEqual(_run(pm.category_map()), {})
        self.assertIsNone(_run(pm.pick_for_category(CAT)))
        self.assertNotEqual(_run(pm.table_version()), v0)
        self.assertEqual(_run(pm.table_version()), pm.compute_version([]))
        self.assertTrue(_run(pm.reactivate("J-U1")))
        self.assertEqual(_run(pm.table_version()), v0)
        self.assertEqual([x["action"] for x in _run(pm.history("J-U1"))],
                         [pm.ACTION_PROMOTED, pm.ACTION_RETIRED, pm.ACTION_REACTIVATED])
        self.assertEqual(len(_run(pm.list_texts(include_retired=False))), 1)


class TestSelectorIntegration(_PromoteMixin):
    def test_v2_on_sends_promoted_text_when_q2_has_no_match(self):
        x1 = _exemplar(reply=REPLY)
        self.approvals(3)
        _run(pm.promote(x1))
        self.on()
        self.intent.return_value = {"intent": "その他", "creditor_name": ""}
        self.customer(_compose("自由文", category=CAT, auto_send=True))
        self.assertEqual(_texts(), [REPLY])
        self.assertFalse(self.approval.called)
        self.assertEqual(self.drafts(), [])
        self.assertEqual(self.chatlog.call_args_list[-1].args[1:3], ("assistant", REPLY))
        # 退役 → 選ばれない（承認へ降格）
        _run(pm.retire("J-U1"))
        self.customer(_compose("自由文", category=CAT, auto_send=True))
        self.assertEqual(_texts(), [REPLY])
        self.assertTrue(self.approval.called)

    def test_promoted_text_not_resent_and_draft_only_category_not_used(self):
        import chat_responder as cr
        x1 = _exemplar(reply=REPLY)
        self.approvals(3)
        _run(pm.promote(x1))
        self.on()
        hist = cr.ChatHistory([{"role": "user", "content": "q"}, {"role": "assistant", "content": REPLY}])
        with patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=hist)):
            self.customer(_compose("自由文", category=CAT, auto_send=True))
        self.assertEqual(_texts(), [])
        self.assertTrue(self.approval.called)
        # Q2 の表で該当がある問いは Q2 が優先（昇格定型は使わない）
        self.approval.reset_mock()
        self.intent.return_value = {"intent": "費用", "creditor_name": ""}
        self.customer(_compose("自由文", category=CAT, auto_send=True))
        self.assertEqual(_texts(), [T.text("J-7")])

    def test_v2_off_ignores_promoted_texts(self):
        x1 = _exemplar(reply=REPLY)
        self.approvals(3)
        _run(pm.promote(x1))
        self.customer(_compose("自由文の回答です。", category="挨拶・雑談"))
        self.assertEqual(_texts(), ["自由文の回答です。"])


class TestExemplarPage(_PromoteMixin):
    def test_button_only_for_candidates_and_promote_retire_via_page(self):
        x1 = _exemplar(reply=REPLY)
        r = _client.get("/app/exemplars", headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("定型に昇格", r.text)
        self.assertIn("昇格した定型はありません", r.text)
        self.approvals(3)
        r = _client.get("/app/exemplars", headers=_auth())
        self.assertIn("定型に昇格（無修正承認 3 回）", r.text)
        self.assertNotIn(USER, r.text)
        r = _client.post("/app/exemplars/promote", data={"exemplar_id": str(x1)}, headers=_auth(), follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertIn("done=promoted", r.headers["location"])
        r = _client.get("/app/exemplars", headers=_auth())
        self.assertIn("J-U1", r.text)
        self.assertIn(REPLY, r.text)
        self.assertNotIn("定型に昇格", r.text)                   # 昇格済みはボタンなし
        r = _client.post("/app/exemplars/promote", data={"exemplar_id": str(x1)}, headers=_auth(), follow_redirects=False)
        self.assertIn("done=not_candidate", r.headers["location"])
        r = _client.post("/app/exemplars/text", data={"text_id": "J-U1", "action": "retire"}, headers=_auth(),
                         follow_redirects=False)
        self.assertIn("done=text_retired", r.headers["location"])
        self.assertEqual(self.texts()[0]["status"], pm.STATUS_RETIRED)
        r = _client.post("/app/exemplars/text", data={"text_id": "J-U1", "action": "reactivate"}, headers=_auth(),
                         follow_redirects=False)
        self.assertIn("done=text_reactivated", r.headers["location"])
        self.assertEqual(_client.post("/app/exemplars/text", data={"text_id": "x", "action": "retire"},
                                      headers=_auth(), follow_redirects=False).status_code, 400)
        self.assertEqual(_client.post("/app/exemplars/promote", data={"exemplar_id": "x"},
                                      headers=_auth(), follow_redirects=False).status_code, 400)
        self.assertEqual(_client.post("/app/exemplars/promote", data={"exemplar_id": "1"},
                                      follow_redirects=False).status_code, 303)   # 未認証


class TestPins(unittest.TestCase):
    def test_sink_allowlist_and_migration(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/jikou_reply_promote.py", "hub/webapp_exemplar_view.py", "hub/jikou_reply_flow.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("jikou_reply_promote", allow)
        src = (REPO / "alembic" / "versions" / "20261013_e6a8c0d2f4b6_jikou_reply_text.py").read_text(encoding="utf-8")
        self.assertIn("revision: str = 'e6a8c0d2f4b6'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'd4f6b8c0e2a4'", src)
        self.assertIn("jikou_reply_text_downgrade_refused_data_present", src)
        self.assertIn("jikou_reply_promote_metadata", (REPO / "alembic" / "env.py").read_text(encoding="utf-8"))
        self.assertEqual(pm.PROMOTION_THRESHOLD, 3)
        self.assertTrue({"時効見立て_条件付き", "法的判断・見通し"} <= pm.DRAFT_ONLY_CATEGORIES)
        self.assertEqual(T.CATEGORY_TO_TEXT_IDS, {})           # コード側の対応表は空のまま（データ表が正）
        fsrc = (REPO / "hub" / "jikou_reply_flow.py").read_text(encoding="utf-8")
        self.assertEqual(fsrc.count("_try_promoted("), 2)
        self.assertEqual(pm.compute_version([]), pm.compute_version([{"text_id": "J-U1", "category": "c", "text_version": "v",
                                                                       "status": "retired"}]))


if __name__ == "__main__":
    unittest.main()
