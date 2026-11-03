"""JIKOU-REPLY-Q4（QUEUE-2026-10-13-D D-3）: PWA で AI 下書き（Q3b）を承認・修正・却下する画面 /app/drafts

固定する仕様:
- 権限: 未認証は 303 → /app/login（_gate）。
- 二重クリック: proposed→approved の CAS で 2 回目は副作用なし（送信 1 回・台帳 1 行）。
- 送信経路: 署名付き ticket・明示確認必須・send_ledger（主体 approved_draft・用途 reply・操作 ID 固定）・
  hub/line_channel.push_text。V2 ON の対応中・人対応中は許可表で停止（送らず failed/blocked）。
- 記録の種別: reply_draft_approval（record_id=draft:{id}）に outcome=approved_asis／approved_edited／rejected と
  unmodified（無修正=True）。送信成功で対応待ちを閉じる・App 28 に assistant 行。
- 匿名化: 修正承認は修正後本文を reply_exemplar に active=False（未承認）で匿名化保存。残留検査に落ちたら保存しない。
- migration d4f6b8c0e2a4（c3e5 の後ろ）: outcome 列。pin: 新 module の sink 違反ゼロ・allowlist 追加ゼロ・main 末尾結線。
"""

import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from test_jikou_reply_q1c import USER, _FakeClient, _auth, _client, _compose, _rec, _run, _texts  # noqa: E402
from test_jikou_reply_q3b import CAT, _Mixin, _exemplar  # noqa: E402

import chat_responder as cr  # noqa: E402
from hub import reply_draft as rd  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_drafts_view as dv  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_APPROVAL_REC = _rec(line_user_id=USER, 顧客名="山田 太郎", 顧客メッセージ="どのくらいかかりますか", カテゴリ=CAT,
                     status="受任", AI下書き="x")


class _DraftMixin(_Mixin):
    def setUp(self):
        super().setUp()
        self.env({"WEBAPP_PASSWORD_HASH": __import__("os").environ["WEBAPP_PASSWORD_HASH"]})
        self.approval_rec = dict(_APPROVAL_REC)
        self._p3 = [patch.object(dv, "_approval_record", AsyncMock(side_effect=lambda rid: self.approval_rec)),
                    patch.object(dv, "_history_rows", AsyncMock(return_value=[{"role": "user", "message": "q", "at": "t"}]))]
        for p in self._p3:
            p.start()

    def tearDown(self):
        for p in self._p3:
            p.stop()
        super().tearDown()

    def make_draft(self, text: str | None = None) -> int:
        """V2 ON・手本あり・会話あり → 承認降格 → 下書き 1 件。戻り値 draft_id。"""
        self.on()
        self.touch("ev-draft")
        _exemplar()
        if text:
            self.gen.return_value = text
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        _run(rp.attach_approval(int(self.pending()[0]["pending_id"]), "500"))
        self.biz.reset_mock()
        _FakeClient.calls.clear()                                     # 承認降格時の J-0 を数えない
        rows = self.drafts()
        self.assertEqual(len(rows), 1)
        return int(rows[0]["draft_id"])

    def open_form(self, did: int) -> dict:
        r = _client.get(f"{dv.PAGE}/{did}", headers=_auth())
        self.assertEqual(r.status_code, 200)
        op = r.text.split('name="op_id" value="')[1].split('"')[0]
        tk = r.text.split('name="ticket" value="')[1].split('"')[0]
        return {"op_id": op, "ticket": tk}

    def act(self, did: int, data: dict):
        return _client.post(f"{dv.PAGE}/{did}/act", data=data, headers=_auth(), follow_redirects=False)

    def approvals(self) -> list:
        return self._rows(rx.reply_draft_approval)

    def exemplars(self) -> list:
        return self._rows(rx.reply_exemplar)


class TestDraftsView(_DraftMixin):
    def test_gate_and_list(self):
        self.assertEqual(_client.get(dv.PAGE, follow_redirects=False).status_code, 303)
        self.assertEqual(_client.post(f"{dv.PAGE}/1/act", data={}, follow_redirects=False).status_code, 303)
        r = _client.get(dv.PAGE, headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertIn("承認待ちの下書きはありません", r.text)
        did = self.make_draft()
        r = _client.get(dv.PAGE, headers=_auth())
        self.assertIn(f"下書き ID {did}", r.text)
        self.assertNotIn(self.gen.return_value, r.text)                  # 本文は一覧に出さない
        self.assertNotIn(USER, r.text)
        api = _client.get("/app/api/drafts", headers=_auth()).json()
        self.assertEqual(api["count"], 1)
        self.assertNotIn("draft_text", str(api))
        self.assertNotIn(USER, str(api))
        self.assertEqual(_client.get(f"{dv.PAGE}/abc", headers=_auth()).status_code, 400)
        self.assertEqual(_client.get(f"{dv.PAGE}/999", headers=_auth()).status_code, 404)

    def test_detail_shows_context_and_draft_without_user_id(self):
        did = self.make_draft()
        r = _client.get(f"{dv.PAGE}/{did}", headers=_auth())
        self.assertIn(self.gen.return_value, r.text)
        self.assertIn("どのくらいかかりますか", r.text)
        self.assertIn("承認して送信", r.text)
        self.assertIn("却下", r.text)
        self.assertNotIn(USER, r.text)

    def test_approve_asis_sends_once_and_records(self):
        did = self.make_draft()
        f = self.open_form(did)
        r = self.act(did, dict(f, action="send", text=self.gen.return_value, confirm="1"))
        self.assertEqual(r.status_code, 303)
        self.assertIn("done=sent", r.headers["location"])
        self.assertEqual(_texts(), [self.gen.return_value])
        ops = [o for o in self.ops() if o["actor"] == sl.ACTOR_APPROVED_DRAFT]   # J-0（bot）は数えない
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["actor"], ops[0]["purpose"], ops[0]["state"]), (sl.ACTOR_APPROVED_DRAFT, "reply", "sent"))
        self.assertTrue(ops[0]["op_id"].startswith(f"draft:{did}:"))
        d = _run(rd.get_draft(did))
        self.assertEqual((d["state"], d["approval_kind"], d["final_text"]), (rd.STATE_SENT, rd.KIND_ASIS, self.gen.return_value))
        a = self.approvals()
        self.assertEqual(len(a), 1)
        self.assertEqual((a[0]["record_id"], a[0]["outcome"], a[0]["unmodified"]), (f"draft:{did}", dv.OUTCOME_ASIS, True))
        self.assertEqual([p["state"] for p in self.pending()], [rp.STATE_HUMAN_REPLIED])
        self.assertEqual(self.chatlog.call_args_list[-1].args[1:3], ("assistant", self.gen.return_value))
        self.assertEqual(len(self.exemplars()), 1)                        # 無修正は手本候補を増やさない
        # 二重クリック（同じフォーム再送）→ already・送信増えない
        r2 = self.act(did, dict(f, action="send", text=self.gen.return_value, confirm="1"))
        self.assertIn("done=already", r2.headers["location"])
        self.assertEqual(len(_texts()), 1)
        self.assertEqual(len([o for o in self.ops() if o["actor"] == sl.ACTOR_APPROVED_DRAFT]), 1)
        self.assertEqual([h["action"] for h in _run(rd.history(did))], [rd.ACTION_CREATED, rd.ACTION_APPROVED, rd.ACTION_SENT])

    def test_approve_edited_saves_anonymized_candidate(self):
        did = self.make_draft()
        f = self.open_form(did)
        edited = "山田様、ご依頼から 2〜4 週間程度で完了する見込みです。携帯 090-1234-5678 までご連絡ください。"
        r = self.act(did, dict(f, action="send", text=edited, confirm="1"))
        self.assertIn("done=sent_edited", r.headers["location"])
        self.assertEqual(_texts(), [edited])
        a = self.approvals()[0]
        self.assertEqual((a["outcome"], a["unmodified"]), (dv.OUTCOME_EDITED, False))
        ex = [e for e in self.exemplars() if not e["active"]]
        self.assertEqual(len(ex), 1)
        self.assertNotIn("山田", ex[0]["human_reply"])
        self.assertNotIn("090-1234-5678", ex[0]["human_reply"])
        self.assertEqual(ex[0]["category_id"], CAT)
        self.assertEqual(ex[0]["source_text_version"], sl.text_version(edited))
        d = _run(rd.get_draft(did))
        self.assertEqual((d["state"], d["approval_kind"]), (rd.STATE_SENT, rd.KIND_EDITED))
        # 未承認候補は参考文例に出ない（active=False）
        self.assertEqual([x["id"] for x in _run(rx.list_exemplars("jikou", category_id=CAT))], [ex[0]["id"] - 1] if ex[0]["id"] > 1 else [])

    def test_reject_records_and_sends_nothing(self):
        did = self.make_draft()
        f = self.open_form(did)
        r = self.act(did, dict(f, action="reject"))
        self.assertIn("done=rejected", r.headers["location"])
        self.assertEqual(_texts(), [])
        self.assertEqual(_run(rd.get_draft(did))["state"], rd.STATE_REJECTED)
        self.assertEqual(self.approvals()[0]["outcome"], dv.OUTCOME_REJECTED)
        self.assertIsNone(self.approvals()[0]["unmodified"])
        self.assertIn("done=already", self.act(did, dict(f, action="send", text="x", confirm="1")).headers["location"])
        self.assertEqual([p["state"] for p in self.pending()], [rp.STATE_OPEN])     # 却下は対応待ちを閉じない

    def test_confirm_ticket_and_input_are_required(self):
        did = self.make_draft()
        f = self.open_form(did)
        self.assertIn("confirm_required", self.act(did, dict(f, action="send", text="x")).headers["location"])
        self.assertIn("bad_input", self.act(did, dict(f, action="send", text="", confirm="1")).headers["location"])
        self.assertEqual(self.act(did, dict(f, action="send", text="x", confirm="1", ticket="bad")).status_code, 400)
        self.assertEqual(self.act(did, dict(f, action="bogus")).status_code, 400)
        self.assertEqual(self.act(did, dict(f, action="send", text="x", confirm="1", op_id="zz")).status_code, 400)
        # 宛先が変わった（ticket のハッシュ不一致）→ stale・送らない
        self.approval_rec = _rec(line_user_id="U" + "f" * 32, 顧客メッセージ="q", カテゴリ=CAT)
        self.assertIn("done=stale", self.act(did, dict(f, action="send", text="x", confirm="1")).headers["location"])
        self.assertEqual(_texts(), [])
        self.assertEqual(_run(rd.get_draft(did))["state"], rd.STATE_PROPOSED)

    def test_destination_unresolved_allows_reject_only(self):
        did = self.make_draft()
        self.approval_rec = None
        r = _client.get(f"{dv.PAGE}/{did}", headers=_auth())
        self.assertIn("却下のみ", r.text)
        self.assertNotIn("承認して送信", r.text)

    def test_policy_stop_blocks_send_and_marks_failed(self):
        did = self.make_draft()
        _run(sl.set_attending("jikou", USER))
        f = self.open_form(did)
        r = self.act(did, dict(f, action="send", text=self.gen.return_value, confirm="1"))
        self.assertIn("done=blocked", r.headers["location"])
        self.assertEqual(_texts(), [])
        d = _run(rd.get_draft(did))
        self.assertEqual(d["state"], rd.STATE_FAILED)
        # PRE-REVIEW-1 #279-3: 送られていない承認は outcome（承認率・無修正承認の母数）に数えない
        self.assertEqual(self.approvals(), [])
        self.assertEqual([p["state"] for p in self.pending()], [rp.STATE_OPEN])

    def test_ledger_unavailable_does_not_send_and_returns_draft_to_proposed(self):
        """PRE-REVIEW-1 #279-1: 承認済み下書きの送信は台帳必須（人の明示送信と同じ fail-closed）。"""
        did = self.make_draft()
        f = self.open_form(did)
        with patch.object(sl, "begin", AsyncMock(return_value=None)):
            r = self.act(did, dict(f, action="send", text=self.gen.return_value, confirm="1"))
        self.assertIn("done=ledger_unavailable", r.headers["location"])
        self.assertEqual(_texts(), [])                                  # 送らない
        self.assertEqual(_run(rd.get_draft(did))["state"], rd.STATE_PROPOSED)   # 承認前に戻す（再承認で送れる）
        self.assertEqual(self.approvals(), [])
        page = _client.get(f"{dv.PAGE}/{did}?done=ledger_unavailable", headers=_auth())
        self.assertIn("台帳が使えないため送信できません", page.text)
        r2 = self.act(did, dict(self.open_form(did), action="send", text=self.gen.return_value, confirm="1"))
        self.assertIn("done=sent", r2.headers["location"])              # 台帳が戻れば送れる
        self.assertEqual(len(_texts()), 1)
        self.assertEqual(self.approvals()[0]["outcome"], dv.OUTCOME_ASIS)

    def test_line_failure_marks_failed(self):
        from test_jikou_reply_q1c import _FakeResp
        did = self.make_draft()
        f = self.open_form(did)
        _FakeClient.responses = [_FakeResp(500, "err")]
        r = self.act(did, dict(f, action="send", text=self.gen.return_value, confirm="1"))
        self.assertIn("done=failed", r.headers["location"])
        self.assertEqual(_run(rd.get_draft(did))["state"], rd.STATE_FAILED)
        self.assertEqual(self.approvals(), [])                          # PRE-REVIEW-1 #279-3: 失敗は承認に数えない


class TestExemplarApi(_Mixin):
    def test_save_exemplar_inactive_and_set_outcome(self):
        self.on()
        res = _run(rx.save_exemplar(business="jikou", inbound_text="q", human_reply="r", stage=rx.STAGE_ENGAGED,
                                    category_id=CAT, active=False))
        self.assertEqual(res, "saved")
        rows = self._rows(rx.reply_exemplar)
        self.assertFalse(rows[0]["active"])
        self.assertEqual(_run(rx.list_exemplars("jikou", category_id=CAT)), [])
        self.assertTrue(_run(rx.approve_exemplar(int(rows[0]["id"]))))
        self.assertEqual(len(_run(rx.list_exemplars("jikou", category_id=CAT))), 1)
        self.assertTrue(_run(rx.record_draft("draft:1", "jikou", CAT, "t")))
        self.assertTrue(_run(rx.set_outcome("draft:1", dv.OUTCOME_REJECTED)))
        self.assertFalse(_run(rx.set_outcome("draft:1", "bogus")))
        self.assertFalse(_run(rx.set_outcome("draft:9", dv.OUTCOME_REJECTED)))


class TestPins(unittest.TestCase):
    def test_sink_wiring_and_migration(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/webapp_drafts_view.py", "hub/reply_exemplar.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("webapp_drafts_view", allow)
        msrc = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        self.assertIn("include_before_catch_all(app, drafts_view_router)", "\n".join(msrc[-6:]))
        self.assertIn("include_before_catch_all(app, exemplar_view_router)", "\n".join(msrc[-10:]))
        p = REPO / "alembic" / "versions" / "20261013_d4f6b8c0e2a4_draft_approval_outcome.py"
        src = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'd4f6b8c0e2a4'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'c3e5a7b9d1f3'", src)
        self.assertIn("reply_draft_approval_outcome_downgrade_refused_data_present", src)
        self.assertIn("outcome", [c.name for c in rx.reply_draft_approval.columns])
        self.assertEqual(dv.OUTCOMES, ("approved_asis", "approved_edited", "rejected"))
        self.assertEqual(set(rx.OUTCOMES), set(dv.OUTCOMES))

    def test_ticket_binding(self):
        import os
        with patch.dict(os.environ, {"WEBAPP_SESSION_SECRET": "s" * 32}):
            t = dv.make_ticket("5", USER, "a" * 32)
            self.assertEqual(dv.parse_ticket(t, "5", "a" * 32), dv._user_hash(USER))
            self.assertIsNone(dv.parse_ticket(t, "6", "a" * 32))
            self.assertIsNone(dv.parse_ticket(t, "5", "b" * 32))
            self.assertIsNone(dv.parse_ticket(t[:-1] + ("0" if t[-1] != "0" else "1"), "5", "a" * 32))
            self.assertIsNone(dv.parse_ticket("junk", "5", "a" * 32))


if __name__ == "__main__":
    unittest.main()
