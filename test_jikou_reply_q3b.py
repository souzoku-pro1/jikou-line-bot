"""JIKOU-REPLY-Q3b（QUEUE-2026-10-13-D D-2）: 定型に該当しない問いへの AI 下書き（手本参考・承認待ち・自動送信なし）

固定する仕様:
- V2 OFF: 下書きは作らない（Claude を呼ばない・表に書かない）＝既存経路不変。
- V2 ON の顧客対応経路で承認へ降格（Q2 該当なし）した受信: 対応待ち（Q1c）に紐づけて reply_draft に 1 件（冪等）。
  同 category の承認済み手本 最大 3 件が無ければ作らない（無言+対応待ち通知のみ）。生成失敗・保存失敗は作らない。
- 検査: 既存サーバーガード（マーカー除去・禁止語・字数・必須語・文体・空返事）。不合格は保存のみ・通知に「要修正」。
  合格でも自動送信しない（顧客への送信 0）。
- 通知: 【下書きあり】固定文言（対応待ち ID・カテゴリ・要修正の有無のみ）を台帳経由で 1 会話 1 日 1 回。本文・氏名・
  LINE userId を載せない。
- /app/pending は下書きのある対応待ちに「下書きあり」印（本文は出さない）。API も要約のみ。
- migration c3e5a7b9d1f3（b2d4 の後ろ）・downgrade はデータがあれば拒否。pin: 新 module の sink 違反ゼロ・
  allowlist 追加ゼロ・chat_responder の結線は 1 行。
"""

import datetime
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import USER, _DbMixin, _auth, _client, _compose, _run, _texts  # noqa: E402

import chat_responder as cr  # noqa: E402
from hub import db  # noqa: E402
from hub import jikou_reply_flow as F  # noqa: E402
from hub import reply_draft as rd  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
CAT = "手続きの一般的な流れ"


def _exemplar(category: str = CAT, stage: str = rx.STAGE_ENGAGED, reply: str = "手続きは受任から 2〜4 週間程度です。") -> int:
    """承認済み手本を 1 件保存し、その id を返す。"""
    import sqlalchemy as sa
    res = _run(rx.save_exemplar(business="jikou", inbound_text="どのくらいかかりますか", human_reply=reply,
                                creditor_name=None, stage=stage, category_id=category))
    assert res == "saved", res

    async def _max():
        async with db.session_scope() as s:
            return int((await s.execute(sa.select(sa.func.max(rx.reply_exemplar.c.id)))).scalar())
    return _run(_max())


class _Mixin(_DbMixin):
    def setUp(self):
        super().setUp()

        async def _create():
            async with db.get_async_engine().begin() as c:
                await c.run_sync(rd.metadata.create_all)
        _run(_create())
        self.intent = AsyncMock(return_value={"intent": "その他", "creditor_name": ""})
        self.gen = AsyncMock(return_value="ご相談の手続きは、ご依頼から 2〜4 週間程度で完了する見込みです。")
        self._extra = [patch.object(F, "classify_intent", self.intent), patch.object(rd, "generate_text", self.gen)]
        for p in self._extra:
            p.start()

    def tearDown(self):
        for p in self._extra:
            p.stop()
        super().tearDown()

    def drafts(self) -> list:
        return self._rows(rd.reply_draft)

    def gen_direct(self, category: str = CAT, status: str = "受任", text: str = "質問です"):
        return _run(rd.maybe_generate(USER, text, [], category, status, profile=cr.JIKOU_PROFILE))


# ── 1. 生成の条件・保存・通知 ───────────────────────────────────────────────────
class TestGenerate(_Mixin):
    def test_off_never_generates(self):
        _exemplar()
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        self.gen.assert_not_called()
        self.assertEqual(self.drafts(), [])

    def test_on_without_exemplars_makes_no_draft_but_pending_and_notice_exist(self):
        self.on()
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        self.gen.assert_not_called()
        self.assertEqual(self.drafts(), [])
        self.assertEqual(len(self.pending()), 1)
        self.assertEqual(self.biz.call_count, 1)                     # Q1c の【対応待ち】のみ
        self.assertEqual(_texts(), [])

    def test_on_with_exemplars_stores_draft_once_and_notifies_once(self):
        self.on()
        self.touch("ev1")
        xid = _exemplar()
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        rows = self.drafts()
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual((r["state"], r["guard_ok"], r["category"], r["exemplar_ids"]),
                         (rd.STATE_PROPOSED, True, CAT, f"[{xid}]"))
        self.assertEqual(r["pending_id"], self.pending()[0]["pending_id"])
        self.assertEqual(r["draft_text"], self.gen.return_value)
        self.assertNotIn(USER, str(r))
        self.assertEqual(self.gen.call_args.args[0][0]["id"], xid)   # 手本が渡る
        notices = [c.args[1] for c in self.biz.call_args_list]
        self.assertEqual(len(notices), 2)                            # 【対応待ち】+【下書きあり】
        self.assertTrue(notices[1].startswith(rd.NOTICE_HEADER))
        self.assertIn(rd.NOTICE_OK, notices[1])
        self.assertNotIn(self.gen.return_value, notices[1])
        self.assertNotIn(USER, notices[1])
        self.assertEqual(_texts(), [sp.J0_TEXT])                     # 顧客へは J-0 だけ（下書きは送らない）
        # 同じ対応待ちには 2 件目を作らない
        self.assertIsNone(self.gen_direct())
        # 新しい受信（別イベント）→ 新しい対応待ち → 下書きは作るが通知は本日 2 通目を省略
        self.touch("ev2")
        self.customer(_compose("自由文 2", category=CAT, auto_send=False), text="別の質問です")
        self.assertEqual(len(self.drafts()), 2)
        self.assertEqual(self.biz.call_count, 2)
        hist = _run(rd.history(int(r["draft_id"])))
        self.assertEqual([h["action"] for h in hist], [rd.ACTION_CREATED])

    def test_guard_failure_is_saved_as_needs_fix(self):
        self.on()
        _exemplar()
        self.gen.return_value = "必ず時効が成立します。安心してください。"
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        r = self.drafts()[0]
        self.assertFalse(r["guard_ok"])
        self.assertIn("禁止語", r["guard_reasons"])
        self.assertIn(rd.NOTICE_NEEDS_FIX, self.biz.call_args_list[-1].args[1])
        self.assertEqual(_texts(), [])

    def test_empty_reply_and_marker_are_needs_fix(self):
        self.on()
        _exemplar()
        self.gen.return_value = "内容を確認のうえ、改めてご連絡いたします。"
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        r = self.drafts()[0]
        self.assertFalse(r["guard_ok"])
        self.assertIn(sp.EMPTY_REPLY_REASON_PREFIX, r["guard_reasons"])
        out, ok, reasons = rd.check_draft("", [], "q", CAT, profile=cr.JIKOU_PROFILE)
        self.assertFalse(ok)
        self.assertIn("空の返信", reasons)
        out, ok, reasons = rd.check_draft("x" * 400, [], "q", "法的判断・見通し", profile=cr.JIKOU_PROFILE)
        self.assertFalse(ok)
        self.assertTrue(any(x.startswith("文字数超過") for x in reasons))
        self.assertFalse(any("カテゴリ" in x for x in reasons))

    def test_generation_failure_or_no_pending_makes_no_draft(self):
        self.on()
        _exemplar()
        self.gen.side_effect = RuntimeError("api")
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        self.assertEqual(self.drafts(), [])
        self.assertEqual(self.biz.call_count, 1)
        self.gen.side_effect = None
        # 対応待ちが無い（直接呼出・open なし）→ None
        _run(rp.mark_not_needed(int(self.pending()[0]["pending_id"])))
        self.assertIsNone(self.gen_direct())
        self.assertEqual(self.drafts(), [])

    def test_category_without_exemplars_is_skipped_even_if_other_category_has(self):
        self.on()
        _exemplar(category="費用の定型案内")
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        self.gen.assert_not_called()
        self.assertEqual(self.drafts(), [])

    def test_state_transitions_are_guarded(self):
        self.on()
        _exemplar()
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        did = int(self.drafts()[0]["draft_id"])
        self.assertTrue(_run(rd.set_state(did, rd.STATE_APPROVED, actor=rd.ACTOR_HUMAN, approval_kind=rd.KIND_ASIS)))
        self.assertFalse(_run(rd.set_state(did, rd.STATE_APPROVED, actor=rd.ACTOR_HUMAN, approval_kind=rd.KIND_ASIS)))
        self.assertFalse(_run(rd.set_state(did, "bogus", actor=rd.ACTOR_HUMAN)))
        self.assertFalse(_run(rd.set_state(did, rd.STATE_APPROVED, actor=rd.ACTOR_HUMAN, approval_kind="x")))
        self.assertTrue(_run(rd.set_state(did, rd.STATE_SENT, actor=rd.ACTOR_HUMAN, op_id="op1",
                                          from_states=(rd.STATE_APPROVED,))))
        d = _run(rd.get_draft(did))
        self.assertEqual((d["state"], d["approval_kind"], d["op_id"]), (rd.STATE_SENT, rd.KIND_ASIS, "op1"))
        self.assertEqual([h["action"] for h in _run(rd.history(did))],
                         [rd.ACTION_CREATED, rd.ACTION_APPROVED, rd.ACTION_SENT])
        self.assertEqual([x["draft_id"] for x in _run(rd.list_drafts("jikou", states=(rd.STATE_SENT,)))], [did])
        self.assertEqual(_run(rd.list_drafts("jikou")), [])


# ── 2. 対応待ち画面の印 ─────────────────────────────────────────────────────────
class TestPendingMark(_Mixin):
    def test_pending_page_and_api_show_draft_mark_without_text(self):
        self.env({"WEBAPP_PASSWORD_HASH": os.environ["WEBAPP_PASSWORD_HASH"]})
        self.on()
        _exemplar()
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        pid = int(self.pending()[0]["pending_id"])
        r = _client.get("/app/pending", headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertIn('class="badge draft">下書きあり', r.text)
        self.assertNotIn(self.gen.return_value, r.text)
        api = _client.get("/app/api/pending/open", headers=_auth()).json()
        self.assertEqual(api["items"][0]["draft"]["draft_id"], int(self.drafts()[0]["draft_id"]))
        self.assertNotIn("draft_text", str(api))
        m = _run(rd.latest_by_pending_ids([pid, 999]))
        self.assertEqual(set(m), {pid})
        self.assertEqual(_run(rd.latest_by_pending_ids([])), {})

    def test_pending_page_without_drafts_has_no_mark(self):
        self.on()
        self.customer(_compose("自由文", category=CAT, auto_send=False))
        r = _client.get("/app/pending", headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertNotIn('class="badge draft"', r.text)          # 印なし（理由ラベルの「下書きあり」は Q1c の文言）


# ── 3. 純関数・pin ─────────────────────────────────────────────────────────────
class TestPins(unittest.TestCase):
    def test_notice_and_messages(self):
        self.assertEqual(rd.notice_key("ref1", "h" * 64, today=datetime.date(2026, 10, 13)), "draft_notice:ref1:2026-10-13")
        self.assertEqual(rd.notice_key("", "h" * 64, today=datetime.date(2026, 10, 13)), "draft_notice:" + "h" * 16 + ":2026-10-13")
        t = rd.notice_text(12, CAT, False)
        self.assertIn("対応待ち ID 12", t)
        self.assertIn(rd.NOTICE_NEEDS_FIX, t)
        msgs = rd.build_messages([{"role": "assistant", "content": "a"}, {"role": "system", "content": "x"}], "q")
        self.assertEqual(msgs, [{"role": "assistant", "content": "a"}, {"role": "user", "content": "q"}])
        self.assertEqual(rd.STATES, ("proposed", "approved", "rejected", "sent", "failed"))
        self.assertIn("300 字以内", rd.DRAFT_SYSTEM)

    def test_new_module_sink_and_wiring(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in ("hub/reply_draft.py", "hub/webapp_pending_view.py"):
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
        self.assertNotIn("reply_draft.py", allow)
        csrc = (REPO / "chat_responder.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("reply_draft.maybe_generate(" in l for l in csrc), 1)
        self.assertIn("from hub import reply_draft", "\n".join(csrc[-4:]))
        for banned in ("set_attending", "conversation_state", "decide_send"):
            self.assertNotIn(banned, "\n".join(csrc))

    def test_migration_chains_after_draft_approval(self):
        p = REPO / "alembic" / "versions" / "20261013_c3e5a7b9d1f3_reply_draft.py"
        src = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'c3e5a7b9d1f3'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'b2d4f6a8c0e1'", src)
        self.assertIn("reply_draft_downgrade_refused_data_present", src)
        self.assertIn("reply_draft_metadata", (REPO / "alembic" / "env.py").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
