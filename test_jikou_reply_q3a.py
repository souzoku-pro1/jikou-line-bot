"""JIKOU-REPLY-Q3a（QUEUE-2026-10-12-C C-2）: 手本の承認・退役画面／承認済み手本の参考文例／無修正承認の計数と候補通知

固定する仕様:
- (a) /app/exemplars は _gate（未認証 303・外部作用なし）。承認（退役 → 承認済み・approved_by/at 更新・履歴 approved）
  と退役（承認済み → 退役・履歴 deactivated）の 2 操作のみ（PRG 303・入力は閉集合・不正は固定 400）。
  画面は匿名化済み文だけを出す（原文・氏名・LINE userId なし）。
- (b) 承認済み（active）手本を category ごとに新しい順で最大 3 件・最大 4 カテゴリ、system prompt の末尾に
  「参考文例（…そのまま写さない）」として添える。手本 0 件なら prompt は従来と完全一致（pin 不変）。
  退役した手本は使わない。自動送信した返信の category の群だけ used（used_count+1・履歴）。
- (c) 起票時の AI 下書きの sha256 を記録し、承認済み下書きの送信成功時に送信文と照合（無修正承認）。同じ category の
  無修正承認が 3 回に達したら業務 LINE に「凍結定型の候補」を 1 回だけ通知（台帳キー・再配送でも 1 通）。
  本文は表に持たない（sha256 のみ）。通知文は category と回数のみ（PII なし）。
- 返信欄の手本保存に App 29 のカテゴリを分類 ID として付ける（閉集合外は NULL）。
- migration b2d4f6a8c0e1（a1c3 の後ろ・downgrade はデータがあれば拒否）。f7b2 の履歴 CHECK に approved を追加。
- 結線は chat_responder 1 行置換 ×5＋EOF・main 1 行置換 ×1＋EOF（allowlist 番地不変）。
"""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
from hub import autoreply_stoplist  # noqa: E402
from hub import db  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import reply_followup as rf  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from hub import webapp_exemplar_view as xv  # noqa: E402
from hub.inbound_event import Base as InboundBase  # noqa: E402
from hub.webapp_auth import MIN_ITERATIONS, hash_password, issue_session  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
JIKOU = line_channel.JIKOU_CHANNEL
USER = "U" + "a" * 32
ATTORNEY = "U" + "c" * 32
_ENV = {"WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
        "WEBAPP_SESSION_SECRET": "s" * 32, "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
        "ATTORNEY_LINE_USER_ID": ATTORNEY, "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz_token",
        "KINTONE_WEBHOOK_TOKEN": "kintone-token"}


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


def _auth():
    return {"Cookie": f"webapp_session={issue_session()}"}


def _run(coro):
    try:
        return asyncio.run(coro)
    finally:
        db.reset_for_tests()


class _FakeResp:
    def __init__(self, status: int, text: str = ""):
        self.status_code = status
        self.is_success = 200 <= status < 300
        self.text = text

    def json(self):
        return {"id": "500"}


class _FakeClient:
    calls: list = []

    def __init__(self, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeClient.calls.append((url, json))
        return _FakeResp(200)


def _compose(reply: str, category: str = "挨拶・雑談", auto_send: bool = True):
    return {"reply": reply, "category": category, "auto_send": auto_send,
            "jikou_update_flag": False, "immediate_notice": "none", "reason": "r"}


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="q3a_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", **_ENV})
        self._env.start()
        os.environ.pop(sl.POLICY_V2_ENV, None)
        os.environ.pop("KINTONE_EVENT_DEDUP_ENABLED", None)
        db.reset_for_tests()
        sp.clear_condition_cache()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                for md in (sl.metadata, rx.metadata, rf.metadata, rp.metadata, InboundBase.metadata):
                    await c.run_sync(md.create_all)
        _run(_create())
        _FakeClient.calls = []
        self.biz = AsyncMock(return_value=True)
        self._patches = [
            patch.object(line_channel.httpx, "AsyncClient", _FakeClient),
            patch.object(sp, "_push_business_http", self.biz),
            patch.object(autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)),
            patch.object(cr, "save_to_chatlog", AsyncMock(return_value="800")),
            patch.object(cr, "_notify_attorney", AsyncMock()),
            patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=cr.ChatHistory())),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        for p in self._patches:
            p.stop()
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _rows(self, table) -> list:
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(table))).fetchall()]
        return _run(_q())

    def save(self, reply="費用は【費用のご案内】のとおりです。", inbound="費用はいくらですか", category="費用の定型案内",
             stage=rx.STAGE_CONSULT, approve=True) -> int:
        """JIKOU-REPLY-Q1b-fix2 BR-10: 保存直後は pending_review で参考文例に使われないため、参考文例のテストは人の承認
        （approve_exemplar）を経た手本として用意する。approve=False は確認待ちのまま（承認画面の遷移テスト用）。"""
        self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text=inbound, human_reply=reply,
                                                stage=stage, category_id=category)), "saved")
        xid = max(r["id"] for r in self._rows(rx.reply_exemplar))
        if approve:
            self.assertTrue(_run(rx.approve_exemplar(xid, actor="human")))
        return xid


# ── 1. 画面（_gate・承認・退役・履歴・PII） ───────────────────────────────────────
class TestExemplarView(_DbMixin):
    def test_gated_and_closed(self):
        self.assertEqual(_client.get("/app/exemplars", follow_redirects=False).status_code, 303)
        self.assertEqual(_client.post("/app/exemplars/update", data={"exemplar_id": "1", "action": "retire"},
                                      follow_redirects=False).status_code, 303)
        for r in xv.router.routes:
            self.assertTrue(getattr(r.endpoint, "__webapp_gate__", False), r.path)
        from hub.webapp_brain_view import _paths_of
        paths = [p for entry in main.app.router.routes for p in _paths_of(entry)
                 if p in ("/app/exemplars", "/app/{_rest:path}")]
        self.assertEqual(paths, ["/app/exemplars", "/app/{_rest:path}"])

    def test_retire_and_approve_with_history_and_no_pii(self):
        xid = self.save(reply="試験 太郎様、費用は44,000円です。電話は090-1234-5678です", inbound="試験 太郎です",
                        approve=False)
        r = _client.get("/app/exemplars", headers=_auth())
        self.assertEqual(r.status_code, 200)
        # fix2 BR-10: 保存直後は「確認待ち」（人がこの画面で匿名化を確認して承認するまで参考文例に使われない）
        self.assertIn('<span class="badge off">確認待ち</span>', r.text)
        self.assertNotIn('<span class="badge">承認済み</span>', r.text)
        self.assertEqual(_run(rx.prompt_note("jikou", "受付")), "")
        r = _client.post("/app/exemplars/update", data={"exemplar_id": str(xid), "action": "approve"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/exemplars?done=approved")
        self.assertEqual(_run(rx.get_exemplar(xid))["status"], "approved")
        r = _client.get("/app/exemplars", headers=_auth())
        self.assertIn('<span class="badge">承認済み</span>', r.text)
        self.assertIn("費用の定型案内", r.text)
        self.assertNotIn("090-1234-5678", r.text)
        self.assertIn("〔電話〕", r.text)
        self.assertEqual(r.headers.get("cache-control"), "no-store, private")
        r = _client.post("/app/exemplars/update", data={"exemplar_id": str(xid), "action": "retire"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/exemplars?done=retired"))
        self.assertFalse(_run(rx.get_exemplar(xid))["active"])
        self.assertIn("手本はありません", _client.get("/app/exemplars", headers=_auth()).text)   # 既定は承認済みのみ
        self.assertIn("退役", _client.get("/app/exemplars?all=1", headers=_auth()).text)
        r = _client.post("/app/exemplars/update", data={"exemplar_id": str(xid), "action": "retire"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/exemplars?done=not_changed")
        r = _client.post("/app/exemplars/update", data={"exemplar_id": str(xid), "action": "approve"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/exemplars?done=approved")
        row = _run(rx.get_exemplar(xid))
        self.assertTrue(row["active"])
        self.assertEqual(row["approved_by"], "human")
        self.assertEqual([h["action"] for h in _run(rx.exemplar_history(xid))],
                         ["created", "approved", "deactivated", "approved"])          # fix2: 保存後の承認が 1 段増える
        self.assertEqual(_client.post("/app/exemplars/update", data={"exemplar_id": "x", "action": "approve"},
                                      headers=_auth(), follow_redirects=False).status_code, 400)
        self.assertEqual(_client.post("/app/exemplars/update", data={"exemplar_id": "1", "action": "delete"},
                                      headers=_auth(), follow_redirects=False).status_code, 400)
        r = _client.post("/app/exemplars/update", data={"exemplar_id": "999", "action": "retire"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/exemplars?done=not_found")
        api = _client.get("/app/api/exemplars", headers=_auth()).json()
        self.assertEqual(api["count"], 1)
        self.assertNotIn("090-1234-5678", json.dumps(api, ensure_ascii=False))


# ── 2. 参考文例（件数上限・0 件時の不変・退役除外・used） ─────────────────────────
class TestPromptNote(_DbMixin):
    def test_zero_exemplars_keeps_prompt_identical(self):
        base = cr.build_system_prompt(status="受任")
        note = _run(rx.prompt_note("jikou", "受任"))
        self.assertEqual(note, "")
        self.assertEqual(cr.build_system_prompt(status="受任", exemplar_note=note), base)

    def test_limits_per_category_and_categories_and_excludes_inactive(self):
        ids = {}
        for cat in ("費用の定型案内", "手続きの一般的な流れ", "必要書類の案内", "進捗の事実回答", "挨拶・雑談"):
            ids[cat] = [self.save(reply=f"{cat} の返信 {i}", inbound=f"質問 {i}", category=cat) for i in range(4)]
        retired = self.save(reply="退役した返信", inbound="q", category="費用の定型案内")
        _run(rx.deactivate_exemplar(retired))
        note = _run(rx.prompt_note("jikou", "受付"))
        self.assertTrue(note.startswith(rx.NOTE_HEADER))
        self.assertNotIn("退役した返信", note)
        self.assertEqual(len(note.ids_by_category), rx.NOTE_MAX_CATEGORIES)
        for cat, used in note.ids_by_category.items():
            self.assertEqual(len(used), rx.NOTE_MAX_PER_CATEGORY)
            self.assertEqual(used, sorted(ids[cat], reverse=True)[:3])     # 新しい順
        self.assertNotIn("費用の定型案内", note.ids_by_category)              # 最も古いカテゴリは落ちる
        self.assertIn("参考文例", note)
        self.assertIn("そのまま写さない", note)
        prompt = cr.build_system_prompt(status="受付", exemplar_note=note)
        self.assertTrue(prompt.endswith(note))
        # 受任後の段階は相談段階の手本を使わない
        self.assertEqual(_run(rx.prompt_note("jikou", "受任")), "")

    def test_customer_path_attaches_note_and_marks_used_only_for_sent_category(self):
        a = self.save(reply="手続きの返信", inbound="流れは", category="手続きの一般的な流れ")
        b = self.save(reply="挨拶の返信", inbound="こんにちは", category="挨拶・雑談")
        seen = {}

        async def _compose_spy(system_prompt, messages, tool=None):
            seen["prompt"] = system_prompt
            return _compose("ご案内します", category="挨拶・雑談")
        sent = AsyncMock()
        with patch.object(cr, "_call_compose_reply", _compose_spy), \
             patch.object(cr, "get_app21_record", AsyncMock(return_value=_rec(status="受付"))):
            _run(cr.handle_customer_message(user_id=USER, user_message="こんにちは", reply_token="rt",
                                            app21_record=_rec(status="受付"), reply_func=sent))
        sent.assert_awaited_once_with("rt", "ご案内します")
        self.assertIn("手続きの返信", seen["prompt"])
        self.assertIn("挨拶の返信", seen["prompt"])
        rows = {r["id"]: r for r in self._rows(rx.reply_exemplar)}
        self.assertEqual((rows[a]["used_count"], rows[b]["used_count"]), (0, 1))
        self.assertEqual([h["action"] for h in _run(rx.exemplar_history(b))],
                         ["created", "approved", "used"])                    # fix2 BR-10: save() が承認を 1 段挟む

    def test_demoted_reply_does_not_mark_used(self):
        b = self.save(reply="挨拶の返信", inbound="こんにちは", category="挨拶・雑談")
        with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("x", category="法的判断・見通し", auto_send=False))), \
             patch.object(cr, "save_to_approval_queue", AsyncMock(return_value="500")), \
             patch.object(cr, "get_app21_record", AsyncMock(return_value=_rec(status="受付"))):
            _run(cr.handle_customer_message(user_id=USER, user_message="q", reply_token="rt",
                                            app21_record=_rec(status="受付"), reply_func=AsyncMock()))
        self.assertEqual({r["id"]: r["used_count"] for r in self._rows(rx.reply_exemplar)}[b], 0)

    def test_format_note_is_pure_and_db_failure_is_silent(self):
        self.assertEqual(rx.format_note([]), "")
        n = rx.format_note([{"id": 1, "category_id": None, "inbound_text": "", "human_reply": "r", "active": True}])
        self.assertIn(rx.NO_CATEGORY_LABEL, n)
        self.assertEqual(n.ids_by_category, {rx.NO_CATEGORY_LABEL: [1]})
        with patch.object(rx, "list_exemplars", AsyncMock(side_effect=RuntimeError("db"))):
            self.assertEqual(_run(rx.prompt_note("jikou", "受付")), "")
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            self.assertEqual(_run(rx.prompt_note("jikou", "受付")), "")


# ── 3. 無修正承認の計数と候補通知（1 回性・PII なし・再配送） ─────────────────────
class TestPromotion(_DbMixin):
    def _queue(self, draft, category="費用の定型案内", rid="500"):
        with patch.object(cr.httpx, "AsyncClient", _FakeClient):
            class _R(_FakeResp):
                def json(self_inner):
                    return {"id": rid}
            with patch.object(_FakeClient, "post", AsyncMock(return_value=_R(200))):
                return _run(cr.save_to_approval_queue(user_id=USER, customer_name="n", customer_message="m",
                                                      ai_draft=draft, category=category, reason="r"))

    def test_record_draft_on_queue_and_unmodified_count_with_single_notice(self):
        for i, rid in enumerate(("1", "2", "3", "4")):
            self.assertEqual(self._queue(f"下書き {i}", rid=rid), rid)
        rows = {r["record_id"]: r for r in self._rows(rx.reply_draft_approval)}
        self.assertEqual(set(rows), {"1", "2", "3", "4"})
        self.assertIsNone(rows["1"]["unmodified"])
        self.assertNotIn("下書き", json.dumps({k: {kk: str(vv) for kk, vv in v.items()} for k, v in rows.items()},
                                           ensure_ascii=False))
        # 1 件目・2 件目: 無修正（通知なし）
        self.assertEqual(_run(sp.approved_draft_sent("1", USER, "下書き 0", "費用の定型案内")), 0)
        self.assertEqual(_run(sp.approved_draft_sent("2", USER, "下書き 1", "費用の定型案内")), 0)
        self.biz.assert_not_awaited()
        # 修正あり: 数えない
        _run(sp.approved_draft_sent("3", USER, "直した下書き", "費用の定型案内"))
        self.assertEqual(_run(rx.count_unmodified("jikou", "費用の定型案内")), 2)
        self.biz.assert_not_awaited()
        # 3 回目の無修正: 候補通知 1 通（category と回数のみ）
        _run(sp.approved_draft_sent("4", USER, "下書き 3", "費用の定型案内"))
        self.assertEqual(self.biz.await_count, 1)
        text = self.biz.await_args.args[1]
        self.assertIn("費用の定型案内", text)
        self.assertIn("3 回", text)
        self.assertNotIn("下書き 3", text)                        # 本文は載せない（固定文言のみ）
        self.assertNotIn(USER, text)
        # 再配送（同じレコードの再送信）は数えず・通知もしない
        _run(sp.approved_draft_sent("4", USER, "下書き 3", "費用の定型案内"))
        self.assertEqual(_run(rx.count_unmodified("jikou", "費用の定型案内")), 3)
        self.assertEqual(self.biz.await_count, 1)
        # 同じ閾値への再到達は台帳キーで 1 通（notice を直接）
        self.assertEqual(_run(sp.send_business_notice("exemplar_promotion:費用の定型案内:3", "x")), rp.NOTIFY_THROTTLED)

    def test_approval_webhook_passes_sent_text(self):
        self._queue("承認する下書き", rid="7")
        record = {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"},
                  "line_user_id": {"value": USER}, "AI下書き": {"value": "承認する下書き"},
                  "カテゴリ": {"value": "費用の定型案内"}, "$revision": {"value": "3"}}
        body = {"record": {"$id": {"value": "7"}, "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}}}
        with patch.object(main, "mark_approval_sent", AsyncMock()), \
             patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=record)):
            r = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual(r.json().get("record_id"), "7")
        row = self._rows(rx.reply_draft_approval)[0]
        self.assertTrue(row["unmodified"])
        self.assertIsNotNone(row["sent_at"])

    def test_unknown_record_or_db_off_is_silent(self):
        self.assertIsNone(_run(rx.record_sent("nope", "x")))
        self.assertEqual(_run(sp.promotion_check("nope", "x")), "none")
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            self.assertFalse(_run(rx.record_draft("1", "jikou", "挨拶・雑談", "x")))
            self.assertIsNone(_run(rx.record_sent("1", "x")))


# ── 4. 返信欄の手本保存に分類 ID ────────────────────────────────────────────────
class TestCategoryOnSave(_DbMixin):
    def test_save_exemplar_keeps_closed_set_category_only(self):
        self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text="q", human_reply="r",
                                                stage=rx.STAGE_CONSULT, category_id="費用の定型案内")), "saved")
        self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text="q", human_reply="r",
                                                stage=rx.STAGE_CONSULT, category_id="勝手な分類")), "saved")
        cats = [r["category_id"] for r in self._rows(rx.reply_exemplar)]
        self.assertEqual(cats, ["費用の定型案内", None])
        src = (REPO / "hub" / "webapp_reply_view.py").read_text(encoding="utf-8")
        self.assertIn('category_id=_v(rec, "カテゴリ")', src)


# ── 5. pin ─────────────────────────────────────────────────────────────────────
class TestPins(unittest.TestCase):
    def test_categories_match_compose_tool_enum(self):
        enum = cr._COMPOSE_REPLY_TOOL["input_schema"]["properties"]["category"]["enum"]
        self.assertEqual(list(rx.CATEGORIES), list(enum))
        self.assertEqual(rx.ACTIONS, ("created", "used", "deactivated", "approved"))
        self.assertEqual((rx.NOTE_MAX_PER_CATEGORY, rx.NOTE_MAX_CATEGORIES, rx.PROMOTION_THRESHOLD), (3, 4, 3))

    def test_wiring_and_sink(self):
        for name in ("hub/reply_exemplar.py", "hub/webapp_exemplar_view.py", "hub/send_policy.py",
                     "hub/webapp_reply_view.py"):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        self.assertNotIn("webapp_exemplar_view", allow)
        entries = json.loads(allow)["entries"]
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries if e.startswith("main.py:")),
                         [663, 685, 1320, 1620, 1917])
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries
                                if e.startswith("chat_responder.py:")), [1660, 1672])
        csrc = (REPO / "chat_responder.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("reply_exemplar.prompt_note(p.name, status)" in l for l in csrc), 1)
        self.assertEqual(sum("reply_exemplar.mark_used_for(_exn, category)" in l for l in csrc), 1)
        self.assertEqual(sum("reply_exemplar.record_draft(record_id" in l for l in csrc), 1)
        self.assertEqual(sum("exemplar_note: str = \"\"" in l for l in csrc), 1)
        self.assertEqual(sum("return prompt + human_note + exemplar_note" in l for l in csrc), 1)
        msrc = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("send_policy.approved_draft_sent(record_id, user_id, ai_draft, category)" in l for l in msrc), 1)
        self.assertIn("include_before_catch_all(app, exemplar_view_router)", "\n".join(msrc[-10:]))

    def test_migration_chain_and_history_check(self):
        p = REPO / "alembic" / "versions" / "20261012_b2d4f6a8c0e1_reply_draft_approval.py"
        src = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'b2d4f6a8c0e1'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'd4f6a8b0c2e4'", src)   # Q1c-fix1: 永続キュー d4f6 の後ろ（鎖は 1 本）
        self.assertIn("reply_draft_approval_downgrade_refused_data_present", src)
        f = (REPO / "alembic" / "versions" / "20261011_f7b2c9d4e1a6_reply_exemplar.py").read_text(encoding="utf-8")
        self.assertIn("'deactivated', 'approved'", f)


if __name__ == "__main__":
    unittest.main()
