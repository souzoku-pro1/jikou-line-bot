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

    def test_exemplar_referenced_reply_is_fixed_to_approval_queue_v2_off_and_on(self):
        """fix1 BQ-01（Codex R-Q3a・Fable 裁定）: 手本を参照して生成した本文は、カテゴリが許可表にあっても **承認前に顧客へ 0 回**
        （V2 OFF: 従来の確認中定型だけ／V2 ON: 無言＋対応待ち・J-0 のみ）。承認キューに固定し、使った手本は結果の category の群だけ used。
        旧テスト「未承認のご案内が 1 回送られる」はこの裁定に合わせて書き換えた（緩和ではなく裁定の追随）。"""
        a = self.save(reply="手続きの返信", inbound="流れは", category="手続きの一般的な流れ")
        b = self.save(reply="挨拶の返信", inbound="こんにちは", category="挨拶・雑談")
        for v2 in ("", "1"):
            seen = {}

            async def _compose_spy(system_prompt, messages, tool=None):
                seen["prompt"] = system_prompt
                return _compose("ご案内します", category="挨拶・雑談")
            sent = AsyncMock(return_value=sl.SEND_SENT)
            queued = AsyncMock(return_value="500")
            env = {sl.POLICY_V2_ENV: "1"} if v2 else {}
            with patch.dict(os.environ, env), patch.object(cr, "_call_compose_reply", _compose_spy), \
                 patch.object(cr, "save_to_approval_queue", queued), \
                 patch.object(cr, "get_app21_record", AsyncMock(return_value=_rec(status="受付"))):
                if not v2:
                    os.environ.pop(sl.POLICY_V2_ENV, None)
                sp.clear_condition_cache()
                _run(cr.handle_customer_message(user_id=USER, user_message="こんにちは", reply_token="rt",
                                                app21_record=_rec(status="受付"), reply_func=sent))
            self.assertIn("手続きの返信", seen["prompt"])
            self.assertIn("挨拶の返信", seen["prompt"])
            texts = [c.args[1] for c in sent.await_args_list]
            self.assertNotIn("ご案内します", texts, f"v2={v2!r}")                       # 生成本文の顧客送信は承認前 0 回
            for t in texts:                                                           # 送られるのは定型（V2 OFF）か J-0（V2 ON）だけ
                self.assertIn(t, (cr.PENDING_REPLY, sp.J0_TEXT) + tuple(cr.PENDING_BY_CATEGORY.values()), f"v2={v2!r} {t!r}")
            queued.assert_awaited_once()
            self.assertEqual(queued.await_args.kwargs["ai_draft"], "ご案内します")
            self.assertIn(rx.EXEMPLAR_REFERENCED_REASON, queued.await_args.kwargs["reason"])
            self.assertEqual(len(_FakeClient.calls), 0)                                # LINE の直 push も無い
        rows = {r["id"]: r for r in self._rows(rx.reply_exemplar)}
        self.assertEqual((rows[a]["used_count"], rows[b]["used_count"]), (0, 2))        # 結果の category の群だけ・固定ごとに 1
        self.assertEqual([h["action"] for h in _run(rx.exemplar_history(b))],
                         ["created", "approved", "used", "used"])

    def test_zero_exemplars_keeps_auto_send_unchanged(self):
        """fix1 BQ-01: 手本を参照しない従来の生成（note == ""）は現状のまま許可表どおり自動送信される。"""
        sent = AsyncMock(return_value=sl.SEND_SENT)
        with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=_compose("ご案内します", category="挨拶・雑談"))), \
             patch.object(cr, "save_to_approval_queue", AsyncMock(return_value="500")) as queued, \
             patch.object(cr, "get_app21_record", AsyncMock(return_value=_rec(status="受付"))):
            _run(cr.handle_customer_message(user_id=USER, user_message="こんにちは", reply_token="rt",
                                            app21_record=_rec(status="受付"), reply_func=sent))
        sent.assert_awaited_once_with("rt", "ご案内します")
        queued.assert_not_awaited()
        g = rx.require_approval_if_referenced(sp.apply_v2(cr.apply_server_guards(_compose("x"), [], "q"), "x"), "")
        self.assertTrue(g.can_auto_send)
        g2 = rx.require_approval_if_referenced(g, rx.format_note([{"id": 1, "category_id": None, "inbound_text": "", "human_reply": "r", "active": True}]))
        self.assertFalse(g2.can_auto_send)
        self.assertEqual(g2.demotion_reasons[-1], rx.EXEMPLAR_REFERENCED_REASON)

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
                                if e.startswith("chat_responder.py:")), [])          # Q3a-fix1 BQ-04: 2 件を回収（allowlist 追加ゼロ・削除のみ）
        csrc = (REPO / "chat_responder.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("reply_exemplar.prompt_note(p.name, status)" in l for l in csrc), 1)
        self.assertEqual(sum("reply_exemplar.mark_used_for(_exn, category)" in l for l in csrc), 1)
        self.assertEqual(sum("reply_exemplar.require_approval_if_referenced(guard, _exn)" in l for l in csrc), 1)   # fix1 BQ-01
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
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'b7d9f1a3c5e7'", src)   # Q1c-fix1: 永続キュー d4f6 の後ろ（鎖は 1 本）
        self.assertIn("reply_draft_approval_downgrade_refused_data_present", src)
        f = (REPO / "alembic" / "versions" / "20261011_f7b2c9d4e1a6_reply_exemplar.py").read_text(encoding="utf-8")
        self.assertIn("'deactivated', 'approved'", f)


if __name__ == "__main__":
    unittest.main()


# ── fix1 BQ-04（Codex R-Q3a・RV-10）: compose_reply の障害 2 分岐のログに生 user_id・例外本文・traceback を出さない ──
class TestBq04OutageLogging(_DbMixin):
    SECRET = "sekrit-token-9f8e7d"

    def _recv(self, exc):
        outage = AsyncMock()
        ack = AsyncMock()
        with patch.object(cr, "_call_compose_reply", AsyncMock(side_effect=exc)), \
             patch.object(cr, "handle_claude_outage", outage), \
             patch.object(sp, "outage_ack", ack), \
             patch.object(cr, "get_app21_record", AsyncMock(return_value=_rec(status="受付"))), \
             self.assertLogs("chat_responder", level="INFO") as cm:
            _run(cr.handle_customer_message(user_id=USER, user_message="q", reply_token="rt",
                                            app21_record=_rec(status="受付"), reply_func=AsyncMock()))
        return "\n".join(cm.output), outage, ack

    def test_unavailable_and_generic_failures_log_fixed_reason_only(self):
        for exc, expect_outage in ((cr.ClaudeUnavailableError(f"upstream 503 for {USER} {self.SECRET}"), True),
                                   (RuntimeError(f"rate limited {USER} {self.SECRET}"), False)):
            out, outage, ack = self._recv(exc)
            self.assertNotIn(USER, out)
            self.assertNotIn(self.SECRET, out)
            self.assertNotIn("Traceback", out)
            self.assertIn("[COMPOSE_REPLY]", out)
            self.assertNotIn(type(exc).__name__, out)                    # 固定理由だけ（型名も本文も出さない）
            self.assertEqual(outage.await_count, 1 if expect_outage else 0)
            self.assertEqual(ack.await_count, 0 if expect_outage else 1)
        src = (REPO / "chat_responder.py").read_text(encoding="utf-8")
        self.assertNotIn('logger.exception("compose_reply', src)
        self.assertEqual(src.count('[COMPOSE_REPLY] unavailable (fixed reason)'), 1)
        self.assertEqual(src.count('[COMPOSE_REPLY] failed (fixed reason)'), 1)


# ── fix1 BQ-03（Codex R-Q3a）: 文脈つき氏名の残留検出・保存拒否・承認画面での修正と確定 ──
class TestBq03ContextName(_DbMixin):
    BAD = "親族の名前はテストハナコです。"

    def test_context_name_is_detected_and_not_saved_nor_referenced(self):
        from hub import pii_scan
        self.assertIn("context_name", pii_scan.residue_hits(self.BAD))
        for s_ in ("氏名：山田太郎", "お名前は Taro Yamada です", "名前が鈴木花子でした", "お名前→ヤマダ"):
            self.assertIn("context_name", pii_scan.residue_hits(s_), s_)
        for s_ in ("お名前はカタカナでご記入ください", "お名前は確認済みです", "名前は不要です", "氏名の記載をお願いします",
                   "お名前はフルネームでお願いします", "名前は〔氏名〕様です"):
            self.assertNotIn("context_name", pii_scan.residue_hits(s_), s_)
        self.assertEqual(pii_scan.residue_hits(self.BAD, heuristics=False), [])        # 凍結定型の検査には使わない
        _, ok = pii_scan.anonymize(self.BAD)
        self.assertFalse(ok)
        self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text="q", human_reply=self.BAD,
                                                stage=rx.STAGE_CONSULT, category_id="挨拶・雑談")), "rejected")
        self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text=self.BAD, human_reply="r",
                                                stage=rx.STAGE_CONSULT, category_id="挨拶・雑談")), "rejected")
        self.assertEqual(self._rows(rx.reply_exemplar), [])                              # pending_review にも入らない
        self.assertEqual(_run(rx.prompt_note("jikou", "受付")), "")                      # 参考文例にも入らない

    def test_approval_screen_edit_is_checked_then_fixed(self):
        xid = self.save(reply="費用は【費用のご案内】のとおりです。", inbound="費用はいくらですか", approve=False)
        html = _client.get("/app/exemplars", headers=_auth()).text
        self.assertIn('name="human_reply"', html)                                       # 確認待ちには修正欄
        self.assertIn("この内容で承認する", html)
        # 残留する修正 → 承認せず・本文も置き換えない
        r = _client.post("/app/exemplars/update", headers=_auth(), follow_redirects=False,
                         data={"exemplar_id": str(xid), "action": "approve", "human_reply": self.BAD, "inbound_text": "費用はいくらですか"})
        self.assertEqual(r.headers["location"], "/app/exemplars?done=residue")
        row = _run(rx.get_exemplar(xid))
        self.assertEqual((row["status"], row["human_reply"]), ("pending_review", "費用は【費用のご案内】のとおりです。"))
        self.assertIn("匿名化が不十分です", _client.get("/app/exemplars?done=residue", headers=_auth()).text)
        self.assertNotIn("テストハナコ", _client.get("/app/exemplars", headers=_auth()).text)
        # 通る修正 → 置き換えて承認・履歴 note・参考文例は修正後の文だけ
        r = _client.post("/app/exemplars/update", headers=_auth(), follow_redirects=False,
                         data={"exemplar_id": str(xid), "action": "approve", "human_reply": "費用は〔費用のご案内〕のとおりです（修正）。",
                               "inbound_text": "費用について"})
        self.assertEqual(r.headers["location"], "/app/exemplars?done=approved")
        row = _run(rx.get_exemplar(xid))
        self.assertEqual((row["status"], row["human_reply"], row["inbound_text"]),
                         ("approved", "費用は〔費用のご案内〕のとおりです（修正）。", "費用について"))
        hist = _run(rx.exemplar_history(xid))
        self.assertEqual([h["action"] for h in hist], ["created", "approved"])
        self.assertEqual(hist[-1]["note"], "edited:human_reply,inbound_text")
        note = _run(rx.prompt_note("jikou", "受付"))
        self.assertIn("（修正）", note)
        self.assertNotIn("テストハナコ", note)
        # 承認済みは直せない（退役 → 修正 → 再承認）
        r = _client.post("/app/exemplars/update", headers=_auth(), follow_redirects=False,
                         data={"exemplar_id": str(xid), "action": "approve", "human_reply": "別の文"})
        self.assertEqual(r.headers["location"], "/app/exemplars?done=not_editable")
        self.assertEqual(_run(rx.get_exemplar(xid))["human_reply"], "費用は〔費用のご案内〕のとおりです（修正）。")
        # 欄なしの承認（従来の 1 ボタン相当）も引き続き通る
        xid2 = self.save(reply="別の手本です。", inbound="q2", approve=False)
        r = _client.post("/app/exemplars/update", headers=_auth(), follow_redirects=False,
                         data={"exemplar_id": str(xid2), "action": "approve"})
        self.assertEqual(r.headers["location"], "/app/exemplars?done=approved")
        self.assertIsNone(_run(rx.exemplar_history(xid2))[-1]["note"])
        self.assertEqual(_run(rx.update_texts(xid2, human_reply="", inbound_text=""))[0], "bad_input")
        self.assertEqual(_run(rx.update_texts(999, human_reply="x", inbound_text=""))[0], "not_found")


# ── fix1 BQ-05（Codex R-Q3a）: 閾値到達の確定（固定キー・並行 2 件で 1 者）・通知失敗の回収 ──
class TestBq05PromotionClaim(_DbMixin):
    def _queue(self, draft, rid, category="費用の定型案内"):
        with patch.object(cr.httpx, "AsyncClient", _FakeClient):
            class _R(_FakeResp):
                def json(self_inner):
                    return {"id": rid}
            with patch.object(_FakeClient, "post", AsyncMock(return_value=_R(200))):
                return _run(cr.save_to_approval_queue(user_id=USER, customer_name="n", customer_message="m",
                                                      ai_draft=draft, category=category, reason="r"))

    def _notices(self):
        return self._rows(rx.exemplar_promotion_notice)

    def test_concurrent_two_reach_threshold_but_only_one_notifies(self):
        for i in range(1, 5):
            self._queue(f"下書き {i}", rid=str(i))
        self.assertEqual(_run(sp.promotion_check("1", "下書き 1")), "none")
        self.assertEqual(_run(sp.promotion_check("2", "下書き 2")), "none")
        self.assertEqual(self._notices(), [])

        async def _both():
            return await asyncio.gather(sp.promotion_check("3", "下書き 3"), sp.promotion_check("4", "下書き 4"))
        results = _run(_both())
        self.assertEqual(sorted(results), ["none", "sent"])                     # 確定者 1・他方は none
        self.assertEqual(self.biz.await_count, 1)
        rows = self._notices()
        self.assertEqual([(r["business"], r["category"], r["threshold"], r["state"], r["attempts"]) for r in rows],
                         [("jikou", "費用の定型案内", 3, "sent", 1)])
        self.assertIsNotNone(rows[0]["notified_at"])
        self.assertEqual(_run(rx.count_unmodified("jikou", "費用の定型案内")), 4)   # 両方とも無修正として数えている
        # 5 件目以降の無修正は確定済み（固定キー）なので通知しない
        self._queue("下書き 5", rid="5")
        self.assertEqual(_run(sp.promotion_check("5", "下書き 5")), "none")
        self.assertEqual(self.biz.await_count, 1)
        self.assertEqual(len(self._notices()), 1)
        text = self.biz.await_args.args[1]
        self.assertIn("費用の定型案内", text)
        self.assertIn("3 回", text)
        self.assertNotIn("下書き 3", text)                                       # 本文は載せない（固定文言のみ）
        self.assertNotIn("下書き 4", text)

    def test_notice_failure_stays_pending_and_recover_job_resends_once(self):
        for i in range(1, 4):
            self._queue(f"下書き {i}", rid=str(i))
        _run(sp.promotion_check("1", "下書き 1"))
        _run(sp.promotion_check("2", "下書き 2"))
        self.biz.return_value = False                                             # LINE が受け付けない
        self.assertEqual(_run(sp.promotion_check("3", "下書き 3")), rp.NOTIFY_FAILED)
        rows = self._notices()
        self.assertEqual((rows[0]["state"], rows[0]["attempts"]), ("pending", 1))
        self.assertEqual(self.biz.await_count, 1)
        self.biz.return_value = True
        sp.register_jobs()
        self.assertIn(sp.run_promotion_notice_job, sl._recover_hooks)            # 回収ジョブに相乗り
        self.assertEqual(_run(sp.run_promotion_notice_job()), {"retried": 1})
        rows = self._notices()
        self.assertEqual((rows[0]["state"], rows[0]["attempts"]), ("sent", 2))
        self.assertEqual(self.biz.await_count, 2)
        self.assertEqual(_run(sp.run_promotion_notice_job()), {"retried": 0})   # 送れたら対象外
        self.assertEqual(_run(sp.send_promotion_notice("jikou", "費用の定型案内", 3)), rp.NOTIFY_THROTTLED)   # 台帳キーで 1 通
        self.assertEqual(self.biz.await_count, 2)
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            self.assertEqual(_run(sp.run_promotion_notice_job()), {"retried": 0})

    def test_table_pin(self):
        self.assertEqual(rx.PROMOTION_TABLE_NAME, "exemplar_promotion_notice")
        self.assertEqual({c.name for c in rx.exemplar_promotion_notice.columns},
                         {"business", "category", "threshold", "state", "attempts", "created_at", "notified_at"})
        self.assertEqual((rx.PROMOTION_PENDING, rx.PROMOTION_SENT), ("pending", "sent"))
        src = (REPO / "hub" / "send_policy.py").read_text(encoding="utf-8")
        self.assertNotIn("!= reply_exemplar.PROMOTION_THRESHOLD", src)          # count == 閾値の瞬間依存は無い
