"""JIKOU-REPLY-Q1b: 大野の返信欄（PWA・明示送信・actor=human）・App 28 human 行・履歴の human→assistant・
手本の匿名化保存・許可表の human 列

固定する仕様（QUEUE-2026-10-11-B Q-3 (a)〜(g)）:
- 返信画面 /app/reply/{App 29 record_id}（GET）と /app/reply/{record_id}/send（POST）は認証関所（_gate）
  必須。未認証は 303 → /app/login（PWA 共通の固定応答・本文なし・外部作用なし）
- 送信は send_ledger を actor=human・purpose=reply・操作 ID 固定で経由（記録 1 件・履歴 created/started/sent）。
  同じ操作 ID の再送信は副作用なし（二重送信・二重記録なし）。通信例外は unconfirmed＝/app/send_ops に
  「大野」として表示（自動再送なし）。非 2xx は failed（App 28 記録なし）
- 明示確認チェックが無ければ送らない。宛先不明（App 29 不在・line_user_id 不正）は送らない
- App 28: 送信成功後に role=human・category=human_reply・auto_sent=no。role に human の選択肢が無ければ
  失敗にせず件数ログで skip（選択肢追加後に有効）
- 履歴組立: get_recent_chat_history / main._normalize_history は human を assistant に変換し、
  顧客対応経路の system prompt に注記 1 行（human 行が無ければ従来と byte 同一）
- 手本: 氏名・住所・電話・メール・口座・郵便番号・7 桁以上の数字列を伏せ字、金額と業者名は保持。
  残留があれば保存しない。DB 未設定は保存しない
- 許可表 (f): human 列のみ適用（明示確認あり→全場面で可・無し→不可）
- 承認画面は参照専用 pin（button 2・input 1・form なし・fetch は参照 API のみ）を維持しつつ返信リンクを持つ
- 新 module は sink AST policy 違反ゼロ・allowlist 追加ゼロ・main の結線は末尾追記のみ（pin 番地不変）
"""

import ast
import asyncio
import datetime
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
from hub import db  # noqa: E402
from hub import kintone  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_reply_view as rv  # noqa: E402
from hub.inbound_event import Base as InboundBase  # noqa: E402
from hub.webapp_auth import MIN_ITERATIONS, hash_password, issue_session  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
_ENV = {"WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
        "WEBAPP_SESSION_SECRET": "s" * 32,
        "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d"}
USER = "U" + "a" * 32
OP = "f" * 32
OP2 = "e" * 32
REC_ID = "123"
PAGE = f"/app/reply/{REC_ID}"
SEND = f"{PAGE}/send"
_FIELDS_WITH_HUMAN = {"role": {"type": "RADIO_BUTTON",
                               "options": {"user": {}, "assistant": {}, "human": {}}}}
_FIELDS_WITHOUT_HUMAN = {"role": {"type": "RADIO_BUTTON", "options": {"user": {}, "assistant": {}}}}


def _rec(**fields):
    return {k: {"value": v} for k, v in fields.items()}


def _approval(**over):
    base = dict(line_user_id=USER, 顧客名="試験 太郎", 顧客メッセージ="アコムから督促が来ました。電話は090-1234-5678です",
                AI下書き="下書き本文", カテゴリ="費用の定型案内")
    base.update(over)
    return _rec(**base)


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


class _FakeClient:
    responses: list = []
    calls: list = []
    raise_exc: Exception | None = None

    def __init__(self, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeClient.calls.append((url, json))
        if _FakeClient.raise_exc is not None:
            raise _FakeClient.raise_exc
        if _FakeClient.responses:
            return _FakeClient.responses.pop(0)
        return _FakeResp(200)


def _form(op=OP, text="お返事します。費用は44,000円です。", confirm="1", exemplar="1"):
    data = {"op_id": op, "text": text}
    if confirm:
        data["confirm"] = confirm
    if exemplar:
        data["exemplar"] = exemplar
    return data


class _DbMixin(unittest.TestCase):
    """sqlite DB（send_ledger・reply_exemplar）＋ LINE/kintone のフェイク。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="q1b_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", **_ENV})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(sl.metadata.create_all)
                await c.run_sync(rx.metadata.create_all)
                await c.run_sync(InboundBase.metadata.create_all)
        _run(_create())
        _FakeClient.responses, _FakeClient.calls, _FakeClient.raise_exc = [], [], None
        self.get_record = AsyncMock(return_value=_approval())
        self.search = AsyncMock(return_value=[])
        self.form_fields = AsyncMock(return_value=_FIELDS_WITH_HUMAN)
        self.create = AsyncMock(return_value="900")
        self.app21 = AsyncMock(return_value=_rec(status="受任", 問い合わせ業者名="アコム"))
        self._patches = [
            patch.object(line_channel.httpx, "AsyncClient", _FakeClient),
            patch.object(kintone, "get_record", self.get_record),
            patch.object(kintone, "search_records", self.search),
            patch.object(kintone, "get_form_fields", self.form_fields),
            patch.object(kintone, "create_record", self.create),
            patch.object(cr, "get_app21_record", self.app21),
        ]
        for p in self._patches:
            p.start()
        rv._skip_count["role_option_missing"] = 0
        main.conversation_histories.pop(USER, None)

    def tearDown(self):
        for p in self._patches:
            p.stop()
        main.conversation_histories.pop(USER, None)
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def ops(self) -> list:
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(
                    sa.select(sl.send_operation))).fetchall()]
        return _run(_q())

    def exemplars(self) -> list:
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(
                    sa.select(rx.reply_exemplar))).fetchall()]
        return _run(_q())

    def post(self, data=None, rec_id=REC_ID):
        return _client.post(f"/app/reply/{rec_id}/send", headers=_auth(),
                            data=(data if data is not None else _form()), follow_redirects=False)


# ── 1. 認証（(g) 認証なしの返信） ──────────────────────────────────────────────
class TestAuth(unittest.TestCase):
    def test_unauthenticated_get_and_post_are_rejected_without_side_effects(self):
        get_record = AsyncMock(return_value=_approval())
        with patch.dict(os.environ, _ENV), patch.object(kintone, "get_record", get_record), \
                patch.object(line_channel.httpx, "AsyncClient", _FakeClient):
            _FakeClient.calls = []
            r = _client.get(PAGE, follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/login"))
            r = _client.post(SEND, data=_form(), follow_redirects=False)
            self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/login"))
            self.assertEqual(r.text, "")
        get_record.assert_not_awaited()
        self.assertEqual(_FakeClient.calls, [])

    def test_routes_are_gated_and_closed(self):
        routes = [r for r in rv.router.routes if hasattr(r, "endpoint")]
        self.assertEqual({(r.path, tuple(r.methods)) for r in routes},
                         {("/app/reply/{record_id}", ("GET",)), ("/app/reply/{record_id}/send", ("POST",))})
        for r in routes:
            self.assertTrue(getattr(r.endpoint, "__webapp_gate__", False), r.path)

    def test_bad_record_id_is_fixed_400_without_reflection(self):
        with patch.dict(os.environ, _ENV):
            for bad in ("x", "1;drop", "0" * 13):
                r = _client.get(f"/app/reply/{bad}", headers=_auth(), follow_redirects=False)
                self.assertEqual((r.status_code, r.text), (400, ""))
                r = _client.post(f"/app/reply/{bad}/send", headers=_auth(), data=_form(),
                                 follow_redirects=False)
                self.assertEqual((r.status_code, r.text), (400, ""))


# ── 2. 送信の往復（(a)(b)(d)(f)） ───────────────────────────────────────────────
class TestExplicitSend(_DbMixin):
    def test_page_renders_without_line_user_id_and_with_fresh_op_id(self):
        self.search.return_value = [
            _rec(role="user", message="こんにちは", 作成日時="2026-10-11T01:00:00Z", category=""),
            _rec(role="human", message="事務所からの返信", 作成日時="2026-10-11T02:00:00Z", category="human_reply"),
        ]
        r = _client.get(PAGE, headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertNotIn(USER, r.text)
        self.assertIn("試験 太郎", r.text)
        self.assertIn("事務所（大野）", r.text)
        self.assertIn("下書き本文", r.text)
        self.assertIn('name="confirm"', r.text)
        self.assertIn('name="exemplar" value="1" checked', r.text)
        import re
        op_ids = re.findall(r'name="op_id" value="([0-9a-f]{32})"', r.text)
        self.assertEqual(len(op_ids), 1)
        self.assertEqual(r.headers["cache-control"], "no-store, private")

    def test_send_records_human_operation_chatlog_and_exemplar(self):
        r = self.post()
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"{PAGE}?done=sent&log=written&ex=saved"))
        # LINE push 1 回（宛先はサーバ内で解決した line_user_id）
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(_FakeClient.calls[0][1]["to"], USER)
        # send_ledger: actor=human・purpose=reply・op_id 固定・sent
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        op = ops[0]
        self.assertEqual((op["op_id"], op["actor"], op["purpose"], op["state"], op["business"]),
                         (OP, "human", "reply", "sent", "jikou"))
        self.assertIsNone(op["inbound_event_id"])
        self.assertEqual(op["text_version"], sl.text_version(_form()["text"]))
        self.assertEqual([h["reason"] for h in _run(sl.operation_history(OP))], ["created", "started", "sent"])
        # App 28: role=human・category=human_reply・auto_sent=no
        self.form_fields.assert_awaited_once()
        self.create.assert_awaited_once()
        app, fields = self.create.await_args.args
        self.assertEqual(app.app_id_env, "APP_CHATLOG")
        self.assertEqual(fields, {"line_user_id": USER, "role": "human", "message": _form()["text"],
                                  "category": "human_reply", "auto_sent": "no"})
        # 手本: 匿名化済み・業者名保持・段階=受任後・op_id 紐付け
        ex = self.exemplars()
        self.assertEqual(len(ex), 1)
        self.assertEqual((ex[0]["business"], ex[0]["stage"], ex[0]["creditor_name"], ex[0]["source_op_id"]),
                         ("jikou", "受任後", "アコム", OP))
        self.assertIn("44,000円", ex[0]["human_reply"])
        self.assertIn("アコム", ex[0]["inbound_text"])
        self.assertNotIn("090-1234-5678", ex[0]["inbound_text"])
        self.assertIn(rx.MASK_PHONE, ex[0]["inbound_text"])
        self.assertIsNone(ex[0]["conversation_ref"])          # 受信が無ければ会話は無い（作らない）

    def test_same_operation_id_is_rejected_without_side_effects(self):
        self.post()
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=duplicate_sent")
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(len(self.ops()), 1)
        self.create.assert_awaited_once()
        self.assertEqual(len(self.exemplars()), 1)
        # 別の操作 ID なら新しい送信（画面を開き直した場合）
        r = self.post(_form(op=OP2))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=saved")
        self.assertEqual(len(self.ops()), 2)

    def test_missing_confirm_or_empty_text_does_not_send(self):
        r = self.post(_form(confirm=""))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=confirm_required")
        r = self.post(_form(text="   "))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=bad_input")
        r = self.post(_form(text="あ" * (rv.MAX_TEXT_CHARS + 1)))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=bad_input")
        r = _client.post(SEND, headers=_auth(), data=_form(op="not-hex"), follow_redirects=False)
        self.assertEqual((r.status_code, r.text), (400, ""))
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])
        self.create.assert_not_awaited()

    def test_unknown_or_invalid_destination_does_not_send(self):
        self.get_record.side_effect = kintone.KintoneError(404, "GAIA_RE01", "nf")
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=not_found")
        self.get_record.side_effect = None
        self.get_record.return_value = _approval(line_user_id="not-a-line-id")
        r = self.post(_form(op=OP2))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=not_found")
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])

    def test_line_non_2xx_is_failed_without_chatlog_or_exemplar(self):
        _FakeClient.responses = [_FakeResp(400, "bad")]
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=failed")
        self.assertEqual(self.ops()[0]["state"], "failed")
        self.create.assert_not_awaited()
        self.assertEqual(self.exemplars(), [])

    def test_exception_is_unconfirmed_and_listed_in_send_ops_as_human(self):
        _FakeClient.raise_exc = RuntimeError("boom")
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=unconfirmed")
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        self.create.assert_not_awaited()
        # 送信確認待ち画面に「大野」の返信として表示（自動再送なし）
        page = _client.get("/app/send_ops", headers=_auth())
        self.assertIn("大野", page.text)
        self.assertIn(OP, page.text)
        self.assertEqual(_client.get("/app/api/send_ops/unconfirmed", headers=_auth()).json()["count"], 1)
        # 同じ操作 ID の再送信は「送信確認待ち」として弾く（再送しない）
        _FakeClient.raise_exc = None
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=duplicate_unconfirmed")
        self.assertEqual(len(_FakeClient.calls), 1)
        # 人の確定で sent にできる（既存の経路）
        r = _client.post("/app/send_ops/confirm", headers=_auth(),
                         data={"op_id": OP, "outcome": "sent", "reason": "line_delivered"},
                         follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/send_ops?done=ok")

    def test_chatlog_skipped_with_count_when_role_option_missing(self):
        self.form_fields.return_value = _FIELDS_WITHOUT_HUMAN
        with self.assertLogs("hub.webapp_reply_view", level="INFO") as cm:
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=skipped&ex=saved")
        self.create.assert_not_awaited()
        self.assertEqual(rv._skip_count["role_option_missing"], 1)
        self.assertTrue(any("role option missing" in m for m in cm.output))
        self.assertFalse(any(USER in m or "お返事" in m for m in cm.output))
        self.post(_form(op=OP2))
        self.assertEqual(rv._skip_count["role_option_missing"], 2)
        # 選択肢が追加されれば（再配備なしで）記録される
        self.form_fields.return_value = _FIELDS_WITH_HUMAN
        r = self.post(_form(op="d" * 32))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=saved")
        self.create.assert_awaited_once()

    def test_chatlog_failure_does_not_undo_send(self):
        self.form_fields.side_effect = RuntimeError("kintone down")
        with self.assertLogs("hub.webapp_reply_view", level="WARNING") as cm:
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=failed&ex=saved")
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.assertTrue(any("write failed (fixed reason)" in m for m in cm.output))
        self.assertFalse(any(USER in m for m in cm.output))

    def test_exemplar_opt_out_and_rejection_do_not_block_send(self):
        r = self.post(_form(exemplar=""))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=off")
        self.assertEqual(self.exemplars(), [])
        with patch.object(rx, "_RESIDUE_RES", rx._RESIDUE_RES + (__import__("re").compile("お返事"),)):
            r = self.post(_form(op=OP2))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=rejected")
        self.assertEqual(self.exemplars(), [])

    def test_exemplar_links_conversation_ref_when_conversation_exists(self):
        _run(sl.touch_inbound("jikou", USER, "evt-1"))
        self.post()
        ex = self.exemplars()
        state = _run(sl.conversation_state("jikou", USER))
        self.assertEqual(ex[0]["conversation_ref"], state["ref"])
        self.assertEqual(self.ops()[0]["conversation_id"], state["conversation_id"])

    def test_memory_history_reflected_as_assistant_only_when_present(self):
        self.post()
        self.assertNotIn(USER, main.conversation_histories)       # 履歴が無ければ作らない
        main.conversation_histories[USER] = [{"role": "user", "content": "質問"}]
        self.post(_form(op=OP2))
        self.assertEqual(main.conversation_histories[USER],
                         [{"role": "user", "content": "質問"},
                          {"role": "assistant", "content": _form()["text"]}])
        self.post(_form(op="c" * 32))                               # 連続は結合（契約: 交互）
        self.assertEqual(len(main.conversation_histories[USER]), 2)
        self.assertEqual(main.conversation_histories[USER][-1]["content"].count(_form()["text"]), 2)

    def test_stage_consult_when_pre_engagement(self):
        self.app21.return_value = _rec(status="決済完了", 問い合わせ業者名="プロミス")
        self.post()
        self.assertEqual((self.exemplars()[0]["stage"], self.exemplars()[0]["creditor_name"]),
                         ("相談", "プロミス"))

    def test_without_database_sends_and_skips_exemplar(self):
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            os.environ.pop("DATABASE_URL", None)
            db.reset_for_tests()
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=skipped")
        self.assertEqual(len(_FakeClient.calls), 1)


# ── 3. 許可表 human 列（(f)） ────────────────────────────────────────────────────
class TestHumanDecision(unittest.TestCase):
    def test_explicit_confirm_allows_in_all_situations(self):
        d = sl.human_explicit_decision(True)
        self.assertTrue(d.allow)
        self.assertEqual(d.reasons, ())
        for paused in sl.CONDITION_VALUES:
            for stop in sl.CONDITION_VALUES:
                for hm in sl.CONDITION_VALUES:
                    for att in (False, True):
                        self.assertTrue(sl.decide_send(sl.ACTOR_HUMAN, paused=paused, stoplisted=stop,
                                                       human_mode=hm, attending=att,
                                                       explicit_confirm=True).allow)

    def test_without_confirm_is_denied_with_fixed_reasons(self):
        d = sl.human_explicit_decision(False)
        self.assertFalse(d.allow)
        self.assertEqual(d.reasons, ("paused:unknown:explicit_confirm_required",
                                     "stoplisted:unknown:explicit_confirm_required",
                                     "human_mode:unknown:explicit_confirm_required"))

    def test_bot_and_approved_draft_columns_unchanged(self):
        self.assertFalse(sl.decide_send(sl.ACTOR_BOT, attending=True).allow)
        self.assertFalse(sl.policy_v2_enabled())


# ── 4. 履歴組立（(c)） ──────────────────────────────────────────────────────────
class TestHistoryConversion(unittest.TestCase):
    def test_model_history_converts_human_and_drops_unknown(self):
        h = cr.model_history([{"role": "user", "content": "a"}, {"role": "human", "content": "b"},
                              {"role": "assistant", "content": "c"}, {"role": "system", "content": "x"}])
        self.assertEqual(list(h), [{"role": "user", "content": "a"}, {"role": "assistant", "content": "b"},
                                   {"role": "assistant", "content": "c"}])
        self.assertEqual(h.human_rows, 1)
        self.assertIsInstance(h, list)
        self.assertEqual(cr.human_history_note(h), cr.HUMAN_HISTORY_NOTE)
        self.assertEqual(cr.human_history_note([]), "")
        self.assertEqual(cr.human_history_note(cr.model_history([{"role": "user", "content": "a"}])), "")
        self.assertEqual((cr.model_role("human"), cr.model_role("user"), cr.model_role(None)),
                         ("assistant", "user", None))

    def test_get_recent_chat_history_converts_human_rows(self):
        class _Resp:
            is_success = True

            @staticmethod
            def json():
                return {"records": [
                    {"role": {"value": "human"}, "message": {"value": "事務所の返信"}},
                    {"role": {"value": "user"}, "message": {"value": "質問"}},
                ]}

        class _Client:
            def __init__(self, *a, **k):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, *a, **k):
                return _Resp()
        with patch.object(cr.httpx, "AsyncClient", _Client), \
                patch.object(cr, "APP_CHATLOG", "28"), patch.object(cr, "TOKEN_CHATLOG", "d"):
            history = _run(cr.get_recent_chat_history(USER))
        self.assertEqual(list(history), [{"role": "user", "content": "質問"},
                                         {"role": "assistant", "content": "事務所の返信"}])
        self.assertEqual(history.human_rows, 1)

    def test_normalize_history_keeps_human_as_assistant(self):
        out = main._normalize_history([{"role": "user", "content": "q"}, {"role": "human", "content": "h"},
                                       {"role": "assistant", "content": "a"}, {"role": "other", "content": "x"}])
        self.assertEqual(out, [{"role": "user", "content": "q"}, {"role": "assistant", "content": "h\na"}])

    def test_customer_prompt_gets_note_only_with_human_rows(self):
        from claude_gateway import ClaudeUnavailableError

        def _system_prompt_for(history):
            call = AsyncMock(side_effect=ClaudeUnavailableError("down"))
            kwargs = dict(user_id="Unote", user_message="質問です", reply_token="tok",
                          app21_record={"status": {"value": "決済完了"}}, reply_func=AsyncMock())
            with patch.object(cr, "_call_compose_reply", call), \
                    patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=history)), \
                    patch.object(cr, "save_to_chatlog", AsyncMock()), \
                    patch.object(cr, "save_to_approval_queue", AsyncMock(return_value="q")), \
                    patch.object(cr, "_notify_attorney", AsyncMock()):
                asyncio.run(cr.handle_customer_message(**kwargs))
            return call.await_args.args[0], call.await_args.args[1]

        plain, msgs = _system_prompt_for([{"role": "assistant", "content": "a"}])
        self.assertNotIn(cr.HUMAN_HISTORY_NOTE, plain)
        with_h = cr.model_history([{"role": "human", "content": "事務所の返信"}])
        noted, msgs2 = _system_prompt_for(with_h)
        self.assertTrue(noted.endswith(cr.HUMAN_HISTORY_NOTE))
        self.assertEqual(noted[:-len(cr.HUMAN_HISTORY_NOTE)], plain)
        self.assertEqual([m["role"] for m in msgs2], ["assistant", "user"])
        self.assertEqual(cr.build_system_prompt(status="決済完了"),
                         cr.build_system_prompt(status="決済完了", human_note=""))
        self.assertEqual(cr.HUMAN_HISTORY_NOTE.count("\n"), 2)   # 空行 + 注記 1 行


# ── 5. 手本の匿名化（(d)(g)） ─────────────────────────────────────────────────
class TestAnonymize(unittest.TestCase):
    def test_masks_pii_and_keeps_amounts_and_creditor(self):
        text = ("試験太郎様、試験 太郎です。アコムの件、費用は44,000円（3社で110,000円）です。"
                "住所は東京都千代田区霞が関1-1-1 ビル3階、電話は03-1234-5678、"
                "メールは taro@example.com、口座番号 1234567、〒100-0001、会員番号 12345678901 です。")
        out, ok = rx.anonymize(text, names=("試験 太郎",))
        self.assertTrue(ok, out)
        for keep in ("アコム", "44,000円", "110,000円", "3社"):
            self.assertIn(keep, out)
        for gone in ("試験太郎", "試験 太郎", "霞が関", "03-1234-5678", "taro@example.com",
                     "1234567", "100-0001", "12345678901"):
            self.assertNotIn(gone, out)
        for mask in (rx.MASK_NAME, rx.MASK_ADDRESS, rx.MASK_PHONE, rx.MASK_EMAIL,
                     rx.MASK_ACCOUNT, rx.MASK_POSTAL, rx.MASK_DIGITS):
            self.assertIn(mask, out)
        self.assertIn("口座番号 " + rx.MASK_ACCOUNT, out)

    def test_name_parts_and_single_char_safety(self):
        out, _ = rx.anonymize("太郎さんへ。林さんは別人です。", names=("試験 太郎", "林 一"))
        self.assertNotIn("太郎", out)
        self.assertIn("林さん", out)                      # 1 文字の姓は伏せない（誤伏せ防止）
        out, _ = rx.anonymize("（未登録）の方へ", names=("（未登録）",))
        self.assertEqual(out, "（未登録）の方へ")

    def test_creditor_with_prefecture_name_is_not_treated_as_address(self):
        out, ok = rx.anonymize("千葉県信用金庫からの督促です", names=())
        self.assertEqual(out, "千葉県信用金庫からの督促です")
        self.assertTrue(ok)

    def test_save_without_db_and_bad_input(self):
        with patch.dict(os.environ, {}):
            os.environ.pop("DATABASE_URL", None)
            db.reset_for_tests()
            self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text="x", human_reply="y")),
                             "skipped")
            self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text="x", human_reply="",
                                                   stage="相談")), "bad_input")
            self.assertEqual(_run(rx.save_exemplar(business="jikou", inbound_text="x", human_reply="y",
                                                   stage="不明")), "bad_input")

    def test_closed_sets(self):
        self.assertEqual(rx.STAGES, ("相談", "受任後"))
        self.assertEqual(rx.TABLE_NAME, "reply_exemplar")
        self.assertEqual({c.name for c in rx.reply_exemplar.columns},
                         {"id", "business", "conversation_ref", "inbound_text", "human_reply",
                          "creditor_name", "stage", "created_at", "used_count", "source_op_id"})


# ── 6. pin（承認画面・sink policy・allowlist・main 結線） ─────────────────────────
class TestPins(unittest.TestCase):
    NEW_FILES = ("hub/reply_exemplar.py", "hub/webapp_reply_view.py")

    def test_approvals_page_links_to_reply_and_keeps_reference_only_pins(self):
        src = (REPO / "webapp" / "approvals.html").read_text(encoding="utf-8")
        self.assertIn('"/app/reply/"', src)
        self.assertIn("この相談者に返信する", src)
        self.assertEqual(src.count("<button"), 2)
        self.assertEqual(len(__import__("re").findall(r"<input\b[^>]*>", src)), 1)
        self.assertNotIn("<form", src.lower())
        self.assertNotIn("line_user_id", src)
        self.assertNotIn("reply", (REPO / "hub" / "webapp_approval_view.py").read_text(encoding="utf-8").lower()
                         .replace("承認", ""))

    def test_new_modules_have_zero_sink_violations_and_no_allowlist_entries(self):
        for name in self.NEW_FILES + ("hub/send_ledger.py", "hub/line_channel.py"):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for key in ("reply_exemplar", "webapp_reply_view"):
            self.assertNotIn(key, allow)
        entries = json.loads(allow)["entries"]
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries if e.startswith("main.py:")),
                         [663, 685, 1320, 1620, 1917])
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries
                                if e.startswith("chat_responder.py:")), [1660, 1672])

    def test_view_module_imports_are_closed(self):
        src = (REPO / "hub" / "webapp_reply_view.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        for banned in ("httpx", "requests", "urllib", "os", "subprocess", "anthropic"):
            self.assertNotIn(banned, imported)
        # kintone 書込は App 28 の create_record のみ（update/delete は無い）
        attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        self.assertIn("create_record", attrs)
        for verb in ("update_record", "delete_record", "upload_file", "_write"):
            self.assertNotIn(verb, attrs)

    def test_main_wiring_is_tail_only_and_before_catch_all(self):
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        tagged = [i for i, l in enumerate(src) if "JIKOU-REPLY-Q1b" in l]
        self.assertGreaterEqual(len(tagged), 3)
        self.assertIn("include_before_catch_all(app, reply_view_router)", "\n".join(src[-6:]))
        from hub.webapp_brain_view import _paths_of
        paths = [p for entry in main.app.router.routes for p in _paths_of(entry)
                 if p in ("/app/reply/{record_id}", "/app/{_rest:path}")]
        self.assertEqual(paths, ["/app/reply/{record_id}", "/app/{_rest:path}"])

    def test_migration_revision_chains_after_send_ledger(self):
        p = REPO / "alembic" / "versions" / "20261011_f7b2c9d4e1a6_reply_exemplar.py"
        src = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'f7b2c9d4e1a6'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'e5f8a1b4c7d9'", src)
        self.assertIn("reply_exemplar_metadata", (REPO / "alembic" / "env.py").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
