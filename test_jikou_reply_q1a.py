"""JIKOU-REPLY-Q1a-SEND-BASE: 送信操作記録・会話・共通排他・送信確認待ち（§10-5・§12 Q1a）

固定する仕様:
- 既存の全送信経路（承認 webhook・画像受領・画像読取・即時定型と PENDING_REPLY・ヒアリング・
  follow・受付番号）は hub/line_channel の 2 プリミティブを通り、送信操作記録が 1 件ずつ残る
  （用途・主体・受信イベント ID・文面の版=hash。本文は保存しない）
- 同一受信イベントの再配送は受信イベント ID＋用途＋連番で検出し二重送信しない（DUPLICATE）
- 並行する 2 受信で会話は 1 つだけ作られる／同じ会話への並行送信は排他で直列化される
- 30 日超の無受信で新会話・遅着（受付時刻が直近受信より古い）で旧会話を再生成しない
- push の例外（結果不明）は unconfirmed。自動再送なし。人の確定（sent/failed・理由は閉集合）
  は send_operation_history に残る（RV-08）
- DB 未設定（DATABASE_URL なし）は記録を素通り、DB 例外時も送信は従来どおり（fail-open・
  固定語彙のログのみ・PII なし）＝相談者から見える挙動は不変
- 対応中は表と遷移関数のみ。既存経路は参照しない
- 停止判定不能の分離（§10-2 の 3 主体×8 行）は純関数・flag JIKOU_SEND_POLICY_V2=0 既定・未接続
- 送信確認待ち画面（/app/send_ops）は認証関所つき・本文/氏名/LINE userId を出さない・
  確定は native form POST（PRG 303）。承認画面はリンクのみ（参照専用 pin 不変）
- 新 module は sink AST policy 違反ゼロ・allowlist 追加ゼロ
"""

import ast
import asyncio
import datetime
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

import main  # noqa: E402
from hub import db  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import webapp_send_ops_view as sov  # noqa: E402
from hub.inbound_event import Base as InboundBase  # noqa: E402
from hub.webapp_auth import MIN_ITERATIONS, hash_password, issue_session  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
_ENV = {"WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
        "WEBAPP_SESSION_SECRET": "s" * 32}
USER = "U" + "a" * 32
USER2 = "U" + "b" * 32
JIKOU = line_channel.JIKOU_CHANNEL
_UTC = datetime.timezone.utc


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
    """line_channel.httpx.AsyncClient の記録用フェイク（POST を記録・所定応答）。"""
    responses: list = []
    calls: list = []
    raise_exc: Exception | None = None
    barrier = None            # 並行テスト用: push の直前で待ち合わせる

    def __init__(self, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def post(self, url, headers=None, json=None):
        if _FakeClient.barrier is not None:
            await _FakeClient.barrier.wait()
        _FakeClient.calls.append((url, json))
        if _FakeClient.raise_exc is not None:
            raise _FakeClient.raise_exc
        if _FakeClient.responses:
            return _FakeClient.responses.pop(0)
        return _FakeResp(200)


def _reset_fake():
    _FakeClient.responses = []
    _FakeClient.calls = []
    _FakeClient.raise_exc = None
    _FakeClient.barrier = None


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="q1a_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", **_ENV})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(sl.metadata.create_all)
                await c.run_sync(InboundBase.metadata.create_all)
        _run(_create())
        _reset_fake()
        self._cp = patch.object(line_channel.httpx, "AsyncClient", _FakeClient)
        self._cp.start()

    def tearDown(self):
        self._cp.stop()
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    # ── 読取 helper ──
    def ops(self) -> list:
        async def _q():
            from hub.db import session_scope
            async with session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(
                    sa.select(sl.send_operation).order_by(sl.send_operation.c.created_at,
                                                          sl.send_operation.c.op_id))).fetchall()]
        return _run(_q())

    def convs(self) -> list:
        async def _q():
            from hub.db import session_scope
            async with session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(
                    sa.select(sl.conversation).order_by(sl.conversation.c.conversation_id))).fetchall()]
        return _run(_q())

    def history(self, op_id: str) -> list:
        return _run(sl.operation_history(op_id))


# ── 1. 全経路で 1 件ずつ記録される ────────────────────────────────────────────
class TestRecordPerPath(_DbMixin):
    def test_push_text_default_is_reply_bot(self):
        self.assertIs(_run(line_channel.push_text(JIKOU, USER, "hello")), True)
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        op = ops[0]
        self.assertEqual((op["business"], op["channel"], op["actor"], op["purpose"], op["state"]),
                         ("jikou", "jikou", "bot", "reply", "sent"))
        self.assertTrue(op["first_reply_target"])
        self.assertIsNone(op["inbound_event_id"])
        self.assertEqual(op["text_version"], sl.text_version("hello"))
        self.assertNotIn("hello", str(op))                  # 本文は保存しない
        self.assertNotIn(USER, str(op))                      # LINE userId は hash のみ
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual([h["reason"] for h in self.history(op["op_id"])],
                         ["created", "started", "sent"])

    def test_reply_with_push_fallback_records_sent_or_failed(self):
        _run(line_channel.reply_with_push_fallback(JIKOU, "tok", USER, "a"))
        _FakeClient.responses = [_FakeResp(400, "bad"), _FakeResp(500, "x")]
        _run(line_channel.reply_with_push_fallback(JIKOU, "tok", USER, "b"))
        states = [o["state"] for o in self.ops()]
        self.assertEqual(states, ["sent", "failed"])
        self.assertEqual(len(_FakeClient.calls), 3)          # reply / reply→push

    def test_purposes_are_recorded_for_each_path(self):
        async def body():
            await sl.with_purpose("receipt_number", line_channel.reply_with_push_fallback,
                                  JIKOU, "t", USER, "r")
            await sl.with_purpose("urgent", line_channel.reply_with_push_fallback,
                                  JIKOU, "t", USER, "u")
            tok = sl.bind_inbound("evt-img")
            try:
                await sl.with_purpose("image_receipt", line_channel.push_text, JIKOU, USER, "i")
                await sl.with_purpose("image_result", line_channel.push_text, JIKOU, USER, "j")
            finally:
                sl.unbind(tok)
            tok = sl.bind_inbound("evt-follow")
            try:
                await sl.with_purpose("follow", line_channel.reply_with_push_fallback,
                                      JIKOU, "t", USER, "f")
            finally:
                sl.unbind(tok)
            async with sl.approved_draft("approval:7:3"):
                await line_channel.push_text(JIKOU, USER, "d")
        _run(body())
        ops = self.ops()
        self.assertEqual([(o["purpose"], o["actor"], o["inbound_event_id"], o["first_reply_target"])
                          for o in ops],
                         [("receipt_number", "bot", None, True), ("urgent", "bot", None, True),
                          ("image_receipt", "bot", "evt-img", True),
                          ("image_result", "bot", "evt-img", True),
                          ("follow", "bot", "evt-follow", False),
                          ("reply", "approved_draft", None, True)])
        self.assertEqual(ops[-1]["op_id"], "approval:7:3")
        self.assertTrue(all(o["state"] == "sent" for o in ops))
        self.assertEqual(len(_FakeClient.calls), 6)

    def test_unknown_purpose_is_other(self):
        _run(sl.with_purpose("not-a-purpose", line_channel.push_text, JIKOU, USER, "x"))
        self.assertEqual(self.ops()[0]["purpose"], "other")

    def test_houki_channel_is_recorded_under_its_business(self):
        with patch.dict(os.environ, {"HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "h"}):
            _run(line_channel.push_text(line_channel.HOUKI_CHANNEL, USER, "x"))
        op = self.ops()[0]
        self.assertEqual((op["business"], op["channel"]), ("souzoku-houki", "souzoku-houki"))
        self.assertNotEqual(sl.line_user_hash("jikou", USER), sl.line_user_hash("souzoku-houki", USER))


# ── 2. 同一受信イベントの再配送で二重送信なし ─────────────────────────────────
class TestRedelivery(_DbMixin):
    def _send(self, event_id, purpose="reply", text="x"):
        async def body():
            tok = sl.bind_inbound(event_id)
            try:
                return await sl.with_purpose(purpose, line_channel.push_text, JIKOU, USER, text)
            finally:
                sl.unbind(tok)
        return _run(body())

    def test_same_event_same_purpose_is_sent_once(self):
        self.assertIs(self._send("evt-1"), True)
        self.assertIs(self._send("evt-1"), True)              # 再配送: 送らずに True
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(len(self.ops()), 1)

    def test_same_event_different_purpose_is_two_operations(self):
        self._send("evt-2", "image_receipt")
        self._send("evt-2", "image_result")
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual(len(self.ops()), 2)

    def test_urgent_is_also_deduplicated_by_event(self):
        self._send("evt-3", "urgent")
        self._send("evt-3", "urgent")
        self.assertEqual(len(_FakeClient.calls), 1)

    def test_two_sends_within_one_event_get_sequence_numbers(self):
        async def body():
            tok = sl.bind_inbound("evt-4")
            try:
                await line_channel.push_text(JIKOU, USER, "a")
                await line_channel.push_text(JIKOU, USER, "b")
            finally:
                sl.unbind(tok)
        _run(body())
        self.assertEqual(sorted(o["inbound_seq"] for o in self.ops()), [1, 2])
        self.assertEqual(len(_FakeClient.calls), 2)

    def test_failed_then_redelivered_retries_without_delete(self):
        _FakeClient.responses = [_FakeResp(500, "x")]
        self._send("evt-5")
        self.assertEqual(self.ops()[0]["state"], "failed")
        self._send("evt-5")                                   # 失敗後の再配送は再試行
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["state"], ops[0]["attempts"]), ("sent", 2))
        self.assertIn("retry_after_failed", [h["reason"] for h in self.history(ops[0]["op_id"])])

    def test_approved_draft_same_operation_id_is_sent_once(self):
        async def body():
            for _ in range(2):
                async with sl.approved_draft("approval:9:1"):
                    await line_channel.push_text(JIKOU, USER, "d")
            async with sl.approved_draft("approval:9:2"):      # 再承認（版が進む）は別操作
                await line_channel.push_text(JIKOU, USER, "d")
        _run(body())
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual([o["op_id"] for o in self.ops()], ["approval:9:1", "approval:9:2"])

    def test_no_event_context_never_deduplicates(self):
        _run(line_channel.push_text(JIKOU, USER, "x"))
        _run(line_channel.push_text(JIKOU, USER, "x"))
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual(len(self.ops()), 2)


# ── 3. 会話: 並行受信で 1 つ・30 日超で新会話・遅着で再生成しない ──────────────
class TestConversation(_DbMixin):
    def test_concurrent_inbound_creates_one_conversation_and_serializes(self):
        async def body():
            barrier = asyncio.Barrier(2)

            async def one(event_id):
                tok = sl.bind_inbound(event_id)
                try:
                    await barrier.wait()
                    return await line_channel.push_text(JIKOU, USER, "x")
                finally:
                    sl.unbind(tok)
            return await asyncio.gather(one("evt-a"), one("evt-b"))
        self.assertEqual(_run(body()), [True, True])
        self.assertEqual(len(self.convs()), 1)
        ops = self.ops()
        self.assertEqual(len(ops), 2)
        # 排他の中で会話版が 1 ずつ進む（直列化＝版が重複しない）
        self.assertEqual(sorted(o["conversation_version"] for o in ops), [2, 3])
        self.assertEqual(self.convs()[0]["version"], 3)

    def test_concurrent_pushes_to_same_conversation_both_complete(self):
        async def body():
            _FakeClient.barrier = asyncio.Barrier(2)
            return await asyncio.gather(line_channel.push_text(JIKOU, USER, "a"),
                                        line_channel.push_text(JIKOU, USER, "b"))
        self.assertEqual(_run(body()), [True, True])
        self.assertEqual([o["state"] for o in self.ops()], ["sent", "sent"])
        self.assertEqual(len(self.convs()), 1)

    def _send_at(self, event_id, received_at):
        async def body():
            tok = sl.bind_inbound(event_id, received_at)
            try:
                await line_channel.push_text(JIKOU, USER, "x")
            finally:
                sl.unbind(tok)
        _run(body())

    def test_gap_over_30_days_starts_new_conversation_late_arrival_does_not(self):
        t0 = datetime.datetime(2026, 1, 1, tzinfo=_UTC)
        self._send_at("e1", t0)
        self._send_at("e2", t0 + datetime.timedelta(days=30))            # ちょうど 30 日=同じ会話
        self.assertEqual(len(self.convs()), 1)
        self._send_at("e3", t0 + datetime.timedelta(days=30, seconds=1) + datetime.timedelta(days=30))
        self.assertEqual(len(self.convs()), 2)                            # 30 日超=新会話
        last = self.convs()[1]["last_inbound_at"]
        self._send_at("e4", t0 + datetime.timedelta(days=1))              # 遅着（古い受付時刻）
        convs = self.convs()
        self.assertEqual(len(convs), 2)                                   # 旧会話を再生成しない
        self.assertEqual(convs[1]["last_inbound_at"], last)               # 直近受信も戻らない
        self.assertEqual(self.ops()[-1]["conversation_id"], convs[1]["conversation_id"])

    def test_other_user_has_its_own_conversation_and_hash(self):
        _run(line_channel.push_text(JIKOU, USER, "x"))
        _run(line_channel.push_text(JIKOU, USER2, "x"))
        convs = self.convs()
        self.assertEqual(len(convs), 2)
        self.assertEqual({c["line_user_hash"] for c in convs},
                         {sl.line_user_hash("jikou", USER), sl.line_user_hash("jikou", USER2)})
        self.assertTrue(all(len(c["ref"]) == 32 for c in convs))
        state = _run(sl.conversation_state("jikou", USER))
        self.assertEqual((state["version"], state["attending"]), (2, False))


# ── 4. 送信確認待ち: 人の確定と履歴・自動再送なし ──────────────────────────────
class TestUnconfirmed(_DbMixin):
    def _unconfirmed(self):
        _FakeClient.raise_exc = RuntimeError("transport")
        with self.assertRaises(RuntimeError):
            _run(line_channel.push_text(JIKOU, USER, "x"))
        _FakeClient.raise_exc = None
        op = self.ops()[0]
        self.assertEqual(op["state"], "unconfirmed")
        return op["op_id"]

    def test_exception_is_unconfirmed_and_human_confirms_with_reason(self):
        op_id = self._unconfirmed()
        items = _run(sl.list_unconfirmed())
        self.assertEqual(len(items), 1)
        self.assertEqual(set(items[0]), {"op_id", "business", "channel", "purpose", "actor",
                                         "started_at", "attempts", "conversation_ref"})
        self.assertNotIn(USER, str(items))
        self.assertEqual(_run(sl.count_unconfirmed()), 1)
        self.assertEqual(_run(sl.confirm_by_human(op_id, "sent", "line_delivered")), "ok")
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.assertEqual(self.ops()[0]["confirmed_by"], "human")
        self.assertEqual([h["reason"] for h in self.history(op_id)],
                         ["created", "started", "unconfirmed", "human:line_delivered"])
        self.assertEqual(_run(sl.confirm_by_human(op_id, "failed", "other")), "not_unconfirmed")
        self.assertEqual(_run(sl.list_unconfirmed()), [])
        self.assertEqual(len(_FakeClient.calls), 1)          # 自動再送なし

    def test_confirm_rejects_bad_input_and_unknown_operation(self):
        op_id = self._unconfirmed()
        self.assertEqual(_run(sl.confirm_by_human(op_id, "sent", "made-up")), "bad_input")
        self.assertEqual(_run(sl.confirm_by_human(op_id, "started", "other")), "bad_input")
        self.assertEqual(_run(sl.confirm_by_human("nope", "failed", "other")), "not_found")
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")

    def test_unconfirmed_blocks_redelivery_of_same_event(self):
        async def body():
            tok = sl.bind_inbound("evt-u")
            try:
                _FakeClient.raise_exc = RuntimeError("t")
                try:
                    await line_channel.push_text(JIKOU, USER, "x")
                except RuntimeError:
                    pass
                _FakeClient.raise_exc = None
            finally:
                sl.unbind(tok)
            tok = sl.bind_inbound("evt-u")
            try:
                return await line_channel.push_text(JIKOU, USER, "x")
            finally:
                sl.unbind(tok)
        self.assertIs(_run(body()), True)
        self.assertEqual(len(_FakeClient.calls), 1)          # 結果不明を未送信に戻さない
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")


# ── 5. 対応中（表と遷移のみ）と「人の操作後に完了」の印 ─────────────────────
class TestAttending(_DbMixin):
    def test_transitions_bump_version_and_keep_history_in_columns(self):
        st = _run(sl.set_attending("jikou", USER))
        self.assertEqual((st["version"], st["attending"]), (2, True))
        c = _run(sl.conversation_state("jikou", USER))
        self.assertTrue(c["attending"] and c["attending_since"] and not c["attending_until"])
        st = _run(sl.end_attending("jikou", USER))
        self.assertEqual((st["version"], st["attending"]), (3, False))
        c = _run(sl.conversation_state("jikou", USER))
        self.assertTrue(c["attending_until"])
        self.assertEqual(len(self.convs()), 1)

    def test_completion_after_human_operation_is_marked(self):
        async def body():
            op = await sl.begin("jikou", "jikou", USER, "x")
            await sl.set_attending("jikou", USER)             # 着手後・完了前の人の操作
            await sl.finish(op, sl.STATE_SENT)
            op2 = await sl.begin("jikou", "jikou", USER, "y")
            await sl.finish(op2, sl.STATE_SENT)
        _run(body())
        self.assertEqual([o["completed_after_human"] for o in self.ops()], [True, False])

    def test_existing_paths_do_not_consult_attending(self):
        _run(sl.set_attending("jikou", USER))
        self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)   # Q1a: 参照しない
        self.assertEqual(len(_FakeClient.calls), 1)
        for name in ("hub/line_channel.py", "main.py", "chat_responder.py",
                     "hub/image_intake.py", "hub/image_analysis.py"):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertNotIn("set_attending", src, name)
            self.assertNotIn("conversation_state", src, name)
            self.assertNotIn("decide_send", src, name)


# ── 6. fail-open: DB 未設定・DB 例外でも送信は従来どおり・PII なし ───────────
class TestFailOpen(unittest.TestCase):
    def setUp(self):
        _reset_fake()
        self._cp = patch.object(line_channel.httpx, "AsyncClient", _FakeClient)
        self._cp.start()

    def tearDown(self):
        self._cp.stop()
        db.reset_for_tests()

    def test_without_database_url_sends_and_records_nothing(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            db.reset_for_tests()
            self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)
            _run(line_channel.reply_with_push_fallback(JIKOU, "t", USER, "y"))
            self.assertIsNone(_run(sl.begin("jikou", "jikou", USER, "z")))
        self.assertEqual(len(_FakeClient.calls), 2)

    def test_database_error_sends_anyway_with_fixed_vocabulary_log(self):
        d = tempfile.mkdtemp(prefix="q1a_nodb_")
        try:
            with patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{d}/empty.db"}):
                db.reset_for_tests()                           # 表が無い DB
                with self.assertLogs("hub.send_ledger", level="WARNING") as cm:
                    self.assertIs(_run(line_channel.push_text(JIKOU, USER, "secret text")), True)
            self.assertEqual(len(_FakeClient.calls), 1)
            joined = "\n".join(cm.output)
            self.assertIn("[SEND_LEDGER] begin failed", joined)
            self.assertNotIn(USER, joined)
            self.assertNotIn("secret text", joined)
            self.assertNotIn("sqlite", joined)
        finally:
            shutil.rmtree(d, ignore_errors=True)


# ── 7. 停止判定不能の分離（純関数・未接続・flag 既定 OFF） ─────────────────────
class TestSendPolicyV2(unittest.TestCase):
    def test_flag_default_off_and_readable(self):
        with patch.dict(os.environ, {k: v for k, v in os.environ.items()
                                     if k != sl.POLICY_V2_ENV}, clear=True):
            self.assertFalse(sl.policy_v2_enabled())
        with patch.dict(os.environ, {sl.POLICY_V2_ENV: "1"}):
            self.assertTrue(sl.policy_v2_enabled())
        self.assertNotIn("policy_v2_enabled", (REPO / "main.py").read_text(encoding="utf-8"))

    def test_bot_column(self):
        self.assertTrue(sl.decide_send("bot").allow)
        for cond in ("paused", "stoplisted", "human_mode"):
            for v in (sl.STOPPED, sl.UNKNOWN):
                d = sl.decide_send("bot", **{cond: v})
                self.assertFalse(d.allow, (cond, v))
                self.assertEqual(d.reasons, (f"{cond}:{v}",))
        self.assertFalse(sl.decide_send("bot", attending=True).allow)
        d = sl.decide_send("bot", stage_known=False)
        self.assertTrue(d.allow and d.stage_independent_only)
        d = sl.decide_send("bot", paused=sl.STOPPED, stoplisted=sl.UNKNOWN)
        self.assertEqual(d.reasons, ("paused:stopped", "stoplisted:unknown"))   # 複数条件の合成

    def test_human_column(self):
        self.assertTrue(sl.decide_send("human", paused=sl.STOPPED).allow)
        self.assertTrue(sl.decide_send("human", human_mode=sl.STOPPED, attending=True).allow)
        for cond in ("paused", "stoplisted", "human_mode"):
            self.assertFalse(sl.decide_send("human", **{cond: sl.UNKNOWN}).allow)
            self.assertTrue(sl.decide_send("human", **{cond: sl.UNKNOWN},
                                           explicit_confirm=True).allow)
        self.assertFalse(sl.decide_send("human", stoplisted=sl.STOPPED).allow)
        self.assertTrue(sl.decide_send("human", stoplisted=sl.STOPPED, explicit_confirm=True).allow)
        self.assertTrue(sl.decide_send("human", stage_known=False).allow)

    def test_approved_draft_column(self):
        a = "approved_draft"
        self.assertTrue(sl.decide_send(a).allow)
        self.assertFalse(sl.decide_send(a, paused=sl.STOPPED).allow)
        self.assertTrue(sl.decide_send(a, paused=sl.STOPPED, reapproved_after_pause=True).allow)
        self.assertFalse(sl.decide_send(a, paused=sl.UNKNOWN).allow)
        self.assertTrue(sl.decide_send(a, paused=sl.UNKNOWN, explicit_confirm=True).allow)
        self.assertFalse(sl.decide_send(a, stoplisted=sl.STOPPED).allow)
        self.assertFalse(sl.decide_send(a, stoplisted=sl.UNKNOWN, explicit_confirm=True).allow)
        self.assertTrue(sl.decide_send(a, human_mode=sl.STOPPED, versions_match=True).allow)
        self.assertFalse(sl.decide_send(a, human_mode=sl.STOPPED, versions_match=False).allow)
        self.assertFalse(sl.decide_send(a, human_mode=sl.UNKNOWN).allow)
        self.assertTrue(sl.decide_send(a, attending=True, versions_match=True).allow)
        self.assertFalse(sl.decide_send(a, attending=True, versions_match=False).allow)
        self.assertTrue(sl.decide_send(a, stage_known=False).allow)
        self.assertFalse(sl.decide_send("someone").allow)
        self.assertFalse(sl.decide_send("bot", paused="maybe").allow)


# ── 8. PWA: 送信確認待ちの一覧と確定 ───────────────────────────────────────────
class TestSendOpsView(_DbMixin):
    def _unconfirmed_op(self):
        _FakeClient.raise_exc = RuntimeError("t")
        with self.assertRaises(RuntimeError):
            _run(line_channel.push_text(JIKOU, USER, "secret body"))
        _FakeClient.raise_exc = None
        return self.ops()[0]["op_id"]

    def test_unauthenticated_is_redirected_to_login(self):
        for path in ("/app/send_ops", "/app/api/send_ops/unconfirmed"):
            r = _client.get(path, follow_redirects=False)
            self.assertEqual((r.status_code, r.headers.get("location")), (303, "/app/login"), path)
        r = _client.post("/app/send_ops/confirm", data={"op_id": "x", "outcome": "sent",
                                                        "reason": "other"}, follow_redirects=False)
        self.assertEqual(r.status_code, 303)

    def test_page_lists_without_pii_and_confirm_round_trip(self):
        op_id = self._unconfirmed_op()
        r = _client.get("/app/send_ops", headers=_auth())
        self.assertEqual(r.status_code, 200)                 # catch-all より前に結線されている
        self.assertIn("送信確認待ち", r.text)
        self.assertIn(op_id, r.text)
        self.assertNotIn(USER, r.text)
        self.assertNotIn("secret body", r.text)
        self.assertNotIn(sl.line_user_hash("jikou", USER), r.text)
        self.assertEqual(r.headers.get("cache-control"), "no-store, private")
        api = _client.get("/app/api/send_ops/unconfirmed", headers=_auth()).json()
        self.assertEqual((api["ok"], api["count"]), (True, 1))
        self.assertNotIn(USER, str(api))
        r = _client.post("/app/send_ops/confirm", headers=_auth(),
                         data={"op_id": op_id, "outcome": "sent", "reason": "line_delivered"},
                         follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/send_ops?done=ok"))
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.assertEqual(self.history(op_id)[-1]["reason"], "human:line_delivered")
        r = _client.post("/app/send_ops/confirm", headers=_auth(),
                         data={"op_id": op_id, "outcome": "failed", "reason": "other"},
                         follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/send_ops?done=not_unconfirmed")
        page = _client.get("/app/send_ops?done=ok", headers=_auth())
        self.assertIn("確定しました", page.text)
        self.assertIn("送信確認待ちはありません", page.text)

    def test_bad_input_is_fixed_400_without_reflection(self):
        for data in ({"op_id": "x y", "outcome": "sent", "reason": "other"},
                     {"op_id": "x", "outcome": "retry", "reason": "other"},
                     {"op_id": "x", "outcome": "sent", "reason": "<script>"}):
            r = _client.post("/app/send_ops/confirm", headers=_auth(), data=data,
                             follow_redirects=False)
            self.assertEqual(r.status_code, 400, data)
            self.assertEqual(r.text, "")

    def test_approvals_page_links_and_keeps_reference_only_pins(self):
        src = (REPO / "webapp" / "approvals.html").read_text(encoding="utf-8")
        self.assertIn('href="/app/send_ops"', src)
        self.assertEqual(src.count("<button"), 2)
        self.assertNotIn("<form", src.lower())
        self.assertNotIn("send_ops", (REPO / "hub" / "webapp_approval_view.py").read_text(encoding="utf-8"))


# ── 9. durable lane との分担: 受信イベント ID が送信操作に乗る・再処理で二重送信なし ──
class TestDurableLaneBinding(_DbMixin):
    def test_event_id_bound_and_reattempt_does_not_resend(self):
        from hub.durable_inbound import record_line_event

        async def fake_process(reply_token, user_id, user_text):
            await main._line_reply_with_fallback(reply_token, user_id, "reply text")

        async def body():
            self.assertEqual(await record_line_event(
                webhook_event_id="wh-1", user_id=USER, signature_result="verified",
                payload=b"{}", event_type="message"), "new")
            with patch.object(main, "_process_line_event", fake_process):
                await main._process_line_event_durable("tok", USER, "hi", "wh-1")
                # 再処理（reattempt 所有権つき）でも送信操作記録が同じ受信イベントを検出
                await main._process_line_event_durable("tok", USER, "hi", "wh-1",
                                                       already_claimed=True)
        _run(body())
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["inbound_event_id"], ops[0]["purpose"], ops[0]["state"]),
                         ("wh-1", "reply", "sent"))
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertIsNone(sl.current_context())             # 文脈が漏れない


# ── 10. PII・sink policy・allowlist・接続の形 ────────────────────────────────────
class TestPiiAndSinkPolicy(unittest.TestCase):
    NEW_FILES = ("hub/send_ledger.py", "hub/webapp_send_ops_view.py")

    def test_new_modules_have_zero_sink_violations_and_no_allowlist_entries(self):
        for name in self.NEW_FILES + ("hub/line_channel.py",):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        self.assertNotIn("send_ledger", allow)
        self.assertNotIn("send_ops", allow)
        self.assertNotIn("line_channel", allow)

    def test_send_ledger_logs_only_constant_strings_and_has_no_io_imports(self):
        tree = ast.parse((REPO / "hub" / "send_ledger.py").read_text(encoding="utf-8"))
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and isinstance(node.func.value, ast.Name) and node.func.value.id == "logger":
                self.assertEqual(len(node.args), 1, "logger は固定文言 1 引数のみ")
                self.assertIsInstance(node.args[0], ast.Constant)
        for banned in ("httpx", "kintone", "anthropic", "requests"):
            self.assertNotIn(banned, imported)

    def test_view_module_has_no_kintone_or_network(self):
        src = (REPO / "hub" / "webapp_send_ops_view.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(a.name.split(".")[0] for a in node.names)
            elif isinstance(node, ast.ImportFrom):
                imported.add((node.module or "").split(".")[0])
        for banned in ("httpx", "requests", "urllib", "os", "subprocess", "kintone"):
            self.assertNotIn(banned, imported)
        # kintone 属性参照・書込 API 名がコード上に無い（docstring は対象外＝AST の Name/Attribute）
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        self.assertNotIn("kintone", names | attrs)
        for verb in ("create_record", "update_record", "delete_record", "post", "put"):
            self.assertNotIn(verb, attrs)
        gated = [r for r in sov.router.routes if hasattr(r, "endpoint")]
        self.assertEqual(len(gated), 3)
        for r in gated:
            self.assertTrue(getattr(r.endpoint, "__webapp_gate__", False), r.path)

    def test_main_hooks_are_one_line_replacements_and_tail_only(self):
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        tagged = [i + 1 for i, l in enumerate(src) if "JIKOU-REPLY-Q1a" in l]
        self.assertGreaterEqual(len(tagged), 6)
        # sink allowlist の main.py 番地（661/683/1318/1618/1915）は不変
        import json
        entries = json.load(open(REPO / "redaction_sink_allowlist.json", encoding="utf-8"))["entries"]
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries if e.startswith("main.py:")),
                         [661, 683, 1318, 1618, 1915])
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries
                                if e.startswith("chat_responder.py:")), [1660, 1672])

    def test_closed_sets_are_pinned(self):
        self.assertEqual(sl.ACTORS, ("bot", "human", "approved_draft"))
        self.assertEqual(sl.PURPOSES, ("reply", "first_reply", "urgent", "image_receipt",
                                       "image_result", "follow", "receipt_number", "other"))
        self.assertEqual(sl.STATES, ("pending", "started", "sent", "unconfirmed", "failed"))
        self.assertEqual(sl.HUMAN_REASONS, ("line_delivered", "line_not_delivered",
                                            "customer_confirmed", "other"))
        self.assertEqual(sl.FIRST_REPLY_EXCLUDED, frozenset({"follow"}))
        self.assertEqual(sl.CONVERSATION_GAP, datetime.timedelta(days=30))
        self.assertEqual(sl.TABLE_NAMES, ("conversation", "send_operation", "send_operation_history"))
        self.assertEqual(len(sl.line_user_hash("jikou", USER)), 64)
        self.assertEqual(sl.line_user_hash("jikou", USER), sl.line_user_hash("jikou", USER))


if __name__ == "__main__":
    unittest.main()
