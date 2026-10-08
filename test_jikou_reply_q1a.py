"""JIKOU-REPLY-Q1a-SEND-BASE（+fix1 BQ-01〜05）: 送信操作記録・会話・共通排他・送信確認待ち

固定する仕様:
- 既存の全送信経路（承認 webhook・画像受領・画像読取・即時定型と PENDING_REPLY・ヒアリング・
  follow・受付番号）は hub/line_channel の 2 プリミティブを通り、送信操作記録が 1 件ずつ残る
  （用途・主体・受信イベント ID・文面の版=hash。本文は保存しない）
- 送信フックの戻り値は 3 値（BQ-01）: push_text は True / SEND_UNCONFIRMED（結果未確認の重複）/ False、
  reply_with_push_fallback は sent / unconfirmed / failed。結果未確認の重複は成功として扱わず、
  既存呼出元は失敗側（受領未記録・送信済更新なし）に倒す。自動再送なし
- 同一受信イベントの再配送は受信イベント ID＋用途＋連番で検出し二重送信しない
- 会話の取得/作成と直近受信時刻の更新は受信側 `touch_inbound()`（BQ-03）: 自動返信しない受信でも
  更新・再配送では更新しない・受信を伴わない送信では会話を作らない（会話 ID NULL で記録）・
  30 日超の新会話は前会話の attending を引き継ぐ
- 滞留 started（既定 10 分）は回収ジョブが unconfirmed に移す（履歴 stale_started・BQ-02）。
  一覧は unconfirmed と滞留 started を表示し、人の確定は両方を受け付ける
- human_version は人の操作でのみ進み、completed_after_human は着手時/完了時の差で判定（BQ-04）
- 人の確定は条件付き UPDATE の更新件数 1 のときだけ履歴を追加して ok（BQ-05・競合側は履歴なし）
- 非 Postgres の排他代替は sqlite だけ（他方言は例外）
- DB 未設定は記録を素通り、DB 例外でも送信は従来どおり（fail-open・固定語彙のログ・PII なし）
- 停止判定不能の分離（§10-2）は純関数・flag JIKOU_SEND_POLICY_V2=0 既定・未接続
- 送信確認待ち画面（/app/send_ops）は認証関所つき・本文/氏名/LINE userId を出さない
- 新 module は sink AST policy 違反ゼロ・allowlist 追加ゼロ
"""

import ast
import asyncio
import datetime
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

import main  # noqa: E402
from hub import db  # noqa: E402
from hub import image_intake  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import scheduler as hub_scheduler  # noqa: E402
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
T0 = datetime.datetime(2026, 1, 1, tzinfo=_UTC)


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

    # ── helper ──
    def touch(self, event_id, received_at=None, user=USER):
        return _run(sl.touch_inbound("jikou", user, event_id, received_at=received_at))

    def send(self, event_id=None, purpose="reply", text="x", user=USER):
        async def body():
            tok = sl.bind_inbound(event_id)
            try:
                return await sl.with_purpose(purpose, line_channel.push_text, JIKOU, user, text)
            finally:
                sl.unbind(tok)
        return _run(body())

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

    def set_started_at(self, op_id: str, at, *, deadline_at="auto", heartbeat_at="auto"):
        """着手時刻を書き換える。期限・ハートビートは既定で着手時刻基準（=滞留）にする。"""
        if deadline_at == "auto":
            deadline_at = at + datetime.timedelta(minutes=sl.deadline_minutes())
        if heartbeat_at == "auto":
            heartbeat_at = at

        async def _u():
            from hub.db import session_scope
            async with session_scope() as s:
                await s.execute(sa.update(sl.send_operation).where(
                    sl.send_operation.c.op_id == op_id).values(
                    started_at=at, deadline_at=deadline_at, last_heartbeat_at=heartbeat_at))
        _run(_u())


# ── 1. 全経路で 1 件ずつ記録される ────────────────────────────────────────────
class TestRecordPerPath(_DbMixin):
    def test_push_text_default_is_reply_bot_without_conversation(self):
        self.assertIs(_run(line_channel.push_text(JIKOU, USER, "hello")), True)
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        op = ops[0]
        self.assertEqual((op["business"], op["channel"], op["actor"], op["purpose"], op["state"]),
                         ("jikou", "jikou", "bot", "reply", "sent"))
        self.assertTrue(op["first_reply_target"])
        self.assertIsNone(op["inbound_event_id"])
        self.assertIsNone(op["conversation_id"])                # BQ-03: 送信時に会話を作らない
        self.assertEqual(self.convs(), [])
        self.assertEqual(op["text_version"], sl.text_version("hello"))
        self.assertNotIn("hello", str(op))                      # 本文は保存しない
        self.assertNotIn(USER, str(op))                          # LINE userId は hash のみ
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual([h["reason"] for h in self.history(op["op_id"])],
                         ["created", "started", "sent"])

    def test_reply_with_push_fallback_returns_three_values(self):
        self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "tok", USER, "a")),
                         sl.SEND_SENT)
        _FakeClient.responses = [_FakeResp(400, "bad"), _FakeResp(500, "x")]
        self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "tok", USER, "b")),
                         sl.SEND_FAILED)
        self.assertEqual([o["state"] for o in self.ops()], ["sent", "failed"])
        self.assertEqual(len(_FakeClient.calls), 3)              # reply / reply→push

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

    def test_send_links_to_latest_conversation_when_inbound_exists(self):
        self.touch("e1", T0)
        self.send("e1")
        op = self.ops()[0]
        self.assertEqual(op["conversation_id"], self.convs()[0]["conversation_id"])
        self.assertEqual(op["conversation_version"], 2)         # 送信着手で版が進む


# ── 2. 同一受信イベントの再配送で二重送信なし（BQ-01 の 3 値） ──────────────
class TestRedelivery(_DbMixin):
    def test_same_event_same_purpose_is_sent_once_and_reported_sent(self):
        self.assertIs(self.send("evt-1"), True)
        self.assertIs(self.send("evt-1"), True)                 # 送信済みの重複: True
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(len(self.ops()), 1)

    def test_same_event_different_purpose_is_two_operations(self):
        self.send("evt-2", "image_receipt")
        self.send("evt-2", "image_result")
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual(len(self.ops()), 2)

    def test_urgent_is_also_deduplicated_by_event(self):
        self.send("evt-3", "urgent")
        self.send("evt-3", "urgent")
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
        self.assertIs(self.send("evt-5"), False)
        self.assertEqual(self.ops()[0]["state"], "failed")
        self.assertIs(self.send("evt-5"), True)                 # 失敗後の再配送は再試行
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        self.assertEqual((ops[0]["state"], ops[0]["attempt_no"]), ("sent", 2))
        self.assertIn("retry_after_failed", [h["reason"] for h in self.history(ops[0]["op_id"])])

    def test_unconfirmed_duplicate_is_reported_as_unconfirmed_not_success(self):
        _FakeClient.raise_exc = RuntimeError("t")
        with self.assertRaises(RuntimeError):
            self.send("evt-u")
        _FakeClient.raise_exc = None
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        self.assertEqual(self.send("evt-u"), sl.SEND_UNCONFIRMED)   # push_text: 成功ではない
        async def rwp():
            tok = sl.bind_inbound("evt-u")
            try:
                return await line_channel.reply_with_push_fallback(JIKOU, "t", USER, "x")
            finally:
                sl.unbind(tok)
        self.assertEqual(_run(rwp()), sl.SEND_UNCONFIRMED)
        self.assertEqual(len(_FakeClient.calls), 1)              # 結果不明を未送信に戻さない
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")

    def test_started_duplicate_is_unconfirmed(self):
        async def body():
            tok = sl.bind_inbound("evt-s")
            try:
                op = await sl.begin("jikou", "jikou", USER, "x")      # started のまま
                self.assertIsInstance(op, sl.Operation)
            finally:
                sl.unbind(tok)
            tok = sl.bind_inbound("evt-s")
            try:
                return await line_channel.push_text(JIKOU, USER, "x")
            finally:
                sl.unbind(tok)
        self.assertEqual(_run(body()), sl.SEND_UNCONFIRMED)
        self.assertEqual(len(_FakeClient.calls), 0)

    def test_approved_draft_same_operation_id_is_sent_once(self):
        async def body():
            r = []
            for _ in range(2):
                async with sl.approved_draft("approval:9:1"):
                    r.append(await line_channel.push_text(JIKOU, USER, "d"))
            async with sl.approved_draft("approval:9:2"):      # 再承認（版が進む）は別操作
                r.append(await line_channel.push_text(JIKOU, USER, "d"))
            return r
        self.assertEqual(_run(body()), [True, True, True])
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual([o["op_id"] for o in self.ops()], ["approval:9:1", "approval:9:2"])

    def test_no_event_context_never_deduplicates(self):
        _run(line_channel.push_text(JIKOU, USER, "x"))
        _run(line_channel.push_text(JIKOU, USER, "x"))
        self.assertEqual(len(_FakeClient.calls), 2)
        self.assertEqual(len(self.ops()), 2)


# ── 2b. BQ-01: 呼出元の分岐（受領未記録・送信済更新なし） ────────────────────
class TestThreeValuedCallers(_DbMixin):
    def test_image_receipt_not_recorded_when_push_is_unconfirmed_duplicate(self):
        create = AsyncMock()
        notify = AsyncMock()
        with patch.object(image_intake, "_latest_marker_row",
                          AsyncMock(return_value=("5", "画像受領:jikou:evt-x"))), \
                patch.object(image_intake, "_latest_receipt_row_id", AsyncMock(return_value="")), \
                patch.object(image_intake, "_latest_row_id_by_category", AsyncMock(return_value="")), \
                patch.object(image_intake, "push_text", AsyncMock(return_value=sl.SEND_UNCONFIRMED)), \
                patch.object(image_intake.kintone, "create_record", create), \
                patch.object(image_intake, "_notify_send_failure", notify):
            self.assertIs(_run(image_intake._send_receipt_and_close("jikou", JIKOU, USER)), False)
        create.assert_not_awaited()                              # 受領済み行を書かない
        notify.assert_awaited_once()                             # 既存の失敗側の経路

    def test_image_receipt_recorded_only_when_push_is_true(self):
        create = AsyncMock()
        with patch.object(image_intake, "_latest_marker_row",
                          AsyncMock(return_value=("5", "画像受領:jikou:evt-x"))), \
                patch.object(image_intake, "_latest_receipt_row_id", AsyncMock(return_value="")), \
                patch.object(image_intake, "_latest_row_id_by_category", AsyncMock(return_value="")), \
                patch.object(image_intake, "push_text", AsyncMock(return_value=True)), \
                patch.object(image_intake.kintone, "create_record", create):
            self.assertIs(_run(image_intake._send_receipt_and_close("jikou", JIKOU, USER)), True)
        create.assert_awaited_once()

    def test_send_line_push_passes_three_values_through(self):
        import chat_responder as cr
        with patch.object(line_channel, "push_text", AsyncMock(return_value=sl.SEND_UNCONFIRMED)):
            self.assertEqual(_run(cr.send_line_push(USER, "x")), sl.SEND_UNCONFIRMED)
        with patch.object(line_channel, "push_text", AsyncMock(return_value=True)):
            self.assertIs(_run(cr.send_line_push(USER, "x")), True)

    def test_approval_webhook_skips_sent_update_on_unconfirmed_duplicate(self):
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        # JIKOU-FURIGANA-1（大野裁定 2026-10-08）: 凍結台本に 2 行追加 → 番地 +2（1597→1599 …）
        self.assertIn("_sl_res = await send_line_push(user_id, ai_draft)", src[1599])
        self.assertIn('if _sl_res == send_ledger.SEND_UNCONFIRMED: return {"ok": True, "skip": "send_unconfirmed_duplicate"}',
                      src[1600])
        self.assertIn("await mark_approval_sent(record_id); await save_to_chatlog(", src[1601])

    def test_states_distinguish_unconfirmed_for_callers_that_ignore_return(self):
        _FakeClient.raise_exc = RuntimeError("t")
        with self.assertRaises(RuntimeError):
            _run(line_channel.reply_with_push_fallback(JIKOU, "t", USER, "x"))
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        self.assertEqual(self.ops()[0]["finished_at"] is not None, True)


# ── 3. 会話: 受信側で更新（BQ-03） ────────────────────────────────────────────
class TestConversation(_DbMixin):
    def test_inbound_without_reply_still_updates_conversation(self):
        r = self.touch("e1", T0)
        self.assertEqual((r["created"], r["updated"]), (True, True))
        c = self.convs()
        self.assertEqual(len(c), 1)
        self.assertEqual((c[0]["last_inbound_event_id"], c[0]["version"], c[0]["human_version"]),
                         ("e1", 1, 1))
        self.assertEqual(self.ops(), [])

    def test_redelivery_does_not_update_last_inbound(self):
        self.touch("e1", T0)
        self.touch("e2", T0 + datetime.timedelta(days=1))
        before = self.convs()[0]
        r = self.touch("e2", T0 + datetime.timedelta(days=5))      # 再配送（同じ受信イベント ID）
        self.assertEqual((r["created"], r["updated"]), (False, False))
        self.assertEqual(self.convs()[0], before)

    def test_concurrent_inbound_creates_one_conversation(self):
        async def body():
            barrier = asyncio.Barrier(2)

            async def one(event_id):
                await barrier.wait()
                return await sl.touch_inbound("jikou", USER, event_id)
            return await asyncio.gather(one("evt-a"), one("evt-b"))
        rs = _run(body())
        self.assertEqual(sorted(r["created"] for r in rs), [False, True])
        self.assertEqual(len(self.convs()), 1)

    def test_concurrent_sends_serialize_versions(self):
        self.touch("e1", T0)

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
        ops = self.ops()
        self.assertEqual(sorted(o["conversation_version"] for o in ops), [2, 3])
        self.assertEqual(self.convs()[0]["version"], 3)

    def test_concurrent_pushes_to_same_user_both_complete(self):
        async def body():
            _FakeClient.barrier = asyncio.Barrier(2)
            return await asyncio.gather(line_channel.push_text(JIKOU, USER, "a"),
                                        line_channel.push_text(JIKOU, USER, "b"))
        self.assertEqual(_run(body()), [True, True])
        self.assertEqual([o["state"] for o in self.ops()], ["sent", "sent"])

    def test_gap_over_30_days_new_conversation_inherits_attending_and_late_arrival_kept(self):
        self.touch("e1", T0)
        self.touch("e2", T0 + datetime.timedelta(days=30))           # ちょうど 30 日=同じ会話
        self.assertEqual(len(self.convs()), 1)
        _run(sl.set_attending("jikou", USER))                        # 対応中にしておく
        self.touch("e3", T0 + datetime.timedelta(days=60, seconds=1))  # 30 日超=新会話
        convs = self.convs()
        self.assertEqual(len(convs), 2)
        self.assertTrue(convs[1]["attending"])                        # 前会話の対応中を引き継ぐ
        self.assertEqual(convs[1]["attending_since"], convs[0]["attending_since"])
        self.assertEqual((convs[1]["version"], convs[1]["human_version"]), (1, 1))
        last = convs[1]["last_inbound_at"]
        self.touch("e4", T0 + datetime.timedelta(days=1))             # 遅着（古い受付時刻）
        convs = self.convs()
        self.assertEqual(len(convs), 2)                               # 旧会話を再生成しない
        self.assertEqual(convs[1]["last_inbound_at"], last)           # 直近受信も戻らない
        self.assertEqual(convs[1]["last_inbound_event_id"], "e4")
        self.send("e4")
        self.assertEqual(self.ops()[-1]["conversation_id"], convs[1]["conversation_id"])

    def test_received_at_comes_from_inbound_event_when_recorded(self):
        from hub.durable_inbound import record_line_event
        _run(record_line_event(webhook_event_id="wh-9", user_id=USER, signature_result="verified",
                               payload=b"{}", event_type="message"))
        r = self.touch("wh-9")
        self.assertTrue(r["created"])
        c = self.convs()[0]
        self.assertIsNotNone(c["last_inbound_at"])
        self.assertEqual(c["last_inbound_event_id"], "wh-9")

    def test_other_user_has_its_own_conversation_and_hash(self):
        self.touch("e1", T0)
        self.touch("e2", T0, user=USER2)
        convs = self.convs()
        self.assertEqual({c["line_user_hash"] for c in convs},
                         {sl.line_user_hash("jikou", USER), sl.line_user_hash("jikou", USER2)})
        self.assertTrue(all(len(c["ref"]) == 32 for c in convs))
        state = _run(sl.conversation_state("jikou", USER))
        self.assertEqual((state["version"], state["human_version"], state["attending"]), (1, 1, False))


# ── 4. 送信確認待ち: 人の確定と履歴・競合（BQ-05）・自動再送なし ───────────────
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
                                         "state", "stale", "started_at", "attempt_no",
                                         "conversation_ref"})
        self.assertEqual((items[0]["state"], items[0]["stale"]), ("unconfirmed", False))
        self.assertNotIn(USER, str(items))
        self.assertEqual(_run(sl.count_unconfirmed()), 1)
        self.assertEqual(_run(sl.confirm_by_human(op_id, "sent", "line_delivered")), "ok")
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.assertEqual(self.ops()[0]["confirmed_by"], "human")
        self.assertEqual([h["reason"] for h in self.history(op_id)],
                         ["created", "started", "unconfirmed", "human:line_delivered"])
        self.assertEqual(_run(sl.confirm_by_human(op_id, "failed", "other")), "already_confirmed")
        self.assertEqual(len(self.history(op_id)), 4)            # 0 件側は履歴なし
        self.assertEqual(_run(sl.list_unconfirmed()), [])
        self.assertEqual(len(_FakeClient.calls), 1)              # 自動再送なし

    def test_confirm_rejects_bad_input_and_unknown_operation(self):
        op_id = self._unconfirmed()
        self.assertEqual(_run(sl.confirm_by_human(op_id, "sent", "made-up")), "bad_input")
        self.assertEqual(_run(sl.confirm_by_human(op_id, "started", "other")), "bad_input")
        self.assertEqual(_run(sl.confirm_by_human("nope", "failed", "other")), "not_found")
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")

    def test_concurrent_human_confirms_only_one_wins_without_duplicate_history(self):
        op_id = self._unconfirmed()

        async def body():
            barrier = asyncio.Barrier(2)

            async def one(outcome):
                await barrier.wait()
                return await sl.confirm_by_human(op_id, outcome, "other")
            return await asyncio.gather(one("sent"), one("failed"))
        rs = _run(body())
        self.assertEqual(sorted(rs), ["already_confirmed", "ok"])
        hist = [h for h in self.history(op_id) if h["reason"].startswith("human:")]
        self.assertEqual(len(hist), 1)
        self.assertIn(self.ops()[0]["state"], ("sent", "failed"))


# ── 4b. 滞留 started の回収（BQ-02） ─────────────────────────────────────────
class TestStaleStarted(_DbMixin):
    def _started(self, minutes_ago: int) -> str:
        op = _run(sl.begin("jikou", "jikou", USER, "x"))
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=minutes_ago))
        return op.op_id

    def test_recover_moves_only_stale_started_to_unconfirmed_with_history(self):
        stale = self._started(11)
        fresh = self._started(1)
        self.assertEqual(_run(sl.recover_stale_started()), 1)
        states = {o["op_id"]: o["state"] for o in self.ops()}
        self.assertEqual((states[stale], states[fresh]), ("unconfirmed", "started"))
        self.assertEqual([h["reason"] for h in self.history(stale)][-1], "stale_started")
        self.assertEqual(_run(sl.recover_stale_started()), 0)        # 冪等
        self.assertEqual(len(_FakeClient.calls), 0)                  # 自動再送なし

    def test_threshold_is_configurable(self):
        # fix2 BQ-06: 回収は閾値超に加えて処理全体の期限切れ・ハートビート途絶が条件。
        # fix4 BQ-11: 補正済み設定を使うため整合する組（10 s < 30 s < 1 分 < 2 分）で下げる
        with patch.dict(os.environ, {sl.STALE_MINUTES_ENV: "2", sl.DEADLINE_MINUTES_ENV: "1",
                                     sl.HEARTBEAT_SECONDS_ENV: "10", sl.SEND_TIMEOUT_SECONDS_ENV: "30"}):
            self.assertEqual((sl.stale_started_minutes(), sl.deadline_minutes()), (2, 1))
            stale = self._started(3)
            self.assertEqual(_run(sl.recover_stale_started()), 1)
        self.assertEqual((sl.stale_started_minutes(), sl.deadline_minutes()), (10, 5))
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        self.assertEqual(self.ops()[0]["op_id"], stale)

    def test_list_shows_stale_started_and_human_can_confirm_it(self):
        stale = self._started(11)
        self._started(1)
        items = _run(sl.list_unconfirmed())
        self.assertEqual([(i["op_id"], i["state"], i["stale"]) for i in items],
                         [(stale, "started", True)])
        self.assertEqual(_run(sl.count_unconfirmed()), 1)
        self.assertEqual(_run(sl.confirm_by_human(stale, "failed", "line_not_delivered")), "ok")
        self.assertEqual({o["op_id"]: o["state"] for o in self.ops()}[stale], "failed")
        self.assertEqual(_run(sl.list_unconfirmed()), [])

    def test_job_is_registered_on_scheduler_and_noop_without_db(self):
        # main の末尾が登録する（静的 pin）。他テストが scheduler の登録表を消すことがあるため
        # 実登録は冪等な register_recover_job() を呼んでから確認する
        self.assertIn("send_ledger.register_recover_job()", (REPO / "main.py").read_text(encoding="utf-8"))
        sl.register_recover_job()
        sl.register_recover_job()                                    # 冪等
        self.assertTrue(hub_scheduler.is_registered(sl.RECOVER_JOB_NAME))
        self.assertEqual(sl.RECOVER_INTERVAL_MINUTES, 5.0)
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True):
            db.reset_for_tests()
            _run(sl.run_recover_job())                                # DB なし=何もしない
        stale = self._started(11)
        _run(sl.run_recover_job())
        self.assertEqual({o["op_id"]: o["state"] for o in self.ops()}[stale], "unconfirmed")


# ── 5. 対応中・human_version（BQ-04） ──────────────────────────────────────────
class TestAttending(_DbMixin):
    def test_transitions_bump_versions(self):
        st = _run(sl.set_attending("jikou", USER))
        self.assertEqual((st["version"], st["human_version"], st["attending"]), (2, 2, True))
        c = _run(sl.conversation_state("jikou", USER))
        self.assertTrue(c["attending"] and c["attending_since"] and not c["attending_until"])
        st = _run(sl.end_attending("jikou", USER))
        self.assertEqual((st["version"], st["human_version"], st["attending"]), (3, 3, False))
        self.assertTrue(_run(sl.conversation_state("jikou", USER))["attending_until"])
        self.assertEqual(len(self.convs()), 1)

    def test_completed_after_human_only_when_human_version_changed(self):
        self.touch("e1", T0)

        async def body():
            op1 = await sl.begin("jikou", "jikou", USER, "x")
            await sl.set_attending("jikou", USER)             # 着手後・完了前の人の操作
            await sl.finish(op1, sl.STATE_SENT)
            op2 = await sl.begin("jikou", "jikou", USER, "y")
            op3 = await sl.begin("jikou", "jikou", USER, "z")  # bot 送信同士（会話版は進む）
            await sl.finish(op3, sl.STATE_SENT)
            await sl.finish(op2, sl.STATE_SENT)
        _run(body())
        self.assertEqual([o["completed_after_human"] for o in self.ops()], [True, False, False])

    def test_bot_sends_do_not_advance_human_version(self):
        self.touch("e1", T0)
        self.send("e1")
        self.send("e2", text="y")                                 # 別の受信イベント
        c = self.convs()[0]
        self.assertEqual((c["version"], c["human_version"]), (3, 1))

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


# ── 6. fail-open・方言の担保 ──────────────────────────────────────────────────
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
            self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "t", USER, "y")),
                             sl.SEND_SENT)
            self.assertIsNone(_run(sl.begin("jikou", "jikou", USER, "z")))
            self.assertIsNone(_run(sl.touch_inbound("jikou", USER, "e")))
            sl.check_dialect_at_startup()                           # 未設定=何もしない
        self.assertEqual(len(_FakeClient.calls), 2)

    def test_database_error_sends_anyway_with_fixed_vocabulary_log(self):
        d = tempfile.mkdtemp(prefix="q1a_nodb_")
        try:
            with patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{d}/empty.db"}):
                db.reset_for_tests()                           # 表が無い DB
                with self.assertLogs("hub.send_ledger", level="WARNING") as cm:
                    self.assertIs(_run(line_channel.push_text(JIKOU, USER, "secret text")), True)
                    self.assertIsNone(_run(sl.touch_inbound("jikou", USER, "e1")))
            self.assertEqual(len(_FakeClient.calls), 1)
            joined = "\n".join(cm.output)
            self.assertIn("[SEND_LEDGER] begin failed", joined)
            self.assertIn("[SEND_LEDGER] touch_inbound failed", joined)
            self.assertNotIn(USER, joined)
            self.assertNotIn("secret text", joined)
            self.assertNotIn("sqlite", joined)
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_unsupported_dialect_is_rejected_at_startup_and_in_exclusion(self):
        with patch.dict(os.environ, {"DATABASE_URL": "mysql://u@h/d"}):
            with self.assertRaises(sl.UnsupportedDialect):
                sl.check_dialect_at_startup()
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://u@h/d"}):
            sl.check_dialect_at_startup()
        with patch.dict(os.environ, {"DATABASE_URL": "sqlite+aiosqlite:///x.db"}):
            sl.check_dialect_at_startup()
        self.assertEqual(sl.SUPPORTED_DIALECTS, ("postgresql", "sqlite"))
        # 排他の中でも方言を検査する（fail-open で握らない）
        class _Eng:
            class dialect:
                name = "mysql"
        with patch.object(db, "get_async_engine", lambda: _Eng), \
                patch.dict(os.environ, {"DATABASE_URL": "mysql://u@h/d"}):
            with self.assertRaises(sl.UnsupportedDialect):
                _run(sl.begin("jikou", "jikou", USER, "x"))
        self.assertIn("send_ledger.check_dialect_at_startup()",
                      (REPO / "main.py").read_text(encoding="utf-8"))


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
        self.assertEqual((api["ok"], api["count"], api["items"][0]["state"]), (True, 1, "unconfirmed"))
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
        self.assertEqual(r.headers["location"], "/app/send_ops?done=already_confirmed")
        page = _client.get("/app/send_ops?done=ok", headers=_auth())
        self.assertIn("確定しました", page.text)
        self.assertIn("送信確認待ちはありません", page.text)

    def test_page_shows_stale_started_and_accepts_confirm(self):
        op = _run(sl.begin("jikou", "jikou", USER, "x"))
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=30))
        r = _client.get("/app/send_ops", headers=_auth())
        self.assertIn("滞留", r.text)
        self.assertIn(op.op_id, r.text)
        r = _client.post("/app/send_ops/confirm", headers=_auth(),
                         data={"op_id": op.op_id, "outcome": "failed", "reason": "line_not_delivered"},
                         follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/send_ops?done=ok")
        self.assertEqual(self.ops()[0]["state"], "failed")

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


# ── 9. durable lane との分担: 受信側の会話更新・受信イベント ID・再処理で再送なし ──
class TestDurableLaneBinding(_DbMixin):
    def test_event_id_bound_conversation_touched_and_reattempt_does_not_resend(self):
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
        convs = self.convs()
        self.assertEqual(len(convs), 1)                          # 受信側で会話が作られる
        self.assertEqual(ops[0]["conversation_id"], convs[0]["conversation_id"])
        self.assertEqual(convs[0]["last_inbound_event_id"], "wh-1")
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertIsNone(sl.current_context())             # 文脈が漏れない

    def test_paused_inbound_still_touches_conversation(self):
        from hub.durable_inbound import record_line_event

        async def body():
            await record_line_event(webhook_event_id="wh-p", user_id=USER,
                                    signature_result="verified", payload=b"{}", event_type="message")
            with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                    patch.object(main, "_handle_paused_inbound", AsyncMock()):
                await main._process_line_event_durable("tok", USER, "hi", "wh-p")
        _run(body())
        self.assertEqual(self.ops(), [])
        self.assertEqual(self.convs()[0]["last_inbound_event_id"], "wh-p")


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
        self.assertGreaterEqual(len(tagged), 8)
        # sink allowlist の main.py 番地は Q1a では不変（661/683/1318/1618/1915）。
        # JIKOU-FURIGANA-1（大野裁定 2026-10-08）: 凍結台本に 2 行追加 → 全番地 +2
        import json
        entries = json.load(open(REPO / "redaction_sink_allowlist.json", encoding="utf-8"))["entries"]
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries if e.startswith("main.py:")),
                         [663, 685, 1320, 1620, 1917])
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries
                                if e.startswith("chat_responder.py:")), [1660, 1672])
        self.assertIn("send_ledger.touch_inbound", src[1219])      # 画像受信（1 行置換）
        self.assertIn("send_ledger.touch_inbound", src[1347])      # durable 受信（1 行置換）

    def test_closed_sets_are_pinned(self):
        self.assertEqual(sl.ACTORS, ("bot", "human", "approved_draft"))
        self.assertEqual(sl.PURPOSES, ("reply", "first_reply", "urgent", "image_receipt",
                                       "image_result", "follow", "receipt_number", "other"))
        self.assertEqual(sl.STATES, ("pending", "started", "sent", "unconfirmed", "failed"))
        self.assertEqual(sl.HUMAN_REASONS, ("line_delivered", "line_not_delivered",
                                            "customer_confirmed", "other"))
        self.assertEqual(sl.FIRST_REPLY_EXCLUDED, frozenset({"follow"}))
        self.assertEqual(sl.CONVERSATION_GAP, datetime.timedelta(days=30))
        self.assertEqual(sl.TABLE_NAMES, ("conversation", "send_operation", "send_operation_history",
                                          "inbound_touch"))
        self.assertEqual((sl.SEND_SENT, sl.SEND_UNCONFIRMED, sl.SEND_FAILED),
                         ("sent", "unconfirmed", "failed"))
        self.assertIn("stale_started", sl.HISTORY_REASONS)
        self.assertEqual(sl.STALE_STARTED_MINUTES_DEFAULT, 10)
        self.assertEqual(len(sl.line_user_hash("jikou", USER)), 64)
        self.assertEqual(sl.line_user_hash("jikou", USER), sl.line_user_hash("jikou", USER))


# ── 11. fix2: 試行の所有と遅着結果（BQ-06）・画像の再配送（BQ-07）・履歴の遷移元（BQ-08） ──
class TestFix2OwnershipAndLateResults(_DbMixin):
    def _begin(self, event_id="evt-r"):
        async def body():
            tok = sl.bind_inbound(event_id)
            try:
                return await sl.begin("jikou", "jikou", USER, "x")
            finally:
                sl.unbind(tok)
        return _run(body())

    async def _touches(self):
        from hub.db import session_scope
        async with session_scope() as s:
            rows = (await s.execute(sa.select(sl.inbound_touch.c.inbound_event_id,
                                              sl.inbound_touch.c.first_received_at)
                                    .order_by(sl.inbound_touch.c.touch_id))).fetchall()
        return [(r[0], sl._aware(r[1])) for r in rows]

    def test_late_finish_after_recovery_and_new_attempt_does_not_confirm_new_attempt(self):
        op1 = self._begin()
        self.assertEqual((op1.attempt_no, len(op1.owner_token)), (1, 32))
        self.set_started_at(op1.op_id, sl._now() - datetime.timedelta(minutes=11))
        self.assertEqual(_run(sl.recover_stale_started()), 1)
        self.assertEqual(_run(sl.confirm_by_human(op1.op_id, "failed", "line_not_delivered")), "ok")
        op2 = self._begin()                                         # 同一操作の試行 2
        self.assertIsInstance(op2, sl.Operation)
        self.assertEqual((op2.op_id, op2.attempt_no), (op1.op_id, 2))
        self.assertNotEqual(op2.owner_token, op1.owner_token)
        self.assertEqual(_run(sl.finish(op1, sl.STATE_SENT)), "late")    # 試行 1 の遅着
        row = self.ops()[0]
        self.assertEqual((row["state"], row["attempt_no"]), ("started", 2))   # 試行 2 は未完了のまま
        last = self.history(op1.op_id)[-1]
        self.assertEqual((last["from"], last["to"], last["reason"]),
                         ("started", "started", "late_result:sent"))
        self.assertEqual(_run(sl.finish(op2, sl.STATE_SENT)), "applied")
        self.assertEqual(self.ops()[0]["state"], "sent")

    def test_late_finish_right_after_recovery_keeps_unconfirmed_and_history_consistent(self):
        op = self._begin()
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=11))
        self.assertEqual(_run(sl.recover_stale_started()), 1)
        self.assertEqual(_run(sl.finish(op, sl.STATE_SENT)), "late")
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        hist = self.history(op.op_id)
        self.assertEqual([h["reason"] for h in hist],
                         ["created", "started", "stale_started", "late_result:sent"])
        self.assertEqual((hist[-1]["from"], hist[-1]["to"]), ("unconfirmed", "unconfirmed"))

    def test_running_process_is_not_recovered_while_heartbeat_or_deadline_alive(self):
        op_hb = self._begin("evt-hb")
        self.set_started_at(op_hb.op_id, sl._now() - datetime.timedelta(minutes=11),
                            heartbeat_at=sl._now() - datetime.timedelta(minutes=1))
        op_dl = self._begin("evt-dl")
        self.set_started_at(op_dl.op_id, sl._now() - datetime.timedelta(minutes=11),
                            deadline_at=sl._now() + datetime.timedelta(minutes=3))
        self.assertEqual(_run(sl.recover_stale_started()), 0)
        self.assertEqual(_run(sl.list_unconfirmed()), [])
        self.assertEqual(_run(sl.confirm_by_human(op_hb.op_id, "failed", "other")),
                         "already_confirmed")                         # 稼働中は人も確定できない
        self.assertIs(_run(sl.heartbeat(op_hb)), True)
        wrong = sl.Operation(**{**op_hb.__dict__, "owner_token": "not-owner"})
        self.assertIs(_run(sl.heartbeat(wrong)), False)
        self.assertEqual(_run(sl.finish(wrong, sl.STATE_SENT)), "late")
        self.assertEqual(_run(sl.finish(op_hb, sl.STATE_SENT)), "applied")

    def test_new_attempt_invalidates_previous_owner_token(self):
        _FakeClient.responses = [_FakeResp(500, "x")]
        self.assertIs(self.send("evt-f"), False)                      # 試行 1 failed
        ops = self.ops()
        self.assertEqual((ops[0]["state"], ops[0]["attempt_no"]), ("failed", 1))
        old = sl.Operation(ops[0]["op_id"], ops[0]["conversation_id"],
                           ops[0]["conversation_version"], None, "jikou", "jikou", "reply",
                           "bot", 1, ops[0]["owner_token"])
        self.assertIs(self.send("evt-f"), True)                       # 試行 2 sent
        self.assertEqual(_run(sl.finish(old, sl.STATE_FAILED)), "late")
        row = self.ops()[0]
        self.assertEqual((row["state"], row["attempt_no"]), ("sent", 2))
        self.assertNotEqual(row["owner_token"], old.owner_token)

    def test_image_redelivery_after_other_event_does_not_advance_or_split_conversation(self):
        self.touch("img-1", T0)                                        # 画像 E1
        self.touch("img-2", T0 + datetime.timedelta(days=1))           # 別イベント E2
        before = self.convs()
        r = self.touch("img-1", T0 + datetime.timedelta(days=40))      # E1 の再配送（30 日超の時刻を模擬）
        self.assertEqual((r["created"], r["updated"]), (False, False))
        after = self.convs()
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0]["last_inbound_at"], before[0]["last_inbound_at"])
        self.assertEqual(after[0]["last_inbound_event_id"], "img-2")
        touches = _run(self._touches())
        self.assertEqual(touches, [("img-1", T0), ("img-2", T0 + datetime.timedelta(days=1))])

    def test_first_received_at_is_recorded_not_recomputed(self):
        r1 = self.touch("img-9")                                       # 受付時の現在時刻を保存
        self.assertTrue(r1["created"])
        t_first = _run(self._touches())[0][1]
        self.touch("img-9", T0)                                        # 再配送（別の時刻でも無視）
        self.assertEqual(_run(self._touches())[0][1], t_first)
        self.assertEqual(len(self.convs()), 1)

    def test_human_confirm_from_stale_started_records_two_steps(self):
        op = self._begin()
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=11))
        self.assertEqual(_run(sl.confirm_by_human(op.op_id, "failed", "line_not_delivered")), "ok")
        hist = self.history(op.op_id)
        self.assertEqual([(h["from"], h["to"], h["reason"]) for h in hist],
                         [(None, "pending", "created"), ("pending", "started", "started"),
                          ("started", "unconfirmed", "human_direct"),
                          ("unconfirmed", "failed", "human:line_not_delivered")])
        self.assertEqual(self.ops()[0]["state"], "failed")

    def test_late_result_and_human_confirm_race(self):
        op = self._begin()
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=11))
        self.assertEqual(_run(sl.recover_stale_started()), 1)

        async def body():
            barrier = asyncio.Barrier(2)

            async def late():
                await barrier.wait()
                return await sl.finish(op, sl.STATE_SENT)

            async def human():
                await barrier.wait()
                return await sl.confirm_by_human(op.op_id, "failed", "customer_confirmed")
            return await asyncio.gather(late(), human())
        self.assertEqual(_run(body()), ["late", "ok"])
        self.assertEqual(self.ops()[0]["state"], "failed")
        reasons = [h["reason"] for h in self.history(op.op_id)]
        self.assertEqual(reasons.count("human:customer_confirmed"), 1)
        self.assertEqual(reasons.count("late_result:sent"), 1)

    def test_concurrent_confirms_on_stale_started(self):
        op = self._begin()
        self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=11))

        async def body():
            barrier = asyncio.Barrier(2)

            async def one(outcome):
                await barrier.wait()
                return await sl.confirm_by_human(op.op_id, outcome, "other")
            return await asyncio.gather(one("sent"), one("failed"))
        self.assertEqual(sorted(_run(body())), ["already_confirmed", "ok"])
        reasons = [h["reason"] for h in self.history(op.op_id)]
        self.assertEqual(reasons.count("human_direct"), 1)
        self.assertEqual(sum(1 for r in reasons if r.startswith("human:")), 1)

    def test_finish_return_values_and_pins(self):
        self.assertEqual(_run(sl.finish(None, sl.STATE_SENT)), "skipped")
        self.assertIn("inbound_touch", sl.TABLE_NAMES)
        self.assertEqual(sl.DEADLINE_MINUTES_DEFAULT, 5)
        self.assertLess(sl.DEADLINE_MINUTES_DEFAULT, sl.STALE_STARTED_MINUTES_DEFAULT)
        self.assertIn("human_direct", sl.HISTORY_REASONS)
        self.assertIn("late_result:sent", sl.HISTORY_REASONS)


# ── 12. fix3: 送信経路の heartbeat と全体期限（BQ-09） ───────────────────────────
class _WaitingClient(_FakeClient):
    """HTTP 待機を模す: release が set されるまで post が返らない。"""
    release: asyncio.Event | None = None
    waiting: asyncio.Event | None = None

    async def post(self, url, headers=None, json=None):
        if _WaitingClient.waiting is not None:
            _WaitingClient.waiting.set()
        if _WaitingClient.release is not None:
            await _WaitingClient.release.wait()
        return await super().post(url, headers=headers, json=json)


class TestFix3HeartbeatAndTimeout(_DbMixin):
    def setUp(self):
        super().setUp()
        self._cp.stop()
        self._cp = patch.object(line_channel.httpx, "AsyncClient", _WaitingClient)
        self._cp.start()
        _WaitingClient.release = None
        _WaitingClient.waiting = None
        # heartbeat 0.05 秒・送信 timeout 20 秒（< deadline 300 秒 < 回収 600 秒）
        self._tenv = patch.dict(os.environ, {sl.HEARTBEAT_SECONDS_ENV: "0.05",
                                             sl.SEND_TIMEOUT_SECONDS_ENV: "20"})
        self._tenv.start()
        self.assertFalse(sl.timing_config()["defaulted"])

    def tearDown(self):
        self._tenv.stop()
        super().tearDown()

    # ループ内から使う async helper（_run 系の同期 helper はループ内で使えない）
    async def _ops_a(self):
        from hub.db import session_scope
        async with session_scope() as s:
            return [dict(r._mapping) for r in (await s.execute(
                sa.select(sl.send_operation).order_by(sl.send_operation.c.created_at))).fetchall()]

    async def _age_row_a(self, op_id: str, minutes: int):
        """着手・期限・ハートビートを minutes 分前に戻す（経過時間の模擬）。"""
        from hub.db import session_scope
        at = sl._now() - datetime.timedelta(minutes=minutes)
        async with session_scope() as s:
            await s.execute(sa.update(sl.send_operation).where(
                sl.send_operation.c.op_id == op_id).values(
                started_at=at, deadline_at=at + datetime.timedelta(minutes=sl.deadline_minutes()),
                last_heartbeat_at=at))

    async def _op_id_a(self):
        return (await self._ops_a())[0]["op_id"]

    async def _heartbeat_at_a(self, op_id: str):
        return next(o for o in await self._ops_a() if o["op_id"] == op_id)["last_heartbeat_at"]

    def test_codex_repro_push_text_waiting_11_minutes_is_not_recovered(self):
        """Codex 再現: push_text の HTTP 待機中に 11 分進める → heartbeat が更新され
        recover / list / confirm の対象にならず、HTTP 完了で finish が確定する。"""
        async def body():
            _WaitingClient.release = asyncio.Event()
            _WaitingClient.waiting = asyncio.Event()
            task = asyncio.create_task(line_channel.push_text(JIKOU, USER, "x"))
            await _WaitingClient.waiting.wait()                    # HTTP 待機に入った
            op_id = await self._op_id_a()
            await self._age_row_a(op_id, 11)                               # 11 分経過を模擬
            aged = sl._aware(await self._heartbeat_at_a(op_id))
            await asyncio.sleep(0.2)                               # heartbeat 数周期
            fresh = sl._aware(await self._heartbeat_at_a(op_id))
            self.assertGreater(fresh, aged)                        # (a) 待機中に更新される
            self.assertEqual(await sl.recover_stale_started(), 0)
            self.assertEqual(await sl.list_unconfirmed(), [])
            self.assertEqual(await sl.confirm_by_human(op_id, "failed", "other"),
                             "already_confirmed")
            n_before = len(asyncio.all_tasks())
            _WaitingClient.release.set()
            self.assertIs(await task, True)
            await asyncio.sleep(0.05)
            n_after = len(asyncio.all_tasks())
            return op_id, n_before, n_after
        op_id, n_before, n_after = _run(body())
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.assertEqual(self.history(op_id)[-1]["reason"], "sent")
        self.assertGreaterEqual(n_before - 2, n_after)             # (c) push + heartbeat が消えた

    def test_reply_with_push_fallback_waiting_is_not_recovered(self):
        async def body():
            _WaitingClient.release = asyncio.Event()
            _WaitingClient.waiting = asyncio.Event()
            task = asyncio.create_task(line_channel.reply_with_push_fallback(JIKOU, "t", USER, "x"))
            await _WaitingClient.waiting.wait()
            op_id = await self._op_id_a()
            await self._age_row_a(op_id, 11)
            await asyncio.sleep(0.2)
            self.assertEqual(await sl.recover_stale_started(), 0)
            self.assertEqual(await sl.count_unconfirmed(), 0)
            _WaitingClient.release.set()
            return await task
        self.assertEqual(_run(body()), sl.SEND_SENT)
        self.assertEqual(self.ops()[0]["state"], "sent")

    def test_send_timeout_finishes_as_unconfirmed_and_raises(self):
        with patch.dict(os.environ, {sl.SEND_TIMEOUT_SECONDS_ENV: "0.3"}):
            async def body():
                _WaitingClient.release = asyncio.Event()          # 返らない HTTP
                n0 = len(asyncio.all_tasks())
                with self.assertRaises(asyncio.TimeoutError):
                    await line_channel.push_text(JIKOU, USER, "x")
                await asyncio.sleep(0.05)
                return n0, len(asyncio.all_tasks())
            n0, n1 = _run(body())
        op = self.ops()[0]
        self.assertEqual(op["state"], "unconfirmed")                 # (b) 台帳が確定する
        self.assertEqual(self.history(op["op_id"])[-1]["reason"], "unconfirmed")
        self.assertEqual(n0, n1)                                     # (c) heartbeat タスクが残らない

    def test_heartbeat_task_is_cancelled_on_http_exception(self):
        async def body():
            _FakeClient.raise_exc = RuntimeError("transport")
            n0 = len(asyncio.all_tasks())
            with self.assertRaises(RuntimeError):
                await line_channel.push_text(JIKOU, USER, "x")
            _FakeClient.raise_exc = None
            await asyncio.sleep(0.05)
            return n0, len(asyncio.all_tasks())
        n0, n1 = _run(body())
        self.assertEqual(n0, n1)
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")

    def test_late_http_success_after_recovery_is_late_result_only(self):
        """(d) heartbeat が止まった（プロセス死相当）試行が回収された後に HTTP が遅れて成功しても
        late_result のみ（fix2 の保証が維持される）。"""
        async def body():
            _WaitingClient.release = asyncio.Event()
            _WaitingClient.waiting = asyncio.Event()
            with patch.object(sl, "heartbeat", AsyncMock(return_value=False)):   # 鼓動なし
                task = asyncio.create_task(line_channel.push_text(JIKOU, USER, "x"))
                await _WaitingClient.waiting.wait()
                op_id = await self._op_id_a()
                await self._age_row_a(op_id, 11)
                await asyncio.sleep(0.1)
                self.assertEqual(await sl.recover_stale_started(), 1)          # 回収される
                self.assertEqual(await sl.confirm_by_human(op_id, "failed", "other"), "ok")
                _WaitingClient.release.set()
                self.assertIs(await task, True)                                 # HTTP 自体は成功
            return op_id
        op_id = _run(body())
        row = self.ops()[0]
        self.assertEqual(row["state"], "failed")                                # 人の確定が正
        reasons = [h["reason"] for h in self.history(op_id)]
        self.assertEqual(reasons[-1], "late_result:sent")
        self.assertEqual(reasons.count("stale_started"), 1)

    def test_timing_config_validation_warns_and_falls_back(self):
        with patch.dict(os.environ, {sl.HEARTBEAT_SECONDS_ENV: "600",
                                     sl.SEND_TIMEOUT_SECONDS_ENV: "240"}):      # heartbeat > timeout
            with self.assertLogs("hub.send_ledger", level="WARNING") as cm:
                cfg = sl.timing_config()
                self.assertFalse(sl.check_timing_config())
            self.assertTrue(cfg["defaulted"])
            self.assertEqual((cfg["heartbeat"], cfg["timeout"], cfg["deadline"], cfg["stale"]),
                             (60.0, 240.0, 300.0, 600.0))
            self.assertIn("timing config inconsistent", "\n".join(cm.output))
        with patch.dict(os.environ, {sl.SEND_TIMEOUT_SECONDS_ENV: "400"}):     # timeout > deadline
            self.assertTrue(sl.timing_config()["defaulted"])
        with patch.dict(os.environ, {sl.DEADLINE_MINUTES_ENV: "10"}):          # deadline == stale
            self.assertTrue(sl.timing_config()["defaulted"])
        with patch.dict(os.environ, {sl.HEARTBEAT_SECONDS_ENV: "abc"}):        # 不正値は既定へ
            self.assertEqual(sl.heartbeat_seconds(), 0.05 if False else sl.timing_config()["heartbeat"])
        self.assertIn("send_ledger.check_timing_config()", (REPO / "main.py").read_text(encoding="utf-8"))

    def test_no_db_path_is_passthrough_without_timeout_or_heartbeat(self):
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        with patch.dict(os.environ, env, clear=True), \
                patch.dict(os.environ, {sl.SEND_TIMEOUT_SECONDS_ENV: "0.1"}):
            db.reset_for_tests()

            async def body():
                _WaitingClient.release = asyncio.Event()
                n0 = len(asyncio.all_tasks())
                task = asyncio.create_task(line_channel.push_text(JIKOU, USER, "x"))
                await asyncio.sleep(0.3)                           # timeout 相当を超えても待つ
                self.assertFalse(task.done())
                _WaitingClient.release.set()
                r = await task
                return r, n0, len(asyncio.all_tasks())
            r, n0, n1 = _run(body())
        self.assertIs(r, True)
        self.assertEqual(n0, n1)


# ── 13. fix4: 設定フォールバックの一貫性（BQ-11）と _now() を進める方式の再現 ────────
class TestFix4TimingFallback(_DbMixin):
    CODEX_ENV = {sl.HEARTBEAT_SECONDS_ENV: "60", sl.SEND_TIMEOUT_SECONDS_ENV: "240",
                 sl.DEADLINE_MINUTES_ENV: "1", sl.STALE_MINUTES_ENV: "2"}      # deadline < timeout

    def test_codex_inconsistent_config_warns_once_and_is_applied_consistently(self):
        sl._timing_warned_for = None
        with patch.dict(os.environ, self.CODEX_ENV):
            with self.assertLogs("hub.send_ledger", level="WARNING") as cm:   # (a) 警告
                self.assertTrue(sl.timing_config()["defaulted"])
                sl.timing_config(); sl.deadline_minutes(); sl.stale_started_minutes()
            self.assertEqual(sum("timing config inconsistent" in l for l in cm.output), 1)   # 1 回だけ
            # 補正済み値を各 getter が返す
            self.assertEqual((sl.deadline_minutes(), sl.stale_started_minutes(),
                              sl.heartbeat_seconds(), sl.send_timeout_seconds()), (5, 10, 60.0, 240.0))
            op = _run(sl.begin("jikou", "jikou", USER, "x"))
            row = self.ops()[0]
            stored = sl._aware(row["deadline_at"]) - sl._aware(row["started_at"])
            self.assertEqual(stored, datetime.timedelta(seconds=300))                   # (b) 保存値
            # (c) 回収境界は 600 秒: 着手 5 分後（鼓動途絶）は対象外、11 分後は回収される
            self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=5))
            self.assertEqual(_run(sl.recover_stale_started()), 0)
            self.assertEqual(_run(sl.list_unconfirmed()), [])
            self.assertEqual(_run(sl.confirm_by_human(op.op_id, "failed", "other")), "already_confirmed")
            self.set_started_at(op.op_id, sl._now() - datetime.timedelta(minutes=11))
            self.assertEqual(_run(sl.recover_stale_started()), 1)
            self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        sl._timing_warned_for = None

    def test_stored_deadline_is_respected_after_config_change(self):
        op = _run(sl.begin("jikou", "jikou", USER, "x"))                   # deadline 5 分で保存
        # 設定を 1 分期限・2 分回収（整合する組）へ変更しても、保存済み期限は再計算しない
        with patch.dict(os.environ, {sl.HEARTBEAT_SECONDS_ENV: "10", sl.SEND_TIMEOUT_SECONDS_ENV: "30",
                                     sl.DEADLINE_MINUTES_ENV: "1", sl.STALE_MINUTES_ENV: "2"}):
            self.assertEqual(sl.deadline_minutes(), 1)
            now = sl._now()
            self.set_started_at(op.op_id, now - datetime.timedelta(minutes=3),
                                deadline_at=now + datetime.timedelta(minutes=2))     # 保存値は未到来
            self.assertEqual(_run(sl.recover_stale_started()), 0)
            self.assertEqual(_run(sl.list_unconfirmed()), [])
            self.set_started_at(op.op_id, now - datetime.timedelta(minutes=3))      # 保存値も経過
            self.assertEqual(_run(sl.recover_stale_started()), 1)

    def test_raw_env_and_validated_config_are_separate(self):
        with patch.dict(os.environ, self.CODEX_ENV):
            self.assertEqual((sl._raw_minutes(sl.DEADLINE_MINUTES_ENV, 5),
                              sl._raw_minutes(sl.STALE_MINUTES_ENV, 10)), (1, 2))      # 生値
            self.assertEqual((sl.deadline_minutes(), sl.stale_started_minutes()), (5, 10))   # 補正済み
        sl._timing_warned_for = None


class TestFix4ClockAdvance(_DbMixin):
    """DB の日時を書き換えず _now() を 11 分進める方式（Codex の独立検証と同じ）。"""

    def setUp(self):
        super().setUp()
        self._cp.stop()
        self._cp = patch.object(line_channel.httpx, "AsyncClient", _WaitingClient)
        self._cp.start()
        _WaitingClient.release = None
        _WaitingClient.waiting = None
        self._tenv = patch.dict(os.environ, {sl.HEARTBEAT_SECONDS_ENV: "0.05",
                                             sl.SEND_TIMEOUT_SECONDS_ENV: "20"})
        self._tenv.start()

    def tearDown(self):
        self._tenv.stop()
        super().tearDown()

    def _run_with_clock(self, send_coro_factory):
        base = sl._now()
        offset = [datetime.timedelta(0)]

        async def body():
            with patch.object(sl, "_now", lambda: base + offset[0]):
                _WaitingClient.release = asyncio.Event()
                _WaitingClient.waiting = asyncio.Event()
                task = asyncio.create_task(send_coro_factory())
                await _WaitingClient.waiting.wait()
                offset[0] = datetime.timedelta(minutes=11)                # 時計を 11 分進める
                await asyncio.sleep(0.2)                                  # heartbeat が新しい時刻で打つ
                from hub.db import session_scope
                async with session_scope() as s:
                    row = dict((await s.execute(sa.select(sl.send_operation))).first()._mapping)
                hb = sl._aware(row["last_heartbeat_at"])
                self.assertGreaterEqual(hb, base + datetime.timedelta(minutes=11))
                self.assertLess(sl._aware(row["deadline_at"]), sl._now())        # 期限は切れている
                self.assertEqual(await sl.recover_stale_started(), 0)            # それでも稼働中
                self.assertEqual(await sl.list_unconfirmed(), [])
                self.assertEqual(await sl.confirm_by_human(row["op_id"], "failed", "other"),
                                 "already_confirmed")
                _WaitingClient.release.set()
                return await task
        return _run(body())

    def test_push_text_clock_advance_is_not_recovered(self):
        self.assertIs(self._run_with_clock(lambda: line_channel.push_text(JIKOU, USER, "x")), True)
        self.assertEqual(self.ops()[0]["state"], "sent")

    def test_reply_with_push_fallback_clock_advance_is_not_recovered(self):
        self.assertEqual(self._run_with_clock(
            lambda: line_channel.reply_with_push_fallback(JIKOU, "t", USER, "x")), sl.SEND_SENT)
        self.assertEqual(self.ops()[0]["state"], "sent")


if __name__ == "__main__":
    unittest.main()
