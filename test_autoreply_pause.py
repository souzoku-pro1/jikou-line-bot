"""AUTOREPLY-PAUSE-IMPL ①全体停止 flag（env AUTOREPLY_PAUSED）のテスト。

固定する契約:
- flag 判定はあらゆる外部作用より前（App21 参照・Claude・LINE 送信・
  承認キュー投入のいずれにも到達しない）
- ON: App21 の有無・ヒアリングセッション中かを問わず自動返信 0 件。
  受信記録（App28）と管理者通知（業務チャネル・人対応と同型）は継続。
  Claude API 呼び出しもスキップ
- App21 レコードなしの新規顧客は PII 配慮の匿名ID（userId 生値を通知に
  載せない）
- OFF（未設定/他値）: 従来挙動と完全一致（ヒアリングフローが Claude→返信へ
  進む回帰を pin）
- durable wrapper（_process_line_event_durable）経由でも
  processing→completed の観測記録は継続（耐久化は無影響）
- AUTOREPLY-02: pause 経路の App28 保存は専用 strict writer
  （_save_paused_chatlog）——env 未設定/HTTP 4xx/5xx=例外化・成功時のみ正常
  return。実契約（writer 実体+mock transport）の negative も固定
- AUTOREPLY-03: 並行一意性は durable 状態機械（record_line_event の排他
  claim）が担保・category 検索は順次 retry の冪等担保のみ（役割分担を pin）
kintone・LINE・Claude は全て mock（実契約テストは httpx transport 層で mock）。
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "t", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}
_SAVED = {k: os.environ.get(k) for k in _ENV}
os.environ.update(_ENV)

import httpx  # noqa: E402
import sqlalchemy as sa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import hub.db as db  # noqa: E402
from hub import durable_inbound as di  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub.inbound_event import Base as InboundBase, InboundEvent  # noqa: E402
import main  # noqa: E402

for _k, _o in _SAVED.items():
    if _o is None:
        os.environ.pop(_k, None)
    else:
        os.environ[_k] = _o

USER = "U-pause-test-0001"

# _Base は pause 経路の保存を mock に差し替える。実契約テスト
# （TestStrictWriterRealContract）はこの実体へ戻して transport 層だけを mock する
_REAL_STRICT_WRITER = main._save_paused_chatlog


def run(coro):
    return asyncio.run(coro)


def app21_record(name="山田太郎", mode="自動", status="受任"):
    return {"顧客名": {"value": name},
            "response_mode": {"value": mode},
            "status": {"value": status}}


class _Base(unittest.TestCase):
    """外部作用を全て mock し、呼出し回数で契約を固定する共通ハーネス"""

    def setUp(self):
        self.reply = AsyncMock()
        self.claude = AsyncMock(return_value="こんにちは")
        self.chatlog = AsyncMock()
        self.notify = AsyncMock(return_value=True)
        self.customer = AsyncMock()
        self.app21 = AsyncMock(return_value=None)
        self.idem_check = AsyncMock(return_value=False)   # 既定=未保存
        for p in [
            patch.object(main, "_line_reply_with_fallback", new=self.reply),
            patch.object(main, "ask_claude", new=self.claude),
            patch.object(main, "save_to_chatlog", new=self.chatlog),
            # pause 経路の strict writer も同じ mock（シグネチャ同一・既存の
            # 呼出し回数/引数の期待値はそのまま固定される）
            patch.object(main, "_save_paused_chatlog", new=self.chatlog),
            patch.object(main, "handle_customer_message", new=self.customer),
            patch.object(main, "get_app21_record", new=self.app21),
            patch.object(main, "_paused_chatlog_already_saved",
                         new=self.idem_check),
            patch.object(main, "ATTORNEY_LINE_USER_ID", "U-attorney"),
            patch.object(main, "emit",
                         new=lambda v, *a, **k: str(v)),   # 内容検証用に素通し
            patch("hub.notify.notify_business", new=self.notify),
        ]:
            p.start()
            self.addCleanup(p.stop)
        # ヒアリングセッション状態の掃除（プロセス共有 dict のため）
        main.conversation_histories.pop(USER, None)
        main.hearing_completed.discard(USER)
        self.addCleanup(lambda: (main.conversation_histories.pop(USER, None),
                                 main.hearing_completed.discard(USER)))

    def _assert_no_autoreply_side_effects(self):
        self.reply.assert_not_awaited()
        self.claude.assert_not_awaited()
        self.customer.assert_not_awaited()

    def _durable_marks(self):
        marks = {"processing": AsyncMock(), "completed": AsyncMock(),
                 "failed": AsyncMock()}
        for name, mock in (("mark_line_processing", marks["processing"]),
                           ("mark_line_completed", marks["completed"]),
                           ("mark_line_failed", marks["failed"])):
            p = patch.object(di, name, new=mock)
            p.start()
            self.addCleanup(p.stop)
        return marks


class TestPausedOn(_Base):
    def test_new_customer_without_app21_recorded_and_notified(self):
        """ON×App21なし（新規・ヒアリング前）: 返信/Claude 0・記録と通知あり・
        通知は匿名ID（userId 生値を載せない）——pause 中の negative"""
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "初めまして"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once_with(USER, "user", "初めまして",
                                              "", "no")
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("【自動返信停止中】", text)
        self.assertIn("匿名ID:", text)
        self.assertNotIn(USER, text, "userId 生値を通知に載せない（PII 配慮）")
        self.assertIn("初めまして", text, "本文は人対応と同型（emit 経由）")

    def test_existing_customer_uses_app21_name(self):
        """ON×App21あり: 顧客名で通知（人対応と同型）・返信/Claude 0"""
        self.app21.return_value = app21_record(name="山田太郎")
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "進捗どうですか"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("山田太郎", text)
        self.assertNotIn("匿名ID:", text)

    def test_mid_hearing_session_is_also_paused(self):
        """ON×ヒアリングセッション中: セッション有無を問わず全停止
        （flag 判定はルーティング判定より前）"""
        main.conversation_histories[USER] = [{"role": "user", "content": "x"}]
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "続きです"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once()
        self.notify.assert_awaited_once()

    def test_app21_lookup_failure_falls_back_to_anonymous(self):
        """App21 参照失敗は best-effort（記録・通知は匿名IDで継続）"""
        self.app21.side_effect = RuntimeError("kintone down")
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event("rt", USER, "こんにちは"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once()
        self.assertIn("匿名ID:", self.notify.await_args.args[1])

    def test_durable_wrapper_still_observes(self):
        """durable wrapper 経由でも processing→completed の観測は継続
        （inbound_event 耐久化は pause の影響を受けない）"""
        from hub import durable_inbound as di
        marks = {"processing": AsyncMock(), "completed": AsyncMock(),
                 "failed": AsyncMock()}
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                patch.object(di, "mark_line_processing",
                             new=marks["processing"]), \
                patch.object(di, "mark_line_completed",
                             new=marks["completed"]), \
                patch.object(di, "mark_line_failed", new=marks["failed"]):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-1"))
        marks["processing"].assert_awaited_once_with("evt-1")
        marks["completed"].assert_awaited_once_with("evt-1")
        marks["failed"].assert_not_awaited()
        self.reply.assert_not_awaited()

    def test_attorney_unset_still_records(self):
        """通知先未設定でも受信記録は行う（記録が主・通知は従）"""
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                patch.object(main, "ATTORNEY_LINE_USER_ID", ""):
            run(main._process_line_event("rt", USER, "こんにちは"))
        self.chatlog.assert_awaited_once()
        self.notify.assert_not_awaited()
        self._assert_no_autoreply_side_effects()


class TestFailClosed(_Base):
    """AUTOREPLY-01: 記録/通知の独立試行・失敗の伝播・retry 冪等性
    （保存は mock 形= RuntimeError 注入。実契約形は
    TestStrictWriterRealContract で固定・本クラスは残置）"""

    def test_save_failure_notify_still_sent_and_durable_failed(self):
        """(i)(ii) save 失敗 → 通知は届く（失敗分類つき=人が知れる）＋
        durable は completed でなく failed（retry 可能）"""
        self.chatlog.side_effect = RuntimeError("kintone down")
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-1"))
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("受信記録に失敗しています", text)
        self.assertIn("RuntimeError", text, "失敗分類を通知に含める")
        marks["failed"].assert_awaited_once()
        self.assertEqual(marks["failed"].await_args.args,
                         ("evt-1", "PausedInboundError"))
        marks["completed"].assert_not_awaited()
        self.reply.assert_not_awaited()

    def test_notify_failure_record_kept_and_durable_failed(self):
        """(i)(ii) notify 失敗 → 記録は残る＋durable failed"""
        self.notify.side_effect = RuntimeError("push down")
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-2"))
        self.chatlog.assert_awaited_once()
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_not_awaited()

    def test_notify_declined_is_also_failure(self):
        """notify_business の False（allowlist 外等の未送信）も失敗として伝播"""
        self.notify.return_value = False
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-3"))
        self.chatlog.assert_awaited_once()
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_not_awaited()

    def test_both_failures_attempted_independently(self):
        """(i) 両方失敗 → 双方とも試行されている（片方の失敗で他方を
        スキップしない）＋durable failed"""
        self.chatlog.side_effect = RuntimeError("kintone down")
        self.notify.side_effect = RuntimeError("push down")
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-4"))
        self.chatlog.assert_awaited_once()
        self.notify.assert_awaited_once()
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_not_awaited()

    def test_retry_is_idempotent_for_chatlog_but_renotifies(self):
        """(iii) retry で App28 増分 0（冪等キーの既存確認）・通知は再送・
        2 回目は completed へ収束"""
        self.notify.side_effect = [RuntimeError("push down"), True]
        self.idem_check.side_effect = [False, True]   # 1回目=未保存/2回目=保存済み
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-5"))
            run(main._process_line_event_durable("rt", USER, "本文", "evt-5"))
        self.assertEqual(self.chatlog.await_count, 1, "App28 増分 0（冪等）")
        self.assertEqual(self.chatlog.await_args.args,
                         (USER, "user", "本文", "paused:evt-5", "no"),
                         "冪等キーを category へ保存（inbound event 識別子）")
        self.assertEqual(self.notify.await_count, 2, "通知は再送（沈黙より安全側）")
        marks["failed"].assert_awaited_once()
        marks["completed"].assert_awaited_once()

    def test_success_path_still_completes(self):
        """成功経路の completed 回帰（fail-closed 化による退行なし）"""
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            run(main._process_line_event_durable("rt", USER, "本文", "evt-6"))
        marks["completed"].assert_awaited_once()
        marks["failed"].assert_not_awaited()

    def test_non_durable_failure_logs_and_raises(self):
        """(ii) 非 durable 経路: 分類付き ERROR ログ（PII 非搭載）＋送出"""
        self.chatlog.side_effect = RuntimeError("kintone down")
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}), \
                self.assertLogs("main", level="ERROR") as logs, \
                self.assertRaises(main.PausedInboundError) as ctx:
            run(main._process_line_event("rt", USER, "秘密の本文"))
        joined = "\n".join(logs.output)
        self.assertIn("chatlog save failed", joined)
        self.assertIn("cls=RuntimeError", joined)
        self.assertNotIn("秘密の本文", joined, "ERROR ログに本文を載せない")
        self.assertNotIn("秘密の本文", str(ctx.exception),
                         "例外メッセージは分類のみ（PII 非搭載）")


class TestStrictWriterRealContract(_Base):
    """AUTOREPLY-02: strict writer の実契約（mock は httpx transport 層のみ・
    _save_paused_chatlog と hub_kintone は実体を通す）"""

    def setUp(self):
        super().setUp()
        # _Base が mock に差し替えた strict writer を実体へ戻す
        p = patch.object(main, "_save_paused_chatlog", new=_REAL_STRICT_WRITER)
        p.start()
        self.addCleanup(p.stop)

    def _mock_kintone_transport(self, handler):
        real_client = httpx.AsyncClient
        transport = httpx.MockTransport(handler)
        p = patch.object(hub_kintone.httpx, "AsyncClient",
                         new=lambda **kw: real_client(transport=transport,
                                                      **kw))
        p.start()
        self.addCleanup(p.stop)

    def _kintone_env(self, **extra):
        return patch.dict(os.environ, {
            "AUTOREPLY_PAUSED": "1", "APP_CHATLOG": "28",
            "TOKEN_CHATLOG": "tok", "KINTONE_SUBDOMAIN": "testsub", **extra})

    def test_env_missing_is_failure_not_silent_skip(self):
        """(i) APP_CHATLOG/TOKEN_CHATLOG 未設定 → silent skip でなく失敗:
        通知に「記録に失敗しています（分類: PausedChatlogConfigError）」が
        載る＋durable failed（fail-open な save_to_chatlog との差分を固定）"""
        marks = self._durable_marks()
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            os.environ.pop("APP_CHATLOG", None)
            os.environ.pop("TOKEN_CHATLOG", None)
            run(main._process_line_event_durable("rt", USER, "本文", "evt-s1"))
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("受信記録に失敗しています", text)
        self.assertIn("PausedChatlogConfigError", text, "失敗分類を通知に含める")
        marks["failed"].assert_awaited_once()
        self.assertEqual(marks["failed"].await_args.args,
                         ("evt-s1", "PausedInboundError"))
        marks["completed"].assert_not_awaited()

    def test_http_500_is_failure_and_durable_failed(self):
        """(ii) kintone HTTP 500（mock transport・実 HTTP 経路）→ 通知に
        分類 KintoneError＋durable failed"""
        self._mock_kintone_transport(lambda req: httpx.Response(
            500, json={"code": "GAIA_UN01", "message": "server error"}))
        marks = self._durable_marks()
        with self._kintone_env():
            run(main._process_line_event_durable("rt", USER, "本文", "evt-s2"))
        text = self.notify.await_args.args[1]
        self.assertIn("受信記録に失敗しています", text)
        self.assertIn("KintoneError", text)
        marks["failed"].assert_awaited_once()
        self.assertEqual(marks["failed"].await_args.args,
                         ("evt-s2", "PausedInboundError"))
        marks["completed"].assert_not_awaited()

    def test_http_4xx_is_failure_and_raises_non_durable(self):
        """(ii) HTTP 400 も例外化（4xx=設定/権限系も沈黙させない）。
        非 durable 経路は PausedInboundError 送出"""
        self._mock_kintone_transport(lambda req: httpx.Response(
            400, json={"code": "CB_VA01", "message": "invalid app"}))
        with self._kintone_env(), \
                self.assertRaises(main.PausedInboundError):
            run(main._process_line_event("rt", USER, "本文"))
        text = self.notify.await_args.args[1]
        self.assertIn("KintoneError", text)

    def test_success_real_contract_completes(self):
        """(iii) 成功時のみ正常 return: 実契約でも completed 回帰＋
        App28 へ POST された record が既存フィールド形（回帰なし）"""
        captured = {}

        def handler(req):
            captured["body"] = json.loads(req.content)
            return httpx.Response(200, json={"id": "12345", "revision": "1"})

        self._mock_kintone_transport(handler)
        marks = self._durable_marks()
        with self._kintone_env():
            run(main._process_line_event_durable("rt", USER, "本文", "evt-s3"))
        marks["completed"].assert_awaited_once()
        marks["failed"].assert_not_awaited()
        rec = captured["body"]["record"]
        self.assertEqual(captured["body"]["app"], "28")
        self.assertEqual(rec["line_user_id"]["value"], USER)
        self.assertEqual(rec["role"]["value"], "user")
        self.assertEqual(rec["message"]["value"], "本文")
        self.assertEqual(rec["category"]["value"], "paused:evt-s3",
                         "冪等キーを category へ保存")
        self.assertEqual(rec["auto_sent"]["value"], "no")


def _line_body(text="こんにちは", event_id="wev-p1", user=USER):
    body = json.dumps({"events": [{
        "type": "message", "webhookEventId": event_id,
        "replyToken": "rt", "source": {"userId": user},
        "message": {"type": "text", "text": text}}]}).encode()
    sig = base64.b64encode(
        hmac.new(b"dummy_secret", body, hashlib.sha256).digest()).decode()
    return body, sig


class TestConcurrentUniquenessPin(unittest.TestCase):
    """AUTOREPLY-03(i): 並行一意性は durable 状態機械が担保——を pin する。

    役割分担（本 suite が固定する契約）:
    - **並行一意性**（同一 webhook_event_id の同時 2 処理の排除）=
      record_line_event の排他 claim（hub/durable_inbound.py・UPDATE の
      guard+RETURNING で勝者 1 者のみ）と webhook 側の "duplicate"=登録 skip
      （main.py）が担保する
    - **順次 retry の冪等**（failed → LINE 再配送での App28 増分 0）=
      _paused_chatlog_already_saved の category 完全一致検索が担保する
      （TestFailClosed.test_retry_is_idempotent_for_chatlog_but_renotifies）
    DB は sqlite（test_rv05_13_durable.py の _DbMixin と同型・最小限）。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="pause03_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db",
            "INBOUND_EVENT_DURABLE_ENABLED": "1", "AUTOREPLY_PAUSED": "1"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(InboundBase.metadata.create_all)
        run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def test_exclusive_claim_single_winner_for_same_event(self):
        """同一 webhook_event_id の重複配送が両方 guard へ到達しても claim
        勝者は 1 者のみ（敗者="duplicate"=pause 処理は登録されない）"""
        kw = dict(user_id=USER, signature_result="verified",
                  payload=b"p", event_type="message")

        async def _scenario():
            o0 = await di.record_line_event(webhook_event_id="pz-excl", **kw)
            # タスク未実行のまま（滞留を模擬）2 つの再配送が claim を競合
            o1 = await di.record_line_event(webhook_event_id="pz-excl", **kw)
            o2 = await di.record_line_event(webhook_event_id="pz-excl", **kw)
            return o0, o1, o2

        o0, o1, o2 = run(_scenario())
        db.reset_for_tests()
        self.assertEqual(o0, "new")
        self.assertEqual([o1, o2].count("reattempt"), 1, "claim 勝者は 1 者のみ")
        self.assertEqual([o1, o2].count("duplicate"), 1, "敗者は登録 skip")

    def test_fresh_processing_redelivery_skips_pause_handling(self):
        """処理中（fresh processing）の再配送は webhook 側で登録 skip＝
        pause 経路（App28 保存）は並行実行されない。別 event id は通常どおり
        1 回処理される（positive control）"""
        paused = AsyncMock()
        client = TestClient(main.app)
        with patch.object(main, "_handle_paused_inbound", new=paused):
            # 先行 claim を模擬: received → processing（claimed_at=now・fresh）
            async def _claim():
                await di.record_line_event(
                    webhook_event_id="pz-busy", user_id=USER,
                    signature_result="verified", payload=b"p",
                    event_type="message")
                await di.mark_line_processing("pz-busy")
            run(_claim())
            db.reset_for_tests()

            body, sig = _line_body(event_id="pz-busy")
            r1 = client.post("/webhook", content=body,
                             headers={"X-Line-Signature": sig})
            self.assertEqual(r1.status_code, 200)
            paused.assert_not_awaited()   # 並行実行なし（duplicate=skip）

            body2, sig2 = _line_body(event_id="pz-free")
            r2 = client.post("/webhook", content=body2,
                             headers={"X-Line-Signature": sig2})
            self.assertEqual(r2.status_code, 200)
            paused.assert_awaited_once()  # 競合の無い event は 1 回だけ処理
            self.assertEqual(paused.await_args.args[2], "pz-free")
        db.reset_for_tests()


class TestPausedOff(_Base):
    def _run_and_assert_normal_hearing(self):
        run(main._process_line_event("rt", USER, "こんにちは"))
        self.claude.assert_awaited_once()
        self.reply.assert_awaited_once()
        self.notify.assert_not_awaited()
        self.chatlog.assert_not_awaited()

    def test_unset_behaves_as_before(self):
        """OFF（未設定）: 従来どおりヒアリングフロー（Claude→返信）へ進む"""
        os.environ.pop("AUTOREPLY_PAUSED", None)
        self._run_and_assert_normal_hearing()

    def test_zero_value_behaves_as_before(self):
        """OFF（=0 等の他値）: ON は "1" のみ（既定=従来動作と完全一致）"""
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "0"}):
            self._run_and_assert_normal_hearing()

    def test_off_human_mode_unchanged(self):
        """OFF×人対応顧客: 既存「人対応」経路が従来どおり動く（回帰 pin）"""
        self.app21.return_value = app21_record(name="佐藤花子", mode="人対応")
        os.environ.pop("AUTOREPLY_PAUSED", None)
        run(main._process_line_event("rt", USER, "相談です"))
        self._assert_no_autoreply_side_effects()
        self.chatlog.assert_awaited_once_with(USER, "user", "相談です",
                                              "", "no")
        self.notify.assert_awaited_once()
        self.assertIn("【人対応中】", self.notify.await_args.args[1])


if __name__ == "__main__":
    unittest.main()
