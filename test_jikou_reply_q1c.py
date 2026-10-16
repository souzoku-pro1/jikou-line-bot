"""JIKOU-REPLY-Q1c（QUEUE-2026-10-12-C C-1）: 許可表 V2（bot／承認済み下書き）・空返事の無言化・J-0・対応待ち

固定する仕様:
- flag JIKOU_SEND_POLICY_V2 未設定（既定 OFF）では既存経路の挙動が不変（降格時は PENDING_REPLY を送る・
  対応中を参照しない・App 39 照会失敗は fail-open・承認 webhook は従来の分岐）。
- (a) V2 ON: bot の送信は begin() の排他の中で decide_send を通す（対応中=停止・App 39 判定不能=停止）。
  承認済み下書きは人対応中・対応中（版一致なし）・判定不能で停止＝送信済み更新へ進まず noop 終端+通知。
- (b) 空返事（語彙表・言い換え・空文字・確認中定型の写し）は自動送信しない。V2 ON の承認降格は PENDING_REPLY を
  送らない（無言）。即時定型（閉集合）は従来どおり送る。障害・例外時も無言。
- (c) J-0 は会話に「初回返信判定の対象」の操作が無いときだけ 1 回（並行二重でも 1 通・2 通目以降は無言）。
  会話が無ければ送らない。
- (d) 無言にした受信は reply_pending（open）に残り、業務 LINE へ固定文言（App 29 No.・カテゴリのみ）を
  台帳経由で 1 会話 1 日 1 回通知。大野の返信（human／approved_draft）で human_replied。/app/pending は _gate。
- 凍結 pin: 顧客対応 prompt は「答えられない問いは空文字」の 1 項だけ追加（test_autoreply_style1 で再 pin）。
  J-0 文言は sha pin（大野承認待ち）。新 module は sink 違反ゼロ・allowlist 追加ゼロ・main/chat_responder の
  結線は 1 行置換と末尾追記のみ（pin 番地不変）。
"""

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
from hub import autoreply_stoplist  # noqa: E402
from hub import db  # noqa: E402
from hub import empty_reply_guard as eg  # noqa: E402
from hub import image_intake  # noqa: E402
from hub import line_channel  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import reply_followup as rf  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from hub import webapp_pending_view as pv  # noqa: E402
from hub.inbound_event import Base as InboundBase  # noqa: E402
from hub.webapp_auth import MIN_ITERATIONS, hash_password, issue_session  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
_client = TestClient(main.app)
JIKOU = line_channel.JIKOU_CHANNEL
USER = "U" + "a" * 32
USER2 = "U" + "b" * 32
ATTORNEY = "U" + "c" * 32
V2_ON = {sl.POLICY_V2_ENV: "1", "ATTORNEY_LINE_USER_ID": ATTORNEY,
         "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "biz_token"}
_ENV = {"WEBAPP_PASSWORD_HASH": hash_password("pw", iterations=MIN_ITERATIONS),
        "WEBAPP_SESSION_SECRET": "s" * 32, "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
        "APP_AUTOREPLY_STOP": "39", "TOKEN_AUTOREPLY_STOP": "t39"}
T0 = datetime.datetime(2026, 10, 12, 1, 0, tzinfo=datetime.timezone.utc)


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


def _texts() -> list[str]:
    return [(j or {}).get("messages", [{}])[0].get("text", "") for _u, j in _FakeClient.calls]


def _compose(reply: str, category: str = "挨拶・雑談", auto_send: bool = True, notice: str = "none"):
    return {"reply": reply, "category": category, "auto_send": auto_send,
            "jikou_update_flag": False, "immediate_notice": notice, "reason": "r"}


class _DbMixin(unittest.TestCase):
    """sqlite DB（送信台帳・手本・記録未了・対応待ち・inbound_event）＋ LINE/kintone/業務通知のフェイク。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="q1c_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", **_ENV})
        self._env.start()
        os.environ.pop(sl.POLICY_V2_ENV, None)
        os.environ.pop("AUTOREPLY_PAUSED", None)
        db.reset_for_tests()
        sp.clear_condition_cache()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                for md in (sl.metadata, rx.metadata, rf.metadata, rp.metadata, InboundBase.metadata):
                    await c.run_sync(md.create_all)
        _run(_create())
        _FakeClient.responses, _FakeClient.calls, _FakeClient.raise_exc = [], [], None
        self.biz = AsyncMock(return_value=True)                      # 業務 LINE push（固定文言）
        self.real_check = autoreply_stoplist.check
        self.real_is_suppressed = autoreply_stoplist.is_suppressed
        self.stop_check = AsyncMock(return_value=autoreply_stoplist.OUTCOME_CLEAR)
        self.app21 = AsyncMock(return_value=_rec(status="受任", 問い合わせ業者名="アコム", response_mode="自動"))
        self.chatlog = AsyncMock(return_value="800")
        self.approval = AsyncMock(return_value="500")
        self.notify_attorney = AsyncMock()
        self._patches = [
            patch.object(line_channel.httpx, "AsyncClient", _FakeClient),
            patch.object(sp, "_push_business_http", self.biz),
            patch.object(autoreply_stoplist, "check", self.stop_check),
            patch.object(autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)),
            patch.object(cr, "get_app21_record", self.app21),
            patch.object(cr, "save_to_chatlog", self.chatlog),
            patch.object(cr, "save_to_approval_queue", self.approval),
            patch.object(cr, "_notify_attorney", self.notify_attorney),
            patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=cr.ChatHistory())),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self):
        # 入れ子の patch.dict は内側から止める（外側を先に止めると内側の snapshot が DATABASE_URL を復活させ、
        # 後続テストが消えた sqlite を掴む＝順序依存の OperationalError）
        for p in reversed(getattr(self, "_extra_patches", [])):
            p.stop()
        for p in self._patches:
            p.stop()
        db.reset_for_tests()
        sp.clear_condition_cache()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def env(self, values: dict):
        """_env より内側の env patch（tearDown で先に止める）。"""
        p = patch.dict(os.environ, values)
        p.start()
        self._extra_patches = getattr(self, "_extra_patches", []) + [p]

    def on(self):
        self.env(V2_ON)
        sp.clear_condition_cache()

    def _rows(self, table) -> list:
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(table))).fetchall()]
        return _run(_q())

    def ops(self) -> list:
        return self._rows(sl.send_operation)

    def pending(self) -> list:
        return self._rows(rp.reply_pending)

    def touch(self, event_id: str, user=USER, at=T0):
        _run(sl.touch_inbound("jikou", user, event_id, received_at=at))

    def run_r(self, coro):
        """coro を実行し (結果, その task 内で記録された停止理由) を返す（ContextVar は task 内で読む）。"""
        async def _w():
            r = await coro
            return r, sp.last_stop_reasons()
        return _run(_w())

    def reply_func(self):
        async def _f(token: str, text: str):
            return await line_channel.reply_with_push_fallback(JIKOU, token, USER, text)
        return _f

    def customer(self, result: dict, text: str = "質問です", exc=None):
        call = AsyncMock(return_value=result) if exc is None else AsyncMock(side_effect=exc)
        with patch.object(cr, "_call_compose_reply", call):
            _run(cr.handle_customer_message(user_id=USER, user_message=text, reply_token="rt",
                                            app21_record=self.app21.return_value,
                                            reply_func=self.reply_func()))


# ── 1. 空返事の検出（語彙表・言い換え・純関数） ────────────────────────────────
class TestEmptyReplyGuard(unittest.TestCase):
    POSITIVE = (
        "", "   ", cr.PENDING_REPLY, cr.PENDING_BY_CATEGORY["費用の定型案内"],
        "ご質問ありがとうございます。担当者より改めてご連絡いたします。",
        "内容を確認の上、改めてご案内いたします。",
        "確認のうえご連絡いたします。",
        "確認して折り返しご連絡します。",
        "確認いたしまして、ご返信させていただきます。",
        "ご要望は弁護士に申し伝えます。",
        "担当の者からご連絡させていただきます。",
        "追ってご連絡いたします。",
        "ご連絡ありがとうございます。確認いたします。",
        "お伺いした内容を確認中です。",
        "少々お時間をいただく場合がございますが、何卒よろしくお願いいたします。",
    )
    NEGATIVE = (
        cr.FEE_GUIDE_TEXT, cr.HOTERASU_STANDARD_REPLY, cr.COURT_DOC_REQUEST_REPLY,
        "最後に返済されたのはいつ頃でしょうか。だいたいで構いません。",
        "受任から完了まで2〜4週間程度です。",
        "最終返済日を確認させていただきます。最後に返済されたのはいつ頃でしょうか。",
        "本日△△に対して、時効援用通知を送付致します。今後は結果の確認までお時間をいただき、手続きを進めさせていただきます。",
        "弁護士の責任のもとで最初から最後まで対応いたします。ご案内の一部にAIを活用していますが、法的判断と手続きはすべて弁護士が行います。",
        "ご質問の点につきましては、当事務所で対応しております。",
    )

    def test_vocabulary_and_paraphrases(self):
        for t in self.POSITIVE:
            with self.subTest(t=t[:20]):
                self.assertTrue(eg.is_empty_reply(t))
        for t in self.NEGATIVE:
            with self.subTest(t=t[:20]):
                self.assertEqual(eg.empty_reply_labels(t), [])

    def test_labels_are_closed_vocabulary(self):
        labels = {l for l, _ in eg.EMPTY_REPLY_PATTERNS} | {eg.LABEL_EMPTY, eg.LABEL_PENDING_TEMPLATE,
                                                         eg.LABEL_CONFIRM_ONLY}
        for t in self.POSITIVE:
            for l in eg.empty_reply_labels(t):
                self.assertIn(l, labels)
        self.assertEqual(eg.empty_reply_labels(""), [eg.LABEL_EMPTY])
        self.assertIn(eg.LABEL_PENDING_TEMPLATE, eg.empty_reply_labels("はい。" + cr.PENDING_REPLY))

    def test_j0_is_fixed_text_not_subject_to_guard_decision(self):
        # J-0 は語彙表に掛かる文だが閉集合の固定文（purpose=first_reply で送る）＝検査対象外は呼出側の契約
        self.assertTrue(eg.is_empty_reply(sp.J0_TEXT))
        self.assertEqual(sp.J0_TEXT_SHA256, "0f1c3ca29956f4000aface287c8dc9aa7c6907e93c9b7f1267858a6c2212b03d")


# ── 2. apply_v2（ガード結果への重ね掛け・OFF/ON） ─────────────────────────────
class TestApplyV2(unittest.TestCase):
    def setUp(self):
        os.environ.pop(sl.POLICY_V2_ENV, None)

    def _guard(self, reply, **kw):
        return cr.apply_server_guards(_compose(reply, **kw), [], "q")

    def test_off_keeps_vocabulary_replies_but_demotes_empty_string(self):
        g = sp.apply_v2(self._guard("確認のうえご連絡いたします。"), "確認のうえご連絡いたします。")
        self.assertTrue(g.can_auto_send)
        g = sp.apply_v2(self._guard(""), "")
        self.assertFalse(g.can_auto_send)
        self.assertEqual(g.demotion_reasons, [sp.EMPTY_REPLY_REASON_PREFIX + eg.LABEL_EMPTY])
        self.assertEqual(g.immediate_notice, "none")

    def test_on_demotes_vocabulary_without_immediate_notice(self):
        with patch.dict(os.environ, V2_ON):
            g = sp.apply_v2(self._guard("担当者より改めてご連絡いたします。"), "担当者より改めてご連絡いたします。")
            self.assertFalse(g.can_auto_send)
            self.assertTrue(g.demotion_reasons[0].startswith(sp.EMPTY_REPLY_REASON_PREFIX))
            self.assertEqual(g.immediate_notice, "none")
            g2 = sp.apply_v2(self._guard("受任から完了まで2〜4週間程度です。"), "受任から完了まで2〜4週間程度です。")
            self.assertTrue(g2.can_auto_send)
            demoted = self._guard("x", category="法的判断・見通し", auto_send=False)
            self.assertIs(sp.apply_v2(demoted, "x"), demoted)       # 既に降格は触らない


# ── 3. 顧客対応経路（OFF 不変／ON 無言+対応待ち+通知・即時定型・J-0・障害） ────────
class TestCustomerPath(_DbMixin):
    def test_off_demotion_sends_pending_reply_as_before(self):
        self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        self.assertEqual(_texts(), [cr.PENDING_REPLY])
        self.assertEqual(self.pending(), [])
        self.biz.assert_not_awaited()
        self.assertEqual([c.args[1:3] for c in self.chatlog.await_args_list][-1], ("assistant", cr.PENDING_REPLY))

    def test_off_vocabulary_reply_is_sent_as_before(self):
        self.customer(_compose("確認のうえご連絡いたします。"))
        self.assertEqual(_texts(), ["確認のうえご連絡いたします。"])

    def test_on_demotion_is_silent_with_pending_and_one_notice_per_day(self):
        self.on()
        self.touch("e1")
        self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        # J-0（会話 1 件目）だけ送られ、PENDING_REPLY は送られない
        self.assertEqual(_texts(), [sp.J0_TEXT])
        rows = self.pending()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["route"], rows[0]["reason"], rows[0]["approval_record_id"],
                          rows[0]["category"], rows[0]["state"], rows[0]["notify_state"],
                          rows[0]["customer_send"]),
                         ("customer", "demoted", "500", "法的判断・見通し", "open", "sent", "j0_sent"))
        self.assertEqual(self.biz.await_count, 1)
        text = self.biz.await_args.args[1]
        self.assertIn("No.500", text)
        self.assertIn("法的判断・見通し", text)
        self.assertNotIn(USER, text)
        self.assertEqual(self.biz.await_args.args[0], ATTORNEY)
        # J-0 の送信操作記録（purpose=first_reply・sent）
        self.assertEqual([(o["purpose"], o["state"]) for o in self.ops() if o["channel"] == "jikou"],
                         [("first_reply", "sent")])
        self.notify_attorney.assert_awaited_once()           # 既存の承認依頼通知は従来どおり
        # 2 回目（同じ日・同じ会話）: 無言・通知は省略（throttled）・J-0 なし
        _FakeClient.calls.clear()
        self.customer(_compose("y", category="法的判断・見通し", auto_send=False))
        self.assertEqual(_texts(), [])
        rows = self.pending()
        self.assertEqual(len(rows), 2)
        self.assertEqual((rows[1]["notify_state"], rows[1]["customer_send"]), ("throttled", "none"))
        self.assertEqual(self.biz.await_count, 1)
        assistant_logs = [c.args[2] for c in self.chatlog.await_args_list if c.args[1] == "assistant"]
        self.assertEqual(assistant_logs, [sp.J0_TEXT])           # 無言は記録しない

    def test_on_empty_reply_on_auto_send_is_silent_and_queued(self):
        self.on()
        self.customer(_compose("内容を確認の上、改めてご案内いたします。"))
        self.assertEqual(_texts(), [])                            # 会話が無い＝J-0 も送らない
        self.approval.assert_awaited_once()
        self.assertEqual(self.pending()[0]["reason"], "empty_reply")
        reason = self.approval.await_args.kwargs["reason"]
        self.assertIn(sp.EMPTY_REPLY_REASON_PREFIX, reason)

    def test_on_immediate_notice_is_still_sent(self):
        self.on()
        self.customer(_compose("x", category="緊急対応", auto_send=False, notice="court_doc_request"),
                      text="裁判所から書類が届きました")
        self.assertEqual(_texts(), [cr.COURT_DOC_REQUEST_REPLY])
        self.assertEqual(self.pending(), [])
        self.biz.assert_not_awaited()

    def test_on_outage_is_silent_and_pending_gets_approval_no_later(self):
        self.on()
        self.touch("e2")
        from claude_gateway import ClaudeUnavailableError
        self.customer(_compose("x"), exc=ClaudeUnavailableError("down"))
        rows = self.pending()
        self.assertEqual((rows[0]["reason"], rows[0]["approval_record_id"]), ("claude_outage", "500"))
        self.assertEqual(_texts(), [sp.J0_TEXT])
        # 一般例外（レート制限等）も無言
        _FakeClient.calls.clear()
        self.customer(_compose("x"), exc=RuntimeError("429"))
        self.assertEqual(_texts(), [])
        self.assertEqual(self.pending()[1]["reason"], "claude_outage")

    def test_off_outage_sends_pending_reply_as_before(self):
        from claude_gateway import ClaudeUnavailableError
        self.customer(_compose("x"), exc=ClaudeUnavailableError("down"))
        self.assertEqual(_texts(), [cr.PENDING_REPLY])
        self.assertEqual(self.pending(), [])

    def test_on_pending_save_failure_means_no_j0(self):
        self.on()
        self.touch("e3")
        with patch.object(rp, "register", AsyncMock(return_value=None)):
            self.customer(_compose("x", category="法的判断・見通し", auto_send=False))
        self.assertEqual(_texts(), [])
        self.biz.assert_not_awaited()


# ── 4. J-0 の 1 回性（排他内の在庫検査・並行二重・会話なし） ───────────────────
class TestFirstReply(_DbMixin):
    def _j0(self):
        with sl.purpose(sl.PURPOSE_FIRST_REPLY):
            return _run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, sp.J0_TEXT))

    def test_second_j0_is_refused_in_ledger_without_http(self):
        self.on()
        self.touch("e1")
        self.assertEqual(self._j0(), sl.SEND_SENT)
        with sl.purpose(sl.PURPOSE_FIRST_REPLY):
            res, reasons = self.run_r(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, sp.J0_TEXT))
        self.assertEqual((res, reasons), (sl.SEND_POLICY_STOPPED, ("first_reply:already",)))
        self.assertEqual(len(_FakeClient.calls), 1)

    def test_any_first_reply_target_blocks_j0(self):
        self.on()
        self.touch("e1")
        _run(line_channel.push_text(JIKOU, USER, "通常の返信"))      # purpose=reply も対象
        self.assertFalse(_run(sp.first_reply_due(USER)))
        self.assertEqual(self._j0(), sl.SEND_POLICY_STOPPED)
        self.touch("e2", at=T0 + datetime.timedelta(days=40))   # 新しい会話
        self.assertTrue(_run(sp.first_reply_due(USER)))
        self.assertEqual(self._j0(), sl.SEND_SENT)

    def test_follow_is_not_a_first_reply_target(self):
        self.on()
        self.touch("e1")
        _run(sl.with_purpose("follow", line_channel.push_text, JIKOU, USER, "あいさつ"))
        self.assertTrue(_run(sp.first_reply_due(USER)))

    def test_concurrent_double_j0_sends_once(self):
        self.on()
        self.touch("e1")

        async def _both():
            with sl.purpose(sl.PURPOSE_FIRST_REPLY):
                return await asyncio.gather(
                    line_channel.reply_with_push_fallback(JIKOU, "rt", USER, sp.J0_TEXT),
                    line_channel.reply_with_push_fallback(JIKOU, "rt", USER, sp.J0_TEXT))
        res = _run(_both())
        self.assertEqual(sorted(res), sorted([sl.SEND_SENT, sl.SEND_POLICY_STOPPED]))
        self.assertEqual(len(_FakeClient.calls), 1)

    def test_no_conversation_means_no_j0(self):
        self.on()
        self.assertFalse(_run(sp.first_reply_due(USER)))
        self.assertEqual(self._j0(), sl.SEND_POLICY_STOPPED)
        self.assertEqual(_FakeClient.calls, [])

    def test_off_first_reply_purpose_sends_as_before(self):
        self.touch("e1")
        self.assertEqual(self._j0(), sl.SEND_SENT)
        self.assertEqual(self._j0(), sl.SEND_SENT)
        self.assertEqual(len(_FakeClient.calls), 2)


# ── 5. 許可表 V2（bot／承認済み下書き・排他内・判定不能・OFF 不変） ────────────────
class TestPolicyV2Ledger(_DbMixin):
    def test_off_bot_ignores_attending_and_stoplist_failure(self):
        _run(sl.set_attending("jikou", USER))
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_UNKNOWN
        self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)
        self.assertEqual(len(_FakeClient.calls), 1)
        self.stop_check.assert_not_awaited()                    # OFF は停止条件を集めない
        self.app21.assert_not_awaited()

    def test_on_bot_is_stopped_while_attending(self):
        self.on()
        _run(sl.set_attending("jikou", USER))
        self.assertEqual(self.run_r(line_channel.push_text(JIKOU, USER, "x")),
                         (sl.SEND_POLICY_STOPPED, ("attending:stopped",)))
        self.assertEqual(_run(line_channel.reply_with_push_fallback(JIKOU, "rt", USER, "x")),
                         sl.SEND_POLICY_STOPPED)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])                        # 行を作らない
        _run(sl.end_attending("jikou", USER))
        sp.clear_condition_cache()
        self.assertIs(_run(line_channel.push_text(JIKOU, USER, "x")), True)

    def test_on_stoplist_unknown_stops_bot_and_approved_draft(self):
        self.on()
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_UNKNOWN
        self.assertEqual(self.run_r(line_channel.push_text(JIKOU, USER, "x")),
                         (sl.SEND_POLICY_STOPPED, ("stoplisted:unknown",)))

        async def _draft():
            async with sl.approved_draft("approval:1:1"):
                return await line_channel.push_text(JIKOU, USER, "draft")
        self.assertEqual(self.run_r(_draft()), (sl.SEND_POLICY_STOPPED, ("stoplisted:unknown",)))
        self.assertEqual(_FakeClient.calls, [])

    def test_on_approved_draft_stops_on_human_mode_attending_and_pause(self):
        self.on()

        async def _draft(op):
            async with sl.approved_draft(op):
                return await line_channel.push_text(JIKOU, USER, "draft")
        self.assertIs(_run(_draft("approval:1:1")), True)                   # 停止条件なし → 送る
        self.app21.return_value = _rec(status="受任", response_mode="人対応")
        sp.clear_condition_cache()
        self.assertEqual(self.run_r(_draft("approval:2:1")),
                         (sl.SEND_POLICY_STOPPED, ("human_mode:stopped:version_mismatch",)))
        self.app21.return_value = _rec(status="受任", response_mode="自動")
        sp.clear_condition_cache()
        _run(sl.set_attending("jikou", USER))
        self.assertEqual(self.run_r(_draft("approval:3:1")),
                         (sl.SEND_POLICY_STOPPED, ("attending:version_mismatch",)))
        _run(sl.end_attending("jikou", USER))
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            sp.clear_condition_cache()
            self.assertEqual(self.run_r(_draft("approval:4:1")),
                             (sl.SEND_POLICY_STOPPED, ("paused:stopped:reapproval_required",)))
        self.assertEqual(len(_FakeClient.calls), 1)

    def test_human_explicit_send_is_not_subject_to_v2(self):
        self.on()
        _run(sl.set_attending("jikou", USER))
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_UNKNOWN
        res = _run(line_channel.push_text_explicit(JIKOU, "f" * 32, USER, "人の返信"))
        self.assertEqual(res.outcome, line_channel.PUSH_SENT)

    def test_other_business_and_notice_channel_are_not_evaluated(self):
        self.on()
        self.app21.side_effect = RuntimeError("must not be called")
        self.stop_check.side_effect = RuntimeError("must not be called")
        self.assertEqual(_run(sp.send_business_notice("k1", "固定文言")), rp.NOTIFY_SENT)
        self.assertEqual(_run(sp.send_business_notice("k1", "固定文言")), rp.NOTIFY_THROTTLED)
        self.assertEqual(self.biz.await_count, 1)

    def test_conditions_are_cached_briefly(self):
        self.on()
        _run(sp.conditions(USER))
        _run(sp.conditions(USER))
        self.assertEqual(self.stop_check.await_count, 1)
        self.assertEqual(self.app21.await_count, 1)
        sp.clear_condition_cache()
        _run(sp.conditions(USER))
        self.assertEqual(self.stop_check.await_count, 2)

    def test_stoplist_check_tristate_keeps_is_suppressed_fail_open(self):
        check, is_suppressed = self.real_check, self.real_is_suppressed
        with patch.object(autoreply_stoplist.kintone, "search_records",
                          AsyncMock(side_effect=RuntimeError("x"))), \
             patch.object(autoreply_stoplist, "_on_lookup_failure", AsyncMock()):
            self.assertEqual(_run(check(USER)), "unknown")
            self.assertFalse(_run(is_suppressed(USER)))
        with patch.object(autoreply_stoplist.kintone, "search_records",
                          AsyncMock(return_value=[{"$id": {"value": "1"}}])):
            self.assertEqual(_run(check(USER)), "stopped")
            self.assertTrue(_run(is_suppressed(USER)))
        with patch.object(autoreply_stoplist.kintone, "search_records", AsyncMock(return_value=[])):
            self.assertEqual(_run(check(USER)), "clear")
        with patch.dict(os.environ, {"APP_AUTOREPLY_STOP": "", "TOKEN_AUTOREPLY_STOP": ""}):
            self.assertEqual(_run(check(USER)), "unconfigured")
        self.assertEqual(_run(check("bad")), "unconfigured")

    def test_image_receipt_is_held_not_failed_when_policy_stops(self):
        self.on()
        _run(sl.set_attending("jikou", USER))
        hold = AsyncMock(return_value=True)
        fail = AsyncMock()
        with patch.object(image_intake, "hold_for_human_mode", hold), \
             patch.object(image_intake, "_notify_send_failure", fail), \
             patch.object(image_intake, "_latest_marker_row", AsyncMock(return_value=("10", "画像受領:jikou:ev1"))), \
             patch.object(image_intake, "_latest_receipt_row_id", AsyncMock(return_value=None)), \
             patch.object(image_intake, "_latest_row_id_by_category", AsyncMock(return_value=None)):
            res = _run(image_intake.send_receipt_and_close("jikou", JIKOU, USER))
        self.assertIsNone(res)
        hold.assert_awaited_once_with("jikou", USER)
        fail.assert_not_awaited()
        self.assertEqual(_FakeClient.calls, [])


# ── 6. 受信入口（main）: 対応中・判定不能は無言+記録+対応待ち+通知（OFF は従来） ────
class TestInboundStop(_DbMixin):
    def setUp(self):
        super().setUp()
        self.main_chatlog = AsyncMock(return_value="801")
        self._mp = patch.object(main, "save_to_chatlog", self.main_chatlog)
        self._mp.start()
        self._patches.append(self._mp)

    def test_off_returns_empty_and_does_not_consult_conditions(self):
        _run(sl.set_attending("jikou", USER))
        self.assertEqual(_run(main._inbound_stop_kind(USER, _rec(response_mode="自動"))), "")
        self.assertEqual(_run(main._inbound_stop_kind(USER, _rec(response_mode="人対応"))), main.STOP_HUMAN)
        self.stop_check.assert_not_awaited()

    def test_on_attending_stops_inbound_silently_with_pending_and_notice(self):
        self.on()
        self.touch("e1")
        _run(sl.set_attending("jikou", USER))
        kind = _run(main._inbound_stop_kind(USER, _rec(response_mode="自動")))
        self.assertEqual(kind, main.STOP_POLICY)
        self.approval.return_value = None
        with patch.object(sp.kintone, "search_records", AsyncMock(return_value=[{"$id": {"value": "77"}}])):
            _run(main._handle_stopped_inbound(kind, USER, "こんにちは", _rec(response_mode="自動")))
        self.main_chatlog.assert_awaited_once_with(USER, "user", "こんにちは", "", "no")
        rows = self.pending()
        self.assertEqual((rows[0]["route"], rows[0]["reason"], rows[0]["approval_record_id"], rows[0]["customer_send"]),
                         ("inbound", "policy_stopped", "77", "none"))
        self.assertEqual(_FakeClient.calls, [])                 # 顧客へは何も送らない（J-0 も送らない）
        self.assertEqual(self.biz.await_count, 1)
        self.assertIn("No.77", self.biz.await_args.args[1])

    def test_on_stoplist_unknown_stops_inbound(self):
        self.on()
        self.stop_check.return_value = autoreply_stoplist.OUTCOME_UNKNOWN
        self.assertEqual(_run(main._inbound_stop_kind(USER, _rec(response_mode="自動"))), main.STOP_POLICY)

    def test_human_mode_keeps_existing_handler(self):
        self.on()
        h = AsyncMock()
        with patch.object(main, "_handle_human_mode_inbound", h):
            _run(main._handle_stopped_inbound(main.STOP_HUMAN, USER, "t", _rec(response_mode="人対応")))
        h.assert_awaited_once()
        self.assertEqual(self.pending(), [])


# ── 7. 承認 webhook（V2 ON: 停止=送信済み更新へ進まず noop 終端+通知／OFF 不変） ────
class TestApprovalWebhook(_DbMixin):
    def setUp(self):
        super().setUp()
        self.env({"KINTONE_WEBHOOK_TOKEN": "kintone-token"})
        os.environ.pop("KINTONE_EVENT_DEDUP_ENABLED", None)

    def _record(self):
        return {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"},
                "line_user_id": {"value": USER}, "AI下書き": {"value": "下書き"},
                "カテゴリ": {"value": "c"}, "$revision": {"value": "3"}}

    def _post(self, rid="7"):
        body = {"record": {"$id": {"value": rid}, "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}}}
        return _client.post("/webhook/kintone/approval?token=kintone-token", json=body)

    def test_on_attending_blocks_approved_draft_and_notifies_once_per_revision(self):
        self.on()
        self.touch("e1")
        _run(sl.set_attending("jikou", USER))
        mark = AsyncMock()
        with patch.object(main, "mark_approval_sent", mark), \
             patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=self._record())):
            r1 = self._post()
            r2 = self._post()
        self.assertEqual(r1.json().get("skip"), "policy_stopped")
        self.assertEqual(r2.json().get("skip"), "policy_stopped")
        mark.assert_not_awaited()
        self.assertEqual([j["to"] for u, j in _FakeClient.calls if "push" in u], [])
        self.assertEqual(self.biz.await_count, 1)                # 同じ承認版は 1 通
        self.assertIn("No.7", self.biz.await_args.args[1])
        self.assertIn("attending:version_mismatch", self.biz.await_args.args[1])

    def test_on_sent_draft_resolves_pending(self):
        self.on()
        self.touch("e1")
        _run(rp.register(business="jikou", channel="jikou", line_user_hash=sl.line_user_hash("jikou", USER),
                         route="customer", reason="demoted", approval_record_id="7"))
        mark = AsyncMock()
        with patch.object(main, "mark_approval_sent", mark), \
             patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=self._record())):
            r = self._post()
        self.assertEqual(r.json().get("record_id"), "7")
        mark.assert_awaited_once()
        self.assertEqual((self.pending()[0]["state"], self.pending()[0]["resolved_by"]),
                         ("human_replied", "approved_draft"))

    def test_off_webhook_sends_even_while_attending(self):
        _run(sl.set_attending("jikou", USER))
        mark = AsyncMock()
        with patch.object(main, "mark_approval_sent", mark), \
             patch.object(main, "save_to_chatlog", AsyncMock()), \
             patch.object(main.hub_kintone, "get_record", AsyncMock(return_value=self._record())):
            r = self._post()
        self.assertEqual(r.json().get("record_id"), "7")
        mark.assert_awaited_once()
        self.assertEqual(len(_FakeClient.calls), 1)


# ── 8. 対応待ち表・画面（_gate・不要・対応終了・human で解除） ──────────────────
class TestPendingTableAndView(_DbMixin):
    def _reg(self, user=USER, **kw):
        base = dict(business="jikou", channel="jikou", line_user_hash=sl.line_user_hash("jikou", user),
                    route="customer", reason="demoted", category="費用交渉・減額相談", approval_record_id="500")
        base.update(kw)
        return _run(rp.register(**base))

    def test_register_is_idempotent_per_inbound_event_and_keeps_history(self):
        a = self._reg(inbound_event_id="ev1")
        b = self._reg(inbound_event_id="ev1")
        c = self._reg(inbound_event_id=None)
        d = self._reg(inbound_event_id=None)
        self.assertEqual(a, b)
        self.assertNotEqual(c, d)
        self.assertEqual(len(self.pending()), 3)
        self.assertEqual([h["to"] for h in _run(rp.history(a))], ["open"])
        self.assertIsNone(_run(rp.register(business="jikou", channel="jikou", line_user_hash="h", route="x",
                                           reason="demoted")))

    def test_human_reply_resolves_all_open_for_user_only(self):
        self._reg(inbound_event_id="ev1")
        self._reg(inbound_event_id="ev2")
        self._reg(user=USER2, inbound_event_id="ev3")
        self.assertEqual(_run(sp.human_replied(USER)), 2)
        states = {r["line_user_hash"]: r["state"] for r in self.pending()}
        self.assertEqual(states[sl.line_user_hash("jikou", USER)], "human_replied")
        self.assertEqual(states[sl.line_user_hash("jikou", USER2)], "open")
        self.assertEqual(_run(sp.human_replied(USER)), 0)
        self.assertEqual(_run(rp.count_open()), 1)

    def test_view_is_gated_and_closed(self):
        self.assertEqual(_client.get("/app/pending", follow_redirects=False).status_code, 303)
        self.assertEqual(_client.post("/app/pending/resolve", data={"pending_id": "1", "outcome": "not_needed"},
                                      follow_redirects=False).status_code, 303)
        for r in pv.router.routes:
            self.assertTrue(getattr(r.endpoint, "__webapp_gate__", False), r.path)
        from hub.webapp_brain_view import _paths_of
        paths = [p for entry in main.app.router.routes for p in _paths_of(entry)
                 if p in ("/app/pending", "/app/{_rest:path}")]
        self.assertEqual(paths, ["/app/pending", "/app/{_rest:path}"])

    def test_view_lists_without_pii_and_supports_not_needed_and_end_attending(self):
        pid = self._reg(inbound_event_id="ev1")
        _run(sl.set_attending("jikou", USER))
        r = _client.get("/app/pending", headers=_auth())
        self.assertEqual(r.status_code, 200)
        self.assertIn("/app/reply/500", r.text)
        self.assertIn("対応中（bot 停止）", r.text)
        self.assertIn("費用交渉・減額相談", r.text)
        self.assertNotIn(USER, r.text)
        self.assertEqual(r.headers.get("cache-control"), "no-store, private")
        h = sl.line_user_hash("jikou", USER)
        r = _client.post("/app/pending/end_attending", data={"user_hash": h}, headers=_auth(),
                         follow_redirects=False)
        self.assertEqual((r.status_code, r.headers["location"]), (303, "/app/pending?done=resumed"))
        self.assertFalse(_run(sl.conversation_state("jikou", USER))["attending"])
        r = _client.post("/app/pending/resolve", data={"pending_id": str(pid), "outcome": "not_needed"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/pending?done=ok")
        self.assertEqual(self.pending()[0]["state"], "not_needed")
        r = _client.post("/app/pending/resolve", data={"pending_id": str(pid), "outcome": "not_needed"},
                         headers=_auth(), follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/pending?done=not_open")
        self.assertEqual(_client.post("/app/pending/resolve", data={"pending_id": "x", "outcome": "not_needed"},
                                      headers=_auth(), follow_redirects=False).status_code, 400)
        self.assertEqual(_client.post("/app/pending/end_attending", data={"user_hash": "zz"},
                                      headers=_auth(), follow_redirects=False).status_code, 400)
        api = _client.get("/app/api/pending/open", headers=_auth()).json()
        self.assertEqual((api["ok"], api["count"]), (True, 0))

    def test_api_omits_user_hash(self):
        self._reg(inbound_event_id="ev1")
        api = _client.get("/app/api/pending/open", headers=_auth()).json()
        self.assertEqual(api["count"], 1)
        self.assertNotIn("line_user_hash", api["items"][0])


# ── 9. ヒアリング経路の入口（demoted_ack・settle・violations） ────────────────────
class TestHearingHelpers(_DbMixin):
    def test_off_demoted_ack_sends_pending_reply_and_settles_history(self):
        send = AsyncMock()
        self.assertEqual(_run(sp.demoted_ack(USER, "rt", "500", "その他判断系", send, cr.PENDING_REPLY)),
                         cr.PENDING_REPLY)
        send.assert_awaited_once_with("rt", USER, cr.PENDING_REPLY)
        hist = [{"role": "user", "content": "q"}, {"role": "assistant", "content": "draft"}]
        sp.settle_demoted_history(hist, cr.PENDING_REPLY)
        self.assertEqual(hist[-1]["content"], cr.PENDING_REPLY)
        self.assertEqual(sp.empty_reply_violations("確認のうえご連絡いたします。"), [])

    def test_on_demoted_ack_is_silent_or_j0_and_drops_history(self):
        self.on()
        send = AsyncMock()
        self.assertEqual(_run(sp.demoted_ack(USER, "rt", "500", "その他判断系", send, cr.PENDING_REPLY)), "")
        send.assert_not_awaited()
        self.assertEqual(self.pending()[0]["route"], "hearing")
        self.touch("e1", user=USER2)
        self.assertEqual(_run(sp.demoted_ack(USER2, "rt", "501", "その他判断系", send, cr.PENDING_REPLY)),
                         sp.J0_TEXT)
        send.assert_awaited_once_with("rt", USER2, sp.J0_TEXT)
        hist = [{"role": "user", "content": "q"}, {"role": "assistant", "content": "draft"}]
        sp.settle_demoted_history(hist, cr.PENDING_REPLY)
        self.assertEqual(hist, [{"role": "user", "content": "q"}])
        self.assertEqual(sp.empty_reply_violations("確認のうえご連絡いたします。"),
                         [sp.EMPTY_REPLY_REASON_PREFIX + eg.LABEL_FOLLOW_UP_PROMISE])
        self.assertEqual(sp.empty_reply_violations("受任から完了まで2〜4週間程度です。"), [])


# ── 10. pin（prompt・結線・sink・migration） ──────────────────────────────────────
class TestPins(unittest.TestCase):
    NEW_FILES = ("hub/send_policy.py", "hub/empty_reply_guard.py", "hub/reply_pending.py",
                 "hub/webapp_pending_view.py")

    def test_prompt_rule_added_once_and_pending_reply_frozen(self):
        base = cr._SYSTEM_PROMPT_BASE
        self.assertEqual(base.count("は reply を空文字にする"), 1)
        self.assertIn("「確認のうえご連絡します」「申し伝えます」「担当者よりご連絡します」のような返事は書かない", base)
        self.assertEqual(cr.PENDING_REPLY, "ありがとうございます。内容を確認の上、改めてご連絡いたします。\n"
                                            "少々お時間をいただく場合がございますが、何卒よろしくお願いいたします。")

    def test_new_modules_have_zero_sink_violations_and_no_allowlist_entries(self):
        # autoreply_stoplist は既存の allowlist 番地（76/103）を持つため対象外（番地不変は test_sink_ast_policy が担保）
        for name in self.NEW_FILES + ("hub/send_ledger.py", "hub/line_channel.py",
                                      "hub/image_intake.py", "hub/webapp_reply_view.py"):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for key in ("send_policy", "empty_reply_guard", "reply_pending", "webapp_pending_view"):
            self.assertNotIn(key, allow)
        self.assertEqual(sorted(int(e.split(":")[1]) for e in json.loads(allow)["entries"]
                                if e.startswith("hub/autoreply_stoplist.py:")), [76, 103])
        entries = json.loads(allow)["entries"]
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries if e.startswith("main.py:")),
                         [663, 685, 1320, 1620, 1917])
        self.assertEqual(sorted(int(e.split(":")[1]) for e in entries
                                if e.startswith("chat_responder.py:")), [1660, 1672])

    def test_wiring_is_one_line_replacements_and_tail_only(self):
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        self.assertIn("include_before_catch_all(app, pending_view_router)", "\n".join(src[-60:]))
        self.assertEqual(sum("_inbound_stop_kind(user_id, app21_record)" in l for l in src), 1)
        self.assertEqual(sum("send_policy.demoted_ack(" in l for l in src), 1)
        self.assertEqual(sum("send_policy.empty_reply_violations(cleaned)" in l for l in src), 1)
        self.assertEqual(sum("_approval_policy_stopped(_ev, record_id, record)" in l for l in src), 1)
        self.assertEqual(src[1601].count("SEND_POLICY_STOPPED"), 1)
        self.assertNotIn("policy_v2_enabled", "\n".join(src))
        self.assertEqual("\n".join(src).count("is_human_mode("), 4)
        csrc = (REPO / "chat_responder.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("send_policy.apply_v2(apply_server_guards(" in l for l in csrc), 1)
        self.assertEqual(sum("send_policy.pending_ack(" in l for l in csrc), 1)
        self.assertEqual(sum("send_policy.outage_ack(" in l for l in csrc), 2)
        self.assertEqual(sum("send_policy.ack_purpose(" in l for l in csrc), 1)
        self.assertNotIn("pending_reply_for(category, profile=p)", "\n".join(csrc))
        # Q1a の pin（既存経路は対応中・decide_send を直接参照しない）を本票でも維持
        for name in ("hub/line_channel.py", "main.py", "chat_responder.py", "hub/image_intake.py"):
            s = (REPO / name).read_text(encoding="utf-8")
            for banned in ("set_attending", "conversation_state", "decide_send"):
                self.assertNotIn(banned, s, name)

    def test_migration_chains_after_reply_exemplar_and_refuses_downgrade_with_data(self):
        p = REPO / "alembic" / "versions" / "20261012_a1c3e5f7b9d2_reply_pending.py"
        src = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'a1c3e5f7b9d2'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'f7b2c9d4e1a6'", src)
        self.assertIn("reply_pending_downgrade_refused_data_present", src)
        self.assertIn("データがあれば downgrade 禁止", src)
        env = (REPO / "alembic" / "env.py").read_text(encoding="utf-8")
        self.assertIn("reply_pending_metadata", env)

    def test_closed_sets(self):
        self.assertEqual(rp.STATES, ("open", "human_replied", "auto_replied", "not_needed"))
        self.assertEqual(rp.REASONS, ("demoted", "empty_reply", "policy_stopped", "claude_outage"))
        self.assertEqual(sl.SEND_POLICY_STOPPED, "policy_stopped")
        self.assertEqual((sl.SEND_SENT, sl.SEND_UNCONFIRMED, sl.SEND_FAILED), ("sent", "unconfirmed", "failed"))
        self.assertEqual(autoreply_stoplist.OUTCOMES, ("stopped", "clear", "unknown", "unconfigured"))
        self.assertEqual(sp.notice_key("ref1", "h" * 64, today=datetime.date(2026, 10, 12)),
                         "pending_notice:ref1:2026-10-12")
        self.assertEqual(sp.notice_key("", "h" * 64, today=datetime.date(2026, 10, 12)),
                         "pending_notice:" + "h" * 16 + ":2026-10-12")


if __name__ == "__main__":
    unittest.main()
