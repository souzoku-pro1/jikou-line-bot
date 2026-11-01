"""JIKOU-REPLY-Q1b: 大野の返信欄（PWA・明示送信・actor=human）・App 28 human 行・履歴の human→assistant・
手本の匿名化保存・許可表の human 列
fix1（Codex R-JIKOU-REPLY-Q1b-1 BR-01〜09・司令塔裁定: 人の明示送信は台帳必須＝fail-closed）

固定する仕様（QUEUE-2026-10-11-B Q-3 (a)〜(g) + fix1）:
- 返信画面 /app/reply/{App 29 record_id}（GET）と /app/reply/{record_id}/send（POST）は認証関所（_gate）
  必須。未認証は 303 → /app/login（PWA 共通の固定応答・本文なし・外部作用なし）
- 送信は send_ledger を actor=human・purpose=reply・操作 ID 固定で経由（記録 1 件・履歴 created/started/sent）。
  同じ操作 ID の再送信は副作用なし（二重送信・二重記録なし）。通信例外は unconfirmed＝/app/send_ops に
  「大野」として表示（自動再送なし）。非 2xx は failed（App 28 記録なし）
- BR-01: 台帳が使えない（DB 未設定・begin 失敗・重複照会の例外）なら送らない（fail-closed・固定文言）。
  bot 経路（push_text / reply_with_push_fallback）の fail-open は不変
- BR-02: finish が applied のときだけ成功処理。skipped は送信確認待ち、late は台帳の現在状態を照合して表示
- BR-03: GET の宛先（App 29 No.・line_user_id ハッシュ・revision）と操作 ID を署名付き hidden に埋め、POST で
  再検証。不一致は送らない。本文ハッシュは着手時に固定し finish で照合（不一致は late）
- BR-05: 送信着手と対応中（attending・human_version+1）は同一 tx。並行二重 POST は 1 送信
- BR-06: 確認文は固定（CONFIRM_LABEL）・チェック必須
- BR-07: human 行は assistant 変換 + 本文先頭に [事務所からの返信]（顧客対応・ヒアリングのメモリ経路とも）。
  送信前サニタイズでマーカーを除去、除去後に空なら降格。human 行が無ければ prompt は従来と完全一致
- BR-08: 送信成功後の App 28 記録失敗は記録未了（op_id 単位）に残し、回収ジョブ（SEND_LEDGER_RECOVER 相乗り）で
  冪等に再試行。同じ操作 ID の再 POST は「送信済み・記録のみ再試行」
- BR-04/BR-09: 匿名化は hub/pii_scan（tools/line_log_anonymize と共通）。氏名（登録氏名・姓のみ・呼びかけ）・
  住所（都道府県なしも）・電話・メール・郵便番号・口座・LINE userId・7 桁以上の数字列。残留があれば保存せず
  件数ログのみ。表は category_id・source_text_version・approved_by/at・active + 履歴表
- 承認画面は参照専用 pin（button 2・input 1・form なし・fetch は参照 API のみ）を維持しつつ返信リンクを持つ
- 新 module は sink AST policy 違反ゼロ・allowlist 追加ゼロ・main の結線は末尾追記のみ（pin 番地不変）
"""

import ast
import asyncio
import datetime
import json
import os
import re
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
from hub import pii_scan  # noqa: E402
from hub import reply_exemplar as rx  # noqa: E402
from hub import reply_followup as rf  # noqa: E402
from hub import reply_pending as rp  # noqa: E402  Q1c-fix2 BC-09: 返信欄は対応待ち表を読めないと送らない（fail-closed）ため表を用意する
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
USER2 = "U" + "b" * 32
OP = "f" * 32
OP2 = "e" * 32
OP3 = "c" * 32
REC_ID = "123"
PAGE = f"/app/reply/{REC_ID}"
SEND = f"{PAGE}/send"
TEXT = "お返事します。費用は44,000円です。"
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


def _form(op=OP, text=TEXT, confirm="1", exemplar="1", rev="0", user=USER, rec_id=REC_ID, ticket=None):
    data = {"op_id": op, "text": text,
            "ticket": (ticket if ticket is not None else rv.make_ticket(rec_id, user, rev, op))}
    if confirm:
        data["confirm"] = confirm
    if exemplar:
        data["exemplar"] = exemplar
    return data


class _DbMixin(unittest.TestCase):
    """sqlite DB（send_ledger・reply_exemplar・reply_followup）＋ LINE/kintone のフェイク。"""

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
                await c.run_sync(rf.metadata.create_all)
                await c.run_sync(rp.metadata.create_all)      # Q1c-fix2 BC-09（本番は migration a1c3／d4f6／b7d9 で存在する表）
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

    def _rows(self, table) -> list:
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(table))).fetchall()]
        return _run(_q())

    def ops(self) -> list:
        return self._rows(sl.send_operation)

    def exemplars(self) -> list:
        return self._rows(rx.reply_exemplar)

    def followups(self) -> list:
        return self._rows(rf.reply_chatlog_followup)

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
                r = _client.post(f"/app/reply/{bad}/send", headers=_auth(), data=_form(rec_id=REC_ID),
                                 follow_redirects=False)
                self.assertEqual((r.status_code, r.text), (400, ""))


# ── 2. 送信の往復（(a)(b)(d)(f) + fix1 BR-01〜03・05・06・08） ──────────────────────
class TestExplicitSend(_DbMixin):
    def test_page_renders_ticket_without_line_user_id_and_posts_with_it(self):
        self.get_record.return_value = _approval(**{"$revision": "7"})
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
        self.assertIn(rv.CONFIRM_LABEL, r.text)                              # BR-06
        self.assertIn('name="confirm" value="1" required', r.text)
        self.assertIn('name="exemplar" value="1" checked', r.text)
        self.assertEqual(r.headers["cache-control"], "no-store, private")
        op_ids = re.findall(r'name="op_id" value="([0-9a-f]{32})"', r.text)
        tickets = re.findall(r'name="ticket" value="([^"]+)"', r.text)
        self.assertEqual((len(op_ids), len(tickets)), (1, 1))
        rid, uhash, rev, oid, _sig = tickets[0].split(".")
        self.assertEqual((rid, rev, oid), (REC_ID, "7", op_ids[0]))
        self.assertEqual(uhash, sl.line_user_hash("jikou", USER)[:32])     # ハッシュのみ（userId は載せない）
        # 画面の hidden をそのまま POST → 送信
        r = self.post(_form(op=op_ids[0], ticket=tickets[0]))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=saved")
        self.assertEqual(len(_FakeClient.calls), 1)

    def test_send_records_human_operation_attending_chatlog_and_exemplar(self):
        r = self.post()
        self.assertEqual((r.status_code, r.headers["location"]), (303, f"{PAGE}?done=sent&log=written&ex=saved"))
        # LINE push 1 回（宛先はサーバ内で解決した line_user_id）
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(_FakeClient.calls[0][1]["to"], USER)
        # send_ledger: actor=human・purpose=reply・op_id 固定・sent・本文ハッシュ固定
        ops = self.ops()
        self.assertEqual(len(ops), 1)
        op = ops[0]
        self.assertEqual((op["op_id"], op["actor"], op["purpose"], op["state"], op["business"]),
                         (OP, "human", "reply", "sent", "jikou"))
        self.assertIsNone(op["inbound_event_id"])
        self.assertEqual(op["text_version"], sl.text_version(TEXT))
        self.assertFalse(op["completed_after_human"])          # 自分の対応中切替は「人の操作後に完了」ではない
        self.assertEqual([h["reason"] for h in _run(sl.operation_history(OP))], ["created", "started", "sent"])
        # BR-05: 送信着手と同じ tx で会話が対応中（受信が無くても会話を起こす・human_version=1 の新規）
        state = _run(sl.conversation_state("jikou", USER))
        self.assertTrue(state["attending"])
        self.assertEqual((state["version"], state["human_version"]), (1, 1))
        self.assertEqual(op["conversation_id"], state["conversation_id"])
        self.assertTrue(state["attending_since"])
        # App 28: role=human・category=human_reply・auto_sent=no
        self.form_fields.assert_awaited_once()
        self.create.assert_awaited_once()
        app, fields = self.create.await_args.args
        self.assertEqual(app.app_id_env, "APP_CHATLOG")
        # fix2 BR-11: 本文欄の末尾に照合用マーカー [op:{op_id}]（新欄なし・表示とモデル入力では除去）
        self.assertEqual(fields, {"line_user_id": USER, "role": "human", "message": rf.with_op_marker(TEXT, OP),
                                  "category": "human_reply", "auto_sent": "no"})
        # fix2 BR-12: 回収表には送信前に prepared が置かれ、記録後に done（本文は保持しない・本文ハッシュは残る）
        fu = self.followups()
        self.assertEqual(len(fu), 1)
        self.assertEqual((fu[0]["op_id"], fu[0]["state"], fu[0]["last_result"], fu[0]["text"],
                          fu[0]["text_version"], fu[0]["owner_token"], fu[0]["deadline_at"]),
                         (OP, "done", "written", None, sl.text_version(TEXT), None, None))
        # 手本: 匿名化済み・業者名保持・段階=受任後・op_id/本文ハッシュ紐付け・active・履歴 created
        ex = self.exemplars()
        self.assertEqual(len(ex), 1)
        self.assertEqual((ex[0]["business"], ex[0]["stage"], ex[0]["creditor_name"], ex[0]["source_op_id"]),
                         ("jikou", "受任後", "アコム", OP))
        self.assertEqual(ex[0]["source_text_version"], sl.text_version(TEXT))
        # fix2 BR-10: 保存直後は pending_review（approved_by／approved_at は NULL・自動マスクだけで承認済みにしない）
        self.assertEqual((ex[0]["approved_by"], bool(ex[0]["active"]), ex[0]["category_id"], ex[0]["status"]),
                         (None, True, None, "pending_review"))
        self.assertIsNone(ex[0]["approved_at"])
        self.assertEqual(_run(rx.list_exemplars("jikou")), [])        # 承認まで下書き参照に出ない
        self.assertIn("44,000円", ex[0]["human_reply"])
        self.assertIn("アコム", ex[0]["inbound_text"])
        self.assertNotIn("090-1234-5678", ex[0]["inbound_text"])
        self.assertIn(rx.MASK_PHONE, ex[0]["inbound_text"])
        self.assertEqual(ex[0]["conversation_ref"], state["ref"])
        hist = _run(rx.exemplar_history(ex[0]["id"]))
        self.assertEqual([(h["action"], h["actor"]) for h in hist], [("created", "human")])

    def test_second_send_bumps_versions_and_keeps_attending(self):
        self.post()
        self.post(_form(op=OP2))
        state = _run(sl.conversation_state("jikou", USER))
        self.assertEqual((state["version"], state["human_version"], state["attending"]), (2, 2, True))
        self.assertEqual(len(self.ops()), 2)

    def test_same_operation_id_is_rejected_without_side_effects(self):
        self.post()
        r = self.post()
        # fix2 BR-12: 回収行は記録済み（done）で必ず存在するため「記録は既に完了」の通知が付く（fix1 は行なし＝通知なし）
        self.assertEqual(r.headers["location"], f"{PAGE}?done=duplicate_sent&log=not_pending")
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(len(self.ops()), 1)
        self.create.assert_awaited_once()
        self.assertEqual(len(self.exemplars()), 1)
        # 別の操作 ID なら新しい送信（画面を開き直した場合）
        r = self.post(_form(op=OP2))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=saved")
        self.assertEqual(len(self.ops()), 2)

    def test_concurrent_double_post_sends_once(self):
        """BR-05: 同じ操作 ID の並行二重 POST は 1 送信（op_id 一意・排他）。"""
        async def body():
            return await asyncio.gather(
                line_channel.push_text_explicit(line_channel.JIKOU_CHANNEL, OP, USER, TEXT),
                line_channel.push_text_explicit(line_channel.JIKOU_CHANNEL, OP, USER, TEXT))
        results = _run(body())
        outcomes = sorted(r.outcome for r in results)
        self.assertEqual(outcomes[1], "sent")
        self.assertIn(outcomes[0], ("duplicate_sent", "duplicate_unconfirmed"))
        self.assertEqual(len(_FakeClient.calls), 1)
        self.assertEqual(len(self.ops()), 1)
        state = _run(sl.conversation_state("jikou", USER))
        self.assertEqual((state["version"], state["human_version"]), (1, 1))

    def test_missing_confirm_or_empty_text_does_not_send(self):
        r = self.post(_form(confirm=""))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=confirm_required")
        r = self.post(_form(text="   "))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=bad_input")
        r = self.post(_form(text="あ" * (rv.MAX_TEXT_CHARS + 1)))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=bad_input")
        r = _client.post(SEND, headers=_auth(), data=_form(op="not-hex", ticket="x"), follow_redirects=False)
        self.assertEqual((r.status_code, r.text), (400, ""))
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])
        self.create.assert_not_awaited()

    def test_ticket_is_required_signed_and_bound(self):
        """BR-03: ticket なし・署名不正・他レコード/他操作 ID への束縛は固定 400（送らない）。"""
        data = _form()
        data.pop("ticket")
        r = _client.post(SEND, headers=_auth(), data=data, follow_redirects=False)
        self.assertEqual((r.status_code, r.text), (400, ""))
        good = rv.make_ticket(REC_ID, USER, "0", OP)
        forged = good[:-1] + ("0" if good[-1] != "0" else "1")
        r = self.post(_form(ticket=forged))
        self.assertEqual((r.status_code, r.text), (400, ""))
        r = self.post(_form(ticket=rv.make_ticket("124", USER, "0", OP)))       # 他レコード
        self.assertEqual(r.status_code, 400)
        r = self.post(_form(ticket=rv.make_ticket(REC_ID, USER, "0", OP2)))     # 他操作 ID
        self.assertEqual(r.status_code, 400)
        with patch.dict(os.environ, {"WEBAPP_SESSION_SECRET": "t" * 32}):     # 鍵が違えば検証不可
            r = self.post(_form(ticket=good))
        self.assertEqual(r.status_code, 400)
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])

    def test_destination_change_since_render_is_not_sent(self):
        """BR-03: GET 時の宛先（ハッシュ・revision）と POST 時の App 29 が食い違えば送らない。"""
        self.get_record.return_value = _approval(**{"$revision": "4"})
        r = self.post(_form(rev="3"))                                           # revision が進んだ
        self.assertEqual(r.headers["location"], f"{PAGE}?done=stale")
        self.get_record.return_value = _approval(line_user_id=USER2, **{"$revision": "3"})
        r = self.post(_form(rev="3"))                                           # 宛先がすり替わった
        self.assertEqual(r.headers["location"], f"{PAGE}?done=stale")
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])
        self.get_record.return_value = _approval(**{"$revision": "3"})
        r = self.post(_form(rev="3"))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=saved")

    def test_text_hash_is_checked_at_finish(self):
        """BR-03: 着手時に固定した本文ハッシュと finish の照合が合わなければ late（状態は変えない）。"""
        async def body():
            async with sl.human_reply(OP):
                op = await sl.begin("jikou", "jikou", USER, TEXT)
            late = await sl.finish(op, sl.STATE_SENT, text_version=sl.text_version("別の本文"))
            applied = await sl.finish(op, sl.STATE_SENT, text_version=sl.text_version(TEXT))
            return late, applied
        self.assertEqual(_run(body()), ("late", "applied"))
        reasons = [h["reason"] for h in _run(sl.operation_history(OP))]
        self.assertEqual(reasons, ["created", "started", "late_result:sent", "sent"])

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

    # ── BR-01: 台帳必須（fail-closed） ─────────────────────────────────────────
    def test_without_database_does_not_send_ledger_unavailable(self):
        """BR-01（fix1 で fail-closed に書き換え: 旧テストは DB なしでも送信していた）: 台帳が無ければ送らない。"""
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            os.environ.pop("DATABASE_URL", None)
            db.reset_for_tests()
            # fix2 BR-12: 送信前に回収表へ prepared を置くため、DB なしの遮断は台帳より先に回収表の段で起きる（同じ fail-closed）
            with self.assertLogs("hub.webapp_reply_view", level="WARNING") as cm:
                r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=ledger_unavailable")
        self.assertEqual(_FakeClient.calls, [])
        self.create.assert_not_awaited()
        self.assertTrue(any("fail-closed" in m for m in cm.output))
        self.assertFalse(any(USER in m or TEXT in m for m in cm.output))
        page = _client.get(f"{PAGE}?done=ledger_unavailable", headers=_auth())
        self.assertIn("台帳が使えないため送信できません。時間をおいて再試行してください", page.text)

    def test_begin_failure_or_duplicate_check_failure_does_not_send(self):
        with patch.object(sl, "begin", AsyncMock(return_value=None)):
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=ledger_unavailable")
        with patch.object(sl, "operation_state", AsyncMock(side_effect=RuntimeError("db down"))):
            r = self.post(_form(op=OP2))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=ledger_unavailable")
        self.assertEqual(_FakeClient.calls, [])
        self.assertEqual(self.ops(), [])

    def test_bot_paths_keep_fail_open_without_database(self):
        """司令塔裁定: Q1a の既存 bot 経路の fail-open は不変。"""
        with patch.dict(os.environ, {"DATABASE_URL": ""}):
            os.environ.pop("DATABASE_URL", None)
            db.reset_for_tests()
            self.assertIs(_run(line_channel.push_text(line_channel.JIKOU_CHANNEL, USER, "x")), True)
        self.assertEqual(len(_FakeClient.calls), 1)

    # ── BR-02: finish の戻り値で分岐 ─────────────────────────────────────────────
    def test_finish_skipped_is_shown_as_unconfirmed_without_success_processing(self):
        real_finish = sl.finish

        async def _skipped(op, outcome, **kw):
            return "skipped"
        with patch.object(sl, "finish", _skipped):
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=record_unconfirmed")
        self.assertEqual(len(_FakeClient.calls), 1)
        self.create.assert_not_awaited()
        self.assertEqual(self.exemplars(), [])
        self.assertEqual(self.ops()[0]["state"], "started")     # 台帳は確定できていない＝回収ジョブ/人の確定へ
        self.assertIs(sl.finish, real_finish)

    def test_finish_late_shows_current_ledger_state_without_success_processing(self):
        real_finish = sl.finish

        async def _late(op, outcome, **kw):
            async with db.session_scope() as s:                # 別の処理が所有者を奪った（回収→新試行）体
                await s.execute(sa.update(sl.send_operation).where(
                    sl.send_operation.c.op_id == op.op_id).values(owner_token="other"))
            return await real_finish(op, outcome, **kw)
        with patch.object(sl, "finish", _late):
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=late&state=started")
        self.create.assert_not_awaited()
        self.assertEqual(self.exemplars(), [])
        self.assertIn("late_result:sent", [h["reason"] for h in _run(sl.operation_history(OP))])
        page = _client.get(f"{PAGE}?done=late&state=started", headers=_auth())
        self.assertIn("遅着と判定", page.text)
        self.assertIn("着手済（確認中）", page.text)

    # ── BR-08: App 28 記録の回収 ───────────────────────────────────────────────
    def test_chatlog_skipped_is_pending_and_recovered_after_option_added(self):
        self.form_fields.return_value = _FIELDS_WITHOUT_HUMAN
        with self.assertLogs("hub.webapp_reply_view", level="INFO") as cm:
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=pending_retry&ex=saved")
        self.create.assert_not_awaited()
        self.assertEqual(rv._skip_count["role_option_missing"], 1)
        self.assertTrue(any("role option missing" in m for m in cm.output))
        self.assertFalse(any(USER in m or "お返事" in m for m in cm.output))
        fu = self.followups()
        self.assertEqual(len(fu), 1)
        self.assertEqual((fu[0]["op_id"], fu[0]["state"], fu[0]["last_result"], fu[0]["attempts"],
                          fu[0]["approval_record_id"], fu[0]["text"]),
                         (OP, "pending", "skipped", 0, REC_ID, TEXT))
        self.assertEqual(fu[0]["line_user_hash"], sl.line_user_hash("jikou", USER))
        # 回収ジョブ（選択肢がまだ無い）: 試行回数に数えず pending のまま
        _run(sl.run_recover_job())
        self.assertEqual((self.followups()[0]["state"], self.followups()[0]["attempts"]), ("pending", 0))
        self.create.assert_not_awaited()
        # 選択肢が追加されれば（再配備なしで）回収ジョブが記録し done・本文は保持しない
        self.form_fields.return_value = _FIELDS_WITH_HUMAN
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()
        fu = self.followups()[0]
        self.assertEqual((fu["state"], fu["last_result"], fu["text"]), ("done", "written", None))
        self.assertIsNotNone(fu["done_at"])
        _run(sl.run_recover_job())                             # 冪等: 2 回目は何もしない
        self.create.assert_awaited_once()

    def test_chatlog_failure_is_pending_and_same_op_repost_retries_record_only(self):
        self.form_fields.side_effect = RuntimeError("kintone down")
        with self.assertLogs("hub.webapp_reply_view", level="WARNING") as cm:
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=pending_retry&ex=saved")
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.assertTrue(any("write failed (fixed reason)" in m for m in cm.output))
        self.assertFalse(any(USER in m for m in cm.output))
        self.assertEqual(self.followups()[0]["last_result"], "failed")
        # 同じ操作 ID の再 POST → 送信済み・記録のみ再試行（LINE には触れない）
        self.form_fields.side_effect = None
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=duplicate_sent&log=written")
        self.assertEqual(len(_FakeClient.calls), 1)
        self.create.assert_awaited_once()
        self.assertEqual(self.followups()[0]["state"], "done")
        r = self.post()                                        # 記録済みなら再試行しない
        self.assertEqual(r.headers["location"], f"{PAGE}?done=duplicate_sent&log=not_pending")
        self.create.assert_awaited_once()

    def test_followup_counts_failures_and_exhausts_without_resending(self):
        self.form_fields.side_effect = RuntimeError("kintone down")
        self.post()
        async def _bump():
            async with db.session_scope() as s:
                await s.execute(sa.update(rf.reply_chatlog_followup).values(attempts=rf.MAX_ATTEMPTS - 2))
        _run(_bump())
        _run(sl.run_recover_job())
        self.assertEqual((self.followups()[0]["state"], self.followups()[0]["attempts"]),
                         ("pending", rf.MAX_ATTEMPTS - 1))
        with self.assertLogs("hub.reply_followup", level="WARNING") as cm:
            _run(sl.run_recover_job())
        self.assertEqual(self.followups()[0]["state"], "exhausted")
        self.assertTrue(any("exhausted" in m for m in cm.output))
        _run(sl.run_recover_job())                             # exhausted は触らない
        self.assertEqual(self.followups()[0]["attempts"], rf.MAX_ATTEMPTS)
        self.assertEqual(len(_FakeClient.calls), 1)            # LINE 再送なし

    def test_followup_refuses_changed_destination(self):
        self.form_fields.side_effect = RuntimeError("kintone down")
        self.post()
        self.form_fields.side_effect = None
        self.get_record.return_value = _approval(line_user_id=USER2)   # App 29 の宛先がすり替わった
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()
        self.assertEqual((self.followups()[0]["state"], self.followups()[0]["last_result"]),
                         ("pending", "destination_mismatch"))

    def test_recover_hook_is_registered_once_and_tolerates_failure(self):
        self.assertEqual(sl._recover_hooks.count(rv.run_chatlog_followup), 1)
        sl.register_recover_hook(rv.run_chatlog_followup)
        self.assertEqual(sl._recover_hooks.count(rv.run_chatlog_followup), 1)
        with patch.object(rf, "pending_ops", AsyncMock(side_effect=RuntimeError("x"))), \
                self.assertLogs("hub.reply_followup", level="WARNING"):
            _run(sl.run_recover_job())                         # 例外は固定語彙で握る（ジョブは落ちない）

    # ── 手本・メモリ履歴 ─────────────────────────────────────────────────────────
    def test_exemplar_opt_out_and_rejection_do_not_block_send(self):
        r = self.post(_form(exemplar=""))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=off")
        self.assertEqual(self.exemplars(), [])
        with patch.object(pii_scan, "RESIDUE_RES", pii_scan.RESIDUE_RES + (("test", re.compile("お返事")),)), \
                self.assertLogs("hub.reply_exemplar", level="INFO") as cm:
            r = self.post(_form(op=OP2))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=rejected")
        self.assertEqual(self.exemplars(), [])
        self.assertTrue(any("rejected by residue check" in m and "total=" in m for m in cm.output))
        self.assertFalse(any("お返事" in m for m in cm.output))   # 本文を出さない

    def test_memory_history_reflected_as_marked_assistant_only_when_present(self):
        self.post()
        self.assertNotIn(USER, main.conversation_histories)       # 履歴が無ければ作らない
        main.conversation_histories[USER] = [{"role": "user", "content": "質問"}]
        self.post(_form(op=OP2))
        marked = f"{cr.HUMAN_MARKER} {TEXT}"
        self.assertEqual(main.conversation_histories[USER],
                         [{"role": "user", "content": "質問"}, {"role": "assistant", "content": marked}])
        self.post(_form(op=OP3))                                   # 連続は結合（契約: 交互）
        self.assertEqual(len(main.conversation_histories[USER]), 2)
        self.assertEqual(main.conversation_histories[USER][-1]["content"].count(marked), 2)

    def test_stage_consult_when_pre_engagement(self):
        self.app21.return_value = _rec(status="決済完了", 問い合わせ業者名="プロミス")
        self.post()
        self.assertEqual((self.exemplars()[0]["stage"], self.exemplars()[0]["creditor_name"]),
                         ("相談", "プロミス"))

    def test_exemplar_lifecycle_used_and_deactivated_with_history(self):
        self.post()
        ex_id = self.exemplars()[0]["id"]
        # fix2 BR-10: pending_review のままでは使えない（人の明示操作 approve_exemplar で approved にしてから）
        self.assertFalse(_run(rx.mark_used(ex_id)))
        self.assertTrue(_run(rx.approve_exemplar(ex_id, actor="human")))
        self.assertFalse(_run(rx.approve_exemplar(ex_id, actor="human")))     # 二重承認は何もしない
        self.assertTrue(_run(rx.mark_used(ex_id)))
        self.assertEqual(len(_run(rx.list_exemplars("jikou", stage="受任後"))), 1)
        self.assertTrue(_run(rx.deactivate_exemplar(ex_id)))
        self.assertFalse(_run(rx.deactivate_exemplar(ex_id)))
        self.assertEqual(_run(rx.list_exemplars("jikou")), [])           # 無効は読まない・行は残る
        row = self.exemplars()[0]
        self.assertEqual((row["used_count"], bool(row["active"]), row["status"], row["approved_by"]),
                         (1, False, "retired", "human"))
        self.assertIsNotNone(row["approved_at"])
        self.assertEqual([h["action"] for h in _run(rx.exemplar_history(ex_id))],
                         ["created", "approved", "used", "deactivated"])


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


# ── 4. 履歴組立（(c) + BR-07 マーカー） ───────────────────────────────────────────
class TestHistoryConversion(unittest.TestCase):
    def test_model_history_converts_human_with_marker_and_drops_unknown(self):
        h = cr.model_history([{"role": "user", "content": "a"}, {"role": "human", "content": "b"},
                              {"role": "assistant", "content": "c"}, {"role": "system", "content": "x"}])
        self.assertEqual(list(h), [{"role": "user", "content": "a"},
                                   {"role": "assistant", "content": "[事務所からの返信] b"},
                                   {"role": "assistant", "content": "c"}])
        self.assertEqual(h.human_rows, 1)
        self.assertIsInstance(h, list)
        self.assertEqual(cr.human_history_note(h), cr.HUMAN_HISTORY_NOTE)
        self.assertEqual(cr.human_history_note([]), "")
        self.assertEqual(cr.human_history_note(cr.model_history([{"role": "user", "content": "a"}])), "")
        self.assertEqual((cr.model_role("human"), cr.model_role("user"), cr.model_role(None)),
                         ("assistant", "user", None))
        self.assertEqual((cr.model_content("human", "b"), cr.model_content("assistant", "b"),
                          cr.model_content("human", "")), ("[事務所からの返信] b", "b", ""))
        self.assertEqual(cr.HUMAN_MARKER, "[事務所からの返信]")
        self.assertIn("このマーカーを出力に含めない", cr.HUMAN_HISTORY_NOTE)
        self.assertEqual(cr.HUMAN_HISTORY_NOTE.count("\n"), 2)   # 空行 + 注記 1 行

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
                                         {"role": "assistant", "content": "[事務所からの返信] 事務所の返信"}])
        self.assertEqual(history.human_rows, 1)

    def test_normalize_history_keeps_human_as_marked_assistant(self):
        out = main._normalize_history([{"role": "user", "content": "q"}, {"role": "human", "content": "h"},
                                       {"role": "assistant", "content": "a"}, {"role": "other", "content": "x"}])
        self.assertEqual(out, [{"role": "user", "content": "q"},
                               {"role": "assistant", "content": "[事務所からの返信] h\na"}])

    def test_sanitize_strips_marker_and_demotes_when_empty(self):
        self.assertEqual(cr.strip_human_marker("[事務所からの返信] 了解です。"), "了解です。")
        self.assertEqual(cr.strip_human_marker("了解です。"), "了解です。")
        out, issues, fatal = cr.sanitize_reply_without_human_marker("[事務所からの返信] 了解です。")
        self.assertEqual((out, fatal), ("了解です。", False))
        self.assertIn("human_marker_removed", issues)
        out, issues, fatal = cr.sanitize_reply_without_human_marker("[事務所からの返信]\n ")
        self.assertEqual((out, fatal, issues), ("", True, [cr.HUMAN_MARKER_ONLY_ISSUE]))
        # マーカーが無ければ従来の sanitize_reply と同じ
        from hub import reply_sanitizer
        self.assertEqual(cr.sanitize_reply_without_human_marker("了解です😊"),
                         reply_sanitizer.sanitize_reply("了解です😊"))

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
        self.assertNotIn(cr.HUMAN_MARKER, plain)
        with_h = cr.model_history([{"role": "human", "content": "事務所の返信"}])
        noted, msgs2 = _system_prompt_for(with_h)
        self.assertTrue(noted.endswith(cr.HUMAN_HISTORY_NOTE))
        self.assertEqual(noted[:-len(cr.HUMAN_HISTORY_NOTE)], plain)       # human 行が無ければ完全一致
        self.assertEqual([m["role"] for m in msgs2], ["assistant", "user"])
        self.assertTrue(msgs2[0]["content"].startswith(cr.HUMAN_MARKER))
        self.assertEqual(cr.build_system_prompt(status="決済完了"),
                         cr.build_system_prompt(status="決済完了", human_note=""))

    def test_customer_reply_with_marker_only_is_demoted(self):
        """BR-07: モデル出力がマーカーだけなら降格（承認キュー）・マーカー付きなら除去して送る。"""
        sent = AsyncMock()
        queue = AsyncMock(return_value="q")
        result = {"reply": "[事務所からの返信]", "category": "挨拶・雑談", "auto_send": True, "reason": "r"}
        kwargs = dict(user_id="Umk", user_message="こんにちは", reply_token="tok",
                      app21_record={"status": {"value": "決済完了"}}, reply_func=sent)
        with patch.object(cr, "_call_compose_reply", AsyncMock(return_value=dict(result))), \
                patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=[])), \
                patch.object(cr, "save_to_chatlog", AsyncMock()), \
                patch.object(cr, "save_to_approval_queue", queue), \
                patch.object(cr, "_notify_attorney", AsyncMock()):
            asyncio.run(cr.handle_customer_message(**kwargs))
            queue.assert_awaited_once()
            self.assertIn("プレースホルダ/内部マーカー残存", queue.await_args.kwargs["reason"])
            sent.assert_awaited_once()
            self.assertNotIn(cr.HUMAN_MARKER, sent.await_args.args[1])      # 顧客にはマーカーだけの文を送らない
            sent.reset_mock()
            queue.reset_mock()
            with patch.object(cr, "_call_compose_reply",
                              AsyncMock(return_value=dict(result, reply="[事務所からの返信] こんにちは。"))):
                asyncio.run(cr.handle_customer_message(**kwargs))
            queue.assert_not_awaited()
            self.assertEqual(sent.await_args.args[1], "こんにちは。")


# ── 5. 手本の匿名化（(d)(g) + BR-04 共通検査器） ────────────────────────────────────
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

    def test_codex_examples_line_user_id_no_prefecture_address_third_party_and_single_char_name(self):
        # (1) U + 英字主体 32 桁
        out, ok = rx.anonymize("ID は Uabcdefghijklmnopqrstuvwxyzabcdef と " + USER + " です")
        self.assertEqual(out, f"ID は {rx.MASK_LINE_USER_ID} と {rx.MASK_LINE_USER_ID} です")
        self.assertTrue(ok)
        # (2) 都道府県なしの住所
        out, _ = rx.anonymize("横浜市中区山下町1-2-3 に住んでいます。新宿区西新宿２丁目８番１号も。")
        self.assertNotIn("山下町", out)
        self.assertNotIn("西新宿", out)
        self.assertEqual(out.count(rx.MASK_ADDRESS), 2)
        # (3) 第三者の氏名らしき固有名詞（登録氏名ではない・様/さん/氏の直前）
        out, _ = rx.anonymize("田中氏と鈴木様と佐藤さんが同席。お客様とご担当者様、お母さんも。氏名は不要。",
                              names=("試験 太郎",))
        for gone in ("田中", "鈴木", "佐藤"):
            self.assertNotIn(gone, out)
        for keep in ("お客様", "ご担当者様", "お母さん", "氏名は不要"):
            self.assertIn(keep, out)
        # (4) 一文字姓の呼びかけ（登録氏名「林 一」の 1 文字部分は単独では伏せないが、呼びかけは伏せる）
        out, _ = rx.anonymize("太郎さんへ。林さんは別人です。林業の話。", names=("試験 太郎", "林 一"))
        self.assertNotIn("太郎", out)
        self.assertNotIn("林さん", out)
        self.assertIn("林業", out)
        self.assertEqual(out.count(rx.MASK_NAME + "さん"), 2)

    def test_name_variants_and_placeholders(self):
        out, _ = rx.anonymize("（未登録）の方へ", names=("（未登録）",))
        self.assertEqual(out, "（未登録）の方へ")
        self.assertEqual(set(pii_scan.name_variants("試験 太郎")), {"試験 太郎", "試験太郎", "試験", "太郎"})
        self.assertEqual(pii_scan.name_variants("試験 太郎")[0], "試験 太郎")          # 長い順
        self.assertEqual(set(pii_scan.name_variants("林 一")), {"林 一", "林一"})         # 1 文字部分は含めない

    def test_creditor_with_prefecture_name_is_not_treated_as_address(self):
        out, ok = rx.anonymize("千葉県信用金庫からの督促です。三菱UFJ銀行の件、44,000円です。新宿区役所で住民票。")
        self.assertEqual(out, "千葉県信用金庫からの督促です。三菱UFJ銀行の件、44,000円です。新宿区役所で住民票。")
        self.assertTrue(ok)

    def test_residue_check_runs_before_amount_restore(self):
        out, ok = rx.anonymize("費用は1000000円です")
        self.assertEqual((out, ok), ("費用は1000000円です", True))
        self.assertEqual(pii_scan.residue_hits("連絡は 090-1234-5678 へ"), ["phone"])
        self.assertEqual(pii_scan.residue_hits("ID " + USER), ["line_user_id"])
        self.assertEqual(pii_scan.residue_hits("〔電話〕〔メール〕"), [])

    def test_tool_shares_the_detector(self):
        from tools import line_log_anonymize as tool
        self.assertTrue(all(rx_ in {r for _n, r in pii_scan.RESIDUE_RES} or rx_.pattern in
                            (r"\d{3}-\d{4}", r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+",
                             r"(?i)(token|secret|key)\s*[:=]\s*\S{8,}") for rx_ in tool._RESIDUE_RES))
        self.assertIn((pii_scan.PHONE_RE, "<電話番号>"), tool._REDACTIONS)
        self.assertIn((pii_scan.LINE_USER_ID_RE, "<LINE ID>"), tool._REDACTIONS)
        red = tool.redact_text("連絡先は 090-1234-5678、〒123-4567、a@b.co.jp、" + USER)
        self.assertEqual(tool.residue_hits(red), [])
        self.assertIn("<LINE ID>", red)

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
        self.assertEqual(rx.TABLE_NAMES, ("reply_exemplar", "reply_exemplar_history"))
        self.assertEqual({c.name for c in rx.reply_exemplar.columns},
                         {"id", "business", "conversation_ref", "inbound_text", "human_reply",
                          "creditor_name", "stage", "created_at", "used_count", "source_op_id",
                          "category_id", "source_text_version", "approved_by", "approved_at", "active",
                          "status"})                                            # fix2 BR-10: status
        self.assertEqual({c.name for c in rx.reply_exemplar_history.columns},
                         {"history_id", "exemplar_id", "action", "actor", "at", "note"})
        self.assertEqual(rx.ACTIONS, ("created", "used", "deactivated", "approved"))   # fix2 BR-10
        self.assertEqual(rx.STATUSES, ("pending_review", "approved", "retired"))
        # fix2 BR-11／BR-12: 本文ハッシュ・所有権・照合したレコード No.・状態 7 値
        self.assertEqual({c.name for c in rf.reply_chatlog_followup.columns},
                         {"op_id", "business", "kind", "approval_record_id", "line_user_hash", "text",
                          "text_version", "state", "attempts", "last_result", "owner_token", "deadline_at",
                          "chatlog_record_id", "created_at", "updated_at", "done_at",
                          "create_state",                                           # fix5 BR-17: 外部作成の確定状態（a7c9e1f3b5d7）
                          "row_version"})                                           # fix6 BR-18: 世代番号（c8e0a2b4d6f8）
        self.assertEqual(rf.STATES, ("prepared", "pending", "unconfigured", "in_progress", "done", "closed",
                                     "exhausted"))
        self.assertEqual(rf.OPEN_STATES, ("prepared", "pending", "unconfigured", "in_progress"))
        self.assertEqual(line_channel.PUSH_OUTCOMES, ("sent", "failed", "unconfirmed", "duplicate_sent",
                                                      "duplicate_unconfirmed", "ledger_unavailable"))
        self.assertEqual(pii_scan.MASKS, ("〔氏名〕", "〔住所〕", "〔電話〕", "〔メール〕", "〔口座〕",
                                          "〔郵便番号〕", "〔数字列〕", "〔LINE ID〕"))


# ── 6. pin（承認画面・sink policy・allowlist・main 結線・migration） ─────────────────
class TestPins(unittest.TestCase):
    NEW_FILES = ("hub/reply_exemplar.py", "hub/webapp_reply_view.py", "hub/pii_scan.py", "hub/reply_followup.py")

    def test_approvals_page_links_to_reply_and_keeps_reference_only_pins(self):
        src = (REPO / "webapp" / "approvals.html").read_text(encoding="utf-8")
        self.assertIn('"/app/reply/"', src)
        self.assertIn("この相談者に返信する", src)
        self.assertEqual(src.count("<button"), 2)
        self.assertEqual(len(re.findall(r"<input\b[^>]*>", src)), 1)
        self.assertNotIn("<form", src.lower())
        self.assertNotIn("line_user_id", src)
        self.assertNotIn("reply", (REPO / "hub" / "webapp_approval_view.py").read_text(encoding="utf-8").lower()
                         .replace("承認", ""))

    def test_new_modules_have_zero_sink_violations_and_no_allowlist_entries(self):
        for name in self.NEW_FILES + ("hub/send_ledger.py", "hub/line_channel.py"):
            src = (REPO / name).read_text(encoding="utf-8")
            self.assertEqual(scan_source(src, name), [], name)
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for key in ("reply_exemplar", "webapp_reply_view", "pii_scan", "reply_followup"):
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
        # pii_scan は純関数（env・DB・HTTP・ログなし）
        psrc = (REPO / "hub" / "pii_scan.py").read_text(encoding="utf-8")
        pimports = set()
        for n in ast.walk(ast.parse(psrc)):
            if isinstance(n, ast.Import):
                pimports.update(a.name.split(".")[0] for a in n.names)
            elif isinstance(n, ast.ImportFrom):
                pimports.add((n.module or "").split(".")[0])
        self.assertEqual(pimports, {"re"})

    def test_main_wiring_is_tail_only_and_before_catch_all(self):
        src = (REPO / "main.py").read_text(encoding="utf-8").split("\n")
        tagged = [i for i, l in enumerate(src) if "JIKOU-REPLY-Q1b" in l]
        self.assertGreaterEqual(len(tagged), 5)
        # Q1c-fix2 BC-12: 「末尾 N 行」の文字列探索はやめ、結線が 1 箇所であることと **実ルート表での一致順** で検証する（下）
        self.assertEqual(sum("include_before_catch_all(app, reply_view_router)" in l for l in src), 1)
        # fix1 BR-07: ヒアリング経路の送信前サニタイズと App 28 復元は 1 行置換（行数不変＝allowlist 番地不変）
        self.assertEqual(sum("sanitize_reply_without_human_marker(" in l for l in src), 1)
        self.assertEqual(sum("model_content(m.get(\"role\")" in l for l in src), 1)
        self.assertNotIn("reply_sanitizer.sanitize_reply(", "\n".join(src))
        from hub.webapp_brain_view import _paths_of
        paths = [p for entry in main.app.router.routes for p in _paths_of(entry)
                 if p in ("/app/reply/{record_id}", "/app/{_rest:path}")]
        self.assertEqual(paths, ["/app/reply/{record_id}", "/app/{_rest:path}"])
        # Q1c-fix2 BC-12: 実ルート表で GET /app/reply/123・POST /app/reply/123/send が catch-all（/app/{_rest:path}）より先に
        # **一致** すること（Starlette の matches で先頭から走査し、最初に FULL 一致した経路が返信欄であること）
        from starlette.routing import Match
        for method, path in (("GET", f"/app/reply/{REC_ID}"), ("POST", f"/app/reply/{REC_ID}/send")):
            scope = {"type": "http", "method": method, "path": path, "root_path": "", "headers": [], "query_string": b""}
            first = None
            for entry in main.app.router.routes:
                try:
                    hit = entry.matches(scope)[0] == Match.FULL
                except Exception:
                    hit = False
                if hit:
                    first = entry
                    break
            self.assertIsNotNone(first, path)
            first_paths = _paths_of(first)                       # include_router の束（_IncludedRouter）なら中の path 一覧
            self.assertTrue({"/app/reply/{record_id}", "/app/reply/{record_id}/send"} & set(first_paths), (path, first_paths))
            self.assertNotIn("/app/{_rest:path}", first_paths, path)

    def test_migration_revision_chains_after_send_ledger_and_refuses_downgrade_with_data(self):
        p = REPO / "alembic" / "versions" / "20261011_f7b2c9d4e1a6_reply_exemplar.py"
        src = p.read_text(encoding="utf-8")
        self.assertIn("revision: str = 'f7b2c9d4e1a6'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'e5f8a1b4c7d9'", src)
        self.assertIn("reply_exemplar_downgrade_refused_data_present", src)
        self.assertIn("データがあれば downgrade 禁止", src)
        env = (REPO / "alembic" / "env.py").read_text(encoding="utf-8")
        self.assertIn("reply_exemplar_metadata", env)
        self.assertIn("reply_followup_metadata", env)


if __name__ == "__main__":
    unittest.main()
