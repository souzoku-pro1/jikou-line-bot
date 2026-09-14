"""SHINDAN-LINE-LINK-1: 友だち追加時の本人専用診断フォームリンクと、フォーム回答の
LINE ユーザーへの直接紐付けの固定。

裁定（逐語）:
 A. 送信契機は時効 /webhook の follow のみ。冪等キーは webhookEventId（durable lane）。
 B. token は userId・氏名を含まない不透明値（secrets.token_urlsafe(32)）・DB 保存・30 日。
 C. URL は /shindan?k=token。
 D. GET k 有効=通常のフォーム（HTML 不変）／無効・期限切れ・使用済み・k なし=k なしと同じ。
 E. POST k 有効: 判定不変・既存レコードは空欄のみ CAS（409 再取得 1 回）・無ければ作成+
    LINEユーザーID（受付番号なし）・成功後 used_at・失敗は used_at 未打刻+判定結果のみ+ERROR 1 行。
 F. k なしの POST は現状維持。 G. 管理者通知なし・固定語彙ログ。 H. 新規 env なし。
"""

import asyncio
import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token",
    "DOCUMENT_WEBHOOK_SECRET": "doc-secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402
import sqlalchemy as sa  # noqa: E402

import main  # noqa: E402
import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import shindan_link as sl  # noqa: E402
from hub.inbound_event import Base as InboundBase  # noqa: E402

REPO = Path(__file__).parent
USER = "Ushindanlink000000000000000000001"
NAME = "山田太郎"
LINE_URL = "https://line.me/R/ti/p/@dummy"
PUBLIC_DOMAIN = "example-app.up.railway.app"
PIN_GREETING = "63e6deb3e0d3f99d8f132b4ad3cace218e18f09fd7157fd816bdbbfe1c49f0e4"
PIN_DONE = "03c87574eda68cf341a35d855acf8ff14ac13c0be2f729ab6392ec5dc9974463"
VALID = {"creditor": "テスト債権者株式会社", "borrow": "5年以上前",
         "last_pay": "5年以上前", "court_doc": "何も届いていない", "website": ""}


def _run(coro):
    return asyncio.run(coro)


def _sign(body: bytes) -> str:
    return base64.b64encode(hmac.new(b"dummy_secret", body, hashlib.sha256).digest()).decode()


def _event_body(event_type="follow", event_id="01FOLLOW0001", user=USER):
    body = json.dumps({"events": [{"type": event_type, "webhookEventId": event_id,
                                   "replyToken": "rt-" + event_id,
                                   "source": {"userId": user}}]}).encode()
    return body, _sign(body)


class _FakeApp21:
    """App 21 の fake: LINEユーザーID 検索（limit 2）・GET・$revision CAS・create。"""
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self._id = 100
        self.create_calls: list[dict] = []
        self.update_calls: list[tuple] = []
        self.get_calls: list[str] = []
        self.conflict_next = 0
        self.create_fail_next = 0

    def add(self, rid, user_id=USER, revision="3", **filled):
        rec = {"$id": {"value": rid}, "$revision": {"value": revision},
               "LINEユーザーID": {"value": user_id}, "status": {"value": "問い合わせ"}}
        for c in sf._LINKED_UPDATE_FIELDS:
            rec[c] = {"value": ""}
        for k, v in filled.items():
            rec[k] = {"value": v}
        self.rows[rid] = rec
        return rec

    @staticmethod
    def _reject_double_wrap(fields):
        for code, v in (fields or {}).items():
            if isinstance(v, dict) and "value" in v:
                raise AssertionError(f"double-wrapped payload: {code}={v!r}")

    async def search_records(self, app, query, fields=None):
        m = re.search(r'LINEユーザーID = "([^"]+)"', query)
        assert m, query
        limit = int(re.search(r"limit (\d+)", query).group(1))
        out = [r for r in self.rows.values() if r["LINEユーザーID"]["value"] == m.group(1)]
        out.sort(key=lambda r: int(r["$id"]["value"]))
        return [{"$id": r["$id"]} for r in out[:limit]]

    async def get_record(self, app, rid):
        self.get_calls.append(str(rid))
        row = self.rows.get(str(rid))
        if row is None:
            raise hub_kintone.KintoneError(404, "GAIA_RE01", "not found")
        return {k: dict(v) for k, v in row.items()}

    async def update_record(self, app, rid, fields, revision=None):
        self._reject_double_wrap(fields)
        self.update_calls.append((str(rid), dict(fields), revision))
        row = self.rows[str(rid)]
        if self.conflict_next > 0:
            self.conflict_next -= 1
            row["$revision"] = {"value": str(int(row["$revision"]["value"]) + 1)}
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        if revision is not None and str(revision) != row["$revision"]["value"]:
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        for k, v in fields.items():
            row[k] = {"value": v}
        row["$revision"] = {"value": str(int(row["$revision"]["value"]) + 1)}

    async def create_record(self, app, fields):
        self._reject_double_wrap(fields)
        self.create_calls.append(dict(fields))
        if self.create_fail_next > 0:
            self.create_fail_next -= 1
            raise hub_kintone.KintoneError(500, "GAIA_XX", "down")
        self._id += 1
        rid = str(self._id)
        rec = {k: {"value": v} for k, v in fields.items()}
        rec["$id"] = {"value": rid}
        rec["$revision"] = {"value": "1"}
        self.rows[rid] = rec
        return rid

    def val(self, rid, code):
        return (self.rows[str(rid)].get(code) or {}).get("value")


class _LogCapture(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)

    def errors(self):
        return [r.getMessage() for r in self.records if r.levelno >= logging.ERROR]

    def text(self):
        return "\n".join(r.getMessage() for r in self.records)


class _DbBase(unittest.TestCase):
    """sqlite（aiosqlite）を 1 テスト 1 ファイルで用意し、shindan_link と inbound_event を作る。"""
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="shindan_link_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db",
            "JIKOU_LINE_ADD_URL": LINE_URL, "ATTORNEY_LINE_USER_ID": "U_attorney",
            "RAILWAY_PUBLIC_DOMAIN": PUBLIC_DOMAIN, "AUTOREPLY_PAUSED": "0"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(InboundBase.metadata.create_all)
                await c.run_sync(sl.metadata.create_all)
        _run(_create())
        db.reset_for_tests()
        # アプリ側 logger だけを捕捉する（sqlalchemy/httpx のデバッグログは
        # テスト基盤の出力=本番の sink ではない）
        self.cap = _LogCapture()
        for name in ("shindan", "hub.shindan_link", "main", "hub.durable_inbound"):
            lg = logging.getLogger(name)
            lvl = lg.level
            lg.setLevel(logging.DEBUG)
            lg.addHandler(self.cap)
            self.addCleanup(lg.removeHandler, self.cap)
            self.addCleanup(lg.setLevel, lvl)

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def q(self, coro):
        r = _run(coro)
        db.reset_for_tests()
        return r

    def rows(self):
        async def _q():
            async with db.session_scope() as s:
                return [dict(r._mapping) for r in (await s.execute(sa.select(sl.shindan_link))).all()]
        return self.q(_q())


# ── T1: 凍結文言・本文の形 ───────────────────────────────────────────────────────
class TestFrozenTexts(unittest.TestCase):
    def test_T1_greeting_pin_and_message_shape(self):
        self.assertEqual(hashlib.sha256(sl.FROZEN_GREETING.encode("utf-8")).hexdigest(),
                         PIN_GREETING)
        url = sl.link_url("https://" + PUBLIC_DOMAIN, "tok_x")
        self.assertEqual(url, f"https://{PUBLIC_DOMAIN}/shindan?k=tok_x")
        msg = sl.build_message(url)
        self.assertEqual(msg, sl.FROZEN_GREETING + "\n" + url)   # 文言+改行+URL のみ
        self.assertEqual(msg.count("\n"), 1)

    def test_done_text_pin_and_linked_result_is_prefix_of_frozen(self):
        self.assertEqual(hashlib.sha256(sf.LINKED_DONE_TEXT.encode("utf-8")).hexdigest(),
                         PIN_DONE)
        for p in "ABCD":
            self.assertTrue(sf.FROZEN_RESULTS[p].startswith(sf.result_text_linked(p)))
            self.assertNotIn("受付番号", sf.result_text_linked(p))

    def test_constants(self):
        self.assertEqual(sl.TTL_DAYS, 30)
        self.assertEqual(sl.QUERY_KEY, "k")
        self.assertEqual(sl.FORM_PATH, "/shindan")
        self.assertEqual(sf.LINK_WRITE_FAILED_REASON, "shindan_link_write_failed")

    def test_public_base_url_env_only(self):
        # fix1 SLL-01: RAILWAY_PUBLIC_DOMAIN のみ・未設定/空/不正形は空文字（fail-closed）
        with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": PUBLIC_DOMAIN}):
            self.assertEqual(sl.public_base_url(), "https://" + PUBLIC_DOMAIN)
        with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": ""}):
            self.assertEqual(sl.public_base_url(), "")
        with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": "bad/host"}):
            self.assertEqual(sl.public_base_url(), "")
        os.environ.pop("RAILWAY_PUBLIC_DOMAIN", None)
        self.assertEqual(sl.public_base_url(), "")
        self.assertEqual(sl.NO_PUBLIC_HOST_REASON, "shindan_link_no_public_host")


# ── T2/T3: follow → 1 回送信・再配送 0・unfollow 0・token は不透明 ─────────────────
class TestFollow(_DbBase):
    def setUp(self):
        super().setUp()
        self.client = TestClient(main.app)
        self.send = AsyncMock()
        patches = [
            patch.object(main, "_line_reply_with_fallback", self.send),
            patch.object(main.autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)),
            patch.dict(os.environ, {"INBOUND_EVENT_DURABLE_ENABLED": "1"}),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def _post(self, body, sig):
        return self.client.post("/webhook", content=body, headers={"X-Line-Signature": sig})

    def test_T2_follow_once_redelivery_zero_unfollow_zero(self):
        body, sig = _event_body("follow", "01FOLLOW0001")
        self.assertEqual(self._post(body, sig).status_code, 200)
        self.assertEqual(self.send.await_count, 1)
        self.assertEqual(self._post(body, sig).status_code, 200)      # 同 event 再配送
        self.assertEqual(self.send.await_count, 1)
        body2, sig2 = _event_body("unfollow", "01UNFOLLOW01")
        self.assertEqual(self._post(body2, sig2).status_code, 200)
        self.assertEqual(self.send.await_count, 1)
        # 別の follow（再 follow）は送る
        body3, sig3 = _event_body("follow", "01FOLLOW0002")
        self._post(body3, sig3)
        self.assertEqual(self.send.await_count, 2)

    def test_T3_message_and_token_opaque_and_stored(self):
        body, sig = _event_body("follow", "01FOLLOW0003")
        self._post(body, sig)
        args = self.send.await_args.args
        self.assertEqual(args[0], "rt-01FOLLOW0003")
        self.assertEqual(args[1], USER)
        text = args[2]
        head, url = text.split("\n")
        self.assertEqual(head, sl.FROZEN_GREETING)
        self.assertTrue(url.startswith(f"https://{PUBLIC_DOMAIN}/shindan?k="))
        token = url.split("k=", 1)[1]
        self.assertGreaterEqual(len(token), 32)
        self.assertNotIn(USER, url)
        self.assertNotIn("U" + USER[1:9], url)
        self.assertNotIn(NAME, text)
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["token"], token)
        self.assertEqual(rows[0]["line_user_id"], USER)
        self.assertIsNone(rows[0]["used_at"])
        exp = sl._aware(rows[0]["expires_at"])
        self.assertAlmostEqual((exp - sl._now()).total_seconds(), 30 * 86400, delta=120)
        # ログに token 全文・userId 全文は出ない
        self.assertNotIn(token, self.cap.text())
        self.assertNotIn(USER, self.cap.text())

    def test_paused_or_stoplist_no_send(self):
        with patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            self._post(*_event_body("follow", "01FOLLOWP"))
        self.send.assert_not_awaited()
        with patch.object(main.autoreply_stoplist, "is_suppressed", AsyncMock(return_value=True)):
            self._post(*_event_body("follow", "01FOLLOWS"))
        self.send.assert_not_awaited()
        self.assertEqual(self.rows(), [])                          # token も発行しない

    def test_no_public_host_no_send_no_token_info_line(self):
        # fix1 SLL-01: env 未設定は Host ヘッダがあっても送らない・token も発行しない
        os.environ.pop("RAILWAY_PUBLIC_DOMAIN", None)
        body, sig = _event_body("follow", "01FOLLOWH")
        resp = self.client.post("/webhook", content=body,
                                headers={"X-Line-Signature": sig,
                                         "host": "evil.example", "x-forwarded-host": "evil.example"})
        self.assertEqual(resp.status_code, 200)
        self.send.assert_not_awaited()
        self.assertEqual([r for r in self.rows()], [])            # DB 行 0
        infos = [r.getMessage() for r in self.cap.records
                 if "shindan_link_no_public_host" in r.getMessage()]
        self.assertEqual(len(infos), 1)
        self.assertEqual(self.cap.records[[r.getMessage() for r in self.cap.records].index(infos[0])].levelno,
                         logging.INFO)
        self.assertNotIn(USER, self.cap.text())
        # 設定済みなら従来どおり（Host ヘッダの別ドメインは URL に現れない）
        with patch.dict(os.environ, {"RAILWAY_PUBLIC_DOMAIN": PUBLIC_DOMAIN}):
            body2, sig2 = _event_body("follow", "01FOLLOWH2")
            self.client.post("/webhook", content=body2,
                             headers={"X-Line-Signature": sig2, "host": "evil.example",
                                      "x-forwarded-host": "evil.example"})
        self.assertEqual(self.send.await_count, 1)
        url = self.send.await_args.args[2].splitlines()[1]
        self.assertTrue(url.startswith(f"https://{PUBLIC_DOMAIN}/shindan?k="))
        self.assertNotIn("evil.example", url)

    def test_non_durable_skipped_no_send_no_token(self):
        # fix2 SLL-01: durable 無効は冪等キーを作れない=送らない・token も発行しない
        with patch.dict(os.environ, {"INBOUND_EVENT_DURABLE_ENABLED": "0"}):
            self.assertEqual(self._post(*_event_body("follow", "01FOLLOWN")).status_code, 200)
        self.send.assert_not_awaited()
        self.assertEqual(self.rows(), [])
        infos = [r for r in self.cap.records
                 if "shindan_link_skipped_non_durable" in r.getMessage()]
        self.assertEqual(len(infos), 1)
        self.assertEqual(infos[0].levelno, logging.INFO)

    def test_missing_webhook_event_id_skipped(self):
        # fix2 SLL-01: webhookEventId 欠落は代替 ID を生成せず送らない（durable ON でも）
        body = json.dumps({"events": [{"type": "follow", "replyToken": "rt-x",
                                       "source": {"userId": USER}}]}).encode()
        self.assertEqual(self._post(body, _sign(body)).status_code, 200)
        self.send.assert_not_awaited()
        self.assertEqual(self.rows(), [])
        self.assertEqual(len([r for r in self.cap.records
                              if "shindan_link_skipped_non_durable" in r.getMessage()]), 1)


# ── T4〜T8: フォーム側 ───────────────────────────────────────────────────────────
class _FormLinkBase(_DbBase):
    def setUp(self):
        super().setUp()
        self.client = TestClient(main.app)
        self.fake = _FakeApp21()
        self.notify_biz = AsyncMock(return_value=True)
        self.notify_admin = AsyncMock(return_value=True)
        patches = [
            patch.object(sf.hub_kintone, "create_record", self.fake.create_record),
            patch.object(sf.hub_kintone, "search_records", self.fake.search_records),
            patch.object(sf.hub_kintone, "get_record", self.fake.get_record),
            patch.object(sf.hub_kintone, "update_record", self.fake.update_record),
            patch.object(sf.notify, "notify_business", self.notify_biz),
            patch.object(sf.notify, "notify_admin_line", self.notify_admin),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        sf._attempts.clear()
        self.addCleanup(sf._attempts.clear)

    def issue(self, user=USER):
        return self.q(sl.issue(user))

    def used_at(self, token):
        return [r["used_at"] for r in self.rows() if r["token"] == token][0]

    def claimed_at(self, token):
        return [r["claimed_at"] for r in self.rows() if r["token"] == token][0]

    def busy_lines(self):
        return [r for r in self.cap.records if "shindan_link_claim_busy" in r.getMessage()]

    def post_linked(self, token, **over):
        data = dict(VALID)
        data.update(over)
        return self.client.post("/shindan", data=data, cookies={sf.LINK_COOKIE: token})


class TestFormGet(_FormLinkBase):
    def test_T4_valid_k_same_html_plus_cookie(self):
        plain = self.client.get("/shindan")
        token = self.issue()
        resp = self.client.get("/shindan", params={"k": token})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.text, plain.text)                    # 表示 HTML は同一
        self.assertEqual(resp.cookies.get(sf.LINK_COOKIE), token)
        self.assertIn("HttpOnly", resp.headers.get("set-cookie", ""))
        self.assertIn("Path=/shindan", resp.headers.get("set-cookie", ""))
        self.assertIsNone(plain.cookies.get(sf.LINK_COOKIE))

    def test_T4_invalid_expired_used_same_as_no_k(self):
        plain = self.client.get("/shindan").text
        # 無効
        r = self.client.get("/shindan", params={"k": "nosuchtoken"})
        self.assertEqual((r.status_code, r.text), (200, plain))
        self.assertIsNone(r.cookies.get(sf.LINK_COOKIE))
        # 期限切れ
        old = sl._now() - datetime.timedelta(days=31)
        expired = self.q(sl.issue(USER, now=old))
        r = self.client.get("/shindan", params={"k": expired})
        self.assertEqual((r.status_code, r.text), (200, plain))
        self.assertIsNone(r.cookies.get(sf.LINK_COOKIE))
        # 使用済み
        used = self.issue()
        claimed = self.q(sl.claim(used))
        self.assertIsNotNone(claimed)
        self.assertTrue(self.q(sl.mark_used(used, claimed)))       # fix2: 自分の予約で確定
        r = self.client.get("/shindan", params={"k": used})
        self.assertEqual((r.status_code, r.text), (200, plain))
        self.assertIsNone(r.cookies.get(sf.LINK_COOKIE))
        self.assertIn("link invalid head=nosu", self.cap.text())
        self.assertNotIn(used, self.cap.text())

    def test_alias_still_404(self):
        token = self.issue()
        self.assertEqual(self.client.get("/shindan/", params={"k": token}).status_code, 404)
        self.assertEqual(self.client.get("/shindan/x", params={"k": token}).status_code, 404)


class TestFormPostLinked(_FormLinkBase):
    def test_T5_existing_record_empty_only_no_create_used_at(self):
        self.fake.add("10", revision="5", 問い合わせ業者名="弁護士入力")
        token = self.issue()
        resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.create_calls, [])                # 新規作成なし
        self.assertEqual(len(self.fake.update_calls), 1)
        rid, fields, rev = self.fake.update_calls[0]
        self.assertEqual((rid, rev), ("10", "5"))
        self.assertEqual(fields, {"診断パターン": "A", "借入時期_テキスト": "5年以上前",
                                  "最終返済日_テキスト": "5年以上前",
                                  "裁判所書類": "何も届いていない"})
        self.assertEqual(self.fake.val("10", "問い合わせ業者名"), "弁護士入力")   # 上書きなし
        self.assertIsNotNone(self.used_at(token))
        self.assertIn(sf.LINKED_DONE_TEXT, resp.text)
        self.assertNotIn("受付番号", resp.text)
        self.assertNotIn("友だち追加", resp.text)
        self.assertIn(sf.PHOTO_ROUTE, resp.text)                    # 写真導線は残す
        self.assertIn("診断結果", resp.text)
        # 使用済み token での再 POST は k なし扱い（受付番号経路）
        resp2 = self.post_linked(token)
        self.assertEqual(resp2.status_code, 200)
        self.assertIn("受付番号", resp2.text)

    def test_T5_query_k_also_accepted_and_all_filled_is_noop_success(self):
        self.fake.add("10", 問い合わせ業者名="a", 借入時期_テキスト="b",
                      最終返済日_テキスト="c", 裁判所書類="d", 診断パターン="B")
        token = self.issue()
        resp = self.client.post("/shindan?k=" + token, data=VALID)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.update_calls, [])
        self.assertIsNotNone(self.used_at(token))
        self.assertIn(sf.LINKED_DONE_TEXT, resp.text)

    def test_T6_no_record_creates_with_line_user_id_no_number(self):
        token = self.issue()
        resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        fields = self.fake.create_calls[0]
        self.assertEqual(fields["LINEユーザーID"], USER)
        self.assertEqual(fields["受付チャネル"], "フォーム")
        self.assertEqual(fields["診断パターン"], "A")
        self.assertNotIn("受付番号", fields)
        self.assertEqual(fields["ラジオボタン"], "なし")
        self.assertIsNotNone(self.used_at(token))
        self.assertNotIn("受付番号", resp.text)
        self.assertIn(sf.LINKED_DONE_TEXT, resp.text)
        # 弁護士通知は固定文言+レコード No+パターン（PII なし）
        text = self.notify_biz.await_args.args[1]
        self.assertIn("案件レコードNo:101", text)
        self.assertNotIn(USER, text)
        self.assertNotIn("テスト債権者", text)

    def test_T7_cas_twice_or_5xx_no_used_at_error_line(self):
        self.fake.add("10", revision="5")
        self.fake.conflict_next = 2
        token = self.issue()
        resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(self.fake.update_calls), 2)            # 初回+再取得後 1 回
        # fix2 SLL-05: GET は find_user_record の 1 回 + 409 後の再取得 1 回のみ（2 回目の
        # 409 では再取得しない=旧実装なら 3 回）
        self.assertEqual(self.fake.get_calls, ["10", "10"])
        self.fake.get_calls.clear()
        self.fake.conflict_next = 2
        outcome, _rid = _run(sf._update_existing_linked(
            self.fake.rows["10"] and {k: dict(v) for k, v in self.fake.rows["10"].items()},
            {"診断パターン": "B", "借入時期_テキスト": "x"}))
        self.assertEqual(outcome, "unconverged")
        self.assertEqual(self.fake.get_calls, ["10"])                # 単体でも再取得は 1 回
        self.assertIsNone(self.used_at(token))
        self.assertIsNone(self.claimed_at(token))                    # fix2 SLL-02: 予約は解放
        self.assertIn(sf.LINKED_DONE_TEXT, resp.text)                # fix2 SLL-04: 固定文言は常に表示
        self.assertNotIn(sf.PHOTO_ROUTE, resp.text)                  # 写真導線なし（レコード未確定）
        self.assertNotIn("受付番号", resp.text)
        self.assertIn("診断結果", resp.text)
        errs = self.cap.errors()
        self.assertEqual(len(errs), 1)
        self.assertIn("shindan_link_write_failed", errs[0])
        self.assertIn("head=" + token[:4], errs[0])
        self.assertNotIn(token, errs[0])
        self.assertNotIn(USER, errs[0])
        self.notify_admin.assert_not_awaited()
        # 解放済み=再 POST で再試行でき、今度は成功して used_at が入る
        self.fake.conflict_next = 0
        resp_retry = self.post_linked(token)
        self.assertEqual(resp_retry.status_code, 200)
        self.assertIsNotNone(self.used_at(token))
        self.assertEqual(self.fake.val("10", "顧客名") if "顧客名" in self.fake.rows["10"] else "", "")
        self.assertEqual(self.fake.val("10", "診断パターン"), "A")
        # 5xx（作成失敗）
        self.cap.records.clear()
        self.fake.rows.clear()
        self.fake.create_fail_next = 1
        token2 = self.issue()
        self.post_linked(token2)
        self.assertIsNone(self.used_at(token2))
        self.assertIsNone(self.claimed_at(token2))
        self.assertEqual(len(self.cap.errors()), 1)
        # 未使用のまま=再送可能
        self.assertIsNotNone(self.q(sl.lookup(token2)))

    def test_T7b_ambiguous_records_no_write(self):
        self.fake.add("10")
        self.fake.add("11")
        token = self.issue()
        self.post_linked(token)
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(self.fake.create_calls, [])
        self.assertIsNone(self.used_at(token))
        self.assertEqual(len(self.cap.errors()), 1)

    def test_T8_no_k_post_unchanged(self):
        with patch.object(sf, "_draw_number", return_value="012345"):
            resp = self.client.post("/shindan", data=VALID)
        self.assertEqual(resp.status_code, 200)
        fields = self.fake.create_calls[0]
        self.assertEqual(fields["受付番号"], "012345")
        self.assertEqual(fields["LINEユーザーID"], "")
        self.assertIn("受付番号：012345", resp.text)
        self.assertIn("友だち追加", resp.text)
        self.assertEqual(self.rows(), [])

    # ── fix2 SLL-02: 同一 token の同時 POST は 1 本だけ書く ──────────────────────
    def test_SLL02_concurrent_same_token_single_create(self):
        token = self.issue()
        link = self.q(sl.lookup(token, include_claimed=True))
        self.assertIsNotNone(link)

        async def _two():
            return await asyncio.gather(sf._handle_submit(dict(VALID), link),
                                        sf._handle_submit(dict(VALID), link))
        r1, r2 = _run(_two())
        db.reset_for_tests()
        bodies = [r.body.decode("utf-8") for r in (r1, r2)]
        self.assertEqual([r.status_code for r in (r1, r2)], [200, 200])
        self.assertEqual(len(self.fake.create_calls), 1)            # App 21 作成 1 件
        self.assertIsNotNone(self.used_at(token))                   # used_at 1 回
        self.assertEqual(len(self.busy_lines()), 1)                 # busy INFO 1 行
        self.assertEqual(self.busy_lines()[0].levelno, logging.INFO)
        self.assertEqual(sorted(sf.PHOTO_ROUTE in b for b in bodies), [False, True])
        for b in bodies:
            self.assertIn(sf.LINKED_DONE_TEXT, b)
            self.assertNotIn("受付番号", b)
        self.assertNotIn(token, self.cap.text())

    def test_SLL02_claimed_token_post_is_busy_not_plain(self):
        # 予約中の token での POST は k なし経路へ落ちない（受付番号を発行しない）
        token = self.issue()
        self.assertIsNotNone(self.q(sl.claim(token)))
        resp = self.post_linked(token)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.fake.create_calls, [])
        self.assertEqual(self.fake.update_calls, [])
        self.assertNotIn("受付番号", resp.text)
        self.assertIn(sf.LINKED_DONE_TEXT, resp.text)
        self.assertEqual(len(self.busy_lines()), 1)
        self.assertIsNone(self.used_at(token))

    def test_SLL02_claim_semantics(self):
        token = self.issue()
        c1 = self.q(sl.claim(token))
        self.assertIsNotNone(c1)
        self.assertIsNone(self.q(sl.claim(token)))                  # 予約中は取れない
        self.assertIsNone(self.q(sl.lookup(token)))                 # GET 判定では無効
        self.assertIsNotNone(self.q(sl.lookup(token, include_claimed=True)))
        self.assertFalse(self.q(sl.mark_used(token, c1 + datetime.timedelta(seconds=1))))  # 他者の予約では確定不可
        self.assertTrue(self.q(sl.release(token)))
        self.assertIsNone(self.claimed_at(token))
        c2 = self.q(sl.claim(token))
        self.assertIsNotNone(c2)
        self.assertTrue(self.q(sl.mark_used(token, c2)))
        self.assertIsNone(self.q(sl.lookup(token, include_claimed=True)))   # 使用済み
        self.assertEqual(sl.CLAIM_TTL_SEC, 120)
        # TTL 超過の予約は取り直せる
        token3 = self.issue()
        old = sl._now() - datetime.timedelta(seconds=sl.CLAIM_TTL_SEC + 5)
        self.assertIsNotNone(self.q(sl.claim(token3, now=old)))
        self.assertIsNotNone(self.q(sl.claim(token3)))

    # ── fix2 SLL-03: cookie の衛生 ───────────────────────────────────────────────
    def _assert_cookie_deleted(self, resp):
        sc = resp.headers.get("set-cookie", "")
        self.assertIn(sf.LINK_COOKIE + "=", sc)
        self.assertIn("Max-Age=0", sc)
        self.assertIn("Path=/shindan", sc)
        self.assertIsNone(self.client.cookies.get(sf.LINK_COOKIE))

    def test_SLL03_invalid_k_deletes_previous_cookie_then_post_is_plain(self):
        self.fake.add("10")                                          # A の既存レコード
        token_a = self.issue()
        self.client.get("/shindan", params={"k": token_a})
        self.assertEqual(self.client.cookies.get(sf.LINK_COOKIE), token_a)
        resp = self.client.get("/shindan", params={"k": "nosuchtokenB"})
        self._assert_cookie_deleted(resp)
        with patch.object(sf, "_draw_number", return_value="012345"):
            post = self.client.post("/shindan", data=VALID)         # cookie なし=k なし経路
        self.assertEqual(post.status_code, 200)
        self.assertIn("受付番号：012345", post.text)
        self.assertEqual(self.fake.update_calls, [])                 # A のレコードに書かない
        self.assertEqual(self.fake.create_calls[0]["LINEユーザーID"], "")
        self.assertIsNone(self.used_at(token_a))

    def test_SLL03_no_k_get_deletes_previous_cookie(self):
        self.fake.add("10")
        token_a = self.issue()
        self.client.get("/shindan", params={"k": token_a})
        resp = self.client.get("/shindan")
        self._assert_cookie_deleted(resp)
        with patch.object(sf, "_draw_number", return_value="012345"):
            post = self.client.post("/shindan", data=VALID)
        self.assertIn("受付番号：012345", post.text)
        self.assertEqual(self.fake.update_calls, [])
        self.assertIsNone(self.used_at(token_a))

    def test_SLL03_claimed_k_get_is_invalid_and_deletes_cookie(self):
        token = self.issue()
        self.assertIsNotNone(self.q(sl.claim(token)))
        resp = self.client.get("/shindan", params={"k": token})
        self._assert_cookie_deleted(resp)

    # ── fix2 SLL-04: 結果ページの差分は 3 点のみ ─────────────────────────────────
    def test_SLL04_linked_result_diff_is_exactly_three_points(self):
        for pattern in "ABCD":
            with self.subTest(pattern=pattern):
                plain = sf._result_html(pattern, "654321", LINE_URL, "uptok")
                linked = sf._result_html_linked(pattern, "uptok")
                expected = plain
                # (1) 受付番号行を出さない
                self.assertIn("<br>受付番号：654321", expected)
                expected = expected.replace("<br>受付番号：654321", "", 1)
                # (2) 友だち追加ボタン+受付番号案内段落を出さない
                button = (f"<a class=\"btn\" href=\"{LINE_URL}\">"
                          "LINEで無料相談する（友だち追加）</a>")
                guide = ("<p>LINEで上記の受付番号をお送りいただくと、ご回答内容を引き継いで"
                         "スムーズにご案内できます。</p>")
                self.assertIn(button + guide, expected)
                # (3) 固定文言を同じ位置に追加
                expected = expected.replace(button + guide, f"<p>{sf.LINKED_DONE_TEXT}</p>", 1)
                self.assertEqual(linked, expected)
        # upload_token 無し（失敗/busy）でも固定文言は表示・写真導線だけ無い
        self.assertIn(sf.LINKED_DONE_TEXT, sf._result_html_linked("A"))
        self.assertNotIn(sf.PHOTO_ROUTE, sf._result_html_linked("A"))

    def test_judgement_unchanged_for_linked(self):
        token = self.issue()
        self.post_linked(token, last_pay="5年以内")
        self.assertEqual(self.fake.create_calls[0]["診断パターン"], "B")


# ── T9: alembic up/down 往復は alembic 起動が許可された test_db_foundation.py に置く
#   （test_db_foundation_hardening M01/D2: subprocess での alembic 起動は同ファイル限定）。
#   ここでは revision の親（origin/main の head）だけを pin する
class TestMigration(unittest.TestCase):
    def test_revision_parent_is_main_head(self):
        src = (REPO / "alembic" / "versions" / "20260914_a7d3f1c9e2b4_shindan_link.py").read_text(encoding="utf-8")
        self.assertIn("revision: str = 'a7d3f1c9e2b4'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'e7a9c4d1f6b3'", src)


if __name__ == "__main__":
    unittest.main()
