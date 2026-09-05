"""JIKOU-FORM-3: 受信書類写真の永続化（Part A: LINE 写真の取得+添付／
Part B: 診断フォームの写真アップロード）。

固定する仕様:
- Part A（hub/image_store）: LINE コンテンツ API からのストリーミング取得
  （Content-Length 事前検査+実読込の上限で即中断）・マジックバイト判定
  （jpeg/png/pdf/heic）・FILE 欄「受信書類写真」への CAS 追記（既存 fileKey 保持・
  409 → 再取得再構成・収束不能=上書きなし+要確認）・レコード未存在=未添付ログ
  のみ。受領返信（IMAGE-INTAKE-1）の成否と独立＝添付失敗でも返信は出る。
- Part B（shindan_form）: 本申込 /shindan は不変。結果画面に使い捨てトークン
  （secrets・受付番号レコード紐付け・TTL 15 分・1 回限り・有界 LRU・単一 worker
  前提）を埋め込み、multipart は専用ルート /shindan/photos/{token} にのみ受ける。
  ゲート（env→POST→トークン→Content-Type/Length→レート→消費）通過後にのみ
  有界ストリーミング解析（starlette の MultiPartParser 不使用・一時ファイルなし）。
- plain 値契約: fake は hub.kintone._wrap 境界を模し二重ラップを拒否する。
"""

import asyncio
import hashlib
import os
import tempfile
import unittest
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
    "APP_HOUKI": "40", "TOKEN_HOUKI": "d",
    "HOUKI_LINE_CHANNEL_SECRET": "houki_secret",
    "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki_token",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

from fastapi.testclient import TestClient  # noqa: E402
from starlette import formparsers as fp  # noqa: E402

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
import shindan_form as sf  # noqa: E402
from hub import houki_case_store  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import image_store as ims  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402
from hub import line_channel  # noqa: E402

JPEG = b"\xff\xd8\xff\xe0" + b"J" * 64
PNG = b"\x89PNG\r\n\x1a\n" + b"P" * 64
PDF = b"%PDF-1.4\n" + b"D" * 64
HEIC = b"\x00\x00\x00\x18ftypheic" + b"H" * 64
UNKNOWN = b"GIF89a" + b"G" * 64
_LINE_URL = "https://lin.ee/example"


def _run(coro):
    return asyncio.run(coro)


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ── kintone フェイク（_wrap 境界を模す・FILE 欄・$revision CAS） ────────────────
class _FakeApp:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self._id = 0
        self._key = 0
        self.uploads: dict[str, tuple[str, int, str]] = {}
        self.conflicts_left = 0          # update を 409 にする残回数
        self.update_error: Exception | None = None
        self.upload_error: Exception | None = None
        self.update_calls = 0

    @staticmethod
    def _reject_double_wrap(fields):
        for code, v in (fields or {}).items():
            if isinstance(v, dict) and "value" in v:
                raise AssertionError(f"double-wrapped payload: {code}={v!r}")

    async def create_record(self, app, fields):
        self._reject_double_wrap(fields)
        self._id += 1
        rid = str(self._id)
        rec = {k: {"value": v} for k, v in fields.items()}
        rec["$id"] = {"value": rid}
        rec["$revision"] = {"value": "1"}
        rec.setdefault(ims.PHOTO_FIELD, {"value": []})
        rec.setdefault("作成日時", {"value": "2026-09-05T00:00:00Z"})
        self.rows[rid] = rec
        return rid

    async def search_records(self, app, query, fields=None):
        return []

    async def get_record(self, app, record_id):
        rec = self.rows.get(str(record_id))
        if rec is None:
            raise hub_kintone.KintoneError(404, "GAIA_RE01", "not found")
        return {k: (dict(v) if isinstance(v, dict) else v)
                for k, v in rec.items()}

    async def update_record(self, app, record_id, fields, revision=None):
        self._reject_double_wrap(fields)
        self.update_calls += 1
        if self.conflicts_left > 0:
            self.conflicts_left -= 1
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        if self.update_error is not None:
            raise self.update_error
        rec = self.rows[str(record_id)]
        cur = int(rec["$revision"]["value"])
        if revision is not None and int(revision) != cur:
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        rec.update({k: {"value": v} for k, v in fields.items()})
        rec["$revision"] = {"value": str(cur + 1)}

    async def upload_file(self, app, filename, content, mime):
        if self.upload_error is not None:
            raise self.upload_error
        self._key += 1
        key = f"key{self._key}"
        self.uploads[key] = (filename, len(content), mime)
        return key

    def photos(self, rid):
        return [f["fileKey"] for f in self.rows[str(rid)][ims.PHOTO_FIELD]["value"]]

    def seed(self, fields: dict, photos: list[str] = ()):
        rid = _run(self.create_record(None, fields))
        self.rows[rid][ims.PHOTO_FIELD] = {"value": [
            {"fileKey": k, "name": f"{k}.jpg", "size": "10",
             "contentType": "image/jpeg"} for k in photos]}
        return rid


class _KintoneBase(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeApp()
        for p in (patch.object(hub_kintone, "create_record", self.fake.create_record),
                  patch.object(hub_kintone, "search_records", self.fake.search_records),
                  patch.object(hub_kintone, "get_record", self.fake.get_record),
                  patch.object(hub_kintone, "update_record", self.fake.update_record),
                  patch.object(hub_kintone, "upload_file", self.fake.upload_file)):
            p.start()
            self.addCleanup(p.stop)


# ── LINE コンテンツ API のフェイク（stream） ──────────────────────────────────
class _FakeStreamResp:
    def __init__(self, status, headers, chunks):
        self.status_code = status
        self.headers = headers
        self._chunks = chunks
        self.read_chunks = 0

    @property
    def is_success(self):
        return 200 <= self.status_code < 300

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    async def aiter_bytes(self):
        for c in self._chunks:
            self.read_chunks += 1
            yield c


class _FakeStreamClient:
    status = 200
    headers: dict = {}
    chunks: list = []
    calls: list = []
    last_resp = None

    def __init__(self, **_kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_a):
        return False

    def stream(self, method, url, headers=None):
        _FakeStreamClient.calls.append({"method": method, "url": url,
                                        "headers": headers or {}})
        resp = _FakeStreamResp(_FakeStreamClient.status,
                               dict(_FakeStreamClient.headers),
                               list(_FakeStreamClient.chunks))
        _FakeStreamClient.last_resp = resp
        return resp

    @classmethod
    def reset(cls, status=200, headers=None, chunks=None):
        cls.status = status
        cls.headers = headers or {}
        cls.chunks = chunks if chunks is not None else [JPEG]
        cls.calls = []
        cls.last_resp = None


# ══════════════════════════════════════════════════════════════════════════════
# Part A: hub/image_store
# ══════════════════════════════════════════════════════════════════════════════
class TestDetectFormat(unittest.TestCase):
    def test_closed_set(self):
        self.assertEqual(ims.detect_format(JPEG), ("jpg", "image/jpeg"))
        self.assertEqual(ims.detect_format(PNG), ("png", "image/png"))
        self.assertEqual(ims.detect_format(PDF), ("pdf", "application/pdf"))
        self.assertEqual(ims.detect_format(HEIC), ("heic", "image/heic"))
        for bad in (UNKNOWN, b"", b"\xff\xd8", b"<html>", b"\x00" * 12):
            with self.subTest(bad=bad[:6]):
                self.assertIsNone(ims.detect_format(bad))

    def test_limits_pinned(self):
        self.assertEqual(ims.MAX_IMAGE_BYTES, 10 * 1024 * 1024)
        self.assertEqual(ims.ATTACH_RETRIES, 3)
        self.assertEqual(ims.PHOTO_FIELD, "受信書類写真")


class TestFetchLineContent(unittest.TestCase):
    def setUp(self):
        _FakeStreamClient.reset()
        p = patch.object(ims.httpx, "AsyncClient", _FakeStreamClient)
        p.start()
        self.addCleanup(p.stop)

    def test_success_uses_channel_token_and_content_url(self):
        _FakeStreamClient.reset(chunks=[JPEG[:10], JPEG[10:]])
        data = _run(ims.fetch_line_content(line_channel.HOUKI_CHANNEL, "m123"))
        self.assertEqual(data, JPEG)
        call = _FakeStreamClient.calls[0]
        self.assertEqual(call["url"],
                         "https://api-data.line.me/v2/bot/message/m123/content")
        # チャネル別 token（env は他 suite が上書きし得るため実値で照合・
        # 時効/相続放棄で別値であることも固定）
        with patch.dict(os.environ, {"LINE_CHANNEL_ACCESS_TOKEN": "tok_jikou",
                                     "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "tok_houki"}):
            _FakeStreamClient.reset()
            _run(ims.fetch_line_content(line_channel.HOUKI_CHANNEL, "m1"))
            _run(ims.fetch_line_content(line_channel.JIKOU_CHANNEL, "m2"))
        self.assertEqual(_FakeStreamClient.calls[0]["headers"]["Authorization"],
                         "Bearer tok_houki")
        self.assertEqual(_FakeStreamClient.calls[1]["headers"]["Authorization"],
                         "Bearer tok_jikou")

    def test_non_2xx_raises_fetch_error(self):
        _FakeStreamClient.reset(status=404)
        with self.assertRaises(ims.ContentFetchError):
            _run(ims.fetch_line_content(line_channel.JIKOU_CHANNEL, "m1"))

    def test_declared_length_over_limit_aborts_before_reading(self):
        _FakeStreamClient.reset(headers={"content-length":
                                         str(ims.MAX_IMAGE_BYTES + 1)})
        with self.assertRaises(ims.ContentTooLarge):
            _run(ims.fetch_line_content(line_channel.JIKOU_CHANNEL, "m1"))
        self.assertEqual(_FakeStreamClient.last_resp.read_chunks, 0)

    def test_streamed_over_limit_aborts_midway(self):
        chunk = b"\xff\xd8\xff" + b"x" * (4 * 1024 * 1024)
        _FakeStreamClient.reset(chunks=[chunk, chunk, chunk, chunk])   # 16MB
        with self.assertRaises(ims.ContentTooLarge):
            _run(ims.fetch_line_content(line_channel.JIKOU_CHANNEL, "m1"))
        self.assertLess(_FakeStreamClient.last_resp.read_chunks, 4)


class TestAttachFiles(_KintoneBase):
    def test_appends_and_preserves_existing(self):
        rid = self.fake.seed({"LINEユーザーID": "U1"}, photos=["old1", "old2"])
        out = _run(ims.attach_files(ims.APP_JIKOU_CASE, rid,
                                    [("a.jpg", JPEG, "image/jpeg"),
                                     ("b.pdf", PDF, "application/pdf")]))
        self.assertEqual(out, "attached")
        self.assertEqual(self.fake.photos(rid), ["old1", "old2", "key1", "key2"])
        self.assertEqual(self.fake.uploads["key1"], ("a.jpg", len(JPEG), "image/jpeg"))
        self.assertEqual(self.fake.rows[rid]["$revision"]["value"], "2")

    def test_conflict_refetch_and_reconstruct(self):
        rid = self.fake.seed({"LINEユーザーID": "U1"}, photos=["old1"])
        self.fake.conflicts_left = 2
        out = _run(ims.attach_files(ims.APP_JIKOU_CASE, rid,
                                    [("a.jpg", JPEG, "image/jpeg")]))
        self.assertEqual(out, "attached")
        self.assertEqual(self.fake.photos(rid), ["old1", "key1"])
        self.assertEqual(self.fake.update_calls, 3)

    def test_unconverged_no_overwrite_after_retry_limit(self):
        rid = self.fake.seed({"LINEユーザーID": "U1"}, photos=["old1"])
        self.fake.conflicts_left = ims.ATTACH_RETRIES + 1
        out = _run(ims.attach_files(ims.APP_JIKOU_CASE, rid,
                                    [("a.jpg", JPEG, "image/jpeg")]))
        self.assertEqual(out, "unconverged")
        self.assertEqual(self.fake.photos(rid), ["old1"])            # 上書きなし
        self.assertEqual(self.fake.rows[rid]["$revision"]["value"], "1")

    def test_upload_or_update_error_is_failed(self):
        rid = self.fake.seed({"LINEユーザーID": "U1"}, photos=["old1"])
        self.fake.upload_error = hub_kintone.KintoneError(403, "GAIA_NO01", "x")
        self.assertEqual(_run(ims.attach_files(
            ims.APP_JIKOU_CASE, rid, [("a.jpg", JPEG, "image/jpeg")])), "failed")
        self.fake.upload_error = None
        self.fake.update_error = hub_kintone.KintoneError(400, "CB_VA01", "x")
        self.assertEqual(_run(ims.attach_files(
            ims.APP_JIKOU_CASE, rid, [("a.jpg", JPEG, "image/jpeg")])), "failed")
        self.assertEqual(self.fake.photos(rid), ["old1"])
        self.assertEqual(_run(ims.attach_files(
            ims.APP_JIKOU_CASE, "999", [("a.jpg", JPEG, "image/jpeg")])), "failed")


class TestIntakeLineImage(_KintoneBase):
    def setUp(self):
        super().setUp()
        _FakeStreamClient.reset()
        for p in (patch.object(ims.httpx, "AsyncClient", _FakeStreamClient),
                  patch.dict(os.environ, {"ATTORNEY_LINE_USER_ID": "Uattorney"})):
            p.start()
            self.addCleanup(p.stop)
        self.admin = AsyncMock(return_value=True)
        self.business = AsyncMock(return_value=True)
        for p in (patch.object(ims.notify, "notify_admin_line", self.admin),
                  patch.object(ims.notify, "notify_business", self.business)):
            p.start()
            self.addCleanup(p.stop)

    def _intake(self, channel="jikou", message_id="m1", rid=""):
        ch = (line_channel.JIKOU_CHANNEL if channel == "jikou"
              else line_channel.HOUKI_CHANNEL)
        app = (ims.APP_JIKOU_CASE if channel == "jikou"
               else houki_case_store.APP_HOUKI_CASE)
        return _run(ims.intake_line_image(channel, ch, app, "U_x", message_id, rid))

    def test_attached_keeps_existing_no_notify(self):
        rid = self.fake.seed({"LINEユーザーID": "U_x"}, photos=["old1"])
        self.assertEqual(self._intake(rid=rid), "attached")
        self.assertEqual(self.fake.photos(rid), ["old1", "key1"])
        self.assertEqual(self.fake.uploads["key1"][0], "line_m1.jpg")
        self.business.assert_not_awaited()
        self.admin.assert_not_awaited()

    def test_no_record_is_pending_without_notify(self):
        self.assertEqual(self._intake(rid=""), "no_record")
        self.assertEqual(_FakeStreamClient.calls, [])            # 取得もしない
        self.business.assert_not_awaited()
        self.assertEqual(self._intake(message_id=""), "no_message_id")

    def test_too_large_and_unknown_format_notify_without_attach(self):
        rid = self.fake.seed({"LINEユーザーID": "U_x"}, photos=["old1"])
        _FakeStreamClient.reset(headers={"content-length": str(ims.MAX_IMAGE_BYTES + 1)})
        self.assertEqual(self._intake(rid=rid), "too_large")
        _FakeStreamClient.reset(chunks=[UNKNOWN])
        self.assertEqual(self._intake(rid=rid), "unknown_format")
        self.assertEqual(self.fake.photos(rid), ["old1"])
        self.assertEqual(self.fake.uploads, {})
        self.assertEqual(self.business.await_count, 2)
        for call in self.business.await_args_list:
            text = call.args[1]
            self.assertIn(f"レコード番号: {rid}", text)
            self.assertNotIn("U_x", text)
            self.assertNotIn("m1", text)

    def test_fetch_failed_and_unconverged_notify(self):
        rid = self.fake.seed({"LINEユーザーID": "U_x"})
        _FakeStreamClient.reset(status=500)
        self.assertEqual(self._intake(rid=rid), "fetch_failed")
        _FakeStreamClient.reset()
        self.fake.conflicts_left = ims.ATTACH_RETRIES + 1
        self.assertEqual(self._intake(rid=rid), "unconverged")
        self.assertEqual(self.fake.photos(rid), [])
        self.assertEqual(self.business.await_count, 2)

    def test_houki_channel_notifies_admin_success_only_throttle(self):
        rid = self.fake.seed({"LINEユーザーID": "U_x"})
        _FakeStreamClient.reset(chunks=[UNKNOWN])
        self.assertEqual(self._intake(channel="houki", rid=rid), "unknown_format")
        self.admin.assert_awaited_once()
        kw = self.admin.await_args.kwargs
        self.assertEqual(kw["throttle_key"], f"houki_image_attach:{rid}")
        self.assertTrue(kw["throttle_on_success_only"])
        self.assertIn("相続放棄・要確認", self.admin.await_args.args[0])
        self.business.assert_not_awaited()

    def test_exception_inside_is_contained(self):
        rid = self.fake.seed({"LINEユーザーID": "U_x"})
        with patch.object(ims, "fetch_line_content",
                          AsyncMock(side_effect=RuntimeError("boom"))):
            self.assertEqual(self._intake(rid=rid), "fetch_failed")


# ── 受領返信フローとの独立性（IMAGE-INTAKE-1 の構造は不変） ───────────────────────
class _Chatlog28:
    """test_image_intake._FakeChatlog28 の最小版（マーカー/受領済み行）。"""

    def __init__(self):
        self.rows = []
        self._id = 0

    async def create(self, app, fields):
        self._id += 1
        self.rows.append({"$id": str(self._id), **fields})
        return str(self._id)

    async def search(self, app, query, fields=None):
        import re
        m_eq = re.search('category = "([^"]+)"', query)
        m_like = re.search('category like "([^"]+)"', query)
        m_uid = re.search('line_user_id = "([^"]+)"', query)
        rows = self.rows
        if m_uid:
            rows = [r for r in rows if r.get("line_user_id") == m_uid.group(1)]
        if m_eq:
            rows = [r for r in rows if r.get("category") == m_eq.group(1)]
        elif m_like:
            rows = [r for r in rows if m_like.group(1) in str(r.get("category") or "")]
        rows = sorted(rows, key=lambda r: int(r["$id"]), reverse="desc" in query)
        return [{"$id": {"value": r["$id"]},
                 "category": {"value": r.get("category", "")}} for r in rows[:1]]

    def receipts(self, channel):
        return [r for r in self.rows if r.get("category") == f"画像受領済:{channel}"]


class TestJikouFlowIndependence(unittest.TestCase):
    USER = "U_form3_jikou"

    def setUp(self):
        ii._pending.clear()
        self.addCleanup(ii._pending.clear)
        self.chatlog = _Chatlog28()
        self.record = {"$id": {"value": "77"}, "response_mode": {"value": "自動"}}
        self.push = AsyncMock(return_value=True)
        self.intake = AsyncMock(return_value="attached")
        for p in (patch.object(hub_kintone, "create_record", self.chatlog.create),
                  patch.object(hub_kintone, "search_records", self.chatlog.search),
                  patch.object(ii, "DEBOUNCE_SEC", 0.01),
                  patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0",
                                          "ATTORNEY_LINE_USER_ID": "Uattorney"}),
                  patch.object(main.autoreply_stoplist, "is_suppressed",
                               AsyncMock(return_value=False)),
                  patch.object(main, "get_app21_record",
                               AsyncMock(return_value=self.record)),
                  patch.object(ii, "push_text", self.push),
                  patch("hub.notify.notify_business", AsyncMock(return_value=True)),
                  patch.object(main, "ATTORNEY_LINE_USER_ID", "Uattorney"),
                  patch.object(ims, "intake_line_image", self.intake)):
            p.start()
            self.addCleanup(p.stop)

    def test_message_id_passed_to_store_with_app21_record(self):
        _run(main._process_line_image_event("t1", self.USER, "ev1", "msg-1"))
        self.intake.assert_awaited_once()
        args = self.intake.await_args.args
        self.assertEqual(args[0], "jikou")
        self.assertIs(args[1], line_channel.JIKOU_CHANNEL)
        self.assertIs(args[2], ims.APP_JIKOU_CASE)
        self.assertEqual(args[3:], (self.USER, "msg-1", "77"))
        self.push.assert_awaited_once()                          # 受領返信は従来どおり
        self.assertEqual(len(self.chatlog.receipts("jikou")), 1)

    def test_store_failure_does_not_stop_receipt(self):
        self.intake.side_effect = RuntimeError("boom")
        _run(main._process_line_image_event("t1", self.USER, "ev1", "msg-1"))
        self.push.assert_awaited_once()
        self.assertEqual(len(self.chatlog.receipts("jikou")), 1)

    def test_no_message_id_keeps_legacy_signature_and_skips_store(self):
        _run(main._process_line_image_event("t1", self.USER, "ev1"))
        self.intake.assert_not_awaited()
        self.push.assert_awaited_once()

    def test_no_record_passes_empty_record_id(self):
        with patch.object(main, "get_app21_record", AsyncMock(return_value=None)):
            _run(main._process_line_image_event("t1", self.USER, "ev1", "msg-2"))
        self.assertEqual(self.intake.await_args.args[5], "")
        self.push.assert_awaited_once()

    def test_human_mode_still_attaches(self):
        self.record["response_mode"] = {"value": "人対応"}
        _run(main._process_line_image_event("t1", self.USER, "ev1", "msg-3"))
        self.intake.assert_awaited_once()
        self.push.assert_not_awaited()                           # 人対応は無言のまま


class TestHoukiFlowIndependence(unittest.TestCase):
    USER = "U_form3_houki"

    def setUp(self):
        ii._pending.clear()
        self.addCleanup(ii._pending.clear)
        self.chatlog = _Chatlog28()
        self.push = AsyncMock(return_value=True)
        self.intake = AsyncMock(return_value="attached")
        self.case = {"$id": {"value": "40-5"}}
        for p in (patch.object(hub_kintone, "create_record", self.chatlog.create),
                  patch.object(hub_kintone, "search_records", self.chatlog.search),
                  patch.object(ii, "DEBOUNCE_SEC", 0.01),
                  patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0"}),
                  patch.object(ii, "is_suppressed", AsyncMock(return_value=False)),
                  patch.object(ii, "push_text", self.push),
                  patch("hub.notify.notify_admin_line", AsyncMock(return_value=True)),
                  patch.object(houki_case_store, "fetch_case",
                               AsyncMock(return_value=self.case)),
                  patch.object(ims, "intake_line_image", self.intake)):
            p.start()
            self.addCleanup(p.stop)

    def test_message_id_passed_with_app40_record(self):
        _run(ii.handle_houki_image(self.USER, "hv1", "msg-h1"))
        self.intake.assert_awaited_once()
        args = self.intake.await_args.args
        self.assertEqual(args[0], "houki")
        self.assertIs(args[1], line_channel.HOUKI_CHANNEL)
        self.assertIs(args[2], houki_case_store.APP_HOUKI_CASE)
        self.assertEqual(args[3:], (self.USER, "msg-h1", "40-5"))
        self.push.assert_awaited_once()
        self.assertEqual(len(self.chatlog.receipts("houki")), 1)

    def test_store_failure_or_no_case_does_not_stop_receipt(self):
        self.intake.side_effect = RuntimeError("boom")
        _run(ii.handle_houki_image(self.USER, "hv1", "msg-h1"))
        self.push.assert_awaited_once()
        with patch.object(houki_case_store, "fetch_case",
                          AsyncMock(return_value=None)):
            self.intake.side_effect = None
            _run(ii.handle_houki_image(self.USER, "hv2", "msg-h2"))
        self.assertEqual(self.intake.await_args.args[5], "")
        self.assertEqual(self.push.await_count, 2)

    def test_legacy_signature_without_message_id(self):
        _run(ii.handle_houki_image(self.USER, "hv1"))
        self.intake.assert_not_awaited()
        self.push.assert_awaited_once()


class TestWebhookWiringPassesMessageId(unittest.TestCase):
    def test_jikou_webhook_passes_message_id(self):
        client = TestClient(main.app)
        handler = AsyncMock()
        body = ('{"events":[{"type":"message","webhookEventId":"wev-1",'
                '"replyToken":"rt","source":{"userId":"U1"},'
                '"message":{"type":"image","id":"555"}}]}').encode()
        with patch.object(main, "verify_signature", lambda b, s: True), \
             patch.object(main, "_process_line_image_event", handler):
            r = client.post("/webhook", content=body,
                            headers={"X-Line-Signature": "x"})
        self.assertEqual(r.status_code, 200)
        handler.assert_awaited_once_with("rt", "U1", "wev-1", "555")

    def test_houki_webhook_passes_message_id(self):
        import houki_bot.router as hr
        client = TestClient(main.app)
        handler = AsyncMock()
        body = ('{"events":[{"type":"message","webhookEventId":"wev-2",'
                '"replyToken":"rt","source":{"userId":"U2"},'
                '"message":{"type":"image","id":"777"}}]}').encode()
        with patch.object(hr, "verify_line_signature", lambda c, b, s: True), \
             patch.object(hr, "houki_channel_disabled_reason", lambda: None), \
             patch.object(hr, "_record_inbound", AsyncMock()), \
             patch.object(ii, "handle_houki_image", handler):
            r = client.post("/webhook/souzoku-houki", content=body,
                            headers={"X-Line-Signature": "x"})
        self.assertEqual(r.status_code, 200)
        handler.assert_awaited_once_with("U2", "wev-2", "777")


class TestHoukiBotClosedSetUnchanged(unittest.TestCase):
    def test_no_new_imports_or_effect_names_in_houki_bot(self):
        # checker 更新なし: houki_bot 内に取得/添付の実体（httpx・upload_file・
        # image_store）を置かない（AST checker の閉集合は不変）
        from pathlib import Path
        for path in Path("houki_bot").glob("*.py"):
            src = path.read_text(encoding="utf-8")
            self.assertNotIn("image_store", src, path)
            self.assertNotIn("upload_file", src, path)
            self.assertNotIn("httpx", src, path)


class TestKnownItemsFromFileField(unittest.TestCase):
    def test_photo_field_value_marks_received(self):
        rec = {"受信書類写真": {"value": [{"fileKey": "k1", "name": "a.jpg"}]}}
        self.assertEqual(cr.build_known_items(rec, [])["書類写真"], "受領済み")
        self.assertNotIn("書類写真", cr.build_known_items(
            {"受信書類写真": {"value": []}}, []))
        self.assertNotIn("書類写真", cr.build_known_items({}, []))
        # 履歴マーカー由来（従来）も維持
        hist = [{"role": "assistant", "content": cr.IMAGE_RECEIPT_REPLY}]
        self.assertEqual(cr.build_known_items(None, hist)["書類写真"], "受領済み")


# ══════════════════════════════════════════════════════════════════════════════
# Part B: shindan_form 写真アップロード
# ══════════════════════════════════════════════════════════════════════════════
def _multipart(parts: list[tuple[str, str | None, bytes]],
               boundary: str = "----form3boundary") -> tuple[str, bytes]:
    """parts=[(name, filename|None, content)] → (Content-Type, body)。"""
    out = bytearray()
    for name, filename, content in parts:
        out += f"--{boundary}\r\n".encode()
        disp = f'Content-Disposition: form-data; name="{name}"'
        if filename is not None:
            disp += f'; filename="{filename}"'
        out += disp.encode() + b"\r\n"
        if filename is not None:
            out += b"Content-Type: application/octet-stream\r\n"
        out += b"\r\n" + content + b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return f"multipart/form-data; boundary={boundary}", bytes(out)


class _NoParser:
    """starlette の multipart/form 解析器・一時ファイル・自前解析器へ到達
    しないことを固定（ゲート前遮断の検証）。"""

    def __init__(self, own_parser: bool = True):
        self.own_parser = own_parser

    def __enter__(self):
        def _boom(*_a, **_k):
            raise AssertionError("parser reached before gate")
        self._p = [patch.object(fp.MultiPartParser, "__init__", _boom),
                   patch.object(fp.FormParser, "__init__", _boom),
                   patch.object(tempfile, "SpooledTemporaryFile", _boom)]
        if self.own_parser:
            self._p.append(patch.object(sf, "_parse_multipart_stream", _boom))
        for p in self._p:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._p:
            p.stop()
        return False


class TestPhotoFrozenTextsAndLimits(unittest.TestCase):
    # 画面文言（司令塔案・弁護士裁定で差し替え可・凍結後は本 pin を票の根拠つきで更新）
    PROMPT_SHA256 = "bafb31fd73d6fd112cb41c6e258d8112f452649ddee72529eee2a48d7d6de86a"
    DONE_SHA256 = "a02995f084ffb406e91a226b715774efe7b737c6129c894af67dfc38da6d9104"

    def test_texts_pinned(self):
        self.assertEqual(_sha(sf.PHOTO_PROMPT_TEXT), self.PROMPT_SHA256)
        self.assertEqual(_sha(sf.PHOTO_DONE_TEXT), self.DONE_SHA256)
        self.assertEqual(
            sf.PHOTO_PROMPT_TEXT,
            "督促状・請求書・訴状などのお写真をお持ちの場合は、こちらから送信"
            "できます（任意・5枚まで）。後からLINEでお送りいただくこともできます。")
        self.assertEqual(sf.PHOTO_DONE_TEXT,
                         "お写真を受け付けました。LINEの無料相談へお進みください。")
        self.assertNotIn("{", sf.PHOTO_PROMPT_TEXT + sf.PHOTO_DONE_TEXT)

    def test_limits_pinned(self):
        self.assertEqual(sf.PHOTO_MAX_PARTS, 5)
        self.assertEqual(sf.PHOTO_MAX_PART_BYTES, 10 * 1024 * 1024)
        self.assertEqual(sf.PHOTO_MAX_TOTAL_BYTES, 30 * 1024 * 1024)
        self.assertEqual(sf.UPLOAD_TOKEN_TTL_SECONDS, 15 * 60)
        self.assertEqual(sf.MAX_UPLOAD_TOKENS, 5000)
        self.assertEqual(sf.PHOTO_ROUTE, "/shindan/photos")
        self.assertEqual(sf.MAX_BODY_BYTES, 64 * 1024)          # 本申込は不変


class TestUploadToken(unittest.TestCase):
    def setUp(self):
        sf._upload_tokens.clear()
        self.addCleanup(sf._upload_tokens.clear)

    def test_issue_peek_claim_release_consume(self):
        # fix1（H3-01）: 未使用 → 予約（claim）→ 解除（release）で未使用へ戻る／
        # 予約 → 確定（consume）で使用済み（削除・以後 404）
        tok = sf.issue_upload_token("12", "123456", 1000.0)
        self.assertGreaterEqual(len(tok), 32)
        self.assertEqual(sf._upload_token_entry(tok, 1000.0), ("12", "123456"))
        self.assertEqual(sf.claim_upload_token(tok, 1001.0), ("12", "123456"))
        self.assertIsNone(sf._upload_token_entry(tok, 1001.0))   # 予約中は無効扱い
        self.assertIsNone(sf.claim_upload_token(tok, 1001.0))    # 二重予約不可
        sf.release_upload_token(tok)
        self.assertEqual(sf._upload_token_entry(tok, 1002.0), ("12", "123456"))
        self.assertEqual(sf._upload_tokens[tok][0], 1000.0)      # TTL は発行時刻基準
        self.assertEqual(sf.claim_upload_token(tok, 1002.0), ("12", "123456"))
        sf.consume_upload_token(tok)
        self.assertNotIn(tok, sf._upload_tokens)
        self.assertIsNone(sf._upload_token_entry(tok, 1002.0))
        self.assertIsNone(sf.claim_upload_token(tok, 1002.0))
        sf.release_upload_token(tok)                              # 使用済みは戻らない
        self.assertNotIn(tok, sf._upload_tokens)
        self.assertIsNone(sf.claim_upload_token("", 1001.0))
        self.assertIsNone(sf.claim_upload_token("nope", 1001.0))

    def test_ttl_expiry(self):
        tok = sf.issue_upload_token("12", "123456", 1000.0)
        self.assertIsNotNone(sf._upload_token_entry(
            tok, 1000.0 + sf.UPLOAD_TOKEN_TTL_SECONDS))
        self.assertIsNone(sf._upload_token_entry(
            tok, 1000.0 + sf.UPLOAD_TOKEN_TTL_SECONDS + 1))
        self.assertNotIn(tok, sf._upload_tokens)                  # 期限切れは削除

    def test_bounded_lru(self):
        with patch.object(sf, "MAX_UPLOAD_TOKENS", 3):
            toks = [sf.issue_upload_token(str(i), "000000", 1000.0) for i in range(5)]
        self.assertEqual(len(sf._upload_tokens), 3)
        self.assertIsNone(sf._upload_token_entry(toks[0], 1000.0))
        self.assertIsNotNone(sf._upload_token_entry(toks[4], 1000.0))

    def test_tokens_are_unpredictable(self):
        a = sf.issue_upload_token("1", "000000", 1.0)
        b = sf.issue_upload_token("1", "000000", 1.0)
        self.assertNotEqual(a, b)


class _PhotoBase(_KintoneBase):
    VALID = {"creditor": "テスト", "borrow": "5年以上前",
             "last_pay": "5年以上前", "court_doc": "何も届いていない"}

    def setUp(self):
        super().setUp()
        self.client = TestClient(main.app)
        sf._attempts.clear()
        sf._photo_attempts.clear()
        sf._upload_tokens.clear()
        for c in (sf._attempts.clear, sf._photo_attempts.clear,
                  sf._upload_tokens.clear):
            self.addCleanup(c)
        self.admin = AsyncMock(return_value=True)
        for p in (patch.dict(os.environ, {"JIKOU_LINE_ADD_URL": _LINE_URL}),
                  patch.object(sf.notify, "notify_business", AsyncMock(return_value=True)),
                  patch.object(sf.notify, "notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)

    def submit(self) -> tuple[str, str]:
        """本申込を 1 回行い (token, record_id) を返す。"""
        before = set(sf._upload_tokens)
        r = self.client.post("/shindan", data=self.VALID)
        self.assertEqual(r.status_code, 200)
        new = set(sf._upload_tokens) - before
        self.assertEqual(len(new), 1)                       # 申込 1 回=トークン 1 個
        tok = new.pop()
        rid = sf._upload_tokens[tok][1]
        return tok, rid

    def post_photos(self, tok, parts, **kw):
        ct, body = _multipart(parts)
        headers = {"Content-Type": ct, **kw.pop("headers", {})}
        return self.client.post(f"{sf.PHOTO_ROUTE}/{tok}", content=body,
                                headers=headers, **kw)


class TestResultPageEmbedsToken(_PhotoBase):
    def test_result_page_has_photo_form_with_token_path(self):
        r = self.client.post("/shindan", data=self.VALID)
        tok, rid = next(iter(sf._upload_tokens.items()))
        html = r.text
        self.assertIn(sf.PHOTO_PROMPT_TEXT, html)
        self.assertIn(f'action="{sf.PHOTO_ROUTE}/{tok}"', html)
        self.assertIn('enctype="multipart/form-data"', html)
        self.assertIn(f'name="{sf.PHOTO_PART_NAME}"', html)
        self.assertIn("multiple", html)
        self.assertEqual(rid[1], self.fake.rows[rid[1]]["$id"]["value"])
        # 本申込の結果文言・受付番号・LINE 誘導は従来どおり
        self.assertIn("受付番号：", html)
        self.assertIn(_LINE_URL, html)
        # 本申込の入口は Form 依存なし（FORM-1 pin と同じ）
        import inspect
        self.assertEqual(list(inspect.signature(sf.shindan_entry).parameters),
                         ["request"])

    def test_form_page_unchanged_no_photo_form(self):
        r = self.client.get("/shindan")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("multipart", r.text)
        self.assertNotIn(sf.PHOTO_ROUTE, r.text)


class TestPhotoGatesBeforeBody(_PhotoBase):
    def test_env_unset_404(self):
        tok, _rid = self.submit()
        os.environ.pop("JIKOU_LINE_ADD_URL", None)
        with _NoParser():
            r = self.post_photos(tok, [("photo", "a.jpg", JPEG)])
            self.assertEqual(r.status_code, 404)
        self.assertIn(tok, sf._upload_tokens)                      # 消費されない

    def test_missing_expired_used_token_404_before_parse(self):
        tok, _rid = self.submit()
        with _NoParser():
            for path in (sf.PHOTO_ROUTE, sf.PHOTO_ROUTE + "/",
                         sf.PHOTO_ROUTE + "/nope", "/shindan/x"):
                ct, body = _multipart([("photo", "a.jpg", JPEG)])
                r = self.client.post(path, content=body,
                                     headers={"Content-Type": ct},
                                     follow_redirects=False)
                self.assertEqual(r.status_code, 404, path)
                self.assertNotIn("location", r.headers)
            # 期限切れ
            issued, rid, num, claimed = sf._upload_tokens[tok]
            sf._upload_tokens[tok] = (issued - sf.UPLOAD_TOKEN_TTL_SECONDS - 1,
                                      rid, num, claimed)
            r = self.post_photos(tok, [("photo", "a.jpg", JPEG)])
            self.assertEqual(r.status_code, 404)
            self.assertNotIn(tok, sf._upload_tokens)
        # 使用済み（正常 1 回 → 同トークン再利用は 404・解析器不到達）
        tok2, rid2 = self.submit()
        self.assertEqual(self.post_photos(tok2, [("photo", "a.jpg", JPEG)]).status_code, 200)
        with _NoParser():
            self.assertEqual(self.post_photos(tok2, [("photo", "b.jpg", JPEG)]).status_code, 404)
        self.assertEqual(self.fake.photos(rid2), ["key1"])

    def test_non_post_methods_404(self):
        tok, _rid = self.submit()
        with _NoParser():
            for method in ("get", "head", "put", "patch", "delete", "options"):
                r = getattr(self.client, method)(f"{sf.PHOTO_ROUTE}/{tok}")
                self.assertEqual(r.status_code, 404, method)
        self.assertIn(tok, sf._upload_tokens)

    def test_content_type_and_length_gate_404(self):
        tok, _rid = self.submit()
        _ct, body = _multipart([("photo", "a.jpg", JPEG)])
        with _NoParser():
            # urlencoded は写真ルートでは受けない
            r = self.client.post(f"{sf.PHOTO_ROUTE}/{tok}", data=self.VALID)
            self.assertEqual(r.status_code, 404)
            # boundary なし
            r = self.client.post(f"{sf.PHOTO_ROUTE}/{tok}", content=body,
                                 headers={"Content-Type": "multipart/form-data"})
            self.assertEqual(r.status_code, 404)
            # Content-Length 超過（宣言値）
            r = self.client.post(
                f"{sf.PHOTO_ROUTE}/{tok}", content=body,
                headers={"Content-Type": _ct,
                         "Content-Length": str(sf.PHOTO_MAX_TOTAL_BYTES + 1)})
            self.assertEqual(r.status_code, 404)
            # Content-Length 欠落（chunked）
            r = self.client.post(f"{sf.PHOTO_ROUTE}/{tok}", content=iter([body]),
                                 headers={"Content-Type": _ct,
                                          "Transfer-Encoding": "chunked"})
            self.assertEqual(r.status_code, 404)
        self.assertIn(tok, sf._upload_tokens)
        self.assertEqual(len(sf._photo_attempts), 0)                # レート計上前

    def test_rate_limit_dedicated_bucket_before_consume(self):
        toks = [self.submit()[0] for _ in range(sf._PHOTO_RATE_LIMIT + 1)]
        for tok in toks[:-1]:
            self.assertEqual(self.post_photos(tok, [("photo", "a.jpg", JPEG)]).status_code, 200)
        with _NoParser():
            r = self.post_photos(toks[-1], [("photo", "a.jpg", JPEG)])
        self.assertEqual(r.status_code, 429)
        self.assertIn(toks[-1], sf._upload_tokens)                  # 429 は消費しない
        # 専用バケット: 写真 POST は _photo_attempts に、本申込は _attempts に別計上
        self.assertEqual(sum(c for _s, c in sf._photo_attempts.values()),
                         sf._PHOTO_RATE_LIMIT + 1)
        self.assertEqual(sum(c for _s, c in sf._attempts.values()),
                         sf._PHOTO_RATE_LIMIT + 1)
        self.assertLess(sf._PHOTO_RATE_LIMIT, sf.RATE_LIMIT)        # 本申込側は 429 でない


class TestPhotoBoundedParsing(_PhotoBase):
    def test_too_many_parts_aborts_immediately(self):
        tok, rid = self.submit()
        parts = [("photo", f"{i}.jpg", JPEG) for i in range(sf.PHOTO_MAX_PARTS + 1)]
        with _NoParser(own_parser=False):
            r = self.post_photos(tok, parts)
        self.assertEqual(r.status_code, 413)
        self.assertEqual(self.fake.photos(rid), [])
        self.assertEqual(self.fake.uploads, {})

    def test_single_file_over_limit_aborts(self):
        tok, rid = self.submit()
        big = b"\xff\xd8\xff" + b"x" * sf.PHOTO_MAX_PART_BYTES      # 10MB+3
        with _NoParser(own_parser=False):
            r = self.post_photos(tok, [("photo", "big.jpg", big)])
        self.assertEqual(r.status_code, 413)
        self.assertEqual(self.fake.uploads, {})

    def test_streaming_abort_does_not_read_rest(self):
        boundary = b"----form3boundary"
        head = (b"--" + boundary + b"\r\nContent-Disposition: form-data; "
                b'name="photo"; filename="a.jpg"\r\n\r\n')
        chunk = b"\xff\xd8\xff" + b"x" * (3 * 1024 * 1024)
        consumed = []

        async def stream():
            yield head
            for i in range(10):                                   # 30MB+ 相当
                consumed.append(i)
                yield chunk
        with self.assertRaises(sf._MultipartLimit):
            _run(sf._parse_multipart_stream(stream(), boundary))
        self.assertLess(len(consumed), 10)                         # 途中で中断

    def test_parser_extracts_only_photo_parts_and_skips_empty(self):
        ct, body = _multipart([("photo", "", b""),                # 未選択の入力
                               ("photo", "a.jpg", JPEG),
                               ("photo", "b.png", PNG)])

        async def stream():
            yield body[:7]
            yield body[7:]
        files = _run(sf._parse_multipart_stream(stream(), b"----form3boundary"))
        self.assertEqual(files, [JPEG, PNG])

    def test_unknown_field_name_rejected(self):
        tok, rid = self.submit()
        r = self.post_photos(tok, [("creditor", None, b"x"),
                                   ("photo", "a.jpg", JPEG)])
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.fake.uploads, {})

    def test_malformed_and_empty_selection(self):
        tok, _rid = self.submit()
        r = self.client.post(f"{sf.PHOTO_ROUTE}/{tok}", content=b"--garbage",
                             headers={"Content-Type":
                                      "multipart/form-data; boundary=zzz"})
        self.assertEqual(r.status_code, 400)
        tok, _rid = self.submit()
        r = self.post_photos(tok, [("photo", "", b"")])
        self.assertEqual(r.status_code, 400)

    def test_no_temp_files_and_no_starlette_parser(self):
        tok, rid = self.submit()
        with _NoParser(own_parser=False):
            r = self.post_photos(tok, [("photo", "a.jpg", JPEG),
                                       ("photo", "b.pdf", PDF)])
        self.assertEqual(r.status_code, 200)
        self.assertEqual(self.fake.photos(rid), ["key1", "key2"])


class TestPhotoAttachAndPages(_PhotoBase):
    def test_success_attaches_to_receipt_record_and_shows_count_only(self):
        tok, rid = self.submit()
        self.fake.rows[rid][ims.PHOTO_FIELD] = {"value": [
            {"fileKey": "human1", "name": "x.jpg"}]}             # 人の添付
        r = self.post_photos(tok, [("photo", "a.jpg", JPEG),
                                   ("photo", "b.heic", HEIC),
                                   ("photo", "c.pdf", PDF)])
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.PHOTO_DONE_TEXT, r.text)
        self.assertIn("3枚", r.text)
        self.assertIn(_LINE_URL, r.text)
        self.assertNotIn("a.jpg", r.text)                          # 入力値の非反射
        self.assertEqual(self.fake.photos(rid), ["human1", "key1", "key2", "key3"])
        number = self.fake.rows[rid]["受付番号"]["value"]
        self.assertEqual([u[0] for u in self.fake.uploads.values()],
                         [f"form_{number}_1.jpg", f"form_{number}_2.heic",
                          f"form_{number}_3.pdf"])
        self.assertEqual([u[2] for u in self.fake.uploads.values()],
                         ["image/jpeg", "image/heic", "application/pdf"])
        self.assertEqual(self.fake.rows[rid]["LINEユーザーID"]["value"], "")
        self.admin.assert_not_awaited()

    def test_unknown_format_no_attach_fixed_400(self):
        tok, rid = self.submit()
        r = self.post_photos(tok, [("photo", "a.jpg", JPEG),
                                   ("photo", "evil.exe", UNKNOWN)])
        self.assertEqual(r.status_code, 400)
        self.assertEqual(self.fake.uploads, {})
        self.assertEqual(self.fake.photos(rid), [])
        self.assertNotIn("evil", r.text)

    def test_attach_failure_fixed_500_and_notify_without_pii(self):
        tok, rid = self.submit()
        number = self.fake.rows[rid]["受付番号"]["value"]
        self.fake.conflicts_left = ims.ATTACH_RETRIES + 1
        r = self.post_photos(tok, [("photo", "a.jpg", JPEG)])
        self.assertEqual(r.status_code, 500)
        self.assertNotIn(number, r.text)
        self.assertEqual(self.fake.photos(rid), [])                 # 上書きなし
        self.admin.assert_awaited_once()
        text = self.admin.await_args.args[0]
        self.assertIn(f"受付番号:{number}", text)
        self.assertIn("区分:unconverged", text)
        self.assertNotIn("テスト", text)                             # 債権者名なし
        self.assertEqual(self.admin.await_args.kwargs["throttle_key"],
                         "shindan_photos")

    def test_form1_public_surface_unchanged(self):
        # 本申込は urlencoded のみ・multipart は従来どおり 404（FORM-1 pin）
        ct, body = _multipart([("photo", "a.jpg", JPEG)])
        with _NoParser():
            r = self.client.post("/shindan", content=body,
                                 headers={"Content-Type": ct})
        self.assertEqual(r.status_code, 404)
        paths = [r.path for r in sf.router.routes]
        self.assertEqual(paths, ["/shindan", sf.PHOTO_ROUTE + "/{token}",
                                 "/shindan/{_rest:path}"])


class TestTokenClaimReleaseConsume(_PhotoBase):
    """JIKOU-FORM-3-fix1（R-JIKOU-FORM-3 H3-01 MEDIUM）: トークンは解析〜検査の
    間だけ予約（claim）し、失敗は解除（release）して再送可能にする。添付呼び出し
    に進む時点で使用済み（consume）に確定し、添付結果に関わらず戻さない。"""

    def _photo_ok(self, tok):
        return self.post_photos(tok, [("photo", "a.jpg", JPEG)])

    def test_1_retry_after_413_is_accepted_once(self):
        tok, rid = self.submit()
        big = b"\xff\xd8\xff" + b"x" * sf.PHOTO_MAX_PART_BYTES
        self.assertEqual(self.post_photos(tok, [("photo", "big.jpg", big)])
                         .status_code, 413)
        self.assertIn(tok, sf._upload_tokens)                    # 未使用へ戻る
        self.assertFalse(sf._upload_tokens[tok][3])
        self.assertEqual(self._photo_ok(tok).status_code, 200)
        self.assertEqual(self.fake.photos(rid), ["key1"])
        self.assertEqual(len(self.fake.uploads), 1)

    def test_2_retry_after_400_is_accepted_once(self):
        tok, rid = self.submit()
        self.assertEqual(self.post_photos(tok, [("photo", "x.exe", UNKNOWN)])
                         .status_code, 400)                      # 形式不正
        self.assertEqual(self.post_photos(tok, [("creditor", None, b"x")])
                         .status_code, 400)                      # 許可外項目名
        r = self.client.post(f"{sf.PHOTO_ROUTE}/{tok}", content=b"--garbage",
                             headers={"Content-Type":
                                      "multipart/form-data; boundary=zzz"})
        self.assertEqual(r.status_code, 400)                     # 形式不正 multipart
        self.assertEqual(self.post_photos(tok, [("photo", "", b"")])
                         .status_code, 400)                      # 未選択
        self.assertIn(tok, sf._upload_tokens)
        self.assertEqual(self._photo_ok(tok).status_code, 200)
        self.assertEqual(self.fake.photos(rid), ["key1"])
        self.assertEqual(len(self.fake.uploads), 1)

    def test_3_resend_after_success_is_404_and_attach_once(self):
        tok, rid = self.submit()
        self.assertEqual(self._photo_ok(tok).status_code, 200)
        self.assertNotIn(tok, sf._upload_tokens)
        with _NoParser():
            self.assertEqual(self._photo_ok(tok).status_code, 404)
        self.assertEqual(self.fake.photos(rid), ["key1"])
        self.assertEqual(len(self.fake.uploads), 1)

    def test_4_concurrent_two_posts_same_token_attach_once(self):
        from starlette.requests import Request
        tok, rid = self.submit()
        ct, body = _multipart([("photo", "a.jpg", JPEG)])

        def _request():
            sent = {"done": False}

            async def receive():
                if sent["done"]:
                    return {"type": "http.request", "body": b"", "more_body": False}
                sent["done"] = True
                await asyncio.sleep(0.01)                        # 解析中に他方が到達
                return {"type": "http.request", "body": body, "more_body": False}
            scope = {"type": "http", "method": "POST", "http_version": "1.1",
                     "path": f"{sf.PHOTO_ROUTE}/{tok}", "query_string": b"",
                     "headers": [(b"content-type", ct.encode()),
                                 (b"content-length", str(len(body)).encode())],
                     "client": ("10.0.0.1", 1234), "server": ("test", 80)}
            return Request(scope, receive)

        async def scenario():
            return await asyncio.gather(
                sf.shindan_photos_entry(_request(), tok),
                sf.shindan_photos_entry(_request(), tok))
        r1, r2 = _run(scenario())
        self.assertEqual(sorted([r1.status_code, r2.status_code]), [200, 404])
        self.assertEqual(self.fake.photos(rid), ["key1"])
        self.assertEqual(len(self.fake.uploads), 1)
        self.assertNotIn(tok, sf._upload_tokens)

    def test_5_expired_token_404_without_parse(self):
        tok, _rid = self.submit()
        issued, rid, num, claimed = sf._upload_tokens[tok]
        sf._upload_tokens[tok] = (issued - sf.UPLOAD_TOKEN_TTL_SECONDS - 1,
                                  rid, num, claimed)
        with _NoParser():
            self.assertEqual(self._photo_ok(tok).status_code, 404)
        self.assertNotIn(tok, sf._upload_tokens)
        self.assertEqual(self.fake.uploads, {})

    def test_6_unexpected_exception_releases_claim(self):
        tok, rid = self.submit()

        async def _boom(*_a, **_k):
            raise RuntimeError("unexpected")
        with patch.object(sf, "_parse_multipart_stream", _boom):
            with self.assertRaises(RuntimeError):
                self._photo_ok(tok)
        self.assertIn(tok, sf._upload_tokens)
        self.assertFalse(sf._upload_tokens[tok][3])              # 予約解除済み
        self.assertEqual(self._photo_ok(tok).status_code, 200)   # 再送可能
        self.assertEqual(self.fake.photos(rid), ["key1"])
        # 形式判定中の例外でも同様に解除される
        tok2, rid2 = self.submit()
        with patch.object(ims, "detect_format",
                          side_effect=RuntimeError("unexpected")):
            with self.assertRaises(RuntimeError):
                self._photo_ok(tok2)
        self.assertIn(tok2, sf._upload_tokens)
        self.assertFalse(sf._upload_tokens[tok2][3])
        self.assertEqual(self._photo_ok(tok2).status_code, 200)

    def test_7_attach_failure_keeps_consumed_no_retry(self):
        # unconverged
        tok, rid = self.submit()
        self.fake.conflicts_left = ims.ATTACH_RETRIES + 1
        self.assertEqual(self._photo_ok(tok).status_code, 500)
        self.assertNotIn(tok, sf._upload_tokens)
        with _NoParser():
            self.assertEqual(self._photo_ok(tok).status_code, 404)
        self.assertEqual(self.fake.photos(rid), [])
        self.admin.assert_awaited_once()
        # attach_files の例外（予期しない失敗）でも consumed のまま
        tok2, rid2 = self.submit()
        with patch.object(ims, "attach_files",
                          AsyncMock(side_effect=RuntimeError("boom"))):
            with self.assertRaises(RuntimeError):
                self._photo_ok(tok2)
        self.assertNotIn(tok2, sf._upload_tokens)
        with _NoParser():
            self.assertEqual(self._photo_ok(tok2).status_code, 404)
        self.assertEqual(self.fake.photos(rid2), [])

    def test_8_gate_order_unchanged_claim_after_rate(self):
        # レート超過（429）は claim 前=トークンは未使用のまま・解析不到達
        toks = [self.submit()[0] for _ in range(sf._PHOTO_RATE_LIMIT + 1)]
        for tok in toks[:-1]:
            self.assertEqual(self._photo_ok(tok).status_code, 200)
        with _NoParser():
            self.assertEqual(self._photo_ok(toks[-1]).status_code, 429)
        self.assertIn(toks[-1], sf._upload_tokens)
        self.assertFalse(sf._upload_tokens[toks[-1]][3])
        # 凍結文言・上限 pin は無変更（本クラスは fix1 で文言に触れない）
        self.assertEqual(_sha(sf.PHOTO_PROMPT_TEXT),
                         TestPhotoFrozenTextsAndLimits.PROMPT_SHA256)
        self.assertEqual(_sha(sf.PHOTO_DONE_TEXT),
                         TestPhotoFrozenTextsAndLimits.DONE_SHA256)
        self.assertEqual(sf.PHOTO_MAX_PARTS, 5)


if __name__ == "__main__":
    unittest.main()
