"""hub/kintone.py の単体テスト（T0-1）

検証項目（docs/architecture/09 T0-1 完了条件）:
  - fields のフラット dict → {"value": ...} 包み
  - GET のみ 1 回リトライ（5xx / 通信エラー）・書き込みはリトライしない
  - update_record の revision 楽観ロック → KintoneConflict
  - create_records の 100 件チャンク分割
  - KintoneError への正規化（status / code / message）
"""

import json
import unittest
from unittest.mock import patch

import httpx

from hub import kintone
from hub.kintone import KintoneApp, KintoneConflict, KintoneError

APP = KintoneApp("テスト", "TEST_APP_ID", "TEST_TOKEN")

_ENV = {
    "KINTONE_SUBDOMAIN": "testsub",
    "TEST_APP_ID": "99",
    "TEST_TOKEN": "tok",
}


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, content=b""):
        self.status_code = status_code
        self._json = json_data
        self.content = content
        self.text = json.dumps(json_data or {}, ensure_ascii=False)

    @property
    def is_success(self):
        return 200 <= self.status_code < 300

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json


class FakeClient:
    """httpx.AsyncClient の差し替え。呼び出しを記録し、queue から応答を返す"""

    queue: list = []      # FakeResponse または Exception
    calls: list = []      # (method, url, kwargs)

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kw):
        return self._next("GET", url, kw)

    async def post(self, url, **kw):
        return self._next("POST", url, kw)

    async def request(self, method, url, **kw):
        return self._next(method, url, kw)

    def _next(self, method, url, kw):
        FakeClient.calls.append((method, url, kw))
        item = FakeClient.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def use_fake(queue):
    FakeClient.queue = list(queue)
    FakeClient.calls = []
    return patch("hub.kintone.httpx.AsyncClient", FakeClient)


class TestKintoneClient(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._env = patch.dict("os.environ", _ENV, clear=False)
        self._env.start()
        self.addCleanup(self._env.stop)

    # ── 基本動作 ────────────────────────────────────────────────

    async def test_create_record_wraps_values(self):
        with use_fake([FakeResponse(200, {"id": "5"})]):
            rid = await kintone.create_record(APP, {"氏名": "太郎", "住所": "川口市"})
        self.assertEqual(rid, "5")
        method, url, kw = FakeClient.calls[0]
        self.assertEqual(method, "POST")
        self.assertIn("https://testsub.cybozu.com/k/v1/record.json", url)
        self.assertEqual(kw["json"]["app"], "99")
        self.assertEqual(kw["json"]["record"], {"氏名": {"value": "太郎"},
                                                "住所": {"value": "川口市"}})
        self.assertEqual(kw["headers"]["X-Cybozu-API-Token"], "tok")

    async def test_get_record(self):
        record = {"status": {"value": "受任"}}
        with use_fake([FakeResponse(200, {"record": record})]):
            got = await kintone.get_record(APP, "12")
        self.assertEqual(got, record)
        _, _, kw = FakeClient.calls[0]
        self.assertEqual(kw["params"], {"app": "99", "id": "12"})

    async def test_search_records_with_fields(self):
        with use_fake([FakeResponse(200, {"records": [{"$id": {"value": "1"}}]})]):
            recs = await kintone.search_records(APP, 'x = "1"', fields=["$id", "氏名"])
        self.assertEqual(len(recs), 1)
        _, _, kw = FakeClient.calls[0]
        self.assertEqual(kw["params"]["fields[0]"], "$id")
        self.assertEqual(kw["params"]["fields[1]"], "氏名")

    async def test_base_url_strips_cybozu_suffix(self):
        with patch.dict("os.environ", {**_ENV, "KINTONE_SUBDOMAIN": "foo.cybozu.com"}):
            with use_fake([FakeResponse(200, {"record": {}})]):
                await kintone.get_record(APP, "1")
        _, url, _ = FakeClient.calls[0]
        self.assertTrue(url.startswith("https://foo.cybozu.com/"))

    # ── リトライ方針: GET のみ1回 ────────────────────────────────

    async def test_get_retries_once_on_5xx(self):
        with use_fake([FakeResponse(500, {"code": "X", "message": "boom"}),
                       FakeResponse(200, {"record": {"a": 1}})]):
            got = await kintone.get_record(APP, "1")
        self.assertEqual(got, {"a": 1})
        self.assertEqual(len(FakeClient.calls), 2)

    async def test_get_retries_once_on_transport_error(self):
        with use_fake([httpx.ConnectError("down"),
                       FakeResponse(200, {"record": {"a": 1}})]):
            got = await kintone.get_record(APP, "1")
        self.assertEqual(got, {"a": 1})
        self.assertEqual(len(FakeClient.calls), 2)

    async def test_get_gives_up_after_retry(self):
        with use_fake([FakeResponse(500), FakeResponse(500)]):
            with self.assertRaises(KintoneError):
                await kintone.get_record(APP, "1")
        self.assertEqual(len(FakeClient.calls), 2)

    async def test_get_does_not_retry_on_4xx(self):
        with use_fake([FakeResponse(404, {"code": "GAIA_RE01", "message": "not found"})]):
            with self.assertRaises(KintoneError) as ctx:
                await kintone.get_record(APP, "1")
        self.assertEqual(len(FakeClient.calls), 1)
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.code, "GAIA_RE01")

    async def test_write_does_not_retry_on_5xx(self):
        """書き込みはリトライしない（二重書き込み防止・設計 03 §3）"""
        with use_fake([FakeResponse(500, {"message": "boom"})]):
            with self.assertRaises(KintoneError):
                await kintone.update_record(APP, "1", {"x": "y"})
        self.assertEqual(len(FakeClient.calls), 1)

    async def test_write_does_not_retry_on_transport_error(self):
        with use_fake([httpx.ConnectError("down")]):
            with self.assertRaises(KintoneError) as ctx:
                await kintone.create_record(APP, {"x": "y"})
        self.assertEqual(len(FakeClient.calls), 1)
        self.assertEqual(ctx.exception.code, "transport_error")

    # ── revision 楽観ロック ──────────────────────────────────────

    async def test_update_record_sends_revision(self):
        with use_fake([FakeResponse(200, {})]):
            await kintone.update_record(APP, "1", {"送信済み": "yes"}, revision="3")
        _, _, kw = FakeClient.calls[0]
        self.assertEqual(kw["json"]["revision"], "3")
        self.assertEqual(kw["json"]["record"], {"送信済み": {"value": "yes"}})

    async def test_update_record_without_revision_omits_key(self):
        with use_fake([FakeResponse(200, {})]):
            await kintone.update_record(APP, "1", {"x": "y"})
        _, _, kw = FakeClient.calls[0]
        self.assertNotIn("revision", kw["json"])

    async def test_revision_conflict_raises_kintone_conflict(self):
        with use_fake([FakeResponse(409, {"code": "GAIA_CO02", "message": "conflict"})]):
            with self.assertRaises(KintoneConflict):
                await kintone.update_record(APP, "1", {"x": "y"}, revision="2")
        self.assertEqual(len(FakeClient.calls), 1)  # 競合もリトライしない

    # ── 一括登録のチャンク分割 ───────────────────────────────────

    async def test_create_records_chunks_by_100(self):
        responses = [
            FakeResponse(200, {"ids": [str(i) for i in range(100)]}),
            FakeResponse(200, {"ids": [str(i) for i in range(100, 200)]}),
            FakeResponse(200, {"ids": [str(i) for i in range(200, 250)]}),
        ]
        with use_fake(responses):
            ids = await kintone.create_records(APP, [{"n": str(i)} for i in range(250)])
        self.assertEqual(len(ids), 250)
        self.assertEqual(len(FakeClient.calls), 3)
        self.assertEqual(len(FakeClient.calls[0][2]["json"]["records"]), 100)
        self.assertEqual(len(FakeClient.calls[2][2]["json"]["records"]), 50)

    # ── ファイル・フォーム設計 ───────────────────────────────────

    async def test_upload_file_returns_filekey(self):
        with use_fake([FakeResponse(200, {"fileKey": "fk1"})]):
            fk = await kintone.upload_file(APP, "a.docx", b"data", "application/x")
        self.assertEqual(fk, "fk1")
        _, url, kw = FakeClient.calls[0]
        self.assertIn("/k/v1/file.json", url)
        self.assertEqual(kw["files"]["file"], ("a.docx", b"data", "application/x"))

    async def test_download_file_returns_bytes(self):
        with use_fake([FakeResponse(200, {}, content=b"PDFDATA")]):
            data = await kintone.download_file(APP, "fk1")
        self.assertEqual(data, b"PDFDATA")

    async def test_get_form_fields(self):
        props = {"status": {"type": "DROP_DOWN"}}
        with use_fake([FakeResponse(200, {"properties": props})]):
            got = await kintone.get_form_fields(APP)
        self.assertEqual(got, props)


if __name__ == "__main__":
    unittest.main()
