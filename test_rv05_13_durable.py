"""RV-05-13 統合テスト: LINE Phase A 記録+観測・sortation 同期台帳・flag OFF 無変更・handler smoke。

DRAFT §8。DB は sqlite（file）。flag OFF は現行挙動と byte 同一（durable コードに入らない）。
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
from unittest.mock import AsyncMock, MagicMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "t", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "SORTATION_INGEST_TOKEN": "sort-token",
}
_SAVED = {k: os.environ.get(k) for k in _ENV}
os.environ.update(_ENV)

import sqlalchemy as sa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import hub.db as db  # noqa: E402
from hub import ingestion_receipt as ir  # noqa: E402
from hub.inbound_event import Base as InboundBase, InboundEvent  # noqa: E402
from hub import durable_inbound as di  # noqa: E402
import main  # noqa: E402

for _k, _o in _SAVED.items():
    if _o is None:
        os.environ.pop(_k, None)
    else:
        os.environ[_k] = _o

_client = TestClient(main.app)
_FLAG = "INBOUND_EVENT_DURABLE_ENABLED"


def _run(c):
    return asyncio.run(c)


def _line_body(text="こんにちは", event_id="wev-1", user="Uabc"):
    body = json.dumps({"events": [{
        "type": "message", "webhookEventId": event_id,
        "replyToken": "rt", "source": {"userId": user},
        "message": {"type": "text", "text": text}}]}).encode()
    sig = base64.b64encode(hmac.new(b"dummy_secret", body, hashlib.sha256).digest()).decode()
    return body, sig


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="rv0513_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db",
                                            **{k: v for k, v in _ENV.items()}})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(ir.metadata.create_all)
                await c.run_sync(InboundBase.metadata.create_all)
        _run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _inbound_rows(self):
        async def _q():
            async with db.session_scope() as s:
                rows = (await s.execute(sa.select(InboundEvent))).scalars().all()
                return [(r.provider, r.state, r.external_event_id) for r in rows]
        r = _run(_q()); db.reset_for_tests(); return r

    def _receipt_states(self):
        async def _q():
            async with db.session_scope() as s:
                return (await s.execute(sa.select(ir.ingestion_receipt.c.last_outcome))).scalars().all()
        r = _run(_q()); db.reset_for_tests(); return r


# ── LINE Phase A（flag ON: 記録+観測 / flag OFF: 現行同一） ──────────────────
class TestLineDurable(_DbMixin):
    def test_flag_on_records_and_completes(self):
        body, sig = _line_body()
        with patch.dict(os.environ, {_FLAG: "1"}), \
             patch.object(main, "_process_line_event", new=AsyncMock(return_value=None)):
            r = _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
        self.assertEqual(r.status_code, 200)
        rows = self._inbound_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "line")               # provider
        self.assertEqual(rows[0][2], "wev-1")              # webhookEventId
        self.assertEqual(rows[0][1], "done")               # processing→done（coarse completed）

    def test_flag_on_duplicate_delivery_idempotent(self):
        body, sig = _line_body(event_id="dup-1")
        with patch.dict(os.environ, {_FLAG: "1"}), \
             patch.object(main, "_process_line_event", new=AsyncMock(return_value=None)):
            _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
            _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
        self.assertEqual(len(self._inbound_rows()), 1)     # UNIQUE(dedup_key) 冪等

    def test_h03_duplicate_delivery_no_double_reply(self):
        # H-03: 重複配送は BackgroundTasks を登録しない（二重返信の遮断）
        body, sig = _line_body(event_id="h03-dup")
        proc = AsyncMock(return_value=None)
        with patch.dict(os.environ, {_FLAG: "1"}), \
             patch.object(main, "_process_line_event", new=proc):
            _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
            _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
        self.assertEqual(proc.await_count, 1)   # 2回配送でも処理は1回だけ

    def test_flag_on_background_crash_marks_failed(self):
        # HOTFIX-01 型: 背景タスクが（内部 try の外で）crash → failed で可視化
        body, sig = _line_body(event_id="crash-1")
        boom = AsyncMock(side_effect=RuntimeError("boom"))
        with patch.dict(os.environ, {_FLAG: "1"}), \
             patch.object(main, "_process_line_event", new=boom):
            _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
        rows = self._inbound_rows()
        self.assertEqual(rows[0][1], "failed")

    def test_flag_off_no_durable_record(self):
        body, sig = _line_body(event_id="off-1")
        os.environ.pop(_FLAG, None)
        with patch.object(main, "_process_line_event", new=AsyncMock(return_value=None)):
            r = _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self._inbound_rows()), 0)     # flag OFF: durable 記録なし


# ── sortation 同期台帳（flag ON: lifecycle 記録・claim fencing） ─────────────
class TestSortationDurable(_DbMixin):
    def _post(self, env_extra=None, fid="F1"):
        ocr = MagicMock(return_value="調査結果通知書のOCR")
        judge = AsyncMock(return_value={"doc_type": "その他", "confidence": 0.1, "reason": "r"})
        files = {"file": ("x.pdf", b"%PDF fake sortation", "application/pdf")}
        data = {"drive_file_id": fid}
        with patch("sortation_ingest._ocr_pdf", new=ocr), \
             patch("sortation_ingest.list_candidates", new=AsyncMock(return_value=[])), \
             patch("sortation_ingest._judge_with_claude", new=judge), \
             patch("sortation_ingest._log_ask", new=AsyncMock(return_value="url")), \
             patch("sortation_ingest._notify_ask", new=AsyncMock(return_value=None)), \
             patch.dict(os.environ, {**_ENV, **(env_extra or {})}):
            return _client.post("/sortation/ingest?token=sort-token", files=files, data=data)

    def test_flag_on_records_receipt_completed(self):
        r = self._post(env_extra={_FLAG: "1"})
        self.assertEqual(r.status_code, 200, r.text)
        states = self._receipt_states()
        self.assertEqual(states, [ir.ST_COMPLETED])        # received→…→completed

    def test_flag_off_no_receipt(self):
        env = dict(_ENV); env.pop(_FLAG, None)
        r = self._post(env_extra={})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(len(self._receipt_states()), 0)   # flag OFF: 台帳に入らない

    def test_h02_completed_duplicate_skips_processing(self):
        # H-02: 既に completed の receipt に再送 → claim 不可 → **同期処理全体 skip**（200 skip）
        pdf = b"%PDF fake sortation"
        sha = hashlib.sha256(pdf).hexdigest()

        async def _pre():
            rid = await ir.upsert_receipt(ingest_type="sortation", caller_id="gas",
                                          source_file_id="F1", source_sha256=sha, case_hint=None)
            ep = await ir.claim(rid)
            await ir.mark_terminal(rid, ep, ir.ST_COMPLETED)
        _run(_pre()); db.reset_for_tests()
        ocr = MagicMock(return_value="x")   # 呼ばれてはいけない
        with patch("sortation_ingest._ocr_pdf", new=ocr), \
             patch.dict(os.environ, {**_ENV, _FLAG: "1"}):
            r = _client.post("/sortation/ingest?token=sort-token",
                             files={"file": ("x.pdf", pdf, "application/pdf")},
                             data={"drive_file_id": "F1"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json().get("action"), "skip")
        ocr.assert_not_called()   # OCR/Claude/ask を実行しない（H-02）

    def test_h04_ask_save_failure_pending_retry_5xx(self):
        # H-04: ask 保存失敗 → PENDING_RETRY → 5xx（成功 ACK にしない）
        ocr = MagicMock(return_value="調査結果通知書")
        judge = AsyncMock(return_value={"doc_type": "その他", "confidence": 0.1, "reason": "r"})
        nr_client = TestClient(main.app, raise_server_exceptions=False)
        with patch("sortation_ingest._ocr_pdf", new=ocr), \
             patch("sortation_ingest.list_candidates", new=AsyncMock(return_value=[])), \
             patch("sortation_ingest._judge_with_claude", new=judge), \
             patch("sortation_ingest._log_ask", new=AsyncMock(side_effect=RuntimeError("kintone down"))), \
             patch("sortation_ingest._notify_ask", new=AsyncMock(return_value=None)), \
             patch.dict(os.environ, {**_ENV, _FLAG: "1"}):
            r = nr_client.post("/sortation/ingest?token=sort-token",
                               files={"file": ("x.pdf", b"%PDF ask", "application/pdf")},
                               data={"drive_file_id": "FA"})
        self.assertGreaterEqual(r.status_code, 500)          # 5xx（成功 ACK にしない）
        self.assertEqual(self._receipt_states(), [ir.ST_PENDING_RETRY])


# ── M-01: flag OFF 機械的担保（durable 呼出は flag 判定の内側のみ） ──────────
class TestFlagOffMechanical(unittest.TestCase):
    def test_durable_calls_guarded_by_flag(self):
        import pathlib
        repo = pathlib.Path(__file__).parent
        main_src = (repo / "main.py").read_text(encoding="utf-8")
        sort_src = (repo / "sortation_ingest.py").read_text(encoding="utf-8")
        # M-06: durable_inbound を module top-level（非インデント）で import しない
        # （flag OFF は import 経路に入らない。flag ON 内の関数ローカル import は可）。
        for src, name in [(main_src, "main.py"), (sort_src, "sortation_ingest.py")]:
            for line in src.splitlines():
                if (line.startswith("import ") or line.startswith("from ")) \
                        and "hub.durable_inbound" in line:
                    self.fail(f"{name}: durable_inbound を top-level import している: {line}")
        # webhook: env 直読みの flag ゲート＋record_line_event は if _durable: 内
        self.assertIn('os.environ.get("INBOUND_EVENT_DURABLE_ENABLED"', main_src)
        self.assertIn("if _durable:", main_src)
        self.assertLess(main_src.index("if _durable:"), main_src.index("record_line_event("))
        # sortation: _durable_enabled() ゲートの内側でのみ upsert_receipt/claim
        self.assertIn("if _durable_enabled():", sort_src)
        self.assertLess(sort_src.index("if _durable_enabled():"), sort_src.index("upsert_receipt"))

    def test_no_durable_write_when_flag_off(self):
        # 挙動: flag OFF は durable モジュールを呼んでも書かない（durable_enabled=False）
        os.environ.pop(_FLAG, None)
        self.assertFalse(di.durable_enabled())


# ── 顧客Bot handler smoke（RV-05-13 後も先頭ログ通過） ──────────────────────
class TestHandlerSmoke(unittest.TestCase):
    def test_process_line_event_head_smoke(self):
        sentinel = RuntimeError("STOP")
        mlog = MagicMock(); mlog.info.side_effect = sentinel
        with patch.object(main, "logger", mlog):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(main._process_line_event("rt", "Ux", "hi"))
        self.assertIs(ctx.exception, sentinel)


if __name__ == "__main__":
    unittest.main()
