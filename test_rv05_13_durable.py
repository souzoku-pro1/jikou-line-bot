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


def _line_body_multi(events, user="Uabc"):
    """events: [(event_id, text), ...] を1バッチに束ねた webhook body＋署名。"""
    body = json.dumps({"events": [{
        "type": "message", "webhookEventId": eid,
        "replyToken": "rt-" + eid, "source": {"userId": user},
        "message": {"type": "text", "text": txt}} for eid, txt in events]}).encode()
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

    def test_hnew01_partial_insert_failure_reattempts_once(self):
        # H-NEW-01 ①: event A 成功＋event B insert 失敗→503→再配送で
        # A が「一回だけ」処理される（永久滞留なし・二重返信なし）。
        import hub.durable_inbound as _di
        real_rle = _di.record_line_event
        fail_b = {"on": True}

        async def _rle_side(**kw):
            if kw["webhook_event_id"] == "evB" and fail_b["on"]:
                raise RuntimeError("event store down for B")   # 部分 insert 失敗
            return await real_rle(**kw)

        proc = AsyncMock(return_value=None)
        body, sig = _line_body_multi([("evA", "msgA"), ("evB", "msgB")])
        with patch.dict(os.environ, {_FLAG: "1"}), \
             patch.object(main, "_process_line_event", new=proc), \
             patch.object(_di, "record_line_event", new=_rle_side):
            r1 = _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
            self.assertGreaterEqual(r1.status_code, 500)   # B 失敗で 503（部分失敗）
            fail_b["on"] = False
            r2 = _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
            self.assertEqual(r2.status_code, 200)          # 再配送は成功
        a = [c for c in proc.call_args_list if c.args[2] == "msgA"]
        b = [c for c in proc.call_args_list if c.args[2] == "msgB"]
        self.assertEqual(len(a), 1)   # A: 滞留せず・二重返信もなく一回だけ処理
        self.assertEqual(len(b), 1)   # B: 再配送で一回処理

    def test_hnew01_terminal_duplicate_skips(self):
        # H-NEW-01 ②: terminal（done）到達済みの重複配送は登録 skip（H-03 回帰維持）
        body, sig = _line_body(event_id="evT", text="msgT")
        proc = AsyncMock(return_value=None)
        with patch.dict(os.environ, {_FLAG: "1"}), \
             patch.object(main, "_process_line_event", new=proc):
            _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})  # →done
            rows = self._inbound_rows()
            self.assertEqual(rows[0][1], "done")           # 1回目で terminal
            _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})  # done→skip
        self.assertEqual(proc.await_count, 1)              # terminal は再登録しない

    def test_hnew01r_reattempt_is_exclusive_claim(self):
        # H-NEW-01-R / M-03: 未終端の重複を re-attempt する際は排他 claim。
        # 2 配送が両方 guard 到達（received）しても "reattempt" は1者のみ
        # （旧コード=state→received では両方 reattempt になり登録2回で FAIL）。
        import hub.durable_inbound as _di
        kw = dict(user_id="U", signature_result="verified",
                  payload=b"p", event_type="message")

        async def _scenario():
            o0 = await _di.record_line_event(webhook_event_id="excl", **kw)  # new(received)
            # タスクは走らせない（滞留を模擬）→ 2 つの再配送が両方 received を見て claim 競合
            o1 = await _di.record_line_event(webhook_event_id="excl", **kw)
            o2 = await _di.record_line_event(webhook_event_id="excl", **kw)
            return o0, o1, o2

        with patch.dict(os.environ, {_FLAG: "1"}):
            o0, o1, o2 = _run(_scenario())
        db.reset_for_tests()
        self.assertEqual(o0, "new")
        self.assertEqual([o1, o2].count("reattempt"), 1)   # 排他: 登録は一回だけ
        self.assertEqual([o1, o2].count("duplicate"), 1)   # 敗者は skip

    def test_m02_attempts_exhaust_to_failed_terminal(self):
        # M-02: attempts 上限到達で failed_exhausted terminal（理由付き）へ。
        # 以後の重複再送は attempts 加算停止（terminal）。
        import hub.durable_inbound as _di
        kw = dict(user_id="U", signature_result="verified",
                  payload=b"p", event_type="message")

        async def _read():
            async with db.session_scope() as s:
                r = (await s.execute(sa.select(InboundEvent.state, InboundEvent.attempts)
                     .where(InboundEvent.external_event_id == "ex"))).one()
                return r.state, r.attempts

        async def _scenario():
            o1 = await _di.record_line_event(webhook_event_id="ex", **kw)   # new(received,1)
            o2 = await _di.record_line_event(webhook_event_id="ex", **kw)   # reattempt(processing,2)
            await _di.mark_line_failed("ex", "BoomError")                    # →failed(2)
            o3 = await _di.record_line_event(webhook_event_id="ex", **kw)   # 上限到達→failed_exhausted
            st3, at3 = await _read()
            o4 = await _di.record_line_event(webhook_event_id="ex", **kw)   # skip・加算停止
            st4, at4 = await _read()
            return o1, o2, o3, o4, st3, at3, st4, at4

        with patch.dict(os.environ, {_FLAG: "1", "INBOUND_LINE_MAX_ATTEMPTS": "2"}):
            o1, o2, o3, o4, st3, at3, st4, at4 = _run(_scenario())
        db.reset_for_tests()
        self.assertEqual([o1, o2, o3, o4], ["new", "reattempt", "duplicate", "duplicate"])
        self.assertEqual(st3, "failed_exhausted")          # 上限到達で terminal
        self.assertEqual(at3, 2)
        self.assertEqual(st4, "failed_exhausted")
        self.assertEqual(at4, 2)                           # duplicate 再送で加算停止

    def test_mtest01_fresh_processing_within_threshold_skips(self):
        # M-TEST-01 ①: claim 後（fresh processing）の即再配送は skip（stale 閾値内）
        import hub.durable_inbound as _di
        kw = dict(user_id="U", signature_result="verified",
                  payload=b"p", event_type="message")

        async def _scenario():
            o1 = await _di.record_line_event(webhook_event_id="fp", **kw)  # new(received)
            o2 = await _di.record_line_event(webhook_event_id="fp", **kw)  # claim→processing(fresh)
            o3 = await _di.record_line_event(webhook_event_id="fp", **kw)  # 即再配送→fresh→skip
            return o1, o2, o3

        with patch.dict(os.environ, {_FLAG: "1"}):
            o1, o2, o3 = _run(_scenario())
        db.reset_for_tests()
        self.assertEqual([o1, o2, o3], ["new", "reattempt", "duplicate"])

    def test_mtest01_stale_processing_reclaims_once(self):
        # M-TEST-01 ②: claimed_at を閾値超に細工→再配送で再 claim・登録は1回だけ
        import hub.durable_inbound as _di
        kw = dict(user_id="U", signature_result="verified",
                  payload=b"p", event_type="message")

        async def _make_stale():
            async with db.session_scope() as s:
                await s.execute(sa.update(InboundEvent)
                                .where(InboundEvent.external_event_id == "sp")
                                .values(claimed_at=sa.func.datetime(sa.func.now(),
                                                                    "-7200 seconds")))

        async def _scenario():
            o1 = await _di.record_line_event(webhook_event_id="sp", **kw)  # new
            o2 = await _di.record_line_event(webhook_event_id="sp", **kw)  # claim→processing
            await _make_stale()                                            # claimed_at 閾値超に細工
            o3 = await _di.record_line_event(webhook_event_id="sp", **kw)  # stale→再claim
            o4 = await _di.record_line_event(webhook_event_id="sp", **kw)  # 直後 fresh→skip
            return o1, o2, o3, o4

        with patch.dict(os.environ, {_FLAG: "1"}):
            o1, o2, o3, o4 = _run(_scenario())
        db.reset_for_tests()
        self.assertEqual([o1, o2], ["new", "reattempt"])
        self.assertEqual(o3, "reattempt")   # stale processing を再 claim（回収・登録）
        self.assertEqual(o4, "duplicate")   # 再 claim 直後は fresh → 登録は1回だけ

    def test_hnew02_fresh_processing_not_reclaimed_on_production_path(self):
        # H-NEW-02: 本番経路（新規 insert→mark_line_processing→重複配送）で、処理中
        # （fresh processing）への重複配送が skip され併走しないこと。
        # 旧コード（mark_line_processing が claimed_at を書かない）では NULL stale 救済に
        # 拾われて "reattempt"＝再 claim（併走）になり FAIL する形。
        import hub.durable_inbound as _di
        body, sig = _line_body(event_id="fresh-prod")
        mid = {}

        async def _proc(reply_token, user_id, user_text):
            # 背景処理の実行中（mark_line_processing 通過後）に重複配送が到着した状況
            mid["outcome"] = await _di.record_line_event(
                webhook_event_id="fresh-prod", user_id="Uabc",
                signature_result="verified", payload=b"p", event_type="message")

        with patch.dict(os.environ, {_FLAG: "1"}), \
             patch.object(main, "_process_line_event", new=_proc):
            r = _client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(mid["outcome"], "duplicate")   # fresh processing → skip（併走なし）

        async def _read():
            async with db.session_scope() as s:
                row = (await s.execute(
                    sa.select(InboundEvent.state, InboundEvent.attempts, InboundEvent.claimed_at)
                    .where(InboundEvent.external_event_id == "fresh-prod"))).one()
                return row.state, row.attempts, row.claimed_at
        st, at, ca = _run(_read()); db.reset_for_tests()
        self.assertEqual(st, "done")                    # 本処理は完走
        self.assertEqual(at, 1)                         # skip は加算しない（M-02-R 維持）
        self.assertIsNotNone(ca)                        # mark_line_processing が claimed_at を設定

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

    def test_mnew02_fence_lost_vendor_pre_no_side_effects(self):
        # M-NEW-02: mark_phase=None（vendor_pre で fence 喪失）→ OCR/ask/forward 非実行・非200
        ocr = MagicMock(return_value="x")
        log_ask = AsyncMock(return_value="url")
        forward = AsyncMock(return_value=None)
        with patch("sortation_ingest._ocr_pdf", new=ocr), \
             patch("sortation_ingest.list_candidates", new=AsyncMock(return_value=[])), \
             patch("sortation_ingest._judge_with_claude",
                   new=AsyncMock(return_value={"doc_type": "その他", "confidence": 0.1, "reason": "r"})), \
             patch("sortation_ingest._log_ask", new=log_ask), \
             patch("sortation_ingest._forward_to_line", new=forward), \
             patch("hub.ingestion_receipt.mark_phase", new=AsyncMock(return_value=None)), \
             patch.dict(os.environ, {**_ENV, _FLAG: "1"}):
            r = _client.post("/sortation/ingest?token=sort-token",
                             files={"file": ("x.pdf", b"%PDF fence", "application/pdf")},
                             data={"drive_file_id": "FV"})
        self.assertEqual(r.status_code, 409)                 # 非200（中断）
        ocr.assert_not_called()                              # OCR 非実行
        log_ask.assert_not_called()                          # ask 非実行
        forward.assert_not_called()                          # forward 非実行

    def test_mnew02_fence_lost_terminal_non_200(self):
        # M-NEW-02: mark_terminal=False（commit で fence 喪失）→ 非200（成功 ACK にしない）
        with patch("sortation_ingest._ocr_pdf", new=MagicMock(return_value="x")), \
             patch("sortation_ingest.list_candidates", new=AsyncMock(return_value=[])), \
             patch("sortation_ingest._judge_with_claude",
                   new=AsyncMock(return_value={"doc_type": "その他", "confidence": 0.1, "reason": "r"})), \
             patch("sortation_ingest._log_ask", new=AsyncMock(return_value="url")), \
             patch("sortation_ingest._notify_ask", new=AsyncMock(return_value=None)), \
             patch("hub.ingestion_receipt.mark_terminal", new=AsyncMock(return_value=False)), \
             patch.dict(os.environ, {**_ENV, _FLAG: "1"}):
            r = _client.post("/sortation/ingest?token=sort-token",
                             files={"file": ("x.pdf", b"%PDF term", "application/pdf")},
                             data={"drive_file_id": "FT"})
        self.assertEqual(r.status_code, 409)                 # 成功 ACK にしない
        self.assertNotIn(ir.ST_COMPLETED, self._receipt_states())

    def test_mnew01r_page_cap_forces_ask_no_ocr(self):
        # M-01-R: Vision ページ上限超過 → OCR せず安全側で ask 縮退（沈黙処理にしない）
        # L-TEST-01: split 解析（split flag ON でも）・Claude 判定の未呼出も明示 assert
        ocr = MagicMock(return_value="should-not-run")
        split = AsyncMock(return_value=(None, None))
        judge = AsyncMock(return_value={"doc_type": "その他", "confidence": 0.9, "reason": "r"})
        with patch("main._pdf_page_count", new=MagicMock(return_value=999)), \
             patch("sortation_ingest._ocr_pdf", new=ocr), \
             patch("sortation_ingest._try_split_analysis", new=split), \
             patch("sortation_ingest.list_candidates", new=AsyncMock(return_value=[])), \
             patch("sortation_ingest._judge_with_claude", new=judge), \
             patch("sortation_ingest._log_ask", new=AsyncMock(return_value="url")), \
             patch("sortation_ingest._notify_ask", new=AsyncMock(return_value=None)), \
             patch.dict(os.environ, {**_ENV, "SORTATION_SPLIT_ENABLED": "1"}):
            r = _client.post("/sortation/ingest?token=sort-token",
                             files={"file": ("x.pdf", b"%PDF big", "application/pdf")},
                             data={"drive_file_id": "FBIG"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json().get("action"), "ask")   # 安全側 ask へ縮退
        ocr.assert_not_called()                           # 上限超過は OCR を回さない
        split.assert_not_awaited()                        # L-TEST-01: split 解析も回さない
        judge.assert_not_awaited()                        # L-TEST-01: Claude 判定も呼ばない

    def test_mnew03_claim_unavailable_response_contract(self):
        # M-NEW-03: claim 不可→state 別応答が §H-06 状態表内に収まる契約テスト
        async def _set_state(rid, st):
            async with db.session_scope() as s:
                await s.execute(sa.update(ir.ingestion_receipt)
                                .where(ir.ingestion_receipt.c.id == rid)
                                .values(last_outcome=st))
        # (state, expected_status, expect_action_skip, claim_succeeds)
        cases = [
            (ir.ST_PROCESSING, 409, False, False),          # 別 request 実行中 → 409
            (ir.ST_VENDOR_PRE, 409, False, False),
            (ir.ST_SENDING, 409, False, False),
            (ir.ST_DUPLICATE_SUSPECT, 409, False, False),
            (ir.ST_COMPLETED, 200, True, False),            # terminal → 200 skip
            (ir.ST_FAILED, 200, True, False),
            (ir.ST_UNKNOWN, 200, True, False),
            (ir.ST_PENDING_RETRY, 200, False, True),        # claim 可 → 再処理（状態表内）
        ]
        for i, (st, code, skip, claimed) in enumerate(cases):
            with self.subTest(state=st):
                fid = f"FC{i}"
                pdf = f"%PDF contract {i}".encode()

                async def _pre():
                    rid = await ir.upsert_receipt(
                        ingest_type="sortation", caller_id="gas", source_file_id=fid,
                        source_sha256=hashlib.sha256(pdf).hexdigest(), case_hint=None)
                    await _set_state(rid, st)
                _run(_pre()); db.reset_for_tests()
                ocr = MagicMock(return_value="x")
                with patch("sortation_ingest._ocr_pdf", new=ocr), \
                     patch("sortation_ingest.list_candidates", new=AsyncMock(return_value=[])), \
                     patch("sortation_ingest._judge_with_claude",
                           new=AsyncMock(return_value={"doc_type": "その他", "confidence": 0.1, "reason": "r"})), \
                     patch("sortation_ingest._log_ask", new=AsyncMock(return_value="url")), \
                     patch("sortation_ingest._notify_ask", new=AsyncMock(return_value=None)), \
                     patch.dict(os.environ, {**_ENV, _FLAG: "1"}):
                    r = _client.post("/sortation/ingest?token=sort-token",
                                     files={"file": (f"{fid}.pdf", pdf, "application/pdf")},
                                     data={"drive_file_id": fid})
                self.assertEqual(r.status_code, code, f"{st}: {r.text}")
                if skip:
                    self.assertEqual(r.json().get("action"), "skip")
                if not claimed:
                    ocr.assert_not_called()                  # claim 不可→外部作用に入らない


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
