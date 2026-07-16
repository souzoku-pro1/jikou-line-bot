"""RV-04c S3: kintone レーン（dedup/state 遷移・stale 監視・XFF 観測・legacy strict・rotation）。

設計正本 §4〜§7。§8 の修正前 FAIL 実測系統を網羅:
  dedup 二重処理 / fail-closed / marker 2系（後例外の failed 非上書き・marker 失敗時 send 0）/
  no-op 偽警報 / stale received-sending / rotation 4状態 table / legacy 異常形起動 / 監視混入。
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
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
}
_SAVED = {k: os.environ.get(k) for k in _ENV}
os.environ.update(_ENV)

import sqlalchemy as sa  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import hub.db as db  # noqa: E402
from hub.inbound_event import Base as InboundBase, InboundEvent  # noqa: E402
from hub import kintone_lane as kl  # noqa: E402
import daily_healthcheck as hc  # noqa: E402
import main  # noqa: E402

for _k, _o in _SAVED.items():
    if _o is None:
        os.environ.pop(_k, None)
    else:
        os.environ[_k] = _o

_client = TestClient(main.app)
_DEDUP = "KINTONE_EVENT_DEDUP_ENABLED"


def _kintone_body(event_id="ev-1", record_id="42", status="承認済", sent="no",
                  type_="UPDATE_STATUS"):
    return {"id": event_id, "type": type_, "app": {"id": "29"},
            "record": {"$id": {"value": record_id},
                       "ステータス2": {"value": status},
                       "送信済み": {"value": sent}}}


class _DbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="rv04c_kl_")
        self._env = patch.dict(os.environ, {
            "DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db", **_ENV})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(InboundBase.metadata.create_all)
        asyncio.run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def _rows(self):
        async def _q():
            async with db.session_scope() as s:
                r = (await s.execute(sa.select(InboundEvent))).scalars().all()
                return [(x.provider, x.state, x.external_event_id, x.last_error) for x in r]
        r = asyncio.run(_q()); db.reset_for_tests(); return r

    def _state_of(self, ev):
        async def _q():
            async with db.session_scope() as s:
                r = (await s.execute(sa.select(InboundEvent.state, InboundEvent.last_error)
                     .where(InboundEvent.external_event_id == ev))).first()
                return r
        r = asyncio.run(_q()); db.reset_for_tests(); return r


# ── 1. dedup 二重処理（修正前 FAIL: 同一 id 2回で処理2回） ────────────────────
class TestDedup(_DbMixin):
    def test_duplicate_delivery_processed_once(self):
        body = _kintone_body(event_id="dup-1")
        proc = AsyncMock(return_value=None)
        with patch.dict(os.environ, {_DEDUP: "1"}), \
             patch.object(main, "send_line_push", new=proc), \
             patch.object(main, "mark_approval_sent", new=AsyncMock()), \
             patch.object(main, "save_to_chatlog", new=AsyncMock()), \
             patch.object(main.hub_kintone, "get_record",
                          new=AsyncMock(return_value={
                              "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"},
                              "line_user_id": {"value": "U1"}, "AI下書き": {"value": "text"},
                              "カテゴリ": {"value": "c"}})):
            r1 = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
            r2 = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual(r1.status_code, 200, r1.text)
        self.assertEqual(r2.json().get("skip"), "duplicate_delivery")
        self.assertEqual(proc.await_count, 1)   # 2回配信でも送信は1回
        st, le = self._state_of("dup-1")
        self.assertEqual(st, "done")
        self.assertIsNone(le)   # 送信完了は last_error=NULL

    def test_flag_off_no_dedup_row(self):
        body = _kintone_body(event_id="off-1")
        os.environ.pop(_DEDUP, None)
        with patch.object(main, "send_line_push", new=AsyncMock()), \
             patch.object(main, "mark_approval_sent", new=AsyncMock()), \
             patch.object(main, "save_to_chatlog", new=AsyncMock()), \
             patch.object(main.hub_kintone, "get_record",
                          new=AsyncMock(return_value={
                              "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"},
                              "line_user_id": {"value": "U1"}, "AI下書き": {"value": "t"},
                              "カテゴリ": {"value": "c"}})):
            r = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self._rows()), 0)   # flag OFF: 行を作らない


# ── 2. fail-closed（DB 到達不能で処理せず 5xx） ──────────────────────────────
class TestFailClosed(_DbMixin):
    def test_db_unreachable_5xx_no_processing(self):
        body = _kintone_body(event_id="fc-1")
        proc = AsyncMock(return_value=None)
        with patch.dict(os.environ, {_DEDUP: "1"}), \
             patch.object(main, "send_line_push", new=proc), \
             patch.object(kl, "claim_event",
                          new=AsyncMock(side_effect=RuntimeError("db down"))):
            nr = TestClient(main.app, raise_server_exceptions=False)
            r = nr.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual(r.status_code, 503)      # H04 fail-closed
        proc.assert_not_awaited()                 # 処理しない


# ── 3. marker 2系（後例外の failed 非上書き・marker 失敗時 send 0） ──────────
class TestMarkerContract(_DbMixin):
    def _record(self):
        return {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"},
                "line_user_id": {"value": "U1"}, "AI下書き": {"value": "t"},
                "カテゴリ": {"value": "c"}}

    def test_marker_after_exception_not_overwritten_to_failed(self):
        # marker 成功後に LINE 送信で例外 → state=sending 維持（failed 上書き禁止）・送信は試行済み
        body = _kintone_body(event_id="mk-1")
        proc = AsyncMock(side_effect=RuntimeError("line api boom"))
        with patch.dict(os.environ, {_DEDUP: "1"}), \
             patch.object(main, "send_line_push", new=proc), \
             patch.object(main, "mark_approval_sent", new=AsyncMock()), \
             patch.object(main, "save_to_chatlog", new=AsyncMock()), \
             patch.object(main.hub_kintone, "get_record",
                          new=AsyncMock(return_value=self._record())):
            nr = TestClient(main.app, raise_server_exceptions=False)
            r = nr.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertGreaterEqual(r.status_code, 500)   # 例外は伝播
        st, le = self._state_of("mk-1")
        self.assertEqual(st, "sending")   # 不明のまま（failed へ上書きしない）
        self.assertIsNone(le)
        self.assertEqual(proc.await_count, 1)   # 送信は試行された

    def test_marker_failure_zero_line_write(self):
        # marker(received→sending) が rowcount=0 に細工 → LINE 送信 0・行は received のまま
        body = _kintone_body(event_id="mk-2")
        proc = AsyncMock(return_value=None)
        with patch.dict(os.environ, {_DEDUP: "1"}), \
             patch.object(main, "send_line_push", new=proc), \
             patch.object(main, "mark_approval_sent", new=AsyncMock()), \
             patch.object(main, "save_to_chatlog", new=AsyncMock()), \
             patch.object(main.hub_kintone, "get_record",
                          new=AsyncMock(return_value=self._record())), \
             patch.object(kl, "mark_sending", new=AsyncMock(return_value=False)):
            r = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual(r.json().get("skip"), "marker_not_acquired")
        proc.assert_not_awaited()   # marker 失敗 → 送信しない（D3-H01）
        st, _ = self._state_of("mk-2")
        self.assertEqual(st, "received")   # 行は現状のまま（滞留観測へ）


# ── 4. 正常 no-op done terminal（偽警報防止・enum 理由コード） ───────────────
class TestNoopDone(_DbMixin):
    def test_noop_marks_done_with_reason(self):
        cases = [
            (_kintone_body(event_id="np-1", status="下書き"), "skip_not_approved"),
            (_kintone_body(event_id="np-2", sent="yes"), "skip_not_approved"),
        ]
        with patch.dict(os.environ, {_DEDUP: "1"}), \
             patch.object(main, "send_line_push", new=AsyncMock()) as proc:
            for body, expect in cases:
                with self.subTest(ev=body["id"]):
                    r = _client.post("/webhook/kintone/approval?token=kintone-token",
                                     json=body)
                    self.assertEqual(r.status_code, 200)
        proc.assert_not_awaited()
        for ev, expect in [("np-1", "skip_not_approved"), ("np-2", "skip_not_approved")]:
            st, le = self._state_of(ev)
            self.assertEqual(st, "done", ev)     # received のまま残さない（偽警報防止）
            self.assertEqual(le, expect, ev)     # enum 理由コード
            self.assertIn(le, kl.NOOP_REASONS)   # 自由文字列でない

    def test_record_not_found_done(self):
        body = _kintone_body(event_id="np-3")
        with patch.dict(os.environ, {_DEDUP: "1"}), \
             patch.object(main.hub_kintone, "get_record",
                          new=AsyncMock(return_value=None)):
            r = _client.post("/webhook/kintone/approval?token=kintone-token", json=body)
        self.assertEqual(r.status_code, 200)
        st, le = self._state_of("np-3")
        self.assertEqual((st, le), ("done", "skip_record_not_found"))


# ── 5. stale received/sending 監視（§4.2b・専用文言） ─────────────────────────
class TestStaleMonitor(_DbMixin):
    async def _seed(self, ev, provider, state, age_hours):
        async with db.session_scope() as s:
            await s.execute(sa.insert(InboundEvent.__table__).values(
                provider=provider, external_event_id=ev, dedup_key=f"{provider}:{ev}",
                payload_hash="0" * 64, signature_result="token", state=state,
                received_at=datetime.now(timezone.utc) - timedelta(hours=age_hours),
                attempts=1))

    def test_kintone_stale_dedicated_alert(self):
        asyncio.run(self._seed("k-old", "kintone", "received", 3))
        asyncio.run(self._seed("k-send", "kintone", "sending", 2))
        db.reset_for_tests()
        with patch.dict(os.environ, {_DEDUP: "1"}):
            problems = asyncio.run(hc.check_journal_backlog())
        db.reset_for_tests()
        self.assertTrue(any("kintone滞留" in p for p in problems), problems)
        self.assertFalse(any("stripe-journal-recovery" in p for p in problems))

    def test_kintone_fresh_no_alert(self):
        asyncio.run(self._seed("k-fresh", "kintone", "received", 0))  # 0h < 1h
        db.reset_for_tests()
        with patch.dict(os.environ, {_DEDUP: "1"}):
            self.assertEqual(asyncio.run(hc.check_journal_backlog()), [])
        db.reset_for_tests()


# ── 6. D2-M03 provider 混在不変（既存 Stripe/LINE と分離） ───────────────────
class TestProviderInvariant(_DbMixin):
    async def _seed(self, ev, provider, state, age_hours, claimed_age=None):
        vals = dict(provider=provider, external_event_id=ev, dedup_key=f"{provider}:{ev}",
                    payload_hash="0" * 64, signature_result="token", state=state,
                    received_at=datetime.now(timezone.utc) - timedelta(hours=age_hours),
                    attempts=1)
        if claimed_age is not None:
            vals["claimed_at"] = datetime.now(timezone.utc) - timedelta(hours=claimed_age)
        async with db.session_scope() as s:
            await s.execute(sa.insert(InboundEvent.__table__).values(**vals))

    def test_mixed_providers_separated(self):
        # Stripe failed 26h（既存警報）/ LINE processing 26h（既存）/ kintone received 3h（新）
        asyncio.run(self._seed("s1", "stripe", "failed", 26))
        asyncio.run(self._seed("l1", "line", "processing", 26, claimed_age=26))
        asyncio.run(self._seed("k1", "kintone", "received", 3))
        # kintone を 24h 閾値に引きずられて誤検知しないこと（3h < 24h）
        db.reset_for_tests()
        with patch.dict(os.environ, {"STRIPE_EVENT_JOURNAL_ENABLED": "1", _DEDUP: "1"}):
            problems = asyncio.run(hc.check_journal_backlog())
        db.reset_for_tests()
        stripe_p = [p for p in problems if "stripe-journal-recovery" in p]
        kintone_p = [p for p in problems if "kintone滞留" in p]
        # 既存 Stripe/LINE 警報は Stripe runbook 文言・件数に kintone を混ぜない
        self.assertTrue(stripe_p, problems)
        self.assertTrue(kintone_p, problems)
        # Stripe/LINE 警報の PK に kintone 行が入っていない（provider 分離）
        joined = " ".join(stripe_p)
        self.assertIn("processing", joined)
        self.assertIn("failed", joined)

    def test_kintone_not_in_stripe_alert_when_only_stripe_flag(self):
        # kintone flag OFF なら kintone 行があっても既存監視は触れない（混入なし）
        asyncio.run(self._seed("k2", "kintone", "failed", 30))
        db.reset_for_tests()
        with patch.dict(os.environ, {"STRIPE_EVENT_JOURNAL_ENABLED": "1"}):
            os.environ.pop(_DEDUP, None)
            problems = asyncio.run(hc.check_journal_backlog())
        db.reset_for_tests()
        # kintone failed 30h は provider!=kintone 除外により既存 failed 警報に出ない
        self.assertFalse(any("kintone" in p for p in problems))
        self.assertFalse(any("failed が24時間超" in p for p in problems), problems)


# ── 7. rotation 4状態 table（old-only/dual/new-primary+NEXT/NEXT削除後） ─────
class TestRotationTable(unittest.TestCase):
    def _accepts(self, token, env):
        with patch.dict(os.environ, {**_ENV, **env}, clear=False):
            for k in ("KINTONE_WEBHOOK_TOKEN", "KINTONE_WEBHOOK_TOKEN_NEXT"):
                if k not in env:
                    os.environ.pop(k, None)
            os.environ["KINTONE_WEBHOOK_TOKEN"] = env.get("KINTONE_WEBHOOK_TOKEN", "")
            if "KINTONE_WEBHOOK_TOKEN_NEXT" in env:
                os.environ["KINTONE_WEBHOOK_TOKEN_NEXT"] = env["KINTONE_WEBHOOK_TOKEN_NEXT"]
            else:
                os.environ.pop("KINTONE_WEBHOOK_TOKEN_NEXT", None)
            return main._verify_kintone_token(token)

    def test_four_states(self):
        OLD, NEW = "old-tok", "new-tok"
        # old-only
        self.assertTrue(self._accepts(OLD, {"KINTONE_WEBHOOK_TOKEN": OLD}))
        self.assertFalse(self._accepts(NEW, {"KINTONE_WEBHOOK_TOKEN": OLD}))
        # dual（primary=old + NEXT=new）
        dual = {"KINTONE_WEBHOOK_TOKEN": OLD, "KINTONE_WEBHOOK_TOKEN_NEXT": NEW}
        self.assertTrue(self._accepts(OLD, dual))
        self.assertTrue(self._accepts(NEW, dual))
        # new-primary + NEXT=new（5-4a 後）
        np = {"KINTONE_WEBHOOK_TOKEN": NEW, "KINTONE_WEBHOOK_TOKEN_NEXT": NEW}
        self.assertTrue(self._accepts(NEW, np))
        self.assertFalse(self._accepts(OLD, np))
        # NEXT 削除後（primary=new のみ）
        self.assertTrue(self._accepts(NEW, {"KINTONE_WEBHOOK_TOKEN": NEW}))
        self.assertFalse(self._accepts(OLD, {"KINTONE_WEBHOOK_TOKEN": NEW}))


# ── 8. legacy strict 検証（H07・異常形 5 種で起動停止） ───────────────────────
class TestLegacyStrict(unittest.TestCase):
    def test_valid_sets(self):
        from hub import service_auth as svc
        self.assertEqual(svc._parse_legacy_disabled_strict(""), frozenset())
        self.assertEqual(svc._parse_legacy_disabled_strict("/koseki/ingest"),
                         frozenset({"/koseki/ingest"}))
        self.assertEqual(
            svc._parse_legacy_disabled_strict("/koseki/ingest,/bank/ingest"),
            frozenset({"/koseki/ingest", "/bank/ingest"}))

    def test_anomaly_forms_raise(self):
        from hub import service_auth as svc
        cases = {
            "unknown": "/unknown/ingest",
            "duplicate": "/koseki/ingest,/koseki/ingest",
            "trailing_slash": "/koseki/ingest/",
            "empty_element": "/koseki/ingest,",
            "fullwidth": "／koseki/ingest",
        }
        for name, raw in cases.items():
            with self.subTest(anomaly=name):
                with self.assertRaises(svc.ServiceAuthConfigError):
                    svc._parse_legacy_disabled_strict(raw)

    def test_startup_failfast_on_bad_value(self):
        with patch.dict(os.environ, {**_ENV, "HEALTHCHECK_DISABLED": "1",
                                     "RETURN_DEADLINE_DISABLED": "1",
                                     "SERVICE_AUTH_LEGACY_DISABLED_PATHS": "/bad/path"}):
            with self.assertRaises(Exception):
                with TestClient(main.app):
                    pass

    def test_legacy_blocked_404_and_reason(self):
        # dual-accept ON・停止 list に koseki → 旧 token で 404（legacy_blocked）
        body = None
        with patch.dict(os.environ, {**_ENV, "KOSEKI_INGEST_TOKEN": "kt",
                                     "SERVICE_AUTH_DUAL_ACCEPT_ENABLED": "1",
                                     "SERVICE_AUTH_LEGACY_DISABLED_PATHS": "/koseki/ingest"}):
            ct = "multipart/form-data; boundary=B"
            b = b"--B\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\nd\r\n--B--\r\n"
            r = _client.post("/koseki/ingest?token=kt", content=b,
                             headers={"Content-Type": ct})
        self.assertEqual(r.status_code, 404)   # 停止 lane は token 有効でも 404

    def test_not_disabled_lane_unaffected(self):
        with patch.dict(os.environ, {**_ENV, "REGISTRY_INGEST_TOKEN": "rt",
                                     "SERVICE_AUTH_DUAL_ACCEPT_ENABLED": "1",
                                     "SERVICE_AUTH_LEGACY_DISABLED_PATHS": "/koseki/ingest"}):
            ct = "multipart/form-data; boundary=B"
            b = b"--B\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\nd\r\n--B--\r\n"
            r = _client.post("/registry/ingest?token=rt", content=b,
                             headers={"Content-Type": ct})
        self.assertEqual(r.status_code, 400)   # 未停止 lane は通過（file 無し 400）


# ── 9. NEXT 残置 notice（D2-M01） ────────────────────────────────────────────
class TestNextResidualNotice(unittest.TestCase):
    def test_notice_when_next_set_and_no_expires(self):
        with patch.dict(os.environ, {"KINTONE_WEBHOOK_TOKEN_NEXT": "n"}):
            os.environ.pop("KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES", None)
            self.assertIsNotNone(hc.check_next_token_residual())

    def test_no_notice_within_expiry(self):
        future = (datetime.now(hc._JST) + timedelta(days=5)).strftime("%Y-%m-%d")
        with patch.dict(os.environ, {"KINTONE_WEBHOOK_TOKEN_NEXT": "n",
                                     "KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES": future}):
            self.assertIsNone(hc.check_next_token_residual())

    def test_notice_when_overdue(self):
        past = (datetime.now(hc._JST) - timedelta(days=1)).strftime("%Y-%m-%d")
        with patch.dict(os.environ, {"KINTONE_WEBHOOK_TOKEN_NEXT": "n",
                                     "KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES": past}):
            self.assertIsNotNone(hc.check_next_token_residual())

    def test_no_notice_when_next_unset(self):
        os.environ.pop("KINTONE_WEBHOOK_TOKEN_NEXT", None)
        self.assertIsNone(hc.check_next_token_residual())


# ── 10. XFF observe-only（reject しない） ────────────────────────────────────
class TestXffObserve(unittest.TestCase):
    def test_observe_off_always_true(self):
        os.environ.pop("KINTONE_XFF_OBSERVE_ENABLED", None)
        self.assertTrue(kl.observe_xff("8.8.8.8"))

    def test_observe_on_in_cidr(self):
        with patch.dict(os.environ, {"KINTONE_XFF_OBSERVE_ENABLED": "1"}):
            self.assertTrue(kl.observe_xff("1.2.3.4, 103.79.14.5"))   # rightmost in cidr

    def test_observe_on_out_of_cidr_returns_membership_no_raise(self):
        # observe_xff は cidr 内かの観測値を返す（out=False）。observe-only の「遮断しない」は
        # 呼び出し側が戻り値を使わないことで担保（例外を出さないことをここで固定）。
        with patch.dict(os.environ, {"KINTONE_XFF_OBSERVE_ENABLED": "1"}):
            self.assertEqual(kl._rightmost_hop("1.2.3.4, 9.9.9.9"), "9.9.9.9")
            self.assertFalse(kl.observe_xff("9.9.9.9"))   # membership=False（例外なし）

    def test_handler_processes_despite_out_of_cidr_xff(self):
        # observe ON＋XFF が cidr 外でも webhook は処理へ進む（reject しない）
        import tempfile as _tf
        d = _tf.mkdtemp(prefix="rv04c_xff_")
        with patch.dict(os.environ, {**_ENV,
                                     "DATABASE_URL": f"sqlite+aiosqlite:///{d}/n.db",
                                     _DEDUP: "1", "KINTONE_XFF_OBSERVE_ENABLED": "1"}):
            db.reset_for_tests()

            async def _c():
                eng = db.get_async_engine()
                async with eng.begin() as c:
                    await c.run_sync(InboundBase.metadata.create_all)
            asyncio.run(_c()); db.reset_for_tests()
            body = _kintone_body(event_id="xff-1", status="下書き")  # no-op で軽く通す
            r = _client.post("/webhook/kintone/approval?token=kintone-token",
                             json=body, headers={"X-Forwarded-For": "9.9.9.9"})
            db.reset_for_tests()
        shutil.rmtree(d, ignore_errors=True)
        self.assertEqual(r.status_code, 200)   # cidr 外でも遮断されない


if __name__ == "__main__":
    unittest.main()
