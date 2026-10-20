"""STARTUP-FAILSAFE-1（QUEUE-2026-10-20-J J-2）: 全鍵失効で全停止しない。

司令塔裁定: 署名経路（戸籍取込み等）は頻度が低く、LINE 受信を道連れにする設計は過剰→新挙動を既定。
pin する契約:
  - 全鍵失効／registry 空／JSON 不正: 起動成功（validate_registry_startup は 0 を返し raise しない）・
    状態 degraded（理由コード 3 種）・署名経路（署名ヘッダ在・署名必須 path）は 401 固定・業務 LINE へ 1 通
  - 鍵正常: 従来どおり（実効鍵数を返す・ok・通知なし・署名検証は従来どおり）
  - SERVICE_AUTH_STARTUP_STRICT=1: 従来の例外（固定文言）・請求時は従来の 503 registry_config_error
  - /health の応答に service_auth（status ok/degraded・reason・signing）を含む。秘密値は出ない
  - 署名基盤を使わない設定（flag OFF・署名必須 path なし）は従来どおり何もしない（ok・signing=off）
  - 通知は台帳 key service_auth_degraded:{JST 日付} で 1 日 1 回（DUPLICATE は送らない・台帳不可は直接送る）
"""

import asyncio
import json
import os
import time
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from hub import send_ledger  # noqa: E402
from hub import service_auth as svc  # noqa: E402
from hub.service_auth import ServiceAuthConfigError  # noqa: E402
from test_rv04b_dual_accept import _client  # noqa: E402  env 事前設定込みの TestClient（別実装を作らない）
from test_rv0102_prep_signed_optin import _SCAN, _scan_body  # noqa: E402

_FLAG = "SERVICE_AUTH_DUAL_ACCEPT_ENABLED"
_REGENV = "SERVICE_HMAC_KEY_REGISTRY"
_REQ = "SERVICE_AUTH_SIGNED_REQUIRED_PATHS"
_STRICT = "SERVICE_AUTH_STARTUP_STRICT"
SECRET = "ef" * 32


def _reg(expires_delta: int, status: str = "active") -> str:
    return json.dumps({"k1": {"secret": SECRET, "caller": "gas-ingest", "allowed_methods": ["POST"],
                              "allowed_paths": ["/scan", "/koseki/ingest"], "status": status,
                              "not_before": 0, "expires_at": int(time.time()) + expires_delta}})


SIG_HEADERS = {"X-Sig-Version": "v1", "X-Sig-Key-Id": "k1", "X-Sig-Caller": "gas-ingest",
               "X-Sig-Timestamp": str(int(time.time())), "X-Sig-Nonce": "0" * 32,
               "X-Sig-Content-SHA256": "0" * 64, "X-Sig-Signature": "00"}


class _Base(unittest.TestCase):
    def setUp(self):
        svc._degraded_reason = None
        self.addCleanup(lambda: setattr(svc, "_degraded_reason", None))

    def env(self, **kw):
        base = {_FLAG: "", _REGENV: "", _REQ: "", _STRICT: ""}
        base.update(kw)
        p = patch.dict(os.environ, base)
        p.start()
        self.addCleanup(p.stop)
        for k, v in base.items():
            if v == "":
                os.environ.pop(k, None)


class TestClassify(_Base):
    def test_codes(self):
        cases = [("", svc.DEGRADED_REGISTRY_EMPTY), ("not-json", svc.DEGRADED_REGISTRY_INVALID),
                 (_reg(-10), svc.DEGRADED_NO_EFFECTIVE_KEYS),
                 (_reg(3600, "revoked"), svc.DEGRADED_NO_EFFECTIVE_KEYS), (_reg(3600), None)]
        for raw, expected in cases:
            with self.subTest(raw=raw[:10]):
                self.env(**{_REGENV: raw})
                self.assertEqual(svc.classify_registry(), expected)


class TestStartup(_Base):
    def test_all_expired_starts_degraded(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        self.assertEqual(svc.validate_registry_startup(), 0)          # raise しない
        self.assertEqual(svc.degraded_reason(), svc.DEGRADED_NO_EFFECTIVE_KEYS)
        self.assertEqual(svc.service_auth_state(),
                         {"status": "degraded", "signing": "on", "reason": "no_effective_keys"})

    def test_empty_and_invalid_start_degraded(self):
        for raw, reason in (("", svc.DEGRADED_REGISTRY_EMPTY), ("{broken", svc.DEGRADED_REGISTRY_INVALID)):
            with self.subTest(reason=reason):
                self.env(**{_FLAG: "1", _REGENV: raw})
                self.assertEqual(svc.validate_registry_startup(), 0)
                self.assertEqual(svc.degraded_reason(), reason)

    def test_healthy_registry_unchanged(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(3600)})
        self.assertEqual(svc.validate_registry_startup(), 1)
        self.assertIsNone(svc.degraded_reason())
        self.assertEqual(svc.service_auth_state(), {"status": "ok", "signing": "on"})

    def test_strict_restores_fail_fast(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10), _STRICT: "1"})
        with self.assertRaises(ServiceAuthConfigError) as ctx:
            svc.validate_registry_startup()
        self.assertEqual(str(ctx.exception), "service auth registry configuration invalid")
        self.assertIsNone(svc.degraded_reason())

    def test_signing_off_is_noop(self):
        self.env(**{_REGENV: "not-json"})
        self.assertEqual(svc.validate_registry_startup(), 0)
        self.assertIsNone(svc.degraded_reason())
        self.assertEqual(svc.service_auth_state(), {"status": "ok", "signing": "off"})

    def test_recovery_clears_degraded(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        svc.validate_registry_startup()
        self.assertIsNotNone(svc.degraded_reason())
        self.env(**{_FLAG: "1", _REGENV: _reg(3600)})
        self.assertEqual(svc.validate_registry_startup(), 1)
        self.assertIsNone(svc.degraded_reason())


class TestStartupNotification(_Base):
    def _patch_notify(self):
        self.notify = AsyncMock(return_value=True)
        p = patch("hub.notify.notify_admin_line", self.notify)
        p.start()
        self.addCleanup(p.stop)

    def test_degraded_notifies_once_with_reason_only(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        self._patch_notify()
        with patch.object(send_ledger, "begin", AsyncMock(return_value=None)):      # 台帳不可→直接送る
            self.assertEqual(asyncio.run(svc.startup_service_auth()), 0)
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[0]
        self.assertIn("【署名経路 停止中】", text)
        self.assertIn("理由コード: no_effective_keys", text)
        self.assertIn("401", text)
        self.assertIn("LINE 受信", text)
        self.assertNotIn(SECRET, text)

    def test_ledger_duplicate_suppresses_second_notice(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        self._patch_notify()
        with patch.object(send_ledger, "begin", AsyncMock(return_value=send_ledger.DUPLICATE_SENT)):
            self.assertTrue(asyncio.run(svc.startup_service_auth()) == 0)
        self.notify.assert_not_awaited()

    def test_ledger_key_is_jst_date(self):
        self.assertRegex(svc._degraded_day_key(), r"^service_auth_degraded:\d{4}-\d{2}-\d{2}$")

    def test_healthy_does_not_notify(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(3600)})
        self._patch_notify()
        self.assertEqual(asyncio.run(svc.startup_service_auth()), 1)
        self.notify.assert_not_awaited()

    def test_notify_failure_does_not_stop_startup(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        p = patch("hub.notify.notify_admin_line", AsyncMock(side_effect=RuntimeError("boom")))
        p.start()
        self.addCleanup(p.stop)
        with patch.object(send_ledger, "begin", AsyncMock(return_value=None)):
            self.assertEqual(asyncio.run(svc.startup_service_auth()), 0)   # 例外は握る


class TestSignedRoutesWhenDegraded(_Base):
    """署名経路は 401 固定・STRICT=1 は従来 503。LINE/health は影響なし。"""

    def setUp(self):
        super().setUp()
        self.client = _client

    def test_signed_request_gets_401_when_degraded(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        svc.validate_registry_startup()
        r = self.client.post(_SCAN, headers={**SIG_HEADERS, "Content-Type": "application/json"}, content=_scan_body())
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["detail"], "signature verification rejected")

    def test_signed_required_path_gets_401_when_degraded(self):
        self.env(**{_REQ: "/scan", _REGENV: ""})
        svc.validate_registry_startup()
        r = self.client.post(_SCAN, headers={**SIG_HEADERS, "Content-Type": "application/json"}, content=_scan_body())
        self.assertEqual(r.status_code, 401)

    def test_strict_keeps_503(self):
        self.env(**{_FLAG: "1", _REGENV: "", _STRICT: "1"})
        r = self.client.post(_SCAN, headers={**SIG_HEADERS, "Content-Type": "application/json"}, content=_scan_body())
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.json()["detail"], "service auth configuration error")

    def test_health_reports_state(self):
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        svc.validate_registry_startup()
        body = self.client.get("/health").json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["service_auth"], {"status": "degraded", "signing": "on",
                                                "reason": "no_effective_keys"})
        self.assertNotIn(SECRET, json.dumps(body))
        self.env(**{_FLAG: "1", _REGENV: _reg(3600)})
        svc.validate_registry_startup()
        self.assertEqual(self.client.get("/health").json()["service_auth"],
                         {"status": "ok", "signing": "on"})
        self.env()
        svc.validate_registry_startup()
        self.assertEqual(self.client.get("/health").json()["service_auth"],
                         {"status": "ok", "signing": "off"})

    def test_unsigned_optin_path_still_accepted_when_degraded(self):
        # dual-accept の非署名受理（従来挙動）は degraded でも変わらない＝認証段は通過する
        self.env(**{_FLAG: "1", _REGENV: _reg(-10)})
        svc.validate_registry_startup()
        r = self.client.post(_SCAN, content=_scan_body(), headers={"Content-Type": "application/json"})
        self.assertEqual(r.status_code, 400)       # 未対応フォルダ名 400＝認証段を通過した判定信号（rv0102 と同じ）


if __name__ == "__main__":
    unittest.main()
