"""P2-CHAIN-008: /health/deps（RCF-M14 監視拡張）のテスト。

- 依存確認の実呼出は mock（ネットワーク非依存）。
- 3 系統: 正常（ok）／依存障害（403→degraded）／タイムアウト（degraded）
  ＋ /health 無変更の回帰・secret 非露出の検査。
- RCF-M14 教訓の pin: 依存が落ちても /health/deps 自体は HTTP 200。
"""

import os
import unittest
from unittest.mock import patch

import httpx

from test_rv04b_dual_accept import _client  # main.app の TestClient を共用
import hub.health_deps as hd

_KEY_ENV = {"GOOGLE_VISION_API_KEY": "dummy-vision-key-for-test"}


class _FakeResp:
    def __init__(self, status_code):
        self.status_code = status_code


def _fake_client(post_result):
    """httpx.AsyncClient 互換の最小 fake。post_result は _FakeResp か例外。"""
    class _Fake:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            if isinstance(post_result, Exception):
                raise post_result
            return post_result
    return _Fake


class TestHealthDeps(unittest.TestCase):
    def test_normal_ok(self):
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient", _fake_client(_FakeResp(200))):
            r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["deps"]["vision"], {"status": "ok"})

    def test_dependency_failure_403_degraded_but_http_200(self):
        # RCF-M14 本体事象（billing 403）: degraded を返しつつ HTTP は 200
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient", _fake_client(_FakeResp(403))):
            r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)   # healthcheck 自体は落とさない
        body = r.json()
        self.assertEqual(body["status"], "degraded")
        self.assertEqual(body["deps"]["vision"],
                         {"status": "error", "http_status": 403})

    def test_timeout_degraded_but_http_200(self):
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(httpx.ReadTimeout("probe timeout"))):
            r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], "degraded")
        self.assertEqual(body["deps"]["vision"], {"status": "timeout"})

    def test_unconfigured_env_degraded(self):
        env = {k: v for k, v in os.environ.items() if k != "GOOGLE_VISION_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["deps"]["vision"]["status"], "unconfigured")

    def test_no_secret_in_response(self):
        # 応答に API key・vision URL を含めない（H02 流儀）
        for resp in (_FakeResp(200), _FakeResp(403)):
            with patch.dict(os.environ, _KEY_ENV), \
                 patch.object(hd.httpx, "AsyncClient", _fake_client(resp)):
                r = _client.get("/health/deps")
            self.assertNotIn(_KEY_ENV["GOOGLE_VISION_API_KEY"], r.text)
            self.assertNotIn("googleapis.com", r.text)


class TestHealthRegression(unittest.TestCase):
    def test_health_unchanged(self):
        # 既存 /health は無変更（死活監視の互換維持・mock 不要で応答すること）。
        # 従来から deps キーは「依存ライブラリの import チェック」であり、
        # 外部サービス probe（vision 等）は /health/deps 側にのみ置く。
        r = _client.get("/health")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertIn("python-docx", body.get("deps", {}))   # 従来のライブラリ確認のまま
        self.assertNotIn("vision", body.get("deps", {}))     # 外部 probe は混ぜない


if __name__ == "__main__":
    unittest.main()
