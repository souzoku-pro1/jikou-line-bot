"""P2-CHAIN-008(+fix1 P2HC-H01): /health/deps と probe_deps_once の分離構造のテスト。

- probe 実行（probe_deps_once・課金 API へ触れ得る側）と公開 GET（キャッシュ参照のみ）
  を分離した構造を pin する。依存確認の実呼出は mock（ネットワーク非依存）。
- 系統: 正常／依存障害（403）／タイムアウト／unconfigured／未 probe（unknown）
  ＋防御 4 点（GET が Vision 呼出を増やさない・generic 例外の固定分類・
  timeout 値の実伝搬・sentinel 非露出）＋ /health 無変更の回帰。
"""

import asyncio
import os
import unittest
from unittest.mock import patch

import httpx

from test_rv04b_dual_accept import _client  # main.app の TestClient を共用
import hub.health_deps as hd

_KEY_ENV = {"GOOGLE_VISION_API_KEY": "dummy-vision-key-for-test"}


class _FakeResp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


def _fake_client(post_result, counter=None, init_kwargs=None):
    """httpx.AsyncClient 互換の最小 fake。
    counter: list — post 呼出のたびに append（呼出回数の計数用）。
    init_kwargs: dict — AsyncClient(...) へ渡された kwargs を記録。"""
    class _Fake:
        def __init__(self, *a, **k):
            if init_kwargs is not None:
                init_kwargs.update(k)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, *a, **k):
            if counter is not None:
                counter.append(1)
            if isinstance(post_result, Exception):
                raise post_result
            return post_result
    return _Fake


def _probe():
    return asyncio.run(hd.probe_deps_once())


class _CacheReset(unittest.TestCase):
    def setUp(self):
        hd._last_result = None

    def tearDown(self):
        hd._last_result = None


class TestProbeAndCache(_CacheReset):
    def test_normal_ok(self):
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient", _fake_client(_FakeResp(200))):
            result = _probe()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["deps"]["vision"], {"status": "ok"})
        self.assertIsNotNone(result["checked_at"])
        r = _client.get("/health/deps")            # GET はキャッシュ返却のみ
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), result)

    def test_dependency_failure_403_degraded_but_http_200(self):
        # RCF-M14 本体事象（billing 403）: degraded を返しつつ GET は 200
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient", _fake_client(_FakeResp(403))):
            _probe()
        r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], "degraded")
        self.assertEqual(body["deps"]["vision"],
                         {"status": "error", "http_status": 403})

    def test_timeout_degraded(self):
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(httpx.ReadTimeout("probe timeout"))):
            _probe()
        r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["deps"]["vision"], {"status": "timeout"})

    def test_unconfigured_env_degraded(self):
        env = {k: v for k, v in os.environ.items() if k != "GOOGLE_VISION_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            _probe()
        r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["deps"]["vision"]["status"], "unconfigured")

    def test_unknown_before_first_probe(self):
        # fix1: キャッシュ未生成時は unknown を 200 で返す（外部呼出しゼロ）
        r = _client.get("/health/deps")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), {"status": "unknown", "deps": {},
                                    "checked_at": None})


class TestDenialOfWalletGuard(_CacheReset):
    """fix1（P2HC-H01・Codex 提案テスト 4 点）: 公開 GET を課金 API 実行器にしない。"""

    def test_a_repeated_get_does_not_increase_vision_calls(self):
        calls = []
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(_FakeResp(200), counter=calls)):
            # 未 probe の GET は外部呼出しゼロ
            for _ in range(3):
                _client.get("/health/deps")
            self.assertEqual(len(calls), 0)
            # probe 1 回 → 以後 GET を何度呼んでも計数は増えない
            _probe()
            self.assertEqual(len(calls), 1)
            for _ in range(5):
                self.assertEqual(_client.get("/health/deps").status_code, 200)
            self.assertEqual(len(calls), 1)

    def test_b_generic_connection_error_fixed_classification(self):
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(ConnectionError("conn refused to internal"))):
            result = _probe()
        self.assertEqual(result["status"], "degraded")
        self.assertEqual(result["deps"]["vision"],
                         {"status": "error", "reason": "ConnectionError"})
        self.assertEqual(_client.get("/health/deps").status_code, 200)

    def test_c_timeout_value_passed_to_client(self):
        captured = {}
        with patch.dict(os.environ, {**_KEY_ENV,
                                     "HEALTH_DEPS_TIMEOUT_SECONDS": "2.5"}), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(_FakeResp(200), init_kwargs=captured)):
            _probe()
        self.assertEqual(captured.get("timeout"), 2.5)
        # 不正値は既定 5.0 へフォールバック
        captured2 = {}
        with patch.dict(os.environ, {**_KEY_ENV,
                                     "HEALTH_DEPS_TIMEOUT_SECONDS": "abc"}), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(_FakeResp(200), init_kwargs=captured2)):
            _probe()
        self.assertEqual(captured2.get("timeout"), 5.0)

    def test_d_sentinel_in_exception_and_vendor_body_not_exposed(self):
        sentinel = "SENTINEL-do-not-expose-1234"
        # 例外文に sentinel
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(RuntimeError(sentinel))):
            _probe()
        r = _client.get("/health/deps")
        self.assertNotIn(sentinel, r.text)
        self.assertEqual(r.json()["deps"]["vision"]["reason"], "RuntimeError")
        # vendor 応答本文に sentinel（403 応答の body）
        hd._last_result = None
        with patch.dict(os.environ, _KEY_ENV), \
             patch.object(hd.httpx, "AsyncClient",
                          _fake_client(_FakeResp(403, text=sentinel))):
            _probe()
        r = _client.get("/health/deps")
        self.assertNotIn(sentinel, r.text)


class TestNoSecretInResponse(_CacheReset):
    def test_no_secret_or_url_in_response(self):
        for resp in (_FakeResp(200), _FakeResp(403)):
            hd._last_result = None
            with patch.dict(os.environ, _KEY_ENV), \
                 patch.object(hd.httpx, "AsyncClient", _fake_client(resp)):
                _probe()
            r = _client.get("/health/deps")
            self.assertNotIn(_KEY_ENV["GOOGLE_VISION_API_KEY"], r.text)
            self.assertNotIn("googleapis.com", r.text)


class TestMinimizedDisclosure(_CacheReset):
    """HEALTH-MIN-1（R-P4-001-1 L01）: 公開情報の最小化の機械 pin。"""

    _ENV_NAMES = ("GOOGLE_VISION_API_KEY", "HEALTH_DEPS_TIMEOUT_SECONDS")

    def test_unconfigured_is_fixed_string_only(self):
        # 前後比較: 旧 {"status","reason"(env 名入り)} → 新 {"status"} のみ
        env = {k: v for k, v in os.environ.items() if k != "GOOGLE_VISION_API_KEY"}
        with patch.dict(os.environ, env, clear=True):
            _probe()
        r = _client.get("/health/deps")
        self.assertEqual(r.json()["deps"]["vision"], {"status": "unconfigured"})
        for name in self._ENV_NAMES:
            self.assertNotIn(name, r.text)
        self.assertNotIn("env ", r.text)

    def test_all_scenarios_schema_and_no_env_names(self):
        scenarios = {
            "ok": (_KEY_ENV, _fake_client(_FakeResp(200))),
            "http_403": (_KEY_ENV, _fake_client(_FakeResp(403))),
            "timeout": (_KEY_ENV, _fake_client(httpx.ReadTimeout("t"))),
            "generic_error": (_KEY_ENV, _fake_client(RuntimeError("x"))),
        }
        for label, (env, fake) in scenarios.items():
            with self.subTest(case=label):
                hd._last_result = None
                with patch.dict(os.environ, env), \
                     patch.object(hd.httpx, "AsyncClient", fake):
                    _probe()
                r = _client.get("/health/deps")
                body = r.json()
                # 応答スキーマの閉集合（top-level / deps 名 / dep 値のキー）
                self.assertEqual(set(body), {"status", "deps", "checked_at"})
                self.assertEqual(set(body["deps"]), {"vision"})   # 抽象名のみ
                for dep in body["deps"].values():
                    self.assertLessEqual(set(dep),
                                         {"status", "reason", "http_status"})
                # 秘密・env 名・内部 URL の非含有
                for name in self._ENV_NAMES:
                    self.assertNotIn(name, r.text)
                self.assertNotIn(_KEY_ENV["GOOGLE_VISION_API_KEY"], r.text)
                self.assertNotIn("googleapis", r.text)


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
