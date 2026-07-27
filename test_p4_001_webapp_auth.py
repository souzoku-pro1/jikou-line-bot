"""P4-001: hub/webapp_auth（PWA 認証境界＋shell 配信）のテスト。

固定する仕様（DRAFT_P4 §3/§4＋裁定・司令塔既定 2026-07-27・fix1）:
- /app 配下は /app/login 以外すべて未認証アクセス拒否（303→/app/login・内容非提供）
- 保護 route は _gate 関所必須を機械強制（PUBLIC_ROUTES 以外の全 route・fix1 M01）
- パスワード照合=env の PBKDF2 ハッシュ（平文 env 禁止）・compare_digest 型
- session=署名付き cookie・期限7日・HttpOnly/SameSite=Strict/Secure・
  署名鍵差し替えで全 session 失効・env 未設定は fail-closed
- cookie 改竄（署名・期限・形式）拒否
- 認証材料の強度検証=設定済みで弱い/破損は起動失敗・未設定は機能無効（fix1 H02）
- ログイン防御=byte 上限・固定窓試行制限（PBKDF2 非到達・fix1 H03）
- ログイン失敗時に入力値を反射しない（module は logging 非使用=構造 pin）
- SW は network-only（Cache Storage 全廃・fix1 H01）
"""

import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

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

import main  # noqa: E402
import hub.webapp_auth as webapp_auth  # noqa: E402
from hub.webapp_auth import (  # noqa: E402
    ATTEMPT_LIMIT,
    MIN_ITERATIONS,
    PUBLIC_ROUTES,
    SESSION_TTL_SECONDS,
    WebAppConfigError,
    _attempts,
    hash_password,
    issue_session,
    validate_config,
    verify_session,
)

_client = TestClient(main.app)
_PW = "correct-horse-battery"
_ENV = {
    "WEBAPP_PASSWORD_HASH": hash_password(_PW, iterations=MIN_ITERATIONS),
    "WEBAPP_SESSION_SECRET": "s" * 32,
}

_PROTECTED = ("/app", "/app/app.js", "/app/manifest.json", "/app/sw.js")


def _login(password):
    with patch.dict(os.environ, _ENV):
        return _client.post("/app/login", data={"password": password},
                            follow_redirects=False)


def _cookie_header(value):
    return {"Cookie": f"webapp_session={value}"}


class TestAuthBoundary(unittest.TestCase):
    def setUp(self):
        _attempts.clear()                # 試行制限の状態をテスト間で独立に

    def test_unauthenticated_access_all_rejected(self):
        with patch.dict(os.environ, _ENV):
            for path in _PROTECTED:
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    self.assertEqual(r.status_code, 303)
                    self.assertEqual(r.headers["location"], "/app/login")
                    self.assertNotIn("ダッシュ", r.text)

    def test_login_page_is_public(self):
        r = _client.get("/app/login")
        self.assertEqual(r.status_code, 200)
        self.assertIn("パスワード", r.text)

    def test_login_success_sets_hardened_cookie_and_grants_access(self):
        r = _login(_PW)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app")
        set_cookie = r.headers["set-cookie"]
        self.assertIn("webapp_session=", set_cookie)
        self.assertIn("HttpOnly", set_cookie)
        self.assertIn("SameSite=strict", set_cookie)
        self.assertIn("Secure", set_cookie)
        self.assertIn("Path=/app", set_cookie)
        value = set_cookie.split("webapp_session=")[1].split(";")[0]
        with patch.dict(os.environ, _ENV):
            ok = _client.get("/app", headers=_cookie_header(value),
                             follow_redirects=False)
            self.assertEqual(ok.status_code, 200)
            self.assertIn("ダッシュボード", ok.text)
            for path in _PROTECTED[1:]:
                self.assertEqual(
                    _client.get(path, headers=_cookie_header(value),
                                follow_redirects=False).status_code, 200, path)

    def test_login_failure_no_session_and_no_reflection(self):
        secret_input = "wrong-pass-SENTINEL-090"
        r = _login(secret_input)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/login?e=1")   # 固定応答のみ
        self.assertNotIn("set-cookie", {k.lower() for k in r.headers})
        self.assertNotIn(secret_input, r.text)                      # 応答へ非反射
        # 構造 pin: module は logging を import しない（ログ反射経路なし）
        src = Path(webapp_auth.__file__).read_text(encoding="utf-8")
        self.assertNotIn("import logging", src)
        self.assertNotIn("logger", src)

    def test_env_unset_fail_closed(self):
        env = {k: v for k, v in os.environ.items()
               if k not in ("WEBAPP_PASSWORD_HASH", "WEBAPP_SESSION_SECRET")}
        with patch.dict(os.environ, env, clear=True):
            r = _client.post("/app/login", data={"password": _PW},
                             follow_redirects=False)
            self.assertEqual(r.headers["location"], "/app/login?e=1")
            self.assertFalse(verify_session("123.abc"))


class TestSessionToken(unittest.TestCase):
    def test_roundtrip_and_expiry(self):
        with patch.dict(os.environ, _ENV):
            v = issue_session(now=1000)
            self.assertTrue(verify_session(v, now=1000 + SESSION_TTL_SECONDS - 1))
            self.assertFalse(verify_session(v, now=1000 + SESSION_TTL_SECONDS + 1))

    def test_tampered_rejected(self):
        with patch.dict(os.environ, _ENV):
            v = issue_session()
            exp, _, sig = v.partition(".")
            self.assertFalse(verify_session(f"{exp}.{'0' * len(sig)}"))  # 署名改竄
            future = str(int(time.time()) + 10 ** 6)
            self.assertFalse(verify_session(f"{future}.{sig}"))          # exp 差替え
            self.assertFalse(verify_session("garbage"))
            self.assertFalse(verify_session(""))
            self.assertFalse(verify_session(None))

    def test_secret_rotation_invalidates_all_sessions(self):
        with patch.dict(os.environ, _ENV):
            v = issue_session()
            self.assertTrue(verify_session(v))
        with patch.dict(os.environ, {**_ENV, "WEBAPP_SESSION_SECRET": "n" * 32}):
            self.assertFalse(verify_session(v))     # 鍵差し替え=全 session 失効

    def test_password_hash_format_and_verify(self):
        h = hash_password("pw", iterations=MIN_ITERATIONS)
        self.assertTrue(h.startswith(f"pbkdf2_sha256${MIN_ITERATIONS}$"))
        with patch.dict(os.environ, {"WEBAPP_PASSWORD_HASH": h}):
            from hub.webapp_auth import _verify_password
            self.assertTrue(_verify_password("pw"))
            self.assertFalse(_verify_password("pw2"))
        with patch.dict(os.environ, {"WEBAPP_PASSWORD_HASH": "plaintext"}):
            from hub.webapp_auth import _verify_password
            self.assertFalse(_verify_password("plaintext"))     # 平文 env は不成立


class TestConfigValidation(unittest.TestCase):
    """fix1 H02: 設定済みで弱い/破損=起動失敗・未設定=機能無効（起動は許容）。"""

    def _no_webapp_env(self):
        return {k: v for k, v in os.environ.items()
                if k not in ("WEBAPP_PASSWORD_HASH", "WEBAPP_SESSION_SECRET")}

    def test_unset_env_does_not_block_startup(self):
        with patch.dict(os.environ, self._no_webapp_env(), clear=True):
            validate_config()            # 例外なし=未設定は機能無効として許容

    def test_valid_env_passes(self):
        with patch.dict(os.environ, _ENV):
            validate_config()

    def test_weak_or_broken_config_fails_startup(self):
        good_hash = _ENV["WEBAPP_PASSWORD_HASH"]
        _, iters, salt, digest = good_hash.split("$")
        cases = {
            "secret_too_short": {"WEBAPP_SESSION_SECRET": "s" * 31},
            "secret_empty": {"WEBAPP_SESSION_SECRET": ""},
            "hash_plaintext": {"WEBAPP_PASSWORD_HASH": "plaintext"},
            "hash_neg_iterations":
                {"WEBAPP_PASSWORD_HASH": f"pbkdf2_sha256$-1${salt}${digest}"},
            "hash_zero_iterations":
                {"WEBAPP_PASSWORD_HASH": f"pbkdf2_sha256$0${salt}${digest}"},
            "hash_low_iterations":
                {"WEBAPP_PASSWORD_HASH":
                 f"pbkdf2_sha256${MIN_ITERATIONS - 1}${salt}${digest}"},
            "hash_huge_iterations":
                {"WEBAPP_PASSWORD_HASH":
                 f"pbkdf2_sha256$99999999999${salt}${digest}"},
            "hash_short_salt":
                {"WEBAPP_PASSWORD_HASH":
                 f"pbkdf2_sha256${iters}$aabb${digest}"},
            "hash_short_digest":
                {"WEBAPP_PASSWORD_HASH":
                 f"pbkdf2_sha256${iters}${salt}$abcd"},
            "hash_non_hex_digest":
                {"WEBAPP_PASSWORD_HASH":
                 f"pbkdf2_sha256${iters}${salt}${'z' * 64}"},
        }
        for label, override in cases.items():
            with self.subTest(case=label):
                with patch.dict(os.environ, {**_ENV, **override}):
                    with self.assertRaises(WebAppConfigError) as ctx:
                        validate_config()
                    for v in override.values():          # 固定理由のみ・値の非反射
                        if v:
                            self.assertNotIn(v, str(ctx.exception))

    def test_runtime_also_rejects_weak_material(self):
        # validate をすり抜けても実行時に fail-closed（未処理例外も出さない）
        from hub.webapp_auth import _sign, _verify_password
        salt = "ab" * 16
        with patch.dict(os.environ, {
                "WEBAPP_PASSWORD_HASH": f"pbkdf2_sha256$-5${salt}${'a' * 64}"}):
            self.assertFalse(_verify_password("pw"))    # 負 iterations→例外なし否
        with patch.dict(os.environ, {"WEBAPP_SESSION_SECRET": "short"}):
            self.assertIsNone(_sign("123"))             # 短い鍵では署名しない
            self.assertIsNone(issue_session())

    def test_hash_password_rejects_out_of_range_iterations(self):
        with self.assertRaises(WebAppConfigError):
            hash_password("pw", iterations=MIN_ITERATIONS - 1)
        with self.assertRaises(WebAppConfigError):
            hash_password("pw", iterations=10 ** 9)

    def test_startup_hook_registered(self):
        # router の startup で validate_config が呼ばれる結線の pin
        self.assertIn(webapp_auth._startup_validate,
                      webapp_auth.router.on_startup)


class TestLoginDefenses(unittest.TestCase):
    """fix1 H03: byte 上限・固定窓試行制限（いずれも PBKDF2 非到達で固定応答）。"""

    def setUp(self):
        _attempts.clear()

    def test_oversize_password_rejected_before_pbkdf2(self):
        big = "あ" * 400                                  # 1200 bytes > 1024
        with patch.object(webapp_auth.hashlib, "pbkdf2_hmac") as mock_kdf:
            r = _login(big)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/login?e=1")
        mock_kdf.assert_not_called()                     # PBKDF2 非到達の pin

    def test_lockout_after_limit_and_no_pbkdf2(self):
        for _ in range(ATTEMPT_LIMIT):
            _login("wrong")
        with patch.object(webapp_auth.hashlib, "pbkdf2_hmac") as mock_kdf:
            r = _login(_PW)                              # 正しい値でもロック中は拒否
        self.assertEqual(r.headers["location"], "/app/login?e=1")
        mock_kdf.assert_not_called()

    def test_window_expiry_unlocks(self):
        for _ in range(ATTEMPT_LIMIT):
            _login("wrong")
        future = time.time() + webapp_auth.ATTEMPT_WINDOW_SECONDS + 1
        with patch.object(webapp_auth.time, "time", return_value=future):
            r = _login(_PW)
        self.assertEqual(r.headers["location"], "/app")  # 窓満了で自然解除

    def test_success_resets_counter(self):
        for _ in range(ATTEMPT_LIMIT - 1):
            _login("wrong")
        self.assertEqual(_login(_PW).headers["location"], "/app")
        self.assertEqual(_login(_PW).headers["location"], "/app")  # 解除済み

    def test_no_raw_ip_in_state(self):
        _login("wrong")
        self.assertTrue(_attempts)
        for key in _attempts:
            self.assertRegex(key, r"^[0-9a-f]{64}$")     # sha256 のみ・生 IP 不在
            self.assertNotIn("testclient", key)


class TestRouteGateEnforcement(unittest.TestCase):
    """fix1 M01: 公開例外リスト以外の全 route が認証関所を持つことの機械検査。"""

    def test_all_non_public_routes_are_gated(self):
        routes = [r for r in webapp_auth.router.routes if hasattr(r, "endpoint")]
        self.assertGreaterEqual(len(routes), 7)          # 走査対象が空でないこと
        for route in routes:
            for method in route.methods:
                if (route.path, method) in PUBLIC_ROUTES:
                    continue
                with self.subTest(path=route.path, method=method):
                    self.assertTrue(
                        getattr(route.endpoint, "__webapp_gate__", False),
                        f"{method} {route.path} に認証関所（_gate）がない")

    def test_public_list_is_login_only(self):
        self.assertEqual(PUBLIC_ROUTES,
                         {("/app/login", "GET"), ("/app/login", "POST")})


class TestPathNormalization(unittest.TestCase):
    """fix1 M02: path 正規化系・未知 /app/* の未認証拒否（内容非提供）。"""

    _VARIANTS = ("/app/", "/app//x", "/app/unknown", "/app/%2e%2e/health",
                 "/app/./x", "/app/index.html", "/app/login/../sw.js")

    def test_variants_unauthenticated_rejected(self):
        with patch.dict(os.environ, _ENV):
            for path in self._VARIANTS:
                with self.subTest(path=path):
                    r = _client.get(path, follow_redirects=False)
                    self.assertIn(r.status_code, (303, 307, 404))
                    if r.status_code in (303, 307):
                        self.assertIn(r.headers["location"],
                                      ("/app/login", "/app"))
                    self.assertNotIn("ダッシュ", r.text)     # 内容非提供

    def test_uppercase_path_serves_nothing(self):
        r = _client.get("/APP", follow_redirects=False)
        self.assertEqual(r.status_code, 404)             # 別 path=内容非提供

    def test_unknown_path_authed_is_404_no_reflection(self):
        with patch.dict(os.environ, _ENV):
            v = issue_session()
            r = _client.get("/app/nope-SENT", headers=_cookie_header(v),
                            follow_redirects=False)
            self.assertEqual(r.status_code, 404)
            self.assertNotIn("nope-SENT", r.text)        # path 値の非反射


class TestModuleRootAndShell(unittest.TestCase):
    def test_webapp_root_absolute_and_cwd_independent(self):
        # fix1 M03: module 位置基準の絶対 path・別 CWD 起動でも配信可能
        import tempfile
        self.assertTrue(webapp_auth.WEBAPP_ROOT.is_absolute())
        old = os.getcwd()
        try:
            os.chdir(tempfile.gettempdir())
            with patch.dict(os.environ, _ENV):
                v = issue_session()
                r = _client.get("/app", headers=_cookie_header(v),
                                follow_redirects=False)
                self.assertEqual(r.status_code, 200)
                self.assertIn("ダッシュボード", r.text)
        finally:
            os.chdir(old)

    def test_sw_is_network_only_no_cache_storage(self):
        # fix1 H01: Cache Storage 全廃の機械検査（ソースパターン）
        sw = Path(webapp_auth.WEBAPP_ROOT / "sw.js").read_text(encoding="utf-8")
        for forbidden in ("caches.", "caches.open", "CacheStorage",
                          'addEventListener("fetch"', "respondWith",
                          "cache-first", "addAll("):
            self.assertNotIn(forbidden, sw, forbidden)
        self.assertIn("network", sw)                     # 設計判断の明記
        self.assertIn("認証境界優先", sw)

    def test_manifest_single_file_naming(self):
        import json
        m = json.loads((webapp_auth.WEBAPP_ROOT / "manifest.json")
                       .read_text(encoding="utf-8"))
        self.assertIn("案件管理", m["name"])        # 仮名称（差し替えはこの1ファイル）
        self.assertEqual(m["scope"], "/app")


if __name__ == "__main__":
    unittest.main()
