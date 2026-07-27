"""P4-001: hub/webapp_auth（PWA 認証境界＋shell 配信）のテスト。

固定する仕様（DRAFT_P4 §3/§4＋裁定・司令塔既定 2026-07-27）:
- /app 配下は /app/login 以外すべて未認証アクセス拒否（303→/app/login・内容非提供）
- パスワード照合=env の PBKDF2 ハッシュ（平文 env 禁止）・compare_digest 型
- session=署名付き cookie・期限7日・HttpOnly/SameSite=Strict/Secure・
  署名鍵差し替えで全 session 失効・env 未設定は fail-closed
- cookie 改竄（署名・期限・形式）拒否
- ログイン失敗時に入力値を反射しない（module は logging 非使用=構造 pin）
- SW キャッシュは shell のみ（sw.js に PII キャッシュ経路なし）
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
from hub.webapp_auth import (  # noqa: E402
    SESSION_TTL_SECONDS,
    hash_password,
    issue_session,
    verify_session,
)

_client = TestClient(main.app)
_PW = "correct-horse-battery"
_ENV = {
    "WEBAPP_PASSWORD_HASH": hash_password(_PW, iterations=1000),  # テストは低回数
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
        src = Path("hub/webapp_auth.py").read_text(encoding="utf-8")
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
        h = hash_password("pw", iterations=1000)
        self.assertTrue(h.startswith("pbkdf2_sha256$1000$"))
        with patch.dict(os.environ, {"WEBAPP_PASSWORD_HASH": h}):
            from hub.webapp_auth import _verify_password
            self.assertTrue(_verify_password("pw"))
            self.assertFalse(_verify_password("pw2"))
        with patch.dict(os.environ, {"WEBAPP_PASSWORD_HASH": "plaintext"}):
            from hub.webapp_auth import _verify_password
            self.assertFalse(_verify_password("plaintext"))     # 平文 env は不成立


class TestShellCachePolicy(unittest.TestCase):
    def test_sw_precaches_shell_only(self):
        sw = Path("webapp/sw.js").read_text(encoding="utf-8")
        self.assertIn('const SHELL = ["/app", "/app/app.js", "/app/manifest.json"]',
                      sw)
        self.assertNotIn("/app/api", sw)            # データ経路をキャッシュしない
        self.assertIn("キャッシュしない", sw)

    def test_manifest_single_file_naming(self):
        import json
        m = json.loads(Path("webapp/manifest.json").read_text(encoding="utf-8"))
        self.assertIn("案件管理", m["name"])        # 仮名称（差し替えはこの1ファイル）
        self.assertEqual(m["scope"], "/app")


if __name__ == "__main__":
    unittest.main()
