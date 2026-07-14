"""RV-04b: ingest 群への HMAC dual-accept 結線テスト（§4 Phase A・§6.3 downgrade 禁止）。

マトリクス: 5入口 × {flag OFF / flag ON 旧token / flag ON 署名 / 併記 downgrade}。
認証ゲート合格の判定信号: file 無し body を送ると endpoint が 400（PDF 要求）を返す
＝「ゲート通過」。ゲート不合格は 404（旧token 経路）/ 401/403/400/409（署名経路）。
"""

import asyncio
import hashlib
import json
import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

# ── main import 前に環境変数（既存 ingest テストと同じ流儀）＋ ingest token ──
_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client", "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token", "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "dummy", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "whsec_dummy", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "KOSEKI_INGEST_TOKEN": "koseki-legacy-token",
    "REGISTRY_INGEST_TOKEN": "registry-legacy-token",
    "BANK_INGEST_TOKEN": "bank-legacy-token",
    "SORTATION_INGEST_TOKEN": "sortation-legacy-token",
    "VALUATION_INGEST_TOKEN": "valuation-legacy-token",
}
_ENV_SAVED = {k: os.environ.get(k) for k in _ENV}
os.environ.update(_ENV)

from fastapi.testclient import TestClient  # noqa: E402

import hub.db as db  # noqa: E402
from hub import service_auth as svc  # noqa: E402
import main  # noqa: E402

for _k, _o in _ENV_SAVED.items():
    if _o is None:
        os.environ.pop(_k, None)
    else:
        os.environ[_k] = _o

# ── テスト用 registry（5入口 path を許可・secret はテスト専用） ──────────────
SECRET_HEX = "cd" * 32
SECRET = bytes.fromhex(SECRET_HEX)
INGEST_PATHS = ["/koseki/ingest", "/registry/ingest", "/bank/ingest",
                "/sortation/ingest", "/valuation/ingest"]
LEGACY_TOKENS = {"/koseki/ingest": "koseki-legacy-token",
                 "/registry/ingest": "registry-legacy-token",
                 "/bank/ingest": "bank-legacy-token",
                 "/sortation/ingest": "sortation-legacy-token",
                 "/valuation/ingest": "valuation-legacy-token"}
REG_JSON = json.dumps({"kid-test": {
    "secret": SECRET_HEX, "caller": "gas-ingest", "allowed_methods": ["POST"],
    "allowed_paths": INGEST_PATHS, "not_before": 0, "expires_at": 2 ** 31,
    "status": "active"}})

_FLAG = "SERVICE_AUTH_DUAL_ACCEPT_ENABLED"
_REGENV = "SERVICE_HMAC_KEY_REGISTRY"

# テスト中に必要な env（token 検証・sortation の vision）。patch.dict で毎テスト適用。
_TOKEN_ENV = {"KOSEKI_INGEST_TOKEN": "koseki-legacy-token",
              "REGISTRY_INGEST_TOKEN": "registry-legacy-token",
              "BANK_INGEST_TOKEN": "bank-legacy-token",
              "SORTATION_INGEST_TOKEN": "sortation-legacy-token",
              "VALUATION_INGEST_TOKEN": "valuation-legacy-token"}
_INGEST_ENV = {**_TOKEN_ENV, "GOOGLE_VISION_API_KEY": "dummy_vision"}


def _nofile_multipart():
    """file フィールドを持たない multipart body（endpoint は file=None→400）。"""
    b = (b"--BND\r\nContent-Disposition: form-data; name=\"x\"\r\n\r\nd\r\n--BND--\r\n")
    return "multipart/form-data; boundary=BND", b


def _sig_headers(path, body, nonce, ts=None, key_id="kid-test", caller="gas-ingest",
                 secret=SECRET):
    ts = ts if ts is not None else int(time.time())
    csha = hashlib.sha256(body).hexdigest()
    canon = svc.canonical_v1(key_id, caller, "POST", path, str(ts), nonce, csha)
    return {"X-Sig-Version": "v1", "X-Sig-Key-Id": key_id, "X-Sig-Caller": caller,
            "X-Sig-Timestamp": str(ts), "X-Sig-Nonce": nonce,
            "X-Sig-Content-SHA256": csha, "X-Sig-Signature": svc.sign_v1(secret, canon)}


_client = TestClient(main.app)


class _DbMixin(unittest.TestCase):
    """署名経路（nonce 消費）が要る test 用に file sqlite を用意する。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="rv04b_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(svc.metadata.create_all)
        asyncio.run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)


def _nonce(tag):
    return hashlib.sha256(tag.encode()).hexdigest()[:32]


# ── flag OFF: 現行挙動と完全同一（署名ヘッダは無視される） ──────────────────
class TestFlagOff(unittest.TestCase):
    def setUp(self):
        self._p = patch.dict(os.environ, _INGEST_ENV)
        self._p.start()
        os.environ.pop(_FLAG, None)   # flag OFF を保証（restore は patch.dict が担う）

    def tearDown(self):
        self._p.stop()

    def test_valid_token_accepted_signature_ignored(self):
        # flag OFF + 有効 token + 署名ヘッダ併記 → 署名は無視され token 経路で通過(=400 file無し)
        for path, tok in LEGACY_TOKENS.items():
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                h = _sig_headers(path, b"MISMATCH", _nonce("off" + path))  # 不正署名でも無視される
                h["Content-Type"] = ct
                r = _client.post(f"{path}?token={tok}", content=body, headers=h)
                self.assertEqual(r.status_code, 400, (path, r.text))  # ゲート通過→file無し400

    def test_bad_token_rejected_404_even_with_sig(self):
        # flag OFF + token 無し + 署名ヘッダ有 → 署名無視・token 経路で 404
        path = "/koseki/ingest"
        ct, body = _nofile_multipart()
        h = _sig_headers(path, body, _nonce("off2"))
        h["Content-Type"] = ct
        r = _client.post(path, content=body, headers=h)   # token 無し
        self.assertEqual(r.status_code, 404)


# ── flag ON: 署名ヘッダ皆無 → 旧 query token（Phase A 併存） ─────────────────
class TestFlagOnLegacyToken(unittest.TestCase):
    def setUp(self):
        self._p = patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON})
        self._p.start()

    def tearDown(self):
        self._p.stop()

    def test_no_sig_valid_token_accepted(self):
        for path, tok in LEGACY_TOKENS.items():
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                r = _client.post(f"{path}?token={tok}", content=body,
                                 headers={"Content-Type": ct})
                self.assertEqual(r.status_code, 400, (path, r.text))  # 通過→400

    def test_no_sig_no_token_404(self):
        ct, body = _nofile_multipart()
        r = _client.post("/koseki/ingest", content=body, headers={"Content-Type": ct})
        self.assertEqual(r.status_code, 404)


# ── flag ON: 有効署名 → 5入口すべて受理・再送は 409 ─────────────────────────
class TestFlagOnSignature(_DbMixin):
    def setUp(self):
        super().setUp()
        self._p = patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON})
        self._p.start()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def test_valid_signature_all_five_accepted(self):
        for path in INGEST_PATHS:
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                h = _sig_headers(path, body, _nonce("ok" + path))
                h["Content-Type"] = ct
                r = _client.post(path, content=body, headers=h)
                # ゲート通過（署名 OK・nonce 消費）→ endpoint file 無し 400
                self.assertEqual(r.status_code, 400, (path, r.text))

    def test_nonce_replay_409(self):
        path = "/bank/ingest"
        ct, body = _nofile_multipart()
        n = _nonce("replay")
        h = _sig_headers(path, body, n)
        h["Content-Type"] = ct
        r1 = _client.post(path, content=body, headers=h)
        self.assertEqual(r1.status_code, 400, r1.text)   # 1回目: 通過→file無し400
        r2 = _client.post(path, content=body, headers=h)  # 同一 nonce 再送
        self.assertEqual(r2.status_code, 409, r2.text)   # replay


# ── downgrade 禁止（§6.3・3系） ──────────────────────────────────────────────
class TestDowngradePrevention(_DbMixin):
    def setUp(self):
        super().setUp()
        self._p = patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON})
        self._p.start()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def test_1_valid_token_plus_bad_sig_rejected(self):
        # ① 有効な旧 token を併記しても不正署名は拒否（token に落ちない）
        path = "/koseki/ingest"
        tok = LEGACY_TOKENS[path]
        ct, body = _nofile_multipart()
        h = _sig_headers(path, b"DIFFERENT-BODY", _nonce("dg1"))  # body 不一致→bad_sig 相当
        h["Content-Type"] = ct
        r = _client.post(f"{path}?token={tok}", content=body, headers=h)
        # 署名経路で拒否（401 body_mismatch）。token 併記でも 200/400 にならない
        self.assertEqual(r.status_code, 401, r.text)
        self.assertNotEqual(r.status_code, 400)   # ゲート通過していない

    def test_2_sig_failure_does_not_fallback_to_token(self):
        # ② 署名経路の失敗が旧 token 経路へフォールバックしない
        path = "/registry/ingest"
        tok = LEGACY_TOKENS[path]
        ct, body = _nofile_multipart()
        h = _sig_headers(path, body, _nonce("dg2"))
        h["X-Sig-Signature"] = "00" * 32   # 署名を破壊
        h["Content-Type"] = ct
        r = _client.post(f"{path}?token={tok}", content=body, headers=h)
        self.assertEqual(r.status_code, 401, r.text)   # bad_sig（token へ落ちない）

    def test_3_matrix_sigOK_tokenOK_bothNone(self):
        # ③ 3系: 署名OK=通過 / token OK(署名皆無)=通過 / 両方無=拒否
        path = "/valuation/ingest"
        tok = LEGACY_TOKENS[path]
        ct, body = _nofile_multipart()
        # 署名 OK
        h = _sig_headers(path, body, _nonce("dg3sig"))
        h["Content-Type"] = ct
        self.assertEqual(_client.post(path, content=body, headers=h).status_code, 400)
        # token OK（署名皆無）
        self.assertEqual(_client.post(f"{path}?token={tok}", content=body,
                                      headers={"Content-Type": ct}).status_code, 400)
        # 両方無
        self.assertEqual(_client.post(path, content=body,
                                      headers={"Content-Type": ct}).status_code, 404)


# ── 顧客Bot 経路 非干渉（handler smoke・顧客Bot経路規律） ────────────────────
class TestCustomerBotUnaffected(unittest.TestCase):
    @staticmethod
    def _all_routes():
        # FastAPI は include_router を _IncludedRouter（.original_router）で包むため再帰で辿る
        out = []

        def walk(routes):
            for r in routes:
                out.append(r)
                sub = getattr(r, "routes", None)
                if sub is None:
                    orig = getattr(r, "original_router", None)
                    sub = getattr(orig, "routes", None) if orig else None
                if sub:
                    walk(sub)
        walk(main.app.routes)
        return out

    def test_body_caching_route_only_on_ingest(self):
        by_path = {}
        for r in self._all_routes():
            p = getattr(r, "path", "")
            if p:
                by_path.setdefault(p, r)
        # ingest 5入口は BodyCachingRoute
        for p in INGEST_PATHS:
            self.assertIn(p, by_path, p)
            self.assertIsInstance(by_path[p], svc.BodyCachingRoute, p)
        # 顧客Bot・その他は BodyCachingRoute でない（結線が漏れていない）
        for p in ["/webhook", "/health", "/scan", "/ocr/fixed-asset"]:
            self.assertNotIsInstance(by_path.get(p), svc.BodyCachingRoute, p)

    def test_process_line_event_head_smoke(self):
        # RV-04b 後も顧客Bot handler が先頭ログを通過（UnboundLocalError 等の回帰なし）
        sentinel = RuntimeError("STOP")
        mlog = MagicMock()
        mlog.info.side_effect = sentinel
        with patch.object(main, "logger", mlog):
            with self.assertRaises(RuntimeError) as ctx:
                asyncio.run(main._process_line_event("rt", "Uabc", "こんにちは"))
        self.assertIs(ctx.exception, sentinel)


if __name__ == "__main__":
    unittest.main()
