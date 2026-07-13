"""P1-103 PoC: NM01 v1（HMAC 署名 contract）の multipart/form-data 適用検証。

**隔離 PoC**（本番 router/ingest 群には一切結線しない・テスト専用 app）。
DRAFT_RV04_HMAC_MIGRATION.md §2.1（canonical=length-prefix）/ §2.3（検証順8段）に準拠し、
GAS/watcher が送る multipart body（PDF 添付）で content SHA-256 の署名検証が成立するかを実測する。

検証事項 a〜e を自動テスト化（COMPLETION_REPORT に実測表）。
"""

import hashlib
import hmac
import time
import unittest

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

# ── NM01 v1 contract 実装（PoC・§2.1/§2.3 準拠） ─────────────────────────────

_ORDER_TAG = "v1"


def canonical_v1(key_id, caller_id, method, normalized_path,
                 timestamp_str, nonce_hex, content_sha256_hex) -> bytes:
    """§2.1 length-prefix: for each f in ORDER: ascii(len(utf8(f))) || ":" || utf8(f) || "\\n"."""
    order = [_ORDER_TAG, key_id, caller_id, method.upper(), normalized_path,
             timestamp_str, nonce_hex, content_sha256_hex]
    out = b""
    for f in order:
        u = f.encode("utf-8")
        out += str(len(u)).encode("ascii") + b":" + u + b"\n"
    return out


def sign_v1(secret: bytes, canonical: bytes) -> str:
    return hmac.new(secret, canonical, hashlib.sha256).hexdigest()


def normalize_path(raw_path: str):
    """§2.1 H02: decode 前 raw path・末尾 slash 除去のみ。%2F/dot segment/連続 slash/
    非 ASCII は拒否（None を返す＝400 相当）。"""
    if any(ord(c) > 127 for c in raw_path):
        return None
    low = raw_path.lower()
    if "%2f" in low:
        return None
    if "//" in raw_path:
        return None
    segs = raw_path.split("/")
    if any(s in (".", "..") for s in segs):
        return None
    if len(raw_path) > 1 and raw_path.endswith("/"):
        raw_path = raw_path.rstrip("/")
    return raw_path


class NonceStore:
    """§2.4 案B 相当（PoC in-memory）: nonce 一回性。INSERT 衝突=replay。"""

    def __init__(self):
        self._seen = {}   # nonce -> expires_at

    def add(self, nonce, key_id, caller, expires_at) -> bool:
        if nonce in self._seen:
            return False
        self._seen[nonce] = expires_at
        return True


def verify_v1(headers, raw_body: bytes, method: str, raw_path: str,
              registry: dict, nonce_store: NonceStore, now: int, skew: int = 300):
    """§2.3 検証順（fail-closed・downgrade 防止）。(status, reason) を返す。"""
    # 1. 署名ヘッダが存在すれば署名経路（token fallback 禁止）。version!=v1=401。
    if any(k.lower().startswith("x-sig-") for k in headers):
        if headers.get("X-Sig-Version") != "v1":
            return 401, "bad_version"
    else:
        return 401, "no_signature"
    key_id = headers.get("X-Sig-Key-Id", "")
    caller = headers.get("X-Sig-Caller", "")
    ts = headers.get("X-Sig-Timestamp", "")
    nonce = headers.get("X-Sig-Nonce", "")
    csha = headers.get("X-Sig-Content-SHA256", "")
    sig = headers.get("X-Sig-Signature", "")
    # 2. key registry（unknown/expired/revoked=401）
    key = registry.get(key_id)
    if key is None or key["status"] != "active":
        return 401, "key_unknown_or_revoked"
    if not (key["not_before"] <= now <= key["expires_at"]):
        return 401, "key_time"
    # 3. caller 一致
    if caller != key["caller"]:
        return 401, "caller_mismatch"
    # 4. method/normalized_path 許可
    npath = normalize_path(raw_path)
    if npath is None:
        return 400, "bad_path"
    if method.upper() not in key["allowed_methods"]:
        return 403, "method_denied"
    if npath not in key["allowed_paths"]:
        return 403, "path_denied"
    # 5. timestamp SKEW
    try:
        ts_i = int(ts)
    except (TypeError, ValueError):
        return 401, "bad_ts"
    if not (now - skew <= ts_i <= now + skew):
        return 401, "skew"
    # 6. content_sha256 == 実 body hash（body 改変検知）
    body_hash = hashlib.sha256(raw_body).hexdigest()
    if not hmac.compare_digest(csha, body_hash):
        return 401, "body_mismatch"
    # 7. 署名再計算（compare_digest）
    expect = sign_v1(key["secret"], canonical_v1(key_id, caller, method, npath,
                                                 ts, nonce, csha))
    if not hmac.compare_digest(sig, expect):
        return 401, "bad_sig"
    # 8. nonce 一回性（再利用=409）
    if not nonce_store.add(nonce, key_id, caller, ts_i + skew):
        return 409, "replay"
    return 200, "ok"


# ── 隔離 app（本番 router に結線しない・PoC 専用） ───────────────────────────

_SECRET = b"poc-hmac-secret-v1-do-not-use-in-prod"
_NOW_HOLDER = {"now": None}   # テストで時刻を固定するためのフック
_REGISTRY = {
    "kid-1": {
        "secret": _SECRET, "caller": "gas-koseki",
        "allowed_methods": {"POST"}, "allowed_paths": {"/koseki/ingest"},
        "not_before": 0, "expires_at": 2 ** 31, "status": "active",
    },
}
_NONCE = NonceStore()


def _build_app():
    app = FastAPI()

    @app.post("/poc/{full_path:path}")
    async def poc(full_path: str, request: Request):
        raw = await request.body()   # ← STOP条件検証: multipart raw bytes 取得
        now = _NOW_HOLDER["now"] or int(time.time())
        # 署名対象 path は X-Sig-Path（PoC では明示・本番は request の raw path）
        sig_path = request.headers.get("X-Sig-Path", "/" + full_path)
        status, reason = verify_v1(request.headers, raw, request.method, sig_path,
                                   _REGISTRY, _NONCE, now)
        return JSONResponse(status_code=status,
                            content={"reason": reason,
                                     "server_body_sha256": hashlib.sha256(raw).hexdigest()})

    @app.post("/poc-coexist")
    async def poc_coexist(request: Request):
        # 本番 ingest 群は body を form-parse する。署名検証で body() を先読みしても、
        # 後続の form() が同一 body を再パースできる（Starlette が body をキャッシュ）
        # ことを実証する（署名レイヤと form 受理の共存可能性）。
        raw = await request.body()
        form = await request.form()
        upload = form.get("file")
        file_bytes = await upload.read() if upload is not None else b""
        return JSONResponse(content={
            "body_sha256": hashlib.sha256(raw).hexdigest(),
            "parsed_file_sha256": hashlib.sha256(file_bytes).hexdigest(),
            "parsed_ok": upload is not None,
        })

    return app


# ── クライアント側（署名付与・GAS/httpx 相当） ─────────────────────────────

def multipart_manual(boundary: str, filename: str, filedata: bytes,
                     ctype="application/pdf"):
    """手組み multipart/form-data（送信生バイトを完全制御）。"""
    body = (
        f"--{boundary}\r\n".encode()
        + f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode()
        + f"Content-Type: {ctype}\r\n\r\n".encode()
        + filedata
        + f"\r\n--{boundary}--\r\n".encode()
    )
    return f"multipart/form-data; boundary={boundary}", body


def multipart_via_httpx(filename: str, filedata: bytes, ctype="application/pdf"):
    """httpx（=requests/GAS UrlFetchApp 相当）の multipart エンコーダで生バイトを得る。"""
    req = httpx.Request("POST", "http://poc/koseki/ingest",
                        files={"file": (filename, filedata, ctype)})
    req.read()   # streaming multipart content を確定させる
    return req.headers["content-type"], req.content


def sign_headers(key_id, caller, method, sig_path, raw_body, ts, nonce):
    csha = hashlib.sha256(raw_body).hexdigest()
    canon = canonical_v1(key_id, caller, method, sig_path, str(ts), nonce, csha)
    return {
        "X-Sig-Version": "v1", "X-Sig-Key-Id": key_id, "X-Sig-Caller": caller,
        "X-Sig-Timestamp": str(ts), "X-Sig-Nonce": nonce,
        "X-Sig-Content-SHA256": csha,
        "X-Sig-Signature": sign_v1(_SECRET, canon),
        "X-Sig-Path": sig_path,
    }


_PDF = b"%PDF-1.4\n" + b"\x00\x01\x02\x03binary body \xff\xfe payload\n" * 40 + b"%%EOF"
_TS = 1_700_000_000
_client = TestClient(_build_app())


def _fresh_nonce(tag=""):
    return hashlib.sha256((tag + str(len(_NONCE._seen)) + "n").encode()).hexdigest()[:32]


class TestHmacMultipartPoC(unittest.TestCase):
    def setUp(self):
        _NOW_HOLDER["now"] = _TS
        _NONCE._seen.clear()

    def _post(self, ctype, body, headers):
        h = dict(headers)
        h["Content-Type"] = ctype
        return _client.post("/poc/koseki/ingest", content=body, headers=h)

    # a. 同一 PDF バイト列で送受信の content hash 一致
    def test_a_content_hash_matches(self):
        ct, body = multipart_manual("BND-AAAA", "戸籍.pdf", _PDF)
        n = _fresh_nonce("a")
        client_sha = hashlib.sha256(body).hexdigest()
        r = self._post(ct, body, sign_headers("kid-1", "gas-koseki", "POST",
                                              "/koseki/ingest", body, _TS, n))
        self.assertEqual(r.status_code, 200, r.json())
        self.assertEqual(r.json()["server_body_sha256"], client_sha)  # 送信=受信 hash 一致

    # b. boundary が変わっても raw bytes 基準で成立（正規化不要）
    def test_b_boundary_agnostic_raw_bytes(self):
        for bnd in ("SHORT", "a" * 70, "----WebKitFormBoundary7MA4YWxkTrZu0gW"):
            with self.subTest(boundary=bnd):
                ct, body = multipart_manual(bnd, "x.pdf", _PDF)
                n = _fresh_nonce("b" + bnd)
                r = self._post(ct, body, sign_headers("kid-1", "gas-koseki", "POST",
                                                      "/koseki/ingest", body, _TS, n))
                self.assertEqual(r.status_code, 200, r.json())
        # boundary は生body に内包され hash 対象 → 各リクエスト自己完結・正規化不要

    # c. body 1byte 改変で拒否
    def test_c_one_byte_tamper_rejected(self):
        ct, body = multipart_manual("BND-C", "x.pdf", _PDF)
        n = _fresh_nonce("c")
        headers = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        tampered = bytearray(body)
        tampered[len(tampered) // 2] ^= 0x01   # 1bit 改変
        r = self._post(ct, bytes(tampered), headers)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["reason"], "body_mismatch")

    # d. replay / 期限外 / unknown key
    def test_d1_replay_rejected(self):
        ct, body = multipart_manual("BND-D1", "x.pdf", _PDF)
        n = _fresh_nonce("d1")
        h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        self.assertEqual(self._post(ct, body, h).status_code, 200)
        r2 = self._post(ct, body, h)                       # 同一 nonce 再送
        self.assertEqual(r2.status_code, 409)
        self.assertEqual(r2.json()["reason"], "replay")

    def test_d2_expired_timestamp_rejected(self):
        ct, body = multipart_manual("BND-D2", "x.pdf", _PDF)
        old = _TS - 301                                    # SKEW=300 超過
        n = _fresh_nonce("d2")
        h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, old, n)
        r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["reason"], "skew")

    def test_d3_unknown_key_rejected(self):
        ct, body = multipart_manual("BND-D3", "x.pdf", _PDF)
        n = _fresh_nonce("d3")
        h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        h["X-Sig-Key-Id"] = "kid-UNKNOWN"                  # 未知 key_id
        r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["reason"], "key_unknown_or_revoked")

    # e. GAS UrlFetchApp 相当（httpx/requests の multipart 自動境界付与）で成立
    def test_e_gas_httpx_multipart(self):
        ct, body = multipart_via_httpx("戸籍謄本.pdf", _PDF)
        self.assertIn("multipart/form-data; boundary=", ct)   # 自動境界付与
        n = _fresh_nonce("e")
        client_sha = hashlib.sha256(body).hexdigest()
        r = self._post(ct, body, sign_headers("kid-1", "gas-koseki", "POST",
                                              "/koseki/ingest", body, _TS, n))
        self.assertEqual(r.status_code, 200, r.json())
        self.assertEqual(r.json()["server_body_sha256"], client_sha)

    # 追加: raw body を取得できること（STOP条件の否定＝FastAPI/Starlette で取得可能）
    def test_raw_body_retrievable(self):
        ct, body = multipart_manual("BND-RAW", "x.pdf", _PDF)
        n = _fresh_nonce("raw")
        r = self._post(ct, body, sign_headers("kid-1", "gas-koseki", "POST",
                                              "/koseki/ingest", body, _TS, n))
        # server が raw body の sha256 を返せている＝request.body() で生バイト取得可
        self.assertEqual(r.json()["server_body_sha256"], hashlib.sha256(body).hexdigest())

    # 追加: path 転用（署名 path と実 path のズレ）→ path_denied
    def test_path_reuse_denied(self):
        ct, body = multipart_manual("BND-P", "x.pdf", _PDF)
        n = _fresh_nonce("p")
        h = sign_headers("kid-1", "gas-koseki", "POST", "/other/path", body, _TS, n)
        r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 403)
        self.assertEqual(r.json()["reason"], "path_denied")

    # 追加: 署名検証の body() 先読みと、後続の form-parse（本番 ingest 群が使う）が
    # 同一 multipart で共存できること（Starlette の body キャッシュ）。
    # これが成立しないと「署名レイヤ→UploadFile 受理」の本番結線が破綻する。
    def test_body_hash_and_form_parse_coexist(self):
        ct, body = multipart_via_httpx("戸籍謄本.pdf", _PDF)
        h = {"Content-Type": ct}
        r = _client.post("/poc-coexist", content=body, headers=h)
        self.assertEqual(r.status_code, 200, r.text)
        j = r.json()
        self.assertTrue(j["parsed_ok"])
        # body() が返した生バイトの hash（署名対象）と、form-parse で取り出した
        # file の中身（PDF 実体）の hash が両立して取得できている。
        self.assertEqual(j["body_sha256"], hashlib.sha256(body).hexdigest())
        self.assertEqual(j["parsed_file_sha256"], hashlib.sha256(_PDF).hexdigest())


if __name__ == "__main__":
    unittest.main()
