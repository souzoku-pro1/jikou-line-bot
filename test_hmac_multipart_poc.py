"""P1-103 PoC: NM01 v1（HMAC 署名 contract）の multipart/form-data 適用検証。

**隔離 PoC**（本番 router/ingest 群には一切結線しない・テスト専用 app）。
DRAFT_RV04_HMAC_MIGRATION.md §2.1（canonical=length-prefix）/ §2.3（検証順8段）に準拠し、
GAS/watcher が送る multipart body（PDF 添付）で content SHA-256 の署名検証が成立するかを実測する。

検証事項 a〜e を自動テスト化（COMPLETION_REPORT に実測表）。
"""

import hashlib
import hmac
import logging
import time
import unittest

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

# ── NM01 v1 contract 実装（PoC・§2.1/§2.3 準拠） ─────────────────────────────

_ORDER_TAG = "v1"
_LOG = logging.getLogger("poc.hmac")   # M01: retiring 受理 warning の出力先


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


def normalize_path(raw_path):
    """§2.1 H02: **decode 前 raw path** を対象。正規化は末尾 slash 除去のみ。
    以下を拒否（None＝400 相当）:
      - 非 ASCII 生バイト（ord > 127）
      - percent-encoding（`%` を1つでも含む＝%2F・%2e・%252F 等の二重符号化を一括排除。
        署名対象 path に符号化を持ち込ませない＝decode で意味が変わる余地をゼロにする）
      - 連続 slash（`//`）
      - dot segment（`.` / `..`）
    raw_path は scope["raw_path"] を latin-1 で 1:1 デコードした str を想定（生バイト保持）。"""
    if raw_path is None:
        return None
    if any(ord(c) > 127 for c in raw_path):       # 非 ASCII 生バイト
        return None
    if "%" in raw_path:                            # percent-encoding は許可しない
        return None
    if "//" in raw_path:                           # 連続 slash
        return None
    if any(s in (".", "..") for s in raw_path.split("/")):   # dot segment
        return None
    if len(raw_path) > 1 and raw_path.endswith("/"):         # 末尾 slash 除去のみ
        raw_path = raw_path.rstrip("/")
    return raw_path


def effective_signed_path(scope, prefix="/poc"):
    """ASGI scope から署名対象の生 path を取り出す（H01: decode 前 raw_path 基準）。
    raw_path 欠落は None を返す＝**fail-closed**（受理しない）。PoC の mount prefix
    "/poc" は生バイトのまま除去する。"""
    rp = scope.get("raw_path")
    if rp is None:
        return None
    s = rp.decode("latin-1")   # bytes→str（1:1・生バイト保持）
    if s == prefix or s.startswith(prefix + "/"):
        return s[len(prefix):] or "/"
    return s


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
    # 2. key registry（§2.5 lifecycle・reason 分離: unknown/revoked/not-before/expired）
    key = registry.get(key_id)
    if key is None:
        return 401, "key_unknown"
    if key["status"] == "revoked":
        return 401, "key_revoked"
    if now < key["not_before"]:
        return 401, "key_not_yet_valid"
    if now > key["expires_at"]:
        return 401, "key_expired"
    if key["status"] not in ("active", "retiring"):
        return 401, "key_unknown"      # 未定義 status は保守的に拒否（fail-closed）
    key_retiring = key["status"] == "retiring"   # §2.5: retiring=受理+警告
    # 3. caller 一致
    if caller != key["caller"]:
        return 401, "caller_mismatch"
    # 4. method/normalized_path 許可（H01: raw_path は実 request path・クライアント
    #    指定の path 系ヘッダは検証対象にしない）
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
    # 全通過。retiring 鍵は受理しつつ警告ログ（§2.5/§6.2: 受理するが警告）。
    if key_retiring:
        # M01: 可視は key_id・caller_id のみ。secret/署名/nonce/body は混入させない。
        _LOG.warning("retiring key accepted: key_id=%s caller=%s", key_id, caller)
        return 200, "ok_retiring"
    return 200, "ok"


# ── 隔離 app（本番 router に結線しない・PoC 専用） ───────────────────────────

_SECRET = b"poc-hmac-secret-v1-do-not-use-in-prod"
_TS = 1_700_000_000          # 固定時刻（registry の lifecycle 境界もこれ基準）
_NOW_HOLDER = {"now": None}   # テストで時刻を固定するためのフック


def _key(status="active", not_before=0, expires_at=2 ** 31, paths=None):
    return {
        "secret": _SECRET, "caller": "gas-koseki",
        "allowed_methods": {"POST"},
        "allowed_paths": paths or {"/koseki/ingest", "/koseki/ingest2"},
        "not_before": not_before, "expires_at": expires_at, "status": status,
    }


# §2.5 lifecycle を網羅する registry（active/retiring/revoked/expired/not-before）
_REGISTRY = {
    "kid-1":        _key(status="active"),
    "kid-retiring": _key(status="retiring"),                     # 受理+警告
    "kid-revoked":  _key(status="revoked"),                      # 受理停止
    "kid-expired":  _key(status="active", expires_at=_TS - 1),   # 失効済
    "kid-future":   _key(status="active", not_before=_TS + 1),   # 有効化前
}
_NONCE = NonceStore()


def _build_app():
    app = FastAPI()

    @app.post("/poc/{full_path:path}")
    async def poc(full_path: str, request: Request):
        raw = await request.body()   # ← STOP条件検証: multipart raw bytes 取得
        now = _NOW_HOLDER["now"] or int(time.time())
        body_sha = hashlib.sha256(raw).hexdigest()
        # H01(fix2): 署名対象 path は ASGI scope["raw_path"]（decode 前生バイト）。
        # decode 済み path（full_path）を使うと %2F 等で separator を smuggling され
        # path 拘束が破れる。raw_path 欠落は fail-closed（受理しない）。
        eff = effective_signed_path(request.scope)
        if eff is None:
            return JSONResponse(status_code=400,
                                content={"reason": "raw_path_unavailable",
                                         "server_body_sha256": body_sha})
        status, reason = verify_v1(request.headers, raw, request.method, eff,
                                   _REGISTRY, _NONCE, now)
        return JSONResponse(status_code=status,
                            content={"reason": reason, "server_body_sha256": body_sha})

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
    # sig_path は「クライアントが署名した path」。H01 後はサーバが実 request path で
    # 再計算するため、実 path と食い違えば bad_sig/path_denied になる（path 系ヘッダは送らない）。
    csha = hashlib.sha256(raw_body).hexdigest()
    canon = canonical_v1(key_id, caller, method, sig_path, str(ts), nonce, csha)
    return {
        "X-Sig-Version": "v1", "X-Sig-Key-Id": key_id, "X-Sig-Caller": caller,
        "X-Sig-Timestamp": str(ts), "X-Sig-Nonce": nonce,
        "X-Sig-Content-SHA256": csha,
        "X-Sig-Signature": sign_v1(_SECRET, canon),
    }


def verify_headers(key_id, caller, method, sig_path, csha, ts, nonce):
    """verify_v1 を直接叩く table test 用。任意フィールドで署名を組む（csha を渡せる）。"""
    canon = canonical_v1(key_id, caller, method, sig_path, str(ts), nonce, csha)
    return {
        "X-Sig-Version": "v1", "X-Sig-Key-Id": key_id, "X-Sig-Caller": caller,
        "X-Sig-Timestamp": str(ts), "X-Sig-Nonce": nonce,
        "X-Sig-Content-SHA256": csha,
        "X-Sig-Signature": sign_v1(_SECRET, canon),
    }


_PDF = b"%PDF-1.4\n" + b"\x00\x01\x02\x03binary body \xff\xfe payload\n" * 40 + b"%%EOF"
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

    def _post_to(self, url, ctype, body, headers):
        """実 request path を明示指定して POST（H01: path 拘束の検証用）。"""
        h = dict(headers)
        h["Content-Type"] = ctype
        return _client.post(url, content=body, headers=h)

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
        self.assertEqual(r.json()["reason"], "key_unknown")

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

    # ── [H01] 実 path 拘束（署名は実 routing path で再計算・path 系ヘッダ非信用） ──
    # allowlist 外の実 path へ、正規 path の署名を転用 → 403 path_denied
    # （実 path が allowlist に無い時点で 4段 path deny・署名検証に至らない）
    def test_h01_foreign_real_path_denied(self):
        ct, body = multipart_manual("BND-H1A", "x.pdf", _PDF)
        n = _fresh_nonce("h1a")
        h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        r = self._post_to("/poc/not/allowed", ct, body, h)   # 実 path=/not/allowed
        self.assertEqual(r.status_code, 403)
        self.assertEqual(r.json()["reason"], "path_denied")

    # allowlist 内の別実 path へ署名を転用 → 実 path で再計算し 401 bad_sig
    # （4段 path deny は通過するが、7段で署名 path≠実 path が露見する）
    def test_h01_allowed_real_path_reuse_bad_sig(self):
        ct, body = multipart_manual("BND-H1B", "x.pdf", _PDF)
        n = _fresh_nonce("h1b")
        h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        r = self._post_to("/poc/koseki/ingest2", ct, body, h)  # 実 path=/koseki/ingest2(許可)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["reason"], "bad_sig")

    # ── [H02] key lifecycle（§2.5・reason 分離） ──
    # retiring = 受理 + 警告（reason=ok_retiring）
    def test_h02_retiring_accepted_with_warning(self):
        ct, body = multipart_manual("BND-RET", "x.pdf", _PDF)
        n = _fresh_nonce("ret")
        h = sign_headers("kid-retiring", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 200, r.json())
        self.assertEqual(r.json()["reason"], "ok_retiring")

    # revoked = 受理停止（401 key_revoked・unknown と reason 分離）
    def test_h02_revoked_key(self):
        ct, body = multipart_manual("BND-REV", "x.pdf", _PDF)
        n = _fresh_nonce("rev")
        h = sign_headers("kid-revoked", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["reason"], "key_revoked")

    # expired = 有効期限切れ（401 key_expired）
    def test_h02_expired_key(self):
        ct, body = multipart_manual("BND-EXP", "x.pdf", _PDF)
        n = _fresh_nonce("exp")
        h = sign_headers("kid-expired", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["reason"], "key_expired")

    # not-before = 有効化前（401 key_not_yet_valid）
    def test_h02_not_yet_valid_key(self):
        ct, body = multipart_manual("BND-FUT", "x.pdf", _PDF)
        n = _fresh_nonce("fut")
        h = sign_headers("kid-future", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 401)
        self.assertEqual(r.json()["reason"], "key_not_yet_valid")

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

    # ── [H01/fix2] raw_path（decode 前生バイト）基準の path 拘束 ──
    # /poc/koseki%2Fingest: decode 後は /koseki/ingest（許可）だが raw は %2F を含む。
    # 攻撃者が decode 後 path で正しく署名しても、サーバは raw_path 基準 → 400 bad_path。
    def test_h01_encoded_slash_smuggle_rejected(self):
        ct, body = multipart_manual("BND-ENC", "x.pdf", _PDF)
        n = _fresh_nonce("enc")
        h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        r = self._post_to("/poc/koseki%2Fingest", ct, body, h)
        self.assertEqual(r.status_code, 400, r.json())
        self.assertEqual(r.json()["reason"], "bad_path")

    # %2e%2e（encoded dot segment）／%252F（二重符号化）／連続 slash の raw path 拒否
    def test_h01_raw_path_malformed_rejected(self):
        for url, tag in [("/poc/a%2e%2e/b", "encdot"),
                         ("/poc/%252F", "dblenc"),
                         ("/poc/x//y", "dslash")]:
            with self.subTest(url=url):
                ct, body = multipart_manual("BND-" + tag, "x.pdf", _PDF)
                n = _fresh_nonce(tag)
                h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
                r = self._post_to(url, ct, body, h)
                self.assertEqual(r.status_code, 400, (url, r.json()))
                self.assertEqual(r.json()["reason"], "bad_path")

    # normalize_path 単体（非 ASCII 生バイト・percent・dot・連続 slash・末尾 slash 除去）
    def test_h01_normalize_path_unit(self):
        self.assertIsNone(normalize_path("/koseki/／ingest"))  # 非 ASCII 生バイト
        self.assertIsNone(normalize_path("/koseki%2fingest"))       # %
        self.assertIsNone(normalize_path("/a/../b"))                # dot segment
        self.assertIsNone(normalize_path("/a//b"))                  # 連続 slash
        self.assertIsNone(normalize_path(None))
        self.assertEqual(normalize_path("/koseki/ingest/"), "/koseki/ingest")  # 末尾 slash 除去
        self.assertEqual(normalize_path("/koseki/ingest"), "/koseki/ingest")

    # scope["raw_path"] 欠落 → fail-closed（helper が None・endpoint は 400）
    def test_h01_missing_raw_path_fail_closed(self):
        self.assertIsNone(effective_signed_path({}))                 # raw_path 欠落
        self.assertIsNone(effective_signed_path({"raw_path": None}))
        self.assertEqual(effective_signed_path({"raw_path": b"/poc/koseki/ingest"}),
                         "/koseki/ingest")
        # endpoint 経路: middleware で raw_path を落とすと 400 raw_path_unavailable
        probe = _build_app()

        @probe.middleware("http")
        async def _strip(request, call_next):
            request.scope.pop("raw_path", None)
            return await call_next(request)

        tc = TestClient(probe)
        ct, body = multipart_manual("BND-NORAW", "x.pdf", _PDF)
        h = sign_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", body, _TS,
                         _fresh_nonce("noraw"))
        h["Content-Type"] = ct
        r = tc.post("/poc/koseki/ingest", content=body, headers=h)
        self.assertEqual(r.status_code, 400, r.text)
        self.assertEqual(r.json()["reason"], "raw_path_unavailable")

    # ── [M01] retiring 受理 warning は 1 回・機微情報ゼロ ──
    def test_m01_retiring_warning_once_no_sensitive(self):
        ct, body = multipart_manual("BND-RETLOG", "x.pdf", _PDF)
        n = _fresh_nonce("retlog")
        h = sign_headers("kid-retiring", "gas-koseki", "POST", "/koseki/ingest", body, _TS, n)
        with self.assertLogs("poc.hmac", level="WARNING") as cm:
            r = self._post(ct, body, h)
        self.assertEqual(r.status_code, 200, r.json())
        warns = [x for x in cm.output if "retiring key accepted" in x]
        self.assertEqual(len(warns), 1)                    # warning は 1 回
        joined = "\n".join(cm.output)
        self.assertIn("kid-retiring", joined)              # key_id 可視
        self.assertIn("gas-koseki", joined)                # caller 可視
        # 機微情報が混入していない（署名・nonce・content hash・secret・body 断片）
        self.assertNotIn(h["X-Sig-Signature"], joined)
        self.assertNotIn(h["X-Sig-Nonce"], joined)
        self.assertNotIn(h["X-Sig-Content-SHA256"], joined)
        self.assertNotIn(_SECRET.decode(), joined)

    # ── §6.1/§6.2 status–reason table を verify_v1 で 1:1 網羅（DRAFT と同期） ──
    def test_status_reason_table(self):
        good = hashlib.sha256(b"body").hexdigest()
        base = ("kid-1", "gas-koseki", "POST", "/koseki/ingest")

        def hv(**kw):
            a = dict(key_id="kid-1", caller="gas-koseki", method="POST",
                     sig_path="/koseki/ingest", csha=good, ts=_TS, nonce="Nx")
            a.update(kw)
            return verify_headers(a["key_id"], a["caller"], a["method"], a["sig_path"],
                                  a["csha"], a["ts"], a["nonce"])

        # (label, headers, method, path, expected_status, expected_reason)
        cases = [
            ("active_ok",       hv(nonce="n01"),                          "POST", "/koseki/ingest", 200, "ok"),
            ("retiring",        hv(key_id="kid-retiring", nonce="n02"),   "POST", "/koseki/ingest", 200, "ok_retiring"),
            ("no_signature",    {},                                       "POST", "/koseki/ingest", 401, "no_signature"),
            ("bad_version",     {**hv(nonce="n03"), "X-Sig-Version": "v2"}, "POST", "/koseki/ingest", 401, "bad_version"),
            ("key_unknown",     hv(key_id="kid-x", nonce="n04"),          "POST", "/koseki/ingest", 401, "key_unknown"),
            ("key_revoked",     hv(key_id="kid-revoked", nonce="n05"),    "POST", "/koseki/ingest", 401, "key_revoked"),
            ("key_not_yet",     hv(key_id="kid-future", nonce="n06"),     "POST", "/koseki/ingest", 401, "key_not_yet_valid"),
            ("key_expired",     hv(key_id="kid-expired", nonce="n07"),    "POST", "/koseki/ingest", 401, "key_expired"),
            ("method_denied",   hv(method="DELETE", nonce="n10"),         "DELETE", "/koseki/ingest", 403, "method_denied"),
            ("path_denied",     hv(sig_path="/not/allowed", nonce="n11"), "POST", "/not/allowed",   403, "path_denied"),
            ("bad_path",        hv(sig_path="/a//b", nonce="n12"),        "POST", "/a//b",           400, "bad_path"),
            ("skew",            hv(ts=_TS - 301, nonce="n13"),            "POST", "/koseki/ingest", 401, "skew"),
            ("bad_ts",          {**hv(nonce="n14"), "X-Sig-Timestamp": "NaN"}, "POST", "/koseki/ingest", 401, "bad_ts"),
            ("body_mismatch",   hv(csha=hashlib.sha256(b"OTHER").hexdigest(), nonce="n15"), "POST", "/koseki/ingest", 401, "body_mismatch"),
        ]
        for label, headers, method, path, exp_st, exp_rs in cases:
            with self.subTest(case=label):
                store = NonceStore()
                st, rs = verify_v1(headers, b"body", method, path, _REGISTRY, store, _TS)
                self.assertEqual((st, rs), (exp_st, exp_rs), label)

        # caller_mismatch（別途: caller を誤らせ署名も一致させる）
        cm_h = verify_headers("kid-1", "wrong-caller", "POST", "/koseki/ingest", good, _TS, "n09")
        st, rs = verify_v1(cm_h, b"body", "POST", "/koseki/ingest", _REGISTRY, NonceStore(), _TS)
        self.assertEqual((st, rs), (401, "caller_mismatch"))

        # bad_sig（署名を破壊）
        bs_h = verify_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", good, _TS, "n16")
        bs_h["X-Sig-Signature"] = "00" * 32
        st, rs = verify_v1(bs_h, b"body", "POST", "/koseki/ingest", _REGISTRY, NonceStore(), _TS)
        self.assertEqual((st, rs), (401, "bad_sig"))

        # replay（同一 nonce 2 回）
        store = NonceStore()
        rp_h = verify_headers("kid-1", "gas-koseki", "POST", "/koseki/ingest", good, _TS, "n17")
        self.assertEqual(verify_v1(rp_h, b"body", "POST", "/koseki/ingest", _REGISTRY, store, _TS), (200, "ok"))
        self.assertEqual(verify_v1(rp_h, b"body", "POST", "/koseki/ingest", _REGISTRY, store, _TS), (409, "replay"))


if __name__ == "__main__":
    unittest.main()
