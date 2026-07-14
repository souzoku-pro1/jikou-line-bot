"""RV-04a: hub/service_auth.py（NM01 v1 HMAC 検証コア）の単体テスト（§6.3 必須一覧）。

- §6.1/§6.2 status–reason table（パラメータ化・DB 経由の 8 段まで）
- caller 不一致 / version 欠落・不正 / timestamp 形式不正 / future skew・±300 境界 /
  required header 欠落 / canonical golden（fixture 照合）/ boundary 衝突・filename quote・CRLF /
  nonce 永続化（DB・再接続で同一 nonce 拒否）/ retiring warning + body sentinel 否定
- key registry の env パース + 起動時検証 / nonce の memory fallback 不在（ソース検査）
"""

import base64
import hashlib
import json
import os
import re
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import sqlalchemy as sa  # noqa: E402

import hub.db as db  # noqa: E402
from hub import service_auth as svc  # noqa: E402

_REPO = Path(__file__).resolve().parent
TS = 1_700_000_000
SECRET_HEX = "ab" * 32              # テスト専用 secret（32byte）
SECRET = bytes.fromhex(SECRET_HEX)


def _key(status="active", not_before=0, expires_at=2 ** 31,
         paths=("/koseki/ingest", "/koseki/ingest2"), methods=("POST",)):
    return {"secret": SECRET_HEX, "caller": "gas-koseki",
            "allowed_methods": list(methods), "allowed_paths": list(paths),
            "not_before": not_before, "expires_at": expires_at, "status": status}


_REG_JSON = json.dumps({
    "kid-1": _key("active"),
    "kid-retiring": _key("retiring"),
    "kid-revoked": _key("revoked"),
    "kid-expired": _key("active", expires_at=TS - 1),
    "kid-future": _key("active", not_before=TS + 1),
})
REGISTRY = svc.parse_registry(_REG_JSON)


_HEX32 = re.compile(r"[0-9a-fA-F]{32}")


def _mknonce(seed):
    """テスト seed を 128bit hex nonce に正規化（H-02 のヘッダ検証を通す）。"""
    s = str(seed)
    return s if _HEX32.fullmatch(s) else hashlib.sha256(s.encode()).hexdigest()[:32]


def _sign(key_id, caller, method, path, body, ts, nonce, secret=SECRET, version="v1"):
    nonce = _mknonce(nonce)   # 128bit hex 固定（署名対象と一致・§2.2）
    csha = hashlib.sha256(body).hexdigest()
    canon = svc.canonical_v1(key_id, caller, method, path, str(ts), nonce, csha)
    return {"X-Sig-Version": version, "X-Sig-Key-Id": key_id, "X-Sig-Caller": caller,
            "X-Sig-Timestamp": str(ts), "X-Sig-Nonce": nonce,
            "X-Sig-Content-SHA256": csha,
            "X-Sig-Signature": svc.sign_v1(secret, canon)}


def _vsig(headers, body, method, path, now=TS, skew=300):
    return svc.verify_signature(headers, body, method, path, REGISTRY, now, skew)


def _mp(boundary, parts):
    out = b""
    for name, filename, ctype, data in parts:
        out += f"--{boundary}\r\n".encode()
        disp = f'Content-Disposition: form-data; name="{name}"'
        if filename is not None:
            disp += f'; filename="{filename}"'
        out += (disp + "\r\n").encode()
        if ctype:
            out += f"Content-Type: {ctype}\r\n".encode()
        out += b"\r\n" + data + b"\r\n"
    return out + f"--{boundary}--\r\n".encode()


_PDF = b"%PDF-1.4\n" + b"\x00\x01\x02\x03bin \xff\xfe body\n" * 20 + b"%%EOF"


# ── key registry の env パース + 起動時検証 ─────────────────────────────────
class TestRegistryParsing(unittest.TestCase):
    def test_valid_entry_fields(self):
        k = REGISTRY["kid-1"]
        self.assertEqual(k.caller, "gas-koseki")
        self.assertEqual(k.secret, SECRET)
        self.assertIn("POST", k.allowed_methods)
        self.assertIn("/koseki/ingest", k.allowed_paths)
        self.assertEqual(k.status, "active")

    def test_malformed_rejected(self):
        bad = [
            '{"k":{"secret":"zz","caller":"c","allowed_methods":["POST"],'
            '"allowed_paths":["/x"],"expires_at":1,"status":"active"}}',            # bad hex
            '{"k":{"secret":"' + "ab" * 8 + '","caller":"c","allowed_methods":["POST"],'
            '"allowed_paths":["/x"],"expires_at":1,"status":"active"}}',            # secret < 32byte
            '{"k":{"secret":"' + SECRET_HEX + '","caller":"c","allowed_methods":["POST"],'
            '"allowed_paths":["/x"],"expires_at":1,"status":"bogus"}}',             # bad status
            '{"k":{"secret":"' + SECRET_HEX + '","caller":"c","allowed_methods":["POST"],'
            '"allowed_paths":["/x"],"status":"active"}}',                           # missing expires_at
            "not json",                                                             # parse error
            '["not","object"]',                                                     # not an object
        ]
        for j in bad:
            with self.subTest(j=j[:40]):
                with self.assertRaises(svc.ServiceAuthConfigError):
                    svc.parse_registry(j)

    def test_load_from_env(self):
        with patch.dict(os.environ, {svc._REGISTRY_ENV: ""}, clear=False):
            self.assertEqual(svc.load_registry_from_env(), {})              # 未設定は空
            with self.assertRaises(svc.ServiceAuthConfigError):
                svc.load_registry_from_env(required=True)                   # required で例外
        with patch.dict(os.environ, {svc._REGISTRY_ENV: _REG_JSON}, clear=False):
            reg = svc.load_registry_from_env()
            self.assertIn("kid-1", reg)


# ── §6.1/§6.2 status–reason table（sync 1〜7 段は verify_signature で網羅） ──
class TestStatusReasonTableSync(unittest.TestCase):
    def test_table(self):
        good = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "n-ok")
        cases = [
            ("active_ok", good, "POST", "/koseki/ingest", b"body", 200, "ok"),
            ("retiring", _sign("kid-retiring", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "n-r"),
             "POST", "/koseki/ingest", b"body", 200, "ok_retiring"),
            ("no_signature", {}, "POST", "/koseki/ingest", b"body", 401, "no_signature"),
            ("bad_version", {**good, "X-Sig-Version": "v2"}, "POST", "/koseki/ingest", b"body", 401, "bad_version"),
            ("key_unknown", _sign("kid-x", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "n1"),
             "POST", "/koseki/ingest", b"body", 401, "key_unknown"),
            ("key_revoked", _sign("kid-revoked", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "n2"),
             "POST", "/koseki/ingest", b"body", 401, "key_revoked"),
            ("key_not_yet", _sign("kid-future", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "n3"),
             "POST", "/koseki/ingest", b"body", 401, "key_not_yet_valid"),
            ("key_expired", _sign("kid-expired", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "n4"),
             "POST", "/koseki/ingest", b"body", 401, "key_expired"),
            ("caller_mismatch", _sign("kid-1", "wrong", "POST", "/koseki/ingest", b"body", TS, "n5"),
             "POST", "/koseki/ingest", b"body", 401, "caller_mismatch"),
            ("method_denied", _sign("kid-1", "gas-koseki", "DELETE", "/koseki/ingest", b"body", TS, "n6"),
             "DELETE", "/koseki/ingest", b"body", 403, "method_denied"),
            ("path_denied", _sign("kid-1", "gas-koseki", "POST", "/not/allowed", b"body", TS, "n7"),
             "POST", "/not/allowed", b"body", 403, "path_denied"),
            ("bad_path", _sign("kid-1", "gas-koseki", "POST", "/a//b", b"body", TS, "n8"),
             "POST", "/a//b", b"body", 400, "bad_path"),
            ("bad_path_pct", _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "n8b"),
             "POST", "/koseki%2Fingest", b"body", 400, "bad_path"),
            ("skew", _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS - 301, "n9"),
             "POST", "/koseki/ingest", b"body", 401, "skew"),
            ("bad_ts", {**good, "X-Sig-Timestamp": "NaN"}, "POST", "/koseki/ingest", b"body", 401, "bad_ts"),
            ("body_mismatch", good, "POST", "/koseki/ingest", b"TAMPERED", 401, "body_mismatch"),
        ]
        for label, h, method, path, body, exp_st, exp_rs in cases:
            with self.subTest(case=label):
                st, rs, _ = _vsig(h, body, method, path)
                self.assertEqual((st, rs), (exp_st, exp_rs), label)

    def test_bad_sig(self):
        h = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "nbs")
        h["X-Sig-Signature"] = "00" * 32
        st, rs, _ = _vsig(h, b"body", "POST", "/koseki/ingest")
        self.assertEqual((st, rs), (401, "bad_sig"))


# ── skew ±300 境界（両側） ───────────────────────────────────────────────────
class TestSkewBoundary(unittest.TestCase):
    def test_boundaries(self):
        for delta, ok in [(300, True), (301, False), (-300, True), (-301, False)]:
            with self.subTest(delta=delta):
                ts = TS + delta
                h = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"b", ts, f"skew{delta}")
                st, rs, _ = _vsig(h, b"b", "POST", "/koseki/ingest", now=TS, skew=300)
                if ok:
                    self.assertEqual((st, rs), (200, "ok"))
                else:
                    self.assertEqual((st, rs), (401, "skew"))


# ── required header 欠落（H-02: 第1段で missing_header・bad_sig 任せにしない） ──
class TestRequiredHeaders(unittest.TestCase):
    def test_each_missing_header_is_missing_header(self):
        base = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"b", TS, "reqh")
        for drop in svc._REQUIRED_HEADERS:
            with self.subTest(drop=drop):
                h = {k: v for k, v in base.items() if k != drop}
                st, rs, ctx = _vsig(h, b"b", "POST", "/koseki/ingest")
                self.assertEqual((st, rs), (401, "missing_header"), drop)
                self.assertIsNone(ctx)

    def test_empty_header_value_is_missing_header(self):
        base = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"b", TS, "reqh2")
        for empty in svc._REQUIRED_HEADERS:
            with self.subTest(empty=empty):
                h = {**base, empty: ""}
                st, rs, _ = _vsig(h, b"b", "POST", "/koseki/ingest")
                self.assertEqual((st, rs), (401, "missing_header"), empty)


# ── H-02: nonce 形式（128bit hex 固定・bad_nonce） ──────────────────────────
class TestNonceFormat(unittest.TestCase):
    def _hdr_with_raw_nonce(self, nonce):
        # 生の nonce 値でヘッダを組む（署名も同じ nonce で作る＝bad_sig ではなく
        # bad_nonce を狙う）。_sign を通さず手組みして正規化を回避する。
        body = b"b"
        csha = hashlib.sha256(body).hexdigest()
        canon = svc.canonical_v1("kid-1", "gas-koseki", "POST", "/koseki/ingest",
                                 str(TS), nonce, csha)
        return {"X-Sig-Version": "v1", "X-Sig-Key-Id": "kid-1",
                "X-Sig-Caller": "gas-koseki", "X-Sig-Timestamp": str(TS),
                "X-Sig-Nonce": nonce, "X-Sig-Content-SHA256": csha,
                "X-Sig-Signature": svc.sign_v1(SECRET, canon)}

    def test_bad_nonce_forms_rejected(self):
        bad = [
            "short",                              # 短い・非 hex
            "z" * 32,                             # 32 文字だが非 hex
            "ab" * 8,                             # 16 文字（64bit・長さ不足）
            "ab" * 32,                            # 64 文字（256bit・長すぎ）
            "0123456789abcdef0123456789abcde",    # 31 文字
            "0123456789abcdef0123456789abcdef0",  # 33 文字
        ]
        for n in bad:
            with self.subTest(nonce=n):
                st, rs, ctx = _vsig(self._hdr_with_raw_nonce(n), b"b", "POST", "/koseki/ingest")
                self.assertEqual((st, rs), (401, "bad_nonce"), n)
                self.assertIsNone(ctx)

    def test_valid_128bit_hex_accepted(self):
        for n in ["0123456789abcdef0123456789abcdef", "AB" * 16, "0" * 32]:
            with self.subTest(nonce=n):
                st, rs, _ = _vsig(self._hdr_with_raw_nonce(n), b"b", "POST", "/koseki/ingest")
                self.assertEqual((st, rs), (200, "ok"), n)


# ── H-01: KeyEntry.secret が repr / str / %r に出ない ───────────────────────
class TestSecretNotInRepr(unittest.TestCase):
    def test_secret_masked_everywhere(self):
        entry = REGISTRY["kid-1"]
        forms = [repr(entry), str(entry), f"{entry!r}", "%r" % (entry,),
                 repr(REGISTRY), f"{REGISTRY!r}"]
        for s in forms:
            with self.subTest(form=s[:40]):
                self.assertNotIn(SECRET_HEX, s)           # hex 表現なし
                self.assertNotIn(SECRET.hex(), s)
                self.assertNotIn(str(SECRET), s)          # bytes repr（b'...'）なし
                self.assertNotIn("\\xab", s)              # バイト列断片なし
                self.assertIn("<redacted>", s)            # マスクが入っている


# ── M-01: raw_path 境界を本番モジュール（effective_signed_path / verify）で固定 ──
class TestRawPathProduction(unittest.TestCase):
    def test_effective_signed_path_from_scope(self):
        self.assertEqual(svc.effective_signed_path({"raw_path": b"/koseki/ingest"}),
                         "/koseki/ingest")
        self.assertIsNone(svc.effective_signed_path({}))               # 欠落=fail-closed
        self.assertIsNone(svc.effective_signed_path({"raw_path": None}))

    def test_mount_prefix_strip_keeps_raw_bytes(self):
        # prefix 除去後も生バイト基準（%2F は decode されない）
        self.assertEqual(
            svc.effective_signed_path({"raw_path": b"/mnt/koseki/ingest"}, prefix="/mnt"),
            "/koseki/ingest")
        self.assertEqual(
            svc.effective_signed_path({"raw_path": b"/mnt/koseki%2Fingest"}, prefix="/mnt"),
            "/koseki%2Fingest")   # 生バイト保持（後段 normalize が %2F を 400 で弾く）

    def test_malformed_raw_path_400_via_verify(self):
        for rp in ["/koseki%2Fingest", "/a/%2e%2e/b", "/koseki/%252F",
                   "/a//b", "/koseki/／ingest"]:
            with self.subTest(rp=rp):
                # decode 後の許可 path で正しく署名しても、生 raw_path 基準で 400 bad_path
                h = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"b", TS, "rp" + rp)
                st, rs, ctx = _vsig(h, b"b", "POST", rp)
                self.assertEqual((st, rs), (400, "bad_path"), rp)
                self.assertIsNone(ctx)

    def test_normalize_path_unit(self):
        self.assertIsNone(svc.normalize_path("/koseki%2fingest"))
        self.assertIsNone(svc.normalize_path("/a/../b"))
        self.assertIsNone(svc.normalize_path("/a//b"))
        self.assertIsNone(svc.normalize_path("/koseki/／ingest"))
        self.assertIsNone(svc.normalize_path(None))
        self.assertEqual(svc.normalize_path("/koseki/ingest/"), "/koseki/ingest")


# ── canonical golden（fixture 照合・cross-language） ─────────────────────────
class TestGoldenVectors(unittest.TestCase):
    def test_python_matches_fixture(self):
        fx = json.loads((_REPO / "docs" / "design-drafts" /
                         "rv04_hmac_golden_vectors.v1.json").read_text(encoding="utf-8"))
        secret = bytes.fromhex(fx["secret_hex_test_only"])
        self.assertGreaterEqual(len(fx["vectors"]), 5)
        for v in fx["vectors"]:
            with self.subTest(vec=v["name"]):
                body = base64.b64decode(v["body_b64"])
                self.assertEqual(hashlib.sha256(body).hexdigest(), v["content_sha256"])
                canon = svc.canonical_v1(v["key_id"], v["caller"], v["method"],
                                         v["normalized_path"], str(v["timestamp"]),
                                         v["nonce"], v["content_sha256"])
                self.assertEqual(base64.b64encode(canon).decode(), v["canonical_b64"])
                self.assertEqual(svc.sign_v1(secret, canon), v["signature"])


# ── multipart 生 body 形状（boundary 衝突 / filename quote / CRLF） ──────────
class TestMultipartShapes(unittest.TestCase):
    def _accept(self, body, path="/koseki/ingest"):
        h = _sign("kid-1", "gas-koseki", "POST", path, body, TS, hashlib.sha256(body).hexdigest()[:24])
        return _vsig(h, body, "POST", path)

    def test_boundary_collision_in_content(self):
        bnd = "BND-COLLIDE"
        # body 内容に boundary 類似列を含めても、生 body 全体の hash 基準で成立
        body = _mp(bnd, [("file", "x.pdf", "application/pdf",
                          b"--BND-COLLIDE not-a-real-part\r\n" + _PDF)])
        st, rs, ctx = self._accept(body)
        self.assertEqual((st, rs), (200, "ok"))
        self.assertIsNotNone(ctx)

    def test_filename_quote_escape(self):
        body = _mp("BND-Q", [("file", 'a"b;c.pdf', "application/pdf", _PDF)])
        st, rs, _ = self._accept(body)
        self.assertEqual((st, rs), (200, "ok"))

    def test_crlf_and_empty_and_multifield(self):
        for body in [_mp("BND-CRLF", [("file", "x.pdf", "application/pdf", b"a\r\nb\r\nc")]),
                     b"",  # 空 body
                     _mp("BND-M", [("meta", None, "application/json", b'{"k":1}'),
                                   ("file", "謄本.pdf", "application/pdf", _PDF)])]:
            with self.subTest(n=len(body)):
                st, rs, _ = self._accept(body)
                self.assertEqual((st, rs), (200, "ok"))


# ── memory fallback 不在（ソース検査） ──────────────────────────────────────
class TestNoMemoryFallback(unittest.TestCase):
    def test_nonce_store_is_db_only(self):
        src = (_REPO / "hub" / "service_auth.py").read_text(encoding="utf-8")
        # consume_nonce は DB（session_scope + signature_nonce）で実装されていること
        idx = src.index("async def consume_nonce")
        body = src[idx:src.index("async def verify_request")]
        self.assertIn("session_scope", body)
        self.assertIn("signature_nonce", body)
        # process-memory な nonce 集合を持たないこと（PoC の NonceStore/_seen は移植しない）
        self.assertNotIn("_seen", src)
        self.assertNotIn("class NonceStore", src)


# ── DB を要する検証（nonce 永続化・replay・retiring warning） ───────────────
class _SqliteDbMixin(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="svc_auth_test_")
        self._url = f"sqlite+aiosqlite:///{self._dir}/test.db"
        self._env = patch.dict(os.environ, {"DATABASE_URL": self._url})
        self._env.start()
        db.reset_for_tests()
        import asyncio

        async def _create():
            engine = db.get_async_engine()
            async with engine.begin() as conn:
                await conn.run_sync(svc.metadata.create_all)
        asyncio.run(_create())
        db.reset_for_tests()

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)


class TestNoncePersistenceDB(_SqliteDbMixin):
    def _verify(self, headers, body, method, path):
        import asyncio
        r = asyncio.run(svc.verify_request(headers, body, method, path,
                                           registry=REGISTRY, now=TS, skew=300))
        db.reset_for_tests()
        return r

    def test_replay_across_reconnect(self):
        # 同一 nonce の 2 回目は、エンジン破棄（=再起動相当・新接続）を挟んでも 409 replay
        h = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "persist-nonce")
        self.assertEqual(self._verify(h, b"body", "POST", "/koseki/ingest"), (200, "ok"))
        # reset_for_tests() で engine 破棄済み → 次回は新規接続だが nonce は DB に残る
        self.assertEqual(self._verify(h, b"body", "POST", "/koseki/ingest"), (409, "replay"))

    def test_distinct_nonce_accepted(self):
        for i in range(3):
            h = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, f"nonce-{i}")
            self.assertEqual(self._verify(h, b"body", "POST", "/koseki/ingest"), (200, "ok"))

    def test_db_row_written(self):
        import asyncio
        h = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "row-check")
        asyncio.run(svc.verify_request(h, b"body", "POST", "/koseki/ingest",
                                       registry=REGISTRY, now=TS, skew=300))
        db.reset_for_tests()

        async def _fetch():
            async with db.session_scope() as s:
                return (await s.execute(sa.select(svc.signature_nonce))).mappings().all()
        rows = asyncio.run(_fetch())
        db.reset_for_tests()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["nonce"], h["X-Sig-Nonce"])   # DB に格納された nonce=送信 nonce
        self.assertEqual(rows[0]["key_id"], "kid-1")


class TestRetiringWarningDB(_SqliteDbMixin):
    def test_retiring_warns_once_no_sensitive(self):
        import asyncio
        h = _sign("kid-retiring", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, "ret-warn")
        with self.assertLogs("hub.service_auth", level="WARNING") as cm:
            r = asyncio.run(svc.verify_request(h, b"body", "POST", "/koseki/ingest",
                                               registry=REGISTRY, now=TS, skew=300))
        db.reset_for_tests()
        self.assertEqual(r, (200, "ok_retiring"))
        warns = [x for x in cm.output if "retiring key accepted" in x]
        self.assertEqual(len(warns), 1)
        joined = "\n".join(cm.output)
        self.assertIn("kid-retiring", joined)            # key_id 可視
        self.assertIn("gas-koseki", joined)              # caller 可視
        # body sentinel 否定（DEFER L02）: 署名/nonce/content hash/secret/body が乗らない
        self.assertNotIn(h["X-Sig-Signature"], joined)
        self.assertNotIn(h["X-Sig-Nonce"], joined)
        self.assertNotIn(h["X-Sig-Content-SHA256"], joined)
        self.assertNotIn(SECRET_HEX, joined)


# ── L-01: nonce 同時競合（UNIQUE 違反で片方が敗者＝409） ─────────────────────
class TestNonceConcurrency(_SqliteDbMixin):
    _NOW = datetime.fromtimestamp(TS, tz=timezone.utc)
    _EXP = datetime.fromtimestamp(TS + 300, tz=timezone.utc)

    def test_same_nonce_second_consume_is_false(self):
        import asyncio
        nonce = "cc" + "0" * 30    # 128bit hex

        async def _flow():
            first = await svc.consume_nonce(nonce, "kid-1", "gas-koseki", self._EXP, now=self._NOW)
            second = await svc.consume_nonce(nonce, "kid-1", "gas-koseki", self._EXP, now=self._NOW)
            return first, second
        first, second = asyncio.run(_flow())
        db.reset_for_tests()
        # 先着＝True・後着は UNIQUE 違反を握って False（例外は verify 側で 409 に写る）
        self.assertEqual((first, second), (True, False))

    def test_direct_unique_violation_yields_409(self):
        import asyncio
        nonce = "dd" + "0" * 30

        async def _preinsert():   # 先着（勝者）を直接 INSERT
            async with db.session_scope() as s:
                await s.execute(sa.insert(svc.signature_nonce).values(
                    nonce=nonce, key_id="kid-1", caller="gas-koseki",
                    seen_at=self._NOW, expires_at=self._EXP))
        asyncio.run(_preinsert())
        db.reset_for_tests()
        # 敗者: 同一 nonce で verify_request → UNIQUE 違反→False→409 replay
        h = _sign("kid-1", "gas-koseki", "POST", "/koseki/ingest", b"body", TS, nonce)
        self.assertEqual(h["X-Sig-Nonce"], nonce)   # nonce が先着と一致していること
        r = asyncio.run(svc.verify_request(h, b"body", "POST", "/koseki/ingest",
                                           registry=REGISTRY, now=TS, skew=300))
        db.reset_for_tests()
        self.assertEqual(r, (409, "replay"))


# NB: signature_nonce migration の up/down 往復（実 sqlite）は、alembic 起動を
# 許可された test_db_foundation.py 側に置く（D2: 本ファイルからは alembic を
# import/subprocess 起動しない）。


if __name__ == "__main__":
    unittest.main()
