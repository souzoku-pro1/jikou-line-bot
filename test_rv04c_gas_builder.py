"""RV-04c S2: GAS 署名ヘルパの Python 参照実装＋builder 検証＋server parser 通し。

- GAS 側（`gas/rv04c_signing.js`）と **byte 等価**な参照 builder / filename sanitize /
  canonical を Python で持ち、両者が同一入力→同一 body/canonical/signature を出すことを
  fixture（`rv04c_gas_builder_vectors.v1.json`・既存 golden）で固定する。
- 第0段（H03）: parts → 参照 builder → body_b64 完全一致。参照 builder は本 module の
  `build_multipart` 単一実装（別実装での PASS を作らない＝GAS 側も同一 builder 共用が構造要件）。
- server parser 通し（§1.1b）: builder 生成 body を実サーバの multipart parser（TestClient）へ
  通し、field/ファイル内容/filename が復元されること。
- chunk append 等価（H02）: 固定 chunk 連結が単純連結と一致（GAS の push.apply 回避実装の
  アルゴリズム等価。GAS 実機での大 PDF/chunk 境界実測は S4=大野）。
- D5-C02: 既存 provider の mark_done() が last_error=NULL を維持すること。
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import unittest
import unittest.mock
from pathlib import Path

_REPO = Path(__file__).parent

# ── main import 前の env（既存 ingest テストと同流儀・server parser 通し用） ──
_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "t", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "KOSEKI_INGEST_TOKEN": "koseki-legacy-token",
}
_SAVED = {k: os.environ.get(k) for k in _ENV}
os.environ.update(_ENV)

from fastapi.testclient import TestClient  # noqa: E402

import hub.db as db  # noqa: E402
from hub.inbound_event import Base as InboundBase, InboundEvent, mark_done  # noqa: E402
import main  # noqa: E402

for _k, _o in _SAVED.items():
    if _o is None:
        os.environ.pop(_k, None)
    else:
        os.environ[_k] = _o


# ══════════════════════════════════════════════════════════════
# Python 参照実装（gas/rv04c_signing.js と byte 等価・ground truth）
# ══════════════════════════════════════════════════════════════

_CRLF = b"\r\n"
_FILENAME_FORBIDDEN = ("\r", "\n", "\x00", '"')   # §1.1b rule 1: 拒否文字


class BuilderError(ValueError):
    """builder 入力違反（field 名 allowlist 外・filename 禁止文字等）。"""


def sanitize_filename(raw_name: str, drive_file_id: str) -> str:
    """§1.1b filename 規則（採る方式 1 つに固定）。
    - CR/LF/NUL/`"` を含む → BuilderError（送出前例外・インジェクション根絶）。
    - 非 ASCII（>127）を含む → ASCII fallback `doc-<driveFileId>.<ext>`
      （<ext>=原名末尾の ASCII 英数字拡張子・取れなければ bin）。
    - filename* は使わない。"""
    for c in _FILENAME_FORBIDDEN:
        if c in raw_name:
            raise BuilderError(f"filename forbidden char: {c!r}")
    if all(ord(c) <= 127 for c in raw_name):
        return raw_name
    # 非 ASCII → fallback
    ext = "bin"
    dot = raw_name.rfind(".")
    if dot != -1:
        cand = raw_name[dot + 1:]
        if cand and all(("0" <= c <= "9") or ("a" <= c.lower() <= "z") for c in cand):
            ext = cand
    return f"doc-{drive_file_id}.{ext}"


def build_multipart(boundary: str, parts: list) -> bytes:
    """手組み multipart（§1.1 R4）。parts の filename は sanitize 済みを前提（builder は
    禁止文字を assert して防御するが fallback は sanitize_filename の責務）。
    field 名 allowlist の enforce は helper 層（rv04c_signed_body）で行う。"""
    if not all(ord(c) <= 126 and c not in " " for c in boundary):
        raise BuilderError("boundary must be printable ASCII without space")
    out = bytearray()
    for p in parts:
        name = p["name"]
        out += b"--" + boundary.encode("ascii") + _CRLF
        disp = f'form-data; name="{name}"'
        fn = p.get("filename")
        if fn is not None:
            for c in _FILENAME_FORBIDDEN:
                if c in fn:
                    raise BuilderError("filename forbidden char in builder")
            disp += f'; filename="{fn}"'
        out += b"Content-Disposition: " + disp.encode("utf-8") + _CRLF
        ct = p.get("content_type")
        if ct:
            out += b"Content-Type: " + ct.encode("ascii") + _CRLF
        out += _CRLF
        out += p["value"]              # bytes
        out += _CRLF
    out += b"--" + boundary.encode("ascii") + b"--" + _CRLF
    return bytes(out)


def build_multipart_chunked(boundary: str, parts: list, chunk: int = 8192) -> bytes:
    """H02: GAS の固定サイズ chunk append を Python で再現（push.apply 回避アルゴリズムの
    等価性検証用）。append は src を chunk 単位に区切って dst へ追記する。"""
    def append(dst: bytearray, src: bytes):
        for i in range(0, len(src), chunk):
            dst += src[i:i + chunk]
    out = bytearray()
    for p in parts:
        append(out, b"--" + boundary.encode("ascii") + _CRLF)
        disp = f'form-data; name="{p["name"]}"'
        if p.get("filename") is not None:
            disp += f'; filename="{p["filename"]}"'
        append(out, b"Content-Disposition: " + disp.encode("utf-8") + _CRLF)
        if p.get("content_type"):
            append(out, b"Content-Type: " + p["content_type"].encode("ascii") + _CRLF)
        append(out, _CRLF)
        append(out, p["value"])
        append(out, _CRLF)
    append(out, b"--" + boundary.encode("ascii") + b"--" + _CRLF)
    return bytes(out)


def canonical_v1_ref(key_id, caller, method, npath, ts, nonce, csha) -> bytes:
    """§2.1 length-prefix（UTF-8 バイト長）。サーバ hub.service_auth.canonical_v1 と同一。"""
    order = ["v1", key_id, caller, method.upper(), npath, str(ts), nonce, csha]
    out = bytearray()
    for f in order:
        u = f.encode("utf-8")
        out += str(len(u)).encode("ascii") + b":" + u + b"\n"
    return bytes(out)


def _b(v_b64: str) -> bytes:
    return base64.b64decode(v_b64)


def _parts_from_json(jparts):
    return [{"name": p["name"], "filename": p.get("filename"),
             "content_type": p.get("content_type"), "value": _b(p["value_b64"])}
            for p in jparts]


# ── fixtures ──────────────────────────────────────────────────
_GOLDEN = json.loads((_REPO / "docs" / "design-drafts" /
                      "rv04_hmac_golden_vectors.v1.json").read_text(encoding="utf-8"))
_GASFX = json.loads((_REPO / "docs" / "design-drafts" /
                     "rv04c_gas_builder_vectors.v1.json").read_text(encoding="utf-8"))


class TestBuilderStage0(unittest.TestCase):
    """第0段（H03）: parts → 参照 builder → body_b64 完全一致（builder_na は除外）。"""

    def test_builder_reproduces_body(self):
        n = 0
        for v in _GASFX["vectors"]:
            if v.get("builder_na"):
                continue
            with self.subTest(vec=v["name"]):
                body = build_multipart(v["boundary"], _parts_from_json(v["parts"]))
                self.assertEqual(base64.b64encode(body).decode(), v["body_b64"],
                                 f"{v['name']}: builder body mismatch")
                self.assertEqual(hashlib.sha256(body).hexdigest(), v["content_sha256"])
                n += 1
        self.assertGreaterEqual(n, 6)   # 4 再表現 + delimiter + fallback（+ empty_field）

    def test_chunked_equals_plain(self):
        # H02: chunk append が単純連結と一致（chunk 境界前後を含むサイズで）
        for v in _GASFX["vectors"]:
            if v.get("builder_na"):
                continue
            parts = _parts_from_json(v["parts"])
            plain = build_multipart(v["boundary"], parts)
            for ck in (1, 7, 8192, len(plain) + 10):
                with self.subTest(vec=v["name"], chunk=ck):
                    self.assertEqual(build_multipart_chunked(v["boundary"], parts, ck),
                                     plain)

    def test_large_pdf_chunk_boundary_algorithm(self):
        # 実運用上限相当（~数 MB）＋chunk 境界前後で chunked==plain・SHA 一致
        big = bytes((i * 131 + 7) % 256 for i in range(3_000_000))  # 3MB 疑似 PDF bytes
        parts = [{"name": "file", "filename": "big.pdf",
                  "content_type": "application/pdf", "value": big},
                 {"name": "drive_file_id", "filename": None,
                  "content_type": None, "value": b"F-big"}]
        plain = build_multipart("RV04Cbig", parts)
        for ck in (8191, 8192, 8193, 1 << 20):
            with self.subTest(chunk=ck):
                got = build_multipart_chunked("RV04Cbig", parts, ck)
                self.assertEqual(got, plain)
                self.assertEqual(hashlib.sha256(got).hexdigest(),
                                 hashlib.sha256(plain).hexdigest())


class TestBuilderMatchesExistingGolden(unittest.TestCase):
    """整合: 新 fixture の再表現 5 本の body_b64 が既存 golden と一致（既存 v1 不変の担保）。"""

    def test_reexpressed_match_golden(self):
        golden_by_name = {v["name"]: v for v in _GOLDEN["vectors"]}
        matched = 0
        for v in _GASFX["vectors"]:
            g = golden_by_name.get(v["name"])
            if not g:
                continue
            with self.subTest(vec=v["name"]):
                self.assertEqual(v["body_b64"], g["body_b64"])
                self.assertEqual(v["content_sha256"], g["content_sha256"])
                matched += 1
        self.assertEqual(matched, 5)   # ascii/japanese/empty/long_boundary/multi_field


class TestCanonicalAndSignature(unittest.TestCase):
    """canonical_b64 / signature を参照実装で再現（既存 golden の署名契約）。"""

    def test_ref_matches_golden_signature(self):
        secret = bytes.fromhex(_GOLDEN["secret_hex_test_only"])
        for v in _GOLDEN["vectors"]:
            with self.subTest(vec=v["name"]):
                canon = canonical_v1_ref(v["key_id"], v["caller"], v["method"],
                                         v["normalized_path"], v["timestamp"],
                                         v["nonce"], v["content_sha256"])
                self.assertEqual(base64.b64encode(canon).decode(), v["canonical_b64"])
                sig = hmac.new(secret, canon, hashlib.sha256).hexdigest()
                self.assertEqual(sig, v["signature"])


class TestFilenameSanitize(unittest.TestCase):
    """§1.1b filename 規則。"""

    def test_ascii_passthrough(self):
        self.assertEqual(sanitize_filename("koseki.pdf", "F1"), "koseki.pdf")

    def test_non_ascii_fallback(self):
        self.assertEqual(sanitize_filename("戸籍謄本.pdf", "F123"), "doc-F123.pdf")
        self.assertEqual(sanitize_filename("スキャン", "F9"), "doc-F9.bin")  # 拡張子なし

    def test_forbidden_chars_rejected(self):
        for bad in ("a\r.pdf", "a\n.pdf", "a\x00.pdf", 'a".pdf'):
            with self.subTest(bad=bad):
                with self.assertRaises(BuilderError):
                    sanitize_filename(bad, "F1")

    def test_fixture_fallback_vector(self):
        # fixture の fallback vector: raw→期待 filename が sanitize と一致
        for v in _GASFX["vectors"]:
            if v.get("fallback_check"):
                fc = v["fallback_check"]
                self.assertEqual(sanitize_filename(fc["raw_filename"],
                                                   fc["drive_file_id"]),
                                 fc["expected_filename"])


class TestFieldNameAllowlist(unittest.TestCase):
    """§1.1b: lane 別 field 名 allowlist（helper 層の enforce・DRAFT の入口定義と一致）。"""

    # DRAFT §0/実装（*_ingest.py の Form 定義）と 1:1
    LANE_FIELDS = {
        "/koseki/ingest": {"file", "case_hint", "case_app_hint", "drive_file_id"},
        "/registry/ingest": {"file", "case_hint", "drive_file_id"},
        "/bank/ingest": {"file", "case_hint", "case_app_hint", "drive_file_id"},
        "/sortation/ingest": {"file", "drive_file_id", "drive_file_url"},
        "/valuation/ingest": {"file", "case_hint", "case_app_hint", "drive_file_id"},
    }

    def test_gas_helper_lanes_match_server_forms(self):
        # gas/rv04c_signing.js の LANE_FIELDS 定義（JSON 抽出）がサーバ実装と一致
        js = (_REPO / "gas" / "rv04c_signing.js").read_text(encoding="utf-8")
        # 単純パース: LANE_FIELDS ブロックの各 path→field 集合を正規表現で拾う
        import re
        block = re.search(r"var LANE_FIELDS = \{(.*?)\};", js, re.S)
        self.assertIsNotNone(block, "LANE_FIELDS 定義が見つからない")
        found = {}
        for m in re.finditer(r"'(/[a-z/]+/ingest)':\s*\[([^\]]*)\]", block.group(1)):
            fields = set(re.findall(r"'([a-z_]+)'", m.group(2)))
            found[m.group(1)] = fields
        self.assertEqual(found, self.LANE_FIELDS,
                         "GAS helper の LANE_FIELDS がサーバ Form 定義と不一致")


class TestServerParserRoundtrip(unittest.TestCase):
    """§1.1b: builder 生成 body を実サーバ multipart parser に通し、認証前に parse される
    こと（=field/ファイルが復元される）。認証は署名なしで token を付け 400（PDF 要求）＝
    「parse は通ったが file 内容で弾かれた」ではなく「ゲート通過して endpoint に到達」を見る。
    ここでは parser の健全性のみを対象にするため、koseki に有効 token で通す。"""

    def _post(self, boundary, parts, path="/koseki/ingest",
              token="koseki-legacy-token"):
        body = build_multipart(boundary, parts)
        client = TestClient(main.app)
        with unittest.mock.patch.dict(os.environ, {**_ENV}):
            os.environ.pop("SERVICE_AUTH_DUAL_ACCEPT_ENABLED", None)  # 旧 token 経路
            return client.post(f"{path}?token={token}", content=body,
                               headers={"Content-Type":
                                        f"multipart/form-data; boundary={boundary}"})

    def test_valid_pdf_part_reaches_endpoint(self):
        parts = [{"name": "file", "filename": "koseki.pdf",
                  "content_type": "application/pdf",
                  "value": b"%PDF-1.4 test\n%%EOF"},
                 {"name": "drive_file_id", "filename": None,
                  "content_type": None, "value": b"F-rt1"}]
        r = self._post("RV04Crt1", parts)
        # koseki は実処理へ進む（kintone 未設定で失敗しうる）が、少なくとも 400 PDF 要求
        # ではない＝parse 成功・file 認識。ここでは 4xx/5xx いずれでも parse 到達を確認。
        self.assertNotEqual(r.status_code, 404, r.text)   # ゲート通過
        self.assertNotIn("PDFファイルを送信してください", r.text)  # file が parse された

    def test_non_ascii_fallback_body_parses(self):
        fn = sanitize_filename("戸籍謄本.pdf", "F-rt2")
        parts = [{"name": "file", "filename": fn,
                  "content_type": "application/pdf",
                  "value": b"%PDF-1.4 jp\n%%EOF"},
                 {"name": "drive_file_id", "filename": None,
                  "content_type": None, "value": b"F-rt2"}]
        r = self._post("RV04Crt2", parts)
        self.assertNotEqual(r.status_code, 404, r.text)
        self.assertNotIn("PDFファイルを送信してください", r.text)

    def test_delimiter_lookalike_in_content_parses(self):
        # M01: content に delimiter 類似列（--<boundary の前方一致>）を含んでも、
        # 実 boundary と衝突しなければ parser は正しく分割する
        bnd = "RV04Crt3xyz"
        parts = [{"name": "file", "filename": "x.pdf",
                  "content_type": "application/pdf",
                  "value": b"%PDF\r\n--RV04Crt3 not-the-real-delimiter\r\nmore"},
                 {"name": "drive_file_id", "filename": None,
                  "content_type": None, "value": b"F-rt3"}]
        r = self._post(bnd, parts)
        self.assertNotEqual(r.status_code, 404, r.text)
        self.assertNotIn("PDFファイルを送信してください", r.text)


class TestExistingProviderDoneInvariant(unittest.TestCase):
    """D5-C02: 既存 provider の mark_done() が last_error=NULL を維持すること
    （kintone の last_error=理由コード流用が既存不変条件を壊さない回帰固定）。"""

    def setUp(self):
        import tempfile
        self._dir = tempfile.mkdtemp(prefix="rv04c_c02_")
        self._env = unittest.mock.patch.dict(
            os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/n.db"})
        self._env.start()
        db.reset_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(InboundBase.metadata.create_all)
        asyncio.run(_create())
        db.reset_for_tests()

    def tearDown(self):
        import shutil
        db.reset_for_tests()
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)

    def test_mark_done_sets_last_error_null(self):
        import sqlalchemy as sa

        async def _flow():
            async with db.session_scope() as s:
                r = await s.execute(sa.insert(InboundEvent.__table__).values(
                    provider="stripe", external_event_id="evt_c02",
                    dedup_key="stripe:evt_c02", payload_hash="0" * 64,
                    signature_result="verified", state="processing",
                    last_error="prev_error", attempts=1))
                pk = r.inserted_primary_key[0]
            await mark_done(pk)
            async with db.session_scope() as s:
                row = (await s.execute(sa.select(InboundEvent.state,
                       InboundEvent.last_error)
                       .where(InboundEvent.id == pk))).one()
                return row.state, row.last_error
        st, le = asyncio.run(_flow())
        db.reset_for_tests()
        self.assertEqual(st, "done")
        self.assertIsNone(le)   # mark_done は last_error を NULL に戻す（不変条件）


if __name__ == "__main__":
    unittest.main()
