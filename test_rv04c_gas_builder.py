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


import re as _re

_DRIVE_ID_RE = _re.compile(r"[A-Za-z0-9_-]{1,128}")


def validate_drive_id(drive_file_id) -> str:
    """M01: fallback へ埋め込む driveFileId を送出前検証（gas validateDriveId_ と等価）。
    固定文字集合 [A-Za-z0-9_-]・長さ 1..128。欠落/非 ASCII/CR/LF/quote は BuilderError。
    fullmatch で末尾改行も弾く（JS の /^...$/ と同等挙動）。"""
    if drive_file_id is None or drive_file_id == "":
        raise BuilderError("driveFileId missing")
    if not isinstance(drive_file_id, str) or not _DRIVE_ID_RE.fullmatch(drive_file_id):
        raise BuilderError("driveFileId invalid charset/length")
    return drive_file_id


def sanitize_filename(raw_name: str, drive_file_id: str) -> str:
    """§1.1b filename 規則（採る方式 1 つに固定）。
    - CR/LF/NUL/`"` を含む → BuilderError（送出前例外・インジェクション根絶）。
    - 非 ASCII（>127）を含む → ASCII fallback `doc-<driveFileId>.<ext>`（M01: driveFileId
      を validate_drive_id で検証してから埋め込む）。
    - filename* は使わない。"""
    for c in _FILENAME_FORBIDDEN:
        if c in raw_name:
            raise BuilderError(f"filename forbidden char: {c!r}")
    if all(ord(c) <= 127 for c in raw_name):
        return raw_name
    validate_drive_id(drive_file_id)   # M01: fallback 埋め込み前に検証
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
        # GAS selftest（gas/rv04c_selftest.js:96）の signed byte 式 ((i*131+7)%256)-128 と
        # **同一バイト列**（signed→8bit で +128 mod 256）。変更時は両側同時に。合成値自体に
        # 意味はなく、cross-machine で一致することのみが要件。
        big = bytes(((i * 131 + 7) + 128) % 256 for i in range(3_000_000))  # 3MB 疑似 PDF bytes
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


# H02: 隔離テスト endpoint（本番ルーティング非接触）で FastAPI 復元後の field/filename/
# file bytes を捕捉し、builder 入力と**完全一致**を assert する。
from fastapi import FastAPI, File, Form, UploadFile  # noqa: E402

_echo_app = FastAPI()


@_echo_app.post("/echo")
async def _echo(file: UploadFile | None = File(default=None),
                drive_file_id: str | None = Form(default=None),
                case_hint: str | None = Form(default=None),
                drive_file_url: str | None = Form(default=None)):
    data = await file.read() if file is not None else b""
    return {"filename": file.filename if file is not None else None,
            "file_hex": data.hex(), "drive_file_id": drive_file_id,
            "case_hint": case_hint, "drive_file_url": drive_file_url}


_echo_client = TestClient(_echo_app)


class TestServerParserRoundtrip(unittest.TestCase):
    """§1.1b（H02）: builder 生成 body を FastAPI multipart parser に通し、復元後の
    field 値・filename・file bytes が builder 入力と **byte 完全一致**することを assert する。
    隔離 endpoint（/echo）を使い本番ルーティングに非接触。"""

    def _roundtrip(self, boundary, parts):
        body = build_multipart(boundary, parts)
        return _echo_client.post("/echo", content=body, headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}"})

    def test_field_and_file_bytes_restored_exactly(self):
        parts = [{"name": "file", "filename": "koseki.pdf",
                  "content_type": "application/pdf",
                  "value": b"%PDF-1.4 test\n%%EOF"},
                 {"name": "drive_file_id", "filename": None,
                  "content_type": None, "value": b"F-rt1"}]
        j = self._roundtrip("RV04Crt1", parts).json()
        self.assertEqual(j["filename"], "koseki.pdf")            # filename 完全一致
        self.assertEqual(j["file_hex"], b"%PDF-1.4 test\n%%EOF".hex())  # file bytes 一致
        self.assertEqual(j["drive_file_id"], "F-rt1")           # field 値一致

    def test_non_ascii_fallback_filename_restored(self):
        fn = sanitize_filename("戸籍謄本.pdf", "F-rt2")           # doc-F-rt2.pdf
        parts = [{"name": "file", "filename": fn,
                  "content_type": "application/pdf", "value": b"%PDF jp\n%%EOF"},
                 {"name": "drive_file_id", "filename": None,
                  "content_type": None, "value": b"F-rt2"}]
        j = self._roundtrip("RV04Crt2", parts).json()
        self.assertEqual(j["filename"], "doc-F-rt2.pdf")        # fallback filename が復元される
        self.assertEqual(j["file_hex"], b"%PDF jp\n%%EOF".hex())

    def test_delimiter_lookalike_file_bytes_full_match(self):
        # M01/H02: content に delimiter 類似列を含んでも file 全 bytes が完全一致で復元される
        val = b"%PDF\r\n--RV04Crt3 not-the-real-delimiter\r\nmore\x00\xff"
        parts = [{"name": "file", "filename": "x.pdf",
                  "content_type": "application/pdf", "value": val},
                 {"name": "drive_file_id", "filename": None,
                  "content_type": None, "value": b"F-rt3"}]
        j = self._roundtrip("RV04Crt3xyz", parts).json()
        self.assertEqual(j["file_hex"], val.hex())   # ファイル全 bytes 一致
        self.assertEqual(j["drive_file_id"], "F-rt3")

    def test_empty_text_field_restored(self):
        parts = [{"name": "file", "filename": "e.pdf",
                  "content_type": "application/pdf", "value": b"%PDF e"},
                 {"name": "case_hint", "filename": None, "content_type": None,
                  "value": b""},
                 {"name": "drive_file_id", "filename": None, "content_type": None,
                  "value": b"F-e"}]
        j = self._roundtrip("RV04Ce", parts).json()
        # 空 field は FastAPI/python-multipart が None に落とす（parser 挙動）。
        # 主眼は他 field/file の完全復元＝builder が壊れていないこと。
        self.assertIn(j["case_hint"], (None, ""))
        self.assertEqual(j["drive_file_id"], "F-e")
        self.assertEqual(j["file_hex"], b"%PDF e".hex())


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


# ── M02: Python 参照 production 前処理（gas rv04cBuildSignedBody_ と等価） ────
_LANE_FIELDS = {
    "/koseki/ingest": {"file", "case_hint", "case_app_hint", "drive_file_id"},
    "/registry/ingest": {"file", "case_hint", "drive_file_id"},
    "/bank/ingest": {"file", "case_hint", "case_app_hint", "drive_file_id"},
    "/sortation/ingest": {"file", "drive_file_id", "drive_file_url"},
    "/valuation/ingest": {"file", "case_hint", "case_app_hint", "drive_file_id"},
}


def _drive_id_of(parts):
    for p in parts:
        if p["name"] == "drive_file_id":
            return p["value"].decode("utf-8")
    return ""


def build_signed_body(path, parts, boundary):
    """gas rv04cBuildSignedBody_ の Python 等価: allowlist→sanitize→fallback→builder。"""
    allowed = _LANE_FIELDS.get(path)
    if allowed is None:
        raise BuilderError(f"unknown lane: {path}")
    drive_id = _drive_id_of(parts)
    built = []
    for p in parts:
        if p["name"] not in allowed:
            raise BuilderError(f"field not allowed for {path}: {p['name']}")
        q = {"name": p["name"], "value": p["value"], "content_type": p.get("content_type")}
        if p.get("filename") is not None:
            q["filename"] = sanitize_filename(p["filename"], drive_id)
        built.append(q)
    return build_multipart(boundary, built)


class TestProductionPipeline(unittest.TestCase):
    """M02/H03残: 前処理を通した body の検証。pipeline 分類（match/reject/skip）に沿って
    **SKIP 変換なし**で判定する（reject は例外送出が PASS 条件・match は body 一致）。"""

    def test_allowlist_rejects_unknown_field(self):
        parts = [{"name": "bogus", "value": b"x"}]
        with self.assertRaises(BuilderError):
            build_signed_body("/koseki/ingest", parts, "B")

    def test_pipeline_classification_no_skip(self):
        # 全 vector を pipeline 分類どおり判定（skip=builder_na のみ・他は match/reject）
        seen = {"match": 0, "reject": 0, "skip": 0}
        for v in _GASFX["vectors"]:
            cls = v.get("pipeline")
            self.assertIn(cls, ("match", "reject", "skip"), v["name"])
            seen[cls] += 1
            if cls == "skip":
                self.assertTrue(v.get("builder_na"), v["name"])   # skip は builder_na のみ
                continue
            parts = []
            for p in v["parts"]:
                q = {"name": p["name"], "value": _b(p["value_b64"]),
                     "content_type": p.get("content_type")}
                if p.get("filename") is not None:
                    # fallback vector は原名を渡して sanitize を経由
                    q["filename"] = (v["fallback_check"]["raw_filename"]
                                     if v.get("fallback_check") and p["name"] == "file"
                                     else p["filename"])
                parts.append(q)
            if cls == "reject":
                with self.subTest(vec=v["name"], expect="reject"):
                    with self.assertRaises(BuilderError):
                        build_signed_body("/koseki/ingest", parts, v["boundary"])
            else:  # match
                with self.subTest(vec=v["name"], expect="match"):
                    body = build_signed_body("/koseki/ingest", parts, v["boundary"])
                    self.assertEqual(base64.b64encode(body).decode(), v["body_b64"])
        self.assertGreaterEqual(seen["match"], 4)
        self.assertGreaterEqual(seen["reject"], 2)   # japanese(missing driveId)・multi_field(meta)

    def test_allowlist_regression_is_reject_not_skip(self):
        # 修正前 FAIL 実測対象: allowlist 退行（未許可 field）は SKIP でなく reject（例外）
        parts = [{"name": "meta", "value": b"x", "content_type": "application/json"},
                 {"name": "file", "filename": "a.pdf", "value": b"%PDF"}]
        with self.assertRaises(BuilderError):   # SKIP で通さない
            build_signed_body("/koseki/ingest", parts, "B")


# ── M01: driveFileId 検証（sanitize fallback へ埋め込む前に固定文字集合） ─────
class TestDriveIdValidation(unittest.TestCase):
    def test_valid_ids(self):
        for ok in ("F1", "abc_DEF-123", "x" * 128):
            self.assertEqual(validate_drive_id(ok), ok)

    def test_invalid_ids_rejected(self):
        for bad in ("", None, "a b", "日本語", "a\r", "a\n", 'a"', "x" * 129, "a.b"):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises(BuilderError):
                    validate_drive_id(bad)

    def test_m01_type_rejection(self):
        # M01残: 型不正（数値・None・配列・dict・bool）を明示拒否（暗黙文字列化なし）
        for bad in (123, None, [1, 2], {"a": 1}, True, 1.5, b"F1"):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises(BuilderError):
                    validate_drive_id(bad)

    def test_m01_gas_has_typeof_guard(self):
        # GAS 側 validateDriveId_ に typeof 文字列ガードがあること（RegExp 暗黙変換の排除）
        js = (_REPO / "gas" / "rv04c_signing.js").read_text(encoding="utf-8")
        self.assertIn("typeof driveFileId !== 'string'", js)

    def test_fallback_rejects_non_ascii_drive_id(self):
        # 非 ASCII filename の fallback に非 ASCII/不正 driveFileId → 例外（契約破りを防ぐ）
        for bad in ("日本語ID", "", "a/b"):
            with self.subTest(bad=repr(bad)):
                with self.assertRaises(BuilderError):
                    sanitize_filename("戸籍.pdf", bad)

    def test_fallback_ok_with_valid_drive_id(self):
        self.assertEqual(sanitize_filename("戸籍.pdf", "F-abc_1"), "doc-F-abc_1.pdf")


# ── H01: SIGNED_LANES 実効化（gas 側 dispatcher の構造テスト） ────────────────
class TestSignedLanesWired(unittest.TestCase):
    def setUp(self):
        self.js = (_REPO / "gas" / "rv04c_signing.js").read_text(encoding="utf-8")

    def test_dispatcher_references_signed_lanes(self):
        # rv04cIngestFetch_ が SIGNED_LANES[path] を実際に参照している（宣言のみでない）
        self.assertIn("function rv04cIngestFetch_", self.js)
        idx = self.js.index("function rv04cIngestFetch_")
        body = self.js[idx:idx + 800]
        self.assertIn("SIGNED_LANES[path]", body)     # 実効参照
        self.assertIn("rv04cSignedFetch_(path", body)  # true 分岐
        self.assertIn("UrlFetchApp.fetch", body)        # false=legacy 分岐
        self.assertIn("?token=", body)                  # legacy は query token

    def test_signed_fetch_no_longer_gates_internally(self):
        # rv04cSignedFetch_ の関数本体は SIGNED_LANES を見ない（dispatcher が唯一のゲート）。
        idx = self.js.index("function rv04cSignedFetch_")
        end = self.js.index("// ── H01", idx)
        self.assertNotIn("SIGNED_LANES", self.js[idx:end])

    def test_h01_no_bypass_signed_fetch_outside_dispatcher(self):
        # H01残: rv04cSignedFetch_ の呼出は dispatcher（rv04cIngestFetch_）内の 1 箇所のみ。
        # 出現＝定義 1（function ...）＋呼出 1（dispatcher 内）＝計 2。自己参照/定義以外の
        # 直接呼出があれば迂回ゲートになる。
        import re
        # ASCII の `(` を伴う参照のみ（コメント内の全角括弧説明は除外）
        occ = [m.start() for m in re.finditer(r"rv04cSignedFetch_\(", self.js)]
        self.assertEqual(len(occ), 2, f"想定外の rv04cSignedFetch_ 呼出/定義数: {len(occ)}")
        # 呼出（function 定義でない方）が dispatcher 関数の範囲内にあること
        disp_start = self.js.index("function rv04cIngestFetch_")
        disp_end = self.js.index("\n}", disp_start)
        call = [o for o in occ
                if not self.js[max(0, o - 9):o].endswith("function ")]
        self.assertEqual(len(call), 1)
        self.assertTrue(disp_start < call[0] < disp_end,
                        "rv04cSignedFetch_ 呼出が dispatcher 外にある（ゲート迂回）")

    def test_h01_legacy_branch_send_identity(self):
        # H01残-3: false/未設定 lane の legacy 送信が現行 watcher 形（?token=・payload・
        # muteHttpExceptions）と一致することを構造で固定。
        disp = self.js[self.js.index("function rv04cIngestFetch_"):]
        disp = disp[:disp.index("\n}")]
        self.assertIn("?token=' + encodeURIComponent(opts.legacyToken)", disp)  # URL token 形
        self.assertIn("payload: opts.legacyPayload", disp)                       # payload passthrough
        self.assertIn("muteHttpExceptions: true", disp)                          # options 一致
        self.assertIn("SIGNED_LANES[path] === true", disp)                       # true のみ署名


# ── H03: secret 一致・全 vector 期待値固定・SKIP 禁止（fixture/selftest 構造） ─
class TestFixtureExpectedValues(unittest.TestCase):
    def test_secret_matches_golden(self):
        self.assertEqual(_GASFX["secret_hex_test_only"], _GOLDEN["secret_hex_test_only"])
        js = (_REPO / "gas" / "rv04c_selftest.js").read_text(encoding="utf-8")
        self.assertIn(_GOLDEN["secret_hex_test_only"], js)   # selftest も golden secret

    def test_all_vectors_have_signing_expected_values(self):
        for v in _GASFX["vectors"]:
            with self.subTest(vec=v["name"]):
                for k in ("content_sha256", "canonical_b64", "signature",
                          "key_id", "caller", "normalized_path", "nonce"):
                    self.assertIn(k, v, f"{v['name']} missing {k}")
                    self.assertTrue(v[k])   # 非 null/非空（期待値欠落=不可）

    def test_signatures_verify_with_golden_secret(self):
        secret = bytes.fromhex(_GASFX["secret_hex_test_only"])
        for v in _GASFX["vectors"]:
            with self.subTest(vec=v["name"]):
                canon = canonical_v1_ref(v["key_id"], v["caller"], v["method"],
                                         v["normalized_path"], v["timestamp"],
                                         v["nonce"], v["content_sha256"])
                self.assertEqual(base64.b64encode(canon).decode(), v["canonical_b64"])
                self.assertEqual(hmac.new(secret, canon, hashlib.sha256).hexdigest(),
                                 v["signature"])

    def test_selftest_no_null_placeholder(self):
        # H03: S4 手転記の null プレースホルダが残っていない
        js = (_REPO / "gas" / "rv04c_selftest.js").read_text(encoding="utf-8")
        self.assertNotIn("expect_body_b64: null", js)
        self.assertNotIn("expect_signature: null", js)
        self.assertIn("var RV04C_VECTORS", js)   # 全 vector 埋め込み

    def test_h03_pipeline_selftest_no_skip_conversion(self):
        # H03残: production pipeline self-test の catch が builder_na 以外を SKIP に落とさない。
        # pipeline は match/reject/skip の明示分類で判定し、reject=例外送出が PASS 条件。
        js = (_REPO / "gas" / "rv04c_selftest.js").read_text(encoding="utf-8")
        pipe = js[js.index("function rv04c_productionPipelineSelfTest"):]
        pipe = pipe[:pipe.index("\n}\n")]
        # builder_na 以外の無条件 SKIP（旧 catch→SKIP continue）が無いこと
        self.assertNotIn("pipeline=SKIP(' + e.message", pipe)   # 例外を SKIP 化しない
        self.assertIn("FAIL(unexpected throw", pipe)            # match で例外→FAIL
        self.assertIn("FAIL(expected reject)", pipe)            # reject で無例外→FAIL
        # 全 vector に pipeline 分類がある（fixture 側）
        for v in _GASFX["vectors"]:
            self.assertIn(v.get("pipeline"), ("match", "reject", "skip"), v["name"])


if __name__ == "__main__":
    unittest.main()
