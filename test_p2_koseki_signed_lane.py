"""P2-CHAIN-001: koseki lane（lane2）署名切替の Railway 側受入テスト。

sortation（lane1）切替時と同型の検分を /koseki/ingest へ横展開する:
- negative 群: 署名欠落／期限外／nonce 再使用／body 改変／unknown key ID → 拒否
- legacy token 経路: dual-accept 併存中の現状挙動維持（flag ON/OFF とも）
- 2キー byte パリティ: parts は {file, drive_file_id} の 2 キーのみ（case_hint 系は
  parts に載せない・サーバ Form(default=None) が None のまま）。合成 file bytes は
  #140 整合版 ((i*131+7)+128)%256（GAS selftest signed 式と同一バイト列）を使用し、
  chunked==plain・署名同一・実サーバゲート通過後の復元 byte 完全一致まで通す。

検証器は既存 hub.service_auth（ingest_guard 結線済み）を再利用し、新規実装はしない。
registry / 署名ヘッダ組立 / DB mixin は test_rv04b_dual_accept と同一物を共用する
（別実装での PASS を作らない）。
"""

import hashlib
import hmac as _hmac
import re
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import os  # noqa: F401  (patch.dict 対象)

from test_rv04b_dual_accept import (  # noqa: E402
    LEGACY_TOKENS, REG_JSON, SECRET, _DbMixin, _FLAG, _INGEST_ENV, _REGENV,
    _client, _nofile_multipart, _nonce, _sig_headers)
from test_rv04c_gas_builder import (  # noqa: E402
    build_multipart, build_multipart_chunked, canonical_v1_ref)

import koseki_ingest  # noqa: E402

_REPO = Path(__file__).parent
_PATH = "/koseki/ingest"
_TOKEN = LEGACY_TOKENS[_PATH]


# ── SIGNED_LANES: 本 PR で切り替わるのは koseki の 1 lane のみ（rollback 状態固定） ──
class TestSignedLanesKosekiOn(unittest.TestCase):
    def test_koseki_true_others_unchanged(self):
        js = (_REPO / "gas" / "rv04c_signing.js").read_text(encoding="utf-8")
        block = re.search(r"var SIGNED_LANES = \{(.*?)\};", js, re.S)
        self.assertIsNotNone(block, "SIGNED_LANES 定義が見つからない")
        found = dict(re.findall(r"'(/[a-z/]+/ingest)':\s*(true|false)", block.group(1)))
        self.assertEqual(found, {
            "/koseki/ingest": "true",      # lane2: 本票で切替
            "/registry/ingest": "false",   # lane3 以降は据え置き
            "/bank/ingest": "false",
            "/sortation/ingest": "false",  # lane1 は live GAS 側で点火済み（repo 基準値は不変）
            "/valuation/ingest": "false",
        })


# ── negative 群（sortation 切替時と同型・すべて拒否を実測） ─────────────────
class TestKosekiSignedNegatives(_DbMixin):
    def setUp(self):
        super().setUp()
        self._p = patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON})
        self._p.start()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def _post(self, body, headers):
        return _client.post(_PATH, content=body, headers=headers)

    def test_signature_header_missing_rejected(self):
        # 署名欠落①: 他の X-Sig-* は揃うが X-Sig-Signature が無い → 401（missing_header）
        ct, body = _nofile_multipart()
        h = _sig_headers(_PATH, body, _nonce("p2k-miss"))
        del h["X-Sig-Signature"]
        h["Content-Type"] = ct
        self.assertEqual(self._post(body, h).status_code, 401)

    def test_no_signature_no_token_404(self):
        # 署名欠落②: 署名ヘッダ皆無＋token 無し → 404（存在しないフリの現状維持）
        ct, body = _nofile_multipart()
        self.assertEqual(self._post(body, {"Content-Type": ct}).status_code, 404)

    def test_expired_timestamp_rejected(self):
        # 期限外: skew(±300s) を大きく超えた過去 ts → 401（skew）
        ct, body = _nofile_multipart()
        h = _sig_headers(_PATH, body, _nonce("p2k-old"), ts=int(time.time()) - 3600)
        h["Content-Type"] = ct
        self.assertEqual(self._post(body, h).status_code, 401)

    def test_nonce_replay_rejected(self):
        # nonce 再使用: 1回目はゲート通過（file 無し 400）・同一 nonce 再送は 409
        ct, body = _nofile_multipart()
        h = _sig_headers(_PATH, body, _nonce("p2k-replay"))
        h["Content-Type"] = ct
        self.assertEqual(self._post(body, h).status_code, 400)
        self.assertEqual(self._post(body, h).status_code, 409)

    def test_body_tamper_rejected(self):
        # body 改変: 別 body へ署名したヘッダで送る → 401（body_mismatch）
        ct, body = _nofile_multipart()
        h = _sig_headers(_PATH, b"DIFFERENT-BODY", _nonce("p2k-tamper"))
        h["Content-Type"] = ct
        self.assertEqual(self._post(body, h).status_code, 401)

    def test_unknown_key_id_rejected(self):
        # unknown key ID: registry に無い kid → 401（key_unknown）
        ct, body = _nofile_multipart()
        h = _sig_headers(_PATH, body, _nonce("p2k-unk"), key_id="kid-unknown")
        h["Content-Type"] = ct
        self.assertEqual(self._post(body, h).status_code, 401)


# ── legacy token 経路: dual-accept 併存中の現状挙動維持 ─────────────────────
class TestKosekiLegacyPathDuringDualAccept(unittest.TestCase):
    def test_flag_on_valid_token_no_sig_accepted(self):
        # flag ON でも署名皆無＋有効 token はゲート通過（file 無し 400）
        with patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON}):
            ct, body = _nofile_multipart()
            r = _client.post(f"{_PATH}?token={_TOKEN}", content=body,
                             headers={"Content-Type": ct})
            self.assertEqual(r.status_code, 400, r.text)

    def test_flag_on_wrong_token_no_sig_404(self):
        with patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON}):
            ct, body = _nofile_multipart()
            r = _client.post(f"{_PATH}?token=wrong", content=body,
                             headers={"Content-Type": ct})
            self.assertEqual(r.status_code, 404, r.text)

    def test_flag_off_valid_token_accepted_sig_ignored(self):
        # flag OFF は完全に旧挙動（不正な署名ヘッダが付いていても token で通過）
        with patch.dict(os.environ, _INGEST_ENV):
            os.environ.pop(_FLAG, None)
            ct, body = _nofile_multipart()
            h = {"Content-Type": ct, "X-Sig-Version": "v1",
                 "X-Sig-Signature": "00" * 32}
            r = _client.post(f"{_PATH}?token={_TOKEN}", content=body, headers=h)
            self.assertEqual(r.status_code, 400, r.text)


# ── 2キー byte パリティ（#140 整合版合成入力・parts は file/drive_file_id のみ） ──
_BIG = bytes(((i * 131 + 7) + 128) % 256 for i in range(3_000_000))  # #140 整合版 3MB
_BOUNDARY = "RV04Ckoseki"
_PARTS_2KEY = [
    {"name": "file", "filename": "big.pdf",
     "content_type": "application/pdf", "value": _BIG},
    {"name": "drive_file_id", "filename": None,
     "content_type": None, "value": b"F-big"},
]


class TestKosekiTwoKeyByteParity(_DbMixin):
    def setUp(self):
        super().setUp()
        self._p = patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON})
        self._p.start()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def test_chunked_equals_plain_and_signature_identical(self):
        # GAS chunk append 等価（chunk 境界前後）→ content-sha 同一 → 署名同一
        plain = build_multipart(_BOUNDARY, _PARTS_2KEY)
        sha_plain = hashlib.sha256(plain).hexdigest()
        ts, nonce = "1752900000", "ab" * 16
        canon_plain = canonical_v1_ref("kid-test", "gas-ingest", "POST", _PATH,
                                       ts, nonce, sha_plain)
        sig_plain = _hmac.new(SECRET, canon_plain, hashlib.sha256).hexdigest()
        for ck in (8191, 8192, 8193):
            with self.subTest(chunk=ck):
                got = build_multipart_chunked(_BOUNDARY, _PARTS_2KEY, ck)
                self.assertEqual(got, plain)
                sha_got = hashlib.sha256(got).hexdigest()
                self.assertEqual(sha_got, sha_plain)
                canon = canonical_v1_ref("kid-test", "gas-ingest", "POST", _PATH,
                                         ts, nonce, sha_got)
                self.assertEqual(
                    _hmac.new(SECRET, canon, hashlib.sha256).hexdigest(), sig_plain)

    def test_signed_two_key_body_accepted_end_to_end(self):
        # builder 生成 body を実サーバの署名ゲート＋multipart parser へ通し、
        # 復元 file bytes / drive_file_id の byte 完全一致と case_hint 系 None を実測
        captured = {}

        async def _stub(pdf_bytes, filename, *, case_hint, case_app_hint,
                        drive_file_id):
            captured.update(pdf_bytes=pdf_bytes, filename=filename,
                            case_hint=case_hint, case_app_hint=case_app_hint,
                            drive_file_id=drive_file_id)
            return {"status": "ok"}

        body = build_multipart(_BOUNDARY, _PARTS_2KEY)
        h = _sig_headers(_PATH, body, _nonce("p2k-parity"))
        h["Content-Type"] = f"multipart/form-data; boundary={_BOUNDARY}"
        with patch.object(koseki_ingest, "ingest_koseki_pdf", _stub):
            r = _client.post(_PATH, content=body, headers=h)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(captured["pdf_bytes"], _BIG)          # byte 完全一致
        self.assertEqual(captured["filename"], "big.pdf")
        self.assertEqual(captured["drive_file_id"], "F-big")
        self.assertIsNone(captured["case_hint"])               # parts 固定（2キーのみ）
        self.assertIsNone(captured["case_app_hint"])


if __name__ == "__main__":
    unittest.main()
