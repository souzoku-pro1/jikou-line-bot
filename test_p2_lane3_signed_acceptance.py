"""P2-CHAIN-006: lane3（bank／valuation）サーバ側受入のテスト先行実装。

現状の実挙動を pin する characterization（コード変更なし・SIGNED_LANES は false のまま）:
- negative 群（koseki 型を 2 lane へ parametrize 共通化）:
  署名欠落／期限外／nonce 再使用／body 改変／unknown key ID → 拒否
- legacy token 経路が**現在も受理される**こと（`SERVICE_AUTH_LEGACY_DISABLED_PATHS`
  非該当＝dual 状態の pin。**点火票（lane3 開通・G-L3-0 充足後）でこの期待値を
  404/`legacy_blocked` へ反転させる前提**）
- byte パリティ: #140 整合合成入力で 4 キー parts の chunked==plain・SHA 同一

前提裁定の記録（P2-BATCH-01）:
- OPEN-2 暫定裁定: LANE_FIELDS の bank/valuation は **4 キーのまま維持**。
  理由: gas_builder fixture vector が valuation の case_hint 許容に依存・
  実送信キーは OPEN-1（watcher 設計）確定時に再裁定。
- SIGNED_LANES は現状維持（false）。**true 化は live 点火と同時の別票**
  （INC-0720 §7(ii) の repo=live 同期規律のため、先行 true 化は逆 drift になる）。

registry／署名ヘッダ組立／DB mixin は test_rv04b_dual_accept と同一物を共用し、
builder は test_rv04c_gas_builder の参照実装を共用する（別実装での PASS を作らない）。
"""

import hashlib
import os
import time
import unittest
from unittest.mock import patch

from test_rv04b_dual_accept import (  # noqa: E402
    LEGACY_TOKENS, REG_JSON, _DbMixin, _FLAG, _INGEST_ENV, _REGENV,
    _client, _nofile_multipart, _nonce, _sig_headers)
from test_rv04c_gas_builder import (  # noqa: E402
    build_multipart, build_multipart_chunked)

_LANES = ["/bank/ingest", "/valuation/ingest"]


# ── negative 群（koseki 型・2 lane parametrize・すべて拒否を実測） ───────────
class TestLane3SignedNegatives(_DbMixin):
    def setUp(self):
        super().setUp()
        self._p = patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON})
        self._p.start()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def test_signature_header_missing_rejected(self):
        # 署名欠落①: 他の X-Sig-* は揃うが X-Sig-Signature が無い → 401
        for path in _LANES:
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                h = _sig_headers(path, body, _nonce("l3-miss" + path))
                del h["X-Sig-Signature"]
                h["Content-Type"] = ct
                self.assertEqual(_client.post(path, content=body, headers=h)
                                 .status_code, 401)

    def test_no_signature_no_token_404(self):
        # 署名欠落②: 署名ヘッダ皆無＋token 無し → 404（存在しないフリ）
        for path in _LANES:
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                self.assertEqual(_client.post(path, content=body,
                                              headers={"Content-Type": ct})
                                 .status_code, 404)

    def test_expired_timestamp_rejected(self):
        # 期限外: skew(±300s) を大きく超えた過去 ts → 401
        for path in _LANES:
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                h = _sig_headers(path, body, _nonce("l3-old" + path),
                                 ts=int(time.time()) - 3600)
                h["Content-Type"] = ct
                self.assertEqual(_client.post(path, content=body, headers=h)
                                 .status_code, 401)

    def test_nonce_replay_rejected(self):
        # nonce 再使用: 1回目はゲート通過（file 無し 400）・同一 nonce 再送は 409
        for path in _LANES:
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                h = _sig_headers(path, body, _nonce("l3-replay" + path))
                h["Content-Type"] = ct
                self.assertEqual(_client.post(path, content=body, headers=h)
                                 .status_code, 400)
                self.assertEqual(_client.post(path, content=body, headers=h)
                                 .status_code, 409)

    def test_body_tamper_rejected(self):
        # body 改変: 別 body へ署名したヘッダで送る → 401（body_mismatch）
        for path in _LANES:
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                h = _sig_headers(path, b"DIFFERENT-BODY", _nonce("l3-tamper" + path))
                h["Content-Type"] = ct
                self.assertEqual(_client.post(path, content=body, headers=h)
                                 .status_code, 401)

    def test_unknown_key_id_rejected(self):
        # unknown key ID: registry に無い kid → 401（key_unknown）
        for path in _LANES:
            with self.subTest(path=path):
                ct, body = _nofile_multipart()
                h = _sig_headers(path, body, _nonce("l3-unk" + path),
                                 key_id="kid-unknown")
                h["Content-Type"] = ct
                self.assertEqual(_client.post(path, content=body, headers=h)
                                 .status_code, 401)


# ── legacy token 経路の現状 pin（dual 状態の characterization） ──────────────
class TestLane3LegacyStillAccepted(unittest.TestCase):
    """`SERVICE_AUTH_LEGACY_DISABLED_PATHS` 非該当＝legacy 受理が現状挙動。
    点火票（lane3 開通時に PATHS へ追記）で本クラスの期待値は 404 系へ反転させる。"""

    def test_flag_on_valid_token_no_sig_accepted(self):
        # dual flag ON でも署名皆無＋有効 token はゲート通過（file 無し 400）
        with patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON}):
            for path in _LANES:
                with self.subTest(path=path):
                    ct, body = _nofile_multipart()
                    r = _client.post(f"{path}?token={LEGACY_TOKENS[path]}",
                                     content=body, headers={"Content-Type": ct})
                    self.assertEqual(r.status_code, 400, (path, r.text))

    def test_flag_off_valid_token_accepted(self):
        # flag OFF（旧挙動）でも同様にゲート通過
        with patch.dict(os.environ, _INGEST_ENV):
            os.environ.pop(_FLAG, None)
            for path in _LANES:
                with self.subTest(path=path):
                    ct, body = _nofile_multipart()
                    r = _client.post(f"{path}?token={LEGACY_TOKENS[path]}",
                                     content=body, headers={"Content-Type": ct})
                    self.assertEqual(r.status_code, 400, (path, r.text))

    def test_wrong_token_404(self):
        with patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: REG_JSON}):
            for path in _LANES:
                with self.subTest(path=path):
                    ct, body = _nofile_multipart()
                    r = _client.post(f"{path}?token=wrong", content=body,
                                     headers={"Content-Type": ct})
                    self.assertEqual(r.status_code, 404, (path, r.text))


# ── byte パリティ（#140 整合版合成入力・4 キー parts＝OPEN-2 暫定裁定どおり） ─
_BIG = bytes(((i * 131 + 7) + 128) % 256 for i in range(3_000_000))  # #140 整合版 3MB
_PARTS_4KEY = [
    {"name": "file", "filename": "big.pdf",
     "content_type": "application/pdf", "value": _BIG},
    {"name": "case_hint", "filename": None, "content_type": None, "value": b"R-1"},
    {"name": "case_app_hint", "filename": None, "content_type": None, "value": b"26"},
    {"name": "drive_file_id", "filename": None, "content_type": None, "value": b"F-big"},
]


class TestLane3FourKeyByteParity(unittest.TestCase):
    def test_bank_chunked_equals_plain(self):
        self._parity("RV04Cbank")

    def test_valuation_chunked_equals_plain(self):
        self._parity("RV04Cvaluation")

    def _parity(self, boundary):
        plain = build_multipart(boundary, _PARTS_4KEY)
        sha_plain = hashlib.sha256(plain).hexdigest()
        for ck in (8191, 8192, 8193):
            with self.subTest(chunk=ck):
                got = build_multipart_chunked(boundary, _PARTS_4KEY, ck)
                self.assertEqual(got, plain)
                self.assertEqual(hashlib.sha256(got).hexdigest(), sha_plain)


if __name__ == "__main__":
    unittest.main()
