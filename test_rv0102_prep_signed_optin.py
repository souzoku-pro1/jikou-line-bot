"""RV-0102-PREP: /scan・/ocr/fixed-asset の署名 opt-in 事前配線テスト（IMPL-BATCH-1 A）。

両入口は旧 query token を持たない（現行=無認証受理）。dual-accept 事前配線の契約:
- flag OFF: 署名ヘッダの有無に関わらず現行挙動と完全同一（回帰 pin）
- flag ON・署名ヘッダ皆無: 従来どおり受理（現行挙動不変・強制化は[人]別票）
- flag ON・署名ヘッダ在: 署名経路のみで判定——署名不正・期限外・replay（nonce
  再使用）・別 path 転用・body 改変・unknown key → 拒否

検証器・registry・署名ヘッダ組立・DB mixin は test_rv04b_dual_accept と同一物を
共用する（別実装での PASS を作らない・test_p2_koseki_signed_lane と同じ流儀）。
ゲート通過の判定信号: /scan は未対応フォルダ名 400・/ocr/fixed-asset は
kintone env 未設定 500（いずれも認証前段を通過した後の endpoint 応答）。
"""

import hashlib
import json
import time
import unittest
from unittest.mock import patch

import os  # noqa: F401  (patch.dict 対象)

from test_rv04b_dual_accept import (  # noqa: E402
    SECRET_HEX, _DbMixin, _FLAG, _REGENV, _client, _nonce, _sig_headers)

_SCAN = "/scan"
_OCR = "/ocr/fixed-asset"

# opt-in 2 path を許可するテスト専用 registry（secret は rv04b と共用）
REG_OPTIN_JSON = json.dumps({"kid-test": {
    "secret": SECRET_HEX, "caller": "gas-ingest", "allowed_methods": ["POST"],
    "allowed_paths": [_SCAN, _OCR], "not_before": 0, "expires_at": 2 ** 31,
    "status": "active"}})

_ENV_ON = {_FLAG: "1", _REGENV: REG_OPTIN_JSON,
           "GOOGLE_VISION_API_KEY": "dummy_vision"}


def _scan_body(folder="未対応F"):
    """ScanRequest として parse 可能な JSON body（未対応フォルダ名 400 が
    ゲート通過の判定信号）。"""
    return json.dumps({"pdf_base64": "eA==", "folder_name": folder},
                      ensure_ascii=False).encode("utf-8")


def _pdf_multipart():
    """/ocr/fixed-asset 用の file 付き multipart（kintone env 未設定 500 が
    ゲート通過の判定信号）。"""
    b = (b"--BND\r\n"
         b'Content-Disposition: form-data; name="file"; filename="a.pdf"\r\n'
         b"Content-Type: application/pdf\r\n\r\n%PDF-1.4\r\n--BND--\r\n")
    return "multipart/form-data; boundary=BND", b


# ── flag OFF: 現行挙動と完全同一（署名ヘッダは無視される・回帰 pin） ──────────
class TestFlagOffUnchanged(unittest.TestCase):
    def setUp(self):
        self._p = patch.dict(os.environ, {"GOOGLE_VISION_API_KEY": "dummy_vision"})
        self._p.start()
        os.environ.pop(_FLAG, None)

    def tearDown(self):
        self._p.stop()

    def test_scan_unsigned_unchanged(self):
        r = _client.post(_SCAN, content=_scan_body(),
                         headers={"Content-Type": "application/json"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("未対応のフォルダ名", r.json()["detail"])

    def test_scan_garbage_sig_headers_ignored(self):
        # flag OFF は不正な署名ヘッダが付いていても現行どおり endpoint へ到達
        h = {"Content-Type": "application/json", "X-Sig-Version": "v1",
             "X-Sig-Signature": "00" * 32}
        r = _client.post(_SCAN, content=_scan_body(), headers=h)
        self.assertEqual(r.status_code, 400)
        self.assertIn("未対応のフォルダ名", r.json()["detail"])

    def test_ocr_unsigned_unchanged(self):
        ct, body = _pdf_multipart()
        r = _client.post(_OCR, content=body, headers={"Content-Type": ct})
        self.assertEqual(r.status_code, 500)   # kintone env 未設定（現行挙動）

    def test_ocr_garbage_sig_headers_ignored(self):
        ct, body = _pdf_multipart()
        h = {"Content-Type": ct, "X-Sig-Version": "v1",
             "X-Sig-Signature": "00" * 32}
        r = _client.post(_OCR, content=body, headers=h)
        self.assertEqual(r.status_code, 500)


# ── flag ON・署名ヘッダ皆無: 従来どおり受理（現行挙動不変の回帰 pin） ────────
class TestFlagOnUnsignedStillAccepted(unittest.TestCase):
    def setUp(self):
        self._p = patch.dict(os.environ, _ENV_ON)
        self._p.start()

    def tearDown(self):
        self._p.stop()

    def test_scan_unsigned_accepted_as_before(self):
        r = _client.post(_SCAN, content=_scan_body(),
                         headers={"Content-Type": "application/json"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("未対応のフォルダ名", r.json()["detail"])

    def test_ocr_unsigned_accepted_as_before(self):
        ct, body = _pdf_multipart()
        r = _client.post(_OCR, content=body, headers={"Content-Type": ct})
        self.assertEqual(r.status_code, 500)


# ── flag ON・署名経路: 正しい署名は通過・不正は拒否（negative 群） ────────────
class TestSignedNegatives(_DbMixin):
    def setUp(self):
        super().setUp()
        self._p = patch.dict(os.environ, _ENV_ON)
        self._p.start()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def _post_scan(self, body, headers):
        headers["Content-Type"] = "application/json"
        return _client.post(_SCAN, content=body, headers=headers)

    def test_valid_signature_passes_gate(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv0102-ok"))
        r = self._post_scan(body, h)
        self.assertEqual(r.status_code, 400, r.text)   # ゲート通過→未対応フォルダ名
        self.assertIn("未対応のフォルダ名", r.json()["detail"])

    def test_valid_signature_passes_gate_ocr(self):
        ct, body = _pdf_multipart()
        h = _sig_headers(_OCR, body, _nonce("rv0102-ok-ocr"))
        h["Content-Type"] = ct
        r = _client.post(_OCR, content=body, headers=h)
        self.assertEqual(r.status_code, 500, r.text)   # ゲート通過→kintone env 未設定

    def test_bad_signature_rejected(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv0102-bad"))
        h["X-Sig-Signature"] = "00" * 32
        self.assertEqual(self._post_scan(body, h).status_code, 401)

    def test_expired_timestamp_rejected(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv0102-old"),
                         ts=int(time.time()) - 3600)
        self.assertEqual(self._post_scan(body, h).status_code, 401)

    def test_nonce_replay_rejected(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv0102-replay"))
        self.assertEqual(self._post_scan(body, dict(h)).status_code, 400)  # 1回目通過
        self.assertEqual(self._post_scan(body, dict(h)).status_code, 409)  # replay

    def test_cross_path_reuse_rejected(self):
        # 別 path 転用: /scan 宛に署名したヘッダを /ocr/fixed-asset へ送る
        # → canonical の path 不一致＝bad_sig（401）
        ct, body = _pdf_multipart()
        h = _sig_headers(_SCAN, body, _nonce("rv0102-cross"))
        h["Content-Type"] = ct
        self.assertEqual(_client.post(_OCR, content=body, headers=h).status_code, 401)

    def test_path_not_in_key_allowlist_rejected(self):
        # 別 path 転用②: opt-in 2 path しか持たない鍵で /koseki/ingest へ
        # 正しく署名しても path_denied（403・鍵の allowed_paths 遮断）
        body = b"x"
        h = _sig_headers("/koseki/ingest", body, _nonce("rv0102-deny"))
        h["Content-Type"] = "application/octet-stream"
        r = _client.post("/koseki/ingest", content=body, headers=h)
        self.assertEqual(r.status_code, 403)

    def test_body_tamper_rejected(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, b"DIFFERENT-BODY", _nonce("rv0102-tamper"))
        self.assertEqual(self._post_scan(body, h).status_code, 401)

    def test_unknown_key_id_rejected(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv0102-unk"), key_id="kid-unknown")
        self.assertEqual(self._post_scan(body, h).status_code, 401)

    def test_signature_header_missing_rejected(self):
        body = _scan_body()
        h = _sig_headers(_SCAN, body, _nonce("rv0102-miss"))
        del h["X-Sig-Signature"]
        self.assertEqual(self._post_scan(body, h).status_code, 401)


if __name__ == "__main__":
    unittest.main()
