"""RV-10 PR-5 redaction 保証テスト（items 8/9/10）。

検証の眼目（テストが「意味を持つ」条件）:
  各アサートは redaction を外すと必ず落ちる。すなわち
    - ベンダ生ボディ / 生例外 / 入力エコー（= _SENTINEL）が
      log・レスポンスに「現れない」ことを assert（ABSENCE）
    - 代わりに可視であるべき情報（HTTPステータス 502、固定文言、
      正しいHTTPステータスコード）が「現れる」ことを assert（PRESENCE）

  item 8: chat_responder の 5 error パス（captured-log）。emit(count) で
          status は残り emit(vendor_raw) で body は抑止される契約を固定。
  item 9: /webhook/stripe 署名検証失敗（endpoint + log）。
  item 10: /scan・/ocr/fixed-asset の失敗分岐 HTTP ステータス（"BASE 不変"）。

本ファイルはテストのみ。production code は一切変更しない。
"""

import base64
import os
import unittest
from unittest.mock import MagicMock, patch

# ── main.py / chat_responder.py は import 時に多数の module-level env を要求する。
#    test_bank_ingest.py と同一の env-setup preamble を踏襲して import を通す。
_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "APP_CHATLOG": "40",
    "TOKEN_CHATLOG": "dummy",
    "GOOGLE_VISION_API_KEY": "dummy_vision",
    # /ocr/fixed-asset の env チェックを通すため（KINTONE_FUDOSAN_DOMAIN は
    # KINTONE_SUBDOMAIN にフォールバック）
    "KINTONE_FUDOSAN_APP_ID": "50",
    "KINTONE_FUDOSAN_API_TOKEN": "dummy",
    "STRIPE_WEBHOOK_SECRET": "whsec_dummy",
    "HEALTHCHECK_DISABLED": "1",
})

from fastapi.testclient import TestClient  # noqa: E402

import chat_responder  # noqa: E402
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]

client = TestClient(main.app)

# ベンダ生ボディ / 生例外 / 入力エコーの目印。log・レスポンスに現れたら leak。
_SENTINEL = "VENDOR_BODY_SENTINEL_ZZZ"


# ══════════════════════════════════════════════════════════════════════════════
# 共通: httpx.AsyncClient の偽物（async context manager）
# ══════════════════════════════════════════════════════════════════════════════

class _FakeResp:
    """失敗レスポンス。body(.text) に _SENTINEL を仕込む。"""
    is_success = False
    status_code = 502
    text = _SENTINEL

    def json(self):
        return {}


class _FakeClient:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, *a, **k):
        return _FakeResp()

    async def post(self, *a, **k):
        return _FakeResp()

    async def put(self, *a, **k):
        return _FakeResp()


def _fake_client_factory(*a, **k):
    return _FakeClient()


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 8 — chat_responder の 5 error パス（captured-log）
# ══════════════════════════════════════════════════════════════════════════════

class Item8ChatResponderErrorPaths(unittest.IsolatedAsyncioTestCase):
    """失敗した httpx レスポンスを log する 5 関数について、
    vendor 生ボディ(_SENTINEL)が log に出ず・status(502)は残ることを固定。
    """

    async def _run_case(self, coro_factory):
        # APP_CHATLOG / APP_APPROVAL 系は module-level に読み込まれるため直接差し替え
        with patch.multiple(
            chat_responder,
            APP_CHATLOG="40", TOKEN_CHATLOG="dummy",
            APP_APPROVAL="29", TOKEN_APPROVAL="dummy",
            _SUBDOMAIN="testsub", _APP21_ID="21", _APP21_TOKEN="dummy",
        ), patch("chat_responder.httpx.AsyncClient", _fake_client_factory):
            with self.assertLogs("chat_responder", level="WARNING") as cm:
                await coro_factory()
        return "\n".join(cm.output)

    async def test_app21_search(self):
        out = await self._run_case(lambda: chat_responder.get_app21_record("U1"))
        self._assert_redacted(out, "get_app21_record")

    async def test_save_to_chatlog(self):
        out = await self._run_case(
            lambda: chat_responder.save_to_chatlog("U1", "user", "hi", "一般", "no"))
        self._assert_redacted(out, "save_to_chatlog")

    async def test_get_recent_chat_history(self):
        out = await self._run_case(
            lambda: chat_responder.get_recent_chat_history("U1", limit=3))
        self._assert_redacted(out, "get_recent_chat_history")

    async def test_get_approval_record(self):
        out = await self._run_case(
            lambda: chat_responder.get_approval_record("100"))
        self._assert_redacted(out, "get_approval_record")

    async def test_mark_approval_sent(self):
        out = await self._run_case(
            lambda: chat_responder.mark_approval_sent("100"))
        self._assert_redacted(out, "mark_approval_sent")

    def _assert_redacted(self, out, name):
        # ABSENCE: vendor 生ボディは抑止される
        self.assertNotIn(
            _SENTINEL, out,
            f"[LEAK] {name}: vendor body(_SENTINEL) が log に露出した:\n{out}")
        # PRESENCE: status code は emit(count) 経由で可視
        self.assertIn(
            "502", out,
            f"{name}: status code 502 が log に現れていない:\n{out}")


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 9 — /webhook/stripe 署名検証失敗（endpoint + log）
# ══════════════════════════════════════════════════════════════════════════════

class Item9StripeSignatureFailure(unittest.TestCase):

    def test_stripe_signature_failure_is_redacted(self):
        with patch("main.stripe.Webhook.construct_event",
                   side_effect=Exception(_SENTINEL)):
            with self.assertLogs("main", level="WARNING") as cm:
                resp = client.post(
                    "/webhook/stripe",
                    content=b'{"any":"body"}',
                    headers={"stripe-signature": "t=1,v1=deadbeef"},
                )
        log_out = "\n".join(cm.output)

        # endpoint: 400 + 固定文言（生例外を含まない）
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["detail"], "署名の検証に失敗しました")
        self.assertNotIn(
            _SENTINEL, resp.text,
            f"[LEAK] stripe: 生例外がレスポンスに露出:\n{resp.text}")
        # log: 生例外(_SENTINEL)は emit(vendor_raw) で抑止される
        self.assertNotIn(
            _SENTINEL, log_out,
            f"[LEAK] stripe: 生例外が log に露出:\n{log_out}")


# ══════════════════════════════════════════════════════════════════════════════
# ITEM 10 — /scan・/ocr/fixed-asset 失敗分岐の HTTP ステータス（"BASE 不変"）
# ══════════════════════════════════════════════════════════════════════════════

class Item10ScanOcrFailureBranches(unittest.TestCase):
    """代表的な失敗分岐について、正確な status code と
    固定 detail（生例外/PII/入力エコーを埋め込まない）を固定する。
    """

    def setUp(self):
        # /ocr/fixed-asset の env チェックは main の module 定数（import 時に確定）を
        # 見る。import 順（他 test が main を先に import）に依存しないよう、OCR 系
        # 定数を非空へ patch して env チェックを必ず通す（テスト隔離・挙動検査は
        # その先の 400/502 分岐に対して行う）。
        for name, val in (("GOOGLE_VISION_API_KEY", "dummy_vision"),
                          ("KINTONE_FUDOSAN_DOMAIN", "testsub"),
                          ("KINTONE_FUDOSAN_APP_ID_OCR", "40"),
                          ("KINTONE_FUDOSAN_API_TOKEN_OCR", "dummy")):
            p = patch.object(main, name, val)
            p.start()
            self.addCleanup(p.stop)

    # ── /scan ─────────────────────────────────────────────────────────────────

    def test_scan_unknown_folder_400(self):
        # folder_name に _SENTINEL を入れ、固定 detail が入力をエコーしないことを確認
        resp = client.post("/scan", json={
            "folder_name": _SENTINEL,
            "pdf_base64": "AAAA",
        })
        self.assertEqual(resp.status_code, 400)
        self.assertNotIn(
            _SENTINEL, resp.text,
            f"[LEAK] /scan unknown-folder: 入力 folder_name がエコー:\n{resp.text}")
        self.assertIn("未対応のフォルダ名", resp.json()["detail"])

    def test_scan_base64_error_400(self):
        with patch("main.base64.b64decode",
                   side_effect=Exception(_SENTINEL)):
            resp = client.post("/scan", json={
                "folder_name": "相談カード",
                "pdf_base64": "not-valid",
            })
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["detail"], "base64デコードエラー")
        self.assertNotIn(
            _SENTINEL, resp.text,
            f"[LEAK] /scan base64-error: 生例外がレスポンスに露出:\n{resp.text}")

    def test_scan_ocr_error_502(self):
        good_b64 = base64.b64encode(b"%PDF-1.4 dummy").decode()
        with patch("main._ocr_pdf_bytes", side_effect=Exception(_SENTINEL)):
            resp = client.post("/scan", json={
                "folder_name": "相談カード",
                "pdf_base64": good_b64,
            })
        self.assertEqual(resp.status_code, 502)
        self.assertEqual(resp.json()["detail"], "OCRエラー（画像認識に失敗しました）")
        self.assertNotIn(
            _SENTINEL, resp.text,
            f"[LEAK] /scan ocr-error: 生例外がレスポンスに露出:\n{resp.text}")

    # ── /ocr/fixed-asset ───────────────────────────────────────────────────────

    def test_ocr_non_pdf_400(self):
        # filename に _SENTINEL を入れ、固定 detail がエコーしないことを確認
        resp = client.post(
            "/ocr/fixed-asset",
            files={"file": (_SENTINEL + ".txt", b"data", "text/plain")},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json()["detail"], "PDFファイルを送信してください")
        self.assertNotIn(
            _SENTINEL, resp.text,
            f"[LEAK] /ocr non-pdf: 入力 filename がエコー:\n{resp.text}")

    def test_ocr_ocr_error_502(self):
        with patch("main._ocr_pdf_bytes", side_effect=Exception(_SENTINEL)):
            resp = client.post(
                "/ocr/fixed-asset",
                files={"file": ("doc.pdf", b"%PDF-1.4 dummy", "application/pdf")},
            )
        self.assertEqual(resp.status_code, 502)
        self.assertEqual(resp.json()["detail"], "OCRエラー（画像認識に失敗しました）")
        self.assertNotIn(
            _SENTINEL, resp.text,
            f"[LEAK] /ocr ocr-error: 生例外がレスポンスに露出:\n{resp.text}")


if __name__ == "__main__":
    unittest.main()
