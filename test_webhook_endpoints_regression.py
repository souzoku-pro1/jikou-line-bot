"""既存2エンドポイントの回帰テスト（T0-1 完了条件）

対象:
  - POST /webhook/kintone/approval  （main.py・App 29 承認 Webhook）
  - POST /document/{secret}          （document_webhook.py・送付状生成）

T0-1（hub/kintone + hub/webhook_auth への移設）の前後で、
URL・レスポンス（ステータスコード・ボディ）・kintone 書き込み内容が
不変であることを保証する。
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

# ── main import 前に環境変数を差し込む（既存テストと同じ流儀） ────────────────
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "HEALTHCHECK_DISABLED": "1",
})

from fastapi.testclient import TestClient  # noqa: E402

import document_webhook  # noqa: E402
import main  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402

client = TestClient(main.app)

APPROVAL_URL = "/webhook/kintone/approval"


def _approval_body(record_id="5", status="承認済", sent="no", **extra):
    record = {
        "$id": {"value": record_id},
        "ステータス2": {"value": status},
        "送信済み": {"value": sent},
    }
    record.update(extra)
    return {"record": record}


class TestApprovalWebhookRegression(unittest.TestCase):
    """POST /webhook/kintone/approval の既存挙動"""

    def test_wrong_token_returns_404(self):
        resp = client.post(f"{APPROVAL_URL}?token=wrong", json=_approval_body())
        self.assertEqual(resp.status_code, 404)

    def test_missing_token_returns_404(self):
        resp = client.post(APPROVAL_URL, json=_approval_body())
        self.assertEqual(resp.status_code, 404)

    def test_invalid_json_returns_400(self):
        resp = client.post(f"{APPROVAL_URL}?token=approve_token", content=b"not-json")
        self.assertEqual(resp.status_code, 400)

    def test_no_record_id_skips(self):
        resp = client.post(f"{APPROVAL_URL}?token=approve_token", json={"record": {}})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True, "skip": "no_record_id"})

    def test_missing_fields_skips(self):
        body = {"record": {"$id": {"value": "5"}}}
        resp = client.post(f"{APPROVAL_URL}?token=approve_token", json=body)
        self.assertEqual(resp.json(), {"ok": True, "skip": "missing_fields"})

    def test_not_triggered_skips(self):
        resp = client.post(f"{APPROVAL_URL}?token=approve_token",
                           json=_approval_body(status="承認待ち"))
        self.assertEqual(resp.json(), {"ok": True, "skip": "not_triggered"})

    def test_record_not_found_skips(self):
        with patch("hub.kintone.get_record",
                   new=AsyncMock(side_effect=hub_kintone.KintoneError(404, "X", "nf"))):
            resp = client.post(f"{APPROVAL_URL}?token=approve_token", json=_approval_body())
        self.assertEqual(resp.json(), {"ok": True, "skip": "record_not_found"})

    def test_already_sent_skips(self):
        latest = {
            "ステータス2": {"value": "承認済"},
            "送信済み": {"value": "yes"},
        }
        with patch("hub.kintone.get_record", new=AsyncMock(return_value=latest)):
            resp = client.post(f"{APPROVAL_URL}?token=approve_token", json=_approval_body())
        self.assertEqual(resp.json(), {"ok": True, "skip": "already_sent_or_not_approved"})

    def test_missing_user_or_draft_skips(self):
        latest = {
            "ステータス2": {"value": "承認済"},
            "送信済み": {"value": "no"},
            "line_user_id": {"value": ""},
            "AI下書き": {"value": ""},
        }
        with patch("hub.kintone.get_record", new=AsyncMock(return_value=latest)):
            resp = client.post(f"{APPROVAL_URL}?token=approve_token", json=_approval_body())
        self.assertEqual(resp.json(), {"ok": True, "skip": "missing_user_or_draft"})

    def test_success_pushes_and_marks_sent(self):
        latest = {
            "ステータス2": {"value": "承認済"},
            "送信済み": {"value": "no"},
            "line_user_id": {"value": "U123"},
            "AI下書き": {"value": "修正済みの返信文"},
            "カテゴリ": {"value": "法的判断・見通し"},
        }
        with patch("hub.kintone.get_record", new=AsyncMock(return_value=latest)), \
             patch("main.send_line_push", new=AsyncMock()) as mock_push, \
             patch("main.mark_approval_sent", new=AsyncMock()) as mock_mark, \
             patch("main.save_to_chatlog", new=AsyncMock()) as mock_log:
            resp = client.post(f"{APPROVAL_URL}?token=approve_token", json=_approval_body())

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True, "record_id": "5"})
        mock_push.assert_awaited_once_with("U123", "修正済みの返信文")
        mock_mark.assert_awaited_once_with("5")
        mock_log.assert_awaited_once_with(
            "U123", "assistant", "修正済みの返信文", "法的判断・見通し", "yes")

    def test_record_id_fallback_key(self):
        """recordId キーのみのボディでも従来どおり動く（フォールバック経路）"""
        body = {"recordId": 5,
                "record": {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}}}
        latest = {
            "ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"},
            "line_user_id": {"value": "U1"}, "AI下書き": {"value": "x"},
            "カテゴリ": {"value": "c"},
        }
        with patch("hub.kintone.get_record", new=AsyncMock(return_value=latest)), \
             patch("main.send_line_push", new=AsyncMock()), \
             patch("main.mark_approval_sent", new=AsyncMock()) as mock_mark, \
             patch("main.save_to_chatlog", new=AsyncMock()):
            resp = client.post(f"{APPROVAL_URL}?token=approve_token", json=body)
        self.assertEqual(resp.json(), {"ok": True, "record_id": "5"})
        mock_mark.assert_awaited_once_with("5")


def _doc_body(record_id="10", status="送付状作成"):
    return {"record": {"$id": {"value": record_id},
                       "書類ステータス": {"value": status}}}


class TestDocumentWebhookRegression(unittest.TestCase):
    """POST /document/{secret} の既存挙動"""

    def test_wrong_secret_returns_403(self):
        resp = client.post("/document/wrong", json=_doc_body())
        self.assertEqual(resp.status_code, 403)
        self.assertEqual(resp.json(), {"error": "forbidden"})

    def test_invalid_json_returns_400(self):
        resp = client.post("/document/doc_secret", content=b"not-json")
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json(), {"error": "invalid json"})

    def test_no_record_id_skips(self):
        resp = client.post("/document/doc_secret", json={"record": {}})
        self.assertEqual(resp.json(), {"ok": True, "skip": "no_record_id"})

    def test_not_triggered_skips(self):
        resp = client.post("/document/doc_secret", json=_doc_body(status="作成中"))
        self.assertEqual(resp.json(), {"ok": True, "skip": "not_triggered"})

    def test_already_done_skips(self):
        record = {"書類ステータス": {"value": "送付状作成済"}}
        with patch("document_webhook._get_record", new=AsyncMock(return_value=record)):
            resp = client.post("/document/doc_secret", json=_doc_body())
        self.assertEqual(resp.json(), {"ok": True, "skip": "already_done"})

    def test_kintone_error_returns_500(self):
        with patch("document_webhook._get_record",
                   new=AsyncMock(side_effect=hub_kintone.KintoneError(500, "X", "boom"))):
            resp = client.post("/document/doc_secret", json=_doc_body())
        self.assertEqual(resp.status_code, 500)
        self.assertEqual(resp.json(), {"error": "internal_error"})

    def test_success_generates_and_writes_back(self):
        """正常系: 実テンプレートで docx 生成 → 添付・ステータス書き戻し（内容不変の検証）"""
        record = {
            "書類ステータス": {"value": "送付状作成"},
            "住所": {"value": "埼玉県川口市1-1"},
            "氏名": {"value": "山田太郎"},
            "被相続人名": {"value": "山田花子"},
        }
        with patch("document_webhook._get_record", new=AsyncMock(return_value=record)), \
             patch("document_webhook._upload_file",
                   new=AsyncMock(return_value="fkey1")) as mock_up, \
             patch("document_webhook._update_record", new=AsyncMock()) as mock_update:
            resp = client.post("/document/doc_secret", json=_doc_body(record_id="10"))

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True, "record_id": "10"})

        # docx が実テンプレートから生成されて添付されること
        up_args = mock_up.await_args.args
        self.assertEqual(up_args[0], "送付状_委任契約書.docx")
        self.assertTrue(up_args[1].startswith(b"PK"), "docx (zip) 形式で生成されていること")

        # kintone 書き込み内容の不変性（添付 fileKey + ステータス完了値・1回の更新）
        mock_update.assert_awaited_once_with("10", {
            "送付状": [{"fileKey": "fkey1"}],
            "書類ステータス": "送付状作成済",
        })


if __name__ == "__main__":
    unittest.main()
