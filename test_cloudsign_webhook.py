"""
cloudsign_webhook.py の回帰テスト

対象バグ:
  - FIELD_STATUS = "契約ステータス" （存在しないフィールドコード） → "status" に修正
  - update_kintone_status で書き込む値 "締結済み" が status フィールドの
    有効な選択肢に存在しない → "受任" に修正

App 21 の status フィールド有効値（2026-07-02 API で確認）:
  完了 / 手続き中 / 問い合わせ / 受付 / 受任 / 不受任 / 決済完了
"""

import importlib
import sys
import types
import unittest
from unittest.mock import MagicMock, patch, call


# ── テスト用に環境変数をダミーで差し込んでからモジュールをロード ────────────
def _load_module():
    env = {
        "CLOUDSIGN_CLIENT_ID": "dummy_client",
        "CLOUDSIGN_WEBHOOK_SECRET": "test_secret",
        "KINTONE_SUBDOMAIN": "testdomain",
        "KINTONE_APP_ID": "21",
        "KINTONE_API_TOKEN": "dummy_token",
    }
    with patch.dict("os.environ", env, clear=False):
        if "cloudsign_webhook" in sys.modules:
            del sys.modules["cloudsign_webhook"]
        import cloudsign_webhook
    return cloudsign_webhook


mod = _load_module()


class TestFieldConstants(unittest.TestCase):
    """フィールドコード・書き込み値の回帰テスト（修正前の値が戻らないことを保証）"""

    def test_field_status_code_is_not_old_value(self):
        """修正前の誤ったフィールドコード '契約ステータス' が使われていないこと"""
        self.assertNotEqual(mod.FIELD_STATUS, "契約ステータス",
                            "FIELD_STATUS が旧来の '契約ステータス' に戻っている。"
                            "kintone App 21 にこのフィールドは存在しない。")

    def test_field_status_code_is_correct(self):
        """正しいフィールドコード 'status' が使われていること"""
        self.assertEqual(mod.FIELD_STATUS, "status")

    def test_status_value_is_valid_option(self):
        """締結完了時に書き込む値が App 21 の有効な選択肢であること"""
        VALID_STATUS_OPTIONS = {"完了", "手続き中", "問い合わせ", "受付", "受任", "不受任", "決済完了"}
        # handle_webhook のソースから書き込み値を確認
        import inspect
        src = inspect.getsource(mod.handle_webhook)
        self.assertIn("受任", src,
                      "handle_webhook が '受任' を書き込んでいない。"
                      "CloudSign 締結完了 = 委任契約書の署名完了 → '受任' が正しい値。")
        self.assertNotIn("締結済み", src,
                         "無効な値 '締結済み' が handle_webhook に残っている。")


class TestUpdateKintoneStatusNormal(unittest.TestCase):
    """正常系: documentID が kintone に存在し、status = '受任' で更新される"""

    def test_found_and_updated(self):
        search_resp = MagicMock()
        search_resp.raise_for_status = MagicMock()
        search_resp.json.return_value = {
            "records": [{"$id": {"value": "42"}}]
        }

        update_resp = MagicMock()
        update_resp.raise_for_status = MagicMock()

        with patch("cloudsign_webhook.requests.get", return_value=search_resp) as mock_get, \
             patch("cloudsign_webhook.requests.put", return_value=update_resp) as mock_put:

            result = mod.update_kintone_status("doc123", "受任")

        self.assertTrue(result)

        # PUT ボディに正しいフィールドコードと値が入っていること
        put_kwargs = mock_put.call_args.kwargs
        record_payload = put_kwargs["json"]["record"]
        self.assertIn("status", record_payload,
                      "PUT ボディに 'status' フィールドがない（'契約ステータス' 等の誤コードを使っている可能性）")
        self.assertNotIn("契約ステータス", record_payload,
                         "PUT ボディに廃止フィールドコード '契約ステータス' が含まれている")
        self.assertEqual(record_payload["status"]["value"], "受任")


class TestUpdateKintoneStatusNotFound(unittest.TestCase):
    """異常系: documentID に一致するレコードが kintone に存在しない"""

    def test_returns_false_when_not_found(self):
        search_resp = MagicMock()
        search_resp.raise_for_status = MagicMock()
        search_resp.json.return_value = {"records": []}

        with patch("cloudsign_webhook.requests.get", return_value=search_resp), \
             patch("cloudsign_webhook.requests.put") as mock_put:

            result = mod.update_kintone_status("nonexistent_doc", "受任")

        self.assertFalse(result)
        mock_put.assert_not_called()


class TestHandleWebhookSecretCheck(unittest.TestCase):
    """合言葉チェック: 不一致なら 404"""

    def test_wrong_secret_returns_404(self):
        code, body = mod.handle_webhook("wrong_secret", {"documentID": "x", "status": 2})
        self.assertEqual(code, 404)

    def test_correct_secret_proceeds(self):
        """正しい secret なら処理が進む（受任更新・state=processed）。
        P1-102: 締結完了通知は業務チャネル notify_business_line 経由へ変更。"""
        with patch.object(mod, "fetch_document",
                          return_value={"id": "doc1", "title": "test", "status": 2}), \
             patch.object(mod, "update_kintone_status", return_value=True) as mock_update, \
             patch.object(mod, "notify_business_line") as mock_biz:
            code, body = mod.handle_webhook("test_secret", {"documentID": "doc1", "status": 2})
        self.assertEqual(code, 200)
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("state"), "processed")
        mock_update.assert_called_once_with("doc1", "受任")
        mock_biz.assert_called_once()


class TestHandleWebhookStatusNotCompleted(unittest.TestCase):
    """status が COMPLETED(2) 以外なら kintone 更新しない"""

    def test_non_completed_status_skips_update(self):
        with patch.object(mod, "update_kintone_status") as mock_update:
            code, _ = mod.handle_webhook("test_secret", {"documentID": "doc1", "status": 1})
        mock_update.assert_not_called()
        self.assertEqual(code, 200)


if __name__ == "__main__":
    unittest.main()
