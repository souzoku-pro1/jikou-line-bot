"""CloudSign webhook の fail-closed 化（P0B-002 / R0A-B03）のテスト

対象: cloudsign_webhook.handle_webhook / verify_completed_document / notify_business_line

固定する仕様:
  1. 照合成功（書類詳細 API が id 一致・status=締結完了(2) を返す）
     → 従来どおり 受任 遷移＋管理者LINE通知（挙動不変）
  2. 照合失敗（API例外 / 404 / 401 / timeout / status不一致 / documentID不整合 /
     想定外レスポンス）→ kintone write 0・顧客チャネル通知 0・受任へ進まない。
     業務指示Botチャネルで要人手確認を警報する（fail-closed）
  3. 同一 documentID の照合失敗が何度来ても受任遷移は 0 のまま
  4. 業務警報は DISPATCHBOT_CHANNEL_ACCESS_TOKEN 未設定なら送らない
     （顧客Bot LINE_CHANNEL_ACCESS_TOKEN へフォールバックしない）

全ケース mock（fetch_document / kintone / LINE）で外部通信なし。
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

import requests


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

SECRET = "test_secret"
COMPLETED_EVENT = {"documentID": "doc1", "status": 2}
COMPLETED_DOC = {"id": "doc1", "title": "委任契約書", "status": 2}


def _http_error(status_code: int) -> requests.HTTPError:
    resp = MagicMock()
    resp.status_code = status_code
    return requests.HTTPError(response=resp)


class TestVerificationSuccessUnchanged(unittest.TestCase):
    """照合成功: 従来どおり受任遷移＋通知（正常系の挙動不変）"""

    def test_success_transitions_and_notifies(self):
        with patch.object(mod, "fetch_document", return_value=dict(COMPLETED_DOC)) as mock_fetch, \
             patch.object(mod, "update_kintone_status", return_value=True) as mock_update, \
             patch.object(mod, "notify_line") as mock_notify, \
             patch.object(mod, "notify_business_line") as mock_biz:
            code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))

        self.assertEqual(code, 200)
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("state"), "processed")
        mock_fetch.assert_called_once_with("doc1")
        mock_update.assert_called_once_with("doc1", "受任")
        # P1-102（RV-10 S1）: 締結完了通知は顧客Bot(notify_line)ではなく
        # 業務チャネル(notify_business_line)へ・書類タイトルは redact される
        mock_notify.assert_not_called()
        mock_biz.assert_called_once()
        biz_msg = mock_biz.call_args.args[0]
        self.assertIn("【締結完了】", biz_msg)
        self.assertIn("doc1", biz_msg)               # documentID は相関 ID として残す
        self.assertNotIn("委任契約書", biz_msg)       # 書類タイトルは redact（非表示）

    def test_wrong_secret_still_404(self):
        code, _ = mod.handle_webhook("wrong", dict(COMPLETED_EVENT))
        self.assertEqual(code, 404)

    def test_non_completed_event_still_skipped(self):
        with patch.object(mod, "fetch_document") as mock_fetch, \
             patch.object(mod, "update_kintone_status") as mock_update:
            code, body = mod.handle_webhook(SECRET, {"documentID": "doc1", "status": 1})
        self.assertEqual(code, 200)
        mock_fetch.assert_not_called()
        mock_update.assert_not_called()


class TestVerificationFailureFailClosed(unittest.TestCase):
    """照合失敗: kintone write 0・顧客通知 0・受任へ進まない・業務警報あり"""

    FAILURE_CASES = [
        ("api例外404", {"side_effect": _http_error(404)}),
        ("api例外401", {"side_effect": _http_error(401)}),
        ("timeout", {"side_effect": requests.Timeout()}),
        ("接続失敗", {"side_effect": requests.ConnectionError()}),
        ("想定外例外", {"side_effect": ValueError("boom")}),
        ("status不一致", {"return_value": {"id": "doc1", "status": 1}}),
        ("statusキー欠落", {"return_value": {"id": "doc1", "title": "t"}}),
        ("idキー欠落", {"return_value": {"title": "t", "status": 2}}),
        ("documentID不整合", {"return_value": {"id": "other", "status": 2}}),
        ("非dictレスポンス", {"return_value": ["unexpected"]}),
    ]

    def test_all_failure_modes_do_not_transition(self):
        for label, fetch_conf in self.FAILURE_CASES:
            with self.subTest(case=label):
                with patch.object(mod, "fetch_document", **fetch_conf), \
                     patch.object(mod, "update_kintone_status") as mock_update, \
                     patch.object(mod, "notify_line") as mock_notify, \
                     patch.object(mod, "notify_business_line") as mock_biz:
                    code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))

                self.assertEqual(code, 200, label)
                self.assertEqual(body.get("state"), "verification_failed", label)
                mock_update.assert_not_called()   # kintone write 0（受任へ進まない）
                mock_notify.assert_not_called()   # 顧客チャネル通知 0
                mock_biz.assert_called_once()     # 要人手確認の業務警報
                msg = mock_biz.call_args.args[0]
                self.assertIn("doc1", msg)
                self.assertIn("失敗分類", msg)

    def test_failure_reason_classification(self):
        """失敗分類は閉集合の固定文字列（RCF-M05: vendor生値を埋め込まない）"""
        cases = [
            ({"side_effect": _http_error(404)}, "api_http_404"),
            ({"side_effect": _http_error(401)}, "api_http_401"),
            ({"side_effect": _http_error(500)}, "api_http_error"),  # 既知集合外は縮退
            ({"side_effect": requests.Timeout()}, "api_timeout"),
            ({"side_effect": requests.ConnectionError()}, "api_request_error"),
            ({"side_effect": ValueError("boom")}, "unexpected_error"),
            ({"return_value": {"id": "doc1", "status": 1}}, "status_mismatch"),
            ({"return_value": {"id": "doc1", "title": "t"}}, "status_mismatch"),
            ({"return_value": {"title": "t", "status": 2}}, "document_id_missing"),
            ({"return_value": {"id": "other", "status": 2}}, "document_id_mismatch"),
            ({"return_value": ["unexpected"]}, "unexpected_response_type"),
        ]
        for fetch_conf, expected_reason in cases:
            with self.subTest(reason=expected_reason):
                with patch.object(mod, "fetch_document", **fetch_conf):
                    doc, reason = mod.verify_completed_document("doc1")
                self.assertIsNone(doc)
                self.assertEqual(reason, expected_reason)

    def test_failure_reason_never_contains_vendor_status_value(self):
        """RCF-M05: vendor が返した status 実値（例: 99999）が分類文字列に漏れない"""
        with patch.object(mod, "fetch_document",
                          return_value={"id": "doc1", "status": 99999}):
            doc, reason = mod.verify_completed_document("doc1")
        self.assertIsNone(doc)
        self.assertEqual(reason, "status_mismatch")
        self.assertNotIn("99999", reason)

    def test_success_verification_returns_doc(self):
        with patch.object(mod, "fetch_document", return_value=dict(COMPLETED_DOC)):
            doc, reason = mod.verify_completed_document("doc1")
        self.assertEqual(reason, "")
        self.assertEqual(doc["title"], "委任契約書")

    def test_id_key_absent_is_fail_closed(self):
        """書類詳細に id キーが無い場合も照合失敗（RCF-H01: fail-closed）。
        CloudSign API が id を返さない仕様と実機確認できた場合のみ、
        実レスポンス fixture を固定した上で別途緩める"""
        with patch.object(mod, "fetch_document",
                          return_value={"title": "t", "status": 2}):
            doc, reason = mod.verify_completed_document("doc1")
        self.assertIsNone(doc)
        self.assertEqual(reason, "document_id_missing")


class TestKintoneUpdateFailed(unittest.TestCase):
    """RCF-M04: 照合成功だが kintone に一致レコードなし（update_kintone_status=False）
    → 受任は成立していないので「締結完了」通知を出さず、要人手確認を業務チャネルで警報"""

    def test_no_match_alerts_and_does_not_notify_customer_channel(self):
        with patch.object(mod, "fetch_document", return_value=dict(COMPLETED_DOC)), \
             patch.object(mod, "update_kintone_status", return_value=False) as mock_update, \
             patch.object(mod, "notify_line") as mock_notify, \
             patch.object(mod, "notify_business_line") as mock_biz:
            code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))

        self.assertEqual(code, 200)
        self.assertEqual(body.get("state"), "kintone_update_failed")  # processed とも
        # verification_failed とも識別できる固有の state
        mock_update.assert_called_once_with("doc1", "受任")
        mock_notify.assert_not_called()   # 「締結完了」通知は出さない
        mock_biz.assert_called_once()     # 要人手確認の業務警報（顧客チャネルへは出さない）
        msg = mock_biz.call_args.args[0]
        self.assertIn("doc1", msg)
        self.assertIn("kintone未更新", msg)


class TestRepeatedFailedEventsNeverTransition(unittest.TestCase):
    """再送: 同一 documentID の照合失敗が複数回来ても受任遷移 0 のまま"""

    def test_three_failed_deliveries_zero_transitions(self):
        with patch.object(mod, "fetch_document", side_effect=_http_error(404)), \
             patch.object(mod, "update_kintone_status") as mock_update, \
             patch.object(mod, "notify_line") as mock_notify, \
             patch.object(mod, "notify_business_line"):
            for _ in range(3):
                code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))
                self.assertEqual(code, 200)
                self.assertEqual(body.get("state"), "verification_failed")

        self.assertEqual(mock_update.call_count, 0)
        self.assertEqual(mock_notify.call_count, 0)


class TestBusinessNotifyChannelPolicy(unittest.TestCase):
    """業務警報のチャネル: DISPATCHBOT のみ・顧客Botへフォールバックしない"""

    def test_no_send_without_dispatchbot_token(self):
        """DISPATCHBOT token 未設定なら、顧客Bot token が設定されていても送らない"""
        env = {
            "LINE_CHANNEL_ACCESS_TOKEN": "customer_token",
            "LINE_ADMIN_USER_ID": "Uadmin",
            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "",  # 未設定相当（patch.dictで復元される）
        }
        with patch.dict("os.environ", env, clear=False), \
             patch.object(mod.requests, "post") as mock_post:
            mod.notify_business_line("警報テスト")
        mock_post.assert_not_called()

    def test_sends_via_dispatchbot_token_when_set(self):
        env = {
            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "dispatch_token",
            "LINE_ADMIN_USER_ID": "Uadmin",
        }
        with patch.dict("os.environ", env, clear=False), \
             patch.object(mod.requests, "post") as mock_post:
            mod.notify_business_line("警報テスト")
        mock_post.assert_called_once()
        kwargs = mock_post.call_args.kwargs
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer dispatch_token")
        self.assertEqual(kwargs["json"]["to"], "Uadmin")

    def test_send_failure_is_swallowed(self):
        env = {
            "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "dispatch_token",
            "LINE_ADMIN_USER_ID": "Uadmin",
        }
        with patch.dict("os.environ", env, clear=False), \
             patch.object(mod.requests, "post", side_effect=RuntimeError("net down")):
            mod.notify_business_line("警報テスト")  # 例外が漏れないこと


if __name__ == "__main__":
    unittest.main()
