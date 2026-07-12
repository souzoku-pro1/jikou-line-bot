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

import json
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
             patch.object(mod, "update_kintone_status", return_value="42") as mock_update, \
             patch.object(mod, "notify_line") as mock_notify, \
             patch.object(mod, "notify_business_line") as mock_biz:
            code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))

        self.assertEqual(code, 200)
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("state"), "processed")
        mock_fetch.assert_called_once_with("doc1")
        mock_update.assert_called_once_with("doc1", "受任")
        # P1-102/102a（RV-10 S1・M06）: 締結完了通知は顧客Bot(notify_line)ではなく
        # 業務チャネル(notify_business_line)へ。書類特定は kintone レコード No で行い、
        # documentID は external_ref として抑止する（業務チャネルにも素で出さない）
        mock_notify.assert_not_called()
        mock_biz.assert_called_once()
        biz_msg = mock_biz.call_args.args[0]
        self.assertIn("【締結完了】", biz_msg)
        self.assertIn("42", biz_msg)                 # kintone レコード No で特定
        self.assertNotIn("doc1", biz_msg)            # documentID は抑止（M06）
        self.assertNotIn("委任契約書", biz_msg)       # 書類タイトルも非表示

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
                self.assertNotIn("doc1", msg)     # M06: documentID は業務チャネルで抑止
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
        self.assertNotIn("doc1", msg)     # M06: documentID は業務チャネルで抑止
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


class TestMismatchEnvelope(unittest.TestCase):
    """P1-102b/c: 失敗経路の App 30「要確認」封筒起票（M06/M07/H03）"""

    APP30 = {"APP_SHIPPING": "30", "TOKEN_SHIPPING": "sh_tok"}

    @staticmethod
    def _resp(payload):
        r = MagicMock()
        r.raise_for_status = MagicMock()
        r.json.return_value = payload
        return r

    @classmethod
    def _records(cls, *pairs):
        """(record_id, document_id) から kintone レコード（$id＋チャネル固有データ）を作る"""
        recs = []
        for rid, doc in pairs:
            key = f"cloudsign_mismatch:{doc}"
            chan = json.dumps(
                {"cloudsign_mismatch": {"冪等キー": key, "documentID": doc,
                                        "失敗理由": "x"}}, ensure_ascii=False)
            recs.append({"$id": {"value": rid},
                         "チャネル固有データ": {"value": chan}})
        return cls._resp({"records": recs})

    def test_files_envelope_when_no_existing(self):
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._resp({"records": []})     # 冪等検索=無し
            rq.post.return_value = self._resp({"id": "555"})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "555")
        rq.post.assert_called_once()
        record = rq.post.call_args.kwargs["json"]["record"]
        self.assertEqual(record["発送ステータス"]["value"], "要確認")
        self.assertEqual(record["チャネル"]["value"], "スキャン受領")
        # トップキー "cloudsign_mismatch" ＋ documentID ＋ 冪等キー ＋ 失敗理由
        chan = record["チャネル固有データ"]["value"]
        self.assertIn("cloudsign_mismatch", chan)
        self.assertIn("doc1", chan)
        self.assertIn("kintone_no_match", chan)

    def test_idempotent_reuses_min_existing_envelope(self):
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(("9", "doc1"), ("7", "doc1"))
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "7")                                  # 最小 No を再利用
        rq.post.assert_not_called()                                # 二重起票しない

    def test_exact_match_excludes_substring_false_positive(self):
        """M07: like は部分一致。冪等キー完全一致で別 documentID(doc12)を誤採用しない。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(("100", "doc12"))  # doc1 の上位文字列
            rq.post.return_value = self._resp({"id": "555"})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "555")                                # doc12 を再利用せず新規起票
        rq.post.assert_called_once()

    def test_special_char_document_id_is_escaped(self):
        """M07: documentID の特殊文字（"）を kintone クエリ用にエスケープする。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._resp({"records": []})
            rq.post.return_value = self._resp({"id": "555"})
            mod.file_mismatch_envelope('doc"1', "kintone_no_match")
        query = rq.get.call_args.kwargs["params"]["query"]
        self.assertIn('doc\\"1', query)                            # " が \" へエスケープ

    def test_compensation_loser_closes_self_and_uses_min(self):
        """H03: create 後の再検索で自分より小さい No があれば自封筒を却下し最小 No へ収束。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.side_effect = [
                self._resp({"records": []}),                       # create 前=無し
                self._records(("3", "doc1"), ("8", "doc1")),       # create 後=並行の3が出現
            ]
            rq.post.return_value = self._resp({"id": "8"})         # 自封筒=8
            rq.put.return_value = self._resp({})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")                                  # 最小 No へ収束
        rq.put.assert_called_once()                                # 自封筒(8)を却下
        put_body = rq.put.call_args.kwargs["json"]
        self.assertEqual(put_body["id"], "8")
        self.assertEqual(put_body["record"]["発送ステータス"]["value"], "却下")

    def test_compensation_winner_keeps_own_no(self):
        """H03: 自分が最小 No なら誰もクローズせず自 No を通知（収束点）。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.side_effect = [
                self._resp({"records": []}),
                self._records(("3", "doc1"), ("8", "doc1")),
            ]
            rq.post.return_value = self._resp({"id": "3"})         # 自封筒=最小
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")
        rq.put.assert_not_called()                                 # 勝者はクローズしない

    def test_close_failure_falls_back_to_own_no(self):
        """H03: 収束クローズ失敗時は warning＋自 No のまま通知（通知は必ず出る）。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.side_effect = [
                self._resp({"records": []}),
                self._records(("3", "doc1"), ("8", "doc1")),
            ]
            rq.post.return_value = self._resp({"id": "8"})
            rq.put.side_effect = RuntimeError("update failed")
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "8")                                  # 自 No で通知継続

    def test_returns_none_when_app30_unset(self):
        env_wo = {"APP_SHIPPING": "", "TOKEN_SHIPPING": ""}
        with patch.dict("os.environ", env_wo, clear=False), \
             patch.object(mod, "requests") as rq:
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertIsNone(no)                                      # 縮退へフォールバック
        rq.get.assert_not_called()
        rq.post.assert_not_called()

    def test_returns_none_on_filing_error(self):
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._resp({"records": []})
            rq.post.side_effect = RuntimeError("kintone down")
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertIsNone(no)                                      # 起票失敗も縮退へ


class TestFailurePathUsesEnvelope(unittest.TestCase):
    """失敗経路の LINE 本文が封筒 record No のみを載せ、documentID/タイトルを載せない"""

    def test_verification_failed_references_envelope_no(self):
        with patch.object(mod, "fetch_document", side_effect=_http_error(404)), \
             patch.object(mod, "update_kintone_status") as mock_update, \
             patch.object(mod, "file_mismatch_envelope", return_value="555") as mock_file, \
             patch.object(mod, "notify_business_line") as mock_biz:
            code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))
        self.assertEqual(body.get("state"), "verification_failed")
        mock_update.assert_not_called()
        mock_file.assert_called_once()
        self.assertEqual(mock_file.call_args.args[0], "doc1")      # documentID を封筒へ
        msg = mock_biz.call_args.args[0]
        self.assertIn("555", msg)                                  # 封筒 record No
        self.assertNotIn("doc1", msg)                              # documentID は非表示
        self.assertNotIn("委任契約書", msg)                         # タイトルは非表示

    def test_kintone_update_failed_references_envelope_no(self):
        with patch.object(mod, "fetch_document", return_value=dict(COMPLETED_DOC)), \
             patch.object(mod, "update_kintone_status", return_value=None), \
             patch.object(mod, "file_mismatch_envelope", return_value="556") as mock_file, \
             patch.object(mod, "notify_business_line") as mock_biz:
            code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))
        self.assertEqual(body.get("state"), "kintone_update_failed")
        mock_file.assert_called_once_with("doc1", "kintone_no_match")
        msg = mock_biz.call_args.args[0]
        self.assertIn("556", msg)
        self.assertNotIn("doc1", msg)

    def test_falls_back_to_degraded_when_filing_fails(self):
        """封筒起票が失敗(None)でも通知は必ず出す（縮退動作へフォールバック）"""
        with patch.object(mod, "fetch_document", side_effect=_http_error(404)), \
             patch.object(mod, "update_kintone_status"), \
             patch.object(mod, "file_mismatch_envelope", return_value=None), \
             patch.object(mod, "notify_business_line") as mock_biz:
            code, body = mod.handle_webhook(SECRET, dict(COMPLETED_EVENT))
        self.assertEqual(body.get("state"), "verification_failed")
        mock_biz.assert_called_once()                              # 通知は必ず出る
        msg = mock_biz.call_args.args[0]
        self.assertIn("失敗分類", msg)
        self.assertNotIn("doc1", msg)                              # 縮退でも documentID 抑止
        self.assertNotIn("record No", msg)                        # 封筒 No は無い


if __name__ == "__main__":
    unittest.main()
