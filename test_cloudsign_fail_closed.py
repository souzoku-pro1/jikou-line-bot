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
    """P1-102b/c/d: 失敗経路の App 30「要確認」封筒起票（M06/M07/H03）"""

    APP30 = {"APP_SHIPPING": "30", "TOKEN_SHIPPING": "sh_tok"}

    @staticmethod
    def _resp(payload, status_code=200):
        r = MagicMock()
        r.status_code = status_code
        r.raise_for_status = MagicMock()
        r.json.return_value = payload
        return r

    @classmethod
    def _records(cls, *rows):
        """(record_id, document_id[, 発送ステータス[, $revision]]) からレコードを作る
        （省略時 ステータス=要確認・revision=1）。$id/$revision/チャネル固有データ/
        発送ステータスを持つ。"""
        recs = []
        for row in rows:
            rid, doc = row[0], row[1]
            status = row[2] if len(row) > 2 else "要確認"
            rev = row[3] if len(row) > 3 else "1"
            key = f"cloudsign_mismatch:{doc}"
            chan = json.dumps(
                {"cloudsign_mismatch": {"冪等キー": key, "documentID": doc,
                                        "失敗理由": "x"}}, ensure_ascii=False)
            recs.append({"$id": {"value": rid},
                         "$revision": {"value": rev},
                         "チャネル固有データ": {"value": chan},
                         "発送ステータス": {"value": status}})
        return cls._resp({"records": recs})

    @staticmethod
    def _put_ids(rq):
        return sorted(c.kwargs["json"]["id"] for c in rq.put.call_args_list)

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

    def test_idempotent_reuses_single_existing(self):
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(("7", "doc1"))     # 既存1件
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "7")                                  # 再利用
        rq.post.assert_not_called()                                # 二重起票しない
        rq.put.assert_not_called()                                 # 単一なら収束不要

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

    # ── H03: 相互不可視 race の後続再送で収束（既存検索での compensation・P1-102d）──
    def test_presearch_convergence_after_invisible_race(self):
        """A:create=3/post=[3]・B:create=8/post=[8] で両者生存 → 後続再送の既存検索で
        [3,8] が見え、winner=3 へ収束。No.8 を却下・No.3(winner)は無変更・通知は No.3。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(("3", "doc1"), ("8", "doc1"))
            rq.put.return_value = self._resp({})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")                                  # 通知は winner No.3
        rq.post.assert_not_called()                                # 再起票しない
        self.assertEqual(self._put_ids(rq), ["8"])                # 敗者8のみ却下・3は無変更
        self.assertEqual(rq.put.call_args.kwargs["json"]
                         ["record"]["発送ステータス"]["value"], "却下")

    def test_presearch_convergence_close_failure_keeps_winner(self):
        """No.8 のクローズが失敗しても winner No.3 を返し通知を維持する。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(("3", "doc1"), ("8", "doc1"))
            rq.put.side_effect = RuntimeError("update failed")
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")                                  # 一部失敗でも winner 継続

    def test_processed_records_are_not_rewritten(self):
        """処理済み(完了)record は敗者でも書き換えない。要確認の敗者のみ却下する。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(
                ("3", "doc1", "要確認"),      # winner（最小・無変更）
                ("5", "doc1", "完了"),        # 人処理済み・敗者だが触らない
                ("8", "doc1", "要確認"))      # 要確認の敗者・却下対象
            rq.put.return_value = self._resp({})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")
        self.assertEqual(self._put_ids(rq), ["8"])                # 8のみ（3=winner,5=完了は不変）

    def test_close_uses_search_time_revision(self):
        """H03 TOCTOU: 却下 PUT に検索時 revision を指定する（楽観ロック）。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(
                ("3", "doc1", "要確認", "2"), ("8", "doc1", "要確認", "4"))
            rq.put.return_value = self._resp({})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")
        put_body = rq.put.call_args.kwargs["json"]
        self.assertEqual(put_body["id"], "8")
        self.assertEqual(put_body["revision"], "4")               # No.8 検索時 revision

    def test_revision_conflict_is_human_wins(self):
        """H03 TOCTOU: 検索時 No.8=要確認/revision=4 だが PUT で revision 競合(409)
        → 再試行・無条件更新なしで却下せず、winner No.3 通知継続・No.8 不変。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(
                ("3", "doc1", "要確認", "2"), ("8", "doc1", "要確認", "4"))
            rq.put.return_value = self._resp({}, status_code=409)  # revision 競合
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")                                 # winner 継続（通知 No.3）
        rq.put.assert_called_once()                               # 再試行しない（1回のみ）
        put_body = rq.put.call_args.kwargs["json"]
        self.assertEqual(put_body["id"], "8")
        self.assertEqual(put_body["revision"], "4")               # 無条件更新でない
        # 409 応答で raise_for_status を呼ばない＝例外化せず人更新優先で握る
        rq.put.return_value.raise_for_status.assert_not_called()

    def test_missing_revision_skips_close_presearch(self):
        """revision 必須（P1-102f）: 検索時 No.8=要確認だが revision 欠落
        → 却下 PUT を送らず（無条件 PUT なし）、固定 warning のみ・winner No.3 通知継続。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.return_value = self._records(
                ("3", "doc1", "要確認", "2"), ("8", "doc1", "要確認", ""))
            with self.assertLogs("cloudsign", level="WARNING") as cm:
                no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")                    # winner 継続（通知 No.3）
        rq.put.assert_not_called()                   # PUT 0（無条件更新しない）
        self.assertTrue(any("revision欠落" in m for m in cm.output))  # 固定 warning

    def test_missing_revision_skips_close_postcreate(self):
        """create 応答で自 revision 欠落＋再検索にも自レコード未反映 → 自 No を収束対象に
        含めても revision 欠落で却下 PUT せず、winner No.3 通知を維持する。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.side_effect = [
                self._resp({"records": []}),                     # create 前=無し
                self._records(("3", "doc1", "要確認", "2")),      # 再検索に自(8)未反映
            ]
            rq.post.return_value = self._resp({"id": "8"})       # 自 revision 欠落
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")                    # winner 継続
        rq.put.assert_not_called()                   # 自(8)は revision 欠落で却下せず

    # ── H03: create 後の再検索での compensation（可視な並行）──
    def test_postcreate_loser_closes_and_converges(self):
        """自 No=8 で create 後、再検索で 3 が見えたら 8 を却下し winner=3 へ収束。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.side_effect = [
                self._resp({"records": []}),                       # create 前=無し
                self._records(("3", "doc1"), ("8", "doc1")),       # create 後=並行の3が可視
            ]
            rq.post.return_value = self._resp({"id": "8"})
            rq.put.return_value = self._resp({})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")
        self.assertEqual(self._put_ids(rq), ["8"])                # 敗者8を却下・3は無変更

    def test_postcreate_winner_closes_loser(self):
        """自 No=3（最小）で create 後、敗者 8 を却下する（winner は自分を閉じない）。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.side_effect = [
                self._resp({"records": []}),
                self._records(("3", "doc1"), ("8", "doc1")),
            ]
            rq.post.return_value = self._resp({"id": "3"})
            rq.put.return_value = self._resp({})
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")
        self.assertEqual(self._put_ids(rq), ["8"])                # 敗者8のみ・自封筒3は閉じない

    def test_postcreate_close_failure_returns_winner(self):
        """create 後の収束クローズが失敗しても winner No を返す（通知は必ず出る）。"""
        with patch.dict("os.environ", self.APP30, clear=False), \
             patch.object(mod, "requests") as rq:
            rq.get.side_effect = [
                self._resp({"records": []}),
                self._records(("3", "doc1"), ("8", "doc1")),
            ]
            rq.post.return_value = self._resp({"id": "8"})
            rq.put.side_effect = RuntimeError("update failed")
            no = mod.file_mismatch_envelope("doc1", "kintone_no_match")
        self.assertEqual(no, "3")                                  # winner を維持

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
