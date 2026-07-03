"""hub/webhook_auth.py の単体テスト（T0-1）"""

import unittest
from unittest.mock import AsyncMock, patch

from hub import kintone
from hub.webhook_auth import extract_record_id, refetch_and_check, verify_token

APP = kintone.KintoneApp("テスト", "TEST_APP_ID", "TEST_TOKEN")


class TestVerifyToken(unittest.TestCase):
    def test_match(self):
        with patch.dict("os.environ", {"WH_TOKEN": "secret123"}):
            self.assertTrue(verify_token("secret123", "WH_TOKEN"))

    def test_mismatch(self):
        with patch.dict("os.environ", {"WH_TOKEN": "secret123"}):
            self.assertFalse(verify_token("wrong", "WH_TOKEN"))

    def test_env_unset_is_deny_all(self):
        """環境変数未設定なら空文字一致でも False（deny-all）"""
        with patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("WH_TOKEN_UNSET", None)
            self.assertFalse(verify_token("", "WH_TOKEN_UNSET"))
            self.assertFalse(verify_token("anything", "WH_TOKEN_UNSET"))

    def test_none_supplied(self):
        with patch.dict("os.environ", {"WH_TOKEN": "secret123"}):
            self.assertFalse(verify_token(None, "WH_TOKEN"))


class TestExtractRecordId(unittest.TestCase):
    def test_id_from_record_body(self):
        body = {"record": {"$id": {"value": "42"}}}
        self.assertEqual(extract_record_id(body), "42")

    def test_fallback_to_record_id_key(self):
        self.assertEqual(extract_record_id({"recordId": 7}), "7")

    def test_missing_returns_none(self):
        self.assertIsNone(extract_record_id({}))
        self.assertIsNone(extract_record_id({"record": "not-a-dict"}))

    def test_numeric_id_is_stringified(self):
        body = {"record": {"$id": {"value": 99}}}
        self.assertEqual(extract_record_id(body), "99")


class TestRefetchAndCheck(unittest.IsolatedAsyncioTestCase):
    async def test_returns_record_when_expects_satisfied(self):
        record = {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "no"}}
        with patch("hub.webhook_auth.kintone.get_record", new=AsyncMock(return_value=record)):
            got = await refetch_and_check(APP, "1", {"ステータス2": "承認済", "送信済み": "no"})
        self.assertEqual(got, record)

    async def test_returns_none_when_value_differs(self):
        record = {"ステータス2": {"value": "承認済"}, "送信済み": {"value": "yes"}}
        with patch("hub.webhook_auth.kintone.get_record", new=AsyncMock(return_value=record)):
            got = await refetch_and_check(APP, "1", {"ステータス2": "承認済", "送信済み": "no"})
        self.assertIsNone(got)

    async def test_returns_none_when_field_missing(self):
        with patch("hub.webhook_auth.kintone.get_record", new=AsyncMock(return_value={})):
            got = await refetch_and_check(APP, "1", {"ステータス2": "承認済"})
        self.assertIsNone(got)

    async def test_returns_none_on_kintone_error(self):
        with patch("hub.webhook_auth.kintone.get_record",
                   new=AsyncMock(side_effect=kintone.KintoneError(404, "X", "nf"))):
            got = await refetch_and_check(APP, "1", {"ステータス2": "承認済"})
        self.assertIsNone(got)


if __name__ == "__main__":
    unittest.main()
