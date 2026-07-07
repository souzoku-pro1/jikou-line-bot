"""hub/notify.py の単体テスト（T0-2）

検証: スロットル・未設定時スキップ・4900字切り詰め・失敗時に例外を出さないこと・
claude_gateway からの re-export 互換。
"""

import json
import unittest
from unittest.mock import patch

from hub import notify

_ENV = {
    "LINE_CHANNEL_ACCESS_TOKEN": "line_tok",
    # 業務通知チャネル（2026-07-07 裁定）。値を固定して他テストモジュールの
    # process env 漏れ（bot_token 等）による非決定性を遮断
    "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "bot_tok",
    "ATTORNEY_LINE_USER_ID": "U_admin",
}


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code
        self.text = "{}"

    @property
    def is_success(self):
        return 200 <= self.status_code < 300


class FakeClient:
    queue: list = []
    calls: list = []

    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, **kw):
        FakeClient.calls.append((url, kw))
        item = FakeClient.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def use_fake(queue):
    FakeClient.queue = list(queue)
    FakeClient.calls = []
    return patch("hub.notify.httpx.AsyncClient", FakeClient)


class TestNotifyAdminLine(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._env = patch.dict("os.environ", _ENV, clear=False)
        self._env.start()
        self.addCleanup(self._env.stop)
        notify._last_notify_at.clear()

    async def test_sends_to_admin(self):
        with use_fake([FakeResponse(200)]):
            await notify.notify_admin_line("警報テスト")
        self.assertEqual(len(FakeClient.calls), 1)
        _, kw = FakeClient.calls[0]
        self.assertEqual(kw["json"]["to"], "U_admin")
        self.assertEqual(kw["json"]["messages"][0]["text"], "警報テスト")
        # 2026-07-07 裁定: 業務通知は指示Botチャネル（旧: line_tok=顧客Bot）
        self.assertEqual(kw["headers"]["Authorization"], "Bearer bot_tok")

    async def test_throttle_suppresses_same_key(self):
        with use_fake([FakeResponse(200), FakeResponse(200)]):
            with patch("hub.notify.time.monotonic", side_effect=[1000.0, 1100.0]):
                await notify.notify_admin_line("1回目", throttle_key="k1")
                await notify.notify_admin_line("2回目", throttle_key="k1")
        self.assertEqual(len(FakeClient.calls), 1, "300秒未満の同一キーは抑制される")

    async def test_throttle_allows_after_interval(self):
        with use_fake([FakeResponse(200), FakeResponse(200)]):
            with patch("hub.notify.time.monotonic", side_effect=[1000.0, 1400.0]):
                await notify.notify_admin_line("1回目", throttle_key="k1")
                await notify.notify_admin_line("2回目", throttle_key="k1")
        self.assertEqual(len(FakeClient.calls), 2, "間隔経過後は再送される")

    async def test_throttle_is_per_key(self):
        with use_fake([FakeResponse(200), FakeResponse(200)]):
            with patch("hub.notify.time.monotonic", side_effect=[1000.0, 1001.0]):
                await notify.notify_admin_line("A", throttle_key="k1")
                await notify.notify_admin_line("B", throttle_key="k2")
        self.assertEqual(len(FakeClient.calls), 2)

    async def test_no_throttle_when_key_empty(self):
        with use_fake([FakeResponse(200), FakeResponse(200)]):
            await notify.notify_admin_line("A")
            await notify.notify_admin_line("B")
        self.assertEqual(len(FakeClient.calls), 2)

    async def test_skips_when_admin_unset(self):
        env = {"LINE_CHANNEL_ACCESS_TOKEN": "line_tok",
               "ATTORNEY_LINE_USER_ID": "", "LINE_ADMIN_USER_ID": ""}
        with patch.dict("os.environ", env, clear=False):
            with use_fake([]):
                await notify.notify_admin_line("誰にも送らない")
        self.assertEqual(len(FakeClient.calls), 0)

    async def test_truncates_to_4900_chars(self):
        with use_fake([FakeResponse(200)]):
            await notify.notify_admin_line("あ" * 6000)
        _, kw = FakeClient.calls[0]
        self.assertEqual(len(kw["json"]["messages"][0]["text"]), 4900)

    async def test_http_failure_does_not_raise(self):
        with use_fake([FakeResponse(500)]):
            await notify.notify_admin_line("失敗しても例外なし")  # 例外が出ないこと

    async def test_transport_error_does_not_raise(self):
        with use_fake([ConnectionError("down")]):
            await notify.notify_admin_line("通信断でも例外なし")


class TestPushLineMessage(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._env = patch.dict("os.environ", _ENV, clear=False)
        self._env.start()
        self.addCleanup(self._env.stop)

    async def test_returns_true_on_success(self):
        with use_fake([FakeResponse(200)]):
            ok = await notify.push_line_message("U1", "hello")
        self.assertTrue(ok)

    async def test_returns_false_on_failure(self):
        with use_fake([FakeResponse(400)]):
            ok = await notify.push_line_message("U1", "hello")
        self.assertFalse(ok)

    async def test_returns_false_without_token(self):
        with patch.dict("os.environ", {"LINE_CHANNEL_ACCESS_TOKEN": ""}, clear=False):
            with use_fake([]):
                ok = await notify.push_line_message("U1", "hello")
        self.assertFalse(ok)
        self.assertEqual(len(FakeClient.calls), 0)

    async def test_default_channel_is_customer_bot(self):
        """既存呼び出し元の無変更回帰: token_env 省略時は従来どおり
        LINE_CHANNEL_ACCESS_TOKEN のチャネルで送る"""
        with use_fake([FakeResponse(200)]):
            ok = await notify.push_line_message("U1", "hello")
        self.assertTrue(ok)
        _, kw = FakeClient.calls[0]
        self.assertEqual(kw["headers"]["Authorization"], "Bearer line_tok")

    async def test_token_env_selects_channel(self):
        """token_env 指定で送信チャネル（Authorization ヘッダ）が切り替わる"""
        env = {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "bot_tok"}
        with patch.dict("os.environ", env, clear=False), \
                use_fake([FakeResponse(200)]):
            ok = await notify.push_line_message(
                "U1", "hello", token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN")
        self.assertTrue(ok)
        _, kw = FakeClient.calls[0]
        self.assertEqual(kw["headers"]["Authorization"], "Bearer bot_tok")

    async def test_token_env_unset_skips(self):
        """指定した token_env が未設定なら送らない（既定と同じ縮退）"""
        with patch.dict("os.environ",
                        {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": ""}, clear=False):
            with use_fake([]):
                ok = await notify.push_line_message(
                    "U1", "x", token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN")
        self.assertFalse(ok)
        self.assertEqual(len(FakeClient.calls), 0)


class TestBusinessChannel(unittest.IsolatedAsyncioTestCase):
    """業務通知の送信チャネル（2026-07-07 裁定）: 指示Botから送る・
    DISPATCHBOT 未設定は既定へフォールバック＋警告ログ（警報の欠落防止）"""

    def setUp(self):
        self._env = patch.dict("os.environ", _ENV, clear=False)
        self._env.start()
        self.addCleanup(self._env.stop)
        notify._last_notify_at.clear()

    async def test_admin_notify_uses_dispatch_bot_channel(self):
        with use_fake([FakeResponse(200)]):
            await notify.notify_admin_line("警報")
        _, kw = FakeClient.calls[0]
        self.assertEqual(kw["headers"]["Authorization"], "Bearer bot_tok",
                         "管理者警報は指示Botチャネル（ヘッダのピン留め）")

    async def test_attorney_approval_uses_dispatch_bot_channel(self):
        record = {"$id": {"value": "1"}, "件名": {"value": "x"},
                  "チャネル": {"value": "郵送"}, "顧客名表示用": {"value": "y"}}
        with use_fake([FakeResponse(200)]):
            await notify.notify_attorney_approval(record)
        _, kw = FakeClient.calls[0]
        self.assertEqual(kw["headers"]["Authorization"], "Bearer bot_tok",
                         "承認依頼は指示Botチャネル（ヘッダのピン留め）")

    async def test_fallback_to_customer_bot_with_warning_when_unset(self):
        with patch.dict("os.environ",
                        {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": ""}, clear=False):
            with self.assertLogs("hub.notify", level="WARNING") as logs:
                with use_fake([FakeResponse(200)]):
                    await notify.notify_admin_line("警報")
        _, kw = FakeClient.calls[0]
        self.assertEqual(kw["headers"]["Authorization"], "Bearer line_tok",
                         "未設定時は既定へフォールバック（警報の欠落防止）")
        self.assertTrue(any("falls back" in m for m in logs.output))

    def test_business_token_env_values(self):
        self.assertEqual(notify.business_token_env(),
                         "DISPATCHBOT_CHANNEL_ACCESS_TOKEN")
        with patch.dict("os.environ",
                        {"DISPATCHBOT_CHANNEL_ACCESS_TOKEN": ""}, clear=False):
            with self.assertLogs("hub.notify", level="WARNING"):
                self.assertEqual(notify.business_token_env(),
                                 "LINE_CHANNEL_ACCESS_TOKEN")


class TestReExport(unittest.TestCase):
    def test_claude_gateway_reexports_notify_admin_line(self):
        """既存の import 経路（from claude_gateway import notify_admin_line）が生きていること"""
        import os
        os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")
        import claude_gateway
        self.assertIs(claude_gateway.notify_admin_line, notify.notify_admin_line)


if __name__ == "__main__":
    unittest.main()
