"""
クレジット残高系エラーの警報化（claude_gateway）の回帰テスト

背景:
  2026-07-03 に Anthropic API のクレジット残高不足（400）が発生した際、
  このエラーはメッセージに "model" を含まずモデル起因判定に乗らないため、
  フォールバック警報が発動せず無警報で沈黙した。
  対策として create_message_with_fallback() に残高系エラーの検知
  → 管理者 LINE 警報（throttle_key="billing_error"）→ 送出を追加した。

オフラインで実行できる（Claude API・LINE API は呼ばない）:
  python -m pytest test_claude_gateway_billing.py -v
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import anthropic
import httpx

import claude_gateway
from claude_gateway import (
    ClaudeUnavailableError,
    _is_billing_error,
    _is_model_error,
    create_message_with_fallback,
)

CREDIT_MSG = (
    "Your credit balance is too low to access the Anthropic API. "
    "Please go to Plans & Billing to upgrade or purchase credits."
)
MODEL_MSG = "The model `claude-old-1` is deprecated."


def _make_error(cls, message: str):
    req = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    resp = httpx.Response(400, request=req)
    return cls(message, response=resp, body={"error": {"message": message}})


class _FakeMessages:
    """messages.create の呼び出しごとに exc_or_result を順に返すスタブ"""

    def __init__(self, sequence):
        self.sequence = list(sequence)
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        item = self.sequence.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class _FakeClient:
    def __init__(self, sequence):
        self.messages = _FakeMessages(sequence)


class TestIsBillingError(unittest.TestCase):
    def test_credit_balance_400_is_billing_error(self):
        exc = _make_error(anthropic.BadRequestError, CREDIT_MSG)
        self.assertTrue(_is_billing_error(exc))

    def test_billing_error_is_not_model_error(self):
        """残高系400がモデル起因判定に乗らないこと（無警報沈黙の原因の固定）"""
        exc = _make_error(anthropic.BadRequestError, CREDIT_MSG)
        self.assertFalse(_is_model_error(exc))

    def test_model_deprecation_400_is_not_billing_error(self):
        exc = _make_error(anthropic.BadRequestError, MODEL_MSG)
        self.assertFalse(_is_billing_error(exc))
        self.assertTrue(_is_model_error(exc))


class TestBillingAlert(unittest.TestCase):
    def test_billing_error_notifies_admin_and_reraises(self):
        """残高系エラー: 管理者警報が発火し、元の例外がそのまま送出される"""
        billing_exc = _make_error(anthropic.BadRequestError, CREDIT_MSG)
        client = _FakeClient([billing_exc])
        mock_notify = AsyncMock()

        with patch.object(claude_gateway, "notify_admin_line", mock_notify):
            with self.assertRaises(anthropic.BadRequestError):
                asyncio.run(
                    create_message_with_fallback(client, context="テスト", messages=[])
                )

        # 管理者警報が1回発火し、残高系の文言・スロットルキーを持つこと
        mock_notify.assert_awaited_once()
        args, kwargs = mock_notify.await_args
        self.assertIn("クレジット残高不足", args[0])
        self.assertIn("Plans & Billing", args[0])
        self.assertEqual(kwargs.get("throttle_key"), "billing_error")
        # フォールバックモデルでの再試行は行われない（同一アカウントのため無意味）
        self.assertEqual(len(client.messages.calls), 1)

    def test_model_error_still_falls_back(self):
        """既存挙動の保護: モデル起因エラーは従来どおりフォールバックする"""
        model_exc = _make_error(anthropic.BadRequestError, MODEL_MSG)
        sentinel = object()
        client = _FakeClient([model_exc, sentinel])
        mock_notify = AsyncMock()

        with patch.object(claude_gateway, "notify_admin_line", mock_notify):
            result = asyncio.run(
                create_message_with_fallback(client, context="テスト", messages=[])
            )

        self.assertIs(result, sentinel)
        self.assertEqual(len(client.messages.calls), 2)
        args, kwargs = mock_notify.await_args
        self.assertIn("フォールバック発動", args[0])

    def test_both_models_fail_still_raises_unavailable(self):
        """既存挙動の保護: 両モデル失敗は従来どおり ClaudeUnavailableError"""
        model_exc = _make_error(anthropic.BadRequestError, MODEL_MSG)
        fallback_exc = _make_error(anthropic.BadRequestError, MODEL_MSG)
        client = _FakeClient([model_exc, fallback_exc])

        with patch.object(claude_gateway, "notify_admin_line", AsyncMock()):
            with self.assertRaises(ClaudeUnavailableError):
                asyncio.run(
                    create_message_with_fallback(client, context="テスト", messages=[])
                )


if __name__ == "__main__":
    unittest.main()
