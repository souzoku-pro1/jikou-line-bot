"""TEXT-APPROVAL-GATE-1（QUEUE-2026-10-20-HI H-0）: hub/approved_texts の契約。

  - 台帳一致（key と本文 SHA）のときだけ本文・不一致／未登録／空は None
  - approved_or_safe は安全文を返し key を記録（本文は記録しない）
  - 通知は 1 日 1 回（台帳 key text_unapproved:{JST 日付}・DUPLICATE は送らない・台帳不可は直接送る）・文面は key のみ
  - gate_and_notify は描画を止めない（通知失敗でも安全文を返す）
  - 初期台帳は安全文 1 件のみ
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from hub import approved_texts as AT  # noqa: E402
from hub import send_ledger  # noqa: E402

UNAPPROVED = "相談のご案内です。未承認の本文。"


class _Base(unittest.TestCase):
    def setUp(self):
        AT.reset_pending_for_tests()
        self.addCleanup(AT.reset_pending_for_tests)


class TestLedger(_Base):
    def test_initial_ledger_is_safe_text_only(self):
        self.assertEqual(set(AT.APPROVED_SHA256), {AT.SAFE_TEXT_KEY})
        self.assertEqual(AT.approved_text(AT.SAFE_TEXT_KEY, AT.SAFE_TEXT), AT.SAFE_TEXT)

    def test_unregistered_or_mismatch_is_none(self):
        self.assertIsNone(AT.approved_text("S-1", UNAPPROVED))
        self.assertIsNone(AT.approved_text(AT.SAFE_TEXT_KEY, UNAPPROVED))      # key は在るが本文が違う
        self.assertIsNone(AT.approved_text(AT.SAFE_TEXT_KEY, ""))
        self.assertIsNone(AT.approved_text("", AT.SAFE_TEXT))

    def test_registering_sha_approves_exact_text_only(self):
        with patch.dict(AT.APPROVED_SHA256, {"S-1": AT.sha256_text(UNAPPROVED)}):
            self.assertEqual(AT.approved_text("S-1", UNAPPROVED), UNAPPROVED)
            self.assertIsNone(AT.approved_text("S-1", UNAPPROVED + "。"))      # 本文を変えると再承認まで None


class TestApprovedOrSafe(_Base):
    def test_safe_text_and_key_recorded(self):
        out = AT.approved_or_safe("S-1", UNAPPROVED)
        self.assertEqual(out, AT.SAFE_TEXT)
        self.assertEqual(AT.pending_unapproved_keys(), ("S-1",))
        AT.approved_or_safe("S-3", UNAPPROVED)
        self.assertEqual(AT.pending_unapproved_keys(), ("S-1", "S-3"))

    def test_approved_passes_through_without_record(self):
        self.assertEqual(AT.approved_or_safe(AT.SAFE_TEXT_KEY, AT.SAFE_TEXT), AT.SAFE_TEXT)
        self.assertEqual(AT.pending_unapproved_keys(), ())

    def test_log_has_key_not_text(self):
        with self.assertLogs("hub.approved_texts", level="WARNING") as cm:
            AT.approved_or_safe("S-2", UNAPPROVED)
        joined = "\n".join(cm.output)
        self.assertIn("key=S-2", joined)
        self.assertNotIn(UNAPPROVED, joined)


class TestNotify(_Base):
    def _patch_notify(self, ok=True):
        self.notify = AsyncMock(return_value=ok)
        p = patch("hub.notify.notify_admin_line", self.notify)
        p.start()
        self.addCleanup(p.stop)

    def test_nothing_pending_no_notify(self):
        self._patch_notify()
        self.assertTrue(asyncio.run(AT.notify_unapproved_once()))
        self.notify.assert_not_awaited()

    def test_notifies_once_with_keys_only_and_consumes(self):
        self._patch_notify()
        AT.approved_or_safe("S-1", UNAPPROVED)
        AT.approved_or_safe("S-2", UNAPPROVED)
        with patch.object(send_ledger, "begin", AsyncMock(return_value=None)):       # 台帳不可→直接送る
            self.assertTrue(asyncio.run(AT.notify_unapproved_once()))
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[0]
        self.assertIn("【未承認文言の参照】", text)
        self.assertIn("key=S-1, S-2", text)
        self.assertNotIn(UNAPPROVED, text)
        self.assertEqual(AT.pending_unapproved_keys(), ())

    def test_ledger_duplicate_suppresses(self):
        self._patch_notify()
        AT.approved_or_safe("S-1", UNAPPROVED)
        with patch.object(send_ledger, "begin", AsyncMock(return_value=send_ledger.DUPLICATE_SENT)):
            self.assertTrue(asyncio.run(AT.notify_unapproved_once()))
        self.notify.assert_not_awaited()
        self.assertEqual(AT.pending_unapproved_keys(), ())

    def test_day_key_format(self):
        self.assertRegex(AT._day_key(), r"^text_unapproved:\d{4}-\d{2}-\d{2}$")

    def test_gate_and_notify_returns_safe_even_if_notify_fails(self):
        p = patch("hub.notify.notify_admin_line", AsyncMock(side_effect=RuntimeError("boom")))
        p.start()
        self.addCleanup(p.stop)
        with patch.object(send_ledger, "begin", AsyncMock(return_value=None)):
            out = asyncio.run(AT.gate_and_notify("S-1", UNAPPROVED))
        self.assertEqual(out, AT.SAFE_TEXT)

    def test_gate_and_notify_approved_no_notify(self):
        self._patch_notify()
        out = asyncio.run(AT.gate_and_notify(AT.SAFE_TEXT_KEY, AT.SAFE_TEXT))
        self.assertEqual(out, AT.SAFE_TEXT)
        self.notify.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
