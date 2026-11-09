# -*- coding: utf-8 -*-
"""TEXT-APPROVAL-4（2026-11-09・大野承認）: K 後追い下書きの管理者向け通知 3 件（hub/k_draft.NOTICE_*_TEXT）を承認台帳に反映。

固定する内容
 - 台帳（hub/approved_texts.APPROVED_SHA256）の追加は 3 行のみ: 閉集合 6 key・他 key の SHA 不変・ソース上の `ops_k_draft_` 行は 3 行
 - 3 件の SHA は本文（NOTICE_*_TEXT）の sha256 と全桁一致＝本文を 1 字も変えていない。1 字違えば不一致（再承認）
 - approved_or_safe で本文がそのまま通る（安全文に置換されない・未承認 key の記録なし）
 - 承認後: notify_once は台帳（send_ledger・key `k_draft_<key>:{JST 日付}`）を通して業務 LINE へ **各 1 日 1 回**（同日 2 回目は throttled・HTTP 1 回）。未承認通知は出ない
 - 承認前の状態（台帳から 3 key を外す）: 送らず key を記録し、未承認通知（text_unapproved:{JST 日付}）が **1 日 1 回**
"""
import asyncio
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

for _k, _v in {
    "KINTONE_SUBDOMAIN": "testsub", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "ANTHROPIC_API_KEY": "dummy_key",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
}.items():
    os.environ.setdefault(_k, _v)   # hub の import に要る既定値（秘密値ではない）

from hub import approved_texts as AT  # noqa: E402
from hub import db  # noqa: E402
from hub import k_draft as kd  # noqa: E402
from hub import notify as hn  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402

REPO = Path(__file__).parent

# 大野が承認した SHA（票 TEXT-APPROVAL-4・全桁）
SHA = {
    "ops_k_draft_invalid_model_v1": "66ab00735f5d7a6c1616ade79c087d6ae53b4f8a63c45bda2f151257f3efc5e6",
    "ops_k_draft_failed_v1": "3159a6b8149829226b831724815f78bb6617035fb457894ac934cf432f268612",
    "ops_k_draft_cost_cap_v1": "12a6c8ea8047718815deb33eaedccb2eaf8e5e3a75ff19d47c66697dda37ef86",
}
SHA_AGE_V1 = "59da41fcf2545b9aaa19175ea18315388e5b1e6df1c65bdbccaccacfc879652d"
SHA_AGE_V2 = "b91070d861198b77c39af30b69e38fa8fef46b28c7fd8a1971813a5f8a78d5c1"


def _run(coro):
    return asyncio.run(coro)


class TestLedgerThreeLines(unittest.TestCase):
    def setUp(self):
        AT.reset_pending_for_tests()

    def test_sha_matches_text_verbatim(self):
        self.assertEqual(set(SHA), set(kd.NOTICE_TEXTS))                                   # 3 件＝k_draft の通知 3 種
        for key, sha in SHA.items():
            text = kd.NOTICE_TEXTS[key]
            self.assertEqual(AT.APPROVED_SHA256[key], sha)                                  # 台帳の値は承認した SHA（全桁）
            self.assertEqual(AT.sha256_text(text), sha)                                     # 本文は 1 字も変えていない
            self.assertTrue(AT.is_approved(key, text))
            self.assertIsNone(AT.approved_text(key, text + "。"))                           # 1 字違えば不一致＝再承認
            self.assertTrue(text.startswith("【K 下書き】"))
            self.assertNotIn("{{", text)                                                    # 差し込みスロットなし（固定文）

    def test_closed_set_and_other_keys_unchanged(self):
        self.assertEqual(set(AT.APPROVED_SHA256), {AT.SAFE_TEXT_KEY, sp.AGE_NOTICE_TEXT_KEY, sp.AGE_NOTICE_TEXT_KEY_V2, *SHA})
        self.assertEqual(AT.APPROVED_SHA256[AT.SAFE_TEXT_KEY], AT.sha256_text(AT.SAFE_TEXT))
        self.assertEqual(AT.APPROVED_SHA256[sp.AGE_NOTICE_TEXT_KEY], SHA_AGE_V1)
        self.assertEqual(AT.APPROVED_SHA256[sp.AGE_NOTICE_TEXT_KEY_V2], SHA_AGE_V2)
        self.assertEqual(AT.SAFE_TEXT, "現在準備中です。弁護士より改めてご連絡いたします。")   # 安全文不変

    def test_source_has_exactly_three_k_entry_lines(self):
        lines = [l for l in (REPO / "hub" / "approved_texts.py").read_text(encoding="utf-8").splitlines() if "ops_k_draft_" in l]
        self.assertEqual(len(lines), 3)
        for l in lines:
            self.assertRegex(l, r'^    "ops_k_draft_[a-z_]+_v1": "[0-9a-f]{64}",')          # 辞書の項目行だけ（関数・分岐の追加なし）
        self.assertEqual(len(AT.APPROVED_SHA256), 6)

    def test_approved_or_safe_passes_text_and_records_nothing(self):
        for key, text in kd.NOTICE_TEXTS.items():
            out = AT.approved_or_safe(key, text)
            self.assertEqual(out, text)
            self.assertIsNot(out, AT.SAFE_TEXT)
        self.assertEqual(AT.pending_unapproved_keys(), ())                                  # 未承認 key の記録なし


class TestNotifyOncePerDay(unittest.TestCase):
    """notify_once を本物の台帳（sqlite）で: 承認後は各 1 日 1 回・承認前は未承認通知 1 日 1 回。"""

    def setUp(self):
        self._dir = tempfile.mkdtemp(prefix="ta4_")
        self._env = patch.dict(os.environ, {"DATABASE_URL": f"sqlite+aiosqlite:///{self._dir}/l.db",
                                            "ATTORNEY_LINE_USER_ID": "Uadmin_test_only"})
        self._env.start()
        os.environ.pop(sl.POLICY_V2_ENV, None)
        db.reset_for_tests()
        sp.clear_condition_cache()
        AT.reset_pending_for_tests()

        async def _create():
            eng = db.get_async_engine()
            async with eng.begin() as c:
                await c.run_sync(sl.metadata.create_all)
        _run(_create())

    def tearDown(self):
        db.reset_for_tests()
        self._env.stop()
        AT.reset_pending_for_tests()
        shutil.rmtree(self._dir, ignore_errors=True)

    def test_approved_each_kind_sent_once_a_day_via_ledger(self):
        push = AsyncMock(return_value=True)
        unapproved_notice = AsyncMock(return_value=True)
        with patch.object(sp, "_push_business_http", push), patch.object(AT, "notify_unapproved_once", unapproved_notice):
            for key in kd.NOTICE_TEXTS:                                                      # 台帳は patch しない（承認済みの本番状態）
                self.assertEqual(_run(kd.notify_once(key)), rp.NOTIFY_SENT)
                self.assertEqual(_run(kd.notify_once(key)), rp.NOTIFY_THROTTLED)             # 同日 2 回目は台帳で止まる（HTTP なし）
        self.assertEqual(push.await_count, 3)                                               # 3 種 × 1 日 1 回
        self.assertEqual([c.args[1] for c in push.await_args_list], [kd.NOTICE_TEXTS[k] for k in kd.NOTICE_TEXTS])   # 本文そのまま
        unapproved_notice.assert_not_awaited()                                              # 未承認通知は出ない
        self.assertEqual(AT.pending_unapproved_keys(), ())

    def test_before_approval_not_sent_and_unapproved_notice_once_a_day(self):
        push = AsyncMock(return_value=True)
        admin = AsyncMock(return_value=True)
        before = {k: v for k, v in AT.APPROVED_SHA256.items() if k not in kd.NOTICE_TEXTS}   # TEXT-APPROVAL-4 前（＝本文を変えて SHA が外れた状態）
        with patch.dict(AT.APPROVED_SHA256, before, clear=True), patch.object(sp, "_push_business_http", push), \
             patch.object(hn, "notify_admin_line", admin), patch.object(hn, "get_admin_line_user_id", lambda: "Uadmin_test_only"):
            for key in kd.NOTICE_TEXTS:
                self.assertEqual(_run(kd.notify_once(key)), "needs_approval")
                self.assertEqual(_run(kd.notify_once(key)), "needs_approval")
        push.assert_not_awaited()                                                           # 承認前の通知文は 1 通も送らない
        self.assertEqual(admin.await_count, 1)                                              # 未承認通知は 1 日 1 回（text_unapproved:{JST 日付}）
        self.assertIn("ops_k_draft_", admin.await_args.args[0])
        self.assertNotIn("Uadmin_test_only", admin.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
