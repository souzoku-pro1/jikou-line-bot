"""hub/approval.py の単体テスト（T1-2）

- 全状態 10×10 = 100 組の総当たりで、許可遷移（8組）以外がすべて拒否されること
- ██ 絶対制約 ██「承認待ち→承認済」を含む『→承認済』の遷移がサーバーに存在しないこと
- claim_execution の冪等（既実行 / revision 競合）
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from hub import approval, kintone
from hub.approval import SERVER_TRANSITIONS, STATUSES, TransitionError

APP = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

# 設計 01 §4 / 03 §5.1 の許可遷移（テスト側に複製して回帰を検知する）
EXPECTED_ALLOWED = {
    ("下書き", "承認待ち"), ("下書き", "エラー"),
    ("承認済", "発送処理中"),
    ("発送処理中", "発送済"), ("発送処理中", "エラー"),
    ("発送済", "返送待ち"), ("発送済", "完了"),
    ("返送待ち", "完了"),
}


class TestTransitionTable(unittest.IsolatedAsyncioTestCase):
    async def test_table_matches_design(self):
        self.assertEqual(set(SERVER_TRANSITIONS), EXPECTED_ALLOWED)

    async def test_all_100_pairs_exhaustive(self):
        """全10状態×10状態の総当たり: 許可8組は更新実行・それ以外は拒否+警報"""
        for frm in STATUSES:
            for to in STATUSES:
                with self.subTest(frm=frm, to=to):
                    with patch("hub.kintone.update_record", new=AsyncMock()) as mock_up, \
                         patch("hub.notify.notify_admin_line", new=AsyncMock()) as mock_alert:
                        if (frm, to) in EXPECTED_ALLOWED:
                            await approval.transition(APP, "1", frm, to)
                            mock_up.assert_awaited_once()
                            mock_alert.assert_not_awaited()
                        else:
                            with self.assertRaises(TransitionError):
                                await approval.transition(APP, "1", frm, to)
                            mock_up.assert_not_awaited()
                            mock_alert.assert_awaited_once()

    async def test_absolute_constraint_no_server_path_to_approved(self):
        """██ 絶対制約 ██ 遷移先が「承認済」の組がサーバー遷移表に1つも存在しない"""
        for frm, to in SERVER_TRANSITIONS:
            self.assertNotEqual(to, "承認済",
                                f"サーバー遷移表に →承認済 が混入している: {frm}→{to}")

    async def test_absolute_constraint_pending_to_approved_raises(self):
        with patch("hub.kintone.update_record", new=AsyncMock()) as mock_up, \
             patch("hub.notify.notify_admin_line", new=AsyncMock()):
            with self.assertRaises(TransitionError):
                await approval.transition(APP, "1", "承認待ち", "承認済")
            mock_up.assert_not_awaited()

    async def test_transition_writes_status_and_extra(self):
        with patch("hub.kintone.update_record", new=AsyncMock()) as mock_up, \
             patch("hub.notify.notify_admin_line", new=AsyncMock()):
            await approval.transition(APP, "5", "下書き", "承認待ち",
                                      extra_fields={"成果物": [{"fileKey": "fk"}]})
        args = mock_up.await_args
        self.assertEqual(args.args[1], "5")
        self.assertEqual(args.args[2],
                         {"発送ステータス": "承認待ち", "成果物": [{"fileKey": "fk"}]})


def _record(executed="no", revision="3"):
    return {"$id": {"value": "7"}, "$revision": {"value": revision},
            "実行済み": {"value": executed}}


class TestClaimExecution(unittest.IsolatedAsyncioTestCase):
    async def test_claims_with_revision(self):
        with patch("hub.kintone.update_record", new=AsyncMock()) as mock_up:
            ok = await approval.claim_execution(APP, _record())
        self.assertTrue(ok)
        kw = mock_up.await_args.kwargs
        self.assertEqual(mock_up.await_args.args[2], {"実行済み": "yes"})
        self.assertEqual(kw["revision"], "3")

    async def test_already_executed_returns_false_without_api_call(self):
        with patch("hub.kintone.update_record", new=AsyncMock()) as mock_up:
            ok = await approval.claim_execution(APP, _record(executed="yes"))
        self.assertFalse(ok)
        mock_up.assert_not_awaited()

    async def test_revision_conflict_returns_false(self):
        conflict = kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        with patch("hub.kintone.update_record", new=AsyncMock(side_effect=conflict)):
            ok = await approval.claim_execution(APP, _record())
        self.assertFalse(ok)


class TestSourceLevelGuarantee(unittest.TestCase):
    def test_dispatch_module_never_writes_status_directly(self):
        """hub/dispatch.py が 発送ステータス を直接書かない
        （遷移は approval.transition 経由のみ＝遷移表の強制が破れない）"""
        import inspect

        from hub import dispatch
        src = inspect.getsource(dispatch)
        self.assertNotIn('"発送ステータス":', src)
        self.assertNotIn("'発送ステータス':", src)


if __name__ == "__main__":
    unittest.main()
