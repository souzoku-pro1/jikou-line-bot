"""JIKOU-REPLY-Q1b-fix5（Codex R-Q1b-fix4 BR-16・17）の回帰テスト。

共通フィクスチャは test_jikou_reply_q1b・fix2・fix4 を再利用。
- BR-16: claim は条件付き UPDATE と同一 tx で最新行を返す／process_one は claim 後の行だけで判断する。
         A が failed を読み claim 前で待機 → B が claim・外部作成・応答喪失で unknown → A が claim 成功 → A は unknown を見て作成しない
         （外部作成 1 回・順序は threading.Event で固定）
- BR-17: 確定状態 create_state と診断理由 last_result の分離／宛先取得の一時失敗で unknown が消えない／単調遷移（逆行・上書きは拒否）／
         migration a7c9e1f3b5d7（f7b2 の次・既存行の移行・none 以外があれば downgrade 拒否）
"""
import asyncio
import datetime
import shutil
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from test_db_foundation import TestAlembicScaffold  # noqa: E402
from test_jikou_reply_q1b import OP, REC_ID, REPO, TEXT, USER, _FakeClient, _run, db, rf, rv, sl  # noqa: E402
from test_jikou_reply_q1b_fix2 import OP3, _args  # noqa: E402
from test_jikou_reply_q1b_fix4 import _Fix4Mixin, _hit  # noqa: E402

F7B2 = "f7b2c9d4e1a6"
A7C9 = "a7c9e1f3b5d7"


async def _wait(ev: threading.Event):
    """threading.Event をイベントループを塞がずに待つ。"""
    await asyncio.get_running_loop().run_in_executor(None, ev.wait)


# ── BR-16: claim 前の古い行で判断しない ──────────────────────────────────────────
class TestClaimReadsBack(_Fix4Mixin):
    def test_claim_returns_fresh_row_in_same_transaction(self):
        self._pending()
        row = _run(rf.claim(OP))
        self.assertEqual((row["op_id"], row["state"], row["create_state"], row["last_result"]),
                         (OP, "in_progress", "none", "failed"))
        self.assertTrue(row["owner_token"])
        self.assertEqual(self.followup()["owner_token"], row["owner_token"])
        self.assertIsNone(_run(rf.claim(OP)))                                       # 取れなければ None

    def test_stale_pre_claim_row_does_not_cause_create(self):
        """A が failed を読み claim 前で待機 → B が claim・外部作成・応答喪失（unknown）→ A が claim 成功 → A は unknown を見て作成しない。"""
        self._pending()
        a_read = threading.Event()
        b_done = threading.Event()
        calls = {"n": 0}
        orig_claim = rf.claim

        async def _claim(op_id, *, now=None):
            calls["n"] += 1
            if calls["n"] == 1:                                                     # A: 行は読み終えた・claim の直前で待機
                a_read.set()
                await _wait(b_done)
            return await orig_claim(op_id, now=now)

        async def _create_lost(*_a, **_k):
            raise RuntimeError("timeout after create")                              # 外部作成は実行・応答喪失

        async def _scenario():
            with patch.object(rf, "claim", _claim):
                a = asyncio.create_task(rf.process_one(OP, **_args()))
                await _wait(a_read)
                self.create.side_effect = _create_lost
                res_b = await rf.process_one(OP, **_args())                         # B
                self.create.side_effect = None
                b_done.set()
                res_a = await a
            return res_a, res_b
        res_a, res_b = _run(_scenario())
        self.assertEqual((res_b, res_a), ("unknown", "unknown"))                    # A は作成しない
        self.create.assert_awaited_once()                                           # 外部作成 1 回（B）
        row = self.followup()
        self.assertEqual((row["state"], row["create_state"], row["last_result"]), ("pending", "unknown", "unknown"))
        self.assertEqual(len(_FakeClient.calls), 1)                                 # LINE 再送なし
        self.assertEqual(calls["n"], 2)


# ── BR-17: 確定状態と診断理由の分離 ───────────────────────────────────────────────
class TestCreateStateColumn(_Fix4Mixin):
    def test_destination_failure_does_not_erase_unknown(self):
        """応答喪失（unknown）→ 宛先取得が一時失敗 → 復旧 → 照合 absent の順序で、次回も unknown 扱いで再作成されない。"""
        self._unknown()
        self.create.assert_awaited_once()
        self.get_record.side_effect = RuntimeError("App 29 down")
        self.assertEqual(_run(rf.process_one(OP, **_args())), "destination_mismatch")
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["create_state"]),
                         ("pending", "destination_mismatch", "unknown"))             # 診断理由だけ更新・確定状態は不変
        self.get_record.side_effect = None
        _run(sl.run_recover_job())                                                  # 復旧・照合 absent
        self.create.assert_awaited_once()                                           # 再作成しない
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["create_state"]), ("pending", "unknown", "unknown"))
        self.search.return_value = [_hit("910", rf.with_op_marker(TEXT, OP))]
        _run(sl.run_recover_job())
        row = self.followup()
        self.assertEqual((row["state"], row["create_state"], row["chatlog_record_id"]), ("done", "created", "910"))

    def test_transitions_are_monotonic(self):
        self._pending()
        token = _run(rf.claim(OP))["owner_token"]
        self.assertEqual(self.followup()["create_state"], "none")

        async def _set(to):
            async with db.session_scope() as s:
                await s.execute(sa.update(rf.reply_chatlog_followup).where(rf.reply_chatlog_followup.c.op_id == OP)
                                .values(create_state=rf._create_state_to(to)))

        def cs():
            return self.followup()["create_state"]
        _run(_set("unknown"))
        self.assertEqual(cs(), "none")                                              # none→unknown は不可
        _run(_set("created"))
        self.assertEqual(cs(), "none")                                              # none→created は不可
        self.assertTrue(_run(rf.confirm_owner_before_create(OP, token)))
        self.assertEqual(cs(), "creating")
        _run(_set("unknown"))
        self.assertEqual(cs(), "unknown")                                           # creating→unknown
        _run(_set("creating"))
        self.assertEqual(cs(), "unknown")                                           # 逆行しない
        self.assertFalse(_run(rf.confirm_owner_before_create(OP, token)))          # unknown からは作成に進ませない
        _run(_set("created"))
        self.assertEqual(cs(), "created")                                           # unknown→created
        for to in ("creating", "unknown"):
            _run(_set(to))
            self.assertEqual(cs(), "created")                                       # 上書きしない
        self.assertEqual(rf.CREATE_STATES, ("none", "creating", "unknown", "created"))
        self.assertEqual(rf.CREATE_TRANSITIONS, {"creating": ("none",), "unknown": ("creating",),
                                                 "created": ("creating", "unknown")})

    def test_written_and_recovered_set_created_and_prepare_rows_start_none(self):
        self.post()
        row = self.followup()
        self.assertEqual((row["state"], row["create_state"], row["last_result"]), ("done", "created", "written"))
        kw = dict(business="jikou", approval_record_id=REC_ID, line_user_hash=sl.line_user_hash("jikou", USER),
                  text=TEXT, text_version=sl.text_version(TEXT))
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        self.assertEqual(self.followup(OP3)["create_state"], "none")
        self.assertEqual(_run(rf.mark_pending("b" * 32, last_result="failed", business="jikou", approval_record_id=REC_ID,
                                              line_user_hash=sl.line_user_hash("jikou", USER), text=TEXT)), "pending")
        self.assertEqual(self.followup("b" * 32)["create_state"], "none")
        # 照合で既存が見つかった行: creating／unknown からは created、none からは none のまま（単調遷移の規則どおり・終端は done）
        self._pending("c" * 32)
        self.search.return_value = [_hit("911", rf.with_op_marker(TEXT, "c" * 32))]
        _run(sl.run_recover_job())
        row = self.followup("c" * 32)
        self.assertEqual((row["state"], row["last_result"], row["create_state"]), ("done", "recovered", "none"))


class TestFix5Migration(unittest.TestCase):
    def test_a7c9_follows_f7b2_migrates_rows_and_refuses_downgrade_with_state(self):
        d = tempfile.mkdtemp(prefix="q1b_fix5_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, _u, nullable = TestAlembicScaffold._brain_mig_tools(dbfile)
        try:
            self.assertEqual(alembic("upgrade", F7B2).returncode, 0)
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute('PRAGMA table_info("reply_chatlog_followup")')}
                self.assertNotIn("create_state", cols)
                for i, lr in enumerate(("unknown", "creating", "written", "recovered", "failed", None)):
                    con.execute("INSERT INTO reply_chatlog_followup (op_id, business, kind, approval_record_id, line_user_hash, "
                                "text, state, attempts, last_result, created_at, updated_at) VALUES (?, 'jikou', 'chatlog_human', "
                                "'1', 'h', 't', 'pending', 0, ?, '2026-10-21 00:00:00', '2026-10-21 00:00:00')", (str(i), lr))
                con.commit()
            finally:
                con.close()
            up = alembic("upgrade", A7C9)                                           # 後続票（Q1c 以降）の head に依存しない
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(A7C9, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                got = dict(con.execute("SELECT op_id, create_state FROM reply_chatlog_followup").fetchall())
                self.assertEqual(got, {"0": "unknown", "1": "creating", "2": "created", "3": "created", "4": "none", "5": "none"})
                self.assertFalse(nullable(con, "reply_chatlog_followup")["create_state"])
            finally:
                con.close()
            down = alembic("downgrade", F7B2)                                       # none 以外がある → 拒否
            self.assertNotEqual(down.returncode, 0)
            self.assertIn("reply_followup_create_state_downgrade_refused_data_present", down.stderr)
            self.assertIn(A7C9, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                con.execute("UPDATE reply_chatlog_followup SET create_state = 'none'")
                con.commit()
            finally:
                con.close()
            self.assertEqual(alembic("downgrade", F7B2).returncode, 0)             # 情報が失われないなら戻せる
            self.assertIn(F7B2, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute('PRAGMA table_info("reply_chatlog_followup")')}
                self.assertNotIn("create_state", cols)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM reply_chatlog_followup").fetchone()[0], 6)   # 行は残る
            finally:
                con.close()
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_migration_source_pins(self):
        src = (REPO / "alembic" / "versions" / "20261021_a7c9e1f3b5d7_reply_followup_create_state.py").read_text(encoding="utf-8")
        self.assertIn("revision: str = 'a7c9e1f3b5d7'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'f7b2c9d4e1a6'", src)
        self.assertIn("reply_followup_create_state_downgrade_refused_data_present", src)
        doc = (REPO / "docs" / "jikou_reply_q1b_contract.md").read_text(encoding="utf-8")
        for needle in ("a7c9e1f3b5d7", "create_state", "単調"):
            self.assertIn(needle, doc, needle)


if __name__ == "__main__":
    unittest.main()
