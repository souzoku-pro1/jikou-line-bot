"""JIKOU-REPLY-Q1b-fix6（Codex R-Q1b-fix5 BR-18）の回帰テスト。

- 世代番号 row_version（migration c8e0a2b4d6f8・既存行 0）: 内容を変える更新ごとに +1（再準備・claim・owner 再確認・release・close・
  mark_pending・note_prepared）。heartbeat は deadline だけ（世代は進めない）。
- close／complete／所有者の終了更新は「読んだ時点の世代と一致」のときだけ。close は同一 tx で送信台帳を再読取（行がある＝未送信ではない）。
- 順序固定（threading.Event）: A が猶予超過 prepared と台帳なしを読む → B が再準備し送信成功 → A が close → 更新 0 件・行は open・本文保持・
  次回回収で App 28 記録。ledger_failed 分岐も同じ順序。
"""
import asyncio
import datetime
import shutil
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import patch

import sqlalchemy as sa

from test_db_foundation import TestAlembicScaffold  # noqa: E402
from test_jikou_reply_q1b import OP, REC_ID, REPO, TEXT, USER, _FakeClient, _FakeResp, _run, db, rf, rv, sl  # noqa: E402
from test_jikou_reply_q1b_fix2 import OP3, _args  # noqa: E402
from test_jikou_reply_q1b_fix4 import _Fix4Mixin  # noqa: E402

A7C9 = "a7c9e1f3b5d7"
C8E0 = "c8e0a2b4d6f8"
KW = dict(business="jikou", approval_record_id=REC_ID, text=TEXT, text_version=sl.text_version(TEXT))


async def _wait(ev: threading.Event):
    await asyncio.get_running_loop().run_in_executor(None, ev.wait)


class _Fix6Mixin(_Fix4Mixin):
    def _kw(self):
        return dict(KW, line_user_hash=sl.line_user_hash("jikou", USER))

    def _age(self, op=OP3, seconds=rf.LEDGER_GRACE_SECONDS + 1):
        async def _q():
            async with db.session_scope() as s:
                await s.execute(sa.update(rf.reply_chatlog_followup).where(rf.reply_chatlog_followup.c.op_id == op).values(
                    created_at=rf._now() - datetime.timedelta(seconds=seconds)))
        _run(_q())

    async def _ledger(self, op, outcome):
        """台帳に op の行を作る（begin → finish(outcome)）。failed の行への再 begin は再試行（attempt_no+1）になる。"""
        async with sl.human_reply(op):
            o = await sl.begin("jikou", "jikou", USER, TEXT)
        assert isinstance(o, sl.Operation), o
        await sl.finish(o, outcome, text_version=sl.text_version(TEXT))

    def _race(self, reason, b_steps):
        """A（process_one）が row0 と台帳を読み close に入る直前で待機 → B（b_steps）→ A の close。A の戻り値を返す。"""
        a_read = threading.Event()
        b_done = threading.Event()
        orig_close = rf.close
        seen = {}

        async def _close(op_id, why, *, expected_version):
            seen["reason"], seen["version"] = why, expected_version
            a_read.set()
            await _wait(b_done)
            return await orig_close(op_id, why, expected_version=expected_version)

        async def _scenario():
            with patch.object(rf, "close", _close):
                a = asyncio.create_task(rf.process_one(OP3, **_args()))
                await _wait(a_read)
                await b_steps()
                b_done.set()
                return await a
        res = _run(_scenario())
        self.assertEqual(seen["reason"], reason)
        return res, seen["version"]


class TestRowVersion(_Fix6Mixin):
    def test_version_increments_on_content_updates_not_on_heartbeat(self):
        kw = self._kw()
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        self.assertEqual(self.followup(OP3)["row_version"], 0)
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "reprepared")
        self.assertEqual(self.followup(OP3)["row_version"], 1)
        self.assertTrue(_run(rf.note_prepared(OP3, "x")))
        self.assertEqual(self.followup(OP3)["row_version"], 2)
        row = _run(rf.claim(OP3))
        self.assertEqual((row["row_version"], self.followup(OP3)["row_version"]), (3, 3))
        token = row["owner_token"]
        self.assertTrue(_run(rf.heartbeat(OP3, token)))
        self.assertEqual(self.followup(OP3)["row_version"], 3)                     # heartbeat は世代を進めない
        self.assertTrue(_run(rf.confirm_owner_before_create(OP3, token)))
        self.assertEqual(self.followup(OP3)["row_version"], 4)
        self.assertFalse(_run(rf._release(OP3, token, "pending", expected_version=3, last_result="x")))   # 古い世代では更新しない
        self.assertEqual(self.followup(OP3)["state"], "in_progress")
        self.assertTrue(_run(rf._release(OP3, token, "pending", expected_version=4, last_result="x")))
        self.assertEqual((self.followup(OP3)["state"], self.followup(OP3)["row_version"]), ("pending", 5))

    def test_close_requires_matching_version_and_rereads_ledger(self):
        kw = self._kw()
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        self.assertFalse(_run(rf.close(OP3, "not_sent", expected_version=7)))      # 世代不一致 → 0 件
        self.assertEqual(self.followup(OP3)["state"], "prepared")
        _run(self._ledger(OP3, "sent"))
        self.assertFalse(_run(rf.close(OP3, "not_sent", expected_version=0)))      # 世代一致でも台帳に行がある → 閉じない
        self.assertFalse(_run(rf.close(OP3, "ledger_failed", expected_version=0)))  # failed ではない → 閉じない
        self.assertFalse(_run(rf.close(OP3, "line_failed", expected_version=0)))
        self.assertEqual((self.followup(OP3)["state"], self.followup(OP3)["text"]), ("prepared", TEXT))
        self.assertEqual(_run(rf.prepare("d" * 32, **kw)), "prepared")
        self.assertTrue(_run(rf.close("d" * 32, "not_sent", expected_version=0)))  # 台帳なし・世代一致 → 閉じる
        self.assertEqual((self.followup("d" * 32)["state"], self.followup("d" * 32)["row_version"]), ("closed", 1))

    def test_line_failure_close_uses_current_version(self):
        _FakeClient.responses = [_FakeResp(500, "x")]
        self.post()
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["row_version"]), ("closed", "line_failed", 1))

    def test_full_flow_versions(self):
        self.post()                                                                 # prepare(0) → claim(1) → confirm(2) → done(3)
        self.assertEqual((self.followup()["state"], self.followup()["row_version"]), ("done", 3))
        self._pending("b" * 32)                                                     # prepare(0) → claim(1) → release failed(2)
        self.assertEqual(self.followup("b" * 32)["row_version"], 2)
        _run(sl.run_recover_job())                                                  # claim(3) → confirm(4) → done(5)
        self.assertEqual((self.followup("b" * 32)["state"], self.followup("b" * 32)["row_version"]), ("done", 5))


class TestStaleCloseRace(_Fix6Mixin):
    def test_not_sent_close_after_reprepare_and_send_does_not_close(self):
        """A が猶予超過 prepared と台帳なしを読む → B が再準備し送信成功 → A の close は更新 0 件・行は open・本文保持 → 次回回収で記録。"""
        kw = self._kw()
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        self._age()

        async def b_steps():
            self.assertEqual(await rf.prepare(OP3, **kw), "reprepared")             # 世代 0 → 1
            await self._ledger(OP3, "sent")                                         # 送信成功
        res, ver = self._race("not_sent", b_steps)
        self.assertEqual((res, ver), ("waiting_ledger", 0))                         # A: 更新 0 件 → 次回へ
        row = self.followup(OP3)
        self.assertEqual((row["state"], row["text"], row["row_version"]), ("prepared", TEXT, 1))
        self.create.assert_not_awaited()
        _run(sl.run_recover_job())                                                  # 次回回収: 台帳 sent → 記録
        self.create.assert_awaited_once()
        self.assertEqual(self.create.await_args.args[1]["message"], rf.with_op_marker(TEXT, OP3))
        self.assertEqual((self.followup(OP3)["state"], self.followup(OP3)["create_state"]), ("done", "created"))

    def test_ledger_failed_close_after_retry_and_send_does_not_close(self):
        """ledger_failed 分岐: A が台帳 failed を読む → B が再準備（世代を進める）し台帳の再試行で送信成功 → A の close は 0 件 → 次回回収で記録。
        （画面からは failed の操作を再送しない＝duplicate_unconfirmed。再試行は台帳の attempt_no+1 経路を直接使って再現）"""
        kw = self._kw()
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        _run(self._ledger(OP3, "failed"))

        async def b_steps():
            self.assertTrue(await rf.note_prepared(OP3, "retry"))                  # 再準備＝世代 0 → 1（本文保持）
            await self._ledger(OP3, "sent")                                         # failed → 再試行 → sent
        res, ver = self._race("ledger_failed", b_steps)
        self.assertEqual((res, ver), ("waiting_ledger", 0))
        row = self.followup(OP3)
        self.assertEqual((row["state"], row["text"], row["row_version"]), ("prepared", TEXT, 1))
        self.assertEqual(_run(sl.operation_state(OP3)), "sent")
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()
        self.assertEqual(self.followup(OP3)["state"], "done")

    def test_without_race_stale_rows_still_close(self):
        kw = self._kw()
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        self._age()
        self.assertEqual(_run(rf.process_one(OP3, **_args())), "closed")
        self.assertEqual((self.followup(OP3)["state"], self.followup(OP3)["last_result"]), ("closed", "not_sent"))
        self.assertEqual(_run(rf.prepare("e" * 32, **kw)), "prepared")
        _run(self._ledger("e" * 32, "failed"))
        self.assertEqual(_run(rf.process_one("e" * 32, **_args())), "closed")
        self.assertEqual(self.followup("e" * 32)["last_result"], "ledger_failed")


class TestFix6Migration(unittest.TestCase):
    def test_c8e0_follows_a7c9_and_round_trips(self):
        d = tempfile.mkdtemp(prefix="q1b_fix6_mig_")
        dbfile = f"{d}/mig.db"
        alembic, tables, _u, nullable = TestAlembicScaffold._brain_mig_tools(dbfile)
        try:
            self.assertEqual(alembic("upgrade", A7C9).returncode, 0)
            con = sqlite3.connect(dbfile)
            try:
                con.execute("INSERT INTO reply_chatlog_followup (op_id, business, kind, approval_record_id, line_user_hash, "
                            "text, state, attempts, created_at, updated_at) VALUES ('a', 'jikou', 'chatlog_human', '1', 'h', 't', "
                            "'pending', 0, '2026-10-21 00:00:00', '2026-10-21 00:00:00')")
                con.commit()
            finally:
                con.close()
            up = alembic("upgrade", C8E0)
            self.assertEqual(up.returncode, 0, f"stderr={up.stderr[-800:]}")
            self.assertIn(C8E0, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                self.assertEqual(con.execute("SELECT row_version FROM reply_chatlog_followup").fetchall(), [(0,)])   # 既存行は 0
                self.assertFalse(nullable(con, "reply_chatlog_followup")["row_version"])
            finally:
                con.close()
            self.assertEqual(alembic("downgrade", A7C9).returncode, 0)
            self.assertIn(A7C9, alembic("current").stdout)
            con = sqlite3.connect(dbfile)
            try:
                cols = {r[1] for r in con.execute('PRAGMA table_info("reply_chatlog_followup")')}
                self.assertNotIn("row_version", cols)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM reply_chatlog_followup").fetchone()[0], 1)
            finally:
                con.close()
        finally:
            shutil.rmtree(d, ignore_errors=True)

    def test_source_pins(self):
        src = (REPO / "alembic" / "versions" / "20261021_c8e0a2b4d6f8_reply_followup_row_version.py").read_text(encoding="utf-8")
        self.assertIn("revision: str = 'c8e0a2b4d6f8'", src)
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'a7c9e1f3b5d7'", src)
        doc = (REPO / "docs" / "jikou_reply_q1b_contract.md").read_text(encoding="utf-8")
        for needle in ("c8e0a2b4d6f8", "row_version", "再読取"):
            self.assertIn(needle, doc, needle)
        self.assertIn("row_version", {c.name for c in rf.reply_chatlog_followup.columns})


if __name__ == "__main__":
    unittest.main()
