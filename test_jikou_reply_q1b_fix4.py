"""JIKOU-REPLY-Q1b-fix4（Codex R-Q1b-fix2 BR-13〜15）の回帰テスト。

共通フィクスチャ（sqlite DB・LINE／kintone のフェイク・定数）は test_jikou_reply_q1b を、回収行の作り方は test_jikou_reply_q1b_fix2 を再利用。
- BR-13: 照合は 4 値（found／absent／unavailable／unconfigured）。取得不能では作成せず pending に残して次回へ／成功応答喪失→次回照合が
         例外→作成されない／like は `op_{op_id}` の 1 単語（記号・% なし）＋本文末尾の等価照合／業務 LINE 通知は 1 日 1 回（台帳 key）
- BR-14: heartbeat で所有期限を延長（所有者のみ）／外部作成の直前に owner を同一 tx で再確認／A 検索後停止→期限切れ→B 作成→A 再開 で
         作成 1 回／作成呼出後の応答喪失は unknown として残し、照合で見つかるまで再作成しない／creating のまま期限切れも同じ
- BR-15: 台帳 begin 失敗→prepared のまま（本文保持）→再 POST（同一 op_id・同一本文）で送信 1 回・done／本文 B の POST は reject・送信ゼロ／
         終端・送信済み・宛先不一致・台帳行ありの prepare は rejected
"""
import asyncio
import datetime
import unittest
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from hub import notify as hub_notify  # noqa: E402
from test_jikou_reply_q1b import (  # noqa: E402
    OP, OP2, PAGE, REC_ID, TEXT, USER, _FakeClient, _auth, _client, _form, _run, db, rf, rv, sl)
from test_jikou_reply_q1b_fix2 import OP3, _FollowupMixin, _args  # noqa: E402

ADMIN = "U" + "d" * 32
TEXT_B = "別の本文です。手続きを進めます。"


def _hit(rid: str, message: str) -> dict:
    return {"$id": {"value": rid}, "message": {"value": message}}


class _Fix4Mixin(_FollowupMixin):
    def _set_last_result(self, value, op=OP, state=None):
        async def _q():
            async with db.session_scope() as s:
                vals = {"last_result": value}
                if state:
                    vals["state"] = state
                await s.execute(sa.update(rf.reply_chatlog_followup).where(rf.reply_chatlog_followup.c.op_id == op).values(**vals))
        _run(_q())

    def _unknown(self, op=OP):
        """作成呼出後に応答喪失（create が例外）→ 回収行 pending／unknown。"""
        self.create.side_effect = RuntimeError("timeout after create")
        r = self.post(_form(op=op))
        self.create.side_effect = None
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=unknown&ex=saved")
        row = self.followup(op)
        self.assertEqual((row["state"], row["last_result"], row["create_state"], row["text"]),
                         ("pending", "unknown", "unknown", TEXT))
        return row


# ── BR-13: 取得不能は不存在ではない ──────────────────────────────────────────────
class TestLookupFourValues(_Fix4Mixin):
    def test_lookup_unavailable_does_not_create_and_is_retried_next_time(self):
        attempts = self._pending()["attempts"]
        self.search.side_effect = RuntimeError("kintone search down")
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()                                            # 取得不能では作成しない
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["attempts"], row["text"]),
                         ("pending", "lookup_unavailable", attempts, TEXT))         # 試行回数に数えず次回へ
        self.assertEqual(_run(rf.process_one(OP, **_args())), "lookup_unavailable")
        self.search.side_effect = None
        _run(sl.run_recover_job())                                                  # 次回: 照合 absent → 作成 1 回
        self.create.assert_awaited_once()
        self.assertEqual(self.followup()["state"], "done")

    def test_lost_success_then_lookup_exception_does_not_create(self):
        """成功応答喪失（create 呼出後に例外）→ 次回の照合が例外 → 作成されない。照合が通れば既存で done（作成は全体で 1 回）。"""
        self._unknown()
        self.create.assert_awaited_once()
        self.search.side_effect = RuntimeError("down")
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()                                           # 増えない
        row = self.followup()                                                       # fix5 BR-17: 診断理由は更新・確定状態は unknown のまま
        self.assertEqual((row["state"], row["last_result"], row["create_state"]), ("pending", "lookup_unavailable", "unknown"))
        self.search.side_effect = None
        self.search.return_value = [_hit("901", rf.with_op_marker(TEXT, OP))]
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["chatlog_record_id"]), ("done", "recovered", "901"))

    def test_main_path_lookup_unavailable_does_not_create(self):
        self.search.side_effect = RuntimeError("down")
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=lookup_unavailable&ex=saved")
        self.create.assert_not_awaited()
        self.assertEqual((self.followup()["state"], self.followup()["last_result"]), ("pending", "lookup_unavailable"))
        self.assertEqual(len(_FakeClient.calls), 1)
        page = _client.get(f"{PAGE}?done=sent&log=lookup_unavailable", headers=_auth())
        self.assertIn("既存記録の照合ができなかったため", page.text)

    def test_marker_query_is_one_word_and_equivalence_is_checked_after_like(self):
        """kintone の like は単語検索（`[ ] : %` はキーワードから除かれ `_` は単語の一部）→ `op_{hex}` の 1 単語で検索し、
        後段で本文末尾の等価照合。途中に含むだけの行・別 op のマーカー行は found にしない。"""
        self.assertEqual(rf.op_marker(OP), f"op_{OP}")
        self.assertTrue(rf.op_marker(OP).replace("_", "").isalnum())
        self.search.return_value = [_hit("1", rf.with_op_marker("x", OP2)),       # 別 op
                                    _hit("2", f"op_{OP} の途中にある"),            # 末尾ではない
                                    _hit("3", rf.with_op_marker("y", OP) + "\n")]  # 末尾（改行つき）
        self.assertEqual(_run(rv.find_human_chatlog(USER, OP)), ("found", "3"))
        q = self.search.await_args.args[1]
        self.assertIn(f'message like "op_{OP}"', q)
        self.assertNotIn("%", q)
        self.assertIn(f"limit {rv.LOOKUP_LIMIT}", q)
        self.assertEqual(self.search.await_args.kwargs["fields"], ["$id", "message"])
        self.search.return_value = [_hit("1", rf.with_op_marker("x", OP2)), _hit("2", f"op_{OP} の途中にある")]
        self.assertEqual(_run(rv.find_human_chatlog(USER, OP)), ("absent", None))
        self.assertTrue(rf.marker_matches(rf.with_op_marker(TEXT, OP), OP))
        self.assertFalse(rf.marker_matches(rf.with_op_marker(TEXT, OP2), OP))
        self.assertFalse(rf.marker_matches("", OP))

    def test_hold_notice_is_sent_once_per_day_via_ledger_key(self):
        self._pending()
        self._pending(OP2)
        self.search.side_effect = RuntimeError("down")
        notify = AsyncMock(return_value=True)
        with patch.object(hub_notify, "get_admin_line_user_id", lambda: ADMIN), \
                patch.object(hub_notify, "notify_admin_line", notify):
            _run(sl.run_recover_job())
            _run(sl.run_recover_job())                                              # 同日 2 回目: 台帳で重複 → 送らない
        notify.assert_awaited_once()
        text = notify.await_args.args[0]
        self.assertIn("件数: 2", text)
        for secret in (OP, OP2, USER, TEXT, REC_ID):
            self.assertNotIn(secret, text)                                          # 件数のみ（op_id・本文・宛先なし）
        ops = [o for o in self.ops() if o["channel"] == rf.LEDGER_CHANNEL]
        self.assertEqual([(o["state"], o["purpose"], o["inbound_event_id"]) for o in ops],
                         [("sent", "other", rf._day_key())])
        self.create.assert_not_awaited()

    def test_hold_notice_skipped_without_admin_id_and_not_sent_when_nothing_held(self):
        self._pending()
        with patch.object(hub_notify, "get_admin_line_user_id", lambda: ""), \
                patch.object(hub_notify, "notify_admin_line", AsyncMock(return_value=True)) as notify:
            self.search.side_effect = RuntimeError("down")
            _run(sl.run_recover_job())
            notify.assert_not_awaited()
            self.assertFalse(_run(rf.notify_holds_once(1)))
        self.search.side_effect = None
        with patch.object(hub_notify, "get_admin_line_user_id", lambda: ADMIN), \
                patch.object(hub_notify, "notify_admin_line", AsyncMock(return_value=True)) as notify:
            _run(sl.run_recover_job())                                              # 保留なし → 通知なし
            notify.assert_not_awaited()
        self.assertEqual(self.followup()["state"], "done")


# ── BR-14: 所有期限切れ後の旧処理 ────────────────────────────────────────────────
class TestOwnershipKeepAlive(_Fix4Mixin):
    def test_heartbeat_extends_deadline_only_for_owner(self):
        self._pending()
        token = _run(rf.claim(OP))["owner_token"]
        before = rf._aware(self.followup()["deadline_at"])
        later = rf._now() + datetime.timedelta(seconds=120)
        self.assertTrue(_run(rf.heartbeat(OP, token, now=later)))
        after = rf._aware(self.followup()["deadline_at"])
        self.assertEqual(after, later + datetime.timedelta(seconds=rf.CLAIM_SECONDS))
        self.assertGreater(after, before)
        self.assertFalse(_run(rf.heartbeat(OP, "not-the-owner", now=later)))
        self.assertEqual(rf._aware(self.followup()["deadline_at"]), after)
        self.assertFalse(_run(rf.confirm_owner_before_create(OP, "not-the-owner")))
        self.assertEqual((self.followup()["last_result"], self.followup()["create_state"]), ("failed", "none"))   # 触らない
        self.assertTrue(_run(rf.confirm_owner_before_create(OP, token)))
        # fix5 BR-17: 確定状態 none→creating・診断理由（last_result）は触らない
        self.assertEqual((self.followup()["last_result"], self.followup()["create_state"]), ("failed", "creating"))
        self.assertFalse(_run(rf.confirm_owner_before_create(OP, token)))          # creating からは進ませない（単調）

    def test_heartbeat_task_keeps_extending_during_slow_create(self):
        self._pending()
        beats = []
        orig = rf.heartbeat

        async def _spy(op_id, token, **kw):
            beats.append(op_id)
            return await orig(op_id, token, **kw)

        async def _slow(*_a, **_k):
            await asyncio.sleep(0.25)
            return "900"
        self.create.side_effect = _slow
        with patch.object(rf, "HEARTBEAT_SECONDS", 0.02), patch.object(rf, "heartbeat", _spy):
            _run(sl.run_recover_job())
        self.create.side_effect = None
        self.assertGreaterEqual(len(beats), 3)
        self.assertEqual(set(beats), {OP})
        self.assertEqual(self.followup()["state"], "done")

    def test_owner_recheck_before_create_prevents_duplicate_after_expiry(self):
        """A が検索後に停止 → 所有期限切れ → B が作成 → A 再開: 外部作成は 1 回（A は作成直前の owner 再確認で止まる）。
        時間は now= の monkeypatch（B は期限切れ後の時刻で claim）。"""
        self._pending()
        later = rf._now() + datetime.timedelta(seconds=rf.CLAIM_SECONDS + 1)
        seen = {}

        async def _find(user_id, op_id):
            if "b" not in seen:
                seen["b"] = None
                # A は検索を終えたところで停止したとみなし、その間に B（期限切れ後）が処理を完了する
                seen["b"] = await rf.process_one(OP, **dict(_args(), find_chatlog=_find), now=later)
            return ("absent", None)
        res_a = _run(rf.process_one(OP, **dict(_args(), find_chatlog=_find)))
        self.assertEqual(seen["b"], "written")
        self.assertEqual(res_a, "not_pending")                                      # A は作成しない
        self.create.assert_awaited_once()
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["chatlog_record_id"]), ("done", "written", None))
        self.assertEqual(len(_FakeClient.calls), 1)                                 # LINE 再送なし

    def test_unknown_result_is_kept_and_not_recreated_until_found(self):
        self._unknown()
        self.create.assert_awaited_once()
        self.assertEqual(_run(rf.process_one(OP, **_args())), "unknown")           # 照合 absent でも作成しない
        self.create.assert_awaited_once()
        self._set_deadline_past()
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()                                           # 期限切れだけを根拠に再作成しない
        self.assertEqual((self.followup()["state"], self.followup()["last_result"]), ("pending", "unknown"))
        self.search.return_value = [_hit("902", rf.with_op_marker(TEXT, OP))]
        _run(sl.run_recover_job())
        self.assertEqual((self.followup()["state"], self.followup()["chatlog_record_id"]), ("done", "902"))
        self.create.assert_awaited_once()
        page = _client.get(f"{PAGE}?done=sent&log=unknown", headers=_auth())
        self.assertIn("記録結果が不明", page.text)

    def test_creating_mark_left_by_crash_is_treated_as_unknown(self):
        """owner 再確認（creating）の直後にプロセス停止 → 期限切れ → 作成されたか不明として扱い再作成しない。"""
        self._pending()
        token = _run(rf.claim(OP))["owner_token"]
        self.assertTrue(_run(rf.confirm_owner_before_create(OP, token)))
        _run(sl.run_recover_job())                                                  # 期限内: 触らない
        self.create.assert_not_awaited()
        self._set_deadline_past()
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()
        self.assertEqual((self.followup()["state"], self.followup()["last_result"]), ("pending", "unknown"))
        self.search.return_value = [_hit("903", rf.with_op_marker(TEXT, OP))]
        _run(sl.run_recover_job())
        self.assertEqual(self.followup()["state"], "done")

    def test_write_human_chatlog_distinguishes_before_and_after_create(self):
        self.form_fields.side_effect = RuntimeError("down")
        self.assertEqual(_run(rv.write_human_chatlog(USER, "x")), "failed")         # 作成呼出前
        self.form_fields.side_effect = None
        self.create.side_effect = RuntimeError("timeout")
        self.assertEqual(_run(rv.write_human_chatlog(USER, "x")), "unknown")        # 作成呼出後

        async def _no():
            return False
        self.create.side_effect = None
        self.create.reset_mock()
        self.assertEqual(_run(rv.write_human_chatlog(USER, "x", _no)), "not_owner")
        self.create.assert_not_awaited()


# ── BR-15: 既存回収行の照合 ───────────────────────────────────────────────────────
class TestPrepareRevalidation(_Fix4Mixin):
    def _begin_fails(self, data=None):
        with patch.object(sl, "begin", AsyncMock(return_value=None)):
            r = self.post(data)
        self.assertEqual(r.headers["location"], f"{PAGE}?done=ledger_unavailable")
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["text"], row["text_version"]),
                         ("prepared", "ledger_unavailable", TEXT, sl.text_version(TEXT)))   # 閉じない・本文保持
        self.assertEqual(_FakeClient.calls, [])
        return row

    def test_begin_failure_keeps_prepared_and_repost_resumes_once(self):
        self._begin_fails()
        r = self.post()                                                             # 同一 op_id・同一本文で再 POST
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=saved")
        self.assertEqual(len(_FakeClient.calls), 1)                                 # 送信 1 回
        self.create.assert_awaited_once()
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["text"]), ("done", "written", None))
        self.assertEqual(self.ops()[0]["state"], "sent")
        r = self.post()                                                             # 3 回目: 台帳 sent → 記録のみ再試行（送らない）
        self.assertEqual(r.headers["location"], f"{PAGE}?done=duplicate_sent&log=not_pending")
        self.assertEqual(len(_FakeClient.calls), 1)

    def test_repost_with_different_text_on_prepared_is_rejected(self):
        self._begin_fails()
        with self.assertLogs("hub.reply_followup", level="INFO") as cm:
            r = self.post(_form(text=TEXT_B))
        self.assertEqual(r.headers["location"], f"{PAGE}?done=op_rejected")
        self.assertEqual(_FakeClient.calls, [])                                     # 送信ゼロ
        self.create.assert_not_awaited()
        row = self.followup()
        self.assertEqual((row["state"], row["text"]), ("prepared", TEXT))           # 本文 A のまま
        self.assertTrue(any("text mismatch" in m for m in cm.output))
        self.assertFalse(any(TEXT in m or TEXT_B in m or USER in m for m in cm.output))
        page = _client.get(f"{PAGE}?done=op_rejected", headers=_auth())
        self.assertIn("新しい操作でやり直してください", page.text)
        self.assertIn("送信していません", page.text)

    def test_prepare_rejects_terminal_sent_mismatch_and_ledger_rows(self):
        kw = dict(business="jikou", approval_record_id=REC_ID, line_user_hash=sl.line_user_hash("jikou", USER),
                  text=TEXT, text_version=sl.text_version(TEXT))
        self.post()                                                                 # OP: done（終端）
        self.assertEqual(_run(rf.prepare(OP, **kw)), "rejected")
        self._pending(OP2)                                                          # OP2: pending（送信済み）
        self.assertEqual(_run(rf.prepare(OP2, **kw)), "rejected")
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        self.assertEqual(_run(rf.prepare(OP3, **dict(kw, line_user_hash="other"))), "rejected")   # 宛先不一致
        self.assertEqual(_run(rf.prepare(OP3, **dict(kw, business="houki"))), "rejected")
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "reprepared")

        async def _ledger_row():
            async with sl.human_reply(OP3):
                return await sl.begin("jikou", "jikou", USER, TEXT)
        self.assertIsInstance(_run(_ledger_row()), sl.Operation)
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "rejected")                   # 台帳に行あり → 未送信を確認できない
        self.assertEqual(self.followup(OP3)["state"], "prepared")
        # fix6: 台帳に行がある（sent）行は close できない → 終端は exhausted 相当の直接更新で作る
        self.assertFalse(_run(rf.close(OP3, "test", expected_version=self.followup(OP3)["row_version"])))

        async def _terminal():
            async with db.session_scope() as s:
                await s.execute(sa.update(rf.reply_chatlog_followup).where(rf.reply_chatlog_followup.c.op_id == OP3)
                                .values(state="closed"))
        _run(_terminal())
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "rejected")

    def test_reprepare_refreshes_grace_window_and_resets_counters(self):
        kw = dict(business="jikou", approval_record_id=REC_ID, line_user_hash=sl.line_user_hash("jikou", USER),
                  text=TEXT, text_version=sl.text_version(TEXT))
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")

        async def _age():
            async with db.session_scope() as s:
                await s.execute(sa.update(rf.reply_chatlog_followup).where(rf.reply_chatlog_followup.c.op_id == OP3).values(
                    created_at=rf._now() - datetime.timedelta(seconds=rf.LEDGER_GRACE_SECONDS - 5), last_result="x"))
        _run(_age())
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "reprepared")
        row = self.followup(OP3)
        self.assertIsNone(row["last_result"])
        self.assertLess(rf._now() - rf._aware(row["created_at"]), datetime.timedelta(seconds=5))
        _run(sl.run_recover_job())                                                  # 猶予が再び始まる＝閉じない
        self.assertEqual(self.followup(OP3)["state"], "prepared")

    def test_vocabulary_pins(self):
        self.assertEqual(rf.LOOKUP_RESULTS, ("found", "absent", "unavailable", "unconfigured"))
        self.assertEqual(rf.PREPARE_REJECT_REASONS,
                         ("terminal", "already_sent", "destination_mismatch", "text_mismatch", "ledger_exists"))
        self.assertEqual((rf.RESULT_LOOKUP_UNAVAILABLE, rf.RESULT_UNKNOWN, rf.WRITE_NOT_OWNER, rf.CREATE_CREATING),
                         ("lookup_unavailable", "unknown", "not_owner", "creating"))
        self.assertIn("op_rejected", rv._DONE_NOTICES)
        self.assertEqual({"lookup_unavailable", "unknown"} - set(rv._LOG_NOTICES), set())
        self.assertEqual(rv._MAIN_PATH_LOG[rf.RESULT_LOOKUP_UNAVAILABLE], "lookup_unavailable")
        self.assertEqual(rv._MAIN_PATH_LOG[rf.RESULT_UNKNOWN], "unknown")
        self.assertEqual(rf.STATES, ("prepared", "pending", "unconfigured", "in_progress", "done", "closed", "exhausted"))
        self.assertLess(rf.HEARTBEAT_SECONDS * 3, rf.CLAIM_SECONDS)
        for secret in (OP, USER, TEXT):
            self.assertNotIn(secret, rf.HOLD_NOTICE_TEXT)


if __name__ == "__main__":
    unittest.main()
