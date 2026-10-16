"""JIKOU-REPLY-Q1b-fix2（Codex R-Q1b-fix1 BR-10〜12）の回帰テスト。

共通フィクスチャ（sqlite DB・LINE／kintone のフェイク・定数）は test_jikou_reply_q1b を再利用する。
- BR-10: 残留検査の追加（住所・続柄＋氏名・氏名候補・6 桁以上の数字列）／手本は pending_review で保存し approved だけを読む／
         Q1b 単体には approved への遷移経路（画面）が無い
- BR-11: 回収の所有権（条件付き UPDATE・owner_token・deadline_at）／App 28 本文マーカー [op:{op_id}] による照合／
         回収ジョブと再 POST の並行（スレッド 2〜10）で外部作成 1 回／done 更新失敗→再試行で作成が増えない／成功応答喪失→照合で done
- BR-12: 送信前 prepared／台帳と突合（sent→記録・unconfirmed→人の確定後・failed／台帳なし→closed）／mark_pending 失敗でも
         prepared が残る／unconfigured→設定後に記録
- docs（契約）・migration の pin
"""
import asyncio
import concurrent.futures
import datetime
import os
import threading
import unittest
from unittest.mock import AsyncMock, patch

import sqlalchemy as sa

from test_jikou_reply_q1b import (  # noqa: E402
    OP, OP2, PAGE, REC_ID, REPO, TEXT, USER, _DbMixin, _FakeClient, _FakeResp, _form, _run, cr, db, pii_scan,
    rf, rv, rx, sl)

OP3 = "a" * 32
SENT_BAD = ("家族の山田花子に連絡してください。", "住所は西新宿二丁目八番一号です。")
SENT_OK = (TEXT, "アコムから督促が来ました。電話は090-1234-5678です", "お返事ありがとうございます。手続きを進めます。",
           "費用は44,000円です。着手金をご確認ください。", "時効援用の通知書を内容証明で発送しました。")


def _args():
    """再 POST（記録のみ再試行）と回収ジョブが process_one に渡す口（画面と同じ）。"""
    return dict(resolve_user_id=rv._resolve_user_id, write_chatlog=rv.write_human_chatlog,
                find_chatlog=rv.find_human_chatlog, ledger_state=sl.operation_state)


class _FollowupMixin(_DbMixin):
    def _pending(self, op=OP):
        """送信は成功・App 28 記録は失敗 → 回収行 pending を作る（fix1 と同じ入口）。"""
        self.form_fields.side_effect = RuntimeError("kintone down")
        self.post(_form(op=op))
        self.form_fields.side_effect = None
        row = self.followup(op)
        self.assertEqual((row["state"], row["last_result"]), ("pending", "failed"))
        return row

    def followup(self, op=OP) -> dict:
        return next(r for r in self.followups() if r["op_id"] == op)

    def _set_deadline_past(self, op=OP):
        async def _q():
            async with db.session_scope() as s:
                await s.execute(sa.update(rf.reply_chatlog_followup).where(rf.reply_chatlog_followup.c.op_id == op)
                                .values(deadline_at=rf._now() - datetime.timedelta(seconds=1)))
        _run(_q())


# ── BR-10: 残留検査の追加 ──────────────────────────────────────────────────────
class TestResidueCheck(unittest.TestCase):
    def test_codex_sentences_are_rejected(self):
        for s in SENT_BAD:
            _, ok = pii_scan.anonymize(s)
            self.assertFalse(ok, s)

    def test_added_patterns_are_detected_on_residue(self):
        """伏せ字後に残っていれば検出（安全側・誤検知許容）。語彙は固定。"""
        cases = {
            "住所は西新宿二丁目八番一号です。": "address",
            "八番一号まで来てください": "address",
            "家族の山田花子に連絡してください。": "relation_name",
            "妻の花子が対応します": "relation_name",
            "山田太郎に伝えます": "name_candidate",
            "受付番号は 123456 です": "digits",
        }
        for text, label in cases.items():
            self.assertIn(label, pii_scan.residue_hits(text), text)
        self.assertEqual(tuple(n for n, _ in pii_scan.RESIDUE_RES),
                         ("email", "phone", "postal", "line_user_id", "digits", "address", "relation_name",
                          "name_candidate"))

    def test_existing_ok_examples_still_pass(self):
        for s in SENT_OK:
            _, ok = pii_scan.anonymize(s)
            self.assertTrue(ok, s)
        # 一般語（停止語・語尾の漢字）は氏名候補にしない
        for s in ("連絡を差し上げます", "時効援用の手続きを進めます", "債権者に通知します", "裁判所から書類が届きます",
                  "事務所に来所をお願いします", "業者が請求を止めます"):
            self.assertNotIn("name_candidate", pii_scan.residue_hits(s), s)

    def test_heuristic_labels_can_be_excluded_for_frozen_texts(self):
        """凍結定型（閉集合・承認済み）の送信前ガードは確定パターンだけ（発見的な氏名検査は自由文の匿名化にだけ使う）。"""
        self.assertEqual(pii_scan.RESIDUE_HEURISTIC_LABELS, ("relation_name", "name_candidate"))
        for s in ("過去の事例をご案内します", "万一時効が完成していなかった場合", "お気軽にご連絡ください"):
            self.assertEqual(pii_scan.residue_hits(s, heuristics=False), [], s)
            self.assertEqual(pii_scan.residue_hits(s), [], s)                      # 停止語でも通る
        self.assertEqual(pii_scan.residue_hits("山田太郎に伝えます", heuristics=False), [])
        self.assertEqual(pii_scan.residue_hits("山田太郎に伝えます"), ["name_candidate"])
        self.assertEqual(pii_scan.residue_hits("電話は09012345678です", heuristics=False), ["phone", "digits"])
        self.assertEqual(pii_scan.residue_hits("住所は西新宿二丁目八番一号です。", heuristics=False), ["address"])

    def test_no_new_redaction_masks(self):
        """fix2 は検出だけを広げる（伏せ字の語彙は不変・凍結）。"""
        self.assertEqual(pii_scan.MASKS, ("〔氏名〕", "〔住所〕", "〔電話〕", "〔メール〕", "〔口座〕",
                                          "〔郵便番号〕", "〔数字列〕", "〔LINE ID〕"))


class TestExemplarPendingReview(_FollowupMixin):
    def test_codex_sentences_are_not_saved_even_as_pending(self):
        for i, s in enumerate(SENT_BAD):
            op = (OP, OP2)[i]
            r = self.post(_form(op=op, text=s))
            self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=written&ex=rejected")
        self.assertEqual(self.exemplars(), [])
        self.assertEqual(_run(rx.list_exemplars("jikou")), [])

    def test_saved_exemplar_is_pending_until_explicit_human_approval(self):
        self.post()
        row = self.exemplars()[0]
        self.assertEqual((row["status"], row["approved_by"], row["approved_at"], bool(row["active"])),
                         ("pending_review", None, None, True))
        self.assertEqual(_run(rx.list_exemplars("jikou")), [])                     # 下書き参照に使わない
        self.assertEqual(_run(rx.list_exemplars("jikou", stage="受任後")), [])
        self.assertFalse(_run(rx.mark_used(row["id"])))                             # 使えない
        self.assertTrue(_run(rx.approve_exemplar(row["id"], actor="human")))
        row = self.exemplars()[0]
        self.assertEqual((row["status"], row["approved_by"]), ("approved", "human"))
        self.assertIsNotNone(row["approved_at"])
        self.assertEqual(len(_run(rx.list_exemplars("jikou"))), 1)
        self.assertEqual([h["action"] for h in _run(rx.exemplar_history(row["id"]))], ["created", "approved"])

    def test_save_with_active_false_is_still_pending_review(self):
        """後続票（Q4）が渡す active=False も状態は pending_review（互換引数・凍結語彙）。"""
        res = _run(rx.save_exemplar(business="jikou", inbound_text="督促が来ました", human_reply=TEXT,
                                    stage="相談", source_op_id=OP3, active=False))
        self.assertEqual(res, "saved")
        self.assertEqual(self.exemplars()[0]["status"], "pending_review")

    def test_q1b_has_no_approval_route(self):
        src = (REPO / "hub" / "webapp_reply_view.py").read_text(encoding="utf-8")
        self.assertNotIn("approve_exemplar(", src)
        self.assertEqual(sorted((r.path, tuple(sorted(r.methods))) for r in rv.router.routes),
                         [(f"{rv.PAGE}/{{record_id}}", ("GET",)), (f"{rv.PAGE}/{{record_id}}/send", ("POST",))])


# ── BR-11: 所有権・マーカー照合・並行 ──────────────────────────────────────────
class TestOwnership(_FollowupMixin):
    def test_claim_is_exclusive_and_expires(self):
        self._pending()
        t1 = _run(rf.claim(OP))
        self.assertTrue(t1)
        self.assertIsNone(_run(rf.claim(OP)))                                       # 取れなければ何もしない
        row = self.followup()
        self.assertEqual((row["state"], row["owner_token"]), ("in_progress", t1))
        self.assertIsNotNone(row["deadline_at"])
        self.assertEqual(_run(rf.open_ops()), [])                                   # 期限内は回収対象外
        self.assertEqual(_run(rf.process_one(OP, **_args())), "not_pending")
        self.create.assert_not_awaited()
        later = rf._now() + datetime.timedelta(seconds=rf.CLAIM_SECONDS + 1)
        self.assertEqual(_run(rf.open_ops(now=later)), [OP])                        # deadline 経過は recover 対象
        t3 = _run(rf.claim(OP, now=later))
        self.assertTrue(t3 and t3 != t1)

    def test_marker_lookup_before_create_recovers_lost_success(self):
        """成功応答喪失: App 28 には既に行がある（マーカーで照合）→ 作成せず done。"""
        self._pending()
        self.search.return_value = [{"$id": {"value": "777"}}]
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["chatlog_record_id"], row["text"]),
                         ("done", "recovered", "777", None))
        q = self.search.await_args.args[1]
        self.assertIn(f'line_user_id = "{USER}"', q)
        self.assertIn(f"[op:{OP}]", q)
        self.assertIn('role in ("human")', q)

    def test_done_update_failure_then_retry_does_not_create_again(self):
        self._pending()
        with patch.object(rf, "_finish_done", AsyncMock(side_effect=RuntimeError("db blip"))), \
                self.assertLogs("hub.reply_followup", level="WARNING"):
            _run(sl.run_recover_job())
        self.create.assert_awaited_once()
        self.assertEqual(self.followup()["state"], "in_progress")                   # 所有したまま（期限内）
        _run(sl.run_recover_job())                                                  # 期限内: 触らない
        self.create.assert_awaited_once()
        self._set_deadline_past()
        self.search.return_value = [{"$id": {"value": "778"}}]                      # 作成済みの行が照合で見つかる
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()                                           # 作成は増えない
        self.assertEqual((self.followup()["state"], self.followup()["chatlog_record_id"]), ("done", "778"))

    def test_parallel_recover_and_repost_create_once(self):
        """回収ジョブと再 POST（記録のみ再試行）をスレッド 2〜10 で並行 → 外部作成は 1 回。"""
        for n, op in ((2, OP), (10, OP2)):
            self.create.reset_mock()
            self._pending(op)

            async def _slow(*_a, **_k):
                await asyncio.sleep(0.3)                                            # 所有中に他スレッドが来る
                return "900"
            self.create.side_effect = _slow
            loop = asyncio.new_event_loop()
            th = threading.Thread(target=loop.run_forever, daemon=True)
            th.start()

            def _job(i):
                coro = rv.run_chatlog_followup() if i % 2 == 0 else rf.process_one(op, **_args())
                return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout=60)
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
                    results = [f.result() for f in [ex.submit(_job, i) for i in range(n)]]
                asyncio.run_coroutine_threadsafe(db.get_async_engine().dispose(), loop).result(timeout=30)
            finally:
                loop.call_soon_threadsafe(loop.stop)
                th.join(10)
            self.create.side_effect = None
            flat = []
            for r in results:
                flat.extend(r.keys() if isinstance(r, dict) else [r])
            self.assertEqual(self.create.await_count, 1, (n, results))
            self.assertEqual(flat.count("written"), 1, (n, results))
            self.assertTrue(set(flat) <= {"written", "not_pending"}, (n, results))
            self.assertEqual(self.followup(op)["state"], "done")
        self.assertEqual(len(_FakeClient.calls), 2)                                 # LINE 再送なし

    def test_marker_is_stripped_for_display_and_model_input(self):
        marked = rf.with_op_marker("x", OP)
        self.assertEqual(marked, f"x\n[op:{OP}]")
        self.assertEqual(rf.strip_op_marker(marked), "x")
        self.assertEqual(rf.strip_op_marker("x"), "x")
        self.assertEqual(rf.strip_op_marker(f"[op:{OP}] の途中"), f"[op:{OP}] の途中")   # 末尾だけ
        self.assertEqual(cr.model_content("human", marked), f"{cr.HUMAN_MARKER} x")
        self.assertEqual(cr.model_content("human", f"[op:{OP}]"), "")
        self.assertEqual(cr.model_content("user", marked), marked)                  # user 行は触らない
        self.search.return_value = [{"role": {"value": "human"}, "message": {"value": marked},
                                     "作成日時": {"value": "2026-10-17T00:00:00Z"}, "category": {"value": ""}}]
        rows = _run(rv._history_rows(USER))
        self.assertEqual([r["message"] for r in rows], ["x"])

    def test_find_human_chatlog_is_bound_and_fail_open_to_create(self):
        self.assertIsNone(_run(rv.find_human_chatlog(USER, "zz")))                  # 不正 op_id: 検索しない
        self.search.assert_not_awaited()
        self.assertIsNone(_run(rv.find_human_chatlog(USER, OP)))                     # 見つからない
        self.search.return_value = [{"$id": {"value": "5"}}]
        self.assertEqual(_run(rv.find_human_chatlog(USER, OP)), "5")
        self.search.side_effect = RuntimeError("down")
        self.assertIsNone(_run(rv.find_human_chatlog(USER, OP)))                     # 読取失敗は None（作成へ）
        with patch.dict(os.environ, {"APP_CHATLOG": ""}):
            self.assertIsNone(_run(rv.find_human_chatlog(USER, OP)))


# ── BR-12: 送信前 prepared・台帳との突合 ──────────────────────────────────────
class TestPreparedAndLedger(_FollowupMixin):
    def test_prepared_is_persisted_before_push(self):
        seen = {}

        class _Peek(_FakeClient):
            async def post(self, url, headers=None, json=None):
                seen["row"] = await rf.get_followup(OP)
                return await super().post(url, headers=headers, json=json)
        with patch.object(rv.line_channel.httpx, "AsyncClient", _Peek):
            self.post()
        row = seen["row"]
        self.assertEqual((row["state"], row["text_version"], row["line_user_hash"], row["text"],
                          row["approval_record_id"]),
                         ("prepared", sl.text_version(TEXT), sl.line_user_hash("jikou", USER), TEXT, REC_ID))
        self.assertEqual(self.followup()["state"], "done")

    def test_line_failure_closes_prepared_row_without_recording(self):
        _FakeClient.responses = [_FakeResp(500, "x")]
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=failed")
        row = self.followup()
        self.assertEqual((row["state"], row["last_result"], row["text"]), ("closed", "line_failed", None))
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()
        self.assertEqual(self.followup()["state"], "closed")

    def test_crash_right_after_sent_is_recorded_by_next_job(self):
        """送信確定直後にプロセス停止（例外注入）→ prepared が残り、次回ジョブが台帳 sent と突合して記録。"""
        with patch.object(rv, "write_human_chatlog", AsyncMock(side_effect=RuntimeError("crash"))):
            with self.assertRaises(RuntimeError):
                self.post()
        self.assertEqual(self.ops()[0]["state"], "sent")
        self.assertEqual(self.followup()["state"], "prepared")
        self.create.assert_not_awaited()
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()
        self.assertEqual(self.create.await_args.args[1]["message"], rf.with_op_marker(TEXT, OP))
        self.assertEqual(self.followup()["state"], "done")

    def test_mark_pending_failure_keeps_prepared_row(self):
        self.form_fields.side_effect = RuntimeError("kintone down")
        with patch.object(rf, "mark_pending", AsyncMock(return_value="failed")):
            r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=failed&ex=saved")
        row = self.followup()
        self.assertEqual((row["state"], row["text"], row["text_version"]), ("prepared", TEXT, sl.text_version(TEXT)))
        self.form_fields.side_effect = None
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()
        self.assertEqual(self.followup()["state"], "done")

    def test_unconfigured_is_recovered_after_configuration(self):
        with patch.dict(os.environ, {"APP_CHATLOG": "", "TOKEN_CHATLOG": ""}):
            r = self.post()
            self.assertEqual(r.headers["location"], f"{PAGE}?done=sent&log=unconfigured&ex=saved")
            row = self.followup()
            self.assertEqual((row["state"], row["last_result"], row["attempts"]), ("unconfigured", "unconfigured", 0))
            _run(sl.run_recover_job())                                              # まだ未設定: 数えず待つ
            row = self.followup()
            self.assertEqual((row["state"], row["attempts"]), ("unconfigured", 0))
            self.create.assert_not_awaited()
        _run(sl.run_recover_job())                                                  # 設定後に記録
        self.create.assert_awaited_once()
        self.assertEqual(self.followup()["state"], "done")

    def test_ledger_unconfirmed_waits_for_human_confirmation(self):
        _FakeClient.raise_exc = RuntimeError("timeout")
        r = self.post()
        self.assertEqual(r.headers["location"], f"{PAGE}?done=unconfirmed")
        _FakeClient.raise_exc = None
        self.assertEqual(self.ops()[0]["state"], "unconfirmed")
        self.assertEqual(self.followup()["state"], "prepared")
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()                                            # 確定前は記録しない
        self.assertEqual(self.followup()["state"], "prepared")
        self.assertEqual(_run(rf.process_one(OP, **_args())), "waiting_ledger")
        self.assertEqual(_run(sl.confirm_by_human(OP, "sent", "line_delivered")), "ok")
        _run(sl.run_recover_job())
        self.create.assert_awaited_once()
        self.assertEqual(self.followup()["state"], "done")
        self.assertEqual(len(_FakeClient.calls), 1)                                 # 自動再送なし

    def test_ledger_failed_by_human_closes_row(self):
        _FakeClient.raise_exc = RuntimeError("timeout")
        self.post()
        _FakeClient.raise_exc = None
        self.assertEqual(_run(sl.confirm_by_human(OP, "failed", "line_not_delivered")), "ok")
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()
        self.assertEqual((self.followup()["state"], self.followup()["last_result"]), ("closed", "ledger_failed"))

    def test_row_without_ledger_is_closed_and_prepare_is_idempotent(self):
        kw = dict(business="jikou", approval_record_id=REC_ID, line_user_hash=sl.line_user_hash("jikou", USER),
                  text=TEXT, text_version=sl.text_version(TEXT))
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "prepared")
        self.assertEqual(_run(rf.prepare(OP3, **kw)), "exists")
        _run(sl.run_recover_job())
        self.create.assert_not_awaited()
        self.assertEqual((self.followup(OP3)["state"], self.followup(OP3)["last_result"]), ("closed", "not_sent"))
        self.assertFalse(_run(rf.close(OP3, "again")))                              # 終端は動かない
        self.assertFalse(_run(rf.complete(OP3)))

    def test_mark_pending_without_prepared_row_inserts_pending(self):
        res = _run(rf.mark_pending(OP3, last_result="failed", business="jikou", approval_record_id=REC_ID,
                                   line_user_hash=sl.line_user_hash("jikou", USER), text=TEXT))
        self.assertEqual(res, "pending")
        self.assertEqual(_run(rf.mark_pending(OP3, last_result="failed")), "exists")
        row = self.followup(OP3)
        self.assertEqual((row["state"], row["text_version"], row["text"]), ("pending", None, TEXT))


# ── pin（migration・docs 契約） ──────────────────────────────────────────────────
class TestPinsFix2(unittest.TestCase):
    def test_migration_same_revision_carries_fix2_columns(self):
        src = (REPO / "alembic" / "versions" / "20261011_f7b2c9d4e1a6_reply_exemplar.py").read_text(encoding="utf-8")
        self.assertIn("down_revision: Union[str, Sequence[str], None] = 'e5f8a1b4c7d9'", src)
        for needle in ('sa.Column("status", sa.Text, nullable=False, server_default="pending_review")',
                       "ck_reply_exemplar_status", "'created', 'used', 'deactivated', 'approved'",
                       'sa.Column("approved_by", sa.Text, nullable=True)',
                       'sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True)',
                       '"text_version"', '"owner_token"', '"deadline_at"', '"chatlog_record_id"',
                       "'prepared', 'pending', 'unconfigured', 'in_progress', 'done', 'closed', 'exhausted'"):
            self.assertIn(needle, src, needle)

    def test_contract_doc_states_ledger_identity_idempotency_and_migration_step(self):
        p = REPO / "docs" / "jikou_reply_q1b_contract.md"
        self.assertTrue(p.exists())
        doc = p.read_text(encoding="utf-8")
        for needle in ("purpose=reply", "actor=human", "op_id", "inbound_event_id", "e5f8a1b4c7d9", "f7b2c9d4e1a6",
                       "a1c3", "b2d4", "pending_review", "[op:", "prepared", "confirm_by_human"):
            self.assertIn(needle, doc, needle)


if __name__ == "__main__":
    unittest.main()
