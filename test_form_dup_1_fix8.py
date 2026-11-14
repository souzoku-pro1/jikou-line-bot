"""FORM-DUP-1-fix8（Codex R-FD1-fix7 BF-12・BF-13・BF-14）: 個別修正ではなく不変条件を 1 か所で保証する。
 不変条件（Fable 裁定）: 「外部書込（kintone の create/update）を開始した後は、成功の記録か unknown の保持の
 どちらかで必ず終わる。予約の解放は、書込を開始していないことが確実な場合に限る。」
 - 予約オブジェクト（_NonceEntry）の write_started を外部書込の呼出直前（同期・await なし）に立てる。
   plain（_persist_with_number）・linked（_handle_submit_linked／_update_existing_linked）で同じ予約を使う
 - 成功は書込が返った直後（通知・mark_used・写真トークン等の他の await より前）に同期で記録する
 - 後始末は _submit_with_nonce の finally の _nonce_settle 1 か所（結果記録済み → そのまま／write_started かつ
   結果なし → unknown 保持／write_started でない → 解放）。_hold_unknown・_nonce_release は撤去（AST で確認）
 - 確定失敗（解放）は kintone の明示的拒否（4xx かつ kintone のエラー code あり）だけ。5xx・通信例外・
   応答解析失敗（JSON 不正・id 欠落）・code の無い 4xx・キャンセル・未分類例外はすべて unknown
 - linked（BF-14）: 通信例外・5xx・書込中キャンセル・未分類例外 → 不明ページ（409）・token 予約（SLL-02）も
   解放しない。作成成功後の mark_used 失敗 → 結果は記録済み（同じ nonce の再送は同じ完了画面・再作成なし）
 既存: fix1 37・fix2 11・fix3 7・fix4 10・fix5 8・fix6 12・fix7 14 件は件数不変。書き換え（削除・skip なし）:
 fix2 test_reserve_and_release_semantics・fix3 test_inflight_entries_count_toward_limit_in_reserve の
 _nonce_release → _nonce_settle、test_shindan_line_link の T7 5xx 部と SLL-07 2 件（結果画面＋解放 → 不明ページ
 ＋保持）。
"""

import ast
import asyncio
import json
import logging
import unittest
from unittest.mock import AsyncMock, patch

from test_form_dup_1 import _Base, _definite_failure  # noqa: E402
from test_form_dup_1_fix2 import _Counting, _request  # noqa: E402
from test_form_dup_1_fix4 import _Clock, _NUMBER_RE  # noqa: E402
from test_form_dup_1_fix7 import HELD_MSG, _transport  # noqa: E402
from test_shindan_line_link import USER, VALID, _run  # noqa: E402

import time as _real_time  # noqa: E402

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402
from hub import kintone as _hub_kintone  # noqa: E402
from hub import shindan_link as sl  # noqa: E402

_REAL_CREATE = _hub_kintone.create_record      # setUp の差し替え前（import 時）の実関数（BF-13 用）
LINK_UNKNOWN_MSG = "shindan_link_write_unknown"
LINK_FAILED_MSG = "shindan_link_write_failed"


class TestWriteStartedInvariant(_Base):
    def setUp(self):
        super().setUp()
        _Counting.made = []
        sf._nonce_marks.clear()
        self.addCleanup(sf._nonce_marks.clear)
        self.clock = _Clock(_real_time.time())
        for p in (patch.object(sf, "_NonceEntry", _Counting),
                  patch.object(sf, "time", self.clock),
                  patch.object(sf, "_draw_number", side_effect=[f"{i:06d}" for i in range(600001, 600099)])):
            p.start()
            self.addCleanup(p.stop)

    # ── helpers ───────────────────────────────────────────────────────────────
    def _form(self, nonce, **over):
        data = dict(VALID)
        data.update(over)
        if nonce:
            data[sf.NONCE_FIELD] = nonce
        return data

    @staticmethod
    def _body(r) -> str:
        return r.text if hasattr(r, "text") else r.body.decode("utf-8")

    def _num(self, r) -> str:
        m = _NUMBER_RE.search(self._body(r))
        self.assertIsNotNone(m)
        return m.group(1)

    def _warn(self, msg):
        return [r for r in self.cap.records if r.levelno == logging.WARNING and msg in r.getMessage()]

    def _assert_unknown_page(self, r):
        body = self._body(r)
        self.assertEqual(r.status_code, 409)
        self.assertIn(sf.UNKNOWN_TEXT, body)
        self.assertIn(f"<h1>{sf.UNKNOWN_TITLE}</h1>", body)
        for absent in ("受付番号", sf.PHOTO_ROUTE, "診断結果", VALID["creditor"], sf.LINKED_DONE_TEXT,
                       sf.PLAIN_PENDING_TEXT, sf.STORE_FULL_TEXT, sf.RESUBMIT_DONE_TEXT, sf.STALE_TEXT):
            self.assertNotIn(absent, body)

    def _assert_linked_result_only(self, r):
        """従来の判定結果のみの画面（固定文言あり・受付番号なし・写真導線なし）。"""
        body = self._body(r)
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.LINKED_DONE_TEXT, body)
        self.assertNotIn(sf.PHOTO_ROUTE, body)
        self.assertNotIn("受付番号", body)

    # ── BF-12（plain）: 通知待機中のキャンセル → 結果は書込直後に記録済み → 再送で作成 1 のまま ─────
    def test_bf12_cancel_while_awaiting_notify_keeps_result_resend_creates_nothing(self):
        n = sf.issue_nonce(self.clock.t)
        gate = asyncio.Event()

        async def _notify(*_a, **_k):
            await gate.wait()
            return True

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            self.assertEqual(len(self.fake.rows), 1)                   # 書込は返っている・受付通知を待機中
            entry = sf._nonce_results[n]
            self.assertEqual(entry.result[0], "plain")                 # 書込直後に同期で記録済み
            self.assertFalse(entry.event.is_set())
            t_a.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await t_a
            self.assertIs(sf._nonce_results[n], entry)                 # 解放されず・不明にもならない
            self.assertEqual(entry.result[0], "plain")
            self.assertTrue(entry.event.is_set())
            return entry, await sf.shindan_entry(_request(self._form(n)))
        with patch.object(sf.notify, "notify_business", _notify):
            entry, r_b = _run(_go())
        db.reset_for_tests()
        self.assertEqual(r_b.status_code, 200)
        self.assertEqual(self._num(r_b), "600001")                     # 1 回目と同じ受付番号
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(len(_Counting.made), 1)                       # 新しい予約も作らない
        self.assertEqual(self._warn(HELD_MSG), [])
        self.assertIn("duplicate post (no write) kind=plain", self.cap.text())
        self.assertNotIn(n, self.cap.text())

    # ── BF-12（linked）: mark_used 待機中のキャンセル → 結果は mark_used より前に記録済み → 再作成なし ──
    def test_bf12_linked_cancel_during_mark_used_keeps_result_no_recreate(self):
        token = self.issue()
        n = sf.issue_nonce(self.clock.t)
        gate = asyncio.Event()
        real_mark = sl.mark_used

        async def _mark(t, c, now=None):
            await gate.wait()
            return await real_mark(t, c, now)

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n), token=token)))
            await asyncio.sleep(0.1)
            self.assertEqual(len(self.fake.rows), 1)                   # 作成は返っている・mark_used 待機中
            entry = sf._nonce_results[n]
            self.assertEqual(entry.result[0], "linked")                # mark_used（await）より前に同期で記録
            t_a.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await t_a
            self.assertIs(sf._nonce_results[n], entry)
            return entry, await sf.shindan_entry(_request(self._form(n), token=token))
        with patch.object(sl, "mark_used", _mark):
            entry, r_b = _run(_go())
        db.reset_for_tests()
        body = self._body(r_b)
        self.assertEqual(r_b.status_code, 200)
        self.assertIn(sf.LINKED_DONE_TEXT, body)                       # 1 回目と同じ linked 完了画面（記憶から）
        self.assertIn(sf.PHOTO_ROUTE, body)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self.fake.create_calls[0]["LINEユーザーID"], USER)
        self.assertEqual(self.upload_rids(), {entry.result[1]})        # 写真導線は同じレコードへ
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(self._warn(HELD_MSG), [])
        self.assertIsNone(self.used_at(token))                         # used_at は打てていない
        self.assertIsNone(self.claimed_at(token))                      # 書込は成功＝token 予約は従来どおり解放
        self.assertIn("duplicate post (no write) kind=linked", self.cap.text())

    # ── BF-13: 実 create_record で保存済み・応答に id 欠落／JSON 不正 → 不明ページ・再送で作成 1 ────────
    def _real_create_patches(self, resp_factory):
        landed = []

        async def _write(method, url, app, json_body):
            landed.append(json_body)                                   # 書込は kintone に届いている
            return resp_factory()
        return landed, (patch.object(sf.hub_kintone, "create_record", _REAL_CREATE),
                        patch.object(sf.hub_kintone, "_write", _write))

    def _bf13(self, resp_factory):
        n = self.get_nonce()
        landed, (p1, p2) = self._real_create_patches(resp_factory)
        with p1, p2:
            r1 = self.post_plain(nonce=n)                              # 実 create_record → resp.json()["id"] で失敗
            self.assertEqual(len(landed), 1)
            self.assertEqual(landed[0]["record"]["受付番号"], {"value": "600001"})
            self._assert_unknown_page(r1)
            entry = sf._nonce_results[n]
            self.assertEqual(entry.result[:3], ("unknown", "", "600001"))
            self.assertEqual(entry.result[3], entry.pattern)
            self.assertTrue(entry.write_started)
            r2 = self.post_plain(nonce=n)                              # 再送 → 不明ページ・作成 0
            self._assert_unknown_page(r2)
            r3 = self.post_plain(nonce=n, creditor="別の債権者")
            self._assert_unknown_page(r3)
            self.assertEqual(len(landed), 1)
        self.assertIs(sf._nonce_results[n], entry)
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.assertEqual(self.notify_admin.await_count, 1)             # 既存の要確認通知のみ
        self.assertIn("unknown", self.notify_admin.await_args.args[0])
        self.notify_biz.assert_not_awaited()
        self.assertIn("create failed kind=unknown (unparsable response)", self.cap.text())
        self.assertNotIn(n, self.cap.text())
        self.assertNotIn("Traceback", self.cap.text())

    def test_bf13_real_create_response_without_id_is_unknown_resend_creates_nothing(self):
        class _Resp:
            @staticmethod
            def json():
                return {"revision": "1"}                               # id 欠落
        self._bf13(_Resp)

    def test_bf13_real_create_invalid_json_is_unknown_resend_creates_nothing(self):
        class _Resp:
            text = "<html>SECRET_HTML</html>"

            @staticmethod
            def json():
                raise json.JSONDecodeError("Expecting value SECRET_JSON", "doc", 0)
        self._bf13(_Resp)
        self.assertNotIn("SECRET_JSON", self.cap.text())
        self.assertNotIn("SECRET_HTML", self.cap.text())

    # ── BF-14（linked）: 通信例外（着）→ 不明ページ・token 予約も解放しない・再送で作成 1 ─────────────
    def _linked_unknown_common(self, token, n, r1, *, rows: int):
        self._assert_unknown_page(r1)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), rows)
        entry = sf._nonce_results[n]
        self.assertEqual(entry.result[0], "unknown")
        self.assertTrue(entry.write_started)
        self.assertIsNotNone(self.claimed_at(token))                   # token 予約（SLL-02）は解放しない
        self.assertIsNone(self.used_at(token))
        errs = self.cap.errors()
        self.assertEqual(len(errs), 1)
        self.assertIn(LINK_UNKNOWN_MSG, errs[0])
        self.assertNotIn(token, errs[0])
        # 同じ nonce の再送 → 不明ページ（照合より前に記憶から）・作成 0
        r2 = self.post_linked_n(token, n)
        self._assert_unknown_page(r2)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIs(sf._nonce_results[n], entry)
        # 別 nonce → token は予約中＝busy（判定結果のみ・作成 0・解放されていない証拠）
        r3 = self.post_linked_n(token, self.get_nonce())
        self._assert_linked_result_only(r3)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.busy_lines()), 1)
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.assertEqual(len(self.cap.errors()), 1)
        self.notify_admin.assert_not_awaited()                         # linked に要確認通知は無い（増やさない）
        self.notify_biz.assert_not_awaited()
        self.assertNotIn(n, self.cap.text())

    def test_bf14_linked_transport_exception_is_unknown_page_hold_no_recreate(self):
        token = self.issue()
        n = self.get_nonce()
        real_create = self.fake.create_record

        async def _create(app, fields):
            await real_create(app, fields)                             # 保存は成立（着）
            raise _transport()                                         # 応答が届かない
        with patch.object(sf.hub_kintone, "create_record", _create):
            r1 = self.post_linked_n(token, n)
        self._linked_unknown_common(token, n, r1, rows=1)

    def test_bf14_linked_5xx_is_unknown_page_hold_no_recreate(self):
        token = self.issue()
        n = self.get_nonce()
        self.fake.create_fail_next = 1                                 # 既定 500（結果不明）
        r1 = self.post_linked_n(token, n)
        self._linked_unknown_common(token, n, r1, rows=0)

    # ── BF-14（linked）: 書込中のキャンセル → unknown 保持（nonce も token 予約も解放しない）────────────
    def test_bf14_linked_cancel_during_create_holds_unknown_and_claim(self):
        token = self.issue()
        n = sf.issue_nonce(self.clock.t)
        gate = asyncio.Event()

        async def _create(app, fields):
            self.fake.create_calls.append(dict(fields))
            await gate.wait()                                          # 書込を開始した後に戻らない
            return "1"

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n), token=token)))
            await asyncio.sleep(0.1)
            self.assertEqual(len(self.fake.create_calls), 1)
            entry = sf._nonce_results[n]
            self.assertTrue(entry.write_started)
            self.assertIsNone(entry.result)
            t_a.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await t_a
            self.assertEqual(entry.result[0], "unknown")               # finally で不明として保持
            self.assertTrue(entry.event.is_set())
            return entry, await sf.shindan_entry(_request(self._form(n), token=token))
        with patch.object(sf.hub_kintone, "create_record", _create):
            entry, r_b = _run(_go())
        db.reset_for_tests()
        self._assert_unknown_page(r_b)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.assertIsNotNone(self.claimed_at(token))                   # token 予約も解放しない
        self.assertIsNone(self.used_at(token))
        self.notify_admin.assert_not_awaited()

    # ── BF-14（linked）: 作成成功後の mark_used 失敗 → 結果は記録済み → 同じ nonce の再送は同じ完了画面・再作成なし ──
    def test_bf14_mark_used_failure_after_create_keeps_result_no_recreate(self):
        token = self.issue()
        n = self.get_nonce()
        with patch.object(sl, "mark_used", AsyncMock(return_value=False)):
            r1 = self.post_linked_n(token, n)
        # 1 回目: 従来どおり判定結果のみ・used_at なし・token 予約は解放（書込は成功している）・ERROR 1 行
        self._assert_linked_result_only(r1)
        self.assertEqual(len(self.fake.rows), 1)
        rid = list(self.fake.rows)[0]
        entry = sf._nonce_results[n]
        self.assertEqual(entry.result, ("linked", rid, "", entry.pattern))   # 書込直後に記録済み
        self.assertIsNone(self.used_at(token))
        self.assertIsNone(self.claimed_at(token))
        self.assertEqual(len(self.cap.errors()), 1)
        self.assertIn(LINK_FAILED_MSG, self.cap.errors()[0])
        # 同じ nonce の再送 → 記録済みの完了画面（同じレコードへの写真導線）・再作成なし・不明ではない
        r2 = self.post_linked_n(token, n)
        body = self._body(r2)
        self.assertEqual(r2.status_code, 200)
        self.assertIn(sf.LINKED_DONE_TEXT, body)
        self.assertIn(sf.PHOTO_ROUTE, body)
        self.assertNotIn(sf.UNKNOWN_TEXT, body)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self.upload_rids(), {rid})
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(self._warn(HELD_MSG), [])
        self.assertIn("duplicate post (no write) kind=linked", self.cap.text())
        # mark_used が例外（DB 障害）でも同じ（別 token・同じ本人 → found → noop → 結果記録・作成なし）
        token2 = self.issue()
        n2 = self.get_nonce()
        with patch.object(sl, "mark_used", AsyncMock(side_effect=RuntimeError("db SECRET_DB"))):
            r3 = self.post_linked_n(token2, n2)
        self._assert_linked_result_only(r3)
        self.assertEqual(sf._nonce_results[n2].result[:2], ("linked", rid))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertNotIn("SECRET_DB", self.cap.text())

    # ── BF-14（linked・更新経路）: update の 5xx → 不明ページ・update 1 回・再取得なし ─────────────────
    def test_bf14_linked_update_5xx_is_unknown_page_update_once(self):
        self.fake.add("10")                                            # 本人の既存レコード（空欄あり）
        token = self.issue()
        n = self.get_nonce()

        async def _update(app, rid, fields, revision=None):
            self.fake.update_calls.append((str(rid), dict(fields), revision))
            raise sf.hub_kintone.KintoneError(503, "GAIA_XX02", "x")
        with patch.object(sf.hub_kintone, "update_record", _update):
            r1 = self.post_linked_n(token, n)
            self._assert_unknown_page(r1)
            r2 = self.post_linked_n(token, n)
            self._assert_unknown_page(r2)
        self.assertEqual(len(self.fake.update_calls), 1)
        self.assertEqual(self.fake.create_calls, [])
        self.assertEqual(self.fake.get_calls, ["10"])                  # CAS の再取得はしない
        entry = sf._nonce_results[n]
        self.assertEqual(entry.result[0], "unknown")
        self.assertTrue(entry.write_started)
        self.assertIsNotNone(self.claimed_at(token))
        self.assertIsNone(self.used_at(token))
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.assertIn(LINK_UNKNOWN_MSG, self.cap.errors()[0])
        # 単体: _update_existing_linked は unknown を返し write_started は立ったまま
        resv = sf._NonceEntry(self.clock.t, ())
        with patch.object(sf.hub_kintone, "update_record", _update):
            outcome, rid = _run(sf._update_existing_linked(
                {k: dict(v) for k, v in self.fake.rows["10"].items()}, {"診断パターン": "B"}, resv))
        db.reset_for_tests()
        self.assertEqual((outcome, rid), ("unknown", "10"))
        self.assertTrue(resv.write_started)
        self.assertIsNone(resv.result)
        # 単体: revision 競合（409・code あり）は明示的拒否＝write_started を下ろして再取得（従来の SLL-05）
        resv2 = sf._NonceEntry(self.clock.t, ())
        self.fake.conflict_next = 2
        outcome2, _ = _run(sf._update_existing_linked(
            {k: dict(v) for k, v in self.fake.rows["10"].items()}, {"診断パターン": "B"}, resv2))
        db.reset_for_tests()
        self.assertEqual(outcome2, "unconverged")
        self.assertFalse(resv2.write_started)

    # ── 票: 明示的 4xx 検証エラーは解放されて取り直し可（plain・linked） ──────────────────────────────
    def test_definite_4xx_reject_releases_and_retake_creates(self):
        # plain: 400 CB_VA01（受付番号以外の欄）＝kintone の明示的拒否 → 解放 → 取り直しで作成
        n = self.get_nonce()
        self.fake.create_fail_next = 1
        self.fake.create_fail_error = sf.hub_kintone.KintoneError(
            400, "CB_VA01", "x", errors={"record.診断パターン.value": {"messages": ["x"]}})
        r1 = self.post_plain(nonce=n)
        self.assertEqual(r1.status_code, 500)
        self.assertNotIn(n, sf._nonce_results)                         # 解放
        self.assertFalse(_Counting.made[0].write_started)              # 確定失敗で下ろされている
        r2 = self.post_plain(nonce=n)
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(self._num(r2), "600002")
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(sf._nonce_results[n].result[0], "plain")
        # linked: 403（code あり）→ 解放（nonce も token 予約も）→ 再送で作成
        token = self.issue()
        n2 = self.get_nonce()
        self.fake.create_fail_next = 1
        self.fake.create_fail_error = _definite_failure()
        r3 = self.post_linked_n(token, n2)
        self._assert_linked_result_only(r3)
        self.assertNotIn(n2, sf._nonce_results)
        self.assertIsNone(self.claimed_at(token))
        self.assertIsNone(self.used_at(token))
        self.assertIn(LINK_FAILED_MSG, self.cap.errors()[-1])          # 直前の ERROR（plain の 1 行目ではない）
        r4 = self.post_linked_n(token, n2)
        self.assertEqual(r4.status_code, 200)
        self.assertIn(sf.PHOTO_ROUTE, self._body(r4))
        self.assertIsNotNone(self.used_at(token))
        self.assertEqual(len(self.fake.create_calls), 4)               # plain 2 + linked 2
        self.assertEqual(len(self.fake.rows), 2)
        self.assertEqual(self._warn(HELD_MSG), [])
        self.assertEqual(len(_Counting.made), 4)                       # 取り直しごとに新しい予約

    # ── 票 4: 確定失敗は「4xx かつ kintone の code あり」だけ。code の無い 4xx・5xx・transport は unknown ──
    def test_4xx_without_kintone_code_is_unknown_not_definite(self):
        K = sf.hub_kintone.KintoneError
        for e in (K(404, "", ""), K(400, "", "<html>"), K(502, "", ""), K(0, "transport_error", "x"),
                  K(500, "GAIA_UN01", "x")):
            self.assertFalse(sf._definite_reject(e), e)
            self.assertEqual(sf._classify_create_error(e), "unknown", e)
        for e in (K(400, "CB_VA01", "x"), K(403, "GAIA_AP15", "x"), K(401, "GAIA_IA02", "x"),
                  K(404, "GAIA_RE01", "x"), sf.hub_kintone.KintoneConflict(409, "GAIA_CO02", "x")):
            self.assertTrue(sf._definite_reject(e), e)
        self.assertEqual(sf._classify_create_error(K(403, "GAIA_AP15", "x")), "failed")
        self.assertEqual(sf._classify_create_error(
            K(400, "CB_VA01", "x", errors={sf.UNIQUE_VIOLATION_ERROR_KEY: {"messages": ["x"]}})), "duplicate")
        # 実入口: code の無い 404（proxy 等の応答の可能性）は結果不明＝保持・不明ページ
        n = self.get_nonce()
        self.fake.create_fail_next = 1
        self.fake.create_fail_error = K(404, "", "")
        r = self.post_plain(nonce=n)
        self._assert_unknown_page(r)
        self.assertEqual(sf._nonce_results[n].result[0], "unknown")
        self.assertTrue(sf._nonce_results[n].write_started)
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        r2 = self.post_plain(nonce=n)
        self._assert_unknown_page(r2)
        self.assertEqual(len(self.fake.create_calls), 1)

    # ── 票: 書込開始前の失敗（token 照合障害・本人レコード検索障害・入力検証）は解放 ─────────────────
    def test_prewrite_failures_release_reservation_and_claim(self):
        token = self.issue()
        n = self.get_nonce()
        # token 照合障害（BF-01）→ 500・予約は解放・write_started は立たない
        with patch.object(sl, "lookup", AsyncMock(side_effect=RuntimeError("db"))):
            r1 = self.post_linked_n(token, n)
        self.assertEqual(r1.status_code, 500)
        self.assertNotIn(n, sf._nonce_results)
        self.assertFalse(_Counting.made[-1].write_started)
        self.assertEqual(self.fake.create_calls, [])
        # 本人レコード検索の障害（find_user_record → failed）→ 書込前 → nonce も token 予約も解放
        with patch.object(sf.hub_kintone, "search_records",
                          AsyncMock(side_effect=sf.hub_kintone.KintoneError(503, "", ""))):
            r2 = self.post_linked_n(token, n)
        self._assert_linked_result_only(r2)
        self.assertNotIn(n, sf._nonce_results)
        self.assertFalse(_Counting.made[-1].write_started)
        self.assertIsNone(self.claimed_at(token))
        self.assertEqual(self.fake.create_calls, [])
        self.assertIn(LINK_FAILED_MSG, self.cap.errors()[0])
        # 復旧後は同じ nonce・同じ token で通常どおり作成
        r3 = self.post_linked_n(token, n)
        self.assertEqual(r3.status_code, 200)
        self.assertIn(sf.PHOTO_ROUTE, self._body(r3))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(sf._nonce_results[n].result[0], "linked")
        self.assertIsNotNone(self.used_at(token))
        # 入力検証（閉集合違反）は予約より前 → エントリも作らない
        made = len(_Counting.made)
        r4 = self.post_plain(nonce=self.get_nonce(), borrow="不正な値")
        self.assertEqual(r4.status_code, 400)
        self.assertEqual(len(_Counting.made), made)
        self.assertEqual(list(sf._nonce_results), [n])
        self.assertEqual(self._warn(HELD_MSG), [])

    # ── 実関数 _persist_with_number: 呼出直前に write_started・返った直後に結果（同期）・確定失敗だけ下ろす ──
    def test_persist_with_number_flags_are_set_synchronously_around_the_write(self):
        _early, pattern, fields_base = sf._prepare_submission(dict(VALID))
        seen = {}
        resv = sf._NonceEntry(self.clock.t, ())
        resv.pattern = pattern
        real_create = self.fake.create_record

        async def _create(app, fields):
            seen["started"] = resv.write_started                       # 呼出時点で既に立っている
            seen["result"] = resv.result
            return await real_create(app, fields)
        with patch.object(sf.hub_kintone, "create_record", _create):
            outcome, rid = _run(sf._persist_with_number(fields_base, "600050", resv))
        db.reset_for_tests()
        self.assertEqual(outcome, "created")
        self.assertEqual(seen, {"started": True, "result": None})
        self.assertEqual(resv.result, ("plain", rid, "600050", pattern))   # 返った直後に同期で記録
        self.assertEqual(resv.number, "600050")
        self.assertTrue(resv.write_started)
        # 確定失敗（403・code あり）→ write_started を下ろす・結果なし（解放対象）
        resv2 = sf._NonceEntry(self.clock.t, ())
        self.fake.create_fail_next, self.fake.create_fail_error = 1, _definite_failure()
        self.assertEqual(_run(sf._persist_with_number(fields_base, "600051", resv2)), ("failed", ""))
        db.reset_for_tests()
        self.assertFalse(resv2.write_started)
        self.assertIsNone(resv2.result)
        # 一意制約違反（確定・再採番可）→ 下ろす
        resv3 = sf._NonceEntry(self.clock.t, ())
        self.fake.create_fail_next = 1
        self.fake.create_fail_error = sf.hub_kintone.KintoneError(
            400, "CB_VA01", "dup", errors={sf.UNIQUE_VIOLATION_ERROR_KEY: {"messages": ["x"]}})
        self.assertEqual(_run(sf._persist_with_number(fields_base, "600052", resv3)), ("duplicate", ""))
        db.reset_for_tests()
        self.assertFalse(resv3.write_started)
        # 5xx → unknown・write_started のまま・結果なし（保持対象）
        resv4 = sf._NonceEntry(self.clock.t, ())
        self.fake.create_fail_next, self.fake.create_fail_error = 1, None
        self.assertEqual(_run(sf._persist_with_number(fields_base, "600053", resv4)), ("unknown", ""))
        db.reset_for_tests()
        self.assertTrue(resv4.write_started)
        self.assertIsNone(resv4.result)
        # 未分類例外（応答解析失敗）→ unknown・write_started のまま・例外は伝播しない
        resv5 = sf._NonceEntry(self.clock.t, ())
        with patch.object(sf.hub_kintone, "create_record", AsyncMock(side_effect=KeyError("id"))):
            self.assertEqual(_run(sf._persist_with_number(fields_base, "600054", resv5)), ("unknown", ""))
        db.reset_for_tests()
        self.assertTrue(resv5.write_started)
        self.assertIsNone(resv5.result)
        # resv なし（従来の呼び方）も動く
        self.assertEqual(_run(sf._persist_with_number(fields_base, "600055"))[0], "created")
        db.reset_for_tests()

    # ── 静的検査（AST）: 解放と unknown 保持は _submit_with_nonce の finally の _nonce_settle 1 か所だけ ────
    def test_static_settle_is_the_only_release_or_hold_site(self):
        with open(sf.__file__, encoding="utf-8") as f:
            tree = ast.parse(f.read())
        funcs = {n.name: n for n in ast.walk(tree)
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        self.assertNotIn("_nonce_release", funcs)
        self.assertNotIn("_hold_unknown", funcs)
        self.assertIn("_nonce_settle", funcs)
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                 and n.func.id in ("_nonce_settle", "_nonce_release", "_hold_unknown")]
        self.assertEqual([c.func.id for c in calls], ["_nonce_settle"])
        # その 1 回は _submit_with_nonce の try の finalbody にある
        swn = funcs["_submit_with_nonce"]
        in_final = [n for t in ast.walk(swn) if isinstance(t, ast.Try)
                    for stmt in t.finalbody for n in ast.walk(stmt) if n is calls[0]]
        self.assertEqual(len(in_final), 1)

        def _tuple_assigns(node, first):
            return [a for a in ast.walk(node) if isinstance(a, ast.Assign)
                    and isinstance(a.value, ast.Tuple) and a.value.elts
                    and isinstance(a.value.elts[0], ast.Constant) and a.value.elts[0].value in first]
        # unknown の保持（result = ("unknown", …)）は _nonce_settle の中にしか無い
        self.assertEqual(len(_tuple_assigns(tree, ("unknown",))), 1)
        self.assertEqual(len(_tuple_assigns(funcs["_nonce_settle"], ("unknown",))), 1)
        # 予約表からの除去（_nonce_results.pop）は _nonce_settle（解放）と _nonce_reserve（保持期限超の破棄）だけ
        pops = sorted({f for f, fn in funcs.items() for n in ast.walk(fn)
                       if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                       and n.func.attr == "pop" and isinstance(n.func.value, ast.Name)
                       and n.func.value.id == "_nonce_results"})
        self.assertEqual(pops, ["_nonce_reserve", "_nonce_settle"])
        # write_started = True の直後の await は kintone の create_record／update_record（間に他の await なし）
        starts = []
        for fname, fn in funcs.items():
            for a in ast.walk(fn):
                if (isinstance(a, ast.Assign) and isinstance(a.targets[0], ast.Attribute)
                        and a.targets[0].attr == "write_started"
                        and isinstance(a.value, ast.Constant) and a.value.value is True):
                    awaits = sorted((n for n in ast.walk(fn) if isinstance(n, ast.Await)
                                     and n.lineno > a.lineno), key=lambda n: (n.lineno, n.col_offset))
                    self.assertTrue(awaits, fname)
                    call = awaits[0].value
                    self.assertIsInstance(call, ast.Call, fname)
                    self.assertEqual(call.func.value.id, "hub_kintone", fname)
                    self.assertIn(call.func.attr, ("create_record", "update_record"), fname)
                    starts.append(fname)
        self.assertEqual(sorted(starts), ["_handle_submit_linked", "_persist_with_number",
                                          "_update_existing_linked"])
        # 成功の記録（result = ("plain"|"linked", …)）は書込の直後の 2 箇所だけ
        rec = sorted(f for f, fn in funcs.items() if _tuple_assigns(fn, ("plain", "linked")))
        self.assertEqual(rec, ["_handle_submit_linked", "_persist_with_number"])
        # _nonce_settle は同期関数（await なし）
        self.assertIsInstance(funcs["_nonce_settle"], ast.FunctionDef)
        self.assertEqual([n for n in ast.walk(funcs["_nonce_settle"]) if isinstance(n, ast.Await)], [])

    def test_fix1_to_fix7_counts_unchanged(self):
        import test_form_dup_1 as fix1
        import test_form_dup_1_fix2 as fix2
        import test_form_dup_1_fix3 as fix3
        import test_form_dup_1_fix4 as fix4
        import test_form_dup_1_fix5 as fix5
        import test_form_dup_1_fix6 as fix6
        import test_form_dup_1_fix7 as fix7
        load = unittest.defaultTestLoader.loadTestsFromModule
        self.assertEqual(tuple(load(m).countTestCases()
                               for m in (fix1, fix2, fix3, fix4, fix5, fix6, fix7)),
                         (37, 11, 7, 10, 8, 12, 14))


if __name__ == "__main__":
    unittest.main()
