"""FORM-DUP-1-fix7（Codex R-FD1-fix6 BF-10・BF-11）:
 BF-11: 書込結果の「確定失敗」（4xx・kintone の明示的拒否）と「結果不明」（unknown: 外部書込を
   開始した後の通信例外・5xx・キャンセル）を nonce の状態でも分ける。確定失敗だけ予約を解放して
   取り直しを許す。unknown は予約を解放せず「不明」として 24 時間保持（容量退避なし）し、初回も
   同じ nonce の再 POST も固定ページ（409・作成 0・採番なし）。WARNING 1 行（件数のみ）。
 BF-10（別方式）: 確定済みエントリを容量で退避するとき「作成済みの印」（sha256(nonce)・期限）を
   24 時間別集合に残す。印のある nonce の再 POST は作成せず fix1 BF-03 と同じ「送信済み」ページ。
   印の集合は上限 200,000・期限切れから捨て・上限到達時は最古から。
 既存: fix1 37・fix2 11・fix3 7・fix4 10・fix5 8・fix6 12 件は件数不変（取り直しを検査する 5 箇所は
 「確定失敗」を差す書き換え・test_jikou_form1 の結果不明 4 件は 409 ページへ書き換え）。
"""

import asyncio
import hashlib
import logging
import os
import unittest
from unittest.mock import patch

from test_form_dup_1 import _Base, _definite_failure  # noqa: E402
from test_form_dup_1_fix2 import _Counting, _request  # noqa: E402
from test_form_dup_1_fix3 import FULL_MSG, _done  # noqa: E402
from test_form_dup_1_fix4 import _Clock, _NUMBER_RE  # noqa: E402
from test_shindan_line_link import VALID, _run  # noqa: E402

import time as _real_time  # noqa: E402

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402

PIN_UNKNOWN = "2981afed6d0c7d50dc5fcef45ac2bffe34776df88096639681df2b2fc0225bf1"
HELD_MSG = "write result unknown (held, no retry)"
DUP_UNKNOWN_MSG = "duplicate post (no write) kind=unknown"
DUP_MARKED_MSG = "duplicate post (no write) kind=marked"
H = 3600
_DOCS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs", "ops", "shindan_form_dedup.md")


def _transport() -> "sf.hub_kintone.KintoneError":
    return sf.hub_kintone.KintoneError(0, "transport_error", "ReadTimeout")


class TestUnknownHoldAndCreatedMark(_Base):
    def setUp(self):
        super().setUp()
        _Counting.made = []
        sf._nonce_marks.clear()
        self.addCleanup(sf._nonce_marks.clear)
        self.clock = _Clock(_real_time.time())
        for p in (patch.object(sf, "_NonceEntry", _Counting),
                  patch.object(sf, "time", self.clock),
                  patch.object(sf, "_draw_number", side_effect=[f"{i:06d}" for i in range(500001, 500099)])):
            p.start()
            self.addCleanup(p.stop)

    # ── helpers ───────────────────────────────────────────────────────────────
    def _form(self, nonce, **over):
        data = dict(VALID)
        data.update(over)
        if nonce:
            data[sf.NONCE_FIELD] = nonce
        return data

    def _num(self, r) -> str:
        m = _NUMBER_RE.search(r.body.decode("utf-8"))
        self.assertIsNotNone(m)
        return m.group(1)

    def _warn(self, msg):
        return [r for r in self.cap.records if r.levelno == logging.WARNING and msg in r.getMessage()]

    def _landed_transport(self, times: int = 1):
        """実 fake の create（保存は成立）を通した後に transport 例外（結果不明・着）。"""
        real_create = self.fake.create_record
        left = {"n": times}

        async def _create(app, fields):
            rid = await real_create(app, fields)
            if left["n"] > 0:
                left["n"] -= 1
                raise _transport()
            return rid
        return patch.object(sf.hub_kintone, "create_record", _create)

    def _unlanded_transport(self, times: int = 1):
        """保存せず transport 例外（結果不明・未着）。"""
        real_create = self.fake.create_record
        left = {"n": times}

        async def _create(app, fields):
            if left["n"] > 0:
                left["n"] -= 1
                self.fake.create_calls.append(dict(fields))
                raise _transport()
            return await real_create(app, fields)
        return patch.object(sf.hub_kintone, "create_record", _create)

    def _assert_unknown_page(self, r):
        body = r.text if hasattr(r, "text") else r.body.decode("utf-8")
        self.assertEqual(r.status_code, 409)
        self.assertIn(sf.UNKNOWN_TEXT, body)
        self.assertIn(f"<h1>{sf.UNKNOWN_TITLE}</h1>", body)
        for absent in ("受付番号", sf.PHOTO_ROUTE, "診断結果", VALID["creditor"],
                       sf.PLAIN_PENDING_TEXT, sf.STORE_FULL_TEXT, sf.RESUBMIT_DONE_TEXT, sf.STALE_TEXT):
            self.assertNotIn(absent, body)

    # ── 票: 保存後に通信例外 → 初回は不明ページ・再送も不明ページ・作成 1 件のまま ───────
    def test_landed_transport_first_and_resend_are_unknown_page_create_stays_1(self):
        n = self.get_nonce()
        with self._landed_transport():
            r1 = self.post_plain(nonce=n)                              # 実関数 _persist_with_number／_handle_submit_plain
        self._assert_unknown_page(r1)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)                       # 保存は成立している（着）
        entry = sf._nonce_results[n]
        self.assertEqual(entry.result[0], "unknown")                   # 予約は解放されず「不明」
        self.assertEqual(entry.result[2], "500001")
        self.assertEqual(len(self._warn(HELD_MSG)), 1)                 # WARNING 1 行
        self.assertIn("count=1", self._warn(HELD_MSG)[0].getMessage())
        self.assertEqual(self.notify_admin.await_count, 1)             # 既存の要確認通知のみ
        self.assertIn("unknown", self.notify_admin.await_args.args[0])
        self.notify_biz.assert_not_awaited()
        # 再送（同じ nonce・内容相違でも）→ 不明ページ・作成なし・新規予約なし・通知なし
        r2 = self.post_plain(nonce=n, creditor="別の債権者")
        self._assert_unknown_page(r2)
        r3 = self.post_plain(nonce=n)
        self._assert_unknown_page(r3)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertIs(sf._nonce_results[n], entry)
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(self.cap.text().count(DUP_UNKNOWN_MSG), 2)
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.assertEqual(self.notify_admin.await_count, 1)
        self.assertNotIn(n, self.cap.text())

    # ── 未着の通信例外・5xx も「不明」（保存 0 でも取り直さない＝fail-closed）────────────
    def test_unlanded_transport_and_5xx_are_held_unknown_no_retake(self):
        n1 = self.get_nonce()
        with self._unlanded_transport():
            r1 = self.post_plain(nonce=n1)
        self._assert_unknown_page(r1)
        self.assertEqual(len(self.fake.rows), 0)
        self._assert_unknown_page(self.post_plain(nonce=n1))
        self.assertEqual(len(self.fake.create_calls), 1)               # 取り直さない
        n2 = self.get_nonce()
        self.fake.create_fail_next = 1                                 # 既定の 5xx（GAIA_XX 500）
        self._assert_unknown_page(self.post_plain(nonce=n2))
        self._assert_unknown_page(self.post_plain(nonce=n2))
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual([sf._nonce_results[k].result[0] for k in (n1, n2)], ["unknown", "unknown"])
        self.assertEqual(len(self._warn(HELD_MSG)), 2)
        self.assertIn("count=2", self._warn(HELD_MSG)[1].getMessage())

    # ── 確定失敗（4xx）は従来どおり予約を解放し取り直し可 ─────────────────────────────
    def test_definite_failure_releases_and_retake_creates(self):
        n = self.get_nonce()
        self.fake.create_fail_next = 1
        self.fake.create_fail_error = _definite_failure()
        r1 = self.post_plain(nonce=n)
        self.assertEqual(r1.status_code, 500)                          # 一般エラー（不明ページではない）
        self.assertNotIn(sf.UNKNOWN_TEXT, r1.text)
        self.assertNotIn(n, sf._nonce_results)                         # 解放
        self.assertEqual(self._warn(HELD_MSG), [])
        r2 = self.post_plain(nonce=n)
        self.assertEqual(r2.status_code, 200)
        self.assertIn("受付番号：", r2.text)
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual(sf._nonce_results[n].result[0], "plain")
        # 再採番上限（duplicate が続く）も従来どおり 500・解放（確定）
        n3 = self.get_nonce()
        dup = sf.hub_kintone.KintoneError(400, "CB_VA01", "dup",
                                          errors={sf.UNIQUE_VIOLATION_ERROR_KEY: {"messages": ["x"]}})
        self.fake.create_fail_next = sf._NUMBER_ATTEMPTS
        self.fake.create_fail_error = dup
        r3 = self.post_plain(nonce=n3)
        self.assertEqual(r3.status_code, 500)
        self.assertNotIn(n3, sf._nonce_results)

    # ── 同時到着: A の書込結果が不明なら待っていた B・C も不明ページ（取り直さない・作成 1）──
    def test_concurrent_waiters_get_unknown_page_when_first_is_unknown(self):
        n = sf.issue_nonce(self.clock.t)
        gate = asyncio.Event()
        real_create = self.fake.create_record

        async def _create(app, fields):
            await gate.wait()
            await real_create(app, fields)                             # 保存は成立
            raise _transport()                                         # 応答が届かない

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            t_b = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            t_c = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            self.assertFalse(t_b.done() or t_c.done())
            gate.set()
            return await asyncio.gather(t_a, t_b, t_c)
        with patch.object(sf.hub_kintone, "create_record", _create):
            r_a, r_b, r_c = _run(_go())
        db.reset_for_tests()
        for r in (r_a, r_b, r_c):
            self._assert_unknown_page(r)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(len(_Counting.made), 1)                       # 取り直し（新しい予約）なし
        self.assertEqual(sf._nonce_results[n].result[0], "unknown")
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.assertEqual(self.cap.text().count(DUP_UNKNOWN_MSG), 2)
        self.assertEqual(self.notify_admin.await_count, 1)

    # ── 書込中のキャンセルも「不明」（予約を解放せず保持・再 POST は不明ページ）────────────
    def test_cancel_during_create_holds_unknown(self):
        n = sf.issue_nonce(self.clock.t)
        gate = asyncio.Event()

        async def _create(app, fields):
            self.fake.create_calls.append(dict(fields))
            await gate.wait()                                          # 書込を開始した後に戻らない
            return "1"

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            self.assertEqual(len(self.fake.create_calls), 1)
            t_a.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await t_a
            entry = sf._nonce_results[n]
            self.assertEqual(entry.result[0], "unknown")               # 解放されていない
            self.assertTrue(entry.event.is_set())
            return await sf.shindan_entry(_request(self._form(n)))
        with patch.object(sf.hub_kintone, "create_record", _create):
            r_b = _run(_go())
        db.reset_for_tests()
        self._assert_unknown_page(r_b)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.notify_admin.assert_not_awaited()                         # キャンセル経路は通知しない（増やさない）

    # ── nonce なしでも結果不明の初回応答は同じ不明ページ（記憶する対象が無いだけ）────────
    def test_unknown_without_nonce_is_unknown_page_too(self):
        with self._landed_transport():
            r = self.post_plain()
        self._assert_unknown_page(r)
        self.assertEqual(sf._nonce_results, {})
        self.assertEqual(len(self._warn(HELD_MSG)), 1)
        self.assertIn("count=0", self._warn(HELD_MSG)[0].getMessage())

    # ── 票: unknown エントリは満杯でも退避されない（処理中と同じ）・24 時間超だけ捨てる ────
    def test_unknown_entries_are_not_evicted_when_full(self):
        now = self.clock.t

        def _unknown(ts):
            e = sf._NonceEntry(ts, ())
            e.result = ("unknown", "", "500090", "A")
            return e
        self.assertFalse(sf._nonce_evictable(_unknown(now), now))
        self.assertFalse(sf._nonce_evictable(_unknown(now - sf.NONCE_TTL_SEC + 1), now))
        self.assertTrue(sf._nonce_evictable(_unknown(now - sf.NONCE_TTL_SEC - 1), now))   # 24 時間超だけ
        with patch.object(sf, "MAX_NONCE_RESULTS", 2):
            self.assertTrue(sf._nonce_put("u1", _unknown(now), now))
            self.assertTrue(sf._nonce_put("u2", _unknown(now), now))
            self.assertFalse(sf._nonce_put("x", sf._NonceEntry(now, ()), now))   # 不明で満杯 → 拒否
            self.assertEqual(list(sf._nonce_results), ["u1", "u2"])
            self.assertEqual(sf._nonce_marks, {})                      # 不明には印を残さない
            # 実入口: 不明 2 本で満杯 → 新規は固定の混雑ページ・create 0・WARNING 1（fix3 維持）
            r = _run(sf.shindan_entry(_request(self._form(sf.issue_nonce(now)))))
            db.reset_for_tests()
            self.assertEqual(r.status_code, 200)
            self.assertIn(sf.STORE_FULL_TEXT, r.body.decode("utf-8"))
            self.assertEqual(self.fake.create_calls, [])
            self.assertEqual(len(self._warn(FULL_MSG)), 1)
            # 24 時間超になれば捨てられて空きができる（印なし）
            self.clock.t += sf.NONCE_TTL_SEC + 1
            self.assertTrue(sf._nonce_put("y", sf._NonceEntry(self.clock.t, ()), self.clock.t))
            self.assertEqual(list(sf._nonce_results), ["u2", "y"])
            self.assertEqual(sf._nonce_marks, {})

    # ── 票: A 成功 → 別 nonce 確定 5000 件で A の結果退避 → A の nonce 再送 → 作成 0・送信済みページ ──
    def test_created_mark_survives_capacity_eviction_and_resend_is_done_page(self):
        n_a = self.get_nonce()
        r_a = self.post_plain(nonce=n_a)
        self.assertEqual(r_a.status_code, 200)
        num_a = _NUMBER_RE.search(r_a.text).group(1)
        now = self.clock.t
        for i in range(sf.MAX_NONCE_RESULTS):                          # 既定上限 5000 の確定済みを流す
            self.assertTrue(sf._nonce_put(f"other{i}", _done(now + i * 1e-3, str(i)), now))
        self.assertNotIn(n_a, sf._nonce_results)                       # A の結果本体は退避済み
        self.assertEqual(len(sf._nonce_results), sf.MAX_NONCE_RESULTS)
        self.assertTrue(sf._mark_valid(n_a, now))                      # 作成済みの印は残る
        self.assertNotIn(n_a, sf._nonce_marks)                         # 印は nonce のハッシュ（生値なし）
        made_before = len(_Counting.made)
        r_b = self.post_plain(nonce=n_a)
        self.assertEqual(r_b.status_code, 200)
        self.assertIn(sf.RESUBMIT_DONE_TEXT, r_b.text)                 # fix1 BF-03 と同じ「送信済み」ページ
        for absent in ("受付番号", num_a, sf.PHOTO_ROUTE, "診断結果", sf.UNKNOWN_TEXT, VALID["creditor"]):
            self.assertNotIn(absent, r_b.text)
        self.assertEqual(len(self.fake.create_calls), 1)               # 作成 0
        self.assertEqual(len(_Counting.made), made_before)             # 予約を取らない
        self.assertNotIn(n_a, sf._nonce_results)
        self.assertIn(DUP_MARKED_MSG, self.cap.text())
        self.assertEqual(self.notify_biz.await_count, 1)               # 受付通知も増えない
        r_c = self.post_plain(nonce=n_a, creditor="別の債権者")       # 内容相違でも同じ
        self.assertIn(sf.RESUBMIT_DONE_TEXT, r_c.text)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertNotIn(n_a, self.cap.text())

    # ── 票: 印の期限切れ後は stale（410）に入るので作成 0 ────────────────────────────
    def test_mark_expiry_falls_into_stale_410_create_0(self):
        n_a = self.get_nonce()
        self.post_plain(nonce=n_a)
        now = self.clock.t
        with patch.object(sf, "MAX_NONCE_RESULTS", 1):
            self.assertTrue(sf._nonce_put("other", _done(now), now))   # A を退避 → 印
        self.assertTrue(sf._mark_valid(n_a, now))
        self.clock.t += sf.NONCE_TTL_SEC + 1                           # 印も nonce も期限切れ
        self.assertFalse(sf._mark_valid(n_a, self.clock.t))
        self.assertEqual(sf.nonce_state(n_a, self.clock.t)[0], "stale")
        r = self.post_plain(nonce=n_a)
        self.assertEqual(r.status_code, 410)
        self.assertIn(sf.STALE_TEXT, r.text)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(sf._nonce_marks, {})                          # 期限切れの印は捨てられた

    # ── 印の集合: 上限 200,000・期限切れから捨て・上限到達時は最古から（既知の制約④）────────
    def test_marks_bounded_expired_first_then_oldest(self):
        now = self.clock.t
        self.assertEqual(sf.MAX_NONCE_MARKS, 200_000)
        with patch.object(sf, "MAX_NONCE_MARKS", 3):
            sf._mark_add("m1", now + 10, now)
            sf._mark_add("m2", now + 20, now)
            sf._mark_add("m3", now + 30, now)
            sf._mark_add("m4", now + 40, now)                          # 上限 → 最古 m1 を捨てる
            self.assertEqual(list(sf._nonce_marks), [sf._mark_key(k) for k in ("m2", "m3", "m4")])
            self.assertFalse(sf._mark_valid("m1", now))
            self.assertTrue(sf._mark_valid("m4", now))
            sf._mark_add("m5", now + 50, now + 25)                     # 期限切れ（m2）から捨てる
            self.assertEqual(list(sf._nonce_marks), [sf._mark_key(k) for k in ("m3", "m4", "m5")])
            self.assertFalse(sf._mark_valid("m3", now + 31))           # 期限切れは照合時にも捨てる
            self.assertEqual(len(sf._nonce_marks), 2)
        # 印の期限は退避したエントリの ts + 24 時間・linked も印・不明は印なし
        sf._nonce_marks.clear()
        with patch.object(sf, "MAX_NONCE_RESULTS", 1):
            linked = sf._NonceEntry(now - 100, ())
            linked.result = ("linked", "7", "", "A")
            self.assertTrue(sf._nonce_put("L", linked, now))
            self.assertTrue(sf._nonce_put("P", _done(now), now))       # L を退避 → 印（期限 now-100+24h）
            self.assertAlmostEqual(sf._nonce_marks[sf._mark_key("L")], now - 100 + sf.NONCE_TTL_SEC)
            self.assertTrue(sf._mark_valid("L", now + sf.NONCE_TTL_SEC - 101))
            self.assertFalse(sf._mark_valid("L", now + sf.NONCE_TTL_SEC - 99))
            self.assertFalse(sf._mark_valid("P", now))                 # 退避されていないものに印は無い

    # ── _nonce_reserve の契約: 印あり＝(_MARKED, False)・既存エントリが優先・別 nonce は無関係 ──
    def test_reserve_returns_marked_sentinel_only_without_entry(self):
        now = self.clock.t
        n, other = sf.issue_nonce(now), sf.issue_nonce(now)
        sf._mark_add(n, now + sf.NONCE_TTL_SEC, now)

        async def _go():
            e, owned = await sf._nonce_reserve(n, (), now)
            self.assertIs(e, sf._MARKED)
            self.assertFalse(owned)
            self.assertEqual(e.result[0], "marked")
            self.assertEqual(sf._nonce_results, {})                    # 予約しない
            e2, owned2 = await sf._nonce_reserve(other, (), now)       # 別 nonce は通常どおり予約
            self.assertTrue(owned2)
            self.assertIsNot(e2, sf._MARKED)
            e2.result = ("plain", "1", "500001", "A")
            sf._mark_add(other, now + sf.NONCE_TTL_SEC, now)           # 印とエントリが両方あれば entry 優先
            e3, owned3 = await sf._nonce_reserve(other, (), now)
            self.assertIs(e3, e2)
            self.assertFalse(owned3)
        _run(_go())
        self.assertEqual(_Counting.made, [sf._nonce_results[other]])

    # ── 不明ページの文言（sha256 pin・全文は報告に）────────────────────────────────
    def test_unknown_text_pin_and_page(self):
        self.assertEqual(hashlib.sha256(sf.UNKNOWN_TEXT.encode("utf-8")).hexdigest(), PIN_UNKNOWN)
        self.assertTrue(sf.UNKNOWN_TEXT.startswith("送信結果を確認できませんでした。"))
        self.assertIn("再送信はせず", sf.UNKNOWN_TEXT)
        self.assertIn("LINE の無料相談", sf.UNKNOWN_TEXT)
        self.assertNotIn("{", sf.UNKNOWN_TEXT)
        self.assertEqual(sf.UNKNOWN_TITLE, "送信結果を確認できませんでした")
        r = sf._unknown_page()
        self._assert_unknown_page(r)
        self.assertEqual(self.cap.records, [])                         # ページ生成自体はログなし

    # ── 既知の制約 4 点が docs に明記されている ─────────────────────────────────────
    def test_known_constraints_documented(self):
        with open(_DOCS, encoding="utf-8") as f:
            doc = f.read()
        for key in ("再起動", "単一 worker", "第三者", "200,000"):
            self.assertIn(key, doc)
        self.assertIn("docs/ops/shindan_form_dedup.md", open(sf.__file__, encoding="utf-8").read())

    def test_fix1_to_fix6_counts_unchanged(self):
        import test_form_dup_1 as fix1
        import test_form_dup_1_fix2 as fix2
        import test_form_dup_1_fix3 as fix3
        import test_form_dup_1_fix4 as fix4
        import test_form_dup_1_fix5 as fix5
        import test_form_dup_1_fix6 as fix6
        load = unittest.defaultTestLoader.loadTestsFromModule
        self.assertEqual(tuple(load(m).countTestCases() for m in (fix1, fix2, fix3, fix4, fix5, fix6)),
                         (37, 11, 7, 10, 8, 12))


if __name__ == "__main__":
    unittest.main()
