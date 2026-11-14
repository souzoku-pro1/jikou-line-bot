"""FORM-DUP-1-fix6（Codex R-FD1-fix5 BF-09・裁定変更）: nonce の状態は 2 つだけ。
 1. 有効（署名正・発行から 24 時間以内）: 経過時間にかかわらず fix2 の予約・待機・結果保持を
    そのまま適用（同一 nonce は必ず 1 本だけが作成し、他は同じ結果）。30 分の期限・expired 分岐・
    waited・nonce_state の "expired" は撤去。
 2. 24 時間超（署名正）: 固定ページ（410・作成 0・採番なし・予約なし・フォームへのリンク）。
 3. 署名不正・nonce なし: 従来どおり「なし」（新規）。
 4. 処理中エントリを退避しない・満杯時の新規拒否は維持。結果確定済みエントリの保持期限は 24 時間。
 fix1 37・fix2 11・fix3 7・fix4 10・fix5 8 件は件数不変（fix1 3 件・fix4 5 件・fix5 7 件は裁定変更で
 期待値を書き換え・削除・skip なし）。
"""

import asyncio
import hashlib
import inspect
import logging
import re
import unittest
from unittest.mock import patch

from test_form_dup_1 import _Base, _definite_failure  # noqa: E402
from test_form_dup_1_fix2 import _Counting, _request  # noqa: E402
from test_form_dup_1_fix3 import FULL_MSG  # noqa: E402
from test_form_dup_1_fix4 import _Clock, _NUMBER_RE  # noqa: E402
from test_shindan_line_link import VALID, _run  # noqa: E402

import time as _real_time  # noqa: E402

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402
from hub import shindan_link as sl  # noqa: E402

PIN_STALE = "5b9b078dac2f6dcb58a3b088996a8d2686c0c6220f16b136749683fd1c46dc1d"
STALE_MSG = "stale nonce (no write)"
H = 3600


class TestNonceValid24h(_Base):
    def setUp(self):
        super().setUp()
        _Counting.made = []
        self.clock = _Clock(_real_time.time())
        for p in (patch.object(sf, "_NonceEntry", _Counting),
                  patch.object(sf, "time", self.clock),
                  patch.object(sf, "_draw_number", side_effect=[f"{i:06d}" for i in range(400001, 400099)])):
            p.start()
            self.addCleanup(p.stop)

    def _form(self, nonce, **over):
        data = dict(VALID)
        data.update(over)
        data[sf.NONCE_FIELD] = nonce
        return data

    def _num(self, r) -> str:
        m = _NUMBER_RE.search(r.body.decode("utf-8"))
        self.assertIsNotNone(m)
        return m.group(1)

    def _warnings(self):
        return [r for r in self.cap.records if r.levelno == logging.WARNING and FULL_MSG in r.getMessage()]

    def _gated_create(self):
        gate = asyncio.Event()
        real_create = self.fake.create_record

        async def _gated(app, fields):
            await gate.wait()
            return await real_create(app, fields)
        return gate, patch.object(sf.hub_kintone, "create_record", _gated)

    def _concurrent_pair(self, advance: float):
        """発行 → 時計を advance 秒進める → 同一 nonce の D・E を同時 POST（D の create を Event で待機）。"""
        n = sf.issue_nonce(self.clock.t)
        self.clock.t += advance
        gate, p = self._gated_create()

        async def _go():
            t_d = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            t_e = asyncio.create_task(sf.shindan_entry(_request(self._form(n, creditor="別の債権者"))))
            await asyncio.sleep(0.05)
            self.assertFalse(t_d.done() or t_e.done())
            self.assertEqual(len(_Counting.made), 1)                  # 予約は 1 本だけ
            self.assertIsNone(sf._nonce_results[n].result)
            gate.set()
            return await asyncio.gather(t_d, t_e)
        with p:
            r_d, r_e = _run(_go())
        db.reset_for_tests()
        return r_d, r_e, n

    def _three_fail(self, advance: float):
        """A（create 待機）→ 時計 advance 秒 → 同一 nonce の B・C 待機 → A が create 失敗で解放。"""
        n = sf.issue_nonce(self.clock.t)
        gate, p = self._gated_create()

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            self.assertIsNone(sf._nonce_results[n].result)
            self.clock.t += advance
            self.assertEqual(sf.nonce_state(n, self.clock.t), ("ok", n))
            t_b = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            t_c = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            self.assertFalse(t_b.done() or t_c.done())
            self.assertEqual(len(_Counting.made), 1)
            self.fake.create_fail_next = 1
            self.fake.create_fail_error = _definite_failure()        # fix7: 取り直しは確定失敗のみ
            gate.set()
            return await asyncio.gather(t_a, t_b, t_c)
        with p:
            r_a, r_b, r_c = _run(_go())
        db.reset_for_tests()
        return r_a, r_b, r_c, n

    # ── 票: 同一 nonce の D・E 同時 POST（発行から 31 分後）→ 作成 1・同じ結果 ──────────
    def test_concurrent_same_nonce_31min_old_creates_once_same_result(self):
        r_d, r_e, n = self._concurrent_pair(31 * 60)
        self.assertEqual((r_d.status_code, r_e.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self._num(r_d), self._num(r_e))
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(sf._nonce_results[n].result[2], self._num(r_d))
        self.assertNotIn("別の債権者", r_e.body.decode("utf-8"))        # 1 回目の結果（相違は件数ログのみ）
        self.assertIn("duplicate post (no write) kind=plain", self.cap.text())
        self.assertNotIn(STALE_MSG, self.cap.text())
        self.assertNotIn("duplicate post pending", self.cap.text())
        self.assertNotIn(n, self.cap.text())

    # ── 票: 同一 nonce の D・E 同時 POST（発行から 23 時間後）→ 作成 1・同じ結果 ─────────
    def test_concurrent_same_nonce_23h_old_creates_once_same_result(self):
        r_d, r_e, n = self._concurrent_pair(23 * H)
        self.assertEqual((r_d.status_code, r_e.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self._num(r_d), self._num(r_e))
        self.assertEqual(len(_Counting.made), 1)
        self.assertNotIn(STALE_MSG, self.cap.text())
        self.assertEqual(self._warnings(), [])

    # ── 票: 順次再送（1 回目完了後に同じ nonce・31 分後・23 時間後）→ 作成 1・同じ結果 ─────
    def test_sequential_resend_after_completion_creates_once_same_result(self):
        n = sf.issue_nonce(self.clock.t)
        r1 = _run(sf.shindan_entry(_request(self._form(n))))
        db.reset_for_tests()
        first = sf._nonce_results[n]
        self.clock.t += 31 * 60
        r2 = _run(sf.shindan_entry(_request(self._form(n, creditor="別の債権者"))))
        db.reset_for_tests()
        self.clock.t += 23 * H - 31 * 60                              # 発行から 23 時間後
        r3 = _run(sf.shindan_entry(_request(self._form(n))))
        db.reset_for_tests()
        for r in (r1, r2, r3):
            self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self._num(r2), self._num(r1))
        self.assertEqual(self._num(r3), self._num(r1))
        self.assertIs(sf._nonce_results[n], first)                     # 捨てて新規にしない
        self.assertEqual(len(_Counting.made), 1)
        self.assertEqual(self.cap.text().count("duplicate post (no write) kind=plain"), 2)
        self.assertNotIn("別の債権者", r2.body.decode("utf-8"))
        self.assertNotIn(STALE_MSG, self.cap.text())

    # ── 票: 24 時間超は固定ページ・作成 0（採番なし・予約なし・フォームへのリンク・410）────
    def test_stale_nonce_is_fixed_page_create_0_no_reservation(self):
        n = sf.issue_nonce(self.clock.t)
        self.assertEqual(sf.nonce_state(n, self.clock.t + sf.NONCE_TTL_SEC - 1), ("ok", n))
        self.clock.t += sf.NONCE_TTL_SEC + 1
        self.assertEqual(sf.nonce_state(n, self.clock.t), ("stale", ""))
        r = _run(sf.shindan_entry(_request(self._form(n))))
        db.reset_for_tests()
        body = r.body.decode("utf-8")
        self.assertEqual(r.status_code, 410)
        self.assertIn(f"<h1>{sf.STALE_TITLE}</h1>", body)
        self.assertIn(sf.STALE_TEXT, body)
        self.assertIn(f'href="{sl.FORM_PATH}"', body)                 # フォームへのリンク
        self.assertIn(sf.STALE_LINK_TEXT, body)
        for absent in ("受付番号", sf.PHOTO_ROUTE, "診断結果", sf.LINKED_DONE_TEXT,
                       sf.PLAIN_PENDING_TEXT, sf.STORE_FULL_TEXT, sf.RESUBMIT_DONE_TEXT,
                       VALID["creditor"]):
            self.assertNotIn(absent, body)
        self.assertEqual(self.fake.create_calls, [])                  # 作成 0
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(sf._nonce_results, {})                       # 予約なし
        self.assertEqual(_Counting.made, [])
        self.assertEqual(self.cap.text().count(STALE_MSG), 1)        # INFO 1 行（定数のみ）
        self.assertNotIn(n, self.cap.text())
        self.assertEqual(self._warnings(), [])
        # 直後にフォームを開き直せば新しい nonce で従来どおり新規
        r_new = _run(sf.shindan_entry(_request(self._form(sf.issue_nonce(self.clock.t)))))
        db.reset_for_tests()
        self.assertEqual(r_new.status_code, 200)
        self._num(r_new)
        self.assertEqual(len(self.fake.create_calls), 1)

    # ── 票: 24 時間超の同時 2 本でも作成 0 ───────────────────────────────────────────
    def test_stale_concurrent_two_posts_create_0(self):
        n = sf.issue_nonce(self.clock.t)
        self.clock.t += sf.NONCE_TTL_SEC + 1

        async def _go():
            return await asyncio.gather(sf.shindan_entry(_request(self._form(n))),
                                        sf.shindan_entry(_request(self._form(n))))
        r1, r2 = _run(_go())
        db.reset_for_tests()
        for r in (r1, r2):
            self.assertEqual(r.status_code, 410)
            self.assertIn(sf.STALE_TEXT, r.body.decode("utf-8"))
        self.assertEqual(self.fake.create_calls, [])
        self.assertEqual(sf._nonce_results, {})
        self.assertEqual(_Counting.made, [])
        self.assertEqual(self.cap.text().count(STALE_MSG), 2)

    # ── 24 時間超はエントリ（確定済み・処理中）があっても固定ページ（照合も待機もしない）──────
    def test_stale_with_finished_or_inflight_entry_is_fixed_page(self):
        n1 = sf.issue_nonce(self.clock.t)
        r1 = _run(sf.shindan_entry(_request(self._form(n1))))
        db.reset_for_tests()
        first = sf._nonce_results[n1]
        n2 = sf.issue_nonce(self.clock.t)
        _run(sf._nonce_reserve(n2, (), self.clock.t))                 # 処理中のまま
        self.clock.t += sf.NONCE_TTL_SEC + 1
        with patch.object(sf, "_NONCE_WAIT_SEC", 5.0):
            t0 = _real_time.time()
            r1b = _run(sf.shindan_entry(_request(self._form(n1))))
            r2b = _run(sf.shindan_entry(_request(self._form(n2))))
            self.assertLess(_real_time.time() - t0, 2.0)              # 待機しない
        db.reset_for_tests()
        self.assertEqual(r1.status_code, 200)
        for r in (r1b, r2b):
            self.assertEqual(r.status_code, 410)
            self.assertIn(sf.STALE_TEXT, r.body.decode("utf-8"))
            self.assertNotIn(sf.PLAIN_PENDING_TEXT, r.body.decode("utf-8"))
        self.assertEqual(len(self.fake.create_calls), 1)              # 1 回目だけ
        self.assertIs(sf._nonce_results[n1], first)                    # 触らない（退避・保持期限で後始末）
        self.assertEqual(len(_Counting.made), 2)                      # 新規予約なし
        self.assertEqual(self.cap.text().count(STALE_MSG), 2)
        self.assertNotIn("duplicate post", self.cap.text())

    # ── 票: A 失敗→B・C 取り直しは 1 本だけ作成（31 分後）────────────────────────────
    def test_first_fails_waiters_retake_one_creates_31min(self):
        r_a, r_b, r_c, n = self._three_fail(31 * 60)
        self.assertEqual(r_a.status_code, 500)
        self.assertEqual((r_b.status_code, r_c.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 2)              # A 失敗 + 取り直し 1 本
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self._num(r_b), self._num(r_c))
        self.assertEqual(len(_Counting.made), 2)
        self.assertIs(sf._nonce_results[n], _Counting.made[1])
        self.assertNotIn(STALE_MSG, self.cap.text())

    # ── 票: A 失敗→B・C 取り直しは 1 本だけ作成（23 時間 50 分後）────────────────────
    def test_first_fails_waiters_retake_one_creates_near_24h(self):
        r_a, r_b, r_c, n = self._three_fail(sf.NONCE_TTL_SEC - 600)
        self.assertEqual(r_a.status_code, 500)
        self.assertEqual((r_b.status_code, r_c.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self._num(r_b), self._num(r_c))
        self.assertEqual(len(_Counting.made), 2)
        self.assertNotIn(STALE_MSG, self.cap.text())

    # ── nonce の状態は ok／stale＋none だけ・expired 分岐と waited は無い ─────────────
    def test_nonce_state_two_states_plus_none_and_no_expired_branch(self):
        now = self.clock.t
        n = sf.issue_nonce(now)
        exp, rnd, sig = n.split(".")
        self.assertEqual(int(exp) - int(now), 24 * H)
        self.assertEqual(sf.nonce_state(n, now), ("ok", n))
        self.assertEqual(sf.nonce_state(n, now + 24 * H - 1), ("ok", n))
        self.assertEqual(sf.nonce_state(n, now + 24 * H), ("stale", ""))
        self.assertEqual(sf.nonce_state(n, now + 10 * 24 * H), ("stale", ""))
        for bad in ("", "x", f"{exp}.{rnd}.{'0' * 32}", f"{int(exp) + 9999}.{rnd}.{sig}",
                    f"{exp}.{'f' * 32}.{sig}", n + "x"):
            self.assertEqual(sf.nonce_state(bad, now), ("none", ""))
            self.assertEqual(sf.nonce_state(bad, now + 10 * 24 * H), ("none", ""))  # 24 時間超でも署名不正は「なし」
        self.assertEqual(sf.verify_nonce(n, now + 24 * H - 1), n)   # fix1 の契約は不変
        self.assertEqual(sf.verify_nonce(n, now + 24 * H), "")
        self.assertFalse(hasattr(sf, "_released_page"))
        self.assertNotIn("allow_new", inspect.signature(sf._nonce_reserve).parameters)
        src = inspect.getsource(sf._submit_with_nonce) + inspect.getsource(sf.nonce_state)
        self.assertIsNone(re.search(r"\bwaited\b|\bexpired\b", src))
        # 署名不正は同一送信と見なさず記憶しない（従来どおり新規）
        bad = f"{exp}.{rnd}.{'0' * 32}"
        _run(sf.shindan_entry(_request(self._form(bad))))
        _run(sf.shindan_entry(_request(self._form(bad))))
        db.reset_for_tests()
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertEqual(sf._nonce_results, {})
        self.assertNotIn(STALE_MSG, self.cap.text())

    # ── 票 4: 結果確定済みの保持期限は 24 時間・処理中は退避も破棄もしない・満杯の拒否は維持 ──
    def test_finished_retention_24h_inflight_kept_and_full_store_rejects(self):
        now = self.clock.t
        n = sf.issue_nonce(now)

        async def _go():
            e1, owned1 = await sf._nonce_reserve(n, ("a",), now)
            self.assertTrue(owned1)
            e1.ts = now - 100 * sf.NONCE_TTL_SEC                      # 処理中はどれだけ古くても同じ entry
            e2, owned2 = await sf._nonce_reserve(n, ("b",), now)
            self.assertIs(e2, e1)
            self.assertFalse(owned2)
            e1.result = ("plain", "1", "400001", "A")
            e1.ts = now - sf.NONCE_TTL_SEC + 1                        # 確定済み・24 時間以内 → 保持
            e3, _ = await sf._nonce_reserve(n, ("c",), now)
            self.assertIs(e3, e1)
            e1.ts = now - sf.NONCE_TTL_SEC - 1                        # 確定済み・24 時間超 → 捨てて新規予約
            e4, owned4 = await sf._nonce_reserve(n, ("d",), now)
            self.assertTrue(owned4)
            self.assertIsNot(e4, e1)
            # 満杯（全部処理中）の新規拒否は維持（fix3・fix4）
            with patch.object(sf, "MAX_NONCE_RESULTS", 1):
                e5, owned5 = await sf._nonce_reserve(sf.issue_nonce(now), ("e",), now)
                self.assertIsNone(e5)
                self.assertFalse(owned5)
            self.assertFalse(sf._nonce_evictable(sf._NonceEntry(now - 100 * sf.NONCE_TTL_SEC, ()), now))
            done = sf._NonceEntry(now, ())
            done.result = ("plain", "2", "400002", "A")
            self.assertTrue(sf._nonce_evictable(done, now))
        _run(_go())

    # ── 24 時間超ページの文言（sha256 pin・全文は報告に）──────────────────────────────
    def test_stale_text_pin_and_page(self):
        self.assertEqual(hashlib.sha256(sf.STALE_TEXT.encode("utf-8")).hexdigest(), PIN_STALE)
        self.assertTrue(sf.STALE_TEXT.startswith("入力から時間が経ったため、送信できませんでした。"))
        self.assertIn("もう一度フォームを開いて入力してください。", sf.STALE_TEXT)
        self.assertNotIn("{", sf.STALE_TEXT)
        self.assertEqual(sf.STALE_TITLE, "送信できませんでした")
        self.assertEqual(sf.NONCE_TTL_SEC, 24 * H)
        r = sf._stale_page()
        body = r.body.decode("utf-8")
        self.assertEqual(r.status_code, 410)
        self.assertIn(f"<h1>{sf.STALE_TITLE}</h1>", body)
        self.assertIn(f"<p>{sf.STALE_TEXT}</p>", body)
        self.assertIn(f'<a class="btn" href="{sl.FORM_PATH}">{sf.STALE_LINK_TEXT}</a>', body)
        self.assertNotIn("受付番号", body)
        self.assertNotIn(sf.PHOTO_ROUTE, body)
        self.assertEqual(self.cap.text().count(STALE_MSG), 1)
        self.assertEqual(self._warnings(), [])

    def test_fix1_37_fix2_11_fix3_7_fix4_10_fix5_8_unchanged(self):
        import test_form_dup_1 as fix1
        import test_form_dup_1_fix2 as fix2
        import test_form_dup_1_fix3 as fix3
        import test_form_dup_1_fix4 as fix4
        import test_form_dup_1_fix5 as fix5
        load = unittest.defaultTestLoader.loadTestsFromModule
        self.assertEqual(tuple(load(m).countTestCases() for m in (fix1, fix2, fix3, fix4, fix5)),
                         (37, 11, 7, 10, 8))


if __name__ == "__main__":
    unittest.main()
