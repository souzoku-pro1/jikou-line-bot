"""FORM-DUP-1-fix4（Codex R-FD1-fix3 BF-07）:
 1. 処理中（予約済み・結果未確定）のエントリは期限にかかわらず退避しない（退避は結果確定済みのみ）。
 2. 署名は正しいが期限切れの nonce は「なし」に変換しない。nonce_state が "expired" を区別し、
    その nonce に処理中または結果確定のエントリがあれば従来の待機または同じ結果を返し、無ければ
    初めて「nonce なし」として扱う（新規予約はしない）。
 3. 満杯時の固定ページの文言を「ただいま混み合っています…」に訂正（sha256 pin）。
 既存 test_form_dup_1 37 件・fix2 11 件・fix3 7 件は件数不変。
"""

import asyncio
import hashlib
import logging
import re
import time as _real_time
import unittest
from unittest.mock import patch

from test_form_dup_1 import _Base  # noqa: E402
from test_form_dup_1_fix2 import _Counting, _request  # noqa: E402
from test_form_dup_1_fix3 import FULL_MSG, _done  # noqa: E402
from test_shindan_line_link import VALID, _run  # noqa: E402

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402
from hub import shindan_link as sl  # noqa: E402

PIN_STORE_FULL = "0bb03ff9cde43a609b525d79538b04fcdfdc5c91472e37af99d66523d6b0ac11"
_NUMBER_RE = re.compile(r"受付番号：([0-9]{6})")


class _Clock:
    """shindan_form.time の差し替え: time() だけ進められる（他は実 time に委譲）。"""

    def __init__(self, t: float):
        self.t = t

    def time(self) -> float:
        return self.t

    def __getattr__(self, name):
        return getattr(_real_time, name)


class TestExpiredNonceAndInflightRetention(_Base):
    def setUp(self):
        super().setUp()
        _Counting.made = []
        self.clock = _Clock(_real_time.time())
        for p in (patch.object(sf, "_NonceEntry", _Counting),
                  patch.object(sf, "time", self.clock),
                  patch.object(sf, "_draw_number", side_effect=[f"{i:06d}" for i in range(200001, 200099)])):
            p.start()
            self.addCleanup(p.stop)

    def _form(self, nonce, **over):
        data = dict(VALID)
        data.update(over)
        data[sf.NONCE_FIELD] = nonce
        return data

    def _warnings(self):
        return [r for r in self.cap.records if r.levelno == logging.WARNING and FULL_MSG in r.getMessage()]

    def _gate_lookup(self):
        gate = asyncio.Event()
        real_lookup = sl.lookup

        async def _gated(token, *a, **kw):
            await gate.wait()
            return await real_lookup(token, *a, **kw)
        return gate, patch.object(sl, "lookup", _gated)

    def _num(self, r) -> str:
        m = _NUMBER_RE.search(r.body.decode("utf-8"))
        self.assertIsNotNone(m)
        return m.group(1)

    # ── 票 2: 期限切れ（署名正）の nonce・処理中 A がいる間に B → 作成 1・同じ結果 ──────
    def test_expired_nonce_while_first_inflight_second_waits_and_gets_same_result(self):
        n = sf.issue_nonce(self.clock.t)                            # 有効 30 分
        gate, p = self._gate_lookup()

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n), token="nosuchtoken")))
            await asyncio.sleep(0.05)
            entry_a = sf._nonce_results[n]
            self.assertIsNone(entry_a.result)
            self.clock.t += sf.NONCE_TTL_SEC + 1                      # A の処理中に nonce が期限切れ
            self.assertEqual(sf.nonce_state(n, self.clock.t)[0], "expired")
            t_b = asyncio.create_task(sf.shindan_entry(_request(self._form(n), token="nosuchtoken")))
            await asyncio.sleep(0.05)
            self.assertFalse(t_b.done())                              # B は A の完了を待つ
            self.assertIs(sf._nonce_results[n], entry_a)              # 新規予約・上書きなし
            gate.set()
            r_a, r_b = await asyncio.gather(t_a, t_b)
            return r_a, r_b, entry_a
        with p:
            r_a, r_b, entry_a = _run(_go())
        db.reset_for_tests()
        self.assertEqual((r_a.status_code, r_b.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self._num(r_b), self._num(r_a))
        self.assertIs(sf._nonce_results[n], entry_a)
        self.assertEqual(len(_Counting.made), 1)                     # B は予約を作らない
        self.assertNotIn("duplicate post pending", self.cap.text())
        self.assertEqual(self._warnings(), [])

    # ── 票 2: 期限切れで結果確定済み → 同じ結果 ──────────────────────────────────
    def test_expired_nonce_with_finished_result_returns_same_result(self):
        n = sf.issue_nonce(self.clock.t)
        r1 = _run(sf.shindan_entry(_request(self._form(n))))
        db.reset_for_tests()
        first = sf._nonce_results[n]
        self.clock.t += sf.NONCE_TTL_SEC + 1
        r2 = _run(sf.shindan_entry(_request(self._form(n, creditor="別の債権者"))))
        db.reset_for_tests()
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(self._num(r2), self._num(r1))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIs(sf._nonce_results[n], first)                    # 捨てて新規にしない
        self.assertEqual(len(_Counting.made), 1)
        self.assertIn("duplicate post (no write) kind=plain", self.cap.text())

    # ── 票 2: 期限切れでエントリ無し → 従来どおり新規（記憶しない）─────────────────
    def test_expired_nonce_without_entry_is_new_and_not_remembered(self):
        n = sf.issue_nonce(self.clock.t)
        self.clock.t += sf.NONCE_TTL_SEC + 1
        r = _run(sf.shindan_entry(_request(self._form(n))))
        db.reset_for_tests()
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self._num(r)
        self.assertNotIn(n, sf._nonce_results)                        # nonce なし扱い＝予約しない
        self.assertEqual(_Counting.made, [])
        # 使用済み token＋期限切れ nonce（エントリ無し）も従来どおり固定の送信済みページ
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))
        n2 = sf.issue_nonce(self.clock.t)
        self.clock.t += sf.NONCE_TTL_SEC + 1
        self.assert_done_page_only(self.post_linked_n(token, n2))
        self.assertEqual(len(self.fake.create_calls), 2)

    # ── 票 1: 時計を 1,801 秒進めて別 nonce を登録しても処理中 A は退避されない ───────
    def test_clock_advance_does_not_evict_inflight_when_other_nonces_register(self):
        n_a = sf.issue_nonce(self.clock.t)
        gate, p = self._gate_lookup()

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n_a), token="nosuchtoken")))
            await asyncio.sleep(0.05)
            entry_a = sf._nonce_results[n_a]
            self.clock.t += sf.NONCE_TTL_SEC + 1                      # 1,801 秒後
            n_c, n_d = sf.issue_nonce(self.clock.t), sf.issue_nonce(self.clock.t)
            r_c = await sf.shindan_entry(_request(self._form(n_c)))  # 空き 1 → 登録・作成
            self.assertIs(sf._nonce_results.get(n_a), entry_a)
            self.assertEqual(list(sf._nonce_results), [n_a, n_c])
            r_d = await sf.shindan_entry(_request(self._form(n_d)))  # 満杯（A 処理中・C 確定）→ C を退避して登録
            self.assertIs(sf._nonce_results.get(n_a), entry_a)       # A は期限切れでも退避されない
            self.assertEqual(list(sf._nonce_results), [n_a, n_d])
            gate.set()
            r_a = await t_a
            r_b = await sf.shindan_entry(_request(self._form(n_a), token="nosuchtoken"))  # 期限切れ nonce の B
            return r_a, r_b, r_c, r_d, entry_a
        with patch.object(sf, "MAX_NONCE_RESULTS", 2), p:
            r_a, r_b, r_c, r_d, entry_a = _run(_go())
        db.reset_for_tests()
        for r in (r_a, r_b, r_c, r_d):
            self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 3)             # A・C・D（B は 0）
        self.assertEqual(self._num(r_b), self._num(r_a))
        self.assertNotEqual(self._num(r_c), self._num(r_a))
        self.assertIs(sf._nonce_results[n_a], entry_a)
        self.assertEqual(self._warnings(), [])

    def test_inflight_full_store_rejects_with_corrected_text_even_after_clock_advance(self):
        n_a1, n_a2 = sf.issue_nonce(self.clock.t), sf.issue_nonce(self.clock.t)
        gate, p = self._gate_lookup()

        async def _go():
            t1 = asyncio.create_task(sf.shindan_entry(_request(self._form(n_a1), token="nosuchtoken")))
            t2 = asyncio.create_task(sf.shindan_entry(_request(self._form(n_a2), token="nosuchtoken")))
            await asyncio.sleep(0.05)
            self.clock.t += sf.NONCE_TTL_SEC + 1                      # 2 本とも「期限切れの処理中」
            r_c = await sf.shindan_entry(_request(self._form(sf.issue_nonce(self.clock.t))))
            self.assertEqual(len(sf._nonce_results), 2)              # 退避されていない
            self.assertEqual(len(self.fake.create_calls), 0)
            gate.set()
            r1, r2 = await asyncio.gather(t1, t2)
            return r_c, r1, r2
        with patch.object(sf, "MAX_NONCE_RESULTS", 2), p:
            r_c, r1, r2 = _run(_go())
        db.reset_for_tests()
        body = r_c.body.decode("utf-8")
        self.assertEqual(r_c.status_code, 200)
        self.assertIn(sf.STORE_FULL_TEXT, body)
        self.assertIn(sf.STORE_FULL_TITLE, body)
        self.assertNotIn(sf.PLAIN_PENDING_TEXT, body)
        self.assertNotIn("受付番号", body)
        self.assertNotIn(sf.PHOTO_ROUTE, body)
        self.assertEqual(len(self._warnings()), 1)
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 2)

    # ── 署名不正は従来どおり「なし」────────────────────────────────────────────
    def test_bad_signature_is_none_as_before(self):
        n = sf.issue_nonce(self.clock.t)
        exp, rnd, sig = n.split(".")
        bad = f"{exp}.{rnd}.{'0' * 32}"
        self.assertEqual(sf.nonce_state(bad, self.clock.t), ("none", ""))
        self.assertEqual(sf.nonce_state(f"{int(exp) + 9999}.{rnd}.{sig}", self.clock.t), ("none", ""))
        self.assertEqual(sf.nonce_state("tampered.value", self.clock.t), ("none", ""))
        self.assertEqual(sf.nonce_state("", self.clock.t), ("none", ""))
        _run(sf.shindan_entry(_request(self._form(bad))))
        _run(sf.shindan_entry(_request(self._form(bad))))
        db.reset_for_tests()
        self.assertEqual(len(self.fake.create_calls), 2)             # 同一送信とは見なさない
        self.assertEqual(sf._nonce_results, {})

    def test_nonce_state_and_verify_nonce_contract(self):
        now = self.clock.t
        n = sf.issue_nonce(now)
        self.assertEqual(sf.nonce_state(n, now), ("ok", n))
        self.assertEqual(sf.nonce_state(" " + n + " ", now), ("ok", n))
        self.assertEqual(sf.nonce_state(n, now + sf.NONCE_TTL_SEC - 1), ("ok", n))
        self.assertEqual(sf.nonce_state(n, now + sf.NONCE_TTL_SEC), ("expired", n))
        self.assertEqual(sf.nonce_state(n, now + 10 * sf.NONCE_TTL_SEC), ("expired", n))
        # verify_nonce の契約（fix1）は不変: 期限切れは ""
        self.assertEqual(sf.verify_nonce(n, now), n)
        self.assertEqual(sf.verify_nonce(n, now + sf.NONCE_TTL_SEC), "")
        self.assertEqual(sf.verify_nonce(n + "x", now), "")
        # 期限切れ nonce は _nonce_reserve(allow_new=False) で新規予約しない
        async def _go():
            e, owned = await sf._nonce_reserve(n, (), now, allow_new=False)
            self.assertIsNone(e)
            self.assertFalse(owned)
            self.assertEqual(sf._nonce_results, {})
            e1, owned1 = await sf._nonce_reserve(n, (), now)
            self.assertTrue(owned1)
            e1.ts -= 10 * sf.NONCE_TTL_SEC                           # どれだけ古くても
            e2, owned2 = await sf._nonce_reserve(n, (), now, allow_new=False)
            self.assertIs(e2, e1)
            self.assertFalse(owned2)
            e1.result = ("plain", "1", "000001", "A")
            e3, owned3 = await sf._nonce_reserve(n, (), now, allow_new=False)
            self.assertIs(e3, e1)                                    # 期限切れの結果も捨てない
        _run(_go())

    # ── 票 1: 処理中は期限にかかわらず退避しない（単体）──────────────────────────
    def test_inflight_is_never_evictable_regardless_of_age(self):
        now = self.clock.t
        old_inflight = sf._NonceEntry(now - 100 * sf.NONCE_TTL_SEC, ())
        self.assertFalse(sf._nonce_evictable(old_inflight, now))
        self.assertTrue(sf._nonce_evictable(_done(now - 100 * sf.NONCE_TTL_SEC), now))
        self.assertTrue(sf._nonce_evictable(_done(now), now))
        with patch.object(sf, "MAX_NONCE_RESULTS", 2):
            self.assertTrue(sf._nonce_put("a", old_inflight, now))
            self.assertTrue(sf._nonce_put("b", sf._NonceEntry(now - 100 * sf.NONCE_TTL_SEC, ()), now))
            self.assertFalse(sf._nonce_put("c", sf._NonceEntry(now, ()), now))   # 全部が期限切れの処理中でも退避しない
            self.assertEqual(list(sf._nonce_results), ["a", "b"])

    # ── 票 3: 満杯時ページの文言（sha256 pin・全文は報告に）────────────────────────
    def test_store_full_text_pin_and_page(self):
        self.assertEqual(hashlib.sha256(sf.STORE_FULL_TEXT.encode("utf-8")).hexdigest(), PIN_STORE_FULL)
        self.assertTrue(sf.STORE_FULL_TEXT.startswith("ただいま混み合っています。"))
        self.assertIn("LINE の無料相談", sf.STORE_FULL_TEXT)
        self.assertNotIn("受け付けています", sf.STORE_FULL_TEXT)
        self.assertNotIn("{", sf.STORE_FULL_TEXT)
        self.assertEqual(sf.STORE_FULL_TITLE, "ただいま混み合っています")
        r = sf._store_full_page()
        body = r.body.decode("utf-8")
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.STORE_FULL_TEXT, body)
        self.assertIn(f"<h1>{sf.STORE_FULL_TITLE}</h1>", body)
        self.assertNotIn(sf.PLAIN_PENDING_TEXT, body)
        self.assertNotIn("受付番号", body)
        self.assertEqual(len(self._warnings()), 1)

    def test_fix1_37_fix2_11_fix3_7_unchanged(self):
        import test_form_dup_1 as fix1
        import test_form_dup_1_fix2 as fix2
        import test_form_dup_1_fix3 as fix3
        load = unittest.defaultTestLoader.loadTestsFromModule
        self.assertEqual((load(fix1).countTestCases(), load(fix2).countTestCases(),
                          load(fix3).countTestCases()), (37, 11, 7))


if __name__ == "__main__":
    unittest.main()
