"""FORM-DUP-1-fix3（Codex R-FD1-fix2 BF-06）: nonce 表の上限到達時の退避から「処理中」
（予約済み・結果未確定・期限内）のエントリを除外する。

固定する内容:
 - 上限到達中に A が token 照合で待機中 → 別 nonce の登録が続いても A は追い出されない
   （A は作成 1 回・同一 nonce の B は A の結果＝同じ受付番号を返す・作成なし）。
 - 退避できるのは「結果確定済み」だけ（最古から）。fix4（BF-07）で「期限切れの処理中」も退避しない
   に訂正（処理中は期限にかかわらず残す）。
 - 処理中で埋まって安全に空きが作れないときは新規予約を拒否し、固定の受付中ページで終了
   （作成 0・採番なし・WARNING 1 行で件数のみ・予約エントリを作らない）。
 - 処理中エントリの個数も上限の内数。
 - 既存 test_form_dup_1 37 件・test_form_dup_1_fix2 11 件は不変。
"""

import asyncio
import logging
import re
import unittest
from unittest.mock import patch

from test_form_dup_1 import _Base  # noqa: E402
from test_form_dup_1_fix2 import _Counting, _request  # noqa: E402
from test_shindan_line_link import VALID, _run  # noqa: E402

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402
from hub import shindan_link as sl  # noqa: E402

_NUMBER_RE = re.compile(r"受付番号：([0-9]{6})")
FULL_MSG = "nonce store full of in-flight entries (no write)"


def _done(ts: float, rid: str = "1") -> sf._NonceEntry:
    e = sf._NonceEntry(ts, ())
    e.result = ("plain", rid, "000001", "A")
    return e


class TestEvictionExcludesInflight(_Base):
    def setUp(self):
        super().setUp()
        _Counting.made = []
        for p in (patch.object(sf, "_NonceEntry", _Counting),
                  patch.object(sf, "_draw_number", side_effect=[f"{i:06d}" for i in range(100001, 100099)])):
            p.start()
            self.addCleanup(p.stop)

    def _form(self, nonce, **over):
        data = dict(VALID)
        data.update(over)
        data[sf.NONCE_FIELD] = nonce
        return data

    def _warnings(self):
        return [r for r in self.cap.records if r.levelno == logging.WARNING and FULL_MSG in r.getMessage()]

    # ── 票: 上限到達中に A が照合待機中 → 別 nonce の登録が続く → A が追い出されない ──
    def test_inflight_survives_eviction_and_same_nonce_returns_its_result(self):
        with patch.object(sf, "MAX_NONCE_RESULTS", 3):
            n_old1, n_old2 = self.get_nonce(), self.get_nonce()
            self.post_plain(nonce=n_old1)                           # 結果確定済み 2 件で表を埋める
            self.post_plain(nonce=n_old2)
            self.assertEqual(len(self.fake.create_calls), 2)
            _Counting.made = []
            n_a = sf.issue_nonce()
            n_cs = [sf.issue_nonce() for _ in range(3)]
            gate = asyncio.Event()
            real_lookup = sl.lookup

            async def _gated_lookup(token, *a, **kw):
                await gate.wait()
                return await real_lookup(token, *a, **kw)

            async def _go():
                t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n_a), token="nosuchtoken")))
                await asyncio.sleep(0.05)
                entry_a = sf._nonce_results[n_a]
                self.assertIsNone(entry_a.result)                    # A は照合待機中（処理中）
                self.assertEqual(len(sf._nonce_results), 3)          # 上限到達
                bodies_c = []
                for n_c in n_cs:                                     # 別 nonce の登録が続く
                    r = await sf.shindan_entry(_request(self._form(n_c)))
                    self.assertEqual(r.status_code, 200)
                    bodies_c.append(r.body.decode("utf-8"))
                    self.assertIs(sf._nonce_results.get(n_a), entry_a)   # A は追い出されない
                    self.assertEqual(len(sf._nonce_results), 3)
                self.assertNotIn(n_old1, sf._nonce_results)          # 退避されたのは結果確定済み
                self.assertNotIn(n_old2, sf._nonce_results)
                self.assertNotIn(n_cs[0], sf._nonce_results)         # 3 本目の C は最古の C を退避
                gate.set()
                r_a = await t_a
                r_b = await sf.shindan_entry(_request(self._form(n_a), token="nosuchtoken"))
                return r_a, r_b, bodies_c, entry_a
            with patch.object(sl, "lookup", _gated_lookup):
                r_a, r_b, bodies_c, entry_a = _run(_go())
            db.reset_for_tests()
        self.assertEqual((r_a.status_code, r_b.status_code), (200, 200))
        body_a, body_b = r_a.body.decode("utf-8"), r_b.body.decode("utf-8")
        num_a = _NUMBER_RE.search(body_a).group(1)
        self.assertEqual(_NUMBER_RE.search(body_b).group(1), num_a)  # B は A の結果
        for b in bodies_c:
            self.assertNotEqual(_NUMBER_RE.search(b).group(1), num_a)
        self.assertEqual(len(self.fake.create_calls), 2 + 1 + 3)     # A 1 回・B 0 回
        self.assertIs(sf._nonce_results[n_a], entry_a)
        self.assertEqual(entry_a.result[2], num_a)
        self.assertEqual(len(_Counting.made), 4)                     # A + C×3・B は予約を作らない
        self.assertEqual(self._warnings(), [])

    # ── 票: 処理中で埋まって空きが作れない → 新規予約拒否・固定ページ・作成 0 ──────
    def test_store_full_of_inflight_rejects_new_reservation_with_fixed_page(self):
        n_a1, n_a2, n_c = sf.issue_nonce(), sf.issue_nonce(), sf.issue_nonce()
        gate = asyncio.Event()
        real_lookup = sl.lookup

        async def _gated_lookup(token, *a, **kw):
            await gate.wait()
            return await real_lookup(token, *a, **kw)

        async def _go():
            t1 = asyncio.create_task(sf.shindan_entry(_request(self._form(n_a1), token="nosuchtoken")))
            t2 = asyncio.create_task(sf.shindan_entry(_request(self._form(n_a2), token="nosuchtoken")))
            await asyncio.sleep(0.05)
            self.assertEqual(len(sf._nonce_results), 2)              # 処理中 2 件で上限
            self.assertEqual(len(self.fake.create_calls), 0)
            r_c = await sf.shindan_entry(_request(self._form(n_c)))  # k なし・待たずに返る
            self.assertEqual(len(self.fake.create_calls), 0)         # 作成 0・採番なし
            self.assertNotIn(n_c, sf._nonce_results)                 # 予約エントリを作らない
            self.assertEqual(len(_Counting.made), 2)
            gate.set()
            r1, r2 = await asyncio.gather(t1, t2)
            return r_c, r1, r2
        with patch.object(sf, "MAX_NONCE_RESULTS", 2), patch.object(sl, "lookup", _gated_lookup):
            r_c, r1, r2 = _run(_go())
        db.reset_for_tests()
        self.assertEqual(r_c.status_code, 200)
        body_c = r_c.body.decode("utf-8")
        self.assertIn(sf.STORE_FULL_TEXT, body_c)                     # 固定ページ（fix4 BF-07 で文言訂正）
        self.assertNotIn(sf.PLAIN_PENDING_TEXT, body_c)
        self.assertNotIn("受付番号", body_c)
        self.assertNotIn(sf.PHOTO_ROUTE, body_c)
        self.assertNotIn(n_c, body_c)
        warns = self._warnings()
        self.assertEqual(len(warns), 1)                              # WARNING 1 行
        self.assertIn("count=2", warns[0].getMessage())              # 件数のみ
        for n in (n_a1, n_a2, n_c):
            self.assertNotIn(n, self.cap.text())
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 2)             # A1・A2 は影響を受けず作成
        self.assertNotIn("duplicate post pending", self.cap.text())  # 待機ページとは別経路

    # ── 退避の対象: 結果確定済みだけ・最古から（fix4 BF-07: 期限切れの処理中も残す）────
    def test_eviction_takes_finished_or_expired_only_oldest_first(self):
        now = sf.time.time()
        fresh = sf._NonceEntry(now, ())                              # 処理中・期限内
        expired_inflight = sf._NonceEntry(now - sf.NONCE_TTL_SEC - 1, ())   # 処理中・期限切れ
        done_old, done_new = _done(now - 100), _done(now)
        with patch.object(sf, "MAX_NONCE_RESULTS", 4):
            for k, e in (("done_old", done_old), ("fresh", fresh),
                         ("expired", expired_inflight), ("done_new", done_new)):
                self.assertTrue(sf._nonce_put(k, e, now))
            self.assertTrue(sf._nonce_put("x1", sf._NonceEntry(now, ()), now))
            self.assertEqual(list(sf._nonce_results), ["fresh", "expired", "done_new", "x1"])
            self.assertTrue(sf._nonce_put("x2", sf._NonceEntry(now, ()), now))
            self.assertEqual(list(sf._nonce_results), ["fresh", "expired", "x1", "x2"])  # 期限切れでも処理中は残す
            self.assertFalse(sf._nonce_put("x3", sf._NonceEntry(now, ()), now))  # 全部処理中 → 拒否
            self.assertEqual(list(sf._nonce_results), ["fresh", "expired", "x1", "x2"])
            self.assertIs(sf._nonce_results["fresh"], fresh)
            self.assertIs(sf._nonce_results["expired"], expired_inflight)
            self.assertEqual(len(sf._nonce_results), 4)

    def test_inflight_entries_count_toward_limit_in_reserve(self):
        async def _go():
            now = sf.time.time()
            n1, n2, n3 = sf.issue_nonce(), sf.issue_nonce(), sf.issue_nonce()
            e1, owned1 = await sf._nonce_reserve(n1, (), now)        # 処理中 1
            self.assertTrue(owned1)
            sf._nonce_put("done", _done(now), now)                   # 結果確定済み 1 → 上限 2
            e2, owned2 = await sf._nonce_reserve(n2, (), now)        # 確定済みを退避して予約
            self.assertTrue(owned2)
            self.assertEqual(list(sf._nonce_results), [n1, n2])
            made_before = len(_Counting.made)
            e3, owned3 = await sf._nonce_reserve(n3, (), now)        # 処理中 2 件＝上限 → 拒否
            self.assertIsNone(e3)
            self.assertFalse(owned3)
            self.assertEqual(list(sf._nonce_results), [n1, n2])      # 既存の予約は無傷
            self.assertIs(sf._nonce_results[n1], e1)
            self.assertIs(sf._nonce_results[n2], e2)
            self.assertEqual(len(_Counting.made), made_before)       # 拒否ではエントリを作らない
            e1.result = ("plain", "1", "000001", "A")                # 1 件確定 → 次は入れる
            sf._nonce_release(n1, e1)
            e3b, owned3b = await sf._nonce_reserve(n3, (), now)
            self.assertTrue(owned3b)
            self.assertEqual(list(sf._nonce_results), [n2, n3])
        with patch.object(sf, "MAX_NONCE_RESULTS", 2):
            _run(_go())

    def test_default_limit_keeps_fresh_inflight_among_many_finished(self):
        now = sf.time.time()
        for i in range(sf.MAX_NONCE_RESULTS):
            self.assertTrue(sf._nonce_put(f"d{i}", _done(now), now))
        fresh = sf._NonceEntry(now, ())
        self.assertTrue(sf._nonce_put("fresh", fresh, now))
        for i in range(sf.MAX_NONCE_RESULTS):
            self.assertTrue(sf._nonce_put(f"e{i}", _done(now), now))
        self.assertEqual(len(sf._nonce_results), sf.MAX_NONCE_RESULTS)
        self.assertIs(sf._nonce_results.get("fresh"), fresh)
        self.assertEqual(list(sf._nonce_results)[0], "fresh")       # 最古だが退避されていない

    def test_same_nonce_waiter_unaffected_by_full_store(self):
        # 表が処理中で満杯でも、既存の予約と同じ nonce の POST は拒否ではなく待機→同じ結果
        n = sf.issue_nonce()
        gate = asyncio.Event()
        real_create = self.fake.create_record

        async def _gated_create(app, fields):
            await gate.wait()
            return await real_create(app, fields)

        async def _go():
            t1 = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            t2 = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            gate.set()
            return await asyncio.gather(t1, t2)
        with patch.object(sf, "MAX_NONCE_RESULTS", 1), \
                patch.object(sf.hub_kintone, "create_record", _gated_create):
            r1, r2 = _run(_go())
        db.reset_for_tests()
        self.assertEqual((r1.status_code, r2.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIn("受付番号：100001", r1.body.decode("utf-8"))
        self.assertIn("受付番号：100001", r2.body.decode("utf-8"))
        self.assertEqual(self._warnings(), [])

    def test_fix1_37_and_fix2_11_unchanged(self):
        import test_form_dup_1 as fix1
        import test_form_dup_1_fix2 as fix2
        self.assertEqual(unittest.defaultTestLoader.loadTestsFromModule(fix1).countTestCases(), 37)
        self.assertEqual(unittest.defaultTestLoader.loadTestsFromModule(fix2).countTestCases(), 11)


if __name__ == "__main__":
    unittest.main()
