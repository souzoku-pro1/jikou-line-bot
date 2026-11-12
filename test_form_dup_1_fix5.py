"""FORM-DUP-1-fix5（Codex R-FD1-fix4 BF-08）: expired の nonce で「最初からエントリが無かった」
（→ 従来どおり新規・k なし・token 照合へ）と「既存予約を待っていたが解放されて無くなった」
（→ 固定ページ・作成 0・採番なし・expired では新規予約を取らない）を区別する。
 既存 test_form_dup_1 37・fix2 11・fix3 7・fix4 10 件は不変。
"""

import asyncio
import unittest
from unittest.mock import patch

from test_form_dup_1 import _Base  # noqa: E402
from test_form_dup_1_fix2 import _Counting, _request  # noqa: E402
from test_form_dup_1_fix4 import _Clock, _NUMBER_RE  # noqa: E402
from test_shindan_line_link import VALID, _run  # noqa: E402

import time as _real_time  # noqa: E402

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402

RELEASED_MSG = "duplicate post after first failed (no write)"
ERROR_TEXT = "システムエラーが発生しました"


class TestExpiredWaiterAfterRelease(_Base):
    def setUp(self):
        super().setUp()
        _Counting.made = []
        self.clock = _Clock(_real_time.time())
        for p in (patch.object(sf, "_NonceEntry", _Counting),
                  patch.object(sf, "time", self.clock),
                  patch.object(sf, "_draw_number", side_effect=[f"{i:06d}" for i in range(300001, 300099)])):
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

    def _three(self, *, advance: bool, fail_first: bool):
        """A（k なし・create を Event で待機）→ 時計（advance）→ 同一 nonce の B・C → 解放。
        fail_first なら A の create が失敗（結果未確定で解放）。"""
        n = sf.issue_nonce(self.clock.t)
        gate = asyncio.Event()
        real_create = self.fake.create_record

        async def _gated_create(app, fields):
            await gate.wait()
            return await real_create(app, fields)

        async def _go():
            t_a = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            entry_a = sf._nonce_results[n]
            self.assertIsNone(entry_a.result)                        # A は照合待機中
            if advance:
                self.clock.t += sf.NONCE_TTL_SEC + 1
                self.assertEqual(sf.nonce_state(n, self.clock.t)[0], "expired")
            t_b = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            t_c = asyncio.create_task(sf.shindan_entry(_request(self._form(n))))
            await asyncio.sleep(0.05)
            self.assertFalse(t_b.done() or t_c.done())               # B・C は待機
            self.assertEqual(len(_Counting.made), 1)
            if fail_first:
                self.fake.create_fail_next = 1
            gate.set()
            return await asyncio.gather(t_a, t_b, t_c), entry_a, n
        with patch.object(sf.hub_kintone, "create_record", _gated_create):
            (r_a, r_b, r_c), entry_a, n = _run(_go())
        db.reset_for_tests()
        return r_a, r_b, r_c, entry_a, n

    # ── 票: A 予約→待機・時計 1,801 秒・B・C 待機・A が結果未確定で失敗して解放 → B・C 固定ページ ──
    def test_expired_waiters_get_fixed_page_after_first_fails_no_create(self):
        r_a, r_b, r_c, entry_a, n = self._three(advance=True, fail_first=True)
        self.assertEqual(r_a.status_code, 500)                        # A は従来どおり 500
        self.assertEqual(len(self.fake.create_calls), 1)              # A の失敗 1 回だけ
        self.assertEqual(len(self.fake.rows), 0)                      # 作成 0
        for r in (r_b, r_c):
            body = r.body.decode("utf-8")
            self.assertEqual(r.status_code, 500)                      # 固定ページ（1 回目と同じエラー画面）
            self.assertIn(ERROR_TEXT, body)
            self.assertNotIn("受付番号", body)                        # 採番なし
            self.assertNotIn(sf.PHOTO_ROUTE, body)
            self.assertNotIn(sf.PLAIN_PENDING_TEXT, body)
            self.assertNotIn(sf.STORE_FULL_TEXT, body)
        self.assertEqual(len(_Counting.made), 1)                      # expired では新規予約を取らない
        self.assertNotIn(n, sf._nonce_results)                        # 解放済みのまま
        self.assertEqual(self.cap.text().count(RELEASED_MSG), 2)      # B・C の INFO 各 1 行
        self.assertNotIn(n, self.cap.text())

    # ── 票: 同じ順序で nonce が期限内なら fix2 どおり取り直し 1 本だけが作成（退行なし）──
    def test_valid_nonce_same_order_one_retaker_creates_as_fix2(self):
        r_a, r_b, r_c, entry_a, n = self._three(advance=False, fail_first=True)
        self.assertEqual(r_a.status_code, 500)
        self.assertEqual((r_b.status_code, r_c.status_code), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 2)              # A 失敗 + 取り直し 1 本
        self.assertEqual(len(self.fake.rows), 1)
        self.assertEqual(self._num(r_b), self._num(r_c))              # もう 1 本は取り直した側の結果
        self.assertEqual(len(_Counting.made), 2)                      # 取り直し＝新しい予約 1 つ
        self.assertIs(sf._nonce_results[n], _Counting.made[1])
        self.assertEqual(sf._nonce_results[n].result[2], self._num(r_b))
        self.assertNotIn(RELEASED_MSG, self.cap.text())

    # ── 同じ順序で A が成功した場合（expired）: B・C は A の結果（fix4 どおり・退行なし）──
    def test_expired_waiters_get_first_result_when_first_succeeds(self):
        r_a, r_b, r_c, entry_a, n = self._three(advance=True, fail_first=False)
        for r in (r_a, r_b, r_c):
            self.assertEqual(r.status_code, 200)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertEqual(self._num(r_b), self._num(r_a))
        self.assertEqual(self._num(r_c), self._num(r_a))
        self.assertEqual(len(_Counting.made), 1)
        self.assertIs(sf._nonce_results[n], entry_a)
        self.assertNotIn(RELEASED_MSG, self.cap.text())

    # ── 票: 最初からエントリ無しの expired は従来どおり新規（k なし・token 照合へ）────────
    def test_expired_without_entry_from_start_is_new_not_fixed_page(self):
        n = sf.issue_nonce(self.clock.t)
        self.clock.t += sf.NONCE_TTL_SEC + 1
        r = _run(sf.shindan_entry(_request(self._form(n))))
        db.reset_for_tests()
        self.assertEqual(r.status_code, 200)
        self._num(r)                                                  # 受付番号あり＝新規
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertNotIn(n, sf._nonce_results)                        # 予約は取らない
        self.assertEqual(_Counting.made, [])
        self.assertNotIn(RELEASED_MSG, self.cap.text())
        # 有効 token 付き（token 照合へ進む）でも同じく新規 linked
        token = self.issue()
        n2 = sf.issue_nonce(self.clock.t)
        self.clock.t += sf.NONCE_TTL_SEC + 1
        r2 = _run(sf.shindan_entry(_request(self._form(n2), token=token)))
        db.reset_for_tests()
        self.assertEqual(r2.status_code, 200)
        self.assertIn(sf.LINKED_DONE_TEXT, r2.body.decode("utf-8"))
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertNotIn(n2, sf._nonce_results)

    # ── 解放後に来た（待っていない）expired は「最初から無し」＝新規（waited の区別）──────
    def test_expired_arriving_after_release_without_waiting_is_new(self):
        n = sf.issue_nonce(self.clock.t)
        self.fake.create_fail_next = 1
        r_a = _run(sf.shindan_entry(_request(self._form(n))))         # A 失敗 → 解放
        db.reset_for_tests()
        self.assertEqual(r_a.status_code, 500)
        self.assertNotIn(n, sf._nonce_results)
        self.clock.t += sf.NONCE_TTL_SEC + 1
        r_b = _run(sf.shindan_entry(_request(self._form(n))))         # 待っていない B
        db.reset_for_tests()
        self.assertEqual(r_b.status_code, 200)
        self._num(r_b)
        self.assertEqual(len(self.fake.rows), 1)
        self.assertNotIn(RELEASED_MSG, self.cap.text())

    # ── 待ちきれない expired は従来どおり受付中ページ（waited でも解放されていない）─────
    def test_expired_waiter_timeout_is_pending_page_unchanged(self):
        n = sf.issue_nonce(self.clock.t)
        _run(sf._nonce_reserve(n, (), self.clock.t))                  # 予約だけ（処理中のまま）
        self.clock.t += sf.NONCE_TTL_SEC + 1
        with patch.object(sf, "_NONCE_WAIT_SEC", 0.05):
            r = _run(sf.shindan_entry(_request(self._form(n))))
        db.reset_for_tests()
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.PLAIN_PENDING_TEXT, r.body.decode("utf-8"))
        self.assertEqual(self.fake.create_calls, [])
        self.assertNotIn(RELEASED_MSG, self.cap.text())

    def test_released_page_is_fixed_500_without_number(self):
        r = sf._released_page()
        body = r.body.decode("utf-8")
        self.assertEqual(r.status_code, 500)
        self.assertIn(ERROR_TEXT, body)
        self.assertNotIn("受付番号", body)
        self.assertEqual(body, sf._error_500().body.decode("utf-8"))  # 1 回目と同じ固定ページ
        self.assertIn(RELEASED_MSG, self.cap.text())

    def test_fix1_37_fix2_11_fix3_7_fix4_10_unchanged(self):
        import test_form_dup_1 as fix1
        import test_form_dup_1_fix2 as fix2
        import test_form_dup_1_fix3 as fix3
        import test_form_dup_1_fix4 as fix4
        load = unittest.defaultTestLoader.loadTestsFromModule
        self.assertEqual(tuple(load(m).countTestCases() for m in (fix1, fix2, fix3, fix4)), (37, 11, 7, 10))


if __name__ == "__main__":
    unittest.main()
