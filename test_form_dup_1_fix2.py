"""FORM-DUP-1-fix2（Codex R-FD1-fix1 BF-05）: nonce の「既存確認」と「処理権の予約」を 1 つの
原子操作にし、token 照合など外部待機の**前**に行う。

固定する内容:
 - 実入口 shindan_entry() に同一 nonce の 2 本を同時 POST し、token 照合（shindan_link.lookup）
   を Event で待機させてから解放しても、create は 1 回・受付番号は同一・結果エントリは
   上書きされない（予約した 1 本だけが先へ進み、他は予約者の完了を待って同じ結果を返す）。
   不明 token・使用済み token・k なし・有効 token の 4 経路で同じ検査。
 - 1 本目が失敗したら予約を解放し、待っていた 2 本目が新たに予約を取って進む。
 - 結果保存・解放は自分の予約（entry の同一性）に対してだけ行う。
 - 既存 test_form_dup_1 37 件は不変。
"""

import asyncio
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

from fastapi import Request

from test_form_dup_1 import _Base  # noqa: E402  fix1 の基盤（fake・DB・nonce helper）を再利用
from test_shindan_line_link import USER, VALID, _run  # noqa: E402

import shindan_form as sf  # noqa: E402
from hub import db  # noqa: E402
from hub import shindan_link as sl  # noqa: E402


def _request(form: dict, token: str | None = None, query_token: str | None = None) -> Request:
    """shindan_entry() に直接渡す POST Request（urlencoded・cookie または ?k=）。"""
    body = urlencode(form).encode("utf-8")
    headers = [(b"content-type", b"application/x-www-form-urlencoded"),
               (b"content-length", str(len(body)).encode("ascii")),
               (b"x-real-ip", b"203.0.113.77")]
    if token:
        headers.append((b"cookie", f"{sf.LINK_COOKIE}={token}".encode("ascii")))
    scope = {"type": "http", "http_version": "1.1", "method": "POST", "scheme": "https",
             "path": "/shindan", "raw_path": b"/shindan", "root_path": "",
             "query_string": f"k={query_token}".encode("ascii") if query_token else b"",
             "headers": headers, "client": ("203.0.113.77", 1234), "server": ("test", 443)}
    sent = {"done": False}

    async def receive():
        if sent["done"]:
            await asyncio.sleep(3600)
        sent["done"] = True
        return {"type": "http.request", "body": body, "more_body": False}
    return Request(scope, receive)


class _Counting(sf._NonceEntry):
    """予約エントリの生成回数（＝予約の取得回数）を数える。"""
    made: list = []

    def __init__(self, ts, sig):
        super().__init__(ts, sig)
        _Counting.made.append(self)


class TestAtomicReservation(_Base):
    def setUp(self):
        super().setUp()
        _Counting.made = []
        p = patch.object(sf, "_NonceEntry", _Counting)
        p.start()
        self.addCleanup(p.stop)

    def _race(self, req_a: Request, req_b: Request, *, gate_lookup=True, gate_create=False):
        """2 本を同時に shindan_entry() へ。token 照合（lookup）または create を Event で止め、
        両方が入口を通過してから解放する。"""
        gate = asyncio.Event()
        real_lookup, real_create = sl.lookup, self.fake.create_record

        async def _gated_lookup(token, *a, **kw):
            await gate.wait()
            return await real_lookup(token, *a, **kw)

        async def _gated_create(app, fields):
            await gate.wait()
            return await real_create(app, fields)

        async def _two():
            t1 = asyncio.create_task(sf.shindan_entry(req_a))
            await asyncio.sleep(0.05)
            t2 = asyncio.create_task(sf.shindan_entry(req_b))
            await asyncio.sleep(0.05)
            gate.set()
            return await asyncio.gather(t1, t2)
        patches = []
        if gate_lookup:
            patches.append(patch.object(sl, "lookup", _gated_lookup))
        if gate_create:
            patches.append(patch.object(sf.hub_kintone, "create_record", _gated_create))
        for p in patches:
            p.start()
        try:
            with patch.object(sf, "_draw_number", return_value="424242"):
                r1, r2 = _run(_two())
        finally:
            for p in patches:
                p.stop()
        db.reset_for_tests()
        return r1.body.decode("utf-8"), r2.body.decode("utf-8"), r1.status_code, r2.status_code

    def _form(self, nonce, **over):
        data = dict(VALID)
        data.update(over)
        data[sf.NONCE_FIELD] = nonce
        return data

    def _assert_single_entry_not_overwritten(self, nonce, rid):
        self.assertEqual(len(_Counting.made), 1)                    # 予約は 1 本だけ
        self.assertIs(sf._nonce_results[nonce], _Counting.made[0])  # 上書きされていない
        self.assertEqual(sf._nonce_results[nonce].result[1], rid)

    # ── 票のテスト: 同一 nonce の 2 本同時 POST・token 照合を待機→解放 ─────────────
    def test_unknown_token_concurrent_same_nonce_single_create_same_number(self):
        n = sf.issue_nonce()
        b1, b2, s1, s2 = self._race(_request(self._form(n), token="nosuchtoken"),
                                    _request(self._form(n), token="nosuchtoken"))
        self.assertEqual((s1, s2), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIn("受付番号：424242", b1)
        self.assertIn("受付番号：424242", b2)
        self.assertEqual(self.fake.create_calls[0]["LINEユーザーID"], "")
        self._assert_single_entry_not_overwritten(n, list(self.fake.rows)[-1])
        self.assertEqual(self.notify_biz.await_count, 1)

    def test_used_token_concurrent_same_nonce_no_create_done_page_both(self):
        token = self.issue()
        self.post_linked_n(token, self.get_nonce(k=token))         # 使用済みにする（create 1）
        self.assertEqual(len(self.fake.create_calls), 1)
        _Counting.made = []
        n = sf.issue_nonce()                                          # 1 回目とは別 nonce
        b1, b2, s1, s2 = self._race(_request(self._form(n), token=token),
                                    _request(self._form(n), token=token))
        self.assertEqual((s1, s2), (200, 200))
        for b in (b1, b2):
            self.assertIn(sf.RESUBMIT_DONE_TEXT, b)
            self.assertNotIn("受付番号", b)
            self.assertNotIn(sf.PHOTO_ROUTE, b)
        self.assertEqual(len(self.fake.create_calls), 1)            # 双子なし
        self.assertEqual(self.fake.update_calls, [])
        self.assertNotIn(n, sf._nonce_results)                      # 結果が無いので予約は解放済み
        self.assertEqual(len(_Counting.made), 2)                    # 2 本目は取り直して進んだ

    def test_plain_no_token_concurrent_same_nonce_single_create_same_number(self):
        n = sf.issue_nonce()
        b1, b2, s1, s2 = self._race(_request(self._form(n)), _request(self._form(n)),
                                    gate_lookup=False, gate_create=True)
        self.assertEqual((s1, s2), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIn("受付番号：424242", b1)
        self.assertIn("受付番号：424242", b2)
        self._assert_single_entry_not_overwritten(n, list(self.fake.rows)[-1])

    def test_valid_token_concurrent_same_nonce_single_create_same_linked_page(self):
        token = self.issue()
        n = sf.issue_nonce()
        b1, b2, s1, s2 = self._race(_request(self._form(n), token=token),
                                    _request(self._form(n), query_token=token))
        self.assertEqual((s1, s2), (200, 200))
        self.assertEqual(len(self.fake.create_calls), 1)            # T6: 新規作成 1 回
        self.assertEqual(self.fake.create_calls[0]["LINEユーザーID"], USER)
        for b in (b1, b2):
            self.assertIn(sf.LINKED_DONE_TEXT, b)
            self.assertNotIn("受付番号", b)
        self._assert_single_entry_not_overwritten(n, list(self.fake.rows)[-1])
        self.assertEqual(sf._nonce_results[n].result[0], "linked")
        self.assertIsNotNone(self.used_at(token))
        self.assertEqual(self.busy_lines(), [])                     # SLL-02 の busy にも入らない

    # ── 1 本目失敗 → 2 本目が取り直して進む ─────────────────────────────────────
    def test_first_fails_second_reserves_anew_and_creates(self):
        n = sf.issue_nonce()
        self.fake.create_fail_next = 1
        b1, b2, s1, s2 = self._race(_request(self._form(n)), _request(self._form(n)),
                                    gate_lookup=False, gate_create=True)
        self.assertEqual((s1, s2), (500, 200))                      # 1 本目 500・2 本目は作成
        self.assertEqual(len(self.fake.create_calls), 2)
        self.assertIn("受付番号：424242", b2)
        self.assertEqual(len(_Counting.made), 2)                    # 取り直し＝新しい予約
        self.assertIs(sf._nonce_results[n], _Counting.made[1])      # 残るのは成功した 2 本目の予約
        self.assertEqual(sf._nonce_results[n].result[1], list(self.fake.rows)[-1])

    def test_existing_result_is_never_overwritten_by_concurrent_posts(self):
        n = self.get_nonce()
        with patch.object(sf, "_draw_number", return_value="111111"):
            self.post_plain(nonce=n)                                 # 1 回目（結果あり）
        first = sf._nonce_results[n]
        _Counting.made = []
        b1, b2, s1, s2 = self._race(_request(self._form(n)), _request(self._form(n)),
                                    gate_lookup=False)
        self.assertEqual((s1, s2), (200, 200))
        self.assertIn("受付番号：111111", b1)
        self.assertIn("受付番号：111111", b2)
        self.assertEqual(len(self.fake.create_calls), 1)
        self.assertIs(sf._nonce_results[n], first)                   # 上書きなし
        self.assertEqual(_Counting.made, [])                         # 新しい予約も作らない

    # ── 予約・解放の原子性と同一性 ───────────────────────────────────────────────
    def test_reserve_and_release_semantics(self):
        async def _go():
            n = sf.issue_nonce()
            e1, owned1 = await sf._nonce_reserve(n, ("a",), sf.time.time())
            e2, owned2 = await sf._nonce_reserve(n, ("b",), sf.time.time())
            self.assertTrue(owned1)
            self.assertFalse(owned2)
            self.assertIs(e1, e2)                                    # 既存の予約を上書きしない
            self.assertEqual(e1.sig, ("a",))
            # 他人の entry での解放は現在の予約を外さない
            foreign = sf._NonceEntry(sf.time.time(), ("x",))
            sf._nonce_release(n, foreign)
            self.assertIs(sf._nonce_results[n], e1)
            self.assertTrue(foreign.event.is_set())
            # 自分の予約を結果なしで解放 → 外れる・待機側が起きる
            sf._nonce_release(n, e1)
            self.assertNotIn(n, sf._nonce_results)
            self.assertTrue(e1.event.is_set())
            # 結果ありの解放は残す
            e3, owned3 = await sf._nonce_reserve(n, ("c",), sf.time.time())
            self.assertTrue(owned3)
            e3.result = ("plain", "9", "000009", "A")
            sf._nonce_release(n, e3)
            self.assertIs(sf._nonce_results[n], e3)
            # 期限切れの結果は捨てて新規に予約
            e3.ts -= sf.NONCE_TTL_SEC + 1
            e4, owned4 = await sf._nonce_reserve(n, ("d",), sf.time.time())
            self.assertTrue(owned4)
            self.assertIsNot(e4, e3)
        _run(_go())

    def test_lock_is_per_running_loop(self):
        async def _lock():
            a = sf._get_nonce_lock()
            self.assertIs(a, sf._get_nonce_lock())
            return a
        l1 = _run(_lock())
        l2 = _run(_lock())
        self.assertIsNot(l1, l2)                                     # 別 loop では別 Lock（束縛エラーなし）

    def test_reservation_precedes_token_lookup(self):
        # 予約は token 照合（外部待機）の前: lookup が呼ばれた時点で entry が辞書にある
        n = sf.issue_nonce()
        seen = {}
        real = sl.lookup

        async def _spy(token, *a, **kw):
            seen["entry_present"] = n in sf._nonce_results
            return await real(token, *a, **kw)
        with patch.object(sl, "lookup", _spy):
            r = _run(sf.shindan_entry(_request(self._form(n), token="nosuchtoken")))
        db.reset_for_tests()
        self.assertEqual(r.status_code, 200)
        self.assertTrue(seen["entry_present"])

    def test_waiter_timeout_is_pending_page_without_create(self):
        n = sf.issue_nonce()
        _run(sf._nonce_reserve(n, (), sf.time.time()))              # 予約だけ（処理中のまま）
        with patch.object(sf, "_NONCE_WAIT_SEC", 0.05):
            r = _run(sf.shindan_entry(_request(self._form(n), token="nosuchtoken")))
        db.reset_for_tests()
        self.assertEqual(r.status_code, 200)
        self.assertIn(sf.PLAIN_PENDING_TEXT, r.body.decode("utf-8"))
        self.assertEqual(self.fake.create_calls, [])

    def test_fix1_tests_still_37(self):
        # 既存 test_form_dup_1 の 37 件は不変（削除・skip なし）
        import test_form_dup_1 as fix1
        suite = unittest.defaultTestLoader.loadTestsFromModule(fix1)
        self.assertEqual(suite.countTestCases(), 37)


if __name__ == "__main__":
    unittest.main()
