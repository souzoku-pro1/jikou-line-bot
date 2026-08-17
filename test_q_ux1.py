"""Q-UX-1: 質問機能の改良 3 点（チャット UI・二重送信根絶・レート計上調整）。

固定する仕様:
- (B) single-flight: 処理中に来た POST は実行せず 303 → /app/q?e=busy。
  API 非呼出し・レート非計上。marker は完了/例外いずれでも必ず解放される。
  実並行（thread + 実 TestClient）でも 2 本目が弾かれることを実測。
- (C) レート計上: 「完了した回答生成」（ok/no_source）のみ計上。error
  （回答生成が完了しなかったもの）は数えない。制限中の試行は窓を延長しない。
  e=rate には回復までの目安秒 retry を添える（質問文は載せない=PII 規律）。
  上限値 10 問/600 秒は不変。
- (A) チャット UI: 質問=右・回答=左の吹き出し・時系列（最新が下）・入力欄は
  画面下部・送信で即時吹き出し＋「回答を作成中…」・二重送信ガード。
  通信方式は不変（native form POST＋PRG・app_fetch は GET 履歴読込のみ）。
"""

import asyncio
import os
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

from test_q_batch1 import (
    _ENV,
    _DbMixin,
    _auth_headers,
    _resp,
    _stub_client,
    _text,
)

import hub.webapp_q as wq
import main
from fastapi.testclient import TestClient

_client = TestClient(main.app)


class _UxMixin(_DbMixin):
    def setUp(self):
        super().setUp()
        del wq._inflight[:]

    def tearDown(self):
        del wq._inflight[:]
        super().tearDown()

    def _post(self, stub, question="質問"):
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub):
            return _client.post("/app/q/ask", data={"question": question},
                                headers=_auth_headers(),
                                follow_redirects=False)


# ── (B) single-flight ────────────────────────────────────────────────────────
class TestSingleFlight(_UxMixin):
    def test_busy_redirect_not_executed_not_counted(self):
        # 処理中 marker が立っている間の POST は実行も計上もされない
        wq._inflight.append(1)
        stub = _stub_client([])
        r = self._post(stub)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/q?e=busy")
        stub.messages.create.assert_not_called()
        self.assertEqual(wq._ask_times, [])

    def test_busy_checked_before_rate(self):
        # 重複 POST は e=rate でなく e=busy（レート計上経路に一切入らない）
        wq._inflight.append(1)
        wq._ask_times.extend([time.time()] * wq.RATE_LIMIT)
        before = list(wq._ask_times)
        r = self._post(_stub_client([]))
        self.assertEqual(r.headers["location"], "/app/q?e=busy")
        self.assertEqual(wq._ask_times, before)

    def test_inflight_cleared_after_completion(self):
        stub = _stub_client([_resp("end_turn", [_text("x")])])
        r = self._post(stub)
        self.assertTrue(r.headers["location"].startswith("/app/q?done="))
        self.assertEqual(wq._inflight, [])
        # 解放済みなので次の質問は busy にならない
        stub2 = _stub_client([_resp("end_turn", [_text("y")])])
        r2 = self._post(stub2)
        self.assertTrue(r2.headers["location"].startswith("/app/q?done="))

    def test_inflight_cleared_even_if_answer_raises(self):
        # _answer_question は設計上 raise しないが、万一の例外でも marker が
        # 残留しない（try/finally）ことを pin
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_answer_question",
                          AsyncMock(side_effect=RuntimeError("boom"))):
            with self.assertRaises(RuntimeError):
                _client.post("/app/q/ask", data={"question": "q"},
                             headers=_auth_headers(), follow_redirects=False)
        self.assertEqual(wq._inflight, [])
        self.assertEqual(wq._ask_times, [])      # 未完了は計上しない

    def test_concurrent_second_post_rejected_live(self):
        # 実並行: 1 本目が回答生成中（API 待ち）に 2 本目を投げると e=busy。
        # gate 解放後、1 本目は正常に完了して計上される
        gate = threading.Event()

        async def _wait_then(*args, **kwargs):
            while not gate.is_set():
                await asyncio.sleep(0.005)
            return _resp("end_turn", [_text("x")])

        stub = _stub_client(None)
        stub.messages.create = AsyncMock(side_effect=_wait_then)
        results = {}

        def _first():
            results["first"] = self._post(stub)

        t = threading.Thread(target=_first)
        t.start()
        try:
            deadline = time.time() + 5
            while not wq._inflight and time.time() < deadline:
                time.sleep(0.005)
            self.assertTrue(wq._inflight, "1 本目が処理中にならない")
            r2 = self._post(_stub_client([]))
            self.assertEqual(r2.headers["location"], "/app/q?e=busy")
        finally:
            gate.set()
            t.join(timeout=10)
        self.assertFalse(t.is_alive())
        self.assertTrue(
            results["first"].headers["location"].startswith("/app/q?done="))
        self.assertEqual(wq._inflight, [])
        self.assertEqual(len(wq._ask_times), 1)   # 完了 1 件のみ計上


# ── (C) レート計上の調整 ─────────────────────────────────────────────────────
class TestRateAccounting(_UxMixin):
    def test_error_attempt_not_counted(self):
        # 回答生成が完了しなかった試行（status=error）は計上しない
        r = self._post(_stub_client([RuntimeError("api down")]))
        self.assertTrue(r.headers["location"].startswith("/app/q?done="))
        self.assertEqual(wq._ask_times, [])

    def test_completed_attempts_counted(self):
        # ok/no_source（完了した回答生成）は計上する
        self._post(_stub_client([_resp("end_turn", [_text("x")])]))
        self.assertEqual(len(wq._ask_times), 1)
        self._post(_stub_client([_resp("end_turn", [_text("y")])]))
        self.assertEqual(len(wq._ask_times), 2)

    def test_retry_estimate_reflects_oldest_entry(self):
        # 最古の計上が窓から抜けるまでの秒数が retry になる（±数秒の目安）
        now = time.time()
        wq._ask_times.extend([now - 500] * wq.RATE_LIMIT)
        r = self._post(_stub_client([]))
        location = r.headers["location"]
        self.assertRegex(location, r"^/app/q\?e=rate&retry=\d+$")  # PII なし
        retry = int(location.split("retry=", 1)[1])
        self.assertGreaterEqual(retry, 95)       # ≒ 600-500=100 秒
        self.assertLessEqual(retry, 102)

    def test_rate_limited_attempt_does_not_extend_window(self):
        # 制限中の試行は計上されない＝窓が延長されず必ず自然回復する
        now = time.time()
        wq._ask_times.extend([now - 100] * wq.RATE_LIMIT)
        before = list(wq._ask_times)
        stub = _stub_client([])
        r = self._post(stub)
        self.assertTrue(
            r.headers["location"].startswith("/app/q?e=rate&retry="))
        self.assertEqual(wq._ask_times, before)   # 追記なし
        stub.messages.create.assert_not_called()

    def test_expired_entries_pruned_and_allowed(self):
        # 窓（600 秒）より古い計上だけなら通る
        wq._ask_times.extend([time.time() - 700] * wq.RATE_LIMIT)
        r = self._post(_stub_client([_resp("end_turn", [_text("x")])]))
        self.assertTrue(r.headers["location"].startswith("/app/q?done="))
        self.assertEqual(len(wq._ask_times), 1)   # 期限切れは掃除済み＋完了 1 件

    def test_limit_values_unchanged(self):
        # 上限値そのものは票の指示どおり不変
        self.assertEqual(wq.RATE_LIMIT, 10)
        self.assertEqual(wq.RATE_WINDOW_SECONDS, 600)


# ── (A) チャット UI（静的 pin・実行系は実機スモーク=[人]） ────────────────────
class TestChatPage(unittest.TestCase):
    def _page(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/q", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        return r.text

    def test_chat_layout_and_transport_unchanged(self):
        src = self._page()
        # 通信方式は不変: native form POST（質問文を GET query に載せない）
        self.assertIn('method="post" action="/app/q/ask"', src)
        self.assertIn('name="question" maxlength="2000"', src)
        # 履歴読込は app_fetch（GET 専用ラッパー）経由のみ・生 fetch token なし
        self.assertIn('app_fetch("/app/api/q/history', src)
        self.assertNotRegex(src, r"(?<![A-Za-z0-9_$])fetch(?![A-Za-z0-9_$])")
        # チャット構造: 吹き出し（質問=me/回答=bot）・時系列（reverse で最新が下）
        self.assertIn('id="chat"', src)
        self.assertIn('id="composer"', src)
        self.assertIn('row("me")', src)
        self.assertIn('row("bot")', src)
        self.assertIn(".reverse()", src)
        self.assertIn("scrollBottom", src)

    def test_double_submit_guard_and_typing_indicator(self):
        src = self._page()
        # 二重送信ガード: sending flag＋preventDefault＋ボタン disabled。
        # textarea は readOnly（disabled だと POST body から落ちるため）
        self.assertIn("if (sending || !text) { ev.preventDefault(); return; }",
                      src)
        self.assertIn("sending = true;", src)
        self.assertIn("qbox.readOnly = true;", src)
        self.assertIn("sendBtn.disabled = true;", src)
        self.assertNotIn("qbox.disabled", src)
        # 送信直後の即時表示: 自分の吹き出し＋「回答を作成中…」
        self.assertIn("回答を作成中", src)

    def test_flash_messages_cover_all_error_params(self):
        src = self._page()
        self.assertIn("質問が多すぎます（10分あたり10問まで）", src)
        self.assertIn("分後に再試行できます", src)       # retry の目安表示
        self.assertIn('params.get("retry")', src)
        self.assertIn("前の質問の回答を作成中です", src)  # e=busy
        self.assertIn("質問を入力してください", src)      # e=input
        self.assertIn("回答の保存に失敗しました", src)    # e=save

    def test_no_new_routes_added(self):
        # (B) は既存 POST /app/q/ask の中で実現（経路の追加なし＝関所の不変）
        paths = sorted({(r.path, m) for r in wq.router.routes
                        if hasattr(r, "endpoint")
                        for m in r.methods if m != "HEAD"})
        self.assertEqual(paths, [
            ("/app/api/q/history", "GET"),
            ("/app/q", "GET"),
            ("/app/q/ask", "POST"),
        ])


if __name__ == "__main__":
    unittest.main()
