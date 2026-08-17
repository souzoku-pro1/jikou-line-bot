"""Q-CHAT-1: 質問機能の会話化（会話文脈の注入＋話題リセット）。

固定する仕様:
- (A) 文脈注入: 直近の会話（前回リセット以降・error 除外）を user/assistant
  交互 message として質問の前に注入。二重上限=直近 10 往復かつ合計 6000 字・
  1 回答 800 字末尾切り詰め・超過は古い側から往復ごと落とす。DISCLAIMER は
  注入時に除去。文脈が読めない場合（migration 未適用等）は無文脈で続行
  （fail-open・質問機能は止めない）。
- (B) 話題リセット: POST /app/q/reset（_gate・PRG・レート非計上・busy 中は
  拒否）。境界=リセット時点の qa_record 最大 id を qa_topic_reset に永続化。
  履歴 API は after_reset/has_reset で境界を可視化（境界不読は has_reset=False
  へ縮退）。画面は「ここから新しい話題」区切り線。
- (C) 履歴は文脈であり出典ではない: subset 照合は当該 turn 内の実測 tool
  結果のみ（履歴があっても turn 内 tool ゼロの submit は no_source）。
"""

import os
import unittest
from unittest.mock import AsyncMock, patch

from test_q_batch1 import (
    _ENV,
    _auth_headers,
    _resp,
    _run,
    _stub_client,
    _submit,
    _text,
)
from test_q_ux1 import _UxMixin, _client

import hub.qa_store as qa_store
import hub.webapp_q as wq


def _seed_qa(question, answer, status="ok", with_disclaimer=True):
    if with_disclaimer:
        answer = answer + "\n\n" + wq.DISCLAIMER
    return _run(qa_store.save_qa(
        user_id="owner", question=question, answer=answer, status=status,
        sources=[], notes=[], model="m", input_tokens=1, output_tokens=1,
        cache_read_tokens=0, cost_usd="0", elapsed_ms=1))


def _ctx_row(question, answer):
    return {"question": question, "answer": answer}


# ── (A) _build_history（純関数・二重上限） ───────────────────────────────────
class TestBuildHistory(unittest.TestCase):
    def test_order_roles_and_disclaimer_stripped(self):
        rows = [  # list_context_qa の戻り＝新しい順
            _ctx_row("Q2", "A2\n\n" + wq.DISCLAIMER),
            _ctx_row("Q1", "A1\n\n" + wq.DISCLAIMER),
        ]
        msgs = wq._build_history(rows)
        self.assertEqual(msgs, [
            {"role": "user", "content": "Q1"},
            {"role": "assistant", "content": "A1"},
            {"role": "user", "content": "Q2"},
            {"role": "assistant", "content": "A2"},
        ])

    def test_long_answer_tail_truncated_mechanically(self):
        rows = [_ctx_row("Q", "あ" * 1000)]
        msgs = wq._build_history(rows)
        self.assertEqual(len(msgs), 2)
        a = msgs[1]["content"]
        self.assertTrue(a.startswith("あ" * wq._HISTORY_ANSWER_MAX_CHARS))
        self.assertTrue(a.endswith(wq._HISTORY_TRUNC_MARK))
        self.assertEqual(
            len(a),
            wq._HISTORY_ANSWER_MAX_CHARS + len(wq._HISTORY_TRUNC_MARK))

    def test_exchange_cap_keeps_newest_10(self):
        rows = [_ctx_row(f"Q{i}", f"A{i}") for i in range(12, 0, -1)]
        msgs = wq._build_history(rows)
        self.assertEqual(len(msgs), wq._HISTORY_MAX_EXCHANGES * 2)
        self.assertEqual(msgs[0]["content"], "Q3")     # 古い 2 往復が落ちる
        self.assertEqual(msgs[-1]["content"], "A12")

    def test_total_chars_cap_drops_oldest_side_whole(self):
        # 1 往復 2000 字 × 5 → 合計上限 6000 字で新しい 3 往復のみ残る
        rows = [_ctx_row("q" * 1000 + str(i), "a" * 999) for i in range(5)]
        msgs = wq._build_history(rows)
        self.assertEqual(len(msgs), 6)
        self.assertEqual(msgs[0]["content"], rows[2]["question"])  # 新しい側優先
        total = sum(len(m["content"]) for m in msgs)
        self.assertLessEqual(total, wq._HISTORY_MAX_TOTAL_CHARS)

    def test_empty_and_blank_rows(self):
        self.assertEqual(wq._build_history([]), [])
        rows = [_ctx_row("", "A"), _ctx_row("Q", ""),
                _ctx_row("Q", wq.DISCLAIMER)]      # 除去後空も skip
        self.assertEqual(wq._build_history(rows), [])


# ── qa_store（境界の永続化と文脈クエリ） ─────────────────────────────────────
class TestQaStoreReset(_UxMixin):
    def test_boundary_none_without_reset(self):
        self.assertIsNone(_run(qa_store.latest_reset_boundary()))

    def test_reset_records_current_max_id(self):
        # qa が空の状態のリセットは境界 0（None=リセット無しと区別される）
        _run(qa_store.save_topic_reset(user_id="owner"))
        self.assertEqual(_run(qa_store.latest_reset_boundary()), 0)
        qid1 = _seed_qa("Q1", "A1")
        _run(qa_store.save_topic_reset(user_id="owner"))
        self.assertEqual(_run(qa_store.latest_reset_boundary()), qid1)
        qid2 = _seed_qa("Q2", "A2")
        _run(qa_store.save_topic_reset(user_id="owner"))
        self.assertEqual(_run(qa_store.latest_reset_boundary()), qid2)

    def test_context_respects_reset_and_excludes_error(self):
        _seed_qa("古い質問", "古い回答")
        _run(qa_store.save_topic_reset(user_id="owner"))
        _seed_qa("新Q1", "新A1")
        _seed_qa("失敗Q", wq.ERROR_ANSWER, status="error")
        _seed_qa("新Q2", "新A2", status="no_source")
        rows = _run(qa_store.list_context_qa(limit=10))
        self.assertEqual([r["question"] for r in rows], ["新Q2", "新Q1"])

    def test_context_without_reset_returns_all_non_error(self):
        _seed_qa("Q1", "A1")
        _seed_qa("Q2", "A2")
        rows = _run(qa_store.list_context_qa(limit=10))
        self.assertEqual([r["question"] for r in rows], ["Q2", "Q1"])


# ── (A) 文脈注入の end-to-end（mock Claude・実 DB） ──────────────────────────
class TestContextInjection(_UxMixin):
    def _messages_sent(self, stub):
        return stub.messages.create.await_args.kwargs["messages"]

    def test_history_injected_before_question(self):
        _seed_qa("Q1", "A1")
        _seed_qa("Q2", "A2")
        stub = _stub_client([_resp("end_turn", [_text("x")])])
        r = self._post(stub, question="Q3")
        self.assertTrue(r.headers["location"].startswith("/app/q?done="))
        msgs = self._messages_sent(stub)
        self.assertEqual(
            [(m["role"], m["content"]) for m in msgs],
            [("user", "Q1"), ("assistant", "A1"),
             ("user", "Q2"), ("assistant", "A2"), ("user", "Q3")])
        # DISCLAIMER は注入時に除去済み（保存時は付与されている）
        self.assertNotIn(wq.DISCLAIMER, msgs[1]["content"])

    def test_reset_cuts_history(self):
        _seed_qa("Q1", "A1")
        _run(qa_store.save_topic_reset(user_id="owner"))
        stub = _stub_client([_resp("end_turn", [_text("x")])])
        self._post(stub, question="Q2")
        msgs = self._messages_sent(stub)
        self.assertEqual([(m["role"], m["content"]) for m in msgs],
                         [("user", "Q2")])

    def test_context_load_failure_fails_open(self):
        # migration 未適用等で文脈が読めなくても質問機能は無文脈で続行
        _seed_qa("Q1", "A1")
        stub = _stub_client([_resp("end_turn", [_text("x")])])
        with patch.object(wq.qa_store, "list_context_qa",
                          AsyncMock(side_effect=RuntimeError("no table"))):
            r = self._post(stub, question="Q2")
        self.assertTrue(r.headers["location"].startswith("/app/q?done="))
        msgs = self._messages_sent(stub)
        self.assertEqual([(m["role"], m["content"]) for m in msgs],
                         [("user", "Q2")])

    def test_history_is_context_not_source(self):
        # (C) 履歴があっても、当該 turn 内で tool を呼ばない submit は
        # 実測出典ゼロ＝no_source へ fail-closed（subset 照合の不変）
        _seed_qa("Q1", "A1")
        stub = _stub_client([
            _resp("tool_use",
                  [_submit("履歴によれば…", [{"app": "App36(相続人)",
                                              "record_id": "201"}])]),
        ])
        r = self._post(stub, question="Q2")
        self.assertTrue(r.headers["location"].startswith("/app/q?done="))
        with patch.dict(os.environ, _ENV):
            rec = _client.get("/app/api/q/history?limit=1",
                              headers=_auth_headers(),
                              follow_redirects=False).json()["records"][0]
        self.assertEqual(rec["status"], "no_source")
        self.assertEqual(rec["sources"], [])


# ── (B) リセット route ───────────────────────────────────────────────────────
class TestResetRoute(_UxMixin):
    def test_unauthenticated_rejected(self):
        with patch.dict(os.environ, _ENV):
            r = _client.post("/app/q/reset", follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/login")

    def test_reset_persists_boundary_and_not_rate_counted(self):
        qid = _seed_qa("Q1", "A1")
        with patch.dict(os.environ, _ENV):
            r = _client.post("/app/q/reset", headers=_auth_headers(),
                             follow_redirects=False)
        self.assertEqual(r.status_code, 303)
        self.assertEqual(r.headers["location"], "/app/q")
        self.assertEqual(_run(qa_store.latest_reset_boundary()), qid)
        self.assertEqual(wq._ask_times, [])       # 質問ではない＝計上しない

    def test_reset_rejected_while_inflight(self):
        wq._inflight.append(1)
        with patch.dict(os.environ, _ENV):
            r = _client.post("/app/q/reset", headers=_auth_headers(),
                             follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/q?e=busy")
        self.assertIsNone(_run(qa_store.latest_reset_boundary()))

    def test_reset_save_failure_redirects_e_save(self):
        with patch.dict(os.environ, _ENV), \
             patch.object(wq.qa_store, "save_topic_reset",
                          AsyncMock(side_effect=RuntimeError("db down"))):
            r = _client.post("/app/q/reset", headers=_auth_headers(),
                             follow_redirects=False)
        self.assertEqual(r.headers["location"], "/app/q?e=save")


# ── (B) 履歴 API の境界可視化 ────────────────────────────────────────────────
class TestHistoryResetFlags(_UxMixin):
    def _history(self, limit=50):
        with patch.dict(os.environ, _ENV):
            r = _client.get(f"/app/api/q/history?limit={limit}",
                            headers=_auth_headers(), follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        return r.json()

    def test_no_reset_flags(self):
        _seed_qa("Q1", "A1")
        data = self._history()
        self.assertFalse(data["has_reset"])
        self.assertEqual([r["after_reset"] for r in data["records"]], [False])

    def test_boundary_flags_split_records(self):
        _seed_qa("Q1", "A1")
        _run(qa_store.save_topic_reset(user_id="owner"))
        _seed_qa("Q2", "A2")
        data = self._history()
        self.assertTrue(data["has_reset"])
        by_q = {r["question"]: r["after_reset"] for r in data["records"]}
        self.assertEqual(by_q, {"Q1": False, "Q2": True})

    def test_boundary_lookup_failure_degrades(self):
        # 境界不読（migration 未適用等）でも台帳表示は生かす（fail-open）
        _seed_qa("Q1", "A1")
        with patch.object(wq.qa_store, "latest_reset_boundary",
                          AsyncMock(side_effect=RuntimeError("no table"))):
            data = self._history()
        self.assertTrue(data["available"])
        self.assertFalse(data["has_reset"])


# ── (B) チャット画面の静的 pin ───────────────────────────────────────────────
class TestChatPageReset(unittest.TestCase):
    def _page(self):
        with patch.dict(os.environ, _ENV):
            r = _client.get("/app/q", headers=_auth_headers(),
                            follow_redirects=False)
        self.assertEqual(r.status_code, 200)
        return r.text

    def test_reset_form_and_divider(self):
        src = self._page()
        self.assertIn('method="post" action="/app/q/reset"', src)
        self.assertIn("新しい話題", src)
        self.assertIn("ここから新しい話題", src)
        self.assertIn("after_reset", src)
        self.assertIn("has_reset", src)
        # 送信中のリセットは抑止（クライアント側二段目）
        self.assertIn('getElementById("resetform")', src)
        self.assertIn("if (sending) { ev.preventDefault(); }", src)
        # 生 fetch token は引き続きゼロ（native form POST のみ）
        self.assertNotRegex(src, r"(?<![A-Za-z0-9_$])fetch(?![A-Za-z0-9_$])")


if __name__ == "__main__":
    unittest.main()
