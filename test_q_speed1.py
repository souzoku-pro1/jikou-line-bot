"""Q-SPEED-1: 応答高速化（同一 turn の tool 並列実行＋会話 prefix の
incremental cache）。

固定する仕様:
- (b) 並列実行: 同一 turn の複数 tool は並列に走る（実測: 相互依存 event で
  直列なら deadlock する形を通す）。tool_result の並び・tool_use_id 対応・
  出典/flag の統合は block 順で決定的。1 本の失敗は他を巻き込まない。
  単一呼び出しは従来どおり直列（挙動同一）。
- (a) incremental cache: 各 turn の messages 末尾 block に cache_control を
  付け直す。breakpoint は常に system 1＋message 1 の計 2 個（上限 4 内）。
- 消費量上限・出典照合・fail-closed は不変（既存テストが担保・ここでは
  並列化後も cap 検査が dispatch 前に効くことを対照）。
"""

import asyncio
import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from test_q_batch1 import (
    _ENV,
    _rec,
    _resp,
    _run,
    _submit,
    _tool_use,
)

import hub.webapp_q as wq


def _ctx():
    return {"sources": [], "source_keys": set(), "flags": set()}


def _heirs(rid, name="山田一郎", kakunin="yes"):
    return {"records": [_rec(**{"$id": rid, "氏名": name,
                                "戸籍確認済": kakunin})],
            "excluded_cancelled_count": 0}


def _blocks(*case_ids):
    return [_tool_use("list_case_heirs", {"case_record_id": c}, tid=f"tu{c}")
            for c in case_ids]


# ── (b) 並列実行 ─────────────────────────────────────────────────────────────
class TestParallelTools(unittest.TestCase):
    def test_tools_in_one_turn_run_concurrently(self):
        # block1（case 1）は block2（case 2）の開始を待つ——直列（block 順）
        # なら永遠に待つ形。並列なら即時に両方完了する
        ev = asyncio.Event()

        async def load(rid):
            if rid == "1":
                await asyncio.wait_for(ev.wait(), timeout=3)
                return _heirs("101")
            ev.set()
            return _heirs("201")

        async def run():
            ctx = _ctx()
            with patch.dict(os.environ, _ENV), \
                 patch.object(wq.souzoku_dash, "_load_heirs", side_effect=load):
                return ctx, await asyncio.wait_for(
                    wq._run_tools(_blocks("1", "2"), ctx), timeout=5)

        ctx, results = _run(run())
        self.assertEqual([r["is_error"] for r in results], [False, False])

    def test_order_ids_sources_flags_deterministic(self):
        # 完了順が逆（block1 が最後に完了）でも、結果・出典・flag は block 順
        ev = asyncio.Event()

        async def load(rid):
            if rid == "1":
                await asyncio.wait_for(ev.wait(), timeout=3)
                return _heirs("101", kakunin="no")     # flag も混ぜる
            ev.set()
            return _heirs("201")

        async def run():
            ctx = _ctx()
            with patch.dict(os.environ, _ENV), \
                 patch.object(wq.souzoku_dash, "_load_heirs", side_effect=load):
                return ctx, await wq._run_tools(_blocks("1", "2"), ctx)

        ctx, results = _run(run())
        self.assertEqual([r["tool_use_id"] for r in results], ["tu1", "tu2"])
        # 出典は block 順（完了順でない）
        self.assertEqual([s["record_id"] for s in ctx["sources"]],
                         ["101", "201"])
        self.assertIn("koseki_unconfirmed", ctx["flags"])
        # 各 tool_result の _citation_keys は自分の呼び出し分のみ
        keys0 = json.loads(results[0]["content"])["_citation_keys"]
        self.assertEqual(keys0, [{"app": "App36(相続人)",
                                  "record_id": "101"}])

    def test_one_failure_does_not_break_others(self):
        async def load(rid):
            if rid == "1":
                raise RuntimeError("kintone down")
            return _heirs("201")

        async def run():
            ctx = _ctx()
            with patch.dict(os.environ, _ENV), \
                 patch.object(wq.souzoku_dash, "_load_heirs", side_effect=load):
                return ctx, await wq._run_tools(_blocks("1", "2"), ctx)

        ctx, results = _run(run())
        self.assertTrue(results[0]["is_error"])
        self.assertFalse(results[1]["is_error"])
        # 失敗側の出典は入らない（fail-closed 維持）
        self.assertEqual([s["record_id"] for s in ctx["sources"]], ["201"])

    def test_single_call_serial_path_unchanged(self):
        async def run():
            ctx = _ctx()
            with patch.dict(os.environ, _ENV), \
                 patch.object(wq.souzoku_dash, "_load_heirs",
                              AsyncMock(return_value=_heirs("201"))):
                return ctx, await wq._run_tools(_blocks("12"), ctx)

        ctx, results = _run(run())
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["tool_use_id"], "tu12")
        self.assertEqual([s["record_id"] for s in ctx["sources"]], ["201"])


# ── (a) incremental cache marker ─────────────────────────────────────────────
class TestCacheMarker(unittest.TestCase):
    def _tool_result(self, tid):
        return {"type": "tool_result", "tool_use_id": tid,
                "content": "{}", "is_error": False}

    def test_marker_moves_to_last_block_only(self):
        m1 = self._tool_result("a")
        m2 = self._tool_result("b")
        m3 = self._tool_result("c")
        messages = [{"role": "user", "content": "質問"},
                    {"role": "assistant", "content": "x"},
                    {"role": "user", "content": [m1, m2]}]
        wq._set_message_cache_marker(messages)
        self.assertNotIn("cache_control", m1)
        self.assertEqual(m2["cache_control"], {"type": "ephemeral"})
        # 次 turn: marker は付け直され、常に 1 個だけ
        messages.append({"role": "user", "content": [m3]})
        wq._set_message_cache_marker(messages)
        self.assertNotIn("cache_control", m2)
        self.assertEqual(m3["cache_control"], {"type": "ephemeral"})
        total = sum(1 for msg in messages
                    if isinstance(msg.get("content"), list)
                    for b in msg["content"]
                    if isinstance(b, dict) and "cache_control" in b)
        self.assertEqual(total, 1)

    def test_string_contents_untouched(self):
        messages = [{"role": "user", "content": "質問"}]
        wq._set_message_cache_marker(messages)     # 例外にならず何もしない
        self.assertEqual(messages, [{"role": "user", "content": "質問"}])

    def test_e2e_breakpoints_bounded(self):
        # 2 turn の実ループで: 各 create 呼出しの messages 中の marker が
        # 常に 1 個以下（system 側と合わせても breakpoint 上限 4 内）
        calls = []

        async def fake_create(**kwargs):
            markers = sum(1 for msg in kwargs["messages"]
                          if isinstance(msg, dict)
                          and isinstance(msg.get("content"), list)
                          for b in msg["content"]
                          if isinstance(b, dict) and "cache_control" in b)
            calls.append(markers)
            if len(calls) == 1:
                return _resp("tool_use", [_tool_use(
                    "list_case_heirs", {"case_record_id": "12"})])
            tool_result = kwargs["messages"][-1]["content"][0]["content"]
            keys = json.loads(tool_result)["_citation_keys"]
            return _resp("tool_use", [_submit("回答です。", keys)])

        stub = SimpleNamespace(messages=SimpleNamespace(
            create=AsyncMock(side_effect=fake_create)))
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs",
                          AsyncMock(return_value=_heirs("201"))):
            result = _run(wq._answer_question("質問"))
        self.assertEqual(result["status"], "ok")   # 高速化後も照合 PASS
        self.assertEqual(calls[0], 0)              # 初回 turn は marker なし
        self.assertEqual(calls[1], 1)              # 2 turn 目は末尾に 1 個のみ


# ── 上限検査が並列実行の前に効くこと（対照・既存仕様の維持） ─────────────────
class TestCapsStillPrecedeExecution(unittest.TestCase):
    def test_over_per_turn_cap_never_dispatches(self):
        blocks = _blocks("1", "2", "3", "4", "5", "6")     # 6 > 5
        stub = SimpleNamespace(messages=SimpleNamespace(
            create=AsyncMock(return_value=_resp("tool_use", blocks))))
        load = AsyncMock(return_value=_heirs("201"))
        with patch.dict(os.environ, _ENV), \
             patch.object(wq, "_anthropic_client", lambda: stub), \
             patch.object(wq.souzoku_dash, "_load_heirs", load):
            result = _run(wq._answer_question("質問"))
        self.assertEqual(result["status"], "error")
        load.assert_not_called()                   # 並列化後も dispatch 非到達


if __name__ == "__main__":
    unittest.main()
