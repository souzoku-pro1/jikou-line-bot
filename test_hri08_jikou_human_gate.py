"""HRI-08｜時効側の人対応ゲート漏れ（裁定 G・G-2・本番稼働経路）

#260 の一覧で残った 3 箇所:
(a) heal による受領返信（main._process_line_event の冒頭）——App 21 の照会より前に走り、
    人対応中でも未返信の画像マーカーがあると受領返信が出ていた。
(b) ヒアリング返信 3 箇所——メモリ上のヒアリング中は App 21 の照会も人対応判定も
    飛ばされ、ヒアリング途中で人対応へ切り替えても AI 返信が続いた。
(c) 画像受領返信——判定がデバウンス（最長 90 秒）の前で、待機中に人対応へ切り替わった束は
    送られていた。

実装:
- 判定は chat_responder.is_human_mode に一本化（テキスト・画像・読解結果の送信直前）。
- (a) 人対応判定を heal の前へ。抑止時は App 28 に保留行（画像人対応保留:jikou）、解除後の
  最初の受信で人対応済行（画像人対応済:jikou）で閉じ、後出し送信しない（裁定 G-2）。
- (b) メモリ上のヒアリング中でも照会+判定。人対応中は送らず、状態遷移も進めない。
  セッションは維持（従来の人対応ブロックと同じ=会話履歴・hearing_completed に触れない）。
- (c) デバウンス後・送信の直前に束の代表が判定し直す。
- App 21 の照会失敗（HTTP 非 2xx=App21LookupError／通信例外）はすべて「送らない」側。
- 友だち追加時のあいさつは対象外（裁定 G-3 待ち）=従来どおり出る。
"""
import asyncio
import io
import os
import re
import unittest
from unittest.mock import AsyncMock, patch

from test_image_intake import _ENV, _FakeChatlog28, _delayed  # noqa: E402  App 28 フェイク

for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import chat_responder as cr  # noqa: E402
import main  # noqa: E402
from hub import image_analysis as ia  # noqa: E402
from hub import image_intake as ii  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402

USER = "U_hri08_jikou"
REPO = os.path.dirname(os.path.abspath(__file__))
HOLD = "画像人対応保留:jikou"
CLOSED = "画像人対応済:jikou"
RECEIPT = "画像受領済:jikou"


def _run(coro):
    return asyncio.run(coro)


def _rec(mode="自動", **extra):
    rec = {"$id": {"value": "10"}, "response_mode": {"value": mode},
           "status": {"value": "問い合わせ"}, "顧客名": {"value": "山田太郎"},
           "受信書類写真": {"value": []}}
    for k, v in extra.items():
        rec[k] = {"value": v}
    return rec


class _Base(unittest.TestCase):
    """App 21 は main.get_app21_record を差し替え（self.record が正・LOOKUP_FAIL で失敗）。
    App 28 はフェイク。顧客向け送信（reply / push）と Claude・通知だけ差し替える。"""

    def setUp(self):
        ii._pending.clear()
        ii._send_claims.clear()
        ia._claims.clear()
        for d in (main.conversation_histories, main.kintone_record_ids,
                  main.user_business_names):
            d.pop(USER, None)
            self.addCleanup(d.pop, USER, None)
        main.hearing_completed.discard(USER)
        self.addCleanup(main.hearing_completed.discard, USER)
        self.store = _FakeChatlog28()
        self.record = None
        self.lookup_fail = None                       # 例外を入れると照会失敗

        async def _get(user_id):
            if self.lookup_fail is not None:
                raise self.lookup_fail
            return dict(self.record) if self.record is not None else None
        self.reply = AsyncMock()                      # 顧客向け reply（ヒアリング・固定文言）
        self.push = AsyncMock(return_value=True)      # 顧客向け push（受領返信）
        self.ask = AsyncMock(return_value="ありがとうございます。①債権者名を教えてください")
        self.biz = AsyncMock(return_value=True)       # 弁護士通知（notify_business）
        self.admin = AsyncMock(return_value=True)     # 管理者通知（notify_admin_line）
        self.chatlog = AsyncMock()
        for p in (
                patch.object(hub_kintone, "create_record", self.store.create),
                patch.object(hub_kintone, "search_records", self.store.search),
                patch.object(ii, "DEBOUNCE_SEC", 0.03),
                patch.dict(os.environ, {"IMAGE_HEAL_DISABLED": "0", "AUTOREPLY_PAUSED": "0",
                                        "ATTORNEY_LINE_USER_ID": "Uattorney"}),
                patch.object(main, "get_app21_record", _get),
                patch.object(main, "_autoreply_paused", lambda: False),
                patch.object(main.autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)),
                patch.object(main, "_line_reply_with_fallback", self.reply),
                patch.object(ii, "push_text", self.push),
                patch.object(ii, "is_suppressed", AsyncMock(return_value=False)),
                patch.object(ii.image_analysis, "analyze_and_reply", AsyncMock(return_value="sent")),
                patch.object(main, "ask_claude", self.ask),
                patch.object(main, "get_recent_chat_history", AsyncMock(return_value=[])),
                patch.object(main, "save_to_chatlog", self.chatlog),
                patch.object(main, "save_to_approval_queue", AsyncMock()),
                patch.object(main, "_store_jikou_image", AsyncMock()),
                patch.object(main, "ATTORNEY_LINE_USER_ID", "Uattorney"),
                patch("hub.notify.notify_business", self.biz),
                patch.object(main.hub_notify, "notify_admin_line", self.admin)):
            p.start()
            self.addCleanup(p.stop)

    # helpers
    def text(self, body="こんにちは"):
        _run(main._process_line_event("rtok", USER, body))

    def image(self, event_id="ev1"):
        _run(main._process_line_image_event("rtok", USER, event_id))

    def seed_marker(self, event_id="IMG1"):
        _run(self.store.create(None, {
            "line_user_id": USER, "role": "user", "message": cr.IMAGE_INBOUND_MARKER,
            "category": ii.marker_category("jikou", event_id), "auto_sent": "no"}))

    def rows(self, category):
        return [r for r in self.store.rows if r.get("category") == category]

    def customer_sends(self):
        return self.reply.await_count + self.push.await_count

    def in_session(self):
        main.conversation_histories[USER] = [
            {"role": "user", "content": "こんにちは"},
            {"role": "assistant", "content": "①債権者名を教えてください"}]


# ── (a) heal による受領返信 ───────────────────────────────────────────────────────
class TestHealGate(_Base):
    def test_human_mode_with_unreplied_marker_sends_nothing_and_holds(self):
        self.record = _rec("人対応")
        self.seed_marker()
        self.text()
        self.assertEqual(self.customer_sends(), 0)          # 修正前: 受領返信が 1 件出ていた
        self.assertEqual(len(self.store.markers("jikou")), 1)   # マーカーは消費しない
        self.assertEqual(self.rows(RECEIPT), [])
        (hold,) = self.rows(HOLD)
        self.assertEqual(hold["message"], ii.IMAGE_HUMAN_HOLD_MARKER)
        self.assertEqual(self.rows(CLOSED), [])
        self.ask.assert_not_awaited()
        self.chatlog.assert_awaited_once_with(USER, "user", "こんにちは", "", "no")
        self.assertIn("【人対応中】", self.biz.await_args.args[1])

    def test_release_closes_held_marker_without_sending(self):
        self.record = _rec("人対応")
        self.seed_marker()
        self.text()
        self.record = _rec("自動")                           # 解除（kintone の手編集）
        self.text()                                          # 解除後の最初の受信
        self.push.assert_not_awaited()                       # 受領返信の後出しなし
        self.assertEqual(self.rows(RECEIPT), [])
        (closed,) = self.rows(CLOSED)
        self.assertEqual(closed["message"], ii.IMAGE_HUMAN_CLOSED_MARKER)
        self.assertEqual(len(self.store.markers("jikou")), 1)
        self.ask.assert_awaited_once()                       # このテキストの通常応答は従来どおり
        self.text()
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows(CLOSED)), 1)

    def test_unreplied_marker_without_hold_is_healed_as_before(self):
        self.record = _rec("自動")
        self.seed_marker()                                   # 送信失敗で残った未返信
        self.text()
        self.assertEqual(self.push.await_count, 1)
        self.assertEqual(self.push.await_args.args[1:], (USER, cr.IMAGE_RECEIPT_REPLY))
        self.assertEqual(len(self.rows(RECEIPT)), 1)
        self.assertEqual(self.rows(CLOSED), [])

    def test_no_record_heals_as_before(self):
        self.seed_marker()                                   # App 21 未作成=自動
        self.text()
        self.assertEqual(self.push.await_count, 1)

    def test_lookup_failure_sends_nothing_including_heal(self):
        for exc in (cr.App21LookupError("status=500"), RuntimeError("transport")):
            with self.subTest(exc=type(exc).__name__):
                self.setUp()
                self.seed_marker()
                self.lookup_fail = exc
                self.text()
                self.assertEqual(self.customer_sends(), 0)
                self.assertEqual(self.rows(HOLD), [])        # 判定不能=保留も閉鎖もしない
                self.assertEqual(self.rows(RECEIPT), [])
                self.ask.assert_not_awaited()
                self.chatlog.assert_awaited_once()
                self.assertIn("照会に失敗", self.admin.await_args.args[0])
                self.assertEqual(self.admin.await_args.kwargs["throttle_key"], "app21_lookup_failed")

    def test_gate_runs_before_heal(self):
        self.record = _rec("自動")
        order = []

        async def _get(user_id):
            order.append("app21")
            return dict(self.record)

        async def _close(*a):
            order.append("close_held")
            return False

        async def _heal(*a):
            order.append("heal")
            return False
        with patch.object(main, "get_app21_record", _get), \
             patch.object(main.image_intake, "close_held_markers", _close), \
             patch.object(main.image_intake, "heal_unreplied", _heal):
            self.text()
        self.assertEqual(order[:3], ["app21", "close_held", "heal"])


# ── (b) メモリ上のヒアリング中の人対応判定 ───────────────────────────────────────
class TestHearingSessionGate(_Base):
    def test_switch_to_human_mode_mid_session_stops_hearing_replies(self):
        self.record = _rec("自動")
        self.in_session()
        self.text("アコムです")
        self.assertEqual(self.ask.await_count, 1)            # 通常のヒアリング継続
        self.assertEqual(self.reply.await_count, 1)
        self.record = _rec("人対応")                        # 途中で人対応へ
        before = list(main.conversation_histories[USER])
        self.text("住所は川口市です")
        self.text("電話番号は…")
        self.assertEqual(self.ask.await_count, 1)            # 修正前: 続いていた
        self.assertEqual(self.reply.await_count, 1)          # 以後のヒアリング返信 0 件
        self.assertEqual(main.conversation_histories[USER], before)   # 状態遷移なし・セッション維持
        self.assertNotIn(USER, main.hearing_completed)
        self.assertNotIn(USER, main.kintone_record_ids)
        human_rows = [c for c in self.chatlog.await_args_list if c.args[3:] == ("", "no")]
        self.assertEqual(len(human_rows), 2)                 # 人対応中の 2 件は App 28 に記録
        self.assertEqual(sum("【人対応中】" in c.args[1] for c in self.biz.await_args_list), 2)
        self.record = _rec("自動")                           # 解除→同じセッションで再開
        self.text("電話番号は090…")
        self.assertEqual(self.ask.await_count, 2)            # 同じセッションで再開
        self.assertEqual(self.reply.await_count, 2)
        self.assertEqual(main.conversation_histories[USER], before)   # 履歴の追記は実物の ask_claude の責務（mock）

    def test_claude_outage_and_gate_demotion_paths_are_silent_in_human_mode(self):
        # ヒアリング経路の 3 送信箇所（通常・Claude 障害・送信ゲート降格）はゲートの後
        self.record = _rec("人対応")
        self.in_session()
        self.ask.side_effect = main.ClaudeUnavailableError("down")
        self.text("こんにちは")
        self.ask.assert_not_awaited()                        # ゲートで止まる=障害経路にも入らない
        self.assertEqual(self.customer_sends(), 0)

    def test_auto_mode_session_is_unchanged(self):
        self.record = _rec("自動")
        self.in_session()
        self.text("アコムです")
        self.ask.assert_awaited_once()
        self.reply.assert_awaited_once()
        self.biz.assert_not_awaited()

    def test_lookup_failure_mid_session_sends_nothing(self):
        self.record = _rec("自動")
        self.in_session()
        self.lookup_fail = cr.App21LookupError("status=502")
        self.text("アコムです")
        self.assertEqual(self.customer_sends(), 0)           # 修正前: 照会せず AI 返信していた
        self.ask.assert_not_awaited()
        self.admin.assert_awaited_once()

    def test_not_in_session_human_mode_still_silent(self):
        self.record = _rec("人対応")
        self.text("こんにちは")
        self.assertEqual(self.customer_sends(), 0)
        self.ask.assert_not_awaited()


# ── (c) 画像受領返信: デバウンス後・送信直前の再判定 ─────────────────────────────
class TestImageDebounceGate(_Base):
    def test_switch_to_human_during_debounce_sends_nothing_and_holds(self):
        self.record = _rec("自動")

        async def scenario():
            t = asyncio.ensure_future(main._process_line_image_event("t", USER, "ev1"))
            await asyncio.sleep(0.01)                        # デバウンス待ちの間に人対応へ
            self.record = _rec("人対応")
            await t
        _run(scenario())
        self.push.assert_not_awaited()                       # 修正前: 受領返信が出ていた
        self.assertEqual(len(self.store.markers("jikou")), 1)
        self.assertEqual(self.rows(RECEIPT), [])
        self.assertEqual(len(self.rows(HOLD)), 1)
        self.assertIn("【人対応中】", self.biz.await_args.args[1])

    def test_human_mode_before_debounce_holds(self):
        self.record = _rec("人対応")
        self.image()
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows(HOLD)), 1)            # 修正前: 保留行なし=解除後に heal が送っていた
        self.assertEqual(len(self.store.markers("jikou")), 1)

    def test_release_after_held_image_closes_without_sending(self):
        self.record = _rec("人対応")
        self.image()
        self.record = _rec("自動")
        self.text()                                          # 解除後の最初の受信（テキスト）
        self.push.assert_not_awaited()
        self.assertEqual(len(self.rows(CLOSED)), 1)
        self.assertEqual(self.rows(RECEIPT), [])

    def test_release_with_new_image_closes_held_then_processes_new(self):
        self.record = _rec("人対応")
        self.image("ev1")
        self.record = _rec("自動")
        self.image("ev2")                                    # 解除後の最初の受信が画像
        closed = self.rows(CLOSED)
        self.assertEqual(len(closed), 1)
        m1, m2 = self.store.markers("jikou")
        self.assertTrue(int(m1["$id"]) < int(closed[0]["$id"]) < int(m2["$id"]))
        self.push.assert_awaited_once()                      # 新しい画像は従来どおり
        self.assertEqual(len(self.rows(RECEIPT)), 1)

    def test_auto_mode_image_sends_receipt_as_before(self):
        self.record = _rec("自動")
        self.image()
        self.push.assert_awaited_once()
        self.assertEqual(self.push.await_args.args[2], cr.IMAGE_RECEIPT_REPLY)
        self.assertEqual(len(self.rows(RECEIPT)), 1)
        self.assertEqual(self.rows(HOLD), [])
        self.assertIn("【書類写真受領】", self.biz.await_args.args[1])

    def test_bundle_of_three_single_recheck_single_hold(self):
        self.record = _rec("自動")

        async def scenario():
            await asyncio.gather(
                main._process_line_image_event("t1", USER, "ev1"),
                _delayed(main._process_line_image_event("t2", USER, "ev2"), 0.005),
                _delayed(main._process_line_image_event("t3", USER, "ev3"), 0.01))
        with patch.object(main, "get_app21_record", side_effect=self._flip_after(3)):
            _run(scenario())
        self.push.assert_not_awaited()
        self.assertEqual(len(self.store.markers("jikou")), 3)
        self.assertEqual(len(self.rows(HOLD)), 1)            # 束の代表だけが保留を記録

    def _flip_after(self, n):
        calls = {"n": 0}

        async def _get(user_id):
            calls["n"] += 1
            return _rec("人対応" if calls["n"] > n else "自動")
        return _get

    def test_lookup_failure_at_send_time_sends_nothing(self):
        self.record = _rec("自動")

        async def scenario():
            t = asyncio.ensure_future(main._process_line_image_event("t", USER, "ev1"))
            await asyncio.sleep(0.01)
            self.lookup_fail = cr.App21LookupError("status=500")
            await t
        _run(scenario())
        self.push.assert_not_awaited()
        self.assertEqual(self.rows(HOLD), [])                # 判定不能=保留も書かない
        self.assertEqual(len(self.store.markers("jikou")), 1)   # 未返信のまま（次の受信で判定）

    def test_lookup_failure_before_marker_raises_and_sends_nothing(self):
        self.lookup_fail = cr.App21LookupError("status=500")
        with patch.object(main, "_notify_image_failure", AsyncMock()) as nf:
            with self.assertRaises(cr.App21LookupError):
                self.image()
        self.push.assert_not_awaited()
        self.assertEqual(self.store.markers("jikou"), [])
        nf.assert_awaited_once()


# ── 対象外の固定: 友だち追加のあいさつは従来どおり出る ─────────────────────────────
class TestFollowGreetingUntouched(unittest.TestCase):
    def test_follow_sends_even_when_app21_lookup_would_fail(self):
        send = AsyncMock()
        with patch.object(main, "_autoreply_paused", lambda: False), \
             patch.object(main.autoreply_stoplist, "is_suppressed", AsyncMock(return_value=False)), \
             patch.object(main, "get_app21_record", AsyncMock(side_effect=RuntimeError("must not be called"))), \
             patch.object(main, "_line_reply_with_fallback", send), \
             patch("hub.durable_inbound.mark_line_completed", AsyncMock()), \
             patch("hub.durable_inbound.mark_line_failed", AsyncMock()), \
             patch("hub.shindan_link.issue", AsyncMock(return_value="tok" * 12)):
            _run(main._process_follow_event("rt", USER, "01FOLLOWHRI08",
                                            "https://example.test", True))
        send.assert_awaited_once()
        self.assertEqual(send.await_args.args[:2], ("rt", USER))
        src = io.open(os.path.join(REPO, "main.py"), encoding="utf-8").read()
        body = src[src.index("async def _process_follow_event"):]
        body = body[:body.index("\nasync def ", 10)] if "\nasync def " in body[10:] else body
        self.assertNotIn("is_human_mode", body)


# ── 判定の一本化（別実装・直書きの禁止） ───────────────────────────────────────────
class TestSingleGateFunction(unittest.TestCase):
    def test_truth_table(self):
        f = cr.is_human_mode
        self.assertTrue(f({"response_mode": {"value": "人対応"}}))
        for rec in (None, {}, {"response_mode": {"value": ""}},
                    {"response_mode": {"value": "自動"}}, {"response_mode": None}):
            self.assertFalse(f(rec))

    def test_no_direct_comparison_outside_the_function(self):
        for rel in ("main.py", "chat_responder.py"):
            src = io.open(os.path.join(REPO, rel), encoding="utf-8").read()
            hits = [m.start() for m in re.finditer(r'==\s*"人対応"', src)]
            self.assertEqual(hits, [], rel)                  # 直書きの比較なし
        self.assertEqual(cr.HUMAN_MODE_VALUE, "人対応")
        cr_src = io.open(os.path.join(REPO, "chat_responder.py"), encoding="utf-8").read()
        self.assertIn("== HUMAN_MODE_VALUE", cr_src[cr_src.index("def is_human_mode"):][:700])
        self.assertEqual(cr_src.count("HUMAN_MODE_VALUE"), 2)   # 定義 1+関数内 1
        src = io.open(os.path.join(REPO, "main.py"), encoding="utf-8").read()
        self.assertNotIn('"response_mode"', src.replace("HUMAN_MODE", ""))   # 欄名の直参照なし
        self.assertEqual(src.count("is_human_mode("), 4)     # テキスト・紐付け後・画像 2 箇所
        ia_src = io.open(os.path.join(REPO, "hub/image_analysis.py"), encoding="utf-8").read()
        blocked = ia_src[ia_src.index("def _blocked("):ia_src.index("async def _store_creditor_names")]
        self.assertIn("is_human_mode(", blocked)             # 読解結果の送信直前確認も同じ関数

    def test_hold_and_closed_rows_hidden_from_history(self):
        records = [
            {"role": {"value": "user"}, "message": {"value": ii.IMAGE_HUMAN_CLOSED_MARKER}},
            {"role": {"value": "user"}, "message": {"value": ii.IMAGE_HUMAN_HOLD_MARKER}},
            {"role": {"value": "assistant"}, "message": {"value": "返信"}},
            {"role": {"value": "user"}, "message": {"value": cr.IMAGE_INBOUND_MARKER}},
        ]

        class _Resp:
            is_success = True

            def json(self):
                return {"records": records}

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, *a, **k):
                return _Resp()
        with patch.object(cr.httpx, "AsyncClient", lambda *a, **k: _Client()), \
             patch.object(cr, "APP_CHATLOG", "28"), patch.object(cr, "TOKEN_CHATLOG", "d"):
            history = _run(cr.get_recent_chat_history(USER))
        self.assertEqual([h["content"] for h in history], [cr.IMAGE_INBOUND_MARKER, "返信"])

    def test_get_app21_record_raises_on_http_failure(self):
        class _Resp:
            is_success = False
            status_code = 502
            text = "bad gateway"

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, *a, **k):
                return _Resp()
        with patch.object(cr.httpx, "AsyncClient", lambda *a, **k: _Client()), \
             patch.object(cr, "_SUBDOMAIN", "testsub"), patch.object(cr, "_APP21_TOKEN", "t"), \
             patch.object(cr, "_APP21_ID", "21"):
            with self.assertRaises(cr.App21LookupError):
                _run(cr.get_app21_record(USER))


if __name__ == "__main__":
    unittest.main()
