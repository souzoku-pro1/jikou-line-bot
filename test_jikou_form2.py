"""JIKOU-FORM-2: 受付番号による LINE 紐付けとフォーム回答の引き継ぎの固定。

固定する仕様（票の逐語）:
- 検知: App 21 に未紐付け（get_app21_record が None）のユーザーからの
  テキストが「6 桁の数字のみ」（前後空白・全角数字は正規化して許容）のとき
  紐付け処理へ。既にレコードがあるユーザーの 6 桁数字は通常メッセージ。
  挿入位置は pause/停止リスト判定の後
- 照合: App 21 を 受付番号=N かつ LINEユーザーID="" かつ 受付チャネル=フォーム
  で照会。ちょうど 1 件かつ作成日時が TTL（30 日）以内なら該当。0 件・複数件・
  期限切れは不該当（固定文言 B・作用 0）
- 紐付け: LINEユーザーID に userId を $revision CAS で書込（409=cas_lost・
  作用 0・不該当扱い）。成功後は同一ターンで通常のヒアリングへ流し、既知項目
  台帳がフォーム 4 項目を既知として注入・AI へ引き継ぎ直後の旨を一度だけ注入。
  AI 失敗時は固定文言 A のみ返信
- 総当たり対策: userId 別 5 回/60 分。超過後は無言+弁護士通知 1 回
  （固定文言・userId 先頭のみ）
- 二重 create 抑止: ヒアリング完了時の KINTONE_RECORD マーカー処理は、
  紐付け済みレコードがあれば create せず update へ振り替え。新規客は従来どおり
- 弁護士通知（紐付け成功）:「【フォーム紐付け】受付番号:xxxxxx → レコード番号:N」
- 固定文言 A/B は sha256 pin（司令塔案・大野裁定で差し替え可）
- App 21 書込は plain 値契約（fake は _wrap 境界を模す）
"""

import asyncio
import hashlib
import os
import re
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "kintone-token",
    "DOCUMENT_WEBHOOK_SECRET": "doc-secret",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "APP_CHATLOG": "28", "TOKEN_CHATLOG": "d",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import main  # noqa: E402
from hub import form_link as fl  # noqa: E402
from hub import kintone as hub_kintone  # noqa: E402

USER = "Uform2user0000000000000000000001"
NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=timezone.utc)


def _run(coro):
    return asyncio.run(coro)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── 凍結文言 pin ────────────────────────────────────────────────────────────────
PIN_A = "00d46c93a508ce7c3cc9a26157fe5a73a32bb8a651d3492c8f6ef87e41d363b0"
PIN_B = "36d937f549106e2d9a5383557ee661f7e4a8c3ace46d43374ca1564d49309fd4"


class TestFrozenTextsAndNumbers(unittest.TestCase):
    def test_reply_pins(self):
        self.assertEqual(hashlib.sha256(
            fl.REPLY_LINKED_FALLBACK.encode("utf-8")).hexdigest(), PIN_A)
        self.assertEqual(hashlib.sha256(
            fl.REPLY_NOT_MATCHED.encode("utf-8")).hexdigest(), PIN_B)

    def test_numbers_pinned(self):
        self.assertEqual(fl.RECEIPT_TTL_DAYS, 30)
        self.assertEqual(fl.ATTEMPT_LIMIT, 5)
        self.assertEqual(fl.ATTEMPT_WINDOW_SECONDS, 3600)
        self.assertEqual(fl.MAX_ATTEMPT_BUCKETS, 5000)


# ── 検知（6 桁のみ・正規化） ─────────────────────────────────────────────────────
class TestDetect(unittest.TestCase):
    def test_detected(self):
        for text in ("123456", " 123456 ", "　１２３４５６　", "１２３４５６",
                     "123456\n", "０１２３４５"):
            with self.subTest(text=repr(text)):
                self.assertEqual(fl.detect_receipt_number(text),
                                 fl.detect_receipt_number(text.strip()))
                self.assertRegex(fl.detect_receipt_number(text), r"^[0-9]{6}$")
        self.assertEqual(fl.detect_receipt_number("１２３４５６"), "123456")
        self.assertEqual(fl.detect_receipt_number("０１２３４５"), "012345")

    def test_not_detected(self):
        for text in ("1234567", "12345", "123456です", "受付番号 123456",
                     "12 3456", "abc123", "", "   ", "１２３４５６７",
                     "123456\n789"):
            with self.subTest(text=repr(text)):
                self.assertIsNone(fl.detect_receipt_number(text))


# ── App 21 の in-memory fake（_wrap 境界+$revision CAS を模す） ──────────────────
class _FakeApp21:
    def __init__(self, loose_query: bool = False):
        self.rows: dict[str, dict] = {}
        self.loose_query = loose_query   # True=受付番号のみで返す（module 側再検査の検証用）
        self.search_calls: list[str] = []
        self.update_calls: list[tuple] = []

    def add(self, rid: str, number: str, *, user_id: str = "",
            channel: str = "フォーム", created: datetime = NOW,
            revision: str = "3", **extra):
        rec = {
            "$id": {"value": rid}, "$revision": {"value": revision},
            "受付番号": {"value": number}, "LINEユーザーID": {"value": user_id},
            "受付チャネル": {"value": channel},
            "作成日時": {"value": _iso(created)},
            "status": {"value": "問い合わせ"},
            "受信書類写真": {"value": []},
            "診断パターン": {"value": "A"},
            "問い合わせ業者名": {"value": "フォーム債権者株式会社"},
            "借入時期_テキスト": {"value": "5年以上前"},
            "最終返済日_テキスト": {"value": "5年以上前"},
            "裁判所書類": {"value": "何も届いていない"},
        }
        for k, v in extra.items():
            rec[k] = {"value": v}
        self.rows[rid] = rec
        return rec

    @staticmethod
    def _reject_double_wrap(fields):
        for code, v in (fields or {}).items():
            if isinstance(v, dict) and "value" in v:
                raise AssertionError(f"double-wrapped payload: {code}={v!r}")

    async def search(self, app, query, fields=None):
        self.search_calls.append(query)
        m = re.search(r'受付番号 = "([0-9]{6})"', query)
        assert m, query
        number = m.group(1)
        out = []
        for r in self.rows.values():
            if r["受付番号"]["value"] != number:
                continue
            if not self.loose_query:
                if r["LINEユーザーID"]["value"] != "":
                    continue
                if r["受付チャネル"]["value"] != "フォーム":
                    continue
            if fields:
                out.append({k: v for k, v in r.items() if k in fields})
            else:
                out.append(dict(r))
        return out

    async def update(self, app, record_id, fields, revision=None):
        self._reject_double_wrap(fields)
        self.update_calls.append((record_id, dict(fields), revision))
        row = self.rows[record_id]
        if revision is not None and str(revision) != row["$revision"]["value"]:
            raise hub_kintone.KintoneConflict(409, "GAIA_CO02", "conflict")
        for k, v in fields.items():
            row[k] = {"value": v}
        row["$revision"] = {"value": str(int(row["$revision"]["value"]) + 1)}

    async def get_by_user(self, user_id):
        for r in self.rows.values():
            if r["LINEユーザーID"]["value"] == user_id:
                return dict(r)
        return None


# ── 照合・紐付け（module 単体） ──────────────────────────────────────────────────
class TestMatchAndBind(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeApp21()
        fl._attempts.clear()
        self.addCleanup(fl._attempts.clear)
        self.notify = AsyncMock(return_value=True)
        patches = [
            patch.object(fl.hub_kintone, "search_records", self.fake.search),
            patch.object(fl.hub_kintone, "update_record", self.fake.update),
            patch.object(fl.notify, "notify_business", self.notify),
            patch.dict(os.environ, {"ATTORNEY_LINE_USER_ID": "U_attorney"}),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def _link(self, number="123456", now=None):
        return _run(fl.try_link(USER, number, now=now or NOW.timestamp()))

    def test_query_shape_pinned(self):
        self.fake.add("10", "123456")
        self._link()
        q = self.fake.search_calls[0]
        self.assertIn('受付番号 = "123456"', q)
        self.assertIn('LINEユーザーID = ""', q)
        self.assertIn('受付チャネル in ("フォーム")', q)

    def test_exactly_one_links_with_cas_and_notify(self):
        self.fake.add("10", "123456", revision="7")
        outcome, rid = self._link()
        self.assertEqual((outcome, rid), ("linked", "10"))
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], USER)
        self.assertEqual(self.fake.update_calls,
                         [("10", {"LINEユーザーID": USER}, "7")])   # plain 値+CAS
        self.notify.assert_awaited_once_with(
            "U_attorney", "【フォーム紐付け】受付番号:123456 → レコード番号:10")

    def test_zero_rows_not_matched(self):
        self.assertEqual(self._link(), ("not_matched", None))
        self.assertEqual(self.fake.update_calls, [])
        self.notify.assert_not_awaited()

    def test_two_rows_not_matched(self):
        self.fake.add("10", "123456")
        self.fake.add("11", "123456")
        self.assertEqual(self._link(), ("not_matched", None))
        self.assertEqual(self.fake.update_calls, [])
        for rid in ("10", "11"):
            self.assertEqual(self.fake.rows[rid]["LINEユーザーID"]["value"], "")

    def test_expired_not_matched(self):
        self.fake.add("10", "123456", created=NOW - timedelta(days=30, seconds=1))
        self.assertEqual(self._link(), ("not_matched", None))
        self.assertEqual(self.fake.update_calls, [])
        # TTL ちょうど（30 日以内）は該当
        self.fake.rows.clear()
        self.fake.add("11", "123456", created=NOW - timedelta(days=30))
        self.assertEqual(self._link()[0], "linked")

    def test_channel_not_form_and_user_nonempty_rejected_defensively(self):
        # fake が受付番号のみで返しても module 側の再検査で不該当
        self.fake.loose_query = True
        self.fake.add("10", "123456", channel="LINE")
        self.assertEqual(self._link(), ("not_matched", None))
        self.fake.rows.clear()
        self.fake.add("11", "123456", user_id="Uother")
        self.assertEqual(self._link(), ("not_matched", None))
        self.assertEqual(self.fake.update_calls, [])
        self.assertEqual(self.fake.rows["11"]["LINEユーザーID"]["value"], "Uother")

    def test_cas_409_is_not_matched_zero_effect(self):
        rec = self.fake.add("10", "123456", revision="3")

        async def _search(app, query, fields=None):
            rows = await self.fake.search(app, query, fields)
            rows[0]["$revision"] = {"value": "2"}   # 取得後に他者が更新した状態
            return rows
        with patch.object(fl.hub_kintone, "search_records", _search):
            self.assertEqual(self._link(), ("not_matched", None))
        self.assertEqual(rec["LINEユーザーID"]["value"], "")
        self.notify.assert_not_awaited()

    def test_search_failure_is_not_matched(self):
        err = hub_kintone.KintoneError(500, "GAIA_XX", "down")
        with patch.object(fl.hub_kintone, "search_records",
                          AsyncMock(side_effect=err)):
            self.assertEqual(self._link(), ("not_matched", None))

    def test_attempt_limit_silent_and_notify_once(self):
        t0 = NOW.timestamp()
        for i in range(fl.ATTEMPT_LIMIT):
            self.assertEqual(self._link("000000", now=t0 + i)[0], "not_matched")
        self.notify.assert_not_awaited()
        self.assertEqual(self._link("000000", now=t0 + 10)[0], "silent")
        self.notify.assert_awaited_once()
        text = self.notify.await_args.args[1]
        self.assertIn("要確認", text)
        self.assertIn(USER[:6], text)
        self.assertNotIn(USER, text)                 # userId 全体は載せない
        self.assertNotIn("000000", text)             # 番号も載せない
        self.assertEqual(self._link("000000", now=t0 + 20)[0], "silent")
        self.notify.assert_awaited_once()            # 2 回目以降は通知しない
        # 超過中は該当レコードがあっても照合しない
        self.fake.add("10", "123456")
        self.assertEqual(self._link("123456", now=t0 + 30)[0], "silent")
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], "")
        # 窓満了で解除
        self.assertEqual(self._link("123456", now=t0 + fl.ATTEMPT_WINDOW_SECONDS + 1)[0],
                         "linked")

    def test_attempt_buckets_bounded(self):
        from collections import OrderedDict
        self.assertIsInstance(fl._attempts, OrderedDict)
        with patch.object(fl, "MAX_ATTEMPT_BUCKETS", 3):
            for i in range(5):
                fl.record_attempt(f"U{i}", NOW.timestamp())
                self.assertLessEqual(len(fl._attempts), 3)


# ── main._process_line_event との結線 ────────────────────────────────────────────
class _FlowBase(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeApp21()
        fl._attempts.clear()
        self.addCleanup(fl._attempts.clear)
        for d in (main.conversation_histories, main.kintone_record_ids,
                  main.user_business_names):
            d.pop(USER, None)
            self.addCleanup(d.pop, USER, None)
        main.hearing_completed.discard(USER)
        self.addCleanup(main.hearing_completed.discard, USER)
        self.reply = AsyncMock()
        self.log = AsyncMock()
        self.ask = AsyncMock(return_value="ありがとうございます。続きをお伺いします。")
        self.create = AsyncMock(return_value="900")
        self.update = AsyncMock()
        self.notify = AsyncMock(return_value=True)
        self.outage = AsyncMock()
        patches = [
            patch.object(main.autoreply_stoplist, "is_suppressed",
                         AsyncMock(return_value=False)),
            patch.object(main, "get_app21_record", self.fake.get_by_user),
            patch.object(main, "get_recent_chat_history",
                         AsyncMock(return_value=[])),
            patch.object(main, "ask_claude", self.ask),
            patch.object(main, "_line_reply_with_fallback", self.reply),
            patch.object(main, "save_to_chatlog", self.log),
            patch.object(main, "post_to_kintone", self.create),
            patch.object(main, "update_kintone_record", self.update),
            patch.object(main, "handle_claude_outage", self.outage),
            patch.object(fl.hub_kintone, "search_records", self.fake.search),
            patch.object(fl.hub_kintone, "update_record", self.fake.update),
            patch.object(fl.notify, "notify_business", self.notify),
            patch.dict(os.environ, {"ATTORNEY_LINE_USER_ID": "U_attorney",
                                    "AUTOREPLY_PAUSED": "0"}),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    def run_event(self, text, user=USER):
        _run(main._process_line_event("tok", user, text))


class TestLinkFlow(_FlowBase):
    def test_link_then_hearing_same_turn_with_known_items(self):
        self.fake.add("10", "123456")
        self.run_event(" １２３４５６ ")
        # 紐付け
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], USER)
        self.notify.assert_awaited_once_with(
            "U_attorney", "【フォーム紐付け】受付番号:123456 → レコード番号:10")
        self.assertEqual(main.kintone_record_ids[USER], "10")
        # 同一ターンでヒアリングへ（引き継ぎ注入 1 回+既知項目にフォーム 4 項目）
        self.ask.assert_awaited_once()
        kw = self.ask.await_args.kwargs
        self.assertTrue(kw.get("form_handover"))
        known = kw["known_items"]
        self.assertEqual(known["債権者名"], "フォーム債権者株式会社")
        self.assertEqual(known["借入時期"], "5年以上前")
        self.assertEqual(known["最終返済日"], "5年以上前")
        self.assertEqual(known["裁判所書類の有無"], "何も届いていない")
        self.reply.assert_awaited_once()
        self.assertEqual(self.reply.await_args.args[2],
                         "ありがとうございます。続きをお伺いします。")

    def test_link_in_memory_session_without_record(self):
        # メモリ上のヒアリング中でも App 21 未紐付けなら 6 桁は紐付けへ
        main.conversation_histories[USER] = [
            {"role": "user", "content": "こんにちは"},
            {"role": "assistant", "content": "①債権者名を教えてください"}]
        self.fake.add("10", "123456")
        self.run_event("123456")
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], USER)
        self.assertTrue(self.ask.await_args.kwargs.get("form_handover"))

    def test_not_matched_fixed_reply_b_no_effects(self):
        self.run_event("123456")
        self.reply.assert_awaited_once()
        self.assertEqual(self.reply.await_args.args[2], fl.REPLY_NOT_MATCHED)
        self.ask.assert_not_awaited()
        self.create.assert_not_awaited()
        self.notify.assert_not_awaited()
        self.assertEqual(self.fake.update_calls, [])
        # 存在有無を漏らさない: 期限切れ・複数件でも同一文言
        self.reply.reset_mock()
        self.fake.add("10", "123456", created=NOW - timedelta(days=60))
        self.run_event("123456")
        self.assertEqual(self.reply.await_args.args[2], fl.REPLY_NOT_MATCHED)
        # 応答にレコード番号・他人の情報を載せない
        self.assertNotIn("10", fl.REPLY_NOT_MATCHED)

    def test_existing_record_user_six_digits_is_normal_message(self):
        self.fake.add("10", "123456", user_id=USER)   # 既に紐付け済み
        with patch.object(fl, "try_link", AsyncMock()) as try_link:
            self.run_event("654321")
        try_link.assert_not_awaited()
        self.ask.assert_awaited_once()                 # 通常ヒアリング
        self.assertFalse(self.ask.await_args.kwargs.get("form_handover"))

    def test_attempt_exceeded_silent_and_one_notify(self):
        for _ in range(fl.ATTEMPT_LIMIT):
            self.run_event("000000")
        self.assertEqual(self.reply.await_count, fl.ATTEMPT_LIMIT)
        self.notify.assert_not_awaited()
        self.run_event("000000")
        self.assertEqual(self.reply.await_count, fl.ATTEMPT_LIMIT)   # 無言
        self.notify.assert_awaited_once()
        self.assertIn(USER[:6], self.notify.await_args.args[1])
        self.run_event("000000")
        self.notify.assert_awaited_once()                             # 1 回のみ
        self.ask.assert_not_awaited()

    def test_ai_failure_on_handover_turn_replies_fixed_a(self):
        self.fake.add("10", "123456")
        self.ask.side_effect = main.ClaudeUnavailableError("down")
        self.run_event("123456")
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], USER)
        self.reply.assert_awaited_once()
        self.assertEqual(self.reply.await_args.args[2], fl.REPLY_LINKED_FALLBACK)
        self.outage.assert_not_awaited()
        self.assertEqual(main.conversation_histories.get(USER, []), [])

    def test_ai_failure_without_handover_uses_existing_outage_path(self):
        self.ask.side_effect = main.ClaudeUnavailableError("down")
        self.run_event("こんにちは")
        self.outage.assert_awaited_once()
        self.reply.assert_not_awaited()

    def test_gate_order_pause_and_stoplist_before_link(self):
        self.fake.add("10", "123456")
        with patch.object(main, "_handle_paused_inbound", AsyncMock()) as paused, \
                patch.dict(os.environ, {"AUTOREPLY_PAUSED": "1"}):
            self.run_event("123456")
        paused.assert_awaited_once()
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], "")
        with patch.object(main.autoreply_stoplist, "is_suppressed",
                          AsyncMock(return_value=True)), \
                patch.object(main, "_handle_suppressed_inbound", AsyncMock()) as sup:
            self.run_event("123456")
        sup.assert_awaited_once()
        self.assertEqual(self.fake.rows["10"]["LINEユーザーID"]["value"], "")
        self.notify.assert_not_awaited()


class TestHandoverPrompt(unittest.TestCase):
    def setUp(self):
        main.conversation_histories.pop(USER, None)
        self.addCleanup(main.conversation_histories.pop, USER, None)

    def _system_for(self, **kw):
        captured = {}

        async def _fake_create(client, **kwargs):
            captured.update(kwargs)
            return object()
        with patch.object(main, "create_message_with_fallback", _fake_create), \
                patch.object(main, "extract_text", lambda r: "ok"):
            _run(main.ask_claude(USER, "123456",
                                 known_items={"債権者名": "X社"}, **kw))
        return captured["system"]

    def test_handover_note_injected_once_only_when_flagged(self):
        s = self._system_for(form_handover=True)
        self.assertIn(fl.HANDOVER_PROMPT_NOTE, s)
        self.assertEqual(s.count(fl.HANDOVER_PROMPT_NOTE), 1)
        self.assertIn("債権者名: X社", s)
        self.assertIn("再質問禁止", s)
        s2 = self._system_for()
        self.assertNotIn(fl.HANDOVER_PROMPT_NOTE, s2)
        self.assertTrue(s2.startswith(main.SYSTEM_PROMPT))   # 本体は不変


class TestDoubleCreateSuppression(_FlowBase):
    MARKER = ('了解しました。\n[KINTONE_RECORD]\n{"問い合わせ業者名": "フォーム債権者株式会社",'
              ' "借入時期_テキスト": "5年以上前", "最終返済日_テキスト": "5年以上前",'
              ' "裁判所書類": "何も届いていない", "信用情報確認": "いいえ"}\n'
              '[/KINTONE_RECORD]')

    def test_bound_user_marker_updates_instead_of_create(self):
        self.fake.add("10", "123456", user_id=USER)      # 紐付け済み
        self.ask.return_value = self.MARKER
        self.run_event("いいえ")
        self.create.assert_not_awaited()
        self.update.assert_awaited_once()
        rid, fields = self.update.await_args.args
        self.assertEqual(rid, "10")
        self.assertEqual(fields["信用情報確認"], "いいえ")
        self.assertNotIn("LINEユーザーID", fields)
        self.assertNotIn("status", fields)                 # 人の変更を上書きしない
        self.assertEqual(main.kintone_record_ids[USER], "10")
        self.assertEqual(self.reply.await_args.args[2], "了解しました。")

    def test_new_customer_marker_still_creates(self):
        self.ask.return_value = self.MARKER
        self.run_event("いいえ")
        self.create.assert_awaited_once()
        rec = self.create.await_args.args[0]
        self.assertEqual(rec["LINEユーザーID"], USER)
        self.assertEqual(rec["status"], "問い合わせ")
        self.update.assert_not_awaited()
        self.assertEqual(main.kintone_record_ids[USER], "900")


if __name__ == "__main__":
    unittest.main()
