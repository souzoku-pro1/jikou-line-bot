"""SOUZOKU-HOUKI-H3: 相続放棄ヒアリング Bot の固定。

固定する仕様（正本 souzoku-houki/02 §1-2〔有効部分〕+ 10-unit-02 §2）:
- record_hearing の逐次 upsert（App 40=APP_HOUKI/TOKEN_HOUKI）: 新規作成
  （受付チャネル=LINE・status=問い合わせ）・既存は空欄のみ更新（非空を
  上書きしない）・許可フィールド閉集合外（弁護士専権/サーバ計算欄）は
  構造的に書けない。
- 日付整合検証: 死亡日と知った日の順序・未来日・形式。矛盾時は日付 3 欄を
  書かず tool_result で聞き直し・2 回失敗で危険類型フラグ「申告内容の矛盾」
  （App 40 の実選択肢値）+承認キュー（App 29 共用）。
- status 遷移の入口: 必須項目充足+hearing_done で 問い合わせ（/空）→
  電話判断待ち の一方向のみ（電話推奨度判定・通知は H-4）。
- 送信ゲート: 第 2 世代ガード機構共用（サニタイズ・300 字/質問数・名乗り/
  記号/無根拠語）+route=houki_hearing（根拠集合空=FAQ 根拠語で降格）。
  違反は App 29+確認中定型。送信は HOUKI_CHANNEL 限定。
- 停止リスト（App 39 共用）・全業務ブレーキ（AUTOREPLY_PAUSED 共用）。
- HOUKI_PROFILE: ヒアリング部分のみ実値・顧客対応部分は fail-closed
  プレースホルダ（auto_send_categories=空集合）。
"""

import asyncio
import datetime
import os
import unittest
from types import SimpleNamespace
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
    "APP_HOUKI": "40", "TOKEN_HOUKI": "d",
    "HOUKI_LINE_CHANNEL_SECRET": "houki_secret",
    "HOUKI_LINE_CHANNEL_ACCESS_TOKEN": "houki_token",
}
for _k, _v in _ENV.items():
    os.environ.setdefault(_k, _v)

import chat_responder as cr  # noqa: E402
from houki_bot import hearing  # noqa: E402
from hub import houki_case_store as store  # noqa: E402
from hub import houki_profile as hp  # noqa: E402
from hub.line_channel import HOUKI_CHANNEL  # noqa: E402


def _run(coro):
    return asyncio.run(coro)


# ── App 40 の in-memory フェイク（hub.kintone の使用 3 API を模す） ─────────────
class _FakeApp40:
    def __init__(self):
        self.rows: dict[str, dict] = {}
        self._id = 0

    async def search_records(self, app, query, fields=None):
        uid = query.split('"')[1]
        found = [r for r in self.rows.values()
                 if r.get("LINEユーザーID", {}).get("value") == uid]
        found.sort(key=lambda r: -int(r["$id"]["value"]))
        return found[:1]

    async def create_record(self, app, fields):
        self._id += 1
        rid = str(self._id)
        rec = dict(fields)
        rec["$id"] = {"value": rid}
        self.rows[rid] = rec
        return rid

    async def update_record(self, app, record_id, fields):
        self.rows[str(record_id)].update(fields)

    def field(self, rid, code):
        return (self.rows[str(rid)].get(code) or {}).get("value")


def _patch_store(fake):
    return (patch.object(store.kintone, "search_records", fake.search_records),
            patch.object(store.kintone, "create_record", fake.create_record),
            patch.object(store.kintone, "update_record", fake.update_record))


class _StoreBase(unittest.TestCase):
    def setUp(self):
        self.fake = _FakeApp40()
        self._patches = _patch_store(self.fake)
        for p in self._patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self._patches])


class TestDateValidation(unittest.TestCase):
    TODAY = datetime.date(2026, 8, 26)

    def _v(self, fields):
        return store.validate_hearing_dates(fields, today=self.TODAY)

    def test_ok_and_partial(self):
        self.assertEqual(self._v({}), [])
        self.assertEqual(self._v({"死亡日_申告": "2026-05-01"}), [])
        self.assertEqual(self._v({"死亡日_申告": "2026-05-01",
                                  "死亡を知った日_申告": "2026-05-01",
                                  "相続人と知った日_申告": "2026-06-10"}), [])

    def test_knew_death_before_death(self):
        v = self._v({"死亡日_申告": "2026-05-02",
                     "死亡を知った日_申告": "2026-05-01"})
        self.assertTrue(any("死亡を知った日_申告が死亡日_申告より前" in x
                            for x in v))

    def test_knew_heir_before_death_and_before_knew_death(self):
        v = self._v({"死亡日_申告": "2026-05-02",
                     "相続人と知った日_申告": "2026-05-01"})
        self.assertTrue(any("相続人と知った日_申告が死亡日_申告より前" in x
                            for x in v))
        v = self._v({"死亡を知った日_申告": "2026-06-01",
                     "相続人と知った日_申告": "2026-05-20"})
        self.assertTrue(any("死亡を知った日_申告より前" in x for x in v))

    def test_future_and_format(self):
        v = self._v({"死亡日_申告": "2026-09-01"})
        self.assertTrue(any("未来日" in x for x in v))
        v = self._v({"死亡日_申告": "2026-05頃"})
        self.assertTrue(any("形式不正" in x for x in v))


class TestSplitValidFields(unittest.TestCase):
    def test_closed_set_and_empty_dropped(self):
        fields, problems = store.split_valid_fields({
            "顧客名": "山田太郎",
            "住所": "  ",                       # 空→落ちる
            "起算日_確定": "2026-05-01",        # 弁護士専権→落ちる
            "status": "受任",                   # サーバ/弁護士管理→落ちる
            "法定満了日": "2026-08-01",         # サーバ計算→落ちる
        })
        self.assertEqual(problems, [])
        self.assertEqual(fields, {"顧客名": "山田太郎"})

    def test_date_trio_dropped_on_problem(self):
        fields, problems = store.split_valid_fields({
            "死亡日_申告": "2026-05-02",
            "死亡を知った日_申告": "2026-05-01",
            "顧客名": "山田太郎",
        }, today=datetime.date(2026, 8, 26))
        self.assertTrue(problems)
        self.assertEqual(fields, {"顧客名": "山田太郎"})   # 日付は両方落ちる

    def test_attorney_only_fields_not_writable(self):
        # 弁護士専権・サーバ計算欄が閉集合に**含まれない**ことを個別 pin
        for code in ("起算日_確定", "起算点確定済", "起算点メモ", "受任判断",
                     "電話要否", "電話推奨度", "法定満了日", "社内締切日",
                     "提出目標日", "残日数", "熟慮期間期限", "status",
                     "単純承認事由フラグ", "本人確認ステータス"):
            self.assertNotIn(code, store.HEARING_WRITABLE_FIELDS)


class TestUpsert(_StoreBase):
    def test_create_sets_channel_and_status(self):
        rid = _run(store.upsert_case_fields("U_h1", {"顧客名": "山田"}, None))
        self.assertEqual(self.fake.field(rid, "受付チャネル"), "LINE")
        self.assertEqual(self.fake.field(rid, "status"), "問い合わせ")
        self.assertEqual(self.fake.field(rid, "LINEユーザーID"), "U_h1")
        self.assertEqual(self.fake.field(rid, "顧客名"), "山田")

    def test_update_only_empty_fields(self):
        rid = _run(store.upsert_case_fields(
            "U_h2", {"顧客名": "山田", "住所": "川口市"}, None))
        existing = self.fake.rows[rid]
        _run(store.upsert_case_fields(
            "U_h2", {"顧客名": "別名で上書きしようとする", "電話番号": "090"},
            existing))
        self.assertEqual(self.fake.field(rid, "顧客名"), "山田")   # 非空は不変
        self.assertEqual(self.fake.field(rid, "電話番号"), "090")  # 空欄は埋まる

    def test_append_creditors_dedup(self):
        rid = _run(store.upsert_case_fields("U_h3", {}, None))
        added = _run(store.append_creditors(rid, self.fake.rows[rid],
                                            ["A社", "B社", "A社", ""]))
        self.assertEqual(added, 2)
        added = _run(store.append_creditors(rid, self.fake.rows[rid],
                                            ["B社", "C社"]))
        self.assertEqual(added, 1)
        rows = self.fake.field(rid, "債権者一覧")
        names = [r["value"]["債権者名"]["value"] for r in rows]
        self.assertEqual(names, ["A社", "B社", "C社"])
        self.assertEqual(rows[0]["value"]["通知要否"]["value"], "未確認")

    def test_mark_date_mismatch_flag_idempotent(self):
        rid = _run(store.upsert_case_fields("U_h4", {}, None))
        self.assertTrue(_run(store.mark_date_mismatch_flag(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])
        self.assertFalse(_run(store.mark_date_mismatch_flag(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])

    def test_status_promotion_one_way(self):
        rid = _run(store.upsert_case_fields("U_h5", {}, None))
        self.assertTrue(_run(store.promote_status_to_phone_triage(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "status"), "電話判断待ち")
        # 受任 等の他 status からは絶対に動かさない
        self.fake.rows[rid]["status"] = {"value": "受任"}
        self.assertFalse(_run(store.promote_status_to_phone_triage(
            rid, self.fake.rows[rid])))
        self.assertEqual(self.fake.field(rid, "status"), "受任")


# ── モデル応答のフェイク ────────────────────────────────────────────────────────
def _text_response(text):
    return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


def _tool_response(tool_input, text=""):
    blocks = []
    if text:
        blocks.append(SimpleNamespace(type="text", text=text))
    blocks.append(SimpleNamespace(type="tool_use", name="record_hearing",
                                  id="tu1", input=tool_input))
    return SimpleNamespace(content=blocks)


class _HearingBase(_StoreBase):
    def setUp(self):
        super().setUp()
        self.uid = f"U_houki_{self.id().rsplit('.', 1)[-1][:20]}"
        hearing.conversation_histories.pop(self.uid, None)
        hearing._date_mismatch_counts.pop(self.uid, None)

    def run_turn(self, responses, text="こんにちは", paused=False,
                 suppressed=False):
        """handle_houki_hearing を 1 回実行し (send, queue, chatlog) を返す。"""
        send, queue, chatlog = AsyncMock(), AsyncMock(return_value="q-1"), \
            AsyncMock()
        model = AsyncMock(side_effect=list(responses))
        with patch.object(hearing, "call_hearing_model", model), \
             patch.object(hearing, "reply_with_push_fallback", send), \
             patch.object(hearing, "save_to_approval_queue", queue), \
             patch.object(hearing, "save_to_chatlog", chatlog), \
             patch.object(hearing, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(hearing, "is_suppressed",
                          AsyncMock(return_value=suppressed)), \
             patch.object(hearing, "autoreply_paused", lambda: paused):
            _run(hearing.handle_houki_hearing("rtok", self.uid, text))
        return send, queue, chatlog


class TestHearingFlow(_HearingBase):
    def test_plain_reply_sent_via_houki_channel(self):
        send, queue, chatlog = self.run_turn(
            [_text_response("お問合せありがとうございます。"
                            "亡くなられた方のお名前を教えていただけますか。")])
        send.assert_awaited_once()
        self.assertIs(send.await_args.args[0], HOUKI_CHANNEL)
        self.assertEqual(send.await_args.args[1], "rtok")
        self.assertEqual(send.await_args.args[2], self.uid)
        queue.assert_not_awaited()
        cats = [c.args[3] for c in chatlog.await_args_list]
        self.assertEqual(cats, ["相続放棄ヒアリング", "相続放棄ヒアリング"])

    def test_tool_use_upserts_and_second_call_replies(self):
        send, _q, _c = self.run_turn(
            [_tool_response({"phase": "1_deceased",
                             "fields": {"被相続人氏名": "山田花子",
                                        "続柄": "子"},
                             "phase_done": True, "hearing_done": False}),
             _text_response("記録しました。次に死亡日を教えて"
                            "いただけますか。")],
            text="母の山田花子が亡くなりました")
        rows = list(self.fake.rows.values())
        self.assertEqual(len(rows), 1)
        rid = rows[0]["$id"]["value"]
        self.assertEqual(self.fake.field(rid, "被相続人氏名"), "山田花子")
        self.assertEqual(self.fake.field(rid, "受付チャネル"), "LINE")
        send.assert_awaited_once()
        self.assertIn("死亡日", send.await_args.args[3])

    def test_date_mismatch_twice_flags_and_queues(self):
        bad = {"phase": "2_dates",
               "fields": {"死亡日_申告": "2026-05-02",
                          "死亡を知った日_申告": "2026-05-01"},
               "phase_done": False, "hearing_done": False}
        # 1 回目: 日付は書かれない・queue なし
        _s, queue, _c = self.run_turn(
            [_tool_response(dict(bad)), _text_response("確認させてください。")])
        rid = list(self.fake.rows)[0]
        self.assertIsNone(self.fake.field(rid, "死亡日_申告"))
        queue.assert_not_awaited()
        # 2 回目: 危険類型フラグ「申告内容の矛盾」+承認キュー
        _s, queue2, _c = self.run_turn(
            [_tool_response(dict(bad)), _text_response("再度確認します。")],
            text="やはり5月1日に知りました")
        self.assertEqual(self.fake.field(rid, "危険類型フラグ"),
                         ["申告内容の矛盾"])
        queue2.assert_awaited_once()
        self.assertIn("日付整合検証の2回失敗",
                      queue2.await_args.kwargs["reason"])

    def test_hearing_done_promotes_status(self):
        filled = {c: "x" for c in store.HEARING_REQUIRED_FIELDS}
        filled["死亡日_申告"] = "2026-05-01"
        filled["死亡を知った日_申告"] = "2026-05-01"
        filled["相続人と知った日_申告"] = "2026-05-02"
        _s, _q, _c = self.run_turn(
            [_tool_response({"phase": "7_applicant", "fields": filled,
                             "phase_done": True, "hearing_done": True}),
             _text_response("ありがとうございます。弁護士が確認いたします。")])
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "status"), "電話判断待ち")

    def test_not_done_keeps_status(self):
        _s, _q, _c = self.run_turn(
            [_tool_response({"phase": "1_deceased",
                             "fields": {"被相続人氏名": "山田"},
                             "phase_done": True, "hearing_done": False}),
             _text_response("続いてお伺いします。")])
        rid = list(self.fake.rows)[0]
        self.assertEqual(self.fake.field(rid, "status"), "問い合わせ")


class TestHearingSendGate(_HearingBase):
    def _assert_demoted(self, send, queue, reason_part):
        send.assert_awaited_once()
        self.assertIs(send.await_args.args[0], HOUKI_CHANNEL)
        self.assertEqual(send.await_args.args[3], hp.HOUKI_PROFILE.pending_reply)
        queue.assert_awaited_once()
        self.assertIn(reason_part, queue.await_args.kwargs["reason"])
        self.assertIn("相続放棄ヒアリング送信ゲートで降格",
                      queue.await_args.kwargs["reason"])

    def test_long_reply_demoted(self):
        send, queue, _c = self.run_turn([_text_response("あ" * 301)])
        self._assert_demoted(send, queue, "文字数超過")

    def test_faq_backed_token_demoted(self):
        # route=houki_hearing は根拠集合空＝時効 FAQ 根拠語も降格
        send, queue, _c = self.run_turn(
            [_text_response("信用情報は5年程度で回復します。")])
        self._assert_demoted(send, queue, "経路（houki_hearing）に根拠のない具体値")

    def test_self_intro_demoted(self):
        send, queue, _c = self.run_turn(
            [_text_response("弁護士の大野と申します。ご相談を伺います。")])
        self._assert_demoted(send, queue, "弁護士本人の名乗り検出")

    def test_gates_paused_and_stoplist(self):
        send, queue, chatlog = self.run_turn(
            [_text_response("x")], paused=True)
        send.assert_not_awaited()
        queue.assert_not_awaited()
        chatlog.assert_not_awaited()
        send, queue, chatlog = self.run_turn(
            [_text_response("x")], suppressed=True)
        send.assert_not_awaited()
        chatlog.assert_not_awaited()

    def test_claude_unavailable_fallback(self):
        send, queue, chatlog = self.run_turn(
            [hp.ClaudeUnavailableError("down")])
        send.assert_awaited_once()
        self.assertEqual(send.await_args.args[3],
                         hp.HOUKI_PROFILE.pending_reply)
        queue.assert_awaited_once()
        self.assertIn("Claude応答不能", queue.await_args.kwargs["reason"])
        self.assertEqual(len(chatlog.await_args_list), 2)


class TestHoukiProfileAndPrompt(unittest.TestCase):
    def test_profile_hearing_values(self):
        p = hp.HOUKI_PROFILE
        self.assertEqual(p.name, "souzoku-houki")
        self.assertEqual(p.hearing_style_route, "houki_hearing")
        self.assertEqual(p.customer_style_route, "houki_customer")
        self.assertEqual(p.update_flag_key, "tanjun_shonin_flag")
        self.assertEqual(p.hearing_statuses, frozenset({"", "問い合わせ"}))
        self.assertIn("受任", p.post_engagement_statuses)
        # 障害/降格時の確認中応答は時効と同一の弁護士確定文言を再利用（裁定）
        self.assertEqual(p.pending_reply, cr.PENDING_REPLY)

    def test_profile_customer_side_fail_closed(self):
        # H-5 までの fail-closed プレースホルダ: 顧客対応は全降格・
        # 必須標準回答/第一報バックストップ無効
        p = hp.HOUKI_PROFILE
        self.assertEqual(p.auto_send_categories, frozenset())
        self.assertEqual(p.mandatory_reply_vocab, ())
        self.assertIsNone(p.first_report_detector)
        g = cr.apply_server_guards(
            {"reply": "ご案内します", "category": "挨拶・雑談",
             "auto_send": True}, [], "こんにちは", profile=p)
        self.assertFalse(g.can_auto_send)

    def test_route_basis_houki_hearing_empty(self):
        self.assertEqual(cr.ROUTE_BASIS["houki_hearing"], frozenset())
        v = cr.style_guard_violations("5年程度かかります",
                                      route="houki_hearing")
        self.assertTrue(any("根拠のない具体値" in x for x in v))

    def test_prompt_verbatim_items(self):
        # 質問項目の文言は正本 souzoku-houki/02 §2 の逐語（要語 pin）
        p = hp.HOUKI_HEARING_PROMPT
        for phrase in (
            "亡くなった方の氏名・依頼者との続柄",
            "最後の住所（市区町村まででも可）・本籍（分かれば）",
            "死亡日（分からなければおおよそ）",
            "死亡を知った日・自分が相続人だと知った日（別々に質問）",
            "知った経緯（役所からの通知・債権者からの請求・親族からの連絡 等）",
            "借金・督促の有無、督促状・訴状が届いているか",
            "依頼者は配偶者・子・親・兄弟姉妹のどれか",
            "先順位者（子・親）の有無と、その人達が放棄したか",
            "同順位の相続人（兄弟等）の人数・一緒に放棄したい人がいるか",
            "依頼者本人が相続人か（親族代理の相談か）",
            "未成年・成年後見の関与有無",
            "手元にある戸籍・住民票の有無（自分で取った/これから）",
            "事務所で職務上請求により取得可能",
            # 財産処分の中立質問（民法921条直結・プロンプト固定文言）
            "使ったり、処分したり、解約したり、そこから何かのお支払いをされた"
            "ものはありますか。",
            "YYYY-MM頃",
            "日付の確定は弁護士が行う",
            "熟慮期間の残日数・間に合うかどうかにも言及しない",
        ):
            with self.subTest(phrase=phrase[:20]):
                self.assertIn(phrase, p)
        # 文体（無内容見本・両業務共通の正）を収載
        self.assertIn(cr.HEARING_STYLE_SECTION_BASE, p)
        self.assertEqual(hp.HEARING_TEMPLATE_BLOCKS_HOUKI, ())

    def test_record_hearing_tool_schema(self):
        tool = hp.RECORD_HEARING_TOOL
        self.assertEqual(tool["name"], "record_hearing")
        self.assertEqual(
            tool["input_schema"]["properties"]["phase"]["enum"],
            ["1_deceased", "2_dates", "3_debts", "4_assets",
             "5_others", "6_koseki", "7_applicant"])
        self.assertEqual(sorted(tool["input_schema"]["required"]),
                         ["fields", "hearing_done", "phase", "phase_done"])


if __name__ == "__main__":
    unittest.main()
