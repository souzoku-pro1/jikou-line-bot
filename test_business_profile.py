"""SOUZOKU-HOUKI-H2: 業務プロファイル化（G1）の固定。

固定する仕様:
- 境界: プロファイル側=業務ごとの閉集合・文言・検知器（カテゴリ・prompt
  テンプレ・禁止語/許可リスト・費用/留保/必須標準回答・即時定型・PENDING・
  文体 route 名・status 語彙・tool schema・許可絵文字・フラグ名・第一報検知器）。
  機構側=サニタイズ・300字/質問数・名乗り/記号/無根拠語防壁・承認線引きの
  骨格・配管・env フラグ解釈（hub/business_profile.py docstring が正）。
- JIKOU_PROFILE は既存 module 定数への**参照の束**（逐語・複製なし）で、
  集約 sha256 で凍結する（既存の凍結 sha256 群〔test_autoreply_style1 等〕は
  無変更のまま並存＝値の変更なし）。
- ガード関数は profile=None（既定）で従来挙動と完全一致し、profile 注入で
  実際に定義が差し替わる（注入機構の実効性）。相続放棄プロファイルの実体は
  本票では作らない（本テストのダミーは注入機構の検証専用の架空値）。

H2-fix1（R-SOUZOKU-HOUKI-H2 H2-01 HIGH・Claude 障害経路の profile 化）:
- 障害時応答は profile.pending_reply を**流用**（独立フィールドなしの裁定・
  hub/business_profile docstring と一致）。ClaudeUnavailableError／一般例外の
  両経路で module 定数 PENDING_REPLY の直接使用を廃止。App28 の assistant
  保存文言も送信と同一値。
- profile 省略（None→JIKOU）は従来 PENDING_REPLY と逐語一致を pin。
  alternate profile では両障害形とも時効文言が送信・保存されない negative。
- フィールド数の確定値=30 を pin（[02]: H2 完了報告の「31」は誤記・実体の
  増減なし）。
"""

import hashlib
import json
import os
import re
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

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

import chat_responder as cr  # noqa: E402
from hub.business_profile import BusinessProfile  # noqa: E402

P = cr.JIKOU_PROFILE


def _result(reply, category="手続きの一般的な流れ", auto=True, **extra):
    return {"reply": reply, "category": category, "auto_send": auto, **extra}


# ── ダミープロファイル（注入機構の検証専用・架空値。相続放棄の実体ではない） ─────
def _dummy_profile(**overrides) -> BusinessProfile:
    base = dict(
        name="dummy",
        hearing_statuses=frozenset({"", "受付中D"}),
        post_engagement_statuses=frozenset({"受任D"}),
        system_prompt_template=(
            "DUMMY-TEMPLATE phase={phase} name={customer_name} "
            "status={status}"),
        compose_tool={"name": "compose_reply",
                      "input_schema": {"type": "object"}},
        update_flag_key="dummy_update_flag",
        # D費用/D見立ては自動送信可カテゴリに含め、費用/留保の**内側ガード**が
        # 実際に評価される形にする（カテゴリ関所で落ちると内側検査に届かない）
        auto_send_categories=frozenset({"D自動", "D費用", "D見立て"}),
        forbidden_patterns=(("D禁止", re.compile(r"ダミー禁止語")),),
        allowlisted_phrases=("許可済みダミー禁止語句",),
        allowed_emoji=frozenset(),
        customer_style_route="customer",   # ROUTE_BASIS 閉集合内の既存 route
        hearing_style_route="hearing",
        style_section="DUMMY-STYLE",
        fee_category="D費用",
        fee_required_phrases=("9,999円",),
        fee_guide_marker="【D費用のご案内】",
        conditional_category="D見立て",
        reservation_general_marker="D留保A",
        reservation_individual_markers=("D留保B1", "D留保B2"),
        mandatory_reply_vocab=("だみー扶助",),
        mandatory_reply_text="D標準回答の全文です。",
        mandatory_reply_notice_key="d_mandatory",
        mandatory_reply_label="D標準回答の不使用",
        pending_reply="D確認中です。",
        pending_by_category={"D費用": "D費用は確認して連絡します。"},
        immediate_notice_texts={"d_mandatory": "D標準回答の全文です。",
                                "d_first": "D第一報の定型です。"},
        template_dedup_markers={"d_mandatory": "D標準回答"},
        urgent_notice_kinds={},
        first_report_detector=lambda msg: "D第一報" in msg,
        first_report_notice_key="d_first",
    )
    base.update(overrides)
    return BusinessProfile(**base)


class TestJikouProfileIsVerbatimBundle(unittest.TestCase):
    """JIKOU_PROFILE は既存定数への参照の束（複製なし・逐語）。"""

    def test_identity_and_equality_to_module_constants(self):
        self.assertIs(P.system_prompt_template, cr._SYSTEM_PROMPT_TMPL)
        self.assertIs(P.compose_tool, cr._COMPOSE_REPLY_TOOL)
        self.assertIs(P.allowed_emoji, cr.ALLOWED_CANONICAL_EMOJI)
        self.assertIs(P.pending_by_category, cr.PENDING_BY_CATEGORY)
        self.assertIs(P.immediate_notice_texts, cr.IMMEDIATE_NOTICE_TEXTS)
        self.assertIs(P.template_dedup_markers, cr._TEMPLATE_DEDUP_MARKERS)
        self.assertIs(P.urgent_notice_kinds, cr.URGENT_NOTICE_KINDS)
        self.assertIs(P.first_report_detector, cr.looks_like_court_doc_report)
        self.assertIs(P.style_section, cr.STYLE_SECTION)
        self.assertEqual(P.hearing_statuses, frozenset(cr.HEARING_STATUSES))
        self.assertEqual(P.post_engagement_statuses,
                         frozenset(cr.POST_ENGAGEMENT_STATUSES))
        self.assertEqual(P.auto_send_categories,
                         frozenset(cr.AUTO_SEND_CATEGORIES))
        self.assertEqual(list(P.forbidden_patterns),
                         list(cr._FORBIDDEN_PATTERNS))
        self.assertEqual(list(P.allowlisted_phrases),
                         list(cr.ALLOWLISTED_PHRASES))
        self.assertEqual(list(P.fee_required_phrases),
                         list(cr.FEE_REQUIRED_PHRASES))
        self.assertEqual(P.fee_guide_marker, cr.FEE_GUIDE_MARKER)
        self.assertEqual(P.reservation_general_marker,
                         cr.RESERVATION_GENERAL_MARKER)
        self.assertEqual(list(P.reservation_individual_markers),
                         list(cr.RESERVATION_INDIVIDUAL_MARKERS))
        self.assertEqual(list(P.mandatory_reply_vocab),
                         list(cr._HOTERASU_VOCAB))
        self.assertEqual(P.mandatory_reply_text, cr.HOTERASU_STANDARD_REPLY)
        self.assertEqual(P.pending_reply, cr.PENDING_REPLY)
        self.assertEqual(
            (P.name, P.update_flag_key, P.fee_category,
             P.conditional_category, P.customer_style_route,
             P.hearing_style_route, P.mandatory_reply_notice_key,
             P.mandatory_reply_label, P.first_report_notice_key),
            ("jikou", "jikou_update_flag", "費用の定型案内",
             "時効見立て_条件付き", "customer", "hearing", "hoterasu",
             "法テラス標準回答の不使用", "court_doc_request"))

    def test_aggregate_hash_pin(self):
        """時効プロファイル=現行値の逐語の束の集約 sha256（H2 収載時点で凍結。
        値の変更は禁止・形式変更は票由来でのみ許容）。"""
        payload = json.dumps({
            "name": P.name,
            "hearing_statuses": sorted(P.hearing_statuses),
            "post_engagement_statuses": sorted(P.post_engagement_statuses),
            "system_prompt_template": P.system_prompt_template,
            "compose_tool": P.compose_tool,
            "update_flag_key": P.update_flag_key,
            "auto_send_categories": sorted(P.auto_send_categories),
            "forbidden_patterns": [(lbl, pat.pattern)
                                   for lbl, pat in P.forbidden_patterns],
            "allowlisted_phrases": list(P.allowlisted_phrases),
            "allowed_emoji": sorted(P.allowed_emoji),
            "customer_style_route": P.customer_style_route,
            "hearing_style_route": P.hearing_style_route,
            "style_section": P.style_section,
            "fee_category": P.fee_category,
            "fee_required_phrases": list(P.fee_required_phrases),
            "fee_guide_marker": P.fee_guide_marker,
            "conditional_category": P.conditional_category,
            "reservation_general_marker": P.reservation_general_marker,
            "reservation_individual_markers":
                list(P.reservation_individual_markers),
            "mandatory_reply_vocab": list(P.mandatory_reply_vocab),
            "mandatory_reply_text": P.mandatory_reply_text,
            "mandatory_reply_notice_key": P.mandatory_reply_notice_key,
            "mandatory_reply_label": P.mandatory_reply_label,
            "pending_reply": P.pending_reply,
            "pending_by_category": dict(P.pending_by_category),
            "immediate_notice_texts": dict(P.immediate_notice_texts),
            "template_dedup_markers": dict(P.template_dedup_markers),
            "urgent_notice_kinds": dict(P.urgent_notice_kinds),
            "first_report_detector": "looks_like_court_doc_report",
            "first_report_notice_key": P.first_report_notice_key,
        }, ensure_ascii=False, sort_keys=True)
        self.assertEqual(
            hashlib.sha256(payload.encode("utf-8")).hexdigest(),
            "024773159246717a1bd3730d667acedcc3d475dd813f54d5b9af10ea35e33fe1")


class TestDefaultEqualsJikouProfile(unittest.TestCase):
    """profile 省略（既定）と profile=JIKOU_PROFILE は完全一致（挙動不変）。"""

    SCENARIOS = (
        (_result("ご案内します"), [], "費用はいくらですか"),
        (_result("はい、ご利用いただけます"), [], "法テラス使えますか？"),
        (_result(cr.HOTERASU_STANDARD_REPLY), [], "法テラスは？"),
        (_result("確実に消滅します"), [], "教えて"),
        (_result("あ" * 301), [], "教えて"),
        (_result("案内文", category="費用の定型案内"), [], "費用は？"),
        (_result("見立てです", category="時効見立て_条件付き",
                 jikou_update_flag=True), [], "できますか"),
        (_result("回答", category="法的判断・見通し", auto=False), [],
         "裁判所から訴状が届きました"),
    )

    def test_guard_results_identical(self):
        for result, history, msg in self.SCENARIOS:
            with self.subTest(msg=msg, reply=result["reply"][:12]):
                g0 = cr.apply_server_guards(dict(result), history, msg)
                g1 = cr.apply_server_guards(dict(result), history, msg,
                                            profile=cr.JIKOU_PROFILE)
                self.assertEqual(
                    (g0.can_auto_send, g0.demotion_reasons,
                     g0.immediate_notice, g0.immediate_notice_text),
                    (g1.can_auto_send, g1.demotion_reasons,
                     g1.immediate_notice, g1.immediate_notice_text))

    def test_helpers_identical(self):
        self.assertEqual(cr.find_forbidden_words("必ず時効になります"),
                         cr.find_forbidden_words("必ず時効になります",
                                                 profile=cr.JIKOU_PROFILE))
        self.assertEqual(cr.classify_routing("受任"),
                         cr.classify_routing("受任", profile=cr.JIKOU_PROFILE))
        self.assertEqual(cr.pending_reply_for("費用の定型案内"),
                         cr.pending_reply_for("費用の定型案内",
                                              profile=cr.JIKOU_PROFILE))
        self.assertEqual(
            cr.build_system_prompt(status="決済完了", customer_name="試験"),
            cr.build_system_prompt(status="決済完了", customer_name="試験",
                                   profile=cr.JIKOU_PROFILE))

    def test_guardresult_default_notice_texts_backward_compat(self):
        # 既存テストの生成形（notice_texts 省略）は module 定数へ解決される
        g = cr.GuardResult(can_auto_send=False, immediate_notice="hoterasu")
        self.assertEqual(g.immediate_notice_text, cr.HOTERASU_STANDARD_REPLY)


class TestInjectionTakesEffect(unittest.TestCase):
    """注入機構の実効性: プロファイル差し替えで実際に定義が切り替わる。"""

    def test_forbidden_patterns_swapped(self):
        d = _dummy_profile()
        self.assertEqual(cr.find_forbidden_words("ダミー禁止語です", profile=d),
                         ["D禁止「ダミー禁止語」"])
        # 時効の禁止語はダミー側では検出されない・許可リストも差し替わる
        self.assertEqual(cr.find_forbidden_words("必ず時効になります",
                                                 profile=d), [])
        self.assertEqual(cr.find_forbidden_words("許可済みダミー禁止語句",
                                                 profile=d), [])

    def test_categories_and_update_flag_swapped(self):
        d = _dummy_profile()
        g = cr.apply_server_guards(_result("短い回答", category="D自動"),
                                   [], "質問", profile=d)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)
        # 時効のカテゴリはダミー側では承認必須扱い
        g = cr.apply_server_guards(
            _result("短い回答", category="手続きの一般的な流れ"),
            [], "質問", profile=d)
        self.assertFalse(g.can_auto_send)
        # ダミーのフラグ名が読まれる（jikou_update_flag は無視される）
        g = cr.apply_server_guards(
            _result("D留保A の見立て", category="D見立て",
                    dummy_update_flag=True, jikou_update_flag=False),
            [], "質問", profile=d)
        self.assertFalse(g.can_auto_send)
        self.assertIn("時効更新事由の疑いフラグあり", g.demotion_reasons)

    def test_mandatory_reply_swapped_and_disableable(self):
        d = _dummy_profile()
        g = cr.apply_server_guards(_result("別の回答", category="D自動"),
                                   [], "だみー扶助は使えますか", profile=d)
        self.assertFalse(g.can_auto_send)
        self.assertIn("D標準回答の不使用", g.demotion_reasons)
        self.assertEqual(g.immediate_notice, "d_mandatory")
        self.assertEqual(g.immediate_notice_text, "D標準回答の全文です。")
        # vocab 空プロファイル=ガード無効（時効の法テラス語彙にも反応しない）
        off = _dummy_profile(mandatory_reply_vocab=())
        g = cr.apply_server_guards(_result("別の回答", category="D自動"),
                                   [], "法テラスは使えますか", profile=off)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_fee_and_reservation_swapped(self):
        d = _dummy_profile()
        g = cr.apply_server_guards(_result("値段のご案内", category="D費用"),
                                   [], "料金は", profile=d)
        self.assertFalse(g.can_auto_send)
        self.assertIn("費用定型の必須文言欠落: 9,999円", g.demotion_reasons)
        g = cr.apply_server_guards(
            _result("9,999円 のご案内", category="D費用"), [], "料金は",
            profile=d)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)
        g = cr.apply_server_guards(
            _result("D留保B1 と D留保B2 を含む見立て", category="D見立て"),
            [], "見込みは", profile=d)
        self.assertTrue(g.can_auto_send, g.demotion_reasons)

    def test_first_report_detector_swapped(self):
        d = _dummy_profile()
        g = cr.apply_server_guards(
            _result("回答", category="その他", auto=False), [],
            "D第一報 が届きました", profile=d)
        self.assertEqual(g.immediate_notice, "d_first")
        self.assertEqual(g.immediate_notice_text, "D第一報の定型です。")
        # detector=None のプロファイルではバックストップ無効
        none_p = _dummy_profile(first_report_detector=None)
        g = cr.apply_server_guards(
            _result("回答", category="その他", auto=False), [],
            "D第一報 が届きました", profile=none_p)
        self.assertEqual(g.immediate_notice, "none")

    def test_pending_texts_swapped(self):
        d = _dummy_profile()
        self.assertEqual(cr.pending_reply_for("D費用", profile=d),
                         "D確認中です。")
        with patch.dict(os.environ, {"PENDING_CONTEXT_ENABLED": "1"}):
            self.assertEqual(cr.pending_reply_for("D費用", profile=d),
                             "D費用は確認して連絡します。")
            self.assertEqual(cr.pending_reply_for("未知", profile=d),
                             "D確認中です。")

    def test_routing_and_template_swapped(self):
        d = _dummy_profile()
        self.assertEqual(cr.classify_routing("受任D", profile=d),
                         "post_engagement")
        self.assertEqual(cr.classify_routing("受任", profile=d),
                         "pre_engagement")   # 時効語彙はダミーでは未知
        prompt = cr.build_system_prompt(status="受任D", customer_name="試験",
                                        profile=d)
        self.assertTrue(prompt.startswith(
            "DUMMY-TEMPLATE phase=受任後 name=試験 status=受任D"))
        prompt2 = cr.build_system_prompt(status="受任D", profile=d,
                                         known_items={"K": "V"})
        self.assertIn("【収集済み項目（既知・再質問禁止）】", prompt2)

    def test_compose_tool_swapped(self):
        d = _dummy_profile()
        captured = {}

        async def fake_create(client, **kw):
            captured.update(kw)
            block = MagicMock()
            block.type = "tool_use"
            block.input = {"reply": "r", "category": "D自動",
                           "auto_send": True}
            resp = MagicMock()
            resp.content = [block]
            return resp

        import asyncio
        with patch.object(cr, "create_message_with_fallback", fake_create):
            asyncio.run(cr._call_compose_reply("sys", [], tool=d.compose_tool))
        self.assertIs(captured["tools"][0], d.compose_tool)
        with patch.object(cr, "create_message_with_fallback", fake_create):
            asyncio.run(cr._call_compose_reply("sys", []))
        self.assertIs(captured["tools"][0], cr._COMPOSE_REPLY_TOOL)


class TestOutagePathProfile(unittest.TestCase):
    """H2-fix1 [01]: Claude 障害経路（ClaudeUnavailableError/一般例外）の
    profile 化——送信文言と App28 保存文言の両方を assert。"""

    def _run_outage(self, exc, profile=None):
        """handle_customer_message を compose 例外で走らせ
        (reply_mock, chatlog_mock, queue_mock) を返す。"""
        import asyncio
        reply, log = AsyncMock(), AsyncMock()
        queue = AsyncMock(return_value="q-1")
        kwargs = dict(
            user_id="Uoutage", user_message="質問です", reply_token="tok",
            app21_record={"status": {"value": "決済完了"}}, reply_func=reply)
        if profile is not None:
            kwargs["profile"] = profile
        with patch.object(cr, "_call_compose_reply",
                          AsyncMock(side_effect=exc)), \
             patch.object(cr, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(cr, "save_to_chatlog", log), \
             patch.object(cr, "save_to_approval_queue", queue), \
             patch.object(cr, "_notify_attorney", AsyncMock()):
            asyncio.run(cr.handle_customer_message(**kwargs))
        return reply, log, queue

    def _assistant_saves(self, log):
        return [c.args[2] for c in log.await_args_list
                if c.args[1] == "assistant"]

    def test_default_profile_matches_legacy_pending_reply(self):
        # profile 省略（None→JIKOU）: 従来 PENDING_REPLY と逐語一致
        # （送信・App28 保存の両方）
        from claude_gateway import ClaudeUnavailableError
        reply, log, queue = self._run_outage(ClaudeUnavailableError("down"))
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[1], cr.PENDING_REPLY)
        self.assertEqual(self._assistant_saves(log), [cr.PENDING_REPLY])
        queue.assert_awaited_once()
        self.assertEqual(queue.await_args.kwargs["ai_draft"],
                         cr.OUTAGE_DRAFT_PLACEHOLDER)   # 下書き雛形は機構側
        # 一般例外経路（従来: 返信のみ・chatlog/queue なし）
        reply2, log2, queue2 = self._run_outage(RuntimeError("rate"))
        reply2.assert_awaited_once()
        self.assertEqual(reply2.await_args.args[1], cr.PENDING_REPLY)
        self.assertEqual(self._assistant_saves(log2), [])
        queue2.assert_not_awaited()

    def test_alternate_profile_claude_unavailable_no_jikou_text(self):
        from claude_gateway import ClaudeUnavailableError
        d = _dummy_profile()
        reply, log, _q = self._run_outage(ClaudeUnavailableError("down"),
                                          profile=d)
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[1], "D確認中です。")
        self.assertNotEqual(reply.await_args.args[1], cr.PENDING_REPLY)
        self.assertEqual(self._assistant_saves(log), ["D確認中です。"])
        # 時効文言はどの保存にも現れない（送信と保存の一致・混入ゼロ）
        for c in log.await_args_list:
            self.assertNotIn(cr.PENDING_REPLY, c.args[2])

    def test_alternate_profile_generic_exception_no_jikou_text(self):
        d = _dummy_profile()
        reply, log, queue = self._run_outage(RuntimeError("rate"), profile=d)
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[1], "D確認中です。")
        self.assertNotEqual(reply.await_args.args[1], cr.PENDING_REPLY)
        self.assertEqual(self._assistant_saves(log), [])   # 従来どおり保存なし
        queue.assert_not_awaited()

    def test_handle_claude_outage_direct_call_with_profile(self):
        import asyncio
        d = _dummy_profile()
        reply, log = AsyncMock(), AsyncMock()
        with patch.object(cr, "save_to_chatlog", log), \
             patch.object(cr, "save_to_approval_queue",
                          AsyncMock(return_value="q-1")), \
             patch.object(cr, "_notify_attorney", AsyncMock()):
            asyncio.run(cr.handle_claude_outage(
                user_id="Uo", user_message="m", reply_token="t",
                reply_func=reply, profile=d))
        self.assertEqual(reply.await_args.args[1], "D確認中です。")
        self.assertEqual(self._assistant_saves(log), ["D確認中です。"])

    def test_field_count_pinned_30(self):
        # [02]: BusinessProfile のフィールド数=30（H2 報告の 31 は誤記・
        # 実体の増減なし）。増減は票由来でのみ行う
        import dataclasses
        self.assertEqual(len(dataclasses.fields(BusinessProfile)), 30)


class TestHandleCustomerMessagePlumbing(unittest.TestCase):
    """handle_customer_message が profile を各段へ引き渡す（配管の実効性）。"""

    def test_profile_reaches_compose_and_guards(self):
        d = _dummy_profile()
        seen = {}

        async def fake_compose(system_prompt, messages, tool=None):
            seen["tool"] = tool
            seen["system_prompt"] = system_prompt
            return {"reply": "短い回答", "category": "D自動",
                    "auto_send": True}

        reply = AsyncMock()
        import asyncio
        with patch.object(cr, "_call_compose_reply", fake_compose), \
             patch.object(cr, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(cr, "save_to_chatlog", AsyncMock()), \
             patch.object(cr, "save_to_approval_queue",
                          AsyncMock(return_value="q-1")), \
             patch.object(cr, "_notify_attorney", AsyncMock()):
            asyncio.run(cr.handle_customer_message(
                user_id="Uh2", user_message="質問", reply_token="tok",
                app21_record={"status": {"value": "受任D"}},
                reply_func=reply, profile=d))
        self.assertIs(seen["tool"], d.compose_tool)
        self.assertTrue(seen["system_prompt"].startswith("DUMMY-TEMPLATE"))
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[1], "短い回答")   # D自動=自動送信

    def test_default_profile_unchanged_path(self):
        # profile 省略時は従来どおり時効テンプレ+時効カテゴリで動く
        seen = {}

        async def fake_compose(system_prompt, messages, tool=None):
            seen["tool"] = tool
            seen["system_prompt"] = system_prompt
            return {"reply": "了解です", "category": "挨拶・雑談",
                    "auto_send": True}

        reply = AsyncMock()
        import asyncio
        with patch.object(cr, "_call_compose_reply", fake_compose), \
             patch.object(cr, "get_recent_chat_history",
                          AsyncMock(return_value=[])), \
             patch.object(cr, "save_to_chatlog", AsyncMock()), \
             patch.object(cr, "save_to_approval_queue",
                          AsyncMock(return_value="q-1")), \
             patch.object(cr, "_notify_attorney", AsyncMock()):
            asyncio.run(cr.handle_customer_message(
                user_id="Uh2b", user_message="こんにちは", reply_token="tok",
                app21_record={"status": {"value": "決済完了"}},
                reply_func=reply))
        self.assertIs(seen["tool"], cr._COMPOSE_REPLY_TOOL)
        self.assertTrue(seen["system_prompt"].startswith(
            "あなたは大野法律事務所"))
        reply.assert_awaited_once()
        self.assertEqual(reply.await_args.args[1], "了解です")


if __name__ == "__main__":
    unittest.main()
