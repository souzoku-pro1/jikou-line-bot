"""JIKOU-REPLY-Q2（QUEUE-2026-10-13-D D-1）: 定型表・選択器・描画器・債権者別ルール表・閉集合送信

固定する仕様:
- (a) 定型表 hub/jikou_reply_texts: J-0〜J-15・J-H1〜H6 を sha pin（TABLE_SHA256_PIN）。全文言は仮置き（provisional）。
  v1.4 に大野原文がある定型は frozen_v14、未決（J-0・J-5b・J-6・J-11・J-13・J-14・J-14b・J-15・J-H1〜H6）は
  needs_approval＝選択器が選ばない。J-7 は凍結 FEE_GUIDE_TEXT と同一。PC-A 起草文に「確認します」系を含めない。
- (b) 選択器 hub/jikou_reply_select: 段階×分類 ID の決定表（スナップショット）・裁判等の申告で J-4/J-12 不可・
  J-H3 否定記録が無ければ J-12 を出さない・該当なしは空（fail-closed）。
- (c) 描画器 hub/jikou_reply_render: 全定型の描画スナップショット・差し込み値以外は不変・型検査・pin 不一致で RenderError。
  J-5 の 2 算式（推奨=上限日+5 年+3 か月→「YYYY年M月末」／基準=評価日−5 年−3 か月→「YYYY年M月」）と境界（月末・うるう年・範囲・和暦）。
- (d) 債権者別ルール表: 既定行（表に無い業者）・仮の行では J-5 を返さない・確定行+最終返済日+通知記録で J-5a。
- (e) V2 OFF は従来経路のまま（選択器を呼ばない）。V2 ON: 閉集合の定型だけ自動送信・該当なしは LLM 自由文を送らず
  承認へ降格（無言+対応待ち・J-0 は会話があるときのみ）・緊急の即時定型は既存経路・許可表で止まれば対応待ち。
- pin: 新 module は sink 違反ゼロ・allowlist 追加ゼロ・chat_responder の結線は本文 1 か所+末尾追記・
  send_policy.J0_TEXT はデータ表を参照（値・sha は C-1 と同一）。
"""

import datetime
import hashlib
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from test_jikou_reply_q1c import USER, _DbMixin, _FakeClient, _compose, _rec, _run, _texts  # noqa: E402

import chat_responder as cr  # noqa: E402
from hub import empty_reply_guard as eg  # noqa: E402
from hub import jikou_reply_flow as F  # noqa: E402
from hub import jikou_reply_render as R  # noqa: E402
from hub import jikou_reply_select as S  # noqa: E402
from hub import jikou_reply_texts as T  # noqa: E402
from hub import reply_pending as rp  # noqa: E402
from hub import send_ledger as sl  # noqa: E402
from hub import send_policy as sp  # noqa: E402
from test_sink_ast_policy import scan_source  # noqa: E402

REPO = Path(__file__).parent
SLOTS = {T.SLOT_BASE_MONTH: "2021年7月", T.SLOT_RECOMMENDED_MONTH: "2027年3月末", T.SLOT_CREDITORS: "アコム"}
D = datetime.date

# 全定型の描画スナップショット（差し込み値は SLOTS・sha256 先頭 16 桁）。文言変更時は大野承認と同時に更新する
RENDER_SNAPSHOT = {
    "J-0": "0f1c3ca29956f400", "J-1": "0a0ad4e62f4f2b0c", "J-2": "aee1701e41699ac5", "J-3": "ffd8405ad62159c1",
    "J-4": "6d792a706da4e4e7", "J-5a": "488e8e48683bb90a", "J-5b": "a284026cf092e4d3", "J-6": "f4c1446b9904a048",
    "J-7": "1581839b9c8cdb68", "J-8": "2a0ee0a849169a40", "J-9": "885724d32dfe7614", "J-10": "3da6369ecd3c9fb0",
    "J-11": "a6eb32697f7bf3c2", "J-12": "79ccad320d8045ca", "J-13": "670cda6aec3ae39e", "J-14": "09a48a5b2ff4e7a1",
    "J-14b": "6836b1a21ceb07a8", "J-15": "a25738fb2cf38123", "J-H1": "aca38a9718be2cfd", "J-H2": "826debc5a47075fc",
    "J-H3": "a341d1311ccff561", "J-H4": "759555c455afba53", "J-H5": "17c4d0960a326f65", "J-H6": "1ab0849d3ac0d219",
}
NEEDS_APPROVAL = ("J-0", "J-5b", "J-6", "J-11", "J-13", "J-14", "J-14b", "J-15",
                  "J-H1", "J-H2", "J-H3", "J-H4", "J-H5", "J-H6")
# 段階×分類 ID の決定表（既定入力: 初回返信あり・使用済みなし・債権者なし・最終返済日なし・申告なし）
DECISION = {
    "consult": {"判明時期": ["J-2"], "住所確認の連絡": ["J-3"], "援用時期": ["J-4"], "費用": ["J-7"]},
    "engaged": {"判明時期": ["J-2"], "住所確認の連絡": ["J-3"], "援用時期": ["J-4"], "費用": ["J-7"], "委任契約": ["J-10"]},
    "unknown": {"判明時期": ["J-2"], "住所確認の連絡": ["J-3"], "援用時期": ["J-4"], "費用": ["J-7"]},
}
CONFIRMED_ACOM = {"company_id": "acom_test", "names": ["アコム"], "version": 1, "status": T.RULE_CONFIRMED,
                  "basis": T.BASIS_LAST_PAYMENT, "period_years": 5, "margin_months": 3, "safety_months": 3,
                  "j5": "J-5a", "notes": "test"}


def _sha16(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


class _RulesPatch:
    """CREDITOR_RULES を差し替え、pin も差し替え後の値にする（表の整合検査を通す）。"""

    def __init__(self, rows):
        self.rows = tuple(rows)

    def __enter__(self):
        self.p1 = patch.object(T, "CREDITOR_RULES", self.rows)
        self.p1.start()
        self.p2 = patch.object(T, "TABLE_SHA256_PIN", T.table_sha256())
        self.p2.start()
        return self

    def __exit__(self, *_a):
        self.p2.stop()
        self.p1.stop()
        return False


# ── 1. 定型表 ────────────────────────────────────────────────────────────────
class TestTable(unittest.TestCase):
    def test_pin_and_closed_sets(self):
        self.assertTrue(T.verify_pin())
        self.assertEqual(T.TABLE_STATUS, "provisional")
        self.assertEqual(set(T.TEXT_IDS), set(RENDER_SNAPSHOT))
        for i, e in T.TEXTS.items():
            self.assertIn(e["status"], T.STATUSES, i)
            self.assertIn(e["stage"], T.STAGES, i)
            self.assertIn(e["unit"], (T.UNIT_CONVERSATION, T.UNIT_COMPANY), i)
            self.assertTrue(set(e["slots"]) <= set(T.SLOTS), i)
            self.assertTrue(e["text"].strip(), i)
            self.assertLessEqual(e["text"].count("。"), 3 if i != "J-7" else 99, i)
        self.assertEqual(T.needs_approval_ids(), NEEDS_APPROVAL)
        self.assertEqual(set(T.INTENT_RULES), set(T.INTENTS))
        self.assertEqual(set(T.INTENT_DESCRIPTIONS), set(T.INTENTS))
        self.assertEqual(T.HEARING_IDS, ("J-H1", "J-H2", "J-H3", "J-H4", "J-H5", "J-H6"))

    def test_fee_text_is_frozen_fee_guide_and_amount_rule(self):
        self.assertEqual(T.text("J-7"), cr.FEE_GUIDE_TEXT)
        for amount in ("44,000円", "22,000円"):
            self.assertIn(amount, T.text("J-7"))
        self.assertTrue(T.TEXTS["J-7"]["length_exempt"])

    def test_j0_is_single_source_with_send_policy(self):
        self.assertEqual(sp.J0_TEXT, T.text("J-0"))
        self.assertEqual(sp.J0_TEXT_SHA256, "0f1c3ca29956f4000aface287c8dc9aa7c6907e93c9b7f1267858a6c2212b03d")

    def test_drafted_texts_avoid_empty_reply_vocabulary(self):
        # PC-A 起草分（司令塔仮置きの J-0 と v1.4 大野原文は除く）は「確認します」「申し伝えます」系を含めない
        for i in NEEDS_APPROVAL:
            if i == "J-0":
                continue
            self.assertEqual(eg.empty_reply_labels(T.text(i)), [], i)
        for i in ("J-5b", "J-13", "J-15"):
            self.assertEqual(T.TEXTS[i]["status"], T.STATUS_NEEDS_APPROVAL)
        self.assertFalse(T.TEXTS["J-13"]["auto"])
        self.assertFalse(T.TEXTS["J-15"]["auto"])

    def test_selectable_means_frozen_and_auto(self):
        self.assertTrue(T.is_selectable("J-7"))
        self.assertFalse(T.is_selectable("J-6"))
        self.assertFalse(T.is_selectable("J-13"))
        self.assertFalse(T.is_selectable("J-X"))

    def test_creditor_rules_default_and_aliases(self):
        self.assertEqual(T.rule_for("")["company_id"], T.DEFAULT_COMPANY_ID)
        self.assertEqual(T.rule_for("アコム")["company_id"], T.DEFAULT_COMPANY_ID)
        self.assertEqual(T.rule_for("アビリオ債権回収株式会社")["company_id"], "abilio")
        self.assertEqual(T.rule_for("新生フィナンシャル")["j5"], "J-5b")
        for r in T.CREDITOR_RULES:
            self.assertEqual(r["status"], T.RULE_PROVISIONAL, r["company_id"])   # 大野確定まで J-5 不可
            self.assertEqual((r["margin_months"], r["safety_months"]), (3, 3))
        rows = T.approval_table()
        self.assertEqual(len(rows), len(T.TEXT_IDS))
        self.assertTrue(all(set(r) >= {"id", "label", "status", "text", "note"} for r in rows))


# ── 2. 描画器・J-5 算式 ────────────────────────────────────────────────────────
class TestRender(unittest.TestCase):
    def test_snapshot_all_texts(self):
        got = {i: _sha16(R.render_one(i, SLOTS)) for i in T.TEXT_IDS}
        self.assertEqual(got, RENDER_SNAPSHOT)

    def test_only_slots_change_and_fee_heading_is_literal(self):
        self.assertEqual(R.render_one("J-2"), T.text("J-2"))
        self.assertIn("【費用のご案内】", R.render_one("J-7"))
        out = R.render_one("J-9", {T.SLOT_CREDITORS: "アコム、プロミス"})
        self.assertEqual(out, T.text("J-9").replace("【社名列挙】", "アコム、プロミス"))
        self.assertEqual(R.render(["J-4", "J-8"]), T.text("J-4") + R.SEPARATOR + T.text("J-8"))

    def test_render_errors_are_fail_closed(self):
        with self.assertRaises(R.RenderError):
            R.render_one("J-99")
        with self.assertRaises(R.RenderError):
            R.render_one("J-5a", {T.SLOT_BASE_MONTH: "2021年7月"})            # 推奨年月なし
        with self.assertRaises(R.RenderError):
            R.render_one("J-5a", {T.SLOT_BASE_MONTH: "7月", T.SLOT_RECOMMENDED_MONTH: "2027年3月末"})
        with self.assertRaises(R.RenderError):
            R.render_one("J-9", {T.SLOT_CREDITORS: "a\nb"})
        with self.assertRaises(R.RenderError):
            R.render([])
        with patch.object(T, "TABLE_SHA256_PIN", "0" * 64):
            with self.assertRaises(R.RenderError):
                R.render_one("J-2")
        self.assertEqual(R.length_exempt_blocks(["J-7", "J-2"]), (T.text("J-7"),))

    def test_j5_two_formulas_and_boundaries(self):
        d = R.j5_dates(D(2021, 12, 31), D(2026, 10, 13))
        self.assertEqual((d["recommended"], d["base"]), (D(2027, 3, 31), D(2021, 7, 13)))
        self.assertEqual((d["recommended_display"], d["base_display"]), ("2027年3月末", "2021年7月"))
        # うるう年の応当日: 2020-02-29 + 5 年 → 2025-02-28 → +3 か月 → 2025-05-28
        self.assertEqual(R.j5_dates(D(2020, 2, 29), D(2026, 1, 31))["recommended"], D(2025, 5, 28))
        # 評価日 1 月 31 日 − 5 年 − 3 か月 → 10 月 31 日（月末維持）／3 月 31 日 → 12 月 31 日
        self.assertEqual(R.j5_dates(D(2020, 1, 31), D(2026, 1, 31))["base"], D(2020, 10, 31))
        self.assertEqual(R.j5_dates(D(2020, 1, 31), D(2026, 3, 31))["base"], D(2020, 12, 31))
        self.assertEqual(R.add_months(D(2023, 1, 31), 1), D(2023, 2, 28))
        self.assertEqual(R.add_months(D(2024, 1, 31), 1), D(2024, 2, 29))
        self.assertEqual(R.add_months(D(2024, 3, 15), -3), D(2023, 12, 15))
        # 表の月数を使う
        self.assertEqual(R.j5_dates(D(2021, 12, 31), D(2026, 10, 13), margin_months=6, safety_months=1)["recommended"],
                         D(2027, 6, 30))

    def test_parse_last_payment(self):
        self.assertEqual(R.parse_last_payment("2021年12月"), D(2021, 12, 31))       # 月のみ→月末
        self.assertEqual(R.parse_last_payment("2021年12月頃"), D(2021, 12, 31))
        self.assertEqual(R.parse_last_payment("2020/2/29"), D(2020, 2, 29))
        self.assertEqual(R.parse_last_payment("２０２１年１２月"), D(2021, 12, 31))
        self.assertEqual(R.parse_last_payment("令和3年12月"), D(2021, 12, 31))
        self.assertEqual(R.parse_last_payment("平成30年2月"), D(2018, 2, 28))
        self.assertEqual(R.parse_last_payment("2021年10月〜12月"), D(2021, 12, 31))   # 範囲→最も遅い月
        self.assertEqual(R.parse_last_payment("2021年10月から2022年1月"), D(2022, 1, 31))
        for bad in ("", "不明", "（未登録）", "5年前", "2021年13月", "2021年2月30日"):
            self.assertIsNone(R.parse_last_payment(bad), bad)


# ── 3. 選択器 ────────────────────────────────────────────────────────────────
class TestSelector(unittest.TestCase):
    def test_decision_table_snapshot(self):
        for stage, table in DECISION.items():
            for intent in T.INTENTS:
                got = list(S.select(S.Inputs(stage=stage, intent=intent)).ids)
                self.assertEqual(got, table.get(intent, []), f"{stage}/{intent}")

    def test_stage_mapping(self):
        self.assertEqual(S.stage_for_status("受任"), S.STAGE_ENGAGED)
        self.assertEqual(S.stage_for_status("完了"), S.STAGE_ENGAGED)
        self.assertEqual(S.stage_for_status(""), S.STAGE_CONSULT)
        self.assertEqual(S.stage_for_status("受任前"), S.STAGE_CONSULT)
        self.assertEqual(S.stage_for_status("決済完了"), S.STAGE_UNKNOWN)

    def test_fail_closed_inputs(self):
        self.assertTrue(S.select(S.Inputs(stage="consult", intent="存在しない")).empty)
        self.assertTrue(S.select(S.Inputs(stage="x", intent="費用")).empty)
        with patch.object(T, "TABLE_SHA256_PIN", "0" * 64):
            self.assertTrue(S.select(S.Inputs(stage="consult", intent="費用")).empty)
        # 下書きのみ・要承認・緊急は空
        for intent in ("判決確定", "電話希望", "緊急・裁判所書類", "その他", "挨拶", "オンライン対応", "和解の流れ"):
            self.assertTrue(S.select(S.Inputs(stage="engaged", intent=intent)).empty, intent)

    def test_court_claim_blocks_j4_and_j12(self):
        self.assertTrue(S.select(S.Inputs(stage="consult", intent="援用時期", court_claim=True)).empty)
        self.assertTrue(S.select(S.Inputs(stage="consult", intent="見立て", court_claim=True,
                                          court_docs_denied=True)).empty)
        self.assertEqual(S.select(S.Inputs(stage="consult", intent="見立て", court_docs_denied=True)).ids, ("J-12",))
        self.assertTrue(S.select(S.Inputs(stage="consult", intent="見立て")).empty)   # J-H3 は要承認→空

    def test_used_ids_and_intro(self):
        self.assertTrue(S.select(S.Inputs(stage="consult", intent="費用", used_ids=("J-7",))).empty)
        sel = S.select(S.Inputs(stage="consult", intent="費用", first_reply_sent=False))
        self.assertEqual(sel.ids, ("J-1", "J-7"))
        sel = S.select(S.Inputs(stage="consult", intent="費用", first_reply_sent=False, used_ids=("J-1",)))
        self.assertEqual(sel.ids, ("J-7",))
        self.assertEqual(S.select(S.Inputs(stage="engaged", intent="必要書類", creditor_name="アコム")).slots,
                         {T.SLOT_CREDITORS: "アコム"})
        self.assertTrue(S.select(S.Inputs(stage="consult", intent="必要書類", creditor_name="アコム")).empty)

    def test_j5_requires_confirmed_rule_and_last_payment(self):
        base = dict(stage="consult", intent="援用時期", creditor_name="アコム", last_payment_text="2021年12月",
                    eval_date=D(2026, 10, 13))
        sel = S.select(S.Inputs(**base))
        self.assertEqual(sel.ids, ("J-4",))                       # 既定行は仮→J-5 なし
        self.assertIn("J-5:rule_provisional", sel.reasons)
        with _RulesPatch(T.CREDITOR_RULES + (CONFIRMED_ACOM,)):
            sel = S.select(S.Inputs(**base))
            self.assertEqual(sel.ids, ("J-4", "J-5a", "J-8"))
            self.assertEqual(sel.slots, {T.SLOT_RECOMMENDED_MONTH: "2027年3月末", T.SLOT_BASE_MONTH: "2021年7月"})
            self.assertEqual(sel.company_id, "acom_test")
            self.assertEqual(sel.j5_detail["recommended"], "2027年3月末")
            self.assertEqual(S.select(S.Inputs(**dict(base, last_payment_text="不明"))).ids, ("J-4",))
            self.assertEqual(S.select(S.Inputs(**dict(base, used_ids=("J-5a@acom_test",)))).ids, ("J-4",))
            self.assertTrue(S.select(S.Inputs(**dict(base, court_claim=True))).empty)
            # J-5b（期限の利益喪失時）は文言が要承認→選ばれない
            lake = dict(CONFIRMED_ACOM, company_id="lake_t", names=["レイク"], basis=T.BASIS_ACCELERATION, j5="J-5b")
            with _RulesPatch(T.CREDITOR_RULES + (lake,)):
                self.assertEqual(S.select(S.Inputs(**dict(base, creditor_name="レイク"))).ids, ("J-4",))
            dropped = S.drop_j5(sel)
            self.assertEqual(dropped.ids, ("J-4",))
            self.assertIn("J-5:notice_unrecorded", dropped.reasons)
            text = R.render(sel.ids, sel.slots)
            self.assertIn("2021年7月より前", text)
            self.assertIn("2027年3月末頃", text)


# ── 4. 送信経路（V2 OFF 不変・V2 ON の閉集合送信・fail-closed） ─────────────────
class TestFlow(_DbMixin):
    def setUp(self):
        super().setUp()
        self.intent = AsyncMock(return_value={"intent": "費用", "creditor_name": ""})
        self._p_intent = patch.object(F, "classify_intent", self.intent)
        self._p_intent.start()

    def tearDown(self):
        self._p_intent.stop()
        super().tearDown()

    def test_off_keeps_existing_path_and_never_classifies(self):
        self.customer(_compose("こちらこそよろしくお願いいたします。", category="挨拶・雑談"))
        self.assertEqual(_texts(), ["こちらこそよろしくお願いいたします。"])
        self.intent.assert_not_called()
        self.assertFalse(self.approval.called)

    def test_on_sends_closed_set_text_only(self):
        self.on()
        self.customer(_compose("自由文の回答です。", category="費用の定型案内"))
        self.assertEqual(_texts(), [T.text("J-7")])
        self.assertFalse(self.approval.called)
        self.assertEqual(self.pending(), [])
        roles = [c.args[1] for c in self.chatlog.call_args_list]
        self.assertEqual(roles, ["user", "assistant"])
        self.assertEqual(self.chatlog.call_args_list[1].args[2], T.text("J-7"))
        self.assertEqual(self.chatlog.call_args_list[1].args[4], "yes")

    def test_on_no_match_demotes_free_text_to_approval_silently(self):
        self.on()
        self.intent.return_value = {"intent": "その他", "creditor_name": ""}
        self.customer(_compose("自由文の回答です。", category="挨拶・雑談", auto_send=True))
        self.assertEqual(_texts(), [])                       # 会話が無いので J-0 も無い
        self.assertTrue(self.approval.called)
        rows = self.pending()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["reason"], rp.REASON_DEMOTED)

    def test_on_no_match_sends_j0_when_conversation_exists(self):
        self.on()
        self.touch("ev1")
        self.intent.return_value = {"intent": "その他", "creditor_name": ""}
        self.customer(_compose("自由文の回答です。", category="挨拶・雑談"))
        self.assertEqual(_texts(), [sp.J0_TEXT])
        self.assertTrue(self.approval.called)

    def test_on_classifier_failure_is_fail_closed(self):
        self.on()
        self.intent.side_effect = RuntimeError("api down")
        self.customer(_compose("自由文の回答です。", category="費用の定型案内"))
        self.assertEqual(_texts(), [])
        self.assertTrue(self.approval.called)

    def test_on_urgent_notice_keeps_existing_path(self):
        self.on()
        self.customer(_compose("下書き", category="緊急対応", auto_send=False, notice="court_doc_request"),
                      text="裁判所から訴状が届きました")
        self.intent.assert_not_called()
        self.assertEqual(_texts(), [cr.COURT_DOC_REQUEST_REPLY])
        self.assertTrue(self.approval.called)

    def test_on_policy_stop_registers_pending_without_draft(self):
        self.on()
        self.touch("ev1")
        _run(sl.set_attending("jikou", USER))
        self.customer(_compose("自由文", category="費用の定型案内"))
        self.assertEqual(_texts(), [])
        self.assertFalse(self.approval.called)
        rows = self.pending()
        self.assertEqual([r["reason"] for r in rows], [rp.REASON_POLICY_STOPPED])

    def test_on_j5_requires_recorded_notice(self):
        self.on()
        self.touch("ev1")
        self.app21.return_value = _rec(status="受任", 問い合わせ業者名="アコム", response_mode="自動",
                                       最終返済日_テキスト="2021年12月")
        self.app21.return_value["$id"] = {"value": "21"}
        self.intent.return_value = {"intent": "援用時期", "creditor_name": ""}
        with _RulesPatch(T.CREDITOR_RULES + (CONFIRMED_ACOM,)):
            self.customer(_compose("自由文", category="手続きの一般的な流れ"))
            texts = _texts()
            self.assertEqual(len(texts), 1)
            self.assertIn("2027年3月末頃", texts[0])
            self.assertIn(T.text("J-4"), texts[0])
            # 300 字を超えるため締め（J-8）を外す（本文は変えない・自己紹介 J-1 は収まるので残る）
            self.assertNotIn(T.text("J-8"), texts[0])
            self.assertTrue(texts[0].startswith(T.text("J-1")))
            self.assertLessEqual(len(texts[0]), 300)
            self.assertEqual(self.biz.call_count, 1)
            notice = self.biz.call_args.args[1]
            self.assertIn("【推奨時期の自動案内】", notice)
            self.assertIn("acom_test", notice)
            self.assertNotIn(USER, notice)
            self.assertNotIn("アコム", notice)
            ops = self.ops()
            self.assertEqual(sorted(o["channel"] for o in ops), ["jikou", "jikou_notice"])
            # 通知を記録できなければ J-5（と J-8）を落として J-4 だけ送る
            _FakeClient.calls.clear()
            self.biz.return_value = False
            self.customer(_compose("自由文", category="手続きの一般的な流れ"))
            self.assertEqual(_texts(), [T.text("J-4")])

    def test_on_used_text_in_history_is_not_resent(self):
        self.on()
        hist = cr.ChatHistory([{"role": "user", "content": "費用は？"}, {"role": "assistant", "content": T.text("J-7")}])
        with patch.object(cr, "get_recent_chat_history", AsyncMock(return_value=hist)):
            self.customer(_compose("自由文", category="費用の定型案内"))
        self.assertEqual(_texts(), [])
        self.assertTrue(self.approval.called)
        self.assertEqual(F.used_ids_from_history(hist), ("J-7",))

    def test_on_guard_rejection_is_fail_closed(self):
        self.on()
        with patch.object(F, "guard_violations", return_value=["forbidden_words"]):
            self.customer(_compose("自由文", category="費用の定型案内"))
        self.assertEqual(_texts(), [])
        self.assertTrue(self.approval.called)

    def test_guard_checks_existing_rules_only(self):
        p = cr.JIKOU_PROFILE
        self.assertEqual(F.guard_violations(R.render(["J-1", "J-7"]), ["J-1", "J-7"], profile=p), [])
        self.assertEqual(F.guard_violations(R.render(["J-4", "J-8"]), ["J-4", "J-8"], profile=p), [])
        self.assertIn("empty_reply", F.guard_violations("", [], profile=p))
        self.assertTrue(F.guard_violations("x" * 400, ["J-2"], profile=p))

    def test_build_inputs_from_app21(self):
        rec = _rec(status="受任", ルックアップ_0="アビリオ債権回収", 最終返済日_テキスト="2021年12月", 裁判所書類="4 何も届いていない")
        inp = F.build_inputs(rec, [], "見立て", "", first_reply_sent=True, update_flag=False)
        self.assertEqual((inp.stage, inp.creditor_name, inp.court_claim, inp.court_docs_denied),
                         (S.STAGE_ENGAGED, "アビリオ債権回収", False, True))
        rec2 = _rec(status="", 問い合わせ業者名="", 裁判所書類="1 訴状が届いた")
        inp2 = F.build_inputs(rec2, [], "援用時期", "レイク", first_reply_sent=False, update_flag=False)
        self.assertEqual((inp2.stage, inp2.creditor_name, inp2.court_claim, inp2.court_docs_denied),
                         (S.STAGE_CONSULT, "レイク", True, False))
        inp3 = F.build_inputs(_rec(status="受任", 裁判所書類="分からない"), [], "見立て", "", first_reply_sent=True,
                              update_flag=True)
        self.assertTrue(inp3.court_claim)
        self.assertFalse(inp3.court_docs_denied)

    def test_shrink_candidates_remove_optional_only(self):
        self.assertEqual(list(F._shrink_candidates(("J-1", "J-4", "J-5a", "J-8"))),
                         [("J-1", "J-4", "J-5a", "J-8"), ("J-1", "J-4", "J-5a"), ("J-4", "J-5a")])
        self.assertEqual(list(F._shrink_candidates(("J-7",))), [("J-7",)])
        self.assertEqual(list(F._shrink_candidates(("J-1",))), [("J-1",)])

    def test_intent_tool_is_closed_enum(self):
        tool = F.intent_tool()
        self.assertEqual(tool["name"], F.INTENT_TOOL_NAME)
        self.assertEqual(tool["input_schema"]["properties"]["intent"]["enum"], list(T.INTENTS))
        self.assertNotIn("reply", tool["input_schema"]["properties"])


# ── 5. pin（結線・sink・allowlist） ───────────────────────────────────────────
class TestPins(unittest.TestCase):
    NEW = ("hub/jikou_reply_texts.py", "hub/jikou_reply_render.py", "hub/jikou_reply_select.py",
           "hub/jikou_reply_flow.py")

    def test_new_modules_have_zero_sink_violations_and_no_allowlist_entries(self):
        allow = (REPO / "redaction_sink_allowlist.json").read_text(encoding="utf-8")
        for name in self.NEW:
            self.assertEqual(scan_source((REPO / name).read_text(encoding="utf-8"), name), [], name)
            self.assertNotIn(name.split("/")[-1], allow)

    def test_wiring_is_single_block_and_tail_import(self):
        csrc = (REPO / "chat_responder.py").read_text(encoding="utf-8").split("\n")
        self.assertEqual(sum("jikou_reply_flow.try_closed_set(" in l for l in csrc), 1)
        self.assertIn("from hub import jikou_reply_flow", "\n".join(csrc[-6:]))
        # Q1a の pin（既存経路は対応中・decide_send を直接参照しない）
        for banned in ("set_attending", "conversation_state", "decide_send"):
            self.assertNotIn(banned, "\n".join(csrc))
        # 1 行置換の pin（Q1c）が動いていない
        self.assertEqual(sum("send_policy.apply_v2(apply_server_guards(" in l for l in csrc), 1)
        self.assertEqual(sum("send_policy.pending_ack(" in l for l in csrc), 1)

    def test_texts_module_has_no_runtime_dependencies(self):
        src = (REPO / "hub" / "jikou_reply_texts.py").read_text(encoding="utf-8")
        for banned in ("import os", "import logging", "from hub", "import chat_responder", "print("):
            self.assertNotIn(banned, src)
        self.assertIn("TABLE_SHA256_PIN = \"" + T.TABLE_SHA256_PIN + "\"", src)


if __name__ == "__main__":
    unittest.main()
