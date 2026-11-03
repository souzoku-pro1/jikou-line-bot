"""jikou_reply_select — JIKOU-REPLY-Q2 (b): 選択器（入力 → 定型 ID の閉集合・サーバ側判定のみ・LLM なし）

正本: 時効LINEボット_返信規則_v1.4.md §2（段階）・§3（各定型の個別条件）・§5（債権者別ルール表）・§10-3・§10-4
（適用条件はサーバ側が最終判定）。QUEUE-2026-10-13-D D-1 (b)。

入力（Inputs）: 段階（相談／受任後／不明）・会話状態（初回返信の有無・使用済み定型）・直前の相談者発話の分類 ID
（閉集合・LLM の tool 強制で得る＝hub/jikou_reply_flow）・債権者名・最終返済日の申告・裁判等の申告・J-H3 の否定記録。
出力（Selection）: 定型 ID の列（0〜3・描画順）と差し込み値。該当なし（空）は fail-closed（呼出側が無言+対応待ち）。

規則:
- 選べるのは hub/jikou_reply_texts.is_selectable（status=frozen_v14・auto=True）の定型だけ。要承認・下書きのみの定型は
  選ばない（fail-closed）。
- 段階: 受任後限定（stage=engaged）の定型は受任後のみ。段階不明は段階非依存のみ（「相談段階」へ変換しない）。
- J-4: 裁判・差押え・判決確定の申告があれば不可。J-12: J-H3 の否定記録（届いていない）がある場合のみ・申告と併用禁止。
  否定記録が無ければ J-H3 を聞く。
- J-5: 債権者別ルール表の確定行（RULE_CONFIRMED）・最終返済日の申告（parse 可）・裁判等の申告なし のときだけ。
  行が仮・申告なし・解釈不能なら J-5 を返さず J-H1 を聞く（表に無い業者は既定行）。J-5b は文言確定（frozen）まで不可。
  同時通知の記録は hub/jikou_reply_flow が送信前に確保する（確保できなければ J-5 を落とす）。
- 連投禁止（R-J4）: used_ids（会話で使用済みの定型 ID）は選ばない。J-5 は会社ごと（used_ids に "J-5a@company_id"）。
- J-1（自己紹介）: 会話に初回返信が無い（first_reply_sent=False）とき、実質回答の冒頭に付ける。
- 聞き取りは 1 回の返信で 2 問まで・定型は合計 3 件まで（§10-3 の出力スキーマ）。
"""

import dataclasses
import datetime

from hub import jikou_reply_render as R
from hub import jikou_reply_texts as T

STAGE_CONSULT = "consult"
STAGE_ENGAGED = "engaged"
STAGE_UNKNOWN = "unknown"
STAGES = (STAGE_CONSULT, STAGE_ENGAGED, STAGE_UNKNOWN)

CONSULT_STATUSES = frozenset({"", "受付", "問い合わせ", "受任前"})
ENGAGED_STATUSES = frozenset({"受任", "手続き中", "完了"})

MAX_IDS = 3
MAX_HEARING = 2

NO_SELECTION_REASON_NO_RULE = "no_rule"
NO_SELECTION_REASON_NOT_SELECTABLE = "not_selectable"
NO_SELECTION_REASON_STAGE = "stage"
NO_SELECTION_REASON_USED = "used"
NO_SELECTION_REASON_CONDITION = "condition"


def stage_for_status(status: str | None) -> str:
    s = (status or "").strip()
    if s in ENGAGED_STATUSES:
        return STAGE_ENGAGED
    if s in CONSULT_STATUSES:
        return STAGE_CONSULT
    return STAGE_UNKNOWN


@dataclasses.dataclass(frozen=True)
class Inputs:
    stage: str                                  # consult / engaged / unknown
    intent: str                                 # 分類 ID（T.INTENTS）
    first_reply_sent: bool = True               # 会話に初回返信判定の対象が 1 件以上ある
    used_ids: tuple = ()                        # 使用済み定型 ID（J-5 は "J-5a@company_id"）
    creditor_name: str = ""                     # 債権者名（App 21・差し込み／ルール表の引当）
    last_payment_text: str = ""                 # 最終返済日の申告（App 21 最終返済日_テキスト）
    court_claim: bool = False                   # 裁判・差押え・判決確定の申告あり
    court_docs_denied: bool = False             # J-H3 の否定記録（裁判所書類は届いていない）
    eval_date: datetime.date | None = None      # 評価日（送信日・None=今日 JST）


@dataclasses.dataclass(frozen=True)
class Selection:
    ids: tuple                                  # 描画順の定型 ID（0〜3）
    slots: dict                                 # 差し込み値
    company_id: str = ""                        # J-5 を含むときの会社 ID
    j5_detail: dict | None = None               # J-5 の算定（通知記録用・PII なし）
    reasons: tuple = ()                         # 落とした候補の固定語彙（ログ用）

    @property
    def empty(self) -> bool:
        return not self.ids


def _today() -> datetime.date:
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).date()


def _stage_ok(text_id: str, stage: str) -> bool:
    e = T.entry(text_id) or {}
    if e.get("stage") == T.STAGE_ENGAGED:
        return stage == STAGE_ENGAGED
    return True


def _j5(inp: Inputs, reasons: list) -> tuple[str | None, dict, str, dict | None]:
    """J-5 の判定。戻り値: (text_id|None, slots, company_id, detail)。"""
    rule = T.rule_for(inp.creditor_name)
    cid = rule["company_id"]
    if inp.court_claim:
        reasons.append("J-5:" + NO_SELECTION_REASON_CONDITION)
        return None, {}, cid, None
    if rule["status"] != T.RULE_CONFIRMED:
        reasons.append("J-5:rule_provisional")
        return None, {}, cid, None
    tid = rule["j5"]
    if not T.is_selectable(tid):
        reasons.append(f"{tid}:" + NO_SELECTION_REASON_NOT_SELECTABLE)
        return None, {}, cid, None
    if f"{tid}@{cid}" in inp.used_ids:
        reasons.append(f"{tid}:" + NO_SELECTION_REASON_USED)
        return None, {}, cid, None
    upper = R.parse_last_payment(inp.last_payment_text)
    if upper is None:
        reasons.append("J-5:no_last_payment")
        return None, {}, cid, None
    d = R.j5_dates(upper, inp.eval_date or _today(), period_years=int(rule["period_years"]),
                   margin_months=int(rule["margin_months"]), safety_months=int(rule["safety_months"]))
    slots = {T.SLOT_RECOMMENDED_MONTH: d["recommended_display"], T.SLOT_BASE_MONTH: d["base_display"]}
    detail = {"company_id": cid, "rule_version": int(rule["version"]), "basis": rule["basis"],
              "recommended": d["recommended_display"], "base": d["base_display"]}
    return tid, slots, cid, detail


def select(inp: Inputs) -> Selection:
    """決定表（T.INTENT_RULES）と個別条件から定型 ID の閉集合を返す（空=fail-closed）。"""
    reasons: list = []
    if not T.verify_pin() or inp.intent not in T.INTENTS or inp.stage not in STAGES:
        return Selection((), {}, reasons=("invalid_input",))
    rule = T.INTENT_RULES[inp.intent]
    plan = list(rule["engaged"] if inp.stage == STAGE_ENGAGED else rule["any"])
    if not plan:
        return Selection((), {}, reasons=(NO_SELECTION_REASON_NO_RULE,))
    ids: list = []
    slots: dict = {}
    company_id = ""
    j5_detail = None
    hearing: list = []
    for item in plan:
        if item == "J-5?":
            tid, s, company_id, j5_detail = _j5(inp, reasons)
            if tid:
                ids.append(tid)
                slots.update(s)
            else:
                hearing.append("J-H1")
            continue
        if item == "J-12?":
            if inp.court_claim:
                reasons.append("J-12:" + NO_SELECTION_REASON_CONDITION)
                continue
            if inp.court_docs_denied:
                ids.append("J-12")
            else:
                hearing.append("J-H3")
            continue
        if item == "J-4" and inp.court_claim:
            reasons.append("J-4:" + NO_SELECTION_REASON_CONDITION)
            continue
        if item == "J-8" and not any(i in T.J5_IDS for i in ids):
            continue        # 待機の締めは推奨時期を示したときだけ
        if item == "J-9":
            name = (inp.creditor_name or "").strip()
            if not name or name == "（未登録）":
                hearing.append("J-H2")
                reasons.append("J-9:slot_missing")
                continue
            slots[T.SLOT_CREDITORS] = name
        ids.append(item)
    # 段階・選択可否・使用済みの検査（候補ごと）
    final: list = []
    for tid in ids:
        if not _stage_ok(tid, inp.stage):
            reasons.append(f"{tid}:" + NO_SELECTION_REASON_STAGE)
            continue
        if not T.is_selectable(tid):
            reasons.append(f"{tid}:" + NO_SELECTION_REASON_NOT_SELECTABLE)
            continue
        if tid in inp.used_ids:
            reasons.append(f"{tid}:" + NO_SELECTION_REASON_USED)
            continue
        final.append(tid)
    for hid in hearing[:MAX_HEARING]:
        if _stage_ok(hid, inp.stage) and T.is_selectable(hid) and hid not in inp.used_ids and hid not in final:
            final.append(hid)
        else:
            reasons.append(f"{hid}:" + NO_SELECTION_REASON_NOT_SELECTABLE)
    if not final:
        return Selection((), {}, reasons=tuple(reasons))
    final = final[:MAX_IDS]
    if not inp.first_reply_sent and T.is_selectable("J-1") and "J-1" not in inp.used_ids:
        final.insert(0, "J-1")          # 自己紹介は 3 件の枠に数えない（冒頭に付ける 1 文）
    if not any(i in T.J5_IDS for i in final):
        company_id, j5_detail = "", None
        slots.pop(T.SLOT_RECOMMENDED_MONTH, None)
        slots.pop(T.SLOT_BASE_MONTH, None)
    return Selection(tuple(final), slots, company_id=company_id, j5_detail=j5_detail, reasons=tuple(reasons))


def drop_j5(sel: Selection) -> Selection:
    """同時通知の記録を確保できなかったとき J-5（と締めの J-8）を落とす。"""
    ids = tuple(i for i in sel.ids if i not in T.J5_IDS and i != "J-8")
    return Selection(ids, {k: v for k, v in sel.slots.items() if k == T.SLOT_CREDITORS},
                     reasons=sel.reasons + ("J-5:notice_unrecorded",))
