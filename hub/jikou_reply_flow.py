"""jikou_reply_flow — JIKOU-REPLY-Q2 (e): 選択器 → 描画器 → 許可表 V2（decide_send）→ send_ledger の結線（V2 ON のみ）

正本: 時効LINEボット_返信規則_v1.4.md R-J3（閉集合のみ・LLM は分類 ID を提案するだけ）・R-J4（連投禁止）・R-J5
（進められるところは進める）・§10-3（fail-closed）・§10-6（J-5 の同時通知の記録）。QUEUE-2026-10-13-D D-1 (e)。

flag JIKOU_SEND_POLICY_V2（既定 OFF）:
- OFF: 本 module は呼ばれない（chat_responder は従来経路のまま・byte 同一）。
- ON: 顧客対応経路で compose_reply の後に `try_closed_set()` を呼ぶ。
  1. compose_reply が緊急の即時定型（immediate_notice≠none）・時効更新事由フラグ・承認必須カテゴリ（緊急対応 等）を
     返した受信は選択器に回さない（既存の緊急定型・承認経路のまま）。
  2. 分類 ID は Claude の tool 強制（select_intent・閉集合 enum）で得る。文面は書かせない。障害・解析不能は fail-closed。
  3. 入力（段階・初回返信の有無・使用済み定型〔会話履歴の assistant 行と逐語照合〕・債権者名・最終返済日・裁判等の
     申告・J-H3 の否定記録）→ hub/jikou_reply_select.select → 空なら False（呼出側が無言+対応待ち=Q1c）。
  4. 描画（hub/jikou_reply_render）→ 既存ガードの検査のみ（構成・禁止語・文体・PII 残滓。J-7 は長さ免除）→ 落ちれば False。
  5. J-5 を含むときは送信前に業務 LINE へ同時通知（send_ledger 経由・固定文言・会社 ID と年月のみ）。記録を確保できなければ
     J-5（と J-8）を落として再描画。残らなければ False。
  6. 送信は呼出側の reply_func（hub/line_channel → begin() の排他の中で decide_send）。purpose=reply。
     許可表で止まれば対応待ち（policy_stopped）に登録して True（下書きは作らない＝受信入口の停止と同じ扱い）。
  7. 送信後に App 28 へ assistant 行（auto_sent=yes・category は compose_reply の値）。
- 真を返したら呼出側は従来の自動送信／承認分岐へ進まない。偽なら呼出側は LLM 自由文を自動送信せず承認へ降格する
  （R-J3・意図した挙動変更＝V2 ON のとき AI 自由文の自動送信は無い）。
- RV-10: ログは定型 ID・件数・固定語彙のみ。通知は会社 ID・年月・App 21 No. のみ。
"""

import json
import logging
import re

from hub import jikou_reply_render as R
from hub import jikou_reply_select as S
from hub import jikou_reply_texts as T
from hub import reply_pending, send_ledger, send_policy
from hub.redact import emit

logger = logging.getLogger("hub.jikou_reply_flow")

BUSINESS = send_policy.BUSINESS
INTENT_TOOL_NAME = "select_intent"
INTENT_MAX_TOKENS = 200
# compose_reply の分類がこの集合なら選択器に回さない（承認必須・緊急）
NAME_CHECK_EXEMPT_IDS = frozenset({"J-0", "J-1"})
SKIP_CATEGORIES = frozenset({"緊急対応", "クレーム・不満", "解約・辞任関係", "本人確認不能・第三者", "費用交渉・減額相談"})
_COURT_CLAIM_RE = re.compile(r"訴状|支払督促|判決|差押|差し押さえ|強制執行")
_COURT_DENIED_RE = re.compile(r"届いていない|届いてない|来ていない|来てない")
DEMOTION_REASON = "閉集合外（Q2: 定型に該当なし・自由文は自動送信しない）"
J5_NOTICE_HEADER = "【推奨時期の自動案内】"
J5_NOTICE_FOOTER = "J-5 の算定記録です（送信前の同時通知・R-J5／§10-6）。"

_INTENT_TOOL = {
    "name": INTENT_TOOL_NAME,
    "description": "相談者の直前の発話を閉集合の分類 ID に当てはめる（文面は書かない）",
    "input_schema": {
        "type": "object",
        "properties": {
            "intent": {"type": "string", "enum": list(T.INTENTS),
                       "description": "直前の相談者発話の分類 ID（最も近い 1 つ。迷えば「その他」）"},
            "creditor_name": {"type": "string",
                              "description": "直前の発話で相談者が挙げた債権者の名称（無ければ空文字）"},
        },
        "required": ["intent", "creditor_name"],
    },
}

_INTENT_SYSTEM = (
    "あなたは法律事務所の LINE 相談窓口の分類器です。相談者の直前の発話を、次の分類 ID のうち最も近い 1 つに当てはめて "
    f"{INTENT_TOOL_NAME} ツールで返してください。返信文は書きません。判断に迷う場合・複数の問いが混ざる場合・"
    "裁判所からの書類や差押えなどの緊急の話題が少しでも含まれる場合は、安全側（「緊急・裁判所書類」または「その他」）を選びます。\n\n"
    "分類 ID の定義:\n" + "\n".join(f"- {k}: {v}" for k, v in T.INTENT_DESCRIPTIONS.items())
)


def intent_tool() -> dict:
    return json.loads(json.dumps(_INTENT_TOOL, ensure_ascii=False))


async def classify_intent(messages: list[dict], *, status: str = "") -> dict:
    """Claude（tool 強制）で分類 ID と債権者名を得る。閉集合外・tool 未使用は ValueError（fail-closed）。"""
    import anthropic
    import chat_responder as cr
    from claude_gateway import create_message_with_fallback
    client = anthropic.AsyncAnthropic(api_key=cr._ANTHROPIC_KEY)
    system = _INTENT_SYSTEM + (f"\n\n案件の段階: {S.stage_for_status(status)}" if status else "")
    resp = await create_message_with_fallback(
        client, context="顧客対応 select_intent", max_tokens=INTENT_MAX_TOKENS,
        system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
        tools=[intent_tool()], tool_choice={"type": "tool", "name": INTENT_TOOL_NAME}, messages=messages)
    block = next((b for b in resp.content if getattr(b, "type", "") == "tool_use"), None)
    if not block:
        raise ValueError("intent_tool_not_called")
    data = dict(block.input or {})
    intent = str(data.get("intent") or "")
    if intent not in T.INTENTS:
        raise ValueError("intent_out_of_set")
    return {"intent": intent, "creditor_name": str(data.get("creditor_name") or "").strip()[:60]}


def _field(rec: dict, code: str) -> str:
    return str(((rec or {}).get(code) or {}).get("value") or "")


def used_ids_from_history(history: list[dict]) -> tuple:
    """会話履歴の assistant 行に定型の本文が逐語で含まれていれば使用済み（R-J4・履歴 10 往復の窓）。
    J-5 は会社を区別できないため本文一致で判定し、J-5 系は既定会社扱い（flow 側で company を付ける）。"""
    texts = [str(m.get("content") or "") for m in (history or []) if m.get("role") == "assistant"]
    used: list = []
    for tid, e in T.TEXTS.items():
        body = e["text"]
        if not body or e["slots"]:
            # 差し込みのある定型は固定部分（先頭の差し込み前）で照合
            head = body.split("【", 1)[0]
            if not head or len(head) < 8:
                continue
            if any(head in t for t in texts):
                used.append(tid)
            continue
        if any(body in t for t in texts):
            used.append(tid)
    return tuple(used)


def build_inputs(app21_record: dict, history: list[dict], intent: str, creditor_hint: str, *,
                 first_reply_sent: bool, update_flag: bool) -> S.Inputs:
    status = _field(app21_record, "status")
    creditor = (_field(app21_record, "ルックアップ_0") or _field(app21_record, "問い合わせ業者名")
                or creditor_hint or "")
    court = _field(app21_record, "裁判所書類")
    court_claim = bool(update_flag) or bool(_COURT_CLAIM_RE.search(court))
    court_denied = (not court_claim) and bool(court) and bool(_COURT_DENIED_RE.search(court))
    used = list(used_ids_from_history(history))
    rule = T.rule_for(creditor)
    used += [f"{i}@{rule['company_id']}" for i in list(used) if i in T.J5_IDS]
    return S.Inputs(stage=S.stage_for_status(status), intent=intent, first_reply_sent=first_reply_sent,
                    used_ids=tuple(used), creditor_name=creditor.strip(),
                    last_payment_text=_field(app21_record, "最終返済日_テキスト"),
                    court_claim=court_claim, court_docs_denied=court_denied)


def guard_violations(text: str, ids, *, profile) -> list[str]:
    """既存ガードの検査のみ（R-J13）: 構成（長さ・質問数。J-7 は免除）・禁止語・文体（J-0/J-1 は名乗り免除）・PII 残滓・空文字。"""
    import chat_responder as cr
    from hub import empty_reply_guard, pii_scan, reply_sanitizer
    v: list[str] = []
    v += reply_sanitizer.structure_violations(text, exempt_blocks=R.length_exempt_blocks(ids))
    hits = cr.find_forbidden_words(text, profile=profile)
    if hits:
        v.append("forbidden_words")
    styled = text
    for i in ids:
        if i in NAME_CHECK_EXEMPT_IDS:
            styled = styled.replace(T.text(i), "")       # J-0/J-1 は名乗り検査を ID で免除（§10-3）
    v += cr.style_guard_violations(styled, route=profile.customer_style_route)
    if pii_scan.residue_hits(text):
        v.append("pii_residue")
    if empty_reply_guard.empty_reply_labels(text) == [empty_reply_guard.LABEL_EMPTY]:
        v.append("empty_reply")                              # 語彙表は閉集合の定型には適用しない（検査は空文字のみ）
    return v


OPTIONAL_IDS = ("J-8", "J-1")     # 300 字を超えるとき後ろから外してよい定型（締め・自己紹介）


def _shrink_candidates(ids):
    """文字数超過のときだけ任意の定型（J-8→J-1）を外した候補を順に返す（本文は変えない・外すだけ）。"""
    cur = list(ids)
    yield tuple(cur)
    for opt in OPTIONAL_IDS:
        if opt in cur and len(cur) > 1:
            cur = [i for i in cur if i != opt]
            yield tuple(cur)


async def _j5_notice(user_id: str, app21_record: dict, sel: S.Selection) -> bool:
    """J-5 の同時通知（送信前）。台帳に記録できた（sent）ときだけ True。"""
    d = sel.j5_detail or {}
    conv = None
    try:
        conv = await send_ledger.conversation_state(BUSINESS, user_id)
    except Exception:
        conv = None
    user_hash = send_ledger.line_user_hash(BUSINESS, user_id)
    key = f"j5_notice:{(conv or {}).get('ref') or user_hash[:16]}:{d.get('company_id', '')}"
    rid = _field(app21_record, "$id")
    text = (f"{J5_NOTICE_HEADER}\nApp 21 No.{rid or '不明'}・会社 ID: {d.get('company_id', '')}（規則版 {d.get('rule_version', '')}）"
            f"\n推奨 {d.get('recommended', '')}／基準 {d.get('base', '')}\n{J5_NOTICE_FOOTER}")
    state = await send_policy.send_business_notice(key, text)
    return state == reply_pending.NOTIFY_SENT


async def try_closed_set(user_id: str, user_message: str, app21_record: dict, history: list[dict], result: dict,
                         *, profile, reply_token: str, reply_func, category: str) -> bool:
    """V2 ON の顧客対応経路。閉集合の定型で返せたら（または許可表で止めて対応待ちにしたら）True。
    それ以外は False（呼出側は自由文を自動送信せず承認へ降格＝無言+対応待ち）。"""
    import chat_responder as cr
    if not send_policy.enabled():
        return False
    if (result.get("immediate_notice") or "none") != "none" or bool(result.get(profile.update_flag_key)) \
            or category in SKIP_CATEGORIES:
        logger.info("[Q2] skipped (urgent/approval-only) category_known=%s", emit(int(category in SKIP_CATEGORIES), "count", "log", "operator"))
        return False
    try:
        cls = await classify_intent(list(history) + [{"role": "user", "content": user_message}],
                                    status=_field(app21_record, "status"))
    except Exception:
        logger.info("[Q2] intent classification failed (fail-closed)")
        return False
    try:
        n = await send_ledger.first_reply_count(BUSINESS, user_id)
    except Exception:
        n = None
    inp = build_inputs(app21_record, history, cls["intent"], cls["creditor_name"],
                       first_reply_sent=(n is None or n > 0), update_flag=bool(result.get(profile.update_flag_key)))
    sel = S.select(inp)
    logger.info("[Q2] selected ids=%s dropped=%s", emit(",".join(sel.ids), "freetext", "log", "operator"),
                emit(",".join(sel.reasons), "freetext", "log", "operator"))
    if sel.empty:
        # JIKOU-REPLY-Q5 (b): 昇格定型（データ表・category 対応）を 1 件。履歴に同文があれば送らない（R-J4）
        return await _try_promoted(user_id, history, category, profile=profile, reply_token=reply_token,
                                   reply_func=reply_func)
    if sel.j5_detail is not None:
        if not await _j5_notice(user_id, app21_record, sel):
            sel = S.drop_j5(sel)
            logger.info("[Q2] J-5 dropped (notice not recorded)")
            if sel.empty:
                return False
    text = ""
    for ids in _shrink_candidates(sel.ids):
        try:
            cand = R.render(ids, sel.slots)
        except R.RenderError as e:
            logger.info("[Q2] render failed reason=%s", emit(str(e), "freetext", "log", "operator"))
            return False
        v = guard_violations(cand, ids, profile=profile)
        if not v:
            text, sel = cand, S.Selection(tuple(ids), sel.slots, company_id=sel.company_id, j5_detail=sel.j5_detail,
                                          reasons=sel.reasons)
            break
        if any(not x.startswith("文字数超過") for x in v):
            logger.info("[Q2] guard rejected count=%s", emit(len(v), "count", "log", "operator"))
            return False
    if not text:
        logger.info("[Q2] guard rejected (length)")
        return False
    res = await send_ledger.with_purpose(send_ledger.PURPOSE_REPLY, reply_func, reply_token, text)
    if res == send_ledger.SEND_POLICY_STOPPED:
        await send_policy._register_and_notify(user_id, route=reply_pending.ROUTE_CUSTOMER,
                                               reason=reply_pending.REASON_POLICY_STOPPED, category=category,
                                               approval_record_id=None)
        logger.info("[Q2] stopped by policy (pending registered)")
        return True
    await cr.save_to_chatlog(user_id, "assistant", text, category, "yes")
    logger.info("[Q2] closed-set reply sent ids=%s len=%s", emit(",".join(sel.ids), "freetext", "log", "operator"),
                emit(len(text), "count", "log", "operator"))
    return True


# ── JIKOU-REPLY-Q5（QUEUE-2026-10-13-D D-4）: 昇格定型の選択（Q2 の表で該当なしのとき）。末尾追記。
async def _try_promoted(user_id: str, history: list[dict], category: str, *, profile, reply_token: str,
                        reply_func) -> bool:
    import chat_responder as cr
    from hub import jikou_reply_promote
    row = await jikou_reply_promote.pick_for_category(category)
    if not row:
        return False
    text = str(row["text"])
    if any(text in str(m.get("content") or "") for m in (history or []) if m.get("role") == "assistant"):
        logger.info("[Q5] promoted text already used in conversation")
        return False
    v = guard_violations(text, (), profile=profile)
    if v:
        logger.info("[Q5] guard rejected count=%s", emit(len(v), "count", "log", "operator"))
        return False
    res = await send_ledger.with_purpose(send_ledger.PURPOSE_REPLY, reply_func, reply_token, text)
    if res == send_ledger.SEND_POLICY_STOPPED:
        await send_policy._register_and_notify(user_id, route=reply_pending.ROUTE_CUSTOMER,
                                               reason=reply_pending.REASON_POLICY_STOPPED, category=category,
                                               approval_record_id=None)
        return True
    await cr.save_to_chatlog(user_id, "assistant", text, category, "yes")
    logger.info("[Q5] promoted reply sent id=%s", emit(str(row["text_id"]), "freetext", "log", "operator"))
    return True
