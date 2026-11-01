"""send_policy — JIKOU-REPLY-Q1c: 許可表 V2（bot／承認済み下書き列）の適用・空返事の無言化・J-0・対応待ちの結線

正本: 時効LINEボット_返信規則_v1.4.md §1 R-J1（無言+対応待ち+通知）・R-J2（初回 1 通）・R-J10／§10-2
（送信主体別の許可表・判定不能の分離）・§10-5（J-0 の判定式は共通排他の中）・§10-6（対応待ち）・§12 Q1c
（許可表の切替＝差分承認）。QUEUE-2026-10-12-C C-1 (a)〜(d)。

flag JIKOU_SEND_POLICY_V2（send_ledger.policy_v2_enabled・既定 OFF）:
- OFF: 本 module の関数は従来の値・従来の呼び出し（PENDING_REPLY の送信・pending_reply_for）をそのまま返す
  ＝既存経路は byte 同一の挙動（test_jikou_reply_q1c が pin）。
- ON（配備後に大野が env で切替）:
  (a) bot／承認済み下書けの送信は send_ledger.begin() の共通排他の中で decide_send（§10-2）を通す。停止条件は
      hub/line_channel が送信前に集める（conditions_if_applicable）: AUTOREPLY_PAUSED（env）・App 39 停止リスト
      （照会失敗＝unknown＝bot は停止）・App 21 response_mode（照会失敗＝unknown）。対応中（attending）は
      排他の中で会話行から読む。承認済み下書きは会話版・承認版の一致を App 29 で持てないため
      versions_match=False（安全側＝人対応中・対応中は送らず通知し、再承認待ちにする）。
  (b) 空返事（hub/empty_reply_guard）は自動送信せず無言。承認降格時の PENDING_REPLY（文脈化定型を含む）も
      送らない（即時定型 6 種は閉集合の承認済み文＝従来どおり送る）。障害・例外時も無言。
  (c) J-0（初回 1 通・司令塔仮置き・大野承認待ち）: 無言にする場面で、その会話に「初回返信判定の対象」の
      送信操作が 1 件も無い（pending/started/sent/unconfirmed いずれも 0 件）ときだけ 1 回送る。
      判定は送信前（first_reply_due）と begin() の排他の中（purpose=first_reply の在庫検査）の二重。
      会話が無い（非 durable 経路）ときは送らない（1 回性を担保できない）。
  (d) 無言にした受信は hub/reply_pending（対応待ち）に永続化し、業務 LINE へ固定文言（App 29 No.・カテゴリ
      のみ・PII なし）を send_ledger 経由（purpose=other・channel=jikou_notice）で **1 会話 1 日 1 回** 通知する。
      対応待ちの保存に失敗したら顧客へは送らない（J-0 も送らない）。
- 判定不能の分離: App 21 の照会失敗は HRI-08 の fail-closed（無言）が先に働く。App 39 の照会失敗は OFF では
  fail-open（従来）、ON では unknown＝bot 停止・対応待ち・通知。
- RV-10: 本 module のログ・通知に本文・氏名・LINE userId を載せない（通知は App 29 No.・カテゴリ・固定文のみ）。
- 本 module は chat_responder を遅延 import する（chat_responder → hub の循環を避ける）。
"""

import contextlib
import contextvars
import dataclasses
import datetime
import functools
import hashlib
import logging
import os
import re
import time

import sqlalchemy as sa

from hub import empty_reply_guard, jikou_reply_texts, kintone, reply_pending, send_ledger
from hub.db import session_scope as db_session_scope
from hub.redact import emit

logger = logging.getLogger("hub.send_policy")

BUSINESS = "jikou"
NOTICE_CHANNEL = "jikou_notice"          # 業務 LINE への対応待ち通知（送信操作記録のチャネル名）
CUSTOMER_CHANNEL = "jikou"               # 許可表 V2 を適用する顧客チャネル（hub/line_channel.JIKOU_CHANNEL.name）
CONDITION_TTL_SECONDS = 30.0             # 停止条件の照会結果の再利用窓（受信入口と送信前で 2 度引かない）
_JST = datetime.timezone(datetime.timedelta(hours=9))

# J-0（初回受付・1 会話 1 回・司令塔仮置き・大野承認待ち）。文言変更は本定数と test の sha pin を同時に更新する
J0_TEXT = jikou_reply_texts.text("J-0")   # JIKOU-REPLY-Q2: データ表（hub/jikou_reply_texts）が正・値は C-1 と同一
J0_TEXT_SHA256 = hashlib.sha256(J0_TEXT.encode("utf-8")).hexdigest()

# 業務 LINE への固定文言（PII なし・App 29 No. とカテゴリは統制値）
NOTICE_HEADER = "【対応待ち】自動返信できない問い合わせがあります。"
NOTICE_FOOTER = "返信欄（/app/reply/承認キュー No.）からご返信ください。同じ相談者への本通知は本日これ以降省略します。"
NOTICE_NO_APPROVAL = "承認キュー: 未作成（/app/pending で確認）"
APPROVAL_BLOCKED_HEADER = "【承認済み下書き・未送信】"
APPROVAL_BLOCKED_FOOTER = "停止条件が解けてから再承認してください（人対応中・対応中の返信は返信欄から）。"

EMPTY_REPLY_REASON_PREFIX = "空返事: "

_last_stop: contextvars.ContextVar = contextvars.ContextVar("send_policy_last_stop", default=())
_APP_APPROVAL = kintone.KintoneApp("App 29 (承認キュー)", "APP_APPROVAL", "TOKEN_APPROVAL")
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
_condition_cache: dict = {}     # user_hash → (monotonic, conditions)


# ── flag ─────────────────────────────────────────────────────────────────────
def enabled() -> bool:
    return send_ledger.policy_v2_enabled()


def _cr():
    import chat_responder
    return chat_responder


# ── 停止条件の収集（(a)） ──────────────────────────────────────────────────────
def _paused_value() -> str:
    return send_ledger.STOPPED if os.environ.get("AUTOREPLY_PAUSED") == "1" else send_ledger.CLEAR


async def _stoplist_value(user_id: str) -> str:
    from hub import autoreply_stoplist
    outcome = await autoreply_stoplist.check(user_id)
    if outcome == autoreply_stoplist.OUTCOME_STOPPED:
        return send_ledger.STOPPED
    if outcome == autoreply_stoplist.OUTCOME_UNKNOWN:
        return send_ledger.UNKNOWN
    return send_ledger.CLEAR


async def _human_mode_value(user_id: str) -> str:
    cr = _cr()
    try:
        rec = await cr.get_app21_record(user_id)
    except Exception:
        return send_ledger.UNKNOWN
    return send_ledger.STOPPED if cr.is_human_mode(rec) else send_ledger.CLEAR


async def conditions(user_id: str, *, now=None) -> dict:
    """停止条件の 3 値（paused / stoplisted / human_mode）。短い窓で再利用する（同一相談者）。"""
    key = send_ledger.line_user_hash(BUSINESS, user_id)
    t = time.monotonic() if now is None else now
    hit = _condition_cache.get(key)
    if hit and t - hit[0] < CONDITION_TTL_SECONDS:
        return dict(hit[1])
    cond = {"paused": _paused_value(),
            "stoplisted": await _stoplist_value(user_id),
            "human_mode": await _human_mode_value(user_id)}
    _condition_cache[key] = (t, dict(cond))
    return cond


def clear_condition_cache() -> None:
    _condition_cache.clear()


async def conditions_if_applicable(channel_name: str, to: str) -> dict | None:
    """hub/line_channel の送信前フック: V2 ON・顧客チャネル・主体が human 以外のときだけ停止条件を集めて返す。
    それ以外（OFF・他業務・業務通知・人の明示送信）は None＝従来どおり。"""
    if not enabled() or channel_name != CUSTOMER_CHANNEL:
        return None
    ctx = send_ledger.current_context()
    actor = ctx.actor if ctx else send_ledger.ACTOR_BOT
    if actor == send_ledger.ACTOR_HUMAN:
        return None
    cond = await conditions(to)
    # 承認済み下書き: 会話版・承認版の一致を持てない（App 29 に欄が無い）＝不一致扱い（安全側）
    cond["versions_match"] = (actor == send_ledger.ACTOR_BOT)
    return cond


def record_stop(reasons) -> None:
    _last_stop.set(tuple(reasons or ()))


def last_stop_reasons() -> tuple:
    return tuple(_last_stop.get())


async def bot_stop_reasons(user_id: str) -> tuple:
    """受信の入口（main）用: V2 ON で bot の自動送信が許可表に落ちる理由（固定語彙）。OFF は ()。
    AUTOREPLY_PAUSED・人対応は上流で先に処理されているため、ここで立つのは主に
    attending:stopped（対応中）と stoplisted:unknown（App 39 照会失敗）。"""
    if not enabled():
        return ()
    cond = await conditions(user_id)
    attending = False
    unknown = False
    try:
        st = await send_ledger.conversation_state(BUSINESS, user_id)
        attending = bool(st and st.get("attending"))
    except Exception:
        unknown = True          # fix1 BC-01: 会話状態（対応中）が確認できない＝停止（fail-closed）
    d = send_ledger.decide_send(send_ledger.ACTOR_BOT, paused=cond["paused"],
                                stoplisted=cond["stoplisted"], human_mode=cond["human_mode"],
                                attending=attending)
    reasons = () if d.allow else tuple(d.reasons)
    if unknown:
        await outage_stop(REASON_ATTENDING_UNKNOWN)
        reasons = reasons + (REASON_ATTENDING_UNKNOWN,)
    return reasons


# ── 空返事（(b)） ──────────────────────────────────────────────────────────────
def empty_reply_violations(text: str) -> list[str]:
    """ヒアリング経路の違反列へ連結する（V2 ON のときだけ空返事を違反として返す）。"""
    if not enabled():
        return []
    labels = empty_reply_guard.empty_reply_labels(text)
    return [EMPTY_REPLY_REASON_PREFIX + "、".join(labels)] if labels else []


def apply_v2(guard, reply_text: str):
    """顧客対応経路: apply_server_guards の結果に空返事検査を重ねる。
    - V2 ON: 自動送信可でも空返事なら降格（即時定型なし＝無言・対応待ち）。
    - V2 OFF: 空文字（空白のみ）だけは送れないため降格（従来の確認中定型で応答）。語彙表は適用しない。"""
    if not getattr(guard, "can_auto_send", False):
        return guard
    labels = empty_reply_guard.empty_reply_labels(reply_text)
    if not labels:
        return guard
    if not enabled() and labels != [empty_reply_guard.LABEL_EMPTY]:
        return guard
    return dataclasses.replace(
        guard, can_auto_send=False,
        demotion_reasons=list(guard.demotion_reasons) + [EMPTY_REPLY_REASON_PREFIX + "、".join(labels)],
        immediate_notice="none")


def _reason_from_guard(guard) -> str:
    for r in getattr(guard, "demotion_reasons", []) or []:
        if str(r).startswith(EMPTY_REPLY_REASON_PREFIX):
            return reply_pending.REASON_EMPTY_REPLY
    return reply_pending.REASON_DEMOTED


# ── J-0（(c)） ────────────────────────────────────────────────────────────────
async def first_reply_due(user_id: str) -> bool:
    """R-J2 の判定式（送信前の読取・排他内の在庫検査は send_ledger.begin）。会話が無ければ False。"""
    try:
        n = await send_ledger.first_reply_count(BUSINESS, user_id)
    except Exception:
        return False
    return n == 0


# ── 対応待ちと通知（(d)） ──────────────────────────────────────────────────────
def _ctx_event_id() -> str | None:
    ctx = send_ledger.current_context()
    return ctx.inbound_event_id if ctx else None


async def _conversation(user_id: str) -> dict | None:
    try:
        return await send_ledger.conversation_state(BUSINESS, user_id)
    except Exception:
        return None


def notice_key(conv_ref: str, user_hash: str, *, today: datetime.date | None = None) -> str:
    day = (today or datetime.datetime.now(_JST).date()).isoformat()
    return f"pending_notice:{conv_ref or user_hash[:16]}:{day}"


def notice_text(approval_record_id: str | None, category: str | None) -> str:
    rid = f"承認キュー No.{approval_record_id}" if approval_record_id else NOTICE_NO_APPROVAL
    cat = f"・カテゴリ: {category}" if category else ""
    return f"{NOTICE_HEADER}\n{rid}{cat}\n{NOTICE_FOOTER}"


async def _push_business_http(to: str, text: str) -> bool:
    from hub import notify
    return await notify.push_line_message(to, text, token_env=notify.business_token_env())


async def send_business_notice(key: str, text: str) -> str:
    """台帳（business=jikou・channel=jikou_notice・purpose=other・受信イベント ID=key）を通した業務 LINE 通知。
    同じ key は 1 通（DUPLICATE_SENT → throttled）。戻り値は reply_pending.NOTIFY_* の閉集合。"""
    to = os.environ.get("ATTORNEY_LINE_USER_ID", "")
    if not to:
        logger.info("[SEND_POLICY] notice skipped (ATTORNEY_LINE_USER_ID not set)")
        return reply_pending.NOTIFY_FAILED
    token = send_ledger.bind_inbound(key)
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(BUSINESS, NOTICE_CHANNEL, to, text)
        if op is send_ledger.DUPLICATE_SENT:
            return reply_pending.NOTIFY_THROTTLED
        if op is send_ledger.DUPLICATE_UNCONFIRMED:
            return reply_pending.NOTIFY_UNCONFIRMED
        if op is None or not isinstance(op, send_ledger.Operation):
            logger.warning("[SEND_POLICY] notice skipped (ledger unavailable) count=%s",
                           emit(1, "count", "log", "operator"))
            return reply_pending.NOTIFY_UNAVAILABLE
        try:
            ok = await send_ledger.guarded_http(op, _push_business_http(to, text))
        except Exception:
            await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
            return reply_pending.NOTIFY_UNCONFIRMED
        led = await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
        if not ok:
            return reply_pending.NOTIFY_FAILED
        return reply_pending.NOTIFY_SENT if led == "applied" else reply_pending.NOTIFY_UNCONFIRMED
    finally:
        send_ledger.unbind(token)


async def _register_and_notify(user_id: str, *, route: str, reason: str, category: str | None,
                               approval_record_id: str | None) -> int | None:
    conv = await _conversation(user_id)
    user_hash = send_ledger.line_user_hash(BUSINESS, user_id)
    pid = await reply_pending.register(
        business=BUSINESS, channel=CUSTOMER_CHANNEL, line_user_hash=user_hash, route=route, reason=reason,
        inbound_event_id=_ctx_event_id(), conversation_id=(conv or {}).get("conversation_id"),
        category=category, approval_record_id=approval_record_id)
    if pid is None:
        # fix1 BC-03: 保存失敗は受信イベント ID を永続キューに残し、回収ジョブで register → 通知 を再処理
        await reply_pending.enqueue_retry(
            business=BUSINESS, channel=CUSTOMER_CHANNEL, inbound_event_id=_ctx_event_id(), line_user_hash=user_hash,
            route=route, reason=reason, conversation_id=(conv or {}).get("conversation_id"), category=category,
            approval_record_id=approval_record_id)
        return None
    logger.info("[SEND_POLICY] pending registered route=%s reason=%s", emit(route, "freetext", "log", "operator"),
                emit(reason, "freetext", "log", "operator"))
    state = await send_business_notice(notice_key((conv or {}).get("ref", ""), user_hash),
                                       notice_text(approval_record_id, category))
    await reply_pending.set_notify_state(pid, state)
    return pid


async def _maybe_j0(user_id: str, pid: int, send) -> str:
    """J-0 を送る条件なら送って J0_TEXT を返す（send は async (text) -> None|str）。それ以外は ""。"""
    if not await first_reply_due(user_id):
        return ""
    with send_ledger.purpose(send_ledger.PURPOSE_FIRST_REPLY):
        try:
            res = await send(J0_TEXT)
        except Exception:
            # fix1 BC-05: 通信例外＝結果不明（台帳は unconfirmed）。送信済みとは記録しない
            await reply_pending.set_customer_send(pid, reply_pending.CUSTOMER_J0_UNCONFIRMED)
            raise
    state = j0_state_for(res)
    await reply_pending.set_customer_send(pid, state)
    return J0_TEXT if state == reply_pending.CUSTOMER_J0_SENT else ""


# ── 各経路の入口（chat_responder／main の 1 行置換から呼ばれる） ─────────────────
async def pending_ack(user_id: str, approval_record_id: str | None, category: str, guard, *, profile=None) -> str:
    """顧客対応経路の承認降格時の応答文。OFF: 従来の pending_reply_for。ON: 対応待ち登録+通知し、
    J-0 の条件なら J0_TEXT（呼出側が purpose=first_reply で送る）、それ以外は ""（無言）。"""
    cr = _cr()
    if not enabled():
        return cr.pending_reply_for(category, profile=profile)
    pid = await _register_and_notify(user_id, route=reply_pending.ROUTE_CUSTOMER,
                                     reason=_reason_from_guard(guard), category=category,
                                     approval_record_id=approval_record_id)
    if pid is None:
        return ""
    if not await first_reply_due(user_id):
        return ""
    _j0_pid.set(pid)        # fix1 BC-05: 送信結果（send_ack → ack_recorded）を見てから customer_send を記録する
    return J0_TEXT


def ack_purpose(profile, guard, ack_text: str) -> str:
    """送信操作記録の用途: 緊急の即時定型=urgent・J-0=first_reply・他=reply（OFF の値は従来と同一）。"""
    if profile.urgent_notice_kinds.get(getattr(guard, "immediate_notice", ""), ""):
        return send_ledger.PURPOSE_URGENT
    if enabled() and ack_text == J0_TEXT:
        return send_ledger.PURPOSE_FIRST_REPLY
    return send_ledger.PURPOSE_REPLY


def settle_demoted_history(history: list, pending_text: str) -> None:
    """ヒアリング経路: 降格した下書きをメモリ履歴からどう扱うか。OFF: 従来どおり確認中定型に置換。
    ON: 送っていない文を履歴に残さない（assistant 行を除く）。呼出側の `if history and
    history[-1]["role"] == "assistant"` の内側で呼ばれる。"""
    if not enabled():
        history[-1]["content"] = pending_text
        return
    history.pop()


async def demoted_ack(user_id: str, reply_token: str, approval_record_id: str | None, category: str,
                      send, pending_text: str) -> str:
    """ヒアリング経路の送信ゲート降格時の応答。OFF: 従来どおり pending_text を send(reply_token, user_id, text)
    で送り、その文を返す。ON: 対応待ち登録+通知し、J-0 の条件なら送って J0_TEXT、それ以外は ""（無言）。"""
    if not enabled():
        await send(reply_token, user_id, pending_text)
        return pending_text
    pid = await _register_and_notify(user_id, route=reply_pending.ROUTE_HEARING,
                                     reason=reply_pending.REASON_DEMOTED, category=category,
                                     approval_record_id=approval_record_id)
    if pid is None:
        return ""

    async def _send(text: str):
        return await send(reply_token, user_id, text)
    return await _maybe_j0(user_id, pid, _send)


async def outage_ack(user_id: str, reply_token: str, reply_func, pending_text: str, *,
                     category: str = "") -> str:
    """Claude 障害・例外時の応答。OFF: 従来どおり reply_func(reply_token, pending_text)。ON: 無言
    （対応待ち reason=claude_outage・通知・J-0 の条件なら 1 通）。"""
    if not enabled():
        await reply_func(reply_token, pending_text)
        return pending_text
    pid = await _register_and_notify(user_id, route=reply_pending.ROUTE_CUSTOMER,
                                     reason=reply_pending.REASON_OUTAGE, category=(category or None),
                                     approval_record_id=None)
    if pid is None:
        return ""

    async def _send(text: str):
        return await reply_func(reply_token, text)
    return await _maybe_j0(user_id, pid, _send)


async def attach_approval(user_id: str, approval_record_id: str | None) -> bool:
    """障害経路で後から作った App 29 レコード No. を、その相談者の最新の open（No. 未設定）に付ける。"""
    if not enabled() or not approval_record_id:
        return False
    try:
        rows = await reply_pending.open_for_user(BUSINESS, send_ledger.line_user_hash(BUSINESS, user_id))
    except Exception:
        return False
    for r in reversed(rows):
        if not r["approval_record_id"]:
            return await reply_pending.attach_approval(r["pending_id"], str(approval_record_id))
    return False


async def latest_approval_record_id(user_id: str) -> str | None:
    """その相談者の直近の App 29 レコード No.（読取のみ・無ければ None）。返信欄は App 29 No. で開くため、
    受信入口で止めた対応待ちにも既存の No. を付けて PWA から返信できるようにする。"""
    if not (_APP_APPROVAL.app_id() and _APP_APPROVAL.token()):
        return None
    uid = str(user_id or "")
    if not _LINE_USER_ID_RE.fullmatch(uid):
        return None
    try:
        rows = await kintone.search_records(_APP_APPROVAL, f'line_user_id = "{uid}" order by $id desc limit 1',
                                            fields=["$id"])
    except Exception:
        logger.info("[SEND_POLICY] approval lookup failed (no record no attached)")
        return None
    rid = str(((rows[0] if rows else {}).get("$id") or {}).get("value") or "")
    return rid or None


async def inbound_stopped(user_id: str, reasons) -> int | None:
    """受信の入口で bot を止めた（許可表 V2）: 対応待ち登録+通知（顧客へは無言・J-0 なし＝停止判定を通過して
    いない）。App 29 の直近レコード No. があれば付ける（返信欄の入口）。"""
    logger.info("[SEND_POLICY] inbound stopped by policy reasons=%s",
                emit(",".join(reasons or ()), "freetext", "log", "operator"))
    rid = await latest_approval_record_id(user_id)
    return await _register_and_notify(user_id, route=reply_pending.ROUTE_INBOUND,
                                      reason=reply_pending.REASON_POLICY_STOPPED, category=None,
                                      approval_record_id=rid)


async def approval_blocked(record_id: str, revision: str, reasons) -> str:
    """承認済み下書きが許可表 V2 で止まった: 業務 LINE へ固定文言（1 承認版 1 通）。"""
    rs = "、".join(reasons or ()) or "policy"
    text = f"{APPROVAL_BLOCKED_HEADER}\n承認キュー No.{record_id}・停止条件: {rs}\n{APPROVAL_BLOCKED_FOOTER}"
    return await send_business_notice(f"approval_blocked:{record_id}:{revision}", text)


PROMOTION_HEADER = "【凍結定型の候補】"
PROMOTION_FOOTER = "/app/exemplars と承認キューを確認し、凍結定型への昇格を検討してください（昇格は Q5・票で反映）。"


async def promotion_check(record_id: str, text: str) -> str:
    """JIKOU-REPLY-Q3a (c): 承認済み下書きの送信文を起票時の下書きと照合し、無修正承認が PROMOTION_THRESHOLD 回に
    達した category は業務 LINE へ固定文言を 1 回だけ通知（台帳キー exemplar_promotion:{category}:{回数}）。
    戻り値: none（照合対象なし／修正あり／閾値未達）・sent・throttled・failed 等（通知の結果）。"""
    from hub import reply_exemplar
    res = await reply_exemplar.record_sent(record_id, text)
    if not res or not res.get("unmodified") or not res.get("category"):
        return "none"
    if int(res.get("count") or 0) != reply_exemplar.PROMOTION_THRESHOLD:
        return "none"
    cat = str(res["category"])
    n = int(res["count"])
    logger.info("[SEND_POLICY] exemplar promotion candidate count=%s", emit(n, "count", "log", "operator"))
    return await send_business_notice(
        f"exemplar_promotion:{cat}:{n}",
        f"{PROMOTION_HEADER}\nカテゴリ「{cat}」の AI 下書きが無修正で {n} 回承認されました。\n{PROMOTION_FOOTER}")


async def approved_draft_sent(record_id: str, user_id: str, text: str | None = None,
                              category: str | None = None) -> int:
    """承認済み下書きの送信成功: (Q1c-fix1 BC-04) **送信着手時点で固定した**対応待ち集合（fixed_targets が束ねたもの）だけを
    human_replied（approved_draft）で閉じる（送信中に届いた受信は open のまま・固定集合が無ければ閉じない）。
    (Q3a) 無修正承認の計数と候補通知（flag 非依存・失敗は握る）。"""
    if text is not None:
        try:
            await promotion_check(record_id, text)
        except Exception:
            logger.warning("[SEND_POLICY] promotion check failed (fixed reason)")
    if not enabled():
        return 0
    ids = _fixed_targets.get()
    _fixed_targets.set(None)
    if not ids:
        return 0
    return await reply_pending.resolve_ids(
        ids, resolved_by=reply_pending.RESOLVED_BY_APPROVED_DRAFT, business=BUSINESS,
        line_user_hash=send_ledger.line_user_hash(BUSINESS, user_id))


async def human_replied(user_id: str, pending_ids) -> int:
    """大野の明示送信（/app/reply）の成功: **送信着手時点で固定した** pending_id 集合（pending_targets の戻り値）だけを
    human_replied で閉じる（fix1 BC-04・flag 非依存＝OFF に戻した後も残件を閉じられる）。送信中に届いた受信は open のまま。"""
    return await reply_pending.resolve_ids(
        pending_ids, resolved_by=reply_pending.RESOLVED_BY_HUMAN, business=BUSINESS,
        line_user_hash=send_ledger.line_user_hash(BUSINESS, user_id))


# ══════════════════════════════════════════════════════════════
# JIKOU-REPLY-Q1c-fix1（Codex R-Q1c BC-01〜05・司令塔裁定）: 末尾追記
#   BC-01 V2 ON の bot／承認済み下書きは、会話状態（attending）または送信台帳が確認できないときは送らない（fail-closed）。
#         hub/line_channel._begin_with_policy が begin()=None を PolicyStopped(ledger:unavailable) に倒し、
#         bot_stop_reasons は会話状態の取得失敗を attending:unknown（停止）にする。業務 LINE へ障害通知 1 日 1 回
#         （台帳 key policy_outage:{JST 日付}・台帳不可なら固定ログのみ）。Q1a の fail-open は V2 OFF の経路に限る。
#   BC-03 対応待ちの保存失敗は永続キュー（reply_pending_retry）へ・通知失敗は台帳 key で再試行（run_pending_retry_job＝
#         send_ledger の回収ジョブに相乗り）。未返信の経過通知（REPLY_PENDING_AGE_MIN 分超・1 日 2 回・件数と経過分のみ）。
#   BC-04 承認・送信着手の時点で返信対象の pending_id 集合を固定（pending_targets／fixed_targets）。
#   BC-05 J-0 の送信済み記録（customer_send=j0_sent）は台帳 finish=applied（SEND_SENT）の後にだけ。失敗・結果不明・停止は
#         別状態。App 28 への写しは確定した文だけ（ack_recorded）。
# ══════════════════════════════════════════════════════════════
REASON_LEDGER_UNAVAILABLE = "ledger:unavailable"       # 台帳（＝会話状態）が確認できない
REASON_ATTENDING_UNKNOWN = "attending:unknown"         # 受信入口で会話状態が確認できない
OUTAGE_NOTICE_HEADER = "【自動送信を停止中・障害】"
OUTAGE_NOTICE_TEXT = (f"{OUTAGE_NOTICE_HEADER}\n会話状態または送信台帳（DB）を確認できないため、bot と承認済み下書きの"
                      "自動送信を止めています（相談者へは無言）。復旧後、対応待ち（/app/pending）と App 28 の受信記録を"
                      "ご確認ください。本通知は本日これ以降省略します。")
AGE_NOTICE_HEADER = "【対応待ち・未返信】"
AGE_NOTICE_FOOTER = "/app/pending からご確認ください。"
REPLY_PENDING_AGE_MIN_DEFAULT = 60                     # 要大野確認（env REPLY_PENDING_AGE_MIN で上書き可）
REPLY_PENDING_AGE_MIN_ENV = "REPLY_PENDING_AGE_MIN"
AGE_NOTICE_HOURS_JST = (9, 15)                         # 1 日 2 回（要大野確認・HOUKI 系ジョブと同じ hub/scheduler の daily）
AGE_JOB_PREFIX = "JIKOU_REPLY_PENDING_AGE"

_j0_pid: contextvars.ContextVar = contextvars.ContextVar("send_policy_j0_pid", default=None)
_fixed_targets: contextvars.ContextVar = contextvars.ContextVar("send_policy_fixed_targets", default=None)
_outage_notified: dict = {}      # JST 日付 → True（プロセス内の省略・正は台帳 key）


def _jst_date(v=None) -> datetime.date:
    if v is None:
        return datetime.datetime.now(_JST).date()
    if v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return v.astimezone(_JST).date()


def outage_key(today: datetime.date | None = None) -> str:
    return f"policy_outage:{(today or _jst_date()).isoformat()}"


async def outage_stop(reason: str) -> None:
    """BC-01: 停止の事実を固定語彙で記録し、業務 LINE へ障害通知を 1 日 1 回だけ送る（台帳不可なら固定ログのみ）。"""
    logger.warning("[SEND_POLICY] auto send stopped (state or ledger unavailable, fail-closed) reason=%s",
                   emit(reason, "freetext", "log", "operator"))
    day = _jst_date().isoformat()
    if _outage_notified.get(day):
        return
    state = await send_business_notice(outage_key(), OUTAGE_NOTICE_TEXT)
    if state in (reply_pending.NOTIFY_SENT, reply_pending.NOTIFY_THROTTLED, reply_pending.NOTIFY_UNCONFIRMED):
        _outage_notified.clear()
        _outage_notified[day] = True


def clear_outage_notice_cache() -> None:
    _outage_notified.clear()


async def ledger_unavailable_stop() -> tuple:
    """hub/line_channel から: V2 ON の bot／承認済み下書きで begin() が None（台帳不可）＝送らない。理由を返す。"""
    await outage_stop(REASON_LEDGER_UNAVAILABLE)
    reasons = (REASON_LEDGER_UNAVAILABLE,)
    record_stop(reasons)
    return reasons


# ── BC-05: J-0 の結果 → customer_send ────────────────────────────────────────────
def j0_state_for(result) -> str:
    """送信フックの戻り値（SEND_SENT／SEND_FAILED／SEND_UNCONFIRMED／SEND_POLICY_STOPPED・None）を閉集合へ。"""
    if result == send_ledger.SEND_SENT:
        return reply_pending.CUSTOMER_J0_SENT
    if result == send_ledger.SEND_FAILED:
        return reply_pending.CUSTOMER_J0_FAILED
    if result == send_ledger.SEND_POLICY_STOPPED:
        return reply_pending.CUSTOMER_J0_STOPPED
    return reply_pending.CUSTOMER_J0_UNCONFIRMED


async def send_ack(user_id: str, ack_text: str, with_purpose, purpose_name: str, fn, *args):
    """顧客対応経路の応答送信（chat_responder の 1 行置換から）。通信例外は J-0 なら結果不明として記録して再送出。"""
    try:
        return await with_purpose(purpose_name, fn, *args)
    except Exception:
        pid = _j0_pid.get()
        if enabled() and ack_text == J0_TEXT and pid is not None:
            _j0_pid.set(None)
            await reply_pending.set_customer_send(pid, reply_pending.CUSTOMER_J0_UNCONFIRMED)
        raise


async def ack_recorded(user_id: str, ack_text: str, result) -> bool:
    """送った応答を App 28 に写すか。OFF: 従来どおり（ack_text があれば写す）。ON: 即時定型は従来どおり、
    J-0 は台帳の結果が sent（finish=applied）のときだけ写し、customer_send を結果どおりに記録する。"""
    if not ack_text:
        return False
    if not enabled() or ack_text != J0_TEXT:
        return True
    pid = _j0_pid.get()
    _j0_pid.set(None)
    state = j0_state_for(result)
    if pid is not None:
        await reply_pending.set_customer_send(pid, state)
    return state == reply_pending.CUSTOMER_J0_SENT


# ── BC-04: 返信対象の固定 ───────────────────────────────────────────────────────
async def pending_targets(user_id: str) -> tuple:
    """送信着手の直前に呼ぶ: その相談者の open の pending_id（読取失敗は ()＝何も閉じない）。"""
    try:
        return tuple(await reply_pending.open_ids_for_user(BUSINESS, send_ledger.line_user_hash(BUSINESS, user_id)))
    except Exception:
        return ()


@contextlib.asynccontextmanager
async def fixed_targets(user_id: str):
    """承認 webhook の送信（main の 1 行置換: `async with send_ledger.approved_draft(...), send_policy.fixed_targets(user_id):`）。
    入口で対応待ち集合を固定し、approved_draft_sent がその集合だけを閉じる（V2 OFF は空）。"""
    _fixed_targets.set(await pending_targets(user_id) if enabled() else ())
    yield


# ── BC-03: 回収ジョブ（永続キューの再処理・通知の再試行）と経過通知 ──────────────────────
def age_minutes() -> int:
    raw = os.environ.get(REPLY_PENDING_AGE_MIN_ENV, "").strip()
    return int(raw) if raw.isdigit() and int(raw) > 0 else REPLY_PENDING_AGE_MIN_DEFAULT


def age_job_name(hour: int) -> str:
    return f"{AGE_JOB_PREFIX}_{int(hour):02d}"


async def _conversation_ref(conversation_id, user_hash: str) -> str:
    try:
        if conversation_id is not None:
            async with db_session_scope() as s:
                ref = (await s.execute(sa.select(send_ledger.conversation.c.ref).where(
                    send_ledger.conversation.c.conversation_id == int(conversation_id)))).scalar()
            if ref:
                return str(ref)
        st = await send_ledger.conversation_state_by_hash(BUSINESS, user_hash)
        return str((st or {}).get("ref") or "")
    except Exception:
        return ""


async def _notify_row(row: dict, created_at) -> str:
    key = notice_key(await _conversation_ref(row.get("conversation_id"), row["line_user_hash"]),
                     row["line_user_hash"], today=_jst_date(created_at))
    return await send_business_notice(key, notice_text(row.get("approval_record_id") or None,
                                                       row.get("category") or None))


async def run_pending_retry_job() -> dict:
    """send_ledger の回収ジョブに相乗り（interval）。(1) 永続キューの再処理（register → 通知）、(2) 通知の再試行
    （台帳 key は登録日のまま＝送れても 1 通・上限 NOTIFY_RETRY_MAX）。例外は固定語彙で握る（他の hook を止めない）。"""
    res = {"requeued": 0, "registered": 0, "register_failed": 0, "notify_retried": 0}
    if not reply_pending._db_configured():
        return res
    try:
        queue = await reply_pending.list_retry_queue()
    except Exception:
        logger.warning("[SEND_POLICY] retry queue read failed (fixed reason)")
        queue = []
    for q in queue:
        res["requeued"] += 1
        pid = await reply_pending.register(
            business=q["business"], channel=q["channel"], line_user_hash=q["line_user_hash"], route=q["route"],
            reason=q["reason"], inbound_event_id=q["inbound_event_id"], conversation_id=q["conversation_id"],
            category=(q["category"] or None), approval_record_id=(q["approval_record_id"] or None))
        if pid is None:
            res["register_failed"] += 1
            await reply_pending.retry_bump(q["retry_id"])
            continue
        res["registered"] += 1
        await reply_pending.retry_done(q["retry_id"], pid)
        state = await _notify_row({"conversation_id": q["conversation_id"], "line_user_hash": q["line_user_hash"],
                                   "approval_record_id": q["approval_record_id"], "category": q["category"]},
                                  q["created_at"])
        await reply_pending.set_notify_state(pid, state)
    try:
        rows = await reply_pending.list_notify_retry()
    except Exception:
        logger.warning("[SEND_POLICY] notify retry read failed (fixed reason)")
        rows = []
    for r in rows:
        res["notify_retried"] += 1
        created = datetime.datetime.fromisoformat(r["created_at"]) if r.get("created_at") else None
        state = await _notify_row(r, created)
        await reply_pending.bump_attempts(r["pending_id"])
        await reply_pending.set_notify_state(r["pending_id"], state)
    if res["registered"] or res["notify_retried"]:
        logger.info("[SEND_POLICY] pending retry job registered=%s notify_retried=%s",
                    emit(res["registered"], "count", "log", "operator"),
                    emit(res["notify_retried"], "count", "log", "operator"))
    return res


def age_notice_text(stats: dict, queued: int, threshold: int) -> str:
    return (f"{AGE_NOTICE_HEADER}\n未返信の対応待ち {stats['open_count']} 件（{threshold} 分超 {stats['over_count']} 件・"
            f"最長 {stats['oldest_minutes']} 分経過）・再処理待ち {queued} 件\n{AGE_NOTICE_FOOTER}")


async def run_age_notice_job(hour: int, *, now=None) -> dict:
    """BC-03 (2): REPLY_PENDING_AGE_MIN 分を超えて open のままの対応待ちを 1 日 2 回まとめて通知（件数と経過分のみ・PII なし）。
    台帳 key `pending_age:{JST 日付}:{時}` で 1 枠 1 通。対象 0 件（再処理待ちも 0）なら送らない。"""
    res = {"over_count": 0, "queued": 0, "notice": ""}
    if not reply_pending._db_configured():
        return res
    try:
        stats = await reply_pending.age_stats(threshold_minutes=age_minutes(), now=now)
        queued = await reply_pending.count_retry_queue()
    except Exception:
        logger.warning("[SEND_POLICY] age notice read failed (fixed reason)")
        return res
    res.update(over_count=stats["over_count"], queued=queued)
    if stats["over_count"] == 0 and queued == 0:
        return res
    day = _jst_date(now).isoformat()
    res["notice"] = await send_business_notice(f"pending_age:{day}:{int(hour):02d}",
                                               age_notice_text(stats, queued, age_minutes()))
    return res


def register_jobs() -> None:
    """main の末尾から（冪等）: 回収ジョブへの相乗り＋経過通知の daily ジョブ（start_all は既存の startup が呼ぶ）。"""
    from hub import scheduler as hub_scheduler
    send_ledger.register_recover_hook(run_pending_retry_job)
    for hour in AGE_NOTICE_HOURS_JST:
        name = age_job_name(hour)
        if not hub_scheduler.is_registered(name):
            hub_scheduler.register_daily(name, hour, functools.partial(run_age_notice_job, hour))
