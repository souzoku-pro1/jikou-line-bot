"""webapp_drafts_view — JIKOU-REPLY-Q4: PWA で AI 下書き（Q3b・reply_draft）を承認・修正・却下する画面

正本: 時効LINEボット_返信規則_v1.4.md R-J8（大野の返信は PWA から）・R-J9（下書き→承認→閾値）・§10-1（下書きは
「このまま送る」「直して送る」「送らない」・承認後の編集は再承認）・§10-7（送った文は由来 category と差分の大小を保存）・
§10-9（集計単位=分類 ID・差分なしで承認の回数）。QUEUE-2026-10-13-D D-3。

- 一覧（/app/drafts）: state=proposed の下書き（ID・カテゴリ・ガード結果・受付時刻・対応待ち ID）。本文は詳細のみ。
- 詳細（/app/drafts/{draft_id}）: 相談者の直近のやり取り（App 28・対応待ち→App 29 No.→line_user_id をサーバ内で解決。
  URL・画面に LINE userId は出さない）・下書き・修正欄・「承認して送信」「却下」。宛先を解決できない下書きは却下のみ。
- 送信は Q1b の返信欄と同じ経路: 署名付き ticket（draft_id・宛先ハッシュ・操作 ID の束縛）・明示確認チェック必須・
  送信操作記録（主体=approved_draft・用途=reply・操作 ID 固定）・hub/line_channel.push_text（V2 ON は排他内で許可表の
  承認済み下書き列）。状態遷移 proposed→approved は CAS（二重クリック・戻る→再送は副作用なし）。
- 記録: 無修正承認（asis）／修正承認（edited）／却下（rejected）を reply_draft_approval（Q3a の表・record_id=draft:{id}）に
  記録し（draft_text_version・sent_text_version・unmodified・outcome）、送信成功時は hub/send_policy.approved_draft_sent
  （対応待ちを閉じる・無修正承認の計数と候補通知）。修正承認は修正後本文を手本候補として reply_exemplar に匿名化保存
  （active=False＝未承認・/app/exemplars で承認）。残留検査に落ちた文は保存しない。
- RV-10: 画面・ログ・通知に LINE userId を出さない。ログは draft_id（record_id 扱い）と固定語彙のみ。
"""

import hashlib
import hmac
import html
import logging
import re
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import line_channel, reply_draft, reply_exemplar, reply_pending, send_ledger, send_policy
from hub.redact import emit
from hub.webapp_auth import _gate, _sign
from hub.webapp_reply_view import _approval_record, _history_rows, _v

logger = logging.getLogger("hub.webapp_drafts_view")

router = APIRouter()

PAGE = "/app/drafts"
BUSINESS = "jikou"
MAX_TEXT_CHARS = reply_draft.MAX_DRAFT_CHARS
RECORD_PREFIX = "draft:"
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_OP_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
_TICKET_RE = re.compile(r"^[0-9]{1,12}\.[0-9a-f]{32}\.[0-9a-f]{32}\.[0-9a-f]{64}$")
_ROLE_LABELS = {"user": "相談者", "assistant": "bot", "human": "事務所（大野）"}
CONFIRM_LABEL = "下書きの内容を確認し、相談者へ送信することを承認します（人対応中・対応中の会話は許可表で止まります）"
OUTCOME_ASIS = "approved_asis"
OUTCOME_EDITED = "approved_edited"
OUTCOME_REJECTED = "rejected"
OUTCOMES = (OUTCOME_ASIS, OUTCOME_EDITED, OUTCOME_REJECTED)

_DONE = {
    "sent": "送信しました。",
    "sent_edited": "修正した本文を送信しました（修正後の文は手本候補として匿名化保存・未承認）。",
    "rejected": "却下しました（相談者へは送っていません）。",
    "already": "この下書きは既に処理済みです（二重送信はしていません）。",
    "confirm_required": "確認チェックが必要です。送信していません。",
    "bad_input": "入力が不正です。送信していません。",
    "not_found": "宛先の相談者を特定できませんでした。送信していません（却下は可能です）。",
    "stale": "相談者情報が更新されました。画面を開き直してください。送信していません。",
    "blocked": "許可表により送信できません（人対応中・対応中・停止設定）。返信欄（/app/reply）からご返信ください。",
    "unconfirmed": "送信結果を確認できませんでした。送信確認待ちで結果を確定してください（自動再送はしません）。",
    "failed": "LINE への送信に失敗しました。送信されていません。",
    "ledger_unavailable": "台帳が使えないため送信できません。時間をおいて再試行してください（下書きは承認前に戻しました）。",
    "db": "記録を読み書きできませんでした。",
}


def _bad_request() -> Response:
    return Response(status_code=400)


def _user_hash(user_id: str) -> str:
    return send_ledger.line_user_hash(BUSINESS, user_id)[:32]


def make_ticket(draft_id: str, user_id: str, op_id: str) -> str | None:
    payload = f"{draft_id}.{_user_hash(user_id)}.{op_id}"
    sig = _sign(payload)
    return f"{payload}.{sig}" if sig else None


def parse_ticket(ticket: str, draft_id: str, op_id: str) -> str | None:
    """署名と束縛（draft_id・op_id）を検証し宛先ハッシュを返す。不正は None。"""
    if not _TICKET_RE.fullmatch(ticket or ""):
        return None
    did, uhash, oid, sig = ticket.split(".")
    expected = _sign(f"{did}.{uhash}.{oid}")
    if expected is None or not hmac.compare_digest(sig, expected):
        return None
    if did != draft_id or oid != op_id:
        return None
    return uhash


async def resolve_destination(draft: dict) -> tuple[str, dict | None]:
    """下書き → 対応待ち → App 29 No. → line_user_id（サーバ内のみ）。解決できなければ ("", rec|None)。"""
    pid = draft.get("pending_id")
    if pid is None:
        return "", None
    try:
        pend = await reply_pending.get_pending(int(pid))
    except Exception:
        return "", None
    rid = str((pend or {}).get("approval_record_id") or "")
    if not _ID_RE.fullmatch(rid):
        return "", None
    rec = await _approval_record(rid)
    user_id = _v(rec, "line_user_id") if rec else ""
    if not _LINE_USER_ID_RE.fullmatch(user_id):
        return "", rec
    return user_id, rec


# ── 画面 ────────────────────────────────────────────────────────────────────────
_STYLE = (
    "<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
    "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
    ".item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
    ".lbl{color:#667;font-size:.78rem;margin-top:.4rem}.meta{color:#8a8f98;font-size:.78rem;margin-top:.3rem}"
    "pre{white-space:pre-wrap;word-break:break-word;font-family:inherit;font-size:.93rem;border-radius:12px;padding:.55rem .8rem;margin:.4rem 0}"
    "pre.user{background:#f2f4f7;border:1px solid #e2e6ec}pre.assistant{background:#eef7e8;border:1px solid #cfe6c2}"
    "pre.human{background:#e8f0fb;border:1px solid #c5d8f2}pre.draft{background:#fff8e6;border:1px solid #f0dca8}"
    ".badge{display:inline-block;border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#fff3d6;color:#8a6d00;margin-right:.35rem}"
    ".ng{background:#fde2e2;color:#8a1f1f}.ok{background:#e2f5e6;color:#1f6b2e}"
    "textarea{width:100%;min-height:9rem;font-size:1rem;border-radius:10px;border:1px solid #c8cdd5;padding:.5rem;box-sizing:border-box}"
    "label{display:block;margin:.5rem 0;font-size:.92rem}button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;"
    "border:1px solid #c8cdd5;background:#fff;min-height:44px;margin-right:.5rem}.notice{background:#e8f0fb;border-radius:10px;"
    "padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}.empty{color:#667;text-align:center;margin:1.2rem 0}.link{margin-top:.35rem}</style>"
)


def _head(title: str) -> list[str]:
    return ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>{html.escape(title)} - 案件管理（仮）</title>", _STYLE, "</head><body>"]


def _badges(d: dict) -> str:
    g = '<span class="badge ok">ガード合格</span>' if d.get("guard_ok") else '<span class="badge ng">要修正</span>'
    cat = f'<span class="badge">{html.escape(str(d.get("category") or "分類なし"))}</span>'
    return cat + g


def _list_page(rows: list[dict], notice: str) -> str:
    e = html.escape
    parts = _head("AI 下書きの承認") + [
        "<h1>AI 下書きの承認</h1>",
        '<p class="sub">自動返信できなかった問いに AI が用意した下書きです。内容を確認し、そのまま／直して送るか、却下してください。'
        '<a href="/app/pending">対応待ち</a>｜<a href="/app/exemplars">手本</a>｜<a href="/app/send_ops">送信確認待ち</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    if not rows:
        parts.append('<p class="empty">承認待ちの下書きはありません</p>')
    for d in rows:
        did = int(d["draft_id"])
        parts.append('<div class="item">' + _badges(d))
        parts.append(f'<div class="link"><a href="{PAGE}/{did}">下書き ID {did} を開く</a></div>')
        parts.append(f'<div class="meta">受付: {e(str(d.get("created_at", "")))}｜対応待ち ID: '
                     f'{e(str(d.get("pending_id") or "なし"))}</div></div>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


def _detail_page(d: dict, rec: dict | None, rows: list[dict], op_id: str, ticket: str | None, notice: str) -> str:
    e = html.escape
    did = int(d["draft_id"])
    parts = _head("AI 下書きの確認") + [
        "<h1>AI 下書きの確認</h1>",
        f'<p class="sub"><a href="{PAGE}">一覧へ戻る</a>｜<a href="/app/pending">対応待ち</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    parts.append('<div class="item">' + _badges(d) +
                 f'<div class="meta">下書き ID {did}｜受付: {e(str(d.get("created_at", "")))}｜状態: {e(str(d.get("state")))}</div>')
    if d.get("guard_reasons"):
        parts.append(f'<div class="lbl">ガードの指摘</div><pre class="user">{e(str(d["guard_reasons"]))}</pre>')
    parts.append("</div>")
    if rec is not None:
        parts.append('<div class="item"><div class="lbl">お客様からのメッセージ（承認キュー）</div>'
                     f'<pre class="user">{e(_v(rec, "顧客メッセージ"))}</pre></div>')
    parts.append('<div class="item"><div class="lbl">直近のやり取り（チャットログ・古い順）</div>')
    if not rows:
        parts.append('<p class="empty">チャットログはありません</p>')
    for r in rows:
        role = str(r.get("role", ""))
        cls = role if role in _ROLE_LABELS else "user"
        parts.append(f'<div class="meta">{e(_ROLE_LABELS.get(role, role))}｜{e(str(r.get("at", "")))}</div>')
        parts.append(f'<pre class="{cls}">{e(str(r.get("message", "")))}</pre>')
    parts.append("</div>")
    if d.get("state") != reply_draft.STATE_PROPOSED:
        parts.append('<div class="item"><div class="lbl">下書き</div>'
                     f'<pre class="draft">{e(str(d.get("final_text") or d.get("draft_text") or ""))}</pre>'
                     '<p class="empty">この下書きは処理済みです</p></div>')
    else:
        parts.append(f'<form method="post" action="{PAGE}/{did}/act" class="item">'
                     f'<input type="hidden" name="op_id" value="{e(op_id)}">'
                     f'<input type="hidden" name="ticket" value="{e(ticket or "")}">'
                     '<div class="lbl">返信文（AI 下書き。直して送れます）</div>'
                     f'<textarea name="text" maxlength="{MAX_TEXT_CHARS}">{e(str(d.get("draft_text") or ""))}</textarea>')
        if ticket:
            parts.append(f'<label><input type="checkbox" name="confirm" value="1"> {e(CONFIRM_LABEL)}</label>'
                         '<button type="submit" name="action" value="send">承認して送信</button>')
        else:
            parts.append('<p class="empty">宛先の相談者を特定できないため送信はできません（却下のみ）</p>')
        parts.append('<button type="submit" name="action" value="reject">却下</button></form>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


# ── route ────────────────────────────────────────────────────────────────────────
async def drafts_page(request: Request):
    code = request.query_params.get("done", "")
    notice = _DONE.get(code, "") if code else ""
    try:
        rows = await reply_draft.list_drafts(BUSINESS)
    except Exception:
        rows, notice = [], _DONE["db"]
    return HTMLResponse(_list_page(rows, notice))


async def draft_detail(request: Request):
    did = str(request.path_params.get("draft_id", ""))
    if not _ID_RE.fullmatch(did):
        return _bad_request()
    code = request.query_params.get("done", "")
    notice = _DONE.get(code, "") if code else ""
    try:
        d = await reply_draft.get_draft(int(did))
    except Exception:
        d = None
    if d is None or d.get("business") != BUSINESS:
        return Response(status_code=404)
    user_id, rec = await resolve_destination(d)
    rows = await _history_rows(user_id) if user_id else []
    op_id = uuid.uuid4().hex
    ticket = make_ticket(did, user_id, op_id) if user_id else None
    return HTMLResponse(_detail_page(d, rec, rows, op_id, ticket, notice))


def _redirect(did: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{did}?done={done}", status_code=303)


async def _record_outcome(did: int, category: str | None, draft_text: str, outcome: str,
                          sent_text: str | None = None) -> None:
    """reply_draft_approval（Q3a の表）に種別を記録（record_id=draft:{id}）。失敗は固定語彙のログだけ。"""
    rid = f"{RECORD_PREFIX}{did}"
    try:
        await reply_exemplar.record_draft(rid, BUSINESS, category, draft_text)
        await reply_exemplar.set_outcome(rid, outcome)
    except Exception:
        logger.warning("[DRAFTS] outcome record failed (fixed reason) draft_id=%s", emit(did, "record_id", "log", "operator"))


async def _save_candidate(user_id: str, rec: dict | None, text: str, op_id: str, category: str | None, did: int) -> str:
    """修正承認: 修正後本文を手本候補（未承認・active=False）として匿名化保存。"""
    inbound = _v(rec, "顧客メッセージ") if rec else ""
    name = _v(rec, "顧客名") if rec else ""
    try:
        state = await send_ledger.conversation_state(BUSINESS, user_id)
    except Exception:
        state = None
    try:
        return await reply_exemplar.save_exemplar(
            business=BUSINESS, inbound_text=inbound, human_reply=text, customer_name=name,
            stage=reply_exemplar._stage_for_status(_v(rec, "status") if rec else ""),
            conversation_ref=(state or {}).get("ref"), source_op_id=op_id,
            source_text_version=send_ledger.text_version(text), category_id=category,
            approved_by=reply_exemplar.APPROVED_BY_HUMAN, active=False)
    except Exception:
        logger.warning("[DRAFTS] candidate save failed (fixed reason) draft_id=%s", emit(did, "record_id", "log", "operator"))
        return "failed"


async def draft_act(request: Request):
    """承認して送信／却下（PRG）。入力は閉集合・固定書式のみ（不正は固定 400）。"""
    did = str(request.path_params.get("draft_id", ""))
    form = await request.form()
    action = str(form.get("action", ""))
    op_id = str(form.get("op_id", ""))
    ticket = str(form.get("ticket", ""))
    text = str(form.get("text", "")).strip()
    confirm = str(form.get("confirm", "")) == "1"
    if not _ID_RE.fullmatch(did) or action not in ("send", "reject") or not _OP_ID_RE.fullmatch(op_id):
        return _bad_request()
    try:
        d = await reply_draft.get_draft(int(did))
    except Exception:
        return _redirect(did, "db")
    if d is None or d.get("business") != BUSINESS:
        return Response(status_code=404)
    if d.get("state") != reply_draft.STATE_PROPOSED:
        return _redirect(did, "already")
    category = d.get("category")
    if action == "reject":
        ok = await reply_draft.set_state(int(did), reply_draft.STATE_REJECTED, actor=reply_draft.ACTOR_HUMAN)
        if not ok:
            return _redirect(did, "already")
        await _record_outcome(int(did), category, str(d["draft_text"]), OUTCOME_REJECTED)
        logger.info("[DRAFTS] rejected draft_id=%s", emit(did, "record_id", "log", "operator"))
        return _redirect(did, "rejected")
    # send
    if not text or len(text) > MAX_TEXT_CHARS:
        return _redirect(did, "bad_input")
    if not confirm:
        return _redirect(did, "confirm_required")
    bound = parse_ticket(ticket, did, op_id)
    if bound is None:
        return _bad_request()
    user_id, rec = await resolve_destination(d)
    if not user_id:
        return _redirect(did, "not_found")
    if _user_hash(user_id) != bound:
        logger.info("[DRAFTS] destination changed since page render (not sent)")
        return _redirect(did, "stale")
    edited = (text != str(d["draft_text"]).strip())
    kind = reply_draft.KIND_EDITED if edited else reply_draft.KIND_ASIS
    ok = await reply_draft.set_state(int(did), reply_draft.STATE_APPROVED, actor=reply_draft.ACTOR_HUMAN,
                                     approval_kind=kind, final_text=text, op_id=f"{RECORD_PREFIX}{did}:{op_id}")
    if not ok:
        return _redirect(did, "already")                   # CAS: 二重クリック・戻る→再送は副作用なし
    # PRE-REVIEW-1 #279-3: outcome（approved_asis／edited）は送信成功の後に記録する（許可表停止・LINE 失敗を承認に数えない）
    # PRE-REVIEW-1 #279-1: 台帳必須（require_ledger）。begin が None なら送らず、下書きを承認前（proposed）へ戻す
    async with send_ledger.approved_draft(f"{RECORD_PREFIX}{did}:{op_id}"), send_policy.fixed_targets(user_id):  # Q1c-fix1 BC-04: 着手時点の対応待ち集合を固定
        res = await line_channel.push_text(line_channel.JIKOU_CHANNEL, user_id, text, require_ledger=True)
    if res == send_ledger.SEND_LEDGER_UNAVAILABLE:
        await reply_draft.set_state(int(did), reply_draft.STATE_PROPOSED, actor=reply_draft.ACTOR_BOT,
                                    from_states=(reply_draft.STATE_APPROVED,), note="ledger_unavailable")
        logger.warning("[DRAFTS] ledger unavailable (not sent) draft_id=%s", emit(did, "record_id", "log", "operator"))
        return _redirect(did, "ledger_unavailable")
    if res == send_ledger.SEND_POLICY_STOPPED:
        await reply_draft.set_state(int(did), reply_draft.STATE_FAILED, actor=reply_draft.ACTOR_BOT,
                                    from_states=(reply_draft.STATE_APPROVED,), note="policy_stopped")
        return _redirect(did, "blocked")
    if res == send_ledger.SEND_UNCONFIRMED:
        return _redirect(did, "unconfirmed")
    if res is not True:
        await reply_draft.set_state(int(did), reply_draft.STATE_FAILED, actor=reply_draft.ACTOR_BOT,
                                    from_states=(reply_draft.STATE_APPROVED,), note="line_failed")
        return _redirect(did, "failed")
    await reply_draft.set_state(int(did), reply_draft.STATE_SENT, actor=reply_draft.ACTOR_BOT,
                                from_states=(reply_draft.STATE_APPROVED,))
    await _record_outcome(int(did), category, str(d["draft_text"]), OUTCOME_EDITED if edited else OUTCOME_ASIS)
    logger.info("[DRAFTS] approved draft sent draft_id=%s edited=%s", emit(did, "record_id", "log", "operator"),
                emit(int(edited), "count", "log", "operator"))
    import chat_responder as cr
    try:
        await cr.save_to_chatlog(user_id, "assistant", text, category or "その他判断系", "yes")
    except Exception:
        logger.warning("[DRAFTS] chatlog write failed (fixed reason)")
    try:
        await send_policy.approved_draft_sent(f"{RECORD_PREFIX}{did}", user_id, text, category)
    except Exception:
        logger.warning("[DRAFTS] post-send hooks failed (fixed reason)")
    if edited:
        await _save_candidate(user_id, rec, text, f"{RECORD_PREFIX}{did}:{op_id}", category, int(did))
    return _redirect(did, "sent_edited" if edited else "sent")


async def api_drafts(request: Request):
    try:
        rows = await reply_draft.list_drafts(BUSINESS)
    except Exception:
        return {"ok": False, "reason": "db", "count": 0, "items": []}
    items = [{k: v for k, v in r.items() if k not in ("line_user_hash", "draft_text", "final_text")} for r in rows]
    return {"ok": True, "count": len(items), "items": items}


router.add_api_route(PAGE, _gate(drafts_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{draft_id}}", _gate(draft_detail), methods=["GET"])
router.add_api_route(f"{PAGE}/{{draft_id}}/act", _gate(draft_act), methods=["POST"])
router.add_api_route("/app/api/drafts", _gate(api_drafts), methods=["GET"])
