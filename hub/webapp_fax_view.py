"""webapp_fax_view — JIKOU-FAX-2: 時効援用通知 PDF の FAX 送信を弁護士が明示承認する画面（/app/fax/{App 21 No.}）

正本: docs/design/jikou-fax-1.md §4-2（PWA 承認・業者名と番号の末尾 4 桁のみ表示・明示確認・署名付き ticket）。
QUEUE-2026-10-13-E E-3。認証 _gate。送信は本画面の POST ハンドラからのみ（hub/jikou_fax.enqueue は署名付き
Approval を要求）。provider 既定 DryRun（実送信なし）。
- 一覧: 債権者スロット 1〜3 ごとに 業者名・宛先の下 4 桁（または送れない理由）・PDF の有無・ticket 付きフォーム。
- 送信（POST /app/fax/{record_id}/send）: slot・op_id・ticket・confirm=1 → PDF 取得→sha→宛先再解決→ticket と一致→
  approve()→enqueue()→kintone 刻印（写し）→PRG。不正入力は固定 400。番号・氏名は画面・URL・ログに出さない。
"""

import hmac
import html
import logging
import re
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import fax_adapter, jikou_fax, kintone
from hub.redact import emit
from hub.webapp_auth import _gate, _sign

logger = logging.getLogger("hub.webapp_fax_view")

router = APIRouter()

PAGE = "/app/fax"
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_OP_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_TICKET_RE = re.compile(r"^[0-9]{1,12}\.[123]\.[0-9]{1,12}\.[0-9a-f]{32}\.[0-9a-f]{64}$")
CONFIRM_LABEL = "宛先（業者名・末尾 4 桁）と通知書 PDF を確認し、FAX 送信を承認します"
_REASON_LABELS = {
    jikou_fax.REASON_MASTER_UNSET: "業者マスタが未設定です（env）",
    jikou_fax.REASON_NOT_FOUND: "業者マスタに該当がありません",
    jikou_fax.REASON_AMBIGUOUS: "業者マスタに同名が複数あります",
    jikou_fax.REASON_FAX_NOT_ALLOWED: "FAX送信可 が「可」ではありません",
    jikou_fax.REASON_INVALID_NUMBER: "FAX 番号の形式が不正です",
    jikou_fax.REASON_PDF_NOT_FOUND: "通知書 PDF が添付されていません",
    jikou_fax.REASON_PDF_INVALID: "添付が PDF ではありません",
    jikou_fax.REASON_KINTONE: "kintone から取得できませんでした",
}
_DONE = {
    "sent": "FAX 送信を受け付けました（台帳に記録・provider の受付 ID を刻印）。",
    "skipped": "同じ宛先・同じ PDF は送信済みのため送っていません（二重送信防止）。",
    "unconfirmed": "送信結果を確認できませんでした。送信確認待ち（/app/send_ops）で確定してください（自動再送なし）。",
    "failed": "送信に失敗しました。送られていません。",
    "ledger_unavailable": "送信台帳に記録できないため送っていません。",
    "confirm_required": "確認チェックが必要です。送っていません。",
    "bad_input": "入力が不正です。送っていません。",
    "stale": "宛先または PDF が画面表示後に変わりました。開き直してください。送っていません。",
    "not_sendable": "宛先または PDF が送れる状態ではありません。送っていません。",
    "provider": "FAX provider が未設定または未実装です。送っていません。",
    "kintone": "案件を取得できませんでした。",
}


def _bad_request() -> Response:
    return Response(status_code=400)


def make_ticket(record_id: str, slot: int, master_record_id: str, op_id: str) -> str | None:
    payload = f"{record_id}.{int(slot)}.{master_record_id}.{op_id}"
    sig = _sign(payload)
    return f"{payload}.{sig}" if sig else None


def parse_ticket(ticket: str, record_id: str, slot: int, op_id: str) -> str | None:
    """署名と束縛（record_id・slot・op_id）を検証しマスタ record_id を返す。不正は None。"""
    if not _TICKET_RE.fullmatch(ticket or ""):
        return None
    rid, s, mid, oid, sig = ticket.split(".")
    expected = _sign(f"{rid}.{s}.{mid}.{oid}")
    if expected is None or not hmac.compare_digest(sig, expected):
        return None
    if rid != record_id or int(s) != int(slot) or oid != op_id:
        return None
    return mid


async def _slot_rows(record: dict) -> list[dict]:
    rows = []
    for slot in jikou_fax.SLOTS:
        name = jikou_fax.creditor_name(record, slot)
        if not name:
            continue
        row = {"slot": slot, "name": name, "last4": "", "reason": "", "master_id": "",
               "pdf": jikou_fax.pdf_file_for_slot(record, slot) is not None}
        try:
            dest = await jikou_fax.resolve_destination(name)
            row["last4"], row["master_id"] = dest.last4, dest.master_record_id
        except jikou_fax.NotSendable as e:
            row["reason"] = e.reason
        rows.append(row)
    return rows


def _page(record_id: str, rows: list[dict], op_id: str, notice: str, provider: str) -> str:
    e = html.escape
    parts = [
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>FAX 送信 - 案件管理（仮）</title>",
        "<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
        "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
        ".item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
        ".bad{color:#8a1f1f}.notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}"
        "button{margin-top:.5rem}</style></head><body>",
        f"<h1>FAX 送信（案件 No.{e(record_id)}）</h1>",
        f'<p class="sub">宛先は業者マスタの番号（末尾 4 桁のみ表示）。provider: {e(provider)}（dryrun は実送信なし）。'
        '<a href="/app/send_ops">送信確認待ち</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    if not rows:
        parts.append('<p class="sub">債権者が入力されていません</p>')
    for r in rows:
        parts.append(f'<div class="item"><div>債権者 {int(r["slot"])}: {e(r["name"])}</div>')
        if r["reason"]:
            parts.append(f'<div class="bad">送れません: {e(_REASON_LABELS.get(r["reason"], r["reason"]))}</div>')
        else:
            parts.append(f'<div>宛先 FAX: ****{e(r["last4"])}</div>')
        parts.append('<div>通知書 PDF: ' + ("あり" if r["pdf"] else '<span class="bad">なし</span>') + "</div>")
        ticket = make_ticket(record_id, r["slot"], r["master_id"], op_id) if (r["master_id"] and r["pdf"]) else None
        if ticket:
            parts.append(f'<form method="post" action="{PAGE}/{e(record_id)}/send">'
                         f'<input type="hidden" name="slot" value="{int(r["slot"])}">'
                         f'<input type="hidden" name="op_id" value="{e(op_id)}">'
                         f'<input type="hidden" name="ticket" value="{e(ticket)}">'
                         f'<label><input type="checkbox" name="confirm" value="1"> {e(CONFIRM_LABEL)}</label>'
                         '<button type="submit">FAX を送信する</button></form>')
        parts.append("</div>")
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def fax_page(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    code = request.query_params.get("done", "")
    notice = _DONE.get(code, "") if code else ""
    try:
        record = await kintone.get_record(jikou_fax.APP_JIKOU, rid)
    except Exception:
        return HTMLResponse(_page(rid, [], uuid.uuid4().hex, _DONE["kintone"], fax_adapter.provider_name()))
    rows = await _slot_rows(record)
    return HTMLResponse(_page(rid, rows, uuid.uuid4().hex, notice, fax_adapter.provider_name()))


def _redirect(rid: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{rid}?done={done}", status_code=303)


async def fax_send(request: Request):
    """明示承認 → 投入（PRG）。入力は閉集合・固定書式のみ。"""
    rid = str(request.path_params.get("record_id", ""))
    form = await request.form()
    slot_s = str(form.get("slot", ""))
    op_id = str(form.get("op_id", ""))
    ticket = str(form.get("ticket", ""))
    confirm = str(form.get("confirm", "")) == "1"
    if not _ID_RE.fullmatch(rid) or slot_s not in ("1", "2", "3") or not _OP_ID_RE.fullmatch(op_id):
        return _bad_request()
    slot = int(slot_s)
    if not confirm:
        return _redirect(rid, "confirm_required")
    bound_master = parse_ticket(ticket, rid, slot, op_id)
    if bound_master is None:
        return _bad_request()
    try:
        record = await kintone.get_record(jikou_fax.APP_JIKOU, rid)
    except Exception:
        return _redirect(rid, "kintone")
    try:
        dest = await jikou_fax.resolve_destination(jikou_fax.creditor_name(record, slot))
        pdf = await jikou_fax.fetch_pdf(record, slot)
    except jikou_fax.NotSendable:
        return _redirect(rid, "not_sendable")
    if dest.master_record_id != bound_master:
        logger.info("[FAX_VIEW] destination changed since page render (not sent)")
        return _redirect(rid, "stale")
    approval = jikou_fax.approve(rid, slot, fax_adapter.pdf_sha256(pdf), dest.master_record_id, op_id)
    if approval is None:
        return _bad_request()
    try:
        res = await jikou_fax.enqueue(approval, pdf, dest)
    except fax_adapter.UnknownProvider:
        return _redirect(rid, "provider")
    except jikou_fax.NotApproved:
        return _bad_request()
    outcome = res["outcome"]
    logger.info("[FAX_VIEW] outcome=%s record_id=%s", emit(outcome, "freetext", "log", "operator"),
                emit(rid, "record_id", "log", "operator"))
    if outcome in (jikou_fax.OUTCOME_SENT, jikou_fax.OUTCOME_UNCONFIRMED, jikou_fax.OUTCOME_FAILED):
        await jikou_fax.stamp_kintone(rid, slot, outcome, res.get("accept_id", ""))
    return _redirect(rid, outcome if outcome in _DONE else "failed")


router.add_api_route(f"{PAGE}/{{record_id}}", _gate(fax_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{record_id}}/send", _gate(fax_send), methods=["POST"])
