"""webapp_send_ops_view — JIKOU-REPLY-Q1a: 送信確認待ち（unconfirmed）の表示と人の確定

正本: 時効LINEボット_返信規則_v1.4.md §10-1（結果不明は「送信確認待ち」として残す・
自動再送なし）・§10-6（送信確認待ちの再試行は結果確認と人への確認依頼のみ）・§12 Q1a
（送信確認待ちの表示と結果確認経路）。

- 認証は P4-001 の関所（hub.webapp_auth の `_gate`）に乗る——公開例外なし。
- 一覧は本文・氏名・LINE userId を出さない（操作 ID・業務・チャネル・用途・主体・着手時刻・
  試行回数・会話の不透明参照 ID のみ＝RV-10）。
- 人の操作は「送信済みとして確定」「失敗として確定」の 2 つ。理由は閉集合
  （send_ledger.HUMAN_REASONS）。確定は send_operation_history に理由つきで残る（RV-08）。
  自動再送はしない（再送の口を持たない）。
- 承認画面（/app/approvals）は既存テストが参照専用 UI を閉集合で pin しているため、
  一覧と操作は本画面（/app/send_ops・サーバ描画・native form POST・PRG 303）に置き、
  承認画面からはリンクで辿る。
- 登録は add_api_route 経由（webapp_q と同じ流儀）。kintone・外部送信は呼ばない。
"""

import html
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import send_ledger
from hub.webapp_auth import _gate

router = APIRouter()

PAGE = "/app/send_ops"
LIST_LIMIT = 50
_OP_ID_RE = re.compile(r"^[A-Za-z0-9:_.-]{1,128}$")
_OUTCOMES = {"sent": send_ledger.STATE_SENT, "failed": send_ledger.STATE_FAILED}
_REASON_LABELS = {
    "line_delivered": "LINE で届いていることを確認した",
    "line_not_delivered": "LINE で届いていないことを確認した",
    "customer_confirmed": "お客様に確認した",
    "other": "その他",
}
_PURPOSE_LABELS = {
    "reply": "返信", "first_reply": "初回受付", "urgent": "緊急定型",
    "image_receipt": "画像受領", "image_result": "画像読取", "follow": "友だち追加",
    "receipt_number": "受付番号", "other": "その他",
}
_ACTOR_LABELS = {"bot": "bot", "human": "大野", "approved_draft": "承認済み下書き"}


def _bad_request() -> Response:
    return Response(status_code=400)     # 固定応答（入力値を反射しない）


def _page(rows: list[dict], notice: str) -> str:
    """サーバ描画（値は全て html.escape・本文なし）。"""
    parts = [
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>送信確認待ち - 案件管理（仮）</title>",
        "<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;"
        "padding:.6rem 1rem 2rem;box-sizing:border-box;background:#eef1f5;color:#222}"
        "h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
        ".item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
        ".meta{color:#8a8f98;font-size:.78rem;margin-top:.3rem}.badge{display:inline-block;"
        "border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#fff3d6;color:#8a6d00;margin-right:.35rem}"
        "form{display:flex;gap:.5rem;flex-wrap:wrap;align-items:center;margin-top:.5rem}"
        "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;"
        "background:#fff;min-height:44px}select{min-height:44px;font-size:.95rem}"
        ".empty{color:#667;text-align:center;margin:1.2rem 0}.notice{background:#e8f0fb;"
        "border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}</style></head><body>",
        "<h1>送信確認待ち</h1>",
        '<p class="sub">LINE への送信結果が確認できなかった操作の一覧です。LINE アプリで'
        "届いているかを確認し、結果を確定してください（自動再送はしません）。"
        '<a href="/app/approvals">返信の承認へ戻る</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{html.escape(notice)}</p>')
    if not rows:
        parts.append('<p class="empty">送信確認待ちはありません</p>')
    for r in rows:
        op_id = html.escape(str(r["op_id"]))
        parts.append('<div class="item">')
        parts.append(f'<span class="badge">{html.escape(_PURPOSE_LABELS.get(r["purpose"], r["purpose"]))}</span>'
                     f'<span class="badge">{html.escape(_ACTOR_LABELS.get(r["actor"], r["actor"]))}</span>'
                     f'<span class="badge">{html.escape(str(r["business"]))}</span>')
        parts.append(f'<div class="meta">操作 ID: {op_id}｜会話参照: {html.escape(str(r["conversation_ref"]))}'
                     f'｜着手: {html.escape(str(r["started_at"]))}｜試行: {int(r["attempts"])}</div>')
        parts.append(f'<form method="post" action="{PAGE}/confirm">'
                     f'<input type="hidden" name="op_id" value="{op_id}">'
                     '<select name="reason">'
                     + "".join(f'<option value="{html.escape(k)}">{html.escape(v)}</option>'
                               for k, v in _REASON_LABELS.items())
                     + '</select>'
                     '<button type="submit" name="outcome" value="sent">送信済みとして確定</button>'
                     '<button type="submit" name="outcome" value="failed">失敗として確定</button>'
                     "</form></div>")
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


_NOTICES = {
    "ok": "確定しました。", "not_found": "対象の操作が見つかりません。",
    "not_unconfirmed": "この操作は既に確定済みです。", "bad_input": "入力が不正です。",
    "db": "記録を読み書きできませんでした。",
}


async def send_ops_page(request: Request):
    code = request.query_params.get("done", "")
    notice = _NOTICES.get(code, "") if code else ""
    try:
        rows = await send_ledger.list_unconfirmed(LIST_LIMIT)
    except Exception:
        rows = []
        notice = _NOTICES["db"]
    return HTMLResponse(_page(rows, notice))


async def send_ops_confirm(request: Request):
    """人の確定（PRG: 303 で一覧へ戻す）。理由・結果は閉集合・操作 ID は固定書式のみ。"""
    form = await request.form()
    op_id = str(form.get("op_id", ""))
    outcome = str(form.get("outcome", ""))
    reason = str(form.get("reason", ""))
    if not _OP_ID_RE.fullmatch(op_id) or outcome not in _OUTCOMES \
            or reason not in send_ledger.HUMAN_REASONS:
        return _bad_request()
    try:
        result = await send_ledger.confirm_by_human(op_id, _OUTCOMES[outcome], reason)
    except Exception:
        result = "db"
    return RedirectResponse(f"{PAGE}?done={result}", status_code=303)


async def api_send_ops_unconfirmed(request: Request):
    """参照 API（本文なし・件数と一覧）。"""
    try:
        rows = await send_ledger.list_unconfirmed(LIST_LIMIT)
    except Exception:
        return {"ok": False, "reason": "db", "count": 0, "items": []}
    return {"ok": True, "count": len(rows), "items": rows}


router.add_api_route(PAGE, _gate(send_ops_page), methods=["GET"])
router.add_api_route(f"{PAGE}/confirm", _gate(send_ops_confirm), methods=["POST"])
router.add_api_route("/app/api/send_ops/unconfirmed", _gate(api_send_ops_unconfirmed),
                     methods=["GET"])
