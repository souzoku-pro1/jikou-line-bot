"""webapp_pending_view — JIKOU-REPLY-Q1c (d): 対応待ち（§10-6）の一覧と人の操作

正本: 時効LINEボット_返信規則_v1.4.md R-J1・R-J11（対応終了＝bot 再開は大野の操作）・R-J14・§10-6。
QUEUE-2026-10-12-C C-1 (d)。/app/send_ops と同型（サーバ描画・native form POST・PRG 303・認証は _gate）。

- 一覧（/app/pending）: 未処理（open）の対応待ちを古い順に表示。本文・氏名・LINE userId は出さない
  （経路・理由・カテゴリ・承認キュー No.（返信欄へのリンク）・会話参照 ID・通知結果・J-0 の有無・
  対応中の状態のみ＝RV-10）。
- 操作は 2 つ（理由は閉集合・自動送信の口は持たない）:
  1. 「不要にする」= reply_pending.mark_not_needed（open → not_needed・履歴あり）
  2. 「対応終了（bot 再開）」= send_ledger.end_attending_by_hash（R-J11。V2 ON では対応中の会話に bot が
     自動送信しないため、大野が明示的に戻す口。会話の参照 ID で指定・userId は画面に出さない）
- 解除の既定経路は大野の返信（/app/reply・Q1b）＝送信成功で同じ相談者の open を human_replied にする
  （hub/send_policy.human_replied）。本画面はそれ以外の操作だけを持つ。
- 参照 API（/app/api/pending/open）: 件数と一覧（本文なし）。
"""

import html
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import reply_pending, send_ledger
from hub.webapp_auth import _gate

router = APIRouter()

PAGE = "/app/pending"
BUSINESS = "jikou"
_ID_RE = re.compile(r"^\d{1,12}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_ROUTE_LABELS = {"customer": "顧客対応", "hearing": "ヒアリング", "inbound": "受信入口（停止条件）"}
_REASON_LABELS = {"demoted": "承認降格（下書きあり）", "empty_reply": "空返事の検出",
                  "policy_stopped": "許可表で bot 停止", "claude_outage": "AI 障害・例外"}
_NOTIFY_LABELS = {"none": "通知なし", "sent": "通知済み", "throttled": "本日通知済み（省略）",
                  "failed": "通知失敗", "unconfirmed": "通知結果未確認", "unavailable": "台帳不可（未通知）"}
_CUSTOMER_LABELS = {"none": "顧客へ送信なし", "j0_sent": "初回 1 通（J-0）送信"}
_NOTICES = {
    "ok": "不要にしました。", "not_open": "この対応待ちは既に処理済みです。",
    "not_found": "対象が見つかりません。", "bad_input": "入力が不正です。",
    "db": "記録を読み書きできませんでした。", "resumed": "対応終了（bot 再開）にしました。",
}


def _bad_request() -> Response:
    return Response(status_code=400)


def _page(rows: list[dict], attending: dict, notice: str) -> str:
    parts = [
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>対応待ち - 案件管理（仮）</title>",
        "<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;"
        "padding:.6rem 1rem 2rem;box-sizing:border-box;background:#eef1f5;color:#222}"
        "h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
        ".item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
        ".meta{color:#8a8f98;font-size:.78rem;margin-top:.3rem}.badge{display:inline-block;"
        "border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#fff3d6;color:#8a6d00;margin-right:.35rem}"
        ".att{background:#fde2e2;color:#8a1f1f}.link{font-size:.95rem;margin-top:.35rem}"
        "form{display:inline-flex;gap:.5rem;flex-wrap:wrap;align-items:center;margin-top:.5rem;margin-right:.5rem}"
        "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;"
        "background:#fff;min-height:44px}"
        ".empty{color:#667;text-align:center;margin:1.2rem 0}.notice{background:#e8f0fb;"
        "border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}</style></head><body>",
        "<h1>対応待ち</h1>",
        '<p class="sub">bot が自動返信できず無言にした受信の一覧です。返信は承認キュー No. の返信欄から'
        "行ってください（返信すると同じ相談者の対応待ちは閉じます）。"
        '<a href="/app/approvals">返信の承認へ戻る</a>｜<a href="/app/send_ops">送信確認待ち</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{html.escape(notice)}</p>')
    if not rows:
        parts.append('<p class="empty">対応待ちはありません</p>')
    for r in rows:
        pid = int(r["pending_id"])
        conv = attending.get(r["line_user_hash"]) or {}
        parts.append('<div class="item">')
        badges = (f'<span class="badge">{html.escape(_ROUTE_LABELS.get(r["route"], r["route"]))}</span>'
                  f'<span class="badge">{html.escape(_REASON_LABELS.get(r["reason"], r["reason"]))}</span>'
                  f'<span class="badge">{html.escape(_NOTIFY_LABELS.get(r["notify_state"], r["notify_state"]))}</span>'
                  f'<span class="badge">{html.escape(_CUSTOMER_LABELS.get(r["customer_send"], r["customer_send"]))}</span>')
        if conv.get("attending"):
            badges += '<span class="badge att">対応中（bot 停止）</span>'
        parts.append(badges)
        if r.get("category"):
            parts.append(f'<div class="link">カテゴリ: {html.escape(str(r["category"]))}</div>')
        rid = str(r.get("approval_record_id") or "")
        if rid and _ID_RE.fullmatch(rid):
            parts.append(f'<div class="link"><a href="/app/reply/{html.escape(rid)}">承認キュー No.{html.escape(rid)} の返信欄へ</a></div>')
        else:
            parts.append('<div class="link">承認キュー: 未作成（LINE アプリで受信を確認し、必要なら手動でご返信ください）</div>')
        parts.append(f'<div class="meta">受付: {html.escape(str(r["created_at"]))}｜会話参照: '
                     f'{html.escape(str(conv.get("ref", "") or ""))}｜ID: {pid}</div>')
        parts.append(f'<form method="post" action="{PAGE}/resolve">'
                     f'<input type="hidden" name="pending_id" value="{pid}">'
                     '<button type="submit" name="outcome" value="not_needed">不要にする</button></form>')
        if conv.get("attending"):
            parts.append(f'<form method="post" action="{PAGE}/end_attending">'
                         f'<input type="hidden" name="user_hash" value="{html.escape(str(r["line_user_hash"]))}">'
                         '<button type="submit">対応終了（bot 再開）</button></form>')
        parts.append("</div>")
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def _attending_map(rows: list[dict]) -> dict:
    out: dict = {}
    for h in {r["line_user_hash"] for r in rows}:
        try:
            out[h] = await send_ledger.conversation_state_by_hash(BUSINESS, h) or {}
        except Exception:
            out[h] = {}
    return out


async def pending_page(request: Request):
    code = request.query_params.get("done", "")
    notice = _NOTICES.get(code, "") if code else ""
    try:
        rows = await reply_pending.list_open(reply_pending.LIST_LIMIT)
        attending = await _attending_map(rows)
    except Exception:
        rows, attending = [], {}
        notice = _NOTICES["db"]
    return HTMLResponse(_page(rows, attending, notice))


async def pending_resolve(request: Request):
    """人の操作: 不要にする（PRG）。outcome は閉集合（not_needed のみ）。"""
    form = await request.form()
    pid = str(form.get("pending_id", ""))
    outcome = str(form.get("outcome", ""))
    if not _ID_RE.fullmatch(pid) or outcome != "not_needed":
        return _bad_request()
    result = await reply_pending.mark_not_needed(int(pid))
    return RedirectResponse(f"{PAGE}?done={result}", status_code=303)


async def pending_end_attending(request: Request):
    """人の操作: 対応終了（bot 再開）。会話は line_user_hash で指定（画面に userId を出さない）。"""
    form = await request.form()
    h = str(form.get("user_hash", ""))
    if not _HASH_RE.fullmatch(h):
        return _bad_request()
    try:
        await send_ledger.end_attending_by_hash(BUSINESS, h)
    except Exception:
        return RedirectResponse(f"{PAGE}?done=db", status_code=303)
    return RedirectResponse(f"{PAGE}?done=resumed", status_code=303)


async def api_pending_open(request: Request):
    try:
        rows = await reply_pending.list_open(reply_pending.LIST_LIMIT)
    except Exception:
        return {"ok": False, "reason": "db", "count": 0, "items": []}
    items = [{k: v for k, v in r.items() if k != "line_user_hash"} for r in rows]
    return {"ok": True, "count": len(items), "items": items}


router.add_api_route(PAGE, _gate(pending_page), methods=["GET"])
router.add_api_route(f"{PAGE}/resolve", _gate(pending_resolve), methods=["POST"])
router.add_api_route(f"{PAGE}/end_attending", _gate(pending_end_attending), methods=["POST"])
router.add_api_route("/app/api/pending/open", _gate(api_pending_open), methods=["GET"])
