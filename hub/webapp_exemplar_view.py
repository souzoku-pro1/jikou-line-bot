"""webapp_exemplar_view — JIKOU-REPLY-Q3a (a): 手本（reply_exemplar）の一覧・承認・退役（PWA）

正本: 時効LINEボット_返信規則_v1.4.md §10-7（手本は大野が印を付けた匿名化済み文だけ）・§10-9（凍結昇格は
反映待ち止まり）・§10-10（RV-08: 履歴）。QUEUE-2026-10-12-C C-2 (a)。/app/send_ops・/app/pending と同型
（サーバ描画・native form POST・PRG 303・認証は _gate）。

- 一覧（/app/exemplars）: 業務・category・段階・匿名化済み本文（相談者文と返信）・状態（承認済み／退役）・
  approved_by / approved_at・使用回数。原文・氏名・LINE userId は表に無い（匿名化済みのみ＝RV-10）。
- 操作は 2 つ（理由は固定・自動送信の口は持たない）:
  1. 「承認」= reply_exemplar.approve_exemplar（退役中 → 承認済み・approved_by=human・approved_at=now・履歴 approved）
  2. 「退役」= reply_exemplar.deactivate_exemplar（承認済み → 退役・履歴 deactivated）
  退役した手本は bot の参考文例に使われない（active のみ）。
- 参照 API（/app/api/exemplars）: 件数と一覧（匿名化済み本文を含む・原文なし）。
"""

import html
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import reply_exemplar
from hub.webapp_auth import _gate

router = APIRouter()

PAGE = "/app/exemplars"
BUSINESS = "jikou"
_ID_RE = re.compile(r"^\d{1,12}$")
_ACTIONS = {"approve": reply_exemplar.approve_exemplar, "retire": reply_exemplar.deactivate_exemplar}
_NOTICES = {
    "approved": "承認しました（bot の参考文例に使われます）。",
    "retired": "退役にしました（参考文例に使われません）。",
    "not_changed": "状態は変わりませんでした（既に同じ状態です）。",
    "not_found": "対象が見つかりません。", "bad_input": "入力が不正です。",
    "db": "記録を読み書きできませんでした。",
}


def _bad_request() -> Response:
    return Response(status_code=400)


def _page(rows: list[dict], notice: str, show_all: bool) -> str:
    e = html.escape
    parts = [
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>手本 - 案件管理（仮）</title>",
        "<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;"
        "padding:.6rem 1rem 2rem;box-sizing:border-box;background:#eef1f5;color:#222}"
        "h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
        ".item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
        ".meta{color:#8a8f98;font-size:.78rem;margin-top:.3rem}.badge{display:inline-block;"
        "border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#fff3d6;color:#8a6d00;margin-right:.35rem}"
        ".off{background:#e5e7eb;color:#555}.q{font-size:.9rem;color:#445;margin-top:.4rem;white-space:pre-wrap}"
        ".a{font-size:.95rem;margin-top:.3rem;white-space:pre-wrap}"
        "form{display:inline-flex;gap:.5rem;align-items:center;margin-top:.5rem;margin-right:.5rem}"
        "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;"
        "background:#fff;min-height:44px}"
        ".empty{color:#667;text-align:center;margin:1.2rem 0}.notice{background:#e8f0fb;"
        "border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}</style></head><body>",
        "<h1>手本（事務所の過去回答）</h1>",
        '<p class="sub">返信欄で「手本にしてよい」と印を付けた返信の匿名化済み文です。承認済みの手本は bot の'
        "下書き生成に「参考文例（そのまま写さない）」として添えられます。"
        f'<a href="{PAGE}{"" if show_all else "?all=1"}">{"承認済みのみ表示" if show_all else "退役も表示"}</a>｜'
        '<a href="/app/approvals">返信の承認へ戻る</a>｜<a href="/app/pending">対応待ち</a></p>',
    ]
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    if not rows:
        parts.append('<p class="empty">手本はありません</p>')
    for r in rows:
        rid = int(r["id"])
        active = bool(r["active"])
        parts.append('<div class="item">')
        parts.append(f'<span class="badge{"" if active else " off"}">{"承認済み" if active else "退役"}</span>'
                     f'<span class="badge">{e(str(r.get("category_id") or "分類なし"))}</span>'
                     f'<span class="badge">{e(str(r["stage"]))}</span>'
                     f'<span class="badge">使用 {int(r["used_count"])} 回</span>')
        if r.get("inbound_text"):
            parts.append(f'<div class="q">相談者: {e(str(r["inbound_text"]))}</div>')
        parts.append(f'<div class="a">返信: {e(str(r["human_reply"]))}</div>')
        parts.append(f'<div class="meta">承認: {e(str(r.get("approved_by") or ""))}｜{e(str(r.get("approved_at") or ""))}'
                     f'｜業者: {e(str(r.get("creditor_name") or "（なし）"))}｜ID: {rid}</div>')
        action = "retire" if active else "approve"
        label = "退役にする" if active else "承認する"
        parts.append(f'<form method="post" action="{PAGE}/update">'
                     f'<input type="hidden" name="exemplar_id" value="{rid}">'
                     f'<button type="submit" name="action" value="{action}">{label}</button></form>')
        parts.append("</div>")
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def exemplars_page(request: Request):
    code = request.query_params.get("done", "")
    notice = _NOTICES.get(code, "") if code else ""
    show_all = request.query_params.get("all", "") == "1"
    try:
        rows = await reply_exemplar.list_for_view(BUSINESS, include_inactive=show_all)
    except Exception:
        rows = []
        notice = _NOTICES["db"]
    return HTMLResponse(_page(rows, notice, show_all))


async def exemplars_update(request: Request):
    """人の操作: 承認／退役（PRG）。action は閉集合・ID は固定書式のみ。"""
    form = await request.form()
    xid = str(form.get("exemplar_id", ""))
    action = str(form.get("action", ""))
    if not _ID_RE.fullmatch(xid) or action not in _ACTIONS:
        return _bad_request()
    try:
        exists = await reply_exemplar.get_exemplar(int(xid))
        if exists is None:
            return RedirectResponse(f"{PAGE}?done=not_found", status_code=303)
        ok = await _ACTIONS[action](int(xid))
    except Exception:
        return RedirectResponse(f"{PAGE}?done=db", status_code=303)
    done = ("approved" if action == "approve" else "retired") if ok else "not_changed"
    return RedirectResponse(f"{PAGE}?done={done}", status_code=303)


async def api_exemplars(request: Request):
    try:
        rows = await reply_exemplar.list_for_view(BUSINESS, include_inactive=True)
    except Exception:
        return {"ok": False, "reason": "db", "count": 0, "items": []}
    return {"ok": True, "count": len(rows), "items": rows}


router.add_api_route(PAGE, _gate(exemplars_page), methods=["GET"])
router.add_api_route(f"{PAGE}/update", _gate(exemplars_update), methods=["POST"])
router.add_api_route("/app/api/exemplars", _gate(api_exemplars), methods=["GET"])
