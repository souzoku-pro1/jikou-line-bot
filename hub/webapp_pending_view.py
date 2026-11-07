"""webapp_pending_view — JIKOU-REPLY-Q1c (d) → UX-PENDING-1 (4): 旧 /app/pending は統合一覧 /app/inbox へ転送

- 旧画面（一覧・不要にする・対応終了（bot 再開））は hub/webapp_inbox_view に統合した。本 module に残るのは
  1. GET /app/pending → 303 /app/inbox（ブックマーク・旧通知文の互換）
  2. 参照 API /app/api/pending/open（件数と一覧・本文なし・line_user_hash を出さない）
- 認証は従来どおり _gate。操作 route（resolve / end_attending）は本 module から無くした（統合一覧側に同等の操作がある）。
"""

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from hub import reply_pending
from hub.webapp_auth import _gate

router = APIRouter()

PAGE = "/app/pending"
INBOX = "/app/inbox"
BUSINESS = "jikou"


async def pending_page(request: Request):
    """UX-PENDING-1 (4): 統合一覧へ転送（query は引き継がない）。"""
    return RedirectResponse(INBOX, status_code=303)


async def api_pending_open(request: Request):
    try:
        rows = await reply_pending.list_open(reply_pending.LIST_LIMIT)
    except Exception:
        return {"ok": False, "reason": "db", "count": 0, "items": []}
    items = [{k: v for k, v in r.items() if k != "line_user_hash"} for r in rows]
    return {"ok": True, "count": len(items), "items": items}


router.add_api_route(PAGE, _gate(pending_page), methods=["GET"])
router.add_api_route("/app/api/pending/open", _gate(api_pending_open), methods=["GET"])
