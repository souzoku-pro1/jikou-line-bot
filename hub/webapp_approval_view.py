"""webapp_approval_view — P4-004: 承認キュー参照（read-only API＋画面・P4 系最小）

正本: DRAFT_P4_PWA_INVENTORY §2（App29 絞込の read-only API＋画面）§5。
認証は P4-001 の関所（hub.webapp_auth の `_gate`）に乗る——公開例外なし。

裁定（[人]済み・2026-07-28）:
- **AI 下書き本文（顧客往復含む）を表示してよい**（内部専用画面）。
- **絞込既定=送信済み=no のみ**（`all=1` で全件切替あり）。

制約（本票の絶対条件）:
- **参照のみ**——承認・送信操作の UI/API は作らない（承認経路は既存 webhook が
  単一の正のまま）。kintone 書込み API の呼出しゼロ（AST 機械検査テストで強制）。

query 安全規律: 絞込値は固定リテラル（送信済み "no"）のみ・自由文字列を query へ
埋めない。ページングは P4-002 と同じ既定（上限50/既定20・不正は固定 400 非反射）。
"""

import re

from fastapi import APIRouter, Request
from fastapi.responses import Response

from hub import kintone
from hub.webapp_auth import WEBAPP_ROOT, _gate

router = APIRouter()

APP_APPROVAL = kintone.KintoneApp("App 29 (承認キュー)", "APP_APPROVAL",
                                  "TOKEN_APPROVAL")

PAGE_LIMIT_MAX = 50
PAGE_LIMIT_DEFAULT = 20
# fix1 M01: line_user_id（生の external ID）は UI 未使用のためブラウザへ送らない。
_FIELDS = ["$id", "顧客名", "顧客メッセージ", "AI下書き",
           "カテゴリ", "判断理由", "ステータス2", "送信済み", "更新日時"]
_INT_RE = re.compile(r"^[0-9]{1,6}$")


def _bad_request() -> Response:
    return Response(status_code=400)     # 固定応答（入力値を反射しない）


@router.get("/app/api/approvals")
@_gate
async def api_approvals(request: Request):
    """承認キュー一覧（既定=送信済み no のみ・all=1 で全件・更新順）。"""
    q = request.query_params
    limit_s = q.get("limit", str(PAGE_LIMIT_DEFAULT))
    offset_s = q.get("offset", "0")
    if not _INT_RE.fullmatch(limit_s) or not _INT_RE.fullmatch(offset_s):
        return _bad_request()
    limit, offset = int(limit_s), int(offset_s)
    if not (1 <= limit <= PAGE_LIMIT_MAX):
        return _bad_request()
    show_all = q.get("all")
    if show_all is not None and show_all != "1":     # 閉集合（発明しない）
        return _bad_request()
    query = f"order by 更新日時 desc limit {limit} offset {offset}"
    if show_all != "1":                  # 裁定: 既定は送信済み=no のみ
        query = '送信済み in ("no") ' + query
    records = await kintone.search_records(APP_APPROVAL, query, fields=_FIELDS)
    return {"records": records, "limit": limit, "offset": offset,
            "all": show_all == "1"}


@router.get("/app/approvals")
@_gate
async def approvals_page(request: Request):
    path = WEBAPP_ROOT / "approvals.html"
    if not path.is_file():
        return Response(status_code=404)
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type="text/html; charset=utf-8")
