"""webapp_case_views — P4-002: 案件一覧＋案件詳細（read-only proxy API＋画面）

正本: DRAFT_P4_PWA_INVENTORY §2 表——案件一覧=App21 検索（status 別・更新順）／
案件詳細=App21 単票＋App30（発送管理・案件絞込）＋App28（チャットログ・件数程度）。
認証は P4-001 の関所（hub.webapp_auth の `_gate`）に**そのまま乗る**——本 module に
公開例外はない（全 route が session 必須・機械検査テストで強制）。

裁定（[人]済み・2026-07-28）:
- 顧客氏名・案件情報は**そのまま表示**（内部専用画面・kintone 直視と同格）。
  応答は kintone レコード構造の素通し（勝手な要約・変形をしない）。
- App28 は**件数のみ**（直近発話の表示は別票）。

司令塔既定:
- ページング上限 50 件/頁・既定 20。
- 一覧の status 絞込集合は EXPECTED_KINTONE_SCHEMA の **App21 status 実選択肢**
  から構成（本 module で値を発明しない・閉集合外は固定 400）。

read-only 徹底（DRAFT §2）: kintone **書込み API の呼出しゼロ**——本 module は
hub.kintone の get_record/search_records のみを使う（AST 機械検査テストで
create/update/delete/upload の不使用を強制）。

query 安全規律: kintone query へ埋める値は「閉集合の status」「数字列検証済み
record_id」「grammar 検証済み line_user_id」のみ（自由文字列を埋めない=非反射）。
"""

import re

from fastapi import APIRouter, Request
from fastapi.responses import Response

import config
from hub import kintone
from hub.webapp_auth import WEBAPP_ROOT, _gate

router = APIRouter()

APP_CASES = kintone.KintoneApp("App 21 (案件)", "KINTONE_APP_ID",
                               "KINTONE_API_TOKEN")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING",
                                  "TOKEN_SHIPPING")
APP_CHATLOG = kintone.KintoneApp("App 28 (チャットログ)", "APP_CHATLOG",
                                 "TOKEN_CHATLOG")

# 司令塔既定: 絞込集合は schema の実選択肢が単一の正（発明しない）
STATUS_OPTIONS = tuple(config.EXPECTED_KINTONE_SCHEMA
                       ["App 21 (案件)"]["fields"]["status"]["required_options"])
PAGE_LIMIT_MAX = 50
PAGE_LIMIT_DEFAULT = 20
_RECORD_ID_RE = re.compile(r"^[0-9]{1,10}$")
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
_CHAT_COUNT_CAP = 500        # kintone limit 上限。到達時は capped=true で明示

_LIST_FIELDS = ["$id", "status", "顧客名", "問い合わせ業者名", "更新日時"]
_SHIPPING_FIELDS = ["$id", "件名", "チャネル", "方向", "発送ステータス",
                    "発送日時", "追跡番号", "送達結果", "更新日時"]


def _bad_request() -> Response:
    return Response(status_code=400)     # 固定応答（入力値を反射しない）


def _file(name: str, media_type: str = "text/html; charset=utf-8") -> Response:
    path = WEBAPP_ROOT / name
    if not path.is_file():
        return Response(status_code=404)
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type=media_type)


def _page_params(request: Request) -> tuple[int, int] | None:
    """limit/offset の解析（不正は None=固定 400）。clamp はせず閉範囲検査。"""
    try:
        limit = int(request.query_params.get("limit", PAGE_LIMIT_DEFAULT))
        offset = int(request.query_params.get("offset", 0))
    except ValueError:
        return None
    if not (1 <= limit <= PAGE_LIMIT_MAX) or offset < 0:
        return None
    return limit, offset


@router.get("/app/api/cases")
@_gate
async def api_cases(request: Request):
    """案件一覧（App21・status 絞込＋更新順・ページング）。"""
    page = _page_params(request)
    if page is None:
        return _bad_request()
    limit, offset = page
    status = request.query_params.get("status")
    query = f"order by 更新日時 desc limit {limit} offset {offset}"
    if status is not None:
        if status not in STATUS_OPTIONS:     # 閉集合外は固定 400（非反射）
            return _bad_request()
        query = f'status in ("{status}") ' + query
    records = await kintone.search_records(APP_CASES, query,
                                           fields=_LIST_FIELDS)
    return {"records": records, "limit": limit, "offset": offset,
            "status_options": list(STATUS_OPTIONS)}


@router.get("/app/api/cases/{record_id}")
@_gate
async def api_case_detail(request: Request):
    """案件詳細（App21 単票＋App30 案件絞込＋App28 件数のみ）。"""
    record_id = request.path_params.get("record_id", "")
    if not _RECORD_ID_RE.fullmatch(record_id):
        return Response(status_code=404)     # 固定（値を反射しない）
    case = await kintone.get_record(APP_CASES, record_id)
    shipping = await kintone.search_records(
        APP_SHIPPING,
        f'案件レコードID = "{record_id}" order by 更新日時 desc limit 50',
        fields=_SHIPPING_FIELDS)
    chat_count = None
    chat_capped = False
    line_user_id = (case.get("LINEユーザーID") or {}).get("value") or ""
    if _LINE_USER_ID_RE.fullmatch(line_user_id):
        # 裁定: 件数のみ（本文は取得しない・fields を $id に限定）
        chats = await kintone.search_records(
            APP_CHATLOG,
            f'line_user_id = "{line_user_id}" limit {_CHAT_COUNT_CAP}',
            fields=["$id"])
        chat_count = len(chats)
        chat_capped = chat_count >= _CHAT_COUNT_CAP
    return {"case": case, "shipping": shipping,
            "chat_count": chat_count, "chat_count_capped": chat_capped}


@router.get("/app/cases")
@_gate
async def cases_page(request: Request):
    return _file("cases.html")


@router.get("/app/case")
@_gate
async def case_page(request: Request):
    return _file("case.html")
