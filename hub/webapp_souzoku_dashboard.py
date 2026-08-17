"""webapp_souzoku_dashboard — PWA-BATCH-1 B: 相続案件ダッシュボード（read-only）

正本: docs/plan/2026-08_pwa-product-design_v2.4.md §4.3/§6.6/§6.7/§6.8/§6.9/
§6.10/§12（branch plan-audit 9406ca6 収載）＋本票 PWA-BATCH-1。
認証は P4-001 の関所（hub.webapp_auth の `_gate`）にそのまま乗る——本 module に
公開例外はない（全 route session 必須・機械検査テストで強制）。

構成（1画面ダッシュボード・書き込み経路ゼロ）:
- 案件一覧 = 相談カード (相続)（SOUZOKU_KINTONE_APP_ID）検索（更新順・ページング。
  自由文字列を kintone query へ埋めない既存規律のため、氏名検索は取得済み頁の
  client 側絞込のみ＝非反射）
- ダッシュボード = 案件単票＋相続人一覧（App36）＋人物情報（App34）＋財産目録
  （App35）＋進行状態（書類ステータス・DerivationRun head・件数 summary）＋
  直近生成書類（App30 案件絞込・kintone 原本リンク）

規律（P4 系先例＋RV08/CANCEL の reader 規律）:
- **read-only 徹底**: kintone 書込み API 呼出しゼロ（AST 機械検査で強制）。
  確定・承認・編集の操作 UI は一切置かない（「機械は確定しない」①§2.7）。
- **App34 読取は person_validity.filter_active_persons 経由**（統合済み無効の
  読み飛ばし・reader manifest 登録済み=test_rv08_soft_merge）。
- **App36 読取は app36_validity.filter_active_heir_rows 経由**（取消済みの
  読み飛ばし・reader manifest 登録済み=test_p3_003c_cancel）。除外件数は
  注記用に返す（未確定・取消の状態注記は表示する＝本票 B(iv)）。
- **PII 非出力**: 本 module は logging を一切 import しない（構造的にログ反射
  経路なし・webapp_auth と同じ流儀）。section 取得失敗（PARTIAL・①§6）は
  固定 flag のみ（例外詳細は応答・ログとも非搭載）。
- query 安全規律: kintone query へ埋める値は「数字列検証済み record_id」
  「数字列検証済み env app_id」のみ（自由文字列を埋めない=非反射）。
"""

import re

from fastapi import APIRouter, Request
from fastapi.responses import Response

import config
from hub import kintone
from hub.app36_validity import CANCELLED_FIELD, filter_active_heir_rows
from hub.person_validity import (APP_KOSEKI_PERSON, MERGE_STATE_FIELD,
                                 filter_active_persons)
from hub.webapp_auth import WEBAPP_ROOT, _gate

router = APIRouter()

APP_SOUZOKU_CASES = kintone.KintoneApp("相談カード (相続)",
                                       "SOUZOKU_KINTONE_APP_ID",
                                       "SOUZOKU_KINTONE_API_TOKEN")
APP_SOUZOKUNIN = kintone.KintoneApp("App 36 (相続人)", "APP_SOUZOKUNIN",
                                    "TOKEN_SOUZOKUNIN")
APP_ZAISAN = kintone.KintoneApp("App 35 (財産)", "APP_ZAISAN", "TOKEN_ZAISAN")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING",
                                  "TOKEN_SHIPPING")

PAGE_LIMIT_MAX = 50
PAGE_LIMIT_DEFAULT = 20
_RECORD_ID_RE = re.compile(r"^[0-9]{1,10}$")

# fields 集合は完全一致 pin（test_pwa_batch1・黙った拡張の防波堤）。
# 画面に必要な field だけ取得（①§12.7 data 最小化）
_CASE_LIST_FIELDS = ["$id", "氏名", "被相続人名", "書類ステータス",
                     "登録日時", "更新日時"]
_PERSON_FIELDS = ["$id", "氏名", "続柄メモ", "生死区分", "被相続人フラグ",
                  "名寄せ確定", "相続人候補", "相続資格", "確認状態",
                  "生年月日", "死亡日", MERGE_STATE_FIELD]
_HEIR_FIELDS = ["$id", "氏名", "続柄", "法定相続分", "状態", "戸籍確認済",
                "印鑑証明", "データ源", "導出元人物ID", CANCELLED_FIELD]
_ASSET_FIELDS = ["$id", "財産種別", "名義", "評価額", "評価方法", "評価基準日",
                 "評価確定", "データ源", "有効"]
_DOC_FIELDS = ["$id", "件名", "チャネル", "方向", "発送ステータス", "発送日時",
               "成果物", "更新日時"]

# 未確定注記（①§2.7「機械は確定しない」の画面反映・固定文言）
NOTICE_READONLY = (
    "この画面は読み取り専用の参照ビューです。機械は確定しません——名寄せ・"
    "相続人資格・評価の確定は既存の関所（弁護士の確定操作）経由でのみ行われ"
    "ます。統合済み無効（App34）・取消済み（App36）の行は共通 filter で読み"
    "飛ばし、除外件数のみ注記表示します")


def _bad_request() -> Response:
    return Response(status_code=400)     # 固定応答（入力値を反射しない）


def _file(name: str) -> Response:
    path = WEBAPP_ROOT / name
    if not path.is_file():
        return Response(status_code=404)
    from fastapi.responses import FileResponse
    return FileResponse(path, media_type="text/html; charset=utf-8")


def _page_params(request: Request) -> tuple[int, int] | None:
    """limit/offset の解析（不正は None=固定 400・P4-002 と同一規律）。"""
    try:
        limit = int(request.query_params.get("limit", PAGE_LIMIT_DEFAULT))
        offset = int(request.query_params.get("offset", 0))
    except ValueError:
        return None
    if not (1 <= limit <= PAGE_LIMIT_MAX) or offset < 0:
        return None
    return limit, offset


# ── section 別 loader（reader 規律の対象は _load_persons / _load_heirs） ──────

async def _load_persons(case_record_id: str) -> dict:
    """App34 人物（reader manifest 登録済み・search=filter 規律）。"""
    records = await kintone.search_records(
        APP_KOSEKI_PERSON,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 200',
        fields=_PERSON_FIELDS)
    active = filter_active_persons(records)
    # RV-08: 統合済み無効の行は読み飛ばし（一点除外）。件数は注記用に返す
    return {"records": active,
            "excluded_merged_count": len(records) - len(active)}


async def _load_heirs(case_record_id: str) -> dict:
    """App36 相続人（reader manifest 登録済み・search=filter 規律）。"""
    rows = await kintone.search_records(
        APP_SOUZOKUNIN,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 200',
        fields=_HEIR_FIELDS)
    active = filter_active_heir_rows(rows)
    # P3-003C-CANCEL: 取消済み行は読み飛ばし（共通 filter）。件数は注記用に返す
    return {"records": active,
            "excluded_cancelled_count": len(rows) - len(active)}


async def _load_assets(case_record_id: str) -> dict:
    """App35 財産（有効 flag はそのまま返し画面で注記表示・行の除外はしない）。"""
    rows = await kintone.search_records(
        APP_ZAISAN,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 200',
        fields=_ASSET_FIELDS)
    return {"records": rows}


async def _load_documents(case_record_id: str) -> dict:
    """App30 直近生成書類（案件アプリID＋案件レコードID の両絞込——App30 は
    時効/相続の両ユニットが同居するため app_id 側も必須）。"""
    app_id = APP_SOUZOKU_CASES.app_id() or ""
    if not app_id.isdigit():
        return {"records": []}           # env 未設定は空（縮退・書き込みなし）
    rows = await kintone.search_records(
        APP_SHIPPING,
        f'案件アプリID = "{app_id}" and 案件レコードID = "{case_record_id}" '
        "order by 更新日時 desc limit 20",
        fields=_DOC_FIELDS)
    return {"records": rows}


async def _load_derivation(case_record_id: str) -> dict:
    """DerivationRun head（P3-001 正規経路の read-only・kinship_view と同型）。"""
    from hub.derivation_models import get_current_head
    head = await get_current_head(case_record_id)
    if head is None:
        return {"head": None}
    return {"head": {"run_id": head.id, "run_status": str(head.status),
                     "provisional": bool(head.provisional)}}


async def _guarded(coro) -> dict:
    """PARTIAL 縮退（①§6・§6.7「欠落 section を明示」）: section 単位の取得
    失敗は ok=False の固定 flag のみで表示を継続する。例外の詳細は応答・ログ
    とも非搭載（本 module は logging を import しない＝反射経路なし）。"""
    try:
        data = await coro
    except Exception:
        return {"ok": False}
    return {"ok": True, **data}


def _links() -> dict | None:
    """kintone 原本への到達リンク材料（①§6.7「summary から原本へ到達」）。
    base・app_id は grammar 検証済みの env 由来値のみ（自由文字列なし）。
    base が正規形でなければ None＝リンク非表示の縮退（config 側で検証済み）。"""
    base = config.kintone_record_link_base()
    if base is None:
        return None
    raw = {"case": APP_SOUZOKU_CASES.app_id(),
           "person": APP_KOSEKI_PERSON.app_id(),
           "heir": APP_SOUZOKUNIN.app_id(),
           "asset": APP_ZAISAN.app_id(),
           "shipping": APP_SHIPPING.app_id()}
    return {"base": base,
            "apps": {k: v for k, v in raw.items() if v and v.isdigit()}}


# ── API（read-only・全 route _gate） ─────────────────────────────────────────

@router.get("/app/api/souzoku/cases")
@_gate
async def api_souzoku_cases(request: Request):
    """相続案件一覧（相談カード・更新順・ページング）。"""
    page = _page_params(request)
    if page is None:
        return _bad_request()
    limit, offset = page
    records = await kintone.search_records(
        APP_SOUZOKU_CASES,
        f"order by 更新日時 desc limit {limit} offset {offset}",
        fields=_CASE_LIST_FIELDS)
    return {"records": records, "limit": limit, "offset": offset}


@router.get("/app/api/souzoku/cases/{record_id}")
@_gate
async def api_souzoku_case_dashboard(request: Request):
    """案件ダッシュボード（1画面分の read-only 集約）。"""
    record_id = request.path_params.get("record_id", "")
    if not _RECORD_ID_RE.fullmatch(record_id):
        return Response(status_code=404)     # 固定（値を反射しない）
    case = await kintone.get_record(APP_SOUZOKU_CASES, record_id)
    return {"case": case,
            "persons": await _guarded(_load_persons(record_id)),
            "heirs": await _guarded(_load_heirs(record_id)),
            "assets": await _guarded(_load_assets(record_id)),
            "documents": await _guarded(_load_documents(record_id)),
            "derivation": await _guarded(_load_derivation(record_id)),
            "links": _links(),
            "notice": NOTICE_READONLY}


@router.get("/app/souzoku")
@_gate
async def souzoku_page(request: Request):
    return _file("souzoku.html")


@router.get("/app/souzoku/case")
@_gate
async def souzoku_case_page(request: Request):
    return _file("souzoku_case.html")
