"""webapp_souzoku_dashboard — PWA-BATCH-1 B: 相続案件ダッシュボード（read-only）

正本: docs/plan/2026-08_pwa-product-design_v2.4.md §4.3/§6.6/§6.7/§6.8/§6.9/
§6.10/§12（branch plan-audit 9406ca6 収載）＋本票 PWA-BATCH-1（fix1 反映済み）。
認証は P4-001 の関所（hub.webapp_auth の `_gate`）にそのまま乗る——本 module に
公開例外はない（全 route session 必須・機械検査テストで強制）。

構成（1画面ダッシュボード・書き込み経路ゼロ）:
- 案件一覧 = 相談カード (相続)（SOUZOKU_KINTONE_APP_ID）検索（更新順・ページング。
  自由文字列を kintone query へ埋めない既存規律のため、氏名検索は取得済み頁の
  client 側絞込のみ＝非反射）
- ダッシュボード = 案件単票＋相続人一覧（App36）＋人物情報（App34）＋財産目録
  （App35）＋進行状態（書類ステータス・DerivationRun head・件数 summary）＋
  直近生成書類（App30 案件絞込・kintone 原本リンク）

規律（P4 系先例＋RV08/CANCEL の reader 規律・fix1 で強化）:
- **read-only 徹底**: kintone 書込み API 呼出しゼロ（AST 機械検査で強制）。
  確定・承認・編集の操作 UI は一切置かない（「機械は確定しない」①§2.7）。
- **PWA-01 field 閉集合**: 取得 fields は「UI 表示・API 使用と 1:1」の閉集合のみ
  （①§12.7 data 最小化）。応答は閉集合への**投影**を重ねる（fixture/実機に余剰
  field が現れても公開範囲が黙って広がらない二重の防御）。filter 判定にのみ使う
  field（統合状態・取消済み）は取得閉集合に含み、応答投影（VIEW）からは落とす。
- **PWA-02 全件カーソル**: App34/App36/App35 は `$id` 厳密単調増加カーソルで
  全件取得（daily_healthcheck H11a・RV08 reconcile の確立型。limit 固定打ち切り
  なし）。件数・除外件数・評価額合計は全件基準。カーソル逆行/重複/非数字・
  page 上限超過は例外＝当該 section PARTIAL（不完全な値を完全値として出さない）。
- **PWA-03 金額整数集計**: 評価額合計はサーバ側 Python int（任意精度＝桁落ち
  なし）。厳密 grammar `^[0-9]+$` を通った値のみ加算し、grammar 外（小数・指数・
  負数・桁区切り等）が 1 件でもあれば「集計不能」（0 円へ落とさない）。空値は
  0 円と区別して合計対象外＋件数注記。JSON へは文字列で搭載（BigInt 非搭載）。
- **App34 読取は person_validity.filter_active_persons 経由**（統合済み無効の
  読み飛ばし・reader manifest 登録済み=test_rv08_soft_merge）。
- **App36 読取は app36_validity.filter_active_heir_rows 経由**（取消済みの
  読み飛ばし・reader manifest 登録済み=test_p3_003c_cancel）。除外件数は
  注記用に返す（未確定・取消の状態注記は表示する＝本票 B(iv)）。
- **PII 非出力**: 本 module は logging を一切 import しない（構造的にログ反射
  経路なし・webapp_auth と同じ流儀）。section 取得失敗（PARTIAL・①§6）は
  固定 flag のみ（例外詳細は応答・ログとも非搭載）。
- query 安全規律: kintone query へ埋める値は「数字列検証済み record_id」
  「数字列検証済み env app_id」「int カーソル」のみ（自由文字列を埋めない=非反射）。
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
# PWA-03: 加算を許す評価額の厳密 grammar（非負整数の数字列のみ。負数は現仕様で
# 不採用＝grammar 外として「集計不能」へ倒す。小数・指数・桁区切りも同様）
_AMOUNT_RE = re.compile(r"^[0-9]+$")
# PWA-02: 全件カーソルの page 単位と暴走上限（上限超過は例外＝PARTIAL。
# 100 page × 100 行 = 案件あたり 10,000 行まで＝実務上十分な上限）
_PAGE_SIZE = 100
_MAX_PAGES = 100

# ── PWA-01: field 閉集合（取得=UI 表示・API 使用と 1:1。完全一致 pin 対象） ──
# FETCH = search_records へ渡す取得閉集合／VIEW = 応答へ投影する表示閉集合。
# 差分は filter 判定にのみ使う field（統合状態・取消済み）だけ。
_CASE_LIST_FIELDS = ["$id", "氏名", "被相続人名", "書類ステータス",
                     "登録日時", "更新日時"]
_CASE_FIELDS = ["$id", "氏名", "被相続人名", "続柄", "書類ステータス",
                "登録日時", "更新日時"]
_PERSON_FETCH_FIELDS = ["$id", "氏名", "続柄メモ", "生死区分", "被相続人フラグ",
                        "名寄せ確定", "相続人候補", "相続資格", "確認状態",
                        MERGE_STATE_FIELD]
_PERSON_VIEW_FIELDS = [f for f in _PERSON_FETCH_FIELDS
                       if f != MERGE_STATE_FIELD]
_HEIR_FETCH_FIELDS = ["$id", "氏名", "続柄", "法定相続分", "状態", "戸籍確認済",
                      "印鑑証明", "データ源", CANCELLED_FIELD]
_HEIR_VIEW_FIELDS = [f for f in _HEIR_FETCH_FIELDS if f != CANCELLED_FIELD]
_ASSET_FIELDS = ["$id", "財産種別", "名義", "評価額", "評価方法", "評価基準日",
                 "評価確定", "データ源", "有効"]
_DOC_FIELDS = ["$id", "件名", "チャネル", "方向", "発送ステータス", "発送日時",
               "成果物"]

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


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "")


def _project(records: list, fields: list) -> list:
    """PWA-01: 応答への投影（閉集合 field のみ通す）。取得 fields 指定と重ねた
    二重の防御——fixture/実機側に余剰 field が現れても応答へ出さない。"""
    allowed = set(fields)
    return [{k: v for k, v in r.items() if k in allowed} for r in records]


def _advance(page: list, cursor: int) -> int:
    """PWA-02: `$id` カーソルの厳密単調増加検証。非数字・重複・逆行は例外
    （＝当該 section PARTIAL。無限 loop と重複/欠落計上を構造的に排除する）。"""
    for r in page:
        rid_s = _v(r, "$id")
        if not rid_s.isdigit():
            raise ValueError("cursor id is not numeric")
        rid = int(rid_s)
        if rid <= cursor:
            raise ValueError("cursor is not strictly increasing")
        cursor = rid
    return cursor


def _sum_assets(rows: list) -> dict:
    """PWA-03: 評価額の全件集計（サーバ側 Python int=任意精度・桁落ちなし）。

    - 有効="no" の行は算入しない（有効 flag はそのまま行表示される）
    - 空値は 0 円と区別して合計対象外（blank_count で注記）
    - 厳密 grammar 外（小数・指数・負数・桁区切り等）が 1 件でもあれば
      「集計不能」＝部分合計も返さない（0 円へ黙って落とさない）
    - 金額は JSON へ文字列で搭載（BigInt/数値化しない・表示 format は画面側固定）
    """
    total = 0
    counted = 0
    unconfirmed = 0
    blank = 0
    for r in rows:
        if _v(r, "有効") == "no":
            continue
        raw = _v(r, "評価額")
        if raw == "":
            blank += 1
            continue
        if not _AMOUNT_RE.fullmatch(raw):
            return {"computable": False}
        total += int(raw)
        counted += 1
        if _v(r, "評価確定") != "yes":
            unconfirmed += 1
    return {"computable": True, "amount": str(total), "counted": counted,
            "unconfirmed_count": unconfirmed, "blank_count": blank}


# ── section 別 loader（reader 規律の対象は _load_persons / _load_heirs） ──────

async def _load_persons(case_record_id: str) -> dict:
    """App34 人物（reader manifest 登録済み・search=filter 規律・全件カーソル）。"""
    active: list = []
    raw_count = 0
    cursor = 0
    for _ in range(_MAX_PAGES):
        page = await kintone.search_records(
            APP_KOSEKI_PERSON,
            f'案件レコードID = "{case_record_id}" and $id > {cursor} '
            f"order by $id asc limit {_PAGE_SIZE}",
            fields=_PERSON_FETCH_FIELDS)
        cursor = _advance(page, cursor)
        raw_count += len(page)
        # RV-08: 統合済み無効の行は page ごとに読み飛ばし（一点除外）
        active = active + filter_active_persons(page)
        if len(page) < _PAGE_SIZE:
            return {"records": _project(active, _PERSON_VIEW_FIELDS),
                    "excluded_merged_count": raw_count - len(active)}
    raise RuntimeError("page limit exceeded")    # 暴走防御＝PARTIAL へ


async def _load_heirs(case_record_id: str) -> dict:
    """App36 相続人（reader manifest 登録済み・search=filter 規律・全件カーソル）。"""
    active: list = []
    raw_count = 0
    cursor = 0
    for _ in range(_MAX_PAGES):
        page = await kintone.search_records(
            APP_SOUZOKUNIN,
            f'案件レコードID = "{case_record_id}" and $id > {cursor} '
            f"order by $id asc limit {_PAGE_SIZE}",
            fields=_HEIR_FETCH_FIELDS)
        cursor = _advance(page, cursor)
        raw_count += len(page)
        # P3-003C-CANCEL: 取消済み行は page ごとに読み飛ばし（共通 filter）
        active = active + filter_active_heir_rows(page)
        if len(page) < _PAGE_SIZE:
            return {"records": _project(active, _HEIR_VIEW_FIELDS),
                    "excluded_cancelled_count": raw_count - len(active)}
    raise RuntimeError("page limit exceeded")    # 暴走防御＝PARTIAL へ


async def _load_assets(case_record_id: str) -> dict:
    """App35 財産（全件カーソル・合計はサーバ側整数集計＝PWA-02/03）。
    有効 flag は行にそのまま返し画面で注記表示（行の除外はしない）。"""
    rows: list = []
    cursor = 0
    for _ in range(_MAX_PAGES):
        page = await kintone.search_records(
            APP_ZAISAN,
            f'案件レコードID = "{case_record_id}" and $id > {cursor} '
            f"order by $id asc limit {_PAGE_SIZE}",
            fields=_ASSET_FIELDS)
        cursor = _advance(page, cursor)
        rows = rows + page
        if len(page) < _PAGE_SIZE:
            return {"records": _project(rows, _ASSET_FIELDS),
                    "total": _sum_assets(rows)}
    raise RuntimeError("page limit exceeded")    # 暴走防御＝PARTIAL へ


async def _load_documents(case_record_id: str) -> dict:
    """App30 直近生成書類（案件アプリID＋案件レコードID の両絞込——App30 は
    時効/相続の両ユニットが同居するため app_id 側も必須）。「直近」の設計どおり
    更新順 20 件のみ（全件カーソルの対象は App34/35/36＝PWA-02 裁定の範囲）。"""
    app_id = APP_SOUZOKU_CASES.app_id() or ""
    if not app_id.isdigit():
        return {"records": []}           # env 未設定は空（縮退・書き込みなし）
    rows = await kintone.search_records(
        APP_SHIPPING,
        f'案件アプリID = "{app_id}" and 案件レコードID = "{case_record_id}" '
        "order by 更新日時 desc limit 20",
        fields=_DOC_FIELDS)
    return {"records": _project(rows, _DOC_FIELDS)}


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
    return {"records": _project(records, _CASE_LIST_FIELDS),
            "limit": limit, "offset": offset}


@router.get("/app/api/souzoku/cases/{record_id}")
@_gate
async def api_souzoku_case_dashboard(request: Request):
    """案件ダッシュボード（1画面分の read-only 集約）。
    PWA-01: 案件単票も閉集合 fields 取得（`$id =` 検索・全 field 取得の
    get_record は使わない）＋応答投影。不存在は固定 404。"""
    record_id = request.path_params.get("record_id", "")
    if not _RECORD_ID_RE.fullmatch(record_id):
        return Response(status_code=404)     # 固定（値を反射しない）
    found = await kintone.search_records(
        APP_SOUZOKU_CASES, f'$id = "{record_id}" limit 1',
        fields=_CASE_FIELDS)
    if not found:
        return Response(status_code=404)
    case = _project(found, _CASE_FIELDS)[0]
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
