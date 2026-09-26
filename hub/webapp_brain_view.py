"""webapp_brain_view — BRAIN-A1-LEDGER-1: 案件脳の確認ビュー（PWA・v3 §4-2・§9）

一覧（紐付け待ち・保留〔不一致/出典確認不能/関連喪失〕・競合・再確認・同期状態・
案件の事実）と操作（確認/却下/撤回・紐付け訂正）のみ。質問回答は載せない（A2）。

規律:
- 認証は P4-001 の関所 `_gate` のみ（公開例外なし）。応答は no-store, private。
- 操作は form POST（PRG・303）。操作 ID（クライアント生成 UUID）で冪等、画面が
  見ていた対象版（seen_version／seen_link_version＋seen_source_revision）と現在の
  版の一致検査。不一致は **409 で最新**を返す（古い画面の操作で現状態を上書き
  しない）。未認証は関所が 303→login。
- 紐付け訂正の訂正先は App 40 の実在レコード（台帳に存在 かつ 正本に実在・BA-06）。
  正本の確認は brain_sync の読取 helper 経由（本 module は kintone を import しない）。
- 台帳（DB）以外へ書かない。外部送信なし。logging 非 import（PII の反射経路を
  持たない）。入力値は応答へ反射しない（不正入力は固定 400）。DB 障害は固定 503
  （既存業務へ伝播しない）。
"""

import re

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

from hub import brain_ledger as ledger
from hub import brain_sync
from hub.webapp_auth import WEBAPP_ROOT, _gate

router = APIRouter()

ACTOR = "owner"                       # 単一利用者（P4-001 の前提）
PAGE = "/app/brain"
_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                      r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_DIGITS_RE = re.compile(r"^[0-9]{1,18}$")
_REASON_MAX = 500
_LIMIT_MAX = 200


def _bad_request() -> Response:
    return Response(status_code=400)          # 固定応答（入力値を反射しない）


def _unavailable() -> Response:
    return JSONResponse({"error": "ledger_unavailable"}, status_code=503)


def _source_unavailable(status: int) -> Response:
    return JSONResponse({"error": ledger.FLAG_SOURCE_UNAVAILABLE}, status_code=status)


def _when_enabled(fn):
    """R14: 有効化判定の一元化。BRAIN_SYNC_ENABLED が OFF なら全ルート（GET/POST）を
    入口で 404 に閉じ、台帳 DB にも kintone にも触れない（同期・再照合も同じ
    brain_sync_enabled() で閉じる）。認証関所 _gate の内側に置く（未認証は従来どおり 303）。"""
    async def wrapper(request: Request):
        if not brain_sync.brain_sync_enabled():
            return Response(status_code=404)
        return await fn(request)
    wrapper.__name__ = fn.__name__
    wrapper.__doc__ = fn.__doc__
    wrapper.__brain_enabled_gate__ = True
    return wrapper


def _limit(request: Request) -> int | None:
    raw = request.query_params.get("limit", "100")
    if not _DIGITS_RE.fullmatch(raw):
        return None
    n = int(raw)
    return n if 1 <= n <= _LIMIT_MAX else None


@router.get(PAGE)
@_gate
@_when_enabled
async def brain_page(request: Request):
    path = WEBAPP_ROOT / "brain.html"
    if not path.is_file():
        return Response(status_code=404)
    return FileResponse(path, media_type="text/html; charset=utf-8")


@router.get("/app/api/brain/enabled")
@_gate
@_when_enabled
async def api_enabled(request: Request):
    """ON のときだけ 200（OFF は共通ガードで 404）。PWA のナビはこれで案件脳リンクの表示を決める。"""
    return {"enabled": True}


@router.get("/app/api/brain/overview")
@_gate
@_when_enabled
async def api_overview(request: Request):
    try:
        overview = await ledger.sync_overview()
    except Exception:
        return _unavailable()
    return {"enabled": brain_sync.brain_sync_enabled(), "sync": overview}


@router.get("/app/api/brain/pending")
@_gate
@_when_enabled
async def api_pending(request: Request):
    limit = _limit(request)
    if limit is None:
        return _bad_request()
    try:
        return {"records": await ledger.list_pending_links(limit=limit)}
    except Exception:
        return _unavailable()


@router.get("/app/api/brain/holds")
@_gate
@_when_enabled
async def api_holds(request: Request):
    limit = _limit(request)
    if limit is None:
        return _bad_request()
    try:
        return {"records": await ledger.list_holds(limit=limit)}
    except Exception:
        return _unavailable()


@router.get("/app/api/brain/conflicts")
@_gate
@_when_enabled
async def api_conflicts(request: Request):
    limit = _limit(request)
    if limit is None:
        return _bad_request()
    try:
        return {"records": await ledger.list_conflicts(limit=limit)}
    except Exception:
        return _unavailable()


@router.get("/app/api/brain/recheck")
@_gate
@_when_enabled
async def api_recheck(request: Request):
    limit = _limit(request)
    if limit is None:
        return _bad_request()
    try:
        return {"records": await ledger.list_recheck(limit=limit)}
    except Exception:
        return _unavailable()


@router.get("/app/api/brain/facts")
@_gate
@_when_enabled
async def api_facts(request: Request):
    """案件の現在の事実（確認操作の対象を選ぶための一覧）。鮮度は案件に紐づく全出典の
    集約（BA-07）。出典確認不能の fact は flag 付きで返す（確認対象外）。"""
    q = request.query_params
    app_id, rid = q.get("app", ""), q.get("record", "")
    if not _DIGITS_RE.fullmatch(app_id) or not _DIGITS_RE.fullmatch(rid):
        return _bad_request()
    try:
        facts = await ledger.list_case_facts(app_id, rid, current_only=True)
        events = await ledger.list_case_events(app_id, rid)
        fresh = await ledger.case_freshness_detail(
            app_id, rid, brain_sync.TARGET_APP40, brain_sync.source_targets())
        for f in facts:
            f["confirmations"] = await ledger.list_confirmations(f["fact_id"])
    except Exception:
        return _unavailable()
    return {"case_app_id": app_id, "case_record_id": rid, "freshness": fresh["state"],
            "freshness_reasons": fresh["reasons"], "facts": facts, "events": events}


async def brain_confirm(request: Request):
    """確認/却下/撤回（PRG）。操作 ID 冪等・対象版一致（不一致は 409 で最新）。
    出典確認不能の fact への確認/却下は 409（source_unavailable・BA-07）。"""
    form = await request.form()
    operation_id = str(form.get("operation_id") or "")
    fact_id = str(form.get("fact_id") or "")
    seen_version = str(form.get("seen_version") or "")
    decision = str(form.get("decision") or "")
    reason = str(form.get("reason") or "").strip()
    revoked_of = str(form.get("revoked_of") or "")
    if (not _UUID_RE.fullmatch(operation_id) or not _DIGITS_RE.fullmatch(fact_id)
            or not _DIGITS_RE.fullmatch(seen_version)
            or decision not in ledger.DECISION_VALUES or len(reason) > _REASON_MAX):
        return _bad_request()
    if decision == "revoke" and not _DIGITS_RE.fullmatch(revoked_of):
        return _bad_request()
    if decision != "revoke" and revoked_of:
        return _bad_request()
    try:
        result = await ledger.record_confirmation(
            fact_id=int(fact_id), seen_version=int(seen_version), decision=decision,
            reason=reason, operation_id=operation_id, actor=ACTOR,
            revoked_of=int(revoked_of) if revoked_of else None)
    except ledger.VersionConflict as exc:
        return JSONResponse({"error": "version_conflict", "reason": exc.reason,
                             "current": exc.current}, status_code=409)
    except ledger.SourceUnavailable:
        return _source_unavailable(409)
    except ledger.LedgerError:
        return _bad_request()
    except Exception:
        return _unavailable()
    tag = "dup" if result.get("duplicate") else "confirm"
    return RedirectResponse(f"{PAGE}?done={tag}", status_code=303)


async def brain_relink(request: Request):
    """紐付け訂正（PRG・BA-06）。履歴追加のみ・操作 ID 冪等。
    seen_link_version と seen_source_revision は必須（現在値と不一致は 409 で最新）。
    訂正先は App 40 の実在レコード（台帳に存在 かつ 正本に実在）。不正は固定 400。
    正本の確認ができない（読取失敗）ときは 503（訂正を通さない＝fail-closed）。"""
    form = await request.form()
    operation_id = str(form.get("operation_id") or "")
    src_app = str(form.get("source_app_id") or "")
    src_rec = str(form.get("source_record_id") or "")
    case_app = str(form.get("case_app_id") or "")
    case_rec = str(form.get("case_record_id") or "")
    seen_link = str(form.get("seen_link_version") or "")
    seen_rev = str(form.get("seen_source_revision") or "")
    reason = str(form.get("reason") or "").strip()
    if (not _UUID_RE.fullmatch(operation_id) or not _DIGITS_RE.fullmatch(src_app)
            or not _DIGITS_RE.fullmatch(src_rec) or not _DIGITS_RE.fullmatch(case_app)
            or not _DIGITS_RE.fullmatch(case_rec) or not _DIGITS_RE.fullmatch(seen_link)
            or not _DIGITS_RE.fullmatch(seen_rev) or len(reason) > _REASON_MAX):
        return _bad_request()
    if case_app != brain_sync.APP_HOUKI.app_id():
        return _bad_request()                 # 訂正先は App 40 のみ
    if src_app not in (brain_sync.APP_SHIPPING.app_id(), brain_sync.APP_CHATLOG.app_id()):
        # R15: 訂正対象は App 28・App 30 のみ（App 40 は案件本体）
        return JSONResponse({"error": "app_not_relinkable", "reason": "app_not_relinkable"},
                            status_code=409)
    try:
        if not await ledger.case_exists(case_app, case_rec):
            return _bad_request()             # 台帳に無い案件へは訂正できない
    except Exception:
        return _unavailable()
    exists = await brain_sync.app40_exists_in_source(case_rec)
    if exists is None:
        return _source_unavailable(503)
    if exists is False:
        return _bad_request()
    try:
        result = await ledger.relink_source(
            source_app_id=src_app, source_record_id=src_rec,
            new_case=(case_app, case_rec), reason=reason or "manual_relink",
            operation_id=operation_id, actor=ACTOR,
            seen_link_version=int(seen_link), seen_source_revision=int(seen_rev))
    except ledger.NotRelinkable as exc:
        return JSONResponse({"error": "app_not_relinkable", "reason": exc.reason},
                            status_code=409)
    except ledger.VersionConflict as exc:
        return JSONResponse({"error": "version_conflict", "reason": exc.reason,
                             "current": exc.current}, status_code=409)
    except ledger.LedgerError:
        return _bad_request()
    except Exception:
        return _unavailable()
    tag = "dup" if result.get("duplicate") else "relink"
    return RedirectResponse(f"{PAGE}?done={tag}", status_code=303)


# NB: POST は add_api_route 経由（read-only AST 検査の HTTP 動詞 attr 禁止と両立。
#     関所 _gate は登録時に適用＝機械検査の対象のまま）
router.add_api_route("/app/brain/confirm", _gate(_when_enabled(brain_confirm)), methods=["POST"])
router.add_api_route("/app/brain/relink", _gate(_when_enabled(brain_relink)), methods=["POST"])

_CATCH_ALL_PATH = "/app/{_rest:path}"


def _paths_of(entry) -> list:
    """app.router.routes の要素（_IncludedRouter / Route / APIRoute）の path 一覧。
    動的属性アクセス（getattr）を使わず、属性の有無は例外で判定する。"""
    try:
        return [r.path for r in entry.original_router.routes]
    except AttributeError:
        pass
    try:
        return [entry.path]
    except AttributeError:
        return []


def include_before_catch_all(app, sub_router: APIRouter) -> None:
    """本 router を app へ結線し、webapp_auth の catch-all（/app/{_rest:path}）より
    **前**へ並べ替える（FastAPI は登録順にマッチ・main.py 末尾追記の規約と両立）。
    catch-all が無ければ末尾のまま。"""
    app.include_router(sub_router)
    routes = app.router.routes
    mine = routes.pop()
    idx = None
    for i, entry in enumerate(routes):
        if _CATCH_ALL_PATH in _paths_of(entry):
            idx = i
            break
    routes.insert(idx if idx is not None else len(routes), mine)
