"""必要戸籍チェックリスト 受け口 — HOUKI-KOSEKI-CHECKLIST-1

shinjutsu_webhook（SOUZOKU-HOUKI-H7C）同型の受け口。App 40 のレコード番号を受け、
hub/houki_koseki_checklist.process_record（初期行の生成 → 連続戸籍充足の候補通知）を即時に走らせる。
定期走査（hub/houki_koseki_checklist.scan_once）が同じ処理を回すため、本受け口は「受任へ遷移した直後に
待たずに初期化したい」ときの任意の口（kintone Webhook／手動 POST・冪等）。

認証（shinjutsu_webhook.houki_webhook_disabled_reason を共用）:
- HOUKI_WEBHOOK_TOKEN 未設定=404・時効側トークンと同値=404・不一致=403
ボディ: kintone Webhook 形（record.$id・app.id 一致検査）と単純形（{"record_id": ...}）。
書込は App 40 の 書類チェック のみ（module 側の契約）。応答は固定語彙（init / candidate）。
"""

import hmac
import logging
import os

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import houki_koseki_checklist as kc
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webhook_auth import extract_record_id
from shinjutsu_webhook import houki_webhook_disabled_reason

logger = logging.getLogger("houki_koseki")
_TOKEN_ENV = "HOUKI_WEBHOOK_TOKEN"

router = APIRouter()


@router.post("/souzoku-houki/koseki-checklist/{secret}")
async def koseki_checklist_webhook(secret: str, request: Request):
    reason = houki_webhook_disabled_reason()
    if reason is not None:
        if reason != "token_unset":
            logger.warning("[HOUKI_KOSEKI] endpoint disabled (token misconfig)")
        return JSONResponse(status_code=404, content={"error": "not found"})
    if not hmac.compare_digest(secret or "", os.environ.get(_TOKEN_ENV, "")):
        return JSONResponse(status_code=403, content={"error": "forbidden"})
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    app_in_body = str(((body.get("app") or {}) if isinstance(body.get("app"), dict) else {}).get("id") or "")
    if app_in_body and app_in_body != str(APP_HOUKI_CASE.app_id()):
        logger.warning("[HOUKI_KOSEKI] app mismatch in webhook body")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "app_mismatch"})
    record_id = str(body.get("record_id") or "") or extract_record_id(body)
    if not record_id or not str(record_id).isdigit():
        logger.warning("[HOUKI_KOSEKI] record id missing in webhook body")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "no_record_id"})
    if kc.disabled():
        return JSONResponse(status_code=200, content={"ok": True, "skip": "disabled"})
    try:
        res = await kc.process_record(str(record_id))
    except Exception:
        logger.error("[HOUKI_KOSEKI] webhook processing failed record_id=%s",
                     emit(str(record_id), "record_id", "log", "operator"))
        return JSONResponse(status_code=500, content={"error": "internal_error"})
    return JSONResponse(status_code=200, content={"ok": True, "record_id": str(record_id), **res})
