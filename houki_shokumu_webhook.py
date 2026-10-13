"""相続放棄案件（App 40）→ 職務上請求の請求案 受け口 — HOUKI-SHOKUMU-LINK-1

shinjutsu_webhook 同型の受け口（認証: HOUKI_WEBHOOK_TOKEN・未設定=404・時効側トークンと同値=404・不一致=403）。
App 40 のレコード番号を受け、hub/houki_shokumu_link.process_record_id で受任確定フックを実行する。
flag SHOKUMU_PLAN_ENABLED が OFF なら I/O ゼロで {"skip": "disabled"}（既定）。
ボディ: kintone Webhook 形（record.$id・app.id の一致検査）と単純形（{"record_id": ...}）の両方。
"""

import hmac
import logging
import os

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import houki_shokumu_link as L
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webhook_auth import extract_record_id
from shinjutsu_webhook import _TOKEN_ENV, houki_webhook_disabled_reason

logger = logging.getLogger("houki_shokumu_link")

router = APIRouter()


@router.post("/souzoku-houki/shokumu-link/{secret}")
async def houki_shokumu_link_webhook(secret: str, request: Request):
    reason = houki_webhook_disabled_reason()
    if reason is not None:
        if reason != "token_unset":
            logger.warning("[SHOKUMU_LINK] endpoint disabled (token misconfig)")
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
        logger.warning("[SHOKUMU_LINK] app mismatch in webhook body")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "app_mismatch"})
    record_id = str(body.get("record_id") or "") or extract_record_id(body)
    if not record_id or not str(record_id).isdigit():
        return JSONResponse(status_code=200, content={"ok": True, "skip": "no_record_id"})
    res = await L.process_record_id(str(record_id))
    status = res.get("status")
    if status == L.RESULT_DISABLED:
        return JSONResponse(status_code=200, content={"ok": True, "skip": "disabled", "record_id": str(record_id)})
    if status == L.RESULT_FAILED:
        logger.warning("[SHOKUMU_LINK] webhook failed record_id=%s", emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=500, content={"error": "failed", "record_id": str(record_id)})
    return JSONResponse(status_code=200, content={"ok": True, "status": status, "record_id": str(record_id),
                                                  "envelope_id": res.get("envelope_id", "")})
