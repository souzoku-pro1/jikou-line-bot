"""相談カード読取 Webhook — HOUKI-CARD-READ

shinjutsu_webhook（H7C）同型の受け口。App 40 のレコードに 相談カード（FILE）を
添付し 相談カード読取=読取依頼 にすると kintone Webhook が本受け口を叩き、
hub/houki_card_read がスキャンを読み取って空欄へ転記する。

認証（H-1 確立形の fail-closed・shinjutsu と同じ判定関数を共用）:
- HOUKI_WEBHOOK_TOKEN 未設定=404（存在しないフリ）
- 時効側トークンとの同値=誤設定として 404（固定文言で警告ログ）
- token 不一致=403

ゲート順: token → body の app.id が App 40 と完全一致（不在・不一致は
skip=app_mismatch・作用 0）→ record id → get_record → 相談カード読取 が
読取依頼 かつ 相談カード が非空（それ以外は skip・作用 0）→ claim
（読取依頼→読取中 を $revision CAS・敗者は skip=cas_lost）→ 読取本体は
BackgroundTasks（kintone Webhook の応答待ちを AI 呼出で占有しない。claim が
同期なので二重配信は敗者 0 作用）。

状態機械は 相談カード読取 欄そのもの（本モジュールはメモリ状態を持たない）。
読取本体の終端（読取済/要確認）と通知は hub/houki_card_read が担う。
"""

import hmac
import logging
import os

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from hub import houki_card_read as reader
from hub import kintone as hub_kintone
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webhook_auth import extract_record_id
from shinjutsu_webhook import _TOKEN_ENV, houki_webhook_disabled_reason

logger = logging.getLogger("houki_card")

router = APIRouter()


def _fv(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


@router.post("/souzoku-houki/card/{secret}")
async def houki_card_webhook(secret: str, request: Request,
                             background: BackgroundTasks):
    reason = houki_webhook_disabled_reason()
    if reason is not None:
        if reason != "token_unset":
            logger.warning("[HOUKI_CARD] endpoint disabled (token misconfig)")
        return JSONResponse(status_code=404, content={"error": "not found"})
    if not hmac.compare_digest(secret or "",
                               os.environ.get(_TOKEN_ENV, "")):
        return JSONResponse(status_code=403, content={"error": "forbidden"})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"error": "invalid json"})

    # app.id は App 40 と完全一致（kintone Webhook 形のみ受ける・不在も不一致）
    app_obj = body.get("app") if isinstance(body.get("app"), dict) else {}
    if str(app_obj.get("id") or "") != str(APP_HOUKI_CASE.app_id()):
        logger.warning("[HOUKI_CARD] app mismatch in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "app_mismatch"})

    record_id = extract_record_id(body)
    if not record_id or not str(record_id).isdigit():
        logger.warning("[HOUKI_CARD] record id missing in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "no_record_id"})
    record_id = str(record_id)

    try:
        record = await hub_kintone.get_record(APP_HOUKI_CASE, record_id)
        if _fv(record, reader.FIELD_STATUS) != reader.STATUS_REQUESTED:
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "status_not_requested"})
        if not ((record.get(reader.FIELD_CARD) or {}).get("value") or []):
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "no_card"})
        if await reader.claim(record) is None:
            logger.info("[HOUKI_CARD] cas lost record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "cas_lost"})
        # claim 後の正本（次 revision）を読取本体へ渡す（CAS の起点を揃える）
        latest = await hub_kintone.get_record(APP_HOUKI_CASE, record_id)
    except Exception as e:
        logger.error("[HOUKI_CARD] error record_id=%s: %s",
                     emit(record_id, "record_id", "log", "operator"),
                     emit(str(e), "vendor_raw", "log", "operator"))
        return JSONResponse(status_code=500,
                            content={"error": "internal_error"})

    logger.info("[HOUKI_CARD] claimed record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    background.add_task(reader.run_card_read, latest)
    return JSONResponse(status_code=200,
                        content={"ok": True, "record_id": record_id,
                                 "claimed": True})
