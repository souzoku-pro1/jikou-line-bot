"""相続放棄 受理通知送付 Webhook — HOUKI-SOUFU-1

App 40（相続放棄案件）の編集 Webhook を受け、status=受理 かつ 受理通知受領日 非空
かつ 受理通知書 添付あり のとき、hub/houki_soufu が 債権者一覧 の対象行ごとに
App 30（発送管理）へ 下書き で起票する（以後は既存の M4 関所に乗る）。

認証（houki_card_webhook / shinjutsu_webhook と同じ fail-closed）:
- HOUKI_WEBHOOK_TOKEN 未設定=404 / 時効側トークンと同値=404 / 不一致=403

ゲート順: token → JSON → body の app.id が App 40 と完全一致（不在・不一致は
skip=app_mismatch）→ record id → 本文 status が 受理/債権者通知 でなければ skip=not_triggered
（作用 0）→ 本体は BackgroundTasks（最新レコードを再取得して起点条件を再判定・
冪等キーで二重起票を防ぐ）。
"""

import hmac
import logging
import os

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from hub import houki_soufu as soufu
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webhook_auth import extract_record_id
from shinjutsu_webhook import _TOKEN_ENV, houki_webhook_disabled_reason

logger = logging.getLogger("houki_soufu")

router = APIRouter()


async def _run(record_id: str) -> None:
    try:
        await soufu.process_soufu(record_id)
    except Exception as e:
        logger.error("[HOUKI_SOUFU] error record_id=%s cls=%s",
                     emit(record_id, "record_id", "log", "operator"),
                     emit(type(e).__name__, "vendor_raw", "log", "operator"))
        await soufu._notify(soufu.NOTIFY_KIND_REVIEW, record_id,
                            f"{soufu.NOTICE_HEAD_REVIEW} 案件レコードNo.{record_id}: 起票処理で"
                            "内部エラーが発生しました（status を「受理」のまま再保存すると再判定されます）")


@router.post("/souzoku-houki/soufu/{secret}")
async def houki_soufu_webhook(secret: str, request: Request, background: BackgroundTasks):
    reason = houki_webhook_disabled_reason()
    if reason is not None:
        if reason != "token_unset":
            logger.warning("[HOUKI_SOUFU] endpoint disabled (token misconfig)")
        return JSONResponse(status_code=404, content={"error": "not found"})
    if not hmac.compare_digest(secret or "", os.environ.get(_TOKEN_ENV, "")):
        return JSONResponse(status_code=403, content={"error": "forbidden"})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    if not isinstance(body, dict):
        return JSONResponse(status_code=400, content={"error": "invalid json"})

    app_obj = body.get("app") if isinstance(body.get("app"), dict) else {}
    if str(app_obj.get("id") or "") != str(APP_HOUKI_CASE.app_id()):
        logger.warning("[HOUKI_SOUFU] app mismatch in webhook body")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "app_mismatch"})

    record_id = extract_record_id(body)
    if not record_id or not str(record_id).isdigit():
        logger.warning("[HOUKI_SOUFU] record id missing in webhook body")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "no_record_id"})
    record_id = str(record_id)

    # 本文ステータス gate（自 update の echo や他ステータスの編集はここで落ちる・作用 0）
    try:
        status_in_body = body["record"][soufu.FIELD_STATUS]["value"]
    except (KeyError, TypeError):
        status_in_body = None
    if status_in_body not in soufu.TRIGGER_STATUSES:
        logger.info("[HOUKI_SOUFU] not triggered record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=200, content={"ok": True, "skip": "not_triggered"})

    background.add_task(_run, record_id)
    return JSONResponse(status_code=200, content={"ok": True, "record_id": record_id, "queued": True})
