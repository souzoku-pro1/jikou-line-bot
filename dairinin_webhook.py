"""代理人目録 自動生成 Webhook — HOUKI-DAIRININ-1

shinjutsu_webhook（SOUZOKU-HOUKI-H7C）同型の受け口。App 40 のレコード番号を受け、hub/houki_dairinin で生成・
凍結検証した docx を App 40「代理人目録」（FILE）へ添付する。送信はしない（添付のみ）。

認証（shinjutsu_webhook.houki_webhook_disabled_reason を共用）: HOUKI_WEBHOOK_TOKEN 未設定=404・時効側と同値=404・
不一致=403。ボディは kintone Webhook 形（record.$id・app.id 一致）と単純形（{"record_id": ...}）。

fail-closed:
- 添付欄「代理人目録」が App 40 に無い（form fields で確認・結果は再利用）= 生成せず skip（field_missing・管理者通知 1 回）
  → 大野の kintone 操作（FILE 欄の追加）待ち。
- 生成拒否（申述書と同じ 10 種）= 生成せず管理者通知（閉集合語彙+レコード番号のみ）。
- 事務所 env（OFFICE_*／OFFICE_BAR／OFFICE_REG）未設定= 空欄で生成し業務 LINE に警告 1 回（throttle key）。
冪等: 添付欄が非空=skip・添付 PUT は $revision CAS（並行二重 POST の敗者=cas_lost）。アップロード/添付失敗=要確認通知+500。
"""

import hmac
import logging
import os
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import houki_dairinin, houki_shinjutsu
from hub import kintone as hub_kintone
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webhook_auth import extract_record_id
from shinjutsu_webhook import houki_webhook_disabled_reason

logger = logging.getLogger("dairinin")

FIELD_ATTACHMENT = "代理人目録"          # App 40 FILE（本票で追加を依頼＝大野の kintone 操作）
_TOKEN_ENV = "HOUKI_WEBHOOK_TOKEN"
_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
FIELD_CACHE_TTL = 3600.0
_field_cache: dict = {"at": 0.0, "ok": None}

router = APIRouter()


def _fv(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


async def _notify(text: str, throttle_key: str = "") -> None:
    try:
        from hub.notify import notify_admin_line
        await notify_admin_line(text, throttle_key=throttle_key)
    except Exception:
        logger.error("[DAIRININ] admin notify failed (fixed text)")


def reset_field_cache() -> None:
    _field_cache.update(at=0.0, ok=None)


async def attachment_field_exists(*, now: float | None = None) -> bool:
    t = time.monotonic() if now is None else now
    if _field_cache["ok"] is not None and t - _field_cache["at"] < FIELD_CACHE_TTL:
        return bool(_field_cache["ok"])
    try:
        props = await hub_kintone.get_form_fields(APP_HOUKI_CASE)
    except Exception:
        logger.warning("[DAIRININ] form fields lookup failed (no attach)")
        return False
    ok = (props.get(FIELD_ATTACHMENT) or {}).get("type") == "FILE"
    _field_cache.update(at=t, ok=ok)
    return ok


@router.post("/souzoku-houki/dairinin/{secret}")
async def dairinin_webhook(secret: str, request: Request):
    reason = houki_webhook_disabled_reason()
    if reason is not None:
        if reason != "token_unset":
            logger.warning("[DAIRININ] endpoint disabled (token misconfig)")
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
        logger.warning("[DAIRININ] app mismatch in webhook body")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "app_mismatch"})
    record_id = str(body.get("record_id") or "") or extract_record_id(body)
    if not record_id or not str(record_id).isdigit():
        logger.warning("[DAIRININ] record id missing in webhook body")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "no_record_id"})
    record_id = str(record_id)

    if not await attachment_field_exists():
        logger.warning("[DAIRININ] attachment field missing (no generate)")
        await _notify(f"【代理人目録】App 40 に添付欄「{FIELD_ATTACHMENT}」（添付ファイル）がありません。"
                      "欄を追加してから再実行してください（生成していません）。", "dairinin_field_missing")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "field_missing"})

    try:
        record = await hub_kintone.get_record(APP_HOUKI_CASE, record_id)
        revision = _fv(record, "$revision")
        attachment = (record.get(FIELD_ATTACHMENT) or {}).get("value") or []
        if attachment:
            logger.info("[DAIRININ] already attached record_id=%s", emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200, content={"ok": True, "skip": "already_attached"})
        try:
            docx_bytes = houki_dairinin.generate(record)
        except houki_shinjutsu.ShinjutsuRejection as e:
            logger.info("[DAIRININ] rejected record_id=%s reasons=%s",
                        emit(record_id, "record_id", "log", "operator"), emit(len(e.reasons), "count", "log", "operator"))
            await _notify(f"【代理人目録】案件レコードNo.{record_id} は次の理由で生成しませんでした:\n・"
                          + "\n・".join(e.reasons) + "\nkintone で入力・確定後、もう一度実行してください。")
            return JSONResponse(status_code=200, content={"ok": True, "skip": "rejected", "reasons": e.reasons})
        missing = houki_dairinin.missing_office_envs()
        if missing:
            logger.warning("[DAIRININ] office env missing count=%s", emit(len(missing), "count", "log", "operator"))
            await _notify("【代理人目録・事務所情報未設定】次の環境変数が未設定のため空欄で生成しました: "
                          + "・".join(missing) + "。Railway に設定すると次回から差し込まれます。", "dairinin_office_env")
        name = _fv(record, "顧客名")
        filename = f"代理人目録_{record_id}_{name}.docx"
        try:
            file_key = await hub_kintone.upload_file(APP_HOUKI_CASE, filename, docx_bytes, _MIME)
            await hub_kintone.update_record(APP_HOUKI_CASE, record_id, {FIELD_ATTACHMENT: [{"fileKey": file_key}]},
                                            revision=revision or None)
        except hub_kintone.KintoneConflict:
            logger.info("[DAIRININ] cas lost record_id=%s", emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200, content={"ok": True, "skip": "cas_lost"})
        except Exception:
            logger.error("[DAIRININ] attach failed record_id=%s", emit(record_id, "record_id", "log", "operator"))
            await _notify(f"【代理人目録・要確認】案件レコードNo.{record_id} の代理人目録は生成できましたが、kintone への"
                          "添付に失敗しました。もう一度実行してください（添付欄が空のままなら再生成されます）。")
            return JSONResponse(status_code=500, content={"error": "attach_failed"})
        logger.info("[DAIRININ] attached record_id=%s bytes=%s", emit(record_id, "record_id", "log", "operator"),
                    emit(len(docx_bytes), "count", "log", "operator"))
    except houki_dairinin.DairininIntegrityError:
        logger.error("[DAIRININ] integrity error (template/body mismatch)")
        await _notify(f"【代理人目録・要確認】案件レコードNo.{record_id} の生成でテンプレート完全性検証に失敗しました"
                      "（添付していません）。")
        return JSONResponse(status_code=500, content={"error": "integrity_error"})
    except Exception as e:
        logger.error("[DAIRININ] error record_id=%s: %s", emit(record_id, "record_id", "log", "operator"),
                     emit(str(e), "vendor_raw", "log", "operator"))
        return JSONResponse(status_code=500, content={"error": "internal_error"})
    return JSONResponse(status_code=200, content={"ok": True, "record_id": record_id})
