"""相続放棄申述書 自動生成 Webhook — SOUZOKU-HOUKI-H7C

notice_webhook（JIKOU-NOTICE-1）同型の受け口。App 40 のレコード番号を受け、
hub/houki_shinjutsu で生成・凍結検証した docx を App 40「申述書」（FILE）へ
添付する。

認証（H-1 確立形の fail-closed）:
- HOUKI_WEBHOOK_TOKEN 未設定=受け口自体を無効（404・存在しないフリ）
- 時効側トークン（DOCUMENT_WEBHOOK_SECRET / KINTONE_WEBHOOK_TOKEN）との
  同値=誤設定として同じく 404（固定文言で警告ログ）
- token 不一致=403（notice 同型）

冪等（二重添付しない・実装方式）:
- 「申述書」欄が既に非空 → skip（作用 0・上書きしない。再生成は添付を
  削除してから再 POST する運用）
- 添付 PUT は取得時 $revision の CAS——並行二重 POST は敗者が 409=cas_lost
  （作用 0）。状態機械は持たない（App 40 の status は動かさない・
  トリガ運用は大野側=kintone Webhook でも手動 POST でも受かる）

fail-closed（票の逐語）:
- 生成拒否（必須欠落・知った日導出不能・マッピング不能）=生成せず
  管理者通知（拒否理由の閉集合語彙+レコード番号のみ・PII 非搭載）
- アップロード/添付失敗=要確認通知+500（生成物を無言で失わない・
  kintone Webhook 経由なら再配送で再試行される）

ボディ: kintone Webhook 形（record.$id）と単純形（{"record_id": ...}）の
両方を受ける（kintone 形のときは app.id の一致も検査）。
"""

import hashlib
import hmac
import logging
import os

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import houki_shinjutsu
from hub import kintone as hub_kintone
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webhook_auth import extract_record_id

logger = logging.getLogger("shinjutsu")

FIELD_ATTACHMENT = "申述書"          # App 40 FILE（form fields API 実測）
_TOKEN_ENV = "HOUKI_WEBHOOK_TOKEN"
# 同値検査の対象=時効側の webhook 系トークン（取り違え・使い回しの誤設定を
# 受け口無効で検知する。H-1 の houki_channel_disabled_reason と同思想）
_JIKOU_TOKEN_ENVS = ("DOCUMENT_WEBHOOK_SECRET", "KINTONE_WEBHOOK_TOKEN")

router = APIRouter()


def houki_webhook_disabled_reason() -> str | None:
    """受け口を無効にする理由（閉集合: token_unset / token_misconfig）。
    None=有効。"""
    token = os.environ.get(_TOKEN_ENV, "")
    if not token:
        return "token_unset"
    for env in _JIKOU_TOKEN_ENVS:
        other = os.environ.get(env, "")
        if other and hmac.compare_digest(token, other):
            return "token_misconfig"
    return None


def _fv(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


async def _notify(text: str) -> None:
    """管理者 LINE 通知（best-effort・固定文言+閉集合語彙+レコード番号のみ）。"""
    try:
        from hub.notify import notify_admin_line
        await notify_admin_line(text)
    except Exception:
        logger.error("[SHINJUTSU] admin notify failed (fixed text)")


@router.post("/souzoku-houki/shinjutsu/{secret}")
async def shinjutsu_webhook(secret: str, request: Request):
    reason = houki_webhook_disabled_reason()
    if reason is not None:
        if reason != "token_unset":
            logger.warning("[SHINJUTSU] endpoint disabled (token misconfig)")
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

    # kintone Webhook 形なら app.id の一致を検査（notice 同型）
    app_in_body = str(((body.get("app") or {}) if isinstance(body.get("app"),
                                                             dict)
                       else {}).get("id") or "")
    if app_in_body and app_in_body != str(APP_HOUKI_CASE.app_id()):
        logger.warning("[SHINJUTSU] app mismatch in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "app_mismatch"})

    record_id = str(body.get("record_id") or "") or extract_record_id(body)
    if not record_id or not str(record_id).isdigit():
        logger.warning("[SHINJUTSU] record id missing in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "no_record_id"})
    record_id = str(record_id)

    try:
        record = await hub_kintone.get_record(APP_HOUKI_CASE, record_id)
        revision = _fv(record, "$revision")

        # 冪等: 申述書欄が既に非空=作用 0（自動上書きしない）
        attachment = (record.get(FIELD_ATTACHMENT) or {}).get("value") or []
        if attachment:
            logger.info("[SHINJUTSU] already attached record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "already_attached"})

        try:
            docx_bytes = houki_shinjutsu.generate(record)
        except houki_shinjutsu.ShinjutsuRejection as e:
            logger.info("[SHINJUTSU] rejected record_id=%s reasons=%s",
                        emit(record_id, "record_id", "log", "operator"),
                        emit(len(e.reasons), "count", "log", "operator"))
            await _notify(
                f"【相続放棄申述書】案件レコードNo.{record_id} は次の理由で"
                "生成しませんでした:\n・" + "\n・".join(e.reasons)
                + "\nkintone で入力・確定後、もう一度実行してください。")
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "rejected", "reasons": e.reasons})

        name = _fv(record, "顧客名")
        filename = f"相続放棄申述書_{record_id}_{name}.docx"
        mime = ("application/vnd.openxmlformats-officedocument"
                ".wordprocessingml.document")
        try:
            file_key = await hub_kintone.upload_file(
                APP_HOUKI_CASE, filename, docx_bytes, mime)
            await hub_kintone.update_record(APP_HOUKI_CASE, record_id, {
                FIELD_ATTACHMENT: [{"fileKey": file_key}],
            }, revision=revision or None)
        except hub_kintone.KintoneConflict:
            # 並行二重 POST の敗者（revision CAS）=作用 0
            logger.info("[SHINJUTSU] cas lost record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "cas_lost"})
        except Exception:
            # アップロード/添付失敗=生成物を無言で失わない（要確認通知+500）
            logger.error("[SHINJUTSU] attach failed record_id=%s",
                         emit(record_id, "record_id", "log", "operator"))
            await _notify(
                f"【相続放棄申述書・要確認】案件レコードNo.{record_id} の"
                "申述書は生成できましたが、kintone への添付に失敗しました。"
                "もう一度実行してください（添付欄が空のままなら再生成されます）。")
            return JSONResponse(status_code=500,
                                content={"error": "attach_failed"})
        logger.info("[SHINJUTSU] attached record_id=%s bytes=%s",
                    emit(record_id, "record_id", "log", "operator"),
                    emit(len(docx_bytes), "count", "log", "operator"))
    except houki_shinjutsu.ShinjutsuIntegrityError:
        logger.error("[SHINJUTSU] integrity error (template/body mismatch)")
        await _notify(
            f"【相続放棄申述書・要確認】案件レコードNo.{record_id} の生成で"
            "テンプレート完全性検証に失敗しました（添付していません）。")
        return JSONResponse(status_code=500,
                            content={"error": "integrity_error"})
    except Exception as e:
        # sink 規律: 例外本文・型名は出さない（固定文言+emit のみ。
        # vendor_raw は emit 側で完全抑止される）
        logger.error("[SHINJUTSU] error record_id=%s: %s",
                     emit(record_id, "record_id", "log", "operator"),
                     emit(str(e), "vendor_raw", "log", "operator"))
        return JSONResponse(status_code=500,
                            content={"error": "internal_error"})

    return JSONResponse(status_code=200,
                        content={"ok": True, "record_id": record_id})
