"""
送付状自動生成 Webhook モジュール

処理フロー:
  1. URL の合言葉（DOCUMENT_WEBHOOK_SECRET）を検証
  2. kintone Webhook ボディからレコード ID を取得
  3. 書類ステータス が「送付状作成」のときだけ実行（誤発火・ループ防止）
  4. 相談カードレコードを取得し差し込みデータを組み立て
  5. templates/送付状_委任契約書.docx に機械置換
  6. kintone にファイルアップロード → 添付フィールドに書き戻し
  7. 書類ステータスを「送付状作成済」に更新
"""

import hmac
import io
import logging
import os
from datetime import date

import httpx

from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式）
from docx import Document
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import kintone as hub_kintone
from hub.webhook_auth import extract_record_id, verify_token

logger = logging.getLogger("document")

# 相談カードアプリのハブ経由接続（T0-1）
_APP = hub_kintone.KintoneApp(
    "相談カード (相続)", "SOUZOKU_KINTONE_APP_ID", "SOUZOKU_KINTONE_API_TOKEN"
)

# ── 定数（後から変更しやすいようにここにまとめる） ─────────────────────────
FIELD_STATUS      = "書類ステータス"   # トリガー用ドロップダウンフィールドコード
TRIGGER_VALUE     = "送付状作成"       # このステータスのときだけ生成を実行
COMPLETED_VALUE   = "送付状作成済"     # 生成完了後に書き込む値
FIELD_ATTACHMENT  = "送付状"           # 添付ファイルフィールドコード（FILE型）
TEMPLATE_PATH     = "docx_templates/送付状_委任契約書.docx"

# ── 環境変数 ────────────────────────────────────────────────────────────────
_SUBDOMAIN        = os.environ.get("KINTONE_SUBDOMAIN", "")
_APP_ID           = os.environ.get("SOUZOKU_KINTONE_APP_ID", "")
_API_TOKEN        = os.environ.get("SOUZOKU_KINTONE_API_TOKEN", "")
_WEBHOOK_SECRET   = os.environ.get("DOCUMENT_WEBHOOK_SECRET", "")


# ── ユーティリティ ───────────────────────────────────────────────────────────

def _kintone_base() -> str:
    sub = _SUBDOMAIN.replace(".cybozu.com", "").strip()
    if sub.startswith("http"):
        return sub.rstrip("/")
    return f"https://{sub}.cybozu.com"


# fill_template / to_wareki は T0-3 で hub/docx_builder.py に移設（実装不変）。
# 既存の import 経路（from document_webhook import fill_template 等）互換のため re-export
from hub.docx_builder import fill_template, to_wareki  # noqa: E402,F401


# ── kintone API（T0-1 で hub/kintone に移設。旧名は委譲ラッパーとして温存） ──

async def _get_record(record_id: str) -> dict:
    return await hub_kintone.get_record(_APP, record_id)


async def _upload_file(filename: str, content: bytes) -> str:
    """multipart でファイルをアップロードして fileKey を返す"""
    mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    return await hub_kintone.upload_file(_APP, filename, content, mime)


async def _update_record(record_id: str, fields: dict) -> None:
    await hub_kintone.update_record(_APP, record_id, fields)


# ── FastAPI Router ───────────────────────────────────────────────────────────

router = APIRouter()


@router.post("/document/{secret}")
async def document_webhook(secret: str, request: Request):
    # 1. 合言葉チェック（hub/webhook_auth。403 を返す点は従来どおり）
    if not verify_token(secret or "", "DOCUMENT_WEBHOOK_SECRET"):
        return JSONResponse(status_code=403, content={"error": "forbidden"})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})

    # 2. レコード ID を取得（hub/webhook_auth・従来と同一ロジック）
    record_id = extract_record_id(body)
    if not record_id:
        logger.warning("レコードIDが取得できませんでした")
        return JSONResponse(status_code=200, content={"ok": True, "skip": "no_record_id"})

    # 3. トリガー判定（Webhook ボディのステータス値で確認）
    try:
        status_in_webhook = body["record"][FIELD_STATUS]["value"]
    except (KeyError, TypeError):
        status_in_webhook = None

    if status_in_webhook != TRIGGER_VALUE:
        # H01: record_id は emit(record_id) 経由・webhook 外部入力の status echo は抑止（drop）
        logger.info("トリガー値不一致のためスキップ record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=200, content={"ok": True, "skip": "not_triggered"})

    try:
        # 4. レコード取得
        record = await _get_record(record_id)

        # ループ防止：既に完了済みなら何もしない
        current_status = record.get(FIELD_STATUS, {}).get("value", "")
        if current_status == COMPLETED_VALUE:
            logger.info("送付状作成済みのためスキップ record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200, content={"ok": True, "skip": "already_done"})

        # 5. 差し込みデータを組み立て
        def fv(code: str) -> str:
            return record.get(code, {}).get("value") or ""

        data = {
            "{{日付}}":      to_wareki(date.today()),
            "{{依頼者住所}}": fv("住所"),
            "{{依頼者氏名}}": fv("氏名"),
            "{{被相続人名}}": fv("被相続人名"),
        }
        # H01: data は 住所/氏名/被相続人名 を含む PII のため log に出さない（record_id のみ）
        logger.info("差し込みデータ組立完了 record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))

        # 6. テンプレート置換
        docx_bytes = fill_template(TEMPLATE_PATH, data)

        # 7a. kintone にファイルアップロード
        file_key = await _upload_file("送付状_委任契約書.docx", docx_bytes)
        # H01: fileKey は kintone の生 external_ref のため抑止（record_id のみ素通し）
        logger.info("ファイルアップロード完了 record_id=%s fileKey=%s",
                    emit(record_id, "record_id", "log", "operator"),
                    emit(file_key, "external_ref", "log", "operator"))

        # 7b. 添付フィールド書き戻し + ステータス更新（1回の PUT にまとめる）
        await _update_record(record_id, {
            FIELD_ATTACHMENT: [{"fileKey": file_key}],
            FIELD_STATUS:     COMPLETED_VALUE,
        })
        logger.info("レコード更新完了 record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))

    except Exception:
        logger.exception("document_webhook処理エラー record_id=%s", record_id)
        return JSONResponse(status_code=500, content={"error": "internal_error"})

    return JSONResponse(status_code=200, content={"ok": True, "record_id": record_id})
