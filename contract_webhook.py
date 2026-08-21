"""委任契約書（時効援用）自動生成 Webhook — CONTRACT-GEN-1（第1版）

処理フロー（document_webhook=送付状と同型）:
  1. URL の合言葉（DOCUMENT_WEBHOOK_SECRET 共用・別 path）を検証
  2. kintone Webhook ボディからレコード ID を取得
  3. 契約書ステータス が「契約書作成」のときだけ実行（誤発火・ループ防止）
  4. App21 レコードを取得し差し込みデータを組み立て
  5. **fail-closed**: 必須項目（依頼者氏名・住所・債権者 1 社以上）が欠落なら
     生成せず、不足フィールド名を明示して拒否（空欄契約書を作らない・
     管理者 LINE へフィールド名のみ通知=値は載せない）
  6. docx_templates/jikou/委任契約書.docx に機械置換（fill_template 共用）
  7. kintone にアップロード → 添付フィールド「委任契約書」書き戻し＋
     ステータス「契約書作成済」更新（1 回の PUT）

差し込み仕様（CONTRACT-GEN-1 設計判断）:
  - {{依頼者氏名}}=顧客名（2 箇所とも）・{{依頼者住所}}=住所
  - {{対象債権者1}}=問い合わせ業者名（既存 field を第 1 債権者の正とする・
    二重入力を作らない）。{{対象債権者2}}/{{対象債権者3}}=同名の新設 field
  - 空き債権者枠・契約年月日は全角空白（原本の〔　〕/年月日欄の体裁を維持・
    契約日は締結時に手書き/CloudSign 上で確定する運用が既定）
  - テンプレ内の報酬等の文言（弁護士凍結事項）には一切触れない（機械置換は
    {{キー}} のみ）

スコープ外: CloudSign 送信 API 結線（第2版・P5）・PDF 自動変換。
"""

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import kintone as hub_kintone
from hub.docx_builder import fill_template
from hub.redact import emit
from hub.webhook_auth import extract_record_id, verify_token

logger = logging.getLogger("contract")

_APP = hub_kintone.KintoneApp(
    "App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")

# ── 定数 ─────────────────────────────────────────────────────────────────────
FIELD_STATUS     = "契約書ステータス"
TRIGGER_VALUE    = "契約書作成"
COMPLETED_VALUE  = "契約書作成済"
FIELD_ATTACHMENT = "委任契約書"
TEMPLATE_PATH    = "docx_templates/jikou/委任契約書.docx"
OUTPUT_FILENAME  = "委任契約書_時効援用.docx"
_BLANK           = "　"                     # 空き枠・契約年月日の全角空白埋め

# 必須項目（fail-closed）: フィールドコード → 不足表示名（値は通知に載せない）
_REQUIRED_NAME   = "顧客名"
_REQUIRED_ADDR   = "住所"
_CREDITOR_FIELDS = ("問い合わせ業者名", "対象債権者2", "対象債権者3")

router = APIRouter()


def _missing_fields(record: dict) -> list[str]:
    """必須欠落のフィールド名一覧（空=生成可）。値は返さない。"""
    def fv(code: str) -> str:
        return str((record.get(code) or {}).get("value") or "").strip()

    missing = []
    if not fv(_REQUIRED_NAME):
        missing.append(_REQUIRED_NAME)
    if not fv(_REQUIRED_ADDR):
        missing.append(_REQUIRED_ADDR)
    if not any(fv(c) for c in _CREDITOR_FIELDS):
        missing.append("債権者（問い合わせ業者名/対象債権者2/対象債権者3 の"
                       "いずれか1つ以上）")
    return missing


def build_fill_data(record: dict) -> dict:
    """差し込み data（{{キー}}→値）。呼出し前に _missing_fields が空であること。"""
    def fv(code: str) -> str:
        return str((record.get(code) or {}).get("value") or "").strip()

    return {
        "{{依頼者氏名}}":  fv(_REQUIRED_NAME),
        "{{依頼者住所}}":  fv(_REQUIRED_ADDR),
        "{{対象債権者1}}": fv("問い合わせ業者名") or _BLANK,
        "{{対象債権者2}}": fv("対象債権者2") or _BLANK,
        "{{対象債権者3}}": fv("対象債権者3") or _BLANK,
        # 契約日欄は空欄（全角空白）で出力し、締結時に確定する運用（既定）
        "{{契約年}}": _BLANK,
        "{{契約月}}": _BLANK,
        "{{契約日}}": _BLANK,
    }


async def _notify_missing(record_id: str, missing: list[str]) -> None:
    """不足項目の明示（管理者 LINE・フィールド名のみ＝値は載せない）。
    通知失敗は生成拒否の結果を変えない（best-effort・ログのみ）。"""
    try:
        from hub.notify import notify_admin_line
        await notify_admin_line(
            f"【委任契約書】案件 No.{record_id} は必須項目が未入力のため"
            f"生成しませんでした。不足: {'・'.join(missing)}。"
            "kintone で入力後、契約書ステータスを「契約書作成」に"
            "設定し直してください")
    except Exception:
        logger.error("[CONTRACT] missing-fields notify failed (fixed text)")


@router.post("/contract/{secret}")
async def contract_webhook(secret: str, request: Request):
    # 1. 合言葉（DOCUMENT_WEBHOOK_SECRET 共用・送付状と同じ流儀）
    if not verify_token(secret or "", "DOCUMENT_WEBHOOK_SECRET"):
        return JSONResponse(status_code=403, content={"error": "forbidden"})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})

    record_id = extract_record_id(body)
    if not record_id:
        logger.warning("[CONTRACT] record id missing in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "no_record_id"})

    # 3. トリガー判定（Webhook ボディの値で早期確認）
    try:
        status_in_webhook = body["record"][FIELD_STATUS]["value"]
    except (KeyError, TypeError):
        status_in_webhook = None
    if status_in_webhook != TRIGGER_VALUE:
        logger.info("[CONTRACT] not triggered record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "not_triggered"})

    try:
        record = await hub_kintone.get_record(_APP, record_id)

        # ループ防止
        current = str((record.get(FIELD_STATUS) or {}).get("value") or "")
        if current == COMPLETED_VALUE:
            logger.info("[CONTRACT] already done record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "already_done"})

        # 5. fail-closed: 必須欠落は生成しない（空欄契約書を作らない）
        missing = _missing_fields(record)
        if missing:
            logger.info("[CONTRACT] missing required fields record_id=%s "
                        "count=%d",
                        emit(record_id, "record_id", "log", "operator"),
                        len(missing))
            await _notify_missing(record_id, missing)
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "missing_fields", "missing": missing})

        # 6. 差し込み（PII は log へ出さない・record_id のみ）
        docx_bytes = fill_template(TEMPLATE_PATH, build_fill_data(record))
        logger.info("[CONTRACT] generated record_id=%s bytes=%d",
                    emit(record_id, "record_id", "log", "operator"),
                    len(docx_bytes))

        # 7. アップロード → 添付＋ステータス更新（1 回の PUT）
        mime = ("application/vnd.openxmlformats-officedocument"
                ".wordprocessingml.document")
        file_key = await hub_kintone.upload_file(
            _APP, OUTPUT_FILENAME, docx_bytes, mime)
        await hub_kintone.update_record(_APP, record_id, {
            FIELD_ATTACHMENT: [{"fileKey": file_key}],
            FIELD_STATUS: COMPLETED_VALUE,
        })
        logger.info("[CONTRACT] attached record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
    except Exception as e:
        # 固定分類のみ（logger.exception 不使用・本文は emit 抑止）
        logger.error("[CONTRACT] error record_id=%s cls=%s: %s",
                     emit(record_id, "record_id", "log", "operator"),
                     type(e).__name__,
                     emit(str(e), "vendor_raw", "log", "operator"))
        return JSONResponse(status_code=500,
                            content={"error": "internal_error"})

    return JSONResponse(status_code=200,
                        content={"ok": True, "record_id": record_id})
