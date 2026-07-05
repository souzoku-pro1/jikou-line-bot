"""POST /koseki/ingest — 戸籍PDFの受領→Vision OCR→戸籍（読解）App 33 登録（R2）

設計: docs/koseki-ocr/01 §1（受領〜保存）・02 §1（App 44=実機33）・07 §2 R2

2026-07-05 裁定（07 §2 R2 の裁定注記を参照）:
- T4-1（M5 /scan/v2・DOC_TYPE_CONFIG）が未実装のため、受領口は本独立
  エンドポイントで暫定分離する。既存 /scan（戸籍謄本フォルダ→App 27）は
  不変で併存（01 §3 の併存期）。入口の振り分けは watcher 側サブフォルダで行う
- R2 の範囲は「OCR 生テキストの保存と登録」まで。Claude 読解（様式判定・
  構造化・confidence・読解JSON 生成）は R3。OCR 生テキストは 読解JSON
  フィールドに {"ocr_text": "..."} の形で仮置きし、R3 の AI 読解が置き換える
  （読解状態=未読解 が「AI 未読解」の印）
- 編製日・消除日等の読解由来フィールドは書かない（R3 スコープ。実機 App 33 は
  文字列型＝和暦原文保持が正・2026-07-05 検収裁定）

挙動:
- token 認証: ?token=（env KOSEKI_INGEST_TOKEN）。不一致・env 未設定は
  404 の「存在しないフリ」（既存 Webhook と同じ流儀）
- APP_KOSEKI_BOOK / TOKEN_KOSEKI_BOOK 未設定は 503 の明示エラー（安全側の受付拒否）
- 冪等: Drive_fileId（省略時は sha256:<PDF ハッシュ>）一致の既存レコードは skip 応答
- ページ画像: PyMuPDF で PNG 化して添付（確認 UI の並置表示用・02 §1）。
  PyMuPDF が利用不能な環境では画像なしで登録を続行する（ログ警告のみ）
- OCR が空でも登録する（粗くても人の確認に回す方が速い・01 §2 の方針）
"""

import hashlib
import json
import os

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from hub import kintone
from hub.webhook_auth import verify_token

router = APIRouter()

APP_KOSEKI_BOOK = kintone.KintoneApp(
    "App 33 (戸籍読解)", "APP_KOSEKI_BOOK", "TOKEN_KOSEKI_BOOK")


def _ocr_pdf(pdf_bytes: bytes, api_key: str) -> str:
    """Vision files:annotate による OCR（既存 /ocr/fixed-asset と同一実装を共用。
    channels/scan_intake への移設は T4 系で実施予定・architecture/08 §5）"""
    from main import _ocr_pdf_bytes  # 実行時 import（循環 import 回避）
    return _ocr_pdf_bytes(pdf_bytes, api_key)


def _render_page_images(pdf_bytes: bytes) -> list[bytes]:
    """PDF をページ単位で PNG 化する（確認 UI の並置表示用・01 §1-1）。
    PyMuPDF 不在なら空リスト（登録は画像なしで続行する安全側）"""
    try:
        import fitz  # PyMuPDF
    except ImportError:
        print("[KOSEKI_INGEST] PyMuPDF が利用できないためページ画像なしで続行")
        return []
    images = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            images.append(page.get_pixmap(dpi=150).tobytes("png"))
    return images


@router.post("/koseki/ingest")
async def koseki_ingest(token: str = "",
                        # file は意図的に optional: 必須（File(...)）にすると
                        # ファイル無しの探信に FastAPI が 422 を返してしまい、
                        # token 検証（404 の存在しないフリ）より先に
                        # エンドポイントの存在が漏れる（2026-07-05 実機確認）
                        file: UploadFile | None = File(default=None),
                        case_hint: str | None = Form(default=None),
                        case_app_hint: str | None = Form(default=None),
                        drive_file_id: str | None = Form(default=None)):
    """戸籍 PDF（複数ページ可）を受領し App 33 に 1 レコード登録する。

    case_hint: 案件レコードID（省略可）。case_app_hint: 案件アプリID（省略可）。
    drive_file_id: 冪等キー（省略時は PDF の SHA-256 から生成）。
    """
    if not verify_token(token, "KOSEKI_INGEST_TOKEN"):
        raise HTTPException(status_code=404, detail="Not Found")

    if not (APP_KOSEKI_BOOK.app_id() and APP_KOSEKI_BOOK.token()):
        raise HTTPException(
            status_code=503,
            detail="APP_KOSEKI_BOOK / TOKEN_KOSEKI_BOOK が未設定です"
                   "（App 33 の環境変数登録後に有効になります）")

    vision_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not vision_key:
        raise HTTPException(status_code=500,
                            detail="環境変数が未設定です: GOOGLE_VISION_API_KEY")

    if file is None or not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    pdf_bytes = await file.read()
    fid = (drive_file_id or "").strip() or \
        f"sha256:{hashlib.sha256(pdf_bytes).hexdigest()}"

    # 冪等: 既処理 PDF は skip（M5 の Drive_fileId 方式・02 §1）
    try:
        existing = await kintone.search_records(
            APP_KOSEKI_BOOK, f'Drive_fileId = "{fid}"', fields=["$id"])
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintone検索エラー: {e}")
    if existing:
        return {"status": "skip", "reason": "既処理（Drive_fileId 一致）",
                "kintone_record_id": str(existing[0]["$id"]["value"])}

    try:
        ocr_text = _ocr_pdf(pdf_bytes, vision_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRエラー: {e}")

    page_images = _render_page_images(pdf_bytes)

    try:
        fields = {
            "原本PDF": [{"fileKey": await kintone.upload_file(
                APP_KOSEKI_BOOK, file.filename, pdf_bytes, "application/pdf")}],
            "Drive_fileId": fid,
            # R2 は OCR 生テキストの仮置きまで。R3 の AI 読解が置き換える（裁定）
            "読解JSON": json.dumps({"ocr_text": ocr_text}, ensure_ascii=False),
            "読解状態": "未読解",
        }
        if page_images:
            keys = []
            for i, png in enumerate(page_images, start=1):
                keys.append({"fileKey": await kintone.upload_file(
                    APP_KOSEKI_BOOK, f"page-{i:03d}.png", png, "image/png")})
            fields["ページ画像"] = keys
        if case_hint:
            fields["案件レコードID"] = case_hint
        if case_app_hint:
            fields["案件アプリID"] = case_app_hint
        record_id = str(await kintone.create_record(APP_KOSEKI_BOOK, fields))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintone登録エラー: {e}")

    # R3（2026-07-05 裁定・A案）: 登録成功後に同期で構造化読解を試みる。
    # 読解の失敗・Claude 全断・例外は本応答を壊さない（レコードは 未読解 の
    # まま残り、koseki_reader.process_unread_records で後日回収できる）。
    # 応答形は従来どおり（読解結果はレコード側に反映される）
    try:
        from koseki_reader import process_record
        reading = await process_record(record_id)
        print(f"[KOSEKI_INGEST] 読解結果 record={record_id}: {reading}")
    except Exception as e:
        print(f"[KOSEKI_INGEST] 読解に失敗（未読解のまま・核関数で回収可能）"
              f" record={record_id}: {e}")

    return {"status": "ok", "kintone_record_id": record_id,
            "page_images": len(page_images), "ocr_chars": len(ocr_text)}
