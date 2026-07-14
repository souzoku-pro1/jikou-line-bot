"""POST /bank/ingest — 通帳・残高証明PDFの受領→読解→App 35 転記（S6-1）

設計: 2026-07-07 S6-1 裁定
- 第1版スコープ: 残高証明・通帳見開き → App 35 財産行（**1口座=1行**）。
  取引明細の構造化・異常検知は本タスク外。**明細アプリは作らない**——
  読解JSON（口座断片）は財産行の原本に .json ファイルとして添付し痕跡を残す
- 入口は koseki/registry 型: ?token=（env BANK_INGEST_TOKEN・404偽装）・
  multipart・PDF必須・原本添付・case_hint/case_app_hint 受け
- 冪等・upsert は S4 の upsert 型: **冪等キー = 正規化した金融機関＋支店＋口座番号**
  （bank: プレフィックス・全半角/ハイフン/空白の正規化）で、upsert 条件は
  （案件レコードID・冪等キー）＝同一口座×案件の再送は残高・基準日の更新。
  案件の逆引きは同一冪等キーの既存財産行から行う
- App 35 転記: 財産種別=預貯金・特定情報は推奨書式
  「○○銀行 △△支店 普通預金 口座番号1234567」・名義=名義人・評価額=残高・
  評価方法=残高証明（**通帳由来は正本選択肢に「通帳」が無いため「その他」を使用し
  備考に「通帳記載の残高による」を明記**・2026-07-07 実装判断）・
  評価基準日=基準日_西暦（和暦原文は添付の読解JSONに保持）・データ源=OCR_残高証明・
  評価確定/有効/名義の不触保護（更新時）は S5 と同一
- 案件紐付け: case_hint → 逆引き → 不能は要確認キュー（S5 封筒・
  チャネル固有データのトップキー = bank_ingest）。確定は関所の
  RESOLVERS["bank_ingest"]（review_resolve 側）が行う
- 既存 /scan（通帳→App 27）経路は不触・仕分け回送対象への追加も本タスク外
"""

import hashlib
import json
import logging
import os
import re
import unicodedata

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from bank_reader import (
    overall_confidence,
    read_bank,
    reread_threshold,
    validate_reading,
)
from hub import kintone
from hub.redact import emit
from hub.service_auth import BodyCachingRoute, ingest_guard  # RV-04b dual-accept 結線

logger = logging.getLogger("bank_ingest")

router = APIRouter(route_class=BodyCachingRoute)

APP_ZAISAN = kintone.KintoneApp("App 財産", "APP_ZAISAN", "TOKEN_ZAISAN")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

UNIT = "相続一般"
DATA_SOURCE = "OCR_残高証明"  # App 35 データ源の実機選択肢（通帳も同値・裁定）


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def normalize_account_part(text: str) -> str:
    """口座キーの正規化: NFKC（全半角）・空白除去・ハイフン類/中点の除去"""
    s = unicodedata.normalize("NFKC", text or "")
    s = re.sub(r"[\s　]+", "", s)
    return re.sub(r"[-‐‑–—―−ｰー・]", "", s)


def account_key(account: dict) -> str:
    """冪等キー = 正規化した金融機関＋支店＋口座番号（案件は upsert 条件側で結合）"""
    return "bank:" + "|".join(normalize_account_part(str(account.get(k) or ""))
                              for k in ("金融機関名", "支店名", "口座番号"))


def _tokutei_joho(account: dict, doc_form: str) -> str:
    """特定情報の推奨書式（裁定の例:「○○銀行 △△支店 普通預金 口座番号1234567」）"""
    parts = []
    if account.get("金融機関名"):
        parts.append(str(account["金融機関名"]))
    if account.get("支店名"):
        parts.append(str(account["支店名"]))
    if account.get("預金種別"):
        parts.append(f"{account['預金種別']}預金")
    if account.get("口座番号"):
        parts.append(f"口座番号{account['口座番号']}")
    return " ".join(parts)


def _valuation_method(doc_form: str) -> tuple[str, str]:
    """(評価方法, 備考) — 通帳は正本選択肢に無いため「その他」＋備考で明示"""
    if doc_form == "残高証明":
        return "残高証明", ""
    return "その他", "通帳記載の残高による（評価方法の正式化は弁護士確認時）"


async def _attach(app: kintone.KintoneApp, filename: str, content: bytes,
                  mime: str) -> dict | None:
    try:
        key = await kintone.upload_file(app, filename, content, mime)
        return {"fileKey": key}
    except Exception as e:
        logger.info("[BANK_INGEST] 添付に失敗（処理続行） cls=%s: %s",
                    type(e).__name__, emit(str(e), "vendor_raw", "log", "operator"))
        return None


async def _file_needs_review(reason: str, detail: dict,
                             pdf_bytes: bytes, filename: str) -> str | None:
    """App 30 要確認キュー起票（S5 封筒・トップキー=bank_ingest）"""
    if not (APP_SHIPPING.app_id() and APP_SHIPPING.token()):
        logger.info("[BANK_INGEST] 要確認起票スキップ（APP_SHIPPING 未設定）")
        return None
    fields = {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": UNIT,
        "件名": f"通帳・残高証明の読解転記: {reason}",
        "エラー詳細": f"{reason}\n{json.dumps(detail, ensure_ascii=False)}"[:500],
        "チャネル固有データ": json.dumps({"bank_ingest": detail},
                                         ensure_ascii=False),
        "実行済み": "no",
    }
    attachment = await _attach(APP_SHIPPING, filename or "残高証明.pdf",
                               pdf_bytes, "application/pdf")
    if attachment:
        fields["成果物"] = [attachment]
    return str(await kintone.create_record(APP_SHIPPING, fields))


async def _resolve_case(case_hint: str | None, key: str) -> str:
    """案件解決: case_hint → 同一冪等キー（口座）の既存財産行からの逆引き"""
    if case_hint:
        return case_hint
    if not APP_ZAISAN.app_id():
        return ""
    rows = await kintone.search_records(
        APP_ZAISAN,
        f'冪等キー = "{key}" and 有効 in ("yes")',
        fields=["案件レコードID"])
    for row in rows:
        case_id = _v(row, "案件レコードID")
        if case_id:
            return case_id
    return ""


async def upsert_account_row(account: dict, doc_form: str, case_record_id: str,
                             *, case_app_id: str = "", pdf_bytes: bytes = b"",
                             filename: str = "") -> dict:
    """口座1件 → App 35 財産行の upsert（1口座=1行・S4 の upsert 型）。

    - upsert 条件 =（案件レコードID・冪等キー〔正規化口座〕・有効=yes）。
      再送は 評価額（残高）・評価基準日 の更新のみ＝評価確定/有効/名義は不触（S5 同一）
    - 読解JSON（口座断片）は原本に .json で添付（明細アプリは作らない・裁定）
    S5-2.5 の確定ハンドラ（案件紐付け不能の解決）からも呼ばれる。
    """
    key = account_key(account)
    method, biko = _valuation_method(doc_form)
    balance = account.get("残高")
    basis_seireki = str(account.get("基準日_西暦") or "")

    attachments = []
    if pdf_bytes:
        pdf = await _attach(APP_ZAISAN, filename or "残高証明.pdf",
                            pdf_bytes, "application/pdf")
        if pdf:
            attachments.append(pdf)
    reading_note = json.dumps({"書類形態": doc_form, "口座": account},
                              ensure_ascii=False).encode("utf-8")
    note = await _attach(APP_ZAISAN, "読解断片.json", reading_note,
                         "application/json")
    if note:
        attachments.append(note)

    existing = await kintone.search_records(
        APP_ZAISAN,
        f'案件レコードID = "{case_record_id}" and 冪等キー = "{key}"'
        f' and 有効 in ("yes")',
        fields=["$id", "原本"])
    if existing:
        zaisan_id = _v(existing[0], "$id")
        fields = {}
        if balance is not None:
            fields["評価額"] = str(balance)
        if basis_seireki:
            fields["評価基準日"] = basis_seireki
        keeps = [{"fileKey": f.get("fileKey")}
                 for f in ((existing[0].get("原本") or {}).get("value") or [])
                 if f.get("fileKey")]
        if attachments:
            fields["原本"] = keeps + attachments
        if fields:
            await kintone.update_record(APP_ZAISAN, zaisan_id, fields)
        return {"zaisan": "updated", "zaisan_record_id": zaisan_id}

    fields = {
        "ユニット種別": UNIT,
        "案件アプリID": case_app_id or os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
        "案件レコードID": case_record_id,
        "財産種別": "預貯金",
        "特定情報": _tokutei_joho(account, doc_form),
        "名義": str(account.get("名義人") or ""),
        "評価額": str(balance) if balance is not None else "",
        "評価方法": method,
        "評価基準日": basis_seireki,
        "データ源": DATA_SOURCE,
        "冪等キー": key,
        "評価確定": "no",
        "有効": "yes",
    }
    if biko:
        fields["備考"] = biko
    if attachments:
        fields["原本"] = attachments
    zaisan_id = str(await kintone.create_record(
        APP_ZAISAN, {k: v for k, v in fields.items() if v != ""}))
    return {"zaisan": "created", "zaisan_record_id": zaisan_id}


async def ingest_bank_pdf(pdf_bytes: bytes, filename: str, *,
                          case_hint: str | None = None,
                          case_app_hint: str | None = None,
                          drive_file_id: str | None = None) -> dict:
    """通帳・残高証明 PDF の読解→転記処理の中核。"""
    vision_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not vision_key:
        raise HTTPException(status_code=500,
                            detail="環境変数が未設定です: GOOGLE_VISION_API_KEY")

    fid = (drive_file_id or "").strip() or \
        f"sha256:{hashlib.sha256(pdf_bytes).hexdigest()}"

    if not (APP_ZAISAN.app_id() and APP_ZAISAN.token()):
        # 転記先が無ければ受領だけしても意味がないため明示エラー（安全側）
        raise HTTPException(status_code=503,
                            detail="APP_ZAISAN / TOKEN_ZAISAN が未設定です")

    def _ocr(pdf: bytes, key: str) -> str:
        from main import _ocr_pdf_bytes  # 実行時 import（循環 import 回避）
        return _ocr_pdf_bytes(pdf, key)

    try:
        ocr_text = _ocr(pdf_bytes, vision_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRエラー: {e}")

    try:
        reading = await read_bank(ocr_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"読解エラー: {e}")

    errors = validate_reading(reading)
    overall = overall_confidence(reading)
    if errors or overall < reread_threshold():
        reason = f"スキーマ逸脱 {len(errors)} 件" if errors else \
            f"全体確信度 {overall} < {reread_threshold()}"
        review_id = await _file_needs_review(
            reason, {"検証エラー": errors, "全体確信度": overall,
                     "Drive_fileId": fid}, pdf_bytes, filename)
        return {"status": "needs_review", "reason": reason,
                "review_record_id": review_id, "全体確信度": overall}

    doc_form = str(reading.get("書類形態") or "不明")
    results = []
    for i, account in enumerate(reading.get("口座") or []):
        key = account_key(account)
        result: dict = {"index": i, "冪等キー": key,
                        "金融機関名": account.get("金融機関名")}
        case_id = await _resolve_case(case_hint, key)
        if not case_id:
            detail = {"理由": "案件紐付け不能", "書類形態": doc_form,
                      "冪等キー": key, "口座": account, "Drive_fileId": fid}
            review_id = await _file_needs_review("案件紐付け不能", detail,
                                                 pdf_bytes, filename)
            result["zaisan"] = "needs_review"
            result["review_record_id"] = review_id
        else:
            result.update(await upsert_account_row(
                account, doc_form, case_id,
                case_app_id=case_app_hint or "",
                pdf_bytes=pdf_bytes, filename=filename))
            result["case_record_id"] = case_id
        results.append(result)

    logger.info("[BANK_INGEST] done file=%s 口座=%s",
                emit(filename, "freetext", "log", "operator"),
                emit(len(results), "count", "log", "operator"))
    return {"status": "ok", "書類形態": doc_form, "全体確信度": overall,
            "results": results, "ocr_chars": len(ocr_text)}


@router.post("/bank/ingest")
async def bank_ingest(_auth: None = Depends(ingest_guard("BANK_INGEST_TOKEN")),
                      # file は意図的に optional: File(...) だと探信に 422 が
                      # 返り 404 偽装より先に存在が漏れる（koseki_ingest と同じ）
                      file: UploadFile | None = File(default=None),
                      case_hint: str | None = Form(default=None),
                      case_app_hint: str | None = Form(default=None),
                      drive_file_id: str | None = Form(default=None)):
    """通帳・残高証明 PDF を受領し、読解→App 財産（1口座=1行）へ転記する。

    認証は前段の ingest_guard（RV-04b dual-accept）が担う。既存 /scan（通帳→App 27）は不変で並存。
    """
    if file is None or not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    return await ingest_bank_pdf(
        await file.read(), file.filename,
        case_hint=case_hint, case_app_hint=case_app_hint,
        drive_file_id=drive_file_id)
