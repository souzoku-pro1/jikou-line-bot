"""POST /valuation/ingest — 評価証明・課税明細PDFの受領→読解→App 25/35 転記（S4-M2）

設計: 2026-07-07 S4 近代化調査＋裁定
- 入口は koseki/registry 型: ?token=（env VALUATION_INGEST_TOKEN・未設定/不一致は
  404 の存在しないフリ）・multipart・PDF必須・原本添付・case_hint/case_app_hint 受け
- **冪等は S4 の upsert 型を継承**（skip 型は採らない＝再送は年度更新として上書き）。
  冪等キーの格納表記は sha256: 付きに統一し、照合（同一PDF判定→原本の重複添付防止）は
  旧素hex も許容する
- 25 名寄せは registry 部品（normalize_addr / _find_fudosan）を流用——正規化完全一致
  のみ update・不一致は create・**曖昧/複数/紐付け不能は要確認キュー**
  （S5 封筒・チャネル固有データのトップキー = valuation_ingest）
- App 35 upsert は S4 の資産を温存: upsert キー（案件レコードID・元レコードID・
  評価基準日）・**評価確定/有効の不触保護**・賦課期日（年度の1月1日）・
  **名義は新規作成時のみ**書き既存行では不触（登記由来が正の序列・裁定）
- 複数物件: 読解 properties 配列を**物件ごと**に名寄せ→upsert
- 既存 /ocr/fixed-asset・units/souzoku/zaisan_sync は**不変で並存**
  （同一PDFが両経路から入っても upsert 冪等で二重行はできない）
- env 縮退: APP_ZAISAN / KINTONE_FUDOSAN_APP_ID / APP_SHIPPING 未設定は
  各処理を独立にスキップ（既存の型どおり）
"""

import hashlib
import json
import logging
import os

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from hub import kintone
from hub.redact import emit
from hub.service_auth import BodyCachingRoute, ingest_guard  # RV-04b dual-accept 結線
from registry_ingest import KIND_TO_APP25, _find_fudosan
from valuation_reader import (
    overall_confidence,
    read_valuation,
    reread_threshold,
    validate_reading,
)

logger = logging.getLogger("valuation_ingest")

router = APIRouter(route_class=BodyCachingRoute)

APP_ZAISAN = kintone.KintoneApp("App 財産", "APP_ZAISAN", "TOKEN_ZAISAN")
APP_FUDOSAN = kintone.KintoneApp(
    "App 25 (不動産)", "KINTONE_FUDOSAN_APP_ID", "KINTONE_FUDOSAN_API_TOKEN")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

UNIT = "相続一般"
# App 35 データ源の実機選択肢に合わせる（評価証明・課税明細とも OCR_課税明細）
DATA_SOURCE = "OCR_課税明細"

# 読解部品の種別（土地/家屋/不明）→ registry 名寄せ部品の種別語彙（土地/建物/不明）
_KIND_TO_REGISTRY = {"土地": "土地", "家屋": "建物", "不明": "不明"}
# → App 35 財産種別
_KIND_TO_ZAISAN = {"土地": "不動産_土地", "家屋": "不動産_建物", "不明": "その他"}


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _kijunbi(year) -> str:
    """評価基準日 = 固定資産税の賦課期日（当該年度の1月1日・S4 温存）"""
    return f"{int(year)}-01-01" if year else ""


def _bare_key(key: str) -> str:
    """冪等キーの照合用正規化（sha256: 付き新表記と S4 旧素hex の互換・裁定）"""
    key = (key or "").strip()
    return key[len("sha256:"):] if key.startswith("sha256:") else key


def same_pdf_key(stored: str, fid: str) -> bool:
    """同一PDF判定（原本の重複添付防止に使う）。表記差（sha256:/素hex）を吸収"""
    return bool(stored) and _bare_key(stored) == _bare_key(fid)


def _registry_shaped(prop: dict) -> dict:
    """読解物件（土地/家屋語彙）→ registry 名寄せ部品の入力形（土地/建物語彙）"""
    return {"種別": _KIND_TO_REGISTRY.get(str(prop.get("種別") or "不明"), "不明"),
            "所在": str(prop.get("所在") or ""),
            "地番": str(prop.get("地番") or ""),
            "家屋番号": str(prop.get("家屋番号") or "")}


def _fudosan_fields(prop: dict, year, owner_name: str) -> dict:
    """物件 → App 25 転記フィールド（評価額・年度は S4 の上書き対象2フィールドを継承）"""
    rp = _registry_shaped(prop)
    fields = {
        "種別": KIND_TO_APP25.get(rp["種別"], "その他"),
        "所在": rp["所在"],
        "地番": rp["地番"],
        "部屋番号": rp["家屋番号"],
    }
    value = prop.get("評価額")
    if value is not None:
        fields["固定資産税評価額"] = str(value)
    if year:
        fields["固定資産税評価年度"] = str(int(year))
    if owner_name:
        fields["状況"] = owner_name
    return {k: v for k, v in fields.items() if v != ""}


def _tokutei_joho(prop: dict) -> str:
    """App 35 特定情報（02 §2.3 の推奨書式・存在項目のみ）"""
    parts = []
    for label, code in (("所在", "所在"), ("地番", "地番"),
                        ("家屋番号", "家屋番号")):
        value = str(prop.get(code) or "")
        if value:
            parts.append(f"{label} {value}")
    return " / ".join(parts)


async def _resolve_case(case_hint: str | None, fudosan_id: str) -> str:
    """案件解決: case_hint → 過去の財産行からの逆引き（S4 と同じ流儀）"""
    if case_hint:
        return case_hint
    if not (fudosan_id and APP_ZAISAN.app_id()):
        return ""
    rows = await kintone.search_records(
        APP_ZAISAN,
        f'元アプリID = "{APP_FUDOSAN.app_id()}" and 元レコードID = "{fudosan_id}"'
        f' and 有効 in ("yes")',
        fields=["案件レコードID"])
    for row in rows:
        case_id = _v(row, "案件レコードID")
        if case_id:
            return case_id
    return ""


async def _attach(app: kintone.KintoneApp, filename: str,
                  pdf_bytes: bytes) -> list | None:
    try:
        key = await kintone.upload_file(
            app, filename or "課税明細.pdf", pdf_bytes, "application/pdf")
        return [{"fileKey": key}]
    except Exception as e:
        logger.info("[VALUATION_INGEST] 原本添付に失敗（処理続行） cls=%s: %s",
                    type(e).__name__, emit(str(e), "vendor_raw", "log", "operator"))
        return None


async def _file_needs_review(reason: str, detail: dict,
                             pdf_bytes: bytes, filename: str) -> str | None:
    """App 30 要確認キュー起票（S5 封筒・トップキー=valuation_ingest）。
    env 未設定はスキップ縮退"""
    if not (APP_SHIPPING.app_id() and APP_SHIPPING.token()):
        logger.info("[VALUATION_INGEST] 要確認起票スキップ（APP_SHIPPING 未設定）")
        return None
    fields = {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": UNIT,
        "件名": f"評価証明・課税明細の読解転記: {reason}",
        "エラー詳細": f"{reason}\n{json.dumps(detail, ensure_ascii=False)}"[:500],
        "チャネル固有データ": json.dumps({"valuation_ingest": detail},
                                         ensure_ascii=False),
        "実行済み": "no",
    }
    attachment = await _attach(APP_SHIPPING, filename, pdf_bytes)
    if attachment:
        fields["成果物"] = attachment
    return str(await kintone.create_record(APP_SHIPPING, fields))


async def upsert_zaisan_from_fudosan(fudosan_id: str, case_record_id: str,
                                     idempotency_key: str,
                                     *, case_app_id: str = "",
                                     owner_name: str = "",
                                     pdf_bytes: bytes = b"",
                                     filename: str = "") -> dict:
    """App 25 のレコードから財産行を upsert する（S4 の資産温存）。

    - upsert キー =（案件レコードID・元レコードID・評価基準日）——同一年度再送は
      上書き・年度違いは別行
    - 既存行の更新では 評価確定・有効・名義 を触らない（不触保護＋登記由来が正）。
      同一PDF（冪等キー照合・素hex互換）なら原本の重複添付もしない
    - 新規作成時のみ 評価確定=no・有効=yes・名義=所有者名 を設定
    S5-2.5 の確定ハンドラ（案件紐付け不能の解決）からも呼ばれる。
    """
    fudosan = await kintone.get_record(APP_FUDOSAN, fudosan_id)
    year = _v(fudosan, "固定資産税評価年度")
    kijunbi = _kijunbi(year)
    prop = {"所在": _v(fudosan, "所在"), "地番": _v(fudosan, "地番"),
            "家屋番号": _v(fudosan, "部屋番号")}
    kind25 = _v(fudosan, "種別")
    zaisan_kind = {"土地": "不動産_土地", "建物": "不動産_建物",
                   "マンション(区分所有)": "不動産_区分建物"}.get(kind25, "その他")

    fields = {
        "ユニット種別": UNIT,
        "案件アプリID": case_app_id or os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
        "案件レコードID": case_record_id,
        "財産種別": zaisan_kind,
        "特定情報": _tokutei_joho(prop),
        "評価額": _v(fudosan, "固定資産税評価額"),
        "評価方法": "固定資産税評価額",
        "評価基準日": kijunbi,
        "データ源": DATA_SOURCE,
        "元アプリID": APP_FUDOSAN.app_id(),
        "元レコードID": fudosan_id,
        "冪等キー": idempotency_key,
    }
    fields = {k: v for k, v in fields.items() if v != ""}

    conds = [f'案件レコードID = "{case_record_id}"',
             f'元レコードID = "{fudosan_id}"']
    if kijunbi:
        conds.append(f'評価基準日 = "{kijunbi}"')
    existing = await kintone.search_records(
        APP_ZAISAN, " and ".join(conds), fields=["$id", "冪等キー", "原本"])
    if existing:
        zaisan_id = _v(existing[0], "$id")
        # 不触保護（S4 温存）: 評価確定・有効・名義 は更新フィールドに含めない
        if pdf_bytes and not same_pdf_key(_v(existing[0], "冪等キー"),
                                          idempotency_key):
            keeps = [{"fileKey": f.get("fileKey")}
                     for f in ((existing[0].get("原本") or {}).get("value") or [])
                     if f.get("fileKey")]
            attachment = await _attach(APP_ZAISAN, filename, pdf_bytes)
            if attachment:
                fields["原本"] = keeps + attachment
        await kintone.update_record(APP_ZAISAN, zaisan_id, fields)
        return {"zaisan": "updated", "zaisan_record_id": zaisan_id}

    # 新規作成時のみ（不触保護の対偶）
    fields["評価確定"] = "no"
    fields["有効"] = "yes"
    if owner_name:
        fields["名義"] = owner_name
    if pdf_bytes:
        attachment = await _attach(APP_ZAISAN, filename, pdf_bytes)
        if attachment:
            fields["原本"] = attachment
    zaisan_id = str(await kintone.create_record(APP_ZAISAN, fields))
    return {"zaisan": "created", "zaisan_record_id": zaisan_id}


async def ingest_valuation_pdf(pdf_bytes: bytes, filename: str, *,
                               case_hint: str | None = None,
                               case_app_hint: str | None = None,
                               drive_file_id: str | None = None) -> dict:
    """評価証明・課税明細 PDF の読解→転記処理の中核。

    /valuation/ingest エンドポイントと、仕分けからの回送（S4-M3 予定）が共用する。
    """
    vision_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not vision_key:
        raise HTTPException(status_code=500,
                            detail="環境変数が未設定です: GOOGLE_VISION_API_KEY")

    # 冪等キーの格納表記は sha256: 付きに統一（裁定）。drive_file_id はそのまま
    fid = (drive_file_id or "").strip() or \
        f"sha256:{hashlib.sha256(pdf_bytes).hexdigest()}"

    zaisan_enabled = bool(APP_ZAISAN.app_id() and APP_ZAISAN.token())
    fudosan_enabled = bool(APP_FUDOSAN.app_id() and APP_FUDOSAN.token())
    if not zaisan_enabled:
        logger.info("[VALUATION_INGEST] APP_ZAISAN 未設定: 財産行転記をスキップ")
    if not fudosan_enabled:
        logger.info("[VALUATION_INGEST] KINTONE_FUDOSAN_APP_ID 未設定: 不動産25転記をスキップ")

    def _ocr(pdf: bytes, key: str) -> str:
        from main import _ocr_pdf_bytes  # 実行時 import（循環 import 回避）
        return _ocr_pdf_bytes(pdf, key)

    try:
        ocr_text = _ocr(pdf_bytes, vision_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRエラー: {e}")

    try:
        reading = await read_valuation(ocr_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"読解エラー: {e}")

    # 品質ゲート: スキーマ逸脱・低確信度は転記せず要確認へ（安全側・S5 と同型）
    errors = validate_reading(reading)
    overall = overall_confidence(reading)
    if errors or overall < reread_threshold():
        reason = f"スキーマ逸脱 {len(errors)} 件" if errors else \
            f"全体確信度 {overall} < {reread_threshold()}"
        review_id = await _file_needs_review(
            reason, {"検証エラー": errors, "全体確信度": overall, "冪等キー": fid},
            pdf_bytes, filename)
        return {"status": "needs_review", "reason": reason,
                "review_record_id": review_id, "全体確信度": overall}

    year = reading.get("年度")
    owner_name = str(reading.get("所有者名") or "")
    results = []
    for i, prop in enumerate(reading.get("物件") or []):
        result: dict = {"index": i, "種別": prop.get("種別"),
                        "所在": prop.get("所在")}
        fudosan_id = ""
        if fudosan_enabled:
            state, exact, partial = await _find_fudosan(_registry_shaped(prop))
            if state == "matched":
                fudosan_id = _v(exact[0], "$id")
                # S4 温存: 既存25には評価額・年度（＋状況）の上書きのみ
                update_fields = {}
                if prop.get("評価額") is not None:
                    update_fields["固定資産税評価額"] = str(prop["評価額"])
                if year:
                    update_fields["固定資産税評価年度"] = str(int(year))
                if update_fields:
                    await kintone.update_record(APP_FUDOSAN, fudosan_id,
                                                update_fields)
                result["fudosan"] = "updated"
            elif state in ("none", "no_key"):
                fudosan_id = str(await kintone.create_record(
                    APP_FUDOSAN, _fudosan_fields(prop, year, owner_name)))
                result["fudosan"] = "created"
            else:  # ambiguous: マージも先頭採用もしない（S5 と同じ裁定・安全側）
                detail = {"理由": "名寄せの曖昧一致", "物件": i,
                          "所在": prop.get("所在"), "冪等キー": fid}
                review_id = await _file_needs_review(
                    "名寄せの曖昧一致（マージ・先頭採用せず）", detail,
                    pdf_bytes, filename)
                result["fudosan"] = "needs_review"
                result["review_record_id"] = review_id
                results.append(result)
                continue

        if zaisan_enabled:
            case_id = await _resolve_case(case_hint, fudosan_id)
            if not case_id:
                detail = {"理由": "案件紐付け不能", "物件": i,
                          "所在": prop.get("所在"),
                          "不動産レコードID": fudosan_id, "冪等キー": fid}
                review_id = await _file_needs_review("案件紐付け不能", detail,
                                                     pdf_bytes, filename)
                result["zaisan"] = "needs_review"
                result["review_record_id"] = review_id
            elif fudosan_id:
                result.update(await upsert_zaisan_from_fudosan(
                    fudosan_id, case_id, fid,
                    case_app_id=case_app_hint or "",
                    owner_name=owner_name,
                    pdf_bytes=pdf_bytes, filename=filename))
                result["case_record_id"] = case_id
            else:
                # 25 なし（env 縮退）: 読解値から直接 財産行を作る縮退
                fields = {
                    "ユニット種別": UNIT,
                    "案件アプリID": case_app_hint or
                    os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
                    "案件レコードID": case_id,
                    "財産種別": _KIND_TO_ZAISAN.get(
                        str(prop.get("種別") or "不明"), "その他"),
                    "特定情報": _tokutei_joho(prop),
                    "評価額": str(prop["評価額"])
                    if prop.get("評価額") is not None else "",
                    "評価方法": "固定資産税評価額",
                    "評価基準日": _kijunbi(year),
                    "データ源": DATA_SOURCE,
                    "冪等キー": fid,
                    "評価確定": "no",
                    "有効": "yes",
                }
                if owner_name:
                    fields["名義"] = owner_name
                attachment = await _attach(APP_ZAISAN, filename, pdf_bytes)
                if attachment:
                    fields["原本"] = attachment
                zaisan_id = str(await kintone.create_record(
                    APP_ZAISAN, {k: v for k, v in fields.items() if v != ""}))
                result.update({"zaisan": "created", "zaisan_record_id": zaisan_id,
                               "case_record_id": case_id})
        results.append(result)

    logger.info("[VALUATION_INGEST] done file=%s 物件=%s",
                emit(filename, "freetext", "log", "operator"),
                emit(len(results), "count", "log", "operator"))
    return {"status": "ok", "全体確信度": overall, "書類種別": reading.get("書類種別"),
            "results": results, "ocr_chars": len(ocr_text)}


@router.post("/valuation/ingest")
async def valuation_ingest(_auth: None = Depends(ingest_guard("VALUATION_INGEST_TOKEN")),
                           # file は意図的に optional: File(...) だと探信に 422 が
                           # 返り 404 偽装より先に存在が漏れる（koseki_ingest と同じ）
                           file: UploadFile | None = File(default=None),
                           case_hint: str | None = Form(default=None),
                           case_app_hint: str | None = Form(default=None),
                           drive_file_id: str | None = Form(default=None)):
    """評価証明・課税明細 PDF を受領し、読解→不動産25・App 財産へ転記する。

    認証は前段の ingest_guard（RV-04b dual-accept）が担う。既存 /ocr/fixed-asset は不変で並存
    （同一PDFが両経路から入っても upsert 冪等で二重行はできない）。
    """
    if file is None or not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    return await ingest_valuation_pdf(
        await file.read(), file.filename,
        case_hint=case_hint, case_app_hint=case_app_hint,
        drive_file_id=drive_file_id)
