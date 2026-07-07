"""POST /registry/ingest — 登記事項証明PDFの受領→OCR→読解→App 25/35 転記（S5-2）

設計: 2026-07-06 S5 設計調査＋裁定（07-07 実装裁定含む）
- 入口は koseki_ingest と同型: ?token=（env REGISTRY_INGEST_TOKEN・未設定/不一致は
  404 の存在しないフリ）・multipart・PDF必須・drive_file_id/sha256 冪等・原本添付。
  仕分けからの自動回送は S5-3（第2版）で本タスク外
- 流れ: Vision OCR → registry_reader（S5-1）読解 → validate/確信度ゲート → 転記
- 名寄せ（不動産25）: 所在＋地番/家屋番号を正規化して**完全一致のみ update**・
  不一致は create・**曖昧一致/複数一致はマージも先頭採用もせず** App 30 要確認
  キュー起票（登記は所有者情報のため S4 の「先頭採用」より安全側に倒す・裁定）
- 種別の写像は本転記層に持つ（S5-1 読解部品は変更しない・裁定）:
  土地→土地・建物→建物・区分建物→マンション(区分所有)・不明→その他
  （実機 App 25 の選択肢 2026-07-07 確認値）
- 乙区は 担保抵当権（有/無）＋担保内容（テキスト）へ転記
  （担保内容フィールドは 2026-07-07 実機追加をフォーム設計APIで確認済み。
  参考実装 registry_to_kintone.py の既知不整合〔存在しないフィールドへの書込・
  種別選択肢〕は本モジュールで解消）
- App 35: 1レコード=1財産を維持。持分・所有者詳細は App 25 が受け皿、
  App 35 の 名義 は表示文字列（例「熊澤正広（持分2分の1）外1名」）
- 既存レコードとの整合: 評価証明由来（S4）の同一物件行があれば**追記**
  （名義・特定情報・原本の追加のみ。評価額・評価確定・データ源・有効は
  上書きしない＝弁護士の確定を守る既存原則）
- env 未設定の縮退: APP_ZAISAN → App 35 転記と冪等チェックをスキップ・
  KINTONE_FUDOSAN_APP_ID → App 25 転記をスキップ・APP_SHIPPING → 要確認
  キュー起票をスキップ（いずれもログのみ・既存の型どおり）
- 既存アプリのフィールドはコード側から一切変更しない・GAS は本タスクでは触らない
"""

import hashlib
import json
import os
import re
import unicodedata

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from hub import kintone
from hub.webhook_auth import verify_token
from registry_reader import (
    overall_confidence,
    read_registry,
    reread_threshold,
    validate_reading,
)

router = APIRouter()

APP_ZAISAN = kintone.KintoneApp("App 財産", "APP_ZAISAN", "TOKEN_ZAISAN")
APP_FUDOSAN = kintone.KintoneApp(
    "App 25 (不動産)", "KINTONE_FUDOSAN_APP_ID", "KINTONE_FUDOSAN_API_TOKEN")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

UNIT = "相続一般"
DATA_SOURCE = "OCR_登記事項証明"

# 読解部品の種別 → 実機 App 25「種別」選択肢（2026-07-07 実機確認値・裁定の写像）
KIND_TO_APP25 = {"土地": "土地", "建物": "建物",
                 "区分建物": "マンション(区分所有)", "不明": "その他"}
# 読解部品の種別 → App 35「財産種別」
KIND_TO_ZAISAN = {"土地": "不動産_土地", "建物": "不動産_建物",
                  "区分建物": "不動産_区分建物", "不明": "その他"}

_HYPHENS = "－‐‑–—―−ｰー"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def normalize_addr(text: str) -> str:
    """所在・地番の表記揺れ正規化（名寄せは正規化後の完全一致のみ・裁定）。
    全角半角（NFKC）・空白除去・ハイフン類の統一・「番地」→「番」・
    算用数字の丁目→漢数字（main._CHOME_KANJI を流用）"""
    if not text:
        return ""
    s = unicodedata.normalize("NFKC", text)
    s = re.sub(r"[\s　]+", "", s)
    for h in _HYPHENS:
        s = s.replace(h, "-")
    s = s.replace("番地", "番")
    from main import _CHOME_KANJI  # 実行時 import（循環 import 回避・既存部品の流用）

    def kanji_chome(m):
        n = int(m.group(1))
        return f"{_CHOME_KANJI.get(n, m.group(1))}丁目"
    return re.sub(r"(\d+)丁目", kanji_chome, s)


def _ocr_pdf(pdf_bytes: bytes, api_key: str) -> str:
    """Vision files:annotate（既存 /ocr/fixed-asset と同一実装を共用）"""
    from main import _ocr_pdf_bytes  # 実行時 import（循環 import 回避）
    return _ocr_pdf_bytes(pdf_bytes, api_key)


def _number_or_none(raw: str) -> str | None:
    """「123.45㎡」等の原文から数値部分を取り出す（NUMBER フィールド用・不可なら None）"""
    m = re.search(r"[\d,]+(?:\.\d+)?", unicodedata.normalize("NFKC", raw or ""))
    return m.group(0).replace(",", "") if m else None


def _floor_areas(floor_text: str) -> dict[str, str]:
    """「1階 58.50㎡ 2階 62.60㎡」→ {"1": "58.50", ...}（registry_to_kintone の流用）"""
    result = {}
    for m in re.finditer(r"(\d+)階(?:部分)?\s*([\d.]+)",
                         unicodedata.normalize("NFKC", floor_text or "")):
        result[m.group(1)] = m.group(2)
    return result


def _floor_count(structure: str) -> str:
    m = re.search(r"(\d+)階建", unicodedata.normalize("NFKC", structure or ""))
    return m.group(1) if m else ""


def owners_display(owners: list[dict]) -> str:
    """App 35 名義の表示文字列（1レコード=1財産を維持・裁定）。
    例: 熊澤正広（持分2分の1）外1名 ／ 単独所有は氏名のみ"""
    if not owners:
        return ""
    first = owners[0]
    name = str(first.get("氏名") or "")
    share = str(first.get("持分") or "")
    label = f"{name}（持分{share}）" if share else name
    if len(owners) > 1:
        label += f"外{len(owners) - 1}名"
    return label


def _match_key(prop: dict) -> str:
    """名寄せキー: 土地=地番・建物/区分建物=家屋番号（無ければ地番）"""
    if prop.get("種別") == "土地":
        return str(prop.get("地番") or "")
    return str(prop.get("家屋番号") or prop.get("地番") or "")


async def _find_fudosan(prop: dict) -> tuple[str, list[dict], list[dict]]:
    """App 25 の候補取得と正規化突合。
    Returns: (状態, 完全一致リスト, 部分一致リスト)。状態 = matched/none/ambiguous/no_key"""
    key = _match_key(prop)
    digits = re.search(r"\d+", unicodedata.normalize("NFKC", key))
    if not digits:
        return "no_key", [], []
    d = digits.group(0)
    candidates = await kintone.search_records(
        APP_FUDOSAN,
        f'地番 like "{d}" or 部屋番号 like "{d}"',
        fields=["$id", "所在", "地番", "部屋番号", "種別"])

    kind25 = KIND_TO_APP25.get(str(prop.get("種別") or "不明"), "その他")
    n_loc, n_key = normalize_addr(str(prop.get("所在") or "")), normalize_addr(key)
    exact, partial = [], []
    for c in candidates:
        if _v(c, "種別") != kind25:
            continue
        c_loc = normalize_addr(_v(c, "所在"))
        c_key_hits = n_key in (normalize_addr(_v(c, "地番")),
                               normalize_addr(_v(c, "部屋番号")))
        if c_loc == n_loc and c_key_hits:
            exact.append(c)
        elif c_loc == n_loc or c_key_hits:
            partial.append(c)
    if len(exact) == 1:
        return "matched", exact, partial
    if len(exact) > 1:
        return "ambiguous", exact, partial
    if partial:
        return "ambiguous", exact, partial  # 曖昧一致はマージせず要確認（裁定）
    return "none", [], []


def _fudosan_fields(prop: dict) -> dict:
    """物件（日本語キー登記JSON）→ App 25 転記フィールド（実在フィールドのみ・
    registry_to_kintone.py の対応表を継承し既知不整合を解消）"""
    kouku = prop.get("甲区") or {}
    owners = kouku.get("所有者") or []
    otsuku = prop.get("乙区") or {}
    fields = {
        "種別": KIND_TO_APP25.get(str(prop.get("種別") or "不明"), "その他"),
        "所在": str(prop.get("所在") or ""),
        "地番": str(prop.get("地番") or ""),
        "地目": str(prop.get("地目") or ""),
        "部屋番号": str(prop.get("家屋番号") or ""),
        "建物名": str(prop.get("種類") or ""),
        "階数": _floor_count(str(prop.get("構造") or "")),
        "持分割合": "・".join(
            f"{o.get('氏名')} {o.get('持分')}".strip() if o.get("持分")
            else str(o.get("氏名") or "") for o in owners),
        "状況": "・".join(
            f"{o.get('氏名')}（{o.get('住所')}）" if o.get("住所")
            else str(o.get("氏名") or "") for o in owners),
        "担保抵当権": "有" if otsuku.get("有効権利あり") else "無",
        "担保内容": str(otsuku.get("内容") or ""),
    }
    chiseki = _number_or_none(str(prop.get("地積") or ""))
    if chiseki is not None:
        fields["地積"] = chiseki
    for floor, area in _floor_areas(str(prop.get("床面積") or "")).items():
        if floor in ("1", "2", "3"):
            fields[f"床面積{floor}階"] = area
    return {k: v for k, v in fields.items() if v != ""}


def _tokutei_joho(prop: dict) -> str:
    """App 35 特定情報（登記の表示を優先書式で・02 §2.3 の推奨書式に合わせる）"""
    parts = []
    for label, code in (("所在", "所在"), ("地番", "地番"), ("地目", "地目"),
                        ("地積", "地積"), ("家屋番号", "家屋番号"), ("種類", "種類"),
                        ("構造", "構造"), ("床面積", "床面積")):
        value = str(prop.get(code) or "")
        if value:
            parts.append(f"{label} {value}")
    return " / ".join(parts)


async def _resolve_case(case_hint: str | None, fudosan_id: str) -> str:
    """案件レコードIDの解決: case_hint → 過去の財産行からの逆引き（S4 と同じ流儀）"""
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
            app, filename or "登記事項証明.pdf", pdf_bytes, "application/pdf")
        return [{"fileKey": key}]
    except Exception as e:
        print(f"[REGISTRY_INGEST] 原本添付に失敗（処理続行）: {e}")
        return None


async def _file_needs_review(reason: str, detail: dict,
                             pdf_bytes: bytes, filename: str) -> str | None:
    """App 30 要確認キューへの起票（S4 の先例と同型）。env 未設定はスキップ縮退"""
    if not (APP_SHIPPING.app_id() and APP_SHIPPING.token()):
        print(f"[REGISTRY_INGEST] 要確認キュー起票スキップ（APP_SHIPPING 未設定）: {reason}")
        return None
    fields = {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": UNIT,
        "件名": f"登記事項証明の読解転記: {reason}",
        "エラー詳細": f"{reason}\n{json.dumps(detail, ensure_ascii=False)}"[:500],
        "チャネル固有データ": json.dumps({"registry_ingest": detail},
                                         ensure_ascii=False),
        "実行済み": "no",
    }
    attachment = await _attach(APP_SHIPPING, filename, pdf_bytes)
    if attachment:
        fields["成果物"] = attachment
    return str(await kintone.create_record(APP_SHIPPING, fields))


async def _upsert_zaisan(prop: dict, fudosan_id: str, case_id: str,
                         fid: str, pdf_bytes: bytes, filename: str) -> dict:
    """App 35 財産行の upsert。評価証明由来（S4）の同一物件行があれば追記のみ
    （評価額・評価確定・データ源・有効は上書きしない）"""
    owners = (prop.get("甲区") or {}).get("所有者") or []
    existing = []
    if fudosan_id and case_id:
        existing = await kintone.search_records(
            APP_ZAISAN,
            f'案件レコードID = "{case_id}" and 元レコードID = "{fudosan_id}"'
            f' and 有効 in ("yes")',
            fields=["$id", "原本"])
    if existing:
        zaisan_id = _v(existing[0], "$id")
        fields = {
            "特定情報": _tokutei_joho(prop),  # 登記の表示を優先（03 §の方針）
            "名義": owners_display(owners),
        }
        # 原本は既存 fileKey を保持したまま登記PDFを追加添付
        keeps = [{"fileKey": f.get("fileKey")}
                 for f in ((existing[0].get("原本") or {}).get("value") or [])
                 if f.get("fileKey")]
        attachment = await _attach(APP_ZAISAN, filename, pdf_bytes)
        if attachment:
            fields["原本"] = keeps + attachment
        await kintone.update_record(APP_ZAISAN, zaisan_id, fields)
        return {"zaisan": "updated", "zaisan_record_id": zaisan_id}

    fields = {
        "ユニット種別": UNIT,
        "案件アプリID": os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
        "案件レコードID": case_id,
        "財産種別": KIND_TO_ZAISAN.get(str(prop.get("種別") or "不明"), "その他"),
        "特定情報": _tokutei_joho(prop),
        "名義": owners_display(owners),
        "データ源": DATA_SOURCE,
        "元アプリID": APP_FUDOSAN.app_id() if fudosan_id else "",
        "元レコードID": fudosan_id,
        "冪等キー": fid,
        "評価確定": "no",
        "有効": "yes",
    }
    attachment = await _attach(APP_ZAISAN, filename, pdf_bytes)
    if attachment:
        fields["原本"] = attachment
    zaisan_id = str(await kintone.create_record(
        APP_ZAISAN, {k: v for k, v in fields.items() if v != ""}))
    return {"zaisan": "created", "zaisan_record_id": zaisan_id}


async def ingest_registry_pdf(pdf_bytes: bytes, filename: str, *,
                              case_hint: str | None = None,
                              drive_file_id: str | None = None) -> dict:
    """登記 PDF の読解→転記処理の中核（S5-3 で分離）。

    /registry/ingest エンドポイントと、仕分けからの回送（sortation_ingest の
    内部呼び出し）が共用する。挙動はエンドポイント時代と不変:
    冪等 skip・品質ゲート・名寄せ・App 25/35 転記・要確認キュー。
    """
    vision_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not vision_key:
        raise HTTPException(status_code=500,
                            detail="環境変数が未設定です: GOOGLE_VISION_API_KEY")

    fid = (drive_file_id or "").strip() or \
        f"sha256:{hashlib.sha256(pdf_bytes).hexdigest()}"

    zaisan_enabled = bool(APP_ZAISAN.app_id() and APP_ZAISAN.token())
    fudosan_enabled = bool(APP_FUDOSAN.app_id() and APP_FUDOSAN.token())

    # 冪等: 既処理 PDF は skip（App 財産の 冪等キー 一致・S4/M5 と同じ方式）
    if zaisan_enabled:
        try:
            existing = await kintone.search_records(
                APP_ZAISAN, f'冪等キー = "{fid}"', fields=["$id"])
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"kintone検索エラー: {e}")
        if existing:
            return {"status": "skip", "reason": "既処理（冪等キー一致）",
                    "zaisan_record_id": str(existing[0]["$id"]["value"])}
    else:
        print("[REGISTRY_INGEST] APP_ZAISAN 未設定: 冪等チェックと財産行転記をスキップ")

    try:
        ocr_text = _ocr_pdf(pdf_bytes, vision_key)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRエラー: {e}")

    try:
        reading = await read_registry(ocr_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"読解エラー: {e}")

    # 品質ゲート: スキーマ逸脱・低確信度は転記せず要確認へ（安全側）
    errors = validate_reading(reading)
    overall = overall_confidence(reading)
    if errors or overall < reread_threshold():
        reason = f"スキーマ逸脱 {len(errors)} 件" if errors else \
            f"全体確信度 {overall} < {reread_threshold()}"
        review_id = await _file_needs_review(
            reason, {"検証エラー": errors, "全体確信度": overall,
                     "冪等キー": fid}, pdf_bytes, filename)
        return {"status": "needs_review", "reason": reason,
                "review_record_id": review_id, "全体確信度": overall}

    results = []
    for i, prop in enumerate(reading.get("物件") or []):
        result: dict = {"index": i, "種別": prop.get("種別"),
                        "所在": prop.get("所在")}
        fudosan_id = ""
        if fudosan_enabled:
            state, exact, partial = await _find_fudosan(prop)
            if state == "matched":
                fudosan_id = _v(exact[0], "$id")
                await kintone.update_record(APP_FUDOSAN, fudosan_id,
                                            _fudosan_fields(prop))
                result["fudosan"] = "updated"
            elif state in ("none", "no_key"):
                fudosan_id = str(await kintone.create_record(
                    APP_FUDOSAN, _fudosan_fields(prop)))
                result["fudosan"] = "created"
            else:  # ambiguous: マージも先頭採用もしない（裁定・安全側）
                detail = {"理由": "名寄せの曖昧一致", "物件": i,
                          "所在": prop.get("所在"), "キー": _match_key(prop),
                          "完全一致件数": len(exact), "部分一致件数": len(partial),
                          "冪等キー": fid}
                review_id = await _file_needs_review(
                    "名寄せの曖昧一致（マージ・先頭採用せず）", detail,
                    pdf_bytes, filename)
                result["fudosan"] = "needs_review"
                result["review_record_id"] = review_id
                results.append(result)
                continue
        else:
            print("[REGISTRY_INGEST] KINTONE_FUDOSAN_APP_ID 未設定: 不動産25転記をスキップ")

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
            else:
                result.update(await _upsert_zaisan(
                    prop, fudosan_id, case_id, fid, pdf_bytes, filename))
                result["case_record_id"] = case_id
        results.append(result)

    print(f"[REGISTRY_INGEST] done file={filename} 物件={len(results)} "
          f"全体確信度={overall}")
    return {"status": "ok", "全体確信度": overall, "results": results,
            "ocr_chars": len(ocr_text)}


@router.post("/registry/ingest")
async def registry_ingest(token: str = "",
                          # file は意図的に optional: File(...) だと探信に 422 が
                          # 返り 404 偽装より先に存在が漏れる（koseki_ingest と同じ）
                          file: UploadFile | None = File(default=None),
                          case_hint: str | None = Form(default=None),
                          drive_file_id: str | None = Form(default=None)):
    """登記事項証明 PDF を受領し、読解→不動産25・App 財産へ転記する。

    case_hint: 案件レコードID（省略可）。drive_file_id: 冪等キー（省略時は sha256）。
    処理の中核は ingest_registry_pdf（仕分けからの回送と共用・S5-3）。
    """
    if not verify_token(token, "REGISTRY_INGEST_TOKEN"):
        raise HTTPException(status_code=404, detail="Not Found")

    if file is None or not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    return await ingest_registry_pdf(
        await file.read(), file.filename,
        case_hint=case_hint, drive_file_id=drive_file_id)
