"""課税明細ライン S4: /ocr/fixed-asset の追記型拡張（App 財産への財産行 upsert）

設計: docs/souzoku-shorui/02 §3・§5、01 §4.1 Step 3、05 §3 S4

- 既存 /ocr/fixed-asset の動作（Vision OCR→抽出→不動産25の所在like検索→
  評価額・年度上書き）は不変。本モジュールは**既存処理成功後にのみ**呼ばれる
- 無効化（安全側）: env `ZAISAN_SYNC_DISABLED=1` で追加処理を丸ごとスキップ。
  `APP_ZAISAN` 未設定時もスキップ。いずれも既存動作だけが残る＝現状と同一
- 案件紐付け（02 §3 Step 1）: case_hint（案件レコードID）→ 無ければ同じ
  不動産レコード由来の過去の財産行から逆引き → どちらも不能なら App 財産に
  登録せず要確認キューへ
- upsert キー: (案件レコードID, 元レコードID, 評価基準日)。同一年度の再送は上書き
- 評価基準日: 固定資産税の賦課期日（当該年度の1月1日）
- 複数ヒット: 不動産25の所在like検索が2件以上でも先頭1件採用の既存挙動は維持し、
  備考に「所在検索N件ヒット・先頭採用」と記録して要確認キューにも起票する
  （検索ロジック自体の改善はスコープ外）
- 要確認キュー: App 30 に 方向=受領・発送ステータス=要確認 で起票
  （architecture/08 §4 の M5 既存設計のまま。専用キューは作らない）
  ※ App 30 の `ユニット種別` 選択肢に「相続一般」の追加が必要（S1 の人作業）
"""

import hashlib
import json
import logging
import os

from hub import kintone
from hub.redact import emit

logger = logging.getLogger("units.souzoku.zaisan_sync")

APP_ZAISAN = kintone.KintoneApp("App 財産", "APP_ZAISAN", "TOKEN_ZAISAN")
APP_FUDOSAN = kintone.KintoneApp(
    "App 25 (不動産)", "KINTONE_FUDOSAN_APP_ID", "KINTONE_FUDOSAN_API_TOKEN")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

UNIT = "相続一般"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _kijunbi(nendo) -> str:
    """評価基準日 = 固定資産税の賦課期日（当該年度の1月1日）"""
    return f"{int(nendo)}-01-01" if nendo else ""


def _tokutei_joho(fudosan: dict) -> str:
    """不動産25のフィールドから 02 §2.3 の推奨書式を組み立てる（存在項目のみ）"""
    parts = []
    for label, code, suffix in (("所在", "所在", ""), ("地番", "地番", ""),
                                ("地目", "地目", ""), ("地積", "地積", "㎡")):
        value = _v(fudosan, code)
        if value:
            parts.append(f"{label} {value}{suffix}")
    return " / ".join(parts)


def _enabled() -> tuple[bool, str]:
    if os.environ.get("ZAISAN_SYNC_DISABLED") == "1":
        return False, "ZAISAN_SYNC_DISABLED=1"
    if not APP_ZAISAN.app_id():
        return False, "APP_ZAISAN 未設定"
    return True, ""


async def _resolve_case(case_hint: str | None, fudosan_record_id: str) -> tuple[str, str]:
    """案件レコードIDの解決: case_hint → 過去の財産行からの逆引き（02 §3 Step 1）"""
    if case_hint:
        return case_hint, "case_hint"
    rows = await kintone.search_records(
        APP_ZAISAN,
        f'元アプリID = "{APP_FUDOSAN.app_id()}" and 元レコードID = "{fudosan_record_id}"'
        f' and 有効 in ("yes")',
        fields=["案件レコードID"])
    for row in rows:
        case_id = _v(row, "案件レコードID")
        if case_id:
            return case_id, "逆引き"
    return "", ""


async def _attach(app: kintone.KintoneApp, filename: str | None,
                  pdf_bytes: bytes) -> list | None:
    """原本 PDF のアップロード。失敗しても本体処理は続行する（添付は補助情報）"""
    try:
        key = await kintone.upload_file(
            app, filename or "課税明細.pdf", pdf_bytes, "application/pdf")
        return [{"fileKey": key}]
    except Exception as e:
        logger.info("[ZAISAN_SYNC] 原本添付に失敗（処理続行） cls=%s: %s",
                    type(e).__name__, emit(str(e), "vendor_raw", "log", "operator"))
        return None


async def _file_needs_review(reason: str, detail: dict,
                             pdf_bytes: bytes, filename: str | None) -> str:
    """App 30 要確認キューへの起票（02 §5・architecture/08 §4 の既存設計のまま）"""
    fields = {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": UNIT,
        "件名": f"課税明細の財産行同期: {reason}",
        "エラー詳細": f"{reason}\n{json.dumps(detail, ensure_ascii=False)}"[:500],
        "チャネル固有データ": json.dumps({"zaisan_sync": detail}, ensure_ascii=False),
        "実行済み": "no",
    }
    attachment = await _attach(APP_SHIPPING, filename, pdf_bytes)
    if attachment:
        fields["成果物"] = attachment
    return str(await kintone.create_record(APP_SHIPPING, fields))


async def sync_fixed_asset(*, fudosan_record_id: str, extracted: dict,
                           shozaichi: str, pdf_bytes: bytes,
                           filename: str | None, case_hint: str | None) -> dict | None:
    """既存処理成功後の追記型同期。

    Returns:
        None … 無効（フラグ/env未設定）。呼び出し側は従来レスポンスをそのまま返す
        dict … 実行結果（status: created / updated / needs_review）
    """
    enabled, why = _enabled()
    if not enabled:
        logger.info("[ZAISAN_SYNC] skipped")
        return None

    # 複数ヒットの記録用に既存検索と同条件で件数を取る（先頭1件採用の既存挙動は不変）
    hits = await kintone.search_records(
        APP_FUDOSAN, f'所在 like "{shozaichi}"', fields=["$id"])
    hit_count = len(hits)

    case_id, case_via = await _resolve_case(case_hint, fudosan_record_id)
    if not case_id:
        detail = {"理由": "案件紐付け不能", "不動産レコードID": fudosan_record_id,
                  "所在検索ヒット件数": hit_count, "抽出": extracted}
        review_id = await _file_needs_review("案件紐付け不能", detail,
                                             pdf_bytes, filename)
        return {"status": "needs_review", "review_record_id": review_id,
                "hit_count": hit_count}

    fudosan = await kintone.get_record(APP_FUDOSAN, fudosan_record_id)
    kijunbi = _kijunbi(extracted.get("年度"))

    fields = {
        "ユニット種別": UNIT,
        "案件アプリID": os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
        "案件レコードID": case_id,
        "財産種別": {"土地": "不動産_土地", "建物": "不動産_建物"}.get(
            _v(fudosan, "種別"), ""),
        "特定情報": _tokutei_joho(fudosan),
        "評価額": str(extracted.get("評価額") or ""),
        "評価方法": "固定資産税評価額",
        "評価基準日": kijunbi,
        "データ源": "OCR_課税明細",
        "元アプリID": APP_FUDOSAN.app_id(),
        "元レコードID": fudosan_record_id,
        "冪等キー": hashlib.sha256(pdf_bytes).hexdigest(),
    }
    if hit_count > 1:
        fields["備考"] = (f"所在検索{hit_count}件ヒット・先頭採用"
                          f"（不動産レコード{fudosan_record_id}）")
    attachment = await _attach(APP_ZAISAN, filename, pdf_bytes)
    if attachment:
        fields["原本"] = attachment

    # upsert キー = (案件, 元レコードID, 評価基準日)。同一年度の再送は上書き
    conds = [f'案件レコードID = "{case_id}"',
             f'元レコードID = "{fudosan_record_id}"']
    if kijunbi:
        conds.append(f'評価基準日 = "{kijunbi}"')
    existing = await kintone.search_records(
        APP_ZAISAN, " and ".join(conds), fields=["$id"])
    if existing:
        zaisan_id = _v(existing[0], "$id")
        await kintone.update_record(APP_ZAISAN, zaisan_id, fields)
        action = "updated"
    else:
        # 初期状態は新規作成時のみ設定（更新時は弁護士の評価確定などを触らない）
        fields["評価確定"] = "no"
        fields["有効"] = "yes"
        zaisan_id = str(await kintone.create_record(APP_ZAISAN, fields))
        action = "created"

    review_id = None
    if hit_count > 1:
        detail = {"理由": "所在検索の複数ヒット", "ヒット件数": hit_count,
                  "採用レコードID": fudosan_record_id, "財産行ID": zaisan_id,
                  "案件レコードID": case_id}
        review_id = await _file_needs_review("所在検索の複数ヒット（先頭採用）",
                                             detail, pdf_bytes, filename)

    result = {"status": action, "zaisan_record_id": zaisan_id,
              "case_record_id": case_id, "case_via": case_via,
              "hit_count": hit_count}
    if review_id:
        result["review_record_id"] = review_id
    return result
