"""職務上請求のLINE対応（D4・第1.5弾）: パラメータ検証・聞き返し定義・App 31照合

設計: docs/dispatch-bot/03（聞き返し）・05（レジストリ）・06（復唱）＋
      docs/architecture/04 §2（チャネル固有JSONスキーマ）

── GUI起票前提の必須項目の洗い出し（教訓③・実物 channels/shokumu_seikyu.py から）──
parse_channel_data が要求（欠けるとエラー遷移）:
  - request_items: 1件以上・type ∈ 対応6種別・count は1以上の整数
find_municipality が要求:
  - municipality（または宛先名）… 未指定はエラー遷移。App 31 未登録は PrepareDeferred
build_request_form_pdfs が要求:
  - 様式1（戸籍謄本・除籍謄本・改製原戸籍を含む場合）: target.生年月日 **必須**
    （欠損→エラー遷移「生年月日が必要です」）
  - 様式2のみ（住民票・除票・附票）: 生年月日は任意（空欄なら非印字）
コード上は任意だが実務上必須として聞くもの:
  - target.対象者（対象者なしの職務上請求は成立しないため必須扱い）
コード上も実務上も任意（聞かない・指示文にあれば載せる）:
  - target.フリガナ／本籍／住所／筆頭者／世帯主
  - purpose（未指定は既定文言 DEFAULT_PURPOSE。復唱後・承認画面で弁護士が確認可能）
"""

import json

from channels.base import PrepareDeferred
from channels.shokumu_seikyu import (
    APP_CITY_MASTER,
    FEE_FIELD_BY_TYPE,
    FORM1,
    FORM_BY_TYPE,
    compute_kogawase,
)
from hub import kintone

# 聞き返し文（レジストリ駆動・モデル生成に任せない。2026-07-04 教訓②）
QUESTIONS = {
    "request_items": ("請求する書類の種別と通数を教えてください"
                      "（例: 戸籍謄本2通と附票1通。対応種別: 戸籍謄本・除籍謄本・"
                      "改製原戸籍・戸籍の附票・住民票・住民票の除票）"),
    "municipality": "請求先の市区町村名を教えてください（例: 川口市）",
    "target_name": "請求対象者（戸籍・住民票の名義人）の氏名を教えてください",
    "birth_date": ("対象者の生年月日を教えてください"
                   "（戸籍系の請求では様式上必須です。例: 昭和25年3月15日 または 1950-03-15）"),
}

DEFAULT_PURPOSE = "受任事件の処理のため"

MSG_ABORTED = "中止しました。App 31 に登録後、もう一度指示してください"

# 対象者情報のキー（04 §2 の target スキーマ）
_TARGET_KEYS = ("対象者", "フリガナ", "本籍", "住所", "筆頭者", "世帯主", "生年月日")


def normalize_params(task_params: dict) -> dict:
    """モデル抽出値の検証・正規化。不正な request_items（未対応種別・通数不正）は
    落とす（→不足として聞き返しに乗る）。target は既知キーのみ・空値除去"""
    out = dict(task_params)

    items = []
    for it in task_params.get("request_items") or []:
        if not isinstance(it, dict):
            continue
        t = str(it.get("type") or "").strip()
        try:
            c = int(it.get("count"))
        except (TypeError, ValueError):
            c = 0
        if t in FEE_FIELD_BY_TYPE and c >= 1:
            items.append({"type": t, "count": c})
    out["request_items"] = items

    target = task_params.get("target") or {}
    out["target"] = {k: str(target.get(k)).strip() for k in _TARGET_KEYS
                     if str(target.get(k) or "").strip()}
    out["municipality"] = str(task_params.get("municipality") or "").strip()
    return out


def includes_form1(items: list[dict]) -> bool:
    """戸籍系（様式1）を含むか（生年月日必須の判定・実装 shokumu_seikyu と同じ表を使用）"""
    return any(FORM_BY_TYPE.get(i["type"]) == FORM1 for i in items)


def first_missing(parsed: dict) -> str | None:
    """不足項目の最初の1つ（1論点・聞く順: 種別通数→自治体→対象者→生年月日〔様式1のみ〕）"""
    p = parsed["task_params"]
    if not p.get("request_items"):
        return "request_items"
    if not p.get("municipality"):
        return "municipality"
    if not (p.get("target") or {}).get("対象者"):
        return "target_name"
    if includes_form1(p["request_items"]) and not p["target"].get("生年月日"):
        return "birth_date"
    return None


async def pre_confirm(parsed: dict) -> tuple[str, str]:
    """復唱前の App 31 照合（⑤）。
    Returns ("proceed", "") / ("choice", 質問文)。
    proceed 時は task_params に kogawase_estimate（小為替概算）を書き込む"""
    p = parsed["task_params"]
    name = p["municipality"]
    records = await kintone.search_records(
        APP_CITY_MASTER,
        f'市区町村名 = "{name.replace(chr(34), chr(92) + chr(34))}" and 有効 in ("yes")')
    if not records:
        return "choice", (
            f"「{name}」は App 31（市区町村マスタ）に未登録です。\n"
            "1. 登録後に再指示する（今回は中止）\n"
            "2. このまま起票する（起票後に登録依頼の警報→App 31 登録→"
            "下書きのまま再保存で自動再処理）\n"
            "番号で選んでください")
    try:
        total, _ = compute_kogawase(p["request_items"], records[0])
        p["kogawase_estimate"] = f"{total:,}円"
    except PrepareDeferred:
        p["kogawase_estimate"] = "概算不能（App 31 の手数料未登録・起票後に登録依頼警報）"
    return "proceed", ""


def choice(parsed: dict, selection: int) -> tuple[str, str]:
    """App 31 未登録時の選択（1=中止 / 2=このまま起票）"""
    if selection == 1:
        return "abort", MSG_ABORTED
    if selection == 2:
        p = parsed["task_params"]
        p["muni_note"] = "（App 31 未登録・起票後に登録依頼警報が届きます）"
        p["kogawase_estimate"] = "概算不可（自治体未登録）"
        return "proceed", ""
    return "invalid", "1 か 2 の番号で選んでください"


def summary_lines(parsed: dict) -> list[str]:
    """復唱フルテンプレに挿入する明細（⑥: 対象者・種別と通数・宛先自治体・小為替概算・注記）"""
    p = parsed["task_params"]
    target = p.get("target") or {}
    birth = f"（{target['生年月日']}生）" if target.get("生年月日") else ""
    items = "・".join(f"{i['type']}{i['count']}通" for i in p["request_items"])
    return [
        f"対象者: {target.get('対象者', '')}{birth}",
        f"請求: {items}",
        f"宛先自治体: {p['municipality']}{p.get('muni_note', '')}",
        f"小為替概算: {p.get('kogawase_estimate', '（未算出）')}",
        "発送には kintone での承認が別途必要です（このOKでは発送されません）",
    ]


def build_channel_json(parsed: dict) -> dict:
    """App 30 チャネル固有データ（parse_channel_data が通る形式・04 §2）"""
    p = parsed["task_params"]
    return {
        "request_items": p["request_items"],
        "municipality": p["municipality"],
        "target": p.get("target") or {},
        "purpose": str(p.get("purpose") or "").strip() or DEFAULT_PURPOSE,
    }
