"""固定資産評価証明・課税明細の構造化読解（S4-M1）: OCR 生テキスト → 評価JSON

設計: 2026-07-07 S4 近代化調査＋裁定
- 本モジュールは**読解部品のみ**（入口エンドポイント・App 25/35 転記は S4-M2）。
  既存 /ocr/fixed-asset・units/souzoku/zaisan_sync は不変（並存）
- 既存 S4 の抽出知見（評価額=円整数・カンマ/円除去、年度=西暦4桁〔令和6年度→2024〕）を
  継承し、方式はテキストJSONから tool use 強制（R3/S5 と同流儀）に置換。
  現行 S4 が単一物件前提だったのに対し、**複数物件（課税明細の複数筆）を配列で抽出**
- toolスキーマのプロパティキーは英数字のみ（Anthropic API 制約
  ^[a-zA-Z0-9_.-]{1,64}$。test_koseki_tool_schema の AST 静的検査が自動で対象化）。
  外部契約は日本語キー——to_japanese_valuation が写像（R3 の _map_flat 型を転用）
- 確信度3層: フィールド別 confidence マップ・全体平均（overall_confidence）・
  env 閾値（VALUATION_REREAD_THRESHOLD・既定 0.5。判定の利用は S4-M2 の責務）
"""

import os
import statistics

import anthropic

from claude_gateway import create_message_with_fallback
from config import VALUATION_READER_PROMPTS

DOC_KINDS = ["評価証明", "課税明細", "不明"]
PROPERTY_KINDS = ["土地", "家屋", "不明"]

_CONFIDENCE_MAP = {
    "type": "object",
    "description": "フィールド名（このツールの英語プロパティ名）→確信度（0〜1）",
    "additionalProperties": {"type": "number"},
}

# 評価読解スキーマ（tool use で強制する）
VALUATION_READING_TOOL = {
    "name": "save_valuation_reading",
    "description": "固定資産評価証明書・課税明細書OCRテキストの構造化読解結果を保存する",
    "input_schema": {
        "type": "object",
        "properties": {
            "doc_kind": {"type": "string", "enum": DOC_KINDS,
                         "description": "書類種別（固定資産評価証明書/課税明細書）。"
                                        "判別できなければ「不明」"},
            "year": {"type": ["integer", "null"],
                     "description": "年度。西暦4桁の整数（例: 令和6年度→2024・"
                                    "令和7年度→2025）。不明なら null"},
            "owner_name": {"type": "string",
                           "description": "所有者または納税義務者の氏名・名称。"
                                          "原文のまま。なければ空文字"},
            "properties": {
                "type": "array",
                "description": "物件（1枚に複数の土地・家屋が載る様式では**すべて**抽出する）",
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": PROPERTY_KINDS,
                                 "description": "土地/家屋。判別できなければ「不明」"},
                        "location": {"type": "string",
                                     "description": "所在。原文表記のまま全体を出力"
                                                    "（切り詰めない）"},
                        "lot_number": {"type": "string",
                                       "description": "地番（土地）。原文のまま"},
                        "building_number": {"type": "string",
                                            "description": "家屋番号（家屋）。原文のまま"},
                        "assessed_value": {"type": ["integer", "null"],
                                           "description": "評価額。円単位の整数"
                                                          "（カンマ・円は除去）。"
                                                          "読み取れなければ null"},
                        "confidence": _CONFIDENCE_MAP,
                    },
                    "required": ["kind", "location"],
                },
            },
            "confidence": _CONFIDENCE_MAP,
        },
        "required": ["doc_kind", "properties"],
    },
}

# ── 写像層: Claude 出力（英語キー）→ 日本語キー JSON（R3 の _map_flat 型を転用）──
# 対応表にあるキーだけ翻訳・無いキーは素通し（日本語キー入力には恒等）・
# 欠落キーは補完しない（必須欠落は validate_reading が検知）

_TOP_JA = {"doc_kind": "書類種別", "year": "年度", "owner_name": "所有者名",
           "properties": "物件"}
_PROPERTY_JA = {"kind": "種別", "location": "所在", "lot_number": "地番",
                "building_number": "家屋番号", "assessed_value": "評価額"}


def _map_flat(obj, key_map: dict):
    """1階層のキー翻訳。confidence マップの中身（フィールド名→数値）も同じ表で翻訳
    （koseki_reader._map_flat と同型・S4 用の対応表で運用）"""
    if not isinstance(obj, dict):
        return obj
    out = {}
    for k, v in obj.items():
        jk = key_map.get(k, k)
        if jk == "confidence" and isinstance(v, dict):
            v = {key_map.get(ck, ck): cv for ck, cv in v.items()}
        out[jk] = v
    return out


def to_japanese_valuation(raw: dict) -> dict:
    """Claude 出力（VALUATION_READING_TOOL の英語キー）を日本語キーへ写像する。

    - 対応表にあるキーのみ翻訳。無いキー（日本語キー・将来の未知キー）は素通し
      （日本語キー入力には恒等＝既存 JSON の再処理でも安全）
    - 欠落キーは補完しない（必須欠落の検知は validate_reading の責務のまま）
    """
    if not isinstance(raw, dict):
        return raw
    mapped = _map_flat(raw, _TOP_JA)
    props = mapped.get("物件")
    if isinstance(props, list):
        mapped["物件"] = [_map_flat(p, _PROPERTY_JA) for p in props]
    return mapped


# ── 検証・確信度（R3/S5 と同じ3層構成） ─────────────────────────────────────

class ValuationReaderError(Exception):
    """読解が実行できなかった（Claude 応答不正等）"""


def _is_conf(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and 0 <= value <= 1


def validate_reading(reading: dict) -> list[str]:
    """日本語キー評価JSONの検証。逸脱の一覧を返す（空リスト = 適合）"""
    errors: list[str] = []
    if reading.get("書類種別") not in DOC_KINDS:
        errors.append(f"書類種別 が許容値外: {reading.get('書類種別')!r}")
    year = reading.get("年度")
    if year is not None and not (isinstance(year, int) and
                                 not isinstance(year, bool)):
        errors.append(f"年度 が整数でも null でもない: {year!r}")
    props = reading.get("物件")
    if not isinstance(props, list) or not props:
        errors.append("物件 が空でない配列でない")
        return errors
    for i, prop in enumerate(props):
        if not isinstance(prop, dict):
            errors.append(f"物件[{i}] がオブジェクトでない")
            continue
        if prop.get("種別") not in PROPERTY_KINDS:
            errors.append(f"物件[{i}].種別 が許容値外: {prop.get('種別')!r}")
        if not isinstance(prop.get("所在"), str) or not prop.get("所在"):
            errors.append(f"物件[{i}].所在 が空でない文字列でない")
        value = prop.get("評価額")
        if value is not None and not (isinstance(value, int) and
                                      not isinstance(value, bool)):
            errors.append(f"物件[{i}].評価額 が整数でも null でもない: {value!r}")
    return errors


def _collect_confidences(reading: dict) -> list[float]:
    values = [float(v) for v in (reading.get("confidence") or {}).values()
              if _is_conf(v)]
    for prop in reading.get("物件") or []:
        if isinstance(prop, dict):
            values += [float(v) for v in (prop.get("confidence") or {}).values()
                       if _is_conf(v)]
    return values


def overall_confidence(reading: dict) -> float:
    """全体確信度 = 全 confidence 値の平均（R3/S5 と同じ・値が無ければ 0.0）"""
    values = _collect_confidences(reading)
    return round(statistics.fmean(values), 3) if values else 0.0


def reread_threshold() -> float:
    """要再読解の閾値（env 上書き可・既定 0.5。判定への利用は S4-M2 の責務）"""
    return float(os.environ.get("VALUATION_REREAD_THRESHOLD", "0.5"))


# ── 読解本体 ──────────────────────────────────────────────────────────────────

def _get_client() -> anthropic.AsyncAnthropic:
    return anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY") or "unset")


async def read_valuation(ocr_text: str) -> dict:
    """OCR テキスト → 日本語キーの評価JSON（tool use 強制・写像層適用済み）。

    検証・確信度判定は行わない（validate_reading / overall_confidence を
    呼び出し側=S4-M2 が使う）。応答不正は ValuationReaderError。
    """
    prompt = VALUATION_READER_PROMPTS["共通"].format(ocr_text=ocr_text)
    response = await create_message_with_fallback(
        _get_client(),
        context="評価証明読解",
        max_tokens=4096,
        tools=[VALUATION_READING_TOOL],
        tool_choice={"type": "tool", "name": VALUATION_READING_TOOL["name"]},
        messages=[{"role": "user", "content": prompt}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == VALUATION_READING_TOOL["name"]:
            return to_japanese_valuation(dict(block.input))
    raise ValuationReaderError(
        f"tool_use ブロックがない応答（stop_reason={response.stop_reason}）")
