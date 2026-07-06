"""登記事項証明書の構造化読解（S5-1）: OCR 生テキスト → Claude 読解 → 登記JSON

設計: 2026-07-06 S5 設計調査＋裁定
- 本モジュールは**読解部品のみ**（kintone への転記・入口エンドポイントは S5-2）。
  参考実装 ocr_to_claude.py の抽出項目表を継承し、方式はテキストJSONから
  tool use 強制（R3/D2 と同流儀・text 応答からの JSON 切り出しはしない）に置換
- 裁定スコープ: 乙区は「有効権利の有無＋内容テキスト」まで（権利単位の構造化は
  スコープ外）。持分は原文文字列で保持（受け皿は App 25・S5-2 の責務。
  App 35 の名義は表示文字列＝1レコード=1財産を崩さない）
- toolスキーマのプロパティキーは英数字のみ（Anthropic API 制約
  ^[a-zA-Z0-9_.-]{1,64}$・2026-07-06 R3 実機400の教訓。test_koseki_tool_schema の
  AST 静的検査が自動で本スキーマも対象にする）。
  外部契約は日本語キー——to_japanese_registry が写像する（R3 の _map_flat 型を転用）
- 日付（受付日・原因日付）は和暦原文のまま保持し、西暦は _西暦 の別キーのみ
  （変換に自信がなければ null。R3 と同じ規約）
- 確信度3層: フィールド別 confidence マップ・全体平均（overall_confidence）・
  env 閾値（REGISTRY_REREAD_THRESHOLD・既定 0.5。判定の利用は S5-2 の責務）
"""

import os
import statistics

import anthropic

from claude_gateway import create_message_with_fallback
from config import REGISTRY_READER_PROMPTS

# 種別の許容値（registry_to_kintone.py の対応表を継承＋安全側の「不明」）
KINDS = ["土地", "建物", "区分建物", "不明"]

_CONFIDENCE_MAP = {
    "type": "object",
    "description": "フィールド名（このツールの英語プロパティ名）→確信度（0〜1）",
    "additionalProperties": {"type": "number"},
}

# 登記読解スキーマ（tool use で強制する）。抽出項目は ocr_to_claude.py の項目表を継承
REGISTRY_READING_TOOL = {
    "name": "save_registry_reading",
    "description": "不動産登記事項証明書OCRテキストの構造化読解結果を保存する",
    "input_schema": {
        "type": "object",
        "properties": {
            "properties": {
                "type": "array",
                "description": "物件（複数の不動産が含まれる場合はすべて抽出する）",
                "items": {
                    "type": "object",
                    "properties": {
                        "kind": {"type": "string", "enum": KINDS,
                                 "description": "種別。判別できなければ「不明」"},
                        # ── 表題部（不動産の表示・原文表記のまま） ──
                        "location": {"type": "string",
                                     "description": "所在。原文表記のまま"},
                        "lot_number": {"type": "string",
                                       "description": "地番（土地）。原文のまま"},
                        "land_category": {"type": "string",
                                          "description": "地目（土地）"},
                        "land_area": {"type": "string",
                                      "description": "地積（土地）。原文のまま（例: 123.45㎡）"},
                        "building_number": {"type": "string",
                                            "description": "家屋番号（建物）。原文のまま"},
                        "building_type": {"type": "string",
                                          "description": "種類（建物。居宅・共同住宅等）"},
                        "structure": {"type": "string",
                                      "description": "構造（建物。例: 木造かわらぶき2階建）"},
                        "floor_area": {"type": "string",
                                       "description": "床面積（建物）。階ごとの記載を原文のまま"
                                                      "（例: 1階 58.50㎡ 2階 62.60㎡）"},
                        # ── 権利部 甲区（最新の所有権・複数所有者対応） ──
                        "kouku": {
                            "type": "object",
                            "description": "権利部甲区のうち**現在有効な最新の所有権登記**",
                            "properties": {
                                "owners": {
                                    "type": "array",
                                    "description": "現在の所有者（共有なら全員）",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string",
                                                     "description": "氏名または名称。原文のまま"},
                                            "address": {"type": "string",
                                                        "description": "住所。原文のまま"},
                                            "share": {"type": "string",
                                                      "description": "持分。原文のまま"
                                                                     "（例: 2分の1。単独所有は空文字）"},
                                            "confidence": _CONFIDENCE_MAP,
                                        },
                                        "required": ["name"],
                                    },
                                },
                                "receipt_date": {"type": "string",
                                                 "description": "受付日。和暦原文のまま"
                                                                "（例: 平成14年3月1日）"},
                                "receipt_date_seireki": {"type": ["string", "null"],
                                                         "description": "受付日の西暦 YYYY-MM-DD。"
                                                                        "変換に自信がなければ null"},
                                "cause": {"type": "string",
                                          "description": "登記原因（売買・相続等。原文のまま）"},
                                "cause_date": {"type": "string",
                                               "description": "原因日付。和暦原文のまま"},
                                "cause_date_seireki": {"type": ["string", "null"],
                                                       "description": "原因日付の西暦 YYYY-MM-DD。"
                                                                      "変換に自信がなければ null"},
                                "confidence": _CONFIDENCE_MAP,
                            },
                            "required": ["owners"],
                        },
                        # ── 権利部 乙区（裁定スコープ: 有無＋内容テキストまで） ──
                        "otsuku": {
                            "type": "object",
                            "description": "権利部乙区。抹消済みを除いた有効な権利のみ対象",
                            "properties": {
                                "has_active_rights": {"type": "boolean",
                                                      "description": "有効な権利（抵当権・"
                                                                     "根抵当権等）の有無"},
                                "detail": {"type": "string",
                                           "description": "有効な権利の内容の要約テキスト"
                                                          "（原文の要点。無ければ空文字）"},
                                "confidence": {"type": "number"},
                            },
                            "required": ["has_active_rights"],
                        },
                        "confidence": _CONFIDENCE_MAP,
                    },
                    "required": ["kind", "location"],
                },
            },
        },
        "required": ["properties"],
    },
}

# ── 写像層: Claude 出力（英語キー）→ 日本語キー JSON（R3 の _map_flat 型を転用）──
# 対応表にあるキーだけ翻訳・無いキーは素通し（日本語キー入力には恒等）・
# 欠落キーは補完しない（必須欠落は validate_reading が検知）

_TOP_JA = {"properties": "物件"}
_PROPERTY_JA = {"kind": "種別", "location": "所在", "lot_number": "地番",
                "land_category": "地目", "land_area": "地積",
                "building_number": "家屋番号", "building_type": "種類",
                "structure": "構造", "floor_area": "床面積",
                "kouku": "甲区", "otsuku": "乙区"}
_KOUKU_JA = {"owners": "所有者", "receipt_date": "受付日",
             "receipt_date_seireki": "受付日_西暦", "cause": "原因",
             "cause_date": "原因日付", "cause_date_seireki": "原因日付_西暦"}
_OWNER_JA = {"name": "氏名", "address": "住所", "share": "持分"}
_OTSUKU_JA = {"has_active_rights": "有効権利あり", "detail": "内容"}


def _map_flat(obj, key_map: dict):
    """1階層のキー翻訳。confidence マップの中身（フィールド名→数値）も同じ表で翻訳
    （koseki_reader._map_flat と同型・S5 用の対応表で運用）"""
    if not isinstance(obj, dict):
        return obj
    out = {}
    for k, v in obj.items():
        jk = key_map.get(k, k)
        if jk == "confidence" and isinstance(v, dict):
            v = {key_map.get(ck, ck): cv for ck, cv in v.items()}
        out[jk] = v
    return out


def _map_property(prop):
    if not isinstance(prop, dict):
        return prop
    mapped = _map_flat(prop, _PROPERTY_JA)
    kouku = mapped.get("甲区")
    if isinstance(kouku, dict):
        kouku = _map_flat(kouku, _KOUKU_JA)
        owners = kouku.get("所有者")
        if isinstance(owners, list):
            kouku["所有者"] = [_map_flat(o, _OWNER_JA) for o in owners]
        mapped["甲区"] = kouku
    if isinstance(mapped.get("乙区"), dict):
        mapped["乙区"] = _map_flat(mapped["乙区"], _OTSUKU_JA)
    return mapped


def to_japanese_registry(raw: dict) -> dict:
    """Claude 出力（REGISTRY_READING_TOOL の英語キー）を日本語キーへ写像する。

    - 対応表にあるキーのみ翻訳。無いキー（日本語キー・将来の未知キー）は素通し
      （日本語キー入力には恒等＝既存 JSON の再処理でも安全）
    - 欠落キーは補完しない（必須欠落の検知は validate_reading の責務のまま）
    """
    if not isinstance(raw, dict):
        return raw
    mapped = _map_flat(raw, _TOP_JA)
    props = mapped.get("物件")
    if isinstance(props, list):
        mapped["物件"] = [_map_property(p) for p in props]
    return mapped


# ── 検証・確信度（R3 と同じ3層構成） ─────────────────────────────────────────

class RegistryReaderError(Exception):
    """読解が実行できなかった（Claude 応答不正等）"""


def _is_conf(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and 0 <= value <= 1


def validate_reading(reading: dict) -> list[str]:
    """日本語キー登記JSONの検証。逸脱の一覧を返す（空リスト = 適合）"""
    errors: list[str] = []
    props = reading.get("物件")
    if not isinstance(props, list) or not props:
        return ["物件 が空でない配列でない"]
    for i, prop in enumerate(props):
        if not isinstance(prop, dict):
            errors.append(f"物件[{i}] がオブジェクトでない")
            continue
        if prop.get("種別") not in KINDS:
            errors.append(f"物件[{i}].種別 が許容値外: {prop.get('種別')!r}")
        if not isinstance(prop.get("所在"), str) or not prop.get("所在"):
            errors.append(f"物件[{i}].所在 が空でない文字列でない")
        kouku = prop.get("甲区")
        if isinstance(kouku, dict):
            owners = kouku.get("所有者")
            if not isinstance(owners, list):
                errors.append(f"物件[{i}].甲区.所有者 が配列でない")
            else:
                for j, owner in enumerate(owners):
                    if not isinstance(owner, dict) or \
                            not isinstance(owner.get("氏名"), str) or \
                            not owner.get("氏名"):
                        errors.append(f"物件[{i}].甲区.所有者[{j}].氏名 が文字列でない")
        elif kouku is not None:
            errors.append(f"物件[{i}].甲区 がオブジェクトでない")
        otsuku = prop.get("乙区")
        if isinstance(otsuku, dict):
            if not isinstance(otsuku.get("有効権利あり"), bool):
                errors.append(f"物件[{i}].乙区.有効権利あり が真偽値でない")
        elif otsuku is not None:
            errors.append(f"物件[{i}].乙区 がオブジェクトでない")
    return errors


def _collect_confidences(reading: dict) -> list[float]:
    values: list[float] = []

    def add_map(obj):
        if isinstance(obj, dict):
            values.extend(float(v) for v in (obj.get("confidence") or {}).values()
                          if _is_conf(v))

    for prop in reading.get("物件") or []:
        if not isinstance(prop, dict):
            continue
        add_map(prop)
        kouku = prop.get("甲区")
        if isinstance(kouku, dict):
            add_map(kouku)
            for owner in kouku.get("所有者") or []:
                add_map(owner)
        otsuku = prop.get("乙区")
        if isinstance(otsuku, dict) and _is_conf(otsuku.get("confidence")):
            values.append(float(otsuku["confidence"]))
    return values


def overall_confidence(reading: dict) -> float:
    """全体確信度 = 全 confidence 値の平均（R3 と同じ・値が無ければ 0.0）"""
    values = _collect_confidences(reading)
    return round(statistics.fmean(values), 3) if values else 0.0


def reread_threshold() -> float:
    """要再読解の閾値（env 上書き可・既定 0.5。判定への利用は S5-2 の責務）"""
    return float(os.environ.get("REGISTRY_REREAD_THRESHOLD", "0.5"))


# ── 読解本体 ──────────────────────────────────────────────────────────────────

def _get_client() -> anthropic.AsyncAnthropic:
    return anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY") or "unset")


async def read_registry(ocr_text: str) -> dict:
    """OCR テキスト → 日本語キーの登記JSON（tool use 強制・写像層適用済み）。

    検証・確信度判定は行わない（validate_reading / overall_confidence を
    呼び出し側=S5-2 が使う）。応答不正は RegistryReaderError。
    """
    prompt = REGISTRY_READER_PROMPTS["共通"].format(ocr_text=ocr_text)
    response = await create_message_with_fallback(
        _get_client(),
        context="登記読解",
        max_tokens=8192,
        tools=[REGISTRY_READING_TOOL],
        tool_choice={"type": "tool", "name": REGISTRY_READING_TOOL["name"]},
        messages=[{"role": "user", "content": prompt}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == REGISTRY_READING_TOOL["name"]:
            return to_japanese_registry(dict(block.input))
    raise RegistryReaderError(
        f"tool_use ブロックがない応答（stop_reason={response.stop_reason}）")
