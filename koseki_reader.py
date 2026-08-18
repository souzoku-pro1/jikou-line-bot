"""戸籍の構造化読解（R3）: OCR 生テキスト → Claude 読解 → 読解JSON 保存

設計: docs/koseki-ocr/01 §1-2〜5・§2・02 §3、07 §2 R2/R3 の裁定注記

2026-07-05 裁定（起動方式・ハイブリッド構成）:
- 核（B案の形）: process_unread_records(limit) / process_record(record_id)。
  対象は 読解状態=未読解 のレコードのみ（人の修正済みレコードを AI 出力で
  上書きしない構造的ガード。C案 Webhook 起動を不採用とした理由と同じ）
- 入口（A案）: /koseki/ingest が登録成功後に process_record を同期呼び出し。
  失敗しても ingest の成功応答は不変（レコードは 未読解 のまま残り、
  本モジュールの核関数で後日回収できる）
- 定期実行・再読解の結線は核関数の再利用で行う（今回スコープ外）

読解の規約:
- claude_gateway.create_message_with_fallback を共用（モデルフォールバック・
  残高警報を継承）。tool use（save_koseki_reading）で構造化出力を強制し、
  text 応答からの JSON 切り出しはしない（dispatch D2 と同じ流儀）
- スキーマは 02 §3 に厳密に従う。日付（編製日・消除日・生年月日・身分事項）は
  和暦原文のまま保持し、西暦は 編製日_西暦 / 消除日_西暦 の別キーのみ
  （変換に自信がなければ null。最終的な西暦確定は人手確認フロー=R4 の対象）
- スキーマ逸脱・低確信度（全体確信度 < KOSEKI_REREAD_THRESHOLD・既定 0.5）は
  安全側に倒し 読解状態=要再読解 とする
- 本モジュールが書く 読解状態 は AI読解済 / 要再読解 の2つだけ。
  それより先へ進める遷移は人手確認フロー（R4）の専権であり、ここには書かない
  （test_koseki_reader の静的検査で固定）
- 保存する 読解JSON には元の ocr_text を必ず残す（再読解の入力を失わないため）
- env KOSEKI_READER_DISABLED=1 で読解を丸ごと無効化（ingest は登録のみになる）
"""

import json
import logging
import os
import re
import statistics
import unicodedata
from datetime import date

import anthropic

from claude_gateway import create_message_with_fallback
from config import KOSEKI_READER_PROMPTS
from hub import kintone
from hub.redact import emit

logger = logging.getLogger("koseki_reader")

APP_KOSEKI_BOOK = kintone.KintoneApp(
    "App 33 (戸籍読解)", "APP_KOSEKI_BOOK", "TOKEN_KOSEKI_BOOK")

STATUS_UNREAD = "未読解"
STATUS_AI_DONE = "AI読解済"
STATUS_REREAD = "要再読解"

# 02 §3 の許容値（App 33 の選択肢と一致）
FORMS = ["現行", "改製原（平成）", "改製原（昭和）", "除籍", "不明"]
IDENTITY_EVENT_TYPES = ["出生", "婚姻", "離婚", "養子縁組", "離縁", "認知",
                        "死亡", "転籍", "入籍", "除籍", "改製", "その他"]

_CONFIDENCE_MAP = {
    "type": "object",
    "description": "フィールド名（このツールの英語プロパティ名）→確信度（0〜1）",
    "additionalProperties": {"type": "number"},
}

# 02 §3 の読解 JSON スキーマ（tool use で強制する）。
# ⚠ プロパティキーは英数字のみ: Anthropic API は input_schema のキー名に
# ^[a-zA-Z0-9_.-]{1,64}$ を強制し、日本語キーは 400 で即時拒否される
# （2026-07-06 実機で判明・test_koseki_tool_schema の静的検査で固定）。
# 保存する読解 JSON は従来どおり 02 §3 の日本語キー——to_japanese_reading が写像する
KOSEKI_READING_TOOL = {
    "name": "save_koseki_reading",
    "description": "戸籍OCRテキストの構造化読解結果を保存する（02 §3 スキーマ）",
    "input_schema": {
        "type": "object",
        "properties": {
            "form": {"type": "string", "enum": FORMS, "description": "様式"},
            "form_confidence": {"type": "number", "description": "様式判定の確信度 0〜1"},
            "koseki": {
                "type": "object",
                "description": "戸籍（表紙・戸籍事項）",
                "properties": {
                    "honseki": {"type": "string", "description": "本籍。原文表記のまま"},
                    "hittousha": {"type": "string",
                                  "description": "筆頭者（戸主含む）・原文表記のまま"},
                    "hensei_date": {"type": "string",
                                    "description": "編製日。和暦原文のまま（例: 昭和32年4月1日）"},
                    "hensei_date_seireki": {"type": ["string", "null"],
                                            "description": "編製日の西暦 YYYY-MM-DD。"
                                                           "変換に自信がなければ null"},
                    "shojo_date": {"type": "string",
                                   "description": "消除日。和暦原文のまま。なければ空文字"},
                    "shojo_date_seireki": {"type": ["string", "null"],
                                           "description": "消除日の西暦 YYYY-MM-DD。"
                                                          "変換に自信がなければ null"},
                    "hensei_reason": {"type": "string",
                                      "description": "編製事由。転籍・改製・婚姻等（原文）"},
                    "juzen_koseki": {
                        "type": "object",
                        "description": "従前戸籍",
                        "properties": {"honseki": {"type": "string"},
                                       "hittousha": {"type": "string"}},
                    },
                    "shin_koseki_honseki": {"type": "string", "description": "新戸籍の本籍"},
                    "confidence": _CONFIDENCE_MAP,
                },
                "required": ["honseki", "hittousha"],
            },
            "persons": {
                "type": "array",
                "description": "戸籍に記録されている人物",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string",
                                 "description": "氏名。旧字体・異体字も原文どおり"},
                        "zokugara": {"type": "string", "description": "続柄"},
                        "birth_date": {"type": "string",
                                       "description": "生年月日。和暦原文のまま"},
                        "removed": {"type": "boolean", "description": "除籍済み"},
                        "removed_reason": {"type": "string", "description": "除籍事由"},
                        "identity_events": {
                            "type": "array",
                            "description": "身分事項",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string",
                                             "enum": IDENTITY_EVENT_TYPES,
                                             "description": "種別"},
                                    "date": {"type": "string",
                                             "description": "日付。和暦原文のまま"},
                                    "aite": {"type": "string", "description": "相手方"},
                                    "biko": {"type": "string",
                                             "description": "備考。種別=その他 のとき原文を保持"},
                                    "confidence": {"type": "number"},
                                },
                                "required": ["type"],
                            },
                        },
                        "confidence": _CONFIDENCE_MAP,
                    },
                    "required": ["name"],
                },
            },
        },
        "required": ["form", "form_confidence", "koseki", "persons"],
    },
}

# ── 写像層: Claude 出力（英語キー）→ 02 §3 の日本語キー JSON ──────────────
# 保存 JSON・validate_reading・App 33 書込・確信度計算・R4 の外部契約は日本語キーの
# まま不変。写像は「対応表にあるキーだけ翻訳・無いキーは素通し」——
# 日本語キー入力には恒等（再読解で旧 JSON を食っても壊れない）・欠落キーは
# 欠落のまま（必須欠落は従来どおり validate_reading が検知する）

_TOP_JA = {"form": "様式", "form_confidence": "様式confidence",
           "koseki": "戸籍", "persons": "人物"}
_KOSEKI_JA = {"honseki": "本籍", "hittousha": "筆頭者",
              "hensei_date": "編製日", "hensei_date_seireki": "編製日_西暦",
              "shojo_date": "消除日", "shojo_date_seireki": "消除日_西暦",
              "hensei_reason": "編製事由", "juzen_koseki": "従前戸籍",
              "shin_koseki_honseki": "新戸籍_本籍"}
_PERSON_JA = {"name": "氏名", "zokugara": "続柄", "birth_date": "生年月日",
              "removed": "除籍済み", "removed_reason": "除籍事由",
              "identity_events": "身分事項"}
_EVENT_JA = {"type": "種別", "date": "日付", "aite": "相手方", "biko": "備考"}


def _map_flat(obj, key_map: dict):
    """1階層のキー翻訳。confidence マップの中身（フィールド名→数値）も同じ表で翻訳"""
    if not isinstance(obj, dict):
        return obj
    out = {}
    for k, v in obj.items():
        jk = key_map.get(k, k)
        if jk == "confidence" and isinstance(v, dict):
            v = {key_map.get(ck, ck): cv for ck, cv in v.items()}
        out[jk] = v
    return out


def _map_person(person):
    if not isinstance(person, dict):
        return person
    mapped = _map_flat(person, _PERSON_JA)
    events = mapped.get("身分事項")
    if isinstance(events, list):
        mapped["身分事項"] = [_map_flat(e, _EVENT_JA) for e in events]
    return mapped


def to_japanese_reading(raw: dict) -> dict:
    """Claude 出力（KOSEKI_READING_TOOL の英語キー）を 02 §3 の日本語キーへ写像する。

    - 対応表にあるキーのみ翻訳。無いキー（日本語キー・将来の未知キー）は素通し
      （日本語キー入力には恒等＝既存 JSON の再処理でも安全）
    - 欠落キーは補完しない（必須欠落の検知は validate_reading の責務のまま）
    """
    if not isinstance(raw, dict):
        return raw
    mapped = _map_flat(raw, _TOP_JA)
    koseki = mapped.get("戸籍")
    if isinstance(koseki, dict):
        koseki = _map_flat(koseki, _KOSEKI_JA)
        if isinstance(koseki.get("従前戸籍"), dict):
            koseki["従前戸籍"] = _map_flat(koseki["従前戸籍"], _KOSEKI_JA)
        mapped["戸籍"] = koseki
    persons = mapped.get("人物")
    if isinstance(persons, list):
        mapped["人物"] = [_map_person(p) for p in persons]
    return mapped


# ── KOSEKI-DATA-1: 和暦→西暦の決定的正規化 ──────────────────────────────────
# 目的: 連続性判定（戸籍不足チェック・第2段票）の入力充足。モデル申告の
# *_西暦 に依存せず、和暦原文から機械変換で埋める。変換規律は fail-closed
# ——grammar 不成立・元号範囲外・暦として不存在の日付は null（誤変換より欠落）

_ERA_BASE = {"明治": 1867, "大正": 1911, "昭和": 1925, "平成": 1988,
             "令和": 2018}
# 各元号の最終年（明治45=1912・大正15=1926・昭和64=1989・平成31=2019。
# 令和は進行中のため上限 98=西暦 2116 を形式上限とする）
_ERA_MAX = {"明治": 45, "大正": 15, "昭和": 64, "平成": 31, "令和": 98}
_WAREKI_RE = re.compile(
    r"^(明治|大正|昭和|平成|令和)(元|\d{1,2})年(\d{1,2})月(\d{1,2})日$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_wareki(text) -> str | None:
    """和暦日付（例: 昭和32年4月1日）を西暦 ISO（YYYY-MM-DD）へ決定的に変換。

    - 全角数字・空白は NFKC/除去で吸収。それ以外の余計な文字が付く場合は
      変換しない（「昭和32年4月1日編製」等は None＝切り出しは読解側の責務）
    - 元号年の範囲外（昭和99年等）・暦に無い日付（2月30日等）は None
    """
    s = unicodedata.normalize("NFKC", str(text or ""))
    s = s.replace(" ", "").replace("　", "").strip()
    m = _WAREKI_RE.fullmatch(s)
    if not m:
        return None
    era, y, month, day = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
    year_n = 1 if y == "元" else int(y)
    if not (1 <= year_n <= _ERA_MAX[era]):
        return None
    year = _ERA_BASE[era] + year_n
    try:
        date(year, month, day)
    except ValueError:
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _valid_iso(value) -> str | None:
    s = str(value or "")
    if not _ISO_DATE_RE.fullmatch(s):
        return None
    y, m, d = (int(x) for x in s.split("-"))
    try:
        date(y, m, d)
    except ValueError:
        return None
    return s


def _normalized_date(wareki, model_seireki):
    """機械変換を優先。不成立ならモデル申告の妥当な ISO のみ許容・それも
    無ければ None（誤変換より欠落）。"""
    det = parse_wareki(wareki)
    if det is not None:
        return det
    return _valid_iso(model_seireki)


def _apply_dates(container: dict, pairs: tuple) -> None:
    """指定ペアの西暦キーを充足し、原文があるのに変換不能だったキーを
    「西暦変換不能」に列挙する（毎回作り直し＝冪等）。confidence マップには
    一切触れない——0.0 を混ぜると全体確信度が汚染され、既存の要再読解判定
    （overall < threshold）を誤爆させるため、fail-closed の明示は独立キーで
    行う（誤変換より欠落・欠落は可視化）。"""
    marks = []
    for src_key, dst_key in pairs:
        normalized = _normalized_date(container.get(src_key),
                                      container.get(dst_key))
        container[dst_key] = normalized
        if normalized is None and str(container.get(src_key) or "").strip():
            marks.append(dst_key)
    if marks:
        container["西暦変換不能"] = marks
    else:
        container.pop("西暦変換不能", None)


def normalize_reading(reading: dict) -> dict:
    """KOSEKI-DATA-1(1): 読解結果の決定的正規化（in-place・冪等）。

    - 戸籍.編製日_西暦／消除日_西暦: 和暦原文からの機械変換で充足
    - 人物[].生年月日_西暦: 新設キー（同上）
    - 原文ありで変換不能は null＋「西暦変換不能」キーで明示（confidence／
      全体確信度・読解状態の判定には影響させない）
    - 既存キー（ocr_text・人物・confidence 等）は上記以外変更しない
      （後方互換）。dict 以外・欠落構造は素通し
    """
    if not isinstance(reading, dict):
        return reading
    koseki = reading.get("戸籍")
    if isinstance(koseki, dict):
        _apply_dates(koseki, (("編製日", "編製日_西暦"),
                              ("消除日", "消除日_西暦")))
    persons = reading.get("人物")
    if isinstance(persons, list):
        for person in persons:
            if isinstance(person, dict):
                _apply_dates(person, (("生年月日", "生年月日_西暦"),))
    return reading


def structured_fields(saved_json: dict) -> dict:
    """KOSEKI-DATA-1(2): App33 の kintone 構造化 field への書き戻し値。

    厳密検証済みの値のみ含める（kintone 側で拒否され得る値を送らない）:
    - 戸籍種別: 様式（FORMS 閉集合＝App33 の選択肢と一致）のみ
    - 編製日／消除日: 正規化済み ISO（YYYY-MM-DD）のみ
    値が無い field はキー自体を含めない（既存値を消さない）。"""
    out: dict = {}
    if not isinstance(saved_json, dict):
        return out
    if saved_json.get("様式") in FORMS:
        out["戸籍種別"] = saved_json["様式"]
    koseki = saved_json.get("戸籍")
    if isinstance(koseki, dict):
        for src, dst in (("編製日_西暦", "編製日"), ("消除日_西暦", "消除日")):
            value = _valid_iso(koseki.get(src))
            if value is not None:
                out[dst] = value
    return out


class KosekiReaderError(Exception):
    """読解が実行できなかった（Claude 応答不正等・レコードは未読解のまま）"""


def _disabled() -> bool:
    return os.environ.get("KOSEKI_READER_DISABLED") == "1"


def _reread_threshold() -> float:
    return float(os.environ.get("KOSEKI_REREAD_THRESHOLD", "0.5"))


def _get_client() -> anthropic.AsyncAnthropic:
    return anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY") or "unset")


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "")


async def _read_with_claude(ocr_text: str) -> dict:
    """OCR テキストを 02 §3 スキーマに構造化する（tool use 強制・D2 と同流儀）"""
    prompt = KOSEKI_READER_PROMPTS["共通"].format(ocr_text=ocr_text)
    response = await create_message_with_fallback(
        _get_client(),
        context="戸籍読解",
        max_tokens=8192,
        tools=[KOSEKI_READING_TOOL],
        tool_choice={"type": "tool", "name": KOSEKI_READING_TOOL["name"]},
        messages=[{"role": "user", "content": prompt}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == KOSEKI_READING_TOOL["name"]:
            # 英語キー（API 制約）→ 02 §3 の日本語キーへ写像して返す。
            # 以降（validate_reading・確信度計算・保存）の契約は従来どおり日本語キー
            return to_japanese_reading(dict(block.input))
    raise KosekiReaderError(
        f"tool_use ブロックがない応答（stop_reason={response.stop_reason}）")


def _is_conf(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and 0 <= value <= 1


def validate_reading(reading: dict) -> list[str]:
    """02 §3 スキーマの検証。逸脱の一覧を返す（空リスト = 適合）"""
    errors: list[str] = []
    if reading.get("様式") not in FORMS:
        errors.append(f"様式が許容値外: {reading.get('様式')!r}")
    if not _is_conf(reading.get("様式confidence")):
        errors.append("様式confidence が 0〜1 の数値でない")
    koseki = reading.get("戸籍")
    if not isinstance(koseki, dict):
        errors.append("戸籍 がオブジェクトでない")
    else:
        for key in ("本籍", "筆頭者"):
            if not isinstance(koseki.get(key), str):
                errors.append(f"戸籍.{key} が文字列でない")
    persons = reading.get("人物")
    if not isinstance(persons, list):
        errors.append("人物 が配列でない")
    else:
        for i, person in enumerate(persons):
            if not isinstance(person, dict) or not isinstance(person.get("氏名"), str):
                errors.append(f"人物[{i}].氏名 が文字列でない")
                continue
            for j, event in enumerate(person.get("身分事項") or []):
                t = (event or {}).get("種別") if isinstance(event, dict) else None
                if t not in IDENTITY_EVENT_TYPES:
                    errors.append(f"人物[{i}].身分事項[{j}].種別 が許容値外: {t!r}")
    return errors


def _collect_confidences(reading: dict) -> list[float]:
    values = []
    if _is_conf(reading.get("様式confidence")):
        values.append(float(reading["様式confidence"]))
    koseki = reading.get("戸籍") or {}
    if isinstance(koseki, dict):
        values += [float(v) for v in (koseki.get("confidence") or {}).values()
                   if _is_conf(v)]
    for person in reading.get("人物") or []:
        if not isinstance(person, dict):
            continue
        values += [float(v) for v in (person.get("confidence") or {}).values()
                   if _is_conf(v)]
        for event in person.get("身分事項") or []:
            if isinstance(event, dict) and _is_conf(event.get("confidence")):
                values.append(float(event["confidence"]))
    return values


def _overall_confidence(reading: dict) -> float:
    values = _collect_confidences(reading)
    return round(statistics.fmean(values), 3) if values else 0.0


async def _save(record_id: str, saved_json: dict, status: str,
                form_conf: float, overall_conf: float) -> None:
    # KOSEKI-DATA-1(2): 構造化 field（戸籍種別・編製日・消除日）を同一 update に
    # 同梱（新規読解分は自動で充足）。値は structured_fields が厳密検証済みの
    # もののみ＝kintone 拒否による保存失敗を持ち込まない
    fields = {
        "読解JSON": json.dumps(saved_json, ensure_ascii=False),
        "読解状態": status,
        "様式確信度": str(round(form_conf, 3)),
        "全体確信度": str(round(overall_conf, 3)),
    }
    fields.update(structured_fields(saved_json))
    await kintone.update_record(APP_KOSEKI_BOOK, record_id, fields)


async def process_record(record_id: str) -> dict:
    """未読解レコード1件を構造化読解する（核関数・単一レコード用）。

    Returns: {"status": "ai_done" | "needs_reread" | "skipped", ...}
    実行不能（Claude 応答不正等）は KosekiReaderError 送出（レコードは未読解のまま）。
    """
    if _disabled():
        return {"status": "skipped", "reason": "KOSEKI_READER_DISABLED=1"}

    record = await kintone.get_record(APP_KOSEKI_BOOK, record_id)
    state = _v(record, "読解状態")
    if state != STATUS_UNREAD:
        return {"status": "skipped", "record_id": record_id,
                "reason": f"読解状態が{state or '空'}（未読解のみ対象）"}

    try:
        ocr_text = str(json.loads(_v(record, "読解JSON") or "{}").get("ocr_text") or "")
    except json.JSONDecodeError:
        ocr_text = ""
    if not ocr_text.strip():
        # OCR 入力が無い＝読解不能。安全側に 要再読解 へ（人がスキャンし直す）
        saved = {"ocr_text": ocr_text,
                 "検証エラー": ["OCRテキストが空のため読解できません（再スキャン・再OCRが必要）"]}
        await _save(record_id, saved, STATUS_REREAD, 0.0, 0.0)
        return {"status": "needs_reread", "record_id": record_id,
                "reason": "OCRテキストなし"}

    reading = await _read_with_claude(ocr_text)

    # R5-1: 低確信度読解へのセカンドオピニオン（視覚再読解・突合）。
    # SECOND_OPINION_ENABLED 既定無効=完全不発。発動時も一次読解の値は不変で、
    # 一致フィールドの確信度引き上げと「セカンドオピニオン」ブロックの追記のみ。
    # 失敗は縮退（一次読解のまま）——読解の成立を壊さない
    from koseki_second_opinion import maybe_second_opinion  # 遅延 import（循環回避）
    reading = await maybe_second_opinion(record, reading)

    # KOSEKI-DATA-1(1): 決定的正規化（和暦→西暦・fail-closed）。読解の既存
    # キーには触れない（後方互換）ため validate/確信度計算の契約は不変
    reading = normalize_reading(reading)

    errors = validate_reading(reading)
    form_conf = float(reading.get("様式confidence")) \
        if _is_conf(reading.get("様式confidence")) else 0.0
    overall = _overall_confidence(reading)

    if errors:
        status, reason = STATUS_REREAD, f"スキーマ逸脱 {len(errors)} 件"
    elif overall < _reread_threshold():
        status, reason = STATUS_REREAD, f"全体確信度 {overall} < {_reread_threshold()}"
    else:
        status, reason = STATUS_AI_DONE, ""

    saved = {**reading, "ocr_text": ocr_text}
    if errors:
        saved["検証エラー"] = errors
    await _save(record_id, saved, status, form_conf, overall)

    result = {"status": "ai_done" if status == STATUS_AI_DONE else "needs_reread",
              "record_id": record_id, "様式確信度": form_conf, "全体確信度": overall}
    if reason:
        result["reason"] = reason
    return result


async def process_unread_records(limit: int = 20) -> list[dict]:
    """未読解レコードを一括処理する（核関数・定期実行/回収用）。

    1件の失敗は他を止めない。結果のリストを返す（0件なら空リスト）。
    """
    if _disabled():
        return []
    records = await kintone.search_records(
        APP_KOSEKI_BOOK,
        f'読解状態 in ("{STATUS_UNREAD}") order by レコード番号 asc limit {int(limit)}',
        fields=["$id"])
    results = []
    for record in records:
        record_id = str((record.get("$id") or {}).get("value") or "")
        try:
            results.append(await process_record(record_id))
        except Exception as e:
            logger.info("[KOSEKI_READER] record %s の読解に失敗（未読解のまま）: %s: %s",
                        emit(record_id, "record_id", "log", "operator"),
                        type(e).__name__,
                        emit(str(e), "vendor_raw", "log", "operator"))
            results.append({"status": "error", "record_id": record_id,
                            "detail": str(e)[:200]})
    return results
