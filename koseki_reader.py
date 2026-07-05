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
import os
import statistics

import anthropic

from claude_gateway import create_message_with_fallback
from config import KOSEKI_READER_PROMPTS
from hub import kintone

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
    "description": "フィールド名→確信度（0〜1）",
    "additionalProperties": {"type": "number"},
}

# 02 §3 の読解 JSON スキーマ（tool use で強制する）
KOSEKI_READING_TOOL = {
    "name": "save_koseki_reading",
    "description": "戸籍OCRテキストの構造化読解結果を保存する（02 §3 スキーマ）",
    "input_schema": {
        "type": "object",
        "properties": {
            "様式": {"type": "string", "enum": FORMS},
            "様式confidence": {"type": "number", "description": "様式判定の確信度 0〜1"},
            "戸籍": {
                "type": "object",
                "properties": {
                    "本籍": {"type": "string", "description": "原文表記のまま"},
                    "筆頭者": {"type": "string", "description": "戸主含む・原文表記のまま"},
                    "編製日": {"type": "string", "description": "和暦原文のまま（例: 昭和32年4月1日）"},
                    "編製日_西暦": {"type": ["string", "null"],
                                    "description": "YYYY-MM-DD。変換に自信がなければ null"},
                    "消除日": {"type": "string", "description": "和暦原文のまま。なければ空文字"},
                    "消除日_西暦": {"type": ["string", "null"],
                                    "description": "YYYY-MM-DD。変換に自信がなければ null"},
                    "編製事由": {"type": "string", "description": "転籍・改製・婚姻等（原文）"},
                    "従前戸籍": {
                        "type": "object",
                        "properties": {"本籍": {"type": "string"},
                                       "筆頭者": {"type": "string"}},
                    },
                    "新戸籍_本籍": {"type": "string"},
                    "confidence": _CONFIDENCE_MAP,
                },
                "required": ["本籍", "筆頭者"],
            },
            "人物": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "氏名": {"type": "string", "description": "旧字体・異体字も原文どおり"},
                        "続柄": {"type": "string"},
                        "生年月日": {"type": "string", "description": "和暦原文のまま"},
                        "除籍済み": {"type": "boolean"},
                        "除籍事由": {"type": "string"},
                        "身分事項": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "種別": {"type": "string",
                                             "enum": IDENTITY_EVENT_TYPES},
                                    "日付": {"type": "string",
                                             "description": "和暦原文のまま"},
                                    "相手方": {"type": "string"},
                                    "備考": {"type": "string",
                                             "description": "種別=その他 のとき原文を保持"},
                                    "confidence": {"type": "number"},
                                },
                                "required": ["種別"],
                            },
                        },
                        "confidence": _CONFIDENCE_MAP,
                    },
                    "required": ["氏名"],
                },
            },
        },
        "required": ["様式", "様式confidence", "戸籍", "人物"],
    },
}


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
            return dict(block.input)
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
    await kintone.update_record(APP_KOSEKI_BOOK, record_id, {
        "読解JSON": json.dumps(saved_json, ensure_ascii=False),
        "読解状態": status,
        "様式確信度": str(round(form_conf, 3)),
        "全体確信度": str(round(overall_conf, 3)),
    })


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
            print(f"[KOSEKI_READER] record {record_id} の読解に失敗（未読解のまま）: {e}")
            results.append({"status": "error", "record_id": record_id,
                            "detail": str(e)[:200]})
    return results
