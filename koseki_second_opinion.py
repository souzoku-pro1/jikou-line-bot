"""戸籍読解のセカンドオピニオン層（R5-1）: 低確信度読解への Claude 視覚読解の併用

設計: 2026-07-07 R5-1 裁定
- 対象は戸籍読解ライン（R3）のみ・第1版
- 発動条件: 一次読解（Vision OCR → Claude 構造化）の全体確信度が
  env SECOND_OPINION_THRESHOLD（既定 0.85）未満、または同閾値未満の
  フィールド確信度を含む場合
- 発動時: App 33 の原本PDFを Claude API（視覚・document ブロック）で再読解し、
  フィールド単位で突合（人物は正規化氏名で対応付け）
- 表記の正規化差を吸収する比較（実装判断）: 大字・漢数字を算用へ変換する
  比較用カノニカライズ（_canon）で突合する。一次読解の**値そのものは変えない**
- 一致 → 当該フィールドの確信度を max(現値, 0.95) へ引き上げ（実装判断:
  独立2経路の一致は高確信とみなす。1.0 にはしない=人の確認の余地を残す）
- 不一致 → 一次値を保持したまま、読解JSON 内の「セカンドオピニオン」ブロックに
  両値を記録し 要目視=true（実装判断: App 33 の既存フィールドは一切変えず、
  読解JSON のトップキー追加のみ——スキーマ検証・R4 の外部契約に無影響）
- 機械はどちらの値も「正」と自動確定しない（両論併記＋人の確認）。
  日付フィールドで片方のみ形式適正な場合は不一致エントリに「形式所見」を
  注記する（実装判断: 注記のみ・値の自動採用はしない）
- env SECOND_OPINION_ENABLED 既定無効（無効時は完全不発・既存経路無影響）
- コスト防御: 1書類（App 33 レコード）あたり再読解1回のみ。読解JSON に
  セカンドオピニオン ブロックが既にあれば再発動しない。高確信度書類は発動しない
"""

import base64
import json
import os
import re
import unicodedata

from config import KOSEKI_SECOND_OPINION_PROMPT
from hub import kintone

AGREE_CONFIDENCE = 0.95  # 一致時の引き上げ先（max(現値, これ)）

SO_KEY = "セカンドオピニオン"

# 比較対象の日付系フィールド（形式所見の対象）
_DATE_FIELDS = {"生年月日", "編製日", "消除日", "日付"}

_KANJI_DIGITS = {"〇": 0, "零": 0, "一": 1, "壱": 1, "壹": 1, "二": 2, "弐": 2,
                 "貳": 2, "三": 3, "参": 3, "參": 3, "四": 4, "肆": 4,
                 "五": 5, "伍": 5, "六": 6, "陸": 6, "七": 7, "漆": 7,
                 "八": 8, "捌": 8, "九": 9, "玖": 9}
_KANJI_UNITS = {"十": 10, "拾": 10, "百": 100, "佰": 100, "千": 1000, "仟": 1000}
_KANJI_CHARS = set(_KANJI_DIGITS) | set(_KANJI_UNITS)

_DATE_RE = re.compile(r"^(明治|大正|昭和|平成|令和)\d{1,3}年(\d{1,2}月)?(\d{1,2}日)?$"
                      r"|^\d{4}-\d{2}-\d{2}$")


def enabled() -> bool:
    return os.environ.get("SECOND_OPINION_ENABLED") == "1"


def threshold() -> float:
    return float(os.environ.get("SECOND_OPINION_THRESHOLD", "0.85"))


def _kanji_run_to_int(run: str) -> int:
    total, num = 0, 0
    for ch in run:
        if ch in _KANJI_DIGITS:
            num = num * 10 + _KANJI_DIGITS[ch]
        else:  # 単位
            total += (num or 1) * _KANJI_UNITS[ch]
            num = 0
    return total + num


def _canon(text: str) -> str:
    """比較用カノニカライズ: NFKC＋空白除去＋大字/漢数字→算用数字。
    値の保存には使わない（一次読解の原文は不変）。小書き仮名は変換しない
    （ョ/ヨ は別文字のまま＝突合で不一致として人に見せる）"""
    s = unicodedata.normalize("NFKC", str(text or ""))
    s = re.sub(r"[\s　]+", "", s)
    out = []
    run = ""
    for ch in s:
        if ch in _KANJI_CHARS:
            run += ch
        else:
            if run:
                out.append(str(_kanji_run_to_int(run)))
                run = ""
            out.append(ch)
    if run:
        out.append(str(_kanji_run_to_int(run)))
    return "".join(out)


def _looks_like_date(value: str) -> bool:
    return bool(_DATE_RE.match(_canon(value)))


def _collect_field_confidences(reading: dict) -> list[float]:
    """フィールド単位の発動判定用（koseki_reader._collect_confidences を共用）"""
    from koseki_reader import _collect_confidences  # 遅延 import（循環回避）
    return _collect_confidences(reading)


def needs_second_opinion(reading: dict) -> bool:
    """発動条件: 全体確信度 < 閾値 または 閾値未満のフィールド確信度を含む。
    既にセカンドオピニオン実施済みの読解には発動しない（1書類1回）"""
    if SO_KEY in (reading or {}):
        return False
    from koseki_reader import _overall_confidence  # 遅延 import（循環回避）
    th = threshold()
    values = _collect_field_confidences(reading)
    if not values:
        return True  # 確信度が皆無 = 低確信度として扱う（安全側）
    return _overall_confidence(reading) < th or any(v < th for v in values)


async def read_pdf_with_claude(pdf_bytes: bytes) -> dict:
    """原本PDFの視覚再読解（既存 R3 部品を流用: 同一 tool スキーマ・写像層・
    モデルフォールバック）。02 §3 日本語キーの読解 JSON を返す"""
    from claude_gateway import create_message_with_fallback
    from koseki_reader import (
        KOSEKI_READING_TOOL, KosekiReaderError, _get_client,
        to_japanese_reading,
    )
    response = await create_message_with_fallback(
        _get_client(),
        context="戸籍セカンドオピニオン",
        max_tokens=8192,
        tools=[KOSEKI_READING_TOOL],
        tool_choice={"type": "tool", "name": KOSEKI_READING_TOOL["name"]},
        messages=[{"role": "user", "content": [
            {"type": "document",
             "source": {"type": "base64", "media_type": "application/pdf",
                        "data": base64.b64encode(pdf_bytes).decode()}},
            {"type": "text", "text": KOSEKI_SECOND_OPINION_PROMPT},
        ]}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == KOSEKI_READING_TOOL["name"]:
            return to_japanese_reading(dict(block.input))
    raise KosekiReaderError(
        f"セカンドオピニオンに tool_use がない応答（stop_reason={response.stop_reason}）")


def _raise_conf(conf_map: dict | None, field: str) -> dict:
    conf_map = dict(conf_map or {})
    current = conf_map.get(field)
    current = float(current) if isinstance(current, (int, float)) \
        and not isinstance(current, bool) else 0.0
    conf_map[field] = max(current, AGREE_CONFIDENCE)
    return conf_map


def _compare(field_label: str, field_name: str, first_value, second_value,
             mismatches: list) -> bool:
    """1フィールドの突合。Returns: 一致（確信度引き上げ対象）か。
    両方空は比較対象外（False・引き上げも記録もしない）"""
    ca, cb = _canon(first_value), _canon(second_value)
    if not ca and not cb:
        return False
    if ca == cb:
        return True
    entry = {"対象": field_label, "一次": first_value, "再読解": second_value}
    if field_name in _DATE_FIELDS:
        fa, fb = _looks_like_date(first_value), _looks_like_date(second_value)
        if fa != fb:
            entry["形式所見"] = ("一次=適正/再読解=不正" if fa
                                 else "一次=不正/再読解=適正")
    mismatches.append(entry)
    return False


def merge_second_opinion(first: dict, second: dict) -> dict:
    """一次読解と視覚再読解のフィールド単位突合（純関数・一次の値は不変）。

    - 一致フィールド: confidence を max(現値, 0.95) へ引き上げ
    - 不一致フィールド: セカンドオピニオン.不一致 に両値を記録（要目視=true）
    - 人物は正規化氏名（_canon）で対応付け。対応が取れない人物は不一致として記録
    Returns: 突合結果を織り込んだ読解 JSON（first のコピー＋SO_KEY ブロック）
    """
    merged = json.loads(json.dumps(first, ensure_ascii=False))  # deep copy
    mismatches: list[dict] = []
    agreed = 0

    # ── 様式 ────────────────────────────────────────────────────────────────
    if _compare("様式", "様式", first.get("様式"), second.get("様式"), mismatches):
        current = merged.get("様式confidence")
        current = float(current) if isinstance(current, (int, float)) \
            and not isinstance(current, bool) else 0.0
        merged["様式confidence"] = max(current, AGREE_CONFIDENCE)
        agreed += 1

    # ── 戸籍（表紙・戸籍事項） ──────────────────────────────────────────────
    k1 = first.get("戸籍") or {}
    k2 = second.get("戸籍") or {}
    if isinstance(k1, dict) and isinstance(k2, dict):
        for field in ("本籍", "筆頭者", "編製日", "消除日", "編製事由",
                      "新戸籍_本籍"):
            if _compare(f"戸籍.{field}", field, k1.get(field), k2.get(field),
                        mismatches):
                merged["戸籍"]["confidence"] = _raise_conf(
                    merged["戸籍"].get("confidence"), field)
                agreed += 1
        j1, j2 = k1.get("従前戸籍") or {}, k2.get("従前戸籍") or {}
        if isinstance(j1, dict) and isinstance(j2, dict):
            for field in ("本籍", "筆頭者"):
                if _compare(f"戸籍.従前戸籍.{field}", field,
                            j1.get(field), j2.get(field), mismatches):
                    agreed += 1  # 従前戸籍に confidence マップは無い（記録のみ）

    # ── 人物（正規化氏名で対応付け） ────────────────────────────────────────
    persons2 = [p for p in second.get("人物") or [] if isinstance(p, dict)]
    by_canon = {}
    for p in persons2:
        by_canon.setdefault(_canon(p.get("氏名")), []).append(p)
    matched_seconds = set()
    for i, p1 in enumerate(first.get("人物") or []):
        if not isinstance(p1, dict):
            continue
        label = f"人物[{p1.get('氏名')}]"
        candidates = by_canon.get(_canon(p1.get("氏名"))) or []
        p2 = candidates[0] if candidates else None
        if p2 is None:
            mismatches.append({"対象": label, "一次": p1.get("氏名"),
                               "再読解": None,
                               "所見": "再読解に対応する人物がいません"})
            continue
        matched_seconds.add(id(p2))
        for field in ("続柄", "生年月日", "除籍事由"):
            if _compare(f"{label}.{field}", field,
                        p1.get(field), p2.get(field), mismatches):
                merged["人物"][i]["confidence"] = _raise_conf(
                    merged["人物"][i].get("confidence"), field)
                agreed += 1
        # 身分事項: 種別ごとの出現順で対応付け（同種別の k 番目同士を突合）
        events2_by_type: dict[str, list] = {}
        for e in p2.get("身分事項") or []:
            if isinstance(e, dict):
                events2_by_type.setdefault(str(e.get("種別")), []).append(e)
        seen_counts: dict[str, int] = {}
        for j, e1 in enumerate(p1.get("身分事項") or []):
            if not isinstance(e1, dict):
                continue
            etype = str(e1.get("種別"))
            k = seen_counts.get(etype, 0)
            seen_counts[etype] = k + 1
            pool = events2_by_type.get(etype) or []
            e2 = pool[k] if k < len(pool) else None
            if e2 is None:
                continue  # 対応する事項が無い（事項の粒度差は v1 では記録しない）
            elabel = f"{label}.身分事項[{etype}#{k + 1}]"
            ok_date = _compare(f"{elabel}.日付", "日付",
                               e1.get("日付"), e2.get("日付"), mismatches)
            ok_aite = _compare(f"{elabel}.相手方", "相手方",
                               e1.get("相手方"), e2.get("相手方"), mismatches)
            if ok_date or ok_aite:
                event = merged["人物"][i]["身分事項"][j]
                current = event.get("confidence")
                current = float(current) if isinstance(current, (int, float)) \
                    and not isinstance(current, bool) else 0.0
                if ok_date and ok_aite or (ok_date and not _canon(e1.get("相手方"))
                                           and not _canon(e2.get("相手方"))):
                    event["confidence"] = max(current, AGREE_CONFIDENCE)
                    agreed += 1
    for p2 in persons2:
        if id(p2) not in matched_seconds:
            mismatches.append({"対象": f"人物[{p2.get('氏名')}]",
                               "一次": None, "再読解": p2.get("氏名"),
                               "所見": "再読解のみに現れる人物です"})

    merged[SO_KEY] = {
        "実施": True,
        "一致": agreed,
        "不一致": mismatches,
        "要目視": bool(mismatches),
    }
    return merged


async def maybe_second_opinion(record: dict, reading: dict) -> dict:
    """process_record からの結線点（フラグ既定無効・失敗は縮退で一次読解のまま）。

    - 無効・高確信度・実施済みは一次読解をそのまま返す（完全不発）
    - 発動時: App 33 レコードの原本PDFを取得 → 視覚再読解 → 突合結果を返す
    - 再読解の失敗は一次読解に SO_KEY のエラー記録を添えて返す（読解は壊さない）
    """
    if not enabled():
        return reading
    if not needs_second_opinion(reading):
        return reading
    from koseki_reader import APP_KOSEKI_BOOK  # 遅延 import（循環回避）
    files = (record.get("原本PDF") or {}).get("value") or []
    file_key = next((f.get("fileKey") for f in files if f.get("fileKey")), None)
    if not file_key:
        return {**reading, SO_KEY: {"実施": False,
                                    "エラー": "原本PDFがありません"}}
    try:
        pdf_bytes = await kintone.download_file(APP_KOSEKI_BOOK, file_key)
        second = await read_pdf_with_claude(pdf_bytes)
        merged = merge_second_opinion(reading, second)
        so = merged[SO_KEY]
        print(f"[SECOND_OPINION] done 一致={so['一致']} "
              f"不一致={len(so['不一致'])} 要目視={so['要目視']}")
        return merged
    except Exception as e:
        print(f"[SECOND_OPINION] 再読解に失敗（一次読解のまま続行）: {e}")
        return {**reading, SO_KEY: {"実施": False, "エラー": str(e)[:200]}}
