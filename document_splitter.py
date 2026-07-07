"""書類分割部品（D1-1・複数書類PDFの区間判定と断片化・純関数群）

設計: 2026-07-07 D1 設計調査＋裁定
- **本タスクは純部品のみ**（sortation への結線・回送は D1-2）。
  既存エンドポイント・GAS は無変更
- 区間判定: ページ別 OCR テキスト → tool use 強制（英語キー・AST 静的検査が
  自動対象化）で segments: [{start_page, end_page, doc_type, confidence}]。
  doc_type は仕分けの既存15種の列挙を流用
- 機械検証: 区間が**連続・非重複・全ページ被覆**でなければ不正。不正または
  区間 confidence < 閾値（env SORTATION_SPLIT_THRESHOLD・既定 0.85）は
  「分割不能」を明示シグナルで返す（安全側＝呼び出し元が全体を ask に落とす）
- 断片化: pymupdf（既存依存）でページ範囲の断片 PDF をメモリ上生成・
  ページ数の検証つき
"""

import os

import anthropic

from claude_gateway import create_message_with_fallback
from config import DOCUMENT_SPLIT_PROMPTS
from sortation_ingest import DOC_TYPES

SPLIT_TOOL = {
    "name": "save_document_segments",
    "description": "複数書類PDFの区間判定結果を保存する（1区間=1書類）",
    "input_schema": {
        "type": "object",
        "properties": {
            "segments": {
                "type": "array",
                "description": "書類区間（ページ昇順・連続・重複なし・全ページ被覆）",
                "items": {
                    "type": "object",
                    "properties": {
                        "start_page": {"type": "integer",
                                       "description": "区間の先頭ページ（1始まり）"},
                        "end_page": {"type": "integer",
                                     "description": "区間の末尾ページ（両端含む）"},
                        "doc_type": {"type": "string", "enum": DOC_TYPES,
                                     "description": "この区間の書類の種類"},
                        "confidence": {"type": "number",
                                       "description": "この区間の切れ目と種別の"
                                                      "両方への自信（0〜1）"},
                    },
                    "required": ["start_page", "end_page", "doc_type",
                                 "confidence"],
                },
            },
        },
        "required": ["segments"],
    },
}


class DocumentSplitError(Exception):
    """分割処理の失敗（Claude 応答不正・断片化不整合等）"""


def split_threshold() -> float:
    return float(os.environ.get("SORTATION_SPLIT_THRESHOLD", "0.85"))


def validate_segments(segments: list[dict], total_pages: int) -> list[str]:
    """区間の機械検証。逸脱の一覧を返す（空リスト = 適合）。

    連続・非重複・全ページ被覆（1〜total_pages）・整数・start<=end を検査。
    confidence の閾値は判定しない（decide の責務・検証は構造のみ）。
    """
    errors: list[str] = []
    if not isinstance(segments, list) or not segments:
        return ["segments が空でない配列でない"]
    for i, seg in enumerate(segments):
        if not isinstance(seg, dict):
            errors.append(f"segments[{i}] がオブジェクトでない")
            continue
        start, end = seg.get("start_page"), seg.get("end_page")
        for label, value in (("start_page", start), ("end_page", end)):
            if not (isinstance(value, int) and not isinstance(value, bool)):
                errors.append(f"segments[{i}].{label} が整数でない: {value!r}")
        if not errors and start > end:
            errors.append(f"segments[{i}]: start_page {start} > end_page {end}")
    if errors:
        return errors

    ordered = sorted(segments, key=lambda s: s["start_page"])
    if ordered[0]["start_page"] != 1:
        errors.append(f"先頭ページが覆われていません"
                      f"（最初の区間が {ordered[0]['start_page']} 始まり）")
    for prev, cur in zip(ordered, ordered[1:]):
        if cur["start_page"] <= prev["end_page"]:
            errors.append(f"区間の重複: p{prev['start_page']}-{prev['end_page']} と "
                          f"p{cur['start_page']}-{cur['end_page']}")
        elif cur["start_page"] != prev["end_page"] + 1:
            errors.append(f"区間の不連続: p{prev['end_page']} の次が "
                          f"p{cur['start_page']}（隙間）")
    if ordered[-1]["end_page"] != total_pages:
        errors.append(f"末尾ページが覆われていません"
                      f"（最後の区間が p{ordered[-1]['end_page']} まで・"
                      f"総ページ {total_pages}）")
    return errors


def _pages_block(page_texts: list[str]) -> str:
    parts = []
    for i, text in enumerate(page_texts, start=1):
        parts.append(f"=== {i}ページ目 ===\n{text}")
    return "\n\n".join(parts)


def _get_client() -> anthropic.AsyncAnthropic:
    return anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY") or "unset")


async def analyze_segments(page_texts: list[str]) -> dict:
    """ページ別 OCR テキスト → 区間判定（検証・閾値込み）。

    Returns:
      {"status": "ok", "segments": [...], "needs_split": bool} … 検証適合。
        needs_split=False は単一区間（分割不要の高速パス・従来経路と同じ扱い）
      {"status": "unsplittable", "reason": ...} … 構造不正 or 低確信度の明示
        シグナル（安全側＝呼び出し元が全体を ask に落とす）
    """
    total_pages = len(page_texts)
    if total_pages == 0:
        return {"status": "unsplittable", "reason": "ページテキストが空です"}
    if total_pages == 1:
        # 1ページは判定不要（分割の余地なし・AI 呼び出しもしない高速パス）
        return {"status": "ok", "needs_split": False,
                "segments": [{"start_page": 1, "end_page": 1,
                              "doc_type": "その他", "confidence": 1.0}]}

    prompt = DOCUMENT_SPLIT_PROMPTS["共通"].format(
        pages_block=_pages_block(page_texts))
    response = await create_message_with_fallback(
        _get_client(),
        context="書類分割判定",
        max_tokens=2048,
        tools=[SPLIT_TOOL],
        tool_choice={"type": "tool", "name": SPLIT_TOOL["name"]},
        messages=[{"role": "user", "content": prompt}],
    )
    segments = None
    for block in response.content:
        if block.type == "tool_use" and block.name == SPLIT_TOOL["name"]:
            segments = dict(block.input).get("segments")
            break
    if segments is None:
        raise DocumentSplitError(
            f"tool_use ブロックがない応答（stop_reason={response.stop_reason}）")

    errors = validate_segments(segments, total_pages)
    if errors:
        return {"status": "unsplittable",
                "reason": "区間検証に不合格: " + " / ".join(errors)}
    threshold = split_threshold()
    low = [s for s in segments
           if not isinstance(s.get("confidence"), (int, float))
           or s["confidence"] < threshold]
    if low:
        notes = "・".join(
            f"p{s['start_page']}-{s['end_page']}({s.get('confidence')})"
            for s in low)
        return {"status": "unsplittable",
                "reason": f"区間確信度が閾値 {threshold} 未満: {notes}"}
    ordered = sorted(segments, key=lambda s: s["start_page"])
    return {"status": "ok", "segments": ordered,
            "needs_split": len(ordered) > 1}


def split_pdf(pdf_bytes: bytes, segments: list[dict]) -> list[bytes]:
    """区間リストに従い断片 PDF をメモリ上で生成する（pymupdf・ページ数検証つき）"""
    import fitz  # PyMuPDF（既存依存）
    fragments: list[bytes] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as src:
        for seg in segments:
            start, end = seg["start_page"], seg["end_page"]
            if not (1 <= start <= end <= src.page_count):
                raise DocumentSplitError(
                    f"区間 p{start}-{end} が PDF の範囲外（総 {src.page_count} ページ）")
            fragment = fitz.open()
            fragment.insert_pdf(src, from_page=start - 1, to_page=end - 1)
            data = fragment.tobytes()
            fragment.close()
            with fitz.open(stream=data, filetype="pdf") as check:
                expected = end - start + 1
                if check.page_count != expected:
                    raise DocumentSplitError(
                        f"断片のページ数不整合: p{start}-{end} 期待{expected} "
                        f"実際{check.page_count}")
            fragments.append(data)
    return fragments
