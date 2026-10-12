"""houki_shokugyo — HOUKI-SHOKUGYO-1: App 40「職業」欄の接続（ヒアリング書込・相談カード読取・申述書差し込み）

経緯: HEARING-FIX1 の止血で「職業」は App 40 に実欄が無かったため書込集合から除外されていた（含めたまま書くと
create/update 全体が kintone に拒否され会話が全断）。本票は欄の追加（大野・CU）を前提に接続を戻すが、**欄の有無を
form fields API で検査（1 時間キャッシュ・失敗は「無い」側）し、無い間は書込から落とす**（fail-closed・会話を止めない）。
QUEUE-2026-10-13-D D-8。
- ヒアリング 7 通（質問文・prompt）は不変。書込集合 HEARING_WRITABLE_FIELDS に 職業 を復帰（upsert の直前で可用性を検査）。
- 相談カード読取（hub/houki_card_read）は同じ書込経路（apply_hearing_fields）を通るため、カードの報告に 職業 が含まれれば
  欄がある限り書ける（v1 カードには項目が無いので実際の書込は v2 以降）。
- 申述書差し込み（hub/houki_shinjutsu）: 様式の「職業」ラベル右のセル（table 4・row 2・cell 5）へ値を入れる。雛形は不変
  （sanctioned substitution・期待列にも同じ差し込みを適用）。
- RV-10: ログは固定語彙のみ。
"""

import logging
import time

from hub import kintone
from hub.houki_case_store import APP_HOUKI_CASE

logger = logging.getLogger("hub.houki_shokugyo")

FIELD = "職業"
MAX_CHARS = 20
CACHE_SECONDS = 3600.0
_cache: dict = {}        # "available" → (monotonic, bool)


async def available(*, now=None) -> bool:
    """App 40 に 職業 欄（SINGLE_LINE_TEXT／DROP_DOWN）があるか（1 時間キャッシュ・照会失敗は False）。"""
    t = time.monotonic() if now is None else now
    hit = _cache.get("available")
    if hit and t - hit[0] < CACHE_SECONDS:
        return hit[1]
    try:
        props = await kintone.get_form_fields(APP_HOUKI_CASE)
        ok = (props.get(FIELD) or {}).get("type") in ("SINGLE_LINE_TEXT", "DROP_DOWN")
    except Exception:
        logger.info("[HOUKI_SHOKUGYO] field check failed (treated as unavailable)")
        return False
    _cache["available"] = (t, ok)
    return ok


def clear_cache() -> None:
    _cache.clear()


async def strip_if_unavailable(fields: dict) -> dict:
    """upsert 直前: 職業 を含む書込は欄の可用性を検査し、無ければ落とす（他欄は書く）。"""
    if FIELD not in (fields or {}):
        return dict(fields or {})
    if await available():
        return dict(fields)
    out = dict(fields)
    out.pop(FIELD, None)
    logger.info("[HOUKI_SHOKUGYO] dropped (field unavailable)")
    return out


def value_for_docx(record: dict) -> str:
    """申述書差し込み用の値（最大 MAX_CHARS・改行なし）。"""
    v = str(((record or {}).get(FIELD) or {}).get("value") or "").strip().replace("\n", " ")
    return v[:MAX_CHARS]
