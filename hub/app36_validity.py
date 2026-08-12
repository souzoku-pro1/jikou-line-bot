"""app36_validity — App36（相続人）行の有効性判定（単一の正・P3-003C-CANCEL §4.2）

正本: `docs/design-drafts/DRAFT_P3_003C_CANCEL.md`（FROZEN・CANCEL-06）＋
RV-08 `hub/person_validity.py` と同型（有効行ヘルパの一点定義）。

- **取消済み行（`取消済み=yes`）は「読み飛ばし」**——行削除はしない（機械削除
  禁止の原則）。consumer（projection の冪等検索・shokumu_plan の条件検査等）は
  本 module の filter を通して除外する（App36 読取の共通 filter・単一の正）。
- `取消済み=yes` を書けるのは**取消関所ハンドラの一本経路のみ**
  （hub/heir_cancel・insert 行無効化の postimage 閉集合 = 戸籍確認済 no＋
  取消済み yes）。yes→no の逆遷移例外も同経路のみ（正本 §3.4 は維持）。
- field 未追加（CU 前）・値が空 ⇒ **有効**（既存レコード互換・person_validity
  と同じ規約）。閉集合外の未知値 ⇒ **無効扱い**（安全側=拾わない・同規約）。
- H11a 監査（daily_healthcheck 監視項目I）は本 filter を**通さない**（設計上の
  例外・最終網）: 取消済み行が人手で yes 化された場合も検知対象に載せる
  （凍結票 §4.4「H11a が最終網」）。
"""

CANCELLED_FIELD = "取消済み"

CANCELLED_YES = "yes"
CANCELLED_NO = "no"
# 閉集合（config.EXPECTED_KINTONE_SCHEMA の required_options と一字一句一致・
# 変更は正本改定と同時のみ）
CANCELLED_VALUES = (CANCELLED_NO, CANCELLED_YES)


def cancelled_state(record: dict) -> str:
    """App36 レコードの 取消済み 値（未設定・空は ""）。"""
    return str((record.get(CANCELLED_FIELD) or {}).get("value") or "").strip()


def is_active_heir_row(record: dict) -> bool:
    """有効行か（取消済みでないか）。空・"no"=有効。"yes"・閉集合外=無効
    （安全側・person_validity と同規約）。"""
    state = cancelled_state(record)
    return state in ("", CANCELLED_NO)


def filter_active_heir_rows(records: list[dict]) -> list[dict]:
    """search 結果から取消済み行を除外する共通 filter（消費側の単一の正）。"""
    return [r for r in records if is_active_heir_row(r)]
