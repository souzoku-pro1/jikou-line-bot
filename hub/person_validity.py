"""App34（人物）の「有効行」定義の単一の正（RV-08 soft merge・裁定②(B)）

正本: docs/design-drafts/DRAFT_RV08_SOFT_MERGE.md §3.1・§4（RV08-03）。

- soft merge された敗者行は物理削除されず「統合状態=統合済み無効」で残置される
  （lineage=統合先人物ID・統合日時）。App34 を読む全 consumer はこのモジュールの
  filter を通して無効化行を読み飛ばす（**一点除外**・裁定②(B)。(B)→(A) 横展開への
  変更は設計改定＋司令塔再裁定を要する——実装票の単独判断で切り替えない）。
- 閉集合 MERGE_STATE_VALUES の増減は DRAFT 改定と同時のみ（テストで pin）。
- 統合状態フィールドが存在しない・値が空のレコードは有効扱い
  （CU 適用前の実機・既存レコードとの互換）。**閉集合外の未知値は無効扱い**
  （安全側・拾わない）。
- `get_record`（$id 直参照）経由は検索 filter が効かないため、取得後に
  is_active_person で状態を確認して無効化行なら要確認へ倒す（RV08-03 の規約）。
"""

from hub import kintone

APP_KOSEKI_PERSON = kintone.KintoneApp(
    "App 34 (人物)", "APP_KOSEKI_PERSON", "TOKEN_KOSEKI_PERSON")

# App34 無効化3フィールド（§3.1・裁定③=§3.1 案承認）
MERGE_STATE_FIELD = "統合状態"
MERGE_LINEAGE_FIELD = "統合先人物ID"
MERGE_DATETIME_FIELD = "統合日時"
# App34 復元操作ID（RV08-IMPL-05・裁定=専用フィールド方式）: 復元 CLI が create 時に
# 決定的 restore operation_id（SHA-256 hex・非PII）を本体保存する器。ACK 喪失後の
# 回収はこのフィールドの完全一致検索＝個体の決定的同定（氏名検索を廃止）
RESTORE_OPERATION_FIELD = "復元操作ID"

MERGE_STATE_ACTIVE = "有効"
MERGE_STATE_MERGED = "統合済み無効"
# 閉集合（値の増減は DRAFT_RV08 改定と同時のみ・test_rv08_soft_merge が pin）
MERGE_STATE_VALUES = (MERGE_STATE_ACTIVE, MERGE_STATE_MERGED)

# consumer が検索 fields へ足す読取フィールド（filter が状態を見られるように）
ACTIVE_FILTER_FIELDS = (MERGE_STATE_FIELD,)


def merge_state(record: dict) -> str:
    """レコードの統合状態の生値（フィールド不在・空は ""）"""
    return str((record.get(MERGE_STATE_FIELD) or {}).get("value") or "").strip()


def is_active_person(record: dict) -> bool:
    """有効行判定の単一の正。空/フィールド不在=有効（CU 適用前互換）・
    「有効」=有効・**閉集合外の未知値は無効扱い**（安全側）"""
    state = merge_state(record)
    return state in ("", MERGE_STATE_ACTIVE)


def filter_active_persons(records: list[dict]) -> list[dict]:
    """無効化行の一点除外（裁定②(B)）。順序は保持する"""
    return [r for r in records if is_active_person(r)]
