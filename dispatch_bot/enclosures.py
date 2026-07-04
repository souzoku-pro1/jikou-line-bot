"""送付案内の同封物選択（2026-07-04 実機エラー対応）

事象: D3 は同封物選択を空のまま起票し、prepare が
「同封物が選択されていません」でエラー遷移した（GUI起票は人間が✅を入れる前提だった）。

設計:
- 選択肢は App 32（同封物ブロックマスタ）の 有効=yes かつ対象ユニット一致の
  レコードから**動的に取得**する（ハードコード禁止）
- ユーザーに見せるのは 表示名・App 30 の 同封物選択 に入れるのは **ブロックキー**
  （App 30 のチェックボックス選択肢はブロックキーと同期している・設計 architecture/02 §4.2）
- 指示文に書類名が含まれる場合は 表示名/ブロックキー と照合できたもののみ採用
"""

from dataclasses import dataclass

from channels.soufu_annai import APP_ENCLOSURE
from hub import kintone

MSG_NO_OPTIONS = ("App 32（同封物ブロックマスタ）に有効な同封物が登録されていません。"
                  "App 32 にブロックを登録するか、kintone から直接起票してください")


@dataclass(frozen=True)
class EnclosureOption:
    key: str    # ブロックキー（App 30 同封物選択 に入れる値）
    label: str  # 表示名（LINE の選択肢・復唱に出す値）


async def list_options(unit: str) -> list[EnclosureOption]:
    """App 32 の有効ブロック（対象ユニット一致）を表示順で返す"""
    records = await kintone.search_records(
        APP_ENCLOSURE, '有効 in ("yes") order by 表示順 asc')
    options = []
    for r in records:
        if unit not in (r.get("対象ユニット", {}).get("value") or []):
            continue
        key = (r.get("ブロックキー", {}).get("value") or "").strip()
        if not key:
            continue
        label = (r.get("表示名", {}).get("value") or "").strip() or key
        options.append(EnclosureOption(key=key, label=label))
    return options


def match_names(names: list | None, options: list[EnclosureOption]) -> list[EnclosureOption]:
    """指示文から抽出された書類名を App 32 の 表示名/ブロックキー と照合。
    照合できたもののみ採用（モデルの創作名は落ちる）。重複は除去"""
    lookup = {}
    for o in options:
        lookup[o.label] = o
        lookup[o.key] = o
    matched, seen = [], set()
    for name in names or []:
        o = lookup.get(str(name).strip())
        if o and o.key not in seen:
            matched.append(o)
            seen.add(o.key)
    return matched


def format_question(options: list[EnclosureOption]) -> str:
    """番号選択式の聞き返し（選択肢は App 32 から動的・ハードコードしない）"""
    lines = ["同封する書類を番号で選んでください（複数可・カンマ区切り）"]
    lines += [f"{i}. {o.label}" for i, o in enumerate(options, 1)]
    return "\n".join(lines)
