"""案件検索（D2: App 21 時効援用案件を顧客名で横断検索）

設計: docs/dispatch-bot/03-natural-language-parser.md §4
- 検索対象はタスクレジストリの search_apps（第1弾はすべて App 21）
- ヒット0件なら「空白除去 or 姓のみ」での再検索を1回だけ行う
- 完了・不受任の案件も候補に含める（⚠付き。除外はしない・03 §4.3）
"""

import logging
from dataclasses import dataclass

from hub import kintone
from hub.redact import emit


logger = logging.getLogger("dispatch_bot.case_search")

APP_CASE = kintone.KintoneApp("App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")

# 起票の妥当性に疑義がある status（候補には出すが⚠を付ける）
_WARN_STATUSES = ("完了", "不受任")


@dataclass
class CaseHit:
    record_id: str
    customer_name: str
    status: str
    unit: str = "時効援用"  # App 21 は時効援用専用（App 33 実装後にユニット列対応）

    @property
    def warn(self) -> bool:
        return self.status in _WARN_STATUSES

    def label(self) -> str:
        base = f"{self.customer_name}（No.{self.record_id}・{self.status}・{self.unit}）"
        return f"⚠ {base}" if self.warn else base


def _escape(name: str) -> str:
    return name.replace('"', '\\"')


async def _search(name: str) -> list[CaseHit]:
    records = await kintone.search_records(
        APP_CASE,
        f'顧客名 like "{_escape(name)}"',
        fields=["$id", "顧客名", "status"],
    )
    return [CaseHit(
        record_id=str(r.get("$id", {}).get("value", "")),
        customer_name=r.get("顧客名", {}).get("value", ""),
        status=r.get("status", {}).get("value", ""),
    ) for r in records]


async def search_cases(customer_name: str) -> list[CaseHit]:
    """顧客名で案件を検索。0件なら空白除去→姓のみ の順で1回だけ再検索（03 §4.1）"""
    name = (customer_name or "").strip()
    if not name:
        return []
    hits = await _search(name)
    if hits:
        return hits

    # 再検索は1回だけ: 空白（全角含む）除去で変わればそれ、変わらなければ姓のみ
    compact = name.replace(" ", "").replace("　", "")
    retry = compact if compact != name else name.split()[0] if " " in name else \
            name.split("　")[0] if "　" in name else ""
    if retry and retry != name:
        logger.info("[DISPATCHBOT] case retry search %s -> %s",
                    emit(name, "name", "log", "operator"),
                    emit(retry, "name", "log", "operator"))
        return await _search(retry)
    return []


def format_choices(hits: list[CaseHit], customer_name: str) -> str:
    """複数候補の番号付き選択肢（03 §4.2）"""
    lines = [f"「{customer_name}」の案件が{len(hits)}件あります。番号で選んでください:"]
    lines += [f"{i}. {h.label()}" for i, h in enumerate(hits, 1)]
    return "\n".join(lines)


NOT_FOUND_MESSAGE = ("該当する案件が見つかりません。氏名の表記を変えて言い直すか、"
                     "kintoneのレコード番号（例: No.45）で指示してください")
