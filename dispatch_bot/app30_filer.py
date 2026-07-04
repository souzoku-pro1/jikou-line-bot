"""App 30（発送管理）への起票（D3・送付案内）

設計: docs/dispatch-bot/06-confirmation-and-safety.md §3.3・03 §6・05 §3.1

- 起票のみ行う（prepare は既存の App 30 Webhook → /hub/dispatch が担う。
  発送ステータスは「下書き」で作成し、それより先へ進めるコードは書かない＝
  既存の状態機械・承認原則に一切干渉しない）
- 二重実行防止の第2層: 起票直前に同じ pending_command_id のレコードが
  既に存在しないかを検索する（プロセス並行・LINE再送の最終防衛線・06 §3.3）
- チャネル固有データに指示Bot由来メタ（指示原文・userId・解釈日時・
  pending_command_id）を残す（恒久ログ・監査・02 §6）
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from dispatch_bot.case_search import APP_CASE
from dispatch_bot.confirm import Pending
from hub import kintone

logger = logging.getLogger("dispatch_bot.app30_filer")

_JST = timezone(timedelta(hours=9))

APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")


def record_url(record_id: str) -> str:
    sub = os.environ.get("KINTONE_SUBDOMAIN", "")
    app_id = os.environ.get("APP_SHIPPING", "")
    return f"https://{sub}.cybozu.com/k/{app_id}/show#record={record_id}"


async def find_existing(command_id: str) -> str | None:
    """同一 pending_command_id の起票済みレコード検索（二重実行防止の第2層）"""
    records = await kintone.search_records(
        APP_SHIPPING, f'チャネル固有データ like "{command_id}"', fields=["$id"])
    if records:
        return str(records[0].get("$id", {}).get("value", ""))
    return None


async def file_soufu_annai(pending: Pending) -> tuple[str, str, bool]:
    """送付案内を App 30 に「下書き」で起票する。

    Returns: (record_id, record_url, already_filed)
    already_filed=True は二重実行ガードで既存レコードを検出した場合（新規作成なし）
    """
    existing = await find_existing(pending.command_id)
    if existing:
        logger.warning("[DISPATCHBOT] duplicate filing blocked cmd=%s -> No.%s",
                       pending.command_id[:8], existing)
        return existing, record_url(existing), True

    # 宛先は App 21 の案件データから（05 §3.1: 宛先は案件から解決）
    case_rec = await kintone.get_record(APP_CASE, pending.case.record_id)
    customer = case_rec.get("顧客名", {}).get("value", "") or pending.case.customer_name

    meta = {"dispatch_bot": {
        "指示原文": pending.instruction_text,
        "userId": pending.user_id,
        "解釈日時": datetime.now(_JST).isoformat(timespec="seconds"),
        "pending_command_id": pending.command_id,
    }}
    fields = {
        "発送ステータス": "下書き",
        "チャネル": "送付案内",
        "ユニット種別": pending.case.unit,
        "件名": f"送付案内（{customer}）",
        "顧客名表示用": customer,
        "宛先名": customer,
        "宛先郵便番号": case_rec.get("郵便番号", {}).get("value", ""),
        "宛先住所": case_rec.get("住所", {}).get("value", ""),
        "案件アプリID": os.environ.get("KINTONE_APP_ID", ""),
        "案件レコードID": pending.case.record_id,
        "実行済み": "no",
        "チャネル固有データ": json.dumps(meta, ensure_ascii=False),
    }
    ids = await kintone.create_records(APP_SHIPPING, [fields])
    rid = str(ids[0])
    logger.info("[DISPATCHBOT] filed App30 No.%s cmd=%s", rid, pending.command_id[:8])
    return rid, record_url(rid), False
