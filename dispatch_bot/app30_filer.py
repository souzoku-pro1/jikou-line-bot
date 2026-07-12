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
from hub.redact import emit


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


def _audit_meta(pending: Pending) -> dict:
    """指示Bot由来の監査メタ（チャネル固有データに併記・02 §6。
    prepare 側の書き戻しは hub/dispatch のマージにより本キーを保持する）"""
    return {"dispatch_bot": {
        "指示原文": pending.instruction_text,
        "userId": pending.user_id,
        "解釈日時": datetime.now(_JST).isoformat(timespec="seconds"),
        "pending_command_id": pending.command_id,
    }}


def _fields_soufu_annai(pending: Pending, case_rec: dict, customer: str) -> dict:
    return {
        "チャネル": "送付案内",
        "件名": f"送付案内（{customer}）",
        "宛先名": customer,
        "宛先郵便番号": case_rec.get("郵便番号", {}).get("value", ""),
        "宛先住所": case_rec.get("住所", {}).get("value", ""),
        # 同封物選択はブロックキーで設定（App 30 のチェックボックス選択肢は
        # App 32 のブロックキーと同期・architecture/02 §4.2。空だと prepare が
        # 「同封物が選択されていません」でエラーになるため必須・2026-07-04 修正）
        "同封物選択": pending.parsed.get("task_params", {}).get("enclosures") or [],
        "チャネル固有データ": json.dumps(_audit_meta(pending), ensure_ascii=False),
    }


def _fields_shokumu_seikyu(pending: Pending, case_rec: dict, customer: str) -> dict:
    """職務上請求（D4）: チャネル固有JSONは channels/shokumu_seikyu.parse_channel_data が
    通る形式（request_items/municipality/target/purpose・04 §2）＋監査メタ併記。
    宛先名は空で起票する（prepare が App 31 から施設名で解決して書き戻す）"""
    from dispatch_bot import shokumu
    channel_json = shokumu.build_channel_json(pending.parsed)
    return {
        "チャネル": "職務上請求",
        "件名": f"職務上請求（{customer}・{channel_json['municipality']}）",
        "宛先名": "",
        "宛先郵便番号": "",
        "宛先住所": "",
        "チャネル固有データ": json.dumps({**channel_json, **_audit_meta(pending)},
                                          ensure_ascii=False),
    }

_FIELDS_BY_TASK = {
    "soufu_annai": _fields_soufu_annai,
    "shokumu_seikyu": _fields_shokumu_seikyu,
}


async def file_from_pending(pending: Pending) -> tuple[str, str, bool]:
    """pending のタスク種別に応じて App 30 に「下書き」で起票する。

    Returns: (record_id, record_url, already_filed)
    already_filed=True は二重実行ガードで既存レコードを検出した場合（新規作成なし）
    """
    existing = await find_existing(pending.command_id)
    if existing:
        logger.info("[DISPATCHBOT] duplicate filing blocked cmd=%s -> No.%s",
                    emit(pending.command_id[:8], "record_id", "log", "operator"),
                    emit(existing, "record_id", "log", "operator"))
        return existing, record_url(existing), True

    # 宛先・顧客名は App 21 の案件データから（05 §3.1: 宛先は案件から解決）
    case_rec = await kintone.get_record(APP_CASE, pending.case.record_id)
    customer = case_rec.get("顧客名", {}).get("value", "") or pending.case.customer_name

    task_type = pending.parsed.get("task_type") or ""
    build = _FIELDS_BY_TASK[task_type]  # レジストリ登録タスクのみ到達（KeyErrorは実装漏れ）
    fields = {
        "発送ステータス": "下書き",
        "ユニット種別": pending.case.unit,
        "顧客名表示用": customer,
        "案件アプリID": os.environ.get("KINTONE_APP_ID", ""),
        "案件レコードID": pending.case.record_id,
        "実行済み": "no",
        **build(pending, case_rec, customer),
    }
    # ★単票API（POST /k/v1/record.json）で起票すること。
    # 一括API（records.json・create_records）は kintone 仕様で「レコード追加」Webhook が
    # 発射されず、/hub/dispatch → prepare が走らない（2026-07-04 実機不具合の原因。
    # サイボウズ 2025-03-21 障害告知でも一括APIは Webhook 非送信が正規仕様と示されている）
    rid = str(await kintone.create_record(APP_SHIPPING, fields))
    logger.info("[DISPATCHBOT] filed App30 No.%s cmd=%s",
                emit(rid, "record_id", "log", "operator"),
                emit(pending.command_id[:8], "record_id", "log", "operator"))
    return rid, record_url(rid), False
