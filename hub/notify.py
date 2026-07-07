"""LINE 通知の一本化（hub/notify）

設計: docs/architecture/03-common-components.md §8

- notify_admin_line: claude_gateway から移設（T0-2）。スロットル実装・挙動は不変。
  既存の import 経路（from claude_gateway import notify_admin_line）は
  claude_gateway 側の re-export で維持される。
- push_line_message: LINE Push の共通実装（一本化の下回り）。
- notify_attorney_approval: 発送管理（App 30）の承認依頼通知（T1-2 で追加）。

呼び出し元の警報文言はここでは持たない（文言は呼び出し元の責務）。
"""

import logging
import os
import time

import httpx

from config import get_admin_line_user_id

logger = logging.getLogger("hub.notify")

# 管理者通知のスロットル（同種の連続障害で LINE を埋めないため・claude_gateway から移設）
_NOTIFY_MIN_INTERVAL_SEC = 300
_last_notify_at: dict[str, float] = {}

_PUSH_URL = "https://api.line.me/v2/bot/message/push"


def business_token_env() -> str:
    """業務通知（管理者警報・承認通知・受領通知）の送信チャネル（2026-07-07 裁定）。

    業務通知は業務指示Bot（DISPATCHBOT_CHANNEL_ACCESS_TOKEN）から送る。
    未設定の環境では既定（顧客Bot）へフォールバック＋警告ログ
    （警報の欠落防止を優先）。顧客向け送信はこの関数を使わない（現状のまま）。
    """
    if os.environ.get("DISPATCHBOT_CHANNEL_ACCESS_TOKEN", ""):
        return "DISPATCHBOT_CHANNEL_ACCESS_TOKEN"
    logger.warning(
        "DISPATCHBOT_CHANNEL_ACCESS_TOKEN unset; "
        "business notification falls back to LINE_CHANNEL_ACCESS_TOKEN")
    return "LINE_CHANNEL_ACCESS_TOKEN"


async def push_line_message(to: str, text: str,
                            token_env: str = "LINE_CHANNEL_ACCESS_TOKEN") -> bool:
    """LINE Push の共通実装。成功で True。失敗はログのみ（例外を送出しない）。

    token_env: 送信チャネルのアクセストークン env 名。既定は顧客Bot
    （LINE_CHANNEL_ACCESS_TOKEN）。業務指示Bot名義で送るときは
    DISPATCHBOT_CHANNEL_ACCESS_TOKEN を指定する（例: 仕分け照会通知）。
    """
    line_token = os.environ.get(token_env, "")
    if not (to and line_token):
        logger.warning("LINE push skipped (no destination or token)")
        return False
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                _PUSH_URL,
                headers={
                    "Authorization": f"Bearer {line_token}",
                    "Content-Type": "application/json",
                },
                json={"to": to, "messages": [{"type": "text", "text": text[:4900]}]},
            )
        if not resp.is_success:
            logger.error("LINE push failed: %s %s", resp.status_code, resp.text[:200])
            return False
        return True
    except Exception:
        logger.exception("LINE push error")
        return False


async def notify_admin_line(text: str, throttle_key: str = "") -> None:
    """管理者に LINE Push で通知する。失敗しても本処理には影響させない。

    claude_gateway から移設（挙動不変）:
      - 管理者 ID / トークン未設定ならスキップ（警告ログのみ）
      - throttle_key 指定時は同一キーの通知を _NOTIFY_MIN_INTERVAL_SEC 秒に1回へ抑制
      - 本文は 4900 文字で切り詰め
    """
    admin_id = get_admin_line_user_id()
    line_token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
    if not (admin_id and line_token):
        logger.warning("admin LINE notify skipped (no admin id or token): %s", text[:100])
        return

    if throttle_key:
        now = time.monotonic()
        last = _last_notify_at.get(throttle_key, 0.0)
        if now - last < _NOTIFY_MIN_INTERVAL_SEC:
            logger.info("admin LINE notify throttled key=%s", throttle_key)
            return
        _last_notify_at[throttle_key] = now

    # 業務通知は指示Botチャネルから（2026-07-07 裁定・14呼び出し元は無変更）
    await push_line_message(admin_id, text, token_env=business_token_env())


async def notify_attorney_approval(record: dict) -> None:
    """発送管理（App 30）の承認依頼を弁護士へ LINE Push する。
    既存 App 29 の「【承認依頼】」と同型（docs/architecture/03 §8）。"""
    attorney_id = os.environ.get("ATTORNEY_LINE_USER_ID", "")
    if not attorney_id:
        logger.warning("attorney approval notify skipped (ATTORNEY_LINE_USER_ID unset)")
        return
    record_id = record.get("$id", {}).get("value", "（不明）")
    text = (
        "【承認依頼】発送\n"
        f"件名: {record.get('件名', {}).get('value', '')}\n"
        f"チャネル: {record.get('チャネル', {}).get('value', '')} / "
        f"顧客: {record.get('顧客名表示用', {}).get('value', '')}\n"
        f"発送管理レコードNo: {record_id}\n"
        "kintone で成果物を確認し、発送ステータスを「承認済」に変更してください。"
    )
    # 業務通知は指示Botチャネルから（2026-07-07 裁定）
    await push_line_message(attorney_id, text, token_env=business_token_env())
