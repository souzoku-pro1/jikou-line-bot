"""LINE 通知の一本化（hub/notify）

設計: docs/architecture/03-common-components.md §8

- notify_admin_line: claude_gateway から移設（T0-2）。スロットル実装・挙動は不変。
  既存の import 経路（from claude_gateway import notify_admin_line）は
  claude_gateway 側の re-export で維持される。
- push_line_message: LINE Push の共通実装（一本化の下回り）。
- notify_attorney_approval（発送管理の承認依頼通知）は T1-2 で追加する。

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


async def push_line_message(to: str, text: str) -> bool:
    """LINE Push の共通実装。成功で True。失敗はログのみ（例外を送出しない）。"""
    line_token = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
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

    await push_line_message(admin_id, text)
