"""LINE指示Bot 入口（D1: /webhook/dispatch-bot・署名検証・ホワイトリスト）

設計: docs/dispatch-bot/02-line-entry-and-auth.md

- 専用LINE公式アカウント（顧客Botとも通知チャネルとも別・設計 01 §5）
- 署名検証は DISPATCHBOT_CHANNEL_SECRET（顧客Botの LINE_CHANNEL_SECRET とは別）。
  未設定なら常に検証失敗（=エンドポイント事実上無効）
- ホワイトリスト DISPATCHBOT_ALLOWED_USER_IDS（カンマ区切り）。
  未設定・空は **deny-all**（hub/webhook_auth の「env未設定=全拒否」と同思想）
- ホワイトリスト外: 応答を返さず沈黙 + 管理者LINE警報（スロットル付き）。
  /hub/dispatch の「token無しは404で存在しないフリ」と同じ防御思想
- 即200 + BackgroundTasks（LINE 2秒タイムアウト対策・既存流儀）
- ログは App 28（顧客チャットログ）に書かない。D1 時点では Railway ログのみ
  （[DISPATCHBOT] プレフィックス・設計 02 §6）
"""

import base64
import hashlib
import hmac
import json
import logging
import os

import httpx
from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from hub import notify
from hub.redact import emit


logger = logging.getLogger("dispatch_bot.router")

router = APIRouter()


def verify_line_signature(body: bytes, signature: str) -> bool:
    """X-Line-Signature の HMAC-SHA256 検証（指示Bot専用 secret）。
    secret 未設定は常に False（deny-all・設計 02 §2）"""
    secret = os.environ.get("DISPATCHBOT_CHANNEL_SECRET", "")
    if not secret or not signature:
        return False
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode("utf-8"), signature)


def allowed_user_ids() -> frozenset[str]:
    """ホワイトリスト（カンマ区切り env）。未設定・空 = 全拒否"""
    raw = os.environ.get("DISPATCHBOT_ALLOWED_USER_IDS", "")
    return frozenset(u.strip() for u in raw.split(",") if u.strip())


def is_allowed(user_id: str) -> bool:
    return bool(user_id) and user_id in allowed_user_ids()


async def _send_reply(reply_token: str, user_id: str, text: str) -> None:
    """指示Bot名義の返信（reply 失敗時は push フォールバック・既存流儀）。
    トークンは DISPATCHBOT_CHANNEL_ACCESS_TOKEN（顧客Botのトークンは使わない）"""
    token = os.environ.get("DISPATCHBOT_CHANNEL_ACCESS_TOKEN", "")
    if not token:
        logger.error("[DISPATCHBOT] ERROR: DISPATCHBOT_CHANNEL_ACCESS_TOKEN 未設定のため返信不可")
        return
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            "https://api.line.me/v2/bot/message/reply",
            headers=headers,
            json={"replyToken": reply_token,
                  "messages": [{"type": "text", "text": text}]},
        )
        if resp.status_code == 200:
            return
        logger.info("[DISPATCHBOT] reply failed (%s), falling back to push",
                    emit(resp.status_code, "count", "log", "operator"))
        await client.post(
            "https://api.line.me/v2/bot/message/push",
            headers=headers,
            json={"to": user_id,
                  "messages": [{"type": "text", "text": text}]},
        )


async def _alert_unauthorized(user_id: str, text: str) -> None:
    """ホワイトリスト外からの入力: 本人には沈黙・管理者に警報（設計 02 §4）"""
    await notify.notify_admin_line(
        "【指示Bot: 許可外ユーザーからの入力】\n"
        f"userId: {user_id[:10]}...\n"
        f"本文: {text[:50]}\n"
        "→ 応答は返していません。心当たりがある場合は\n"
        "   DISPATCHBOT_ALLOWED_USER_IDS への追加を検討してください。",
        throttle_key=f"dispatchbot_unauthorized:{user_id}",
    )


async def process_dispatch_bot_event(reply_token: str, user_id: str, user_text: str) -> None:
    """メッセージイベントの本処理（BackgroundTasks で実行）"""
    try:
        if not is_allowed(user_id):
            # 沈黙（reply も push もしない）＋管理者警報のみ
            logger.info("[DISPATCHBOT] unauthorized userId=%s...",
                        emit(user_id[:10], "record_id", "log", "operator"))
            await _alert_unauthorized(user_id, user_text)
            return

        logger.info("[DISPATCHBOT] message userId=%s... text=%s",
                    emit(user_id[:10], "record_id", "log", "operator"),
                    emit(user_text[:50], "freetext", "log", "operator"))
        # D2: 解析→案件検索→解釈結果の提示（復唱確認・起票は D3）
        from dispatch_bot.handler import handle_message
        reply_text = await handle_message(user_id, user_text)
        if reply_text:
            await _send_reply(reply_token, user_id, reply_text)
    except Exception:
        import traceback
        logger.error("[DISPATCHBOT] ERROR: process failed userId=%s...:",
                     emit(user_id[:10], "record_id", "log", "operator"))
        logger.error("%s", emit(traceback.format_exc(), "vendor_raw", "log", "operator"))


async def _process_follow_event(user_id: str) -> None:
    """友だち追加: ホワイトリスト外なら沈黙＋警報のみ（設計 02 §4）"""
    if not is_allowed(user_id):
        await _alert_unauthorized(user_id, "（友だち追加イベント）")
        return
    logger.info("[DISPATCHBOT] follow userId=%s... (allowed)",
                emit(user_id[:10], "record_id", "log", "operator"))


@router.post("/webhook/dispatch-bot")
async def dispatch_bot_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")
    if not verify_line_signature(body, signature):
        # LINEプラットフォーム以外からの偽装（既存 /webhook と同じ 400）
        raise HTTPException(status_code=400, detail="Invalid signature")

    data = json.loads(body)
    for event in data.get("events", []):
        user_id = (event.get("source") or {}).get("userId", "")
        if event.get("type") == "follow":
            background_tasks.add_task(_process_follow_event, user_id)
            continue
        if event.get("type") != "message":
            continue
        if (event.get("message") or {}).get("type") != "text":
            continue
        background_tasks.add_task(
            process_dispatch_bot_event,
            event.get("replyToken", ""), user_id, event["message"].get("text", ""),
        )
    return {"status": "ok"}
