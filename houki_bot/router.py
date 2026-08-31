"""相続放棄 LINE 入口（SOUZOKU-HOUKI-H1: /webhook/souzoku-houki）

設計: docs/architecture/10-unit-02-souzoku-houki.md §10.1（webhook パス分離）
+ SOUZOKU-HOUKI-SURVEY 設計案（dispatch_bot/router.py 同型）。

- 相続放棄専用 LINE 公式アカウント（時効の顧客 Bot・指示Bot とも別チャネル）
- 署名検証は HOUKI_LINE_CHANNEL_SECRET（hub/line_channel.HOUKI_CHANNEL）。
  **secret 未設定は受け口自体を無効＝404**（存在しないフリ・fail-closed。
  /hub/dispatch の「token 無しは 404」と同じ防御思想。大野が env を投入する
  まで endpoint は外形上存在しない）
- fix1 [02]: secret が時効側と同値・access token が設定済みかつ時効側と
  同値の誤設定も受け口無効＝404（hub/line_channel.houki_channel_disabled_reason
  の固定語彙閉集合・時効側は通常動作継続）
- v1（H-1）の挙動は **deny-all 既定**: 受信イベントの検証・記録（Railway
  ログ・PII は emit 抑止）と管理者 LINE 通知のみ。**顧客への reply/push は
  一切行わない**（ヒアリング Bot は H-3 で載せる。それまで受信は人対応＝
  通知で大野に可視化する。通知は userId 単位で throttle）
- App 28 チャットログ・App 21 等 kintone への書き込みも v1 では行わない
  （記録の永続化は H-3 の設計に含める）
- 即 200 + BackgroundTasks（LINE 2 秒タイムアウト対策・既存流儀）
- 時効チャネルの資格情報（顧客 Bot の secret / access token env）は参照
  しない。deny-all（送信・HTTP・kintone/DB 書込の不在）は fix1 [01] で
  AST checker（test_houki_bot_policy.py: import 閉集合・notify 許可属性
  =notify_admin_line のみ・動的アクセス遮断）が構造的に固定する
"""

import json
import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from hub import image_intake
from hub import notify
from hub.line_channel import (HOUKI_CHANNEL, houki_channel_disabled_reason,
                              verify_line_signature)
from hub.redact import emit
from houki_bot.hearing import handle_houki_hearing

logger = logging.getLogger("houki_bot.router")

router = APIRouter()

# 受信イベント種別（固定語彙・通知/ログにこのまま載せる。PII なし）
_KIND_TEXT = "テキスト"
_KIND_IMAGE = "画像"
_KIND_OTHER_MESSAGE = "その他メッセージ"
_KIND_FOLLOW = "友だち追加"


def _event_kind(event: dict) -> str | None:
    """通知対象イベントの種別（対象外は None＝無視）。"""
    etype = event.get("type")
    if etype == "follow":
        return _KIND_FOLLOW
    if etype != "message":
        return None
    mtype = (event.get("message") or {}).get("type")
    if mtype == "text":
        return _KIND_TEXT
    if mtype == "image":
        return _KIND_IMAGE
    return _KIND_OTHER_MESSAGE


async def _record_inbound(user_id: str, kind: str) -> None:
    """v1 の受信処理: 記録（ログ）+ 管理者通知のみ。顧客への送信は行わない。

    通知本文は固定文言+種別+userId 先頭 10 文字（顧客メッセージ本文は
    載せない＝dispatch_bot の警報より狭い。相続放棄の顧客本文は PII のため）。"""
    # sink 規律: kind は固定語彙のため分岐で**固定文言**として出し、
    # 可変値は emit の直接呼び出しのみを logger 引数に渡す
    if kind == _KIND_TEXT:
        logger.info("[HOUKI] inbound kind=text userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
    elif kind == _KIND_IMAGE:
        logger.info("[HOUKI] inbound kind=image userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
    elif kind == _KIND_FOLLOW:
        logger.info("[HOUKI] inbound kind=follow userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
    else:
        logger.info("[HOUKI] inbound kind=other_message userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
    await notify.notify_admin_line(
        "【相続放棄LINE】新チャネルで受信がありました\n"
        f"種別: {kind}\n"
        f"userId: {user_id[:10]}...\n"
        "（H-1: 自動応答なし・要人対応。返信は LINE 公式アカウントの"
        "管理画面から行ってください）",
        throttle_key=f"houki_inbound:{user_id}",
    )


@router.post("/webhook/souzoku-houki")
async def houki_webhook(request: Request, background_tasks: BackgroundTasks):
    # fail-closed: secret 未設定・時効側資格情報との同値（fix1 [02]）＝
    # 受け口自体を無効（404・存在しないフリ）。理由の閉集合は
    # hub/line_channel.houki_channel_disabled_reason が単一の正
    reason = houki_channel_disabled_reason()
    if reason is not None:
        if reason != "secret_unset":
            # 誤設定のみ固定文言で警告（未設定=点火前の既定状態は無音）
            logger.warning(
                "[HOUKI] endpoint disabled (channel credential misconfig)")
        raise HTTPException(status_code=404, detail="not found")

    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")
    if not verify_line_signature(HOUKI_CHANNEL, body, signature):
        # LINE プラットフォーム以外からの偽装（既存 /webhook と同じ 400）
        raise HTTPException(status_code=400, detail="Invalid signature")

    data = json.loads(body)
    for event in data.get("events", []):
        kind = _event_kind(event)
        if kind is None:
            continue
        user_id = (event.get("source") or {}).get("userId", "")
        # SOUZOKU-HOUKI-H3: テキストはヒアリング会話へ（deny-all を置換）。
        # 画像・友だち追加・その他メッセージは H-1 の記録+管理者通知のまま
        # （画像 AI 判断はさせない・時効の要件4と同じ原則）
        if kind == _KIND_TEXT:
            background_tasks.add_task(
                handle_houki_hearing,
                event.get("replyToken", ""), user_id,
                (event.get("message") or {}).get("text", ""))
            continue
        background_tasks.add_task(_record_inbound, user_id, kind)
        if kind == _KIND_IMAGE:
            # IMAGE-INTAKE-1: 受領返信（束ね方式）を追加。既存の管理者通知
            # （_record_inbound・300 秒スロットル）は維持。event id 不明は
            # 冪等キーが作れないため受領返信なし（通知のみ）
            image_event_id = event.get("webhookEventId") or (
                (event.get("message") or {}).get("id", ""))
            background_tasks.add_task(
                image_intake.handle_houki_image, user_id, image_event_id)
    return {"status": "ok"}
