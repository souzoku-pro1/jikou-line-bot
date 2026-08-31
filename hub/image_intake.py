"""画像受信の複数枚まとめ受領返信 — IMAGE-INTAKE-1（両チャネル・AI読解なし）

スコープ（票裁定）:
- 画像バイナリの取得（LINE コンテンツ API）と kintone 添付は**保留**——
  App 21 / App 40 の FILE 欄を form fields API で実測した結果、受信書類写真
  用の適切な欄が存在しない（全欄が特定書類の出力/収納スロット）。
  欄の追加は大野専権（CU）＝不足フィールドとして報告済み。追加後の小票で
  本 module に取得+添付を実装する（AI 読解は IMG-2）。
- 本票は「複数枚まとめ受領返信（デバウンス）」のみ。

束ね方式（IMAGE-INTAKE-SURVEY 提言 3 の導出型）:
1. 受信ごとの App 28 マーカー保存（category=画像受領:{event_id}・冪等キー）は
   従来どおり（時効側=main.py の strict 保存・相続放棄側=本 module）。
2. 返信は DEBOUNCE_SEC 待つ。in-memory 予約 _pending（H4-fix2 の
   _notify_in_flight と同型: 単一イベントループ前提・予約の確認と更新は
   **await を挟まない同期区間**で行う）で新着受信に代表を譲る。
3. 代表候補は App 28 照会（自分のマーカーがそのユーザーの最新受領行か）で
   確定してから push 1 通（reply token は待ち合わせで失効するため不使用）。
4. fail-safe（無返信を作らない）: in-memory 予約の消失（再起動相当）=
   個別返信に縮退・App 28 照会失敗=個別返信に縮退。束ね損ねは重複受領文の
   み＝安全側。

弁護士決定（凍結）: 受領文言は両チャネルとも chat_responder.IMAGE_RECEIPT_REPLY
（「書類のお写真を受領いたしました。弁護士が確認のうえご連絡いたします。」）
を使用。枚数の文言追加はしない。
"""

import asyncio
import logging
import os

from chat_responder import (IMAGE_INBOUND_MARKER, IMAGE_RECEIPT_REPLY,
                            save_to_chatlog)
from hub import kintone
from hub import notify
from hub.autoreply_stoplist import is_suppressed
from hub.line_channel import HOUKI_CHANNEL, push_text
from hub.redact import emit

logger = logging.getLogger("hub.image_intake")

# デバウンス秒数（実装判断・完了報告に明記）: LINE の複数枚送信は通常
# 数秒〜数十秒間隔で届く。90 秒は「ゆっくり撮り直しながらの連続送信」を
# 概ね 1 束に収めつつ、受領応答の遅延として許容できる上限として採用
DEBOUNCE_SEC = 90

_APP_CHATLOG = kintone.KintoneApp(
    "App 28 (チャットログ)", "APP_CHATLOG", "TOKEN_CHATLOG")
_MARKER_PREFIX = "画像受領:"

# 束ね予約: "channel:userId" → 最新受信のマーカー行 ID（単一イベントループ
# 前提の in-memory。消失時は個別返信へ縮退＝無返信にはならない）
_pending: dict[str, str] = {}


def _key(channel_name: str, user_id: str) -> str:
    return f"{channel_name}:{user_id}"


async def latest_marker_row_id(user_id: str) -> str:
    """そのユーザーの最新の画像受領マーカー行 ID（App 28 が永続正本）。"""
    rows = await kintone.search_records(
        _APP_CHATLOG,
        f'line_user_id = "{user_id}" and category like "{_MARKER_PREFIX}" '
        "order by $id desc limit 1",
        fields=["$id"])
    if not rows:
        return ""
    return str(((rows[0].get("$id") or {}).get("value")) or "")


async def debounce_and_elect(channel_name: str, user_id: str,
                             my_row_id: str, latest_row_query) -> bool:
    """デバウンス待ち→代表選出。True=返信する（代表 or 縮退の個別返信）・
    False=沈黙（より新しい受信のタスクが代表して返信する）。

    - 予約の登録・照合・解除は await を挟まない同期区間（H4-fix2 同型）＝
      並行受信でも代表は 1 タスクに絞られる
    - 予約消失（再起動相当）・照会失敗は**返信する側**へ倒す（fail-safe:
      無返信を作らない。最悪は束ね損ねの個別返信）
    """
    key = _key(channel_name, user_id)
    my = str(my_row_id)
    _pending[key] = my                       # 登録（同期区間）
    await asyncio.sleep(DEBOUNCE_SEC)
    cur = _pending.get(key)
    if cur is None:
        # in-memory 状態消失（再起動相当・別代表の確定後）=個別返信に縮退
        logger.info("[IMAGE_INTAKE] pending state lost "
                    "(degraded to individual reply)")
        return True
    if cur != my:
        return False                         # 新着が待機中=代表を譲る
    _pending.pop(key, None)                  # 自分の予約のみ解除（同期区間）
    try:
        latest = await latest_row_query()
    except Exception:
        logger.info("[IMAGE_INTAKE] latest-marker query failed "
                    "(degraded to individual reply)")
        return True                          # fail-safe
    return str(latest) == my                 # 永続正本（App 28）で代表確定


# ── 相続放棄チャネルの画像受信（受領返信の新設・IMAGE-INTAKE-1） ─────────────────
async def _alert_houki_image_failure(user_id: str, what: str) -> None:
    """要確認通知（固定文言+userId 先頭のみ・PII 非搭載）。"""
    await notify.notify_admin_line(
        f"【相続放棄・要確認】書類写真の{what}に失敗しました。"
        "LINE アプリで受信をご確認ください。\n"
        f"userId: {user_id[:10]}...",
        throttle_key=f"houki_image_failure:{user_id}",
    )


async def handle_houki_image(user_id: str, event_id: str) -> None:
    """相続放棄チャネルの画像受信（router から BackgroundTasks で実行）。

    時効側（main._process_line_image_event）と同じゲート順・同じ冪等設計:
    全業務ブレーキ→停止リスト→冪等 pre-check→マーカー strict 保存
    （保存不能=返信しない fail-closed+要確認通知）→並行配送の勝者決定
    （最小 $id）→束ね選出→push 受領返信+assistant 記録。
    管理者への受信通知は router 側の既存 _record_inbound（300 秒スロットル）を
    維持し、本関数では送らない。"""
    if not event_id:
        logger.warning("[IMAGE_INTAKE] houki image without event id (skip)")
        return
    if os.environ.get("AUTOREPLY_PAUSED") == "1":
        logger.info("[IMAGE_INTAKE] paused (global brake) userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
        return
    if await is_suppressed(user_id):
        logger.info("[IMAGE_INTAKE] suppressed (stoplist) userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
        return

    idem_key = _MARKER_PREFIX + event_id
    # 冪等 pre-check（失敗は続行に倒してよい——返信の重複は strict 保存+勝者
    # 決定が防ぐ。時効側 fix1[03] と同型）
    try:
        rows = await kintone.search_records(
            _APP_CHATLOG, f'category = "{idem_key}" order by $id asc limit 1',
            fields=["$id"])
        if rows:
            logger.info("[IMAGE_INTAKE] duplicate delivery skipped")
            return
    except Exception:
        logger.info("[IMAGE_INTAKE] idempotency pre-check failed "
                    "(strict save still guards)")

    # マーカー strict 保存（保存を確約できなければ返信しない=fail-closed）
    try:
        my_id = str(await kintone.create_record(_APP_CHATLOG, {
            "line_user_id": user_id,
            "role": "user",
            "message": IMAGE_INBOUND_MARKER,
            "category": idem_key,
            "auto_sent": "no",
        }))
    except Exception:
        logger.error("[IMAGE_INTAKE] houki marker save failed (fixed reason)")
        await _alert_houki_image_failure(user_id, "受領記録")
        return

    # 並行 2 配送の勝者決定（同一冪等キー最小 $id・時効側 fix1[03] と同型）
    try:
        rows = await kintone.search_records(
            _APP_CHATLOG, f'category = "{idem_key}" order by $id asc limit 1',
            fields=["$id"])
    except Exception:
        logger.error("[IMAGE_INTAKE] winner query failed (no reply)")
        await _alert_houki_image_failure(user_id, "受領処理")
        return
    if not rows or str(((rows[0].get("$id") or {}).get("value")) or "") \
            != my_id:
        logger.info("[IMAGE_INTAKE] concurrent duplicate lost")
        return

    async def _latest():
        return await latest_marker_row_id(user_id)

    if not await debounce_and_elect("houki", user_id, my_id, _latest):
        logger.info("[IMAGE_INTAKE] bundled (superseded)")
        return
    await push_text(HOUKI_CHANNEL, user_id, IMAGE_RECEIPT_REPLY)
    await save_to_chatlog(user_id, "assistant", IMAGE_RECEIPT_REPLY,
                          "画像受領", "yes")
    logger.info("[IMAGE_INTAKE] houki receipt sent userId=%s...",
                emit(user_id[:10], "record_id", "log", "operator"))
