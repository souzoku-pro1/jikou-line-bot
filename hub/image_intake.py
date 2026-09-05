"""画像受信の複数枚まとめ受領返信 — IMAGE-INTAKE-1（+fix1）

スコープ（票裁定）:
- 画像バイナリの取得（LINE コンテンツ API）と kintone 添付は**保留**——
  App 21 / App 40 の FILE 欄を form fields API で実測した結果、受信書類写真
  用の適切な欄が存在しない（全欄が特定書類の出力/収納スロット）。
  欄の追加は大野専権（CU）＝不足フィールドとして報告済み。追加後の小票で
  本 module に取得+添付を実装する（AI 読解は IMG-2）。
- 本票は「複数枚まとめ受領返信（デバウンス）」のみ。

fix1（R-IMAGE-INTAKE-01/02/03・統一設計）: 返信完了の正本を App 28 に置く
——H-4 の確立形（冪等キー=永続非空・送信 True 時のみ閉鎖・自己修復発火）と
同じ型。
- マーカー（user 行）: category = 画像受領:{channel}:{event_id}
  （fix1[02]: チャネル識別子込み。旧形式 画像受領:{event_id} の既存行は
  新クエリに一致しない=返信済みの既往として扱う）
- 受領返信済み（assistant 行）: category = 画像受領済:{channel}。
  **push 成功（True）を確認できたときだけ**保存する（fix1[03]）
- 未返信の永続判定: チャネル別の最新マーカー行 $id > 最新受領済み行 $id
  （受領済み行なしを含む）
- 自己修復発火（fix1[01]）: 同一ユーザーの次のイベント受信時
  （時効=テキスト worker・相続放棄=ヒアリング入口・画像は代表選出自体が
  回収）に heal_unreplied() が未返信を検知して受領返信を送る。起動時 sweep は
  実装しない=「次のイベントまで返信が出ない」残余は票報告に明記

束ね方式:
1. 受信ごとに App 28 マーカーを strict 保存（冪等キー）。
2. 返信は DEBOUNCE_SEC 待つ。in-memory 予約 _pending（H4-fix2 の
   _notify_in_flight と同型: 予約の確認・更新は await を挟まない同期区間）で
   新着受信に代表を譲る。
3. 代表候補は App 28 照会（チャネル別の最新受領行=自分）で確定してから
   push 1 通（reply token は待ち合わせで失効するため不使用）。
4. fail-safe: in-memory 予約の消失・照会失敗は個別返信側へ縮退（束ね損ね=
   重複受領文のみ・安全側）。push 失敗・受領済み行の保存失敗は「未返信」の
   まま残り、自己修復発火が回収する（無返信を恒久化しない）。

弁護士決定（凍結）: 受領文言は両チャネルとも chat_responder.IMAGE_RECEIPT_REPLY
を使用。枚数の文言追加はしない。
"""

import asyncio
import logging
import os

from chat_responder import IMAGE_INBOUND_MARKER, IMAGE_RECEIPT_REPLY
from hub import houki_case_store
from hub import image_analysis
from hub import image_store
from hub import kintone
from hub import notify
from hub.autoreply_stoplist import is_suppressed
from hub.line_channel import HOUKI_CHANNEL, push_text
from hub.redact import emit

logger = logging.getLogger("hub.image_intake")

# デバウンス秒数（実装判断・票報告に明記）: LINE の複数枚送信は通常
# 数秒〜数十秒間隔で届く。90 秒は「ゆっくり撮り直しながらの連続送信」を
# 概ね 1 束に収めつつ、受領応答の遅延として許容できる上限として採用
DEBOUNCE_SEC = 90

_APP_CHATLOG = kintone.KintoneApp(
    "App 28 (チャットログ)", "APP_CHATLOG", "TOKEN_CHATLOG")
_MARKER_PREFIX = "画像受領:"          # 実カテゴリ= 画像受領:{channel}:{event_id}
_RECEIPT_PREFIX = "画像受領済:"       # 実カテゴリ= 画像受領済:{channel}

# 束ね予約: "channel:userId" → 最新受信イベントの冪等キー（単一イベント
# ループ前提の in-memory。消失時は個別返信へ縮退＝無返信にはならない）
_pending: dict[str, str] = {}

# fix2[fix1-01]: 送信 claim（"channel:userId"・単一保持者）。代表経路と
# heal 経路は**同じ send_receipt_and_close を通る**ため、claim もここで共有
# される（片方だけ見る抜け道がない）。確認→取得は await を挟まない同期区間
# （H4-fix2 の _notify_in_flight / fix1 の _pending と同型）。
# 【単一 worker 前提（既存裁定）】conversation_histories・_notify_in_flight・
# _pending と同じく in-memory 排他は uvicorn workers=1（Procfile 実測・
# test_image_intake が pin）が前提。worker 複数化票では永続 CAS/一意キーに
# よる排他へ置換すること（既知の制約・司令塔裁定でスコープ外）
_send_claims: set[str] = set()


def marker_category(channel_name: str, event_id: str) -> str:
    return f"{_MARKER_PREFIX}{channel_name}:{event_id}"


def _receipt_category(channel_name: str) -> str:
    return f"{_RECEIPT_PREFIX}{channel_name}"


def _key(channel_name: str, user_id: str) -> str:
    return f"{channel_name}:{user_id}"


async def _latest_marker_row(channel_name: str,
                             user_id: str) -> tuple[str, str]:
    """そのユーザー×チャネルの最新の画像受領マーカー行 ($id, category)
    （App 28 が永続正本・fix1[02]: チャネル識別込みで判定する）。"""
    rows = await kintone.search_records(
        _APP_CHATLOG,
        f'line_user_id = "{user_id}" and '
        f'category like "{_MARKER_PREFIX}{channel_name}:" '
        "order by $id desc limit 1",
        fields=["$id", "category"])
    if not rows:
        return "", ""
    row = rows[0]
    return (str(((row.get("$id") or {}).get("value")) or ""),
            str(((row.get("category") or {}).get("value")) or ""))


async def latest_marker_row_id(channel_name: str, user_id: str) -> str:
    rid, _cat = await _latest_marker_row(channel_name, user_id)
    return rid


async def latest_marker_category(channel_name: str, user_id: str) -> str:
    """代表判定用: 最新マーカー行の category（=最新イベントの冪等キー）。
    同一イベントの並行二重配送は同じ category を共有するため、勝者決定
    （最小 $id）と代表判定（最新イベント）が矛盾しない（fix1 実装時の
    barrier 並行テストで発見した統合バグの根治）。"""
    _rid, cat = await _latest_marker_row(channel_name, user_id)
    return cat


async def _latest_receipt_row_id(channel_name: str, user_id: str) -> str:
    rows = await kintone.search_records(
        _APP_CHATLOG,
        f'line_user_id = "{user_id}" and '
        f'category = "{_receipt_category(channel_name)}" '
        "order by $id desc limit 1",
        fields=["$id"])
    if not rows:
        return ""
    return str(((rows[0].get("$id") or {}).get("value")) or "")


async def debounce_and_elect(channel_name: str, user_id: str,
                             my_token: str, latest_token_query) -> bool:
    """デバウンス待ち→代表選出。True=返信する（代表 or 縮退の個別返信）・
    False=沈黙（より新しい受信のタスクが代表して返信する）。

    - 予約の登録・照合・解除は await を挟まない同期区間（H4-fix2 同型）＝
      並行受信でも代表は 1 タスクに絞られる
    - 予約消失（再起動相当）・照会失敗は**返信する側**へ倒す（fail-safe:
      無返信を作らない。最悪は束ね損ねの個別返信）
    """
    key = _key(channel_name, user_id)
    my = str(my_token)
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
        latest = await latest_token_query()
    except Exception:
        logger.info("[IMAGE_INTAKE] latest-marker query failed "
                    "(degraded to individual reply)")
        return True                          # fail-safe
    # token=マーカー category＝イベント冪等キー（行 ID 比較だと同一
    # イベントの二重配送行が勝者を「非最新」に見せて無返信になる）
    return str(latest) == my                 # 永続正本（App 28）で代表確定


async def send_receipt_and_close(channel_name: str, channel,
                                 user_id: str) -> bool | None:
    """受領返信の送信+閉鎖（_send_receipt_and_close）。JIKOU-IMG-2: 時効
    チャネルで True（送信+閉鎖成功）のときだけ、claim 解放後に 2 通目
    （書類写真の AI 読解→債権者確認+未回答質問）を続ける。IMG-1 の受領返信・
    heal・claim の構造は不変。2 通目の失敗は握る（受領返信の結果を変えない）。"""
    result = await _send_receipt_and_close(channel_name, channel, user_id)
    if result is True and channel_name == "jikou":
        try:
            # event_id は最新マーカー行の category（画像受領:jikou:{event_id}）
            cat = await latest_marker_category("jikou", user_id)
            event_id = cat.rsplit(":", 1)[-1] if cat else ""
            if event_id:
                await image_analysis.analyze_and_reply(user_id, event_id)
        except Exception:
            logger.error("[IMAGE_INTAKE] image analysis hook failed "
                         "(fixed reason)")
    return result


async def _send_receipt_and_close(channel_name: str, channel,
                                  user_id: str) -> bool | None:
    """受領返信を push し、**成功（True）を確認できたときだけ**受領済み行を
    保存して冪等を閉じる（fix1[03]・H-4 の「通知 True 時のみ書込」と同型）。

    戻り値（fix2[fix1-01]）:
      True  = 送信+閉鎖に成功
      None  = 送信不要（claim を他タスクが保持中／永続再確認で返信済み・
              マーカーなし）＝呼び出し側は沈黙してよい（失敗ではない）
      False = 送信失敗（非 2xx・通信例外・受領済み行の保存失敗）＝未返信の
              まま（heal が回収）。fix3: 要確認通知は**本関数内で発火済み**
              （呼び出し側は通知しない=二重通知防止）。None は「閉鎖済み」と
              「他タスクが claim 保持中」を区別しない——どちらも呼び出し側の
              正しい動作は沈黙であり、失敗時の通知保証は保持者側の False
              経路が担うため区別は不要

    fix2[fix1-01]: 送信 claim——確認→取得は await を挟まない同期区間で行い、
    push 前に単一保持者へ閉じる（代表経路と heal 経路の共有関門）。claim は
    try/finally で**全終了経路（成功・失敗・例外）**で解放される。claim 取得
    後に永続正本（App 28）で未返信を再確認する＝並行相手が閉じ切った後の
    後追い送信（claim 解放後の TOCTOU）も遮断。"""
    key = _key(channel_name, user_id)
    if key in _send_claims:                  # 確認→取得（同期区間・awaitなし）
        logger.info("[IMAGE_INTAKE] send claim held elsewhere (skip)")
        return None
    _send_claims.add(key)
    try:
        # claim 取得後の永続再確認（未返信でなければ送らない）
        marker = await latest_marker_row_id(channel_name, user_id)
        if not marker:
            return None
        receipt = await _latest_receipt_row_id(channel_name, user_id)
        if receipt and int(receipt) > int(marker):
            return None                      # 既に閉鎖済み（重複送信の遮断）
        try:
            sent = await push_text(channel, user_id, IMAGE_RECEIPT_REPLY)
        except Exception:
            logger.error("[IMAGE_INTAKE] receipt push transport error "
                         "(stays unreplied)")
            await _notify_send_failure(channel_name, user_id)
            return False
        if sent is not True:
            logger.error("[IMAGE_INTAKE] receipt push rejected "
                         "(stays unreplied)")
            await _notify_send_failure(channel_name, user_id)
            return False
        try:
            await kintone.create_record(_APP_CHATLOG, {
                "line_user_id": user_id,
                "role": "assistant",
                "message": IMAGE_RECEIPT_REPLY,
                "category": _receipt_category(channel_name),
                "auto_sent": "yes",
            })
        except Exception:
            # 送信は済んでいる。閉鎖に失敗＝未返信のまま→自己修復で再送
            # （at-least-once・重複受領文は許容）
            logger.error("[IMAGE_INTAKE] receipt record save failed "
                         "(will re-fire)")
            await _notify_send_failure(channel_name, user_id)
            return False
        return True
    finally:
        _send_claims.discard(key)            # 全終了経路で解放（リークなし）


async def heal_unreplied(channel_name: str, channel, user_id: str) -> bool:
    """自己修復発火（fix1[01]）: 未返信（最新マーカー行 > 最新受領済み行）を
    検知したら受領返信を送って閉じる。送ったら True。

    - _pending に予約がある間は生きた待機タスクに任せる（早すぎる送信をしない）
    - 照会失敗・送信失敗は静かに次のイベントへ持ち越す（fail-open・
      毎イベント再試行になるため通知はしない=スパム防止。代表経路の失敗
      通知は呼び出し側が担う）
    - 例外は外へ出さない（テキスト会話・ヒアリングを道連れにしない）
    """
    # テスト既定無効の env ゲート（conftest が IMAGE_HEAL_DISABLED=1 を
    # setdefault・KOSEKI_READER_DISABLED と同区分）: 本発火は全テキスト受信の
    # 入口に配線されるため、これを知らない既存テストから実 kintone へ到達
    # し得る。本番は env 未設定＝有効
    if os.environ.get("IMAGE_HEAL_DISABLED") == "1":
        return False
    try:
        if _key(channel_name, user_id) in _pending:
            return False                     # 生きた待機タスクが回収する
        # 未返信判定は send_receipt_and_close の claim 取得後の永続再確認に
        # 集約（fix2: 判定と送信の間に await の隙間を作らない）。返信済み・
        # マーカーなし・claim 保持中は None（送信不要）が返る
        result = await send_receipt_and_close(channel_name, channel, user_id)
        if result is True:
            logger.info("[IMAGE_INTAKE] unreplied marker healed")
        return result is True
    except Exception:
        logger.info("[IMAGE_INTAKE] heal check failed (retry on next event)")
        return False


# ── 相続放棄チャネルの画像受信（受領返信の新設・IMAGE-INTAKE-1） ─────────────────
async def _notify_send_failure(channel_name: str, user_id: str) -> None:
    """fix3[fix2-01]: 受領返信の失敗通知を**関門（send_receipt_and_close）内**へ
    一元化——保持者が代表でも heal でも、False 確定のその場で必ず 1 回発火する
    （claim 競合で後発が None 終了しても「通知 0 回」にならない）。

    重複制御（実装判断）:
    - houki: notify_admin_line + throttle_key（userId 別・300 秒）+
      **throttle_on_success_only=True**（H4-fix1 整合: 通知自体の送信失敗が
      interval を占有して「1 回も出ない」状態を作らない。成功後 300 秒は
      同一ユーザーの連続失敗を集約）
    - jikou: notify_business（弁護士宛の既存流儀）にはスロットル機構がない
      ため都度通知（失敗は代表 1 回/束+heal は顧客イベント時のみ=低頻度）
    通知の失敗は握る（本体の未返信状態は App 28 に残り heal が回収）。"""
    try:
        if channel_name == "houki":
            await notify.notify_admin_line(
                "【相続放棄・要確認】書類写真の受領返信の送信に失敗しました。"
                "LINE アプリで受信をご確認ください。\n"
                f"userId: {user_id[:10]}...",
                # fix4[fix3-01]: 受領返信失敗は**専用キー**（既存系の受領記録/
                # 勝者判定失敗と分離——共有キー+モード混在で「失敗刻印が
                # 本通知を黙らせて実送信 0 回」になる穴を塞ぐ）
                throttle_key=f"houki_image_send_failure:{user_id}",
                throttle_on_success_only=True,
            )
        else:
            attorney = os.environ.get("ATTORNEY_LINE_USER_ID", "")
            if attorney:
                await notify.notify_business(
                    attorney,
                    "【要確認】書類写真の受領返信の送信に失敗しました。"
                    "LINE アプリで受信を確認し、必要なら手動でご返信"
                    "ください")
    except Exception:
        logger.error("[IMAGE_INTAKE] failure notify also failed (fixed text)")


async def _alert_houki_image_failure(user_id: str, what: str) -> None:
    """要確認通知（固定文言+userId 先頭のみ・PII 非搭載）。"""
    await notify.notify_admin_line(
        f"【相続放棄・要確認】書類写真の{what}に失敗しました。"
        "LINE アプリで受信をご確認ください。\n"
        f"userId: {user_id[:10]}...",
        throttle_key=f"houki_image_failure:{user_id}",
        # fix4[fix3-01]: houki 画像系の失敗通知は success_only へ統一——
        # 「通知の送信失敗が刻印を残して以後を黙らせる」経路をこの系から一掃
        # （既定 False 挙動に依存する他の共用 caller には触れない）
        throttle_on_success_only=True,
    )


async def store_houki_image(user_id: str, message_id: str) -> str:
    """JIKOU-FORM-3 Part A: 相続放棄チャネルの受信画像を取得し App 40 の
    受信書類写真へ添付する（受領返信の成否と独立・例外は外へ出さない）。
    対象レコード=App 40 の LINEユーザーID 一致レコード（未存在は未添付=保留）。"""
    if not message_id:
        return "no_message_id"
    try:
        rec = await houki_case_store.fetch_case(user_id)
        rid = str(((rec or {}).get("$id") or {}).get("value") or "")
        return await image_store.intake_line_image(
            "houki", HOUKI_CHANNEL, houki_case_store.APP_HOUKI_CASE,
            user_id, message_id, rid)
    except Exception:
        logger.error("[IMAGE_INTAKE] houki image store failed (fixed reason)")
        return "failed"


async def handle_houki_image(user_id: str, event_id: str,
                             message_id: str = "") -> None:
    """相続放棄チャネルの画像受信（router から BackgroundTasks で実行）。

    時効側（main._process_line_image_event）と同じゲート順・同じ冪等設計:
    全業務ブレーキ→停止リスト→冪等 pre-check→マーカー strict 保存
    （保存不能=返信しない fail-closed+要確認通知）→並行配送の勝者決定
    （最小 $id）→束ね選出→push 成功時のみ受領済み行で閉鎖（fix1[03]）。
    管理者への受信通知は router 側の既存 _record_inbound（300 秒スロットル）を
    維持し、本関数では送らない。
    JIKOU-FORM-3 Part A: 勝者決定後に取得+添付（store_houki_image）を行う。
    束ね返信・heal・claim の構造は不変（添付の失敗は返信を止めない）。"""
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

    idem_key = marker_category("houki", event_id)
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

    # JIKOU-FORM-3 Part A: 取得+添付は勝者のみ・デバウンス待ちの前に行う
    # （受領返信の成否と独立。不成立でも返信は続行する）
    await store_houki_image(user_id, message_id)

    async def _latest():
        return await latest_marker_category("houki", user_id)

    if not await debounce_and_elect("houki", user_id, idem_key, _latest):
        logger.info("[IMAGE_INTAKE] bundled (superseded)")
        return
    result = await send_receipt_and_close("houki", HOUKI_CHANNEL, user_id)
    if result is None:
        # fix2: claim 保持中/既に閉鎖済み=送信不要（他タスクが閉じる・沈黙）
        logger.info("[IMAGE_INTAKE] receipt not needed (claimed or closed)")
        return
    if result is not True:
        # fix1[03]: 未返信のまま（自己修復が回収）。要確認通知は fix3 で
        # 関門内に一元化済み（ここでは通知しない=二重通知防止）
        return
    logger.info("[IMAGE_INTAKE] houki receipt sent userId=%s...",
                emit(user_id[:10], "record_id", "log", "operator"))
