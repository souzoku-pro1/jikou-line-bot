"""autoreply_stoplist — AUTOREPLY-STOPLIST-1: 自動返信の個人別停止リスト

裁定済み方針(C)+(B): kintone 専用アプリ（LINE userId 基準＝個人情報未入力の
問い合わせ段階でも特定可能）で停止対象を管理し、受信時の自動返信と
承認キュー経由の自動送信の両方を抑止する。

契約:
- is_suppressed(user_id): 停止リストに userId のレコードがあれば True。
  **fail-open（裁定済み）**: env 未設定（機能未接続）・userId grammar 外・
  照会失敗はすべて False＝自動返信を止めない（顧客対応の穴を作らない）。
  検証済み userId のみを query へ埋める（自由文字列を kintone query へ
  埋めない既存規律）
- 照会失敗が FAILURE_ALERT_THRESHOLD 件連続したら管理者 LINE へ警報
  （FAILURE_ALERT_INTERVAL_SEC に 1 回の throttle・成功で連続数リセット）
- count_suppressed(user_id): 抑止件数の計数（プロセス内・匿名ID 単位）＋
  追跡ログ。恒久的な追跡は App28 受信記録（冪等キー
  category="stoplist:{webhook_event_id}"）で行える
- PII: userId の生値をログへ出さない（匿名ID = SHA-256 先頭 8 桁・pause
  経路と同じ流儀）
"""

import hashlib
import logging
import re
import time

from hub import kintone

logger = logging.getLogger("autoreply_stoplist")

APP_AUTOREPLY_STOP = kintone.KintoneApp(
    "App 39 (自動返信停止リスト)", "APP_AUTOREPLY_STOP",
    "TOKEN_AUTOREPLY_STOP")

_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")

FAILURE_ALERT_THRESHOLD = 3
FAILURE_ALERT_INTERVAL_SEC = 3600.0

# プロセス内状態（単一 worker 前提・再起動でリセット＝警報は再発時に再送）
_failure_state: dict = {"consecutive": 0, "last_alert_monotonic": None}
_suppressed_counts: dict = {}


def _anon(user_id: str) -> str:
    return hashlib.sha256(str(user_id).encode("utf-8")).hexdigest()[:8]


def _configured() -> bool:
    return bool(APP_AUTOREPLY_STOP.app_id() and APP_AUTOREPLY_STOP.token())


async def is_suppressed(user_id: str) -> bool:
    """停止リスト照会（fail-open）。True のとき呼び出し側は自動返信/自動送信を
    行わない。False は「止めない」（未設定・不正 ID・照会失敗を含む）。"""
    if not _configured():
        return False
    uid = str(user_id or "")
    if not _LINE_USER_ID_RE.fullmatch(uid):
        return False
    try:
        rows = await kintone.search_records(
            APP_AUTOREPLY_STOP, f'LINE_userId = "{uid}" limit 1',
            fields=["$id"])
    except Exception as e:
        await _on_lookup_failure(type(e).__name__)
        return False
    _failure_state["consecutive"] = 0
    return bool(rows)


async def _on_lookup_failure(error_cls: str) -> None:
    """照会失敗の計数と throttle つき警報（分類名のみ・PII 非搭載）。"""
    _failure_state["consecutive"] += 1
    n = _failure_state["consecutive"]
    logger.error("[AUTOREPLY_STOPLIST] lookup failed (fail-open: autoreply "
                 "continues) cls=%s consecutive=%d", error_cls, n)
    if n < FAILURE_ALERT_THRESHOLD:
        return
    now = time.monotonic()
    last = _failure_state["last_alert_monotonic"]
    if last is not None and now - last < FAILURE_ALERT_INTERVAL_SEC:
        return
    _failure_state["last_alert_monotonic"] = now
    try:
        from hub.notify import notify_admin_line
        await notify_admin_line(
            f"【自動返信停止リスト】照会失敗が連続 {n} 件です"
            f"（分類: {error_cls}）。停止リストは fail-open のため自動返信は"
            "継続しています（停止したい相手にも返信され得ます）。kintone "
            "接続と APP_AUTOREPLY_STOP/TOKEN_AUTOREPLY_STOP を確認して"
            "ください")
    except Exception:
        logger.error("[AUTOREPLY_STOPLIST] failure alert send failed "
                     "(fixed text only)")


def count_suppressed(user_id: str) -> int:
    """抑止 1 件を計数し、その userId（匿名ID）での累計を返す＋追跡ログ。"""
    anon = _anon(user_id)
    _suppressed_counts[anon] = _suppressed_counts.get(anon, 0) + 1
    n = _suppressed_counts[anon]
    logger.info("[AUTOREPLY_STOPLIST] suppressed anon_id=%s nth=%d "
                "suppressed_users=%d", anon, n, len(_suppressed_counts))
    return n
