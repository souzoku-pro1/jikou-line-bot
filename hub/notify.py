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
from hub.redact import emit

logger = logging.getLogger("hub.notify")

# 管理者通知のスロットル（同種の連続障害で LINE を埋めないため・claude_gateway から移設）
_NOTIFY_MIN_INTERVAL_SEC = 300
_last_notify_at: dict[str, float] = {}
# fix2[H4-fix1-01]: throttle_on_success_only 経路の**送信中予約**。従来の既定
# 経路は送信前刻印が排他を兼ねるが、成功時のみ刻印の opt-in 経路は
# 「未刻印確認→両方送信」の interleave で並行二重送信し得るため、送信中の
# key を予約して後発を遮断する。本 module は lock を持たない単一イベント
# ループ前提（実測: threading/asyncio.Lock 不使用）——予約の確認と設定は
# await を挟まない同期区間で行う＝check-then-act は割り込まれない
_notify_in_flight: set[str] = set()

_PUSH_URL = "https://api.line.me/v2/bot/message/push"

_BUSINESS_TOKEN_ENV = "DISPATCHBOT_CHANNEL_ACCESS_TOKEN"


def business_token_env() -> str:
    """業務通知（管理者警報・承認通知・受領通知・弁護士通知）の送信チャネル。

    **fail-closed（P1-102・RV-10 §4）**: 業務通知は業務指示Bot
    （DISPATCHBOT_CHANNEL_ACCESS_TOKEN）**のみ**から送る。未設定でも顧客Bot へは
    フォールバックしない（顧客チャネルに業務/顧客 PII を乗せない）。未設定時は
    この env 名を返すだけで、実送信は push_line_message 側が token 空でスキップする
    （＝送信ゼロ＋警告ログ）。欠落検知は daily_healthcheck の dead-man（heartbeat）が担う。
    """
    return _BUSINESS_TOKEN_ENV


def _business_recipients() -> frozenset[str]:
    """S1/dead-man 用の狭い allowlist（弁護士・管理者のみ）。notify_business が使う。"""
    out = set()
    attorney = os.environ.get("ATTORNEY_LINE_USER_ID", "")
    admin = get_admin_line_user_id()
    if attorney:
        out.add(attorney)
    if admin:
        out.add(admin)
    return frozenset(out)


def business_channel_allowlist() -> frozenset[str]:
    """業務チャネル（DISPATCHBOT）で送信を許す全宛先（H02・push_line_message の関所）。

    正当な直接 caller の宛先を用途つきで登録（それ以外への DISPATCHBOT 送信は拒否）:
      - ATTORNEY_LINE_USER_ID    : 弁護士（S1 通知・承認依頼・sortation 照会の最終 fallback）
      - 管理者（LINE_ADMIN_USER_ID / get_admin_line_user_id）: 死活監視・管理者警報
      - LINE_USER_ID             : /ocr/fixed-asset の受領通知先（事務所 PC watcher 運用）
      - SORTATION_ASK_TO         : 書類仕分け照会の宛先（sortation_ingest._ask_recipient）
      - DISPATCHBOT_ALLOWED_USER_IDS : 指示Bot のホワイトリスト（仕分け照会の fallback 宛先）
    """
    out = set(_business_recipients())
    for key in ("LINE_USER_ID", "SORTATION_ASK_TO"):
        v = os.environ.get(key, "").strip()
        if v:
            out.add(v)
    for u in os.environ.get("DISPATCHBOT_ALLOWED_USER_IDS", "").split(","):
        u = u.strip()
        if u:
            out.add(u)
    return frozenset(out)


async def push_line_message(to: str, text: str,
                            token_env: str = "LINE_CHANNEL_ACCESS_TOKEN") -> bool:
    """LINE Push の共通実装。成功で True。失敗はログのみ（例外を送出しない）。

    token_env: 送信チャネルのアクセストークン env 名。既定は顧客Bot
    （LINE_CHANNEL_ACCESS_TOKEN）。業務指示Bot名義で送るときは
    DISPATCHBOT_CHANNEL_ACCESS_TOKEN を指定する（例: 仕分け照会通知）。
    業務チャネルの送信成功時は dead-man 用 heartbeat を記録する（P1-102）。
    """
    line_token = os.environ.get(token_env, "")
    if not (to and line_token):
        logger.warning("LINE push skipped (no destination or token)")
        return False
    # H02: 業務チャネルへの送信は allowlist を強制（宛先の迂回を防ぐ）
    if token_env == _BUSINESS_TOKEN_ENV and to not in business_channel_allowlist():
        logger.warning("business channel push skipped (recipient not allowlisted)")
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
    except Exception:
        # 固定分類のみ（例外本文・token を出さない・logger.exception 不使用・L01）
        logger.error("LINE push error (request failed)")
        return False

    if not resp.is_success:
        # vendor 生レスポンスは emit(vendor_raw) で完全抑止（P1-102・台帳 :69 解消）
        logger.error(
            "LINE push failed status=%s body=%s",
            emit(resp.status_code, "count", "log", "operator"),
            emit(resp.text, "vendor_raw", "log", "operator"))
        return False

    # M01: heartbeat 記録は独立 best-effort（DB 失敗は warning のみ・HTTP 結果に影響させない）
    if token_env == _BUSINESS_TOKEN_ENV:
        try:
            from hub.notify_heartbeat import record_success
            await record_success("business")
        except Exception:
            logger.warning("business heartbeat record failed (non-fatal)")
    return True


async def notify_business(to: str, text: str) -> bool:
    """業務チャネル（DISPATCHBOT）への送信（宛先 allowlist 検証つき・P1-102）。

    宛先が allowlist（ATTORNEY_LINE_USER_ID / 管理者）外なら送信しない（fail-closed）。
    本文は呼び出し側で emit 済みであること（PII を素で渡さない）。
    """
    if to not in _business_recipients():
        logger.warning("business notify skipped (recipient not in allowlist)")
        return False
    return await push_line_message(to, text, token_env=business_token_env())


def _log_throttled(throttle_key: str) -> None:
    """throttle INFO を出す（H01）。throttle_key は `kind:{id}` 形（例
    prepare_deferred:{record_id} / dispatchbot_unauthorized:{user_id}）を取り得るため、
    key 全体は出さず **種別プレフィックスのみ**を固定語彙の定数として可視化する
    （ID＝record_id/user_id 等の混入を排除）。未知の頭は unknown_kind 固定文言。"""
    head = throttle_key.split(":", 1)[0]
    if head == "prepare_deferred":
        logger.info("admin LINE notify throttled kind=prepare_deferred")
    elif head == "dispatchbot_unauthorized":
        logger.info("admin LINE notify throttled kind=dispatchbot_unauthorized")
    elif head == "houki_inbound":
        logger.info("admin LINE notify throttled kind=houki_inbound")
    elif head == "dispatchbot_filing_error":
        logger.info("admin LINE notify throttled kind=dispatchbot_filing_error")
    elif head == "invalid_transition":
        logger.info("admin LINE notify throttled kind=invalid_transition")
    elif head == "no_adapter":
        logger.info("admin LINE notify throttled kind=no_adapter")
    elif head == "billing_error":
        logger.info("admin LINE notify throttled kind=billing_error")
    elif head == "fallback_activated":
        logger.info("admin LINE notify throttled kind=fallback_activated")
    elif head == "fallback_failed":
        logger.info("admin LINE notify throttled kind=fallback_failed")
    elif head == "hub_dispatch_error":
        logger.info("admin LINE notify throttled kind=hub_dispatch_error")
    elif head == "return_deadline_fetch_error":
        logger.info("admin LINE notify throttled kind=return_deadline_fetch_error")
    elif head == "return_deadline_check":
        logger.info("admin LINE notify throttled kind=return_deadline_check")
    elif head == "shindan_numbering":
        logger.info("admin LINE notify throttled kind=shindan_numbering")
    # JIKOU-IMG-2: 書類写真の AI 読解系 3 種+既存で未登録だった 5 種を可視化
    # （動作変更なし・unknown_kind 表示の解消のみ）
    elif head == "image_analysis_send_failure":
        logger.info("admin LINE notify throttled kind=image_analysis_send_failure")
    elif head == "image_analysis_store":
        logger.info("admin LINE notify throttled kind=image_analysis_store")
    elif head == "image_analysis_court":
        logger.info("admin LINE notify throttled kind=image_analysis_court")
    elif head == "houki_image_send_failure":
        logger.info("admin LINE notify throttled kind=houki_image_send_failure")
    elif head == "houki_image_failure":
        logger.info("admin LINE notify throttled kind=houki_image_failure")
    elif head == "houki_image_attach":
        logger.info("admin LINE notify throttled kind=houki_image_attach")
    elif head == "shindan_photos":
        logger.info("admin LINE notify throttled kind=shindan_photos")
    elif head == "form_link_merge":
        logger.info("admin LINE notify throttled kind=form_link_merge")
    # HOUKI-IMG-2: 相続放棄の書類写真読解 5 種
    elif head == "houki_image_analysis_court":
        logger.info("admin LINE notify throttled kind=houki_image_analysis_court")
    elif head == "houki_image_analysis_notice":
        logger.info("admin LINE notify throttled kind=houki_image_analysis_notice")
    elif head == "houki_image_analysis_disposition":
        logger.info("admin LINE notify throttled "
                    "kind=houki_image_analysis_disposition")
    elif head == "houki_image_analysis_send_failure":
        logger.info("admin LINE notify throttled "
                    "kind=houki_image_analysis_send_failure")
    elif head == "houki_image_analysis_store":
        logger.info("admin LINE notify throttled kind=houki_image_analysis_store")
    elif head == "houki_card_read":
        logger.info("admin LINE notify throttled kind=houki_card_read")
    elif head == "houki_card_read_failure":
        logger.info("admin LINE notify throttled kind=houki_card_read_failure")
    elif head == "houki_contract_created":
        logger.info("admin LINE notify throttled kind=houki_contract_created")
    elif head == "houki_contract_needs_review":
        logger.info("admin LINE notify throttled kind=houki_contract_needs_review")
    elif head == "houki_contract_failed":
        logger.info("admin LINE notify throttled kind=houki_contract_failed")
    else:
        logger.info("admin LINE notify throttled kind=unknown_kind")


async def notify_admin_line(text: str, throttle_key: str = "",
                            throttle_on_success_only: bool = False) -> bool:
    """管理者に LINE Push で通知する。失敗しても本処理には影響させない。
    送信できたら True・スキップ/失敗/スロットルは False（P1-102 dead-man 検証用）。

    claude_gateway から移設（挙動は不変・戻り値のみ追加）:
      - 管理者 ID 未設定ならスキップ（警告ログのみ）
      - throttle_key 指定時は同一キーの通知を _NOTIFY_MIN_INTERVAL_SEC 秒に1回へ抑制
      - 本文は 4900 文字で切り詰め

    throttle_on_success_only（SOUZOKU-HOUKI-H4-fix1[H4-01]・opt-in）:
      True のときスロットル刻印を**送信成功時のみ**行う——送信失敗が interval
      を占有して直後の再試行を機械的に拒否し続けない（通知成功を冪等キー
      書込の条件にする houki 電話推奨度通知向け）。既定 False は従来どおり
      試行時に刻印＝**共用先（時効側 caller）の挙動は不変**。
    """
    admin_id = get_admin_line_user_id()
    if not admin_id:
        logger.warning("admin LINE notify skipped (no admin id)")
        return False

    reserved = False
    if throttle_key:
        now = time.monotonic()
        last = _last_notify_at.get(throttle_key, 0.0)
        if now - last < _NOTIFY_MIN_INTERVAL_SEC:
            _log_throttled(throttle_key)   # 種別のみ可視（key 内の ID は出さない）
            return False
        if throttle_on_success_only:
            # fix2[H4-fix1-01]: スロットル確認と予約設定を同一同期区間で
            # 行う（await なし=原子的）。予約済み key への後発は送信しない
            if throttle_key in _notify_in_flight:
                logger.info("admin LINE notify suppressed (same key in flight)")
                return False
            _notify_in_flight.add(throttle_key)
            reserved = True
        else:
            _last_notify_at[throttle_key] = now

    # 業務通知は指示Botチャネルから（fail-closed・P1-102）
    try:
        sent = await push_line_message(admin_id, text,
                                       token_env=business_token_env())
        if sent and throttle_key and throttle_on_success_only:
            # 成功=予約を成功時刻の刻印へ確定（以後は通常スロットルが効く）
            _last_notify_at[throttle_key] = time.monotonic()
    finally:
        if reserved:
            # 自処理が設定した予約のみ解除（予約は単一保持者＝他処理の予約は
            # 共存し得ない。成功刻印 _last_notify_at には触れない）
            _notify_in_flight.discard(throttle_key)
    return sent


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
