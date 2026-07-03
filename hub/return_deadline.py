"""返送期限監視ジョブ（hub/return_deadline・T1-4）

設計: docs/architecture/03 §9（ジョブレジストリ登録・毎日 8:00 JST）、04 §1・§4

- App 30 の「返送待ち」レコードのうち返送期限を超過したものを検出し、
  LINE 警報（hub/notify 経由・スロットル付き）を出す。**状態は変更しない**
  （消込は M5 の仕事。警報を受けた人が追跡番号で確認する・ハブ 04 §4）
- 期限当日は超過ではない（期限日いっぱいは待つ）。超過は翌日から
- 返送期限が未設定の返送待ちレコードは「期限未設定」として同じ警報に含める
  （設定漏れを放置すると永遠に警報されないため・データ欠損の検知）
- 返送期限の自動設定: compute_deadline() を dispatch（発送済→返送待ち遷移）が使う
"""

import logging
import os
from datetime import date, datetime, timedelta, timezone

from config import UNIT_CONFIG
from hub import kintone, notify
from hub import scheduler as hub_scheduler

logger = logging.getLogger("hub.return_deadline")

_JST = timezone(timedelta(hours=9))

APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

JOB_NAME = "RETURN_DEADLINE"
_DEFAULT_HOUR_JST = 8
_DEFAULT_DEADLINE_DAYS = 21


def _today_jst() -> date:
    return datetime.now(_JST).date()


def compute_deadline(unit: str) -> str:
    """発送済→返送待ち遷移時に設定する返送期限（YYYY-MM-DD）。
    日数はユニット設定（UNIT_CONFIG.return_deadline_days・既定21日）"""
    days = UNIT_CONFIG.get(unit, {}).get("return_deadline_days", _DEFAULT_DEADLINE_DAYS)
    return (_today_jst() + timedelta(days=days)).isoformat()


async def return_deadline_check() -> list[str]:
    """返送待ちレコードの期限を検査し、超過・期限未設定の一覧を返す。
    該当があれば LINE 警報を1通にまとめて送る（正常時は何もしない）。"""
    today = _today_jst()
    try:
        records = await kintone.search_records(
            APP_SHIPPING,
            '発送ステータス in ("返送待ち")',
            fields=["$id", "件名", "チャネル", "顧客名表示用", "返送期限", "追跡番号"],
        )
    except kintone.KintoneError as e:
        logger.error("return_deadline_check fetch failed: %s", e)
        await notify.notify_admin_line(
            "【返送期限監視: 実行失敗】\n"
            f"App 30 の検索に失敗しました: {str(e)[:200]}",
            throttle_key="return_deadline_fetch_error",
        )
        return []

    problems: list[str] = []
    for rec in records:
        rid = rec.get("$id", {}).get("value", "?")
        subject = rec.get("件名", {}).get("value", "")
        tracking = rec.get("追跡番号", {}).get("value", "") or "（未入力）"
        deadline_raw = rec.get("返送期限", {}).get("value", "")

        if not deadline_raw:
            problems.append(f"・No.{rid} {subject} / 返送期限が未設定 / 追跡番号: {tracking}")
            continue
        try:
            deadline = date.fromisoformat(deadline_raw)
        except ValueError:
            problems.append(f"・No.{rid} {subject} / 返送期限が不正な値: {deadline_raw!r}")
            continue
        if deadline < today:  # 期限当日は超過ではない
            overdue_days = (today - deadline).days
            problems.append(
                f"・No.{rid} {subject} / 期限 {deadline_raw}（超過{overdue_days}日）"
                f" / 追跡番号: {tracking}"
            )

    if problems:
        body = "\n".join(problems)
        await notify.notify_admin_line(
            "【返送期限超過】\n"
            f"{body}\n"
            "追跡番号で配達状況を確認してください（レコードは返送待ちのままです。"
            "書類が届いていればスキャン受領で消込されます）。",
            throttle_key="return_deadline_check",
        )
        logger.warning("return deadline problems: %d", len(problems))
    else:
        logger.info("return_deadline_check OK (返送待ち %d件・超過なし)", len(records))
    return problems


def register_return_deadline_job() -> None:
    """FastAPI startup から呼ぶ。RETURN_DEADLINE_DISABLED=1 で無効化できる。"""
    if os.environ.get("RETURN_DEADLINE_DISABLED", "") == "1":
        logger.info("return deadline job disabled by RETURN_DEADLINE_DISABLED=1")
        return
    hour = int(os.environ.get("RETURN_DEADLINE_HOUR_JST", str(_DEFAULT_HOUR_JST)))
    if not hub_scheduler.is_registered(JOB_NAME):
        hub_scheduler.register_daily(JOB_NAME, hour, return_deadline_check)
    hub_scheduler.start_all()
