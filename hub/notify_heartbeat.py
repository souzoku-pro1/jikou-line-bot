"""業務通知チャネルの dead-man 用 heartbeat（P1-102・RV-10 §4.2 最小版）

業務通知（DISPATCHBOT チャネル）が成功するたびに「最終成功時刻」を app-state（DB）へ
記録する。毎朝の daily_healthcheck がこの時刻の鮮度を検証し、長時間無音なら
「業務通知経路が死んでいる可能性」を検知する（統合形・新規メッセージ追加なし）。

fail-open にならない設計:
- DATABASE_URL 未設定 / テーブル未適用（migration 前）のときは静かに no-op
  （通知本体は止めない）。記録失敗が通知失敗を引き起こさない。
"""

import os
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from hub.db import session_scope

# app-state の専用 metadata（P1-007b 裁定=別 metadata 方式）。
# alembic env.py の target_metadata に統合する（M05）。migration は手書き。
metadata = sa.MetaData()
_metadata = metadata  # 後方互換の別名

notify_heartbeat = sa.Table(
    "notify_heartbeat", metadata,
    sa.Column("channel", sa.Text, primary_key=True),
    sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=False),
)

_DB_NOT_READY = (sa.exc.ProgrammingError, sa.exc.OperationalError)

# heartbeat 状態の識別子（H01: table 未適用と「行なし」を区別する）
DB_UNSET = "db_unset"
TABLE_MISSING = "table_missing"
EMPTY = "empty"


def _skip() -> bool:
    return not os.environ.get("DATABASE_URL")


def _table_missing(exc: Exception) -> bool:
    return "notify_heartbeat" in str(exc).lower()


def _upsert_stmt(channel: str, now: datetime):
    """DB 方言別の ON CONFLICT DO UPDATE（M02・競合安全な upsert）。"""
    url = os.environ.get("DATABASE_URL", "")
    values = {"channel": channel, "last_success_at": now}
    if url.startswith("postgres"):
        stmt = pg_insert(notify_heartbeat).values(**values)
        return stmt.on_conflict_do_update(
            index_elements=[notify_heartbeat.c.channel],
            set_={"last_success_at": now})
    # sqlite（テスト）ほか
    stmt = sqlite_insert(notify_heartbeat).values(**values)
    return stmt.on_conflict_do_update(
        index_elements=[notify_heartbeat.c.channel],
        set_={"last_success_at": now})


async def record_success(channel: str = "business") -> None:
    """業務通知成功時刻を upsert する（ON CONFLICT DO UPDATE）。DB 未設定/未適用は no-op。"""
    if _skip():
        return
    now = datetime.now(timezone.utc)
    try:
        async with session_scope() as session:
            await session.execute(_upsert_stmt(channel, now))
    except _DB_NOT_READY as e:
        if _table_missing(e):
            return  # migration 未適用 — 静かにスキップ
        raise


def record_success_sync(channel: str = "business") -> None:
    """業務通知成功時刻を同期エンジンで upsert する（CloudSign 等の sync 経路用・M03）。
    DB 未設定/未適用は no-op。呼び出し側は best-effort で例外を握りつぶすこと。"""
    if _skip():
        return
    from hub.db import get_engine
    now = datetime.now(timezone.utc)
    try:
        with get_engine().begin() as conn:
            conn.execute(_upsert_stmt(channel, now))
    except _DB_NOT_READY as e:
        if _table_missing(e):
            return
        raise


async def get_last_success(channel: str = "business") -> datetime | None:
    """最終成功時刻。DB 未設定/未適用/行なしは None。"""
    status, value = await get_heartbeat_status(channel)
    return value if status == "ok" else None


async def get_heartbeat_status(channel: str = "business"):
    """(status, datetime|None) を返す（H01: 未適用と行なしを区別）:
      ("db_unset", None) / ("table_missing", None) / ("empty", None) /
      ("ok", datetime)
    """
    if _skip():
        return DB_UNSET, None
    try:
        async with session_scope() as session:
            row = (await session.execute(
                sa.select(notify_heartbeat.c.last_success_at)
                .where(notify_heartbeat.c.channel == channel))).first()
        return ("ok", row[0]) if row else (EMPTY, None)
    except _DB_NOT_READY as e:
        if _table_missing(e):
            return TABLE_MISSING, None
        raise
