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

from hub.db import session_scope

_metadata = sa.MetaData()

# Core Table（ORM Base に載せず、migration は手書き。autogenerate 対象外）
notify_heartbeat = sa.Table(
    "notify_heartbeat", _metadata,
    sa.Column("channel", sa.Text, primary_key=True),
    sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=False),
)

_DB_NOT_READY = (sa.exc.ProgrammingError, sa.exc.OperationalError)


def _skip() -> bool:
    return not os.environ.get("DATABASE_URL")


def _table_missing(exc: Exception) -> bool:
    return "notify_heartbeat" in str(exc).lower()


async def record_success(channel: str = "business") -> None:
    """業務通知成功時刻を upsert する。DB 未設定/未適用は no-op。"""
    if _skip():
        return
    now = datetime.now(timezone.utc)
    try:
        async with session_scope() as session:
            updated = await session.execute(
                sa.update(notify_heartbeat)
                .where(notify_heartbeat.c.channel == channel)
                .values(last_success_at=now))
            if updated.rowcount == 0:
                await session.execute(
                    sa.insert(notify_heartbeat)
                    .values(channel=channel, last_success_at=now))
    except _DB_NOT_READY as e:
        if _table_missing(e):
            return  # migration 未適用 — 静かにスキップ
        raise


async def get_last_success(channel: str = "business") -> datetime | None:
    """最終成功時刻。DB 未設定/未適用は None。"""
    if _skip():
        return None
    try:
        async with session_scope() as session:
            row = (await session.execute(
                sa.select(notify_heartbeat.c.last_success_at)
                .where(notify_heartbeat.c.channel == channel))).first()
        return row[0] if row else None
    except _DB_NOT_READY as e:
        if _table_missing(e):
            return None
        raise
