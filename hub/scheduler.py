"""定期ジョブレジストリ（hub/scheduler）

設計: docs/architecture/03-common-components.md §9
daily_healthcheck.py のスケジューラループの一般化（T0-2）。

規約:
  - 各ジョブは try/except で隔離される（1ジョブの失敗が他ジョブを止めない）
  - ジョブ自身が冪等であること（再デプロイの重なりで同日2回走っても安全）
  - 登録は import 時 / startup 時、起動は main.py の startup で start_all() を1回
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

logger = logging.getLogger("hub.scheduler")

_JST = timezone(timedelta(hours=9))

CoroFactory = Callable[[], Awaitable]


@dataclass
class _Job:
    name: str
    kind: str                 # "daily" | "interval"
    coro_factory: CoroFactory
    hour_jst: int = 0         # daily 用
    minutes: float = 0.0      # interval 用
    task: asyncio.Task | None = field(default=None, compare=False)


_jobs: dict[str, _Job] = {}


def register_daily(name: str, hour_jst: int, coro_factory: CoroFactory) -> None:
    """毎日 hour_jst 時（JST）に coro_factory() を実行するジョブを登録する"""
    if name in _jobs:
        raise ValueError(f"job already registered: {name}")
    _jobs[name] = _Job(name=name, kind="daily", coro_factory=coro_factory, hour_jst=hour_jst)


def register_interval(name: str, minutes: float, coro_factory: CoroFactory) -> None:
    """minutes 間隔で coro_factory() を実行するジョブを登録する（初回は間隔待ちの後）"""
    if name in _jobs:
        raise ValueError(f"job already registered: {name}")
    _jobs[name] = _Job(name=name, kind="interval", coro_factory=coro_factory, minutes=minutes)


def is_registered(name: str) -> bool:
    return name in _jobs


def start_all() -> None:
    """未起動の登録ジョブを起動する。冪等（2回呼んでも二重起動しない）。
    実行中のイベントループが必要（FastAPI startup から呼ぶ）。"""
    loop = asyncio.get_running_loop()
    for job in _jobs.values():
        if job.task is None or job.task.done():
            job.task = loop.create_task(_job_loop(job), name=f"hub.scheduler:{job.name}")
            logger.info("scheduler job started: %s (%s)", job.name, job.kind)


def stop_all() -> None:
    """全ジョブを停止し登録を破棄する（テスト・シャットダウン用）"""
    for job in _jobs.values():
        if job.task is not None and not job.task.done():
            job.task.cancel()
    _jobs.clear()


def _seconds_until_next_run(hour_jst: int, now: datetime | None = None) -> float:
    """次の hour_jst:00 JST までの秒数（daily_healthcheck から移設・ロジック不変）"""
    now = now or datetime.now(_JST)
    next_run = now.replace(hour=hour_jst, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    return (next_run - now).total_seconds()


async def _run_job_once(job: _Job) -> None:
    """1回分の実行。例外はジョブ内に隔離する（他ジョブ・ループ本体を止めない）"""
    try:
        await job.coro_factory()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("scheduled job failed: %s", job.name)


async def _job_loop(job: _Job) -> None:
    while True:
        if job.kind == "daily":
            wait = _seconds_until_next_run(job.hour_jst)
            # Railway ログで起動登録を確認できるよう print も出す（uvicorn 配下では
            # モジュールロガーの INFO がハンドラ未設定で出力されないため・従来と同形式）
            print(f"[{job.name}] scheduler registered: next run in {wait:.0f} sec "
                  f"(daily {job.hour_jst:02d}:00 JST)", flush=True)
        else:
            wait = job.minutes * 60
        await asyncio.sleep(wait)
        await _run_job_once(job)
