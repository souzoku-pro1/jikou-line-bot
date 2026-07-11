"""DB接続の一点集約（P1-004・O-01 PostgreSQL）

設計（製品設計完全版 §14.4・§17.2 / P1-004 設計判断 D1〜D5）:
- **DATABASE_URL 未設定でもアプリは正常起動する**（D3・lazy初期化）。
  import 時・startup 時には接続もエンジン生成もしない。DB を使う機能が
  最初に呼んだ時点で初めてエンジンを作る。未設定のままその時点に達したら
  DatabaseNotConfigured で明示的に失敗する（黙って劣化しない）
- **エンジン生成・セッション管理はこのモジュール以外に書かない**（D4）
- **migration はアプリからは実行しない**（D2・明示コマンドのみ。
  本番は大野が `railway run alembic upgrade head`。README §migration 参照）
- driver は psycopg(v3)。Railway の DATABASE_URL は postgresql://
  （旧形式は postgres://）のため、SQLAlchemy 2 + psycopg3 の
  postgresql+psycopg:// へ正規化する
"""

import os
from contextlib import asynccontextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)


class DatabaseNotConfigured(RuntimeError):
    """DATABASE_URL 未設定のまま DB 機能に到達した（設定漏れの明示化）"""


_engine: Engine | None = None
_async_engine: AsyncEngine | None = None
_async_session_factory: async_sessionmaker | None = None


def normalize_url(url: str) -> str:
    """postgres:// / postgresql:// を postgresql+psycopg:// に正規化する。
    既に driver 明示済み・別スキーム（テスト用 sqlite:// 等）はそのまま返す"""
    if url.startswith("postgresql+psycopg://"):
        return url
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://"):]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://"):]
    return url


def database_url() -> str:
    """接続URL（正規化済み）。未設定は fail-closed（値をログに出さない）"""
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise DatabaseNotConfigured(
            "DATABASE_URL が未設定です。DB を使う機能はこのエラーで停止します"
            "（DB を使わない既存機能には到達しない・D3）")
    return normalize_url(url)


def get_engine() -> Engine:
    """同期エンジン（alembic env.py・運用スクリプト用）。lazy生成・以後共有"""
    global _engine
    if _engine is None:
        _engine = create_engine(database_url(), pool_pre_ping=True)
    return _engine


def get_async_engine() -> AsyncEngine:
    """非同期エンジン（FastAPI ハンドラ側から使う。P1-005 以降の標準）"""
    global _async_engine
    if _async_engine is None:
        _async_engine = create_async_engine(database_url(), pool_pre_ping=True)
    return _async_engine


def get_async_session_factory() -> async_sessionmaker:
    global _async_session_factory
    if _async_session_factory is None:
        _async_session_factory = async_sessionmaker(
            get_async_engine(), expire_on_commit=False)
    return _async_session_factory


@asynccontextmanager
async def session_scope():
    """commit/rollback を構造で強制する非同期セッション（P1-005 以降の標準入口）:

        async with session_scope() as session:
            ...
    正常終了で commit・例外で rollback して再送出する
    """
    factory = get_async_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


def reset_for_tests() -> None:
    """テスト専用: lazy キャッシュを破棄する（本番コードから呼ばない）。
    async エンジンの dispose はイベントループが要るためテストでは参照破棄のみ"""
    global _engine, _async_engine, _async_session_factory
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _async_engine = None
    _async_session_factory = None
