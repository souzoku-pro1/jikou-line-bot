"""alembic 実行環境（P1-004）

- 接続URLは env DATABASE_URL のみから解決する（alembic.ini には書かない）。
  正規化（postgres:// → postgresql+psycopg://）は hub/db.py に一点集約（D4）
- 実行は明示コマンドのみ（アプリからの自動 upgrade 禁止・D2）。
  本モジュールを import するのは alembic CLI だけで、アプリ本体からは import しない
- target_metadata は P1-005（InboundEvent / IngestionReceipt）でモデルの
  metadata を接続する（autogenerate 用）。現状は None（D5: baseline のみ）
"""

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import create_engine

# リポジトリ直下を import path に（どこから起動しても hub.db を解決できるように）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hub.db import database_url  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# P1-005 で models の metadata に差し替える（autogenerate 用）
target_metadata = None


def run_migrations_offline() -> None:
    """--sql（offline）モード: 接続せずSQLスクリプトを生成する"""
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """通常モード: DATABASE_URL へ接続して migration を適用する"""
    engine = create_engine(database_url(), pool_pre_ping=True)
    try:
        with engine.connect() as connection:
            context.configure(connection=connection,
                              target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
