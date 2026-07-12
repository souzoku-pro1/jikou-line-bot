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

# リポジトリ直下を import path に（どこから起動しても hub.db を解決できるように）
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hub.db import database_url, dispose_all, get_engine  # noqa: E402
from hub.inbound_event import Base  # noqa: E402（ORM metadata の起点・P1-005a）
from hub.notify_heartbeat import metadata as heartbeat_metadata  # noqa: E402（P1-102・M05）

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# autogenerate 用の統合 metadata（P1-007b 裁定=用途別の別 metadata を list で統合）。
# 新しい app-state モデル群はそれぞれの metadata をこの list に加えること。
target_metadata = [Base.metadata, heartbeat_metadata]


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
    """通常モード: DATABASE_URL へ接続して migration を適用する。
    エンジンは hub/db.py の一点集約から取得する（D4・独自 create_engine 禁止は
    test_db_foundation_hardening の AST 検査で恒久固定）。CLI プロセス終了前に
    dispose_all() で後片付けする（dispose の責任もこのモジュールが持つ）"""
    try:
        engine = get_engine()
        with engine.connect() as connection:
            context.configure(connection=connection,
                              target_metadata=target_metadata)
            with context.begin_transaction():
                context.run_migrations()
    finally:
        dispose_all()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
