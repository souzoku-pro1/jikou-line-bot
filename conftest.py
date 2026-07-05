"""
pytest 共通設定

1. Windows ローカル環境では Python 標準の証明書ストアが OS の証明書を
   参照できず SSL エラーになることがあるため、truststore があれば
   OS 証明書ストアを注入する（Railway/Linux 上では実質 no-op）。

2. ANTHROPIC_API_KEY が未設定なら、リポジトリ直下の .env.test から読み込む。
   .env.test にはテスト専用Workspaceのキーだけを置くこと（本番キーは置かない。
   .gitignore 済みでコミットされない）。書式: KEY=VALUE の行のみ対応。

3. R3 戸籍読解（koseki_reader）はテストでは既定無効にする。
   /koseki/ingest は登録成功後に同期読解を呼ぶため（A案・2026-07-05 裁定）、
   reader を知らない既存テストから実 API（kintone / Claude・.env.test の
   実キー含む）へ到達し得る。reader 自身のテストは KOSEKI_READER_DISABLED を
   外した上で全 I/O をモックする。本番は env 未設定＝有効。
"""

import os
from pathlib import Path

os.environ.setdefault("KOSEKI_READER_DISABLED", "1")

try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass


def _load_env_test() -> None:
    env_file = Path(__file__).parent / ".env.test"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key and key not in os.environ:
            os.environ[key] = value


_load_env_test()
