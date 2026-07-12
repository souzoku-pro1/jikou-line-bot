"""app ロガーの出力配線（RV-10 PR-4a/PR-4b・1点集約）。

uvicorn（`main:app`）配下でも standalone CLI（`if __name__ == "__main__"`）でも、
app モジュールの INFO を stdout に到達させるための共有セットアップ。main.py と
print 移送済みの CLI が同じ関数を1回呼ぶ（挙動は P1-107a と同一・純移設）。
"""

import logging
import sys


def configure_app_logging() -> None:
    """root ロガーに stdout handler を1つだけ付け、INFO 以上を
    timestamp/level/logger名/message 形式で出力する。

    - 二重付与ガード: 既に root へ handler がある場合（uvicorn --log-config /
      daily_healthcheck.py の __main__ basicConfig 等）は付与も level 変更もしない
      （既存挙動を尊重・M01）。
    - 多弁なサードパーティ（httpx/httpcore/urllib3）の per-request INFO は本番ログを
      洪水にするため WARNING へ引き上げる（app 由来の INFO 可視化が目的のため）。
    """
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(handler)
    root.setLevel(logging.INFO)   # handler を付与した場合のみ level を設定
    for _noisy in ("httpx", "httpcore", "urllib3"):
        logging.getLogger(_noisy).setLevel(logging.WARNING)
