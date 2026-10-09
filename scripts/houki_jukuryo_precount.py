# -*- coding: utf-8 -*-
"""相続放棄 熟慮期間 監視（HOUKI-JUKURYO-2）の初回配備前 事前集計 — 読取専用・件数のみ

HOUKI-JUKURYO-2-fix1 付随 8／fix2 付随 6: 本番 App 40 を GET だけして、初回実行で何が起きるかを
件数で示す（書込・送信なし）。Codex が全文審査できるようリポジトリ scripts/ に置く。

出力（件数のみ・レコード番号・氏名・個別日付は出さない＝RV-10）:
  - fetched / targets（受任後 8 status × 申述提出日 空）
  - 起算日未確定（起算日_確定 空）
  - 残日数 <= 14 / <= 7 / 期限超過（< 0）
  - 通知済み閾値 の写しの現状（14日前 / 7日前 が立っている件数）
  - 初回の最大 push 数 = ≤14 件数 + ≤7 件数 + 1（未確定件数通知。未確定 0 件なら +0）
計算は hub/houki_jukuryo と同じ関数（jukuryo_deadline / is_target / compute）を使う。

使い方（リポジトリ直下で env を注入して・司令塔の票で実行）:
    cd C:/work/jikou-line-bot && railway run python scripts/houki_jukuryo_precount.py
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import truststore
    truststore.inject_into_ssl()
except Exception:
    pass
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from hub import houki_jukuryo as hj  # noqa: E402
from hub.redact import emit  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("scripts.houki_jukuryo_precount")
_JST = timezone(timedelta(hours=9))


async def main() -> int:
    today = datetime.now(_JST).date()
    records = await hj.fetch_all_targets()
    targets = [r for r in records if hj.is_target(r)]
    unset = le14 = le7 = overdue = marked14 = marked7 = 0
    for r in targets:
        comp = hj.compute(r, today)
        if comp is None:
            unset += 1
            continue
        le14 += comp.remaining <= 14
        le7 += comp.remaining <= 7
        overdue += comp.remaining < 0
        marks = hj.notified_values(r)
        marked14 += "14日前" in marks
        marked7 += "7日前" in marks
    max_push = le14 + le7 + (1 if unset else 0)
    # sink 方針: 値は emit 契約経由（count は値域検証つき素通し）。ラッパー関数は AST が emit と
    # 認識しないため直接書く
    logger.info("today(JST)=%s", emit(today.isoformat(), "record_id", "log", "operator"))
    logger.info("fetched=%s targets(受任後×未提出)=%s",
                emit(len(records), "count", "log", "operator"),
                emit(len(targets), "count", "log", "operator"))
    logger.info("起算日未確定=%s", emit(unset, "count", "log", "operator"))
    logger.info("残日数<=14=%s 残日数<=7=%s 期限超過(<0)=%s",
                emit(le14, "count", "log", "operator"),
                emit(le7, "count", "log", "operator"),
                emit(overdue, "count", "log", "operator"))
    logger.info("通知済み閾値 写し: 14日前=%s 7日前=%s",
                emit(marked14, "count", "log", "operator"),
                emit(marked7, "count", "log", "operator"))
    logger.info("初回の最大 push 数（<=14 + <=7 + 未確定件数通知 1）=%s",
                emit(max_push, "count", "log", "operator"))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
