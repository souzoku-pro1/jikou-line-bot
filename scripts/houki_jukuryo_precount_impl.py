# -*- coding: utf-8 -*-
"""houki_jukuryo_precount の実装部（scripts/houki_jukuryo_precount.py の CLI 入口から import される）。

分離の理由（fix4 BH-12）: sink 方針（test_sink_ast_policy）は `from hub.redact import emit` を
モジュール最上位の 1 形式でだけ信頼する。CLI 入口は hub の import 失敗も捕捉して固定理由コードで
終わる必要があるため、hub に依存する処理をこのモジュールに置き、入口は本モジュールの import 自体を
try で囲む。本モジュールは kintone への GET（hub.houki_jukuryo.fetch_all_targets）以外の外部
アクセスを行わない（書込・送信・DB なし）。出力は件数のみ（emit 契約経由）。
"""

import logging
import os
from datetime import datetime, timedelta, timezone

from hub import houki_jukuryo as hj
from hub.redact import emit

logger = logging.getLogger("scripts.houki_jukuryo_precount")

FAULT_ENV = "HOUKI_PRECOUNT_FAULT"
FAULT_MARKER_ENV = "HOUKI_PRECOUNT_FAULT_MARKER"
_JST = timezone(timedelta(hours=9))


def _fault() -> str:
    return os.environ.get(FAULT_ENV, "").strip()


def _marker() -> str:
    return os.environ.get(FAULT_MARKER_ENV, "injected-failure")


def fake_records() -> list[dict]:
    """fake_records／compute 注入用の合成レコード（PII なし・ID は 1/2/3）。"""
    return [
        {"$id": {"value": "1"}, "status": {"value": "受任"}, "申述提出日": {"value": ""},
         "起算日_確定": {"value": "2026-06-20"}, "通知済み閾値": {"value": ["14日前"]}},
        {"$id": {"value": "2"}, "status": {"value": "書類収集中"}, "申述提出日": {"value": ""},
         "起算日_確定": {"value": ""}, "通知済み閾値": {"value": []}},
        {"$id": {"value": "3"}, "status": {"value": "問い合わせ"}, "申述提出日": {"value": ""},
         "起算日_確定": {"value": "2026-06-20"}, "通知済み閾値": {"value": []}},
    ]


async def fetch() -> list[dict]:
    """対象レコードの取得（kintone GET のみ）。注入: fetch=例外・fake_records/compute=合成レコード。"""
    f = _fault()
    if f == "fetch":
        raise RuntimeError(_marker())
    if f == "fetch_exit":                       # fix5 BH-13: 依存先の sys.exit 相当
        raise SystemExit(_marker())
    if f in ("fake_records", "compute"):
        return fake_records()
    return await hj.fetch_all_targets()


def compute(records: list[dict]) -> dict:
    """件数の集計（hub.houki_jukuryo と同じ is_target / compute / notified_values）。"""
    if _fault() == "compute":
        raise ValueError(_marker())
    today = datetime.now(_JST).date()
    targets = [r for r in records if hj.is_target(r)]
    c = {"fetched": len(records), "targets": len(targets), "unset": 0, "le14": 0, "le7": 0,
         "overdue": 0, "marked14": 0, "marked7": 0}
    for r in targets:
        comp = hj.compute(r, today)
        if comp is None:
            c["unset"] += 1
            continue
        c["le14"] += comp.remaining <= 14
        c["le7"] += comp.remaining <= 7
        c["overdue"] += comp.remaining < 0
        marks = hj.notified_values(r)
        c["marked14"] += "14日前" in marks
        c["marked7"] += "7日前" in marks
    c["max_push"] = c["le14"] + c["le7"] + (1 if c["unset"] else 0)
    return c


def report(c: dict) -> None:
    """件数のみを stdout（入口が繋いだロガー）へ。値は emit 契約経由（count は値域検証つき素通し）。
    ラッパー関数は AST が emit と認識しないため直接書く。"""
    logger.info("集計日は実行時 JST（日付は出力しない）")                     # fix3 BH-09: 固定文言のみ
    logger.info("fetched=%s targets(受任後×未提出)=%s",
                emit(c["fetched"], "count", "log", "operator"),
                emit(c["targets"], "count", "log", "operator"))
    logger.info("起算日未確定=%s", emit(c["unset"], "count", "log", "operator"))
    logger.info("残日数<=14=%s 残日数<=7=%s 期限超過(<0)=%s",
                emit(c["le14"], "count", "log", "operator"),
                emit(c["le7"], "count", "log", "operator"),
                emit(c["overdue"], "count", "log", "operator"))
    logger.info("通知済み閾値 写し: 14日前=%s 7日前=%s",
                emit(c["marked14"], "count", "log", "operator"),
                emit(c["marked7"], "count", "log", "operator"))
    logger.info("初回の最大 push 数（<=14 + <=7 + 未確定件数通知 1）=%s",
                emit(c["max_push"], "count", "log", "operator"))
