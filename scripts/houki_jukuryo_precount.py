# -*- coding: utf-8 -*-
"""相続放棄 熟慮期間 監視（HOUKI-JUKURYO-2）の初回配備前 事前集計 — 読取専用・件数のみ（CLI 入口）

HOUKI-JUKURYO-2-fix1 付随 8／fix2 付随 6／fix4 BH-12: 本番 App 40 を GET だけして、初回実行で何が
起きるかを件数で示す（書込・送信・DB アクセスなし）。実装は scripts/houki_jukuryo_precount_impl.py
（hub に依存する部分）。本ファイルは hub に依存せず、実装の import 失敗も含めて全失敗を捕捉する。

出力（件数のみ・レコード番号・氏名・日付の個別値は出さない＝RV-10。集計日も出さず
「集計日は実行時 JST」の固定文言のみ・fix3 BH-09）:
  - fetched / targets（受任後 8 status × 申述提出日 空）
  - 起算日未確定（起算日_確定 空）
  - 残日数 <= 14 / <= 7 / 期限超過（< 0）
  - 通知済み閾値 の写しの現状（14日前 / 7日前 が立っている件数）
  - 初回の最大 push 数 = ≤14 件数 + ≤7 件数 + 1（未確定件数通知。未確定 0 件なら +0）
計算は hub/houki_jukuryo と同じ関数（jukuryo_deadline / is_target / compute）を使う。

失敗時（fix4 BH-12）: import・取得・集計のどこで失敗しても、例外本文・Traceback・依存ライブラリの
ログは stdout/stderr に出さない。stdout に固定の理由コード 1 行 `PRECOUNT_FAILED:import` /
`PRECOUNT_FAILED:fetch` / `PRECOUNT_FAILED:compute`（main の外の想定外は `PRECOUNT_FAILED:unexpected`）
を出し、終了コード 2 で終わる。root／依存ロガーは WARNING 以上に抑止したうえで出力先を
NullHandler にし、本スクリプトのロガーだけを stdout に繋ぐ。sys.excepthook も固定文言に差し替える。

実行手順（司令塔の票で・リポジトリ直下で Railway の env を注入して実行。本番 kintone への GET のみ・
書込・送信・DB アクセスなし）:
    cd C:/work/jikou-line-bot && railway run python scripts/houki_jukuryo_precount.py
**出力は標準出力（stdout）**（fix3 BH-11）。終了コード 0 = 正常・2 = 失敗（理由コードを参照）。

テスト用の注入点（HOUKI_PRECOUNT_FAULT・未設定なら何もしない。test_houki_jukuryo_precount.py が
subprocess で使う。本番では設定しない）:
  import        … 存在しないモジュールの import を強制（import 失敗の経路）
  fetch         … 取得で例外（HOUKI_PRECOUNT_FAULT_MARKER の文字列を例外本文に含める＝漏洩検査用）
  compute       … 集計で例外（同上・取得は合成レコード）
  fake_records  … kintone に触らず合成レコード（PII なし）で正常経路を通す
"""

import asyncio
import importlib
import logging
import os
import sys

EXIT_FAILED = 2
FAULT_ENV = "HOUKI_PRECOUNT_FAULT"
REASON_IMPORT = "PRECOUNT_FAILED:import"
REASON_FETCH = "PRECOUNT_FAILED:fetch"
REASON_COMPUTE = "PRECOUNT_FAILED:compute"
REASON_UNEXPECTED = "PRECOUNT_FAILED:unexpected"
IMPL_MODULE = "scripts.houki_jukuryo_precount_impl"


def _configure_logging() -> logging.Logger:
    """root／依存ロガーは WARNING 以上に抑止し出力先を NullHandler に（何も出さない）。
    本スクリプトのロガーだけを stdout に繋ぐ（propagate=False）。"""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(logging.NullHandler())
    root.setLevel(logging.WARNING)
    for name in ("httpx", "httpcore", "hub", "asyncio", "truststore"):
        logging.getLogger(name).setLevel(logging.WARNING)
    log = logging.getLogger("scripts.houki_jukuryo_precount")
    log.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    log.addHandler(handler)
    log.setLevel(logging.INFO)
    log.propagate = False
    return log


logger = _configure_logging()


def _excepthook(exc_type, exc, tb):
    """想定外の例外（main の外・終了処理中など）: 固定文言のみ・Traceback を出さない。
    （sink 方針: logger の引数はリテラルで書く＝REASON_* と同値・テストが同値を pin）"""
    logger.error("PRECOUNT_FAILED:unexpected")


sys.excepthook = _excepthook


def _load():
    """実装モジュールの import（hub の import 失敗もここに含まれる）。"""
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    if os.environ.get(FAULT_ENV, "").strip() == "import":
        importlib.import_module("houki_precount_nonexistent_module_for_test")
    try:
        import truststore
        truststore.inject_into_ssl()
    except Exception:
        pass
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return importlib.import_module(IMPL_MODULE)


def main() -> int:
    """CLI 入口: 全失敗を捕捉し、固定の理由コードと非ゼロ終了にする（例外本文は出さない）。"""
    try:
        impl = _load()
    except Exception:
        logger.error("PRECOUNT_FAILED:import")
        return EXIT_FAILED
    try:
        records = asyncio.run(impl.fetch())
    except Exception:
        logger.error("PRECOUNT_FAILED:fetch")
        return EXIT_FAILED
    try:
        impl.report(impl.compute(records))
    except Exception:
        logger.error("PRECOUNT_FAILED:compute")
        return EXIT_FAILED
    return 0


if __name__ == "__main__":
    sys.exit(main())
