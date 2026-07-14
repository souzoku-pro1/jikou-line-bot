# 作業記録 2026-07-14: RV-05-13-fix（R-RV-05-13 レビュー所見の修正）

- 対象: `docs/work-logs/2026-07-14_RV-05-13_durable-inbound-impl.md` の実装（SHA 8dfb2d3）に対する Codex レビュー R-RV-05-13 の全所見修正
- BASE: 8dfb2d3 / BRANCH: `feat/rv05-13-durable-inbound`（継続）
- 正本: `docs/design-drafts/DRAFT_RV05_DURABLE_INBOUND.md`（rev5・**変更禁止**・実装を正本へ合わせる）
- flag: `INBOUND_EVENT_DURABLE_ENABLED`（既定 OFF＝現行挙動 byte 同一・不変）

## 0. 結論サマリ

R-RV-05-13 の必須修正（H-01〜H-05・M-01〜M-07・L-01）を全て実装。核心バグ H-04（ask 保存
失敗が握られ「200 成功 ACK＋completed」で終わる＝照会が黙って消える）を**修正前 SHA 8dfb2d3 で
実測再現**（§4）。修正後は同一シナリオで **500＋pending_retry**（GAS 再送・照会は失われない）。

- `test_ingestion_receipt.py` 14 / `test_rv05_13_durable.py` 12 → **26 passed / 0 failed**
- 全 suite: §7 に実出力。

## 1. 所見 → 修正 対応表

| 所見 | 種別 | 修正 | 実装位置 | 担保テスト |
|---|---|---|---|---|
| H-01 | fence 喪失時の外部作用停止 | `mark_phase()` の None（fence 喪失）・`mark_terminal()` の False を**処理中断**にし、OCR/ask/通知/forward に進まず 200 も返さない（409）。§H-06 表準拠 | `sortation_ingest.py` vendor_pre marker（`_ep is None→409`）・terminal（`not mark_terminal→409`） | `test_m04_reconcile_invalidates_inflight_epoch` |
| H-02 | claim 敗者・completed 重複の全 skip | claim None 時 `get_state`→ completed/failed/unknown は `{"action":"skip"}` の 200、in-progress は 409。OCR/Claude/ask/通知を実行しない | `sortation_ingest.py` claim ブロック | `test_h02_completed_duplicate_skips_processing` |
| H-03 | LINE 二重返信の遮断 | `record_line_event()` の `"duplicate"` 戻り値で `_process_line_event_durable` 登録を skip（＋dedup 系列計上） | `main.py` webhook（`if outcome=="duplicate": continue`） | `test_h03_duplicate_delivery_no_double_reply` |
| **H-04** | ask 失敗→PENDING_RETRY→5xx | `_log_ask`/`_notify_ask` の例外握りを durable 経路で解除（`raise_on_error=_receipt is not None`）→外側 try が捕捉→`mark_pending_retry`→再送出（5xx）。**修正前 FAIL 実測必須**（§4） | `sortation_ingest.py` `_log_ask`/`_notify_ask`・SENDING ブロック | `test_h04_ask_save_failure_pending_retry_5xx`＋§4 実測 |
| H-05 | DB clock 単一源 | 全書込の時刻を `sa.func.now()`（SQL 側）へ。app `datetime.now()` 廃止。`record_line_event` は Core insert で `received_at=now()` 明示 | `hub/ingestion_receipt.py` `_now`/`_stale_cutoff`・`hub/durable_inbound.py` | 全 ledger テスト（DB clock 前提） |
| M-01 | reconciliation も監査 | `reconcile_stale` を `UPDATE...RETURNING`＋同一 tx の attempt INSERT に統一（epoch++） | `hub/ingestion_receipt.py` `reconcile_stale` | `test_m01_reconciliation_writes_audit_row` |
| M-02 | duplicate_suspect の atomic 化 | pre-SELECT を廃し、guard（`source_sha256 != new OR coalesce(case_hint)!=…`）付き atomic UPDATE RETURNING に | `hub/ingestion_receipt.py` `upsert_receipt` IntegrityError 分岐 | `test_condition7_duplicate_suspect`・`test_dedup_same_elements_skips` |
| M-03/M-04 | テスト追補 | 下記を追加: 中断（terminal False/mark_phase None）・reconciliation×in-flight barrier・claim 敗者 skip・completed 重複 skip・ask 失敗→5xx・LINE duplicate skip | 両テストファイル | 上記各行 |
| M-05 | 修正前 FAIL 実測 | 実行可能スクリプト＋実出力全文を本 work-log §4 に保存 | 本ファイル §4 | — |
| M-06 | flag OFF で durable を import しない | main/sortation の module top-level から `hub.durable_inbound` import を排除。flag 判定は env 直読み（`_durable_enabled()`／inline）。durable 呼出は判定の内側の関数ローカル import のみ | `main.py`・`sortation_ingest.py` | `test_durable_calls_guarded_by_flag` |
| M-07 | lease 600s の定量化 | §5 に外部 call の timeout 設定値＋marker 間最長区間＋安全率の根拠を記載 | 本ファイル §5 | — |
| L-01 | DB 値（小文字）を正本 | 状態定数は小文字（`"completed"` 等）で実装。DRAFT に表記対応注記 1 行（§6） | `hub/ingestion_receipt.py` ST_*・DRAFT 注記 | — |

## 2. M-06 の選択と理由

**選択: 遅延 import（flag OFF は `hub.durable_inbound` を一切 import しない）。**
DRAFT/work-log の記述を実装実態に合わせるのではなく、実装側を「import しない構造」にした。

- 理由: DRAFT §1 の不変条件「flag OFF＝現行挙動と byte 同一」を最も強く担保するのは、
  OFF 経路が durable モジュールの副作用（logger 設定・sink 登録・DB metadata 参照）に一切触れ
  ないこと。記述合わせでは「import はするが呼ばない」となり、import 時副作用の混入余地が残る。
- 実装: `main.py`/`sortation_ingest.py` の module top-level に `hub.durable_inbound` を置かず、
  flag 判定（`os.environ.get("INBOUND_EVENT_DURABLE_ENABLED")` の inline／`_durable_enabled()`）
  が真の分岐内でのみ関数ローカル import する。
- 機械的担保: `test_durable_calls_guarded_by_flag` が両ファイルの非インデント行に
  `hub.durable_inbound` の import が無いことを検査（関数ローカル import は許容）。

## 3. H-D4 不変条件の維持（変更が壊していないこと）

- H-D4-01「epoch を進めない state 変更は存在しない」: 追加した中断/skip 分岐はいずれも
  **新規 UPDATE を発行しない**（claim None/mark_phase None/terminal False で分岐して抜けるだけ）
  ため、単一 UPDATE 統一を崩さない。
- H-D4-02「receipt.last_outcome が状態正本」: H-02 の応答判定は `get_state`（receipt 値）参照。
  processing_attempt は監査専用のまま（判定に使わない）。

## 4. 【M-05/H-04】修正前 FAIL 実測（成功 ACK バグの実証）

### 4.1 何を示すか
ask 保存（`kintone.create_record`）が失敗したとき、修正前コード（SHA 8dfb2d3）は
`_log_ask` 内の `except Exception: … return None` で例外を握り、外側の
`try/except: mark_pending_retry; raise` が発火せず、`mark_terminal(COMPLETED)` に到達して
**HTTP 200（成功 ACK）＋ receipt=completed** を返す。GAS はこれを成功とみなし再送しない＝
照会（人手確認依頼）が黙って失われる。これが H-04。

### 4.2 方式（忠実性）
`_log_ask`/`_notify_ask` 自体は patch せず、**実依存 `kintone.create_record` だけ**を失敗
させる。これにより「_log_ask が実際に例外を握って None を返す」修正前の制御フローを再現する
（`_log_ask` を丸ごと mock すると握り挙動を回避してしまい、実証にならないため）。

### 4.3 実行手順
```bash
# 修正前（PRE-FIX）: SHA 8dfb2d3 の worktree で実行
git worktree add --detach /c/work/rv0513-prefix-wt 8dfb2d3
cp <本スクリプト> /c/work/rv0513-prefix-wt/demo_h04_prefix_fail.py
cd /c/work/rv0513-prefix-wt && PYTHONUTF8=1 python demo_h04_prefix_fail.py   # exit 0 = BUG 再現
# 修正後（POST-FIX）: 現行ブランチで同一スクリプトを実行
cd /c/work/jikou-line-bot && PYTHONUTF8=1 python demo_h04_prefix_fail.py     # exit 1 = 再現せず（修正済）
```

### 4.4 スクリプト全文（`demo_h04_prefix_fail.py`）
```python
"""H-04 修正前FAIL実測（PRE-FIX / SHA 8dfb2d3）.

方式: _log_ask / _notify_ask 自体は patch せず、その内部依存 kintone.create_record だけを
失敗させる。これにより「_log_ask が例外を内部で握って None を返す→外側 try/except が発火
しない→mark_terminal(completed)→200」という修正前の実際の制御フローを再現する。
"""
import asyncio, os, shutil, tempfile
from unittest.mock import AsyncMock, MagicMock, patch

_ENV = {
    "ANTHROPIC_API_KEY": "dummy", "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token", "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26", "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "c", "CLOUDSIGN_WEBHOOK_SECRET": "cs",
    "KINTONE_WEBHOOK_TOKEN": "t", "DOCUMENT_WEBHOOK_SECRET": "d",
    "APP_APPROVAL": "29", "TOKEN_APPROVAL": "d", "HEALTHCHECK_DISABLED": "1",
    "STRIPE_WEBHOOK_SECRET": "w", "GOOGLE_VISION_API_KEY": "dummy_vision",
    "SORTATION_INGEST_TOKEN": "sort-token",
    "APP_SORTATION_LOG": "38", "TOKEN_SORTATION_LOG": "logtok",   # _log_ask を kintone まで到達させる
    "SORTATION_ASK_TO": "Uattorney", "DISPATCHBOT_CHANNEL_ACCESS_TOKEN": "dtok",
    "INBOUND_EVENT_DURABLE_ENABLED": "1",
}
os.environ.update(_ENV)

import sqlalchemy as sa
from fastapi.testclient import TestClient
import hub.db as db
from hub import ingestion_receipt as ir
from hub.inbound_event import Base as InboundBase
import main


def _run(c): return asyncio.run(c)


def main_demo():
    d = tempfile.mkdtemp(prefix="h04demo_")
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{d}/n.db"
    db.reset_for_tests()

    async def _create():
        eng = db.get_async_engine()
        async with eng.begin() as c:
            await c.run_sync(ir.metadata.create_all)
            await c.run_sync(InboundBase.metadata.create_all)
    _run(_create()); db.reset_for_tests()

    ocr = MagicMock(return_value="調査結果通知書のOCR")
    judge = AsyncMock(return_value={"doc_type": "その他", "confidence": 0.1, "reason": "ask"})
    failing_kintone = AsyncMock(side_effect=RuntimeError("kintone 5xx (ask 保存失敗)"))

    client = TestClient(main.app, raise_server_exceptions=False)
    with patch("sortation_ingest._ocr_pdf", new=ocr), \
         patch("sortation_ingest.list_candidates", new=AsyncMock(return_value=[])), \
         patch("sortation_ingest._judge_with_claude", new=judge), \
         patch("sortation_ingest.kintone.create_record", new=failing_kintone), \
         patch("sortation_ingest.push_line_message", new=AsyncMock(return_value=None)):
        r = client.post("/sortation/ingest?token=sort-token",
                        files={"file": ("x.pdf", b"%PDF ask fail", "application/pdf")},
                        data={"drive_file_id": "FA"})

    async def _states():
        async with db.session_scope() as s:
            return (await s.execute(sa.select(ir.ingestion_receipt.c.last_outcome))).scalars().all()
    states = _run(_states()); db.reset_for_tests()

    print("=" * 68)
    print("H-04 修正前FAIL実測  (PRE-FIX / SHA 8dfb2d3)")
    print("=" * 68)
    print(f"ask 保存 (kintone.create_record) 呼び出し回数 : {failing_kintone.await_count}")
    print(f"  └ 例外送出したか                          : "
          f"{'YES（ask は実際に失敗した）' if failing_kintone.await_count else 'NO'}")
    print(f"HTTP status                                  : {r.status_code}")
    print(f"receipt.last_outcome                         : {states}")
    print("-" * 68)
    bug = (r.status_code == 200 and states == [ir.ST_COMPLETED])
    if bug:
        print("結果: **BUG 再現** — ask 保存が失敗したのに 200(成功ACK)＋completed。")
        print("      → GAS は再送しない。照会(人手確認依頼)が黙って消える。")
        print("VERDICT: PRE-FIX FAIL（期待挙動 5xx+pending_retry に対して 200+completed）")
    else:
        print(f"結果: 期待外（status={r.status_code}, states={states}）")
        print("VERDICT: 再現せず（要調査）")
    print("=" * 68)
    shutil.rmtree(d, ignore_errors=True)
    return bug


if __name__ == "__main__":
    raise SystemExit(0 if main_demo() else 1)
```

### 4.5 実出力（PRE-FIX / SHA 8dfb2d3）— 全文
```
2026-07-14 20:34:17,731 INFO sortation_ingest [SORTATION] judged file=（external_ref・非表示） customer=（値なし・非表示）
2026-07-14 20:34:17,731 INFO sortation_ingest [SORTATION] 仕分けログ登録に失敗（照会通知は継続）: RuntimeError （構造化値・非表示・?要素）
====================================================================
H-04 修正前FAIL実測  (PRE-FIX / SHA 8dfb2d3)
====================================================================
ask 保存 (kintone.create_record) 呼び出し回数 : 1
  └ 例外送出したか                          : YES（ask は実際に失敗した）
HTTP status                                  : 200
receipt.last_outcome                         : ['completed']
--------------------------------------------------------------------
結果: **BUG 再現** — ask 保存が失敗したのに 200(成功ACK)＋completed。
      → GAS は再送しない。照会(人手確認依頼)が黙って消える。
VERDICT: PRE-FIX FAIL（期待挙動 5xx+pending_retry に対して 200+completed）
====================================================================
```
`exit code = 0`（BUG 再現）。ログ 2 行目 `[SORTATION] 仕分けログ登録に失敗（照会通知は継続）:
RuntimeError` が「握り」の実発生を示し、直後に **200＋completed** で終わっている。

### 4.6 実出力（POST-FIX / 現行ブランチ・同一スクリプト）— 全文
```
2026-07-14 20:34:21,071 INFO sortation_ingest [SORTATION] judged file=（external_ref・非表示） customer=（値なし・非表示）
====================================================================
H-04 修正前FAIL実測  (PRE-FIX / SHA 8dfb2d3)
====================================================================
ask 保存 (kintone.create_record) 呼び出し回数 : 1
  └ 例外送出したか                          : YES（ask は実際に失敗した）
HTTP status                                  : 500
receipt.last_outcome                         : ['pending_retry']
--------------------------------------------------------------------
結果: 期待外（status=500, states=['pending_retry']）
VERDICT: 再現せず（要調査）
====================================================================
```
`exit code = 1`（BUG 再現せず＝修正済）。同一シナリオが **500＋pending_retry**（GAS 再送・
照会は失われない）。修正前の「握り」ログ行も消えている（例外が上位へ伝播したため）。

## 5. 【M-07】lease 600 秒の定量的根拠

lease（`INBOUND_RECONCILE_STALE_SECONDS`・既定 600）は「1 回の claim が、fence を更新する
書込（claim / mark_phase / mark_terminal は各々 `last_heartbeat_at=now()` を書く）**の間**、
receipt を非終端で保持しうる最長壁時計」を超えている必要がある。健全な処理中 request が
reconciliation に誤って reclaim されないための下限であって、相互排他の正しさ保証ではない
（正しさは epoch fencing が担保。lease 超過で late write が来ても §4.6/`test_m04` の通り abort）。

### 5.1 sortation 同期経路の外部 call と timeout 設定値（実測列挙）

| 呼出 | 実装 | 設定 timeout | marker 間区間 |
|---|---|---|---|
| Vision OCR | `main._vision_annotate`（`urllib.request.urlopen`） | **明示 timeout なし**（socket 既定） | claim→vendor_pre |
| Claude 判定 | `claude_gateway.create_message_with_fallback`（`anthropic.AsyncAnthropic`） | SDK 既定 `Timeout(connect=5, read/write/pool=600)`・`max_retries=2`・＋gateway で FALLBACK_MODEL 1 回 | claim→vendor_pre |
| ask 保存 | `hub.kintone._write`（`httpx.AsyncClient()`） | httpx 既定 **5s**（connect/read/write/pool） | sending→terminal |
| 照会通知 | `hub.notify.push_line_message`（`httpx.AsyncClient()`） | httpx 既定 **5s** | sending→terminal |
| forward | koseki/valuation/registry ingest（入れ子で OCR＋Claude＋kintone を再実行） | 上記の入れ子 | sending→terminal |

（anthropic SDK 既定値はローカルで確認: `Timeout(connect=5.0, read=600, write=600, pool=600)`,
`DEFAULT_MAX_RETRIES=2`。）

### 5.2 marker 間の最長区間 → lease の下限

各 `mark_phase`/`claim`/`mark_terminal` が `last_heartbeat_at=now()` を書くため、lease が
超えねばならないのは「連続する 2 つの fence 更新書込の**間隔**」であり、request 全体の総和では
ない。重い区間は 2 つ:

1. `claim → mark_phase(vendor_pre)`: Vision OCR ＋ Claude 判定。支配項は **Claude の SDK read
   timeout 600s**（Vision は明示 timeout 無しだが、実務の法律書類 PDF は ≤ 数十ページ＝
   5 ページ/バッチで数バッチ・実測秒〜低分）。
2. `sending → mark_terminal`（forward 経路）: 入れ子 ingest（Vision＋Claude）で再び ~600s 級。

支配項＝**Claude 1 呼出の SDK ceiling 600s**。lease 600s はこの「単一外部 call の最大 configured
ceiling」に一致させた値。安全率は次の 3 点で確保:

- (a) lease ≥ 支配的単一 call ceiling（600s）。
- (b) 各 mark_phase が lease をリセットするため、実経過は「最長 marker 間区間」だけ超えなければ
  よく、request 総和ではない。
- (c) 正しさは lease に依存しない: 誤 reclaim（epoch++）が起きても in-flight の terminal/heartbeat
  は 0 行で abort（`test_m04_reconcile_invalidates_inflight_epoch`・§4.6）。

ローカル（外部 call を mock）の 1 request 実測は DB 支配で数十 ms 級であり実 timeout の代表値
ではない。実 ceiling は上表の SDK/socket 既定値が上限。

### 5.3 既知の限界（OPEN・RV-06 送り）

Vision（`urllib`）に**明示 timeout が無い**ため、ハングした socket では call が無限に伸びうる。
将来 per-call の明示 timeout を導入する場合、lease は `sum(vision_timeout + claude_timeout) ×
安全率` で再導出すべき。現状は「支配項＝Claude 600s ＋ heartbeat/fencing」で 600s とした。

## 6. 【L-01】DRAFT 表記対応注記

状態値の**正本は DB 実値（小文字）**（`received`/`processing`/`vendor_pre`/`sending`/
`completed`/`pending_retry`/`failed`/`unknown`/`duplicate_suspect`）。DRAFT 本文の大文字表記
（RECEIVED 等）は表示名であり、`DRAFT_RV05_DURABLE_INBOUND.md` に対応注記 1 行を追記した。

## 7. テスト・全 suite 実出力

### 7.1 RV-05-13 対象 suite
```
$ PYTHONUTF8=1 python -m pytest test_ingestion_receipt.py test_rv05_13_durable.py -q
26 passed, 5 warnings, 2 subtests passed in 4.91s
```
内訳: `test_ingestion_receipt.py` 14（ledger／fencing／M-01／M-04）・
`test_rv05_13_durable.py` 12（LINE Phase A／sortation 同期／flag OFF 機械担保／
H-02／H-03／H-04）。

### 7.2 sink 方針・sentinel（RV-10 台帳 resync 後）
```
$ PYTHONUTF8=1 python -m pytest test_sink_ast_policy.py test_redaction_sentinels.py -q
17 passed, 89 subtests passed in 7.02s
```
台帳 resync は `sink:logger` 9 件の行移動のみ（total 61 不変・baseline 211 の
単調減少維持・manifest 不変・新規違反ゼロ）。

### 7.3 全 suite
```
$ PYTHONUTF8=1 python -m pytest -q
1 failed, 1359 passed, 5 warnings, 439 subtests passed in 43.17s
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy
```

**唯一の FAIL は本修正と無関係の既存アーティファクト**。`test_classification_accuracy`
は `@skipUnless(ANTHROPIC_API_KEY)` の**実 Claude API 呼出テスト**で、単体実行では
skip される。full suite では先行テストモジュールが `os.environ` に
`ANTHROPIC_API_KEY="dummy"` を残すため skip 条件が外れ、dummy キーで実 API を叩いて
失敗する（accuracy < 閾値）。

- **修正前 SHA 8dfb2d3 でも同一 FAIL を実測**（下記）ため、本修正が原因でない:
```
# worktree 8dfb2d3（clean base）
$ PYTHONUTF8=1 python -m pytest -q
1 failed, 1354 passed, 5 warnings, 439 subtests passed in 46.82s
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy
```
- Δ = base 1,354 → 本ブランチ 1,359 = **+5 passed（本修正の新規テスト）・回帰ゼロ・
  同一 pre-existing FAIL 1**。実 `ANTHROPIC_API_KEY` を与える `railway run` 実行では
  この行も pass する（実装 work-log の 1,347 passed は real key 実行時の値）。
