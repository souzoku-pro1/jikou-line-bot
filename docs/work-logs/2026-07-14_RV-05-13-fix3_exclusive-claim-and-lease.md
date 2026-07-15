# 作業記録 2026-07-14: RV-05-13-fix3（R-RV-05-13-3 所見の修正）

- 対象: R-RV-05-13-3（fix2 = 7401d7c への再レビュー）の必須修正
- BASE: 7401d7c / BRANCH: feat/rv05-13-durable-inbound（継続）
- 正本: DRAFT_RV05_DURABLE_INBOUND.md rev5（本文契約は不変）
- DO_NOT_CHANGE 遵守: fencing core・§H-06 応答表・顧客Bot本体・Stripe は未接触

## 0. 結論サマリ

- **H-NEW-01-R**（re-attempt の非排他）: fix2 の re-attempt は target を `state='received'` に
  していたため、同一 event の 2 配送が両方 guard（`state IN (received,failed)`）に到達すると
  **両方が rowcount 1 で "reattempt"** を返し、処理タスクが 2 回登録され得た（二重返信）。
  target を `state='processing'` へ変え**排他 claim**化（勝者 1 者・敗者は guard 不成立で skip）。
- **M-02**（attempts 上限後の無限加算）: 上限到達で `failed_exhausted` terminal（理由付き）へ
  遷移し、以後の重複再送で attempts 加算を停止。§6 カウンタ（series B）で可視化。
- **M-01**（lease 過小）: lease 1800→**4500s**。Vision＋Claude primary＋fallback＋backoff/前後の
  最悪合計で再定量（§5）。

- 対象 suite（`test_rv05_13_durable.py`＋`test_ingestion_receipt.py`）: **33 passed**（fix2 31 ＋2）。
- 全 suite: §6（base 比較つき）。

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 実装位置 | 担保テスト |
|---|---|---|---|
| **H-NEW-01-R** | re-attempt を排他 claim へ。`UPDATE … SET state='processing', attempts=attempts+1 WHERE dedup_key=? AND state IN ('received','failed') AND attempts<max`（rowcount 1 のみ reattempt・0 は skip）。processing 中 duplicate は既存分岐で skip | `hub/durable_inbound.py` record_line_event | test_hnew01r_reattempt_is_exclusive_claim（同時2配送→登録1回・旧コードは2でFAIL） |
| **M-01** | lease 1800→4500s（`_DEFAULT_STALE_SECONDS`）。根拠は §5 に完全列挙 | `hub/durable_inbound.py` | §5（全 suite 回帰なし） |
| **M-02** | attempts 上限到達で `failed_exhausted` terminal（`last_error="attempts_exhausted"`）へ。以後の重複再送は guard 不成立で attempts 加算停止。§6 series B（`failed_exhausted`）で可視 | `hub/durable_inbound.py` record_line_event | test_m02_attempts_exhaust_to_failed_terminal |
| **M-03** | 上記 H-NEW-01-R テストで吸収 | — | test_hnew01r_reattempt_is_exclusive_claim |

## 2. H-NEW-01-R（排他 claim）の設計

### 2.1 fix2 の欠陥
fix2 の re-attempt UPDATE は `.values(state="received", attempts=attempts+1)`。target が
`received` のままなので、同一 event の同時 2 配送が両方 `state IN (received,failed)` guard を
満たし、直列化後も**両方 rowcount 1**（1者目 received→received、2者目も received のまま）で
"reattempt" を返す。→ 処理タスク 2 重登録＝二重返信の再発。

### 2.2 修正（排他化）
target を `state='processing'` へ:
- 1者目: `received→processing`（rowcount 1）→ "reattempt"。
- 2者目: guard `state IN (received,failed)` に対し state は既に `processing` → rowcount 0 →
  claim 敗者。以降 exhausted guard も不成立 → 実行中 duplicate として skip（"duplicate"）。

DB の行ロックで 2 UPDATE は直列化され、2者目は**1者目コミット後の新 state（processing）**で
guard 再評価されるため、排他が成立する（postgres/sqlite いずれも行単位で直列）。

### 2.3 background との整合
"reattempt" で state が既に `processing` のため、`_process_line_event_durable` の
`mark_line_processing`（WHERE state='received'）は no-op（count は発火）。`mark_line_completed`
（processing→done）は通常どおり成立。→ 処理完了・観測は不変、二重処理のみを除去。

## 3. M-02（attempts 上限 → terminal・加算停止）

record_line_event の重複分岐を 3 段に:
1. **排他 claim**（received/failed・`attempts<max` → processing）。成功＝reattempt。
2. **上限到達遷移**（received/failed・`attempts>=max` → `failed_exhausted`・理由
   `attempts_exhausted`・processed_at=now()）。成功＝count series B `failed_exhausted`＋skip。
3. **terminal/実行中の skip**: done／failed_exhausted は WHERE から除外（**attempts 加算停止**）。
   processing（実行中・claim 敗者）のみ再送圧の観測として attempts を bump。

max（既定5・env `INBOUND_LINE_MAX_ATTEMPTS`）到達までに正確に max 回の処理機会を与え、
max 回目も失敗したら terminal 化する（poison event の無限 re-attempt / 無限加算を止める）。

## 4. 位置づけ（fencing と lease の役割）

**fencing（epoch/state guard）が最終防衛**であり、二重処理・二重返信を実際に不可能にするのは
排他 claim と terminal guard である。**lease（reconcile 秒数）は誤 stale 判定を減らすための値**で
あって、それ自体は正しさを保証しない（誤って reclaim しても fencing で late write は abort）。

## 5. 【M-01】lease 4500s の再定量化

単一 claim が 2 つの fence 更新書込（claim/mark_phase/mark_terminal は各々
`last_heartbeat_at=now()` を書く）の**間**、receipt を非終端で保持しうる最悪壁時計を列挙:

| 要素 | 最悪値 | 内訳 |
|---|---|---|
| Vision OCR | 120s × 最大バッチ数 5 = **600s** | 明示 timeout 120s（fix2）× 5ページ/バッチ・≤25ページ天井（sortation 書類の現実的上限） |
| Claude PRIMARY | **1800s** | read timeout 600s ×(1+SDK retry 2) |
| Claude FALLBACK | **1800s** | gateway の FALLBACK_MODEL も SDK で 600×3 |
| SDK backoff（retry 間指数） | **~100s** | PRIMARY/FALLBACK の retry 間 backoff の合計概算 |
| 前後処理（kintone ask httpx 5s×数回・marker 書込・response 構築） | **~200s** | ask 保存/通知（httpx 既定 5s）＋DB 書込 |
| **合計** | **≈ 4500s** | 600+1800+1800+100+200 |

- 支配区間は `claim→vendor_pre`（Vision＋Claude）。fix2 の 1800 は Vision バッチと FALLBACK を
  数えておらず過小だった。4500 は上表の最悪合計に一致させた値。
- **正しさは lease に依存しない**（§4）: 4500 を超えて誤 reclaim されても in-flight の
  terminal/heartbeat は epoch guard で 0 行 abort（RV-05-13-fix work-log §4.6・test_m04）。
  4500 は「健全な処理中を誤って stale とみなさない」liveness 下限。
- env `INBOUND_RECONCILE_STALE_SECONDS` で上書き可。

（fix1 §5＝600・fix2 §5＝1800 前提。本 fix3 §5 が最新の正本。）

## 6. テスト・全 suite 実出力（base 比較）

### 6.1 対象 suite
```
$ PYTHONUTF8=1 python -m pytest test_rv05_13_durable.py test_ingestion_receipt.py -q
33 passed, 5 warnings, 10 subtests passed in 6.00s
```
fix2 の 31 ＋2（H-NEW-01-R 排他 claim・M-02 exhaustion）。

### 6.2 全 suite（base 7401d7c 比較）
```
$ PYTHONUTF8=1 python -m pytest -q
# fix3（本ブランチ HEAD）
1 failed, 1366 passed, 5 warnings, 447 subtests passed in 44.19s
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy

# base 7401d7c（fix2）
1 failed, 1364 passed, 5 warnings, 447 subtests passed
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy
```
- Δ = 1,364 → **1,366 passed（＋2＝本 fix の新規テスト）・回帰ゼロ**。
- 唯一の FAIL は fix1 で実証済みの**既存アーティファクト**（`@skipUnless(ANTHROPIC_API_KEY)` の
  実 Claude API テストが full suite の env 漏れで dummy キー実行され落ちる）。base でも同一に発生し
  本 fix と無関係。real key の `railway run` 実行では pass。
- sink 台帳: 本 fix は sink 行のシフトなし（resync 不要・total 61 不変）。
