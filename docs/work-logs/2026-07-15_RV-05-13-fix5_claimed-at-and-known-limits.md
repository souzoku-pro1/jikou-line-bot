# 作業記録 2026-07-15: RV-05-13-fix5（R-RV-05-13-5 所見の修正）

- 対象: R-RV-05-13-5（fix4 = a1d35b5 への再レビュー）の必須修正
- BASE: a1d35b5 / BRANCH: feat/rv05-13-durable-inbound（継続）
- 正本: DRAFT_RV05_DURABLE_INBOUND.md rev5（§H-01 逸脱宣言節に既知制約を追記）
- DO_NOT_CHANGE 遵守: fencing core・§H-06 応答表・顧客Bot本体・Stripe は未接触

## 0. 結論サマリ

- **H-NEW-02**: `mark_line_processing()` が `claimed_at` を書かないため、本番経路
  （新規 insert→mark_line_processing）の **fresh processing が重複配送の NULL stale 救済に
  拾われて再 claim＝併走**し得た。`claimed_at=now()`（SQL 側）を設定して閉塞。
  NULL stale guard 自体は **fix5 以前の旧行救済として維持**。
- **H-NEW-01-R3**: 既知制約として明文化（コード変更なし）——「LINE 再配送終了後の stale 行は
  自動回収の契機を失い滞留。検知は §6 観測・回収は人手 reset」を DRAFT §H-01＋本 work-log へ。
  人手 reset runbook は §4。
- **M-DOC-01**: fix4 work-log §3.3/§5.1 の「fencing が二重処理を不可能にする」を
  「claim UPDATE は併走 claim 間の排他のみ・旧 task 失効 guard なし・まれな併走は
  比較裁定どおり受容・検知可能」へ正確化。
- **L-TEST-01**: page-cap テストに split 解析・Claude 判定の未呼出 assert を追補。
- 対象 suite: **37 passed**（base 36 ＋1＝H-NEW-02 テスト）。全 suite は §6。

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 実装位置 | 担保テスト |
|---|---|---|---|
| **H-NEW-02** | `mark_line_processing()` で `state='processing'` と同時に `claimed_at=now()`（SQL 側）を設定。record_line_event の NULL stale guard は旧行救済として維持（コメントで役割を明記） | `hub/durable_inbound.py` mark_line_processing | test_hnew02_fresh_processing_not_reclaimed_on_production_path（旧コードで "reattempt"＝併走になり **FAIL する形**・実測済み） |
| **H-NEW-01-R3** | 既知制約の明文化（コード変更なし）: DRAFT §H-01 逸脱宣言節＋本 work-log §3。人手 reset runbook を §4 に1節追加。K4 ON の再配送持続時間（指数バックオフ）と 3600 秒閾値の整合も記載 | DRAFT §H-01・本 §3/§4 | —（docs） |
| **M-DOC-01** | fix4 work-log §3.3/§5.1 の fencing 過大主張を正確化 | fix4 work-log §3.3/§5.1 | —（docs） |
| **L-TEST-01** | page-cap テストに split 解析（split flag ON でも）・Claude 判定の未呼出 assert を追補 | test_mnew01r_page_cap_forces_ask_no_ocr | 同左 |

## 2. H-NEW-02（初回 claimed_at 設定）

### 2.1 穴の構造（fix4 まで）
`claimed_at` を書くのは record_line_event の**排他 claim（重複配送側）のみ**で、本番の主経路
（新規 insert `state='received'` → BackgroundTasks → `mark_line_processing` → processing）では
**NULL のまま**だった。重複配送の claim guard は `processing AND (claimed_at IS NULL OR
claimed_at < now()-Nsec)` を claim 可能とするため、**処理中の fresh processing が NULL 側の条件で
即・再 claim され併走（二重返信リスク）**になる。stale 閾値 3600 秒は NULL には効いていなかった。

### 2.2 修正
`mark_line_processing()` の UPDATE に `claimed_at=sa.func.now()`（DB clock・H-05 と同流儀）を追加。
これで本番経路の processing 行は常に claim 時刻を持ち、重複配送は 3600 秒閾値内なら skip される。
`claimed_at IS NULL` の guard 分岐は**削除しない**——fix5 デプロイ以前に processing で滞留した
旧行（claimed_at NULL）の回収経路として維持（役割をコメントに明記）。

### 2.3 担保テスト（旧コードで FAIL する形・実測）
`test_hnew02_fresh_processing_not_reclaimed_on_production_path`: 本番経路どおり署名付き webhook
POST（新規 insert）→ 背景 wrapper が `mark_line_processing` を通過 → 処理本体の実行中に重複配送
（record_line_event）到着、を再現。修正後は `"duplicate"`（skip・attempts=1 のまま・claimed_at
非 NULL・最終 done）。**旧コード（claimed_at なし）に対して実測で FAILED を確認**
（"reattempt"＝再 claim・併走）。

## 3. H-NEW-01-R3（既知制約の明文化・コード変更なし）

**制約**: stale processing 回収（fix4）の駆動は **LINE 再配送のみ**。LINE 再配送が終了した後に
stale 化した行（crash 滞留）は自動回収の契機を失う——**台帳上は人手 reset まで non-terminal の
まま滞留**し、顧客対応上は**次のイベント（顧客の再発話等）まで沈黙**する。次のイベントは別
dedup_key であり**旧行を回収しない**。

- **検知**: §6 観測——**最古 non-terminal 滞留時間**（LINE は processing 滞留）と**収束率低下**
  （daily_healthcheck / dead-man 統合・DRAFT §6 軸A）。
- **回収**: **人手 reset**（runbook: §4）。専用 reconciliation は導入しない（Phase A＝
  「継続 worker/自動 replay を持たない」原則の帰結として受容し、解消は RV-06）。
- **K4 整合（1行）**: K4 ON の LINE 再配送は指数バックオフで後期ほど疎になるため、stale 閾値
  3600 秒（既定）を超える頃には再送が尽きていることがあり得る——閾値は再配送ウィンドウ内の
  過剰併走を抑える下限であり、末尾再送による回収は保証しない（保証しない前提で本制約を宣言）。

DRAFT §H-01（逸脱宣言節）にも同旨を追記済み。

## 4. 人手 reset runbook（LINE stale processing）

前提: 本番は Railway PostgreSQL。PC-A からは `DATABASE_PUBLIC_URL` 経由で接続する
（P1-004 migration 基盤の work-log と同流儀・internal host は PC-A から不達・URL 値は表示しない）。**Phase A は payload 本文を保存しない（payload_hash のみ）ため、サーバ側
からの本文再現・自動再処理は不可能**（DRAFT §H-NEW-04）。回復の実体は顧客への聞き直し（DRAFT
§9.4「同じ質問を再提示」）であり、reset は台帳を収束させ再 claim 可能にする操作である。

### 4.1 対象特定
```sql
SELECT id, external_event_id, caller_id, attempts, received_at, claimed_at
FROM inbound_event
WHERE provider = 'line' AND state = 'processing'
  AND (claimed_at IS NULL OR claimed_at < now() - interval '3600 seconds')
ORDER BY received_at;
```
（閾値は env `INBOUND_LINE_STALE_PROCESSING_SECONDS` の実運用値に合わせる。0 件なら滞留なしで終了。）

### 4.2 UPDATE 文の完成形
対象 `id` を確認のうえ、目的別にどちらか一方を実行する。

**(a) 再 claim 可能へ戻す**（再配送がまだ来得る／検証で再処理させたい場合）:
```sql
UPDATE inbound_event
SET state = 'failed', last_error = 'manual_reset_stale_processing'
WHERE provider = 'line' AND state = 'processing' AND id = <対象id>
  AND (claimed_at IS NULL OR claimed_at < now() - interval '3600 seconds');
```

**(b) 打ち切って収束させる**（再配送終了済みで再処理の見込みなし・顧客対応は聞き直しで実施）:
```sql
UPDATE inbound_event
SET state = 'failed_exhausted', processed_at = now(),
    last_error = 'manual_reset_stale_processing_closed'
WHERE provider = 'line' AND state = 'processing' AND id = <対象id>
  AND (claimed_at IS NULL OR claimed_at < now() - interval '3600 seconds');
```
（いずれも stale 条件を WHERE に残す＝**生きている処理を誤って reset しない**排他。rowcount 0 は
「その行が動いた＝処理が生きていた」なので再度 4.1 から。）

### 4.3 再処理確認
1. (a) の場合: 再配送/重複到着で `attempts` が増え `state` が `processing→done` へ遷移することを
   4.1 と同型の SELECT（`WHERE id = <対象id>`）で確認。再配送が来ない場合は自動再処理は起きない
   ——顧客へ聞き直し（§9.4）で回復し、行は (b) で収束させる。
2. (b) の場合: `state='failed_exhausted'` を確認（以後の重複は skip・attempts 加算なし）。
3. いずれも §6 観測で **最古 non-terminal 滞留の解消・収束率の回復**を確認して終了。

## 5. M-DOC-01 / L-TEST-01

- **M-DOC-01**: fix4 work-log §3.3 末尾・§5.1 の「正しさの最終防衛は fencing」「二重処理・
  二重返信を実際に不可能にする」を撤回し、「LINE の claim UPDATE は**併走 claim 間の排他のみ**・
  **旧 task 失効 guard なし**（epoch なし・§H-01）・まれな併走は**比較裁定どおり受容・検知可能**」
  へ正確化（fix4 work-log を直接修正・修正である旨を注記）。sortation の epoch fencing の
  0 行 abort（test_m04）は事実として維持。
- **L-TEST-01**: `test_mnew01r_page_cap_forces_ask_no_ocr` に `SORTATION_SPLIT_ENABLED=1` を与えた
  うえで `_try_split_analysis` の `assert_not_awaited()`（上限超過は split 解析も回さない）と
  `_judge_with_claude` の `assert_not_awaited()`（Claude 判定も呼ばない）を追補。

## 6. テスト・全 suite 実出力（base a1d35b5 比較）

### 6.1 対象 suite
```
$ PYTHONUTF8=1 python -m pytest test_rv05_13_durable.py test_ingestion_receipt.py -q
37 passed, 5 warnings, 10 subtests passed in 16.53s
```
base（a1d35b5）は同 2 ファイル 36 collected ＝ **+1（H-NEW-02 テスト）**。

### 6.2 全 suite（base a1d35b5 比較）
```
$ PYTHONUTF8=1 python -m pytest -q
# fix5（本修正）
1 failed, 1370 passed, 5 warnings, 447 subtests passed in 71.34s
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy

# base a1d35b5（fix4・別 worktree で実測）
1 failed, 1369 passed, 5 warnings, 447 subtests passed in 70.00s
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy
```
- Δ = 1,369 → **1,370 passed（＋1＝H-NEW-02 テスト）・回帰ゼロ**。
- 唯一の FAIL は fix1 以来の**既存アーティファクト**（実 Claude API テストが full suite の
  env 漏れで dummy キー実行され落ちる）。base でも同一に発生し本 fix と無関係。

## 7. 枠消化の日次一行
- 2026-07-15: RV-05-13-fix5（H-NEW-02 claimed_at・H-NEW-01-R3 明文化＋runbook・M-DOC-01・
  L-TEST-01）。開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
