# 作業記録 2026-07-14: RV-05-13-fix4（R-RV-05-13-4 所見の修正）

- 対象: R-RV-05-13-4（fix3 = cf08b12 への再レビュー）の必須修正
- BASE: cf08b12 / BRANCH: feat/rv05-13-durable-inbound（継続）
- 正本: DRAFT_RV05_DURABLE_INBOUND.md rev5（本文契約は不変）
- DO_NOT_CHANGE 遵守: fencing core・§H-06 応答表・顧客Bot本体・Stripe・ingestion_receipt 側契約 は未接触

## 0. 結論サマリ

- **H-NEW-01-R2**（processing 滞留の回収）: fix3 までは `processing` のまま hard-kill された行
  （mark_line_failed も走らなかった）が再配送でも skip され続け滞留し得た。re-attempt guard に
  **stale processing 回収**（claim 後 stale 秒超の processing を再 claim）を追加。回収駆動は
  **LINE 再配送のみ**（専用 reconciliation は持たない・Phase A 維持）。
- **M-01-R**: work-log の lease 主張を「**運用上限＋fencing 最終防衛**」へ修正（§5）。Vision
  ページ上限を実装で強制（超過は安全側で ask 縮退）。
- **M-02-R**: attempts の加算を **claim 成功（rowcount 1）時のみ**へ（skip 時は加算しない）。

- 対象 suite（`test_rv05_13_durable.py`＋`test_ingestion_receipt.py`）: **35 passed**（fix3 33 ＋2）
  ＋ page-cap テスト（sortation 側）で **合計 +3**。
- 全 suite: §6（base 比較つき）。

## 1. inbound_event 列の確認結果（claimed_at 相当の有無と選択）

**確認結果: `inbound_event.claimed_at`（`DateTime(timezone=True)`）は既存**。
- モデル: `hub/inbound_event.py` の `InboundEvent.claimed_at`（Stripe の stale processing 判定
  D12/RCF-M06 用に導入済み）。
- migration: `alembic/versions/20260711_f8ef81de70a5_inbound_event_claimed_at.py`（add_column 済）。

**選択: 既存 `claimed_at` 列を LINE の stale processing 回収に流用**（新規 migration は作らない）。
本 branch の migration `c4f1a2b7d8e9`（ingestion_receipt）へは一切追加していない（票の指示どおり）。
LINE 経路では claim 成功時に `claimed_at=now()`（SQL 側）を書き、`received`（未 claim）は NULL、
旧行の NULL も stale 扱いで回収対象にする。

## 2. 所見 → 修正 対応表

| 所見 | 修正 | 実装位置 | 担保テスト |
|---|---|---|---|
| **H-NEW-01-R2** | re-attempt guard に stale processing 回収を追加: `state IN ('received','failed') OR (state='processing' AND (claimed_at IS NULL OR claimed_at < now()-Nsec))` かつ `attempts<max`。claim 成功時のみ `claimed_at=now()`。回収駆動＝LINE 再配送のみ | `hub/durable_inbound.py` record_line_event・`_line_stale_cutoff`・`line_stale_processing_seconds`（既定3600s） | test_mtest01_fresh_processing_within_threshold_skips（①）・test_mtest01_stale_processing_reclaims_once（②） |
| **M-01-R(a)** | work-log §5 の lease 主張を「運用上限＋fencing 最終防衛」へ修正 | §5 | — |
| **M-01-R(b)** | Vision ページ上限を実装で強制（既定25＝5バッチ×5ページ・D1 整合・env SORTATION_MAX_PAGES）。超過は OCR せず安全側 ask 縮退 | `sortation_ingest.py` `_sortation_max_pages`・page-cap ガード | test_mnew01r_page_cap_forces_ask_no_ocr |
| **M-02-R** | attempts 加算を claim 成功（rowcount 1）時のみへ。skip（terminal／実行中）時の観測 bump を廃止 | `hub/durable_inbound.py` record_line_event | test_m02_attempts_exhaust_to_failed_terminal（加算停止を継続担保） |
| **M-TEST-01** | ①fresh processing→skip ②stale processing→再claim・登録1回 のテスト追加 | test 追補 | 上記①② |

## 3. H-NEW-01-R2（stale processing 回収）の設計と比較裁定

### 3.1 guard
claim 対象 = `received`／`failed`、**または** claim 後 stale 秒（既定3600s）を超えた
`processing`（`claimed_at` NULL の旧行も対象）。かつ `attempts<max`。claim 成功時に
`state='processing', attempts++, claimed_at=now()`（排他・SQL 側 now()）。

### 3.2 回収駆動は LINE 再配送のみ（Phase A 維持）
専用の reconciliation ループは持たない。GAS/LINE の再配送が来たときに record_line_event が
stale processing を再 claim する。DRAFT §3.1（Phase A＝記録＋観測・自動 replay なし）の枠内。

### 3.3 比較裁定（**滞留 vs 併走のトレードオフ**）
stale 回収は「真にクラッシュした処理」だけでなく「stale 秒を超えて**まだ生きている**処理」も
再 claim し得る（＝二重処理・二重返信の可能性）。それでも回収する理由:

- **回収しない場合の失敗様態＝滞留**: 返信が**0 回**（顧客への沈黙）。監視でも「来ていない」は
  検知しづらい（HOTFIX-01 型の沈黙全滅と同種）。
- **回収する場合の最悪様態＝併走**: 返信が**2 回**。顧客・事務所とも**目に見えて検知可能**で、
  運用で気付いて是正できる。
- **裁定: 検知可能な 2 回返信（安全側）> 検知困難な 0 回沈黙**。stale 閾値を 3600s と長めに
  取ることで、健全処理を誤って併走させる確率は実務上小さい（LINE 応答は通常秒オーダー）。

正しさの最終防衛は fencing（排他 claim・§4）であり、stale 回収は「滞留を沈黙のまま放置しない」
ための可視・回収機構である。

## 4. M-02-R（attempts は claim 成功時のみ加算）

fix3 では skip 分岐（実行中 processing の重複）で観測目的に attempts を bump していた。これは
「attempts＝claim 回数」の意味を曖昧にし、`attempts<max` guard の精度を落とす。fix4 で
**加算は排他 claim 成功（rowcount 1）時のみ**に統一。再送圧の観測は §6 カウンタ（series A
dedup_skip 等）で行い、attempts は純粋に claim 回数を表す。exhaustion 遷移・skip では加算しない。

## 5. 【M-01-R】lease の位置づけ修正 と Vision ページ上限

### 5.1 lease の主張を「運用上限＋fencing 最終防衛」へ修正
fix1〜fix3 の §5 は lease 値（600→1800→4500）を「最悪合計」と記していたが、これは
「lease が相互排他の上界を保証する」かのような**過大主張**だった。正しい位置づけ:

- **lease（reconcile_stale 秒・4500s）は運用上限**: 通常運用で処理が収まると見込む上限であり、
  「健全な処理中を誤って stale とみなさない」ための liveness 下限。外部 call の設定 timeout
  （Vision 120×バッチ・Claude primary 1800・fallback 1800・backoff/前後）の合算を目安に置くが、
  **これを超えても正しさは壊れない**。
- **fencing が最終防衛**: 誤って stale 判定・再 claim（epoch++）が起きても、in-flight の
  terminal/heartbeat は epoch guard で 0 行 abort（RV-05-13-fix §4.6・test_m04）。二重処理・
  二重返信を実際に不可能にするのは fencing（および LINE 側の排他 claim）であって lease ではない。

（fix1 §5＝600・fix2 §5＝1800・fix3 §5＝4500「最悪合計」。本 fix4 §5 が最新の正本＝
lease は運用上限、正しさは fencing。値は 4500 のまま。）

### 5.2 Vision ページ上限の実装強制
lease 定量が前提とする「≤バッチ数×5ページ」を実装で強制する。`sortation_ingest` は OCR 前に
`_pdf_page_count` を取り、`SORTATION_MAX_PAGES`（既定25＝5バッチ×5ページ・D1 の 5ページ/req
制約と整合）を超えたら **OCR/split を回さず** OCR try 内で ValueError を送出→既存の ask 安全側
縮退（doc_type=不明で人手へ）に落ちる。判定不能（PyMuPDF 不在等で page count=None）は従来どおり
（Vision 既定5ページに縮退）。上限超過を沈黙処理せず人手検知に回すのが安全側（§3.3 と同思想）。

## 6. テスト・全 suite 実出力（base 比較）

### 6.1 対象 suite
```
$ PYTHONUTF8=1 python -m pytest test_rv05_13_durable.py test_ingestion_receipt.py -q
35 passed, 5 warnings, 10 subtests passed in 8.22s
```
fix3 の 33 ＋2（M-TEST-01 ①②）。page-cap テストを含めた durable 単体 22 passed。

### 6.2 sink 方針・sentinel（台帳 resync 後）
```
$ PYTHONUTF8=1 python -m pytest test_sink_ast_policy.py test_redaction_sentinels.py -q
（緑）
```
resync は sortation_ingest.py の `sink:logger` 7 件の行移動のみ（total 61 不変・baseline 211
単調減少維持・manifest 不変・新規違反ゼロ）。

### 6.3 全 suite（base cf08b12 比較）
```
$ PYTHONUTF8=1 python -m pytest -q
# fix4（本ブランチ HEAD）
1 failed, 1369 passed, 5 warnings, 447 subtests passed in 44.77s
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy

# base cf08b12（fix3）
1 failed, 1366 passed, 5 warnings, 447 subtests passed
FAILED test_triage_classification.py::TestTriageClassification::test_classification_accuracy
```
- Δ = 1,366 → **1,369 passed（＋3＝M-TEST-01 ①②＋page-cap）・回帰ゼロ**。
- 唯一の FAIL は fix1 で実証済みの**既存アーティファクト**（`@skipUnless(ANTHROPIC_API_KEY)` の
  実 Claude API テストが full suite の env 漏れで dummy キー実行され落ちる）。base でも同一に発生し
  本 fix と無関係。real key の `railway run` 実行では pass。
