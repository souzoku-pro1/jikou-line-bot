# 作業記録 2026-08-09: 日次統合（MAINT-2・索引形式）

- TASK_ID: MAINT-2／実施: PC-A＋[人]（push/マージ/実機操作）＋司令塔（裁定・レビュー）
- 位置づけ: **本 log は 2026-08-09（10 日空白からの再開日）の全成果の索引**。
  詳細の正本は各 DRAFT・各 work-log・PR 履歴（本文で参照）。
- 記録日: 2026-08-10（8/9 深夜跨ぎ分を含む）。

## 1. 成果索引（時系列・merge SHA / レビュー ID つき）

| # | 成果 | PR / SHA | 正本・詳細 |
|---|---|---|---|
| 1 | **現況突合**（再開・盤面確認） | — | origin/main=#187（`5e82a7b`）・全 suite **1911 passed** 実測・work-log 最新 7/27・未マージは MAINT-1 のみ、を機械確認 |
| 2 | **MAINT-1 着地**（held封筒×find_existing統合pin・ENVELOPE_FLOW改定記録・bucket集合完全一致テスト） | **#188**（`1afcd0f`→merge `791ba35`） | R-MAINT-1 PASS |
| 3 | **koseki lane2 ゲート4クローズ**（D-7 能動404・legacy_blocked 当日採取） | **#189**（`f7817cf`→merge `861458a`） | `2026-08-09_P2-koseki-lane2-D7-close.md`（[人]能動404×3・21:13/21:19/21:23 JST・legacy_blocked=3 と ok=0 の分離計数・現デプロイ世代内） |
| 4 | **lane2 証跡③ retirement クローズ**（3点充足） | **#190**（`e7b0a9d`+`98b9a28`→merge `4afc4be`） | `2026-08-09_P2-koseki-lane2-retirement.md`（env削除・GAS定数削除・22:40 JST トリガー跨ぎ実見。7/06 log の token 値断片マスク=規律17 同梱） |
| 5 | **P3-003C 設計凍結**（held/rejected 語彙） | **#191**（4 commits→merge `42b486e`） | `DRAFT_P3_003C_HELD_REJECTED.md`＝**FROZEN**。レビュー: R-P3-003C-D1=CR→fix1→D2=CR→fix2→**D3=PASS**。裁定①〜⑥全確定（①allowlist 3値対称＝A・②held封筒維持＝A・③rejected行き止まり受容＝A・④取消別票＝A・⑤理由記録なし＝A・⑥文言第1案） |
| 6 | **P3-003C-IMPL 実装完了**（レビュー中） | branch `p3-003c-impl` **`0ba7fb3`**（未マージ） | **R-P3-003C-IMPL-1 レビュー中**。leaf判定一般化・語彙/復唱・App36照会ゼロ構造・単一App30 update・例外正規化・test_p3_003c_impl.py 26テスト |
| 7 | **実機デー事前調査**（ブロックA/E・読取のみ） | — | 本 log §2 |
| 8 | **MAINT-2**（本票・日次統合＋台帳同期＋Release A 棚卸し） | branch `docs/daily-0809-ledger-sync` | 台帳 `automation-task-ledger.md` 2026-08-09 追記節・Release A 棚卸しは完了報告（チャット）＋台帳依存注記 |

- テスト系譜（本日区間）: **1911 → 1937**（P3-003C-IMPL の +26。全 PASS・
  `--ignore=test_triage_classification.py` 基準）。

## 2. 実機デー事前調査の結果固定（2026-08-09・読取のみ・点火なし）

### 2.1 ブロックA（durable 点火・§8.1 P0）

- **(a) alembic: NG＝点火中止条件該当**——`heads(code)=e7a3c9d2b5f1`／
  `current(db)=c4f1a2b7d8e9`。**未適用 2 本**: `d5e2b8a1c7f3`（P3-001
  derivation_run+heir_confirmation_decision）・`e7a3c9d2b5f1`（P3-002
  template_version）＝いずれも P3 系＝**ブロックEの前提表**。解消は
  `railway run alembic upgrade head`（[人]・実機デー最初の工程が合理的）。
- (b) durable 必須3表（inbound_event/ingestion_receipt/processing_attempt）:
  **機械照合 OK**（列/FK/index/UNIQUE 完全一致・signature_nonce 存在）。
- (c) `STRIPE_EVENT_JOURNAL_ENABLED = 1`（live 実測）。
- (d) baseline: 両表 **0 件**（クエリ成功・点火直前の再採取で当日値を固定する運用）。

### 2.2 ブロックE（相続人導出点火）env 現況（名前の有無のみ・値非表示）

- `APP_SOUZOKUNIN`/`TOKEN_SOUZOKUNIN`＝投入済み・`APP_SHIPPING`/`TOKEN_SHIPPING`＝投入済み
- `ATTORNEY_ALLOWLIST`＝**未投入**（点火前に要投入・flag より先）
- `HEIR_DERIVATION_ENABLED`＝未投入=OFF（期待どおり）
- 投入手順書ドラフトは 8/9 チャット報告（ALLOWLIST→flag の順・受理値 1/true/on/yes・
  rollback=flag 削除で即 OFF）。

### 2.3 Windows 環境注記（runbook 注記候補・司令塔裁定待ち）

- §8.1 の検査 2/4（psycopg async）は Windows の ProactorEventLoop と非互換で
  素のままでは落ちる。**`WindowsSelectorEventLoopPolicy` 指定を加えた同一 SELECT**
  で実行した（読取内容は逐語同一）。実機デーに[人]が Windows から実行する場合も
  同じ対処が必要——runbook（DRAFT_P2_DURABLE_IGNITION §8.1）への注記追加を提案。

## 3. 残タスクの現在地（8/9 終了時点）

- **R-P3-003C-IMPL-1**: レビュー中（対象 `0ba7fb3`・BASE `42b486e`）。所見→fix 巡→
  マージで held/rejected 語彙が本番入り（flag OFF のため挙動不変）。
- **実機デー（点火群）**: migration 2 本適用 → ブロックA（durable）→
  ブロックE（ALLOWLIST→flag）→各ゲート確認。手順・中止条件は 8/9 調査で固定済み。
- **lane3（bank/valuation）**: G-L3-0 ゲート（DRAFT_P2_LANE3 §）・司令塔裁定待ち。
- **P4-003（書類到着状況）**: scan 20 件（大野提供サンプル）到着が設計着手ゲート。
- **P4-005（相続人関係図）**: P3 merge 済みにより**解錠済み・未着手**（次票候補）。
- **P5-002 以降（協議書 Word 化）**: P5-001（条項ライブラリ・#172）まで完了。
  書式受入（[人]）が次ゲート。

## 4. 枠消化の日次一行

- 2026-08-09: 再開日として現況突合→MAINT-1 着地→lane2 ゲート4/retirement 完全
  クローズ（#188〜#190）→P3-003C 設計凍結（#191・D3 巡 PASS）→P3-003C-IMPL 実装
  （0ba7fb3・レビュー中）→実機デー事前調査（migration 2 本未適用の発見）→MAINT-2
  日次統合。テスト 1911→1937。モデル実測 = Fable 5。
