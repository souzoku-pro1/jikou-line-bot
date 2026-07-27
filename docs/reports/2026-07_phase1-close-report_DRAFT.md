# Phase 1 完了報告（DRAFT・PC-A 整形／司令塔確定待ち）

- 状態: **DRAFT**（PR 化は #141 マージ後・司令塔指示後）。費用欄は司令塔記入。
- 期間: **2026-07-11（Phase 0 クローズ／Phase 1 着手）〜 2026-07-18（S5 cutover）**。
- 基準 SHA（本 DRAFT 作成時 main）: `1d7c425`。

## 1. サマリ

Phase 1 は「顧客 Bot 中核の可観測性・耐障害性の底上げ」と「GAS/kintone 連携の認証堅牢化」を主軸に、
**redaction 契約の機械強制**・**durable 基盤（LINE durable〔Phase A〕のみ flag OFF 待機・Stripe journal は
既存稼働）**・**HMAC 署名移行（RV-04a/b/c）**・
**dead-man/probe 監視**・**dependency lock+SBOM** を達成。実機移行は **sortation lane の署名経路化＋
legacy 停止＋token rotation** まで到達（**retirement evidence 2 点充足・1 点暫定〔§7 (i) 追補待ち〕**・
S5 work-log）。

## 2. マージ PR 一覧（Phase 1・2026-07-11〜18・merge-commit ベース 32 件〔#101–141 の git 集計〕＋merge-commit を持たない PR 1 件〔#130〕＝総数 33 件）

| 期間 | PR（番号: 概要） |
|---|---|
| 07-11 | #101 P1-004 migration 基盤クローズ／#102 P1-005a Stripe inbound／#103 P1-005b journal 硬化／#104 P1-005 クローズ／#105 P1-006 evidence／#106 P1-007 設計 DRAFT |
| 07-12 | #107 P1-007a drafts revision／#108 P1-007b template-fields／#109 P1-007c drafts verify／#110 P1-101 redaction 契約／#111 P1-101 worklog／#112 P1-102 S1 fail-closed／#114 P1-104 S2 応答最小 |
| 07-13 | #116 P1-107a logging 配線／#118 P1-107b print 移送／#120 P1-110 AST 硬化／#122 P1-112 frozen-prints／#124 P1-103 HMAC multipart PoC |
| 07-14 | #125 HOTFIX-01 emit unbound／#127 RV-04a HMAC server core／#128 RV-04b dual-accept |
| 07-15 | #131 RV-05-13 durable inbound／#132 RV05-13 worklog／#133 P1-114 registry fail-fast／#134 P1-113+RCF-M08／#135 RV-12 dependency lock |
| 07-17 | #136 小粒バッチクローズ／#137 RV-04c S2 GAS 署名（+fix1/fix2）／#138 RV-04c S3 kintone レーン（+fix1/fix2）／#139 S4/S5 チェックリスト／#140 S4-1 hash 突合 fix |
| 07-18 | #141 S5 cutover クローズ（**マージ済み 2026-07-19**） |

（※ squash マージ等で merge-commit を持たない PR〔例 #130 K1-K4 手順書〕は本表の git 集計に出ないが main 反映済み。）

## 3. テスト推移（実測系列・work-log ベース）

**1,072**（Phase 1 着手 baseline）→ **1,290**（HOTFIX-01）→ **1,347**（RV-05-13 実装）→ **1,370**（RV-05-13 fix5）
→ **1,379**（RV-05-13 マージ後 base 2b33ffa）→ **1,458**（現 main 7264c16/1d7c425）。単調増加・テスト削除/skip 追加なし。
既知の恒常 1 FAIL は `test_triage_classification`（full-suite の env 漏れで dummy キー実行・real key で pass・全期間同一）。

## 4. Codex レビュー巡数（把握できる範囲・主要ワークストリーム）

- **RV-05-13**: DRAFT 5 巡（R-D〜R-D5）＋実装後 fix 7 巡（R-RV-05-13〜-7）。
- **RV-04c**: DRAFT D2〜D5（司令塔裁定反映 4 版）＋S2 fix1/fix2・S3 fix1/fix2・S4 チェックリスト fix1・S4-1 hash 突合。
- **P1-114/P1-113**: 各 fix1 巡（R-P1-114／R-P1-113-M08）。
- **P1-007/P1-101/P1-103/RV-04**: 設計・PoC・本体で各複数巡（R-P1-007/-101/-103/-103-2、R-RV-04）。
- 各 fix は「修正前 FAIL 実測を .md 保存」の規律で回した（work-log に実出力固定）。

## 5. インシデント総覧と恒久策

| # | インシデント | 恒久策 |
|---|---|---|
| HOTFIX-01（07-14） | `_process_line_event` の関数内 `redact.emit` re-import による UnboundLocalError 全停止（顧客影響ゼロ・棚卸し済み） | 1 行削除＋回帰 3 本／**P1-113 で AST スキャナが関数内 `emit` import を emit_shadow 検知**（tree.body 直下のみ信頼）／顧客 Bot 経路の handler smoke 必須化 |
| RCF-M08 | dead-man 警報の約 2 日周期オシレーション（警報送信自体が heartbeat を更新する自己参照） | stale 時 synthetic probe 実送で死活実測・成功=生存/失敗のみ警報 |
| 7/18-1 GAS アカウント取り違え | 旧版プロジェクトへ誤投入 | 正本へ再投入・旧版から削除（完了7/19・単独所有実見済み・S5 §4(ii)） |
| 7/18-2 registry JSON 破損で起動停止 | — | **P1-114 の正常動作**（沈黙 500 でなく起動 fail-fast）・rollback 即復旧 |
| 7/18-3 Script Properties 形式誤り | key ID を名前欄へ投入 | キー名/値の 2 行形式へ是正 |
| 7/18-4 Vision API billing 403 | 仕分け判定が全件「不明」フォールバック | **RCF-M14 起票**（billing 有効化[人]・API 監視拡張=Phase 2 候補） |
| 7/18-5 rotation 5-3 の 404 継続 | 値の取り違え（7/18 23:33 JST 等） | 最終的に 200・rotation 4 工程完了（D-6b） |

## 6. 実機移行の達成事項

- **HMAC 署名経路**（NM01 v1・length-prefix canonical・nonce DB 一回性・fail-closed／downgrade 防止）を
  ingest 5 入口へ dual-accept 結線。**sortation lane を本番署名化**（GAS→HMAC→200）。
- **legacy 段階停止**（`SERVICE_AUTH_LEGACY_DISABLED_PATHS`・起動 strict 検証・停止 lane 404＋legacy_blocked 計数）。
- **token rotation 2 系統**: ingest（SORTATION 前進失効）／kintone webhook（`KINTONE_WEBHOOK_TOKEN` 4 工程 rotation・NEXT 期限管理）。
- **dead-man/probe 監視**（業務通知 heartbeat・stale probe・kintone 滞留/失敗の provider 分離）。
- **durable 基盤**（inbound_event/ingestion_receipt・epoch fencing・**LINE durable〔Phase A〕のみ
  flag OFF 待機・Stripe journal は既存稼働**）。
- **redaction 台帳 61 件**（sink:print ゼロ・emit 契約 AST 強制）・**dependency lock+SBOM**（universal・CycloneDX 1.6）。

## 7. 残置・申し送り

- (i) `SORTATION_INGEST_TOKEN` env 削除＋GAS `SORTATION_TOKEN` 定数削除 —
  **完了（2026-07-27・[人]実見・時刻記録なし）**: env は Railway Variables 一覧に
  不存在・GAS は全文検索ヒット 0・削除後の sortation signed lane 正常継続を
  Railway HTTP Logs で観測（正本: S5 close work-log §4(i) の 2026-07-27 追補）。
  **これをもって Phase 1 完全クローズ**。
- (ii) 旧版 GAS プロジェクトの `RV04C_` プロパティ削除 — **完了（7/19・単独所有実見済み・
  #141 正本 work-log §4(ii) 参照）**。
- (iii) **RCF-M14**（Vision billing・別裁定）。
- (iv) **残 lane 切替（koseki/registry/bank/valuation）は Phase 2**（材料: S4-5-PREP-LANE23／本報告 §Phase2）。
- (v) **`INBOUND_EVENT_DURABLE_ENABLED` 点火は別裁定**（LINE redelivery=K4 切替と同時・RV-06）。
- (vi) RCF-M11/M13（司令塔台帳・DEFER）。

## 8. 費用トラッキング

- **Fable枠内消化・実費はConsole請求確定後に追記**。

## 9. モデル実測
- Phase 1 全タスク 開始/終了とも **Fable 5（claude-fable-5）**。

## 10. 最終事実（2026-07-19 追記）

- **#141 マージ済み**（merge commit `10f57c0`・main FF 済み）。
- **Phase 1 完了日 = 2026-07-19**。
- **最終 PR 番号 = #141**。
- **条件付き事項**: 削除追補（**§7 (i) のみ**: `SORTATION_INGEST_TOKEN` env 削除＋GAS
  `SORTATION_TOKEN` 定数削除）は **7/22 予定**（§7 (ii) は 7/19 完了済みのため対象外）。
