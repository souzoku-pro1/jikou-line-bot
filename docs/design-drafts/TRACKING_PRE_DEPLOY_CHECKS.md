# TRACKING: デプロイ前推奨回帰の追跡リスト（Codex 受容条件のフォロー）

- TASK_ID: P2-CHAIN-014（起票・PC-A）／記録日 2026-07-27
- 目的: Codex レビューで「**受容（マージ妨げにしない）が、実機での実測を推奨**」と
  裁定された検証項目を一覧化し、実施タイミングを固定して失念を防ぐ。
  本書は**追跡リスト**であり、各項目の実施自体は該当タイミングの票で行う（[人]ゲート）。
- 実施タイミングは**項目別**（MAIN-CONS-fix1・RMC-M02 で改定）:
  **#1/#2 = 各対象実装の migration が main へ merge された後・durable 点火とは独立に
  実施可**（対象テーブルが PG 実機に存在すれば足りる）。
  **#3 = durable 点火（`INBOUND_EVENT_DURABLE_ENABLED` ON・DRAFT_P2_DURABLE_IGNITION
  §6）後の観測ウィンドウ内**（監視項目G が動作する条件下での実測のため）。

## 追跡項目

| # | 検証項目 | 起票元 REVIEW_ID | 実施タイミング | 状態 |
|---|---|---|---|---|
| 1 | **PG 実機での並行 activate 実測**: TemplateVersion の単一 active 制約（部分 unique）＋activate() の競合安全化を Railway PostgreSQL 実機で実測。**合格条件は invariant 中心（RMC-M03）**: (a) 並行 activate 完了後も **active は常に最大1** (b) **敗者は拒否される**（DB 部分 unique 由来の IntegrityError／rowcount 検査由来の ActivationConflictError の**いずれの経路でも正当**・経路は問わない） (c) **敗者の transaction は全体 rollback**（旧 active の retire が巻き戻り「active 0 件」を残さない）。SQLite 検証済み・PG は方言差の確認 | **R-P3-002-3**（P3-002 受容条件） | **TemplateVersion migration の main merge 後**（durable 点火と独立・RMC-M02） | 未実施 |
| 2 | **PG 実機での並行 DerivationRun 作成実測**: derivation_run の single-root 部分 unique（`uq_derivation_run_single_root`）＋supersedes UNIQUE が並行初回作成の競合を PG 実機でも遮断することを実測（#1 と同型・P3-001） | R-P3-002-3 の P3-001 同型展開（P3-001 受容条件フォロー） | **DerivationRun migration の main merge 後**（durable 点火と独立・RMC-M02。#1 と同一セッションで実施可） | 未実施 |
| 3 | **PostgreSQL received/processing 境界値実測**: LINE durable 滞留監視（daily_healthcheck **監視項目G**＝`hub/durable_inbound.check_line_backlog`）の閾値まわりの境界値を PG 実機の時刻型・TZ 挙動で実測（SQLite とのタイムスタンプ比較差の確認・RMC-H01 で実装どおりに訂正）。閾値は **received/processing 各 env**（`INBOUND_LINE_STALE_RECEIVED_SECONDS`／`INBOUND_LINE_STALE_PROCESSING_SECONDS`・**既定 3600 秒=1h**）。境界値回帰は **G系の 1h 境界（既定値・閾値ちょうど・±1 秒・claimed_at NULL）＋設定別境界（env を変更した値での境界）**で実施 | **R-P2-LINE-BACKLOG-1**（提案・受容条件ではない） | durable 点火後の観測ウィンドウ内（監視項目G の動作条件下） | 未実施 |

## 運用メモ

- 実施時の接続は READ 中心だが並行 INSERT を伴うため、**本番 DB とは分離した検証用
  スキーマ/一時テーブル**（または点火検証用に司令塔が指定する環境）で行うこと。
  接続方式は既存規律どおり（PC-A からは `DATABASE_PUBLIC_URL` 経由・値は非表示）。
- 実測完了時は本表の「状態」を更新し、実測ログ（件数・所要・競合結果のみ・
  RV10 policy 準拠で PII/secret 非出力）を work-log に残す。
- 本リストへの追加は「Codex 受容条件・提案のうち実機依存で即時実施できないもの」に限る。
  コード修正を要する指摘は本リストに載せず通常の fix 票で処理する。
