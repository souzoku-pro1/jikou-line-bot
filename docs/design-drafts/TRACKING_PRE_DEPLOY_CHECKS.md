# TRACKING: デプロイ前推奨回帰の追跡リスト（Codex 受容条件のフォロー）

- TASK_ID: P2-CHAIN-014（起票・PC-A）／記録日 2026-07-27
- 目的: Codex レビューで「**受容（マージ妨げにしない）が、実機での実測を推奨**」と
  裁定された検証項目を一覧化し、実施タイミングを固定して失念を防ぐ。
  本書は**追跡リスト**であり、各項目の実施自体は該当タイミングの票で行う（[人]ゲート）。
- 実施の共通タイミング: **durable 点火（`INBOUND_EVENT_DURABLE_ENABLED` ON・
  DRAFT_P2_DURABLE_IGNITION §6）後の実機検証と同回**。点火後の観測ウィンドウで
  本番同型の Railway PostgreSQL に接続できるため、PG 実機系の実測をまとめて消化する。

## 追跡項目

| # | 検証項目 | 起票元 REVIEW_ID | 実施タイミング | 状態 |
|---|---|---|---|---|
| 1 | **PG 実機での並行 activate 実測**: TemplateVersion の単一 active 制約（部分 unique）が並行 activate 競合下で「勝者 1・敗者は IntegrityError」となることを Railway PostgreSQL 実機で実測（SQLite 検証済み・PG は方言差の確認） | **R-P3-002-3**（P3-002 受容条件） | durable 点火後の実機検証と同回 | 未実施 |
| 2 | **PG 実機での並行 DerivationRun 作成実測**: derivation_run の single-root 部分 unique（`uq_derivation_run_single_root`）＋supersedes UNIQUE が並行初回作成の競合を PG 実機でも遮断することを実測（#1 と同型・P3-001） | R-P3-002-3 の P3-001 同型展開（P3-001 受容条件フォロー） | durable 点火後の実機検証と同回（#1 と同一セッションで実施可） | 未実施 |
| 3 | **PostgreSQL received/processing 境界値実測**: journal 滞留監視（daily_healthcheck 監視項目E系）の 24h 閾値まわりの境界値（閾値ちょうど・±1 秒・claimed_at NULL）を PG 実機の時刻型・TZ 挙動で実測（SQLite とのタイムスタンプ比較差の確認） | **R-P2-LINE-BACKLOG-1**（提案・受容条件ではない） | durable 点火後の実機検証と同回（点火直後の観測強化ウィンドウ内） | 未実施 |

## 運用メモ

- 実施時の接続は READ 中心だが並行 INSERT を伴うため、**本番 DB とは分離した検証用
  スキーマ/一時テーブル**（または点火検証用に司令塔が指定する環境）で行うこと。
  接続方式は既存規律どおり（PC-A からは `DATABASE_PUBLIC_URL` 経由・値は非表示）。
- 実測完了時は本表の「状態」を更新し、実測ログ（件数・所要・競合結果のみ・
  RV10 policy 準拠で PII/secret 非出力）を work-log に残す。
- 本リストへの追加は「Codex 受容条件・提案のうち実機依存で即時実施できないもの」に限る。
  コード修正を要する指摘は本リストに載せず通常の fix 票で処理する。
