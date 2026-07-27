# 作業記録 2026-07-21: P2 バッチ実行（BATCH-01〜03-fix）クローズ

- TASK_ID: P2-BATCH-01／01-fix／02／02-fix／03-fix・P2-CHAIN-007-fix2〜fix4（本記録=P2-MEGA-01 TASK 9）
- 実施: PC-A（実装・DRAFT）＋Codex（レビュー）＋司令塔（裁定）＋[人]（push・merge）／記録日 2026-07-21
- ステータス: **本記録時点で 6 branch 全て main 反映済み**（PR #149〜#154）。
  全 suite 基準 **1 failed（triage 既知）＋1514 passed・skip 0**。

## 1. 成果一覧（PR・レビュー経緯）

| PR | branch | 内容 | レビュー経緯 |
|---|---|---|---|
| #149 | p2-lane3-tests | lane3（bank/valuation）受入テスト先行実装（+11・legacy 受理 pin・4キー parity） | R-P2-LANE3-TESTS-1 PASS |
| #150 | p2-healthcheck-deps | `/health/deps`（RCF-M14 監視拡張）→ fix1 で **probe/参照分離**（denial-of-wallet 遮断） | R1=CR(P2HC-H01)→fix1→R2 PASS |
| #151 | p3-prep-inventory | Phase 3 control plane 棚卸し DRAFT → fix1 で TemplateVersion 保存先・封筒フロー向き是正 | R1=CR(P3PREP-H01/H02)→fix1→R2 PASS |
| #152 | p2-messenger-v1-design | 伝書鳩 v1 設計 DRAFT → fix1 構造保証化・fix2 self-hash 格下げ＋書込不可 outbox 必須化 | R1/R2=CR→fix1/fix2→R3 PASS |
| #153 | p2-gas-drift-tool | GAS drift 検知ツール → fix1 secret 拡張/必須ゲート/exit 契約 → fix2 構造化 writer/両側判定/manifest 正本 | R1/R2=CR→fix1/fix2→R3 PASS |
| #154 | p2-durable-prep | durable 点火裁定材料 DRAFT（fix1〜fix4: K4 格下げ・滞留監視単独ゲート・照合源実在化・failed_exhausted 定義統一） | R1〜R4=CR→fix1〜fix4→R5 PASS |

## 2. 主要裁定の確定（DRAFT へ収載済み・詳細は各正本）

- **durable 点火**: 唯一の前提＝received/processing 滞留監視（durable flag 配下・Stripe flag
  非依存・LINE 警報）。**点火ゲート＝検査関数＋daily_healthcheck 結線の両方 merge**。
  K4 は補助へ格下げ（200 ACK 後 crash を回収できないため）。
- **状態語彙**: `done`=照合源による根拠がある場合のみ／`failed_exhausted`=再試行を行わない
  ことが確定した打切り（自動=attempts 上限・手動=再配送終了済みの[人]判断・manual_closed
  分類で識別）。received の閉鎖は照合源（external_event_id 等・実在するもののみ）が無ければ
  **残置が唯一の扱い**（Phase A は raw payload 非保存）。
- **伝書鳩 v1**: 禁止記載でなく構造保証（read-only sandbox・書込不可 outbox・fine-grained
  PAT・clasp credential 分離）。OPEN-A/B/C は裁定待ち。
- **lane3**: OPEN-2 暫定=4キー維持・SIGNED_LANES 先行 true 化は逆 drift のため点火票と同時。

## 3. インシデント・学び（コード規律）

- **sink AST 方針の行番号 pin**: allowlist が `main.py`／`daily_healthcheck.py` の行番号を
  固定しているため、途中への行挿入が既存 pin を壊す。回避は (i) 末尾結線（/health/deps）
  (ii) 台帳の行番号 refresh（entry 数・manifest 不変の機械更新）。
- **untracked scan 間隙**: `scan_repo` は git 追跡ファイルのみ走査するため、**pytest→git add
  の順だと新規ファイルの sink 違反が commit 後に顕在化**する（P2-CHAIN-011 で実発生・
  fix1 追記で解消）。以後は **add 後に全 suite** を通す。
- **emit_shadow 保護名**: `emit` という関数名は hub.redact.emit の shadow 検査に掛かる。
  drift ツールの構造化 writer は `report` 名を採用。
- **GAS 全置換禁止（INC-0720 §7）** の機械化が `tools/gas_drift_check.py` として第1段稼働
  （snapshot 手貼り運用・SIGNED_LANES 期待行列対比・secret 6 パターン防御・exit 契約 0/1/2）。

## 4. 次工程（P2-MEGA-01 後続で着手済み/予定）

- TASK 7: P2-CHAIN-012 滞留監視実装（点火ゲート充足）→ TASK 8: P3-001 DerivationRun+HCD →
  TASK 10: P3-002 TemplateVersion → TASK 11: docs/architecture への確定仕様昇格。
- durable 点火票は TASK 7 merge 後に司令塔裁定（§6 前提条件 (a)(b) の[人]確認込み）。
