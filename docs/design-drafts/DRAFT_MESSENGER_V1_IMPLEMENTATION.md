# DRAFT: 伝書鳩 v1 実装設計（搬送自動化の最小実装計画）

- TASK_ID: P2-CHAIN-009（設計 DRAFT・実装しない）／記録日 2026-07-21／起草 PC-A
- **上位規範（FROZEN）**: `DRAFT_MESSENGER_AUTOMATION.md`（#142）§2 不変条件。
  権限境界（main・既存 ref の更新/上書き push（force-with-lease 等）・merge・env・
  migration・本番デプロイは [人] 専権／自動化は**搬送のみ**）・宛先明記・redaction/secret
  規律・tree 一致機械確認・モデル実測記録を**一切変更しない**。
- 位置づけ: 同 DRAFT の段B（半自動搬送）〜段C最小（搬送 bot 化）の v1 具体化。裁定は司令塔。

## 1. Codex 搬送の自動化（レビュー往復のファイル投函/回収）

- **構成案**: `tools/codex_relay/`（実装票で新設）
  - `inbox/`: PC-A がレビュー依頼を**固定様式ファイル**で投函
    （`R-<対象>-<巡>.request.md`: TASK_ID／対象 SHA／重点項目のみ。**PC-A の自己評価・
    修正意図の説明は書かない**＝レビューへの誘導を構造的に排除）。
  - `run_codex_review.(ps1|py)`: 依頼ファイルを読み、**対象 SHA を detached worktree に
    checkout**（`git worktree add --detach`）して Codex CLI を headless 実行。
    working copy の未 commit 変更を混ぜない＝**レビュー対象の同一性を機械保証**。
  - `outbox/`: 所見を `R-<対象>-<巡>.findings.md` で回収（宛先・TASK_ID ヘッダ付き）。
- **Codex 独立性の担保**: (i) 入力は SHA＋固定様式のみ (ii) worktree は read-only 扱いで
  レビュー後に破棄 (iii) 依頼文テンプレートは repo に固定し PC-A が都度文面を作らない
  (iv) 所見ファイルは PC-A が**編集せず**そのまま司令塔へ搬送（搬送規律）。
- **失敗時 fallback**: CLI 失敗・認証切れ・タイムアウト時は**現行の [人] 貼付運用へ即戻す**
  （スクリプトは搬送のみで、失敗時に代替実行しない＝`!` 規律と同型）。
- **裁定事項（OPEN-A）**: Codex CLI の実行主体を PC-A 起動にするか [人] 起動にするか
  （FROZEN は「搬送のみ機械化」— レビュー実行は merge/env/push のいずれでもないが、
  実行主体の機械化に当たるかは司令塔解釈の裁定を仰ぐ）。

## 2. gh CLI 導入計画

- **自動化範囲**: PR 作成（`gh pr create` 本文付与・base/head 指定）・PR 番号/URL の取得・
  PR コメント投稿（レビュー往復の搬送先）。**push・merge・approve は対象外のまま**
  （[人] 専権・現行案1規律不変。PR 作成は案1で PC-A 可の既存範囲）。
- **7月教訓（auth コンテキスト喪失）への対策**: gh 不在時に credential manager から
  token を取り出す代替は**恒久禁止**（7/19 の権限拒否の経緯を規律化）。
  票の冒頭で `gh auth status` を機械確認し、失効時は**提案止まりで [人] の再 login 待ち**。
- **導入手順**（[人] 工程明示）:
  1. [人] `winget install GitHub.cli`
  2. [人] `gh auth login`（ブラウザ対話・scope は repo 最小）
  3. PC-A `gh auth status` 確認 → 以後の票で PR 作成を自動化
- **効果**: 現行「push 後に [人] が compare URL を 1 クリック」の接点を除去。

## 3. clasp 化計画（GAS repo⇔live 同期の段階導入）

| 段 | 内容 | 実行主体 | INC-0720 §7 との整合 |
|---|---|---|---|
| 第1段 | **drift 検知のみ**（read-only）。当面は手動 snapshot＋`tools/gas_drift_check.py`（P2-CHAIN-011）で運用し、clasp 導入後は `clasp pull` を一時ディレクトリへ行い同ツールで照合 | PC-A 可（読取のみ） | §7(ii) の drift 検知を機械化 |
| 第2段 | **pull 回収の定型化**: live 全文の取得を機械化し、§7(i) の「期待行列の読み合わせ」を SIGNED_LANES 抽出・対比で自動化 | PC-A 可（読取のみ） | 読み合わせの人的ミスを排除 |
| 第3段 | **push 反映**: `clasp push` による live 更新 | **[人] 専権**（本番反映に該当・FROZEN） | **全置換禁止のツール強制**（下記） |
- **全置換禁止の強制方法（第3段の前提）**: push 用 wrapper は (i) repo 側 SIGNED_LANES と
  **期待行列ファイル**（repo 内・司令塔裁定値）の一致 (ii) 直前の drift 検査（第1段ツール
  exit 0）—— の両方が通らない限り `clasp push` コマンド自体を出力しない。
  ＝「repo=live 同期が確認できた状態からの反映」だけを許す（INC-0720 の再発を構造遮断）。
- **前提（[人]確認）**: GAS プロジェクトへの clasp 認証（Google アカウント・
  Apps Script API 有効化）は [人] 工程。認証情報は PC-A 側に置かない。

## 4. Remote Control 前提の運用フロー改訂案

| 接点（[人] の手作業） | 現状 | v1 導入後 |
|---|---|---|
| 票の PC-A への貼付 | 手動 | 手動のまま（RC で場所を選ばず投入可） |
| PC-A 報告の司令塔/Codex への貼付 | 手動 | **codex_relay 投函/回収で自動**（§1） |
| Codex 所見の PC-A への貼付 | 手動 | 同上 |
| PR 作成（compare URL クリック） | 手動 | **gh CLI で自動**（§2） |
| push（新規/FF） | `!` 1 行 | `!` 1 行のまま（**専権不変**） |
| merge・env・点火 | 手動 | 手動のまま（**専権不変**） |
| GAS drift 確認 | 目視読み合わせ | **ツール照合**（§3 第1〜2段） |
- 接点数: 現状 7 → v1 後 4（うち 3 は専権として意図的に残す）。**削るのは搬送のみ**。

## 5. 実装票スコープ案と優先順位（司令塔裁定用）

| 優先 | 票案 | スコープ | 依存 |
|---|---|---|---|
| 1 | gh CLI 導入（§2） | [人] 2 工程＋PC-A 検証・規律追記 | なし（即効・最小） |
| 2 | drift 検知第2段（§3） | clasp pull 定型化＋期待行列ファイル | P2-CHAIN-011（第1段）・[人] clasp 認証 |
| 3 | codex_relay（§1） | tools/ 新設＋固定様式・fallback 手順 | OPEN-A 裁定 |
| 4 | RC 運用改訂（§4） | 運用文書のみ | 1〜3 の実績 |
- v1 に**含めない**: 段C の完全 bot 化（PR コメント/ラベル・トリガ駆動）・push/merge/env の
  いかなる自動化（FROZEN）。
