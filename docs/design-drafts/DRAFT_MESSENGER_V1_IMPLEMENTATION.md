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
- **Codex 独立性の機械強制（fix1・P2MSG-H01: 「禁止と記載」でなく迂回不能な構造へ）**:
  - **read-only sandbox**: レビュー用 worktree は書込不可の実行文脈で与える。
  - **network／credential 拒否**: レビュー実行プロセスへ push 資格情報・API token・
    credential helper を渡さない（拒否がデフォルトの実行環境）。
  - **固定 SHA 検証**: 実行直前に worktree HEAD ＝ 依頼記載 SHA を機械照合・不一致は不実行。
  - **実行前後の git status 機械比較**: `git status --porcelain`＋HEAD を前後比較し、
    差分があれば所見を無効として破棄（対象無改変の機械証明）。
  - 実現手段の候補（**選定は OPEN-B 起票・司令塔裁定**）: (i) Codex CLI の sandbox／
    approval オプション (ii) OS 権限（読取専用 ACL・別ユーザー実行） (iii) 使い捨て
    コンテナ。どれを採っても担保すべき不変条件は上記 4 点で共通。
- **依頼/所見の完全性保証（fix1・P2MSG-H02）**:
  - inbox 依頼様式は**「固定の全差分レビュー契約」を必須項目**とする（対象 SHA の全差分が
    常にレビュー範囲。**重点項目は追加のみ可・範囲を狭める記載欄を様式に置かない**＝
    PC-A が見せたくない箇所を外す経路を構造的に排除）。
  - outbox 所見は **Codex 出力の self-hash を所見末尾に付与**し、**司令塔が独立に照合**
    （PC-A 編集は hash 不一致で検出）。代替/将来案: **PC-A 非経由の配送路**
    （Codex→司令塔直接チャネル）を段C で設計。
  - **PC-A の編集検出方法**: (i) self-hash 照合 (ii) 依頼と所見の TASK_ID／SHA 突合
    (iii) 司令塔の抜き打ち再実行依頼（同一 SHA・同一契約で再現照合）。
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
- **credential の限定（fix1・P2MSG-H03）**: **fine-grained PAT** で
  `contents: read`＋`pull_requests: write` **のみ**を付与（本 repo 限定）。
  **classic の repo scope（全権）は使用禁止**。push は PC-A の gh credential では
  構造的に不可能（contents: write を持たない）＝「push しない」が権限で強制される。
  fine-grained 未対応の操作が必要になった場合は**自動化せず [人] 工程へ倒す**。
- **導入手順**（[人] 工程明示）:
  1. [人] `winget install GitHub.cli`
  2. [人] fine-grained PAT を発行（本 repo・contents:read＋pull_requests:write のみ）し
     `gh auth login --with-token` で投入（classic repo scope は発行しない）
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
- **credential 分離（fix1・P2MSG-H04）**: pull 用と push 用を**別 credential・
  別実行環境に分離**する。
  - **pull 用（read 系）**: PC-A 環境に置く。読取専用の認可範囲に限定
    （drift 検知・snapshot 取得のみ）。
  - **push 用**: **[人] 環境のみ**に置き、**PC-A には置かない**（PC-A からの live 反映が
    権限で構造的に不可能）。
  - **単一 credential で pull/push 両方が可能な構成は禁止**（第1〜2段の read-only 保証が
    credential レベルで崩れるため）。
- **前提（[人]確認）**: GAS プロジェクトへの clasp 認証（Google アカウント・
  Apps Script API 有効化）は [人] 工程。認可範囲の分離可否（Apps Script API の
  read 限定可否）は導入時に[人]実見で確認。

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

**fix1 再評価: 構造保証コストを織り込み「保証が軽い順」に並べ替え**（保証の実装が
軽いものから着手し、重い保証（sandbox・配送路）は裁定と環境整備を待つ）:

| 優先 | 票案 | スコープ | 構造保証コスト | 依存 |
|---|---|---|---|---|
| 1 | drift ツール流用（§3 第1〜2段） | P2-CHAIN-011 実装済みツールの運用組込み＋期待行列ファイル | **軽**（read-only・実装済み） | [人] clasp 認証（第2段のみ） |
| 2 | gh fine-grained 導入（§2） | [人] PAT 発行 2 工程＋PC-A 検証・規律追記 | **軽**（権限は PAT 設定のみで強制） | なし |
| 3 | codex_relay（§1） | tools/ 新設＋固定様式・self-hash・sandbox 選定 | **重**（H01 の 4 保証＋H02 の完全性設計） | OPEN-A／OPEN-B 裁定 |
| 4 | clasp 第3段〜RC 運用改訂（§3/§4） | push wrapper＋運用文書 | **重**（credential 分離環境の整備） | 1〜3 の実績・[人] 環境分離 |
- v1 に**含めない**: 段C の完全 bot 化（PR コメント/ラベル・トリガ駆動）・push/merge/env の
  いかなる自動化（FROZEN）。
