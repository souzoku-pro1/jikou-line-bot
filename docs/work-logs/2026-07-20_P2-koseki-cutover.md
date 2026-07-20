# 作業記録 2026-07-20: P2 koseki（lane2）cutover — 署名経路化

- TASK_ID: P2-CHAIN-001（＋fix1r）／P2-CHAIN-002（本記録）／実施: PC-A＋[人]／記録日 2026-07-20
- 手順書: `docs/runbooks/2026-07_S4-S5_cutover-checklist.md`（D-2〜D-3 相当を lane2 に適用）
- 対象 lane: **koseki（lane2・`/koseki/ingest`）**。sortation（lane1）は S5 で完了済み。
  registry/bank/valuation（lane3 以降）は未着手・SIGNED_LANES false のまま。
- ステータス: **D-3〜D-5 相当まで完了（7/20・§6.1）**。残置は koseki 能動 404（D-7 相当）と
  sortation 残置(i)のみ・7/22 実施（§6.1）。
  **INC-0720（SEV-3・lane1 一時 404・7/20 中に復旧済み）あり**（§INC-0720）。

## 1. 概要（Railway 側）

- lane2（`/koseki/ingest`）の署名経路化。**PR #143**（`5422dea` feat ＋ fix1 `1ebf1d5`）。
- レビュー経緯: **R-P2-KOSEKI-1 = CR（P2K-H01）→ fix1r 適用 → R-P2-KOSEKI-2 = PASS**。
- merge 済み（main 反映・merge commit `82ac42a`）。Railway は main merge で自動デプロイ。
- 署名検証はサーバ側既存 `hub.service_auth`（ingest_guard・dual-accept 結線済み）を再利用・
  新規実装なし。Railway 側コード変更は gas 完成形＋テストのみ。

## 2. 2キー契約（P2K-H01）

- `LANE_FIELDS['/koseki/ingest']` を `['file', 'drive_file_id']` の **2 キーへ縮小**（他 4 lane 不変）。
  fail-fast は**既存の `rv04cBuildSignedBody_` allowlist enforce（明示 throw）が実効化**され、
  case_hint 系 part は黙って落ちず即時失敗（P1-114 思想・重複実装なし）。
- gas_builder 不変条件の**契約改定（司令塔裁定）**: 旧「GAS allowlist＝サーバ Form 1:1
  （dual-accept 期の characterization）」→ 新「**GAS allowlist ⊆ サーバ Form**。koseki は
  2 キー送信契約・サーバ側 `Form(default=None)` は受入互換のため無変更」。
- テスト: **`python -m pytest -q` 全実行で 1 failed（`test_triage_classification::
  test_classification_accuracy`＝既知）＋ 1474 passed・skip 0**。負系（署名欠落/期限外/
  nonce 再使用/body 改変/unknown key ID）・legacy 併存維持・2 キー byte パリティ（#140 整合版）・
  2 キー契約強制（throw×2・順序含め完全一致・sortation no-op 回帰）を固定。

## 3. live GAS 反映（7/20・[人]）

- `rv04c_signing` を merged main 版（`82ac42a` の `gas/rv04c_signing.js`・
  `SIGNED_LANES['/koseki/ingest']: true`）へ**全置換した。この全置換により repo 値
  `sortation=false` が live の `true` を上書きし、lane1 が legacy 経路へ回帰
  （サーバは legacy 停止済み＝`SERVICE_AUTH_LEGACY_DISABLED_PATHS=/sortation/ingest`）
  → 404**（→ §INC-0720。7/20 中に復旧済み）。
- 戸籍読解ブロック（ブロック②）を **`rv04cIngestFetch_` 置換**: parts は 2 キー
  （`file`=bytes＋filename＋contentType／`drive_file_id`=utf8Bytes_）・`legacyToken` は
  既存定数 `KOSEKI_TOKEN`・`legacyPayload` は現行送信と同一（dispatcher 契約
  `{parts, legacyPayload, legacyToken}` 準拠・README 手順 5）。
- **事前 drift 確認**: 置換前の live コードが repo 写し（`legacy/gas/コード.js` ブロック②）と
  一致することを実見してから置換（写しベース・照合ファースト）。
- rollback は `SIGNED_LANES['/koseki/ingest']=false` の定数 1 箇所（legacy 送信は byte 同一）。

## 4. D-3 相当の成立（7/20・当日採取）

- 実機: テスト戸籍 PDF 1 件投入 → トリガー跨ぎ → **`[済]` リネーム実見**。
- GAS 実行ログ: 全実行「完了」・エラーなし（[人]実見）。
- Railway HTTP Logs: **POST `/koseki/ingest` 200・`?token=` なし**（[人]実見・署名経路での受理）。
- **観測窓の限界（S5C-M01 と同様の注記）**: 上記は [人] の画面実見による**一次記録**であり、
  PC-A がライブログから独立再計数したものではない。

## 5. R1 DEFER 項目の消化

- 実機 `opts.parts` 2 キーは、**live コード実見（貼付内容＝parts 2 要素）＋ 200 受理＋
  サーバ Form 互換**で担保。
- GAS 実機 throw テスト（allowlist 外 part の実機拒否）は**未実施**（S4 実機ゲート扱い・
  Codex R2 裁定「デプロイ前必須の不足なし」）。

## 6. 残置（7/22 の回で判断）

- (i) **D-4 相当**: 署名成功 2〜3 件・legacy 0 の並行観測。
  → **§6.1 で 7/20 前倒し充足（当初計画 7/22）**。
- (ii) **D-5 相当**: `SERVICE_AUTH_LEGACY_DISABLED_PATHS` へ `/koseki/ingest` 追記
  （順序は checklist §5.1 準拠・server→credential→GAS の逆順禁止）。
  → **§6.1 で 7/20 前倒し完了（同上）**。
- (iii) sortation 残置(i)（`SORTATION_INGEST_TOKEN` 削除・7/22 予定）と**同回に合流**。

## 6.1 D-4/D-5 相当の完了（7/20 追記）

- **D-4 相当充足（7/20・[人]実見の一次記録）**:
  - 署名経路 POST `/koseki/ingest` **200 = 計 2 件**（初回 D-3 の 1 件＋追加 1 件）・
    `[済]` リネーム実見・Railway HTTP Logs で **`?token=` 付き 0 件**。
  - **充足基準の裁定**: sortation は初 lane のため 3 件としたが、koseki は**同一署名基盤
    （本番実績あり）＋行列 pin 済み**（PR #145）のため **2 件で充足**（司令塔裁定）。
- **D-5 相当完了（7/20・[人]）**:
  - `SERVICE_AUTH_LEGACY_DISABLED_PATHS` を **`/sortation/ingest,/koseki/ingest`** へ更新・
    デプロイ緑。
  - **停止後健全性**: `/health` ok ＋ 実機 1 件追加投入 → `[済]` リネーム
    （＝legacy 停止後も署名経路無傷・**計 3 件目の 200**）。
- **残置の更新**（7/22 の回で実施）:
  - (a) koseki **能動 404（D-7 相当）**: PowerShell 実測＋`legacy_blocked` 当日採取。
  - (b) **sortation 残置(i)**（token/env・GAS 定数削除）: INC-0720 復旧から **2 日の安定観測を
    経る司令塔裁定により 7/22 維持**。
- **観測窓の限界（S5C-M01 型・明記）**: 本節の 200 件数・`?token=` 0 件・デプロイ緑・`/health` は
  いずれも **[人] の画面実見による一次記録**であり、PC-A がライブログから独立再計数したもの
  ではない。

## INC-0720（SEV-3）: 全置換による lane1 SIGNED_LANES 巻き戻し → 一時 404

- **経緯**: 7/20 `rv04c_signing` 全置換（17 時台）→ Codex **R-P2-KOSEKI-LOG-1 が
  P2KL-H01 として巻き戻し経路を机上検出**（repo/live 差分の推論）→ [人] が Railway 実見で
  **POST `/sortation/ingest` 404 連続を確認**（7/20）→ live の
  `SIGNED_LANES['/sortation/ingest']` を `true` へ **1 行修正** → トリガー跨ぎ後
  **200 復帰を [人] 実見**。
- **実害**: 仕分け遅延のみ。「200 以外リネームせず自然リトライ」設計により、対象ファイルは
  未整理フォルダ残留 → 復旧後自動再送（**404 件数 ≠ ファイル件数**）。
  顧客 LINE 経路・データへの影響なし。
- **検出者**: Codex（机上・repo/live 差分の推論）。**司令塔検収の見落とし**（S4-5 の lane1
  切替が live 直編集だった事実と「全置換」指示の矛盾）が原因。
- **live 最終行列（7/20 復旧後・[人]実見の一次記録・P2KL-H01 の要求）**:

  | lane | SIGNED_LANES |
  |---|---|
  | `/sortation/ingest` | **true** |
  | `/koseki/ingest` | **true** |
  | `/registry/ingest` | false |
  | `/bank/ingest` | false |
  | `/valuation/ingest` | false |

## 7. 再発防止規律（INC-0720 起点）

- (i) GAS への repo 写し反映は**「全置換」を禁止**する。反映指示には必ず **SIGNED_LANES
  全 5 lane の期待行列を明記**し、反映後に [人] が live の行列を**読み合わせる**。
- (ii) live 直編集で切替済みの lane がある間は、**repo 側 SIGNED_LANES を live 実態に
  同期させる PR を cutover work-log と同時に出す**（repo/live drift の恒久解消）。

