# DRAFT: RV-08 — 名寄せ soft merge / unmerge 移行設計票

- status: **DRAFT**（凍結は D 巡後・R-RV08-D1 から）
- TASK_ID: DOCS-BATCH-1 A（起草）／実装は別票
- 目的: person_merge 実行系の**敗者物理削除を恒久禁止**し、soft merge（無効化マーク
  残置・lineage 保持）／unmerge（復元）可能な構造へ移行する（v2.4 RV-08 の消込）。

## 1. 実装現実の実査（2026-08-11・rg＋実ファイル読解）

### 1.1 現行の物理削除（person_merge_exec.py・R4-2b T1）

- 統合実行 `execute_merge`（:219）の順序固定（docstring・部分成功設計）:
  「ガード再読 → 監査JSON生成・封筒添付 → 参照付け替え → 勝者更新 →
  **敗者削除** → 封筒クローズ。**監査添付が成功するまで App 34 に書かない**」
- 削除の実体（:290）: `await kintone.delete_record(APP_KOSEKI_PERSON, cand.loser_id)`
  ＝**物理削除**。
- 監査JSON（:249-260）は「削除前の敗者レコード全体を verbatim 保持」
  （`"敗者レコード": loser` ＝GET 形そのまま・参照付け替え計画・成立シグナル込み）。
- 復元部品は**既に存在**: `restore_payload_from_audit(audit)`（:185-197・
  「監査JSON → create_record 用 payload（**人手復元用**・08 手順書から使う）」）。
  ただし create_record による復元は**新しいレコード番号**になる＝旧 ID への参照
  （親エッジ）は自動では戻らない。
- 参照構造（docstring 実装調査 2026-07-07）: App34 を参照するのは **App34 自身の
  親エッジ4フィールド（父/母/養父/養母人物ID）のみ**。App33/36 に人物 ID 参照なし。
  削除前に親エッジを勝者へ付け替え（`_find_referrers` :200-216・`PARENT_EDGE_FIELDS`）。
- flag: `PERSON_MERGE_ENABLED`（R4-2a と共用・既定 OFF・無効時は実行も不発 :223）。
- 候補検出（R4-2a・person_merge.py）は**別モジュール分離**＝「確定への機械遷移が
  存在しない」を AST 検査で固定（docstring）。「別人」裁定は封筒クローズのみ
  （`reject_pair` :306・App34 不書込・再起票抑止は `_already_filed`）。

### 1.2 token 権限の現況（docs/evidence/KINTONE_TOKEN_MATRIX.md）

- `TOKEN_KOSEKI_PERSON`（App 34）: コード要求 R / W / **★D**
  （person_merge_exec.py の delete_record が唯一の D 要求箇所）。
- 同 §「★ RV-08 関連」: 実権限の削除チェック有無は **BLOCKED_NEEDS_HUMAN**
  （kintone 画面でしか見えない）。「RV-08 の soft merge 化までは削除権限を
  **外す**ことが封じ込め」と記録済み。
- 2026-07-11 work-log: 「person_merge物理削除（B05）| Phase 1で**soft merge化
  （RV-08）**」・soft merge 設計時の注意「人の操作で`名寄せ確定=確定`を統合なしに
  書けること」「**統合済み無効**状態を区別すること」。

## 2. 要求（v2.4 RV-08 → 要件写像）

| RV-08 要求 | 本票の要件 |
|---|---|
| soft merge 化 | R1: 敗者は削除せず**無効化マークで残置**（lineage=どの勝者へ統合されたかを敗者側に保持） |
| 過去削除レコードの復元 tool | R2: 監査JSON からの**復元手順の tool 化**（既存 `restore_payload_from_audit` を土台に、親エッジ再結線まで含める） |
| 旧削除 route の無効化 | R3: `delete_record` 呼出しの**コードからの除去**（AST 検査で App34 への delete 不在を固定） |
| 通常 token の App34 削除権限除去 | R4: `TOKEN_KOSEKI_PERSON` の削除権限を**実権限から外す**（[人]・kintone 画面） |

## 3. 設計骨子

### 3.1 soft merge（R1）

- App34 に**無効化系フィールドを追加**（[人] CU・§5 前提）:
  `統合状態`（DROP_DOWN: 有効 / **統合済み無効**・既定 有効）・
  `統合先人物ID`（SINGLE_LINE_TEXT・lineage）・`統合日時`（DATETIME）。
- `execute_merge` の順序固定は**維持**し、「敗者削除」段のみ置換:
  監査添付 → 参照付け替え → 勝者更新 → **敗者無効化 update**
  （統合状態=統合済み無効・統合先人物ID=勝者 ID・統合日時）→ 封筒クローズ。
  **監査成功が無効化の前提**（既存規律の維持・監査JSON の内容・添付先も不変）。
- 無効化行の下流除外: App34 を読む全 consumer（kinship_graph・heir_derivation・
  shokumu_plan・person_confirm 等）で「統合済み無効」行を**読み飛ばす**か、
  検索クエリで除外する（影響範囲は実装票で rg 全数調査・§6 裁定②）。
- 候補検出（R4-2a）は**不変**。ただし検出クエリが無効化行を候補に載せない除外を
  追加する（検出 logic 自体は不変・入力集合の絞りのみ）。

### 3.2 unmerge（R2・復元）

- soft merge 後の復元 = 敗者行の `統合状態=有効` へ戻す＋勝者から転記した
  フィールドの巻き戻し判断＋親エッジの再付け替え。**全て人の操作（関所型・
  封筒起票→確定）**。機械は復元を提案するだけで確定しない（既存原則と整合）。
- **過去に物理削除済みのレコード**（soft merge 移行前の削除分）: 監査JSON からの
  復元 tool（`restore_payload_from_audit` 拡張）。復元は新レコード番号になるため
  監査JSON の「参照付け替え」計画を逆適用して親エッジを再結線する。旧 ID の
  完全復元は kintone 仕様上不可能＝**新 ID での復元＋lineage 記録**を仕様とする。

### 3.3 旧 route 無効化（R3）と権限（R4）

- `kintone.delete_record` の App34 向け呼出しをコードから除去。
  **AST 検査**（test_p3_core_ast_policy / test_sink_ast_policy の型）で
  「person_merge_exec に delete_record 呼出しが存在しない」を恒久 pin。
- R4（token 削除権限除去）は**[人]・kintone 画面操作**。順序: **R3 merge →
  R4 権限除去**（逆順だと移行期間中の merge 実行が 403 で機能不全＝
  TOKEN_MATRIX の注記どおり）。

## 4. flag・移行・テスト骨子

- flag: 新設 `PERSON_MERGE_SOFT_ENABLED`（既定 OFF）**か** `PERSON_MERGE_ENABLED`
  の意味変更かは裁定①。既定 OFF の間は現行挙動不変（両時点残置）。
- テスト要件（骨子）: (i) soft merge 実行で delete が発行されない（mock で
  delete 呼出しゼロを assert） (ii) 無効化 update の field 集合固定
  (iii) 監査失敗時は無効化も行われない（順序固定の回帰） (iv) 無効化行が
  候補検出・グラフ・導出入力から除外される (v) unmerge の関所型遷移
  (vi) AST: App34 delete 不在 pin (vii) 既存テストの削除・緩和なし。

## 5. 前提（[人]ゲート・着手前に要）

1. **App34 フィールド追加（CU 作業）**: 統合状態・統合先人物ID・統合日時
   （名称・選択肢値は §6 裁定③）。
2. **token 権限変更**: `TOKEN_KOSEKI_PERSON` の削除権限除去（R3 merge 後）。
   現状の実権限確認（削除チェック有無）も未実施（TOKEN_MATRIX BLOCKED）。

## 6. 裁定欄（OPEN・司令塔）

| # | 論点 | 選択肢 | 状態 |
|---|---|---|---|
| ① | flag 方式 | (A) 新設 `PERSON_MERGE_SOFT_ENABLED`（旧 flag と併存・両 ON で soft が優先） (B) `PERSON_MERGE_ENABLED` の意味を soft へ切替（旧物理削除 route はコードごと消えるため flag 追加なし） | **OPEN** |
| ② | 無効化行の下流除外の実装単位 | (A) 各 consumer の検索クエリへ except 条件を横展開 (B) App34 読取の共通ヘルパを新設し一点除外（ヘルパ化は影響大＝実装票で rg 全数調査後に裁定） | **OPEN** |
| ③ | 無効化フィールドの名称・値 | 上記 3.1 案（統合状態/統合先人物ID/統合日時）の承認 or 修正（work-log 注記の「『統合済み無効』状態の区別」要求を満たす形） | **OPEN** |
| ④ | 過去削除分の復元 tool の置き場 | (A) 手動 CLI（08 手順書拡張・実行は[人]） (B) 関所語彙（封筒起票→確定で復元） | **OPEN** |
| ⑤ | 勝者転記の巻き戻し範囲（unmerge 時） | (A) 転記フィールドの機械巻き戻しはしない（監査JSON を見て人手） (B) 監査JSON の差分から機械提案（適用は人） | **OPEN** |

## 7. 両時点残置

- 本 DRAFT は初版。改定は fix 節を追記し、初版記述は撤回理由と併せて残す
  （遡及書き換えにしない・確立規律）。
