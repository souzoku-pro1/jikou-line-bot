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
  親エッジ4フィールド（父/母/養父/養母人物ID）のみ**。~~App33/36 に人物 ID 参照なし~~。
  削除前に親エッジを勝者へ付け替え（`_find_referrers` :200-216・`PARENT_EDGE_FIELDS`）。
  - **【fix1・RV08-01 訂正】「App36 に人物 ID 参照なし」は 2026-07-07 時点の調査で
    あり現行では誤り**——P3-003b（2026-07-30 以降）で App36 は `導出元人物ID`
    （＝App34 `$id`・保存層 person_id grammar `_SOURCE_PERSON_ID_RE = _PERSON_ID_RE`
    と逐語一致）を**冪等キーの片翼として保持・検索**する（hub/heir_projection.py 実査:
    `_project_row` の write `fields = {..., "導出元人物ID": pid, ...}`・検索
    `案件レコードID = "..." and 導出元人物ID = "{pid}"`〔:424-428/:606-610〕）。
    person_merge の勝者/敗者付け替え・無効化の設計は **App36 の当該参照を含む
    「App34 ID 保持箇所の全 consumer」を対象にしなければならない**（親エッジ4
    フィールドだけでは不足）。全数調査は §6 裁定⑥（**rg 全数調査の完了を凍結条件
    とする**）。
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

### 3.2a 部分失敗の回収（fix1・RV08-02 追加）

- soft merge は複数 update の逐次実行（参照付け替え→勝者更新→敗者無効化）であり、
  **途中失敗＝中間状態**が起き得る。回収設計:
  - **operation_id**: 1 回の統合実行に一意 ID を発番し、監査JSON・封筒 detail・
    各 update の記録に貫通させる（どの操作の中間状態かを機械判別可能にする）。
  - **preimage 保存**: 書き込み対象（勝者・敗者・付け替え対象行）の**書込み前の値**を
    監査JSONへ保存（既存の「敗者レコード verbatim」を勝者・付け替え行へ拡張）。
    無効化完了後の **postimage も併せて監査**（preimage/postimage の両監査）。
  - **再実行の照合**: 再実行は preimage/operation_id と現在値を照合し、
    **一致（未適用）→続行・適用済み→skip・不一致（第三者変更の疑い）→
    write 0 で要確認**（盲目再適用しない）。
  - **unmerge の上書き禁止**: unmerge は対象行の現在値が postimage と一致する
    場合のみ機械候補とし、不一致（統合後に人手編集あり）は**上書きせず要確認**。
  - **封筒 open 維持**: 部分失敗時は封筒をクローズしない（要確認のまま残し、
    detail へ operation_id・到達段を追記＝再指示で回収可能・heir_projection の
    resumed 型と同型）。

### 3.3 旧 route 無効化（R3）と権限（R4）

- `kintone.delete_record` の App34 向け呼出しをコードから除去。
  **AST 検査**（test_p3_core_ast_policy / test_sink_ast_policy の型）で
  「person_merge_exec に delete_record 呼出しが存在しない」を恒久 pin。
- R4（token 削除権限除去）は**[人]・kintone 画面操作**。順序: **R3 merge →
  R4 権限除去**（逆順だと移行期間中の merge 実行が 403 で機能不全＝
  TOKEN_MATRIX の注記どおり）。

## 4. flag・移行・テスト骨子

- ~~flag: 新設 `PERSON_MERGE_SOFT_ENABLED`（既定 OFF）**か** `PERSON_MERGE_ENABLED`
  の意味変更かは裁定①。既定 OFF の間は現行挙動不変（両時点残置）~~
  **【fix2・RV08-04 裁定確定＝(B) 採用】`PERSON_MERGE_ENABLED` の意味を soft merge
  へ置換する。新 flag は作らない。物理削除コードは完全除去。**
  - **「現行挙動不変」の撤回（理由付き残置）**: flag ON 時の挙動は
    **物理削除 → soft merge へ変わる**。これは**意図的な安全方向の変更**であり
    「flag ON の従来挙動維持」を設計目標にしない（物理削除を残す fallback は
    RV-08 の趣旨〔削除の恒久禁止〕に反するため）。
  - **flag × コード状態の全象限表**:

    | flag | 挙動 | 物理削除への経路 |
    |---|---|---|
    | `PERSON_MERGE_ENABLED` ON | **soft merge**（無効化 update・§3.1） | **なし**（delete 呼出しはコードに存在しない・AST pin） |
    | 同 OFF（未設定含む） | merge 実行不発（候補検出・関所とも既存どおり辞退） | なし（同上） |

  - fallback 経路の不存在は R3 の AST pin（App34 向け delete_record 不在）が
    構造で保証する（設定・env の組合せいかんで物理削除に戻る象限が存在しない）。
- テスト要件（骨子）: (i) soft merge 実行で delete が発行されない（mock で
  delete 呼出しゼロを assert） (ii) 無効化 update の field 集合固定
  (iii) 監査失敗時は無効化も行われない（順序固定の回帰） (iv) 無効化行が
  候補検出・グラフ・導出入力から除外される (v) unmerge の関所型遷移
  (vi) AST: App34 delete 不在 pin (vii) 既存テストの削除・緩和なし。
- **有効行定義の構造検査（fix1・RV08-03 追加）**:
  - 「有効行」の定義（統合状態 in 閉集合・除外条件）は**単一の正**（共通定数/
    ヘルパ）に置き、**閉集合を pin するテスト**で固定（値の増減は本 DRAFT 改定と
    同時のみ）。
  - **新 consumer 検査**: App34 を read する箇所の全数列挙（rg）に対し「有効行
    filter を通っているか」を機械検査（AST または import 検査）で強制——
    filter を通らない新規 read の追加を CI で検出する（無検査の read が
    無効化行を拾う回帰を構造で防ぐ）。
  - **直接 get の状態確認**: `get_record`（$id 直参照）経由は検索 filter が
    効かないため、取得後に統合状態を確認して無効化行なら要確認へ倒す規約を
    置く（negative テスト: 無効化行の $id を直接与えた場合に処理へ入らない）。
  - negative 骨子: 無効化行×（候補検出/グラフ/導出/職務上請求 target/直接 get）
    の各面で「拾わない・書かない・要確認」を実測。

## 5. 前提（[人]ゲート・着手前に要）

1. **App34 フィールド追加（CU 作業）**: 統合状態・統合先人物ID・統合日時
   （名称・選択肢値は §6 裁定③）。
2. **token 権限変更**: `TOKEN_KOSEKI_PERSON` の削除権限除去（R3 merge 後）。
   現状の実権限確認（削除チェック有無）も未実施（TOKEN_MATRIX BLOCKED）。

## 6. 裁定欄（OPEN・司令塔）

| # | 論点 | 選択肢 | 状態 |
|---|---|---|---|
| ① | flag 方式 | (A) 新設 `PERSON_MERGE_SOFT_ENABLED`（旧 flag と併存・両 ON で soft が優先） (B) `PERSON_MERGE_ENABLED` の意味を soft へ切替（旧物理削除 route はコードごと消えるため flag 追加なし） | **RESOLVED＝(B)**（fix2・RV08-04 司令塔裁定・§4 の全象限表参照） |
| ② | 無効化行の下流除外の実装単位 | (A) 各 consumer へ横展開 (B) 共通ヘルパ一点除外 | **RESOLVED＝(B) 既定**（fix5 裁定——単一の正の確立パターン。⑥の全数調査で影響過大と実装票が判断した場合は (A) を対案提示可） |
| ③ | 無効化フィールドの名称・値 | §3.1 案（統合状態/統合先人物ID/統合日時） | **RESOLVED＝§3.1 案承認**（fix5 裁定——「統合済み無効」の区別要求を満たす） |
| ④ | 過去削除分の復元 tool の置き場 | (A) 手動 CLI (B) 関所語彙 | **RESOLVED＝(A) 手動 CLI**（fix5 裁定——過去削除分は低頻度・08 手順書拡張で足り、関所語彙の新設コストを避ける） |
| ⑤ | 勝者転記の巻き戻し範囲（unmerge 時） | (A) 人手のみ (B) 機械提案 | **RESOLVED＝postimage 完全一致のみ自動巻き戻し候補・変更検出は要確認**（fix5 裁定——§3.2a と同一の照合規律へ統一・盲目巻き戻しをしない） |
| ⑥ | **App34 ID 保持箇所の全数と保存先の意味論**（fix1・RV08-01） | rg 全数調査＋履歴/current/種類別の分類・**調査完了が凍結条件** | **RESOLVED＝§10 に全数調査を収載（fix5・本票内で実施）**。App36 `導出元人物ID` は**「履歴として敗者 ID を残す＝付け替えない」を既定方針**。個別論点は §10 の選択肢欄（凍結条件は**充足**） |
| ⑦ | 部分失敗回収の器（fix1・RV08-02） | (A) 監査JSON＋封筒 detail (B) DB 台帳併用 | **RESOLVED＝(B) DB 台帳**（fix5 裁定——immutable 追記・P3-001 流儀。kintone 添付は検索・機械照合に不向き） |

## 7. 両時点残置

- 本 DRAFT は初版。改定は fix 節を追記し、初版記述は撤回理由と併せて残す
  （遡及書き換えにしない・確立規律）。

## 8. fix1 改定記録（R-DOCS-BATCH-1-D1・2026-08-11・全所見 ACCEPT）

- **RV08-01**: §1.1 の「App33/36 に人物 ID 参照なし」（2026-07-07 調査の引用）を
  **撤回・訂正**——P3-003b 以降 App36 が `導出元人物ID` で App34 ID を保持する
  実装現実を実査引用で追記。裁定⑥（全 consumer/保存先意味論の rg 全数調査＝
  凍結条件）を新設。
- **RV08-02**: §3.2a（部分失敗の回収——operation_id・preimage/postimage 両監査・
  再実行照合・不一致 write 0・unmerge 上書き禁止・封筒 open 維持）を新設。
  裁定⑦（回収の器）を新設。
- **RV08-03**: §4 テスト骨子へ有効行定義の構造検査（閉集合 pin・新 consumer 機械
  検査・直接 get の状態確認・negative 骨子）を追加。

## 9. fix2 改定記録（R-DOCS-BATCH-1-D2・2026-08-11・前巡全所見 RESOLVED）

- **RV08-04（裁定＝推奨形採用）**: 裁定①を **(B)＝`PERSON_MERGE_ENABLED` の意味
  置換**で確定。新 flag 不使用・物理削除コード完全除去。§4 の「既定 OFF の間は
  現行挙動不変」を**撤回**（flag ON の挙動変更〔物理削除→soft〕は意図的な安全
  方向の変更と明記）し、flag×コード状態の全象限表（fallback 経路の不存在込み）を
  追加。

## 10. App34 ID 保持箇所の全数調査（fix5・裁定⑥の収載・2026-08-11 rg 実測）

対象: 非テストコード全体（`人物ID`／`person_id`／`decedent_person_id`／
`input_person_ids` の rg 全数）。ヒットファイル: config.py・
dispatch_bot/heir_derive_task.py・heir_derivation.py・hub/derivation_models.py・
hub/heir_envelope.py・hub/heir_projection.py・hub/shokumu_plan.py・
hub/webapp_kinship_view.py・kinship_graph.py・koseki_person_sync.py・
person_merge_exec.py（＋alembic migration・tracking_pg_harness＝schema/合成で対象外）。

### 10.1 保存先（App34 ID が永続化される場所）と意味論分類

| 保存先 | 保持形 | 分類 | 統合時の扱い |
|---|---|---|---|
| App34 親エッジ4（父/母/養父/養母人物ID） | kintone 恒常 | **current** | 勝者へ付け替え（既存 `_find_referrers`/`PARENT_EDGE_FIELDS` を維持） |
| App36 `導出元人物ID`（heir_projection :615 write・:427/:609 検索＝冪等キー片翼） | kintone 恒常 | **履歴（既定方針・fix5 裁定）** | **付け替えない**＝敗者 ID を残す。正しい反映は再導出→新 run→confirmed の既存原理（下 10.2-i） |
| DerivationRun（DB immutable）: `decedent_person_id`・`input_person_ids`・`input_person_revisions` キー・`result_payload.heirs[].person_id` | DB 恒常 | **履歴** | 付け替え禁止は immutable trigger が構造保証。再導出が正 |
| App30 封筒 detail: person_merge `勝者候補/敗者候補/ペアキー` | kintone 恒常 | **履歴** | 統合裁定の記録・不変（`_already_filed` の再起票抑止にも使用） |
| App30 封筒 detail: heir_derivation `保留人物ID`（DETAIL_HELD_PERSONS_KEY） | kintone 恒常 | **種類別**（10.2-ii） | 記録としては履歴・未クローズ封筒の再確定時は再検証で遮断 |
| App30 封筒 detail: shokumu_plan candidates `person_id`・`plan_idem` キー・M1 target | kintone 恒常 | **履歴＋実行時再計算** | 確定時 stale 再計算が App34 を読み直すため、無効化は有効行 filter（RV08-03）で自然反映 |
| 監査JSON（App30 成果物添付・敗者レコード verbatim/参照付け替え計画） | 添付 | **履歴** | verbatim 保持・付け替え禁止（復元の原資） |

### 10.2 個別論点（停止せず選択肢提示・実装票で確定）

- **(i) App36 導出元人物ID が敗者 ID のまま残る帰結**: shokumu_plan §2B-5
  （App36 の person 集合×run payload の一致検証）は敗者 ID 残置で**不一致＝条件
  未充足へ安全側に落ちる**（自然遮断・実測済みの 6 条件設計）。選択肢:
  (a) この自然遮断のみで足りるとする（既定案・追加実装なし）
  (b) 無効化行を指す App36 行の可視化検査（daily_healthcheck 監査 or 関所警告）を
  追加する——採否は実装票。
- **(ii) 未クローズ封筒の保留人物ID**: 再確定（resumed 経路）の phase 1 再検証で
  当該 person が無効化行なら**要確認へ倒す**（RV08-03 の「直接 get の状態確認」
  規約に含める・既定案）。対案=封筒 detail の書き換え（不採用推奨・履歴改変になる）。
- **(iii) koseki_person_sync の冪等キー（戸籍レコードID＋氏名）**: 無効化行が
  冪等ヒットした場合に**再生成を抑止する現行挙動を維持**（重複人物の再出現を
  防ぐ・既定案）か、有効行のみ照合（無効化後に再生成を許す）か——実装票で
  kinship 側の要請と併せ確定。

### 10.3 実行時 read のみの consumer（保存しない・RV08-03 有効行 filter の適用対象）

kinship_graph／webapp_kinship_view／heir_derivation（HeirPerson.record_id 読取）／
heir_derive_task（App34 全件読取→run 起票）／shokumu_plan `_load_persons`／
person_confirm 系。いずれも保存先ではなく、裁定②(B) の共通ヘルパ一点除外の
適用面（§4 の新 consumer 機械検査の対象リスト初期値）。

## 11. fix5 改定記録（司令塔裁定の一括反映・2026-08-11・D5=4票 DESIGN_OK 後）

- 裁定②〜⑦を RESOLVED 化（§6 の各行に裁定内容と1行理由を記載）。
- **裁定⑥は本 fix 内で rg 全数調査を実施し §10 へ収載**——保存先7類型の
  履歴/current/種類別分類・App36 `導出元人物ID`=「履歴として敗者 ID を残す＝
  付け替えない」の既定方針・個別論点3件（§10.2・停止せず選択肢提示）。
- **凍結条件の充足状態**: 裁定⑥の凍結条件（全数調査の収載）は**充足**。
  未充足の残条件なし（§5 の[人]ゲート2件は実装着手前提であり凍結条件ではない）。
