# DRAFT: P3-003C-CANCEL — confirmed 済み projection の取消（§8-4 別票）

- status: **FROZEN**（凍結・2026-08-11・R-DOCS-BATCH-1-D8 PASS）
- TASK_ID: DOCS-BATCH-1 B（起草）／実装は別票
- 位置づけ: DRAFT_P3_003C_HELD_REJECTED（FROZEN）の**裁定④で別票化された残置**の消込。
  誤確定（confirmed の判断ミス）の救済経路を設計する。

## 1. 残置記載の全量引用（DRAFT_P3_003C_HELD_REJECTED.md）

### §8 裁定表・行4（§8-4・408行の表ヘッダと413行）

> | # | 論点 | 選択肢 | 推奨 | 影響 |
> |---|---|---|---|---|
> | 4 | **confirmed 済み run への held/rejected**（projection 取消） | (A) 中止・別票（§3.2 案） (B) 本票で取消経路まで設計 | **(A)**——App36 巻き戻し（yes→no 逆遷移禁止 §3.4 と衝突）を伴い、本票のスコープ（関所前の判断語彙）を超える | (A) 確定後の誤りは再導出→新 run→confirmed で前へ回す（既存原理） (B) スコープ膨張・§3.4 との整合再設計が必要 |

### §9 スコープ外・419行

> - confirmed 済み projection の取消・App36 巻き戻し（§8-4・別票）。

## 2. 実装現実の実査（2026-08-11）

- **中止セルの現実装**（hub/heir_projection.py `_BLOCKED_REASONS`）:
  `"already_confirmed": "確定済みです（取消は別途・書き込みなし）"` ——
  confirmed 済み run への held/rejected は§3.2-v2 遷移表で**中止**（本票はこの
  「取消は別途」の別途を定義する）。
- **decision 鎖の構造制約**（hub/derivation_models.py）:
  - `ck_heir_decision_decision`: decision は **CHECK 制約で 3 値閉集合**
    （'confirmed','held','rejected'）＝第4値の追加は **migration を伴う**。
  - `uq_heir_decision_supersedes`（UNIQUE(supersedes_decision_id)）＋
    `uq_heir_decision_single_root`＝一本鎖。leaf 判定は `get_leaf_decision`（単一の正）。
  - 台帳は **immutable**（UPDATE/DELETE は ImmutableRecordError・追記のみ）。
- **App36 側の制約**（正本 DRAFT_APP36 §3.4 遷移表・121-126行）:
  「yes → no は**禁止**（逆遷移不可）」「機械は yes を no に落とさない」。
  取消はこの禁止に対する**明示の人手例外**を必要とする。
- **監査側**（P3-003C-H11a・daily_healthcheck 監視項目I・merge 済み #199）:
  「confirmed の有効 leaf が無いのに 戸籍確認済=yes」を検知する。**取消の設計は
  この監査と整合させる必要がある**（取消後に yes が残置すれば検知対象になる＝
  むしろ整合的。取消後 no へ戻すなら検知対象から消える）。

## 3. 要件

- R1: **取消も人の操作**（関所型・復唱つき）。機械は取消を提案も実行もしない
  （「機械は確定しない」原則の対称形＝「機械は取り消さない」）。
  **【fix1・CANCEL-04 具体化】**: (i) 取消の開始は**弁護士の明示操作のみ**
  （ATTORNEY_ALLOWLIST 検証・裁定④）——検知・監査・エラーからの機械起案を
  しない。 (ii) **機械は取消理由を生成しない**（理由の記録要否・形式は人の入力
  か記録なしかを裁定⑦）。 (iii) 影響範囲の収集（対象 run・App36 行・成果物参照の
  列挙）は**人が対象を指定した後**にのみ実行する読取専用の支援であり、収集結果は
  復唱に載せる。 (iv) **復唱対象の明記**: 案件レコードID・対象 run id・巻き戻し
  対象の App36 record ID 集合・（裁定②の帰結たる）巻き戻し内容の要約——
  値の PII 非搭載規律は既存どおり（record ID・件数のみ）。
- R2: 取消の decision 鎖への表現は**追記のみ**（immutable 維持・遡及書き換えなし）。
- R3: App36 巻き戻しは §3.4 の禁止に対する**構造化された例外**として設計する
  （取消関所ハンドラの一本経路のみが実行可・H11a 監査との整合を明記）。
- R4: 下流波及の扱いを定義する（成果物=Phase5 生成物・App30 封筒・
  後続 run の supersede 連鎖）。
- R5: flag 配下（既定 OFF）・全 suite 無変更 PASS＋追加分。

## 4. 設計骨子と選択肢

### 4.1 decision 鎖への取消の表現（裁定①・本票の中心論点）

| 案 | 内容 | 利点 | 難点 |
|---|---|---|---|
| (A) supersede 型 | confirmed leaf を **rejected が supersede** する遷移を、取消関所経由のときのみ許可（§3.2-v2 の中止セルを「取消語彙」で解禁） | migration 不要・既存 leaf 判定がそのまま効く（leaf=rejected＝未正当化＝H11a 監査とも自然整合） | 「rejected=結果の否認」と「取消=確定の撤回」の**意味が混ざる**（P3-003C 裁定③が守った意味の単純性を崩す） |
| (B) 新 decision 値 `revoked` | CHECK 制約へ第4値追加（migration）＋遷移表へ confirmed→revoked 行を追加 | 意味が明確・監査/可視化で取消を区別可能 | **migration 必要**・DECISIONS 閉集合に依存する全検査（AST/テスト/H11a 判定）の改定波及 |
（推奨は保留——(A) は H11a 監査（confirmed leaf 消失→検知対象化）との整合が
最も素直だが、意味論の混合は §8 裁定③の趣旨と衝突し得るため司令塔裁定とする）

**【fix1・CANCEL-03】初版の「案(C) run 供給側で吸収」は取消方式の選択肢から
外す（撤回・両時点残置のため下に残す）**——(C) は取消の台帳表現を持たず
「旧 run の confirmed leaf が有効なまま残る」ため、取消方式ではなく
**非採用の比較対象＝現行運用そのもの**（誤確定は再導出→新 run→confirmed で
**将来へ向けて訂正**する・DRAFT_P3_003C 裁定④の影響欄の既存原理）である。
本票が設計するのは「この現行運用では足りない場合（App36 の誤 yes を戻したい・
台帳に取消を記録したい）」の経路であり、(C) はその不足の説明として §4.1 に
比較対象として残置する:

> （非採用比較対象・現行運用）取消 decision を作らず、再導出（新 run・supersede）→
> 新 run の confirmed で前へ回す。台帳構造完全不変だが、旧 run の confirmed leaf が
> 残り App36 巻き戻しの根拠記録が台帳に載らない。

### 4.1a 取消可能条件と巻き戻しの照合（fix1・CANCEL-01 追加）

- **取消可能条件の固定**: 取消対象は「対象 run の**有効 leaf が confirmed**」の
  場合**のみ**（held/rejected leaf・decision なし・鎖破損は取消対象外＝それぞれ
  既存の中止セル/破損警報へ）。**新 run が存在する場合**（対象 run が head でない）
  の取消可否は裁定⑥。
  **【fix2・CANCEL-05 追加】さらに「write-set の schema version と存在」を取消
  可能条件に含める**——対象 confirmed の write-set が (i) 存在し (ii) 本設計の
  schema version で解釈可能、の両方を phase 1 で確認する（欠落・旧 version・
  parse 不能は下記 legacy 扱い）。
- **【fix2・CANCEL-05 裁定＝2世代分割】confirmed の世代で経路を分ける**:
  - **write-set 保存開始後の confirmed**: 自動巻き戻し候補＋関所対象（§4.1a の
    照合規律どおり）。
  - **legacy confirmed（write-set 保存開始前・欠落・schema 不明）**:
    **自動巻き戻し禁止・App36 へ write 0・人手調査**へ倒す（postimage 照合の
    根拠が無い巻き戻しは盲目適用になるため）。legacy に対する「App36 は触らず
    **取消台帳のみ追記**する専用運用」（台帳上は取消済み・実機修正は人手）は
    裁定⑧。
- **write-set の保存**: confirmed の projection 実行時に「実際に書いた行と内容」
  ＝write-set（App36 record ID・**insert / update の区別**・書込み field 集合・
  書込み**前**の preimage）を保存する（保存の器は裁定⑤）。
- **巻き戻しの照合**: 取消時、対象行の**現在値が projection の postimage と完全
  一致する場合のみ**「自動巻き戻し候補」として復唱に載せる。**不一致（projection
  後に人手編集・他 run の更新あり）は write 0 で要確認**（機械は上書きしない）。
  insert 行の巻き戻し＝行無効化（裁定②(B) の型）・update 行の巻き戻し＝preimage
  復元、を候補として区別する。

### 4.2 App36 巻き戻し（裁定②）

- 巻き戻し対象は**機械由来フィールドと 戸籍確認済 のみ**（human_state 保護の対称）。
- ~~選択肢: (A) `戸籍確認済=no` へ戻す＋機械由来フィールドは残置（行削除しない）
  (B) 行自体を「取消済み」状態で無効化（RV-08 soft merge の型を借用）
  (C) App36 は触らず H11a 監査の検知に載せて人手修正へ誘導。~~
  （fix5 裁定②＝update 行 preimage 復元／insert 行 無効化 → **fix6・CANCEL-06 で
  postimage 閉集合まで確定**。旧選択肢は撤回・残置）
- **update 行の取消 = preimage 復元**（fix5 裁定のまま・不変）。
- **insert 行の取消 = 無効化（削除しない）**。無効化後の **postimage を閉集合で
  確定**:
  - `戸籍確認済 = no`
  - 新設フィールド **`取消済み` = yes**（App36 への CU 追加が前提・§4.5 前提欄）
- **yes→no の書換えは取消関所ハンドラ経路のみの設計上の例外**（正本 §3.4 の
  逆遷移禁止は維持され、本例外は一本経路の構造化された escape のみ。機械の
  再導出・projection は従来どおり yes を no に落とさない）。
- **consumer の除外 = `取消済み=yes` 行の読み飛ばし**（RV08 の有効行ヘルパと
  同型——App36 読取の共通 filter に含める・単一の正）。
- **H11a 監査との整合（変更不要）**: insert 行は `戸籍確認済=no` 化により
  **H11a の検知対象外**（監査は yes 行のみ走査）＝取消済み行が監査を汚さず、
  **H11a 側の変更は不要**。
- いずれも**取消関所ハンドラの一本経路**からのみ実行（§3.4 例外の構造化）。

### 4.2a 前提（[人]ゲート・fix6/CANCEL-06 追加）

1. **App36 フィールド追加（CU 作業）**: `取消済み`（no/yes・既定 no 想定・実機の
   型/値は CU 時確定）。config.EXPECTED_KINTONE_SCHEMA への監視追随を実装票で
   同時に行う。

### 4.3 下流波及（裁定③）

- App30 封筒: confirmed でクローズ済みの封筒は**再オープンしない**（再オープン
  遷移が存在しない実装現実・P3-003C 裁定②と同根）。取消は**新規の取消封筒**を
  起票して記録する（監査可視性）。
- 成果物（Phase5・協議書等）: 生成済み成果物の回収は**機械はしない**（人の運用）。
  取消封筒の detail へ「要回収の成果物参照」を列挙するまでを機械の責務とする。
- 後続 run: 取消後の正しい確定は**再導出→新 run→confirmed**（既存原理を維持・
  取消は「前へ回す」ための前処理と位置づける）。

### 4.4 取消封筒の状態機械（fix1・CANCEL-02 追加）

- **phase 順序の固定**（confirmed handler の3 phase と同型・宙吊りを作らない順）:
  phase 1=読取専用の全件再検証（取消可能条件・write-set 照合・ALLOWLIST）→
  phase 2=台帳追記（裁定①の取消表現・単一 txn）→ phase 3=App36 巻き戻し
  （裁定②）→ 取消封筒クローズ。**phase 2 より前に App36 へ書かない**。
- **二重取消の冪等キー**: `heir_cancel:{case_record_id}:{run_id}` で取消封筒を
  一意化（既存 `_already_filed` 型の状態不問照合＝同一 run への取消再起票を抑止。
  取消済み run への再取消は phase 1 の leaf 再判定で中止）。
- **ACK 喪失の回収**: phase 2 完了後のクラッシュ（台帳に取消記録あり・App36
  未巻き戻し）は、**再実行時の phase 1 再検証**が「取消記録済み・巻き戻し未了」を
  検出して phase 3 のみ再実行（resumed 型・heir_projection fix2 M02 と同型）。
- **reconcile**: 「取消記録あり×App36 巻き戻し未了×封筒 open」の滞留は
  daily_healthcheck 系の検査対象にできる（入口・周期は裁定⑦の器と併せて実装票）。
  H11a 監査（confirmed leaf 無しの yes 検知）が裁定①(A) の場合の**最終網**になる
  ことを設計上明記（取消後に yes が残置すれば翌朝検知される）。
- **再実行時の再検証**: いずれの再入も phase 1 を素通りしない（write-set 照合・
  現在値照合を毎回実施＝盲目再適用しない・CANCEL-01 と同じ照合規律）。

## 5. flag・テスト骨子

- flag: `HEIR_CANCEL_ENABLED`（新設・既定 OFF・語彙可視性連動＝P3-003-CMD の型）。
- テスト骨子: (i) 取消語彙は flag OFF で不可視・辞退 (ii) ALLOWLIST 検証
  （3 decision 対称の裁定①=(A) を取消にも適用） (iii) 二重取消・取消後の
  再確定の遷移表 (iv) App36 巻き戻しの write 集合固定・human_state 非接触
  (v) H11a 監査との整合（取消後の検知有無が裁定①②の帰結と一致すること）
  (vi) 既存テスト無変更 PASS。

## 6. 裁定欄（OPEN・司令塔）

| # | 論点 | 選択肢 | 状態 |
|---|---|---|---|
| ① | 取消の decision 鎖表現 | (A) supersede 型 / (B) 新値 revoked（migration）——(C) は fix1 で撤回済み | **RESOLVED＝(A) supersede 型**（fix5 裁定——schema 不変優先・migration と閉集合検査の改定波及を避ける。H11a 監査との整合も最も素直） |
| ② | App36 巻き戻し方式 | §4.2 (A)/(B)/(C) | **RESOLVED＝update 行は preimage へ・insert 行は無効化（削除しない）**（fix5 裁定——§4.1a の write-set 区別と 1:1 対応・機械削除禁止の原則維持） |
| ③ | 下流波及の責務境界 | §4.3 案 | **RESOLVED＝§4.3 案承認**（fix5 裁定——取消封筒起票・成果物は列挙まで・再確定は再導出一本） |
| ④ | 取消権限 | (A) ATTORNEY_ALLOWLIST と同一 (B) 専用 allowlist | **RESOLVED＝(A)**（fix5 裁定——現体制は単独弁護士・社員弁護士が入る時点で再裁定） |
| ⑤ | write-set / preimage の保存の器（fix1・CANCEL-01） | (A) 封筒 detail (B) DB 台帳（projection_log） (C) 監査JSON | **RESOLVED＝(B) DB 台帳（projection_log）**（fix5 裁定——immutable 追記・run 紐付けの機械照合に最適・P3-001 流儀） |
| ⑥ | 新 run 存在時の取消可否（fix1・CANCEL-01） | (A) 不可（head のみ） (B) 可 | **RESOLVED＝(A) 不可**（fix5 裁定——head のみ取消可・非 head の誤 projection は要確認へ。stale ガードと同じ原理） |
| ⑦ | 取消理由の記録（fix1・CANCEL-04） | (A) 記録なし (B) 固定 enum | **RESOLVED＝(A) 記録なし**（fix5 裁定——P3-003C 裁定⑤と対称・理由体系の設計を前提にしない） |
| ⑧ | legacy confirmed の専用運用（fix2・CANCEL-05） | (A) 取消対象外 (B) 取消台帳のみ追記 | **RESOLVED＝(B) 取消台帳のみ追記**（fix5 裁定——監査可視性優先・App36 不接触＝実機修正は人手のまま台帳に取消が残る） |

## 7. 両時点残置

- 本 DRAFT は初版。改定は fix 節を追記し、初版記述は撤回理由と併せて残す。

## 8. fix1 改定記録（R-DOCS-BATCH-1-D1・2026-08-11・全所見 ACCEPT）

- **CANCEL-01**: §4.1a 新設——取消可能条件を「有効 confirmed leaf のみ」に固定・
  write-set（insert/update 区別・preimage）保存・postimage 完全一致時のみ自動
  巻き戻し候補・不一致は write 0 要確認。裁定⑤（器）⑥（新 run 存在時）を新設。
- **CANCEL-02**: §4.4 新設——取消封筒の状態機械（phase 順序固定・二重取消の
  冪等キー・ACK 喪失の resumed 型回収・reconcile・再入時の毎回再検証）。
- **CANCEL-03**: §4.1 の案(C) を取消方式の選択肢から**撤回**し「非採用比較対象
  （現行運用＝新 run で将来へ訂正）」へ分離（裁定①は (A)/(B) の二択に更新）。
- **CANCEL-04**: §3 R1 を具体化——取消開始は弁護士の明示操作のみ・機械は理由を
  生成しない・影響収集は人の指定後・復唱対象を明記。裁定⑦（理由記録）を新設。

## 9. fix2 改定記録（R-DOCS-BATCH-1-D2・2026-08-11・前巡全所見 RESOLVED）

- **CANCEL-05（裁定＝2世代分割）**: §4.1a へ (i) 取消可能条件に write-set の
  **schema version／存在確認**を追加 (ii) **世代分割**——write-set 保存開始後の
  confirmed のみ自動候補＋関所対象・**legacy confirmed は自動巻き戻し禁止・
  App36 write 0・人手調査**。legacy 専用運用（App36 不接触・取消台帳のみ追記）を
  裁定⑧として新設。

## 10. fix5 改定記録（司令塔裁定の一括反映・2026-08-11・D5=4票 DESIGN_OK 後）

- 裁定①〜⑧を全件 RESOLVED 化（§6 各行に裁定と1行理由）: ①=(A)supersede 型・
  ②=update 行 preimage 復元/insert 行無効化・③=§4.3 案・④=(A)ALLOWLIST 同一
  （社員弁護士時に再裁定）・⑤=(B)DB 台帳 projection_log・⑥=(A)head のみ・
  ⑦=(A)理由記録なし・⑧=(B)取消台帳のみ追記。
- OPEN 残なし（実装票の着手ゲートは flag・ALLOWLIST 等の既存[人]ゲートのみ）。

## 11. fix6 改定記録（R-DOCS-BATCH-1-D6・2026-08-11・CANCEL-06）

- **CANCEL-06**: §4.2 の insert 行取消を **postimage 閉集合まで確定**——
  `戸籍確認済=no`＋新設 `取消済み=yes`（App36 CU 追加は §4.2a 前提欄・[人]）。
  yes→no は**取消関所経路のみの設計上の例外**と明記（§3.4 逆遷移禁止は維持）。
  consumer 除外＝`取消済み=yes` 読み飛ばし（RV08 有効行ヘルパと同型）。
  **H11a 期待＝no 化により監査対象外（H11a 変更不要）**を明記。update 行＝
  preimage 復元（不変）。旧選択肢 (A)(B)(C) は撤回・取り消し線残置。

## 12. 凍結記録（2026-08-11・R-DOCS-BATCH-1-D8 PASS）

- **巡歴**: 起草（DOCS-BATCH-1 B）→ D1（CANCEL-01〜04・fix1）→ D2（CANCEL-05・
  fix2）→ D3〜D5（DESIGN_OK・fix5 で裁定①〜⑧全件 RESOLVED）→ D6（CANCEL-06・
  fix6）→ D7（PASS・不触）→ **D8 PASS＝本凍結**。
- **凍結条件充足の要旨**: 裁定①〜⑧全件 RESOLVED・取消可能条件/状態機械/
  postimage 閉集合/2世代分割まで確定・OPEN 残なし。
- **実装着手ゲートは凍結条件ではなく実装前提**（[人]: App36 `取消済み`
  フィールドの CU 追加〔§4.2a〕・flag `HEIR_CANCEL_ENABLED` 投入・
  ATTORNEY_ALLOWLIST 投入〔ブロックE〕）。
- **以後の変更は設計改定＋司令塔再裁定を要する**。
