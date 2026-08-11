# DRAFT: P3-003C-CANCEL — confirmed 済み projection の取消（§8-4 別票）

- status: **DRAFT**（凍結は D 巡後・R-CANCEL-D1 から）
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
| (C) run 供給側で吸収 | 取消 decision を作らず、**再導出（新 run・supersede）→ 新 run を held のまま**にする運用で実質取消 | 台帳構造完全不変 | 旧 run の confirmed leaf が残る＝「有効な確定」が見かけ上残置・App36 巻き戻しの根拠記録が台帳に載らない |

（推奨は保留——(A) は H11a 監査（confirmed leaf 消失→検知対象化）との整合が
最も素直だが、意味論の混合は §8 裁定③の趣旨と衝突し得るため司令塔裁定とする）

### 4.2 App36 巻き戻し（裁定②）

- 巻き戻し対象は**機械由来フィールドと 戸籍確認済 のみ**（human_state 保護の対称）。
- 選択肢: (A) `戸籍確認済=no` へ戻す＋機械由来フィールドは残置（行削除しない）
  (B) 行自体を「取消済み」状態で無効化（RV-08 soft merge の型を借用）
  (C) App36 は触らず H11a 監査の検知に載せて人手修正へ誘導。
- いずれも**取消関所ハンドラの一本経路**からのみ実行（§3.4 例外の構造化）。

### 4.3 下流波及（裁定③）

- App30 封筒: confirmed でクローズ済みの封筒は**再オープンしない**（再オープン
  遷移が存在しない実装現実・P3-003C 裁定②と同根）。取消は**新規の取消封筒**を
  起票して記録する（監査可視性）。
- 成果物（Phase5・協議書等）: 生成済み成果物の回収は**機械はしない**（人の運用）。
  取消封筒の detail へ「要回収の成果物参照」を列挙するまでを機械の責務とする。
- 後続 run: 取消後の正しい確定は**再導出→新 run→confirmed**（既存原理を維持・
  取消は「前へ回す」ための前処理と位置づける）。

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
| ① | 取消の decision 鎖表現 | §4.1 (A) supersede 型 / (B) 新値 revoked（migration） / (C) run 側吸収 | **OPEN** |
| ② | App36 巻き戻し方式 | §4.2 (A) yes→no＋残置 / (B) 行無効化 / (C) 触らず検知誘導 | **OPEN** |
| ③ | 下流波及の責務境界 | §4.3 案（取消封筒起票・成果物は列挙まで・再確定は再導出一本）の承認 or 修正 | **OPEN** |
| ④ | 取消権限 | (A) ATTORNEY_ALLOWLIST と同一 (B) 取消専用のより狭い allowlist | **OPEN** |

## 7. 両時点残置

- 本 DRAFT は初版。改定は fix 節を追記し、初版記述は撤回理由と併せて残す。
