# DRAFT: P3-003b 関所 side の前提設計（App36 projection・設計のみ・実装禁止・fix2）

- TASK_ID: P3-003B-D 設計票（設計のみ・コード/テスト実装禁止）／記録日 2026-07-28
  （fix1: D1 反映＋[人]裁定4件確定／fix2: R-P3-003B-D2 反映・記録日 2026-07-29）
- 目的: 凍結 `DRAFT_P3_003_ENVELOPE_FLOW.md`（以下「正本」）§3.2/§3.3 の App36
  projection を**実装可能にするための前提**を固定する——(1) App36 追加 field
  仕様 (2) 冪等キーの App36 上の実現方式と検索規則 (3) derive_heirs 出力→App36
  「続柄」「法定相続分」の表記写像 (4) phase 3 書込み規則 (5) 既存行の移行設計。
- 正本参照（**矛盾を作らない・編集しない**）: 正本 §3.2（3 phase）・§3.3（冪等キー=
  case_record_id＋person_id／H10 条件付き更新／human_state 保護／stale ガード）・
  §3.4（戸籍確認済 yes 遷移表・H11）・§6（欠落補記）・
  `DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md` §3。
- **スコープ外（明記）**: E0–E3 effect level・放棄写像（declarations.renounced →
  App36 状態「放棄済み」）は **v2.4 正本（repo 外）依存**であり本票では固定しない
  （正本 §2.4／§3.6 OPEN のとおり）。本票は「機械が確定後に App36 へ projection
  する際の field・キー・表記・書込み規則・移行」だけを対象とする。
- 実装現実の実査基盤（2026-07-28・read-only）: `hub/derivation_models.py`
  （`_ZOKUGARA_TO_RELATION`・`build_run_payload`・share の直列化）／
  `heir_derivation.py`（zokugara ラベル生成）／`config.EXPECTED_KINTONE_SCHEMA`
  の App36 実 field／正本 §3.2〜3.4。

## 0. [人]裁定の確定記録（fix1・本文と1対1対応）

| 裁定 ID | 論点 | 確定 | 反映先 |
|---|---|---|---|
| **裁定1** | relation_key の粒度（representative が3区分を collapse） | **(A) 条件付き**——result_payload に続柄区分コード（固定 ASCII enum）を追加。§3.5/P3-001 の改定を伴うため、その改定票の着地を P3-003b 実装の前提とする（§3.2・§6-3・M03） | §3.2／§6-3 |
| **裁定2** | App36「続柄」dropdown 欠落 | **(A)**——[人] が dropdown に不足区分を追加。**「その他」への集約はしない**（粒度喪失を許容しない） | §3.2／§6-2 |
| **裁定3** | 胎児行の扱い | **胎児が存在する案件は App36 projection 全体を要確認・停止**（除外案(A)は撤回）。民法886条（胎児は既に生まれたものとみなす）ゆえ確定前提が流動的で、部分反映は誤り。警報＋人対応・出生による実 person 化まで停止（§2A）。**凍結時に[人]の明示確認を要する** | §2A／§6-4 |
| **裁定4** | 単独相続の法定相続分表記 | **`"1分の1"`**（機械的・写像規則を分岐させない） | §3.3 |

## 1. 前提の要約（実装現実・設計判断の土台）

- **result_payload は relation_key（ASCII enum）のみを保持**し、日本語の zokugara
  区分は `build_run_payload` で**落ちる**（正本 §4・PII 統制）。App36「続柄」
  （DROP_DOWN・日本語）へ写すには relation_key からの写像になる——が、relation_key
  は zokugara より**粗い**（representative が3区分を collapse・裁定1）。
- **person_id は App34 の `$id`（数字列）**。ただし**胎児は run 内合成 ID
  `胎児:F{n}`（出現順連番）**で、`build_run_payload` の採番は出現ごと＝
  **run を跨いで安定しない**（同一胎児が再導出で別番号になり得る）→胎児案件は
  停止（裁定3）。
- App36「続柄」「状態」「戸籍確認済」は DROP_DOWN/RADIO で**値域が固定**（実査）。

## 2. App36 追加 field 仕様（[人]が kintone に追加する際の仕様書）

正本 §3.3 の H10（条件付き更新）と冪等キー（case_record_id＋person_id）を実装可能に
するため、App36 に**新規2 field**が要る（正本 §6 の BLOCKED CU の具体化）。

| field 名（案） | 型 | 値の grammar（M02・保存層と逐語一致） | 検索可否 | 用途・根拠 |
|---|---|---|---|---|
| `current_derivation_run_id` | SINGLE_LINE_TEXT | **二段検査（fix2 M02）**: (1) regex `^[1-9][0-9]{0,18}$`（正の整数・前ゼロなし・最大19桁） (2) **数値上限**——int 化して **signed BigInt 上限 `9223372036854775807`（2^63−1）以下**であること。19桁は regex を通るが int64 を超える値（例 9999999999999999999）があるため、regex だけでは不十分＝**必ず数値比較を重ねる** | **要・完全一致検索** | H10 条件付き更新の前提。各 App36 人物レコードが「どの run 由来か」を保持し supersedes 連鎖の祖先判定に使う（正本 §3.3 H10） |
| `導出元人物ID` | SINGLE_LINE_TEXT | **`^[0-9]{1,10}$`**（App34 `$id`＝保存層 heir_derivation の person_id grammar と逐語一致）。**胎児 ID（`胎児:F…`）は本 field に入らない**——胎児案件は projection 停止（裁定3）のため | **要・完全一致検索** | 冪等キー（case_record_id＋person_id）の person_id 片。App36 に person_id 保持 field が現状無いため新設（正本 §3.3 冪等キー＝registry_ingest._upsert_zaisan 型の person 単位一意） |

- **grammar の出所（M02）**: `導出元人物ID` の `^[0-9]{1,10}$` は封筒側（P3-003a
  `_CASE_RECORD_ID_RE`/person_id）および heir_derivation の App34 `$id` 表記と逐語
  一致（保存層で数字列 1〜10 桁）。`case_record_id`（既存 field で表現）も同 grammar。
  胎児合成 ID grammar（`^胎児:F[1-9][0-9]*$`）は**参考記載のみ**で、本 field の
  許可 grammar には含めない（胎児案件は停止＝§2A・裁定3）。
- **型の選択理由（SINGLE_LINE_TEXT・数値型でない）**: 既存 App30/App36 の ID 系
  field が SINGLE_LINE_TEXT の数字列で統一（実査）。ID を数値計算しない・既存踏襲・
  前ゼロ等の表現差回避のため SINGLE_LINE_TEXT を推奨。
- **既存 field との非干渉**: 追加は上記2点のみ。機械由来 field（続柄/法定相続分/
  データ源）と human_state field（戸籍確認済/状態）には触れない（正本 §3.3 保護）。

## 2A. 胎児案件の projection 停止（裁定3・[人]確定・凍結時に明示確認要）

- **規則**: 導出結果に **1 行でも zokugara=胎児（relation_key=fetus）が含まれる案件**
  は、**App36 projection を全体停止して要確認**とする（confirmed decision があっても
  App36 へは書かない・封筒はクローズせず要確認のまま or 明示保留）。
- **理由（民法886条）**: 胎児は相続については既に生まれたものとみなされるが、
  死産なら遡って相続人でなかったことになる（886条2項）。確定前提が出生まで流動的で、
  **胎児を含む相続人構成を部分的に App36 へ確定反映するのは誤り**。加えて胎児の
  person_id は run 跨ぎ非安定（§1）で冪等キーに載せられない。
- **運用**: 業務チャネル警報（案件 record_id・胎児行の件数のみ・氏名非出力）＋
  [人]対応。**出生による実 person 化（App34 に実 record 追加）後に通常 projection**
  へ復帰（その経路は別票）。
- **安定 ID 方式は将来票**: 胎児に案件内安定 ID を採番して部分反映する案は
  build_run_payload の採番規約改定を伴い波及が大きいため、本票では採らない。
- **凍結時の明示確認**: 本停止方針は業務影響（胎児案件は自動反映されない）が
  大きいため、**凍結判定時に[人]の明示承認を要する**（裁定4件のうち唯一、
  運用停止を伴うため）。

## 3. derive_heirs 出力 → App36「続柄」「法定相続分」写像

### 3.1 続柄（DROP_DOWN・値域固定）

- **App36「続柄」の実 dropdown 値域（実査）**: 配偶者／子／直系尊属／兄弟姉妹／
  甥姪（代襲）／受遺者（相続人外）／その他（7値）。
- **zokugara 区分 9 種と写像**（裁定1-(A) の続柄区分コードを介した total 写像）:

| # | zokugara 区分（原文） | relation_key（現保存値） | 続柄区分コード（裁定1・新設 enum 案） | App36 続柄（裁定2 で dropdown 拡張後） |
|---|---|---|---|---|
| 1 | 配偶者 | spouse | `spouse` | 配偶者（既存） |
| 2 | 子 | child | `child` | 子（既存） |
| 3 | 直系尊属 | lineal_ascendant | `lineal_ascendant` | 直系尊属（既存） |
| 4 | 兄弟姉妹 | sibling | `sibling` | 兄弟姉妹（既存） |
| 5 | 甥姪（代襲） | representative | `nephew_niece_rep` | 甥姪（代襲）（既存） |
| 6 | 孫（代襲） | representative | `grandchild_rep` | **孫（代襲）**（裁定2 で追加） |
| 7 | 再代襲（曾孫等） | representative | `further_rep` | **再代襲（曾孫等）**（裁定2 で追加） |
| 8 | 胎児 | fetus | `fetus` | **projection 停止**（裁定3・§2A・続柄は書かない） |
| 9 | 数次承継（No.… の …） | successive | `successive` | **数次承継**（裁定2 で追加） |

- **核心問題の再掲（裁定1 の背景）**: result_payload は relation_key のみ保持し、
  representative が 孫（代襲）／甥姪（代襲）／再代襲 を **collapse** するため、
  relation_key からは「孫」か「甥姪」かを復元できない。裁定1-(A) で**続柄区分コード
  （zokugara 相当の固定 ASCII enum）を payload に追加**して total 写像を成立させる。

### 3.2 続柄写像の裁定（確定）

- **裁定1（粒度・確定=(A) 条件付き）**: result_payload に**続柄区分コード
  （固定 ASCII enum）**を追加する。上表の9コードで閉じる（`spouse/child/
  lineal_ascendant/sibling/nephew_niece_rep/grandchild_rep/further_rep/fetus/
  successive`）。この改定は §3.5 payload schema／P3-001 の改定であり、その改定票の
  着地が **P3-003b 実装の前提**（§6-3）。
- **続柄区分コードの取扱い契約（fix2 M03・P3-001 改定票に含める条件）**:
  - **最小化対象**: 氏名は保持しないが、**person_id と結合された続柄は「個人に関する
    情報」**（誰がどの続柄か）である。PII だと断定はしない（法的カテゴリの enum）が、
    **最小化対象**として扱い、必要範囲（続柄写像）を超えて保持・流通させない。
  - **保存の閉集合強制**: 続柄区分コードは**上記固定9値以外を保存しない**
    （schema allowlist に enum を加え、enum 外は PayloadPolicyError で保存拒否＝
    relation_key/facts と同型の grammar 強制）。
  - **非露出**: 続柄区分コードの値を**ログ・例外文言・業務通知へ出さない**
    （封筒 detail・relation_key と同じ非露出規律）。
  - これらを **P3-001（§3.5 result schema allowlist）改定票の条件**に含める
    （enum 追加・allowlist 拡張・hash 再計算範囲の改定を同時に定義する・§7）。
  - **旧 run（コード欠落）**: 改定前に保存された run は続柄区分コードを持たないため
    **精密 projection 不可＝要確認扱い**（relation_key からの粗い写像に頼らず、
    続柄を書かず要確認とする。§7 の BLOCKED と一貫）。
- **裁定2（dropdown・確定=(A)）**: [人] が App36「続柄」dropdown に **孫（代襲）／
  再代襲（曾孫等）／数次承継**を追加。**「その他」への集約はしない**（粒度喪失を
  許容しない）。胎児は projection 停止（裁定3）のため dropdown 追加不要。
- 帰結: 裁定1＋裁定2 で **続柄区分コード→拡張 dropdown の total 写像**（上表）が
  実装可能。両方 kintone 実機変更／schema 改定を伴い **[人] ゲート**。

### 3.3 法定相続分（SINGLE_LINE_TEXT）

- **保存形**: result_payload の share は `"{numerator}/{denominator}"`（既約分数・
  例 `"1/2"`・`"1/6"`。`build_run_payload` の直列化を実査）。**grammar は保存層
  （derivation_models `_SHARE_RE`）と逐語一致で `^[0-9]{1,6}/[1-9][0-9]{0,5}$`
  を採る（fix2 M02）**——分子は 0 を許す（`[0-9]{1,6}`。0 分子＝該当なしの表現を
  保存層が許容している事実に合わせる。保存層が正）／分母は正（`[1-9][0-9]{0,5}`）。
  projection は保存層の grammar をそのまま受ける（一致側を採る。もし 0 分子を
  App36 で扱わない等の変更が要るなら、それは**保存層 grammar の改定を前提**とする
  別事項＝本票では改定しない）。
- **表記写像規則**: `"n/d"` → **`"d分のn"`**（例 `"1/2"`→`"2分の1"`・`"1/6"`→
  `"6分の1"`）。numerator=n・denominator=d。
- **単独相続（`"1/1"`）**: **`"1分の1"`（裁定4・確定）**——写像規則を分岐させない。
- **share=None**（held で相続分未確定）: 法定相続分は**空欄**（機械は書かない）。
- **端数・約分**: share は Fraction 厳密演算の既約分数で既に約分済み。projection 側で
  再約分・四捨五入は**しない**（丸めは相続分の意味を壊す）。

## 4. phase 3 の App36 書込み規則（H01・**主体別2表**・正本 §3.2〜3.4 と逐語整合）

正本 §3.2 phase 3（decision=confirmed のときのみ App36 upsert）・§3.3（H10 条件付き
更新・human_state 保護・stale ガード）・§3.4（戸籍確認済 yes 遷移＝弁護士のみ・
逆遷移禁止）を、**書込み主体ごとに**列別可否表へ分離する（fix2 H01）。主体は
2つ——**(A) confirmed handler**（`_resolve_heir_derivation` の phase 3・decided_by が
ATTORNEY_ALLOWLIST 検証済み）と **(B) 機械再導出**（confirm を伴わない導出 run の
projection）。

### 4A. 主体(A) = confirmed handler（ATTORNEY_ALLOWLIST 検証後の phase 3）

| App36 列 | insert（0件一致） | update（既存行・H10 祖先一致） | 根拠（正本） |
|---|---|---|---|
| 続柄／法定相続分／データ源 | 書く（§3.1/§3.3・データ源="戸籍読解" 相当） | 差分更新 | §3.3 機械由来 |
| current_derivation_run_id／導出元人物ID | 書く | current を新 run へ進める | §3.3 H10 |
| **戸籍確認済** | **`no→yes` を設定**（decided_by の allowlist 検証済み＝§3.2 phase 2・§3.4） | **`no→yes` を設定**（insert/update を問わず confirmed handler は yes を書ける） | §3.2 phase 2／§3.4 |
| 状態 | 初期値のまま（放棄写像はスコープ外） | 手修正を上書きしない | §3.3 保護 |
| 氏名／住所／生年月日／本籍／連絡先 | 機械は書かない（person_id のみ・氏名非保持） | 同左 | 正本 §4 PII |

- **確定点（fix2 H01）**: 戸籍確認済 の `no→yes` は **confirmed handler なら
  insert/update を問わず許可**する。これは正本 §2.2 の「human_state は run＋最新
  decision の join projection で読む」と一致——decision が confirmed（弁護士）なら
  projection 上 yes であり、App36 の当該列へ yes を反映するのが正しい。
- `yes→no`（逆遷移）は confirmed handler も**起こさない**（§3.4 逆遷移禁止。
  confirmed は yes へ上げる方向のみ）。

### 4B. 主体(B) = 機械再導出（confirm を伴わない projection）

| App36 列 | insert（0件一致） | update（H10 祖先一致） | 根拠（正本） |
|---|---|---|---|
| 続柄／法定相続分／データ源 | 書く | 差分更新 | §3.3 機械由来 |
| current_derivation_run_id／導出元人物ID | 書く | current を新 run へ進める | §3.3 H10 |
| **戸籍確認済** | 触れない（初期値のまま＝no） | **読み書き対象に含めない**——既存 `yes` を絶対に下げない | §3.4 逆遷移禁止・§3.3 保護 |
| 状態 | 初期値のまま | 手修正を上書きしない | §3.3 保護 |
| 氏名等 | 機械は書かない | 同左 | 正本 §4 PII |

- **主体(B) の核心（fix2 H01・機械側に限定した制約）**: 機械再導出は戸籍確認済 列を
  **読み書き対象に含めない**（H10 差分更新の対象列から除外）。したがって機械再導出が
  `yes→no` を起こす経路が**構造的に存在しない**。この「読み書き対象に含めない」は
  **主体(B) の記述であり、主体(A) の confirmed handler には掛からない**（(A) は
  §4A のとおり yes を書く）。

- **stale ガード（正本 §3.3・両主体共通）**: 確定/再導出時に対象 run が supersedes
  連鎖の head でなければ projection せず aborted（両表の update 自体に到達しない）。

## 5. 冪等キーの App36 上の実現方式と検索規則（＋TOCTOU 正確化）

- **冪等キー**（正本 §3.3）= `case_record_id ＋ person_id`。App36 上の実現:
  `案件レコードID`（既存 field）＝ case_record_id／`導出元人物ID`（§2 新設）＝ person_id。
- **upsert 検索クエリ**（読取専用の探索・複数検索で可）:
  ```
  案件レコードID = "<case_record_id>" and 導出元人物ID = "<person_id>"
  order by $id asc limit 2
  ```
  値は kintone query へ埋める前に grammar（§2）で検証（数字列のみ・注入遮断）。
- **1件一致時の状態閉集合（H02-iii・実装票で網羅すること）**:

  | 状態 | 判定 | アクション |
  |---|---|---|
  | same run 再実行（current_derivation_run_id == 新 run.id） | 冪等ヒット | **no-op**（既に当該 run で projection 済み。二重書込みしない） |
  | 祖先 run（current が新 run の supersedes 連鎖の祖先） | H10 更新可 | 機械由来列を差分**更新**＋current を新 run へ進める（§4 表） |
  | current_derivation_run_id が空・不正（grammar 外） | 移行前/破損 | **要確認**（write 0・§H02 の backfill 前提と接続・警報） |
  | 無関係 run（祖先でも子孫でもない・別系列） | 競合 | projection **せず要確認**（write 0・「別系列の run が既存」警報） |
  | 祖先確認中に DB 不達（run 系列照会が失敗） | 判定不能 | **write 0・要確認**（結果不明を確定扱いにしない） |
  | 子孫 run（current の方が新しい） | stale | §3.3 stale ガードで aborted（本表前に弾かれる） |

- **2件以上一致** → 冪等キー重複＝異常。**書かず要確認**（件数と record_id のみ・
  氏名非出力）。
- **TOCTOU の残存リスク（M01・正確化）**: **同一 head run に対する並行 projection**で、
  両者が「0件」を検索してから双方 insert すると **二重 insert が起こり得る**。
  これは **stale ガードの対象外**（stale ガードは「より新しい run の存在」を見るもので、
  同一 run の並行初回書込みは検知しない）。正本 §2.2 の best-effort 受容の範囲だが、
  **実害の収束は重複検知＋人手**に委ねる:
  - **重複検知（実装票の必須要件）**: projection 後（または daily 監査で）
    `案件レコードID＋導出元人物ID` が2件以上ある App36 行を検出→**業務チャネル警報**
    （件数・record_id のみ）。
  - **人手収束手順（実装票の必須要件・手順書化・fix2 M01 の決定規則）**: 警報を
    受けた [人] が重複行を次の一意規則で収束させる:
    1. **残す1行の決定規則**: (i) `current_derivation_run_id` が最も新しい
       （head に近い）行を残す。 (ii) **同一 head 並行重複では current が同値になる**
       ため決定不能——その場合は **`$id`（App36 record ID）が最小の1行を残す**
       （一意な tiebreak・どの環境でも同じ行が残る決定的規則）。
    2. **削除前の保全確認**: 削除する行に **human_state（戸籍確認済=yes・状態の
       手修正）や 氏名/住所/連絡先 等の手入力情報が「残す行には無く削除行だけに
       ある」場合は削除しない**——先に残す行へ手で集約してから削除する（機械由来列
       以外の情報を消さない）。
    3. 機械は自動削除しない（immutable 台帳の削除操作を機械に持たせない規律）。
       kintone 上の削除/無効化は [人] が実施。
  - 新規の原子性機構は作らない（正本と整合）。

## 6. 既存 App36 行の移行設計（H02・点火ゲート）

新設2 field（§2）は既存 App36 行では**空**になる。projection の H10 判定は
current_derivation_run_id に依存するため、移行を点火ゲートとして固定する。

1. **点火前データ調査（点火ゲート・[人]）**: App36 の**全既存行数**と、そのうち
   `current_derivation_run_id` が空の行数を実機で調査する（read-only 集計・氏名非
   出力・件数のみ）。**この調査結果の確認を projection 点火（`HEIR_DERIVATION_ENABLED`
   相当の projection 有効化）の前提条件**とする。
2. **方針の分岐（fix2 H02・(b) を安全側へ全面改定）**:
   - **(a) App36 実質空**（既存行ゼロ or 導出対象案件に既存行なし）: backfill 不要。
     projection は新規 insert から始まる。**まず (a) を前提確認**（実機調査で確定）。
   - **(b) 既存行あり（改定・安全側）**: **当該案件は projection を停止する
     （write 0・要確認）**。**意図的な重複 insert で後から収束させる方式は採らない**
     ——既存行には human_state（戸籍確認済=yes・状態の手修正）や 氏名/住所/連絡先 等の
     手入力情報が既にあり、機械が突合せず新規 insert すると **これらと切り離された
     重複行が生まれ、収束時に手入力情報を失う危険**があるため。
     手順: (i) 機械は当該案件を projection せず要確認警報（件数・record_id のみ）
     → (ii) **[人] が既存行と導出 person の対応を確認し、新設2 field
     （`導出元人物ID`・`current_derivation_run_id`）を手で backfill** → (iii)
     backfill 完了後にのみ通常 update 経路（§5 の1件一致＝祖先判定）へ入れる。
   - **この方式が保全を構造的に満たす理由**: 既存行を消さず・複製せず、機械は
     backfill 済み行に対してのみ機械由来列を差分更新する（§4B）。human_state と
     手入力列は §4 の書込み表で保護対象のまま＝**移行によって手入力情報が失われる
     経路が存在しない**。
3. **移行中の 1件一致（current 空）の扱い**: §5 の状態表のとおり **write 0・要確認**
   （空の current は H10 の祖先判定ができない＝安全側）。backfill 完了案件のみ通常
   update 経路へ乗る。**意図的重複を移行手段にしない**（本節 (b) 改定と一貫）。

## 7. 実装票への申し送り（本票が確定した前提）

- **確定（実装票がこの仕様で書ける）**: App36 追加 field（§2・grammar M02）／
  冪等キー検索規則・1件一致状態表（§5）／phase 3 書込み規則（§4）／法定相続分表記
  （§3.3・裁定4）／胎児案件停止（§2A・裁定3）／既存行移行（§6）／重複検知＋人手収束
  （§5・実装票の必須要件）。
- **[人]ゲート（実装の前提・解けるまで BLOCKED）**: 裁定1（続柄区分コードの
  §3.5/P3-001 改定票の着地）／裁定2（dropdown 拡張の kintone 実機変更）／§2A の胎児
  停止の凍結時明示承認／§6 の点火前データ調査。これらが揃うまで続柄 projection は
  実装 BLOCKED。
- E0–E3・放棄写像はスコープ外（v2.4 正本確認後の別票）。

## 8. 裁定反映確認表／実装前ゲート表（fix2 M04・2表に分離）

### 8A. 裁定反映確認表（裁定1〜4 と反映節の1対1）

| 裁定 ID | 確定内容 | 反映節 |
|---|---|---|
| 裁定1 | 続柄区分コード（固定9値 ASCII enum）を payload へ追加＝(A) 条件付き | §3.1 表／§3.2／§3.2 取扱い契約（M03） |
| 裁定2 | App36「続柄」dropdown に 孫（代襲）/再代襲/数次承継 を追加（その他集約なし） | §3.1 表／§3.2 |
| 裁定3 | 胎児案件は App36 projection 全体停止・要確認（除外案 A 撤回・886条） | §1／§2A／§3.1 表（#8） |
| 裁定4 | 単独相続の法定相続分表記＝`"1分の1"` | §3.3 |

### 8B. 実装前ゲート表（[人]・すべて解けるまで続柄 projection は BLOCKED）

| # | ゲート項目 | 種別 | 関連節 | [人]回答欄 |
|---|---|---|---|---|
| 1 | App36 への `current_derivation_run_id`／`導出元人物ID` 実機 field 追加・field コード確定・完全一致検索可否 | 実機 field 追加 | §2 | 〔ここに大野回答〕 |
| 2 | App36「続柄」dropdown へ 孫（代襲）／再代襲（曾孫等）／数次承継 を追加（その他集約なし） | dropdown 拡張 | 裁定2／§3.2 | 〔ここに大野回答〕 |
| 3 | result_payload への続柄区分コード追加＋取扱い契約（enum 閉集合・最小化・非露出・hash 再計算）を含む §3.5/P3-001 改定票の起票・着地 | P3-001 改定 | 裁定1／§3.2（M03）／§7 | 〔ここに大野回答〕 |
| 4 | 既存 App36 行の点火前データ調査（全行数・current 空件数）と方針(a)/(b) の確定 | 既存データ調査 | §6 | 〔ここに大野回答〕 |
| 5 | 胎児案件の projection 全体停止・要確認方針の**明示承認**（運用停止を伴う） | 裁定3 の明示承認 | 裁定3／§2A | 〔ここに大野回答〕 |
