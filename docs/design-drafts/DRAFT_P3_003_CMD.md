# DRAFT: P3-003-CMD 導出コマンド経路 — 設計（実装禁止・凍結先行・fix4）

- TASK_ID: P3-003-CMD 設計票（設計のみ・コード/テスト実装禁止）／記録日 2026-07-27
  （fix1: D1 反映／fix2: D2 反映＋[人]再裁定反映／fix3: D3 反映／
  fix4: R-P3-003-CMD-D4 反映・記録日 2026-07-28）
- 調査 BASE: origin/main（p3-003a 着地済み）。**R-P3-003-CMD-D5 で凍結再判定**。
- 正本参照（矛盾を作らない・編集しない）: DRAFT_P3_003_ENVELOPE_FLOW **§6 統一契約が正**
  （search 失敗=write 0／policy 失敗=I/O 0／create 通信失敗=結果不明・再実行時に
  完全一致検索で reconcile）・**§2.2 の TOCTOU 受容**・DRAFT_APP36 §2/§3.7・
  P3-001・P3-003a（§3B の公開契約改定を含む）。

## 1. 経路全体（各段の入出力契約と担当モジュール）

```
[人]（弁護士・LINE 指示Bot）
  │ 語彙「相続人を導出して」（§2）
  ▼
dispatch_bot（既存基盤）: router（署名検証+ホワイトリスト）→ parser（task_type 分類）
  → TASK_REGISTRY entry task_type="heir_derivation"（新規1エントリ）
  → confirm フロー（既存 Pending 確認・「はい」で実行）
  ▼
dispatch_bot/heir_derive_task.py（新規・隔離 module＝person_merge_task と同型）
  1. flag ゲート: HEIR_DERIVATION_ENABLED（既定 OFF→固定文言で辞退・I/O ゼロ）
  2. 案件特定: 既存 confirm フローの案件指定（顧客名突合/No.直指定＝上位 T2 責務）
     → case_app_id / case_record_id
  3. App34 読取: kintone.search_records(App34, 案件参照=case_record_id・$revision 込み)
     → **読取後に全レコードの案件参照=case_record_id を検証**（別案件人物の混入は
     中止・fix2 M03）→ heir_derivation.persons_from_records（既存・読取専用変換）
  4. 導出: heir_derivation.derive_heirs(persons, declarations, kosekis, ...)
     （凍結エンジン・無改変）→ Derivation（derived/held/error）
     ※ **error は保存しない**（§8 裁定6改定・固定応答＋構造化ログのみ＝§3）
  5. payload 変換: hub.derivation_models.build_run_payload → validate（既存）
  6. run 保存: hub.derivation_models.create_derivation_run（P3-001 正規経路）
     → run_id。field 写像は §4A の表が正（**derived/held のみで閉じる**）
  7. 封筒結線: hub.heir_envelope.file_heir_envelope(run)（P3-003a 公開関数・
     §3B の改定契約）→ filed / already_filed / disabled
  8. 応答: 指示Bot 返信（結果 summary・PII なし・§5/§6）
```

- 担当の境界: **heir_derive_task は「読取→組立→正規経路の呼出し」のみ**。検証・
  不変条件は既存の器に委ね、**新しい保存経路・検証の複製を作らない**（AST 機械検査の
  正規 module 集合も変更しない）。
- run 読み戻し: 手順 7 は保存直後の run をそのまま渡す。

### 1.1 compute_input_hash の canonical 仕様（fix1 H01→fix2 M01/M02 拡充）

- 正本 §2.1: 対象=persons＋input_person_revisions＋kosekis＋declarations＋at_date＋
  engine_version＋frozen_case_version（裁定5: engine_version・frozen_case_version を
  hash 材料に含める・2026-07-27）。
- **canonical schema（列単位・固定）**:

| 列 | 型・整列・正規化規則 |
|---|---|
| v | 固定値 2（fix2 で persons 展開を追加した schema 版数。変更時に増分） |
| case_app_id / case_record_id | str（数字列） |
| at_date | str "YYYY-MM-DD" |
| engine_version / frozen_case_version | str 定数（§8 裁定3） |
| persons | **record_id（int 昇順）で整列した「HeirPerson 全 engine 入力 field の固定 pair list」**（fix2 M01・下記） |
| person_revisions | record_id（int 昇順）整列の [record_id, revision] pair list。**revision は persons 内容の代替ではなく併存材料**（revision が変わらない編集経路や取得タイミング差に対し、内容 field 自体も hash 対象とすることで再現性を二重化する） |
| declarations.renounced / .disqualified | set→文字列昇順 sorted list |
| declarations.fetuses | 入力順保持 list（順序に意味・並べ替えない） |
| declarations.adoption_kinds | key（int 昇順）整列の [key, value] pair list |
| kosekis | (A) 採用中は JSON null 固定（§8 裁定2） |

- **persons の固定 pair list（fix2 M01・並び順この通り・全 engine 入力 field）**:
  `[["record_id", v], ["name", v], ["alive", v], ["death_date", v],
  ["death_wareki", v], ["is_decedent", v], ["father_id", v], ["mother_id", v],
  ["adoptive_father_id", v], ["adoptive_mother_id", v],
  ["born_before_parents_adoption", v],
  ["events", [[事項種別, 年月日, 相手方], ...（入力順保持）]]]`
  — heir_derivation.HeirPerson／LifeEvent の全 field と一致（エンジン入力の完全写像。
  field 追加はエンジン改定＝schema 版数 v の増分と同時のみ）。
- **入力型・grammar の固定（fix2 M02・「1 と "1" の別 hash」の遮断）**:
  canonical へ入れる値は**すべて「kintone API が返す文字列表現」または bool** に
  統一する。**型不正は暗黙変換せず canonical 化中止**（policy error）——
  revision=数字列 str（int が来たら拒否）／declaration の人物 ID・adoption_kinds の
  key=数字列 str・value=str（非 str は拒否）／is_decedent・
  born_before_parents_adoption=bool のみ。
- **値の扱い（固定）**: bool→JSON true/false／欠損→JSON null／空文字は "" のまま
  保持（null と区別）／文字列は Unicode NFC 正規化／**改行・制御文字（C0/C1）を
  含む材料値は canonical 化せず導出中止**。
- **直列化**: `json.dumps(canonical, ensure_ascii=False, sort_keys=True,
  separators=(",", ":"))` → UTF-8 → SHA-256 → 小文字 hex64。
- **PII 統制**: canonical bytes は**氏名・身分事項を含む**ため、**保存・ログ出力を
  一切しない**（保持するのは hash 値のみ・§5）。

#### 1.1a canonical 層の責務分離（fix3 M02・field 別の固定表）

**原則**（fix5 M01 で一意化）: canonical 層の責務は「**型固定＋文字面の健全性**
（型検査・数字列 grammar・NFC・C0/C1 拒否）」のみ。**意味検証は canonical 層の
責務外**——現状、変換層（persons_from_records）と凍結エンジンが値を**利用**するが、
**完全な値域検証はどの層も行わない**（canonical 層に意味検証を複製して二重の正を
作らない、という方針は不変）。

| field | 許容型 | 空文字 | 数字列 grammar | enum/date 等の意味検証の担当層 |
|---|---|---|---|---|
| record_id | str | 不可 | `^[0-9]+$` 必須（canonical 層で検査） | 存在検証=App34 読取（§1 手順3） |
| name | str | 可（"" 保持） | なし | なし（原文のまま・エンジンも解釈しない） |
| alive | str | 不可 | なし | **どの層も値域（生存/死亡/不明）を機械強制しない（fix4 M01→fix5 逐語訂正）**——エンジンの `_classify_death()` は「生存」→alive・「不明」→unknown、**それ以外の値（値域外を含む）**は死亡日なし=undated／死亡日あり=文字列比較で pre/post/same へ進む。canonical=型のみ |
| death_date | str | 可 | なし | **どの層も形式（YYYY-MM-DD）・実在性を完全保証しない（fix4 M01）**——変換層は kintone DATE field の値をそのまま写すだけ（kintone 側の field 型が事実上の保証源）。canonical=型のみ |
| death_wareki | str | 可 | なし | なし（参考原文） |
| is_decedent / born_before_parents_adoption | bool のみ（type is bool） | — | — | canonical=型のみ（真偽の妥当性=エンジン） |
| father_id / mother_id / adoptive_father_id / adoptive_mother_id | str | 可（親不明=""） | **非空なら** `^[0-9]+$`（canonical 層で検査） | 参照整合=エンジン |
| events[].kind | str | 不可 | なし | **語彙の閉集合検証はどの層にもない（fix5 訂正・「参考提示用」は撤回）**——`kind=="婚姻"/"離婚"` は配偶者関係の成立・解消判定に使われ**導出結果へ直接影響**し、変換層は `kind=="死亡"` を death_wareki 抽出に使用する。canonical=型のみ |
| events[].date / events[].partner | str | 可 | なし | なし（和暦原文・氏名原文のまま） |
| revision | str | 不可 | `^[0-9]+$` 必須 | なし（kintone が正） |
| declarations の人物 ID（renounced/disqualified/adoption_kinds の key） | str | 不可 | `^[0-9]+$` 必須 | 参照整合=エンジン |
| declarations.fetuses の要素 / adoption_kinds の value | str | 不可 | なし | 値域（普通養子/特別養子）=エンジン |
| case_app_id / case_record_id / at_date | str | 不可 | ID は `^[0-9]+$`・at_date は `^\d{4}-\d{2}-\d{2}$`（文字面のみ） | 日付の実在性=変換層 |

- 表の違反（型不正・grammar 不正・C0/C1・必須空）は**canonical 化中止**
  （policy error・§5A の payload_policy 枠・値は非反射で位置情報のみ）。
- **実装済みの保証と将来保証の分離（fix4 M01・[人]裁定済み）**: 上表の
  「担当層」列は**現に実装されている保証だけ**を記録する（「エンジンが検証する
  はず」という期待を保証として書かない）。alive 値域・death_date 形式/実在性・
  events[].kind 語彙は**現状どの層も完全保証しない**——hash 再現性の観点では
  canonical の型固定＋文字面検査で足りており、意味検証を将来追加する場合は
  **本設計の改定ではなく別票**（エンジンまたは変換層の改定として起票）とする。
- **field 集合の機械検査（fix3 M02）**: `dataclasses.fields(HeirPerson)`／
  `fields(LifeEvent)` の名前集合と canonical schema の field 集合の**完全一致**を
  実装票テストで assert する（§7-20。エンジンに field が追加されたのに canonical
  仕様（schema 版数 v）が未更新のまま、という乖離を構造的に FAIL させる）。

## 2. 起動条件

- **語彙**: 主形「相続人を導出して」。registry の説明には「相続人」「導出」の
  両語を含む明示指示のみ該当と記載。**誤爆の最終防波堤は既存 confirm フロー**。
- **主体と経路の限定**: `/webhook/dispatch-bot` のみ（署名検証＋ホワイトリスト＝
  弁護士のみ）。自動起動は作らない（§3.7 裁定）。
- **flag ゲート**: `HEIR_DERIVATION_ENABLED`（既定 OFF・hub.heir_envelope の判定
  関数を再利用）。OFF 時は固定文言で辞退し I/O ゼロ（**task 直接呼出しでも同様**＝
  ゲートは task 冒頭・テスト §7-15）。語彙一覧にも OFF 時は載せない。
  **実行途中の OFF**（run 保存後に flag が落ちた場合）は file_heir_envelope が
  disabled を返す＝応答「run 保存済み・封筒未起票（機能停止中）。再開後の再指示で
  回収」（§6 の envelope_result=disabled・テスト §7-16）。
  §3A の暫定条件は flag OFF 維持を前提とした初版仕様。

## 3. 失敗時挙動（§6 統一契約＋§3B 改定契約の上での CMD 側設計）

- **冪等性の正確な記述（fix1 H02）**: run 側=DB 制約（single-root／supersedes
  UNIQUE）で**一方のみ成立**の強い保証／封筒側=**検索型 best-effort 冪等**
  （§2.2 TOCTOU 受容・稀な二重封筒は要確認止まり・対外効果ゼロ・関所で検知）。
- **自動リトライ: しない**（fix1: 順次再指示による reconcile と、機械的再試行が
  TOCTOU 窓を踏む頻度を上げる重複リスクとの比較衡量）。
- 失敗の分類と応答（固定文言＋分類のみ。例外分類は §5A の表が正）:
  | 失敗段 | write 状態 | [人]への応答（指示Bot 返信） |
  |---|---|---|
  | App34 読取失敗・混入検知 | write 0 | 「読取に失敗/案件不一致（分類名）。再指示で再試行できます」 |
  | derive 失敗（error）| **run 非保存・write 0**（§8 裁定6改定） | 「導出エラー: 保留理由の件数のみ（保存はしていません）」＋`[HEIR-CMD] run=not_saved_error` |
  | 被相続人 0名/複数名 | 同上（エンジン error＝保存対象外） | 同上（件数のみ） |
  | payload/validate 失敗 | run 未保存・write 0 | 「保存規格に不適合のため中止（分類名）」＋業務チャネル警報 |
  | run 保存失敗（競合含む） | DB tx 内（部分状態なし） | 「保存に失敗/競合（分類名）。再指示で再試行」 |
  | 封筒 policy 失敗（EnvelopePolicyError） | run 保存済み・封筒 I/O 0 | 「run #N は保存済み・封筒の前提検証で中止」 |
  | 封筒 search 失敗（EnvelopeSearchError） | run 保存済み・封筒 write 0 | 「run #N は保存済み・封筒起票のみ失敗。再指示で封筒のみ再試行」 |
  | 封筒 create 通信失敗（EnvelopeCreateUnknownError） | run 保存済み・**封筒は結果不明** | 「run #N は保存済み・封筒は結果不明。再指示すると完全一致検索で回収」 |
- **error run の監査**: DB レベルの error 監査（導出失敗履歴の永続化）が必要に
  なった場合は、**immutable 台帳とは別の append-only 監査テーブルを新票**で設計する
  （derivation_run へ error を混ぜない＝head 連鎖・封筒対象の語彙を derived/held に
  閉じたまま保つ）。

### 3A. Declarations・kosekis の暫定条件（fix1 H03・[人]裁定改定 2026-07-27）

- **Declarations（裁定1）**: 供給源未確認（§9-2）の間は空で導出し、保存 run に
  **provisional=True を強制**＋応答へ「申告事項（放棄・欠格・胎児・養子区分）は
  未反映＝弁護士確認必須」を固定表示＋封筒 detail の provisional で関所可視。
  (B) は §9-2 実機確認後の別途裁定。
- **kosekis（裁定2）**: None の間は **rank=3 の導出を常に held として保存**
  （保存時の安全側格下げ・エンジン無改変・導出事実の rank/result_payload は保持）。
  (B) は §9-3 後の別票。

### 3B. P3-003a 公開契約の改定（fix2 H03・[人]承認済み→fix3 M01 精密化）

- `file_heir_envelope` の失敗は**段階別の固定例外3種で閉じる**
  （`stage` 属性の値域も **{"policy", "search", "create"} で閉じる**・fix3 M01）:
  - EnvelopePolicyError（stage="policy"・kintone I/O 前＝I/O 0）
  - **EnvelopeSearchError**（stage="search"・search 段の I/O 失敗・write 0）
  - **EnvelopeCreateUnknownError**（stage="create"・create 通信失敗＝ACK 不明・
    結果不明）
  §6 統一契約の意味論（I/O 0／write 0／結果不明）は不変＝**例外の型と stage で
  どの段の失敗かを機械判定可能にする**改定。
- **vendor 例外非保持の具体契約（fix3 M01→fix4 H02・[人]裁定済み）**:
  - wrapper 例外の `args` は**固定値のみ**（分類名・stage。vendor 例外の
    message/str を含めない）。
  - vendor 例外を**属性へ保存しない**（`self.original = e` 型の保持を禁止）。
  - **ラップ構造の指定（fix4 H02）**: vendor 例外を捕捉した `except` ブロックでは
    **分類（stage）だけを変数へ記録して例外を抜け、固定 wrapper は except ブロックの
    **外**で raise する**——
    ```python
    stage_failed = None
    try:
        ...  # vendor I/O
    except RequestException:
        stage_failed = "search"
    if stage_failed is not None:
        raise EnvelopeSearchError(stage_failed)   # except の外 → __context__ is None
    ```
    これにより **`__context__ is None`・`__cause__ is None` が実際に成立**する。
    （`raise ... from None` は `__suppress_context__` で表示を抑制するだけで
    `__context__` に vendor 例外オブジェクトが残るため、「連鎖全段に sentinel
    非残存」の検査と両立しない——fix3 の from None 指定は本構造へ置換・改定。）
- **実装票への要求事項**: 本改定は P3-003a の契約変更のため、実装票で
  **契約 pin テスト（TestFailureBehaviorContract）の同時更新**を必須とする——
  search 失敗=EnvelopeSearchError・create 失敗=EnvelopeCreateUnknownError・
  ACK 喪失回収テストの例外型 assert に加え、**sentinel 入り vendor 例外を発生させ、
  wrapper の `str()`／`repr()`／`args` に sentinel 非残存・
  `__context__ is None`・`__cause__ is None` の検査**（§7-18・fix4 H02 で
  実装可能な形に同期）と **stage 値域 {"policy","search","create"} の
  閉集合 pin** を必須とする。

## 4. 冪等・二重起動（同一案件への連続コマンド）

1. **導出前チェック**: 現 head run（case_record_id で supersede されていない run。
   **保存対象が derived/held のみ**〔§8 裁定6改定〕のため error 除外条件は不要）を
   取得し、新 input_hash が head と同一なら **run を作らない**（§8 裁定5）。
   この場合も **file_heir_envelope(head) は呼ぶ**＝封筒未起票／ACK 不明の回収。
2. **入力が変わった場合**: 新 run を head の supersede として保存→新封筒。
3. **並行二重起動**: run は DB 制約が一方のみを成立（他方は IntegrityError→
   run_conflict 応答）。封筒は検索型 best-effort 冪等（稀な重複は許容・§3）。
4. `get_current_head(case_record_id)`（read-only・SELECT のみ）を
   hub/derivation_models へ新設（§8 裁定4）。

### 4A. DerivationRun への写像表（fix1 M01→fix2 H01: derived/held のみで閉じる）

**前提: status=error の Derivation は保存しない**（§8 裁定6改定・本表に error 行は
存在しない）。

| DerivationRun field | 供給源（Derivation／App34／案件情報） |
|---|---|
| case_app_id / case_record_id | confirm フローで確定した案件 |
| decedent_person_id | 被相続人フラグ=yes の record_id（0名/複数名はエンジン error＝**保存対象外**・§3） |
| at_date | 被相続人の death_date（エンジン入力と同一値） |
| frozen_case_version | 凍結表 version 定数（§8 裁定3・"v0.1"） |
| input_person_revisions | App34 取得時の record_id→`$revision` |
| input_person_ids | App34 取得 record_id の list（int 昇順） |
| input_hash | compute_input_hash（§1.1） |
| status | Derivation.status（**derived／held のみ**。rank=3 かつ kosekis 未供給は held へ格下げ=§3A 裁定2） |
| rank | Derivation.rank（0/1/2/3） |
| result_payload | build_run_payload（validate が enum/grammar 強制） |
| result_hash | canonical(result_payload) の SHA-256 hex64（§4B） |
| lawyer_flags | build_run_payload の戻り |
| provisional | Derivation.provisional OR True（§3A 裁定1） |
| supersedes_run_id | get_current_head の id（初回 run は未設定） |
| engine_version | エンジン version 定数（§8 裁定3 と同枠・凍結エンジンへの承認変更として記録） |

### 4B. result_hash の canonical 仕様（fix1 M01）

- 対象=validate 通過後の result_payload（非 PII が構造保証）。直列化=§1.1 と同一
  規則→SHA-256 hex64。heirs の並び順は build_run_payload の出力順を保持。
  決定性はテスト（§7-14）で pin。

## 5. PII 規律（経路上のデータの流れと漏れ防止）

| 区間 | 顧客データ | 統制 |
|---|---|---|
| App34 読取→derive | 氏名・続柄・生年月日等が**メモリ内のみ** | 保存しない・ログに出さない |
| canonical bytes（§1.1） | **氏名・身分事項を含む** | **保存・ログ出力を一切しない**（hash 値のみ） |
| run 保存 | person_id のみ | P3-001 実装済み |
| 封筒 | detail 閉集合・件名は No./run# のみ | P3-003a 実装済み |
| 指示Bot 応答 | 件数・run id・封筒 No のみ | 本設計で固定 |
| ログ | emit 契約の ID/件数のみ・例外は type 名分類のみ | §5A/§6・実装票で sink 検査 |

### 5A. 例外分類表（fix1 M02→fix2 H03/H04→fix3 H01 で run/封筒の軸を分離）

| 例外 | heir_derive_task の扱い | ログ enum（§6 の 2軸） | [人]応答（固定文言＋分類名のみ） |
|---|---|---|---|
| ChainIntegrityError | 捕捉→固定応答 | run=failed:chain_integrity ／ envelope=skipped | 「保存の前提が変化。再指示してください」 |
| IntegrityError（並行競合） | 捕捉→run_conflict 応答 | run=run_conflict ／ envelope=skipped | 「並行実行と競合。再指示で回収できます」 |
| PayloadPolicyError | 捕捉→固定応答＋**業務チャネル警報**（規格逸脱＝バグ疑い） | run=failed:payload_policy ／ envelope=skipped | 「保存規格に不適合のため中止」 |
| EnvelopePolicyError（stage="policy"） | 捕捉→固定応答 | run=created|no_change ／ **envelope=failed:policy** | 「run #N は保存済み・封筒の前提検証で中止」 |
| **EnvelopeSearchError**（stage="search"・§3B） | 捕捉→固定応答 | run=created|no_change ／ **envelope=failed:search** | 「run #N は保存済み・封筒起票のみ失敗。再指示で再試行」 |
| **EnvelopeCreateUnknownError**（stage="create"・§3B） | 捕捉→ack_unknown 応答 | run=created|no_change ／ **envelope=ack_unknown** | 「run #N は保存済み・封筒は結果不明。再指示で回収」 |
| KintoneError（App34 読取） | 捕捉→固定応答 | run=failed:kintone_read ／ envelope=skipped | 「読取に失敗。再指示で再試行」 |
| ImmutableRecordError | 捕捉→固定応答＋業務警報（到達＝バグ） | run=failed:immutable ／ envelope=skipped | 「内部整合性エラー」 |
| 想定外の Exception（**run 保存前**・fix4 H01） | **伝播**（握り潰し禁止・dispatch_bot 上位の既存エラー処理へ。finally でログ emit＋pending invalidate 後に再送出） | run=failed:unexpected ／ envelope=skipped | 上位既定 |
| 想定外の Exception（**run 保存後**＝封筒呼出し中・応答/ログ生成中・fix4 H01） | 同上（伝播。run は保存済みのまま残る＝再指示で封筒 reconcile 可能） | run=created|no_change ／ **envelope=failed:unexpected** | 上位既定 |

- **run_result の failed:<分類> 閉集合（fix2 M04→fix3 H01 で整理）**:
  **{chain_integrity, payload_policy, kintone_read, immutable, unexpected}**——
  run 段の失敗のみ（run_conflict／not_saved_error は独立 enum 値）。
  **封筒段の分類（policy／search／create）は run_result に混入させない**——
  封筒段の失敗は run が保存済みの事後であり、envelope_result 側
  （failed:policy／failed:search／ack_unknown）だけで表現する（§6 の対応表が正）。
- **pending の invalidate（fix2 H04・[人]裁定済み）**: **CMD の execute_fn 内
  finally で実施**（成功／分類済み失敗／想定外例外のすべての終端で invalidate＝
  task 固有の実装）。**dispatch_bot handler 本体は無改変**・既存タスクの
  「二重 OK」動作は**不変**（スコープ境界。heir_derivation だけが finally 方式）。
- **非露出の固定**: 例外本文・App34 の値（氏名 sentinel）は応答文・LINE 通知・
  ログのいずれにも出さない（type 名の分類のみ）。

## 6. 観測（fix1 M03→fix2 M04: 2軸分離→fix3 H01: enum 完全化＋合法組合せ表）

- **構造化ログ（固定 enum・emit 契約）**:
  `[HEIR-CMD] run=<run_result> envelope=<envelope_result> case=<id>
  run_id=<id|-> envelope_no=<record_id|->`
- **run_result enum（閉集合）**:
  | enum | 意味 |
  |---|---|
  | created | 新 run 保存 |
  | no_change | head と同一 input_hash＝run 非作成 |
  | not_saved_error | derive error＝非保存（§8 裁定6改定） |
  | run_conflict | 保存の並行競合 |
  | failed:<分類> | その他 run 段失敗（§5A の run 段閉集合5種のみ） |
- **envelope_result enum（閉集合・fix3 H01 で失敗値を完全化→fix4 H01 で
  unexpected を段階分離）**:
  | enum | 意味 |
  |---|---|
  | filed | 新規封筒起票 |
  | already_filed | 既存封筒回収（reconcile 計数） |
  | **failed:policy** | EnvelopePolicyError（stage="policy"・I/O 0 で中止） |
  | **failed:search** | EnvelopeSearchError（stage="search"・write 0 で失敗） |
  | **failed:unexpected** | run 保存後（封筒呼出し中・応答/ログ生成中）の想定外例外（fix4 H01・run は保存済みのまま＝再指示で reconcile） |
  | ack_unknown | EnvelopeCreateUnknownError（**「結果不明」として failed とは別扱い**——失敗確定ではなく reconcile 対象） |
  | disabled | flag OFF（実行途中 OFF の境界含む・§2） |
  | skipped | run 側が created/no_change 以外＝封筒段に未到達 |
- **合法な (run_result, envelope_result) の対応表（fix3 H01→fix4 H01 完全化・
  これが閉集合の正。§5A の全例外行がこの表のいずれかに写像される＝「全例外に
  合法組合せが存在する」）**:
  | run_result | 許容される envelope_result |
  |---|---|
  | created ／ no_change | filed ／ already_filed ／ disabled |
  | created ／ no_change | failed:policy ／ failed:search ／ **failed:unexpected** ／ ack_unknown |
  | not_saved_error ／ run_conflict ／ failed:<run 段分類5種（unexpected は**run 保存前**のみ）> | **skipped のみ** |
  - **ログ生成関数は定義外の組合せを拒否する**（emit 前に対応表と照合し、表外は
    ValueError＝バグの即時顕在化。「封筒段に未到達なのに filed」等の矛盾ログを
    構造的に排除する契約）。§5A の各例外→2軸値の写像はこの表の部分集合であることを
    実装票の table test（§7-17）で機械検査。
  - **unexpected の段階分離（fix4 H01）**: 発生段階で表現を分ける——run 保存前の
    想定外=(failed:unexpected, skipped)／run 保存後の想定外=(created|no_change,
    failed:unexpected)。「run=failed:unexpected かつ envelope=failed:*」の組合せは
    定義外（run 段で死んだなら封筒段には到達していない）。
- 値は case/run/record ID と件数のみ。[人]の確認手段は fix1 と同じ（指示Bot 応答・
  App30 封筒・Railway ログ検索）。daily_healthcheck 追加は初版なし。

## 7. テスト計画（実装票で書くテストの一覧・fix2 で M03/H03/H04・fix3 で 17〜20 を追加）

1. registry/parser: 語彙分類（正例）＋誤爆 negative。
2. flag ゲート: OFF=I/O ゼロ＋固定文言／ON=経路実行。
3. 正常系 E2E（mock 境界=kintone のみ）: App34 mock→derive 実→run 保存（sqlite）→
   file_heir_envelope 実→filed。
4. 冪等: 同一入力再コマンド→run 追加ゼロ＋already_filed（no_change 経路）。
5. 再導出: 入力変化→新 run が head を supersede＋新封筒。
6. 失敗経路: App34 読取失敗／**derive error＝run 非保存・DB 行ゼロ・応答とログのみ**
  （裁定6改定の pin）／validate 失敗（run 未保存）／EnvelopeSearchError（run 残存・
  封筒 write 0）／EnvelopeCreateUnknownError（ACK 不明→再指示回収・
  **契約 pin テストの同時更新**=§3B）。
7. 並行3種: run 同時初回（single-root 競合→run_conflict）／supersede 競合／
   封筒 TOCTOU（重複許容の pin）。
8. PII 非漏れ: 応答文・ログ・例外文言に氏名 sentinel が現れない。
9. canonical hash: 順序入替=同一 hash／意味的1項目変更=別 hash／決定性／
   **persons 内容 field の変更（revision 不変でも）＝別 hash**（fix2 M01 の併存材料）。
10. confirm フロー: 復唱内容／「はい」実行／「いいえ」中止・write 0／
    **期限切れ・二重 OK・別指示割込み**（fix2 M03: 期限切れ後の OK=無効・
    二重 OK=1回のみ実行・割込み後の旧 pending 無効）。
11. App34 異常系: 対象 0 件／被相続人 0名・複数名（**非保存**・応答のみ）／
    $revision 欠落（canonical 化中止）／**別案件人物の混入検知＝中止**（fix2 M03）。
12. 例外分類: §5A の表の各行（変換/伝播・固定文言・警報有無）。
13. **pending state 3系統**（fix2 H04）: 成功／分類済み失敗／想定外例外——
    いずれも execute_fn の finally で invalidate されること・既存タスクの二重 OK
    動作が不変であること。
14. 写像・result_hash: §4A 全 field 充足（error 行なし）／result_hash 決定性／
    §3A 裁定（provisional 強制・rank3 held 格下げ）の pin。
15. **canonical 入力の型・文字（fix2 M02/M03）**: int revision の拒否
    （"1" と 1 の別 hash 遮断＝型不正拒否）／NFC 同値の同一 hash／C0/C1 混入拒否／
    null と空文字の区別。
16. **flag 境界（fix2 M03）**: task 直接呼出しでも OFF=I/O ゼロ／実行途中 OFF
    （run 保存後）→ envelope disabled 応答・再指示で回収。
17. **2軸 enum の table test（fix3 M03-i→fix4 H01 同期）**: §6 の合法組合せ表を
    定数としてテストに収載し、実装のログ生成関数と**全対一致**を検査（分岐網羅では
    なく「表との一致」——表にある組合せは全て受理・表にない組合せは全て拒否、の
    両方向）。§5A の各例外→2軸値の写像が表の部分集合であること＝**§5A の全例外行に
    合法組合せが存在する**ことも同時に assert（unexpected の run 保存前/後の
    段階分離を含む・(failed:unexpected, failed:*) が拒否されることの負系込み）。
18. **安全な例外ラップ（fix3 M03-ii→fix4 H02 で実装可能な形に同期・§3B）**:
    sentinel 入り vendor 例外を各段（policy／search／create）で発生させ、
    wrapper 例外の `str()`／`repr()`／`args` に sentinel 非残存・
    **`__context__ is None`・`__cause__ is None`**（§3B のラップ構造＝except
    ブロック外 raise が成立している証明）・stage 値域
    {"policy","search","create"} の閉集合 pin。
19. **canonical blob の非残存（fix3 M03-iii→fix4 M02 で対象を正確化）**:
    非残存の対象は**直列化済み canonical blob**（§1.1 の json.dumps 出力文字列/
    bytes）とし、検査範囲は**永続化（DB 全行）・ログ出力・応答文・全例外の
    str/repr**。**エンジン入力 mock の呼出し引数に氏名等が正規入力として現れる
    ことは許容**（それはエンジンの正当な入力であり漏れではない）。これと分離して、
    **呼出し終了後に canonical blob の永続的コピーが残らないこと**（compute_input_hash
    が blob を module 変数・キャッシュ・戻り値に保持せず hash 値のみ返すこと）を
    別の検査として実施。
20. **field 集合の構造試験（fix3 M03-iv・§1.1a）**: `dataclasses.fields(HeirPerson)`
    ／`fields(LifeEvent)` の名前集合と canonical schema の field 集合の完全一致を
    assert——エンジンへ field を追加すると、canonical 仕様（schema 版数 v）を
    更新しない限り**このテストが FAIL する**構造にする（hash 材料の黙った欠落防止）。

## 8. 裁定記録（[人]。改定履歴付き・遡及書き換えにしない）

1. **Declarations 供給源**（2026-07-27 改定）: 「空でも安全側」撤回→空導出は
   provisional=True 強制＋応答明示（§3A）。(B) は §9-2 後に別途裁定。
2. **kosekis**（2026-07-27 改定）: 初版 None＋rank=3 は常に held 保存（§3A）。
3. **frozen_case_version**（2026-07-27）: (A) 採用——凍結表 version 定数を
   heir_derivation に定数化。**定数追加は凍結エンジンへの承認変更として記録**
   （engine_version 定数も同枠）。
4. **新設ヘルパの置き場所**（2026-07-27）: (A) 採用——compute_input_hash／
   get_current_head は hub/derivation_models。
5. **head 同一 input_hash 時に run を作らない**（2026-07-27）: 採用。
   engine_version・frozen_case_version を hash 材料に含める。
6. **derive error 時の run 保存**（改定履歴）:
   - 初裁定（2026-07-27・D1 後）: (A) 保存を採用（error は head 連鎖外等の細部付き）。
   - **再裁定（2026-07-27・D2 後・現行）: 初裁定を撤回し「保存しない」へ改定**——
     derive error 時は run 非保存・固定応答＋構造化ログ（`run=not_saved_error`）
     のみ。0名/複数名エラーも同枠（保存対象外）。写像表は derived/held のみで
     閉じる。**DB レベルの error 監査が必要になれば append-only の別テーブルを
     新票で設計**（immutable 台帳へ error を混ぜない）。
7. **P3-003a 公開契約の改定**（2026-07-27・[人]承認済み・fix2 H03）: 段階別固定
   例外（EnvelopeSearchError／EnvelopeCreateUnknownError・stage 属性・vendor 本文
   非保持）。実装票で契約 pin テストの同時更新を必須化（§3B）。
8. **pending invalidate の実装位置**（2026-07-27・[人]裁定済み・fix2 H04）:
   CMD の execute_fn 内 finally（task 固有）。dispatch_bot handler 本体は無改変・
   既存タスクの二重 OK 動作不変（§5A）。
9. **例外ラップの構造**（2026-07-28・[人]裁定済み・fix4 H02）: vendor 例外を
   捕捉した except ブロックの**外**で固定 wrapper を raise する構造を指定
   （`__context__ is None` を実際に満たす。fix3 の `from None` 指定は
   `__suppress_context__` による表示抑制に留まり「連鎖全段に sentinel 非残存」と
   両立しないため置換・改定＝§3B）。
10. **§1.1a 責務分離表の正確化**（2026-07-28・[人]裁定済み・fix4 M01）:
    alive 値域・death_date 形式/実在性・events[].kind 語彙は「canonical では
    意味検証しない・既存層も完全保証しない」と現状を正確に記録。実装済み保証と
    将来保証を分離し、意味検証の将来追加は本設計の改定ではなく別票とする。

## 9. 実機確認事項（[人]・凍結後も実装前に要確定）

1. **App34 の案件参照フィールド**の実フィールドコード（検索キー: 案件レコードID
   相当）と、App34 レコードの `$revision` が API で取得できること。
2. App34 に**申告系フィールド**（放棄・欠格廃除・胎児・養子区分）が実在するか
   （§8-1 の (B) 可否の前提）。
3. App33（戸籍）読解データの取得可否と形式（§8-2 の (B) の前提）。
4. 相続案件アプリ（App26）と App34 の紐付け運用（§1 手順3 の混入検証 field の実在
   確認を含む）。
5. 語彙一覧への公開タイミング（flag 点火と同時か・先行して文言だけ載せるか）。
