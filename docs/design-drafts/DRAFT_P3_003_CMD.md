# DRAFT: P3-003-CMD 導出コマンド経路 — 設計（実装禁止・凍結先行・fix1）

- TASK_ID: P3-003-CMD 設計票（設計のみ・コード/テスト実装禁止）／記録日 2026-07-27
  （fix1: R-P3-003-CMD-D1 反映＋裁定反映）
- 調査 BASE: origin/main（p3-003a 着地済み）。**R-P3-003-CMD-D2 で凍結判定**。
- 正本参照（矛盾を作らない・編集しない）: DRAFT_P3_003_ENVELOPE_FLOW **§6 統一契約が正**
  （search 失敗=write 0／policy 失敗=I/O 0／create 通信失敗=結果不明・再実行時に
  完全一致検索で reconcile）・**§2.2 の TOCTOU 受容**（検索型冪等は完全な原子性を
  持たない・二重封筒は要確認止まりで許容）・DRAFT_APP36 §2/§3.7・P3-001・P3-003a。

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
     → heir_derivation.persons_from_records（既存・読み取り専用変換）
  4. 導出: heir_derivation.derive_heirs(persons, declarations, kosekis, ...)
     （凍結エンジン・無改変）→ Derivation（derived/held/error）
  5. payload 変換: hub.derivation_models.build_run_payload → validate（既存・
     胎児合成ID/enum/grammar はここで強制）
  6. run 保存: hub.derivation_models.create_derivation_run（P3-001 正規経路・
     head 一意性/supersedes 連鎖は器が強制）→ run_id。field 写像は §4A の表が正
  7. 封筒結線: hub.heir_envelope.file_heir_envelope(run)（P3-003a 公開関数・
     契約は §6 統一契約）→ filed / already_filed / disabled / not_target
  8. 応答: 指示Bot 返信（結果 summary・PII なし・§5/§6）
```

- 担当の境界: **heir_derive_task は「読取→組立→正規経路の呼出し」のみ**。検証・
  不変条件は既存の器（validate 群・create_derivation_run・file_heir_envelope）に
  委ね、**新しい保存経路・検証の複製を作らない**（AST 機械検査の正規 module 集合も
  変更しない）。
- run 読み戻し: 手順 7 は保存直後の run をそのまま渡す（file_heir_envelope は
  読取専用・result_payload 非読取が契約）。

### 1.1 compute_input_hash の canonical 仕様（fix1 H01・固定 schema）

- 正本 §2.1: 対象=persons＋input_person_revisions＋kosekis＋declarations＋at_date＋
  engine_version＋frozen_case_version（**裁定5: engine_version・frozen_case_version
  を hash 材料に含めることを採用・2026-07-27**）。
- **canonical schema（列単位・固定）**:

| 列 | 型・整列・正規化規則 |
|---|---|
| v | 固定値 1（canonical schema 版数。schema 変更時に増分＝hash 世代の明示） |
| case_app_id / case_record_id | str（数字列・封筒境界 grammar と同一） |
| at_date | str "YYYY-MM-DD"（エンジン入力の確定西暦・そのまま） |
| engine_version / frozen_case_version | str 定数（§8 裁定3） |
| person_ids | **int 昇順に整列した record_id の list**（値は str のまま・整列キーのみ int 変換） |
| person_revisions | record_id（int 昇順）で整列した **[record_id, revision] の pair list**（dict を使わない＝キー順の処理系差を排除） |
| declarations.renounced / .disqualified | **set→文字列昇順の sorted list** |
| declarations.fetuses | **入力順を保持した list**（表示ラベル列＝順序に意味がある・並べ替えない） |
| declarations.adoption_kinds | key（int 昇順）で整列した [key, value] pair list |
| kosekis | **(A) 採用中は JSON null 固定**（§8 裁定2・(B) 採用時に canonical 化を追補してから使用） |

- **値の扱い（固定）**: bool→JSON true/false／欠損→JSON null／**空文字は "" のまま
  保持（null と区別する）**／文字列は **Unicode NFC 正規化**を適用（kintone 由来の
  合成・分解差を吸収）。**改行・制御文字（C0/C1）を含む材料値は canonical 化せず
  導出中止**（policy error・曖昧値を hash に入れない。旧記述「改行非依存」は削除）。
- **直列化**: `json.dumps(canonical, ensure_ascii=False, sort_keys=True,
  separators=(",", ":"))` → UTF-8 bytes → SHA-256 → **小文字 hex64**
  （封筒境界 grammar と整合）。
- **PII 統制**: canonical bytes（fetuses ラベル等の自由文字列を含み得る）は
  **保存・ログ出力しない**（保持するのは hash 値のみ）。
- テスト計画（§7-9）: **順序入替（persons の取得順・set の列挙順）＝同一 hash**／
  **意味的1項目変更（revision 1件・at_date 等）＝別 hash**／直列化の決定性、を必須。

## 2. 起動条件

- **語彙**: 主形「相続人を導出して」。registry の説明には「相続人」「導出」の
  **両語を含む明示指示のみ**該当と記載。**誤爆の最終防波堤は既存 confirm フロー**
  （案件名＋実行内容の復唱→「はい」で実行・それ以外は中止）。
- **主体と経路の限定**: `/webhook/dispatch-bot` のみ（署名検証＋ホワイトリスト＝
  弁護士のみ）。顧客 Bot からは到達不能。cron・startup・webhook 連鎖からの
  自動起動は作らない（§3.7 裁定「初版は自動起動しない」）。
- **flag ゲート**: `HEIR_DERIVATION_ENABLED`（既定 OFF・hub.heir_envelope の判定
  関数を再利用）。OFF 時は固定文言で辞退し **I/O ゼロ**。語彙一覧にも OFF 時は
  載せない。**§3A の暫定条件（申告未確認・kosekis 未供給）は flag OFF 維持を前提と
  した初版仕様**であり、点火判断時に §9 の実機確認と併せて再裁定する。

## 3. 失敗時挙動（§6 統一契約の上での CMD 側設計・fix1 H02 訂正）

前提（§6 統一契約・封筒側）: search 失敗=write 0／policy 失敗=I/O 0／
create 通信失敗=**結果不明（ACK 不明）**・再実行時は冪等キーの完全一致検索で
reconcile。**リトライ判断は CMD 側の責務**。

- **冪等性の正確な記述（fix1 H02: 「全段冪等」を訂正）**:
  - **run 側**: DB 制約（single-root／supersedes UNIQUE）により**一方のみが成立**
    する強い保証（重複 run は構造的に不可）。
  - **封筒側**: **検索型 best-effort 冪等**（P3-003a 正本 §2.2 の TOCTOU 受容）。
    検索→起票の間隙で**稀な二重封筒は許容**——重複しても「要確認」止まりで
    **対外効果ゼロ**・人の関所（二重確定ガード）で検知・収束可能。
- **自動リトライ: しない（推奨・初版）**。理由（fix1 訂正）: 「完全冪等だから」
  **ではなく**、(a) 起動主体が[人]（対話中）であり**順次の再指示で reconcile が
  成立**する（already_filed 回収・run は DB 制約で保護） (b) 自動リトライは
  ACK 不明直後の機械的再試行で**検索型冪等の TOCTOU 窓を踏む頻度を上げ**、検知
  可能とはいえ重複封筒のノイズを増やす——順次再指示（人のテンポ）との
  **比較衡量**で後者を採る。
- 失敗の分類と応答（すべて固定文言＋分類のみ・§5。例外分類は §5A の表が正）:
  | 失敗段 | write 状態 | [人]への応答（指示Bot 返信） |
  |---|---|---|
  | App34 読取失敗 | write 0 | 「読取に失敗（分類名）。再指示で再試行できます」 |
  | derive 失敗（error run） | run は status=error で保存（§8 裁定6） | 「導出エラー: 保留理由の件数のみ。詳細は kintone で確認」 |
  | payload/validate 失敗 | run 未保存・write 0 | 「導出結果が保存規格に適合せず中止（分類名）」＋業務チャネル警報 |
  | run 保存失敗（競合含む） | DB tx 内（部分状態なし） | 「保存に失敗/競合（分類名）。再指示で再試行」 |
  | 封筒 search 失敗 | run 保存済み・封筒 write 0 | 「run #N は保存済み・封筒起票のみ失敗。再指示で封筒のみ再試行」 |
  | 封筒 create 通信失敗 | run 保存済み・**封筒は結果不明** | 「run #N は保存済み・封筒は結果不明。再指示すると完全一致検索で回収（稀に重複した場合も要確認止まり・関所で検知）」 |

### 3A. Declarations・kosekis の暫定条件（fix1 H03・[人]裁定改定 2026-07-27）

旧記述「空/None でも安全側に倒れる」は**撤回**する。裁定:

- **Declarations（裁定1）**: 供給源未確認（§9-2）の間、CMD は Declarations 空で
  導出するが、**保存する run に明示的な held/provisional 条件を課す**——
  (i) **provisional=True を強制**（機械 flags と独立に「申告事項未反映」を印字）
  (ii) 応答文へ「申告事項（放棄・欠格・胎児・養子区分）は未反映＝弁護士確認必須」を
  固定表示 (iii) 封筒 detail の provisional=True が関所で可視。
  (B)（App34 写像）は **§9-2 実機確認後の別途裁定**。
- **kosekis（裁定2）**: None の間は **rank=3（第3順位）の導出を常に held として
  保存する**（F5 収集見込み検査が働かないため確定扱いにしない。derive の結果
  status が derived でも、rank=3 かつ kosekis 未供給なら status=held で保存＝
  保存時の安全側格下げ・**エンジンは無改変**・導出事実の rank/result_payload は
  保持）。(B)（App33 取得）は **§9-3 実機確認後の別票**。

## 4. 冪等・二重起動（同一案件への連続コマンド）

1. **導出前チェック**: 現 head run（case_record_id で supersede されていない run・
   **error run は head 連鎖外＝判定対象外**〔§8 裁定6〕）を取得し、新 input_hash が
   head と同一なら **run を作らない**（§8 裁定5）。この場合も
   **file_heir_envelope(head) は呼ぶ**＝封筒未起票／ACK 不明の回収。
   応答「入力に変化なし・封筒 No.X」。
2. **入力が変わった場合**: 新 run を head の supersede として保存→新封筒
   （input_hash が変わる＝新冪等キー）。
3. **並行二重起動（fix1 H02 訂正）**: 指示Bot は confirm フローで実質直列。万一
   並行した場合、**run は DB 制約が一方のみを成立**させ（他方は IntegrityError→
   run_conflict 応答・再指示で回収）、**封筒は検索型 best-effort 冪等のため稀な
   二重起票があり得るが許容**（要確認止まり・対外効果ゼロ・関所の二重確定ガードで
   検知）。
4. head 取得 helper `get_current_head(case_record_id)`（read-only・SELECT のみ・
   error run を除外）を hub/derivation_models へ新設（§8 裁定4）。

### 4A. DerivationRun への写像表（fix1 M01・全必須 field）

| DerivationRun field | 供給源（Derivation／App34／案件情報） |
|---|---|
| case_app_id / case_record_id | confirm フローで確定した案件（App26 の app id・record id） |
| decedent_person_id | App34 読取結果のうち被相続人フラグ=yes の record_id（0名/複数名はエンジンが error＝§5A の入力異常経路） |
| at_date | 被相続人の death_date（App34 由来・エンジン入力と同一値） |
| frozen_case_version | heir_derivation の凍結表 version 定数（§8 裁定3・"v0.1"） |
| input_person_revisions | App34 取得時の record_id→`$revision`（canonical では pair list・§1.1） |
| input_person_ids | App34 取得 record_id の list（int 昇順） |
| input_hash | compute_input_hash（§1.1） |
| status | Derivation.status。ただし **rank=3 かつ kosekis 未供給は held へ格下げ**（§3A 裁定2） |
| rank | Derivation.rank（0/1/2/3） |
| result_payload | build_run_payload（胎児合成 ID・enum/grammar は validate が強制） |
| result_hash | **canonical(result_payload) の SHA-256 hex64**（§4B） |
| lawyer_flags | build_run_payload の戻り（enum 検証済み） |
| provisional | Derivation.provisional **OR True（§3A 裁定1・申告未確認の間は強制 True）** |
| supersedes_run_id | get_current_head の id（初回 run は未設定。**error run は supersede しない/されない＝非設定・§8 裁定6**） |
| engine_version | heir_derivation のエンジン version 定数（§8 裁定3 と同枠で定数新設＝凍結エンジンへの承認変更として記録） |

### 4B. result_hash の canonical 仕様（fix1 M01）

- 対象=validate 通過後の result_payload（enum/grammar 済み＝**非 PII が構造保証**）。
- 直列化=§1.1 と同一規則（`json.dumps(..., ensure_ascii=False, sort_keys=True,
  separators=(",", ":"))` → UTF-8 → SHA-256 → 小文字 hex64）。
  heirs の並び順は build_run_payload の出力順（導出順）を**そのまま**とする
  （並び自体が導出事実の一部・並べ替えない）。決定性はテスト（§7-14）で pin。

## 5. PII 規律（経路上のデータの流れと漏れ防止）

| 区間 | 顧客データ | 統制 |
|---|---|---|
| App34 読取→derive | 氏名・続柄・生年月日等が**メモリ内のみ** | 保存しない・ログに出さない |
| canonical bytes（§1.1） | fetuses ラベル等 | **保存・ログ出力しない**（hash 値のみ） |
| run 保存 | person_id のみ（氏名は build_run_payload で落ちる） | P3-001 実装済み |
| 封筒 | detail 閉集合・件名は No./run# のみ | P3-003a 実装済み |
| 指示Bot 応答 | 件数・run id・封筒 No のみ（氏名・人物一覧を返さない） | 本設計で固定 |
| ログ | emit 契約の ID/件数のみ・例外は type 名分類のみ | §5A/§6・実装票で sink 検査 |

### 5A. 例外分類表（fix1 M02・変換/伝播と pending の扱い）

| 例外 | heir_derive_task の扱い | [人]応答（固定文言＋分類名のみ） | pending |
|---|---|---|---|
| ChainIntegrityError（P3-001 連鎖 guard） | 捕捉→固定応答へ変換 | 「保存の前提が変化（分類名）。再指示してください」 | invalidate |
| IntegrityError（並行競合・single-root 等） | 捕捉→run_conflict 応答 | 「並行実行と競合。再指示で回収できます」 | invalidate |
| PayloadPolicyError（validate） | 捕捉→固定応答＋**業務チャネル警報**（規格逸脱＝バグ疑い） | 「保存規格に不適合のため中止（分類名）」 | invalidate |
| EnvelopePolicyError（封筒境界） | 捕捉→固定応答 | 「封筒の前提検証で中止（分類名）」 | invalidate |
| KintoneError（App34 読取/封筒 I/O） | 捕捉→固定応答（§3 の失敗段別文言） | 「読取/起票に失敗（分類名）。再指示で再試行」 | invalidate |
| ImmutableRecordError | 捕捉→固定応答＋業務警報（設計上到達しない＝発生はバグ） | 「内部整合性エラー（分類名）」 | invalidate |
| 想定外の Exception | **伝播**（握り潰し禁止・dispatch_bot 上位の既存エラー処理＝警報系に委ねる） | 上位既定 | invalidate（上位既定） |

- **pending は全終端（成功/失敗/例外）で invalidate**（結果に依らず保持しない。
  再実行は必ず新しいコマンド＋confirm から＝古い確認内容での再発火を作らない）。
- **非露出の固定**: 例外本文・App34 の値（氏名 sentinel）は応答文・LINE 通知・
  ログのいずれにも**出さない**（type 名の分類のみ・テスト §7-8/§7-12 で pin）。

## 6. 観測（fix1 M03・ログ enum の固定）

- **構造化ログ（固定 enum・emit 契約）**: `[HEIR-CMD] result=<enum> case=<id>
  run=<id|-> envelope=<record_id|->`。result の enum（**固定・最低区分**）:

| enum | 意味 |
|---|---|
| filed | 新規封筒起票まで完了 |
| already_filed | 既存封筒回収（reconcile 発生の計数） |
| ack_unknown | 封筒 create 通信失敗＝結果不明のまま終了（再指示待ち） |
| run_conflict | run 保存の並行競合（IntegrityError） |
| not_target | 封筒対象外（error run 等） |
| no_change | head と同一 input_hash＝run 非作成（封筒回収結果を併記） |
| failed:<分類> | その他失敗（§5A の分類名） |

- 値は **case/run/record ID と件数のみ**（本文・氏名なし）。
- **[人]の確認手段**: (i) 指示Bot 応答 (ii) kintone App30 封筒 (iii) Railway ログの
  `[HEIR-CMD]`/`[HEIR-ENV]` 検索（already_filed／ack_unknown の件数で reconcile
  状況を把握）。
- daily_healthcheck への計数追加は初版ではしない（必要になれば別票）。

## 7. テスト計画（実装票で書くテストの一覧・fix1 で D1 提案を全収載）

1. registry/parser: 語彙の task_type 分類（正例）＋誤爆 negative。
2. flag ゲート **2種**: OFF=I/O ゼロ＋固定文言／ON=経路実行。
3. 正常系 E2E（mock 境界=kintone のみ）: App34 mock→derive（実エンジン）→
   run 保存（sqlite 実 DB）→file_heir_envelope（実関数＋kintone mock）→filed。
4. 冪等: 同一入力の再コマンド→run 追加ゼロ＋already_filed 回収（no_change 経路）。
5. 再導出: 入力変化（$revision 変化）→新 run が head を supersede＋新封筒。
6. 失敗経路: App34 読取失敗／derive error run 保存／validate 失敗（run 未保存）／
   封筒 search 失敗（run 残存・封筒 write 0）／封筒 create 例外
   （契約 pin TestFailureBehaviorContract 再利用＝ACK 不明→再指示回収）。
7. **並行3種**: (a) run 同時初回（single-root 競合→一方 IntegrityError＝
   run_conflict 応答） (b) supersede 競合（同一 head への二重 supersede→一方拒否）
   (c) 封筒 TOCTOU（検索すり抜けの二重起票が「要確認2枚」で収まり例外にならない
   ＝重複許容の pin）。
8. PII 非漏れ: 応答文・ログ・例外文言に App34 氏名 sentinel が現れない。
9. **canonical hash 3種**: 順序入替（persons 取得順・set 列挙順）＝同一 hash／
   意味的1項目変更（revision/at_date）＝別 hash／直列化の決定性（2回計算一致）。
10. **confirm フロー4種**: 復唱内容（案件名・実行内容）／「はい」で実行／
    「いいえ」等で中止・write 0／pending の全終端 invalidate（成功後・失敗後の
    再発火なし）。
11. **App34 異常系**: 対象案件 0 件／被相続人フラグ 0 名・複数名（エンジン error の
    保存と応答）／$revision 欠落（canonical 化中止＝policy error）。
12. **例外分類**: §5A の表の各行（変換/伝播・応答固定文言・業務警報の有無・
    pending invalidate）。
13. **error head**: error run が head 連鎖外（supersedes 非設定・同一 hash 判定
    対象外・封筒 not_target・旧 run/封筒の地位不変）であることの pin（§8 裁定6）。
14. **写像・result_hash**: §4A 写像表の全 field 充足／result_hash の決定性・
    heirs 並び保持／§3A 裁定（provisional 強制 True・rank3 held 格下げ）の pin。

## 8. 裁定記録（2026-07-27・[人]。旧「未裁定論点」節は本節で解消）

1. **Declarations 供給源**: **裁定改定**（fix1 H03）——「空でも安全側」は撤回。
   初版は空で導出するが **provisional=True 強制＋応答明示**の条件付き（§3A）。
   (B) App34 写像は §9-2 実機確認後に別途裁定。
2. **kosekis**: 初版 None。ただし **rank=3 は常に held 保存**の条件付き（§3A）。
   (B) は §9-3 後の別票。
3. **frozen_case_version**: **(A) 採用**——凍結表 version 定数（"v0.1"）を
   heir_derivation に定数化。**定数追加は凍結エンジンへの承認変更として記録**
   （実装票の diff・work-log に明示。engine_version 定数も同枠）。
4. **新設ヘルパの置き場所**: **(A) 採用**——compute_input_hash／get_current_head は
   hub/derivation_models（AST 正規 module 内・SELECT のみ）。
5. **head 同一 input_hash 時に run を作らない**: **採用**。engine_version・
   frozen_case_version を hash 材料に含める（＝エンジン/凍結表の更新は「入力変化」
   として新 run になる）。
6. **derive error 時の run 保存**: **(A) 採用・細部確定**——error run は
   **supersedes 非設定（head 連鎖外）**・**同一 hash 判定の対象外**・封筒は
   not_target・**旧 run／既存封筒の地位は不変**（error が head を奪わない）。

## 9. 実機確認事項（[人]・凍結後も実装前に要確定）

1. **App34 の案件参照フィールド**の実フィールドコード（検索キー: 案件レコードID
   相当）と、App34 レコードの `$revision` が API で取得できること。
2. App34 に**申告系フィールド**（放棄・欠格廃除・胎児・養子区分）が実在するか
   （§8-1 の (B) 可否の前提）。
3. App33（戸籍）読解データの取得可否と形式（§8-2 の (B) の前提）。
4. 相続案件アプリ（App26）と App34 の紐付け運用（1案件に他案件の人物が混ざらない
   ことの運用確認）。
5. 語彙一覧への公開タイミング（flag 点火と同時か・先行して文言だけ載せるか）。
