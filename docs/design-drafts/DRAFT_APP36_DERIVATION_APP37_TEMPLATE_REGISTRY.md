# DRAFT: App36 相続人導出 / App37 割付 / TemplateVersion registry 実装設計

> **status: DRAFT（司令塔裁定待ち）・実装開始根拠にしない。**
> 対象SHA 7b03069。R4-3実装・config・封筒/関所パターンの実物調査に基づく叩き台。
> 製品設計完全版v2.4 §8.11/§9.21/§9.23。

## 0. 現況（実物確認）
- **R4-3（heir_derivation.py）は実装済み・純関数・書き込みゼロ**（docstring明記「App34/36への
  書き込みゼロ・封筒起票も本スコープ外=R4-3b」）。凍結テスト47ケース（09-heir-test-cases.md・
  大野2026-07-07承認）でPASS固定。
- **R4-3b（導出結果→App36起票）は未実装**。`APP_SOUZOKUNIN`/`APP_WARITSUKE` は config スキーマ定義と
  スキーマ整合テストのみ。KintoneApp 定義・create_record・変換関数いずれも不在。
- config スキーマは起票を見越して先行整備済み（App36続柄＝HeirCandidate.zokugaraと対応、
  法定相続分=TEXTでFraction文字列可、データ源=戸籍読解）。

## 1. heir_derivation の入出力（結線点の確定）

- 入力: `derive_heirs(persons: list[HeirPerson], declarations, kosekis, decedent_id, at_date)`。
  `persons_from_records(records: list[dict])`（heir_derivation.py:105）が **App34 GET形dict → HeirPerson**
  の読み取り専用変換（自身はfetchしない）。
- 出力: `Derivation`（status/heirs/shares/flags/hold_reasons/rank/provisional）。
  `HeirCandidate`（person_id/name/zokugara/share:Fraction/basis/facts/via）。
- グラフ連携: `required_persons(graph, decedent)`（:709）は kinship_graph を入力に取りZ1ゲート絞り込み。
- **要弁護士フラグ**: flags が1つでもあれば `provisional`（参考値）。**確定は弁護士**（機械は起票まで）。

## 2. DerivationRun テーブル（app-state DB・§9.21）

導出の実行履歴を残す（監査・再現・「いつの戸籍/申告で導出したか」）。§9.21 の field を実装型へ写像案:

| 列 | 型 | 内容 |
|---|---|---|
| id | BigInteger PK | |
| case_app_id / case_record_id | Text | 案件参照（ハブ共通方式） |
| decedent_person_id | Text | 被相続人（App34 record id） |
| at_date | Text | 相続開始日（和暦原文はApp34側・ここは確定西暦） |
| input_person_ids | JSONB | 導出に使ったApp34 record idの集合（再現性） |
| input_hash | Text | persons+declarations の正規化SHA-256（同一入力の再導出dedup） |
| status | Text | derived / held / error（Derivation.status） |
| rank | Integer | 1/2/3/0 |
| result | JSONB | shares（person_id→分数文字列）・heirs明細・flags・hold_reasons |
| provisional | Boolean | 要弁護士フラグ有無 |
| engine_version | Text | heir_derivation のバージョン（テストケース版 v0.1 等・再導出判定） |
| created_at | DateTime(tz) | |

- **PII方針**: result に氏名を持つか要裁定（RV-10）。person_id＋shares のみにして氏名は
  App34/36参照で解決する案が安全（【論点1】）。
- migration は alembic 第4弾（P1-004基盤に乗せる・手動DDL禁止）。DerivationRun は
  inbound_event と同じ `Base`（hub/inbound_event.Base）に相乗りか別metadataか【論点2】。

## 3. App36 起票設計（R4-3b・封筒→関所パターンに乗せる）

実物の2段パターン（person_merge/review_resolve）に接続する。**結線点は調査で確定済み**:

### 3.1 封筒起票（機械）
- 雛形 = `person_merge._file_candidate` / `app30_filer.file_from_pending`
  （App30 に `発送ステータス:"要確認"` ＋ `チャネル固有データ:{"heir_derivation": detail}` ＋
  `create_record(APP_SHIPPING)`・**単票API必須**）。
- detail に DerivationRun の id と shares/heirs（person_id ベース）を格納。
- 冪等キー = `heir_derivation:{case_record_id}:{input_hash}`（同一入力の再導出で二重封筒を防ぐ・
  person_merge の `_pair_key` と同型）。
- flags（要弁護士）があっても封筒は起票する（人が関所で見て確定/保留を判断）。

### 3.2 関所（人の確定）
- `review_resolve.RESOLVERS`（review_resolve.py:371）にトップキー `"heir_derivation"` ＋ ハンドラ ＋
  必要env（`APP_SOUZOKUNIN`/`TOKEN_SOUZOKUNIN`）を追加 → `resolve_group` 経由で発火。
- ハンドラは既存 `_resolve_koseki` 型（phase1: 全件再読の二重確定ガード → phase2: 本テーブルupsert →
  App30を`完了`/`実行済み:yes`でクローズ）。
- **本テーブル = App36**: `HeirCandidate → App36レコード` の写像:
  - zokugara → `続柄`（選択肢が対応済み）／share → `法定相続分`（Fraction文字列）／
    person_id → `相続人レコードID` 参照は無し（App36は人物のprojectionなので `氏名`等はApp34から解決）／
    `データ源:"戸籍読解"`／`戸籍確認済:"no"`（yes は弁護士のみ）／`状態:"通常"`（放棄はdeclarations由来）。
- **App36 を current projection とする**: 同一 case×person の再確定は upsert（冪等キー＝
  case_record_id＋person_id）。registry_ingest._upsert_zaisan（:275）が既存の型
  （search→update/create分岐）。

### 3.3 新規に必要なもの（未存在）
1. `KintoneApp("App 36 (相続人)", "APP_SOUZOKUNIN", "TOKEN_SOUZOKUNIN")` インスタンス定義
2. `derive_heirs`出力 → App36 fields の変換関数（純関数・テスト可能）
3. RESOLVERS ハンドラ（heir_derivation キー）
4. env flag（`HEIR_DERIVATION_ENABLED` 案・既定OFF・person_sync 慣行と同じ安全側）
5. 起動経路: R4-1（人物確定後）の後続として、または指示Bot語彙「相続人を導出して」

## 4. App37 割付の入力正本化

- スキーマ（config:530-557）: 財産レコードID(App35参照)・相続人レコードID(App36参照)・
  取得区分(6種)・持分/代償金額/条件メモ・有効。**財産×相続人の対応の1行=1割付**。
- App37 は**人が入力する正本**（機械は導出しない＝誰が何を取得するかは遺産分割協議の結果）。
  → 実装は「指示Bot or kintone直接入力での割付登録」＋スキーマ死活監視。導出エンジン不要。
- **App36 が起票済みであることが前提**（相続人レコードIDを参照するため）。R4-3b が先。
- 実スキーマ突合: config は 2026-07-06 フォーム設計取得APIで実機11フィールド一致を確認済み
  （config:528コメント）。**追加のCU作業は現時点で不要**（スキーマは既に実機整合）。
  ただし App36 側は起票コードが無いだけでスキーマは整合済み → CU不要。

## 5. TemplateVersion registry（§9.23）

成果物生成（財産目録・遺産分割協議書・遺言）のテンプレを版管理する。

### 5.1 テーブル案（app-state DB）
| 列 | 型 | 内容 |
|---|---|---|
| id | BigInteger PK | |
| template_key | Text | 論理名（例 zaisan_mokuroku / isan_bunkatsu_kyogisho） |
| version | Text | セマンティック版（v1.0 等） |
| file_path | Text | repo内テンプレパス or Drive fileId |
| content_hash | Text | テンプレ実体のSHA-256（改変検知） |
| placeholders | JSONB | 差込プレースホルダ集合（EXPECTED_DOCX_TEMPLATES と突合） |
| status | Text | draft / active / retired |
| activated_at | DateTime(tz) | |

### 5.2 既存資産との接続
- 現状 `config.EXPECTED_DOCX_TEMPLATES` と `hub/docx_builder.validate_template`（daily_healthcheck
  監視項目C）がテンプレのプレースホルダを静的検査済み。TemplateVersion はこれを**DB化して版と
  activ状態を持たせる**もの。content_hash で「テンプレが編集されたら version 更新を強制」。
- 登録フロー案: テンプレ追加/更新 → migration or 管理スクリプトで registry に登録
  （active は1 template_key につき1版）→ 生成時は active 版のみ使用。
- P1〜P4テンプレ（実物）の登録: **BLOCKED_NEEDS_HUMAN**（どのテンプレを正本とするか・
  Drive上の実体の確定は大野）。

### 5.3 Phase 5（成果物生成）への接続点
- 生成器（zaisan_mokuroku.py 等）は active TemplateVersion を引く → docx_builder で差込 →
  Drive/App添付。**前提ゲート**: App36（相続人確定）＋App37（割付確定）＋App35（財産）が
  揃い、かつ「戸籍確認済=yes（弁護士）」であること（souzoku-shorui設計のゲートと同じ）。
- DerivationRun.provisional=True（要弁護士フラグ）なら生成拒否（安全側）。

## 6. 論点・BLOCKED
- 【論点1】DerivationRun.result に氏名を保存するか（RV-10・person_id のみが安全）
- 【論点2】DerivationRun/TemplateVersion の metadata（inbound_event.Base 相乗り or 別Base）
- 【論点3】App36 起票の起動経路（R4-1後続の自動封筒 or 指示Bot語彙）
- 【論点4】App36 の続柄「甥姪（代襲）」等とderive出力の写像の網羅性（凍結47ケースで検証）
- 【論点5】App37割付を指示Botで入れるか kintone直接入力か
- BLOCKED_NEEDS_HUMAN:
  - App36/App37 の env（`APP_SOUZOKUNIN`/`TOKEN_SOUZOKUNIN`/`APP_WARITSUKE`/`TOKEN_WARITSUKE`）
    本番投入・実番号確定（現状 optional で未投入なら監視スキップ）
  - App36トークンの権限（create/update・KINTONE_TOKEN_MATRIX の要確認と同列）
  - TemplateVersion に載せる P1〜P4 テンプレ実物の正本確定（Drive）
  - 遺産分割協議の割付を誰がどう入力する運用か（事務所フロー）
