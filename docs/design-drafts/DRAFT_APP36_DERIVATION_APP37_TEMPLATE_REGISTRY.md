# DRAFT: App36 相続人導出 / App37 割付 / TemplateVersion registry 実装設計（v2・Codexレビュー反映）

> **status: DRAFT（司令塔裁定待ち）・実装開始根拠にしない。**
> 対象SHA 7b03069。R4-3実装・config・封筒/関所パターンの実物調査に基づく叩き台。
> 製品設計完全版v2.4 §8.11/§9.21/§9.23。R-P1-007-drafts-v2（全ACCEPT・REJECT0）反映。
> **OPEN は仮決めせず owner を明記。**

---

## ★共有節: 実装順序骨子（M11・3 DRAFT 共通）

（詳細は DRAFT_RV04_HMAC_MIGRATION §共有節）
1. redaction contract 確定 → 2. RV10 S1切替＋fail-closed → 3. RV04 multipart PoC →
4. RV04 GAS群 HMAC＋dual-accept → 5. RV10 S2/S3/S4＋AST →
6. **App36 DerivationRun（immutable）＋App36 projection 起票（R4-3b）** →
7. **App37 割付＋TemplateVersion registry** →
8. dead-man 監視＋dual-accept 廃止＋kintone webhook 代替。

本書は段6・段7 の設計。

---

## 0. 現況（実物確認）
- **R4-3（heir_derivation.py）は実装済み・純関数・書き込みゼロ**（docstring「App34/36への書き込み
  ゼロ・封筒起票も本スコープ外=R4-3b」）。凍結テスト47ケース（09-heir-test-cases.md・
  大野2026-07-07承認）で PASS 固定。
- **R4-3b（導出→App36起票）は未実装**。`APP_SOUZOKUNIN`/`APP_WARITSUKE` は config スキーマ定義と
  スキーマ整合テストのみ。KintoneApp 定義・create_record・変換関数いずれも不在。

## 1. heir_derivation の入出力（結線点）
- 入力: `derive_heirs(persons, declarations, kosekis, decedent_id, at_date)`。
  `persons_from_records(records)`（heir_derivation.py:105）が App34 GET形dict → HeirPerson の
  読み取り専用変換。
- 出力: `Derivation`（status/heirs/shares/flags/hold_reasons/rank/provisional）。
  `HeirCandidate`（person_id/name/zokugara/share:Fraction/basis/facts/via）。
- グラフ連携: `required_persons(graph, decedent)`（:709）が Z1 ゲート絞り込み。
- flags が1つでもあれば `provisional`。**確定は弁護士**。

## 2. DerivationRun テーブル（app-state DB・§9.21 全field・immutable）

導出の実行を **immutable な監査レコード**として残す（HIGH: 正本 §9.21 全 field 反映）:

| 列 | 型 | 内容 |
|---|---|---|
| id | BigInteger PK | |
| case_app_id / case_record_id | Text | 案件参照 |
| decedent_person_id | Text | 被相続人（App34 record id） |
| at_date | Text | 相続開始日（確定西暦） |
| **frozen_case_version** | Text | 導出時の案件/凍結表バージョン（再現性・§9.21） |
| **input_person_revisions** | JSONB | App34 各 record の `$revision` 集合（後で人物が変わったら検知） |
| input_person_ids | JSONB | 使った App34 record id 集合 |
| **input_hash** | Text | **正規化SHA-256。対象＝persons ＋ input_person_revisions ＋ kosekis ＋ declarations ＋ at_date ＋ engine_version ＋ frozen_case_version**（HIGH: 同一入力の厳密同定） |
| status | Text | derived / held / error |
| rank | Integer | 1/2/3/0 |
| **result_payload** | JSONB | **person_id のみ**の shares/heirs（§4・氏名非保持・facts最小化・schema allowlist） |
| **result_hash** | Text | result_payload の SHA-256（改ざん検知・run 比較） |
| **lawyer_flags** | JSONB | 要弁護士フラグ（flags を構造化・provisional 由来） |
| provisional | Boolean | フラグ有無 |
| **human_state** | Text | pending / confirmed / rejected（人の確定状態） |
| **decided_by** | Text | 確定した弁護士識別（人の操作者） |
| **supersedes_run_id** | BigInteger | この run が置き換える旧 run（再導出の連鎖） |
| engine_version | Text | heir_derivation バージョン（テスト版 v0.1 等） |
| created_at | DateTime(tz) | |

- **immutable（HIGH）**: DerivationRun は **UPDATE/DELETE を拒否**（追記のみ）。人の確定や
  再導出は**新 run を作り supersedes_run_id で連鎖**。`supersedes` 連鎖の検証（循環禁止・
  同一 case で active head は1つ）をテストで固定。
- migration は alembic 第4弾（P1-004基盤・手動DDL禁止）。
- **【裁定済み・2026-07-12 司令塔】** DerivationRun/TemplateVersion は **専用モジュールの
  別 metadata**（inbound_event.Base への相乗りはしない・L03 準拠）。app-state のモデル群を
  用途別モジュールに分け、alembic の target_metadata は各 Base を統合して autogenerate する。

## 3. App36 起票設計（R4-3b・封筒→関所・projection 保護更新）

### 3.1 封筒起票（機械）
- 雛形 = `person_merge._file_candidate` / `app30_filer.file_from_pending`（App30 に
  `発送ステータス:"要確認"` ＋ `チャネル固有データ:{"heir_derivation": detail}` ＋
  `create_record(APP_SHIPPING)`・**単票API必須**）。detail に DerivationRun.id を格納。
- 冪等キー = `heir_derivation:{case_record_id}:{input_hash}`（同一入力の再導出で二重封筒防止）。

### 3.2 関所（人の確定）
- `review_resolve.RESOLVERS`（review_resolve.py:371）にトップキー `"heir_derivation"` ＋ ハンドラ ＋
  env（`APP_SOUZOKUNIN`/`TOKEN_SOUZOKUNIN`）追加 → `resolve_group` 経由。
- ハンドラは既存 `_resolve_koseki` 型（phase1: 全件再読の二重確定ガード → phase2: App36 upsert →
  App30 を `完了`/`実行済み:yes` クローズ）。**確定時に DerivationRun.human_state=confirmed・
  decided_by を新 run で記録**。

### 3.3 App36 を current projection とする（HIGH: run 比較＋human_state 保護）
- **projection 更新は run の新旧比較に基づく**: 新 run の result と現 App36 を突合し、
  **差分のみ**更新（全消し再作成しない）。
- **human_state 保護**: App36 の人が触ったフィールド（特に `戸籍確認済=yes`・`状態` の手修正）は
  **機械の再導出で上書きしない**（人の確定を機械が壊さない）。上書き対象は機械由来フィールド
  （続柄/法定相続分/データ源）に限定。
- 冪等キー＝case_record_id＋person_id。registry_ingest._upsert_zaisan（:275）が既存の型。

### 3.4 「戸籍確認済=yes」遷移表（HIGH: 弁護士のみ・逆遷移禁止）
| from | to | 許可主体 | 記録 |
|---|---|---|---|
| no | yes | **弁護士のみ** | decided_by＋decided_at を記録 |
| yes | no | **禁止**（逆遷移不可） | — |
| （機械の再導出） | yes を維持 | 機械は yes を no に落とさない | 3.3 の保護と一致 |

### 3.5 result_payload の schema allowlist（HIGH: 氏名非保持）
- result_payload は **person_id のみ**（氏名・住所・生年月日を保持しない）。氏名等は App34/36
  参照で実行時解決。facts は導出根拠の最小限（条文キー等）に絞る。
- schema allowlist で「許可キー以外は保存拒否」（PII 混入を構造で防ぐ・RV10 §1.2 と整合）。

### 3.6 放棄（相続放棄）の写像（OPEN: 凍結表追補）
- declarations.renounced の人物を App36 `状態:"放棄済み"` に写像する**方針は明記**するが、
  放棄が順位繰上げに与える影響（次順位の相続人化）の写像網羅は凍結47ケースに**追補が要る**。
- 【OPEN・owner=大野（弁護士承認）】放棄→順位繰上げの写像を凍結表に追補。判断材料: 現行47ケースの
  放棄カバレッジ・弁護士レビュー。**仮決めしない**（承認まで放棄写像は confirmed にしない）。

### 3.7 新規に必要なもの（未存在）
1. `KintoneApp("App 36 (相続人)", "APP_SOUZOKUNIN", "TOKEN_SOUZOKUNIN")` 定義
2. `derive_heirs` 出力 → App36 fields 変換（純関数・schema allowlist 準拠）
3. RESOLVERS ハンドラ（heir_derivation）
4. env flag（`HEIR_DERIVATION_ENABLED`・既定OFF）
5. 起動経路: **【裁定済み・2026-07-12 司令塔】初版=指示Bot語彙「相続人を導出して」**。
   R4-1（人物確定）後続の自動導出は**運用安定後の追加候補**（初版では自動起動しない・
   人が明示的に導出を指示する）。

## 4. App37 割付の入力正本化（承認者・revision・snapshot）

- スキーマ（config:530-557）: 財産レコードID(App35)・相続人レコードID(App36)・取得区分(6種)・
  持分/代償金額/条件メモ・有効。財産×相続人の1行=1割付。
- App37 は**人が入力する正本**（機械は導出しない）。App36 起票済みが前提（相続人レコードID参照）。
- **追加すべきフィールド（HIGH）**: `承認者`（割付を承認した弁護士）・`revision`（割付の版）・
  `成果物生成時snapshot`（生成時点の割付内容を凍結＝後から割付が変わっても既発行成果物の
  再現性を担保）。※これらは実機 App37 への**フィールド追加が要る**＝BLOCKED（CU/kintone）。
- 実スキーマ突合: config は 2026-07-06 実機11フィールド一致確認済み。ただし上記3フィールドは
  **新規追加分**＝実機未整備。
- 【OPEN・owner=大野】App37 割付を指示Bot で入れるか kintone 直接入力か（事務所フロー）。

## 5. TemplateVersion registry（§9.23 全field・bytes再現・単一active）

### 5.1 テーブル案（§9.23 全 field）
| 列 | 型 | 内容 |
|---|---|---|
| id | BigInteger PK | |
| template_key | Text | 論理名（zaisan_mokuroku 等） |
| version | Text | セマンティック版 |
| **artifact_type** | Text | 成果物種別（財産目録 / 遺産分割協議書 / 遺言 等） |
| **unit_type** | Text | ユニット種別（時効援用 / 相続放棄 / 相続一般 / 補助金）。§8.15 のユニット別テンプレ非混在を列で強制（生成時に案件 unit と一致するテンプレのみ選択可） |
| **purpose** | Text | 適用範囲・用途（例「4社目以降用」「法テラス案件用」等の適用条件） |
| **file_ref** | Text | repo path or Drive fileId |
| **content_hash** | Text | テンプレ実体 SHA-256 |
| **content_bytes_ref** | Text | **バイト再現の保存先**（生成物の bytes 再現 contract 用・§5.2） |
| placeholders | JSONB | 差込プレースホルダ集合 |
| **mapping_version** | Text | データ→差込フィールドの写像ルール版（生成 rule。bytes 再現の対象・§5.2） |
| **clause_library_version** | Text | 条項ライブラリ版（協議書等の条項雛形。生成 rule。bytes 再現の対象・§5.2） |
| **created_by / approved_by** | Text | 登録者・承認者 |
| status | Text | draft / active / retired |
| **activated_at** | DateTime(tz) | active 化時刻。**＝正本 §9.23 の effective_from 相当**（この版が有効になった発効時刻） |
| **approved_at** | DateTime(tz) | 承認時刻（approved_by と対） |
| retired_at | DateTime(tz) | retired 化時刻 |

### 5.2 bytes 再現 contract（HIGH）
- 「同じ template_version ＋ **同じ mapping_version ＋ 同じ clause_library_version** ＋
  同じ差込データ → 同じ出力 bytes」を contract とする。テンプレ実体を content_hash＋
  content_bytes_ref で固定し、生成 rule（mapping・条項ライブラリの版）と生成器のバージョンも
  記録する。**再現テスト**（golden bytes）で固定（フォント埋め込み・タイムスタンプ等の
  非決定要素を排除する方式を含む）。

### 5.3 単一 active 制約（HIGH: 実装方式）
- 1 template_key につき active は1版のみ。**部分ユニーク制約**（`UNIQUE(template_key) WHERE
  status='active'`）で DB レベル強制、または active 化を「旧 active を retired にする条件付き
  遷移＋トランザクション」で保証。叩き台推奨: 部分ユニークインデックス（PostgreSQL 対応）。

### 5.4 Phase 5（成果物生成）接続点
- 生成器は active TemplateVersion を引く → docx_builder 差込 → Drive/App添付。
- **前提ゲート**: App36（相続人確定）＋App37（割付確定・承認者あり）＋App35（財産）＋
  「戸籍確認済=yes（弁護士）」＋DerivationRun.human_state=confirmed。
- DerivationRun.provisional=True なら生成拒否。生成時は App37 snapshot を凍結（§4）。

## 6. OPEN・BLOCKED
- 【OPEN・owner=大野（弁護士承認）】放棄→順位繰上げの凍結表追補（§3.6）。
- 【OPEN・owner=大野】App37 割付の入力運用（§4）。
- 【裁定済み・2026-07-12】metadata 分離＝専用モジュールの別 metadata（§2）／
  App36 起動経路＝初版は指示Bot語彙（§3.7）。
- BLOCKED_NEEDS_HUMAN:
  - App36/App37 env（`APP_SOUZOKUNIN`/`TOKEN_SOUZOKUNIN`/`APP_WARITSUKE`/`TOKEN_WARITSUKE`）
    本番投入・実番号確定・**App36トークン権限**（KINTONE_TOKEN_MATRIX と同列）。
  - App37 への新規フィールド（承認者/revision/snapshot）の実機追加（CU/kintone）。
  - TemplateVersion に載せる P1〜P4 テンプレ実物の正本確定＋bytes 再現の非決定要素排除（Drive）。
