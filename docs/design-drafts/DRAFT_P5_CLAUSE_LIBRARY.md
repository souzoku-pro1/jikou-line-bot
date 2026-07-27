# DRAFT: Phase 5（成果物生成）準備調査 — 条項ライブラリと差込み設計

- TASK_ID: P2-BATCH-06 / TASK 16（READ_ONLY 調査＋DRAFT 起票・PC-A）／記録日 2026-07-27
- 調査 BASE: origin/main `d87b3d6`（読取のみ）＋ **TemplateVersion はレビュー中
  `origin/feature/p3-002-template-version`（`644666d`）の読取参照のみ**（変更なし）
- 正本: DRAFT_APP36 §5（§9.23 全 field・§5.2 bytes 再現 contract・§5.4 Phase 5 接続点）
- **本書は設計 DRAFT であり実装しない**。書式現物（大野の Word）未着のため、
  第1例（遺産分割協議書）の具体 placeholder は型の提示に留める。

## 0. 前提となる既存資産（実物確認）

- **TemplateVersion**（p3-002・読取参照）: `clause_library_version`（NOT NULL・frozen）・
  `placeholders`（JSON list・frozen）・`mapping_version`・`generator_version`・
  `content_hash`＋`content_bytes_ref`（テンプレ実体固定）。lifecycle は
  draft→active→retired（単一 active・承認ゲート）。
- **docx 生成基盤**（hub/docx_builder.py・稼働中）: `fill_template`（`{{key}}` 置換・
  run 分割対応）・`fill_template_multiline`・**`fill_table_rows`（`{{行:` marker の
  表行繰返し）**・`resolve_template`（`docx_templates/<unit>/<種別>.docx` 規約）・
  `validate_template`（placeholder 欠落の healthcheck 検知）。
- python-docx は requirements.txt 導入済み（送付案内・職務上請求で bytes 生成実績）。
- §5.4 前提ゲート（生成器の入口条件・逐語）: App36＋App37＋App35＋「戸籍確認済=yes」＋
  human_state=confirmed・**provisional=True は生成拒否**。

## 1. 条項ライブラリのデータ構造案

### 1.1 保管形式の選択（比較）

| 案 | 版管理 | [人]レビュー性 | bytes 再現との整合 |
|---|---|---|---|
| (a) repo 内 versioned ファイル（`clauses/<version>/*.yaml` or 単一 JSON） | git＝そのまま監査可能 | **diff で条項本文レビュー可（推奨）** | `clause_library_version`＝ディレクトリ名/ファイル hash で固定 |
| (b) app-state DB テーブル（TemplateVersion と同 metadata） | immutable 行＋版列 | kintone/DB 閲覧が要る | 可（ただし migration・AST 検査対象の拡張が必要） |

- **推奨 = (a)**。条項本文は「弁護士が読む法律文書の部品」であり、git diff による
  [人]レビューが本質。DB 化は「条項の動的編集 UI」が要件化した時点で再検討
  （Release A は read-only・当面不要）。`clause_library_version` には
  `clauses/v1`（ディレクトリ）＋内容 hash を記録し、TemplateVersion の frozen 列と
  同じ強度で再現性を担保する。

### 1.2 条項レコードの schema 案（1 条項 = 1 エントリ）

```yaml
clause_id: ibun_kyogi.acquisition_per_heir   # 文書種別.条項キー（ASCII・一意）
title: 相続人別の取得財産条項
applies:                                     # 適用条件（AND。語彙は P3-001 enum を再利用）
  relation_keys_any: [spouse, child]         # 相続人構成（result_payload.relation_key 語彙）
  rank_in: [1]                               # DerivationRun.rank
  flags_none: [successive_inheritance]       # lawyer_flags（この flag があれば不適用）
  requires_human: false                      # true=適用可否そのものを[人]選択（自動判定しない）
body_template: |                             # 本文（placeholder は §2 の3種別のみ）
  相続人{{氏名}}は、次の財産を取得する。
  {{行:財産目録}}
repeat: per_heir                             # none / per_heir（相続人ごと繰返し）
order: 30                                    # 文書内の並び順
since_version: v1                            # 導入版（監査用）
notes: 大野レビュー時のメモ（生成物には出さない）
```

- **適用条件の語彙は P3-001 の保存語彙（relation_key／lawyer_flags の ASCII enum）を
  単一の正として再利用**する（新しい自由文字列語彙を作らない）。導出結果
  （DerivationRun.result_payload＋HCD confirmed）だけから適用可否が機械決定できる
  条項と、`requires_human: true`（[人]が App37 割付等で選ぶ）条項を構造で分離する。
- 検証器（実装票）: enum 外の語彙・placeholder 未定義・order 重複を CI で拒否
  （validate_template と同じ「人が編集して壊す事故」の検知思想）。

## 2. 差込み設計の型 — 遺産分割協議書を第1例に

placeholder 種別は **3 種のみ**に固定する（種別を増やさないことが再現性と
レビュー性の担保）:

| 種別 | 記法（既存資産との対応） | 例 |
|---|---|---|
| (i) 単純差込み | `{{key}}`（fill_template 既存） | `{{被相続人氏名}}`・`{{相続開始日和暦}}`（to_wareki） |
| (ii) 繰返し（相続人ごと） | 条項 `repeat: per_heir` ＋表内は `{{行:...}}`（fill_table_rows 既存） | 相続人ごとの取得条項・署名欄・財産目録行 |
| (iii) 条件付き条項 | **docx 内に条件記法を持たない**。条項単位の include/exclude を §1.2 `applies` が生成器側で決定し、採用条項列を順に組み立てる | 代襲がある場合の特記・数次相続の経緯条項 |

- 設計原則: **条件分岐をテンプレート（docx）に埋めない**。docx は静的な条項部品の
  集合で、選択ロジックは条項ライブラリ（データ）＋生成器（コード）に置く。
  これにより (a) 大野の書式 Word を「条項に切る」だけで移行できる (b) 条件の変更が
  YAML diff でレビューできる (c) bytes 再現の検証対象が「条項列＋差込みデータ」に
  閉じる。
- 氏名等の PII は**生成時に App34/36 から実行時解決**（DerivationRun には person_id
  しか無い＝P3-001 の氏名非保持と整合。生成器は confirmed projection を読む）。

## 3. docx 生成の技術選択肢（比較）

| 案 | ループ/条件 | 依存追加 | bytes 再現 | 判定 |
|---|---|---|---|---|
| (a) python-docx＋docx_builder 拡張 | (ii) は fill_table_rows 実績あり。段落単位の条項組立ては新設（条項=段落列の追記） | なし（導入済み） | 生成器を自前で握るため非決定要素を制御しやすい | **推奨** |
| (b) docxtpl（jinja2） | 強い（for/if を docx 内に書ける） | 追加 | jinja2 の docx 内ロジックが §2 の「条件をテンプレに埋めない」原則と衝突 | 不採用推奨 |
| (c) 直接 XML 組立て | 自由 | なし | 完全制御 | 過剰（書式再現の工数大） |

- **bytes 再現契約（§5.2）との整合**（案 (a) の実装時要点・golden テストで固定）:
  - docx core properties（created/modified）を**固定値**に設定（python-docx で設定可）。
  - zip エントリの**順序・タイムスタンプの決定性は実測で要確認**（python-docx の
    save 実装依存・非決定なら正規化保存を挟む）。→ **実装票の受入条件**に
    「同一入力 2 回生成の bytes 一致」を含める。
  - `generator_version` には生成器モジュール版＋python-docx の pin 版（requirements.lock）
    を写像し、**lock 更新時は generator_version を必ず上げる**運用を明記。

## 4. [人]素材の受入仕様 — 大野の書式 Word 到着時の手順書（1本目で型を作る）

1. **受領・保管**: 原本 docx は repo に**そのまま置かない**（[人]の管理場所で保管）。
   repo に入るのは placeholder 化済みテンプレのみ（§5.2 の content_hash 対象）。
2. **機械解析（PC-A）**: python-docx で段落・表・run 構造を抽出し、
   (a) 固有値（氏名・日付・金額）候補 (b) 繰返し構造（相続人数に依存する段落/行）
   (c) 条件で入り切りしそうな段落、の 3 分類の**候補一覧表**を作る（本文は
   構造レビューに必要な範囲のみ・成果物は work-log でなく票の添付とし PII 規律は
   書式のサンプル値の扱いを[人]に確認してから）。
3. **placeholder 化案の提示**: 変換前後の対照表（原文断片→`{{key}}`／条項分割案・
   §1.2 の clause_id 案つき）を[人]レビューに出す。
4. **[人]レビュー・承認**: placeholder 命名・条項の切り方・適用条件の初期値を大野が
   確定（法律文書としての条項単位の妥当性は弁護士判断）。
5. **登録**: テンプレ docx を docx_templates 規約に配置・validate_template 登録・
   golden fixture（合成データでの期待 bytes）作成。TemplateVersion への登録
   （draft 起票→[人]activate）は P3-002 merge 後の実装票で。
- この手順書自体が「1本目の型」であり、2 本目以降（相続関係説明図・財産目録等）は
  同じ 5 段で回す。

## 5. 実装票スコープ案

| 票案 | スコープ | 依存 |
|---|---|---|
| P5-001: 条項ライブラリ器 | §1.2 schema・loader・検証器（enum 整合/placeholder 定義/order）・合成条項でのテスト | P3-001 merge（enum 語彙の import）。書式現物 **不要** |
| P5-002: 遺産分割協議書テンプレ第1号 | §4 手順 1-5 の実施（解析→placeholder 化→[人]レビュー→登録） | **書式現物到着**（[人]）＋P5-001 |
| P5-003: 生成器 | active TemplateVersion 取得→§5.4 前提ゲート検査→条項組立て→docx_builder 差込み→bytes 再現 golden（同一入力 2 回一致・core properties 固定） | P3-002 merge＋P5-001＋P5-002 |
| （後続） | Drive/App 添付・App37 snapshot 凍結結線（§4 正本）・2 本目以降のテンプレ | P5-003＋P3-003 系 |
- 先行着手可能なのは **P5-001 のみ**（合成データで完結）。P5-002 は書式現物、
  P5-003 は P3-002 merge がゲート。
