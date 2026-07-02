# 発送/受領ハブ 統合アーキテクチャ設計書

大野法律事務所 業務システム（jikou-line-bot リポジトリ）に追加する5モジュールを、
個別のツギハギではなく一つの「**発送/受領ハブ**」として統合するための設計書一式。

- 作成日: 2026-07-02
- 前提コードベース: PR #4（耐障害化3点セット）マージ後の main
- **本設計書は設計のみ。実装コードは含まない。**

## ファイル構成と読む順序

| # | ファイル | 内容 |
|---|---|---|
| 1 | [01-overview.md](01-overview.md) | 全体アーキテクチャ図・設計思想・責務分担・制約の適用方法 |
| 2 | [02-kintone-design.md](02-kintone-design.md) | 新設 kintone アプリのフィールド定義、既存 App 21/28/29 との関係 |
| 3 | [03-common-components.md](03-common-components.md) | 共通ライブラリ `hub/` の構成（宛名ラベル・docx・承認・警報 等） |
| 4 | [04-module-01-shokumu-seikyu.md](04-module-01-shokumu-seikyu.md) | モジュール1: 職務上請求 |
| 5 | [05-module-02-enaishomei-csv.md](05-module-02-enaishomei-csv.md) | モジュール2: e内容証明 差込差出しCSV |
| 6 | [06-module-03-fax.md](06-module-03-fax.md) | モジュール3: FAX自動送信 |
| 6a | [06a-fax-provider.md](06a-fax-provider.md) | FAXプロバイダ選定（T6-1 実施済み・推奨: InterFAX） |
| 7 | [07-module-04-soufu-annai.md](07-module-04-soufu-annai.md) | モジュール4: 送付案内システム |
| 8 | [08-module-05-scan-pipeline.md](08-module-05-scan-pipeline.md) | モジュール5: スキャン→OCR→kintone 受領パイプライン |
| 9 | [09-implementation-plan.md](09-implementation-plan.md) | 依存関係・推奨実装順序・**Sonnet 発注用タスク分解（完了条件付き）** |

## Sonnet への発注方法

1. [09-implementation-plan.md](09-implementation-plan.md) のタスク表から、依存関係を満たした
   未着手タスクを1つ選ぶ（1タスク＝1セッションで完結する粒度に分割済み）
2. タスク定義（目的・作業内容・完了条件）と、タスクに記載された「参照設計書」の
   該当ファイルをプロンプトに添付して発注する
3. 完了条件には必ずテスト（既存の `test_cloudsign_webhook.py` /
   `test_triage_classification.py` と同じ unittest 流儀）を含めてある。
   全テスト PASS を確認してから次のタスクへ進む

## 全モジュール共通の設計原則（詳細は 01-overview.md）

1. **平常時は人間の作業ゼロ、異常時のみ LINE 警報**（耐障害化3点セットと同じ思想）
2. **弁護士名義の対外発信（内容証明・FAX・郵送）は必ず承認ステップを挟む**
   — ハブの状態機械上、承認済を経ずに発信系チャネルへ到達する経路は存在しない
3. **チャネルはアダプタ、案件はユニット** — 将来の相続放棄・相続一般・補助金ユニットは
   マスタ行とテンプレートの追加だけでハブに乗る（ハブ本体のコード変更なし）
