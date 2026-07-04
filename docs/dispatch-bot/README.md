# LINE指示Bot（dispatch入口）＋実行キュー 設計書

オーナー（弁護士）がスマホのLINEから自然言語で業務指示を出せる入口の設計。
設計のみ・実装はD系列タスク（07）で行う。

> 名称注意: 本設計の「dispatch-bot」は既存 `hub/dispatch.py`（App 30 チャネル
> ディスパッチャ）とは別物。実装モジュールは `dispatch_bot/` パッケージ。

## ファイル構成と読む順序

| # | ファイル | 内容 |
|---|---|---|
| 1 | [01-overview.md](01-overview.md) | 全体フロー・確定済み設計判断1〜12・二台帳制・LINE OKとkintone承認の違い・既存エンドポイント一覧と衝突確認 |
| 2 | [02-line-entry-and-auth.md](02-line-entry-and-auth.md) | 専用LINE公式アカウント・ホワイトリスト・署名検証・沈黙拒否＋警報・顧客Botとの分離 |
| 3 | [03-natural-language-parser.md](03-natural-language-parser.md) | claude_gateway経由の構造化解析（tool use）・案件検索・同姓同名・聞き返し |
| 4 | [04-execution-queue.md](04-execution-queue.md) | 実行キュー（EXEC_QUEUE・**App番号は作成時確定・env APP_EXEC_QUEUE**）のフィールド・状態機械 |
| 5 | [05-task-registry.md](05-task-registry.md) | タスクレジストリ（登録のみで種別追加）・初期セットの自動範囲/承認範囲・台帳2-5接続点 |
| 6 | [06-confirmation-and-safety.md](06-confirmation-and-safety.md) | リスク比例の復唱・pending_command_id・対外送信禁止リスト・今日の要対応一覧 |
| 7 | [07-implementation-tasks.md](07-implementation-tasks.md) | D系列タスク分解（第1弾〜第4弾・T/H系列依存） |

## 3行サマリ

1. 専用LINEアカウント→署名検証・ホワイトリスト→Claude解析→復唱→**OKで起票のみ**
2. 二台帳制: 発送・受領は App 30（既存状態機械に合流）／それ以外の内部タスクは実行キュー
3. **LINE上のOKは対外実行の承認ではない**。CloudSign送信・FAX・内容証明・課金・顧客送信は必ずkintone承認
