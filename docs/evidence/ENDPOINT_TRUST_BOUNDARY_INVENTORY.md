# エンドポイント信頼境界インベントリ（G1証拠・P1-006）

- 対象SHA: `588d416`（FastAPI 全 route の静的棚卸し）
- 認証方式の実体は各 file:行のソースで確認済み。RV番号は docs/work-logs の
  2026-07-11 Phase 0A 現況固定の仮番号に対応。

## エンドポイント一覧（全15 route）

| # | path | method | 認証方式 | 呼出し元 | 外部到達性 | 最悪効果 | RV |
|---|---|---|---|---|---|---|---|
| 1 | `/health` | GET | **なし** | 監視・人 | 公開 | read（依存ライブラリの死活のみ・機密なし） | — |
| 2 | `/webhook` | POST | LINE署名(HMAC-SHA256) | LINE(顧客Bot) | 公開 | write(App21/28/29)＋顧客LINE送信＋Claude課金 | RV-04(署名OK/idempotency OPEN) |
| 3 | `/webhook/kintone/approval` | POST | query token(`KINTONE_WEBHOOK_TOKEN`) | kintone webhook | 公開 | write(App29)＋顧客LINE送信 | RV-03 |
| 4 | `/webhook/dispatch-bot` | POST | LINE署名＋ホワイトリスト | LINE(指示Bot) | 公開 | write(App30/34ほか)＋業務LINE。**削除経路の起点**(person_merge) | RV-04 |
| 5 | `/ocr/fixed-asset` | POST | **なし** | 事務所PC watcher(repo外) | 公開 | write(App25/35)＋業務LINE＋Vision/Claude課金 | **RV-02(無認証)** |
| 6 | `/scan` | POST | **なし** | GAS(3フォルダループ) | 公開 | write(相談カード/戸籍/通帳)＋Claude課金 | **RV-01(無認証)** |
| 7 | `/webhook/stripe` | POST | Stripe署名(construct_event) | Stripe | 公開 | write(App21・決済/受任)＋journal | RV-05(署名OK・journalでdedup済) |
| 8 | `/koseki/ingest` | POST | query token(`KOSEKI_INGEST_TOKEN`) | GAS(戸籍読解フォルダ) | 公開 | write(App33/30)＋Vision/Claude課金 | RV-03 |
| 9 | `/registry/ingest` | POST | query token(`REGISTRY_INGEST_TOKEN`) | GAS(登記フォルダ) | 公開 | write(App25/35/30)＋課金 | RV-03 |
| 10 | `/valuation/ingest` | POST | query token(`VALUATION_INGEST_TOKEN`) | GAS/回送 | 公開 | write(App25/35/30)＋課金 | RV-03(token未投入=未稼働) |
| 11 | `/bank/ingest` | POST | query token(`BANK_INGEST_TOKEN`) | GAS/回送 | 公開 | write(App35/30)＋課金 | RV-03(token未投入=未稼働) |
| 12 | `/sortation/ingest` | POST | query token(`SORTATION_INGEST_TOKEN`) | GAS(未整理フォルダ) | 公開 | write(App38)＋業務LINE＋in-process回送 | RV-03 |
| 13 | `/cloudsign/webhook/{secret}` | POST | path secret(`CLOUDSIGN_WEBHOOK_SECRET`)＋API照合 | CloudSign | 公開 | write(App21受任)＋業務LINE。fail-closed済(P0B) | RV-06 |
| 14 | `/document/{secret}` | POST | path secret(`DOCUMENT_WEBHOOK_SECRET`) | kintone webhook | 公開 | write(App26)＋docx生成 | RV-03類 |
| 15 | `/hub/dispatch` | POST | query token(`HUB_WEBHOOK_TOKEN`) | kintone webhook | 公開 | write(App30・状態機械)＋業務LINE | RV-03 |

補足:
- 全 route が Railway の公開URLに露出（内部専用ネットワークに隔離された route は無い）。
- token 方式（#3,8,9,10,11,12,14,15）は共通ヘルパ `hub/webhook_auth.verify_token`
  （env未設定=deny-all・`hmac.compare_digest`・不一致404）。ただし **token が URL
  query/path に載る**ため、アクセスログ・リファラ・プロキシで漏れうる（RV-04でHMAC化予定）。
- #5,#6 は認証ゼロ。#6を止めるとGAS3フォルダループ全体が道連れ停止（P0B-001）。

## §8.10 の4信頼境界への分類案

**境界A: Public ingress（署名検証あり・第三者が正規に叩く）**
- #2 /webhook（LINE署名）・#4 /webhook/dispatch-bot（LINE署名+WL）・
  #7 /webhook/stripe（Stripe署名）・#13 /cloudsign（合言葉+API照合）
- 方針: 署名/照合が第一防壁。idempotency（RV-05/07）と redelivery 対策が主課題。

**境界B: Legacy caller（自前の共有secretで叩く社内呼出し）**
- #3 approval・#8 koseki・#9 registry・#10 valuation・#11 bank・
  #12 sortation・#14 document・#15 hub/dispatch（いずれも query/path token）
- 呼出し元: GAS（正本はGASエディタ側）・kintone webhook。
- 方針: RV-04でheader HMAC＋timestampへ移行。GAS/kintone webhook設定の同時切替が必要
  （呼出し元8種・切替はBLOCKED_NEEDS_HUMAN）。

**境界C: Unauthenticated（認証なし・最優先で塞ぐ）**
- #5 /ocr/fixed-asset・#6 /scan
- 方針: GAS堅牢化→認証新設の順（サーバ先行停止はGAS道連れ）。watcherはrepo外。

**境界D: Internal/Worker（プロセス内・HTTP非経由）**
- sortation→koseki/registry/valuation の回送は in-process 関数呼び出し（HTTP非経由）。
  scheduler（返送期限・死活監視）も in-process asyncio。
- 方針: 認証境界の対象外だが、durable化（RV-05/13）の対象。

## 最悪効果の観点での優先度

1. **#6 /scan・#5 /ocr（境界C・無認証）**: 誰でも write＋LLM課金を起動可能。
2. **#4 dispatch-bot（境界A）**: 唯一 App34 削除経路の入口（署名+WLで守られるが影響大）。
3. **境界B の query token 群**: token がURLに載る構造的弱点（RV-04）。

## BLOCKED_NEEDS_HUMAN
- 各 route の本番での実トラフィック有無（Railwayアクセスログ）
- GAS/watcher/kintone webhook の登録URL実体（呼出し元の確定）
