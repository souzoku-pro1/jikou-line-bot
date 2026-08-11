# 作業記録 2026-08-11: ブロックA点火（INBOUND_EVENT_DURABLE_ENABLED・§8.1 P0 全通過）

- TASK_ID: BLOCK-A-PRECHECK（事前検査）＋点火（[人]）＋スモーク検分＋BLOCK-A-WORKLOG（本記録）
- 実施: [人]=大野（migration 適用・flag 投入・スモーク発話/投入）／PC-A（read-only 検査・事後 SELECT・本記録）
- 正本 runbook: `docs/design-drafts/DRAFT_P2_DURABLE_IGNITION.md` §8（fix3 M01 の記録規律に従い、
  baseline 実出力と (c)(d) 事後 SELECT 実出力を本 log へ並べて保存する）
- 事前調査: `2026-08-09_daily-consolidated.md` §2（8/9 時点の中止条件=migration 2本未適用は本日解消）

**時刻注記（重要）**: DB の `received_at` 等・Railway アプリログはすべて **UTC**。
JST は **+9h**（例: `12:57 UTC` = `21:57 JST`）。git の committer date は JST(+09:00)。

## 0. 手順0: migration 2本適用（[人]・`!`・DATABASE_PUBLIC_URL 経由）

- 1回目 `railway run alembic upgrade head` は **internal host 不達で失敗**
  （`failed to resolve host 'postgres.railway.internal'`・PC-A からは既知の罠）。
- 2回目・確立済みの PUBLIC 経由で成功:

```
INFO  [alembic.runtime.migration] Running upgrade c4f1a2b7d8e9 -> d5e2b8a1c7f3, P3-001: derivation_run + heir_confirmation_decision（App36 導出台帳・NH01 交差）
INFO  [alembic.runtime.migration] Running upgrade d5e2b8a1c7f3 -> e7a3c9d2b5f1, P3-002: template_version（TemplateVersion registry・§9.23 全 field・単一 active）
```

- 突合（`alembic current`）: **`e7a3c9d2b5f1 (head)`** — code heads と単一一致。
- 8/9 事前調査の点火中止条件（§2.1(a)・未適用2本）は**本工程で解消**。

## 1. §8.1(a) 点火前 read-only 機械検査（PC-A・SELECT のみ・全OK）

Windows 対処: 検査2・4（psycopg async）のみ `WindowsSelectorEventLoopPolicy` 指定を
先頭に付加（8/9 と同一手法・SELECT 内容は runbook と逐語同一）。コンソール cp932 のため
script 内日本語の画面表示は一部文字化け（判定は exit code と OK/NG 行で機械確認）。

### 検査1: alembic 単一一致（H03）— OK
```
heads(code) = ['e7a3c9d2b5f1']
current(db) = ['e7a3c9d2b5f1']
OK: alembic current == heads（単一）
```

### 検査2: 必須3表の機械照合（H01/H02・fix3 H01 の FK/index 完全照合込み）— OK
```
indexes: ['inbound_event_pkey', 'ingestion_receipt_pkey', 'ix_inbound_event_provider_state',
 'ix_inbound_event_received_at', 'ix_ingestion_receipt_last_outcome',
 'processing_attempt_pkey', 'uq_inbound_event_dedup_key', 'uq_ingestion_receipt_idem',
 'uq_processing_attempt_receipt_epoch']
OK: 必須 3 table の table/column/constraint 照合一致
```
（列集合・UNIQUE 名前/列指定・FK `processing_attempt.receipt_id → ingestion_receipt.id
ON DELETE CASCADE`・index 3本＋UNIQUE index 3本: NG 0件。`signature_nonce` 存在・警告なし）

### 検査3: STRIPE_EVENT_JOURNAL_ENABLED — OK
```
STRIPE_EVENT_JOURNAL_ENABLED = 1
```

### 検査4: baseline 当日値（点火前・2026-08-11 採取）
```
inbound_event baseline:      （0行 = 全 provider/state で 0 件）
ingestion_receipt baseline:  （0行 = 全 last_outcome で 0 件）
```
**両表 0 件**＝点火後差分の基準は「完全に空」。

## 2. 点火（[人]）

- env 投入: **`INBOUND_EVENT_DURABLE_ENABLED=1`**（flag 名のみ記録・`railway variables --set`・
  投入により Railway が自動再デプロイ）。

## 3. §8.1(b) deploy 直後ゲート（全OK）

- 再デプロイ: **● Online**・deployment ID **`64bc3ce9…` → `39ce2122-bb7f-45a0-820b-75b7f672a8d5`** へ世代交代。
- `[RV05] startup reconcile` **成功ログ実見**（「skipped」なし＝flag 有効・DB 到達）:
```
INFO:     Waiting for application startup.
2026-08-11 12:51:59,478 INFO main [RV05] startup reconcile: to_pending_retry=0 to_unknown=0
INFO:     Application startup complete.
```
（12:51 UTC = 21:51 JST。reconcile 対象 0 件は baseline 0 件と整合）
- 起動 traceback なし・`/health` **200 / status "ok"**（deps: python-docx / reportlab
  〔ipaexg.ttf〕/ graphviz すべて ok・WebFetch 実測）。

## 4. §8.1(c) LINE スモーク（最終 OK・経緯込み）

### 経緯: 初回発話は指示Bot宛＝0行NG→原因特定→顧客Botへ再発話
- 初回発話（12:54:46 UTC = 21:54 JST）は **`/webhook/dispatch-bot`（指示Bot）** に着信:
```
INFO:     100.64.0.3:39448 - "POST /webhook/dispatch-bot HTTP/1.1" 200 OK
2026-08-11 12:54:46,162 INFO dispatch_bot.router [DISPATCHBOT] message userId=U179359d73... text=（freetext・非表示）
```
- durable 記録（RV-05）の対象は**顧客Bot の `/webhook` のみ**（指示Bot は対象外）のため
  事後 SELECT は **0 行 = NG**。ただし**点火自体の異常ではない**（`/webhook` 未着信で
  0 行はコード上正しい・flag 有効は (b) の reconcile 成功で証跡済み）。誤発話の
  切り分けが即座にできたのは着信 path のログ実測による。
- **顧客Bot へ再発話**（12:57:59 UTC = 21:57 JST）→ 下記 OK。

### 事後 SELECT（read-only・期待形一致）
```
inbound_event now:
  ('line', 'done', 1, 2026-08-11 12:57:59 UTC)
line rows (baseline=0 → 全行が増分):
  (id=1, state='done', attempts=1, claimed=true, received_at=12:57:59 UTC, processed_at=None)
COUNT = 1
JUDGE = OK: 1 row / done / attempts=1 / claimed=true
```
- **増分ちょうど1行・done・attempts=1・claimed=true**（二重記録なし）。
- `processed_at=None` は実装どおり（LINE lane の done 遷移 `mark_line_completed`
  〔hub/durable_inbound.py〕は state のみ更新。processed_at を書くのは stripe/kintone
  lane と failed_exhausted のみ。(c) のゲート条件にも processed_at は含まれない）。

## 5. §8.1(d) sortation スモーク（OK）

### 事後 SELECT（read-only・期待形一致）
```
ingestion_receipt（baseline=0 → 全行が増分）:
  (id=1, last_outcome='completed', epoch=4, first_seen=13:06:02 UTC, source_file_id あり, ingest_type='sortation')

processing_attempt:
  (receipt_id=1, epoch=1, phase='processing', 13:06:02 UTC)
  (receipt_id=1, epoch=2, phase='vendor_pre',  13:06:07 UTC)
  (receipt_id=1, epoch=3, phase='sending',     13:06:19 UTC)
  (receipt_id=1, epoch=4, phase='completed',   13:06:20 UTC)

COUNT receipts = 1 / attempts = 4
(receipt_id, epoch) duplicate = False
JUDGE = OK
```
- **receipt 1行・completed（正常終端）・epoch=4**・attempt 遷移4段が完全記録・
  `(receipt_id, epoch)` 重複なし（`uq_processing_attempt_receipt_epoch` 違反なし）。
  投入 13:06 UTC = 22:06 JST。所要約18秒で終端。
- Drive 側の `[照会中]`/`[済]` リネーム・LINE 通知は[人]の目視確認（runbook の確認点）。

## 6. 判定サマリ

| ゲート | 結果 |
|---|---|
| 手順0: migration 2本適用 | OK（current=heads=`e7a3c9d2b5f1` 単一一致） |
| §8.1(a) 事前検査4項目 | 全OK（baseline 両表0件） |
| §8.1(b) deploy 直後 | OK（[RV05] reconcile 成功・traceback なし・/health 200） |
| §8.1(c) LINE スモーク | OK（1行・done・attempts=1・claimed=true） |
| §8.1(d) sortation スモーク | OK（receipt 1行・completed・遷移4段・重複なし） |

**→ ブロックA点火は §8.1 P0 全通過で完了。**

## 7. 残観測（明朝以降）

- **§8.2 P1（点火後 24h）**: 監視項目G（LINE received/processing 滞留・既定 1h 閾値）の
  **警報 0**・日次死活監視の通知に新規異常なし・LINE 応答の苦情/異常なし・
  `[RV05]` 系ログにエラーなし・5xx 増なし（sortation H-04 の 5xx は自然リトライ回収を確認）。
- **H11a 監査（監視項目I）の初実働検分（明朝の daily_healthcheck・3点）**:
  App36 env は投入済み（8/9 §2.2）＋本日の P3-001 migration 適用で
  「テーブル不在スキップ」も解消＝**追加点火なしで次回実行から実働**。
  (i) 「App36 decision監査の実行自体が失敗: 〜」警報が**出ない**こと
  (ii) Railway ログに `App36 decision audit OK` が出ること
  (iii) 実機 App36 に対する `$id` カーソル query の初実測が通ること
  （App36 は 0 件想定＝期待値は警報なし）。
