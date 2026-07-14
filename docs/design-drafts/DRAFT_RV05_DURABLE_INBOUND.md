# DRAFT: RV-05/RV-13 durable InboundEvent / IngestionReceipt 横展開

- 状態: **DRAFT（設計固定・実装は次票）** / 対象: RV-05（BackgroundTasks 再起動消失）・RV-13（sortation process-memory 重複防止・失敗の成功ACK飲み込み）・RCF-M10（顧客Bot返信の観測性）
- 正本: 製品設計master v2.4 **§9.17**（InboundEvent・IngestionReceipt）・**§9.17.1**（ConversationSession・PendingCommand＝本票 OUT_OF_SCOPE・余地確保のみ）・**§8.8**（durable worker 運用契約・原則のみ流用）／関連 G1・G3 受入条件
- 司令塔裁定（論点6件）を本 DRAFT に反映済み（各節に［裁定N］で明示）
- レビュー: R-RV-05-13-D（設計裁定）→ 実装票発行

> **master §9.17 引用（FIXED）**: 「public webhook、GAS、watcher から受けた request を、ACK 後の process memory へ預けない。InboundEvent 必須field= provider、external_event_id、caller_id、payload_ref／hash、received_at、signature_result、state、attempt_count。IngestionReceipt 必須field= source_file_id、source_sha256、ingest_type、case_hint、first_seen_at、last_outcome、downstream_refs、idempotency_key。external event ID または caller＋source ID＋hash へ unique 制約。payload に顧客 data がある場合は暗号化／不変参照と retention を定義し、通常 log へ複製しない。」
> **master §8.8 引用（FIXED・原則のみ流用）**: 「外部実行を request 内処理／BackgroundTasks／process-memory queue／browser request の寿命へ結び付けない。durable DB を読む継続 worker として起動し再開する。startup と定期で expired lease を回収。vendor call 開始前を証明できる job だけ再 queue、開始後/不明は UNKNOWN。claim は atomic update/lock と lease、concurrency 上限、同一 idempotency key を二 worker が実行しない。graceful shutdown は新規 lease 停止＋実行中 attempt の durable marker 確定。health は poll/成功/queue lag/expired lease/UNKNOWN を返す。」
> （注: §8.8 は Phase 6 の Outbox worker 契約。本票は **lease/atomic claim/startup reconciliation/graceful shutdown の原則だけ流用し、OutboxJob 自体は作らない**［裁定2］。）

---

## §1 スコープと既存 InboundEvent（P1-005a）との統合方針

**対象**: LINE webhook（顧客Bot `/webhook`・業務Bot 通知先）と **sortation 入口**（`/sortation/ingest`）の durable 化。
**OUT_OF_SCOPE**: ConversationSession/PendingCommand 完全 durable 化（RV-06・§9.17.1）・Stripe/CloudSign（RV-07/11）・dead-man 第二経路（RCF-M08）・PWA/auth。ただし**表設計で RV-06 の余地を塞がない**（§2.4）。

### 1.1 既存 InboundEvent 統合＝【同居（provider 列）を推奨】

既存 `inbound_event` 表（P1-005a・Stripe）は §9.17 の必須 field を**既に全て保持**する（`provider / external_event_id / caller_id / payload_hash / received_at / signature_result / state / attempts(=attempt_count) / dedup_key(unique) / processed_at / last_error / claimed_at`）。

| 選択肢 | 内容 | 長所 | 短所 | 裁定 |
|---|---|---|---|---|
| **A. 同居（推奨）** | 既存 `inbound_event` に `provider="line"` 行を追加。**ALTER 不要**（列は充足） | 既存 durable 基盤（claim/stale 回収/IntegrityError dedup）を再利用・migration ゼロ・DO_NOT_CHANGE「既存表 ALTER なし」に自然適合 | provider 別の ACK/reprocess セマンティクスを1表に同居させる（下記で分岐を明示） | **採用** |
| B. 別表 | `line_inbound_event` を新設 | provider 分離が明快 | 同一ロジックの二重管理・migration 増・§9.17 の「1 model」意図から乖離 | 不採用 |

**同居の要点**: 表は共有するが**状態機械の再送主体は provider 依存**。Stripe は「再送主体=Stripe」（D14: in_progress→503）だが、**LINE は自動再送主体が設定依存**（§3・K4）。したがって同居でも provider 別に「未処理救済の起動主体」を分けて設計する（§4 の startup 回収が LINE の主救済経路）。

## §2 schema（InboundEvent 拡張 / IngestionReceipt 新設・既存表 ALTER 回避）

### 2.1 InboundEvent（同居・**ALTER なし**）
既存列で LINE を収容:
- `provider = "line"`（業務Bot 通知は同 provider・`event_type` で区別可）
- `external_event_id = LINE webhookEventId`（［裁定4］）
- `dedup_key = "line:<webhookEventId>"`（`UNIQUE(dedup_key)` が delivery 重複の冪等実体）
- `caller_id`・`signature_result`（X-Line-Signature 検証結果）・`payload_hash`（生 body の sha256）・`state`・`attempts`・`claimed_at`
- **顧客 data 非複製**: payload 本文は列に入れない（§9.17「通常 log へ複製しない」・P1-005a D8 継承）。`payload_hash` と最小抽出（`event_type` 等）のみ。

### 2.2 IngestionReceipt（**新表**・file 系 ingest 専用・LINE には使わない［裁定4］）
§9.17 必須 field ＋ unique（[裁定1] = **最小形**）:
```
ingestion_receipt(
  id            PK,
  source_file_id   TEXT NOT NULL,   -- drive_file_id 等
  source_sha256    TEXT NOT NULL,   -- PDF 生バイト sha256
  ingest_type      TEXT NOT NULL,   -- "sortation"（将来 koseki/registry/...）
  caller_id        TEXT,            -- unique 構成要素
  case_hint        TEXT,
  first_seen_at    ts NOT NULL,
  last_outcome     TEXT NOT NULL,   -- 状態機械（§5.2）
  downstream_refs  TEXT,            -- 起票済み record_id 等（PII でない参照のみ）
  idempotency_key  TEXT NOT NULL,   -- = caller_id||":"||source_file_id||":"||source_sha256
  UNIQUE(idempotency_key)
)
```
- unique 制約は［裁定1］の **caller＋source_file_id＋sha256**（＝`idempotency_key` に畳んで単一 UNIQUE）。§9.17 の「caller＋source ID＋hash」に一致。
- `downstream_refs` は起票結果の **record_id 等の非 PII 参照のみ**（顧客氏名・本文は入れない）。

### 2.3 migration 方針（既存表 ALTER 回避）
- **InboundEvent は migration なし**（同居＝既存列で足りる）。
- **IngestionReceipt のみ新規 migration**（`down_revision = <現 head b7d3e1a9c2f4>`・`upgrade=create_table`/`downgrade=drop_table`）。適用は大野（RV-04a 方式・`DATABASE_PUBLIC_URL` 経由）。
- model は `hub/` 新モジュール（`hub/ingestion_receipt.py`）に `sa.Table`＋専用 `metadata`、`alembic/env.py` の `target_metadata` list へ統合（notify_heartbeat/service_auth と同方式）。

### 2.4 RV-06 の余地を塞がない
ConversationSession/PendingCommand（§9.17.1）は本票で作らないが、InboundEvent の `external_event_id`／`state`／将来の `last_event_id` 参照で会話 state 再構成の起点になり得るよう、**InboundEvent 側に session 固有列を足さない**（会話 state は RV-06 の別表に持たせる）。IngestionReceipt も file ingest 限定に閉じ、会話系と混ぜない。

## §3 ACK 契約（経路別・［裁定3/4］）

**共通原則（G1/G3）**: **durable commit 前に成功 ACK（200）を返さない**。event store 停止時は vendor retry 可能 response（5xx）または独立 durable ingress。**process-memory fallback は禁止**。

### 3.1 LINE webhook（顧客/業務Bot）［裁定3/4］
順序を固定:
```
1. 受信・X-Line-Signature 検証（既存）
2. events[] を 1 event = 1 InboundEvent 行として durable insert
   （external_event_id = webhookEventId・UNIQUE(dedup_key) で delivery 重複は冪等）
3. insert 完了後に 200 を返す
4. durable state（InboundEvent）から非同期 consumer が処理（返信生成・起票）
```
- **DB 停止時 = 5xx**（memory fallback 禁止・G3）。
- **［裁定3・K4］LINE redelivery は設定依存**。DRAFT は両ケースを規定:
  - **redelivery ON**: 5xx で LINE が再配送 → 次回受理で回復（lost 0）。
  - **redelivery OFF**: 5xx=当該 event 喪失を**許容**するが、**喪失を観測可能にする**（受付前に落ちた event は observability で「受理失敗」として計上・§6）。→ K4 で[人]確認。
- 1 webhook に複数 event → 各 event 独立行（1つの insert 失敗で全体 5xx＝LINE 再配送に委ねる。ON 前提なら安全・OFF なら §6 で可視化）。

### 3.2 sortation `/sortation/ingest`（RV-13）
- process-memory の `_seen_drive_file_ids` 重複防止を **IngestionReceipt 冪等へ置換**。
- **ask task 保存失敗を成功 ACK にしない**（RV-13）: forward/ask の downstream 失敗は `last_outcome = PENDING_RETRY` 等で可視化し、成功 response で飲み込まない。
- receipt durable insert（UNIQUE 衝突=重複＝一回処理）→ 判定処理 → outcome 確定。DB 停止時は 5xx（GAS 側 retry 可能）。

## §4 consumer / lease / startup 回収 / graceful shutdown（§8.8 原則の最小適用・［裁定2］）

**専用 worker process は作らない**。アプリ内**非同期 consumer**（asyncio task・startup 起動）で InboundEvent/IngestionReceipt の未処理行を処理する。§8.8 の原則のみ流用:

- **atomic claim + lease**: 条件付き UPDATE + RETURNING で1行を1 consumer が取得（既存 InboundEvent の `claimed_at` + stale 再claim パターンを踏襲・P1-005b）。同一 `dedup_key`/`idempotency_key` を二重実行しない。
- **startup reconciliation**: 起動時に `state ∈ {received, processing}` かつ lease 期限切れ（`claimed_at` NULL/古い）を再 queue。**vendor call 開始前を証明できる行だけ再実行**（開始後/不明は §8.8 の UNKNOWN 相当＝`state=needs_review` 等で人手可視化に送る・二重返信を避ける）。
  - LINE は自動再送主体が弱い（K4 OFF 時）ため、**この startup 回収が LINE の主救済経路**（HOTFIX-01 型の沈黙障害を「受理済み未処理」から救う）。
- **concurrency 上限**: 設定値（env）で consumer 並列度を上限化。
- **graceful shutdown**: FastAPI shutdown で新規 claim を止め、実行中 attempt の durable marker（`processing`/`completed`/`failed`）を確定してから終了。強制終了後も startup reconciliation で回復。
- **注**: OutboxJob（送信側 durable queue）は本票で作らない（Phase 6）。ここは**受信側の durable 化と受理済み event の回収**に限定。

## §5 冪等・replay・attempt 上限

- **冪等（[裁定4]）**: LINE=`UNIQUE(dedup_key="line:<webhookEventId>")`／sortation=`UNIQUE(idempotency_key=caller:file_id:sha256)`。重複配送・重複投入は INSERT の UNIQUE 衝突で検知し**処理・返信・起票は一回**。
- **replay で遷移一回（G3）**: 同一 external event の再配送で state 遷移は1回だけ（既存行が `completed` なら skip・`failed` なら reprocess・§9.17.1 の「replay で遷移一回」を InboundEvent にも適用）。
- **attempt 上限**: `attempts`/`attempt_count` が上限超過で `state=failed`（可視化）。無限リトライしない。失敗は §6 で観測。

## §6 観測性（RCF-M10・PII 非混入・emit 契約）［裁定5］

**ログベース集計でよい**（dashboard 不要）。全カウンタ/ログは **emit 契約経由**（key_id/record_id/count 等の値域検証済み型のみ・**顧客情報/本文/payload は出さない**）。

- **InboundEvent state 遷移カウント**: `received / processing / completed / failed`。
- **返信結果 emit**: `reply_ok / reply_fail / exception`（顧客Bot 返信の成功/失敗/例外率＝RCF-M10）。
- **lifecycle ログ**: `event_id（=record_id 値域）/ 受理 / 処理開始 / 完了 / 失敗`。**顧客情報・本文は出さない**（HOTFIX-01 の「背景タスク全滅が沈黙」を、返信結果カウンタで可視化して構造的に防ぐ）。
- sortation: `last_outcome` 遷移（received/processing/completed/PENDING_RETRY/failed）を count。
- 既存 RV-10 redaction sink 方針に適合（emit ラップ＝AST 台帳の新規違反にしない設計を優先）。

## §7 feature flag・段階導入・rollback

- **flag `INBOUND_EVENT_DURABLE_ENABLED`（既定 OFF）**。
  - **OFF = 現行 BackgroundTasks 経路と完全同一**（byte 同一・handler smoke で固定）。durable insert/consumer は起動しない。
  - **ON** = §3 の ACK 順序＋§4 consumer 起動。
- **段階導入**: sortation（観測性先行・挙動変更小）→ LINE 業務Bot → LINE 顧客Bot の順を推奨（P1-005 survey の着手順に整合）。各段は flag 内の sub-gate（provider 別 env）で細分化可（実装票で確定）。
- **rollback = flag OFF**（env 切替のみ）。DB 停止時も flag OFF なら現行経路（ただし現行は event 消失リスクあり＝これが RV-05 の是正対象）。

## §8 テスト計画

- **unit/contract**: durable insert→200→consumer 処理の順序・冪等（重複配送 1回）・attempt 上限→failed。
- **negative（crash/replay/保存失敗）**:
  - **crash 回収**: InboundEvent insert 後・処理前に crash 相当（consumer 未処理で残す）→ startup reconciliation で1回だけ処理。
  - **replay**: 同一 webhookEventId 再配送 → 遷移一回。
  - **ask task 保存失敗**: sortation の downstream 失敗 → 成功 ACK にせず `PENDING_RETRY`。
- **regression**: 全 suite（現行 1,328+）維持・**顧客Bot handler smoke 必須**（`_process_line_event` 先頭ログ通過）。
- **flag OFF 完全同一**: 既存 LINE/sortation テスト＋handler smoke が flag OFF で不変。
- **レイテンシ［裁定6］**: durable insert は**1回のみ追加**。テストで webhook 応答の上限アサート（例: insert 経路がテスト DB で N ms 以内）を**実装票で要否判定**（本 DRAFT は計測方法を規定・LINE 要件超過の懸念が出たら設計持ち帰り＝STOP）。
- **修正前 FAIL 実測（現行が event 消失することの実証）**: flag OFF（現行 BackgroundTasks）で「insert 相当の前に 200 を返し、その後 crash 相当で処理が失われる」ことを再現するスクリプトを **work-log(.md) にスクリプト本文＋実出力全文で固定**（追跡 .py に print を置かない・RV-04b の規律）。独立再現可能な形で保存。

## §9 OPEN / [人]確認（K4）

- **K4（新規・[人]/大野）**: **LINE Developers コンソールの webhook 再配送(redelivery)設定の有無を確認**。ON なら 5xx で自動回復（lost 0）。OFF なら 5xx=当該 event 喪失を許容し §6 の観測性で補う（喪失を可視化）。この結果で §3.1 の運用前提を確定。
- **OPEN-1**: startup reconciliation の「vendor call 開始前を証明できる」判定粒度（LINE 返信は reply API 呼出前を `processing` 前で確定するか、専用 marker を持つか）。→ 実装票で確定（二重返信ゼロが要件）。
- **OPEN-2**: consumer concurrency 既定値と graceful shutdown の待機上限。→ 実装票。
- **OPEN-3**: 段階導入の sub-gate 粒度（provider 別 env を切るか単一 flag か）。→ 実装票。
- **OPEN-4（RV-06 連携）**: ConversationSession/PendingCommand 導入時に InboundEvent の `external_event_id` を last_event_id 参照に使う設計余地（§2.4 で塞がないことのみ確認・本票は着手せず）。

---

### 付録: 司令塔裁定の反映対応表
| 裁定 | 反映箇所 |
|---|---|
| 1 IngestionReceipt=§9.17 field＋unique(caller+file_id+sha256)最小形・状態機械は提案 | §2.2・§5.2 |
| 2 lease/attempt=§8.8原則の最小適用・専用worker作らずアプリ内非同期consumer | §4 |
| 3 LINE ACK順序＋DB停止5xx＋K4再配送両ケース | §3.1・§9 |
| 4 粒度: 1 event=1 InboundEvent(external=webhookEventId)・IngestionReceiptはfile系専用 | §2.1・§2.2・§5 |
| 5 RCF-M10=state遷移＋返信結果emit・ログ集計 | §6 |
| 6 レイテンシ=durable insert 1回・計測方法記載・実装票で判定 | §8 |
