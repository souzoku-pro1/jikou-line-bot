# DRAFT: RV-05/RV-13 durable InboundEvent / IngestionReceipt 横展開（rev2）

- 状態: **DRAFT rev2（R-RV-05-13-D 全所見反映・設計固定・実装は次票）**
- 対象: RV-05（BackgroundTasks 再起動消失）・RV-13（sortation process-memory 重複防止・失敗の成功ACK飲み込み）・RCF-M10（顧客Bot返信の観測性）
- 正本: 製品設計master v2.4 **§9.17**（InboundEvent・IngestionReceipt）・**§9.17.1**（ConversationSession・PendingCommand＝本票 OUT_OF_SCOPE）・**§8.8**（durable worker 運用契約・原則のみ流用）／G1・G3
- rev2 差分: §0 スコープ再構成を新設。B-01〜B-03/H-01〜H-05/M-01〜M-03/L-01 を反映（末尾「所見対応表」）。

> **master §9.17 引用（FIXED）**: 「public webhook、GAS、watcher から受けた request を、ACK 後の process memory へ預けない。InboundEvent 必須field= provider、external_event_id、caller_id、payload_ref／hash、received_at、signature_result、state、attempt_count。IngestionReceipt 必須field= source_file_id、source_sha256、ingest_type、case_hint、first_seen_at、last_outcome、downstream_refs、idempotency_key。external event ID または caller＋source ID＋hash へ unique 制約。payload に顧客 data がある場合は暗号化／不変参照と retention を定義し、通常 log へ複製しない。」
> **master §8.8 引用（FIXED・原則のみ）**: 「外部実行を request 内処理／BackgroundTasks／process-memory queue／browser request の寿命へ結び付けない。durable DB を読む継続 worker として起動し再開。startup と定期で expired lease 回収。vendor call 開始前を証明できる job だけ再 queue、開始後/不明は UNKNOWN。claim は atomic update/lock と lease、concurrency 上限、同一 idempotency key を二 worker が実行しない。graceful shutdown は新規 lease 停止＋実行中 attempt の durable marker 確定。health は poll/成功/queue lag/expired lease/UNKNOWN を返す。」
> （注: §8.8 は Phase 6 の Outbox worker 契約。本票は **lease/atomic claim/startup reconciliation/graceful shutdown/fencing の原則だけ流用し、OutboxJob 自体は作らない**。）

---

## §0 スコープ再構成【rev2 新設・最重要】

「durable 化」は経路ごとに**安全に到達できる範囲が異なる**。特に**顧客Bot の自動 replay は ConversationSession/PendingCommand（§9.17.1・RV-06）が無いと二重返信を生む**ため、本票では自動 replay を行わない。経路別に段階を分離する:

| 経路 | 本票 Phase A の範囲 | 自動 replay | 再送主体 | 前提 |
|---|---|---|---|---|
| **顧客Bot `/webhook`** | **記録＋観測のみ**（InboundEvent durable insert・返信結果カウンタ・**crash した未処理 event を可視化**） | **なし**（startup 自動再実行しない） | なし（RV-06 導入後に durable replay を別票で） | RV-06 の session/command 表（本票では**依存表を明記して塞がない**） |
| **業務Bot 通知（LINE）** | 記録＋観測のみ（同上・provider="line"/event_type で区別） | なし | なし | 同上 |
| **sortation `/sortation/ingest`** | **完全 durable 化**（IngestionReceipt 冪等・PENDING_RETRY 可視化・**GAS が再送主体**） | consumer が未処理を処理／GAS が 5xx で再送 | **GAS（5xx 契約）** | なし（file ingest は再送安全） |
| **Stripe（既存 P1-005a）** | **不変**（provider="stripe"・D14 の 503/再送は Stripe 主体） | 既存どおり | Stripe | provider 別に分離（本票で触れない） |

**帰結**:
- 顧客/業務Bot は Phase A で**「受理済み未処理 event を捨てない・可視化する」**までを達成（HOTFIX-01 型の 31.7h 沈黙障害を**カウンタ＋dead-man で検知可能**にする＝RV-05 の主目的の前半）。**自動再返信は RV-06 後**（二重返信リスクを持たない範囲に限定）。
- sortation は Phase A で**完全 durable**（GAS 再送で lost 0・RV-13 の重複防止/失敗飲み込み解消）。
- この分離により「LINE には自動再送主体がない」問題を、**顧客Bot=観測先行／sortation=GAS 再送**で吸収する。

## §1 統合方針（H-03/H-05: 同居 vs 別表の再比較・attempt/fencing 込み）

§9.17 の必須 field は既存 `inbound_event`（P1-005a）が**全保持**。fencing/attempt 履歴は**新表 `inbound_event_attempt`**に分離（既存表 ALTER 回避）。

| 選択肢 | InboundEvent 本体 | attempt/fencing | 判定 |
|---|---|---|---|
| **A. 同居＋attempt別表（推奨）** | 既存 `inbound_event` に provider="line" 行（**ALTER 0**） | **新表 `inbound_event_attempt`**（fencing token・lease・attempt 履歴） | **採用**。既存 durable 基盤再利用・ALTER 0・fencing を ALTER なしで足せる |
| B. LINE 専用別表 | `line_inbound_event` 新設 | 同上 | 不採用（同一ロジック二重管理・§9.17「1 model」から乖離） |

**採用理由（H-03/H-05）**: InboundEvent 本体は provider 列で同居（状態機械は §5.1 で provider 別に分岐）。**fencing（stale worker が新 claim 後に commit するのを防ぐ）を既存表に ALTER で足せない**ため、`inbound_event_attempt`（新表）に **fencing token（単調増加 lease epoch）**を持たせ、consumer は「自分の attempt が最新である」ことを条件に commit する（§4.3）。これで ALTER 0 と fencing を両立。

## §2 schema

### 2.1 InboundEvent（同居・**ALTER なし**）
既存列で収容: `provider="line"` / `external_event_id=webhookEventId` / `dedup_key`（§2.3）/ `caller_id` / `signature_result`（X-Line-Signature 結果）/ `payload_hash` / `state`（§5.1）/ `attempts` / `claimed_at` / `processed_at` / `last_error`。

### 2.2 IngestionReceipt（**新表**・file 系 ingest 専用・LINE 不使用）
§9.17 必須 field ＋ 最小 unique:
```
ingestion_receipt(
  id PK, source_file_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
  ingest_type TEXT NOT NULL, caller_id TEXT NOT NULL, case_hint TEXT,
  first_seen_at ts NOT NULL, last_outcome TEXT NOT NULL,
  downstream_refs TEXT,           -- 起票 record_id 等・非PII参照のみ
  idempotency_key TEXT NOT NULL,  -- §2.3 の escape 規則で生成
  UNIQUE(idempotency_key)
)
```

### 2.3 冪等キー contract（H-04: escape・NULL 禁止・衝突）
- **NULL 禁止**: 冪等キー構成要素（caller_id・source_file_id・source_sha256／LINE は webhookEventId）は**全て NOT NULL・空文字禁止**。いずれか欠落なら **durable insert を拒否し 5xx**（受理しない・fail-close）。
- **delimiter injection の排除（escape）**: 素の `":"` 連結は要素内の `":"` で衝突する。**length-prefix 連結**（NM01 canonical と同方式）で生成:
  `key = sha256( for f in fields: ascii(len(utf8(f)))||":"||utf8(f)||"\n" )` の hex。
  - LINE: fields=`["line", webhookEventId]` → `dedup_key="line:"+hex`（provider prefix は表示用・unique は hex 部）。
  - sortation: fields=`["sortation", caller_id, source_file_id, source_sha256]`。
  - これで要素内 `":"`/改行があっても衝突・偽装不能。
- **衝突 contract**: `UNIQUE` 違反（INSERT 失敗）は **冪等 skip**（重複配送/再投入＝一回処理）であり**エラーにしない**（IntegrityError→既存行の state で分岐・§5.3）。

### 2.4 inbound_event_attempt（**新表**・fencing・H-03/H-05）
```
inbound_event_attempt(
  id PK,
  inbound_event_id  BIGINT NOT NULL,   -- inbound_event.id 参照（論理・FK は任意）
  attempt_no        INT NOT NULL,      -- 単調増加（= fencing token）
  lease_epoch       INT NOT NULL,      -- claim ごとに増加。commit 時に「最新 epoch か」を検証
  claimed_at        ts NOT NULL,
  lease_expires_at  ts NOT NULL,
  phase             TEXT NOT NULL,     -- claimed / vendor_pre / SENDING / terminal
  outcome           TEXT,              -- §H-02 封筒（completed/failed/reply_fail/no_reply_intended/UNKNOWN）
  UNIQUE(inbound_event_id, attempt_no)
)
```
- **fencing**: consumer は claim 時に `attempt_no=max+1, lease_epoch++` の行を insert。terminal commit は「この attempt が当該 event の最新 attempt」を条件に UPDATE（stale worker の後追い commit を弾く）。
- **ALTER 0**: fencing/lease/attempt 履歴を InboundEvent 本体を触らず別表で表現。RV-06 の session 系とも独立。

### 2.5 migration 方針・RV-06 余地
- InboundEvent は migration なし（同居）。**新規 migration = `ingestion_receipt` ＋ `inbound_event_attempt`**（`down_revision=<現head b7d3e1a9c2f4>`・create/drop）。適用は大野（PUBLIC URL）。model は `hub/` 新モジュール＋専用 metadata を `alembic/env.py` list へ統合。
- **RV-06 余地（§2.4 塞がない）**: InboundEvent に session 固有列を足さない。会話 state/command は RV-06 の別表（ConversationSession/PendingCommand・§9.17.1）に持たせ、InboundEvent の `external_event_id` を last_event_id 参照の起点にできる形を残す。

## §2.6 B-01: payload 保存設計（DB 列・log 複製禁止・retention）
- **本文 payload は DB 列に保存しない**（§9.17「顧客 data は暗号化/不変参照・通常 log へ複製しない」）。保存は **`payload_hash`（生 body sha256）＋最小抽出（event_type 等・非 PII）**のみ（P1-005a D8 継承）。
- **log 複製禁止**: emit 契約でカウンタ/lifecycle のみ。payload 本文・顧客氏名・本文は emit で suppress される kind のみ通す（vendor_raw/name/freetext は出さない）。
- **retention = OPEN（P16）**: 暗号化保存が将来必要になった場合の retention 期限は **P16 で確定**。本票は**仮運用として本文非保存**（hash＋最小抽出）で開始し、監査要件が出たら P16 で暗号化列＋retention を設計（別票）。

## §3 ACK 契約（経路別・§0 と整合）

**共通（G1/G3）**: durable commit 前に 200 を返さない。event store 停止時は 5xx（vendor retry 可能 response）。**process-memory fallback 禁止**。

### 3.1 LINE webhook（顧客/業務Bot・Phase A=記録+観測）
```
1. X-Line-Signature 検証（既存・不変）
2. events[] を 1 event=1 InboundEvent 行 durable insert
   （external_event_id=webhookEventId・UNIQUE(dedup_key) で delivery 重複冪等）
   ├ 冪等キー要素 NULL/空 → 5xx（§2.3・受理しない）
   └ DB 停止 → 5xx（memory fallback 禁止）
3. insert 完了後に 200
4. 【Phase A】consumer は返信生成を実行するが、**crash 後の startup 自動 replay はしない**
   （二重返信回避・RV-06 後に durable replay 化）。未処理で残った event は §6 で「滞留」として可視化。
```
- **B-02: K4 前提条件化**: 顧客Bot の**自動 replay を有効化する将来票の前提条件**として K4（LINE 再配送設定確認）を**ブロッキング前提**に格上げ。Phase A（観測のみ）は K4 未確定でも可（再送しないため）。
- **B-02: insert 失敗/到達差分カウンタ**: durable insert **前**に落ちた event（署名 OK だが insert 失敗＝5xx 返却）を `inbound_insert_fail` として計上。LINE 側「到達したはずの webhookEventId」との差分観測で「受理前喪失」を可視化（K4 OFF 時の喪失も数で捕捉）。

### 3.2 sortation（RV-13・完全 durable・GAS 再送主体）
- `_seen_drive_file_ids`（process-memory）→ **IngestionReceipt 冪等**へ置換。
- **B-03: SENDING marker・UNKNOWN・自動再送禁止**: forward/ask など downstream の**vendor 呼出前に `phase=vendor_pre`、呼出中に `phase=SENDING` を durable marker**。crash 復帰時、
  - `vendor_pre` まで（呼出前を証明）→ 再実行可。
  - `SENDING`（呼出後/不明）→ **UNKNOWN**（自動再送せず `last_outcome=UNKNOWN`・**人手確認**へ）。二重 forward を作らない。
- **ask task 保存失敗を成功 ACK にしない**（RV-13）: `last_outcome=PENDING_RETRY` で可視化。GAS は 5xx で再送（再送主体=GAS）。

## §4 consumer / lease / startup 回収 / graceful shutdown / fencing（§8.8 原則）

- **専用 worker なし**（[裁定2]）。アプリ内**非同期 consumer**（asyncio・startup 起動・flag ON 時のみ）。
- **§4.1 atomic claim + lease**: 条件付き UPDATE + RETURNING で1行を1 consumer が取得。`inbound_event_attempt` に attempt 行 insert（`attempt_no++`・`lease_epoch++`）。同一 dedup_key/idempotency_key を二重実行しない（[裁定2]）。
- **§4.2 startup reconciliation**: 起動時、lease 期限切れの未処理を回収。**但し §0 の範囲**:
  - **sortation**: 再実行可（GAS 再送安全・vendor_pre まで）。SENDING は UNKNOWN。
  - **顧客/業務Bot（Phase A）**: **自動再実行しない**（観測のみ）。滞留を §6 で可視化し、必要なら人手。
- **§4.3 fencing（H-03/H-05）**: terminal commit は「自 attempt が当該 event の最新 attempt（最大 attempt_no・最新 lease_epoch）」を条件に UPDATE。stale worker（古い lease）の commit を弾く。
- **§4.4 concurrency 上限**: env で consumer 並列度を上限化。
- **§4.5 graceful shutdown**: shutdown で新規 claim 停止＋実行中 attempt の durable marker（phase/outcome）確定後に終了。強制終了後も startup reconciliation で回復（sortation）／可視化（Bot）。

## §5 状態機械・冪等・replay・attempt 上限

### 5.1 provider 別 状態機械表（H-03）
| provider | states | 遷移の要点 | 再送主体 | startup 自動 replay |
|---|---|---|---|---|
| **line（Bot・Phase A）** | received→processing→{completed / failed / reply_fail / no_reply_intended} ／ SENDING→UNKNOWN | 返信結果は §H-02 封筒で terminal 分類。crash 中 SENDING は UNKNOWN | なし | **なし**（観測のみ） |
| **sortation** | received→processing→{completed / PENDING_RETRY / failed} ／ vendor_pre→SENDING→UNKNOWN | ask 保存失敗=PENDING_RETRY・SENDING crash=UNKNOWN | **GAS（5xx）** | あり（vendor_pre まで） |
| **stripe（既存）** | received→processing→{done / failed}（in_progress=503・D14） | **不変** | Stripe | 既存どおり |

### 5.2 H-02: wrapper 結果封筒 contract（terminal 分類）
処理 wrapper は必ず terminal outcome を1つ返す（sink で握り潰さない）:
- **completed**: 返信送信成功（reply API 2xx）＋ downstream 完了。
- **failed**: 例外/恒久失敗（attempt 上限内で reprocess 可能なら processing へ戻す・上限超で failed 確定）。
- **reply_fail**: 返信 API が失敗（送信不達）。※Phase A は自動再送しない＝可視化のみ。
- **no_reply_intended**: 返信を意図しない event（既読/フォロー解除等）＝正常終了（沈黙障害と区別）。
封筒により「返信すべきだったのに沈黙」（reply_fail/failed）と「正常な無返信」（no_reply_intended）を**明確に分離**（HOTFIX-01 の再発検知に必須）。

### 5.3 冪等・replay・attempt 上限
- 重複配送/再投入 → UNIQUE 衝突 → 既存行 state で分岐（completed=skip・failed=reprocess）。**replay で遷移一回**（G3）。
- `attempt_count` 上限超 → `failed`（無限リトライ禁止・§6 で可視化）。

## §6 観測性設計【H-01: 全面改訂】

**目的**: HOTFIX-01 型「背景処理全滅が沈黙」を**構造的に検知**する。全出力は **emit 契約経由・PII/本文/payload 非混入**（record_id/count 等の値域検証型のみ）。ログ集計でよい（dashboard 不要）。

### 6.1 カウンタ（RCF-M10・状態遷移 + 返信結果）
- 状態遷移: `received / processing / completed / failed`（＋ `inbound_insert_fail`＝受理前喪失・B-02）。
- 返信結果封筒: `reply_ok(completed) / reply_fail / exception(failed) / no_reply_intended`。
- sortation: `PENDING_RETRY / UNKNOWN` の件数。

### 6.2 heartbeat / lag / 滞留 / 収束率 / alert / dead-man
- **heartbeat**: consumer の最終 poll・最終成功時刻を app-state（既存 notify_heartbeat 方式）に記録。
- **queue lag**: `received` から `processing` までの滞留時間・未処理（received/processing で lease 切れ）件数。
- **滞留（backlog）**: 一定時間 `completed/failed` に至らない event 数。
- **収束率**: 受理数に対する terminal 到達率（completed+failed+no_reply_intended）/received。低下＝処理停止の兆候。
- **alert 経路 / dead-man**: 既存 daily_healthcheck / notify_heartbeat の dead-man に **「consumer 最終成功の鮮度」「滞留閾値超過」「reply_fail/UNKNOWN 増加」**を統合（新規メッセージ増やさず既存経路へ）。HOTFIX-01 の教訓（webhook 200 だけでは沈黙を検知不可）を、**返信結果封筒＋収束率＋dead-man**で塞ぐ。
- health（§8.8 準拠・参考値）: 最終 poll / 最終成功 / queue lag / expired lease / UNKNOWN 件数を返せる形にする（Phase A はログでよい）。

## §7 feature flag・段階導入・rollback【M-01: flag OFF 機械的担保】

- **flag `INBOUND_EVENT_DURABLE_ENABLED`（既定 OFF）**。
- **M-01 機械的担保**:
  1. **最上段短絡**: webhook/sortation 受理部の**最上段で flag 判定**。OFF なら**現行 BackgroundTasks 経路へ即分岐**（durable insert/consumer コードに一切入らない＝byte 同一）。
  2. **startup/shutdown hook 非接触**: consumer の startup 起動・graceful shutdown marker は **flag ON 時のみ登録/実行**。OFF 時は startup/shutdown の既存挙動に一切追加しない（scheduler 等と干渉しない）。
  3. **AST 検査**: 「durable insert / consumer 起動が flag 判定の内側からのみ到達可能」「flag OFF 経路に durable 呼出が無い」ことを静的テストで機械強制（RV-04b の `_has_signature_headers` 検査に倣う）。「durable commit 前に 200 を返す経路が存在しない」も source 検査。
- **段階導入**: sortation（GAS 再送・観測性先行）→ 業務Bot → 顧客Bot（観測のみ）。provider 別 sub-gate（env）で細分化可（OPEN-3）。
- **rollback = flag OFF**（env 切替のみ）。

## §8 テスト計画【M-03/L-01 改訂】

**方針**: 各テストは **naive 実装（現行＝event 消失/二重処理/失敗飲み込み）が FAIL する形**で書く（検証力担保）。列挙ケースを網羅。

### 8.1 unit / contract
- durable insert→200→consumer 処理の順序固定。
- 冪等キー escape（要素内 `":"`/改行/空/NULL）・衝突 contract（UNIQUE 違反=skip）・NULL→5xx。
- 状態機械（§5.1 provider 別）・封筒 terminal 分類（§5.2 の completed/failed/reply_fail/no_reply_intended を各々）。
- fencing: stale attempt の commit 拒否（古い lease_epoch は terminal 更新不可）。

### 8.2 negative（crash / replay / 保存失敗）— **naive が FAIL する形**
- **crash 回収（sortation）**: insert 後・処理前 crash → startup reconciliation で1回だけ処理。**現行（BackgroundTasks）は event 消失**を実証（§8.4）。
- **crash 中 SENDING → UNKNOWN**: 二重 forward を作らない（自動再送しない）。
- **顧客Bot Phase A**: crash した未処理 event が **§6 で滞留として可視化**される（自動 replay は**しない**＝二重返信ゼロ）。
- **replay**: 同一 webhookEventId 再配送 → 遷移一回（completed は skip）。
- **ask 保存失敗**: 成功 ACK にせず `PENDING_RETRY`（現行は成功 response で飲み込む＝FAIL 実証）。

### 8.3 regression / flag OFF
- 全 suite（現行 1,328+）維持・**顧客Bot handler smoke 必須**（`_process_line_event` 先頭ログ通過）。
- **flag OFF 完全同一**: 既存 LINE/sortation テスト＋handler smoke が flag OFF で不変。§7 の AST 検査（durable 呼出が OFF 経路に無い）。

### 8.4 レイテンシ（M-03: 呼出回数/timeout 固定）
- **durable insert は 1 回のみ追加**（§3）。テストは**曖昧な ms 実測ではなく「DB 書込呼出回数 = 受理あたり 1」を固定アサート**＋consumer とは非同期（webhook 応答は insert+200 のみ）。timeout は固定値でアサート（環境差に依存しない）。LINE 応答要件超過の懸念が出たら**設計持ち帰り（STOP）**。

### 8.5 修正前 FAIL 実測（現行が event 消失することの実証）
- flag OFF（現行 BackgroundTasks）で「200 を返した後に処理タスクが crash 相当で失われる」ことを再現するスクリプトを **work-log(.md) に本文＋実出力全文で固定**（追跡 .py に print 禁止・RV-04b 規律）。independent に再現可能な形。実測コードは実装票で採取（本 DRAFT は方法規定）。

## §9 OPEN / [人]確認（K4）/ 計上境界表（M-02）

### 9.1 M-02: 3カウント境界表（全ケースの計上先を確定）
| ケース | received | insert_fail | processing | completed | reply_fail | failed | no_reply_intended | dedup_skip | UNKNOWN | PENDING_RETRY |
|---|---|---|---|---|---|---|---|---|---|---|
| 署名OK・durable insert 成功 | ✓ | | | | | | | | | |
| 署名OK・insert 失敗(DB停止)→5xx | | ✓ | | | | | | | | |
| consumer 処理開始 | | | ✓ | | | | | | | |
| 返信送信成功＋downstream 完了 | | | | ✓ | | | | | | |
| 返信 API 不達 | | | | | ✓ | | | | | |
| handler 例外(上限超) | | | | | | ✓ | | | | |
| 返信不要 event(既読等) | | | | | | | ✓ | | | |
| 重複配送(既存 completed) | | | | | | | | ✓ | | |
| crash 中 SENDING→復帰 | | | | | | | | | ✓ | |
| ask 保存失敗(sortation) | | | | | | | | | | ✓ |
（1 event は複数行に計上され得る：received→processing→completed の遷移は各段で1回ずつ。dedup_skip は received に含めない＝重複を二重計上しない。）

### 9.2 OPEN / K4
- **K4（[人]/大野・B-02 で前提条件化）**: LINE Developers コンソールの **webhook 再配送設定の有無を確認**。**顧客Bot の自動 replay を将来有効化する票のブロッキング前提**。Phase A（観測のみ）は不要。
- **OPEN-1**: startup reconciliation の「vendor call 開始前を証明」粒度（sortation の vendor_pre/SENDING marker 実装位置）。→ 実装票（二重 forward ゼロが要件）。
- **OPEN-2**: consumer concurrency 既定値・graceful shutdown 待機上限。→ 実装票。
- **OPEN-3**: 段階導入 sub-gate（provider 別 env か単一 flag か）。→ 実装票。
- **OPEN-4（RV-06 連携）**: 顧客Bot durable replay は RV-06（session/command）導入後の別票。本票は §2.4/§0 で余地を塞がないことのみ担保。
- **OPEN-P16**: payload 暗号化保存＋retention（B-01）。監査要件が出たら別票。

---

### 所見対応表（R-RV-05-13-D → rev2 反映箇所）
| 所見 | 反映箇所 |
|---|---|
| §0 スコープ再構成（顧客Bot=記録+観測のみ/sortation=GAS再送/Stripe不変） | §0・§3・§5.1 |
| B-01 payload 保存設計（列/log複製禁止/retention=OPEN P16/仮運用） | §2.6・§9.2 |
| B-02 K4 前提条件化＋insert失敗/到達差分カウンタ | §3.1・§6.1・§9.2 |
| B-03 SENDING marker・UNKNOWN 人手・自動再送禁止 | §3.2・§4.2・§4.3・§5.1 |
| H-01 §6 観測性設計 全面改訂（heartbeat/lag/滞留/収束率/alert/dead-man） | §6 |
| H-02 wrapper 結果封筒（completed/failed/reply_fail/no_reply_intended） | §5.2 |
| H-03/H-05 provider別状態機械表＋inbound_event_attempt新表・fencing・同居再比較 | §1・§2.4・§4.3・§5.1 |
| H-04 冪等キー escape・NULL禁止・衝突contract | §2.3 |
| M-01 flag OFF 機械的担保（最上段短絡/hook非接触/AST検査） | §7 |
| M-02 3カウント境界表 | §9.1 |
| M-03/L-01 §8 テスト計画改訂（naive FAIL形・網羅・レイテンシ呼出回数/timeout固定） | §8 |
