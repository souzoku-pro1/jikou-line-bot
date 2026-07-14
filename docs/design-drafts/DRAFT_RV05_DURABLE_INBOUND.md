# DRAFT: RV-05/RV-13 durable InboundEvent / IngestionReceipt 横展開（rev3・最終）

- 状態: **DRAFT rev3（R-RV-05-13-D2 全所見・司令塔具体裁定 14 件反映・設計固定）**
- 対象: RV-05（BackgroundTasks 再起動消失）・RV-13（sortation process-memory・失敗の成功ACK飲み込み）・RCF-M10（顧客Bot返信の観測性）
- 正本: 製品設計master v2.4 **§9.17 / §9.17.1 / §8.8**（引用は下記）／G1・G3
- rev3 差分: B-01/B-02・H-01〜H-07・M-01〜M-04・L-01 を反映（末尾「所見対応表」）。**Stripe 状態機械は source 実測で転記**（`hub/inbound_event.py` + `main.py::stripe_webhook`）。

> **master §9.17（FIXED）**: 「public webhook、GAS、watcher の request を ACK 後 process memory へ預けない。InboundEvent 必須field= provider、external_event_id、caller_id、payload_ref／hash、received_at、signature_result、state、attempt_count。IngestionReceipt 必須field= source_file_id、source_sha256、ingest_type、case_hint、first_seen_at、last_outcome、downstream_refs、idempotency_key。external event ID または caller＋source ID＋hash へ unique。payload に顧客 data がある場合は暗号化／不変参照と retention を定義し、通常 log へ複製しない。」
> **master §8.8（FIXED・原則のみ流用）**: 「外部実行を request 内/BackgroundTasks/process-memory queue へ結び付けない。durable DB を読む継続 worker で再開。startup と定期で expired lease 回収。vendor call 開始前を証明できる job だけ再 queue、開始後/不明は UNKNOWN。claim は atomic update/lock と lease、concurrency 上限、同一 idempotency key を二 worker が実行しない。graceful shutdown は新規 lease 停止＋実行中 attempt の durable marker 確定。health は poll/成功/queue lag/expired lease/UNKNOWN を返す。」
> （§8.8 は Phase 6 Outbox worker 契約。本票は lease/atomic claim/startup reconciliation/graceful shutdown/fencing の**原則のみ**流用し OutboxJob は作らない。）

---

## §0 スコープ再構成（経路別・段階分離）

| 経路 | Phase A の範囲 | 自動 replay | 再送主体 | §9.17 準拠 |
|---|---|---|---|---|
| **顧客Bot `/webhook`** | 記録＋観測のみ（durable insert・返信結果カウンタ・未処理可視化） | **なし** | なし（RV-06 後に別票） | **限定逸脱**（§H-01） |
| **業務Bot 通知（LINE）** | 同上 | なし | なし | 限定逸脱 |
| **sortation `/sortation/ingest`** | 完全 durable（IngestionReceipt 冪等・PENDING_RETRY・SENDING/UNKNOWN） | consumer 処理 / GAS 5xx 再送 | **GAS（5xx 契約）** | 準拠 |
| **Stripe（既存 P1-005a）** | **不変** | 既存 | Stripe | 準拠 |

## §H-01 LINE Phase A の限定逸脱宣言【独立節・rev3】

**明示宣言**: 顧客/業務Bot の Phase A は **§9.17 の「process restart 後は durable state から再構成」原則への一時的・限定的逸脱**である。
- **逸脱の内容**: InboundEvent へ durable 記録はするが、**crash 後の未処理 event を自動 replay しない**（＝durable state からの再実行を保留する）。
- **逸脱の理由**: 顧客Bot 返信の安全な再開には ConversationSession/PendingCommand（§9.17.1）が必要で、無いまま自動 replay すると**二重返信**を生む（§9.17.1「memory fallback で成功 ACK しない／別 user/channel/case への転用拒否」を満たせない）。
- **逸脱で失うもの・代替**: 自動回復は得られないが、**未処理 event を「滞留」として観測可能にし、収束率低下・dead-man で検知**する（HOTFIX-01 型沈黙障害の**検知**は達成）。回復（再返信）は人手 or RV-06。
- **解消条件**: **RV-06（session/command durable 化）完了で本逸脱を解消**し、顧客Bot も durable replay へ移行（別票・K4 が前提＝§9.2）。
- 本節は逸脱を**隠さず宣言**し、G3「LINE event replay で遷移一回」を Phase A では「replay しない（＝遷移も再実行もしない）」形で満たす（冪等記録のみ）ことを明記する。

## §1 統合方針（H-03/H-05: 同居＋汎用 attempt 表）

| 選択肢 | InboundEvent 本体 | attempt/fencing | 判定 |
|---|---|---|---|
| **A. 同居＋汎用 attempt 別表（推奨）** | 既存 `inbound_event` に provider 行（**ALTER 0**） | **新表 `processing_attempt`（汎用・target_kind/target_id）** | **採用** |
| B. LINE 専用別表 | 新設 | 同上 | 不採用 |

## §2 schema

### 2.1 InboundEvent（同居・**ALTER なし**）
既存列で収容。**LINE の userId は既存 `caller_id` 列に保存**（H-02）: `provider="line"` / `external_event_id=webhookEventId` / `caller_id=LINE userId` / `signature_result` / `payload_hash` / `state`（§5.1）/ `attempts` / `claimed_at`（=fence token・§B-02）/ `processed_at` / `last_error`。

### 2.2 IngestionReceipt（新表・file ingest 専用・LINE 不使用）
```
ingestion_receipt(id PK, source_file_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
  ingest_type TEXT NOT NULL, caller_id TEXT NOT NULL, case_hint TEXT,
  first_seen_at ts NOT NULL, last_outcome TEXT NOT NULL, downstream_refs TEXT,
  idempotency_key TEXT NOT NULL, claimed_at ts, UNIQUE(idempotency_key))
```

### 2.3 processing_attempt（**汎用・新表**・B-01/H-03/H-05）
親（InboundEvent でも IngestionReceipt でも）を **target_kind/target_id** で汎用参照:
```
processing_attempt(
  id PK,
  target_kind  TEXT NOT NULL,   -- "inbound_event" | "ingestion_receipt"
  target_id    BIGINT NOT NULL, -- 親行 id
  attempt_no   INT NOT NULL,    -- 親ごと単調増加（履歴・観測用）
  fence_token  TEXT NOT NULL,   -- claim 時の親 claimed_at 値（§B-02 の epoch）
  claimed_at   ts NOT NULL,
  lease_expires_at ts NOT NULL,
  phase        TEXT NOT NULL,   -- claimed / vendor_pre / SENDING / terminal
  outcome      TEXT,            -- §H-07 terminal 集合
  UNIQUE(target_kind, target_id, attempt_no)
)
```
- B-01: **汎用表**にすることで InboundEvent/IngestionReceipt 双方の attempt/fencing を1表で扱う（将来 provider 追加も target_kind で吸収）。

### 2.4 冪等キー contract（H-04: escape・NULL 禁止・衝突→case_hint 比較）
- **NULL/空 禁止**: 構成要素が1つでも NULL/空なら **durable insert 拒否＝5xx**（fail-close）。
- **escape**: 素の `":"` 連結は禁止。**length-prefix 連結の sha256**（NM01 canonical 方式）:
  `key = hex(sha256( for f in fields: ascii(len(utf8(f)))||":"||utf8(f)||"\n" ))`。
  LINE: `["line", webhookEventId]`／sortation: `["sortation", caller_id, source_file_id, source_sha256]`。
- **衝突 contract（H-04・duplicate_suspect）**: `UNIQUE(idempotency_key)` 違反時、
  1. 既存行の **`case_hint`（及び source_sha256）を新規要求と比較**。
  2. **一致 → dedup skip**（同一物の重複配送＝一回処理・冪等）。
  3. **不一致 → `duplicate_suspect`**（同一 file_id/sha だが case_hint 等が食い違う＝取り違え疑い）を last_outcome に立て、**人手確認へ**（自動処理しない）。§6 で `duplicate_suspect` を alert。

## §2.6 payload / PII 方針（B-01 の retention・H-02 の userId）
- **payload 本文は保存しない**（§9.17）。`payload_hash`＋最小抽出（event_type 等・非PII）のみ。**log へ複製しない**（emit で vendor_raw/name/freetext は suppress）。
- **H-02: LINE userId は `caller_id` 列に保存する**（人手確認に必須）。userId は擬似匿名 ID だが PII 相当として扱う:
  - **DB 列にのみ保存・log には出さない**（lifecycle ログは `emit(caller_id, "external_ref", ...)` で **suppress**＝表示されない）。
  - **retention = OPEN（P16）**。本票は仮運用として userId を列保持（暗号化列＋retention は監査要件時に P16 別票）。
- **人手確認 runbook（H-02）**: `state ∈ {UNKNOWN, duplicate_suspect}` の行 →（管理者が DB で）`caller_id`(userId)/`external_event_id` を取得 → **App 28（照会/レビューキュー）で該当顧客レコードを照合** → 手動対応（再返信要否の判断）。userId→顧客の対応付けは App 28 側で行い、**DB とログには氏名/本文を持ち込まない**。

### 2.7 migration・RV-06 余地
- InboundEvent は migration なし。**新規 migration = `ingestion_receipt` ＋ `processing_attempt`**（`down_revision=<現head b7d3e1a9c2f4>`）。適用は大野（PUBLIC URL）。
- RV-06 余地: InboundEvent に session 固有列を足さない（§9.17.1 は別表・別票）。

## §B-02 claim / terminal の atomic SQL 契約（fencing・疑似コード）

**epoch = 親行の単一カウンタ = 親行 `claimed_at`**（親ごとに1つ）。claim/terminal は**単一 atomic SQL**で行い、**「事前 SELECT → 判断 → UPDATE」の read-modify-write は禁止**（race）。

**claim（atomic・no pre-SELECT）**:
```sql
-- 未処理 or lease 切れの1行を条件付き UPDATE で奪う。claimed_at が fence token。
UPDATE inbound_event
   SET claimed_at = :now, attempts = attempts + 1, state = 'processing'
 WHERE id = :id
   AND state IN ('received','processing')
   AND (claimed_at IS NULL OR claimed_at < :stale_cutoff)
RETURNING id, claimed_at AS fence_token;   -- 0 行 = 他 worker が保持中 → 諦める
-- 併せて processing_attempt に (attempt_no, fence_token=claimed_at, phase='claimed') を INSERT
```

**terminal commit（fencing・atomic）**:
```sql
-- 自分の fence_token（claim 時の claimed_at）と一致する時だけ terminal 化。
UPDATE inbound_event
   SET state = :terminal, processed_at = :now, last_error = :err
 WHERE id = :id AND claimed_at = :fence_token;   -- 0 行 = 新 worker が再claim済み → stale → abort（commit しない）
```
- **fence の意味**: 新しい worker が再claim すると親 `claimed_at` が変わる → 旧 worker の terminal UPDATE は 0 行 → **stale worker の後追い commit を弾く**（§8.8 fencing）。
- **禁止の明文化**: `SELECT state FROM inbound_event WHERE id=?` で読んでからアプリ側で分岐して UPDATE、は**禁止**（並行で state が動く）。必ず `UPDATE ... WHERE <条件> RETURNING` の rowcount/RETURNING で判定する。
- IngestionReceipt も同型（`claimed_at` を fence に、`UPDATE ingestion_receipt ... WHERE claimed_at=:fence`）。

## §3 ACK 契約

**共通（G1/G3）**: durable commit 前に 200 を返さない・event store 停止は 5xx・process-memory fallback 禁止。

### 3.1 LINE webhook（Phase A=記録+観測）
`署名検証→events[] を 1 event=1 InboundEvent durable insert→200→consumer 処理（自動 replay なし）`。冪等要素 NULL/空→5xx・DB 停止→5xx。

**transaction 契約（M-03）**: **1 event = 1 insert・event ごとに独立 transaction**でコミットする（1 webhook 内の複数 event を1トランザクションに束ねない）。**一部 event の insert が失敗しても成功済み event 行はロールバックせず残し、webhook 全体は 5xx を返す**。LINE が再配送すると、既に insert 済みの event は `UNIQUE(dedup_key)` で冪等 skip され、未 insert の event だけが改めて記録される（成功分の二重処理も、失敗分の喪失も起こさない）。

### 3.2 sortation（完全 durable・GAS 再送主体）
`_seen_drive_file_ids`→IngestionReceipt 冪等。**vendor 呼出前 `phase=vendor_pre`・呼出中 `phase=SENDING` を durable marker**。crash 復帰は vendor_pre まで再実行可・SENDING は **UNKNOWN（自動再送せず人手）**。ask 保存失敗→`PENDING_RETRY`（成功 ACK にしない）→GAS 5xx 再送。

### §H-06 sortation: GAS 再送時 state 別 応答表（8 状態）
GAS は 5xx で再送する。同一 idempotency_key の再投入が既存 receipt の各 state に当たった時の応答/claim 可否:

| # | state | GAS が受ける HTTP | claim 可否 | 備考 |
|---|---|---|---|---|
| 1 | received | 200（記録済・consumer が処理） | consumer が保持 | 再投入は冪等 |
| 2 | processing（lease 有効） | 200（処理中・冪等） | 不可（leased） | 二重処理しない |
| 3 | processing（lease 切れ=stale） | 再claim→処理→200/5xx | **可** | stale 回収 |
| 4 | vendor_pre（stale） | 再claim→処理→200/5xx | **可** | 呼出前を証明→安全 |
| 5 | SENDING（stale） | 200（UNKNOWN 化・人手待ち） | **不可** | 自動再送禁止＝二重forward回避 |
| 6 | completed | 200（skip・冪等） | 不可 | terminal |
| 7 | PENDING_RETRY | 再claim→再試行→200/5xx | **可** | downstream 失敗の再試行 |
| 8 | failed（attempt 上限） | 200（terminal・§6 で alert） | 不可 | 無限再送しない |
（`duplicate_suspect` は §2.4 の衝突経路で別処理＝人手。上表 8 状態とは別軸。）

## §4 consumer / lease / 回収 / shutdown / fencing
- 専用 worker なし・アプリ内非同期 consumer（flag ON 時のみ startup 起動）。
- claim/terminal は §B-02 の atomic SQL・fencing。concurrency は env 上限。
- **startup reconciliation**: sortation は vendor_pre まで再実行可・SENDING は UNKNOWN。**顧客/業務Bot（Phase A）は自動再実行しない**（滞留可視化のみ・§H-01）。
- graceful shutdown: 新規 claim 停止＋実行中 attempt の durable marker 確定後に終了。

## §5 状態機械（provider 別・§5.3 廃止＝H-05）

**§5.3（旧・汎用冪等節）は廃止**し、provider 別規則に分割する。terminal 集合は §H-07 に統一。

### 5.1 provider 別 状態機械表
**Stripe（source 実測・`hub/inbound_event.py`＋`main.py::stripe_webhook`）**:
| 項目 | 実測値 |
|---|---|
| state 値 | `processing` / `done` / `failed`（この3値のみ） |
| record_stripe_event outcome | `new`（INSERT→processing）/ `reprocess`（既存 failed を claim・または stale processing を再claim→processing）/ `skipped_duplicate`（既存 done）/ `in_progress`（既存 processing・15分以内） |
| HTTP 応答 | `skipped_duplicate`→**200** `{"journal":"skipped_duplicate"}` / `in_progress`→**503**（D14）/ `reprocess`→reconciliation（App21 照合・既起票なら mark_done→200 `reconciled`）→未起票なら処理→mark_done→**200** / `new`→処理→mark_done→**200** / 処理中例外→mark_failed→**再送出（5xx）** |
| 遷移 | INSERT→`processing`／`failed`+再配送→claim→`processing`／`processing`(stale: claimed_at NULL or <15分cutoff)→再claim→`processing`／`processing`(15分以内)→(不変)+attempts+1→`in_progress`(503)／`done`+再配送→(不変)+attempts+1→`skipped_duplicate`(200)／mark_done→`done`／mark_failed→`failed` |
| 再送主体 | **Stripe**（指数バックオフ最大3日）。stale 再claim の起動主体も Stripe 再配送 |
| startup 自動 replay | なし（回収の起動主体＝Stripe 再配送・既存設計 D14/D15 不変） |

**LINE（Bot・Phase A）**: `received→processing→{completed / failed / reply_fail / no_reply_intended}`。**SENDING/UNKNOWN は持たない**（M-04: 返信中 crash は `processing` のまま＝**stale processing 滞留**として §6 の最古滞留・収束率で観測する。SENDING/UNKNOWN 系は **sortation 専用**）。**自動 replay なし**・再送主体なし。

**sortation**: `received→processing→{completed / PENDING_RETRY / failed}`／`vendor_pre→SENDING→UNKNOWN`／衝突時 `duplicate_suspect`。再送主体 **GAS（5xx）**。startup 再実行は vendor_pre まで。

### 5.2 H-02 wrapper 結果封筒（terminal 分類）
処理 wrapper は必ず1つの terminal を返す（sink で握らない）: `completed` / `failed` / `reply_fail` / `no_reply_intended`。**「返信すべきだったのに沈黙（reply_fail/failed）」と「正常な無返信（no_reply_intended）」を分離**（HOTFIX-01 再発検知の要）。
- **M-01 返信 fallback**: Reply API 失敗→Push API 成功（`_line_reply_with_fallback` 相当）は**メッセージが届いた＝`completed`**とし、別カウンタ **`reply_fallback_ok`** で可視化（reply 側劣化の兆候を捕捉）。**Reply/Push 双方失敗のみ `reply_fail`**。

## §6 観測性設計（H-01 全面改訂・H-07 alert 軸分離）

全出力 emit 契約・PII/本文/payload 非混入・ログ集計でよい。

### 6.1 §H-07 terminal 集合の統一（M-04 二重分類反映）
- **terminal（終端）集合** = `{ completed, failed, no_reply_intended, skipped_duplicate, `**`reply_fail`（LINE）**`, `**`UNKNOWN`（sortation）**` }`（＝それ以上**自動**処理しない終端）。
  - **二重分類（M-04・H-07 整合）**: **`reply_fail`（LINE の返信終端・不達）と `UNKNOWN`（sortation の SENDING crash・人手確認待ちの終端）は terminal でありつつ軸B 警戒でもある**（終端到達だが放置不可＝人手 alert）。
- **非terminal（中間/要処理）** = `{ received, processing, vendor_pre, SENDING, PENDING_RETRY }`。
- **警戒（軸B・人手/alert）集合** = `{ reply_fail, UNKNOWN, duplicate_suspect }`（うち reply_fail/UNKNOWN は terminal と**二重分類**）。
- **収束率** = **terminal 到達数（reply_fail・UNKNOWN を含む）/ received**（低下＝処理停止兆候）。分子は本 terminal 集合と一致させる。

### 6.2 §H-07 alert 軸の分離
alert を混同しないよう軸を分ける:
- **軸A 処理停止**: consumer heartbeat（最終 poll/最終成功）鮮度・queue lag・収束率低下・滞留（一定時間 non-terminal）。
- **軸B 要人手**: 警戒集合（reply_fail / UNKNOWN / duplicate_suspect）の件数増。
- **軸C 受理前喪失**: `inbound_insert_fail`（B-02→本節で §H-03 と接続）。
- 各軸を daily_healthcheck / notify_heartbeat の dead-man に統合（新規メッセージ増やさない）。

### 6.3 §H-03 喪失検知範囲の正直な限定（3層）
「lost event 0」は**達成範囲を正直に限定**する:
| 層 | 検知対象 | 手段 | 限界 |
|---|---|---|---|
| L1 **直接検知** | durable insert に失敗した event（署名OK・5xx 返却） | `inbound_insert_fail` カウンタ | サーバに**到達した**もののみ |
| L2 **間接検知** | サーバ**停止中**に到達し受理されなかった event | 外部監視（uptime/LB 5xx 率・Railway メトリクス）で「停止窓」を検知→その窓に来た event は喪失疑い | 件数は**推定**（正確な欠番は不明） |
| L3 **完全差分** | LINE が送った webhookEventId 全量との差分 | **K4（再配送 or 監査 API）確定後**に照合 | K4 未確定では**不可能** |
→ 本票は L1 を確実に・L2 を運用監視で・**L3 は K4 後**と明記。「lost 0」は L1（受理後 crash）に限り達成、L2/L3 は限定と宣言。

## §7 feature flag・段階導入・rollback（M-01 機械的担保）
- flag `INBOUND_EVENT_DURABLE_ENABLED`（既定 OFF）。
- **M-01**: (1)受理部**最上段で flag 判定・OFF は現行 BackgroundTasks へ即分岐**（durable コードに入らない）(2)consumer startup/graceful shutdown は **flag ON 時のみ登録**(3)**AST 検査**で「durable insert/consumer 起動が flag 判定内側からのみ到達」「commit 前 200 経路の不在」「OFF 経路に durable 呼出なし」を機械強制。
- 段階導入: sortation→業務Bot→顧客Bot（観測のみ）。rollback=flag OFF。

## §8 テスト計画（M-03/L-01・naive FAIL 形）

### 8.1 unit / contract
ACK 順序・冪等 escape（要素内`:`/改行/空/NULL→5xx）・衝突（一致=skip / 不一致=duplicate_suspect）・状態機械（§5.1 provider 別）・封筒 terminal（§5.2 各）・fencing（stale の terminal UPDATE=0 行で abort）・atomic claim（pre-SELECT 方式が無いこと＝§7 AST）・transaction 契約（§3.1: 1 event=1 insert・一部失敗で成功分残し全体 5xx）。

### 8.2 negative（naive が FAIL する形）
crash 回収（sortation・現行は event 消失を実証＝**§8.5** の修正前 FAIL 実測で固定）／SENDING crash→UNKNOWN（sortation・二重forward なし）／**顧客Bot Phase A crash→`processing` 滞留として可視化（自動 replay しない）**／replay 一回／ask 保存失敗→PENDING_RETRY（現行は成功 response 飲み込みを実証）。

### 8.3 regression / flag OFF / Stripe 非破壊
全 suite（1,328+）維持・**顧客Bot handler smoke 必須**・flag OFF 完全同一（§7 AST）。既存 Stripe テスト（`test_inbound_event_stripe.py` 等）が **不変で通る**（§5.1 の Stripe 欄は現行実装のまま・M-04 非破壊）。

### 8.4 レイテンシ（M-03）
durable insert = **1 event あたり DB 書込呼出 1 回を固定アサート**（曖昧な ms 実測でなく**呼出回数**）＋webhook 応答は insert+200 のみ（consumer は非同期）。timeout は固定値でアサート。LINE 応答要件超過の懸念が出たら STOP（設計持ち帰り）。

### 8.5 修正前 FAIL 実測（現行が event 消失することの実証）
現行（flag OFF・BackgroundTasks）で「200 を返した後に処理タスクが crash 相当で失われる」ことを再現するスクリプトを **work-log(.md) に本文＋実出力全文**で固定（追跡 .py に print 禁止・RV-04b 規律）。§8.2 の crash 回収テストはこの実証（§8.5）を naive baseline として参照する。実測は実装票で採取。

## §9 OPEN / K4 / 計上境界表（M-02）

### 9.1 M-02 計上境界表（各ケースの計上先を確定）
| ケース | received | insert_fail | processing | completed | reply_fallback_ok | reply_fail | failed | no_reply_intended | dedup_skip | UNKNOWN | PENDING_RETRY | duplicate_suspect |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 署名OK・insert 成功 | ✓ | | | | | | | | | | | |
| insert 失敗(DB停止)→5xx | | ✓ | | | | | | | | | | |
| consumer 処理開始 | | | ✓ | | | | | | | | | |
| Reply 成功＋downstream 完了 | | | | ✓ | | | | | | | | |
| **Reply 失敗→Push 成功(fallback)** | | | | ✓ | ✓ | | | | | | | |
| **Reply/Push 双方失敗** | | | | | | ✓ | | | | | | |
| handler 例外(上限超) | | | | | | | ✓ | | | | | |
| 返信不要 event | | | | | | | | ✓ | | | | |
| 重複配送(既存 completed・要素一致) | | | | | | | | | ✓ | | | |
| crash 中 SENDING→復帰(sortation) | | | | | | | | | | ✓ | | |
| ask 保存失敗(sortation) | | | | | | | | | | | ✓ | |
| 同一 file_id/sha・case_hint 不一致 | | | | | | | | | | | | ✓ |
（M-01: **Reply 失敗→Push 成功は `completed`（メッセージは届いた）＋別カウンタ `reply_fallback_ok` で可視化**・**Reply/Push 双方失敗は `reply_fail`**。1 event は遷移各段で1回ずつ計上。dedup_skip は received に含めない＝二重計上しない。SENDING/UNKNOWN 行は sortation 専用〔M-04〕。）

### 9.2 OPEN / K4
- **K4（[人]/大野・前提条件）**: LINE Developers の webhook 再配送設定確認。**顧客Bot 自動 replay 有効化票（RV-06 後）のブロッキング前提**。Phase A（観測のみ）は不要。§H-03 L3 の完全差分も K4 後。
- **OPEN-1**: vendor_pre/SENDING marker の実装位置（二重 forward ゼロが要件）。→ 実装票。
- **OPEN-2**: consumer concurrency 既定・shutdown 待機上限。→ 実装票。
- **OPEN-3**: 段階導入 sub-gate（provider 別 env か単一 flag か）。→ 実装票。
- **OPEN-P16**: userId/payload の暗号化保存＋retention（B-01/H-02）。監査要件時に別票。

---

### 所見対応表（R-RV-05-13-D2 → rev3）
| 所見 | 反映箇所 |
|---|---|
| B-01 processing_attempt 汎用表（target_kind/target_id） | §2.3 |
| B-02 epoch=親行単一カウンタ・atomic SQL 契約疑似コード・事前SELECT禁止 | §B-02 |
| H-01 LINE Phase A 限定逸脱宣言（独立節） | §H-01 |
| H-02 userId DB列保存＋PII方針＋App28 人手 runbook | §2.1・§2.6 |
| H-03 喪失検知の3層限定（直接/間接/K4後） | §6.3 |
| H-04 衝突contract（case_hint比較→skip/conflict）＋duplicate_suspect | §2.4 |
| H-05 §5.3廃止・provider別分割・Stripe欄を実測修正 | §5・§5.1 |
| H-06 GAS再送時 state別応答表（8状態×HTTP×claim可否） | §H-06 |
| H-07 terminal集合統一＋alert軸分離 | §6.1・§6.2 |
| M-01 flag OFF 機械的担保（§7）／**Reply失敗→Push成功=completed＋reply_fallback_ok・双方失敗=reply_fail** | §7・§5.2・§9.1 |
| M-02 計上境界表（reply_fallback_ok 列追加） | §9.1 |
| M-03 レイテンシ=**1 event あたり**呼出回数/timeout固定／**transaction 契約（event独立tx・一部失敗で成功分残し全体5xx）** | §8.4・§3.1 |
| M-04 Stripe 非破壊／**LINE から SENDING/UNKNOWN 除去（返信中crash=stale processing滞留で観測）・LINE terminal に reply_fail・sortation terminal に UNKNOWN（二重分類）** | §5.1・§6.1・§8.3 |
| L-01 用語/terminal 集合の一貫化・**§8 subsection 番号化（§8.2 crash→§8.5 修正前FAIL 参照修正）** | §6.1・§8.2・§8.5 |
