# DRAFT: RV-05/RV-13 durable InboundEvent / IngestionReceipt 横展開（rev5・最終）

- 状態: **DRAFT rev5（R-RV-05-13-D4 の HIGH 2 件＋設計数理を確定・実装票発行可の想定）**
- 対象: RV-05（BackgroundTasks 再起動消失）・RV-13（sortation process-memory・失敗の成功ACK飲み込み）・RCF-M10（顧客Bot返信の観測性）
- 正本: 製品設計master v2.4 **§9.17 / §9.17.1 / §8.8**（引用は下記）／G1・G3
- rev5 差分: **H-D4-01（epoch 不変条件・単一 UPDATE 統一）**・**H-D4-02（状態正本=receipt 行一本化・attempt 監査専用）**・M-D4-01〜07・L-D4-01 を反映（末尾「所見対応表」）。sortation=同期モデル（rev4・B-NEW-01）を前提。Stripe は source 実測のまま非破壊。

> **master §9.17（FIXED）**: 「public webhook、GAS、watcher の request を ACK 後 process memory へ預けない。InboundEvent 必須field= provider、external_event_id、caller_id、payload_ref／hash、received_at、signature_result、state、attempt_count。IngestionReceipt 必須field= source_file_id、source_sha256、ingest_type、case_hint、first_seen_at、last_outcome、downstream_refs、idempotency_key。external event ID または caller＋source ID＋hash へ unique。payload に顧客 data がある場合は暗号化／不変参照と retention を定義し、通常 log へ複製しない。」
> **master §8.8（FIXED・原則のみ流用）**: 「外部実行を request 内/BackgroundTasks/process-memory queue へ結び付けない。durable DB を読む継続 worker で再開。startup と定期で expired lease 回収。vendor call 開始前を証明できる job だけ再 queue、開始後/不明は UNKNOWN。claim は atomic update/lock と lease、concurrency 上限、同一 idempotency key を二 worker が実行しない。graceful shutdown は新規 lease 停止＋実行中 attempt の durable marker 確定。health は poll/成功/queue lag/expired lease/UNKNOWN を返す。」
> （§8.8 は Phase 6 Outbox worker 契約。本票は **fencing/atomic claim/reconciliation の原則のみ**流用し、**継続 worker/OutboxJob/poll consumer は作らない**＝§4。）

---

## §0 スコープ再構成（経路別・段階分離）【B-NEW-01 反映】

| 経路 | 処理モデル | Phase A の範囲 | 回復（回運び手） | 再送主体 | §9.17 |
|---|---|---|---|---|---|
| **顧客Bot `/webhook`** | 受理=durable insert・処理=既存 BackgroundTasks | 記録＋観測のみ（自動 replay なし） | 人手/RV-06 | なし | **限定逸脱**（§H-01） |
| **業務Bot 通知（LINE）** | 同上 | 同上 | 人手/RV-06 | なし | 限定逸脱 |
| **sortation `/sortation/ingest`** | **同期処理（B-NEW-01）**: 処理は **GAS request 内で完結**（UploadFile bytes を運び手にする） | **IngestionReceipt=冪等/可視化/fencing の durable 台帳**（PDF 保存なし・**非同期 consumer なし**） | **GAS 再送（bytes 再持参）** | **GAS（5xx 契約）** | 準拠 |
| **Stripe（既存 P1-005a）** | 既存（不変） | **不変** | Stripe | Stripe | 準拠 |

**B-NEW-01（sortation 同期モデルの確定）**:
- 処理の**運び手 = GAS request**（PDF bytes を都度持参）。サーバは bytes を保存しない（§2.6・PII 回避）。
- **receipt は処理キューではなく「冪等・可視化・fencing」の durable 台帳**。receipt を読んで後から処理する**非同期 consumer は作らない**。
- **startup reconciliation は可視化遷移のみ**（放置された receipt を `PENDING_RETRY`／`UNKNOWN` に遷移させるだけ・**再処理しない**）。
- **回復の入力 = GAS 再送**（5xx を受けた GAS が bytes を再持参 → その request 内で同期再処理）。サーバ単独では bytes が無く再処理不能（§H-NEW-04）。

## §H-01 LINE Phase A の限定逸脱宣言【独立節】

**明示宣言**: 顧客/業務Bot の Phase A は §9.17 の「restart 後 durable state から再構成」原則への**一時的・限定的逸脱**。
- **逸脱内容**: InboundEvent へ durable 記録するが **crash 後の未処理 event を自動 replay しない**。
- **理由**: 安全な返信再開には ConversationSession/PendingCommand（§9.17.1・RV-06）が必要。無いまま replay すると**二重返信**。
- **失うもの/代替**: 自動回復なし。未処理は「滞留」として収束率低下・dead-man で**検知**（HOTFIX-01 型沈黙障害の検知は達成）。回復は人手/RV-06。
- **既知制約（H-NEW-01-R3・fix5 明文化）**: stale processing 回収（fix4）の駆動は **LINE 再配送のみ**。したがって **LINE 再配送が終了した後に stale 化した行は自動回収の契機を失う**——台帳上は人手 reset まで non-terminal のまま滞留し、顧客対応上は次のイベント（顧客の再発話等・別 dedup_key＝旧行は回収しない）まで沈黙する。検知は **§6 観測**（最古 non-terminal 滞留時間・収束率低下）、回収は**人手 reset**（runbook: work-log 2026-07-15_RV-05-13-fix5 §4）。K4 ON の LINE 再配送は指数バックオフで後期ほど疎になるため、stale 閾値 3600 秒（既定）を超える頃には再送が尽きていることがあり得る＝閾値は再配送ウィンドウ内の過剰併走を抑える下限であり、末尾再送による回収は保証しない。**LINE Phase A は epoch fencing を持たない**（inbound_event に epoch 列を足さない＝ALTER 0 維持・sortation の epoch fencing は §B-02）——排他 claim UPDATE が保証するのは**併走 claim 間の排他のみ**で claim 済み旧 task の失効 guard は無く、**stale 再 claim と生存中の旧 task の併走はあり得る**。まれな併走（二重返信）は比較裁定（work-log fix4 §3.3）どおり**受容・検知可能**（fix6: 旧「fencing 不要＝BackgroundTasks が1回だけ実行し競合 consumer が再claim しない」記述は fix4 の stale 再 claim 導入と矛盾するため削除し本行へ一本化）。
- **解消条件**: RV-06 完了で逸脱解消 → 顧客Bot も durable replay へ（別票・K4 前提）。

## §1 統合方針（H-05: 同居＋fencing は sortation 専用）

| 表 | 役割 | ALTER |
|---|---|---|
| `inbound_event`（既存） | LINE を provider="line" 行で**同居**（Stripe と同表・§9.17 field 充足） | **0** |
| `ingestion_receipt`（新表） | sortation の冪等/可視化/fencing 台帳（**epoch 列**・§2.2） | 新規 |
| `processing_attempt`（新表・**sortation 専用・監査**） | epoch 毎の claim 履歴（`FK(receipt_id)`・`UNIQUE(receipt_id,epoch)`・判定に使わない＝H-D4-02） | 新規 |

## §2 schema

### 2.1 InboundEvent（同居・**ALTER なし**・fencing なし）
`provider="line"` / `external_event_id=webhookEventId` / **`caller_id=LINE userId`**（H-02）/ `signature_result` / `payload_hash` / `state`（§5.1）/ `attempts` / `processed_at` / `last_error`。**epoch/fence 列は足さない**（Phase A は epoch fencing を持たない・併走受容は §H-01）。

### 2.2 IngestionReceipt（新表・**epoch fencing**・H-NEW-01）
```
ingestion_receipt(
  id PK, source_file_id TEXT NOT NULL, source_sha256 TEXT NOT NULL,
  ingest_type TEXT NOT NULL, caller_id TEXT NOT NULL, case_hint TEXT,
  first_seen_at ts NOT NULL, last_outcome TEXT NOT NULL, downstream_refs TEXT,
  idempotency_key TEXT NOT NULL,
  epoch INTEGER NOT NULL DEFAULT 0,     -- H-NEW-01: fencing カウンタ（claimed_at 方式を廃止）
  last_heartbeat_at ts,                 -- 同期処理中の生存証明（lease 相当）
  UNIQUE(idempotency_key))
```
- **claimed_at 方式は廃止**し `epoch INTEGER` に置換（新表なので **ALTER 0** 維持）。`last_heartbeat_at` の鮮度が「処理中の request が生きているか」の lease 相当。
- **状態正本＝receipt 行に一本化（H-D4-02）**: `last_outcome` が**唯一の権威ある state 値**で、`received / processing / vendor_pre / SENDING / completed / PENDING_RETRY / failed / UNKNOWN / duplicate_suspect` を取る（vendor_pre/SENDING も **attempt.phase ではなく receipt.last_outcome の値**）。**判定は receipt 行のみで行い、processing_attempt は参照しない**（§2.3）。
  - **表記対応注記（L-01）**: 本文の大文字表記（`SENDING`/`PENDING_RETRY`/`UNKNOWN`）は可読性のための表示名であり、**DB 実値・実装正本は全て小文字**（`sending`/`pending_retry`/`unknown`）。実装の状態定数（`hub/ingestion_receipt.py` の `ST_*`）と DB `last_outcome` 値が正本で、本 DRAFT の大小混在はそれらのエイリアス表示にすぎない。

### 2.3 processing_attempt（新表・**監査専用**・FK 直参照＝M-D4-06）
**polymorphic（target_kind/target_id）を廃止**し、`ingestion_receipt` を FK で直参照する（sortation 専用・監査ログ）:
```
processing_attempt(
  id PK,
  receipt_id  BIGINT NOT NULL REFERENCES ingestion_receipt(id) ON DELETE CASCADE,
  epoch       INTEGER NOT NULL,  -- receipt.epoch のスナップショット（監査）
  attempted_at ts NOT NULL,      -- 記録時刻（DB clock）
  phase       TEXT NOT NULL,     -- claimed / vendor_pre / SENDING / terminal（監査）
  outcome     TEXT,
  UNIQUE(receipt_id, epoch))      -- epoch あたり1行（監査整合）
)
```
- **監査専用・判定には使わない（H-D4-02）**: fencing/状態判定は **receipt.epoch / receipt.last_outcome のみ**で行う。processing_attempt は「いつ どの epoch で どの phase を試みたか」の履歴のみ。
- **ON DELETE CASCADE**: receipt を（retention/監査で）削除する場合、attempt も追随（孤児行を残さない）。
- **LINE（inbound_event）は attempt を作らない**（epoch fencing を持たない・§H-01）。将来 provider 追加は別 attempt 表 or 別票で扱う（polymorphic に戻さない）。

### 2.4 冪等キー contract（H-04）
- **NULL/空 禁止** → durable insert 拒否＝5xx。
- **escape**: length-prefix 連結の sha256（NM01 方式）: `key=hex(sha256( for f in fields: ascii(len(utf8(f)))||":"||utf8(f)||"\n" ))`。sortation: `["sortation", caller_id, source_file_id, source_sha256]`／LINE: `["line", webhookEventId]`。
- **衝突 contract（duplicate_suspect＝held・H-NEW-03）**: `UNIQUE` 違反時、既存行の `case_hint`/`source_sha256` を新要求と比較 → **一致=dedup skip**（冪等）／**不一致=`duplicate_suspect`（held）**＝人手（自動処理しない・§6 alert）。

## §2.6 payload / PII / runbook

- **payload 本文・PDF bytes は保存しない**（§9.17）。`source_sha256`／`payload_hash`＋最小抽出のみ。log へ複製しない（emit で vendor_raw/name/freetext suppress）。
- **H-02: LINE userId は `caller_id` 列に保存**（人手確認に必須）。DB 列のみ・log は `emit(caller_id,"external_ref",…)` で suppress。retention=OPEN（P16）。
- **人手確認 runbook（H-02＋H-NEW-04 限定）**: `last_outcome/state ∈ {UNKNOWN, duplicate_suspect}` の行 → 管理者が DB で `caller_id`/`external_event_id`/`source_file_id` を取得 → **App 28（照会/レビューキュー）で顧客レコード照合**。
  - **H-NEW-04（限定）**: **本文/PDF は保存していないため復元不能**。runbook の回復手段は「**復元**」ではなく「**聞き直し/再取得**」:
    - LINE UNKNOWN 相当（Phase A の滞留）: 顧客へ**聞き直す**（同じ質問を再提示）か、状況を確認。過去本文は再現しない。
    - sortation UNKNOWN: **GAS/担当者に該当 PDF の再送を依頼**（bytes を再持参させる＝同期再処理の入力）。
    - duplicate_suspect: App 28 で case_hint 相違を人手判定し、正しい case へ紐付け直す（自動処理はしない）。

### 2.7 migration・RV-06 余地
- InboundEvent は migration なし。**新規 migration = `ingestion_receipt` ＋ `processing_attempt`**（`down_revision=<現head b7d3e1a9c2f4>`）。適用は大野（PUBLIC URL）。
- RV-06 余地: inbound_event に session 列を足さない（§9.17.1 は別表・別票）。

## §B-02 epoch fencing の atomic SQL 契約【H-D4-01・単一 UPDATE パターンへ統一】

**不変条件（H-D4-01・最重要）**: **「epoch を進めない state 変更は存在しない」**。`last_outcome` を変える遷移（claim / terminal / reconciliation / 人手 reset）は**すべて `epoch = epoch + 1` を伴う単一 atomic UPDATE**で行う。これにより epoch は receipt の**版番号（version）兼 fence**になり、任意の in-flight request の `my_epoch` は次の遷移で必ず無効化される。全時刻は **DB clock（`now()`）**・**事前 SELECT→判断→UPDATE は禁止**。

**統一パターン（全 state 遷移）**:
```sql
UPDATE ingestion_receipt
   SET epoch = epoch + 1, last_outcome = :new_state, last_heartbeat_at = now() [, downstream_refs=…]
 WHERE id = :receipt_id
   AND <guard>            -- 現 last_outcome / epoch / heartbeat 鮮度に対する条件
 RETURNING epoch AS new_epoch;   -- 0 行 = guard 不成立（他遷移が先着）→ 何もしない
```
各遷移は `<guard>` だけが異なる:

| 遷移 | new_state | guard | 意味 |
|---|---|---|---|
| **claim** | `processing` | `last_outcome IN ('received','PENDING_RETRY')` | 未処理を1つだけ奪う。RETURNING の epoch が my_epoch（fence） |
| **terminal** | `completed`/`failed` | `epoch = :my_epoch` | 自分の epoch のままなら確定。0 行=再claim済み=stale→abort |
| **PENDING_RETRY** | `PENDING_RETRY` | `epoch = :my_epoch` | downstream 失敗の可視化（自分の epoch のみ） |
| **reconciliation（可視化）** | `PENDING_RETRY` / `UNKNOWN` | `last_outcome IN ('received','processing','vendor_pre'[/'SENDING']) AND last_heartbeat_at < :stale_cutoff` | 放置行を可視化遷移（epoch++ で in-flight を無効化・**再処理しない**） |
| **人手 reset** | `received` | `last_outcome IN ('UNKNOWN','duplicate_suspect')` | 人手解決後の再投入許可（epoch++） |

- 各遷移の直後、**同一 tx で `processing_attempt` に監査行を INSERT**（`receipt_id, epoch=new_epoch, phase`）。attempt は監査のみで判定に使わない（§2.3・H-D4-02）。
- **heartbeat は state 遷移ではない**（`last_outcome` を変えない）→ epoch を進めない唯一の書込。ただし fence 付き: `UPDATE … SET last_heartbeat_at=now() WHERE id=? AND epoch=:my_epoch`（0 行=stale→中断）。
- **fence の帰結**: 新 request が claim/reconciliation すると epoch++ → 旧 request の terminal/heartbeat（`WHERE epoch=:my_epoch`）は 0 行 → **stale の後追い commit を弾く**（二重処理回避）。
- **DB clock**: `now()`（DB 側）で lease 鮮度判定（多インスタンス/時計ずれに強い）。
- **禁止**: `SELECT last_outcome …` で読んで分岐して UPDATE は禁止。必ず `UPDATE … WHERE <guard> RETURNING`／rowcount で判定。

## §3 ACK 契約

**共通（G1/G3）**: durable commit 前に 200 を返さない・store 停止は 5xx・process-memory fallback 禁止。

### 3.1 LINE webhook（Phase A=記録+観測）
`署名検証→events[] を 1 event=1 InboundEvent durable insert→200→既存 BackgroundTasks で処理（自動 replay なし）`。冪等要素 NULL/空→5xx・DB 停止→5xx。
**transaction 契約（M-03）**: **1 event=1 insert・event ごと独立 transaction**。一部 insert 失敗でも成功分は残し webhook 全体は 5xx。再配送は成功分を `UNIQUE(dedup_key)` で冪等 skip・未 insert 分のみ記録。

### 3.2 sortation（**同期処理**・B-NEW-01）
```
1. GAS が PDF bytes を POST（運び手＝この request）
2. idempotency_key 生成（§2.4）→ ingestion_receipt を upsert（初回=received・DB 停止→5xx）
3. epoch claim（§B-02・同一 tx）→ **この request 内で同期処理**:
   phase='vendor_pre'（vision 呼出前）→ 'SENDING'（forward/ask 呼出中）を durable marker（heartbeat 付き）
4. 成功→terminal='completed'→200 ／ downstream 保存失敗→'PENDING_RETRY'→**5xx（GAS 再送）**
   ／ vendor 呼出中に request 死亡→receipt は SENDING のまま（次の GAS 再送 or startup が UNKNOWN 化）
```
- **非同期 consumer なし**（処理は request 内で完結）。receipt は冪等/可視化/fencing 台帳。
- **ask 保存失敗を成功 ACK にしない**（RV-13）→ `PENDING_RETRY`→5xx→GAS 再送で同期再試行。

### §H-06 sortation: GAS 再送への応答【H-D4-01: receipt.last_outcome × lease 鮮度の排他表】
GAS は 5xx で bytes を再持参して再送する。判定は **receipt.last_outcome（状態正本）× lease 鮮度（`last_heartbeat_at` の stale 判定）のみ**で行う（排他・重複しない行）。lease 鮮度が無関係な行は「—」。

| # | last_outcome | lease 鮮度 | claim UPDATE の guard 成否 | GAS 再送への応答 | 同期再処理 |
|---|---|---|---|---|---|
| 1 | `received` | — | 成立（`IN (received,PENDING_RETRY)`） | claim→同期処理→200/5xx | **する** |
| 2 | `PENDING_RETRY` | — | 成立 | claim→同期再試行→200/5xx | **する** |
| 3 | `processing` / `vendor_pre` | **fresh** | 不成立（processing で guard 外） | 200（処理中・冪等）or 409 | しない（別 request 実行中） |
| 4 | `processing` / `vendor_pre` | **stale** | reconciliation が先に `PENDING_RETRY` へ→行#2 で claim | 再claim→同期再処理→200/5xx | **する**（vendor 前を証明） |
| 5 | `SENDING` | **fresh** | 不成立 | 200（処理中・冪等）or 409 | しない |
| 6 | `SENDING` | **stale** | reconciliation が `UNKNOWN` へ（行#8） | 200（人手待ち・UNKNOWN） | **しない**（二重 forward 回避） |
| 7 | `completed` / `failed` | — | 不成立（terminal） | 200 skip（冪等）／failed は §6 alert | しない |
| 8 | `UNKNOWN` | — | 不成立 | 200（人手解決待ち） | **しない**（人手 reset まで） |
| 9 | `duplicate_suspect`（held） | — | 不成立 | 200（人手・held・§2.4） | **しない** |

**遷移操作（state ではなく操作・別掲）**:
- **reconciliation**: stale の `processing/vendor_pre → PENDING_RETRY`、`SENDING → UNKNOWN`（§B-02・可視化のみ・再処理なし）。上表の #4/#6 はこの遷移の帰結。
- **人手 reset**: `UNKNOWN / duplicate_suspect → received`（§B-02・epoch++）。以後の GAS 再送は行#1 として claim 可能になる。**reset は「受信した state」ではなく管理操作**なので表の行にしない。

## §4 処理モデル・fencing・startup reconciliation・shutdown【B-NEW-01: consumer 廃止】

- **継続 worker/poll consumer は作らない**。sortation は**同期処理**（GAS request 内）。LINE Phase A は**既存 BackgroundTasks**（§H-01 の逸脱どおり）。
- **fencing/claim**: sortation のみ §B-02 の epoch atomic SQL。同一 idempotency_key を二 request が同時処理しない。
- **startup reconciliation（可視化のみ・B-NEW-01）**: 起動時・定期に、**heartbeat が stale な非 terminal receipt を可視化遷移**させる（**再処理はしない**）:
  - `received`/`processing`/`vendor_pre`（stale）→ `PENDING_RETRY`（GAS 再送を促す）。
  - `SENDING`（stale）→ `UNKNOWN`（人手）。
  - LINE Phase A（inbound_event）: reconciliation 対象外（滞留は §6 で観測のみ・§H-01）。
- **graceful shutdown**: sortation は request 内同期のため、shutdown 時に処理中 request があれば通常の request 完了を待つ（新規は LB 側で止まる）。durable marker（phase/heartbeat）は既に確定済み。強制終了後は startup reconciliation が可視化で拾う。

## §5 状態機械（provider 別）

### 5.1 provider 別 状態機械表
**Stripe（source 実測・`hub/inbound_event.py`＋`main.py::stripe_webhook`・不変）**:
| 項目 | 実測値 |
|---|---|
| state 値 | `processing` / `done` / `failed`（3値のみ） |
| outcome | `new`（INSERT→processing）/ `reprocess`（既存 failed を claim・または stale processing 再claim→processing）/ `skipped_duplicate`（既存 done）/ `in_progress`（既存 processing・15分以内） |
| HTTP | `skipped_duplicate`→**200**／`in_progress`→**503**（D14）／`reprocess`→App21 照合(既起票=mark_done→200 reconciled)→未起票は処理→mark_done→**200**／`new`→処理→mark_done→**200**／処理中例外→mark_failed→**5xx** |
| 遷移 | INSERT→`processing`／`failed`+再配送→claim→`processing`／`processing`(stale)→再claim→`processing`／`processing`(15分内)→+attempts→`in_progress`(503)／`done`+再配送→+attempts→`skipped_duplicate`(200)／mark_done→`done`／mark_failed→`failed` |
| 再送/回収主体 | **Stripe**（指数バックオフ最大3日・stale 再claim も Stripe 再配送起点） |
| startup 自動 replay | なし（既存 D14/D15 不変） |

**LINE（Bot・Phase A）**: `received→processing→{completed / failed / reply_fail / no_reply_intended}`。**SENDING/UNKNOWN・fencing なし**（返信中 crash は `processing` のまま＝stale processing 滞留として §6 で観測。SENDING/UNKNOWN/epoch は sortation 専用）。自動 replay なし・再送主体なし。

**sortation（同期）**: `received→processing→{completed / PENDING_RETRY / failed}`／`vendor_pre→SENDING→(stale で)UNKNOWN`／衝突 `duplicate_suspect`（held）。再送主体 **GAS（5xx）**。startup は**可視化遷移のみ**（→PENDING_RETRY/UNKNOWN・再処理なし）。

### 5.2 wrapper 結果封筒（terminal 分類）
処理 wrapper は1つの terminal を返す（sink で握らない）: `completed`/`failed`/`reply_fail`(LINE)/`no_reply_intended`(LINE)／sortation は `completed`/`failed`/`PENDING_RETRY`(非terminal)/`UNKNOWN`。
- **M-01 返信 fallback**: Reply 失敗→Push 成功=**`completed`**＋別カウンタ **`reply_fallback_ok`**。Reply/Push 双方失敗のみ `reply_fail`。

## §6 観測性設計（H-NEW-03 terminal 再定義・alert 軸分離）

全出力 emit 契約・PII/本文/payload 非混入・ログ集計でよい。

### 6.1 §H-NEW-03 terminal / held / 収束率（再定義）
- **terminal（自動処理の終端）集合** = `{ completed, failed, no_reply_intended, reply_fail(LINE), UNKNOWN(sortation) }`。
  - **`skipped_duplicate` は terminal から除外**（H-NEW-03）: 重複配送の結果であって「その event の処理終端」ではない（元 event の terminal が実体）。dedup は分母側で扱う（下記）。
  - reply_fail(LINE)・UNKNOWN(sortation) は terminal かつ**軸B 警戒**の二重分類（終端だが放置不可）。
- **held 集合** = `{ duplicate_suspect }`（人手解決待ち＝自動は止まるが「既知状態に到達」）。
- **非terminal（要処理/進行中）** = `{ received, processing, vendor_pre, SENDING, PENDING_RETRY }`。
- **収束率（H-NEW-03 / M-D4）** = **(terminal 到達数 ＋ held 数) / distinct receipt**。
  - 分子に **held を含める**（既知状態に収束済み・stuck ではない）。
  - 分母は **distinct receipt**（同一 idempotency_key の receipt は1件・`dedup_skip`/`skipped_duplicate` や再送・reset で二重計上しない）。
  - §5.1・§9.1・本節で terminal/held の集合定義を**完全一致**させる。

**カウンタ3系列の分離（M-D4）**: 二重計上を避けるため計数を3系列に分ける。**収束率は系列B/系列A のみで算出**し、系列C（運用操作）は率に混ぜない:
| 系列 | 計数 | 用途 |
|---|---|---|
| **A 受理（distinct）** | distinct receipt 成立数・`inbound_insert_fail`・`dedup_skip`（=同一物の重複配送・分母に含めない） | 分母（distinct）と受理前喪失 |
| **B 終端/held（1 receipt 1回）** | `completed`・`reply_fallback_ok`(内数)・`reply_fail`・`failed`・`no_reply_intended`・`UNKNOWN`・`duplicate_suspect(held)` | 分子（terminal+held） |
| **C 運用操作（再入・非計率）** | `manual_reset`・`concurrent_reject`（並行再送の敗者）・`PENDING_RETRY 遷移回数` | 運用監視のみ（**収束率に入れない**・同一 receipt を何度でも通り得る） |
`manual_reset` は**専用カウンタ**（人手 reset 回数）で、reset された receipt は系列A では新規計上せず、次の terminal 到達で系列B に1回だけ計上する。

### 6.2 alert 軸の分離（H-07・consumer 前提を除去）
- **軸A 処理停止**: **収束率低下・滞留（一定時間 non-terminal のまま）・最古 non-terminal の滞留時間**（sortation は heartbeat stale 件数・LINE は processing 滞留）。※**poll/consumer heartbeat 前提の記述は削除**（継続 worker を持たないため。sortation の生存指標は receipt の heartbeat/遷移レート、LINE は state 遷移レート）。
- **軸B 要人手**: 警戒集合 `{ reply_fail, UNKNOWN, duplicate_suspect }` の件数増。
- **軸C 受理前喪失**: `inbound_insert_fail`（§6.3 と接続）。
- 各軸を daily_healthcheck / notify_heartbeat の dead-man に統合（新規メッセージ増やさない）。

### 6.3 喪失検知範囲の正直な限定（3層・H-03）
| 層 | 検知対象 | 手段 | 限界 |
|---|---|---|---|
| L1 直接 | durable insert 失敗（署名OK・5xx） | `inbound_insert_fail` | 到達したもののみ |
| L2 間接 | サーバ停止中に到達し未受理 | 外部監視（uptime/5xx 率・Railway メトリクス）で停止窓検知 | 件数は推定 |
| L3 完全差分 | LINE webhookEventId 全量差分 | **K4 確定後**照合 | K4 未確定では不可 |
→ 「lost 0」は L1 に限り達成・L2/L3 は限定と宣言。

## §7 feature flag・段階導入・rollback（M-01 機械的担保）
- flag `INBOUND_EVENT_DURABLE_ENABLED`（既定 OFF）。
- **M-01**: (1)受理部**最上段で flag 判定・OFF は現行経路へ即分岐**（sortation は現行 process-memory `_seen_drive_file_ids`／LINE は現行 BackgroundTasks・durable コードに入らない）(2)startup reconciliation（可視化）は **flag ON 時のみ登録**(3)**AST 検査**で「durable insert/claim が flag 判定内側のみ」「commit 前 200 の不在」「OFF 経路に durable 呼出なし」を機械強制。
- 段階導入: **sortation（同期・観測性先行）→ 業務Bot → 顧客Bot（観測のみ）**。rollback=flag OFF。

## §8 テスト計画（M-06 条件表・naive FAIL 形）

### 8.1 §M-06 テスト条件表（7 件・各々 naive がFAILする形）
| # | 条件 | 期待（本設計） | naive（現行）が FAIL する点 |
|---|---|---|---|
| 1 | sortation 同期処理中に request 死亡（vendor_pre） | **startup reconciliation は `PENDING_RETRY` 可視化のみ（再処理しない）**／**GAS 再送が claim して一回だけ処理**（epoch++ で旧 in-flight は無効化） | 現行は process-memory で重複判定が消え二重処理 or 喪失（M-D4-04: startup が再処理しない点を固定） |
| 2 | SENDING 中に death | stale→reconciliation で `UNKNOWN`・**自動再送しない**（二重 forward なし） | 現行は再送で二重 forward |
| 3 | 並行 GAS 再送（同一 idempotency_key） | claim UPDATE の **guard（`last_outcome IN (received,PENDING_RETRY)`）で先着1つだけ成立**・敗者は rowcount 0 で claim せず **`concurrent_reject`（=系列C・dedup ではない）**＝200/409 | 現行は process-memory 競合で二重（M-D4-05: 敗者は receipt-state guard で弾かれる／UNIQUE(epoch) は監査整合の副次） |
| 4 | stale epoch の terminal commit | `WHERE epoch=:my_epoch` 0 行で **abort** | naive は上書きして stale 結果を確定 |
| 5 | ask 保存失敗 | `PENDING_RETRY`→5xx（成功 ACK にしない） | 現行は成功 response で飲み込み |
| 6 | 冪等キー要素 NULL/空 or 要素内 `:` | 5xx（NULL）/ escape で衝突しない | naive は `:` 連結で衝突・NULL で誤一致 |
| 7 | duplicate_suspect（同 file_id/sha・case_hint 相違） | `held`・人手・自動処理しない | naive は上書き or 取り違え起票 |

### 8.2 negative（naive FAIL 形）
crash 回収（sortation・現行 event 消失を実証＝**§8.5**）／SENDING death→UNKNOWN（二重forward なし）／**顧客Bot Phase A crash→`processing` 滞留可視化（自動 replay しない）**／replay 一回／ask 保存失敗→PENDING_RETRY。

### 8.3 regression / flag OFF / Stripe 非破壊
全 suite（1,328+）維持・**顧客Bot handler smoke 必須**・flag OFF 完全同一（§7 AST）・既存 Stripe テスト不変（§5.1 Stripe 欄実測のまま・M-04 非破壊）。

### 8.4 レイテンシ（M-03）
sortation は元々同期処理のため durable 化の追加=**receipt upsert＋epoch claim の DB 書込のみ**を「**1 request あたり固定回数**」でアサート（ms 実測でなく呼出回数）。LINE は durable insert=**1 event あたり 1 回**。timeout 固定値。要件超過懸念で STOP。

### 8.5 修正前 FAIL 実測
現行の event 消失/二重処理/飲み込みを再現するスクリプトを **work-log(.md) に本文＋実出力全文**で固定（追跡 .py に print 禁止）。§8.1/§8.2 の各条件の naive baseline として参照。実測は実装票。

## §9 OPEN / K4 / 計上境界表（M-02＋M-05）

### 9.1 計上境界表（M-05: +6 行）
系列: **A受理**（received/insert_fail/dedup_skip）・**B終端held**（completed/reply_fallback_ok/reply_fail/failed/no_reply_intended/UNKNOWN/duplicate_suspect）・**C運用**（concurrent_reject/manual_reset・**収束率に入れない**）。processing は中間状態（B到達で解消）。

| ケース | received | insert_fail | processing | completed | reply_fallback_ok | reply_fail | failed | no_reply_intended | dedup_skip | UNKNOWN | PENDING_RETRY | duplicate_suspect | concurrent_reject | manual_reset |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 署名OK/receipt 成立 | ✓ | | | | | | | | | | | | | |
| insert 失敗(DB停止)→5xx | | ✓ | | | | | | | | | | | | |
| 処理開始(claim) | | | ✓ | | | | | | | | | | | |
| Reply 成功＋完了 | | | | ✓ | | | | | | | | | | |
| Reply 失敗→Push 成功 | | | | ✓ | ✓ | | | | | | | | | |
| Reply/Push 双方失敗 | | | | | | ✓ | | | | | | | | |
| handler 例外(上限超) | | | | | | | ✓ | | | | | | | |
| 返信不要 event(LINE) | | | | | | | | ✓ | | | | | | |
| 重複配送(要素一致) | | | | | | | | | ✓ | | | | | |
| SENDING death→UNKNOWN | | | | | | | | | | ✓ | | | | |
| ask 保存失敗→PENDING_RETRY | | | | | | | | | | | ✓ | | | |
| case_hint 相違→duplicate_suspect | | | | | | | | | | | | ✓ | | |
| **[M-05] 並行 GAS 再送・敗者（guard 不成立）** | | | | | | | | | | | | | ✓ | |
| **[M-05] stale epoch terminal abort** | | | | | | | | | | | | | ✓ | |
| **[M-05] vendor_pre stale→reconciliation→再claim→completed** | | | | ✓ | | | | | | | | | | |
| **[M-05] UNKNOWN に GAS 再送（人手前）** | | | | | | | | | | | | | ✓ | |
| **[M-05] 人手 reset（UNKNOWN/dup_suspect→received）** | | | | | | | | | | | | | | ✓ |
| **[M-05] PENDING_RETRY に再送→completed** | | | | ✓ | | | | | | | | | | |
（**収束率分子=terminal＋held（duplicate_suspect）。分母=distinct receipt**（dedup_skip/skipped_duplicate/再送/reset で二重計上しない）。**concurrent_reject/manual_reset=系列C＝収束率に入れない**。SENDING/UNKNOWN/epoch は sortation 専用。同一 receipt は B系列に1回だけ計上。）

### 9.2 OPEN / K4
- **K4（[人]/大野・前提条件）**: LINE 再配送設定確認（顧客Bot 自動 replay 有効化票=RV-06 後のブロッキング前提・§H-03 L3 も K4 後）。Phase A（観測）は不要。
- **OPEN-1**: vendor_pre/SENDING marker の実装位置（二重 forward ゼロが要件）。
- **OPEN-2**: heartbeat stale 判定閾値（lease 相当秒数）・並行 GAS 再送の 409 vs 200 方針。
- **OPEN-3**: 段階導入 sub-gate（provider 別 env か単一 flag か）。
- **OPEN-P16**: userId/payload の暗号化保存＋retention。

**持ち越し OPEN（実装票の着手条件として列挙・M-D4-07/L-D4-01）**:
1. **stale 判定閾値の確定**: `last_heartbeat_at` の stale_cutoff 秒数（vision/forward の最大所要時間＋余裕）。短すぎると生存中を再claim、長すぎると回復遅延。実装票で env 化＋既定値裁定。
2. **並行再送応答の 409 vs 200**: guard 不成立の敗者へ 200（冪等・処理中）か 409（明示競合）か。GAS のバックオフ挙動と整合させて確定。
3. **reconciliation の起動契機**: startup のみか定期 tick か。定期 tick は「継続 worker を作らない」原則との整合を明記（軽量 sweep=単発 UPDATE で consumer ではない旨）。
4. **attempt 表 retention**: 監査ログの保持期間・肥大化対策（epoch あたり1行）。ON DELETE CASCADE と retention（P16）の関係。
5. **duplicate_suspect の自動判定粒度**: case_hint 相違のみか、source_sha256 一致・file_id 一致の組合せ条件を厳密化（誤検知/見逃しの境界）。
6. **failed 上限（attempt_count）の値**: 何回で `failed` 終端にするか。GAS 再送の指数バックオフとの兼ね合い。
7. **LINE Phase A の「滞留」閾値**: processing のまま何分で軸A alert とするか（返信生成の最大所要時間基準）。RV-06 で durable replay 化するまでの暫定監視値。
（上記は本 DRAFT で設計を固定せず、**実装票で数値/契機を裁定**する条件。設計構造は本 DRAFT で確定済み。）

---

### 所見対応表（R-RV-05-13-D4 → rev5）
| 所見 | 反映箇所 |
|---|---|
| **H-D4-01（HIGH）** 不変条件「epoch を進めない state 変更は存在しない」を明文化・claim/reconciliation/人手reset を全て `epoch=epoch+1` の**単一 atomic UPDATE パターンに統一**（疑似SQL置換） | §B-02・§H-06 |
| **H-D4-02（HIGH）** 状態正本＝親 receipt 行へ一本化（vendor_pre/SENDING を `last_outcome` 値に・**attempt 表は監査専用**・判定に使わない） | §2.2・§2.3・§5.1 |
| **M-D4-01** §H-06 を「receipt.last_outcome × lease 鮮度」の**排他表**へ書き直し・reset は遷移操作として別掲 | §H-06 |
| **M-D4-02** 収束率 分母=**distinct receipt**・**reset 専用カウンタ**・**カウンタ3系列（A受理/B終端held/C運用）の分離** | §6.1・§9.1 |
| **M-D4-04** テスト#1 期待値修正（startup は PENDING_RETRY 可視化のみ・再処理は GAS 再送） | §8.1 |
| **M-D4-05** テスト#3 期待値修正（敗者は receipt-state guard で rowcount 0＝`concurrent_reject`・UNIQUE(epoch) は監査副次） | §8.1・§9.1 |
| **M-D4-06** attempt 表の polymorphic 廃止 → **FK(receipt_id)＋ON DELETE CASCADE** | §2.3 |
| **M-D4-07 / L-D4-01 / 持ち越し OPEN 7 件** を §9 OPEN へ列挙（実装票の着手条件） | §9.2 |
| （前提）B-NEW-01 sortation 同期モデル・H-NEW-01〜04・M-01〜M-06・L-NEW-01（rev4） | §0/§2/§3/§4/§5/§6/§7/§8/§9 |
