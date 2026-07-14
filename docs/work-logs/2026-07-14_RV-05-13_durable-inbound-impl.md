# 作業記録 2026-07-14: RV-05-13 durable InboundEvent/IngestionReceipt 実装

- Phase/Gate: Phase 1・G1（process-memory critical queue 廃止・durable commit 前に成功 ACK を返さない）
- 正本: `docs/design-drafts/DRAFT_RV05_DURABLE_INBOUND.md`（rev5）に厳密準拠
- flag: `INBOUND_EVENT_DURABLE_ENABLED`（既定 OFF＝現行挙動と byte 同一）
- 全 suite: **1,347 passed / FAIL 0 / skip 0**（baseline 1,328＋新規19）

## 1. 実装物

| 対象 | ファイル | 内容 |
|---|---|---|
| ledger core | `hub/ingestion_receipt.py` | epoch fencing 状態機械（H-D4-01 単一 UPDATE 統一・H-D4-02 receipt 状態正本・attempt 監査専用・冪等キー length-prefix・duplicate_suspect・convergence 集計 M-D5-01） |
| migration | `alembic/versions/20260714_c4f1a2b7d8e9_ingestion_receipt.py` | `ingestion_receipt`＋`processing_attempt`（FK/ON DELETE CASCADE・既存表 ALTER なし） |
| flag/観測/LINE | `hub/durable_inbound.py` | flag・3系列カウンタ（emit 契約）・LINE Phase A 記録（record_line_event・coarse observe） |
| LINE 結線 | `main.py` webhook | flag ON: durable insert→200→BackgroundTasks（`_process_line_event_durable` wrap・**本体不変**）・startup reconciliation（flag ON のみ） |
| sortation 結線 | `sortation_ingest.py` | flag ON: 受理→claim→vendor_pre→SENDING→completed/PENDING_RETRY・claim で二重 forward 回避（shadow・応答 shape 不変） |
| tests | `test_ingestion_receipt.py`（12）・`test_rv05_13_durable.py`（9） | §8 条件表・flag OFF 無変更・handler smoke・M-01 機械的担保 |

## 2. DRAFT 条項 → 実装 対応表

| DRAFT | 実装 |
|---|---|
| §2.2/2.3 schema・§2.7 migration | `hub/ingestion_receipt.py`・migration c4f1a2b7d8e9・env.py 統合 |
| §2.4 冪等キー（length-prefix・NULL 拒否・衝突→duplicate_suspect） | `build_idempotency_key`・`upsert_receipt`（ReceiptConflict） |
| §B-02 H-D4-01 単一 UPDATE パターン・fence・DB clock | `_transition`（epoch=epoch+1 統一）・claim/mark_terminal/mark_pending_retry/reconcile_stale/manual_reset・heartbeat（非遷移 fence） |
| §3.1 LINE ACK 順序（durable→200→BG・DB 停止 5xx） | main webhook（flag ON・record_line_event 例外→503） |
| §3.2 sortation 同期・SENDING marker・PENDING_RETRY | sortation_ingest（vendor_pre/SENDING・失敗→mark_pending_retry→再送出 5xx） |
| §4 consumer なし・startup 可視化のみ | 非同期 consumer 未実装・`reconcile_stale`（startup・可視化遷移のみ） |
| §5.1 provider 別状態機械（Stripe 不変） | ST_* 定数・Stripe 経路未接触 |
| §6 観測性（3系列・emit・distinct 集計） | `count(series,...)`・`convergence_stats`（M-D5-01 集計クエリ） |
| §7 flag OFF 機械的担保 | `durable_enabled()` 最上段短絡・startup hook flag ON のみ・source 検査テスト |
| §8 テスト条件表 | test_ingestion_receipt（#1〜#4,#7）・test_rv05_13_durable（LINE/sortation/flag OFF/smoke） |

## 3. 修正前 FAIL 実測（現行の沈黙喪失）

現行（flag OFF＝BackgroundTasks）は受理 event を durable に残さず、背景処理 crash で**痕跡ゼロで沈黙喪失**する。flag ON は durable 記録で検知可能。独立再現スクリプト（追跡 .py に置かない・RV-04b 規律）:

```python
# 一時ファイルで実行（TestClient(raise_server_exceptions=False)）
# 署名付き LINE webhook を POST し、背景 _process_line_event を crash させる。
os.environ.pop("INBOUND_EVENT_DURABLE_ENABLED", None)   # flag OFF（現行）
client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
print("flag OFF:", rows())     # inbound_event 行を照会
os.environ["INBOUND_EVENT_DURABLE_ENABLED"] = "1"       # flag ON（本実装）
client.post("/webhook", content=body, headers={"X-Line-Signature": sig})
print("flag ON:", rows())
```

**実出力（実測・2026-07-14）**:
```
flag OFF（現行）: webhook 200 後・背景 crash → inbound_event 行 = [] （痕跡ゼロ＝沈黙喪失）
flag ON （本実装）: 同上 → inbound_event 行 = [('line', 'failed')] （received→failed で可視化＝検知可能）
結論: 現行は喪失が痕跡ゼロで沈黙（HOTFIX-01 型）。本実装は durable 記録で検知可能。
```
（自動テスト側は `test_rv05_13_durable.py::TestLineDurable::test_flag_on_background_crash_marks_failed` と `test_flag_off_no_durable_record` で恒久固定。）

## 4. §9.2 OPEN 7件の実装時裁定（記録）

1. **stale 判定閾値**: `INBOUND_RECONCILE_STALE_SECONDS`（env）・既定 **600 秒**（下記 M-D5-02）。
2. **並行再送応答 409 vs 200**: sortation は claim 敗者を **二重 forward 回避で forward せず、応答自体は 200**（kintone 冪等で再処理安全・GAS 無変更を優先）。明示 409 は将来 GAS がハンドルできる段で。→ 実装は 200＋forward 抑止。
3. **reconciliation 起動契機**: **startup 1回**のみ実装（定期 tick は未実装＝継続 worker を作らない原則を厳守）。定期化は別票（軽量 sweep=単発 UPDATE）。
4. **attempt 表 retention**: ON DELETE CASCADE で receipt 削除に追随。retention 期限は P16（未実装・OPEN 継続）。
5. **duplicate_suspect 粒度**: `source_sha256` 一致かつ `case_hint` 相違を衝突とする（`upsert_receipt` の same 判定）。厳密化（file_id 条件等）は運用データを見て別票。
6. **failed 上限（attempt_count）**: 本実装は自動 failed 上限を**未設定**（sortation は GAS 再送主体・completed/PENDING_RETRY で回る）。上限導入は別票。
7. **LINE 滞留閾値**: 観測（processing 滞留）は §6 の集計に委ね、alert 閾値は未数値化（暫定監視値は運用で調整）。RV-06 で durable replay 化まで暫定。

## 5. M-D5-02 lease 時間の根拠

- `reconcile_stale_seconds` 既定 = **600 秒（10 分）**。sortation の外部 call（Vision OCR＋Claude 判定＋LINE forward）は実測で通常数秒〜十数秒、分割/大 PDF でも数十秒。10 分は**最大外部 call 時間より十分長く**、生存中の request を誤って stale 扱いしない余裕を持つ。
- **外部 call 中の heartbeat は実装しない**（M-D5-02 の裁定どおり）: 同期モデルでは request が生きている限り処理は進み、10 分の長 lease で代替する。heartbeat が要るほど長い処理が観測されたら閾値調整 or heartbeat 導入（OPEN）。

## 6. スコープ・逸脱・持ち帰り

- **L-D5-01**: phase 表記（vendor_pre/SENDING）は全て `receipt.last_outcome` の atomic 遷移として実装（attempt.phase は監査コピー）。
- **L-D5-02**: duplicate_suspect も epoch++ の atomic UPDATE（`upsert_receipt`→`_transition`・case_hint 比較を same 判定に含む）。
- **LINE 細分封筒は未実装（STOP 相当・DO_NOT_CHANGE 準拠）**: `reply_fail`/`no_reply_intended`/`reply_fallback_ok` の区別は `_process_line_event` **本体**が outcome を返す必要があり、DO_NOT_CHANGE「顧客Bot返信生成ロジック本体」に抵触するため**実装しない**。本実装は coarse observe（received→processing→completed/failed）で **HOTFIX-01 型の背景全滅を可視化**する（RCF-M10 の主目的＝沈黙検知を達成）。細分は RV-06 で本体に薄い outcome hook を入れる別票。
- **sortation の「completed 重複に cached response を返す」は未実装（DRAFT の response-reconstruction OPEN）**: receipt はレスポンスを保存しないため、重複再送は kintone 冪等の再処理で同一 shape 応答を返す（shadow 台帳）。応答キャッシュ化は別票。
- **BASE 齟齬**: DRAFT branch（`docs/rv05-durable-inbound-draft`）が main 未マージのため、本実装は DRAFT branch から分岐（feature branch が DRAFT＋実装を束ねる）。マージ運用は司令塔裁定。

## 7. 枠消化の日次一行
- 2026-07-14: RV-05-13 実装（ledger/fencing・migration・LINE Phase A・sortation 同期台帳・flag OFF 既定・§8 テスト・修正前 FAIL 実測）。開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
