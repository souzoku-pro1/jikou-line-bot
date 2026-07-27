# DRAFT: P2 durable 点火 — 裁定材料（INBOUND_EVENT_DURABLE_ENABLED）

- TASK_ID: P2-CHAIN-007（READ_ONLY 調査＋DRAFT 起票・PC-A）／記録日 2026-07-20
- 調査 BASE: origin/main `7ff7775`（読取のみ・env/GAS/本番/secret 非接触）
- 状態: **DRAFT**（点火可否・時期は司令塔裁定。live 依存の未確定は「[人]確認」と明示）
- 設計正本: `docs/design-drafts/DRAFT_RV05_DURABLE_INBOUND.md`（rev5）

## 1. flag 参照箇所の全列挙と OFF/ON 分岐

grep 実出力（本番コード・test 除く・**fix2 L01: G 監視ゲート込みへ更新**）:
```
hub/durable_inbound.py:23:_FLAG = "INBOUND_EVENT_DURABLE_ENABLED"
                          （durable_enabled()＝record_line_event と check_line_backlog の二重ゲート）
main.py:93:  (startup)        os.environ.get("INBOUND_EVENT_DURABLE_ENABLED", ...)
main.py:568: (LINE webhook)   os.environ.get("INBOUND_EVENT_DURABLE_ENABLED", ...)
sortation_ingest.py:136: (_durable_enabled)  同上
daily_healthcheck.py (run_healthcheck・監視項目G ゲート)  env 直読みで同 flag を判定
```
| 箇所 | OFF（既定） | ON |
|---|---|---|
| `main.py:93`（startup） | `hub.durable_inbound` を **import せず一切実行しない**（M-06・env 直読み） | 放置 receipt の**可視化 reconciliation を 1 回実行（再処理しない）** |
| `main.py:568`（LINE webhook） | 現行挙動と **byte 同一** | `record_line_event` で inbound_event へ **durable 記録**（受理→200→既存 BackgroundTasks。**自動 replay なし＝Phase A**） |
| `sortation_ingest.py:434/480/562` | 縮退（登録/通知失敗は握って続行） | ingestion_receipt 台帳（冪等/可視化/fencing）・vendor 呼出前 `vendor_pre` marker・**H-04: ask 保存/通知失敗を握らず送出→5xx**（GAS「200 以外リネームせず自然リトライ」と整合） |
| `daily_healthcheck.py`（**監視項目G ゲート**・P2-CHAIN-012 で追加） | `hub.durable_inbound` を import せず G 検査を実行しない（M-06 同型） | `check_line_backlog`（LINE received/processing 滞留・env 閾値既定 1h）を日次実行 |

判定は全箇所同一（`{"1","true","on","yes"}`・check_line_backlog 内の
`durable_enabled()` も同値集合の二重検査）。**OFF への rollback は env 1 本で即時・
byte 同一**（ただし状態の完全復元ではない——残余は 03-common §12.1 RMC-M04/fix2 M03）。

## 2. 書込み経路と現在の稼働状況

- **InboundEvent（`hub/inbound_event.py`）**:
  - **Stripe journal（P1-005a）**: `main.py:1237 _stripe_journal_enabled()`＝**別 flag
    `STRIPE_EVENT_JOURNAL_ENABLED=1`**（本 flag と独立）。Phase 1 close 報告の
    「Stripe journal は既存稼働」と整合 — **live env で ON の実態は[人]確認**。
  - **LINE Phase A**: `record_line_event`（本 flag ON 時のみ・§1）。
- **IngestionReceipt（`hub/ingestion_receipt.py`）**: sortation の durable 経路（本 flag ON 時のみ）。
- **テーブルは migration 済み資産**: `20260711_a3ea96f2e1a8_inbound_event` /
  `20260711_f8ef81de70a5_inbound_event_claimed_at` / `20260714_c4f1a2b7d8e9_ingestion_receipt`
  （参考: `20260714_b7d3e1a9c2f4_signature_nonce` は S5 以降本番稼働中＝**alembic 適用が
  本番で回っている傍証**。ただし当該 3 本の適用実態は **[人]確認**（§6 前提条件 (a)））。

## 3. K4（redelivery）との依存関係

- 正本 §9.2 の逐語:
  > **K4（[人]/大野・前提条件）**: LINE 再配送設定確認（顧客Bot 自動 replay 有効化票=RV-06 後のブロッキング前提・§H-03 L3 も K4 後）。**Phase A（観測）は不要**。
- 正本の記述上、K4 が直接の前提となるのは **RV-06（顧客 Bot 自動 replay・別票）と
  §H-03 L3（完全差分照合）**。ただし——
- **【fix1・P2DP-H01】「K4 未確定でも分離点火可」の根拠は撤回する**。理由＝**現状の
  滞留検知の限界**（Codex 指摘・実装事実）:
  - `daily_healthcheck.check_journal_backlog` は **`STRIPE_EVENT_JOURNAL_ENABLED=1`
    依存**（別 flag。OFF なら LINE 行も含め何も見ない）。
  - 検知対象 state は **processing（24h 超 stale）と failed（24h 超）のみ**。
    **`received` の滞留は検知対象外**。
  - 具体例（Codex 指摘）: LINE webhook の **batch 途中 503**（複数 event 中の一部 insert 後に
    5xx）で、成功分の行が `received` のまま残る。再配送が無ければ（K4 未確認）誰も claim せず、
    **どの監視にも掛からず沈黙滞留**する。
  - ＝旧記述「観測 3 系列で検知されるため可視化は損なわれない」は**実態と不一致**であり修正:
    観測 3 系列（A 受理/B 終端 held/C 運用）は**カウンタ/ログ集計**であって、`received`
    滞留行を能動警報する仕組みではない。
- **点火の前提条件（fix2・司令塔裁定・唯一の前提＝経路 (A)）**:
  - **(A) 必須**: **received／processing の滞留監視の実装**（別票・§7 スコープ案）。
    監視要件（裁定そのまま）: **durable flag 配下で動作**・**received と processing の
    両 state を対象**・**閾値超過で LINE 警報**（既存 dead-man／notify 系の流儀）・
    **`STRIPE_EVENT_JOURNAL_ENABLED` に依存しない**。実装・merge 後に点火可。
  - **【fix2・P2DP2-H01】旧経路 (B)「K4 同時点火」は単独の点火条件から削除**（司令塔裁定）。
    理由: **K4 は非 2xx への再配送**であり、**200 ACK 後の BackgroundTask crash による
    processing 滞留を回収できない**（LINE は 200 を受け取っており再配送しない＝
    redelivery が来ない滞留クラスが残る）。K4 は「**あれば received（非 2xx 起因分）の
    一部を減らす補助**」に格下げし、**必須ゲートとしない**。
- 分離点火時のその他の影響（前提条件とは独立に不変）:
  - 二重返信は terminal state（done/failed_exhausted）skip で遮断（まれな stale 併走は
    正本の比較裁定どおり受容・検知可能）。
  - **挙動変化が出るのは sortation の H-04**（ask 保存/通知の失敗が縮退→5xx へ変わる）。
    5xx は GAS 自然リトライで回収される設計だが、点火直後は §6 の観測を要す。

## 4. 点火時のマイグレーション・env 変更の要否（名前のみ）

- **新規 migration: 不要**（必要 3 テーブルは §2 の既存 migration 資産。適用実態のみ[人]確認）。
- **env（値は非出力）**: 点火 = `INBOUND_EVENT_DURABLE_ENABLED` の 1 本。
  任意 tuning（**既定値あり・投入不要**）: `INBOUND_RECONCILE_STALE_SECONDS`（既定 4500）・
  `INBOUND_LINE_MAX_ATTEMPTS`（既定 5）・`INBOUND_LINE_STALE_PROCESSING_SECONDS`（既定 3600）。
- コード変更・GAS 変更: **不要**。

## 5. テスト充足状況

| 資産 | 件数 | ON 時挙動カバー |
|---|---|---|
| `test_rv05_13_durable.py` | 23 | **あり**（TestLineDurable／TestSortationDurable＝flag ON 系・TestFlagOffMechanical＝OFF 機械的同一・TestHandlerSmoke） |
| `test_inbound_event_stripe.py` | 11 | Stripe journal（別 flag）の遷移系 |
| `test_ingestion_receipt.py` | 14 | receipt 台帳・reconcile |
| `test_journal_backlog_check.py` | 9 | 台帳滞留の運用監視 |

＝ ON 時挙動はテスト済み。**点火にテスト追加は必須でない**（点火後の実測は §6 手順で担保）。

## 6. 裁定材料（点火の前提条件・手順案・リスク・rollback）

- **前提条件**:
  - (a) **[人]確認**: 本番 DB で inbound_event / ingestion_receipt / （claimed_at 列）の
    存在確認（alembic 適用実態）。
  - (b) **[人]確認**: `STRIPE_EVENT_JOURNAL_ENABLED` の live ON 実態（§2 の整合）。
  - (c) **【fix3・P2DP3-M02】点火ゲート＝「滞留検査関数」と「daily_healthcheck 結線」の
    両方が merge 済み**（司令塔裁定）。結線を別票に分けた場合は**両票完了まで点火不可**
    （検査関数だけでは警報が発火しない＝ゲート未充足）。
    K4 は補助（あれば望ましいが点火条件ではない・§3）。
- **手順案（fix2・(A) 単独前提へ改訂）**:
  - **前段**: ①別票（§7・P2-CHAIN-012 想定）で received/processing 滞留監視の
    **検査関数＋daily_healthcheck 結線の両方**を実装・merge（分票時は両票完了まで
    進まない・fix3）→ ②以降は共通手順へ。
  - **共通手順**:
    1. [人] `INBOUND_EVENT_DURABLE_ENABLED` を ON → デプロイ緑。
    2. startup ログで `[RV05] startup reconcile` の出力を実見（[人]）。
    3. LINE テスト発話 1 件 → 応答正常＋inbound_event に provider="line" の行が
       terminal（done）へ到達（[人]実見）。
    4. sortation テスト 1 件 → 200＋receipt 行・従来どおり `[照会中]`/`[済]`（[人]実見）。
    5. 観測 3 系列（A 受理/B 終端 held/C 運用）のカウンタ初期値を記録＋
       滞留監視の警報 0 を確認。
- **リスク**: (i) sortation H-04 の 5xx 化（自然リトライで回収・点火直後は観測強化）
  (ii) DB 到達不能時は durable 経路が fail 側（握らない設計）＝DB 障害が可視化される反面
  5xx が増える (iii) received/stale の沈黙滞留（§3・前提条件 (A) はこの遮断のため）。
- **rollback**: env OFF（1 本）→ **即時に現行挙動と byte 同一**（M-06 で import すらしない）。
  - **【fix1・P2DP-M01】「記録済み行は残置で無害（読み手なし）」は撤回**。実態:
    `check_journal_backlog` は **provider != kintone で LINE 行も読み**、残置行が
    processing/failed のままだと **24 時間後に Stripe runbook 文言の警報**が
    （`STRIPE_EVENT_JOURNAL_ENABLED=1` なら）発火する＝「読み手なし」ではない。
  - rollback 手順に追加: **残置行の状態確認と閉鎖（[人]確認・fix2 で安全側へ定義）**:
    - **received 行を根拠なく `done` へ更新することは禁止**。
      **【fix3・P2DP3-M01 裁定】`done` は「照合源による根拠がある場合のみ許可」**へ変更
      （管理終端の新 state は作らない＝migration 回避・裁定）。
    - **【fix4・P2DP4-M01 裁定】`failed_exhausted` の定義統一**:
      **「再試行を行わないことが確定した打切り」**。
      - **自動**: `attempts >= max`（実装済みの遷移・retry_exhausted 系）。
      - **手動**: **再配送終了済みで再処理見込みなしと [人] が判断した打切り**
        （runbook work-log 7/15 §4.4 (b) の既存手順と整合）。
      - **区別（運用案）**: 手動遷移時は `last_error` を固定分類（例: `manual_closed`）へ
        更新し、自動上限（retry_exhausted 系）と識別可能にする（分類のみ・本文非搭載の
        D17 流儀維持。分類値の実装確定は監視票 P2-CHAIN-012 かその後続）。
      - **runbook との整合（注記）**: fix3 の「attempts 上限に**限定**」という表現は、
        runbook §4.4 (b) が既に認める**手動打切り**（exhausted=true でない行の
        「再配送終了済み・再処理見込みなし」判断）と矛盾していたため、本定義へ差し替える。
        runbook 自体は **merge 済み歴史記録のため変更しない**（司令塔裁定）。
      - **不変の核（fix3 維持）**: 照合源で正常処理済みを証明できた行は `done`（根拠必須）。
        **「処理済み」の行を `failed_exhausted` に入れることは引き続き禁止**。
    - **【fix3・P2DP3-H01】「payload から対象イベントを特定」は撤回**。実装事実:
      Phase A は **raw payload を保存しない** — `hub/inbound_event.py` の保存列は
      provider／external_event_id／caller_id／dedup_key／**payload_hash（SHA-256 のみ）**／
      event_type／signature_result／received_at／state／processed_at／attempts／
      last_error（分類のみ・本文/PII/vendor 生値なし）／claimed_at で、payload 本文の列は
      存在しない（`hub/durable_inbound.py` 冒頭「PII/本文/payload 非混入」・
      work-log 2026-07-15_RV-05-13-fix5 の D17/RCF-M05 流儀とも整合）。
    - **照合源は「実在するもののみ」（裁定）**: `external_event_id`（LINE
      webhookEventId）・既存の構造化ログ・LINE 側の配信記録等。
      **照合源が存在しない場合は「確認不能として残置」が唯一の扱い**。
    - **安全側の閉鎖手順（fix3 改訂）**: (i) received 行の `external_event_id` 等を上記
      照合源と突合（[人]）→ (ii) **照合源で正常処理済みを証明できた行のみ**手動で
      `done` へ（[人]個別判断・理由を運用メモに event id とともに記録）→
      (iii) **証明できない行は残置のまま監視対象として追跡**（無理に閉じない）。
    - **注意（fix2）**: runbook work-log 2026-07-15_RV-05-13-fix5 §4.1 の SQL は
      **`state='processing'` 専用**（claimed_at 判定を含む）で、**received には
      そのまま使えない**。received 用の参照 SQL（**SELECT のみ**・UPDATE は [人] の
      個別判断）:
      ```sql
      SELECT id, external_event_id, attempts, received_at
      FROM inbound_event
      WHERE provider = 'line' AND state = 'received'
      ORDER BY received_at;
      ```
    - processing 残置の reset は従来どおり runbook §4 に準拠。

## 7. 経路 (A) 監視実装の別票スコープ案（P2-CHAIN-012 想定・fix2）

- **実装対象**: `hub/durable_inbound.py`（または専用 module）に滞留検査関数を新設。
  - durable flag（`INBOUND_EVENT_DURABLE_ENABLED`）配下でのみ動作（OFF は完全 no-op・
    M-06 流儀＝env 直読みで import 不発）。
  - 対象: `provider='line'` の **received（閾値超）と processing（stale 閾値超）** の両方。
    **`STRIPE_EVENT_JOURNAL_ENABLED` に依存しない**（check_journal_backlog とは独立の
    専用検査・既存 Stripe 監視の挙動を変えない）。
  - 閾値: received 用の専用 env（名前のみ・既定値つき）を新設し、processing は既存
    `INBOUND_LINE_STALE_PROCESSING_SECONDS` と整合させる。
- **警報結線**: daily_healthcheck の problems へ合流（既存 dead-man／notify 系の流儀・
  LINE 警報・D17 流儀＝件数と PK のみ・payload/eventID 非搭載）。
- **テスト**: flag OFF no-op／received 滞留検知／processing stale 検知／閾値境界／
  Stripe flag 非依存（OFF でも検知）／警報文面の redaction（PK・件数のみ）。
- **既存テストへの非波及方針**: 新設関数＋新規テストファイルのみ。
  `check_journal_backlog`・既存 `run_healthcheck` 系テストは無変更
  （結線行の追加が既存テストを壊す場合は P2HC-fix1 と同型で「結線は別票」に分離）。
- **【fix3・P2DP3-M02】結線を別票に分離した場合でも、点火ゲートは
  「検査関数＋結線の両方 merge 済み」のまま**（§6 前提条件 (c) と同一。結線票の完了までは
  警報が発火しないため、検査関数のみの merge では点火できない）。

## 8. 点火 runbook（MAIN-CONS-fix1・R-DEPLOY-READINESS-1 反映）

R-DEPLOY-READINESS-1（対象 main `9e5708e`）の観測一覧を採録し、§6 手順案を
実施形へ具体化する。**P0 は全項目必須**・P1/P2 は観測継続。

### 8.0 更新注記（RMC-M01 との整合）

§6 rollback の fix1 記述「check_journal_backlog は provider != kintone で LINE 行も
読む」は **MAIN-CONS-fix1（RMC-M01 裁定）で解消**——E 系は provider='stripe' 限定と
なり、**flag OFF 後の残置 LINE 行はどの監視にも載らない（無監視）**。したがって
rollback 時の残置行の照合・手動閉鎖（§6・03-common §12.1 RMC-M04）は**必須手順**である
（「24h 後に Stripe 文言で警報が出るから気づける」という消極的検知はもう無い）。

### 8.1 P0（必須ゲート・fix2 H01/H02/H03/M02 改訂）

**(a) 点火前（[人]・read-only 機械検査）** — **SELECT のみ・DDL/DML 混入禁止**。
期待集合との**機械照合で不足があれば非0終了＝点火中止**。**secret 値は表示しない**
（flag 値・スキーマ名等の非 secret は表示可。DATABASE_PUBLIC_URL 経由・repo 直下で実行）:

```bash
cd /c/work/jikou-line-bot

# 1) migration 適用実態（H03: current と heads の単一一致を機械判定・不一致は exit 1）
railway run python - << 'PY'
import os, sys
os.environ['DATABASE_URL'] = os.environ['DATABASE_PUBLIC_URL']
import sqlalchemy as sa
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
heads = set(ScriptDirectory.from_config(Config('alembic.ini')).get_heads())
url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
with sa.create_engine(url).connect() as c:
    current = set(MigrationContext.configure(c).get_current_heads())
print('heads(code) =', sorted(heads)); print('current(db) =', sorted(current))
if len(heads) != 1 or current != heads:
    print('NG: current と heads が単一一致しない → 点火中止'); sys.exit(1)
print('OK: alembic current == heads（単一）')
PY

# 2) 必須 3 table = inbound_event / ingestion_receipt / processing_attempt（H01）。
#    table/column/constraint/index を期待集合と機械照合（H02: table_schema =
#    current_schema() でアプリ schema へ限定・不足は exit 1）。
#    signature_nonce は追加確認（存在のみ・欠落は警告表示に降格）
railway run python - << 'PY'
import os, sys, asyncio, sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
EXPECT_COLS = {
    'inbound_event': {'id','provider','external_event_id','caller_id','dedup_key',
                      'payload_hash','event_type','signature_result','received_at',
                      'state','processed_at','attempts','last_error','claimed_at'},
    'ingestion_receipt': {'id','source_file_id','source_sha256','ingest_type',
                          'caller_id','case_hint','first_seen_at','last_outcome',
                          'downstream_refs','idempotency_key','epoch',
                          'last_heartbeat_at'},
    'processing_attempt': {'id','receipt_id','epoch','attempted_at','phase','outcome'},
}
EXPECT_UNIQUE_NAMES = {('processing_attempt', 'uq_processing_attempt_receipt_epoch')}
EXPECT_UNIQUE_COLS = {('inbound_event', 'dedup_key'),
                      ('ingestion_receipt', 'idempotency_key')}
url = os.environ['DATABASE_PUBLIC_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
async def main():
    ng = []
    eng = create_async_engine(url)
    async with eng.connect() as c:
        for t, cols in EXPECT_COLS.items():
            r = await c.execute(sa.text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() AND table_name = :t"), {'t': t})
            got = {row[0] for row in r}
            if not got:
                ng.append(f'table 不在: {t}')
            elif not cols <= got:
                ng.append(f'{t} 列不足: {sorted(cols - got)}')
        r = await c.execute(sa.text(
            "SELECT table_name, constraint_name FROM information_schema.table_constraints "
            "WHERE table_schema = current_schema() AND constraint_type = 'UNIQUE'"))
        uniq_names = {(row[0], row[1]) for row in r}
        for t, name in EXPECT_UNIQUE_NAMES:
            if (t, name) not in uniq_names:
                ng.append(f'UNIQUE 不足: {t}.{name}')
        for t, col in EXPECT_UNIQUE_COLS:
            r = await c.execute(sa.text(
                "SELECT 1 FROM information_schema.constraint_column_usage ccu "
                "JOIN information_schema.table_constraints tc "
                "  ON tc.constraint_name = ccu.constraint_name "
                " AND tc.table_schema = ccu.table_schema "
                "WHERE ccu.table_schema = current_schema() AND ccu.table_name = :t "
                "  AND ccu.column_name = :c AND tc.constraint_type = 'UNIQUE'"),
                {'t': t, 'c': col})
            if not r.first():
                ng.append(f'UNIQUE 不足: {t}.{col}')
        # FK 完全照合（fix3 H01(b): 子列・参照先 table.列・delete rule の3点を機械照合）
        r = await c.execute(sa.text(
            "SELECT kcu.column_name, ccu.table_name, ccu.column_name, rc.delete_rule "
            "FROM information_schema.referential_constraints rc "
            "JOIN information_schema.table_constraints tc "
            "  ON tc.constraint_name = rc.constraint_name "
            " AND tc.table_schema = rc.constraint_schema "
            "JOIN information_schema.key_column_usage kcu "
            "  ON kcu.constraint_name = rc.constraint_name "
            " AND kcu.constraint_schema = rc.constraint_schema "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON ccu.constraint_name = rc.constraint_name "
            " AND ccu.constraint_schema = rc.constraint_schema "
            "WHERE rc.constraint_schema = current_schema() "
            "  AND tc.table_name = 'processing_attempt' "
            "  AND tc.constraint_type = 'FOREIGN KEY'"))
        fks = {tuple(row) for row in r}
        if ('receipt_id', 'ingestion_receipt', 'id', 'CASCADE') not in fks:
            ng.append('FK 不一致: processing_attempt.receipt_id → ingestion_receipt.id '
                      f'ON DELETE CASCADE を満たさない（実測: {sorted(fks)}）')
        # index 完全照合（fix3 H01(a): 名前＋対象列を pg_indexes.indexdef と機械照合）
        r = await c.execute(sa.text(
            "SELECT tablename, indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = current_schema() "
            "AND tablename IN ('inbound_event','ingestion_receipt','processing_attempt')"))
        idx = {(row[0], row[1]): row[2] for row in r}
        import re as _re
        def _idx_cols(indexdef):
            m = _re.search(r'\(([^)]*)\)', indexdef)
            return [x.strip().strip('"') for x in m.group(1).split(',')] if m else []
        EXPECT_INDEXES = [   # (table, index名, 対象列の順序列)
            ('inbound_event', 'ix_inbound_event_provider_state', ['provider', 'state']),
            ('inbound_event', 'ix_inbound_event_received_at', ['received_at']),
            ('ingestion_receipt', 'ix_ingestion_receipt_last_outcome', ['last_outcome']),
        ]
        for t, name, cols in EXPECT_INDEXES:
            d = idx.get((t, name))
            if d is None:
                ng.append(f'index 不足: {t}.{name}')
            elif _idx_cols(d) != cols:
                ng.append(f'index 列不一致: {t}.{name} 期待={cols} 実測={_idx_cols(d)}')
        # UNIQUE 系 index（auto 生成名があり得るため列で照合・UNIQUE 指定を必須とする）
        EXPECT_UNIQUE_INDEXES = [
            ('inbound_event', ['dedup_key']),
            ('ingestion_receipt', ['idempotency_key']),
            ('processing_attempt', ['receipt_id', 'epoch']),
        ]
        for t, cols in EXPECT_UNIQUE_INDEXES:
            hit = any(it == t and 'UNIQUE' in d.upper() and _idx_cols(d) == cols
                      for (it, _n), d in idx.items())
            if not hit:
                ng.append(f'UNIQUE index 不足: {t} {cols}')
        print('indexes:', sorted(n for (_t, n) in idx))
        # 追加確認: signature_nonce（存在のみ・HMAC nonce は点火とは独立の基盤）
        r = await c.execute(sa.text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = 'signature_nonce'"))
        if not r.first():
            print('警告: signature_nonce 不在（点火ゲートではないが確認を推奨）')
    await eng.dispose()
    if ng:
        print('NG:'); [print(' ', x) for x in ng]; sys.exit(1)
    print('OK: 必須 3 table の table/column/constraint 照合一致')
asyncio.run(main())
PY

# 3) STRIPE_EVENT_JOURNAL_ENABLED の確認（flag 値のみ・secret 値は表示しない）
railway run python -c "import os; print('STRIPE_EVENT_JOURNAL_ENABLED =', os.environ.get('STRIPE_EVENT_JOURNAL_ENABLED'))"

# 4) baseline 記録（M02: 点火前の provider/state 別件数・最古時刻。点火後差分の基準）
railway run python - << 'PY'
import os, asyncio, sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
url = os.environ['DATABASE_PUBLIC_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
async def main():
    eng = create_async_engine(url)
    async with eng.connect() as c:
        r = await c.execute(sa.text(
            "SELECT provider, state, count(*), min(received_at) FROM inbound_event "
            "GROUP BY provider, state ORDER BY 1, 2"))
        print('inbound_event baseline:'); [print(' ', tuple(row)) for row in r]
        r = await c.execute(sa.text(
            "SELECT last_outcome, count(*) FROM ingestion_receipt "
            "GROUP BY 1 ORDER BY 1"))
        print('ingestion_receipt baseline:'); [print(' ', tuple(row)) for row in r]
    await eng.dispose()
asyncio.run(main())
PY
```

**(b) deploy 直後（必須ゲート）**:
- **`[RV05] startup reconcile` の成功ログを実見すること（必須）。
  「skipped」が出た場合は点火中止**（flag が効いていない／DB 未到達のいずれか。
  原因確定まで OFF に戻す）。
- 起動 traceback なし・`Application startup complete`・`/health` 200（WebFetch）。

**(c) LINE スモーク（M02: baseline との差分確認＋fix3 M01: 対象一意化）**:
テスト発話 1 件 → 応答正常＋(a)-4 の baseline に対し **provider='line' の行が 1 件だけ
増加**・その行が **attempts=1・claimed_at 非 NULL**・received→processing→**done の
terminal 到達**を実見（§6 手順 3。行が 2 件以上増えた/attempts>1 は二重記録の疑い＝調査）。
**対象行の一意化（read-only 事後 SELECT）**: テストイベントの `external_event_id`
（LINE webhookEventId・増分行から特定）で限定して確認する:

```sql
-- read-only（SELECT のみ）。:ev はテストイベントの webhookEventId
SELECT id, state, attempts, (claimed_at IS NOT NULL) AS claimed,
       received_at, processed_at
FROM inbound_event
WHERE provider = 'line' AND external_event_id = :ev;
-- 期待: 1 行のみ・state='done'・attempts=1・claimed=true
```

**(d) sortation スモーク（M02＋fix3 M01: 対象一意化）**: signed lane 1 件 → 200＋
receipt 行＋従来どおり `[照会中]`/`[済]` リネーム（§6 手順 4）。加えて
**processing_attempt に当該 receipt の遷移行が記録され、(receipt_id, epoch) が一意**
（uq_processing_attempt_receipt_epoch 違反なし）であることを、**テストファイルの
source file ID で限定した read-only 事後 SELECT** で確認する:

```sql
-- read-only（SELECT のみ）。:fid はテスト投入した Drive file ID
SELECT id, last_outcome, epoch FROM ingestion_receipt WHERE source_file_id = :fid;
-- 期待: 1 行のみ・last_outcome が正常終端・epoch >= 1
SELECT receipt_id, epoch, phase, outcome FROM processing_attempt
WHERE receipt_id = (SELECT id FROM ingestion_receipt WHERE source_file_id = :fid)
ORDER BY epoch;
-- 期待: (receipt_id, epoch) の重複なし・遷移が記録されている
```

**記録（fix3 M01）**: (a)-4 の baseline 実出力と (c)(d) の事後 SELECT 実出力は
**同一の work-log（点火当日の作業記録）へ並べて保存**する（差分の突合を後から
再検証できる形にする。出力は件数・state・時刻のみで PII を含まない）。

### 8.2 P1（点火後 24h の観測）

- 監視項目G（LINE received/processing 滞留・既定 1h 閾値）の**警報 0**。
- 日次死活監視の通知に新規異常なし・LINE 応答遅延の苦情/異常なし。
- `[RV05]` 系ログにエラーなし・5xx 増加なし（sortation H-04 の 5xx 化は自然リトライで
  回収されることを確認・§6 リスク (i)）。

### 8.3 P2（1 週間の観測）

- inbound_event の state 分布（received/processing 残置 0・done 率）を週次で確認。
- `failed_exhausted` の新規発生 0（発生時は runbook 2026-07-15_RV-05-13-fix5 §4）。
- 問題なければ観測強化を解除し、TRACKING_PRE_DEPLOY_CHECKS #3（G 系境界値の PG 実測）を
  この観測ウィンドウ内で消化する。
