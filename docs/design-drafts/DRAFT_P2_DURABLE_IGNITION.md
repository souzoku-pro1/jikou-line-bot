# DRAFT: P2 durable 点火 — 裁定材料（INBOUND_EVENT_DURABLE_ENABLED）

- TASK_ID: P2-CHAIN-007（READ_ONLY 調査＋DRAFT 起票・PC-A）／記録日 2026-07-20
- 調査 BASE: origin/main `7ff7775`（読取のみ・env/GAS/本番/secret 非接触）
- 状態: **DRAFT**（点火可否・時期は司令塔裁定。live 依存の未確定は「[人]確認」と明示）
- 設計正本: `docs/design-drafts/DRAFT_RV05_DURABLE_INBOUND.md`（rev5）

## 1. flag 参照箇所の全列挙と OFF/ON 分岐

grep 実出力（本番コード・test 除く）:
```
hub/durable_inbound.py:23:_FLAG = "INBOUND_EVENT_DURABLE_ENABLED"
main.py:93:  (startup)        os.environ.get("INBOUND_EVENT_DURABLE_ENABLED", ...)
main.py:568: (LINE webhook)   os.environ.get("INBOUND_EVENT_DURABLE_ENABLED", ...)
sortation_ingest.py:136: (_durable_enabled)  同上
```
| 箇所 | OFF（既定） | ON |
|---|---|---|
| `main.py:93`（startup） | `hub.durable_inbound` を **import せず一切実行しない**（M-06・env 直読み） | 放置 receipt の**可視化 reconciliation を 1 回実行（再処理しない）** |
| `main.py:568`（LINE webhook） | 現行挙動と **byte 同一** | `record_line_event` で inbound_event へ **durable 記録**（受理→200→既存 BackgroundTasks。**自動 replay なし＝Phase A**） |
| `sortation_ingest.py:434/480/562` | 縮退（登録/通知失敗は握って続行） | ingestion_receipt 台帳（冪等/可視化/fencing）・vendor 呼出前 `vendor_pre` marker・**H-04: ask 保存/通知失敗を握らず送出→5xx**（GAS「200 以外リネームせず自然リトライ」と整合） |

判定は 3 箇所とも同一（`{"1","true","on","yes"}`）。**OFF への rollback は env 1 本で即時・byte 同一**。

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
  - (c) **§3 経路 (A)＝滞留監視の実装・merge（fix2・唯一の必須ゲート）**。
    K4 は補助（あれば望ましいが点火条件ではない・§3）。
- **手順案（fix2・(A) 単独前提へ改訂）**:
  - **前段**: ①別票（§7・P2-CHAIN-012 想定）で received/processing 滞留監視を実装・
    merge → ②以降は共通手順へ。
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
  5xx が増える (iii) received/stale の沈黙滞留（§3・前提条件 (A)/(B) はこの遮断のため）。
- **rollback**: env OFF（1 本）→ **即時に現行挙動と byte 同一**（M-06 で import すらしない）。
  - **【fix1・P2DP-M01】「記録済み行は残置で無害（読み手なし）」は撤回**。実態:
    `check_journal_backlog` は **provider != kintone で LINE 行も読み**、残置行が
    processing/failed のままだと **24 時間後に Stripe runbook 文言の警報**が
    （`STRIPE_EVENT_JOURNAL_ENABLED=1` なら）発火する＝「読み手なし」ではない。
  - rollback 手順に追加: **残置行の状態確認と閉鎖（[人]確認・fix2 で安全側へ定義）**:
    - **received 行を根拠なく `done` へ更新することは禁止**（未処理イベントを処理済みに
      偽装する誤閉鎖の危険。`done` は「処理が正常終端した」の記録であり、閉鎖の道具に
      使わない）。
    - **安全側の閉鎖手順**: (i) received 行の内容確認（[人]・payload から対象イベントを
      特定）→ (ii) **実業務が別経路で処理済みと確認できた行のみ**手動で
      `failed_exhausted` へ（理由をメモ列等に記録・列がなければ運用メモに event id を
      記録）→ (iii) **確認不能な行は残置のまま監視対象として追跡**（無理に閉じない）。
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
