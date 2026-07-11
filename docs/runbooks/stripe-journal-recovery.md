# Runbook: Stripe journal（inbound_event）滞留の復旧

- 対象: 日次死活監視の「journal滞留」警報（監視項目E・P1-005d）
- 前提知識: journal の状態機械は processing → done / failed。
  実行中(15分以内)の重複配送は503でStripeに再送させ、15分超のprocessing残置は
  次のStripe再送が自動回収（stale再claim）する。**Stripeの自動再送は最大3日**。
  この runbook が必要になるのは「24時間以上残留」の警報時のみ。
- 実行者: 大野のみ（DB書き込み・Stripe Dashboard操作を含むため）。
  PC-A/Claude Code はこの runbook の SQL/操作を裁定なしに実行しない。

## 1. 滞留行の確認（読み取り）

Railway CLI から psql で接続（値は表示されるため画面共有時は注意）:

```
railway connect Postgres
```

滞留行の確認クエリ（そのまま貼り付け可）:

```sql
-- processing の24時間超残留（claimed_at NULL は列追加前の行）
SELECT id, provider, event_type, state, attempts,
       received_at, claimed_at, last_error
FROM inbound_event
WHERE state = 'processing'
  AND (claimed_at IS NULL OR claimed_at < now() - interval '24 hours')
ORDER BY id;

-- failed の24時間超残留
SELECT id, provider, event_type, state, attempts,
       received_at, processed_at, last_error
FROM inbound_event
WHERE state = 'failed'
  AND received_at < now() - interval '24 hours'
ORDER BY id;
```

該当行の `external_event_id`（evt_…）が Stripe Dashboard での特定キー:

```sql
SELECT id, external_event_id FROM inbound_event WHERE id IN (対象PK);
```

## 2. processing 残置の復旧（Stripe 手動再送）

原因の典型: 処理中クラッシュ後、Stripe の自動再送が終わっていた/届いていない。

1. Stripe Dashboard → 開発者 → Webhook → 対象エンドポイント → イベント一覧で
   `external_event_id`（evt_…）を検索（**イベントは15日以内なら再送可能**）
2. 「再送信」を実行
3. 期待される流れ: 残置行が15分窓を超えていれば**再claim→再処理→done**。
   万一15分以内なら503が返る→15分後にもう一度再送
4. 再送後、§1のクエリで該当PKが `done` になったことを確認

## 3. failed 滞留の復旧

failed は「kintone POST が失敗し続けた」行。**再送の前に必ず App 21 を照合**
（ACK不明のケースでは kintone 側にレコードが既にできていることがある。
再処理経路は自動で照合するが、人手確認でも同じ順序を守る）:

1. kintone App 21 を「Stripe決済ID」で検索（値は §1 の行に紐づく決済ID。
   Stripe Dashboard のイベント詳細 `data.object.id`＝cs_… でも確認可）
2. **レコードあり** → 起票済み。行を done に閉じる（§4のUPDATE・POSTは不要）
3. **レコードなし** → §2 と同じ手順で Stripe から手動再送
   （再処理経路が照合→未起票を確認→POST→done まで自動で行う）
4. `last_error` が HTTPStatusError 以外（例: JournalRowMissing）の場合は
   復旧より先に司令塔へ報告（構造異常の可能性）

## 4. 3日超過（自動再送終了後）かつ Dashboard 再送も不可の場合

イベントが15日を超えて再送不能、または再送しても失敗し続ける場合の最終手段。

1. Stripe Dashboard のイベント詳細から顧客名・メール・決済ID（cs_…）を確認
2. kintone App 21 に**手動で**レコード起票（フィールド: 顧客名/メールアドレス/
   Stripe決済ID/入金状況=入金済み/ステータス=決済完了 — /webhook/stripe の
   自動起票と同一内容にする）
3. journal 行を手動で done に閉じる（**実行は大野のみ**）:

```sql
UPDATE inbound_event
SET state = 'done',
    processed_at = now(),
    last_error = 'manual_recovery'
WHERE id = 対象PK
  AND state IN ('processing', 'failed');  -- 条件付き（誤爆防止）
-- 実行後 rowcount が対象件数と一致することを確認
```

4. 対応内容を work-log に1行残す（PK・対応日・手動起票の有無）

## 5. 予防・関連

- 監視: 日次死活監視（毎朝7:00 JST）が24時間超の残留を業務LINEに警報
- 15分窓の調整: env `STALE_PROCESSING_MINUTES`（既定15）
- journal 自体の無効化（緊急時）: `STRIPE_EVENT_JOURNAL_ENABLED` を外す
  （従来挙動に完全復帰。ただしdedupも失われる点に注意）
