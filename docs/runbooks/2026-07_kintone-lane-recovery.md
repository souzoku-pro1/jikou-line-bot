# Runbook: kintone レーン滞留・失敗の人手リカバリ（RV-04c）

- 対象: `inbound_event` の provider="kintone" 行（承認キュー App 29 webhook・dedup flag ON）。
- 正本設計: `docs/design-drafts/DRAFT_RV04C_CALLER_MIGRATION.md` §4.2 / §4.2b。
- 警報元: daily_healthcheck（7:00 JST）——**kintone滞留(未処理)** / **kintone失敗** の 2 文言。
- 前提: kintone webhook は**再送しない**ため、サーバ単独の自動再処理はない。回復の実体は
  **人手でレコードを再操作**（新しい webhook `id` で再配信させる）＝§4.2 の裁定どおり。
  **機械は状態の提示まで・自動再送はしない**。

## 0. state と意味（主要 state 3 値・§4.2）

| state | 意味 | 再操作の可否 |
|---|---|---|
| `received` | claim 済み・送信未着手（marker 前で滞留） | **安全に再操作可**（送信は確実に起きていない） |
| `sending` | 送信着手 marker 後で滞留 | **「送信済みの可能性あり」**（§2 の 3 点突合で判別） |
| `failed` | marker 前の確定失敗（transient get_record 失敗等・未送信確定） | **安全に再操作可** |
| `done` | 完了 or 正常 no-op（last_error で判別: NULL=送信完了 / `skip_*`=no-op） | 不要 |

## 1. 対象特定（READ_ONLY・PC-A 実施可）

```sql
-- 滞留（未処理）: received/sending
SELECT id, state, external_event_id, received_at, last_error
FROM inbound_event
WHERE provider='kintone' AND state IN ('received','sending')
  AND received_at < now() - interval '1 hour'   -- KINTONE_STALE_EVENT_HOURS
ORDER BY received_at;

-- 失敗: failed（分類は last_error＝get_record_error_<status> 等）
SELECT id, external_event_id, received_at, last_error
FROM inbound_event
WHERE provider='kintone' AND state='failed'
ORDER BY received_at;
```

## 2. `sending` の判別（3 点突合・§4.2 D3/D4）

`sending` 滞留は「LINE 送信済みかもしれない」状態。以下 3 点を突き合わせて送信有無を判別する:

1. **App 29 の「送信済み」フィールド** — yes なら送信＋フラグ更新まで到達済みの可能性大。
2. **LINE 側の実受信** — 該当ユーザーのトークに当該回答が届いているか（弁護士 or 管理画面）。
3. **Railway ログ** — 当該時刻に `[LINE] reply/push OK` 行があるか（時刻は UTC・JST=+9h）。

- 3 点が「送信済み」で揃う → 再操作しない（二重通知を避ける）。行は手動で `done` へ寄せてよい。
- 3 点が「未送信」で揃う → 安全に再操作（レコードを再度トリガー＝新 `id` で再配信）。
- **揃わない/不明** → **最終判断は人**（大野）。内容の重要度で「二重通知リスクを取る（再操作）」か
  「未達リスクを取る（保留）」を選ぶ。機械は判断しない。

## 3. `failed`（transient 失敗）の対応

- `last_error` の分類（`get_record_error_<status>`・`get_record_error_0`=通信/timeout・
  `get_record_error_5xx`・`get_record_error_401` 等）で一次原因を見る。
- kintone/ネットワークの一時障害が回復していれば、**レコード再操作で新 `id` の再配信**により
  再処理される（旧 `failed` 行はそのまま観測記録として残す or 手動で終息）。
- 401/403 が続く場合は認証（token/rotation）を確認（§5.2）。

## 4. 手動終息（任意・台帳を収束させる）

再操作で回復を確認した後、旧行を手動で終息させたい場合のみ:

```sql
-- 送信済みを確認した sending 行を done へ（last_error はそのまま or 手動注記）
UPDATE inbound_event SET state='done', processed_at=now()
WHERE provider='kintone' AND id=<対象id> AND state='sending';

-- 再処理見込みのない failed を打ち切り（記録として last_error は残す）
-- ※必要な場合のみ。failed のままでも再配信で新行が処理される。
```

（いずれも [人]。PC-A は §1 の SELECT と §2-3 のログ突合まで READ_ONLY で補助する。）

## 5. 参照

- id 欠落/型不正で**行が残らない拒否**（H01）は滞留監視の対象外。Railway ログの
  `kintone webhook rejected pre-claim: invalid_or_missing_id` で観測する（webhook 発行元＝
  kintone の設定不整合の疑い）。
- rotation（token 更新）は DRAFT §5.2・work-log 2026-07-16_RV-04c-S3。
