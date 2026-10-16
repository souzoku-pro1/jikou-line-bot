# 送信台帳 purpose の正式移行（JIKOU-SEND-PURPOSE-2・準備票）

- 票: JIKOU-SEND-PURPOSE-2（QUEUE-2026-10-13-E E-2）。作成のみ・**適用しない**（migration の本番適用・flag ON は大野）。
- 現状: 送信台帳 `send_operation.purpose` は閉集合（CHECK `ck_send_operation_purpose`: reply / first_reply / urgent /
  image_receipt / image_result / follow / receipt_number / other）。閉集合に無い用途は **purpose=other ＋ channel ＋
  inbound_event_id の接頭辞 `{種別}:{業務キー}`** で代替している（HOUKI-JUKURYO-2 で始めた暫定方式）。
- 方針: 接頭辞の種別名をそのまま新 purpose 名にする（1 対 1・名前は変えない）。**inbound_event_id は書き換えない**
  （業務キーは従来どおり `{種別}:{業務キー}` のまま・purpose 列だけ other → 種別名）。再配送の重複検出（business・channel・
  inbound_event_id・purpose・inbound_seq）は purpose の表現が変わっても同じ操作として扱う（§4 の照合関数）。

## 1. 一覧表（旧キー形式 → 新 purpose 名）

| # | 種別（接頭辞） | 新 purpose | 旧表現（purpose / channel / inbound_event_id） | 発生元（module・票） | 業務キー |
|---|---|---|---|---|---|
| 1 | `pending_notice` | `pending_notice` | other / jikou_notice / `pending_notice:{会話 ref or hash16}:{JST 日付}` | hub/send_policy.notice_key（JIKOU-REPLY-Q1c） | 会話×日（【対応待ち】1 会話 1 日 1 回） |
| 2 | `draft_notice` | `draft_notice` | other / jikou_notice / `draft_notice:{会話 ref or hash16}:{JST 日付}` | hub/reply_draft.notice_key（Q3b） | 会話×日（【下書きあり】） |
| 3 | `approval_blocked` | `approval_blocked` | other / jikou_notice / `approval_blocked:{App 29 No.}:{revision}` | hub/send_policy.approval_blocked（Q1c） | 承認版 |
| 4 | `exemplar_promotion` | `exemplar_promotion` | other / jikou_notice / `exemplar_promotion:{category}:{回数}` | hub/send_policy.promotion_check（Q3a） | 分類×閾値 |
| 5 | `j5_notice` | `j5_notice` | other / jikou_notice / `j5_notice:{会話 ref or hash16}:{company_id}` | hub/jikou_reply_flow._j5_notice（Q2） | 会話×会社 |
| 6 | `jikou_metrics_weekly` | `jikou_metrics_weekly` | other / jikou_notice / `jikou_metrics_weekly:{JST 週開始日}` | hub/jikou_reply_metrics（Q6） | 週 |
| 7 | `houki_jukuryo_14` | `houki_jukuryo_14` | other / houki_jukuryo / `houki_jukuryo_14:{App 40 No.}:{期限}` | hub/houki_jukuryo（HOUKI-JUKURYO-2・main） | 案件×期限 |
| 8 | `houki_jukuryo_7` | `houki_jukuryo_7` | other / houki_jukuryo / `houki_jukuryo_7:{App 40 No.}:{期限}` | 同上 | 案件×期限 |
| 9 | `houki_jukuryo_unset` | `houki_jukuryo_unset` | other / houki_jukuryo / `houki_jukuryo_unset:{JST 日付}` | 同上 | 日 |
| 10 | `houki_overdue` | `houki_overdue` | other / houki_overdue / `houki_overdue:{JST 日付}` | hub/houki_overdue（HOUKI-JUKURYO-3） | 日 |
| 11 | `houki_reply` | `houki_reply` | other / souzoku-houki / `houki_reply:{LINE webhookEventId}` | hub/line_channel.houki_reply_context（HOUKI-SEND-LEDGER-1） | 受信イベント |
| 12 | `fax` | `fax` | other / fax / `fax:{App 21 No.}:{slot}:{PDF sha256 先頭 16}` | hub/jikou_fax（JIKOU-FAX-2） | 案件×枠×PDF |
| 13 | `send_ledger_down` | `send_ledger_down` | other / send_ledger_down / `send_ledger_down:{JST 日付}` | hub/send_failclosed（JIKOU-SEND-FAILCLOSED-1） | 日 |

- 対象外（既に閉集合内）: reply（bot 返信・人の明示送信〔actor=human〕・承認済み下書き〔actor=approved_draft〕）・first_reply（J-0）・
  urgent・image_receipt・image_result・follow・receipt_number。
- 接頭辞が上表に無い other 行（例: テスト用の未知 purpose）は other のまま（書き換えない）。
- コード側の対応表: `hub/send_ledger.PURPOSE_KINDS`（種別 → purpose・migration と同じ 13 行・テストで同一性を pin）。

## 2. 件数の数え方（両方式を合算）

```sql
-- 種別 X の件数（移行前後・flag の ON/OFF に関係なく同じ値になる）
SELECT COUNT(*) FROM send_operation
 WHERE purpose = 'X' OR (purpose = 'other' AND inbound_event_id LIKE 'X:%');

-- 全種別の内訳（旧表現／新表現を分けて数える）
SELECT k.kind,
       SUM(CASE WHEN o.purpose = 'other' THEN 1 ELSE 0 END) AS legacy_rows,
       SUM(CASE WHEN o.purpose = k.kind THEN 1 ELSE 0 END)  AS v2_rows
  FROM (VALUES ('pending_notice'),('draft_notice'),('approval_blocked'),('exemplar_promotion'),('j5_notice'),
               ('jikou_metrics_weekly'),('houki_jukuryo_14'),('houki_jukuryo_7'),('houki_jukuryo_unset'),
               ('houki_overdue'),('houki_reply'),('fax'),('send_ledger_down')) AS k(kind)
  JOIN send_operation o
    ON (o.purpose = k.kind) OR (o.purpose = 'other' AND o.inbound_event_id LIKE k.kind || ':%')
 GROUP BY k.kind ORDER BY k.kind;
```

## 3. 照合 SQL（migration の前後で件数が一致すること）

```sql
-- (1) 適用前: 旧表現の件数を控える（種別ごと）
SELECT substr(inbound_event_id, 1, instr(inbound_event_id, ':') - 1) AS kind, COUNT(*)
  FROM send_operation WHERE purpose = 'other' AND inbound_event_id LIKE '%:%'
 GROUP BY 1 ORDER BY 1;
-- Postgres は split_part(inbound_event_id, ':', 1)

-- (2) 適用後: 新表現の件数（= (1) と一致）・旧表現の残り（= 0 になるべき。残るのは上表に無い接頭辞だけ）
SELECT purpose, COUNT(*) FROM send_operation
 WHERE purpose IN ('pending_notice','draft_notice','approval_blocked','exemplar_promotion','j5_notice',
                   'jikou_metrics_weekly','houki_jukuryo_14','houki_jukuryo_7','houki_jukuryo_unset',
                   'houki_overdue','houki_reply','fax','send_ledger_down')
 GROUP BY purpose ORDER BY purpose;
SELECT COUNT(*) FROM send_operation WHERE purpose = 'other' AND (
       inbound_event_id LIKE 'pending_notice:%' OR inbound_event_id LIKE 'draft_notice:%' OR
       inbound_event_id LIKE 'approval_blocked:%' OR inbound_event_id LIKE 'exemplar_promotion:%' OR
       inbound_event_id LIKE 'j5_notice:%' OR inbound_event_id LIKE 'jikou_metrics_weekly:%' OR
       inbound_event_id LIKE 'houki_jukuryo_14:%' OR inbound_event_id LIKE 'houki_jukuryo_7:%' OR
       inbound_event_id LIKE 'houki_jukuryo_unset:%' OR inbound_event_id LIKE 'houki_overdue:%' OR
       inbound_event_id LIKE 'houki_reply:%' OR inbound_event_id LIKE 'fax:%' OR
       inbound_event_id LIKE 'send_ledger_down:%');   -- 期待 0

-- (3) 一意制約の衝突がないこと（同じ business・channel・inbound_event_id・inbound_seq で other と新 purpose の両方が
--     存在すると UPDATE が uq_send_operation_inbound に当たる → 適用前に確認。期待 0 行）
SELECT business, channel, inbound_event_id, inbound_seq, COUNT(*)
  FROM send_operation WHERE inbound_event_id IS NOT NULL
 GROUP BY 1,2,3,4 HAVING COUNT(DISTINCT purpose) > 1;
```

## 4. コード（flag JIKOU_SEND_PURPOSE_V2・既定 OFF）

- `hub/send_ledger.py`（凍結の閉集合 `PURPOSES` は不変。V2 用の集合 `PURPOSES_V2 = PURPOSES + 新 13 件` を別名で持つ）:
  - `purpose_v2_enabled()`: env `JIKOU_SEND_PURPOSE_V2` が 1/true/yes/on。
  - `kind_of(inbound_event_id)`: 接頭辞（最初の `:` の前）が `PURPOSE_KINDS` にあればその種別・無ければ None。
  - `canonical_purpose(purpose, inbound_event_id)`: other＋既知接頭辞 → 種別名、それ以外はそのまま（両方式の正規化）。
  - `canonical_key(business, channel, purpose, inbound_event_id, inbound_seq)` / `same_key(row_a, row_b)`: 両方式を
    同一 key として照合する関数（書式が違っても同じ操作なら True）。
  - `effective_purpose(purpose, inbound_event_id)`: **ON かつ migration 適用済み**（`alembic_version` に本 revision がある・
    10 分キャッシュ）なら other＋既知接頭辞を種別名に置き換えて記録。OFF・未適用は従来どおり other（CHECK 違反で記録が
    落ちて fail-open になる事故を防ぐ）。
  - `begin()`: 記録する purpose は `effective_purpose` の値。再配送の既存行検索は purpose の **両表現**（種別名と other）で
    行う（flag の ON/OFF・移行前後をまたいでも 2 回目を送らない）。
- 呼出側（send_policy・reply_draft・houki_jukuryo・houki_overdue・line_channel・jikou_fax・send_failclosed）は不変
  （`purpose(PURPOSE_OTHER)` のまま。置換は begin() の一点）。

## 5. Postgres 固有（要 Postgres-ak5B 実走・保留）

- CHECK 制約の差し替え: `ALTER TABLE send_operation DROP CONSTRAINT ck_send_operation_purpose; ALTER TABLE ... ADD CONSTRAINT
  ck_send_operation_purpose CHECK (purpose IN (...))`（sqlite は batch モードで表を作り直す。migration は dialect で分岐）。
- UPDATE は 1 トランザクション（行数は §3 (1) の合計）。`uq_send_operation_inbound`（business・channel・inbound_event_id・
  purpose・inbound_seq）は purpose の値が変わるだけなので衝突しない（§3 (3) で事前確認）。
- downgrade は逆変換（種別名 → other・inbound_event_id は不変）＋ CHECK を旧集合へ戻す。新 purpose の行が残っていれば
  CHECK で落ちるので、先に UPDATE してから制約を戻す。
- 実走手順（本番）: `alembic current` → §3 (1)(3) → `alembic upgrade f8b0c2d4e6a8` → §3 (2) → Railway env
  `JIKOU_SEND_PURPOSE_V2=1` → 再デプロイ。順番を変えない（flag を先に ON にしても未適用なら従来方式のまま＝安全側）。
