# 運用手順書（runbook）— 障害 6 場面の見分け方・止めるもの・確認・復旧・復旧後の確認

- 作成: 2026-10-16（OPS-RUNBOOK-1・QUEUE-2026-10-16-R F-6）。対象: Railway 上の jikou-line-bot（単一 Web サービス＋Postgres）。
- 読者: 大野（1 人・深夜）。**上から順に、書いてあるコマンドだけを打つ**。コマンドは秘密値を表示しない形にしてある
  （`railway variables` は値を表示するので使わない。値を見る必要があるときは Railway のダッシュボードで該当キーだけを開く）。
- 共通の前提: Railway CLI はログイン済み・プロジェクト `creative-manifestation` のサービス `jikou-line-bot` にリンク済み（`railway status` で確認）。
  Postgres へは `railway connect Postgres`（psql が開く。画面共有中は接続文字列が出るので注意）。
- 共通の原則: **自動再送はしない**（送信台帳の unconfirmed は人が確定）・**migration の downgrade はデータがあれば拒否される**（RV-08）・
  **顧客 Bot の停止は `AUTOREPLY_PAUSED=1`**（相談者への自動返信を全チャネルで止める。業務 LINE 通知は止まらない）。
- 既存の個別 runbook: docs/runbooks/stripe-journal-recovery.md（Stripe journal 滞留）・docs/runbooks/2026-07_kintone-lane-recovery.md
  （kintone レーン滞留）・docs/runbooks/dependency-lock-sbom.md（依存 lock）。本書はそれらを置き換えず、入口（場面の見分け）を提供する。
- 日次死活監視（daily_healthcheck.py・毎朝 7 時 JST・異常時のみ業務 LINE 1 通）の文言と本書の節の対応は §0 の表。

## 0. まず見分ける（症状 → 節）

| 症状（業務 LINE の警報・相談者の訴え・画面の表示） | 節 |
|---|---|
| 「送信台帳: 結果未確認…」「送信台帳: started のまま…」「DB 疎通: 接続に失敗」／PWA が「記録を読み書きできませんでした」／返信欄が送れない | §1 送信台帳 DB 停止 |
| Railway のデプロイが失敗／サービスが Crashed／LINE が無応答で Railway ログに起動エラー | §2 Railway 再起動 |
| 「kintone監視の実行自体が失敗」「kintone滞留」／ログに `GAIA_` や HTTP 429／承認や転記が止まる | §3 kintone API 制限到達 |
| 「LINE 疎通（…）: HTTP 401」「送信失敗率…」／LINE Developers の Webhook 検証が失敗／相談者から「返事が来ない」 | §4 LINE webhook 失敗連発 |
| `alembic upgrade` がエラーで止まった／起動後に「no such table」「UndefinedColumn」 | §5 migration 失敗時のロールバック |
| 「モデル監視の実行自体が失敗」「model NG」／Claude 呼出の例外が連続・承認キューに「確認中」定型が並ぶ／クレジット残高警報 | §6 LLM API 障害 |

---

## 1. 送信台帳 DB（Postgres）停止

**症状の見分け方**
- 業務 LINE に「DB 疎通: 接続に失敗（OperationalError 等）」または「送信台帳: …」の警報。PWA（/app/…）で固定文言「記録を読み書きできませんでした」。
- Railway ログ（`railway logs`）に `send_ledger` の `ledger unavailable` 系の固定語彙が連続。
- 挙動: bot の自動返信は **fail-open**（DB が無くても送る・記録だけ落ちる）。大野の返信欄（/app/reply）と承認済み下書きの送信は **fail-closed**（送らない）。
  JIKOU_SEND_FAILCLOSED=1 を入れている場合は bot 経路も送らない（この env は PR #292 feature/jikou-send-failclosed-1 の merge 後に有効。merge 前は存在しない）。

**止めるもの**
- 原則止めない（bot は fail-open で動く）。相談者へ重複送信の恐れがある（Postgres が断続的に落ちている）ときだけ `AUTOREPLY_PAUSED=1`。

**確認コマンド**
```
railway status                       # リンク先の確認
railway logs --lines 200             # 直近ログ（接続エラーの種類を見る・値は出ない）
railway connect Postgres             # psql が開けるか＝DB 自体の生死
```
psql が開いたら（読み取りのみ）:
```sql
SELECT state, COUNT(*) FROM send_operation GROUP BY state;
SELECT COUNT(*) FROM send_operation WHERE state='unconfirmed';
SELECT op_id, channel, created_at FROM send_operation WHERE state='started' ORDER BY created_at LIMIT 20;
```

**復旧手順**
1. Railway ダッシュボードで Postgres サービスの状態を確認。停止していれば Postgres を再起動（Deployments → Restart）。
2. 再起動後、Web サービスも再起動（§2 の手順）。接続プールの古い接続を捨てる。
3. 起動後 5 分で回収ジョブ `SEND_LEDGER_RECOVER` が started の滞留を unconfirmed に移す。unconfirmed は **人が** `/app/send_ops` で
   「送信済み／失敗」を確定する（LINE アプリの実送信履歴と突合）。自動再送はしない。
4. `AUTOREPLY_PAUSED=1` を入れた場合は、DB が安定してから外す。

**復旧後の確認**
```
railway run python daily_healthcheck.py      # 異常があれば exit 1 と警報 1 通（同日 2 回目は skipped）
```
- `/app/send_ops` の未確認件数が 0 になっていること。`/app/pending` の対応待ちが増えていないこと。

---

## 2. Railway 再起動

**症状の見分け方**
- Railway のサービスが Crashed／Deploy failed。`railway logs` の先頭に `ImportError`／`KeyError: 'XXX'`（env 未設定）／ポート待受前の例外。
- LINE が全く応答しない（webhook が 5xx）。日次死活監視自体が来ない（毎朝 7 時に何も無い＝正常の可能性もあるため、`railway logs` で `healthcheck OK` を確認）。

**止めるもの**
- なし（再起動は無停止ではないが短時間）。再起動を繰り返す場合は原因を直すまで新しいデプロイをしない。

**確認コマンド**
```
railway status
railway logs --lines 300
railway run python -c "import main"          # 起動時 import が通るか（env 未設定は KeyError の名前で分かる）
```

**復旧手順**
1. 直前の変更がデプロイ起因なら、Railway ダッシュボードの Deployments で **1 つ前の成功デプロイへ Rollback**（マージ＝自動デプロイのため、
   コードの revert は後で PR として行う）。
2. env 未設定（KeyError の名前）なら Railway の Variables に該当キーを追加（値はダッシュボードで入力・CLI に貼らない）。
3. 原因が外部（Postgres・LINE）なら §1／§4 へ。
4. 再起動: Deployments → 最新の成功デプロイ → Restart。

**復旧後の確認**
- `railway logs` に `Application startup complete` と `healthcheck` の起動ログ。LINE Developers の Webhook「検証」が 200。
- 業務 LINE に dead-man 警報が出ていないこと。翌朝の日次死活監視 OK を確認。

---

## 3. kintone API 制限到達・kintone 障害

**症状の見分け方**
- `railway logs` に HTTP 429 または kintone のエラーコード `GAIA_*`（`GAIA_IA02`＝APIトークンの権限・「アプリを更新」押し忘れ、`GAIA_RE01`/`GAIA_DA02` 等）。
- 業務 LINE「kintone監視の実行自体が失敗」「kintone滞留（未処理）」「kintone失敗」。承認キュー（App 29）の承認が LINE に届かない・App 21 への転記が止まる。
- kintone の 1 アプリあたり 1 日の API 上限（REST API リクエスト数）に達した場合は翌日 0 時（JST）まで回復しない。

**止めるもの**
- 大量に kintone を叩くジョブ: `HOUKI_KOSEKI_CHECKLIST_DISABLED=1`（30 分走査）・`HOUKI_OVERDUE_DISABLED=1`・`JIKOU_FOLLOWUP_DISABLED=1`・
  `JIKOU_PROGRESS_DISABLED=1`（未 merge の票を入れている場合）。相談者対応（App 21 の照会）は止めない。

**確認コマンド**
```
railway logs --lines 300 | grep -i "429\|GAIA_"      # 発生箇所とコード（値は出ない）
railway run python daily_healthcheck.py              # B 項目（フォーム設計の取得）が通るか＝疎通
```
psql（kintone レーンの滞留・docs/runbooks/2026-07_kintone-lane-recovery.md §1 と同じ）:
```sql
SELECT state, COUNT(*) FROM inbound_event WHERE provider='kintone' GROUP BY state;
```

**復旧手順**
1. 429／上限なら待つ（翌日 0 時 JST）。急ぐ場合は kintone 管理画面で該当アプリの API 実行数を確認し、不要な走査ジョブを上の env で止める。
2. `GAIA_IA02` なら該当アプリの APIトークンのアクセス権（閲覧・追加・編集）を確認し、**「アプリを更新」を押す**（既往 2 回）。
3. kintone レーンに滞留が残っていれば 2026-07_kintone-lane-recovery.md の手順（**再操作で新しい webhook を出させる**・自動再送なし）。
4. 止めた env は回復後に戻す。

**復旧後の確認**
- `railway run python daily_healthcheck.py` で kintone 系の警報が消えていること。承認キューで 1 件テスト承認し LINE へ届くこと（相談者ではなく自分の LINE で）。

---

## 4. LINE webhook 失敗連発・LINE 送信失敗

**症状の見分け方**
- LINE Developers コンソールの Webhook 検証が失敗／エラー統計が増加。`railway logs` に `signature` 不一致・`/webhook` の 4xx/5xx。
- 業務 LINE「LINE 疎通（顧客チャネル）: HTTP 401」（トークン失効）「送信台帳: 直近 24 時間の送信失敗率 …%」。
- 相談者から「返事が来ない」。`/app/send_ops` に failed／unconfirmed が並ぶ。

**止めるもの**
- 返信の重複・誤送信が疑われるとき: `AUTOREPLY_PAUSED=1`。それ以外は止めずに原因を直す。
- `INBOUND_EVENT_DURABLE_ENABLED` は **OFF に戻さない**（OFF では webhook 再配送の重複遮断が無くなり二重返信し得る）。

**確認コマンド**
```
railway logs --lines 300 | grep -i "webhook\|signature\|line"   # 失敗の種類（署名／5xx／timeout）
railway run python daily_healthcheck.py                          # M 項目（bot/info）で 401 かどうか
```
psql:
```sql
SELECT state, COUNT(*) FROM send_operation WHERE created_at > now() - interval '24 hours' GROUP BY state;
SELECT state, COUNT(*) FROM inbound_event WHERE provider='line' GROUP BY state;
```

**復旧手順**
1. 署名不一致: LINE Developers の Channel secret と Railway の `LINE_CHANNEL_SECRET`（相続放棄は `HOUKI_LINE_CHANNEL_SECRET`）が一致しているか
   （ダッシュボードで該当キーだけを開いて末尾数文字を比べる）。違えば Railway 側を更新して再起動。
2. HTTP 401: チャネルアクセストークン（長期）を LINE Developers で再発行し、`LINE_CHANNEL_ACCESS_TOKEN`／`HOUKI_LINE_CHANNEL_ACCESS_TOKEN`／
   業務用 `DISPATCHBOT_CHANNEL_ACCESS_TOKEN` を更新して再起動。
3. 5xx／timeout が LINE 側（LINE の障害情報ページ）なら待つ。LINE の再配送は受け取るたびに durable 台帳で重複遮断される。
4. 回復後、`/app/send_ops` の unconfirmed を人が確定（LINE アプリの送信履歴と突合）。failed は送っていないので、必要なら返信欄から送り直す。

**復旧後の確認**
- LINE Developers の Webhook 検証が成功。自分の LINE から 1 通送って返信が 1 通だけ来ること（二重返信が無いこと）。
- 翌朝の日次死活監視で「LINE 疎通」「送信失敗率」の警報が無いこと。

---

## 5. migration 失敗時のロールバック

**症状の見分け方**
- `railway run alembic upgrade head` がエラー終了（途中の DDL で失敗）／起動後に `no such table`／`UndefinedColumn`／PWA の固定文言「記録を読み書きできませんでした」。
- `railway run alembic current` の revision が期待（docs/handover_jikou_reply.md の表）と違う。

**止めるもの**
- 失敗した migration の対象機能に依存する新デプロイを止める（Railway で該当デプロイを Rollback）。相談者対応は fail-open のため継続する。

**確認コマンド**
```
railway run alembic current                 # 今の revision
railway run alembic history | head -40      # 直線のチェーン（分岐が無いこと）
railway run alembic upgrade head --sql      # 実行せず SQL を出す（何をしようとしているか）
```

**復旧手順**
1. 失敗が「途中まで DDL が当たった」状態なら、Postgres では DDL がトランザクション内のため通常は丸ごと戻っている。`alembic current` が 1 つ前のままなら、
   原因（権限・既存表との衝突）を直してから再度 `upgrade head`。
2. 適用済みだが戻したいとき: `railway run alembic downgrade <1 つ前の revision>`。**対象表に 1 行でもデータがあれば downgrade は例外で拒否される**
   （RV-08・`*_downgrade_refused_data_present`）。その場合は戻さず、コード側を Rollback（§2）して **表は残す**。
3. 「head が複数」と出たら（並列票の merge 直後に起き得る）: 後に入った票の `down_revision` を付け替える PR が要る。適用はその PR の後。
4. 直したら `railway run alembic upgrade head` → `railway run alembic current` で確認。

**復旧後の確認**
- 起動ログにエラーが無い。`railway run python daily_healthcheck.py` で DB 疎通 OK。該当画面（/app/…）が固定文言ではなく一覧を返す。

---

## 6. LLM API（Anthropic Claude）障害 ※票の表記「OpenAI API 障害」に相当（本システムの LLM は Anthropic）

**症状の見分け方**
- 業務 LINE「モデル監視の実行自体が失敗」「model NG: …」、または Claude のクレジット残高警報（残高系 400 を警報化済み）。
- `railway logs` に `anthropic` の 5xx／timeout／`overloaded_error` が連続。相談者への返信が「確認中」の定型のみになり、承認キュー（App 29）に未送信が並ぶ。
- 相続放棄ヒアリング・相談カード読取・上申書下書き（Claude を使う機能）が全て止まる。

**止めるもの**
- 止めない（設計上、Claude 断は定型＋承認キュー＋業務通知へ縮退する）。誤回答の恐れ（フォールバックモデルの品質）があるときだけ `AUTOREPLY_PAUSED=1`。

**確認コマンド**
```
railway run python daily_healthcheck.py              # A 項目: PRIMARY／FALLBACK モデルの有効性
railway logs --lines 200 | grep -i "anthropic\|claude\|overloaded\|credit"
```
- Anthropic のステータスページ（status.anthropic.com）と Console の残高を確認（ブラウザ）。

**復旧手順**
1. 残高切れ: Console でリチャージ（自動リチャージの設定を確認）。復旧後は追加操作なし。
2. モデル ID の廃止（A 項目 NG）: `PRIMARY_MODEL`／`FALLBACK_MODEL` を有効な ID に更新して再起動（README「モデル入替」の手順・トリアージ回帰は merge 前に 1 回）。
3. 障害中に承認キューへ溜まった下書きは、回復後に PWA（/app/approvals・/app/drafts）で人が確認して送る（自動では送らない）。対応待ち（/app/pending）も同様。
4. 相続放棄の「確認中」定型を受け取った相談者には、回復後に人が返信する。

**復旧後の確認**
- `railway run python daily_healthcheck.py` が OK。自分の LINE で 1 問投げて AI 返信（または承認キュー起票）が戻ること。承認キューの残が捌けていること。

---

## 付録 A. 停止・縮退に使う env（値はダッシュボードで設定・変更後は再起動）

| env | 効果 | 戻し忘れ注意 |
|---|---|---|
| `AUTOREPLY_PAUSED=1` | 相談者への自動返信を全業務で停止（受信は記録のみ・業務通知は継続） | ◎ |
| `HEALTHCHECK_DISABLED=1` | 日次死活監視を止める（警報が来なくなる＝障害に気づけない） | ◎ |
| `HOUKI_KOSEKI_CHECKLIST_DISABLED` / `HOUKI_OVERDUE_DISABLED` / `JIKOU_REPLY_METRICS_DISABLED` / `JIKOU_FOLLOWUP_DISABLED` / `JIKOU_PROGRESS_DISABLED` | 個別ジョブの停止（kintone 負荷・誤通知の切り分け） | ○ |
| `INBOUND_EVENT_DURABLE_ENABLED` | **OFF にしない**（重複遮断が消える） | — |
| `JIKOU_SEND_POLICY_V2` | 許可表 V2（ON で自動返信は閉集合定型のみ・無言＋対応待ち） | 設計切替・障害対応では触らない |

## 付録 B. 日次死活監視の点検項目（daily_healthcheck.py）

A モデル有効性／B kintone フォーム設計／C docx テンプレ／D App30-32 同期／E Stripe journal 滞留／F 業務通知 dead-man／G LINE durable 滞留／
H 未知 provider／I App36 decision 監査／**J 送信台帳（unconfirmed 24h・started 60 分・失敗率 20%）／K 対応待ちの最古（24h）／
L 熟慮期間 7 日以内未提出／M 疎通（DB・LINE 顧客／業務）**（J〜M は OPS-RUNBOOK-1 で追加・閾値は daily_healthcheck.py 末尾の定数・要大野確認）。
異常時のみ 1 通・同日 2 回目は送信台帳 key `healthcheck:{JST 日付}` で skipped・正常時は何も送らない。
