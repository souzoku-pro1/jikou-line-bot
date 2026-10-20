# 外部死活監視（EXTERNAL-UPTIME-1）— 設計と設定手順（コード不変・値なし）

票: QUEUE-2026-10-20-J J-3（2026-10-21・PC-A）。
背景: 2026-10-17 00:10 JST のデプロイで起動時 fail-fast が走り、本番が **4 日間停止**した。アプリ内の日次死活監視
（`daily_healthcheck.py`・毎朝 7 時 JST）は「アプリが動いているとき」にしか走らないため、停止そのものには誰も気づけなかった。
Railway の外から `/health` を叩く監視を置き、落ちたら業務 LINE（指示 Bot）へ届くようにする。

---

## 1. 要件（票文）

| 項目 | 値 |
|---|---|
| 監視対象 | `GET https://jikou-line-bot-production.up.railway.app/health`（HTTPS・公開・認証なし） |
| 間隔 | 5 分 |
| 発報条件 | **2 回連続失敗**（HTTP 200 以外・タイムアウト・接続不可） |
| 通知先 | 業務 LINE（指示 Bot＝`DISPATCHBOT_CHANNEL_ACCESS_TOKEN` の送信チャネル・宛先は大野） |
| 制約 | 無料枠で運用・値（トークン・URL の秘密部分）は本書に書かない |

`/health` の応答は `{"status":"ok","deps":{…},"service_auth":{"status":"ok|degraded",…}}`（`service_auth` は
STARTUP-FAILSAFE-1〔J-2〕merge 後）。**HTTP 200 かつ本文に `"status":"ok"` を含む**ことを正常とみなす
（キーワード監視が使える場合は `"status":"ok"` を必須文字列にする）。

---

## 2. 設計上の要点 — 「通知経路はアプリに依存させない」

票文の構成案は「監視サービスの Webhook → アプリの専用経路 `/hooks/uptime` → 業務 LINE」だが、**監視が検知したいのは
「アプリが落ちている」状態**であり、そのときアプリ上の `/hooks/uptime` も落ちている。この経路だけに頼ると、
**肝心の全停止時に LINE 通知が一切届かない**（今回の 4 日間停止と同じ沈黙になる）。

したがって:

1. **一次通知はアプリ外で完結させる**（監視サービス → 大野の端末）。
   - (a) 監視サービスの標準チャネル（メール／スマホ push／SMS）。設定が最も簡単で Railway に依存しない。
   - (b) LINE へ届けたい場合は、**Railway 外の中継**を 1 つ置く: 監視サービスの Webhook → **GAS Web アプリ**（既存の
     GAS プロジェクト「相続書類自動化」と同じ Google アカウントで新規作成）→ LINE Messaging API push（指示 Bot の
     トークンは GAS の Script Properties に保存）。GAS は Railway と独立に動くため、アプリ全停止時でも届く。
2. **アプリ側の `/hooks/uptime` は二次経路**とする（復旧通知「UP に戻った」の受け口・将来の /app 表示用）。
   実装は次票（本票は設計のみ・§5）。

GAS 中継の設計（値なし）:

| 項目 | 設計 |
|---|---|
| 入口 | GAS Web アプリの `doPost`（URL は GAS が発行・URL 自体が秘匿情報＝監視サービスにだけ登録） |
| 認可 | 監視サービスが付けられる固定ヘッダまたは URL クエリの共有トークン（Script Properties `UPTIME_RELAY_TOKEN`）を**定数時間比較**。不一致は 200 で無視（存在を教えない） |
| 本文 | 監視サービスの payload は**読まない**（文面の差し込みをしない）。固定文言＋「DOWN／UP」の 2 値だけを payload の決まったフィールドから取り、他は捨てる（文言インジェクション防止） |
| 送信 | `POST https://api.line.me/v2/bot/message/push`（指示 Bot のチャネルアクセストークン＝Script Properties `DISPATCHBOT_TOKEN`・宛先 userId＝Script Properties `UPTIME_NOTIFY_TO`）。固定文言例: `【外部監視】/health が 2 回連続で失敗（DOWN）。Railway の Deployments と railway logs を確認してください。` ／ `【外部監視】/health 復旧（UP）。` |
| 抑制 | 同一状態の連続通知は GAS 側で 30 分に 1 回（CacheService）。UP→DOWN／DOWN→UP の遷移は必ず送る |
| 保全 | 指示 Bot トークンが GAS にも置かれる＝**資格情報の所在が 1 つ増える**。`docs/credentials_expiry.md` に行を追加（J-1 merge 後の docs 追随票） |

補完策（設定のみ・推奨）: **Railway のプロジェクト Webhook／通知**で「Deployment CRASHED / FAILED」をメールや
Discord/Slack に流す（Railway → Project Settings → Webhooks。要大野確認: 無料枠での提供範囲）。今回の事故は
「デプロイが CRASHED のまま放置」だったので、この 1 本だけでも初動が 4 日→数分になる。

---

## 3. 監視サービスの比較（無料枠・2026-10-21 時点の二次情報・**要大野確認**）

要件: HTTPS GET・5 分間隔・Webhook 通知（GAS 中継用）・2 回連続失敗で発報。
下表は各社公式ページと比較記事からの要約で、**無料枠の条件は変わりやすい**。契約前に公式の料金ページで
「間隔・監視数・Webhook の有無・商用利用可否」を確認すること。

| サービス | 無料枠（二次情報） | 5 分間隔 | Webhook（無料枠） | 2 回連続失敗 | 所感 |
|---|---|---|---|---|---|
| **Better Stack Uptime** | 監視 10 本・3 分間隔（公式比較記事）／5 分とする記事もあり | ○ | ○と記す比較記事あり（公式の無料枠明記は未確認） | 「確認期間（confirmation period）」で設定 | スマホ push・電話通知も持つ＝一次通知 (a) の候補として最有力。Webhook が無料で使えれば GAS 中継にも |
| **UptimeRobot** | 監視 50 本・5 分間隔・メール通知・ログ 3 か月 | ○ | **不明**（Team 以上とする記事と Pro で可とする記事が混在） | 「アラート前の確認（再試行）」で設定 | 監視数が多く設定が簡単。LINE 中継に Webhook が要るなら有料の可能性＝一次通知はメール/アプリ push で |
| **StatusCake** | 監視 10 本・5 分間隔（公式料金ページ）・Slack/Discord/Telegram 連携 | ○ | 要確認（連携一覧に Webhook があるか） | 「確認サーバ数」で設定 | 無料で 5 分間隔が公式に明記されている。Telegram 等への直接通知も一次通知 (a) になりうる |
| cron-job.org（参考） | 無料・URL 定期実行・失敗/復旧をメール通知 | ○（60 回/時まで） | 不明 | — | **連続失敗 25 回超で自動停止**の記載あり＝長期停止中に監視が止まる。採用しない |

推奨: **Better Stack（一次=push・二次=Webhook→GAS→LINE）**を第 1 候補、**StatusCake** を第 2 候補。UptimeRobot は
Webhook の無料可否が確認できた場合のみ。どれも「2 回連続失敗」は確認期間／再試行回数の設定で表現する
（5 分間隔なら確認 5 分＝実質 2 回目で発報）。

出典（二次情報）: [UptimeRobot Pricing 2026（notifier.so）](https://notifier.so/guides/uptimerobot-pricing-2026/)・
[UptimeRobot 代替（host-tracker）](https://www.host-tracker.com/blog/uptimerobot-alternative)・
[uptimerobot pricing and review（cubeapm）](https://cubeapm.com/blog/uptimerobot-pricing-and-review/)・
[Better Stack: Uptime Robot alternatives](https://betterstack.com/community/comparisons/uptime-robot-alternatives)・
[uptimerobot vs betterstack（hyperping）](https://hyperping.com/blog/uptimerobot-vs-betterstack)・
[StatusCake pricing（公式）](https://statuscake.com/pricing)・
[StatusCake vs UptimeRobot（notifier.so）](https://notifier.so/guides/statuscake-vs-uptimerobot/)・
[cron-job.org](https://cron-job.org/)。

---

## 4. 大野が画面で行う設定手順（1 ページ・値なし）

前提: 監視サービスのアカウントは事務所のメールで作成（個人アカウントにしない）。パスワード・API キー・Webhook URL は
パスワードマネージャへ。本書と Issue には書かない。

1. **監視サービスにサインアップ**（§3 の第 1 候補）。2 要素認証を有効にする。
2. **モニターを 1 本作成**
   - 種別: HTTP(S)／URL 監視。URL: `https://jikou-line-bot-production.up.railway.app/health`。メソッド GET。
   - 間隔: 5 分。タイムアウト: 30 秒。
   - 正常判定: HTTP 2xx。キーワード監視が選べる場合は本文に `"status":"ok"` を**含む**こと。
   - 発報条件: 連続 2 回失敗（「確認期間 5 分」「再試行 1 回」等、サービスの表現に合わせる）。
   - 名前: `jikou-line-bot /health`（通知文に出る名前・PII なし）。
3. **一次通知（アプリ外）を設定**: 通知先にメール（事務所）とスマホ push（サービスのアプリ）を追加。可能なら電話/SMS も。
   テスト通知ボタンで 1 回受信を確認。
4. **二次通知（LINE）— GAS 中継を作る**（任意・Webhook が無料枠で使える場合）
   1. Google ドライブ → 新規 → Apps Script。プロジェクト名 `uptime-relay`。
   2. プロジェクトの設定 → スクリプト プロパティに 3 つ: `UPTIME_RELAY_TOKEN`（ローカルで `python -c "import secrets; print(secrets.token_hex(32))"` で生成）・`DISPATCHBOT_TOKEN`（指示 Bot のチャネルアクセストークン。LINE Developers の該当チャネルから）・`UPTIME_NOTIFY_TO`（大野の LINE userId＝Railway env `ATTORNEY_LINE_USER_ID` と同値）。
   3. `doPost(e)` を実装（PC-A が次票で雛形を repo `gas/uptime_relay.js` に置く・本票は設計のみ）: トークン照合→状態 2 値抽出→固定文言→LINE push→CacheService で抑制。
   4. デプロイ → ウェブアプリ → 実行ユーザー「自分」・アクセス「全員」→ URL を控える（この URL と token が秘匿情報）。
   5. 監視サービスの通知先に Webhook を追加: URL は手順 4 の URL に `?token=<UPTIME_RELAY_TOKEN>` を付けたもの（ヘッダが使えるサービスではヘッダに）。テスト通知で LINE に 1 通届くこと。
5. **Railway 側の補完**: Railway → プロジェクト → Settings → Webhooks（または Notifications）で Deployment の
   失敗/クラッシュをメールへ。1 回テスト。
6. **記録**: `docs/credentials_expiry.md` に「監視サービスのアカウント／GAS `uptime-relay` の Script Properties 3 件」を
   追記（値なし・担当＝大野）。パスワードマネージャに格納。
7. **訓練（任意・業務時間外）**: Railway でサービスを一時停止（または環境変数で意図的に起動失敗）→ 5〜10 分で
   一次通知と LINE 通知が届くことを確認→戻す。**顧客 LINE の受信が止まる時間帯になるため深夜に 10 分以内で**。

---

## 5. アプリ側 `/hooks/uptime`（次票・設計のみ）

目的: 二次経路（復旧「UP」通知の受け口・将来 /app に「外部監視の最終受信時刻」を表示）。一次通知の代替ではない。

| 項目 | 設計 |
|---|---|
| 経路 | `POST /hooks/uptime/{token}`。`token` は env `UPTIME_HOOK_TOKEN`（32 byte hex）と**定数時間比較**。未設定・不一致は 404（存在しないフリ＝既存流儀） |
| 認可の性質 | 監視サービスは HMAC 署名（X-Sig-*）を付けられないため **registry（HMAC 鍵）を使わない静的トークン方式**。これにより J-2 の degraded（registry 起因の 401）とは**構造的に無関係**（票文「J-2 の 401 対象外」はこの意味で満たす）。票文の「署名付き」を HMAC と読むか静的トークンで足りるかは**要裁定** |
| 入力 | 本文は JSON として parse するだけで、固定の 2 フィールド（状態 up/down・モニター名）以外は捨てる。64 KB 上限・Content-Type 限定 |
| 作用 | `notify_admin_line` へ固定文言（状態 2 値＋受信時刻）。同一状態は 30 分 1 回（既存 throttle_key）。kintone 書込なし・顧客 LINE 送信なし |
| ログ | 固定語彙のみ（token・payload は出さない）。sink allowlist 追加ゼロ（新 module で emit 経由） |
| /app | 「外部監視 最終受信: hh:mm（UP/DOWN）」を /app ホームに 1 行（read-only・P4-001 の関所内） |
| テスト | 404（未設定/不一致）・200（一致）・固定文言・抑制・PII 非掲載・RV-10 |

---

## 6. 残課題・裁定事項

- 監視サービスの選定（§3 の無料枠は要確認）と、Webhook の無料可否で GAS 中継を置くかの判断。
- 票文「署名付き」の解釈（HMAC か静的トークンか）＝§5。
- `docs/credentials_expiry.md` への追記は J-1 merge 後の小票（本票は docs ブランチ独立のため触らない）。
- 監視の空振り（Railway 側の一時的な 502 等）で LINE が鳴りすぎる場合は確認期間を 10 分に伸ばす。
