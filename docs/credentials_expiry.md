# 期限を持つ資格情報の一覧と更新手順（値なし）

作成: REGISTRY-EXPIRY-WARN-1（QUEUE-2026-10-20-J J-1・2026-10-21）。
背景: 2026-10-17 00:10 JST、HMAC 鍵 registry の全鍵が `expires_at` 超過のまま再デプロイされ、起動時 fail-fast で
本番が 4 日間停止した（LINE 受信も停止）。期限を持つ資格情報を 1 か所に列挙し、「どこに期限があり・誰が・どう更新するか」を
値を書かずに残す。**値（トークン・鍵・パスワード）は本書にも Issue にも書かない。**

凡例: 期限の所在=「期限がどこに記録されているか」。自動検知=日次死活監視（`daily_healthcheck.py`）で拾えるか。

| 資格情報（env 名） | 用途 | 期限の所在 | 自動検知 | 更新手順（概要）・担当 |
|---|---|---|---|---|
| `SERVICE_HMAC_KEY_REGISTRY` | 署名付き受信（GAS `/koseki`・`/sortation`・`/scan` 等、事務所 PC watcher `/ocr/fixed-asset`） | registry JSON 内 各鍵の `expires_at`（unix 秒・鍵ごと） | **あり**（監視項目J: 全鍵が N 日以内=高・毎日／一部=中・週 1 回。`REGISTRY_EXPIRY_WARN_DAYS` 既定 14・要大野確認） | `railway run python scripts/registry_tool.py check` → 延長は `extend --until YYYY-MM-DD`、rotation は `rebuild`（新 kid 追加→送信側切替→旧 revoked）。set は別コマンド・`&&` 直結禁止（README「HMAC 鍵 registry の点検と延長」）。送信側の鍵更新: GAS Script Properties `RV04C_KEY_ID`/`RV04C_SECRET_HEX`（gas/README.md）・watcher は事務所 PC の設定（runbook §2.5）。担当=大野 |
| `LINE_CHANNEL_ACCESS_TOKEN`（時効 Bot）・`HOUKI_LINE_CHANNEL_ACCESS_TOKEN`（相続放棄 Bot）・`DISPATCHBOT_CHANNEL_ACCESS_TOKEN`（指示 Bot） | LINE Messaging API 送信 | LINE Developers コンソールで発行する長期トークンは**無期限**（失効は手動 revoke 時のみ）。短期トークン方式は使っていない | なし（送信失敗は notify の失敗ログ・dead-man で間接検知） | コンソールで再発行→Railway env 差し替え→デプロイ後にテスト送信。旧トークンは新トークン確認後に revoke。担当=大野 |
| `LINE_CHANNEL_SECRET`・`HOUKI_LINE_CHANNEL_SECRET` | LINE Webhook 署名検証 | 無期限 | なし（不一致は Webhook 401 の連続） | コンソールで再発行→env 差し替え（**Webhook は再配送されないため、差し替えは問い合わせの少ない時間帯に・デプロイ完了後にテスト送信**）。担当=大野 |
| `KINTONE_API_TOKEN`・`TOKEN_CHATLOG`・`TOKEN_APPROVAL`・`TOKEN_SOUZOKUNIN`・`TOKEN_SHIPPING`・`TOKEN_HOUKI`・`KINTONE_FUDOSAN_API_TOKEN` ほか `TOKEN_*` | kintone REST API（アプリ別） | 無期限（アプリ設定で無効化・削除した時点で失効） | 部分的（監視項目 kintone schema は認証失敗を「監視の実行自体が失敗」として検知） | kintone アプリ設定 → API トークン → 再生成→ env 差し替え。権限（閲覧/追加/編集/削除・アプリ管理）を同じにする。担当=大野 |
| `KINTONE_WEBHOOK_TOKEN`（`_NEXT`・`_NEXT_EXPIRES`） | kintone→アプリ Webhook の合言葉 | `KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES`（rotation 中のみ・YYYY-MM-DD） | **あり**（NEXT 残置 notice: 期限超過で通知本文に 1 行） | RV-04c rotation 手順（docs/runbooks）。担当=大野 |
| `CLOUDSIGN_CLIENT_ID`・`CLOUDSIGN_WEBHOOK_SECRET` | クラウドサイン API／Webhook | client_id は無期限。API のアクセストークンは client_id から**都度取得**（コード側で保持しない） | なし（取得失敗は契約書送信の失敗通知） | クラウドサイン管理画面で再発行→env 差し替え。担当=大野 |
| `ANTHROPIC_API_KEY`（`_DEV`） | Claude API | 無期限（Console で revoke 時のみ）。**残高切れ**は期限ではないが同じ停止原因 | **あり**（残高系エラーの管理者 LINE 警報・PR #14／モデル存在監視） | Console で再発行→env 差し替え。自動リチャージ設定を維持。担当=大野 |
| `GOOGLE_VISION_API_KEY` | OCR（Vision API） | 無期限（制限・無効化は GCP 側） | あり（`/health/deps` の probe・403 早期可視化） | GCP コンソールで再発行→env 差し替え。担当=大野 |
| `STRIPE_WEBHOOK_SECRET` | Stripe Webhook 署名 | 無期限（エンドポイント再作成時に変わる） | なし（不一致は Webhook 400 の連続） | Stripe ダッシュボードで再生成→env 差し替え。担当=大野 |
| `WEBAPP_PASSWORD_HASH`・`WEBAPP_SESSION_SECRET` | PWA ログイン／署名 cookie | 無期限（rotate は運用判断・2 env 同時差し替え＝全 session 失効） | なし | README「PWA」節の rotate スクリプト。担当=大野 |
| `HOUKI_WEBHOOK_TOKEN`・`DOCUMENT_WEBHOOK_SECRET`・各 `*_INGEST_TOKEN`（query token 方式） | 受け口の合言葉 | 無期限 | なし | 再生成→env と送信側（kintone Webhook URL／GAS）を同時に差し替え。担当=大野 |
| Railway CLI ログイン（PC-A・事務所 PC） | デプロイ検分・`railway run` | トークンは長期だが端末ごと | なし | `railway login`（ブラウザ）。担当=各端末の操作者 |
| GitHub（Git Credential Manager） | push／PR 作成 | PAT なら有効期限あり（発行時に設定） | なし | GitHub Settings → Tokens で再発行。担当=大野 |

## 運用メモ

- **期限が明示的に存在するのは registry 鍵と `KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES` のみ**。この 2 つは日次死活監視が拾う。
  それ以外は「無期限だが人が revoke/再発行したとき」に止まるため、差し替えは必ず「新を投入→確認→旧を無効化」の順で行う。
- registry 鍵の失効日は **2027-01-01 00:00 JST**（2026-10-20 再構成時に設定）。rotation（新 kid 追加→送信側切替→旧 revoked）は
  失効の 1 か月以上前に始める。送信側（GAS／watcher）の切替手順は `pgv/gas_key_redistribute.md`（J-5）を参照。
- env の投入は Railway の env 保存＝自動再デプロイ。**空文字での上書き**を防ぐため、`registry_tool.py` は空・不正・実効鍵 0 では
  ファイルを書かない。set は別コマンド・`&&` 直結禁止。
- 本書は値を含まないため repo に置く。値の保管場所（パスワードマネージャ）は本書に書かない。
