# jikou-line-bot

大野法律事務所 時効援用 LINE Bot（Railway 上で稼働）

---

## アーキテクチャ概要

```
LINE ユーザー
    │ LINE Messaging API (Webhook)
    ▼
Railway / FastAPI (main.py)
    │
    ├─ [署名検証] → 即 200 返却
    │
    └─ BackgroundTasks
          │
          ├─ ルーティング判定
          │     ├─ ヒアリング未完了 → ask_claude() [Claude: config.PRIMARY_MODEL]
          │     │     └─ KINTONE_RECORD / KINTONE_UPDATE マーカー抽出 → kintone App 21
          │     │
          │     └─ ヒアリング完了済み → chat_responder.handle_customer_message()
          │           ├─ Claude tool use (compose_reply) で返信案・カテゴリ判定
          │           ├─ AUTO_SEND_CATEGORIES → LINE Reply/Push で即時返信
          │           └─ 承認必須カテゴリ → App 29（承認キュー）保存
          │                 + 弁護士 LINE Push 通知
          │                 + 顧客に定型文返信
          │
          └─ LINE Reply API（失敗時は Push API にフォールバック）

Claude 呼び出しの耐障害化（claude_gateway.py）
    全 Claude 呼び出しは claude_gateway.create_message_with_fallback() を経由:
      PRIMARY_MODEL でエラー（404 / model_not_found / 廃止に伴う400系）
        → FALLBACK_MODEL で1回だけ自動リトライ + 管理者へ LINE Push 警報
      FALLBACK も失敗
        → 顧客には定型の「確認中」応答を返し、App 29 に要対応レコードを作成
    モデル名は config.py（PRIMARY_MODEL / FALLBACK_MODEL）で一元管理。

日次死活監視（daily_healthcheck.py）
    FastAPI startup で起動し、毎日 HEALTHCHECK_HOUR_JST 時（デフォルト 7 時）に実行:
      A. Anthropic Models API で PRIMARY / FALLBACK モデルの有効性確認
      B. kintone App 21/28/29 のフォーム設計（フィールドコード・型・選択肢値）が
         config.EXPECTED_KINTONE_SCHEMA と一致するか検証
    異常時のみ LINE で管理者に通知。正常時はログのみ。

kintone
    ├─ App 21: 案件レコード（LINEユーザーID / status / 顧客情報）
    ├─ App 28: チャットログ
    └─ App 29: 承認キュー（AI下書き / ステータス2 / 送信済み）

その他エンドポイント
    ├─ POST /webhook/stripe              Stripe 決済完了 → App 21 登録
    ├─ POST /cloudsign/webhook/{secret}  CloudSign 締結完了 → kintone 更新
    ├─ POST /document/{secret}           kintone Webhook → 送付状 docx 自動生成
    ├─ POST /webhook/kintone/approval    承認キュー承認済 → LINE Push 送信
    ├─ POST /scan                        GAS PDF → OCR → Claude 抽出 → kintone
    └─ POST /ocr/fixed-asset             固定資産税 PDF → OCR → kintone
```

---

## 環境変数一覧

| 変数名 | 説明 |
|---|---|
| `LINE_CHANNEL_SECRET` | LINE Bot チャンネルシークレット |
| `LINE_CHANNEL_ACCESS_TOKEN` | LINE Bot チャンネルアクセストークン |
| `ANTHROPIC_API_KEY` | Anthropic Claude API キー |
| `KINTONE_SUBDOMAIN` | kintone サブドメイン（例: `edmjisxyx9uc`） |
| `KINTONE_APP_ID` | App 21 のアプリID |
| `KINTONE_API_TOKEN` | App 21 の API トークン |
| `APP_CHATLOG` | App 28 のアプリID |
| `TOKEN_CHATLOG` | App 28 の API トークン |
| `APP_APPROVAL` | App 29 のアプリID |
| `TOKEN_APPROVAL` | App 29 の API トークン |
| `ATTORNEY_LINE_USER_ID` | 弁護士の LINE ユーザーID（承認通知送信先） |
| `KINTONE_WEBHOOK_TOKEN` | 承認 Webhook の合言葉トークン |
| `CLOUDSIGN_CLIENT_ID` | クラウドサイン API クライアントID |
| `CLOUDSIGN_WEBHOOK_SECRET` | CloudSign Webhook 合言葉 |
| `DOCUMENT_WEBHOOK_SECRET` | 送付状生成 Webhook 合言葉 |
| `STRIPE_WEBHOOK_SECRET` | Stripe Webhook 署名シークレット |
| `LINE_ADMIN_USER_ID` | （任意）管理者通知先。未設定なら `ATTORNEY_LINE_USER_ID` に通知 |
| `HEALTHCHECK_HOUR_JST` | （任意）日次死活監視の実行時刻（JST・デフォルト `7`） |
| `HEALTHCHECK_DISABLED` | （任意）`1` で日次死活監視を無効化 |

---

## 日常運用

### 承認フローの運用手順

1. 弁護士の LINE に「【承認依頼】」通知が届く
2. kintone App 29（承認キュー）を開く
3. 該当レコードの「AI下書き」を確認・必要に応じて編集
4. 「ステータス2」フィールドを **`承認済`** に変更して保存
5. kintone Webhook が `/webhook/kintone/approval?token=...` を叩き、
   編集後の「AI下書き」が顧客の LINE に Push 送信される
6. 「送信済み」が自動で `yes` に更新される（二重送信防止）

### AUTO_SEND_CATEGORIES の調整

`chat_responder.py` の `AUTO_SEND_CATEGORIES` セットを編集する。
現在の自動送信カテゴリ（弁護士確認不要）:

```python
AUTO_SEND_CATEGORIES = {
    "挨拶・雑談",
    "手続きの一般的な流れ",
    "必要書類の案内",
    "費用の定型案内",
    "進捗の事実回答",
    "営業案内・アクセス",
}
```

変更後は feature ブランチでコミット → PR → main マージ → Railway 自動デプロイ。

### モデル廃止通知が来たときの運用手順

Anthropic からモデル廃止（deprecation / retirement）通知メールが来たら、
**テスト実行 → 合格 → 設定のモデル名更新** の順で対応する。

1. **後継モデルを決める**
   通知メールまたは https://platform.claude.com/docs/en/about-claude/models/overview
   で後継モデルの ID を確認する（例: `claude-sonnet-4-6` → `claude-sonnet-5`）。

2. **feature ブランチで `config.py` を更新**
   ```python
   PRIMARY_MODEL  = "<新モデルID>"
   FALLBACK_MODEL = "<さらにその予備。PRIMARYと同時に廃止されないモデル>"
   ```
   ※ モデル系列によって `FALLBACK_EXTRA_PARAMS` の調整が必要（config.py のコメント参照）。

3. **回帰テストを実行し、合格を確認**
   ```bash
   railway run python -m pytest test_triage_classification.py -v -s   # 分類一致率 95% 以上で合格
   python -m pytest test_cloudsign_webhook.py -v                       # 既存回帰テスト
   railway run python daily_healthcheck.py                             # 新モデルIDの有効性確認
   ```
   分類一致率が 95% を下回る場合は、`chat_responder.py` のシステムプロンプトを
   調整して再テストするか、別のモデルを検討する。

4. **PR → main マージ → Railway 自動デプロイ**
   デプロイ後、LINE でテストメッセージを1件送って動作確認する。

なお、対応が間に合わないままモデルが廃止された場合も、フォールバック機構が
`FALLBACK_MODEL` で応答を継続し、管理者に LINE で警報が届く（サービスは止まらない）。

### 日次死活監視

毎日 `HEALTHCHECK_HOUR_JST` 時（デフォルト 7 時 JST）にアプリ内で自動実行される。
**異常時のみ** 管理者 LINE に「【日次死活監視: 異常検知】」が届く。

| 通知内容 | 原因 | 対応 |
|---|---|---|
| モデルが Models API に存在しない | モデル廃止 | 上記「モデル廃止通知が来たときの運用手順」を実施 |
| フィールドが存在しない / 型不一致 | kintone アプリのフィールドコード変更・削除 | kintone 側を戻すか、コード（`config.EXPECTED_KINTONE_SCHEMA` と参照箇所）を修正 |
| 選択肢がない | kintone のドロップダウン選択肢の削除・改名 | 同上（コードが書き込む値は選択肢に存在しないと登録エラーになる） |

手動実行（異常があれば exit code 1）:

```bash
railway run python daily_healthcheck.py
```

### Claude フォールバック警報が届いたとき

「【Claudeフォールバック発動】」: PRIMARY_MODEL がモデル起因エラーになり、
FALLBACK_MODEL で応答継続中。サービスは動いているが、早めに
「モデル廃止通知が来たときの運用手順」で PRIMARY_MODEL を更新する。

「【Claude応答不能・要対応】」: 両モデルとも失敗。顧客には「確認中」定型文が
返り、App 29（承認キュー）に `AI障害・要対応` カテゴリのレコードが作成される。
App 29 を開き、AI下書き欄に返信文を記入 →「承認済」にして手動返信する。

### status 変更直後に挙動が不審な場合

kintone で顧客 status を変更した直後に LINE メッセージが届いた場合、
サーバーの in-memory キャッシュ（`conversation_histories`・`hearing_completed`）が
古い状態を保持していることがある。Railway ダッシュボードまたは以下で再起動する:

```bash
railway redeploy --yes
```

再起動後は in-memory がリセットされ、次回メッセージ受信時に App 21 の
最新 status でルーティングが行われる。

### 環境変数の変更時の注意

Railway で環境変数を変更すると**コンテナが自動再起動**される。
再起動中（通常 30〜60 秒）は LINE Webhook の応答が遅延し、
LINE が「配信失敗」と判断するケースがある（LINE はリトライしない）。
**問い合わせの少ない時間帯**（深夜〜早朝）に変更することを推奨。

---

## 既知の不具合（今回スコープ外）

| # | ファイル | 状態 | 内容 |
|---|---|---|---|
| 1 | `cloudsign_webhook.py` | ✅ 修正済 | `FIELD_STATUS = "契約ステータス"` → `"status"` に修正。書き込み値 `"締結済み"`（有効な選択肢外）→ `"受任"` に修正。`test_cloudsign_webhook.py` に回帰テストあり。 |
| 2 | `main.py` | ✅ 修正済 | `業者名`（LOOKUP 型）への直接書き込みを除去（PR #3）。Claude が出力する `問い合わせ業者名`（SINGLE_LINE_TEXT）を直接 kintone へ送るよう変更。※README 初版では `cloudsign_webhook.py` への誤帰属があったため訂正。 |
| 3 | `main.py` `/webhook/stripe` | ⚠️ 未対応 | kintone に `"ステータス"` フィールドコードで書き込んでいるが、kintone 予約語のため実際のフィールドコードが異なる可能性がある（例: `ステータス2`）。要確認。 |
| 4 | 全般 | ⚠️ 未対応 | コンテナ再起動で `conversation_histories` / `kintone_record_ids` / `hearing_completed` が消える。ヒアリング途中のユーザーは最初からやり直しになる。永続化が必要な場合は Redis 等の導入が必要。 |
| 5 | `chat_responder.py` | ✅ 修正済 | `HEARING_STATUSES` に `"受付中"` が入っていたが、App 21 の status 実選択肢は `"受付"`（2026-07-02 フォーム設計 API で確認）。`config.py` で `"受付"` に修正。従来は status=受付 の顧客が誤って受任前モードにルーティングされていた。 |

---

## デプロイ

GitHub `main` ブランチへのプッシュで Railway が自動デプロイする。

```bash
# 開発フロー
git checkout -b feature/xxx
# ... 編集 ...
git add <files>
git commit -m "feat: ..."
git push origin feature/xxx
gh pr create --base main
# レビュー後マージ → Railway 自動デプロイ
```

## ヘルスチェック

```
GET https://jikou-line-bot-production.up.railway.app/health
→ {"status": "ok", "deps": {...}}
```
