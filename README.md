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
    "時効見立て_条件付き",   # v2: 一般論の断言 / 条件付きの個別見立て（留保文言必須）
}
```

変更後は feature ブランチでコミット → PR → main マージ → Railway 自動デプロイ。

### サーバー側ガード（応答方針v2・2026-07 追加）

自動送信の前に `chat_responder.apply_server_guards()` がコードでチェックし、
違反時は承認キューに降格する（降格理由は App 29 の「判断理由」に併記される）:

- **禁止語照合**: 断定語（確実に/絶対に/間違いなく/必ず消滅 等）・
  行動指示語（払わないで/無視して/連絡しないで/放置して/出ないで 等）・
  禁止表現（時効間近）。断定語は否定形（「必ず消滅するとは保証できない」
  「絶対に大丈夫とは言えません」等）を除外する。
  例外として、受任後顧客への定型指示2種（弁護士確認済み文言
  `APPROVED_PHONE_INSTRUCTION`=電話対応 / `APPROVED_DUNNING_INSTRUCTION`=督促状対応）
  は許可リストで通す。督促状定型は裁判所書類の但し書き込みの全文のみ許可
  （但し書きを省略した部分利用は自動的に降格される）。
- **費用の定型案内**: 必須文言（44,000円/税込/前払い/分割払い/不成立時の費用発生）が
  欠けた送信文は降格。固定文は `FEE_GUIDE_TEXT`（金額表記は「44,000円（税込）」で
  統一・2026-07-03 弁護士確定）。会話単位チェック: 固定文を送付済みの顧客への
  続き質問（「3社だといくら」等）には簡潔な回答を許容する。
- **時効見立て_条件付き**: 留保文言（一般論のただし書き or 個別見立ての
  条件+確定留保）が無い送信文、および時効更新事由の疑いフラグ
  （`jikou_update_flag`）が立った顧客への時効見立ては降格。

承認キュー行きの場合も、以下は定型文のみ即時送信される
（`PENDING_REPLY` の代わり。実質回答は承認制のまま）:

| ケース | 定型文 |
|---|---|
| 裁判所書類の第一報 | `COURT_DOC_REQUEST_REPLY`（全ページの写真送付依頼） |
| 諦め・離脱の兆候 | `CHURN_NEUTRAL_REPLY`（中立引き止め文） |
| 税金・個人からの借入れ | `OUT_OF_SCOPE_DEBT_REPLY`（個別案内の予告） |

ガードの回帰テスト（オフライン・APIキー不要）:

```bash
python -m pytest test_server_guards.py -v
```

### FAQ・応答型（システムプロンプト内の弁護士確定知識）

顧客対応Claudeの標準回答は `chat_responder.py` のシステムプロンプト
（`_SYSTEM_PROMPT_BASE`）に集約されている。**数値・条件・言い回しはすべて
弁護士確定済みのため、変更は必ず弁護士の指示に基づき、トリアージ回帰
（実測）を通してからマージすること。**

- FAQ第1弾（v2）: 期間 / 来所不要 / 完了報告 / 信用情報 / 督促停止 /
  家族への秘匿 / 対象債権 / 古い借金 / 業者からの電話 / 家族からの相談
- FAQ第2弾（v2.3）: 本人確認書類 / 督促状なし・業者名不明 / 改姓 /
  発送タイミング / 振込先・領収書・家族名義カード / キャンセル制度 /
  保証人 / 自己破産検討歴 / 減額通知（言い回し厳守） / 業者の反論 /
  過払い金 / 1社のみ / 亡くなった親 / 差押え中 / 生活保護・外国籍・
  海外在住 / 対応時間・所在地・電話番号 / AI活用の開示 / 実績 /
  結果判明まで / 追加依頼の費用 / 証明書なし
- 応答型: 時効見立て_条件付き（A一般論/B個別見立て・留保文言必須）/
  判断分岐提示型（受任前の督促対応）/ 切り分け型（キャンセル制度説明と
  申し出、援用可否と相続放棄の選択相談、差押え中の一般論と第一報）

### 応答方針の変更履歴（2026-07-03）

| PR | 内容 |
|---|---|
| #13 | v2: 時効見立て_条件付き新設・法律知識/FAQ第1弾・即時定型文3種・費用固定文・サーバー側ガード |
| #16 | v2.1: 断定要求の留保付き自動送信・督促対応のフェーズ出し分け・営業固有情報の扱い |
| #14 | Anthropicクレジット残高系エラーの管理者LINE警報化 |
| #17 | v2.2: 費用「44,000円（税込）」確定・法テラスFAQ・費用ガードの会話単位化 |
| #19 | v2.3: FAQ第2弾28項目・「時効間近」禁止語・事務所固有情報の登録 |

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
   python -m pytest test_server_guards.py -v                           # サーバー側ガード（オフライン）
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

「【Anthropicクレジット残高不足・要対応】」: APIクレジットが枯渇し、
Claude が全停止している（フォールバックも同一アカウントのため復旧しない）。
console.anthropic.com の Plans & Billing でクレジットを補充する。
復旧までの間、顧客には「確認中」定型文のみが返る。
※ 2026-07-03 に自動リチャージを設定済みのため通常は発生しない想定。

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
