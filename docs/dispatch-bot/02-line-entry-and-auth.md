# 02. LINE入口と認証：専用アカウント・ホワイトリスト・沈黙拒否

## 1. 専用LINE公式アカウントの構成（確定判断1）

- 新規にLINE公式アカウント（Messaging APIチャネル）を1つ作成する【人・LINE Developers】
  - アカウント名の例:「大野法律事務所 業務指示」（顧客の目に触れないため任意）
  - 応答モード: Bot（応答メッセージOFF・Webhook ON）
  - 友だち追加はオーナー（と、将来許可する事務員）のみ。**友だち追加用QRは公開しない**
- Webhook URL: `https://<Railway本番ドメイン>/webhook/dispatch-bot`
- 顧客Bot（時効援用公式アカウント）・弁護士向け通知チャネルとは**チャネルもsecretも
  アクセストークンも完全に別**。分離理由は 01 §5 のとおり（OKの意味の固定）

## 2. 環境変数（新設。トークン直書き禁止）

| 変数名 | 用途 |
|---|---|
| `DISPATCHBOT_CHANNEL_SECRET` | 指示Botチャネルの署名検証用secret |
| `DISPATCHBOT_CHANNEL_ACCESS_TOKEN` | 指示Botからの返信・push用トークン |
| `DISPATCHBOT_ALLOWED_USER_IDS` | ホワイトリスト。LINE userId のカンマ区切り（第1弾はオーナー1名を想定） |

- 命名は既存流儀（`LINE_CHANNEL_SECRET`/`LINE_CHANNEL_ACCESS_TOKEN` が顧客Bot用に
  既に使われているため、接頭辞 `DISPATCHBOT_` で明確に分離する）
- **env未設定時の挙動**: `DISPATCHBOT_CHANNEL_SECRET` 未設定なら署名検証は常に失敗
  （=エンドポイント事実上無効）。`DISPATCHBOT_ALLOWED_USER_IDS` 未設定・空なら
  **全userIdを拒否**（deny-all。hub/webhook_auth.verify_token の「env未設定=全拒否」と同思想）

## 3. Webhook受信（確定判断10）

```
POST /webhook/dispatch-bot
  1. X-Line-Signature を DISPATCHBOT_CHANNEL_SECRET で HMAC-SHA256 検証
     （main.py verify_signature と同方式。ただし secret は指示Bot専用のものを使う）
     NG → 400（LINEプラットフォーム以外からの偽装。既存 /webhook と同じ）
  2. 即 200 を返し、イベント処理は BackgroundTasks へ（LINE 2秒タイムアウト対策・既存流儀）
  3. BackgroundTasks 内:
     a. event.type == "message" かつ text のみ処理（それ以外は無視）
     b. userId をホワイトリスト判定（§4）
     c. 通過 → 解析へ（03）
```

- 実装は新規パッケージ `dispatch_bot/`（router.py）に閉じ、main.py には
  `app.include_router(dispatch_bot_router)` の1行だけ追加する
  （cloudsign_router / hub_dispatch_router と同じ追加方式。**既存の /webhook
  （顧客Bot）のコードパスには一切手を入れない**）

## 4. ホワイトリスト外からの入力: 沈黙拒否＋警報（確定判断10）

- ホワイトリスト外の userId からのメッセージには**応答を一切返さない**（replyもpushもしない）
  - 防御思想は `/hub/dispatch` の「token無しは404で存在しないフリ」と同じ:
    アカウントの存在・用途を探られても、反応がなければ攻撃面が見えない
- 同時に管理者LINE（既存の `hub/notify.notify_admin_line`・通知チャネル側）へ警報:

```
【指示Bot: 許可外ユーザーからの入力】
userId: U1234...（先頭10文字のみ）
本文: <先頭50文字>
→ 応答は返していません。心当たりがある場合は
   DISPATCHBOT_ALLOWED_USER_IDS への追加を検討してください。
```

- throttle_key=`dispatchbot_unauthorized:<userId>`（同一人物の連投で警報が
  洪水にならないよう既存のスロットル300秒に乗せる）
- 友だち追加イベント（follow）も同様: ホワイトリスト外なら沈黙＋警報のみ

## 5. 顧客Bot・通知チャネルとの分離設計（確定判断1・9）

| 項目 | 顧客Bot（既存） | 指示Bot（新設） |
|---|---|---|
| エンドポイント | POST /webhook | POST /webhook/dispatch-bot |
| secret/token | LINE_CHANNEL_SECRET 等 | DISPATCHBOT_* |
| 応答生成 | chat_responder（ガード・FAQ・トリアージ） | dispatch_bot/（**chat_responder不使用**） |
| システムプロンプト | 顧客対応用 | 指示解析用（03 §5。完全別物） |
| 会話ログ | App 28 チャットログ | **App 28 に書かない**（§6） |
| Claude呼び出し | claude_gateway 経由 | claude_gateway 経由（**ここだけ共用**・context="指示Bot" を明示） |
| 警報送信先 | 通知チャネル（LINE_USER_ID 等） | 同左（警報は従来の通知チャネルに出す。指示Botは警報の出口にしない） |

- 指示Botの障害・例外が顧客対応に波及しない構造:
  ルーター・状態・プロンプトすべて独立。共有点は claude_gateway と hub/ 共通
  ライブラリ（notify・kintone）のみで、いずれも読み出し方向の共用
- ClaudeUnavailableError（両モデル断）時の指示Bot挙動: オーナーに
  「現在AIが応答できません。復旧後にもう一度指示してください」と定型返信
  （顧客Botのような承認キュー起票はしない。オーナー自身が指示者のため定型で足りる）

## 6. ログ保存先

- **App 28（チャットログ）には保存しない**（確定判断9: ログを顧客Botと混ぜない）
- 第1弾: Railwayログ（print/logging）に `[DISPATCHBOT]` プレフィックスで
  受信・解析結果・復唱・OK/期限切れ・起票結果を出力（既存の [WEBHOOK]/[PROCESS] 流儀）
- 第2弾以降: 実行キュー（EXEC_QUEUE）のレコード自体が指示の恒久ログになる
  （指示原文・解釈結果JSON・実行ログのフィールドを持つ。04 §1）。
  App 30 起票分はチャネル固有データに `dispatch_bot` 由来メタ（指示原文・userId・
  解釈日時）を残す（03 §6）

## 7. テスト方法

- 単体（unittest・既存流儀）:
  - 署名検証: 正しい署名で200・不正署名で400・secret未設定で400
  - ホワイトリスト: 許可userIdで処理継続・非許可で「reply/push呼び出しゼロ＋
    notify_admin_line 1回（throttle_key検証）」・env空でdeny-all
  - 即200: BackgroundTasks に積まれること（TestClient・test_hub_dispatch と同型）
  - 顧客Bot非干渉: /webhook（既存）のテストが無変更で全PASSのまま
- 実機（ローカル→本番の順）:
  1. LINE Developersコンソールで指示BotのWebhook URLに `/webhook/dispatch-bot` を設定・検証ボタン
  2. オーナーのスマホから「テスト」と送信 → Railwayログに [DISPATCHBOT] 受信が出る
  3. 別アカウント（ホワイトリスト外）から送信 → 応答なし＋管理者LINEに警報が届く
