# 07. 実装タスク分解（D系列）

- 1タスク=1セッション・完了条件付き（T/H系列と同じ発注方式。09-implementation-plan §2 の雛形を使用）
- 共通制約: 既存テスト無変更で全PASS／顧客Bot（/webhook）の挙動を変えない／
  デプロイ・push は発注者の指示があるまでしない／トークン直書き禁止

## 0. T/H系列との依存関係（サマリ）

| 依存先 | 状態（2026-07-04） | 依存する内容 |
|---|---|---|
| T0-1/T0-2（hub/kintone・notify・scheduler） | 実施済み | 案件検索・警報・将来のdigestジョブ |
| T1-1/T1-2（App 30・状態機械・/hub/dispatch） | 実施済み | 起票先・起票後のprepare自動実行 |
| T2-1/T2-2（M4 送付案内） | 実施済み | D3 の対応タスク（送付案内） |
| T3-1〜T3-3（M1 職務上請求） | 実施済み | D4 の対応タスク（職務上請求） |
| H8（CloudSign/Stripe一般化） | 未実施 | 台帳2-5（契約書送信の一気通貫）の前提。**D7 は generate_draft_only のため H8 に依存しない** |
| S系列（財産目録・協議書） | 未実施 | D12 の前提（S1〜S3・S6） |
| T4系（M5 /scan/v2） | 未実施 | D9 の OCR投入は現行 /scan 経路で実装可（v2化されたら追従） |

D系列内の依存: D1→D2→D3（第1弾・直列）→D4（第1.5弾）。
D5→D6→D7〜D10（第2弾・D5のkintone作成が先行）。D11〜D13（第3弾）。D14〜（第4弾・構想）。

---

## 第1弾（対応タスク: 送付案内のみ）

### D1 LINE入口＋ホワイトリスト＋沈黙警報　★実施済み（2026-07-04）
- 目的: /webhook/dispatch-bot の新設と認証境界の確立（02）
- 実装ノート（2026-07-04）:
  - dispatch_bot/router.py 新設。main.py への変更は import＋include_router の2行のみ
    （既存ルーターと同ブロック・顧客Bot /webhook のコードパス変更ゼロをdiffで確認）
  - 署名検証は DISPATCHBOT_CHANNEL_SECRET 専用（顧客Bot secret では通らないことを
    テストで担保）。secret未設定=常に400・ホワイトリスト未設定/空=deny-all
  - 許可外は沈黙＋notify_admin_line（throttle_key=dispatchbot_unauthorized:<userId>・
    警報にはuserId先頭10文字と本文先頭50文字のみ）。follow イベントも同様
  - 固定応答は reply→失敗時 push フォールバック（DISPATCHBOT_CHANNEL_ACCESS_TOKEN 使用）
  - env 3種は 2026-07-04 に Railway 登録済み（ALLOWED_USER_IDS=オーナー1名）
  - テスト: test_dispatch_bot_entry.py 16件（chat_responder 非import・顧客トークン
    不使用・App 28 非書き込みのソース検査を含む）。全体 363 passed / 2 skipped
- 依存: なし（既存 hub/notify・webhook流儀のみ）
- 変更対象: main.py（include_router 1行のみ）
- 新規: `dispatch_bot/__init__.py`・`dispatch_bot/router.py`・`test_dispatch_bot_entry.py`
- kintone作業: なし
- 環境変数: `DISPATCHBOT_CHANNEL_SECRET`・`DISPATCHBOT_CHANNEL_ACCESS_TOKEN`・
  `DISPATCHBOT_ALLOWED_USER_IDS`【人: LINE公式アカウント新規作成＋railway variables 登録】
- テスト条件: 署名検証（正/不正/未設定）・ホワイトリスト（許可/拒否は沈黙+警報1回/env空=全拒否）・
  即200+BackgroundTasks・受信テキストのエコー返信（仮実装。D2で差し替え）
- 完了条件: [ ] 新規テストPASS [ ] 既存テスト無変更で全PASS [ ] /webhook（顧客Bot）のコードパス変更ゼロ（diffで確認）
- デプロイ前確認: main.py の差分が include_router 1行であること
- 実機確認: LINE Developers でWebhook検証→オーナー送信でエコー→別アカウントで沈黙+警報

### D2 自然言語解析＋案件検索　★実施済み（2026-07-04）
- 目的: claude_gateway 経由の構造化解析と App 21 横断検索（03）
- 実装ノート（2026-07-04）:
  - dispatch_bot/ に parser.py（tool use強制・context="指示Bot解析"・正規化）・
    case_search.py（顧客名like・0件時は空白除去/姓のみで1回だけ再検索・完了/不受任は⚠付き）・
    registry.py（TaskSpec＋soufu_annaiのみ・解析プロンプトの種別一覧はレジストリから自動生成）・
    handler.py（応答組み立て・セッション）を新設
  - 聞き返しセッションはインメモリ30分TTL・ユーザーごと最大1件。回答は元指示に
    「（追加回答）」で結合して再解析。2往復で打ち切り（03 §7 どおり）
  - 複数候補は番号選択（同姓同名はNo・statusで区別）。範囲外番号は再案内
  - D2の終点は【解釈結果】の提示（復唱・pending・起票はD3）。intent=confirm は
    「確認待ちの指示はありません」・query は「第2弾で実装」・未対応タスクは
    「第1弾では送付案内のみ対応しています」
  - ClaudeUnavailableError は定型返信（起票ゼロ・顧客Botのような承認キュー起票はしない）
  - D1テストのうち固定応答を検証していた2件は、D2仕様（⑤固定応答の置換）に伴い
    handler モック方式に更新（それ以外の既存テストは無変更）
  - テスト: test_dispatch_bot_parser.py 26件。全体 389 passed / 2 skipped
- 依存: D1
- 変更対象: dispatch_bot/router.py（エコーを解析に差し替え）
- 新規: `dispatch_bot/parser.py`（tool useスキーマ・プロンプト）・`dispatch_bot/case_search.py`・
  `dispatch_bot/registry.py`（TaskSpec骨格＋soufu_annaiエントリのみ）・`test_dispatch_bot_parser.py`
- kintone作業: なし
- 環境変数: なし（既存 KINTONE_APP_ID/KINTONE_API_TOKEN を検索に使用）
- テスト条件: 代表指示→期待JSONゴールデン（送付案内・OK・キャンセル・雑談→unknown）・
  案件検索1件/複数選択肢/0件/No直指定・完了案件警告・ClaudeUnavailable→定型返信
- 完了条件: [ ] ゴールデンテストPASS [ ] chat_responder を import しないこと（テストで静的検査） [ ] 既存全PASS

### D3 復唱＋pending＋App 30起票（送付案内）　★実施済み（2026-07-04）
- 目的: 第1弾の完成。OK→App 30 下書き起票→既存prepare合流（06・05 §3.1）
- 実装ノート（2026-07-04）:
  - dispatch_bot/confirm.py: Pending（UUID・30分TTL・単回消込・ユーザーごと最大1件・
    インメモリ=再起動で安全側）・リスク別復唱テンプレ（低=簡潔版2行。中高用フルテンプレも
    実装済みだが D3 の登録タスクは低のみ）
  - dispatch_bot/app30_filer.py: App 30 へ「下書き」起票のみ（発送ステータスを先へ進める
    コードなし=承認原則の維持をソース検査テストで固定）。宛先は App 21 から解決。
    チャネル固有データに dispatch_bot メタ（指示原文/userId/解釈日時/pending_command_id）
  - 二重実行防止の多層: pending 単回消込＋起票直前の pending_command_id 既存検索
    （既存検出時は「起票済みです・二重実行を防止しました」）
  - 割込み無効化: pending 有効中の別指示は「先ほどの確認は取り消しました。」を前置し
    新しい解析へ。OK/キャンセル/番号選択/聞き返し回答のみ現対話への応答
  - 起票失敗: ユーザーに定型返信＋管理者警報（throttle_key=dispatchbot_filing_error）。
    pending は消込（再指示でやり直し）
  - キャンセル（pendingあり）=「キャンセルしました。もう一度指示し直してください」
  - D2テストのうち終点【解釈結果】提示を検証していた4箇所を復唱検証に更新
    （仕様進化に伴うもの。それ以外の既存テストは無変更）
  - テスト: test_dispatch_bot_confirm.py 12件。全体 407 passed / 2 skipped
- **これで第1弾（LINE→送付案内起票）完成**。実機一巡: 指示→復唱→OK→App 30 に
  下書き→既存 Webhook が prepare→承認待ち＋弁護士LINE→（kintoneで承認）→印刷指示
- 依存: D2・T2-2（実施済み）
- 変更対象: dispatch_bot/router.py・registry.py
- 新規: `dispatch_bot/confirm.py`（復唱テンプレ2種・pending管理〔インメモリ・30分・単回・割込み無効〕）・
  `dispatch_bot/app30_filer.py`（起票のみ。prepareは呼ばない）・`test_dispatch_bot_confirm.py`
- kintone作業: なし（App 30 は既存）
- 環境変数: なし（APP_SHIPPING/TOKEN_SHIPPING 既存を使用）
- テスト条件: 復唱簡潔版の文言・OK→起票フィールド全項目（チャネル固有データの dispatch_bot メタ含む）・
  期限切れOK無視・二重OK・割込み無効化・pending_command_id 重複起票ガード・
  再起動消失＝期限切れ扱いのシミュレーション
- 完了条件: [ ] 一巡テスト（指示→復唱→OK→App 30下書き作成）PASS [ ] LINE OKで発送ステータスが
  下書きより先へ進まないこと（承認原則の検証） [ ] 既存全PASS
- 実機確認: 「〇〇さんに送付案内を作って」→復唱→OK→kintoneに下書き→既存Webhookで
  承認待ち+成果物が付くこと（発送承認は従来どおりkintoneで）

## 第1.5弾

### D4 職務上請求の指示対応（聞き返しフロー込み）　★実施済み（2026-07-04）
- 目的: チャネル固有JSON（request_items/target/municipality）の対話的な組み立て（03 §7・05 §3.1）
- 実装ノート（2026-07-04）:
  - **第一工程（教訓③）: 必須項目の洗い出し**を実物（parse_channel_data /
    build_request_form_pdfs / find_municipality）から実施。結果は dispatch_bot/shokumu.py
    冒頭に明文化: コード必須=request_items（種別∈6種・通数≥1）・municipality・
    様式1（戸籍系）を含む場合の target.生年月日／実務必須扱い=target.対象者／
    任意=フリガナ・本籍・住所・筆頭者・世帯主・purpose（既定文言 DEFAULT_PURPOSE）
  - レジストリに shokumu_seikyu 登録（リスク=中・App 30起票・max_clarify=8）。
    TaskSpec に D4 フック追加: param_normalizer / missing_param_fn / pre_confirm_fn /
    choice_fn / summary_fn / max_clarify / required_desc（handler はフックの有無だけを見る）
  - 聞き返し多段化（03 §7 の D4 差分どおり）: 1論点1往復×必要項目・同一論点の再質問1回まで・
    全体8往復で打ち切り。質問順=種別通数→自治体→対象者→生年月日（様式1のみ・様式2のみなら聞かない）
  - 一括抽出: 解析ヒントで request_items/municipality/target を tool use 抽出し、
    取れた項目の聞き返しはスキップ。未対応種別・通数不正は normalize で落として聞き返しへ
  - App 31 照合（pre_confirm）: 登録済み→小為替概算を復唱に表示（手数料未登録は「概算不能」）／
    未登録→「1. 中止 / 2. このまま起票（PrepareDeferred の既存挙動で警報→登録→再保存）」の選択
  - 復唱は中リスクのフルテンプレ（06 §2.2）＋summary_fn の明細（対象者・種別と通数・
    宛先自治体・小為替概算・「発送には kintone での承認が別途必要」注記）
  - 起票: app30_filer を file_from_pending にタスク汎用化。チャネル固有JSONは
    parse_channel_data 実物を通す形式＋dispatch_bot 監査メタ併記（マージ基盤の上）
  - テスト: test_dispatch_bot_shokumu.py 12件（全項目聞く経路・一括抽出スキップ・
    様式2のみ生年月日スキップ・同一論点打ち切り・8往復打ち切り・App 31未登録3分岐・
    手数料未登録の概算注記・parse_channel_data 実通過）。全体 440 passed / 2 skipped
  - 既存テスト3件を「shokumu_seikyu=未登録」前提から更新（未登録題材を fax_send に変更）
- 依存: D3・T3-3（実施済み）
- 変更対象: registry.py（shokumu_seikyuエントリ追加）・parser.py（missing_fields聞き返しの往復結合）
- 新規: `test_dispatch_bot_shokumu.py`
- kintone作業: なし
- テスト条件: 不足項目の聞き返し→回答結合→フルテンプレ復唱（中リスク）→起票JSONが
  `parse_channel_data`（channels/shokumu_seikyu）の検証を通ること・様式1の生年月日必須の
  事前検知（欠けたまま起票せず聞き返す）・2往復打ち切り
- 完了条件: [ ] 聞き返し一巡テストPASS [ ] 起票JSONが既存 prepare でエラーなく処理される結線テストPASS [ ] 既存全PASS

## 第2弾（実行キュー）

### D5 実行キューアプリ作成【人・GUI】＋監視登録
- 目的: EXEC_QUEUE の実体化（04）。**App番号はこの時点で確定**（確定判断4）
- 依存: なし（D6の前提）
- 作業: 【人】kintone GUIでアプリ作成（04 §1 のフィールド23個。cu-app30.md と同型の
  Computer Use 指示書を作成してもよい）・APIトークン発行・
  `railway variables --set APP_EXEC_QUEUE=<実番号> --set TOKEN_EXEC_QUEUE=<トークン>`
  【Sonnet】EXPECTED_KINTONE_SCHEMA への登録・スキーマ突合スクリプトでの機械検収
- 完了条件: [ ] form/fields.json 突合で全フィールド一致 [ ] 死活監視組み込み [ ] 既存全PASS

### D6 実行キュー結線（状態機械・Webhook受け口・pending永続化判断）
- 目的: 受付→確認待ち→実行待ち→処理中→完了/エラー の遷移実装（04 §2）
- 依存: D5
- 新規: `dispatch_bot/exec_queue.py`（遷移表・claim・/exec-queue/dispatch受け口）・テスト
- 環境変数: `EXEC_QUEUE_WEBHOOK_TOKEN`
- 設計判断ポイント（実装時に決定・04 §3）: pending を確認待ちレコード方式へ移すか、
  インメモリ継続か（キャンセル堆積とのトレードオフ）
- 完了条件: [ ] 遷移表網羅テストPASS [ ] claim冪等テストPASS [ ] token無し404 [ ] 既存全PASS

### D7 委任契約書の生成タスク（generate_draft_only）
- 目的: contract_draft の実行アダプタ（05 §3.3。**台帳2-5への接続点**）
- 依存: D6・既存 docx_templates/送付状_委任契約書.docx とは別に委任契約書テンプレの
  実物確認【人: テンプレ提供】
- 制約: **CloudSign送信コードを含めない**（auto_scope=generate_draft_only。
  2-5実装時に approval_scope 拡張で吸収）
- 完了条件: [ ] 生成docxがキューに添付され実行ステータス=完了（承認要否=要）で停止 [ ] 送信系APIの
  import が存在しないこと（静的検査） [ ] 既存全PASS

### D8 今日の要対応一覧（即答型）
- 目的: daily_digest ほか照会4種（06 §7・05 §2）
- 依存: D2（即答分岐）。実行キュー項目の集計は D6 後に追加
- テスト条件: 各ソースモック→表示順・畳み・リンク・4900字切り詰め・
  更新系API不使用（モック検査）・外部API不使用
- 完了条件: [ ] 表示ゴールデンテストPASS [ ] 集計と送信の分離（§7.3 の毎朝配信ジョブを
  env切替で登録できる構造） [ ] 既存全PASS

### D9 OCR投入＋Drive整理
- 目的: ocr_intake（現行 /scan 経路への投入）・drive_organize（移動案生成→承認→実行）
- 依存: D6。Drive APIの認可方式は実装時に確定（GAS経由かサービスアカウントか）
- 完了条件: [ ] 投入→結果レコードURL返信 [ ] Drive移動が承認前に実行されないテスト [ ] 既存全PASS

### D10 owner-manual更新候補
- 目的: 実装差分→owner-manual.md 更新候補一覧の生成（適用は人）
- 依存: D6
- 完了条件: [ ] 候補一覧が成果物添付されるのみ（docs自動書き換えなし） [ ] 既存全PASS

## 第3弾

### D11 登記取得準備（touki_prep・課金操作なし）
- 依存: D6。不動産アプリ（既存）参照
### D12 財産目録・協議書ドラフト
- 依存: D6＋**S系列（S1〜S3・S6）実装済みが前提**（未実施。着手時に要確認）
### D13 解決事例作成＋Search Console分析
- 依存: D6。公開は人（対外送信禁止リスト8）

（第3弾の詳細分解は第2弾完了後に本書を改訂して確定する。属性はレジストリ 05 §3.2 が正）

## 第4弾（構想・タスク化しない）

- 事務所PCエージェント連携（実行場所=office_pc のタスク。ocr_watcher の常駐方式を発展）
- 顧客ポータル・不動産業務への横展開
- 着手時に本設計の 01〜06 を前提に別途設計する

## 検収の共通観点（全Dタスク）

- 既存テスト無変更で全PASS（`git ls-files "test_*.py"` 方式で実行）
- /webhook（顧客Bot）・/hub/dispatch の挙動不変（回帰テスト）
- LINE OK で対外効果が発生しない（禁止リスト突合テスト）
- 秘密情報の直書きなし（環境変数のみ）
