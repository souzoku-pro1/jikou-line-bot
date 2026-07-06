# 既存システム 現状調査報告書

- 調査日: 2026-07-03
- 対象: jikou-line-bot リポジトリ（main ブランチ・PR #5 マージ後）および作業ディレクトリ内の未追跡ファイル
- 目的: 外部アドバイザーへの現状共有。**本調査でコード・設定の変更は行っていない**
- 前提知識: 大野法律事務所（埼玉県川口市）の時効援用業務を LINE Bot + kintone + Railway（FastAPI）で自動化したシステム。現在、発送/受領ハブと相続放棄ユニットの設計フェーズが完了し、実装前の段階

---

## 1. リポジトリ全体の棚卸し

### 1.1 Git 管理下のファイル（本番デプロイ対象・15ファイル）

Railway は GitHub main ブランチから自動デプロイ。`Procfile` の `uvicorn main:app` で単一 Web サービスとして稼働。

| ファイル | 役割 |
|---|---|
| `main.py`（927行） | FastAPI 本体。LINE Webhook・ヒアリング Claude・承認 Webhook・Stripe Webhook・OCR 2エンドポイント（/scan・/ocr/fixed-asset）を含む中核 |
| `chat_responder.py`（535行） | 受任前後の顧客対応 Claude（tool use でカテゴリ判定→自動送信 or 承認キュー） |
| `claude_gateway.py`（159行） | 全 Claude 呼び出しの共通ゲートウェイ（PRIMARY→FALLBACK 自動切替＋管理者 LINE 警報・スロットル付き） |
| `config.py`（123行） | モデル名・status ルーティング分類・kintone スキーマ想定値（死活監視の照合元）の一元管理 |
| `daily_healthcheck.py`（198行） | 日次死活監視（モデル有効性＋App 21/28/29 のフォーム設計検証）。アプリ内スケジューラ |
| `cloudsign_webhook.py`（224行） | CloudSign 締結 Webhook → 書類詳細 API で真正性確認 → App 21 status=受任 更新＋LINE 通知 |
| `document_webhook.py`（210行） | kintone Webhook → 送付状 docx 自動生成 → kintone 添付書き戻し（相談カードアプリ用） |
| `registry_to_kintone.py`（295行） | 不動産登記 JSON → kintone 不動産アプリ一括登録の手動 CLI スクリプト |
| `test_cloudsign_webhook.py` / `test_triage_classification.py` | 回帰テスト（unittest。後者は分類一致率 95% 基準・railway run 前提） |
| `conftest.py` | pytest 共通設定（Windows ローカルの SSL 証明書対策） |
| `Procfile` / `requirements.txt` | Railway 起動定義／依存（fastapi, uvicorn, httpx, anthropic, python-multipart, python-docx, requests, stripe） |
| `README.md` | アーキテクチャ・運用手順・既知の不具合一覧 |
| `docx_templates/送付状_委任契約書.docx` | 送付状生成のテンプレート（現在唯一の docx テンプレート） |

### 1.2 未追跡ファイル（作業ディレクトリに同居。デプロイ対象外）

| グループ | ファイル例 | 区分 |
|---|---|---|
| **OCR ローカル運用ツール** | `ocr_watcher.py`（デスクトップの OCR_inbox フォルダを監視し PDF を /ocr/fixed-asset へ自動送信）・`setup_ocr_watcher.bat`（Windows タスクスケジューラへログオン時自動起動を登録） | **本番運用の一部**（ただし事務所 PC 依存・リポジトリ未追跡） |
| OCR 実験スクリプト | `ocr_test.py`（Vision OCR 単体）・`ocr_to_claude.py`（登記 OCR→JSON 構造化） | 作りかけ/実験（registry_to_kintone.py の前工程に相当） |
| 相談カード PDF 生成 | `create_sozo_card.py`（相続放棄相談カード）・`create_souzokujin_card.py`・`create_isan_card.py`・`create_zaisan_sheet.py` | 単発ツール（reportlab 使用。紙の相談カード様式の生成） |
| WordPress 運用 | `wp-auto-poster/`（記事自動投稿・スケジューラ登録バッチあり）・`swell_converter.py`・`post_from_html.py`・`update_*.py`・`wp_*.py`・`insert_common_faq.py`・`redo_faq.py`・`fix_warn_files.py`・`read_*.py`・backup-*.html・`soudan-lp-v2.html` 等 | **別系統**（jikou-law.jp サイト運用。本システムとは独立） |
| 残骸 | `main.py.bak`・`tmp*.docx`・`debug_out.txt`・`docx_content.txt` 等 | 未使用 |
| データ | `kintone_業者マスタ.csv` | 業者マスタ（債権者連絡先）の kintone エクスポート。**コードからの参照はない** |

> ⚠ セキュリティ注意: WordPress 系の一部スクリプト（`update_188.py` 等）に**認証情報が平文で埋め込まれたまま残置**されている。リポジトリ未追跡のため GitHub には出ていないが、整理・失効を推奨。

---

## 2. スキャン OCR パイプラインの現状（最重要）

OCR は**独立した2系統**が稼働している。設計書（docs/architecture/08）が「M5 スキャン受領」として拡張対象にしているのは主に系統Aである。

### 2.1 系統A: Google Drive（GAS）→ `POST /scan`（相続系書類）

```
[トリガー] Google Drive の書類種別フォルダ（相談カード/戸籍謄本/通帳）に PDF が置かれる
    ↓ Google Apps Script（GAS）が検知し、PDF を base64 化して POST
[受信] POST /scan {pdf_base64, folder_name, file_name}   ※main.py 内・Pydantic で camelCase 別名も受理
    ↓
[OCR] _ocr_pdf_bytes(): Vision API files:annotate（DOCUMENT_TEXT_DETECTION・言語ヒント ja/en）
      PDF を base64 のまま直接送信（PyMuPDF 等でのページ分割はしない）
    ↓
[抽出] _extract_by_folder(): フォルダ名に対応するプロンプト（_SCAN_FOLDER_CONFIG）で
      claude_gateway 経由の Claude 呼び出し → JSON 抽出（コードフェンス除去のみの素朴なパース）
    ↓
[転記] _post_scan_to_kintone(): フォルダ別の kintone アプリへ新規レコード作成
      （None 項目は送信しない＝DATE 型 400 エラー対策。全値を str() で文字列化）
```

- **GAS 側の実装は 2026-07-03 に clasp で取得済み**（`legacy/gas/`・正本は GAS 側・
  プロジェクト名「相続書類自動化」）。実体は **約40行の `onFileAdded()` 1関数のみ**:
  - 方式: イベント駆動ではなく**ポーリング型**（3フォルダを毎回全走査し、ファイル名が
    `[済]` で始まらないものを処理）。トリガーは GAS UI 設定の installable トリガーで、
    **周期はコードから確認できない**（時間主導型と推定・要 UI 確認）
  - 処理済み管理: フォルダ移動ではなく**ファイル名の先頭に `[済]` を付与するリネーム方式**。
    エラーファイルの隔離先はない
  - **リトライ・try/catch・警報は一切なし**。Railway が 4xx/5xx を返すと
    `UrlFetchApp.fetch` が例外を投げて**その実行全体が停止**する。失敗ファイルは
    リネームされないため次回トリガーで再試行される（=偶発的リトライ）が、
    **恒久エラーのファイルが1つあると毎回そこで停止し、全フォルダの後続ファイルが
    処理されなくなる**（poison-pill 構造）
  - Railway URL・3フォルダの Drive フォルダ ID はハードコード。送信ペイロードは
    `{fileData, fileName, folderName}`（Railway 側 ScanRequest の別名と一致）
- 【2026-07-06 追記】GAS は**4フォルダ監視**に拡張（`戸籍読解` フォルダ→ multipart で `POST /koseki/ingest`・既存3フォルダ→/scan は無変更。詳細は legacy/gas/README.md）
- Drive フォルダ構成: `相談カード` / `戸籍謄本` / `通帳` の3フォルダ（ID は legacy/gas/ に
  記録）。処理済みファイルも同一フォルダ内に `[済]` 付きで残り続ける

**対応済み書類種別と抽出項目:**

| 種別 | 登録先（環境変数） | 抽出項目 |
|---|---|---|
| 相談カード | `SOUZOKU_KINTONE_APP_ID` / `SOUZOKU_KINTONE_API_TOKEN` | 氏名・生年月日・住所・電話番号・メールアドレス・被相続人名・続柄・被相続人生年月日・被相続人死亡日・被相続人住所・被相続人本籍（11項目）＋ファイル名・登録日時を付加 |
| 戸籍謄本 | `KOSEKI_KINTONE_APP_ID` / `KOSEKI_KINTONE_API_TOKEN` | 氏名・生年月日・死亡日・続柄・婚姻関係・養子縁組・本籍・筆頭者（8項目）＋ファイル名・登録日時 |
| 通帳 | `KINTONE_SCAN_APP_ID_TSUCHOU` / `KINTONE_SCAN_API_TOKEN_TSUCHOU` | 金融機関名・口座番号・名義人・残高（4項目） |

### 2.2 系統B: 事務所 PC 監視 → `POST /ocr/fixed-asset`（固定資産税）

```
[トリガー] 事務所 PC の「デスクトップ/OCR_inbox」フォルダに PDF を置く
    ↓ ocr_watcher.py（watchdog。タスクスケジューラでログオン時自動起動・失敗時1分間隔3回再起動）
    が multipart で POST → 成功時は「送信済み」、失敗時は「エラー」サブフォルダへ移動（ログファイルあり）
[処理] /ocr/fixed-asset: Vision OCR → Claude 抽出（評価額・年度・所在地・地番）
    → 所在地の正規化（都道府県除去・丁目の漢数字化・番地表記統一。丁目は1〜20のみ対応）
    → kintone 不動産アプリを「所在 like」で部分一致検索 → 先頭1件の評価額・年度を上書き更新
    → LINE Push で完了通知（LINE_USER_ID 宛・未設定ならスキップ）
```

### 2.3 現状の完成度

**動いている部分:**
- 系統A/B とも「PDF→OCR→Claude 抽出→kintone 転記」の一巡は実装済みで本番稼働中
- Claude 呼び出しは両系統ともフォールバック機構（claude_gateway）に乗っている
- 系統B はフォルダ移動（送信済み/エラー）とログ・自動再起動まで整備されている

**バグ・未完成・制限:**

| # | 内容 |
|---|---|
| 1 | **原本 PDF が kintone に残らない**（抽出テキストのみ転記。両系統とも添付なし） |
| 2 | **重複投入の防止が `[済]` リネームのみ**（同じ PDF を2回置けば2レコード。GAS のトリガー実行が重なった場合もリネーム前に二重送信され得る） |
| 3 | **案件への紐付けがない**（系統A は常に新規レコード作成。既存案件との照合なし） |
| 4 | 書類種別は**人がフォルダに仕分け**する前提（自動分類なし） |
| 5 | 系統A のエラー処理は**GAS 側に皆無**（取得コードで確認済み）。Railway の 4xx/5xx で GAS 実行が例外停止し、**恒久エラーのファイル1つで全フォルダの後続処理が止まる**。警報にも乗らない |
| 6 | 系統B の検索は「先頭1件を更新」のため、**所在地の部分一致が複数件ヒットしても先頭に上書き**する |
| 7 | Claude の JSON 出力パースが素朴（コードフェンス除去のみ）。不正 JSON は 502 で終わる |
| 8 | OCR・kintone 呼び出しの一部が同期実装（urllib）で async ハンドラをブロックする |
| 9 | `/health` が PyMuPDF（fitz）の有無を報告するが **requirements.txt に含まれておらず**、現行 OCR は PyMuPDF 不要の方式。ヘルスチェック項目が実装と乖離 |

**ハードコードされている主な値:**
- 系統A の許容フォルダ名3種と抽出プロンプト（main.py `_SCAN_FOLDER_CONFIG`）
- 系統B の検索フィールド `所在`、更新フィールド `固定資産税評価額`・`固定資産税評価年度`
- 所在地正規化の丁目変換テーブル（一〜二十）
- 全 kintone アプリが同一サブドメイン前提（`KINTONE_SUBDOMAIN` を共用）
- 系統B の監視フォルダパス（デスクトップ固定・ocr_watcher.py 内）

---

## 3. kintone アプリの現状

### 3.1 コードが参照している全アプリ

| アプリ | ID の注入方法 | 用途 | コードが読み書きする主要フィールド |
|---|---|---|---|
| App 21 案件（時効援用） | env `KINTONE_APP_ID` / `KINTONE_API_TOKEN` ※ただし **Stripe Webhook のみ `"app": 21` をハードコード** | 案件・顧客マスタ | status（DROP_DOWN: 問い合わせ/受付/受任/手続き中/完了/不受任/決済完了）・LINEユーザーID・顧客名・住所・生年月日・電話番号・メールアドレス・問い合わせ業者名・借入時期_テキスト・最終返済日_テキスト・裁判所書類・信用情報確認・cloudsign_document_id。Stripe 経路のみ: Stripe決済ID・入金状況・**「ステータス」**（既知の不具合 #3: 実フィールドコードと不一致の疑い） |
| App 28 チャットログ | env `APP_CHATLOG` / `TOKEN_CHATLOG` | LINE 会話ログ（顧客対応 Claude の文脈・直近10往復） | line_user_id・role・message・category・auto_sent |
| App 29 承認キュー | env `APP_APPROVAL` / `TOKEN_APPROVAL` | AI 下書きの弁護士承認→LINE 送信 | line_user_id・顧客名・顧客メッセージ・AI下書き・カテゴリ・判断理由・ステータス2（承認待ち/承認済）・送信済み（冪等フラグ） |
| 相談カード（相続） | env `SOUZOKU_KINTONE_APP_ID` / `SOUZOKU_KINTONE_API_TOKEN` | /scan 登録先 ＋ document_webhook の送付状生成元 | OCR 11項目＋ファイル名・登録日時／書類ステータス（送付状作成→送付状作成済）・送付状（FILE）・氏名・住所・被相続人名 |
| 戸籍謄本 | env `KOSEKI_KINTONE_APP_ID` / `KOSEKI_KINTONE_API_TOKEN` | /scan 登録先 | OCR 8項目＋ファイル名・登録日時 |
| 通帳 | env `KINTONE_SCAN_APP_ID_TSUCHOU` / `KINTONE_SCAN_API_TOKEN_TSUCHOU` | /scan 登録先 | OCR 4項目 |
| 不動産 | env `KINTONE_FUDOSAN_APP_ID` / `KINTONE_FUDOSAN_API_TOKEN` | /ocr/fixed-asset 更新先・registry_to_kintone 登録先 | 所在・地番・種別・地目・地積・床面積1〜3階・持分割合・担保抵当権・担保内容・固定資産税評価額・固定資産税評価年度 ほか |

### 3.2 補足

- **日次死活監視（スキーマ検証）の対象は App 21/28/29 のみ**。相続系4アプリ（相談カード・戸籍・通帳・不動産）はフィールド改名等の破壊的変更を検知できない
- 業者マスタ（債権者の FAX・住所）は CSV エクスポートが手元にあるのみで、コードは未参照
- 設計書で新設予定の App 30〜35（発送管理・市区町村・同封物・相続放棄案件・予約枠・家裁マスタ）は**すべて未作成**

---

## 4. 外部連携の現状

| 連携先 | 実装状況 | 手動のままの箇所 |
|---|---|---|
| **LINE Messaging API** | Webhook 署名検証・即時200＋BackgroundTasks 処理・Reply 失敗時 Push フォールバック・管理者/弁護士への警報 Push（スロットル付き）。実装は main.py / chat_responder / claude_gateway / cloudsign_webhook に**4重に分散** | 承認キューの承認操作（設計どおりの意図的な人手）。公式アカウントは時効援用の1チャネルのみ |
| **CloudSign** | 締結 Webhook 受信（URL 合言葉）→書類詳細 API で真正性確認（トークン自動更新・401 リトライ付き）→App 21 status=受任 更新→LINE 通知 | **契約書の作成・送信は手動**。documentID の App 21 への保存も送信時の手作業前提。締結後の後続処理（書類収集開始等）は未接続 |
| **Stripe** | checkout.session.completed の Webhook 受信（署名検証あり）→App 21 に**新規レコード作成** | **決済リンクの作成・送付は手動**。既存案件への紐付けなし（新規作成のため案件が二重になり得る）＋不具合 #3 未修正 |
| **Claude API** | claude_gateway に一元化済み（PRIMARY=claude-sonnet-4-6 / FALLBACK=claude-sonnet-5・モデル起因エラーの自動切替・両断時は承認キューへ要対応レコード）。日次でモデル有効性を監視 | モデル更新の判断・config 書き換え（運用手順書あり） |
| **Google Vision API** | API キー方式で files:annotate を直接呼び出し（OCR 2系統で共用） | — |
| **Google Drive / GAS** | Railway 側の受け口（/scan）＋GAS ポーリングスクリプト（約40行・`legacy/gas/` に写し取得済み 2026-07-03）。エラー処理・警報なし（§2.1） | フォルダへの書類仕分け（人手）・エラー時の `[済]` 手動リネーム・トリガー周期の管理（GAS UI） |

---

## 5. 昨日までの成果物（設計書）の所在

いずれも main にマージ済み（PR #5）。実装コードは未着手。

**docs/architecture/**（発送/受領ハブ・11ファイル）:
README.md / 01-overview.md / 02-kintone-design.md / 03-common-components.md /
04-module-01-shokumu-seikyu.md / 05-module-02-enaishomei-csv.md / 06-module-03-fax.md /
06a-fax-provider.md / 07-module-04-soufu-annai.md / 08-module-05-scan-pipeline.md /
09-implementation-plan.md

**docs/souzoku-houki/**（相続放棄ユニット・11ファイル）:
README.md / 01-flow.md / 02-bot-hearing.md / 03-jukuryo-kikan.md / 04-kintone-design.md /
05-reservation.md / 06-documents.md / 07-reuse.md / 08-implementation-plan.md /
09-shinjutsu-package.md / 10-koseki-matrix.md

---

## 6. ギャップ一覧（設計書の前提 vs コードの現状）

設計書は「現状をこう変える」文書なので大半は計画どおりの未実装だが、
**設計が既存事実として前提にしている事項と現状が食い違う箇所**、および実装前に解消すべき事項を挙げる。

| # | 設計書の前提・記述 | 実際の現状 | 影響 |
|---|---|---|---|
| 1 | GAS の写しを docs で管理する方針（architecture/08 §6） | **解消済み（2026-07-03）**: `legacy/gas/` に clasp で取得（正本は GAS 側・同期は clasp pull）。判明事項: ポーリング型・`[済]` リネーム方式・エラー処理なし（poison-pill 構造・§2.1）。**トリガー周期のみ GAS UI での確認が未了** | T4 系（/scan/v2・GAS 改修）の設計前提が確定。設計 08 §6 の「処理済み/要確認フォルダへ移動」方式は現行の「リネーム方式」からの変更になる点に注意 |
| 2 | 時効援用ユニットの案件アプリ参照は env 注入が規約（architecture/02 冒頭） | Stripe Webhook のみ `"app": 21` と `"ステータス"` をハードコード（既知の不具合 #3） | 設計 G3/H8 で解消予定と整合済み。ただし**それまで Stripe 決済は案件二重登録のリスクが残存** |
| 3 | hub/ 共通化の移設元として「LINE Push は4重実装」「kintone I/O は5重実装」（architecture/03） | 実態と一致（main / chat_responder / cloudsign_webhook / claude_gateway / document_webhook / daily_healthcheck に分散） | 食い違いなし。T0 系リファクタの規模感の裏付け |
| 4 | docx テンプレートは `docx_templates/<ユニット>/<種別>.docx` の規約配置（architecture/03 §6） | 現状はルート直下に1ファイルのみ（`送付状_委任契約書.docx`）。ユニット別ディレクトリなし | T0-3（resolve_template）実装時に既存ファイルの互換パス維持が必要（設計に明記済み） |
| 5 | reportlab を新規採用予定（architecture/03 §7・requirements 追加は T1-3） | サーバー側 requirements に未追加。ただし**ローカルの相談カード生成スクリプトで使用実績あり**（フォント登録ノウハウが既にある） | プラス材料。T1-3 の参考実装としてローカルスクリプトが使える |
| 6 | M5 の受領先アプリに `Drive_fileId`・`原本PDF` フィールドを追加予定（architecture/08 §5） | 現行 /scan は原本添付・冪等キーとも無し（§2.3 の制限 1・2） | 計画どおりの未実装。ただし**それまで重複登録・原本不在が本番で継続** |
| 7 | 死活監視は「新設アプリを作った直後に EXPECTED_KINTONE_SCHEMA へ登録」が規約 | 既存の相続系4アプリ（相談カード・戸籍・通帳・不動産）が**現時点で監視対象外** | 規約を過去分に遡及適用するタスクがどの計画にも入っていない。**小タスクとして追加を推奨** |
| 8 | 相続放棄設計は「App 28 チャットログを LINE 2チャネルでそのまま共用」（souzoku-houki/07） | App 28 は line_user_id でのみ分離。同一人物が両チャネルを友だち追加した場合の userId は LINE 仕様上チャネルごとに異なるため実害はないが、**ユニット識別列がない** | 運用上は識別可能（userId が異なる）。将来の集計要件次第で列追加 |
| 9 | 設計の警報・ジョブ基盤（hub/scheduler のジョブレジストリ）前提 | 現状のスケジューラは daily_healthcheck 専用の単機能ループ | 計画どおり（T0-2 で一般化）。熟慮期間ジョブ等はこれが前提のため T0-2 が事実上の最優先 |
| 10 | `/health` は依存チェックの受け口（設計 T1-3 で reportlab を追加予定） | 現状チェック対象は PyMuPDF のみで、しかも requirements に無く常に NG 表示の可能性（§2.3 #9） | ヘルスチェックの実態乖離。T1-3 のついでに整理可能 |
| 11 | 業者マスタを M2/M3 の宛先ソースにする前提（architecture/02 §5。アプリID・フィールドコードは「実機確認タスクあり」） | コードからの参照ゼロ・手元に CSV のみ。kintone アプリとしての実在・フィールド構成は未確認 | 設計に「要実機確認」と明記済みだが、**T5/T6 着手前の確認事項**として残存 |
| 12 | ヒアリング状態はサーバー再起動で消える既知不具合 #4（README） | 未解消（conversation_histories 等が in-memory） | 相続放棄側は逐次 upsert 設計（H3）で回避予定だが、**時効援用の現行ヒアリングには残存** |

### 総括

- 本番で安定稼働しているのは「LINE ヒアリング〜承認フロー」「CloudSign/Stripe の受信側」「OCR 2系統の一巡」で、耐障害化（フォールバック・死活監視・回帰テスト）も App 21/28/29 の範囲では整備済み
- 最大の構造的課題は、①OCR パイプラインに冪等性・案件紐付け・原本保全がないこと、②送信系（契約書送付・決済リンク・書類発送）がすべて手動であること、③GAS と事務所 PC 常駐スクリプトという**リポジトリ外・監視外のコンポーネント**が本番経路に含まれること
- ①②は設計書（T系列/H系列タスク）でカバー済み。③のうち GAS の写しは 2026-07-03 に取得済み（`legacy/gas/`）。残る推奨事項は、ギャップ #7（既存アプリの死活監視追加）のタスク化と、GAS トリガー周期の UI 確認、および GAS の poison-pill 構造（エラーファイル1つで系統A全体が停止）への暫定対処（muteHttpExceptions＋エラーリネーム程度の小改修）である
