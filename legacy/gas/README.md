# legacy/gas — Drive監視→/scan 連携 GAS のローカルコピー

- **正本は GAS 側**（このディレクトリは読み取り用のコピー。ここを編集しても GAS には反映されない）
- GAS プロジェクト: **相続書類自動化**
  - scriptId: `1N-GZ0lciPrU-tllnAaJcjcHh91jkQNfWxfA2xk943i7RcSJYc4ZOMdKF`
  - 編集 URL: https://script.google.com/d/1N-GZ0lciPrU-tllnAaJcjcHh91jkQNfWxfA2xk943i7RcSJYc4ZOMdKF/edit
  - 所有アカウント: t-ohno@sozoku-law.com
- **同期方法**: このディレクトリで `clasp pull`（要 `clasp login` 済み）
  - この PC では SSL 証明書の都合で環境変数 `NODE_OPTIONS=--use-system-ca` を
    設定してから実行すること（未設定だと証明書エラーになる）
  - `clasp push` は GAS 側を上書きするため、意図的なデプロイ時以外は実行しないこと
- 取得日: 2026-07-03（clasp 3.3.0 / clasp clone で取得）
- **2026-07-06 同期**: 戸籍読解ブロック追加分は clasp の Google 再認証失効（invalid_rapt）のため **GAS エディタからの手動転記**で反映（大野が実物と構造一致を目視確認済み）。clasp 再ログイン→pull での機械同期は別タスク

## 内容

| ファイル | 内容 |
|---|---|
| `コード.js` | `onFileAdded()` 1関数のみ。**4つの Drive フォルダを監視**（2026-07-06 戸籍読解を追加）: ①相談カード/戸籍謄本/通帳 → base64 JSON で `POST /scan`（従来どおり・無変更） ②戸籍読解 → multipart で `POST /koseki/ingest?token=`（独立ブロック・ファイル単位 try/catch・muteHttpExceptions・`[済]` リネームは HTTP 200 時のみ）。KOSEKI_TOKEN の実物値は GAS エディタ側のみ（この写しではプレースホルダ） |
| `appsscript.json` | マニフェスト（Asia/Tokyo・V8。トリガー定義は含まれない） |

## 注意（現状の既知の挙動）

- トリガーは GAS の UI で設定された installable トリガー（コードからは周期を確認できない。
  実装がポーリング型のため時間主導型と推定）
- try/catch・リトライ・警報は一切ない。Railway が 4xx/5xx を返すと `UrlFetchApp.fetch` が
  例外を投げて**その実行全体が停止**する（詳細は docs/current-state-report.md §2）
