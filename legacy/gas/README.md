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

## 内容

| ファイル | 内容 |
|---|---|
| `コード.js` | `onFileAdded()` 1関数のみ。3つの Drive フォルダ（相談カード/戸籍謄本/通帳）を全走査し、`[済]` プレフィックスのないファイルを base64 化して Railway `POST /scan` へ送信、成功後にファイル名へ `[済]` を付与 |
| `appsscript.json` | マニフェスト（Asia/Tokyo・V8。トリガー定義は含まれない） |

## 注意（現状の既知の挙動）

- トリガーは GAS の UI で設定された installable トリガー（コードからは周期を確認できない。
  実装がポーリング型のため時間主導型と推定）
- try/catch・リトライ・警報は一切ない。Railway が 4xx/5xx を返すと `UrlFetchApp.fetch` が
  例外を投げて**その実行全体が停止**する（詳細は docs/current-state-report.md §2）
