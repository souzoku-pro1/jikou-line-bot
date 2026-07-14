# Runbook: K1〜K4 caller 移行 確認手順（大野が実行・所要 15〜20 分）

- 目的: RV-04（HMAC 署名移行）/ RV-05（LINE durable 化）の**設計前提を実機で確定**するための確認。
- 実行者: 大野。各項目は**画面を見て YES/NO/値を記録欄に書き込むだけ**。判断・実装は不要。
- 出典: `docs/design-drafts/DRAFT_RV04_HMAC_MIGRATION.md`（§3 kintone webhook 代替 K1/K2/K3）・`DRAFT_RV05_DURABLE_INBOUND.md`（§9.2 K4）。
- 使い方: 上から順に実行し、各「記録欄」を埋める → このファイルを保存して司令塔へ共有（または結果だけ返信）。

---

## K1: kintone webhook payload に event id があるか（所要 約4分）

**背景**: kintone webhook に一意な event id があれば冪等キーに使える。無ければ payload hash で代替する（設計分岐）。

**手順**:
1. kintone にログイン → 対象アプリ（例: **App 30 / App 29** など webhook を設定しているアプリ）を開く。
2. 右上の歯車 **[アプリの設定]** → 左メニュー **[設定]** タブ → **[Webhook]** を開く。
3. 登録済み Webhook の行の **[通知するイベント]** と、可能なら **[Webhook の詳細/ヘルプ]** を確認。
   - kintone の webhook body は一般に `{"type","app","record","recordId","url", ...}` 形式。**一意な "event id"（配信ごとに変わる ID）フィールドが body にあるか**を見る。
4. 実 body を確認する最短手段（どちらか）:
   - (a) **[Webhook のテスト送信]** ボタンがあれば押し、受信側ログ（Railway）で body を確認。
   - (b) 実際に1回 webhook を発火させ（対象アプリでレコードを1件更新）、Railway ログの受信 body を見る。
   - **スクショ推奨**: Webhook 設定画面、および受信 body（`recordId` や `type` は見えるが「配信ごとの一意 id」があるか）。

**判定の目安**: `recordId`（レコード ID）は「レコードの id」であって「配信の event id」ではない。**同一レコードを2回更新した時に body 中で値が変わる一意フィールドがあるか**が本質。

**記録欄（K1）**:
- 配信ごとに一意な event id フィールドはあったか: **YES / NO**（→ ）
- あった場合、そのフィールド名: **（　　　　　　　　）**
- 確認方法: テスト送信 / 実発火 / その他（　　　）
- スクショ添付: あり / なし

---

## K2: source restriction（送信元 IP 制限）が可能か（所要 約4分）

**背景**: kintone webhook に IP 制限がかけられれば、署名不可の webhook でも送信元を絞れる（K1 案の防御束の1つ）。

**手順**:
1. 対象アプリ → 歯車 **[アプリの設定]** → **[設定]** タブ → **[Webhook]**。
2. Webhook 編集画面に **[送信元 IP アドレス制限]** / **[IP アドレス]** 等の設定欄があるか確認。
3. 無ければ、システム管理側も確認: 右上ユーザー名/歯車 → **[cybozu.com 共通管理]** → **[セキュリティ]** → **[アクセス制限（IP アドレス）]** に、**webhook 送信元（kintone→当サーバ）方向**を制限できる設定があるか。
   - 注: cybozu の IP 制限は主に「クライアント→kintone のアクセス」向け。**webhook 送信元（kintone のアウトバウンド IP）を当サーバ側で許可リスト化**できるかが実務上の論点。当サーバ（Railway）側で kintone の送信元 IP を allowlist する運用も選択肢（その場合 kintone の webhook 送信元 IP レンジが公開/固定かも確認）。

**記録欄（K2）**:
- kintone 側で webhook に IP 制限の設定欄があったか: **YES / NO**（→ ）
- cybozu 共通管理でアクセス制限（IP）は使えるか: **YES / NO**
- kintone の webhook 送信元 IP は固定/公開されているか（分かれば）: **YES / NO / 不明**
- スクショ添付: あり / なし

---

## K3: GAS の UrlFetchApp でカスタムヘッダを付けられるか（所要 約4分・コピペ1回）

**背景**: HMAC 署名移行（RV-04）は GAS が `X-Sig-*` カスタムヘッダを付けられることが前提。**下のスクリプトをコピペして1回実行**すれば、カスタムヘッダが実際に届くか（＝署名を付けられるか）が分かる。

**手順**:
1. Google スプレッドシート or Apps Script（script.google.com）で新規プロジェクトを作成。
2. 下のコードを**丸ごと貼り付け** → 関数 `checkCustomHeader` を実行（初回は権限承認）。
3. 実行ログ（表示 → ログ / 実行数）に出る結果を記録欄へ。

```javascript
function checkCustomHeader() {
  // httpbin.org は受け取ったヘッダをそのまま JSON で返す公開エコーサーバ。
  // UrlFetchApp が X-Sig-* カスタムヘッダを送出できるかを実測する。
  var url = 'https://httpbin.org/anything';
  var res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    headers: {
      'X-Sig-Version': 'v1',
      'X-Sig-Test': 'hello-from-gas'
    },
    payload: JSON.stringify({ probe: 'k3' }),
    muteHttpExceptions: true
  });
  var body = JSON.parse(res.getContentText());
  var received = body.headers || {};
  // httpbin はヘッダ名を Title-Case で返す（X-Sig-Version 等）
  var got = {
    'X-Sig-Version': received['X-Sig-Version'] || received['X-sig-version'] || null,
    'X-Sig-Test': received['X-Sig-Test'] || received['X-sig-test'] || null
  };
  Logger.log('HTTP status: ' + res.getResponseCode());
  Logger.log('受信したカスタムヘッダ: ' + JSON.stringify(got));
  Logger.log('=> X-Sig-* が両方 hello/v1 で返れば「カスタムヘッダ送出 OK」');
  return got;
}
```

**判定の目安**: ログの「受信したカスタムヘッダ」に `"X-Sig-Version":"v1"` と `"X-Sig-Test":"hello-from-gas"` が**両方**出れば OK（＝GAS で HMAC 署名ヘッダを付けられる）。`null` なら NG。

**記録欄（K3）**:
- HTTP status: **（　　　）**（200 期待）
- X-Sig-Version が "v1" で返ったか: **YES / NO**
- X-Sig-Test が "hello-from-gas" で返ったか: **YES / NO**
- 総合: カスタムヘッダ送出 **OK / NG**
- ログのスクショ添付: あり / なし

---

## K4: LINE Developers の webhook 再配送設定の現在値（顧客Bot・業務Bot 両チャネル）（所要 約5分）

**背景**: RV-05 の durable 化で、DB 停止時にサーバが 5xx を返す。**LINE 側で webhook 再配送（redelivery）が ON なら 5xx で自動回復（lost 0）**、OFF なら当該 event は喪失（観測性で補う）。顧客Bot と業務Bot（別チャネルなら別々）両方の現在値を確認する。

**手順**:
1. https://developers.line.biz/console/ にログイン。
2. **プロバイダー** → **チャネル**（Messaging API）を開く。**顧客Bot 用チャネル**を選択。
3. **[Messaging API]** タブを開く。
4. **[Webhook settings]**（Webhook 設定）セクションで以下の現在値を確認:
   - **Use webhook**（Webhook の利用）: ON / OFF
   - **Webhook redelivery**（Webhook の再送 / 再配信）: ON / OFF ← **これが本命**
   - （参考）**Error statistics** / **Verify** ボタンの有無。
5. 同じ手順を**業務Bot 用チャネル**でも実施（別チャネルの場合）。同一チャネルで兼ねている場合はその旨を記録。
6. **スクショ推奨**: 各チャネルの Webhook settings セクション（redelivery の ON/OFF が見える状態）。

**記録欄（K4）**:

| チャネル | Use webhook | **Webhook redelivery** | チャネル ID/名（任意） |
|---|---|---|---|
| 顧客Bot | ON / OFF | **ON / OFF** | （　　　） |
| 業務Bot | ON / OFF | **ON / OFF** | （　　　） |

- 顧客Bot と業務Bot は同一チャネルか別チャネルか: **同一 / 別々**
- スクショ添付: あり / なし

---

## まとめ記録（提出用・ここだけ返信でも可）

| 項目 | 結果 |
|---|---|
| K1 event id | YES（フィールド名: 　） / NO |
| K2 source restriction | YES / NO / 不明 |
| K3 GAS カスタムヘッダ | OK / NG |
| K4 顧客Bot redelivery | ON / OFF |
| K4 業務Bot redelivery | ON / OFF |

- 実施日: 2026-07-　　
- 備考（気づいた点）:

**この結果の使い道（参考・大野の作業は不要）**:
- K1=NO → 冪等キーは payload hash 方式で確定。K1=YES → event id を冪等キーに採用。
- K2/K3 → kintone webhook の代替設計（K1/K2/K3 案）と RV-04 署名移行の可否判断材料。
- K4=ON → RV-05 顧客Bot 自動 replay 有効化の前提クリア。K4=OFF → 5xx 時喪失を観測性で補う運用に確定。
