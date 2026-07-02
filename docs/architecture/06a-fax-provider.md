# 06a. クラウドFAX プロバイダ選定（T6-1 スパイク成果物）

- 調査日: 2026-07-02（Web 公開情報ベース。契約前に最新の料金表・API仕様書で再確認すること）
- 対象要件: [06-module-03-fax.md](06-module-03-fax.md) §0 の要件 1〜4
- 結論: **InterFAX（国内代理店: 株式会社ドゥイット / interfax.jp）を推奨**。次点 NetFax（NetReal）

## 1. 想定送信量（料金比較の前提）

受任通知 FAX: 月 10〜50 件 × 2〜3 枚（送信票込み）= **月 20〜150 枚** を想定。
「平常時ゼロ件の月もあり得る」ため、月額固定費の低さを重視する。

## 2. 比較表

| | 要件1: REST API で PDF 送信 | 要件2: 送達結果の API 取得 | 要件3: 国内 0AB〜J 宛 | 要件4: 料金（税込目安） | 開発者向け情報 |
|---|---|---|---|---|---|
| **InterFAX**（Upland / 国内代理店ドゥイット） | ○ REST `POST /outbound/faxes`（PDF ネイティブ対応）。公式 SDK（Node/Java/.NET/PHP/Ruby） | ○ `GET /outbound/faxes/{id}` ポーリング + **完了コールバック（Webhook）あり**。自動リトライ内蔵（既定4回/3分間隔） | ○（090/080/070・一部フリーダイヤルは制限） | 初期 1,100円 / **月額 275円** / 国内 24.2円/枚 | 日本語ドキュメント・国内サポート・**テスト用FAX番号**（エラー系の擬似送信可）・国内実装事例あり |
| **NetFax FAX送信API**（NetReal） | ○ REST（JSON・PDF を base64 送信） | ○ 送信予約時の ID で結果確認 API（ポーリングのみ） | ○ | 初期 0円 / **月額 3,300円** / 6.6〜11円/枚 | 国内サービス。SDK なし（素の REST） |
| **faximoSilver**（エディックワークス） | ○ Web-API（REST/SOAP・PDF 可・送信専用） | ○ API 取得 + エラー時メール。再送機能あり | ○ | **非公開**（見積り/シミュレータ。参考: faximo 系の送信単価 14〜15.4円/枚） | 対応形式64種。履歴保持60日 |
| ネクスウェイ FNX e-帳票FAXサービス | △ Web-API は **SOAP**（ほか FTP/SMTP 等）。大量帳票向け | △ 送信結果の取得手段は要個別確認 | ○ | 非公開（枚数ベースの個別見積り） | エンタープライズ向け。小規模事務所には過剰 |
| eFax / eFax Corporate | △ API は法人契約のアドオン（eFax Router 月 11,000円〜等） | △ 契約形態依存 | ○ | 通常プラン月 2,585円（送受各150枚込み・超過11円/枚）だが**通常プランに送信 API なし** | API 利用の敷居が高い |
| 秒速FAX送信（Toones） | **✗ 送信 API なし**（API は送信ログ・PDF の参照専用） | △ ログ API（最新50件・60秒間隔制限） | ○ | 7〜10円/枚（成功時のみ課金・月額なし）と最安級 | 送信自体を API で起動できず要件1で脱落 |
| MOVFAX（日本テレネット） | ✗ 公開 API なし（UI 利用前提） | ✗ | ○ | 月 1,078円〜 | 要件1で脱落 |

### 想定送信量での月額試算（国内・税込概算）

| 月間枚数 | InterFAX | NetFax（6.6円プラン） | 備考 |
|---|---|---|---|
| 0枚（受任なしの月） | **275円** | 3,300円 | 固定費の差がそのまま出る |
| 50枚 | **1,485円** | 3,630円 | |
| 150枚 | **3,905円** | 4,290円 | |
| 300枚 | 7,535円 | **5,280円** | 損益分岐は**月約170枚**。これを超える見込みが立ったら再評価 |

## 3. 推奨: InterFAX（選定理由）

1. **要件適合が最も深い**: REST で PDF を1リクエスト送信（`POST /outbound/faxes`）、
   ジョブ ID による `GET /outbound/faxes/{id}` ポーリング — 06 §0 の
   `FaxProvider.send() / get_status()` インターフェースに**そのまま1対1で写像できる**。
   さらに完了コールバック（Webhook）も提供されており、将来ポーリングを置き換え可能
2. **テスト用 FAX 番号**が提供され、話中・不達などのエラー系を擬似的に発生させられる。
   06 §6 の「ポーリング全分岐テスト」を実環境相当で検証でき、`railway run` +
   `skipUnless` の実疎通テストが安全に書ける（実業者に誤送信するリスクがない）
3. **月額固定費が最小**（275円）。受任ゼロの月のコストがほぼゼロで、
   想定送信量（月〜150枚）では総額でも最安
4. 国内代理店（ドゥイット）の日本語ドキュメント・サポートと、国内企業の実装事例
   （Rails での送信＋5分間隔ポーリング運用）が公開されており、実装リスクが低い
5. 送信リトライ（既定4回/3分）がプロバイダ側に内蔵されている

### 設計への反映（T6-2 で実装時に守ること）

- **二重リトライの回避**: 06 §1 の「FAILED → 最大3回自動再送」は、プロバイダ内蔵リトライ
  （4回/3分）が尽きた後の最終 FAILED に対するアプリ層再送である。実装時は
  InterFAX 側リトライ設定を確認し、アプリ層再送は間隔を空けた再投入（新ジョブ）として
  `attempts` に記録する
- 携帯番号帯（090/080/070）宛は送信不可・特別料金のため、06 §3 の番号バリデーションで
  **固定電話帯のみ許可**に絞る（業者・官公庁宛て前提なので実害なし）
- 認証は Basic 認証（ユーザーID/パスワード）。Railway 環境変数
  `FAX_PROVIDER=interfax` / `FAX_API_USER` / `FAX_API_PASSWORD` とする
- 公式 Python SDK は主要言語リストに含まれないため、httpx で REST を直接叩く
  （既存コードの流儀とも一致。SDK 依存を増やさない）

### 次点: NetFax（切替条件）

送信量が**月170枚超**で定着した場合、または InterFAX の契約・サポートに問題が出た場合の
乗り換え先。REST + ポーリングの構成が同型のため、`FaxProvider` 実装の追加のみで切替可能
（これがプロバイダ非依存インターフェースを設計した理由）。

## 4. 契約前の確認事項（人の作業）

- [ ] interfax.jp のトライアルを申し込み、実アカウントで `POST /outbound/faxes` 疎通
- [ ] 最新料金表の確認（本書の金額は 2026-07 時点の公開情報）
- [ ] リトライ既定値（4回/3分）と、リトライ中のステータス遷移の実挙動確認
- [ ] 送信元 FAX 番号表示（発信者番号）の扱い — 受任通知に事務所 FAX 番号を表示できるか
- [ ] 完了コールバック（Webhook）の利用可否と設定方法（将来のポーリング置換用）

## 5. 情報源

- InterFAX 国内: [サービストップ](https://www.interfax.jp/)・[送信サービス仕様](https://www.interfax.jp/send/spec.html)・[料金](https://interfax.jp/price/index.html)
- InterFAX 開発者向け: [Upland InterFAX Developers](https://uplandsoftware.com/interfax/developers/)・[完了コールバック（Webhook）ドキュメント](https://docs.uplandsoftware.com/interfax/documentation/dev-guide/receive-outgoing-fax-confirmations-via-callback-to-your-web-app)・[公式 SDK 群（GitHub）](https://github.com/interfax)
- 国内実装事例: [InterFAXを使用したFAX送信機能の実装（Zenn・Linc'well）](https://zenn.dev/lincwell_inc/articles/interfax-blog)
- NetFax: [FAX API](https://www.netfax.jp/api)
- faximo / faximoSilver: [価格](https://www.edicworks.com/service/faximo/price.html)・[faximoSilver 仕様](https://www.edicworks.com/service/faximosilver/spec.html)
- ネクスウェイ: [FNX e-帳票FAXサービス](https://b2bform.nexway.co.jp/service/print)・[API連携コラム](https://b2bform.nexway.co.jp/column/87)
- eFax: [料金](https://www.efax.com/pricing)・[eFax Corporate 料金](https://www.efaxcorporate.jp/price)
- 秒速FAX送信: [API機能](https://fax.toones.jp/send/system/api.html)・[料金](https://fax.toones.jp/send/plan.html)
- MOVFAX: [料金・仕様](https://movfax.jp/price/)
