# secret / PII 露出 全数調査（RV-10 証拠・P1-006）

- 対象SHA: `588d416`（git管理下 *.py・test_*/legacy除外の静的調査）
- 調査範囲: (1) logger.*/print の出力、(2) LINE push/reply/notify 文面、
  (3) 例外 detail / HTTPレスポンス body。secret 実値・env 値は読んでいない。
- **secret（token/API key/接続URL）の出力到達: 該当ゼロ**（token は全て HTTP ヘッダ・
  env 参照のみ。DATABASE_URL をログ/例外に出す箇所も無し）。露出面は全て **PII**。

## 重大度の定義
- **S1（顧客LINE誤送信級）**: 顧客Bot トークンで業務/機微 PII を送る＝宛先/権限取り違えで
  顧客の最機微情報が漏れる最悪組合せ。
- **S2（永続ログ残留級）**: `logger.*`（Railway 集約ログに保持）や HTTP レスポンス body に
  戸籍/相談/財産等の濃い PII が乗る。保持期間が長く露出面が広い。
- **S3（一時 print 級）**: `print`（stdout・Railway ログに出るが相対的に短命）に PII。
- **S4（弱・識別子/部分/内部エラー）**: userId・氏名断片・kintone エラー本文等。

## 集計
- 確定 PII 露出箇所: **約40件**（logger/print系16 + LINE文面系15 + 例外/body系9、一部重複）
- secret 露出: **0件**
- 重大度内訳: **S1=4件 / S2=約12件 / S3=約14件 / S4=約10件**

---

## S1（顧客LINE誤送信級・最優先）

| file:行 | 出力先 | 内容 | 分類 |
|---|---|---|---|
| chat_responder.py:996 `_notify_attorney` | **顧客Botトークン**で弁護士へ | 顧客氏名＋相談本文[:200]（希死念慮・差押等の機微・urgent_kind付き） | 氏名＋会話本文 |
| main.py:389-393 人対応通知 | **顧客Botトークン**で弁護士へ | 顧客氏名＋会話本文**全文**（user_text 切詰めなし） | 氏名＋会話本文 |
| cloudsign_webhook.py:294 `notify_line` | **顧客Botトークン**で管理者へ | 締結書類タイトル（顧客名含み得る）＋documentID | 氏名(弱) |
| （推測）hub/notify.business_token_env フォールバック | `DISPATCHBOT_CHANNEL_ACCESS_TOKEN` 未設定時 **顧客Bot** へ | dispatch/approval/return_deadline/healthcheck の全業務通知（顧客名等） | 設定依存・要env確認 |

chat_responder の弁護士/人対応通知は `_LINE_TOKEN=LINE_CHANNEL_ACCESS_TOKEN`（顧客Bot）
**固定**でありフォールバックではなく常時この経路。4件目はフォールバック発火時のみ（BLOCKED: env実値未確認）。

## S2（永続ログ残留級・HTTPレスポンス／logger）

| file:行 | 出力先 | 内容 | 分類 |
|---|---|---|---|
| main.py:1007-1012 `/scan` 200 body | HTTP body | 相談カード/戸籍謄本/通帳の抽出全内容 | 戸籍/相談/財産（最重） |
| main.py:786-787 | HTTP 422 detail | 固定資産抽出dict（所在地/地番） | 住所/財産 |
| main.py:799-800 | HTTP 404 detail | 所在地/地番 | 住所 |
| main.py:710→796 / 735→806 | HTTP 502 detail | kintone応答本文丸ごと（`raise Exception(f"…{resp.text}")`・レコード内容含み得る） | 内部情報/住所/財産（RCF-M05違反） |
| bank_ingest.py:280 200 body | HTTP body | results（金融機関名・口座） | 財産 |
| registry_ingest.py:429 200 body | HTTP body | results（登記＝不動産/住所） | 住所/財産 |
| valuation_ingest.py:374 200 body | HTTP body | results（評価額） | 財産 |
| document_webhook.py:131 | logger.info（永続ログ） | 依頼者住所＋氏名＋被相続人名 | 住所/氏名（委任契約webhookで毎回） |
| main.py:822 ocr_fixed_asset 業務LINE | LINE業務 | 所在地(住所)/地番/評価額/recordID | 住所/財産 |
| hub/return_deadline.py:87 業務LINE | LINE業務 | 件名(氏名含み得る)/追跡番号 | 氏名(弱)/追跡番号 |

## S3（一時 print 級）

koseki_ingest.py:175（戸籍構造化読解結果**全体**＝氏名/生年月日/続柄/本籍・毎回同期出力）／
main.py:938（/scan kintone POST body 全fields）／main.py:993（/scan extracted）／
main.py:1102（決済完了: 氏名＋メール）／main.py:305（更新body全fields）／
main.py:357・490（LINE本文先頭20-30字）／main.py:454（KINTONE_UPDATE抽出）／
main.py:705・781・790（所在地/評価額）／main.py:729（固定資産body）／
registry_to_kintone.py:269（所有者氏名＋所在）／person_confirm.py:134（人物氏名）／
dispatch_bot/case_search.py:67（検索対象氏名）。※koseki_ingest.py:175 は内容の濃さでは
S2級だが print のため S3 に分類（redaction時は最優先で対処）。

## S4（弱・識別子/部分/内部エラー）

hub/dispatch.py:78/141/167/181/209（レコードNo＋顧客名＋str(e)・業務Botなら正チャネル）／
dispatch_bot/handler.py:110（指示原文[:100]）／dispatch_bot/router.py:80（userId[:10]＋本文[:50]）／
hub/notify.py:88（text[:100]・【推測】）／sortation_ingest.py file_name系（命名規則依存・【推測】）／
claude_gateway.py:72/119/135（Anthropicエラー本文・非PII）／sortation_ingest.py:223（str(e)[:200]）。

## 正常（本人宛・正チャネルのPIIは設計上正当）
- main.py:461・549（顧客への Claude 返信/承認済下書き）・chat_responder.py:1156/1174 は
  **本人宛**のため PII送信だが正当。dispatch_bot/router.py:52・sortation_ingest.py:352 は
  業務Bot固定で正チャネル。cloudsign notify_business_line（:268/288）は固定文字列で非該当。

---

## redaction 方針案（実装は次セッション・本タスクは調査のみ）

**方針: 出力の一点集約 + 分類タグ付き redaction。**

1. `hub/redact.py`（新規案）に `safe(value, kind)` を集約:
   - `kind` = `name|address|phone|email|birthdate|koseki|asset|freetext|record_id`
   - 既定は「マスク or 要約」（例: 氏名→`○○（伏字）`、record_id は素通し、
     freetext→出力しない or 長さのみ）。RCF-M05 の「分類のみ」と同じ流儀を PII へ拡張。
2. **ログ**: print を廃し logger 経由に統一。PII を含み得る値は `safe()` を通す。
   `document_webhook.py:131` のような logger.info の PII 直渡しを最優先で置換。
3. **HTTPレスポンス**: ingest/scan/ocr の 200 body から抽出全内容を外し、
   件数・record_id・冪等キーのみ返す（koseki_ingest.py:183 が良い既存例）。
   例外 detail は `str(e)`/`resp.text` 直渡しを禁止し分類文字列へ（AST call-policy で機械強制、
   P1-005b の mark_failed テストと同型）。
4. **LINEチャネル**: S1 の3件を業務Bot（`DISPATCHBOT_*`）へ移送し、
   `business_token_env` のフォールバックを fail-closed 化するか裁定（H04 と統合）。
   顧客Bot トークンで業務/機微 PII を送る経路をゼロにする。
5. 機械強制テスト: 「print 直書き禁止」「例外 detail への str(e)/resp.text 直渡し禁止」
   「顧客Bot token での業務通知禁止」を AST/静的検査で固定（notify_channel_policy の拡張）。

## BLOCKED_NEEDS_HUMAN
1. `DISPATCHBOT_CHANNEL_ACCESS_TOKEN` の本番投入有無（S1-4のフォールバック発火可否）
2. Railway ログの保持期間・アクセス権（S2 の実害範囲の確定）
3. redaction の要件（どこまで伏せるか）は法務判断＝大野の裁定
