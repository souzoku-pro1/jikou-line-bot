# kintone トークン権限マトリクス（G1証拠・P1-006）

- 対象SHA: `588d416`（読み取り専用の静的調査。kintone実環境へはアクセスしていない）
- 目的: RV-08（App 34 削除権限）ほかの裁定材料。「**コードが要求する権限**」は
  ソースから確定できるが、「**実権限**（トークンに実際に付与された権限）」は
  kintone 管理画面でしか見えない＝BLOCKED_NEEDS_HUMAN。
- 最小権限原則の観点: コードが read しかしない App に write/delete 権限が
  付いていれば過剰付与。逆にコードが write/delete するのに実権限が無ければ実行時 403。

## 凡例
- 操作: R=search_records/get_record（読）, W=create_record/update_record/upload_file（書）,
  D=delete_record（削除）
- 「コード要求」= ソース上でそのトークンに対して発行される操作の和集合
- 「実権限」= kintone 側の設定（**要画面確認**）

## トークン env × App × 操作

| トークン env | app_id env | 対象App | コードが要求する操作 | 主な使用箇所（file:行） |
|---|---|---|---|---|
| `KINTONE_API_TOKEN` | `KINTONE_APP_ID` | App 21（案件） | R / **W** | main.py:280,296（hearing更新）・main.py:932（/scan 相談カードはSOUZOKU側）・Stripe起票 main.py:1106・reconciliation GET main.py:1086・cloudsign_webhook.py:160/179（受任更新）・chat_responder.py:760・dispatch_bot/case_search.py:14 |
| `TOKEN_CHATLOG` | `APP_CHATLOG` | App 28（チャットログ） | R / **W** | chat_responder.py:793/822（会話ログ書込・読出） |
| `TOKEN_APPROVAL` | `APP_APPROVAL` | App 29（承認キュー） | R / **W** | chat_responder.py:868/889/914・main.py:529（承認webhook） |
| `SOUZOKU_KINTONE_API_TOKEN` | `SOUZOKU_KINTONE_APP_ID` | 相談カード（相続・App 26系） | R / **W** | /scan main.py:860・document_webhook.py:31・customer_directory.py:79・kinship_renderer.py:24（関係図添付） |
| `KOSEKI_KINTONE_API_TOKEN` | `KOSEKI_KINTONE_APP_ID` | 戸籍謄本（相続・旧スキャン先） | **W** | /scan main.py:866（戸籍謄本フォルダ登録） |
| `KINTONE_SCAN_API_TOKEN_TSUCHOU` | `KINTONE_SCAN_APP_ID_TSUCHOU` | 通帳（相続） | **W** | /scan main.py:879（通帳フォルダ登録・env未設定なら不稼働） |
| `KINTONE_FUDOSAN_API_TOKEN` | `KINTONE_FUDOSAN_APP_ID` | App 25（不動産） | R / **W** | /ocr/fixed-asset main.py:696/721・registry_ingest.py:49・valuation_ingest.py:41・zaisan_sync.py:29・registry_to_kintone.py:209 |
| `TOKEN_SHIPPING` | `APP_SHIPPING` | App 30（発送管理／要確認キュー封筒） | R / **W** | 全ingest系の要確認起票・hub/approval.py:71/88・hub/dispatch.py・review_resolve.py・person_merge*・koseki_ingest.py:73 ほか多数 |
| `TOKEN_CITY_MASTER` | `APP_CITY_MASTER` | App 31（市区町村マスタ） | R | shokumu_seikyu.py:480・import_city_master.py（投入時のみW） |
| `TOKEN_ENCLOSURE` | `APP_ENCLOSURE` | App 32（同封物ブロックマスタ） | R | soufu_annai.py:61/319 |
| `TOKEN_KOSEKI_BOOK` | `APP_KOSEKI_BOOK` | App 33（戸籍読解） | R / **W** | koseki_ingest.py:164・koseki_reader.py:308（読解結果更新）・review_resolve.py:244・koseki_person_sync.py:205 |
| `TOKEN_KOSEKI_PERSON` | `APP_KOSEKI_PERSON` | App 34（人物） | R / **W**（~~★D~~ RV-08 で D 要求消滅） | koseki_person_sync.py（起票）・person_confirm.py（確認更新）・person_merge.py（自動候補マーク）・person_merge_exec.py（勝者更新・**敗者無効化 update**）・person_restore_cli.py（復元 create/親エッジ再結線）——**delete_record 呼出しは RV-08 実装で完全除去（AST pin・2026-08-12）** |
| `TOKEN_ZAISAN` | `APP_ZAISAN` | App 35（財産） | R / **W** | registry_ingest.py:300/320・valuation_ingest.py・bank_ingest.py:186/208・zaisan_sync.py |
| `TOKEN_SOUZOKUNIN` | `APP_SOUZOKUNIN` | App 36（相続人） | （現状参照コードなし） | config.py監視定義のみ。R4-3未実装のため書込みコード不在 |
| `TOKEN_WARITSUKE` | `APP_WARITSUKE` | App 37（割付） | （現状参照コードなし） | config.py監視定義のみ |
| `TOKEN_SORTATION_LOG` | `APP_SORTATION_LOG` | App 38（仕分けログ） | R / **W** | sortation_ingest.py:309（起票）・dispatch_bot/sortation_assign.py:217（状態更新） |

補足:
- App 21 の書込トークン（`KINTONE_API_TOKEN`）は **cloudsign / stripe / hearing /
  指示Bot** と広く共有され、read も write も同一トークン。分離されていない。
- webhook 認証用の `KINTONE_WEBHOOK_TOKEN`（main.py:495）は kintone データ操作トークン
  ではなく、/webhook/kintone/approval への合言葉。本マトリクス対象外だが混同注意。
- OCR 系（main.py:696-721）は `KINTONE_FUDOSAN_API_TOKEN` を `_OCR` サフィックスの
  別名変数で読むが**実体は同一 env**（main.py:142）＝同一トークン。

## ★ RV-08 関連の要確認事項（最重要 BLOCKED_NEEDS_HUMAN）

- **App 34（人物）のトークン `TOKEN_KOSEKI_PERSON` に「レコード削除」権限が付いているか**。
  コードは person_merge_exec.py:286 で `delete_record`（敗者の物理削除）を要求する。
  - 実権限に削除が**付いている** → `PERSON_MERGE_ENABLED=1` かつ人の二段確認で
    物理削除が実行され得る（RV-08 の soft merge 化までは削除権限を**外す**ことが封じ込め）
  - 実権限に削除が**無い** → merge実行時に kintone 403 で失敗（安全側だが機能不全）
  → kintone 管理画面（アプリ設定 → APIトークン → 該当トークンのアクセス権）で
    「レコード削除」チェックの有無を確認する必要がある。
- **【2026-08-12 追記・RV-08 実装（rv08-impl）】** soft merge 化により
  `delete_record` 呼出しはコードから完全除去（AST pin）＝**コード要求は R/W のみ**。
  凍結票 §3.3 の順序どおり **R3（本実装の merge）→ R4（削除権限の除去）**:
  merge 後は削除権限を外しても機能不全にならないため、[人] が kintone 画面で
  「レコード削除」チェックを**外す**こと（上記の実権限確認と同時に実施可）。
- **【2026-08-13 追記・R4 完了】** PR #205 merge（R3・main=a420019）＋本番
  migration `b9c4e7f2a6d1` 適用後、[人]（弁護士）が kintone 画面で
  `TOKEN_KOSEKI_PERSON` の**「レコード削除」チェックを除去済み**。
  → 本 ★ 項目の BLOCKED_NEEDS_HUMAN は**消込**（実権限=R/W・コード要求と一致。
  物理削除経路はコード（AST pin）と権限の両面で閉鎖）。

## 実権限確認が必要な項目（BLOCKED_NEEDS_HUMAN・kintone画面）

1. 全トークンの付与権限（レコード閲覧/追加/編集/**削除**/アプリ管理）を画面で列挙
2. 過剰付与の検出: コード要求が R のみ（City/Enclosure/Souzokunin/Waritsuke）の
   トークンに W/D が付いていないか
3. ~~App 34 削除権限（上記★）~~ → **2026-08-13 消込（R4 完了・上記追記参照）**
4. App 21 共有トークンの権限範囲（決済・受任・hearing で同一トークン＝影響範囲大）
