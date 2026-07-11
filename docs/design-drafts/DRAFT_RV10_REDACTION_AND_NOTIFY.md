# DRAFT: RV-10 PII redaction + notify fail-closed 統合設計

> **status: DRAFT（司令塔裁定待ち）・実装開始根拠にしない。**
> 対象SHA 7b03069。docs/evidence/SECRET_PII_EXPOSURE_REPORT.md（S1〜S4・約40件）を解消する叩き台。

## 0. 全体方針
出力（ログ・HTTPレスポンス・LINE文面・例外）を**一点集約**し、PIIは分類タグ付きで
redactする。secret露出は現状0件（evidence確認済み）なので対象はPII。
H04裁定「イ」（business token未設定時fail-closed＋独立dead-man監視）を同時に実装する。

## 1. hub/redact.py 一点集約案

```python
def safe(value, kind) -> str: ...   # kind別のマスク/要約を返す
```
- `kind` 一覧: `name`（氏名）/ `address`（住所・本籍・所在）/ `phone` / `email` /
  `birthdate` / `koseki`（戸籍構造化データ）/ `asset`（財産・評価額・口座）/
  `freetext`（相談本文・会話・指示原文）/ `record_id`（素通し）/ `count`（件数・素通し）
- **伏字仕様は【大野裁定待ち】**（法務判断）。選択肢:
  - (あ) 完全マスク: `name→"＊＊"`, `email→"＊＊＊"`, `freetext→"（本文N文字・非表示）"`
  - (い) 部分マスク: `name→"田＊＊"`, `email→"t＊＊@＊＊"`, `address→"川口市＊＊"`
  - (う) ハッシュ化: `name→"name#a1b2"`（名寄せ用に一貫・可読性なし）
  - 【論点1】ログ用と（正当な）顧客宛LINE用で伏字レベルを変える設計にするか
    （顧客本人宛は伏せない・ログは(あ)・業務LINEは(い) 等）。
- record_id/冪等キー/件数は素通し（運用に必要・非PII）。

## 2. S1（顧客Bot誤送信 4経路）→ 業務チャネル移送＋本文要約

| 経路 | 現状 | 設計 |
|---|---|---|
| chat_responder.py:996 `_notify_attorney` | 顧客Bot固定・氏名＋相談本文[:200] | 業務Bot（DISPATCHBOT）へ移送＋本文は`safe(freetext)`要約。urgent_kindは分類として残す |
| main.py:389-393 人対応通知 | 顧客Bot固定・氏名＋会話全文 | 業務B20へ移送＋`safe(name)`＋`safe(freetext)`。全文送信を廃止 |
| cloudsign_webhook.py:294 notify_line | 顧客Bot・書類タイトル | notify_business_line（業務Bot・既存）へ統一＋タイトルは`safe(name)`扱い |
| business_token_env フォールバック | 未設定時顧客Botへ | §4 の fail-closed 化で解消 |

移送先の唯一の業務通知関数に集約する（後述 §4）。

## 3. S2/S3/S4 の解消

- **S2（応答body・永続ログ）**:
  - ingest/scan/ocr の 200 body から抽出全内容を除去 → `{件数, record_id, 冪等キー}` のみ
    （koseki_ingest.py:183 が既存の良い型）。GAS/watcherは登録結果の詳細を必要としない。
  - 例外 detail の `str(e)`/`resp.text` 直渡し（main.py:786/799/710→796/735→806）を
    分類文字列へ（`kintone_search_failed` 等）。RCF-M05 と同流儀。
  - document_webhook.py:131 の logger.info(PII直渡し) を `safe()` 経由へ（最優先）。
- **S3（print 全廃）**: 全 print を logger 化し、PII含み得る値は `safe()` 通す。
  koseki_ingest.py:175（戸籍読解結果全体）を最優先。決済print（main.py:1102）も。
- **S4（str(e)分類化）**: hub/dispatch・handler・claude_gateway 等の `str(e)[:N]` を
  例外クラス名＋分類へ。顧客名を含む `_summary` は `safe(name)` 経由。

## 4. H04「イ」: notify fail-closed ＋ dead-man 監視

### 4.1 fail-closed 化
`hub/notify.business_token_env()` の「未設定→顧客Botフォールバック」を廃止し、
**業務通知は DISPATCHBOT_CHANNEL_ACCESS_TOKEN が無ければ送信しない＋警告ログ**にする
（顧客Botに業務PIIを乗せない）。※現行は「警報欠落防止優先」で fallback していたため、
欠落を埋める代替が §4.2。

### 4.2 独立 dead-man 監視（通知経路自体の死活）
- 問題: fail-closed にすると「業務通知が全く出ない」状態に気付けない。
- 案: **通知経路の生存を別経路で監視**する。
  - (i) 日次死活監視（監視項目E の隣）に「DISPATCHBOT token 未設定」チェックを追加し、
    **未設定なら顧客Bot（＝生きている既定経路）で1回だけ管理者へ警告**（PIIなし・
    「業務通知チャネル未設定」の固定文言のみ＝顧客Botに乗るがPIIではない）。
  - (ii) さらに「最後に業務通知が成功した時刻」を app-state に記録し、N時間無音なら
    別経路（メール/顧客Bot固定文言）でheartbeat警告（dead-man switch）。
- 【論点2】dead-man の実装深度（(i)のみ / (i)+(ii)）。(ii)は送信成功時刻の永続化が要る。

## 5. AST 機械強制テスト（notify_channel_policy の拡張）
既存 test_notify_channel_policy.py（push_line_message の token_env 明示強制）を土台に:
- **print 直書き禁止**（logger 経由に統一。既存の一部 print は移行期間中は許可リスト）
- **例外 detail への str(e)/resp.text 直渡し禁止**（HTTPException(detail=...) の引数検査・
  P1-005b の mark_failed call-policy と同型）
- **顧客Bot token での業務通知禁止**（send_line_push/顧客token直呼びで業務文面を送る経路の検出）
- **PII変数の logger/print 直渡し検出**は完全自動化は困難（変数名ヒューリスティック止まり）
  →【論点3】どこまで機械強制するか（強制 vs レビュー規律）。

## 6. 段階PR案（移行順序）
1. **PR-1: S1（4経路）＋§4 fail-closed**（最優先・顧客への機微漏れを止める）
2. **PR-2: hub/redact.py 導入＋S2 応答body最小化＋例外分類化**
3. **PR-3: S3 print全廃＋document_webhook logger redact**
4. **PR-4: S4 str(e)分類化＋AST機械強制テスト＋dead-man監視(ii)**
各PRで全suite回帰・既存テスト無変更を維持。伏字仕様（§1）は PR-2 の前に裁定が要る。

## 7. 論点・BLOCKED
- 【論点1】伏字レベル（あ/い/う）＋ログ用/顧客宛用の出し分け（法務判断）
- 【論点2】dead-man 監視の深度（(i)/(i)+(ii)）
- 【論点3】AST機械強制の範囲（PII変数検出をどこまで）
- 【論点4】S1移送後、弁護士が受け取る業務Botの宛先（ATTORNEY_LINE_USER_ID で足りるか）
- BLOCKED_NEEDS_HUMAN: DISPATCHBOT_CHANNEL_ACCESS_TOKEN 本番投入有無（fallback発火の現況）・
  Railwayログ保持期間（S2実害範囲）・伏字要件の法務確定
