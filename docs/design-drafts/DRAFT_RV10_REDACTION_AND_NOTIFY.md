# DRAFT: RV-10 PII redaction + notify fail-closed 統合設計（v2・Codexレビュー反映）

> **status: DRAFT（司令塔裁定待ち）・実装開始根拠にしない。**
> 対象SHA 7b03069。docs/evidence/SECRET_PII_EXPOSURE_REPORT.md（S1〜S4・約40件）を解消する叩き台。
> R-P1-007-drafts-v2 の所見（全ACCEPT・REJECT0）を反映。**OPEN は仮決めせず owner を明記。**

---

## ★共有節: 実装順序骨子（M11・3 DRAFT 共通）

（RV04/RV10/App36 で同一。詳細は DRAFT_RV04_HMAC_MIGRATION §共有節）
1. redaction contract 確定（本書 §1・OPEN の伏字水準を大野裁定で埋める）
2. RV10 S1 切替＋notify fail-closed
3. RV04 multipart body-hash PoC（別票）
4. RV04 GAS群 header HMAC＋dual-accept Phase A（downgrade 禁止）
5. RV10 S2/S3/S4 段階解消＋AST 機械強制
6. App36 DerivationRun（immutable）＋App36 projection 起票（R4-3b）
7. App37 割付＋TemplateVersion registry
8. dead-man 監視＋RV04 dual-accept 廃止＋kintone webhook 代替（K選択後）

---

## 0. 全体方針
出力（ログ・HTTPレスポンス・LINE文面・例外）を**一点集約**する。ただし単一の `safe()` では
不足で、**sink（出力先）× audience（受け手）別の出力 policy** に再設計する（HIGH）。
secret 露出は現状0件（evidence確認済み）なので対象は PII。H04裁定「イ」も同時実装。

## 1. redaction contract（sink/audience policy・単一 safe() から再設計）

### 1.1 policy テーブル
出力は必ず `emit(value, kind, sink, audience)` を通す。sink×audience で許可水準が変わる:

| sink | audience | 既定水準 |
|---|---|---|
| log（Railway集約） | 運用者 | **氏名等は完全抑止**（既定・§1.4）。record_id/count のみ素通し |
| http_response | 呼出し元(GAS/watcher) | 件数・record_id・冪等キーのみ。抽出内容禁止 |
| line_business | 弁護士/管理者 | `kind` 別に要約（氏名は必要最小・freetext は要約） |
| line_customer | 顧客本人 | 本人の情報は**そのまま可**（正当・本人宛のみ） |
| exception_detail | 呼出し元 | 固定分類文字列のみ（値禁止） |

### 1.2 kind 一覧（§13.1 禁止カテゴリを追加）
`name/address/phone/email/birthdate/koseki/asset/freetext/record_id/count` に加え、
**§13.1 の禁止カテゴリを分類として明示**（HIGH）:
`contract`（契約書本文）/`fax`（FAX本文）/`qa`（Q&A本文）/`vendor_raw`（kintone/LINE/Claude/
Stripe の生レスポンス）。これらは **log/exception には常に完全抑止**（line_customer 本人宛の
正当ケースを除く）。

### 1.3 unknown kind・構造化値の扱い（HIGH: fail-closed）
- **unknown kind（未登録の分類）→ 完全抑止**（`（分類不明・非表示）`）。素通しにしない。
- **構造化値（dict/list/dataclass 等）→ 完全抑止**（`（構造化値・非表示・N要素）`）。
  「json.dumps で丸ごと」を構造的に禁止（S2 の /scan body・S3 の POST body 全 fields が該当）。

### 1.4 通常 log の既定＝完全抑止（OPEN: 出し分け水準）
- 通常ログの氏名・住所・戸籍・財産・freetext は **既定で完全抑止**（マスクではなく非表示）。
- 【OPEN・owner=大野】ログ用と業務LINE用で伏字水準を変えるか（部分マスク許容の範囲）。
  判断材料: 運用でのデバッグ可否 vs 漏洩時実害・法務。**仮決めしない**（既定=完全抑止のまま
  この裁定が入るまで緩めない）。

### 1.5 redaction 失敗時（HIGH: fail-closed）
- `emit` 内で例外（unknown kind 解決不能・変換失敗）→ **固定文言に縮退**（`（redaction失敗・非表示）`）
  ＋**独立警報**（業務通知経路とは別の dead-man 系・§4.2）＋業務処理とは**別 state** で記録
  （業務処理を止めない・しかし黙らせない）。

## 2. S1（顧客Bot誤送信 4経路）→ 業務チャネル移送＋要約

| 経路 | 現状 | 設計 |
|---|---|---|
| chat_responder.py:996 `_notify_attorney` | 顧客Bot固定・氏名＋相談本文[:200] | line_business へ移送＋freetext は要約。urgent_kind は分類として残す |
| main.py:389-393 人対応通知 | 顧客Bot固定・氏名＋会話全文 | line_business へ移送＋name 最小＋freetext 要約。全文送信廃止 |
| cloudsign_webhook.py:294 notify_line | 顧客Bot・書類タイトル | notify_business_line（業務Bot）へ統一＋タイトルは name 扱い |
| business_token_env フォールバック | 未設定時顧客Botへ | §4 fail-closed 化で解消 |

## 3. S2/S3/S4 の解消（policy 適用）
- **S2**: ingest/scan/ocr の 200 body → `{件数, record_id, 冪等キー}`（koseki_ingest.py:183 が既存例）。
  例外 detail の `str(e)`/`resp.text` 直渡し（main.py:786/799/710→796/735→806）を分類文字列へ。
  document_webhook.py:131 の logger.info(PII直渡し) を `emit(...,log,...)` 経由へ（最優先）。
- **S3**: 全 print を logger 化し `emit` 通す。koseki_ingest.py:175（戸籍全体・構造化値=完全抑止）
  ・決済 print（main.py:1102）最優先。
- **S4**: `str(e)[:N]` を例外クラス名＋分類へ。顧客名を含む `_summary` は `emit(name,...)`。

## 4. H04「イ」: notify fail-closed ＋ dead-man 監視

### 4.1 fail-closed 化
`hub/notify.business_token_env()` の「未設定→顧客Botフォールバック」を廃止し、業務通知は
`DISPATCHBOT_CHANNEL_ACCESS_TOKEN` が無ければ**送信しない＋警告ログ**（顧客Botに業務PIIを
乗せない）。欠落を埋める代替が §4.2。

### 4.2 dead-man 監視（HIGH: 外部主体 heartbeat・別 credential・宛先 allowlist）
自プロセス内の監視では「プロセスごと落ちた」場合に気付けない。**外部主体**が生存を監視する:
- 通知経路が「最後に業務通知に成功した時刻」を app-state に記録。
- **外部の heartbeat 主体**（別プロセス/cron/外形監視）が N 時間ごとにこの値を確認し、
  無音なら警報。**警報は業務通知とは別 credential**（別 LINE チャネル or メール）で、
  **宛先は allowlist**（固定の管理者宛先のみ・誤宛先防止）。
- 【OPEN・owner=大野/司令塔】外部主体の実体（GitHub Actions cron / 外形監視 SaaS / 別Bot）と
  別 credential の用意。判断材料: 既存 weekly-triage cron の流用可否・追加コスト。

## 5. AST 機械強制テスト（notify_channel_policy の拡張）
- print 直書き禁止（logger 経由へ・移行期は許可リスト）。
- 例外 detail への str(e)/resp.text 直渡し禁止（HTTPException 引数検査・P1-005b と同型）。
- 顧客Bot token での業務通知禁止。
- **【裁定済み・2026-07-12 司令塔】機械強制の範囲＝(a) sink 関数の import 境界
  （`emit` を経由しない生 logger/response/line 書き込み経路の検出）＋(b) print /
  resp.text / str(e) の直接出力の禁止、までをテストで固定する。PII 変数名検出
  （変数名ヒューリスティック）は対象外＝レビュー規律で担保**（過検知/漏れが避けられず
  機械強制に不適なため）。

## 6. 段階PR案（順序を再編: contract 先行）
1. **PR-1: redaction contract（emit + policy + kind + §13.1禁止 + unknown/構造化=完全抑止 +
   失敗縮退）** ＋ AST 土台。**伏字水準 OPEN の既定=完全抑止で先行可**（緩めるのは裁定後）。
2. **PR-2: S1（4経路）＋§4 fail-closed**（顧客への機微漏れ停止）。
3. **PR-3: S2 応答body最小化＋例外分類化**。
4. **PR-4: S3 print全廃＋document_webhook redact**。
5. **PR-5: S4 str(e)分類化＋AST 機械強制の本格化**。
6. **PR-6: dead-man 監視（外部主体・別credential・allowlist）**。
各 PR で全 suite 回帰・既存テスト無変更を維持。

## 7. OPEN・BLOCKED
- 【OPEN・owner=大野】伏字水準（ログ/業務LINE の出し分け・既定=完全抑止）。
- 【OPEN・owner=大野/司令塔】dead-man 外部主体と別 credential の実体。
- 【裁定済み・2026-07-12】AST 機械強制の範囲＝sink import 境界＋print/resp.text/str(e)
  直接出力のテスト固定まで。PII 変数名検出は対象外（レビュー規律・§5）。
- 【OPEN・owner=大野】**過去ログの裁定**（既に Railway 集約ログに残った PII の保持/削除方針・
  保持期間・アクセス権）。判断材料: ログ基盤の保持設定・法務。
- BLOCKED_NEEDS_HUMAN: DISPATCHBOT_CHANNEL_ACCESS_TOKEN 本番投入有無（fallback 発火の現況）・
  Railway ログ保持期間/アクセス権。
