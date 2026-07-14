# 作業記録 2026-07-14: P1-103 NM01 v1 HMAC multipart PoC（3巡）と v1 凍結

- Phase/Gate: Phase 1・RV-04（query token → header HMAC 移行）の先行条件 PoC（M11 段3）
- 実施: Claude Code（PC-A）／レビュー: Codex（R-P1-103 / -2 / -3）
- 結果: **PR #124 マージ済み**（merge commit `031d278`）。NM01 v1 を **FROZEN**（範囲限定）に確定
- テスト: **1,266 → 1,287 passed / FAIL 0 / skip 0**（`--ignore=test_triage_classification.py`・PoC 21 tests）

## 1. 目的と成果物

GAS/watcher が送る multipart/form-data（PDF 添付）で、NM01 v1 の content SHA-256 署名契約が成立するかを実測する**隔離 PoC**（`test_hmac_multipart_poc.py`・本番 router/ingest 群に一切結線しない）。RV-04 body 実装の前に「v1 を凍結してよいか」を判定する材料を揃えるのが狙い。

## 2. 3巡の経緯（初回 → fix → fix2）

| 巡 | 対象 | 主眼 | 対象SHA |
|---|---|---|---|
| 初回（P1-103） | PoC 本体 | 検証 a〜e 実測・raw body 取得可・body()×form() 共存 | `ec8dc30`/`9f7a6ca` |
| fix（P1-103-fix） | R-P1-103 反映 | H01 実 path 拘束・H02 key lifecycle reason 分離・§6 整備 | `19a2a3e`/`8e55bf2` |
| fix2（P1-103-fix2） | R-P1-103-2 反映 | H01 を `scope["raw_path"]`（decode 前）基準へ・retiring 警告log・reason を §6.2 に一本化・status-reason table | `86168bc`/`df7fab3` |

- **初回**: 検証事項 a〜e を全自動化。`httpx.Request(files=...)` は `req.read()` を挟まないと `RequestNotRead` になる点を吸収。content 対象＝送出生 body 全体で hash 一致・boundary は hash 対象に内包され正規化不要、を実証。
- **fix（H01/H02）**: 検証器が client 指定 `X-Sig-Path` ヘッダを path の真実源にしていた欠陥を除去し、実 routing path で再計算。key lifecycle の reason を `key_unknown`/`key_revoked`/`key_not_yet_valid`/`key_expired` に分離、retiring は受理+警告。
- **fix2（H01 深掘り）**: 「実 routing path」でも **decode 済み path** を使うと `%2F` が `/` に化けて separator を smuggling される、と R-P1-103-2 が指摘。署名対象 path を **ASGI `scope["raw_path"]`（decode 前生バイト）**へ変更し、`%` を含む path・`//`・dot segment・非 ASCII 生バイトを 400 `bad_path`、`raw_path` 欠落は fail-closed に。reason contract を §6.2 に一本化（§2.5 は参照化）。

## 3. Codex レビュー結果と裁定

| 巡 | REVIEW | RESULT | 反映/裁定 |
|---|---|---|---|
| R-P1-103 | 1巡 | 指摘（H01 path 真実源・H02 lifecycle） | fix で反映 |
| R-P1-103-2 | 1巡 | 指摘（raw_path 基準・retiring 警告log・reason 一本化） | fix2 で反映 |
| R-P1-103-3 | 1巡 | **PASS**（残り所見は DEFER 裁定） | 下記 DEFER |

- **DEFER（HMAC 本体票へ送り）**: **L01/L03**（DRAFT 文書保守・golden 固定手順の細目）・**L02**（body sentinel の assert 強化）。いずれも PoC の結論を覆さない文書/テスト精緻化のため、RV-04 body 実装票で回収する。

## 4. %2F smuggling の実証（検証力担保）

修正前（decode 済み path 基準）の検証器に `/poc/koseki%2Fingest`（decode 後 `/koseki/ingest`＝許可）を投げると **200/ok（攻撃を受理）**。raw_path 基準では **400/bad_path** で拒否。pre-fix/pre-fix2 に対し H01/H02 の新テストが FAIL することをスクラッチで実測し、テストの検知力を担保した。

## 5. NM01 v1 凍結（範囲限定）

- **FROZEN 範囲 = ① content 対象の定義 ＋ ② canonical 形式（§2.1 length-prefix・ORDER）**に限定。
- **③ §2.3 検証順の実装詳細**は凍結対象外（H01/H02 で硬化。§6.1/§6.2 の status–reason 表を単一の正とする）。
- **④ GAS 適用性 = CONDITIONALLY_RESOLVED**。条件 = (i) GAS caller が multipart を**手組み**（固定 boundary 自選・Content-Type 明示 set・hash は同一バイト列）、(ii) **caller 移行段階の[人]実機検分**で GAS 実体 payload の hash 一致を確認。サーバ契約・wire 形式・path 拘束・key lifecycle は PoC で実証済み。フィールド別 hash への再設計は**不要**。

## 6. デプロイ/マージ

- **PR #124 マージ = `031d278`**（`df7fab3` を祖先に含む）。変更は `test_hmac_multipart_poc.py`＋`docs/design-drafts/DRAFT_RV04_HMAC_MIGRATION.md` のみ（**本番コード 0 ファイル**＝挙動変化なし）。
- 検分: 新 deployment ● Online・`/health` 200・観測後退なし。

## 7. 枠消化の日次一行

- 2026-07-14: P1-103 PoC 3巡（初回/fix/fix2）＋ NM01 v1 凍結（範囲限定）＋ %2F smuggling 実証＋ work-log 固定。開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。

## 8. クローズ

NM01 v1 は content 対象＋canonical 形式で凍結。次は RV-04 body 本体票（DEFER L01/L02/L03 の回収・GAS 手組み multipart の[人]実機検分）。
