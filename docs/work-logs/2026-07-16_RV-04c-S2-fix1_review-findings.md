# 作業記録 2026-07-16: RV-04c S2-fix1（R-RV-04C-S2 所見 5 件）

- 票: RV-04c S2-fix1／対象 BRANCH: feat/rv04c-s2-fix1（feat/rv04c-s2-gas-signing c707174 から継続）
- FROZEN 非抵触・台帳 61 不変（変更は .js／test／fixture のみ・サーバ contract 非接触）

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 位置 | テスト |
|---|---|---|---|
| **H01** SIGNED_LANES 未参照 | dispatcher `rv04cIngestFetch_(path, opts)` を新設し **SIGNED_LANES[path] を実効化**——true=`rv04cSignedFetch_`（署名）／false・未設定=**既存 legacy fetch（query token・現行送信と byte 同一）**。`rv04cSignedFetch_` から SIGNED_LANES 判定を外し dispatcher を唯一のゲートに。README の「false に戻す rollback」が実際に成立 | `gas/rv04c_signing.js` | `TestSignedLanesWired`（2・構造テスト） |
| **H02** parser テスト検証力不足 | **復元一致方式**へ——隔離テスト endpoint `/echo`（本番ルーティング非接触）で FastAPI 復元後の **field 値・filename・file bytes を完全一致 assert**。delimiter vector は file 全 bytes 一致まで固定 | `test_rv04c_gas_builder.py` `TestServerParserRoundtrip`（4） | 同左 |
| **H03** secret 不一致＋期待値 null 全 SKIP | ①`RV04C_TEST_SECRET_HEX` を正本 golden（1dfa2f…）と一致 ②**全 vector の期待値（body_b64/hash/canonical_b64/signature）を fixture へ固定**＋selftest.js に全 vector 埋め込み（**S4 手作業転記の廃止**） ③期待値欠落=FAIL（**SKIP 禁止**・builder_na のみ enum 例外） | `gas/rv04c_selftest.js`・`docs/design-drafts/rv04c_gas_builder_vectors.v1.json` | `TestFixtureExpectedValues`（4） |
| **M01** driveFileId 未検証 | `validateDriveId_`（gas）／`validate_drive_id`（Python 参照）——固定文字集合 `[A-Za-z0-9_-]{1,128}`。sanitizeFilename_ の fallback 埋め込み前に検証。非 ASCII/CR/LF/quote/欠落は例外。テスト vector 追加 | `gas/rv04c_signing.js`・test | `TestDriveIdValidation`（4） |
| **M02** production 前処理未検証 | `rv04cBuildSignedBody_`（allowlist→sanitize→fallback→builder）を**純関数化**し `rv04cSignedFetch_` と共用。GAS 側 `rv04c_productionPipelineSelfTest`（S4 実行）を goldenSelfTest と並置。Python 参照 `build_signed_body` で repo 側検証 | `gas/rv04c_signing.js`・`gas/rv04c_selftest.js`・test | `TestProductionPipeline`（2） |

## 2. 修正前 FAIL 実測（H02/H03・実出力）

```
=== H02 修正前FAIL実測: 旧parserテスト様式（!=404・特定文言なし）===
  500応答: status=500 → 旧様式でPASS判定 = True（期待True＝検証力不足の実証）
  422応答も同様にPASS判定する（field/file/filename復元をassertしていないため）
  => 修正後は復元一致方式（field値・file bytes・fallback filename の完全一致）

=== H03 修正前FAIL実測: 旧selftest secret不一致・期待値null全SKIP ===
  旧RV04C_TEST_SECRET_HEX = 0011223344556677…
  正本fixture secret      = 1dfa2f9f6becae8c…
  一致 = False（期待False＝S4でsignature転記するとstage3必ずFAIL）
  旧代表vector expect_*=null → 全段SKIP（検証器として機能しない）
  => 修正後: golden secret一致・全vector期待値をfixture固定・SKIP禁止（builder_naのみenum例外）
```

## 3. テスト（対象・台帳）

```
$ PYTHONUTF8=1 python -m pytest test_rv04c_gas_builder.py -q
27 passed, 81 subtests passed
```
- S2 の 14 → **27**（+13＝H01 2・H02 4・H03 4・M01 4・M02 2、既存微修正）。
- sink 台帳 **61 不変**（.js はスキャナ対象外・test は EXCLUDE／test_ prefix・fixture は非コード＝
  **diff ゼロ**）。sink:print ゼロ維持。

## 4. GAS 実機（S4=大野・不変の前提）

- `rv04c_goldenSelfTest`（全 vector 4 段・secret 埋込済み）・`rv04c_productionPipelineSelfTest`
  （前処理経由）・`rv04c_builderLargeTest` を S4 で実行しログ採取。**S4 手作業転記は不要**
  （期待値は全て fixture／selftest.js に固定済み）。
- watcher の各 `UrlFetchApp.fetch('/…/ingest?token=…')` を **`rv04cIngestFetch_('/…/ingest',
  {parts, legacyPayload, legacyToken})` へ置換**（H01 実効化）。lane 切替は `SIGNED_LANES` 定数。

## 5. 全 suite（worktree・base 比較）

（COMPLETION_REPORT に実出力）。base 2b33ffa（+S2 の 14）→ 本 fix で test 差分。

## 6. 枠消化の日次一行
- 2026-07-16: RV-04c S2-fix1（SIGNED_LANES 実効化・parser 復元一致・secret/期待値固定・
  driveFileId 検証・production 前処理関数）。開始/終了とも **モデル実測 = Fable 5**。
