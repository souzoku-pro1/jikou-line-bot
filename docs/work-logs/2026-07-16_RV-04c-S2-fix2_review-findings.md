# 作業記録 2026-07-16: RV-04c S2-fix2（R-RV-04C-S2-2 残所見 3 件）

- 票: RV-04c S2-fix2／対象 BRANCH: feat/rv04c-s2-fix1 系（c1c807e から継続）
- FROZEN 非抵触・台帳 61 不変（変更は .js／README／test／fixture のみ・.py source 非接触＝diff ゼロ）

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 位置 | テスト |
|---|---|---|---|
| **H01残** 唯一ゲート迂回の是正 | ①`gas/README.md` の S4 手順を「各 UrlFetchApp.fetch を **`rv04cIngestFetch_`** へ置換」に訂正（`rv04cSignedFetch_` 直接置換の記述を全廃・迂回禁止を明記） ②構造検査: `rv04cSignedFetch_(` の呼出/定義は計 2（定義＋dispatcher 内 1）のみ・dispatcher 外呼出を禁止 ③legacy 分岐の送信同一性（`?token=`＋encodeURIComponent・`payload: opts.legacyPayload`・`muteHttpExceptions`）を固定 | `gas/README.md`・`gas/rv04c_signing.js` | `TestSignedLanesWired`（+3） |
| **H03未解消** self-test の SKIP 変換根絶 | `rv04c_productionPipelineSelfTest` の catch を廃し **vector.pipeline（match/reject/skip）で明示判定**——`reject`=例外送出が PASS 条件・`match`=body 一致・`skip`=builder_na のみ。**builder_na 以外の SKIP 変換を根絶**。multi_field（meta 未許可）・japanese（driveId 欠落）は `reject` 明示。Python 参照/構造テストにも SKIP 検出を追加 | `gas/rv04c_selftest.js`・fixture（pipeline 追加）・test | `TestProductionPipeline`・`TestFixtureExpectedValues`（+2） |
| **M01残** GAS 型検証 | `validateDriveId_` に **`typeof driveFileId !== 'string'`** ガードを追加（RegExp 暗黙文字列化の排除）。型不正（数値/null/undefined/配列/dict/bool）の拒否を GAS（構造）／Python（`validate_drive_id`）両側で固定 | `gas/rv04c_signing.js`・test | `TestDriveIdValidation`（+2） |

## 2. 修正前 FAIL 実測（H03残・実出力）

```
=== H03残 修正前FAIL実測: 旧 rv04c_productionPipelineSelfTest の SKIP 変換 ===
  旧実装（S2-fix1）: allowlist退行/driveID検証失敗/builder異常を catch→SKIP→continue
  → multi_field(meta未許可) や退行注入が "SKIP" として通り、FAIL にならない（検証漏れ）
  実証（Python参照で旧SKIP方式を模擬）:
    allowlist退行(meta): 例外送出=True → 旧方式判定=SKIP(通過)（検証されず通過＝修正前FAIL相当）
    新方式: pipeline=reject 明示 → 例外送出で PASS(reject)・無例外なら FAIL（SKIP 変換なし）
```

## 3. テスト（対象・台帳）

```
$ PYTHONUTF8=1 python -m pytest test_rv04c_gas_builder.py -q
33 passed, 95 subtests passed
```
- S2-fix1 の 27 → **33**（+6＝H01残 3・H03 2・M01残 2、pipeline 判定を subtests 化）。
- sink 台帳 **61 不変**（.js/README/fixture/test のみ＝スキャナ対象外・diff ゼロ）。

## 4. FROZEN 非抵触・S4 前提

- サーバ contract・.py source 非接触。GAS 実機（`rv04c_goldenSelfTest`・
  `rv04c_productionPipelineSelfTest`〔pipeline 分類で SKIP なし〕・`rv04c_builderLargeTest`）は
  S4=大野。**watcher 結線は `rv04cIngestFetch_` へ置換**（唯一ゲート・README 訂正済み）。

## 5. 全 suite（worktree・base 比較）

（COMPLETION_REPORT に実出力）。base S2-fix1 相当 → 本 fix で test +6。

## 6. 枠消化の日次一行
- 2026-07-16: RV-04c S2-fix2（唯一ゲート迂回是正・pipeline SKIP 根絶・driveId 型検証）。
  開始/終了とも **モデル実測 = Fable 5**。
