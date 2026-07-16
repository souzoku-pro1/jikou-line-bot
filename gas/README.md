# gas/ — RV-04c GAS 署名ヘルパ（repo 正本 → S4 で GAS へ反映）

- `rv04c_signing.js` — NM01 v1 HMAC 署名ヘルパ（builder・filename sanitize・canonical・
  HMAC・`rv04cSignedFetch_`・`SIGNED_LANES`）。DRAFT_RV04C_CALLER_MIGRATION.md §1〜§3 準拠。
- `rv04c_selftest.js` — golden self-test（`rv04c_goldenSelfTest`）＋大サイズ builder テスト
  （`rv04c_builderLargeTest`）。GAS 実機で実行し Logger 出力を採取する。

## 正本の扱い（重要）

- **この署名ヘルパは S4 で GAS 本番へ反映するまで repo（このディレクトリ）が正本**（RV-04c
  票規約）。S4 反映後は `legacy/gas/README.md` と同じ「**正本は GAS 側**」規約へ移行する
  （以後 repo 側は読み取りコピー・編集は GAS→`clasp pull` で同期）。
- 対して `legacy/gas/` は既存 watcher の読み取りコピー（正本は GAS 側）。

## S4 で大野が行う工程（PC-A は実行しない）

1. `rv04c_signing.js` / `rv04c_selftest.js` を GAS プロジェクト「相続書類自動化」へ反映
   （clasp push または エディタ手動転記）。
2. **Script Properties** に `RV04C_KEY_ID` / `RV04C_SECRET_HEX` を投入
   （secret は大野がローカル生成＝`python -c "import secrets; print(secrets.token_hex(32))"`。
   **コードに平文で書かない**・§3 M03 の採用条件 5 点を満たすこと）。
3. `rv04c_selftest.js` の各 vector に fixture（`docs/design-drafts/rv04c_gas_builder_vectors.v1.json`・
   `rv04_hmac_golden_vectors.v1.json`）の期待値を転記し、`rv04c_goldenSelfTest` を実行。
   全 vector PASS（第0段 body byte 一致を含む）の Logger 出力をスクショ→PC-A が .md へ保存。
4. `rv04c_builderLargeTest` を実行し、出力 sha256 を Python 側（`test_rv04c_gas_builder.py`
   `TestBuilderStage0::test_large_pdf_chunk_boundary_algorithm` と同一入力）と突合。
5. watcher の各 `UrlFetchApp.fetch('/…/ingest?token=…')` を `rv04cSignedFetch_('/…/ingest', parts)`
   へ置換し、`SIGNED_LANES.<lane>=true` で lane 単位に署名経路へ切替（rollback は定数 1 箇所）。

## PC-A 側（repo）の検証

- `test_rv04c_gas_builder.py`: `rv04c_signing.js` と **byte 等価**な Python 参照実装で
  builder/canonical/署名を再現し、fixture・server parser 通し・chunk 等価・filename 規則・
  lane allowlist の一致を pytest で固定する（GAS 実機実行は上記 S4=大野）。
