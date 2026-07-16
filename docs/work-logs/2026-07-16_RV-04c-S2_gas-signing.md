# 作業記録 2026-07-16: RV-04c S2（GAS 署名実装・golden 突合・parser 通し）

- 票: RV-04c S2／設計正本: `DRAFT_RV04C_CALLER_MIGRATION.md` rev D5（d48222b・FROZEN 扱い）
- BASE: origin/main 2b33ffa ／ BRANCH: feat/rv04c-s2-gas-signing
- 同梱 docs: D5-L01（§4.2 表題訂正）は DRAFT が置かれる `docs/rv04c-draft` ブランチ側で
  別コミット（本ブランチには DRAFT が無いため・§6 参照）。

## 1. 実装物

| 対象 | ファイル | 内容 |
|---|---|---|
| GAS 署名ヘルパ（repo 正本→S4 で GAS 反映） | `gas/rv04c_signing.js` | byte 規約 R1〜R5・手組み builder（`buildMultipart_`）・filename sanitize（`sanitizeFilename_`）・canonical（`canonicalV1_`）・HMAC（`hmacHex_`）・`rv04cSignedFetch_`・`SIGNED_LANES`・`LANE_FIELDS`（lane 別 field allowlist） |
| GAS self-test（実機=大野） | `gas/rv04c_selftest.js` | `rv04c_goldenSelfTest`（第0段 builder byte 一致＋3 段照合・**本番 builder 共用**）・`rv04c_builderLargeTest`（H02 大 PDF/chunk） |
| gas 規約 | `gas/README.md` | 「S4 反映まで repo 正本→以後 GAS 側正本」・S4 で大野が行う工程・secret 非埋込 |
| 新 fixture | `docs/design-drafts/rv04c_gas_builder_vectors.v1.json` | 既存 5 本の parts 再表現＋delimiter 類似列内包（M01）・ASCII fallback（§1.1b）・空 field。**既存 `rv04_hmac_golden_vectors.v1.json` は不変** |
| Python 参照＋検証 | `test_rv04c_gas_builder.py` | GAS と byte 等価の参照 builder・第0段/3 段照合・chunk 等価・filename 規則・lane allowlist の JS↔サーバ一致・server parser 通し・D5-C02 |

**GAS 実機実行はこの PC 不可**（node/clasp 無し）。PC-A の担保は「GAS と byte 等価な Python
参照実装での fixture 突合＋実サーバ parser 通し」。GAS 実機での golden self-test・大 PDF
テストは **S4 で大野が実行**（`gas/README.md`・DRAFT §2 の「大野スクショ→PC-A 保存」フロー）。

## 2. golden self-test 実出力（Python 参照側・第0段含む）

```
=== 第0段（H03）parts→参照builder→body byte一致（builder共用） ===
  ascii_filename_multipart           PASS  (156B sha=68ba13471fc5…)
  japanese_filename_multipart        PASS  (157B sha=0585ce11133e…)
  empty_body                         builder_na（空body・hash検証のみ）
  long_boundary                      PASS  (362B sha=42f2135195a1…)
  multi_field                        PASS  (263B sha=db7fbb9a0344…)
  delimiter_lookalike_in_content     PASS  (279B sha=f49d80990b16…)
  ascii_fallback_filename            PASS  (225B sha=720d423d5015…)
  empty_text_field                   PASS  (283B sha=15cba38affef…)

=== 既存golden 3段照合（body_hash / canonical_b64 / signature）5本 ===
  ascii_filename_multipart           hash=P canonical=P signature=P
  japanese_filename_multipart        hash=P canonical=P signature=P
  empty_body                         hash=P canonical=P signature=P
  long_boundary                      hash=P canonical=P signature=P
  multi_field                        hash=P canonical=P signature=P

=== 新規vector（M01 delimiter / ASCII fallback / 空field） ===
  delimiter_lookalike_in_content     builder=PASS
  ascii_fallback_filename            builder=PASS fallback: 住民票の写し.pdf→doc-F-fb.pdf
  empty_text_field                   builder=PASS
```

- 第0段（H03）: parts→**本番 builder**→body_b64 の byte 完全一致を全 multipart vector で確認
  （builder 共用＝別実装での PASS を作らない構造要件は、参照 builder が本 module 単一実装で
  あること＋GAS 側 self-test が `rv04c_signing.js` の builder を呼ぶことで担保）。
- 既存 golden 5 本は body_b64 が新 fixture の再表現と一致（`TestBuilderMatchesExistingGolden`）
  ＝既存 v1 fixture 不変の担保。

## 3. GAS 実機テスト（H02・大 PDF/chunk 境界）

- **PC-A 側（アルゴリズム等価）**: `test_large_pdf_chunk_boundary_algorithm` で **3MB 疑似 PDF**＋
  chunk 境界（8191/8192/8193/1MB）で `build_multipart_chunked == build_multipart`（単純連結）
  ＆ SHA-256 一致を実測（PASS）。これで chunk append アルゴリズム（push.apply 回避）の
  正しさを固定。
- **GAS 実機側（S4=大野）**: `gas/rv04c_selftest.js::rv04c_builderLargeTest` を GAS で実行し、
  出力 sha256 を上記 Python 同一入力の SHA と突合する（Logger 出力を .md 保存）。**これは
  GAS ランタイムでの byte 無変換送出の最終確認であり本 S2 では未実行**（node/clasp 不在）。

## 4. server parser 通し（§1.1b）

`TestServerParserRoundtrip`（TestClient・旧 token 経路で認証通過後、実 multipart parser へ）:
- 正常 PDF part → endpoint 到達（`PDFファイルを送信してください` が返らない＝file が parse された）。
- ASCII fallback filename（`doc-F-rt2.pdf`）body → parse 成功。
- delimiter 類似列内包 content（`--RV04Crt3 not-the-real-delimiter`）→ 実 boundary と衝突せず
  parse 成功（M01: boundary 安全性の根拠は nonce ランダム性であり、類似列は parse を壊さない
  ことを実測）。

## 5. registry 準備（§3・コードのみ・secret は S4 で大野）

`SERVICE_HMAC_KEY_REGISTRY`（env・JSON）へ S4 で大野が投入する entry の**形式**（**secret 値は
placeholder を置かない**＝未投入時は P1-114 の起動時 4象限 fail-fast に落ちる形が正）:

```json
{
  "gas-ingest-2026-07a": {
    "secret": "<大野が secrets.token_hex(32) で生成した 64 hex・PC-A は扱わない>",
    "caller": "gas-ingest",
    "allowed_methods": ["POST"],
    "allowed_paths": ["/koseki/ingest", "/registry/ingest", "/bank/ingest",
                      "/sortation/ingest", "/valuation/ingest"],
    "not_before": <投入時刻 unix 秒>,
    "expires_at": <次回 rotation 予定 unix 秒>,
    "status": "active"
  }
}
```

（caller 1 本＋allowed_paths 5 入口＝§3/§9-1 の条件付き採用裁定どおり。GAS 側 Script Properties
`RV04C_KEY_ID=gas-ingest-2026-07a` / `RV04C_SECRET_HEX=<同値>` も同時投入・§3 M03 の 5 条件。）

## 6. テスト（対象＋全 suite 実測）

```
$ PYTHONUTF8=1 python -m pytest test_rv04c_gas_builder.py -q
14 passed, 53 subtests passed
```
- 内訳: 第0段 builder byte 一致／chunk 等価／大 PDF chunk 境界／既存 golden 一致／
  canonical＋signature／filename sanitize（passthrough・fallback・禁止文字）／fixture fallback／
  lane allowlist JS↔サーバ一致／server parser 通し 3 本／D5-C02（mark_done last_error=NULL）。
- sink 台帳 **61 不変**（新規ファイルは .js＝スキャナ対象外・.py テストは EXCLUDE 対象／
  test_ prefix・.json は非コード）・sink:print ゼロ維持。
- FROZEN 非抵触: `hub/service_auth.py`（canonical/verify_*）・migration・既存 fixture は
  非接触。既存 golden／reason 表／rv04b dual-accept は全 GREEN（§7 全 suite）。

## 7. 全 suite（worktree・base 比較）

（COMPLETION_REPORT に実出力）。base 2b33ffa 1,379 → **+14（test_rv04c_gas_builder）**・回帰ゼロ。

## 8. 枠消化の日次一行
- 2026-07-16: RV-04c S2（GAS 署名ヘルパ・builder byte 検証・parser 通し・chunk 等価・
  registry 形式）。GAS 実機は S4=大野。開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
