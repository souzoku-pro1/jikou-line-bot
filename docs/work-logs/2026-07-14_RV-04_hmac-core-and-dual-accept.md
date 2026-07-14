# 作業記録 2026-07-14: RV-04（query token → header HMAC 移行）— 検証コア本番化と dual-accept 結線

- Phase/Gate: Phase 1・RV-04（G1 の dual-accept→new-only cutover 要件の前半）
- 実施: Claude Code（PC-A）／独立レビュー: Codex（R-RV-04a / -2 / -b）
- 正本: `docs/design-drafts/DRAFT_RV04_HMAC_MIGRATION.md`（NM01 v1 FROZEN・§2.3・§4 Phase A・§6.1/6.2/6.3）／`docs/design-drafts/rv04_hmac_golden_vectors.v1.json`
- 結果: **RV-04a（PR #127・`a80e3a2`）＋ RV-04b（PR #128・`fd7764a`）マージ済み**。HMAC 検証コア本番化・nonce store DB 化・5 ingest 入口への dual-accept 結線（**flag 既定 OFF＝本番挙動無変化**）まで完了。

## 1. RV-04a — HMAC 検証コアの本番モジュール化（PR #127・`a80e3a2`）

- `hub/service_auth.py` 新設: §2.3 の 8 段検証を `verify_signature`（1〜7段・純関数）＋`verify_request`（8段・DB nonce）に分離。path は ASGI `scope["raw_path"]`（decode 前生バイト）基準（H01）。key registry は env（JSON）から読み起動時検証。§6.2 reason contract を単一の正として実装（retiring は受理+warning 1回・可視は key_id/caller のみ）。
- **nonce store（案B・司令塔裁定 2026-07-14）**: alembic migration **`b7d3e1a9c2f4`**（`signature_nonce(nonce PK, key_id, caller, seen_at, expires_at)` + `ix_signature_nonce_expires_at`）。UNIQUE(nonce) が replay 検知の実体。`consume_nonce` は DB のみ（process-memory 禁止・DB 到達不能は fail-close）。
- golden 5本（`rv04_hmac_golden_vectors.v1.json`・日本語ファイル名/空body/境界長/複数field）を固定＝cross-language 照合材料。
- **レビュー往復**:
  - **R-RV-04a = CHANGES_REQUIRED（HIGH 2）**: H-01 KeyEntry.secret が repr に露出／H-02 nonce 必須化・必須ヘッダ第1段拒否の不足（bad_sig 任せ）。
  - **RV-04a-fix** で反映: secret repr 封鎖（`field(repr=False)`+`__repr__`）・nonce 128bit hex 必須（`bad_nonce`）・必須7ヘッダ欠落を第1段 `missing_header`・DB CHECK(length(nonce)=32)・raw_path 本番テスト・nonce 同時競合の 409 実測。golden byte 不変・8段判定順不変・§6.2 不変。
  - **R-RV-04a-2 = PASS_WITH_FINDINGS**: L-01（confirm 証跡の保存様式）等を残し合格。

## 2. RV-04b — ingest 群への dual-accept 結線（PR #128・`fd7764a`）

- `verify_request` を新 ingest 5 入口（koseki/registry/bank/sortation/valuation）の前段へ `ingest_guard`（Depends）で結線。FastAPI が Form/File を deps より前に消費し生 body が取れない問題を **`BodyCachingRoute`**（flag ON かつ署名ヘッダ在時のみ form parse 前に body をキャッシュ・**ingest 5 入口のみ適用・顧客 Bot /webhook 不適用**）で解決。
- **feature flag `SERVICE_AUTH_DUAL_ACCEPT_ENABLED`（既定 OFF）**: OFF=旧 query token のみ（署名ヘッダ無視・byte 同一）／ON・署名在=署名経路のみ（token へ fallback しない＝downgrade 防止）／ON・署名皆無=旧 token（Phase A 併存）。
- 判定を emit 契約でログ（key_id/caller/reason のみ）・拒否 body は固定文字列（reason 素通しなし）。migration 不要（RV-04a の nonce 表を使用）。
- **R-RV-04b = PASS_WITH_FINDINGS（HIGH 0）**: downgrade 禁止・flag OFF 無変更・BodyCachingRoute スコープ・顧客Bot 非干渉・golden/canonical 不変・fail-close をいずれも独立実測で確認。Low 3 件（下記 §7）。

## 3. migration 本番適用の証跡

- **適用**: `railway run python -c "...DATABASE_URL=DATABASE_PUBLIC_URL; alembic upgrade head"`（PC-A の内部ホストは不達のため PUBLIC 経由・URL 値は非表示）。`3e59f8270aa8 → b7d3e1a9c2f4, signature_nonce` を実行。
- **事後検分（READ_ONLY）= PASS**: `alembic_version=['b7d3e1a9c2f4']`／table_exists=True／columns=`[nonce,key_id,caller,seen_at,expires_at]`／pk=`[nonce]`／check_constraints=`[ck_signature_nonce_len]`（length=32）／indexes=`[ix_signature_nonce_expires_at]`／row_count=0（空・未結線ゆえ当然）。
- migration は DB のみの変更でアプリ再デプロイを伴わず、稼働中 deployment は無変更・`/health` 200 継続。

## 4. デプロイ検分 2 回

| 検分 | deployment | 起動(JST) | traceback/import | `/health` | flag/registry | 判定 |
|---|---|---|---|---|---|---|
| RV-04a マージ後 | `39a838b0` | 2026-07-14 11:27 | 0 / 0（`signature_nonce` 参照 0＝未結線で無害） | 200 | — | **PASS** |
| RV-04b マージ後 | `6fd9b5ac` | 2026-07-14 12:36 | 0 / 0（BodyCachingRoute 起因失敗 0） | 200 | flag/registry とも**未設定（OFF）** | **PASS** |

いずれも scheduler 2 ジョブ（7:00 死活監視 / 8:00 返送期限 JST）登録変化なし・**本番挙動無変化**（RV-04a=未結線／RV-04b=flag OFF）。※アプリログは UTC 表記のため +9h で JST 正規化。

## 5. 逸脱記録

1. **RV-04a の PC-A 独断 rebase**: ブランチ作成時のローカル main が古く（`16cf4bc`）、コミット後に origin/main（`2ca62ab`）へ rebase して票 BASE に一致させた。差分は #126 の work-log 2 本のみ（加算・非衝突）で**実害なし・受理**。恒久策 = **分岐前に `git fetch && git pull --ff-only`**（本 work-log の分岐で適用済み）。
2. **司令塔モデルの一時切替**: 期間中に司令塔側モデルが一時切替。裁定は再確認済みで内容不変。
3. **ff08aac の FAIL 0 申告齟齬**: RV-04b 完了報告が `ff08aac` を「1,328 passed / FAIL 0」と申告したが、実 SHA では evidence の追跡 `.py` の `print()` により `test_sink_ast_policy` が FAIL（`sink:print` 5 件）。報告値はコミット前（`.py` 未追跡）の測定。**PC-A 自主の pre-merge 検分が捕捉し `ae584ba` で解消**（`.py`→`.md` 化）。Codex R-RV-04b が独立に再確認（Medium）。
4. **`confirm_rv04a_fix.py` 証跡不在（L-01）**: RV-04a-fix の修正前 FAIL 実測スクリプトを scratchpad のみに置き履歴に残さなかった。→ 下記 §6 の規律で恒久化。

## 6. 新規規律（恒久）

- **commit → suite → 報告 の順序固定**: FAIL 0 等の数値はコミット後の実 SHA で測る（コミット前の untracked 状態で測らない）。
- **evidence は `.md` 固定・追跡 `.py` に `print()` を入れない**: RV-10 print 全廃方針（P1-112 で sink:print=0 達成）を保つ。スクリプト本文＋実出力全文を work-log(.md) に固定する。
- **修正前 FAIL 実測は独立再現可能な形で保存**: 履歴に残る work-log(.md) にスクリプト＋実出力を固定（scratchpad 限りにしない）。
- **分岐前に `git fetch && git pull --ff-only`**（rebase 逸脱の恒久策）。

## 7. Codex 所見の裁定記録

- **R-RV-04b Low-1（壊れ registry JSON→500）＋ Low-2（nonce replay が /bank のみ・5 入口 parametrize 不足）→ P1-114 として起票**: startup での registry fail-fast 検証（壊れ JSON を起動段で止め、request 時 500 の連鎖を防ぐ）＋ replay を 5 入口 parametrize。**flag ON 化の前提**・見積 1h。
- **R-RV-04b Low-3（flag ON 大 body のメモリ 2 倍常駐）→ RCF 台帳 DEFER**: 署名 content_sha256 に生 body 全体が必須で不可避・PDF 有界・flag opt-in のため実害限定。台帳で追跡のみ。

## 8. 次段への申し送り

- **flag ON 化の前提 4 点**（すべて [人]/大野の裁定・操作を伴う）:
  1. **key registry 発行**（`SERVICE_HMAC_KEY_REGISTRY` の JSON・secret は本番管理・コミット/チャット非表示）
  2. **flag ON**（`SERVICE_AUTH_DUAL_ACCEPT_ENABLED=1`）
  3. **GAS caller 側の署名付与**（手組み multipart・固定 boundary・content_sha256 を送出生バイトで計算＝§2.1 実装制約）
  4. **[人]実機確認**（GAS UrlFetchApp の最終 payload hash がサーバ受信生 body と一致することの実地検証・§7）
- **直前提**: kintone webhook 代替 K1/K2/K3 の選択（§3・ヘッダ不可の 3 本は HMAC 不適用のため別設計）。P1-114（fail-fast＋replay parametrize）は flag ON 前に回収推奨。

## 9. クローズ

RV-04 は「検証コア本番化（RV-04a）→ dual-accept 結線（RV-04b・flag OFF）」まで到達。**本番は flag OFF で無変化**、nonce 表は適用済み。以降は Phase B（GAS 署名付与・[人]実機）→ Phase C（旧 token 停止）へ。開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
