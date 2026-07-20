# Runbook: RV-04c S4/S5 実機移行チェックリスト（GAS 署名・rotation・retirement）

- 設計正本: **`docs/design-drafts/DRAFT_RV04C_CALLER_MIGRATION.md` rev D5・
  SHA `c32c45df42370618e43903f94a59715081d23552`**（branch docs/rv04c-draft・**逸脱禁止**）。
  本チェックリストは正本の §1〜§7 を実機手順へ落とすもの。判断は正本に従う。
- 実装参照（main 反映済み）: work-log `2026-07-16_RV-04c-S2*` / `*-S3*`・`gas/README.md`・
  runbook `2026-07_kintone-lane-recovery.md`。
- ラベル: **[人]=大野**（実機操作・secret 生成・env/GAS 変更）／**[PC-A]=READ_ONLY 検分**
  （ログ実測・値は見ない）／**[司令塔]=裁定**（点火可否・観測期間・rollback 判断）。
- **本チェックリストに `INBOUND_EVENT_DURABLE_ENABLED` 点火は含めない**（別裁定・LINE
  redelivery 切替=K4 と同時）。

---

## A. GAS 実機テスト（S4 前段・K3-test プロジェクト流用可）

- **[人]** `gas/rv04c_signing.js` と `gas/rv04c_selftest.js` を GAS プロジェクトへ反映
  （clasp push またはエディタ手動転記）。Script Properties 未投入でも self-test は
  テスト専用 secret（fixture 同値）で完結する。
- **[人]** 次の 3 関数を実行し Logger 出力をスクショ。**[PC-A]** が実出力を .md へ保存。

| 関数 | 合格条件 |
|---|---|
| `rv04c_goldenSelfTest` | 全 vector で stage0/1/2/3 が **PASS**（builder_na は stage0=NA・他段 PASS）。**SKIP が 1 つも無い**こと。末尾 `golden self-test total = PASS` |
| `rv04c_productionPipelineSelfTest` | 各 vector が `vector.pipeline` どおり——`match`=`PASS`・`reject`=`PASS(reject)`・`skip`=builder_na のみ。**`FAIL(...)` が 1 つも無い**こと。末尾 `production pipeline self-test = PASS` |
| `rv04c_builderLargeTest` | 3MB・chunk 境界で body を構築し `sha256` を出力。**[PC-A]** が Python 側 `test_rv04c_gas_builder.py::TestBuilderStage0::test_large_pdf_chunk_boundary_algorithm` と同一入力の sha256 と突合＝一致 |

- **唯一ゲートの明示**: watcher からの実送信は必ず **`rv04cIngestFetch_(path, {parts,
  legacyPayload, legacyToken})` 経由**にする（`rv04cSignedFetch_` を watcher から直接呼ばない
  ＝SIGNED_LANES ゲート迂回で rollback が効かなくなる・H01残）。self-test の pipeline 関数も
  本番前処理 `rv04cBuildSignedBody_` を共用しており、直接 builder を叩く別経路は作らない。
- **停止条件**: いずれかに SKIP/FAIL が出たら **[人]は S4 を進めず [司令塔] へ報告**。

## B. secret 生成と投入（[人]・PC-A は値を見ない）

- **[人]** ローカルで生成（**完成形**・PC-A は実行しない・値は表示しない）:
  ```
  python -c "import secrets; print(secrets.token_hex(32))"
  ```
  → 出力の 64 桁 hex を以下では「値」とだけ呼ぶ（本書に値を書かない）。
- **投入先1: GAS Script Properties**（プロジェクトの設定 → スクリプト プロパティ）:
  - キー `RV04C_KEY_ID` = 値 `gas-ingest-2026-07a`
  - キー `RV04C_SECRET_HEX` = 「値」（上記 hex）
- **投入先2: Railway env `SERVICE_HMAC_KEY_REGISTRY`**（JSON・値は表示しない）——
  **既存 JSON を置換しない。既存 entry を全件保持したまま `gas-ingest-2026-07a` を追加する**
  （S4C-H01）。追加する entry:
  ```json
  "gas-ingest-2026-07a": {
    "secret": "「値」（GAS と同一 hex）",
    "caller": "gas-ingest",
    "allowed_methods": ["POST"],
    "allowed_paths": ["/koseki/ingest", "/registry/ingest", "/bank/ingest",
                      "/sortation/ingest", "/valuation/ingest"],
    "not_before": <投入時刻 unix 秒>,
    "expires_at": <次回 rotation 予定 unix 秒>,
    "status": "active"
  }
  ```
  - caller 1 本＋allowed_paths 5 入口（§3/§9-1 の条件付き採用）。GAS と Railway に**同一 hex**。
  - **placeholder 厳禁**（未投入で点火すると P1-114 の起動時 4象限 fail-fast に落ちる＝これは
    正常動作。§D-1 注記）。
  - **投入前後の集合一致確認（S4C-H01・値は見ない）**:
    - **投入前**: 現 registry の **key ID 一覧のみ**を控える（値・secret は見ない）。取得例
      （Railway の env 値を [人] が手元で・**key 名のみ抽出**）:
      `python -c "import json,sys; print(sorted(json.loads(sys.stdin.read()).keys()))"`
      に現 JSON を渡す（出力は key ID の配列だけ・secret は出ない）。
    - **投入後**: 同じ方法で key ID 一覧を再取得し、**「投入前の既存 key ID 全件 ＋
      gas-ingest-2026-07a」の集合と一致**することを確認（既存 entry の消失・上書きがないこと）。
    - **現状 registry が空/未設定の場合**: 新規 JSON（上記 entry を 1 件だけ持つオブジェクト）で
      可。この場合の投入後 key ID 一覧は `["gas-ingest-2026-07a"]` の 1 件。

## C. GAS 共同編集者ゼロの実見（M03・[人]・S4 前チェック）

- **[人]** GAS エディタ右上の **[共有]**（または「プロジェクトの共有」）を開き、
  **編集者（Editor）が所有者 t-ohno@… の 1 名のみ**であることを実見→スクショ。
  - 閲覧者(Viewer)含め第三者が居ないこと。居れば **§3 M03-5 により即 rotation 対象**＝
    点火前に是正し [司令塔] へ報告。
- 併せて Script Properties 採用条件 5 点（単独所有・共同編集者ゼロ・定期監査・secret/log 禁止・
  権限変更時 rotation）の充足を確認。

- **[PC-A] 点火前ゲート（S4C-M02・kintone レーン点火の前提）**: App 29 refetch の
  **404 厳格分類**（HTTP 404 ＋ vendor code `GAIA_RE01` のみ no-op done／未知 code・code 欠落・
  非 JSON は failed_preflight）の対応テストが **main で PASS** していること、および
  **S3 work-log（`2026-07-16_RV-04c-S3-fix2_review-findings.md` H02残）に記載がある**ことを
  確認する（`test_rv04c_kintone_lane.py::TestGetRecordClassification` の 404×既知/未知/欠落
  対照）。**本番での vendor 異常の人工発生は不要**（テスト済みであることの確認のみ）。

## D. 点火順序（各工程に停止条件つき）

### D-1. dual-accept 点火 [人]→[PC-A]
- **[人]** 問い合わせの少ない時間帯に `SERVICE_AUTH_DUAL_ACCEPT_ENABLED=1`（+ B の registry 投入済み）。
- **[PC-A]** /health 200・起動成功を実測。**registry 不備なら起動が P1-114 で停止する＝正常動作**
  （壊れ registry を沈黙 500 にしない設計）。この場合 [人] は registry を修正して再投入。
- **停止条件**: /health が 200 にならない・起動ログに registry 以外の traceback → [司令塔] 報告。
- **rollback**: env を外す（即時・全 lane 旧 query 挙動へ）。

### D-2. GAS watcher 結線（lane 順次）[人]→[PC-A]
- **[人]** watcher の各 `UrlFetchApp.fetch('/…/ingest?token=…')` を
  **`rv04cIngestFetch_('/…/ingest', {parts, legacyPayload, legacyToken})`** へ置換。
  `SIGNED_LANES.<lane>=true` を **1 lane ずつ**（順序: sortation → koseki → registry/bank/
  valuation）。false のままの lane は legacy 送信（byte 同一）。
- **[PC-A]** 対象 lane の decision ログ `reason=ok` と 200 継続を実測。
- **停止条件**: 対象 lane で 401/403/409 が出る → **[人]は `SIGNED_LANES.<lane>=false` に戻し**
  （rollback は定数 1 箇所）[司令塔] 報告。

### D-3. 署名成功実測 [PC-A]
- **[PC-A]** 各 lane で署名経路 200（`reason=ok`）の実ログを採取（UTC→JST）。

### D-4. 並行観測（cadence 基準）[司令塔]→[PC-A]
- **[PC-A]** lane 別に「署名成功継続 かつ **legacy 成功（token 経路 200）が 0 に収束**」を観測。
- 観測窓は **lane 別最大実行間隔 × 2 かつ 署名成功最低 N=3 回**（§7-3・一律日数は用いない）。
- **[PC-A]** この **lane 別 legacy 成功 0 収束の集計値**を後段 retirement（D-7）で S5 work-log へ
  保存する（S4C-M01・保存工程は D-7 に記載）。
- **停止条件**: legacy 成功が 0 に収束しない lane は停止（D-5）へ進めない。

### D-5. legacy 段階停止 [人]→[PC-A]
- **[人]** 収束を確認した lane から `SERVICE_AUTH_LEGACY_DISABLED_PATHS` に **既知 5 path の
  厳格集合**で追加（例 `/sortation/ingest,/koseki/ingest`）。異常形（未知値・重複・末尾
  slash・空要素・全角）は**起動時 strict 検証で固定文言停止**＝設定ミスを弾く（正常動作）。
- **[PC-A]** 停止 lane への旧 token アクセスが **404＋`legacy_blocked` 計数**・署名経路は 200 継続。
- **rollback（順序付き・§5.1）**: ①server で当該 path を list から外す→②旧 credential の
  残存確認→③GAS `SIGNED_LANES.<lane>=false`。順序厳守（逆順は全断窓）。

### D-6. rotation（承認キュー token・4 工程＋5-4 の 3 小工程）[人]→[PC-A]
> 承認キュー token は露出済み扱い＝**必須**。§5.2 に厳密準拠。

| 工程 | [人]操作 | [PC-A]確認 | rollback |
|---|---|---|---|
| 5-1 期限付き dual-accept | `KINTONE_WEBHOOK_TOKEN_NEXT` に新 token＋`KINTONE_WEBHOOK_TOKEN_NEXT_EXPIRES`（日付）を投入 | 旧 token 200 継続（無停止） | `_NEXT`/`_EXPIRES` を外す |
| 5-2 kintone URL 更新 | kintone App 29 Webhook URL を新 token 付きへ | — | URL を旧へ戻す（5-1 併存中は無停止） |
| 5-3 新 token 実着確認 | App 29 でテスト遷移 1 回 | 新 token 認証成功＋旧 token 到達 0 を実測 | （確認のみ） |
| 5-4a primary 差替え | `KINTONE_WEBHOOK_TOKEN` を新値へ（`_NEXT`=新のまま・再デプロイ） | 起動正常・webhook 200 | 前進のみ（旧値再投入不可） |
| 5-4b 成功ログ実測 | App 29 テスト遷移 1 回 | **primary 差替え後**の新 token 成功を実測 | （確認のみ） |
| 5-4c NEXT 削除 | `KINTONE_WEBHOOK_TOKEN_NEXT`（＋`_EXPIRES`）削除（再デプロイ） | webhook 200・NEXT 残置 notice が消える | `_NEXT` 再投入で 5-4a 直後へ |

- **旧値再投入不可**（露出済み）: 5-4a 以降の問題は「さらに次の新値」で**前へ回す**。
- **NEXT 残置**: 期限（`_NEXT_EXPIRES`）超過で起動ログ固定文言警告＋daily_healthcheck に notice
  （**警報でない**）。期限 owner=[人]。
- ingest 旧 `*_INGEST_TOKEN` 削除・GAS 旧 token 定数削除は **当該 lane の D-5 安定確認後**
  （§5.1 rollback の手順 2 が参照するため）。

### D-7. retirement evidence（3 点セット＋計数・S5 work-log 固定）
- **[PC-A]** 以下 3 点を S5 work-log（.md）に実出力で固定:
  1. **署名成功の実送**: 各 lane で署名経路 200 の実ログ（観測窓内 N 回）。
  2. **能動 404 試験**: **[人]** が旧 query token を用いて手動アクセスし 404 になることを
     発生させる（S4C-H02）。**[PC-A] の役割は HTTP 結果と固定 reason ログ（`legacy_blocked`）
     の読取検分のみ**（能動アクセス自体は行わない）。**旧 token 値は [人] の手元のみで扱い、
     チャット/報告への転記は禁止**（値は表示しない）。
  3. **credential 削除**: `*_INGEST_TOKEN` env 削除済み・GAS 旧 token 定数削除済み・承認キュー
     旧 token が kintone URL から消えたこと（[人]実見）＋ 5-4 完了。
- **計数の保存（S4C-M01・retirement 要件の一部）**:
  - **[PC-A]** **D-4 の lane 別「legacy 成功 0 収束」の集計値**を **S5 work-log へ保存**する
    （停止判断の裏づけ・lane 別に「署名成功継続／legacy 成功が 0 に収束」の実測窓を記録）。
  - **[PC-A]** legacy 停止後、**観測窓内の `legacy_blocked` 専用 reason の試行件数を、署名
    成功 reason（`ok`）と分離して計数**し、**S5 work-log へ保存**する（「試行はあるが遮断
    されている」と「そもそも来ていない」を区別・成功 0 の裏づけ）。
- **retirement 宣言の要件**: 上記 **証跡 3 点（署名成功実送／能動 404／credential 削除）＋
  D-4 収束集計値＋停止後 legacy_blocked 計数** が揃うこと。
- **[司令塔]** 全要件の確認で retirement 完了裁定。

---

## E. 補足

- kintone レーン（`KINTONE_EVENT_DEDUP_ENABLED`・`KINTONE_XFF_OBSERVE_ENABLED`）は D と独立に
  点火可（別 env）。滞留/失敗の人手対応は `2026-07_kintone-lane-recovery.md`。
- 本チェックリストの全 env は既定 OFF/未設定＝現行挙動不変。点火は [人]、タイミングは [司令塔]。

## F. GAS 反映の恒久規律（INC-0720 起点・2026-07-21 昇格）

> 転記元: `docs/work-logs/2026-07-20_P2-koseki-cutover.md` §7（再発防止規律）。
> 以後の**全 lane・全 GAS 反映**に適用する運用規律（追記・本チェックリスト本文は不変）。

- (i) GAS への repo 写し反映は**「全置換」を禁止**する。反映指示には必ず **SIGNED_LANES
  全 5 lane の期待行列を明記**し、反映後に [人] が live の行列を**読み合わせる**。
- (ii) live 直編集で切替済みの lane がある間は、**repo 側 SIGNED_LANES を live 実態に
  同期させる PR を cutover work-log と同時に出す**（repo/live drift の恒久解消）。
- (iii) 機械化: 読み合わせ・drift 検査は `tools/gas_drift_check.py`
  （snapshot 手貼り運用・SIGNED_LANES 期待行列対比・exit 0/1/2）を用いてよい。
  一致（exit 0）を確認できない状態からの live 反映は行わない。
