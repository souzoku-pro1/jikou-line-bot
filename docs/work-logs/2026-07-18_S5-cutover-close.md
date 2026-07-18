# 作業記録 2026-07-18: RV-04c S5 cutover クローズ（retirement evidence 固定）

- TASK_ID: S5-RETIREMENT-EVIDENCE（＋fix1）／実施: PC-A（READ_ONLY 検分＋docs）／記録日 2026-07-19
- 正本: `DRAFT_RV04C_CALLER_MIGRATION.md` rev D5・SHA `c32c45df42370618e43903f94a59715081d23552`
- 手順書: `docs/runbooks/2026-07_S4-S5_cutover-checklist.md`
- 対象 lane: **sortation（lane1）のみ**本番署名移行完了。koseki/registry 等は Phase 2（§末）。
- **ステータス: 条件付きクローズ（削除実見の追補待ち・S5C-H01）**。retirement 3 点のうち証跡③
  （credential 失効）は D-5 の env/GAS 定数**削除が未充足**のため暫定。削除実見の追補で充足化する。

## 0. 実施済み事実（司令塔記録・2026-07-18）

| 工程 | 事実 |
|---|---|
| D-3 署名成功 | sortation 署名経路の本番成功（GAS→HMAC→200→`[照会中]` リネーム・初回 7/18 午後） |
| D-4 収束 | 署名成功 3 回以上（`[照会中]` 3 件）・legacy 成功 0 収束（HTTP Logs に token 付き POST なし） |
| D-5 legacy 停止 | `SERVICE_AUTH_LEGACY_DISABLED_PATHS=/sortation/ingest` 投入済み |
| D-7 能動404 | [人] が旧 SORTATION token で POST → `{"detail":"Not Found"}`＝404 実測 |
| D-6a | `SORTATION_INGEST_TOKEN` 前進失効（新値・未配布） |
| D-6b | `KINTONE_WEBHOOK_TOKEN` rotation 4 工程完了（5-1 NEXT 併存→5-2 URL 更新→5-3 実着 200→5-4a 差替え→5-4b 200→5-4c NEXT 削除）。5-3 途中で 404 継続期間あり（値の取り違え・7/18 23:33 JST の 404 等）→最終 200 |

## 1. ログ計数（PC-A 読取・**世代交代による制約を明記**）

- **重要な制約**: 現デプロイ **`e519b725`** は **2026-07-18 15:02:42 UTC（＝2026-07-19 00:02 JST）起動**。
  7/18 午後の D-3/D-4/D-7（例: 能動404 は 7/18 23:33 JST＝14:33 UTC）は**前世代デプロイ**の
  期間で、Railway は**旧世代のアプリログを世代交代で失効**させるため、**7/18 の実イベントを
  現時点のライブログから独立再計数することはできない**（`railway logs` は現デプロイ分のみ・
  `--since 2026-07-18T13:00Z..15:00Z` 指定でも現デプロイ外は 0 件）。7/18 の計数は上記
  **司令塔記録（HTTP Logs 実見・[人]実測）を一次証跡**とする。
- **現デプロイ窓（2026-07-19 00:02 JST 以降）のライブ実測**:
  ```
  railway logs（現デプロイ e519b725 全期間）
  → POST /sortation/ingest : 0 件
  → reason=ok（service-auth ingest decision）: 0 件
  → legacy_blocked : 0 件
  → ?token= 付き POST /sortation/ingest : 0 件
  ```
  ＝当窓で sortation ingest トラフィック自体が無く（GAS トリガー未発火 or 対象ファイル無し）、
  **legacy 成功 0 の収束が継続**していることの傍証（新規 legacy 成功が発生していない）。
  署名成功の新規計上も当窓では 0（トラフィックが無いため）。
- **D-4 根拠（?token= 付き成功の最終時刻・以後 0）**: 7/18 の HTTP Logs で司令塔が
  「token 付き POST なし（収束）」を確認済み。現デプロイ窓でも ?token= 付き成功 0 件を
  ライブ確認（上記）＝**最終 legacy 成功以降 0 件を維持**。
- **観測窓の限界（S5C-M01・明記）**: 司令塔一次記録から固定できる範囲は「**D-3 初回署名成功
  = 2026-07-18 午後**、以後 **`[照会中]` 3 件**（署名成功 3 回以上）」まで。**lane 別最大実行間隔の
  実測 cadence（観測窓＝間隔×2 の基礎値）・署名成功 3 件の正確な各時刻は、旧世代デプロイの
  アプリ/HTTP ログが世代交代で失効したため独立再検証不能**（一次記録依存）。したがって D-4 の
  収束判定は「司令塔一次記録（HTTP Logs 実見）＋現デプロイ窓のライブ 0 件」に依拠し、
  ログからの厳密な cadence 再算出は不可である旨を retirement 判定に反映する（§2）。

## 2. retirement evidence（3 点・充足）

| # | 証跡 | 状態 | 根拠 |
|---|---|---|---|
| ① 署名成功の実送 | sortation 署名経路 200×3 以上（`[照会中]` 3 件） | **充足（一次記録依存・S5C-M01）** | D-3/D-4（司令塔記録・7/18 HTTP Logs。cadence/各時刻は旧世代ログ失効で再検証不能） |
| ② 能動404実測 | 旧 SORTATION token → 404・**reason=legacy_blocked 対応ログ採取済み**（§D-7・00:29 JST 再試験） | **充足** | D-7（[人]実測＋PC-A ログ採取・現デプロイ世代内） |
| ③ credential 失効 | `KINTONE_WEBHOOK_TOKEN` rotation 4 工程完了・NEXT 削除済み／`SORTATION_INGEST_TOKEN` **前進失効（新値・未配布）**。ただし **D-5 の env 削除＋GAS 旧定数削除は未充足（残置）** | **暫定（S5C-H01）** | D-6a/D-6b（司令塔記録）＋§4-i 残置 |
| ＋計数 | D-4 収束集計（署名≥3・legacy 0）／D-5 停止後 legacy_blocked を ok と分離 | 7/18 分は一次記録・現窓ライブは §1／00:29 再試験は §D-7 で採取 | §1・§D-7 |

→ **retirement 判定 = 条件付きクローズ（S5C-H01）**。証跡①②は充足（②は 00:29 再試験で reason
出所まで採取）。**証跡③は「rotation 完了＋前進失効」までで、D-5 が要求する
`SORTATION_INGEST_TOKEN` env 削除・GAS `SORTATION_TOKEN` 定数削除が未実施のため暫定**。
D-5 安定確認後の**削除実見の追補をもって充足化**する。またライブ再計数は世代交代で制約され、
D-4 の cadence/各時刻は一次記録依存である（S5C-M01）。

### 2b. D-7 能動404 の当日ログ採取（H02・現デプロイ世代内＝retrievable）

**2026-07-19 00:29 JST 再試験**（[人] が旧 SORTATION token で POST・PC-A がログ採取）:
```
2026-07-18 15:28:58,760 INFO hub.service_auth service-auth ingest decision key_id=（record_id・非表示） caller=（record_id・非表示） reason=legacy_blocked
INFO:     100.64.0.3:29454 - "POST /sortation/ingest?token=<masked> HTTP/1.1" 404 Not Found
```
- UTC `15:28:58` ＝ **JST 2026-07-19 00:28:58**（≈00:29 JST）。現デプロイ `e519b725`
  （15:02:42 UTC 起動）の世代内でログ採取可能。
- **`reason=legacy_blocked`（`hub.service_auth`）と HTTP 404 が対応**＝停止 lane への旧 token 試行が
  token 検証前に 404（存在しないフリ）で遮断され、専用 reason で計数されていることを実出力で固定。
- URL の `token=` 以降は**マスク**（旧 token 値が写るため。採取行では値は空表示だったが規律に従い
  マスク表記とする）。
- **区別（S5C 追補）**: 初回 **7/18 23:33 JST の試験**は「**HTTP 404 のみ・reason 出所未証明**」
  （当時のログは前世代で失効・decision ログ未採取）。本 00:29 JST 再試験で **reason=legacy_blocked
  まで採取**し、能動404 の証跡を確定した。

## 3. 経過インシデント（記録）

1. **GAS アカウント取り違え**（S5C-H03・**対処済み 7/19**）: 旧版プロジェクト（tototonature 側
   6/02 版・誤投入先）へ RV04C_ プロパティ/コードを誤投入 → 正本プロジェクトへ再投入。
   **7/19 に [人] が旧版から `RV04C_KEY_ID`/`RV04C_SECRET_HEX` を削除**し、**共有実見＝編集者・
   閲覧者とも本人のみ（単独所有）を確認**。→ **secret 露出先は本人アカウント内に限定**と証明され、
   **司令塔裁定により rotation 不要**（露出が第三者に及んでいないため）。§4-ii=完了。
2. **registry JSON 破損による起動停止**: `SERVICE_HMAC_KEY_REGISTRY` 破損で起動停止＝
   **P1-114 の正常動作**（沈黙 500 でなく起動 fail-fast）。rollback で即復旧。
3. **Script Properties 投入形式誤り**: key ID を「名前」欄へ投入 → キー名/値の 2 行形式へ是正。
4. **Vision API billing 403 検知**: → **RCF-M14 起票**（§4-iii）。
5. **rotation 5-3 の 404 継続**（値の取り違え・7/18 23:33 JST 等）→ 最終的に 200（D-6b 完了）。

## 4. 残置事項

- (i) **`SORTATION_INGEST_TOKEN` env 削除**と **GAS `SORTATION_TOKEN` 定数削除**は D-5 安定確認後の
  後日（§5.1 rollback 手順 2 が旧 credential 残存を参照するため、安定まで残す設計どおり）。
  **← retirement 証跡③の充足に必要な未実施項目（S5C-H01・条件付きクローズの追補対象）。
  削除実見をもって証跡③を充足化し retirement を完全クローズする。**
- (ii) **旧版 GAS プロジェクトの `RV04C_` プロパティ削除**: **完了（7/19・[人]）**。削除実施・
  単独所有実見・rotation 不要裁定（§3-1）。
- (iii) **RCF-M14 Vision billing 対処**（別裁定・`DRAFT_RCF-M14_vision-billing.md`）。
- (iv) **koseki/registry 等の残 lane 切替は Phase 2**（本 S5 は sortation lane1 のみ）。切替材料は
  S4-5-PREP-LANE23 報告（写しベース・照合ファースト）に準備済み。

## 5. 点火状態の最終スナップショット（flag/env・名前のみ・値非表示）

現デプロイ `e519b725`（7/19 00:02 JST 起動）の起動検分＋司令塔記録ベース。**env 値は読み取らない**
（`railway variables` は値を露出するため使わない）。名前と状態のみ:

| env（名前） | 状態 | 根拠 |
|---|---|---|
| `SERVICE_AUTH_DUAL_ACCEPT_ENABLED` | **ON（設定）** | 司令塔記録（点火済み）。起動が registry/legacy strict 検証を通過＝ON 前提と整合 |
| `SERVICE_HMAC_KEY_REGISTRY` | **設定（gas-ingest-2026-07a 等）** | 起動成功＝P1-114 4象限 PASS（破損なら起動停止） |
| `SERVICE_AUTH_LEGACY_DISABLED_PATHS` | **設定（`/sortation/ingest`）** | D-5・起動 strict 検証通過（異常形なら固定文言停止） |
| `KINTONE_WEBHOOK_TOKEN` | **設定（rotation 後の新値）** | D-6b 完了 |
| `KINTONE_WEBHOOK_TOKEN_NEXT` / `_EXPIRES` | **削除済み** | D-6b 5-4c。起動ログに NEXT 残置 notice/警告なし＝削除と整合 |
| `INBOUND_EVENT_DURABLE_ENABLED` | **未設定=OFF（推定）** | 起動ログに `[RV05]` reconcile 出力なし＝OFF |
| `KINTONE_EVENT_DEDUP_ENABLED` | **未設定=OFF（推定）** | 別裁定・本 cutover 対象外 |
| `KINTONE_XFF_OBSERVE_ENABLED` | **未設定=OFF（推定）** | 別裁定 |
| `STRIPE_EVENT_JOURNAL_ENABLED` | 既存（本 cutover 対象外） | — |

- 起動ログ: scheduler 2 ジョブ登録・`Application startup complete`・**traceback/エラーなし**。
  /health = **200・"status":"ok"**。
- ※「推定」は起動ログの signal ベース（値は未読）。厳密な env 名一覧が要る場合は [人] が
  Railway Variables 画面で確認（値露出を伴うため PC-A は実施しない）。

## 6. 枠消化の日次一行
- 2026-07-18: RV-04c S5 cutover クローズ（sortation lane1 retirement evidence 3 点固定・
  インシデント記録・残置事項・flag スナップショット）。ログ世代交代の制約を明記。
  開始/終了とも **モデル実測 = Fable 5**。
