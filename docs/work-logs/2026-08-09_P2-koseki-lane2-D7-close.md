# 作業記録 2026-08-09: koseki lane2 ゲート4クローズ（D-7 能動404・legacy_blocked 当日採取）

- TASK_ID: KOSEKI-LANE2-D7-CLOSE／実施: PC-A（READ_ONLY 検分＋docs のみ・コード変更なし）／記録日 2026-08-09
- 手順書: `docs/runbooks/2026-07_S4-S5_cutover-checklist.md`（D-7・S4C-H02/S4C-M01）
- 先例形式: `2026-07-18_S5-cutover-close.md` §2b（sortation lane1 の D-7 能動404 採取）と同型
- 前提記録: `2026-07-20_P2-koseki-cutover.md`（D-3〜D-5 相当完了・残置(a)=koseki 能動404）
- 対象 lane: **koseki（lane2）のみ**。本書は D-7 証跡②（能動404＋`legacy_blocked` 対応ログ）と
  計数分離（S4C-M01）の固定のみを行う。証跡③（credential 削除）は本書の対象外（§4）。

## 1. 実施済み事実（2026-08-09）

- **[人]** が本日 2026-08-09、旧 query token を用いて `POST /koseki/ingest` へ能動アクセスし、
  **HTTP 404（`{"detail":"Not Found"}`）を実測**（S4C-H02 の役割分担どおり。PC-A は能動アクセスを
  行わず、HTTP 結果と固定 reason ログの読取検分のみ）。
- 試行は計 **3 回**（Railway ログ実測・いずれも JST 2026-08-09 夜）:
  | # | UTC | JST | query の実態 |
  |---|---|---|---|
  | 1 | 2026-08-09 12:13:40,941 | 2026-08-09 21:13:40 | `token=` の値がプレースホルダ文字列（「値」の URL エンコード）＝token 実値でない |
  | 2 | 2026-08-09 12:19:00,225 | 2026-08-09 21:19:00 | 旧 token 実値（マスク） |
  | 3 | 2026-08-09 12:23:24,367 | 2026-08-09 21:23:24 | 旧 token 実値（マスク） |
- 3 回とも **404＋`reason=legacy_blocked`**。試行 1（token 実値でないもの）も同一 reason であることは
  「停止 path は token 検証**前**に 404（存在しないフリ）で遮断」という設計（先例 §2b と同一）と整合。

## 2. 採取ログ（PC-A・railway CLI 読取・現デプロイ世代内）

**現デプロイ世代**: deployment `d59b3a84-f64d-48fa-84fe-62f37729d8ea`
（2026-07-31 07:43:37 UTC 起動・commit `5e82a7b` = main PR #187・status RUNNING）。
本日の 3 試行はすべて**この世代内**で採取可能（retrievable・世代交代制約なし）。

採取行（`railway logs` 実出力。**URL の `token=` 以降は旧 token 実値が写るためマスク**・
`key_id`/`caller` はアプリ自身のマスク出力のまま）:

```
2026-08-09 12:13:40,941 INFO hub.service_auth service-auth ingest decision key_id=（record_id・非表示） caller=（record_id・非表示） reason=legacy_blocked
INFO:     100.64.0.3:48662 - "POST /koseki/ingest?token=<masked> HTTP/1.1" 404 Not Found
2026-08-09 12:19:00,225 INFO hub.service_auth service-auth ingest decision key_id=（record_id・非表示） caller=（record_id・非表示） reason=legacy_blocked
INFO:     100.64.0.4:56288 - "POST /koseki/ingest?token=<masked> HTTP/1.1" 404 Not Found
2026-08-09 12:23:24,367 INFO hub.service_auth service-auth ingest decision key_id=（record_id・非表示） caller=（record_id・非表示） reason=legacy_blocked
INFO:     100.64.0.5:46684 - "POST /koseki/ingest?token=<masked> HTTP/1.1" 404 Not Found
```

- **`hub.service_auth` の `reason=legacy_blocked` と HTTP 404 が 3 組とも 1:1 で対応**＝停止 lane への
  旧 token 試行が専用 reason で計数されつつ 404 遮断されていることを実出力で固定（D-7 証跡②）。

## 3. 計数（S4C-M01・`legacy_blocked` を `ok` と分離）

観測窓 = 今回採取できたログ範囲 **2026-08-02 14:14 UTC 〜 2026-08-09 12:23 UTC**（約 7 日・185 行。
`railway logs` の取得上限による窓であり、世代起動 7/31〜8/2 の区間は本採取に含まれない——制約として明記）:

| 計数対象 | 件数 | 備考 |
|---|---|---|
| `reason=legacy_blocked` | **3** | すべて本日の [人] 能動試験（§1 の 3 試行）。第三者・GAS からの legacy 試行は **0** |
| `reason=ok`（署名成功） | **0** | 窓内に koseki ingest の署名トラフィック自体なし（GAS トリガー対象なし） |
| `?token=` 付き POST の 200 | **0** | legacy 成功 0 の継続 |
| `POST /koseki/ingest` 総数 | **3** | 全件 404（上記 3 試行のみ） |

- 「**試行はあるが遮断されている**」（本日の能動試験 3 件＝全件 `legacy_blocked`）と
  「**そもそも来ていない**」（それ以外の期間の legacy 試行 0）の区別を実測で固定。
- 署名経路の 200 継続は窓内トラフィック 0 のため新規計上なし（先例 §1 と同じ傍証構造:
  legacy 成功が新規発生していないことの傍証。署名経路の実送 200 は
  `2026-07-20_P2-koseki-cutover.md` §6.1 の一次記録＝計 3 件が既存証跡）。

## 4. lane2 の D-7 retirement evidence 現況

| # | 証跡 | 状態 | 根拠 |
|---|---|---|---|
| ① 署名成功の実送 | 200 計 3 件（D-5 停止後 1 件含む） | 充足（一次記録・7/20） | `2026-07-20_P2-koseki-cutover.md` §6.1 |
| ② 能動404実測 | 旧 token → 404＋`reason=legacy_blocked` 対応ログ・当日採取・現世代内 | **本書で充足（2026-08-09）** | §1・§2 |
| ＋計数 | `legacy_blocked` を `ok` と分離して保存 | **本書で充足** | §3 |
| ③ credential 削除 | `*_INGEST_TOKEN` env 削除・GAS 旧 token 定数削除・[人]実見 | **未充足（本書対象外・残置）** | checklist D-6 末尾「当該 lane の D-5 安定確認後」。実施要否・時期は司令塔裁定 |

→ 本書は**ゲート4（D-7 の②＋計数）のクローズ固定**まで。lane2 retirement の完全クローズは
証跡③の充足後に司令塔が裁定する（sortation 先例では削除実見の追補 §4-i と同型の後日工程）。

## 5. D-8 以降の照会結果（旧 token rotation の定め）

- checklist の D 節は **D-7 で終わり（次節は「E. 補足」）、D-8 以降は存在しない**。
- 旧 token rotation の手順・タイミングに関する既存の定めは以下（逐語引用・実行はしていない）:
  - D-6 表題・前提:
    > ### D-6. rotation（承認キュー token・4 工程＋5-4 の 3 小工程）[人]→[PC-A]
    > > 承認キュー token は露出済み扱い＝**必須**。§5.2 に厳密準拠。
  - D-6 末尾（ingest 旧 token の削除タイミング）:
    > - ingest 旧 `*_INGEST_TOKEN` 削除・GAS 旧 token 定数削除は **当該 lane の D-5 安定確認後**
    >   （§5.1 rollback の手順 2 が参照するため）。
  - D-7 証跡③（削除の完了定義）:
    > 3. **credential 削除**: `*_INGEST_TOKEN` env 削除済み・GAS 旧 token 定数削除済み・承認キュー
    >    旧 token が kintone URL から消えたこと（[人]実見）＋ 5-4 完了。
- 承認キュー token（`KINTONE_WEBHOOK_TOKEN`）の rotation 4 工程は **lane1 の回（7/18・D-6b）で完了済み**。
  koseki 固有の残りは ingest 旧 token の**削除**（証跡③）であり、rotation の要否・時期は
  司令塔が正本に基づき裁定する（本書は引用のみ）。

## 6. 枠消化の日次一行

- 2026-08-09: koseki lane2 ゲート4クローズ（D-7 能動404 の当日ログ採取・`legacy_blocked`×3＝
  全件 [人] 能動試験・`ok` と分離計数・現デプロイ世代 `d59b3a84`(#187) 内で retrievable・
  証跡③は残置＝司令塔裁定待ち）。実施 = PC-A（READ_ONLY 検分＋docs・コード変更なし）。
  モデル実測 = Fable 5。
