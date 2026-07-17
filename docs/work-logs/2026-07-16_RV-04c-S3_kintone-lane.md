# 作業記録 2026-07-16: RV-04c S3（サーバ側 kintone レーン・stale 監視・legacy strict・rotation）

- 票: RV-04c S3／設計正本: `DRAFT_RV04C_CALLER_MIGRATION.md` rev D5（FROZEN 扱い）
- BASE: origin/main 2b33ffa ／ BRANCH: feat/rv04c-s3-kintone-lane（S2 と独立）
- FROZEN 非抵触: `hub/service_auth.py` の canonical/verify_signature/verify_request は未接触
  （追加は legacy 停止分岐・strict 検証のみ）。

## 1. 実装物

| 対象 | ファイル | 内容 |
|---|---|---|
| kintone レーン core | `hub/kintone_lane.py`（新規） | id 冪等 claim（INSERT UNIQUE 勝者）・state 遷移（received/sending/done/failed）・marker 契約（rowcount=1）・phase 別失敗遷移・正常 no-op done＋enum 理由コード（NOOP_REASONS）・stale_hours・XFF observe |
| webhook 結線 | `main.py` `/webhook/kintone/approval` | flag ON: dedup claim→state 遷移・§4.2 phase 表どおり（marker→送信→done／no-op→done／DB 不能→503 H04）。**flag OFF は kintone_lane 非 import で byte 同一**。`_verify_kintone_token`（primary/NEXT dual-accept・§5.2） |
| stale 監視 | `daily_healthcheck.py` `check_journal_backlog` | provider 次元追加（既存 Stripe/LINE は `provider!=kintone` に限定＝不変）＋kintone 専用検査（既定1h・専用文言・§4.2b）。`check_next_token_residual`（NEXT 残置 notice・D2-M01） |
| legacy strict | `hub/service_auth.py` | `SERVICE_AUTH_LEGACY_DISABLED_PATHS` 起動時 strict 検証（H07・既知5 path 厳格集合・異常形は固定文言で起動停止）＋実行時 404（legacy_blocked 計数） |
| startup 結線 | `main.py` startup | legacy strict 検証・NEXT 残置起動ログ警告 |
| tests | `test_rv04c_kintone_lane.py`（新規・25） | §8 全系統 |

**flag 既定 OFF（`KINTONE_EVENT_DEDUP_ENABLED` 未設定）＝現行挙動と byte 同一・ALTER 0**
（inbound_event 既存列のみ・state は Text 値追加）。

## 2. 修正前 FAIL 実測（挙動レベル・実出力）

新規モジュール `hub.kintone_lane` は旧ツリーに無く test 直接実行は ImportError になるため、
**挙動レベル**で実証（flag OFF=旧挙動／旧 `check_journal_backlog` を git show で復元）:

```
=== (A) dedup 修正前挙動: flag OFF（旧＝dedup 未結線）で重複配信が2回処理される ===
  旧挙動 send_line_push 呼出回数 = 2（期待 2＝保護なしで二重処理・修正前FAIL）
  => 修正後 flag ON では 1（dedup が2回目を skip）

=== (B) journal混入 修正前挙動: 旧 check_journal_backlog（provider 無差別） ===
  旧 check_journal_backlog 出力: ['journal滞留: failed が24時間超 1件 (PK=[1]) —
    runbook: docs/runbooks/stripe-journal-recovery.md']
  => kintone failed 行が Stripe runbook 文言の警報に混入 = True（期待 True＝修正前FAIL）
  => 修正後は provider!=kintone 除外で混入しない・kintone は専用文言（§4.2b）
```

その他系統（marker・no-op・stale・rotation・legacy 異常形）は forward テストで固定
（下記 §3）。marker/no-op/rotation は「flag OFF=保護なし」が (A) と同型の before に当たる。

## 3. §8 受入系統 → テスト対応

| §8 系統 | テスト |
|---|---|
| dedup 二重処理 | `TestDedup::test_duplicate_delivery_processed_once`（2配信→送信1回・done/NULL）＋flag OFF 無行 |
| fail-closed | `TestFailClosed`（claim 例外→503・処理 0） |
| marker 後例外の failed 非上書き | `TestMarkerContract::test_marker_after_exception_not_overwritten_to_failed`（例外→state=sending 維持・送信試行済み） |
| marker 失敗時 send 0 | `test_marker_failure_zero_line_write`（rowcount=0→送信0・received のまま） |
| no-op 偽警報防止 | `TestNoopDone`（各分類→done＋enum 理由コード・NOOP_REASONS 内） |
| stale received/sending | `TestStaleMonitor`（3h→kintone 専用警報・0h→無警報） |
| provider 混在不変（D2-M03） | `TestProviderInvariant`（Stripe/LINE/kintone 混在→分離・24h が kintone に波及せず・flag OFF で混入なし） |
| rotation 4 状態 table | `TestRotationTable`（old-only/dual/new+NEXT/NEXT削除後 × 旧新 token） |
| legacy strict 異常形 | `TestLegacyStrict`（valid 集合／異常形5種 raise／起動 fail-fast／404+legacy_blocked／未停止 lane 不変） |
| NEXT 残置 notice | `TestNextResidualNotice`（未設定/期限内/超過/NEXT なし） |
| XFF observe-only | `TestXffObserve`（OFF=素通し／in-cidr／out-cidr は membership False だが遮断しない・handler は 200） |

## 4. テスト（対象＋台帳）

```
$ PYTHONUTF8=1 python -m pytest test_rv04c_kintone_lane.py -q
25 passed, 7 subtests passed
```
- sink 台帳 **61 不変**（行移動 5 件 resync: daily_healthcheck 343→394・362→418／service_auth
  477→528／main 675→754・971→1050）。**新規 sink ゼロ**（kintone_lane の emit は module
  top-level import＝信頼形式・P1-113 の emit_shadow 規則に自ら抵触→是正済み。notice ログは
  固定文言化）。sink:print ゼロ維持。
- 既存回帰（test_webhook_endpoints_regression・test_s1_failclosed・test_rv04b_dual_accept・
  test_hotfix_emit_unbound）全 GREEN＝flag OFF byte 同一・legacy 追加が既存署名経路に非干渉。

## 5. 全 suite（worktree・base 比較）

（COMPLETION_REPORT に実出力）。base 2b33ffa 1,379 → **+25（test_rv04c_kintone_lane）**・回帰ゼロ。

## 6. 申し送り（S4 前提）

- flag 点火順（別 env・独立）: `KINTONE_EVENT_DEDUP_ENABLED`（dedup+state）／
  `KINTONE_XFF_OBSERVE_ENABLED`（観測）／`SERVICE_AUTH_LEGACY_DISABLED_PATHS`（停止・段4）／
  `KINTONE_WEBHOOK_TOKEN_NEXT`+`_EXPIRES`（rotation・段5）。いずれも既定 OFF/未設定。
- kintone stale runbook の本文は §4.2 人手再操作手順（本 work-log 群参照）。
- 監視統合: kintone flag ON かつ DATABASE_URL 在で check_journal_backlog が kintone 専用検査を
  実行（Stripe flag と独立）。

## 7. 枠消化の日次一行
- 2026-07-16: RV-04c S3（kintone id 冪等・state 遷移・stale 監視 provider 分離・legacy strict・
  rotation dual-accept）。開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
