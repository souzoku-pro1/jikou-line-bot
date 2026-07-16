# 作業記録 2026-07-16: RV-04c S3-fix1（R-RV-04C-S3 所見 8 件）

- 票: RV-04c S3-fix1／対象 BRANCH: feat/rv04c-s3-kintone-lane（4190e57 から継続）
- FROZEN 非抵触・台帳 61 不変（行移動 9 件 resync のみ）・flag OFF byte 不変（再確認済み §5）

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 位置 | テスト |
|---|---|---|---|
| **H01** | flag ON で id 欠落/空/型不正（dict/list/bool/float）は claim 前に拒否＝**LINE write 0・400 固定 reason・claim_event 未到達**。`extract_event_id` を scalar 限定へ厳格化。拒否は行が残らず滞留監視外のため `observe_pre_claim_reject`（固定 reason）で別計数 | `hub/kintone_lane.py`・`main.py` | `TestInvalidId`（3） |
| **H02** | get_record 例外分類: **404 確定のみ** skip_record_not_found（no-op done）。その他 KintoneError（status 0=timeout/接続・401/403・5xx）は **mark_failed_preflight＋LINE write 0**（done 化しない） | `main.py` | `TestGetRecordClassification`（3） |
| **H03** | legacy 停止の startup 検証・実行時照合を **dual_accept_enabled() で gate**。OFF は検証も照合もしない（env が inert＝旧経路 byte 不変） | `hub/service_auth.py` | `TestLegacyStrict`（H03 3 本追加） |
| **M01** | claim_event の IntegrityError を**同一 dedup_key 行の実在再照合**へ: 存在=duplicate・不在=再送出（別制約違反等の異常を duplicate に握り潰さない） | `hub/kintone_lane.py` | `TestClaimIntegrity`（2） |
| **M02** | mark_noop_done/mark_done/mark_failed_preflight を **rowcount=1 必須**化・0 は `KintoneLaneStateError`（固定分類例外） | `hub/kintone_lane.py` | `TestRowcountRequired`（2） |
| **M03** | `docs/runbooks/2026-07_kintone-lane-recovery.md` 新設（sending 判別 3 点突合・最終判断は人・自動再送なし）。警報文言の runbook 参照を S2 work-log から本 runbook へ修正 | runbook・daily_healthcheck | 監視テストが参照リンクを assert |
| **M04** | §4.2b に **provider 別最古時刻**の算出・表示を追加。混在 fixture（閾値直前/直後・複数 provider 複数行）で件数/最古時刻/文面を完全 assert | `daily_healthcheck.py` | `TestMonitorOldestAndFailed`（3） |
| **M05** | kintone 専用監視に **failed（分類別: get_record_error_<status> 等）**を追加・**stale 系と文言分離**（「kintone滞留(未処理)」/「kintone失敗」）・M03 runbook へリンク | `daily_healthcheck.py` | 同上 |

## 2. 修正前 FAIL 実測（H02/H01/H03・pre-fix1 コードに新テストを適用）

```
$ pytest TestGetRecordClassification TestInvalidId::test_missing_id_rejected_400_no_claim \
         TestLegacyStrict::test_h03_dual_accept_off_bad_value_no_failfast
# H02: 現行（全 KintoneError→record_not_found）が timeout/5xx/401/403 を done 化する:
E   AssertionError: 'record_not_found' != 'get_record_error'   （timeout・skip 分類）
E   AssertionError: 'done' != 'failed'   （status=500 / 401 / 403 の最終 state）
# H01: 現行は id 欠落でも処理してしまう（400 で拒否しない）:
E   AssertionError: 200 != 400
# H03: 現行は dual-accept OFF でも停止 list を検証し bad 値で起動 fail-fast する（＝旧不変でない）:
FAILED …test_h03_dual_accept_off_bad_value_no_failfast
6 failed, 2 passed
```

修正後は同テスト全 pass（§3）。**特に「現行が timeout を done 化する」形を実測**（票 H02 要求）。

## 3. テスト（対象・台帳）

```
$ PYTHONUTF8=1 python -m pytest test_rv04c_kintone_lane.py -q
40 passed, 14 subtests passed
```
- S3 の 25 → **40**（+15＝H01 3・H02 3・M01 2・M02 2・M04/M05 3・H03 2）。
- sink 台帳 **61 不変**（行移動 9 件 resync＝`_iso` ヘルパ追加等のシフトのみ・**新規 sink ゼロ**）。

## 4. 既知制約（work-log 明記・H01）

- **claim 前拒否は行が残らない**（id 欠落/型不正）。したがって**滞留監視（§4.2b）の対象外**。
  観測は Railway ログの固定文言 `kintone webhook rejected pre-claim: invalid_or_missing_id`
  で行う（別計数）。これは webhook 発行元（kintone）の設定不整合の疑いであり、サーバ側の
  データ欠損ではない（正常な id が来れば dedup/state 機構が働く）。

## 5. flag OFF byte 不変の再確認

- `KINTONE_EVENT_DEDUP_ENABLED` 未設定: webhook handler は kintone_lane を import せず現行経路。
- `SERVICE_AUTH_DUAL_ACCEPT_ENABLED` 未設定: legacy 停止 list を検証も参照もしない（H03）。
- 既存回帰（test_webhook_endpoints_regression・test_s1_failclosed・test_rv04b_dual_accept・
  test_hotfix_emit_unbound）全 GREEN。全 suite は COMPLETION_REPORT に実出力。

## 6. 枠消化の日次一行
- 2026-07-16: RV-04c S3-fix1（H01 id 拒否・H02 例外分類・H03 gate・M01 IntegrityError 再照合・
  M02 rowcount・M03 runbook・M04 最古時刻・M05 failed 監視）。
  開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
