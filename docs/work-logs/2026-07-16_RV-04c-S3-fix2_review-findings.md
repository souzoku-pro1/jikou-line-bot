# 作業記録 2026-07-16: RV-04c S3-fix2（R-RV-04C-S3-2 残所見 4 件）

- 票: RV-04c S3-fix2／対象 BRANCH: feat/rv04c-s3-kintone-lane（882fcce から継続）
- FROZEN 非抵触・台帳 61 不変（行移動 2 件 resync）

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 位置 | テスト |
|---|---|---|---|
| **H02残** 404 確定条件の厳格化 | record 不存在の確定を **HTTP 404 かつ既知 vendor code（GAIA_RE01）**の組合せのみに限定。404×未知 code・code 欠落・非 JSON（code=""）、および 404 以外はすべて `mark_failed_preflight`。`RECORD_NOT_FOUND_CODES` allowlist＋`is_record_not_found(status, code)` | `hub/kintone_lane.py`・`main.py` | `TestGetRecordClassification`（404×既知=done／404×未知=failed／404×欠落=failed／predicate） |
| **M04残** 最古時刻 assert の実質化 | 時刻固定 fixture（5h/3h/0.5h 前）で最古値の **完全一致 assert**（`最古={_iso(t5)}`）＋Stripe/LINE/kintone を同一 fixture に入れた最終回帰（LINE processing 行を含む・provider 分離） | `daily_healthcheck.py`（不変・test 追加） | `test_oldest_time_exact_value`・`test_mixed_all_providers_including_line_processing` |
| **S32-M01** startup 試験の因果特定 | 有効な `SERVICE_HMAC_KEY_REGISTRY` を投入し legacy list のみ正常/異常に切替。異常時の起動停止が **legacy 固定文言（"legacy disabled paths configuration invalid"）** であることまで assert（P1-114 の registry 失敗と区別） | test | `test_s32m01_legacy_cause_isolated_from_registry`・対照 |
| **S32-M02** テスト event ID の決定化 | `id(exc) % 1000` を廃止し **status/内容から決定的かつ一意**な event ID（`gr-404known`・`gr-status-500` 等）を生成。subtest 間衝突を構造的に排除 | test | 同 `TestGetRecordClassification` |

## 2. 修正前 FAIL 実測（H02残・実出力）

```
$ pytest TestGetRecordClassification::test_404_unknown_code_is_failed \
         TestGetRecordClassification::test_404_missing_code_is_failed   （pre-fix2 コード）
E   AssertionError: 'record_not_found' != 'get_record_error'   （404×未知 code）
E   AssertionError: 'record_not_found' != 'get_record_error'   （404×code 欠落）
```
**現行（HTTP 404 なら code を問わず done 化）**が、app/endpoint/設定起因の 404（未知 code・
code 欠落）を skip_record_not_found として **done 化＝未処理を正常終了に固定**する形を実測。
修正後は failed_preflight（LINE write 0・分類コード `get_record_error_404_<code>`）へ。

## 3. テスト（対象・台帳）

```
$ PYTHONUTF8=1 python -m pytest test_rv04c_kintone_lane.py -q
46 passed, 14 subtests passed
```
- S3-fix1 の 40 → **46**（+6＝H02残 3・M04残 2・S32-M01 1。S32-M02 は既存テストの決定化）。
- sink 台帳 **61 不変**（main.py の H02残 コメント追加による行移動 2 件 resync のみ）。

## 4. FROZEN 非抵触・flag OFF 不変

- サーバ contract（canonical/verify_*）・migration 非接触。`is_record_not_found` は
  kintone_lane 内の純関数。flag OFF（`KINTONE_EVENT_DEDUP_ENABLED` 未設定）は現行経路
  （`_ev=None`→従来どおり `record=None`→record_not_found）で byte 不変。

## 5. 全 suite（worktree・base 比較）

（COMPLETION_REPORT に実出力）。base S3-fix1 相当 → 本 fix で +6。

## 6. 枠消化の日次一行
- 2026-07-16: RV-04c S3-fix2（404×vendor code 厳格化・最古時刻完全一致・startup 因果特定・
  event ID 決定化）。開始/終了とも **モデル実測 = Fable 5**。
