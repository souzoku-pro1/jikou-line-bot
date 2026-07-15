# 作業記録 2026-07-15: P1-114（service auth registry fail-fast・replay 5入口展開）

- 票: Phase 1 小粒バッチ PR-A（P1-114 単独・flag ON 解錠条件のため独立検収）
- BASE: origin/main b726171 ／ BRANCH: feat/p1-114-auth-failfast
- 正本: DRAFT_RV04_HMAC_MIGRATION.md（§2.3/§6.1 検証コア・reason 契約は不変）

## 1. 修正内容

### 1.1 壊れ JSON registry の fail-fast 化（沈黙 500 の排除）
従来: `SERVICE_HMAC_KEY_REGISTRY` が壊れ JSON/構造違反のとき、`authorize_ingest` 内の
`load_registry_from_env()` が `ServiceAuthConfigError` を送出→未捕捉→**署名リクエスト毎に
沈黙 500**（起動は成功してしまい、設定ミスが検知しづらい）。

修正（2層）:
1. **起動時 fail-fast（主防衛）**: `hub/service_auth.py` に `validate_registry_startup()` を追加し
   `main.py` の startup event 冒頭で呼ぶ。dual-accept flag ON かつ registry 不正なら
   `ServiceAuthConfigError` をそのまま送出して**起動を失敗させる**（明示的エラーで停止）。
   flag OFF は registry 非参照＝何もしない（現行挙動不変）。
2. **初回参照時の防衛（従防衛）**: `authorize_ingest` の署名経路で `ServiceAuthConfigError` を
   捕捉し、固定 reason `registry_config_error` を既存 decision sink（`_log_ingest_decision`）で
   明示ログ→**明示 503**（detail 固定文字列）。既存の共通 raise に合流させることで
   **新規 sink を増やさない**（redaction 台帳 61 件維持・sink:print ゼロ維持。台帳は
   行移動 3 件のみ resync: `hub/service_auth.py:419→438`・`main.py:669→675`・`main.py:965→971`）。

検証コア（canonical/verify_signature/verify_request・§6.1 status–reason 表）は不変。
既存 reason に `registry_config_error` を追加はしていない（§6.1 の表は署名検証の reason 契約・
本件は設定エラーの運用ログであり、レスポンス detail も固定文字列で分岐情報を漏らさない）。

### 1.2 replay 検証テストの ingest 5入口 parametrize 展開
従来 `test_nonce_replay_409` は `/bank/ingest` のみ。`INGEST_PATHS` 5入口
（koseki/registry/bank/sortation/valuation）× {1回目=通過(400 file無し)・同一 nonce 再送=409}
へ subTest 展開（`test_nonce_replay_409_all_five`）。

## 2. 修正前 FAIL 実測（実出力全文・旧コード＝fix を stash して測定）

```
$ PYTHONUTF8=1 python -m pytest test_rv04b_dual_accept.py::TestRegistryFailFast -v -p no:warnings
============================= test session starts =============================
platform win32 -- Python 3.12.10, pytest-9.1.1, pluggy-1.6.0 -- C:\Users\User\AppData\Local\Programs\Python\Python312\python.exe
cachedir: .pytest_cache
rootdir: C:\work\jikou-line-bot
plugins: anyio-4.14.1
collecting ... collected 4 items

test_rv04b_dual_accept.py::TestRegistryFailFast::test_request_time_broken_registry_explicit_503_not_500 FAILED [ 25%]
test_rv04b_dual_accept.py::TestRegistryFailFast::test_startup_failfast_broken_json_flag_on FAILED [ 50%]
test_rv04b_dual_accept.py::TestRegistryFailFast::test_startup_ok_valid_registry_flag_on PASSED [ 75%]
test_rv04b_dual_accept.py::TestRegistryFailFast::test_startup_ok_when_flag_off_registry_unreferenced PASSED [100%]

================================== FAILURES ===================================
_ TestRegistryFailFast.test_request_time_broken_registry_explicit_503_not_500 _

self = <test_rv04b_dual_accept.TestRegistryFailFast testMethod=test_request_time_broken_registry_explicit_503_not_500>

    def test_request_time_broken_registry_explicit_503_not_500(self):
        # 初回参照時の防衛: 壊れ registry の署名リクエストは明示 503（沈黙 500 にしない）
        # （旧コード: ServiceAuthConfigError 未捕捉 → 500 → FAIL する形）
        path = "/koseki/ingest"
        ct, body = _nofile_multipart()
        h = _sig_headers(path, body, _nonce("ff503"))
        h["Content-Type"] = ct
        nr_client = TestClient(main.app, raise_server_exceptions=False)
        with patch.dict(os.environ, {**_INGEST_ENV, _FLAG: "1", _REGENV: self._BROKEN}):
            r = nr_client.post(path, content=body, headers=h)
>       self.assertEqual(r.status_code, 503, r.text)
E       AssertionError: 500 != 503 : Internal Server Error

test_rv04b_dual_accept.py:296: AssertionError
_______ TestRegistryFailFast.test_startup_failfast_broken_json_flag_on ________

self = <test_rv04b_dual_accept.TestRegistryFailFast testMethod=test_startup_failfast_broken_json_flag_on>

    def test_startup_failfast_broken_json_flag_on(self):
        # 起動時 fail-fast: flag ON + 壊れ JSON → startup が明示例外で停止
        # （旧コード: 起動は成功し、署名リクエスト毎に沈黙 500 → FAIL する形）
        with patch.dict(os.environ, {**self._SCHED_OFF, _FLAG: "1", _REGENV: self._BROKEN}):
>           with self.assertRaises(svc.ServiceAuthConfigError):
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
E           AssertionError: ServiceAuthConfigError not raised

test_rv04b_dual_accept.py:269: AssertionError
=========================== short test summary info ===========================
FAILED test_rv04b_dual_accept.py::TestRegistryFailFast::test_request_time_broken_registry_explicit_503_not_500
FAILED test_rv04b_dual_accept.py::TestRegistryFailFast::test_startup_failfast_broken_json_flag_on
========================= 2 failed, 2 passed in 5.40s =========================
```

**旧コードの沈黙 500（`500 != 503 : Internal Server Error`）と「起動時に落ちない」を実測**。
修正後は同 4 テスト全 pass。

## 3. 受入条件との対応

| 受入条件 | 担保 |
|---|---|
| 壊れ JSON 注入テストで fail-fast 挙動を実測 | §2 の修正前 FAIL 実測＋修正後 pass（startup 明示例外／初回参照 503） |
| 5入口 × replay のテスト網羅 | `test_nonce_replay_409_all_five`（5 subTests・1回目 400／再送 409） |
| 既存 1,370 passed 維持＋新規テスト増分 | §4 全 suite 実出力 |

## 4. テスト（対象 suite・全 suite は COMPLETION_REPORT に記載）

```
$ PYTHONUTF8=1 python -m pytest test_rv04b_dual_accept.py test_sink_ast_policy.py test_redaction_sentinels.py -q
32 passed, 5 warnings, 109 subtests passed in 15.62s
```
- test_rv04b_dual_accept: 11 → **15**（+4 = TestRegistryFailFast。replay は同数のまま 5 subTests 化）。
- sink 台帳: **total 61 不変**（行移動 3 件のみ）・baseline 211 単調減少維持・manifest 不変・
  新規違反ゼロ・sink:print ゼロ維持。

## 5. 枠消化の日次一行
- 2026-07-15: P1-114（registry fail-fast 2層・replay 5入口展開・台帳 resync 3件）。
  開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
