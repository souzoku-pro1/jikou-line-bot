# 作業記録 2026-07-15: P1-114-fix1（R-P1-114 所見対応・registry fail-fast 4象限化）

- 票: P1-114-fix1（PR #133 への追補）／BRANCH: feat/p1-114-auth-failfast（27514e1=local de3e366 から継続）
- 規律遵守: commit→suite→報告固定・修正前 FAIL 実出力を本 .md へ保存・台帳 61 不変

## 1. 所見 → 修正 対応表

| 所見 | 修正 | 実装位置 | 担保テスト |
|---|---|---|---|
| **RP1114-H01** | fail-fast を4象限に拡張: `load_registry_strict()` 新設——① env 欠損・空文字 ② JSON 破損・非 object ③ entry 型不正（非 dict・必須 field 欠落・型違い） ④ **実効鍵数 0**（`{}`・全 revoked・全 expires_at 超過。実効鍵= status active/retiring かつ期限内）。startup（`validate_registry_startup`）と初回参照（`authorize_ingest`）の**両層が同一 strict 読込**を使い、欠損・空が「空 registry による署名拒否（key_unknown 401）」へ流れる経路を排除＝設定不備はすべて `registry_config_error` 固定 reason の 503 に統一 | `hub/service_auth.py` | `test_h01_startup_failfast_all_quadrants`（10象限値×固定文言）・`test_h01_request_time_503_all_quadrants`（10象限値×503固定detail） |
| **RP1114-M01** | startup 境界の例外を**固定文言のみ**へ変換: `ServiceAuthConfigError("service auth registry configuration invalid")` を `from None` で送出（元例外の key_id・field 名・registry 断片を traceback 連鎖表示させない）。詳細診断は既存 decision sink（redact 経由・request 時の固定 reason）のみ。**新規 sink 追加なし** | 同上 | `test_m01_sentinel_not_in_exception_log_or_body`（sentinel key_id/secret を注入→起動例外 repr・整形 traceback・全ログ出力・503 body の不含を機械確認） |

## 2. 修正前 FAIL 実測（実出力・旧コード＝de3e366 の hub/service_auth.py を stash して測定）

```
$ PYTHONUTF8=1 python -m pytest test_rv04b_dual_accept.py::TestRegistryFailFast::test_h01_startup_failfast_all_quadrants \
    test_rv04b_dual_accept.py::TestRegistryFailFast::test_h01_request_time_503_all_quadrants \
    test_rv04b_dual_accept.py::TestRegistryFailFast::test_m01_sentinel_not_in_exception_log_or_body -p no:warnings
（抜粋・要点）
# ① 欠損・空／④ 実効鍵0 → 旧コードは起動成功してしまう:
E   AssertionError: ServiceAuthConfigError not raised            （q1_env_unset / q1_env_empty /
                                                                   q4_zero_keys / q4_all_revoked / q4_all_expired）
# ②③ → 例外は出るが詳細メッセージ（M01 違反）:
E   AssertionError: 'registry JSON parse error: Expecting property name enclosed in double quotes' != 'service auth registry configuration invalid'
E   AssertionError: 'registry must be a JSON object of key_id -> entry' != 'service auth registry configuration invalid'
E   AssertionError: "key 'kid': entry must be an object" != 'service auth registry configuration invalid'
E   AssertionError: "key 'kid': missing field caller" != 'service auth registry configuration invalid'
E   AssertionError: "key 'kid': secret must be hex string" != 'service auth registry configuration invalid'
# 初回参照側 → 欠損・空・実効鍵0 が 401（空 registry の署名拒否）へ流れる:
E   AssertionError: 401 != 503 : ('q1_env_unset', '{"detail":"signature verification rejected"}')
E   AssertionError: 401 != 503 : ('q1_env_empty', '{"detail":"signature verification rejected"}')
E   AssertionError: 401 != 503 : ('q4_zero_keys', '{"detail":"signature verification rejected"}')
# M01 sentinel → 旧コードは起動例外 repr に key_id 断片が混入:
E   AssertionError: 'SENTINEL-KID-73AF' unexpectedly found in
    'ServiceAuthConfigError("key 'SENTINEL-KID-73AF': missing field allowed_methods")'
======================== 16 failed, 2 passed in 5.54s =========================
```
修正後は同 3 テスト（subTest 10象限×2層＋sentinel 4面）全 pass。

## 3. 設計メモ

- **実効鍵数 0 の定義**: `status in ("active","retiring") and now <= expires_at` の鍵が 0。
  revoked のみ・全鍵失効は「鍵数 > 0 でも運用上ゼロ」として fail-fast（`not_before` 未来の鍵は
  ローテーション準備中として実効に数えない判断はしない＝expires 内なら実効）。
- **flag OFF 完全不変**: strict 読込は flag ON の startup／署名ヘッダ在の請求時のみ通る。
  flag OFF は registry 非参照（既存 `test_startup_ok_when_flag_off_registry_unreferenced`・
  flag OFF 系 I01 テストが引き続き GREEN＝§4）。
- `load_registry_from_env()`（旧 API）は温存（純関数テスト等の互換）。結線経路は strict のみ。
- 台帳: **61 不変**（行移動 1 件のみ resync: `hub/service_auth.py:438→477`）。

## 4. テスト（対象 suite・全 suite は報告に記載）

```
$ PYTHONUTF8=1 python -m pytest test_rv04b_dual_accept.py test_service_auth.py test_sink_ast_policy.py test_redaction_sentinels.py -q
61 passed, 5 warnings, 197 subtests passed in 15.81s
```
test_rv04b_dual_accept: 15 → **18**（+3 本＝H01 startup／H01 request 503／M01 sentinel・
subTests +20。既存 4 本のうち broken-JSON startup/503 の 2 本は象限テストに包含されるが
回帰固定として温存）。

## 5. 枠消化の日次一行
- 2026-07-15: P1-114-fix1（strict 4象限 fail-fast・固定文言化・sentinel 不含機械確認・台帳 resync 1件）。
  開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
