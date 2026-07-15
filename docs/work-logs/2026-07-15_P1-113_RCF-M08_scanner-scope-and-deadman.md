# 作業記録 2026-07-15: P1-113（AST スコープ調整）＋ RCF-M08（dead-man 警報オシレーション恒久修正）

- 票: Phase 1 小粒バッチ PR-B（挙動系小粒の束）
- BASE: origin/main b726171 ／ BRANCH: feat/p1-113-rcfm08

## 1. P1-113: AST/whitelist スキャナのスコープ調整（既存裁定どおり）

### 1.1 裁定と修正
裁定（HOTFIX-01 work-log §7）: 「AST スコープ区別（module-level と関数内 import の扱い）を
統合・整理する」。HOTFIX-01 の真因型＝**関数内の `from hub.redact import emit` は emit を
関数全体でローカル変数化**し、後から「import 位置より前の emit 参照」が入った瞬間に
UnboundLocalError で発火する時限爆弾だが、従来の sink スキャナ（`test_sink_ast_policy.py`
`collect_bindings`）は**関数内の信頼形式 import も module-level と区別せず「信頼」扱い**していた
（既存の専用回帰 `test_hotfix_emit_unbound.py` は main.py＋hub/ のみが対象で、レポ全域は未走査）。

修正: `collect_bindings` に関数スコープ内 import の事前収集を追加し、**信頼形式は
「module-level の alias 無し `from hub.redact import emit`」のみ**に限定。関数内の同形 import は
`emit_shadow` として記録・poison（ファイル内の全 emit 呼び出しを不信頼化）。スキャナの対象は
従来どおりレポ全域（git 管理下の非テスト .py・legacy/alembic 除外）のため、HOTFIX-01 型の
時限爆弾が**全ファイルで恒久検知**される（専用回帰テストは従来どおり併存）。

### 1.2 修正前実測（旧スキャナ＝b726171 の scan_source に新 fixture を適用・実出力）
```
expected rules per case: ['emit_shadow', 'sink:logger']
nested_trusted_import_in_function: old=['(違反なし=素通し)'] -> FAIL(旧=素通し) / new=['emit_shadow', 'sink:logger']
nested_import_with_module_level_trusted: old=['(違反なし=素通し)'] -> FAIL(旧=素通し) / new=['emit_shadow', 'sink:logger']
nested_trusted_import_async_function: old=['(違反なし=素通し)'] -> FAIL(旧=素通し) / new=['emit_shadow', 'sink:logger']
```
（同 3 fixture は `TestScannerDetection.VIOLATION_CASES` へ恒久追加。旧スキャナでは
`test_violation_patterns` が FAIL する形。）

### 1.3 受入条件との対応
- **emit() 契約・shadow-ban 規律に非抵触**: 信頼形式の定義を狭める方向のみ（合格していた
  安全パターンは全て合格のまま＝SAFE_CASES 不変・全 pass）。レポ実走査でも新規違反ゼロ
  （現行コードに関数内 redact.emit import は存在しない）＝**台帳 61 件不変**。
- **scanner 自体のテスト GREEN**: §3 実出力。

## 2. RCF-M08: dead-man 警報オシレーションの恒久修正

### 2.1 根因
P1-102 の dead-man（監視項目F）は「業務通知の最終成功時刻（heartbeat）が 25h 超なら警報」の
**受動監視**。低トラフィック時は業務通知が healthcheck の警報自身しかないため、
**警報の送信成功がその heartbeat を更新する自己参照ループ**になる:
Day1 7:00 stale→警報（送信成功=heartbeat 更新）→ Day2 7:00 24h<25h で OK（何も送らない）→
Day3 7:00 48h>25h でまた警報 —— **約2日周期の偽警報オシレーション**
（P1-104 §5 で「観察後に再裁定」とされた事項の恒久化）。警報文面は「経路が死んでいる可能性」
だが、警報が届いた事実自体がそれを否定する＝実障害ゼロでも隔日で誤警報が届く。

### 2.2 恒久修正（受動監視 → stale 時のみ能動 probe）
stale 検知時に即警報せず、同一チャネルへ **synthetic heartbeat を1通実送して死活を実測**する
（`_send_heartbeat_probe`・応答不要の定型文・throttle なし）:
- **送信成功** = チャネル生存の実証。notify 層（hub/notify.py）が heartbeat を更新するため
  次回は鮮度 OK。**警報は出さない**（偽警報の根絶）。「警報が届く=チャネル生存の証明」の
  性質は probe 自体が引き継ぐ（届く=生存・届かない=下記）。
- **送信失敗/例外** = 実死のみ dead-man 警報（`…無音かつ死活確認送信に失敗（dead-man）…`）。
  run_healthcheck の警報送信も同経路で失敗するため、既存の `_deadman_alt_alert`＋error ログ
  （Railway 監視）へも従来どおり落ちる。

制約遵守: S1 通知経路（notify_admin_line/notify_business・fail-closed・allowlist）は不変。
synthetic heartbeat 機能（P1-102）は「送信成功が heartbeat を記録する」仕組みごと不変
（probe はその上に乗るだけ）。閾値 25h（BUSINESS_NOTIFY_STALE_HOURS）も不変。
低トラフィック定常では約2日毎に「定期死活確認」1通が届く（従来の偽警報と同頻度・
正しいラベル）。頻度調整は env で可能。

### 2.3 修正前 FAIL 実測（実出力・旧コード＝daily_healthcheck.py を stash して測定）
```
$ PYTHONUTF8=1 python -m pytest test_s1_failclosed.py::TestDeadmanLiveness -v -p no:warnings
test_s1_failclosed.py::TestDeadmanLiveness::test_dispatchbot_unset_is_reported PASSED [ 12%]
test_s1_failclosed.py::TestDeadmanLiveness::test_empty_table_is_abnormal PASSED [ 25%]
test_s1_failclosed.py::TestDeadmanLiveness::test_fresh_heartbeat_ok PASSED [ 37%]
test_s1_failclosed.py::TestDeadmanLiveness::test_no_database_url_skips PASSED [ 50%]
test_s1_failclosed.py::TestDeadmanLiveness::test_stale_heartbeat_probe_exception_reports_deadman PASSED [ 62%]
test_s1_failclosed.py::TestDeadmanLiveness::test_stale_heartbeat_probe_failure_reports_deadman PASSED [ 75%]
test_s1_failclosed.py::TestDeadmanLiveness::test_stale_heartbeat_probe_success_no_alarm FAILED [ 87%]
test_s1_failclosed.py::TestDeadmanLiveness::test_table_missing_skips PASSED [100%]
E       AssertionError: Lists differ: ['業務通知経路が約30時間無音（dead-man）: DISPATCHBOTチャネルの死活を確認してください'] != []
test_s1_failclosed.py:268: AssertionError
========================= 1 failed, 7 passed in 4.50s =========================
```
**旧コードがオシレーションの実体（チャネル生存でも偽 dead-man 警報を返す）を実測**。
修正後は同 suite 全 pass。
（補足: probe 失敗/例外→警報の 2 テストは旧コードでも「警報が出る」ため pass する。
旧挙動を弁別するのは probe 成功→警報なしの 1 本。）

### 2.4 受入条件との対応
- 根因の説明: §2.1（自己参照ループ）／修正のテスト実証: §2.3＋probe 失敗系 2 本＋
  fresh 時 probe 不送 assert。
- 通知経路の既存テスト全 GREEN: §3（test_s1_failclosed 全体・test_notification_redaction・
  test_pr4c_print_removal を含む）。

## 3. テスト（対象 suite・全 suite は COMPLETION_REPORT に記載）

```
$ PYTHONUTF8=1 python -m pytest test_s1_failclosed.py test_sink_ast_policy.py \
    test_redaction_sentinels.py test_notification_redaction.py test_pr4c_print_removal.py -q
54 passed, 98 subtests passed in 17.50s
```
- test_s1_failclosed: +2（probe 成功/例外系。stale 系 1 本は probe 契約へ改修）。
- test_sink_ast_policy: fixture +3（P1-113）。
- sink 台帳: **total 61 不変**（行移動 2 件のみ resync: daily_healthcheck.py 312→338・331→357）・
  baseline 211 単調減少維持・manifest 不変・新規違反ゼロ・sink:print ゼロ維持。

## 4. 枠消化の日次一行
- 2026-07-15: P1-113（関数内 redact.emit import の emit_shadow 化・fixture 3本）＋
  RCF-M08（stale 時 synthetic probe・偽警報オシレーション根絶・台帳 resync 2件）。
  開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
