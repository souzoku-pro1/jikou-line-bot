# 作業記録 2026-07-15: P1-113-fix1（R-P1-113-M08 所見対応・信頼 import の top-level 厳密化）

- 票: P1-113-fix1（PR #134 への追補）／BRANCH: feat/p1-113-rcfm08（remote 743a53d = local c449639 から継続）
- 規律遵守: commit→suite→報告固定・修正前 FAIL（新旧比較）実出力を本 .md へ保存・台帳 61 不変

## 1. RP1113-H01: 信頼 import のスコープを module top-level（tree.body 直下）へ厳密化

### 1.1 修正
P1-113（PR #134 初版）は関数/async 関数配下のみを除外していたが、**class body・if/try/with 等
その他ネスト配下**の `from hub.redact import emit` は依然「信頼」扱いだった。fix1 で信頼判定を
**`tree.body` の直接子である ImportFrom（alias 無し）のみ**に限定し、ネスト配下は種別を問わず
すべて `emit_shadow` として poison（関数配下＝HOTFIX-01 型 UnboundLocalError 時限爆弾・
その他ネスト＝条件付き束縛/別名前空間で信頼が静的に確定しない、の統一扱い）。
「直下」は tree.body の**メンバーであること**であり先頭位置は要求しない（SAFE fixture
`emit_import_top_level_not_first` で固定）。

### 1.2 検知 fixture（3本追加）と新旧比較実測（実出力・旧=現行 743a53d の scan_source）
```
expected rules per case: ['emit_shadow', 'sink:logger']
nested_trusted_import_class_body: old=['(違反なし=素通し)'] -> FAIL(旧=素通し) / new=['emit_shadow', 'sink:logger']
nested_trusted_import_if_block: old=['(違反なし=素通し)'] -> FAIL(旧=素通し) / new=['emit_shadow', 'sink:logger']
nested_trusted_import_try_block: old=['(違反なし=素通し)'] -> FAIL(旧=素通し) / new=['emit_shadow', 'sink:logger']
```
（class body／if 配下／try 配下の 3 本とも**旧コードは素通し**・新コードは検出。fixture は
`TestScannerDetection.VIOLATION_CASES` へ恒久追加＝旧コードでは `test_violation_patterns` が
FAIL する形。）

### 1.3 レポ全域再走査
`scan_repo()` 実行: **解析エラー 0・スキャナ規則の新規違反 0（偽陽性なし）**。
検出差分は RCFM08-M01 のコメント追記による daily_healthcheck.py の**行移動 2 件のみ**
（343/362 へ resync・**台帳 61 不変**）。

## 2. RCFM08-M01: 「チャネル生存」の定義明記（docs＋コードコメント）

**定義（固定）**: probe 成功＝**LINE Push API の 2xx 受理**であり、証明されるのは
**token・宛先・通信経路の生存**まで。**管理者端末での実表示・端末の通知設定・Bot の
ブロック有無は保証しない**。dead-man が検知するのは「送信経路の死」であって
「弁護士に見えていること」ではない（過大主張しない）。

- コードコメント: `daily_healthcheck.py` の probe 判定分岐＋`_send_heartbeat_probe`
  docstring に同旨を固定。
- 本 work-log（および PR #134 の P1-113/RCF-M08 work-log の「届く=生存の証明」記述は
  本定義で読み替える＝上書き注記）。

## 3. テスト（対象 suite・全 suite は報告に記載）

```
$ PYTHONUTF8=1 python -m pytest test_sink_ast_policy.py test_redaction_sentinels.py \
    test_s1_failclosed.py test_hotfix_emit_unbound.py -q
48 passed, 4 warnings, 96 subtests passed in 9.44s
```
- fixture: VIOLATION_CASES +3（class/if/try）・SAFE_CASES +1（top-level 非先頭）＝subTests +4。
- 台帳: **61 不変**（行移動 2 件 resync のみ）・baseline 211 単調減少維持・新規違反ゼロ。

## 4. 参考（PC-A 対応不要・転記）
- RCFM08-M02 は **RCF-M11** として司令塔台帳へ DEFER 登録（マージ非阻害・flag/点火裁定の考慮事項）。

## 5. 枠消化の日次一行
- 2026-07-15: P1-113-fix1（tree.body 直下限定・fixture 3+1・全域走査クリーン・M01 定義固定）。
  開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
