# 作業記録 2026-07-12: P1-107b print 全廃本体 + CLI logging 配線（RV-10 PR-4b）

- Phase/Gate: Phase 1・RV-10 PR-4b（print 全廃・台帳消化）
- 実施: Claude Code（PC-A・機械置換は subagent 6並列）／レビュー: Codex（R-P1-107b・全2巡）
- 結果: **PR #118 マージ済み**（base `172ddef` → マージ後 main = `fc369c83e2b38975e8a688d876817a083da9e3ad`）
- 全suite: **1,242 passed / FAIL 0**（`--ignore=test_triage_classification.py`・+309 subtests）

## 1. task_id / commit

- **P1-107b（+fix）／P1-109**。累積2commit: `a7b6119`（print 移送本体）・`b22b31d`（CLI logging 配線）。

## 2. 最終成果

- **sink:print 134 件を emit/logger へ移送**（daily_healthcheck.py・hub/scheduler.py を除く27ファイル）＋各ファイルの定数 print。INFO 配線（P1-107a）済みで Railway 可視性維持。挙動・戻り値・HTTP ステータス不変（出力面のみ）。
- **CLI logging 配線の共有化**: `_configure_app_logging()` を `hub/logging_setup.py::configure_app_logging()` へ純移設（挙動差ゼロ）。main.py は alias で module-level 呼び出し。print 移送済みで `__main__` を持つ standalone CLI 全4件（registry_to_kintone / import_city_master / make_zaisan_mokuroku_template / channels.shokumu_seikyu）の `__main__` 冒頭で呼び出し、standalone 起動時も app INFO が stdout へ出るようにした。
- **凍結2ファイル維持**: daily_healthcheck.py（`[HEALTHCHECK] OK` print）・hub/scheduler.py（scheduler 登録 print・97-100 の二重出力）は未変更（PR-4c で回収）。

## 3. R-P1-107b 全2巡

| 巡 | 対象SHA | RESULT | 要点 |
|---|---|---|---|
| 1 | `a7b6119` | CHANGES_REQUIRED | H01（standalone CLI が INFO 配線を持たず起動時 app INFO が消失）／M01（政策テストの差分ギャップ→PR-5 編入）／L01 |
| 2 | `b22b31d` | **PASS_WITH_FINDINGS・マージ推奨** | CLI 配線共有化で H01 解消。残 findings は PR-5 持ち越しで合意 |

## 4. 司令塔裁定

- **保留リストは全件「抑止側」で確定**。緩和候補（都道府県名＝公開マスタ・pair_key の数値部）は**記録のみ**（本 PR では抑止のまま・将来必要なら別票で可視化裁定）。
- **例外 print→logger の新規 sink:logger 25 件＝`type(e).__name__`（型名）可視の受容債務**（fix2 裁定準拠）。全25件を検査し PII/status の生値混入なしを確認済み。

## 5. 台帳（redaction_sink_allowlist.json）

- **171 → 62**。内訳（62）: **sink:print 2（凍結: daily_healthcheck/scheduler）** / sink:logger 41（既存16＋型名25）/ logger_exception 9 / sink:httpexception 10。
- no_new/stale/monotonic PASS。baseline_count=211 据え置き。

## 6. テスト

- **1,242 passed / FAIL 0**（1,239 基準から **+3**: dead-man 代替通知 assertTrue 強化・未知 throttle prefix 検証・CLI logging 2本）。
- 仕様変更に伴う期待値更新1件: parser の parsed ログが PII を emit 抑止・raw confidence を drop するようになったため、生値ログ存在検査を redaction 検査へ更新（緩和ではない）。

## 7. P1-109 デプロイ確認（READ_ONLY・観測サマリ）

- マージ後 main = `fc369c8`（PR #118）。**Railway auto-deploy = ● Online**（新 deployment ID `36d7add9…`）。
- 本番ログ観測（presence/absence のみ・PII 非転記）:
  - 起動健全（起動マーカ 7・INFO 新 format 2・**ERROR/Traceback/handler 異常 0**）。
  - **httpx/httpcore/urllib3 の INFO 洪水なし**（0）。
  - 移送後の運用タグ（`[SORTATION]` 等）は**観測対象なし**（アプリ低トラフィック・観測窓に webhook 活動なし）。観測窓に出た `[HEALTHCHECK]`/`[RETURN_DEADLINE]` は**凍結スケジューラ登録 print**（PR-4c 対象・期待どおり print 経由）。

## 8. PR-5 編入リスト（累積・テスト増強）

- BASE/TARGET 差分 manifest 検査（移送の網羅性固定）。
- sentinel 注入系: 全 logger.info への PII sentinel・notify 本文・dispatch parser 実 parse。
- ClaudeUnavailableError の型分岐回帰。
- AST 呼び出し順序固定（configure_app_logging が __main__ 冒頭で最初に呼ばれること）。
- CLI usage/handler 尊重の subprocess 検証（引数付き CLI・既存 handler 尊重の実起動）。

## 9. PR-4c 予定

- 凍結 print の回収: `daily_healthcheck.py:330`（`[HEALTHCHECK] OK`）・`hub/scheduler.py:99`（scheduler 登録・二重出力）。**明朝 dead-man 実地検収後**に実施（観測対象を凍結する必要があるため）。

## 10. 実装メモ

- 機械置換は subagent 6並列で実施し、AST 3検査＋全 suite で中央検証（passthrough kind の値域検証が誤 kind の自己防御となる設計）。
