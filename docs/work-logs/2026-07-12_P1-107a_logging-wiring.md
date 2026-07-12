# 作業記録 2026-07-12: P1-107a app ロガーの stdout 出力配線 + 危険 sink/通知の全数抑止（RV-10 PR-4a）

- Phase/Gate: Phase 1・RV-10 PR-4a（PR-4 print 全廃の前提整備）
- 実施: Claude Code（PC-A）／レビュー: Codex（R-P1-107a・全4巡）
- 結果: **PR #116 マージ済み**（base `1f65e98` → マージ後 main = `b075de1f6e78fe343923157f2d50eb89a067f67d`）
- 全suite: **1,239 passed / FAIL 0**（`--ignore=test_triage_classification.py`・+305 subtests）

## 1. task_id / commit 構成

- **P1-107a（+fix〜fix4）／P1-108**
- 累積 **5 commit**（PR #116 = `p1-107a-logging-wiring`）:
  - `72084f4` P1-107a（logging 配線本体）
  - `b5699b0` fix（H01 危険 INFO 抑止・M01 既存 handler 尊重・M02 test env 復元）
  - `2625b2f` fix2（保留6件: 例外本文/documentID 抑止）
  - `59916f0` fix3（H01 record_id→emit・H02 通知本文 str(e) 除去）
  - `bce3842` fix4（H01 throttle 種別化・H02 problems/ClaudeUnavailableError 本文抑止・M01 test 拡張）

## 2. 最終成果

- **app ロガーの stdout 出力配線**（`main._configure_app_logging`・1点集約）: uvicorn 配下で root に stdout handler を付け INFO 以上を timestamp/level/logger名/message で出力。既存 handler があれば付与も level 変更もしない（M01）。サードパーティ（httpx/httpcore/urllib3）の per-request INFO は WARNING へ抑制（洪水回避）。
- **配線で本番可視になる危険 sink の全数抑止**: PII（住所/氏名/被相続人名）・vendor 応答本文（resp.text）・external_ref（documentID/fileKey）・例外本文（str(e)）を emit 契約経由へ。record_id は `emit(record_id)`（値域検証つき）、status echo は enum 検証か固定文言、throttle_key は種別プレフィックスのみ可視（ID 排除）。
- **通知本文（業務 LINE）の例外本文抑止**: notify_admin_line 等の本文と ClaudeUnavailableError message から str(e) を除去し「固定文言＋type(e).__name__＋対象 record No」へ統一。

## 3. R-P1-107a 全4巡の経緯

| 巡 | 対象SHA | RESULT | 要点 |
|---|---|---|---|
| 1 | （未公開） | **BLOCKED** | SHA 未公開のまま依頼＝司令塔の手順ミス。実レビュー不可 |
| 2 | `b5699b0` | CHANGES_REQUIRED | H01 で可視化した値に PII/external_ref 残存・M01（既存 handler の level 上書き）・M02（test env 恒久汚染） |
| 3 | `2625b2f`〜`59916f0` | CHANGES_REQUIRED | throttle_key の ID 混入・problems 集約経路の str(e) 残存 |
| 4 | `bce3842` | **PASS_WITH_FINDINGS・マージ推奨** | 残 findings は PR-4b 持ち越しで合意 |

## 4. 司令塔裁定の記録

- **例外本文＝全 sink 抑止**（log/通知とも）。可視は **type(e).__name__（クラス名）のみ**。
- **throttle_key ＝ 種別プレフィックスのみ可視**（key 内の record_id/user_id 等は出さない）。
- **record_id ＝ `emit(record_id)`（値域検証つき）**へ統一。生の素通し禁止。
- **status echo ＝ enum allowlist 検証済みの正規化値のみ可視**、検証外は固定文言＋抑止。

## 5. 台帳（redaction_sink_allowlist.json）

- **188 → 171（PR 全体で -17）**。内訳（171）: **print 136 / logger 16 / logger_exception 9 / httpexception 10**。
- `baseline_count=211` 上限据え置き。no_new/stale/monotonic PASS。

## 6. テスト

- **1,239 passed / FAIL 0**（基準 1,228 から **+11**: logging wiring 5本 ＋ notification redaction 6本）。

## 7. P1-108 デプロイ確認（READ_ONLY・観測サマリ）

- マージ後 main = `b075de1`。**Railway auto-deploy = ● Online**（新 deployment ID `83447070…`）。
- 本番ログ観測（presence/absence のみ・PII 非転記）:
  - **INFO ログが新 format で出力**され始めている（timestamp/level/logger 形式）＝配線有効。
  - **httpx/httpcore/urllib3 の INFO 洪水なし**（HTTP Request 行 0）＝抑制有効（RP1107A-L01 DEFER 観察の初回＝クリーン）。
  - **起動時エラー・handler 関連の異常なし**（ERROR/Traceback 0・lastResort/no-handler 兆候 0・起動マーカ健全）。
  - ※低トラフィックのため観測行数は少量。継続観察は RP1107A-L01 で。

## 8. DEFER / 持ち越し

- **RCF-M09**: `sortation_ingest` の forwarded[].error（断片化失敗の str(e)）が HTTP 応答/結果 dict に露出。PR-4b 後に回収。
- **koseki_second_opinion:305**: vendor 例外本文が kintone フィールドへ**永続保存**される。**P1-008 調査対象**へ追記。
- **RP1107A-L01**: 第三者 INFO（httpx 等）の本番挙動を継続観察（今回初回はクリーン）。
- **PR-4b 持ち越し（テスト/実装）**: test(a) の `assertTrue(alt_body)` 強化・未知 throttle prefix の sentinel test・`scheduler.py:97-100` 二重出力の回収・**print 置換本体（171→52 想定）**。

## 9. 手順規律の追加（本記録で固定）

- **Codex レビュー依頼は branch の push 完了を確認してから発行する**（R-P1-107a 1巡目の BLOCKED＝SHA 未公開の再発防止）。

## 10. 次の一手

- **PR-4b（P1-107b）**: scheduler 二重出力回収 → print 置換本体（sink:print 136 の emit/logger 化・台帳 171→52 想定）。
