# 作業記録 2026-07-11: Phase 0B — CloudSign封じ込め完了

- Phase/Gate: **Phase 0B・CloudSign封じ込め完了**
- 記録日: 2026-07-11 ／ 実施: Claude Code（PC-A）／ タスク: P0B-001〜005
- 出典: 各タスクCOMPLETION_REPORT・Codexレビュー・git/PR実出力（推測補完なし）

## 1. 成果: R0A-B03（CloudSign fail-open）= **FIXED（本番反映済み・2026-07-11）**

- **PR #97 マージ済み**（マージコミット `b6e9ec7`）・Railwayデプロイ成功・サービスオンライン確認済み
- 対象SHA: `69c243c`（base `2822f898`・変更3ファイル: cloudsign_webhook.py／
  test_cloudsign_fail_closed.py〔新規〕／test_cloudsign_webhook.py）
- 修正内容の要約:
  1. 照合失敗（API例外・非dict・id欠落/不一致・status欠落/不一致）は全てfail-closed —
     受任遷移0・顧客チャネル通知0・業務指示Botチャネル警報のみ（顧客Botへfallbackしない）
  2. 照合成功でもkintone一致レコードなしは `state=kintone_update_failed` で要人手確認警報
     （「締結完了」通知を出さない）
  3. 失敗分類は閉集合の固定文字列（vendor生値・PII・token・生レスポンスをログに埋め込まない）
  4. 正常系（照合成功＋レコードあり→受任＋通知＋`state=processed`・200）は挙動不変
- テスト: 全suite **1,072 passed・FAIL 0**（baseline 1,059＋新規。既存の削除/skip/緩和なし）

## 2. レビュー経緯（Codex R-CLOUDSIGN-FAILCLOSED）

| rev | SHA | 判定 |
|---|---|---|
| rev1 | `1d7b299` | CHANGES_REQUIRED（H01: id欠落が照合成功に倒れる偽陽性 ほか M03/M04/M05 の4点） |
| rev2 | `69c243c` | **PASS_WITH_FINDINGS（4点解消・デプロイ可）** |

## 3. Phase 1へDEFERした残findings（裁定によるDEFER・見落としではない）

- **RCF-M01**: 成功再送のidempotency / event journal（RV-05/07と統合して設計）
- **RCF-M02**: 業務LINE警報の非2xx検知（送達確認）
- **RCF-L01**: document IDのlog台帳化（retention裁定 O-06/O-32 とセット）
- **RCF-L02**: path secret → header HMAC化（RV-04と統合）

## 4. デプロイ後の監視事項

- CloudSign書類詳細APIの `status`/`id` キー体系は**実機未確認**（sandbox未検証）。
  初回の締結eventで Railwayログの `state` が
  `processed`（想定どおり）／`verification_failed`／`kintone_update_failed` の
  どれになるかを必ず確認する。想定と異なる場合も危険側（受任自動遷移）へは進まず、
  安全側停止＋業務チャネル警報になる設計
- sandbox確認できたら、匿名化した実レスポンスfixtureを **Phase 1 の contract test** に固定する

## 5. Phase 0B 残・未着手の封じ込め候補（次タスク）

1. **/scan・/ocr/fixed-asset の無認証**（RV-01/02）。
   ※/scanのサーバ側停止はGAS既存3フォルダループ（try/catchなし）を道連れにするため、
   止める場合はGAS側操作が唯一安全（P0B-001調査済み）
2. **notify fallback**（H04・NEEDS_HUMAN: 7/7裁定「警報欠落防止優先」vs fail-closed方針の衝突。
   大野裁定待ち）
3. **person_merge物理削除**（B05・NEEDS_HUMAN: `PERSON_MERGE_ENABLED` の本番現在値が未確認。
   Phase 0B完了までの明示OFF裁定を推奨）

## 6. 現在のbaseline（main反映後・実測）

- main = `b6e9ec7`（PR #97マージコミット）にて:
  `python -m pytest --ignore=test_triage_classification.py -q`
  → **1072 passed, 3 warnings, 231 subtests passed**（FAIL 0・skip 0）
- 除外は従来どおり test_triage_classification.py（実Claude API・実kintone到達のため。
  conftest.py:8-16。railway run での実測は別途）
