# 作業記録 2026-07-12: P1-110 S4/AST本格化 + redaction恒久保証テスト（RV-10 PR-5）

- Phase/Gate: Phase 1・RV-10 PR-5（検査基盤・恒久保証テスト）
- 実施: Claude Code（PC-A・endpoint/captured-log は subagent 並列）／レビュー: Codex（R-P1-110・全2巡）
- 結果: **PR #120 マージ済み**（base `a367528` → マージ後 main = `e058ffd810bfd7bd21984427b3c4a478e8589835`）
- 全suite: **1,264 passed / FAIL 0**（`--ignore=test_triage_classification.py`・+334 subtests）
- 本番: Railway auto-deploy = ● Online（deployment `0ed9374f…`）。test/infra のみのため観測は Online 確認のみ。

## 1. task_id / commit

- **P1-110（+fix）／P1-111**。累積2commit: `2b7901b`（PR-5 本体）・`9634cae`（M01/L01 fix）。

## 2. 最終成果

- **台帳 manifest 必須化**: `redaction_sink_allowlist.json` に `manifest`（(file,rule)→理由・31キー）を追加。台帳の各エントリの (file,rule) は manifest 登録が必須（未登録の新規追加は FAIL＝silent な新規 sink debt を防ぐ）。**理由の非空検査**も追加。既存3検査（no_new/stale/monotonic）は維持・baseline_count=211 据置。
- **CLI 配線 AST 順序固定**: 「`__main__` body の最初の実行文が `configure_app_logging()`」を AST で固定（4 CLI の import を module-level へ移送）。
- **sentinel 注入テスト群 7系統**（items 5-11・最終 **+22 テスト**）: 転換 sink:logger 25件の全数 AST 安全パターン固定／dispatch parser 実 parse の氏名・params 非露出／ClaudeUnavailableError の型名のみ回帰／chat_responder 5経路の status 可視+vendor body 抑止／Stripe 署名失敗の 400+固定 detail+生例外非混入／`/ocr`・`/scan` 失敗分岐 status 固定／CLI 起動時 usage INFO+exit1・既存 handler 尊重の subprocess 検証。
- **sentinel 検出ゼロ**＝抑止実装（emit 契約・固定文言）の**実測裏付け**。本 PR で本番コード変更は不要（テスト隔離の setUp 補強のみ）。

## 3. R-P1-110 全2巡

| 巡 | 対象SHA | RESULT | 要点 |
|---|---|---|---|
| 1 | `2b7901b` | CHANGES_REQUIRED | M01（endpoint テストで env 汚染再導入）／L01（manifest 改竄耐性＝理由非空） |
| 2 | `9634cae` | **PASS_WITH_FINDINGS・マージ推奨** | env 完全復元＋復元検証／manifest 理由非空検査で解消 |

## 4. 司令塔裁定

- **L01**: manifest による diff レビュー可能化で目的達成。完全防御は human gate（main マージ＝大野のみ）との**二層**で担保。
- **RP1110-2-L01**（配線順序の実行時保証）＋**復元処理の引数化**（restore を共通ヘルパ化）は **9月候補**。
- **mutation test**（抑止を外すと必ず落ちる保証の機械化）は **9月候補**。

## 5. 運用: Fable 枠内期間の更新

- 公式サポート記事を確認済み。**Fable 枠内期間は 7/19 23:59 PT（7/20 15:59 JST）まで延長**。
- コスト計画を**約1週間後ろ倒し**。重量級（HMAC PoC / InboundEvent / soft merge / App36-37）を**枠内窓に集中投下**する方針。

## 6. 明日の第一手

1. **朝 7:00 dead-man 初回警報の実地検収**（テーブル空のため「空」警報が届く＝正常のブートストラップ）。
2. **PR-4c**: 凍結 print 2件回収（`daily_healthcheck.py:330` / `hub/scheduler.py:99`）。
3. **P1-103**: multipart PoC（HMAC 検証系）。

## 7. 本日サマリ（2026-07-12）

- **マージ4本**: PR #114（P1-104 S2応答body最小化）／PR #116（P1-107a logging配線）／PR #118（P1-107b print全廃）／**PR #120（PR-5 AST本格化）**。
- **台帳 207 → 62**（sink:print 全廃〔凍結2件のみ残〕・sink:logger/httpexception/logger_exception を manifest 管理）。
- **テスト 1,228 → 1,264**（+36: dead-man/allowlist/notification/logging/CLI/redaction sentinel 群）。
- **RV-10 実質完了**（残は凍結 print 2件の PR-4c 回収のみ）。dead-man 監視・emit 契約・print 全廃・恒久保証テストが本番稼働。
