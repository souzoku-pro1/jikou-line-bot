# DRAFT: LINE-LOG-1 実行手順書（準備票成果物・実行前 Codex レビュー対象)

- TASK_ID: LINE-LOG-1 準備票（相1 並行・実ログ非接触）／記録日 2026-07-27
- 正本: DRAFT_LINE_QUALITY_IMPROVEMENT §4.1（allowlist 固定仕様・保管規定・標本設計）・
  §4.4 G1/G2。**本手順書と変換仕様（tools/line_log_anonymize.py）は実行前に
  Codex レビューを通すこと**（G1 の前提・PII 漏れの第三者確認）。
- 状態: **DRAFT**（実行フェーズは G1/G2 通過後のみ。本票では合成データ試験まで）。

## 1. 前提と絶対条件（§4.1）

- 実行者は**[人]（大野）のみ**。PC-A は raw に接触しない。
- raw export は**[人]端末ローカルのみ**（クラウド同期フォルダ禁止・転送しない・
  ディスク暗号化必須）。**保持は export 時点から絶対上限7日**（変換成否無関係）。
- 出力（匿名化コーパス）は allowlist フィールドのみ・司令塔管理領域へ保管
  （repo コミット禁止）。

## 2. 実行手順（[人]・番号順）

1. **G1 確認**: 本手順書・変換仕様の Codex レビュー PASS を確認（未 PASS なら中止）。
2. **export**: kintone App28（チャットログ）を CSV export（範囲=直近90日・
   列: 作成日時/line_user_id/role/message/category/auto_sent）。
   App29（承認キュー）も CSV export（列: line_user_id/AI下書き/送信済み・
   delivery 判定の突合用）。export 日時を work-log に記録（削除期限の起点）。
3. **変換実行**（ローカル）:
   `python tools/line_log_anonymize.py --app28-csv <28.csv> --app29-csv <29.csv>
   --out corpus.json --checklist-out checklist.csv`
4. **fail-closed 除外の処理**: summary の除外理由を確認。
   - `text_over_400`＝**要旨化が必要**: raw の写し上で該当発話を要旨化
     （§4.1 相談本文契約: 必要最小限・固有事実は要旨・原文全文禁止）→ 再実行。
   - `residue_detected`＝パターン残存: 該当箇所を手動でトークン置換 → 再実行。
   - 再実行しても解消しないスレッドは**除外のまま確定**（無理に含めない）。
5. **氏名等の非パターン PII の目視除去**: corpus.json の全スレッドを目視し、
   氏名・屋号・地名等（パターン検査で拾えない PII）を要旨化/トークン化。
6. **層の補正（(c) fallback 判定・§6）** と **希少層の充足確認**（summary の
   synthetic_needed>0 の層は合成ケースで充足＝lineq/synthetic_threads が該当層を
   各3本保有済み）。
7. **目視チェックリスト**: checklist.csv にスレッドごとの残存 PII 無し確認を
   記入・署名（G2 の前提）。
8. **引渡し**: corpus.json＋checklist.csv を司令塔管理領域へ（司令塔指定経路のみ）。
9. **raw 削除**: App28/29 CSV と中間ファイルを削除し、削除日時を work-log に記録
   （期限=export から7日以内・変換未完でも削除して再 export からやり直す）。

## 3. delivery 判定の固定規則（変換仕様 §5）

assistant 発話の text により機械判定する:
(i) PENDING_REPLY 定型と完全一致 → `demoted` (ii) 即時定型（crisis_support／
urgent_seizure_panic／court_doc_request）の固有句を含む → `immediate`
(iii) App29 の**送信済み** AI下書き と完全一致 → `approved` (iv) それ以外 → `auto`。
定型文言はスクリプトに複製で埋め込み、本番定数との drift は同期テストが検知する。

## 4. 希少層 (c)（送信失敗 fallback）の判定規則

- delivery からは (c) と通常降格を**機械判別できない**（どちらも PENDING_REPLY）。
- **固定規則**: [人] が障害記録（work-log の INC 記録・Railway 障害時間帯）と突合し、
  **障害時間帯内の PENDING_REPLY 縮退スレッドのみ**を (c) に指定する
  （corpus.json の該当 thread の layer を `rare:fallback` へ手動補正・補正内容を
  work-log に記録）。**判定不能なスレッドは (c) に数えない**（合成ケースで充足）。

## 5. 希少層の重複計数規則（固定案・受容条件1の充足）

1 スレッドが複数の希少層条件に該当する場合（例: 危機通知の後に人対応モードで無言化）:
- **優先順位 (a) 人対応無言 ＞ (b) 危機/裁判所即時通知 ＞ (c) 送信失敗 fallback で
  単一層にのみ割当てる**（機械判定は (a)(b) まで・(c) は §4 の[人]補正）。
- **二重計数は禁止**: 割当てられなかった層の最低件数（各3件）へは算入しない。
  不足分は合成ケースで充足し、**実例数と合成代替数は別集計**（§4.1・summary の
  synthetic_needed で機械算出）。

## 6. 出力仕様（allowlist・§4.1 の固定仕様表に完全準拠）

- corpus.json: `{"threads": [{thread_id, layer, turns:[{role, text,
  (assistant のみ) category, delivery}]}], "summary": {件数のみ}}`。
  表に無いフィールドは出力しない（meta.amount_band/date_band は要旨化工程で
  [人]が必要と判断した場合のみ付与・粒度は[人]裁定）。
- summary は件数・層分布・除外理由のみ（本文・ID 系を含めない）。

## 7. 未確定事項（実行前に確定させる・Codex レビュー観点）

- (i) App29 CSV の突合精度（承認時に先生が下書きを修正した場合、送信文と AI下書き が
  不一致→ auto に誤判定される）: 誤判定率の見込みと許容可否は Codex レビューで判定。
  必要なら「App29 突合は前方一致/類似度」へ改訂（本手順書の改訂＝再レビュー）。
- (ii) meta.amount_band/date_band の丸め粒度（[人]裁定・§4.1）。
