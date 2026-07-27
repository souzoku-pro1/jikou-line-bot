# DRAFT: LINE-LOG-1 実行手順書（準備票成果物・実行前 Codex レビュー対象・fix1）

- TASK_ID: LINE-LOG-1 準備票（相1 並行・実ログ非接触）／記録日 2026-07-27（fix1 反映）
- 正本: DRAFT_LINE_QUALITY_IMPROVEMENT §4.1（allowlist 固定仕様・保管規定・標本設計）・
  §4.4 G1/G2。**本手順書と変換仕様（tools/line_log_anonymize.py）は実行前に
  Codex レビューを通すこと**（G1 の前提・PII 漏れの第三者確認）。
- 状態: **DRAFT**（実行フェーズは G1/G2 通過後のみ。本票では合成データ試験まで）。

## 1. 前提と絶対条件（§4.1）

- 実行者は**[人]（大野）のみ**。PC-A は raw に接触しない。
- raw export は**[人]端末ローカルのみ**（クラウド同期禁止・転送しない・
  ディスク暗号化必須）。**保持は export 時点から絶対上限7日**（変換成否無関係）。
- 匿名化成果物は allowlist フィールドのみ・司令塔管理領域へ（repo コミット禁止）。

## 2. 実行手順（[人]・番号順・**一方向工程**〔fix1 H04〕）

工程は「convert（中間）→[人]手修正→reverify（全件再検査）→PASS→引渡し」の
一方向のみ。**手修正後のファイルを reverify を経ずに引き渡すことは禁止**。

1. **G1 確認**: 本手順書・変換仕様の Codex レビュー PASS＋§7 の STOP 項目
   全確定を確認（未充足なら中止）。
2. **export**: App28 CSV（範囲=直近90日・列: 作成日時/line_user_id/role/message/
   category/auto_sent）＋App29 CSV（列: line_user_id/AI下書き/送信済み・
   §7-1 確定後は対応条件に必要な列を追加）。export 日時を work-log に記録。
3. **convert（中間生成）**:
   `python tools/line_log_anonymize.py convert --app28-csv <28.csv>
   --app29-csv <29.csv> --out mid.json --summary-out summary1.json`
   — corpus 本体（threads のみ）と summary（運用メタ・**別ファイル**=fix1 H02）
   を出力。**checklist はこの段階では生成されない**。
4. **fail-closed 除外の処理**: summary1 の除外理由を確認。
   - `text_over_400`＝要旨化が必要: mid.json 上で該当発話を要旨化（§4.1 相談本文
     契約: 必要最小限・固有事実は要旨・原文全文禁止）。
   - `residue_detected`／`role_out_of_enum`／`category_out_of_enum`＝raw 側の
     データ問題: 原因を確認し、解消できない場合は**除外のまま確定**。
5. **[人]手修正（mid.json 上）**: 氏名・屋号・地名等（パターンで拾えない PII）の
   要旨化/トークン化。必要に応じ meta.amount_band/date_band を付与（粒度[人]裁定）。
6. **fallback 補正 ID の確定（fix1 H03・構造化入力）**: 障害記録（work-log の INC・
   Railway 障害時間帯）と突合し、層(c) に該当する thread_id を JSON list
   （例: `["C012","C031"]`）として fb.json に作成。**判定不能は入れない**
   （合成で充足）。突合根拠を work-log に記録。
7. **reverify（全件再検査・fix1 H04）**:
   `python tools/line_log_anonymize.py reverify --in mid.json --fallback-ids fb.json
   --out final.json --summary-out summary2.json --checklist-out checklist.csv`
   — schema／400字／enum（role/category/delivery/layer）／残存パターン／
   allowlist 外フィールドを**全件再検査**。**FAIL（非0終了）の間は checklist・
   最終成果物は生成されない＝引渡し不可**。修正して PASS まで繰り返す。
8. **標本充足の確認（fix1 M02/H03）**: summary2 を確認 —
   - `main_shortfall` に**正の値がある場合は G2 停止**（不足の扱い＝抽出期間延長か
     不足受容かを司令塔裁定へ）。
   - `rare_counts`／`synthetic_needed` は数値で確認（不足層は lineq 合成スレッドで
     充足・実例数と合成数は別集計のまま記録）。
9. **目視チェックリスト**: checklist.csv にスレッドごとの残存 PII 無し確認を
   記入・署名（G2 の前提）。
10. **引渡し**: final.json＋summary2.json＋checklist.csv を司令塔管理領域へ
    （司令塔指定経路のみ）。
11. **raw 削除**: App28/29 CSV・mid.json 等の中間ファイルを削除し、削除日時を
    work-log に記録（期限=export から7日以内・未完でも削除して再 export）。

## 3. delivery 判定の固定規則（fix1 M01: **正規化後全文一致**が一次判定）

実ログの定型は Bot 送信文そのものであるため、空白・改行を正規化した**全文一致**で
判定する（部分一致・特徴句 marker 方式は**廃止**＝定型句を含む通常回答を誤判定
しない）: (i) PENDING_REPLY と全文一致 → `demoted` (ii) 即時定型5種のいずれかと
全文一致 → `immediate` (iii) App29 の送信済み AI下書き（正規化）と全文一致 →
`approved` (iv) それ以外 → `auto`。定型全文はスクリプトへ複製で埋め込み、
本番定数との drift・正規化後の一意性は同期テストが検知する。

## 4. 希少層 (c)（送信失敗 fallback）の判定規則（fix1 H03）

- delivery からは (c) と通常降格を**機械判別できない**（どちらも PENDING_REPLY）。
- **固定規則**: [人]が障害記録と突合した thread_id 一覧を**構造化補正ファイル**
  （fb.json）として reverify へ渡す。変換器は**優先順位 a>b>c を再適用**
  （rare:silent／rare:immediate は不変・それ以外を rare:fallback へ）し、
  summary（rare_counts／main_counts／main_shortfall／synthetic_needed）を
  **再計算**する（demoted との二重計上なし・対照テストで固定）。
  **判定不能なスレッドは (c) に数えない**（合成ケースで充足）。

## 5. 希少層の重複計数規則（固定案・受容条件1の充足）

1 スレッドが複数の希少層条件に該当する場合:
- **優先順位 (a) 人対応無言 ＞ (b) 危機/裁判所即時通知 ＞ (c) 送信失敗 fallback で
  単一層にのみ割当てる**（機械判定は (a)(b)・(c) は §4 の補正入力。変換器の
  再適用ロジックがこの優先順位を強制する）。
- **二重計数は禁止**: 割当てられなかった層の最低件数（各3件）へは算入しない。
  不足分は合成ケースで充足し、**実例数と合成代替数は別集計**（summary の
  rare_counts／synthetic_needed で機械算出）。

## 6. 出力仕様（allowlist・§4.1 の固定仕様表に完全準拠）

- final.json（corpus 本体）: `{"threads": [{thread_id, layer, turns:[{role, text,
  (assistant のみ) category, delivery}]}]}` — **allowlist フィールドのみ**。
- summary2.json（**別ファイル**・運用メタデータ〔fix1 H02(ii)〕）: 件数・層分布・
  不足数・除外理由のみ（本文・ID 系を含めない）。
- checklist.csv: reverify PASS 時のみ生成（H04）。

## 7. 実行前レビュー必須の STOP 項目（**全確定まで G1 通過不可**）

1. **App28↔App29 の対応条件の確定（fix1 M03・必須）**: App29 レコードと App28
   assistant 行の対応規則が未確定のまま実行してはならない。
   **固定案（提案・[人]実機確認と分離）**: 「App29.line_user_id が App28 の
   line_user_id と完全一致」＋「AI下書き（正規化）と App28.message（正規化）の
   全文一致」＋「App28 行の作成日時が App29 レコード更新日時から **±24h の
   時間窓**内」の3条件 AND。時間窓と App29 export への更新日時列の追加は
   **[人]実機確認事項**（kintone export 列の実在確認を含む）。
2. App29 突合の誤判定リスク（承認時に先生が下書きを修正した場合、送信文と
   AI下書き が不一致→ auto 誤判定）: 許容可否と対応（前方一致/類似度への改訂
   要否）を Codex レビューで判定。
3. meta.amount_band/date_band の丸め粒度（[人]裁定・§4.1）。
