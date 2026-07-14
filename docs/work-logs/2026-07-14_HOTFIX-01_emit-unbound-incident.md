# インシデント記録 2026-07-14: HOTFIX-01 — _process_line_event 全停止（UnboundLocalError: emit）

- 分類: Sev — 顧客 Bot の中核経路（LINE 返信）全停止 / **顧客影響ゼロ（棚卸し済み）**
- 実施: Claude Code（PC-A）／事後監査: Codex（R-HOTFIX-01・**PASS**）
- 結果: **PR #125 マージ済み**（merge commit `16cf4bc`）・本番反映・LINE 返信復旧を実測確認
- テスト baseline: **1,290 passed / FAIL 0 / skip 0**（main `16cf4bc`）

## 1. 事象

`main.py` の `_process_line_event`（LINE イベントの重い処理＝返信生成の background task）が、先頭ログ（`[PROCESS] start`・383 行相当）の `emit(...)` 参照で **`UnboundLocalError: cannot access local variable 'emit'`** を毎回送出。webhook は 200 を返す一方、背景タスクが先頭で全滅し、**到達した全 LINE メッセージの処理・返信が沈黙裏に失敗**していた。

## 2. 障害ウィンドウ

**2026-07-13 02:06 JST 〜 2026-07-14 09:50 JST（約 31.7 時間）**。
- 開始 = PR #118（merge `fc369c8`・2026-07-13 02:06:15 JST）の自動デプロイ。
- 終了 = HOTFIX-01 デプロイ（deployment `66b97366`・createdAt 2026-07-14 09:49:26 JST／起動 09:50 JST）。
- 中間の再デプロイ（PR #120/#122/#124）も `main.py:421` 不変のため障害は連続。
- 実測データ点: `UnboundLocalError` を 2026-07-13 18:55 JST に確認（アプリログは UTC 表記＝09:55:02Z）。旧デプロイのログは世代交代で失効（`Deployment not found`）のため初出/最終の全量は事後実測不能。
- **顧客影響: ゼロ（棚卸し済み）**。当該ウィンドウ中に返信喪失で実害となる顧客案件は無かったことを確認。

## 3. 真因（単純 blame では誤認する構造）

- **混入 = `eb40672`（P1-102・2026-07-12）**: `_process_line_event` 内の `if ATTORNEY_LINE_USER_ID:` 分岐に関数ローカル `from hub.redact import emit` を追加。当時 `emit` 使用は **import より後のみ**で実行時に束縛済み → **無害**。
- **発火 = `a7b61192`（P1-107b「print 全廃本体」・2026-07-13 01:27 JST）**: 先頭ログ（383/384 行）と 410 行を `emit(...)` 化し、**import より前に `emit` を使う経路**を初めて作った。Python は関数内 import により `emit` を関数全体でローカル変数扱いにするため、先頭ログの参照が未束縛でクラッシュ。
- 教訓: 「関数内 import は、その名前を関数全体でローカル化する」。後から**その名前を import 位置より前で使う変更**が入った瞬間に発火する時限式。単独の blame（421 行）では P1-102 を誤って主犯と見なすため、**使用順**まで追う必要がある。

## 4. 検知失敗の構造（なぜ 1,290 件が素通ししたか）

- **handler 未実行**: 既存テストは `_process_line_event` を**実行しなかった**（重い依存のため）。UnboundLocalError は import 時ではなく**実行時**に出るため、import・型・静的検査を全て通過した。
- **sentinel/AST の検査対象外**: RV-10 の redaction sentinel は「emit 出力に機微が乗らないか」を、AST allowlist は「sink の増減」を見る設計で、**emit 自体が呼べるか（scope 健全性）は対象外**だった。
- **監視空白**: 顧客返信の**成否を観測する常設シグナルが無く**、webhook 200 だけでは背景タスクの全滅を検知できなかった。P1-103-verify のデプロイ検分（ログ実読）で初めて発覚。

## 5. 修正

- `main.py`（旧 421 行）の関数ローカル `from hub.redact import emit` を**削除**（module-level import `main.py:54` で解決。同分岐の `from hub.notify import notify_business` は維持）。
- 回帰テスト `test_hotfix_emit_unbound.py`（3本）を追加し、**修正前コードで 3本とも FAIL を実測確認**:
  1. bytecode 検査（`emit` が `_process_line_event` の `co_varnames` に無く `co_names` にある）
  2. 先頭ログ実行（logger を sentinel で停止し、UnboundLocalError なく到達）
  3. AST 走査（`main.py` 全域＋`hub/` 配下に関数内 `redact.emit` 再 import が無い＝0件）
- 1行削除に伴う行番号シフトで `redaction_sink_allowlist.json` の main.py 2 entry を **611→610 / 894→893** に再同期（同一 sink・**台帳60・sink 債務の増減なし**）。
- **マージ = `16cf4bc`（PR #125）**。デプロイ後、`[WEBHOOK] queued → [PROCESS] start → [ROUTING] → [LINE] reply OK` の実イベントで **LINE 返信復旧を実測**・`UnboundLocalError` 0・traceback 0・`/health` 200。

## 6. 逸脱と事後監査

- **事前レビュー省略の逸脱**: 顧客中核経路の全停止のため、司令塔裁定により**事前 Codex レビューを省略**して即修正・即デプロイ（顧客影響最小化を優先）。
- **R-HOTFIX-01（事後監査）= PASS**。修正の正当性・回帰テストの検知力・台帳再同期の妥当性を確認。

## 7. 新規裁定（再発防止）

- **RCF-M10**: 顧客返信の**観測性**を恒久シグナル化し、**InboundEvent（受入条件）へ統合**する（webhook 200 と返信成否を分離監視。背景タスク全滅を検知可能に）。
- **P1-113（小粒）**: AST スコープ区別（module-level と関数内 import の扱い）を統合・整理する票。
- **運用規律 追加**: **顧客 Bot 経路（`_process_line_event` 等）に触れる差分は、handler の smoke 実行を必須検査に指定**する（実行時エラーを import/静的検査だけで見逃さない）。

## 8. テスト baseline・枠消化の日次一行

- baseline 確定: **1,290 passed / FAIL 0 / skip 0**・main `16cf4bc`・**台帳60**（行番号再同期済み）。
- 2026-07-14: HOTFIX-01（真因特定・1行削除・回帰3本・台帳再同期）＋デプロイ検分＋障害ウィンドウ確定＋インシデント記録固定。開始/終了とも **モデル実測 = Fable 5（claude-fable-5）**。
