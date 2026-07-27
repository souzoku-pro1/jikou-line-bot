# DRAFT: P3-003-CMD 導出コマンド経路 — 設計（実装禁止・凍結先行）

- TASK_ID: P3-003-CMD 設計票（設計のみ・コード/テスト実装禁止）／記録日 2026-07-27
- 調査 BASE: origin/main（p3-003a 着地済み）。**本書は設計文書であり、Codex レビュー
  （R-P3-003-CMD-D1）→凍結→実装票の順で進む**（裁定4）。
- 正本参照（矛盾を作らない・編集しない）: DRAFT_P3_003_ENVELOPE_FLOW **§6 統一契約が正**
  （search 失敗=write 0／policy 失敗=I/O 0／create 通信失敗=結果不明・再実行時に
  完全一致検索で reconcile）・DRAFT_APP36 §2/§3.7・P3-001（DerivationRun の器）・
  P3-003a（`hub/heir_envelope.file_heir_envelope`＝結線点の公開関数）。

## 1. 経路全体（各段の入出力契約と担当モジュール）

```
[人]（弁護士・LINE 指示Bot）
  │ 語彙「相続人を導出して」（§2）
  ▼
dispatch_bot（既存基盤）: router（署名検証+ホワイトリスト）→ parser（task_type 分類）
  → TASK_REGISTRY entry task_type="heir_derivation"（新規1エントリ）
  → confirm フロー（既存 Pending 確認・「はい」で実行）
  ▼
dispatch_bot/heir_derive_task.py（新規・隔離 module＝person_merge_task と同型）
  1. flag ゲート: HEIR_DERIVATION_ENABLED（既定 OFF→固定文言で辞退・何もしない）
  2. 案件特定: 既存 confirm フローの案件指定（顧客名突合/No.直指定＝上位 T2 責務）
     → case_app_id / case_record_id
  3. App34 読取: kintone.search_records(App34, 案件参照=case_record_id)
     → heir_derivation.persons_from_records（既存・読み取り専用変換）
     入出力契約: 入力=App34 レコード列（$revision 含めて取得）／出力=HeirPerson 列
  4. 導出: heir_derivation.derive_heirs(persons, declarations, kosekis, ...)
     （凍結エンジン・無改変）→ Derivation（derived/held/error）
  5. payload 変換: hub.derivation_models.build_run_payload → validate（既存・
     胎児合成ID/enum/grammar はここで強制）
  6. run 保存: hub.derivation_models.create_derivation_run（P3-001 の正規経路・
     head 一意性/supersedes 連鎖は器が強制）→ run_id
  7. 封筒結線: hub.heir_envelope.file_heir_envelope(run)（P3-003a 公開関数・
     契約は §6 統一契約）→ filed / already_filed / disabled / not_target
  8. 応答: 指示Bot 返信（結果 summary・PII なし・§5/§6）
```

- 担当の境界: **heir_derive_task は「読取→組立→正規経路の呼出し」のみ**を持つ。
  検証・不変条件はすべて既存の器（validate 群・create_derivation_run・
  file_heir_envelope）に委ね、**本 task 内に新しい保存経路・検証の複製を作らない**
  （AST 機械検査の正規 module 集合も変更しない）。
- run 読み戻し: 手順 7 は保存直後の run（ORM 行）をそのまま渡す（再 SELECT 不要。
  file_heir_envelope は読取専用・result_payload 非読取が契約）。

### 1.1 input_hash の生成（新規・純関数）

- 正本 §2.1: 対象=persons＋input_person_revisions＋kosekis＋declarations＋at_date＋
  engine_version＋frozen_case_version の**正規化 SHA-256**。
- 設計: `compute_input_hash(...)` を**純関数**として新設（置き場所は実装票で
  hub/derivation_models を推奨=単一の正・未裁定論点 §8-4）。正規化=キー順ソートの
  JSON（ensure_ascii=False・separators 固定）→ sha256 hexdigest（**小文字 hex64**＝
  封筒境界 grammar と整合）。
- input_person_revisions: App34 取得時の `$revision` を record_id→revision の辞書で
  そのまま採用（後で人物が変わったら hash が変わる＝再導出で新封筒）。

## 2. 起動条件

- **語彙**: 主形「相続人を導出して」。registry の display_name/説明には
  「相続人」「導出」の**両語を含む明示指示のみ**が該当と記載し、parser の分類を
  絞る。**誤爆の最終防波堤は既存 confirm フロー**（案件名＋実行内容の復唱→「はい」
  で実行・それ以外は中止）＝新設の防御を作らず既存の型に乗る。
- **主体と経路の限定**: 起動経路は `/webhook/dispatch-bot` のみ（署名検証＋
  ホワイトリスト済み・弁護士のみ）。顧客 Bot（/webhook）からは到達不能。
  他経路（cron・startup・webhook 連鎖）からの自動起動は**作らない**
  （§3.7 裁定「初版は自動起動しない」）。
- **flag ゲート**: `HEIR_DERIVATION_ENABLED`（既定 OFF・hub.heir_envelope の既存
  判定関数を再利用）。OFF 時は task 冒頭で「本機能は未点火」固定文言を返し、
  **App34 読取を含む一切の I/O を行わない**。語彙一覧（parser への提示）にも
  flag OFF 時は載せない（誤爆自体を減らす）。

## 3. 失敗時挙動（§6 統一契約の上での CMD 側設計）

前提（§6 統一契約・封筒側）: search 失敗=write 0／policy 失敗=I/O 0／
create 通信失敗=**結果不明（ACK 不明）**・再実行時は冪等キーの完全一致検索で
reconcile（already_filed 回収）。**リトライ判断は CMD 側の責務**。

- **自動リトライ: しない（推奨・初版）**。理由: (a) 全段が冪等（再コマンドで安全に
  やり直せる・§4） (b) 自動再試行は ACK 不明時の二重動作の検討を増やすだけで、
  起動主体が[人]（対話中）なので再指示コストが低い。
- 失敗の分類と応答（すべて固定文言＋分類のみ・§5）:
  | 失敗段 | write 状態 | [人]への応答（指示Bot 返信） |
  |---|---|---|
  | App34 読取失敗 | write 0 | 「読取に失敗（分類名）。再指示で再試行できます」 |
  | derive 失敗（error run） | run は status=error で**保存する**（監査・§8-6） | 「導出エラー: 保留理由の件数のみ。詳細は kintone で確認」 |
  | payload/validate 失敗 | run 未保存・write 0 | 「導出結果が保存規格に適合せず中止（分類名）」＋業務チャネル警報 |
  | run 保存失敗 | DB 例外は伝播→ write は DB tx 内（部分状態なし） | 「保存に失敗（分類名）。再指示で再試行」 |
  | 封筒 search 失敗 | run 保存済み・封筒 write 0 | 「run #N は保存済み・封筒起票のみ失敗。**再指示で封筒のみ再試行**（冪等）」 |
  | 封筒 create 通信失敗 | run 保存済み・**封筒は結果不明** | 「run #N は保存済み・封筒は結果不明。**再指示すると完全一致検索で回収**（二重起票しない）」 |
- 再指示時の挙動が reconcile を兼ねる（§4）。**[人]への通知経路は指示Bot 返信を
  一次**とし、validate 失敗（規格逸脱＝バグの疑い）のみ業務チャネル警報を併用。

## 4. 冪等・二重起動（同一案件への連続コマンド）

1. **導出前チェック**: 現 head run（case_record_id で supersedes されていない run）を
   取得し、**新しく計算した input_hash が head と同一なら run を作らない**
   （重複 run 防止）。この場合も **file_heir_envelope(head) は呼ぶ**＝封筒未起票／
   ACK 不明の回収（already_filed or filed）。応答は「入力に変化なし・封筒 No.X」。
2. **入力が変わった場合**: 新 run を head の supersede として保存（P3-001 の器が
   連鎖健全性を強制）→ 新封筒（input_hash が変わる＝新冪等キー）。
3. **並行二重起動**: 指示Bot は同一ユーザー対話の逐次処理＋confirm フローで実質
   直列。万一並行しても P3-001 の DB 制約（single-root／supersedes UNIQUE）が
   片方を IntegrityError で拒否し、封筒側は完全一致検索が二重起票を遮断。
   拒否された側は失敗応答（再指示で回収）。
4. head 取得 helper `get_current_head(case_record_id)`（read-only・SELECT のみ）を
   新設する（置き場所は §8-4）。

## 5. PII 規律（経路上のデータの流れと漏れ防止）

| 区間 | 顧客データ | 統制 |
|---|---|---|
| App34 読取→derive | 氏名・続柄・生年月日等が**メモリ内のみ**を流れる | 保存しない・ログに出さない |
| run 保存 | **person_id のみ**（build_run_payload が氏名を落とす・validate が enum/grammar 強制・胎児は合成ID） | P3-001 実装済み |
| 封筒 | detail 閉集合（run 参照のみ）・件名は No./run# のみ | P3-003a 実装済み |
| 指示Bot 応答 | **件数・run id・封筒 No のみ**（氏名・人物一覧を返さない。内容確認は kintone 画面で行う） | 本設計で固定 |
| ログ | emit 契約経由の record_id/件数のみ・例外は type 名分類のみ（RCF-M05 流儀） | 実装票で sink 検査に乗る |
- エラーメッセージ・応答文へ App34 の値（氏名等）を**埋めない**ことをテスト計画
  （§7-8）で pin する。

## 6. 観測（成功/失敗/reconcile の計数と[人]の確認手段）

- **構造化ログ（固定文言＋emit）**: `[HEIR-CMD] derived run=<id> case=<id>`／
  `[HEIR-CMD] skipped(no-change) case=<id>`／`[HEIR-CMD] failed(<分類>) case=<id>`。
  封筒側の filed/already_filed は既存 `[HEIR-ENV]` ログが担う（already_filed の
  発生＝reconcile の計数に使える）。Railway ログの grep で集計可能な語彙に固定。
- **[人]の確認手段**: (i) 指示Bot 応答（その場の結果） (ii) kintone App30 封筒
  （要確認一覧） (iii) Railway ログの `[HEIR-CMD]`/`[HEIR-ENV]` 検索。
- daily_healthcheck への計数追加は**初版ではしない**（コマンド起動＝[人]対話型で
  沈黙障害が起きにくい。滞留系の監視は封筒の「要確認」残数として kintone 上で
  可視・必要になれば別票）。

## 7. テスト計画（実装票で書くテストの一覧）

1. registry/parser: 語彙の task_type 分類（正例）＋**誤爆 negative**
   （「相続人を確認して」「導出って何」等が heir_derivation にならない）。
2. flag ゲート: OFF で I/O ゼロ（kintone mock 未呼出し）＋固定文言応答。
3. 正常系 E2E（mock 境界=kintone のみ）: App34 mock→derive（実エンジン）→
   run 保存（sqlite 実 DB）→file_heir_envelope（実関数＋kintone mock）→filed。
4. 冪等: 同一入力の再コマンド→run 追加ゼロ＋already_filed 回収（reconcile 経路）。
5. 再導出: 入力変化（$revision 変化）→ 新 run が head を supersede＋新封筒。
6. 失敗経路: App34 読取失敗／derive error run 保存／validate 失敗（run 未保存）／
   封筒 search 失敗（run 残存・封筒 write 0）／封筒 create 例外（**契約 pin
   TestFailureBehaviorContract の再利用**＝ACK 不明→再指示回収）。
7. 並行二重起動: single-root IntegrityError 側の失敗応答（DB 制約は P3-001 テスト
   資産を再利用）。
8. PII 非漏れ: 応答文・ログ・例外文言に App34 の氏名 sentinel が現れない。
9. input_hash: 正規化の決定性（キー順・改行非依存）・revisions 変化で hash 変化・
   hex64 形式（封筒 grammar 整合）。

## 8. 未裁定論点（選択肢＋推奨＋理由・**勝手に決めない**）

1. **Declarations（放棄・欠格・胎児・養子区分）の供給源**:
   (A) 初版は空で導出（申告が要るケースはエンジンの保留/flag に任せる）
   (B) App34 の申告系フィールドから写像（実在すれば）。
   **推奨=(A)**（申告フィールドの実機仕様が未確定〔§9-2〕なまま写像を作らない。
   空でも derived/held は安全側に倒れる）。
2. **kosekis（App33 読解）の受け渡し**: (A) 初版 None（F5 収集見込み検査なし）
   (B) App33 から取得して渡す。**推奨=(A)**（§9-3 の実機確認後に (B) を別票）。
3. **frozen_case_version の値**: (A) 凍結表 version 定数（"v0.1"）を
   heir_derivation に定数化して参照 (B) env。**推奨=(A)**（凍結表と同居＝単一の正。
   エンジン無改変の原則に触れるため、定数追加の可否自体を裁定に掛ける）。
4. **新設ヘルパの置き場所**（compute_input_hash／get_current_head）:
   (A) hub/derivation_models（AST 正規 module 内・SELECT のみ）
   (B) heir_derive_task 内。**推奨=(A)**（hash 仕様と head 定義は台帳の契約＝
   単一の正に置く。task 側は呼ぶだけ）。
5. **head と同一 input_hash 時に run を作らない仕様**（§4-1）: 正本 §2.1 の
   「同一入力の再導出で二重封筒防止」を run 側にも及ぼす拡張解釈。
   **推奨=採用**（immutable 台帳に無意味な重複行を積まない）。相違があれば裁定を。
6. **derive error 時に status=error の run を保存するか**: (A) 保存（監査・正本
   §2.1 に error は status 語彙として存在） (B) 保存せず応答のみ。**推奨=(A)**
   （ただし error run は封筒 not_target＝起票されない。head 連鎖に error run を
   含めるかは (A) 採用時の細部裁定）。

## 9. 実機確認事項（[人]）

1. **App34 の案件参照フィールド**の実フィールドコード（検索キー: 案件レコードID
   相当）と、対象案件の App34 レコードに `$revision` が API で取得できること。
2. App34 に**申告系フィールド**（放棄・欠格廃除・胎児・養子区分）が実在するか
   （§8-1 の (B) 可否の前提）。
3. App33（戸籍）読解データの取得可否と形式（§8-2 の (B) の前提）。
4. 相続案件アプリ（App26）と App34 の紐付け運用（1案件に他案件の人物が混ざらない
   ことの運用確認）。
5. 語彙一覧への公開タイミング（flag 点火と同時か・先行して文言だけ載せるか）。
