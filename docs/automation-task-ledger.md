# 自動化タスク台帳（automation-task-ledger.md）

- 初収載: 2026-07-07（dispatch-bot/05 §3.3 が参照しつつ**リポジトリ未収載だった台帳の実体化**。
  2026-07-04 work-log の継続課題を解消。過去分は work-logs/ と PR 履歴が正）
- 運用: 完了は PR 番号つきで §1 へ・未着手は §2 へ・実機未検証の在庫は §3 へ。
  着手は司令塔チャットの裁定・指示があったもののみ（2026-07-07 運用規律）

## 1. 完了（2026-07-06〜07 反映分）

| 項目 | PR / 実施 | 備考 |
|---|---|---|
| 書類仕分け第2段（照会ログ App 38＋LINE仕分け語彙） | #68 | App 38 実機検収合格・実機で稼働 |
| S5 登記事項証明（読解 S5-1／転記+入口 S5-2） | #69・#70 | 実機検収済み（熊澤・不動産25 No.97/98） |
| S5-2.5 確定の関所（コア T1／Bot語彙 T2） | #71・#72 | 実機で No.7/8/10 確定済み |
| R4-0 戸籍の案件紐付け（要確認起票＋関所ハンドラ） | #73 | 遡及起票 No.10・実機確定済み |
| R4-1 人物レコード生成（＋冪等クエリ in 演算子修正・回収関数） | #74・#76 | 実機で熊澤5名を App 34 に生成済み |
| 業務通知の指示Botチャネル分離（フォールバック付き） | #75 | 実機有効（DISPATCHBOT トークン登録済み） |
| 入口統合T1（仕分け→戸籍/登記の自動回送・ゲート2層） | #77 | フラグ既定無効（§3 参照） |
| S4 近代化（M1 読解／M2 /valuation/ingest／M3 回送ゲート追加） | #78・#79・#80 | token 未登録（§3 参照） |
| Z1 関係図グラフ構造体（中間表現・生成前提の列挙型検証） | #81 | 純データ部品 |
| Z2 関係図レンダラ（graphviz）＋Railpack 補修 | #82・#83 | dot 2.43＋IPAexGothic 実機解決済み（fc-list 確認） |
| S6-1 通帳・残高証明（読解＋/bank/ingest・1口座=1行） | #84 | token 未登録（§3 参照） |
| clasp 復旧（logout→login→pull・invalid_rapt 解消） | 2026-07-07 実施 | 機械同期経路が復旧 |
| legacy/gas の GAS 正本同期（+137行・トークンはプレースホルダ化） | commit `41a580c`（branch: feature/sync-legacy-gas） | **push 済み・PR 未作成＝未マージ**（要 PR 指示） |
| /scan の kintone 400 調査（氏名必須欠落）・仕分け照会チャネル修正 ほか | #66・#67 等 | 詳細は work-logs |

テスト系譜（本日区間）: 554 → … → 761 → 770 → 784 → 803 → 805 → 818 → 831 → **851**（全PASS）。

## 1a. 完了反映（2026-08-09 追記・P3/P4/P5 系。詳細は各 DRAFT/work-log が正本）

7/08〜8/09 区間の完了を状態同期する（過去分の詳細は work-logs/・PR 履歴が正。
本節は台帳としての消込のみ）:

| 項目 | PR / 実施 | 備考 |
|---|---|---|
| P3-001 導出台帳（DerivationRun+HeirConfirmationDecision・凍結エンジン） | merge 済み（zokugara 改定 #184 含む） | **migration `d5e2b8a1c7f3` は本番 DB 未適用**（実機デー工程・下 2a） |
| P3-002 TemplateVersion registry | merge 済み | **migration `e7a3c9d2b5f1` は本番 DB 未適用**（同上） |
| P3-003a 封筒起票（heir_envelope） | merge 済み | flag `HEIR_DERIVATION_ENABLED` 既定 OFF |
| P3-003-CMD 導出コマンド経路8段 | #186 | 同上（語彙可視性も flag 連動） |
| P3-003b 関所+projection（confirmed 一本・App36 write 0 原則） | #187（設計 #183） | 同上 |
| **P3-003c held/rejected 語彙**（ENVELOPE_FLOW §3.2 残件の**消込**） | 設計凍結 **#191**（D3 PASS）・実装 `p3-003c-impl:0ba7fb3`＝**R-P3-003C-IMPL-1 レビュー中** | 「held/rejected の細部は実装票で」の別票は**本票で解消**（裁定①〜⑥確定・残る別票は 取消=裁定④ / H11 検知 / E0–E3 / 放棄写像のみ） |
| P4-001 認証+PWA shell（案(b) session） | #173 | Release A 先行 3 画面の基盤 |
| P4-002 案件一覧+詳細 | #176 | read-only proxy API+画面2枚 |
| P4-004 承認キュー参照 | #182 | 参照のみ（App30 絶対制約と非干渉） |
| P5-001 条項ライブラリ | #172 | P5-002 以降は書式受入（[人]）待ち |
| koseki lane2 cutover 完全クローズ（ゲート4+証跡③ retirement 3点充足） | #189・#190 | `2026-08-09_P2-koseki-lane2-D7-close.md`・`_retirement.md` |
| MAINT-1 小粒バッチ | #188 | held封筒×find_existing 統合pin ほか |

テスト系譜（2026-08-09 時点）: 851 → …（7月区間は work-logs 参照）… → 1911 → **1937**（全PASS）。

## 1b. 完了反映（2026-08-11 追記・NEXT-BATCH-SURVEY 消込）

NEXT-BATCH-SURVEY（司令塔側文書）の **#1〜#5・#7A・#10 を消込**。対応の実体:

| 項目 | PR / 実施 | 備考 |
|---|---|---|
| MAINT-4 小粒束ね4点（App38 死活監視登録・App30「人の確認待ち」文言・担保内容空化・App25 実機追随）＝**§2 の4行の消込** | **#198** | テスト 2019→2035 |
| P3-003C-H11→**H11a**（App36「戸籍確認済=yes」decision 監査・監視項目I・案(a)裁定） | **#199**（初版+fix1） | テスト →2057。**8/11 の migration 適用＋App36 env 投入済みにより追加点火なしで実働**——ただし実働は前提3点の成立が条件（E2-01 補記）: **(i) `DATABASE_URL` 設定済み (ii) `HEALTHCHECK_DISABLED` ≠ 1 (iii) HEALTHCHECK scheduler が startup で登録済み**（`[HEALTHCHECK] scheduler registered` ログ）。初実働検分=翌朝 daily_healthcheck **4点**（scheduler 登録ログ確認を含む・`2026-08-11_block-a-ignition.md` §7 の3点＋(iii)） |
| RV-0102-PREP（/scan・/ocr/fixed-asset の署名 opt-in 事前配線） | **#200**（IMPL-BATCH-1 A） | テスト →2073。強制化・GAS/watcher 点火は[人]別途 |
| **ブロックA点火完了**（migration 2本適用→`INBOUND_EVENT_DURABLE_ENABLED=1`→§8.1 P0 全通過） | 2026-08-11 実施（[人]+PC-A 検分） | 正本: `2026-08-11_block-a-ignition.md`。残観測=§8.2 P1（24h） |
| **SHOKUMU-PLAN-FB1 は DEFER 裁定**（供給方法が一意に定まらず停止→読解語彙拡張票へ付け替え） | IMPL-BATCH-1 B 停止報告 | 受け皿: `DRAFT_KOSEKI_VOCAB_EXT.md`（DOCS-BATCH-1 C 起草・D巡待ち）。FB1 再発行は同票 merge・実機 CU 後 |

## 1c. 完了反映（2026-08-17 追記・8/12〜17 消込）

8/11 更新（`10b0380`）以降の merge・実施を状態同期する（詳細は各 work-log・
PR が正本。本節は台帳としての消込のみ）:

| 項目 | PR / 実施 | 備考 |
|---|---|---|
| AUTOREPLY-PAUSE（①全体停止 flag `AUTOREPLY_PAUSED`・受信記録/通知は継続。fix1〜3=fail-closed 化・strict writer 化・処理所有権の単一化） | **#206**（2026-08-13 merge） | flag 投入は[人]（未投入=挙動不変） |
| P3-003C-CANCEL 実装（取消関所 `hub/heir_cancel`・App36 有効性 filter `hub/app36_validity`・write-set 台帳。fix1=write-set 先行保存/三値判定・fix2=未回収 pending の fail-closed 中止） | **#207**（2026-08-14 merge・f1b9dad） | migration **`f3d8c1a4e9b2`（projection_log）は本番適用済み**（2026-08-14。2026-08-17 に current=head・immutable trigger・CHECK 閉集合・ROWS=0 を再実測）。点火は[人]ゲート `HEIR_CANCEL_ENABLED`（未設定=OFF）。CANCEL-IMPL-06（LOW・型注釈）は司令塔持ち票 |
| RV02 封鎖: `SERVICE_AUTH_SIGNED_REQUIRED_PATHS` の器＋encoded alias 遮断（fix1・legacy 停止 list にも同修正。正規化後照合・fail-closed 入口遮断） | **#208**（2026-08-16 merge・420fcf6） | alias 遮断は本番実測済み（404・`bad_path_blocked` 計数）。**(c-0) registry 更新実施済み**（2026-08-16: `gas-ingest-2026-07a` へ `/scan` 追記＋新 kid `pc-watcher-2026-08a`〔/ocr/fixed-asset・expires 2026-10-01 JST〕）。残[人]ゲート=(c) GAS/watcher 署名付与→(d) unsigned_accepted 到達率実測→(e) 強制化 env 投入。手順書=RV02-CLOSE-PLAN（branch plan-audit `09877ec`） |
| PWA-BATCH-1（PWA 骨格＋相続案件ダッシュボード read-only。R-PWA-1 HIGH3 反映=field 閉集合 FETCH/VIEW・`$id` 全件カーソル・サーバ側整数集計） | **#209**（2026-08-17 merge・6a6380d） | **ブロック F env（`WEBAPP_PASSWORD_HASH`+`WEBAPP_SESSION_SECRET`）2026-08-17 投入済み=P4-001 の本番ログイン有効**（起動 4 象限通過・スモーク 4 点 OK・誤パスワード固定拒否実測）。残=大野の実機確認 5 段階（ホーム画面追加〜キャッシュ非保持） |

テスト系譜（2026-08-17 時点）: 2073 → **2295**（全PASS・`--ignore=test_triage_classification.py` 基準）。

**裁定記録（2026-08-17・大野）**: ③項目10（Q系質問応答）は要件1（専用 LINE アカウント新設）を改め **PWA に搭載**する。要件2〜7（読み取り専用・出典明示・未確定注記・第1版=kintone構造化+読解JSON・OCRテキスト保存・信頼度格付け）は維持。要件6（仕分け時 OCR テキスト保存）と PDF 全文検索（第2版）は別票。実装=Q-BATCH-1（branch q-batch-1）。

## 2a. 未着手項目の解錠条件（2026-08-09 追記・依存注記）

§2/§3 の 7 月裁定項目（正当放置を含む）は**そのまま**とし、現時点の主要ゲートを
依存注記として固定する:

| ゲート（解錠条件） | 解錠される項目 | 現況 |
|---|---|---|
| **scan 20 件到着**（大野提供サンプル・UX 設計素材） | P4-003（書類到着状況・書類ビュー/仕分け結果確認） | [人]待ち |
| **協議書 Word 書式の受入**（[人]） | P5-002 以降（条項差込み・docx 生成） | [人]待ち |
| **実機デー（migration 2 本適用→点火群）** | ブロックA（durable=`INBOUND_EVENT_DURABLE_ENABLED`）・ブロックE（`ATTORNEY_ALLOWLIST`→`HEIR_DERIVATION_ENABLED`）・§3 の env 投入 3 種（#7）との同日消化も可 | **2026-08-11 前半消化**: migration 2本適用済み（current=heads=`e7a3c9d2b5f1` 実測）・**ブロックA点火完了**（§8.1 P0 全通過・`2026-08-11_block-a-ignition.md`）。**残=ブロックE**（ALLOWLIST→flag の順・[人]）＋§3 env 3種＋§8.2 P1 観測 |
| **G-L3-0**（lane3 点火前ゲート・司令塔裁定） | lane3（bank/valuation の署名切替・`SIGNED_LANES` 残 3 lane） | 裁定待ち |
| **R-P3-003C-IMPL-1 PASS→マージ** | held/rejected 語彙の本番入り（flag OFF のため挙動不変） | レビュー中（`0ba7fb3`） |
| P3 merge 済み（**解錠済み**） | P4-005（相続人関係図・導出結果重畳含む） | ~~未着手・次票候補（scan 不要・先行可）~~ **実装・merge 済み**（**#194**・2026-08-10。App33 取得済み戸籍一覧は MAINT-3 B=#195 で追加）——本行は 8/11 更新時点で既に stale だった（2026-08-17 消込） |

## 2. 登録済み・未着手（司令塔の裁定・指示待ち）

| 項目 | 内容 | 依存・前提 |
|---|---|---|
| R4-2 名寄せ | 統合候補の検出＋関所での統合/別人確定（機械は確定しない） | **従前戸籍（入間・熊澤博）の取得待ち**・複数戸籍が揃ってから実質意味を持つ |
| Z3 法定相続情報一覧図 | reportlab A4縦・下部5cm・続柄表記テーブル・複雑フラグ/未確定の生成拒否・docx版 | **様式スパイク【人】: 法務局の最新の一覧図作成案内の入手**・R5（導出）と 36 の具体続柄の器の裁定 |
| S6-2 取引明細・異常検知 | 通帳明細の構造化・使途異常の検知 | 設計 **`DRAFT_S6_2_ANOMALY.md` 起草済み**（2026-08-11・R-S62-D1 待ち）。実装は 9 月レーン |
| B系 補助金ユニット | — | **設計書作成中**（司令塔側） |
| OCR 2回問題の改善 | 仕分け回送時の ocr_text 内部受け渡し（T1 裁定で第1版許容とした改善キュー） | koseki/registry/valuation 中核の引数拡張 |
| ~~config App 25 監視エントリの追随更新~~ ~~App 38 の死活監視登録~~ ~~App 30 通知文言の改善~~ ~~担保内容の「記録なし」原文の空化~~ | **#198（MAINT-4）で4点とも完了**（§1b へ移記・2026-08-11 消込） | — |
| dispatch-bot 第2弾 D5〜／e内容証明05 修正3点／座標キャリブレーション【人】等 | 2026-07-04 work-log からの継続 | — |

## 3. 実機フェーズの検証リスト（未検証在庫）

コードはマージ済みだが**実機未検証・未有効化**のもの。実行・有効化は全て**明示指示待ち**。

| # | 項目 | 有効化・手順 |
|---|---|---|
| 1 | 入口統合T1 の回送実機（戸籍/登記/評価証明×auto→読解ライン直行） | `SORTATION_FORWARD_ENABLED=1` 投入 → 未整理フォルダへ実PDF投入 → 全通確認 |
| 2 | S4 実物検証（/valuation/ingest に実課税明細） | `VALUATION_INGEST_TOKEN` 投入 → curl or 回送 → 25/35 反映確認 |
| 3 | S6 実物検証（/bank/ingest に実残高証明・実通帳） | `BANK_INGEST_TOKEN` 投入 → 1口座=1行・upsert 再送の確認 |
| 4 | 関係図の実描画（railway run で dot 実行・日本語PDF/SVG の目視） | 生成前提（名寄せ確定等）を満たす案件が必要 → 当面はテスト用確定データで確認 |
| 5 | 名寄せ同値化の目視（normalize_addr の統合拡大が既存 25 データと衝突しないか） | 既存 25 レコードの所在・地番を一覧で目視（読み取りのみ） |
| 6 | App 26 への FILE フィールド「関係図」追加 | 【人・kintone】追加後に Z2 の添付関数が使用可能 |
| 7 | env 投入3種 | `SORTATION_FORWARD_ENABLED` / `VALUATION_INGEST_TOKEN` / `BANK_INGEST_TOKEN`（`!` railway variables で投入・投入時に疎通確認を実施） |
