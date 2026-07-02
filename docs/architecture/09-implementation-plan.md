# 09. 実装計画：依存関係・推奨実装順序・Sonnet 発注用タスク分解

## 1. 依存関係と実装順序の根拠

```
P0 共通基盤抽出（リファクタ・挙動不変）
 └→ P1 ハブ中核（App 30・承認/ディスパッチ・ラベルエンジン・ジョブレジストリ）
      └→ P2 M4 送付案内   ← 最初のチャネル（外部API無し・マスタ/承認/docx/ラベルを全部通す）
           ├→ P3 M1 職務上請求（+App 31・重ね打ち・返送待ち）
           │    └→ P4 M5 スキャン受領（消込対象=M1 の返送待ちが存在してから）
           ├→ P5 M2 e内容証明（P2 完了後ならいつでも。P3/P4 と並行可）
           └→ P6 M3 FAX（P2 完了後ならいつでも。ただし T6-1 契約スパイクだけ先行着手）
P7 仕上げ（運用ドキュメント・全体回帰）
```

順序の根拠（手戻り防止）:

1. **P0 を最初に**: 5モジュール全部が hub/ を使う。後から抽出すると5モジュール分の書き直しになる
2. **M4 を最初のチャネルに**: 外部 API・座標精度・仕様確定スパイクのいずれも不要で、
   共通基盤（マスタ→prepare→承認→dispatch→物理発送）を最小リスクで実証できる。
   ここで見つかる基盤の設計不備は P3 以降に波及する前に直せる
3. **M1 → M5 の順**: M5 の中核機能「返送消込」は M1 の返送待ちレコードが無いとテストできない
4. **M3 を最後に**: 外部プロバイダ契約という不確定要素を持つため。ただし契約リードタイム対策で
   **T6-1（選定スパイク）だけは P2 と並行して先に発注してよい**（コード依存が無いため）
5. kintone アプリ作成（人の GUI 作業）は各フェーズ先頭のタスクに含め、
   スキーマを `EXPECTED_KINTONE_SCHEMA` に同時登録して翌朝から監視に乗せる

## 2. Sonnet への発注方法（再掲・README 参照）

- 1タスク = 1セッションで完結する粒度。タスク定義＋「参照設計書」をプロンプトに添付する
- 各タスクは feature ブランチ → PR → main の既存フローで完結させる
- **完了条件は全て「テスト PASS」を含む**。既存テスト
  （`test_cloudsign_webhook.py` / `test_triage_classification.py`）が壊れていないことは全タスク共通の完了条件

### 発注プロンプト雛形

```
リポジトリ: jikou-line-bot（Railway/FastAPI + kintone）
タスク: <タスクID> <タスク名>
参照設計書: docs/architecture/ の <該当ファイル>（添付）
既存コードの流儀（unittest / httpx モック / 合言葉Webhook / config一元管理）を踏襲すること。
完了条件: <タスクの完了条件をそのまま貼る> + 既存テスト全 PASS
```

## 3. タスク一覧（推奨実装順）

### P0: 共通基盤抽出（すべて挙動不変リファクタ）

#### T0-1 `hub/kintone.py` + `hub/webhook_auth.py` の新設
- 参照: 03 §3・§4
- 作業: KintoneApp と共通クライアント関数群、webhook_auth 3関数を新規実装。
  `main.py` の `/webhook/kintone/approval` と `document_webhook.py` を hub 経由に置き換え
  （URL・レスポンス・kintone 書き込み内容は不変。旧関数は re-export で温存）
- 完了条件:
  - [ ] `test_hub_kintone.py` / `test_hub_webhook_auth.py` 新規（httpx モック、
        revision 楽観ロック・KintoneConflict・GET のみ1回リトライを含む）
  - [ ] 既存テスト無変更で全 PASS
  - [ ] `/webhook/kintone/approval` と `/document/{secret}` の既存挙動の回帰テストを追加して PASS

#### T0-2 `hub/notify.py` + `hub/scheduler.py` の新設
- 参照: 03 §8・§9
- 作業: `notify_admin_line` を claude_gateway から移設（re-export 維持）、
  LINE Push 実装の一本化、ジョブレジストリ実装、daily_healthcheck を register_daily 経由に移行
- 完了条件:
  - [ ] `test_hub_notify.py` / `test_hub_scheduler.py` 新規（スロットル・ジョブ隔離・時刻計算）
  - [ ] `railway run python daily_healthcheck.py` の手動実行インターフェースが従前どおり動く
  - [ ] 既存テスト全 PASS

#### T0-3 `hub/docx_builder.py` の新設 + `config.py` UNIT_CONFIG
- 参照: 03 §6・§10
- 作業: fill_template / to_wareki 移設（re-export 維持）、resolve_template / validate_template 追加、
  UNIT_CONFIG（時効援用のみ）追加、テンプレート検査を daily_healthcheck に登録
- 完了条件:
  - [ ] `test_hub_docx_builder.py` 新規（差込・和暦・規約解決・プレースホルダ検査）
  - [ ] 既存 `/document/{secret}` 回帰テスト PASS・既存テスト全 PASS

### P1: ハブ中核

#### T1-1 App 30 発送管理の作成とスキーマ監視
- 参照: 02 §2・§6・§7
- 作業: 【人の作業】kintone で App 30 を 02 §2.1 どおり作成し、Webhook・一覧・API トークンを設定、
  Railway に `APP_SHIPPING` / `TOKEN_SHIPPING` / `HUB_WEBHOOK_TOKEN` を登録。
  【Sonnet】`EXPECTED_KINTONE_SCHEMA` に App 30 を追加
- 完了条件:
  - [ ] `railway run python daily_healthcheck.py` が App 30 込みで exit 0
  - [ ] スキーマ定義のユニットテスト（選択肢網羅）PASS・既存テスト全 PASS

#### T1-2 状態機械・承認・ディスパッチャ（`hub/approval.py` / `hub/dispatch.py` / `channels/base.py`）
- 参照: 03 §5、01 §4
- 作業: 遷移表・claim_execution（revision 楽観ロック）・`POST /hub/dispatch`・
  CHANNEL_REGISTRY・notify_attorney_approval。テスト用フェイクチャネルで一巡を検証
- 完了条件:
  - [ ] `test_hub_dispatch.py` 新規: 下書き→prepare→承認待ち／承認済→claim→dispatch→発送済／
        二重 Webhook で dispatch 1回／禁止遷移の総当たり検査／却下・エラー経路
  - [ ] **「承認待ち→承認済」へ遷移させるコードパスが存在しない**ことをテストで担保
        （transition() が該当遷移を人以外に許さない）
  - [ ] 既存テスト全 PASS

#### T1-3 `hub/address_label.py`（reportlab 座標印字エンジン）
- 参照: 03 §7
- 作業: requirements.txt に reportlab 追加、フォント同梱、render_overlay（grid モード込み）／
  render_letterpack_label／render_label_sheet
- 完了条件:
  - [ ] `test_hub_address_label.py` 新規（PDF 生成スモーク・ページサイズ・オフセット環境変数・
        長文縮小）PASS
  - [ ] `/health` の依存チェックに reportlab+フォントを追加し OK
  - [ ] 既存テスト全 PASS

#### T1-4 返送期限監視ジョブ
- 参照: 03 §9、04 §1・§4
- 作業: `return_deadline_check` を register_daily に登録（返送期限超過 → LINE 警報・状態維持）、
  発送済→返送待ち遷移時の期限自動設定
- 完了条件:
  - [ ] `test_return_deadline.py` 新規（日付固定モック・超過/非超過/警報文言）PASS
  - [ ] 既存テスト全 PASS

### P2: M4 送付案内（最初のチャネル）

#### T2-1 App 32 作成 + 同封物ブロック取得・送付案内 docx 生成
- 参照: 07 §1〜§2、02 §4
- 作業: 【人】App 32 作成・App 30 `同封物選択` 選択肢投入・env 登録。
  【Sonnet】スキーマ監視追加・App30/32 同期検査を daily_healthcheck に登録・
  `channels/soufu_annai.py` の prepare 前半（ブロック取得・docx 生成）・テンプレ docx 新規
- 完了条件:
  - [ ] `test_soufu_annai.py`: ソート・無効除外・ユニットフィルタ・未定義キー・複数行差込 PASS
  - [ ] 同期検査の異常系テスト（App 32 に選択肢外キー）PASS・既存テスト全 PASS

#### T2-2 M4 完成（AI 特記事項・宛名ラベル・チャネル結線）
- 参照: 07 §1・§3〜§6
- 作業: compose_note（tool use・失敗時空欄続行）、render_label_sheet 結線、
  CHANNEL_REGISTRY 登録、返送要否分岐、manual_mailing の dispatch
- 完了条件:
  - [ ] 起票→prepare→承認→dispatch→発送済→（返送要否分岐）の統合テスト（モック）PASS
  - [ ] AI 失敗時に prepare が成功し特記事項が空欄になるテスト PASS
  - [ ] `railway run` での実地一巡（テスト宛先1件・実 kintone）確認手順を README 追記
  - [ ] 既存テスト全 PASS

### P3: M1 職務上請求

#### T3-1 App 31 作成・初期データ投入・起票〜prepare（小為替計算）
- 参照: 04 §1〜§2、02 §3
- 作業: 【人】App 31 作成・env 登録。【Sonnet】総務省コード CSV からの一括投入スクリプト
  （registry_to_kintone.py と同方式）・スキーマ監視追加・`channels/shokumu_seikyu.py` の
  prepare（宛先解決・手数料計算・チェックリスト PDF）・チャネル固有データ検証
- 完了条件:
  - [ ] 投入スクリプトの dry-run モードと件数検証 PASS（実投入は人が実行）
  - [ ] `test_shokumu_seikyu.py`: 小為替計算（複数種別・欠損手数料→エラー遷移）PASS
  - [ ] 既存テスト全 PASS

#### T3-2 重ね打ち PDF・レターパックラベル
- 参照: 04 §3、03 §7
- 作業: FORM_COORDS 座標表（実用紙実測は人が行い値を提供）・重ね打ち PDF 生成・
  レターパック往復ラベル・キャリブレーション手順の README 化
- 完了条件:
  - [ ] 座標表全キー印字・grid モード・オフセットのテスト PASS
  - [ ] 試し刷り確認（人）で実用紙に整合 — タスク完了は「テスト PASS + 試し刷り手順書」まで
  - [ ] 既存テスト全 PASS

#### T3-3 M1 結線（発送済→返送待ち→期限監視）
- 参照: 04 §1・§4〜§6
- 作業: CHANNEL_REGISTRY 登録・投函後の状態遷移・追跡番号運用・統合テスト
- 完了条件:
  - [ ] 起票→承認→発送済→返送待ち→（期限超過警報）の統合テスト PASS
  - [ ] 冪等（二重 Webhook）テスト PASS・既存テスト全 PASS

### P4: M5 スキャン受領

#### T4-1 分類器と `/scan/v2`（既存 `/scan` 互換維持）
- 参照: 08 §1〜§2・§5
- 作業: classify_document tool・DOC_TYPE_CONFIG（既存3プロンプト移植）・冪等チェック・
  受領先アプリへの `原本PDF`/`Drive_fileId` 追加（人）・原本添付
- 完了条件:
  - [ ] 分類一致率回帰テスト（サンプルセット・95% 基準・`railway run` ガード）作成し合格
  - [ ] `/scan` 既存挙動の回帰テスト PASS・冪等テスト PASS・既存テスト全 PASS

#### T4-2 紐付け・返送消込・要確認キュー・GAS 仕様
- 参照: 08 §3〜§7
- 作業: 3段階紐付け・消込・要確認レコード＋reprocess・`docs/gas/scan_inbox.gs.md` 作成
- 完了条件:
  - [ ] 紐付け3段階・複数一致→要確認・消込（返送待ち→完了）・再処理一巡のテスト PASS
  - [ ] GAS 仕様書がレビュー可能な状態（コード写し込み）・既存テスト全 PASS

### P5: M2 e内容証明

#### T5-1 公式仕様確定スパイク + CSV ビルダ
- 参照: 05 §0・§2・§4
- 作業: 【人】最新の公式ガイド（差込差出し編）と公式テンプレート CSV を入手して添付。
  【Sonnet】05 §2 の表を確定値で更新（設計書を更新するタスク）・
  build_sashikomi_csv・validate_text_jis・ゴールデンファイルテスト
- 完了条件:
  - [ ] 05 §2 が「要確認」なしの確定仕様になっている
  - [ ] CP932・列順・住所分割・JIS 外文字検出のテスト PASS・既存テスト全 PASS

#### T5-2 M2 結線（本文 docx・自動起票・アップロード忘れ監視）
- 参照: 05 §1・§3・§5〜§6
- 作業: 内容証明テンプレ docx（文面は弁護士提供）・CloudSign 締結→自動起票・
  zip 成果物・CHANNEL_REGISTRY 登録・発送処理中滞留の日次監視
- 完了条件:
  - [ ] 締結→起票→prepare→承認→dispatch の統合テスト PASS
        （`test_cloudsign_webhook.py` 拡張含む）
  - [ ] 業者マスタ未登録→要確認のテスト PASS・滞留監視テスト PASS・既存テスト全 PASS

### P6: M3 FAX

#### T6-1 プロバイダ選定スパイク（**実施済み 2026-07-02**）
- 参照: 06 §0
- 作業: 候補 API の仕様・料金・送達結果取得方式の比較表を `docs/architecture/06a-fax-provider.md`
  に作成し、推奨1社と FaxProvider インターフェースの適合性を結論
- 完了条件:
  - [x] 比較表と推奨（コード無し・ドキュメントのみ）→ 06a 作成済み。推奨 InterFAX・次点 NetFax
  - [ ] 【人】06a §4 の契約前確認（トライアル申込・最新料金確認）を実施し契約判断

#### T6-2 FAX 送信アダプタ + 受任通知自動起票
- 参照: 06 §1〜§4
- 作業: FaxProvider 実装（選定プロバイダ）・受任通知 reportlab PDF・送信票・
  CloudSign 締結→FAX 下書き自動起票・番号バリデーション・CHANNEL_REGISTRY 登録
- 完了条件:
  - [ ] プロバイダモックでの dispatch 冪等テスト・番号検証テスト PASS
  - [ ] 締結→自動起票テスト PASS（業者 FAX 無し→要確認含む）・既存テスト全 PASS
  - [ ] `railway run` + skipUnless の実疎通テスト（自事務所 FAX 宛）を用意

#### T6-3 送達結果ポーリング・書き戻し
- 参照: 06 §1・§5
- 作業: `fax_status_poll`（15分）・DELIVERED/FAILED/リトライ3回/24h 未確定の全分岐・書き戻し
- 完了条件:
  - [ ] ポーリング全分岐のテスト PASS（時刻固定モック）・既存テスト全 PASS

### P7: 仕上げ

#### T7-1 運用ドキュメント・全体回帰
- 参照: 全設計書
- 作業: README に「発送/受領ハブ運用手順」（承認・警報対応・キャリブレーション・
  マスタメンテ・モデル更新時の分類回帰）を追記。環境変数一覧更新。全テストスイート実行
- 完了条件:
  - [ ] 全テスト PASS（`railway run` 系含む）
  - [ ] `railway run python daily_healthcheck.py` が新設アプリ全部込みで exit 0
  - [ ] README の運用手順が既存の書式（表・手順番号）と整合

## 4. タスクと参照設計書の対応（発注時に添付するファイル）

| タスク | 添付する設計書 |
|---|---|
| T0-1〜T0-3 | 01, 03 |
| T1-1 | 01, 02 |
| T1-2 | 01, 02, 03 |
| T1-3, T1-4 | 03, 04 |
| T2-1, T2-2 | 02, 03, 07 |
| T3-1〜T3-3 | 02, 03, 04 |
| T4-1, T4-2 | 02, 03, 08 |
| T5-1, T5-2 | 02, 03, 05 |
| T6-1〜T6-3 | 02, 03, 06 |
| T7-1 | README + 全体 |
