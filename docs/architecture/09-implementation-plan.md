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

#### T0-1 `hub/kintone.py` + `hub/webhook_auth.py` の新設（**実施済み 2026-07-03**）
- 参照: 03 §3・§4
- 作業: KintoneApp と共通クライアント関数群、webhook_auth 3関数を新規実装。
  `main.py` の `/webhook/kintone/approval` と `document_webhook.py` を hub 経由に置き換え
  （URL・レスポンス・kintone 書き込み内容は不変。旧関数は re-export で温存）
- 完了条件:
  - [x] `test_hub_kintone.py` / `test_hub_webhook_auth.py` 新規（httpx モック、
        revision 楽観ロック・KintoneConflict・GET のみ1回リトライを含む）
  - [x] 既存テスト無変更で全 PASS（cloudsign 8件・triage 分類一致率テスト含む）
  - [x] `/webhook/kintone/approval` と `/document/{secret}` の既存挙動の回帰テストを追加して PASS
        （`test_webhook_endpoints_regression.py`）
- 実装ノート: 承認 Webhook の LINE 送信・送信済み更新・チャットログ保存
  （`send_line_push` / `mark_approval_sent` / `save_to_chatlog`）は chat_responder のまま
  （エラーを握りつぶす既存セマンティクスの維持。ユニット一般化 G1/H7 のスコープで扱う）。
  hub 経由化したのは 合言葉検証・recordId 抽出・最新レコード再取得と、
  document_webhook の kintone I/O 全部

#### T0-2 `hub/notify.py` + `hub/scheduler.py` の新設（**実施済み 2026-07-03**）
- 参照: 03 §8・§9
- 作業: `notify_admin_line` を claude_gateway から移設（re-export 維持）、
  LINE Push 実装の一本化、ジョブレジストリ実装、daily_healthcheck を register_daily 経由に移行
- 完了条件:
  - [x] `test_hub_notify.py` / `test_hub_scheduler.py` 新規（スロットル・ジョブ隔離・時刻計算）
  - [x] `railway run python daily_healthcheck.py` の手動実行インターフェースが従前どおり動く
        （__main__ 経路・exit code・[HEALTHCHECK] ログ書式まで確認）
  - [x] 既存テスト全 PASS（cloudsign / T0-1 回帰 / triage 分類一致率含む）
- 実装ノート: LINE Push の一本化は hub/notify（push_line_message + notify_admin_line）
  まで。chat_responder / cloudsign_webhook / main.py に残る Push 実装の呼び替えは
  各モジュールの一般化タスク（G1〜G4）のスコープ。ジョブ登録名 "HEALTHCHECK" により
  Railway の登録ログは従来と同一書式。二重 startup 時に旧実装はループが二重化し得たが、
  レジストリ化で1タスクに保証される（改善・test_hub_scheduler で検証）

#### T0-3 `hub/docx_builder.py` の新設 + `config.py` UNIT_CONFIG（**実施済み 2026-07-03**）
- 参照: 03 §6・§10
- 作業: fill_template / to_wareki 移設（re-export 維持）、resolve_template / validate_template 追加、
  UNIT_CONFIG（時効援用のみ）追加、テンプレート検査を daily_healthcheck に登録
- 完了条件:
  - [x] `test_hub_docx_builder.py` 新規（差込〔run分割・表セル〕・和暦〔改元境界〕・
        規約解決〔新ユニット=エントリ追加のみの検証込み〕・プレースホルダ検査）
  - [x] 既存 `/document/{secret}` 回帰テスト PASS・既存テスト全 PASS
        （101件 + triage 分類一致率 PASS・healthcheck ドライラン異常0件）
- 実装ノート: テンプレート検査は config.EXPECTED_DOCX_TEMPLATES（パス→差込キーの
  レジストリ）を新設して監視項目Cとして登録。実テンプレートとレジストリの一致は
  test_hub_docx_builder が担保（誤警報防止）。list_placeholders() をレジストリ整備の
  補助として追加

### P1: ハブ中核

#### T1-1 App 30 発送管理の作成とスキーマ監視（**実施済み 2026-07-03**）
- 参照: 02 §2・§6・§7
- 作業: 【人の作業】kintone で App 30 を 02 §2.1 どおり作成し、Webhook・一覧・API トークンを設定、
  Railway に `APP_SHIPPING` / `TOKEN_SHIPPING` / `HUB_WEBHOOK_TOKEN` を登録。
  【Sonnet】`EXPECTED_KINTONE_SCHEMA` に App 30 を追加
- 完了条件:
  - [x] `railway run python daily_healthcheck.py` が App 30 込みで exit 0（異常0件・8アプリ目として検証）
  - [x] スキーマ定義のユニットテスト（選択肢網羅）PASS・既存テスト全 PASS（`test_config_schema.py`・全110件）
- 実装ノート: アプリ作成は Computer Use（docs/instructions/cu-app30.md）で実施したが、
  フィールドコードが英語に意訳される等4種の不一致が発生 → フォーム更新 API で修正
  （25件リネーム・NUMBER→SINGLE_LINE_TEXT の2件再作成・登録時初期値2件 OFF）し、
  フォーム設計 API の機械突合で **27/27 全一致**を確認してから登録した。
  実アプリ ID = 30。Webhook 登録と `HUB_WEBHOOK_TOKEN` は /hub/dispatch 実装後（T1-2）に実施。
  `同封物選択` は仮選択肢「（未設定）」のため required_options を置かない（T2-1 で差し替え）

#### T1-2 状態機械・承認・ディスパッチャ（`hub/approval.py` / `hub/dispatch.py` / `channels/base.py`）（**実施済み 2026-07-03**）
- 参照: 03 §5、01 §4
- 作業: 遷移表・claim_execution（revision 楽観ロック）・`POST /hub/dispatch`・
  CHANNEL_REGISTRY・notify_attorney_approval。テスト用フェイクチャネルで一巡を検証
- 完了条件:
  - [x] `test_hub_dispatch.py` 新規: 下書き→prepare→承認待ち／承認済→claim→dispatch→発送済／
        二重 Webhook で dispatch 1回／禁止遷移の総当たり検査／却下・エラー経路
        （+ `test_hub_approval.py`: 10×10全組の総当たり・claim 3分岐）
  - [x] **「承認待ち→承認済」へ遷移させるコードパスが存在しない**ことをテストで担保
        （SERVER_TRANSITIONS に →承認済 の組が無いことの検査 + 全状態からの遷移拒否 +
         dispatch モジュールが 発送ステータス を直接書かないソースレベル検査）
  - [x] 既存テスト全 PASS（187件・無変更）
- 実装ノート: 未対応チャネルは「状態を変えず警報のみ」（承認可能性の保全・エラー遷移にしない）。
  manual_mailing チャネルは 発送処理中 で停止し印刷指示を LINE 通知（発送済への変更は事務員）。
  返送期限の自動設定は T1-4 で追加。**kintone 側の Webhook 登録と HUB_WEBHOOK_TOKEN の
  Railway 登録はデプロイ後の人の作業**（App 30 作成手順書 §4 参照）

#### T1-3 `hub/address_label.py`（reportlab 座標印字エンジン）（**実施済み 2026-07-03**）
- 参照: 03 §7
- 作業: requirements.txt に reportlab 追加、フォント同梱、render_overlay（grid モード込み）／
  render_letterpack_label／render_label_sheet
- 完了条件:
  - [x] `test_hub_address_label.py` 新規（PDF 生成スモーク・ページサイズ・オフセット環境変数・
        長文縮小）PASS（18件・面付け複数ページ・返信用ラベル・フォント冪等含む）
  - [x] `/health` の依存チェックに reportlab+フォントを追加し OK
  - [x] 既存テスト全 PASS（224件）
- 実装ノート: IPAex ゴシックを assets/fonts/ に同梱（IPA Font License v1.0・ライセンス文書
  同梱・欠損時は reportlab 内蔵 CID フォントにフォールバックし /health に表示）。
  PDF は invariant モードで生成（同一入力→同一バイト列・テスト決定性）。
  返信用ラベルの事務所情報は env（OFFICE_NAME/OFFICE_ZIP/OFFICE_ADDRESS/OFFICE_TEL・
  config.get_office_info()）。**未設定だと render_reply_label が ValueError**
  → Railway への登録が M1/M4 稼働前の人の作業。
  ローカルの create_*.py（Meiryo 参照）は Windows 専用のため流用せず同梱方式を採用

#### T1-4 返送期限監視ジョブ（**実施済み 2026-07-03**）
- 参照: 03 §9、04 §1・§4
- 作業: `return_deadline_check` を register_daily に登録（返送期限超過 → LINE 警報・状態維持）、
  発送済→返送待ち遷移時の期限自動設定
- 完了条件:
  - [x] `test_return_deadline.py` 新規（日付固定モック・超過/非超過/警報文言）PASS
        （期限当日は非超過・超過1日・期限未設定/不正値の通知・複数件1通集約・登録8:00 JST）
  - [x] 既存テスト全 PASS（203件）
- 実装ノート: 期限未設定の返送待ちレコードも警報に含める（設定漏れ=永遠に警報されない
  事故の防止）。ジョブは毎日 8:00 JST（RETURN_DEADLINE_HOUR_JST で変更・
  RETURN_DEADLINE_DISABLED=1 で停止）。返送期限の自動設定は
  UNIT_CONFIG.return_deadline_days（既定21日）。
  付随修正: test_hub_dispatch のダミー ANTHROPIC_API_KEY が triage テストの
  skipUnless を誤解除する問題を修正（import 後にダミーのみ除去）

### P2: M4 送付案内（最初のチャネル）

#### T2-1 App 32 作成 + 同封物ブロック取得・送付案内 docx 生成（**実施済み 2026-07-03**）
- 参照: 07 §1〜§2、02 §4
- 作業: 【人】App 32 作成・App 30 `同封物選択` 選択肢投入・env 登録。
  【Sonnet】スキーマ監視追加・App30/32 同期検査を daily_healthcheck に登録・
  `channels/soufu_annai.py` の prepare 前半（ブロック取得・docx 生成）・テンプレ docx 新規
- 完了条件:
  - [x] `test_soufu_annai.py`: ソート・無効除外・ユニットフィルタ・未定義キー・複数行差込 PASS（17件）
  - [x] 同期検査の異常系テスト（App 32 に選択肢外キー）PASS・既存テスト全 PASS（241件）
- 実装ノート: 複数行差込のため hub/docx_builder に fill_template_multiline を追加
  （\n を <w:br/> に変換。既存 fill_template は不変）。同期検査は監視項目Dとして登録。
  テンプレート docx_templates/jikou/送付案内.docx は**仮雛形（文面は弁護士監修前提）**。
  アダプタは prepare のみ実装で **CHANNEL_REGISTRY 未登録**（登録・dispatch・ラベル・
  AI 特記事項・返送要否分岐は T2-2）。App 30 選択肢投入と App 32 ブロック行の登録は
  T2-2 完了後の実運用開始時に人が実施（同期規約 02 §4.2 の順序で）

#### T2-2 M4 完成（AI 特記事項・宛名ラベル・チャネル結線）（**実施済み 2026-07-03**）
- 参照: 07 §1・§3〜§6
- 作業: compose_note（tool use・失敗時空欄続行）、render_label_sheet 結線、
  CHANNEL_REGISTRY 登録、返送要否分岐、manual_mailing の dispatch
- 完了条件:
  - [x] 起票→prepare→承認→dispatch の統合テスト（モック）PASS
        （下書き→承認待ち〔成果物2点添付・特記事項書き戻し・弁護士通知〕→
         承認済→claim→発送処理中〔印刷指示で停止〕→二重Webhook冪等）
  - [x] AI 失敗時に prepare が成功し特記事項が空欄になるテスト PASS
        （＋人が書いた特記事項を AI が上書きしないテスト）
  - [x] 実地一巡（テスト宛先1件・実 kintone）確認手順を README 追記
  - [x] 既存テスト全 PASS（247件）
- 実装ノート: **返送要否分岐の自動遷移は保留** — 物理郵送では「発送済」への変更が人の操作で
  あり、発送済 Webhook を処理対象にする変更は T1-2 の確定挙動（発送済=skip・テスト済み）の
  変更を伴うため。返送要否は チャネル固有データ の needs_return フラグとして prepare 時に
  記録済みで、自動遷移は M1（T3-3・同じ要件を持つ）でまとめて扱う。それまで返送待ちへの
  変更は人（HUMAN_TRANSITIONS の範囲）。
  AI 特記事項は「本文_特記事項が空のときだけ」生成し kintone に書き戻し（承認前に弁護士が
  編集可能）。人の記入値は上書きしない。ラベルは宛先面＋返信用（事務所宛「行」）の A4 面付け。
  **追記（2026-07-03）**: テンプレートを事務所の実書式（送付状_委任契約書のみ__Python用.docx・
  リポジトリに正本参照として収載）ベースの正式版に差し替え。差出人ブロックは env 駆動
  （OFFICE_FAX / OFFICE_ATTORNEY を追加）、本文は定型＋返送依頼＋ご依頼者表示（空・宛先と
  同一なら非印字）、書類表（No./書類名/部数/備考）は hub/docx_builder の
  **fill_table_rows / fill_template_with_table（S2 設計の前倒し実装）**で行複製。
  備考=案内文＋返送要否の定型文言。部数は App 32 の「部数」列（**未作成・列追加は人の作業。
  無い間は既定1**）。fetch_blocks は fields 無指定（列追加に自動追従）

### P3: M1 職務上請求

#### T3-1 App 31 作成・初期データ投入・起票〜prepare（小為替計算）　★実施済み（2026-07-03）
- 参照: 04 §1〜§2、02 §3
- 作業: 【人】App 31 作成・env 登録。【Sonnet】総務省コード CSV からの一括投入スクリプト
  （registry_to_kintone.py と同方式）・スキーマ監視追加・`channels/shokumu_seikyu.py` の
  prepare（宛先解決・手数料計算・チェックリスト PDF）・チャネル固有データ検証
- 完了条件:
  - [x] 投入スクリプトの dry-run モードと件数検証 PASS（実投入は人が実行）
  - [x] `test_shokumu_seikyu.py`: 小為替計算（複数種別・欠損手数料→**登録依頼警報**）PASS
  - [x] 既存テスト全 PASS（290 passed / 2 skipped）
- 実装ノート（2026-07-03）:
  - **設計変更（発注者指示）**: 住所未登録・手数料未登録の自治体宛は「エラー遷移」ではなく
    **「App 31 への登録依頼」LINE 警報**（状態は下書きのまま）。`channels/base.PrepareDeferred`
    例外を新設し、`hub/dispatch._handle_prepare` が捕捉して遷移なし＋
    「エラーではありません」明記の警報を送る（throttle_key=`prepare_deferred:{recordId}`）。
    登録後に下書きのまま再保存すれば Webhook 再発火で自動再処理。02 §3・04 §5 も同期済み
  - 起票内容の不備（未対応の請求種別・JSON 不正・通数不正）は従来どおりエラー遷移
  - 投入スクリプトは `import_city_master.py`。dry-run 既定・`--execute` で本実行の二段階。
    件数検証（1,500〜2,000・6桁コード・重複）・既存団体コードスキップで再実行安全。
    投入は 団体コード/都道府県/市区町村名/有効=yes のみ（住所・手数料は実測運用）
  - App 31 のスキーマ監視は T1-1 時点で EXPECTED_KINTONE_SCHEMA に登録済み
  - 除籍謄本と改製原戸籍は同一手数料フィールド（`手数料_除籍改製原`）を共有
  - **CHANNEL_REGISTRY 未登録**（T2-1 と同じ前倒し防止。結線は T3-3）。実データ投入も未実施
    （dry-run→発注者承認→`--execute` の手順で人が実行）

#### T3-2 重ね打ち PDF・レターパックラベル　★実施済み（2026-07-03）
- 参照: 04 §3、03 §7
- 作業: FORM_COORDS 座標表（実用紙実測は人が行い値を提供）・重ね打ち PDF 生成・
  レターパック往復ラベル・キャリブレーション手順の README 化
- 完了条件:
  - [x] 座標表全キー印字・grid モード・オフセットのテスト PASS
  - [ ] 試し刷り確認（人）で実用紙に整合 — タスク完了は「テスト PASS + 試し刷り手順書」まで
        （**手順書は 04a として作成済み。実測・試し刷りは人の作業として未実施**）
  - [x] 既存テスト全 PASS（308 passed / 2 skipped）
- 実装ノート（2026-07-03）:
  - `channels/shokumu_seikyu.py` に FORM_COORDS（26項目・A4 左下原点mm）・
    `build_request_form_pdf`（grid= で方眼重畳）・`build_calibration_pdf`（方眼＋全キー名印字）。
    **座標は設計初期値（FORM_VERSION=v0）。実測キャリブレーションは 04a の手順で人が実施**
  - チェック印は「レ」（U+2713 は IPAex に無くトーフ化しうるため）。長文は fit_font_size で
    縮小（欄ごとの許容幅 `_FORM_MAX_WIDTH_MM`）。利用目的は42字で2行に折り返し
  - `hub/address_label.render_letterpack_roundtrip` 新設（2ページ: 1p=宛先「御中」・
    2p=返信用事務所宛「行」）。render_letterpack_label に honorific 引数追加（既定「様」・後方互換）
  - prepare 成果物は3点に: 発送準備チェックリスト.pdf / 職務上請求書_重ね打ち.pdf /
    レターパック往復ラベル.pdf。OFFICE_* 未設定時は宛先面のみに縮退し
    チェックリストに「返信用は手書き」と明記（prepare は止めない・アダプタは notify 禁止のため）
  - 校正用PDF3点の CLI: `python -m channels.shokumu_seikyu <出力フォルダ>`。
    手順書は docs/architecture/04a-shokumu-seikyu-calibration.md（オーナー向け・印刷倍率100%必須等）
  - 弁護士登録番号は env `OFFICE_ATTORNEY_REG`（任意・未設定なら空欄=手書き）
  - テストは test_shokumu_form.py（T3-1 の test_shokumu_seikyu.py は無変更）
- 追加要件の実装ノート（2026-07-03・実物用紙の確認により2様式対応へ改訂）:
  - 職務上請求書は2様式（様式第1号=戸籍謄本等・戸籍法10条の2 / 様式第2号=住民票の写し等・
    住基法12条の3）。FORM1_COORDS（35項目）/ FORM2_COORDS（38項目）に分割し、
    請求書類種別から自動判定（戸籍・除籍・原戸籍→様式1 / 住民票・除票・附票→様式2）。
    混在請求（例: 戸籍＋附票）は2枚生成
  - 様式1は「いずれかに○」のため**1枚につき1種別**（複数種別は種別ごとに1枚）。
    様式2は該当種別すべてに○で1枚に集約
  - **生年月日の様式別バリデーション**: 様式1は必須（欠損→ShokumuSeikyuError→エラー遷移＋
    「生年月日が必要です」警報）・様式2は任意（空欄なら非印字・エラーにしない）。
    和暦「昭和25年3月15日」/ ISO「1950-03-15」両対応（元号丸＋年月日に分解して印字）
  - 請求者欄の印字: 様式1=既定OFF（請求者欄印字済みの用紙在庫のため）・様式2=既定ON。
    チャネル固有データ print_requester: {"form1": bool, "form2": bool} でレコード単位上書き。
    弁護士会名は env OFFICE_BAR_ASSOCIATION（任意）
  - キャリブレーションPDFも様式別2枚（CLI は計5PDF出力）。04a 手順書を2様式版に改訂
  - 手数料明細（チェックリスト）に様式名を付記（附票300円=手数料_附票 は様式2側で整合確認済み）
  - **既存テストへの影響**: 要件「様式1の生年月日必須」により、T3-1 テストの
    フィクスチャ（生年月日なしで戸籍謄本を請求）が定義上不正な入力となったため、
    test_shokumu_seikyu.py の**フィクスチャ2箇所に生年月日を追加**（アサーションは無変更）。
    T3-2 の test_shokumu_form.py は2様式版に全面改訂（マージ前のため既存扱いでない）

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

### T9系: 相続放棄ユニット（**欠番・H系列に統合済み**）

T9-1〜T9-11 は 2026-07-03 の正本統合により **docs/souzoku-houki/08 の H系列（H1〜H16）に
統合済み・欠番**とする。相続放棄ユニットの正本は docs/souzoku-houki/（弁護士監修済み）であり、
10-unit-02-souzoku-houki.md は参考資料。**発注には H系列のタスク定義を使うこと。**

過去の参照の読み替え用に T9↔H の対応表を残す:

| 旧T9 | H系列（正） | 備考 |
|---|---|---|
| T9-1 | H1 | App 33。予約枠 App 34 は H6 のとおり採用（不採用案は撤回） |
| T9-2 | H2 | webhookパスは `/webhook/souzoku-houki` を採用（10-unit-02 §10.1 から移植・07 §3） |
| T9-3 | H3 | |
| T9-4 | H4 | H系の複雑性フラグ＋三段チャネル分岐が正（電話推奨度判定は不採用） |
| T9-5 | H5 | 期日二本立て（souzoku-houki/03）が正 |
| T9-6 | H8 | 受任確定＝CloudSign 締結＋Stripe 決済の両方（H系が正・決済先行案は不採用） |
| T9-7 | H7 | 回帰テスト方式（差分実行・週次cron）は 02 §5 に出典付きで移植 |
| T9-8 | H9/H10 | |
| T9-9 | H12/H16 | |
| T9-10 | H11/H13/H14 | |
| T9-11 | （対応なし） | 債権者への受理通知送付・料金体系は未監修のため不採用。採用時は H系列に新タスクとして追加する |

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
| （T9系） | 欠番（H系列へ統合。相続放棄の発注は docs/souzoku-houki/08 の H系列を使用） |
