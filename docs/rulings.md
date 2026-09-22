# 裁定の一覧（収集のみ）

リポジトリ内のコメント・テスト名・PR 本文・作業メモから、「裁定」「弁護士決定」「司令塔裁定」「[人]裁定」
として参照されている文言を集めた一覧。**内容の解釈・改変はしていない**（引用は原文のまま。長いものは … で切った）。

- 作成: 2026-09-20（HRI 系の指示票 §4）。コード変更なし。
- 追記: 2026-09-22 ── 裁定 B・G-2・G-3 の正文（§1-3・§1-5・§1-6。出典: 大野決定 2026-09-22（司令塔経由））。
- 行番号の基準: `907254e`（PR #259 の先端。PR #256〜#259 のチェーンを含む）。`main` に未 merge のファイル
  （`hub/human_reply_intake.py`・`hub/jikou_case_create.py`・`test_hri*.py` など）を指す箇所は、チェーンの
  merge 後に `main` で読める。
- PR 本文は 2026-09-20 時点の全 259 件を対象に検索した。
- 作業メモ = 実装担当（PC-A）の作業メモ。リポジトリ外にあり、決定時の原文ではない。
- 収集対象外: `docs/plan/` にある「裁定待ち」「裁定の手順」だけを述べた行（約 100 行）は個別に載せていない。

## 状態の凡例

| 状態 | 意味 |
|---|---|
| 原文あり | 裁定そのものの文言が、決定時に書かれたリポジトリ内ファイル・PR 本文・設計書にある |
| 要約のみ・原文未確認 | 後から書かれた言及・要約しか見つからない（票の要約・作業メモのみ、など） |
| 未発見 | 名前は参照されているが、文言がどこにも見つからない |

---

## 1. HUMAN-REPLY-INTAKE-1 と HRI-01〜07（PR #256〜#260）

### 1-1. 「裁定（第 1 段の決定事項）」 ── 原文あり（ただし 2 つの出所で番号の振り方が違う）

- `hub/human_reply_intake.py:3-19` は 1・2・4・6 の番号で記載（3 と 5 はこのファイルに無い）。
  - 「1. 起点=返答単独判定・常時。相談者からの受信テキストを、ボットのヒアリング中／人対応／ヒアリング完了後のいずれでも判定にかける。直前の事務所側メッセージの有無は条件にしない…呼び出しはヒアリング処理の**後**に置く」
  - 「2. 通知は欄名と kintone レコードのリンクのみ（値は載せない・RV-10 維持）」
  - 「4. 対象欄: 時効=JIKOU_FIELDS（本人情報 7+ヒアリング 5。郵便番号 は App 21 に欄が実在するときだけ…）・相続放棄=HEARING_WRITABLE_FIELDS − NOT_ASKED_FIELDS…すべて空欄のみ」
  - 「6. レコード未作成: 時効はヒアリングの作成と同じ最小レコード…を 1 回だけ作成（…複数件は書かない）。相続放棄は…apply_hearing_fields の作成経路（existing=None）」
- `houki_bot/hearing.py:251-254`: 「HUMAN-REPLY-INTAKE-1 裁定 3: 人対応ゲート（App 40 response_mode=人対応）。…顧客へは一切送信せず、App 28 に受信を記録…管理者へ通知」
- PR #256 本文「## 裁定(第 1 段の決定事項)と実装」は 1〜5 の番号で記載（6 は無い）: 1 起点=返答単独判定・常時／2 ヒアリング優先／3 通知は欄名と kintone レコードのリンクのみ／4 相続放棄は人対応ゲートを新設。取込はゲートの影響を受けず常時走る／5 レコード未作成時…
- 固定しているテスト: `test_human_reply_intake.py:3-16`（「固定する仕様（裁定の逐語）」）、`TestHoukiWiring::test_human_mode_silent_record_notify_and_intake_runs`、`TestJikouWiring::test_human_mode_path_also_runs_intake`、`TestSchemaAndParse::test_target_field_sets`
- 設計書 `人対応返答取込_設計.md`（リポジトリ外）には裁定そのものは無い。「## 5. 大野に要る決定事項」に大野への質問 7 項目があり、「停止（裁定待ち）」で終わる。
- PR #256 本文: 「指示 Bot「返信」タスク起点(第 1 段の A1 案)は不採用」

### 1-2. 「裁定 G」（HRI） ── 要約のみ・原文未確認

- PR #257 本文: 「裁定 G(人対応中は画像受領返信を含め顧客向け送信を一切発生させない)に反します。」
- HRI-01 の指示票の要約: 「人対応中は、画像受領返信を含め顧客向け送信を一切発生させない（裁定G）」
- `houki_bot/hearing.py`（PR #257）・`test_hri01_human_gate.py` のコメントが上記の要約を引用している。

### 1-3. 裁定 G-2｜人対応中に抑止した自動送信の解除後の扱い ── 原文あり（正文）

出典: 大野決定 2026-09-22（司令塔経由）。初出は 2026-09-20 の指示票（PR #257 本文へ転記）。

> - 人対応中に抑止した顧客向け自動送信（受領返信・読解結果を含む）は、人対応の解除後も顧客へ送らない。
> - 抑止した事実は App 28 に「保留」行として記録する。
> - 解除後の最初の受信時に、保留行より古い未回収マーカーを「人対応済」行で閉じる。保留行が存在しないマーカーは閉じない（送信失敗等の通常の未返信は従来どおり扱う）。
> - App 40 の編集 webhook は追加しない（案2を採用）。

- 運用ルール（コード外）: 「#256 から続く PR チェーンが本番に届くまで、相続放棄で人対応モードを使わない。」「人対応が必要になった場合は HOUKI_LINE_CHANNEL_SECRET を外してチャネルを止める。」
- 参照: PR #257 本文・PR #260 本文、`hub/image_intake.py`（PR #257 以降の「裁定 G-2」コメント）、`test_hri01_human_gate.py`、`test_hri06_houki_image_human_gate.py`

### 1-4. 「裁定 A」「裁定 C」（HRI） ── 要約のみ・原文未確認（引き続き）

- リポジトリ・全 PR 本文・作業メモ・設計書のいずれにも、この名前の付いた文言は無い。
- 指示票の要約として伝わっている内容:
  - C（要約）: 「取込経路の409は初回＋再試行1回、再取得1回」（HRI-05 の票。2026-09-22 の票で「裁定C要約」として再掲）。
    実装: PR（HRI-05）`hub/human_reply_intake.py` INTAKE_CAS_ATTEMPTS=2 / INTAKE_CAS_REFETCHES=1、
    `hub/houki_case_store.apply_hearing_fields(cas_attempts=, cas_refetches=)`、`test_hri05_intake_cas_limit.py`
  - A: 内容の記載なし
- 裁定 B は 2026-09-22 に正文が出た（§1-5）。
- C の要約に近い文言は、裁定 C の名前なしで存在する: 「書込: 空欄のみ・$revision CAS・409 再取得 1 回・上書きなし」（`hub/human_reply_intake.py:26`、`test_human_reply_intake.py:8`、PR #256 本文）。「初回+再試行1回」という語句はどこにも無い。
- 同じ英字が別票で別の意味に使われている点に注意: SHINDAN-LINE-LINK-1 の裁定 A〜H（その G は「管理者通知は出さない」）、GATE-EXEMPT-FIX-1 の裁定 A〜F と A'、PR #209 の「PWA-02（全件カーソル・裁定A）」。

### 1-5. 裁定 B｜AI 応答の項目値の検証 ── 原文あり（正文）

出典: 大野決定 2026-09-22（司令塔経由）。

> AI応答の項目値は、サーバ側で項目別の選択肢に対して検証する。検証前に、選択肢と
> 同義の表記（例：続柄「息子」→「子」）を正規化表に基づき正規化する。正規化表に無い
> 選択肢外の値は、スキーマ逸脱としてその応答全体をAI失敗として停止する。項目単位の
> 部分採用はしない。正規化表の追加は大野の裁定事項とする。

- 実装: PR（HRI-04）`hub/intake_choice_synonyms.json`（正規化表・初期案=要承認）、
  `hub/human_reply_intake.apply_choice_policy`、`test_hri04_choice_policy.py`
- 対象欄（選択肢を持つ 6 欄=`houki_case_store.HEARING_CHOICE_FIELDS` のうち取込対象）: 続柄・本人区分・相続順位・同時申述希望・財産処分有無・訴訟督促有無。時効の取込欄はすべて自由記述=対象外。

### 1-6. 裁定 G-3｜友だち追加時のあいさつ ── 原文あり（正文）

出典: 大野決定 2026-09-22（司令塔経由）。

> 友だち追加時のあいさつは、人対応ゲートの対象外とする。追加時点で当該ユーザーに
> 人対応状態は存在し得ないため。ブロック解除による再追加も同様に扱う。

- 参照: PR #260 本文の一覧 J14、PR #262（HRI-08）本文・`test_hri08_jikou_human_gate.py`
  `TestFollowGreetingUntouched`（対象外であることの固定）

---

## 2. JIKOU-FORM 系列

### 2-1. 「R-JIKOU-FORM-2 fix2 の裁定」 ── 原文あり（決定時のコードコメント。「裁定」の呼び名は後付け）

- `main.py:964-970`: 「判定順（fix2=fix1-01）: (a) 同ターンで紐付けが成立した場合は確定した linked_id を最優先（ローカルの bind 結果で判定・userId 検索の結果に依存しない）(b) …_known_rec の $id (c) いずれもない場合のみ create」
- `main.py:979-981`: 「分裂の検知: …統合先は本人性を確定した linked_id のまま・別レコードへは書かない。弁護士が突合できるよう要確認通知」
- 固定しているテスト: `test_jikou_form2.py` `TestDoubleCreateSuppression::test_link_turn_prefers_linked_id_over_other_known_rec`
- 「裁定」として参照している箇所: `hub/form_link.py:36-39`、`test_hri07_app21_unique_readiness.py:10`、`TestBindRecordConvergence::test_split_ruling_is_unchanged_without_constraint`、PR #259 本文
- PR #240（FORM-2）の本文は空。

### 2-2. JIKOU-FORM-1 fix2（結果不明時の番号照会・同番号再試行の撤去） ── 原文あり（ただし「裁定」とは呼ばれていない）

- `shindan_form.py:34-44`: 「fix2（fix1-01）: 結果不明時の「受付番号で照会→在れば成功扱い」と「同番号で create 再試行」は撤去した。受付番号は 6 桁乱数で他申込と衝突し得るため、番号一致だけでは今回の書込か別申込の既存レコードかを識別できず…（Codex 第 1 案・CU を伴うため今回は不採用）」
- テスト: `test_jikou_form1.py` `TestWriteConvergence`
- PR 本文には無い（PR #239 に fix2 の節は無い）。

### 2-3. 「その他の督促通知」は C 非該当=司令塔裁定 ── 原文あり（この 1 行が唯一の文言）

- `shindan_form.py:12-13`: 「判定はサーバ側のみ（優先順 C→B→D→A・②は判定に使わず保存のみ・「その他の督促通知」は裁判所手続でないため C 非該当=司令塔裁定）」
- PR #239 本文。テスト: `test_jikou_form1.py` `test_all_15_combinations` ほか

### 2-4. その他

- 「固定文言 A/B は司令塔案（大野裁定で差し替え可…）」: `hub/form_link.py:26`、`test_jikou_form2.py:20`（裁定はまだ出ていない）
- 「画面文言…弁護士裁定で差し替え可」: `shindan_form.py:284`、`test_jikou_form3.py:648`、PR #242 本文
- 「App 21 直載せ裁定」: 作業メモのみ ── 要約のみ・原文未確認

---

## 3. GATE-EXEMPT-FIX-1 ── 原文あり

- **裁定 A〜F**（逐語）: `test_gate_exempt_fix.py:7-13`（「裁定（逐語）」）、PR #255 本文
  - A: 「免除判定は「正規化後の完全一致」…それ以外の文字差（一字でも）は不一致」
  - B: 原文に置換／C: 既存の凍結テンプレ集合のみ／D: 字数計上は置換後…自由文／E: 固定語彙 1 行でログ
  - F: 「類似度しきい値方式は採用しない」
  - 実装側の言い換え: `hub/reply_sanitizer.py:19-27`
- **裁定 A'**（逐語）: `hub/reply_sanitizer.py:29-40`（「裁定 A' 逐語」）
  - 「A'. 凍結ブロックの照合は、既存サニタイザの除去処理（許可外絵文字の除去・Markdown 装飾記号の除去…）を通した後の文面に対して行う。…除去処理と 7 種以外の一字の差は不一致。類似度方式は不採用。」
  - 続く実装メモ: 「照合は sanitize_reply の「除去処理の後・字数検査の前」の 1 か所のみ（PRE_MATCH_TRANSFORMS に固定した strip_markdown → strip_emoji の直後）。structure_violations は再照合せず…」
  - テスト: `test_gate_exempt_fix.py` `TestRulingAPrime`（T8〜T13）、`test_T12_pre_match_transforms_pinned_and_single_match_site`
  - PR 本文には無い（PR #255 は A〜F のみ）。

---

## 4. 弁護士決定

いずれも決定時のコード記録として原文あり。弁護士の発言の逐語と明記されたものは無い。

- **HOUKI-HEARING-UX-1 決定 A**（7 通構成）: `hub/houki_profile.py:127-131`「旧正本…の 1 問ずつ方式は本票の弁護士決定 A で置き換え、未成年・後見の質問は決定 B で撤去」、`hub/houki_case_store.py:107`、`test_houki_hearing.py:950-951`
- **弁護士決定 B**（質問しない・記録しない欄=未成年後見関与）: `hub/houki_profile.py:46-48`・`:169-170`（「【聞かないこと（弁護士決定）】」）、`hub/human_reply_intake.py:12-13`、`test_houki_hearing.py` `test_prompt_minor_guardian_removed`、PR #256 本文
- **弁護士決定 C**（#9 未成年・後見関与は判定から外す）: `hub/houki_phone_triage.py:78-80`、`test_houki_phone_triage.py` `TestRule9MinorGuardianRemoved`
- **受け流し文**: `hub/houki_case_store.py:173`「弁護士決定・凍結。法的説明は送らない」
- **申述書**: `hub/houki_shinjutsu.py:8-13`（丸数字置換の閉じたマッピング／語句系は触らない／「「相続財産の概略」欄なしの様式のまま提出運用」）、`scripts/make_shinjutsu_template.py:13-18`、PR #235 本文「### 弁護士決定（凍結）の実装」
  - 「この様式のまま=大野裁定」という呼び名は作業メモのみ。リポジトリと PR #235 は「弁護士決定」と呼んでいる。
- **受領文言**: `hub/image_intake.py:37-38`「弁護士決定（凍結）: 受領文言は両チャネルとも…IMAGE_RECEIPT_REPLY を使用。枚数の文言追加はしない」
- **弁護士決定事項（2026-07-03 確定・変更禁止）**: `docs/architecture/10-unit-02-souzoku-houki.md:23`（表 `:25-32`: 受任フロー・料金 88,000 円・債権者 3 社まで・キャンセル ほか）、`:37`「決済 → CloudSign契約 の順…（2026-07-03 弁護士決定）」
- **「弁護士確定」と表記されているもの**（裁定とは呼ばれていない）: PR #250 HOUKI-JUKURYO-CRON-1「## 弁護士確定（凍結…）」1〜6、PR #16「弁護士指示（2026-07-03）による応答方針v2.1」
- `test_triage_classification.py:99`「弁護士裁定により期待値をautoに変更」 ── 要約のみ・原文未確認

---

## 5. AUTOREPLY・LINE 品質

- **AUTOREPLY-STYLE-1「文体のみ」** ── 原文あり: PR #229 本文「### 方針（裁定済み） 真似るのは**文体のみ**。金額・法的見立ての中身は従来どおり弁護士確定定型と承認制に従う。」、`chat_responder.py:347-350`、`test_autoreply_style1.py` `TestStyleInBothPrompts::test_style_only_ruling_is_explicit`
  - 「文体のみ・見本の数値/固有名詞/名乗り不引用」という語句は作業メモの表現。
- **AUTOREPLY-STOPLIST-1「裁定済み方針(C)+(B)」「fail-open（裁定済み）」** ── 要約のみ・原文未確認（(C)(B) の選択肢の文言は見つからない）: `hub/autoreply_stoplist.py:3,9`、`config.py:700`、`main.py:1545`、`test_autoreply_stoplist.py:5`、PR #219 本文
- **AUTOREPLY-GEN2 fix1[04] 法テラス語彙閉集合（本裁定で確定）** ── 要約のみ・原文未確認: `chat_responder.py:203`、`test_autoreply_gen2.py` `test_hoterasu_vocab_closure`、PR #225 本文。`chat_responder.py:231`「大野裁定の文言へ差し替える」（未決）
- **LINE-Q-002「[人]裁定の記録（2026-07-27）: 期限文言なし」** ── 原文あり: `docs/proposals/LINE_Q_002_ACK_PROPOSALS.md:19-21`、PR #169 本文。同文書の 裁定1・裁定2（`:5`・`:117`）は要約のみ
- **レビュー対応の規律**（「[人] 方針」許可文脈の閉集合方式／「[人] 指示「追加できるなら遮断を優先」」／「[人] 指針」共通ヘルパ化は merge 後） ── 作業メモのみ。要約のみ・原文未確認
- **司令塔裁定の分割法**（`test_db_foundation.py` を `-n 0` 単独+残りを `-n auto`） ── 作業メモのみ。要約のみ・原文未確認
- **HEARING-FAQ-BASIS-1 裁定 1〜9** ── 作業メモのみ（リポジトリ・全 PR 本文に該当なし）。要約のみ・原文未確認
  - 作業メモは逐語の A〜J が `ヒアリング定型回答_確定案_2026-09-14.md`（リポジトリ外）にあるとしている。

---

## 6. SOUZOKU-HOUKI・IMAGE-INTAKE・SHINDAN-LINE-LINK・その他の時効票

- **「独立フィールドなし裁定」（H2-fix1 [01]）** ── 原文あり: `hub/business_profile.py:21-23`「**pending_reply を流用**する（独立フィールドは持たない・H2-fix1 [01] 裁定。送信文言と App28 の assistant 保存文言は同一値）」、`chat_responder.py:1548-1550`、`hub/houki_profile.py:231`、`test_business_profile.py:17-18`、PR #231 本文
- **「App28/29/39/AUTOREPLY_PAUSED 共用裁定」** ── 呼び名は作業メモのみ。裁定としては要約のみ・原文未確認
  - リポジトリは「裁定」の語なしで共用を記載: `hub/houki_profile.py:16-19`「会話ログ=App 28 共用（正本 §11「完全共用」…）。承認キュー=App 29 共用…停止リスト=App 39 共用…AUTOREPLY_PAUSED は全業務ブレーキとして共用」
- **IMAGE-INTAKE-1「スコープ（票裁定）」**: `hub/image_intake.py:3-9`「画像バイナリの取得…と kintone 添付は**保留**…本票は「複数枚まとめ受領返信（デバウンス）」のみ」、PR #237 本文
- **「単一 worker 前提（既存裁定）…司令塔裁定でスコープ外」** ── 要約のみ・原文未確認: `hub/image_intake.py:75-78`、`hub/houki_card_read.py:79-81`、`test_image_intake.py` `TestSingleWorkerPinned::test_procfile_single_worker`、PR #237 本文（「司令塔指定=第 3 案」）
- **「H0-APP-2 採用裁定」**（App 40 財産 4 欄形）: `hub/houki_case_store.py:60` ── 要約のみ・原文未確認
- **HOUKI-SOUFU-1「裁定（第 1 段 D1〜D9・2026-09-08）」**: `hub/houki_soufu.py:10-17` に D1〜D5 の文言、PR #251 本文に D1〜D9 の 1 行ラベルの表。PR は原本として `相続放棄_発送設計.md`（リポジトリ外・未確認）を指している ── D1〜D5 は原文あり、D6〜D9 はラベルのみ
- **LABEL-PRINT-1 裁定 D1〜D9**: PR #252 本文（ラベルのみ） ── 要約のみ・原文未確認
- **SHINDAN-LINE-LINK-1 裁定 A〜H** ── 原文あり: `hub/shindan_link.py:3-13`（A・B・C・G・H）、`test_shindan_line_link.py:4-12`（「裁定（逐語）」A〜H。E は「空欄のみ CAS（409 再取得 1 回）」を含む）
- **JIKOU-NOTICE-1「大野裁定（改変禁止）」** ── 原文あり: `notice_webhook.py:17-24`（宛先「債権者各位」のまま／事務所住所ブロック固定／旧住所行／対象債権者 1〜3 ごとに 1 通）、`make_notice_template.py:19-23`、`test_jikou_notice1.py:8-11`、PR #227 本文
- **CONTRACT-GEN**
  - 「CloudSign 連携の一線（裁定済み方針）…送信 API（PUT /documents/{id}）は呼ばない」: `contract_webhook.py:51-55`、`test_contract_gen2.py` `TestSendApiNeverCalled::test_no_put_in_source`、`docs/instructions/cu-app21-contract.md:56`、PR #223 本文
  - 「Word と同一レイアウトは目標にしない（CONTRACT-GEN-2 裁定）」: `contract_pdf.py:10`
- **業務通知は指示Botチャネルから（2026-07-07 裁定）** ── 要約のみ・原文未確認: `main.py:1861`、`hub/notify.py:359`、`test_hub_notify.py:15,73`、`test_notify_channel_policy.py:1,63`、PR #75 本文

---

## 7. 相続・戸籍・P3/P4/P5・基盤

**設計書の裁定表** ── 原文あり（各設計書の裁定表に記載）

| 設計書 | 裁定 | 主な参照先 |
|---|---|---|
| `docs/design-drafts/DRAFT_P3_003_CMD.md:392-425` | §8 裁定 1〜10 | `dispatch_bot/heir_derive_task.py`、`hub/heir_envelope.py`、`hub/derivation_models.py`、`test_p3_003_cmd_impl.py` |
| `DRAFT_P3_003B_DESIGN.md:27-30`・`:178` | 裁定 1〜4、fix3 H01 [人]裁定（App36 write 0） | `hub/heir_projection.py`、`test_p3_003b_projection.py`、`test_config_schema.py:314` |
| `DRAFT_P3_003C_HELD_REJECTED.md:388-406` | 裁定①〜⑥ | `hub/heir_projection.py:306`、PR #191 |
| `DRAFT_P3_003C_CANCEL.md:181-188` | 裁定①〜⑧、CANCEL-05、CANCEL-IMPL-05 裁定=(b) | `hub/heir_cancel.py`、`test_p3_003c_cancel.py`、PR #207 |
| `DRAFT_RV08_SOFT_MERGE.md:164-170` | 裁定①〜⑦、RV08-IMPL-05／07 | `person_merge_exec.py`、`hub/person_validity.py`、`test_rv08_soft_merge.py`、PR #205 |
| `DRAFT_SHOKUMU_PLAN.md:655-665` | §5 ①〜⑧、fix2〜fix7 司令塔裁定（③⑤⑥⑧ は推奨のみで採否の記載なし=要約のみ） | `hub/shokumu_plan.py`、`test_shokumu_plan.py`、PR #197 |
| `DRAFT_SHOKUMU_PLAN_FB2.md:295-299` | ①〜⑤、FB2-09 案A | ── |
| `DRAFT_S6_2_ANOMALY.md:202-211` | ①〜⑩ | ── |
| `DRAFT_KOSEKI_VOCAB_EXT.md:163-169` | ①〜⑦、VOCAB-05／06 | ── |
| `DRAFT_RV04_HMAC_MIGRATION.md:57,62,131` | NM01、案B nonce 表 | `hub/service_auth.py:10` |
| そのほか | `DRAFT_P2_DURABLE_IGNITION.md`、`DRAFT_RV10_REDACTION_AND_NOTIFY.md:109`、`DRAFT_P3_003_ENVELOPE_FLOW.md:234`、`DRAFT_P4_PWA_INVENTORY.md:51`、`DRAFT_LINE_LOG_1_PROCEDURE.md:38` | ── |

**P3-001** ── 原文あり
- fix4 H02・fix5 M01 司令塔裁定（胎児合成 ID）: `hub/derivation_models.py:231-242,392-394`、`test_p3_001_derivation_models.py:499-567`
- fix3 改定裁定（AST 機械検査）: `hub/derivation_models.py:572-582`、`test_p3_core_ast_policy.py`、`docs/work-logs/2026-07-27_P2-mega-fix-rounds.md:32-39`

**司令塔裁定 3 件（2026-07-12）** ── 原文あり: `docs/work-logs/2026-07-12_R-P1-007-drafts-v2_裁定.md:48`、`hub/derivation_models.py:8`、`hub/template_registry.py:6`、`alembic/env.py:35`、PR #108 本文

**P4・P5 の [人]裁定** ── 原文あり
- 認証: `hub/webapp_auth.py:3-5`、`test_p4_001_webapp_auth.py:3`
- SW キャッシュ全廃（H01）: `webapp/sw.js:1`、`test_pwa_batch1.py:25,682`、`test_maint3_webapp_fetch.py:431`、PR #209 本文
- 表示: `hub/webapp_case_views.py:8-11`、`hub/webapp_approval_view.py:6-8`、`test_p4_002_case_views.py:3`、`test_p4_004_approval_view.py:3`、PR #182
- P5-001 裁定 3 点: `hub/clause_library.py:4-10`、`clauses/v1/iso_kyogi.yaml:2`、`test_p5_001_clause_library.py:3`、PR #172 本文
- 脅威モデル（2026-08-10 司令塔裁定）: `test_maint3_webapp_fetch.py:195,211,295-299,439`、PR #195

**Q 機能 大野裁定 2026-08-17（PWA 搭載）** ── 原文あり: `docs/automation-task-ledger.md:77`（`:79` Q-03/Q-04 DEFER）、`hub/webapp_q.py:5,85`、`hub/qa_store.py:4`、`test_q_batch1.py:3`、PR #213・#214・#215 本文

**ZAISAN「大野裁定」**（B 部=相続開始時残高・ZAISAN-GEN-2 下書き生成） ── 要約のみ・原文未確認: `units/souzoku/zaisan_xlsx.py:8,16-22`、`zaisan_webhook.py:25,197`、`config.py:547`、`test_zaisan_gen1.py`、`docs/instructions/cu-zaisan-mokuroku.md:20,53`、PR #224・#226

**戸籍・相続人（2026-07-05〜07）**
- 原文あり
  - 検収裁定「実機が正」（和暦原文）: `docs/koseki-ocr/02-data-schema.md:14`、`config.py:358`、`koseki_ingest.py:14`、PR #52
  - 受領口暫定分離・A+B ハイブリッド・C 案不採用: `docs/koseki-ocr/07-implementation-plan.md:35,46,76`、`koseki_ingest.py:5-12`、`koseki_reader.py:5-13`、PR #53・#55
  - 名寄せ 裁定1・裁定3: `person_merge.py:20-24`、`person_merge_exec.py:9-16`、PR #90・#91
  - 「別人」裁定は人による判定を指す運用上の語（弁護士・司令塔の裁定ではない）: `person_merge_exec.py:34,571-592`
- 要約のみ・原文未確認
  - R4-3 D-1〜D-5 裁定: `heir_derivation.py:4,15,20,712`、`kinship_graph.py:3,208`、`docs/koseki-ocr/09-heir-test-cases.md:52,71`、PR #94
  - S5・S4・S6-1 裁定: `registry_ingest.py`、`valuation_ingest.py`、`bank_ingest.py`、`sortation_ingest.py` ほか
  - KOSEKI-DATA-1 fix1(02)・fix2(06): `koseki_reader.py:227,253,289`、`test_koseki_data1.py`、PR #220
  - KOSEKI-CHECK-1 例外: `koseki_coverage.py:37,85`、PR #221

**基盤**
- 原文あり
  - D13 裁定: `hub/inbound_event.py:24-27`
  - P1-102 司令塔裁定 3 件（dead-man 統合形・M06 App30 封筒方式・H03）: `docs/work-logs/2026-07-12_P1-102_s1-failclosed.md:28`、`cloudsign_webhook.py:238,543,548,601-602`、PR #112
  - P1-107a 司令塔裁定の記録: 作業ログ `:33`、`claude_gateway.py:72,119`
  - 比較裁定「検知可能な 2 回返信（安全側）> 検知困難な 0 回沈黙」: `docs/work-logs/2026-07-14_RV-05-13-fix4_*.md:44`
  - 大野裁定「イ」: `docs/work-logs/2026-07-11_Phase0クローズ_Phase1着手.md:23`（O-01=PostgreSQL は `:42`）
  - P2K-H01 契約改定: `test_rv04c_gas_builder.py:289-291`、PR #144
  - OPEN-2 暫定裁定（4 キー維持）: `test_p2_lane3_signed_acceptance.py:11-14,149`、PR #149
- 要約のみ・原文未確認
  - P1-107 系 fix2 裁定: `redaction_sink_allowlist.json:4,118-156`
  - RMC-M01・MAIN-CONS-fix2 M01: `daily_healthcheck.py:16,219,316`
  - H11a 案(a): `daily_healthcheck.py:384`、PR #199
  - P1-104 再裁定: `daily_healthcheck.py:523`
  - 裁定8/RMC-M03: `tools/tracking_pg_harness.py:7,185`
- 未裁定（OPEN）: RV-10 出し分け水準（`hub/redact.py:15,169-170`）

**計画文書** ── 原文あり: `docs/plan/2026-07-10_role-division_fable-codex.md:54`（「# 1. 最終裁定」）、`docs/plan/2026-08_execution-plan.md:48,164,176,316`、`docs/plan/2026-08_pwa-product-design_v2.4.md:3444,3810-3859`、`docs/plan/2026-08-15_audit.md:10`

---

## 8. 「要約のみ・原文未確認」の一覧

- HRI 裁定 G（G-2・G-3 は正文あり=§1-3・§1-6）
- HRI 裁定 C（要約のみ=§1-4）
- 「App28/29/39/AUTOREPLY_PAUSED 共用裁定」（裁定としては）
- HEARING-FAQ-BASIS-1 裁定 1〜9
- 「App 21 直載せ裁定」
- AUTOREPLY-STOPLIST 方針(C)/(B)・fail-open
- 法テラス語彙閉集合の裁定
- 2026-07-07 業務通知チャネル裁定
- IMAGE-INTAKE「司令塔裁定でスコープ外」「第 3 案」
- H0-APP-2 採用裁定
- HOUKI-SOUFU D6〜D9・LABEL-PRINT-1 D1〜D9
- レビュー対応の [人] 方針／指示／指針、司令塔裁定の分割法
- `test_triage_classification.py` の弁護士裁定
- ZAISAN 大野裁定
- D-1〜D-5・S4・S5・S6-1・2026-07-06 裁定
- KOSEKI-DATA-1・KOSEKI-CHECK-1 裁定
- CONTRACT-GEN-1/2 裁定（CloudSign の一線を除く）
- P1-107 fix2 裁定・RMC-M01・MAIN-CONS-fix2 M01・H11a 案(a)・裁定8/RMC-M03
- PWA-02 裁定A
- LINE-Q-002 裁定1/2
- SHOKUMU_PLAN §5 ③⑤⑥⑧
- PR 本文に言及だけがあり文言が無いもの: #54 方針1、#68 案A、#73、#93、#95、#99、#107、#110、#111、#113、#117、#123、#126、#129、#156、#162、#174、#177、#183、#191、#203、#204、#208

## 9. 名前は参照されているが文言が見つからないもの

- HRI「裁定 A」: どこにも出現しない
- HRI「裁定 C」: この名前の付いた文言は無い（§1-4 の要約のみ）。裁定 B は 2026-09-22 に正文（§1-5）
- HRI「裁定 G」の原文（G-2・G-3 は正文あり）
- HUMAN-REPLY-INTAKE-1 第 1 段の裁定の、`hub/human_reply_intake.py` の番号での 5、PR #256 の番号での 6（2 つの出所の番号が対応しない）
- 「この様式のまま=大野裁定」という語句（リポジトリ・PR には無い。リポジトリは「弁護士決定」）
- 「文体のみ・見本の数値/固有名詞/名乗り不引用」という語句（リポジトリ・PR には無い）
- JIKOU-FORM-1 fix2 を「裁定」と名付けた箇所
- HEARING-FAQ-BASIS-1 の文言（リポジトリ・PR には無い）
