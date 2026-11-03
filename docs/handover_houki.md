# 相続放棄（souzoku-houki）機能 引継ぎ書

- 作成: 2026-10-13（HOUKI-E2E-1・QUEUE-2026-10-13-E E-5）。対象は main（#271 まで）＋ 未 merge の 5 branch
  （houki-dairinin-1・houki-koseki-checklist-1・houki-jijou-1・houki-shokumu-link-1・houki-jukuryo-3）＋ 小票 2 本
  （houki-send-ledger-1・houki-shokugyo-1）。branch 由来の事項には `[branch]` を付す。
- 設計正本: docs/souzoku-houki/README.md（全体）・01-flow（フロー）・02-bot-hearing（ヒアリング）・03-jukuryo-kikan（熟慮期間）・
  04-kintone-design（kintone・**★App 33/34 は仮番号＝実装は App 40**）・06-documents（書類）・09-shinjutsu-package・10-koseki-matrix。
  docs/architecture/10-unit-02-souzoku-houki.md。
- 共通規律: RV-08（削除せず状態・履歴の追加）・RV-10（氏名・住所・LINE userId・本文をログ・通知・画面・URL に載せない。レコード
  番号・欄名・件数・固定語彙のみ）・承認なしの送信経路を作らない・kintone の承認なし書込ゼロ（例外は票で裁定された自動欄）。
- 共通の受け口認証（`shinjutsu_webhook.houki_webhook_disabled_reason`）: env `HOUKI_WEBHOOK_TOKEN` 未設定 → 404。時効側の
  `DOCUMENT_WEBHOOK_SECRET`／`KINTONE_WEBHOOK_TOKEN` と同値 → 404（誤設定の fail-closed）。不一致 → 403。body の app.id が
  App 40 でなければ `skip=app_mismatch`。
- 単一 worker 前提（docstring 明記）: ヒアリング会話履歴（in-memory）・相談カード読取の世代フェンス・受理通知送付の asyncio.Lock・
  scheduler（熟慮期間・超過・戸籍）。

## 1. LINE チャネルとヒアリング（houki_bot/）

| 項目 | 内容 |
|---|---|
| 入口 | `POST /webhook/souzoku-houki`（houki_bot/router.py）。署名検証 `hub.line_channel.verify_line_signature(HOUKI_CHANNEL)`。`houki_channel_disabled_reason()` が非 None なら 404。即 200・処理は BackgroundTasks |
| env | `HOUKI_LINE_CHANNEL_SECRET`・`HOUKI_LINE_CHANNEL_ACCESS_TOKEN`（hub/line_channel.HOUKI_CHANNEL・name=`souzoku-houki`）・`AUTOREPLY_PAUSED`（"1" で全業務ブレーキ）・`ANTHROPIC_API_KEY` |
| 流れ | AUTOREPLY_PAUSED → 記録のみ／App 39 停止リスト → 無応答／履歴は in-memory（空なら App 28 から復元）／App 40 の既知項目を注入（再質問禁止）／Claude tool `record_hearing`／送信ゲート route=`houki_hearing`（違反は App 29 承認キュー＋「確認中」定型）／送信は `reply_with_push_fallback(HOUKI_CHANNEL)` のみ／App 28 に category=相続放棄ヒアリング |
| 7 通台本 | `houki_case_store.HEARING_ROUNDS`（弁護士決定・凍結）: 亡くなった方／日付／借金と財産／他の相続人／戸籍（記録欄なし）／ご依頼者ご自身／ご相談の区分 |
| 書込集合 | `HEARING_WRITABLE_FIELDS`（frozenset・test で pin）。main では「職業」を除外（App 40 に実欄が無かった止血）。`[shokugyo-1]` で復帰＋可用性ゲート（hub/houki_shokugyo: form fields 1 時間キャッシュ・無ければ 職業 だけ落として他欄は書く） |
| 補助欄 | `LINEユーザーID`（検索キー）・`受付チャネル`=LINE・`status`（問い合わせ→電話判断待ち の一方向のみ）・`危険類型フラグ`・`債権者一覧` SUBTABLE（新規行 通知要否=未確認）・`日付申告メモ` の固定マーカー「【日付整合エラー検知】」・`電話推奨度`／`電話推奨根拠` |
| 選択肢の閉集合 | `HEARING_CHOICE_FIELDS`: 続柄／本人区分／未成年後見関与／財産処分有無／訴訟督促有無／相続順位／同時申述希望（選択肢外は write 0＋聞き直し） |
| 必須 10 欄 | `HEARING_REQUIRED_FIELDS` |
| 電話推奨度 | hub/houki_phone_triage: 通知「【電話推奨度】相続放棄」（throttle key `houki_phone_triage:{record_id}`） |
| 管理者通知 | 「【相続放棄LINE】新チャネルで受信がありました」（throttle `houki_inbound:{userId}`・本文は載せない） |
| 送信台帳 | main: `reply_with_push_fallback` が begin(business=channel=`souzoku-houki`)。`[send-ledger-1]`: 受信ごとに `hub.line_channel.touch_houki_inbound(user_id, event_id)`、返信は `houki_reply_context(event_id)`（purpose=other・受信イベント ID=`houki_reply:{webhookEventId}`）。houki_bot は send_ledger を直接 import しない（test_houki_bot_policy の閉集合） |
| 既知の制約 | houki_bot の import 閉集合（notify は notify_admin_line のみ）。Claude 全断は定型＋承認キュー。履歴は単一 worker |
| テスト | test_houki_bot_entry／test_houki_bot_policy／test_houki_hearing／test_houki_phone_triage／test_houki_image_analysis／`[send-ledger-1]` test_houki_send_ledger／`[shokugyo-1]` test_houki_shokugyo |

## 2. 相談カード（来所用 A4・読取）

| 項目 | 内容 |
|---|---|
| 入口 | `POST /souzoku-houki/card/{secret}`（houki_card_webhook.py）。`相談カード読取` が 読取依頼／読取中 かつ `相談カード` 非空。読取依頼は `$revision` CAS で claim（敗者 `cas_lost`）。読取中は `更新日時` が 10 分より古いときだけ reclaim（`in_flight`）。本体 `reader.run_card_read_by_id`（BackgroundTasks） |
| env | `HOUKI_WEBHOOK_TOKEN`・`ANTHROPIC_API_KEY` |
| kintone App 40 | 読: `相談カード`(FILE)・`相談カード読取`(DROP_DOWN: 未読取／読取依頼／読取中／読取済／要確認)・`更新日時`。書: `相談カード読取` の遷移＋ `apply_hearing_fields`（空欄のみ・confidence=high）・`append_creditors`。`NEVER_WRITE`（未成年後見関与・死亡日・起算日_確定 ほか）。status／response_mode は書かない |
| 雛形 | hub/houki_card（`CARD_VERSION=v1`・26 項目・7 群・債権者 3 行・第 5 群は記録欄なし）・docx_templates/houki/相談カード.docx・scripts/make_houki_card.py |
| 読取 | hub/houki_card_read（`MAX_FILES=5`・画像 5MB・PDF 10 頁・timeout 60s・retry 1・SYSTEM_PROMPT の sha pin・claim 世代フェンス）。HEIC／上限超は 要確認 |
| 通知 | 「【相談カード読取】案件レコードNo.{id}」「【相談カード読取・要確認】」（値は出さない） |
| 制約 | v1 カードに「職業」項目は無い（実書込は v2 以降）。単一 worker |
| テスト | test_houki_card／test_houki_card_read |

## 3. 申述書（H7C）

| 項目 | 内容 |
|---|---|
| 入口 | `POST /souzoku-houki/shinjutsu/{secret}`（shinjutsu_webhook.py）。kintone 形と `{"record_id"}` の両方。冪等: `申述書` 非空 → `skip=already_attached`。添付 PUT は `$revision` CAS。添付失敗は 500＋要確認通知 |
| 固定 pin | `TEMPLATE_PATH=docx_templates/houki/相続放棄申述書.docx`・`TEMPLATE_SHA256=a6bbc4d1…6c46`・`TEMPLATE_CANONICAL_SHA256=a2da8826…fc1a`（更新手順は scripts/make_shinjutsu_template.py） |
| 生成 | `docx_builder.fill_runs` のみ（fill_template 禁止）。丸数字: 続柄 `KANKEI_MAP`(7)・知った日の区分 `SHITTA_MAP`(4)・放棄の理由 `RIYU_MAP`(6)・セル `_CIRCLE_CELLS`。凍結検証 `ShinjutsuIntegrityError`。拒否 `ShinjutsuRejection.reasons`（10 種） |
| `[shokugyo-1]` | 「職業」ラベル右のセル `_SHOKUGYO_CELL=(4,2,5)` に差し込み（雛形・SHA 不変・期待列にも同じ差し込み・空なら従来と同一） |
| kintone App 40 | 読: 顧客名／furigana／住所／電話番号／生年月日／被相続人氏名／被相続人ふりがな／被相続人本籍／被相続人最後の住所／死亡日_申告／相続の開始を知った日／知った日の区分(+その他)／続柄／続柄その他／放棄の理由／管轄家庭裁判所／書類チェック。書: `申述書`(FILE) のみ。status は動かさない |
| 通知 | 「【相続放棄申述書】」「【相続放棄申述書・要確認】」 |
| 制約 | 「相続財産の概略」欄なしの様式。語句系選択（元号・都道府県）は手書き運用 |
| テスト | test_houki_shinjutsu |

## 4. 代理人目録 `[houki-dairinin-1]`

| 項目 | 内容 |
|---|---|
| 入口 | `POST /souzoku-houki/dairinin/{secret}`（dairinin_webhook.py・main.py 末尾で include）。添付欄 `代理人目録` が無ければ `skip=field_missing`（通知「【代理人目録】App 40 に添付欄…」・form fields 3600 秒キャッシュ）。`already_attached`／`rejected`／`cas_lost` |
| env | `OFFICE_ATTORNEY`・`OFFICE_NAME`・`OFFICE_ADDRESS`・`OFFICE_TEL`・`OFFICE_FAX`・`OFFICE_BAR`（新）・`OFFICE_REG`（新）・`OFFICE_ZIP`（任意）。未設定は空欄で生成＋通知「【代理人目録・事務所情報未設定】」（env 名のみ） |
| 固定 pin | `docx_templates/houki/代理人目録.docx`・`TEMPLATE_SHA256=0a73c687…d77d`・`LABELS` 14 行・`FOOTER`・`TITLE`。scripts/make_dairinin_template.py。`DairininIntegrityError` |
| kintone App 40 | 読: 被相続人氏名（事件名）・管轄家庭裁判所・事件番号（空可）・顧客名・住所・申述人本籍（欄なし想定）。拒否条件は申述書と同じ 10 種。書: `代理人目録`(FILE・**大野が CU で追加**) のみ |
| 制約 | 送信なし（添付のみ）。FAX は `OFFICE_FAX` のみ |
| テスト | test_houki_dairinin |

## 5. 事情説明書（上申書_熟慮期間）`[houki-jijou-1]`

| 項目 | 内容 |
|---|---|
| 入口（PWA） | `GET /app/houki/jijou/{record_id}`（下書き表示・書込ゼロ）／`POST …/generate`（承認して生成）。認証 `_gate`。署名付き ticket（record_id／revision／op_id）＋明示確認。revision 不一致は stale。下書きは in-process 1800 秒キャッシュ |
| env | `OFFICE_ATTORNEY`（{{代理人氏名}}）。Claude は既存キー |
| LLM | `draft_sections`（tool `draft_jijou`・`MAX_DRAFT_TOKENS=1500`・各節 `MAX_SECTION_CHARS=1200`）。不明箇所は「【弁護士確認】」。素材一致検査 `check_against_source`（日付・年月・金額・固有名詞）違反は生成しない（`JijouGuardError`） |
| 固定 pin | `docx_templates/houki/上申書_熟慮期間.docx`・`TEMPLATE_SHA256=0a05be02…e036`・5 節の見出し凍結（関係／経緯／理由／債務／処分）・`CLOSING`。scripts/make_jijou_template.py |
| kintone App 40 | 読 `SOURCE_FIELDS` 20 欄（顧客名・被相続人氏名・続柄・各日付申告・日付申告メモ・知った経緯・財産 4 欄・財産処分有無・訴訟督促有無・起算日_確定・起算点メモ・相続の開始を知った日・死亡日・管轄家庭裁判所）＋表示用 案件類型。書: `事情説明書`(FILE・**大野が CU で追加**・無い間は添付せず固定文言) |
| 制約 | 承認なしでは生成・添付しない。送信なし。AI の独自法的主張は禁止（起算点は `起算点メモ` の人の判断に依拠） |
| テスト | test_houki_jijou |

## 6. 戸籍チェックリスト `[houki-koseki-checklist-1]`

| 項目 | 内容 |
|---|---|
| 入口 | `POST /souzoku-houki/koseki-checklist/{secret}`（houki_koseki_webhook.py）／scheduler `HOUKI_KOSEKI_CHECKLIST`（interval 30 分・`SCAN_LIMIT=100`・main.py 末尾で登録） |
| env | `HOUKI_KOSEKI_CHECKLIST_DISABLED`（既定 有効）・`HOUKI_WEBHOOK_TOKEN`・業務 LINE（`business_token_env()`／`get_admin_line_user_id`） |
| 台帳 | business=`souzoku-houki`・channel=`houki_koseki`・purpose=other・受信イベント ID=`koseki_complete:{record_id}`（1 回のみ） |
| kintone App 40 | 読: status・続柄・複雑性フラグ・連続戸籍充足（yes は弁護士のみ）。書: `書類チェック` SUBTABLE（書類名／対象者／取得方法／書類状態）のみ。初期行 `COMMON_ROWS`＋続柄別マトリクス。対象 `ENGAGED_STATUSES=(受任, 書類収集中, 申述書作成)` |
| 通知 | 「【連続戸籍充足の候補】」「【必要書類の個別確定が必要】」「【書類チェック・要確認】」 |
| 制約 | 初期行は 書類チェック が空のときのみ（冪等）。スキーマ不一致は write 0（fail-closed）。書類名は既存 7 択・順位 2/3・代襲は「その他」＋対象者（司令塔仮置き） |
| テスト | test_houki_koseki_checklist |

## 7. 職務上請求接続 `[houki-shokumu-link-1]`

| 項目 | 内容 |
|---|---|
| 入口 | `POST /souzoku-houki/shokumu-link/{secret}`（houki_shokumu_webhook.py）→ `hub/houki_shokumu_link.process_record_id`。scheduler なし |
| flag／env | `SHOKUMU_PLAN_ENABLED`（既定 OFF・OFF は I/O ゼロ＝disabled・ON は大野）・`HOUKI_WEBHOOK_TOKEN`・`APP_HOUKI`（hub/heir_envelope `_unit_for_case` に APP_HOUKI→相続放棄 を追加） |
| kintone | 読 App 40: status・被相続人氏名・被相続人最後の住所。自治体は App 31（`shokumu_plan.resolve_app31`）。書 App 30: 提案封筒（発送ステータス=要確認・方向=受領・チャネル=スキャン受領・ユニット種別=相続放棄・案件アプリID／レコードID・実行済み=no・件名・チャネル固有データ JSON 最上位キー `houki_shokumu_plan`）。冪等キー `houki_shokumu_plan:{App40 No.}:{plan_hash}` |
| 対象 | `ENGAGED_STATUSES={受任, 書類収集中, 申述書作成}`・`LINE_TYPE=joh_removed`（除票 1 件）。結果語彙 disabled／not_engaged／problems／filed／already_filed／failed |
| 制約 | 封筒は「要確認の提案」で止まる。確定・M1 発行・座標校正は別票 |
| テスト | test_houki_shokumu_link |

## 8. 熟慮期間（main）と期限超過 `[houki-jukuryo-3]`

| 項目 | 内容 |
|---|---|
| scheduler | main: `HOUKI_JUKURYO_{HH}`（daily 8／13／18 時 JST・**main.py のコメント「8:00 JST」と不一致**）。`[jukuryo-3]`: `HOUKI_OVERDUE`（daily 9 時・`app.router.on_startup.append`） |
| env | 新規なし（業務 LINE token／admin id）。`[jukuryo-3]` `HOUKI_OVERDUE_DISABLED`（既定 有効） |
| 算定（凍結・大野裁定 2026-10-08） | 入力 `起算日_確定` のみ。期限＝3 か月後の応当日の前日（応当日なしは月末日の前日）。伸長なし。14 日前／7 日前の各 1 回。対象＝受任後 8 status かつ 申述提出日 空。起算日未確定は件数通知のみ（1 日 1 回） |
| kintone App 40 | 読: status・申述提出日・起算日_確定・熟慮期間期限・残日数・通知済み閾値。書（main のみ）: `熟慮期間期限`(DATE)・`残日数`(NUMBER)・`通知済み閾値`(CHECK_BOX「14日前」「7日前」・送信成功後の写し・`$revision` CAS）。`[jukuryo-3]` は書込なし |
| 台帳 | business=`souzoku-houki`・channel=`houki_jukuryo`・purpose=other・受信イベント ID=`houki_jukuryo_14:{No}:{期限}`／`houki_jukuryo_7:…`／`houki_jukuryo_unset:{JST日付}`。`[jukuryo-3]` channel=`houki_overdue`・`houki_overdue:{JST日付}`（0 件は送らない）。台帳不可は送らない。結果不明は unconfirmed（/app/send_ops） |
| 画面 | `[jukuryo-3]` `GET /app/houki/overdue`・`/app/api/houki/overdue`（案件番号・期限・残日数のみ） |
| 通知 | 「【相続放棄 熟慮期間】{日付}」「起算日未確定 {n} 件」「【相続放棄 期限超過】{日付} 時点で…{n} 件」 |
| テスト | test_houki_jukuryo／test_houki_jukuryo_precount／`[jukuryo-3]` test_houki_overdue |

## 9. 受理通知送付（発送）

| 項目 | 内容 |
|---|---|
| 入口 | `POST /souzoku-houki/soufu/{secret}`（status が 受理／債権者通知 以外は `skip=not_triggered`）→ `houki_soufu.process_soufu` |
| 条件 | status∈{受理, 債権者通知} かつ 受理通知受領日 非空 かつ 受理通知書 添付あり。債権者一覧の 通知要否=要・送付状態=未・債権者住所 非空 の行ごとに App 30 へ下書き 1 件（以後は既存 M4 関所） |
| kintone | App 40 読: status・受理通知受領日・受理通知書(FILE)・被相続人グループID・顧客名・債権者一覧。書: 送付状態(未→起票済→送付済)・送付発送管理No・追加料金対象(4 か所目以降 yes)。status は 受理→債権者通知 の一方向のみ。App 30 下書き（件名「受理通知送付（{債権者名}）」・同封物=受理通知書写し・チャネル固有データ `houki_soufu_key`）。App 32 同封物マスタ |
| env | `APP_SHIPPING`／`TOKEN_SHIPPING`・`APP_ENCLOSURE`／`TOKEN_ENCLOSURE`・`HOUKI_WEBHOOK_TOKEN` |
| 冪等 | `houki_soufu:{record_id}:{row_id}`・App 40 `$revision` CAS（`CLAIM_TTL_SEC=600`）・record_id ごとの asyncio.Lock |
| 送付状 | hub/houki_soufu_letter: `docx_templates/houki/受理通知送付状.docx`・`TEMPLATE_SHA256=e082fb5d…d383`・`BODY_SHA256=652b7893…ed32`・差し込み 15・和暦 |
| 書き戻し | hub/dispatch が App 30 の 発送済／完了 で App 40 の 送付状態=送付済 |
| 通知 | 「【相続放棄 受理通知送付】」「【相続放棄 受理通知送付・要確認】」 |
| テスト | test_houki_soufu |

## 10. 契約（参考）

- hub/houki_contract＋contract_webhook: `POST /souzoku-houki/contract/{secret}`（委任契約書・CloudSign・`docx_templates/houki/委任契約書.docx`）。test_houki_contract_gen。
- hub/houki_profile: `HOUKI_PROFILE`（App 28／29／39 共用・post_engagement_statuses 8 値）。

## 11. env 一覧

| env | 使用元 |
|---|---|
| HOUKI_LINE_CHANNEL_SECRET／HOUKI_LINE_CHANNEL_ACCESS_TOKEN | hub/line_channel.HOUKI_CHANNEL・houki_bot/router |
| HOUKI_WEBHOOK_TOKEN | shinjutsu／card／soufu／contract／`[br]` dairinin／koseki／shokumu の各 webhook |
| DOCUMENT_WEBHOOK_SECRET・KINTONE_WEBHOOK_TOKEN | 同値検査のみ（時効側） |
| AUTOREPLY_PAUSED | houki_profile.autoreply_paused |
| ANTHROPIC_API_KEY | houki_profile・phone_triage・card_read・`[jijou]` |
| APP_HOUKI／TOKEN_HOUKI | APP_HOUKI_CASE（全機能）・`[shokumu]` heir_envelope |
| APP_SHIPPING／TOKEN_SHIPPING・APP_ENCLOSURE／TOKEN_ENCLOSURE | houki_soufu・`[shokumu]`（App 30） |
| 業務 LINE（business_token_env()＝DISPATCHBOT 系・admin id） | houki_jukuryo・`[koseki]`・`[jukuryo-3]`・phone_triage・各 webhook 通知 |
| OFFICE_ATTORNEY／OFFICE_NAME／OFFICE_ADDRESS／OFFICE_TEL／OFFICE_FAX／OFFICE_BAR／OFFICE_REG／OFFICE_ZIP | `[dairinin]`（OFFICE_ATTORNEY は `[jijou]` も） |
| SHOKUMU_PLAN_ENABLED（既定 OFF） | `[shokumu]` |
| HOUKI_KOSEKI_CHECKLIST_DISABLED／HOUKI_OVERDUE_DISABLED（既定 有効） | `[koseki]`／`[jukuryo-3]` |

## 12. kintone 欄（App 40）の書込元まとめ

- ヒアリング／カード読取: `HEARING_WRITABLE_FIELDS`（空欄のみ・カードは confidence=high）＋ `債権者一覧.債権者名`・`危険類型フラグ`・`電話推奨度`・`電話推奨根拠`・`日付申告メモ` マーカー・`status`（問い合わせ→電話判断待ち）・`相談カード読取`。
- 書類: `申述書`／`[dairinin]` `代理人目録`／`[jijou]` `事情説明書`（FILE・後 2 つは CU 追加前提）。
- 熟慮期間: `熟慮期間期限`・`残日数`・`通知済み閾値`。
- 受理通知送付: `債権者一覧` の 送付状態・送付発送管理No・追加料金対象・`status`（受理→債権者通知）。
- `[koseki]`: `書類チェック` SUBTABLE。
- 弁護士専権（コードは書かない）: `起算日_確定`・`起算点メモ`・`電話要否`・`連続戸籍充足`・`申述提出日`・`受理通知受領日`・`死亡日`・`相続の開始を知った日`（確定欄）・`response_mode`。

## 13. 既知の制約・未決（引継ぎ時に確認）

1. main.py の HOUKI_JUKURYO のコメント「8:00 JST」と実装（8／13／18 時）が不一致（コードが正）。
2. docs/souzoku-houki/04-kintone-design.md の ★App 33／34 は仮番号（実装は App 40）。
3. CU で追加が必要な欄: `代理人目録`(FILE)・`事情説明書`(FILE)・`職業`(SINGLE_LINE_TEXT／DROP_DOWN)。無い間は各機能が fail-closed（添付せず通知／書かない）。
4. 相続放棄専用の `/app/souzoku*` 画面は無い（`/app/souzoku` は相続（souzoku-shorui）側の read-only）。相続放棄の PWA は `[jijou]` と `[jukuryo-3]` の 2 画面。
5. main.py 末尾追記の branch が複数あるため、merge 順で rebase が要る（pr_bodies/_merge_order.md）。
6. 送信台帳の purpose は閉集合（other＋接頭辞で代替）。正式移行は JIKOU-SEND-PURPOSE-2（docs/send_ledger_purpose_migration.md）。
