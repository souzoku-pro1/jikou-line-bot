# SOUZOKU-1 設計書: 争いのない相続 一気通貫（受付 → 聞取り → 戸籍収集 → 協議書案 → 署名押印 → 相続税要否 → 不動産売却引継ぎ）

- 作成: 2026-10-16（QUEUE-2026-10-16-R F-4・PC-A）。**設計のみ・コードなし。**
- 前提コードベース: main `8e1ab5a`（#271 merge 後）＋ 未 merge の相続放棄 7 機能（feature/houki-e2e-1 に統合済み・docs/handover_houki.md）。
- 位置づけ: 相続放棄（souzoku-houki・H 系列）と相続書類生成ライン（souzoku-shorui・S 系列）と戸籍読解（koseki-ocr・R 系列）の
  **間を LINE 受付から売却引継ぎまで一本の案件フローとして繋ぐ**設計。新しい生成エンジン・新しい送信経路は作らない。
- 正本の優先順位: 本書 > docs/souzoku-shorui（書類の中身）> docs/koseki-ocr（戸籍・相続人）> docs/souzoku-houki（流用元の流儀）。
  矛盾があれば本書の「再利用箇所」の列を正とし、差分は実装票で裁定を取る。
- 本書で決めない（大野裁定が要る）事項は各段階の「人が判断する点」と末尾 §10 にまとめた。**相談者向け文言は全て要承認**
  （hub/jikou_reply_texts.py と同じ「要承認」印・sha pin の別表に置く）。金額・日数・件数の閾値は定数ファイル 1 か所
  （hub/souzoku_constants.py を新設・要大野確認）。

## 0. 全体像

```
[相談者 LINE]                 [Railway: hub/ + units/souzoku/]                      [kintone]                 [人（大野・事務）]
 ① 相続相談受付 ──────────▶ 質問票 6 問（時効の診断フォーム／ヒアリング台本の同型） ──▶ 相続案件 App S1（新設）     受付通知を見る
 ② 相続人・財産の聞取り ───▶ LINE ヒアリング（相続放棄 7 通台本の同型・空欄のみ書く） ──▶ App S1 + App 36/35 候補行    聞取り結果を確認
 ③ 戸籍収集 ────────────────▶ 必要戸籍チェックリスト（HOUKI-KOSEKI-CHECKLIST の同型）    ──▶ App S1 書類チェック      職務上請求・承認（M1 関所）
                               職務上請求の提案封筒（HOUKI-SHOKUMU-LINK の同型）        ──▶ App 30 要確認封筒        
                               戸籍読解 → 人物 → 相続人候補（R2〜R4・既存）              ──▶ App 33/34 → App 36      相続人の確定（戸籍確認済=yes）
 ④ 遺産分割協議書案 ─────────▶ 財産目録（ZAISAN-GEN・既存）＋ 協議書「8割初稿」（S 系列）  ──▶ App 35/37 → docx       割付の入力・全文確認
 ⑤ 署名押印の進行管理 ───────▶ 発送（App 30）・返送期限（return_deadline）・署名状況の台帳  ──▶ App 30 / App S1         郵送・受領・印鑑証明の確認
 ⑥ 相続税申告要否の一次判定 ─▶ 基礎控除との比較（定数・課税価格は財産目録の評価額合計）   ──▶ App S1 判定欄           税理士紹介の要否を判断
 ⑦ 不動産売却への引継ぎ ─────▶ 引継ぎ書（docx）＋ 不動産行の抽出（App 25/35）             ──▶ App 30 発送 or 添付      業者選定・依頼者への説明
```

- 通知は **業務 LINE のみ**（相談者への自動送信は ①の受付確認 1 通と、承認後の進捗連絡〔JIKOU-PROGRESS-1 と同じ Q4 経路〕に限る）。
- 全段階で「提示は機械・確定は人」（koseki-ocr/04・heir_derivation の原則）。kintone の承認なし書込は **空欄のみ・候補行のみ・
  自動欄のみ**（相続放棄の HEARING_WRITABLE_FIELDS の流儀）。
- 送信台帳（hub/send_ledger）は business=`souzoku`（新値）・channel は段階ごと・purpose は閉集合 8 値のうち other＋接頭辞
  （JIKOU-SEND-PURPOSE-2 の正式移行後に新 purpose を追加）。

## 1. 段階 ①: 相続相談受付（LINE）— SOUZOKU-2 の「第 1 段階」

| 観点 | 内容 |
|---|---|
| 入力 | LINE（**既存の相続放棄チャネル HOUKI_CHANNEL を共用するか、新チャネルを作るかは大野判断**・§10-1）。友だち追加 → 用件の選択（相続放棄／相続手続き／時効）→ 「相続手続き」で質問票へ |
| 出力 | 相続案件レコード（App S1）1 件・業務 LINE へ受付通知 1 通（件数と種別のみ）・相談者へ受付確認 1 通（要承認文言） |
| 質問票（6 問・閉集合） | 1 被相続人の氏名（ふりがな付き） 2 死亡日 3 相続人の続柄と人数（配偶者／子／父母／兄弟姉妹／その他 × 人数） 4 遺言の有無（あり／なし／不明） 5 主な財産の種類（不動産／預貯金／有価証券／その他・複数選択） 6 争いの有無（ある／ない／わからない）。**6=ある は「当事務所では扱っていない」旨（要承認文言）で終了**（レコードは status=対象外 で残す・RV-08） |
| 人が判断する点 | 受任可否（争いの有無の再確認・利益相反）・初回面談の要否・費用説明。**機械は受任しない** |
| AI が自動化する点 | 質問票の進行（時効の診断フォーム shindan_form／hub/form_link と相続放棄ヒアリング houki_bot の同型。自由文の解釈は Claude tool `record_souzoku_intake`・選択肢は閉集合で検証・閉集合外は聞き直し）・ふりがなの正規化（hub/hearing_update.normalize_furigana 流用）・日付の整合（死亡日が未来・不正は聞き直し） |
| kintone アプリ案 | **App S1 相続案件（新設・番号は作成時採番）**: status（問い合わせ／受任前相談／受任／戸籍収集中／財産調査中／協議書作成中／署名押印中／完了／対象外／不受任）・LINEユーザーID・受付チャネル・顧客名・顧客ふりがな・電話番号・住所・被相続人氏名・被相続人ふりがな・死亡日_申告・死亡日（確定・弁護士）・相続人構成（SUBTABLE: 続柄／人数）・遺言の有無・財産種別（CHECK_BOX）・争いの有無・受任日・担当メモ・相談者区分（相続人本人／代理）。※ 既存「相談カード（相続）」（SOUZOKU_KINTONE_APP_ID・App 26）は来所用カードと書類ステータスの器として残し、**LINE 受付の案件は App S1 に置く**（§10-2 で統合の要否を裁定） |
| 送信台帳 purpose 案 | 受付確認: `souzoku_intake_ack`（移行前は other＋`souzoku_intake_ack:{inbound_event_id}`）・業務通知: `souzoku_notice`（channel）＋key `souzoku_intake:{inbound_event_id}` |
| RV-08／RV-10 | 対象外・不受任もレコードを消さない（status で表す）。通知は「相続相談 1 件・相続人 n 名・財産種別 x」のみ（氏名・続柄の個別値は載せない）。質問票の回答は DB 表 `souzoku_intake`（migration 作成のみ・未適用）に **kintone 書込前の一時保管**として置き、App S1 作成後は参照のみ |
| 再利用箇所 | shindan_form.py（診断フォームの状態機械・token）・hub/form_link.py（受付番号の引継ぎ）・houki_bot/（LINE webhook・署名検証・在庫注入・tool 記録）・hub/houki_case_store.py（HEARING_CHOICE_FIELDS の閉集合検査・空欄のみ書く apply）・hub/send_policy.send_business_notice（業務通知）・hub/line_channel.reply_with_push_fallback（唯一の送信プリミティブ） |

## 2. 段階 ②: 相続人・財産の聞取り

| 観点 | 内容 |
|---|---|
| 入力 | ①の案件（受任前相談 or 受任）・LINE の会話・来所時の相談カード（既存 App 26 の OCR 読取 `/scan`） |
| 出力 | App S1 の聞取り欄（空欄のみ）・App 36 相続人の **候補行**（データ源=ヒアリング・戸籍確認済=no）・App 35 財産の **候補行**（データ源=ヒアリング・評価確定=no・特定情報は申告どおり） |
| 聞取り台本（7 通・凍結は大野） | 1 被相続人（氏名・本籍・最後の住所） 2 相続人（配偶者の有無・子の人数と氏名・先に亡くなった子の有無〔代襲〕） 3 先順位者がいない場合の父母・兄弟姉妹 4 遺言（公正証書／自筆／なし・保管場所） 5 財産（不動産の所在・預貯金の金融機関・有価証券・保険） 6 債務（借入・保証・葬儀費用） 7 ご依頼者ご自身（連絡先・相続人との連絡状況）。**文言は要承認・hub/souzoku_hearing_texts（仮）に別表** |
| 人が判断する点 | 相続人の範囲の見立て（代襲・数次・養子は heir_derivation の要弁護士フラグ）・財産の網羅性（名寄せの要否）・単純承認事由の有無の説明 |
| AI が自動化する点 | 台本の進行・既知項目の再質問禁止（在庫注入）・App 36/35 候補行の起票（**候補のみ**・冪等キー＝案件 No.＋種別＋特定情報 sha）・財産種別の分類（閉集合） |
| kintone アプリ案 | App S1 に聞取り欄（被相続人本籍・被相続人最後の住所・遺言種別・遺言保管場所・債務メモ・葬儀費用メモ）。App 36（相続人・実 36）は既存 field のまま（データ源=ヒアリング）。App 35（財産・実 35）は既存 field のまま（データ源 に「ヒアリング」が無ければ CU 追加・§10-3） |
| 送信台帳 purpose 案 | ヒアリング返信: other＋`souzoku_reply:{webhookEventId}`（HOUKI-SEND-LEDGER-1 の同型）。通知: `souzoku_notice`＋`souzoku_hearing_done:{案件No}` |
| RV-08／RV-10 | 候補行は削除せず `有効=no`／`取消済み` で無効化。相続人の氏名は通知に載せない（件数のみ）。会話履歴は App 28（category=相続ヒアリング）に残す |
| 再利用箇所 | houki_bot/（HEARING_ROUNDS の構造・send gate route）・hub/houki_case_store.apply_hearing_fields（空欄のみ・confidence）・hub/houki_card_read（相談カードの読取・claim 世代フェンス）・hub/zaisan 系 units/souzoku/zaisan_sync（App 35 の冪等キー・有効/無効）・hub/brain_ledger（案件脳の fact 台帳: 聞取り値を locator 付きで記録） |

## 3. 段階 ③: 戸籍収集

| 観点 | 内容 |
|---|---|
| 入力 | App S1（受任）・被相続人の本籍（申告）・相続人構成（申告） |
| 出力 | 必要戸籍チェックリスト初期行（App S1 書類チェック SUBTABLE）・職務上請求の提案封筒（App 30・要確認）・戸籍 PDF の読解（App 33）・人物（App 34）・相続人候補の導出（DerivationRun → App 36 関所）・相続関係図（kinship_renderer） |
| 人が判断する点 | 職務上請求の承認（M1 関所・弁護士名義）・読解結果の確認（人物確認 person_confirm）・**相続人の確定（App 36 戸籍確認済=yes は弁護士のみ）**・連続戸籍の充足（yes は弁護士のみ） |
| AI が自動化する点 | 初期行の生成（続柄マトリクス）・請求先自治体の解決（App 31）・請求書類の生成（channels/shokumu_seikyu）・戸籍の OCR 読解（koseki_reader）・従前戸籍チェーン照合（koseki_chain）・不足戸籍の指摘（koseki_coverage・read-only）・相続順位と法定相続分の**提示**（heir_derivation・Fraction）・関係図の描画 |
| kintone アプリ案 | App S1 に `書類チェック` SUBTABLE（相続放棄と同じ 4 列: 書類名／対象者／取得方法／書類状態・選択肢も同じ）と `連続戸籍充足`（弁護士のみ yes）。App 33/34/36 は既存のまま（ユニット種別に「相続一般」を追加＝UNIT_CONFIG） |
| 送信台帳 purpose 案 | 通知: `souzoku_notice`＋`souzoku_koseki_complete:{案件No}`（1 回）／`souzoku_heirs_proposed:{run_id}`。相談者への「戸籍が揃いました」連絡は **進捗連絡の下書き**（Q4 承認・purpose=reply・actor=approved_draft） |
| RV-08／RV-10 | 封筒は「要確認」で止まる（確定・発送は人）。読解 JSON・人物は削除しない（統合・復元は person_merge/restore の既存規律）。通知は案件 No. と件数のみ |
| 再利用箇所 | hub/houki_koseki_checklist（KOSEKI_MATRIX・schema_ok・冪等）・hub/houki_shokumu_link＋hub/shokumu_plan＋hub/heir_envelope（提案封筒・`_unit_for_case` に相続一般を追加）・koseki_ingest／koseki_reader／koseki_person_sync／person_confirm／person_merge（R2〜R4・そのまま）・heir_derivation＋hub/derivation_models＋hub/heir_projection（P3-001〜003・App 36 関所）・kinship_graph／kinship_renderer（Z1/Z2）・koseki_coverage（KOSEKI-CHECK-1）・hub/webapp_kinship_view（関係図画面） |

## 4. 段階 ④: 遺産分割協議書案の生成

| 観点 | 内容 |
|---|---|
| 入力 | 確定相続人（App 36 戸籍確認済=yes・取消済み=no）・財産（App 35 有効=yes・評価確定の有無）・割付（App 37）・条項ライブラリ（clauses/v1・hub/clause_library） |
| 出力 | 財産目録（xlsx/docx・既存 ZAISAN-GEN）・遺産分割協議書「8割初稿」docx（souzoku-shorui/03）・生成通知（業務 LINE） |
| 人が判断する点 | 割付（誰が何を取得するか）の入力は人（合意形成は人のプロセス）・特殊条項（【弁護士記入】）・最終文言の全文確認・未成年／後見の特別代理人の要否 |
| AI が自動化する点 | 前文（当事者表示）・財産の特定（App 35 特定情報の差込）・取得条項（割付の取得区分→条項の対応・対応が無ければ生成停止）・一般条項・署名押印欄・割付整合検証（未割付／持分合計／代償対応）・下書き印（評価未確定行がある場合） |
| kintone アプリ案 | App S1 に `協議書ステータス`（作成／作成中／作成済／要確認・ZAISAN-GEN の状態機械と同型）・`協議書`(FILE)・`財産目録ステータス`／`財産目録`(FILE・App 26 の既存 field を S1 にも)。App 37 割付は既存のまま。条項マスタは **repo 内 YAML（P5-001 裁定）**で App 39 条項マスタは作らない |
| 送信台帳 purpose 案 | 通知: `souzoku_notice`＋`souzoku_kyogisho_generated:{案件No}:{revision}`。相談者連絡は進捗連絡の下書き |
| RV-08／RV-10 | 生成物は上書きせず版を重ねる（FILE 複数添付・旧版は残す）。協議書に載る氏名・住所は kintone の値のみ（AI が補完しない）。LLM は条項の文章化に使わない（差込のみ） |
| 再利用箇所 | zaisan_webhook＋units/souzoku/zaisan_mokuroku／zaisan_xlsx／zaisan_sync（財産目録・状態機械・検証）・hub/docx_builder.fill_template／fill_table_rows（可変行）・hub/clause_library（条項 YAML・placeholder parser）・docs/souzoku-shorui/03（生成ロジック・割付整合）・hub/webapp_souzoku_dashboard（案件単票・read-only） |

## 5. 段階 ⑤: 署名押印の進行管理

| 観点 | 内容 |
|---|---|
| 入力 | 協議書（確定版）・相続人一覧（住所・連絡先・印鑑証明の状態） |
| 出力 | 相続人ごとの発送レコード（App 30・送付案内チャネル・同封物=協議書／返信用封筒／説明文）・返送期限の自動設定・署名状況の台帳（App S1 SUBTABLE `署名状況`: 相続人レコードID／発送日／返送日／署名押印／印鑑証明受領）・期限超過の業務通知 |
| 人が判断する点 | 郵送方式（持ち回り署名か各別送付か）・催促の要否と文面（相談者以外の相続人への連絡は**全て人**）・不備（押印漏れ・印鑑相違）の判断 |
| AI が自動化する点 | 発送レコードの下書き起票（1 相続人 1 件・冪等キー `souzoku_sign:{案件No}:{相続人No}`）・返送期限（hub/return_deadline）・受領の反映（App 30 の返送待ち→完了 で署名状況を更新）・全員分の揃い判定→通知・期限超過の日次件数通知（HOUKI-OVERDUE の同型） |
| kintone アプリ案 | App S1 `署名状況` SUBTABLE（上記列）・`署名完了日`。App 30 は既存のまま（チャネル=送付案内・ユニット種別に相続一般） |
| 送信台帳 purpose 案 | 発送は App 30 の承認関所（送信台帳は LINE のみを扱うため対象外）。通知: `souzoku_notice`＋`souzoku_sign_complete:{案件No}`／`souzoku_sign_overdue:{JST日付}`。相談者（依頼者）への進捗連絡は下書き |
| RV-08／RV-10 | 相続人の住所・氏名は App 30 の宛先 field のみに書く（通知・ログは件数）。催促文は送らない（人が書く）。返送日は上書きせず履歴 |
| 再利用箇所 | channels/soufu_annai（送付案内 docx・manual_mailing）・hub/dispatch・hub/approval（承認関所・状態遷移）・hub/return_deadline（返送期限）・hub/houki_soufu（相続放棄 受理通知送付の 1 行 1 件起票の流儀）・hub/houki_overdue（期限超過の日次件数通知）・hub/address_label（宛名ラベル） |

## 6. 段階 ⑥: 相続税申告要否の一次判定

| 観点 | 内容 |
|---|---|
| 入力 | 法定相続人の数（App 36 確定分・放棄者も数に含める〔相続税法 15 条 2 項〕）・財産の評価額合計（App 35 有効行・評価確定の有無）・債務・葬儀費用（App 35 の債務種別）・生命保険等のみなし相続財産（申告） |
| 出力 | App S1 `相続税_一次判定`（要申告の可能性あり／基礎控除内の見込み／判定不能）・`相続税_判定根拠`（基礎控除額・課税価格見込み・未確定行の件数のみの固定語彙）・業務通知（要申告の可能性あり のときだけ） |
| 人が判断する点 | **判定の確定と税理士紹介は弁護士**（本機能は一次判定の提示のみ・相談者には機械の判定を送らない）。相続税評価額（路線価等）は固定資産税評価額と異なるため、不動産がある案件は常に「要税理士確認」の印 |
| AI が自動化する点 | 基礎控除額＝定数（3,000 万円＋600 万円×法定相続人の数・**hub/souzoku_constants に置き要大野確認**）・課税価格見込み＝有効財産の評価額合計−債務−葬儀費用＋みなし財産（申告値）・判定（見込み ≥ 基礎控除×安全係数〔定数・既定 0.8・要大野確認〕→ 要申告の可能性あり）・未確定行があれば「判定不能（下書き）」 |
| kintone アプリ案 | App S1 に `相続税_一次判定`（DROP_DOWN 3 値）・`相続税_判定根拠`（MULTI_LINE_TEXT・固定語彙）・`税理士紹介`（未／紹介済／不要・人） |
| 送信台帳 purpose 案 | 通知: `souzoku_notice`＋`souzoku_tax_flag:{案件No}:{判定版}` |
| RV-08／RV-10 | 判定は履歴を持つ（判定版）。相談者へは送らない（弁護士の説明の材料）。金額は App S1 の自分の案件欄にのみ書く |
| 再利用箇所 | units/souzoku/zaisan_mokuroku.fetch_zaisan_records（有効行の取得・小計）・heir_derivation（法定相続人の数の提示・放棄者の扱いは別途定数で）・hub/webapp_souzoku_dashboard（評価額合計の全件カーソル） |

## 7. 段階 ⑦: 不動産売却への引継ぎ

| 観点 | 内容 |
|---|---|
| 入力 | 協議書確定（取得者の決定）・不動産行（App 25 不動産 ＋ App 35 財産種別=不動産_*）・取得者（App 37 割付）・相続登記の状況（人） |
| 出力 | 売却引継ぎ書 docx（物件の特定情報・取得者・登記状況・連絡先の窓口・留意事項〔弁護士記入〕）・App 30 発送レコード（提携業者宛・要確認）・App S1 `売却引継ぎ`（未／引継ぎ済／対象外）・業務通知 |
| 人が判断する点 | 売却の要否（依頼者の意思）・業者の選定・引継ぎ時期（相続登記完了後か並行か）・個人情報の第三者提供の同意（**依頼者の同意記録が無ければ生成しない＝fail-closed**） |
| AI が自動化する点 | 不動産行と取得者の抽出・引継ぎ書の差込生成（hub/docx_builder）・同意記録と登記状況の前提チェック（HOUKI-TEISHUTSU-1 の提出前チェックの同型）・発送レコードの下書き起票 |
| kintone アプリ案 | App S1 に `売却引継ぎ`・`売却引継ぎ同意日`（人）・`売却引継ぎ書`(FILE)・`相続登記状況`（未／申請中／完了・人）。業者マスタは App 30 の宛先 field に直接（マスタ化は件数が増えてから） |
| 送信台帳 purpose 案 | 通知: `souzoku_notice`＋`souzoku_sale_handover:{案件No}`。業者への送付は App 30（承認関所） |
| RV-08／RV-10 | 同意日が無ければ生成しない。引継ぎ書に載せる個人情報は取得者の氏名・連絡窓口（事務所）に限る（依頼者の電話・LINE は載せない）。版を重ねる |
| 再利用箇所 | registry_reader／registry_ingest（登記事項証明の読解・App 25/35）・valuation_ingest（評価証明）・hub/docx_builder（送付状・引継ぎ書の素の段落＝HOUKI-TEISHUTSU-1 の送付状雛形の流儀）・channels/soufu_annai＋hub/dispatch（発送）・hub/houki_teishutsu.checklist（前提チェックの構造） |

## 8. 横断事項

### 8.1 既存コード（相続放棄 7 機能）の再利用マップ

| 相続放棄の機能 | 相続一般での使い方 | 変更の要否 |
|---|---|---|
| LINE ヒアリング（houki_bot・7 通台本・在庫注入・tool 記録） | ①②の質問票・聞取り | **台本と書込集合を案件アプリ別に差し替える抽象化**（`HOUKI_PROFILE` → `SOUZOKU_PROFILE`・hub/houki_profile の同型）。送信プリミティブは不変 |
| 相談カード読取（houki_card_read） | ②の来所カード | 既存 App 26 の `/scan` を維持し、App S1 への転記 adapter を足す |
| 申述書・代理人目録・事情説明書（docx 生成・SHA pin・凍結検証） | ④⑦の協議書・引継ぎ書 | 生成の型（雛形 SHA・構造検証・差込後の逐語一致）をそのまま使う。雛形は新規 |
| 戸籍チェックリスト（houki_koseki_checklist） | ③ | KOSEKI_MATRIX を相続一般用（配偶者＋子／直系尊属／兄弟姉妹・代襲）に拡張し、案件アプリを引数化 |
| 職務上請求接続（houki_shokumu_link・heir_envelope） | ③ | `_unit_for_case` に相続一般（App S1）を追加・提案封筒の件名を変える |
| 熟慮期間（houki_jukuryo）・期限超過（houki_overdue） | ⑤の返送期限超過・⑥の申告期限（10 か月）の目安通知 | 期限の算定関数を差し替え可能にする（3 か月→10 か月・起算日=死亡日）。通知の台帳方式はそのまま |
| 受理通知送付（houki_soufu・App 30 下書き起票） | ⑤の署名用送付・⑦の業者送付 | 1 行 1 件の起票と冪等キーの流儀をそのまま。同封物ブロック（App 32）に相続一般の項目を追加 |
| 家裁提出パッケージ（houki_teishutsu・提出前チェック・送付状） | ④の生成前チェック・⑦の引継ぎ前チェック | CheckItem の構造と「不足があればボタンを出さない」画面の型を流用 |

### 8.2 データの置き場（DB と kintone の分担）

- kintone が正: 案件（App S1）・相続人（App 36）・財産（App 35）・割付（App 37）・発送（App 30）・戸籍／人物（App 33/34）。
- DB（Postgres・alembic CLI のみ）: 送信台帳（既存）・案件脳の fact 台帳（既存 brain_ledger・locator 付き）・`souzoku_intake`（①の一時保管・migration 作成のみ）・
  `souzoku_sign_event`（⑤の署名イベント履歴・RV-08）・下書き（reply_draft・既存）。**kintone に書く前の値と履歴だけを DB に置く**（二重の正本を作らない）。

### 8.3 権限・承認

- 承認なしの対外発信ゼロ: 相談者への自動返信は ①の受付確認 1 通（要承認文言・台帳 key で 1 回）だけ。以後は全て Q4 の承認画面を通す下書き。
- 弁護士専権欄（コードは書かない）: 受任日・死亡日（確定）・戸籍確認済・連続戸籍充足・評価確定・協議書の確定・相続税判定の確定・売却引継ぎ同意日・相続登記状況。
- kintone の書込は「空欄のみ」「候補行のみ」「自動欄（状態・期限・判定の一次値）」の 3 種に限定し、各 module の `WRITE_FIELDS` 閉集合を test で pin する（相続放棄の流儀）。

### 8.4 RV-08／RV-10 の共通注意

- 相続人は依頼者以外の第三者を含む。**第三者の氏名・住所は通知・ログ・PWA 一覧に載せない**（案件 No. と件数・続柄の種類まで）。協議書・送付状など法的に必要な書面にのみ差し込む。
- 対象外（争いあり）の受付も消さない（status=対象外・理由は閉集合）。
- 判定（相続人・相続税）は版を持ち、再判定は上書きではなく新版（DerivationRun の run_id と同じ考え）。

## 9. 工数見積と実装順（票の切り方案）

| 票 ID（案） | 内容 | 依存 | 見積（PC-A 実装・テスト含む） |
|---|---|---|---|
| **SOUZOKU-2**（第 1 段階・G-5） | ①受付: 質問票 6 問・souzoku_intake 表（migration 作成のみ）・受付通知 1 通・受付確認 1 通（要承認）・flag `SOUZOKU_INTAKE_ENABLED` 既定 OFF・OFF で既存フローに影響ゼロ | 本書 | 1 票（0.5 日） |
| SOUZOKU-3 | App S1 の設計確定（欄名・選択肢の最終版）＋ EXPECTED_KINTONE_SCHEMA 登録＋ UNIT_CONFIG「相続一般」＋ souzoku_intake → App S1 転記（欄があるときだけ） | SOUZOKU-2・【人】App S1 作成 | 1 票（0.5 日） |
| SOUZOKU-4 | ②聞取り: SOUZOKU_PROFILE（台本 7 通・要承認）・空欄のみ書込・App 36/35 候補行の起票（冪等） | SOUZOKU-3 | 2 票（1.5 日） |
| SOUZOKU-5 | ③戸籍: 相続一般の KOSEKI_MATRIX・チェックリスト初期行・職務上請求の提案封筒（unit 追加）・連続戸籍充足の候補通知 | SOUZOKU-3 | 1 票（1 日） |
| SOUZOKU-6 | ③相続人確定の接続: 既存 R2〜R4・P3 系列を App S1 から辿れるようにする（ユニット種別・案件参照 4 点）・PWA の案件単票に相続一般を表示 | SOUZOKU-5 | 1 票（1 日） |
| SOUZOKU-7 | ④協議書「8割初稿」: 条項 YAML の実条項（**文面は大野**）・割付整合検証・docx 生成・協議書ステータスの状態機械（ZAISAN-GEN 同型）・財産目録の App S1 対応 | SOUZOKU-6・S2（fill_table_rows 済）・【人】条項文面 | 3 票（2.5 日） |
| SOUZOKU-8 | ⑤署名押印: 署名状況 SUBTABLE・App 30 下書き起票（1 相続人 1 件）・返送期限・揃い判定・超過の日次通知 | SOUZOKU-7 | 2 票（1.5 日） |
| SOUZOKU-9 | ⑥相続税一次判定: 定数（基礎控除・安全係数・要大野確認）・判定と根拠（固定語彙）・通知 | SOUZOKU-6（相続人数）・ZAISAN（評価額） | 1 票（0.5 日） |
| SOUZOKU-10 | ⑦売却引継ぎ: 前提チェック（同意・登記状況）・引継ぎ書 docx（素の段落・SHA pin）・App 30 下書き | SOUZOKU-7 | 1 票（1 日） |
| SOUZOKU-E2E | 通しテスト（①〜⑦・kintone/LINE 全モック・承認なし書込／送信ゼロ）＋引継ぎ書 | 全部 | 1 票（1 日） |

- 合計 14 票・約 11 日（PC-A の実装日数・大野の承認待ち時間を含まない）。各票は「migration 作成のみ・flag 既定 OFF・push／merge は大野」の連票規律。
- 先に大野の作業が要るもの: App S1 の作成（SOUZOKU-3 の前）・台本と受付確認文言の承認（SOUZOKU-2/4）・条項文面（SOUZOKU-7）・基礎控除等の定数確認（SOUZOKU-9）・LINE チャネルの選択（§10-1）。

## 10. 大野裁定が要る事項（本書では決めない）

1. **LINE チャネル**: 相続一般を既存の相続放棄チャネル（HOUKI_CHANNEL）に相乗りさせるか、第 3 のチャネルを作るか。相乗りなら友だち追加直後の用件選択（相続放棄／相続手続き）を追加する。
2. **案件アプリ**: 新設 App S1 か、既存「相談カード（相続）」（App 26）を案件アプリに昇格させるか（souzoku-shorui/README は昇格案）。本書は **新設**を推奨（来所カードの OCR 器と LINE 受付の案件を混ぜない・欄の追加が多い）。
3. App 35 `データ源` に「ヒアリング」が無い場合の CU 追加。
4. 争いの有無=ある のときの対応文言（「当事務所では扱っていない」）と、「わからない」の扱い（受任前相談へ進めるか）。
5. ⑤の催促・⑦の業者選定は人が行う前提でよいか（自動化しない）。
6. 相続税一次判定の定数（基礎控除 3,000 万円＋600 万円×人数・安全係数 0.8）と、放棄者を人数に含める扱い。
7. 署名押印の方式（持ち回り／各別送付）の既定値。

## 11. 用語

- App S1: 本書で新設を提案する「相続案件」アプリ（番号は作成時採番・env 注入）。App 33〜37 は実番号（docs/souzoku-build/README）。
- 「下書き」: Q4（/app/drafts）に積む承認待ちの相談者向け文面（JIKOU-PROGRESS-1 の仕組み）。承認なしでは送られない。
- 「候補行」: データ源=ヒアリング・確認フラグ=no で起票する kintone 行（確定は人）。
