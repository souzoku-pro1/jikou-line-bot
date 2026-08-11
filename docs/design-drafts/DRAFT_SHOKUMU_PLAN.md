# SHOKUMU-PLAN — 職務上請求の自動判定起票（必要戸籍 plan の提案封筒・**設計凍結 FROZEN**）

- **STATUS: 設計凍結（FROZEN・R-SHOKUMU-PLAN-D9 PASS・2026-08-11）**。
  以後の変更は**改定記録の追記＋再D巡・再凍結**による（暗黙の上書き禁止）。
  実装は本凍結仕様に基づく実装票で行う（コード/テスト実装は実装票まで禁止のまま）。
- TASK_ID: SHOKUMU-PLAN-D 設計票（設計凍結済み）／記録日 2026-08-10
  （fix1: R-SHOKUMU-PLAN-D1 の H01/H02/H03/M01〜M04 全所見反映・同日。改定は
  両時点残置——§1.6 追加実査・§2A〜§2C・§4-v2・§4A・§5/§6 追記・§8 改定記録／
  fix2: R-SHOKUMU-PLAN-D2 反映・2026-08-11——H01〔hash 材料補完+読値対応表 §4-v2.1〕・
  H02〔マトリクス 1:1 突合 §2A.2+type 実査 §2A.3〕・H03〔plan 由来 M1 の冪等 §4B〕・
  M01〔§2C(ii) 厳密化〕・M02〔§4A 相関制約〕・M03〔§2D 出所固定〕・M04〔§6 追加〕／
  fix3: R-SHOKUMU-PLAN-D3 反映・2026-08-11・**司令塔 4 裁定**——H01〔分離方式=
  plan_hash は正本内容のみ・§2C は起票時の非内容的フィルタ〕・H02〔束ね=自治体×
  対象者×様式・plan_idem 再定義・sibling_death 親エッジ共有規則〕・H03〔open 限定
  回収=却下非抑止の構造化・部分失敗は封筒 open 維持〕・M04〔3 裁定への同期〕・
  §8-3 改定記録／
  fix4: R-SHOKUMU-PLAN-D4 反映・2026-08-11・司令塔裁定——H01-01〔canonical 全候補
  保存・§2C フィルタは M1 create 直前のみ〕・H01-02〔plan 横断冪等キー=plan_hash
  除去・HIT 時 line_type 集合比較〕・M01〔§4-v2.1 保証文言の限定〕・M04-01〔§6
  同期 5 点〕・§8-4 改定記録／
  fix5: R-SHOKUMU-PLAN-D5 反映・2026-08-11・司令塔裁定=Codex 提案採用——H01
  〔m1_fingerprint 二段構え・plan_lines 比較撤回〕・M01〔canonical candidates の
  tie-break 全順序〕・§6 対照 4 形・§8-5 改定記録／
  fix6: R-SHOKUMU-PLAN-D6 反映・2026-08-11——H01〔fingerprint 材料の A/B 二層化・
  channel_json 完成形＝A層・正規化規則の完全固定〕・M01〔App31 snapshot 方式A・
  person_id 正式収載・完全順序と併合規則〕・§6 対照 9 形増補＋1:1 宣言・
  §8-6 改定記録／
  fix7: R-SHOKUMU-PLAN-D7 反映・2026-08-11・司令塔裁定——H01〔count 併合を
  channel_json 完成前へ・byte 一致不変条件〕・M01〔有効=コードポイント順〕・
  §6-44/46 増補＋1:1 宣言更新・§8-7 改定記録／
  fix8: R-SHOKUMU-PLAN-D8 反映・2026-08-11・最小同期——H02〔§2D 監査メタ閉集合へ
  m1_fingerprint を追加し §4B と同期・受入条件 6 点〕・§6-48/49 増補＋1:1 宣言
  更新・§8-8 改定記録）
- 盤面: 8月構想・項目1。判断部品はすべて既存——本票は**結線の設計**であり新エンジンを作らない。
- 実装現実の実査基盤（2026-08-10・read-only・main `6312112`）:
  `koseki_chain.py`（F5 判定）／`docs/souzoku-houki/10-koseki-matrix.md`（H系列③・凍結）／
  `dispatch_bot/shokumu.py`＋`channels/shokumu_seikyu.py`（M1・実物検収済み）／
  `hub/derivation_models.py`・`hub/heir_projection.py`（P3 系）／`review_resolve.py`（App33 参照・関所型）／
  `hub/heir_envelope.py`（封筒冪等の型）。
- 次レビュー: R-SHOKUMU-PLAN-D9（**凍結判定・H02 同期確認の軽量再走**。経緯: D1=凍結不適格→fix1→
  D2=H01/H02/H03+M01〜M04〔M01 のみ RESOLVED〕→fix2。対応は §8 改定記録）。

## 0. 原則（本票の背骨・§2 で構造化）

**機械は「提案まで」**——(i) 必要戸籍 plan の導出と不足の提示は機械
(ii) 請求の起票確定は[人]（App30 要確認封筒の関所） (iii) 対外送信は M1 既存の
承認フロー（App30 承認待ち→承認済は弁護士の kintone 操作＝hub/approval.py の
絶対制約・本票はこれに一切触れない）。3 段のどれも省略しない。

## 1. 実装現実の実査（実物逐語・結線点の確認）

### 1.1 koseki_chain の F5 判定（入出力）

- 位置づけ（koseki_chain.py:5-10 逐語）:
  > - 出力は**収集見込み（弁護士確認前）の参考判定**に留める（01 §4・04 §1）。
  >   OCR誤読でリンクが切れる実例（「鹿浜三丁目12」vs「鹿浜三丁目1261番地」）が
  >   あるため、機械判定を戸籍収集完了の確定に使わない。
  >   相続順位エンジンは F5（兄弟姉妹相続の収集不足）の保留理由の提示に使う
- 入力: App33 レコード（GET 形・`読解JSON` 列）または読解 JSON dict の list
  （`_reading` が両形を受ける・koseki_chain.py:18-27）。**kintone 呼出しなしの純関数**。
- 出力（`assess_chain`・:65-74）: `{"リンク": [...], "未収集": [{"本籍","筆頭者"}...],
  "注記": "収集見込み（弁護士確認前・…）"}`——**未収集 = 従前戸籍の記載があるのに
  収集済み戸籍にリンクできないもの（＝取得候補）**。
- 順位別（`assess_for_rank`・:76-87）: rank=1「出生から死亡までの連続」／rank=3
  「加えて父母それぞれの出生までの連続」の必要範囲文言を付す。

### 1.2 H系列③ 続柄別戸籍マトリクス（10-koseki-matrix.md・凍結）

- §1 全類型共通・絶対必須（逐語・最優先）:
  > | **被相続人の住民票除票（または戸籍の附票）** | **管轄家裁（最後の住所地）の
  >   確定に必要**（09 §2.3 の管轄マッチの入力）。続柄に関わらず必ず取得 | …
  >   （書類収集ループの最初の請求対象。管轄が確定しないと申述先が決まらないため最優先） |
- §2 続柄別セット: 子=除籍謄本(死亡記載)+申述人現在戸籍／親=出生〜死亡連続+現在戸籍／
  兄弟姉妹=出生〜死亡連続+両親双方の死亡確認戸籍+現在戸籍／甥姪(代襲)=兄弟姉妹セット
  全部+親の死亡を証する戸籍。
- §2 の不可侵注記（逐語）:
  > - **先順位者の放棄による順位繰上りの場合**（複雑性フラグ「先順位放棄」）は、
  >   マトリクスによる自動導出を行わず**弁護士が必要書類を個別確定**する
  > - マトリクスは config のデータ（dict）として持ち、家裁の運用差・弁護士の方針変更は
  >   データ修正のみで反映（コード変更なし）
- §4 M1 反復ループ（逐語・連鎖の承認規律）:
  > （「次は○○市の戸籍を請求します。よろしいですか」を承認キューに積む。
  > **連鎖の各請求も職務上請求＝対外発信のため承認必須**）

### 1.3 M1（職務上請求）の起票入力（dispatch_bot/shokumu.py docstring 逐語）

> parse_channel_data が要求（欠けるとエラー遷移）:
>   - request_items: 1件以上・type ∈ 対応6種別・count は1以上の整数
> find_municipality が要求:
>   - municipality（または宛先名）… 未指定はエラー遷移。App 31 未登録は PrepareDeferred
> build_request_form_pdfs が要求:
>   - 様式1（戸籍謄本・除籍謄本・改製原戸籍を含む場合）: target.生年月日 **必須**
>   - 様式2のみ（住民票・除票・附票）: 生年月日は任意（空欄なら非印字）
> …
>   - purpose … 未指定時は**ユニット種別ごとの確定文言**（PURPOSE_BY_UNIT・
>     2026-07-04 弁護士判断）を使用。

- ＝ plan から M1 へ渡す最小完全集合は **request_items（種別×通数）＋municipality＋
  target（対象者・様式1なら生年月日）＋unit（purpose 解決用）**。宛先引当て（App31
  市区町村マスタ）・料金（FEE_FIELD_BY_TYPE／compute_kogawase）・様式生成は M1 既存の
  まま（本票は入力を作るだけ）。

### 1.4 DerivationRun／App36 と収集済み戸籍の突合方法

- **必要人物と続柄**: head run（`get_current_head`・P3-001 正規経路の read）の
  `result_payload.heirs[]`＝`{person_id, zokugara_code, share}`（P3-001 改定で
  zokugara_code は凍結 9 値 enum）。rank は run.rank。**確定済みの正本は App36**
  （confirmed decision 後の projection・P3-003B §4A）——plan の入力にどちらを使うかは
  裁定①（§5）。
- **収集済み戸籍**: App33（`APP_KOSEKI_BOOK`・review_resolve.py:43-44）を
  `案件レコードID = "<case>"` で検索し `読解JSON` 列を F5（1.1 の入力形そのまま）へ
  渡す（koseki_ingest.py:160 が `読解JSON` に構造化読解を保存済み・:170 が案件参照を保持）。
- **対象者の実データ**: App34（人物・derive 経路と同じ read）から氏名・生年月日
  （身分事項の出生行）を引く——M1 target の材料。

### 1.5 封筒・関所の型（流用元）

- 起票: `hub/heir_envelope` の型——App30 要確認封筒・チャネル固有データのトップキー
  方式・`冪等キー` 平文保持＋`find_existing` 完全一致検索の二重起票ガード・単票 API 必須。
- 関所: `review_resolve.RESOLVERS` へのトップキー追加＋T2 既存フロー（復唱→30分
  pending→OK 単回・decided_by 伝搬）。P3-003c で decision 種別伝搬も確立済み。

### 1.6 追加実査（fix1・H02/H03/M03 の根拠）

- **M1 の App30 起票 fields**（dispatch_bot/app30_filer.py `_fields_shokumu_seikyu`
  :74-88 逐語）:
  > チャネル固有JSONは channels/shokumu_seikyu.parse_channel_data が通る形式
  > （request_items/municipality/target/purpose・04 §2）＋監査メタ併記。
  > 宛先名は空で起票する（prepare が App 31 から施設名で解決して書き戻す）
  fields = `チャネル="職務上請求"`・`件名=f"職務上請求（{customer}・{municipality}）"`・
  `宛先名=""`・`チャネル固有データ=json({**channel_json, **監査メタ})`＋共通部
  （file_from_pending :115-123: `発送ステータス="下書き"`・ユニット種別・顧客名表示用・
  案件アプリID・案件レコードID・`実行済み="no"`・**単票 API 必須** :124-127）。
  channel JSON の組立は `dispatch_bot.shokumu.build_channel_json(parsed)`。
- **App31 照合**（channels/shokumu_seikyu.py `find_municipality` :473-488 逐語）:
  > キー: チャネル固有データ municipality → 無ければ 宛先名。
  > 未登録は PrepareDeferred（登録依頼警報・状態は変えない）
  検索は `市区町村名 = "<name>" and 有効 in ("yes")` の**完全一致**。
  **本籍・住所文字列から市区町村名を切り出す既存関数は存在しない**（実査結果・
  `municipality_office_name` は名称→施設名変換のみ）→ §2A で規則を定義。
- **除票の請求先の正本候補**: App34（人物）に `住所最新`・`本籍最新`
  （SINGLE_LINE_TEXT・EXPECTED_KINTONE_SCHEMA 実査）が存在。被相続人行の
  `住所最新`（最後の住所地）を住民票除票の請求先の第一材料とする。**空の場合は
  「要入力」提示**（推測禁止・道案内型・H03）。

## 2. 設計骨子（一気通貫・機械は提案まで）

```
[1] plan 生成（機械・read-only）
    head run（or App36・裁定①）→ 続柄別マトリクス（§1.2・config データ参照）で
    必要書類セットを展開 → App33 収集済み戸籍と F5（assess_for_rank）で突合 →
    不足リスト＝請求候補（除票/附票は常に最優先行・収集済みなら充足表示）
[2] 提案封筒（機械・App30 起票=対外効果ゼロ）
    App30 要確認封筒（トップキー "shokumu_plan"）へ「請求提案 N 件」を起票。
    detail = {case_record_id, plan_hash, 材料参照（run_id/App33 集合 hash）,
    請求候補（municipality 候補・種別・通数・様式1/2 区分・対象 person_id）}
    ＝**F5 の注記（収集見込み・弁護士確認前）を封筒とその表示に必ず引き継ぐ**
[3] 関所確定（[人]・T2 語彙）
    「請求案を確定して」→ 復唱（請求先・種別・通数の列挙）→ OK →
    _resolve_shokumu_plan（RESOLVERS 追加）が確定処理
[4] M1 結線（既存経路・機械は下書きまで）
    確定された請求候補ごとに、M1 の App30 起票（channels/shokumu_seikyu の
    チャネル固有 JSON＝request_items/municipality/target/purpose(unit)）を
    **既存の単票起票 API で作成**→ 既存 prepare（宛先引当て・小為替計算・様式
    重ね打ち PDF）→ **承認待ち**で停止（＝全成果物下書き止まり）
[5] 発送承認（[人]・既存のまま）
    App30 承認待ち→承認済（弁護士の kintone 操作・絶対制約）→ 既存発送フロー
```

- [1]-[2] は confirmed handler 同様 **1 行でも判定不能なら全体を要確認へ倒す**
  fail-closed。plan の中身は「提案」であることを応答・封筒・画面文言で常時明示
  （F5 の「収集見込み」注記の写像＝白を確定と誤認させない）。
- [4] は **M1 を呼ぶだけ**（様式・料金・宛先のロジックを一切複製しない）。App31
  未登録の municipality は M1 既存の PrepareDeferred 挙動へ委ねる（先回りしない）。
- 連鎖ループ（受領→読解→次請求・§1.2 の H10）は**本票スコープ外**——本票は
  「現時点の不足に対する 1 巡分の提案」まで（§7・裁定④）。

## 2A. マトリクス行→M1 入力の写像表（fix1 H03・fix2 H02 で完全化）

（fix2 H02: 凍結マトリクスとの 1:1 突合〔§2A.2〕により**欠落 2 行を追加**・
F5 不足行の request_type は type 実査〔§2A.3〕により**要入力へ改定**——初版の
「除籍謄本／改製原戸籍」記載は撤回・両時点残置は §8 fix2）

| 行類型（enum・§4A） | request_items.type | 様式 | target の person_id 選定規則 | municipality の材料 |
|---|---|---|---|---|
| `joh_removed`: 住民票除票（共通） | 住民票の除票 | 様式2（生年月日任意） | **被相続人**＝App34 `被相続人フラグ=yes` 行（第二段では head run.decedent_person_id と一致検証） | 被相続人の App34 `住所最新`（§1.6）→ §2A.1 切り出し |
| `fuhyo`: 戸籍の附票（除票不能時の切替・共通） | 戸籍の附票 | 様式2 | 同上 | 被相続人の App34 `本籍最新` → §2A.1 切り出し |
| `decedent_joseki`: 被相続人の除籍謄本〔死亡記載〕（**fix2 追加・子セット**） | 除籍謄本 | 様式1（**生年月日必須**） | 被相続人 | 被相続人の `本籍最新` → §2A.1 切り出し |
| `chain_missing`: 出生〜死亡連続戸籍の F5 不足分 | **要入力（fix2 改定・§2A.3）** | 様式1（**生年月日必須**） | 被相続人 | F5 `未収集[].本籍` → §2A.1 切り出し |
| `parents_death`: 両親双方の死亡確認戸籍（兄弟姉妹系） | 除籍謄本 | 様式1 | **父・母**＝被相続人行の App34 `父人物ID`／`母人物ID`（不在は「要入力」） | 当該親の `本籍最新`（空なら「要入力」） |
| `sibling_death`: 申述人の親（兄弟姉妹）の死亡を証する戸籍（**fix2 追加・甥姪セット**） | 除籍謄本 | 様式1 | **当該兄弟姉妹**＝甥姪 heirs 行から App34 `父人物ID`/`母人物ID` で遡って特定（特定不能・複数解釈は「要入力」） | 当該兄弟姉妹の `本籍最新`（空なら「要入力」） |
| `applicant_current`: 申述人の現在戸籍 | 戸籍謄本 | 様式1 | **常に person_id null＋input_required**（裁定⑦=(C) の機械的固定・§4A 相関制約） | 「要入力」固定 |

### 2A.2 凍結マトリクスとの 1:1 突合表（fix2 H02）

| H系列③ §1/§2 の凍結行 | 写像表の行類型 | 突合 |
|---|---|---|
| §1 住民票除票（または戸籍の附票）——全類型共通・最優先 | `joh_removed`／`fuhyo` | 1:1 |
| §2 子: 被相続人の除籍謄本（死亡記載） | `decedent_joseki` | 1:1（**初版欠落→fix2 補完**） |
| §2 各類型: 申述人の現在戸籍 | `applicant_current` | 1:1 |
| §2 親・兄弟姉妹・甥姪: 出生から死亡までの連続戸籍 | `chain_missing`（F5 突合で不足分のみ） | 1:1 |
| §2 兄弟姉妹・甥姪: 両親双方の死亡が確認できる戸籍 | `parents_death` | 1:1 |
| §2 甥姪: 申述人の親（被相続人の兄弟姉妹）の死亡を証する戸籍 | `sibling_death` | 1:1（**初版欠落→fix2 補完**） |
| §2 注記: 先順位放棄（複雑性フラグ） | 展開なし＝共通行のみ+個別確定警報（§3-2） | 1:1 |

実装はマトリクス config データと本表の**機械照合テスト**（§6-13）で凍結——行の
増減はマトリクス改定（[人]・データ修正）と同時にのみ起きる。

### 2A.3 request_items type の実査と F5 不足行の扱い（fix2 H02・司令塔裁定の適用）

- M1 の type 閉集合（channels/shokumu_seikyu.py `FEE_FIELD_BY_TYPE` :53-60 実査）:
  **`戸籍謄本・除籍謄本・改製原戸籍・戸籍の附票・住民票・住民票の除票` の 6 種のみ**。
  `FORM_BY_TYPE`（:92-95）も同 6 種で閉じ、**「連続戸籍一式」「出生から死亡まで」
  相当の複合 type は存在しない**。
- → 裁定どおり **F5 不足行の request_type は初版「要入力」に倒す**——不足戸籍が
  除籍謄本か改製原戸籍かは取得済み戸籍の記載実態（改製/転籍の別）に依存し機械が
  確定できない。誤 type の請求書印字より道案内が安全。**複合 type の新設は別票**
  （M1 の様式・料金・丸位置の改定を伴うため本票では行わない）。

- **生年月日（様式1 必須）**: 対象 person の App34 身分事項・出生行の和暦
  （kinship_graph `_first_event_date(events,"出生")` と同一規則）。**取得不能は
  「要入力」提示**（推測・空欄印字への先回りをしない＝道案内型・司令塔裁定どおり）。
- **束ね規則（初版「同一自治体」→fix3 H02 で正規化・両時点残置）**: 束ねてよいのは
  **同一自治体×同一対象者（person_id）×同一様式**の候補のみ（1 つの M1 起票=
  request_items 複数行・M1 が複数 items を受ける実装事実 §1.6 による）。
  **異なる人物・異なる様式は別 M1 起票**——職務上請求の target・様式は 1 起票
  1 対象のため（初版の「自治体のみ」の束ねは対象者の異なる行を混載し得た＝撤回）。
- **sibling_death の親選定の正規規則（fix3 H02 で凍結）**: 甥姪 heir（申述人側）の
  App34 `父人物ID`/`母人物ID` のうち、**被相続人と父母（親エッジ）を共有する側**を
  被相続人の兄弟姉妹＝対象者とする（App34 親エッジの共有判定・確定的 ID 参照のみ・
  氏名照合は使わない）。共有判定不成立・両親とも判定不能・複数解釈は
  **「要入力」**（推測しない）。

### 2A.1 市区町村名の切り出し規則（新規定義・既存関数なし＝§1.6 実査）

- 入力: 住所/本籍の全文文字列。出力: App31 `市区町村名` 完全一致検索に使う名称。
- 規則（閉じた文法・実装票で pin）: 先頭の都道府県名（`…都|…道|…府|…県`・
  任意）を除去した後、**最初の `市`・`区`・`町`・`村` 終端までの最短一致**を
  切り出す。政令指定都市（「○○市△△区」）は**市までで切らず区まで含める第二候補も
  生成**し、App31 照合は「区まで」→「市まで」の順に試す（**登録粒度は App31 の
  運用データ依存＝§5 裁定⑧**）。
- **切り出し失敗・両候補とも App31 未登録・複数解釈**は municipality を
  **「要入力」提示**（plan 候補行に理由付きで残す・機械は推測しない）。
  App31 未登録そのものの救済は M1 既存の PrepareDeferred（登録依頼警報）に一本化。

## 2B. 第二段（続柄別セット）の入力条件（fix1 M01・6 条件で凍結）

第二段の plan 生成は以下 **6 条件をすべて充足**した場合のみ（1 つでも欠ければ
続柄別セットは生成せず「要確認（条件未充足の列挙＝道案内）」）:

1. head run が**ちょうど 1 件**存在（get_current_head 非 None）。
2. head run の有効 leaf decision が **confirmed**（P3-003c の leaf 判定を流用）。
3. App36 の当該案件全行の `current_derivation_run_id` が **head run.id と一致**。
4. App36 当該全行の `戸籍確認済` が **yes**。
5. App36 行の `導出元人物ID` 集合と続柄が **head run result_payload.heirs の
   person_id 集合・zokugara_code 写像と一致**。
6. 冪等キー重複・欠落・余剰行の**いずれかがあれば全体要確認**（部分生成しない）。

## 2C. 二段の重複排除（fix1 H02・照合方式(a)・**fix3 H01: 非内容的フィルタとして分離**）

（fix3 H01・司令塔裁定=分離方式）本節の照合は **plan_hash 確定後・起票時に適用する
非内容的フィルタ**である——App33 充足は候補の `status="fulfilled"` 化（内容側＝
plan_hash の (4) に反映）だが、**App30 の封筒・M1 起票状態の照合は plan_hash の
材料でない**（§4-v2）。既起票の吸収は最終的に **§4B の plan_idem 完全一致**の責務
（本節は提案ノイズを減らす前段フィルタ・取り零しても plan_idem が二重起票を止める）。

- **phase 識別**: plan 封筒 detail に `phase ∈ {"common", "full"}` を持つ
  （common=第一段・除票/附票のみ／full=第二段・続柄別セット込み）。
- **第二段生成時の共通行の扱い**（重複排除・照合方式(a)）:
  1. App33 収集済み照合——除票/附票が**取得済み**（App33 に該当読解あり）なら
     共通行は「充足」表示のみ（請求候補にしない）。
  2. App30 照合——未完了の共通行が既に飛んでいないかを **2 面**で照合:
     (i) **plan 封筒**: トップキー `shokumu_plan`＋`案件レコードID=case`＋
     detail.phase="common"＋発送ステータス=要確認（未クローズ）
     (ii) **M1 起票**（fix2 M01 で厳密化）: `チャネル="職務上請求"`＋
     `案件レコードID=case`＋発送ステータスが terminal（完了/エラー）以外で
     候補を絞った上で、**チャネル固有データを JSON parse して照合**——
     (a) `request_items[].type` に 住民票の除票/戸籍の附票 を含む、かつ
     (b) `target` の対象 person（被相続人）または **M1 冪等キー（§4B・
     `plan_idem` の case+municipality+様式 部分〔fix3 H02 のキー形〕）**が一致、
     かつ (c) `municipality` が
     当該候補行と一致——の 3 点一致のみを「起票済み」とみなす（like 文字列
     一致の誤爆〔件名・purpose 内の語など〕を排除。**parse 不能な既存レコードは
     照合不成立＝安全側で「未起票扱い」**にせず**要確認へ倒す**——壊れ JSON の
     存在自体が異常のため）。
  3. **フィルタの適用時点（fix3「除外」→fix4 H01-01 で改定・両時点残置）**:
     - （履歴・fix3 まで）「該当すれば第二段 plan から共通行を**除外**」——**撤回**:
       封筒生成時に候補を削除すると candidates が plan_hash（正本内容）と 1:N に
       なり、状態反転（例: common 封筒を却下した後に full を確定）で共通行を
       復元できない。
     - **fix4 正**: 封筒 candidates は**未フィルタの canonical 全候補**
       （plan_hash と 1:1・§4A）を保存する。本節の照合は **M1 create 直前の
       実行時にのみ適用**し、該当候補を**skip**（起票しない）——封筒・復唱の
       表示では「起票済み/取得済み」の注記を付してよい（候補自体は削除しない）。
     - **状態反転の復元**: common 封筒却下→full 確定の場合、full の candidates に
       共通行が保持されているため、確定時点の実行時フィルタで「起票済みでない」と
       判定されれば**自然に復元起票される**（§6-31）——生成時除外ではこの復元が
       構造的に不可能だった。
  4. **却下の非抑止（fix2 M01→fix3 H03 で構造化・両時点残置）**:
     - （履歴・fix2）「同一材料での再指示は封筒冪等（already_filed）で同一封筒に
       回収」——**撤回**: 却下＝封筒クローズ後は同一封筒への回収が[人]の再オープン
       不能（HUMAN_TRANSITIONS に完了→要確認なし）と矛盾し、却下が事実上の
       恒久抑止になっていた。
     - **fix3 正**: 封筒冪等（find_existing 型照合）の**回収対象を open
       （発送ステータス=要確認 かつ 実行済み=no）封筒に限定**する。同一冪等キーの
       封筒が **terminal（完了等）しか無ければ新規起票**する——却下後の再指示は
       新しい封筒として再提案される＝**却下=非抑止が構造的に成立**
       （却下履歴の記録・抑止は本票では持たない）。
- **第一段未実施案件の取りこぼし防止**: 上記照合で**共通行が存在しない**
  （収集済みでも起票済みでもない）場合、第二段 plan に**共通行を含める**
  （第一段の実施有無に依存しない＝漏れゼロの規則）。

## 2D. M1 結線の具体呼出し先（fix1 M03・実査で固定）

- **channel JSON の組立**: `dispatch_bot.shokumu.build_channel_json(parsed)` を
  そのまま呼ぶ（parsed.task_params に request_items/municipality/target/unit を
  詰める＝§1.3 の最小完全集合。purpose は渡さず PURPOSE_BY_UNIT 解決に委ねる）。
- **App30 初期 fields の組立責務**: plan 確定ハンドラが
  `_fields_shokumu_seikyu`（§1.6）と**同一の field 集合**を組み立てて
  `hub.kintone.create_record`（**単票 API**）で起票する。
  `file_from_pending` は Pending（LINE 指示）前提のため直接は呼ばず、
  **同一 field 集合の byte 水準の一致をテストで pin**（§6-12）——将来
  `_fields_shokumu_seikyu` が公開ヘルパ化されれば置換（実装票判断・挙動同一）。
- **fields 値の出所の固定（fix2 M03）**:
  - `顧客名表示用`: **App21/26 案件レコードの `顧客名` を確定時に再取得**した値
    （file_from_pending :110-111 と同一の正本・plan 封筒には保存しない）。
  - `案件アプリID`: **env 参照**——相続ユニットのため `SOUZOKU_KINTONE_APP_ID`
    （file_from_pending の `KINTONE_APP_ID` は時効ユニット向けの同型・ユニット別
    env の選択は unit から解決）。
  - `ユニット種別`: 案件由来（heir_envelope `_unit_for_case` と同一写像）。
  - **plan 経路の監査メタ構造（fix3→fix8 で §4B と同期・両時点残置）**:
    - （履歴・fix3〜fix7）旧閉集合は `m1_fingerprint` を含まなかった——**fix8 で
      同期**（fix5/6 が §4B 側へ導入した保存キーの §2D への反映漏れ＝D8 H02）。
    - **fix8 正（閉集合）**:
      ```json
      {
        "filed_by": "shokumu_plan",
        "plan_envelope_no": "<App30 record_id>",
        "plan_hash": "<hex64>",
        "plan_idem": "<§4B のキー>",
        "m1_fingerprint": "<hex64>",
        "plan_lines": ["<line_type>", "..."]
      }
      ```
    ——`_audit_meta`（command_id 基点）とはキーを分ける（plan 起票は LINE 指示
    command でなく封筒確定が起点のため。閉集合・値域は §4B で pin）。
    **受入条件（fix8・実装票の必須要件）**:
    1. 監査メタのキー閉集合に **`m1_fingerprint` が存在**する。
    2. grammar は `^[0-9a-f]{64}$`。
    3. **App30 create 時に保存**する（起票と同時・後書きしない）。
    4. plan_idem HIT 時の再照合は**既存保存値を JSON parse して完全一致比較**。
    5. **欠落・不正 grammar・parse 不能は skip 回収せず要確認へ落とす**（安全側——
       比較不能な既存下書きを「一致」扱いにしない）。
    6. **§2D と §4B の監査メタキー集合が byte 水準で一致**する構造 pin（§6-48）。
  - **`_fields_shokumu_seikyu` 変更時の同期方式**: 実装票で
    **共用関数化を第一候補**（`_fields_shokumu_seikyu` を「channel_json＋監査メタを
    引数に取る公開ヘルパ」へ挙動同一で抽出し両経路が呼ぶ）。抽出を見送る場合は
    **byte 一致 pin（§6-12）が変更時に必ず割れる**ことをもって同期を強制する
    （どちらでも「二重定義の黙った乖離」は構造的に起きない）。
- 起票後は既存経路のまま: App30「レコード追加」Webhook → /hub/dispatch →
  `channels/shokumu_seikyu.prepare`（`parse_channel_data`→`find_municipality`→
  様式生成→**承認待ち**）。plan 側は prepare に一切関与しない。

## 3. 凍結正本との整合（設計上の不可侵・実装票へ逐語で引き継ぐ）

1. **H系列③マトリクスの凍結**: 続柄別セット・最優先=住民票除票（または戸籍の附票）は
   §1.2 の逐語のまま。実装は「config のデータ（dict）」として持つ（正本の指定・
   コード変更なしで方針変更を反映）。**セットの内容を本票・実装票が改変しない**。
2. **先順位放棄（複雑性フラグ）**: マトリクス自動導出を行わず共通の除票行のみ提案＋
   「必要書類の個別確定が必要です」警報（§1.2 逐語のまま・機械は個別判断しない）。
3. **purpose 文言**: `PURPOSE_BY_UNIT`（ユニット別確定・2026-07-04 弁護士判断）を
   そのまま使用。**plan 側で purpose を生成・改変しない**（unit を渡すだけ）。
4. **料金・期日**: `FEE_FIELD_BY_TYPE`／`compute_kogawase`／収集見込み日数の扱いは
   M1・正本の既存定義のまま（plan は参照もしない——料金計算は M1 prepare の責務）。
5. **全成果物下書き止まり**: plan 封筒（要確認）→ M1 起票（承認待ち）のどこにも
   自動送信・自動承認の経路を作らない。承認済への遷移をサーバに作らない絶対制約
   （hub/approval.py:5-9）は本票の全段に適用。

## 4. 冪等・訂正（二重起票防止と上流訂正）

- **封筒冪等キー**: `shokumu_plan:{case_record_id}:{plan_hash}`（heir_envelope の
  `冪等キー` 平文＋`find_existing` 完全一致の型を流用・二重起票ガード 2 層）。
- **plan_hash（初版・fix1 H01 で §4-v2 へ具体化＝両時点残置）**: plan の**材料**の
  正規化 hash——(i) 入力正本の id（head run.id または App36 行集合・裁定①に従う）
  (ii) App33 収集済み戸籍の record_id＋読解 hash のソート列 (iii) マトリクス version
  （config データの版）。**提案内容でなく材料を hash する**＝同一材料からの再生成は
  同一 plan（already_filed 回収）・材料が変われば別 plan（新封筒）。

### 4-v2. plan_hash 材料の具体定義（fix1 H01・stale 保証と 1:1）

**plan_hash の定義（fix3 H01・司令塔裁定=分離方式で確定）**: plan_hash は
**「正本内容」のみ**——下記 (1)〜(7) の正規化 JSON（sort_keys・ensure_ascii なし）の
SHA-256。**App30/M1 の起票・封筒状態（§2C が読む非内容的状態）は材料に含めない**
——それらは「候補集合の内容」でなく「今起票すべきか」のフィルタであり、
§2C（起票時フィルタ）と §4B（plan_idem 完全一致）が吸収する（§8-3）:

1. **入力正本の id**: phase="full" は head run.id（数字）／phase="common" は
   `null`（run 非依存・裁定①(C)）。
2. **App34 使用 field snapshot hash**（fix1 H01→fix2 で補完）: 候補生成・M1 入力に
   **実際に使用した** person 行を **person_id を JSON 構成要素として明文化**した形
   `{"<person_id>": {field: 値, ...}, ...}` で person_id 昇順に並べた正規化 JSON の
   SHA-256。使用 field の閉集合（fix2 で 2 field 追加）＝
   `{"氏名", "住所最新", "本籍最新", "死亡日", "父人物ID", "母人物ID",
   "身分事項.出生行の年月日", "被相続人フラグ"}`（§2A の写像・§2B/§2A の
   被相続人特定が読む field と**同一集合**・これ以外を hash に入れない）。
3. **App36 行集合 hash**（phase="full" のみ・fix1 H01 の具体化）: 当該案件の
   App36 全行の `($id, $revision, current_derivation_run_id, 導出元人物ID)` を
   $id 昇順で並べた正規化 JSON の SHA-256。
4. **App33 収集済み集合**: record_id＋読解JSON の SHA-256 の $id 昇順ソート列。
5. **マトリクス version**（config データの版数文字列）。
6. **head.decedent_person_id**（fix2 追加・phase="full"）: 被相続人の同一性を
   run 側からも固定（App34 の被相続人フラグ付替えと run の不整合を検出）。
7. **App31 照合 snapshot**（fix2 追加・fix6 M01 で行構造と順序を確定＝方式A）:
   候補行ごとの `{"line_type": ..., "person_id": <数字列 or "">,
   "app31_record_id": ..., "市区町村名": ..., "有効": ...,
   "fallback": "ward"|"city"|""}`——**person_id を行へ正式収載**（fix6・同一
   line_type で対象者が異なる行の順序を一意化）。「要入力」行は照合系 3 キー
   （app31_record_id/市区町村名/有効）を `""`・fallback を `""` とする
   （照合を行っていない事実ごと固定・null 非使用は §4B fix6 と同一規則）。
   **完全順序（tie-break・fix6）**: `line_type` 定義順 → `person_id`
   （"" 先頭・数値昇順）→ `市区町村名`（コードポイント昇順）→
   `app31_record_id`（"" 先頭・数値昇順）→ `有効`（**fix7 で比較規則を補完**——
   保存文字列の **UTF-8 コードポイント昇順・"" 先頭**＝市区町村名と同規則・
   値域に依存しない機械的順序。値域 grammar〔`yes|no|""` 等〕を保存境界で固定する
   場合もあり得るが、**順序の一意性はコードポイント順で担保**する。fix6 の
   順序未指定は撤回）→ `fallback` 定義順
   （ward→city→""）。**全キー完全一致の行は 1 行へ併合する**（併合あり・一意）。
   **snapshot に存在しないキーを比較・順序決定に使わない**（構造 pin・§6-47——
   candidates 側のみにある status/count 等を snapshot の同一性判定へ持ち込まない）。
   正規化 JSON（§4B fix6 と同一規則）の SHA-256。

### 4-v2.1 「§2A/§2B が読む値」↔「hash 材料」対応表（fix2 H01・1:1 の機械的提示）

| plan/M1 入力が読む値 | 読む場所 | hash 材料 |
|---|---|---|
| person_id（対象者の同一性） | §2A target 選定 | (2) の JSON キー＋(6) |
| 氏名・生年月日（出生行）・死亡日 | §2A target／様式1 | (2) |
| 住所最新・本籍最新 | §2A municipality 材料 | (2) |
| 父人物ID・母人物ID | §2A `parents_death`/`sibling_death` | (2) |
| 被相続人フラグ | §2A 被相続人特定 | (2)〔fix2 追加〕 |
| head run の同一性・heirs/zokugara | §2B 条件 1-2-5 | (1)＋(3)（$revision 経由） |
| head の被相続人 | §2B・§2A 一致検証 | (6)〔fix2 追加〕 |
| App36 行の run 一致・確認状態 | §2B 条件 3-4-6 | (3) |
| App33 収集済み（F5 入力） | §2A `chain_missing`・§2C 充足判定 | (4) |
| マトリクス行類型・セット | §2A/§2A.2 | (5) |
| App31 照合結果（宛先の実引当て） | §2A.1・M1 municipality | (7)〔fix2 追加〕 |

→ **本表の主張の範囲（fix3 H01 で限定訂正）**: 「**候補集合の同一性を決める
読取値**で hash 材料に載らないものは存在しない」（本表が §6-12 系テストの正）。
以下は**意図的に hash 対象外**（対応表の対象外＝stale 検出を要しない根拠つき）:
- **§2C が読む App30/M1 の起票・封筒状態**——候補の内容を変えず「今回起票するか」
  だけを変える非内容的状態。変化は plan_hash 不変のまま**起票時フィルタ**（§2C）と
  **plan_idem 完全一致**（§4B）が吸収する。
- **§2D の確定時再取得値**（顧客名表示用・案件アプリID 等の App30 fields 材料）——
  **常に確定時点の最新を読む**設計（封筒へ保存しない・M02）のため snapshot が
  存在せず、stale 検出の対象になり得ない（古い値が使われる経路がない）。
App31 レコードの更新・無効化（(7)）・被相続人フラグ付替え（(2)/(6)）は候補集合の
内容を変えるため plan_hash を変え、確定時再計算で stale 検出される。

- **stale 保証との 1:1**（§4「App34/36 変更時 stale」の成立根拠）: 上表のとおり
  **候補由来の M1 入力に影響し得る上流変更は必ず plan_hash を変える**
  （fix4 M01 で限定訂正——確定時再取得値〔§2D・候補由来でない fields 材料〕と
  非内容的状態〔§2C〕は本保証の対象外＝それぞれ最新読み・実行時フィルタが担う）。
  確定時は材料の現在値から plan_hash を**再計算**して封筒 detail と照合
  （不一致=aborted・write 0）＝§4A の snapshot hash 保存（M02）と一体の設計。
- **上流訂正（App34/36 変更・再導出・戸籍追加受領）時の失効**:
  - 新 head run／App33 追加 → plan_hash が変わる → 新封筒。**旧封筒は確定時の
    再検証で失効**——確定ハンドラ phase 1 で「材料の現在値から plan_hash を再計算し
    封筒 detail と一致」を検証、不一致は aborted（「前提が変わっています。新しい
    請求案から確定してください」・write 0）。stale ガードの型（P3-003B）と同型。
  - 旧封筒の後始末は[人]（要確認→完了 or 下書きの HUMAN_TRANSITIONS・機械は閉じない）。
- **M1 側の二重起票（初版・fix2 H03 で撤回＝§4B が正）**: 初版は「M1 既存の運用に
  委ねる」としていたが、**M1 の既存二重起票ガード（find_existing）は
  `command_id`（LINE 指示）基点であり plan 由来の起票には効かない**（D2 指摘）。
  §4B の plan 由来 M1 冪等キーで置き換える（両時点残置）。

## 4B. plan 由来 M1 起票の冪等（fix2 H03・決定的冪等キーの新設）

- **M1 冪等キー（fix3→fix4 H01-02 で再々定義＝plan 横断で安定・両時点残置）**:
  `shokumu_plan:{case_record_id}:{municipality}:{person_id or "-"}:{様式}`
  （様式 ∈ `form1|form2`）。**plan_hash をキーから除去**——キーは「案件×宛先×
  対象者×様式」という**業務上の起票単位**を表し、**全 plan（材料変化後の新 plan
  含む）を横断して安定**する。plan_hash 込みの旧キー（fix3）では材料が変わる度に
  キーが変わり、**同一起票単位の既存 M1 下書きを別物として再発行**してしまう
  （初版 `{line_type}` 込みキー・fix3 `{plan_hash}` 込みキーはいずれも撤回）。
  束ねに含めた line_type 集合はキーに載せず request_items と監査メタ
  （`plan_lines`）に記録する。
  grammar: `^shokumu_plan:[0-9]{1,10}:[^:]{1,64}:([0-9]{1,10}|-):(form1|form2)$`。
- **照合 HIT 時の比較（fix4=plan_lines 集合→fix5 H01 で m1_fingerprint へ改定・
  司令塔裁定=Codex 提案採用・両時点残置）**:
  - （履歴・fix4）plan_lines 集合比較のみ——**不足として撤回**: line_type 集合が
    同じでも count・target 実値（生年月日訂正等）・宛先 App31 引当てが異なる既存
    下書きを skip 回収してしまう（古い内容の請求書が生き残る）。
  - **fix5（二段構え・材料列挙——fix6 で二層化へ改定＝両時点残置）**: 材料閉集合を
    「request_items＋target（person_id・氏名・生年月日・本籍・住所・筆頭者）＋
    municipality＋unit＋様式＋plan_lines」・空値 null 統一——**fix6 で撤回**:
    (i) M1 実入力と監査材料が混在し「実入力に影響する値の漏れ」を構造検査できない
    (ii) purpose が漏れ・筆頭者は plan 経路が渡さない field だった
    (iii) null 統一は channel_json 実値（空文字）と乖離。
  - **fix6 正（二層化・§1.6 実査の実出力形に基づく）**: `m1_fingerprint` ＝
    **A層＋B層の canonical object の SHA-256**。
    - **A層＝正規化済み M1 実入力**: plan 経路が M1 へ渡す **channel_json 完成形
      そのもの**——実査（dispatch_bot/shokumu.py `build_channel_json` :177-183）:
      ```python
      return {
          "request_items": p["request_items"],
          "municipality": p["municipality"],
          "target": p.get("target") or {},
          "purpose": resolved_purpose(p),
      }
      ```
      の 4 キーと 1:1。purpose は **resolved 後の文字列**（`resolved_purpose`
      :114-119＝明示指定→`PURPOSE_BY_UNIT`。相続放棄=
      「受任事件（相続放棄申述）の申述に必要な戸籍等の取得のため」:58）。
      **target は plan 経路の閉集合＝`対象者`・`生年月日`・`本籍`・`住所` の
      4 キーのみ**——実装スキーマの既知 7 キー（対象者/フリガナ/本籍/住所/筆頭者/
      世帯主/生年月日）のうち **フリガナ・世帯主・筆頭者は plan 経路では非搭載**と
      確定（App34 に対応する機械材料が無く、様式生成は「空の項目は印字しない」
      〔channels/shokumu_seikyu.py:303〕・筆頭者/世帯主も `target.get(...)` の
      任意参照〔:353・:391〕のため非搭載で様式が成立する）。**plan 経路が target へ
      この 4 キー以外を書かないことを構造 pin**（§6-42）。
    - **B層＝監査・引当て補強材料**: `{"app31_record_id": str,
      "plan_lines": [...], "unit": str, "form": "form1"|"form2"}`。
    - **不変条件（明文化）**: 「**M1 実入力に影響する値は例外なく A層に含まれる。
      A層の漏れは設計違反**」——A層は build_channel_json の**出力そのもの**を
      直列化するため、channel_json へのキー追加・値変更は自動的に A層へ入る
      （実入力の漏れが構造的に起きない）。
    - **正規化規則（fix6 で完全固定・実装票が推測ゼロで書ける水準）**:
      1. canonical JSON: `json.dumps(obj, sort_keys=True, ensure_ascii=False,
         separators=(",", ":"))` の **UTF-8 bytes** を SHA-256（hex 小文字 64 桁）。
      2. **型規則**: channel_json 実値の型を固定——`request_items[].count` は
         **int（JSON 数値）**・その他の値はすべて **str**。**fingerprint 算出時の
         型変換は禁止**（count を文字列化しない・数字文字列を数値化しない）。
      3. **欠落・空文字・null の統一規則**: **全キー必須収載**（A層 4 キー・
         target の 4 キー・B層 4 キーは常に存在させる。キー欠落は保存境界で拒否）。
         **値なしは空文字 `""` に統一・null は使わない**（channel_json 実装が
         空値を `""`/空 dict で作る事実と一致——「空文字 vs null の区別」は
         null 非使用により発生しない）。
      4. **Unicode 正規化: 不採用（NFC を適用しない）**——App34/kintone 保存値の
         byte をそのまま比較する（NFC 同一視は表記差の業務判断を機械へ持ち込み、
         請求書印字値と指紋の乖離を生むため採らない）。
      5. **同一 type の request_items（fix6 の「算出時併合」と読める記述は
         fix7 で撤回・方式A＝司令塔裁定・両時点残置）**: count 合算併合は
         **fingerprint 算出時の正規化ではなく、plan 側の channel_json 完成前の
         正規化**として行う——束ね（§2A）で channel_json を組み立てる時点で
         type 昇順 sort＋同一 type の count 合算を適用し、**併合済みの
         channel_json を (a) fingerprint A層 (b) App30 create（§2D）
         (c) M1 prepare（App30 経由）のすべてへ同一 object として渡す**。
         plan_lines は enum 定義順 sort・unique（従来どおり）。
         **不変条件（fix7・明文化）**: 「**fingerprint へ渡した A層と App30
         create へ渡した channel_json は byte 一致する。fingerprint 内部だけの
         併合は設計違反**」——指紋と実起票が別内容になる経路を構造的に持たない。
    ——結果として**入力の並び順だけが異なる場合は同値**になる（§6-38）。
    - **HIT 時の判定**: plan_idem 一致 → `m1_fingerprint` 比較——**一致 = skip
      回収**（already_filed・新規 create しない）／**不一致 = 当該候補を要確認**
      （「同一起票単位で内容の異なる既存下書きがあります」の道案内・**自動 merge・
      自動再発行は禁止**——既存下書きの取扱いは[人]が承認キューで判断）。
  - **§4-v2.1 との接続（fix5・明記）**: §4-v2.1 の保証「候補由来の M1 入力に
    影響し得る上流変更は必ず plan_hash を変える」により、上流訂正は必ず新 plan
    （新封筒）を生む。新 plan の確定時、同一 plan_idem の旧下書きが残っていれば
    **m1_fingerprint が必ず不一致**（M1 入力が変わった＝fingerprint 材料が変わった）
    となり要確認へ倒れる——**追跡例**: 生年月日を App34 で訂正 → snapshot hash
    変化 → plan_hash 変化 → 新封筒 → 確定 → plan_idem HIT（業務単位は同一）→
    fingerprint 不一致（target.生年月日が相違）→ 要確認（古い下書きの生き残りゼロ）。
- **二層の関係（fix4 H01-02 で再記述）**: §2C の既存 M1 照合は**共通行（除票/附票）
  に限った前段の表示フィルタ**（提案ノイズ低減・削除しない・取り零し許容）。
  **§4B の plan_idem 照合が最終防壁**であり、**全行類型（束ね単位すべて）を覆う**
  ——M1 create 直前に必ず通るため、§2C が拾えない行類型・状態でも二重起票は
  ここで止まる。
- **保存場所**: 起票する M1 レコードの**チャネル固有データ内・監査メタの
  `plan_idem`＋`m1_fingerprint`（fix5）キー**（§2D の監査メタ構造。channel JSON の
  parse_channel_data が読む既知キーと衝突しない追加キー＝prepare 挙動不変。
  `m1_fingerprint` grammar: `^[0-9a-f]{64}$`）。
- **起票前ガード**: 各候補の create 前に App30 を
  `チャネル="職務上請求"`＋`案件レコードID=case` で絞り、チャネル固有データを
  **JSON parse して `plan_idem` の完全一致**を照合（P3-003a find_existing の
  完全一致型・like の部分一致誤爆なし）。一致あり=当該候補は **already_filed
  として回収**（新規 create しない）。
- **部分失敗後の原子性（再確定で回収・fix3 H03 で入口を明確化）**: 確定で n 件を
  順次起票し k 件目で失敗した場合——作成済み k−1 件は残存（下書き=無害）・
  **plan 封筒は open（要確認/no）のまま維持**（クローズしない）・応答は
  「n 件中 k−1 件起票済み。再確定で残りを再試行します」。**reconcile の入口は
  「同一封筒の再確定」＝新しい関所往復**（復唱→OK を経る・open 封筒だから可能＝
  §2C-4 の open 限定回収と整合）。再確定は同一 plan_hash（stale 検証通過）の下で
  各候補の plan_idem を再照合し、既存分を already_filed 回収・**残りのみ create**
  → 全件揃った時点で封筒クローズ（実行済み yes）＝**冪等な再実行で完結する原子性**
  （P3-003c §4.1「decision 一度きり＋side effect 冪等」と同型の構造）。
- **ACK 不明の reconcile（P3-003a §3B 同型）**: create の通信失敗は「未作成」と
  断定しない（POST 成功・応答喪失があり得る）。例外は伝播（握り潰し禁止）し、
  再確定時の **plan_idem 完全一致照合が reconcile を兼ねる**（成功していた分は
  already_filed 回収・二重起票しない）。

## 4A. plan 封筒 detail の閉集合（fix1 M02・司令塔裁定=person_id のみ保存）

heir_envelope 同型の水準（キー閉集合・型・値域 grammar・等値ガード・トップキー一意）:

| キー | 型・値域（grammar） | 備考 |
|---|---|---|
| `case_record_id` | `^[0-9]{1,10}$` | |
| `phase` | enum `{"common","full"}` | §2C |
| `run_id` | `^[1-9][0-9]{0,18}$` or null（common） | P3 系 grammar と逐語一致 |
| `plan_hash` | `^[0-9a-f]{64}$` | §4-v2 |
| `app34_snapshot_hash` | `^[0-9a-f]{64}$` | §4-v2 (2) の単独値（確定時比較用） |
| `app36_rows_hash` | `^[0-9a-f]{64}$` or null（common） | §4-v2 (3) |
| `matrix_version` | `^[0-9A-Za-z.\-]{1,32}$` | |
| `candidates` | list（下記の行 dict のみ）。**未フィルタの canonical 全候補＝plan_hash と 1:1**（fix4 H01-01・§2C フィルタで削除しない）。**並び順は決定的（fix5 M01・下記 tie-break 全順序）** | |
| `冪等キー` | `shokumu_plan:{case}:{plan_hash}` の平文 | find_existing 照合用 |

candidates 行の閉集合: `line_type`（enum＝§2A 行類型 **7 値**〔fix2 H02 で 2 行
追加〕）・`request_type`（FEE_FIELD_BY_TYPE のキー集合内 or 固定値 `"要入力"`）・
`count`（正整数）・`person_id`（`^[0-9]{1,10}$` or null）・`municipality`
（切り出し結果 or 固定値 `"要入力"`）・`status`
（enum `{"propose","fulfilled","input_required"}`）。

**candidates の相関制約（fix2 M02・Codex 提示どおり凍結・保存境界で検証）**:
1. **M1 起票対象は `status="propose"` のみ**（fulfilled/input_required は起票しない）。
2. **`status="input_required"` の行は write 0**（M1 起票に進まない・道案内表示のみ）。
3. `municipality="要入力"` の行は**必ず `status="input_required"`**（propose と併存不可）。
4. `request_type="要入力"` の行も**必ず `status="input_required"`**（§2A.3・同上）。
5. **`line_type="applicant_current"` は常に `person_id=null` かつ
   `status="input_required"`**（裁定⑦=(C) の機械的固定）。
6. **`applicant_current` 以外の行は `person_id` 必須**（null 不可・grammar 適合）。
7. `status="fulfilled"` は**表示専用**（起票・write の対象に一切ならない）。

相関制約は**保存境界（封筒起票時の detail 検証）で強制**する——違反は
EnvelopeDetailPolicyError 同型で保存拒否（起票せず異常扱い・kintone write 0）。
確定側も同じ検証を再実行（保存済み detail の改変・壊れの防御）。

**candidates の決定的並び順（fix5 M01・tie-break 全順序で凍結）**: candidates は
次の**完全順序**で sort して保存する（生成過程・入力順に依存しない＝同一材料から
常に byte 同一の canonical detail・plan_hash の決定性の前提）:
1. `line_type`（§2A の enum 定義順: joh_removed → fuhyo → decedent_joseki →
   chain_missing → parents_death → sibling_death → applicant_current）
2. → `person_id`（null を先頭・非 null は数値昇順）
3. → `municipality`（"要入力" を先頭・他は UTF-8 コードポイント昇順）
4. → `request_type`（"要入力" を先頭・他は FEE_FIELD_BY_TYPE 定義順）
5. → `status`（propose → fulfilled → input_required）
6. → `count`（昇順・最終 tie-break。全 6 键一致の重複行は生成時点で 1 行に併合）

**§4-v2 (7)〔App31 照合 snapshot〕の並び（fix5→fix6 M01 で確定・両時点残置）**:
fix5 の「candidates と同一規則を適用（line_type→person_id→municipality）」は
**不完全として撤回**（snapshot 行に person_id が正式収載されておらず・snapshot に
存在しないキー〔request_type/status〕を含む順序指定だった）。fix6 は **方式A**——
snapshot 行へ person_id を正式収載し、**snapshot 自身のキーのみ**による完全順序
（§4-v2 (7): line_type 定義順→person_id→市区町村名→app31_record_id→有効→
fallback 定義順・完全一致行は併合）で決定性を担保する。

- **氏名・生年月日・住所の実値は保存しない**（司令塔裁定）——封筒は
  **person_id＋snapshot hash のみ**を持ち、確定時に App34 を**再取得**して
  M1 入力を組み立て、`app34_snapshot_hash` の再計算比較で stale を検出する
  （＝§4-v2 と一体。municipality は自治体名のみ＝住所全文を封筒へ写さない）。
- 起票時閉集合の等値ガード・閉集合外キーの保存拒否（EnvelopeDetailPolicyError
  同型）・トップキー `shokumu_plan` の一意性は heir_envelope の型を逐語で踏襲。

## 5. 裁定欄（[人]。P3-003C-D §8 形式・選択肢+推奨+影響。推測で決めない）

| # | 論点 | 選択肢 | 推奨 | 影響 |
|---|---|---|---|---|
| 1 | **plan 入力の正本**（続柄別セットの展開元） | (A) confirmed decision 済み head run のみ（App36 と一致した確定続柄） (B) head run があれば未確定（derived/held）でも提案 (C) 二段——共通行（除票/附票・続柄非依存）は run 非依存で提案可・続柄別セットは confirmed 後 | **(C)**——マトリクス§1 が「続柄に関わらず必ず取得・最優先」と定める除票/附票は確定を待つ理由がなく、続柄別は誤続柄での請求提案を避けるため confirmed 後 | (A) 最も安全だが除票の最優先性（管轄確定の入口）が遅れる。(B) 未確定続柄での提案が承認キューに乗る。(C) 実装は二段になるが凍結正本の優先順位と一致 |
| 2 | **plan 生成の起動** | (A) T2 語彙の明示指示のみ（「請求案を出して」） (B) confirmed 確定の直後に自動起票 (C) A+B 併用 | **(A)**——P3-003a 裁定「自動起動しない」と同型・初版は人の指示で 1 巡。自動化は運用実績後の別票 | (A) ひと手間残る。(B) 確定 handler への結線が増え P3-003c の再開経路との相互作用検証が要る |
| 3 | **封筒の粒度** | (A) 案件 1 封筒（全請求候補まとめ・確定も一括） (B) 請求先（municipality）単位に 1 封筒 | **(A)**——関所の確認は「この案件の請求計画」を一望して判断するのが自然で、確定後の M1 起票を候補ごとに分けるのは [4] の実装で可能（封筒粒度と M1 起票粒度は独立） | (B) は封筒が乱立しキューが汚れる。(A) は部分確定（一部の請求先だけ確定）ができない——部分確定が要るなら (B) か「候補の取捨は復唱前の選択」の追加設計 |
| 4 | **連鎖ループ（H10）のスコープ** | (A) 本票は 1 巡分の提案まで・受領→読解→次請求の連鎖は別票 (B) 本票で連鎖まで設計 | **(A)**——§1.2 の反復ループは M5 受領・チェックリスト（H9）に跨り、判断材料（受領分類の実装状態）が別系。1 巡分でも「再指示すれば新材料で新 plan」により実務は回る | (B) はスコープ膨張。(A) は請求のたびに人の指示が要る（初版の意図的制約） |
| 5 | **確定者の要件** | (A) ATTORNEY_ALLOWLIST 必須（P3-003c と対称） (B) 不要（対外送信の関所は M1 承認が別にあるため） | **(B)**——plan 確定は「M1 の下書きを作る」内部操作であり、対外発信の防壁は既存の承認フロー（弁護士）が担う。二重の弁護士ゲートは運用負担 | (A) だと事務員が請求案を進められない。(B) でも最終防壁（承認済み遷移）は弁護士のまま不変 |
| 6 | **flag** | 新設 `SHOKUMU_PLAN_ENABLED`（既定 OFF・語彙可視性連動＝P3-003-CMD の型） | 新設（既定 OFF） | 点火は[人]・実機デー系の運用に載せる |
| 7 | **申述人の特定正本**（fix1 H03 で確定できず再掲） | (A) 案件（App26）の依頼者＝顧客を申述人とみなし、App34 上の対応 person を[人]が確定入力 (B) App34 に「申述人フラグ」field を新設（kintone 実機変更・[人]） (C) 初版は申述人現在戸籍の行を常に「要入力」提示（機械は person を選ばない） | **(C)**——正本が repo に存在せず、誤った申述人での請求提案は実害があるため初版は道案内に倒す | (A)(B) は kintone 設計/運用の確定が前提。(C) は申述人行だけ人の入力が毎回要る |
| 8 | **App31 の登録粒度**（政令市の市/区・fix1 H03 で確定できず再掲） | (A) 区単位で登録（切り出しは区優先＝§2A.1 の既定） (B) 市単位で登録 | **(A)**——住民票・戸籍事務は区役所所管が通例。§2A.1 は区→市の順で照合するため (B) 運用でも動く | 登録は[人]の App31 運用。粒度が混在しても照合順で吸収されるが、正は一方に揃えるのが望ましい |

**fix1 追記（Codex 助言・裁定②の適用範囲の明確化）**: 裁定②（起動=語彙のみ）は、
H系列③ §1 の旧記述「受任確定と同時に M1 職務上請求を自動起票」を**起動時点に
ついてのみ上書き**する（初版は受任フックでなく[人]の語彙指示で起動）。
**除票/附票の最優先性・内容・優先順位は不変**（マトリクスの凍結は §3-1 のまま・
受任フック起動の復活は運用実績後の別票裁定）。

## 6. テスト計画（実装票の受入条件案・系統立て）

1. **plan 生成（純関数）**: マトリクス写像（続柄→セット・凍結データとの一致 pin）／
   除票/附票の最優先行が常に先頭／F5 突合（未収集→請求候補・収集済み→充足表示）／
   先順位放棄フラグ→共通行のみ+警報文言／判定不能（読解 JSON 欠損等）→全体要確認。
2. **F5 注記の写像**: 「収集見込み（弁護士確認前）」が plan・封筒 detail・応答の
   全てに存在（白画面/断定表示にしない——P4-005 の道案内写像と同じ規律）。
3. **冪等（fix4 M04-01(ii) で open 限定へ修正・§6-27 との衝突解消）**:
   同一材料＋**open 封筒あり**→ already_filed（新規起票なし）／同一材料でも
   **terminal のみ**→ 新規起票（§2C-4 fix3 の却下非抑止と一貫）／App33 追加・
   新 run・マトリクス版更新→ 新 plan_hash・新封筒／find_existing 型の完全一致検索。
4. **stale/訂正**: 確定時の plan_hash 再検証——材料変化後の旧封筒確定は aborted・
   write 0・固定文言。
5. **関所**: 復唱に請求先・種別・通数・「提案であり承認は別」の明示／OK 単回・
   pending invalidate／unsupported（対象外 source）／裁定⑤の確定者要件どおり。
6. **M1 結線**: 確定→ 候補ごとの M1 App30 起票 fields が §1.3 の必須集合を満たす
   （request_items 種別 enum・様式1 のとき生年月日必須・unit→purpose は
   PURPOSE_BY_UNIT の値そのもの＝**文言の byte 一致 pin**）／App31 未登録は
   PrepareDeferred へ委譲（plan 側で先回りしない）／料金計算を plan 側が行わない
   （compute_kogawase 呼出しゼロの AST pin）。
7. **原則の構造 pin**: plan 経路に承認済み遷移・送信系 API の呼出しゼロ（App30
   絶対制約の AST/契約 pin・SERVER_TRANSITIONS 不変）／全成果物が 要確認 or
   承認待ち で停止することの状態遷移テスト。
8. **flag**: 既定 OFF で語彙非公開・I/O ゼロ（P3-003-CMD の flag ゲート pin と同型）。

**fix1 追加（M04・Codex 列挙の negative 11 系統）**:

9. **未 confirmed 状態別 write 0**: §2B 条件 2 の否定形——leaf なし／held／rejected の
   各状態で第二段が生成されず App30/M1 write 0＋条件未充足の道案内応答。
10. **二段重複なし**: 第一段実施済み（plan 封筒 open／M1 起票済み〔非 terminal〕／
    App33 取得済み）の 3 面それぞれで第二段に共通行が**含まれない**（§2C の
    照合 3 分岐 parametrize）＋第一段不存在なら**含まれる**（漏れゼロ）。
11. **App36 不一致系**: §2B 条件 3〜6 の否定形（run ID 不一致行あり・戸籍確認済 no
    行あり・person_id 集合不一致・重複/欠落/余剰）→ 全体要確認・write 0。
12. **App34 訂正 stale**: plan 起票後に使用 field（氏名/住所最新/本籍最新等）を
    変更 → 確定時の snapshot hash 再計算不一致 → aborted・write 0・固定文言。
    **非使用 field の変更では失効しない**（§4-v2 (2) の閉集合 pin・両方向）。
13. **マトリクス同一性 pin**: config データが H系列③ §2 の凍結セットと 1:1
    （行類型・書類種別・加算関係の byte 水準照合・「その他」集約なし）。
14. **detail 閉集合違反**: §4A の閉集合外キー・grammar 外値（氏名等の実値混入を
    含む）が保存拒否（EnvelopeDetailPolicyError 同型）。
15. **purpose 非上書きの構造 pin**: plan 経路のどこにも purpose キーを書く文が
    無い（AST・build_channel_json への引数にも purpose を渡さない）＋
    PURPOSE_BY_UNIT の**文言 byte 一致**（§6-6 と統合）。
16. **承認済み不書込み**: plan 経路の全 kintone write の対象 field に
    `発送ステータス` の値として「承認済」が現れない（SERVER_TRANSITIONS 不変＋
    write 値の閉集合 pin）。
17. **部分起票の原子性（fix2 H03 で実在ガードの記述へ書換え）**: 確定で n 件を
    順次起票し k 件目で例外 → 作成済み k−1 件は残存（下書き＝無害）・plan 封筒は
    **クローズしない**・応答は「n 件中 k−1 件起票済み・再確定で残りを再試行」。
    再確定は **§4B の `plan_idem` 完全一致照合**で既存 k−1 件を already_filed
    回収し**残りのみ create**・全件揃いで封筒クローズ——を実出力で pin
    （初版の「M1 既存ガード〔件名/監査メタ〕」への依存記述は撤回・§4B が正）。
18. **flag OFF の完全 I/O ゼロ**: `SHOKUMU_PLAN_ENABLED` 未設定で plan 語彙の
    App30 search・create が**ゼロ**（search も呼ばない・P3-003-CMD の冒頭辞退型）。
19. **（予備）App31 照合順**: §2A.1 の区→市の照合順・両方不在の「要入力」化。

**fix2 追加（M04・D2 指摘の negative）**:

20. **App31 更新後 stale**: plan 起票後に採用 App31 レコードの名称変更・無効化
    （有効=no）→ §4-v2 (7) の再計算不一致 → 確定 aborted・write 0。
21. **被相続人フラグ変更 stale**: App34 の被相続人フラグ付替え → (2)/(6) の
    不一致 → aborted・write 0（run 側 decedent との不整合検出を含む）。
22. **request_type 未確定の write 0**: `chain_missing` 行（request_type=要入力）が
    M1 起票に**進まない**（相関制約 2/4・§2A.3——input_required の起票ゼロ）。
23. **欠落行のマトリクス一致**: `decedent_joseki`・`sibling_death`（fix2 追加行）を
    含む §2A.2 突合表と config データの機械照合（§6-13 の完全化）。
24. **M1 冪等キーの照合系**: `plan_idem` 完全一致のみ回収（部分一致・別案件の
    同型キー・壊れ JSON〔parse 不能→要確認へ倒す・未起票扱いにしない〕を
    parametrize）。
25. **ACK 不明後の既存回収**: create 例外（ACK 不明）→ 再確定で当該候補が
    already_filed 回収され**二重起票ゼロ**（§4B reconcile の実出力 pin）。
26. **相関制約違反の保存拒否**: §4A 相関制約 1〜7 の各違反形（propose なのに
    municipality=要入力・applicant_current に person_id 等）が保存境界で拒否。
27. **却下の非抑止（fix3 H03 で書換え・旧記述〔同一材料=already_filed 回収〕は
    撤回）**: [人]が封筒を閉じた（terminal 化）後の同一材料での再指示 →
    **新規封筒として起票される**（open 限定回収・§2C-4 fix3——却下が再提案を
    止めない構造の実出力 pin）。open 封筒が残っている間の再指示は
    already_filed 回収（新規起票なし）。

**fix3 追加（M04・3 裁定への同期）**:

28. **束ね正規化**: 同一自治体×異 person_id／同一自治体×同一 person_id×異様式の
    候補が**別 M1 起票に分離**される（plan_idem も別・§2A fix3 束ね規則）。
    同一自治体×同一 person_id×同一様式のみ 1 起票へ束ね（request_items 複数行＋
    監査メタ plan_lines に行類型集合）。
29. **§2C 状態変化の吸収**: plan 起票後に App30/M1 の起票・封筒状態だけが変化
    （共通行の M1 が完了へ・別 plan 封筒がクローズ等）→ **plan_hash は不変**
    （stale にならない）・確定時の起票時フィルタ＋plan_idem 照合が重複を吸収して
    残り候補のみ起票（H01 分離方式の実出力 pin）。
30. **部分失敗→open 維持→再確定 reconcile の状態遷移**: k 件目失敗 → 封筒
    open 維持・応答道案内 → **同一封筒の再確定（新しい関所往復）** →
    plan_idem 回収＋残り create → 全件で封筒クローズ——の遷移列を実出力で pin
    （途中で封筒がクローズされる経路・関所を経ない再試行経路が無いこと）。

**fix4 追加（M04-01・裁定への同期）**:

31. **状態反転の復元（H01-01 の実出力 pin）**: common 封筒を[人]が却下（terminal
    化）→ その後 full 封筒を確定 → full の canonical candidates に保持された
    共通行が実行時フィルタを通過（M1 未起票・App33 未取得）して**復元起票される**。
    生成時除外だった場合に失われる経路の再現込み。
32. **sibling_death 親選定の 4 分岐**: (a) 共有親が一意に特定→対象者確定
    (b) 共有親なし (c) 両親とも共有に該当（判別不能） (d) 親 ID 欠損——
    **(b)(c)(d) はいずれも「要入力」**（推測ゼロ・parametrize）。
33. **plan_lines の grammar**: 7 値 enum（§2A 行類型）のみ・**sort 済み・unique**・
    enum 外値/重複/非 list は保存境界で拒否（監査メタも grammar 検証の対象）。
34. **m1_fingerprint 不一致（fix4=plan_lines 比較→fix5 で実質検査へ書換え）**:
    同一 plan_idem の既存 M1 の `m1_fingerprint` と今回の正規化 M1 実入力の
    fingerprint が不一致 → 当該候補は**要確認**（自動 merge・自動再発行が発生
    しないこと・skip 回収は fingerprint 一致時のみ——§4B fix5）。

**fix5 追加（H01 対照 4 形）**:

35. **fingerprint 一致 skip**: 同一 plan_idem・同一 M1 実入力 → skip 回収
    （新規 create ゼロ・already_filed 応答）。
36. **生年月日訂正後の不一致**: App34 で target の生年月日を訂正 → 新 plan 経由の
    確定で plan_idem HIT・fingerprint 不一致 → 要確認（§4B の追跡例の実出力 pin・
    古い下書きが skip 回収で生き残らないこと）。
37. **request_items.count 変化**: 同一 line_type 集合のまま count のみ変化 →
    fingerprint 不一致 → 要確認（fix4 の plan_lines 比較では検出不能だった形）。
38. **並び順の正規化実効性**: request_items・plan_lines の入力順だけが異なる
    同一内容 → fingerprint **同値**（誤った不一致=要確認の誤爆が出ないこと）。

**fix6 追加（H01/M01 対照の完全化）**:

39. **resolved purpose のみ変更**（明示指定の追加・PURPOSE_BY_UNIT の版差等）→
    fingerprint **不一致**（A層に purpose が入っている実質検査）。
40. **App31 record id のみ変更**（同名で別レコードへ引当て替え）→ fingerprint
    **不一致**（B層 app31_record_id の実質検査）。
41. **target 搭載 4 field の個別変更**: 対象者／生年月日／本籍／住所を
    **1 つずつ**変更 → **各々 fingerprint 不一致**（parametrize・A層 target の
    field 単位実質検査）。
42. **fingerprint 対象外 field のみ変更 → 同値＋非搭載の構造 pin**: plan 経路が
    target へ書かない field（フリガナ・世帯主・筆頭者）は fingerprint に影響せず、
    **そもそも plan 経路の channel_json 組立てが target へ 4 キー以外を書かない**
    ことを構造 pin（AST/実出力の両面）。
43. **欠落・空文字・null の正規化境界**: キー欠落＝保存境界で拒否／空値は `""`
    のみ（null 混入は grammar 拒否）／`""` と欠落が同値扱いにならない（欠落は
    そもそも保存されない）ことを parametrize。
44. **同一 type request_items の重複正規化（fix7 で 4 点へ増補・方式A の実質検査）**:
    (a) 分割入力（`[{除籍謄本,1},{除籍謄本,1}]`）が channel_json 完成前に
    `[{除籍謄本,2}]` へ併合される（fingerprint が一意・分割に依存しない）
    (b) **併合済み channel_json が実際の App30 起票（create fields のチャネル固有
    データ）に使用される**（併合前の形が App30 へ流れない）
    (c) **fingerprint A層と App30 create へ渡した channel_json の byte 一致**
    （§4B fix7 不変条件の実出力 pin）
    (d) 併合済み入力が**既存 M1 経由で様式1 PDF 1 枚・count 合計値（2 通）**で
    生成される（prepare 挙動の end-to-end 確認・M1 側は無改変のまま）。
45. **App31 snapshot 入力順のみ相違 → plan_hash 同値**（§4-v2 (7) の完全順序
    sort の実効性・M01）。
46. **snapshot の完全 tie-break（fix7 で 3 形へ増補）**: 同一 line_type×同一
    市区町村名で person_id・app31_record_id が異なる複数行 → 完全順序で一意に
    整列（parametrize）。加えて——
    (a) **先行 4 キー同一・`有効` のみ相違** → コードポイント昇順（"" 先頭）で
    一意に整列（fix7 の比較規則の実質検査）
    (b) **先行 5 キー同一・`fallback` のみ相違** → 定義順（ward→city→""）で一意
    (c) **全 6 キー一致の複数行** → 1 行へ併合（併合規則の実出力 pin）。
47. **非存在キー参照なしの構造 pin**: snapshot の順序決定・同一性判定が
    snapshot 自身のキー（line_type/person_id/市区町村名/app31_record_id/有効/
    fallback）のみを参照する（candidates 側の status/count 等への参照ゼロ・AST pin）。

**fix8 追加（H02・監査メタ同期）**:

48. **監査メタ閉集合の §2D/§4B 同期 pin**: §2D の監査メタキー閉集合（filed_by/
    plan_envelope_no/plan_hash/plan_idem/m1_fingerprint/plan_lines）と §4B の
    保存キー集合が **byte 水準で一致**する（単一の定数を両所が参照する実装を
    含めて pin——二重定義の黙った乖離を構造的に遮断）。
49. **m1_fingerprint の照合安全側**: 既存 M1 の監査メタで `m1_fingerprint` が
    **欠落・grammar 不正（hex64 外）・JSON parse 不能**のいずれか → **skip 回収
    せず当該候補を要確認**へ（比較不能を「一致」扱いにしない・parametrize）。

**宣言（fix6→fix7→fix8 で更新）**: §6 対照（1〜49・fix7 増補〔44 の 4 点・46 の
3 形〕・fix8 増補〔48・49〕込み）は **H01（plan_hash／m1_fingerprint の各保証・
channel_json 完成前併合と byte 一致不変条件）・M01（canonical 順序の決定性・
`有効` コードポイント順を含む完全 tie-break）・H02（監査メタ閉集合の §2D/§4B
同期と保存・照合の安全側）の保証項目と 1:1** である——保証の各文（§4-v2・
§4-v2.1・§4A 全順序・§4B fix6/fix7・§4-v2 (7) fix7・§2D fix8 受入条件）に
対応する対照が本節に存在し、対照の無い保証・保証の無い対照は存在しない。

## 7. スコープ外（明記）

- 受領→読解→次請求の**連鎖ループ**（H10・M5/チェックリスト H9 連携）——裁定④どおり別票。
- チェックリスト SUBTABLE の初期行生成（H9・受任確定フック）・14日遅延警報（§1 の
  運用警報）——souzoku-houki 側の別系。
- App31 市区町村マスタの整備・宛先データの拡充（[人]運用）。
- 相続放棄以外のユニット（時効援用等）への展開——purpose がユニット別確定である
  ため構造は共通だが、マトリクスは souzoku-houki 固有。初版は相続ユニットのみ。

## 8. fix1 改定記録（R-SHOKUMU-PLAN-D1・2026-08-10。両時点残置・遡及書き換えにしない）

D1 判定: 凍結不適格（HIGH3+MEDIUM4）。fix1 で以下を反映:

- **H01**: plan_hash 材料を §4-v2 で具体化——App34 **使用 field snapshot hash**
  （閉集合＝§2A 写像の読む field と同一）と App36 行集合 hash（$id/$revision/
  current/導出元人物ID）を追加。stale 保証が hash 材料と 1:1 で成立する根拠を明記。
- **H02**（司令塔裁定=照合方式(a)）: §2C——detail に phase 識別・第二段の共通行は
  App33 収集済み＋App30 二面（plan 封筒/M1 起票・実 field の照合キー）で重複排除・
  不存在なら第二段に含める（漏れゼロ）。
- **H03**: §2A 写像表（行類型 5 種×request_type/様式/person_id 規則/municipality
  材料）・§2A.1 切り出し規則（既存関数なしの実査に基づく新規定義・区→市照合順）・
  取得不能は「要入力」道案内。**正本が定まらなかった 2 項目**（申述人の特定・
  App31 登録粒度）は裁定⑦⑧へ再掲。
- **M01**: §2B——第二段入力条件を Codex 提示の 6 条件で凍結。
- **M02**（司令塔裁定=person_id のみ保存）: §4A——detail 閉集合を heir_envelope
  同型水準で具体化・氏名/生年月日/住所の実値非保存・確定時再取得＋hash 比較
  （H01 と一体設計）。
- **M03**: §2D——呼出し先を build_channel_json＋`_fields_shokumu_seikyu` 同一
  field 集合の単票 create に固定（prepare 以降は既存経路・関与しない）。
- **M04**: §6 に negative 11 系統（9〜19）を追加。
- **裁定②の適用範囲**: H系列③の旧「受任時自動起票」を**起動時点についてのみ**
  上書きする旨を §5 末尾に明記（内容・優先順位は不変）。
- 次レビュー: **R-SHOKUMU-PLAN-D2**（BASE=origin/main `6312112`・TARGET=
  shokumu-plan-design の fix1 commit）。→ 実施済み・結果は下記 fix2 記録。

## 8-2. fix2 改定記録（R-SHOKUMU-PLAN-D2・2026-08-11。両時点残置・遡及書き換えにしない）

D2 判定: H01/H02/H03＋M01〜M04（M01 のみ RESOLVED）。fix2 で以下を反映:

- **H01**: §4-v2 に材料 3 点を補完——person_id を snapshot JSON の構成要素として
  明文化・`被相続人フラグ` を使用 field 閉集合へ追加・(6) head.decedent_person_id・
  (7) **App31 照合 snapshot**（採用レコード id/名称/有効/fallback 採用結果）。
  §4-v2.1 に「読む値↔hash 材料」対応表を新設し 1:1 を機械的に提示。
- **H02**: §2A.2 凍結マトリクス 1:1 突合表を新設し**欠落 2 行を補完**
  （`decedent_joseki`〔子: 被相続人の除籍謄本・死亡記載〕・`sibling_death`
  〔甥姪: 兄弟姉妹の死亡を証する戸籍〕）。§2A.3 で type 閉集合を実査
  （FEE_FIELD_BY_TYPE 6 種のみ・複合 type 不存在）し、**F5 不足行の request_type
  は初版「要入力」に倒す**と凍結（type 新設は別票）。
- **H03**: §4B 新設——plan 由来 M1 の**決定的冪等キー**
  `shokumu_plan:{case}:{plan_hash}:{line_type}:{municipality}:{person_id|-}` を
  監査メタ `plan_idem` に保存・起票前の JSON parse 完全一致照合・部分失敗後の
  再確定回収（原子性）・ACK 不明 reconcile（P3-003a §3B 同型）。初版 §4 の
  「M1 既存運用に委ねる」記述と §6-17 の旧記述は**撤回**（command_id 基点の
  既存ガードは plan 由来に効かないため）。
- **M01**: §2C(ii) を JSON parse 後の 3 点一致（type＋person/plan_idem＋
  municipality）へ厳密化・parse 不能は要確認へ・**却下=非抑止**（§2C-4）を明記。
- **M02**: §4A に candidates 相関制約 7 項（propose のみ起票／input_required=
  write 0／要入力→必ず input_required／applicant_current=常に null+input_required
  〔裁定⑦の機械的固定〕／他行 person_id 必須／fulfilled=表示専用）＋保存境界検証。
- **M03**: §2D に fields 値の出所（顧客名表示用=案件レコード再取得・案件アプリID=
  ユニット別 env・監査メタ構造の定義）と `_fields_shokumu_seikyu` 同期方式
  （共用関数化第一候補・byte 一致 pin が代替強制）を固定。
- **M04**: §6 に 20〜27 の 8 系統を追加（App31/被相続人フラグ stale・
  request_type 未確定 write 0・欠落行照合・plan_idem 照合系・ACK 回収・
  相関制約違反・却下再提案）。
- 次レビュー: **R-SHOKUMU-PLAN-D3**（**凍結判定**・BASE=origin/main `6312112`・
  TARGET=shokumu-plan-design の fix2 commit）。→ 実施済み・結果は §8-3。

## 8-3. fix3 改定記録（R-SHOKUMU-PLAN-D3・2026-08-11・司令塔 4 裁定。両時点残置）

D3 判定: 凍結不適格（HIGH3+MEDIUM1・M01〜M03 は RESOLVED）。fix3 で司令塔裁定を反映:

- **H01（裁定=分離方式）**: plan_hash＝**正本内容のみ**（§4-v2 (1)〜(7)）と確定。
  §2C の重複排除を「hash 確定後・起票時の**非内容的フィルタ**」へ分離（既起票の
  吸収は §4B plan_idem 完全一致の責務）。§2D の確定時再取得値は hash 対象外
  ——常に最新読みのため snapshot が存在せず stale 検出を要しない根拠を明記。
  §4-v2.1 対応表の主張を「**候補集合の同一性を決める読取値**」に限定訂正。
- **H02（裁定=束ね単位の正規化）**: 束ね＝**同一自治体×同一対象者×同一様式**のみ
  （異人物・異様式は別 M1）。plan_idem を
  `{case}:{plan_hash}:{municipality}:{person_id|-}:{form1|form2}` へ再定義
  （束ね単位と 1:1・line_type 集合は request_items＋監査メタ `plan_lines` に記録・
  初版キーは撤回）。sibling_death の親選定＝**App34 親エッジ共有判定**（被相続人と
  父母を共有する側・判定不能は要入力）を正規規則として凍結。
- **H03（裁定=open 限定回収）**: 封筒冪等の回収対象を **open（要確認/no）封筒に
  限定**・terminal のみなら新規起票＝**却下=非抑止の構造的成立**（fix2 の
  「同一材料=already_filed 回収」記述は撤回・§2C-4）。M1 部分失敗時は**封筒 open
  維持**・**同一封筒の再確定（新しい関所往復）が reconcile 入口**（P3-003c §4.1
  同型・§4B）。§6-27 の旧記述も撤回・書換え。
- **M04**: §6 に 28〜30 を追加（束ね正規化の分離・§2C 状態変化の hash 不変＋
  フィルタ吸収・部分失敗→open→再確定 reconcile の遷移列）＋ §6-27 書換え。
- 次レビュー: **R-SHOKUMU-PLAN-D4**（**凍結判定**・BASE=origin/main `6312112`・
  TARGET=shokumu-plan-design の fix3 commit）。→ 実施済み・結果は §8-4。

## 8-4. fix4 改定記録（R-SHOKUMU-PLAN-D4・2026-08-11・司令塔裁定。両時点残置）

D4 判定: H02/H03=RESOLVED・H01 分割 2 件＋M01＋M04-01。fix4 で以下を反映:

- **H01-01（裁定=canonical 保存方式）**: 封筒 candidates＝**未フィルタの canonical
  全候補（plan_hash と 1:1）**を保存（§4A）。§2C フィルタは **M1 create 直前の
  実行時適用のみ**へ移動（fix3 までの「封筒生成時に候補を除外」は撤回・表示は
  「起票済み」注記可）。状態反転（common 却下→full 確定）で共通行が復元起票される
  構造的成立を §2C-3 と §6-31 に明示。
- **H01-02（裁定=plan 横断冪等キー）**: plan_idem を
  `shokumu_plan:{case}:{municipality}:{person_id|-}:{form}` へ再々定義
  （**plan_hash 除去**＝業務上の起票単位として全 plan 横断で安定・grammar 更新・
  初版/fix3 キーは撤回）。**HIT 時は line_type 集合（plan_lines）比較——一致=skip
  回収／不一致=要確認（自動 merge・自動再発行禁止）**を凍結。§2C=共通行限定の
  前段表示フィルタ／§4B=全行類型を覆う最終防壁、の二層関係を再記述。
- **M01**: §4-v2.1 の保証文言を「**候補由来の M1 入力**に影響し得る上流変更は必ず
  plan_hash を変える」へ限定訂正（確定時再取得値・非内容的状態は対象外の根拠つき）。
- **M04-01**: §6 同期 5 点——(i) §6-31 反転ケースの復元起票 (ii) §6-3 を open 限定
  回収へ修正（§6-27 との衝突解消） (iii) §6-32 sibling_death 4 分岐（後 3 者=要入力）
  (iv) §6-33 plan_lines grammar（7 値 enum・sort・unique・不正拒否）
  (v) §6-34 plan_idem 内容不一致=要確認。
- 次レビュー: **R-SHOKUMU-PLAN-D5**（**凍結判定**・BASE=origin/main `6312112`・
  TARGET=shokumu-plan-design の fix4 commit）。→ 実施済み・結果は §8-5。

## 8-5. fix5 改定記録（R-SHOKUMU-PLAN-D5・2026-08-11・司令塔裁定=Codex 提案採用。両時点残置）

D5 判定: H01-01/M01（前巡）=RESOLVED・H01-02=UNRESOLVED＋新 M01。fix5 で反映:

- **H01（m1_fingerprint 二段構え）**: plan_idem（業務単位の安定キー）＋**正規化
  M1 実入力の fingerprint**（材料閉集合: request_items〔sort 済み〕・target 実値
  〔person_id/氏名/生年月日/本籍/住所/筆頭者〕・municipality〔採用名+App31 id〕・
  unit・様式・plan_lines／canonical JSON・key sort・配列 sort・SHA-256）を監査メタ
  へ保存。HIT 判定=「plan_idem 一致→fingerprint 比較→一致=skip 回収／不一致=
  要確認（自動 merge・自動再発行禁止）」。fix4 の plan_lines 集合比較は**不足として
  撤回**（count・target 実値・宛先引当ての相違を見逃す）。§4-v2.1 の plan_hash
  保証と fingerprint 検出の接続を追跡例（生年月日訂正→新 plan→HIT→不一致→要確認）
  つきで明記。§6-34 を fingerprint 不一致の実質検査へ書換え。
- **M01（canonical 順序）**: candidates の決定的並び順を tie-break 全順序
  （line_type 定義順→person_id→municipality→request_type→status→count・
  全键一致は 1 行併合）で凍結（§4A）。§4-v2 (7) App31 snapshot の並びにも同一
  規則を適用（hash 決定性の担保）。
- **§6 対照 4 形追加**（35〜38）: fingerprint 一致 skip／生年月日訂正→不一致／
  count 変化→不一致（fix4 比較の盲点）／並び順のみ相違→同値（正規化の実効性）。
- 次レビュー: **R-SHOKUMU-PLAN-D6**（**凍結判定**・BASE=origin/main `6312112`・
  TARGET=shokumu-plan-design の fix5 commit）。→ 実施済み・結果は §8-6。

## 8-6. fix6 改定記録（R-SHOKUMU-PLAN-D6・2026-08-11。両時点残置・遡及書き換えにしない）

D6 判定: CHANGES_REQUIRED（H01/M01=HIGH・両方 UNRESOLVED）。fix6 で反映
（着手前に build_channel_json :177-183・resolved_purpose :114-119・
PURPOSE_BY_UNIT :56-59・様式生成の target 参照〔:303-313/:350-353/:387-391〕を
実確認し、実出力形を §4B へ逐語引用）:

- **H01（fingerprint 材料の二層化）**: A層＝**channel_json 完成形そのもの**
  （request_items/municipality/target/purpose——purpose は resolved 後の文字列）・
  B層＝監査・引当て補強（app31_record_id/plan_lines/unit/form）。target は
  plan 経路の閉集合 **4 キー（対象者/生年月日/本籍/住所）**と確定——実装スキーマの
  既知 7 キーのうち**フリガナ・世帯主・筆頭者は非搭載**（様式生成が任意参照で
  非搭載でも成立する実装事実に基づく・構造 pin=§6-42）。不変条件「M1 実入力に
  影響する値は例外なく A層に含まれる。A層の漏れは設計違反」を明文化。fix5 の
  材料列挙・null 統一は撤回（履歴残置）。
- **H01（正規化規則の完全固定）**: UTF-8 bytes→SHA-256／separators=(",",":")／
  ensure_ascii=false／key 辞書順／型固定（count=int・他 str・算出時型変換禁止）／
  **全キー必須収載＋値なしは "" 統一・null 非使用**（欠落は保存境界で拒否）／
  **NFC 不採用**（byte 比較・採否と理由を明記）／同一 type request_items は
  **count 合算で併合**。
- **M01（App31 snapshot・方式A）**: snapshot 行へ **person_id を正式収載**し、
  完全順序＝line_type 定義順→person_id→市区町村名→app31_record_id→有効→
  fallback 定義順（ward→city→""）。**全キー完全一致行は 1 行へ併合**。
  **snapshot に存在しないキーを比較・順序決定に使わない**構造 pin（§6-47）。
  fix5 の「candidates と同一規則」指定は不完全として撤回（履歴残置）。
- **§6 増補（39〜47・9 形）**＋**1:1 宣言**（§6 対照と H01/M01 保証の対応が
  過不足なく存在する）。
- 次レビュー: **R-SHOKUMU-PLAN-D7**（**凍結判定の再走**・BASE=origin/main
  `6312112`・TARGET=shokumu-plan-design の fix6 commit）。→ 実施済み・結果は §8-7。

## 8-7. fix7 改定記録（R-SHOKUMU-PLAN-D7・2026-08-11・司令塔裁定。両時点残置）

D7 判定: CHANGES_REQUIRED（H01/M01=HIGH・残余 2 点のみ）。fix7 で反映:

- **H01（方式A・司令塔裁定）**: 同一 type の count 合算併合を「fingerprint 算出時の
  正規化」から「**plan 側 channel_json 完成前の正規化**」へ移動（§4B 正規化規則 5・
  fix6 の「算出時併合」と読める記述は撤回・残置）。併合済み channel_json を
  (a) fingerprint A層 (b) App30 create (c) M1 prepare の全てへ**同一 object**として
  渡すことを明記し、不変条件化——「**fingerprint へ渡した A層と App30 create へ
  渡した channel_json は byte 一致する。fingerprint 内部だけの併合は設計違反**」。
  §6-44 を 4 点へ増補（分割入力の併合／併合済みが実起票に使用／byte 一致／
  既存 M1 経由で様式1 PDF 1 枚・count 合計 2 通）。
- **M01（司令塔裁定）**: snapshot 完全順序の第 5 キー `有効` の比較規則を固定——
  **保存文字列の UTF-8 コードポイント昇順（"" 先頭）**＝市区町村名と同規則・
  値域に依存しない機械的順序（値域 grammar を保存境界で固定する場合も順序の
  一意性はコードポイント順で担保。fix6 の順序未指定は撤回・補完）。
  §6-46 を 3 形へ増補（有効のみ相違／fallback のみ相違／全 6 キー一致の併合）。
- **1:1 宣言を更新**（§6 対照〔34〜47＋増補〕と H01/M01 保証の対応が過不足なく
  存在することを再宣言）。
- 次レビュー: **R-SHOKUMU-PLAN-D8**（**凍結判定の再走**・BASE=origin/main
  `6312112`・TARGET=shokumu-plan-design の fix7 commit）。→ 実施済み・結果は §8-8。

## 8-8. fix8 改定記録（R-SHOKUMU-PLAN-D8・2026-08-11・最小同期。両時点残置）

D8 判定: CHANGES_REQUIRED——ただし **H01/M01 は両方 RESOLVED**（再改定禁止）。
残余は新規 **H02（§2D 監査メタ閉集合と m1_fingerprint 保存の同期漏れ）の 1 点のみ**。
fix8 で反映:

- **H02**: §2D の監査メタ閉集合へ `m1_fingerprint`（hex64）を追加し §4B と同期
  （fix3〜fix7 の旧閉集合は「fix8 で同期」明記の上残置）。受入条件 6 点を明示——
  キー存在／grammar `^[0-9a-f]{64}$`／App30 create 時保存／HIT 再照合は既存保存値の
  JSON parse 完全一致／欠落・不正 grammar・parse 不能は skip せず要確認（安全側）／
  §2D と §4B のキー集合 byte 一致の構造 pin。
- **§6 増補**: 48（閉集合同期 pin）・49（fingerprint 欠落/不正/parse 不能→要確認）。
  1:1 宣言を H02 込みへ更新。
- 次レビュー: **R-SHOKUMU-PLAN-D9**（**凍結判定・H02 同期確認の軽量再走**・
  BASE=origin/main `6312112`・TARGET=shokumu-plan-design の fix8 commit）。
  → 実施済み・**PASS**（下記 §9 凍結記録）。

## 9. 凍結記録（R-SHOKUMU-PLAN-D9 PASS・2026-08-11）

- **凍結日**: 2026-08-11。
- **凍結判定**: R-SHOKUMU-PLAN-D9 **PASS**（凍結適格・H02 RESOLVED・新規所見なし・
  TARGET=`43656ee`）。**司令塔裁定により設計凍結が確定**。
- **レビュー履歴（D 巡 9 回・fix 8 回）**: D1（凍結不適格 HIGH3+MEDIUM4）→fix1→
  D2（HIGH3+MEDIUM4）→fix2→D3（司令塔 4 裁定）→fix3→D4（H01 分割 2 件）→fix4→
  D5（H01-02+M01）→fix5→D6（H01/M01）→fix6→D7（残余 2 点）→fix7→
  D8（H02 のみ）→fix8→**D9 PASS**。
- **以後の変更**: **改定記録の追記＋再D巡・再凍結**による（暗黙の上書き禁止・
  遡及書き換え禁止・両時点残置の規律は凍結後も継続）。
- **実装票への引き継ぎ**: 凍結仕様全文（§0〜§7・裁定①〜⑧・§8 系改定記録の確定
  事項）＋テスト計画 §6 全 49 系統。実装票の起票・着手タイミングは司令塔裁定。
