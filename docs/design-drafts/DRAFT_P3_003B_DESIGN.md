# DRAFT: P3-003b 関所 side の前提設計（App36 projection・設計のみ・実装禁止）

- TASK_ID: P3-003B-D 設計票（設計のみ・コード/テスト実装禁止）／記録日 2026-07-28
- 目的: 凍結 `DRAFT_P3_003_ENVELOPE_FLOW.md`（以下「正本」）§3.2/§3.3 の App36
  projection を**実装可能にするための前提3点**を固定する——(1) App36 追加 field
  仕様 (2) 冪等キーの App36 上の実現方式と検索規則 (3) derive_heirs 出力→App36
  「続柄」「法定相続分」の表記写像表。
- 正本参照（**矛盾を作らない・編集しない**）: 正本 §3.2（3 phase）・§3.3（冪等キー=
  case_record_id＋person_id／H10 条件付き更新／human_state 保護／stale ガード）・
  §6（欠落補記）・`DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md` §3。
- **スコープ外（明記）**: E0–E3 effect level・放棄写像（declarations.renounced →
  App36 状態「放棄済み」）は **v2.4 正本（repo 外）依存**であり本票では固定しない
  （正本 §2.4／§3.6 OPEN のとおり）。本票は「機械が確定後に App36 へ projection
  する際の field・キー・表記」だけを対象とする。
- 実装現実の実査基盤（2026-07-28・read-only）: `hub/derivation_models.py`
  （`_ZOKUGARA_TO_RELATION`・`build_run_payload`・share の直列化）／
  `heir_derivation.py`（zokugara ラベル生成）／`config.EXPECTED_KINTONE_SCHEMA`
  の App36 実 field。

## 0. 前提の要約（実装現実・設計判断の土台）

- **result_payload は relation_key（ASCII enum）のみを保持**し、日本語の zokugara
  区分は `build_run_payload` で**落ちる**（正本 §4・PII 統制）。App36「続柄」
  （DROP_DOWN・日本語）へ写すには relation_key からの写像になる——が、relation_key
  は zokugara より**粗い**（representative が3区分を collapse・§3 論点1）。
- **person_id は App34 の `$id`（数字列）**。ただし**胎児は run 内合成 ID
  `胎児:F{n}`（出現順連番）**で、`build_run_payload` の採番は出現ごと＝
  **run を跨いで安定しない**（同一胎児が再導出で別番号になり得る）。冪等キーの
  片割れに person_id を使う設計に直接影響する（§2 論点）。
- App36「続柄」「状態」は DROP_DOWN で**値域が固定**（下表）。導出 9 区分の一部は
  dropdown に対応値が無い（§3 論点2）。

## 1. App36 追加 field 仕様（[人]が kintone に追加する際の仕様書）

正本 §3.3 の H10（条件付き更新）と冪等キー（case_record_id＋person_id）を実装可能に
するため、App36 に**新規2 field**が要る（正本 §6 の BLOCKED CU の具体化）。

| field 名（案） | 型 | 値の grammar | 検索可否 | 用途・根拠 |
|---|---|---|---|---|
| `current_derivation_run_id` | SINGLE_LINE_TEXT | 数字列（`^[0-9]+$`。DerivationRun.id=BigInt を文字列化） | **要・完全一致検索** | H10 条件付き更新の前提。各 App36 人物レコードが「どの run 由来か」を保持し、supersedes 連鎖の祖先判定に使う（正本 §3.3 H10） |
| `導出元人物ID` | SINGLE_LINE_TEXT | 数字列（App34 `$id`）または合成 `胎児:F{n}`（§2 論点で扱い確定） | **要・完全一致検索** | 冪等キー（case_record_id＋person_id）の person_id 片。App36 に person_id を保持する field が現状無いため新設（正本 §3.3 冪等キー＝registry_ingest._upsert_zaisan 型の person 単位一意） |

- **型の選択理由（SINGLE_LINE_TEXT・数値型でない）**: 既存 App30 の
  `案件レコードID`・`案件アプリID`、App36 の `案件レコードID` が SINGLE_LINE_TEXT の
  数字列で統一されている（実査）。kintone の NUMBER 型でも完全一致検索は可能だが、
  ID を数値計算しない・既存踏襲・前ゼロ等の表現差を避ける観点から SINGLE_LINE_TEXT
  を推奨。
- **検索クエリでの利用**: 両 field とも kintone の `field = "値"` 完全一致で引く
  （§2）。**インデックス相当の設定は kintone 側の運用**（[人]・実機作成時）。
- **既存 field との非干渉**: 追加は上記2点のみ。既存の機械由来 field（続柄／
  法定相続分／データ源）と human_state field（戸籍確認済／状態）には触れない
  （正本 §3.3 human_state 保護）。

## 2. 冪等キーの App36 上の実現方式と検索規則

- **冪等キー**（正本 §3.3）= `case_record_id ＋ person_id`。App36 上の実現:
  - `案件レコードID`（既存 field・SINGLE_LINE_TEXT）＝ case_record_id
  - `導出元人物ID`（§1 新設）＝ person_id
- **upsert 検索クエリ**（読取専用の探索・単票 API 不要の複数検索で可）:
  ```
  案件レコードID = "<case_record_id>" and 導出元人物ID = "<person_id>"
  order by $id asc limit 2
  ```
  - **0 件** → 新規 insert（続柄/法定相続分/データ源/current_derivation_run_id を
    機械由来として書く。human_state field は初期値のまま）。
  - **1 件** → H10 条件付き更新（正本 §3.3）へ。`current_derivation_run_id` が
    新 run の supersedes 連鎖の**祖先である場合のみ**機械由来 field を差分更新し、
    成功時に `current_derivation_run_id` を新 run へ進める。
  - **2 件以上** → 冪等キー重複＝異常。**書かず要確認**（業務チャネル警報・
    件数と record_id のみ・氏名非出力）。
- **検索の値の安全化**: case_record_id・person_id（数字列）は kintone query へ
  埋める前に grammar（`^[0-9]+$`）を検証。胎児合成 ID を許容する場合は
  `^胎児:F[0-9]+$` も許可 grammar に含める（§2 論点の裁定に従う）。
- **TOCTOU の扱い**: 検索→更新の間隙は正本 §2.2 の受容（best-effort 冪等）と同じ。
  完全な原子性は持たない（稀な二重 projection は human_state 保護と stale ガードで
  実害を抑止）。**新規に原子性機構を作らない**（正本と整合）。

### 2A. 未裁定論点（胎児 person_id の run 跨ぎ非安定・[人]裁定要）

- **問題**: 胎児行の person_id は `胎児:F{n}`（run 内出現順・build_run_payload
  fix5）で、**再導出で同一胎児が別番号になり得る**。冪等キー
  case_record_id＋person_id が run を跨いで胎児を一意に追跡できない。
- **選択肢**:
  - **(A) 胎児行は App36 へ projection しない**（confirmed でも保留）。理由: 胎児は
    出生擬制（886条）で確定前提が流動的・provisional=True（正本 §3A）とも整合。
    実氏名 person も無い。**推奨**——正確性を損なわず、非安定 ID を冪等キーに
    載せない。App36 反映は出生後の実 person 化を待つ別票。
  - (B) 胎児行も projection し、person_id に合成 ID を格納。難点: run 跨ぎで重複行が
    増殖・追跡不能（非推奨）。
  - (C) 胎児に案件内安定 ID を別途採番する仕組みを新設。難点: build_run_payload の
    採番規約（正本 §? fix5）改定を伴い波及大（本票スコープ外・別票）。
- **推奨=(A)**。本票では「胎児行は App36 projection 対象外（confirmed 後も出生まで
  保留）」を暫定前提とし、確定は [人]。

## 3. derive_heirs 出力 → App36「続柄」「法定相続分」写像表

### 3.1 続柄（DROP_DOWN・値域固定）

- **App36「続柄」の実 dropdown 値域（実査）**: 配偶者／子／直系尊属／兄弟姉妹／
  甥姪（代襲）／受遺者（相続人外）／その他（7値）。
- **derive_heirs の zokugara 区分 9 種**（`_ZOKUGARA_TO_RELATION` の8キー＋
  `数次承継` 前方一致）と relation_key（result_payload に保存される ASCII enum）:

| # | zokugara 区分（原文） | relation_key（保存値） | App36 続柄への写像案 | 対応可否 |
|---|---|---|---|---|
| 1 | 配偶者 | spouse | 配偶者 | ○ 完全一致 |
| 2 | 子 | child | 子 | ○ |
| 3 | 直系尊属 | lineal_ascendant | 直系尊属 | ○ |
| 4 | 兄弟姉妹 | sibling | 兄弟姉妹 | ○ |
| 5 | 甥姪（代襲） | representative | 甥姪（代襲） | △ representative 共有 |
| 6 | 孫（代襲） | representative | **対応値なし** | ✗ dropdown 欠落＋粒度喪失 |
| 7 | 再代襲（曾孫等） | representative | **対応値なし** | ✗ 同上 |
| 8 | 胎児 | fetus | **対応値なし**（§2A で projection 外を推奨） | ✗ |
| 9 | 数次承継（No.… の …） | successive | **対応値なし** | ✗ |

- **設計上の核心問題（2つ）**:
  - **論点1（粒度喪失）**: result_payload は relation_key のみ保持し、
    representative は 孫（代襲）／甥姪（代襲）／再代襲 を **collapse** する。
    projection は relation_key からしか続柄を決められないため、
    **relation_key=representative から「孫」か「甥姪」かを復元できない**。
  - **論点2（dropdown 欠落）**: 孫（代襲）／再代襲／胎児／数次承継 に対応する
    dropdown 値が App36 に無い。

### 3.2 未裁定論点（続柄写像・[人]裁定要）

- **論点1（relation_key の粒度）の選択肢**:
  - **(A) result_payload に非PIIの続柄区分コードを追加**（zokugara 区分の ASCII
    enum・氏名等を含まないため PII ではない）。projection は区分コード→続柄で
    total 写像可能。**推奨**——正確性が要件・区分は法的カテゴリで非PII。
    ただし **§3.5 payload schema／P3-001 の改定を伴う**（正本改定と同時＝別票の
    前提。本設計票は「必要性の明文化」まで）。
  - (B) relation_key のまま projection し、representative は一律「甥姪（代襲）」。
    難点: 孫（代襲）を誤表示（不可）。
  - (C) representative 行は続柄を書かず App36「状態=代襲」のみ立てる。難点:
    続柄が空欄になり一覧性を損なう（次善）。
- **論点2（dropdown 欠落）の選択肢**:
  - **(A) [人] が App36「続柄」dropdown に不足値を追加**（孫（代襲）／
    再代襲（曾孫等）／数次承継。胎児は §2A で projection 外を推奨のため任意）。
    **推奨**——(論点1-A) と組み合わせると total な続柄写像が成立。
  - (B) 欠落区分は「その他」へ寄せる。難点: 粒度喪失・法定相続分との突合が困難
    （次善）。
- **統合推奨**: 論点1-(A)＋論点2-(A) を採ると、**区分コード→拡張 dropdown の
  total 写像**が実装できる（本票の写像表がその仕様原型）。両方とも kintone 実機
  変更／schema 改定を伴うため **[人] ゲート**であり、実装票の前提になる。

### 3.3 法定相続分（SINGLE_LINE_TEXT）

- **保存形**: result_payload の share は `"{numerator}/{denominator}"`（既約分数・
  例 `"1/2"`・`"1/6"`。`build_run_payload` の直列化を実査）。
- **App36「法定相続分」への表記写像規則（案）**:
  - `"n/d"` → **`"d分のn"`**（日本語慣用・例 `"1/2"`→`"2分の1"`・`"1/6"`→
    `"6分の1"`）。numerator=n・denominator=d。
  - **単独相続（`"1/1"`）の表記**: 小裁定——(A)`"1分の1"`（機械的・写像規則が
    単純）／(B)`"全部"`（法律実務の慣用）。**推奨=(A)**（写像規則を分岐させない・
    表示整形は画面側の別責務）。
  - **share=None**（held で相続分未確定の行）: 法定相続分は**空欄**（機械は書かない）。
- **端数・約分**: share は Fraction 厳密演算の既約分数（heir_derivation §900 演算）で
  既に約分済み。projection 側で再約分・四捨五入は**しない**（丸めは相続分の意味を
  壊す）。分子分母をそのまま `"d分のn"` に流す。

## 4. 実装票への申し送り（本票が確定した前提）

- App36 追加 field（§1）・冪等キー検索規則（§2）・法定相続分表記規則（§3.3）は
  **本設計で確定**（実装票はこの仕様で書ける）。
- **続柄写像（§3.2）は [人] 裁定と kintone 実機変更が前提**——論点1-(A)（区分コード
  追加＝§3.5/P3-001 改定）と論点2-(A)（dropdown 拡張）が解けるまで、P3-003b の
  続柄 projection は**実装 BLOCKED**。暫定運用は論点1-(C)（続柄空欄＋状態=代襲）で
  degrade 可能だが、正確性要件との兼ね合いで [人] 判断。
- 胎児行（§2A）は projection 外（推奨）＝実装票では対象から除外。
- E0–E3・放棄写像はスコープ外（v2.4 正本確認後の別票）。

## 5. 実機確認事項（[人]・実装前に要確定）

1. App36 への `current_derivation_run_id`／`導出元人物ID` の実機追加・field コード
   確定・完全一致検索の可否（§1）。
2. App36「続柄」dropdown への不足区分追加の可否・追加値の文言（§3.2 論点2）。
3. result_payload への続柄区分コード追加（§3.5/P3-001 改定）の起票判断
   （§3.2 論点1-(A)）。
4. 胎児行を App36 projection 外とする暫定前提の承認（§2A）。
5. 単独相続の法定相続分表記（`"1分の1"` か `"全部"`・§3.3）。
