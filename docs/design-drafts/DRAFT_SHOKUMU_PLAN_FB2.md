# DRAFT: SHOKUMU-PLAN-FB2 — 受任確定フックによる請求案の自動起票

- status: **DRAFT**（凍結は D 巡後・R-FB2-D1 から）
- TASK_ID: DOCS-FB2（起草）／実装は別票
- 位置づけ: SHOKUMU-PLAN 凍結票（DRAFT_SHOKUMU_PLAN.md・FROZEN・main 取り込み済み）
  の**裁定②で別票残置された「受任フック自動起票」**の消込。起動時点のみを扱い、
  plan の内容・優先順位・防壁は凍結票のまま一切変えない。

## 1. 実地確認（2026-08-11・rg＋実ファイル読解・全量引用）

### 1.1 凍結票の裁定②該当記載（DRAFT_SHOKUMU_PLAN.md）

§5 末尾（668-672行・fix1 追記）:

> **fix1 追記（Codex 助言・裁定②の適用範囲の明確化）**: 裁定②（起動=語彙のみ）は、
> H系列③ §1 の旧記述「受任確定と同時に M1 職務上請求を自動起票」を**起動時点に
> ついてのみ上書き**する（初版は受任フックでなく[人]の語彙指示で起動）。
> **除票/附票の最優先性・内容・優先順位は不変**（マトリクスの凍結は §3-1 のまま・
> 受任フック起動の復活は運用実績後の別票裁定）。

§8-1 fix1 改定記録（883-885行）:

> - **裁定②の適用範囲**: H系列③の旧「受任時自動起票」を**起動時点についてのみ**
>   上書きする旨を §5 末尾に明記（内容・優先順位は不変）。

実装側の対応（dispatch_bot/shokumu_plan_task.py docstring・merge 済み）:

> 正本: DRAFT_SHOKUMU_PLAN.md（FROZEN）。裁定②=**起動は語彙のみ**（「請求案を
> 出して」・受任フック自動起票は別票）。

### 1.2 受任確定の設計正本（H系列・docs/souzoku-houki/）

01-flow.md:35・40-41:

> 🤖 **受任確定トリガー = CloudSign 締結 + Stripe 決済完了の両方**が揃った時点で
> 🤖 委任契約書 …… CloudSign 電子締結（[4]。締結+決済完了=受任確定） — 電子のまま
> 🤖 手続委任状 …… **必ず紙**（家裁提出のため署名押印必須）。受任確定と同時に

10-koseki-matrix.md:11（裁定②が起動時点のみ上書きした旧記述の正本）:

> | **被相続人の住民票除票（または戸籍の附票）** | **管轄家裁（最後の住所地）の確定に
> 必要**（09 §2.3 の管轄マッチの入力）。続柄に関わらず必ず取得 | **受任確定と同時に
> M1 職務上請求を自動起票**（書類収集ループの最初の請求対象。管轄が確定しないと
> 申述先が決まらないため最優先） |

08-implementation-plan.md（H8/H9・受任確定の実装計画）:

> :97   LINE 定型フロー・**受任確定トリガー = 締結+決済完了の両方**（01 [4]）・滞留リマインド
> :101  [ ] 締結のみ/決済のみでは受任にならないテスト PASS
> :103  ### H9 受任判断フロー + 受任時フック（委任状・本人確認・チェックリスト初期化）
> :106  受任確定フック: **手続委任状の M4 自動起票**（委任状＋記入例＋返信用封筒・

### 1.3 現行実装の実査（重要な実装現実）

- **時効援用 unit（App21）には受任遷移の実装がある**——ただし
  **【fix1・FB2-01 訂正・補強】App21 にも「締結+決済」の結合遷移は存在せず、
  2 経路は独立**:
  - Stripe checkout 完了 → App21 **新規レコード作成**（`ステータス=決済完了`・
    `入金状況=入金済み`・main.py:1363）——CloudSign 側の状態を参照しない。
  - CloudSign 締結 Webhook → 書類詳細 API で真正性照合（fail-closed・照合失敗は
    「受任への自動遷移は行っていません」）→ 成功時のみ
    `update_kintone_status(document_id, "受任")`（cloudsign_webhook.py:537）——
    **決済成立の照合はしない**（status 更新のみ・**相互照合なし**）。
  - ＝「両方が揃ったことを機械が判定する結合点」は**時効援用側にも存在しない**
    （運用順序が事実上の担保）。FB2 が要る結合状態機械は**完全新設**（§3.0）。
- **相続放棄 unit（H系列・App26 系）の受任確定はコード未実装**——H8（契約決済）・
  H9（受任判断・受任時フック）は 08 実装計画のタスクであり、cloudsign_webhook /
  Stripe 起票の現行結線は **App21（時効援用）のみ**（`KINTONE_APP_ID=21` 固定・
  rg で相続側の受任遷移コードはヒットなし）。
  **＝本票が結線すべき「受任確定イベント」は現行コードに存在しない**（§3 裁定①）。
- **語彙起動経路（merge 済み・flag OFF）**: dispatch_bot/registry.py:266-290
  （`task_type="shokumu_plan"`・visible_fn=flag 連動・execute_fn=
  shokumu_plan_task.execute）→ `build_plan`（read-only・problems ありは道案内応答
  ＝起票なし fail-closed）→ `file_plan_envelope`（App30 要確認・封筒冪等キー
  `shokumu_plan:{case_record_id}:{plan_hash}`・**open 限定回収**〔hub/shokumu_plan.py
  :758-780〕）。M1 側の最終防壁は §4B の plan_idem／m1_fingerprint（凍結済み）。

## 2. 要件

- R1: **受任確定（締結+決済の両方成立）を起点に plan 生成→提案封筒の起票までを
  自動起動**する。**確定は従来どおり関所の人**（「機械は確定しない」不変——
  自動化されるのは提案の起票までで、M1 の起票・承認・発送の全防壁は不変）。
- R2: **防壁は凍結票の既存機構をそのまま流用し、新しい防壁を作らない**——
  封筒冪等（plan_hash・open 限定回収）・§2C 実行時フィルタ・§4B plan_idem／
  m1_fingerprint・§2 fail-closed（problems→起票なし）。
  **【fix2・R2 文言の限定】**本要件は「**plan 内容・M1 領域の防壁を新設しない**」
  の意に限定する——engagement_event 台帳（§3.2）は**新起動経路（受任フック）固有
  の冪等制御**であり、plan/M1 の防壁ではなく**新設が正当**（誤読防止の明記）。
- R3: flag（既定 OFF）配下。`SHOKUMU_PLAN_ENABLED`（語彙・plan 生成の親 flag）が
  OFF なら自動起動も**構造的に不発**（二重ゲート・親 flag 優位）。
- R4: 失敗・結果不明は封筒規律準拠で回収する（沈黙させない・§4）。

## 3. 設計骨子

### 3.0 受任確定の結合状態機械（前提設計・fix1/FB2-01 追加）

**事実定義「受任確定 = 締結 + 決済の両方」（H系列正本 01 [4]）は不変**。本節は
その事実を機械が一度だけ正しく判定するための**結合状態機械の要件を閉集合**で
定義する（§1.3 のとおり結合点は現行に存在しない＝新設。詳細設計は裁定①=(a) の
とおり H9 実装票で本要件を満たして行う）:

1. **(i) 事実の保存先**: 締結事実・決済事実は **App26 案件レコードの独立
   フィールド**に保存する（webhook の揮発イベントを正としない）。
2. **(ii) 決定的相関キー**: 両 webhook が**同一 App26 案件へ決定的に相関**できる
   キー（CloudSign documentID・Stripe 決済 ID の案件フィールド保持）を持つ。
   氏名・メール等の曖昧照合は使わない。
3. **(iii) 到着順不変の一回成立**: 署名→決済・決済→署名の**どちらの到着順でも
   「受任確定」が一度だけ成立**する遷移（後着側が両立を判定して成立させる）。
4. **(iv) 冪等・競合**: webhook 再送・同時到着でも成立は一度（決定的キーによる
   冪等判定・同時到着の競合は kintone revision 楽観ロック等で一方に収束）。
5. **(v) 正本フィールド**: 「受任確定」の**正本フィールド・値域（閉集合）・
   更新主体（結合状態機械のみが書く）**を一意に定める。
6. **(vi) plan 起動前の再検証**: フック発火後・plan 起動前に **App26 正本を
   再取得し両条件を再検証**する（`refetch_and_check` の既存流儀・イベント記憶を
   信用しない）。
7. **(vii) fail-closed**: 相関不能・複数一致・片方のみ成立は **write 0**
   （起動しない・§3.2 の problem 通知へ）。

### 3.1 起動条件（受任確定の検知）

- **検知するイベント**: 「締結+決済の両方が成立した瞬間」＝両条件の**後着**が
  揃った時点で 1 回。片方のみでは起動しない（08 :101 のテスト要件と同型）。
- **検知点＝裁定①で確定: (a) H9 受任時フックへの結線**（fix1 反映）——
  設計正本どおり H8/H9 実装票の着地後にフック 1 点を追加（手続委任状 M4 自動起票と
  同じフック点）。**§3.0 の結合状態機械の正本遷移を先に設計することを前提に含む**
  （H9 実装票が §3.0 の 7 要件を満たす遷移を実装し、本票のフックはその成立点に
  結線する）。
  - ~~(b) cloudsign_webhook の unit 分岐拡張~~（**不採用**——決済照合を webhook
    handler へ埋め込む H8 の部分先行実装となり、結合状態機械の正本を持たないまま
    片側 handler に判定が散る）
  - ~~(c) kintone Webhook（App26 ステータス変更）購読~~（**不採用**——「受任確定」
    正本フィールドの更新主体が定まる前に購読側を作ると、手入力遷移も発火源に
    なり §3.0(v) の更新主体一意性と衝突する）
- いずれの方式でも、**受任確定の判定源は kintone の案件状態**（締結・決済の
  記録フィールド）に一本化し、Webhook payload を信用せず**最新レコード再取得で
  再判定**する（`refetch_and_check` の既存流儀）。

### 3.2 自動起動の範囲と経路

- 実行内容は**語彙経路と同一の部品を共用**する: `build_plan(case_record_id)` →
  problems なしのときのみ `file_plan_envelope(materials)`。**新しい plan 生成
  経路・新しい起票経路は作らない**（呼出し元が語彙 task か受任フックかの差のみ）。
- **problems あり（判定不能）＝裁定③確定: (A) 通知あり**（fix1・FB2-04 具体化）:
  - **閉集合の problem code → 固定文面の写像**で通知する（自由文の生成をしない。
    code 集合は build_plan の条件未充足列挙＋§3.0(vii) の相関系を実装票で閉じる）。
  - 通知内容は**案件番号・件数・安全な条件名のみ**——**氏名・住所・本籍・
    生年月日・戸籍内容・例外本文は非搭載**（実装票で sentinel テストとして pin）。
  - **通知失敗で plan 封筒の作成や判定を解除しない**（通知は best-effort・
    判定/起票の成否と独立。起票なしの判定は通知失敗でも維持）。
- **二重起動の冪等＝裁定②確定: (B) 基礎＋決定的 engagement_event_id**
  （fix1・FB2-02 具体化）:
  - 受任確定の成立ごとに**決定的 engagement_event_id**（乱数不使用）を発番し、
    **イベント処理状態**を永続化する。**結果状態は閉集合
    `{pending / problem_held / envelope_filed / failed / reconciled}`**。
  - **【fix2・FB2-05】engagement_event_id の構成を凍結**:
    - **grammar**: `shokumu_engagement:{case_record_id}:{generation}`——
      固定 namespace（ASCII リテラル `shokumu_engagement`）＋ App26 record ID
      （数字列 `^[0-9]{1,10}$`・既存 case grammar と逐語一致）＋ generation。
      **区切りはコロン・この順序で固定**（要素追加・順序変更は本 DRAFT 改定と
      同時のみ）。
    - **【fix3・FB2-09 裁定=案A】初版は一案件一受任のみ・generation は常に 1**
      （grammar の `{generation}` は**将来拡張のための桁**であり初版は**定数 1**）。
      ~~初回受任確定=1・結合状態機械が再成立ごとに +1 で採番する単調増加値~~
      ~~世代が進むのは結合状態機械が受任確定の再成立を正規に判定した場合のみ~~
      （fix2 の「再成立ごと +1」記述は**撤回**——再成立の世代状態機械〔受任取消・
      再契約〕は未設計であり、採番規則だけ先に置くと未定義動作の入口になる）。
      - **generation=1 の event 台帳が既存の案件で結合状態機械が再成立を検知した
        場合＝自動処理せず write 0＋要確認通知**。
      - **generation>1（受任取消・再契約の世代状態機械）は将来の別票で設計**。
    - **documentID・決済 ID の訂正・差替えは新 event_id を生成しない**——
      同一 generation の事実材料が変わった場合は「**内容不一致の要確認**」へ
      倒す（同一受任の二重起票経路を閉鎖）。
  - **【fix2・FB2-05／fix3・FB2-08 同期版】状態遷移表（凍結・これが唯一の正。
    §3.2 決定的 join・§4 reconcile の記述は本表を参照する——本表にない遷移を
    reconcile も起動経路も行わない）**:

    | from | to | 実行主体 | 条件 |
    |---|---|---|---|
    | （新規） | pending | 受任フック | 成立時の先行確保（UNIQUE 収束） |
    | pending | problem_held | 自動起動経路 | build_plan 条件不充足 |
    | pending | envelope_filed | 自動起動経路 | 封筒作成成功 |
    | pending | envelope_filed | **reconcile** | idem キー一致の **open 封筒 1 件・内容一致**（ACK-loss 回収） |
    | pending | reconciled | **reconcile** | **terminal 封筒 1 件・内容一致** |
    | pending | failed | 自動起動経路 **又は** reconcile | 実行失敗・例外／idem キー保存済み・**封筒 0 件を確認** |
    | failed | envelope_filed | **reconcile**（**照合による状態収束・起動経路の再実行ではない**） | 既存 **open** 封筒 1 件・**内容一致** |
    | failed | reconciled | reconcile | 既存 **terminal** 封筒 1 件・**内容一致**（~~既存封筒 1 件~~——fix4・FB2-11 で terminal へ限定） |
    | problem_held | reconciled | reconcile | **語彙起動による対応封筒を確認** |

    - **terminal 状態 = `envelope_filed` / `reconciled`**（以後の遷移なし）。
    - **同一状態への冪等再実行 = no-op**（重複 webhook・再実行で状態は変わらない）。
    - **禁止遷移（表にない遷移）= write 0＋要確認通知**（黙って上書きしない）。
    - **CAS/UNIQUE 競合・不明状態 = fail-closed**（起票せず要確認へ）。
    - **problem_held / failed からの自動起票はしない**——回収は**人の語彙起動**
      または **reconcile の照合収束**のみ（機械の自動再試行を作らない）。
    - **【fix3・FB2-08】failed からの回収の一意化**: **自動再試行はしない**。
      reconcile 照合で**封筒 0 件なら failed 維持＋通知**（勝手に作らない）。
      **封筒の新規作成は人の語彙経路のみ**。人が語彙起動で封筒を作った後は、
      **次回 reconcile が failed→envelope_filed 又は reconciled へ収束**させる。
    - **【fix4・FB2-11】failed 行 2 遷移の排他化**: failed→envelope_filed は
      「**open** 封筒 1 件・内容一致」・failed→reconciled は「**terminal** 封筒
      1 件・内容一致」で**観測が排他**——**同一 from 状態×同一観測で遷移先が
      一意に決まる**（pending 時の ACK-loss 回収規則〔open→envelope_filed／
      terminal→reconciled〕と**対称**）。旧条件「既存封筒 1 件」は撤回
      （open 含みでは 2 行が同時成立し**表の決定性が崩れる**ため）。
  - ~~単純 boolean（発火済みフラグ）~~ **不採用**——boolean では problem_held
    （条件未充足で起票見送り）後に「発火済み」となり**回収不能**になる（状態
    閉集合なら problem_held を reconcile・再判定の対象にできる）。
  - **状態保存と封筒作成の競合・部分失敗＝【fix2・FB2-06】処理順を固定し
    決定的 join を確立**:
    1. `pending` 確保（engagement_event_id の UNIQUE で同時発火を一方に収束）
    2. `build_plan`（read-only）
    3. **plan_hash 確定**
    4. **封筒 idem キー（`shokumu_plan:{case}:{plan_hash}`）を台帳へ先行保存**
    5. 封筒作成（`file_plan_envelope`）
    6. `envelope_filed` へ更新
    ——**イベント台帳と App30 の間に原子的トランザクションは存在しない前提**で、
    手順 4 の先行保存が台帳↔封筒の**決定的 join キー**になる（封筒作成後・状態
    更新前のクラッシュでも、台帳の idem キーから封筒を一意に照合できる）。
    - **reconcile の照合**: 台帳の idem キーで App30 を **open/terminal を問わず
      read-only 検索**（決定的 join）。**既存封筒が terminal ＝新規自動起票せず
      `reconciled` へ収束**。検索結果 **0 件＝failed 維持＋通知（fix3・FB2-08 で
      一意化——~~回収続行（failed 系の再実行対象）~~は撤回・自動再試行しない）**・
      **1 件＝状態収束**・**複数件・内容不一致＝自動統合・自動再発行せず要確認**。
      **照合の結果として許される状態遷移は §3.2 の状態遷移表が唯一の正**
      （fix3 相互参照）。
    - **既存の open 限定回収規律（凍結票 `file_plan_envelope`）は不変**——
      本節の「terminal 問わず検索」は reconcile の**読取専用照合**にのみ適用され、
      起票側の冪等（open 限定回収）には手を入れない。
    - **ACK-loss テスト**（手順 5-6 間クラッシュ・4-5 間クラッシュの両面で
      二重起票ゼロ・reconcile 収束を実測）を**実装票の受け入れ条件へ追加**。
      **【fix4・FB2-11】failed 分岐 4 件を受け入れ条件へ直接固定**:
      (1) failed＋open 封筒 1 件・内容一致 → `envelope_filed`
      (2) failed＋terminal 封筒 1 件・内容一致 → `reconciled`
      (3) failed＋封筒 0 件 → `failed` 維持＋通知
      (4) failed＋複数件又は内容不一致 → **write 0＋要確認**。
  - **人が語彙から起動する既存経路は別扱いで常設維持**（本状態機械のゲート外・
    いつでも人が再起動できる回収口）。

### 3.3 flag

- 新設 `SHOKUMU_PLAN_AUTOFILE_ENABLED`（既定 OFF）。判定は
  `shokumu_plan_enabled() and autofile_enabled()` の**二重ゲート**（親 flag OFF で
  自動起動も不発・rollback は子 flag 削除で即 OFF・語彙経路は不変）。
- **【fix1・FB2-03】二重 flag の凍結条件（実装票の受け入れ条件へ pin）**:
  - **flag 判定は外部読取りより前**に置く（kintone・DB への一切のアクセスの前）。
  - **片方でも OFF なら build_plan・kintone 検索・封筒操作・通知のすべてが 0 回**
    （I/O ゼロ・ログのみ可）。
  - **両 flag とも既定 OFF**。
  - **4 象限（親×子の ON/OFF）を直接テスト**する（mock の呼出し回数 0 の assert
    込み）——本 4 点を実装票の受け入れ条件とする。

## 4. 失敗・結果不明の回収（封筒規律準拠）

- **起動失敗（build_plan 例外・kintone 不達）**: 握らず業務 LINE 通知（分類のみ）＋
  ログ。**語彙経路が常設の代替入口**（「請求案を出して」で人がいつでも再起動
  できる＝回収経路が構造的に存在する）。
- **ACK 喪失（起票後クラッシュ・結果不明）**: 封筒冪等の open 限定回収が再実行を
  吸収（既存 `file_plan_envelope` の「既存封筒 No を回収しました」応答と同じ実体）。
- **受任確定イベント自体の取り零し＝裁定④確定: reconcile 導入**（fix1 反映）——
  **「受任正本（App26）× イベント処理状態（engagement_event_id 台帳）×
  対応封筒（App30）」の三面照合**を**日次**で実行し、不整合（受任確定済みなのに
  状態なし・pending/problem_held の滞留・envelope_filed なのに封筒不在等）を
  **検出時は通知・`problem_held`/`reconciled` への held 化まで**とする
  （**自動起票はしない**——起票は~~次のフック発火 or~~ **語彙経路の人の判断のみ**。
  【fix2・FB2-07】「次のフック発火」は撤回——同一受任 generation のフックは
  一度きり〔engagement_event_id の一回性〕であり再発火は設計上存在しない。
  起票の再試行入口は人の語彙経路のみ）。
- **【fix2・FB2-07】状態意味の分離（本文固定）**:
  - `problem_held` = **条件不充足で人の対応待ち**（build_plan の problems・
    §3.0(vii) の相関系。対応後の起票は人の語彙経路）。
  - `failed` = **実行失敗で回収待ち**（例外・kintone 不達。回収=~~再実行 or~~
    **reconcile 照合収束または人の語彙起動のみ**〔fix3・FB2-08 一意化——
    起動経路の自動再実行は存在しない〕）。
  - `reconciled` = **照合完了・新規自動起票が不要と確認された終端**。

## 4a. 実装・点火前提（[人]ゲート一覧・fix3/FB2-10 新設）

**いずれも凍結条件ではなく実装・点火前提**（本 DRAFT の凍結を妨げない）。4 段分離:

1. **(i) App26 CU フィールド追加（[人]専権）**: 締結事実／決済事実／CloudSign
   documentID／Stripe 決済 ID／受任確定正本／generation 保存先。
   **field code・型・値域・schema 監視（EXPECTED_KINTONE_SCHEMA 追随）は
   実装票で固定**。**CU 未適用時は fail-closed**（結合状態機械は成立判定せず・
   自動起動は発火しない）。
2. **(ii) event 台帳 migration**（コード実装票の対象・alembic・P3-001 流儀）。
3. **(iii) flag 投入**: `SHOKUMU_PLAN_AUTOFILE_ENABLED`（[人]・親 flag
   `SHOKUMU_PLAN_ENABLED` との二重ゲートは §3.3）。
4. **(iv) 本番点火**（段取り書方式・[人]——ブロックA点火の §8.1 型: 事前
   read-only 検査→点火→スモーク→観測）。

## 5. 裁定欄（司令塔）

| # | 論点 | 選択肢 | 状態 |
|---|---|---|---|
| ① | 受任確定の検知点 | (a) H9 フック結線 / (b) cloudsign unit 分岐 / (c) kintone Webhook 購読 | **RESOLVED＝(a) H9 フック結線**（fix1 裁定——正本の想定形。§3.0 の結合状態機械の正本遷移を先に設計する前提込み。(b)(c) の不採用理由は §3.1 に転記済み） |
| ② | 受任フックの 1 回性 | (A) 封筒冪等のみ (B) 発火記録あり | **RESOLVED＝(B) 基礎＋決定的 engagement_event_id＋結果状態閉集合 {pending/problem_held/envelope_filed/failed/reconciled}**（fix1 裁定——単純 boolean は problem_held 後の回収不能のため不採用・§3.2） |
| ③ | problems 時の通知 | (A) 業務 LINE 道案内 (B) 通知なし | **RESOLVED＝(A) 通知あり**（fix1 裁定——閉集合 problem code→固定文面・PII 非搭載 pin・通知失敗で判定を解除しない・§3.2） |
| ④ | 取り零し reconcile | (A) 置かない (B) 日次検出→通知のみ | **RESOLVED＝導入**（fix1 裁定——受任正本×イベント処理状態×対応封筒の三面照合・日次・検出時は通知と held 化まで・自動起票しない・§4） |
| ⑤ | 起動対象 unit | (A) 相続放棄のみ (B) 相続一般へ展開 | **RESOLVED＝(A) 初版は相続放棄のみ**（fix1 裁定——PLAN_UNIT 前提の実勢どおり。一般相続への拡張は別票） |

## 6. 両時点残置

- 本 DRAFT は初版。改定は fix 節を追記し、初版記述は撤回理由と併せて残す。

## 7. fix1 改定記録（R-FB2-D1・2026-08-12・全所見 ACCEPT＋裁定5件織り込み）

- **FB2-01（HIGH）**: §1.3 を訂正・補強——「App21 にも締結+決済の結合遷移は
  存在せず 2 経路は独立（相互照合なし）」を明記。§3.0 新設＝受任確定の結合状態
  機械の 7 要件閉集合（(i)保存先〜(vii)fail-closed）。事実定義「受任確定=締結+
  決済の両方」（H系列正本）は不変であることを明記。
- **FB2-02（HIGH）＋裁定②**: 1 回性を (B) 基礎＋決定的 engagement_event_id＋
  結果状態閉集合で確定。boolean 不採用理由（problem_held 後の回収不能）を残置。
  状態保存×封筒作成の競合・部分失敗の回収を定義。語彙経路は別扱い常設維持。
- **FB2-03（MED）**: 二重 flag の凍結条件 4 点（判定位置・片方 OFF で外部作用
  0 回・両既定 OFF・4 象限直接テスト）を実装票の受け入れ条件として pin。
- **FB2-04（MED）＋裁定③**: 通知あり（(A)）で確定——閉集合 problem code→固定
  文面写像・案件番号/件数/安全な条件名のみ（氏名・住所・本籍・生年月日・戸籍
  内容・例外本文の非搭載 pin）・通知失敗で判定解除しない。
- **裁定①**: (a) H9 フック結線で確定（結合状態機械の正本遷移を先に設計する前提
  込み・(b)(c) 不採用理由を §3.1 へ転記）。**裁定④**: reconcile 導入（三面照合・
  日次・通知/held まで）。**裁定⑤**: 初版は相続放棄のみ。
- 裁定欄 5 件を全件 RESOLVED 化（各行に裁定＋1行理由）。

## 8. fix2 改定記録（R-FB2-D2・2026-08-12・全所見 ACCEPT）

- **FB2-05（HIGH）**: engagement_event_id を凍結——grammar
  `shokumu_engagement:{case_record_id}:{generation}`（固定 namespace・数字列
  case grammar 逐語一致・世代=正の整数の単調増加・コロン区切り・順序固定）。
  documentID/決済 ID の訂正・差替えは**新 event_id を生成せず「内容不一致の
  要確認」**（二重起票経路の閉鎖）。**状態遷移表を凍結**（許可遷移7行・実行主体・
  terminal={envelope_filed, reconciled}・同一状態冪等 no-op・禁止遷移=write 0＋
  要確認通知・CAS/UNIQUE 競合と不明状態=fail-closed・problem_held/failed からの
  自動起票なし）。
- **FB2-06（HIGH）**: イベント台帳×封筒層の**決定的 join** を確立——処理順を
  6 段（pending 確保→build_plan→plan_hash 確定→**封筒 idem キーの台帳先行保存**→
  封筒作成→envelope_filed）で固定。reconcile は台帳 idem キーで open/terminal
  問わず read-only 検索（0 件=回収続行・1 件=状態収束・複数/内容不一致=自動統合
  せず要確認・terminal 既存=reconciled 収束）。**原子的 txn なし前提の ACK-loss
  テストを実装票受け入れ条件へ追加**。凍結票の open 限定回収規律は不変と明記。
- **FB2-07（MED）**: 状態意味の分離を本文固定（problem_held=人の対応待ち／
  failed=回収待ち／reconciled=照合完了の終端）。「次のフック発火」文言を撤回
  （取り消し線＋理由=同一 generation の再発火は設計上存在しない。起票再試行は
  人の語彙経路のみ）。
- **R2 文言の限定**: 「新しい防壁を作らない」を plan 内容・M1 領域に限定——
  engagement_event 台帳は新起動経路固有の冪等制御で新設が正当（誤読防止）。

## 9. fix3 改定記録（R-FB2-D3・2026-08-12・全所見 ACCEPT・FB2-09 は推奨案A採用）

- **FB2-08（HIGH）**: 状態遷移表を reconcile 動作と同期（fix3 同期版へ差し替え・
  条件列追加）——pending→envelope_filed（reconcile・open 封筒 1 件内容一致）／
  pending→reconciled（reconcile・terminal 封筒 1 件内容一致）／pending→failed
  （起動経路又は reconcile・idem キー保存済み封筒 0 件確認）／failed→reconciled
  （reconcile・既存封筒 1 件内容一致）／problem_held→reconciled（reconcile・
  語彙起動による対応封筒確認）を追加。**failed からの回収を一意化**（自動再試行
  なし・封筒 0 件=failed 維持＋通知・新規作成は人の語彙経路のみ・次回 reconcile が
  収束）。**failed→envelope_filed の主体は reconcile（照合による状態収束）であり
  起動経路の再実行ではない**と明記。表⇔§3.2/§4 の相互参照を張り「表が唯一の正」を
  維持（§4 の「回収続行」旧文言・FB2-07 の「回収=再実行」旧文言は撤回・残置）。
- **FB2-09（HIGH・裁定=案A）**: 初版は**一案件一受任のみ・generation は常に
  定数 1**（grammar の桁は将来拡張用）。generation=1 既存案件での再成立検知=
  **自動処理せず write 0＋要確認通知**。generation>1（受任取消・再契約の世代状態
  機械）は将来の別票。fix2 の「再成立ごと +1」記述を撤回（取り消し線＋理由。
  注記: 票指定の撤回対象 §3.0(iii)(v) に +1 記述は存在せず、実所在は §3.2 の
  grammar 節〔fix2〕であったため同所で撤回した。§3.0(iii)(v) は案A と整合のため
  不変）。
- **FB2-10（MED）**: §4a「実装・点火前提（[人]ゲート一覧）」新設・4 段分離
  （App26 CU／event 台帳 migration／flag 投入／本番点火）。いずれも**凍結条件では
  なく実装・点火前提**と明記。

## 10. fix4 改定記録（R-FB2-D4・2026-08-12・FB2-11 のみ・09/10 は RESOLVED）

- **FB2-11（HIGH）**: 状態遷移表の failed 行 2 遷移を**排他化**——
  failed→envelope_filed=「open 封筒 1 件・内容一致」／failed→reconciled=
  「**terminal** 封筒 1 件・内容一致」（旧「既存封筒 1 件」は撤回・取り消し線残置
  ——open 含みでは 2 行が同時成立し表の決定性が崩れる）。pending 時の ACK-loss
  回収規則と対称化し「同一 from×同一観測で遷移先一意」を明記。ACK-loss テストへ
  **failed 分岐 4 件**（open1件→filed／terminal1件→reconciled／0件→維持+通知／
  複数・不一致→write 0+要確認）を実装票受け入れ条件として直接固定。
