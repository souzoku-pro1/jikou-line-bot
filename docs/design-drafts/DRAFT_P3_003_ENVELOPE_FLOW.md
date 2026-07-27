# DRAFT: P3-003 封筒フロー結線 — DerivationRun→App30 要確認封筒→[人]関所→HCD 追記＋App36 projection 更新

- TASK_ID: P2-BATCH-05 / TASK 13（READ_ONLY 調査＋DRAFT 起票・PC-A）／記録日 2026-07-27
- 調査 BASE: origin/main `d87b3d6`（読取のみ）＋ p3-001/p3-002 レビュー中 branch の読取参照
  （`origin/feature/p3-001-derivation-run`・変更なし）
- 正本: `DRAFT_APP36_DERIVATION_APP37_TEMPLATE_REGISTRY.md` §3.1-3.2 ＋
  `DRAFT_P3_CONTROL_PLANE_INVENTORY.md` §5 の P3-003 票案（**修正済みの向き**:
  DerivationRun → App30 要確認封筒 → [人]関所 → HCD 追記＋App36 projection 更新。
  旧記述「承認前 App36 を入力に App30 起票」の逆向きは撤回済み）
- **本書は設計 DRAFT であり実装しない**。E系 effect level（E0–E3）の組込みは
  **v2.4 正本の逐語確認後の別票**（§2.4 で再掲）。

## 1. 結線対象の現状調査（実物逐語）

### 1.1 App30 状態機械（hub/approval.py・稼働中）

絶対制約（hub/approval.py:5-9 逐語）:

> ██ 絶対制約 ██
> 「承認待ち → 承認済」への遷移を行うコードパスをサーバー側に作らない。
> 承認は弁護士の kintone 操作のみ。SERVER_TRANSITIONS に遷移先が「承認済」の
> 組が存在しないことを test_hub_approval.py が恒久的に担保する。

サーバー許可遷移（hub/approval.py:27-36 `SERVER_TRANSITIONS` 逐語）:

```python
SERVER_TRANSITIONS = frozenset({
    ("下書き", "承認待ち"),      # prepare 成功（成果物添付済み）
    ("下書き", "エラー"),        # prepare 失敗
    ("承認済", "発送処理中"),    # dispatch 開始（claim 通過後）
    ("発送処理中", "発送済"),    # dispatch 成功（自動送信チャネル）
    ("発送処理中", "エラー"),    # dispatch 失敗（リトライ超過含む）
    ("発送済", "返送待ち"),      # 返送想定あり
    ("発送済", "完了"),          # 返送想定なし
    ("返送待ち", "完了"),        # M5 返送消込・送達確認
})
```

人の遷移（hub/approval.py:39-45 `HUMAN_TRANSITIONS`）には
`("要確認", "下書き"), ("要確認", "完了")` が既に定義済み＝**「要確認」封筒を人が
閉じる遷移は状態機械上の既存語彙**であり、P3-003 は状態機械への追加を要しない。
ただし後述 §3.2 のとおり、**確定ハンドラのクローズは既存 RESOLVERS と同型の
「発送ステータス:完了＋実行済み:yes」直接書込み**（`_resolve_koseki` 型）を踏襲する。

### 1.2 冪等ガード（hub/approval.py:78-96 `claim_execution` 逐語・要点）

```python
async def claim_execution(app: kintone.KintoneApp, record: dict) -> bool:
    if record.get("実行済み", {}).get("value", "") != "no":
        return False
    record_id = str(record["$id"]["value"])
    revision = record.get("$revision", {}).get("value")
    try:
        await kintone.update_record(app, record_id, {"実行済み": "yes"}, revision=revision)
    except kintone.KintoneConflict:
        ...
        return False
    return True
```

＝「実行済み=no ＋ `$revision` 指定更新」の claim パターン。**True を返した呼び出し元
だけが処理を実行してよい**（二重 Webhook・並行プロセスの両方を遮断）。

### 1.3 App29 webhook（main.py:622-751 `/webhook/kintone/approval`・稼働中）

- 冪等の三層: (i) flag `KINTONE_EVENT_DEDUP_ENABLED` ON 時は webhook top-level `id` で
  `claim_event`（inbound_event 冪等記録・duplicate は即 return・main.py:653-677）
  (ii) webhook body の高速チェック（`ステータス2=承認済` かつ `送信済み=no`・:686-698）
  (iii) **最新レコード再読**後の同条件再確認（:700-723・「先生の修正を反映するため」）。
- 送信着手 marker（received→sending・:734-740）成功が LINE 送信の前提（fail-closed）。
- P3-003 への含意: **App29 webhook 自体には触れない**。P3-003 の関所は App30 封筒
  （トップキー方式・review_resolve）であり、App29（承認キュー=LINE 応答承認）とは別経路。
  ただし冪等の作法（claim → 再読 → 実処理 → terminal 書込み）は同型を踏襲する。

### 1.4 封筒起票の既存雛形（2 系統・実物）

- `person_merge._file_candidate`（person_merge.py:273-299）: App30 に
  `発送ステータス:"要確認"`・`チャネル固有データ:{"person_merge": detail}`・
  `実行済み:"no"` で起票。**P3-003 の直接の雛形**（要確認封筒＋トップキー）。
- `dispatch_bot/app30_filer.file_from_pending`（app30_filer.py:96-132）:
  起票前の `find_existing`（チャネル固有データ like 検索）による二重起票ガード第2層＋
  **単票API必須**の実機根拠（app30_filer.py:124-127 逐語）:

  > ★単票API（POST /k/v1/record.json）で起票すること。
  > 一括API（records.json・create_records）は kintone 仕様で「レコード追加」Webhook が
  > 発射されず、/hub/dispatch → prepare が走らない（2026-07-04 実機不具合の原因）

### 1.5 関所の既存枠組み（review_resolve.py）

- `RESOLVERS`（review_resolve.py:394-400）: チャネル固有データの**トップキー→
  (確定ハンドラ, 必要 env) の登録辞書**。現在 4 キー
  （registry_ingest / koseki_ingest / valuation_ingest / bank_ingest）。
- `resolve_group`（:403-）: 上位=T2（指示Bot）から呼ばれる入口。未知キーは
  unsupported の明示応答・env 不足は unavailable・二重確定ガード発動は aborted。
- `_resolve_koseki`（:222-）: phase 1=書き込み直前の**全件再読**（1件でも変化なら
  全体中止）→ phase 2=対象アプリ更新→App30 クローズ。**P3-003 ハンドラの型**。

### 1.6 P3-001 の HCD 契約（レビュー中 branch `hub/derivation_models.py` 読取）

- `DerivationRun`: 純粋 immutable（ORM listener＋DB trigger の二重で UPDATE/DELETE 拒否）。
  human_state/decided_by/decided_at は**持たない**。
- `HeirConfirmationDecision`: 追記のみ。`decision IN ('confirmed','held','rejected')`・
  `decided_by`/`decided_at` 必須・`amendments`（正本 §2.2「修正内容」）・
  `supersedes_decision_id` UNIQUE＋自己参照 CHECK 拒否＋
  **single-root 部分 unique（`uq_heir_decision_single_root`: 同一 run の root decision は
  1 行のみ）**。
- 例外語彙: `ImmutableRecordError` / `PayloadPolicyError` / `ChainIntegrityError`。
- **未実装（P3-003 の結線先）**: 封筒起票・RESOLVERS ハンドラ・App36 projection・
  `KintoneApp("App 36 (相続人)", "APP_SOUZOKUNIN", "TOKEN_SOUZOKUNIN")` 定義・
  env flag `HEIR_DERIVATION_ENABLED`（正本 §3.7）。

## 2. 封筒起票の設計（機械・正本 §3.1）

### 2.1 起票関数（新規モジュール案: `hub/heir_envelope.py`）

- 入力: 確定待ちの `DerivationRun`（status=derived または held）。
- App30 への起票 fields（`person_merge._file_candidate` 同型）:
  - `発送ステータス: "要確認"`・`実行済み: "no"`・`ユニット種別`（案件から）
  - `件名: "相続人導出の確認: 案件 No.{case_record_id}（run #{run_id}）"`
    （**氏名・PII は件名に入れない** — result_payload が person_id のみ（§3.5）で
    あることと整合。人が見る詳細は kintone 画面の App36/34 参照で解決）
  - `チャネル固有データ: {"heir_derivation": detail}`（トップキー方式）。
    detail = `{"derivation_run_id": <id>, "case_record_id": ..., "input_hash": ...,
    "result_hash": ..., "provisional": ..., "lawyer_flags": ...}`
    ＝ **DerivationRun.id を格納**（正本 §3.1）。payload 本体は封筒へ複製しない
    （正本=DB 行・封筒は参照のみ。改ざん面と PII 面の両方を最小化）。
  - **単票 API（create_record）必須**（§1.4 の実機根拠）。
- 起票の起動: P3 初版は**指示Bot語彙「相続人を導出して」の導出完了直後に同期起票**
  （§3.7 裁定: 自動起動しない）。導出（run INSERT）と起票（App30 create）は別システム
  なので原子性はない → 冪等キー（§2.2）で再実行安全にする。

### 2.2 冪等キー

- 正本 §3.1 どおり **`heir_derivation:{case_record_id}:{input_hash}`**。
  detail に平文で持ち、起票前に `app30_filer.find_existing` 同型の
  `チャネル固有データ like "<冪等キー>"` 検索で二重封筒を遮断（第2層）。
- 同一入力の再導出（input_hash 同一）→ 既存封筒を再利用（新規起票しない）。
  入力が変わった再導出（input_hash 相違・supersedes 連鎖の新 run）→ 新封筒。
  旧封筒が未確定のまま残る場合の扱いは §3.3（stale 封筒ガード）。
- 検索ベースの遮断は完全な原子性を持たない（検索→起票の間隙）。許容根拠:
  二重封筒は**要確認どまり**で対外効果ゼロ・人の確定側（§3.2）に二重確定ガードが
  あるため実害は「封筒が 2 枚見える」に限定。App29 webhook のような DB claim の
  導入は初版では過剰（**裁定不要の既定として二重防御はこの 2 層**とする）。

### 2.3 DerivationRun との参照方向

- 封筒 → run: detail.derivation_run_id（§2.1）。
- run → 封筒: **持たない**（DerivationRun は immutable のため封筒 ID の後書きが不可能。
  逆参照が要る場面は detail からの検索で足りる）。

### 2.4 E系 effect level の扱い（明示）

- 封筒起票は「人の確認を要求する」だけで対外効果を生まない（E0 相当の想定）が、
  **E0–E3 の定義正本は v2.4（repo 外）であり逐語未確認**。
  → **effect level のフィールド化・レベル別分岐は v2.4 正本確認後の別票**
  （DRAFT_P3_CONTROL_PLANE_INVENTORY §4 の④）。本票では封筒に E 値を書かない。

## 3. [人]関所後の設計（正本 §3.2 — HCD 追記＋App36 projection 更新）

### 3.1 RESOLVERS 登録

- `review_resolve.RESOLVERS` にトップキー `"heir_derivation"` を追加:
  `("heir_derivation": (_resolve_heir_derivation, (APP_SHIPPING, APP_SOUZOKUNIN)))`。
  env `APP_SOUZOKUNIN`/`TOKEN_SOUZOKUNIN` は新規（§4.1）。
- 入口は既存 `resolve_group` のまま（指示Bot=T2 経由・未知キー明示応答の規律を継承）。

### 3.2 確定ハンドラ `_resolve_heir_derivation`（`_resolve_koseki` 型・3 phase）

- **phase 1（二重確定ガード）**: 封筒の全件再読（`実行済み=no`・`発送ステータス=要確認`
  を再確認・1件でも変化なら全体中止=aborted）。加えて **DB 側ガード**:
  対象 run に既に root decision が存在すれば中止（`uq_heir_decision_single_root` が
  DB レベルでも二重 INSERT を拒否する — P3-001 契約 §1.6 との整合点）。
- **phase 2（HCD 追記）**: `HeirConfirmationDecision` を **1 行 INSERT**
  （decision=confirmed/held/rejected・decided_by・decided_at・amendments=人の修正内容）。
  **DerivationRun は書き換えない**（immutable・書けば ImmutableRecordError）。
  human_state は「run＋最新 decision の join projection」で読む（正本 §2.2）。
  decided_by は **`ATTORNEY_ALLOWLIST`（env）検証**（正本 §3.4 H11 防御側）:
  allowlist 外の decided_by による confirmed（＝戸籍確認済 yes 遷移を伴う）は拒否。
- **phase 3（App36 projection 更新＋クローズ）**: decision=confirmed のときのみ
  App36 upsert（§3.3）→ App30 封筒を `発送ステータス:完了＋実行済み:yes` でクローズ
  （`_resolve_koseki` のクローズ値と同一）。held/rejected は App36 に触れず封筒のみ
  クローズ（held は件名に保留理由を残すか等の細部は実装票で）。

### 3.3 App36 projection 更新（正本 §3.3 の契約に従う）

- 冪等キー = case_record_id＋person_id（既存型: registry_ingest._upsert_zaisan）。
- **条件付き更新規則（H10）**: 対象 App36 の `current_derivation_run_id` が新 run の
  supersedes 連鎖上の**祖先である場合のみ**更新し、成功時に新 run へ進める
  （古い run・無関係 run による上書きを構造的に遮断）。
- **human_state 保護**: 機械由来フィールド（続柄/法定相続分/データ源）のみ差分更新。
  `戸籍確認済=yes`・`状態` の手修正は上書きしない（正本 §3.3/§3.4）。
- **stale 封筒ガード**: 確定時点で対象 run が supersedes 連鎖の head でない
  （＝より新しい run が存在する）場合は projection 更新せず aborted
  （「新しい導出があります」の明示応答）。冪等キーが input_hash 単位のため、
  旧封筒への遅れた確定が新結果を巻き戻す事故をここで遮断する。
- 検知側（H11）: 「decision なしの 戸籍確認済=yes」を daily_healthcheck で監査
  （正本 §3.4）— **これは P3-003 スコープ外の別票**（§5 の分割参照）。

## 4. kintone 実結線の要否分解（モック境界）

| 結線点 | 実結線の要否 | モック境界 |
|---|---|---|
| App30 封筒起票（create_record） | 既存 APP_SHIPPING/TOKEN_SHIPPING で可（新規 env 不要） | `hub.kintone.create_record` を mock（既存テスト流儀） |
| App30 再読・クローズ（get/update） | 同上 | 同上 |
| **App36 upsert** | **新規 env `APP_SOUZOKUNIN`/`TOKEN_SOUZOKUNIN` が必要**＝kintone 側の App 36 実機作成・`current_derivation_run_id` フィールド追加（正本 §3.3 H10・BLOCKED CU）・API token 発行が前提 | `hub.kintone` mock で設計上のテストは完結可（実機なしで実装票は進められる） |
| DB（run 読取・HCD INSERT） | Railway PostgreSQL（P3-001 の migration 済みが前提） | テストは SQLite（P3-001 テスト流儀）・PG 実機実測は TRACKING_PRE_DEPLOY_CHECKS #2 と同回 |
| ATTORNEY_ALLOWLIST | 新規 env（値=弁護士識別の集合） | テストは env 直投入 |

- **[人]確認事項**: (i) App 36 の実機作成・フィールド追加・token 発行のタイミング
  (ii) ATTORNEY_ALLOWLIST の値（弁護士識別の表記） (iii) 封筒件名の文言。
  実結線（env 投入・実機疎通）は**すべて[人]ゲート**であり、実装票はモック境界まで。

## 5. 実装票スコープ案（P3-003a/b 分割・依存=p3-001/002 merge）

| 票案 | スコープ | 依存 |
|---|---|---|
| **P3-003a: 封筒起票（機械側）** | `hub/heir_envelope.py` 新規（§2: 要確認封筒・冪等キー・単票API・find_existing 型ガード）＋指示Bot導出完了への結線＋flag `HEIR_DERIVATION_ENABLED`（既定OFF）＋テスト（起票 fields・冪等・PII 非混入） | **p3-001 merge**（DerivationRun 実体）。p3-002 とは独立 |
| **P3-003b: 関所＋projection（人側）** | RESOLVERS `"heir_derivation"` 追加＋`_resolve_heir_derivation`（§3.2 の 3 phase・ATTORNEY_ALLOWLIST 防御）＋App36 upsert（§3.3 H10/human_state 保護/stale ガード）＋`KintoneApp` App36 定義＋テスト（二重確定・immutable 遵守・保護規則） | p3-001 merge＋**P3-003a**＋App36 実機作成（[人]・モックまでなら不要） |
| （別票・後続） | H11 検知側（daily_healthcheck 監査）／E0–E3 組込み（v2.4 正本確認後）／放棄写像（正本 §3.6 OPEN・弁護士承認待ち） | それぞれ独立に裁定 |

- 分割の根拠: a は DB→kintone の一方向・対外効果ゼロで小さく先行でき、
  b は kintone 実機（App36）と ATTORNEY_ALLOWLIST の[人]確認を含むため、
  レビュー・実機確認の重心が異なる。
- p3-002（TemplateVersion）は P3-003 の直接依存ではない（成果物生成=Phase 5 接続点）が、
  同一 metadata 群の migration 順序の都合上 **merge 順は p3-001 → p3-002 → P3-003a** を既定とする。

## 6. 欠落補記（2026-07-27・P3-003a 実装時。**凍結逸脱ではなく前提欠落の補記**）

- **前提欠落の発見**: §2.1/§5 の「指示Bot導出完了への結線」は、**導出コマンド
  （語彙ハンドラ「相続人を導出して」→ App34 読取 → derive_heirs → run 保存）の実行
  経路が repo に未実装**であり結線先が存在しない（`derive_heirs`／
  `create_derivation_run` の runtime 呼出しはゼロ・正本 §3.7 item 5 の起動経路は
  未着手）。導出コマンド自体の設計も本 DRAFT §2 に未収録だった。
- **裁定（2026-07-27・[人]・条件4点）**: P3-003a は**封筒側のみ実装** —
  (a) `hub/heir_envelope.py` の `file_heir_envelope(run)`＝**結線点の公開関数**として
  契約を明文化（入力=DerivationRun 参照のみ・冪等キー生成規則・戻り値4状態・
  失敗時挙動=kintone 例外は握らず送出/部分状態なし/冪等キーで再実行安全）。
  テストは公開関数を直接呼ぶ形で完結（実行経路不在でも検証可能）。
  (b) **導出コマンド経路（語彙ハンドラ→App34読取→derive→run保存）は別票＝要設計**。
  (c) **導出コマンド設計票は司令塔が別途起票**する（**P3-003a の完了条件に含めない**）。
  (d) R-P3-001-7 M01 は同梱 commit 済み。
- 実装上の確定事項（DRAFT が未固定だった点の解決）: 封筒の チャネル/方向 は既存語彙
  「スキャン受領/受領」を踏襲（person_merge 同型）。専用チャネル値の新設は kintone
  フィールド変更（[人]・BLOCKED）を要するため初版では行わない。
- **fix1 追記（R-P3-003A-1 対応）**:
  - **ユニット種別（H03）**: 凍結 §2.1 どおり**案件由来**＝案件アプリ ID→ユニットの
    写像で解決（App21=時効援用／App26=相続一般）。**案件から解決不能な場合は
    起票せず異常扱い**（EnvelopePolicyError・kintone write ゼロ）。
  - **導出コマンド票への申し送り（M02→fix2 H02 訂正・固定事項）**:
    `file_heir_envelope` の失敗時契約＝**search/create/policy いずれの失敗も例外伝播
    （握り潰し禁止・新規起票を成功扱いにしない）**。
    **create の通信失敗は「結果不明（ACK 不明）」**——POST が kintone 側で成功し
    応答のみ喪失した可能性があるため「封筒未作成」とは断定しない。
    **再実行時は冪等キーの完全一致検索（H01）が reconcile を担い、成功済み封筒が
    見つかれば already_filed として回収（二重起票しない）**。リトライ判断は
    導出コマンド側の責務（契約 pin テスト=TestFailureBehaviorContract・
    ACK 喪失回収=test_ack_lost_create_reconciled_on_retry）。
