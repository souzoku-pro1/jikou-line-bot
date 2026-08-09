# P3-003c held/rejected 語彙の設計（判断保留・否認の decision 経路・**設計凍結 FROZEN**）

- **STATUS: 設計凍結（FROZEN・R-P3-003C-D3 PASS・2026-08-09）**。
  レビュー履歴: R-P3-003C-D1=CHANGES_REQUIRED（H01/H02/M01/M02/M03・§10）→ fix1 →
  R-P3-003C-D2=CHANGES_REQUIRED（前巡5所見全RESOLVED＋新規H01-R2/M01/L01・§11）→
  fix2 → **R-P3-003C-D3=PASS（凍結判定）**。以後の変更は改定記録の追記のみ
  （遡及書き換え禁止）。実装は本凍結仕様に基づく実装票（P3-003c-impl）で行う。
- TASK_ID: P3-003C-D 設計票（コード/テスト実装は実装票まで禁止）／記録日 2026-08-09
  （fix1: R-P3-003C-D1 の H01/H02/M01/M02/M03 全所見反映・同日。改定は両時点残置——
  初版の root 判定案は**撤回**し §3.3-v2 の leaf 判定へ・§10 改定記録／
  fix2: R-P3-003C-D2 の H01-R2〔side effect の resumable 化=§4.1〕・M01〔並行競合の
  例外正規化=§3.3-v2〕・L01〔§10 行数証跡の訂正〕反映・同日・§11 改定記録）
- 目的: 凍結 `DRAFT_P3_003_ENVELOPE_FLOW.md`（以下「正本」）§3.2 の残件
  「held/rejected は App36 に触れず封筒のみクローズ（held は件名に保留理由を残すか等の
  細部は実装票で）」を設計凍結する。P3-003b（confirmed 一本・merge 済み #187）の
  実装現実を土台に、held（判断保留）／rejected（否認）の decision 経路を固定する。
- 正本参照（矛盾を作らない・編集しない）: 正本 §3.2〜3.4／`DRAFT_P3_003B_DESIGN`
  （§4B fix3 H01=機械は App36 write 0・§9-v2=resumable projection・封筒クローズ=held 行 0 のみ）／
  `DRAFT_P3_003_CMD` §8（裁定記録の形式先例）。
- 実装現実の実査基盤（2026-08-09・read-only・初版時 main `861458a`。
  **fix1 BASE 訂正（H02）**: 現 origin/main は `4afc4be`（#190 retirement 票 merge 済み）。
  本 branch の分岐点（merge-base）は `4afc4be` と一致＝**rebase 不要**・main に対する
  純差分は本 DRAFT 1 ファイルのみ（機械確認の実出力は §10）。実査引用の対象コードは
  `861458a`→`4afc4be` 間で不変〔差分は #190＝docs 2 ファイルのみ〕）:
  `hub/derivation_models.py`（HCD 契約・`create_confirmed_decisions_for_heads`）／
  `hub/heir_projection.py`（confirmed handler 3 phase）／`review_resolve.py`
  （RESOLVERS・resolve_group の能力ベース引数）／`dispatch_bot/review_resolve_task.py`
  （T2 語彙・復唱→pending 30分単回）／`hub/approval.py`（App30 状態機械）。
- 次レビュー: R-P3-003C-D3（fix2 反映後・H01-R2/M01/L01 の RESOLVED 判定中心・
  **凍結判定**。経緯: R-P3-003C-D1=CHANGES_REQUIRED〔§10〕→ fix1 → R-P3-003C-D2=
  前巡 5 所見全 RESOLVED＋新規 H01-R2/M01/L01〔§11〕→ fix2）。

## 0. 用語の一意化（「held」3 概念の分離・本書全体で厳守）

実査で「held」が 3 層に存在することを確認した。本書では以下に呼び分ける（混同すると
設計を誤る——特に (2) と (3) は主体も粒度も別物）:

| 呼称（本書） | 実体 | 主体・粒度 | 実装現況 |
|---|---|---|---|
| **run-held** | `DerivationRun.status='held'`（derivation_models.py:53 `status IN ('derived','held','error')`） | 機械・run 単位（導出時の保留保存・CMD 裁定2: kosekis None＋rank=3 は常に held 保存） | 実装済み。confirmed handler は derived/held 両方を確定対象に受理（heir_projection.py:233） |
| **row-held** | projection 行の保留（`_project_row` 戻り値 "held"・保留人物ID detail 追記） | 機械・App36 行単位（直前再検証の競合等） | 実装済み（§9-v2・封筒クローズは row-held=0 のみ） |
| **decision-held / decision-rejected** | `HeirConfirmationDecision.decision='held'/'rejected'`（derivation_models.py:97 CheckConstraint） | **[人]（弁護士）・封筒（run）単位の判断** | **DB 契約のみ実装済み・経路未実装＝本票の対象** |

## 1. 実装現実の実査（実物逐語・P3-003C の接続点）

### 1.1 HCD 契約は held/rejected を最初から受容している（derivation_models.py:92-121）

```python
sa.CheckConstraint("decision IN ('confirmed', 'held', 'rejected')",
                   name="ck_heir_decision_decision"),
sa.UniqueConstraint("supersedes_decision_id", name="uq_heir_decision_supersedes"),
sa.Index("uq_heir_decision_single_root", "derivation_run_id", unique=True,
         ... postgresql_where=sa.text("supersedes_decision_id IS NULL")),
```

＝ decision 3 値・**同一 run 内の decision 一本鎖**（root は 1 行・supersedes は UNIQUE・
自己参照拒否・cross-run supersede 拒否 :548）・append-only（UPDATE/DELETE は
ImmutableRecordError）。**held/rejected の記録形は新規テーブル・新規列を要しない**。
訂正の正規形は「新 decision 行＋supersedes_decision_id」（§2.2 正本）。

### 1.2 【要改修点・実査の主発見】再開経路が decision 値を見ていない（derivation_models.py:804-810）

```python
root = (await s.execute(
    sa.select(t.c.id).where(
        t.c.derivation_run_id == run_id,
        t.c.supersedes_decision_id.is_(None)).limit(1))).first()
if root is not None:
    out[run_id] = "resumed"   # 再開経路（INSERT せず projection のみ）
```

現行の resumed 判定は「root decision の**存在**」のみで、**decision 値を SELECT して
いない**。confirmed 一本の現況では root=confirmed しかあり得ないため正しいが、
**decision-held/rejected を導入した瞬間、「held/rejected の root がある run」への確定
指示が resumed 扱いになり、否認済み run が App36 へ projection される**。
→ P3-003c は本ヘルパの判定改修を**必須の前提**とする。
- （履歴・初版）改修案を「root 判定に decision 値を読む」としていたが、
  **fix1 H01 で撤回**——root の値だけでは held→confirmed 等の supersede 後の有効判断を
  表せない（root=held のまま leaf=confirmed になり得る）。**有効 leaf 判定**（§3.3-v2）
  へ改定した。confirmed 一本のままなら挙動同一＝後方互換（テスト計画 §7-1）は不変。

### 1.3 T2 の関所型二重確認は汎用機構（review_resolve_task.py:80-100）

`_confirm`: 復唱文言を組み → `confirm.create(user_id, parsed, hit, base_text)`
（**pending 発行・30分単回・OK で execute**）→ execute が
`resolve_group(group, case_id, decided_by=pending.user_id)`（review_resolve_task.py:224-227）。
pending invalidate は execute 内 finally（CMD 裁定8・全終端で無効化）。
＝**held/rejected も同じ関所（復唱→OK→単回実行→decided_by 伝搬）に乗せられる**。
新しい確認機構は不要。

### 1.4 resolve_group の能力ベース引数（review_resolve.py:435-437）

```python
import inspect
if "decided_by" in inspect.signature(handler).parameters:
    return await handler(group, case_record_id, decided_by=decided_by)
```

＝「ハンドラ固有分岐は作らない・signature が受ける引数だけ渡す」既存規律。
**decision 種別の伝搬も同型で拡張できる**（`decision` を signature に持つハンドラ
＝heir_derivation のみ＝へだけ渡す。既存 4 ハンドラ無変更・§2.3）。

### 1.5 封筒クローズと App30 状態機械（heir_projection.py:380-394／approval.py:39-45）

- 現行クローズ: row-held=0 のときのみ `発送ステータス:完了＋実行済み:yes`
  （`_resolve_koseki` 型の直接書込み）。row-held>0 は封筒を要確認のまま残し
  detail へ `保留人物ID` を事後追記（起票時閉集合への注記拡張・正本 §7 改定記録）。
- `HUMAN_TRANSITIONS` に `("要確認", "下書き"), ("要確認", "完了")` あり＝人が封筒を
  閉じる遷移は既存語彙。**「完了→要確認」の再オープン遷移は人・サーバどちらにも
  存在しない**——一度閉じた封筒は戻せない（§5 の設計制約）。

## 2. (a) LINE 語彙設計（関所型・復唱・二重確認）

### 2.1 発話語彙（T2・review_resolve タスクの拡張）

| 発話（例） | decision | 既存フローとの差分 |
|---|---|---|
| 「要確認を確定して」（現行） | confirmed | 変更なし |
| 「要確認を**保留**して」 | held | 新規・同一タスク内の decision 分岐 |
| 「要確認を**否認**して」「差し戻して」 | rejected | 新規・同上 |

- 案件指定・グループ選択・顧客突合・番号選択は**現行フローを共用**（分岐は decision
  種別のみ）。語彙の同義語集合（「保留」「ペンディング」／「否認」「差し戻し」「却下」等）
  は実装票で閉集合として固定し、**曖昧発話は confirmed に倒さない**（決め打ち禁止・
  聞き返し）。
- **対象外 source の明示応答**: decision 語彙（保留/否認）が heir_derivation 以外の
  グループ（koseki_ingest 等）へ向いた場合は unsupported の明示応答
  （「このグループは確定のみ対応です」型・黙って確定に倒さない）。

### 2.2 復唱文言（二重確認・誤爆遮断）

- 復唱は decision 種別を**明示的に含める**（confirmed と押し間違えで不可逆な記録が
  残るため）。例（文言は[人]確認・§8-6）:
  - held: 「要確認N件を **保留** として記録します（App36 への反映はありません・
    封筒は要確認のまま残ります）。OK / キャンセル（30分有効）」
  - rejected: 「要確認N件を **否認** として記録します（App36 への反映はありません・
    この封筒はクローズされ、再確定には再導出が必要です）。OK / キャンセル（30分有効）」
- **不可逆性の非対称の反映**: rejected の復唱には帰結（封筒クローズ・再導出要）を
  必ず含める。held は可逆（後から確定可）なので簡潔でよい。
- 30分単回 pending・OK 一回性・finally invalidate は現行機構をそのまま共用（§1.3）。

## 3. (b) decision 記録の形（confirmed との対称性・supersede 関係）

### 3.1 記録形（対称・既存契約のまま）

- held/rejected とも `HeirConfirmationDecision` へ **1 行 INSERT**（decision 値のみ相違・
  decided_by/decided_at 必須・amendments は初版 NULL＝§8-5）。DerivationRun は不改変。
- **一括 CAS txn も対称**: グループ全 item を単一 txn・head CAS・途中失敗全体 rollback・
  同一 run 重複排除（fix2 H01-R2 と同一規律）。

### 3.2 supersede 関係（初版表・**fix1 M02 で §3.2-v2 の全遷移表へ置換・履歴残置**）

（履歴・初版の 6 行表は「先行 root」を軸にしていたが、fix1 H01 の leaf 判定への改定に
伴い軸を「**有効 leaf**」へ変更し、9 組合せの閉じた表 §3.2-v2 が正となる。初版表は
git 履歴で参照可能・本文からは v2 表へ一本化する）

### 3.2-v2 全遷移表（fix1 M02・**有効 leaf × 指示 decision の 3×3＋先行なし＝閉じた 10 行**）

前提: 判定軸は「decision 鎖の**有効 leaf**」（supersede されていない末端・§3.3-v2。
一本鎖ゆえ高々 1 行）。表のアクションはすべて §3.3-v2 の単一トランザクション内で確定する。

| 有効 leaf | 指示 | decision 追記 | 帰結 |
|---|---|---|---|
| （decision なし） | 確定 | **root confirmed INSERT** | projection → 封筒クローズ判定（row-held=0 でクローズ） |
| （decision なし） | 保留 | **root held INSERT** | 封筒 open 維持＋判断注記=held（§4） |
| （decision なし） | 否認 | **root rejected INSERT** | 封筒クローズ＋判断注記=rejected（§4） |
| leaf=confirmed | 確定 | **なし（"resumed"）** | **封筒 open かつ run=head のときのみ** projection 再開（§9-v2 の正規再開経路）。封筒クローズ済みなら封筒再読で aborted・head でなければ stale aborted |
| leaf=confirmed | 保留 | **なし・aborted** | 「確定済みです（取消は別途）」——projection 済み結果の取消は App36 巻き戻しを伴い**別票**（裁定④=(A) 採用・§8） |
| leaf=confirmed | 否認 | **なし・aborted** | 同上（裁定④） |
| leaf=held | 確定 | **confirmed が leaf を supersede**（supersedes_decision_id=leaf.id） | projection 実行 → 封筒クローズ判定・判断注記を confirmed へ**更新**（M03・§4） |
| leaf=held | 保留 | **なし（no-op）** | 固定応答「既に保留です」＝同値 decision の連鎖を作らない。**ただし封筒 open で判断注記が未適用（前回の後続 write 失敗）なら注記=held を冪等再適用**（§4.1 fix2 H01-R2） |
| leaf=held | 否認 | **rejected が leaf を supersede** | 封筒クローズ＋判断注記=rejected（§4） |
| leaf=rejected | 確定／保留／否認 | **なし・aborted（3 指示とも）** | 確定/保留=「否認済みです。再導出してください」（不可逆・§5）。**否認（再）=decision 追加なし**——封筒クローズ済みなら既否認の固定応答「既に否認済みです」・**封筒 open（前回の後続 write 失敗）なら注記=rejected とクローズを冪等再適用**（§4.1 fix2 H01-R2） |

- 表は 10 行で閉じる（leaf は高々 1 行・値は 3 値＋不在の 4 状態 × 指示 3 値 ＝ 12 組の
  うち leaf=rejected の 3 指示を 1 行に束ねた表記）。**表にない遷移は存在しない**
  （未知状態は ChainIntegrityError 型の中止・値非表示）。

### 3.3 ヘルパの一般化（初版・**fix1 H01 で撤回・履歴残置**）

（履歴・初版は「root 判定で decision 値を読む」改修案だったが、root=held が
confirmed に supersede された後も root 行は held のまま残るため、**root の値では
有効判断を表せない**。fix1 H01 で撤回し §3.3-v2 の leaf 判定へ改定）

### 3.3-v2 有効 leaf 判定（fix1 H01・実装票要件）

- **判定対象**: decision 鎖の**有効 leaf** ＝ 当該 run の decision 行のうち
  「他行の `supersedes_decision_id` に参照されていない末端行」。一本鎖
  （single-root＋supersedes UNIQUE・実査 §1.1）ゆえ**高々 1 行**。
  ```sql
  -- 概念 SQL（実装票で確定・read）: 有効 leaf
  SELECT d.* FROM heir_confirmation_decision d
  WHERE d.derivation_run_id = :run_id
    AND NOT EXISTS (SELECT 1 FROM heir_confirmation_decision s
                    WHERE s.supersedes_decision_id = d.id)
  ```
- **単一トランザクション内で判定→アクション**（`create_confirmed_decisions_for_heads`
  の一括 CAS txn を decision パラメタ化して拡張・グループ原子性/head CAS/途中失敗
  全体 rollback/同一 run 重複排除の既存規律は不変）:
  - leaf なし → 指示 decision の **root INSERT**
  - leaf=confirmed → 指示=確定なら "resumed"（INSERT なし）・保留/否認なら中止（§3.2-v2）
  - leaf=held → 指示=確定/否認なら **supersede INSERT**（supersedes=leaf.id）・
    保留なら no-op（INSERT なし）
  - leaf=rejected → 中止（否認再指示のみ固定応答・いずれも INSERT なし）
- **uq_heir_decision_supersedes 競合が「二度目の supersede」で発生しない構造**:
  - **逐次実行では発生しない**——supersede INSERT は同一 txn 内の leaf 判定直後にのみ
    行われ、supersede された行はその瞬間から leaf でなくなる。次の指示は新 leaf
    （直前の supersede 行）を見るため、**既に supersede 済みの行を再度 supersede
    しようとする経路が正常フローに存在しない**。
  - **並行実行では DB が後詰め**——2 つの txn が同じ leaf を読み双方 INSERT した場合、
    後着が `uq_heir_decision_supersedes`（UNIQUE）で IntegrityError→**グループ全体
    rollback**（write 0）。再指示時は新 leaf に基づき §3.2-v2 表どおりの応答になる
    （例: 先着が confirmed 化済みなら再指示は resumed／中止）。＝ UNIQUE 制約は
    正常経路の分岐条件ではなく**並行 race の後詰め**としてのみ働く。
- **並行競合の例外正規化（fix2 M01）**: 上記 race の後着が受ける DB 例外
  （`uq_heir_decision_supersedes` / `uq_heir_decision_single_root` 由来の
  IntegrityError）は、txn 境界で **`ChainIntegrityError` へ正規化**する
  （既存の CAS 中止と同じ固定例外へ合流。同型の新固定例外を設ける場合も
  「固定文言・値非搭載」の契約は同一）。呼出し側はこれを受けて**グループ全体
  aborted＋固定応答**（「確定中に前提が変化しました…」型・既存 ChainIntegrityError
  ハンドリングと同一経路）へ落とす。**vendor/DB 例外本文は応答・ログ・警報へ
  露出しない**（P3-001 非露出契約と同型・例外連鎖の扱いは CMD 裁定9 の構造を踏襲）。
- **leaf 検索の fail-closed（fix2・§7-19 と対）**: leaf 検索が**複数行**を返した場合は
  一本鎖の DB 破損（あってはならない状態）として **ChainIntegrityError 型の中止**
  （write 0・固定文言・警報）。0 件=decision なし・1 件=正常の 3 分類で閉じる。
- 既存呼出し（confirmed）は挙動同一を pin（§7-1）。関数名の変更可否は実装票判断
  （公開契約ではないが test が参照）。

## 4. (c) 封筒の状態遷移（decision 別・凍結案）

| decision | 封筒（App30） | 根拠 |
|---|---|---|
| confirmed | 現行どおり（row-held=0 でクローズ・>0 は要確認維持＋保留人物ID） | §9-v2 実装済み |
| **held** | **クローズしない＝要確認のまま維持**（§8-2 裁定対象） | (i) held の耐久可視性は App30 キューが担う——row-held の §9-v2 と同じ原理 (ii) クローズすると「後から確定」の T2 経路（要確認一覧から選ぶ）が消え、**再オープン遷移が存在しない**（実査 §1.5）ため楔になる |
| **rejected** | **クローズ（`発送ステータス:完了＋実行済み:yes` の既存直接書込み型）**（§8-3 裁定対象） | 否認済み封筒を要確認キューに残すとキューが汚れ、誤確定の的になる。再確定の正規経路は再導出→新封筒（§5） |

- **detail 事後注記**: held/rejected とも封筒 detail へ判断注記キー
  （案: `"判断": {"decision": "held"|"rejected", "decided_at": <ISO8601>}`・decided_by は
  **書かない**=LINE user ID を kintone detail に置かない・PII 最小化）を追記する。
  起票時閉集合（_DETAIL_KEYS）は不変・`保留人物ID` と同じ「事後注記拡張」の型
  （正本 §7 改定記録の先例に追記する形で改定）。冪等照合（トップキー＋冪等キー完全
  一致）への非干渉も同先例と同じ。
- **判断注記の更新規則（fix1 M03・司令塔裁定）**: held→confirmed の supersede 時は
  判断注記キーを **confirmed へ更新する（除去しない）**——注記の終端値は常に有効 leaf
  と一致し、封筒 detail 単体で「最後の判断」が追える（`"判断": {"decision":
  "confirmed", ...}` へ上書き。履歴は HCD 鎖が正・detail は最新値のみ）。
  held→rejected も同様に rejected へ更新。
- **キーの分離維持（fix1 M03）**: decision 判断注記（キー `判断`・封筒単位・[人]の
  判断）と row-held の `保留人物ID`（App36 行単位・機械の保留）は**別キーのまま維持**
  し統合しない——§0 の粒度・主体の相違をキー空間でも保つ（held→confirmed の
  projection で row-held が発生した場合、`判断`=confirmed と `保留人物ID`=[...] が
  **併存**するのが正しい状態表現）。
- held 封筒が要確認一覧に残る間の表示: 一覧の件名は起票時のまま（変更しない・
  kintone 画面では detail 注記で判別可）。**T2 応答側で「保留中」を付記する**かは
  実装票の表示判断（挙動に影響しない）。

### 4.1 App30 後続 write の再開規則（fix2 H01-R2・side effect の resumable 化）

decision の DB commit（§3.3-v2 の単一 txn）と App30 の後続 write（判断注記の追記・
held=open 維持／rejected=完了+yes クローズ）は**別システムで原子性がない**。
decision commit 成功後に App30 write が失敗すると「decision は記録済み・封筒は
未注記/未クローズ」の中間状態が残る。これを**同一指示の再発行で冪等回収する**:

- **(a) leaf=held への保留再指示**: decision 追加なしで、**判断注記=held を冪等
  再適用**する（注記が既に held なら実質 no-op・固定応答「既に保留です」のみ）。
- **(b) leaf=rejected への否認再指示**: decision 追加なしで、**判断注記=rejected と
  `発送ステータス:完了＋実行済み:yes` クローズを冪等再適用**する
  （両方適用済みなら固定応答「既に否認済みです」のみ）。
- **(c) 再開条件**: side effect 再開は**封筒が open（発送ステータス=要確認 かつ
  実行済み=no）の場合に限る**。クローズ済み封筒への再指示は従来どおり固定応答
  （App30 の再読で判定＝gate 系検証と同じ読取を流用・追加照会なし）。
- **confirmed resume（§9-v2）との対称性（明記）**: 本規則は confirmed の再開経路
  「root decision 既存でも封筒 open＋run=head なら decision 追加なしで projection
  のみ再実行」と**同一の構造**——decision（DB・一度きり）と side effect（kintone・
  冪等再適用可）を分離し、**再指示を正規の再開手段とする**。3 decision すべてが
  「decision 一度きり＋side effect 冪等」の同型で閉じる。
- 失敗時の応答: App30 write 失敗自体は既存の例外伝播（握り潰し禁止）どおり
  aborted 応答で返し、「同じ指示をもう一度送ると続きから再適用されます」を明示する
  （文言は[人]確認・§8-6 の枠）。

## 5. (d) rejected 後の再導出・再確定経路

- **正規経路（一本）**: rejected → [人] が App34 等の入力を修正 → 「相続人を導出して」
  （P3-003-CMD 実装済み経路）→ input_hash が変わる → 新 run（supersedes_run_id=旧 run）
  → **新封筒**（冪等キー=case+input_hash が変わるため find_existing に当たらない・
  正本 §2.2 実装済み）→ 新封筒を確定。stale ガードにより旧封筒への遅れた確定は
  構造的に不能（head でない）。
- **入力が変わらない再導出**: CMD 裁定5「head 同一 input_hash 時に run を作らない」
  により新 run が生まれない。rejected root は同一 run 上で翻せない（§3.2）ため、
  このケースは**行き止まり**になる——扱いを §8-3 で裁定する（推奨: 導出コマンド応答で
  「否認済み・入力未変更」を明示し、翻意の救済は司令塔経由の運用〔held を経ずに
  rejected した誤操作の頻度は低い前提〕。CMD 裁定5 の改変はしない）。
- **rejected の誤爆対策は入口側で**: 上記が不可逆（封筒クローズ＋同一 run 翻意不可）
  だからこそ、§2.2 の復唱に帰結を明記し二重確認で遮断する（機構の追加より語彙の明確化）。

## 6. (e) App36 への影響（write 0 の維持）

- **held/rejected は App36 へ一切書かない**（insert/update/current 前進すべてなし）。
  正本 §3.2「App36 upsert は decision=confirmed のときのみ」の逐語維持であり、
  P3-003B §4B（機械 write 0）と合わせて **App36 書込み主体は confirmed handler の
  一本経路のまま不変**。
- held/rejected 経路のハンドラ実装は `hub/heir_projection` 内に置くが、phase 3
  （App36 upsert）へ**到達しない構造**（decision 分岐で phase 3 を confirmed のみに
  ゲート）とし、AST/契約テストで「held/rejected 経路に App36 の kintone 呼出しゼロ」
  を pin する（§7-4）。
- **分岐位置の固定（fix1 M01・司令塔裁定）**: held/rejected の分岐は
  **「App30 封筒再読 → run 検証（grammar/実在/case 一致/status）→ head 確認（stale
  ガード）→ 有効 leaf 判定（§3.3-v2）→ ATTORNEY_ALLOWLIST 検証」の後**、かつ
  **「App36 row-plan 構築（冪等キー search を含む）より前」**に置く。
  ＝held/rejected 経路は **App36 への照会（search_records 含む）に構造上到達しない**
  （「書かない」だけでなく「読まない」を分岐位置で保証・App36 に異常行〔重複・
  current 不正等〕が存在しても held/rejected の記録は妨げられない——§7-14）。
- **validation 範囲（fix1 M01・司令塔裁定どおり）**:
  - **gate 系（3 decision 共通）**: 封筒再読（要確認/実行済み no）・derivation_run_id
    grammar・run 実在/case 一致/status（derived/held）・head 確認（stale）・
    有効 leaf 判定・ATTORNEY_ALLOWLIST（裁定①=(A)）。
  - **projection 系（confirmed のみ）**: 胎児停止・旧 payload（zokugara_code 欠落）
    判別・写像/share grammar・冪等キー search と 6 状態分類・祖先照会。
    ＝held/rejected は「結果の中身」を検査しない（判断保留・否認は結果の精密検証を
    前提としない判断であり、App36 に触れない以上 projection 系検査は不要）。
- ATTORNEY_ALLOWLIST 検証の適用範囲は §8-1 の裁定事項（App36 write を伴わないため
  H11 の防御根拠が confirmed と同一ではない——ただし判断記録の主体保証の観点で
  推奨は「3 decision とも必須」）。

## 7. (f) テスト計画（実装票の受入条件案）

1. **後方互換 pin**: ヘルパ一般化（§3.3）後も confirmed 経路の挙動同一
   （既存 test_p3_003b_projection・test_p3_003_cmd_impl の全 green 維持・1911 基準）。
2. **decision 連鎖の契約**: held→confirmed supersede／held→held no-op／
   rejected→confirmed 中止／confirmed→held/rejected 中止（§3.2 表の全行 parametrize）。
   single-root・supersedes UNIQUE・自己参照/cross-run 拒否は既存 pin を流用。
3. **§1.2 の regress 防止（最重要）**: root=rejected の run に「確定して」→
   **App36 kintone 呼出しゼロ＋aborted 応答**（resumed 誤判定の遮断を明示 pin）。
   root=held → supersede 後に projection 実行（正常系）。
4. **App36 write 0**: held/rejected 経路で `APP_SOUZOKUNIN` への
   create/update/search 呼出しゼロ（mock 検証・封筒 update のみ許可）。
5. **封筒状態**: held=クローズなし＋detail 判断注記／rejected=完了+yes クローズ。
   注記キーが冪等照合（find_existing）に非干渉であること（保留人物ID の統合 pin 同型）。
6. **語彙・関所**: 保留/否認の発話分類（同義語閉集合・曖昧は聞き返し）・復唱文言に
   decision 種別と帰結が含まれること・OK 単回・pending invalidate（finally）。
7. **allowlist**: §8-1 の裁定結果どおりの検証適用（適用なら 3 decision とも
   allowlist 外 aborted を parametrize）。
8. **対象外 source**: 保留/否認語彙 × koseki_ingest 等 → unsupported 明示応答。

**fix1 追加（R-P3-003C-D1 指摘の 8 系統・いずれも §3.2-v2 表と 1:1 対応で pin）**:

9. **held→confirmed→row-held→再確定 resume の全連鎖**: 保留 → 確定（supersede
   INSERT・projection 実行）→ projection 中に row-held 発生（封筒 open 維持・
   `判断`=confirmed と `保留人物ID` 併存）→ 収束後の再確定 → leaf=confirmed の
   resumed 経路（decision 追加なし）で残り行のみ再反映 → row-held=0 でクローズ。
10. **rejected→rejected**: decision 追加なし・既否認の固定応答（DB 行数不変を pin）。
11. **confirmed→confirmed**: 封筒 open＋run=head のときのみ resume（decision 追加
    なし）。封筒クローズ済み → 封筒再読 aborted／head でない → stale aborted の
    3 分岐を parametrize。
12. **rejected 後・同一 input 再導出の全面 no-op**: 導出コマンドが run を作らない
    （CMD 裁定5）・封筒を作らない・応答が「否認済み・入力未変更」を明示（§5）。
13. **rejected 後・入力変更の新 run 経路**: 新 run（supersedes_run_id=旧）→ 新封筒
    起票 → 新封筒の確定が成功し、**旧封筒への遅れた確定は stale aborted**。
14. **decision 処理時の App36 無照会（異常行存在下でも）**: App36 に冪等キー重複行・
    current 不正行が存在する状態で held/rejected を指示 → **App36 への
    search/create/update 呼出しゼロ**で decision 記録が成功（mock 全記録の検証・
    §6 分岐位置の構造保証の pin）。
15. **判断注記の終端一致**: held→confirmed 後に注記=confirmed（除去されない・M03）／
    held→rejected 後に注記=rejected／注記は常に有効 leaf と一致することを全遷移で
    確認（`保留人物ID` との併存ケース含む）。
16. **allowlist 3 値対称拒否**: allowlist 外 decided_by × confirmed/held/rejected の
    3 指示すべて aborted・DB/kintone write 0（裁定①=(A) の対称適用を parametrize）。

**fix2 追加（R-P3-003C-D2 H01-R2/M01 対応の 4 系統）**:

17. **side effect 再開（§4.1）**: decision commit 成功→App30 更新失敗（mock で
    kintone 例外注入）→ 同一指示の再発行で decision 追加なし＋side effect が
    冪等再適用される——held（注記=held）／rejected（注記=rejected＋完了+yes）の
    **両方**を parametrize。再適用完了後のさらなる再指示は固定応答のみ。
18. **並行 supersede 競合の正規化（§3.3-v2 M01）**: 同一 leaf への並行 supersede
    （UNIQUE 違反注入）→ **ChainIntegrityError 型へ正規化**され、グループ全体
    rollback（decision 含む write 0）＋固定 aborted 応答。**DB 例外本文が応答・
    ログ・警報に非露出**であることを併せて pin。
19. **leaf 検索の 3 分類 fail-closed（§3.3-v2）**: 0 件=root INSERT 経路／1 件=正常
    判定／**複数件（一本鎖破損を人工的に構成）=ChainIntegrityError 型中止・write 0**
    の 3 分岐を parametrize（破損の検出が黙って先勝ちにならないこと）。
20. **detail 既存キーの保持（§4.1×M03）**: 判断注記の追記・更新時に、封筒 detail の
    既存キー（冪等キー・derivation_run_id・`保留人物ID` 等）が**すべて保持される**
    こと（dict 全体置換による欠落の防止・保留人物ID 併存ケースを含む）。

## 8. 裁定欄（[人]。CMD §8 形式・選択肢+推奨+影響。推測で決めない）

**fix1 裁定確定記録（R-P3-003C-D1 対応指示 2026-08-09・司令塔）**:
- **①=(A) 確定**——held/rejected の経路に ATTORNEY_ALLOWLIST 検証を含める指定
  （§6 分岐位置の gate 系に明記）。
- **④=(A) 確定**——confirmed 済みへの held/rejected は中止・取消は別票（§3.2-v2 表）。
- **validation 範囲の裁定**——gate 系=3 値共通／projection 系=confirmed のみ（§6）。
- **M03 の裁定**——held→confirmed は判断注記を confirmed へ更新（除去しない）・
  decision 注記と row-held の別キー維持（§4）。
- **②③は初版推奨を前提に fix1 の表・テスト系統を構成**（対応指示のテスト系統
  9/12/13 が held=封筒 open・rejected=クローズ+再導出一本を前提とするため）。
  明示裁定は凍結判定時に確認する。⑤⑥は open のまま。
  →（凍結時に下記で全確定・open 解消）

**凍結時 裁定確定記録（2026-08-09・司令塔裁定・R-P3-003C-D3 で追加[人]確認不要と判定）**:
- **②=(A) 確定**——held の封筒はクローズしない（要確認のまま維持・§4 案どおり）。
- **③=(A) 確定**——rejected はクローズ＋再導出一本・入力未変更の翻意は行き止まり受容
  （救済は司令塔経由の運用・§5 案どおり）。
- **⑤=(A) 確定**——保留/否認の理由記録は初版なし（amendments NULL）。
- **⑥=文言第1案採用**——§2.2 の復唱文言案を第1案として採用。
→ これで §8 の裁定①〜⑥は全件確定（open なし）。凍結仕様は本裁定を含めて閉じる。

| # | 論点 | 選択肢 | 推奨 | 影響 |
|---|---|---|---|---|
| 1 | **held/rejected の decided_by に ATTORNEY_ALLOWLIST 検証を課すか** | (A) 3 decision とも必須 (B) confirmed のみ（held/rejected は App36 write 0 のため） | **(A)**——判断記録（immutable 台帳）の主体保証を decision 間で対称にする。H11 の文言（confirmed=yes 遷移の防御）を超える適用だが、held/rejected も「弁護士の判断」を記録する行為であるため | (A) allowlist 未投入だと held/rejected も全拒否（fail-closed）。(B) だと事務員等の識別でも保留/否認が記録される |
| 2 | **held の封筒**: 要確認のまま維持か | (A) クローズしない（§4 案） (B) クローズする | **(A)**——再オープン遷移が存在しない実装現実（§1.5）と、held の耐久可視性=App30 キューという §9-v2 の確立原理による | (A) 要確認キューに保留封筒が滞留し得る（可視性とのトレード・意図的） (B) 後から確定する経路が閉じ、再導出強制になる |
| 3 | **rejected の封筒と「入力未変更の翻意」**: クローズ＋再導出一本で行き止まりを受容するか | (A) 受容（§5 案・救済は司令塔経由の運用） (B) rejected も封筒を残し confirmed supersede を許す（§3.2 表の rejected 行を held 同様に変更） (C) 同一 input_hash でも rejected root があれば再導出で新 run を許す（CMD 裁定5 の例外新設） | **(A)**——rejected の意味（結果の否認）を単純に保ち、翻意の頻度が低い前提で機構を増やさない。(B) は held との意味差が消える。(C) は裁定5 の改変で波及が大きい | (A) 誤って否認した場合の復旧は司令塔対応（頻度低の想定・復唱で遮断） (B)(C) は誤操作に強いが意味論/既裁定の改変を伴う |
| 4 | **confirmed 済み run への held/rejected**（projection 取消） | (A) 中止・別票（§3.2 案） (B) 本票で取消経路まで設計 | **(A)**——App36 巻き戻し（yes→no 逆遷移禁止 §3.4 と衝突）を伴い、本票のスコープ（関所前の判断語彙）を超える | (A) 確定後の誤りは再導出→新 run→confirmed で前へ回す（既存原理） (B) スコープ膨張・§3.4 との整合再設計が必要 |
| 5 | **保留/否認の理由記録** | (A) 初版は記録しない（amendments NULL） (B) 固定選択肢 enum を amendments へ (C) LINE 自由文を amendments へ | **(A)**——LINE 自由文は PII 混入面が広く（P3-001 非露出契約と衝突しやすい）、固定 enum は理由体系の設計（別裁定）を先に要する。運用上の理由メモは kintone 側（人手）で足りる | (A) 台帳単体では保留理由が追えない（App30/kintone メモ併読） (B)(C) は理由体系/redaction の追加設計が前提 |
| 6 | **語彙・復唱の文言確定**（§2 の文言・同義語閉集合） | 文言案の承認 or 修正 | §2.2 案を叩き台に[人]確定 | 文言のみ（構造に影響なし） |

## 9. スコープ外（明記・fix1 で不変）

- confirmed 済み projection の取消・App36 巻き戻し（§8-4・別票）。
- E0–E3 effect level・放棄写像（v2.4 正本依存・従来どおり別票）。
- H11 検知側（daily_healthcheck 監査）への decision-held/rejected の反映
  （検知票は正本 §3.4 の別票のまま・本票の decision が増えても「decision なしの
  yes」検知ロジックは不変）。
- run-held（機械保留）の解消経路の拡充（導出コマンド票の領分）。

## 10. fix1 改定記録（R-P3-003C-D1・2026-08-09。両時点残置・遡及書き換えにしない）

- **H01（leaf 判定への改定）**: 初版 §3.3「root 判定に decision 値を読む」を撤回し
  §3.3-v2 の**有効 leaf 判定（単一 txn 内）**へ改定。supersede 後の有効判断を root では
  表せないため。uq_heir_decision_supersedes は正常経路の分岐条件ではなく並行 race の
  後詰めであることを構造で明示。
- **H02（branch 純度の機械確認・BASE 訂正）**: 実出力（2026-08-09）:
  ```
  origin/main                      = 4afc4be40e85dca33d0b4b6d2faf0960ac7f8abc（#190 merge）
  git merge-base origin/main p3-003c-design = 4afc4be40e85dca33d0b4b6d2faf0960ac7f8abc
  git diff origin/main...p3-003c-design --stat =
    docs/design-drafts/DRAFT_P3_003C_HELD_REJECTED.md | 244 ++++++++++++++++++++++
    1 file changed, 244 insertions(+)
  ```
  （**fix2 L01 訂正**: 上記 244 insertions は**初版 `0fc36f4` 時点の値**（fix1 作業時の
  採取）。fix1 反映後の対象 SHA `3814a02` 時点の実出力は
  `383 insertions(+)`（1 file changed・同コマンドで再採取 2026-08-09）。
  両時点残置——D2 レビューの対象 SHA に対する行数証跡としては 383 が正）
  merge-base が現 origin/main と一致＝**rebase 不要**（分岐点が #190 を既に包含）。
  main に対する純差分は本 DRAFT 1 ファイルのみ。次回レビュー BASE は `4afc4be` へ訂正。
- **M01（分岐位置）**: held/rejected の分岐位置を「gate 系検証の後・App36 row-plan
  構築（search 含む）より前」に固定（§6）。validation 範囲の裁定（gate=3 値共通／
  projection=confirmed のみ）を明記。
- **M02（全遷移表）**: §3.2 の 6 行表を §3.2-v2 の閉じた 10 行表（有効 leaf 4 状態×
  指示 3 値）へ置換。rejected→rejected=追加なし固定応答・confirmed→confirmed=
  open+head 時のみ resume を明文化。
- **M03（判断注記）**: held→confirmed で注記を confirmed へ更新（除去しない）・
  decision 注記（`判断`）と row-held（`保留人物ID`）の別キー維持（§4）。
- **テスト計画**: §7 に 8 系統（9〜16）を追加。
- 次レビュー: **R-P3-003C-D2**（BASE=origin/main `4afc4be`・TARGET=p3-003c-design の
  fix1 commit）。→ 実施済み・結果は §11。

## 11. fix2 改定記録（R-P3-003C-D2・2026-08-09。両時点残置・遡及書き換えにしない）

R-P3-003C-D2 判定: **前巡 5 所見（H01/H02/M01/M02/M03）全 RESOLVED**・
新規 H01-R2/M01/L01。fix2 で以下を反映:

- **H01-R2（side effect の resumable 化）**: decision DB commit 後の App30 後続 write
  失敗の再開規則を §4.1 に固定——(a) leaf=held+保留再指示=注記 held の冪等再適用
  (b) leaf=rejected+否認再指示=注記 rejected＋完了/yes クローズの冪等再適用
  (c) 再開条件=封筒 open（要確認/no）限定・クローズ済みは固定応答。
  confirmed resume（§9-v2）と対称の「decision 一度きり＋side effect 冪等」構造で
  3 decision が同型に閉じることを明記。§3.2-v2 表の該当 2 セルへ §4.1 参照を追記。
- **M01（例外正規化）**: 並行 supersede 競合の IntegrityError
  （uq_heir_decision_supersedes／uq_heir_decision_single_root 由来）を
  **ChainIntegrityError へ正規化**（または同型の新固定例外・固定文言・値非搭載）し、
  グループ全体 aborted＋固定応答へ落とす規則を §3.3-v2 に明記。vendor/DB 例外本文の
  非露出（既存契約と同型）。あわせて leaf 検索複数件=一本鎖破損の fail-closed を追加。
- **L01（§10 行数証跡）**: 244 insertions は初版時点値と明記し、対象 SHA `3814a02`
  時点の実出力 **383 insertions** を訂正併記（両時点残置・§10）。
- **テスト計画**: §7 に 4 系統（17〜20）を追加（side effect 再開・並行競合正規化・
  leaf 3 分類 fail-closed・detail 既存キー保持）。
- 次レビュー: **R-P3-003C-D3**（H01-R2/M01/L01 の RESOLVED 判定中心・**凍結判定**。
  BASE=origin/main `4afc4be`・TARGET=p3-003c-design の fix2 commit。凍結判定時の
  [人]明示確認事項: §8 の裁定②③の明示裁定＋⑤⑥）。→ 実施済み・PASS（§12）。

## 12. 凍結記録（R-P3-003C-D3 PASS・2026-08-09）

- **判定**: H01-R2/M01/L01 全 RESOLVED・**凍結 PASS**。裁定②③⑤⑥は司令塔裁定で
  全確定（§8 凍結時裁定確定記録・追加[人]確認不要と判定）。
- **D3 の M01（実装票要件へ収載・設計改定なし）**: held/rejected の App30 後続 write
  （判断注記＋rejected のクローズ）は**単一の App30 update（1 呼出しの一括更新）**で
  行うこと——注記とステータス/実行済みを別呼出しに分けると §4.1 の中間状態が増える。
  判断注記の `decided_at` は**（再適用時も）leaf の保存値**を用いる——再指示の
  時刻で上書きしない＝注記が decision 台帳と常に一致。**§7-17 の pin 対象に含める**。
- **実装票への引き継ぎ**: 本凍結仕様（§2〜§6・§3.2-v2 表・§3.3-v2・§4.1）＋
  テスト計画 §7 全 20 系統＋上記 M01 要件。実装票の起票・着手タイミングは司令塔裁定。
