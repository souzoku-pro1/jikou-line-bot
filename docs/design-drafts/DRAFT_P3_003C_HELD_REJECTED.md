# DRAFT: P3-003c held/rejected 語彙の設計（判断保留・否認の decision 経路・設計のみ・実装禁止）

- TASK_ID: P3-003C-D 設計票（設計凍結用 DRAFT・コード/テスト実装禁止）／記録日 2026-08-09
- 目的: 凍結 `DRAFT_P3_003_ENVELOPE_FLOW.md`（以下「正本」）§3.2 の残件
  「held/rejected は App36 に触れず封筒のみクローズ（held は件名に保留理由を残すか等の
  細部は実装票で）」を設計凍結する。P3-003b（confirmed 一本・merge 済み #187）の
  実装現実を土台に、held（判断保留）／rejected（否認）の decision 経路を固定する。
- 正本参照（矛盾を作らない・編集しない）: 正本 §3.2〜3.4／`DRAFT_P3_003B_DESIGN`
  （§4B fix3 H01=機械は App36 write 0・§9-v2=resumable projection・封筒クローズ=held 行 0 のみ）／
  `DRAFT_P3_003_CMD` §8（裁定記録の形式先例）。
- 実装現実の実査基盤（2026-08-09・read-only・main `861458a`）:
  `hub/derivation_models.py`（HCD 契約・`create_confirmed_decisions_for_heads`）／
  `hub/heir_projection.py`（confirmed handler 3 phase）／`review_resolve.py`
  （RESOLVERS・resolve_group の能力ベース引数）／`dispatch_bot/review_resolve_task.py`
  （T2 語彙・復唱→pending 30分単回）／`hub/approval.py`（App30 状態機械）。
- 次レビュー: R-P3-003C-D1（設計レビュー・D巡）。

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
→ P3-003c は本ヘルパの root 判定を decision 値込みへ改修することを**必須の前提**とする
（§3.3）。confirmed 一本のままなら挙動同一＝後方互換（テスト計画 §7-1）。

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

### 3.2 supersede 関係（同一 run 内 decision 連鎖の意味論・凍結）

| 先行 root | 後続の指示 | 記録形 | 帰結 |
|---|---|---|---|
| （なし） | 保留/否認/確定 | **root decision**（supersedes NULL） | 各 decision の帰結（§4） |
| held | 確定 | **confirmed が held を supersede**（supersedes_decision_id=held.id・新 root は作らない＝single-root 遵守） | projection 実行・封筒クローズ判定へ |
| held | 保留（再） | INSERT しない（no-op 応答「既に保留です」）＝**同値 decision の連鎖を作らない** | 変化なし |
| held | 否認 | rejected が held を supersede | 封筒クローズ（§4） |
| rejected | 確定/保留 | **INSERT しない・aborted**（「否認済みです。再導出してください」）＝rejected は同一 run 上で翻せない（§5・§8-3） | 変化なし |
| confirmed | 保留/否認 | **INSERT しない・aborted**（projection 済み結果の取消は App36 巻き戻しを伴い本票スコープ外＝別票） | 変化なし |

- decision 連鎖は `uq_heir_decision_supersedes`（UNIQUE）により一本鎖が DB 強制される
  ＝「held を 2 つの decision が同時に supersede」は片方が IntegrityError で敗退
  （並行確定の構造遮断・実査 §1.1）。

### 3.3 ヘルパの一般化（§1.2 の要改修点の解消・実装票要件）

- `create_confirmed_decisions_for_heads` を decision パラメタ化
  （案: `create_decisions_for_heads(case_record_id, run_ids, decision=..., ...)`）し、
  **root 判定で decision 値を読む**:
  - root=confirmed → 従来どおり "resumed"（projection のみ再実行）
  - root=held → 指示が confirmed なら **supersede INSERT**（§3.2）・held なら no-op
  - root=rejected → ChainIntegrityError 型の中止（値は文言に載せない）
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
- held 封筒が要確認一覧に残る間の表示: 一覧の件名は起票時のまま（変更しない・
  kintone 画面では detail 注記で判別可）。**T2 応答側で「保留中」を付記する**かは
  実装票の表示判断（挙動に影響しない）。

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

## 8. 裁定欄（[人]。CMD §8 形式・選択肢+推奨+影響。推測で決めない）

| # | 論点 | 選択肢 | 推奨 | 影響 |
|---|---|---|---|---|
| 1 | **held/rejected の decided_by に ATTORNEY_ALLOWLIST 検証を課すか** | (A) 3 decision とも必須 (B) confirmed のみ（held/rejected は App36 write 0 のため） | **(A)**——判断記録（immutable 台帳）の主体保証を decision 間で対称にする。H11 の文言（confirmed=yes 遷移の防御）を超える適用だが、held/rejected も「弁護士の判断」を記録する行為であるため | (A) allowlist 未投入だと held/rejected も全拒否（fail-closed）。(B) だと事務員等の識別でも保留/否認が記録される |
| 2 | **held の封筒**: 要確認のまま維持か | (A) クローズしない（§4 案） (B) クローズする | **(A)**——再オープン遷移が存在しない実装現実（§1.5）と、held の耐久可視性=App30 キューという §9-v2 の確立原理による | (A) 要確認キューに保留封筒が滞留し得る（可視性とのトレード・意図的） (B) 後から確定する経路が閉じ、再導出強制になる |
| 3 | **rejected の封筒と「入力未変更の翻意」**: クローズ＋再導出一本で行き止まりを受容するか | (A) 受容（§5 案・救済は司令塔経由の運用） (B) rejected も封筒を残し confirmed supersede を許す（§3.2 表の rejected 行を held 同様に変更） (C) 同一 input_hash でも rejected root があれば再導出で新 run を許す（CMD 裁定5 の例外新設） | **(A)**——rejected の意味（結果の否認）を単純に保ち、翻意の頻度が低い前提で機構を増やさない。(B) は held との意味差が消える。(C) は裁定5 の改変で波及が大きい | (A) 誤って否認した場合の復旧は司令塔対応（頻度低の想定・復唱で遮断） (B)(C) は誤操作に強いが意味論/既裁定の改変を伴う |
| 4 | **confirmed 済み run への held/rejected**（projection 取消） | (A) 中止・別票（§3.2 案） (B) 本票で取消経路まで設計 | **(A)**——App36 巻き戻し（yes→no 逆遷移禁止 §3.4 と衝突）を伴い、本票のスコープ（関所前の判断語彙）を超える | (A) 確定後の誤りは再導出→新 run→confirmed で前へ回す（既存原理） (B) スコープ膨張・§3.4 との整合再設計が必要 |
| 5 | **保留/否認の理由記録** | (A) 初版は記録しない（amendments NULL） (B) 固定選択肢 enum を amendments へ (C) LINE 自由文を amendments へ | **(A)**——LINE 自由文は PII 混入面が広く（P3-001 非露出契約と衝突しやすい）、固定 enum は理由体系の設計（別裁定）を先に要する。運用上の理由メモは kintone 側（人手）で足りる | (A) 台帳単体では保留理由が追えない（App30/kintone メモ併読） (B)(C) は理由体系/redaction の追加設計が前提 |
| 6 | **語彙・復唱の文言確定**（§2 の文言・同義語閉集合） | 文言案の承認 or 修正 | §2.2 案を叩き台に[人]確定 | 文言のみ（構造に影響なし） |

## 9. スコープ外（明記）

- confirmed 済み projection の取消・App36 巻き戻し（§8-4・別票）。
- E0–E3 effect level・放棄写像（v2.4 正本依存・従来どおり別票）。
- H11 検知側（daily_healthcheck 監査）への decision-held/rejected の反映
  （検知票は正本 §3.4 の別票のまま・本票の decision が増えても「decision なしの
  yes」検知ロジックは不変）。
- run-held（機械保留）の解消経路の拡充（導出コマンド票の領分）。
