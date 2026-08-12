# 作業記録 2026-08-12: 設計フリーズ・スプリント（8月設計レーン全弾完了）

- TASK_ID: DOCS-BATCH-1（起草〜FREEZE）・DOCS-FB2（起草〜FREEZE）・本記録=DOCS-WORKLOG-2
- 実施: PC-A（起草・fix 反映・凍結手続き）＋Codex（D 巡レビュー）＋司令塔（裁定）＋
  [人]=大野（push・merge 判断）
- 期間: 2026-08-11 夜〜08-12 未明（1 スプリントで設計5票を凍結水準へ）
- 情報源: git 履歴（origin/docs-batch-1・origin/docs-fb2 の全コミット）と各 DRAFT の
  改定記録節（詳細の正本は各 DRAFT）

## 1. DOCS-BATCH-1 系列（4票・BASE=3c3cdb2・最終SHA=2e2783f）

- **経過**: 4票起草（A=RV08 / B=CANCEL / C=VOCAB / D=S6-2・E=docs小粒2点）→
  D1〜D8（fix1〜fix7・全26コミット）→ 凍結手続き（freeze 4コミット）。
- **最終状態**:
  | DRAFT | status | 備考 |
  |---|---|---|
  | DRAFT_RV08_SOFT_MERGE | **FROZEN**（D8 PASS） | soft merge/unmerge・裁定①〜⑦確定・App34 ID 全数調査収載（§10）＝凍結条件充足 |
  | DRAFT_P3_003C_CANCEL | **FROZEN**（D8 PASS） | confirmed 取消・supersede 型・write-set/preimage・2世代分割・裁定①〜⑧確定 |
  | DRAFT_S6_2_ANOMALY | **FROZEN**（D8 PASS） | 明細行データ化+A1死後出金/A2生前大口・行identity一本化・ingestion遷移表・裁定①〜⑩確定 |
  | DRAFT_KOSEKI_VOCAB_EXT | **APPROVED（条件付き凍結適格）** | **停止条件=除附票実物の大野確認のみ**（§5前提3）。確認後に FROZEN 化 commit。設計内容は凍結相当の規律（変更=司令塔再裁定要）に服する |
- fix 巡の要点（詳細は各 DRAFT の改定記録節）: D1=起草所見（実査訂正含む）→
  D2=方式裁定 → D3〜D5=DESIGN_OK と裁定一括 RESOLVED（fix5）→ D6/D7=確定化の
  残余 → D8=凍結判定。撤回はすべて取り消し線＋理由付き残置（両時点残置）。
- E 系（同 branch）: DRAFT_P2_DURABLE_IGNITION §8.1 Windows 注記（E1）・台帳の
  NEXT-BATCH-SURVEY 消込＋H11a 実働前提3点補記（E2・E2-01）。

## 2. DOCS-FB2 系列（1票・BASE=3c3cdb2・最終SHA=904a48e）

- **経過**: 起草（a59dba7）→ D1〜D5（fix1〜fix4）→ **FROZEN**（R-FB2-D5 PASS・
  FB2-12 LOW は司令塔 DEFER 残置）。
- **凍結内容の要点**（DRAFT_SHOKUMU_PLAN_FB2 §11 が正本）:
  - **受任確定の結合状態機械**（§3.0・7 要件閉集合）。実査の重要発見:
    **App21（時効援用）にも締結+決済の結合遷移は存在せず 2 経路は独立**
    （Stripe=レコード作成+入金済み／CloudSign=status 受任更新・相互照合なし）
    ＝結合点は全 unit で完全新設。事実定義「受任確定=締結+決済の両方」
    （H系列正本）は不変。
  - **engagement_event 層**: grammar `shokumu_engagement:{case}:{generation}`・
    **generation は初版定数 1**（一案件一受任・>1 は将来別票）・
    **状態遷移表 9 行が唯一の正**（terminal={envelope_filed, reconciled}・
    failed 行は open/terminal で排他・決定的 join=封筒 idem キー先行保存）。
  - **封筒層**: 凍結票 `file_plan_envelope`（open 限定回収）の流用・不変。
  - reconcile 三面照合（受任正本×イベント状態×封筒・日次・通知/held まで・
    自動起票なし）・裁定①〜⑤確定（H9 フック・台帳方式・通知あり・reconcile
    導入・相続放棄のみ）。

## 3. 新たに確定した[人]前提（kintone CU フィールド追加の一覧）

いずれも**凍結条件ではなく実装前提**（field code・型・値域・schema 監視追随は
各実装票で固定）:

| App | 追加フィールド | 由来 |
|---|---|---|
| App36（相続人） | `取消済み`（no/yes） | CANCEL §4.2a（insert 行取消の postimage） |
| App33（戸籍読解） | `対象人物ID` | VOCAB §5 前提4（相関一致の person 照合） |
| App35（財産） | `被相続人口座マーク`＋`対象人物ID` | S6-2 §4a（人手口座指定の器） |
| App26（相談カード・相続） | 締結事実／決済事実／CloudSign documentID／Stripe 決済 ID／受任確定正本／generation 保存先（6 保存先） | FB2 §4a(i)（結合状態機械の事実保存） |

（App34 の統合状態3フィールド〔RV08 §3.1〕・token 権限変更〔RV08 R4〕も既掲の
[人]前提として継続）

## 4. merge 待ちと解禁条件・付随記録

- **merge 待ち 2 ブランチ**: `docs-batch-1`（**2e2783f**）・`docs-fb2`（**904a48e**）
  ——いずれも docs のみ・push 済み。
- **解禁条件**: §8.2 P1 観測（監視項目G 警報0・日次死活監視に新規異常なし・5xx
  増なし）＋ **H11a 初実働検分 4 点**（scheduler 登録ログ・実行失敗警報なし・
  `App36 decision audit OK` ログ・$id カーソル query 正常完了）。
  MORNING-GATE-CHECK は 08-12 00:42/00:44 JST の 2 回とも**時刻未到来で BLOCKED**
  （初回 daily_healthcheck の発火は 08-12 7:00 JST・3 独立時計〔PC-A/DB/Railway〕の
  一致を実測）。**7:00 JST 以降に再々走**。
- **付随記録**: 2026-08-11 22:15 JST（13:15 UTC）に Railway 側の再デプロイが発生し
  現デプロイは `fb4de5a6` へ世代交代（**操作起因でないことを確認済み・実害なし**。
  startup reconcile 成功・scheduler 再登録済み〔next run 7:00 JST〕・traceback なし）。
  あわせて `railway logs` の既定呼出しが startup 断片のみ返す挙動を確認——履歴は
  `--lines`／`--since/--until`／`--http`（5xx は `--filter "@httpStatus:>=500"`）で
  取得する（次回検分の道具立て）。

## 5. 実装弾の発注順（司令塔裁定済み）

1. **RV08**（soft merge/unmerge——[人]前提: App34 CU 3 フィールド・token 権限）
2. **CANCEL**（confirmed 取消——[人]前提: App36 `取消済み` CU・flag・ALLOWLIST）
3. **VOCAB**（読解語彙拡張——**CU＋除附票実物確認の完了後**）
4. **S6-2**（取引明細・異常検知——**9月レーン**）
- **FB2 実装は別途**（H8/H9 実装票との依存整理・App26 CU 6 保存先が前提）。

## 6. 枠消化の日次一行

- 2026-08-11〜12: MAINT-4→H11a→RV-0102-PREP→実機デー前半（migration 2本＋
  ブロックA点火 §8.1 P0 全通過）→設計フリーズ・スプリント（DOCS-BATCH-1 4票＋
  FB2 1票=計5票を D 巡完走・3+1 FROZEN / 1 APPROVED）。テスト系譜 2035→2073。
  モデル実測 = Fable 5。
