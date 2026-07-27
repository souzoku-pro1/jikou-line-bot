# 作業記録 2026-07-27: P2-MEGA-01 fix巡1〜4（P3-001/002）＋P2-BATCH-05

- TASK_ID: P2-MEGA-01（fix巡1〜4）／P2-BATCH-05（TASK 12〜14）／実施: PC-A＋[人]／記録日 2026-07-27
- 対象 branch: `feature/p3-001-derivation-run`・`feature/p3-002-template-version`
  （レビュー中・stacked: p3-002 の base=p3-001）＋ BATCH-05 の新規 3 branch
- ステータス: fix巡4 まで完了・remote 反映済み（push 5 本・[人]実行）。
  **R-P3-001-5／R-P3-002-5 のレビュー待ち**。merge は[人]の一括 merge 回で実施予定。

## 1. P3-001（DerivationRun/HCD）レビュー往復の経緯

| 巡 | commit | 主対応 |
|---|---|---|
| feat | `84a58e8` | 2表＋NH01分離・immutable 二重強制（ORM listener＋DB trigger）・supersedes UNIQUE・migration `d5e2b8a1c7f3` |
| fix1 | `e44ebcf` | R-P3-001-1: payload allowlist 入口ガード（§3.5・PII様拒否）・supersedes 連鎖健全性（実在/同一case/既supersede/head一意）・rank CHECK |
| fix2 | `1cdbe41` | R-P3-001-2: field 別 grammar/enum（person_id=数字列・share 分数・facts=条文キー17種）・single-root 部分unique（並行初回作成も DB 遮断）・HCD 連鎖 guard 横展開・Core 迂回の既知限界を pin テスト＋docstring 化 |
| fix3 | `4c62175` | R-P3-001-3: 実導出整合 grammar（胎児ID形式の明示列挙）・flag 全数写像（F6 欠落をテストが実検出→追補）・凍結47ケース接続テスト新設・HCD root 一意・**AST 機械検査新設＋pin テスト撤回**（§3 裁定） |
| fix4 | `9596e3f` | R-P3-001-4: AST 残余4経路の機械検出化・**胎児合成ID裁定の収載**（§3）・47ケース対応表（テストID集合完全一致）・flag 抽出の AST literal 化 |

## 2. P3-002（TemplateVersion）レビュー往復の経緯

| 巡 | commit | 主対応 |
|---|---|---|
| feat | `fb9443c` | §9.23 全18列・内容12列 frozen（ORM＋trigger）・DELETE 全面拒否・単一 active 部分unique＋activate() 同一tx・migration `e7a3c9d2b5f1` |
| fix1 | `46abd1b` | R-P3-002-1: draft 限定作成（repo＋INSERT trigger）・activate 競合安全化（rowcount 検査→tx 全体 rollback・TOCTOU seam 再現テスト）・approved_* write-once・generator_version 列追加 |
| fix2 | `ccee35a` | R-P3-002-2: 承認ゲート trigger 化（承認3点必須・事前設定禁止・片側拒否）・retired_at 必須・frozen 比較の IS DISTINCT FROM 厳密化（purpose NULL↔空文字） |
| fix3 | `214d527` | R-P3-002-3: TRIM 空白承認者拒否・状態×lifecycle 完全表の trigger 化・**PG 並行実測=受容（追跡リスト行き・§3）** |
| fix4 | `644666d` | R-P3-002-4: **lifecycle 遷移履歴込み完全化**（draft→retired の activated_at NULL 維持／active→retired の OLD 値固定／retired→retired の全列不変・`_gate_sql` 単一ソース化）・空白類列挙 TRIM（8種・残余は repository 層が正と明文化）・table-driven 全行列26行へ置換 |

- fix4 の「テスト提案6点」は逐語が repo 外のため **H01/M01 整合の5点と解釈して実装**
  （除外1点=PG 並行は追跡リスト済み）。解釈相違は R-P3-002-5 で回収予定。

## 3. 主要裁定（司令塔・本巡群で確定したもの）

1. **胎児合成ID（fix4・H02）**: 胎児IDは「役割語の自由文字列」を保存しない。導出器
   （凍結）の出力 `胎児:{label}` は変えず、**保存層 build_run_payload で
   `胎児:F{n}`（run 内出現順連番）へ写像**。元ラベル対応も保存しない（run 内で
   再導出可能）。role語 enum 方式は不採用（fetuses が自由入力である以上 enum は
   実データで割れる）。grammar は `^胎児:F[0-9]+$` へ縮小。
2. **AST 機械検査の採用（fix3・改定裁定）**: JSON payload 検査の DB trigger 化は
   実用不能・SQLAlchemy に table レベル Core insert event も無い → Core 迂回の防御は
   **AST 機械検査（test_p3_core_ast_policy・git 追跡 *.py 全域）＋正規 module 内
   ガードの二段**とする。fix4 で import 表・Table メソッド形・raw SQL へ経路拡張。
3. **pin テスト撤回（fix3・Codex 判定採用）**: 旧「Core 迂回が成功する現状挙動の
   pin テスト」は**脆弱性目録になるため削除**。
4. **lifecycle 完全表（fix3→fix4）**: TemplateVersion の状態不変条件（draft=全NULL／
   active=承認3点＋retired_at NULL／retired=retired_at 必須）に加え、fix4 で
   **遷移履歴込み**（どの遷移で何が変わってよいか）まで trigger で固定。
5. **PG 実機並行実測=受容・追跡**（fix3 (c)）: `TRACKING_PRE_DEPLOY_CHECKS.md` へ
   起票済み（durable 点火後の実機検証と同回・BATCH-05 TASK 12）。

## 4. P2-BATCH-05 成果（3 branch・同日 remote 反映）

| branch | HEAD | 内容 |
|---|---|---|
| `feature/p2-tracking-and-tool-hardening` | `67adcac` | P2-CHAIN-014: TRACKING_PRE_DEPLOY_CHECKS.md 起票＋gas_drift_check fix3（P2DRIFT3-M01 制御文字拒否/マスク・L01 gas/legacy 同名衝突 exit 2・テスト2件） |
| `feature/p3-003-design` | `44e2438` | P3-003 設計 DRAFT（封筒フロー結線・実装なし・docs のみ） |
| `feature/line-quality-design` | `2f1c389` | LINE 応答品質改善 DRAFT（新トラック初票・実装なし・顧客データ非接触） |

## 5. テスト推移（実測・全て `--ignore=test_triage_classification.py`）

| 時点 | 結果 |
|---|---|
| origin/main `d87b3d6`（基準・7/27 実測） | **1509 passed, 620 subtests**（76.20s） |
| p3-001 branch（fix4 後） | **1550 passed, 668 subtests**（60.03s） |
| p3-002 branch（fix4 後） | **1556 passed, 666 subtests**（72.78s） |

- **票記載の基準値 1514 との差異**: 当日実測は 1509。集計時点・集計条件
  （subtests の数え方等）の差と推定されるが未特定（[人]確認事項・次巡で照合）。
- BATCH-05 増分: `test_p2_gas_drift_tool.py` 20 passed（+2）・`test_sink_ast_policy.py`
  12 passed（回帰なし確認）。

## 6. 実行コマンド（P2BLOG-L01 対応・当日 PC-A が実行した主要コマンド）

```
# テスト（各 branch で）
python -m pytest --ignore=test_triage_classification.py -q
python -m pytest test_p3_001_derivation_models.py test_p3_001_frozen_cases.py \
    test_p3_core_ast_policy.py test_heir_derivation.py -q          # FIX L 対象
python -m pytest test_p3_002_template_version.py -q                # FIX M 対象
python -m pytest test_p2_gas_drift_tool.py -v                      # BATCH-05 TASK 12
python -m pytest test_sink_ast_policy.py -q

# branch 作成（各タスク・origin/main 起点）/ fix巡は既存 branch へ switch
git switch -c <branch> origin/main
git switch feature/p3-001-derivation-run   # fix4 は既存レビュー branch の継続

# push（全て[人]が ! で実行・PC-A は deny 遮断どおり実行せず）
git push -u origin feature/p2-tracking-and-tool-hardening
git push -u origin feature/p3-003-design
git push -u origin feature/line-quality-design
git push origin feature/p3-001-derivation-run     # 4c62175..9596e3f
git push origin feature/p3-002-template-version   # 214d527..644666d
```

- env・GAS・本番・secret・顧客データへの接触なし。railway CLI・migration 実行なし。

## 7. 運用メモ

- Codex 宛て定型（読取専用指示）の誤送が 1 件あり → [人]が票でないことを明示し
  通常運用へ復帰（実害なし・PC-A は待機で正しく停止）。
- PR は未作成（[人]の merge 回で一括する方針・案1規律の範囲内で PR 作成も可だが
  今回は不要と指示）。
- 未決の継続: (1) R-P3-001-5／R-P3-002-5 の判定 (2) merge 適格分の一括 merge 回
  (3) durable 点火裁定→TRACKING_PRE_DEPLOY_CHECKS 3 項目の実機実測。
