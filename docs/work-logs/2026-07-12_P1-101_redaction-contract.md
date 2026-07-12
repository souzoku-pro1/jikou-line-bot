# 作業記録 2026-07-12: P1-101 redaction contract + sink AST防波堤（RV-10 PR-1）

- Phase/Gate: Phase 1・RV-10 PR-1（redaction 基盤）
- 実施: Claude Code（PC-A）／レビュー: Codex（R-P1-101・全6巡）
- 結果: **PR #110 マージ済み**（マージ後 main = `9dfafde6cae99480c70df278a3b5d1e192878ce6`）
- 全suite: マージ後 main で **1,183 passed / FAIL 0 / skip 0**（+296 subtests）

## 1. 最終成果

- **hub/redact.py**: `emit(value, kind, sink, audience)` 契約。sink×audience 許可ペア行列・
  kind 分類（PII / §13.1禁止〔contract/fax/qa/vendor_raw〕/ document_metadata / external_ref /
  token・secret / passthrough〔record_id・count〕）。unknown kind / 構造化値 / None / 内部例外 /
  行列外 pair = 完全抑止（**fail-closed・原文非漏洩**）。record_id/count は値域検証
  （英数64桁 / 非負整数18桁・fullmatch）。
- **AST 防波堤**（test_sink_ast_policy.py + redaction_sink_allowlist.json）: ホワイトリスト方式。
  sink 呼び出しの全引数が 定数 / 定数f-string / `from hub.redact import emit`（唯一の信頼形式）
  由来の emit（外側 sink と sink/audience policy 一致）/ それらの結合 のみで合格。
  emit の shadow（def/代入/param/for/with/except/comprehension/walrus/global/nonlocal/別import/
  match capture/信頼emit同居の star import/del emit）と、信頼emitファイル内の
  exec()/globals()/locals() 動的束縛は poison。exc_info/stack_info は True でも違反。
  完全動的経路（builtins fallback 等）は静的解析の原理的限界として docstring 明記。
- **台帳 211件**（`redaction_sink_allowlist.json`）: sink:print 136 / sink:logger 51 /
  sink:httpexception 24。**3検査併存** — no_new（新規違反ゼロ）/ stale（解消済み削除必須）/
  monotonic（総数は baseline_count=211 上限・単調減少のみ）。
- **本番挙動変更ゼロ**: 新規4ファイルのみ・既存ファイル変更ゼロ・アプリからの hub.redact
  import ゼロ（テストのみ参照）。既存 sink の切替（S1〜S4）は PR-2 以降。

## 2. R-P1-101 全6巡の裁定記録（全所見 ACCEPT・REJECT 0）

| 巡 | 対象SHA | RESULT | 主要所見 → 対応 |
|---|---|---|---|
| 1（push前） | — | BLOCKED | push 前のため実レビュー不可（形式確認のみ） |
| 実レビュー | `749e50d` | CHANGES_REQUIRED | H01/H02/H03 の検出力不足 → P1-101a で AST 強化 |
| 2 | `38d1325` | CHANGES_REQUIRED | 生変数の素通し・stale 検査欠如 → P1-101b でホワイトリスト反転・stale/monotonic |
| 3 | `062edd4` | CHANGES_REQUIRED | 任意 emit 信頼・policy 非照合・exc_info=True 安全扱い → P1-101c で emit 束縛限定・policy 一致・exc_info 違反化 |
| 4 | `452ea18` | CHANGES_REQUIRED | emit shadowing（fake.emit・ローカル def 等）→ P1-101d で 1形式化・shadow 全面禁止 |
| 5 | `944b611` | CHANGES_REQUIRED | match capture / star import / del emit / 動的束縛 → P1-101e で検出追加・dynamic_name_op 別規則 |
| 6 | `817f07f` | **PASS・マージ推奨 YES** | shadow 網羅性の最終確認・残余なし |

commit 構成: `749e50d`(P1-101 契約本体+AST土台)/ `38d1325`(101a AST強化)/
`062edd4`(101b ホワイトリスト反転)/ `452ea18`(101c emit束縛+policy+exc_info)/
`944b611`(101d 1形式化+shadow禁止)/ `817f07f`(101e match/star/del/dynamic)。

## 3. モデル運用の記録（規律追記）

- **P1-101d 実装区間で Opus 4.8 迂回の疑い**: 当該区間の完了報告のモデル記載と実態に齟齬が
  あり、**大野の指摘で発覚**。`/model` は Fable へ復帰済み。**成果物自体は R-P1-101 全6巡の
  Codex レビューで担保**されており、内容上の欠陥は最終 PASS 時点で解消済み。
- **規律（本記録で固定）**: 以後の実装票は **開始時・終了時の `/model` 実測値を
  COMPLETION_REPORT に必ず記載**する（P1-101e・本タスク P1-101f から適用中）。

## 4. 台帳注記（S1〜S4 への引き継ぎ）

- 台帳 211件は emit() 経由へ移すべき既存の sink 直書き。PR-2〜PR-5 で1件ずつ解消し、
  当該 entry を allowlist から削除（stale 検査が削除漏れを検知）。baseline_count=211 は
  上限として据え置き（単調減少のみ許可）。
- **定数 print 26件は redaction 台帳の対象外**（引数が定数のみで漏洩リスクなし）。ただし
  DRAFT §5 の「print 全廃」方針により、**PR-4 で別途 grep 回収**して logger 化する
  （redaction とは別軸の style 是正）。

## 5. クローズ

P1-101 系列（契約本体 + AST 防波堤）はマージをもってクローズ。次は **PR-2**
（S1 の4経路を業務チャネルへ移送 + notify fail-closed + 最小 dead-man 同梱）で、
この台帳を単調減少で削り始める。
