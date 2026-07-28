# DRAFT: TRACKING #1/#2 — PG 実機並行実測の段取り手順書

- TASK_ID: TRACKING-PREP（準備票・実施は[人]が日程を決めた実施日の票で行う）
- 記録日: 2026-07-28／出典: `docs/design-drafts/TRACKING_PRE_DEPLOY_CHECKS.md`
  #1（TemplateVersion 並行 activate）/#2（DerivationRun 並行作成）。
  両項目とも **migration が main へ merge 済みで実施可能条件入り**（durable 点火と独立）。
- 実測部品: `tools/tracking_pg_harness.py`（合成データのみ・接続先はローカル限定を
  機械強制・本書 §4）。

## 1. 前提（環境実査 2026-07-28・PC-A）

| 項目 | 実査結果 | 帰結 |
|---|---|---|
| docker | **なし**（`docker` コマンド不在） | コンテナ PG は Docker Desktop 導入が前提 |
| psql / initdb | **なし**（PATH に PostgreSQL クライアント/サーバなし） | ネイティブ PG は導入が前提 |
| psycopg | **あり**（3.3.4・requirements.lock 準拠） | ハーネス側の接続要件は充足 |
| alembic | あり（versions に derivation_run / template_version 済み） | migration 適用可能 |

**PC-A 単独では現状ローカル PG を起動できない**。実施日に[人]が下記いずれかの
手段を用意する（本書はどちらでも進められるよう両方を記す）:

- **(A) PostgreSQL ネイティブ導入**（zip 版可・サービス登録不要の一時インスタンス）
- **(B) Docker Desktop 導入後にコンテナ起動**（後片付けが最も簡単・推奨）

## 2. 分離規律（本番接触禁止）

- 検証は**使い捨てのローカル PG インスタンス＋検証専用 DB**（例: `tracking_check`）
  で行う。本番（Railway）へは接続しない——ハーネス自体が
  **`TRACKING_PG_URL` の host を localhost/127.0.0.1/::1 に限定**しており、
  Railway/本番 URL を誤って与えても接続前に固定文言で拒否する（URL 値は非表示）。
- 周囲の `DATABASE_URL` はハーネスが**無視して上書き**する（誤接続経路なし）。
  **`DATABASE_URL` は migrate 子プロセス内だけで設定され、利用者は設定しない**
  （fix2 M02: §3.2 のラッパー経由が唯一の適用経路・人が設定する手順は存在しない）。
- データは全て合成（uuid ベース key・数字列 person_id）。片付けは **DB ごと削除**
  （行 delete はしない——immutable 台帳の削除操作を書かない規律とも整合）。

## 3. 段取り

### 3.1 PG 起動（いずれか）

**(B) Docker（推奨・導入済みの場合）**
```powershell
docker run --name trk-pg -e POSTGRES_PASSWORD=trk -e POSTGRES_DB=tracking_check `
  -p 127.0.0.1:5433:5432 -d postgres:16
```

**(A) ネイティブ（zip 版の一時インスタンス）**
```powershell
& "C:\pgsql\bin\initdb.exe" -D C:\pgsql\trkdata -U postgres -E UTF8
& "C:\pgsql\bin\pg_ctl.exe" -D C:\pgsql\trkdata -o "-p 5433" start
& "C:\pgsql\bin\createdb.exe" -p 5433 -U postgres tracking_check
```

接続 URL（以降 `<URL>`）: `postgresql://postgres:trk@127.0.0.1:5433/tracking_check`
（(A) はパスワード部なし）。**port 5433 を使い、既定 5432 と混同しない**。

### 3.2 スキーマ適用（fix1 H02: **ハーネスの migrate ラッパーが唯一の適用経路**）

```powershell
$env:TRACKING_PG_URL = "<URL>"
python tools\tracking_pg_harness.py migrate   # alembic upgrade head（実 DDL 全部）
```

- **「人が DATABASE_URL を直接設定して alembic を叩く」手順は廃止**（fix1 H02）——
  ラッパーが検証・再構築済みのローカル URL を**子プロセスの env にのみ**
  DATABASE_URL として渡して `python -m alembic upgrade head` を実行する。
  親環境は一切変更されず、**失敗時も env 残置が構造的に発生しない**。
  子プロセス出力は URL・パスワードを伏字化して表示（D2 policy どおり alembic は
  import せず明示コマンド実行のまま）。
- 代替: ハーネスの `--create-tables`（`metadata.create_all`）でも対象 table と
  部分 unique index は作られ **invariant 検証には十分**。ただし immutable trigger・
  approve_gate trigger 等の migration 固有 DDL は作られない——**実測の本旨
  （#1/#2 は index/制約の方言差確認）には影響しないが、migrate 適用を正とする**。

### 3.3 実測

```powershell
$env:TRACKING_PG_URL = "<URL>"     # 3.2 で設定済みならそのまま
python tools\tracking_pg_harness.py --check all --rounds 20
Remove-Item Env:TRACKING_PG_URL
```

- `--check 1` / `--check 2` で個別実行可。`--rounds` は 20 を既定推奨・**1 以上の
  整数のみ**（0/負数は固定文言＋終了コード2・fix1 M01）
  （競合の再現回数を稼ぐ。1 round = #1 同一 draft 競走＋異 draft 競走・
  #2 root 競走＋supersede 競走）。
- URL は**検証済み要素からの再構築**方式（fix1 H01）: query/fragment 付き URL
  （`?host=`／`hostaddr=`／`service=` 等の接続先上書きパラメータ）は固定文言で
  拒否される（URL 値は表示されない）。
- 事前にハーネス自己検証を走らせる場合: `--sqlite-selftest`（一時 SQLite・
  **#1/#2 の実測ではない**。準備票時点で PASS 済み: #1 loser=
  ActivationConflictError×3・#2 loser=ChainIntegrityError/IntegrityError 混在）。

## 4. 合格条件（invariant 中心＝裁定8/RMC-M03・ハーネスが機械判定）

**#1（TemplateVersion 並行 activate）**
- (a) 並行 activate 完了後も **active は常に最大1**（実測 assert は「厳密に 1」）
- (b) **敗者は拒否される**——DB 部分 unique 由来の **IntegrityError**／rowcount
  検査由来の **ActivationConflictError** の**いずれの経路でも正当**（経路は問わない）
- (c) **敗者の transaction は全体 rollback**——旧 active の retire が巻き戻り
  「active 0 件」を残さない（(a) の active==1 assert が 0 件残置を検出する。
  既存 active ありの変形 round で retire 巻戻りを実測）
- 注: 実行が直列化に落ちた round（2本目が friendly check の ValueError になる）は
  「並行にならなかった回」として `serialized_rounds` に別計上される——敗者拒否の
  別経路ではない。serialized が rounds の大半を占める場合は rounds を増やす。

**#2（DerivationRun 並行作成）**
- 並行 root 作成: 勝者 1・敗者 1（**IntegrityError**（single-root 部分 unique）
  または正規経路 pre-check の **ChainIntegrityError** のいずれも正当）・
  root（supersedes IS NULL）行は常に 1
- 並行 supersede: 同一 head への置換は 1 本のみ成立（supersedes UNIQUE）・
  敗者は上記いずれかの例外・head 連鎖は一本鎖のまま

ハーネスの終了コード: 0=全 invariant PASS／1=FAIL（round 別の違反行を出力）／
2=接続設定・--rounds 不正の拒否。出力は件数・例外クラス名・所要秒のみ
（RV10: PII/secret 非出力）。

## 5. 結果の work-log 保存様式

`docs/work-logs/<実施日>_tracking-pg-1-2.md` に以下のみを記録
（RV10 policy: 件数・所要・競合結果のみ・URL/データ値は書かない）:

```markdown
# TRACKING #1/#2 PG 実機並行実測（<実施日>）
- 環境: ローカル PG <版>（docker|native）・alembic head=<revision>
- コマンド: tracking_pg_harness --check all --rounds <N>
- #1: loser_classes=<dict> serialized_rounds=<n> invariants=<PASS|FAIL> 所要=<#1 elapsed 出力値>s
- #2: loser_classes=<dict> invariants=<PASS|FAIL> 所要=<#2 elapsed 出力値>s
- 判定: #1/#2 とも合格条件（裁定8 (a)(b)(c)）充足 → TRACKING 表の状態を「実施済み
  （<日付>・work-log 参照）」へ更新（別 commit）
```

- **所要の計測範囲（fix1 M02・ハーネス出力と一意対応）**: ハーネスが
  `time.monotonic()` で check ごとに計時し `#1 elapsed=<秒>s`／`#2 elapsed=<秒>s`
  を出力する。範囲=当該 check 関数の全区間（合成データ作成＋並行競走＋invariant
  検証 select を含む）。work-log にはこの出力値を転記する（手計時しない）。

TRACKING_PRE_DEPLOY_CHECKS.md の #1/#2「状態」列の更新は**実施日の票**で行う
（本準備票では変更しない）。

## 6. 後片付け

```powershell
# (B) docker の場合
docker rm -f trk-pg
# (A) ネイティブの場合
& "C:\pgsql\bin\pg_ctl.exe" -D C:\pgsql\trkdata stop
Remove-Item -Recurse -Force C:\pgsql\trkdata
# 共通: env が残っていないことを確認
Get-ChildItem Env: | Where-Object Name -match "TRACKING_PG_URL|DATABASE_URL"
```

## 7. 本準備票の到達点と残り

- 済: ハーネス実装（接続ローカル限定の機械強制・invariant 機械判定・
  SQLite selftest PASS）・本手順書。
- 未（実施日・[人]ゲート）: PG 手段の用意（§1）→ §3 実測 → §5 work-log →
  TRACKING 表更新。#3（received/processing 境界値）は durable 点火後・本書の対象外。
