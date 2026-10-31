# runbook: 案件脳 backfill の移行窓（BRAIN-ID-1a・M1 → backfill → M2）と Windows からの実行

作成: 2026-10-11（QUEUE-2026-10-11-B Q-7・docs 追随）。コード不変。
正本: `scripts/brain_case_backfill.py` docstring（§3-1・§14-2・§14-3）、`hub/brain_migration.py`、PR #266 本文。
実施記録: 2026-10-06 に本番で完了済み（下記 §3）。本書は**再実行・他環境での再現・Windows 操作者向けの補足**。

## 1. 移行窓の順序（正本どおり・順序を変えない）

```
(0) サービスの BRAIN_SYNC_ENABLED=0 → 15 分待つ（稼働中 run を止める）
(1) M1 を適用:   railway run alembic upgrade c9d2e5f8a1b3
(2) dry-run:     railway run python scripts/brain_case_backfill.py          # 既定 --dry-run（stop_stale_runs を含む）
                 → count_running_runs=0 を確認・件数を控える
(3) merge（旧コード→新コードの配備）
(4) --apply:     railway run python scripts/brain_case_backfill.py --apply  # 冪等・running run が残っていれば中止
(5) --verify:    railway run python scripts/brain_case_backfill.py --verify
(6) M2 を適用:   railway run alembic upgrade head                            # upgrade() 冒頭で --verify と同じ検算
(7) 再開順序（スクリプトが出す RESUME_ORDER）: サービス OFF のまま一回限りのプロセスで
    BRAIN_SYNC_ENABLED=1 の recheck（App 40）を complete=True まで → サービスの BRAIN_SYNC_ENABLED=1
```

- **dry-run は M1 適用後に実行する**（教訓 2026-10-06: M0（b8c1d4e7f2a5）の DB では plan が case_identity 表を参照して `error: unexpected`（rc=3）。running run の確認だけなら M0 でも count までは出る）。「dry-run → M1」の順は不可。
- `--apply` 後に旧コードへ戻すと idem_key が新形式のため重複登録の懸念（M1 downgrade は key を戻さない・M2 downgrade のみ戻す）。戻すなら M2 downgrade まで。
- alembic は PC-A（Windows）から PUBLIC URL 経由で実行する（internal host は不達）: `cd /c/work/jikou-line-bot && railway run python -c "import os; os.environ['DATABASE_URL']=os.environ['DATABASE_PUBLIC_URL']; import alembic.config; alembic.config.main(argv=['current'])"`（値は表示しない・**必ずリポジトリ直下で**）。

## 2. Windows（PC-A）からの実行で必要な補足

### 2-1. psycopg async の ProactorEventLoop 罠
Windows の既定イベントループでは `Psycopg cannot use the 'ProactorEventLoop'` で接続に失敗する（2026-10-06 実発生）。`railway run python -c "..."` で hub の async（psycopg3）を回す一行は、**先頭に次を入れる**:

```
import asyncio; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy());
```

対象になるもの:
- `scripts/brain_case_backfill.py` が出力する RESUME_ORDER の recheck コマンド（`hub.brain_sync.recheck_target('app40')`）。スクリプト本体（sync engine）は影響なし。案内文そのものは未修正（コード不変の票のため）。Windows から打つときは次の形に読み替える:

```
railway run env BRAIN_SYNC_ENABLED=1 python -c "import asyncio,os; asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()); os.environ['DATABASE_URL']=os.environ['DATABASE_PUBLIC_URL']; from hub import brain_sync as s; print([asyncio.run(s.recheck_target('app40'))['complete'] for _ in range(50)])"
```
- 送信台帳・手本集など hub の async を直接呼ぶ読取スクリプト（例: `pgverify/fee_reply_check.py`・`scripts/houki_jukuryo_precount.py:102` は対処済み）。
- SQLite（aiosqlite）のローカル試走では再現しないため、PG 向けの一行は必ずこの点を確認する。

### 2-2. その他
- `railway run` は `cd /c/work/jikou-line-bot` してから（railway link はディレクトリ単位・alembic.ini は CWD）。
- 出力の `\r` で ID が壊れることがある（`| tr -d '\r'`）。
- 値（URL・ID・氏名）は表示しない（RV-10）。件数と固定語彙のみ控える。

## 3. 実施記録（2026-10-06・本番）
1. BRAIN_SYNC_ENABLED=0 → 15 分 → dry-run（count_running_runs=0）
2. M1 c9d2e5f8a1b3 適用（current 実測）
3. PR #266 merge（main d8eb953・16:02 JST・デプロイ SUCCESS・/health 200）
4. dry-run OK（案件キー 1・fact 40・event 77・ingest 77）→ `--apply`（cases_created=1・keys 77 再計算・line_user 1・verify OK）→ `--verify` OK
5. M2 d1e4f7a0b3c6 適用（検算通過）→ current=d1e4f7a0b3c6
6. recheck app40 完了（truststore + Selector loop の一行・complete=True）→ BRAIN_SYNC_ENABLED=1 投入（デプロイ SUCCESS 16:21 JST）
7. 以後の head: e5f8a1b4c7d9（JIKOU-REPLY-Q1a・PR #267・2026-10-07 適用）。次は f7b2c9d4e1a6（JIKOU-REPLY-Q1b・merge 前に適用・PR 本文の手順）。

## 4. 改善候補（別票・コード変更）
- `scripts/brain_case_backfill.py` の RESUME_ORDER 案内文に Windows 向けの policy 行を含める（または案内文で本書を参照）。
- hub の async を呼ぶ CLI は `scripts/houki_jukuryo_precount.py` と同じく `sys.platform == "win32"` で policy を設定する共通ヘルパへ。
