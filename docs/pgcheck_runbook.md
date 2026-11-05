# pgcheck 実走手順（Postgres 固有の検証を 1 行で回す・PGCHECK-PREP-1）

対象: 本番と同じ Railway Postgres（サービス Postgres-ak5B）に対し、**検証用スキーマ**（`pgcheck_<時刻>_<乱数>`・終了時に DROP）
だけを使って migration 往復・送信台帳の並行性・回収表の所有権・ジョブの 2 プロセス同時起動・advisory lock の残留を実測する。
本番の表（public スキーマ）には触れない。LINE／kintone への HTTP は送らない（送信関数はフェイク）。

runner はリポジトリ外: `C:\Users\User\pgverify\pgv\pgcheck_runner.py`（PC-A）。結果は `C:\Users\User\pgverify\pgv\pgcheck_<日付>.md`。

## 1. 大野の手順（秘密値は 1 度も画面に出さない）

1. **TCP proxy を作る**: Railway ダッシュボード → プロジェクト `creative-manifestation` → サービス `Postgres-ak5B` → Settings → Networking →
   「TCP Proxy」を有効化（ポートは任意）。有効化すると `DATABASE_PUBLIC_URL` が公開ホスト:ポートに更新される。
2. **接続文字列を環境変数 1 つに入れる**（PowerShell・値を表示しない）:
   ```powershell
   cd C:\work\jikou-line-bot
   $env:PGCHECK_DATABASE_URL = (& "$env:USERPROFILE\.railway\bin\railway.exe" variables --service Postgres-ak5B --json | ConvertFrom-Json).DATABASE_PUBLIC_URL
   if (-not $env:PGCHECK_DATABASE_URL) { "取得できませんでした（railway link を確認）" }
   ```
   Git Bash なら:
   ```bash
   cd /c/work/jikou-line-bot && export PATH="$PATH:/c/Users/User/.railway/bin"
   export PGCHECK_DATABASE_URL="$(railway variables --service Postgres-ak5B --json | python -c 'import json,sys;print(json.load(sys.stdin)["DATABASE_PUBLIC_URL"])')"
   ```
3. **1 行で実行**（まず dry-run で手順と項目を確認 → `--run`）:
   ```bash
   python C:/Users/User/pgverify/pgv/pgcheck_runner.py --all                      # dry-run（接続しない）
   python C:/Users/User/pgverify/pgv/pgcheck_runner.py --all --run --repo C:/work/jikou-line-bot
   ```
   - `--list` で項目一覧、`--only migration,ledger` のように選択。`--repo` は検証したい作業木（migration ファイルや
     hub/reply_followup の有無は作業木に依存＝無い項目は SKIP と表示）。
   - 終了コード 0=全 PASS／1=FAIL あり／2=設定不備。結果 md は毎回新規作成。
4. **proxy を消す**: Railway → Postgres-ak5B → Settings → Networking → TCP Proxy を削除。`DATABASE_PUBLIC_URL` が内部値に戻ることを確認。
   PowerShell の `$env:PGCHECK_DATABASE_URL` はウィンドウを閉じれば消える（`Remove-Item Env:PGCHECK_DATABASE_URL` でも可）。

## 2. 項目と合格条件

| 項目 | 内容 | 合格 |
|---|---|---|
| migration | e5f8→f7b2→a1c3→b2d4→c3e5→d4f6→e6a8→f8b0 を 1 段ずつ upgrade、並列ヘッド a9b1（e6a8 の子）・b0c1（e5f8 の子）があれば個別に upgrade、metadata の表が揃うこと、reply_exemplar に 1 行あると `downgrade e5f8` が拒否されること、全ヘッド→e5f8→heads の往復 | 各段 rc=0・拒否が出る・current が期待どおり |
| ledger | 同一 key 50 並行 begin（Operation 1・None 0）・finish の applied/late・failed 再試行の遅着 late・台帳障害で begin None（fail-open は bot 経路のみ）・recover_stale_started 1 件・**別プロセス 4 並行**（advisory lock） | すべて期待値 |
| followup | 回収表 claim の並行 2／5／10（所有権 1）・process_one 並行 10（外部書込 1）・別プロセス 2 claim（1） | 同 |
| jobs | 熟慮期間 `houki_jukuryo.send_notice`・フォロー `send_policy.send_business_notice`・ヘルスチェック `daily_healthcheck._notify_once_per_day` を **2 プロセス同時**（Barrier）→ フェイク送信 1 回 | 各 1 回（`_notify_once_per_day` が無い作業木＝concurrency-fix-1 未取込は FAIL と表示＝既知の反例） |
| locks | `send_ledger._exclusive` の中で advisory lock ≥1・外で 0・全項目終了後の pg_locks 残留 0 | 同 |

## 3. 読み方・困ったとき

- 出力と md には接続文字列・host・user・password が出ない（伏せ字 `***`／`<DSN>`）。例外本文も同じ。
- 途中で止まった場合は検証用スキーマが残ることがある: `DROP SCHEMA "pgcheck_..." CASCADE`（psql 等）。本番 public には影響しない。
- Windows の psycopg async は Selector loop を要するため runner 側で設定済み（アプリ側の設定は変えない）。
- `--selftest-sqlite` は一時 SQLite で手順の自己検証（Postgres の実測ではない・advisory lock／別プロセス項目は SKIP）。

## 4. 並列ヘッドの付け替え案（merge 順が決まった時点で後に入る側を 1 行直す）

現在の未適用 migration（PR 群）:

```
e5f8（適用済み・送信台帳） → f7b2（Q1b）→ a1c3（Q1c）→ b2d4（Q3a）→ c3e5（Q3b）→ d4f6（Q4）→ e6a8（Q5）→ f8b0（SEND-PURPOSE-2）
                           ↘ b0c1（SOUZOKU-2・e5f8 の子）                                             ↘ a9b1（FOLLOWUP-1・e6a8 の子）
```

- **a9b1 × f8b0**（どちらも e6a8 の子）: 推奨 merge 順は Q 系列 → SEND-PURPOSE-2 → FOLLOWUP-1。この順なら
  `20261016_a9b1c3d5e7f0_jikou_case_result.py` の `down_revision` を `'e6a8c0d2f4b6'` → `'f8b0c2d4e6a8'` に付け替え、
  `hub/send_ledger.PURPOSE_V2_REVISIONS` に `a9b1c3d5e7f0` を追加（test_jikou_send_purpose_2 の `_chain_after` pin が要求）。
  逆順（FOLLOWUP-1 が先）なら f8b0 の `down_revision` を `'a9b1c3d5e7f0'` に。
- **b0c1 × f7b2**（どちらも e5f8 の子）: SOUZOKU-2 は時効 Q 系列と独立なので、merge 時点の head（Q 系列が先なら f8b0 または a9b1）を
  `20261016_b0c1d2e3f4a5_souzoku_intake.py` の `down_revision` に付け替える。SOUZOKU-2 が先に入る場合は f7b2 の `down_revision` を
  `'b0c1d2e3f4a5'` に（f7b2 は本番未適用なので同一 revision の更新で可）。
- どちらも「後に入る PR で down_revision を 1 行直し、test_db_foundation の head 明示パターンに沿って往復テストを更新」が全作業。
  付け替え後に本 runner の `--only migration` で往復を確認してから本番 `alembic upgrade head`。

## 5. 本番適用の前後（従来どおり）

- 適用前: `alembic current` が期待の revision であること（PUBLIC_URL 経由・リポジトリ直下・値は表示しない）。
- 適用: `cd /c/work/jikou-line-bot && railway run python -c "import os,alembic.config;os.environ['DATABASE_URL']=os.environ['DATABASE_PUBLIC_URL'];alembic.config.main(argv=['upgrade','head'])"`。
- 適用後: `current` が head・アプリ再起動不要（起動時 upgrade はしない設計）。downgrade はデータがあると拒否される migration がある（手順書どおり退避してから）。
