# -*- coding: utf-8 -*-
"""BRAIN-ID-1a: 案件脳の backfill（M1 → **本スクリプト** → M2・裁定 R28）

正本: Desktop\\claude\\案件脳_設計_v4.3.md §3-1（移行 (2)(3)）・§14-2・§14-3・§14-7。
本体は hub/brain_migration.py（検算 verify は M2 の upgrade() 冒頭と**同じ関数**）。

使い方（Railway の本番 DB へは大野が railway run で実行・DATABASE_PUBLIC_URL を優先）:
  python scripts/brain_case_backfill.py               # --dry-run（既定・書かない。ただし
                                                      #   stop_stale_runs は実行する・R28）
  python scripts/brain_case_backfill.py --apply       # 冪等・バッチ tx・件数レポート
  python scripts/brain_case_backfill.py --verify      # 検算のみ（M2 の前提検査と同じ）
  例: railway run python scripts/brain_case_backfill.py --apply

前提（§3-1）: BRAIN_SYNC_ENABLED=0 → 15 分待つ → --dry-run（stop_stale_runs と
count_running_runs を表示）→ count=0 を確認 → --apply → --verify → alembic upgrade head。
--apply は running の run が残っていれば中止する（稼働中 backfill はしない）。

やること（冪等・既に case がある案件キーは再作成しない）:
  1. 既存の案件キー（case_app_id, case_record_id）ごとに case を 1 件（registered・
     created_via=backfill）と case_identity（kintone_record・名前空間
     "kintone:{KINTONE_SUBDOMAIN}:{app_id}"）を作る
  2. 必須表（case_fact / case_event / case_derivation）と NULL 可の表（source_ingest・
     link_history の prev/new・case_fact の prev_case_id）の case_id を埋める
  3. case_event.idem_key を case_id 内包の新形式で再計算して書き換える
  4. App 40 の現在 fact に LINE ユーザーID があれば関係者識別子（line_user・名前空間
     "line:{チャネル名}"）を作る（無ければ次の App 40 同期で埋まる）
  5. 検算（--verify と同じ）

env: DATABASE_PUBLIC_URL（優先）または DATABASE_URL、KINTONE_SUBDOMAIN（必須・名前空間）。

出力規律（BI-02・RV-10）: 出力は件数と固定語彙のみ（レコード番号・LINE ID・氏名は出さない）。
接続・書込・検算・終了処理のすべての例外は CLI 境界で捕捉し、固定理由（閉集合 FAIL_REASONS:
connect_failed / write_failed / verify_failed / unexpected）と非ゼロの終了コードだけを出す。
例外本文・SQL・SQL パラメータ・トレースバックは stdout/stderr のどちらにも出さない。

終了コード: 0=成功／1=検算の不成立・running run あり・案件キーの曖昧（固定語彙の中止）／
2=設定不備／3=例外（固定理由）。
"""

import argparse
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

FAIL_CONNECT = "connect_failed"
FAIL_WRITE = "write_failed"
FAIL_VERIFY = "verify_failed"
FAIL_UNEXPECTED = "unexpected"
FAIL_REASONS = (FAIL_CONNECT, FAIL_WRITE, FAIL_VERIFY, FAIL_UNEXPECTED)
# hub.brain_migration が ValueError で返す固定語彙の中止理由（それ以外の ValueError は例外扱い）
ABORT_REASONS = ("kintone_namespace_malformed", "case_key_ambiguous")
RC_OK, RC_NOT_OK, RC_CONFIG, RC_ERROR = 0, 1, 2, 3

RESUME_ORDER = (
    "切替後の再開順序（§14-3）:",
    "  1. サービスの BRAIN_SYNC_ENABLED は OFF のまま alembic upgrade head（M2）を適用する",
    "  2. サービスを ON にする前に、一回限りのプロセスだけ BRAIN_SYNC_ENABLED=1 を付けて",
    "     App 40 の追跡再照合を一巡させる（identity=kintone_record・line_user が正本の現在値で揃う）:",
    "       railway run env BRAIN_SYNC_ENABLED=1 python -c \"import asyncio,os;"
    "os.environ['DATABASE_URL']=os.environ['DATABASE_PUBLIC_URL'];"
    "from hub import brain_sync as s;"
    "print([asyncio.run(s.recheck_target('app40'))['complete'] for _ in range(50)])\"",
    "     （complete=True が出るまで。サービス側の env は触らない）",
    "  3. その後にサービスの BRAIN_SYNC_ENABLED=1（scheduler の通常運転＝App 28/30 の同期・再照合）",
    "     （未採用のまま通過した会話を作らない）",
)


class _Failed(Exception):
    """CLI 境界で固定理由に写した失敗（本文は FAIL_REASONS のいずれか）。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason if reason in FAIL_REASONS else FAIL_UNEXPECTED


class _Aborted(Exception):
    """固定語彙の中止（ABORT_REASONS）。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _guard(reason: str, fn, *args, **kwargs):
    """fn の例外を固定理由へ写す。元の例外は連鎖させない（from None）＝本文・SQL・
    パラメータがどこにも出ない。固定語彙の ValueError だけは中止理由として通す。"""
    try:
        return fn(*args, **kwargs)
    except (_Failed, _Aborted):
        raise
    except ValueError as exc:
        text = str(exc)
        if text in ABORT_REASONS:
            raise _Aborted(text) from None
        raise _Failed(reason) from None
    except Exception:
        raise _Failed(reason) from None


def _print_counts(out, title: str, counts: dict, indent: str = "  ") -> None:
    out.write(f"{title}\n")
    for k in sorted(counts):
        v = counts[k]
        if isinstance(v, dict):
            _print_counts(out, f"{indent}{k}:", v, indent + "  ")
        else:
            out.write(f"{indent}{k}={v}\n")


def _print_verify(out, label: str, result: dict, *, note: str = "") -> int:
    out.write(f"{label}: {'OK' if result['ok'] else 'FAILED'}{'' if result['ok'] else note}\n")
    for prob in result["problems"]:
        out.write(f"problem: {prob}\n")
    return RC_OK if result["ok"] else RC_NOT_OK


def _run(args, out, conn, brain_migration, namespace_of, line_ns) -> int:
    if args.verify:
        result = _guard(FAIL_VERIFY, brain_migration.verify, conn)
        out.write(f"verify: {'OK' if result['ok'] else 'FAILED'}\n")
        _print_counts(out, "counts:", result["counts"])
        for prob in result["problems"]:
            out.write(f"problem: {prob}\n")
        if result["ok"]:
            out.write("\n".join(RESUME_ORDER) + "\n")
        return RC_OK if result["ok"] else RC_NOT_OK
    stopped = _guard(FAIL_WRITE, brain_migration.stop_stale_runs, conn,
                     older_than_minutes=args.stale_minutes)
    running = _guard(FAIL_WRITE, brain_migration.count_running_runs, conn)
    if args.apply:
        out.write(f"stop_stale_runs(older_than={args.stale_minutes}min)={stopped} "
                  f"count_running_runs={running}\n")
        if running:
            out.write("abort: running の sync_run が残っています（BRAIN_SYNC_ENABLED=0 で "
                      "15 分待ってから再実行）\n")
            return RC_NOT_OK
        counts = _guard(FAIL_WRITE, brain_migration.backfill, conn, namespace_of=namespace_of,
                        line_namespace=line_ns, batch=args.batch)
        _print_counts(out, "apply:", counts)
        result = _guard(FAIL_VERIFY, brain_migration.verify, conn)
        return _print_verify(out, "verify", result)
    out.write("mode: DRY-RUN（台帳は変更しない・stop_stale_runs のみ実行）\n")
    out.write(f"stop_stale_runs(older_than={args.stale_minutes}min)={stopped} "
              f"count_running_runs={running}\n")
    planned = _guard(FAIL_UNEXPECTED, brain_migration.plan, conn, namespace_of=namespace_of,
                     line_namespace=line_ns)
    _print_counts(out, "plan:", planned)
    result = _guard(FAIL_VERIFY, brain_migration.verify, conn)
    _print_verify(out, "verify(now)", result, note="（--apply 前は FAILED が正常）")
    return RC_OK


def _main(args, out) -> int:
    url = os.environ.get("DATABASE_PUBLIC_URL", "").strip() or os.environ.get(
        "DATABASE_URL", "").strip()
    if not url:
        out.write("config error: DATABASE_PUBLIC_URL / DATABASE_URL が未設定です\n")
        return RC_CONFIG
    subdomain = os.environ.get("KINTONE_SUBDOMAIN", "").strip()
    if not subdomain and not args.verify:
        out.write("config error: KINTONE_SUBDOMAIN が未設定です（名前空間を作れない）\n")
        return RC_CONFIG
    os.environ["DATABASE_URL"] = url            # hub.db の一点集約経由で接続（D4）

    def _imports():
        from hub import brain_ledger, brain_migration, brain_sync, db
        return brain_ledger, brain_migration, brain_sync, db

    ledger, brain_migration, brain_sync, db = _guard(FAIL_UNEXPECTED, _imports)

    def namespace_of(app_id: str) -> str:
        return ledger.kintone_namespace(subdomain, app_id)

    line_ns = _guard(FAIL_UNEXPECTED, brain_sync.line_namespace)
    conn = None
    rc = RC_ERROR
    try:
        engine = _guard(FAIL_CONNECT, db.get_engine)
        conn = _guard(FAIL_CONNECT, engine.connect)
        rc = _run(args, out, conn, brain_migration, namespace_of, line_ns)
    finally:
        # 終了処理の例外も固定理由にする（先に起きた失敗は上書きしない）
        try:
            if conn is not None:
                conn.close()
            db.dispose_all()
        except Exception:
            if rc == RC_OK:
                raise _Failed(FAIL_UNEXPECTED) from None
    return rc


def main(argv=None, out=sys.stdout) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="既定: 変更件数の表示のみ")
    mode.add_argument("--apply", action="store_true", help="backfill を実行（冪等）")
    mode.add_argument("--verify", action="store_true", help="検算のみ（M2 の前提検査と同じ）")
    p.add_argument("--batch", type=int, default=500, help="commit の単位（既定 500）")
    p.add_argument("--stale-minutes", type=int, default=15,
                   help="stop_stale_runs の閾値（既定 15 分・R28）")
    args = p.parse_args(argv)
    if args.batch < 1:
        out.write("config error: --batch は 1 以上\n")
        return RC_CONFIG
    try:
        return _main(args, out)
    except _Aborted as exc:
        out.write(f"abort: {exc.reason}\n")
        return RC_NOT_OK
    except _Failed as exc:
        out.write(f"error: {exc.reason}\n")
        return RC_ERROR
    except Exception:                           # 想定外はすべて固定理由（本文を出さない）
        out.write(f"error: {FAIL_UNEXPECTED}\n")
        return RC_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
