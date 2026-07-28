"""tracking_pg_harness — TRACKING #1/#2 の PG 実機並行実測ハーネス（合成データのみ）

出典: docs/design-drafts/TRACKING_PRE_DEPLOY_CHECKS.md #1/#2。
段取りの正: docs/design-drafts/DRAFT_TRACKING_PG_PROCEDURE.md（本スクリプトは
その手順書から呼ばれる実測部品。単独では検証用ローカル PG に対してのみ動く）。

検証内容（合格条件は invariant 中心＝裁定8/RMC-M03）:
- #1 並行 activate（TemplateVersion）:
  (a) 並行 activate 完了後も active は常に最大1（実測では厳密に 1）
  (b) 敗者は拒否される——DB 部分 unique 由来の IntegrityError／rowcount 検査由来の
      ActivationConflictError の**いずれの経路でも正当**（経路は問わない）
  (c) 敗者の transaction は全体 rollback（旧 active の retire が巻き戻り
      「active 0 件」を残さない）
- #2 並行 DerivationRun 作成:
  single-root 部分 unique（uq_derivation_run_single_root）＋supersedes UNIQUE が
  並行初回作成／並行 supersede の競合を遮断（敗者=IntegrityError または
  正規経路 pre-check の ChainIntegrityError・root/head は常に 1）

接続規律（本番接触禁止の機械強制）:
- 接続先は env **TRACKING_PG_URL** のみ（DATABASE_URL の周囲値は**無視して上書き**）。
- host が localhost/127.0.0.1/::1 以外は固定文言で拒否（Railway・本番 URL を
  誤って与えても接続しない）。URL 値はエラー文言・出力へ一切出さない。
- fix1 H01: URL は**検証済み要素からの再構築**方式——query/fragment は全面拒否
  （host/hostaddr/service/options 等の接続先上書きパラメータの迂回を遮断）し、
  元 URL 文字列は接続に使わない。
- fix1 H02: alembic 適用も本ハーネスの `migrate` サブコマンド経由が唯一の経路
  （検証済み URL を**子プロセスの env にのみ** DATABASE_URL として渡す＝
  親環境は不変・失敗時も env 残置が構造的にゼロ。alembic は import しない=
  D2 policy 整合の明示コマンド実行）。
- データは全て合成（uuid ベースの key・数字列 person_id）。削除は行わない——
  検証 DB ごと捨てるのが正（手順書の後片付け参照）。

selftest: `--sqlite-selftest` は一時 SQLite でハーネス自身の論理を検証する
（**#1/#2 の実測ではない**。出力にも SELFTEST と明記される）。

出力は件数・例外クラス名のみ（RV10 policy: PII/secret 非出力）。
"""

import argparse
import asyncio
import os
import re
import sys
import tempfile
import time
import uuid
from urllib.parse import quote, unquote, urlsplit

# `python tools/tracking_pg_harness.py` 直接実行でも hub/ を import 可能にする
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_URL_ENV = "TRACKING_PG_URL"
_ALLOWED_HOSTS = {"localhost", "127.0.0.1", "::1"}
_ROUNDS_DEFAULT = 5


class HarnessConfigError(ValueError):
    """接続設定の拒否（固定文言のみ・URL 値は含めない）。"""


_DBNAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,63}$")


def _validated_local_url(url: str) -> str:
    """検証済み要素からの**再構築**方式（fix1 H01）。

    authority（user/password/hostname/port）と dbname のみを解析・検証し、
    **query/fragment は全面拒否**——libpq/psycopg は接続文字列の query で
    host/hostaddr/port/service/options 等の**接続先上書き**が可能なため、
    閉集合の許可も置かず原則どおり拒否する。検証後は元 URL を返さず、
    検証済み要素だけから新しい接続 URL を組み立てて返す（未知の構成要素が
    素通りする経路を構造的に遮断）。エラー文言に URL 値は含めない。
    """
    if not url:
        raise HarnessConfigError(
            f"{_URL_ENV} が未設定です（検証用ローカル PG の URL を設定）")
    parts = urlsplit(url)
    if parts.scheme not in ("postgresql", "postgresql+psycopg", "postgres"):
        raise HarnessConfigError(
            f"{_URL_ENV}: scheme は postgresql 系のみ許可（値は表示しない）")
    if parts.query or parts.fragment:
        raise HarnessConfigError(
            f"{_URL_ENV}: query/fragment は不可（host/hostaddr/service 等の"
            "接続先上書きパラメータを遮断するため全面拒否・値は表示しない）")
    try:
        hostname = parts.hostname
        port = parts.port                # 非数値 port はここで ValueError
    except ValueError:
        raise HarnessConfigError(f"{_URL_ENV}: port が不正です（値は表示しない）")
    if hostname not in _ALLOWED_HOSTS:
        raise HarnessConfigError(
            f"{_URL_ENV}: host はローカル（localhost/127.0.0.1/::1）のみ許可"
            "（本番・Railway への接続は本ハーネスでは構造的に不可・値は表示しない）")
    dbname = parts.path.lstrip("/")
    if not _DBNAME_RE.fullmatch(dbname):
        raise HarnessConfigError(
            f"{_URL_ENV}: DB 名は英数・-・_（63 文字以内）のみ許可（値は表示しない）")
    # ── 検証済み要素のみで再構築（元 URL は以後使わない）──
    auth = ""
    if parts.username is not None:
        # urlsplit の username/password は未デコード → 一旦 unquote してから
        # 再 quote（二重エンコードせず正規形で再構築）
        auth = quote(unquote(parts.username), safe="")
        if parts.password is not None:
            auth += ":" + quote(unquote(parts.password), safe="")
        auth += "@"
    host = f"[{hostname}]" if ":" in hostname else hostname   # IPv6 (::1)
    portpart = f":{port}" if port is not None else ""
    return f"postgresql://{auth}{host}{portpart}/{dbname}"


def _tv_fields(template_key: str, version: str) -> dict:
    return dict(template_key=template_key, version=version,
                artifact_type="docx", unit_type="case",
                file_ref=f"synthetic/{template_key}/{version}.docx",
                content_hash="ch" * 32, content_bytes_ref="synthetic-bytes-ref",
                placeholders=["氏名"], mapping_version="mv-1",
                clause_library_version="v1:" + "0" * 64,
                generator_version="gv-1", created_by="harness")


def _run_fields(case_record_id: str, input_hash: str, supersedes=None) -> dict:
    f = dict(case_app_id="26", case_record_id=case_record_id,
             decedent_person_id="10", at_date="2026-01-01",
             frozen_case_version="v1", input_person_revisions={"11": 1},
             input_person_ids=["10", "11"], input_hash=input_hash,
             status="derived", rank=1,
             result_payload={"heirs": [{"person_id": "11", "share": "1/1"}]},
             result_hash="rh" * 32, provisional=True, engine_version="hd-1")
    if supersedes is not None:
        f["supersedes_run_id"] = supersedes
    return f


async def _race(coro_a, coro_b, accepted: tuple) -> tuple[list, list]:
    """2接続並行実行。戻り=(成功 list, 拒否クラス名 list)。
    accepted 外の例外は invariant 違反として送出（握らない）。"""
    barrier = asyncio.Barrier(2)

    async def _go(coro_fn):
        await barrier.wait()
        try:
            return ("ok", await coro_fn())
        except accepted as e:
            return ("rejected", type(e).__name__)

    results = await asyncio.gather(_go(coro_a), _go(coro_b))
    winners = [r for r in results if r[0] == "ok"]
    losers = [r[1] for r in results if r[0] == "rejected"]
    return winners, losers


async def _check1(rounds: int, out) -> bool:
    """#1: TemplateVersion 並行 activate（2 draft 競走＋既存 active ありの変形）。"""
    import sqlalchemy as sa
    from sqlalchemy.exc import IntegrityError

    from hub.db import session_scope
    from hub.template_registry import (ActivationConflictError, TemplateVersion,
                                       activate, create_template_version)

    t = TemplateVersion.__table__

    async def _active_count(key: str) -> int:
        async with session_scope() as s:
            return (await s.execute(
                sa.select(sa.func.count()).select_from(t)
                .where(t.c.template_key == key,
                       t.c.status == "active"))).scalar_one()

    ok = True
    loser_classes: dict[str, int] = {}
    serialized = 0
    for rnd in range(rounds):
        key = f"trk1-{uuid.uuid4().hex[:12]}"
        with_pre_active = rnd % 2 == 1     # 変形: 既存 active ありで retire 巻戻り検証
        if with_pre_active:
            v0 = await create_template_version(**_tv_fields(key, "v0"))
            await activate(v0, approved_by="harness")

        # 主シナリオ: **同一 draft** の並行 activate（決定的に一方が拒否される）。
        # 拒否経路: 真並行=ActivationConflictError（rowcount 0）／部分 unique の
        # IntegrityError のいずれも正当（裁定8 (b)）。直列化実行に落ちた場合のみ
        # 2本目は friendly check の ValueError（対象が draft でない）になる——
        # これは敗者拒否の別経路ではなく「並行にならなかった」回として別計上。
        v1 = await create_template_version(**_tv_fields(key, "v1"))
        winners, losers = await _race(
            lambda: activate(v1, approved_by="harness"),
            lambda: activate(v1, approved_by="harness"),
            (IntegrityError, ActivationConflictError, ValueError))
        for name in losers:
            if name == "ValueError":
                serialized += 1
            else:
                loser_classes[name] = loser_classes.get(name, 0) + 1
        active = await _active_count(key)
        # (a)(c): active は厳密に 1（敗者 rollback で 0 件にならない）
        round_ok = (active == 1 and len(winners) == 1 and len(losers) == 1)

        # 副シナリオ: 異なる draft 同士（直列化なら両成功=適法。invariant は
        # active==1 と、敗者が出た場合の例外クラスのみ検査）
        v2 = await create_template_version(**_tv_fields(key, "v2"))
        v3 = await create_template_version(**_tv_fields(key, "v3"))
        w2, l2 = await _race(
            lambda: activate(v2, approved_by="harness"),
            lambda: activate(v3, approved_by="harness"),
            (IntegrityError, ActivationConflictError))
        for name in l2:
            loser_classes[name] = loser_classes.get(name, 0) + 1
        active2 = await _active_count(key)
        round_ok = round_ok and (active2 == 1) and (len(w2) + len(l2) == 2)
        if not round_ok:
            ok = False
            out.write(f"#1 round={rnd} INVARIANT FAIL: active={active} "
                      f"active2={active2} winners={len(winners)} "
                      f"losers={len(losers)} pre_active={with_pre_active}\n")
    out.write(f"#1 activate race: rounds={rounds} "
              f"loser_classes={dict(sorted(loser_classes.items()))} "
              f"serialized_rounds={serialized} "
              f"invariants={'PASS' if ok else 'FAIL'}\n")
    return ok


async def _check2(rounds: int, out) -> bool:
    """#2: DerivationRun 並行 root 作成＋並行 supersede。"""
    import sqlalchemy as sa
    from sqlalchemy.exc import IntegrityError

    from hub.db import session_scope
    from hub.derivation_models import (ChainIntegrityError, DerivationRun,
                                       create_derivation_run)

    t = DerivationRun.__table__
    ok = True
    loser_classes: dict[str, int] = {}
    for rnd in range(rounds):
        case = f"trk2-{uuid.uuid4().hex[:12]}"
        winners, losers = await _race(
            lambda: create_derivation_run(**_run_fields(case, "a" * 64)),
            lambda: create_derivation_run(**_run_fields(case, "b" * 64)),
            (IntegrityError, ChainIntegrityError))
        async with session_scope() as s:
            roots = (await s.execute(
                sa.select(sa.func.count()).select_from(t)
                .where(t.c.case_record_id == case,
                       t.c.supersedes_run_id.is_(None)))).scalar_one()
        root_ok = (roots == 1 and len(winners) == 1 and len(losers) == 1)

        head_id = winners[0][1] if winners else None
        sup_ok = False
        if head_id is not None:
            w2, l2 = await _race(
                lambda: create_derivation_run(
                    **_run_fields(case, "c" * 64, supersedes=head_id)),
                lambda: create_derivation_run(
                    **_run_fields(case, "d" * 64, supersedes=head_id)),
                (IntegrityError, ChainIntegrityError))
            async with session_scope() as s:
                superseders = (await s.execute(
                    sa.select(sa.func.count()).select_from(t)
                    .where(t.c.supersedes_run_id == head_id))).scalar_one()
            sup_ok = (superseders == 1 and len(w2) == 1 and len(l2) == 1)
            for name in l2:
                loser_classes[name] = loser_classes.get(name, 0) + 1
        for name in losers:
            loser_classes[name] = loser_classes.get(name, 0) + 1
        if not (root_ok and sup_ok):
            ok = False
            out.write(f"#2 round={rnd} INVARIANT FAIL: root_ok={root_ok} "
                      f"supersede_ok={sup_ok}\n")
    out.write(f"#2 run race: rounds={rounds} "
              f"loser_classes={dict(sorted(loser_classes.items()))} "
              f"invariants={'PASS' if ok else 'FAIL'}\n")
    return ok


async def _amain(args, out) -> int:
    from hub import db
    from hub.derivation_models import DerivationBase
    import hub.template_registry  # noqa: F401  metadata へ template_version を登録

    if args.create_tables:
        eng = db.get_async_engine()
        async with eng.begin() as c:
            await c.run_sync(DerivationBase.metadata.create_all)

    # fix1 M02: 各 check を単調時計で個別計時。計測範囲=check 関数の全区間
    # （合成データ作成＋並行競走＋invariant 検証 select を含む・work-log 様式と同一）
    ok = True
    if args.check in ("1", "all"):
        t0 = time.monotonic()
        ok = await _check1(args.rounds, out) and ok
        out.write(f"#1 elapsed={time.monotonic() - t0:.1f}s\n")
    if args.check in ("2", "all"):
        t0 = time.monotonic()
        ok = await _check2(args.rounds, out) and ok
        out.write(f"#2 elapsed={time.monotonic() - t0:.1f}s\n")
    await db.adispose_all()
    out.write(f"result: {'PASS' if ok else 'FAIL'}"
              f"{' (SELFTEST: #1/#2 の実測ではない)' if args.sqlite_selftest else ''}\n")
    return 0 if ok else 1


# PostgreSQL DSN 様文字列（driver 修飾込み）。空白・引用符まで貪欲に拾い、
# password が *** マスク済みの表現でも username/host/dbname を残さない。
_DSN_RE = re.compile(r"postgres(?:ql)?(?:\+[A-Za-z0-9_]+)?://[^\s'\"]+")


def _sanitize_output(text: str, url: str) -> str:
    """migrate 子プロセス出力の構造化 sanitizer（fix2 H01）。

    2 層構造: ①既知 secret の完全一致置換——URL 全体・password の
    **encoded（URL 中の表記）/decoded（unquote 後）/再 quote 形の全形**を含める。
    ②DSN 様文字列の汎用 regex 置換——SQLAlchemy が password をマスクした
    `postgresql://user:***@host/db` 表現でも username/host/dbname が残るため、
    DSN の形をした文字列は丸ごと固定文言 `<DSN>` へ置換する。
    """
    parts = urlsplit(url)
    secrets = [url]
    if parts.password:
        secrets.append(parts.password)                       # encoded（URL 中表記）
        secrets.append(unquote(parts.password))              # decoded
        secrets.append(quote(unquote(parts.password), safe=""))   # 正規 quote 形
    for s in secrets:
        if s:
            text = text.replace(s, "***")
    return _DSN_RE.sub("<DSN>", text)


def _run_migrate(url: str, out) -> int:
    """alembic upgrade head のラッパー（fix1 H02・唯一の適用経路）。

    検証・再構築済み URL を **子プロセスの env にのみ** DATABASE_URL として渡して
    `python -m alembic upgrade head` を実行する——親プロセスの環境は一切変更せず、
    失敗時も env 残置が構造的に発生しない（「人が DATABASE_URL を直接設定して
    alembic を叩く」経路の廃止）。migration は明示コマンドのまま（D2 policy:
    アプリ module から alembic を import しない——本関数も import しない）。
    子プロセス出力は _sanitize_output（fix2 H01: 既知 secret 全形の完全一致置換＋
    DSN 様文字列の汎用置換）を通してから表示する。
    起動形は本関数内の 1 回のみ・argv 固定・shell 不使用・固定 cwd・検証済み
    子 env（fix2 M01: test_tracking_prep_harness の AST 構造テストで pin）。
    """
    import subprocess

    child_env = {**os.environ, "DATABASE_URL": url}
    child_env.pop(_URL_ENV, None)        # 子には DATABASE_URL のみ渡す
    proc = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=_REPO_ROOT, env=child_env, capture_output=True, text=True)
    for stream in (proc.stdout, proc.stderr):
        if stream.strip():
            out.write(_sanitize_output(stream, url).rstrip() + "\n")
    if proc.returncode == 0:
        out.write("migrate: alembic upgrade head 完了（env 残置なし）\n")
        return 0
    out.write(f"migrate: 失敗（exit {proc.returncode}）\n")
    return 1


def main(argv=None, out=sys.stdout) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("command", nargs="?", choices=("run", "migrate"),
                   default="run",
                   help="run=実測（既定）/ migrate=alembic upgrade head を"
                        "検証済み URL で適用（fix1 H02・唯一の適用経路）")
    p.add_argument("--check", choices=("1", "2", "all"), default="all")
    p.add_argument("--rounds", type=int, default=_ROUNDS_DEFAULT)
    p.add_argument("--create-tables", action="store_true",
                   help="metadata.create_all で対象 table を作成"
                        "（alembic 適用済み DB では不要）")
    p.add_argument("--sqlite-selftest", action="store_true",
                   help="一時 SQLite でハーネス論理の自己検証（#1/#2 実測ではない）")
    args = p.parse_args(argv)

    if args.rounds < 1:                  # fix1 M01: 正整数のみ（固定文言・rc 2）
        out.write("config error: --rounds は 1 以上の整数を指定してください\n")
        return 2

    from hub import db
    if args.command == "migrate":        # fix1 H02: ラッパー経由が唯一の適用経路
        try:
            url = _validated_local_url(os.environ.get(_URL_ENV, ""))
        except HarnessConfigError as e:
            out.write(f"config error: {e}\n")
            return 2
        return _run_migrate(url, out)

    if args.sqlite_selftest:
        tmp = tempfile.mkdtemp(prefix="trk_selftest_")
        os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp}/t.db"
        args.create_tables = True
        out.write("mode: SQLITE SELFTEST（ハーネス自己検証・#1/#2 の実測ではない）\n")
    else:
        try:
            url = _validated_local_url(os.environ.get(_URL_ENV, ""))
        except HarnessConfigError as e:
            out.write(f"config error: {e}\n")
            return 2
        os.environ["DATABASE_URL"] = url   # 周囲の DATABASE_URL は無視して上書き
        out.write("mode: LOCAL PG（TRACKING_PG_URL・host はローカル限定検証済み）\n")
    db.reset_for_tests()
    return asyncio.run(_amain(args, out))


if __name__ == "__main__":
    raise SystemExit(main())
