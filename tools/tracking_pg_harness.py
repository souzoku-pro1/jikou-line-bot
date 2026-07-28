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
- データは全て合成（uuid ベースの key・数字列 person_id）。削除は行わない——
  検証 DB ごと捨てるのが正（手順書の後片付け参照）。

selftest: `--sqlite-selftest` は一時 SQLite でハーネス自身の論理を検証する
（**#1/#2 の実測ではない**。出力にも SELFTEST と明記される）。

出力は件数・例外クラス名のみ（RV10 policy: PII/secret 非出力）。
"""

import argparse
import asyncio
import os
import sys
import tempfile
import uuid
from urllib.parse import urlsplit

# `python tools/tracking_pg_harness.py` 直接実行でも hub/ を import 可能にする
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

_URL_ENV = "TRACKING_PG_URL"
_ALLOWED_HOSTS = {"localhost", "127.0.0.1", "::1"}
_ROUNDS_DEFAULT = 5


class HarnessConfigError(ValueError):
    """接続設定の拒否（固定文言のみ・URL 値は含めない）。"""


def _validated_local_url(url: str) -> str:
    if not url:
        raise HarnessConfigError(
            f"{_URL_ENV} が未設定です（検証用ローカル PG の URL を設定）")
    parts = urlsplit(url)
    if parts.scheme not in ("postgresql", "postgresql+psycopg", "postgres"):
        raise HarnessConfigError(
            f"{_URL_ENV}: scheme は postgresql 系のみ許可（値は表示しない）")
    if parts.hostname not in _ALLOWED_HOSTS:
        raise HarnessConfigError(
            f"{_URL_ENV}: host はローカル（localhost/127.0.0.1/::1）のみ許可"
            "（本番・Railway への接続は本ハーネスでは構造的に不可・値は表示しない）")
    return url


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

    ok = True
    if args.check in ("1", "all"):
        ok = await _check1(args.rounds, out) and ok
    if args.check in ("2", "all"):
        ok = await _check2(args.rounds, out) and ok
    await db.adispose_all()
    out.write(f"result: {'PASS' if ok else 'FAIL'}"
              f"{' (SELFTEST: #1/#2 の実測ではない)' if args.sqlite_selftest else ''}\n")
    return 0 if ok else 1


def main(argv=None, out=sys.stdout) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--check", choices=("1", "2", "all"), default="all")
    p.add_argument("--rounds", type=int, default=_ROUNDS_DEFAULT)
    p.add_argument("--create-tables", action="store_true",
                   help="metadata.create_all で対象 table を作成"
                        "（alembic 適用済み DB では不要）")
    p.add_argument("--sqlite-selftest", action="store_true",
                   help="一時 SQLite でハーネス論理の自己検証（#1/#2 実測ではない）")
    args = p.parse_args(argv)

    from hub import db
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
