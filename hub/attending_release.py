"""attending_release — UX-PENDING-1-fix1 BU-04（Codex R-UX1）: 「対応終了」の閉じ記録と対応中解除（bot 再開）の整合

閉じ記録（reply_pending の closed_external／approval_close の done_external）と send_ledger の対応中解除（conversation.attending）は
別の表・別の排他にあり 1 つのトランザクションにまとめられない。そこで:
  1. 閉じ記録と **同一トランザクション** で「解除未完了」（本表・state=pending）を永続化する（片方だけ残らない）。
  2. その直後に解除を試みる。失敗しても pending が残り、回収ジョブ（send_ledger の回収 hook・5 分間隔）が再試行する。
  3. 再操作（already＝既に閉じている）でも、その件の pending があれば再試行する。
  4. 解除対象は閉じた時点の会話 ID と human_version（人の操作でのみ進む版）で固定し、別の世代（閉じた後に大野が再び対応中にした等）は
     解除しない（stale＝完了扱い・対応中はそのまま）。
  5. fix2 BU-08（Codex R-UX1-fix1）: 閉じた時点でハッシュ・会話・human_version を特定できなかった件は **自動解除しない**
     （state=needs_confirm＝「解除の要確認」）。回収で対象を確定する経路は無い。大野が一覧の [対応中を解除する] を押したときだけ、
     その時点の会話・世代を取得して **先に永続化**（pending・世代固定）し、固定した世代に対して解除を試みる。失敗しても再試行は
     永続化した世代に対してのみ行う（押した後に進んだ世代は採用しない）。
本文・氏名・LINE userId は置かない（ハッシュ・ID・状態・回数・時刻のみ）。物理削除なし（done／abandoned に遷移するだけ）。
"""

import datetime
import logging

import sqlalchemy as sa

from hub import send_ledger
from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.attending_release")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

TABLE_NAME = "attending_release"
BUSINESS = "jikou"

SOURCE_PENDING = "pending"          # 対応待ち（reply_pending.pending_id）
SOURCE_OLD = "old"                  # 承認キューだけの件（approval_close・App 29 No.）
SOURCES = (SOURCE_PENDING, SOURCE_OLD)
STATE_PENDING = "pending"           # 解除未完了
STATE_DONE = "done"                 # 解除済み（または解除不要が確定）
STATE_ABANDONED = "abandoned"       # 試行上限（運用で確認）
STATE_NEEDS_CONFIRM = "needs_confirm"   # fix2 BU-08: 対象（会話・世代）未特定＝自動解除しない・大野の確認で解除
STATES = (STATE_PENDING, STATE_DONE, STATE_ABANDONED, STATE_NEEDS_CONFIRM)
CONFIRM_UNRESOLVED = "unresolved"       # fix2 BU-08: 押したが相談者（ハッシュ）を特定できない（needs_confirm のまま）
CONFIRM_FIXED_FAILED = "fixed_failed"   # fix2 BU-08: 世代を固定したが解除に失敗（pending＝回収が同じ世代で再試行）
CONFIRM_NOT_TARGET = "not_target"       # fix2 BU-08: needs_confirm でない（処理済み・無い）
RESULT_RELEASED = send_ledger.RELEASE_RELEASED
RESULT_ALREADY_OFF = send_ledger.RELEASE_ALREADY_OFF
RESULT_STALE = send_ledger.RELEASE_STALE
RESULT_NO_CONVERSATION = send_ledger.RELEASE_NO_CONVERSATION
MAX_ATTEMPTS = 20                   # 5 分間隔の回収 × 20 ≒ 100 分

attending_release = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("release_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("source_kind", sa.Text, nullable=False),
    sa.Column("source_id", sa.Text, nullable=False),
    sa.Column("approval_record_id", sa.Text, nullable=True),
    sa.Column("line_user_hash", sa.Text, nullable=True),            # 承認キューだけの件で未解決なら NULL（回収で解決）
    sa.Column("conversation_id", _BIG, nullable=True),              # 閉じた時点の会話（固定）
    sa.Column("human_version", _BIG, nullable=True),                # 閉じた時点の人の操作の版（固定）
    sa.Column("state", sa.Text, nullable=False, server_default=STATE_PENDING),
    sa.Column("result", sa.Text, nullable=True),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
    sa.Index("ix_attending_release_state", "business", "state"),
    sa.Index("ix_attending_release_source", "source_kind", "source_id"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _row(r) -> dict:
    d = dict(r._mapping)
    for k in ("created_at", "updated_at", "done_at"):
        v = d.get(k)
        d[k] = v.isoformat() if isinstance(v, datetime.datetime) else None
    return d


async def record_in_tx(s, *, business: str, source_kind: str, source_id: str, approval_record_id: str | None,
                       line_user_hash: str | None, now) -> int:
    """閉じ記録と同じトランザクション s に「解除未完了」を 1 行置く。ハッシュが分かれば今の会話 ID・human_version を固定する（pending）。
    fix2 BU-08: ハッシュが無い・会話が無い＝対象を固定できないときは needs_confirm（自動解除しない・大野の確認で解除）。"""
    conv_id = hv = None
    if line_user_hash:
        row = await send_ledger._latest_conversation(s, business, line_user_hash)
        if row is not None:
            conv_id, hv = int(row.conversation_id), int(row.human_version)
    res = await s.execute(sa.insert(attending_release).values(
        business=business, source_kind=source_kind, source_id=str(source_id),
        approval_record_id=(str(approval_record_id) if approval_record_id else None),
        line_user_hash=line_user_hash, conversation_id=conv_id, human_version=hv,
        state=(STATE_PENDING if conv_id is not None else STATE_NEEDS_CONFIRM), attempts=0, created_at=now, updated_at=now))
    return int(res.inserted_primary_key[0])


async def close_pending(pending_id: int, actor: str, *, business: str = BUSINESS, now=None) -> tuple:
    """対応待ちを closed_external にし、同一 tx で解除未完了を置く。戻り値 (code, release_id)。code は ok／not_open／not_found／db。"""
    from hub import reply_pending
    now = now or _now()
    try:
        async with session_scope() as s:
            code = await reply_pending.close_from_open_in_tx(s, int(pending_id), reply_pending.STATE_CLOSED_EXTERNAL, actor, now)
            if code != "ok":
                return code, None
            row = (await s.execute(sa.select(reply_pending.reply_pending.c.line_user_hash,
                                             reply_pending.reply_pending.c.approval_record_id).where(
                reply_pending.reply_pending.c.pending_id == int(pending_id)))).first()
            rid = await record_in_tx(s, business=business, source_kind=SOURCE_PENDING, source_id=str(pending_id),
                                     approval_record_id=row.approval_record_id, line_user_hash=row.line_user_hash, now=now)
        return "ok", rid
    except Exception:
        logger.error("[ATTENDING_RELEASE] close pending failed (fixed reason)")
        return "db", None


async def close_old(business: str, record_id: str, actor: str, line_user_hash: str | None, *, now=None) -> tuple:
    """承認キューだけの件を done_external で閉じ、同一 tx で解除未完了を置く。戻り値 (code, release_id)。code は ok／already／bad_input／db。"""
    from hub import approval_close
    now = now or _now()
    try:
        async with session_scope() as s:
            code = await approval_close.close_in_tx(s, business, record_id, approval_close.OUTCOME_DONE_EXTERNAL, actor, now)
            if code != "ok":
                return code, None
            rid = await record_in_tx(s, business=business, source_kind=SOURCE_OLD, source_id=str(record_id),
                                     approval_record_id=str(record_id), line_user_hash=line_user_hash, now=now)
        return "ok", rid
    except Exception:
        logger.error("[ATTENDING_RELEASE] close old failed (fixed reason)")
        return "db", None


async def get_release(release_id: int) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(attending_release).where(
            attending_release.c.release_id == int(release_id)))).first()
    return _row(r) if r is not None else None


async def _bump(release_id: int, now) -> str:
    async with session_scope() as s:
        await s.execute(sa.update(attending_release).where(
            attending_release.c.release_id == int(release_id), attending_release.c.state == STATE_PENDING).values(
            attempts=attending_release.c.attempts + 1, updated_at=now))
        n = (await s.execute(sa.select(attending_release.c.attempts).where(
            attending_release.c.release_id == int(release_id)))).scalar()
        if int(n or 0) >= MAX_ATTEMPTS:
            await s.execute(sa.update(attending_release).where(
                attending_release.c.release_id == int(release_id), attending_release.c.state == STATE_PENDING).values(
                state=STATE_ABANDONED, updated_at=now))
            return STATE_ABANDONED
    return STATE_PENDING


async def attempt(release_id: int, *, now=None) -> str:
    """解除未完了（pending＝会話・世代が固定済み）を 1 回試みる。戻り値は行の state（done／pending／abandoned／needs_confirm）。
    例外は握って pending のまま（回数を進める）。
    fix2 BU-08: 対象が固定されていない行は触らない（needs_confirm に移す＝大野の確認で解除）。回収で対象を確定する経路は無い。"""
    now = now or _now()
    row = await get_release(release_id)
    if row is None or row["state"] != STATE_PENDING:
        return (row or {}).get("state", "not_found")
    user_hash, conv_id, hv = row["line_user_hash"], row["conversation_id"], row["human_version"]
    if not user_hash or conv_id is None or hv is None:
        return await _mark_needs_confirm(release_id, now)
    try:
        result = await send_ledger.release_attending(row["business"], user_hash, conversation_id=int(conv_id),
                                                     human_version=int(hv), now=now)
    except Exception:
        logger.warning("[ATTENDING_RELEASE] release attempt failed (fixed reason)")
        return await _bump(release_id, now)
    return await _finish(release_id, result, user_hash, int(conv_id), int(hv), now)


async def _mark_needs_confirm(release_id: int, now) -> str:
    async with session_scope() as s:
        await s.execute(sa.update(attending_release).where(
            attending_release.c.release_id == int(release_id), attending_release.c.state == STATE_PENDING).values(
            state=STATE_NEEDS_CONFIRM, updated_at=now))
    return STATE_NEEDS_CONFIRM


async def current_status(row: dict, *, resolve_hash=None) -> dict:
    """fix2 BU-08: 押す前の表示用。{"hash": str|None, "conversation": {conversation_id, human_version, attending}|None}。
    会話 ID・human_version・対応中は **1 回の読取** で取る（押したときの固定に使う）。本文・userId は含まない。"""
    user_hash = row.get("line_user_hash")
    if not user_hash and resolve_hash and row.get("approval_record_id"):
        try:
            user_hash = await resolve_hash(row["approval_record_id"])
        except Exception:
            user_hash = None
    conv = None
    if user_hash:
        async with session_scope() as s:
            r = await send_ledger._latest_conversation(s, row["business"], user_hash)
        if r is not None:
            conv = {"conversation_id": int(r.conversation_id), "human_version": int(r.human_version), "attending": bool(r.attending)}
    return {"hash": user_hash, "conversation": conv}


async def confirm_release(release_id: int, *, resolve_hash=None, now=None) -> tuple:
    """fix2 BU-08: 大野が [対応中を解除する] を押したときだけ呼ぶ（needs_confirm の行のみ）。
    その時点の会話 ID・human_version を取得して **先に永続化**（state=pending・世代固定）し、固定した世代に対して解除を 1 回試みる。
    戻り値 (code, state)。code: released／already_off／no_conversation（完了）・fixed_failed（固定済み・回収が同じ世代で再試行）・
    unresolved（特定できず・needs_confirm のまま）・not_target（needs_confirm でない）。"""
    now = now or _now()
    row = await get_release(release_id)
    if row is None or row["state"] != STATE_NEEDS_CONFIRM:
        return CONFIRM_NOT_TARGET, (row or {}).get("state", "not_found")
    st = await current_status(row, resolve_hash=resolve_hash)
    user_hash, conv = st["hash"], st["conversation"]
    if not user_hash:
        return CONFIRM_UNRESOLVED, STATE_NEEDS_CONFIRM
    if conv is None:
        await _finish(release_id, RESULT_NO_CONVERSATION, user_hash, None, None, now, from_state=STATE_NEEDS_CONFIRM)
        return RESULT_NO_CONVERSATION, STATE_DONE
    async with session_scope() as s:                 # 先に永続化（固定）。以後の再試行はこの世代に対してのみ
        res = await s.execute(sa.update(attending_release).where(
            attending_release.c.release_id == int(release_id), attending_release.c.state == STATE_NEEDS_CONFIRM).values(
            line_user_hash=user_hash, conversation_id=int(conv["conversation_id"]), human_version=int(conv["human_version"]),
            state=STATE_PENDING, updated_at=now))
    if int(res.rowcount or 0) != 1:
        return CONFIRM_NOT_TARGET, (await get_release(release_id) or {}).get("state", "not_found")
    state = await attempt(release_id, now=now)
    if state == STATE_DONE:
        return ((await get_release(release_id) or {}).get("result") or RESULT_RELEASED), STATE_DONE
    return CONFIRM_FIXED_FAILED, state


async def _finish(release_id: int, result: str, user_hash: str, conv_id, hv, now, *, from_state: str = STATE_PENDING) -> str:
    async with session_scope() as s:
        await s.execute(sa.update(attending_release).where(
            attending_release.c.release_id == int(release_id), attending_release.c.state == from_state).values(
            state=STATE_DONE, result=result, line_user_hash=user_hash, conversation_id=conv_id, human_version=hv,
            attempts=attending_release.c.attempts + 1, updated_at=now, done_at=now))
    logger.info("[ATTENDING_RELEASE] done result=%s", emit(result, "freetext", "log", "operator"))
    return STATE_DONE


async def pending_for_source(source_kind: str, source_id: str) -> list[int]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(attending_release.c.release_id).where(
            attending_release.c.source_kind == source_kind, attending_release.c.source_id == str(source_id),
            attending_release.c.state == STATE_PENDING).order_by(attending_release.c.release_id.asc()))).scalars().all()
    return [int(x) for x in rows]


async def retry_for_source(source_kind: str, source_id: str, *, now=None) -> list[str]:
    """再操作（already）のとき: その件の解除未完了（pending＝世代固定済み）を再試行する。needs_confirm は触らない。"""
    try:
        ids = await pending_for_source(source_kind, source_id)
    except Exception:
        logger.warning("[ATTENDING_RELEASE] pending lookup failed (fixed reason)")
        return []
    return [await attempt(i, now=now) for i in ids]


async def list_needs_confirm(limit: int = 50) -> list[dict]:
    """fix2 BU-08: 「解除の要確認」の行（古い順）。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(attending_release).where(
            attending_release.c.state == STATE_NEEDS_CONFIRM).order_by(attending_release.c.release_id.asc())
            .limit(int(limit)))).all()
    return [_row(r) for r in rows]


async def list_pending(limit: int = 50) -> list[int]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(attending_release.c.release_id).where(
            attending_release.c.state == STATE_PENDING).order_by(attending_release.c.release_id.asc())
            .limit(int(limit)))).scalars().all()
    return [int(x) for x in rows]


async def run_release_job(*, now=None, limit: int = 50) -> dict:
    """回収ジョブ（send_ledger の回収 hook に相乗り）: 解除未完了（pending＝世代固定済み）を順に再試行する。件数だけ返す。
    fix2 BU-08: needs_confirm（対象未特定）は対象外＝回収で対象を確定・解除することはない。"""
    out = {"pending": 0, "done": 0, "abandoned": 0}
    try:
        ids = await list_pending(limit)
    except Exception:
        logger.warning("[ATTENDING_RELEASE] job list failed (fixed reason)")
        return out
    for i in ids:
        st = await attempt(i, now=now)
        out[st if st in out else "pending"] += 1
    return out
