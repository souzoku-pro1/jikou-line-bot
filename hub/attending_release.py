"""attending_release — UX-PENDING-1-fix1 BU-04（Codex R-UX1）: 「対応終了」の閉じ記録と対応中解除（bot 再開）の整合

閉じ記録（reply_pending の closed_external／approval_close の done_external）と send_ledger の対応中解除（conversation.attending）は
別の表・別の排他にあり 1 つのトランザクションにまとめられない。そこで:
  1. 閉じ記録と **同一トランザクション** で「解除未完了」（本表・state=pending）を永続化する（片方だけ残らない）。
  2. その直後に解除を試みる。失敗しても pending が残り、回収ジョブ（send_ledger の回収 hook・5 分間隔）が再試行する。
  3. 再操作（already＝既に閉じている）でも、その件の pending があれば再試行する。
  4. 解除対象は閉じた時点の会話 ID と human_version（人の操作でのみ進む版）で固定し、別の世代（閉じた後に大野が再び対応中にした等）は
     解除しない（stale＝完了扱い・対応中はそのまま）。承認キューだけの件で LINE userId を kintone から引けなかったときは
     ハッシュ NULL のまま残し、回収で引けた時点の会話・世代に固定してから解除する。
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
STATES = (STATE_PENDING, STATE_DONE, STATE_ABANDONED)
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
    """閉じ記録と同じトランザクション s に「解除未完了」を 1 行置く。ハッシュが分かれば今の会話 ID・human_version を固定する。"""
    conv_id = hv = None
    if line_user_hash:
        row = await send_ledger._latest_conversation(s, business, line_user_hash)
        if row is not None:
            conv_id, hv = int(row.conversation_id), int(row.human_version)
    res = await s.execute(sa.insert(attending_release).values(
        business=business, source_kind=source_kind, source_id=str(source_id),
        approval_record_id=(str(approval_record_id) if approval_record_id else None),
        line_user_hash=line_user_hash, conversation_id=conv_id, human_version=hv,
        state=STATE_PENDING, attempts=0, created_at=now, updated_at=now))
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


async def attempt(release_id: int, *, resolve_hash=None, now=None) -> str:
    """解除を 1 回試みる。戻り値は行の state（done／pending／abandoned）。例外は握って pending のまま（回数を進める）。
    resolve_hash: 承認キュー No. → line_user_hash（kintone から引く・呼び出し側が渡す。無ければ解決しない）。"""
    now = now or _now()
    row = await get_release(release_id)
    if row is None or row["state"] != STATE_PENDING:
        return (row or {}).get("state", "not_found")
    user_hash = row["line_user_hash"]
    conv_id, hv = row["conversation_id"], row["human_version"]
    try:
        if not user_hash:
            user_hash = (await resolve_hash(row["approval_record_id"])) if (resolve_hash and row["approval_record_id"]) else None
            if not user_hash:
                return await _bump(release_id, now)
        if conv_id is None:
            # 閉じた時点では会話を特定できなかった（ハッシュ未解決）→ 解決できた今の会話・世代に固定する
            st = await send_ledger.conversation_state_by_hash(row["business"], user_hash)
            if st is None:
                return await _finish(release_id, RESULT_NO_CONVERSATION, user_hash, None, None, now)
            conv_id = int(st["conversation_id"])
            hv = int((await send_ledger.conversation_version_by_hash(row["business"], user_hash)) or 0)
        result = await send_ledger.release_attending(row["business"], user_hash, conversation_id=conv_id, human_version=hv, now=now)
    except Exception:
        logger.warning("[ATTENDING_RELEASE] release attempt failed (fixed reason)")
        return await _bump(release_id, now)
    return await _finish(release_id, result, user_hash, conv_id, hv, now)


async def _finish(release_id: int, result: str, user_hash: str, conv_id, hv, now) -> str:
    async with session_scope() as s:
        await s.execute(sa.update(attending_release).where(
            attending_release.c.release_id == int(release_id), attending_release.c.state == STATE_PENDING).values(
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


async def retry_for_source(source_kind: str, source_id: str, *, resolve_hash=None, now=None) -> list[str]:
    """再操作（already）のとき: その件の解除未完了を再試行する。"""
    try:
        ids = await pending_for_source(source_kind, source_id)
    except Exception:
        logger.warning("[ATTENDING_RELEASE] pending lookup failed (fixed reason)")
        return []
    return [await attempt(i, resolve_hash=resolve_hash, now=now) for i in ids]


async def list_pending(limit: int = 50) -> list[int]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(attending_release.c.release_id).where(
            attending_release.c.state == STATE_PENDING).order_by(attending_release.c.release_id.asc())
            .limit(int(limit)))).scalars().all()
    return [int(x) for x in rows]


async def run_release_job(*, resolve_hash=None, now=None, limit: int = 50) -> dict:
    """回収ジョブ（send_ledger の回収 hook に相乗り）: 解除未完了を順に再試行する。件数だけ返す。"""
    out = {"pending": 0, "done": 0, "abandoned": 0}
    try:
        ids = await list_pending(limit)
    except Exception:
        logger.warning("[ATTENDING_RELEASE] job list failed (fixed reason)")
        return out
    for i in ids:
        st = await attempt(i, resolve_hash=resolve_hash, now=now)
        out[st if st in out else "pending"] += 1
    return out
