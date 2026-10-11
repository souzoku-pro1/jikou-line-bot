"""reply_followup — 大野の明示送信の後処理（App 28 human 行）の記録未了と回収（JIKOU-REPLY-Q1b-fix1 BR-08）

正本: 時効LINEボット_返信規則_v1.4.md §10-1「App 28 への記録失敗は対応中を失わせず、記録は回収ジョブ
（Q1b）で回収する」。Codex R-JIKOU-REPLY-Q1b-1 BR-08。

- 表 reply_chatlog_followup = 送信操作記録（send_operation）の **付帯情報**（op_id を主キー＝記録未了
  フラグ）。送信成功（finish=applied）の後に App 28 human 行の書込が failed / skipped（role 選択肢なし）
  だったとき pending で置く。send_operation 自体の列は増やさない（e5f8a1b4c7d9 適用済みの表を ALTER
  しない＝付帯表で持つ）。
- 回収: 既存の SEND_LEDGER_RECOVER（5 分ごと）に相乗り（send_ledger.register_recover_hook）。
  操作 ID 単位に **冪等** に再試行する——pending の行だけを対象にし、App 28 の create が成功した
  ときだけ done にする（done への更新は条件付き UPDATE＝並行して同じ op_id を処理しても 2 件目は
  not_pending）。宛先（line_user_id）は表に置かず App 29 の record_id から再解決し、送信時の
  line_user_hash と一致するときだけ書く（宛先のすり替え防止・RV-10）。
- 本文: App 28 に書くまで本表に保持し、done で NULL にする（行は消さない＝RV-08）。
- 自動再送はしない（LINE には触れない）。role 選択肢なし（skipped）は試行回数に数えない
  （大野の kintone 操作を待つ）。failed が MAX_ATTEMPTS 回続いたら exhausted（固定語彙ログ・
  以後は触らない）。
- 同じ操作 ID の再 POST（画面）は「送信済み・記録のみ再試行」に分岐し、retry_one() を直接呼ぶ。
- RV-10: ログは固定語彙＋件数のみ。
"""

import datetime
import logging

import sqlalchemy as sa

from hub import db
from hub.db import DatabaseNotConfigured, session_scope
from hub.redact import emit

logger = logging.getLogger("hub.reply_followup")

metadata = sa.MetaData()
TABLE_NAME = "reply_chatlog_followup"
KIND_CHATLOG_HUMAN = "chatlog_human"
STATE_PENDING = "pending"
STATE_DONE = "done"
STATE_EXHAUSTED = "exhausted"
STATES = (STATE_PENDING, STATE_DONE, STATE_EXHAUSTED)
MAX_ATTEMPTS = 100
BATCH_LIMIT = 20
# retry_one の戻り値（閉集合）
RESULT_WRITTEN = "written"
RESULT_SKIPPED = "skipped"
RESULT_FAILED = "failed"
RESULT_UNCONFIGURED = "unconfigured"
RESULT_NOT_PENDING = "not_pending"
RESULT_NOT_FOUND = "not_found"
RESULT_DESTINATION_MISMATCH = "destination_mismatch"
RESULT_EXHAUSTED = "exhausted"

reply_chatlog_followup = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("op_id", sa.Text, primary_key=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("approval_record_id", sa.Text, nullable=False),
    sa.Column("line_user_hash", sa.Text, nullable=False),
    sa.Column("text", sa.Text, nullable=True),
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_result", sa.Text, nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state IN ('pending', 'done', 'exhausted')", name="ck_reply_followup_state"),
    sa.Index("ix_reply_followup_state", "state", "created_at"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


async def mark_pending(op_id: str, *, business: str, approval_record_id: str,
                       line_user_hash: str, text: str, last_result: str) -> str:
    """送信成功後の App 28 記録失敗を記録未了として置く。戻り値: pending / exists / skipped（DB なし）/
    failed（固定語彙ログ）。"""
    if not _db_configured():
        return "skipped"
    now = _now()
    try:
        async with session_scope() as s:
            row = (await s.execute(sa.select(reply_chatlog_followup.c.state).where(
                reply_chatlog_followup.c.op_id == str(op_id)))).first()
            if row is not None:
                return "exists"
            await s.execute(sa.insert(reply_chatlog_followup).values(
                op_id=str(op_id), business=business, kind=KIND_CHATLOG_HUMAN,
                approval_record_id=str(approval_record_id), line_user_hash=line_user_hash,
                text=text, state=STATE_PENDING, attempts=0, last_result=last_result,
                created_at=now, updated_at=now))
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] mark pending failed (fixed reason)")
        return "failed"
    return "pending"


async def get_followup(op_id: str) -> dict | None:
    if not _db_configured():
        return None
    async with session_scope() as s:
        row = (await s.execute(sa.select(reply_chatlog_followup).where(
            reply_chatlog_followup.c.op_id == str(op_id)))).first()
    return dict(row._mapping) if row is not None else None


async def pending_ops(limit: int = BATCH_LIMIT) -> list[str]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_chatlog_followup.c.op_id).where(
            reply_chatlog_followup.c.state == STATE_PENDING)
            .order_by(reply_chatlog_followup.c.created_at.asc()).limit(int(limit)))).fetchall()
    return [r[0] for r in rows]


async def _set(op_id: str, where_state: str, **values) -> bool:
    """条件付き UPDATE（state が where_state のときだけ）。戻り値=更新したか。"""
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_chatlog_followup).where(
            reply_chatlog_followup.c.op_id == str(op_id),
            reply_chatlog_followup.c.state == where_state).values(updated_at=_now(), **values))
        return int(r.rowcount or 0) == 1


async def retry_one(op_id: str, *, resolve_user_id, write_chatlog) -> str:
    """pending の 1 件を再試行する（冪等: pending でなければ何もしない）。
    resolve_user_id(approval_record_id) -> line_user_id|None（App 29 から再解決）、
    write_chatlog(user_id, text) -> written / skipped / failed / unconfigured。
    戻り値は閉集合（RESULT_*）。"""
    row = await get_followup(op_id)
    if row is None:
        return RESULT_NOT_FOUND
    if row["state"] != STATE_PENDING:
        return RESULT_NOT_PENDING
    from hub.send_ledger import line_user_hash
    try:
        user_id = await resolve_user_id(row["approval_record_id"])
    except Exception:
        user_id = None
    if not user_id or line_user_hash(row["business"], user_id) != row["line_user_hash"]:
        await _set(op_id, STATE_PENDING, last_result=RESULT_DESTINATION_MISMATCH)
        return RESULT_DESTINATION_MISMATCH
    result = await write_chatlog(user_id, row["text"] or "")
    if result == RESULT_WRITTEN:
        # done は条件付き（並行して同じ op_id を処理した 2 件目は not_pending）。本文は保持しない
        if await _set(op_id, STATE_PENDING, state=STATE_DONE, text=None, done_at=_now(),
                      last_result=RESULT_WRITTEN):
            return RESULT_WRITTEN
        return RESULT_NOT_PENDING
    if result == RESULT_FAILED:
        attempts = int(row["attempts"]) + 1
        if attempts >= MAX_ATTEMPTS:
            await _set(op_id, STATE_PENDING, state=STATE_EXHAUSTED, attempts=attempts,
                       last_result=RESULT_FAILED)
            logger.warning("[REPLY_FOLLOWUP] chatlog retry exhausted (max attempts)")
            return RESULT_EXHAUSTED
        await _set(op_id, STATE_PENDING, attempts=attempts, last_result=RESULT_FAILED)
        return RESULT_FAILED
    # skipped（role 選択肢なし）/ unconfigured: 試行回数に数えず待つ
    await _set(op_id, STATE_PENDING, last_result=str(result))
    return str(result) if result in (RESULT_SKIPPED, RESULT_UNCONFIGURED) else RESULT_FAILED


async def run_followup(*, resolve_user_id, write_chatlog, limit: int = BATCH_LIMIT) -> dict:
    """回収ジョブ本体（SEND_LEDGER_RECOVER から呼ばれる）。戻り値は結果種別ごとの件数。"""
    counts: dict[str, int] = {}
    if not _db_configured():
        return counts
    try:
        ops = await pending_ops(limit)
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] pending scan failed (fixed reason)")
        return counts
    for op_id in ops:
        try:
            res = await retry_one(op_id, resolve_user_id=resolve_user_id, write_chatlog=write_chatlog)
        except Exception:
            res = RESULT_FAILED
            logger.warning("[REPLY_FOLLOWUP] retry failed (fixed reason)")
        counts[res] = counts.get(res, 0) + 1
    if counts:
        logger.info("[REPLY_FOLLOWUP] retried=%s written=%s",
                    emit(sum(counts.values()), "count", "log", "operator"),
                    emit(counts.get(RESULT_WRITTEN, 0), "count", "log", "operator"))
    return counts
