"""approval_close — UX-PENDING-1 (2): 承認キュー（App 29）だけにある件を画面で閉じた記録（追記のみ・RV-08）

V2 以前の承認キュー項目（対応待ちの表に行が無く、kintone の承認キューに「送信済み=no」で残っている件）を、統合一覧の
「対応終了」「不要」で閉じるための記録。kintone は書かない（承認が無いため）。最新行の outcome がその件の状態:
  done_external（別経路で返信済み・対応終了）／not_needed（不要）／reopened（未処理に戻した）。
物理削除は無い。レコード ID・結果・操作者・時刻だけを置く（本文・氏名・LINE userId なし）。
"""

import datetime
import logging

import sqlalchemy as sa

from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.approval_close")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

TABLE_NAME = "approval_close"

OUTCOME_DONE_EXTERNAL = "done_external"
OUTCOME_NOT_NEEDED = "not_needed"
OUTCOME_REOPENED = "reopened"
OUTCOMES = (OUTCOME_DONE_EXTERNAL, OUTCOME_NOT_NEEDED, OUTCOME_REOPENED)
CLOSED_OUTCOMES = (OUTCOME_DONE_EXTERNAL, OUTCOME_NOT_NEEDED)

approval_close = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("close_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("approval_record_id", sa.Text, nullable=False),
    sa.Column("outcome", sa.Text, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Index("ix_approval_close_record", "business", "approval_record_id"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _iso(v) -> str:
    return v.isoformat() if isinstance(v, datetime.datetime) else ""


async def _latest(s, business: str, record_id: str):
    return (await s.execute(sa.select(approval_close.c.outcome, approval_close.c.at)
                            .where(approval_close.c.business == business,
                                   approval_close.c.approval_record_id == str(record_id))
                            .order_by(approval_close.c.close_id.desc()).limit(1))).first()


async def close(business: str, record_id: str, outcome: str, actor: str, *, now=None) -> str:
    """閉じる（追記）。戻り値 ok / already（既に閉じている）/ bad_input / db。"""
    if outcome not in CLOSED_OUTCOMES or not str(record_id).isdigit():
        return "bad_input"
    now = now or _now()
    try:
        async with session_scope() as s:
            cur = await _latest(s, business, record_id)
            if cur is not None and cur.outcome in CLOSED_OUTCOMES:
                return "already"
            await s.execute(sa.insert(approval_close).values(
                business=business, approval_record_id=str(record_id), outcome=outcome, actor=actor, at=now))
        logger.info("[APPROVAL_CLOSE] closed outcome=%s", emit(outcome, "freetext", "log", "operator"))
        return "ok"
    except Exception:
        logger.error("[APPROVAL_CLOSE] close failed (fixed reason)")
        return "db"


async def reopen(business: str, record_id: str, actor: str, *, now=None) -> str:
    """未処理に戻す（追記）。戻り値 ok / not_closed / db。"""
    now = now or _now()
    try:
        async with session_scope() as s:
            cur = await _latest(s, business, record_id)
            if cur is None or cur.outcome not in CLOSED_OUTCOMES:
                return "not_closed"
            await s.execute(sa.insert(approval_close).values(
                business=business, approval_record_id=str(record_id), outcome=OUTCOME_REOPENED, actor=actor, at=now))
        return "ok"
    except Exception:
        logger.error("[APPROVAL_CLOSE] reopen failed (fixed reason)")
        return "db"


async def latest_map(business: str) -> dict:
    """{approval_record_id: {"outcome","at"}}（最新行だけ）。DB 不可は例外（呼び出し側で扱う）。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(approval_close.c.approval_record_id, approval_close.c.outcome,
                                          approval_close.c.at)
                                .where(approval_close.c.business == business)
                                .order_by(approval_close.c.close_id.asc()))).fetchall()
    out: dict = {}
    for r in rows:
        out[str(r.approval_record_id)] = {"outcome": r.outcome, "at": _iso(r.at)}
    return out


async def closed_ids(business: str) -> set:
    return {rid for rid, v in (await latest_map(business)).items() if v["outcome"] in CLOSED_OUTCOMES}


async def list_closed(business: str, limit: int = 50) -> list[dict]:
    """閉じた件（最新行が閉じ）を新しい順に。"""
    m = await latest_map(business)
    rows = [{"approval_record_id": rid, "outcome": v["outcome"], "at": v["at"]}
            for rid, v in m.items() if v["outcome"] in CLOSED_OUTCOMES]
    rows.sort(key=lambda r: r["at"], reverse=True)
    return rows[: int(limit)]
