"""IngestionReceipt — file ingest（sortation 等）の durable 冪等/可視化/fencing 台帳（RV-05-13）

正本: docs/design-drafts/DRAFT_RV05_DURABLE_INBOUND.md（rev5）。
- 同期モデル（B-NEW-01）: 処理は GAS request 内で完結。receipt は処理キューではなく
  「冪等・可視化・fencing」の台帳。PDF bytes は保存しない。非同期 consumer は作らない。
- **不変条件（H-D4-01）**: 「epoch を進めない state 変更は存在しない」。state 遷移
  （claim/terminal/PENDING_RETRY/reconciliation/reset/duplicate_suspect）は全て
  `epoch=epoch+1` の単一 atomic UPDATE。heartbeat のみ非遷移（last_outcome 不変・fence 付き）。
- **状態正本（H-D4-02）**: `last_outcome` が唯一の権威 state。processing_attempt は監査専用。
- **時刻は DB clock 単一源（H-05）**: 全書込は SQL 側 `now()`（app プロセス時計を使わない）。
- state 値は **DB 値（小文字）が正本（L-01）**。DRAFT 本文の大文字表記は表示名。
"""

import hashlib
import logging
import os

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from hub.db import session_scope
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由

logger = logging.getLogger("hub.ingestion_receipt")

metadata = sa.MetaData()

ingestion_receipt = sa.Table(
    "ingestion_receipt", metadata,
    sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
              primary_key=True, autoincrement=True),
    sa.Column("source_file_id", sa.Text, nullable=False),
    sa.Column("source_sha256", sa.Text, nullable=False),
    sa.Column("ingest_type", sa.Text, nullable=False),
    sa.Column("caller_id", sa.Text, nullable=False),
    sa.Column("case_hint", sa.Text),
    sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("last_outcome", sa.Text, nullable=False),
    sa.Column("downstream_refs", sa.Text),
    sa.Column("idempotency_key", sa.Text, nullable=False, unique=True),
    sa.Column("epoch", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)),
)

processing_attempt = sa.Table(
    "processing_attempt", metadata,
    sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
              primary_key=True, autoincrement=True),
    sa.Column("receipt_id",
              sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
              sa.ForeignKey("ingestion_receipt.id", ondelete="CASCADE"), nullable=False),
    sa.Column("epoch", sa.Integer, nullable=False),
    sa.Column("attempted_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("phase", sa.Text, nullable=False),
    sa.Column("outcome", sa.Text),
    sa.UniqueConstraint("receipt_id", "epoch", name="uq_processing_attempt_receipt_epoch"),
)

# ── state 値（last_outcome の全域・小文字が DB 正本＝L-01） ─────────────────
ST_RECEIVED = "received"
ST_PROCESSING = "processing"
ST_VENDOR_PRE = "vendor_pre"
ST_SENDING = "sending"
ST_COMPLETED = "completed"
ST_PENDING_RETRY = "pending_retry"
ST_FAILED = "failed"
ST_UNKNOWN = "unknown"
ST_DUPLICATE_SUSPECT = "duplicate_suspect"

_CLAIMABLE = (ST_RECEIVED, ST_PENDING_RETRY)
_STALE_PRE_VENDOR = (ST_RECEIVED, ST_PROCESSING, ST_VENDOR_PRE)
TERMINAL = frozenset({ST_COMPLETED, ST_FAILED})
HELD = frozenset({ST_DUPLICATE_SUSPECT})


class ReceiptConflict(Exception):
    """duplicate_suspect: 同一 idempotency_key に既存行があり要素が食い違う（人手）。"""
    def __init__(self, receipt_id: int):
        self.receipt_id = receipt_id


def _now():
    """H-05: DB clock（SQL 側 now()）。app プロセス時計を使わない。"""
    return sa.func.now()


def _stale_cutoff(stale_seconds: int):
    """DB clock 基準の「now() - N 秒」を dialect 別に構成（H-05）。"""
    n = int(stale_seconds)
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("sqlite") or "sqlite" in url:
        return sa.func.datetime(sa.func.now(), f"-{n} seconds")
    return sa.func.now() - sa.text(f"interval '{n} seconds'")


def build_idempotency_key(*fields: str) -> str:
    """§2.4: length-prefix 連結の sha256。NULL/空要素は ValueError（呼び出し側で 5xx）。"""
    out = b""
    for f in fields:
        if f is None or f == "":
            raise ValueError("idempotency key element is NULL/empty")
        u = str(f).encode("utf-8")
        out += str(len(u)).encode("ascii") + b":" + u + b"\n"
    return hashlib.sha256(out).hexdigest()


def _idem_key(ingest_type, caller_id, source_file_id, source_sha256) -> str:
    return build_idempotency_key(
        "sortation" if ingest_type == "sortation" else ingest_type,
        caller_id, source_file_id, source_sha256)


async def upsert_receipt(*, ingest_type: str, caller_id: str, source_file_id: str,
                         source_sha256: str, case_hint: str | None) -> int:
    """初回受理で receipt(received) を作り id を返す。既存（同一 idempotency_key）なら:
      - source_sha256 と case_hint が一致 → 冪等（既存 id を返す）
      - 不一致 → duplicate_suspect（epoch++ の atomic UPDATE・M-02）→ ReceiptConflict
    冪等キー要素が NULL/空なら ValueError（呼び出し側 5xx・fail-close）。時刻は DB clock。"""
    key = _idem_key(ingest_type, caller_id, source_file_id, source_sha256)
    try:
        async with session_scope() as s:
            res = await s.execute(sa.insert(ingestion_receipt).values(
                source_file_id=source_file_id, source_sha256=source_sha256,
                ingest_type=ingest_type, caller_id=caller_id, case_hint=case_hint,
                first_seen_at=_now(), last_heartbeat_at=_now(),
                last_outcome=ST_RECEIVED, idempotency_key=key, epoch=0)
                .returning(ingestion_receipt.c.id))
            return res.scalar_one()
    except IntegrityError:
        pass  # UNIQUE(idempotency_key) 衝突 = 既存あり

    # M-02: pre-SELECT なしの atomic UPDATE。case_hint/source_sha256 の相違を guard に含める。
    conflict_id = None
    conflict_epoch = None
    async with session_scope() as s:
        res = await s.execute(
            sa.update(ingestion_receipt)
            .where(ingestion_receipt.c.idempotency_key == key,
                   sa.or_(ingestion_receipt.c.source_sha256 != source_sha256,
                          sa.func.coalesce(ingestion_receipt.c.case_hint, "") != (case_hint or "")))
            .values(epoch=ingestion_receipt.c.epoch + 1,
                    last_outcome=ST_DUPLICATE_SUSPECT, last_heartbeat_at=_now())
            .returning(ingestion_receipt.c.id, ingestion_receipt.c.epoch))
        row = res.first()
        if row is not None:
            conflict_id, conflict_epoch = row.id, row.epoch
            await s.execute(sa.insert(processing_attempt).values(
                receipt_id=conflict_id, epoch=conflict_epoch, attempted_at=_now(),
                phase=ST_DUPLICATE_SUSPECT))
        else:
            # 同一要素 → 冪等。id 取得（決定は UPDATE で既に済み・race なし）。
            dedup_id = (await s.execute(sa.select(ingestion_receipt.c.id)
                        .where(ingestion_receipt.c.idempotency_key == key))).scalar_one()
    if conflict_id is not None:
        logger.warning("ingestion_receipt duplicate_suspect id=%s",
                       emit(conflict_id, "record_id", "log", "operator"))
        raise ReceiptConflict(conflict_id)
    return dedup_id


async def _transition(receipt_id: int, new_state: str, *, guard_states,
                      my_epoch: int | None = None, extra_guard=None,
                      downstream_refs: str | None = None) -> int | None:
    """H-D4-01 統一パターン: epoch=epoch+1 の単一 atomic UPDATE（時刻は DB clock）。
    成功時は新 epoch を返し、同一 tx で processing_attempt に監査行を残す。guard 不成立は None。"""
    conds = [ingestion_receipt.c.id == receipt_id]
    if guard_states is not None:
        conds.append(ingestion_receipt.c.last_outcome.in_(guard_states))
    if my_epoch is not None:
        conds.append(ingestion_receipt.c.epoch == my_epoch)
    if extra_guard is not None:
        conds.append(extra_guard)
    values = {"epoch": ingestion_receipt.c.epoch + 1, "last_outcome": new_state,
              "last_heartbeat_at": _now()}
    if downstream_refs is not None:
        values["downstream_refs"] = downstream_refs
    async with session_scope() as s:
        res = await s.execute(
            sa.update(ingestion_receipt).where(sa.and_(*conds)).values(**values)
            .returning(ingestion_receipt.c.epoch))
        new_epoch = res.scalar_one_or_none()
        if new_epoch is None:
            return None
        await s.execute(sa.insert(processing_attempt).values(
            receipt_id=receipt_id, epoch=new_epoch, attempted_at=_now(), phase=new_state))
    return new_epoch


async def claim(receipt_id: int) -> int | None:
    """claim guard: last_outcome IN (received, pending_retry)。成功で my_epoch。
    None は claim できず（processing/terminal 等）＝並行敗者/二重処理回避。"""
    return await _transition(receipt_id, ST_PROCESSING, guard_states=_CLAIMABLE)


async def mark_phase(receipt_id: int, my_epoch: int, phase: str) -> int | None:
    """vendor_pre / sending の可視化遷移（fence: epoch=my_epoch）。None=fence 喪失（中断すべき）。"""
    assert phase in (ST_VENDOR_PRE, ST_SENDING)
    return await _transition(receipt_id, phase, guard_states=None, my_epoch=my_epoch)


async def heartbeat(receipt_id: int, my_epoch: int) -> bool:
    """非遷移（last_outcome 不変）・fence 付き。epoch を進めない唯一の書込（時刻は DB clock）。
    True=最新 / False=再claim された（stale・中断すべき）。"""
    async with session_scope() as s:
        res = await s.execute(
            sa.update(ingestion_receipt)
            .where(ingestion_receipt.c.id == receipt_id,
                   ingestion_receipt.c.epoch == my_epoch)
            .values(last_heartbeat_at=_now()))
        return res.rowcount == 1


async def mark_terminal(receipt_id: int, my_epoch: int, outcome: str,
                        downstream_refs: str | None = None) -> bool:
    """terminal 確定（fence: epoch=my_epoch）。False=stale→abort（処理中断すべき）。"""
    assert outcome in (ST_COMPLETED, ST_FAILED)
    ep = await _transition(receipt_id, outcome, guard_states=None, my_epoch=my_epoch,
                           downstream_refs=downstream_refs)
    return ep is not None


async def mark_pending_retry(receipt_id: int, my_epoch: int) -> bool:
    """downstream 保存失敗の可視化（fence）。GAS が 5xx で再送・再 claim できる。"""
    ep = await _transition(receipt_id, ST_PENDING_RETRY, guard_states=None, my_epoch=my_epoch)
    return ep is not None


async def manual_reset(receipt_id: int) -> bool:
    """人手 reset（unknown/duplicate_suspect → received・epoch++）。系列C: manual_reset。"""
    ep = await _transition(receipt_id, ST_RECEIVED,
                           guard_states=(ST_UNKNOWN, ST_DUPLICATE_SUSPECT))
    if ep is not None:
        logger.info("ingestion_receipt manual_reset id=%s",
                    emit(receipt_id, "record_id", "log", "operator"))
    return ep is not None


async def get_state(receipt_id: int) -> str | None:
    """receipt の現 last_outcome（H-02 応答マッピング用・claim 済み判定には使わない）。"""
    async with session_scope() as s:
        return (await s.execute(sa.select(ingestion_receipt.c.last_outcome)
                .where(ingestion_receipt.c.id == receipt_id))).scalar_one_or_none()


async def reconcile_stale(stale_seconds: int) -> dict:
    """startup/定期の可視化のみ（再処理しない・§4）。M-01: bulk UPDATE...RETURNING で
    epoch++（in-flight を無効化）し、同一 tx で監査行を INSERT。時刻は DB clock。"""
    cutoff = _stale_cutoff(stale_seconds)
    stale = sa.or_(ingestion_receipt.c.last_heartbeat_at.is_(None),
                   ingestion_receipt.c.last_heartbeat_at < cutoff)
    async with session_scope() as s:
        r1 = await s.execute(
            sa.update(ingestion_receipt)
            .where(ingestion_receipt.c.last_outcome.in_(_STALE_PRE_VENDOR), stale)
            .values(epoch=ingestion_receipt.c.epoch + 1, last_outcome=ST_PENDING_RETRY,
                    last_heartbeat_at=_now())
            .returning(ingestion_receipt.c.id, ingestion_receipt.c.epoch))
        pr = r1.all()
        for row in pr:
            await s.execute(sa.insert(processing_attempt).values(
                receipt_id=row.id, epoch=row.epoch, attempted_at=_now(),
                phase=ST_PENDING_RETRY))
        r2 = await s.execute(
            sa.update(ingestion_receipt)
            .where(ingestion_receipt.c.last_outcome == ST_SENDING, stale)
            .values(epoch=ingestion_receipt.c.epoch + 1, last_outcome=ST_UNKNOWN,
                    last_heartbeat_at=_now())
            .returning(ingestion_receipt.c.id, ingestion_receipt.c.epoch))
        un = r2.all()
        for row in un:
            await s.execute(sa.insert(processing_attempt).values(
                receipt_id=row.id, epoch=row.epoch, attempted_at=_now(),
                phase=ST_UNKNOWN))
    return {"to_pending_retry": len(pr), "to_unknown": len(un)}


async def convergence_stats() -> dict:
    """M-D5-01: 収束率を「DB 最新状態の distinct 集計」で算出（加算カウンタでない）。
    分子=terminal+held の distinct receipt・分母=distinct receipt 全体。二重計上しない。"""
    async with session_scope() as s:
        rows = (await s.execute(
            sa.select(ingestion_receipt.c.last_outcome,
                      sa.func.count(ingestion_receipt.c.id))
            .group_by(ingestion_receipt.c.last_outcome))).all()
    by_state = {state: n for state, n in rows}
    total = sum(by_state.values())
    converged = sum(n for st, n in by_state.items() if st in TERMINAL or st in HELD)
    return {"by_state": by_state, "distinct_total": total, "converged": converged,
            "convergence_rate": (converged / total) if total else None}
