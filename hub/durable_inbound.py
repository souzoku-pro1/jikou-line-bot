"""durable_inbound — RV-05-13 の flag・観測性（3系列カウンタ）・LINE Phase A 記録

正本: docs/design-drafts/DRAFT_RV05_DURABLE_INBOUND.md（rev5）。
- flag `INBOUND_EVENT_DURABLE_ENABLED`（既定 OFF＝現行挙動と byte 同一）。
- 観測性は emit 契約経由（PII/本文/payload 非混入）・ログ集計。3系列（A受理/B終端held/C運用）。
- LINE Phase A: inbound_event へ provider="line" で durable 記録（記録＋観測のみ・自動 replay なし）。
"""

import hashlib
import logging
import os

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from hub.db import session_scope
from hub.inbound_event import InboundEvent, payload_sha256
from hub.ingestion_receipt import build_idempotency_key
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由

logger = logging.getLogger("hub.durable_inbound")

_FLAG = "INBOUND_EVENT_DURABLE_ENABLED"
_FLAG_TRUE = frozenset({"1", "true", "on", "yes"})
_STALE_ENV = "INBOUND_RECONCILE_STALE_SECONDS"
_DEFAULT_STALE_SECONDS = 600   # M-D5-02: 外部 call 最大時間より十分長い（根拠は work-log）


def durable_enabled() -> bool:
    """RV-05-13 feature flag。既定 OFF（未設定/0）＝現行 BackgroundTasks/process-memory 挙動。"""
    return os.environ.get(_FLAG, "").strip().lower() in _FLAG_TRUE


def reconcile_stale_seconds() -> int:
    raw = os.environ.get(_STALE_ENV, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return _DEFAULT_STALE_SECONDS
    return v if v > 0 else _DEFAULT_STALE_SECONDS


# ── 観測性（3系列カウンタ・emit 契約・§6） ───────────────────────────────────
# 系列A 受理 / 系列B 終端held / 系列C 運用。lifecycle は event_id（record_id）のみ可視。

def count(series: str, name: str, event_id: str | int = "") -> None:
    """emit 契約でカウント/lifecycle をログ（PII/本文なし）。series=A/B/C・name=遷移名。"""
    logger.info("durable-inbound counter series=%s name=%s id=%s",
                emit(series, "record_id", "log", "operator"),
                emit(name, "record_id", "log", "operator"),
                emit(event_id, "record_id", "log", "operator"))


# ── LINE Phase A: durable 記録（記録＋観測のみ・§3.1） ───────────────────────

def line_dedup_key(webhook_event_id: str) -> str:
    """§2.4: length-prefix 連結の sha256。表示用 prefix 付き。"""
    return "line:" + build_idempotency_key("line", webhook_event_id)


async def record_line_event(*, webhook_event_id: str, user_id: str,
                            signature_result: str, payload: bytes,
                            event_type: str | None) -> str:
    """§3.1: 1 event=1 InboundEvent durable insert（provider=line・独立 tx）。
    冪等要素 NULL/空は ValueError（呼び出し側 5xx）。UNIQUE(dedup_key) 衝突=冪等 skip。
    戻り値: "new"（初回）/ "duplicate"（重複配送）。**処理は既存 BackgroundTasks・自動 replay なし**。"""
    if not webhook_event_id:
        raise ValueError("webhook_event_id is empty")
    dedup = line_dedup_key(webhook_event_id)
    row = InboundEvent(
        provider="line", external_event_id=webhook_event_id, caller_id=user_id,
        dedup_key=dedup, payload_hash=payload_sha256(payload),
        event_type=event_type, signature_result=signature_result,
        state="received")
    try:
        async with session_scope() as s:
            s.add(row)
            await s.flush()
        count("A", "received", webhook_event_id)
        return "new"
    except IntegrityError:
        count("A", "dedup_skip", webhook_event_id)
        return "duplicate"


async def mark_line_processing(webhook_event_id: str) -> None:
    """coarse observe: 処理開始（received→processing）。HOTFIX-01 型の「processing に到達しない」
    滞留を可視化する。fence 不要（Phase A は競合 consumer なし・§H-01）。"""
    async with session_scope() as s:
        await s.execute(sa.update(InboundEvent)
                        .where(InboundEvent.dedup_key == line_dedup_key(webhook_event_id),
                               InboundEvent.state == "received")
                        .values(state="processing"))
    count("B", "processing", webhook_event_id)


async def mark_line_completed(webhook_event_id: str) -> None:
    """coarse observe: 背景処理が例外送出せず戻った（processing→completed）。
    ※ _process_line_event 本体は内部で例外を握るため、reply_fail/no_reply_intended の
      細分は本 Phase A では区別しない（本体変更が要るため OPEN・work-log 参照）。"""
    async with session_scope() as s:
        await s.execute(sa.update(InboundEvent)
                        .where(InboundEvent.dedup_key == line_dedup_key(webhook_event_id),
                               InboundEvent.state == "processing")
                        .values(state="done"))
    count("B", "completed", webhook_event_id)


async def mark_line_failed(webhook_event_id: str, error_class: str) -> None:
    """coarse observe: 背景処理が（内部 try の外で）例外送出＝HOTFIX-01 型の全滅を可視化。"""
    async with session_scope() as s:
        await s.execute(sa.update(InboundEvent)
                        .where(InboundEvent.dedup_key == line_dedup_key(webhook_event_id))
                        .values(state="failed", last_error=(error_class or "unknown")[:100]))
    count("B", "failed", webhook_event_id)
