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
# M-01(fix3): Vision(120×最大バッチ)＋Claude primary(1800)＋fallback(1800)＋backoff/前後処理の
# 最悪合計 ≈ 4500s（完全列挙は work-log §5）。fencing が最終防衛・lease は誤 stale 低減の値。
_DEFAULT_STALE_SECONDS = 4500
_LINE_MAX_ATTEMPTS_ENV = "INBOUND_LINE_MAX_ATTEMPTS"
_DEFAULT_LINE_MAX_ATTEMPTS = 5   # H-NEW-01: poison event の無限 re-attempt を止める上限
_LINE_STALE_PROC_ENV = "INBOUND_LINE_STALE_PROCESSING_SECONDS"
# H-NEW-01-R2: LINE processing が claim 後この秒数を超えて更新されなければ「クラッシュ滞留」と
# みなし、次の再配送で再 claim する（回収駆動は LINE 再配送のみ・専用 reconciliation は持たない）。
_DEFAULT_LINE_STALE_PROCESSING_SECONDS = 3600
# LINE Phase A の終端 state（ここへ到達済みは重複配送を skip＝二重返信を遮断・attempts 加算停止）
_LINE_TERMINAL = frozenset({"done", "failed_exhausted"})


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


def line_stale_processing_seconds() -> int:
    """LINE processing の stale 回収閾値（env INBOUND_LINE_STALE_PROCESSING_SECONDS・
    既定3600秒）。この秒数を超えた processing 行は再配送で再 claim される（H-NEW-01-R2）。"""
    raw = os.environ.get(_LINE_STALE_PROC_ENV, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return _DEFAULT_LINE_STALE_PROCESSING_SECONDS
    return v if v > 0 else _DEFAULT_LINE_STALE_PROCESSING_SECONDS


def _line_stale_cutoff(stale_seconds: int):
    """DB clock 基準の「now() - N 秒」を dialect 別に構成（H-05・ingestion_receipt と同流儀）。"""
    n = int(stale_seconds)
    url = os.environ.get("DATABASE_URL", "")
    if url.startswith("sqlite") or "sqlite" in url:
        return sa.func.datetime(sa.func.now(), f"-{n} seconds")
    return sa.func.now() - sa.text(f"interval '{n} seconds'")


def line_max_attempts() -> int:
    """LINE Phase A の再処理上限（env INBOUND_LINE_MAX_ATTEMPTS・既定5）。
    未終端の重複配送は上限内でのみ re-attempt する（H-NEW-01）。"""
    raw = os.environ.get(_LINE_MAX_ATTEMPTS_ENV, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return _DEFAULT_LINE_MAX_ATTEMPTS
    return v if v > 0 else _DEFAULT_LINE_MAX_ATTEMPTS


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
    冪等要素 NULL/空は ValueError（呼び出し側 5xx）。

    戻り値（呼び出し側は "duplicate" のみ処理登録を skip・他は登録）:
      "new"       — 初回 insert。
      "reattempt" — 重複配送だが**未終端**を**排他 claim**（state→processing・claimed_at=now()）
                    できた1者のみ。受理対象は received／failed、および **claim 後 stale 秒を超えた
                    processing（クラッシュ滞留の回収・H-NEW-01-R2）**。INSERT 後クラッシュ／部分
                    insert 失敗（他 event の 503）による永久滞留を断つため再処理を登録する。
      "duplicate" — terminal（done／failed_exhausted）到達済み、または processing（stale 秒内＝
                    実行中・claim 敗者含む）、または attempts 上限到達。登録を skip（二重返信を遮断）。
    **自動 replay はしない**（回収駆動は LINE 再配送のみ・専用 reconciliation は持たない）。"""
    if not webhook_event_id:
        raise ValueError("webhook_event_id is empty")
    dedup = line_dedup_key(webhook_event_id)
    # H-05: received_at は DB clock（now()）。既存 InboundEvent モデルの app-clock default を
    # 使わず Core insert で SQL 側 now() を明示（Stripe 経路の default は不変）。
    try:
        async with session_scope() as s:
            await s.execute(sa.insert(InboundEvent.__table__).values(
                provider="line", external_event_id=webhook_event_id, caller_id=user_id,
                dedup_key=dedup, payload_hash=payload_sha256(payload),
                event_type=event_type, signature_result=signature_result,
                state="received", received_at=sa.func.now(), attempts=1))
        count("A", "received", webhook_event_id)
        return "new"
    except IntegrityError:
        pass  # dedup_key 衝突＝重複配送。以降で **DB 最新 state** に応じて分岐

    _max = line_max_attempts()
    # H-NEW-01-R2: 未終端の claim 対象 = received／failed、または stale 秒を超えた processing
    # （claimed_at NULL の旧行も回収対象）。claim 成功時のみ claimed_at=now() を更新。
    stale_cutoff = _line_stale_cutoff(line_stale_processing_seconds())
    claimable = sa.or_(
        InboundEvent.state.in_(("received", "failed")),
        sa.and_(InboundEvent.state == "processing",
                sa.or_(InboundEvent.claimed_at.is_(None),
                       InboundEvent.claimed_at < stale_cutoff)))
    async with session_scope() as s:
        # 排他 claim（state→processing・claimed_at=now()）。target=processing のため、同時 2 配送が
        # 両方 guard へ来ても勝者は 1 者だけ（敗者は state/claimed_at が動き guard 不成立＝rowcount 0）。
        # M-02-R: attempts の加算はこの claim 成功（rowcount 1）時のみ。
        claimed = await s.execute(
            sa.update(InboundEvent)
            .where(InboundEvent.dedup_key == dedup, claimable,
                   InboundEvent.attempts < _max)
            .values(state="processing", attempts=InboundEvent.attempts + 1,
                    claimed_at=sa.func.now())
            .returning(InboundEvent.id))
        if claimed.scalar_one_or_none() is not None:
            outcome, series_name = "reattempt", "reattempt"
        else:
            # M-02: 上限到達（claim 可能状態だが attempts>=max）→ failed_exhausted terminal
            # （理由付き）。以後の重複再送では guard 不成立となり skip・attempts も動かない。
            exhausted = await s.execute(
                sa.update(InboundEvent)
                .where(InboundEvent.dedup_key == dedup, claimable,
                       InboundEvent.attempts >= _max)
                .values(state="failed_exhausted", processed_at=sa.func.now(),
                        last_error="attempts_exhausted")
                .returning(InboundEvent.id))
            if exhausted.scalar_one_or_none() is not None:
                outcome, series_name = "duplicate", "failed_exhausted"
            else:
                # terminal（done／failed_exhausted）／stale 秒内の processing（実行中・claim 敗者）
                # → skip。**attempts は加算しない**（M-02-R: 加算は claim 成功時のみ）。
                outcome, series_name = "duplicate", "dedup_skip"
    count("B" if series_name == "failed_exhausted" else "A", series_name, webhook_event_id)
    return outcome


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
