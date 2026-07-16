"""kintone_lane — RV-04c S3: kintone webhook の id 冪等・state 遷移・XFF 観測・stale 監視補助。

設計正本: DRAFT_RV04C_CALLER_MIGRATION.md rev D5（§4.2・§4.2b・§4.1）。
- flag `KINTONE_EVENT_DEDUP_ENABLED`（既定 OFF＝現行挙動と byte 同一・env 直読みゲート）。
- inbound_event 同居（provider="kintone"・**ALTER 0**）。state は received/sending/done/failed の
  4 値（sending=送信着手 marker）。再配送は存在しないため再 claim 経路なし。
- DB 到達不能は fail-closed（H04・呼び出し側 5xx）。marker 成功（rowcount=1）が送信の前提（D3-H01）。
"""

import logging
import os

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError

from hub.db import session_scope
from hub.inbound_event import InboundEvent, payload_sha256
from hub.ingestion_receipt import build_idempotency_key
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式・module top-level）

logger = logging.getLogger("hub.kintone_lane")

_FLAG = "KINTONE_EVENT_DEDUP_ENABLED"
_FLAG_TRUE = frozenset({"1", "true", "on", "yes"})
_STALE_ENV = "KINTONE_STALE_EVENT_HOURS"
_DEFAULT_STALE_HOURS = 1

# §4.2 D4-M01: 正常 no-op の done terminal 理由コード（enum 固定・自由文字列禁止）。
# 既存 handler の skip 分岐と 1:1（値は last_error 列へ「terminal 理由コード」として流用）。
NOOP_REASONS = frozenset({
    "skip_not_approved",       # ステータス2!=承認済 or 送信済み!=no（webhook body / refetch）
    "skip_already_sent",       # refetch で既に送信済み=yes
    "skip_record_not_found",   # refetch でレコード不在
    "skip_missing_fields",     # webhook body の必須 field 欠落
    "skip_missing_user_or_draft",  # user_id/AI下書き 欠落
})

_PROVIDER = "kintone"

# H02残: kintone の「record 不存在」を示す既知 vendor code（no-op done を許すのはこれのみ）。
# 404 でも app/endpoint/設定起因の 404（未知 code・code 欠落・非 JSON＝code 空）は含めない。
# 出典: kintone REST API—GAIA_RE01 = "The specified record is not found."（HTTP 404）。
RECORD_NOT_FOUND_CODES = frozenset({"GAIA_RE01"})


def is_record_not_found(status: int, code: str) -> bool:
    """H02残: record 不存在の確定条件＝HTTP 404 **かつ** 既知 record-not-found code。
    404×未知 code・code 欠落（非 JSON 含む）は False（＝呼び出し側は failed_preflight）。"""
    return status == 404 and (code or "") in RECORD_NOT_FOUND_CODES


class KintoneLaneStateError(RuntimeError):
    """M02: terminal/marker UPDATE の rowcount!=1（行消失・想定外 state）。fail-closed。"""


def dedup_enabled() -> bool:
    return os.environ.get(_FLAG, "").strip().lower() in _FLAG_TRUE


def stale_hours() -> int:
    raw = os.environ.get(_STALE_ENV, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return _DEFAULT_STALE_HOURS
    return v if v > 0 else _DEFAULT_STALE_HOURS


def extract_event_id(body) -> str | None:
    """kintone webhook body の top-level `id`（通知ごとに一意・K1 確定）。
    H01: **scalar（str/int）かつ非空**のみ受理。欠落・空・型不正（dict/list/bool 等）は
    None（呼び出し側は claim せず 400 系で拒否＝LINE write 0）。"""
    if not isinstance(body, dict):
        return None
    v = body.get("id")
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, str):
        return v if v.strip() else None
    return None   # dict/list/float 等の型不正


def dedup_key(event_id: str) -> str:
    return "kintone:" + build_idempotency_key("kintone", event_id)


def observe_pre_claim_reject() -> None:
    """H01: id 欠落/空/型不正で claim 前に拒否した件を固定 reason で観測する。
    拒否イベントは行が残らず滞留監視の対象外のため、ここで別計数する（固定文言・emit 不要）。"""
    logger.warning("kintone webhook rejected pre-claim: invalid_or_missing_id")


async def claim_event(*, event_id: str, caller_id: str, event_type: str | None,
                      payload: bytes) -> str:
    """§4.2: INSERT UNIQUE 勝者が claim。戻り値:
      "new"       — 初回 insert（state=received・呼び出し側が処理続行）。
      "duplicate" — dedup_key 衝突（重複配信・skip・処理登録しない）。
    DB 到達不能は例外送出（H04 fail-closed・呼び出し側 5xx）。"""
    dk = dedup_key(event_id)
    try:
        async with session_scope() as s:
            await s.execute(sa.insert(InboundEvent.__table__).values(
                provider=_PROVIDER, external_event_id=event_id, caller_id=caller_id,
                dedup_key=dk, payload_hash=payload_sha256(payload),
                event_type=event_type, signature_result="token",
                state="received", received_at=sa.func.now(), attempts=1))
        return "new"
    except IntegrityError:
        # M01: IntegrityError を無条件に duplicate としない。同一 dedup_key 行の実在を再照合し、
        # 存在＝真の重複配信（duplicate）・不在＝別制約違反等の異常として再送出（fail-closed）。
        async with session_scope() as s:
            exists = (await s.execute(sa.select(InboundEvent.id).where(
                InboundEvent.dedup_key == dk))).first()
        if exists:
            return "duplicate"
        raise


async def _event_pk(event_id: str) -> int | None:
    async with session_scope() as s:
        row = (await s.execute(sa.select(InboundEvent.id).where(
            InboundEvent.dedup_key == dedup_key(event_id)))).first()
        return row[0] if row else None


async def mark_noop_done(event_id: str, reason: str) -> None:
    """§4.2 D4-M01: 正常 no-op を done terminal 化（LINE write 0・理由コードは enum 固定）。"""
    if reason not in NOOP_REASONS:
        raise ValueError(f"noop reason not in enum: {reason}")
    async with session_scope() as s:
        r = await s.execute(sa.update(InboundEvent)
                            .where(InboundEvent.dedup_key == dedup_key(event_id),
                                   InboundEvent.state == "received")
                            .values(state="done", processed_at=sa.func.now(),
                                    last_error=reason))
        if r.rowcount != 1:   # M02: 行消失・想定外 state は fail-closed
            raise KintoneLaneStateError("mark_noop_done rowcount!=1")


async def mark_sending(event_id: str) -> bool:
    """§4.2 D3-H01: 送信着手 marker（received→sending）。rowcount=1（成功）が LINE 送信の
    前提条件。戻り値 True のときのみ呼び出し側は LINE 送信してよい。False は LINE write 0。"""
    async with session_scope() as s:
        r = await s.execute(sa.update(InboundEvent)
                            .where(InboundEvent.dedup_key == dedup_key(event_id),
                                   InboundEvent.state == "received")
                            .values(state="sending"))
        return r.rowcount == 1


async def mark_done(event_id: str) -> None:
    """全副作用完了後の terminal（sending→done・last_error=NULL＝送信完了の印）。"""
    async with session_scope() as s:
        r = await s.execute(sa.update(InboundEvent)
                            .where(InboundEvent.dedup_key == dedup_key(event_id),
                                   InboundEvent.state == "sending")
                            .values(state="done", processed_at=sa.func.now(),
                                    last_error=None))
        if r.rowcount != 1:   # M02
            raise KintoneLaneStateError("mark_done rowcount!=1")


async def mark_failed_preflight(event_id: str, error_class: str) -> None:
    """§4.2 phase 表: marker **前**の確定失敗のみ failed（未送信確定）。
    marker 後は呼ばない（sending 維持・failed 上書き禁止）。error_class は分類コード
    （H02 の get_record_error 等・M05 の failed 分類監視で参照）。"""
    async with session_scope() as s:
        r = await s.execute(sa.update(InboundEvent)
                            .where(InboundEvent.dedup_key == dedup_key(event_id),
                                   InboundEvent.state == "received")
                            .values(state="failed", processed_at=sa.func.now(),
                                    last_error=(error_class or "unknown")[:100]))
        if r.rowcount != 1:   # M02
            raise KintoneLaneStateError("mark_failed_preflight rowcount!=1")


# ── §4.1 XFF 観測モード（observe-only・reject しない） ───────────────────────
_XFF_ENV = "KINTONE_XFF_OBSERVE_ENABLED"
# K2 素材: cybozu.com アウトバウンド公開帯（webhook 専用ではない・帯変更は告知のみ）
_KINTONE_CIDR = "103.79.14.0/24"


def xff_observe_enabled() -> bool:
    return os.environ.get(_XFF_ENV, "").strip().lower() in _FLAG_TRUE


def _rightmost_hop(xff_header: str) -> str | None:
    """XFF の最後の hop（Railway edge が最後に append した実 peer 相当）。
    先頭はクライアント注入可のため信頼しない（§4.1 の限界評価どおり）。"""
    if not xff_header:
        return None
    parts = [p.strip() for p in xff_header.split(",") if p.strip()]
    return parts[-1] if parts else None


def observe_xff(xff_header: str, cidr: str = _KINTONE_CIDR) -> bool:
    """observe-only: rightmost hop が cidr 内かを返す（reject しない）。不一致は warning ログ
    （IP は external_ref 扱いで emit・値の生表示はしない）。判定不能（ヘッダ無し等）は True
    扱い（観測モードでは遮断しない）。"""
    if not xff_observe_enabled():
        return True
    import ipaddress
    hop = _rightmost_hop(xff_header)
    if hop is None:
        return True
    try:
        ok = ipaddress.ip_address(hop) in ipaddress.ip_network(cidr)
    except ValueError:
        ok = False
    if not ok:
        logger.warning("kintone webhook XFF rightmost hop outside allowed CIDR: %s",
                       emit(hop, "external_ref", "log", "operator"))
    return ok
