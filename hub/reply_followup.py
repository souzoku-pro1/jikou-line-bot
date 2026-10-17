"""reply_followup — 大野の明示送信の後処理（App 28 human 行）の回収表（JIKOU-REPLY-Q1b-fix1 BR-08・fix2 BR-11／BR-12）

正本: 時効LINEボット_返信規則_v1.4.md §10-1「App 28 への記録失敗は対応中を失わせず、記録は回収ジョブ（Q1b）で回収する」。
Codex R-JIKOU-REPLY-Q1b-1 BR-08・R-Q1b-fix1 BR-11（App 28 二重作成）・BR-12（回収情報の欠落）。docs/jikou_reply_q1b_contract.md。

- 表 reply_chatlog_followup = 送信操作記録（send_operation）の **付帯情報**（op_id を主キー）。fix2 で「送信前に prepared として永続化」
  へ広げた（本文ハッシュ・宛先ハッシュ・op_id・後処理状態を送信の前に置く＝プロセス停止や mark_pending 失敗で情報が消えない）。
- 状態（閉集合）: prepared（送信前に置いた）→ pending（送信済み・App 28 未記録）／unconfigured（App 28 未設定・設定後に再試行）
  → in_progress（回収中・owner_token＋deadline_at で所有）→ done。closed（台帳が failed／台帳なし＝未送信で終端）。exhausted
  （failed が MAX_ATTEMPTS 回）。done／closed／exhausted は終端（行は消さない＝RV-08）。
- BR-11 所有権: App 28 書込の前に **条件付き UPDATE で in_progress を取る**（owner_token・deadline_at・send_ledger の試行所有と
  同じ流儀）。取れなければ何もしない。deadline を過ぎた in_progress は再取得できる（回収対象）。
- BR-11 照合: App 28 の本文欄にマーカー `[op:{op_id}]`（BR-07 のマーカー方式の流用・新欄なし）を含めて書く。作成の前に op_id で
  App 28 を検索し、既存があれば作成せず done（成功応答喪失の修復）。
- BR-12 突合: 回収ジョブは prepared／pending／unconfigured／期限切れ in_progress を send_ledger の状態と突合する——
  台帳 sent → 記録（未記録なら）、台帳 unconfirmed／started／pending → 人の確定（confirm_by_human）を待つ、台帳 failed または
  台帳行なし → closed（未送信）。
- 自動再送はしない（LINE には触れない）。role 選択肢なし（skipped）・未設定（unconfigured）は試行回数に数えない。
- 同じ操作 ID の再 POST（画面）は「送信済み・記録のみ再試行」に分岐し、process_one() を直接呼ぶ（所有権は同じ規則）。
- RV-10: ログは固定語彙＋件数のみ。本文は App 28 に書くまで保持し、done で NULL にする。
"""

import datetime
import logging
import re
import uuid

import sqlalchemy as sa

from hub import db
from hub.db import DatabaseNotConfigured, session_scope
from hub.redact import emit

logger = logging.getLogger("hub.reply_followup")

metadata = sa.MetaData()
TABLE_NAME = "reply_chatlog_followup"
KIND_CHATLOG_HUMAN = "chatlog_human"
STATE_PREPARED = "prepared"
STATE_PENDING = "pending"
STATE_UNCONFIGURED = "unconfigured"
STATE_IN_PROGRESS = "in_progress"
STATE_DONE = "done"
STATE_CLOSED = "closed"
STATE_EXHAUSTED = "exhausted"
STATES = (STATE_PREPARED, STATE_PENDING, STATE_UNCONFIGURED, STATE_IN_PROGRESS, STATE_DONE, STATE_CLOSED, STATE_EXHAUSTED)
OPEN_STATES = (STATE_PREPARED, STATE_PENDING, STATE_UNCONFIGURED, STATE_IN_PROGRESS)
MAX_ATTEMPTS = 100
BATCH_LIMIT = 20
CLAIM_SECONDS = 300
LEDGER_GRACE_SECONDS = 600            # fix3: prepared 行は台帳行が現れるまで（prepare→begin の窓）この秒数は閉じない
# process_one の戻り値（閉集合）
RESULT_WRITTEN = "written"
RESULT_RECOVERED = "recovered"          # App 28 に既に同じ op_id の行があった（作成せず done）
RESULT_SKIPPED = "skipped"
RESULT_FAILED = "failed"
RESULT_UNCONFIGURED = "unconfigured"
RESULT_NOT_PENDING = "not_pending"      # 終端済み、または所有権を取れなかった
RESULT_NOT_FOUND = "not_found"
RESULT_DESTINATION_MISMATCH = "destination_mismatch"
RESULT_EXHAUSTED = "exhausted"
RESULT_WAITING_LEDGER = "waiting_ledger"   # 台帳が未確定（人の確定待ち）
RESULT_CLOSED = "closed"                   # 台帳 failed／なし＝未送信
# 台帳の状態語彙（hub/send_ledger と同じ・循環 import を避けるため文字列で持つ）
_LEDGER_SENT = "sent"
_LEDGER_FAILED = "failed"
OP_MARKER_PREFIX = "[op:"
OP_MARKER_RE = re.compile(r"\n?\[op:[0-9a-f]{32}\]\s*$")

reply_chatlog_followup = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("op_id", sa.Text, primary_key=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("approval_record_id", sa.Text, nullable=False),
    sa.Column("line_user_hash", sa.Text, nullable=False),
    sa.Column("text", sa.Text, nullable=True),
    sa.Column("text_version", sa.Text, nullable=True),           # fix2 BR-12: 承認された本文の sha256（台帳と同じ）
    sa.Column("state", sa.Text, nullable=False),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("last_result", sa.Text, nullable=True),
    sa.Column("owner_token", sa.Text, nullable=True),            # fix2 BR-11: 回収の所有権
    sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("chatlog_record_id", sa.Text, nullable=True),      # fix2 BR-11: 照合で見つけた／作成した App 28 レコード No.
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state IN ('prepared', 'pending', 'unconfigured', 'in_progress', 'done', 'closed', 'exhausted')",
                       name="ck_reply_followup_state"),
    sa.Index("ix_reply_followup_state", "state", "created_at"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _aware(v):
    if v is not None and v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return v


def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


# ── マーカー（BR-11 照合用・BR-07 の方式を流用） ──────────────────────────────────
def op_marker(op_id: str) -> str:
    return f"{OP_MARKER_PREFIX}{op_id}]"


def with_op_marker(text: str, op_id: str) -> str:
    return f"{text}\n{op_marker(op_id)}"


def strip_op_marker(text: str) -> str:
    return OP_MARKER_RE.sub("", str(text or ""))


# ── 書込（状態遷移はすべて条件付き UPDATE） ──────────────────────────────────────
async def prepare(op_id: str, *, business: str, approval_record_id: str, line_user_hash: str, text: str,
                  text_version: str | None) -> str:
    """BR-12: 送信の **前** に prepared を置く。戻り値: prepared / exists / skipped（DB なし）/ failed（固定語彙ログ）。"""
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
                text=text, text_version=text_version, state=STATE_PREPARED, attempts=0, last_result=None,
                created_at=now, updated_at=now))
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] prepare failed (fixed reason)")
        return "failed"
    return "prepared"


async def _set(op_id: str, where_states, **values) -> bool:
    """条件付き UPDATE（state が where_states のいずれかのときだけ）。戻り値=更新したか。"""
    states = (where_states,) if isinstance(where_states, str) else tuple(where_states)
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_chatlog_followup).where(
            reply_chatlog_followup.c.op_id == str(op_id),
            reply_chatlog_followup.c.state.in_(list(states))).values(updated_at=_now(), **values))
        return int(r.rowcount or 0) == 1


async def mark_pending(op_id: str, *, last_result: str, business: str = "", approval_record_id: str = "",
                       line_user_hash: str = "", text: str = "") -> str:
    """送信成功後の App 28 記録失敗を記録未了として置く（prepared → pending／unconfigured）。
    prepared 行が無い（旧経路・prepare 失敗）ときは insert で補う。戻り値: pending / unconfigured / exists / skipped / failed。"""
    if not _db_configured():
        return "skipped"
    to_state = STATE_UNCONFIGURED if last_result == RESULT_UNCONFIGURED else STATE_PENDING
    try:
        if await _set(op_id, STATE_PREPARED, state=to_state, last_result=str(last_result)):
            return to_state
        async with session_scope() as s:
            row = (await s.execute(sa.select(reply_chatlog_followup.c.state).where(
                reply_chatlog_followup.c.op_id == str(op_id)))).first()
            if row is not None:
                return "exists"
            now = _now()
            await s.execute(sa.insert(reply_chatlog_followup).values(
                op_id=str(op_id), business=business, kind=KIND_CHATLOG_HUMAN,
                approval_record_id=str(approval_record_id), line_user_hash=line_user_hash,
                text=text, text_version=None, state=to_state, attempts=0, last_result=str(last_result),
                created_at=now, updated_at=now))
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] mark pending failed (fixed reason)")
        return "failed"
    return to_state


async def complete(op_id: str, *, chatlog_record_id: str | None = None, last_result: str = RESULT_WRITTEN) -> bool:
    """App 28 に書けた（または既存行を照合した）→ done（本文は保持しない）。終端以外からのみ。"""
    try:
        return await _set(op_id, OPEN_STATES, state=STATE_DONE, text=None, done_at=_now(), last_result=last_result,
                          owner_token=None, deadline_at=None,
                          chatlog_record_id=(str(chatlog_record_id) if chatlog_record_id else None))
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] complete failed (fixed reason)")
        return False


async def close(op_id: str, reason: str) -> bool:
    """未送信で終端（台帳 failed／台帳なし／許可表で停止 等）。本文は保持しない。"""
    try:
        return await _set(op_id, OPEN_STATES, state=STATE_CLOSED, text=None, last_result=str(reason),
                          owner_token=None, deadline_at=None, done_at=_now())
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] close failed (fixed reason)")
        return False


async def get_followup(op_id: str) -> dict | None:
    if not _db_configured():
        return None
    async with session_scope() as s:
        row = (await s.execute(sa.select(reply_chatlog_followup).where(
            reply_chatlog_followup.c.op_id == str(op_id)))).first()
    return dict(row._mapping) if row is not None else None


async def open_ops(limit: int = BATCH_LIMIT, *, now=None) -> list[str]:
    """回収対象: prepared／pending／unconfigured、および deadline を過ぎた in_progress。"""
    now = now or _now()
    t = reply_chatlog_followup
    async with session_scope() as s:
        rows = (await s.execute(sa.select(t.c.op_id, t.c.state, t.c.deadline_at).where(
            t.c.state.in_(list(OPEN_STATES))).order_by(t.c.created_at.asc()).limit(int(limit) * 2))).fetchall()
    out = []
    for r in rows:
        if r.state == STATE_IN_PROGRESS:
            d = _aware(r.deadline_at)
            if d is not None and d > now:
                continue
        out.append(r.op_id)
        if len(out) >= int(limit):
            break
    return out


async def pending_ops(limit: int = BATCH_LIMIT) -> list[str]:
    """fix1 互換の名前（回収対象の op_id 一覧）。"""
    return await open_ops(limit)


async def claim(op_id: str, *, now=None) -> str | None:
    """BR-11: 回収の所有権を取る（prepared／pending／unconfigured、または期限切れ in_progress → in_progress）。
    戻り値は owner_token（取れなければ None）。"""
    now = now or _now()
    token = uuid.uuid4().hex
    t = reply_chatlog_followup
    async with session_scope() as s:
        r = await s.execute(sa.update(t).where(
            t.c.op_id == str(op_id),
            sa.or_(t.c.state.in_([STATE_PREPARED, STATE_PENDING, STATE_UNCONFIGURED]),
                   sa.and_(t.c.state == STATE_IN_PROGRESS,
                           sa.or_(t.c.deadline_at.is_(None), t.c.deadline_at < now)))).values(
            state=STATE_IN_PROGRESS, owner_token=token, deadline_at=now + datetime.timedelta(seconds=CLAIM_SECONDS),
            updated_at=now))
        return token if int(r.rowcount or 0) == 1 else None


async def _release(op_id: str, token: str, to_state: str, **values) -> bool:
    """所有している行だけを to_state へ戻す（所有権を失っていれば何もしない）。"""
    t = reply_chatlog_followup
    async with session_scope() as s:
        r = await s.execute(sa.update(t).where(t.c.op_id == str(op_id), t.c.state == STATE_IN_PROGRESS,
                                                t.c.owner_token == token).values(
            state=to_state, owner_token=None, deadline_at=None, updated_at=_now(), **values))
        return int(r.rowcount or 0) == 1


async def _finish_done(op_id: str, token: str, *, record_id: str | None, last_result: str) -> bool:
    return await _release(op_id, token, STATE_DONE, text=None, done_at=_now(), last_result=last_result,
                          chatlog_record_id=(str(record_id) if record_id else None))


async def process_one(op_id: str, *, resolve_user_id, write_chatlog, find_chatlog=None, ledger_state=None,
                      now=None) -> str:
    """1 件を処理する（冪等・所有権つき）。
    resolve_user_id(approval_record_id) -> line_user_id|None（App 29 から再解決）、
    write_chatlog(user_id, text_with_marker) -> written / skipped / failed / unconfigured（または (結果, record_id)）、
    find_chatlog(user_id, op_id) -> record_id|None（App 28 を op_id マーカーで検索）、
    ledger_state(op_id) -> send_ledger の状態（None=台帳行なし）。戻り値は閉集合（RESULT_*）。"""
    row = await get_followup(op_id)
    if row is None:
        return RESULT_NOT_FOUND
    if row["state"] not in OPEN_STATES:
        return RESULT_NOT_PENDING
    # BR-12: 台帳との突合（送信が確定していない行は記録しない・未送信は閉じる）
    if ledger_state is not None:
        try:
            state = await ledger_state(op_id)
        except Exception:
            return RESULT_WAITING_LEDGER
        if state is None:
            # fix3（PRE-REVIEW-1 #272 低）: prepared は送信の **前** に置かれる。台帳行が無いのは「まだ begin していない」
            # 可能性があるので、作成から LEDGER_GRACE_SECONDS の間は閉じずに待つ（prepare→begin の ms 窓で回収ジョブが
            # 先に not_sent で閉じ、その後の記録未了が回収対象外になる穴を塞ぐ）
            created = _aware(row.get("created_at"))
            if created is not None and (now or _now()) - created < datetime.timedelta(seconds=LEDGER_GRACE_SECONDS):
                return RESULT_WAITING_LEDGER
            await close(op_id, "not_sent")
            return RESULT_CLOSED
        if state == _LEDGER_FAILED:
            await close(op_id, "ledger_failed")
            return RESULT_CLOSED
        if state != _LEDGER_SENT:
            return RESULT_WAITING_LEDGER
    token = await claim(op_id, now=now)
    if token is None:
        return RESULT_NOT_PENDING
    from hub.send_ledger import line_user_hash
    try:
        user_id = await resolve_user_id(row["approval_record_id"])
    except Exception:
        user_id = None
    if not user_id or line_user_hash(row["business"], user_id) != row["line_user_hash"]:
        await _release(op_id, token, STATE_PENDING, last_result=RESULT_DESTINATION_MISMATCH)
        return RESULT_DESTINATION_MISMATCH
    # BR-11: 作成前に op_id で照合（成功応答喪失の修復）
    if find_chatlog is not None:
        try:
            existing = await find_chatlog(user_id, str(op_id))
        except Exception:
            existing = None
        if existing:
            await _finish_done(op_id, token, record_id=str(existing), last_result=RESULT_RECOVERED)
            return RESULT_RECOVERED
    result = await write_chatlog(user_id, with_op_marker(row["text"] or "", str(op_id)))
    record_id = None
    if isinstance(result, tuple):
        result, record_id = result[0], result[1]
    if result == RESULT_WRITTEN:
        if await _finish_done(op_id, token, record_id=record_id, last_result=RESULT_WRITTEN):
            return RESULT_WRITTEN
        return RESULT_NOT_PENDING
    if result == RESULT_FAILED:
        attempts = int(row["attempts"]) + 1
        if attempts >= MAX_ATTEMPTS:
            # fix3: 終端なので本文は保持しない（done／closed と同じ）
            await _release(op_id, token, STATE_EXHAUSTED, attempts=attempts, last_result=RESULT_FAILED, text=None)
            logger.warning("[REPLY_FOLLOWUP] chatlog retry exhausted (max attempts)")
            return RESULT_EXHAUSTED
        await _release(op_id, token, STATE_PENDING, attempts=attempts, last_result=RESULT_FAILED)
        return RESULT_FAILED
    if result == RESULT_UNCONFIGURED:
        await _release(op_id, token, STATE_UNCONFIGURED, last_result=RESULT_UNCONFIGURED)
        return RESULT_UNCONFIGURED
    # skipped（role 選択肢なし）: 試行回数に数えず待つ
    await _release(op_id, token, STATE_PENDING, last_result=RESULT_SKIPPED)
    return RESULT_SKIPPED if result == RESULT_SKIPPED else RESULT_FAILED


async def retry_one(op_id: str, *, resolve_user_id, write_chatlog, find_chatlog=None, ledger_state=None) -> str:
    """fix1 互換の名前（画面の「記録のみ再試行」が呼ぶ）。process_one と同じ。"""
    return await process_one(op_id, resolve_user_id=resolve_user_id, write_chatlog=write_chatlog,
                             find_chatlog=find_chatlog, ledger_state=ledger_state)


async def run_followup(*, resolve_user_id, write_chatlog, find_chatlog=None, ledger_state=None,
                       limit: int = BATCH_LIMIT) -> dict:
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
            res = await process_one(op_id, resolve_user_id=resolve_user_id, write_chatlog=write_chatlog,
                                    find_chatlog=find_chatlog, ledger_state=ledger_state)
        except Exception:
            res = RESULT_FAILED
            logger.warning("[REPLY_FOLLOWUP] retry failed (fixed reason)")
        counts[res] = counts.get(res, 0) + 1
    if counts:
        logger.info("[REPLY_FOLLOWUP] retried=%s written=%s",
                    emit(sum(counts.values()), "count", "log", "operator"),
                    emit(counts.get(RESULT_WRITTEN, 0) + counts.get(RESULT_RECOVERED, 0), "count", "log", "operator"))
    return counts
