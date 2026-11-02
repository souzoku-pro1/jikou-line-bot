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
- fix4（Codex R-Q1b-fix2 BR-13〜15）:
  BR-13 照合は 4 値（found／absent／unavailable／unconfigured）。**取得不能（unavailable）は「不存在」ではない**ので作成せず、
  回収行を pending（last_result=lookup_unavailable）に残して次回に回す。業務 LINE への通知は 1 日 1 回（台帳 key
  reply_followup_hold:{JST 日付}・notify_holds_once）。
  BR-14 回収中は heartbeat（send_ledger と同じ流儀）で owner の deadline を延ばし、**外部作成の直前**に「自分がまだ owner か」を
  同一 tx の条件付き UPDATE で再確認する（confirm_owner_before_create・同時に last_result=creating を置く）。失われていれば作成しない。
  作成呼出後に応答を失った行（write が unknown／creating のまま期限切れ）は last_result=unknown として残し、**期限切れだけを根拠に
  再作成しない**（op_id 照合で既存が見つかるまで作成しない）。
  BR-15 prepare() は同 op_id の既存行を 業務・宛先ハッシュ・本文ハッシュ・状態 で照合し、未送信を確実に確認できる（台帳行なし・
  prepared・ハッシュ一致）ときだけ同一 tx で再準備（reprepared）。終端・送信済み・不一致・台帳行ありは rejected（呼出側は送らない）。
  台帳 begin 失敗の経路では回収行を閉じず prepared のまま残す（本文保持）＝同一 op_id・同一本文の再 POST で再開できる。
  マーカーは kintone の単語検索仕様（`[ ] : %` 等の記号はキーワードから除かれ、`_` は単語の一部）に合わせ `op_{op_id}`
  （英数字と `_` のみ・本文末尾に 1 行）。like 検索の後段で本文末尾の等価照合（marker_matches）を行う。
- fix5（Codex R-Q1b-fix4 BR-16・17）:
  BR-16 claim() は条件付き UPDATE と **同一トランザクション** で取得後の最新行を返す。process_one() は claim 前に読んだ行を
  作成可否の判断（確定状態・診断理由・状態）に使わず、claim の戻り値だけを使う（claim 前の古い行で unknown を見落とさない）。
  BR-17 列を分離: `create_state`＝**外部作成の確定状態**（none／creating／unknown／created・migration a7c9e1f3b5d7）、
  `last_result`＝**直近の診断理由**（destination_mismatch・lookup_unavailable・failed・written 等）。宛先取得失敗などは診断理由だけを
  更新し、確定状態は触らない。確定状態の遷移は単調（none→creating→unknown／created、unknown→created のみ）で、条件付き UPDATE
  （CASE 式）が逆行・上書きを拒否する。
- fix6（Codex R-Q1b-fix5 BR-18）: 回収行に **世代番号 row_version**（migration c8e0a2b4d6f8・既存行は 0）を持たせ、内容を変える更新
  （prepare の再準備・claim・owner 再確認・release・close・complete・mark_pending・note_prepared）のたびに +1 する。close／complete と
  所有者の終了更新は「読んだ時点の row_version と一致する場合のみ」の条件付き UPDATE（一致しなければ何もせず次回へ）。close は
  **同一トランザクション内で送信台帳の最新状態を再読取** し、台帳に行がある（＝未送信ではない）なら not_sent で閉じない／failed でなければ
  ledger_failed で閉じない。heartbeat は deadline だけを延ばす（内容を変えないため世代は進めない＝所有者側の世代追跡を壊さない）。
"""

import asyncio
import contextlib
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
HEARTBEAT_SECONDS = 60                # fix4 BR-14: 回収中の所有期限の延長間隔（CLAIM_SECONDS より十分短く）
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
RESULT_LOOKUP_UNAVAILABLE = "lookup_unavailable"   # fix4 BR-13: App 28 の照合が取得不能（不存在ではない）→ 作成せず次回へ
RESULT_UNKNOWN = "unknown"                 # fix4 BR-14: 作成呼出後に応答喪失（作成されたか不明）→ 照合で見つかるまで作成しない
WRITE_NOT_OWNER = "not_owner"              # fix4 BR-14: write_chatlog が作成直前の owner 再確認に失敗（作成していない）
# fix5 BR-17: 外部作成の確定状態（create_state 列・閉集合・単調遷移）
CREATE_NONE = "none"
CREATE_CREATING = "creating"               # owner 再確認〜作成呼出の間の印（この印のまま期限切れ＝unknown 扱い）
CREATE_UNKNOWN = "unknown"                 # 作成呼出後に応答喪失（作成されたか不明）
CREATE_CREATED = "created"                 # 作成できた／既存行を照合した
CREATE_STATES = (CREATE_NONE, CREATE_CREATING, CREATE_UNKNOWN, CREATE_CREATED)
CREATE_TRANSITIONS = {CREATE_CREATING: (CREATE_NONE,),
                      CREATE_UNKNOWN: (CREATE_CREATING,),
                      CREATE_CREATED: (CREATE_CREATING, CREATE_UNKNOWN)}
# find_chatlog の戻り値（4 値・fix4 BR-13）
LOOKUP_FOUND = "found"
LOOKUP_ABSENT = "absent"
LOOKUP_UNAVAILABLE = "unavailable"
LOOKUP_UNCONFIGURED = "unconfigured"
LOOKUP_RESULTS = (LOOKUP_FOUND, LOOKUP_ABSENT, LOOKUP_UNAVAILABLE, LOOKUP_UNCONFIGURED)
# prepare() の rejected 理由（閉集合・ログは固定文言）
PREPARE_REJECT_REASONS = ("terminal", "already_sent", "destination_mismatch", "text_mismatch", "ledger_exists")
# 1 日 1 回の業務 LINE 通知（台帳 key）
LEDGER_BUSINESS = "jikou"
LEDGER_CHANNEL = "reply_followup"
_JST = datetime.timezone(datetime.timedelta(hours=9))
HOLD_NOTICE_TEXT = ("【チャットログ記録の保留】事務所からの返信のチャットログ（App 28）記録で、既存記録の照合ができない、"
                    "または記録結果が不明の操作があります。二重記録を避けるため自動では作成せず、回収ジョブが照合を続けます。\n"
                    "件数: {count}\n"
                    "確認: 回収表 reply_chatlog_followup の state=pending・last_result=lookup_unavailable／unknown の行。"
                    "kintone に同じ操作の記録（本文末尾の op_ マーカー）があれば次回の照合で完了になります。")
# 台帳の状態語彙（hub/send_ledger と同じ・循環 import を避けるため文字列で持つ）
_LEDGER_SENT = "sent"
_LEDGER_FAILED = "failed"
OP_MARKER_PREFIX = "op_"              # fix4: 英数字と _ のみ（kintone 単語検索で 1 単語として一致する）。旧 `[op:…]` は除去だけ受ける
OP_MARKER_RE = re.compile(r"\n?(?:op_[0-9a-f]{32}|\[op:[0-9a-f]{32}\])\s*$")

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
    sa.Column("last_result", sa.Text, nullable=True),            # fix5 BR-17: 直近の診断理由（確定状態は create_state）
    sa.Column("create_state", sa.Text, nullable=False, server_default=CREATE_NONE),   # fix5 BR-17: 外部作成の確定状態
    sa.Column("row_version", sa.Integer, nullable=False, server_default="0"),         # fix6 BR-18: 世代番号（内容更新ごとに +1）
    sa.Column("owner_token", sa.Text, nullable=True),            # fix2 BR-11: 回収の所有権
    sa.Column("deadline_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("chatlog_record_id", sa.Text, nullable=True),      # fix2 BR-11: 照合で見つけた／作成した App 28 レコード No.
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("done_at", sa.DateTime(timezone=True), nullable=True),
    sa.CheckConstraint("state IN ('prepared', 'pending', 'unconfigured', 'in_progress', 'done', 'closed', 'exhausted')",
                       name="ck_reply_followup_state"),
    sa.CheckConstraint("create_state IN ('none', 'creating', 'unknown', 'created')", name="ck_reply_followup_create_state"),
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
    return f"{OP_MARKER_PREFIX}{op_id}"


def marker_matches(text: str, op_id: str) -> bool:
    """fix4 BR-13: 検索結果の本文が **この op_id のマーカーで終わる** ことの等価照合（like は単語検索なので後段で厳密に判定）。"""
    lines = str(text or "").rstrip().split("\n")
    return bool(lines) and lines[-1].strip() == op_marker(str(op_id))


def with_op_marker(text: str, op_id: str) -> str:
    return f"{text}\n{op_marker(op_id)}"


def strip_op_marker(text: str) -> str:
    return OP_MARKER_RE.sub("", str(text or ""))


# ── 書込（状態遷移はすべて条件付き UPDATE） ──────────────────────────────────────
def _prepare_mismatch(row, business: str, line_user_hash: str, text_version: str | None) -> str | None:
    """fix4 BR-15: 既存行との照合。None=再準備可（prepared・業務・宛先ハッシュ・本文ハッシュが一致）、それ以外は rejected の理由。"""
    if row.state in (STATE_DONE, STATE_CLOSED, STATE_EXHAUSTED):
        return "terminal"
    if row.state != STATE_PREPARED:
        return "already_sent"
    if row.business != business or row.kind != KIND_CHATLOG_HUMAN or row.line_user_hash != line_user_hash:
        return "destination_mismatch"
    if not text_version or row.text_version != text_version:
        return "text_mismatch"
    return None


def _log_prepare_rejected(reason: str) -> None:
    if reason == "terminal":
        logger.info("[REPLY_FOLLOWUP] prepare rejected (terminal state)")
    elif reason == "already_sent":
        logger.info("[REPLY_FOLLOWUP] prepare rejected (already sent)")
    elif reason == "destination_mismatch":
        logger.info("[REPLY_FOLLOWUP] prepare rejected (destination mismatch)")
    elif reason == "text_mismatch":
        logger.info("[REPLY_FOLLOWUP] prepare rejected (text mismatch)")
    else:
        logger.info("[REPLY_FOLLOWUP] prepare rejected (ledger row exists)")


async def prepare(op_id: str, *, business: str, approval_record_id: str, line_user_hash: str, text: str,
                  text_version: str | None) -> str:
    """BR-12: 送信の **前** に prepared を置く。
    fix4 BR-15: 同 op_id の既存行は 業務・宛先ハッシュ・本文ハッシュ・状態 を照合し、**未送信を確実に確認できる**
    （台帳に行なし・回収行 prepared・ハッシュ一致）ときだけ同一 tx で再準備する。
    戻り値: prepared（新規）/ reprepared（再準備・送信可）/ rejected（終端・送信済み・不一致・台帳行あり＝送らない）/
    skipped（DB なし）/ failed（固定語彙ログ）。"""
    if not _db_configured():
        return "skipped"
    now = _now()
    t = reply_chatlog_followup
    try:
        async with session_scope() as s:
            row = (await s.execute(sa.select(t).where(t.c.op_id == str(op_id)))).first()
            if row is None:
                await s.execute(sa.insert(t).values(
                    op_id=str(op_id), business=business, kind=KIND_CHATLOG_HUMAN,
                    approval_record_id=str(approval_record_id), line_user_hash=line_user_hash,
                    text=text, text_version=text_version, state=STATE_PREPARED, attempts=0, last_result=None,
                    created_at=now, updated_at=now))
                return "prepared"
            reason = _prepare_mismatch(row, business, line_user_hash, text_version)
            if reason is None:
                from hub.send_ledger import send_operation
                led = (await s.execute(sa.select(send_operation.c.op_id).where(
                    send_operation.c.op_id == str(op_id)))).first()
                if led is not None:
                    reason = "ledger_exists"
            if reason is not None:
                _log_prepare_rejected(reason)
                return "rejected"
            # fix6 BR-18: 読んだ世代と一致するときだけ再準備し、世代を進める（claim と同じ条件付き UPDATE の排他）
            r = await s.execute(sa.update(t).where(t.c.op_id == str(op_id), t.c.state == STATE_PREPARED,
                                                    t.c.row_version == int(row.row_version)).values(
                text=text, attempts=0, last_result=None, created_at=now, updated_at=now,
                row_version=int(row.row_version) + 1))
            if int(r.rowcount or 0) != 1:
                _log_prepare_rejected("terminal")
                return "rejected"
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] prepare failed (fixed reason)")
        return "failed"
    return "reprepared"


async def note_prepared(op_id: str, reason: str) -> bool:
    """fix4 BR-15: 台帳 begin 失敗など「送っていない」経路の印を prepared 行に残す（閉じない・本文は保持）。"""
    try:
        return await _set(op_id, STATE_PREPARED, last_result=str(reason))
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] note prepared failed (fixed reason)")
        return False


async def _set(op_id: str, where_states, *, expected_version: int | None = None, **values) -> bool:
    """条件付き UPDATE（state が where_states のいずれかのときだけ・fix6: expected_version があれば世代一致も AND）。
    世代は +1 する。戻り値=更新したか。"""
    states = (where_states,) if isinstance(where_states, str) else tuple(where_states)
    t = reply_chatlog_followup
    where = [t.c.op_id == str(op_id), t.c.state.in_(list(states))]
    if expected_version is not None:
        where.append(t.c.row_version == int(expected_version))
    async with session_scope() as s:
        r = await s.execute(sa.update(t).where(*where).values(updated_at=_now(), row_version=t.c.row_version + 1, **values))
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


async def complete(op_id: str, *, expected_version: int, chatlog_record_id: str | None = None,
                   last_result: str = RESULT_WRITTEN) -> bool:
    """App 28 に書けた（または既存行を照合した）→ done（本文は保持しない）。終端以外から、かつ読んだ世代と一致するときのみ（fix6）。"""
    try:
        return await _set(op_id, OPEN_STATES, expected_version=expected_version, state=STATE_DONE, text=None,
                          done_at=_now(), last_result=last_result, owner_token=None, deadline_at=None,
                          create_state=_create_state_to(CREATE_CREATED),
                          chatlog_record_id=(str(chatlog_record_id) if chatlog_record_id else None))
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] complete failed (fixed reason)")
        return False


async def close(op_id: str, reason: str, *, expected_version: int) -> bool:
    """未送信で終端（台帳 failed／台帳なし／LINE 失敗 等）。本文は保持しない。
    fix6 BR-18: (1) 読んだ時点の世代 expected_version と一致する行だけ（一致しなければ何もせず False＝次回へ）。
    (2) **同一トランザクション内で送信台帳の最新状態を再読取** する——台帳に行がある＝未送信ではないので not_sent では閉じない、
    台帳が failed でなければ ledger_failed では閉じない、その他の理由も台帳が sent／unconfirmed／started／pending なら閉じない。"""
    t = reply_chatlog_followup
    try:
        from hub.send_ledger import send_operation
        async with session_scope() as s:
            led = (await s.execute(sa.select(send_operation.c.state).where(
                send_operation.c.op_id == str(op_id)))).first()
            led_state = str(led.state) if led is not None else None
            if reason == "not_sent" and led_state is not None:
                logger.info("[REPLY_FOLLOWUP] close skipped (ledger row appeared)")
                return False
            if reason == "ledger_failed" and led_state != _LEDGER_FAILED:
                logger.info("[REPLY_FOLLOWUP] close skipped (ledger no longer failed)")
                return False
            if led_state not in (None, _LEDGER_FAILED):
                logger.info("[REPLY_FOLLOWUP] close skipped (ledger not failed)")
                return False
            r = await s.execute(sa.update(t).where(
                t.c.op_id == str(op_id), t.c.state.in_(list(OPEN_STATES)),
                t.c.row_version == int(expected_version)).values(
                state=STATE_CLOSED, text=None, last_result=str(reason), owner_token=None, deadline_at=None,
                done_at=_now(), updated_at=_now(), row_version=t.c.row_version + 1))
            return int(r.rowcount or 0) == 1
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


async def claim(op_id: str, *, now=None) -> dict | None:
    """BR-11: 回収の所有権を取る（prepared／pending／unconfigured、または期限切れ in_progress → in_progress）。
    fix5 BR-16: 条件付き UPDATE と **同一トランザクション** で取得後の最新行を読み戻して返す（dict・owner_token を含む）。
    取れなければ None。呼び出し側は作成可否の判断にこの戻り値だけを使う（claim 前に読んだ行は使わない）。"""
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
            updated_at=now, row_version=t.c.row_version + 1))
        if int(r.rowcount or 0) != 1:
            return None
        row = (await s.execute(sa.select(t).where(t.c.op_id == str(op_id), t.c.owner_token == token))).first()
        return dict(row._mapping) if row is not None else None


def _create_state_to(to: str):
    """fix5 BR-17: 確定状態の単調遷移を 1 文の CASE 式で表す（許される前状態からだけ to へ・それ以外は現在値のまま）。"""
    t = reply_chatlog_followup
    return sa.case((t.c.create_state.in_(list(CREATE_TRANSITIONS[to])), to), else_=t.c.create_state)


async def _release(op_id: str, token: str, to_state: str, *, expected_version: int, **values) -> bool:
    """所有している行だけを to_state へ戻す（所有権を失っていれば何もしない）。
    fix6 BR-18: 読んだ時点の世代（claim の読み戻し＋自分の owner 再確認で進めた分）と一致するときだけ。世代を +1 する。"""
    t = reply_chatlog_followup
    async with session_scope() as s:
        r = await s.execute(sa.update(t).where(t.c.op_id == str(op_id), t.c.state == STATE_IN_PROGRESS,
                                                t.c.owner_token == token, t.c.row_version == int(expected_version)).values(
            state=to_state, owner_token=None, deadline_at=None, updated_at=_now(), row_version=t.c.row_version + 1, **values))
        return int(r.rowcount or 0) == 1


async def heartbeat(op_id: str, token: str, *, now=None) -> bool:
    """fix4 BR-14: 回収中の所有期限を延ばす（所有者のみ）。戻り値=延長できたか（所有権を失っていれば False）。
    fix6: deadline だけを更新し、世代（row_version）は進めない（内容を変えない・所有者側の世代追跡を壊さない）。"""
    now = now or _now()
    t = reply_chatlog_followup
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(t).where(t.c.op_id == str(op_id), t.c.state == STATE_IN_PROGRESS,
                                                    t.c.owner_token == token).values(
                deadline_at=now + datetime.timedelta(seconds=CLAIM_SECONDS), updated_at=now))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] heartbeat failed (fixed reason)")
        return False


@contextlib.asynccontextmanager
async def heartbeat_task(op_id: str, token: str):
    """fix4 BR-14: 照合・外部作成を await している間、別タスクが HEARTBEAT_SECONDS ごとに heartbeat() を呼ぶ
    （send_ledger.heartbeat_task と同じ流儀）。終了時に必ず cancel して待つ。ログは出さない。"""
    interval = HEARTBEAT_SECONDS

    async def _beat():
        try:
            while True:
                await asyncio.sleep(interval)
                await heartbeat(op_id, token)
        except asyncio.CancelledError:
            pass

    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


async def confirm_owner_before_create(op_id: str, token: str, *, now=None) -> bool:
    """fix4 BR-14: **外部作成の直前**に「自分がまだ owner か」を同一 tx の条件付き UPDATE で再確認する。
    fix5 BR-17: 同時に確定状態を none→creating へ進める（none 以外＝unknown／creating／created の行は作成に進ませない）。
    期限も延長する。失われていれば False＝呼び出し側は作成しない。診断理由（last_result）は触らない。"""
    now = now or _now()
    t = reply_chatlog_followup
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(t).where(t.c.op_id == str(op_id), t.c.state == STATE_IN_PROGRESS,
                                                    t.c.owner_token == token, t.c.create_state == CREATE_NONE).values(
                create_state=CREATE_CREATING, deadline_at=now + datetime.timedelta(seconds=CLAIM_SECONDS), updated_at=now,
                row_version=t.c.row_version + 1))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.warning("[REPLY_FOLLOWUP] owner recheck failed (fixed reason)")
        return False


async def _lookup(find_chatlog, user_id: str, op_id: str) -> tuple[str, str | None]:
    """fix4 BR-13: find_chatlog の戻り値を 4 値 (status, record_id) に正規化する。例外は unavailable（不存在ではない）。
    旧形式（record_id|None）も受ける（found／absent）。find_chatlog が無ければ absent（照合手段なし＝従来どおり作成へ）。"""
    if find_chatlog is None:
        return LOOKUP_ABSENT, None
    try:
        res = await find_chatlog(user_id, op_id)
    except Exception:
        return LOOKUP_UNAVAILABLE, None
    if isinstance(res, tuple):
        status = str(res[0]) if res else LOOKUP_UNAVAILABLE
        rid = res[1] if len(res) > 1 and res[1] else None
        if status not in LOOKUP_RESULTS:
            status = LOOKUP_UNAVAILABLE
        return status, (str(rid) if rid else None)
    if res:
        return LOOKUP_FOUND, str(res)
    return LOOKUP_ABSENT, None


async def _finish_done(op_id: str, token: str, *, expected_version: int, record_id: str | None, last_result: str) -> bool:
    return await _release(op_id, token, STATE_DONE, expected_version=expected_version, text=None, done_at=_now(),
                          last_result=last_result,
                          create_state=_create_state_to(CREATE_CREATED),
                          chatlog_record_id=(str(record_id) if record_id else None))


async def process_one(op_id: str, *, resolve_user_id, write_chatlog, find_chatlog=None, ledger_state=None,
                      now=None) -> str:
    """1 件を処理する（冪等・所有権つき）。
    resolve_user_id(approval_record_id) -> line_user_id|None（App 29 から再解決）、
    write_chatlog(user_id, text_with_marker, confirm_owner) -> written / skipped / failed（作成呼出前の失敗）/
      unknown（作成呼出後に応答喪失）/ not_owner（confirm_owner が False＝作成していない）/ unconfigured（または (結果, record_id)）。
      confirm_owner() は **外部作成の直前** に呼ぶこと（fix4 BR-14）、
    find_chatlog(user_id, op_id) -> (found|absent|unavailable|unconfigured, record_id|None)（fix4 BR-13・4 値）、
    ledger_state(op_id) -> send_ledger の状態（None=台帳行なし）。戻り値は閉集合（RESULT_*）。
    fix5 BR-16: 作成可否の判断（確定状態 create_state・状態）は claim() が同一 tx で読み戻した行だけで行う。"""
    # claim 前の読取は 存在・開状態・台帳との突合 にだけ使う（fix5 BR-16: 作成可否の判断には使わない）
    row0 = await get_followup(op_id)
    if row0 is None:
        return RESULT_NOT_FOUND
    if row0["state"] not in OPEN_STATES:
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
            created = _aware(row0.get("created_at"))
            if created is not None and (now or _now()) - created < datetime.timedelta(seconds=LEDGER_GRACE_SECONDS):
                return RESULT_WAITING_LEDGER
            # fix6 BR-18: 読んだ世代と一致し、同一 tx で台帳を再読取しても行が無いときだけ閉じる（一致しなければ次回へ）
            if await close(op_id, "not_sent", expected_version=int(row0["row_version"])):
                return RESULT_CLOSED
            return RESULT_WAITING_LEDGER
        if state == _LEDGER_FAILED:
            if await close(op_id, "ledger_failed", expected_version=int(row0["row_version"])):
                return RESULT_CLOSED
            return RESULT_WAITING_LEDGER
        if state != _LEDGER_SENT:
            return RESULT_WAITING_LEDGER
    # fix5 BR-16: 以降の判断は claim が同一 tx で読み戻した最新行だけで行う
    row = await claim(op_id, now=now)
    if row is None:
        return RESULT_NOT_PENDING
    token = row["owner_token"]
    ver = int(row["row_version"])                 # fix6 BR-18: 以降の終了更新は この世代（＋自分が進めた分）と一致するときだけ
    # fix4 BR-14／fix5 BR-17: 確定状態が creating（作成直前の印のまま期限切れ）／unknown（応答喪失）の行は再作成しない。
    # 以下の release は診断理由（last_result）だけを更新し、確定状態は creating→unknown の単調遷移以外は触らない
    unknown_before = row.get("create_state") in (CREATE_CREATING, CREATE_UNKNOWN)
    cs = {"create_state": _create_state_to(CREATE_UNKNOWN)} if unknown_before else {}
    from hub.send_ledger import line_user_hash
    try:
        user_id = await resolve_user_id(row["approval_record_id"])
    except Exception:
        user_id = None
    if not user_id or line_user_hash(row["business"], user_id) != row["line_user_hash"]:
        await _release(op_id, token, STATE_PENDING, expected_version=ver, last_result=RESULT_DESTINATION_MISMATCH, **cs)
        return RESULT_DESTINATION_MISMATCH
    async with heartbeat_task(op_id, token):
        # BR-11／fix4 BR-13: 作成前に op_id で照合（成功応答喪失の修復）。4 値で分岐し、取得不能は「不存在」扱いしない
        status, existing = await _lookup(find_chatlog, user_id, str(op_id))
        if status == LOOKUP_FOUND:
            await _finish_done(op_id, token, expected_version=ver, record_id=existing, last_result=RESULT_RECOVERED)
            return RESULT_RECOVERED
        if status == LOOKUP_UNCONFIGURED:
            await _release(op_id, token, STATE_UNCONFIGURED, expected_version=ver, last_result=RESULT_UNCONFIGURED, **cs)
            return RESULT_UNCONFIGURED
        if status == LOOKUP_UNAVAILABLE:
            await _release(op_id, token, STATE_PENDING, expected_version=ver, last_result=RESULT_LOOKUP_UNAVAILABLE, **cs)
            return RESULT_LOOKUP_UNAVAILABLE
        if unknown_before:
            await _release(op_id, token, STATE_PENDING, expected_version=ver, last_result=RESULT_UNKNOWN, **cs)
            return RESULT_UNKNOWN

        async def _confirm_owner() -> bool:
            return await confirm_owner_before_create(op_id, token, now=now)
        result = await write_chatlog(user_id, with_op_marker(row["text"] or "", str(op_id)), _confirm_owner)
    record_id = None
    if isinstance(result, tuple):
        result, record_id = result[0], result[1]
    if result in (RESULT_WRITTEN, RESULT_UNKNOWN):
        ver += 1                                    # owner 再確認（confirm_owner）が世代を 1 つ進めている
    if result == RESULT_WRITTEN:
        if await _finish_done(op_id, token, expected_version=ver, record_id=record_id, last_result=RESULT_WRITTEN):
            return RESULT_WRITTEN
        return RESULT_NOT_PENDING
    if result == WRITE_NOT_OWNER:
        return RESULT_NOT_PENDING                   # 所有権を失っていた（作成していない・行は新しい owner のもの）
    if result == RESULT_UNKNOWN:
        await _release(op_id, token, STATE_PENDING, expected_version=ver, last_result=RESULT_UNKNOWN,
                       create_state=_create_state_to(CREATE_UNKNOWN))
        logger.warning("[REPLY_FOLLOWUP] chatlog create result unknown (kept, no auto re-create)")
        return RESULT_UNKNOWN
    if result == RESULT_FAILED:
        attempts = int(row["attempts"]) + 1
        if attempts >= MAX_ATTEMPTS:
            # fix3: 終端なので本文は保持しない（done／closed と同じ）
            await _release(op_id, token, STATE_EXHAUSTED, expected_version=ver, attempts=attempts,
                           last_result=RESULT_FAILED, text=None)
            logger.warning("[REPLY_FOLLOWUP] chatlog retry exhausted (max attempts)")
            return RESULT_EXHAUSTED
        await _release(op_id, token, STATE_PENDING, expected_version=ver, attempts=attempts, last_result=RESULT_FAILED)
        return RESULT_FAILED
    if result == RESULT_UNCONFIGURED:
        await _release(op_id, token, STATE_UNCONFIGURED, expected_version=ver, last_result=RESULT_UNCONFIGURED)
        return RESULT_UNCONFIGURED
    # skipped（role 選択肢なし）: 試行回数に数えず待つ
    await _release(op_id, token, STATE_PENDING, expected_version=ver, last_result=RESULT_SKIPPED)
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
    held = counts.get(RESULT_LOOKUP_UNAVAILABLE, 0) + counts.get(RESULT_UNKNOWN, 0)
    if held:
        try:
            await notify_holds_once(held)
        except Exception:
            logger.warning("[REPLY_FOLLOWUP] hold notice failed (fixed reason)")
    return counts


# ── fix4 BR-13: 保留（照合不能・結果不明）の業務 LINE 通知（1 日 1 回・台帳 key） ─────────────
def _day_key(now: datetime.datetime | None = None) -> str:
    now = now or datetime.datetime.now(_JST)
    return "reply_followup_hold:" + now.astimezone(_JST).strftime("%Y-%m-%d")


async def notify_holds_once(count: int) -> bool:
    """照合不能／結果不明の回収行がある日に 1 回だけ業務 LINE へ通知する（台帳 key reply_followup_hold:{JST 日付}・
    hub.approved_texts.notify_unapproved_once と同じ流儀）。件数のみ（op_id・本文・宛先は載せない）。
    DUPLICATE は送らない。台帳不可は直接送る（throttle）。管理者 ID 未設定は何もしない（False）。"""
    from hub import send_ledger
    from hub.notify import get_admin_line_user_id, notify_admin_line
    admin_id = get_admin_line_user_id() or ""
    if not admin_id:
        return False
    text = HOLD_NOTICE_TEXT.format(count=int(count))
    token = send_ledger.bind_inbound(_day_key())
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(LEDGER_BUSINESS, LEDGER_CHANNEL, admin_id, text)
    except Exception:
        op = None
    finally:
        send_ledger.unbind(token)
    if op is send_ledger.DUPLICATE_SENT or op is send_ledger.DUPLICATE_UNCONFIRMED:
        logger.info("[REPLY_FOLLOWUP] hold notice skipped (already sent today per ledger)")
        return True
    if not isinstance(op, send_ledger.Operation):
        return await notify_admin_line(text, throttle_key="reply_followup_hold:daily")
    try:
        ok = await notify_admin_line(text, throttle_key="reply_followup_hold:daily")
    except Exception:
        await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
        raise
    await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
    return ok
