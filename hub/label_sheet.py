"""label_sheet — LABEL-PRINT-1 D2: ラベルシート残量とジョブ状態機械（durable DB）

正本: LABEL-PRINT-1 票（裁定 D2・第 2 段 1）＋ fix1（Codex LP-01〜06 採用・ジョブ状態機械）。

- 置き場所は既存の durable DB（hub/db.py・alembic）。kintone に新アプリは作らない。
- 1 シート = 1 行（label_sheet_state・sheet_id は S-1, S-2, …）。現在シートは同レイアウトの最新行。
  used_faces: 人が「印刷済」にした面。history: 消費バッチの履歴（「戻す」は直前バッチ）。
- **面の予約はジョブ（label_print_job）の行そのもの**（LP-01/02）: status ∈ {reserved, attached,
  printed, undone, cancelled}。reserved/attached が「その面を占有中（印刷待ち）」。
  予約は OK 受信時に 1 トランザクションで行い、シート行を行ロック（FOR UPDATE・sqlite は無視）で
  読み直して「used でも他ジョブの reserved/attached でもない」ことを確認してから挿入する。
  event_id（LINE の webhookEventId）は一意制約＝再配送の無効化。
- 操作コマンド（印刷済／戻す／新しいシート）は label_sheet_op（event_id 一意）に記録し、状態更新と
  同一トランザクションで確定する（LP-06）。同 event_id の再配送は状態を変えず結果を返す。
- 機械は「次の空き面」を提案するだけで、面を消費（used）するのは人の「印刷済」のみ。
- **PII 規律**: 本 module は logging を import しない。氏名・住所は保存しない
  （レコード番号・面番号・ファイル名・fileKey のみ）。

状態遷移（job.status）:
  (OK 受信) → reserved ──添付成功──→ attached ──「印刷済」──→ printed ──「戻す」──→ undone
             reserved（添付失敗で残留）──同一宛先・同一面の別イベント＝回収──→ attached
  cancelled は将来の明示取消用（本票では遷移しない）。
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from hub.db import session_scope

# app-state 専用 metadata（alembic env.py の target_metadata list に統合）
metadata = sa.MetaData()

DEFAULT_LAYOUT = "A4_2x5_aone31514"

JOB_RESERVED = "reserved"
JOB_ATTACHED = "attached"
JOB_PRINTED = "printed"
JOB_UNDONE = "undone"
JOB_CANCELLED = "cancelled"
JOB_STATUSES = (JOB_RESERVED, JOB_ATTACHED, JOB_PRINTED, JOB_UNDONE, JOB_CANCELLED)
ACTIVE_STATUSES = (JOB_RESERVED, JOB_ATTACHED)      # 面を占有中（印刷待ち）

OP_PRINTED = "printed"
OP_UNDO = "undo"
OP_NEW_SHEET = "new_sheet"

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_Json = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

label_sheet_state = sa.Table(
    "label_sheet_state", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("layout", sa.Text, nullable=False),
    sa.Column("used_faces", _Json, nullable=False),
    sa.Column("history", _Json, nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("updated_by", sa.Text, nullable=False, server_default=""),
)

label_print_job = sa.Table(
    "label_print_job", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),      # job_id
    sa.Column("event_id", sa.Text, nullable=False, unique=True),
    sa.Column("app", sa.Text, nullable=False),
    sa.Column("record", sa.Text, nullable=False),
    sa.Column("role", sa.Text, nullable=False),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("face", sa.Integer, nullable=False),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("file_key", sa.Text, nullable=False, server_default=""),
    sa.Column("filename", sa.Text, nullable=False, server_default=""),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("created_by", sa.Text, nullable=False, server_default=""),
    sa.CheckConstraint(
        "status IN ('reserved', 'attached', 'printed', 'undone', 'cancelled')",
        name="ck_label_print_job_status"),
)

label_sheet_op = sa.Table(
    "label_sheet_op", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
    sa.Column("event_id", sa.Text, nullable=False, unique=True),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("payload", _Json, nullable=False),
    sa.Column("result", _Json, nullable=False),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("created_by", sa.Text, nullable=False, server_default=""),
)


@dataclass(frozen=True)
class Job:
    job_id: int
    event_id: str
    app: str
    record: str
    role: str
    sheet_id: str
    face: int
    status: str
    file_key: str = ""
    filename: str = ""

    @property
    def target(self) -> tuple[str, str, str]:
        return (self.app, self.record, self.role)


@dataclass(frozen=True)
class SheetState:
    row_id: int
    sheet_id: str
    layout: str
    used: tuple[int, ...]
    history: tuple[tuple[int, ...], ...]
    active: tuple[Job, ...] = ()          # このシートの reserved/attached ジョブ（面を占有中）

    @property
    def pending(self) -> tuple[int, ...]:
        """印刷待ち（占有中）の面（昇順・重複なし）"""
        return tuple(sorted({j.face for j in self.active}))

    def free_faces(self, per_page: int) -> list[int]:
        """提案可能な面（使用済み・占有中を除く・昇順）"""
        taken = set(self.used) | set(self.pending)
        return [f for f in range(1, per_page + 1) if f not in taken]

    def next_free(self, per_page: int) -> int | None:
        free = self.free_faces(per_page)
        return free[0] if free else None

    def remaining(self, per_page: int) -> int:
        return len(self.free_faces(per_page))

    def jobs_on(self, face: int) -> list[Job]:
        return [j for j in self.active if j.face == face]


@dataclass(frozen=True)
class Reservation:
    """reserve_face の結果。outcome ∈ duplicate_event / sheet_changed / face_taken / attached / recover / reserved"""
    outcome: str
    job: Job | None = None
    state: SheetState | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _job(row) -> Job:
    return Job(job_id=int(row.id), event_id=row.event_id, app=row.app, record=row.record,
               role=row.role, sheet_id=row.sheet_id, face=int(row.face), status=row.status,
               file_key=row.file_key or "", filename=row.filename or "")


async def _active_jobs(session, sheet_id: str, *, lock: bool = False) -> list[Job]:
    q = (sa.select(label_print_job)
         .where(label_print_job.c.sheet_id == sheet_id,
                label_print_job.c.status.in_(ACTIVE_STATUSES))
         .order_by(label_print_job.c.id.asc()))
    if lock:
        q = q.with_for_update()
    return [_job(r) for r in (await session.execute(q)).fetchall()]


async def _state_of(session, row, *, lock: bool = False) -> SheetState:
    return SheetState(
        row_id=int(row.id), sheet_id=row.sheet_id, layout=row.layout,
        used=tuple(int(f) for f in (row.used_faces or [])),
        history=tuple(tuple(int(f) for f in batch) for batch in (row.history or [])),
        active=tuple(await _active_jobs(session, row.sheet_id, lock=lock)),
    )


async def _latest(session, layout: str, *, lock: bool = False):
    q = (sa.select(label_sheet_state)
         .where(label_sheet_state.c.layout == layout)
         .order_by(label_sheet_state.c.id.desc()).limit(1))
    if lock:
        q = q.with_for_update()
    return (await session.execute(q)).first()


async def _insert_sheet(session, layout: str, user_id: str) -> SheetState:
    count = (await session.execute(
        sa.select(sa.func.count()).select_from(label_sheet_state)
        .where(label_sheet_state.c.layout == layout))).scalar() or 0
    sheet_id = f"S-{int(count) + 1}"
    result = await session.execute(sa.insert(label_sheet_state).values(
        sheet_id=sheet_id, layout=layout, used_faces=[], history=[],
        updated_at=_now(), updated_by=user_id))
    return SheetState(row_id=int(result.inserted_primary_key[0]), sheet_id=sheet_id,
                      layout=layout, used=(), history=(), active=())


async def _update_sheet(session, state: SheetState, user_id: str, **values) -> None:
    await session.execute(
        sa.update(label_sheet_state)
        .where(label_sheet_state.c.id == state.row_id)
        .values(updated_at=_now(), updated_by=user_id, **values))


async def _update_job(session, job_id: int, **values) -> None:
    await session.execute(
        sa.update(label_print_job).where(label_print_job.c.id == job_id)
        .values(updated_at=_now(), **values))


async def _find_op(session, event_id: str):
    return (await session.execute(
        sa.select(label_sheet_op).where(label_sheet_op.c.event_id == event_id))).first()


# ── 読み取り ────────────────────────────────────────────────────────────────

async def get_state(layout: str = DEFAULT_LAYOUT) -> SheetState | None:
    """現在シート（最新行）。未作成なら None。"""
    async with session_scope() as session:
        row = await _latest(session, layout)
        return await _state_of(session, row) if row else None


async def ensure_state(user_id: str, layout: str = DEFAULT_LAYOUT) -> SheetState:
    """現在シートを返す（無ければ S-1 を作る）。"""
    async with session_scope() as session:
        row = await _latest(session, layout)
        if row:
            return await _state_of(session, row)
        return await _insert_sheet(session, layout, user_id)


async def get_job(job_id: int) -> Job | None:
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(label_print_job).where(label_print_job.c.id == job_id))).first()
        return _job(row) if row else None


# ── ジョブ状態機械（OK 受信・予約＝1 トランザクション） ──────────────────────

async def reserve_face(event_id: str, *, app: str, record: str, role: str, sheet_id: str,
                       face: int, explicit: bool, user_id: str,
                       layout: str = DEFAULT_LAYOUT) -> Reservation:
    """OK 受信時の予約（LP-01/02/04）。1 トランザクション・シート行を行ロックで読み直す。
    - event_id 既存 → duplicate_event（何もしない）
    - 現在シートが sheet_id と違う → sheet_changed（予約しない）
    - 同一宛先・同一面の reserved（添付未完）→ recover（再予約せず既存ジョブを返す・event_id を更新）
    - 同一宛先・同一面の attached → attached（添付済み）
    - 他宛先の reserved/attached がその面にある → face_taken
    - used の面は明示指定（explicit）のときだけ「再印字」として予約を許す。提案面が埋まっていれば face_taken
    - それ以外 → job を reserved で挿入（予約＝この行）→ reserved"""
    async with session_scope() as session:
        dup = (await session.execute(
            sa.select(label_print_job).where(label_print_job.c.event_id == event_id))).first()
        if dup:
            return Reservation("duplicate_event", _job(dup))
        row = await _latest(session, layout, lock=True)
        if row is None or row.sheet_id != sheet_id:
            return Reservation("sheet_changed")
        state = await _state_of(session, row, lock=True)
        target = (app, record, role)
        same = [j for j in state.active if j.face == face and j.target == target]
        if same:
            job = same[0]
            if job.status == JOB_ATTACHED:
                return Reservation("attached", job, state)
            await _update_job(session, job.job_id, event_id=event_id)
            return Reservation("recover", Job(**{**job.__dict__, "event_id": event_id}), state)
        if any(j.face == face for j in state.active):
            return Reservation("face_taken", None, state)
        if face in state.used and not explicit:
            return Reservation("face_taken", None, state)
        result = await session.execute(sa.insert(label_print_job).values(
            event_id=event_id, app=app, record=record, role=role, sheet_id=sheet_id,
            face=face, status=JOB_RESERVED, file_key="", filename="",
            created_at=_now(), updated_at=_now(), created_by=user_id))
        job = Job(job_id=int(result.inserted_primary_key[0]), event_id=event_id, app=app,
                  record=record, role=role, sheet_id=sheet_id, face=face, status=JOB_RESERVED)
        return Reservation("reserved", job, state)


async def set_job_file_key(job_id: int, file_key: str, filename: str) -> None:
    """アップロード成功（添付前）: fileKey を保存して回収時に再利用する（status は reserved のまま）"""
    async with session_scope() as session:
        await _update_job(session, job_id, file_key=file_key, filename=filename)


async def clear_job_file_key(job_id: int) -> None:
    """再利用した fileKey で添付できなかった（期限切れ等）: 次の回収で再アップロードさせる"""
    async with session_scope() as session:
        await _update_job(session, job_id, file_key="", filename="")


async def mark_job_attached(job_id: int, file_key: str, filename: str) -> None:
    """添付成功: reserved → attached（1 トランザクション）"""
    async with session_scope() as session:
        await _update_job(session, job_id, status=JOB_ATTACHED, file_key=file_key,
                          filename=filename)


# ── 操作コマンド（event_id 冪等・状態更新と同一トランザクション） ──────────────

async def consume_printed(event_id: str, user_id: str, face: int | None = None,
                          layout: str = DEFAULT_LAYOUT) -> tuple[str, SheetState | None, list[int]]:
    """「印刷済」: attached の面を used へ（人の操作のみが面を消費する）。face 指定なし＝attached 全部。
    戻り値: (outcome, 状態, 消費した面)。outcome ∈ applied / duplicate_event / nothing / no_sheet。
    ジョブは attached → printed。消費 0 件なら状態を変えない（op は記録する）。"""
    async with session_scope() as session:
        op = await _find_op(session, event_id)
        if op:
            row = await _latest(session, layout)
            state = await _state_of(session, row) if row else None
            return "duplicate_event", state, list((op.result or {}).get("faces") or [])
        row = await _latest(session, layout, lock=True)
        if not row:
            return "no_sheet", None, []
        state = await _state_of(session, row, lock=True)
        targets = [j for j in state.active if j.status == JOB_ATTACHED
                   and (face is None or j.face == face)]
        consumed = sorted({j.face for j in targets})
        if consumed:
            used = list(state.used) + [f for f in consumed if f not in state.used]
            history = [list(b) for b in state.history] + [consumed]
            await _update_sheet(session, state, user_id, used_faces=used, history=history)
            for j in targets:
                await _update_job(session, j.job_id, status=JOB_PRINTED)
        await session.execute(sa.insert(label_sheet_op).values(
            event_id=event_id, kind=OP_PRINTED, payload={"face": face},
            result={"faces": consumed}, sheet_id=state.sheet_id, created_at=_now(),
            created_by=user_id))
        row = await _latest(session, layout)
        return ("applied" if consumed else "nothing"), await _state_of(session, row), consumed


async def undo_last(event_id: str, user_id: str,
                    layout: str = DEFAULT_LAYOUT) -> tuple[str, SheetState | None, list[int]]:
    """「戻す」: 直前の消費バッチを取り消す（面は空きに戻る・ジョブは printed → undone）。
    戻り値: (outcome, 状態, 取り消した面)。outcome ∈ applied / duplicate_event / nothing / no_sheet。"""
    async with session_scope() as session:
        op = await _find_op(session, event_id)
        if op:
            row = await _latest(session, layout)
            state = await _state_of(session, row) if row else None
            return "duplicate_event", state, list((op.result or {}).get("faces") or [])
        row = await _latest(session, layout, lock=True)
        if not row:
            return "no_sheet", None, []
        state = await _state_of(session, row, lock=True)
        undone: list[int] = []
        if state.history:
            history = [list(b) for b in state.history]
            undone = history.pop()
            used = [f for f in state.used if f not in undone]
            await _update_sheet(session, state, user_id, used_faces=used, history=history)
            for f in undone:
                latest_printed = (await session.execute(
                    sa.select(label_print_job.c.id)
                    .where(label_print_job.c.sheet_id == state.sheet_id,
                           label_print_job.c.face == f,
                           label_print_job.c.status == JOB_PRINTED)
                    .order_by(label_print_job.c.id.desc()).limit(1))).first()
                if latest_printed:
                    await _update_job(session, int(latest_printed[0]), status=JOB_UNDONE)
        await session.execute(sa.insert(label_sheet_op).values(
            event_id=event_id, kind=OP_UNDO, payload={}, result={"faces": undone},
            sheet_id=state.sheet_id, created_at=_now(), created_by=user_id))
        row = await _latest(session, layout)
        return ("applied" if undone else "nothing"), await _state_of(session, row), undone


async def new_sheet(event_id: str, user_id: str,
                    layout: str = DEFAULT_LAYOUT) -> tuple[str, SheetState]:
    """「新しいシート」: 使用済みを空にした新しい行を開始する（旧行は残す）。
    同 event_id の再配送はシートを増やさず当時の結果を返す。"""
    async with session_scope() as session:
        op = await _find_op(session, event_id)
        if op:
            row = await _latest(session, layout)
            return "duplicate_event", await _state_of(session, row)
        await _latest(session, layout, lock=True)
        state = await _insert_sheet(session, layout, user_id)
        await session.execute(sa.insert(label_sheet_op).values(
            event_id=event_id, kind=OP_NEW_SHEET, payload={}, result={"sheet_id": state.sheet_id},
            sheet_id=state.sheet_id, created_at=_now(), created_by=user_id))
        return "applied", state


def status_text(state: SheetState, per_page: int) -> str:
    """残量の復唱文（レコード番号・面番号・残量のみ＝PII なし・D8）。"""
    used = "・".join(str(f) for f in sorted(state.used)) or "なし"
    pending = "・".join(str(f) for f in state.pending) or "なし"
    return (f"シート {state.sheet_id}（{per_page} 面）: 使用済み {len(state.used)} 面"
            f"（{used}）・印刷待ち {len(state.pending)} 面（{pending}）・"
            f"残り {state.remaining(per_page)} 面")
