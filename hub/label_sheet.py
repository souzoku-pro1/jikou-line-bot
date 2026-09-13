"""label_sheet — LABEL-PRINT-1 D2: ラベルシート残量とジョブ状態機械（durable DB）

正本: LABEL-PRINT-1 票（裁定 D2・第 2 段 1）＋ fix1（Codex LP-01〜06）＋ fix2（Codex LPF1-01〜04）。

- 置き場所は既存の durable DB（hub/db.py・alembic）。kintone に新アプリは作らない。
- **シート管理行（label_sheet_control・layout 主キー）が現在シートの単一の正**（LPF1-03）。
  現在シートの取得・予約・消費・戻す・新しいシート はすべて、まず該当 layout の管理行を
  SELECT … FOR UPDATE でロックしてから行う（「最新行」を探すロックは使わない）。新しいシートの
  作成も同じロック内で行い current_sheet_id を更新してコミットする。label_sheet_state は
  (layout, sheet_no) 一意＝同時実行で同じ S-1 が二重作成されないことを DB で保証。
- 1 シート = 1 行（label_sheet_state・sheet_id は S-1, S-2, …）。used_faces: 人が「印刷済」にした面。
  history: 消費バッチの履歴 {job_ids, faces, prior_used}（「戻す」は直前バッチ・LPF1-04）。
- **面の予約はジョブ（label_print_job）の行そのもの**（LP-01/02）: status ∈ {reserved, attached,
  printed, undone, cancelled, failed}。reserved/attached/failed が「その面を占有中（印刷待ち）」。
- **所有者と期限**（LPF1-01）: reserved のジョブは owner_token（実行ごと）と lease_expires_at
  （既定 LABEL_JOB_LEASE_SEC=600 秒）を持つ。回収（recover）は「reserved かつ期限切れ」または
  「failed（明示的な失敗確定）」のときだけ。期限内の reserved は in_progress（外部処理を始めない）。
  回収は所有者と期限を自分のものに CAS で書き換えてから進む。fileKey 保存・attached 化・失敗確定は
  「owner_token が自分 かつ status が reserved」条件の UPDATE（0 行＝所有権喪失 OwnershipLost）。
  printed/undone の後に attached へ戻す経路はない。
- **イベント履歴**（LPF1-02）: label_event（event_id 一意・kind ∈ {ok, printed, undo, new_sheet}）。
  OK の新規予約・回収の両方で job の更新と同一トランザクションで記録する。job.event_id は
  作成時の値を保持し上書きしない。event_id 重複は全 kind で no-op（duplicate_event）。
- 機械は「次の空き面」を提案するだけで、面を消費（used）するのは人の「印刷済」のみ。
- **PII 規律**: 本 module は logging を import しない。氏名・住所は保存しない
  （レコード番号・面番号・ファイル名・fileKey のみ）。

状態遷移（job.status）:
  (OK 受信) → reserved ──添付成功──→ attached ──「印刷済」──→ printed ──「戻す」──→ undone
             reserved ──例外・タイムアウト──→ failed ──同一宛先・同一面の別イベント＝回収──→ reserved
             reserved（期限切れ・failed 化できず）──回収──→ reserved（所有者・期限を更新）
  cancelled は将来の明示取消用（本票では遷移しない）。
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError

from hub.db import session_scope

# app-state 専用 metadata（alembic env.py の target_metadata list に統合）
metadata = sa.MetaData()

DEFAULT_LAYOUT = "A4_2x5_aone31514"
LABEL_JOB_LEASE_SEC = 600          # reserved の所有期限（既定 10 分・env ではなく定数）

JOB_RESERVED = "reserved"
JOB_ATTACHED = "attached"
JOB_PRINTED = "printed"
JOB_UNDONE = "undone"
JOB_CANCELLED = "cancelled"
JOB_FAILED = "failed"
JOB_STATUSES = (JOB_RESERVED, JOB_ATTACHED, JOB_PRINTED, JOB_UNDONE, JOB_CANCELLED, JOB_FAILED)
ACTIVE_STATUSES = (JOB_RESERVED, JOB_ATTACHED, JOB_FAILED)      # 面を占有中（印刷待ち）

EV_OK = "ok"
EV_PRINTED = "printed"
EV_UNDO = "undo"
EV_NEW_SHEET = "new_sheet"
EVENT_KINDS = (EV_OK, EV_PRINTED, EV_UNDO, EV_NEW_SHEET)

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_BigInt = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_Json = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

label_sheet_control = sa.Table(
    "label_sheet_control", metadata,
    sa.Column("layout", sa.Text, primary_key=True),
    sa.Column("current_sheet_id", sa.Text, nullable=False, server_default=""),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
)

label_sheet_state = sa.Table(
    "label_sheet_state", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("layout", sa.Text, nullable=False),
    sa.Column("sheet_no", sa.Integer, nullable=False, server_default="0"),
    sa.Column("used_faces", _Json, nullable=False),
    sa.Column("history", _Json, nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("updated_by", sa.Text, nullable=False, server_default=""),
    sa.Index("uq_label_sheet_state_layout_sheet_no", "layout", "sheet_no", unique=True),
)

label_print_job = sa.Table(
    "label_print_job", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),      # job_id
    sa.Column("event_id", sa.Text, nullable=False, unique=True),           # 作成イベント（不変）
    sa.Column("app", sa.Text, nullable=False),
    sa.Column("record", sa.Text, nullable=False),
    sa.Column("role", sa.Text, nullable=False),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("face", sa.Integer, nullable=False),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("file_key", sa.Text, nullable=False, server_default=""),
    sa.Column("filename", sa.Text, nullable=False, server_default=""),
    sa.Column("owner_token", sa.Text, nullable=False, server_default=""),
    sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("created_by", sa.Text, nullable=False, server_default=""),
    sa.CheckConstraint(
        "status IN ('reserved', 'attached', 'printed', 'undone', 'cancelled', 'failed')",
        name="ck_label_print_job_status"),
)

label_event = sa.Table(
    "label_event", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
    sa.Column("event_id", sa.Text, nullable=False, unique=True),
    sa.Column("kind", sa.Text, nullable=False),
    sa.Column("job_id", _BigInt, nullable=True),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("payload", _Json, nullable=False),
    sa.Column("result", _Json, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("created_by", sa.Text, nullable=False, server_default=""),
    sa.CheckConstraint("kind IN ('ok', 'printed', 'undo', 'new_sheet')",
                       name="ck_label_event_kind"),
)


class OwnershipLost(RuntimeError):
    """所有権喪失: owner_token が自分でない／status が reserved でない（UPDATE 0 行）"""


@dataclass(frozen=True)
class Job:
    job_id: int
    event_id: str                 # 作成イベント（回収で上書きしない）
    app: str
    record: str
    role: str
    sheet_id: str
    face: int
    status: str
    file_key: str = ""
    filename: str = ""
    owner_token: str = ""
    lease_expires_at: datetime | None = None

    @property
    def target(self) -> tuple[str, str, str]:
        return (self.app, self.record, self.role)

    def lease_active(self, now: datetime | None = None) -> bool:
        """期限内の reserved（処理中＝回収不可）"""
        if self.status != JOB_RESERVED or self.lease_expires_at is None:
            return False
        return _aware(self.lease_expires_at) > (now or _now())

    @property
    def recoverable(self) -> bool:
        """回収できる: failed、または reserved で期限切れ"""
        return self.status == JOB_FAILED or (self.status == JOB_RESERVED and not self.lease_active())


@dataclass(frozen=True)
class Batch:
    """消費バッチ（「印刷済」1 回分）。prior_used: 操作前から used だった面（再印字）"""
    faces: tuple[int, ...]
    job_ids: tuple[int, ...] = ()
    prior_used: tuple[int, ...] = ()

    def to_json(self) -> dict:
        return {"faces": list(self.faces), "job_ids": list(self.job_ids),
                "prior_used": list(self.prior_used)}

    @classmethod
    def from_json(cls, raw) -> "Batch":
        if isinstance(raw, dict):
            return cls(faces=tuple(int(f) for f in raw.get("faces") or []),
                       job_ids=tuple(int(j) for j in raw.get("job_ids") or []),
                       prior_used=tuple(int(f) for f in raw.get("prior_used") or []))
        return cls(faces=tuple(int(f) for f in (raw or [])))     # 旧形式（面のみ）


@dataclass(frozen=True)
class SheetState:
    row_id: int
    sheet_id: str
    layout: str
    used: tuple[int, ...]
    history: tuple[Batch, ...]
    active: tuple[Job, ...] = ()          # このシートの reserved/attached/failed ジョブ（面を占有中）

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
    """reserve_face の結果。
    outcome ∈ duplicate_event / sheet_changed / face_taken / attached / in_progress / recover / reserved"""
    outcome: str
    job: Job | None = None
    state: SheetState | None = None


@dataclass(frozen=True)
class UndoResult:
    faces: tuple[int, ...]          # 直前バッチの面（全部）
    freed: tuple[int, ...]          # used から外した面（prior_used でない面）
    kept_used: tuple[int, ...]      # 再印字のため used のまま残した面
    job_ids: tuple[int, ...]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(dt: datetime) -> datetime:
    """sqlite は naive で返るため UTC とみなす（PostgreSQL は tz 付き）"""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _new_owner() -> str:
    return uuid.uuid4().hex[:12]


def _job(row) -> Job:
    return Job(job_id=int(row.id), event_id=row.event_id, app=row.app, record=row.record,
               role=row.role, sheet_id=row.sheet_id, face=int(row.face), status=row.status,
               file_key=row.file_key or "", filename=row.filename or "",
               owner_token=row.owner_token or "", lease_expires_at=row.lease_expires_at)


async def _active_jobs(session, sheet_id: str) -> list[Job]:
    q = (sa.select(label_print_job)
         .where(label_print_job.c.sheet_id == sheet_id,
                label_print_job.c.status.in_(ACTIVE_STATUSES))
         .order_by(label_print_job.c.id.asc()))
    return [_job(r) for r in (await session.execute(q)).fetchall()]


async def _state_of(session, row) -> SheetState:
    return SheetState(
        row_id=int(row.id), sheet_id=row.sheet_id, layout=row.layout,
        used=tuple(int(f) for f in (row.used_faces or [])),
        history=tuple(Batch.from_json(b) for b in (row.history or [])),
        active=tuple(await _active_jobs(session, row.sheet_id)),
    )


# ── シート管理行（LPF1-03: 全操作の入口ロック） ─────────────────────────────

async def _lock_control(session, layout: str):
    """label_sheet_control の該当 layout 行を FOR UPDATE でロックして返す（sqlite は無視）。
    行が無ければ作る（migration が既定 layout を seed する。ここは別 layout・テスト用の保険）。
    同時作成の競合は主キー違反＝savepoint で吸収し、再度ロック取得。"""
    q = (sa.select(label_sheet_control)
         .where(label_sheet_control.c.layout == layout).with_for_update())
    row = (await session.execute(q)).first()
    if row is not None:
        return row
    try:
        async with session.begin_nested():
            await session.execute(sa.insert(label_sheet_control).values(
                layout=layout, current_sheet_id="", updated_at=_now()))
    except IntegrityError:
        pass
    return (await session.execute(q)).first()


async def _current_row(session, control):
    """管理行が指す現在シート行。未作成（current_sheet_id 空）なら None。"""
    if not control.current_sheet_id:
        return None
    return (await session.execute(
        sa.select(label_sheet_state)
        .where(label_sheet_state.c.layout == control.layout,
               label_sheet_state.c.sheet_id == control.current_sheet_id))).first()


async def _insert_sheet(session, control, user_id: str) -> SheetState:
    """新しいシート行を作り管理行の current_sheet_id を更新する（管理行ロック内で呼ぶ）。
    (layout, sheet_no) 一意＝ロックをすり抜けた同時作成は DB が拒否する。"""
    layout = control.layout
    max_no = (await session.execute(
        sa.select(sa.func.max(label_sheet_state.c.sheet_no))
        .where(label_sheet_state.c.layout == layout))).scalar() or 0
    sheet_no = int(max_no) + 1
    sheet_id = f"S-{sheet_no}"
    result = await session.execute(sa.insert(label_sheet_state).values(
        sheet_id=sheet_id, layout=layout, sheet_no=sheet_no, used_faces=[], history=[],
        updated_at=_now(), updated_by=user_id))
    await session.execute(
        sa.update(label_sheet_control).where(label_sheet_control.c.layout == layout)
        .values(current_sheet_id=sheet_id, updated_at=_now()))
    return SheetState(row_id=int(result.inserted_primary_key[0]), sheet_id=sheet_id,
                      layout=layout, used=(), history=(), active=())


async def _update_sheet(session, state: SheetState, user_id: str, **values) -> None:
    await session.execute(
        sa.update(label_sheet_state)
        .where(label_sheet_state.c.id == state.row_id)
        .values(updated_at=_now(), updated_by=user_id, **values))


async def _find_event(session, event_id: str):
    return (await session.execute(
        sa.select(label_event).where(label_event.c.event_id == event_id))).first()


async def _record_event(session, event_id: str, kind: str, sheet_id: str, user_id: str,
                        payload: dict, result: dict, job_id: int | None = None) -> None:
    await session.execute(sa.insert(label_event).values(
        event_id=event_id, kind=kind, job_id=job_id, sheet_id=sheet_id, payload=payload,
        result=result, created_at=_now(), created_by=user_id))


async def _owned_update(session, job_id: int, owner_token: str, **values) -> int:
    """所有者条件つき更新（LPF1-01）: owner_token が自分 かつ status が reserved。戻り値＝更新行数"""
    result = await session.execute(
        sa.update(label_print_job)
        .where(label_print_job.c.id == job_id,
               label_print_job.c.owner_token == owner_token,
               label_print_job.c.status == JOB_RESERVED)
        .values(updated_at=_now(), **values))
    return int(result.rowcount or 0)


# ── 読み取り ────────────────────────────────────────────────────────────────

async def get_state(layout: str = DEFAULT_LAYOUT) -> SheetState | None:
    """現在シート（管理行が指す行）。未作成なら None。"""
    async with session_scope() as session:
        control = await _lock_control(session, layout)
        row = await _current_row(session, control)
        return await _state_of(session, row) if row else None


async def ensure_state(user_id: str, layout: str = DEFAULT_LAYOUT) -> SheetState:
    """現在シートを返す（無ければ S-1 を作る）。初期作成も管理行ロック内。"""
    async with session_scope() as session:
        control = await _lock_control(session, layout)
        row = await _current_row(session, control)
        if row:
            return await _state_of(session, row)
        return await _insert_sheet(session, control, user_id)


async def get_job(job_id: int) -> Job | None:
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(label_print_job).where(label_print_job.c.id == job_id))).first()
        return _job(row) if row else None


# ── ジョブ状態機械（OK 受信・予約＝1 トランザクション） ──────────────────────

async def reserve_face(event_id: str, *, app: str, record: str, role: str, sheet_id: str,
                       face: int, explicit: bool, user_id: str,
                       layout: str = DEFAULT_LAYOUT,
                       lease_sec: int | None = None) -> Reservation:
    """OK 受信時の予約（LP-01/02/04・LPF1-01/02/03）。1 トランザクション・管理行ロック。
    - event_id が label_event に既存 → duplicate_event（何もしない・全 kind 共通）
    - 現在シートが sheet_id と違う → sheet_changed（予約しない）
    - 同一宛先・同一面の attached → attached（添付済み）
    - 同一宛先・同一面の reserved で期限内 → in_progress（外部処理を始めない）
    - 同一宛先・同一面の failed／期限切れ reserved → recover（所有者・期限を CAS で自分に）
    - 他宛先の占有がその面にある → face_taken
    - used の面は明示指定（explicit）のときだけ「再印字」として予約を許す。提案面が埋まっていれば face_taken
    - それ以外 → job を reserved で挿入（所有者・期限つき）→ reserved
    reserved/recover は label_event(kind=ok) を同一トランザクションで記録する。"""
    if lease_sec is None:
        lease_sec = LABEL_JOB_LEASE_SEC          # 実行時参照（テストで差し替え可）
    async with session_scope() as session:
        dup = await _find_event(session, event_id)
        if dup:
            job = None
            if dup.job_id is not None:
                r = (await session.execute(
                    sa.select(label_print_job).where(label_print_job.c.id == int(dup.job_id)))).first()
                job = _job(r) if r else None
            return Reservation("duplicate_event", job)
        control = await _lock_control(session, layout)
        row = await _current_row(session, control)
        if row is None or row.sheet_id != sheet_id:
            return Reservation("sheet_changed")
        state = await _state_of(session, row)
        target = (app, record, role)
        now = _now()
        payload = {"app": app, "record": record, "role": role, "face": face, "explicit": explicit}
        same = [j for j in state.active if j.face == face and j.target == target]
        if same:
            job = same[0]
            if job.status == JOB_ATTACHED:
                return Reservation("attached", job, state)
            if job.lease_active(now):
                return Reservation("in_progress", job, state)
            owner = _new_owner()
            expires = now + timedelta(seconds=lease_sec)
            result = await session.execute(
                sa.update(label_print_job)
                .where(label_print_job.c.id == job.job_id,
                       label_print_job.c.owner_token == job.owner_token,
                       label_print_job.c.status == job.status)
                .values(status=JOB_RESERVED, owner_token=owner, lease_expires_at=expires,
                        updated_at=now))
            if int(result.rowcount or 0) != 1:
                return Reservation("in_progress", job, state)      # CAS 負け＝別実行が先に回収
            await _record_event(session, event_id, EV_OK, sheet_id, user_id, payload,
                                {"outcome": "recover", "job_id": job.job_id}, job_id=job.job_id)
            recovered = Job(**{**job.__dict__, "status": JOB_RESERVED, "owner_token": owner,
                               "lease_expires_at": expires})
            return Reservation("recover", recovered, state)
        if any(j.face == face for j in state.active):
            return Reservation("face_taken", None, state)
        if face in state.used and not explicit:
            return Reservation("face_taken", None, state)
        owner = _new_owner()
        expires = now + timedelta(seconds=lease_sec)
        result = await session.execute(sa.insert(label_print_job).values(
            event_id=event_id, app=app, record=record, role=role, sheet_id=sheet_id,
            face=face, status=JOB_RESERVED, file_key="", filename="",
            owner_token=owner, lease_expires_at=expires,
            created_at=now, updated_at=now, created_by=user_id))
        job_id = int(result.inserted_primary_key[0])
        await _record_event(session, event_id, EV_OK, sheet_id, user_id, payload,
                            {"outcome": "reserved", "job_id": job_id}, job_id=job_id)
        job = Job(job_id=job_id, event_id=event_id, app=app, record=record, role=role,
                  sheet_id=sheet_id, face=face, status=JOB_RESERVED, owner_token=owner,
                  lease_expires_at=expires)
        return Reservation("reserved", job, state)


async def set_job_file_key(job_id: int, owner_token: str, file_key: str, filename: str) -> None:
    """アップロード成功（添付前）: fileKey を保存して回収時に再利用する（status は reserved のまま）。
    所有者条件つき（0 行＝OwnershipLost）"""
    async with session_scope() as session:
        if await _owned_update(session, job_id, owner_token, file_key=file_key, filename=filename) != 1:
            raise OwnershipLost()


async def clear_job_file_key(job_id: int, owner_token: str) -> None:
    """再利用した fileKey で添付できなかった（期限切れ等）: 次の回収で再アップロードさせる。
    所有者条件つき（0 行＝OwnershipLost）"""
    async with session_scope() as session:
        if await _owned_update(session, job_id, owner_token, file_key="", filename="") != 1:
            raise OwnershipLost()


async def mark_job_attached(job_id: int, owner_token: str, file_key: str, filename: str) -> None:
    """添付成功: reserved → attached（所有者条件つき・0 行＝OwnershipLost）。
    status が reserved 以外（printed/undone 等）からは attached にならない。"""
    async with session_scope() as session:
        if await _owned_update(session, job_id, owner_token, status=JOB_ATTACHED,
                               file_key=file_key, filename=filename) != 1:
            raise OwnershipLost()


async def mark_job_failed(job_id: int, owner_token: str) -> bool:
    """添付処理の途中で例外・タイムアウト: reserved → failed（明示的な失敗確定＝即回収可）。
    所有者条件つき。戻り値＝確定できたか（False でも期限切れで回収される）"""
    async with session_scope() as session:
        return await _owned_update(session, job_id, owner_token, status=JOB_FAILED) == 1


# ── 操作コマンド（event_id 冪等・状態更新と同一トランザクション） ──────────────

async def _dup_result(session, ev, layout: str):
    control = await _lock_control(session, layout)
    row = await _current_row(session, control)
    return (await _state_of(session, row) if row else None), (ev.result or {})


async def consume_printed(event_id: str, user_id: str, face: int | None = None,
                          layout: str = DEFAULT_LAYOUT) -> tuple[str, SheetState | None, list[int]]:
    """「印刷済」: attached の面を used へ（人の操作のみが面を消費する）。face 指定なし＝attached 全部。
    戻り値: (outcome, 状態, 消費した面)。outcome ∈ applied / duplicate_event / nothing / no_sheet。
    ジョブは attached → printed。バッチには {job_ids, faces, prior_used} を保存（LPF1-04）。
    消費 0 件なら状態を変えない（イベントは記録する）。"""
    async with session_scope() as session:
        ev = await _find_event(session, event_id)
        if ev:
            state, result = await _dup_result(session, ev, layout)
            return "duplicate_event", state, list(result.get("faces") or [])
        control = await _lock_control(session, layout)
        row = await _current_row(session, control)
        if not row:
            return "no_sheet", None, []
        state = await _state_of(session, row)
        targets = [j for j in state.active if j.status == JOB_ATTACHED
                   and (face is None or j.face == face)]
        consumed = sorted({j.face for j in targets})
        job_ids = [j.job_id for j in targets]
        if consumed:
            batch = Batch(faces=tuple(consumed), job_ids=tuple(job_ids),
                          prior_used=tuple(f for f in consumed if f in state.used))
            used = list(state.used) + [f for f in consumed if f not in state.used]
            history = [b.to_json() for b in state.history] + [batch.to_json()]
            await _update_sheet(session, state, user_id, used_faces=used, history=history)
            await session.execute(
                sa.update(label_print_job)
                .where(label_print_job.c.id.in_(job_ids),
                       label_print_job.c.status == JOB_ATTACHED)
                .values(status=JOB_PRINTED, updated_at=_now()))
        await _record_event(session, event_id, EV_PRINTED, state.sheet_id, user_id,
                            {"face": face}, {"faces": consumed, "job_ids": job_ids})
        row = await _current_row(session, control)
        return ("applied" if consumed else "nothing"), await _state_of(session, row), consumed


async def undo_last(event_id: str, user_id: str,
                    layout: str = DEFAULT_LAYOUT) -> tuple[str, SheetState | None, UndoResult | None]:
    """「戻す」: 直前の消費バッチを取り消す（LPF1-04）。
    prior_used=False の面だけ used から外し、prior_used=True（再印字）の面は used のまま。
    バッチのジョブは printed → undone（再印字ジョブも undone・面は空きにしない）。
    戻り値: (outcome, 状態, UndoResult)。outcome ∈ applied / duplicate_event / nothing / no_sheet。"""
    async with session_scope() as session:
        ev = await _find_event(session, event_id)
        if ev:
            state, result = await _dup_result(session, ev, layout)
            res = UndoResult(tuple(result.get("faces") or ()), tuple(result.get("freed") or ()),
                             tuple(result.get("kept_used") or ()), tuple(result.get("job_ids") or ()))
            return "duplicate_event", state, (res if res.faces else None)
        control = await _lock_control(session, layout)
        row = await _current_row(session, control)
        if not row:
            return "no_sheet", None, None
        state = await _state_of(session, row)
        res = None
        if state.history:
            batch = state.history[-1]
            freed = tuple(f for f in batch.faces if f not in batch.prior_used)
            kept = tuple(f for f in batch.faces if f in batch.prior_used)
            used = [f for f in state.used if f not in freed]
            history = [b.to_json() for b in state.history[:-1]]
            await _update_sheet(session, state, user_id, used_faces=used, history=history)
            if batch.job_ids:
                await session.execute(
                    sa.update(label_print_job)
                    .where(label_print_job.c.id.in_(list(batch.job_ids)),
                           label_print_job.c.status == JOB_PRINTED)
                    .values(status=JOB_UNDONE, updated_at=_now()))
            res = UndoResult(batch.faces, freed, kept, batch.job_ids)
        result = ({"faces": list(res.faces), "freed": list(res.freed), "kept_used": list(res.kept_used),
                   "job_ids": list(res.job_ids)} if res else {"faces": []})
        await _record_event(session, event_id, EV_UNDO, state.sheet_id, user_id, {}, result)
        row = await _current_row(session, control)
        return ("applied" if res else "nothing"), await _state_of(session, row), res


async def new_sheet(event_id: str, user_id: str,
                    layout: str = DEFAULT_LAYOUT) -> tuple[str, SheetState]:
    """「新しいシート」: 使用済みを空にした新しい行を開始する（旧行は残す）。管理行ロック内で作成し
    current_sheet_id を更新。同 event_id の再配送はシートを増やさず当時の結果を返す。"""
    async with session_scope() as session:
        ev = await _find_event(session, event_id)
        if ev:
            state, _ = await _dup_result(session, ev, layout)
            return "duplicate_event", state
        control = await _lock_control(session, layout)
        state = await _insert_sheet(session, control, user_id)
        await _record_event(session, event_id, EV_NEW_SHEET, state.sheet_id, user_id, {},
                            {"sheet_id": state.sheet_id})
        return "applied", state


def status_text(state: SheetState, per_page: int) -> str:
    """残量の復唱文（レコード番号・面番号・残量のみ＝PII なし・D8）。"""
    used = "・".join(str(f) for f in sorted(state.used)) or "なし"
    pending = "・".join(str(f) for f in state.pending) or "なし"
    return (f"シート {state.sheet_id}（{per_page} 面）: 使用済み {len(state.used)} 面"
            f"（{used}）・印刷待ち {len(state.pending)} 面（{pending}）・"
            f"残り {state.remaining(per_page)} 面")
