"""label_sheet — LABEL-PRINT-1 D2: ラベルシート残量の永続状態（durable DB）

正本: LABEL-PRINT-1 票（裁定 D2・第 2 段 1）。設計サーベイ Desktop\\claude\\ラベル印字_設計.md §6.2。

- 置き場所は既存の durable DB（hub/db.py・alembic）。kintone に新アプリは作らない。
- 機械は「次の空き面」を提案するだけで、面を消費するのは人の「印刷済」のみ。
  「戻す」で直前の消費を取り消し、「新しいシート」で使用済みを空にする。
  「残量」で状態を返す。
- 1 シート = 1 行（sheet_id は S-1, S-2, …）。現在シートは同レイアウトの最新行。
  行を消さない（過去シートは履歴として残る）。
- used_faces: 使用済み面（人が印刷済にした面）。pending_faces: PDF を添付済みで
  印刷待ちの面（提案時に予約＝次の空き面の候補から外す）。history: 消費バッチの
  履歴（「戻す」は直前バッチを取り消す）。
- 冪等: label_print_job に冪等キー（label_print:{app}:{record}:{role}:{sheet}:{face}）
  を一意で保存し、同キー再要求は添付を繰り返さない。
- **PII 規律**: 本 module は logging を import しない。氏名・住所は保存しない
  （レコード番号・面番号・ファイル名・fileKey のみ）。
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from hub.db import session_scope

# app-state 専用 metadata（alembic env.py の target_metadata list に統合）
metadata = sa.MetaData()

DEFAULT_LAYOUT = "A4_2x5_aone31514"

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_Json = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

label_sheet_state = sa.Table(
    "label_sheet_state", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("layout", sa.Text, nullable=False),
    sa.Column("used_faces", _Json, nullable=False),
    sa.Column("pending_faces", _Json, nullable=False),
    sa.Column("history", _Json, nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("updated_by", sa.Text, nullable=False, server_default=""),
)

label_print_job = sa.Table(
    "label_print_job", metadata,
    sa.Column("id", _BigIntPK, primary_key=True, autoincrement=True),
    sa.Column("idempotency_key", sa.Text, nullable=False, unique=True),
    sa.Column("sheet_id", sa.Text, nullable=False),
    sa.Column("face", sa.Integer, nullable=False),
    sa.Column("filename", sa.Text, nullable=False),
    sa.Column("file_key", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("created_by", sa.Text, nullable=False, server_default=""),
)


@dataclass(frozen=True)
class SheetState:
    row_id: int
    sheet_id: str
    layout: str
    used: tuple[int, ...]
    pending: tuple[int, ...]
    history: tuple[tuple[int, ...], ...]

    def free_faces(self, per_page: int) -> list[int]:
        """提案可能な面（使用済み・印刷待ちを除く・昇順）。"""
        taken = set(self.used) | set(self.pending)
        return [f for f in range(1, per_page + 1) if f not in taken]

    def next_free(self, per_page: int) -> int | None:
        free = self.free_faces(per_page)
        return free[0] if free else None

    def remaining(self, per_page: int) -> int:
        return len(self.free_faces(per_page))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _to_state(row) -> SheetState:
    return SheetState(
        row_id=int(row.id), sheet_id=row.sheet_id, layout=row.layout,
        used=tuple(int(f) for f in (row.used_faces or [])),
        pending=tuple(int(f) for f in (row.pending_faces or [])),
        history=tuple(tuple(int(f) for f in batch) for batch in (row.history or [])),
    )


async def _latest(session, layout: str):
    return (await session.execute(
        sa.select(label_sheet_state)
        .where(label_sheet_state.c.layout == layout)
        .order_by(label_sheet_state.c.id.desc()).limit(1))).first()


async def _insert_sheet(session, layout: str, user_id: str) -> SheetState:
    count = (await session.execute(
        sa.select(sa.func.count()).select_from(label_sheet_state)
        .where(label_sheet_state.c.layout == layout))).scalar() or 0
    sheet_id = f"S-{int(count) + 1}"
    result = await session.execute(sa.insert(label_sheet_state).values(
        sheet_id=sheet_id, layout=layout, used_faces=[], pending_faces=[],
        history=[], updated_at=_now(), updated_by=user_id))
    return SheetState(row_id=int(result.inserted_primary_key[0]), sheet_id=sheet_id,
                      layout=layout, used=(), pending=(), history=())


async def get_state(layout: str = DEFAULT_LAYOUT) -> SheetState | None:
    """現在シート（最新行）。未作成なら None。"""
    async with session_scope() as session:
        row = await _latest(session, layout)
        return _to_state(row) if row else None


async def ensure_state(user_id: str, layout: str = DEFAULT_LAYOUT) -> SheetState:
    """現在シートを返す（無ければ S-1 を作る）。"""
    async with session_scope() as session:
        row = await _latest(session, layout)
        if row:
            return _to_state(row)
        return await _insert_sheet(session, layout, user_id)


async def new_sheet(user_id: str, layout: str = DEFAULT_LAYOUT) -> SheetState:
    """「新しいシート」: 使用済みを空にした新しい行を開始する（旧行は残す）。"""
    async with session_scope() as session:
        return await _insert_sheet(session, layout, user_id)


async def _update(session, state: SheetState, user_id: str, **values) -> None:
    await session.execute(
        sa.update(label_sheet_state)
        .where(label_sheet_state.c.id == state.row_id)
        .values(updated_at=_now(), updated_by=user_id, **values))


async def mark_pending(face: int, user_id: str, layout: str = DEFAULT_LAYOUT) -> SheetState:
    """PDF 添付済み＝印刷待ちの面として予約する（消費はしない）。"""
    async with session_scope() as session:
        row = await _latest(session, layout)
        state = _to_state(row) if row else await _insert_sheet(session, layout, user_id)
        pending = list(state.pending)
        if face not in pending:
            pending.append(face)
        await _update(session, state, user_id, pending_faces=pending)
        return SheetState(state.row_id, state.sheet_id, state.layout, state.used,
                          tuple(pending), state.history)


async def consume_printed(user_id: str, face: int | None = None,
                          layout: str = DEFAULT_LAYOUT) -> tuple[SheetState | None, list[int]]:
    """「印刷済」: 印刷待ちの面を使用済みへ移す（人の操作のみが面を消費する）。
    face 指定なし＝印刷待ち全部。指定あり＝その面だけ（印刷待ちに無い面は消費しない）。
    戻り値: (更新後の状態, 消費した面)。消費 0 件なら状態は変えない。"""
    async with session_scope() as session:
        row = await _latest(session, layout)
        if not row:
            return None, []
        state = _to_state(row)
        pending = list(state.pending)
        consumed = [f for f in pending if face is None or f == face]
        if not consumed:
            return state, []
        used = list(state.used) + [f for f in consumed if f not in state.used]
        rest = [f for f in pending if f not in consumed]
        history = [list(b) for b in state.history] + [consumed]
        await _update(session, state, user_id, used_faces=used, pending_faces=rest,
                      history=history)
        new = SheetState(state.row_id, state.sheet_id, state.layout, tuple(used),
                         tuple(rest), tuple(tuple(b) for b in history))
        return new, consumed


async def undo_last(user_id: str, layout: str = DEFAULT_LAYOUT) -> tuple[SheetState | None, list[int]]:
    """「戻す」: 直前の消費バッチを取り消す（面は印刷待ちには戻さず、空きに戻す）。
    戻り値: (更新後の状態, 取り消した面)。履歴が無ければ [] で状態は変えない。"""
    async with session_scope() as session:
        row = await _latest(session, layout)
        if not row:
            return None, []
        state = _to_state(row)
        if not state.history:
            return state, []
        history = [list(b) for b in state.history]
        last = history.pop()
        used = [f for f in state.used if f not in last]
        await _update(session, state, user_id, used_faces=used, history=history)
        new = SheetState(state.row_id, state.sheet_id, state.layout, tuple(used),
                         state.pending, tuple(tuple(b) for b in history))
        return new, list(last)


async def find_job(idempotency_key: str) -> dict | None:
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(label_print_job)
            .where(label_print_job.c.idempotency_key == idempotency_key))).first()
        if not row:
            return None
        return {"sheet_id": row.sheet_id, "face": int(row.face),
                "filename": row.filename, "file_key": row.file_key}


async def record_job(idempotency_key: str, *, sheet_id: str, face: int, filename: str,
                     file_key: str, user_id: str) -> None:
    async with session_scope() as session:
        await session.execute(sa.insert(label_print_job).values(
            idempotency_key=idempotency_key, sheet_id=sheet_id, face=face,
            filename=filename, file_key=file_key, created_at=_now(), created_by=user_id))


def status_text(state: SheetState, per_page: int) -> str:
    """残量の復唱文（レコード番号・面番号・残量のみ＝PII なし・D8）。"""
    used = "・".join(str(f) for f in sorted(state.used)) or "なし"
    pending = "・".join(str(f) for f in state.pending) or "なし"
    return (f"シート {state.sheet_id}（{per_page} 面）: 使用済み {len(state.used)} 面"
            f"（{used}）・印刷待ち {len(state.pending)} 面（{pending}）・"
            f"残り {state.remaining(per_page)} 面")
