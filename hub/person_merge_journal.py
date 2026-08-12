"""person_merge_journal — 名寄せ統合の immutable 操作台帳（RV-08 §3.2a・裁定⑦(B)）

正本: docs/design-drafts/DRAFT_RV08_SOFT_MERGE.md §3.2a。

- soft merge は複数 update の逐次実行（参照付け替え→勝者更新→敗者無効化）であり
  途中失敗＝中間状態が起き得る。その回収の器が本台帳（immutable 追記・P3-001 流儀）。
- 1 回の統合実行に **operation_id** を発番し、監査JSON・封筒 detail・台帳行に貫通
  させる（どの操作の中間状態かを機械判別可能にする）。
- **PII 非保持（RV10 準拠）**: 書込み前後の**値そのもの**（人物レコード verbatim）は
  kintone 封筒添付の監査JSONが正（復元の原資・§10.1）。本台帳の payload には
  再実行照合用の **fingerprint（正規化 SHA-256）と record ID のみ**を保存し、
  氏名・続柄等の PII を DB へ持ち込まない（derivation_run §3.5 と同じ規律）。
- stage 閉集合: preimage（書込み前・照合基準）/ postimage（無効化完了・完了マーク）/
  restore（過去物理削除分の復元 CLI の lineage 記録・§3.2）。
- 再実行の照合（§3.2a）: preimage 行が存在し postimage 行が無い operation が
  「未完了」。現在値の fingerprint を preimage/expected-post と照合し、
  一致（未適用）→続行・適用済み→skip・不一致→write 0 で要確認。
"""

import hashlib
import json
from contextlib import asynccontextmanager
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from hub.db import session_scope
from hub.derivation_models import immutable_trigger_ddl

_BigIntPK = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

STAGE_PREIMAGE = "preimage"
STAGE_POSTIMAGE = "postimage"
STAGE_RESTORE = "restore"


class MergeJournalError(RuntimeError):
    """操作台帳への記録失敗（台帳なしで kintone へ書かない・fail-closed の分類名）。"""


class PersonMergeJournalBase(DeclarativeBase):
    """app-state DB 専用 metadata（L03: 他 Base と相乗りしない・P3-001 と同じ裁定）。"""


class PersonMergeOperation(PersonMergeJournalBase):
    """統合操作の追記行（UPDATE/DELETE 禁止・訂正不能）。"""

    __tablename__ = "person_merge_operation"
    __table_args__ = (
        sa.CheckConstraint(
            "stage IN ('preimage', 'postimage', 'restore')",
            name="ck_person_merge_operation_stage"),
        # 同一 operation の同一 stage は 1 行のみ（再実行の重複追記を DB で拒否）
        sa.UniqueConstraint("operation_id", "stage",
                            name="uq_person_merge_operation_stage"),
        sa.Index("ix_person_merge_operation_envelope",
                 "envelope_record_id", "pair_key"),
    )

    id: Mapped[int] = mapped_column(_BigIntPK, primary_key=True, autoincrement=True)
    operation_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    pair_key: Mapped[str] = mapped_column(sa.Text, nullable=False)
    envelope_record_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    winner_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    loser_id: Mapped[str] = mapped_column(sa.Text, nullable=False)
    stage: Mapped[str] = mapped_column(sa.Text, nullable=False)
    payload: Mapped[dict] = mapped_column(_JSON, nullable=False)  # fingerprint/ID のみ
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())


# ── ORM 層の immutable 強制（P3-001 と同型・(i)/(ii) の二重） ─────────────────

def _reject_mutation(mapper, connection, target):  # noqa: ARG001
    raise RuntimeError(
        "person_merge_operation is append-only (UPDATE/DELETE 禁止)")


sa.event.listen(PersonMergeOperation, "before_update", _reject_mutation)
sa.event.listen(PersonMergeOperation, "before_delete", _reject_mutation)

# ── DB 層の immutable 強制（trigger・create_all/migration 双方に付与） ────────

_tbl = PersonMergeJournalBase.metadata.tables["person_merge_operation"]
_ddl = immutable_trigger_ddl("person_merge_operation")
for _stmt in _ddl["sqlite"]:
    sa.event.listen(_tbl, "after_create",
                    sa.DDL(_stmt).execute_if(dialect="sqlite"))
for _stmt in _ddl["postgresql"]:
    sa.event.listen(_tbl, "after_create",
                    sa.DDL(_stmt).execute_if(dialect="postgresql"))


# ── fingerprint（照合の正規形・PII 非保持の要） ──────────────────────────────

# kintone GET レコードのシステムフィールド型（fingerprint 対象外）。
# $revision/更新系は書込みで必ず動くため照合から除外する
_VOLATILE_TYPES = {"RECORD_NUMBER", "__ID__", "__REVISION__", "CREATOR",
                   "CREATED_TIME", "MODIFIER", "UPDATED_TIME", "STATUS",
                   "STATUS_ASSIGNEE", "CATEGORY"}
# 統合日時は operation 毎に異なる実行時刻＝照合対象外（RV-08 実装判断）
_EXCLUDED_FIELDS = {"統合日時"}


def _canon_cell(cell: dict):
    if cell.get("type") == "SUBTABLE":
        rows = []
        for row in cell.get("value") or []:
            rows.append(tuple(sorted(
                (c, str((v or {}).get("value") or ""))
                for c, v in (row.get("value") or {}).items())))
        return sorted(rows)
    value = cell.get("value")
    if isinstance(value, list):   # 添付・複数選択等は文字列化した集合で安定化
        return sorted(str(v) for v in value)
    return str(value or "")


def record_fingerprint(record: dict) -> str:
    """kintone GET 形レコードの正規化 SHA-256（照合用・値は台帳へ保存しない）。"""
    canon = {}
    for code, cell in record.items():
        if not isinstance(cell, dict) or cell.get("type") in _VOLATILE_TYPES:
            continue
        if code in _EXCLUDED_FIELDS or code.startswith("$"):
            continue
        canon[code] = _canon_cell(cell)
    data = json.dumps(canon, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def fingerprint_with_updates(record: dict, updates: dict) -> str:
    """record に update payload を仮適用した後の fingerprint（expected-post）。
    updates は update_record と同じフラット形（サブテーブルは payload 行形）。"""
    merged = dict(record)
    for code, value in updates.items():
        if isinstance(value, list):
            merged[code] = {"type": "SUBTABLE", "value": value}
        else:
            base = merged.get(code) or {}
            merged[code] = {"type": base.get("type") or "SINGLE_LINE_TEXT",
                            "value": value}
    return record_fingerprint(merged)


# ── 追記・照会（読み書きは本モジュール経由のみ） ─────────────────────────────

async def stages_in_session(session, operation_id: str) -> dict[str, dict]:
    """operation_id の stage → payload 写像（呼出し元の session/txn 内で読む版。
    RV08-IMPL-08 の直列化区間からロック下の再読に使う）。"""
    t = PersonMergeOperation.__table__
    rows = (await session.execute(
        sa.select(t.c.stage, t.c.payload)
        .where(t.c.operation_id == operation_id)
        .order_by(t.c.id.asc()))).all()
    return {r.stage: r.payload for r in rows}


async def insert_stage_in_session(session, *, operation_id: str, pair_key: str,
                                  envelope_record_id: str, winner_id: str,
                                  loser_id: str, stage: str,
                                  payload: dict) -> None:
    """呼出し元の session/txn 内で 1 行追記する（commit は呼出し元＝
    RV08-IMPL-08 の直列化区間ではロック解放と同時に確定する）。"""
    session.add(PersonMergeOperation(
        operation_id=operation_id, pair_key=pair_key,
        envelope_record_id=envelope_record_id,
        winner_id=winner_id, loser_id=loser_id,
        stage=stage, payload=payload))
    await session.flush()   # UNIQUE 競合をこの場で顕在化（黙って commit 時に落とさない）


async def record_stage(*, operation_id: str, pair_key: str,
                       envelope_record_id: str, winner_id: str, loser_id: str,
                       stage: str, payload: dict) -> None:
    """台帳へ 1 行追記する（独立 txn）。失敗は MergeJournalError（fail-closed）。"""
    try:
        async with session_scope() as session:
            await insert_stage_in_session(
                session, operation_id=operation_id, pair_key=pair_key,
                envelope_record_id=envelope_record_id,
                winner_id=winner_id, loser_id=loser_id,
                stage=stage, payload=payload)
    except Exception as e:
        raise MergeJournalError(type(e).__name__) from e


async def find_stages(operation_id: str) -> dict[str, dict]:
    """operation_id の stage → payload 写像（(operation_id, stage) UNIQUE のため
    各 stage 高々 1 行）。復元 CLI の決定的 operation_id 照合（RV08-IMPL-01/02）が
    使う。失敗は MergeJournalError（判定不能のまま書かせない・fail-closed）。"""
    try:
        async with session_scope() as session:
            return await stages_in_session(session, operation_id)
    except Exception as e:
        raise MergeJournalError(type(e).__name__) from e


@asynccontextmanager
async def restore_serialization(operation_id: str):
    """復元 CLI の臨界区間の直列化（RV08-IMPL-08）。stage 閉集合は不変——
    ロックは既存行/既存テーブルの DB 機構のみで実現する:

    - **postgresql**: 当該 operation の既存行（pending の preimage 行）への
      `SELECT ... FOR UPDATE`＝行ロック。競合プロセスは commit まで待機する
      （FOR UPDATE は UPDATE を発行しないため immutable trigger に抵触しない）。
    - **sqlite**: 0 行 UPDATE（`WHERE 1=0`）で RESERVED（DB 単位の書込み）ロックを
      先頭で取得＝以後の書込み txn を直列化。行に触れないため BEFORE UPDATE
      trigger（FOR EACH ROW）は発火しない。busy timeout は driver 既定（5 秒）。

    区間内で stages_in_session（ロック下の再読）→ kintone create/relink →
    insert_stage_in_session（完了記録）を行い、`async with` の正常終了＝commit で
    ロックを解放する。例外時は rollback（完了記録は残らない・preimage は独立 txn
    で保存済みのため回収可能性は不変）。"""
    async with session_scope() as session:
        t = PersonMergeOperation.__table__
        if session.get_bind().dialect.name == "postgresql":
            await session.execute(
                sa.select(t.c.id).where(t.c.operation_id == operation_id)
                .with_for_update())
        else:
            await session.execute(
                sa.update(t).where(sa.text("1=0"))
                .values(operation_id=t.c.operation_id))
        yield session


async def list_open_operations(after_id: int = 0, limit: int = 100) -> list[dict]:
    """未完了 operation（preimage あり・postimage なし）の**カーソルページング**
    列挙（RV08-IMPL-10・restore 系は対象外＝merge 操作のみ）。
    未完了判定は SQL 側（NOT EXISTS）で行い、`id > after_id` のカーソルで
    全件を有限回で走査できる（打ち切りなし）。
    Returns: [{"row_id","operation_id","pair_key","envelope_record_id",
               "winner_id","loser_id","payload"}]。失敗は MergeJournalError。"""
    try:
        async with session_scope() as session:
            t = PersonMergeOperation.__table__
            d = PersonMergeOperation.__table__.alias("d")
            rows = (await session.execute(
                sa.select(t.c.id, t.c.operation_id, t.c.pair_key,
                          t.c.envelope_record_id, t.c.winner_id,
                          t.c.loser_id, t.c.payload)
                .where(t.c.stage == STAGE_PREIMAGE,
                       t.c.id > int(after_id),
                       sa.not_(t.c.operation_id.like("restore-%")),
                       sa.not_(sa.exists().where(
                           (d.c.operation_id == t.c.operation_id)
                           & d.c.stage.in_((STAGE_POSTIMAGE, STAGE_RESTORE)))))
                .order_by(t.c.id.asc())
                .limit(int(limit)))).all()
    except Exception as e:
        raise MergeJournalError(type(e).__name__) from e
    return [{"row_id": r.id, "operation_id": r.operation_id,
             "pair_key": r.pair_key,
             "envelope_record_id": r.envelope_record_id,
             "winner_id": r.winner_id, "loser_id": r.loser_id,
             "payload": r.payload} for r in rows]


async def find_open_operation(envelope_record_id: str,
                              pair_key: str) -> dict | None:
    """未完了 operation（preimage あり・postimage なし）の最新 1 件を返す。
    Returns: {"operation_id", "payload"}（無ければ None）。
    失敗は MergeJournalError（判定不能のまま書かせない）。"""
    try:
        async with session_scope() as session:
            t = PersonMergeOperation.__table__
            rows = (await session.execute(
                sa.select(t.c.operation_id, t.c.stage, t.c.payload)
                .where(t.c.envelope_record_id == envelope_record_id,
                       t.c.pair_key == pair_key)
                .order_by(t.c.id.asc()))).all()
    except Exception as e:
        raise MergeJournalError(type(e).__name__) from e
    done = {r.operation_id for r in rows if r.stage == STAGE_POSTIMAGE}
    open_ops = [r for r in rows
                if r.stage == STAGE_PREIMAGE and r.operation_id not in done]
    if not open_ops:
        return None
    last = open_ops[-1]
    return {"operation_id": last.operation_id, "payload": last.payload}
