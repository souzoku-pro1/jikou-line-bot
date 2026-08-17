"""qa_store — Q-BATCH-1 B: Q&A 台帳（質問・回答・出典・コストの保存層）

正本: ③（docs/plan/2026-08_execution-plan.md）項目10＋12.2-1（Q&A 一覧=
「Q&A 台帳のビュー」）＋大野裁定 2026-08-17（Q 機能は PWA 搭載）。

- **業務データと分離した専用テーブル**（kintone・既存業務 DB 表へは一切
  触れない——本 module は kintone を import しない・機械検査で pin）。
- immutable trigger は置かない（票の指定・監査台帳でなく参照用ビューの実体）。
- 回答は常に「参考情報であり確定ではない」定型文とセットで保存する
  （定型文の実体は hub.webapp_q.DISCLAIMER・保存側は素通し）。
- **PII 規律**: 本 module は logging を一切 import しない（質問・回答の
  ログ反射経路を構造的に持たない）。
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from hub.db import session_scope

# app-state 専用 metadata（alembic env.py の target_metadata list に統合済み）
metadata = sa.MetaData()

# 保存 status の閉集合（webapp_q の返す status と一字一句一致・テストで pin）
STATUS_VALUES = ("ok", "no_source", "error")

qa_record = sa.Table(
    "qa_record", metadata,
    sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
              primary_key=True, autoincrement=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("user_id", sa.Text, nullable=False),
    sa.Column("question", sa.Text, nullable=False),
    sa.Column("answer", sa.Text, nullable=False),
    sa.Column("status", sa.Text, nullable=False),
    # 出典配列（app・レコード番号・リンク）と注記配列（未確定・信頼度格付け）
    sa.Column("sources", sa.JSON().with_variant(
        postgresql.JSONB(), "postgresql"), nullable=False),
    sa.Column("notes", sa.JSON().with_variant(
        postgresql.JSONB(), "postgresql"), nullable=False),
    sa.Column("model", sa.Text, nullable=False),
    sa.Column("input_tokens", sa.Integer, nullable=False, server_default="0"),
    sa.Column("output_tokens", sa.Integer, nullable=False, server_default="0"),
    sa.Column("cache_read_tokens", sa.Integer, nullable=False,
              server_default="0"),
    # コスト概算は文字列（Decimal 由来・float 非経由。不明モデルは "unknown"）
    sa.Column("cost_usd", sa.Text, nullable=False),
    sa.Column("elapsed_ms", sa.Integer, nullable=False, server_default="0"),
    sa.CheckConstraint(
        "status IN ('ok', 'no_source', 'error')", name="ck_qa_record_status"),
)


# Q-CHAT-1(B): 話題リセット境界。リセット時点の qa_record 最大 id を保存し、
# 会話文脈はこの境界より後（id >）の行のみ参照する（id 単調増加基準＝時計
# 精度に依存しない）。qa_record 本体・status 閉集合には触れない
qa_topic_reset = sa.Table(
    "qa_topic_reset", metadata,
    sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
              primary_key=True, autoincrement=True),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("user_id", sa.Text, nullable=False),
    sa.Column("last_qa_id", sa.BigInteger().with_variant(
        sa.Integer(), "sqlite"), nullable=False, server_default="0"),
)


async def save_qa(*, user_id: str, question: str, answer: str, status: str,
                  sources: list, notes: list, model: str, input_tokens: int,
                  output_tokens: int, cache_read_tokens: int, cost_usd: str,
                  elapsed_ms: int) -> int:
    """1 問 1 行で保存し id を返す。status は閉集合のみ（DB CHECK でも防御）。"""
    async with session_scope() as session:
        result = await session.execute(sa.insert(qa_record).values(
            user_id=user_id, question=question, answer=answer, status=status,
            sources=sources, notes=notes, model=model,
            input_tokens=input_tokens, output_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens, cost_usd=cost_usd,
            elapsed_ms=elapsed_ms))
        return int(result.inserted_primary_key[0])


def _row_to_dict(row) -> dict:
    return {
        "id": row.id,
        "created_at": row.created_at.isoformat() if row.created_at else "",
        "user_id": row.user_id,
        "question": row.question,
        "answer": row.answer,
        "status": row.status,
        "sources": row.sources,
        "notes": row.notes,
        "model": row.model,
        "input_tokens": row.input_tokens,
        "output_tokens": row.output_tokens,
        "cache_read_tokens": row.cache_read_tokens,
        "cost_usd": row.cost_usd,
        "elapsed_ms": row.elapsed_ms,
    }


async def list_qa(*, limit: int, offset: int) -> list[dict]:
    """Q&A 台帳のビュー（新しい順・ページング。③12.2-1）。"""
    async with session_scope() as session:
        rows = (await session.execute(
            sa.select(qa_record).order_by(qa_record.c.id.desc())
            .limit(limit).offset(offset))).fetchall()
        return [_row_to_dict(r) for r in rows]


async def save_topic_reset(*, user_id: str) -> int:
    """Q-CHAT-1(B): 話題リセットを記録し id を返す。境界は現時点の qa_record
    最大 id（無ければ 0）＝以降の質問は境界より後の行だけを文脈にする。"""
    async with session_scope() as session:
        last = (await session.execute(sa.select(
            sa.func.coalesce(sa.func.max(qa_record.c.id), 0)))).scalar()
        result = await session.execute(sa.insert(qa_topic_reset).values(
            user_id=user_id, last_qa_id=int(last or 0)))
        return int(result.inserted_primary_key[0])


async def latest_reset_boundary() -> int | None:
    """最新の話題リセット境界（qa_record id）。リセット未実行は None。"""
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(qa_topic_reset.c.last_qa_id)
            .order_by(qa_topic_reset.c.id.desc()).limit(1))).first()
        return int(row[0]) if row else None


async def list_context_qa(*, limit: int) -> list[dict]:
    """Q-CHAT-1(A): 会話文脈用の直近 Q&A（新しい順・最大 limit 件）。
    最後のリセット境界より後の行のみ・status=error（回答生成が完了しなかった
    もの）は文脈価値が無いため除外。履歴は文脈情報であり出典ではない。"""
    async with session_scope() as session:
        row = (await session.execute(
            sa.select(qa_topic_reset.c.last_qa_id)
            .order_by(qa_topic_reset.c.id.desc()).limit(1))).first()
        query = sa.select(qa_record).where(qa_record.c.status != "error")
        if row is not None:
            query = query.where(qa_record.c.id > int(row[0]))
        rows = (await session.execute(
            query.order_by(qa_record.c.id.desc()).limit(limit))).fetchall()
        return [_row_to_dict(r) for r in rows]
