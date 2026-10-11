"""reply_exemplar — JIKOU-REPLY-Q1b: 大野の返信を「手本」として匿名化して保存する（§10-7・§12 Q1b）

正本: Desktop/claude/時効LINEボット_返信規則_v1.4.md §7（大野の返信の扱い）・§10-7（手本は
匿名化済み文だけ・検査に落ちた文は手本にしない・分類 ID は閉集合）・§10-10（新設 DB 表・
操作・状態遷移の履歴を残す RV-08・起動時に migration を走らせない）。QUEUE-2026-10-11-B Q-3 (d)。
fix1（Codex R-JIKOU-REPLY-Q1b-1）:
- BR-04: 匿名化・残留検査は hub/pii_scan（tools/line_log_anonymize と共通の検査器）に委譲。
  「確実に匿名化できない文は保存しない」（司令塔裁定）＝残留検査に落ちたら保存せず件数ログのみ
  （本文を出さない）。送信は成功扱い（呼び出し側は保存結果を画面に出すだけ）。
- BR-09: 表を裁定の定義に合わせる——category_id（分類 ID・閉集合は Q3b で確定・今は NULL）・
  source_text_version（送信操作記録の text_version＝原文の sha256・原文は持たない）・
  approved_by / approved_at（「手本にしてよい」と印を付けた主体と時刻）・active（無効化は
  削除ではなく false＝RV-08）・履歴表 reply_exemplar_history（created / used / deactivated）。

- 表 reply_exemplar: id・business・conversation_ref（send_ledger.conversation.ref＝不透明参照・
  無ければ NULL）・inbound_text（匿名化済み）・human_reply（匿名化済み）・creditor_name（業者名
  は保持）・stage（相談 / 受任後）・created_at・used_count（下書き生成での使用回数・Q3b が進める）・
  source_op_id（送信操作記録 op_id）・category_id・source_text_version・approved_by・approved_at・active。
- DB 未設定（DATABASE_URL なし）は保存しない（skipped）。DB 例外は固定語彙のログだけ（failed）。
  手本の保存失敗は送信や App 28 記録に影響しない（呼び出し側で順序を固定）。
- RV-10: 本表に LINE userId・原文は置かない。ログに本文・氏名を出さない。
"""

import datetime
import logging

import sqlalchemy as sa

from hub import db, pii_scan
from hub.db import DatabaseNotConfigured, session_scope
from hub.pii_scan import (MASK_ACCOUNT, MASK_ADDRESS, MASK_DIGITS, MASK_EMAIL,  # noqa: F401（再公開）
                          MASK_LINE_USER_ID, MASK_NAME, MASK_PHONE, MASK_POSTAL)
from hub.redact import emit

logger = logging.getLogger("hub.reply_exemplar")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

STAGE_CONSULT = "相談"
STAGE_ENGAGED = "受任後"
STAGES = (STAGE_CONSULT, STAGE_ENGAGED)
TABLE_NAME = "reply_exemplar"
HISTORY_TABLE_NAME = "reply_exemplar_history"
MAX_TEXT_CHARS = 4000
APPROVED_BY_HUMAN = "human"
ACTION_CREATED = "created"
ACTION_USED = "used"
ACTION_DEACTIVATED = "deactivated"
ACTIONS = (ACTION_CREATED, ACTION_USED, ACTION_DEACTIVATED)

reply_exemplar = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("conversation_ref", sa.Text, nullable=True),
    sa.Column("inbound_text", sa.Text, nullable=False),
    sa.Column("human_reply", sa.Text, nullable=False),
    sa.Column("creditor_name", sa.Text, nullable=True),
    sa.Column("stage", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("used_count", sa.Integer, nullable=False, server_default="0"),
    sa.Column("source_op_id", sa.Text, nullable=True),
    # fix1 BR-09
    sa.Column("category_id", sa.Text, nullable=True),
    sa.Column("source_text_version", sa.Text, nullable=True),
    sa.Column("approved_by", sa.Text, nullable=False, server_default=APPROVED_BY_HUMAN),
    sa.Column("approved_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("active", sa.Boolean, nullable=False, server_default=sa.true()),
    sa.CheckConstraint("stage IN ('相談', '受任後')", name="ck_reply_exemplar_stage"),
    sa.Index("ix_reply_exemplar_lookup", "business", "stage", "created_at"),
    sa.Index("ix_reply_exemplar_active", "business", "active"),
)

reply_exemplar_history = sa.Table(
    HISTORY_TABLE_NAME, metadata,
    sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("exemplar_id", _BIG, sa.ForeignKey("reply_exemplar.id"), nullable=False),
    sa.Column("action", sa.Text, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("note", sa.Text, nullable=True),
    sa.CheckConstraint("action IN ('created', 'used', 'deactivated')",
                       name="ck_reply_exemplar_history_action"),
    sa.Index("ix_reply_exemplar_history_ex", "exemplar_id"),
)

TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)

# ── 匿名化（共通検査器 hub/pii_scan に委譲・BR-04） ─────────────────────────────
_reject_count = {"residue": 0}


def anonymize(text: str, *, names: tuple | list = ()) -> tuple[str, bool]:
    """匿名化して (匿名化済み文, 残留検査に合格したか) を返す。業者名は触らない（呼び出し側が
    別列に保持）。金額は保持。names は相談者名（表記ゆれは内部で展開）。"""
    return pii_scan.anonymize(text, names=names)


# ── 保存 ────────────────────────────────────────────────────────────────────
def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


async def _history(s, exemplar_id: int, action: str, actor: str, now, note: str | None = None):
    await s.execute(sa.insert(reply_exemplar_history).values(
        exemplar_id=exemplar_id, action=action, actor=actor, at=now, note=note))


async def save_exemplar(*, business: str, inbound_text: str, human_reply: str,
                        customer_name: str = "", creditor_name: str | None = None,
                        stage: str = STAGE_CONSULT, conversation_ref: str | None = None,
                        source_op_id: str | None = None, source_text_version: str | None = None,
                        category_id: str | None = None,
                        approved_by: str = APPROVED_BY_HUMAN) -> str:
    """匿名化して保存する。戻り値は固定語彙: saved / rejected（残留あり・保存しない・件数ログ）/
    skipped（DB 未設定）/ bad_input / failed（DB 例外・固定語彙ログ）。"""
    if stage not in STAGES or not str(human_reply or "").strip():
        return "bad_input"
    if not _db_configured():
        return "skipped"
    names = (customer_name,) if customer_name else ()
    reply_a, ok1 = anonymize(str(human_reply)[:MAX_TEXT_CHARS], names=names)
    inbound_a, ok2 = anonymize(str(inbound_text or "")[:MAX_TEXT_CHARS], names=names)
    if not (ok1 and ok2):
        _reject_count["residue"] += 1
        logger.info("[REPLY_EXEMPLAR] rejected by residue check (not saved) total=%s",
                    emit(_reject_count["residue"], "count", "log", "operator"))
        return "rejected"
    now = _now()
    try:
        async with session_scope() as s:
            res = await s.execute(sa.insert(reply_exemplar).values(
                business=business, conversation_ref=conversation_ref or None,
                inbound_text=inbound_a, human_reply=reply_a,
                creditor_name=(creditor_name or None), stage=stage,
                created_at=now, used_count=0, source_op_id=source_op_id or None,
                category_id=category_id or None,
                source_text_version=source_text_version or None,
                approved_by=approved_by, approved_at=now, active=True))
            await _history(s, int(res.inserted_primary_key[0]), ACTION_CREATED, approved_by, now)
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] save failed (fixed reason)")
        return "failed"
    return "saved"


async def mark_used(exemplar_id: int, *, actor: str = "bot") -> bool:
    """下書き生成で使ったら used_count を進め履歴を残す（Q3b の書込口）。"""
    now = _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_exemplar).where(
            reply_exemplar.c.id == int(exemplar_id), reply_exemplar.c.active.is_(True)).values(
            used_count=reply_exemplar.c.used_count + 1))
        if int(r.rowcount or 0) != 1:
            return False
        await _history(s, int(exemplar_id), ACTION_USED, actor, now)
    return True


async def deactivate_exemplar(exemplar_id: int, *, actor: str = APPROVED_BY_HUMAN) -> bool:
    """手本を無効にする（削除しない＝RV-08・履歴 deactivated）。"""
    now = _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_exemplar).where(
            reply_exemplar.c.id == int(exemplar_id), reply_exemplar.c.active.is_(True)).values(
            active=False))
        if int(r.rowcount or 0) != 1:
            return False
        await _history(s, int(exemplar_id), ACTION_DEACTIVATED, actor, now)
    return True


async def list_exemplars(business: str, *, stage: str | None = None, limit: int = 5,
                         category_id: str | None = None) -> list[dict]:
    """同じ業務（・段階・分類）の **active な** 手本を新しい順に最大 limit 件（Q3b の読取口）。"""
    q = sa.select(reply_exemplar).where(reply_exemplar.c.business == business,
                                        reply_exemplar.c.active.is_(True))
    if stage:
        q = q.where(reply_exemplar.c.stage == stage)
    if category_id:
        q = q.where(reply_exemplar.c.category_id == category_id)
    q = q.order_by(reply_exemplar.c.created_at.desc(), reply_exemplar.c.id.desc()).limit(int(limit))
    async with session_scope() as s:
        rows = (await s.execute(q)).fetchall()
    return [dict(r._mapping) for r in rows]


async def exemplar_history(exemplar_id: int) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_exemplar_history).where(
            reply_exemplar_history.c.exemplar_id == int(exemplar_id))
            .order_by(reply_exemplar_history.c.history_id))).fetchall()
    return [dict(r._mapping) for r in rows]
