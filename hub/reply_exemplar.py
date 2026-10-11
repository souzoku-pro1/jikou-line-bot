"""reply_exemplar — JIKOU-REPLY-Q1b: 大野の返信を「手本」として匿名化して保存する（§10-7・§12 Q1b）

正本: Desktop/claude/時効LINEボット_返信規則_v1.4.md §7（大野の返信の扱い）・§10-7（手本は
匿名化済み文だけ・検査に落ちた文は手本にしない）・§10-10（新設 DB 表・起動時に migration を
走らせない）。QUEUE-2026-10-11-B Q-3 (d)。

- 表 reply_exemplar: id・business・conversation_ref（send_ledger.conversation.ref＝不透明参照・
  無ければ NULL）・inbound_text（匿名化済み）・human_reply（匿名化済み）・creditor_name（業者名
  は保持）・stage（相談 / 受任後）・created_at・used_count（下書き生成での使用回数・Q3b が進める）・
  source_op_id（送信操作記録 op_id・本文は持たない）。
- 匿名化（§10-7・票 (d)）: 氏名（引数で渡された相談者名の表記ゆれ）・住所・電話・メール・
  口座番号・郵便番号・7 桁以上の数字列を伏せ字にする。**金額（「円」「万円」付き）と業者名は
  保持**する。匿名化の後に残留検査（メール・電話・郵便番号・7 桁以上）を行い、残留があれば
  保存しない（戻り値 rejected）。
- DB 未設定（DATABASE_URL なし）は保存しない（skipped）。DB 例外は固定語彙のログだけ（failed）。
  手本の保存失敗は送信や App 28 記録に影響しない（呼び出し側で順序を固定）。
- RV-10: 本表に LINE userId・原文は置かない。ログに本文・氏名を出さない。
"""

import datetime
import logging
import re

import sqlalchemy as sa

from hub import db
from hub.db import DatabaseNotConfigured, session_scope

logger = logging.getLogger("hub.reply_exemplar")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")

STAGE_CONSULT = "相談"
STAGE_ENGAGED = "受任後"
STAGES = (STAGE_CONSULT, STAGE_ENGAGED)
TABLE_NAME = "reply_exemplar"
MAX_TEXT_CHARS = 4000

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
    sa.CheckConstraint("stage IN ('相談', '受任後')", name="ck_reply_exemplar_stage"),
    sa.Index("ix_reply_exemplar_lookup", "business", "stage", "created_at"),
)

# ── 匿名化 ────────────────────────────────────────────────────────────────────
MASK_NAME = "〔氏名〕"
MASK_ADDRESS = "〔住所〕"
MASK_PHONE = "〔電話〕"
MASK_EMAIL = "〔メール〕"
MASK_ACCOUNT = "〔口座〕"
MASK_POSTAL = "〔郵便番号〕"
MASK_DIGITS = "〔数字列〕"

# 金額は保持する（「12,345円」「3万円」「44,000 円」）。先に退避して最後に戻す
_AMOUNT_RE = re.compile(r"[0-9０-９][0-9０-９,，]*\s?(?:万)?円")
_AMOUNT_SLOT = "\u0000AMT{}\u0000"
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"(?<![0-9０-９])0\d{1,4}[-‐－\s]?\d{1,4}[-‐－\s]?\d{3,4}(?![0-9０-９])")
_POSTAL_RE = re.compile(r"〒?\s?(?<![0-9０-９-‐－])\d{3}[-‐－]\d{4}(?![0-9０-９-‐－])")
# 口座: 「口座番号 1234567」「普通 1234567」「口座:1234567」等（ラベルは残し番号だけ伏せる）
_ACCOUNT_RE = re.compile(r"(口座番号|口座|普通|当座)(\s*[:：]?\s*)[0-9０-９]{4,8}(?![0-9０-９])")
# 住所: 都道府県 + 市区郡町村 を含む連続（空白・句読点で終端）。業者名（「〇〇県信用金庫」等）は
# 市区郡町村が続かない限り一致しない
_ADDRESS_RE = re.compile(
    r"(?:北海道|東京都|京都府|大阪府|[^\s、。]{2,3}県)[^\s、。]{1,12}?(?:市|区|郡|町|村)[^\s、。]{0,40}")
_DIGITS_RE = re.compile(r"(?<![0-9０-９])[0-9０-９]{7,}(?![0-9０-９])")
_NAME_SPLIT_RE = re.compile(r"[\s　]+")
_RESIDUE_RES = (_EMAIL_RE, _PHONE_RE, _POSTAL_RE, _DIGITS_RE)


def _name_variants(name: str) -> list[str]:
    """相談者名の表記ゆれ（全体・空白除去・姓/名の各部。1 文字の部分は対象外＝誤伏せ防止）。"""
    name = (name or "").strip()
    if not name or name.startswith("（"):
        return []
    out = {name, name.replace(" ", "").replace("　", "")}
    out.update(p for p in _NAME_SPLIT_RE.split(name) if len(p) >= 2)
    return sorted((v for v in out if len(v) >= 2), key=len, reverse=True)


def anonymize(text: str, *, names: tuple | list = ()) -> tuple[str, bool]:
    """匿名化して (匿名化済み文, 残留検査に合格したか) を返す。業者名は触らない（呼び出し側が
    別列に保持）。金額は保持。names は相談者名（表記ゆれは内部で展開）。"""
    s = str(text or "")
    amounts: list[str] = []

    def _stash(m):
        amounts.append(m.group(0))
        return _AMOUNT_SLOT.format(len(amounts) - 1)

    s = _AMOUNT_RE.sub(_stash, s)
    for n in names or ():
        for v in _name_variants(n):
            s = s.replace(v, MASK_NAME)
    s = _EMAIL_RE.sub(MASK_EMAIL, s)
    s = _ACCOUNT_RE.sub(lambda m: m.group(1) + m.group(2) + MASK_ACCOUNT, s)
    s = _PHONE_RE.sub(MASK_PHONE, s)          # 電話を先に（郵便番号の 3-4 桁が電話の一部に一致しないよう）
    s = _POSTAL_RE.sub(MASK_POSTAL, s)
    s = _ADDRESS_RE.sub(MASK_ADDRESS, s)
    s = _DIGITS_RE.sub(MASK_DIGITS, s)
    ok = not any(rx.search(s) for rx in _RESIDUE_RES)
    for i, a in enumerate(amounts):
        s = s.replace(_AMOUNT_SLOT.format(i), a)
    return s, ok


# ── 保存 ────────────────────────────────────────────────────────────────────
def _db_configured() -> bool:
    try:
        db.database_url()
    except DatabaseNotConfigured:
        return False
    return True


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


async def save_exemplar(*, business: str, inbound_text: str, human_reply: str,
                        customer_name: str = "", creditor_name: str | None = None,
                        stage: str = STAGE_CONSULT, conversation_ref: str | None = None,
                        source_op_id: str | None = None) -> str:
    """匿名化して保存する。戻り値は固定語彙: saved / rejected（残留あり・保存しない）/
    skipped（DB 未設定）/ bad_input / failed（DB 例外・固定語彙ログ）。"""
    if stage not in STAGES or not str(human_reply or "").strip():
        return "bad_input"
    if not _db_configured():
        return "skipped"
    names = (customer_name,) if customer_name else ()
    reply_a, ok1 = anonymize(str(human_reply)[:MAX_TEXT_CHARS], names=names)
    inbound_a, ok2 = anonymize(str(inbound_text or "")[:MAX_TEXT_CHARS], names=names)
    if not (ok1 and ok2):
        logger.info("[REPLY_EXEMPLAR] rejected by residue check (not saved)")
        return "rejected"
    try:
        async with session_scope() as s:
            await s.execute(sa.insert(reply_exemplar).values(
                business=business, conversation_ref=conversation_ref or None,
                inbound_text=inbound_a, human_reply=reply_a,
                creditor_name=(creditor_name or None), stage=stage,
                created_at=_now(), used_count=0, source_op_id=source_op_id or None))
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] save failed (fixed reason)")
        return "failed"
    return "saved"


async def list_exemplars(business: str, *, stage: str | None = None, limit: int = 5) -> list[dict]:
    """同じ業務（・段階）の新しい順に最大 limit 件（Q3b の下書き生成が使う読取口）。"""
    q = sa.select(reply_exemplar).where(reply_exemplar.c.business == business)
    if stage:
        q = q.where(reply_exemplar.c.stage == stage)
    q = q.order_by(reply_exemplar.c.created_at.desc(), reply_exemplar.c.id.desc()).limit(int(limit))
    async with session_scope() as s:
        rows = (await s.execute(q)).fetchall()
    return [dict(r._mapping) for r in rows]
