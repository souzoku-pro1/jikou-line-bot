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
fix2（Codex R-Q1b-fix1 BR-10・司令塔裁定「自由文の手本は人が匿名化を確認するまで下書き参照に使わない」）:
- 保存時の状態は **status=pending_review**（approved_by／approved_at は NULL）。自動マスクだけで承認済みにしない。
- 下書き参照（list_exemplars＝Q3a 以降の参考文例・Q2 選択器・Q3b 下書き）は **status=approved のものだけ** を読む。
- approved への遷移は人の明示操作（approve_exemplar）のみ。Q1b 単体では遷移の画面を持たない（Q3a の承認画面が接続する）。
- 退役は status=retired（active=False）。mark_used は approved だけ。
"""

import datetime
import hashlib
import logging

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as _pg
from sqlalchemy.dialects import sqlite as _sqlite

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
ACTION_APPROVED = "approved"          # JIKOU-REPLY-Q3a (a) 再承認・fix2 BR-10: 人の明示操作で承認済みへ（同じ語彙）
ACTIONS = (ACTION_CREATED, ACTION_USED, ACTION_DEACTIVATED, ACTION_APPROVED)
STATUS_PENDING_REVIEW = "pending_review"   # fix2 BR-10: 保存直後（人の匿名化確認待ち・下書き参照に使わない）
STATUS_APPROVED = "approved"               # 人が確認して承認（下書き参照に使う）
STATUS_RETIRED = "retired"                 # 退役（削除しない）
STATUSES = (STATUS_PENDING_REVIEW, STATUS_APPROVED, STATUS_RETIRED)

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
    sa.Column("approved_by", sa.Text, nullable=True),                  # fix2 BR-10: 承認まで NULL
    sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("active", sa.Boolean, nullable=False, server_default=sa.true()),
    sa.Column("status", sa.Text, nullable=False, server_default=STATUS_PENDING_REVIEW),   # fix2 BR-10
    sa.CheckConstraint("stage IN ('相談', '受任後')", name="ck_reply_exemplar_stage"),
    sa.CheckConstraint("status IN ('pending_review', 'approved', 'retired')", name="ck_reply_exemplar_status"),
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
    sa.CheckConstraint("action IN ('created', 'used', 'deactivated', 'approved')",
                       name="ck_reply_exemplar_history_action"),
    sa.Index("ix_reply_exemplar_history_ex", "exemplar_id"),
)

TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)

# ── JIKOU-REPLY-Q3a (c): 承認キューの下書きと送信文の照合（無修正承認の計数） ──────────────
DRAFT_TABLE_NAME = "reply_draft_approval"
reply_draft_approval = sa.Table(
    DRAFT_TABLE_NAME, metadata,
    sa.Column("record_id", sa.Text, primary_key=True),              # App 29 レコード No.
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("category", sa.Text, nullable=True),
    sa.Column("draft_text_version", sa.Text, nullable=False),      # 起票時の AI 下書きの sha256（本文は持たない）
    sa.Column("sent_text_version", sa.Text, nullable=True),        # 送信した文の sha256
    sa.Column("unmodified", sa.Boolean, nullable=True),            # 無修正承認（NULL=未送信）
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
    sa.Index("ix_reply_draft_approval_cat", "business", "category", "unmodified"),
)
PROMOTION_THRESHOLD = 3                 # 無修正承認がこの回数に達したら「凍結定型の候補」通知（昇格は Q5）
# Q3a-fix1 BQ-05（Codex R-Q3a）: 候補通知の確定表。閾値到達を (business, category, threshold) の固定キーで **1 行だけ** 確定し
# （同一 tx・INSERT … ON CONFLICT DO NOTHING＝並行 2 件でも 1 者だけ）、確定した者が台帳キーで通知する。通知に失敗したら
# state=pending のまま残り、回収ジョブ（send_policy.run_promotion_notice_job）が再通知する。本文・ID は置かない。
PROMOTION_TABLE_NAME = "exemplar_promotion_notice"
PROMOTION_PENDING = "pending"
PROMOTION_SENT = "sent"
exemplar_promotion_notice = sa.Table(
    PROMOTION_TABLE_NAME, metadata,
    sa.Column("business", sa.Text, primary_key=True),
    sa.Column("category", sa.Text, primary_key=True),
    sa.Column("threshold", sa.Integer, primary_key=True),
    sa.Column("state", sa.Text, nullable=False, server_default=PROMOTION_PENDING),
    sa.Column("attempts", sa.Integer, nullable=False, server_default="0"),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("notified_at", sa.DateTime(timezone=True), nullable=True),
)

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
                        approved_by: str = APPROVED_BY_HUMAN, active: bool = True) -> str:
    """匿名化して保存する。戻り値は固定語彙: saved / rejected（残留あり・保存しない・件数ログ）/
    skipped（DB 未設定）/ bad_input / failed（DB 例外・固定語彙ログ）。
    fix2 BR-10: 保存時の状態は常に pending_review（approved_by／approved_at は NULL）。approved_by は履歴 created の
    actor（保存した主体）にだけ使う。active は後続票（Q4 の修正承認＝未承認候補 active=False）との互換のため列に写すが、
    状態は常に pending_review（下書き参照の可否は status で決まる）。"""
    if stage not in STAGES or not str(human_reply or "").strip():
        return "bad_input"
    if not _db_configured():
        return "skipped"
    names = (customer_name,) if customer_name else ()
    category_id = category_id if category_id in CATEGORIES else None     # JIKOU-REPLY-Q3a: 閉集合外は NULL
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
                approved_by=None, approved_at=None, active=bool(active), status=STATUS_PENDING_REVIEW))
            await _history(s, int(res.inserted_primary_key[0]), ACTION_CREATED, approved_by, now)
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] save failed (fixed reason)")
        return "failed"
    return "saved"


async def approve_exemplar(exemplar_id: int, *, actor: str = APPROVED_BY_HUMAN, note: str | None = None) -> bool:
    """fix2 BR-10: 人の明示操作で承認済みにする（pending_review／retired → approved・approved_by/at・active=True・
    履歴 approved）。既に approved なら False。Q1b は画面を持たない（Q3a の承認画面が呼ぶ）。"""
    now = _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_exemplar).where(
            reply_exemplar.c.id == int(exemplar_id), reply_exemplar.c.status != STATUS_APPROVED).values(
            status=STATUS_APPROVED, active=True, approved_by=actor, approved_at=now))
        if int(r.rowcount or 0) != 1:
            return False
        await _history(s, int(exemplar_id), ACTION_APPROVED, actor, now, note)   # Q3a-fix1 BQ-03: 修正して承認したときは note に欄名
    return True


async def mark_used(exemplar_id: int, *, actor: str = "bot") -> bool:
    """下書き生成で使ったら used_count を進め履歴を残す（Q3b の書込口・fix2: approved だけ）。"""
    now = _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_exemplar).where(
            reply_exemplar.c.id == int(exemplar_id), reply_exemplar.c.status == STATUS_APPROVED,
            reply_exemplar.c.active.is_(True)).values(
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
            reply_exemplar.c.id == int(exemplar_id), reply_exemplar.c.status != STATUS_RETIRED).values(
            active=False, status=STATUS_RETIRED))
        if int(r.rowcount or 0) != 1:
            return False
        await _history(s, int(exemplar_id), ACTION_DEACTIVATED, actor, now)
    return True


async def list_exemplars(business: str, *, stage: str | None = None, limit: int = 5,
                         category_id: str | None = None) -> list[dict]:
    """同じ業務（・段階・分類）の **approved な** 手本を新しい順に最大 limit 件（Q3b の読取口・fix2 BR-10:
    pending_review は読まない）。"""
    q = sa.select(reply_exemplar).where(reply_exemplar.c.business == business,
                                        reply_exemplar.c.status == STATUS_APPROVED,
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


# ══════════════════════════════════════════════════════════════
# JIKOU-REPLY-Q3a（QUEUE-2026-10-12-C C-2）: 承認・退役の画面口／参考文例／無修正承認の計数
#   - (a) approve_exemplar（退役 → 承認済み・履歴 approved）・list_for_view・get_exemplar
#   - (b) prompt_note: 承認済み（active）の手本を category ごとに新しい順で最大 NOTE_MAX_PER_CATEGORY 件、
#         最大 NOTE_MAX_CATEGORIES カテゴリを system prompt の末尾に「参考文例（事務所の過去回答・そのまま
#         写さない）」として添える文を返す。手本が 0 件なら ""（prompt は従来と完全一致＝pin 不変）。
#         返信の category は compose の出力なので生成前には決まらない＝カテゴリ別に並べて添え、モデルが自分の
#         category の例を参照する（2 回呼ばない）。使った手本は mark_used_for（結果の category の群だけ）。
#   - (c) record_draft（起票時の下書き sha256）・record_sent（送信文の sha256 と照合＝無修正承認）・
#         count_unmodified（category ごとの無修正承認回数）。通知は hub/send_policy.approved_draft_sent。
#   RV-10: 本表に本文は持たない（sha256 のみ）。参考文例は匿名化済み文だけを使う。
# ══════════════════════════════════════════════════════════════
# compose_reply の category 閉集合（chat_responder._COMPOSE_REPLY_TOOL の enum と一致＝test が pin）
CATEGORIES = (
    "挨拶・雑談", "手続きの一般的な流れ", "必要書類の案内", "費用の定型案内", "進捗の事実回答",
    "営業案内・アクセス", "時効見立て_条件付き", "法的判断・見通し", "費用交渉・減額相談", "クレーム・不満",
    "解約・辞任関係", "緊急対応", "本人確認不能・第三者", "その他判断系",
)
NOTE_MAX_PER_CATEGORY = 3
NOTE_MAX_CATEGORIES = 4
NOTE_HEADER = "\n\n【参考文例（事務所の過去回答・文体と構成の参照のみ・そのまま写さない・個別の事情や数値は引用しない）】"
NO_CATEGORY_LABEL = "分類なし"


def _iso(v) -> str:
    if v is None:
        return ""
    if v.tzinfo is None:
        v = v.replace(tzinfo=datetime.timezone.utc)
    return v.isoformat()


def _row(r) -> dict:
    d = dict(r._mapping)
    d["created_at"] = _iso(d.get("created_at"))
    d["approved_at"] = _iso(d.get("approved_at"))
    d["active"] = bool(d.get("active"))
    d["used_count"] = int(d.get("used_count") or 0)
    return d


async def get_exemplar(exemplar_id: int) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(reply_exemplar).where(reply_exemplar.c.id == int(exemplar_id)))).first()
    return _row(r) if r is not None else None


async def list_for_view(business: str, *, include_inactive: bool = False, limit: int = 200) -> list[dict]:
    q = sa.select(reply_exemplar).where(reply_exemplar.c.business == business)
    if not include_inactive:
        q = q.where(reply_exemplar.c.active.is_(True))
    q = q.order_by(reply_exemplar.c.created_at.desc(), reply_exemplar.c.id.desc()).limit(int(limit))
    async with session_scope() as s:
        rows = (await s.execute(q)).fetchall()
    return [_row(r) for r in rows]


# approve_exemplar は fix2 BR-10 で status ベース（pending_review／retired → approved）に一本化（上の定義・Q3a の再承認も同じ口）。


class ExemplarNote(str):
    """system prompt 末尾に添える参考文例（str 互換）。ids_by_category は使った手本の ID（mark_used_for 用）。"""
    ids_by_category: dict = {}


def _stage_for_status(status: str) -> str:
    try:
        from chat_responder import classify_routing
        return STAGE_ENGAGED if classify_routing(str(status or "")) == "post_engagement" else STAGE_CONSULT
    except Exception:
        return STAGE_CONSULT


def format_note(rows: list[dict]) -> ExemplarNote:
    """手本の列（新しい順）から参考文例の文を組む（純関数）。0 件は ""。"""
    groups: dict = {}
    order: list = []
    for r in rows:
        if not r.get("active", True):
            continue
        key = r.get("category_id") or NO_CATEGORY_LABEL
        if key not in groups:
            if len(order) >= NOTE_MAX_CATEGORIES:
                continue
            groups[key] = []
            order.append(key)
        if len(groups[key]) < NOTE_MAX_PER_CATEGORY:
            groups[key].append(r)
    if not order:
        note = ExemplarNote("")
        note.ids_by_category = {}
        return note
    lines = [NOTE_HEADER]
    ids: dict = {}
    for key in order:
        lines.append(f"■ カテゴリ: {key}")
        ids[key] = [int(r["id"]) for r in groups[key]]
        for i, r in enumerate(groups[key], start=1):
            q = str(r.get("inbound_text") or "").strip()
            if q:
                lines.append(f"例{i}（相談者）: {q}")
            lines.append(f"例{i}（事務所の返信）: {str(r.get('human_reply') or '').strip()}")
    note = ExemplarNote("\n".join(lines))
    note.ids_by_category = ids
    return note


async def prompt_note(business: str, status: str) -> ExemplarNote:
    """承認済みの手本から参考文例の文を作る（DB 未設定・例外は ""＝従来どおり）。"""
    if not _db_configured():
        return format_note([])
    try:
        rows = await list_exemplars(business, stage=_stage_for_status(status),
                                    limit=NOTE_MAX_PER_CATEGORY * len(CATEGORIES))
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] prompt note skipped (fixed reason)")
        return format_note([])
    return format_note(rows)


async def mark_used_for(note, category: str) -> int:
    """自動送信した返信の category の群だけ used（履歴 used・used_count+1）。戻り値は件数。"""
    ids = (getattr(note, "ids_by_category", None) or {}).get(category) or []
    n = 0
    for xid in ids:
        try:
            if await mark_used(int(xid)):
                n += 1
        except Exception:
            logger.warning("[REPLY_EXEMPLAR] mark_used failed (fixed reason)")
    return n


async def record_draft(record_id, business: str, category: str | None, text: str) -> bool:
    """起票時の AI 下書きの sha256 を記録（App 29 レコード No. 単位・既存はそのまま）。失敗は握る。"""
    if not record_id or not _db_configured():
        return False
    now = _now()
    try:
        async with session_scope() as s:
            exists = (await s.execute(sa.select(reply_draft_approval.c.record_id).where(
                reply_draft_approval.c.record_id == str(record_id)))).scalar()
            if exists is not None:
                return False
            await s.execute(sa.insert(reply_draft_approval).values(
                record_id=str(record_id), business=business,
                category=(category if category in CATEGORIES else None),
                draft_text_version=hashlib.sha256(str(text or "").encode("utf-8")).hexdigest(),
                created_at=now))
        return True
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] record_draft failed (fixed reason)")
        return False


async def record_sent(record_id, text: str) -> dict | None:
    """承認済み下書きの送信成功: 送信文の sha256 を照合して無修正か記録し、その category の無修正承認回数を返す
    （{"category", "unmodified", "count"}）。起票記録が無い・DB 未設定・失敗は None。"""
    if not record_id or not _db_configured():
        return None
    now = _now()
    tv = hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()
    try:
        async with session_scope() as s:
            row = (await s.execute(sa.select(reply_draft_approval).where(
                reply_draft_approval.c.record_id == str(record_id)))).first()
            if row is None:
                return None
            if row.sent_at is not None:
                return None                      # 既に照合済み（再配送）
            unmodified = (row.draft_text_version == tv)
            await s.execute(sa.update(reply_draft_approval).where(
                reply_draft_approval.c.record_id == str(record_id)).values(
                sent_text_version=tv, unmodified=unmodified, sent_at=now))
            n = int((await s.execute(sa.select(sa.func.count()).select_from(reply_draft_approval).where(
                reply_draft_approval.c.business == row.business,
                reply_draft_approval.c.category == row.category,
                reply_draft_approval.c.unmodified.is_(True)))).scalar() or 0)
            # Q3a-fix1 BQ-05: 閾値到達（>=）を同一 tx で固定キーに確定。確定できた 1 者だけ claimed=True（並行 2 件でも 1 者）
            claimed = False
            if unmodified and row.category and n >= PROMOTION_THRESHOLD:
                claimed = await _claim_promotion(s, row.business, row.category, PROMOTION_THRESHOLD, now)
        return {"category": row.category, "unmodified": unmodified, "count": n, "claimed": claimed,
                "threshold": PROMOTION_THRESHOLD}
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] record_sent failed (fixed reason)")
        return None


async def count_unmodified(business: str, category: str) -> int:
    async with session_scope() as s:
        return int((await s.execute(sa.select(sa.func.count()).select_from(reply_draft_approval).where(
            reply_draft_approval.c.business == business, reply_draft_approval.c.category == category,
            reply_draft_approval.c.unmodified.is_(True)))).scalar() or 0)


# ══════════════════════════════════════════════════════════════
# JIKOU-REPLY-Q3a-fix1 BQ-01（Codex R-Q3a REJECT・Fable 裁定 2026-11-03）
#   手本（参考文例）を prompt に添えて生成した返信は、カテゴリが許可表にあっても **自動送信しない**。承認キュー（App 29・
#   対応待ち）に固定し、Q4／返信画面での承認後にだけ送る。手本 0 件（note == ""）の生成と定型は従来どおり（許可表どおり）。
#   V2 ON/OFF の双方に効く（guard の段で降格するため、OFF は確認中の定型・ON は無言＋対応待ち＝既存の降格経路）。
#   used（used_count＋履歴）は、固定した下書きが承認キューに入った時点で結果の category の群だけに付ける。
# ══════════════════════════════════════════════════════════════
EXEMPLAR_REFERENCED_REASON = "手本参照（承認後のみ送信・Q3a-fix1 BQ-01）"


def require_approval_if_referenced(guard, note):
    """参考文例つきで生成した返信を承認必須に降格する（note が空＝手本 0 件なら guard をそのまま返す）。"""
    if not str(note or "") or not getattr(guard, "can_auto_send", False):
        return guard
    import dataclasses
    reasons = list(getattr(guard, "demotion_reasons", []) or []) + [EXEMPLAR_REFERENCED_REASON]
    return dataclasses.replace(guard, can_auto_send=False, demotion_reasons=reasons)


# ── JIKOU-REPLY-Q3a-fix1 BQ-03（Codex R-Q3a）: 承認画面で匿名化済み本文を人が修正・確定する ──
EDIT_FIELDS = ("human_reply", "inbound_text")


async def update_texts(exemplar_id: int, *, human_reply: str, inbound_text: str) -> tuple[str, list[str]]:
    """匿名化済み本文を人の修正で置き換える（承認前）。残留検査（発見的検査を含む）に 1 つでも掛かれば置き換えず
    ("residue", 掛かった種別) を返す。戻り値 (ok|residue|bad_input|not_found|not_editable, hits)。承認済みの手本は直さない（退役→修正→再承認）。"""
    reply = str(human_reply or "").strip()[:MAX_TEXT_CHARS]
    inbound = str(inbound_text or "").strip()[:MAX_TEXT_CHARS]
    if not reply:
        return "bad_input", []
    hits = sorted(set(pii_scan.residue_hits(reply) + pii_scan.residue_hits(inbound)))
    if hits:
        _reject_count["residue"] += 1
        logger.info("[REPLY_EXEMPLAR] edit rejected by residue check (not saved) total=%s",
                    emit(_reject_count["residue"], "count", "log", "operator"))
        return "residue", hits
    async with session_scope() as s:
        cur = (await s.execute(sa.select(reply_exemplar.c.status).where(reply_exemplar.c.id == int(exemplar_id)))).scalar()
        if cur is None:
            return "not_found", []
        if cur == STATUS_APPROVED:
            return "not_editable", []
        await s.execute(sa.update(reply_exemplar).where(reply_exemplar.c.id == int(exemplar_id)).values(
            human_reply=reply, inbound_text=inbound))
    return "ok", []


# ── JIKOU-REPLY-Q3a-fix1 BQ-05（Codex R-Q3a）: 候補通知の確定（固定キー・1 者）と回収 ──
async def _claim_promotion(s, business: str, category: str, threshold: int, now) -> bool:
    """(business, category, threshold) を INSERT … ON CONFLICT DO NOTHING（PostgreSQL／sqlite）。rowcount 1 なら確定者。"""
    values = dict(business=business, category=category, threshold=int(threshold), state=PROMOTION_PENDING, attempts=0,
                  created_at=now)
    dialect = (await s.connection()).dialect.name
    keys = ["business", "category", "threshold"]
    if dialect == "postgresql":
        stmt = _pg.insert(exemplar_promotion_notice).values(**values).on_conflict_do_nothing(index_elements=keys)
    elif dialect == "sqlite":
        stmt = _sqlite.insert(exemplar_promotion_notice).values(**values).on_conflict_do_nothing(index_elements=keys)
    else:                                                     # 他方言: 既存確認 → 挿入（重複は PK 違反で例外＝呼び出し側で握る）
        cur = (await s.execute(sa.select(exemplar_promotion_notice.c.state).where(
            exemplar_promotion_notice.c.business == business, exemplar_promotion_notice.c.category == category,
            exemplar_promotion_notice.c.threshold == int(threshold)))).first()
        if cur is not None:
            return False
        stmt = sa.insert(exemplar_promotion_notice).values(**values)
    res = await s.execute(stmt)
    return int(res.rowcount or 0) == 1


async def mark_promotion_sent(business: str, category: str, threshold: int, *, now=None) -> bool:
    now = now or _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(exemplar_promotion_notice).where(
            exemplar_promotion_notice.c.business == business, exemplar_promotion_notice.c.category == category,
            exemplar_promotion_notice.c.threshold == int(threshold)).values(
            state=PROMOTION_SENT, notified_at=now, attempts=exemplar_promotion_notice.c.attempts + 1))
        return int(r.rowcount or 0) == 1


async def bump_promotion_attempt(business: str, category: str, threshold: int) -> bool:
    async with session_scope() as s:
        r = await s.execute(sa.update(exemplar_promotion_notice).where(
            exemplar_promotion_notice.c.business == business, exemplar_promotion_notice.c.category == category,
            exemplar_promotion_notice.c.threshold == int(threshold)).values(
            attempts=exemplar_promotion_notice.c.attempts + 1))
        return int(r.rowcount or 0) == 1


async def list_promotion_pending(limit: int = 20) -> list[dict]:
    """確定済み・未通知（pending）の候補（回収ジョブ用・古い順）。"""
    async with session_scope() as s:
        rows = (await s.execute(sa.select(exemplar_promotion_notice).where(
            exemplar_promotion_notice.c.state == PROMOTION_PENDING)
            .order_by(exemplar_promotion_notice.c.created_at.asc()).limit(int(limit)))).fetchall()
    return [{"business": r.business, "category": r.category, "threshold": int(r.threshold), "attempts": int(r.attempts),
             "created_at": _iso(r.created_at)} for r in rows]
