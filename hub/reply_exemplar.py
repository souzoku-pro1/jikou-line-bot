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
    sa.Column("outcome", sa.Text, nullable=True),                  # JIKOU-REPLY-Q4: approved_asis / approved_edited / rejected
    sa.Index("ix_reply_draft_approval_cat", "business", "category", "unmodified"),
)
PROMOTION_THRESHOLD = 3                 # 無修正承認がこの回数に達したら「凍結定型の候補」通知（昇格は Q5）

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
                approved_by=None, approved_at=None, active=bool(active),           # JIKOU-REPLY-Q4: 候補は active=False
                status=STATUS_PENDING_REVIEW))                                       # fix2 BR-10: 保存時は常に確認待ち
            await _history(s, int(res.inserted_primary_key[0]), ACTION_CREATED, approved_by, now)
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] save failed (fixed reason)")
        return "failed"
    return "saved"


async def approve_exemplar(exemplar_id: int, *, actor: str = APPROVED_BY_HUMAN) -> bool:
    """fix2 BR-10: 人の明示操作で承認済みにする（pending_review／retired → approved・approved_by/at・active=True・
    履歴 approved）。既に approved なら False。Q1b は画面を持たない（Q3a の承認画面が呼ぶ）。"""
    now = _now()
    async with session_scope() as s:
        r = await s.execute(sa.update(reply_exemplar).where(
            reply_exemplar.c.id == int(exemplar_id), reply_exemplar.c.status != STATUS_APPROVED).values(
            status=STATUS_APPROVED, active=True, approved_by=actor, approved_at=now))
        if int(r.rowcount or 0) != 1:
            return False
        await _history(s, int(exemplar_id), ACTION_APPROVED, actor, now)
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
        return {"category": row.category, "unmodified": unmodified, "count": n}
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] record_sent failed (fixed reason)")
        return None


async def count_unmodified(business: str, category: str) -> int:
    async with session_scope() as s:
        return int((await s.execute(sa.select(sa.func.count()).select_from(reply_draft_approval).where(
            reply_draft_approval.c.business == business, reply_draft_approval.c.category == category,
            reply_draft_approval.c.unmodified.is_(True)))).scalar() or 0)


# ── JIKOU-REPLY-Q4（QUEUE-2026-10-13-D D-3）: 下書きの処理種別（reply_draft_approval.outcome）。末尾追記。
OUTCOME_APPROVED_ASIS = "approved_asis"
OUTCOME_APPROVED_EDITED = "approved_edited"
OUTCOME_REJECTED = "rejected"
OUTCOMES = (OUTCOME_APPROVED_ASIS, OUTCOME_APPROVED_EDITED, OUTCOME_REJECTED)


async def set_outcome(record_id, outcome: str) -> bool:
    """record_draft 済みの行に種別を記録（閉集合外・行なし・DB 未設定は False）。"""
    if outcome not in OUTCOMES or not record_id or not _db_configured():
        return False
    try:
        async with session_scope() as s:
            r = await s.execute(sa.update(reply_draft_approval).where(
                reply_draft_approval.c.record_id == str(record_id)).values(outcome=outcome))
            return int(r.rowcount or 0) == 1
    except Exception:
        logger.warning("[REPLY_EXEMPLAR] set_outcome failed (fixed reason)")
        return False
