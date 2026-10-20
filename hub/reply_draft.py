"""reply_draft — JIKOU-REPLY-Q3b: 定型に該当しない問いへの AI 下書き（承認済み手本を参考・承認待ちに積む・自動送信しない）

正本: 時効LINEボット_返信規則_v1.4.md R-J1（下書きを作れれば添えて大野に通知）・R-J9（育てる: 下書き→承認）・
§10-7（同じ分類 ID の匿名化済み手本・通知・ログへ LLM の原文を渡さない）・§12 Q3b。QUEUE-2026-10-13-D D-2。

- 発火: V2 ON の顧客対応経路で閉集合（Q2）に該当せず承認へ降格した受信（対応待ち=Q1c が登録済み）。
  同じ対応待ちに下書きは 1 件（冪等）。対応待ちが無い（保存失敗・DB 未設定）ときは作らない（fail-closed）。
- (a) 生成: 同 category の承認済み（active）手本 最大 3 件（新しい順）+直近履歴を Claude に渡して下書き 1 通。
  **手本 0 件なら下書きを作らない**（対応待ちと Q1c の通知だけ）。生成物は表 reply_draft（新 migration）に保存。
  業務 LINE へ「下書きあり」固定文言（対応待ち ID・カテゴリ・要修正の有無のみ・PII なし）を send_ledger 経由で
  **1 会話 1 日 1 回**（台帳キー draft_notice:{会話参照}:{JST 日付}）。
- (b) 検査: 既存サーバーガード（sanitize=マーカー除去・禁止語・字数・必須語・文体・空返事）を通し、不合格は
  guard_ok=False で保存のみ（通知文に「要修正」）。合格でも自動送信はしない（承認は Q4 の画面）。
- (c) /app/pending は下書きのある対応待ちに「下書きあり」印（hub/webapp_pending_view が latest_by_pending_ids を読む）。
- 状態（閉集合・RV-08 履歴 reply_draft_history）: proposed → approved / rejected（Q4）→ sent / failed（Q4 の送信）。
- RV-10: ログは固定語彙と件数のみ。通知に本文・氏名・LINE userId を載せない。表に LINE userId・原文の相談者発話は
  置かない（inbound_text_version=sha256 のみ）。
"""

import datetime
import hashlib
import json
import logging

import sqlalchemy as sa

from hub import db, reply_exemplar, reply_pending, send_ledger, send_policy
from hub.db import DatabaseNotConfigured, session_scope
from hub.redact import emit

logger = logging.getLogger("hub.reply_draft")

metadata = sa.MetaData()
_BIG = sa.BigInteger().with_variant(sa.Integer(), "sqlite")
_JST = datetime.timezone(datetime.timedelta(hours=9))

BUSINESS = "jikou"
TABLE_NAME = "reply_draft"
HISTORY_TABLE_NAME = "reply_draft_history"
TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)

STATE_PROPOSED = "proposed"
STATE_APPROVED = "approved"
STATE_REJECTED = "rejected"
STATE_SENT = "sent"
STATE_FAILED = "failed"
STATES = (STATE_PROPOSED, STATE_APPROVED, STATE_REJECTED, STATE_SENT, STATE_FAILED)
KIND_ASIS = "asis"
KIND_EDITED = "edited"
KINDS = (KIND_ASIS, KIND_EDITED)
ACTION_CREATED = "created"
ACTION_APPROVED = "approved"
ACTION_REJECTED = "rejected"
ACTION_SENT = "sent"
ACTION_FAILED = "failed"
ACTIONS = (ACTION_CREATED, ACTION_APPROVED, ACTION_REJECTED, ACTION_SENT, ACTION_FAILED)
ACTOR_BOT = "bot"
ACTOR_HUMAN = "human"

EXEMPLAR_LIMIT = 3
MAX_TOKENS = 700
MAX_DRAFT_CHARS = 2000
LIST_LIMIT = 100
NOTICE_HEADER = "【下書きあり】自動返信できない問いに AI の下書きを用意しました。"
NOTICE_FOOTER = "/app/pending（対応待ち）から確認してください。同じ相談者への本通知は本日これ以降省略します。"
NOTICE_NEEDS_FIX = "要修正（サーバーガード不合格）"
NOTICE_OK = "ガード合格（承認で送信可）"

DRAFT_SYSTEM = (
    "あなたは法律事務所の LINE 相談窓口の返信下書き担当です。相談者の直前の発話に対する返信案を 1 通だけ作成してください。\n"
    "規則:\n"
    "- 下の【参考文例】（事務所の過去回答）の文体・構成・判断の範囲に倣う。文例をそのまま写さず、個別の事情や数値は引用しない。\n"
    "- 敬体。結論を断定しない。行動を指示しない。法的判断・見立て・推奨時期は書かない（弁護士が判断する旨にとどめる）。\n"
    "- 住所・会員番号・口座番号・他人の氏名を復唱しない。相談者の登録名以外の固有名詞を書かない。\n"
    "- 「確認のうえご連絡します」「申し伝えます」「追ってご案内します」のような中身の無い返信は書かない。答えられる範囲で答える。\n"
    "- 費用は事務所の固定文（1社あたり44,000円（税込）・3社目から22,000円（税込）・前払い）を変えない。\n"
    "- 300 字以内。箇条書き記号・絵文字・markdown を使わない。本文だけを返す（前置き・署名なし）。"
)

reply_draft = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("draft_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("pending_id", _BIG, nullable=True),                  # reply_pending.pending_id（別 metadata・FK なし）
    sa.Column("line_user_hash", sa.Text, nullable=False),
    sa.Column("conversation_ref", sa.Text, nullable=True),
    sa.Column("category", sa.Text, nullable=True),
    sa.Column("inbound_text_version", sa.Text, nullable=False),    # 相談者発話の sha256（原文は持たない）
    sa.Column("draft_text", sa.Text, nullable=False),
    sa.Column("guard_ok", sa.Boolean, nullable=False),
    sa.Column("guard_reasons", sa.Text, nullable=True),            # 固定語彙を " / " で連結
    sa.Column("exemplar_ids", sa.Text, nullable=False),            # JSON 配列（手本 id）
    sa.Column("state", sa.Text, nullable=False, server_default=STATE_PROPOSED),
    sa.Column("approval_kind", sa.Text, nullable=True),            # asis / edited（Q4）
    sa.Column("final_text", sa.Text, nullable=True),               # 修正後本文（Q4）
    sa.Column("op_id", sa.Text, nullable=True),                    # 送信操作記録 op_id（Q4）
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("state IN ('proposed', 'approved', 'rejected', 'sent', 'failed')", name="ck_reply_draft_state"),
    sa.CheckConstraint("approval_kind IS NULL OR approval_kind IN ('asis', 'edited')", name="ck_reply_draft_kind"),
    sa.Index("ix_reply_draft_state", "business", "state", "created_at"),
    sa.Index("ix_reply_draft_pending", "pending_id"),
)

reply_draft_history = sa.Table(
    HISTORY_TABLE_NAME, metadata,
    sa.Column("history_id", _BIG, primary_key=True, autoincrement=True),
    sa.Column("draft_id", _BIG, sa.ForeignKey("reply_draft.draft_id"), nullable=False),
    sa.Column("action", sa.Text, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("note", sa.Text, nullable=True),
    sa.CheckConstraint("action IN ('created', 'approved', 'rejected', 'sent', 'failed')",
                       name="ck_reply_draft_history_action"),
    sa.Index("ix_reply_draft_history_d", "draft_id"),
)


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _db_configured() -> bool:
    try:
        db.database_url()
        return True
    except DatabaseNotConfigured:
        return False


def _iso(v) -> str:
    return v.isoformat() if isinstance(v, datetime.datetime) else (str(v) if v else "")


def _row(r) -> dict:
    d = dict(r._mapping)
    for k in ("created_at", "updated_at"):
        d[k] = _iso(d.get(k))
    try:
        d["exemplar_ids"] = json.loads(d.get("exemplar_ids") or "[]")
    except Exception:
        d["exemplar_ids"] = []
    return d


async def _hist(s, draft_id: int, action: str, actor: str, now, note: str | None = None) -> None:
    await s.execute(sa.insert(reply_draft_history).values(draft_id=int(draft_id), action=action, actor=actor,
                                                          at=now, note=note))


# ── 保存・読取 ────────────────────────────────────────────────────────────────
async def create(*, business: str, pending_id: int | None, line_user_hash: str, conversation_ref: str | None,
                 category: str | None, inbound_text: str, draft_text: str, guard_ok: bool, guard_reasons: list,
                 exemplar_ids: list, now=None) -> int | None:
    """下書き 1 件を保存（履歴 created）。DB 未設定・例外は None（固定語彙のログのみ）。"""
    if not _db_configured():
        return None
    now = now or _now()
    try:
        async with session_scope() as s:
            res = await s.execute(sa.insert(reply_draft).values(
                business=business, pending_id=(int(pending_id) if pending_id is not None else None),
                line_user_hash=line_user_hash, conversation_ref=conversation_ref,
                category=(category if category in reply_exemplar.CATEGORIES else None),
                inbound_text_version=hashlib.sha256(str(inbound_text or "").encode("utf-8")).hexdigest(),
                draft_text=str(draft_text or "")[:MAX_DRAFT_CHARS], guard_ok=bool(guard_ok),
                guard_reasons=(" / ".join(str(r) for r in guard_reasons) or None),
                exemplar_ids=json.dumps([int(i) for i in exemplar_ids]), state=STATE_PROPOSED,
                created_at=now, updated_at=now))
            did = int(res.inserted_primary_key[0])
            await _hist(s, did, ACTION_CREATED, ACTOR_BOT, now, note=("guard_ok" if guard_ok else "guard_failed"))
        return did
    except Exception:
        logger.warning("[REPLY_DRAFT] create failed (fixed reason)")
        return None


async def get_draft(draft_id: int) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(reply_draft).where(reply_draft.c.draft_id == int(draft_id)))).first()
    return _row(r) if r else None


async def list_drafts(business: str, *, states: tuple = (STATE_PROPOSED,), limit: int = LIST_LIMIT) -> list[dict]:
    q = sa.select(reply_draft).where(reply_draft.c.business == business)
    if states:
        q = q.where(reply_draft.c.state.in_(list(states)))
    q = q.order_by(reply_draft.c.created_at.asc(), reply_draft.c.draft_id.asc()).limit(int(limit))
    async with session_scope() as s:
        rows = (await s.execute(q)).fetchall()
    return [_row(r) for r in rows]


async def latest_by_pending_ids(pending_ids) -> dict:
    """対応待ち ID → 下書きの要約（draft_id・state・guard_ok・category）。本文は返さない（一覧の印用）。"""
    ids = [int(i) for i in (pending_ids or []) if i is not None]
    if not ids:
        return {}
    q = sa.select(reply_draft.c.draft_id, reply_draft.c.pending_id, reply_draft.c.state, reply_draft.c.guard_ok,
                  reply_draft.c.category).where(reply_draft.c.pending_id.in_(ids)).order_by(reply_draft.c.draft_id.asc())
    async with session_scope() as s:
        rows = (await s.execute(q)).fetchall()
    out: dict = {}
    for r in rows:
        out[int(r.pending_id)] = {"draft_id": int(r.draft_id), "state": r.state, "guard_ok": bool(r.guard_ok),
                                  "category": r.category}
    return out


async def exists_for_pending(pending_id: int) -> bool:
    async with session_scope() as s:
        v = (await s.execute(sa.select(reply_draft.c.draft_id).where(
            reply_draft.c.pending_id == int(pending_id)).limit(1))).scalar()
    return v is not None


async def set_state(draft_id: int, to_state: str, *, actor: str, approval_kind: str | None = None,
                    final_text: str | None = None, op_id: str | None = None, note: str | None = None,
                    from_states: tuple = (STATE_PROPOSED,), now=None) -> bool:
    """状態遷移（Q4 の承認・却下・送信結果）。from_states に無ければ False（二重操作の抑止）。履歴あり。"""
    if to_state not in STATES or (approval_kind is not None and approval_kind not in KINDS):
        return False
    now = now or _now()
    values: dict = {"state": to_state, "updated_at": now}
    if approval_kind is not None:
        values["approval_kind"] = approval_kind
    if final_text is not None:
        values["final_text"] = str(final_text)[:MAX_DRAFT_CHARS]
    if op_id is not None:
        values["op_id"] = op_id
    async with session_scope() as s:
        res = await s.execute(sa.update(reply_draft).where(reply_draft.c.draft_id == int(draft_id),
                                                           reply_draft.c.state.in_(list(from_states))).values(**values))
        if int(res.rowcount or 0) != 1:
            return False
        await _hist(s, int(draft_id), {STATE_APPROVED: ACTION_APPROVED, STATE_REJECTED: ACTION_REJECTED,
                                      STATE_SENT: ACTION_SENT, STATE_FAILED: ACTION_FAILED}.get(to_state, ACTION_CREATED),
                    actor, now, note=note)
    return True


async def history(draft_id: int) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(reply_draft_history).where(
            reply_draft_history.c.draft_id == int(draft_id)).order_by(reply_draft_history.c.history_id))).fetchall()
    return [{"action": r.action, "actor": r.actor, "at": _iso(r.at), "note": r.note} for r in rows]


# ── 生成（Claude・手本付き） ────────────────────────────────────────────────────
def build_messages(history: list[dict], user_message: str) -> list[dict]:
    out = [{"role": m["role"], "content": m["content"]} for m in (history or [])
           if m.get("role") in ("user", "assistant") and m.get("content")]
    out.append({"role": "user", "content": user_message})
    return out


async def generate_text(exemplar_rows: list[dict], history: list[dict], user_message: str) -> str:
    """Claude で下書き本文を生成（参考文例は system 末尾）。例外は呼出側で握る。"""
    import anthropic
    import chat_responder as cr
    from claude_gateway import create_message_with_fallback
    note = reply_exemplar.format_note(exemplar_rows)
    client = anthropic.AsyncAnthropic(api_key=cr._ANTHROPIC_KEY)
    resp = await create_message_with_fallback(
        client, context="顧客対応 draft(Q3b)", max_tokens=MAX_TOKENS,
        system=[{"type": "text", "text": DRAFT_SYSTEM + str(note), "cache_control": {"type": "ephemeral"}}],
        messages=build_messages(history, user_message))
    return "".join(getattr(b, "text", "") for b in resp.content if getattr(b, "type", "") == "text").strip()


def check_draft(text: str, history: list[dict], user_message: str, category: str, *, profile) -> tuple[str, bool, list]:
    """既存サーバーガードの検査のみ（マーカー除去・サニタイズ・禁止語・字数・必須語・文体・空返事）。
    戻り値: (検査後本文, 合格, 理由の列)。"""
    import chat_responder as cr
    from hub import empty_reply_guard
    out, _issues, fatal = cr.sanitize_reply_without_human_marker(str(text or ""), allowed_emoji=profile.allowed_emoji)
    reasons: list = []
    if fatal or not out.strip():
        reasons.append("プレースホルダ/内部マーカー残存" if fatal else "空の返信")
    g = cr.apply_server_guards({"reply": out, "category": category, "auto_send": True, profile.update_flag_key: False,
                                "immediate_notice": "none"}, history, user_message, sanitize_fatal=fatal, profile=profile)
    if category not in profile.auto_send_categories:
        # 承認必須カテゴリは「カテゴリ外」の降格が付くので、検査の内容だけを見る
        probe = cr.apply_server_guards({"reply": out, "category": next(iter(sorted(profile.auto_send_categories))),
                                        "auto_send": True, profile.update_flag_key: False, "immediate_notice": "none"},
                                       history, user_message, sanitize_fatal=fatal, profile=profile)
        reasons += list(probe.demotion_reasons)
    else:
        reasons += list(g.demotion_reasons)
    labels = empty_reply_guard.empty_reply_labels(out)
    if labels and "空の返信" not in reasons:
        reasons.append(send_policy.EMPTY_REPLY_REASON_PREFIX + "、".join(labels))
    seen: list = []
    for r in reasons:
        if r not in seen:
            seen.append(r)
    return out, not seen, seen


def notice_key(conv_ref: str, user_hash: str, *, today: datetime.date | None = None) -> str:
    day = (today or datetime.datetime.now(_JST).date()).isoformat()
    return f"draft_notice:{conv_ref or user_hash[:16]}:{day}"


def notice_text(pending_id: int | None, category: str | None, guard_ok: bool) -> str:
    pid = f"対応待ち ID {pending_id}" if pending_id is not None else "対応待ち ID 不明"
    cat = f"・カテゴリ: {category}" if category else ""
    return f"{NOTICE_HEADER}\n{pid}{cat}・{NOTICE_OK if guard_ok else NOTICE_NEEDS_FIX}\n{NOTICE_FOOTER}"


async def maybe_generate(user_id: str, user_message: str, history: list[dict], category: str, status: str, *,
                         profile) -> int | None:
    """顧客対応経路の承認降格（V2 ON）後に呼ぶ。下書きを作れれば保存して draft_id、作らなければ None。
    手本 0 件・対応待ちなし・既に下書きあり・生成失敗・保存失敗はすべて None（顧客へは何も送らない）。"""
    if not send_policy.enabled() or not _db_configured():
        return None
    user_hash = send_ledger.line_user_hash(BUSINESS, user_id)
    try:
        opens = await reply_pending.open_for_user(BUSINESS, user_hash)
    except Exception:
        opens = []
    if not opens:
        logger.info("[REPLY_DRAFT] skipped (no open pending)")
        return None
    pending_id = int(opens[-1]["pending_id"])
    try:
        if await exists_for_pending(pending_id):
            return None
        rows = await reply_exemplar.list_exemplars(BUSINESS, stage=reply_exemplar._stage_for_status(status),
                                                   limit=EXEMPLAR_LIMIT, category_id=category)
    except Exception:
        logger.warning("[REPLY_DRAFT] exemplar lookup failed (fixed reason)")
        return None
    if not rows:
        logger.info("[REPLY_DRAFT] skipped (no approved exemplars) count=%s", emit(0, "count", "log", "operator"))
        return None
    try:
        raw = await generate_text(rows, history, user_message)
    except Exception:
        logger.warning("[REPLY_DRAFT] generation failed (fixed reason)")
        return None
    text, ok, reasons = check_draft(raw, history, user_message, category, profile=profile)
    if not text.strip():
        logger.info("[REPLY_DRAFT] skipped (empty generation)")
        return None
    conv = None
    try:
        conv = await send_ledger.conversation_state(BUSINESS, user_id)
    except Exception:
        conv = None
    did = await create(business=BUSINESS, pending_id=pending_id, line_user_hash=user_hash,
                       conversation_ref=(conv or {}).get("ref"), category=category, inbound_text=user_message,
                       draft_text=text, guard_ok=ok, guard_reasons=reasons, exemplar_ids=[int(r["id"]) for r in rows])
    if did is None:
        return None
    logger.info("[REPLY_DRAFT] created guard_ok=%s reasons=%s", emit(int(ok), "count", "log", "operator"),
                emit(len(reasons), "count", "log", "operator"))
    try:
        await send_policy.send_business_notice(notice_key((conv or {}).get("ref", ""), user_hash),
                                               notice_text(pending_id, category, ok))
    except Exception:
        logger.warning("[REPLY_DRAFT] notice failed (fixed reason)")
    return did
