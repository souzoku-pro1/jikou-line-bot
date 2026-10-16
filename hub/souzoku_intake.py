"""souzoku_intake — SOUZOKU-2: LINE／Web からの相続相談の受付（第 1 段階・受付のみ・既定 OFF）

QUEUE-2026-10-16-R G-5。正本: docs/souzoku-1-design.md §1（受付）・§8.2（DB と kintone の分担）・§9 SOUZOKU-2。

- flag `SOUZOKU_INTAKE_ENABLED`（既定 OFF）。OFF のとき受付の route は 404・ジョブなし・既存の時効／相続放棄フローに一切触れない
  （本 module は既存 module を変更せず、main.py 末尾の結線 3 行だけ）。
- 質問票 6 問（閉集合・hub/souzoku_constants）: 1 被相続人の氏名（ふりがな付き）2 死亡日 3 相続人の続柄と人数 4 遺言の有無
  5 主な財産の種類（複数選択）6 争いの有無。6=ある は「当事務所では扱っていない」旨（要承認文言 S-2）で終了（記録は status=out_of_scope で残す＝RV-08）。
- 書込先: kintone の相続アプリは未作成のため、同一 DB の表 `souzoku_intake`（migration 作成のみ・未適用）。kintone 同期は保留一覧（欄名案は
  docs/souzoku-1-design.md §1 App S1）。
- 受付完了時に業務 LINE へ通知 1 通（送信台帳 business=souzoku・channel=souzoku_notice・purpose=other・受信イベント ID
  `souzoku_intake:{intake_id}`・HOUKI-JUKURYO の send_notice と同型＝台帳不可は送らない・自動再送なし）。通知は
  「相続相談 1 件・相続人 n 名・財産種別 x・遺言 y・争い z」だけ（氏名・続柄の個別値は載せない＝RV-10）。
- 相談者への自動返信は受付確認 1 通のみ（要承認文言 S-1・**画面表示**。LINE push はチャネルの裁定後＝保留）。新しい送信経路は作らない
  （hub/line_channel を import しない）。
- 相続人の氏名・続柄は PII: 表にだけ置き、通知・ログには件数と種別のみ。
"""

import datetime
import json
import logging
import os
import re
import unicodedata
import uuid

import sqlalchemy as sa

from hub import send_ledger
from hub.db import DatabaseNotConfigured, session_scope
from hub.notify import business_channel_allowlist, business_token_env, get_admin_line_user_id, push_line_message
from hub.redact import emit
from hub.souzoku_constants import (DISPUTE_OPTIONS, DISPUTE_OUT_OF_SCOPE, HEIR_RELATIONS, INTAKE_MAX_HEIRS_PER_RELATION,
                                   INTAKE_MAX_NAME_CHARS, PROPERTY_TYPES, WILL_OPTIONS)

logger = logging.getLogger("hub.souzoku_intake")

metadata = sa.MetaData()
FLAG_ENV = "SOUZOKU_INTAKE_ENABLED"
BUSINESS = "souzoku"
LEDGER_CHANNEL = "souzoku_notice"
LEDGER_KIND = "souzoku_intake"
TABLE_NAME = "souzoku_intake"
HISTORY_TABLE_NAME = "souzoku_intake_history"
TABLE_NAMES = (TABLE_NAME, HISTORY_TABLE_NAME)

STATUS_RECEIVED = "received"
STATUS_OUT_OF_SCOPE = "out_of_scope"
STATUSES = (STATUS_RECEIVED, STATUS_OUT_OF_SCOPE)
NOTIFY_NONE = "none"
NOTIFY_SENT = "sent"
NOTIFY_THROTTLED = "throttled"
NOTIFY_FAILED = "failed"
NOTIFY_UNCONFIRMED = "unconfirmed"
NOTIFY_UNAVAILABLE = "unavailable"
NOTIFY_STATES = (NOTIFY_NONE, NOTIFY_SENT, NOTIFY_THROTTLED, NOTIFY_FAILED, NOTIFY_UNCONFIRMED, NOTIFY_UNAVAILABLE)
SOURCE_WEB = "web"
NOTICE_HEADER = "【相続相談 受付】"
_KANA_RE = re.compile(r"^[ぁ-ゖー\s]+$")

souzoku_intake = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("intake_id", sa.Text, primary_key=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("source", sa.Text, nullable=False),
    sa.Column("line_user_hash", sa.Text, nullable=True),
    sa.Column("deceased_name", sa.Text, nullable=False),
    sa.Column("deceased_kana", sa.Text, nullable=False),
    sa.Column("death_on", sa.Date, nullable=False),
    sa.Column("heirs_json", sa.Text, nullable=False),            # [{"relation": "子", "count": 2}, ...]
    sa.Column("heirs_total", sa.Integer, nullable=False),
    sa.Column("will_status", sa.Text, nullable=False),
    sa.Column("property_types", sa.Text, nullable=False),        # "不動産,預貯金"
    sa.Column("dispute", sa.Text, nullable=False),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("notify_state", sa.Text, nullable=False, server_default=NOTIFY_NONE),
    sa.Column("inbound_event_id", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("status IN ('received', 'out_of_scope')", name="ck_souzoku_intake_status"),
    sa.CheckConstraint("notify_state IN ('none', 'sent', 'throttled', 'failed', 'unconfirmed', 'unavailable')",
                       name="ck_souzoku_intake_notify"),
    sa.UniqueConstraint("inbound_event_id", name="uq_souzoku_intake_event"),
    sa.Index("ix_souzoku_intake_created", "business", "created_at"),
)

souzoku_intake_history = sa.Table(
    HISTORY_TABLE_NAME, metadata,
    sa.Column("history_id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True, autoincrement=True),
    sa.Column("intake_id", sa.Text, sa.ForeignKey("souzoku_intake.intake_id"), nullable=False),
    sa.Column("field", sa.Text, nullable=False),
    sa.Column("from_value", sa.Text, nullable=True),
    sa.Column("to_value", sa.Text, nullable=False),
    sa.Column("actor", sa.Text, nullable=False),
    sa.Column("at", sa.DateTime(timezone=True), nullable=False),
    sa.Index("ix_souzoku_intake_history_i", "intake_id"),
)


# ── 純関数 ────────────────────────────────────────────────────────────────────
def enabled() -> bool:
    return os.environ.get(FLAG_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def normalize_kana(text) -> str:
    """ふりがなを ひらがな（NFKC・カタカナ→ひらがな・空白は 1 つ）に正規化。ひらがな以外が残れば ""。"""
    s = unicodedata.normalize("NFKC", str(text or "")).strip()
    s = "".join(chr(ord(c) - 0x60) if "ァ" <= c <= "ヶ" else c for c in s)
    s = " ".join(s.split())
    return s if s and _KANA_RE.fullmatch(s) else ""


def parse_date(text) -> datetime.date | None:
    s = unicodedata.normalize("NFKC", str(text or "")).strip().replace("/", "-")
    if not re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", s):
        return None
    try:
        y, m, d = (int(x) for x in s.split("-"))
        return datetime.date(y, m, d)
    except ValueError:
        return None


def validate(form: dict, *, today: datetime.date | None = None) -> tuple[dict, list[str]]:
    """質問票 6 問の検査。戻り値 (data, errors)。errors は固定語彙（入力値を含まない）。"""
    today = today or datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).date()
    errors: list[str] = []
    name = " ".join(str(form.get("deceased_name", "")).split())
    if not name or len(name) > INTAKE_MAX_NAME_CHARS:
        errors.append("deceased_name")
    kana = normalize_kana(form.get("deceased_kana", ""))
    if not kana or len(kana) > INTAKE_MAX_NAME_CHARS:
        errors.append("deceased_kana")
    death = parse_date(form.get("death_on", ""))
    if death is None or death > today or death.year < 1900:
        errors.append("death_on")
    heirs = []
    total = 0
    for rel in HEIR_RELATIONS:
        raw = str(form.get(f"heirs_{rel}", "") or "").strip()
        if not raw:
            continue
        if not raw.isdigit() or int(raw) < 0 or int(raw) > INTAKE_MAX_HEIRS_PER_RELATION:
            errors.append("heirs")
            break
        n = int(raw)
        if n:
            heirs.append({"relation": rel, "count": n})
            total += n
    if "heirs" not in errors and total == 0:
        errors.append("heirs")
    will = str(form.get("will_status", ""))
    if will not in WILL_OPTIONS:
        errors.append("will_status")
    props_raw = form.get("property_types", [])
    if isinstance(props_raw, str):
        props_raw = [props_raw]
    props = [p for p in PROPERTY_TYPES if p in set(str(x) for x in props_raw)]
    if not props:
        errors.append("property_types")
    dispute = str(form.get("dispute", ""))
    if dispute not in DISPUTE_OPTIONS:
        errors.append("dispute")
    data = {"deceased_name": name, "deceased_kana": kana, "death_on": death, "heirs": heirs, "heirs_total": total,
            "will_status": will, "property_types": props, "dispute": dispute}
    return data, errors


def status_for(data: dict) -> str:
    return STATUS_OUT_OF_SCOPE if data.get("dispute") == DISPUTE_OUT_OF_SCOPE else STATUS_RECEIVED


def ledger_key(intake_id: str) -> str:
    return f"{LEDGER_KIND}:{intake_id}"


def notice_text(data: dict, status: str) -> str:
    """件数と種別のみ（氏名・続柄の個別値・日付は載せない）。"""
    kind = "対象外（争いあり）" if status == STATUS_OUT_OF_SCOPE else "受付"
    return (f"{NOTICE_HEADER}{kind} 1 件・相続人 {int(data.get('heirs_total', 0))} 名・財産種別 "
            f"{'／'.join(data.get('property_types', [])) or '（未選択）'}・遺言 {data.get('will_status', '')}・争い {data.get('dispute', '')}。"
            "詳細は DB（souzoku_intake）を確認してください。")


# ── 表 ───────────────────────────────────────────────────────────────────────
async def save(data: dict, *, source: str = SOURCE_WEB, line_user_hash: str | None = None, now=None) -> str:
    """1 件保存（履歴 created）。戻り値 intake_id。DB 未設定は DatabaseNotConfigured。"""
    if not os.environ.get("DATABASE_URL"):
        raise DatabaseNotConfigured("DATABASE_URL is not set")
    now = now or _now()
    intake_id = uuid.uuid4().hex
    status = status_for(data)
    async with session_scope() as s:
        await s.execute(sa.insert(souzoku_intake).values(
            intake_id=intake_id, business=BUSINESS, source=source, line_user_hash=line_user_hash,
            deceased_name=data["deceased_name"], deceased_kana=data["deceased_kana"], death_on=data["death_on"],
            heirs_json=json.dumps(data["heirs"], ensure_ascii=False), heirs_total=int(data["heirs_total"]),
            will_status=data["will_status"], property_types=",".join(data["property_types"]), dispute=data["dispute"],
            status=status, notify_state=NOTIFY_NONE, inbound_event_id=ledger_key(intake_id), created_at=now, updated_at=now))
        await s.execute(sa.insert(souzoku_intake_history).values(
            intake_id=intake_id, field="status", from_value=None, to_value=status, actor="system", at=now))
    logger.info("[SOUZOKU_INTAKE] saved status=%s heirs=%s", emit(status, "freetext", "log", "operator"),
                emit(int(data["heirs_total"]), "count", "log", "operator"))
    return intake_id


async def set_notify_state(intake_id: str, state: str, *, now=None) -> None:
    if state not in NOTIFY_STATES:
        return
    now = now or _now()
    async with session_scope() as s:
        r = (await s.execute(sa.select(souzoku_intake.c.notify_state).where(souzoku_intake.c.intake_id == intake_id))).first()
        if r is None:
            return
        await s.execute(sa.update(souzoku_intake).where(souzoku_intake.c.intake_id == intake_id)
                        .values(notify_state=state, updated_at=now))
        await s.execute(sa.insert(souzoku_intake_history).values(
            intake_id=intake_id, field="notify_state", from_value=r.notify_state, to_value=state, actor="system", at=now))


async def get(intake_id: str) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(souzoku_intake).where(souzoku_intake.c.intake_id == intake_id))).first()
    return dict(r._mapping) if r else None


async def count(*, status: str | None = None) -> int:
    q = sa.select(sa.func.count()).select_from(souzoku_intake).where(souzoku_intake.c.business == BUSINESS)
    if status:
        q = q.where(souzoku_intake.c.status == status)
    async with session_scope() as s:
        return int((await s.execute(q)).scalar() or 0)


# ── 業務 LINE 通知（台帳経由・HOUKI-JUKURYO の send_notice と同型・自動再送なし）──────
async def _push_business(admin_id: str, text: str) -> bool:
    return await push_line_message(admin_id, text, token_env=business_token_env())


async def send_notice(intake_id: str, text: str) -> str:
    """戻り値は NOTIFY_*。台帳が使えなければ送らない（fail-closed）。"""
    admin_id = get_admin_line_user_id()
    if not admin_id or admin_id not in business_channel_allowlist() or not os.environ.get(business_token_env(), ""):
        logger.warning("[SOUZOKU_INTAKE] notice skipped (recipient or token not configured)")
        return NOTIFY_FAILED
    token = send_ledger.bind_inbound(ledger_key(intake_id))
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(BUSINESS, LEDGER_CHANNEL, admin_id, text)
        if op is send_ledger.DUPLICATE_SENT:
            return NOTIFY_THROTTLED
        if op is send_ledger.DUPLICATE_UNCONFIRMED:
            return NOTIFY_UNCONFIRMED
        if op is None or not isinstance(op, send_ledger.Operation):
            logger.warning("[SOUZOKU_INTAKE] notice skipped (ledger unavailable)")
            return NOTIFY_UNAVAILABLE
        try:
            ok = await send_ledger.guarded_http(op, _push_business(admin_id, text))
        except Exception:
            await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
            return NOTIFY_UNCONFIRMED
        led = await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
        if not ok:
            return NOTIFY_FAILED
        return NOTIFY_SENT if led == "applied" else NOTIFY_UNCONFIRMED
    finally:
        send_ledger.unbind(token)


async def receive(data: dict, *, source: str = SOURCE_WEB, line_user_hash: str | None = None, now=None) -> tuple[str, str, str]:
    """保存 → 通知（1 通・台帳 key）→ notify_state の記録。戻り値 (intake_id, status, notify_state)。"""
    intake_id = await save(data, source=source, line_user_hash=line_user_hash, now=now)
    status = status_for(data)
    try:
        state = await send_notice(intake_id, notice_text(data, status))
    except Exception:
        logger.warning("[SOUZOKU_INTAKE] notice failed (fixed reason)")
        state = NOTIFY_FAILED
    try:
        await set_notify_state(intake_id, state, now=now)
    except Exception:
        logger.warning("[SOUZOKU_INTAKE] notify_state write failed (fixed reason)")
    return intake_id, status, state
