"""時効 LINE 友だち追加 → 本人専用の診断フォームリンク — SHINDAN-LINE-LINK-1

裁定:
 A. 送信契機は時効 /webhook の follow イベントのみ（unfollow・再 follow 以外では送らない）。
    冪等キーは webhookEventId（durable lane の既存規則=record_line_event）。配線は main.py
 B. リンクの符号（token）は LINE userId や氏名を一切含めない不透明値
    （secrets.token_urlsafe(32)）。durable DB の shindan_link（token 主キー・line_user_id・
    created_at・expires_at・used_at）に保存。有効期限 TTL_DAYS=30
 C. URL は既存 /shindan にクエリ k=token を付けるだけ
 G. 管理者通知は出さない。ログは固定語彙のみ（token は先頭 4 文字まで・userId 全文は出さない）
 H. 新規 env なし。公開ホストは Railway が注入する RAILWAY_PUBLIC_DOMAIN（既存の platform 変数）
    のみ（fix1 SLL-01: リクエスト由来の Host 等は使わない・未設定は fail-closed で送らない）
"""

import datetime
import logging
import os
import secrets

import sqlalchemy as sa

from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.shindan_link")

# app-state 専用 metadata（alembic env.py の target_metadata list に統合）
metadata = sa.MetaData()

shindan_link = sa.Table(
    "shindan_link", metadata,
    sa.Column("token", sa.Text, primary_key=True),
    sa.Column("line_user_id", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
              server_default=sa.func.now()),
    sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
    # fix2 SLL-02: 書込前の予約（同一 token の同時 POST による二重作成の防止）。
    # 予約は CLAIM_TTL_SEC で自然解放（書込失敗時は release で即時解放）
    sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
)

CLAIM_TTL_SEC = 120

# ── 凍結文言（逐語・改変禁止・sha256 pin は test_shindan_line_link） ────────────
FROZEN_GREETING = (
    "ご登録ありがとうございます。まず1分ほどの簡易診断にお答えいただくと、"
    "ご案内がスムーズになります(診断を希望されない場合は、そのままご質問を"
    "お送りいただくこともできます)。")

TTL_DAYS = 30
QUERY_KEY = "k"
FORM_PATH = "/shindan"
_PUBLIC_DOMAIN_ENV = "RAILWAY_PUBLIC_DOMAIN"       # Railway 注入の既存 platform 変数


def token_head(token: str) -> str:
    """ログ用（先頭 4 文字まで・裁定 G）。"""
    return str(token or "")[:4]


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _aware(dt) -> datetime.datetime | None:
    """sqlite は naive で返るため UTC とみなして aware 化（PostgreSQL は aware のまま）。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt


NO_PUBLIC_HOST_REASON = "shindan_link_no_public_host"


def public_base_url() -> str:
    """リンクの基底 URL（https 固定）。fix1（SLL-01）: RAILWAY_PUBLIC_DOMAIN **のみ**。
    未設定・空・不正形は空文字＝呼び出し側は token を発行せず送らない（fail-closed）。
    Host / X-Forwarded-Host 等のリクエスト由来の値は一切使わない。"""
    domain = os.environ.get(_PUBLIC_DOMAIN_ENV, "").strip()
    if not domain or "/" in domain or " " in domain:
        return ""
    return f"https://{domain}"


def link_url(base_url: str, token: str) -> str:
    return f"{base_url.rstrip('/')}{FORM_PATH}?{QUERY_KEY}={token}"


def build_message(url: str) -> str:
    """送信本文=凍結文言+改行+URL のみ（他の文は付けない）。"""
    return FROZEN_GREETING + "\n" + url


async def issue(line_user_id: str, now: datetime.datetime | None = None) -> str:
    """不透明 token を発行して保存し、token を返す。"""
    now = now or _now()
    token = secrets.token_urlsafe(32)
    async with session_scope() as s:
        await s.execute(sa.insert(shindan_link).values(
            token=token, line_user_id=line_user_id, created_at=now,
            expires_at=now + datetime.timedelta(days=TTL_DAYS), used_at=None))
    logger.info("[SHINDAN_LINK] issued head=%s",
                emit(token_head(token), "record_id", "log", "operator"))
    return token


async def lookup(token: str, now: datetime.datetime | None = None,
                 include_claimed: bool = False) -> dict | None:
    """有効（存在・期限内・未使用）なら {"token","line_user_id","expires_at"}。
    それ以外は None。fix2: 予約中（claimed_at が CLAIM_TTL_SEC 以内）は既定で無効
    （GET の cookie 付与を防ぐ）。POST は include_claimed=True で予約中も返し、
    claim の結果（busy）で扱う＝k なし経路へ落ちない。"""
    if not token:
        return None
    now = now or _now()
    async with session_scope() as s:
        row = (await s.execute(
            sa.select(shindan_link.c.line_user_id, shindan_link.c.expires_at,
                      shindan_link.c.used_at, shindan_link.c.claimed_at)
            .where(shindan_link.c.token == token))).first()
    if row is None:
        return None
    line_user_id, expires_at, used_at, claimed_at = row
    if used_at is not None:
        return None
    expires_at = _aware(expires_at)
    if expires_at is None or expires_at <= now:
        return None
    claimed_at = _aware(claimed_at)
    if (not include_claimed and claimed_at is not None
            and claimed_at > now - datetime.timedelta(seconds=CLAIM_TTL_SEC)):
        return None
    return {"token": token, "line_user_id": line_user_id, "expires_at": expires_at}


async def claim(token: str, now: datetime.datetime | None = None
                ) -> datetime.datetime | None:
    """fix2 SLL-02: 書込前の予約。未使用・期限内・未予約（または予約が
    CLAIM_TTL_SEC 超過）のときだけ claimed_at=now を書き、rowcount=1 なら now
    （自分の予約の印）を返す。取れなければ None（呼び出し側は busy）。"""
    now = now or _now()
    stale = now - datetime.timedelta(seconds=CLAIM_TTL_SEC)
    async with session_scope() as s:
        res = await s.execute(
            sa.update(shindan_link)
            .where(shindan_link.c.token == token,
                   shindan_link.c.used_at.is_(None),
                   shindan_link.c.expires_at > now,
                   sa.or_(shindan_link.c.claimed_at.is_(None),
                          shindan_link.c.claimed_at < stale))
            .values(claimed_at=now))
        return now if (res.rowcount or 0) == 1 else None


async def release(token: str, claimed_at: datetime.datetime) -> bool:
    """fix2 SLL-02: 書込失敗時の解放（claimed_at を NULL に戻す）。
    fix3 SLL-06: 解放できるのは**自分の予約**だけ（token 一致 AND used_at IS NULL AND
    claimed_at = 自分の予約時刻）。TTL 超過後に他者が再予約していれば rowcount 0 で
    False（例外にしない）＝旧予約者が新予約者の予約を壊せない。"""
    async with session_scope() as s:
        res = await s.execute(
            sa.update(shindan_link)
            .where(shindan_link.c.token == token, shindan_link.c.used_at.is_(None),
                   shindan_link.c.claimed_at == claimed_at)
            .values(claimed_at=None))
        return (res.rowcount or 0) == 1


async def find_user_record(app, line_user_id: str) -> tuple[dict | None, str]:
    """E-1: line_user_id で App 21 を検索（hearing 側と同じ欄 LINEユーザーID・limit 2）。
    (record, method)。method: found / none / ambiguous / failed。
    ちょうど 1 件のときだけ GET した最新レコードを返す（複数件は書かない）。"""
    from hub import kintone as hub_kintone
    try:
        rows = await hub_kintone.search_records(
            app, f'LINEユーザーID = "{line_user_id}" order by $id asc limit 2',
            fields=["$id"])
        if not rows:
            return None, "none"
        if len(rows) >= 2:
            return None, "ambiguous"
        rid = str(((rows[0].get("$id") or {}).get("value") or "")).strip()
        if not rid:
            return None, "failed"
        return await hub_kintone.get_record(app, rid), "found"
    except hub_kintone.KintoneError:
        return None, "failed"


async def mark_used(token: str, claimed_at: datetime.datetime,
                    now: datetime.datetime | None = None) -> bool:
    """used_at を打つ（未使用かつ claimed_at が自分の予約のときだけ・rowcount 1 で
    True）。fix2 SLL-02: 他者の予約や解放後の再予約を確定させない。"""
    now = now or _now()
    async with session_scope() as s:
        res = await s.execute(
            sa.update(shindan_link)
            .where(shindan_link.c.token == token, shindan_link.c.used_at.is_(None),
                   shindan_link.c.claimed_at == claimed_at)
            .values(used_at=now))
        return (res.rowcount or 0) == 1
