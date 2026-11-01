"""jikou_report_link — 完了報告書 PDF の本人専用 URL（JIKOU-STAGE-APP-1・QUEUE-2026-10-20-HI I-4'）

shindan_link と同型: 不透明 token（secrets.token_urlsafe(32)・氏名／LINE userId／案件 No. を含まない）を DB 表
jikou_report_link に保存し、**72 時間**有効。公開 GET /jikou/report/{token} は token が有効なときだけ PDF を返す
（無効・期限切れは 404・存在しないフリ）。token は 1 つの報告書（doc_id）に結び付く。管理者通知なし・ログは固定語彙
（token は先頭 4 文字まで）。発行は hub/jikou_stage の確認処理からのみ（画面は持たない）。
"""

from __future__ import annotations

import datetime
import logging
import re
import secrets

import sqlalchemy as sa
from fastapi import APIRouter, Request
from fastapi.responses import Response

from hub import jikou_cert
from hub import shindan_link
from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.jikou_report_link")

BUSINESS = "jikou"
TABLE_NAME = "jikou_report_link"
TABLE_NAMES = (TABLE_NAME,)
TTL_HOURS = 72
PATH = "/jikou/report"
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{32,64}$")

metadata = sa.MetaData()
jikou_report_link = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("token", sa.Text, primary_key=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("case_record_id", sa.Text, nullable=False),
    sa.Column("doc_id", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    sa.Index("ix_jikou_report_link_doc", "business", "doc_id"),
)

router = APIRouter()


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _aware(dt):
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=datetime.timezone.utc)


def token_head(token: str) -> str:
    return str(token or "")[:4]


def link_url(base_url: str, token: str) -> str:
    return f"{base_url.rstrip('/')}{PATH}/{token}"


async def issue(case_record_id: str, doc_id: str, *, now: datetime.datetime | None = None) -> str:
    """報告書 doc_id への本人専用 token を発行する（72 時間）。"""
    now = now or _now()
    token = secrets.token_urlsafe(32)
    async with session_scope() as s:
        await s.execute(sa.insert(jikou_report_link).values(
            token=token, business=BUSINESS, case_record_id=str(case_record_id), doc_id=str(doc_id),
            created_at=now, expires_at=now + datetime.timedelta(hours=TTL_HOURS)))
    logger.info("[JIKOU_REPORT_LINK] issued token=%s", emit(token_head(token), "record_id", "log", "operator"))
    return token


async def lookup(token: str, *, now: datetime.datetime | None = None) -> dict | None:
    """有効（存在・期限内）なら {"case_record_id", "doc_id"}。それ以外は None。"""
    if not token or not _TOKEN_RE.fullmatch(token):
        return None
    now = now or _now()
    async with session_scope() as s:
        r = (await s.execute(sa.select(jikou_report_link).where(jikou_report_link.c.token == token))).first()
    if r is None or _aware(r.expires_at) <= now:
        return None
    return {"case_record_id": r.case_record_id, "doc_id": r.doc_id}


def public_url_for(token: str) -> str:
    """RAILWAY_PUBLIC_DOMAIN から公開 URL を組む（shindan_link と同じ解決・未設定は空＝呼出側は送らない）。"""
    base = shindan_link.public_base_url()
    return link_url(base, token) if base else ""


async def report_download(request: Request):
    token = str(request.path_params.get("token", ""))
    hit = await lookup(token)
    if hit is None:
        return Response(status_code=404)
    try:
        doc = await jikou_cert.get_doc(hit["case_record_id"], hit["doc_id"])
    except Exception:
        doc = None
    if doc is None:
        return Response(status_code=404)
    return Response(content=doc["pdf"], media_type="application/pdf",
                    headers={"Content-Disposition": 'inline; filename="report.pdf"', "Cache-Control": "no-store, private",
                             "X-Robots-Tag": "noindex"})


router.add_api_route(PATH + "/{token}", report_download, methods=["GET"])
