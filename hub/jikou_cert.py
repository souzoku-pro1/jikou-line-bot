"""jikou_cert — 時効援用完了報告書（docx＋PDF）の生成と保管（JIKOU-CERT-1・QUEUE-2026-10-20-HI I-1）

- 対象: hub/jikou_followup の結果記録で **成立（established）** が保存された 案件×債権者（枠）。成立以外・日付欠落は生成不可。
- 文面: hub/jikou_doc_texts（要承認）。**承認台帳 hub/approved_texts に本文 SHA が載っている文言だけ**を描画する
  （TEXT-APPROVAL-GATE-1・載っていなければ GenerationRefused("text_unapproved")＝生成不可。安全文への置換はしない）。
- 雛形: docx_templates/jikou/時効援用完了報告書.docx（素の段落・明朝・太字なし・scripts/make_jikou_cert_template.py・SHA pin）。
- PDF: contract_pdf.docx_to_pdf_bytes（既存経路・段落描画）。
- 保管: DB 表 jikou_cert_doc に 案件×枠 ごとの版番号（1, 2, …）で重ねる。**上書きしない**。App への添付はしない。
- 生成は人の明示操作（/app/jikou/cert の確認付き POST）または I-4' の確認ボタン処理から呼ぶ。自動生成はしない。
- RV-10: 本文に入る相談者の情報は宛名（顧客名）のみ。LINE ユーザー ID・電話番号・メールアドレスは App 21 の値で照合し、
  本文に含まれていれば生成を拒否する。ログ・通知には案件 No.・枠・版だけ。
"""

from __future__ import annotations

import datetime
import hashlib
import io
import logging
import os
import re
import uuid
import zipfile

import sqlalchemy as sa

from hub import approved_texts, docx_builder
from hub import jikou_doc_texts as T
from hub import jikou_followup as F
from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.jikou_cert")

BUSINESS = "jikou"
TABLE_NAME = "jikou_cert_doc"
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "docx_templates", "jikou", "時効援用完了報告書.docx")
TEMPLATE_SHA256 = "9dd0abf9dbbe2f50c94b2413934985902796e2a21d28b197b7c3b14362462b92"     # scripts/make_jikou_cert_template.py の出力（決定的 zip）
_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)
SUMMARY_MAX = 400
_LINE_USER_RE = re.compile(r"U[0-9a-f]{32}")

# 事務所情報（env・値は Railway。OFFICE_BAR / OFFICE_REG は本票で名称を固定＝houki_dairinin と同じ）
OFFICE_ENV = {"事務所名": "OFFICE_NAME", "事務所住所": "OFFICE_ADDRESS", "電話": "OFFICE_TEL",
              "弁護士名": "OFFICE_ATTORNEY", "弁護士会": "OFFICE_BAR", "登録番号": "OFFICE_REG"}
# App 21 の相談者 PII（本文に入っていれば拒否）
PII_FIELDS = ("LINEユーザーID", "電話番号", "メールアドレス")
NAME_FIELD = "顧客名"

metadata = sa.MetaData()
jikou_cert_doc = sa.Table(
    TABLE_NAME, metadata,
    sa.Column("doc_id", sa.Text, primary_key=True),
    sa.Column("business", sa.Text, nullable=False),
    sa.Column("case_record_id", sa.Text, nullable=False),
    sa.Column("slot", sa.Integer, nullable=False),
    sa.Column("version", sa.Integer, nullable=False),
    sa.Column("creditor_name", sa.Text, nullable=True),
    sa.Column("docx", sa.LargeBinary, nullable=False),
    sa.Column("pdf", sa.LargeBinary, nullable=False),
    sa.Column("sha256_docx", sa.Text, nullable=False),
    sa.Column("sha256_pdf", sa.Text, nullable=False),
    sa.Column("texts_version", sa.Text, nullable=False),
    sa.Column("template_sha256", sa.Text, nullable=False),
    sa.Column("created_by", sa.Text, nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.CheckConstraint("slot IN (1, 2, 3)", name="ck_jikou_cert_doc_slot"),
    sa.CheckConstraint("version >= 1", name="ck_jikou_cert_doc_version"),
    sa.UniqueConstraint("business", "case_record_id", "slot", "version", name="uq_jikou_cert_doc_version"),
    sa.Index("ix_jikou_cert_doc_case", "business", "case_record_id"),
)
TABLE_NAMES = (TABLE_NAME,)


class GenerationRefused(RuntimeError):
    """理由は固定コード: not_established / dates_missing / creditor_missing / name_missing / text_unapproved /
    texts_pin / template_sha / placeholder_left / pii_in_body / pdf_failed / no_row"""


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _fv(record: dict | None, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def template_ok() -> bool:
    try:
        with open(TEMPLATE_PATH, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest() == TEMPLATE_SHA256
    except OSError:
        return False


def _approved(key: str) -> str:
    """文言表の本文を承認台帳で照合。台帳に無ければ生成不可（安全文への置換はしない＝文書）。"""
    body = approved_texts.approved_text(key, T.text(key))
    if body is None:
        raise GenerationRefused("text_unapproved")
    return body


def _fill(template: str, values: dict) -> str:
    out = template
    for k, v in values.items():
        out = out.replace(k, v)
    return out


def office_values() -> dict:
    return {"{{" + k + "}}": os.environ.get(env, "").strip() for k, env in OFFICE_ENV.items()}


def compose(record: dict | None, row: dict, *, today: datetime.date | None = None) -> dict:
    """App 21 レコードと結果行から雛形の差し込み値を組む。条件不足は GenerationRefused。"""
    if not T.verify_pin():
        raise GenerationRefused("texts_pin")
    if (row or {}).get("result") != F.RESULT_ESTABLISHED:
        raise GenerationRefused("not_established")
    shipped_on, answered_on = row.get("shipped_on"), row.get("answered_on")
    if not shipped_on or not answered_on:
        raise GenerationRefused("dates_missing")
    creditor = str(row.get("creditor_name") or "").strip()
    if not creditor:
        raise GenerationRefused("creditor_missing")
    name = _fv(record, NAME_FIELD)
    if not name:
        raise GenerationRefused("name_missing")
    today = today or datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).date()
    summary = " ".join(str(row.get("summary") or "").split())[:SUMMARY_MAX] or "（記録なし）"
    slots = {"{{宛名}}": name, "{{債権者名}}": creditor, "{{通知発送日}}": docx_builder.to_wareki(shipped_on),
             "{{回答受領日}}": docx_builder.to_wareki(answered_on), "{{回答要旨}}": summary,
             "{{作成日}}": docx_builder.to_wareki(today), **office_values()}
    texts = {k: _approved(k) for k in T.TEXT_IDS}          # 全文言が台帳に載っていることが前提（1 件でも無ければ拒否）
    return {
        "{{表題}}": _fill(texts["C-1"], slots), "{{作成日}}": _fill(texts["C-8"], slots), "{{宛名}}": _fill(texts["C-2"], slots),
        "{{本文}}": _fill(texts["C-3"], slots),
        "{{明細1}}": _fill(texts["C-4"], slots), "{{明細2}}": _fill(texts["C-5"], slots),
        "{{明細3}}": _fill(texts["C-6"], slots), "{{明細4}}": _fill(texts["C-7"], slots),
        "{{署名1}}": _fill(texts["C-9"], slots), "{{署名2}}": _fill(texts["C-10"], slots),
        "{{署名3}}": _fill(texts["C-11"], slots), "{{署名4}}": _fill(texts["C-12"], slots),
    }


def forbidden_values(record: dict | None) -> tuple[str, ...]:
    """本文に含まれてはいけない相談者の値（App 21 の LINE ユーザー ID・電話番号・メールアドレス・非空のみ）。"""
    vals = []
    for code in PII_FIELDS:
        v = _fv(record, code)
        if v:
            vals.append(v)
            digits = re.sub(r"\D", "", v)
            if len(digits) >= 10:
                vals.append(digits)
    return tuple(vals)


def _extract_text(docx_bytes: bytes) -> str:
    from docx import Document
    d = Document(io.BytesIO(docx_bytes))
    return "\n".join(p.text for p in d.paragraphs)


def _deterministic_zip(docx_bytes: bytes) -> bytes:
    src = zipfile.ZipFile(io.BytesIO(docx_bytes))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            zi = zipfile.ZipInfo(info.filename, date_time=_FIXED_ZIP_TIME)
            zi.compress_type = zipfile.ZIP_DEFLATED
            zi.external_attr = info.external_attr
            dst.writestr(zi, src.read(info.filename))
    return buf.getvalue()


def render(data: dict, *, forbidden: tuple[str, ...] = ()) -> bytes:
    if not template_ok():
        raise GenerationRefused("template_sha")
    raw = docx_builder.fill_template_multiline(TEMPLATE_PATH, data)
    text = _extract_text(raw)
    if "{{" in text:
        raise GenerationRefused("placeholder_left")
    if _LINE_USER_RE.search(text) or any(v and v in text for v in forbidden):
        raise GenerationRefused("pii_in_body")
    return _deterministic_zip(raw)


def to_pdf(docx_bytes: bytes) -> bytes:
    import contract_pdf
    try:
        return contract_pdf.docx_to_pdf_bytes(docx_bytes)
    except Exception as e:  # noqa: BLE001  既存経路の失敗は固定コードへ（本文・例外文は出さない）
        raise GenerationRefused("pdf_failed") from e


async def store(case_record_id: str, slot: int, creditor_name: str | None, docx: bytes, pdf: bytes, *,
                actor: str, now: datetime.datetime | None = None) -> dict:
    """案件×枠の次の版で保存（上書きしない）。"""
    now = now or _now()
    async with session_scope() as s:
        t = jikou_cert_doc
        cur = (await s.execute(sa.select(sa.func.max(t.c.version)).where(
            t.c.business == BUSINESS, t.c.case_record_id == str(case_record_id), t.c.slot == int(slot)))).scalar()
        version = int(cur or 0) + 1
        did = uuid.uuid4().hex
        await s.execute(sa.insert(t).values(
            doc_id=did, business=BUSINESS, case_record_id=str(case_record_id), slot=int(slot), version=version,
            creditor_name=creditor_name, docx=docx, pdf=pdf, sha256_docx=hashlib.sha256(docx).hexdigest(),
            sha256_pdf=hashlib.sha256(pdf).hexdigest(), texts_version=T.TABLE_SHA256_PIN[:16],
            template_sha256=TEMPLATE_SHA256, created_by=actor, created_at=now))
    logger.info("[JIKOU_CERT] stored case=%s slot=%s version=%s", emit(str(case_record_id), "record_id", "log", "operator"),
                emit(int(slot), "count", "log", "operator"), emit(version, "count", "log", "operator"))
    return {"doc_id": did, "version": version}


async def generate(case_record_id: str, slot: int, *, actor: str, record: dict | None = None,
                   today: datetime.date | None = None, now: datetime.datetime | None = None) -> dict:
    """人の明示操作（または I-4' の確認処理）から呼ぶ生成の本体。失敗は GenerationRefused（固定コード）。"""
    rows = {int(r["slot"]): r for r in await F.rows_for_case(str(case_record_id))}
    row = rows.get(int(slot))
    if row is None:
        raise GenerationRefused("no_row")
    if record is None:
        from hub import kintone
        try:
            record = await kintone.get_record(F.APP21, str(case_record_id))
        except Exception as e:  # noqa: BLE001
            raise GenerationRefused("name_missing") from e
    data = compose(record, row, today=today)
    docx = render(data, forbidden=forbidden_values(record))
    pdf = to_pdf(docx)
    out = await store(case_record_id, slot, row.get("creditor_name"), docx, pdf, actor=actor, now=now)
    out["slot"] = int(slot)
    return out


async def list_docs(case_record_id: str) -> list[dict]:
    async with session_scope() as s:
        t = jikou_cert_doc
        rows = (await s.execute(sa.select(t.c.doc_id, t.c.slot, t.c.version, t.c.creditor_name, t.c.created_at, t.c.created_by)
                                .where(t.c.business == BUSINESS, t.c.case_record_id == str(case_record_id))
                                .order_by(t.c.slot.asc(), t.c.version.desc()))).fetchall()
    return [dict(r._mapping) for r in rows]


async def get_doc(case_record_id: str, doc_id: str) -> dict | None:
    async with session_scope() as s:
        t = jikou_cert_doc
        r = (await s.execute(sa.select(t).where(t.c.business == BUSINESS, t.c.case_record_id == str(case_record_id),
                                                t.c.doc_id == str(doc_id)))).first()
    return dict(r._mapping) if r else None
