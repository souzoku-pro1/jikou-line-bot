"""souzoku_kyogisho — 遺産分割協議書（案）docx の生成と保管（SOUZOKU-4・QUEUE-2026-10-17-H H-5）

- 雛形 docx_templates/souzoku/遺産分割協議書.docx（素の段落のみ・明朝・太字なし・scripts/make_souzoku_kyogisho_template.py・SHA pin）。
- 条文は hub/souzoku_texts（要承認・pin 不一致なら生成しない）。差し込みだけで LLM は使わない。
- 生成は人の明示操作（画面）のみ。App には添付せず DB（souzoku_kyogisho）に版を重ねて保管し、画面からダウンロードする。
- 本文に入るのは 被相続人の氏名・死亡日・相続人ラベル・財産の表示（口座は下 4 桁）・金額。相続人の氏名・住所・生年月日は入れない
  （空欄線＝弁護士が記入・docs §4「協議書に載る氏名・住所は kintone の値のみ・AI が補完しない」）。
"""
import datetime
import hashlib
import io
import logging
import os
import re
import uuid
import zipfile

import sqlalchemy as sa

from hub import docx_builder
from hub import souzoku_assets as A
from hub import souzoku_texts as T
from hub.db import session_scope
from hub.redact import emit

logger = logging.getLogger("hub.souzoku_kyogisho")

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docx_templates", "souzoku", "遺産分割協議書.docx")
TEMPLATE_SHA256 = "635ab6aa1fcbdb3ab45e0318b781a6f540522ccd1fac13648f9d72c45bdc9328"     # scripts/make_souzoku_kyogisho_template.py の出力
PLACEHOLDERS = ("{{透かし}}", "{{表題}}", "{{前文}}", "{{相続人一覧}}", "{{取得条項}}", "{{代償金条項}}", "{{記載外条項}}", "{{注記}}",
                "{{後文}}", "{{作成日}}", "{{署名欄}}")
_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)
_LONG_DIGITS_RE = re.compile(r"\d{5,}")          # 口座番号の全桁らしき数字列（金額は 3 桁区切りで書くため当たらない）
_DUE_DEFAULT = "別途定める日"


class GenerationRefused(RuntimeError):
    pass


def template_ok() -> bool:
    try:
        with open(TEMPLATE_PATH, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest() == TEMPLATE_SHA256
    except OSError:
        return False


def _yen(n: int) -> str:
    return f"{int(n):,}"


def _wareki_or_iso(d) -> str:
    try:
        return docx_builder.to_wareki(d) if d else "＿＿年＿＿月＿＿日"
    except Exception:
        return str(d)


def compose(intake: dict, heirs_result: dict, assets: list[dict], *, today=None) -> dict:
    """差し込み値を組む（純関数）。条文が空（pin 不一致）なら GenerationRefused。"""
    if not T.verify_pin() or not all(T.text(k) for k in T.TEXT_IDS):
        raise GenerationRefused("texts_pin")
    heirs = list(heirs_result.get("heirs") or [])
    if not heirs:
        raise GenerationRefused("no_heirs")
    active = [a for a in assets if a.get("status", A.STATUS_ACTIVE) == A.STATUS_ACTIVE]
    today = today or datetime.date.today()
    heir_lines = [T.text("K-3").replace("{{続柄}}", h["label"]).replace("{{法定相続分}}", h["share"]) for h in heirs]
    clauses, comps, notes, art = [], [], [], 0
    numbered = {a["asset_id"]: i for i, a in enumerate(active, start=1)}
    for h in heirs:
        mine = [a for a in active if a.get("acquirer_label") == h["label"]]
        if not mine:
            continue
        art += 1
        lines = [T.text("K-4").replace("{{条}}", str(art)).replace("{{続柄}}", h["label"])]
        for a in mine:
            lines.append(T.text("K-5").replace("{{番号}}", str(numbered[a["asset_id"]])).replace("{{財産}}", A.describe(a["kind"], a["detail"])))
        clauses.append("\n".join(lines))
        for a in mine:
            comp = a.get("compensation") or {}
            if comp:
                art += 1
                comps.append(T.text("K-6").replace("{{条}}", str(art)).replace("{{続柄}}", h["label"]).replace("{{相手}}", comp["to"])
                             .replace("{{金額}}", _yen(comp["yen"])).replace("{{支払期限}}", comp.get("due") or _DUE_DEFAULT))
    unallocated = [A.describe(a["kind"], a["detail"]) for a in active if not a.get("acquirer_label")]
    if unallocated:
        notes.append(T.text("K-10").replace("{{財産}}", "／".join(unallocated)))
    if any(a.get("value_yen") is None for a in active):
        notes.append(T.text("K-11"))
    art += 1
    other = T.text("K-7").replace("{{条}}", str(art))
    data = {
        "{{透かし}}": T.text("K-0"),
        "{{表題}}": T.text("K-1"),
        "{{前文}}": T.text("K-2").replace("{{被相続人}}", str(intake.get("deceased_name") or "")).replace("{{死亡日}}", _wareki_or_iso(intake.get("death_on"))),
        "{{相続人一覧}}": "\n".join(heir_lines),
        "{{取得条項}}": "\n".join(clauses) if clauses else "（取得条項なし＝割付が未入力）",
        "{{代償金条項}}": "\n".join(comps),
        "{{記載外条項}}": other,
        "{{注記}}": "\n".join(notes),
        "{{後文}}": T.text("K-8").replace("{{通数}}", str(len(heirs))),
        "{{作成日}}": _wareki_or_iso(today),
        "{{署名欄}}": "\n\n".join(T.text("K-9").replace("{{続柄}}", h["label"]) for h in heirs),
    }
    return data


def render(data: dict) -> bytes:
    if not template_ok():
        raise GenerationRefused("template_sha")
    raw = docx_builder.fill_template_multiline(TEMPLATE_PATH, data)
    text = _extract_text(raw)
    if "{{" in text:
        raise GenerationRefused("placeholder_left")
    if _LONG_DIGITS_RE.search(text):                 # 口座番号の全桁らしき数字列は本文に入れない（下 4 桁のみ）
        raise GenerationRefused("long_digits")
    if not text.startswith(T.text("K-0")):
        raise GenerationRefused("watermark")
    return _deterministic_zip(raw)


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


async def store(intake_id: str, content: bytes, *, actor: str = "human", now=None) -> dict:
    now = now or datetime.datetime.now(datetime.timezone.utc)
    did = uuid.uuid4().hex
    sha = hashlib.sha256(content).hexdigest()
    async with session_scope() as s:
        cur = (await s.execute(sa.select(sa.func.max(A.souzoku_kyogisho.c.version)).where(
            A.souzoku_kyogisho.c.intake_id == str(intake_id)))).scalar()
        version = int(cur or 0) + 1
        await s.execute(sa.insert(A.souzoku_kyogisho).values(
            doc_id=did, intake_id=str(intake_id), business=A.BUSINESS, version=version, sha256=sha, size_bytes=len(content),
            content=content, texts_version=T.TABLE_SHA256_PIN[:16], template_sha256=TEMPLATE_SHA256, created_by=actor, created_at=now))
    logger.info("[SOUZOKU_KYOGISHO] generated version=%s bytes=%s", emit(version, "count", "log", "operator"),
                emit(len(content), "count", "log", "operator"))
    return {"doc_id": did, "version": version, "sha256": sha}


async def list_docs(intake_id: str) -> list[dict]:
    async with session_scope() as s:
        rows = (await s.execute(sa.select(A.souzoku_kyogisho.c.doc_id, A.souzoku_kyogisho.c.version, A.souzoku_kyogisho.c.sha256,
                                          A.souzoku_kyogisho.c.size_bytes, A.souzoku_kyogisho.c.created_at)
                                .where(A.souzoku_kyogisho.c.intake_id == str(intake_id)).order_by(A.souzoku_kyogisho.c.version.desc()))).fetchall()
    return [dict(r._mapping) for r in rows]


async def get_doc(intake_id: str, doc_id: str) -> dict | None:
    async with session_scope() as s:
        r = (await s.execute(sa.select(A.souzoku_kyogisho).where(A.souzoku_kyogisho.c.doc_id == str(doc_id),
                                                                  A.souzoku_kyogisho.c.intake_id == str(intake_id)))).first()
    return dict(r._mapping) if r is not None else None
