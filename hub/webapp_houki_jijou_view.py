"""webapp_houki_jijou_view — HOUKI-JIJOU-1 (a): /app/houki/jijou/{App 40 No.} 下書き表示・修正欄・「承認して生成」

正本: docs/souzoku-houki/06-documents.md §3（承認の二重化・必ず精読・修正）。QUEUE-2026-10-13-D D-5。
- GET: App 40 レコード（素材欄）→ Claude 下書き（5 節・素材だけ・30 分の in-process キャッシュ〔レコード No.+revision〕）→
  素材の一覧・5 節の修正欄・素材一致検査の結果・署名付き ticket（レコード No.・revision・操作 ID）。kintone 書込ゼロ。
- POST /generate: ticket 検証・明示確認チェック必須・revision 一致（不一致は stale）・素材一致検査（違反は生成しない・
  修正欄を保持して再表示）→ docx 生成（雛形 SHA pin・凍結検証）→ App 40 FILE 欄「事情説明書」へ upload+添付
  （$revision CAS・既に添付済みなら作用 0）。送信なし。
- FILE 欄が無い間は添付せず固定文言で案内（生成物を失わない: 添付失敗は 500 ではなく画面に固定文言）。
- RV-10: 画面は App 40 の素材のみ（LINE userId・電話・住所は出さない）。ログは No.・件数・固定語彙。
"""

import hashlib
import hmac
import html
import logging
import re
import time
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import houki_jijou as J
from hub import kintone
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webapp_auth import _gate, _sign

logger = logging.getLogger("hub.webapp_houki_jijou_view")

router = APIRouter()

PAGE = "/app/houki/jijou"
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_OP_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_REV_RE = re.compile(r"^[0-9]{1,12}$")
_TICKET_RE = re.compile(r"^[0-9]{1,12}\.[0-9]{1,12}\.[0-9a-f]{32}\.[0-9a-f]{64}$")
DRAFT_CACHE_SECONDS = 1800.0
CONFIRM_LABEL = "下書きを精読・修正し、上申書（事情説明書）として生成・添付することを承認します（裁判所への提出は別途）"
# 画面に出す素材（PII の少ない欄に限る・電話・住所・LINE userId は出さない）
SHOWN_SOURCE = ("顧客名", "被相続人氏名", "続柄", "続柄その他", "死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告",
                "日付申告メモ", "知った経緯", "財産_負債", "財産処分有無", "訴訟督促有無", "起算日_確定", "起算点メモ",
                "管轄家庭裁判所", "案件類型")
_DONE = {
    "generated": "上申書（事情説明書）を生成し、App 40 の「事情説明書」欄に添付しました。",
    "already": "この案件には既に事情説明書が添付されています（上書きしません）。",
    "confirm_required": "承認のチェックが必要です。生成していません。",
    "stale": "案件レコードが更新されました。画面を開き直してください。生成していません。",
    "not_found": "案件レコードを取得できませんでした。",
    "field_missing": "App 40 に FILE 欄「事情説明書」が無いため添付できません。kintone で欄を追加してから再度実行してください。",
    "attach_failed": "生成はできましたが App 40 への添付に失敗しました。もう一度実行してください。",
    "integrity": "雛形または生成物の検証に失敗したため添付していません。",
    "bad_input": "入力が不正です。",
}
_draft_cache: dict = {}      # record_id → (monotonic, revision, sections)
_field_cache: dict = {}      # "exists" → (monotonic, bool)
FIELD_CACHE_SECONDS = 3600.0


def _bad_request() -> Response:
    return Response(status_code=400)


def _v(rec: dict, code: str) -> str:
    return str(((rec or {}).get(code) or {}).get("value") or "").strip()


def make_ticket(record_id: str, revision: str, op_id: str) -> str | None:
    payload = f"{record_id}.{revision}.{op_id}"
    sig = _sign(payload)
    return f"{payload}.{sig}" if sig else None


def parse_ticket(ticket: str, record_id: str, op_id: str) -> str | None:
    """署名と束縛を検証し revision を返す。不正は None。"""
    if not _TICKET_RE.fullmatch(ticket or ""):
        return None
    rid, rev, oid, sig = ticket.split(".")
    expected = _sign(f"{rid}.{rev}.{oid}")
    if expected is None or not hmac.compare_digest(sig, expected):
        return None
    if rid != record_id or oid != op_id:
        return None
    return rev


async def attachment_field_exists(*, now=None) -> bool:
    t = time.monotonic() if now is None else now
    hit = _field_cache.get("exists")
    if hit and t - hit[0] < FIELD_CACHE_SECONDS:
        return hit[1]
    try:
        props = await kintone.get_form_fields(APP_HOUKI_CASE)
        ok = (props.get(J.FIELD_ATTACHMENT) or {}).get("type") == "FILE"
    except Exception:
        return False
    _field_cache["exists"] = (t, ok)
    return ok


def clear_caches() -> None:
    _draft_cache.clear()
    _field_cache.clear()


async def _record(record_id: str) -> dict | None:
    try:
        return await kintone.get_record(APP_HOUKI_CASE, record_id)
    except Exception:
        return None


async def cached_draft(record_id: str, rec: dict) -> tuple[dict, str]:
    """下書き（30 分キャッシュ・revision が変われば作り直し）。失敗は空の 5 節と理由。"""
    rev = _v(rec, "$revision")
    hit = _draft_cache.get(record_id)
    if hit and time.monotonic() - hit[0] < DRAFT_CACHE_SECONDS and hit[1] == rev:
        return dict(hit[2]), ""
    try:
        sections = await J.draft_sections(rec)
    except Exception:
        logger.info("[JIJOU] draft generation failed (fixed reason) record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return {k: "" for k in J.SECTION_KEYS}, "下書きを生成できませんでした（AI 応答不能）。空欄から記入できます。"
    _draft_cache[record_id] = (time.monotonic(), rev, dict(sections))
    return sections, ""


_STYLE = (
    "<style>body{font-family:sans-serif;max-width:760px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
    "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
    ".item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
    ".lbl{color:#667;font-size:.78rem;margin-top:.4rem}.meta{color:#8a8f98;font-size:.78rem;margin-top:.3rem}"
    "pre{white-space:pre-wrap;word-break:break-word;font-family:inherit;font-size:.9rem;background:#f2f4f7;border:1px solid #e2e6ec;"
    "border-radius:12px;padding:.5rem .8rem;margin:.3rem 0}textarea{width:100%;min-height:6rem;font-size:1rem;border-radius:10px;"
    "border:1px solid #c8cdd5;padding:.5rem;box-sizing:border-box}label{display:block;margin:.5rem 0;font-size:.92rem}"
    "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;min-height:44px}"
    ".notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}"
    ".warn{background:#fde2e2;border-radius:10px;padding:.5rem .8rem;color:#8a1f1f;margin:.4rem 0}"
    ".empty{color:#667;text-align:center;margin:1.2rem 0}</style>"
)


def _page(record_id: str, rec: dict | None, sections: dict, op_id: str, ticket: str | None, notices: list[str],
          violations: list[str], attached: bool) -> str:
    e = html.escape
    parts = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
             '<meta name="viewport" content="width=device-width, initial-scale=1">',
             "<title>上申書（事情説明書）- 案件管理（仮）</title>", _STYLE, "</head><body>",
             "<h1>上申書（事情説明書）の下書き</h1>",
             '<p class="sub">AI の下書きは素材（App 40 の記録）の整理と文章化の補助です。主張の根拠は起算点メモ（弁護士記入）から取り、'
             '必ず精読・修正のうえ承認してください。素材に無い日付・金額・固有名詞があると生成できません（素材を kintone で直してください）。'
             '<a href="/app/souzoku">相続放棄の一覧へ戻る</a></p>']
    for n in notices:
        if n:
            parts.append(f'<p class="notice">{e(n)}</p>')
    if rec is None or ticket is None:
        parts.append('<p class="empty">案件レコードを取得できませんでした</p><script src="/app/shell.js"></script></body></html>')
        return "".join(parts)
    parts.append(f'<div class="item"><div class="meta">App 40 No.{e(record_id)}｜revision {e(_v(rec, "$revision"))}'
                 f'｜status: {e(_v(rec, "status"))}</div><div class="lbl">素材（App 40）</div>')
    for code in SHOWN_SOURCE:
        val = _v(rec, code)
        if val:
            parts.append(f'<div class="lbl">{e(code)}</div><pre>{e(val)}</pre>')
    parts.append("</div>")
    if violations:
        parts.append('<div class="warn">素材一致検査の違反（生成できません）: ' + e("、".join(violations)) + "</div>")
    if attached:
        parts.append('<div class="item"><p class="empty">この案件には既に事情説明書が添付されています</p></div>')
    parts.append(f'<form method="post" action="{PAGE}/{e(record_id)}/generate" class="item">'
                 f'<input type="hidden" name="op_id" value="{e(op_id)}"><input type="hidden" name="ticket" value="{e(ticket)}">')
    for key, heading, _ph in J.SECTIONS:
        parts.append(f'<div class="lbl">{e(heading)}</div>'
                     f'<textarea name="{e(key)}" maxlength="{J.MAX_SECTION_CHARS}">{e(str(sections.get(key) or ""))}</textarea>')
    parts.append(f'<label><input type="checkbox" name="confirm" value="1"> {e(CONFIRM_LABEL)}</label>'
                 '<button type="submit">承認して生成（App 40 に添付）</button></form>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def jijou_page(request: Request):
    record_id = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(record_id):
        return _bad_request()
    code = request.query_params.get("done", "")
    notices = [_DONE.get(code, "")] if code else []
    rec = await _record(record_id)
    if rec is None:
        return HTMLResponse(_page(record_id, None, {}, "", None, notices, [], False))
    sections, why = await cached_draft(record_id, rec)
    if why:
        notices.append(why)
    op_id = uuid.uuid4().hex
    ticket = make_ticket(record_id, _v(rec, "$revision"), op_id)
    attached = bool((rec.get(J.FIELD_ATTACHMENT) or {}).get("value"))
    return HTMLResponse(_page(record_id, rec, sections, op_id, ticket, notices,
                              J.check_against_source(sections, rec), attached))


def _redirect(record_id: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{record_id}?done={done}", status_code=303)


async def jijou_generate(request: Request):
    record_id = str(request.path_params.get("record_id", ""))
    form = await request.form()
    op_id = str(form.get("op_id", ""))
    ticket = str(form.get("ticket", ""))
    confirm = str(form.get("confirm", "")) == "1"
    sections = {k: str(form.get(k, "")).strip()[:J.MAX_SECTION_CHARS] for k in J.SECTION_KEYS}
    if not _ID_RE.fullmatch(record_id) or not _OP_ID_RE.fullmatch(op_id):
        return _bad_request()
    rev = parse_ticket(ticket, record_id, op_id)
    if rev is None:
        return _bad_request()
    if not confirm:
        return _redirect(record_id, "confirm_required")
    rec = await _record(record_id)
    if rec is None:
        return _redirect(record_id, "not_found")
    if _v(rec, "$revision") != rev:
        return _redirect(record_id, "stale")
    if (rec.get(J.FIELD_ATTACHMENT) or {}).get("value"):
        return _redirect(record_id, "already")
    violations = J.check_against_source(sections, rec)
    if violations:
        logger.info("[JIJOU] guard violations count=%s record_id=%s", emit(len(violations), "count", "log", "operator"),
                    emit(record_id, "record_id", "log", "operator"))
        new_ticket = make_ticket(record_id, rev, op_id)
        return HTMLResponse(_page(record_id, rec, sections, op_id, new_ticket, [], violations, False))
    if not await attachment_field_exists():
        return _redirect(record_id, "field_missing")
    try:
        docx_bytes = J.generate(rec, sections)
    except J.JijouIntegrityError:
        logger.error("[JIJOU] integrity error (template/body mismatch)")
        return _redirect(record_id, "integrity")
    except J.JijouGuardError:
        return _redirect(record_id, "bad_input")
    mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    try:
        file_key = await kintone.upload_file(APP_HOUKI_CASE, f"上申書_事情説明書_{record_id}.docx", docx_bytes, mime)
        await kintone.update_record(APP_HOUKI_CASE, record_id, {J.FIELD_ATTACHMENT: [{"fileKey": file_key}]},
                                    revision=rev)
    except kintone.KintoneConflict:
        return _redirect(record_id, "stale")
    except Exception:
        logger.error("[JIJOU] attach failed record_id=%s", emit(record_id, "record_id", "log", "operator"))
        return _redirect(record_id, "attach_failed")
    _draft_cache.pop(record_id, None)
    logger.info("[JIJOU] generated and attached record_id=%s bytes=%s", emit(record_id, "record_id", "log", "operator"),
                emit(len(docx_bytes), "count", "log", "operator"))
    return _redirect(record_id, "generated")


router.add_api_route(f"{PAGE}/{{record_id}}", _gate(jijou_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{record_id}}/generate", _gate(jijou_generate), methods=["POST"])
