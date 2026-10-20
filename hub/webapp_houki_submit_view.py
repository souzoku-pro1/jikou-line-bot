"""webapp_houki_submit_view — HOUKI-TEISHUTSU-1 (1)(2)(4): /app/houki/submit/{App 40 No.} 提出前チェック・送付状・提出日の記録

QUEUE-2026-10-16-R F-3。認証は _gate（PWA の人＝大野のみ）。HOUKI-JIJOU-1 の画面と同型（署名付き ticket〔No.・revision・操作 ID〕・
明示確認・PRG 303・入力は閉集合＋固定書式・不正は固定 400）。

- GET: App 40 レコード → 提出前チェック（hub/houki_teishutsu.checklist）・収入印紙／予納郵券（hub/houki_fees・要大野確認の印）・
  同封書類の見込み一覧。**不足があれば「送付状を生成」「提出日を記録」のフォームを出さない**（表示のみ・提出は人が行う）。
  GET は kintone 書込ゼロ・upload なし。
- POST /generate: ticket・明示確認・revision 一致・充足の再検査 → 送付状 docx を生成し、App 40 に FILE 欄「送付状」があれば
  upload＋添付（$revision CAS）、無ければ docx をそのままダウンロードで返す（生成物を失わない）。
- POST /submitted: ticket・明示確認・revision 一致・日付（YYYY-MM-DD）→ App 40「申述提出日」に書く（$revision CAS・人の操作）。
  以後、既存 HOUKI-JUKURYO の対象（申述提出日が空）から外れ 14／7 日前通知は止まる。
- RV-10: 画面は 案件 No.・管轄家裁・事件名（被相続人氏名）・申述人（顧客名）・チェック結果・金額のみ。住所・電話・LINE userId は出さない。
  送信なし。書く欄は 申述提出日 と 送付状 だけ。
"""

import hmac
import html
import logging
import re
import time
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import houki_fees as FEES
from hub import houki_teishutsu as T
from hub import kintone
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit
from hub.webapp_auth import _gate, _sign

logger = logging.getLogger("hub.webapp_houki_submit_view")

router = APIRouter()

PAGE = "/app/houki/submit"
API = "/app/api/houki/submit"
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_OP_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_TICKET_RE = re.compile(r"^[0-9]{1,12}\.[0-9]{1,12}\.[0-9a-f]{32}\.[0-9a-f]{64}$")
FIELD_CACHE_SECONDS = 3600.0
CONFIRM_GENERATE = "提出前チェックの内容を確認し、送付状を生成することを承認します（裁判所への提出は人が行います）"
CONFIRM_SUBMITTED = "家庭裁判所へ提出したことを確認し、提出日を記録します（熟慮期間の通知は止まります）"
_DONE = {
    "generated": "送付状を生成し、App 40 の「送付状」欄に添付しました。",
    "submitted": "提出日を記録しました（熟慮期間の 14／7 日前通知は以後送られません）。",
    "already_submitted": "この案件は既に提出日が記録されています。",
    "not_ready": "提出前チェックに不足があるため実行していません。",
    "confirm_required": "確認のチェックが必要です。実行していません。",
    "stale": "案件レコードが更新されました。画面を開き直してください。実行していません。",
    "not_found": "案件レコードを取得できませんでした。",
    "integrity": "雛形または生成物の検証に失敗したため生成していません。",
    "rejected": "送付状に必要な項目（管轄家庭裁判所・顧客名・被相続人氏名）が不足しています。",
    "attach_failed": "生成はできましたが App 40 への添付に失敗しました。もう一度実行してください。",
    "bad_input": "入力が不正です（提出日は YYYY-MM-DD）。",
    "kintone": "案件レコード（App 40）を読めませんでした。",
}
_field_cache: dict = {}
_STYLE = (
    "<style>body{font-family:sans-serif;max-width:760px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
    "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}h2{font-size:1.02rem;margin:1rem 0 .3rem}"
    ".sub{color:#667;font-size:.85rem;margin:0 0 .8rem}table{width:100%;border-collapse:collapse;background:#fff;border-radius:14px;"
    "overflow:hidden}th,td{padding:.5rem .7rem;border-bottom:1px solid #e2e6ec;text-align:left;font-size:.93rem}th{background:#f2f4f7;"
    "color:#556}.ok{color:#1f6b2e;font-weight:bold}.ng{color:#8a1f1f;font-weight:bold}.item{background:#fff;border:1px solid #d8dde5;"
    "border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}label{display:block;margin:.4rem 0;font-size:.92rem}"
    "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;min-height:44px}"
    ".notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}.warn{background:#fff3d6;"
    "color:#8a6d00;border-radius:10px;padding:.5rem .8rem;margin:.4rem 0}.meta{color:#8a8f98;font-size:.78rem}</style>"
)


def _bad_request() -> Response:
    return Response(status_code=400)


def _v(rec: dict, code: str) -> str:
    return str(((rec or {}).get(code) or {}).get("value") or "").strip()


def make_ticket(record_id: str, revision: str, op_id: str) -> str | None:
    payload = f"{record_id}.{revision}.{op_id}"
    sig = _sign(payload)
    return f"{payload}.{sig}" if sig else None


def parse_ticket(ticket: str, record_id: str, op_id: str) -> str | None:
    """署名と束縛（record_id・op_id）を検証し revision を返す。不正は None。"""
    if not _TICKET_RE.fullmatch(ticket or ""):
        return None
    rid, rev, oid, sig = ticket.split(".")
    expected = _sign(f"{rid}.{rev}.{oid}")
    if expected is None or not hmac.compare_digest(sig, expected):
        return None
    if rid != record_id or oid != op_id:
        return None
    return rev


async def soufujo_field_exists(*, now=None) -> bool:
    t = time.monotonic() if now is None else now
    hit = _field_cache.get("exists")
    if hit and t - hit[0] < FIELD_CACHE_SECONDS:
        return hit[1]
    try:
        props = await kintone.get_form_fields(APP_HOUKI_CASE)
        ok = (props.get(T.FIELD_SOUFUJO) or {}).get("type") == "FILE"
    except Exception:
        return False
    _field_cache["exists"] = (t, ok)
    return ok


def clear_caches() -> None:
    _field_cache.clear()


async def _record(record_id: str) -> dict | None:
    try:
        return await kintone.get_record(APP_HOUKI_CASE, record_id)
    except Exception:
        return None


def _page(record_id: str, rec: dict | None, items: list, ready: bool, submitted: str, op_id: str, ticket: str | None,
          notice: str, today) -> str:
    e = html.escape
    parts = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
             '<meta name="viewport" content="width=device-width, initial-scale=1">',
             f"<title>提出前チェック No.{e(record_id)} - 案件管理（仮）</title>", _STYLE, "</head><body>",
             f"<h1>相続放棄 家裁提出パッケージ No.{e(record_id)}</h1>",
             '<p class="sub">提出前チェック・送付状・提出日の記録。提出そのものは人が行います。'
             '<a href="/app/houki/overdue">期限超過</a>｜<a href="/app/souzoku">相続放棄の一覧</a></p>']
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    if rec is None:
        parts.append('<script src="/app/shell.js"></script></body></html>')
        return "".join(parts)
    court = _v(rec, T.FIELD_COURT)
    parts.append('<div class="item">'
                 f"<div>事件名: {e(T.D.case_name(rec))}</div><div>申述人: {e(_v(rec, T.FIELD_CUSTOMER))}</div>"
                 f"<div>管轄家庭裁判所: {e(court or '（未入力）')}</div>"
                 f'<div class="meta">{e(str(today))} 時点・revision {e(_v(rec, "$revision"))}</div></div>')
    parts.append("<h2>提出前チェック</h2><table><tr><th>項目</th><th>状態</th><th>詳細</th></tr>")
    for it in items:
        cls = "ok" if it.ok else "ng"
        parts.append(f'<tr><td>{e(it.label)}</td><td class="{cls}">{"充足" if it.ok else "不足"}</td><td>{e(it.detail)}</td></tr>')
    parts.append("</table>")
    p = FEES.postage_for(court)
    parts.append("<h2>収入印紙・予納郵券</h2>"
                 f'<div class="item"><div>収入印紙: {e(FEES.fmt_yen(FEES.stamp_yen(1)))}（申述人 1 人）'
                 f'<span class="meta">　{e(FEES.STAMP_STATUS)}</span></div>'
                 f'<div>予納郵券（{e(p.court)}）: {e(FEES.fmt_yen(p.yen))}<span class="meta">　{e(p.breakdown)}・{e(p.status)}</span></div></div>')
    parts.append("<h2>同封書類（見込み）</h2><div class=\"item\">" +
                 "".join(f"<div>{e(line)}</div>" for line in T.enclosure_text(T.enclosures(rec)).split("\n")) + "</div>")
    if submitted:
        parts.append(f'<p class="notice">提出済み（申述提出日 {e(submitted)}）。熟慮期間の通知は送られません。</p>')
    elif not ready:
        parts.append('<p class="warn">不足があるため、送付状の生成と提出日の記録はできません（提出ボタンは表示されません）。</p>')
    elif ticket:
        parts.append(f'<form method="post" action="{PAGE}/{e(record_id)}/generate" class="item">'
                     f'<input type="hidden" name="op_id" value="{e(op_id)}"><input type="hidden" name="ticket" value="{e(ticket)}">'
                     f'<label><input type="checkbox" name="confirm" value="1"> {e(CONFIRM_GENERATE)}</label>'
                     '<button type="submit">送付状を生成</button></form>')
        parts.append(f'<form method="post" action="{PAGE}/{e(record_id)}/submitted" class="item">'
                     f'<input type="hidden" name="op_id" value="{e(op_id)}"><input type="hidden" name="ticket" value="{e(ticket)}">'
                     f'<label>提出日 <input type="date" name="submitted_on" value="{e(str(today))}"></label>'
                     f'<label><input type="checkbox" name="confirm" value="1"> {e(CONFIRM_SUBMITTED)}</label>'
                     '<button type="submit">提出日を記録</button></form>')
    else:
        parts.append('<p class="warn">署名鍵（WEBAPP_SESSION_SECRET）が未設定のため操作できません。</p>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def submit_page(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    code = request.query_params.get("done", "")
    notice = _DONE.get(code, "") if code else ""
    rec = await _record(rid)
    today = T.today_jst()
    if rec is None:
        return HTMLResponse(_page(rid, None, [], False, "", "", None, notice or _DONE["kintone"], today))
    items = T.checklist(rec, today)
    op_id = uuid.uuid4().hex
    ticket = make_ticket(rid, _v(rec, "$revision"), op_id)
    return HTMLResponse(_page(rid, rec, items, T.is_ready(items), T.submitted_on(rec), op_id, ticket, notice, today))


def _redirect(rid: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{rid}?done={done}", status_code=303)


async def _checked_record(request: Request, rid: str):
    """POST 共通: ticket・明示確認・revision 一致・充足・未提出。戻り値 (rec, rev, form) か (None, redirect, None)。"""
    form = await request.form()
    op_id = str(form.get("op_id", ""))
    ticket = str(form.get("ticket", ""))
    if not _OP_ID_RE.fullmatch(op_id):
        return None, _bad_request(), None
    rev = parse_ticket(ticket, rid, op_id)
    if rev is None:
        return None, _bad_request(), None
    if str(form.get("confirm", "")) != "1":
        return None, _redirect(rid, "confirm_required"), None
    rec = await _record(rid)
    if rec is None:
        return None, _redirect(rid, "not_found"), None
    if _v(rec, "$revision") != rev:
        return None, _redirect(rid, "stale"), None
    if T.submitted_on(rec):
        return None, _redirect(rid, "already_submitted"), None
    if not T.is_ready(T.checklist(rec, T.today_jst())):
        return None, _redirect(rid, "not_ready"), None
    return rec, rev, form


async def submit_generate(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    rec, rev, _form = await _checked_record(request, rid)
    if rec is None:
        return rev
    try:
        docx_bytes = T.generate(rec)
    except T.TeishutsuIntegrityError:
        logger.error("[TEISHUTSU] integrity error (template/body mismatch)")
        return _redirect(rid, "integrity")
    except T.TeishutsuRejection:
        return _redirect(rid, "rejected")
    mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    if not await soufujo_field_exists():
        logger.info("[TEISHUTSU] soufujo downloaded (no FILE field) record_id=%s", emit(rid, "record_id", "log", "operator"))
        return Response(content=docx_bytes, media_type=mime,
                        headers={"Content-Disposition": f'attachment; filename="soufujo_{rid}.docx"'})
    try:
        file_key = await kintone.upload_file(APP_HOUKI_CASE, f"送付状_{rid}.docx", docx_bytes, mime)
        await kintone.update_record(APP_HOUKI_CASE, rid, {T.FIELD_SOUFUJO: [{"fileKey": file_key}]}, revision=rev)
    except kintone.KintoneConflict:
        return _redirect(rid, "stale")
    except Exception:
        logger.error("[TEISHUTSU] attach failed record_id=%s", emit(rid, "record_id", "log", "operator"))
        return _redirect(rid, "attach_failed")
    logger.info("[TEISHUTSU] soufujo attached record_id=%s", emit(rid, "record_id", "log", "operator"))
    return _redirect(rid, "generated")


async def submit_submitted(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    rec, rev, form = await _checked_record(request, rid)
    if rec is None:
        return rev
    d = T.parse_date(form.get("submitted_on", ""))
    if d is None or d > T.today_jst():
        return _redirect(rid, "bad_input")
    try:
        await kintone.update_record(APP_HOUKI_CASE, rid, {T.FIELD_SUBMITTED: d.isoformat()}, revision=rev)
    except kintone.KintoneConflict:
        return _redirect(rid, "stale")
    except Exception:
        logger.error("[TEISHUTSU] submitted write failed record_id=%s", emit(rid, "record_id", "log", "operator"))
        return _redirect(rid, "attach_failed")
    logger.info("[TEISHUTSU] submitted recorded record_id=%s", emit(rid, "record_id", "log", "operator"))
    return _redirect(rid, "submitted")


async def api_submit(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    rec = await _record(rid)
    if rec is None:
        return {"ok": False, "reason": "kintone"}
    today = T.today_jst()
    items = T.checklist(rec, today)
    p = FEES.postage_for(_v(rec, T.FIELD_COURT))
    return {"ok": True, "record_id": rid, "ready": T.is_ready(items), "submitted_on": T.submitted_on(rec),
            "items": [{"key": i.key, "label": i.label, "ok": i.ok, "detail": i.detail} for i in items],
            "fees": {"stamp_yen": FEES.stamp_yen(1), "postage_yen": p.yen, "postage_status": p.status},
            "enclosures": T.enclosures(rec)}


router.add_api_route(f"{PAGE}/{{record_id}}", _gate(submit_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{record_id}}/generate", _gate(submit_generate), methods=["POST"])
router.add_api_route(f"{PAGE}/{{record_id}}/submitted", _gate(submit_submitted), methods=["POST"])
router.add_api_route(f"{API}/{{record_id}}", _gate(api_submit), methods=["GET"])
