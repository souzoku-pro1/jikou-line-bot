"""webapp_jikou_result_view — JIKOU-FOLLOWUP-1 (2): 債権者回答の結果記録画面 /app/jikou/result/{案件ID}

QUEUE-2026-10-16-R F-1。認証は _gate（PWA の人＝大野のみ・human ロール）。/app/pending・/app/drafts と同型
（サーバ描画・native form POST・PRG 303・入力は閉集合＋固定書式・不正は固定 400）。

- GET: App 21 レコード（債権者の枠 1〜3 の社名・発送日欄があればその値）と本表（hub/jikou_followup）の記録を並べる。
  GET は DB・kintone とも **書込ゼロ**。App 21 を読めないときは本表の記録だけで表示する（固定文言）。
- POST /save: 明示確認（confirm=1）必須。枠ごとに 結果（成立／不成立／一部成立／回答なし）・回答日・回答要旨を記録
  （結果が空の枠は変更しない）。発送日は App 21 に欄が無い（または空の）ときだけ人が入力できる。
  kintone への写しは欄がそろっているときだけ（hub/jikou_followup.copy_result_to_kintone・無ければ skipped）。
- 画面に出すのは 案件 No.・社名・日付・結果・要旨（人が書いた自由記述・_gate の内側のみ）。顧客名・住所・電話・LINE userId は
  App 21 から読まず、画面にも API にも出さない（RV-10）。相談者への送信経路は持たない。
"""

import html
import logging
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import jikou_followup as F
from hub import kintone
from hub.redact import emit
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_jikou_result_view")

router = APIRouter()

PAGE = "/app/jikou/result"
API = "/app/api/jikou/result"
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_NOTICES = {
    "saved": "記録しました。",
    "saved_copied": "記録しました（kintone にも写しを書きました）。",
    "saved_copy_skipped": "記録しました（kintone に対応欄が無いため写しは書いていません）。",
    "saved_copy_failed": "記録しました（kintone への写しは失敗しました。次回の保存で再試行されます）。",
    "nothing": "変更はありませんでした（結果が選ばれていません）。",
    "confirm_required": "「内容を確認しました」にチェックしてから保存してください。記録していません。",
    "bad_input": "入力が不正です（成立・不成立・一部成立は回答日が必要です）。記録していません。",
    "not_found": "案件の債権者を特定できませんでした。記録していません。",
    "db": "記録を読み書きできませんでした。",
    "kintone": "案件情報（App 21）を読めませんでした。本表の記録のみ表示しています。",
}
_STYLE = (
    "<style>body{font-family:sans-serif;max-width:760px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
    "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}h2{font-size:1.02rem;margin:1rem 0 .3rem}"
    ".sub{color:#667;font-size:.85rem;margin:0 0 .8rem}.item{background:#fff;border:1px solid #d8dde5;border-radius:14px;"
    "padding:.75rem 1rem;margin:.6rem 0}.lbl{color:#667;font-size:.78rem;margin-top:.4rem}.meta{color:#8a8f98;font-size:.78rem}"
    "label{display:block;margin:.4rem 0;font-size:.92rem}select,input[type=date]{font-size:1rem;padding:.35rem;border-radius:8px;"
    "border:1px solid #c8cdd5}textarea{width:100%;min-height:5rem;font-size:1rem;border-radius:10px;border:1px solid #c8cdd5;"
    "padding:.5rem;box-sizing:border-box}button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;"
    "background:#fff;min-height:44px}.notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}"
    ".badge{display:inline-block;border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#fff3d6;color:#8a6d00;"
    "margin-right:.35rem}.ok{background:#e2f5e6;color:#1f6b2e}.ng{background:#fde2e2;color:#8a1f1f}</style>"
)


def _bad_request() -> Response:
    return Response(status_code=400)


def _d(v) -> str:
    return v.isoformat() if v else ""


async def _case_record(record_id: str) -> dict | None:
    try:
        return await kintone.get_record(F.APP21, record_id)
    except Exception:
        return None


def _merge(record: dict | None, rows: list[dict]) -> list[dict]:
    """枠 1〜3 の表示行: 社名は App 21 を優先・無ければ本表。どちらにも無い枠は出さない。"""
    by_slot = {int(r["slot"]): r for r in rows}
    names = dict(F.creditors_of(record)) if record else {}
    out = []
    for slot in F.SLOTS:
        name = names.get(slot) or (by_slot.get(slot) or {}).get("creditor_name")
        if not name:
            continue
        r = by_slot.get(slot) or {}
        out.append({"slot": slot, "creditor_name": name, "result": r.get("result"), "answered_on": r.get("answered_on"),
                    "summary": r.get("summary"), "recorded_at": r.get("recorded_at")})
    return out


def _page(record_id: str, items: list[dict], shipped: dict, notice: str) -> str:
    e = html.escape
    parts = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
             '<meta name="viewport" content="width=device-width, initial-scale=1">',
             f"<title>案件結果 No.{e(record_id)} - 案件管理（仮）</title>", _STYLE, "</head><body>",
             f"<h1>時効援用 案件結果 No.{e(record_id)}</h1>",
             '<p class="sub">債権者ごとの回答結果を記録します（人の入力のみ・相談者への送信はありません）。'
             f'<a href="/app/jikou/cert/{e(record_id)}">完了報告書</a>｜'      # JIKOU-CERT-1
             f'<a href="/app/jikou/receipt/{e(record_id)}">領収書</a>｜'        # JIKOU-RECEIPT-1
             '<a href="/app/reply/metrics">返信計測</a>｜<a href="/app/pending">対応待ち</a></p>']
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    if not items:
        parts.append('<p class="notice">この案件の債権者（問い合わせ業者名／対象債権者2／対象債権者3）が見つかりません。</p>')
        parts.append('<script src="/app/shell.js"></script></body></html>')
        return "".join(parts)
    parts.append(f'<form method="post" action="{PAGE}/{e(record_id)}/save">')
    parts.append('<div class="item"><h2>発送日</h2>')
    if shipped.get("source") == "kintone":
        parts.append(f'<div>{e(_d(shipped.get("date")))}（App 21 の「{e(F.FIELD_SHIPPED)}」）</div>')
    else:
        parts.append(f'<label>発送日 <input type="date" name="shipped_on" value="{e(_d(shipped.get("date")))}"></label>'
                     f'<div class="meta">App 21 に「{e(F.FIELD_SHIPPED)}」欄が無い（または空の）ため、ここで記録します。'
                     "発送日を基準に 14 日・28 日で業務 LINE に未記録の案件を通知します。</div>")
    parts.append("</div>")
    for it in items:
        slot = int(it["slot"])
        badge = (f'<span class="badge ok">{e(F.RESULT_LABELS.get(it["result"], ""))}</span>' if it.get("result")
                 else '<span class="badge ng">未記録</span>')
        parts.append(f'<div class="item"><h2>枠 {slot}: {e(it["creditor_name"])}</h2>{badge}')
        if it.get("answered_on"):
            parts.append(f'<div class="meta">回答日 {e(_d(it["answered_on"]))}</div>')
        if it.get("summary"):
            parts.append(f'<div class="lbl">記録済みの回答要旨</div><div>{e(str(it["summary"]))}</div>')
        parts.append(f'<label>結果 <select name="result_{slot}"><option value="">（変更しない）</option>')
        for code in F.RESULTS:
            parts.append(f'<option value="{e(code)}">{e(F.RESULT_LABELS[code])}</option>')
        parts.append("</select></label>")
        parts.append(f'<label>回答日 <input type="date" name="answered_on_{slot}" value="{e(_d(it.get("answered_on")))}"></label>')
        parts.append(f'<label>回答要旨（自由記述・{F.MAX_SUMMARY_CHARS} 字まで）'
                     f'<textarea name="summary_{slot}"></textarea></label></div>')
    parts.append('<label><input type="checkbox" name="confirm" value="1"> 内容を確認しました</label>'
                 '<button type="submit">記録する</button></form>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def _shipped_info(record: dict | None, rows: list[dict]) -> dict:
    """発送日の表示元: App 21 の欄（あれば・値があれば）> 本表。"""
    if record is not None and await F.shipped_field_available():
        d = F.parse_date(F._fv(record, F.FIELD_SHIPPED))
        if d is not None:
            return {"source": "kintone", "date": d}
    dates = [r["shipped_on"] for r in rows if r.get("shipped_on")]
    return {"source": "human", "date": (min(dates) if dates else None)}


async def result_page(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    code = request.query_params.get("done", "")
    notice = _NOTICES.get(code, "") if code else ""
    record = await _case_record(rid)
    try:
        rows = await F.rows_for_case(rid)
    except Exception:
        rows = []
        notice = _NOTICES["db"]
    if record is None and not notice:
        notice = _NOTICES["kintone"]
    items = _merge(record, rows)
    shipped = await _shipped_info(record, rows)
    return HTMLResponse(_page(rid, items, shipped, notice))


def _redirect(rid: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{rid}?done={done}", status_code=303)


async def result_save(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    form = await request.form()
    if str(form.get("confirm", "")) != "1":
        return _redirect(rid, "confirm_required")
    entries: dict = {}
    for slot in F.SLOTS:
        result = str(form.get(f"result_{slot}", "")).strip()
        if not result:
            continue
        ok, entry = F.validate_entry(result, str(form.get(f"answered_on_{slot}", "")), form.get(f"summary_{slot}", ""))
        if not ok:
            return _redirect(rid, "bad_input")
        entries[slot] = entry
    shipped_text = str(form.get("shipped_on", "")).strip()
    shipped = F.parse_date(shipped_text) if shipped_text else None
    if shipped_text and shipped is None:
        return _redirect(rid, "bad_input")
    record = await _case_record(rid)
    try:
        rows = await F.rows_for_case(rid)
        creditors = F.creditors_of(record) if record else [(int(r["slot"]), r["creditor_name"]) for r in rows
                                                            if r.get("creditor_name")]
        if not creditors:
            return _redirect(rid, "not_found")
        known = {s for s, _ in creditors}
        if any(s not in known for s in entries):
            return _redirect(rid, "bad_input")
        await F.ensure_rows(rid, creditors)
        shipped_from_kintone = record is not None and await F.shipped_field_available() and \
            F.parse_date(F._fv(record, F.FIELD_SHIPPED)) is not None
        if shipped is not None and not shipped_from_kintone:
            await F.set_shipped(rid, shipped, source="human", actor=F.ACTOR_HUMAN)
        for slot, entry in entries.items():
            await F.record_result(rid, slot, result=entry["result"], answered_on=entry["answered_on"],
                                  summary=entry["summary"], actor=F.ACTOR_HUMAN)
    except Exception:
        logger.warning("[JIKOU_RESULT] save failed (fixed reason)")
        return _redirect(rid, "db")
    if not entries:
        return _redirect(rid, "saved" if shipped is not None else "nothing")
    await F.run_result_hooks(rid, record)          # JIKOU-PROGRESS-1: 債権者回答到着の下書き積み（送信はしない）
    copies = [await F.copy_result_to_kintone(rid, slot, entry) for slot, entry in entries.items()]
    logger.info("[JIKOU_RESULT] recorded case=%s slots=%s", emit(rid, "record_id", "log", "operator"),
                emit(len(entries), "count", "log", "operator"))
    if all(c == "written" for c in copies):
        return _redirect(rid, "saved_copied")
    if all(c == "skipped" for c in copies):
        return _redirect(rid, "saved_copy_skipped")
    return _redirect(rid, "saved_copy_failed")


async def api_result(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    try:
        rows = await F.rows_for_case(rid)
    except Exception:
        return {"ok": False, "reason": "db"}
    return {"ok": True, "record_id": rid,
            "rows": [{"slot": int(r["slot"]), "creditor": r.get("creditor_name"), "shipped_on": _d(r.get("shipped_on")),
                      "result": r.get("result"), "answered_on": _d(r.get("answered_on")),
                      "has_summary": bool(r.get("summary"))} for r in rows]}


router.add_api_route(PAGE + "/{record_id}", _gate(result_page), methods=["GET"])
router.add_api_route(PAGE + "/{record_id}/save", _gate(result_save), methods=["POST"])
router.add_api_route(API + "/{record_id}", _gate(api_result), methods=["GET"])


def wire_all(app) -> None:
    """main.py 末尾の 1 行: 画面の結線＋日次フォロージョブの startup 登録（Q6 と同方式）。"""
    from hub.webapp_brain_view import include_before_catch_all
    include_before_catch_all(app, router)
    app.router.on_startup.append(F.register_followup_job)
