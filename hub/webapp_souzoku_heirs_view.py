"""webapp_souzoku_heirs_view — 相続人の確定（第 2 段階）画面 /app/souzoku/{案件ID}/heirs（SOUZOKU-3・QUEUE-2026-10-17-H H-4）

正本: docs/souzoku-1-design.md §2。
- 認証は P4-001 の関所（hub.webapp_auth._gate）。flag SOUZOKU_INTAKE_ENABLED が OFF なら 404（受付フォームと同じ）。
- 案件 ID は souzoku_intake.intake_id（32 hex）。存在しなければ 404（固定応答・反射しない）。
- 入力は **人数と状態**（続柄ごとの行・氏名を持たない＝RV-10）。保存のたびに新しい版の draft（上書きしない＝RV-08）。
- 判定結果は hub/souzoku_heirs.determine（判定表 hub/souzoku_law_constants・要大野確認）。画面には「弁護士確認前」の印を常に出す。
- 承認（status=confirmed）は人の明示操作（確認チェック＋POST）のみ。kintone・LINE には書かない・送らない。
- URL・ログ・通知に相続人の氏名は載らない（件数・版・結果の閉集合のみ）。
"""
import html
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import souzoku_heirs as H
from hub import souzoku_intake as S
from hub import souzoku_law_constants as L
from hub.redact import emit
from hub.webapp_auth import _gate

import logging

logger = logging.getLogger("hub.webapp_souzoku_heirs_view")
router = APIRouter()
PAGE = "/app/souzoku"
_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_STATUS_LABELS = {L.STATUS_NONE: "いない", L.STATUS_ALIVE: "生存", L.STATUS_DECEASED: "死亡（代襲）",
                  L.STATUS_RENOUNCED: "相続放棄", L.STATUS_DISQUALIFIED: "欠格", L.STATUS_DISINHERITED: "廃除"}
_SPOUSE_LABELS = {L.STATUS_ALIVE: "いる", L.STATUS_NONE: "いない", L.STATUS_DECEASED: "先に死亡", L.STATUS_RENOUNCED: "相続放棄"}
_KIND_LABELS = {L.KIND_REAL: "実子", L.KIND_ADOPTED: "養子"}
_BLOOD_LABELS = {L.BLOOD_FULL: "全血", L.BLOOD_HALF: "半血"}
_WILL_LABELS = {L.WILL_NONE: "なし", L.WILL_NOTARIAL: "公正証書遺言", L.WILL_HOLOGRAPH: "自筆証書遺言", L.WILL_UNKNOWN: "不明"}
_RESULT_LABELS = {H.RESULT_DETERMINED: "判定（法定相続分）", H.RESULT_NEEDS_ATTORNEY: "要弁護士判断", H.RESULT_NO_HEIRS: "法定相続人なし（要弁護士判断）"}
_NOTICES = {
    "saved": "聞取りを保存し、法定相続人を判定しました（弁護士確認前）。",
    "confirmed": "この版を確定しました（status=confirmed）。",
    "already": "この版は既に確定済みです。",
    "confirm_required": "確認チェックが必要です。確定していません。",
    "bad_input": "入力が不正です。保存していません。",
    "db": "記録を読み書きできませんでした。",
}


def _not_found() -> Response:
    return Response(status_code=404)


def _bad_request() -> Response:
    return Response(status_code=400)


def _redirect(intake_id: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{intake_id}/heirs?done={done}", status_code=303)


async def _intake_exists(intake_id: str) -> bool:
    import sqlalchemy as sa
    from hub.db import session_scope
    async with session_scope() as s:
        return (await s.execute(sa.select(S.souzoku_intake.c.intake_id).where(
            S.souzoku_intake.c.intake_id == intake_id))).first() is not None


# ── form ⇄ 構造 ──────────────────────────────────────────────────────────────
def parse_form(form) -> dict:
    """画面の入力（人数と状態）を souzoku_heirs.normalize_input の形へ。氏名欄は無い。"""
    def g(k, d=""):
        v = form.get(k)
        return str(v) if v is not None else d
    children = [{"kind": g(f"child_{i}_kind", L.KIND_REAL), "status": g(f"child_{i}_status", L.STATUS_NONE),
                 "descendants": g(f"child_{i}_descendants", "0"), "descendants_deceased": g(f"child_{i}_descendants_deceased", "0")}
                for i in range(1, L.MAX_CHILD_ROWS + 1)]
    siblings = [{"blood": g(f"sib_{i}_blood", L.BLOOD_FULL), "status": g(f"sib_{i}_status", L.STATUS_NONE),
                 "children": g(f"sib_{i}_children", "0"), "children_deceased": g(f"sib_{i}_children_deceased", "0")}
                for i in range(1, L.MAX_SIBLING_ROWS + 1)]
    return {"spouse": g("spouse", L.STATUS_NONE), "children": children,
            "ascendants": {"father": g("father", L.STATUS_NONE), "mother": g("mother", L.STATUS_NONE),
                           "grandparents_alive": g("grandparents_alive", "0")},
            "siblings": siblings, "will": g("will", L.WILL_NONE),
            "special": {k: bool(g(k)) for k in ("successive", "fetus", "designated_shares", "renunciation_pending")}}


def _sel(name: str, options: dict, current: str) -> str:
    e = html.escape
    return (f'<select name="{e(name)}">' + "".join(
        f'<option value="{e(k)}"{" selected" if k == current else ""}>{e(v)}</option>' for k, v in options.items()) + "</select>")


def _num(name: str, current, hi: int) -> str:
    return f'<input type="number" name="{html.escape(name)}" min="0" max="{hi}" value="{int(current or 0)}" inputmode="numeric">'


def _form_html(intake_id: str, inp: dict | None) -> str:
    e = html.escape
    inp = inp or {"spouse": L.STATUS_NONE, "children": [], "ascendants": {}, "siblings": [], "will": L.WILL_NONE, "special": {}}
    p = [f'<form method="post" action="{PAGE}/{e(intake_id)}/heirs">']
    p.append("<fieldset><legend>配偶者</legend>" + _sel("spouse", _SPOUSE_LABELS, inp.get("spouse", L.STATUS_NONE)) + "</fieldset>")
    p.append("<fieldset><legend>子（実子・養子・先に亡くなった子は代襲者＝孫の人数）</legend>")
    rows = list(inp.get("children") or [])
    for i in range(1, L.MAX_CHILD_ROWS + 1):
        c = rows[i - 1] if i - 1 < len(rows) else {}
        p.append(f'<div class="row">子{i}: ' + _sel(f"child_{i}_kind", _KIND_LABELS, c.get("kind", L.KIND_REAL))
                 + _sel(f"child_{i}_status", _STATUS_LABELS, c.get("status", L.STATUS_NONE))
                 + " 代襲者（孫）" + _num(f"child_{i}_descendants", c.get("descendants", 0), L.MAX_DESCENDANTS_PER_ROW)
                 + " うち死亡 " + _num(f"child_{i}_descendants_deceased", c.get("descendants_deceased", 0), L.MAX_DESCENDANTS_PER_ROW) + "</div>")
    p.append("</fieldset>")
    asc = inp.get("ascendants") or {}
    p.append("<fieldset><legend>直系尊属（子がいないとき）</legend>父 " + _sel("father", _STATUS_LABELS, asc.get("father", L.STATUS_NONE))
             + " 母 " + _sel("mother", _STATUS_LABELS, asc.get("mother", L.STATUS_NONE))
             + " 祖父母の生存人数 " + _num("grandparents_alive", asc.get("grandparents_alive", 0), 4) + "</fieldset>")
    p.append("<fieldset><legend>兄弟姉妹（子も直系尊属もいないとき・先に亡くなった人は代襲者＝甥姪の人数）</legend>")
    rows = list(inp.get("siblings") or [])
    for i in range(1, L.MAX_SIBLING_ROWS + 1):
        s = rows[i - 1] if i - 1 < len(rows) else {}
        p.append(f'<div class="row">兄弟姉妹{i}: ' + _sel(f"sib_{i}_blood", _BLOOD_LABELS, s.get("blood", L.BLOOD_FULL))
                 + _sel(f"sib_{i}_status", _STATUS_LABELS, s.get("status", L.STATUS_NONE))
                 + " 代襲者（甥姪）" + _num(f"sib_{i}_children", s.get("children", 0), L.MAX_DESCENDANTS_PER_ROW)
                 + " うち死亡 " + _num(f"sib_{i}_children_deceased", s.get("children_deceased", 0), L.MAX_DESCENDANTS_PER_ROW) + "</div>")
    p.append("</fieldset>")
    sp = inp.get("special") or {}
    p.append("<fieldset><legend>遺言・特殊事情</legend>遺言 " + _sel("will", _WILL_LABELS, inp.get("will", L.WILL_NONE)))
    for k, lbl in (("successive", "数次相続あり"), ("fetus", "胎児あり"), ("designated_shares", "相続分の指定・遺贈あり"),
                   ("renunciation_pending", "相続放棄の申述が未確定")):
        p.append(f'<label><input type="checkbox" name="{k}" value="1"{" checked" if sp.get(k) else ""}> {lbl}</label>')
    p.append("</fieldset>")
    p.append('<button type="submit">保存して判定する（弁護士確認前）</button></form>')
    return "".join(p)


def _result_html(intake_id: str, row: dict | None) -> str:
    e = html.escape
    if row is None:
        return '<p class="empty">まだ判定がありません。上の聞取りを保存すると判定します。</p>'
    res = row["result_detail"]
    p = [f'<div class="mark">{e(L.REVIEW_MARK)}（判定表 要大野確認・版 {e(row["law_table_version"])}）</div>',
         f'<p>版 {int(row["version"])}・状態 <span class="badge">{"確定済み" if row["status"] == H.STATUS_CONFIRMED else "下書き"}</span>'
         f'・結果 <span class="badge">{e(_RESULT_LABELS.get(res.get("result", ""), res.get("result", "")))}</span></p>']
    if res.get("heirs"):
        p.append("<table><tr><th>相続人</th><th>法定相続分</th><th>根拠条文</th><th>備考</th></tr>")
        for h in res["heirs"]:
            p.append(f'<tr><td>{e(h["label"])}</td><td>{e(h["share"])}</td><td>{e("・".join(h["basis"]))}</td><td>{e(h.get("note", ""))}</td></tr>')
        p.append("</table>")
    for r in res.get("attorney_reasons") or []:
        p.append(f'<p class="warn">要弁護士判断: {e(r)}</p>')
    for n in res.get("notes") or []:
        p.append(f'<p class="note">{e(n)}</p>')
    if row["status"] == H.STATUS_DRAFT:
        p.append(f'<form method="post" action="{PAGE}/{e(intake_id)}/heirs/confirm"><input type="hidden" name="heirs_id" value="{e(row["heirs_id"])}">'
                 '<label><input type="checkbox" name="confirm" value="1" required> 弁護士が判定表と結果を確認した</label>'
                 '<button type="submit">この版を確定する</button></form>')
    else:
        p.append(f'<p class="meta">確定: {e(str(row.get("confirmed_by") or ""))}｜{e(str(row.get("confirmed_at") or ""))}</p>')
    return "".join(p)


def _page(intake_id: str, row: dict | None, notice: str) -> str:
    e = html.escape
    parts = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
             '<meta name="viewport" content="width=device-width, initial-scale=1">',
             "<title>相続人の確定 - 案件管理（仮）</title>",
             "<style>body{font-family:sans-serif;max-width:860px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
             "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem}"
             ".card{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
             ".mark{display:inline-block;background:#fde2e2;color:#8a1f1f;border-radius:10px;padding:.3rem .8rem;font-weight:bold}"
             ".badge{display:inline-block;border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#fff3d6;color:#8a6d00}"
             ".row{margin:.25rem 0}fieldset{border:1px solid #d8dde5;border-radius:10px;margin:.5rem 0}select,input{font-size:.95rem;min-height:36px}"
             "table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #e3e6ec;padding:.3rem .4rem;text-align:left;font-size:.92rem}"
             ".warn{background:#fde2e2;color:#8a1f1f;border-radius:10px;padding:.4rem .8rem}.note{color:#445;font-size:.9rem}"
             ".notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}.meta{color:#8a8f98;font-size:.78rem}"
             "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;min-height:44px}</style></head><body>",
             "<h1>相続人の確定（第 2 段階）</h1>",
             f'<p class="sub">案件 {e(intake_id[:8])}…。人数と状態だけを入力します（氏名はここに入れません）。判定は法定相続分の参考であり、'
             f'<b>{e(L.REVIEW_MARK)}</b> の印が付きます。<a href="/app/souzoku">相続ダッシュボード</a></p>']
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    parts.append('<div class="card"><h2>判定結果</h2>' + _result_html(intake_id, row) + "</div>")
    parts.append('<div class="card"><h2>聞取り（構造）</h2>' + _form_html(intake_id, row["input"] if row else None) + "</div>")
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


# ── routes ────────────────────────────────────────────────────────────────────
async def heirs_page(request: Request):
    if not S.enabled():
        return _not_found()
    intake_id = str(request.path_params.get("intake_id", ""))
    if not _ID_RE.fullmatch(intake_id):
        return _not_found()
    try:
        if not await _intake_exists(intake_id):
            return _not_found()
        row = await H.latest(intake_id)
    except Exception:
        return HTMLResponse(_page(intake_id, None, _NOTICES["db"]), status_code=200)
    notice = _NOTICES.get(request.query_params.get("done", ""), "")
    return HTMLResponse(_page(intake_id, row, notice))


async def heirs_save(request: Request):
    if not S.enabled():
        return _not_found()
    intake_id = str(request.path_params.get("intake_id", ""))
    if not _ID_RE.fullmatch(intake_id):
        return _not_found()
    try:
        if not await _intake_exists(intake_id):
            return _not_found()
    except Exception:
        return _redirect(intake_id, "db")
    form = await request.form()
    try:
        raw = parse_form(form)
        H.normalize_input(raw)
    except H.InvalidInput:
        return _redirect(intake_id, "bad_input")
    try:
        out = await H.save_draft(intake_id, raw)
    except Exception:
        logger.warning("[SOUZOKU_HEIRS] save failed (fixed reason)")
        return _redirect(intake_id, "db")
    logger.info("[SOUZOKU_HEIRS] saved version=%s", emit(out["version"], "count", "log", "operator"))
    return _redirect(intake_id, "saved")


async def heirs_confirm(request: Request):
    if not S.enabled():
        return _not_found()
    intake_id = str(request.path_params.get("intake_id", ""))
    if not _ID_RE.fullmatch(intake_id):
        return _not_found()
    form = await request.form()
    heirs_id = str(form.get("heirs_id") or "")
    if not _ID_RE.fullmatch(heirs_id):
        return _bad_request()
    if not form.get("confirm"):
        return _redirect(intake_id, "confirm_required")
    try:
        row = await H.latest(intake_id)
        if row is None or row["heirs_id"] != heirs_id:
            return _bad_request()                    # 最新版以外は確定できない（古い版の確定を防ぐ）
        ok = await H.confirm(heirs_id)
    except Exception:
        return _redirect(intake_id, "db")
    return _redirect(intake_id, "confirmed" if ok else "already")


router.add_api_route(f"{PAGE}/{{intake_id}}/heirs", _gate(heirs_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{intake_id}}/heirs", _gate(heirs_save), methods=["POST"])
router.add_api_route(f"{PAGE}/{{intake_id}}/heirs/confirm", _gate(heirs_confirm), methods=["POST"])
