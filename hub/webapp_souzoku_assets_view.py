"""webapp_souzoku_assets_view — 財産目録・分割案・遺産分割協議書（案）の画面（SOUZOKU-4・QUEUE-2026-10-17-H H-5）

/app/souzoku/{案件ID}/assets                 財産の一覧・追加・無効化・割付（取得者・代償金）・法定相続分との差異
/app/souzoku/{案件ID}/kyogisho               協議書（案）の生成（人の明示操作・確認チェック必須）と生成済み一覧
/app/souzoku/{案件ID}/kyogisho/{doc_id}.docx ダウンロード（App には添付しない）
- 認証は _gate。flag SOUZOKU_INTAKE_ENABLED OFF=404。案件（souzoku_intake）が無ければ 404。
- 相続人は souzoku_heirs の最新版（確定済みを優先・無ければ下書き）のラベル。画面と docx には「弁護士確認前」の印。
- 口座番号は入力を受けても下 4 桁しか保存・表示しない。生年月日の欄は無い。ログは件数・種別語彙のみ。
"""
import html
import logging
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import souzoku_assets as A
from hub import souzoku_heirs as H
from hub import souzoku_intake as S
from hub import souzoku_kyogisho as K
from hub import souzoku_law_constants as L
from hub import souzoku_texts as T
from hub.redact import emit
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_souzoku_assets_view")
router = APIRouter()
PAGE = "/app/souzoku"
_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_NOTICES = {
    "added": "財産を追加しました。", "voided": "財産を無効にしました（行は残ります）。", "allocated": "割付を保存しました。",
    "bad_input": "入力が不正です。保存していません。", "not_found": "対象が見つかりません。", "db": "記録を読み書きできませんでした。",
    "generated": "遺産分割協議書（案・弁護士確認前）を生成しました。下の一覧からダウンロードできます。",
    "confirm_required": "確認チェックが必要です。生成していません。",
    "no_heirs": "相続人の判定がありません。先に /heirs で聞取りを保存してください。",
    "refused": "生成できませんでした（条文の承認表または雛形の検証に失敗）。",
}


def _not_found() -> Response:
    return Response(status_code=404)


def _redirect(intake_id: str, tail: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{intake_id}/{tail}?done={done}", status_code=303)


async def _intake(intake_id: str) -> dict | None:
    import sqlalchemy as sa
    from hub.db import session_scope
    async with session_scope() as s:
        r = (await s.execute(sa.select(S.souzoku_intake).where(S.souzoku_intake.c.intake_id == intake_id))).first()
    return dict(r._mapping) if r is not None else None


async def _heirs_result(intake_id: str) -> dict | None:
    rows = await H.versions(intake_id)
    confirmed = [r for r in rows if r["status"] == H.STATUS_CONFIRMED]
    row = (confirmed or rows)[-1] if rows else None
    return row["result_detail"] if row else None


def _style() -> str:
    return ("<style>body{font-family:sans-serif;max-width:900px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;background:#eef1f5;color:#222}"
            "h1{font-size:1.25rem}.card{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
            ".mark{display:inline-block;background:#fde2e2;color:#8a1f1f;border-radius:10px;padding:.3rem .8rem;font-weight:bold}"
            "table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #e3e6ec;padding:.3rem .4rem;text-align:left;font-size:.9rem}"
            ".notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0}.note{color:#445;font-size:.9rem}"
            "button{font-size:.9rem;padding:.4rem .8rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;min-height:40px}"
            "select,input{font-size:.9rem;min-height:34px}form.inline{display:inline}</style>")


def _assets_page(intake_id: str, heirs: dict | None, assets: list[dict], docs: list[dict], notice: str) -> str:
    e = html.escape
    labels = A.heir_labels(heirs)
    p = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">',
         "<title>財産目録・分割案 - 案件管理（仮）</title>", _style(), "</head><body>",
         "<h1>財産目録・分割案（第 4 段階）</h1>",
         f'<p class="note">案件 {e(intake_id[:8])}…　<a href="{PAGE}/{e(intake_id)}/heirs">相続人の確定へ</a>　<span class="mark">{e(L.REVIEW_MARK)}</span></p>']
    if notice:
        p.append(f'<p class="notice">{e(notice)}</p>')
    if not labels:
        p.append(f'<p class="notice">{e(_NOTICES["no_heirs"])}</p>')
    # 一覧＋割付
    p.append('<div class="card"><h2>財産</h2>')
    if not assets:
        p.append("<p class=\"note\">財産はまだありません。</p>")
    else:
        p.append("<table><tr><th>#</th><th>種別・表示</th><th>評価額</th><th>取得者</th><th>代償金</th><th></th></tr>")
        for i, a in enumerate(assets, start=1):
            comp = a.get("compensation") or {}
            comp_txt = f'{e(comp["to"])} へ {int(comp["yen"]):,} 円' if comp else ""
            opts = '<option value="">（未割付）</option>' + "".join(
                f'<option value="{e(lb)}"{" selected" if a.get("acquirer_label") == lb else ""}>{e(lb)}</option>' for lb in labels)
            copts = '<option value="">（なし）</option>' + "".join(
                f'<option value="{e(lb)}"{" selected" if comp.get("to") == lb else ""}>{e(lb)}</option>' for lb in labels)
            p.append(f"<tr><td>{i}</td><td>{e(A.describe(a['kind'], a['detail']))}</td>"
                     f"<td>{(format(int(a['value_yen']), ',') + ' 円') if a.get('value_yen') is not None else '未確定'}</td>"
                     f'<td><form class="inline" method="post" action="{PAGE}/{e(intake_id)}/assets/{e(a["asset_id"])}/allocate">'
                     f'<select name="acquirer">{opts}</select></td>'
                     f'<td><select name="comp_to">{copts}</select> <input type="text" name="comp_yen" value="{e(str(comp.get("yen", "")) if comp else "")}" size="10" placeholder="金額"> '
                     f'<input type="date" name="comp_due" value="{e(comp.get("due", "")) if comp else ""}"> <button type="submit">割付</button></form></td>'
                     f'<td><form class="inline" method="post" action="{PAGE}/{e(intake_id)}/assets/{e(a["asset_id"])}/void"><button type="submit">無効</button></form></td></tr>')
        p.append("</table>")
    p.append("</div>")
    # 追加フォーム
    p.append(f'<div class="card"><h2>財産を追加</h2><form method="post" action="{PAGE}/{e(intake_id)}/assets">')
    p.append('種別 <select name="kind">' + "".join(f'<option value="{k}">{e(v)}</option>' for k, v in A.KIND_LABELS.items()) + "</select> 評価額（円・未確定は空） <input type=\"text\" name=\"value_yen\" size=\"12\">")
    for kind, fields in A.FIELDS.items():
        p.append(f'<fieldset><legend>{e(A.KIND_LABELS[kind])}</legend>')
        for key, lbl in fields:
            name = f"{kind}__{key}"
            if key == "account_last4":
                p.append(f'<label>口座番号（全桁を入れても下 4 桁しか保存しません） <input type="text" name="{kind}__account_number" size="14"></label>')
            else:
                p.append(f'<label>{e(lbl)} <input type="text" name="{name}" size="24"></label>')
        p.append("</fieldset>")
    p.append('<button type="submit">追加</button></form></div>')
    # 差異
    d = A.diff_table(heirs, assets)
    p.append('<div class="card"><h2>法定相続分との差異（参考・弁護士確認前）</h2>')
    p.append(f'<p class="note">評価額合計 {d["total_yen"]:,} 円・評価未確定 {d["unvalued"]} 件・未割付 {d["unallocated"]} 件</p>')
    if d["rows"]:
        p.append("<table><tr><th>相続人</th><th>法定相続分</th><th>法定相続分相当額</th><th>取得額（代償金込み）</th><th>差</th></tr>")
        for r in d["rows"]:
            p.append(f"<tr><td>{e(r['label'])}</td><td>{e(r['share'])}</td><td>{r['expected_yen']:,} 円</td><td>{r['acquired_yen']:,} 円</td><td>{r['diff_yen']:+,} 円</td></tr>")
        p.append("</table>")
    p.append("</div>")
    # 協議書
    p.append(f'<div class="card"><h2>遺産分割協議書（案）</h2><p class="note">条文は要承認表（{e(T.TABLE_VERSION)}）。生成物には透かし文言が入ります。App には添付しません。</p>')
    p.append(f'<form method="post" action="{PAGE}/{e(intake_id)}/kyogisho"><label><input type="checkbox" name="confirm" value="1" required> 弁護士確認前の案として生成する</label> '
             '<button type="submit">生成する</button></form>')
    if docs:
        p.append("<table><tr><th>版</th><th>SHA-256</th><th>サイズ</th><th>作成</th><th></th></tr>")
        for dd in docs:
            p.append(f"<tr><td>{int(dd['version'])}</td><td>{e(dd['sha256'][:16])}…</td><td>{int(dd['size_bytes']):,}</td><td>{e(str(dd['created_at']))}</td>"
                     f'<td><a href="{PAGE}/{e(intake_id)}/kyogisho/{e(dd["doc_id"])}.docx">ダウンロード</a></td></tr>')
        p.append("</table>")
    p.append('</div><script src="/app/shell.js"></script></body></html>')
    return "".join(p)


def _ctx(request: Request):
    intake_id = str(request.path_params.get("intake_id", ""))
    return intake_id if (S.enabled() and _ID_RE.fullmatch(intake_id)) else None


async def assets_page(request: Request):
    intake_id = _ctx(request)
    if not intake_id:
        return _not_found()
    try:
        if await _intake(intake_id) is None:
            return _not_found()
        heirs = await _heirs_result(intake_id)
        assets = await A.list_assets(intake_id)
        docs = await K.list_docs(intake_id)
    except Exception:
        return HTMLResponse(_assets_page(intake_id, None, [], [], _NOTICES["db"]))
    return HTMLResponse(_assets_page(intake_id, heirs, assets, docs, _NOTICES.get(request.query_params.get("done", ""), "")))


async def assets_add(request: Request):
    intake_id = _ctx(request)
    if not intake_id:
        return _not_found()
    form = await request.form()
    kind = str(form.get("kind") or "")
    if kind not in A.KINDS:
        return _redirect(intake_id, "assets", "bad_input")
    raw = {key: form.get(f"{kind}__{key}") for key, _ in A.FIELDS[kind]}
    if kind == A.KIND_DEPOSIT:
        raw["account_number"] = form.get(f"{kind}__account_number")
    try:
        if await _intake(intake_id) is None:
            return _not_found()
        await A.add_asset(intake_id, kind, raw, form.get("value_yen"))
    except A.InvalidAsset:
        return _redirect(intake_id, "assets", "bad_input")
    except Exception:
        logger.warning("[SOUZOKU_ASSETS] add failed (fixed reason)")
        return _redirect(intake_id, "assets", "db")
    return _redirect(intake_id, "assets", "added")


async def assets_void(request: Request):
    intake_id = _ctx(request)
    asset_id = str(request.path_params.get("asset_id", ""))
    if not intake_id or not _ID_RE.fullmatch(asset_id):
        return _not_found()
    try:
        ok = await A.void_asset(intake_id, asset_id)
    except Exception:
        return _redirect(intake_id, "assets", "db")
    return _redirect(intake_id, "assets", "voided" if ok else "not_found")


async def assets_allocate(request: Request):
    intake_id = _ctx(request)
    asset_id = str(request.path_params.get("asset_id", ""))
    if not intake_id or not _ID_RE.fullmatch(asset_id):
        return _not_found()
    form = await request.form()
    try:
        labels = A.heir_labels(await _heirs_result(intake_id))
        ok = await A.allocate(intake_id, asset_id, form.get("acquirer"),
                              {"to": form.get("comp_to"), "yen": form.get("comp_yen"), "due": form.get("comp_due")}, valid_labels=labels)
    except A.InvalidAsset:
        return _redirect(intake_id, "assets", "bad_input")
    except Exception:
        return _redirect(intake_id, "assets", "db")
    return _redirect(intake_id, "assets", "allocated" if ok else "not_found")


async def kyogisho_generate(request: Request):
    intake_id = _ctx(request)
    if not intake_id:
        return _not_found()
    form = await request.form()
    if not form.get("confirm"):
        return _redirect(intake_id, "assets", "confirm_required")
    try:
        intake = await _intake(intake_id)
        if intake is None:
            return _not_found()
        heirs = await _heirs_result(intake_id)
        if not heirs or not heirs.get("heirs"):
            return _redirect(intake_id, "assets", "no_heirs")
        assets = await A.list_assets(intake_id)
        content = K.render(K.compose(intake, heirs, assets))
        out = await K.store(intake_id, content)
    except K.GenerationRefused:
        logger.warning("[SOUZOKU_KYOGISHO] generation refused (fixed reason)")
        return _redirect(intake_id, "assets", "refused")
    except Exception:
        logger.warning("[SOUZOKU_KYOGISHO] generation failed (fixed reason)")
        return _redirect(intake_id, "assets", "db")
    logger.info("[SOUZOKU_KYOGISHO] stored version=%s", emit(out["version"], "count", "log", "operator"))
    return _redirect(intake_id, "assets", "generated")


async def kyogisho_download(request: Request):
    intake_id = _ctx(request)
    doc_id = str(request.path_params.get("doc_id", ""))
    if not intake_id or not _ID_RE.fullmatch(doc_id):
        return _not_found()
    try:
        row = await K.get_doc(intake_id, doc_id)
    except Exception:
        return _not_found()
    if row is None:
        return _not_found()
    return Response(content=bytes(row["content"]),
                    media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    headers={"Content-Disposition": f'attachment; filename="kyogisho_draft_v{int(row["version"])}.docx"',
                             "Cache-Control": "no-store, private"})


router.add_api_route(f"{PAGE}/{{intake_id}}/assets", _gate(assets_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{intake_id}}/assets", _gate(assets_add), methods=["POST"])
router.add_api_route(f"{PAGE}/{{intake_id}}/assets/{{asset_id}}/void", _gate(assets_void), methods=["POST"])
router.add_api_route(f"{PAGE}/{{intake_id}}/assets/{{asset_id}}/allocate", _gate(assets_allocate), methods=["POST"])
router.add_api_route(f"{PAGE}/{{intake_id}}/kyogisho", _gate(kyogisho_generate), methods=["POST"])
router.add_api_route(f"{PAGE}/{{intake_id}}/kyogisho/{{doc_id}}.docx", _gate(kyogisho_download), methods=["GET"])
