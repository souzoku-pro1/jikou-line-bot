"""webapp_jikou_receipt_view — JIKOU-RECEIPT-1: 領収書の発行・再発行・ダウンロード画面 /app/jikou/receipt/{案件ID}

QUEUE-2026-10-20-HI I-2。認証は _gate（PWA の人＝大野のみ）。/app/jikou/cert と同型（サーバ描画・native form POST・PRG 303）。
- GET: App 21 の入金状況・債権者数・料金規則による内訳（未請求分）と、既発行の領収書（番号・日付・合計・再発行）。書込ゼロ。
- POST /issue: 明示確認（confirm=1）必須。hub/jikou_receipt.issue（入金済み以外・未請求 0・文言未承認は固定理由で拒否）。
- POST /reissue/{receipt_id}: 確認必須。同番号＋「再発行」。
- GET /doc/{receipt_id}.xlsx: ダウンロード（_gate の内側のみ・ファイル名に氏名なし）。
- 画面に出すのは 案件 No.・社数・金額・番号・日付。顧客名・住所・電話・LINE userId は出さない（RV-10）。送信経路なし。
"""

import html
import logging
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import jikou_followup as F
from hub import jikou_receipt as R
from hub import kintone
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_jikou_receipt_view")

router = APIRouter()

PAGE = "/app/jikou/receipt"
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_DOC_RE = re.compile(r"^[0-9a-f]{32}$")
_NOTICES = {
    "issued": "領収書を発行しました。",
    "reissued": "領収書を再発行しました（同じ番号・「再発行」表示）。",
    "confirm_required": "「内容を確認しました」にチェックしてから発行してください。発行していません。",
    "refused_not_paid": "App 21 の入金状況が「入金済み」ではないため発行できません。",
    "refused_no_creditors": "債権者（問い合わせ業者名／対象債権者2／対象債権者3）が無いため発行できません。",
    "refused_nothing_to_bill": "未請求の社数がありません（追加依頼があれば App 21 に債権者を追加してから）。",
    "refused_name_missing": "案件情報（App 21）を読めないか宛名が空のため発行できません。",
    "refused_text_unapproved": "但し書きの文言が承認台帳に未登録のため発行できません（司令塔の票で承認後に発行可）。",
    "refused_texts_pin": "文面表の pin が一致しないため発行できません。",
    "refused_template_sha": "雛形 xlsx の pin が一致しないため発行できません。",
    "refused_placeholder_left": "差し込みが残ったため発行を中止しました。",
    "refused_number_alloc": "領収書番号を確保できませんでした。もう一度お試しください。",
    "refused_not_found": "再発行の元となる領収書が見つかりません。",
    "db": "保管表を読み書きできませんでした。",
}
_STYLE = (
    "<style>body{font-family:sans-serif;max-width:760px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
    "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}h2{font-size:1.02rem;margin:1rem 0 .3rem}"
    ".sub{color:#667;font-size:.85rem;margin:0 0 .8rem}.item{background:#fff;border:1px solid #d8dde5;border-radius:14px;"
    "padding:.75rem 1rem;margin:.6rem 0}.meta{color:#8a8f98;font-size:.78rem}label{display:block;margin:.4rem 0;font-size:.92rem}"
    "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;min-height:44px}"
    ".notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}"
    "table{border-collapse:collapse}td,th{border:1px solid #d8dde5;padding:.25rem .6rem;text-align:right}th{background:#f4f6f9}</style>"
)


def _bad_request() -> Response:
    return Response(status_code=400)


def _not_found() -> Response:
    return Response(status_code=404)


def _redirect(rid: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{rid}?done={done}", status_code=303)


def _yen(n) -> str:
    return f"{int(n):,}"


def _page(rid: str, paid: str, count: int, already: int, b: R.Breakdown | None, receipts: list[dict], notice: str) -> str:
    e = html.escape
    parts = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
             '<meta name="viewport" content="width=device-width, initial-scale=1">',
             f"<title>領収書 No.{e(rid)} - 案件管理（仮）</title>", _STYLE, "</head><body>",
             f"<h1>時効援用 領収書 No.{e(rid)}</h1>",
             f'<p class="sub">入金確認後に人の操作で発行します（金額は料金規則から自動・手入力なし・相談者への送信はありません）。'
             f'<a href="/app/jikou/result/{e(rid)}">結果記録</a>｜<a href="/app/jikou/cert/{e(rid)}">完了報告書</a></p>']
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    parts.append(f'<div class="item"><h2>状態</h2><div>入金状況: {e(paid or "（読めません）")}</div>'
                 f'<div>債権者数: {count} 社（請求済み {already} 社）</div></div>')
    if b and b.count_billed > 0:
        parts.append('<div class="item"><h2>今回の内訳（未請求分）</h2><table><tr><th>内容</th><th>数量</th><th>単価</th><th>金額</th></tr>'
                     f'<tr><td style="text-align:left">1〜2社目</td><td>{b.qty1}</td><td>{_yen(b.unit1)}</td><td>{_yen(b.amt1)}</td></tr>'
                     f'<tr><td style="text-align:left">3社目以降</td><td>{b.qty2}</td><td>{_yen(b.unit2)}</td><td>{_yen(b.amt2)}</td></tr>'
                     f'<tr><td style="text-align:left">小計</td><td></td><td></td><td>{_yen(b.subtotal)}</td></tr>'
                     f'<tr><td style="text-align:left">消費税（{R.TAX_RATE_PERCENT}%）</td><td></td><td></td><td>{_yen(b.tax)}</td></tr>'
                     f'<tr><td style="text-align:left">合計</td><td></td><td></td><td>{_yen(b.total)}</td></tr></table>'
                     f'<form method="post" action="{PAGE}/{e(rid)}/issue">'
                     '<label><input type="checkbox" name="confirm" value="1"> 内容を確認しました</label>'
                     '<button type="submit">領収書を発行</button></form></div>')
    else:
        parts.append('<div class="item"><h2>今回の内訳</h2><div class="meta">未請求の社数がありません。</div></div>')
    if receipts:
        parts.append('<div class="item"><h2>発行済み</h2>')
        for r in receipts:
            tag = "（再発行）" if r.get("reissue") else ""
            parts.append(f'<div class="meta">No. {e(str(r["number"]))}{tag} {e(str(r["issued_on"]))} 合計 {_yen(r["total"])} 円'
                         f'（{int(r["count_billed"])} 社） <a href="{PAGE}/{e(rid)}/doc/{e(r["receipt_id"])}.xlsx">xlsx</a>')
            if not r.get("reissue"):
                parts.append(f' <form method="post" action="{PAGE}/{e(rid)}/reissue/{e(r["receipt_id"])}" style="display:inline">'
                             '<input type="hidden" name="confirm" value="1"><button type="submit">再発行</button></form>')
            parts.append("</div>")
        parts.append("</div>")
    parts.append('<p class="meta">PDF 変換は xlsx 用の経路が無いため xlsx のみ（保留）。</p>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def receipt_page(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    code = request.query_params.get("done", "")
    notice = _NOTICES.get(code, "") if code else ""
    try:
        record = await kintone.get_record(F.APP21, rid)
    except Exception:
        record = None
    paid = R._fv(record, R.FIELD_PAYMENT) if record else ""
    count = len(F.creditors_of(record)) if record else 0
    try:
        already = await R.billed_count(rid)
        receipts = await R.list_receipts(rid)
    except Exception:
        already, receipts, notice = 0, [], _NOTICES["db"]
    b = R.compute(count, already) if count else None
    return HTMLResponse(_page(rid, paid, count, already, b, receipts, notice))


async def receipt_issue(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    form = await request.form()
    if str(form.get("confirm", "")) != "1":
        return _redirect(rid, "confirm_required")
    try:
        await R.issue(rid, actor="human")
    except R.IssueRefused as e:
        return _redirect(rid, "refused_" + str(e))
    except Exception:
        logger.warning("[JIKOU_RECEIPT] issue failed (fixed reason)")
        return _redirect(rid, "db")
    return _redirect(rid, "issued")


async def receipt_reissue(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    receipt_id = str(request.path_params.get("receipt_id", ""))
    if not _ID_RE.fullmatch(rid) or not _DOC_RE.fullmatch(receipt_id):
        return _bad_request()
    form = await request.form()
    if str(form.get("confirm", "")) != "1":
        return _redirect(rid, "confirm_required")
    try:
        await R.reissue(rid, receipt_id, actor="human")
    except R.IssueRefused as e:
        return _redirect(rid, "refused_" + str(e))
    except Exception:
        logger.warning("[JIKOU_RECEIPT] reissue failed (fixed reason)")
        return _redirect(rid, "db")
    return _redirect(rid, "reissued")


async def receipt_download(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    receipt_id = str(request.path_params.get("receipt_id", ""))
    if not _ID_RE.fullmatch(rid) or not _DOC_RE.fullmatch(receipt_id):
        return _bad_request()
    try:
        doc = await R.get_receipt(rid, receipt_id)
    except Exception:
        return _not_found()
    if doc is None:
        return _not_found()
    fname = f"jikou_receipt_{rid}_{doc['number']}{'_reissue' if doc['reissue'] else ''}.xlsx"
    return Response(content=doc["xlsx"], media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"', "Cache-Control": "no-store"})


router.add_api_route(PAGE + "/{record_id}", _gate(receipt_page), methods=["GET"])
router.add_api_route(PAGE + "/{record_id}/issue", _gate(receipt_issue), methods=["POST"])
router.add_api_route(PAGE + "/{record_id}/reissue/{receipt_id}", _gate(receipt_reissue), methods=["POST"])
router.add_api_route(PAGE + "/{record_id}/doc/{receipt_id}.xlsx", _gate(receipt_download), methods=["GET"])


def wire_all(app) -> None:
    from hub.webapp_brain_view import include_before_catch_all
    include_before_catch_all(app, router)
