"""webapp_jikou_cert_view — JIKOU-CERT-1: 時効援用完了報告書の生成・ダウンロード画面 /app/jikou/cert/{案件ID}

QUEUE-2026-10-20-HI I-1。認証は _gate（PWA の人＝大野のみ）。/app/jikou/result と同型（サーバ描画・native form POST・PRG 303）。
- GET: 結果記録（hub/jikou_followup）のうち 成立 の枠と、保管済みの報告書（版・作成日時）を並べる。書込ゼロ。
- POST /generate/{slot}: 明示確認（confirm=1）必須。hub/jikou_cert.generate（成立以外・文言未承認・PII 混入は固定理由で拒否）。
- GET /doc/{doc_id}.pdf | .docx: 保管済みの版をダウンロード（_gate の内側のみ）。
- 画面に出すのは 案件 No.・枠・社名・版・日時・固定理由。顧客名・住所・電話・LINE userId は出さない（RV-10）。
- 相談者への送信経路は持たない（送信は I-4' の kintone 承認ボタン経由のみ）。
"""

import html
import logging
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import jikou_cert as C
from hub import jikou_followup as F
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_jikou_cert_view")

router = APIRouter()

PAGE = "/app/jikou/cert"
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_DOC_RE = re.compile(r"^[0-9a-f]{32}$")
_NOTICES = {
    "generated": "完了報告書を生成しました（新しい版）。",
    "confirm_required": "「内容を確認しました」にチェックしてから生成してください。生成していません。",
    "refused_not_established": "結果が「成立」でない枠は生成できません。",
    "refused_dates_missing": "発送日または回答日が未記録のため生成できません（結果記録画面で記録してください）。",
    "refused_creditor_missing": "債権者名が無いため生成できません。",
    "refused_name_missing": "案件情報（App 21）を読めないか宛名が空のため生成できません。",
    "refused_text_unapproved": "報告書の文面が承認台帳に未登録のため生成できません（司令塔の票で承認後に生成可）。",
    "refused_texts_pin": "文面表の pin が一致しないため生成できません。",
    "refused_template_sha": "雛形 docx の pin が一致しないため生成できません。",
    "refused_placeholder_left": "差し込みが残ったため生成を中止しました。",
    "refused_pii_in_body": "本文に相談者の連絡先が含まれるため生成を中止しました。",
    "refused_pdf_failed": "PDF 変換に失敗したため生成を中止しました（docx も保存していません）。",
    "refused_no_row": "この枠の結果記録がありません。",
    "db": "保管表を読み書きできませんでした。",
}
_STYLE = (
    "<style>body{font-family:sans-serif;max-width:760px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
    "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}h2{font-size:1.02rem;margin:1rem 0 .3rem}"
    ".sub{color:#667;font-size:.85rem;margin:0 0 .8rem}.item{background:#fff;border:1px solid #d8dde5;border-radius:14px;"
    "padding:.75rem 1rem;margin:.6rem 0}.meta{color:#8a8f98;font-size:.78rem}label{display:block;margin:.4rem 0;font-size:.92rem}"
    "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;min-height:44px}"
    ".notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}"
    ".badge{display:inline-block;border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#e2f5e6;color:#1f6b2e}</style>"
)


def _bad_request() -> Response:
    return Response(status_code=400)


def _not_found() -> Response:
    return Response(status_code=404)


def _redirect(rid: str, done: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}/{rid}?done={done}", status_code=303)


def _page(rid: str, rows: list[dict], docs: list[dict], notice: str) -> str:
    e = html.escape
    parts = ['<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
             '<meta name="viewport" content="width=device-width, initial-scale=1">',
             f"<title>完了報告書 No.{e(rid)} - 案件管理（仮）</title>", _STYLE, "</head><body>",
             f"<h1>時効援用完了報告書 No.{e(rid)}</h1>",
             f'<p class="sub">結果が「成立」の債権者ごとに生成します（人の明示操作のみ・自動生成も相談者への送信もしません）。'
             f'<a href="/app/jikou/result/{e(rid)}">結果記録</a>｜<a href="/app/pending">対応待ち</a></p>']
    if notice:
        parts.append(f'<p class="notice">{e(notice)}</p>')
    by_slot: dict = {}
    for d in docs:
        by_slot.setdefault(int(d["slot"]), []).append(d)
    shown = 0
    for r in rows:
        slot = int(r["slot"])
        if r.get("result") != F.RESULT_ESTABLISHED:
            continue
        shown += 1
        parts.append(f'<div class="item"><h2>枠 {slot}: {e(str(r.get("creditor_name") or ""))}</h2>'
                     f'<span class="badge">成立</span>')
        for d in by_slot.get(slot, []):
            parts.append(f'<div class="meta">版 {int(d["version"])}（{e(str(d.get("created_at") or "")[:16])}）'
                         f' <a href="{PAGE}/{e(rid)}/doc/{e(d["doc_id"])}.pdf">PDF</a>'
                         f' <a href="{PAGE}/{e(rid)}/doc/{e(d["doc_id"])}.docx">docx</a></div>')
        parts.append(f'<form method="post" action="{PAGE}/{e(rid)}/generate/{slot}">'
                     '<label><input type="checkbox" name="confirm" value="1"> 内容を確認しました</label>'
                     '<button type="submit">完了報告書を生成（新しい版）</button></form></div>')
    if not shown:
        parts.append('<p class="notice">結果が「成立」の枠がありません。結果記録画面で記録してください。</p>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def cert_page(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    code = request.query_params.get("done", "")
    notice = _NOTICES.get(code, "") if code else ""
    try:
        rows = await F.rows_for_case(rid)
        docs = await C.list_docs(rid)
    except Exception:
        rows, docs, notice = [], [], _NOTICES["db"]
    return HTMLResponse(_page(rid, rows, docs, notice))


async def cert_generate(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    slot = str(request.path_params.get("slot", ""))
    if not _ID_RE.fullmatch(rid) or slot not in ("1", "2", "3"):
        return _bad_request()
    form = await request.form()
    if str(form.get("confirm", "")) != "1":
        return _redirect(rid, "confirm_required")
    try:
        await C.generate(rid, int(slot), actor="human")
    except C.GenerationRefused as e:
        return _redirect(rid, "refused_" + str(e))
    except Exception:
        logger.warning("[JIKOU_CERT] generate failed (fixed reason)")
        return _redirect(rid, "db")
    return _redirect(rid, "generated")


async def cert_download(request: Request):
    rid = str(request.path_params.get("record_id", ""))
    doc_id = str(request.path_params.get("doc_id", ""))
    ext = str(request.path_params.get("ext", ""))
    if not _ID_RE.fullmatch(rid) or not _DOC_RE.fullmatch(doc_id) or ext not in ("pdf", "docx"):
        return _bad_request()
    try:
        doc = await C.get_doc(rid, doc_id)
    except Exception:
        return _not_found()
    if doc is None:
        return _not_found()
    body = doc["pdf"] if ext == "pdf" else doc["docx"]
    media = "application/pdf" if ext == "pdf" else "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    fname = f"jikou_cert_{rid}_{int(doc['slot'])}_v{int(doc['version'])}.{ext}"      # 案件 No.・枠・版のみ（氏名なし）
    return Response(content=body, media_type=media,
                    headers={"Content-Disposition": f'attachment; filename="{fname}"', "Cache-Control": "no-store"})


router.add_api_route(PAGE + "/{record_id}", _gate(cert_page), methods=["GET"])
router.add_api_route(PAGE + "/{record_id}/generate/{slot}", _gate(cert_generate), methods=["POST"])
router.add_api_route(PAGE + "/{record_id}/doc/{doc_id}.{ext}", _gate(cert_download), methods=["GET"])


def wire_all(app) -> None:
    """main.py 末尾の 1 行（Q6 と同方式・catch-all より先に結線）。"""
    from hub.webapp_brain_view import include_before_catch_all
    include_before_catch_all(app, router)
