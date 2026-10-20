"""相続相談 受付フォーム（公開ページ）— SOUZOKU-2（QUEUE-2026-10-16-R G-5・第 1 段階・既定 OFF）

時効診断フォーム（shindan_form）と同じ仕組み（公開 route・fail-closed・固定文言・非反射・レート制限）を相続相談の受付に使う。
- prefix /souzoku-intake（/app 配下に置かない＝test_p4_001 の PUBLIC_ROUTES pin 維持）。
- 公開条件 fail-closed: flag `SOUZOKU_INTAKE_ENABLED` が OFF（既定）なら全メソッド・全別名で 404（存在しないフリ）。
- 6 問（hub/souzoku_intake.validate・閉集合）。不正は固定文言のみ（入力値を画面に戻さない＝非反射。選択肢は固定の一覧を再表示）。
- 争い=ある → 要承認文言 S-2 で終了（記録は status=out_of_scope で残す）。それ以外 → 保存＋業務 LINE 通知 1 通＋受付確認 S-1（画面）。
- 相談者への LINE 送信はしない（新しい送信経路を作らない）。LINE 連携（友だち追加→本人専用リンク）はチャネル裁定後の別票。
- RV-10: 入力値をログ・通知・エラー画面に出さない。
"""

import html
import secrets
import logging
import os
import time
from collections import OrderedDict

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from hub import souzoku_intake as S
from hub import souzoku_intake_texts as T
from hub import approved_texts  # TEXT-APPROVAL-GATE-1（H-0）: 相談者向け文言は承認台帳経由でしか描画しない
from hub.db import DatabaseNotConfigured
from hub.redact import emit
from hub.souzoku_constants import (DISPUTE_OPTIONS, HEIR_RELATIONS, INTAKE_MAX_BODY_BYTES, INTAKE_RATE_LIMIT,
                                   INTAKE_RATE_MAX_BUCKETS, INTAKE_RATE_WINDOW_SECONDS, PROPERTY_TYPES, WILL_OPTIONS)

logger = logging.getLogger("souzoku_intake_form")

router = APIRouter()
PATH = "/souzoku-intake"
_ENTRY_METHODS = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"]
_FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"
CLIENT_IP_HEADER = "X-Real-IP"
_ERROR_LABELS = {
    "deceased_name": "亡くなられた方のお名前を入力してください。",
    "deceased_kana": "ふりがなは ひらがな で入力してください。",
    "death_on": "亡くなられた日を 年-月-日 の形式で入力してください（未来の日付は入力できません）。",
    "heirs": "相続人の人数を 1 人以上、続柄ごとに数字で入力してください。",
    "will_status": "遺言の有無を選んでください。",
    "property_types": "主な財産の種類を 1 つ以上選んでください。",
    "dispute": "相続人の間の争いの有無を選んでください。",
    "nonce": "フォームを開き直してから送信してください。",
}
_PAGE_STYLE = ("body{font-family:sans-serif;background:#eef1f5;color:#222;margin:0;padding:1rem}.card{max-width:640px;margin:0 auto;"
               "background:#fff;border-radius:14px;padding:1rem 1.2rem}h1{font-size:1.2rem}fieldset{border:1px solid #d8dde5;"
               "border-radius:10px;margin:.6rem 0;padding:.6rem .8rem}legend{font-weight:bold}label{display:block;margin:.3rem 0}"
               "input[type=text],input[type=date],input[type=number]{font-size:1rem;padding:.4rem;border:1px solid #c8cdd5;border-radius:8px}"
               "button{font-size:1rem;padding:.6rem 1.2rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;min-height:44px}"
               ".err{background:#fde2e2;color:#8a1f1f;border-radius:10px;padding:.5rem .8rem}.note{color:#667;font-size:.9rem}")
_buckets: "OrderedDict[str, tuple[float, int]]" = OrderedDict()


def _not_found() -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": "Not Found"})


def _page(title: str, body_html: str, status: int = 200) -> HTMLResponse:
    return HTMLResponse(
        '<!doctype html><html lang="ja"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex">'
        f"<title>{html.escape(title)}</title><style>{_PAGE_STYLE}</style></head>"
        f'<body><div class="card">{body_html}</div></body></html>', status_code=status)


def _rate_key(request: Request) -> str:
    ip = (request.headers.get(CLIENT_IP_HEADER) or "").strip()
    if not ip and request.client:
        ip = request.client.host or ""
    return ip or "unknown"


def rate_exceeded(key: str, now: float | None = None) -> bool:
    now = time.monotonic() if now is None else now
    for _ in range(2):                                        # 先頭（最古）から定数ステップで期限切れを捨てる
        if not _buckets:
            break
        k, (start, _n) = next(iter(_buckets.items()))
        if now - start >= INTAKE_RATE_WINDOW_SECONDS:
            _buckets.popitem(last=False)
        else:
            break
    start, n = _buckets.get(key, (now, 0))
    if now - start >= INTAKE_RATE_WINDOW_SECONDS:
        start, n = now, 0
    n += 1
    _buckets[key] = (start, n)
    _buckets.move_to_end(key)
    while len(_buckets) > INTAKE_RATE_MAX_BUCKETS:
        _buckets.popitem(last=False)
    return n > INTAKE_RATE_LIMIT


def reset_rate_limit_for_tests() -> None:
    _buckets.clear()


_DISPUTE_CODES = {"ある": "a", "ない": "n", "わからない": "u"}
_DISPUTE_FROM_CODE = {v: k for k, v in _DISPUTE_CODES.items()}
_DONE_STATUSES = (S.STATUS_RECEIVED, S.STATUS_OUT_OF_SCOPE)


def _new_nonce() -> str:
    return secrets.token_hex(16)


def _form_html(errors: list[str], nonce: str | None = None) -> str:
    e = html.escape
    parts = ["<h1>相続のご相談 受付</h1>",
             '<p class="note">6 つの質問にお答えください。お名前以外の個人情報は、この画面では入力しません。</p>']
    if errors:
        parts.append('<div class="err">' + "".join(f"<div>{e(_ERROR_LABELS.get(x, '入力を確認してください。'))}</div>" for x in errors) + "</div>")
    parts.append(f'<form method="post" action="{PATH}">')
    parts.append('<fieldset><legend>1. 亡くなられた方（被相続人）のお名前</legend>'
                 '<label>氏名 <input type="text" name="deceased_name" required></label>'
                 '<label>ふりがな（ひらがな） <input type="text" name="deceased_kana" required></label></fieldset>')
    parts.append('<fieldset><legend>2. 亡くなられた日</legend><label><input type="date" name="death_on" required></label></fieldset>')
    parts.append('<fieldset><legend>3. 相続人の続柄と人数（該当するものに人数を入力）</legend>')
    for rel in HEIR_RELATIONS:
        parts.append(f'<label>{e(rel)} <input type="number" name="heirs_{e(rel)}" min="0" max="20" inputmode="numeric"> 人</label>')
    parts.append("</fieldset>")
    parts.append('<fieldset><legend>4. 遺言の有無</legend>' +
                 "".join(f'<label><input type="radio" name="will_status" value="{e(o)}" required> {e(o)}</label>' for o in WILL_OPTIONS) +
                 "</fieldset>")
    parts.append('<fieldset><legend>5. 主な財産の種類（複数選択可）</legend>' +
                 "".join(f'<label><input type="checkbox" name="property_types" value="{e(o)}"> {e(o)}</label>' for o in PROPERTY_TYPES) +
                 "</fieldset>")
    parts.append('<fieldset><legend>6. 相続人の間で遺産の分け方に争いはありますか</legend>' +
                 "".join(f'<label><input type="radio" name="dispute" value="{e(o)}" required> {e(o)}</label>' for o in DISPUTE_OPTIONS) +
                 "</fieldset>")
    # PRE-REVIEW-1 #307-1: 冪等キー（画面ごとに 1 つ・再送は同じ nonce＝2 件目を作らない）
    parts.append(f'<input type="hidden" name="nonce" value="{html.escape(nonce or _new_nonce())}">')
    parts.append('<button type="submit">送信する</button></form>')
    return "".join(parts)


async def _result_html(status: str, dispute: str) -> str:
    """TEXT-APPROVAL-GATE-1（H-0）: S-1〜S-3 は承認台帳（hub/approved_texts.APPROVED_SHA256）に本文 SHA が載るまで
    安全文に置き換わり、業務 LINE へ 1 日 1 回「未承認文言の参照 key=…」が届く（文言表の status 印とは独立の機械的な関所）。"""
    e = html.escape
    if status == S.STATUS_OUT_OF_SCOPE:
        return "<h1>ご入力ありがとうございました</h1><p>" + e(await approved_texts.gate_and_notify("S-2", T.text("S-2"))) + "</p>"
    body = "<h1>受け付けました</h1><p>" + e(await approved_texts.gate_and_notify("S-1", T.text("S-1"))) + "</p>"
    if dispute == "わからない":
        body += '<p class="note">' + e(await approved_texts.gate_and_notify("S-3", T.text("S-3"))) + "</p>"
    return body


def _content_ok(request: Request) -> bool:
    ct = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if ct != _FORM_CONTENT_TYPE:
        return False
    cl = request.headers.get("content-length")
    try:
        return cl is not None and 0 <= int(cl) <= INTAKE_MAX_BODY_BYTES
    except ValueError:
        return False


async def intake_entry(request: Request):
    if not S.enabled():
        return _not_found()
    if not T.all_approved():
        # PRE-REVIEW-1 #307-2: flag ON でも要承認の文言が残る間は受付画面を出さない（固定文・保存も通知もしない）
        return _page("準備中", "<h1>準備中です</h1><p>受付の準備が整うまでお待ちください。</p>", 503)
    if request.method == "GET":
        done = request.query_params.get("done", "")
        if done in _DONE_STATUSES:                           # PRG の結果画面（status と争いの区分だけ・PII なし）
            return _page("相続のご相談 受付", await _result_html(done, _DISPUTE_FROM_CODE.get(request.query_params.get("d", ""), "")))
        return _page("相続のご相談 受付", _form_html([], _new_nonce()))
    if request.method != "POST":
        return _not_found()
    if rate_exceeded(_rate_key(request)):
        return _page("しばらくお待ちください", "<h1>しばらくお待ちください</h1><p>送信回数の上限に達しました。時間をおいて再度お試しください。</p>", 429)
    if not _content_ok(request):
        return _page("入力を確認してください", "<h1>入力を確認してください</h1><p>フォームからもう一度送信してください。</p>", 400)
    form = await request.form()
    plain = {k: form.getlist(k) if k == "property_types" else form.get(k) for k in form.keys()}
    data, errors = S.validate(plain)
    nonce = str(form.get("nonce") or "")
    if not S.NONCE_RE.fullmatch(nonce):
        errors = list(errors) + ["nonce"]
    if errors:
        return _page("相続のご相談 受付", _form_html(errors, _new_nonce()), 400)
    try:
        intake_id, status, state = await S.receive(data, nonce=nonce)
    except DatabaseNotConfigured:
        logger.warning("[SOUZOKU_INTAKE] db not configured (intake not saved)")
        return _page("エラーが発生しました", "<h1>エラーが発生しました</h1><p>申し訳ありません。時間をおいて再度お試しください。</p>", 500)
    except Exception:
        logger.warning("[SOUZOKU_INTAKE] save failed (fixed reason)")
        return _page("エラーが発生しました", "<h1>エラーが発生しました</h1><p>申し訳ありません。時間をおいて再度お試しください。</p>", 500)
    logger.info("[SOUZOKU_INTAKE] received status=%s notify=%s", emit(status, "freetext", "log", "operator"),
                emit(state, "freetext", "log", "operator"))
    # PRE-REVIEW-1 #307-1: PRG（303 → 結果画面）。再読み込み・戻るで POST が再送されない
    return RedirectResponse(f"{PATH}?done={status}&d={_DISPUTE_CODES.get(data['dispute'], '')}", status_code=303)


async def intake_alias_not_found(request: Request):
    return _not_found()


router.add_api_route(PATH, intake_entry, methods=_ENTRY_METHODS, include_in_schema=False)
router.add_api_route(PATH + "/{_rest:path}", intake_alias_not_found, methods=_ENTRY_METHODS, include_in_schema=False)
