"""webapp_reply_view — JIKOU-REPLY-Q1b: 大野の返信欄（PWA・明示送信）

正本: 時効LINEボット_返信規則_v1.4.md §7（大野の返信の扱い）・§10-1（返信欄・明示送信・
操作 ID・送信確認待ち）・§10-2（許可表の「大野の明示送信」列）・§10-7（手本）・§12 Q1b。
QUEUE-2026-10-11-B Q-3 (a)(b)(d)(f)。

- 入口: 承認画面（/app/approvals）の各件に「この相談者に返信する」リンク。承認画面は参照専用の
  閉集合 pin（button 2・input 1・form なし・fetch は参照 API のみ）を維持するため、返信欄は本画面
  （/app/reply/{App 29 record_id}・サーバ描画・native form POST・PRG 303）に置く
  （/app/send_ops と同型）。URL・画面に LINE userId を載せない（宛先は App 29 のレコード ID で
  指定し、サーバ内で line_user_id を解決する＝RV-10）。
- 認証は P4-001 の関所（_gate）＝承認 webhook と同じく「大野のみ」の前提。未認証は 303 → /app/login。
- 送信: 操作 ID（画面描画時に発行する uuid・hidden）を固定し、send_ledger を actor=human・
  purpose=reply で経由（hub/line_channel.push_text_explicit＝guarded_http・3 値＋重複 2 値）。
  通信例外・期限到達は「送信確認待ち」（/app/send_ops に既存どおり表示・自動再送なし）。
  同じ操作 ID の再送信（二重クリック・再読込後の再送）は副作用なしで弾く。
- 許可表 (f): human 列のみ適用（send_ledger.human_explicit_decision）。停止判定の照会はしない
  （対象外）。画面の明示確認チェックが無ければ送らない（固定 303 で戻す）。
- App 28 (b): 送信成功後に role=human・category=human_reply・auto_sent=no で 1 行書く。App 28 の
  role に human の選択肢が無ければ **失敗にせず件数ログで skip**（選択肢追加＝大野の kintone 操作
  の後に有効になる）。時刻は kintone の作成日時。
- 手本 (d): 画面のチェック（既定 ON）で hub/reply_exemplar に匿名化して保存（残留検査に落ちたら
  保存しない）。段階は App 21 の status（classify_routing）・業者名は App 21 の問い合わせ業者名。
- メモリ履歴: ヒアリング経路のプロセス内履歴（main.conversation_histories）に当該相談者の
  履歴があれば assistant として反映（§7「ヒアリング側のメモリ履歴にも human 行を反映」）。
- 順序（副作用）: [重複検査 → 許可表 → LINE 送信 → App 28 記録 → メモリ反映 → 手本保存]。
  送信後の失敗は送信を取り消さず、結果コードを画面に出す。
- RV-10: ログは固定語彙＋emit のみ。本文・氏名・LINE userId はログに出さない。
"""

import html
import logging
import re
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import kintone, line_channel, reply_exemplar, send_ledger
from hub.redact import emit
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_reply_view")

router = APIRouter()

PAGE = "/app/reply"
BUSINESS = "jikou"
APP_APPROVAL = kintone.KintoneApp("App 29 (承認キュー)", "APP_APPROVAL", "TOKEN_APPROVAL")
APP_CHATLOG = kintone.KintoneApp("App 28 (チャットログ)", "APP_CHATLOG", "TOKEN_CHATLOG")
ROLE_HUMAN = "human"
CATEGORY_HUMAN_REPLY = "human_reply"
HISTORY_LIMIT = 20
MAX_TEXT_CHARS = 2000
_RECORD_ID_RE = re.compile(r"^[0-9]{1,12}$")
_OP_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
_ROLE_LABELS = {"user": "相談者", "assistant": "bot", ROLE_HUMAN: "事務所（大野）"}

# 送信結果（done）・App 28 記録（log）・手本（ex）の閉集合（画面文言は固定・入力値を反射しない）
_DONE_NOTICES = {
    "sent": "送信しました。",
    "failed": "LINE への送信に失敗しました（LINE 側が受け付けませんでした）。送信されていません。",
    "unconfirmed": "送信結果を確認できませんでした。送信確認待ちで結果を確定してください（自動再送はしません）。",
    "duplicate_sent": "この操作は既に送信済みです（二重送信はしていません）。",
    "duplicate_unconfirmed": "この操作は送信確認待ちです。送信確認待ちで結果を確定してください。",
    "confirm_required": "送信内容の確認チェックが必要です。送信していません。",
    "not_found": "宛先の相談者を特定できませんでした。送信していません。",
    "bad_input": "入力が不正です。送信していません。",
    "blocked": "許可表により送信できません。",
}
_LOG_NOTICES = {
    "written": "チャットログ（App 28）に事務所の返信として記録しました。",
    "skipped": "チャットログの role に「human」の選択肢が無いため記録を省略しました（kintone 側で選択肢を追加すると記録されます）。",
    "failed": "チャットログへの記録に失敗しました（送信は完了しています）。",
    "unconfigured": "チャットログは未設定のため記録していません。",
}
_EX_NOTICES = {
    "saved": "手本として匿名化して保存しました。",
    "rejected": "匿名化の残留検査に通らなかったため手本には保存していません。",
    "skipped": "手本の保存先（DB）が未設定のため保存していません。",
    "failed": "手本の保存に失敗しました（送信は完了しています）。",
    "off": "",
    "bad_input": "",
}

_skip_count = {"role_option_missing": 0}


def _bad_request() -> Response:
    return Response(status_code=400)     # 固定応答（入力値を反射しない）


def _v(rec: dict, code: str) -> str:
    return str(((rec or {}).get(code) or {}).get("value") or "")


async def _approval_record(record_id: str) -> dict | None:
    try:
        return await kintone.get_record(APP_APPROVAL, record_id)
    except Exception:
        return None


async def _history_rows(user_id: str) -> list[dict]:
    """App 28 の直近行（role・message・作成日時）。内部行（画像の保留/人対応済マーカー）は除く。"""
    if not (APP_CHATLOG.app_id() and APP_CHATLOG.token()):
        return []
    from hub.image_intake import IMAGE_HUMAN_CLOSED_MARKER, IMAGE_HUMAN_HOLD_MARKER
    try:
        rows = await kintone.search_records(
            APP_CHATLOG, f'line_user_id = "{user_id}" order by $id desc limit {HISTORY_LIMIT}',
            fields=["role", "message", "作成日時", "category"])
    except Exception:
        return []
    out = []
    for r in reversed(rows):
        msg = _v(r, "message")
        if msg in (IMAGE_HUMAN_HOLD_MARKER, IMAGE_HUMAN_CLOSED_MARKER):
            continue
        out.append({"role": _v(r, "role"), "message": msg, "at": _v(r, "作成日時")})
    return out


# ── App 28 への human 行（b） ────────────────────────────────────────────────────
async def write_human_chatlog(user_id: str, text: str) -> str:
    """送信成功後の App 28 記録。戻り値は閉集合: written / skipped（role に human の選択肢なし・
    件数ログ）/ failed（固定語彙ログ）/ unconfigured。"""
    if not (APP_CHATLOG.app_id() and APP_CHATLOG.token()):
        return "unconfigured"
    try:
        props = await kintone.get_form_fields(APP_CHATLOG)
        options = (props.get("role") or {}).get("options") or {}
        if ROLE_HUMAN not in options:
            _skip_count["role_option_missing"] += 1
            logger.info("[REPLY] chatlog human row skipped (role option missing) total=%s",
                        emit(_skip_count["role_option_missing"], "count", "log", "operator"))
            return "skipped"
        await kintone.create_record(APP_CHATLOG, {
            "line_user_id": user_id, "role": ROLE_HUMAN, "message": text,
            "category": CATEGORY_HUMAN_REPLY, "auto_sent": "no"})
    except Exception:
        logger.warning("[REPLY] chatlog human row write failed (fixed reason)")
        return "failed"
    return "written"


def reflect_memory_history(user_id: str, text: str) -> bool:
    """ヒアリング経路のプロセス内履歴に assistant として反映（履歴が無い相談者は何もしない＝
    次の受信で App 28 から復元される）。戻り値=反映したか。"""
    try:
        import main as _main
        hist = _main.conversation_histories.get(user_id)
    except Exception:
        return False
    if hist is None:
        return False
    if hist and hist[-1].get("role") == "assistant":
        hist[-1]["content"] = str(hist[-1].get("content") or "") + "\n" + text
    else:
        hist.append({"role": "assistant", "content": text})
    return True


async def _save_exemplar(user_id: str, inbound_text: str, text: str, op_id: str,
                         customer_name: str) -> str:
    creditor, stage = None, reply_exemplar.STAGE_CONSULT
    try:
        from chat_responder import classify_routing, get_app21_record
        rec = await get_app21_record(user_id)
        if rec:
            creditor = (_v(rec, "ルックアップ_0") or _v(rec, "問い合わせ業者名") or None)
            if classify_routing(_v(rec, "status")) == "post_engagement":
                stage = reply_exemplar.STAGE_ENGAGED
    except Exception:
        pass
    ref = None
    try:
        state = await send_ledger.conversation_state(BUSINESS, user_id)
        ref = state["ref"] if state else None
    except Exception:
        ref = None
    return await reply_exemplar.save_exemplar(
        business=BUSINESS, inbound_text=inbound_text, human_reply=text,
        customer_name=customer_name, creditor_name=creditor, stage=stage,
        conversation_ref=ref, source_op_id=op_id)


# ── 画面 ────────────────────────────────────────────────────────────────────
def _page(record_id: str, rec: dict | None, rows: list[dict], op_id: str,
          notices: list[str]) -> str:
    e = html.escape
    parts = [
        '<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>相談者への返信 - 案件管理（仮）</title>",
        "<style>body{font-family:sans-serif;max-width:720px;margin:0 auto;"
        "padding:.6rem 1rem 2rem;box-sizing:border-box;background:#eef1f5;color:#222}"
        "h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
        ".item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
        ".name{font-size:1.05rem;font-weight:bold}.lbl{color:#667;font-size:.78rem;margin-top:.4rem}"
        "pre{white-space:pre-wrap;word-break:break-word;font-family:inherit;font-size:.93rem;"
        "border-radius:12px;padding:.55rem .8rem;margin:.4rem 0}"
        "pre.user{background:#f2f4f7;border:1px solid #e2e6ec}pre.assistant{background:#eef7e8;border:1px solid #cfe6c2}"
        "pre.human{background:#e8f0fb;border:1px solid #c5d8f2}.meta{color:#8a8f98;font-size:.78rem;margin-top:.3rem}"
        "textarea{width:100%;min-height:9rem;font-size:1rem;border-radius:10px;border:1px solid #c8cdd5;"
        "padding:.5rem;box-sizing:border-box}label{display:block;margin:.5rem 0;font-size:.92rem}"
        "button{font-size:.95rem;padding:.5rem 1rem;border-radius:10px;border:1px solid #c8cdd5;"
        "background:#fff;min-height:44px}.notice{background:#e8f0fb;border-radius:10px;"
        "padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}.empty{color:#667;text-align:center;margin:1.2rem 0}"
        "</style></head><body>",
        "<h1>相談者への返信</h1>",
        '<p class="sub">事務所（大野）からの返信を LINE で送ります。送信は送信操作記録を通り、'
        "結果不明は送信確認待ちに残ります（自動再送なし）。"
        f'<a href="/app/approvals">返信の承認へ戻る</a>｜<a href="/app/send_ops">送信確認待ち</a></p>',
    ]
    for n in notices:
        if n:
            parts.append(f'<p class="notice">{e(n)}</p>')
    if rec is None:
        parts.append('<p class="empty">宛先の相談者を特定できませんでした</p>')
        parts.append('<script src="/app/shell.js"></script></body></html>')
        return "".join(parts)
    parts.append('<div class="item">')
    parts.append(f'<div class="name">{e(_v(rec, "顧客名") or "（お名前未登録）")}</div>')
    parts.append(f'<div class="meta">承認キュー No.{e(record_id)}｜カテゴリ: {e(_v(rec, "カテゴリ"))}</div>')
    parts.append('<div class="lbl">お客様からのメッセージ（承認キュー）</div>')
    parts.append(f'<pre class="user">{e(_v(rec, "顧客メッセージ"))}</pre></div>')
    parts.append('<div class="item"><div class="lbl">直近のやり取り（チャットログ・古い順）</div>')
    if not rows:
        parts.append('<p class="empty">チャットログはありません</p>')
    for r in rows:
        role = str(r.get("role", ""))
        cls = role if role in _ROLE_LABELS else "user"
        parts.append(f'<div class="meta">{e(_ROLE_LABELS.get(role, role))}｜{e(str(r.get("at", "")))}</div>')
        parts.append(f'<pre class="{cls}">{e(str(r.get("message", "")))}</pre>')
    parts.append("</div>")
    draft = _v(rec, "AI下書き")
    parts.append(f'<form method="post" action="{PAGE}/{e(record_id)}/send" class="item">'
                 f'<input type="hidden" name="op_id" value="{e(op_id)}">'
                 '<div class="lbl">返信文（AI 下書きがあれば初期表示。直して送れます）</div>'
                 f'<textarea name="text" maxlength="{MAX_TEXT_CHARS}" required>{e(draft)}</textarea>'
                 '<label><input type="checkbox" name="confirm" value="1"> 内容を確認しました'
                 "（停止中・人対応中でも事務所の判断で送ります）</label>"
                 '<label><input type="checkbox" name="exemplar" value="1" checked> '
                 "この返信を手本として保存する（氏名・連絡先などを伏せて保存）</label>"
                 '<button type="submit">LINE で送信する</button></form>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


async def reply_page(request: Request):
    record_id = str(request.path_params.get("record_id", ""))
    if not _RECORD_ID_RE.fullmatch(record_id):
        return _bad_request()
    q = request.query_params
    notices = [_DONE_NOTICES.get(q.get("done", ""), ""), _LOG_NOTICES.get(q.get("log", ""), ""),
               _EX_NOTICES.get(q.get("ex", ""), "")]
    rec = await _approval_record(record_id)
    user_id = _v(rec, "line_user_id") if rec else ""
    if rec is not None and not _LINE_USER_ID_RE.fullmatch(user_id):
        rec = None
    rows = await _history_rows(user_id) if rec is not None else []
    return HTMLResponse(_page(record_id, rec, rows, uuid.uuid4().hex, notices))


async def reply_send(request: Request):
    """明示送信（PRG: 303 で本画面へ戻す）。入力は閉集合・固定書式のみ（不正は固定 400）。"""
    record_id = str(request.path_params.get("record_id", ""))
    form = await request.form()
    op_id = str(form.get("op_id", ""))
    text = str(form.get("text", "")).strip()
    confirm = str(form.get("confirm", "")) == "1"
    want_exemplar = str(form.get("exemplar", "")) == "1"
    if not _RECORD_ID_RE.fullmatch(record_id) or not _OP_ID_RE.fullmatch(op_id):
        return _bad_request()
    back = f"{PAGE}/{record_id}"
    if not text or len(text) > MAX_TEXT_CHARS:
        return RedirectResponse(f"{back}?done=bad_input", status_code=303)
    if not confirm:
        return RedirectResponse(f"{back}?done=confirm_required", status_code=303)
    # 同じ操作 ID の再送信は副作用なしで弾く（二重クリック・戻る→再送）
    try:
        state = await send_ledger.operation_state(op_id)
    except Exception:
        state = None
    if state == send_ledger.STATE_SENT:
        return RedirectResponse(f"{back}?done=duplicate_sent", status_code=303)
    if state is not None:
        return RedirectResponse(f"{back}?done=duplicate_unconfirmed", status_code=303)
    rec = await _approval_record(record_id)
    user_id = _v(rec, "line_user_id") if rec else ""
    if rec is None or not _LINE_USER_ID_RE.fullmatch(user_id):
        return RedirectResponse(f"{back}?done=not_found", status_code=303)
    decision = send_ledger.human_explicit_decision(confirm)
    if not decision.allow:
        return RedirectResponse(f"{back}?done=blocked", status_code=303)
    try:
        outcome = await line_channel.push_text_explicit(line_channel.JIKOU_CHANNEL, op_id, user_id, text)
    except Exception:
        logger.warning("[REPLY] explicit send unconfirmed (exception) record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))
        return RedirectResponse(f"{back}?done=unconfirmed", status_code=303)
    if outcome != line_channel.PUSH_SENT:
        return RedirectResponse(f"{back}?done={outcome}", status_code=303)
    logger.info("[REPLY] explicit send ok record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    log_res = await write_human_chatlog(user_id, text)
    reflect_memory_history(user_id, text)
    ex_res = "off"
    if want_exemplar:
        ex_res = await _save_exemplar(user_id, _v(rec, "顧客メッセージ"), text, op_id,
                                      _v(rec, "顧客名"))
    return RedirectResponse(f"{back}?done=sent&log={log_res}&ex={ex_res}", status_code=303)


router.add_api_route(f"{PAGE}/{{record_id}}", _gate(reply_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{record_id}}/send", _gate(reply_send), methods=["POST"])
