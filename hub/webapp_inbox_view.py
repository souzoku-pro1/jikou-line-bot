"""webapp_inbox_view — UX-PENDING-1 (2)(4)(5): 対応待ちの統合一覧（/app/inbox）・署名付きリンクの着地（/app/go/r|c）

設計: docs/ux_pending_1_design.md §2（画面 B）・§4（画面 C）・§7（リンク）・§9（記録）。

- 一覧（GET /app/inbox）: 対応待ち（reply_pending の open）と、承認キュー（App 29・送信済み=no）のうち対応待ちの表が
  参照していない件（V2 以前の項目＝「旧」の札）を **1 つの一覧** に古い順で出す。各件のボタンは 3 つだけ:
    [返信する]（/app/reply/{No.} への移動のみ）／[対応終了]（別経路で返信済み・bot 再開）／[不要]。
  `?closed=1` で閉じた件（参照＋[未処理に戻す]）。本文は承認キューの顧客メッセージを出す（内部専用画面・承認画面と同じ裁定）。
  LINE userId・line_user_hash は画面に出さない（RV-10）。
- 操作（POST /app/inbox/close・/app/inbox/reopen）: 入力は閉集合（kind ∈ {pending, old}・id は数字・outcome ∈ 2 値）。不正は固定 400。
  対応待ち → reply_pending（closed_external / not_needed / open に戻す・履歴あり）。承認キューだけの件 → hub/approval_close（追記のみ・
  kintone は書かない）。「対応終了」は同時に send_ledger.end_attending_by_hash（bot 再開）。
- 署名付きリンク（GET /app/go/r/{符号}・GET/POST /app/go/c/{符号}）: hub/pending_link で検証。
    r: ok → 303 /app/reply/{No.}（承認キューが無い件は一覧へ）／expired・invalid → 一覧へ（固定の案内 1 行・大野裁定: 失効はエラー画面で終わらせない）
    c: ok → 確認画面 1 枚（GET は副作用なし）→ POST で対応終了（冪等: 既に閉じていれば処理せず案内だけ）
- 「解除の要確認」（POST /app/inbox/release・fix2 BU-08）: 対応終了にしたとき bot の対応中（会話・世代）を特定できなかった件を
  一覧の上部に出し、現在の状態（対応中／対応中でない／会話なし／特定不能）を表示する。大野が [対応中を解除する] を押したときだけ
  その時点の世代を固定して解除する（hub/attending_release.confirm_release）。回収ジョブは固定済みの世代にしか触らない。
- 認証はすべて _gate（未ログインは /app/login?next=… → ログイン後にここへ戻る）。送信の口は無い（承認なし送信経路ゼロ）。
- 自動送信・kintone 書込・物理削除は無い。ログは固定語彙のみ。
"""

import datetime
import html
import logging
import re

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import approval_close, attending_release, kintone, pending_link, reply_pending, send_ledger
from hub.webapp_auth import _gate

logger = logging.getLogger("hub.webapp_inbox_view")

router = APIRouter()

PAGE = "/app/inbox"
GO = "/app/go"
REPLY = "/app/reply"
BUSINESS = "jikou"
APP_APPROVAL = kintone.KintoneApp("App 29 (承認キュー)", "APP_APPROVAL", "TOKEN_APPROVAL")
_FIELDS = ["$id", "顧客名", "顧客メッセージ", "カテゴリ", "送信済み", "更新日時", "line_user_id"]
OLD_LIMIT = 50                  # 一覧に出す「旧」項目の上限（除外後の件数）
OLD_PAGE = 50                   # fix1 BU-05: App 29 を 1 ページ何件ずつ読むか
OLD_MAX_PAGES = 10              # fix1 BU-05: 続きを読む上限（50×10＝500 件まで＝1 回の走査）
OLD_SCAN = OLD_PAGE * OLD_MAX_PAGES   # fix2 BU-09: 1 回の走査件数。上限に達したら「続きを読み込む」で次の 500 件
EXCLUDE_QUERY_MAX = 100         # fix2 BU-09: 除外 ID（対応待ちが参照する No.・閉じ済み No.）がこの件数以下なら kintone の問い合わせ条件に入れる
# fix3 BU-10: 続きは offset ではなく **安定キー**（前回走査した最後のレコード No.＝last_id・問い合わせは $id < last_id・$id 降順）。
# 除外集合（閉じ済み・対応待ちの参照）が別タブの操作で途中で変わっても、offset のずれによる欠落が起きない。途中で増えた新規項目は
# $id が大きいので続きには現れず（重複しない）、先頭（次の最初の走査）に出る（欠落しない）。old_from（offset）は廃止。
LAST_ID_PARAM = "last_id"
CLOSED_LIMIT = 50

KIND_PENDING = "pending"
KIND_OLD = "old"
KINDS = (KIND_PENDING, KIND_OLD)
OUTCOME_DONE = "done_external"
OUTCOME_NOT_NEEDED = "not_needed"
OUTCOMES = (OUTCOME_DONE, OUTCOME_NOT_NEEDED)
ACTOR_INBOX = "human:inbox"
ACTOR_LINK = "human:link"

_ID_RE = re.compile(r"^[0-9]{1,12}$")
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
_JST = datetime.timezone(datetime.timedelta(hours=9))

_ROUTE_LABELS = {"customer": "顧客対応", "hearing": "ヒアリング", "inbound": "受信入口で停止"}
_REASON_LABELS = {"demoted": "bot が判断できず下書きだけ作成", "empty_reply": "bot の返事が空だった",
                  "policy_stopped": "許可表で bot 停止", "claude_outage": "AI の障害・例外"}
_STATE_LABELS = {"open": "未処理", "human_replied": "返信済み（画面から）", "auto_replied": "bot が返信",
                 "not_needed": "不要", "closed_external": "対応終了（別の方法で返信済み）"}
_OLD_OUTCOME_LABELS = {"done_external": "対応終了（別の方法で返信済み）", "not_needed": "不要"}
_NOTICES = {
    "closed": "対応終了にしました。", "not_needed": "不要にしました。", "reopened": "未処理に戻しました。",
    "closed_release_pending": "対応終了にしました。bot の再開（対応中の解除）は未完了のため、回収ジョブが自動で再試行します。",
    "closed_release_confirm": "対応終了にしました。bot の「対応中」を特定できなかったため、この一覧の上部「解除の要確認」で現在の状態を確認し、必要なら解除してください。",
    "released": "対応中を解除しました（bot を再開）。",
    "release_not_needed": "対応中ではなかったため、解除不要として確認済みにしました。",
    "release_pending": "解除を試みましたが完了していません。回収ジョブが、いま固定した世代に対してだけ再試行します。",
    "release_unresolved": "相談者を特定できないため解除できません（承認キューの line_user_id を確認してください）。",
    "already": "この件は既に処理済みです。", "not_found": "対象が見つかりません。", "bad_input": "入力が不正です。",
    "db": "記録を読み書きできませんでした。", "not_closed": "この件は閉じていません。",
}
_CONFIRM_STATUS = {   # fix2 BU-08: 押す前に出す現在の状態（ハッシュ・userId・本文は出さない）
    "attending": "bot 停止中（対応中）。解除すると bot が再開します。",
    "off": "対応中ではありません（解除は不要です）。",
    "none": "会話の記録がありません（解除は不要です）。",
    "unresolved": "相談者を特定できません（承認キューの line_user_id を確認してください）。",
}
_CONFIRM_BUTTON = {"attending": "対応中を解除する", "off": "解除不要として確認済みにする", "none": "解除不要として確認済みにする"}
_LINK_NOTICES = {
    "expired": "リンクの期限が切れています。下の一覧から該当の件を開いてください。",
    "invalid": "リンクが無効です（期限切れか、改ざんの疑い）。下の一覧から該当の件を開いてください。",
    "noreply": "この件には承認キューが無いため返信画面を開けません。LINE アプリで返信し、下の一覧で「対応終了」を押してください。",
}

_CSS = ("body{font-family:sans-serif;max-width:720px;margin:0 auto;padding:.6rem 1rem 2rem;box-sizing:border-box;"
        "background:#eef1f5;color:#222}h1{font-size:1.25rem;margin:.4rem 0 .1rem}.sub{color:#667;font-size:.85rem;margin:0 0 .8rem}"
        "a{color:#1a3c6e}.item{background:#fff;border:1px solid #d8dde5;border-radius:14px;padding:.75rem 1rem;margin:.6rem 0}"
        ".name{font-size:1.05rem;font-weight:bold}.meta{color:#8a8f98;font-size:.78rem;margin-top:.3rem}"
        ".badge{display:inline-block;border-radius:999px;padding:.12rem .6rem;font-size:.78rem;background:#e3e7ee;color:#445;margin:.15rem .35rem .15rem 0}"
        ".badge.att{background:#fde2e2;color:#8a1f1f}.badge.warn{background:#fff3d6;color:#8a6d00}.badge.info{background:#e8f0fb;color:#1a5cb0}"
        ".badge.old{background:#eee;color:#555;border:1px solid #ccc}"
        "pre{white-space:pre-wrap;word-break:break-word;font-family:inherit;font-size:.93rem;border-radius:12px;padding:.55rem .8rem;"
        "margin:.4rem 0;background:#f2f4f7;border:1px solid #e2e6ec}.lbl{color:#667;font-size:.78rem;margin-top:.4rem}"
        ".actions{display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.6rem}.actions form{margin:0}"
        "button,.btn{font-size:.95rem;padding:.55rem 1rem;border-radius:10px;border:1px solid #c8cdd5;background:#fff;color:#222;"
        "min-height:44px;text-decoration:none;display:inline-flex;align-items:center}"
        ".btn.primary,button.primary{background:#1a3c6e;color:#fff;border-color:#1a3c6e;font-weight:bold}"
        ".empty{color:#667;text-align:center;margin:1.2rem 0}.notice{background:#e8f0fb;border-radius:10px;padding:.5rem .8rem;color:#1a5cb0;margin:.4rem 0}"
        ".hint{color:#667;font-size:.85rem}")


def _bad_request() -> Response:
    return Response(status_code=400)


def _v(rec: dict, code: str) -> str:
    return str(((rec or {}).get(code) or {}).get("value") or "")


def _parse_dt(s: str) -> datetime.datetime | None:
    s = str(s or "").strip()
    if not s:
        return None
    try:
        d = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetime.timezone.utc)
    return d


def fmt_jst(s: str, now: datetime.datetime | None = None) -> str:
    """JST の「今日 16:58」／「11/2 16:58」（本文・ID を含まない）。"""
    d = _parse_dt(s)
    if d is None:
        return ""
    t = d.astimezone(_JST)
    n = (now or datetime.datetime.now(datetime.timezone.utc)).astimezone(_JST)
    hm = f"{t.hour:02d}:{t.minute:02d}"
    if (t.year, t.month, t.day) == (n.year, n.month, n.day):
        return f"今日 {hm}"
    return f"{t.month}/{t.day} {hm}"


# ── データの組み立て ────────────────────────────────────────────────────────
async def _records_by_ids(ids: list[str]) -> dict:
    """承認キューをレコード ID で取り寄せる（固定書式の ID のみ・取得失敗は空）。"""
    ids = [i for i in ids if _ID_RE.fullmatch(str(i))]
    if not ids or not (APP_APPROVAL.app_id() and APP_APPROVAL.token()):
        return {}
    out: dict = {}
    for i in range(0, len(ids), 50):
        chunk = ids[i:i + 50]
        q = "$id in (" + ",".join(f'"{x}"' for x in chunk) + f") limit {len(chunk)}"
        try:
            rows = await kintone.search_records(APP_APPROVAL, q, fields=_FIELDS)
        except Exception:
            logger.warning("[INBOX] approval lookup failed (fixed reason)")
            continue
        for r in rows:
            out[_v(r, "$id")] = r
    return out


def _old_query(exclude: set, limit: int, last_id: str | None = None) -> str:
    """fix2 BU-09: 除外 ID が EXCLUDE_QUERY_MAX 件以下なら `$id not in (...)` を問い合わせ条件に入れる（多ければ走査で除外）。
    fix3 BU-10: 並びは $id 降順・続きは `$id < last_id`（安定キー・offset は使わない）。"""
    ids = sorted((x for x in exclude if _ID_RE.fullmatch(str(x))), key=int)
    cond = '送信済み in ("no")'
    if last_id:
        cond += f" and $id < {int(last_id)}"
    if 0 < len(ids) <= EXCLUDE_QUERY_MAX:
        cond += " and $id not in (" + ",".join(f'"{x}"' for x in ids) + ")"
    return f"{cond} order by $id desc limit {limit}"


async def _scan_old(exclude: set | None = None, last_id: str | None = None) -> tuple[list[dict], str, bool, int]:
    """承認キューの未送信（送信済み=no）をレコード No. の大きい順に走査する（V2 以前の項目の候補）。
    戻り値 (records, next_last_id, has_more, scanned)。next_last_id＝この走査で読んだ最後のレコード No.（除外した行も含む・
    続きは `$id < next_last_id`）。scanned＝この走査で読んだ件数。
    fix1 BU-05: exclude（対応待ちが参照する No.・閉じ済みの No.）を除いた後の件数が OLD_LIMIT に達するまで続きのページを読む。
    fix2 BU-09: 1 回の走査は OLD_SCAN 件まで。上限に達して続きがあり得るときは has_more=True（未処理 0 件と区別）。
    fix3 BU-10: 続きは安定キー。除外集合が途中で変わっても（別タブで閉じた等）走査済みの No. より前だけを読むので欠落しない。
    途中で増えた新規項目（No. が大きい）は続きには出ず先頭の走査に出る（重複・欠落なし）。"""
    cur = str(last_id or "")
    if not (APP_APPROVAL.app_id() and APP_APPROVAL.token()):
        return [], cur, False, 0
    exclude = exclude or set()
    out: list[dict] = []
    scanned = 0
    for _ in range(OLD_MAX_PAGES):
        try:
            rows = await kintone.search_records(APP_APPROVAL, _old_query(exclude, OLD_PAGE, cur), fields=_FIELDS)
        except Exception:
            logger.warning("[INBOX] approval list failed (fixed reason)")
            return out, cur, False, scanned
        for i, rec in enumerate(rows):
            rid = _v(rec, "$id")
            if not _ID_RE.fullmatch(rid):
                continue
            if cur and int(rid) >= int(cur):
                continue                                     # 並びの保証外（安定キーより新しい行は読まない）
            cur = rid                                        # 走査した最後の No.（除外した行も進める）
            scanned += 1
            if rid not in exclude:
                out.append(rec)
                if len(out) >= OLD_LIMIT:
                    more = (i + 1 < len(rows)) or (len(rows) >= OLD_PAGE)
                    return out, cur, more, scanned
        if len(rows) < OLD_PAGE:
            return out, cur, False, scanned
    return out, cur, True, scanned


async def _old_records(exclude: set | None = None) -> list[dict]:
    """先頭からの 1 走査分（互換）。"""
    return (await _scan_old(exclude, None))[0]


async def _attending_map(hashes: set) -> dict:
    out: dict = {}
    for h in hashes:
        try:
            out[h] = await send_ledger.conversation_state_by_hash(BUSINESS, h) or {}
        except Exception:
            out[h] = {}
    return out


def _pending_item(r: dict, rec: dict | None, conv: dict) -> dict:
    rid = str(r.get("approval_record_id") or "")
    return {"kind": KIND_PENDING, "id": str(r["pending_id"]), "record_id": rid if _ID_RE.fullmatch(rid) else "",
            "name": (_v(rec, "顧客名") if rec else ""), "message": (_v(rec, "顧客メッセージ") if rec else ""),
            "category": r.get("category") or (_v(rec, "カテゴリ") if rec else ""),
            "at": r.get("created_at") or "", "state": r.get("state") or "", "route": r.get("route") or "",
            "reason": r.get("reason") or "", "customer_send": r.get("customer_send") or "",
            "attending": bool(conv.get("attending")), "old": False}


def _old_item(rec: dict, closed: dict | None = None) -> dict:
    return {"kind": KIND_OLD, "id": _v(rec, "$id"), "record_id": _v(rec, "$id"), "name": _v(rec, "顧客名"),
            "message": _v(rec, "顧客メッセージ"), "category": _v(rec, "カテゴリ"), "at": _v(rec, "更新日時"),
            "state": (closed or {}).get("outcome", "open"), "route": "", "reason": "", "customer_send": "",
            "attending": False, "old": True, "closed_at": (closed or {}).get("at", "")}


def _sort_key(item: dict):
    d = _parse_dt(item.get("at", ""))
    return d or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)


async def load_open_view(last_id: str = "") -> tuple[list[dict], dict]:
    """未処理: 対応待ち（open）＋承認キューだけの未送信（対応待ちの表に無く・閉じていない）。古い順。
    fix2 BU-09／fix3 BU-10: 戻り値 (items, scan)。scan={"last_id","next","scanned","more","found"}＝承認キューの走査範囲
    （last_id より前を scanned 件）と続きの有無（続きは next を last_id に）。"""
    rows = await reply_pending.list_open(reply_pending.LIST_LIMIT)
    attending = await _attending_map({r["line_user_hash"] for r in rows})
    recs = await _records_by_ids([str(r.get("approval_record_id") or "") for r in rows])
    items = [_pending_item(r, recs.get(str(r.get("approval_record_id") or "")), attending.get(r["line_user_hash"]) or {})
             for r in rows]
    referenced = await reply_pending.approval_record_ids_all()
    closed = await approval_close.closed_ids(BUSINESS)                 # fix2 BU-09: 閉じ済み ID の集合を先に取る
    old, nxt, more, scanned = await _scan_old(set(referenced) | set(closed), last_id or None)   # fix1 BU-05／fix3 BU-10: 安定キー
    for rec in old:
        items.append(_old_item(rec))
    items.sort(key=_sort_key)
    return items, {"last_id": str(last_id or ""), "next": str(nxt), "scanned": int(scanned), "more": bool(more), "found": len(old)}


async def load_open_items() -> list[dict]:
    return (await load_open_view(""))[0]


async def load_confirm_items() -> list[dict]:
    """fix2 BU-08: 「解除の要確認」（対象未特定のまま対応終了にした件）。押す前に現在の状態を出す。本文・userId・ハッシュは出さない。"""
    rows = await attending_release.list_needs_confirm()
    recs = await _records_by_ids([str(r.get("approval_record_id") or "") for r in rows])
    out = []
    for r in rows:
        st = await attending_release.current_status(r, resolve_hash=_hash_for_record)
        conv = st["conversation"]
        key = "unresolved" if not st["hash"] else ("none" if conv is None else ("attending" if conv["attending"] else "off"))
        rec = recs.get(str(r.get("approval_record_id") or ""))
        out.append({"release_id": str(r["release_id"]), "record_id": str(r.get("approval_record_id") or ""),
                    "kind": r["source_kind"], "id": str(r["source_id"]), "name": (_v(rec, "顧客名") if rec else ""),
                    "closed_at": r.get("created_at") or "", "status": key})
    return out


async def load_closed_items() -> list[dict]:
    """閉じた件（参照）: 対応待ちの open 以外＋承認キューの閉じ記録。新しい順。"""
    rows = await reply_pending.list_closed(CLOSED_LIMIT)
    recs = await _records_by_ids([str(r.get("approval_record_id") or "") for r in rows])
    items = [_pending_item(r, recs.get(str(r.get("approval_record_id") or "")), {}) for r in rows]
    for it, r in zip(items, rows):
        it["closed_at"] = r.get("resolved_at") or r.get("updated_at") or ""
    closed_rows = await approval_close.list_closed(BUSINESS, CLOSED_LIMIT)
    crecs = await _records_by_ids([c["approval_record_id"] for c in closed_rows])
    for c in closed_rows:
        rec = crecs.get(c["approval_record_id"]) or {"$id": {"value": c["approval_record_id"]}}
        items.append(_old_item(rec, c))
    items.sort(key=lambda it: _parse_dt(it.get("closed_at", "")) or _sort_key(it), reverse=True)
    return items


# ── 画面 ────────────────────────────────────────────────────────────────────
def _head(title: str) -> str:
    return ('<!DOCTYPE html><html lang="ja"><head><meta charset="utf-8">'
            '<meta name="viewport" content="width=device-width, initial-scale=1"><meta name="color-scheme" content="light">'
            f"<title>{html.escape(title)} - 案件管理（仮）</title><style>{_CSS}</style></head><body>")


def _badges(it: dict) -> str:
    e = html.escape
    out = []
    if it["old"]:
        out.append('<span class="badge old">旧</span>')
    if it["attending"]:
        out.append('<span class="badge att">bot 停止中（対応中）</span>')
    if it["record_id"]:
        out.append('<span class="badge warn">未送信の下書きあり</span>')
    if it["reason"]:
        out.append(f'<span class="badge">{e(_REASON_LABELS.get(it["reason"], it["reason"]))}</span>')
    if it["route"]:
        out.append(f'<span class="badge">{e(_ROUTE_LABELS.get(it["route"], it["route"]))}</span>')
    if it["customer_send"] == "j0_sent":
        out.append('<span class="badge info">初回 1 通（J-0）送信済み</span>')
    if it["category"]:
        out.append(f'<span class="badge">{e(str(it["category"]))}</span>')
    return "".join(out)


def _card(it: dict, closed_view: bool) -> str:
    e = html.escape
    parts = ['<div class="item">']
    parts.append(f'<div class="name">{e(it["name"] or "（お名前未登録）")}'
                 f'<span class="meta"> 受付 {e(fmt_jst(it["at"]))}</span></div>')
    parts.append(f'<div>{_badges(it)}</div>')
    if it["message"]:
        parts.append(f'<div class="lbl">お客様からのメッセージ</div><pre>{e(it["message"])}</pre>')
    elif not it["record_id"]:
        parts.append('<p class="hint">承認キューが無い件です。LINE アプリで受信を確認し、返信したら「対応終了」を押してください。</p>')
    ref = f"No.{e(it['record_id'])}" if it["record_id"] else f"ID {e(it['id'])}"
    if closed_view:
        label = (_OLD_OUTCOME_LABELS.get(it["state"], it["state"]) if it["old"]
                 else _STATE_LABELS.get(it["state"], it["state"]))
        parts.append(f'<div class="meta">{ref}｜{e(label)}｜{e(fmt_jst(it.get("closed_at", "")))}</div>')
        if it["old"] or it["state"] in reply_pending.CLOSED_BY_HUMAN_STATES:
            parts.append(f'<div class="actions"><form method="post" action="{PAGE}/reopen">'
                         f'<input type="hidden" name="kind" value="{it["kind"]}"><input type="hidden" name="id" value="{e(it["id"])}">'
                         '<button type="submit">未処理に戻す</button></form></div>')
        parts.append("</div>")
        return "".join(parts)
    parts.append(f'<div class="meta">{ref}</div>')
    parts.append('<div class="actions">')
    if it["record_id"]:
        parts.append(f'<a class="btn primary" href="{REPLY}/{e(it["record_id"])}">返信する</a>')
    hidden = f'<input type="hidden" name="kind" value="{it["kind"]}"><input type="hidden" name="id" value="{e(it["id"])}">'
    parts.append(f'<form method="post" action="{PAGE}/close">{hidden}'
                 f'<button type="submit" name="outcome" value="{OUTCOME_DONE}">対応終了</button></form>')
    parts.append(f'<form method="post" action="{PAGE}/close">{hidden}'
                 f'<button type="submit" name="outcome" value="{OUTCOME_NOT_NEEDED}">不要</button></form>')
    parts.append("</div></div>")
    return "".join(parts)


def _confirm_card(c: dict) -> str:
    e = html.escape
    ref = f"No.{e(c['record_id'])}" if c["record_id"] else f"ID {e(c['id'])}"
    parts = ['<div class="item">',
             f'<div class="name">{e(c["name"] or "（お名前未登録）")}<span class="meta"> 対応終了 {e(fmt_jst(c["closed_at"]))}</span></div>',
             '<div><span class="badge att">解除の要確認</span>' + ('<span class="badge old">旧</span>' if c["kind"] == KIND_OLD else "") + "</div>",
             f'<div class="meta">{ref}</div>',
             f'<div class="lbl">現在の状態</div><div>{e(_CONFIRM_STATUS.get(c["status"], _CONFIRM_STATUS["unresolved"]))}</div>']
    label = _CONFIRM_BUTTON.get(c["status"])
    if label:
        parts.append(f'<div class="actions"><form method="post" action="{PAGE}/release">'
                     f'<input type="hidden" name="release_id" value="{e(c["release_id"])}">'
                     f'<button type="submit" class="primary">{e(label)}</button></form></div>')
    parts.append("</div>")
    return "".join(parts)


def _scan_line(scan: dict) -> str:
    """fix2 BU-09: 「○件走査・続きあり」。上限到達と未処理 0 件を区別する。fix3 BU-10: 続きのリンクは安定キー（last_id）。"""
    if not scan:
        return ""
    where = f"No.{html.escape(scan['last_id'])} より前を" if scan["last_id"] else "先頭から"
    rng = f"{where} {scan['scanned']} 件走査しました（この範囲の未処理 {scan['found']} 件）"
    if scan["more"]:
        return (f'<p class="sub">承認キューの未送信を {rng}。続きがあります。'
                f'<a href="{PAGE}?{LAST_ID_PARAM}={html.escape(scan["next"])}">続きを読み込む（次の {OLD_SCAN} 件）</a></p>')
    if scan["last_id"]:
        return f'<p class="sub">承認キューの未送信を {rng}。続きはありません。<a href="{PAGE}">先頭に戻る</a></p>'
    return ""


def _page(items: list[dict], notices: list[str], closed_view: bool, confirm: list[dict] | None = None,
          scan: dict | None = None) -> str:
    e = html.escape
    parts = [_head("対応待ち"), "<h1>対応待ち</h1>",
             '<p class="sub">bot が自動返信できなかった相談と、未送信の返信下書きの一覧です。'
             '<a href="/app/approvals">承認</a>｜<a href="/app/send_ops">送信確認待ち</a></p>']
    for n in notices:
        if n:
            parts.append(f'<p class="notice">{e(n)}</p>')
    if confirm and not closed_view:
        parts.append(f'<h2 class="sub">解除の要確認（{len(confirm)} 件）</h2>'
                     '<p class="hint">対応終了にしたとき、bot の「対応中」を特定できなかった件です。現在の状態を確認して押してください。</p>')
        parts.extend(_confirm_card(c) for c in confirm)
    if closed_view:
        parts.append(f'<p class="sub">閉じた件（直近 {CLOSED_LIMIT} 件・新しい順）。<a href="{PAGE}">未処理の一覧へ戻る</a></p>')
    if not items:
        if closed_view:
            parts.append('<p class="empty">閉じた件はありません</p>')
        elif scan and scan.get("more"):
            parts.append('<p class="empty">この範囲には対応待ちがありません（続きがあります）</p>')   # fix2 BU-09: 上限到達は 0 件と区別
        else:
            parts.append('<p class="empty">対応待ちはありません</p>')
    for it in items:
        parts.append(_card(it, closed_view))
    if not closed_view:
        parts.append(_scan_line(scan or {}))
        parts.append(f'<p class="sub"><a href="{PAGE}?closed=1">閉じた件も表示する</a></p>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


def _confirm_page(it: dict, token: str) -> str:
    e = html.escape
    parts = [_head("対応終了の確認"), "<h1>対応終了にしますか？</h1>",
             '<p class="sub">この相談者に、LINE アプリなど別の方法で返信済みのときに押してください。bot を再開します。'
             "顧客への送信はしません。</p>", '<div class="item">',
             f'<div class="name">{e(it["name"] or "（お名前未登録）")}<span class="meta"> 受付 {e(fmt_jst(it["at"]))}</span></div>',
             f'<div>{_badges(it)}</div>']
    if it["message"]:
        parts.append(f'<div class="lbl">お客様からのメッセージ</div><pre>{e(it["message"])}</pre>')
    parts.append(f'<div class="actions"><form method="post" action="{GO}/{pending_link.KIND_CLOSE}/{e(token)}">'
                 '<button type="submit" class="primary">対応終了にする</button></form>'
                 f'<a class="btn" href="{PAGE}">やめる（対応待ちへ）</a></div></div>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


# ── route: 一覧 ──────────────────────────────────────────────────────────────
async def inbox_page(request: Request):
    q = request.query_params
    closed_view = q.get("closed") == "1"
    notices = [_NOTICES.get(q.get("done", ""), ""), _LINK_NOTICES.get(q.get("link", ""), "")]
    last_id = q.get(LAST_ID_PARAM, "") or ""                 # fix3 BU-10: 続きは安定キー（数字のみ・不正 400）。old_from（offset）は廃止＝無視して先頭から
    if last_id and not _ID_RE.fullmatch(last_id):
        return _bad_request()
    confirm: list[dict] = []
    scan: dict = {}
    try:
        if closed_view:
            items = await load_closed_items()
        else:
            items, scan = await load_open_view(last_id)   # fix2 BU-09／fix3 BU-10
    except Exception:
        items, notices = [], notices + [_NOTICES["db"]]
    if not closed_view:
        try:
            confirm = await load_confirm_items()      # fix2 BU-08
        except Exception:
            confirm = []
            logger.warning("[INBOX] release confirm list failed (fixed reason)")
    return HTMLResponse(_page(items, notices, closed_view, confirm, scan))


async def _hash_for_record(record_id: str) -> str | None:
    """承認キューだけの件: レコードの line_user_id をサーバー内で解決してハッシュ化（画面・URL には出さない）。引けなければ None。"""
    try:
        rec = await kintone.get_record(APP_APPROVAL, record_id)
    except Exception:
        return None
    uid = _v(rec, "line_user_id")
    if not _LINE_USER_ID_RE.fullmatch(uid):
        return None
    return send_ledger.line_user_hash(BUSINESS, uid)


async def _after_close(release_id) -> str:
    """fix1 BU-04: 閉じ記録の直後に解除を 1 回試みる。完了しなければ回収ジョブに委ねる（閉じ記録は残る）。
    fix2 BU-08: 対象を特定できなかった件（needs_confirm）は自動解除せず「解除の要確認」へ。"""
    st = await attending_release.attempt(release_id) if release_id is not None else None
    if st == attending_release.STATE_DONE:
        return "closed"
    if st == attending_release.STATE_NEEDS_CONFIRM:
        return "closed_release_confirm"
    return "closed_release_pending"


async def close_item(kind: str, item_id: str, outcome: str, actor: str) -> str:
    """閉じる（画面・署名リンク共通）。戻り値は _NOTICES の閉集合コード。
    fix1 BU-04: 「対応終了」は閉じ記録と同一 tx で「解除未完了」を置き、直後に解除を試みる。失敗は回収で再試行。
    再操作（already）でも未完了があれば再試行する。"""
    if kind == KIND_PENDING:
        pid = int(item_id)
        if outcome == OUTCOME_DONE:
            res, rid = await attending_release.close_pending(pid, actor)
            if res == "ok":
                return await _after_close(rid)
            if res == "not_open":
                await attending_release.retry_for_source(attending_release.SOURCE_PENDING, str(pid))
        else:
            res = await reply_pending.mark_not_needed(pid)
            if res == "ok":
                return "not_needed"
        return {"not_open": "already", "not_found": "not_found"}.get(res, "db")
    if outcome == OUTCOME_DONE:
        res, rid = await attending_release.close_old(BUSINESS, item_id, actor, await _hash_for_record(item_id))
        if res == "ok":
            return await _after_close(rid)
        if res == "already":
            await attending_release.retry_for_source(attending_release.SOURCE_OLD, str(item_id))
    else:
        res = await approval_close.close(BUSINESS, item_id, outcome, actor)
        if res == "ok":
            return "not_needed"
    return {"already": "already", "bad_input": "bad_input"}.get(res, "db")


async def run_release_job() -> None:
    """fix1 BU-04: 回収ジョブへの相乗り（解除未完了＝世代固定済みの再試行）。fix2 BU-08: 対象未特定の件はここでは確定・解除しない。"""
    await attending_release.run_release_job()


async def inbox_release(request: Request):
    """fix2 BU-08: [対応中を解除する]。needs_confirm の行だけが対象。その時点の世代を固定（永続化）してから解除する。"""
    form = await request.form()
    rid = str(form.get("release_id", ""))
    if not _ID_RE.fullmatch(rid):
        return _bad_request()
    try:
        code, _state = await attending_release.confirm_release(int(rid), resolve_hash=_hash_for_record)
    except Exception:
        logger.warning("[INBOX] release confirm failed (fixed reason)")
        code = "db"
    done = {attending_release.RESULT_RELEASED: "released", attending_release.RESULT_ALREADY_OFF: "release_not_needed",
            attending_release.RESULT_NO_CONVERSATION: "release_not_needed", attending_release.RESULT_STALE: "release_not_needed",
            attending_release.CONFIRM_FIXED_FAILED: "release_pending", attending_release.CONFIRM_UNRESOLVED: "release_unresolved",
            attending_release.CONFIRM_NOT_TARGET: "already"}.get(code, "db")
    return RedirectResponse(f"{PAGE}?done={done}", status_code=303)


async def inbox_close(request: Request):
    form = await request.form()
    kind, item_id, outcome = str(form.get("kind", "")), str(form.get("id", "")), str(form.get("outcome", ""))
    if kind not in KINDS or not _ID_RE.fullmatch(item_id) or outcome not in OUTCOMES:
        return _bad_request()
    code = await close_item(kind, item_id, outcome, ACTOR_INBOX)
    return RedirectResponse(f"{PAGE}?done={code}", status_code=303)


async def inbox_reopen(request: Request):
    form = await request.form()
    kind, item_id = str(form.get("kind", "")), str(form.get("id", ""))
    if kind not in KINDS or not _ID_RE.fullmatch(item_id):
        return _bad_request()
    if kind == KIND_PENDING:
        res = await reply_pending.reopen(int(item_id), actor=ACTOR_INBOX)
        code = {"ok": "reopened", "not_closed": "not_closed", "not_found": "not_found"}.get(res, "db")
    else:
        res = await approval_close.reopen(BUSINESS, item_id, ACTOR_INBOX)
        code = {"ok": "reopened", "not_closed": "not_closed"}.get(res, "db")
    return RedirectResponse(f"{PAGE}?closed=1&done={code}", status_code=303)


# ── route: 署名付きリンク ──────────────────────────────────────────────────────
def _to_inbox(link: str) -> RedirectResponse:
    return RedirectResponse(f"{PAGE}?link={link}", status_code=303)


async def _verified(request: Request, kind: str):
    """(status, pending_id)。種別違いは invalid。"""
    status, k, pid = pending_link.verify(str(request.path_params.get("token", "")))
    if status == pending_link.STATUS_OK and k != kind:
        return pending_link.STATUS_INVALID, None
    return status, pid


async def go_reply(request: Request):
    status, pid = await _verified(request, pending_link.KIND_REPLY)
    if status != pending_link.STATUS_OK:
        logger.info("[INBOX] link rejected (fixed reason)")
        return _to_inbox(status)
    try:
        row = await reply_pending.get_pending(pid)
    except Exception:
        return RedirectResponse(f"{PAGE}?done=db", status_code=303)
    rid = str((row or {}).get("approval_record_id") or "")
    if not row:
        return RedirectResponse(f"{PAGE}?done=not_found", status_code=303)
    if not _ID_RE.fullmatch(rid):
        return _to_inbox("noreply")
    return RedirectResponse(f"{REPLY}/{rid}", status_code=303)


async def go_close_confirm(request: Request):
    status, pid = await _verified(request, pending_link.KIND_CLOSE)
    if status != pending_link.STATUS_OK:
        logger.info("[INBOX] link rejected (fixed reason)")
        return _to_inbox(status)
    try:
        row = await reply_pending.get_pending(pid)
    except Exception:
        return RedirectResponse(f"{PAGE}?done=db", status_code=303)
    if not row:
        return RedirectResponse(f"{PAGE}?done=not_found", status_code=303)
    if row["state"] != reply_pending.STATE_OPEN:
        return RedirectResponse(f"{PAGE}?done=already", status_code=303)
    rid = str(row.get("approval_record_id") or "")
    recs = await _records_by_ids([rid]) if _ID_RE.fullmatch(rid) else {}
    conv = (await _attending_map({row["line_user_hash"]})).get(row["line_user_hash"]) or {}
    it = _pending_item(row, recs.get(rid), conv)
    return HTMLResponse(_confirm_page(it, str(request.path_params.get("token", ""))))


async def go_close(request: Request):
    status, pid = await _verified(request, pending_link.KIND_CLOSE)
    if status != pending_link.STATUS_OK:
        return _to_inbox(status)
    code = await close_item(KIND_PENDING, str(pid), OUTCOME_DONE, ACTOR_LINK)
    return RedirectResponse(f"{PAGE}?done={code}", status_code=303)


router.add_api_route(PAGE, _gate(inbox_page), methods=["GET"])
router.add_api_route(f"{PAGE}/close", _gate(inbox_close), methods=["POST"])
router.add_api_route(f"{PAGE}/reopen", _gate(inbox_reopen), methods=["POST"])
router.add_api_route(f"{PAGE}/release", _gate(inbox_release), methods=["POST"])     # fix2 BU-08
router.add_api_route(f"{GO}/{pending_link.KIND_REPLY}/{{token}}", _gate(go_reply), methods=["GET"])
router.add_api_route(f"{GO}/{pending_link.KIND_CLOSE}/{{token}}", _gate(go_close_confirm), methods=["GET"])
router.add_api_route(f"{GO}/{pending_link.KIND_CLOSE}/{{token}}", _gate(go_close), methods=["POST"])
# fix1 BU-04: 回収ジョブへの相乗り（冪等登録）
send_ledger.register_recover_hook(run_release_job)
