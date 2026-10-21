"""webapp_reply_view — JIKOU-REPLY-Q1b: 大野の返信欄（PWA・明示送信）

正本: 時効LINEボット_返信規則_v1.4.md §7（大野の返信の扱い）・§10-1（返信欄・明示送信・
操作 ID・送信確認待ち・送信着手と同じ排他で対応中にする）・§10-2（許可表の「大野の明示送信」列）・
§10-7（手本）・§12 Q1b。QUEUE-2026-10-11-B Q-3 (a)(b)(d)(f)。
fix1（Codex R-JIKOU-REPLY-Q1b-1 BR-01〜09・司令塔裁定: 人の明示送信は台帳必須＝fail-closed）。

- 入口: 承認画面（/app/approvals）の各件に「この相談者に返信する」リンク。承認画面は参照専用の
  閉集合 pin（button 2・input 1・form なし・fetch は参照 API のみ）を維持するため、返信欄は本画面
  （/app/reply/{App 29 record_id}・サーバ描画・native form POST・PRG 303）に置く
  （/app/send_ops と同型）。URL・画面に LINE userId を載せない（宛先は App 29 のレコード ID で
  指定し、サーバ内で line_user_id を解決する＝RV-10）。
- 認証は P4-001 の関所（_gate）＝承認 webhook と同じく「大野のみ」の前提。未認証は 303 → /app/login。
- BR-03 宛先の固定: GET で表示した宛先（App 29 No.・line_user_id のハッシュ・App 29 revision）と
  操作 ID を **署名付き hidden（ticket）** に埋め、POST で署名と束縛を再検証し、App 29 を再取得して
  ハッシュ・revision が一致するときだけ送る（不一致＝「相談者情報が更新されました。画面を開き直して
  ください」・送らない）。本文のハッシュは着手時に台帳へ固定し（text_version）、finish で照合する。
- BR-01 台帳必須: send_ledger.begin() が None（DB 未設定・記録失敗）なら送らず固定文言
  「台帳が使えないため送信できません」（fail-closed。Q1a の bot 経路の fail-open は不変）。
  重複検査（operation_state）が例外のときも同じ（送らない）。
- BR-02 finish の戻り値: applied のときだけ成功処理（App 28 記録・手本保存・「送信しました」）。
  skipped（台帳の確定失敗）は「送信確認待ち」として表示、late（遅着）は現在の台帳状態を照合して表示。
  いずれも自動再送なし・App 28 記録・手本保存はしない。
- BR-05 対応中: 送信着手と set_attending（attending=human・human_version+1）は send_ledger.begin()
  の **同一トランザクション・会話単位の排他内** で確定（human_reply 文脈の attend=True）。
- BR-06 確認文: 「全体停止・停止リスト・人対応中の状態は確認していません。これを承知のうえ送信する」
  （チェック必須・無ければ送らない）。許可表 (f) は human 列のみ（send_ledger.human_explicit_decision）。
- BR-08 App 28 記録の回収: 送信成功後の App 28 書込が failed / skipped（role 選択肢なし）なら
  hub/reply_followup に記録未了（op_id 単位）を置き、SEND_LEDGER_RECOVER（5 分ごと）に相乗りした
  回収で冪等に再試行する。同じ操作 ID の再 POST は「送信済み・記録のみ再試行」に分岐。
- App 28 (b): 送信成功後に role=human・category=human_reply・auto_sent=no で 1 行書く。App 28 の
  role に human の選択肢が無ければ **失敗にせず件数ログで skip**（選択肢追加＝大野の kintone 操作
  の後、回収ジョブが記録する）。時刻は kintone の作成日時。
- 手本 (d)・BR-04/BR-09: 画面のチェック（既定 ON）で hub/reply_exemplar に匿名化して保存（共通検査器
  hub/pii_scan・残留検査に落ちたら保存しない＝件数ログのみ）。段階は App 21 の status
  （classify_routing）・業者名は App 21 の問い合わせ業者名。source_text_version は台帳の text_version。
- BR-07 メモリ履歴: ヒアリング経路のプロセス内履歴（main.conversation_histories）に当該相談者の
  履歴があれば assistant（本文先頭に [事務所からの返信] マーカー）として反映。
- 順序（副作用）: [入力検査 → 明示確認 → ticket 検証 → 重複検査 → App 29 再取得と宛先照合 → 許可表 →
  LINE 送信（台帳 begin/finish） → App 28 記録（失敗は記録未了へ） → メモリ反映 → 手本保存]。
- RV-10: ログは固定語彙＋emit のみ。本文・氏名・LINE userId はログに出さない。
fix2（Codex R-Q1b-fix1 BR-10〜12・docs/jikou_reply_q1b_contract.md）:
- BR-12: 送信の **前** に回収表（hub/reply_followup）へ prepared（本文・本文ハッシュ・宛先ハッシュ・op_id）を永続化し、送信後に
  状態を進める。prepared を置けない（DB 不可）ときは台帳必須と同じく **送らない**。回収ジョブは台帳の状態と突合する。
- BR-11: App 28 の本文欄にマーカー `[op:{op_id}]` を含めて書く（表示・モデル入力では除去）。回収は所有権（in_progress）を取り、
  作成前に op_id で App 28 を検索する（成功応答喪失の修復）。
- BR-10: 手本は pending_review で保存（人が匿名化を確認して承認するまで下書き参照に使わない）。
- 冪等性は **操作（op_id）単位**（同一 inbound_event_id に対する人の別操作は別送信・司令塔裁定）。
fix4（Codex R-Q1b-fix2 BR-13〜15）:
- BR-13: find_human_chatlog は 4 値 (found|absent|unavailable|unconfigured, record_id)。検索失敗は unavailable（「不存在」ではない）
  → process_one は作成せず pending に残す（業務 LINE 通知は回収ジョブが 1 日 1 回）。マーカーは kintone の単語検索仕様に合わせ
  `op_{op_id}`（記号 `[ ] : %` はキーワードから除かれる・`_` は単語の一部）。like は 1 単語の等価一致になるが、後段で本文末尾の
  等価照合（reply_followup.marker_matches）まで行う。
- BR-14: write_human_chatlog は作成呼出の **直前** に confirm_owner()（所有権の再確認・同一 tx）を呼び、False なら作成しない
  （not_owner）。作成呼出後の例外は unknown（作成されたか不明＝回収側は照合で見つかるまで再作成しない）。作成呼出前の失敗だけ failed。
- BR-15: prepare() が rejected（同 op_id の既存行が終端・送信済み・本文／宛先不一致・台帳行あり）なら送らず「新しい操作でやり直し」。
  台帳 begin 失敗（ledger_unavailable）では回収行を閉じず prepared のまま残す（本文保持）＝同一 op_id・同一本文の再 POST で再開できる。
"""

import hmac
import html
import logging
import re
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from hub import kintone, line_channel, reply_exemplar, reply_followup, send_ledger
from hub.redact import emit
from hub.webapp_auth import _gate, _sign

logger = logging.getLogger("hub.webapp_reply_view")

router = APIRouter()

PAGE = "/app/reply"
BUSINESS = "jikou"
APP_APPROVAL = kintone.KintoneApp("App 29 (承認キュー)", "APP_APPROVAL", "TOKEN_APPROVAL")
APP_CHATLOG = kintone.KintoneApp("App 28 (チャットログ)", "APP_CHATLOG", "TOKEN_CHATLOG")
ROLE_HUMAN = "human"
CATEGORY_HUMAN_REPLY = "human_reply"
HISTORY_LIMIT = 20
LOOKUP_LIMIT = 10                     # fix4 BR-13: op_id 照合で取り寄せる最大件数（後段で本文末尾を等価照合）
MAX_TEXT_CHARS = 2000
_RECORD_ID_RE = re.compile(r"^[0-9]{1,12}$")
_OP_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-f]{32}$")
_REVISION_RE = re.compile(r"^[0-9]{1,12}$")
_TICKET_RE = re.compile(r"^[0-9]{1,12}\.[0-9a-f]{32}\.[0-9]{1,12}\.[0-9a-f]{32}\.[0-9a-f]{64}$")
_ROLE_LABELS = {"user": "相談者", "assistant": "bot", ROLE_HUMAN: "事務所（大野）"}
CONFIRM_LABEL = "全体停止・停止リスト・人対応中の状態は確認していません。これを承知のうえ送信する"

# 送信結果（done）・App 28 記録（log）・手本（ex）・台帳状態（state）の閉集合（画面文言は固定・入力値を反射しない）
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
    # fix1
    "ledger_unavailable": "台帳が使えないため送信できません。時間をおいて再試行してください。",
    "stale": "相談者情報が更新されました。画面を開き直してください。送信していません。",
    "record_unconfirmed": "送信結果を台帳に確定できませんでした。送信確認待ちで結果を確定してください"
                          "（自動再送はしません。チャットログ記録・手本保存は行っていません）。",
    "late": "この操作の結果は台帳で遅着と判定されました（別の処理が先に確定しています）。"
            "チャットログ記録・手本保存は行っていません。現在の台帳状態: ",
    # fix4 BR-15
    "op_rejected": "この操作 ID は再利用できません（送信済み・終了済み、または本文・宛先が前回と異なります）。"
                   "画面を開き直して新しい操作でやり直してください。送信していません。",
}
_STATE_LABELS = {"sent": "送信済み", "unconfirmed": "送信確認待ち", "failed": "失敗",
                 "started": "着手済（確認中）", "pending": "着手前", "none": "不明"}
_LOG_NOTICES = {
    "written": "チャットログ（App 28）に事務所の返信として記録しました。",
    "skipped": "チャットログの role に「human」の選択肢が無いため記録を省略しました"
               "（kintone 側で選択肢を追加すると回収ジョブが記録します）。",
    "failed": "チャットログへの記録に失敗しました（送信は完了しています。回収ジョブが再試行します）。",
    "unconfigured": "チャットログは未設定のため記録していません（設定後に回収ジョブが記録します）。",
    # fix1 BR-08
    "pending_retry": "チャットログへの記録は未了です。回収ジョブ（5 分ごと）が再試行します。",
    "not_pending": "チャットログの記録は既に完了しています。",
    "destination_mismatch": "宛先の照合に失敗したためチャットログの記録を保留しています。",
    "exhausted": "チャットログの記録の再試行回数が上限に達しました。手動で記録してください。",
    "not_found": "",
    # fix2 BR-11／BR-12
    "recovered": "チャットログに既に同じ操作の記録があったため、新たに作成せず完了にしました。",
    "waiting_ledger": "送信結果が台帳で確定するまでチャットログの記録を保留しています（送信確認待ちで確定してください）。",
    "closed": "この操作は未送信として閉じました（チャットログには記録しません）。",
    # fix4 BR-13／BR-14
    "lookup_unavailable": "チャットログの既存記録の照合ができなかったため、記録を保留しました"
                          "（二重記録を避けるため作成していません。回収ジョブが再試行します）。",
    "unknown": "チャットログへの記録結果が不明です（記録された可能性があります）。二重記録を避けるため自動では再作成せず、"
               "回収ジョブが既存記録の照合を続けます。",
}
# fix3: 主経路の process_one の結果 → 画面の通知語彙（それ以外＝回収ジョブ側が所有している等は「回収ジョブが再試行」）
_MAIN_PATH_LOG = {
    reply_followup.RESULT_WRITTEN: "written",
    reply_followup.RESULT_RECOVERED: "recovered",
    reply_followup.RESULT_UNCONFIGURED: "unconfigured",
    reply_followup.RESULT_FAILED: "pending_retry",
    reply_followup.RESULT_SKIPPED: "pending_retry",
    reply_followup.RESULT_NOT_PENDING: "pending_retry",
    reply_followup.RESULT_LOOKUP_UNAVAILABLE: "lookup_unavailable",
    reply_followup.RESULT_UNKNOWN: "unknown",
}
_EX_NOTICES = {
    "saved": "手本として匿名化して保存しました（確認待ち。承認するまで下書きの参考には使われません）。",
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


def _revision(rec: dict | None) -> str:
    rev = _v(rec, "$revision") if rec else ""
    return rev if _REVISION_RE.fullmatch(rev) else "0"


def _user_hash(user_id: str) -> str:
    return send_ledger.line_user_hash(BUSINESS, user_id)[:32]


async def _approval_record(record_id: str) -> dict | None:
    try:
        return await kintone.get_record(APP_APPROVAL, record_id)
    except Exception:
        return None


async def _resolve_user_id(record_id: str) -> str | None:
    """App 29 のレコード ID から宛先を再解決（回収ジョブ・記録のみ再試行で使う）。"""
    rec = await _approval_record(str(record_id))
    user_id = _v(rec, "line_user_id") if rec else ""
    return user_id if _LINE_USER_ID_RE.fullmatch(user_id) else None


# ── BR-03: 署名付き ticket（宛先・revision・操作 ID の束縛） ──────────────────────
def make_ticket(record_id: str, user_id: str, revision: str, op_id: str) -> str | None:
    payload = f"{record_id}.{_user_hash(user_id)}.{revision}.{op_id}"
    sig = _sign(payload)
    return f"{payload}.{sig}" if sig else None


def parse_ticket(ticket: str, record_id: str, op_id: str) -> tuple[str, str] | None:
    """署名と束縛（record_id・op_id）を検証し (user_hash, revision) を返す。不正は None。"""
    if not _TICKET_RE.fullmatch(ticket or ""):
        return None
    rid, uhash, rev, oid, sig = ticket.split(".")
    expected = _sign(f"{rid}.{uhash}.{rev}.{oid}")
    if expected is None or not hmac.compare_digest(sig, expected):
        return None
    if rid != record_id or oid != op_id:
        return None
    return uhash, rev


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
        out.append({"role": _v(r, "role"), "message": reply_followup.strip_op_marker(msg), "at": _v(r, "作成日時")})
    return out


# ── App 28 への human 行（b） ────────────────────────────────────────────────────
async def write_human_chatlog(user_id: str, text: str, confirm_owner=None) -> str:
    """送信成功後の App 28 記録。戻り値は閉集合: written / skipped（role に human の選択肢なし・件数ログ）/
    failed（**作成呼出前**の失敗・固定語彙ログ）/ unconfigured / not_owner（fix4 BR-14: 作成直前の所有権再確認に失敗・作成していない）/
    unknown（fix4 BR-14: 作成呼出後に応答喪失＝作成されたか不明。回収側は照合で見つかるまで再作成しない）。"""
    if not (APP_CHATLOG.app_id() and APP_CHATLOG.token()):
        return "unconfigured"
    try:
        props = await kintone.get_form_fields(APP_CHATLOG)
        options = (props.get("role") or {}).get("options") or {}
    except Exception:
        logger.warning("[REPLY] chatlog human row write failed (fixed reason)")
        return "failed"
    if ROLE_HUMAN not in options:
        _skip_count["role_option_missing"] += 1
        logger.info("[REPLY] chatlog human row skipped (role option missing) total=%s",
                    emit(_skip_count["role_option_missing"], "count", "log", "operator"))
        return "skipped"
    # fix4 BR-14: 外部作成の直前に「自分がまだ owner か」を同一 tx で再確認（失われていれば作成しない）
    if confirm_owner is not None and not await confirm_owner():
        logger.info("[REPLY] chatlog human row not created (ownership lost before create)")
        return reply_followup.WRITE_NOT_OWNER
    try:
        await kintone.create_record(APP_CHATLOG, {
            "line_user_id": user_id, "role": ROLE_HUMAN, "message": text,
            "category": CATEGORY_HUMAN_REPLY, "auto_sent": "no"})
    except Exception:
        logger.warning("[REPLY] chatlog human row create result unknown (fixed reason)")
        return reply_followup.RESULT_UNKNOWN
    return "written"


async def find_human_chatlog(user_id: str, op_id: str) -> tuple[str, str | None]:
    """fix2 BR-11／fix4 BR-13: App 28 に同じ操作のマーカー `op_{op_id}` を持つ human 行があるかを **4 値** で返す。
    (found, record_id) / (absent, None) / (unavailable, None)＝検索失敗（不存在ではない・呼び出し側は作成しない）/
    (unconfigured, None)。kintone の like は単語検索（記号はキーワードから除かれる）なので、マーカーは英数字と `_` のみとし
    1 単語で検索、後段で本文末尾の等価照合（marker_matches）を行う（ヒットは直近 LOOKUP_LIMIT 件を照合）。"""
    if not (APP_CHATLOG.app_id() and APP_CHATLOG.token()):
        return reply_followup.LOOKUP_UNCONFIGURED, None
    if not _OP_ID_RE.fullmatch(str(op_id)):
        return reply_followup.LOOKUP_ABSENT, None
    marker = reply_followup.op_marker(str(op_id))
    try:
        rows = await kintone.search_records(
            APP_CHATLOG, f'line_user_id = "{user_id}" and role in ("{ROLE_HUMAN}") and '
                         f'message like "{marker}" order by $id asc limit {LOOKUP_LIMIT}',
            fields=["$id", "message"])
    except Exception:
        logger.warning("[REPLY] chatlog lookup unavailable (fixed reason)")
        return reply_followup.LOOKUP_UNAVAILABLE, None
    for r in rows:
        if reply_followup.marker_matches(_v(r, "message"), str(op_id)) and _v(r, "$id"):
            return reply_followup.LOOKUP_FOUND, _v(r, "$id")
    return reply_followup.LOOKUP_ABSENT, None


async def run_chatlog_followup() -> dict:
    """BR-08／fix2 BR-12: SEND_LEDGER_RECOVER に相乗りする回収（台帳と突合し、所有権つきで冪等に記録）。"""
    return await reply_followup.run_followup(resolve_user_id=_resolve_user_id, write_chatlog=write_human_chatlog,
                                             find_chatlog=find_human_chatlog, ledger_state=send_ledger.operation_state)


def reflect_memory_history(user_id: str, text: str) -> bool:
    """ヒアリング経路のプロセス内履歴に assistant（[事務所からの返信] マーカー付き）として反映
    （履歴が無い相談者は何もしない＝次の受信で App 28 から復元される）。戻り値=反映したか。"""
    try:
        import main as _main
        from chat_responder import HUMAN_MARKER
        hist = _main.conversation_histories.get(user_id)
    except Exception:
        return False
    if hist is None:
        return False
    marked = f"{HUMAN_MARKER} {text}"
    if hist and hist[-1].get("role") == "assistant":
        hist[-1]["content"] = str(hist[-1].get("content") or "") + "\n" + marked
    else:
        hist.append({"role": "assistant", "content": marked})
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
        conversation_ref=ref, source_op_id=op_id,
        source_text_version=send_ledger.text_version(text))


# ── 画面 ────────────────────────────────────────────────────────────────────
def _page(record_id: str, rec: dict | None, rows: list[dict], op_id: str, ticket: str | None,
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
    if rec is None or ticket is None:
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
                 f'<input type="hidden" name="ticket" value="{e(ticket)}">'
                 '<div class="lbl">返信文（AI 下書きがあれば初期表示。直して送れます）</div>'
                 f'<textarea name="text" maxlength="{MAX_TEXT_CHARS}" required>{e(draft)}</textarea>'
                 f'<label><input type="checkbox" name="confirm" value="1" required> {e(CONFIRM_LABEL)}</label>'
                 '<label><input type="checkbox" name="exemplar" value="1" checked> '
                 "この返信を手本として保存する（氏名・連絡先などを伏せて保存）</label>"
                 '<button type="submit">LINE で送信する</button></form>')
    parts.append('<script src="/app/shell.js"></script></body></html>')
    return "".join(parts)


def _notices(q) -> list[str]:
    done = q.get("done", "")
    done_text = _DONE_NOTICES.get(done, "")
    if done == "late" and done_text:
        done_text += _STATE_LABELS.get(q.get("state", ""), _STATE_LABELS["none"])
    return [done_text, _LOG_NOTICES.get(q.get("log", ""), ""), _EX_NOTICES.get(q.get("ex", ""), "")]


async def reply_page(request: Request):
    record_id = str(request.path_params.get("record_id", ""))
    if not _RECORD_ID_RE.fullmatch(record_id):
        return _bad_request()
    notices = _notices(request.query_params)
    rec = await _approval_record(record_id)
    user_id = _v(rec, "line_user_id") if rec else ""
    if rec is not None and not _LINE_USER_ID_RE.fullmatch(user_id):
        rec = None
    rows = await _history_rows(user_id) if rec is not None else []
    op_id = uuid.uuid4().hex
    ticket = make_ticket(record_id, user_id, _revision(rec), op_id) if rec is not None else None
    return HTMLResponse(_page(record_id, rec, rows, op_id, ticket, notices))


def _redirect(back: str, done: str, **extra) -> RedirectResponse:
    qs = "&".join([f"done={done}"] + [f"{k}={v}" for k, v in extra.items() if v])
    return RedirectResponse(f"{back}?{qs}", status_code=303)


async def reply_send(request: Request):
    """明示送信（PRG: 303 で本画面へ戻す）。入力は閉集合・固定書式のみ（不正は固定 400）。"""
    record_id = str(request.path_params.get("record_id", ""))
    form = await request.form()
    op_id = str(form.get("op_id", ""))
    ticket = str(form.get("ticket", ""))
    text = str(form.get("text", "")).strip()
    confirm = str(form.get("confirm", "")) == "1"
    want_exemplar = str(form.get("exemplar", "")) == "1"
    if not _RECORD_ID_RE.fullmatch(record_id) or not _OP_ID_RE.fullmatch(op_id):
        return _bad_request()
    bound = parse_ticket(ticket, record_id, op_id)
    if bound is None:
        return _bad_request()                      # 署名不正・束縛不一致は固定 400（送らない）
    ticket_hash, ticket_rev = bound
    back = f"{PAGE}/{record_id}"
    if not text or len(text) > MAX_TEXT_CHARS:
        return _redirect(back, "bad_input")
    if not confirm:
        return _redirect(back, "confirm_required")
    # 同じ操作 ID の再送信は副作用なしで弾く（二重クリック・戻る→再送）。照会不能は fail-closed
    try:
        state = await send_ledger.operation_state(op_id)
    except Exception:
        logger.warning("[REPLY] ledger unavailable (duplicate check failed, not sent)")
        return _redirect(back, "ledger_unavailable")
    if state == send_ledger.STATE_SENT:
        # BR-08: 送信済み・記録のみ再試行
        fu = await _safe_followup(op_id)
        if fu is not None and fu["state"] in reply_followup.OPEN_STATES:
            res = await reply_followup.process_one(op_id, resolve_user_id=_resolve_user_id,
                                                   write_chatlog=write_human_chatlog,
                                                   find_chatlog=find_human_chatlog,
                                                   ledger_state=send_ledger.operation_state)
            return _redirect(back, "duplicate_sent", log=res)
        return _redirect(back, "duplicate_sent", log=("not_pending" if fu is not None else ""))
    if state is not None:
        return _redirect(back, "duplicate_unconfirmed")
    # BR-03: 宛先の再取得と照合（ハッシュ・revision が GET 時と一致するときだけ送る）
    rec = await _approval_record(record_id)
    user_id = _v(rec, "line_user_id") if rec else ""
    if rec is None or not _LINE_USER_ID_RE.fullmatch(user_id):
        return _redirect(back, "not_found")
    if _user_hash(user_id) != ticket_hash or _revision(rec) != ticket_rev:
        logger.info("[REPLY] destination changed since page render (not sent)")
        return _redirect(back, "stale")
    decision = send_ledger.human_explicit_decision(confirm)
    if not decision.allow:
        return _redirect(back, "blocked")
    # fix2 BR-12: 送信の前に回収表へ prepared を置く（置けなければ台帳必須と同じく送らない）
    prepared = await reply_followup.prepare(
        op_id, business=BUSINESS, approval_record_id=record_id,
        line_user_hash=send_ledger.line_user_hash(BUSINESS, user_id), text=text,
        text_version=send_ledger.text_version(text))
    if prepared in ("skipped", "failed"):
        logger.warning("[REPLY] followup unavailable (fail-closed, not sent) record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))
        return _redirect(back, "ledger_unavailable")
    if prepared == "rejected":
        # fix4 BR-15: 同 op_id の既存行が 終端／送信済み／本文・宛先不一致／台帳行あり → 送らず「新しい操作でやり直し」
        logger.info("[REPLY] operation rejected by followup table (not sent)")
        return _redirect(back, "op_rejected")
    result = await line_channel.push_text_explicit(line_channel.JIKOU_CHANNEL, op_id, user_id, text)
    if result.outcome == line_channel.PUSH_LEDGER_UNAVAILABLE:
        logger.warning("[REPLY] ledger unavailable (not sent) record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))
        # fix4 BR-15: 閉じない（本文保持・prepared のまま）＝同一 op_id・同一本文の再 POST で再開できる
        await reply_followup.note_prepared(op_id, "ledger_unavailable")
        return _redirect(back, "ledger_unavailable")
    if result.outcome in (line_channel.PUSH_DUPLICATE_SENT, line_channel.PUSH_DUPLICATE_UNCONFIRMED):
        return _redirect(back, result.outcome)
    if result.outcome == line_channel.PUSH_FAILED:
        await reply_followup.close(op_id, "line_failed")          # 未送信で終端（回収しない）
    if result.outcome == line_channel.PUSH_UNCONFIRMED:
        logger.warning("[REPLY] explicit send unconfirmed (exception) record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))
        return _redirect(back, "unconfirmed")
    # BR-02: finish の戻り値で分岐（applied のときだけ成功処理）
    if result.ledger == "late":
        return _redirect(back, "late", state=(result.state or "none"))
    if result.ledger != "applied":
        return _redirect(back, "record_unconfirmed")
    if result.outcome != line_channel.PUSH_SENT:
        return _redirect(back, result.outcome)
    logger.info("[REPLY] explicit send ok record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    # fix3（PRE-REVIEW-1 #272-1）: 主経路の App 28 書込も回収ジョブと同じ関門（claim → op_id 照合 → 書込 → done）を通す。
    # 回収ジョブ（prepared も対象）と主経路が同じ op を同時に扱っても、所有権を取れた側だけが create する（二重作成なし）。
    async def _same_user(_rid: str) -> str:
        return user_id
    res = await reply_followup.process_one(op_id, resolve_user_id=_same_user, write_chatlog=write_human_chatlog,
                                           find_chatlog=find_human_chatlog, ledger_state=send_ledger.operation_state)
    log_res = _MAIN_PATH_LOG.get(res, "pending_retry")
    reflect_memory_history(user_id, text)
    ex_res = "off"
    if want_exemplar:
        ex_res = await _save_exemplar(user_id, _v(rec, "顧客メッセージ"), text, op_id,
                                      _v(rec, "顧客名"))
    return _redirect(back, "sent", log=log_res, ex=ex_res)


async def _safe_followup(op_id: str) -> dict | None:
    try:
        return await reply_followup.get_followup(op_id)
    except Exception:
        return None


router.add_api_route(f"{PAGE}/{{record_id}}", _gate(reply_page), methods=["GET"])
router.add_api_route(f"{PAGE}/{{record_id}}/send", _gate(reply_send), methods=["POST"])
# BR-08: 回収ジョブへの相乗り（冪等登録・main の末尾で register_recover_job が走る前後どちらでも可）
send_ledger.register_recover_hook(run_chatlog_followup)
