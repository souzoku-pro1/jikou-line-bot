"""jikou_stage — 時効案件進行アプリ（kintone・スマホ前提）との連携（JIKOU-STAGE-APP-1・QUEUE-2026-10-20-HI I-4'）

大野裁定: 時効の確認と依頼者への報告は kintone の新アプリ 1 画面で完結。大野が押すのは「時効確認済にする」「不成立にする」の
2 ボタン＝承認。送る文言は承認台帳の固定文（hub/jikou_stage_texts R-1〜R-3）のみ。Q4 への下書き積みは作らない。

- 有効条件: env JIKOU_STAGE_APP_ID と JIKOU_STAGE_WEBHOOK_SECRET の両方（書込には JIKOU_STAGE_API_TOKEN も要る）。無効なら
  受け口 404・kintone 呼出ゼロ・ジョブは何もしない。
- 1 レコード＝案件×債権者（欄コードは docs/kintone_app_jikou_stage.md）。
- 写し（ボットが進める状態）: 受任→入金確認→発送済→回答待ち。既存の事実（App 21 入金状況・結果記録の発送日／回答日）から導き、
  **前にしか進めない**・人の状態（時効確認済／不成立／報告済／完了／保留）には触れない。op_id（送信台帳op_id 欄）照合で冪等。
- 受信: POST /hooks/kintone/jikou-stage/{secret}（kintone プロセス管理の状態変更 Webhook・type=UPDATE_STATUS）。
  シークレット不一致＝401・レコード ID／状態／更新者（＝承認者）を取り出す。同一レコード×同一状態は 1 回だけ処理
  （送信台帳の op_id＝jikou_stage:{record_id}:{状態} と 送信台帳op_id 欄で冪等）。
- 「時効確認済」: 取消猶予 JIKOU_CONFIRM_GRACE_MIN 分（既定 10・要大野確認・0=即送）の後、
  (a) 結果記録を成立で保存 → (b) 完了報告書 PDF（hub/jikou_cert）→ (c) 固定文 R-1＋本人専用 URL（72 時間・PII なし）を
  既存の人による明示送信経路（push_text_explicit・actor=human・op_id 固定）で送信 → (d) 台帳 applied 後に状態を「報告済」へ・
  送信日時と版番号を写す。(b)(c) 失敗は状態を戻さず「要対応」＋業務 LINE 1 通（依頼者には送らない）。
- 「不成立」: 固定文 R-3（証明書なし）→「報告済(不成立)」。全債権者が報告済／報告済(不成立)になったら R-2 を 1 通・全て「完了」。
- 猶予中に「回答待ち」へ戻せば送らない。処理は単一 worker 前提（_pending はプロセス内・再起動時は未処理の確認済レコードを
  30 分ごとの走査で拾う）。
- 禁止: Webhook 以外からこの送信経路を叩けない（受け口は secret 必須・/app には送信ボタンを置かない）。依頼者の氏名・生年月日・
  住所・電話をログ・通知・URL に載せない。kintone 書込は状態・日時・番号・op_id の写しのみ（依頼者欄は作成時の転記だけ）。
"""

from __future__ import annotations

import datetime
import hmac
import logging
import os

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import approved_texts, jikou_cert, jikou_report_link, kintone, line_channel
from hub import jikou_followup as F
from hub import jikou_stage_texts as T
from hub.redact import emit
from hub.webhook_auth import extract_record_id

logger = logging.getLogger("hub.jikou_stage")

ENV_APP_ID = "JIKOU_STAGE_APP_ID"
ENV_SECRET = "JIKOU_STAGE_WEBHOOK_SECRET"
ENV_TOKEN = "JIKOU_STAGE_API_TOKEN"
ENV_GRACE_MIN = "JIKOU_CONFIRM_GRACE_MIN"
ENV_DUE_DAYS = "JIKOU_CONFIRM_DUE_DAYS"
GRACE_MIN_DEFAULT = 10            # 要大野確認
DUE_DAYS_DEFAULT = 7              # 要大野確認（一覧「確認漏れ」の閾値・アプリ側の一覧条件に使う）
PATH = "/hooks/kintone/jikou-stage"
STAGE_APP = kintone.KintoneApp("時効案件進行", ENV_APP_ID, ENV_TOKEN)
JOB_GRACE = "JIKOU_STAGE_GRACE"
JOB_MIRROR = "JIKOU_STAGE_MIRROR"
MIRROR_INTERVAL_MIN = 30.0
RECOVER_INTERVAL_MIN = 30.0
_JST = datetime.timezone(datetime.timedelta(hours=9))

# 欄コード（docs/kintone_app_jikou_stage.md と一致させる）
F_CASE, F_SLOT, F_CREDITOR = "案件レコードID", "枠", "債権者名"
F_SHIPPED, F_ANSWERED, F_ANSWER_MEMO = "通知発送日", "回答受領日", "回答内容メモ"
F_NAME, F_BIRTH, F_ADDR = "氏名", "生年月日", "住所"
F_STATUS = "ステータス"
F_REPORT_SENT_AT, F_REPORT_VERSION, F_RECEIPT_NO, F_OP_ID = "報告送信日時", "報告書版番号", "領収書番号", "送信台帳op_id"
F_ACTION_NEEDED, F_ACTION_REASON = "要対応", "要対応理由"
ACTION_NEEDED_VALUE = "要対応"
APP21_NAME, APP21_BIRTH, APP21_ADDR, APP21_LINE, APP21_PAYMENT = "顧客名", "生年月日", "住所", "LINEユーザーID", "入金状況"
APP21_PAID = "入金済み"

# 状態（プロセス管理）とアクション名
S_ENGAGED, S_PAID, S_SHIPPED, S_WAITING = "受任", "入金確認", "発送済", "回答待ち"
S_CONFIRMED, S_REPORTED, S_DONE = "時効確認済", "報告済", "完了"
S_FAILED, S_REPORTED_FAILED, S_HOLD = "不成立", "報告済(不成立)", "保留"
BOT_CHAIN = (S_ENGAGED, S_PAID, S_SHIPPED, S_WAITING)
A_PAID, A_SHIPPED, A_WAITING = "入金確認にする", "発送済にする", "回答待ちにする"
A_REPORTED, A_REPORTED_FAILED, A_DONE = "報告済にする", "報告済(不成立)にする", "完了にする"
BOT_ACTIONS = {S_PAID: A_PAID, S_SHIPPED: A_SHIPPED, S_WAITING: A_WAITING}
HUMAN_ACTIONS = ("時効確認済にする", "不成立にする", "回答待ちに戻す", "保留にする")
HUMAN_TRIGGER_STATES = (S_CONFIRMED, S_FAILED)
FINAL_STATES = (S_REPORTED, S_REPORTED_FAILED, S_DONE)

router = APIRouter()

_pending: dict[tuple[str, str], dict] = {}     # (record_id, status) → {"due": datetime, "approver": str}
_last_recover: datetime.datetime | None = None


def enabled() -> bool:
    return bool(os.environ.get(ENV_APP_ID, "").strip()) and bool(os.environ.get(ENV_SECRET, "").strip())


def grace_minutes() -> int:
    raw = os.environ.get(ENV_GRACE_MIN, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return GRACE_MIN_DEFAULT
    return v if v >= 0 else GRACE_MIN_DEFAULT


def due_days() -> int:
    raw = os.environ.get(ENV_DUE_DAYS, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return DUE_DAYS_DEFAULT
    return v if v > 0 else DUE_DAYS_DEFAULT


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _fv(record: dict | None, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def op_id_for(record_id: str, status: str) -> str:
    return f"jikou_stage:{record_id}:{status}"


def all_done_op_id(case_record_id: str) -> str:
    return f"jikou_stage:{case_record_id}:all_done"


def pending_items() -> dict:
    return dict(_pending)


def reset_for_tests() -> None:
    global _last_recover
    _pending.clear()
    _last_recover = None


# ── Webhook 受け口 ─────────────────────────────────────────────────────────────

def _secret_ok(secret: str) -> bool:
    expected = os.environ.get(ENV_SECRET, "")
    return bool(expected) and hmac.compare_digest(secret or "", expected)


async def stage_webhook(request: Request):
    if not enabled():
        return JSONResponse(status_code=404, content={"error": "not found"})
    secret = str(request.path_params.get("secret", ""))
    if not _secret_ok(secret):
        logger.warning("[JIKOU_STAGE] webhook rejected (bad secret)")
        return JSONResponse(status_code=401, content={"error": "unauthorized"})
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    if not isinstance(body, dict) or body.get("type") != "UPDATE_STATUS":
        return JSONResponse(status_code=200, content={"ok": True, "skip": "not_status_change"})
    app_id = str(((body.get("app") or {}).get("id")) or "")
    if app_id and app_id != os.environ.get(ENV_APP_ID, "").strip():
        return JSONResponse(status_code=200, content={"ok": True, "skip": "other_app"})
    record = body.get("record") or {}
    record_id = extract_record_id(body)
    if not record_id or not str(record_id).isdigit():
        return JSONResponse(status_code=200, content={"ok": True, "skip": "no_record_id"})
    status = _fv(record, F_STATUS)
    approver = str((((record.get("更新者") or {}).get("value")) or {}).get("code") or "")
    queued = await on_status_change(str(record_id), status, approver)
    return JSONResponse(status_code=200, content={"ok": True, "queued": queued})


router.add_api_route(PATH + "/{secret}", stage_webhook, methods=["POST"])


async def on_status_change(record_id: str, status: str, approver: str, *, now: datetime.datetime | None = None) -> bool:
    """状態変更 1 件の受理。確認／不成立は猶予付きで予約（猶予 0 は即処理）・回答待ちは予約の取消。戻り値=予約／処理したか。"""
    now = now or _now()
    if status == S_WAITING:
        for key in [k for k in _pending if k[0] == record_id]:
            _pending.pop(key, None)
        logger.info("[JIKOU_STAGE] confirm cancelled record_id=%s", emit(record_id, "record_id", "log", "operator"))
        return False
    if status not in HUMAN_TRIGGER_STATES:
        return False
    key = (record_id, status)
    if key in _pending:
        return True                                         # 重複 Webhook: 予約は 1 つ
    grace = grace_minutes()
    if grace == 0:
        await process(record_id, status, approver, now=now)
        return True
    _pending[key] = {"due": now + datetime.timedelta(minutes=grace), "approver": approver}
    logger.info("[JIKOU_STAGE] confirm queued record_id=%s", emit(record_id, "record_id", "log", "operator"))
    return True


async def tick(now: datetime.datetime | None = None) -> int:
    """猶予が明けた予約を処理する（1 分間隔のジョブ）。30 分ごとに未処理の確認済レコードも拾う。戻り値=処理件数。"""
    global _last_recover
    now = now or _now()
    done = 0
    for key, item in list(_pending.items()):
        if item["due"] <= now:
            _pending.pop(key, None)
            await process(key[0], key[1], item["approver"], now=now)
            done += 1
    if enabled() and (_last_recover is None or (now - _last_recover) >= datetime.timedelta(minutes=RECOVER_INTERVAL_MIN)):
        _last_recover = now
        try:
            done += await recover_unprocessed(now=now)
        except Exception:
            logger.warning("[JIKOU_STAGE] recover scan failed (fixed reason)")
    return done


async def recover_unprocessed(*, now: datetime.datetime | None = None) -> int:
    """再起動で予約が消えた場合の保険: 状態が時効確認済／不成立で op_id 欄が空・更新から猶予以上経過したレコードを処理する。"""
    now = now or _now()
    rows = await kintone.search_records(STAGE_APP, f'{F_STATUS} in ("{S_CONFIRMED}", "{S_FAILED}") and {F_OP_ID} = ""')
    n = 0
    for r in rows:
        updated = F.parse_date(_fv(r, "更新日時")[:10])
        if updated is None:
            continue
        if (now.astimezone(_JST).date() - updated).days < 0:
            continue
        rid = _fv(r, "$id")
        if (rid, _fv(r, F_STATUS)) in _pending:
            continue
        await process(rid, _fv(r, F_STATUS), str((((r.get("更新者") or {}).get("value")) or {}).get("code") or ""), now=now)
        n += 1
    return n


# ── 確認処理 ───────────────────────────────────────────────────────────────────

async def _mark_action_needed(rec: dict, record_id: str, reason: str) -> str:
    """状態は戻さず「要対応」を立て、業務 LINE へ 1 通（依頼者には送らない）。"""
    try:
        await kintone.update_record(STAGE_APP, record_id, {F_ACTION_NEEDED: [ACTION_NEEDED_VALUE], F_ACTION_REASON: reason},
                                    revision=_fv(rec, "$revision") or None)
    except Exception:
        logger.warning("[JIKOU_STAGE] action-needed write failed (fixed reason)")
    from hub.notify import notify_admin_line
    try:
        await notify_admin_line(f"【時効案件進行: 要対応】レコードNo: {record_id}\n理由コード: {reason}\n"
                                "依頼者への送信は行っていません。kintone の時効案件進行アプリで内容を確認してください。",
                                throttle_key=f"jikou_stage_action_needed:{record_id}")
    except Exception:
        logger.error("[JIKOU_STAGE] action-needed notice failed (request failed)")
    logger.warning("[JIKOU_STAGE] action needed record_id=%s reason=%s", emit(record_id, "record_id", "log", "operator"),
                   emit(reason, "record_id", "log", "operator"))
    return "action_needed"


def _approved(key: str) -> str | None:
    return approved_texts.approved_text(key, T.text(key))


async def process(record_id: str, status: str, approver: str, *, now: datetime.datetime | None = None) -> str:
    """「時効確認済」「不成立」の本処理。戻り値は固定コード（sent / already / cancelled / action_needed / disabled / skip）。"""
    now = now or _now()
    if not enabled():
        return "disabled"
    try:
        rec = await kintone.get_record(STAGE_APP, record_id)
    except Exception:
        logger.warning("[JIKOU_STAGE] stage record read failed (fixed reason)")
        return "skip"
    op_id = op_id_for(record_id, status)
    if _fv(rec, F_OP_ID) == op_id:
        return "already"                                      # 同一レコード×同一状態は 1 回だけ（送信台帳 op_id 欄で冪等）
    if _fv(rec, F_STATUS) != status:
        return "cancelled"                                    # 猶予中に戻された／別状態へ進んだ
    case = _fv(rec, F_CASE)
    try:
        slot = int(_fv(rec, F_SLOT) or "0")
    except ValueError:
        slot = 0
    if not case or slot not in F.SLOTS:
        return await _mark_action_needed(rec, record_id, "case_or_slot_missing")
    try:
        app21 = await kintone.get_record(F.APP21, case)
    except Exception:
        return await _mark_action_needed(rec, record_id, "app21_unreadable")
    line_user = _fv(app21, APP21_LINE)
    if not line_user:
        return await _mark_action_needed(rec, record_id, "no_line_user")
    creditor = _fv(rec, F_CREDITOR) or dict(F.creditors_of(app21)).get(slot, "")
    answered = F.parse_date(_fv(rec, F_ANSWERED))
    shipped = F.parse_date(_fv(rec, F_SHIPPED))
    if answered is None:
        return await _mark_action_needed(rec, record_id, "answered_on_missing")
    # (a) 結果記録（F-1）。発送日は空欄のときだけ写す
    try:
        creditors = F.creditors_of(app21) or [(slot, creditor)]
        await F.ensure_rows(case, creditors, now=now)
        if shipped:
            await F.set_shipped(case, shipped, source="kintone", actor=approver or "kintone", now=now, only_if_empty=True)
        result = F.RESULT_ESTABLISHED if status == S_CONFIRMED else F.RESULT_NOT_ESTABLISHED
        ok = await F.record_result(case, slot, result=result, answered_on=answered, summary=F.normalize_summary(_fv(rec, F_ANSWER_MEMO)),
                                   actor=approver or "kintone", now=now)
        if not ok:
            return await _mark_action_needed(rec, record_id, "result_row_missing")
    except Exception:
        return await _mark_action_needed(rec, record_id, "result_write_failed")
    version = ""
    if status == S_CONFIRMED:
        # (b) 完了報告書 PDF
        try:
            doc = await jikou_cert.generate(case, slot, actor=approver or "kintone", record=app21, now=now)
        except jikou_cert.GenerationRefused as e:
            return await _mark_action_needed(rec, record_id, f"cert_{e}")
        except Exception:
            return await _mark_action_needed(rec, record_id, "cert_failed")
        version = str(doc["version"])
        body = _approved("R-1")
        if body is None:
            return await _mark_action_needed(rec, record_id, "text_unapproved")
        token = await jikou_report_link.issue(case, doc["doc_id"], now=now)
        url = jikou_report_link.public_url_for(token)
        if not url:
            return await _mark_action_needed(rec, record_id, "public_url_unset")
        text = body.replace(T.SLOT_URL, url).replace(T.SLOT_CREDITOR, creditor)
        action = A_REPORTED
    else:
        body = _approved("R-3")
        if body is None:
            return await _mark_action_needed(rec, record_id, "text_unapproved")
        text = body.replace(T.SLOT_CREDITOR, creditor)
        action = A_REPORTED_FAILED
    # (c) 既存の人による明示送信経路（actor=human・op_id 固定・台帳必須）
    res = await line_channel.push_text_explicit(line_channel.JIKOU_CHANNEL, op_id, line_user, text)
    sent = (res.outcome == line_channel.PUSH_SENT and res.ledger == line_channel.LEDGER_APPLIED) \
        or res.outcome == line_channel.PUSH_DUPLICATE_SENT
    if not sent:
        return await _mark_action_needed(rec, record_id, f"send_{res.outcome}")
    # (d) 写し: 送信日時・版番号・op_id → 状態
    fields = {F_REPORT_SENT_AT: now.astimezone(_JST).strftime("%Y-%m-%dT%H:%M:%S%z"), F_OP_ID: op_id}
    if version:
        fields[F_REPORT_VERSION] = version
    try:
        await kintone.update_record(STAGE_APP, record_id, fields, revision=_fv(rec, "$revision") or None)
        await kintone.update_status(STAGE_APP, record_id, action)
    except Exception:
        return await _mark_action_needed(rec, record_id, "status_write_failed")
    logger.info("[JIKOU_STAGE] reported record_id=%s", emit(record_id, "record_id", "log", "operator"))
    try:
        await maybe_all_done(case, line_user)
    except Exception:
        logger.warning("[JIKOU_STAGE] all-done check failed (fixed reason)")
    return "sent"


async def maybe_all_done(case_record_id: str, line_user: str) -> bool:
    """案件の全レコードが報告済／報告済(不成立)／完了なら R-2 を 1 通送り、全て「完了」にする。"""
    rows = await kintone.search_records(STAGE_APP, f'{F_CASE} = "{case_record_id}"')
    if not rows:
        return False
    statuses = [_fv(r, F_STATUS) for r in rows]
    if any(s not in FINAL_STATES for s in statuses) or all(s == S_DONE for s in statuses):
        return False
    body = _approved("R-2")
    if body is None:
        logger.warning("[JIKOU_STAGE] all-done text unapproved (not sent)")
        return False
    res = await line_channel.push_text_explicit(line_channel.JIKOU_CHANNEL, all_done_op_id(case_record_id), line_user, body)
    if not ((res.outcome == line_channel.PUSH_SENT and res.ledger == line_channel.LEDGER_APPLIED)
            or res.outcome == line_channel.PUSH_DUPLICATE_SENT):
        return False
    for r in rows:
        if _fv(r, F_STATUS) != S_DONE:
            try:
                await kintone.update_status(STAGE_APP, _fv(r, "$id"), A_DONE)
            except Exception:
                logger.warning("[JIKOU_STAGE] done status write failed (fixed reason)")
    return True


# ── 写し（受任→入金確認→発送済→回答待ち） ─────────────────────────────────────

async def stage_rows_for_case(case_record_id: str) -> list[dict]:
    return await kintone.search_records(STAGE_APP, f'{F_CASE} = "{case_record_id}" order by {F_SLOT} asc')


async def ensure_records(case_record_id: str, app21: dict) -> int:
    """受任時: 債権者ごとに 1 件（既存の枠は作らない）。依頼者情報は作成時に転記（以後は App 21 が正）。"""
    existing = {_fv(r, F_SLOT) for r in await stage_rows_for_case(case_record_id)}
    created = 0
    for slot, name in F.creditors_of(app21):
        if str(slot) in existing:
            continue
        await kintone.create_record(STAGE_APP, {F_CASE: str(case_record_id), F_SLOT: str(slot), F_CREDITOR: name,
                                                F_NAME: _fv(app21, APP21_NAME), F_BIRTH: _fv(app21, APP21_BIRTH),
                                                F_ADDR: _fv(app21, APP21_ADDR), F_OP_ID: "created"})
        created += 1
    if created:
        logger.info("[JIKOU_STAGE] records created case=%s n=%s", emit(case_record_id, "record_id", "log", "operator"),
                    emit(created, "count", "log", "operator"))
    return created


def desired_bot_state(app21: dict, row: dict | None) -> str:
    if row and row.get("answered_on"):
        return S_WAITING
    if row and row.get("shipped_on"):
        return S_SHIPPED
    if _fv(app21, APP21_PAYMENT) == APP21_PAID:
        return S_PAID
    return S_ENGAGED


async def mirror_case(case_record_id: str, app21: dict | None = None) -> int:
    """App 21・結果記録の事実から、ボット管轄の状態を前方へ進める（人の状態には触れない）。戻り値=進めた件数。"""
    if not enabled():
        return 0
    if app21 is None:
        app21 = await kintone.get_record(F.APP21, case_record_id)
    await ensure_records(case_record_id, app21)
    rows = {int(r["slot"]): r for r in await F.rows_for_case(case_record_id)}
    moved = 0
    for rec in await stage_rows_for_case(case_record_id):
        cur = _fv(rec, F_STATUS)
        if cur not in BOT_CHAIN:
            continue
        try:
            slot = int(_fv(rec, F_SLOT))
        except ValueError:
            continue
        target = desired_bot_state(app21, rows.get(slot))
        ci, ti = BOT_CHAIN.index(cur), BOT_CHAIN.index(target)
        if ti <= ci:
            continue
        rid = _fv(rec, "$id")
        for nxt in BOT_CHAIN[ci + 1: ti + 1]:
            await kintone.update_status(STAGE_APP, rid, BOT_ACTIONS[nxt])
        row = rows.get(slot) or {}
        fields = {F_OP_ID: f"mirror:{target}"}
        if row.get("shipped_on") and not _fv(rec, F_SHIPPED):
            fields[F_SHIPPED] = row["shipped_on"].isoformat()
        if row.get("answered_on") and not _fv(rec, F_ANSWERED):
            fields[F_ANSWERED] = row["answered_on"].isoformat()
        await kintone.update_record(STAGE_APP, rid, fields)
        moved += 1
    return moved


async def on_result_recorded(case_record_id: str, record: dict | None) -> None:
    """hub/jikou_followup.register_result_hook: 結果記録の保存時に写しを進める（失敗は握る）。"""
    if not enabled():
        return
    try:
        await mirror_case(case_record_id, record)
    except Exception:
        logger.warning("[JIKOU_STAGE] mirror on result failed (fixed reason)")


async def mirror_scan(*, now: datetime.datetime | None = None) -> int:
    """30 分ごと: 受任済み（更新 7 日以内）の App 21 と、ボット管轄状態にある進行レコードの案件を写す。"""
    if not enabled():
        return 0
    now = now or _now()
    since = (now - datetime.timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    cases: set[str] = set()
    try:
        for r in await kintone.search_records(F.APP21, f'status in ("受任") and 更新日時 > "{since}"', fields=["$id"]):
            cases.add(_fv(r, "$id"))
        for r in await kintone.search_records(STAGE_APP, f'{F_STATUS} in ("{S_ENGAGED}", "{S_PAID}", "{S_SHIPPED}")', fields=[F_CASE]):
            cases.add(_fv(r, F_CASE))
    except Exception:
        logger.warning("[JIKOU_STAGE] mirror scan read failed (fixed reason)")
        return 0
    moved = 0
    for case in sorted(c for c in cases if c):
        try:
            moved += await mirror_case(case)
        except Exception:
            logger.warning("[JIKOU_STAGE] mirror failed for one case (fixed reason)")
    return moved


# ── 結線 ──────────────────────────────────────────────────────────────────────

def register_jobs() -> None:
    from hub import scheduler
    if not scheduler.is_registered(JOB_GRACE):
        scheduler.register_interval(JOB_GRACE, 1.0, tick)
    if not scheduler.is_registered(JOB_MIRROR):
        scheduler.register_interval(JOB_MIRROR, MIRROR_INTERVAL_MIN, mirror_scan)
    scheduler.start_all()


def wire_all(app) -> None:
    """main.py 末尾の 1 行: Webhook 受け口・本人専用 URL の公開 GET・結果記録フック・ジョブ登録。"""
    app.include_router(router)
    app.include_router(jikou_report_link.router)
    F.register_result_hook(on_result_recorded)
    app.router.on_startup.append(register_jobs)
