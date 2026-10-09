"""相続放棄 熟慮期間 監視（hub/houki_jukuryo・HOUKI-JUKURYO-2 / fix1）

App 40（相続放棄案件）の受任後案件について熟慮期間（3 か月）の期限を **1 か所で算出**し、
App 40 の「熟慮期間期限」「残日数」に保存し、期限 14 日前・7 日前に業務 LINE へ 1 回ずつ
通知する。HOUKI-JUKURYO-CRON-1（申告 3 日付の代用順・法定/社内の 2 本立て・履歴欄）は
本票で全面差し替え。

大野裁定（2026-10-08・法的判断・凍結）:
 (a) 期限日 = 起算日_確定 の 3 か月後の応当日の **前日**（安全側・民法原則より 1 日早い）。
     例: 起算日_確定 4/10 → 期限 7/9。応当日が無い月（例 11/30 の 3 か月後）は
     翌月末日の前日ではなく、**その月の末日の前日**（11/30 → 2/27〔平年〕・2/28〔閏年〕）。
 (b) 期間伸長は扱わない（伸長後の欄・分岐を作らない）。
 (c) アラートは期限の 14 日前と 7 日前の 2 回（各 1 回のみ・重複送信なし）。期限超過の
     通知は作らない。

司令塔裁定:
 - 入力は 起算日_確定 のみ（申告 3 日付・起算点確定済・法定満了日・社内締切日 は読まない）。
   起算日_確定 が空なら算出しない（受任後なら「起算日未確定 n 件」の件数通知のみ・1 日 1 回）。
 - 熟慮期間期限／残日数 はシステム所有欄: 起算日_確定 が変われば **再計算して上書き**
   （空欄のみ CAS ではない）。起算日_確定 が空に戻れば両欄を空に戻す。
 - 対象 = 受任後の全 status（HOUKI_PROFILE.post_engagement_statuses の 8 値）かつ 申述提出日 が空。
 - 通知文は固定文言＋レコード番号＋期限＋残日数のみ（氏名等の PII を載せない・RV-10）。

fix1（Codex R-HOUKI-JUKURYO-2 BH-01〜04・司令塔裁定「通知を送った事実の正は send_ledger」）:
 - BH-02/03: 通知（14/7 日前・起算日未確定）は **送信台帳（hub/send_ledger・本番 DB・永続）**を
   通す。1 レコード×1 閾値 = 1 送信操作 = 1 LINE push（分割の単位と台帳の単位を一致させる・
   操作の付帯情報列を増やさない）。業務キーは inbound_event_id に
   「{種別}:{レコード ID}:{期限日}」（未確定件数は「{種別}:{JST 日付}」）として置き、
   同キーの sent／unconfirmed があれば送らない（台帳の重複検出をそのまま使う）。
   HTTP は guarded_http（heartbeat・timeout）経由。結果不明は unconfirmed（自動再送なし・
   /app/send_ops に出る）。kintone の 通知済み閾値 は **写し**（finish(sent) が applied の後に
   刻印・刻印失敗は次回実行で刻印だけ再試行＝再通知しない）。
 - fix2（Codex BH-05〜07・司令塔裁定「熟慮期間通知ジョブは台帳必須」）: 台帳が使えない
   （begin が None）ときは **送らない**（fail-closed・固定理由 ledger_unavailable の件数ログ・
   次回再試行）。finish(sent) の戻り値が applied のときだけ sent と刻印へ進み、late／skipped は
   unconfirmed 扱い（刻印しない・自動再送しない）。/app/send_ops は channel=houki_jukuryo の操作に
   限って業務キーを閉集合で解析し「案件番号・期限・14日前/7日前」（未確定件数は「対象日」）を表示。
 - purpose: 本番 DB の CHECK 制約 ck_send_operation_purpose（migration e5f8）が閉集合を固定して
   おり、票の規律「migration なし」の下では新 purpose を挿入できない。purpose は other とし、
   種別は inbound_event_id の接頭辞（houki_jukuryo_14 / houki_jukuryo_7 / houki_jukuryo_unset）と
   channel="houki_jukuryo" で判別する（purpose の正式追加は migration を伴う別票）。
 - BH-01: 保存の 409 再取得後は対象（status・申述提出日・起算日_確定）を再評価し、対象外に
   なっていれば更新を中止する（件数ログのみ）。
 - BH-04: 期限が変われば業務キーが変わる（同一案件・同一閾値でも訂正後は 1 回通知される）。
   notify のメモリ throttle は使わない。

書く欄は 熟慮期間期限・残日数（算出結果）と 通知済み閾値（送信成功後の刻印）の 3 つだけ。
他の欄・status は書かない。順序: 算出 → 通知（台帳 begin → push → finish）→ 1 レコード
1 回の $revision CAS で保存（409 は再取得して 1 回だけ再計算・再試行）。
登録方式は hub/return_deadline と同じ（hub/scheduler の daily・8/13/18 JST・単一 worker 前提）。
新規 env は読まない。App 21 には触れない。hub/notify の切り詰め（4900 字）には触れない。
"""

import calendar
import functools
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import httpx

from hub import kintone, send_ledger
from hub import scheduler as hub_scheduler
from hub.houki_case_store import APP_HOUKI_CASE
from hub.houki_profile import HOUKI_PROFILE
from hub.notify import business_channel_allowlist, business_token_env, get_admin_line_user_id
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由

logger = logging.getLogger("hub.houki_jukuryo")

_JST = timezone(timedelta(hours=9))
_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# ── 定数（1 か所・テストで pin） ─────────────────────────────────────────────
JOB_NAME = "HOUKI_JUKURYO"                   # ジョブ名は f"{JOB_NAME}_{HH}"（時刻ごとに 1 つ）
RUN_HOURS_JST = (8, 13, 18)                  # CRON-1 から時刻を維持（票 6）

FIELD_STATUS = "status"
FIELD_SUBMITTED = "申述提出日"
FIELD_START = "起算日_確定"                   # 唯一の入力（司令塔裁定）
FIELD_DEADLINE = "熟慮期間期限"               # 出力 1（DATE・システム所有）
FIELD_REMAINING = "残日数"                    # 出力 2（NUMBER・システム所有）
FIELD_NOTIFIED = "通知済み閾値"               # 通知済みの写し（CHECK_BOX・既存欄）
WRITE_FIELDS = frozenset({FIELD_DEADLINE, FIELD_REMAINING, FIELD_NOTIFIED})   # 書く欄の閉集合

# 対象 status（受任後の 8 値・hub/houki_profile が単一の正）
TARGET_STATUSES: tuple = tuple(sorted(HOUKI_PROFILE.post_engagement_statuses))

JUKURYO_MONTHS = 3                           # 裁定 (a)
ALERT_DAYS = (14, 7)                         # 裁定 (c)・残日数がこの値以下で未通知なら送る
NOTIFIED_VALUES = {14: "14日前", 7: "7日前"}  # 通知済み閾値 の実選択肢値（form fields 実測）

# 送信台帳（fix1 BH-02/03）: business は相続放棄・channel は本ジョブ・purpose は DB CHECK 制約の
# 範囲内（other）。種別は inbound_event_id の接頭辞で表す
LEDGER_BUSINESS = "souzoku-houki"
LEDGER_CHANNEL = "houki_jukuryo"
LEDGER_PURPOSE = send_ledger.PURPOSE_OTHER
LEDGER_KIND_14 = "houki_jukuryo_14"
LEDGER_KIND_7 = "houki_jukuryo_7"
LEDGER_KIND_UNSET = "houki_jukuryo_unset"
LEDGER_KINDS = {14: LEDGER_KIND_14, 7: LEDGER_KIND_7}

# 送信結果（固定語彙）
SEND_SENT = "sent"                           # 台帳に記録し、送信し、finish(sent) が applied
SEND_DUPLICATE_SENT = "duplicate_sent"       # 台帳に sent がある（送らない・刻印の再試行のみ）
SEND_DUPLICATE_UNCONFIRMED = "duplicate_unconfirmed"   # 台帳に未確定がある（送らない・人の確定待ち）
SEND_UNCONFIRMED = "unconfirmed"             # push の結果不明、または finish が applied でない（自動再送なし）
SEND_FAILED = "failed"                       # push 失敗・宛先なし（次回実行で再試行）
SEND_LEDGER_UNAVAILABLE = "ledger_unavailable"   # fix2 BH-05: 台帳が使えない → 送らない（fail-closed・次回再試行）

NOTICE_HEADER = "【相続放棄 熟慮期間】"
NOTICE_FOOTER = "期限＝起算日_確定の3か月後の応当日の前日。レコード番号と残日数のみ・詳細はApp 40で確認してください。"
UNSET_LABEL = "起算日未確定"
UNSET_NOTE = "（受任後・申述提出日なし・起算日_確定が空。個別のレコードはPWA/App 40で確認してください）"
SEARCH_FIELDS = ["$id", "$revision", FIELD_STATUS, FIELD_SUBMITTED, FIELD_START,
                 FIELD_DEADLINE, FIELD_REMAINING, FIELD_NOTIFIED]
SEARCH_LIMIT = 500                           # kintone records.json の上限（ページング）

def _today_jst() -> date:
    return datetime.now(_JST).date()


def _v(record: dict, code: str) -> str:
    val = (record.get(code) or {}).get("value")
    if isinstance(val, list):
        return ",".join(str(x) for x in val)
    return str(val or "").strip()


def _parse_date(text: str) -> date | None:
    try:
        return date.fromisoformat(text) if text else None
    except ValueError:
        return None


# ── 期限計算（pure・唯一の正。phone_triage もここを使う） ──────────────────────
def jukuryo_deadline(start: date) -> date:
    """裁定 (a): start の 3 か月後の応当日の前日。応当日が無い月はその月の末日の前日。
    例: 4/10 → 7/9・11/30 → 2/27（平年）/ 2/28（閏年）・1/31 → 4/29・3/1 → 5/31。"""
    month_index = start.month - 1 + JUKURYO_MONTHS
    year = start.year + month_index // 12
    month = month_index % 12 + 1
    last = calendar.monthrange(year, month)[1]
    anniversary = date(year, month, min(start.day, last))
    return anniversary - timedelta(days=1)


def remaining_days(deadline: date, today: date) -> int:
    """残日数（期限日当日=0・超過は負）。"""
    return (deadline - today).days


def resolve_start(record: dict) -> date | None:
    """起算日_確定 のみ（空・不正は None=算出しない）。"""
    return _parse_date(_v(record, FIELD_START))


@dataclass(frozen=True)
class Computation:
    start: date
    deadline: date
    remaining: int


def compute(record: dict, today: date) -> Computation | None:
    start = resolve_start(record)
    if start is None:
        return None
    deadline = jukuryo_deadline(start)
    return Computation(start, deadline, remaining_days(deadline, today))


# ── 対象・通知判定・保存差分（pure） ─────────────────────────────────────────
def is_target(record: dict) -> bool:
    """受任後 8 status かつ 申述提出日 空（検索条件と二重に検査・防御的）。"""
    return _v(record, FIELD_STATUS) in TARGET_STATUSES and not _v(record, FIELD_SUBMITTED)


def search_query(after_id: str | None = None) -> str:
    statuses = ",".join(f'"{s}"' for s in TARGET_STATUSES)
    cond = f'{FIELD_STATUS} in ({statuses}) and {FIELD_SUBMITTED} = ""'
    if after_id:
        cond += f" and $id > {int(after_id)}"
    return f"{cond} order by $id asc limit {SEARCH_LIMIT}"


def notified_values(record: dict) -> list[str]:
    val = (record.get(FIELD_NOTIFIED) or {}).get("value")
    if isinstance(val, list):
        return [str(x) for x in val]
    return [x for x in str(val or "").split(",") if x]


def alert_candidates(remaining: int) -> list[int]:
    """残日数が閾値以下の閾値（14→7 の順）。1 回性は台帳で判定する（写しは代替）。
    当日に走れなかった分も翌日以降に拾う。期限超過でも未通知の閾値があれば送る
    （取りこぼしを黙らせない・「超過」の通知は作らない）。"""
    return [d for d in ALERT_DAYS if remaining <= d]


def ledger_key(kind: str, business_key: str) -> str:
    """台帳の業務キー（inbound_event_id）。{種別}:{業務キー}。"""
    return f"{kind}:{business_key}"


def alert_business_key(record_id: str, comp: Computation) -> str:
    """BH-04: 期限日を含める＝期限が訂正されれば別の送信操作になり、訂正後に 1 回通知される。"""
    return f"{record_id}:{comp.deadline.isoformat()}"


def _stored_remaining(record: dict) -> int | None:
    text = _v(record, FIELD_REMAINING)
    try:
        return int(float(text)) if text else None
    except ValueError:
        return None


def field_updates(record: dict, comp: Computation | None) -> dict:
    """保存すべき差分（熟慮期間期限・残日数）。値が同じなら空（同日再実行で書かない）。
    comp が None（起算日_確定 空）で欄に値が残っていれば空に戻す（システム所有欄）。"""
    out: dict = {}
    if comp is None:
        if _v(record, FIELD_DEADLINE):
            out[FIELD_DEADLINE] = ""
        if _v(record, FIELD_REMAINING):
            out[FIELD_REMAINING] = ""
        return out
    if _parse_date(_v(record, FIELD_DEADLINE)) != comp.deadline:
        out[FIELD_DEADLINE] = comp.deadline.isoformat()
    if _stored_remaining(record) != comp.remaining:
        out[FIELD_REMAINING] = str(comp.remaining)
    return out


def record_writes(record: dict, today: date, mark_days: list[int]) -> dict:
    """1 レコードの書込集合 = 算出差分 + 送信済み閾値の刻印（既存の刻印は保持）。"""
    fields = field_updates(record, compute(record, today))
    if mark_days:
        current = notified_values(record)
        merged = current + [NOTIFIED_VALUES[d] for d in mark_days if NOTIFIED_VALUES[d] not in current]
        if merged != current:
            fields[FIELD_NOTIFIED] = merged
    assert set(fields) <= WRITE_FIELDS
    return fields


# ── 通知本文（pure・PII なし） ───────────────────────────────────────────────
def format_alert_line(record_id: str, day: int, comp: Computation) -> str:
    return (f"・No.{record_id} 期限 {comp.deadline.isoformat()} 残 {comp.remaining} 日"
            f"（{day}日前の通知）")


def alert_text(today: date, record_id: str, day: int, comp: Computation) -> str:
    """1 レコード×1 閾値 = 1 通（台帳の単位と一致）。"""
    return "\n".join([f"{NOTICE_HEADER} {today.isoformat()}",
                      format_alert_line(record_id, day, comp), NOTICE_FOOTER])


def unset_text(today: date, count: int) -> str:
    return f"{NOTICE_HEADER} {today.isoformat()}\n{UNSET_LABEL} {count} 件{UNSET_NOTE}"


# ── 送信（台帳 → push → 確定） ───────────────────────────────────────────────
async def _push_admin_http(admin_id: str, text: str) -> bool:
    """業務チャネル（DISPATCHBOT）で管理者へ push する HTTP 部分。宛先 allowlist（H02）は
    hub/notify と同じ関所。transport 例外は上へ（guarded_http の呼出側で unconfirmed）。
    非 2xx は False（failed）。本文・宛先は emit 契約の外へ出さない。"""
    if admin_id not in business_channel_allowlist():
        logger.warning("houki_jukuryo push skipped (recipient not allowlisted)")
        return False
    token = os.environ.get(business_token_env(), "")
    if not token:
        logger.warning("houki_jukuryo push skipped (no business token)")
        return False
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _PUSH_URL,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"to": admin_id, "messages": [{"type": "text", "text": text[:4900]}]},
        )
    if not resp.is_success:
        logger.error("houki_jukuryo push failed status=%s body=%s",
                     emit(resp.status_code, "count", "log", "operator"),
                     emit(resp.text, "vendor_raw", "log", "operator"))
    return bool(resp.is_success)


async def send_notice(kind: str, business_key: str, text: str) -> str:
    """台帳を通した 1 通の送信。戻り値は固定語彙（SEND_*）。
    - 台帳に同キーの sent → SEND_DUPLICATE_SENT（送らない）
    - 台帳に同キーの未確定 → SEND_DUPLICATE_UNCONFIRMED（送らない・人の確定待ち）
    - 台帳が使えない（begin が None＝DB 未設定・記録失敗）→ **送らない**（fix2 BH-05・fail-closed・
      固定理由の件数ログのみ・次回実行で再試行）。時効側の fail-open（Q1a 限定仮置き）は変えない
    - push の例外（timeout 含む）→ finish(unconfirmed)・SEND_UNCONFIRMED（自動再送なし）
    - push 成功でも finish(sent) が applied でない（late／skipped）→ SEND_UNCONFIRMED（fix2 BH-06・
      写しを作らない・集計は unconfirmed・自動再送なし）"""
    admin_id = get_admin_line_user_id()
    if not admin_id:
        logger.warning("houki_jukuryo notice skipped (no admin id)")
        return SEND_FAILED
    token = send_ledger.bind_inbound(ledger_key(kind, business_key))
    try:
        with send_ledger.purpose(LEDGER_PURPOSE):
            op = await send_ledger.begin(LEDGER_BUSINESS, LEDGER_CHANNEL, admin_id, text)
        if op is send_ledger.DUPLICATE_SENT:
            return SEND_DUPLICATE_SENT
        if op is send_ledger.DUPLICATE_UNCONFIRMED:
            return SEND_DUPLICATE_UNCONFIRMED
        if op is None:
            logger.warning("houki_jukuryo notice skipped (ledger_unavailable) count=%s",
                           emit(1, "count", "log", "operator"))
            return SEND_LEDGER_UNAVAILABLE
        try:
            ok = await send_ledger.guarded_http(op, _push_admin_http(admin_id, text))
        except Exception:
            await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
            logger.warning("houki_jukuryo push result unknown (unconfirmed)")
            return SEND_UNCONFIRMED
        if not ok:
            await send_ledger.finish(op, send_ledger.STATE_FAILED)
            return SEND_FAILED
        applied = await send_ledger.finish(op, send_ledger.STATE_SENT)
        if applied != "applied":
            logger.warning("houki_jukuryo finish not applied (treated as unconfirmed)")
            return SEND_UNCONFIRMED
        return SEND_SENT
    finally:
        send_ledger.unbind(token)


# ── kintone 読取（全件取得）・書込（3 欄のみ・CAS） ─────────────────────────
async def fetch_all_targets() -> list[dict]:
    """受任後×未提出を $id asc・500 件ごとに全件取得（$id > 最後の id で継続）。"""
    out: list[dict] = []
    after: str | None = None
    while True:
        rows = await kintone.search_records(APP_HOUKI_CASE, search_query(after), fields=SEARCH_FIELDS)
        out.extend(rows)
        if len(rows) < SEARCH_LIMIT:
            return out
        last = _v(rows[-1], "$id")
        if not last.isdigit() or (after is not None and int(last) <= int(after)):
            logger.warning("houki_jukuryo paging stopped (id not advancing)")
            return out
        after = last


WRITE_WRITTEN = "written"
WRITE_NOOP = "noop"
WRITE_ABORTED = "aborted"        # BH-01: 409 再取得後に対象外になっていた
WRITE_FAILED = "failed"


async def commit_writes(record: dict, today: date, mark_days: list[int]) -> str:
    """算出差分＋刻印を 1 回の $revision CAS で保存。409 は最新を再取得して 1 回だけ
    再計算・再試行。BH-01: 再取得後に対象外（status 変更・申述提出日 入力・起算日_確定 空）
    になっていれば更新を中止（件数ログのみ・次回実行で改めて判定）。"""
    rid = _v(record, "$id")
    rec = record
    for attempt in range(2):
        fields = record_writes(rec, today, mark_days)
        if not fields:
            return WRITE_NOOP
        try:
            await kintone.update_record(APP_HOUKI_CASE, rid, fields,
                                        revision=_v(rec, "$revision") or None)
            return WRITE_WRITTEN
        except kintone.KintoneConflict:
            if attempt == 1:
                logger.warning("houki_jukuryo write CAS conflict twice record=%s",
                               emit(rid, "record_id", "log", "operator"))
                return WRITE_FAILED
            try:
                rec = await kintone.get_record(APP_HOUKI_CASE, rid)
            except kintone.KintoneError as e:
                logger.warning("houki_jukuryo refetch failed cls=%s record=%s",
                               emit(type(e).__name__, "vendor_raw", "log", "operator"),
                               emit(rid, "record_id", "log", "operator"))
                return WRITE_FAILED
            if not is_target(rec) or resolve_start(rec) is None:
                logger.info("houki_jukuryo write aborted after refetch (no longer target) count=%s",
                            emit(1, "count", "log", "operator"))
                return WRITE_ABORTED
        except kintone.KintoneError as e:
            logger.warning("houki_jukuryo write failed cls=%s record=%s",
                           emit(type(e).__name__, "vendor_raw", "log", "operator"),
                           emit(rid, "record_id", "log", "operator"))
            return WRITE_FAILED
    return WRITE_FAILED


# ── ジョブ本体 ───────────────────────────────────────────────────────────────
def _empty_result() -> dict:
    return {"targets": 0, "candidates": 0, "sent": 0, "duplicate": 0, "remark": 0,
            "unconfirmed": 0, "failed": 0, "ledger_unavailable": 0, "unset": 0, "unset_result": "",
            "written": 0, "write_failed": 0, "aborted": 0}


async def jukuryo_daily_check(run_hour: int = RUN_HOURS_JST[0]) -> dict:
    """受任後×未提出の案件を算出し、(1) 14/7 日前の各閾値を台帳で 1 回性判定して送信、
    (2) 送信済み（今回 sent・台帳 sent で刻印未了）の刻印と 熟慮期間期限／残日数 の差分を
    1 レコード 1 回で保存、(3) 起算日未確定の件数を台帳で 1 日 1 回通知。
    run_hour は 3 回のジョブで共通の処理（判定は欄と台帳で行うため時刻で分けない）。
    台帳が使えないときは通知を送らない（fail-closed・期限欄の保存は行う）。"""
    res = _empty_result()
    today = _today_jst()
    try:
        records = await fetch_all_targets()
    except kintone.KintoneError as e:
        logger.error("houki_jukuryo fetch failed cls=%s",
                     emit(type(e).__name__, "vendor_raw", "log", "operator"))
        return res
    res["targets"] = len(records)

    marks: dict[str, list[int]] = {}
    unset = 0
    for rec in records:
        if not is_target(rec):
            continue
        rid = _v(rec, "$id")
        comp = compute(rec, today)
        if comp is None:
            unset += 1
            continue
        notified = notified_values(rec)
        for day in alert_candidates(comp.remaining):
            res["candidates"] += 1
            marked = NOTIFIED_VALUES[day] in notified
            r = await send_notice(LEDGER_KINDS[day], alert_business_key(rid, comp),
                                  alert_text(today, rid, day, comp))
            if r == SEND_SENT:
                res["sent"] += 1
                marks.setdefault(rid, []).append(day)
            elif r == SEND_DUPLICATE_SENT:
                res["duplicate"] += 1
                if not marked:                       # 刻印未了 → 刻印だけ再試行（再通知しない）
                    res["remark"] += 1
                    marks.setdefault(rid, []).append(day)
            elif r in (SEND_DUPLICATE_UNCONFIRMED, SEND_UNCONFIRMED):
                res["unconfirmed"] += 1
            elif r == SEND_LEDGER_UNAVAILABLE:
                res["ledger_unavailable"] += 1
            else:
                res["failed"] += 1
    res["unset"] = unset

    for rec in records:
        if not is_target(rec):
            continue
        rid = _v(rec, "$id")
        if not record_writes(rec, today, marks.get(rid, [])):
            continue
        outcome = await commit_writes(rec, today, marks.get(rid, []))
        if outcome == WRITE_WRITTEN:
            res["written"] += 1
        elif outcome == WRITE_ABORTED:
            res["aborted"] += 1
        elif outcome == WRITE_FAILED:
            res["write_failed"] += 1

    if unset > 0:
        r = await send_notice(LEDGER_KIND_UNSET, today.isoformat(), unset_text(today, unset))
        res["unset_result"] = r
        if r == SEND_FAILED:
            logger.warning("houki_jukuryo unset-count notice not sent")

    logger.info("houki_jukuryo run: targets=%s candidates=%s sent=%s duplicate=%s remark=%s "
                "unconfirmed=%s failed=%s ledger_unavailable=%s unset=%s written=%s "
                "write_failed=%s aborted=%s",
                emit(res["targets"], "count", "log", "operator"),
                emit(res["candidates"], "count", "log", "operator"),
                emit(res["sent"], "count", "log", "operator"),
                emit(res["duplicate"], "count", "log", "operator"),
                emit(res["remark"], "count", "log", "operator"),
                emit(res["unconfirmed"], "count", "log", "operator"),
                emit(res["failed"], "count", "log", "operator"),
                emit(res["ledger_unavailable"], "count", "log", "operator"),
                emit(res["unset"], "count", "log", "operator"),
                emit(res["written"], "count", "log", "operator"),
                emit(res["write_failed"], "count", "log", "operator"),
                emit(res["aborted"], "count", "log", "operator"))
    return res


def job_name(hour: int) -> str:
    return f"{JOB_NAME}_{hour:02d}"


def register_houki_jukuryo_job() -> None:
    """FastAPI startup から呼ぶ（return_deadline と同方式・env なし）。
    RUN_HOURS_JST の各時刻に 1 ジョブずつ登録（hub/scheduler は 1 ジョブ 1 時刻）。"""
    for hour in RUN_HOURS_JST:
        name = job_name(hour)
        if not hub_scheduler.is_registered(name):
            hub_scheduler.register_daily(name, hour, functools.partial(jukuryo_daily_check, hour))
    hub_scheduler.start_all()
