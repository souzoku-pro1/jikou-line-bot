"""相続放棄 熟慮期間 監視（hub/houki_jukuryo・HOUKI-JUKURYO-2）

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
 - 通知済みの記録は App 40 の既存欄 通知済み閾値（CHECK_BOX・選択肢 14日前／7日前）で判定。
   同日に何度走っても、通知済みの値が立っている閾値は送らない。
 - 通知文は固定文言＋レコード番号＋期限＋残日数のみ（氏名等の PII を載せない・RV-10）。

書く欄は 熟慮期間期限・残日数（算出結果）と 通知済み閾値（送信成功後の刻印）の 3 つだけ。
他の欄・status は書かない。順序: 算出 → 通知送信 → 送信成功（sent/throttled）を確認してから
1 レコード 1 回の $revision CAS で保存（409 は再取得して 1 回だけ再計算・再試行）。
登録方式は hub/return_deadline と同じ（hub/scheduler の daily・8/13/18 JST・単一 worker 前提）。
新規 env は読まない。App 21 には触れない。hub/notify の切り詰め（4900 字）には触れない。
"""

import calendar
import functools
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from hub import kintone, notify
from hub import scheduler as hub_scheduler
from hub.houki_case_store import APP_HOUKI_CASE
from hub.houki_profile import HOUKI_PROFILE
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由

logger = logging.getLogger("hub.houki_jukuryo")

_JST = timezone(timedelta(hours=9))

# ── 定数（1 か所・テストで pin） ─────────────────────────────────────────────
JOB_NAME = "HOUKI_JUKURYO"                   # ジョブ名は f"{JOB_NAME}_{HH}"（時刻ごとに 1 つ）
RUN_HOURS_JST = (8, 13, 18)                  # CRON-1 から時刻を維持（票 6）
NOTIFY_KIND = "houki_jukuryo_daily"          # throttle_key = f"{NOTIFY_KIND}:{YYYY-MM-DD}:{digest}"
NOTICE_MAX_CHARS = 3800                      # 1 通の上限（notify 側 4900 より小さい）
DIGEST_HEX_LEN = 16

FIELD_STATUS = "status"
FIELD_SUBMITTED = "申述提出日"
FIELD_START = "起算日_確定"                   # 唯一の入力（司令塔裁定）
FIELD_DEADLINE = "熟慮期間期限"               # 出力 1（DATE・システム所有）
FIELD_REMAINING = "残日数"                    # 出力 2（NUMBER・システム所有）
FIELD_NOTIFIED = "通知済み閾値"               # 通知済みの刻印（CHECK_BOX・既存欄）
WRITE_FIELDS = frozenset({FIELD_DEADLINE, FIELD_REMAINING, FIELD_NOTIFIED})   # 書く欄の閉集合

# 対象 status（受任後の 8 値・hub/houki_profile が単一の正）
TARGET_STATUSES: tuple = tuple(sorted(HOUKI_PROFILE.post_engagement_statuses))

JUKURYO_MONTHS = 3                           # 裁定 (a)
ALERT_DAYS = (14, 7)                         # 裁定 (c)・残日数がこの値以下で未通知なら送る
NOTIFIED_VALUES = {14: "14日前", 7: "7日前"}  # 通知済み閾値 の実選択肢値（form fields 実測）

NOTICE_HEADER = "【相続放棄 熟慮期間】"
NOTICE_FOOTER = "期限＝起算日_確定の3か月後の応当日の前日。レコード番号と残日数のみ・詳細はApp 40で確認してください。"
UNSET_LABEL = "起算日未確定"
UNSET_NOTE = "（受任後・申述提出日なし・起算日_確定が空。個別のレコードはPWA/App 40で確認してください）"
SEARCH_FIELDS = ["$id", "$revision", FIELD_STATUS, FIELD_SUBMITTED, FIELD_START,
                 FIELD_DEADLINE, FIELD_REMAINING, FIELD_NOTIFIED]
SEARCH_LIMIT = 500                           # kintone records.json の上限（ページング）

# 起算日未確定の件数通知を送った日（プロセス内・1 日 1 回の判定。再起動で消えるが
# notify 側の同一キー throttle と合わせて同日重複を抑える）
_unset_notified_on: date | None = None


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


def alerts_due(remaining: int, notified: list[str]) -> list[int]:
    """残日数が閾値以下で、その閾値の通知済みが無いもの（当日に走れなかった分も翌日以降に
    拾う。期限超過でも未通知の閾値があれば送る＝取りこぼしを黙らせない）。"""
    return [d for d in ALERT_DAYS if remaining <= d and NOTIFIED_VALUES[d] not in notified]


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
    """1 レコードの書込集合 = 算出差分 + 送信成功した閾値の刻印（既存の刻印は保持）。"""
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


@dataclass
class Entry:
    record: dict
    record_id: str
    comp: Computation
    days: list[int]                        # 送る閾値（14／7）

    def lines(self) -> list[str]:
        return [format_alert_line(self.record_id, d, self.comp) for d in self.days]

    def facts(self) -> list[dict]:
        return [{"record_id": self.record_id, "alert_day": d,
                 "deadline": self.comp.deadline.isoformat(), "remaining": self.comp.remaining}
                for d in self.days]


@dataclass
class Notice:
    entries: list[Entry] = field(default_factory=list)
    text: str = ""
    digest: str = ""


def plan_alerts(records: list[dict], today: date) -> tuple[list[Entry], int]:
    """(通知対象 Entry, 起算日未確定の件数)。対象外レコードは無視。"""
    entries: list[Entry] = []
    unset = 0
    for rec in records:
        if not is_target(rec):
            continue
        comp = compute(rec, today)
        if comp is None:
            unset += 1
            continue
        days = alerts_due(comp.remaining, notified_values(rec))
        if days:
            entries.append(Entry(rec, _v(rec, "$id"), comp, days))
    return entries, unset


def digest_of(entries: list[Entry]) -> str:
    facts = sorted((f for e in entries for f in e.facts()),
                   key=lambda f: (f["record_id"], f["alert_day"]))
    material = json.dumps(facts, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:DIGEST_HEX_LEN]


def notify_key(today: date, digest: str) -> str:
    return f"{NOTIFY_KIND}:{today.isoformat()}:{digest}"


def unset_key(today: date) -> str:
    return f"{NOTIFY_KIND}:{today.isoformat()}:unset"


def _notice_text(today: date, bodies: list[str], n: int, total: int) -> str:
    head = f"{NOTICE_HEADER} {today.isoformat()} ({n}/{total})"
    return "\n".join([head, *bodies, NOTICE_FOOTER])


def build_notices(today: date, entries: list[Entry], max_chars: int | None = None) -> list[Notice]:
    """案件単位で NOTICE_MAX_CHARS 以内に分割。各通に (n/N)。"""
    limit = NOTICE_MAX_CHARS if max_chars is None else max_chars
    if not entries:
        return []
    frame = len(_notice_text(today, [], 99, 99))
    budget = max(limit - frame, 1)
    groups: list[list[tuple[Entry, str]]] = []
    cur: list[tuple[Entry, str]] = []
    cur_len = 0
    for e in entries:
        body = "\n".join(e.lines())
        add = len(body) + (1 if cur else 0)
        if cur and cur_len + add > budget:
            groups.append(cur)
            cur, cur_len = [], 0
            add = len(body)
        cur.append((e, body))
        cur_len += add
    if cur:
        groups.append(cur)
    total = len(groups)
    return [Notice([e for e, _b in g], _notice_text(today, [b for _e, b in g], i, total),
                   digest_of([e for e, _b in g])) for i, g in enumerate(groups, 1)]


def unset_text(today: date, count: int) -> str:
    return f"{NOTICE_HEADER} {today.isoformat()}\n{UNSET_LABEL} {count} 件{UNSET_NOTE}"


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


async def commit_writes(record: dict, today: date, mark_days: list[int]) -> bool:
    """算出差分＋刻印を 1 回の $revision CAS で保存。409 は最新を再取得して 1 回だけ
    再計算・再試行（起算日_確定 が同時に変わっていれば新しい値で算出）。失敗は False。"""
    rid = _v(record, "$id")
    rec = record
    for attempt in range(2):
        fields = record_writes(rec, today, mark_days)
        if not fields:
            return True
        try:
            await kintone.update_record(APP_HOUKI_CASE, rid, fields,
                                        revision=_v(rec, "$revision") or None)
            return True
        except kintone.KintoneConflict:
            if attempt == 1:
                logger.warning("houki_jukuryo write CAS conflict twice record=%s",
                               emit(rid, "record_id", "log", "operator"))
                return False
            try:
                rec = await kintone.get_record(APP_HOUKI_CASE, rid)
            except kintone.KintoneError as e:
                logger.warning("houki_jukuryo refetch failed cls=%s record=%s",
                               emit(type(e).__name__, "vendor_raw", "log", "operator"),
                               emit(rid, "record_id", "log", "operator"))
                return False
        except kintone.KintoneError as e:
            logger.warning("houki_jukuryo write failed cls=%s record=%s",
                           emit(type(e).__name__, "vendor_raw", "log", "operator"),
                           emit(rid, "record_id", "log", "operator"))
            return False
    return False


# ── ジョブ本体 ───────────────────────────────────────────────────────────────
async def jukuryo_daily_check(run_hour: int = RUN_HOURS_JST[0]) -> dict:
    """受任後×未提出の案件を算出し、(1) 14/7 日前の未通知分を送信、(2) 送信成功分の刻印と
    熟慮期間期限／残日数 の差分を 1 レコード 1 回で保存、(3) 起算日未確定の件数を 1 日 1 回通知。
    run_hour は 3 回のジョブで共通の処理（判定は欄の値で行うため時刻で分けない）。戻り値は件数。"""
    global _unset_notified_on
    today = _today_jst()
    try:
        records = await fetch_all_targets()
    except kintone.KintoneError as e:
        logger.error("houki_jukuryo fetch failed cls=%s",
                     emit(type(e).__name__, "vendor_raw", "log", "operator"))
        return {"targets": 0, "alerts": 0, "unset": 0, "notices": 0, "sent": 0,
                "failed": 0, "written": 0, "write_failed": 0, "unset_notified": False}

    entries, unset = plan_alerts(records, today)
    notices = build_notices(today, entries)
    sent = failed = 0
    marks: dict[str, list[int]] = {}
    for nt in notices:
        result = await notify.notify_admin_line_result(nt.text, throttle_key=notify_key(today, nt.digest))
        if result not in ("sent", "throttled"):
            failed += 1
            logger.warning("houki_jukuryo notice not sent (no mark written for that notice)")
            continue
        sent += 1
        for e in nt.entries:
            marks[e.record_id] = e.days

    written = write_failed = 0
    for rec in records:
        if not is_target(rec):
            continue
        rid = _v(rec, "$id")
        if not record_writes(rec, today, marks.get(rid, [])):
            continue
        if await commit_writes(rec, today, marks.get(rid, [])):
            written += 1
        else:
            write_failed += 1

    unset_notified = False
    if unset > 0 and _unset_notified_on != today:
        result = await notify.notify_admin_line_result(unset_text(today, unset),
                                                       throttle_key=unset_key(today))
        if result in ("sent", "throttled"):
            _unset_notified_on = today
            unset_notified = True
        else:
            logger.warning("houki_jukuryo unset-count notice not sent")

    logger.info("houki_jukuryo run: targets=%s alerts=%s unset=%s notices=%s sent=%s "
                "failed=%s written=%s write_failed=%s",
                emit(len(records), "count", "log", "operator"),
                emit(sum(len(e.days) for e in entries), "count", "log", "operator"),
                emit(unset, "count", "log", "operator"),
                emit(len(notices), "count", "log", "operator"),
                emit(sent, "count", "log", "operator"),
                emit(failed, "count", "log", "operator"),
                emit(written, "count", "log", "operator"),
                emit(write_failed, "count", "log", "operator"))
    return {"targets": len(records), "alerts": sum(len(e.days) for e in entries), "unset": unset,
            "notices": len(notices), "sent": sent, "failed": failed, "written": written,
            "write_failed": write_failed, "unset_notified": unset_notified}


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
