"""相続放棄 熟慮期間 日次監視（hub/houki_jukuryo・HOUKI-JUKURYO-CRON-1）

App 40（相続放棄案件）の受任案件について熟慮期間（3 か月）の期日を日次で判定し、
迫った案件を 1 日 1 通の LINE で弁護士へ通知する。**判定のみ**——期日欄・ステータス・
その他の欄は一切書かない。書くのは 熟慮期間通知履歴（MULTI_LINE_TEXT）だけ。

弁護士確定（凍結・改変禁止）:
 1. 起算日 = 相続人と知った日_申告 → 空なら 死亡を知った日_申告 → 空なら 死亡日_申告。
    いずれも空なら「起算日未設定」。通知本文に使用欄と「申告ベース・要確認」を明記。
 2. 法定満了日（計算値）= 起算日の 3 か月後の応当日。応当日が無い月は月末。
 3. 社内締切日（計算値）= 法定満了日の 10 日前。
 4. 弁護士が 法定満了日／社内締切日 の欄に値を入れていればその値を優先（計算値で
    上書きしない・欄には書かない）。本文に「弁護士設定」か「計算値」かを明記。
 5. 通知タイミング: 社内締切 7 日前／社内締切当日／法定満了 3 日前／法定満了 2 日前〜
    当日は毎日／満了日超過は毎日（「満了超過」）。
 6. 対象: status = 受任 かつ 申述提出日 が空 のレコードのみ。

冪等（再起動・二重起動に耐える）: 熟慮期間通知履歴 に「{YYYY-MM-DD} {マイルストーン} 通知済」
を $revision CAS で追記し、同日同マイルストーンが既にあれば送らない。追記に失敗
（409 等）したら送らない（fail-closed・翌日再判定）。in-memory の判定は持たない。

登録方式は hub/return_deadline と同じ（hub/scheduler の daily・毎朝 8:00 JST・単一 worker
前提）。新規 env は読まない。App 21 には触れない。
"""

import calendar
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from hub import kintone, notify
from hub import scheduler as hub_scheduler
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由

logger = logging.getLogger("hub.houki_jukuryo")

_JST = timezone(timedelta(hours=9))

# ── 定数（1 か所・テストで pin） ─────────────────────────────────────────────
JOB_NAME = "HOUKI_JUKURYO"
HOUR_JST = 8
NOTIFY_KIND = "houki_jukuryo_daily"          # throttle_key = f"{NOTIFY_KIND}:{YYYY-MM-DD}"

FIELD_STATUS = "status"                      # 票の「相談状況」= App 40 実コード status（label ステータス）
STATUS_JUNIN = "受任"
FIELD_SUBMITTED = "申述提出日"
FIELD_HISTORY = "熟慮期間通知履歴"
FIELD_LEGAL_DEADLINE = "法定満了日"
FIELD_INTERNAL_DEADLINE = "社内締切日"
START_DATE_FIELDS = ("相続人と知った日_申告", "死亡を知った日_申告", "死亡日_申告")   # 代用順（凍結 1）

JUKURYO_MONTHS = 3                           # 凍結 2
INTERNAL_MARGIN_DAYS = 10                    # 凍結 3
INTERNAL_PRE_DAYS = 7                        # 凍結 5: 社内締切 7 日前
LEGAL_PRE_DAYS = 3                           # 凍結 5: 法定満了 3 日前
LEGAL_DAILY_FROM_DAYS = 2                    # 凍結 5: 法定満了 2 日前〜当日は毎日

SOURCE_ATTORNEY = "弁護士設定"
SOURCE_COMPUTED = "計算値"
START_UNSET = "起算日未設定"
DECLARED_NOTE = "申告ベース・要確認"

MS_INTERNAL_PRE = "社内締切7日前"
MS_INTERNAL_DAY = "社内締切当日"
MS_LEGAL_PRE = "法定満了3日前"
MS_LEGAL_2 = "法定満了2日前"
MS_LEGAL_1 = "法定満了1日前"
MS_LEGAL_DAY = "法定満了当日"
MS_OVERDUE = "満了超過"
MILESTONES = (MS_INTERNAL_PRE, MS_INTERNAL_DAY, MS_LEGAL_PRE,
              MS_LEGAL_2, MS_LEGAL_1, MS_LEGAL_DAY, MS_OVERDUE)

HISTORY_SUFFIX = "通知済"
NOTICE_HEADER = "【相続放棄 熟慮期間】"
SEARCH_FIELDS = ["$id", "$revision", FIELD_STATUS, FIELD_SUBMITTED, FIELD_HISTORY,
                 FIELD_LEGAL_DEADLINE, FIELD_INTERNAL_DEADLINE, *START_DATE_FIELDS]
SEARCH_LIMIT = 500


def _today_jst() -> date:
    return datetime.now(_JST).date()


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _parse_date(text: str) -> date | None:
    try:
        return date.fromisoformat(text) if text else None
    except ValueError:
        return None


# ── 期日計算（pure） ─────────────────────────────────────────────────────────
def add_months(d: date, months: int) -> date:
    """d の months か月後の応当日。応当日が無い月はその月の末日（凍結 2）。"""
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last))


def resolve_start(record: dict) -> tuple[date | None, str]:
    """起算日と使用欄（凍結 1）。全て空なら (None, START_UNSET)。"""
    for code in START_DATE_FIELDS:
        d = _parse_date(_v(record, code))
        if d is not None:
            return d, code
    return None, START_UNSET


@dataclass(frozen=True)
class Deadlines:
    start: date | None
    start_source: str          # 使用欄コード or START_UNSET
    legal: date | None
    legal_source: str          # SOURCE_ATTORNEY / SOURCE_COMPUTED / START_UNSET
    internal: date | None
    internal_source: str

    @property
    def determinable(self) -> bool:
        return self.legal is not None and self.internal is not None


def compute_deadlines(record: dict) -> Deadlines:
    """凍結 1〜4 の適用。欄には書かない（戻り値のみ）。

    法定満了日 欄に弁護士の値があればそれを優先し、無ければ起算日から計算。
    社内締切日 欄に値があればそれを優先し、無ければ法定満了日の 10 日前。
    起算日が無く 法定満了日 欄も空なら判定不能（START_UNSET）。"""
    start, start_source = resolve_start(record)
    legal_set = _parse_date(_v(record, FIELD_LEGAL_DEADLINE))
    if legal_set is not None:
        legal, legal_source = legal_set, SOURCE_ATTORNEY
    elif start is not None:
        legal, legal_source = add_months(start, JUKURYO_MONTHS), SOURCE_COMPUTED
    else:
        legal, legal_source = None, START_UNSET
    internal_set = _parse_date(_v(record, FIELD_INTERNAL_DEADLINE))
    if internal_set is not None:
        internal, internal_source = internal_set, SOURCE_ATTORNEY
    elif legal is not None:
        internal, internal_source = legal - timedelta(days=INTERNAL_MARGIN_DAYS), SOURCE_COMPUTED
    else:
        internal, internal_source = None, START_UNSET
    return Deadlines(start, start_source, legal, legal_source, internal, internal_source)


def milestones_for(today: date, dl: Deadlines) -> list[str]:
    """当日に該当するマイルストーン名（凍結 5）。前日・翌日は非該当。複数同時可。"""
    if not dl.determinable:
        return []
    out: list[str] = []
    if today == dl.internal - timedelta(days=INTERNAL_PRE_DAYS):
        out.append(MS_INTERNAL_PRE)
    if today == dl.internal:
        out.append(MS_INTERNAL_DAY)
    if today == dl.legal - timedelta(days=LEGAL_PRE_DAYS):
        out.append(MS_LEGAL_PRE)
    remaining = (dl.legal - today).days
    if 0 <= remaining <= LEGAL_DAILY_FROM_DAYS:
        out.append({2: MS_LEGAL_2, 1: MS_LEGAL_1, 0: MS_LEGAL_DAY}[remaining])
    if remaining < 0:
        out.append(MS_OVERDUE)
    return out


# ── 対象・履歴・本文（pure） ─────────────────────────────────────────────────
def is_target(record: dict) -> bool:
    """凍結 6（検索条件と二重に検査・防御的）。"""
    return _v(record, FIELD_STATUS) == STATUS_JUNIN and not _v(record, FIELD_SUBMITTED)


def search_query() -> str:
    return (f'{FIELD_STATUS} in ("{STATUS_JUNIN}") and {FIELD_SUBMITTED} = "" '
            f'order by $id asc limit {SEARCH_LIMIT}')


def history_line(today: date, milestone: str) -> str:
    return f"{today.isoformat()} {milestone} {HISTORY_SUFFIX}"


def history_has(history: str, today: date, milestone: str) -> bool:
    line = history_line(today, milestone)
    return any(ln.strip() == line for ln in (history or "").splitlines())


def append_history_text(history: str, line: str) -> str:
    return f"{history}\n{line}" if history else line


def _fmt(d: date | None) -> str:
    return d.isoformat() if d else "未設定"


def format_item(record_id: str, milestone: str, dl: Deadlines) -> str:
    """1 案件 1 行（個人情報なし: レコード番号・期日・根拠のみ）。"""
    if dl.start is not None:
        start_text = f"{_fmt(dl.start)}（{dl.start_source}・{DECLARED_NOTE}）"
    else:
        start_text = START_UNSET
    return (f"・No.{record_id} {milestone} / 法定満了 {_fmt(dl.legal)}（{dl.legal_source}）"
            f" / 社内締切 {_fmt(dl.internal)}（{dl.internal_source}）"
            f" / 起算日 {start_text}")


def build_notice(today: date, items: list[str], unset_ids: list[str]) -> str:
    lines = [f"{NOTICE_HEADER} {today.isoformat()}"]
    if items:
        lines.extend(items)
    else:
        lines.append("本日のマイルストーン該当なし")
    if unset_ids:
        lines.append(f"{START_UNSET}: " + "、".join(f"No.{r}" for r in unset_ids))
    lines.append("期日は申告欄からの計算を含みます。レコードの期日欄・ステータスは変更していません。")
    return "\n".join(lines)


# ── kintone 書込（履歴欄のみ・CAS） ──────────────────────────────────────────
async def append_history(record: dict, line: str) -> bool:
    """熟慮期間通知履歴 へ 1 行を $revision CAS で追記。409・その他失敗は False
    （送らない＝fail-closed。翌日の再判定に任せる）。他の欄には書かない。"""
    rid = _v(record, "$id")
    new_text = append_history_text(_v(record, FIELD_HISTORY), line)
    try:
        await kintone.update_record(APP_HOUKI_CASE, rid, {FIELD_HISTORY: new_text},
                                    revision=_v(record, "$revision") or None)
    except kintone.KintoneConflict:
        logger.warning("houki_jukuryo history CAS conflict record=%s",
                       emit(rid, "record_id", "log", "operator"))
        return False
    except kintone.KintoneError as e:
        logger.warning("houki_jukuryo history write failed cls=%s record=%s",
                       emit(type(e).__name__, "vendor_raw", "log", "operator"),
                       emit(rid, "record_id", "log", "operator"))
        return False
    return True


# ── 日次ジョブ ───────────────────────────────────────────────────────────────
async def jukuryo_daily_check() -> dict:
    """受任×未提出の案件を判定し、当日分を 1 通で通知する。戻り値は件数（テスト用）。"""
    today = _today_jst()
    try:
        records = await kintone.search_records(APP_HOUKI_CASE, search_query(), fields=SEARCH_FIELDS)
    except kintone.KintoneError as e:
        logger.error("houki_jukuryo fetch failed cls=%s",
                     emit(type(e).__name__, "vendor_raw", "log", "operator"))
        return {"targets": 0, "items": 0, "unset": 0, "sent": False}

    items: list[str] = []
    unset_ids: list[str] = []
    for rec in records:
        if not is_target(rec):
            continue
        rid = _v(rec, "$id")
        dl = compute_deadlines(rec)
        history = _v(rec, FIELD_HISTORY)
        if not dl.determinable:
            if history_has(history, today, START_UNSET):
                continue
            if await append_history(rec, history_line(today, START_UNSET)):
                unset_ids.append(rid)
            continue
        pending = [m for m in milestones_for(today, dl) if not history_has(history, today, m)]
        if not pending:
            continue
        history_after = history
        for ms in pending:
            line = history_line(today, ms)
            rec_now = {**rec, FIELD_HISTORY: {"value": history_after}}
            if not await append_history(rec_now, line):
                break                                  # fail-closed: 以降の同案件分も送らない
            history_after = append_history_text(history_after, line)
            rec["$revision"] = {"value": str(int(_v(rec, "$revision") or 0) + 1)}
            items.append(format_item(rid, ms, dl))

    sent = False
    if items or unset_ids:
        sent = await notify.notify_admin_line(
            build_notice(today, items, unset_ids),
            throttle_key=f"{NOTIFY_KIND}:{today.isoformat()}")
        if not sent:
            logger.warning("houki_jukuryo daily notice not sent")
    logger.info("houki_jukuryo daily: targets=%s items=%s unset=%s",
                emit(len(records), "count", "log", "operator"),
                emit(len(items), "count", "log", "operator"),
                emit(len(unset_ids), "count", "log", "operator"))
    return {"targets": len(records), "items": len(items), "unset": len(unset_ids), "sent": sent}


def register_houki_jukuryo_job() -> None:
    """FastAPI startup から呼ぶ（return_deadline と同方式・env なし・毎朝 8:00 JST）。"""
    if not hub_scheduler.is_registered(JOB_NAME):
        hub_scheduler.register_daily(JOB_NAME, HOUR_JST, jukuryo_daily_check)
    hub_scheduler.start_all()
