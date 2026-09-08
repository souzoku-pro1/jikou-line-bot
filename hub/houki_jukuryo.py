"""相続放棄 熟慮期間 日次監視（hub/houki_jukuryo・HOUKI-JUKURYO-CRON-1 / fix1 / fix2）

App 40（相続放棄案件）の受任案件について熟慮期間（3 か月）の期日を判定し、
迫った案件を LINE で弁護士へ通知する。**判定のみ**——期日欄・ステータス・
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

fix2（Codex HJC-01〜04）:
 A. 通知は案件行の集合として組み立て、1 通 NOTICE_MAX_CHARS 字以内に案件単位で分割
    （1 案件で超える行は要点へ縮約し「（縮約）」を明記・黙った切り捨てなし）。各通に
    「(n/N)」。送信キーは houki_jukuryo_daily:{日付}:{digest}（fix4: digest=その通の
    案件×マイルストーンの構造化データ {record_id, milestone_name, due_date, legal_deadline,
    internal_deadline, start_basis, delayed} を並べ替えた正規化 JSON の sha256 先頭 16 桁）
    ＝同一内容の再送だけが
    throttled=成功扱い。履歴追記は sent/throttled になった通の案件だけ。
 B. App 40 の全件取得（$id asc・500 件ごとに $id > 最後の id で継続）。
 C. マイルストーンを単発 ONE_SHOT（社内締切 7 日前／当日・法定満了 3 日前）と毎日 DAILY
    （法定満了 2 日前〜当日・満了超過）に分ける。ONE_SHOT は該当日が今日以前 7 日以内で
    履歴に同名かつ同じ該当日の行「{名}@{該当日}」が（日付を問わず）無ければ対象＝送信失敗の
    翌日以降も回収され、本文に「（遅延・本来 {該当日}）」を付ける（fix3: 履歴行は
    「{今日} {名}@{該当日} 通知済」。弁護士が期日欄を動かして該当日が変われば再通知）。ジョブは
    8:00・13:00・18:00 JST に登録し、各回は履歴の無い分だけを送る。起算日未設定の列挙は
    8:00 の回のみ。

冪等はレコードの履歴で判定する（in-memory なし・再起動・二重起動に耐える）。
順序（fix1）: 送信 → 送信成功（sent/throttled）を確認してから履歴追記。失敗した通の案件は
追記しない（次回実行で再対象）。追記の CAS 失敗は警告のみ（同日再実行の二重通知は許容）。
登録方式は hub/return_deadline と同じ（hub/scheduler の daily・単一 worker 前提）。
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
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由

logger = logging.getLogger("hub.houki_jukuryo")

_JST = timezone(timedelta(hours=9))

# ── 定数（1 か所・テストで pin） ─────────────────────────────────────────────
JOB_NAME = "HOUKI_JUKURYO"                   # ジョブ名は f"{JOB_NAME}_{HH}"（時刻ごとに 1 つ）
RUN_HOURS_JST = (8, 13, 18)                  # fix2 C-4: 同日再送（各回は履歴の無い分だけ）
UNSET_LIST_HOUR_JST = 8                      # fix2 C-5: 起算日未設定の列挙はこの回のみ
NOTIFY_KIND = "houki_jukuryo_daily"          # throttle_key = f"{NOTIFY_KIND}:{YYYY-MM-DD}:{digest}"
NOTICE_MAX_CHARS = 3800                      # fix2 A-1: 1 通の上限（notify 側 4900 より小さい）
DIGEST_HEX_LEN = 16

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
ONE_SHOT_RECOVERY_DAYS = 7                   # fix2 C-3: 単発の未送信回収窓（該当日 ≥ 今日−7）

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
ONE_SHOT = (MS_INTERNAL_PRE, MS_INTERNAL_DAY, MS_LEGAL_PRE)          # fix2 C-1
DAILY = (MS_LEGAL_2, MS_LEGAL_1, MS_LEGAL_DAY, MS_OVERDUE)
MILESTONES = ONE_SHOT + DAILY

HISTORY_SUFFIX = "通知済"
DELAY_MARK = "遅延・本来"                   # 本文「（遅延・本来 YYYY-MM-DD）」
HISTORY_DUE_SEP = "@"                        # fix3: ONE_SHOT 履歴「{今日} {名}@{該当日} 通知済」
COMPACT_MARK = "（縮約）"
NOTICE_HEADER = "【相続放棄 熟慮期間】"
NOTICE_FOOTER = "期日は申告欄からの計算を含みます。レコードの期日欄・ステータスは変更していません。"
NOTICE_EMPTY = "本日のマイルストーン該当なし"
SEARCH_FIELDS = ["$id", "$revision", FIELD_STATUS, FIELD_SUBMITTED, FIELD_HISTORY,
                 FIELD_LEGAL_DEADLINE, FIELD_INTERNAL_DEADLINE, *START_DATE_FIELDS]
SEARCH_LIMIT = 500                           # kintone records.json の上限（fix2 B: ページング）


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


@dataclass(frozen=True)
class Milestone:
    name: str
    due: date                  # 本来の該当日（DAILY は今日）

    def delayed(self, today: date) -> bool:
        return self.due < today


def milestones_for(today: date, dl: Deadlines) -> list[Milestone]:
    """今日に該当するマイルストーン（凍結 5・境界判定は不変）。
    ONE_SHOT は fix2 C-3 の回収窓（該当日 ≤ 今日 かつ 該当日 ≥ 今日−7 日）で拾う
    （履歴による除外は plan 側）。DAILY は当日一致のみ。複数同時可。"""
    if not dl.determinable:
        return []
    out: list[Milestone] = []
    earliest = today - timedelta(days=ONE_SHOT_RECOVERY_DAYS)
    for name, due in ((MS_INTERNAL_PRE, dl.internal - timedelta(days=INTERNAL_PRE_DAYS)),
                      (MS_INTERNAL_DAY, dl.internal),
                      (MS_LEGAL_PRE, dl.legal - timedelta(days=LEGAL_PRE_DAYS))):
        if earliest <= due <= today:
            out.append(Milestone(name, due))
    remaining = (dl.legal - today).days
    if 0 <= remaining <= LEGAL_DAILY_FROM_DAYS:
        out.append(Milestone({2: MS_LEGAL_2, 1: MS_LEGAL_1, 0: MS_LEGAL_DAY}[remaining], today))
    if remaining < 0:
        out.append(Milestone(MS_OVERDUE, today))
    return out


# ── 対象・履歴・本文（pure） ─────────────────────────────────────────────────
def is_target(record: dict) -> bool:
    """凍結 6（検索条件と二重に検査・防御的）。"""
    return _v(record, FIELD_STATUS) == STATUS_JUNIN and not _v(record, FIELD_SUBMITTED)


def search_query(after_id: str | None = None) -> str:
    cond = f'{FIELD_STATUS} in ("{STATUS_JUNIN}") and {FIELD_SUBMITTED} = ""'
    if after_id:
        cond += f" and $id > {int(after_id)}"
    return f"{cond} order by $id asc limit {SEARCH_LIMIT}"


def history_line(today: date, milestone: str, due: date | None = None) -> str:
    """履歴行（fix3）。DAILY／起算日未設定は「{今日} {名} 通知済」、ONE_SHOT は該当日を
    必ず含む「{今日} {名}@{該当日} 通知済」（遅延でも同形式・「本来」表記は @該当日 に統合）。"""
    label = f"{milestone}{HISTORY_DUE_SEP}{due.isoformat()}" if due is not None else milestone
    return f"{today.isoformat()} {label} {HISTORY_SUFFIX}"


def _history_tokens(history: str):
    """新形式のみ: (日付, 名[@該当日], 残り)。旧形式（fix2 まで）は本番未配備のため非対応。"""
    for ln in (history or "").splitlines():
        parts = ln.strip().split(" ", 2)
        if len(parts) >= 3:
            yield parts[0], parts[1], parts[2]


def history_has(history: str, today: date, milestone: str) -> bool:
    """同日・同名の履歴行があるか（DAILY／起算日未設定の除外）。"""
    return any(d == today.isoformat() and n == milestone and rest.startswith(HISTORY_SUFFIX)
               for d, n, rest in _history_tokens(history))


def history_has_one_shot(history: str, milestone: str, due: date) -> bool:
    """同名かつ同じ該当日（名@該当日）の履歴行が日付を問わず有るか（ONE_SHOT の除外・fix3）。
    弁護士が期日欄を動かして該当日が変われば別行になり再通知される。"""
    label = f"{milestone}{HISTORY_DUE_SEP}{due.isoformat()}"
    return any(n == label and rest.startswith(HISTORY_SUFFIX)
               for _d, n, rest in _history_tokens(history))


def append_history_text(history: str, line: str) -> str:
    return f"{history}\n{line}" if history else line


def _fmt(d: date | None) -> str:
    return d.isoformat() if d else "未設定"


def format_item(record_id: str, ms: Milestone, dl: Deadlines, today: date | None = None) -> str:
    """1 案件 1 行（個人情報なし: レコード番号・期日・根拠のみ）。"""
    if dl.start is not None:
        start_text = f"{_fmt(dl.start)}（{dl.start_source}・{DECLARED_NOTE}）"
    else:
        start_text = START_UNSET
    label = ms.name
    if today is not None and ms.delayed(today):
        label += f"（{DELAY_MARK} {ms.due.isoformat()}）"
    return (f"・No.{record_id} {label} / 法定満了 {_fmt(dl.legal)}（{dl.legal_source}）"
            f" / 社内締切 {_fmt(dl.internal)}（{dl.internal_source}）"
            f" / 起算日 {start_text}")


def format_item_compact(record_id: str, names: list[str], dl: Deadlines) -> str:
    """fix2 A-1: 1 案件の行が上限を超えるときの要点（レコード番号・マイルストーン・期日）。"""
    return (f"・No.{record_id} {'/'.join(names)} / 法定満了 {_fmt(dl.legal)}"
            f" / 社内締切 {_fmt(dl.internal)}{COMPACT_MARK}")


@dataclass
class Entry:
    """通知本文の案件単位（分割の最小単位）。"""
    record: dict
    record_id: str
    lines: list[str]                       # 本文行（1 マイルストーン 1 行）
    keys: list[tuple[str, str]]            # (record_id, マイルストーン名)
    history_lines: list[str]               # 送信成功後に追記する履歴行
    compact: str = ""                      # 上限超過時の縮約行（空なら不要）
    facts: list[dict] = field(default_factory=list)   # fix4: digest の材料（構造化・案件×マイルストーン）

    def text(self) -> str:
        return "\n".join(self.lines)


def start_basis(dl: Deadlines) -> str:
    """digest 用の起算日根拠: 使用欄名／弁護士設定（起算日空で 法定満了日 が弁護士設定）／起算日未設定。"""
    if dl.start is not None:
        return dl.start_source
    if dl.legal_source == SOURCE_ATTORNEY:
        return SOURCE_ATTORNEY
    return START_UNSET


def milestone_fact(record_id: str, name: str, due: date | None, dl: Deadlines, today: date) -> dict:
    """fix4 HJCF2-01: 案件×マイルストーンの構造化データ（該当日・期日・根拠・遅延を含む）。"""
    return {
        "record_id": record_id,
        "milestone_name": name,
        "due_date": due.isoformat() if due else None,
        "legal_deadline": dl.legal.isoformat() if dl.legal else None,
        "internal_deadline": dl.internal.isoformat() if dl.internal else None,
        "start_basis": start_basis(dl),
        "delayed": bool(due is not None and due < today),
    }


def plan_today(records: list[dict], today: date, include_unset: bool = True) -> list[Entry]:
    """当日の通知計画（pure・書込なし）。当日分（DAILY）／同名（ONE_SHOT）の履歴が
    既にあるものは除外（同日再実行・回収済みの二重防止）。"""
    entries: list[Entry] = []
    for rec in records:
        if not is_target(rec):
            continue
        rid = _v(rec, "$id")
        dl = compute_deadlines(rec)
        history = _v(rec, FIELD_HISTORY)
        if not dl.determinable:
            if include_unset and not history_has(history, today, START_UNSET):
                entries.append(Entry(rec, rid, [f"{START_UNSET}: No.{rid}"], [(rid, START_UNSET)],
                                     [history_line(today, START_UNSET)],
                                     facts=[milestone_fact(rid, START_UNSET, None, dl, today)]))
            continue
        pending = []
        for ms in milestones_for(today, dl):
            skip = (history_has_one_shot(history, ms.name, ms.due) if ms.name in ONE_SHOT
                    else history_has(history, today, ms.name))
            if not skip:
                pending.append(ms)
        if not pending:
            continue
        entries.append(Entry(
            rec, rid,
            [format_item(rid, ms, dl, today) for ms in pending],
            [(rid, ms.name) for ms in pending],
            [history_line(today, ms.name, ms.due if ms.name in ONE_SHOT else None) for ms in pending],
            compact=format_item_compact(rid, [ms.name for ms in pending], dl),
            facts=[milestone_fact(rid, ms.name, ms.due, dl, today) for ms in pending]))
    return entries


@dataclass
class Notice:
    entries: list[Entry] = field(default_factory=list)
    text: str = ""
    digest: str = ""


def digest_material(entries: list[Entry]) -> str:
    """fix4 HJCF2-01: 通に含まれる案件×マイルストーンの構造化データを record_id・milestone_name で
    並べ替え、正規化 JSON にする（文字列連結は使わない・HCG-03 と同方針）。"""
    facts = sorted((f for e in entries for f in e.facts),
                   key=lambda f: (f["record_id"], f["milestone_name"]))
    return json.dumps(facts, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest_of(entries: list[Entry]) -> str:
    """該当日・法定満了日・社内締切日・根拠・遅延のいずれかが変われば別値（履歴で未送信と
    判定される変更は必ず digest も変わる）。案件順の入替では同値。"""
    return hashlib.sha256(digest_material(entries).encode("utf-8")).hexdigest()[:DIGEST_HEX_LEN]


def notify_key(today: date, digest: str) -> str:
    return f"{NOTIFY_KIND}:{today.isoformat()}:{digest}"


def _notice_text(today: date, bodies: list[str], n: int, total: int) -> str:
    head = f"{NOTICE_HEADER} {today.isoformat()} ({n}/{total})"
    return "\n".join([head, *(bodies or [NOTICE_EMPTY]), NOTICE_FOOTER])


def build_notices(today: date, entries: list[Entry],
                  max_chars: int | None = None) -> list[Notice]:
    """fix2 A-1: 案件単位で NOTICE_MAX_CHARS 以内に分割。1 案件で超える行は縮約
    （COMPACT_MARK を明記・黙った切り捨てなし）。各通に (n/N)。"""
    limit = NOTICE_MAX_CHARS if max_chars is None else max_chars
    if not entries:
        return []
    frame = len(_notice_text(today, [], 99, 99))          # 見出し+空+末尾の器（最大幅）
    budget = max(limit - frame, 1)
    groups: list[list[tuple[Entry, str]]] = []
    cur: list[tuple[Entry, str]] = []
    cur_len = 0
    for e in entries:
        body = e.text()
        if len(body) > budget and e.compact:
            body = e.compact
        if len(body) > budget:                             # 縮約後も超える＝要点だけ
            body = (f"・No.{e.record_id} {'/'.join(n for _r, n in e.keys)}{COMPACT_MARK}")[:budget]
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
    out: list[Notice] = []
    for i, g in enumerate(groups, 1):
        ents = [e for e, _b in g]
        out.append(Notice(ents, _notice_text(today, [b for _e, b in g], i, total), digest_of(ents)))
    return out


# ── kintone 読取（fix2 B: 全件取得）・書込（履歴欄のみ・CAS） ─────────────────
async def fetch_all_targets() -> list[dict]:
    """受任×未提出を $id asc・500 件ごとに全件取得（$id > 最後の id で継続）。
    共通 search_records は変更しない。"""
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


async def append_history(record: dict, line: str) -> bool:
    """熟慮期間通知履歴 へ 1 行を $revision CAS で追記。409・その他失敗は False。
    他の欄には書かない。"""
    return await append_history_lines(record, [line])


async def append_history_lines(record: dict, lines: list[str]) -> bool:
    """熟慮期間通知履歴 へ複数行を 1 回の $revision CAS 更新で追記（同一案件の
    同日分をまとめる）。409・その他失敗は False。他の欄には書かない。"""
    rid = _v(record, "$id")
    new_text = _v(record, FIELD_HISTORY)
    for line in lines:
        new_text = append_history_text(new_text, line)
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


# ── ジョブ本体 ───────────────────────────────────────────────────────────────
async def jukuryo_daily_check(run_hour: int = UNSET_LIST_HOUR_JST) -> dict:
    """受任×未提出の案件を判定し、当回の未送信分を分割送信する（fix1: 送信 → 成功
    確認 → 履歴追記。fix2: 通ごとに digest キーで送り、sent/throttled の通の案件だけ
    履歴追記）。起算日未設定の列挙は run_hour == UNSET_LIST_HOUR_JST の回のみ。
    戻り値は件数（テスト用）。"""
    today = _today_jst()
    try:
        records = await fetch_all_targets()
    except kintone.KintoneError as e:
        logger.error("houki_jukuryo fetch failed cls=%s",
                     emit(type(e).__name__, "vendor_raw", "log", "operator"))
        return {"targets": 0, "items": 0, "unset": 0, "notices": 0, "sent": 0,
                "failed": 0, "history_failed": 0}

    entries = plan_today(records, today, include_unset=(run_hour == UNSET_LIST_HOUR_JST))
    notices = build_notices(today, entries)
    sent = failed = history_failed = 0
    for nt in notices:
        result = await notify.notify_admin_line_result(nt.text, throttle_key=notify_key(today, nt.digest))
        if result not in ("sent", "throttled"):
            failed += 1
            logger.warning("houki_jukuryo notice not sent (history not written for that notice)")
            continue
        sent += 1
        for e in nt.entries:
            if not await append_history_lines(e.record, e.history_lines):
                history_failed += 1
                logger.warning("houki_jukuryo history append failed after notice record=%s",
                               emit(e.record_id, "record_id", "log", "operator"))
    n_items = sum(len(e.keys) for e in entries if e.keys[0][1] != START_UNSET)
    n_unset = sum(1 for e in entries if e.keys[0][1] == START_UNSET)
    logger.info("houki_jukuryo run: targets=%s items=%s unset=%s notices=%s sent=%s "
                "failed=%s history_failed=%s",
                emit(len(records), "count", "log", "operator"),
                emit(n_items, "count", "log", "operator"),
                emit(n_unset, "count", "log", "operator"),
                emit(len(notices), "count", "log", "operator"),
                emit(sent, "count", "log", "operator"),
                emit(failed, "count", "log", "operator"),
                emit(history_failed, "count", "log", "operator"))
    return {"targets": len(records), "items": n_items, "unset": n_unset, "notices": len(notices),
            "sent": sent, "failed": failed, "history_failed": history_failed}


def job_name(hour: int) -> str:
    return f"{JOB_NAME}_{hour:02d}"


def register_houki_jukuryo_job() -> None:
    """FastAPI startup から呼ぶ（return_deadline と同方式・env なし）。
    fix2 C-4: RUN_HOURS_JST の各時刻に 1 ジョブずつ登録（hub/scheduler は 1 ジョブ 1 時刻）。"""
    for hour in RUN_HOURS_JST:
        name = job_name(hour)
        if not hub_scheduler.is_registered(name):
            hub_scheduler.register_daily(name, hour, functools.partial(jukuryo_daily_check, hour))
    hub_scheduler.start_all()
