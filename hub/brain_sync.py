"""brain_sync — BRAIN-A1-LEDGER-1: kintone → 台帳の同期（v3 §5・BD-05/12/17/18）

対象:
- App 40 全レコード（案件そのもの・converter app40_record/1）
- App 30 のうち 案件アプリID = APP_HOUKI の値 の行（判定は brain_link: 案件レコードID
  が数字で App 40 に実在し ユニット種別=相続放棄 のみ auto。他は保留＝紐付け待ち）。
  fact の subject は shipping:{App 30 レコード番号}（R9・発送ごとに別 subject）
- App 28 のうち R5 の行（category が相続放棄の閉集合、line_user_id が**正本** App 40
  の LINEユーザーID にちょうど 1 件一致・BA-04）。case_event（種別=会話）として
  登録し fact にはしない（R9）。それ以外は取り込まない

カーソル（§5・BA-09）: 更新日時 + レコード番号で並べ（page_order 固定）、走査上限
（scan_upper_bound）を取得条件 更新日時 <= 上限 に含める。ページ単位で台帳書込と
カーソル前進（page_position）を同一トランザクションで行い、失敗したページを飛び越えて
進めない。ページ上限で未完了になった走査は次回 page_position から同じ上限で再開し、
完了したときだけ confirmed_until（確認済み範囲）を上限へ進め、次回は
「前回位置 − 重複窓（10 分）」から新しい上限で取り直す。kintone/DB 障害で終わった
走査は再開位置を捨てて窓から取り直す（取りこぼしより重複を選ぶ）。

冪等: 取込の冪等キー既知（同じ案件・現在 fact/event あり）なら再処理しない。
欄の消去は新 fact（空値）+ supersedes。取得失敗は「値なし」に変換しない。
サブテーブル行の消去（BA-03）は完全取得した新版でのみ判定（brain_ledger 側）。
関連喪失（R10）: 参照消去・別アプリへの移動・不正参照・R5 不成立は
brain_link.link_change_for → brain_ledger の共通処理へ（通常同期・再照合とも）。
追跡再照合（BA-05）: source_ingest の出典を record 番号順に上限件数ずつ再取得し、
継続位置を sync_cursor(kind=recheck) に持って全件を巡回する。一巡完了で
recheck_completed_at。App 28 も対象。
定期照合（1 日 1 回）: revision 一覧を取り直して差分を検出し取り込む（3 対象）。

実行管理: 外部 API 待機中に DB トランザクションを持たない（ページを取り切って
から 1 トランザクション）。DB 障害は本 module 内で握り、既存業務へ伝播させない。
有効化: BRAIN_SYNC_ENABLED（既定 OFF・R7）。scheduler へは常に登録し、coro の
先頭で検査（OFF なら即 return・ログ 1 行）。kintone への書込ゼロ（読取のみ）。

ログ規律: 固定語彙・件数・レコード番号のみ（hub.redact.emit 経由）。
氏名・値・locator の中身は出さない。
"""

import datetime
import logging
import os
import re

import config
from hub import brain_ledger as ledger
from hub import brain_link
from hub import kintone
from hub import scheduler as hub_scheduler
from hub.redact import emit

logger = logging.getLogger("hub.brain_sync")

_FLAG_TRUE = ("1", "true", "on", "yes")
JOB_NAME = "BRAIN_SYNC"
INTERVAL_MINUTES = 5.0
WINDOW_MINUTES = 10
PAGE_SIZE = 100
MAX_PAGES_PER_RUN = 200
RECONCILE_INTERVAL = datetime.timedelta(hours=24)
RECHECK_INTERVAL = datetime.timedelta(hours=24)
RECHECK_BATCH = 100
_IN_CHUNK = 50

TARGET_APP40 = "app40"
TARGET_APP30 = "app30"
TARGET_APP28 = "app28"
TARGETS = (TARGET_APP40, TARGET_APP30, TARGET_APP28)
CONVERTER = {TARGET_APP40: ("app40_record", "1"),
             TARGET_APP30: ("app30_record", "1"),
             TARGET_APP28: ("app28_row", "2")}

APP_HOUKI = kintone.KintoneApp("App 40 (相続放棄案件)", "APP_HOUKI", "TOKEN_HOUKI")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")
APP_CHATLOG = kintone.KintoneApp("App 28 (チャットログ)", "APP_CHATLOG", "TOKEN_CHATLOG")

# R5: App 28 の相続放棄カテゴリ閉集合（定数化）。会話行のみ（マーカー行は対象外）
HOUKI_CHAT_CATEGORIES = frozenset({"相続放棄ヒアリング", "相続放棄ヒアリング・要確認"})
HOUKI_CHAT_CATEGORY_PREFIXES = ("画像解析:houki:",)
APP40_LINE_ID_FIELD = "LINEユーザーID"
APP40_LINE_ID_ITEM = ledger.APP40_PREFIX + APP40_LINE_ID_FIELD

# case_event の要約に使う閉集合（値は非 PII の選択肢のみ・閉集合外は other）
APP40_STATUS_OPTIONS = frozenset({
    "問い合わせ", "電話判断待ち", "電話調整中", "決済待ち", "契約待ち", "受任",
    "書類収集中", "申述書作成", "裁判所提出済", "照会書対応", "受理", "債権者通知",
    "完了", "不受任", "辞任"})
APP30_STATUS_OPTIONS = frozenset({
    "下書き", "承認待ち", "承認済", "発送処理中", "発送済", "返送待ち", "完了",
    "エラー", "却下", "要確認"})
APP30_SHIPPED_STATES = frozenset({"発送済", "返送待ち", "完了"})
CHAT_ROLES = frozenset({"user", "assistant", "staff", "system"})
EVENT_CASE_STATUS = "case_status_observed"
EVENT_SHIPPING_STATUS = "shipping_status_observed"
EVENT_SHIPPED_AT_SYNC = "shipping_confirmed_at_sync"
EVENT_CHAT = "chat_message"                      # R9: 会話（App 28 レコード 1 件＝1 出来事）

_KINTONE_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_DIGITS_RE = re.compile(r"^[0-9]{1,10}$")
_LINE_ID_RE = re.compile(r"^U[0-9a-f]{32}$")

FAIL_KINTONE = "kintone_fetch_failed"
FAIL_DB = "db_write_failed"
FAIL_MISMATCH = "mismatch_hold"
FAIL_PAGE_LIMIT = "page_limit_reached"
# R12: _prepare_ingest の戻り値「判定不能」（処理不要 None と区別する番兵）
PENDING_RECHECK = ledger.FLAG_PENDING_RECHECK
# R13: 既知の旧 revision による処理省略（復旧の契機にしない・None と区別する番兵）
STALE_KNOWN = "stale_known"


def brain_sync_enabled() -> bool:
    """flag BRAIN_SYNC_ENABLED（既定 OFF・値集合は既存 flag 群と同一・R7）。"""
    return os.environ.get(config.BRAIN_SYNC_ENABLED_ENV, "").strip().lower() in _FLAG_TRUE


def source_targets() -> dict:
    """出典アプリ ID → カーソル対象名（鮮度集約・BA-07 で使う）。"""
    return {APP_HOUKI.app_id(): TARGET_APP40, APP_SHIPPING.app_id(): TARGET_APP30,
            APP_CHATLOG.app_id(): TARGET_APP28}


# ── 変換器（record → facts / events） ───────────────────────────────────────

def _v(record: dict, code: str):
    return (record.get(code) or {}).get("value")


def _s(record: dict, code: str) -> str:
    return str(_v(record, code) or "").strip()


def _revision(record: dict) -> int:
    try:
        return int(_s(record, "$revision") or "0")
    except ValueError:
        return 0


def _parse_dt(text: str):
    if not text or not _KINTONE_DT_RE.fullmatch(text):
        return None
    return datetime.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=datetime.timezone.utc)


def _fmt_dt(dt: datetime.datetime) -> str:
    return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


_APP40_TABLES = ((ledger.APP40_CREDITOR_TABLE, ledger.APP40_CREDITOR_COLUMNS,
                  ledger.SUBJECT_CREDITOR_PREFIX),
                 (ledger.APP40_DOCUMENT_TABLE, ledger.APP40_DOCUMENT_COLUMNS,
                  ledger.SUBJECT_DOCUMENT_PREFIX))


def _row_ids(record: dict, table: str) -> set | None:
    """サブテーブルの行 ID 集合。表の欄がレコードに無い（fields 絞り込み・部分取得）
    ときは None＝「完全に取得できていない」として消去判定に使わない（BA-03）。"""
    if table not in record:
        return None
    ids = set()
    for row in (_v(record, table) or []):
        row_id = str((row or {}).get("id") or "").strip()
        if not _DIGITS_RE.fullmatch(row_id):
            return None                      # 行 ID の無い行がある＝比較できない
        ids.add(row_id)
    return ids


def app40_subjects_seen(record: dict) -> dict:
    """BA-03: 新版に存在するサブテーブル subject（接頭辞 → 行 ID 集合／None）。"""
    return {prefix: _row_ids(record, table) for table, _cols, prefix in _APP40_TABLES}


def convert_app40(record: dict) -> tuple:
    """App 40 レコード → (SourceRef, case_key, facts, events)。案件キー = 自分自身。"""
    rid = _s(record, "$id")
    app_id = APP_HOUKI.app_id()
    src = ledger.SourceRef(app_id, rid, _revision(record), *CONVERTER[TARGET_APP40],
                           updated_at=_s(record, "更新日時"))
    facts = []
    for field, vtype in ledger.app40_fields().items():
        if field not in record:
            continue                         # 欄が無い＝部分取得。消去とは判断しない（BA-03）
        facts.append(ledger.FactIn(
            ledger.app40_field_subject(field), ledger.APP40_PREFIX + field, vtype,
            _v(record, field), ledger.make_locator(field),
            ledger.app40_field_confidence(field)))
    for table, columns, prefix in _APP40_TABLES:
        for row in (_v(record, table) or []):
            row_id = str((row or {}).get("id") or "").strip()
            if not _DIGITS_RE.fullmatch(row_id):
                continue                     # 行 ID の無い行は subject を作れない
            values = (row or {}).get("value") or {}
            for col, vtype in columns.items():
                facts.append(ledger.FactIn(
                    prefix + row_id, f"{ledger.APP40_PREFIX}{table}.{col}", vtype,
                    ((values.get(col) or {}).get("value")),
                    ledger.make_locator(f"{table}.{col}", row_id), "high"))
    status = _s(record, "status")
    events = [ledger.EventIn(
        EVENT_CASE_STATUS,
        "status:" + (status if status in APP40_STATUS_OPTIONS else "other"),
        ledger.make_locator("status"), occurred_at=_parse_dt(_s(record, "更新日時")))]
    return src, (app_id, rid), facts, events


def _app30_source(record: dict) -> ledger.SourceRef:
    return ledger.SourceRef(APP_SHIPPING.app_id(), _s(record, "$id"), _revision(record),
                            *CONVERTER[TARGET_APP30], updated_at=_s(record, "更新日時"))


def convert_app30(record: dict, decision: brain_link.LinkDecision) -> tuple:
    """App 30 レコード → (SourceRef, case_key|None, facts, events)。
    subject は shipping:{レコード番号}（R9: 同じ発送の同じ項目だけを競合判定する）。"""
    src = _app30_source(record)
    if not decision.auto:
        return src, None, [], []
    facts, events = _app30_facts_events(record)
    return src, decision.case_key, facts, events


def _app30_facts_events(record: dict) -> tuple:
    """案件に依らない App 30 の fact/event（stale 保存でも同じ形・R11）。"""
    rid = _s(record, "$id")
    subject = ledger.SUBJECT_SHIPPING_PREFIX + rid
    facts = []
    for field, vtype in ledger.app30_fields().items():
        if field not in record:
            continue                         # 欄が無い＝部分取得。消去とは判断しない
        facts.append(ledger.FactIn(
            subject, ledger.APP30_PREFIX + field, vtype, _v(record, field),
            ledger.make_locator(field), "high"))
    status = _s(record, "発送ステータス")
    events = [ledger.EventIn(
        EVENT_SHIPPING_STATUS,
        "shipping:" + (status if status in APP30_STATUS_OPTIONS else "other"),
        ledger.make_locator("発送ステータス"),
        occurred_at=_parse_dt(_s(record, "発送日時")))]
    if status in APP30_SHIPPED_STATES and not _s(record, "発送日時"):
        # §4-3: 現在状態から過去の発送日時は復元できない＝「同期時点で発送済を確認」
        events.append(ledger.EventIn(EVENT_SHIPPED_AT_SYNC, "shipping:confirmed_at_sync",
                                     ledger.make_locator("発送日時"), occurred_at=None))
    return facts, events


def houki_chat_category(category: str) -> bool:
    if category in HOUKI_CHAT_CATEGORIES:
        return True
    return any(category.startswith(p) for p in HOUKI_CHAT_CATEGORY_PREFIXES)


def _chat_summary(record: dict) -> str:
    """固定語彙のみ（本文・LINE ID は載せない）: chat:{role}:{category 区分}。"""
    role = _s(record, "role")
    role = role if role in CHAT_ROLES else "other"
    cat = _s(record, "category")
    if cat == "相続放棄ヒアリング":
        kind = "hearing"
    elif cat == "相続放棄ヒアリング・要確認":
        kind = "hearing_confirm"
    else:
        kind = "image"
    return f"chat:{role}:{kind}"


def convert_app28(record: dict, case_key: tuple) -> tuple:
    """App 28 レコード → (SourceRef, case_key, [], events)。R9: fact ではなく
    case_event（種別=会話・冪等キーは App 28 レコード番号で 1 件・版更新で増やさない）。"""
    rid = _s(record, "$id")
    app_id = APP_CHATLOG.app_id()
    src = ledger.SourceRef(app_id, rid, _revision(record), *CONVERTER[TARGET_APP28],
                           updated_at=_s(record, "更新日時"))
    occurred = _parse_dt(_s(record, "作成日時"))
    events = [ledger.EventIn(EVENT_CHAT, _chat_summary(record),
                             ledger.make_locator("message"), occurred_at=occurred,
                             per_record=True)]
    return src, case_key, [], events


# ── kintone 取得（読取のみ・query は検証済み値だけを埋める） ─────────────────

def _page_query(base: str, cursor: tuple | None, start: str | None,
                upper: str | None = None) -> str:
    """keyset ページング。cursor=(更新日時, $id) の次から。start は窓の始点。
    upper は走査上限（更新日時 <= 上限・BA-09）。"""
    conds = []
    if base:
        conds.append(f"({base})")
    if upper:
        if not _KINTONE_DT_RE.fullmatch(upper):
            raise ValueError("upper_malformed")
        conds.append(f'更新日時 <= "{upper}"')
    if cursor is not None:
        ts, rid = cursor
        if not _KINTONE_DT_RE.fullmatch(ts) or not _DIGITS_RE.fullmatch(rid):
            raise ValueError("cursor_malformed")
        conds.append(f'(更新日時 > "{ts}" or (更新日時 = "{ts}" and $id > {rid}))')
    elif start:
        if not _KINTONE_DT_RE.fullmatch(start):
            raise ValueError("start_malformed")
        conds.append(f'更新日時 >= "{start}"')
    where = " and ".join(conds)
    return (where + " " if where else "") + f"order by 更新日時 asc, $id asc limit {PAGE_SIZE}"


def _window_start(cursor_updated_at: str) -> str | None:
    dt = _parse_dt(cursor_updated_at)
    if dt is None:
        return None
    return _fmt_dt(dt - datetime.timedelta(minutes=WINDOW_MINUTES))


async def app40_exists_in_source(record_id: str) -> bool | None:
    """参照先 App 40 の実在を**正本**で確認する（読取のみ・失敗は None）。
    紐付け訂正の訂正先検査（BA-06）に使う。"""
    if not _DIGITS_RE.fullmatch(str(record_id)):
        return False
    try:
        found = await kintone.search_records(APP_HOUKI, f'$id = "{record_id}" limit 1',
                                             fields=["$id"])
    except Exception:
        return None
    return bool(found)


async def _app40_exists(record_id: str) -> bool | None:
    """参照先 App 40 の実在確認: 台帳（既知の出典）→ kintone。失敗は None。"""
    try:
        if await ledger.latest_known_revision(APP_HOUKI.app_id(), record_id) is not None:
            return True
    except Exception:
        return None
    return await app40_exists_in_source(record_id)


async def _decide_app30(record: dict) -> tuple:
    """戻り値 (decision, ok)。ok=False は参照先の実在確認が失敗（判定不能・R12）。"""
    houki = APP_HOUKI.app_id()
    ref_rec = brain_link._v(record, brain_link.FIELD_CASE_RECORD)
    exists = None
    ok = True
    if (brain_link._v(record, brain_link.FIELD_CASE_APP) == houki
            and _DIGITS_RE.fullmatch(ref_rec)):
        exists = await _app40_exists(ref_rec)
        ok = exists is not None
    return brain_link.decide_app30_reference(record, houki, exists), ok


async def _decide_app28(record: dict) -> tuple:
    """R5 の判定を**正本**で行う（BA-04）。戻り値 (decision|None, reason, ok)。
    ok=False は検索失敗＝判定不能（採用しない・既存の関連も変えない＝fail-closed）。"""
    if not houki_chat_category(_s(record, "category")):
        return None, brain_link.REASON_CATEGORY_OUT, True
    luid = _s(record, "line_user_id")
    if not _LINE_ID_RE.fullmatch(luid):
        return None, brain_link.REASON_LINE_NOT_UNIQUE, True
    try:
        found = await kintone.search_records(
            APP_HOUKI, f'{APP40_LINE_ID_FIELD} = "{luid}" limit 2', fields=["$id"])
    except Exception:
        return None, None, False
    ids = [_s(r, "$id") for r in found if _DIGITS_RE.fullmatch(_s(r, "$id"))]
    if len(ids) != 1:
        return None, brain_link.REASON_LINE_NOT_UNIQUE, True
    decision = brain_link.decide_app28_row(record, [(APP_HOUKI.app_id(), ids[0])])
    return decision, brain_link.REASON_AUTO, True


async def _stale_input(target: str, record: dict) -> tuple | None:
    """R11: 系列で見た最大 revision より小さい入力は履歴保存のみ（判定も履歴も変えない）。
    戻り値: None（stale でない）／()（stale だが既知＝何もしない）／取込タプル。"""
    app_id = _app_of(target).app_id()
    rid, rev = _s(record, "$id"), _revision(record)
    known = await ledger.latest_known_revision(app_id, rid)
    if known is None or rev >= known:
        return None
    src = ledger.SourceRef(app_id, rid, rev, *CONVERTER[target],
                           updated_at=_s(record, "更新日時"))
    if await ledger.known_revision(src):
        return ()
    facts = _app30_facts_events(record)[0] if target == TARGET_APP30 else []
    return src, None, facts, [], {"stale": True}


async def _prepare_ingest(target: str, record: dict):
    """レコード → 取込タプル (src, case_key, facts, events, extras)。None = 対象外・
    処理不要。PENDING_RECHECK = 判定不能（正本の検索/実在確認の失敗・R12。関連は
    保持し pending_recheck として記録・次回の再照合対象）。関連の移動／喪失（R10）は
    extras["link_change"] として同一トランザクションで適用される。遅着の旧 revision は
    履歴保存のみ（R11）。紐付け履歴（変化なし）はここで積む。"""
    extras: dict = {}
    if target != TARGET_APP40:
        stale = await _stale_input(target, record)
        if stale is not None:
            return stale or STALE_KNOWN      # R11/R13: 旧版は履歴のみ・復旧の契機にしない
    if target == TARGET_APP40:
        src, case_key, facts, events = convert_app40(record)
        extras["subjects_seen"] = app40_subjects_seen(record)
    elif target == TARGET_APP30:
        decision, ok = await _decide_app30(record)
        if not ok:
            return PENDING_RECHECK
        rid, rev = _s(record, "$id"), _revision(record)
        last = await ledger.latest_link(APP_SHIPPING.app_id(), rid)
        if (last is not None and last.get("actor") != "system" and last.get("new_case")
                and last.get("source_revision") is not None
                and rev <= int(last["source_revision"])):
            # 人の紐付け訂正は、出典の revision が変わるまで自動判定より優先する
            decision = brain_link.LinkDecision("auto", tuple(last["new_case"]),
                                               brain_link.REASON_MANUAL_PINNED)
        src, case_key, facts, events = convert_app30(record, decision)
        prev = await ledger.current_case_of_source(src.app_id, src.record_id)
        change = brain_link.link_change_for(decision, prev)
        if change is not None:
            extras["link_change"] = change
        elif decision.reason != brain_link.REASON_MANUAL_PINNED:
            await brain_link.record_decision(src.app_id, src.record_id, decision, prev)
        extras["hold_reason"] = decision.reason
    else:
        rid = _s(record, "$id")
        decision, reason, ok = await _decide_app28(record)
        if not ok:
            return PENDING_RECHECK           # 判定不能＝採用しない・既存の関連も変えない
        prev = await ledger.current_case_of_source(APP_CHATLOG.app_id(), rid)
        if decision is None:
            change = brain_link.link_change_for(None, prev, lost_reason=reason,
                                                ingest_state="detached")
            if change is None:
                # R5: 取り込まない・保留にもしない。判定は成功したので pending は解除（BA-15）
                await ledger.set_pending_recheck(APP_CHATLOG.app_id(), rid, False)
                return None
            src = ledger.SourceRef(APP_CHATLOG.app_id(), rid, _revision(record),
                                   *CONVERTER[TARGET_APP28], updated_at=_s(record, "更新日時"))
            return src, None, [], [], {"link_change": change, "ingest_state": "detached",
                                       "hold_reason": reason}
        src, case_key, facts, events = convert_app28(record, decision.case_key)
        change = brain_link.link_change_for(decision, prev)
        if change is not None:
            extras["link_change"] = change
    if "link_change" not in extras:
        status = await ledger.ingest_status(src, case_key)
        if status["known"] and status["same_case"] and (
                case_key is None or status["has_current_facts"]):
            if target != TARGET_APP40:
                # 判定が成功した＝pending_recheck を解除（処理不要と判定不能を区別・R12）
                await ledger.set_pending_recheck(src.app_id, src.record_id, False)
            if status["state"] == "unavailable":
                # R13: 有効な最新版の照合に成功（取込は不要）＝unavailable を復旧
                await ledger.mark_source_checked(src.app_id, src.record_id)
            return None                      # revision 既知＝再処理しない
    return src, case_key, facts, events, extras


def _base_query(target: str) -> str:
    if target == TARGET_APP30:
        houki = APP_HOUKI.app_id()
        if not _DIGITS_RE.fullmatch(houki):
            raise ValueError("houki_app_id_missing")
        return f'案件アプリID = "{houki}"'
    return ""


def _app_of(target: str) -> kintone.KintoneApp:
    return {TARGET_APP40: APP_HOUKI, TARGET_APP30: APP_SHIPPING,
            TARGET_APP28: APP_CHATLOG}[target]


def _log_run_ok(target: str, pages: int, records: int, inserted: int) -> None:
    if target == TARGET_APP40:
        logger.info("[BRAIN_SYNC] app40 run ok pages=%s records=%s inserted=%s",
                    emit(pages, "count", "log", "operator"),
                    emit(records, "count", "log", "operator"),
                    emit(inserted, "count", "log", "operator"))
    elif target == TARGET_APP30:
        logger.info("[BRAIN_SYNC] app30 run ok pages=%s records=%s inserted=%s",
                    emit(pages, "count", "log", "operator"),
                    emit(records, "count", "log", "operator"),
                    emit(inserted, "count", "log", "operator"))
    else:
        logger.info("[BRAIN_SYNC] app28 run ok pages=%s records=%s inserted=%s",
                    emit(pages, "count", "log", "operator"),
                    emit(records, "count", "log", "operator"),
                    emit(inserted, "count", "log", "operator"))


def _log_run_failed(target: str, failure: str) -> None:
    # 固定語彙のみ（failure は閉集合・分岐で定数化）
    if failure == FAIL_KINTONE:
        logger.warning("[BRAIN_SYNC] run failed (kintone_fetch_failed)")
    elif failure == FAIL_DB:
        logger.warning("[BRAIN_SYNC] run failed (db_write_failed)")
    elif failure == FAIL_MISMATCH:
        logger.warning("[BRAIN_SYNC] run halted (mismatch_hold)")
    else:
        logger.warning("[BRAIN_SYNC] run failed (page_limit_reached)")


def _log_pending(count: int) -> None:
    logger.warning("[BRAIN_SYNC] run partial (pending_recheck) count=%s",
                   emit(count, "count", "log", "operator"))


def _resume_position(cur: dict | None) -> tuple | None:
    """未完了走査（ページ上限で止まった run）の再開位置と上限（BA-09）。"""
    if not cur or cur.get("state") != "incomplete":
        return None
    pos = cur.get("page_position") or {}
    upper = cur.get("scan_upper_bound") or ""
    ts, rid = str(pos.get("after_updated_at") or ""), str(pos.get("after_record_id") or "")
    if not (_KINTONE_DT_RE.fullmatch(upper) and _KINTONE_DT_RE.fullmatch(ts)
            and _DIGITS_RE.fullmatch(rid)):
        return None
    return upper, (ts, rid)


async def sync_target(target: str, *, now=None) -> dict:
    """1 対象アプリの同期を 1 回実行する（ページ単位の原子的前進）。"""
    now = now or _now()
    app = _app_of(target)
    cur = await ledger.get_cursor(target)
    resume = _resume_position(cur)
    if resume is not None:
        scan_upper, cursor = resume          # 未完了走査を同じ上限で再開
        start = None
    else:
        scan_upper = _fmt_dt(now)
        cursor = None
        start = _window_start(cur["cursor_updated_at"]) if cur else None
    run_id = await ledger.start_run(target, scan_upper, now=now)
    base = _base_query(target)
    pages = 0
    records_seen = 0
    inserted = 0
    result = {"target": target, "run_id": run_id, "status": "ok", "pages": 0,
              "records": 0, "inserted": 0, "held": 0, "resumed": resume is not None,
              "scan_upper_bound": scan_upper, "pending_recheck": 0,
              "pending_unregistered": 0}
    pending = 0
    unregistered = 0
    first_unregistered = None                # 未登録で判定不能だった最初の (更新日時, $id)
    while pages < MAX_PAGES_PER_RUN:
        query = _page_query(base, cursor, start, scan_upper)
        try:
            page = await kintone.search_records(app, query)       # DB を持たず待つ
        except Exception:
            await _fail(target, run_id, FAIL_KINTONE, cursor, pages, records_seen, now)
            result.update(status="failed", failure=FAIL_KINTONE)
            return result
        ingests = []
        last = None
        for record in page:
            ts, rid = _s(record, "更新日時"), _s(record, "$id")
            if not _KINTONE_DT_RE.fullmatch(ts) or not _DIGITS_RE.fullmatch(rid):
                continue
            last = (ts, rid)
            try:
                prepared = await _prepare_ingest(target, record)
                if prepared is PENDING_RECHECK:
                    # R12: 判定不能。関連は保持し pending_recheck を記録。BA-17: 出典行が
                    # 無い（初見）ときは run/cursor に件数と未完了を永続化し、次回は
                    # その位置から取り直す（再起動後も再試行される）
                    flagged = await ledger.set_pending_recheck(app.app_id(), rid, True, now=now)
                    if flagged == 0 and await ledger.latest_known_revision(
                            app.app_id(), rid) is None:
                        unregistered += 1
                        if first_unregistered is None:
                            first_unregistered = (ts, rid)
                    pending += 1
                    continue
                if prepared is STALE_KNOWN:
                    continue
            except Exception:
                await _fail(target, run_id, FAIL_DB, cursor, pages, records_seen, now)
                result.update(status="failed", failure=FAIL_DB)
                return result
            if prepared is not None:
                ingests.append(prepared)
        records_seen += len(page)
        if last is None:
            break
        pages += 1
        try:
            summaries = await ledger.advance_cursor_with_page(
                target, run_id, ingests=ingests, cursor_updated_at=last[0],
                cursor_record_id=last[1], pages_done=pages,
                records_seen=records_seen, scan_upper_bound=scan_upper, now=now)
        except ledger.MismatchError:
            await _fail(target, run_id, FAIL_MISMATCH, cursor, pages - 1, records_seen, now)
            result.update(status="failed", failure=FAIL_MISMATCH)
            return result
        except Exception:
            await _fail(target, run_id, FAIL_DB, cursor, pages - 1, records_seen, now)
            result.update(status="failed", failure=FAIL_DB)
            return result
        inserted += sum(s.get("inserted", 0) for s in summaries)
        result["held"] += sum(1 for s in summaries if s.get("state") == "held")
        cursor = last
        if len(page) < PAGE_SIZE:
            break
    else:
        # ページ上限: 走査は未完了のまま（再開位置と上限を残す・完了扱いにしない）
        await _fail(target, run_id, FAIL_PAGE_LIMIT, cursor, pages, records_seen, now,
                    keep_position=True)
        result.update(status="failed", failure=FAIL_PAGE_LIMIT)
        return result
    if unregistered:
        # BA-17: 走査は終えたが未登録の未解決が残る＝synced にせず confirmed_until も進めない。
        # カーソルは最初の未解決レコードの位置へ戻し、次回はその 10 分前から取り直す
        await ledger.set_cursor_state(
            target, "incomplete", now=now, scan_upper_bound=scan_upper, page_position=None,
            cursor_updated_at=first_unregistered[0], cursor_record_id=first_unregistered[1],
            pending_unregistered=unregistered, last_run_id=run_id)
        await ledger.finish_run(run_id, "partial", failure="pending_unregistered",
                                pages_done=pages, records_seen=records_seen,
                                pending_unregistered=unregistered, now=now)
    else:
        await ledger.set_cursor_state(target, "synced", now=now, confirmed_until=scan_upper,
                                      scan_upper_bound=scan_upper, page_position=None,
                                      pending_unregistered=0, last_ok_at=now,
                                      last_run_id=run_id)
        await ledger.finish_run(run_id, "partial" if pending else "ok", pages_done=pages,
                                records_seen=records_seen,
                                confirmed_range={"until": scan_upper}, now=now)
    result.update(pages=pages, records=records_seen, inserted=inserted,
                  pending_recheck=pending, pending_unregistered=unregistered,
                  status="partial" if pending else "ok")
    _log_run_ok(target, pages, records_seen, inserted)
    if pending:
        _log_pending(pending)
    return result


async def _fail(target: str, run_id: int, failure: str, cursor, pages: int,
                records: int, now, *, keep_position: bool = False) -> None:
    incomplete = ({"after_updated_at": cursor[0], "after_record_id": cursor[1]}
                  if cursor else {"after_updated_at": None, "after_record_id": None})
    try:
        await ledger.finish_run(run_id, "failed", failure=failure,
                                incomplete_page=incomplete, pages_done=pages,
                                records_seen=records, now=now)
        extra = {} if keep_position else {"page_position": None}
        await ledger.set_cursor_state(
            target, "error" if failure in (FAIL_KINTONE, FAIL_DB) else "incomplete",
            now=now, last_run_id=run_id, **extra)
    except Exception:
        pass                                  # DB 障害は brain 内で握る
    _log_run_failed(target, failure)


# ── 定期照合（revision 一覧の差分） ──────────────────────────────────────────

async def reconcile_target(target: str, *, now=None) -> dict:
    """対象レコードの revision 一覧を取り直し、台帳より新しいものを取り込む。
    App 28 は category が閉集合の行だけを見る（他は対象外・DB にも触れない）。"""
    now = now or _now()
    app = _app_of(target)
    base = _base_query(target)
    fields = ["$id", "$revision", "更新日時"]
    if target == TARGET_APP28:
        fields.append("category")
    after = "0"
    changed = 0
    seen = 0
    for _ in range(MAX_PAGES_PER_RUN):
        where = (f"({base}) and " if base else "") + f"$id > {after}"
        try:
            page = await kintone.search_records(
                app, f"{where} order by $id asc limit {PAGE_SIZE}", fields=fields)
        except Exception:
            logger.warning("[BRAIN_SYNC] reconcile failed (kintone_fetch_failed)")
            return {"status": "failed", "changed": changed}
        if not page:
            break
        for r in page:
            rid = _s(r, "$id")
            if not _DIGITS_RE.fullmatch(rid):
                continue
            after = rid
            if target == TARGET_APP28 and not houki_chat_category(_s(r, "category")):
                continue
            seen += 1
            known = await ledger.latest_known_revision(app.app_id(), rid)
            if known is not None and _revision(r) <= known:
                continue
            if await _ingest_single(target, rid):
                changed += 1
        if len(page) < PAGE_SIZE:
            break
    await ledger.set_cursor_state(target, (await ledger.get_cursor(target) or {}).get(
        "state", "incomplete"), now=now, last_reconcile_at=now)
    logger.info("[BRAIN_SYNC] reconcile ok seen=%s changed=%s",
                emit(seen, "count", "log", "operator"),
                emit(changed, "count", "log", "operator"))
    return {"status": "ok", "changed": changed, "seen": seen}


async def _ingest_single(target: str, record_id: str) -> bool:
    """1 レコードを取り直して取り込む（照合・再照合用）。取得失敗は False。"""
    app = _app_of(target)
    try:
        found = await kintone.search_records(app, f'$id = "{record_id}" limit 1')
    except Exception:
        return False
    if not found:
        await ledger.mark_source_unavailable(app.app_id(), record_id)
        return False
    prepared = await _prepare_ingest(target, found[0])
    if prepared is PENDING_RECHECK:
        await ledger.set_pending_recheck(app.app_id(), record_id, True)
        return False
    if prepared is None or prepared is STALE_KNOWN:
        return False
    src, case_key, facts, events, extras = prepared
    await ledger.ingest_source(src, case_key, facts, events, extras=extras)
    return True


# ── 追跡再照合（既取込の出典の再取得・BA-05） ───────────────────────────────

def _recheck_in_progress(rc: dict | None) -> bool:
    return bool(rc) and rc.get("state") in ("incomplete", "error") \
        and bool(rc.get("recheck_started_at"))


async def _recheck_one(target: str, app, rid: str, record: dict | None, now) -> dict:
    out = {"unavailable": 0, "moved": 0, "pending": 0}
    if record is None:
        await ledger.mark_source_unavailable(app.app_id(), rid, now=now)
        out["unavailable"] = 1
        return out
    prev = await ledger.current_case_of_source(app.app_id(), rid)
    prepared = await _prepare_ingest(target, record)
    if prepared is PENDING_RECHECK:
        # R12: 判定不能。既存の状態（unavailable 等）は復旧させない
        await ledger.set_pending_recheck(app.app_id(), rid, True, now=now)
        out["pending"] = 1
        return out
    if prepared is STALE_KNOWN:
        return out                           # R13: 既知旧版による省略は復旧の契機にしない
    if prepared is None:
        # 有効な最新版の判定に成功し取込は不要（同じ案件・現在値あり）＝復旧してよい
        await ledger.mark_source_checked(app.app_id(), rid, now=now)
        return out
    src, case_key, facts, events, extras = prepared
    summary = await ledger.ingest_source(src, case_key, facts, events, extras=extras,
                                         now=now)
    if summary.get("stale"):
        return out                           # R11/R13: 履歴保存のみ・関連も状態も変えない
    # R13/BA-14: 有効な最新版の照合・取込が成功した後にだけ復旧（取込側で ingested へ
    # 戻した行以外＝held/detached の出典の unavailable 行もここで戻す）
    await ledger.mark_source_checked(app.app_id(), rid, now=now)
    if prev is not None and (case_key is None or tuple(prev) != tuple(case_key)):
        out["moved"] = 1                     # 移動または関連喪失（R10 の共通処理を通過）
    elif summary.get("moved"):
        out["moved"] = 1
    return out


async def recheck_target(target: str, *, now=None, batch: int = RECHECK_BATCH) -> dict:
    """source_ingest の出典をレコード番号順に batch 件ずつ再取得し、(a) 案件参照の
    移動／喪失 → R10 の共通処理、(b) 取得不能 → 出典確認不能、を反映する。
    継続位置は sync_cursor(kind=recheck) に持ち、途中失敗でも保持。一巡完了で
    recheck_completed_at（「全件照合済み」は一巡完了後のみ）。"""
    now = now or _now()
    app = _app_of(target)
    rc = await ledger.get_cursor(target, kind=ledger.CURSOR_KIND_RECHECK)
    if _recheck_in_progress(rc):
        after = rc["cursor_record_id"] or "0"
        started_at = datetime.datetime.fromisoformat(rc["recheck_started_at"])
    else:
        after = "0"
        started_at = now
    # R12: 判定不能（pending_recheck）の出典は毎回先に再照合し、その後に位置順の batch
    pending_first = await ledger.list_sources(app.app_id(), pending_only=True, limit=batch)
    positional = await ledger.list_sources(app.app_id(), after=after, limit=batch)
    pending_ids = {s["record_id"] for s in pending_first}
    counts = {"unavailable": 0, "moved": 0, "checked": 0, "pending": 0}
    position = after

    async def _persist(state: str) -> None:
        await ledger.set_cursor_state(
            target, state, kind=ledger.CURSOR_KIND_RECHECK, now=now,
            cursor_record_id=position, recheck_started_at=started_at)

    async def _run_chunks(sources: list, advance: bool) -> bool:
        nonlocal position
        for i in range(0, len(sources), _IN_CHUNK):
            chunk = sources[i:i + _IN_CHUNK]
            ids = [s["record_id"] for s in chunk if _DIGITS_RE.fullmatch(s["record_id"])]
            if ids:
                try:
                    found = await kintone.search_records(
                        app, "$id in (" + ",".join(f'"{x}"' for x in ids)
                        + f") limit {len(ids)}")
                except Exception:
                    await _persist("error")
                    logger.warning("[BRAIN_SYNC] recheck failed (kintone_fetch_failed)")
                    return False
                present = {_s(r, "$id"): r for r in found}
                for rid in ids:
                    one = await _recheck_one(target, app, rid, present.get(rid), now)
                    for k in ("unavailable", "moved", "pending"):
                        counts[k] += one[k]
                    counts["checked"] += 1
            if advance:
                position = chunk[-1]["record_id"]
                await _persist("incomplete")
        return True

    ok = await _run_chunks(pending_first, advance=False)
    if ok:
        ok = await _run_chunks([s for s in positional if s["record_id"] not in pending_ids],
                               advance=True)
    if not ok:
        return {"status": "failed", "complete": False, "position": position, **counts,
                "pending_recheck": counts["pending"]}
    pending_left = await ledger.count_pending_recheck(app.app_id())
    complete = len(positional) < batch and pending_left == 0
    if complete:
        await ledger.set_cursor_state(
            target, "synced", kind=ledger.CURSOR_KIND_RECHECK, now=now,
            cursor_record_id="", recheck_started_at=started_at,
            recheck_completed_at=now)
        logger.info("[BRAIN_SYNC] recheck pass complete checked=%s",
                    emit(counts["checked"], "count", "log", "operator"))
    else:
        await _persist("incomplete")          # pending が残る間は一巡完了にしない
    await ledger.set_cursor_state(target, (await ledger.get_cursor(target) or {}).get(
        "state", "incomplete"), now=now, last_recheck_at=now)
    logger.info("[BRAIN_SYNC] recheck ok unavailable=%s moved=%s",
                emit(counts["unavailable"], "count", "log", "operator"),
                emit(counts["moved"], "count", "log", "operator"))
    if counts["pending"]:
        _log_pending(counts["pending"])
    return {"status": "partial" if counts["pending"] else "ok", "complete": complete,
            "position": position, **counts, "pending_recheck": counts["pending"],
            "pending_left": pending_left}


# ── ジョブ ────────────────────────────────────────────────────────────────────

_disabled_logged: list = []


def _due(last_iso: str, interval: datetime.timedelta, now) -> bool:
    if not last_iso:
        return True
    try:
        last = datetime.datetime.fromisoformat(last_iso)
    except ValueError:
        return True
    if last.tzinfo is None:
        last = last.replace(tzinfo=datetime.timezone.utc)
    return now - last >= interval


async def _recheck_due(target: str, cur: dict, now) -> bool:
    """一巡の途中（未完了・失敗）なら毎回続きを回し、完了後は 24h ごと。"""
    rc = await ledger.get_cursor(target, kind=ledger.CURSOR_KIND_RECHECK)
    if _recheck_in_progress(rc):
        return True
    return _due(cur.get("last_recheck_at", ""), RECHECK_INTERVAL, now)


async def run_brain_sync_job() -> dict:
    """scheduler から 5 分ごとに呼ばれる。OFF なら即 return（ログ 1 行）。
    DB 障害・kintone 障害は本関数内で握り、例外を scheduler へ返さない。"""
    if not brain_sync_enabled():
        if not _disabled_logged:
            _disabled_logged.append(1)
            logger.info("[BRAIN_SYNC] disabled (BRAIN_SYNC_ENABLED off)")
        return {"status": "disabled"}
    now = _now()
    out = {"status": "ok", "targets": {}}
    for target in TARGETS:
        try:
            out["targets"][target] = await sync_target(target, now=now)
        except Exception:
            logger.error("[BRAIN_SYNC] job stage failed (db_or_runtime_error)")
            out["status"] = "degraded"
    try:
        for target in TARGETS:
            cur = await ledger.get_cursor(target) or {}
            if _due(cur.get("last_reconcile_at", ""), RECONCILE_INTERVAL, now):
                await reconcile_target(target, now=now)
            if await _recheck_due(target, cur, now):
                await recheck_target(target, now=now)
    except Exception:
        logger.error("[BRAIN_SYNC] periodic stage failed (db_or_runtime_error)")
        out["status"] = "degraded"
    return out


def register_brain_sync_job() -> None:
    """main.py の末尾から呼ぶ（常に登録・実行可否は coro 先頭の flag 検査）。
    start_all は既存の startup 経路（healthcheck 等）が呼ぶ。"""
    if not hub_scheduler.is_registered(JOB_NAME):
        hub_scheduler.register_interval(JOB_NAME, INTERVAL_MINUTES, run_brain_sync_job)
