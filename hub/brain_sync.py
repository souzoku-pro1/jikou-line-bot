"""brain_sync — BRAIN-A1-LEDGER-1: kintone → 台帳の同期（v3 §5・BD-05/12/17/18）

対象:
- App 40 全レコード（案件そのもの・converter app40_record/1）
- App 30 のうち 案件アプリID = APP_HOUKI の値 の行（判定は brain_link: 案件レコードID
  が数字で App 40 に実在し ユニット種別=相続放棄 のみ auto。他は保留＝紐付け待ち）
- App 28 のうち R5 の行（category が相続放棄の閉集合、line_user_id が App 40 の
  LINEユーザーID に完全一致・ちょうど 1 件）。それ以外は取り込まない

カーソル（§5）: 更新日時 + レコード番号で並べ（page_order 固定）、次回は
「前回の走査上限時点 − 重複窓（10 分）」から取り直す。ページ単位で
台帳書込とカーソル前進を同一トランザクションで行い（brain_ledger.
advance_cursor_with_page）、失敗したページを飛び越えてカーソルを進めない。
走査が最後まで終わったときだけ confirmed_until（確認済み範囲）を走査上限時点へ。

冪等: 取込の冪等キー既知（同じ案件・現在 fact あり）なら再処理しない。
欄の消去は新 fact（空値）+ supersedes。取得失敗は「値なし」に変換しない。
追跡再照合（1 日 1 回・上限件数）: source_ingest の出典を再取得し、
(a) 案件参照の変化 → link_history + 旧案件ビューから除外（brain_ledger 側）
(b) 取得不能 → 出典確認不能（unavailable）として利用停止（削除と推定しない）
定期照合（1 日 1 回）: revision 一覧を取り直して差分を検出し取り込む。

実行管理: 外部 API 待機中に DB トランザクションを持たない（ページを取り切って
から 1 トランザクション）。DB 障害は本 module 内で握り、既存業務へ伝播させない。
有効化: BRAIN_SYNC_ENABLED（既定 OFF・R7）。scheduler へは常に登録し、coro の
先頭で検査（OFF なら即 return・ログ 1 行）。kintone への書込ゼロ。

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
CONVERTER = {TARGET_APP40: ("app40_record", "1"),
             TARGET_APP30: ("app30_record", "1"),
             TARGET_APP28: ("app28_row", "1")}

APP_HOUKI = kintone.KintoneApp("App 40 (相続放棄案件)", "APP_HOUKI", "TOKEN_HOUKI")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")
APP_CHATLOG = kintone.KintoneApp("App 28 (チャットログ)", "APP_CHATLOG", "TOKEN_CHATLOG")

# R5: App 28 の相続放棄カテゴリ閉集合（定数化）。会話行のみ（マーカー行は対象外）
HOUKI_CHAT_CATEGORIES = frozenset({"相続放棄ヒアリング", "相続放棄ヒアリング・要確認"})
HOUKI_CHAT_CATEGORY_PREFIXES = ("画像解析:houki:",)
APP40_LINE_ID_ITEM = ledger.APP40_PREFIX + "LINEユーザーID"

# case_event の要約に使う閉集合（値は非 PII の選択肢のみ・閉集合外は other）
APP40_STATUS_OPTIONS = frozenset({
    "問い合わせ", "電話判断待ち", "電話調整中", "決済待ち", "契約待ち", "受任",
    "書類収集中", "申述書作成", "裁判所提出済", "照会書対応", "受理", "債権者通知",
    "完了", "不受任", "辞任"})
APP30_STATUS_OPTIONS = frozenset({
    "下書き", "承認待ち", "承認済", "発送処理中", "発送済", "返送待ち", "完了",
    "エラー", "却下", "要確認"})
APP30_SHIPPED_STATES = frozenset({"発送済", "返送待ち", "完了"})
EVENT_CASE_STATUS = "case_status_observed"
EVENT_SHIPPING_STATUS = "shipping_status_observed"
EVENT_SHIPPED_AT_SYNC = "shipping_confirmed_at_sync"

_KINTONE_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_DIGITS_RE = re.compile(r"^[0-9]{1,10}$")
_LINE_ID_RE = re.compile(r"^U[0-9a-f]{32}$")

FAIL_KINTONE = "kintone_fetch_failed"
FAIL_DB = "db_write_failed"
FAIL_MISMATCH = "mismatch_hold"
FAIL_PAGE_LIMIT = "page_limit_reached"


def brain_sync_enabled() -> bool:
    """flag BRAIN_SYNC_ENABLED（既定 OFF・値集合は既存 flag 群と同一・R7）。"""
    return os.environ.get(config.BRAIN_SYNC_ENABLED_ENV, "").strip().lower() in _FLAG_TRUE


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


def convert_app40(record: dict) -> tuple:
    """App 40 レコード → (SourceRef, case_key, facts, events)。案件キー = 自分自身。"""
    rid = _s(record, "$id")
    app_id = APP_HOUKI.app_id()
    src = ledger.SourceRef(app_id, rid, _revision(record), *CONVERTER[TARGET_APP40],
                           updated_at=_s(record, "更新日時"))
    facts = []
    for field, vtype in ledger.app40_fields().items():
        facts.append(ledger.FactIn(
            ledger.app40_field_subject(field), ledger.APP40_PREFIX + field, vtype,
            _v(record, field), ledger.make_locator(field),
            ledger.app40_field_confidence(field)))
    for table, columns, prefix in (
            (ledger.APP40_CREDITOR_TABLE, ledger.APP40_CREDITOR_COLUMNS,
             ledger.SUBJECT_CREDITOR_PREFIX),
            (ledger.APP40_DOCUMENT_TABLE, ledger.APP40_DOCUMENT_COLUMNS,
             ledger.SUBJECT_DOCUMENT_PREFIX)):
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


def convert_app30(record: dict, decision: brain_link.LinkDecision) -> tuple:
    """App 30 レコード → (SourceRef, case_key|None, facts, events)。"""
    rid = _s(record, "$id")
    app_id = APP_SHIPPING.app_id()
    src = ledger.SourceRef(app_id, rid, _revision(record), *CONVERTER[TARGET_APP30],
                           updated_at=_s(record, "更新日時"))
    if not decision.auto:
        return src, None, [], []
    facts = []
    for field, vtype in ledger.app30_fields().items():
        facts.append(ledger.FactIn(
            ledger.SUBJECT_CASE, ledger.APP30_PREFIX + field, vtype, _v(record, field),
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
    return src, decision.case_key, facts, events


def houki_chat_category(category: str) -> bool:
    if category in HOUKI_CHAT_CATEGORIES:
        return True
    return any(category.startswith(p) for p in HOUKI_CHAT_CATEGORY_PREFIXES)


def convert_app28(record: dict, case_key: tuple) -> tuple:
    rid = _s(record, "$id")
    app_id = APP_CHATLOG.app_id()
    src = ledger.SourceRef(app_id, rid, _revision(record), *CONVERTER[TARGET_APP28],
                           updated_at=_s(record, "更新日時"))
    occurred = _parse_dt(_s(record, "作成日時"))
    facts = [ledger.FactIn(ledger.SUBJECT_CASE, ledger.APP28_PREFIX + field, vtype,
                           _v(record, field), ledger.make_locator(field), "high",
                           occurred_at=occurred)
             for field, vtype in ledger.app28_fields().items()]
    return src, case_key, facts, []


# ── kintone 取得（読取のみ・query は検証済み値だけを埋める） ─────────────────

def _page_query(base: str, cursor: tuple | None, start: str | None) -> str:
    """keyset ページング。cursor=(更新日時, $id) の次から。start は窓の始点。"""
    conds = []
    if base:
        conds.append(f"({base})")
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


async def _app40_exists(record_id: str) -> bool | None:
    """参照先 App 40 の実在確認: 台帳（既知の出典）→ kintone。失敗は None。"""
    try:
        if await ledger.latest_known_revision(APP_HOUKI.app_id(), record_id) is not None:
            return True
    except Exception:
        return None
    try:
        found = await kintone.search_records(APP_HOUKI, f'$id = "{record_id}" limit 1',
                                             fields=["$id"])
    except Exception:
        return None
    return bool(found)


async def _decide_app30(record: dict) -> brain_link.LinkDecision:
    houki = APP_HOUKI.app_id()
    ref_rec = brain_link._v(record, brain_link.FIELD_CASE_RECORD)
    exists = None
    if (brain_link._v(record, brain_link.FIELD_CASE_APP) == houki
            and _DIGITS_RE.fullmatch(ref_rec)):
        exists = await _app40_exists(ref_rec)
    return brain_link.decide_app30_reference(record, houki, exists)


async def _prepare_ingest(target: str, record: dict) -> tuple | None:
    """レコード → 取込タプル（None = 対象外・スキップ）。紐付け履歴もここで積む。"""
    if target == TARGET_APP40:
        src, case_key, facts, events = convert_app40(record)
    elif target == TARGET_APP30:
        decision = await _decide_app30(record)
        rid, rev = _s(record, "$id"), _revision(record)
        last = await ledger.latest_link(APP_SHIPPING.app_id(), rid)
        if (last is not None and last.get("actor") != "system" and last.get("new_case")
                and last.get("source_revision") is not None
                and rev <= int(last["source_revision"])):
            # 人の紐付け訂正は、出典の revision が変わるまで自動判定より優先する
            decision = brain_link.LinkDecision("auto", tuple(last["new_case"]),
                                               brain_link.REASON_MANUAL_PINNED)
            src, case_key, facts, events = convert_app30(record, decision)
        else:
            src, case_key, facts, events = convert_app30(record, decision)
            prev = await ledger.current_case_of_source(src.app_id, src.record_id)
            await brain_link.record_decision(src.app_id, src.record_id, decision, prev)
    else:
        if not houki_chat_category(_s(record, "category")):
            return None
        luid = _s(record, "line_user_id")
        if not _LINE_ID_RE.fullmatch(luid):
            return None
        matches = await ledger.find_case_by_item_value(APP40_LINE_ID_ITEM, luid)
        decision = brain_link.decide_app28_row(record, matches)
        if decision is None:
            return None                      # R5: 取り込まない・保留にもしない
        src, case_key, facts, events = convert_app28(record, decision.case_key)
    status = await ledger.ingest_status(src, case_key)
    if status["known"] and status["same_case"] and (
            case_key is None or status["has_current_facts"]):
        return None                          # revision 既知＝再処理しない
    return src, case_key, facts, events


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


async def sync_target(target: str, *, now=None) -> dict:
    """1 対象アプリの同期を 1 回実行する（ページ単位の原子的前進）。"""
    now = now or _now()
    app = _app_of(target)
    cur = await ledger.get_cursor(target)
    start = _window_start(cur["cursor_updated_at"]) if cur else None
    scan_upper = _fmt_dt(now)
    run_id = await ledger.start_run(target, scan_upper, now=now)
    base = _base_query(target)
    cursor = None
    pages = 0
    records_seen = 0
    inserted = 0
    result = {"target": target, "run_id": run_id, "status": "ok", "pages": 0,
              "records": 0, "inserted": 0, "held": 0}
    while pages < MAX_PAGES_PER_RUN:
        query = _page_query(base, cursor, start)
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
                records_seen=records_seen, now=now)
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
        await _fail(target, run_id, FAIL_PAGE_LIMIT, cursor, pages, records_seen, now)
        result.update(status="failed", failure=FAIL_PAGE_LIMIT)
        return result
    await ledger.set_cursor_state(target, "synced", now=now, confirmed_until=scan_upper,
                                  last_ok_at=now, last_run_id=run_id)
    await ledger.finish_run(run_id, "ok", pages_done=pages, records_seen=records_seen,
                            confirmed_range={"until": scan_upper}, now=now)
    result.update(pages=pages, records=records_seen, inserted=inserted)
    _log_run_ok(target, pages, records_seen, inserted)
    return result


async def _fail(target: str, run_id: int, failure: str, cursor, pages: int,
                records: int, now) -> None:
    incomplete = ({"after_updated_at": cursor[0], "after_record_id": cursor[1]}
                  if cursor else {"after_updated_at": None, "after_record_id": None})
    try:
        await ledger.finish_run(run_id, "failed", failure=failure,
                                incomplete_page=incomplete, pages_done=pages,
                                records_seen=records, now=now)
        await ledger.set_cursor_state(
            target, "error" if failure in (FAIL_KINTONE, FAIL_DB) else "incomplete",
            now=now, last_run_id=run_id)
    except Exception:
        pass                                  # DB 障害は brain 内で握る
    _log_run_failed(target, failure)


# ── 定期照合（revision 一覧の差分） ──────────────────────────────────────────

async def reconcile_target(target: str, *, now=None) -> dict:
    """対象レコードの revision 一覧を取り直し、台帳より新しいものを取り込む。"""
    now = now or _now()
    app = _app_of(target)
    base = _base_query(target)
    after = "0"
    changed = 0
    seen = 0
    for _ in range(MAX_PAGES_PER_RUN):
        where = (f"({base}) and " if base else "") + f"$id > {after}"
        try:
            page = await kintone.search_records(
                app, f"{where} order by $id asc limit {PAGE_SIZE}",
                fields=["$id", "$revision", "更新日時"])
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
    if prepared is None:
        return False
    src, case_key, facts, events = prepared
    await ledger.ingest_source(src, case_key, facts, events)
    return True


# ── 追跡再照合（既取込の出典の再取得） ──────────────────────────────────────

async def recheck_target(target: str, *, now=None, batch: int = RECHECK_BATCH) -> dict:
    """source_ingest の出典を再取得し、(a) 案件参照の移動 → 訂正履歴＋旧案件除外、
    (b) 取得不能 → 出典確認不能、を反映する。上限 batch 件/回。"""
    now = now or _now()
    app = _app_of(target)
    sources = (await ledger.list_sources(app.app_id()))[:batch]
    unavailable = 0
    moved = 0
    for i in range(0, len(sources), _IN_CHUNK):
        chunk = sources[i:i + _IN_CHUNK]
        ids = [s["record_id"] for s in chunk if _DIGITS_RE.fullmatch(s["record_id"])]
        if not ids:
            continue
        try:
            found = await kintone.search_records(
                app, "$id in (" + ",".join(f'"{x}"' for x in ids) + f") limit {len(ids)}")
        except Exception:
            logger.warning("[BRAIN_SYNC] recheck failed (kintone_fetch_failed)")
            return {"status": "failed", "unavailable": unavailable, "moved": moved}
        present = {_s(r, "$id"): r for r in found}
        for rid in ids:
            record = present.get(rid)
            if record is None:
                await ledger.mark_source_unavailable(app.app_id(), rid, now=now)
                unavailable += 1
                continue
            await ledger.mark_source_checked(app.app_id(), rid, now=now)
            prev = await ledger.current_case_of_source(app.app_id(), rid)
            prepared = await _prepare_ingest(target, record)
            if prepared is None:
                continue
            src, case_key, facts, events = prepared
            summary = await ledger.ingest_source(src, case_key, facts, events, now=now)
            if prev is not None and case_key is not None and tuple(prev) != tuple(case_key):
                moved += 1
            elif summary.get("moved"):
                moved += 1
    await ledger.set_cursor_state(target, (await ledger.get_cursor(target) or {}).get(
        "state", "incomplete"), now=now, last_recheck_at=now)
    logger.info("[BRAIN_SYNC] recheck ok unavailable=%s moved=%s",
                emit(unavailable, "count", "log", "operator"),
                emit(moved, "count", "log", "operator"))
    return {"status": "ok", "unavailable": unavailable, "moved": moved}


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
    for target in (TARGET_APP40, TARGET_APP30, TARGET_APP28):
        try:
            out["targets"][target] = await sync_target(target, now=now)
        except Exception:
            logger.error("[BRAIN_SYNC] job stage failed (db_or_runtime_error)")
            out["status"] = "degraded"
    try:
        for target in (TARGET_APP40, TARGET_APP30):
            cur = await ledger.get_cursor(target) or {}
            if _due(cur.get("last_reconcile_at", ""), RECONCILE_INTERVAL, now):
                await reconcile_target(target, now=now)
            if _due(cur.get("last_recheck_at", ""), RECHECK_INTERVAL, now):
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
