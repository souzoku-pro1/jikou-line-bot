"""houki_koseki_checklist — HOUKI-KOSEKI-CHECKLIST-1: 受任時の必要戸籍チェックリスト初期行と連続戸籍充足の候補通知

正本: docs/souzoku-houki/10-koseki-matrix.md §1（共通・絶対必須＝除票）・§2（続柄別マトリクス・先順位放棄は
個別確定）・§4（連続完成の宣言は弁護士のみ＝機械は候補通知まで）・04-kintone-design §1.5（書類チェック SUBTABLE）。
QUEUE-2026-10-12-C C-4。司令塔仮置き: 書類名は既存 7 択をそのまま使い、順位 2/3・代襲は「その他」+対象者欄で表現。

- 初期行の生成（(a)）: status が受任系（ENGAGED_STATUSES）のレコードで 書類チェック が空のときだけ、続柄（App 40
  DROP_DOWN・実測値）からマトリクス（KOSEKI_MATRIX）で行を作り $revision CAS で 1 回書く。既存行が 1 行でもあれば
  追加しない（冪等）。409 は再取得して「空のまま」なら 1 回だけ再試行、行ができていれば exists。
  先順位放棄（複雑性フラグ）・続柄が閉集合外/不明は共通行（除票・委任状・印紙・郵券）だけ生成し
  「必要書類の個別確定が必要」を通知する（10 §2）。
- 発火点: (1) 定期走査（SCAN_JOB_NAME・interval）= status が受任系へ遷移した後の最初の走査で初期化（遷移経路を問わない
  ＝CloudSign 締結・手入力・読取報告のどれでも拾う）。(2) 受け口 /souzoku-houki/koseki-checklist/{secret}
  （kintone Webhook／手動 POST・shinjutsu_webhook と同じ認証）＝即時。
- 候補通知（(b)）: 戸籍系の行がすべて 書類状態=受領 で、連続戸籍充足 が yes でないレコードに「連続戸籍充足の候補」を
  1 回だけ通知（送信台帳 business=souzoku-houki・channel=houki_koseki・purpose=other・受信イベント ID=
  koseki_complete:{record_id}＝DUPLICATE_SENT で 2 通目なし）。yes を入れるのは弁護士（本 module は書かない）。
- スキーマ検査（fail-closed）: 書込前に form fields で SUBTABLE の列と選択肢を確認し、想定（列コード 4・書類名 7 択・
  取得方法 3 択・書類状態 6 択）と食い違えば書かず通知（1 回）＝HEARING-FIX1 の「選択肢外=write 0」と同じ流儀。
- 書込は App 40 の 書類チェック 1 欄のみ（plain 値=行 list・HOUKI-STORE-FIX1 の契約）。status・連続戸籍充足・
  弁護士専権欄は書かない。
- RV-10: 通知・ログはレコード No.・続柄の種別・件数のみ（氏名・本文なし）。
"""

import functools
import logging
import os
import time

import httpx

from hub import kintone, send_ledger
from hub import scheduler as hub_scheduler
from hub.houki_case_store import APP_HOUKI_CASE
from hub.notify import business_channel_allowlist, business_token_env, get_admin_line_user_id
from hub.redact import emit

logger = logging.getLogger("hub.houki_koseki_checklist")

BUSINESS = "souzoku-houki"
LEDGER_CHANNEL = "houki_koseki"
SCAN_JOB_NAME = "HOUKI_KOSEKI_CHECKLIST"
SCAN_INTERVAL_MINUTES = 30.0
DISABLE_ENV = "HOUKI_KOSEKI_CHECKLIST_DISABLED"
SCAN_LIMIT = 100
_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# App 40（実測・docs 04 §1.5）
TABLE = "書類チェック"
COL_NAME = "書類名"
COL_TARGET = "対象者"
COL_METHOD = "取得方法"
COL_STATE = "書類状態"
COLUMNS = (COL_NAME, COL_TARGET, COL_METHOD, COL_STATE)
FIELD_STATUS = "status"
FIELD_KANKEI = "続柄"
FIELD_COMPLEX = "複雑性フラグ"
FIELD_CONTINUOUS = "連続戸籍充足"
FLAG_PRIOR_RENUNCIATION = "先順位放棄"

DOC_DEATH_KOSEKI = "被相続人死亡記載戸籍"
DOC_JOHYO = "被相続人住民票除票"
DOC_APPLICANT_KOSEKI = "申述人戸籍"
DOC_POA = "委任状"
DOC_STAMP = "収入印紙"
DOC_POSTAGE = "切手"
DOC_OTHER = "その他"
DOC_OPTIONS = (DOC_DEATH_KOSEKI, DOC_JOHYO, DOC_APPLICANT_KOSEKI, DOC_POA, DOC_STAMP, DOC_POSTAGE, DOC_OTHER)
METHOD_MAIL = "依頼者郵送"
METHOD_REQUEST = "職務上請求"
METHOD_OFFICE = "事務所手配"
METHOD_OPTIONS = (METHOD_MAIL, METHOD_REQUEST, METHOD_OFFICE)
STATE_TODO = "未"
STATE_RECEIVED = "受領"
STATE_NOT_NEEDED = "不要"
STATE_OPTIONS = (STATE_TODO, "依頼中", "発送済", "請求中", STATE_RECEIVED, STATE_NOT_NEEDED)

# 受任系（初期行を生成する段階・HOUKI_PROFILE.post_engagement_statuses の前半）
ENGAGED_STATUSES = ("受任", "書類収集中", "申述書作成")

# 対象者欄の固定文（「その他」行の種別＝連続戸籍など 7 択に無い書類はここで表す）
TARGET_DECEASED = "被相続人"
TARGET_APPLICANT = "申述人"
TARGET_OFFICE = "事務所手配"
TARGET_CONTINUOUS = "被相続人（出生から死亡までの連続戸籍）"
TARGET_PARENTS_DEATH = "被相続人の両親双方（死亡記載戸籍）"
TARGET_APPLICANT_PARENT_DEATH = "申述人の親（被相続人の子・死亡記載戸籍）"
TARGET_SIBLING_DEATH = "申述人の親（被相続人の兄弟姉妹・死亡記載戸籍）"

Row = tuple  # (書類名, 対象者, 取得方法)
COMMON_ROWS: tuple = (
    (DOC_JOHYO, TARGET_DECEASED, METHOD_REQUEST),          # 10 §1: 管轄確定のため最優先
    (DOC_POA, TARGET_APPLICANT, METHOD_MAIL),
    (DOC_STAMP, TARGET_OFFICE, METHOD_OFFICE),
    (DOC_POSTAGE, TARGET_OFFICE, METHOD_OFFICE),
)
_CHILD_ROWS: tuple = (
    (DOC_DEATH_KOSEKI, TARGET_DECEASED, METHOD_REQUEST),
    (DOC_APPLICANT_KOSEKI, TARGET_APPLICANT, METHOD_REQUEST),
)
_ASCENDANT_ROWS: tuple = (
    (DOC_OTHER, TARGET_CONTINUOUS, METHOD_REQUEST),
    (DOC_APPLICANT_KOSEKI, TARGET_APPLICANT, METHOD_REQUEST),
)
_SIBLING_ROWS: tuple = (
    (DOC_OTHER, TARGET_CONTINUOUS, METHOD_REQUEST),
    (DOC_OTHER, TARGET_PARENTS_DEATH, METHOD_REQUEST),
    (DOC_APPLICANT_KOSEKI, TARGET_APPLICANT, METHOD_REQUEST),
)
# 続柄（App 40 DROP_DOWN 実測値・hub/houki_shinjutsu.KANKEI_MAP と同じ語彙）→ 追加行（共通行は別）
KOSEKI_MATRIX: dict = {
    "子": _CHILD_ROWS,
    "配偶者": _CHILD_ROWS,                       # 配偶者は常に相続人（子セットと同じ 2 通・10 §2 に明記なし＝仮置き）
    "孫": _CHILD_ROWS + ((DOC_OTHER, TARGET_APPLICANT_PARENT_DEATH, METHOD_REQUEST),),   # 代襲（子の死亡）
    "直系尊属（父母・祖父母）": _ASCENDANT_ROWS,
    "兄弟姉妹": _SIBLING_ROWS,
    "おいめい": _SIBLING_ROWS + ((DOC_OTHER, TARGET_SIBLING_DEATH, METHOD_REQUEST),),   # 代襲
}
# 連続戸籍充足の候補判定に数える「戸籍系」の行＝書類名が戸籍 2 種、または その他 で対象者に「戸籍」を含む
_KOSEKI_DOCS = (DOC_DEATH_KOSEKI, DOC_APPLICANT_KOSEKI)

RESULT_CREATED = "created"
RESULT_INDIVIDUAL = "individual"        # 共通行のみ生成（個別確定が必要）
RESULT_EXISTS = "exists"
RESULT_NOT_ENGAGED = "not_engaged"
RESULT_SCHEMA_MISMATCH = "schema_mismatch"
RESULT_CAS_LOST = "cas_lost"
RESULT_FAILED = "failed"

_schema_cache: dict = {"at": 0.0, "ok": None}
SCHEMA_TTL_SECONDS = 3600.0


def _v(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def _rows(record: dict) -> list:
    return list((((record or {}).get(TABLE) or {}).get("value")) or [])


def _cell(row: dict, col: str) -> str:
    return str((((row or {}).get("value") or {}).get(col) or {}).get("value") or "").strip()


def disabled() -> bool:
    return os.environ.get(DISABLE_ENV, "").strip().lower() in ("1", "true", "on", "yes")


# ── 純関数: 行の導出 ─────────────────────────────────────────────────────────
def plan_rows(record: dict) -> tuple[list[Row], bool]:
    """初期行の列と「個別確定が必要か」。先順位放棄フラグ・続柄が閉集合外は共通行のみ+個別確定。"""
    flags = (record.get(FIELD_COMPLEX) or {}).get("value") or []
    if isinstance(flags, str):
        flags = [flags]
    kankei = _v(record, FIELD_KANKEI)
    if FLAG_PRIOR_RENUNCIATION in [str(f) for f in flags] or kankei not in KOSEKI_MATRIX:
        return list(COMMON_ROWS), True
    return list(COMMON_ROWS) + list(KOSEKI_MATRIX[kankei]), False


def to_subtable(rows: list[Row]) -> list[dict]:
    return [{"value": {COL_NAME: {"value": n}, COL_TARGET: {"value": t},
                       COL_METHOD: {"value": m}, COL_STATE: {"value": STATE_TODO}}}
            for n, t, m in rows]


def is_engaged(record: dict) -> bool:
    return _v(record, FIELD_STATUS) in ENGAGED_STATUSES


def continuous_candidate(record: dict) -> bool:
    """戸籍系の行がすべて 受領（または 不要）で 1 行以上が 受領、かつ 連続戸籍充足 が yes でない。"""
    if _v(record, FIELD_CONTINUOUS) == "yes":
        return False
    koseki = [r for r in _rows(record)
              if _cell(r, COL_NAME) in _KOSEKI_DOCS
              or (_cell(r, COL_NAME) == DOC_OTHER and "戸籍" in _cell(r, COL_TARGET))]
    if not koseki:
        return False
    states = [_cell(r, COL_STATE) for r in koseki]
    return all(s in (STATE_RECEIVED, STATE_NOT_NEEDED) for s in states) and STATE_RECEIVED in states


# ── スキーマ検査（fail-closed） ───────────────────────────────────────────────
def schema_ok(properties: dict) -> bool:
    """form fields の properties から SUBTABLE の列と選択肢を検査（純関数）。"""
    table = (properties or {}).get(TABLE) or {}
    if table.get("type") != "SUBTABLE":
        return False
    fields = table.get("fields") or {}
    for col in COLUMNS:
        if col not in fields:
            return False
    for col, opts in ((COL_NAME, DOC_OPTIONS), (COL_METHOD, METHOD_OPTIONS), (COL_STATE, STATE_OPTIONS)):
        have = set((fields[col].get("options") or {}).keys())
        if not set(opts) <= have:
            return False
    return True


async def _schema_verified(*, now: float | None = None) -> bool:
    t = time.monotonic() if now is None else now
    if _schema_cache["ok"] is not None and t - _schema_cache["at"] < SCHEMA_TTL_SECONDS:
        return bool(_schema_cache["ok"])
    try:
        props = await kintone.get_form_fields(APP_HOUKI_CASE)
        ok = schema_ok(props)
    except Exception:
        logger.warning("[HOUKI_KOSEKI] form fields lookup failed (no write)")
        return False
    _schema_cache.update(at=t, ok=ok)
    if not ok:
        logger.error("[HOUKI_KOSEKI] subtable schema mismatch (no write)")
        await _notify_admin("【書類チェック・要確認】App 40 の書類チェック（サブテーブル）の列または選択肢が想定と"
                            "異なるため、初期行の自動生成を止めています。列（書類名・対象者・取得方法・書類状態）と"
                            "選択肢を確認してください。", "houki_koseki_schema")
    return ok


def reset_schema_cache() -> None:
    _schema_cache.update(at=0.0, ok=None)


# ── 通知 ───────────────────────────────────────────────────────────────────
async def _notify_admin(text: str, key: str) -> None:
    try:
        from hub.notify import notify_admin_line
        await notify_admin_line(text, throttle_key=key)
    except Exception:
        logger.error("[HOUKI_KOSEKI] admin notify failed (fixed text)")


async def _push_admin_http(admin_id: str, text: str) -> bool:
    if admin_id not in business_channel_allowlist():
        logger.warning("[HOUKI_KOSEKI] push skipped (recipient not allowlisted)")
        return False
    token = os.environ.get(business_token_env(), "")
    if not token:
        logger.warning("[HOUKI_KOSEKI] push skipped (no business token)")
        return False
    async with httpx.AsyncClient() as client:
        resp = await client.post(_PUSH_URL, headers={"Authorization": f"Bearer {token}",
                                                     "Content-Type": "application/json"},
                                 json={"to": admin_id, "messages": [{"type": "text", "text": text[:4900]}]})
    return bool(resp.is_success)


async def send_notice(key: str, text: str) -> str:
    """送信台帳を通した 1 通（hub/houki_jukuryo.send_notice と同型・自動再送なし）。
    戻り値: sent / duplicate / unconfirmed / failed / ledger_unavailable。"""
    admin_id = get_admin_line_user_id()
    if not admin_id:
        logger.warning("[HOUKI_KOSEKI] notice skipped (no admin id)")
        return "failed"
    token = send_ledger.bind_inbound(key)
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(BUSINESS, LEDGER_CHANNEL, admin_id, text)
        if op is send_ledger.DUPLICATE_SENT or op is send_ledger.DUPLICATE_UNCONFIRMED:
            return "duplicate"
        if not isinstance(op, send_ledger.Operation):
            logger.warning("[HOUKI_KOSEKI] notice skipped (ledger unavailable)")
            return "ledger_unavailable"
        try:
            ok = await send_ledger.guarded_http(op, _push_admin_http(admin_id, text))
        except Exception:
            await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
            return "unconfirmed"
        led = await send_ledger.finish(op, send_ledger.STATE_SENT if ok else send_ledger.STATE_FAILED)
        if not ok:
            return "failed"
        return "sent" if led == "applied" else "unconfirmed"
    finally:
        send_ledger.unbind(token)


def candidate_text(record_id: str) -> str:
    return (f"【連続戸籍充足の候補】案件レコードNo.{record_id} の書類チェックで戸籍系の書類がすべて受領になりました。"
            "出生から死亡までの連続が完成しているか確認し、問題なければ kintone の「連続戸籍充足」を yes にしてください"
            "（宣言は弁護士のみ・自動では書き込みません）。")


def individual_text(record_id: str) -> str:
    return (f"【必要書類の個別確定が必要】案件レコードNo.{record_id} は先順位放棄または続柄が未確定のため、"
            "共通の書類（除票・委任状・印紙・郵券）だけを書類チェックに生成しました。必要な戸籍を kintone で"
            "個別に確定してください。")


# ── 初期行の生成（CAS・冪等） ───────────────────────────────────────────────
async def init_checklist(record_id: str, record: dict | None = None) -> str:
    """受任系のレコードに初期行を 1 回だけ生成する。戻り値は RESULT_*。"""
    record_id = str(record_id)
    try:
        rec = record if record is not None else await kintone.get_record(APP_HOUKI_CASE, record_id)
    except Exception:
        logger.warning("[HOUKI_KOSEKI] record fetch failed (no write)")
        return RESULT_FAILED
    if not is_engaged(rec):
        return RESULT_NOT_ENGAGED
    if _rows(rec):
        return RESULT_EXISTS
    if not await _schema_verified():
        return RESULT_SCHEMA_MISMATCH
    rows, individual = plan_rows(rec)
    for attempt in range(2):
        try:
            await kintone.update_record(APP_HOUKI_CASE, record_id, {TABLE: to_subtable(rows)},
                                        revision=_v(rec, "$revision") or None)
            break
        except kintone.KintoneConflict:
            if attempt == 1:
                logger.info("[HOUKI_KOSEKI] cas lost twice (no write) record_id=%s",
                            emit(record_id, "record_id", "log", "operator"))
                return RESULT_CAS_LOST
            try:
                rec = await kintone.get_record(APP_HOUKI_CASE, record_id)
            except Exception:
                return RESULT_FAILED
            if _rows(rec):
                return RESULT_EXISTS
            if not is_engaged(rec):
                return RESULT_NOT_ENGAGED
        except Exception:
            logger.error("[HOUKI_KOSEKI] checklist write failed record_id=%s",
                         emit(record_id, "record_id", "log", "operator"))
            return RESULT_FAILED
    logger.info("[HOUKI_KOSEKI] checklist initialized record_id=%s rows=%s individual=%s",
                emit(record_id, "record_id", "log", "operator"), emit(len(rows), "count", "log", "operator"),
                emit(int(individual), "count", "log", "operator"))
    if individual:
        await send_notice(f"koseki_individual:{record_id}", individual_text(record_id))
        return RESULT_INDIVIDUAL
    return RESULT_CREATED


async def check_candidate(record_id: str, record: dict) -> str:
    """連続戸籍充足の候補なら 1 回通知（台帳キーで重複なし）。戻り値 sent/duplicate/none/…"""
    if not continuous_candidate(record):
        return "none"
    return await send_notice(f"koseki_complete:{record_id}", candidate_text(str(record_id)))


async def process_record(record_id: str, record: dict | None = None) -> dict:
    """受け口・走査の共通処理: 初期化 → 候補通知。"""
    rid = str(record_id)
    try:
        rec = record if record is not None else await kintone.get_record(APP_HOUKI_CASE, rid)
    except Exception:
        return {"init": RESULT_FAILED, "candidate": "none"}
    init = await init_checklist(rid, rec)
    if init in (RESULT_CREATED, RESULT_INDIVIDUAL):
        return {"init": init, "candidate": "none"}          # 生成直後は全行「未」＝候補にならない
    cand = await check_candidate(rid, rec) if init == RESULT_EXISTS else "none"
    return {"init": init, "candidate": cand}


# ── 定期走査 ─────────────────────────────────────────────────────────────────
async def fetch_engaged(limit: int = SCAN_LIMIT) -> list[dict]:
    q = ("status in (" + ", ".join(f'"{s}"' for s in ENGAGED_STATUSES) + f") order by $id asc limit {int(limit)}")
    return await kintone.search_records(APP_HOUKI_CASE, q,
                                        fields=["$id", "$revision", FIELD_STATUS, FIELD_KANKEI, FIELD_COMPLEX,
                                                FIELD_CONTINUOUS, TABLE])


async def scan_once() -> dict:
    """受任系レコードを走査: 書類チェックが空なら初期化・揃っていれば候補通知。件数だけログ。"""
    if disabled():
        return {"skipped": "disabled"}
    if not (APP_HOUKI_CASE.app_id() and APP_HOUKI_CASE.token()):
        return {"skipped": "unconfigured"}
    try:
        records = await fetch_engaged()
    except Exception:
        logger.warning("[HOUKI_KOSEKI] scan fetch failed (fixed reason)")
        return {"skipped": "fetch_failed"}
    counts: dict = {"records": len(records), "created": 0, "individual": 0, "candidate": 0}
    for rec in records:
        rid = _v(rec, "$id")
        try:
            res = await process_record(rid, rec)
        except Exception:
            logger.warning("[HOUKI_KOSEKI] record processing failed (fixed reason)")
            continue
        if res["init"] == RESULT_CREATED:
            counts["created"] += 1
        elif res["init"] == RESULT_INDIVIDUAL:
            counts["individual"] += 1
        if res["candidate"] == "sent":
            counts["candidate"] += 1
    logger.info("[HOUKI_KOSEKI] scan done records=%s created=%s individual=%s candidate=%s",
                emit(counts["records"], "count", "log", "operator"), emit(counts["created"], "count", "log", "operator"),
                emit(counts["individual"], "count", "log", "operator"), emit(counts["candidate"], "count", "log", "operator"))
    return counts


def register_houki_koseki_job() -> None:
    """FastAPI startup（main 末尾）から呼ぶ。実行可否は scan_once 先頭の env 検査（既定 有効）。"""
    if not hub_scheduler.is_registered(SCAN_JOB_NAME):
        hub_scheduler.register_interval(SCAN_JOB_NAME, SCAN_INTERVAL_MINUTES, functools.partial(scan_once))
