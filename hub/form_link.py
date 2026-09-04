"""受付番号による LINE 紐付け（JIKOU-FORM-2）

HP の時効診断フォーム（shindan_form.py・FORM-1）が発行した 6 桁の受付番号を
時効 LINE で受け取り、App 21 のフォーム由来レコードに LINEユーザーID を紐付ける。
紐付け後は既存ヒアリングフロー（main._process_line_event）へ同一ターンで流し、
既知項目台帳（chat_responder.build_known_items）がフォーム回答 4 項目を既知として
注入する（残りだけ聞く）。

設計（票の逐語）:
- 検知: App 21 に未紐付け（get_app21_record が None）のユーザーのテキストが
  「6 桁の数字のみ」（NFKC 正規化で全角数字・前後空白を許容）のとき。
  検知位置は main 側（pause/停止リスト判定の後）
- 照合: 受付番号=N かつ LINEユーザーID="" かつ 受付チャネル=フォーム を照会し、
  **ちょうど 1 件**かつ作成日時が RECEIPT_TTL_DAYS 以内なら該当。0 件・複数件・
  期限切れ・（防御的再検査で）チャネル≠フォーム／userId 非空は不該当
- 紐付け: LINEユーザーID を $revision CAS で書込（KintoneConflict=cas_lost・
  作用 0・不該当扱い）。plain 値契約（hub.kintone._wrap が包む）
- 総当たり対策: userId 別 ATTEMPT_LIMIT 回/ATTEMPT_WINDOW_SECONDS の固定窓
  （OrderedDict LRU・MAX_ATTEMPT_BUCKETS 厳密上限・shindan_form 同型）。
  超過後は無言（固定文言 B も返さない）+弁護士通知 1 回（超過の瞬間のみ・
  固定文言・userId 先頭 6 文字のみ）
- 応答規律: 完全一致・存在有無を応答で区別しない（不該当は固定文言 B 統一）。
  レコード番号・他人の情報を応答に載せない
- 弁護士通知（紐付け成功）:「【フォーム紐付け】受付番号:xxxxxx → レコード番号:N」
  のみ（notify_business 流儀・PII なし・best-effort）
- 固定文言 A/B は司令塔案（大野裁定で差し替え可・test_jikou_form2 が sha256 pin）
"""

import hashlib
import logging
import os
import re
import time
import unicodedata
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from hub import kintone as hub_kintone
from hub import notify
from hub.redact import emit

logger = logging.getLogger("form_link")

APP_JIKOU_CASE = hub_kintone.KintoneApp(
    "App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")

NUMBER_FIELD = "受付番号"
USER_FIELD = "LINEユーザーID"
CHANNEL_FIELD = "受付チャネル"
CREATED_FIELD = "作成日時"
CHANNEL_FORM = "フォーム"

RECEIPT_TTL_DAYS = 30

# ── お客様向け固定文言（司令塔案・凍結後は sha256 pin） ─────────────────────────
# A: 紐付け成功・AI 失敗時のフォールバック
REPLY_LINKED_FALLBACK = (
    "受付番号を確認しました。診断フォームでご回答いただいた内容を引き継いで"
    "おります。続きをお伺いしますので、少々お待ちください。")
# B: 不該当（存在有無を区別しない同一文言）
REPLY_NOT_MATCHED = (
    "受付番号が確認できませんでした。番号をお確かめのうえ、もう一度お送り"
    "ください。受付番号をお持ちでない場合は、そのままご相談内容をお送り"
    "いただいて構いません。")

# AI へ「フォーム回答を引き継いだ直後のターン」である旨を一度だけ注入する文
# （main.ask_claude が form_handover=True のターンにのみ system へ追記）
HANDOVER_PROMPT_NOTE = (
    "【フォーム回答の引き継ぎ（このターンのみ）】\n"
    "直前に受付番号が確認され、HP の診断フォームでお客様が回答した内容"
    "（上記の収集済み項目）を引き継ぎました。冒頭でフォームの回答を"
    "引き継いだ旨を一言添え、既知の項目は聞き直さずに、残りの未回答項目の"
    "質問へ進んでください。受付番号そのものへの言及や再確認は不要です。")

# ── 検知 ────────────────────────────────────────────────────────────────────────
_RECEIPT_RE = re.compile(r"[0-9]{6}")


def detect_receipt_number(text: str) -> str | None:
    """テキストが「6 桁の数字のみ」なら半角 6 桁を返す（NFKC で全角数字・
    全角空白を正規化し前後空白を除去）。それ以外は None。"""
    s = unicodedata.normalize("NFKC", text or "").strip()
    return s if _RECEIPT_RE.fullmatch(s) else None


# ── 試行回数制限（userId 別・固定窓・OrderedDict LRU 厳密上限） ──────────────────
ATTEMPT_LIMIT = 5
ATTEMPT_WINDOW_SECONDS = 3600
MAX_ATTEMPT_BUCKETS = 5000
_PRUNE_STEPS = 2
_attempts: "OrderedDict[str, tuple[float, int]]" = OrderedDict()


def _attempt_key(user_id: str) -> str:
    return hashlib.sha256(user_id.encode("utf-8")).hexdigest()


def record_attempt(user_id: str, now: float) -> str:
    """紐付け試行を 1 回計上し、状態を返す:
    allow=上限内／exceeded_first=この試行で初めて上限超過（通知 1 回の契機）／
    exceeded=既に超過中。窓満了で自然解除。期限切れ掃除は先頭から定数ステップ。"""
    key = _attempt_key(user_id)
    for _ in range(_PRUNE_STEPS):
        if not _attempts:
            break
        oldest = next(iter(_attempts))
        if now - _attempts[oldest][0] >= ATTEMPT_WINDOW_SECONDS:
            del _attempts[oldest]
        else:
            break
    if key in _attempts:
        start, count = _attempts[key]
        if now - start >= ATTEMPT_WINDOW_SECONDS:
            start, count = now, 0
        _attempts.move_to_end(key)
    else:
        start, count = now, 0
        while len(_attempts) >= MAX_ATTEMPT_BUCKETS:
            _attempts.popitem(last=False)
    count += 1
    _attempts[key] = (start, count)
    if count <= ATTEMPT_LIMIT:
        return "allow"
    return "exceeded_first" if count == ATTEMPT_LIMIT + 1 else "exceeded"


# ── 照合・紐付け ─────────────────────────────────────────────────────────────────
def _value(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "")


def _parse_created(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _within_ttl(record: dict, now: float) -> bool:
    created = _parse_created(_value(record, CREATED_FIELD))
    if created is None:
        return False                                  # 作成日時不明=不該当
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
    return now_dt - created <= timedelta(days=RECEIPT_TTL_DAYS)


async def find_form_record(number: str, now: float) -> dict | None:
    """受付番号でフォーム由来・未紐付けのレコードをちょうど 1 件に確定する。"""
    query = (f'{NUMBER_FIELD} = "{number}" and {USER_FIELD} = "" '
             f'and {CHANNEL_FIELD} in ("{CHANNEL_FORM}")')
    rows = await hub_kintone.search_records(
        APP_JIKOU_CASE, query,
        fields=["$id", "$revision", NUMBER_FIELD, USER_FIELD, CHANNEL_FIELD,
                CREATED_FIELD])
    if len(rows) != 1:
        logger.info("[FORM_LINK] not exactly one row count=%s",
                    emit(len(rows), "count", "log", "operator"))
        return None
    rec = rows[0]
    # 防御的再検査（query と同条件を module 側でも確認する）
    if (_value(rec, NUMBER_FIELD) != number
            or _value(rec, USER_FIELD) != ""
            or _value(rec, CHANNEL_FIELD) != CHANNEL_FORM):
        logger.info("[FORM_LINK] row failed re-check")
        return None
    if not _within_ttl(rec, now):
        logger.info("[FORM_LINK] row expired (ttl)")
        return None
    return rec


async def bind_record(user_id: str, record: dict) -> str | None:
    """LINEユーザーID を $revision CAS で書込。成功=record_id・409/失敗=None。"""
    rid = _value(record, "$id")
    rev = _value(record, "$revision")
    if not rid or not rev:
        return None
    try:
        await hub_kintone.update_record(
            APP_JIKOU_CASE, rid, {USER_FIELD: user_id}, revision=rev)
    except hub_kintone.KintoneConflict:
        logger.info("[FORM_LINK] cas_lost record_id=%s",
                    emit(rid, "record_id", "log", "operator"))
        return None
    except hub_kintone.KintoneError as e:
        logger.warning("[FORM_LINK] bind failed code=%s",
                       emit(e.code, "vendor_raw", "log", "operator"))
        return None
    return rid


def _attorney_id() -> str:
    return os.environ.get("ATTORNEY_LINE_USER_ID", "")


async def _notify(text: str) -> None:
    to = _attorney_id()
    if not to:
        logger.info("[FORM_LINK] ATTORNEY_LINE_USER_ID not set, notify skipped")
        return
    try:
        sent = await notify.notify_business(to, text)
    except Exception:
        sent = False
    if not sent:
        logger.warning("[FORM_LINK] attorney notify failed")


async def try_link(user_id: str, number: str,
                   now: float | None = None) -> tuple[str, str | None]:
    """紐付け処理の単一入口。戻り値 (outcome, record_id):
    linked=紐付け成功／not_matched=不該当（固定文言 B）／silent=試行上限超過
    （無言・超過の瞬間に弁護士通知 1 回）。"""
    now = time.time() if now is None else now
    state = record_attempt(user_id, now)
    if state != "allow":
        if state == "exceeded_first":
            logger.warning("[FORM_LINK] attempt limit exceeded (silent)")
            await _notify(
                "【フォーム紐付け・要確認】受付番号の照合回数が上限を超えたため、"
                "以後の照合を無応答にしています（LINEユーザーID先頭:"
                f"{user_id[:6]}）。総当たりの可能性があります。")
        return "silent", None
    try:
        rec = await find_form_record(number, now)
    except hub_kintone.KintoneError as e:
        logger.warning("[FORM_LINK] search failed code=%s",
                       emit(e.code, "vendor_raw", "log", "operator"))
        rec = None
    if rec is None:
        return "not_matched", None
    rid = await bind_record(user_id, rec)
    if rid is None:
        return "not_matched", None
    logger.info("[FORM_LINK] linked record_id=%s",
                emit(rid, "record_id", "log", "operator"))
    await _notify(f"【フォーム紐付け】受付番号:{number} → レコード番号:{rid}")
    return "linked", rid


# ── fix1-01: 紐付け後の再評価用の最新状態取得（fail-closed の材料） ──────────────
async def fetch_linked_record(record_id: str) -> dict | None:
    """紐付け先レコードを ID で再取得（GET・冪等）。失敗は None（呼び出し元が
    fail-closed=自動返信しない+弁護士通知）。userId 検索の成否に依存しない。"""
    try:
        return await hub_kintone.get_record(APP_JIKOU_CASE, record_id)
    except hub_kintone.KintoneError as e:
        logger.warning("[FORM_LINK] post-link refetch failed code=%s",
                       emit(e.code, "vendor_raw", "log", "operator"))
        return None


async def notify_fail_closed(record_id: str) -> None:
    await _notify(
        "【フォーム紐付け・要確認】紐付けは成立しました（レコード番号:"
        f"{record_id}）が、直後のレコード再取得に失敗したため自動返信を行って"
        "いません。App 21 のレコードと受信内容（App 28）を確認し、必要なら"
        "手動で対応してください。")


# ── fix1-03: ヒアリング内容の既存レコードへの統合（$revision CAS・空欄のみ） ─────
# Bot が埋める欄の閉集合（KINTONE_RECORD マーカーの 5 項目）。これ以外の欄
# （顧客名・status・LINEユーザーID 等）はマーカーに含まれても書かない
BOT_FILL_FIELDS = frozenset({
    "問い合わせ業者名", "借入時期_テキスト", "最終返済日_テキスト",
    "裁判所書類", "信用情報確認"})
MERGE_RETRIES = 3        # 409 → 再取得・再構成の再試行上限


def _merge_plan(latest: dict, candidate: dict) -> dict:
    """最新レコードで空欄の Bot 欄にだけ候補値を入れる（人の値は上書きしない・
    H-3 upsert_case_fields の「空欄のみ」流儀）。"""
    return {k: v for k, v in candidate.items() if _value(latest, k) == ""}


async def merge_hearing_fields(record_id: str, fields: dict) -> str:
    """既存レコードへヒアリング抽出値を統合する。戻り値:
    updated=書込成立／noop=埋める欄なし（書込 0）／
    unconverged=409 が再試行上限まで続いた（上書きせず中止+要確認通知）／
    failed=取得/書込の確定失敗（書込 0+要確認通知）。"""
    candidate = {k: str(v) for k, v in (fields or {}).items()
                 if k in BOT_FILL_FIELDS and str(v or "").strip()}
    outcome = "unconverged"
    for _attempt in range(MERGE_RETRIES + 1):
        try:
            latest = await hub_kintone.get_record(APP_JIKOU_CASE, record_id)
        except hub_kintone.KintoneError as e:
            logger.warning("[FORM_LINK] merge refetch failed code=%s",
                           emit(e.code, "vendor_raw", "log", "operator"))
            outcome = "failed"
            break
        to_write = _merge_plan(latest, candidate)
        if not to_write:
            return "noop"
        try:
            await hub_kintone.update_record(
                APP_JIKOU_CASE, record_id, to_write,
                revision=_value(latest, "$revision"))
            return "updated"
        except hub_kintone.KintoneConflict:
            logger.info("[FORM_LINK] merge cas conflict (retry)")
            continue
        except hub_kintone.KintoneError as e:
            logger.warning("[FORM_LINK] merge update failed code=%s",
                           emit(e.code, "vendor_raw", "log", "operator"))
            outcome = "failed"
            break
    logger.error("[FORM_LINK] merge not applied outcome=%s record_id=%s",
                 emit(outcome, "freetext", "log", "operator"),
                 emit(record_id, "record_id", "log", "operator"))
    await notify.notify_admin_line(
        "【フォーム紐付け・要確認】ヒアリング内容の既存レコードへの統合更新が"
        f"確定できませんでした（区分:{outcome} レコード番号:{record_id}）。"
        "上書きせず中止しています。App 21 のレコードと App 28 の会話を確認して"
        "ください。",
        throttle_key="form_link_merge",
    )
    return outcome
