"""相続放棄 受理通知書写しの発送 起票（hub/houki_soufu・HOUKI-SOUFU-1）

App 40（相続放棄案件）が status=受理 かつ 受理通知受領日 非空 かつ 受理通知書（FILE）添付
のとき、債権者一覧 の対象行（通知要否=要 かつ 送付状態=未 かつ 債権者住所 非空）ごとに
App 30（発送管理）へ **下書き** で 1 件起票し、以後は既存の /hub/dispatch（M4 送付案内）の
関所（承認待ち→大野が 承認済→発送処理中→事務員が 発送済→完了）に乗せる。
承認を経ない発送経路は作らない（App 30 の 発送ステータス を 下書き より先へ進める
コードは本 module に無い）。

裁定（第 1 段 D1〜D9・2026-09-08）:
- D1 送付単位: 申述人（レコード）ごとに、その 債権者一覧 の各行へ 1 通（連名にしない）。
- D2 対象行: 通知要否=要 かつ 送付状態=未 のみ。
- D3 住所: 債権者住所 が空の 要 行は起票せず要確認通知。債権者郵便番号 は空でも起票可。
- D4 4 か所目以降: 起票する。追加料金対象=yes（管理レコード先頭・番号昇順・行順で、
  正規化した債権者名の和集合における出現順で 4 番目以降＝契約書と同じ規則）。
- D5 status: 1 件でも起票できたら 受理→債権者通知 を一方向 CAS で 1 回だけ。完了 は人。
- 送付状の必須値（10 欄）が 1 つでも空なら起票 0 ＋要確認通知（欠けている欄名のみ）。

冪等（fix1 A・Codex HS-01）: 行ごとに App 40 の $revision CAS で 未→起票済・送付発送管理No=起票中 を
claim → 既存検索（キー houki_soufu:{record_id}:{row_id}・App 30 チャネル固有データ）→ 無ければ
App 30 作成 → 番号を CAS で書く。claim の 409 は他者が処理中としてスキップ。作成/番号書込に失敗した
行は 起票中 のまま残り、次回実行で回収（既存があれば番号、無ければ作成）。record_id ごとの
asyncio.Lock（in-memory・単一 worker）で同一レコードの並行処理を直列化。
起点（fix1 B・HS-02）: status ∈ {受理, 債権者通知}（債権者通知 でも未 行の起票と回収を行う）。
受理→債権者通知 の遷移は現在が 受理 のときだけ 1 回。
書き戻し: App 30 が 発送済/完了 になったとき（hub/dispatch）、案件アプリID=App 40 の
レコードだけ 債権者一覧 の該当行を 送付状態=送付済 に更新（CAS・409 は再取得 1 回）。
App 40 で書くのは 債権者一覧（送付状態／送付発送管理No／追加料金対象）と status の
一方向遷移のみ。通知本文に個人情報は載せない（レコード番号・行番号・欄名のみ）。
"""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from hub import kintone, notify
from hub import houki_contract as hc
from hub import houki_soufu_letter as letter
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit

logger = logging.getLogger("hub.houki_soufu")

APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")
APP_ENCLOSURE = kintone.KintoneApp("App 32 (同封物ブロックマスタ)", "APP_ENCLOSURE", "TOKEN_ENCLOSURE")

# ── App 40 ──────────────────────────────────────────────────────────────────
FIELD_STATUS = "status"
STATUS_ACCEPTED = "受理"
STATUS_NOTIFYING = "債権者通知"
STATUS_COMPLETED = "完了"
FIELD_ACCEPT_RECEIVED = "受理通知受領日"
FIELD_ACCEPT_FILE = "受理通知書"
FIELD_GROUP = "被相続人グループID"
FIELD_CUSTOMER = "顧客名"
CREDITOR_TABLE = "債権者一覧"
COL_NAME = "債権者名"
COL_ZIP = "債権者郵便番号"
COL_ADDR = "債権者住所"
COL_NOTIFY = "通知要否"
COL_STATE = "送付状態"
COL_SHIP_NO = "送付発送管理No"
COL_EXTRA = "追加料金対象"
NOTIFY_REQUIRED = "要"
STATE_TODO = "未"
STATE_FILED = "起票済"
STATE_SENT = "送付済"
SHIP_NO_CLAIMING = "起票中"                                 # fix1 A: claim 中の 送付発送管理No（fix2: 起票中:{token}:{期限}）
CLAIM_TTL_SEC = 600                                         # fix2 A-1: claim の期限（10 分）
DUPLICATE_ERROR_DETAIL = "二重起票（正: 発送管理 No.{canonical}）。本レコードは使用しません。"   # fix2 A-4
PENDING_DEDUPE_KEY = "pending_dedupe"                       # fix3 A-1: App 30 チャネル固有データ のフラグ
TRIGGER_STATUSES = (STATUS_ACCEPTED, STATUS_NOTIFYING)      # fix1 B: 債権者通知 でも回収
EXTRA_YES = "yes"
EXTRA_NO = "no"
INCLUDED_DESTINATIONS = hc.INCLUDED_DESTINATIONS          # 3（契約書と同じ規則）

# ── App 30 ──────────────────────────────────────────────────────────────────
UNIT = "相続放棄"
CHANNEL = "送付案内"
ENCLOSURE_BLOCK_KEY = "受理通知書写し"                     # App 32 ブロックキー＝App 30 同封物選択 の選択肢
SHIPPING_STATUS_DRAFT = "下書き"
KEY_PREFIX = "houki_soufu"
CHANNEL_DATA_KEY = "houki_soufu_key"
_CAS_RETRIES = 2                                          # 初回 + 409 後の再取得 1 回

NOTIFY_KIND_FILED = "houki_soufu_filed"
NOTIFY_KIND_REVIEW = "houki_soufu_needs_review"
NOTICE_HEAD_FILED = "【相続放棄 受理通知送付】"
NOTICE_HEAD_REVIEW = "【相続放棄 受理通知送付・要確認】"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def soufu_key(record_id: str, row_id: str) -> str:
    return f"{KEY_PREFIX}:{record_id}:{row_id}"


def rows_of(record: dict) -> list[dict]:
    return list(((record.get(CREDITOR_TABLE) or {}).get("value") or []))


def row_val(row: dict, col: str) -> str:
    return str((((row.get("value") or {}).get(col) or {}).get("value") or "")).strip()


def row_id(row: dict) -> str:
    return str(row.get("id") or "")


def is_triggered(record: dict) -> bool:
    """起点条件: status ∈ {受理, 債権者通知}（fix1 B・債権者通知 は未 行の起票と回収のため）
    かつ 受理通知受領日 非空 かつ 受理通知書 添付あり（後 2 条件は凍結どおり）。"""
    files = (record.get(FIELD_ACCEPT_FILE) or {}).get("value") or []
    return (_v(record, FIELD_STATUS) in TRIGGER_STATUSES
            and bool(_v(record, FIELD_ACCEPT_RECEIVED)) and bool(files))


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def new_owner_token() -> str:
    """fix2 A-1: 実行ごとの所有者 token（uuid4 先頭 12 桁）。"""
    return uuid.uuid4().hex[:12]


def claim_value(token: str, now: datetime | None = None) -> str:
    """送付発送管理No の claim 値「起票中:{owner_token}:{expires_at ISO}」（期限 CLAIM_TTL_SEC）。"""
    exp = (now or _now_utc()) + timedelta(seconds=CLAIM_TTL_SEC)
    return f"{SHIP_NO_CLAIMING}:{token}:{exp.strftime('%Y-%m-%dT%H:%M:%SZ')}"


def parse_claim(value: str) -> tuple[str, datetime | None] | None:
    """claim 値を (token, expires_at) に分解。claim でなければ None。旧形式「起票中」は ("", None)。"""
    v = str(value or "").strip()
    if v == SHIP_NO_CLAIMING:
        return "", None
    if not v.startswith(SHIP_NO_CLAIMING + ":"):
        return None
    parts = v.split(":", 2)
    if len(parts) != 3:
        return "", None
    try:
        exp = datetime.strptime(parts[2], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        exp = None
    return parts[1], exp


def claim_owner(row: dict) -> str | None:
    """行の claim token（claim でなければ None）。"""
    parsed = parse_claim(row_val(row, COL_SHIP_NO))
    return None if parsed is None else parsed[0]


def claim_expired(row: dict, now: datetime | None = None) -> bool:
    """claim が期限切れ（旧形式・期限不明も期限切れ扱い）。claim でなければ False。"""
    parsed = parse_claim(row_val(row, COL_SHIP_NO))
    if parsed is None:
        return False
    _token, exp = parsed
    return exp is None or exp <= (now or _now_utc())


def is_recovery_row(row: dict, now: datetime | None = None) -> bool:
    """fix2 A-2: 回収対象＝送付状態=起票済 かつ（番号空〔旧形式〕または claim が期限切れ）。
    期限内の claim は稼働中の所有者がいるものとして触らない。"""
    if row_val(row, COL_STATE) != STATE_FILED:
        return False
    ship_no = row_val(row, COL_SHIP_NO)
    if ship_no == "":
        return True
    return parse_claim(ship_no) is not None and claim_expired(row, now)


def is_active_claim(row: dict, now: datetime | None = None) -> bool:
    return (row_val(row, COL_STATE) == STATE_FILED and parse_claim(row_val(row, COL_SHIP_NO)) is not None
            and not claim_expired(row, now))


def classify_rows(record: dict, now: datetime | None = None
                  ) -> tuple[list[tuple[int, dict]], list[int], list[tuple[int, dict]]]:
    """(起票対象 [(行番号, row)], 住所未入力の 要 行番号, 回収対象 [(行番号, row)])。行番号は 1 始まり。
    期限内の 起票中 は分類に入れない（稼働中）。"""
    targets: list[tuple[int, dict]] = []
    no_addr: list[int] = []
    recovery: list[tuple[int, dict]] = []
    for i, row in enumerate(rows_of(record), 1):
        if not row_id(row):
            continue
        if is_recovery_row(row, now):
            recovery.append((i, row))
            continue
        if row_val(row, COL_NOTIFY) != NOTIFY_REQUIRED or row_val(row, COL_STATE) != STATE_TODO:
            continue
        if not row_val(row, COL_ADDR):
            no_addr.append(i)
            continue
        targets.append((i, row))
    return targets, no_addr, recovery


_locks: dict[str, tuple[object, asyncio.Lock]] = {}


def _lock_for(record_id: str) -> asyncio.Lock:
    """fix1 A-5: record_id ごとの asyncio.Lock（in-memory・単一 worker・永続 claim の補助）。
    ループが変わったら作り直す（テスト・再起動）。"""
    loop = asyncio.get_running_loop()
    entry = _locks.get(record_id)
    if entry is None or entry[0] is not loop:
        entry = (loop, asyncio.Lock())
        _locks[record_id] = entry
    return entry[1]


def extra_fee_names(group: list[dict]) -> set[str]:
    """D4: 管理レコード先頭・番号昇順・行順で正規化した債権者名の和集合を作り、
    出現順で INCLUDED_DESTINATIONS+1 番目以降を追加料金対象とする。"""
    order: list[str] = []
    seen: set[str] = set()
    for rec in group:
        for row in rows_of(rec):
            key = hc.normalize_creditor(row_val(row, COL_NAME))
            if key and key not in seen:
                seen.add(key)
                order.append(key)
    return set(order[INCLUDED_DESTINATIONS:])


def order_group(record: dict, members: list[dict]) -> list[dict]:
    """管理レコード（被相続人グループID == 自レコード番号）を先頭、他は番号昇順。"""
    gid = _v(record, FIELD_GROUP)
    by_id = {_v(r, "$id"): r for r in members}
    by_id[_v(record, "$id")] = record
    ordered = sorted(by_id.values(), key=lambda r: int(_v(r, "$id") or 0))
    if gid and gid in by_id:
        ordered = [by_id[gid]] + [r for r in ordered if _v(r, "$id") != gid]
    return ordered


async def fetch_group(record: dict) -> list[dict]:
    gid = _v(record, FIELD_GROUP)
    if not gid:
        return [record]
    members = await kintone.search_records(
        APP_HOUKI_CASE, f'{FIELD_GROUP} = "{gid}" order by $id asc limit 500',
        fields=["$id", FIELD_GROUP, CREDITOR_TABLE])
    return order_group(record, members)


async def enclosure_problem() -> str | None:
    """同封物ブロックの実在確認（閉集合: block_missing / option_missing / None）。"""
    blocks = await kintone.search_records(APP_ENCLOSURE, '有効 in ("yes")',
                                          fields=["ブロックキー", "対象ユニット"])
    ok = any(_v(b, "ブロックキー") == ENCLOSURE_BLOCK_KEY
             and UNIT in ((b.get("対象ユニット") or {}).get("value") or []) for b in blocks)
    if not ok:
        return "block_missing"
    fields = await kintone.get_form_fields(APP_SHIPPING)
    options = set(((fields.get("同封物選択") or {}).get("options") or {}).keys())
    if ENCLOSURE_BLOCK_KEY not in options:
        return "option_missing"
    return None



def build_shipping_fields(case: dict, row: dict, key: str) -> dict:
    name = row_val(row, COL_NAME)
    return {
        "発送ステータス": SHIPPING_STATUS_DRAFT,
        "ユニット種別": UNIT,
        "チャネル": CHANNEL,
        "方向": "発送",
        "案件アプリID": str(APP_HOUKI_CASE.app_id()),
        "案件レコードID": _v(case, "$id"),
        "顧客名表示用": _v(case, FIELD_CUSTOMER),
        "件名": f"受理通知送付（{name}）",
        "宛先名": name,
        "宛先郵便番号": row_val(row, COL_ZIP),
        "宛先住所": row_val(row, COL_ADDR),
        "実行済み": "no",
        "同封物選択": [ENCLOSURE_BLOCK_KEY],
        "チャネル固有データ": json.dumps({
            CHANNEL_DATA_KEY: key, "row_id": row_id(row), "case_record_id": _v(case, "$id"),
            PENDING_DEDUPE_KEY: True,                       # fix3 A-1: 重複確認が済むまで prepare に入らない
        }, ensure_ascii=False),
    }


async def update_row(record_id: str, target_row_id: str, updates: dict,
                     latest: dict | None = None) -> bool:
    """債権者一覧 の 1 行を更新（他行・他列保全・$revision CAS・409 は再取得 1 回）。"""
    for attempt in range(_CAS_RETRIES):
        if latest is None:
            latest = await kintone.get_record(APP_HOUKI_CASE, record_id)
        rows = rows_of(latest)
        hit = False
        for row in rows:
            if row_id(row) == target_row_id:
                for col, val in updates.items():
                    row.setdefault("value", {})[col] = {"value": val}
                hit = True
        if not hit:
            logger.warning("[HOUKI_SOUFU] row not found record_id=%s",
                           emit(record_id, "record_id", "log", "operator"))
            return False
        try:
            await kintone.update_record(APP_HOUKI_CASE, record_id, {CREDITOR_TABLE: rows},
                                        revision=_v(latest, "$revision") or None)
            return True
        except kintone.KintoneConflict:
            latest = None
            if attempt + 1 >= _CAS_RETRIES:
                logger.warning("[HOUKI_SOUFU] row update CAS conflict record_id=%s",
                               emit(record_id, "record_id", "log", "operator"))
    return False


async def update_row_once(record_id: str, target_row_id: str, updates: dict, latest: dict) -> bool:
    """fix2 A-5: 与えられた最新レコードの revision で 1 回だけ CAS 更新（409 は False・再取得しない）。"""
    rows = rows_of(latest)
    hit = next((r for r in rows if row_id(r) == target_row_id), None)
    if hit is None:
        return False
    for col, val in updates.items():
        hit.setdefault("value", {})[col] = {"value": val}
    try:
        await kintone.update_record(APP_HOUKI_CASE, record_id, {CREDITOR_TABLE: rows},
                                    revision=_v(latest, "$revision") or None)
        return True
    except kintone.KintoneConflict:
        return False


async def claim_row(record_id: str, target_row_id: str, latest: dict | None,
                    token: str, extra: bool = False) -> bool:
    """fix1 A-1 / fix2 A-1: 行を 未→起票済・送付発送管理No=起票中:{token}:{期限} に CAS で claim
    （1 回のみ・409 は False＝他者が処理中としてスキップ）。追加料金対象 は claim と同時に立てる。"""
    if latest is None:
        latest = await kintone.get_record(APP_HOUKI_CASE, record_id)
    rows = rows_of(latest)
    hit = next((r for r in rows if row_id(r) == target_row_id), None)
    if hit is None or row_val(hit, COL_STATE) != STATE_TODO:
        return False
    hit["value"][COL_STATE] = {"value": STATE_FILED}
    hit["value"][COL_SHIP_NO] = {"value": claim_value(token)}
    if extra or row_val(hit, COL_EXTRA) == EXTRA_YES:
        hit["value"][COL_EXTRA] = {"value": EXTRA_YES}
    try:
        await kintone.update_record(APP_HOUKI_CASE, record_id, {CREDITOR_TABLE: rows},
                                    revision=_v(latest, "$revision") or None)
        return True
    except kintone.KintoneConflict:
        logger.info("[HOUKI_SOUFU] claim lost record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return False


async def claim_recovery_row(record_id: str, target_row_id: str, latest: dict | None,
                             token: str) -> bool:
    """fix2 A-2: 回収対象（番号空〔旧形式〕または期限切れ claim）の行を、自分の token・新しい期限に
    CAS で書き換えてから進む。期限内の claim（他者稼働中）や対象外は False。409 は False。"""
    if latest is None:
        latest = await kintone.get_record(APP_HOUKI_CASE, record_id)
    rows = rows_of(latest)
    hit = next((r for r in rows if row_id(r) == target_row_id), None)
    if hit is None or not is_recovery_row(hit):
        return False
    hit["value"][COL_SHIP_NO] = {"value": claim_value(token)}
    try:
        await kintone.update_record(APP_HOUKI_CASE, record_id, {CREDITOR_TABLE: rows},
                                    revision=_v(latest, "$revision") or None)
        return True
    except kintone.KintoneConflict:
        return False


def channel_data(shipping: dict) -> dict:
    try:
        data = json.loads(_v(shipping, "チャネル固有データ") or "{}")
    except ValueError:
        data = {}
    return data if isinstance(data, dict) else {}


def is_pending_dedupe(shipping: dict) -> bool:
    """fix3 A-1/A-2: 起票直後（重複確認前）のレコード。prepare はこれが true の間は入らない。"""
    return channel_data(shipping).get(PENDING_DEDUPE_KEY) is True


async def find_all_shipping(key: str) -> list[dict]:
    """fix2 A-4 / fix3 B-1: 同じ houki_soufu_key を持つ App 30 レコード全件（$id 昇順・キー完全一致）。
    番号復旧・作成後のどちらもこれを通す（先頭 1 件だけ見る経路は無い）。"""
    records = await kintone.search_records(
        APP_SHIPPING, f'チャネル固有データ like "{key}" order by $id asc limit 100',
        fields=["$id", "$revision", "発送ステータス", "チャネル固有データ"])
    out = [r for r in records if channel_data(r).get(CHANNEL_DATA_KEY) == key]
    return sorted(out, key=lambda r: int(_v(r, "$id") or 0))


async def _clear_pending_cas(shipping: dict) -> bool:
    """正のレコードの pending_dedupe を false に CAS 更新（この編集が App 30 の webhook を発火させ
    dispatcher が prepare を始める）。409 は再取得 1 回で再判定。"""
    rec = shipping
    for attempt in range(_CAS_RETRIES):
        data = channel_data(rec)
        if data.get(PENDING_DEDUPE_KEY) is not True:
            return True
        data[PENDING_DEDUPE_KEY] = False
        try:
            await kintone.update_record(APP_SHIPPING, _v(rec, "$id"),
                                        {"チャネル固有データ": json.dumps(data, ensure_ascii=False)},
                                        revision=_v(rec, "$revision") or None)
            return True
        except kintone.KintoneConflict:
            rec = await kintone.get_record(APP_SHIPPING, _v(rec, "$id"))
    return False


async def _void_duplicate_cas(record_id: str, shipping: dict, canonical: str) -> bool:
    """正以外のレコード: 再取得した現在状態が 下書き かつ pending_dedupe=true のものだけ
    下書き→エラー（エラー詳細に正の番号）を revision つき CAS で書く。物理削除は RV-08 pin により行わない。
    hub/approval.transition は revision を受け取らない（凍結）ため、同じ遷移表（SERVER_TRANSITIONS）で
    許容を確認したうえで本 module が CAS 書込を行う。409 は再取得 1 回で再判定。対象外・失敗は False。"""
    from hub import approval
    assert (SHIPPING_STATUS_DRAFT, "エラー") in approval.SERVER_TRANSITIONS
    rec = shipping
    for attempt in range(_CAS_RETRIES):
        if _v(rec, "発送ステータス") != SHIPPING_STATUS_DRAFT or not is_pending_dedupe(rec):
            return False
        data = channel_data(rec)
        data[PENDING_DEDUPE_KEY] = False
        try:
            await kintone.update_record(
                APP_SHIPPING, _v(rec, "$id"),
                {"発送ステータス": "エラー",
                 "エラー詳細": DUPLICATE_ERROR_DETAIL.format(canonical=canonical),
                 "チャネル固有データ": json.dumps(data, ensure_ascii=False)},
                revision=_v(rec, "$revision") or None)
            logger.info("[HOUKI_SOUFU] duplicate draft voided record_id=%s shipping=%s canonical=%s",
                        emit(record_id, "record_id", "log", "operator"),
                        emit(_v(rec, "$id"), "record_id", "log", "operator"),
                        emit(canonical, "record_id", "log", "operator"))
            return True
        except kintone.KintoneConflict:
            rec = await kintone.get_record(APP_SHIPPING, _v(rec, "$id"))
    return False


async def resolve_duplicates(record_id: str, key: str, found: list[dict]) -> tuple[str, bool]:
    """fix3 A-3: 同キー全件（$id 昇順）から番号最小を正とし、正は pending_dedupe を外す。正以外は
    下書き∧pending のものだけ エラー 化。エラー 化できないもの（既に 下書き でない・pending 済み・CAS 失敗）が
    残れば要確認通知（両番号）。戻り値 (正の番号, 未解決あり)。"""
    canonical = _v(found[0], "$id")
    unresolved: list[str] = []
    for other in found[1:]:
        if not await _void_duplicate_cas(record_id, other, canonical):
            unresolved.append(_v(other, "$id"))
    if not await _clear_pending_cas(found[0]):
        unresolved.append(canonical)
    if unresolved:
        await _notify(NOTIFY_KIND_REVIEW, record_id,
                      f"{NOTICE_HEAD_REVIEW} 案件レコードNo.{record_id}: 二重起票の疑い（正: 発送管理 No.{canonical}・"
                      f"未解決: No.{'、No.'.join(unresolved)}）。No.{canonical} を正として行に書きました。"
                      "未解決のレコードを確認してください。")
    return canonical, bool(unresolved)


async def promote_status(record_id: str) -> bool:
    """受理 → 債権者通知 を一方向 CAS で 1 回だけ。既に 債権者通知 以降なら触らない。"""
    for attempt in range(_CAS_RETRIES):
        latest = await kintone.get_record(APP_HOUKI_CASE, record_id)
        if _v(latest, FIELD_STATUS) != STATUS_ACCEPTED:
            return False
        try:
            await kintone.update_record(APP_HOUKI_CASE, record_id, {FIELD_STATUS: STATUS_NOTIFYING},
                                        revision=_v(latest, "$revision") or None)
            return True
        except kintone.KintoneConflict:
            continue
    return False


async def _notify(kind: str, record_id: str, text: str) -> None:
    try:
        await notify.notify_admin_line(text, throttle_key=f"{kind}:{record_id}")
    except Exception:
        logger.error("[HOUKI_SOUFU] admin notify failed (fixed text)")


def review_text(record_id: str, missing: list[str], no_addr: list[int], enclosure: str | None) -> str:
    lines = [f"{NOTICE_HEAD_REVIEW} 案件レコードNo.{record_id}"]
    if missing:
        lines.append("送付状の必須欄が未入力: " + "・".join(missing))
    if no_addr:
        lines.append("債権者住所が未入力の行（通知要否=要）: " + "、".join(f"{n} 行目" for n in no_addr))
    if enclosure == "block_missing":
        lines.append(f"同封物ブロック「{ENCLOSURE_BLOCK_KEY}」が App 32 に未登録（対象ユニット {UNIT}・有効=yes）")
    elif enclosure == "option_missing":
        lines.append(f"App 30 同封物選択 の選択肢に「{ENCLOSURE_BLOCK_KEY}」がありません")
    lines.append("住所を入力して保存すれば起票されます（status は変更不要）。")
    return "\n".join(lines)


def filed_text(record_id: str, filed: list[str], aligned: int, review_count: int, promoted: bool,
               duplicates_unresolved: int = 0) -> str:
    lines = [f"{NOTICE_HEAD_FILED} 案件レコードNo.{record_id}"]
    lines.append(f"起票 {len(filed)} 件（発送管理 No.{'、'.join(filed) if filed else 'なし'}）")
    if aligned:
        lines.append(f"既存起票に揃えた行 {aligned} 件")
    if duplicates_unresolved:
        lines.append(f"重複未解決 {duplicates_unresolved} 件（別途通知）")
    if review_count:
        lines.append(f"要確認 {review_count} 件（別途通知）")
    if promoted:
        lines.append(f"status を「{STATUS_NOTIFYING}」に進めました。")
    lines.append("発送管理で成果物を確認し、承認してください。")
    return "\n".join(lines)


def row_eligible(row: dict) -> bool:
    """fix2 B-2: 新規作成の前提（通知要否=要 かつ 債権者住所 非空）。"""
    return row_val(row, COL_NOTIFY) == NOTIFY_REQUIRED and bool(row_val(row, COL_ADDR))


async def _file_claimed_row(record_id: str, record: dict, target_row_id: str, token: str,
                            result: dict, filed_ids: list[str], recovery: bool = False) -> None:
    """claim 済みの行について（fix2 A-3〜A-5・B）:
    1. 行を再取得し、送付発送管理No の token が自分のものであることを確認（違えば所有権喪失としてスキップ）
    2. 既存検索（番号復旧）→ 無ければ、回収時は 通知要否=要 かつ 住所非空 を再確認（満たさなければ
       行を 未・番号空 に CAS で戻し要確認）→ App 30 作成 → 同キー 2 件以上なら番号最小を正（自分の
       下書き を削除／下書き でなければ要確認）
    3. 手順 1 の revision で番号を CAS 書込。409 なら 起票中 のまま（期限切れ後に回収）。"""
    key = soufu_key(record_id, target_row_id)
    latest = await kintone.get_record(APP_HOUKI_CASE, record_id)
    row = next((r for r in rows_of(latest) if row_id(r) == target_row_id), None)
    if row is None or claim_owner(row) != token:
        result["lost"] += 1
        logger.info("[HOUKI_SOUFU] claim ownership lost record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return
    row_no = next((i for i, r in enumerate(rows_of(latest), 1) if row_id(r) == target_row_id), 0)
    found = await find_all_shipping(key)
    if found:
        # fix3 B-1: 番号復旧でも全件検索→重複確認（pending 解除）を必ず通す
        shipping_id, unresolved = await resolve_duplicates(record_id, key, found)
        if unresolved:
            result["duplicates_unresolved"] += 1
        else:
            result["aligned"] += 1
    else:
        if recovery and not row_eligible(row):
            reverted = await update_row_once(record_id, target_row_id,
                                             {COL_STATE: STATE_TODO, COL_SHIP_NO: ""}, latest)
            result["reverted"] += 1
            await _notify(NOTIFY_KIND_REVIEW, record_id,
                          f"{NOTICE_HEAD_REVIEW} 案件レコードNo.{record_id} 行 {row_no}: 起票中でしたが"
                          "通知要否/住所の条件を満たさないため取り消しました"
                          + ("" if reverted else "（行の 送付状態 を手で 未 に戻してください）"))
            return
        try:
            shipping_id = str(await kintone.create_record(APP_SHIPPING, build_shipping_fields(latest, row, key)))
        except Exception as e:
            result["pending"] += 1
            logger.warning("[HOUKI_SOUFU] App30 create failed (row left claiming) record_id=%s cls=%s",
                           emit(record_id, "record_id", "log", "operator"),
                           emit(type(e).__name__, "vendor_raw", "log", "operator"))
            return
        logger.info("[HOUKI_SOUFU] filed App30 record_id=%s shipping=%s",
                    emit(record_id, "record_id", "log", "operator"),
                    emit(shipping_id, "record_id", "log", "operator"))
        # fix3 A-3: 作成直後に同キー全件を取得して重複確認（正の pending 解除・他は エラー 化）
        found = await find_all_shipping(key)
        if not found:
            found = [{"$id": {"value": shipping_id}, "$revision": {"value": ""},
                      "発送ステータス": {"value": SHIPPING_STATUS_DRAFT},
                      "チャネル固有データ": {"value": json.dumps({CHANNEL_DATA_KEY: key, PENDING_DEDUPE_KEY: True})}}]
        canonical, unresolved = await resolve_duplicates(record_id, key, found)
        if unresolved:
            result["duplicates_unresolved"] += 1
        elif canonical == shipping_id:
            filed_ids.append(shipping_id)
        else:
            result["aligned"] += 1
        shipping_id = canonical
    if not await update_row_once(record_id, target_row_id, {COL_SHIP_NO: shipping_id}, latest):
        result["pending"] += 1                            # 起票中 のまま＝期限切れ後に回収
        logger.warning("[HOUKI_SOUFU] shipping number write failed (row left claiming) record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))


async def process_soufu(record_id: str) -> dict:
    """起票本体（BackgroundTasks から呼ぶ）。fix1 A: record_id ごとの Lock で直列化し、
    行ごとに claim（CAS・fix2: 所有者 token＋期限）→ App 30 作成 → 番号書込。
    B: status=債権者通知 でも 未 行の起票と回収を行う。戻り値は件数（テスト用）。"""
    async with _lock_for(record_id):
        return await _process_soufu_locked(record_id, new_owner_token())


async def _process_soufu_locked(record_id: str, token: str) -> dict:
    result = {"filed": 0, "aligned": 0, "review": 0, "pending": 0, "recovered": 0,
              "lost": 0, "reverted": 0, "duplicates_unresolved": 0, "promoted": False, "skip": ""}
    record = await kintone.get_record(APP_HOUKI_CASE, record_id)
    if not is_triggered(record):
        result["skip"] = "not_triggered"
        return result
    missing = letter.missing_case_fields(record)
    targets, no_addr, recovery = classify_rows(record)
    enclosure = await enclosure_problem() if (targets or recovery) else None
    if missing or enclosure:
        result["review"] = 1
        await _notify(NOTIFY_KIND_REVIEW, record_id, review_text(record_id, missing, no_addr, enclosure))
        logger.info("[HOUKI_SOUFU] needs review record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return result
    if not targets and not recovery:
        result["skip"] = "no_target_rows"
        if no_addr:
            result["review"] = 1
            await _notify(NOTIFY_KIND_REVIEW, record_id, review_text(record_id, [], no_addr, None))
        return result

    group = await fetch_group(record)
    extra = extra_fee_names(group)
    filed_ids: list[str] = []
    latest: dict | None = record
    # A-4 回収: 番号空（旧形式）または期限切れ claim の行を、自分の token で claim し直してから片付ける
    for _n, row in recovery:
        if not await claim_recovery_row(record_id, row_id(row), latest, token):
            latest = None
            continue
        latest = None
        before = (result["aligned"], len(filed_ids))
        await _file_claimed_row(record_id, record, row_id(row), token, result, filed_ids, recovery=True)
        if (result["aligned"], len(filed_ids)) != before:
            result["recovered"] += 1
    # A-1〜A-3: 未 行は claim（所有者 token）→ 所有権確認 → 作成 → 番号
    for _n, row in targets:
        is_extra = hc.normalize_creditor(row_val(row, COL_NAME)) in extra
        if not await claim_row(record_id, row_id(row), latest, token, extra=is_extra):
            latest = None                                # 409: 他者が処理中 → 再取得して次の行へ
            continue
        latest = None
        await _file_claimed_row(record_id, record, row_id(row), token, result, filed_ids)
    result["filed"] = len(filed_ids)
    if filed_ids or result["aligned"] or result["duplicates_unresolved"]:
        result["promoted"] = await promote_status(record_id)   # 現在が 受理 のときだけ 1 回
    if no_addr:
        result["review"] += 1
        await _notify(NOTIFY_KIND_REVIEW, record_id, review_text(record_id, [], no_addr, None))
    if result["pending"]:
        result["review"] += 1
        await _notify(NOTIFY_KIND_REVIEW, record_id,
                      f"{NOTICE_HEAD_REVIEW} 案件レコードNo.{record_id}: 発送管理の作成または番号書込に失敗した行が"
                      f" {result['pending']} 件あります（次回の保存時に自動で回収します）")
    if filed_ids or result["aligned"] or result["recovered"] or result["duplicates_unresolved"]:
        await _notify(NOTIFY_KIND_FILED, record_id,
                      filed_text(record_id, filed_ids, result["aligned"], result["review"], result["promoted"],
                                 result["duplicates_unresolved"]))
    return result


# ── 発送済の書き戻し（hub/dispatch から） ────────────────────────────────────
def is_houki_shipping(shipping: dict) -> bool:
    return _v(shipping, "案件アプリID") == str(APP_HOUKI_CASE.app_id())


async def mark_row_sent(shipping: dict) -> bool:
    """App 30 発送済/完了 → 案件アプリID=App 40 のときだけ 債権者一覧 の該当行を 送付済 に。
    時効側（App 21）のレコードは対象外（呼び出し側でも判定）。"""
    if not is_houki_shipping(shipping):
        return False
    try:
        data = json.loads(_v(shipping, "チャネル固有データ") or "{}")
    except ValueError:
        data = {}
    case_id = str(data.get("case_record_id") or _v(shipping, "案件レコードID"))
    rid_row = str(data.get("row_id") or "")
    if not (case_id and rid_row):
        return False
    ok = await update_row(case_id, rid_row, {COL_STATE: STATE_SENT})
    if not ok:
        await _notify(NOTIFY_KIND_REVIEW, case_id,
                      f"{NOTICE_HEAD_REVIEW} 案件レコードNo.{case_id}: 発送管理 No.{_v(shipping, '$id')} の"
                      "発送済を 債権者一覧 へ書き戻せませんでした（送付状態 を手で 送付済 にしてください）")
    return ok


async def write_back_safely(shipping: dict) -> bool:
    """fix1 D: hub/dispatch から呼ぶ書き戻し。取得・更新のタイムアウト/通信例外/409 再取得失敗の
    いずれでも要確認通知（案件番号・発送管理番号・手順のみ）。通知失敗は ERROR ログ（固定文言+番号）。
    例外は外へ出さない（dispatch 本体を落とさない・時効側は呼び出し側で除外）。"""
    if not is_houki_shipping(shipping):
        return False
    case_id = _v(shipping, "案件レコードID")
    ship_id = _v(shipping, "$id")
    try:
        return await mark_row_sent(shipping)
    except Exception as e:
        logger.error("[HOUKI_SOUFU] write-back exception case=%s shipping=%s cls=%s",
                     emit(case_id, "record_id", "log", "operator"),
                     emit(ship_id, "record_id", "log", "operator"),
                     emit(type(e).__name__, "vendor_raw", "log", "operator"))
        try:
            # fix2 C: 3 値で送達を確認。failed は ERROR ログ（固定文言+案件番号+発送管理番号）。throttled は成功扱い
            outcome = await notify.notify_admin_line_result(
                f"{NOTICE_HEAD_REVIEW} 案件レコードNo.{case_id}: 発送管理 No.{ship_id} の発送済を"
                "債権者一覧へ書き戻せませんでした（通信エラー）。App 40 の債権者一覧 該当行の"
                " 送付状態 を手で 送付済 にしてください。",
                throttle_key=f"{NOTIFY_KIND_REVIEW}:{case_id}:{ship_id}")
            if outcome not in ("sent", "throttled"):
                logger.error("[HOUKI_SOUFU] write-back notice not delivered case=%s shipping=%s",
                             emit(case_id, "record_id", "log", "operator"),
                             emit(ship_id, "record_id", "log", "operator"))
        except Exception:
            logger.error("[HOUKI_SOUFU] write-back notify failed case=%s shipping=%s",
                         emit(case_id, "record_id", "log", "operator"),
                         emit(ship_id, "record_id", "log", "operator"))
        return False


def shipping_app_env_ready() -> bool:
    return bool(os.environ.get("APP_SHIPPING") and os.environ.get("TOKEN_SHIPPING"))
