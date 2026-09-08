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

冪等: キー houki_soufu:{record_id}:{row_id} を App 30 チャネル固有データ に持ち、起票前に
既存検索（存在すれば起票せず行を 起票済 に揃える）。行の 送付状態≠未 はスキップ。
書き戻し: App 30 が 発送済/完了 になったとき（hub/dispatch）、案件アプリID=App 40 の
レコードだけ 債権者一覧 の該当行を 送付状態=送付済 に更新（CAS・409 は再取得 1 回）。
App 40 で書くのは 債権者一覧（送付状態／送付発送管理No／追加料金対象）と status の
一方向遷移のみ。通知本文に個人情報は載せない（レコード番号・行番号・欄名のみ）。
"""

import json
import logging
import os

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
    """起点条件（凍結）: status=受理 かつ 受理通知受領日 非空 かつ 受理通知書 添付あり。"""
    files = (record.get(FIELD_ACCEPT_FILE) or {}).get("value") or []
    return (_v(record, FIELD_STATUS) == STATUS_ACCEPTED
            and bool(_v(record, FIELD_ACCEPT_RECEIVED)) and bool(files))


def classify_rows(record: dict) -> tuple[list[tuple[int, dict]], list[int]]:
    """(起票対象 [(行番号, row)], 住所未入力の 要 行番号)。行番号は 1 始まり。"""
    targets: list[tuple[int, dict]] = []
    no_addr: list[int] = []
    for i, row in enumerate(rows_of(record), 1):
        if row_val(row, COL_NOTIFY) != NOTIFY_REQUIRED or row_val(row, COL_STATE) != STATE_TODO:
            continue
        if not row_id(row):
            continue
        if not row_val(row, COL_ADDR):
            no_addr.append(i)
            continue
        targets.append((i, row))
    return targets, no_addr


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


async def find_existing_shipping(key: str) -> str | None:
    records = await kintone.search_records(
        APP_SHIPPING, f'チャネル固有データ like "{key}"', fields=["$id"])
    return _v(records[0], "$id") if records else None


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
    lines.append("入力・登録後に status を「受理」のまま再保存すると再判定されます。")
    return "\n".join(lines)


def filed_text(record_id: str, filed: list[str], aligned: int, review_count: int, promoted: bool) -> str:
    lines = [f"{NOTICE_HEAD_FILED} 案件レコードNo.{record_id}"]
    lines.append(f"起票 {len(filed)} 件（発送管理 No.{'、'.join(filed) if filed else 'なし'}）")
    if aligned:
        lines.append(f"既存起票に揃えた行 {aligned} 件")
    if review_count:
        lines.append(f"要確認 {review_count} 件（別途通知）")
    if promoted:
        lines.append(f"status を「{STATUS_NOTIFYING}」に進めました。")
    lines.append("発送管理で成果物を確認し、承認してください。")
    return "\n".join(lines)


async def process_soufu(record_id: str) -> dict:
    """起票本体（BackgroundTasks から呼ぶ）。戻り値は件数（テスト用）。"""
    result = {"filed": 0, "aligned": 0, "review": 0, "promoted": False, "skip": ""}
    record = await kintone.get_record(APP_HOUKI_CASE, record_id)
    if not is_triggered(record):
        result["skip"] = "not_triggered"
        return result
    missing = letter.missing_case_fields(record)
    targets, no_addr = classify_rows(record)
    enclosure = await enclosure_problem() if targets else None
    if missing or enclosure:
        result["review"] = 1
        await _notify(NOTIFY_KIND_REVIEW, record_id, review_text(record_id, missing, no_addr, enclosure))
        logger.info("[HOUKI_SOUFU] needs review record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return result
    if not targets:
        result["skip"] = "no_target_rows"
        if no_addr:
            result["review"] = 1
            await _notify(NOTIFY_KIND_REVIEW, record_id, review_text(record_id, [], no_addr, None))
        return result

    group = await fetch_group(record)
    extra = extra_fee_names(group)
    filed_ids: list[str] = []
    latest = record
    for _n, row in targets:
        rid_row = row_id(row)
        key = soufu_key(record_id, rid_row)
        existing = await find_existing_shipping(key)
        if existing:
            shipping_id = existing
            result["aligned"] += 1
        else:
            shipping_id = str(await kintone.create_record(APP_SHIPPING, build_shipping_fields(record, row, key)))
            filed_ids.append(shipping_id)
            logger.info("[HOUKI_SOUFU] filed App30 record_id=%s shipping=%s",
                        emit(record_id, "record_id", "log", "operator"),
                        emit(shipping_id, "record_id", "log", "operator"))
        updates = {COL_STATE: STATE_FILED, COL_SHIP_NO: shipping_id}
        if hc.normalize_creditor(row_val(row, COL_NAME)) in extra or row_val(row, COL_EXTRA) == EXTRA_YES:
            updates[COL_EXTRA] = EXTRA_YES
        ok = await update_row(record_id, rid_row, updates, latest)
        latest = None                                    # 次行は最新を再取得
        if not ok:
            result["review"] += 1
            await _notify(NOTIFY_KIND_REVIEW, record_id,
                          f"{NOTICE_HEAD_REVIEW} 案件レコードNo.{record_id}: 債権者一覧の行更新に失敗"
                          f"（発送管理 No.{shipping_id} は起票済み。行の 送付状態 を手で 起票済 にしてください）")
    result["filed"] = len(filed_ids)
    if filed_ids or result["aligned"]:
        result["promoted"] = await promote_status(record_id)
    if no_addr:
        result["review"] += 1
        await _notify(NOTIFY_KIND_REVIEW, record_id, review_text(record_id, [], no_addr, None))
    await _notify(NOTIFY_KIND_FILED, record_id,
                  filed_text(record_id, filed_ids, result["aligned"], result["review"], result["promoted"]))
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


def shipping_app_env_ready() -> bool:
    return bool(os.environ.get("APP_SHIPPING") and os.environ.get("TOKEN_SHIPPING"))
