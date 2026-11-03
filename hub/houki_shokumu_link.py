"""houki_shokumu_link — HOUKI-SHOKUMU-LINK-1: App 40（相続放棄案件）から職務上請求の請求案（提案封筒）を起票できるようにする

正本: DRAFT_SHOKUMU_PLAN.md（職務上請求の請求案・提案封筒=App 30 要確認・M1 承認が対外防壁）・docs/souzoku-houki/10-koseki-matrix.md
§1（被相続人の住民票除票が最優先）。QUEUE-2026-10-13-D D-6。読取報告: 請求案は案件が App 21/26 固定で App 40 から起票不能・
flag OFF・座標未校正。本票:
- (a) App 40 を「相続放棄」ユニットとして写像（hub/heir_envelope._unit_for_case に APP_HOUKI → 相続放棄 を追加）。
  受任確定フック（C-4 と同じ遷移点: status が 受任／書類収集中／申述書作成）で **被相続人の住民票除票（joh_removed）** の
  請求計画（提案封筒・App 30・発送ステータス=要確認・実行済み=no・ユニット種別=相続放棄・案件アプリID=App 40）を
  自動起票する。冪等キー `houki_shokumu_plan:{App 40 No.}:{plan_hash}`（open な同一キーの封筒があれば already_filed）。
  **flag SHOKUMU_PLAN_ENABLED は OFF のまま**（OFF では I/O ゼロで disabled を返す・ON は大野）。
- (b) 座標校正（様式の印字位置）は触らない（別票）。
- 封筒の detail は相続放棄専用キー `houki_shokumu_plan`（hub/shokumu_plan の閉集合 detail は App 34 の person_id を必須と
  するため流用しない）。関所確定→M1 委譲（shokumu_plan._resolve_shokumu_plan）は既存キー `shokumu_plan` だけを見るので、
  本票の封筒は「要確認の提案」で止まる（確定・M1 発行は座標校正と同じ別票）。
- 発火: 受け口 `/souzoku-houki/shokumu-link/{secret}`（App 40 レコード編集の kintone Webhook／手動 POST・申述書の受け口と
  同じ認証）。走査ジョブは置かない（flag OFF の間は作用が無いため）。
- 材料: 被相続人氏名・被相続人最後の住所（App 40）→ 自治体は hub/shokumu_plan.resolve_app31（App 31 マスタ）。
  解決不能は input_required（起票はする・人が入力）。被相続人氏名が空なら起票しない（problems）。
- RV-10: ログ・通知にレコード No.・件数・固定語彙のみ（氏名・住所は載せない）。
"""

import json
import logging
import re

from hub import kintone, shokumu_plan
from hub.houki_case_store import APP_HOUKI_CASE
from hub.redact import emit

logger = logging.getLogger("hub.houki_shokumu_link")

UNIT = shokumu_plan.PLAN_UNIT                       # "相続放棄"
TOP_KEY = "houki_shokumu_plan"                       # チャネル固有データ の最上位キー（既存 shokumu_plan とは別）
ENGAGED_STATUSES = frozenset({"受任", "書類収集中", "申述書作成"})
LINE_TYPE = "joh_removed"                            # 被相続人の住民票除票（10 §1 最優先）
FIELD_STATUS = "status"
FIELD_DECEDENT = "被相続人氏名"
FIELD_LAST_ADDRESS = "被相続人最後の住所"
RESULT_DISABLED = "disabled"
RESULT_NOT_ENGAGED = "not_engaged"
RESULT_PROBLEMS = "problems"
RESULT_FILED = "filed"
RESULT_ALREADY = "already_filed"
RESULT_FAILED = "failed"
_DETAIL_KEYS = frozenset({"case_app_id", "case_record_id", "unit", "line_type", "request_type", "municipality",
                          "app31_record_id", "status", "plan_hash", "matrix_version", "冪等キー"})
_CASE_RE = re.compile(r"^[0-9]{1,10}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")


class HoukiPlanError(ValueError):
    """detail の保存境界検証に落ちた（起票しない）。"""


def _v(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def is_engaged(record: dict) -> bool:
    return _v(record, FIELD_STATUS) in ENGAGED_STATUSES


def houki_app_id() -> str:
    return str(APP_HOUKI_CASE.app_id() or "").strip()


def plan_hash(record_id: str, decedent: str, address: str, municipality: str) -> str:
    return shokumu_plan.canonical_sha256({"source": "app40", "case_record_id": str(record_id), "decedent": decedent,
                                          "address": address, "municipality": municipality,
                                          "matrix_version": shokumu_plan.MATRIX_VERSION, "line_type": LINE_TYPE})


def validate_detail(detail: dict) -> None:
    if set(detail) != _DETAIL_KEYS:
        raise HoukiPlanError("detail は閉集合のみ（キー過不足）")
    if not _CASE_RE.fullmatch(str(detail["case_record_id"])) or not _CASE_RE.fullmatch(str(detail["case_app_id"])):
        raise HoukiPlanError("案件 ID grammar 外")
    if detail["unit"] != UNIT or detail["line_type"] != LINE_TYPE:
        raise HoukiPlanError("unit/line_type は固定値のみ")
    if detail["request_type"] != shokumu_plan.LINE_REQUEST_TYPE[LINE_TYPE]:
        raise HoukiPlanError("request_type は固定値のみ")
    if not isinstance(detail["municipality"], str) or not detail["municipality"]:
        raise HoukiPlanError("municipality は非空 str のみ")
    if detail["status"] not in ("propose", "input_required"):
        raise HoukiPlanError("status は閉集合のみ")
    if detail["municipality"] == shokumu_plan.INPUT_REQUIRED and detail["status"] != "input_required":
        raise HoukiPlanError("municipality 要入力の行は必ず input_required")
    if not _HEX64_RE.fullmatch(str(detail["plan_hash"])):
        raise HoukiPlanError("plan_hash grammar 外")
    if detail["冪等キー"] != f"{TOP_KEY}:{detail['case_record_id']}:{detail['plan_hash']}":
        raise HoukiPlanError("冪等キーが構成規則と不一致")


async def build_detail(record: dict) -> tuple[dict | None, list[str]]:
    """App 40 レコード → 提案封筒の detail。起票不能は (None, problems)。"""
    rid = _v(record, "$id")
    decedent = _v(record, FIELD_DECEDENT)
    address = _v(record, FIELD_LAST_ADDRESS)
    problems: list[str] = []
    if not _CASE_RE.fullmatch(rid):
        problems.append("案件レコード No. 不正")
    if not decedent:
        problems.append("被相続人氏名が未入力です")
    if not houki_app_id():
        problems.append("APP_HOUKI 未設定")
    if problems:
        return None, problems
    app31 = await shokumu_plan.resolve_app31(address) if address else {
        "municipality": shokumu_plan.INPUT_REQUIRED, "app31_record_id": "", "有効": "", "fallback": ""}
    muni = str(app31.get("municipality") or shokumu_plan.INPUT_REQUIRED)
    h = plan_hash(rid, decedent, address, muni)
    detail = {
        "case_app_id": houki_app_id(), "case_record_id": rid, "unit": UNIT, "line_type": LINE_TYPE,
        "request_type": shokumu_plan.LINE_REQUEST_TYPE[LINE_TYPE], "municipality": muni,
        "app31_record_id": str(app31.get("app31_record_id") or ""),
        "status": "input_required" if muni == shokumu_plan.INPUT_REQUIRED else "propose",
        "plan_hash": h, "matrix_version": shokumu_plan.MATRIX_VERSION, "冪等キー": f"{TOP_KEY}:{rid}:{h}",
    }
    validate_detail(detail)
    return detail, []


async def find_envelopes(case_record_id: str, idem_key: str) -> list[dict]:
    esc = idem_key.replace('"', "")
    records = await kintone.search_records(
        shokumu_plan.APP_SHIPPING, f'チャネル固有データ like "{esc}"',
        fields=["$id", "チャネル固有データ", "発送ステータス", "実行済み"])
    out = []
    for rec in records:
        try:
            data = json.loads(_v(rec, "チャネル固有データ"))
        except (ValueError, TypeError):
            continue
        detail = data.get(TOP_KEY)
        if isinstance(detail, dict) and detail.get("冪等キー") == idem_key \
                and str(detail.get("case_record_id")) == str(case_record_id):
            out.append(rec)
    return out


async def file_envelope(detail: dict) -> dict:
    """提案封筒の起票（App 30・案件アプリID=App 40・ユニット種別=相続放棄）。open な同一キーは already_filed。"""
    validate_detail(detail)
    idem_key = detail["冪等キー"]
    for rec in await find_envelopes(detail["case_record_id"], idem_key):
        if _v(rec, "発送ステータス") == shokumu_plan.STATUS_PENDING and _v(rec, "実行済み") == "no":
            return {"status": RESULT_ALREADY, "envelope_id": _v(rec, "$id")}
    record_id = await kintone.create_record(shokumu_plan.APP_SHIPPING, {
        "発送ステータス": shokumu_plan.STATUS_PENDING,
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": UNIT,
        "案件アプリID": detail["case_app_id"],
        "案件レコードID": detail["case_record_id"],
        "実行済み": "no",
        "件名": f"職務上請求の請求案（相続放棄）: 案件 No.{detail['case_record_id']}（被相続人の住民票除票・候補 1 件）",
        "チャネル固有データ": json.dumps({TOP_KEY: detail}, ensure_ascii=False),
    })
    return {"status": RESULT_FILED, "envelope_id": str(record_id)}


async def process_record(record: dict) -> dict:
    """受任確定フック本体。flag OFF は I/O ゼロ。受任系でなければ作用 0。"""
    rid = _v(record, "$id")
    if not shokumu_plan.shokumu_plan_enabled():
        return {"status": RESULT_DISABLED, "record_id": rid}
    if not is_engaged(record):
        return {"status": RESULT_NOT_ENGAGED, "record_id": rid}
    try:
        detail, problems = await build_detail(record)
        if detail is None:
            logger.info("[SHOKUMU_LINK] not filed (problems) record_id=%s count=%s",
                        emit(rid, "record_id", "log", "operator"), emit(len(problems), "count", "log", "operator"))
            return {"status": RESULT_PROBLEMS, "record_id": rid, "problems": problems}
        res = await file_envelope(detail)
    except Exception as e:
        logger.warning("[SHOKUMU_LINK] filing failed cls=%s record_id=%s",
                       emit(type(e).__name__, "vendor_raw", "log", "operator"), emit(rid, "record_id", "log", "operator"))
        return {"status": RESULT_FAILED, "record_id": rid}
    if res["status"] == RESULT_FILED:
        logger.info("[SHOKUMU_LINK] filed record_id=%s", emit(rid, "record_id", "log", "operator"))
    else:
        logger.info("[SHOKUMU_LINK] already filed record_id=%s", emit(rid, "record_id", "log", "operator"))
    return dict(res, record_id=rid)


async def process_record_id(record_id: str) -> dict:
    if not shokumu_plan.shokumu_plan_enabled():
        return {"status": RESULT_DISABLED, "record_id": str(record_id)}
    try:
        record = await kintone.get_record(APP_HOUKI_CASE, str(record_id))
    except Exception as e:
        logger.warning("[SHOKUMU_LINK] fetch failed cls=%s record_id=%s",
                       emit(type(e).__name__, "vendor_raw", "log", "operator"),
                       emit(record_id, "record_id", "log", "operator"))
        return {"status": RESULT_FAILED, "record_id": str(record_id)}
    return await process_record(record)
