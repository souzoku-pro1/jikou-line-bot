"""shokumu_plan — 職務上請求の自動判定起票（SHOKUMU-PLAN 実装票）

正本: `DRAFT_SHOKUMU_PLAN.md`（**FROZEN**・R-SHOKUMU-PLAN-D9 PASS・2026-08-11）。
本 module は凍結仕様の実装であり設計判断を持たない（変更は再D巡・再凍結による）。

骨子（正本 §2・原則 §0）: plan 生成（機械・read-only）→ 提案封筒（App30・
canonical 未フィルタ全候補保存）→ 関所確定（[人]・T2）→ M1 既存経路への委譲
（様式・料金・purpose・宛先ロジックの複製禁止）→ 既存承認フロー。
**機械は提案まで**——承認済みへのサーバ遷移コードは本 module に存在しない。

- 行類型 7 値・マトリクス（§2A/§2A.2・config データ）・candidates 完全順序 6 鍵
  （§4A fix5）・相関制約 7 項（§4A fix2/fix4）・canonical 全候補保存（fix4 H01-01・
  §2C フィルタは M1 create 直前限定）。
- hash 群（§4-v2/§4B）: plan_hash（正本内容 (1)〜(7)）・plan_idem（業務単位・
  plan 横断）・m1_fingerprint（A層=channel_json 完成形そのもの＋B層=監査材料・
  fix6/fix7）。canonical 正規化＝UTF-8→SHA-256・separators=(",",":")・
  ensure_ascii=False・key 辞書順・型固定（count=int/他 str）・NFC 不採用。
- 監査メタ閉集合（§2D fix8・6 キー）は PLAN_AUDIT_META_KEYS の**単一定数**を
  §2D 側（fields 組立）と §4B 側（照合）の両所が参照する。
- 回収・冪等（§2C-4 fix3/§4B）: 封筒冪等は open（要確認/no）限定回収・terminal
  のみなら新規起票（却下非抑止）。plan_idem HIT は m1_fingerprint 完全一致で
  skip／不一致・欠落・不正 grammar・parse 不能は要確認（安全側）。
- flag `SHOKUMU_PLAN_ENABLED`（既定 OFF・裁定⑥）。
- PII 規律: 応答・ログ・警報は件数・record_id・自治体名のみ（氏名・住所全文を
  封筒 detail・ログへ載せない＝§4A M02 person_id のみ保存）。
"""

import hashlib
import json
import os
import re
from dataclasses import dataclass, field

from hub import kintone
from hub.heir_envelope import _unit_for_case
from hub.person_validity import MERGE_STATE_FIELD, filter_active_persons

APP_SHIPPING = kintone.KintoneApp(
    "App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")
APP_KOSEKI_PERSON = kintone.KintoneApp(
    "App 34 (人物)", "APP_KOSEKI_PERSON", "TOKEN_KOSEKI_PERSON")
APP_KOSEKI_BOOK = kintone.KintoneApp(
    "App 33 (戸籍読解)", "APP_KOSEKI_BOOK", "TOKEN_KOSEKI_BOOK")

TOP_KEY = "shokumu_plan"
# purpose 解決用ユニット（§4B A層の purpose=resolved 後文字列・§7「初版は相続
# ユニットのみ」。PURPOSE_BY_UNIT の凍結キーと 1:1——App30 の「ユニット種別」
# field（_unit_for_case=相続一般）とは別物で、こちらは M1 の利用目的文言の解決に
# のみ使う）
PLAN_UNIT = "相続放棄"
STATUS_PENDING = "要確認"
INPUT_REQUIRED = "要入力"

# ── 行類型（§2A・7 値 enum・定義順=完全順序の第 1 鍵）────────────────────────
LINE_TYPES = ("joh_removed", "fuhyo", "decedent_joseki", "chain_missing",
              "parents_death", "sibling_death", "applicant_current")
STATUS_ORDER = ("propose", "fulfilled", "input_required")

# ── マトリクス（§2A.2 突合表と 1:1・config データ・§3-1 凍結）────────────────
# 家裁の運用差・弁護士の方針変更はこのデータ修正のみで反映（コード変更なし）。
MATRIX_VERSION = "shokumu-plan-matrix-v1"
KOSEKI_MATRIX = {
    "common": ("joh_removed", "fuhyo"),           # §1 全類型共通・最優先
    "by_rank": {                                   # §2 続柄別（rank→行類型）
        1: ("decedent_joseki", "applicant_current"),
        2: ("chain_missing", "applicant_current"),
        3: ("chain_missing", "parents_death", "applicant_current"),
    },
    "daishu_extra": ("sibling_death",),            # 甥姪（nephew_niece_rep）加算
}
# 行類型→request_type（§2A 写像表。chain_missing は要入力=§2A.3・
# applicant_current は戸籍謄本・様式は §2A 表のとおり）
LINE_REQUEST_TYPE = {
    "joh_removed": "住民票の除票",
    "fuhyo": "戸籍の附票",
    "decedent_joseki": "除籍謄本",
    "chain_missing": INPUT_REQUIRED,
    "parents_death": "除籍謄本",
    "sibling_death": "除籍謄本",
    "applicant_current": "戸籍謄本",
}
LINE_FORM = {                    # 様式（§2A 表・form1/form2）
    "joh_removed": "form2", "fuhyo": "form2", "decedent_joseki": "form1",
    "chain_missing": "form1", "parents_death": "form1",
    "sibling_death": "form1", "applicant_current": "form1",
}

# ── 監査メタ閉集合（§2D fix8・単一定数を §2D/§4B 両所が参照）────────────────
PLAN_AUDIT_META_KEYS = ("filed_by", "plan_envelope_no", "plan_hash",
                        "plan_idem", "m1_fingerprint", "plan_lines")

# ── grammar（§4A/§4B）────────────────────────────────────────────────────────
_CASE_RE = re.compile(r"^[0-9]{1,10}$")
_PERSON_RE = re.compile(r"^[0-9]{1,10}$")
_RUN_ID_RE = re.compile(r"^[1-9][0-9]{0,18}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_MATRIX_VER_RE = re.compile(r"^[0-9A-Za-z.\-]{1,32}$")
_PLAN_IDEM_RE = re.compile(
    r"^shokumu_plan:[0-9]{1,10}:[^:]{1,64}:([0-9]{1,10}|-):(form1|form2)$")
_DETAIL_KEYS = frozenset({
    "case_record_id", "phase", "run_id", "plan_hash", "app34_snapshot_hash",
    "app36_rows_hash", "matrix_version", "candidates", "冪等キー"})
_CAND_KEYS = frozenset({"line_type", "request_type", "count", "person_id",
                        "municipality", "status"})
# App34 使用 field 閉集合（§4-v2 (2)・8 field・これ以外を hash に入れない）
APP34_SNAPSHOT_FIELDS = ("氏名", "住所最新", "本籍最新", "死亡日",
                         "父人物ID", "母人物ID", "身分事項.出生行の年月日",
                         "被相続人フラグ")


class PlanPolicyError(ValueError):
    """保存境界の検証違反（閉集合外・grammar 外・相関制約違反）。
    kintone への write は発生しない（fail-closed・値は文言に載せない）。"""


def shokumu_plan_enabled() -> bool:
    """flag SHOKUMU_PLAN_ENABLED（既定 OFF・値集合は既存 flag 群と同一流儀）。"""
    return os.environ.get("SHOKUMU_PLAN_ENABLED", "").strip().lower() in (
        "1", "true", "on", "yes")


# ══════════════════════════════════════════════════════════════
# canonical 正規化・hash（§4B fix6 規則 1〜5・§4-v2）
# ══════════════════════════════════════════════════════════════

def canonical_sha256(obj) -> str:
    """canonical JSON（key 辞書順・separators=(",",":")・ensure_ascii=False）の
    UTF-8 bytes の SHA-256 hex（小文字 64 桁）。型変換はしない（§4B 規則 1-2）。
    NFC 正規化は行わない（§4B 規則 4・byte 比較）。"""
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def plan_idem_key(case_record_id: str, municipality: str, person_id,
                  form: str) -> str:
    """M1 冪等キー（§4B fix4: 業務上の起票単位・plan 横断で安定・plan_hash 非含有）。"""
    pid = person_id if person_id else "-"
    key = f"shokumu_plan:{case_record_id}:{municipality}:{pid}:{form}"
    if not _PLAN_IDEM_RE.fullmatch(key):
        raise PlanPolicyError("plan_idem が grammar 外です（値は表示しません）")
    return key


def m1_fingerprint(channel_json: dict, app31_record_id: str,
                   plan_lines: list, unit: str, form: str) -> str:
    """m1_fingerprint（§4B fix6/fix7）: A層=channel_json 完成形**そのもの**＋
    B層=監査・引当て補強材料の canonical object の SHA-256。

    - A層は build_channel_json の出力 dict をそのまま受ける（**呼出し元は
      App30 create へ渡す channel_json と同一 object を渡すこと**＝byte 一致
      不変条件・§4B fix7。count 合算併合は channel_json 完成前に済んでいる）。
    - B層: app31_record_id（str・要入力は ""）・plan_lines（enum 定義順 sort・
      unique）・unit・form。
    """
    aux = {"app31_record_id": app31_record_id or "",
           "plan_lines": sorted(set(plan_lines), key=LINE_TYPES.index),
           "unit": unit or "", "form": form}
    return canonical_sha256({"m1_input": channel_json, "aux": aux})


_CHANNEL_JSON_KEYS = ("request_items", "municipality", "target", "purpose")


def validate_audit_meta(meta: dict) -> None:
    """監査メタの共通検証境界（IMPL-fix1 IMPL-02・単一関数）。

    新規保存時と plan_idem HIT 再照合時の**双方**で通す——
    PLAN_AUDIT_META_KEYS との完全一致（余分・不足キー拒否）＋各値 grammar。
    不適合は PlanPolicyError（HIT 側は held/要確認へ写像・比較不能を一致扱い
    にしない＝§2D fix8-5）。
    """
    if not isinstance(meta, dict) or set(meta) != set(PLAN_AUDIT_META_KEYS):
        raise PlanPolicyError("監査メタのキー集合が閉集合と不一致")
    if meta["filed_by"] != "shokumu_plan":
        raise PlanPolicyError("filed_by が不正")
    if not (isinstance(meta["plan_envelope_no"], str)
            and meta["plan_envelope_no"].isdigit()):
        raise PlanPolicyError("plan_envelope_no grammar 外")
    for k in ("plan_hash", "m1_fingerprint"):
        if not (isinstance(meta[k], str) and _HEX64_RE.fullmatch(meta[k])):
            raise PlanPolicyError(f"{k} grammar 外")
    if not (isinstance(meta["plan_idem"], str)
            and _PLAN_IDEM_RE.fullmatch(meta["plan_idem"])):
        raise PlanPolicyError("plan_idem grammar 外")
    lines = meta["plan_lines"]
    if not isinstance(lines, list) or not lines:
        raise PlanPolicyError("plan_lines は非空 list のみ")
    if any(x not in LINE_TYPES for x in lines):
        raise PlanPolicyError("plan_lines は 7 値 enum のみ")
    if len(set(lines)) != len(lines):
        raise PlanPolicyError("plan_lines は unique であること")
    if lines != sorted(lines, key=LINE_TYPES.index):
        raise PlanPolicyError("plan_lines は enum 定義順 sort 済みであること")


def _extract_audit(channel_data: dict):
    """既存 M1 のチャネル固有データから監査メタ部を検証つきで抽出（IMPL-02）。

    保存形は channel_json 4 キー＋監査メタ 6 キーの合成（§2D）——**総キー集合の
    完全一致**（余分・不足キー拒否）＋validate_audit_meta。不適合は None
    （呼出し側が held/要確認へ倒す・create 0）。
    """
    allowed = set(_CHANNEL_JSON_KEYS) | set(PLAN_AUDIT_META_KEYS)
    if not isinstance(channel_data, dict) or set(channel_data) != allowed:
        return None
    meta = {k: channel_data[k] for k in PLAN_AUDIT_META_KEYS}
    try:
        validate_audit_meta(meta)
    except PlanPolicyError:
        return None
    return meta


# ══════════════════════════════════════════════════════════════
# 市区町村切り出し・App31 引当て（§2A.1・snapshot は §4-v2 (7) 方式A）
# ══════════════════════════════════════════════════════════════

_PREF_RE = re.compile(r"^(?:東京都|北海道|(?:京都|大阪)府|.{2,3}県)")
_WARD_RE = re.compile(r"^(.+?市.+?区)")
_CITY_RE = re.compile(r"^(.+?[市区町村])")


def extract_municipality_candidates(address: str) -> list[tuple[str, str]]:
    """住所/本籍文字列 → App31 照合候補 [(名称, fallback 種別)]（§2A.1）。

    都道府県 prefix を除去後、政令市（○○市△△区）は区まで（"ward"）→
    市まで（"city"）の順。通常は最初の 市/区/町/村 終端（"city"）のみ。
    切り出し失敗は空リスト（→要入力）。
    """
    s = (address or "").strip()
    if not s:
        return []
    s = _PREF_RE.sub("", s)
    out = []
    ward = _WARD_RE.match(s)
    if ward:
        out.append((ward.group(1), "ward"))
    city = _CITY_RE.match(s)
    if city and (not ward or city.group(1) != ward.group(1)):
        out.append((city.group(1), "city"))
    return out


async def resolve_app31(address: str) -> dict:
    """App31 引当て（§2A.1 の照合順・§4-v2 (7) の snapshot 行材料）。

    Returns: {"municipality": 名称 or 要入力, "app31_record_id": str or "",
              "有効": str or "", "fallback": "ward"|"city"|""}
    両候補とも未登録・切り出し失敗は municipality=要入力（照合系キーは ""）。
    """
    from channels.shokumu_seikyu import APP_CITY_MASTER
    for name, kind in extract_municipality_candidates(address):
        records = await kintone.search_records(
            APP_CITY_MASTER, f'市区町村名 = "{name}" and 有効 in ("yes")')
        if records:
            rec = records[0]
            return {"municipality": name,
                    "app31_record_id":
                        str((rec.get("$id") or {}).get("value") or ""),
                    "有効": "yes", "fallback": kind}
    return {"municipality": INPUT_REQUIRED, "app31_record_id": "",
            "有効": "", "fallback": ""}


def sort_app31_snapshot(rows: list[dict]) -> list[dict]:
    """App31 snapshot の完全順序（§4-v2 (7) fix6/fix7・方式A）と併合。

    順序: line_type 定義順 → person_id（"" 先頭・数値昇順）→ 市区町村名
    （コードポイント昇順）→ app31_record_id（"" 先頭・数値昇順）→ 有効
    （コードポイント昇順・"" 先頭）→ fallback 定義順（ward→city→""）。
    **全キー完全一致の行は 1 行へ併合**。順序・同一性判定は snapshot 自身の
    6 キーのみを参照（非存在キー参照禁止・§6-47）。
    """
    fb_order = {"ward": 0, "city": 1, "": 2}

    def _num(value):
        return (0, 0) if value == "" else (1, int(value))

    def key(r):
        return (LINE_TYPES.index(r["line_type"]), _num(r["person_id"]),
                r["市区町村名"], _num(r["app31_record_id"]), r["有効"],
                fb_order[r["fallback"]])

    seen = set()
    out = []
    for r in sorted(rows, key=key):
        t = (r["line_type"], r["person_id"], r["市区町村名"],
             r["app31_record_id"], r["有効"], r["fallback"])
        if t in seen:
            continue                     # 全キー一致行の併合
        seen.add(t)
        out.append({k: r[k] for k in ("line_type", "person_id", "市区町村名",
                                      "app31_record_id", "有効", "fallback")})
    return out


# ══════════════════════════════════════════════════════════════
# candidates（§4A: 完全順序 6 鍵・相関制約 7 項・閉集合 grammar）
# ══════════════════════════════════════════════════════════════

def sort_candidates(cands: list[dict]) -> list[dict]:
    """candidates の決定的並び順（§4A fix5・tie-break 全順序）と全鍵一致併合。"""
    req_order = _request_type_order()
    st_order = {s: i for i, s in enumerate(STATUS_ORDER)}

    def _pid(value):
        return (0, 0) if value is None else (1, int(value))

    def key(c):
        return (LINE_TYPES.index(c["line_type"]), _pid(c["person_id"]),
                (0, "") if c["municipality"] == INPUT_REQUIRED
                else (1, c["municipality"]),
                (0, 0) if c["request_type"] == INPUT_REQUIRED
                else (1, req_order[c["request_type"]]),
                st_order[c["status"]], c["count"])

    seen = set()
    out = []
    for c in sorted(cands, key=key):
        t = tuple(c[k] for k in ("line_type", "person_id", "municipality",
                                 "request_type", "status", "count"))
        if t in seen:
            continue                     # 全 6 鍵一致は 1 行に併合（§4A fix5）
        seen.add(t)
        out.append(dict(c))
    return out


def _request_type_order() -> dict:
    from channels.shokumu_seikyu import FEE_FIELD_BY_TYPE
    return {t: i for i, t in enumerate(FEE_FIELD_BY_TYPE)}


def validate_detail(detail: dict) -> None:
    """封筒 detail の保存境界検証（§4A・閉集合/grammar/相関制約/並び順）。"""
    if set(detail) != _DETAIL_KEYS:
        raise PlanPolicyError("detail は閉集合のみ（キー過不足）")
    if not _CASE_RE.fullmatch(str(detail["case_record_id"])):
        raise PlanPolicyError("case_record_id grammar 外")
    if detail["phase"] not in ("common", "full"):
        raise PlanPolicyError("phase は閉集合のみ")
    run_id = detail["run_id"]
    if detail["phase"] == "common":
        if run_id is not None:
            raise PlanPolicyError("common の run_id は null")
    elif not (isinstance(run_id, str) and _RUN_ID_RE.fullmatch(run_id)):
        raise PlanPolicyError("run_id grammar 外")
    for k in ("plan_hash", "app34_snapshot_hash"):
        if not (isinstance(detail[k], str) and _HEX64_RE.fullmatch(detail[k])):
            raise PlanPolicyError(f"{k} grammar 外")
    a36 = detail["app36_rows_hash"]
    if detail["phase"] == "common":
        if a36 is not None:
            raise PlanPolicyError("common の app36_rows_hash は null")
    elif not (isinstance(a36, str) and _HEX64_RE.fullmatch(a36)):
        raise PlanPolicyError("app36_rows_hash grammar 外")
    if not _MATRIX_VER_RE.fullmatch(str(detail["matrix_version"])):
        raise PlanPolicyError("matrix_version grammar 外")
    expected = f"shokumu_plan:{detail['case_record_id']}:{detail['plan_hash']}"
    if detail["冪等キー"] != expected:
        raise PlanPolicyError("冪等キーが構成規則と不一致")
    cands = detail["candidates"]
    if not isinstance(cands, list) or not cands:
        raise PlanPolicyError("candidates は非空 list")
    for c in cands:
        _validate_candidate(c)
    if cands != sort_candidates(cands):
        raise PlanPolicyError("candidates が完全順序（§4A fix5）で並んでいない")


def _validate_candidate(c: dict) -> None:
    from channels.shokumu_seikyu import FEE_FIELD_BY_TYPE
    if set(c) != _CAND_KEYS:
        raise PlanPolicyError("candidates 行は閉集合のみ")
    if c["line_type"] not in LINE_TYPES:
        raise PlanPolicyError("line_type は 7 値 enum のみ")
    if c["request_type"] != INPUT_REQUIRED \
            and c["request_type"] not in FEE_FIELD_BY_TYPE:
        raise PlanPolicyError("request_type は閉集合（type 6 種 or 要入力）のみ")
    if not isinstance(c["count"], int) or c["count"] <= 0:
        raise PlanPolicyError("count は正整数のみ")
    pid = c["person_id"]
    if pid is not None and not (isinstance(pid, str)
                                and _PERSON_RE.fullmatch(pid)):
        raise PlanPolicyError("person_id grammar 外")
    if not isinstance(c["municipality"], str) or not c["municipality"]:
        raise PlanPolicyError("municipality は非空 str のみ")
    if c["status"] not in STATUS_ORDER:
        raise PlanPolicyError("status は閉集合のみ")
    # 相関制約（§4A fix2/fix4・7 項）
    if c["municipality"] == INPUT_REQUIRED and c["status"] != "input_required":
        raise PlanPolicyError("municipality 要入力の行は必ず input_required")
    if c["request_type"] == INPUT_REQUIRED and c["status"] != "input_required":
        raise PlanPolicyError("request_type 要入力の行は必ず input_required")
    if c["line_type"] == "applicant_current":
        if pid is not None or c["status"] != "input_required":
            raise PlanPolicyError(
                "applicant_current は常に person_id null＋input_required（裁定⑦）")
    elif pid is None:
        raise PlanPolicyError("applicant_current 以外は person_id 必須")


# ══════════════════════════════════════════════════════════════
# 材料収集と plan 生成（§2 [1]・§2B・§4-v2——確定時の再計算と共用＝1:1 保証）
# ══════════════════════════════════════════════════════════════

def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _birth_wareki(record: dict) -> str:
    rows = (record.get("身分事項") or {}).get("value") or []
    for row in rows:
        value = row.get("value") or {}
        if str((value.get("事項種別") or {}).get("value") or "") == "出生":
            return str((value.get("年月日") or {}).get("value") or "")
    return ""


@dataclass
class PlanMaterials:
    """plan 生成/確定時 stale 再計算の共通材料（§4-v2 と 1:1）。"""
    case_record_id: str
    phase: str
    run_id: str | None = None
    decedent_person_id: str | None = None
    persons: dict = field(default_factory=dict)       # pid → App34 record
    app33: list = field(default_factory=list)         # [[rid, sha256], ...]
    app36_rows_hash: str | None = None
    app31_snapshot: list = field(default_factory=list)
    candidates: list = field(default_factory=list)
    problems: list = field(default_factory=list)      # 道案内（全体要確認）
    guidance: list = field(default_factory=list)      # 参考注記（§2 F5 注記等）

    def app34_snapshot(self) -> dict:
        """§4-v2 (2): 使用 person×使用 field 閉集合の snapshot（person_id を
        JSON 構成要素として明文化・person_id 昇順は canonical の sort_keys）。"""
        out = {}
        for pid, rec in self.persons.items():
            out[pid] = {
                "氏名": _v(rec, "氏名"), "住所最新": _v(rec, "住所最新"),
                "本籍最新": _v(rec, "本籍最新"), "死亡日": _v(rec, "死亡日"),
                "父人物ID": _v(rec, "父人物ID"), "母人物ID": _v(rec, "母人物ID"),
                "身分事項.出生行の年月日": _birth_wareki(rec),
                "被相続人フラグ": _v(rec, "被相続人フラグ"),
            }
        return out

    def plan_hash(self) -> str:
        """§4-v2: 正本内容 (1)〜(7) のみの canonical hash（§2C の非内容的状態・
        §2D の確定時再取得値は材料に含めない＝fix3 分離方式）。"""
        return canonical_sha256({
            "run_id": self.run_id,                             # (1)
            "app34_snapshot_hash":
                canonical_sha256(self.app34_snapshot()),       # (2)
            "app36_rows_hash": self.app36_rows_hash,           # (3)
            "app33_set": self.app33,                           # (4)
            "matrix_version": MATRIX_VERSION,                  # (5)
            "decedent_person_id": self.decedent_person_id,     # (6)
            "app31_snapshot": self.app31_snapshot,             # (7)
        })

    def detail(self) -> dict:
        plan_hash = self.plan_hash()
        return {
            "case_record_id": self.case_record_id,
            "phase": self.phase,
            "run_id": self.run_id,
            "plan_hash": plan_hash,
            "app34_snapshot_hash": canonical_sha256(self.app34_snapshot()),
            "app36_rows_hash": self.app36_rows_hash,
            "matrix_version": MATRIX_VERSION,
            "candidates": self.candidates,
            "冪等キー": f"shokumu_plan:{self.case_record_id}:{plan_hash}",
        }


async def _load_persons(case_record_id: str) -> list[dict]:
    records = await kintone.search_records(
        APP_KOSEKI_PERSON,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 500',
        fields=["$id", "氏名", "住所最新", "本籍最新", "死亡日", "父人物ID",
                "母人物ID", "被相続人フラグ", "身分事項", MERGE_STATE_FIELD])
    # RV-08: 無効化行（統合済み無効）は plan 対象に載せない（一点除外・裁定②(B)。
    # 確定時の stale 再計算も本関数を通るため無効化が自然反映される・§10.1）
    return filter_active_persons(records)


async def _load_kosekis(case_record_id: str) -> list[dict]:
    if not (APP_KOSEKI_BOOK.app_id() and APP_KOSEKI_BOOK.token()):
        return []
    return await kintone.search_records(
        APP_KOSEKI_BOOK,
        f'案件レコードID = "{case_record_id}" order by $id asc limit 100',
        fields=["$id", "読解JSON"])


def _senjun_hoki_flagged(head_run) -> bool:
    """先順位放棄（複雑性フラグ・§3-2）の検出。

    供給源（Declarations／案件の複雑性フラグ）は P3 裁定1 で **未供給（空）に
    凍結**されており、現行の実装現実では本フラグが立つ経路が存在しない
    （常に False）。供給源の結線は Declarations 供給票（別票）の着地後——
    検出時の挙動（マトリクス展開なし＝共通行のみ＋個別確定警報）は §3-2 の
    凍結どおり本 module 側に実装済み（テストは本関数の差し替えで pin）。
    """
    return False


async def build_plan(case_record_id: str) -> PlanMaterials:
    """plan 生成（§2 [1]・read-only）＝確定時 stale 再計算と同一関数。

    - 二段（裁定①(C)）: 共通行は run 非依存で常に生成・続柄別は §2B の 6 条件
      充足時のみ（未充足は条件列挙を guidance に載せ common phase へ縮退）。
    - **1 行でも判定不能（被相続人特定不能・親/兄弟姉妹の特定不能等）なら
      problems へ列挙し全体を要確認へ倒す**（§2 fail-closed・封筒起票なし）。
    - F5 の「収集見込み（弁護士確認前）」注記を guidance へ必ず引き継ぐ（§2）。
    """
    from koseki_chain import assess_for_rank

    m = PlanMaterials(case_record_id=case_record_id, phase="common")
    persons = await _load_persons(case_record_id)
    by_id = {_v(r, "$id"): r for r in persons}

    decedents = [r for r in persons if _v(r, "被相続人フラグ") == "yes"]
    if len(decedents) != 1:
        m.problems.append(
            f"被相続人を特定できません（被相続人フラグ=yes が {len(decedents)} 行・"
            "人物確認語彙で特定してください）")
        return m
    decedent = decedents[0]
    dec_id = _v(decedent, "$id")
    m.decedent_person_id = None          # common では (6) は null（§4-v2）
    m.persons[dec_id] = decedent

    kosekis = await _load_kosekis(case_record_id)
    m.app33 = sorted(
        ([_v(r, "$id"),
          hashlib.sha256(_v(r, "読解JSON").encode("utf-8")).hexdigest()]
         for r in kosekis), key=lambda x: int(x[0]))

    cands: list[dict] = []
    snapshot_rows: list[dict] = []

    async def add_line(line_type: str, person_id, address: str,
                       status_override: str | None = None):
        req = LINE_REQUEST_TYPE[line_type]
        if line_type == "applicant_current":
            muni = INPUT_REQUIRED
            app31 = {"municipality": INPUT_REQUIRED, "app31_record_id": "",
                     "有効": "", "fallback": ""}
        else:
            app31 = await resolve_app31(address)
            muni = app31["municipality"]
        status = status_override or "propose"
        if muni == INPUT_REQUIRED or req == INPUT_REQUIRED:
            status = "input_required"    # 相関制約 3/4（§4A）
        # IMPL-fix1 IMPL-01: 様式1（生年月日必須・§2A/§1.3）で対象 person の
        # 出生行年月日が取得不能（空）なら municipality/request_type の要入力と
        # 同列に input_required（空のまま propose→M1 エラー遷移へ進む経路を遮断）
        if LINE_FORM[line_type] == "form1" and person_id is not None:
            rec = by_id.get(person_id)
            if rec is None or not _birth_wareki(rec):
                status = "input_required"
        cands.append({"line_type": line_type, "request_type": req, "count": 1,
                      "person_id": person_id, "municipality": muni,
                      "status": status})
        snapshot_rows.append({"line_type": line_type,
                              "person_id": person_id or "",
                              "市区町村名": app31["municipality"]
                              if muni != INPUT_REQUIRED else "",
                              "app31_record_id": app31["app31_record_id"],
                              "有効": app31["有効"],
                              "fallback": app31["fallback"]})

    # ── 共通行（§1 最優先・run 非依存）。附票は「除票不能時の切替」＝切替判断は
    #    [人]（除票不能の事実の供給源が無いため機械は propose しない・input_required）
    await add_line("joh_removed", dec_id, _v(decedent, "住所最新"))
    await add_line("fuhyo", dec_id, _v(decedent, "本籍最新"),
                   status_override="input_required")

    # ── 第二段（§2B 6 条件）────────────────────────────────────────────────
    full_ok, run, conds = await _second_stage_conditions(case_record_id)
    if not full_ok:
        m.guidance.extend(conds)
        m.candidates = sort_candidates(cands)
        m.app31_snapshot = sort_app31_snapshot(snapshot_rows)
        m.guidance.append(
            "収集見込み（弁護士確認前・OCR表記揺れでリンクが切れる可能性が"
            "あります）")               # F5 注記の写像（§2）
        return m

    m.phase = "full"
    m.run_id = str(run.id)
    m.decedent_person_id = str(run.decedent_person_id)
    if str(run.decedent_person_id) != dec_id:
        m.problems.append(
            "被相続人が run と App34 で不一致です（フラグ付替えの疑い・要確認）")
        return m
    m.app36_rows_hash = await _app36_rows_hash(case_record_id)

    if _senjun_hoki_flagged(run):
        # §3-2: マトリクス展開なし＝共通行のみ＋個別確定警報
        m.guidance.append("必要書類の個別確定が必要です（先順位放棄・"
                          "マトリクスによる自動導出は行いません）")
    else:
        rank = int(run.rank)
        lines = KOSEKI_MATRIX["by_rank"].get(rank, ())
        heirs = (run.result_payload or {}).get("heirs") or []
        if rank == 3 and any(h.get("zokugara_code") == "nephew_niece_rep"
                             for h in heirs):
            lines = tuple(lines) + KOSEKI_MATRIX["daishu_extra"]
        for line_type in lines:
            if line_type == "decedent_joseki":
                await add_line(line_type, dec_id, _v(decedent, "本籍最新"))
            elif line_type == "chain_missing":
                assessment = assess_for_rank(kosekis, rank)
                for missing in assessment["未収集"]:
                    await add_line(line_type, dec_id,
                                   str(missing.get("本籍") or ""))
                m.guidance.append(assessment["注記"])   # F5 注記の写像
            elif line_type == "parents_death":
                for code in ("父人物ID", "母人物ID"):
                    pid = _v(decedent, code)
                    parent = by_id.get(pid)
                    if not pid or parent is None:
                        m.problems.append(
                            f"{code} の人物を特定できません（要入力・"
                            "書き込みなし）")
                        continue
                    m.persons[pid] = parent
                    await add_line(line_type, pid, _v(parent, "本籍最新"))
            elif line_type == "sibling_death":
                for h in heirs:
                    if h.get("zokugara_code") != "nephew_niece_rep":
                        continue
                    sib = _resolve_sibling(by_id, dec_id,
                                           str(h.get("person_id") or ""))
                    if sib is None:
                        m.problems.append(
                            "兄弟姉妹（申述人の親）を特定できません"
                            "（親エッジ共有判定の不成立・要入力・書き込みなし）")
                        continue
                    m.persons[sib] = by_id[sib]
                    await add_line(line_type, sib, _v(by_id[sib], "本籍最新"))
            elif line_type == "applicant_current":
                await add_line(line_type, None, "")
    if m.problems:
        return m
    m.candidates = sort_candidates(cands)
    m.app31_snapshot = sort_app31_snapshot(snapshot_rows)
    m.guidance.append(
        "収集見込み（弁護士確認前・OCR表記揺れでリンクが切れる可能性があります）")
    return m


def _resolve_sibling(by_id: dict, dec_id: str, nephew_pid: str) -> str | None:
    """sibling_death の親選定（§2A fix3・fix6: App34 親エッジ共有判定の 4 分岐）。

    甥姪行の父/母のうち、**被相続人と父母（親エッジ）を共有する側**を返す。
    (a) 一意特定→pid／(b) 共有なし・(c) 両方該当・(d) 親 ID 欠損→None（要入力）。
    """
    nephew = by_id.get(nephew_pid)
    dec = by_id.get(dec_id)
    if nephew is None or dec is None:
        return None
    dec_parents = {p for p in (_v(dec, "父人物ID"), _v(dec, "母人物ID")) if p}
    if not dec_parents:
        return None                       # (d) 被相続人側の親 ID 欠損
    # IMPL-fix1 IMPL-03（司令塔裁定）: 「親 ID 欠損」は**片側欠損を含む**——
    # 甥姪の父母人物 ID の一方でも欠損なら自動確定しない（両側が揃う場合のみ
    # 共有親判定）。欠損側の親が別の兄弟姉妹である可能性を排除できず、
    # 半血判定（民法 900 条④但書）を誤り得るため安全側へ固定（要入力）
    parent_ids = [_v(nephew, "父人物ID"), _v(nephew, "母人物ID")]
    if not all(parent_ids):
        return None                      # (d) 片側欠損を含む親 ID 欠損
    hits = []
    for pid in parent_ids:
        cand = by_id.get(pid)
        if cand is None:
            return None                  # (d) 参照先レコード不在も欠損扱い
        cand_parents = {p for p in (_v(cand, "父人物ID"),
                                    _v(cand, "母人物ID")) if p}
        if cand_parents & dec_parents:
            hits.append(pid)
    if len(hits) == 1:                    # (a) 一意特定のみ採用
        return hits[0]
    return None                           # (b) 共有なし／(c) 両方該当


async def _second_stage_conditions(case_record_id: str):
    """§2B の 6 条件（充足可否, head run, 未充足の道案内列挙）。"""
    from hub.derivation_models import get_current_head, get_leaf_decision
    from hub.heir_projection import ZOKUGARA_CODE_TO_APP36

    conds: list[str] = []
    head = await get_current_head(case_record_id)
    if head is None:
        return False, None, ["条件未充足: 導出 run がありません（§2B-1）"]
    leaf = await get_leaf_decision(head.id)
    if leaf is None or leaf.decision != "confirmed":
        return False, None, ["条件未充足: 導出が confirmed ではありません（§2B-2）"]
    rows = await kintone.search_records(
        kintone.KintoneApp("App 36 (相続人)", "APP_SOUZOKUNIN",
                           "TOKEN_SOUZOKUNIN"),
        f'案件レコードID = "{case_record_id}" order by $id asc limit 100',
        fields=["$id", "$revision", "current_derivation_run_id",
                "導出元人物ID", "戸籍確認済", "続柄"])
    heirs = (head.result_payload or {}).get("heirs") or []
    expect = {str(h.get("person_id") or ""):
              ZOKUGARA_CODE_TO_APP36.get(h.get("zokugara_code"), "")
              for h in heirs}
    got: dict[str, str] = {}
    for r in rows:
        if _v(r, "current_derivation_run_id") != str(head.id):
            conds.append("条件未充足: App36 に run 不一致行があります（§2B-3）")
        if _v(r, "戸籍確認済") != "yes":
            conds.append("条件未充足: App36 に 戸籍確認済=yes でない行があります"
                         "（§2B-4）")
        pid = _v(r, "導出元人物ID")
        if pid in got:
            conds.append("条件未充足: App36 に冪等キー重複行があります（§2B-6）")
        got[pid] = _v(r, "続柄")
    if not conds and got != expect:
        conds.append("条件未充足: App36 の person/続柄集合が run と不一致です"
                     "（§2B-5/6）")
    if conds:
        return False, None, sorted(set(conds))
    return True, head, []


async def _app36_rows_hash(case_record_id: str) -> str:
    rows = await kintone.search_records(
        kintone.KintoneApp("App 36 (相続人)", "APP_SOUZOKUNIN",
                           "TOKEN_SOUZOKUNIN"),
        f'案件レコードID = "{case_record_id}" order by $id asc limit 100',
        fields=["$id", "$revision", "current_derivation_run_id", "導出元人物ID"])
    tuples = sorted(
        ([_v(r, "$id"), _v(r, "$revision"),
          _v(r, "current_derivation_run_id"), _v(r, "導出元人物ID")]
         for r in rows), key=lambda x: int(x[0]))
    return canonical_sha256(tuples)


# ══════════════════════════════════════════════════════════════
# 提案封筒（§2 [2]・heir_envelope 型・open 限定回収=§2C-4 fix3）
# ══════════════════════════════════════════════════════════════

async def _find_envelopes(case_record_id: str, idem_key: str) -> list[dict]:
    esc = idem_key.replace('"', "")
    records = await kintone.search_records(
        APP_SHIPPING, f'チャネル固有データ like "{esc}"',
        fields=["$id", "チャネル固有データ", "発送ステータス", "実行済み"])
    out = []
    for rec in records:
        try:
            data = json.loads(_v(rec, "チャネル固有データ"))
        except (ValueError, TypeError):
            continue
        detail = data.get(TOP_KEY)
        if isinstance(detail, dict) and detail.get("冪等キー") == idem_key:
            out.append(rec)
    return out


async def file_plan_envelope(materials: "PlanMaterials") -> dict:
    """提案封筒の起票（§2 [2]）。

    - 封筒冪等（§2C-4 fix3・open 限定回収）: 同一冪等キーの **open
      （要確認/実行済み no）封筒があれば already_filed（新規起票しない）。
      **terminal のみなら新規起票**（却下=非抑止の構造的成立）。
    - detail は canonical 未フィルタ全候補（fix4 H01-01）＋保存境界検証
      （validate_detail）。単票 API 必須（§1.5）。
    """
    detail = materials.detail()
    validate_detail(detail)
    idem_key = detail["冪等キー"]
    for rec in await _find_envelopes(materials.case_record_id, idem_key):
        if _v(rec, "発送ステータス") == STATUS_PENDING \
                and _v(rec, "実行済み") == "no":
            return {"status": "already_filed", "record_id": _v(rec, "$id")}
    unit = _unit_for_case(os.environ.get("SOUZOKU_KINTONE_APP_ID", "").strip())
    record_id = await kintone.create_record(APP_SHIPPING, {
        "発送ステータス": STATUS_PENDING,
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": unit,
        "案件アプリID": os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
        "案件レコードID": materials.case_record_id,
        "実行済み": "no",
        "件名": f"職務上請求の請求案: 案件 No.{materials.case_record_id}"
                f"（{detail['phase']}・候補 {len(detail['candidates'])} 件）",
        "チャネル固有データ": json.dumps({TOP_KEY: detail},
                                          ensure_ascii=False),
    })
    return {"status": "filed", "record_id": str(record_id)}


# ══════════════════════════════════════════════════════════════
# 関所確定 → M1 委譲（§2 [3]-[4]・§2C 実行時フィルタ・§4B 冪等）
# ══════════════════════════════════════════════════════════════

_COMMON_TYPES = ("住民票の除票", "戸籍の附票")


async def _existing_m1_records(case_record_id: str) -> list[dict]:
    return await kintone.search_records(
        APP_SHIPPING,
        f'チャネル in ("職務上請求") and 案件レコードID = "{case_record_id}"'
        " order by $id asc limit 100",
        fields=["$id", "チャネル固有データ", "発送ステータス"])


def _parse_m1_meta(rec: dict):
    """既存 M1 のチャネル固有データを JSON parse（§4B・parse 不能は None）。"""
    try:
        data = json.loads(_v(rec, "チャネル固有データ"))
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _common_line_skip(candidate: dict, m1_records: list[dict],
                      app33_fulfilled: bool) -> bool:
    """§2C 実行時フィルタ（M1 create 直前限定・fix4 H01-01）。共通行のみ対象。

    (i) App33 充足 (ii) 未 terminal の既存 M1 に JSON parse 後の 3 点一致
    （request_items type・plan_idem の対象 person 断片・municipality）があれば
    skip。**取り零しは §4B の plan_idem 照合（最終防壁）が止める**（§2C は
    前段フィルタ・parse 不能レコードはここでは判定せず §4B へ委ねる）。
    """
    if candidate["line_type"] not in ("joh_removed", "fuhyo"):
        return False
    if app33_fulfilled:
        return True
    for rec in m1_records:
        if _v(rec, "発送ステータス") in ("完了", "エラー"):
            continue
        data = _parse_m1_meta(rec)
        if data is None:
            continue
        items = data.get("request_items") or []
        types = {i.get("type") for i in items if isinstance(i, dict)}
        if not (types & set(_COMMON_TYPES)):
            continue
        if data.get("municipality") != candidate["municipality"]:
            continue
        pid_part = f":{candidate['person_id']}:" \
            if candidate["person_id"] else ":-:"
        if pid_part in str(data.get("plan_idem") or ""):
            return True
    return False


async def _resolve_shokumu_plan(group, case_record_id: str) -> dict:
    """請求案封筒の確定（§2 [3]-[4]）。

    gate（封筒再読・detail 検証・stale 再計算）→ §2C 実行時フィルタ →
    束ね（自治体×対象者×様式・§2A fix3）→ §4B plan_idem/m1_fingerprint 照合 →
    M1 App30 起票（既存 prepare へ委譲・**下書きまで**）→ 全 propose 完了で
    封筒クローズ。部分失敗は例外伝播（封筒 open 維持・再確定=新関所往復が
    reconcile 入口・§4B fix3）。承認済みへの遷移コードは存在しない（§0）。
    """
    if not _CASE_RE.fullmatch(case_record_id or ""):
        return {"status": "aborted",
                "reason": "案件レコードIDが数字列ではありません（書き込みなし）"}
    results = []
    for item in group.items:
        record = await kintone.get_record(APP_SHIPPING, item.record_id)
        status, executed = _v(record, "発送ステータス"), _v(record, "実行済み")
        if status != STATUS_PENDING or executed != "no":
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} が要確認ではなくなって"
                              f"います（発送ステータス={status}・実行済み="
                              f"{executed}）。グループ全体を中止しました"
                              "（書き込みなし）"}
        try:
            detail = dict(item.detail)
            validate_detail(detail)
        except PlanPolicyError:
            return {"status": "aborted",
                    "reason": "封筒 detail が保存規格に不適合です"
                              "（要確認・書き込みなし）"}
        if detail["case_record_id"] != case_record_id:
            return {"status": "aborted",
                    "reason": "封筒の案件が指定案件と一致しません（書き込みなし）"}
        # ── stale 再計算（§4-v2: 材料の現在値で plan_hash を再計算し照合。
        #    生成と同一関数 build_plan を使う＝読値と材料の 1:1 保証）────────────
        current = await build_plan(case_record_id)
        if current.problems:
            return {"status": "aborted",
                    "reason": "前提の再確認で判定不能があります（"
                              + "／".join(current.problems) + "・書き込みなし）"}
        if current.plan_hash() != detail["plan_hash"]:
            return {"status": "aborted",
                    "reason": "前提が変わっています（材料の変更を検出）。"
                              "新しい請求案から確定してください（書き込みなし）"}
        outcome = await _issue_m1_for_envelope(item, detail, current)
        results.append(outcome)
    return {"status": "resolved", "case_record_id": case_record_id,
            "items": results}


def _bundle_candidates(cands: list[dict]) -> dict:
    """束ね（§2A fix3: 同一自治体×同一対象者×同一様式のみ・propose 限定）。

    同一 request_type は count 合算（channel_json 完成前の正規化・§4B fix7
    方式A——fingerprint 算出時ではなくここで併合する）。
    """
    bundles: dict[tuple, dict] = {}
    for c in cands:
        if c["status"] != "propose":
            continue                       # 相関制約 1/2/7（§4A）
        form = LINE_FORM[c["line_type"]]
        key = (c["municipality"], c["person_id"] or "-", form)
        b = bundles.setdefault(key, {"municipality": c["municipality"],
                                     "person_id": c["person_id"],
                                     "form": form, "lines": [], "items": {}})
        b["lines"].append(c["line_type"])
        b["items"][c["request_type"]] = \
            b["items"].get(c["request_type"], 0) + c["count"]
    return bundles


async def _issue_m1_for_envelope(item, detail: dict,
                                 materials: "PlanMaterials") -> dict:
    from dispatch_bot import shokumu

    case_record_id = detail["case_record_id"]
    m1_records = await _existing_m1_records(case_record_id)
    # §2C(ii) fix2/§6-24: チャネル固有データが**非空なのに JSON parse 不能**な
    # 既存 M1 は照合不成立＝「未起票扱い」にせず**全体を要確認へ倒す**（安全側・
    # 壊れ JSON の存在自体が異常）
    for rec in m1_records:
        raw = _v(rec, "チャネル固有データ")
        if raw and _parse_m1_meta(rec) is None:
            return {"review_record_id": item.record_id,
                    "phase": detail["phase"], "issued": 0, "recovered": 0,
                    "held": len(detail["candidates"]), "skipped": 0,
                    "envelope_closed": False,
                    "reason": "既存の職務上請求レコードに解釈不能なデータが"
                              "あります（要確認・書き込みなし）"}
    # 除票/附票の App33 充足判定材料（読解 JSON の書類種別）は現行読解に存在
    # しないため常に非充足（§2C-1 の判定は供給後に実効化・安全側=再提案しても
    # §4B が二重起票を止める）
    app33_fulfilled = False
    skipped: list[str] = []
    plan_cands = []
    for c in detail["candidates"]:
        if _common_line_skip(c, m1_records, app33_fulfilled):
            skipped.append(c["line_type"])
        else:
            plan_cands.append(c)
    bundles = _bundle_candidates(plan_cands)
    unit_field = _unit_for_case(
        os.environ.get("SOUZOKU_KINTONE_APP_ID", "").strip())
    case_rec = await kintone.get_record(
        kintone.KintoneApp("相談カード (相続)", "SOUZOKU_KINTONE_APP_ID",
                           "SOUZOKU_KINTONE_API_TOKEN"), case_record_id)
    customer = _v(case_rec, "顧客名")     # §2D fix2: 確定時再取得（保存しない）

    issued, recovered, held = [], [], []
    for _key, b in sorted(bundles.items()):
        idem = plan_idem_key(case_record_id, b["municipality"],
                             b["person_id"], b["form"])
        target = _build_target(materials, b["person_id"])
        # channel_json 完成前の正規化済み request_items（type 昇順・合算済み）
        request_items = [{"type": t, "count": n}
                         for t, n in sorted(b["items"].items())]
        parsed = {"task_params": {"request_items": request_items,
                                  "municipality": b["municipality"],
                                  "target": target, "unit": PLAN_UNIT}}
        channel_json = shokumu.build_channel_json(parsed)
        app31_id = next((r["app31_record_id"]
                         for r in materials.app31_snapshot
                         if r["市区町村名"] == b["municipality"]), "")
        fingerprint = m1_fingerprint(channel_json, app31_id,
                                     b["lines"], PLAN_UNIT, b["form"])
        hit = _match_plan_idem(m1_records, idem)
        if hit is not None:
            audit_saved = _extract_audit(hit)     # IMPL-02: 共通検証境界を通す
            if audit_saved is None:
                held.append(idem)      # 閉集合/grammar 不適合 → 要確認・create 0
                continue
            if audit_saved["m1_fingerprint"] == fingerprint:
                recovered.append(idem)     # 一致 = skip 回収（§4B fix5）
                continue
            held.append(idem)              # 不一致 = 要確認（自動 merge 禁止）
            continue
        audit = {"filed_by": "shokumu_plan",
                 "plan_envelope_no": str(item.record_id),
                 "plan_hash": detail["plan_hash"],
                 "plan_idem": idem,
                 "m1_fingerprint": fingerprint,
                 "plan_lines": sorted(set(b["lines"]), key=LINE_TYPES.index)}
        validate_audit_meta(audit)     # IMPL-02: 保存時も共通検証境界を通す
        # §2D: _fields_shokumu_seikyu と同一 field 集合（byte 水準一致は
        # テストで pin）＋ file_from_pending 共通部。単票 API・**下書き止まり**
        await kintone.create_record(APP_SHIPPING, {
            "発送ステータス": "下書き",
            "ユニット種別": unit_field,
            "顧客名表示用": customer,
            "案件アプリID": os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
            "案件レコードID": case_record_id,
            "実行済み": "no",
            "チャネル": "職務上請求",
            "件名": f"職務上請求（{customer}・{channel_json['municipality']}）",
            "宛先名": "",
            "宛先郵便番号": "",
            "宛先住所": "",
            "チャネル固有データ": json.dumps({**channel_json, **audit},
                                              ensure_ascii=False),
        })
        issued.append(idem)

    closed = not held
    if closed:
        await kintone.update_record(APP_SHIPPING, item.record_id, {
            "発送ステータス": "完了", "実行済み": "yes"})
    return {"review_record_id": item.record_id, "phase": detail["phase"],
            "issued": len(issued), "recovered": len(recovered),
            "held": len(held), "skipped": len(skipped),
            "envelope_closed": closed}


def _match_plan_idem(m1_records: list[dict], idem: str):
    """§4B: plan_idem 完全一致の既存 M1（JSON parse・like 誤爆なし）。"""
    for rec in m1_records:
        data = _parse_m1_meta(rec)
        if data is not None and data.get("plan_idem") == idem:
            return data
    return None


def _build_target(materials: "PlanMaterials", person_id) -> dict:
    """M1 target（§4B fix6: 4 キー閉集合・全キー必須・値なしは ""）。

    確定時に App34 を再取得済みの materials.persons から組み立てる（§4A M02:
    封筒には実値を保存しない・stale 検出は plan_hash 再計算が担う）。
    フリガナ・世帯主・筆頭者は**非搭載**（§4B fix6 の閉集合・構造 pin 対象）。
    """
    snap = materials.app34_snapshot().get(person_id or "", {})
    return {"対象者": snap.get("氏名", ""),
            "生年月日": snap.get("身分事項.出生行の年月日", ""),
            "本籍": snap.get("本籍最新", ""),
            "住所": snap.get("住所最新", "")}
