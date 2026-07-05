"""財産目録 docx の生成（S3・units 層）

設計: docs/souzoku-shorui/02-zaisan-mokuroku.md・05 §3 S3

- App 財産（env APP_ZAISAN / TOKEN_ZAISAN・フィールドは
  docs/instructions/cu-app35-zaisan.md の19個）から案件の財産行を取得し、
  docx_templates/souzoku/財産目録.docx（規約配置・resolve_template）に差し込む。
  テンプレートは標準形で後日オーナーの書式に差し替え前提
  （make_zaisan_mokuroku_template.py で再生成できる）
- 可変行は S2 の fill_table_rows（row_marker 方式）。空の種別は「該当なし」行
- 金額の3桁カンマ+円表記・合計計算はコード側で行う（テンプレートに式を持たせない）
- 種別別の `特定情報` は 02 §2.3 の推奨書式を列に分解する。書式に合わない値は
  先頭列にそのまま印字する（情報を落とさない安全側）
- 前提条件ガード（全行 評価確定=yes・02 §6）は guards.ensure_valuations_confirmed
  （協議書 03 §5・遺言 04 と共有する単一実装）
"""

import io
from datetime import date

from docx import Document

from config import get_office_info
from hub import kintone
from hub.docx_builder import fill_table_rows, fill_template, resolve_template, to_wareki
from units.souzoku.guards import ensure_valuations_confirmed

APP_ZAISAN = kintone.KintoneApp("App 財産", "APP_ZAISAN", "TOKEN_ZAISAN")

UNIT = "相続一般"
DOC_TYPE = "財産目録"

# 消極財産（債務の部）に区分する財産種別（cu-app35 の選択肢13個のうち2つ）
NEGATIVE_TYPES = ("債務", "葬儀費用")


class ZaisanMokurokuError(Exception):
    """財産目録が生成できない（設定不足・データ不足）"""


def _val(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _amount(record: dict) -> int:
    raw = (record.get("評価額") or {}).get("value")
    if raw in (None, ""):
        return 0
    return int(float(raw))


def _yen(n: int) -> str:
    """3桁カンマ+円（数値の書式化は units 層で行う・02 §2）"""
    return f"{n:,}円"


def _yen_or_blank(record: dict) -> str:
    raw = (record.get("評価額") or {}).get("value")
    if raw in (None, ""):
        return ""
    return _yen(int(float(raw)))


def _parse_slash_kv(text: str) -> dict:
    """『所在 川口市○○ / 地番 12番3 / …』（02 §2.3 の不動産書式）を dict にする"""
    pairs: dict[str, str] = {}
    for part in text.replace("　", " ").replace("\n", " ").split("/"):
        key, _, value = part.strip().partition(" ")
        if key and value.strip():
            pairs[key] = value.strip()
    return pairs


def _fudousan_row(record: dict) -> dict:
    info = _val(record, "特定情報")
    kv = _parse_slash_kv(info)
    if "所在" not in kv:  # 推奨書式でない → 全文を先頭列に
        return {"所在": info, "地番家屋番号": "", "地目種別": "",
                "地積床面積": "", "持分": "", "評価額": _yen_or_blank(record)}
    return {
        "所在": kv["所在"],
        "地番家屋番号": kv.get("地番") or kv.get("家屋番号", ""),
        "地目種別": kv.get("地目") or kv.get("種類", ""),
        "地積床面積": kv.get("地積") or kv.get("床面積", ""),
        "持分": kv.get("持分", ""),
        "評価額": _yen_or_blank(record),
    }


def _yokin_row(record: dict) -> dict:
    """『○○銀行 △△支店 普通預金 口座番号1234567』（02 §2.3）を列に分解する"""
    info = _val(record, "特定情報")
    tokens = info.replace("　", " ").split()
    account = next((t for t in tokens if t.startswith("口座番号")), "")
    rest = [t for t in tokens if t != account]
    if not account or len(rest) < 3:  # 推奨書式でない → 全文を先頭列に
        return {"金融機関": info, "支店": "", "種別": "", "口座番号": "",
                "死亡日残高": _yen_or_blank(record)}
    return {
        "金融機関": rest[0],
        "支店": rest[1],
        "種別": " ".join(rest[2:]),
        "口座番号": account.removeprefix("口座番号"),
        "死亡日残高": _yen_or_blank(record),
    }


def _sonota_row(record: dict) -> dict:
    return {
        "銘柄内容": _val(record, "特定情報"),
        # App 財産に数量フィールドは無い（将来スキーマ拡張用・無ければ空欄）
        "数量": _val(record, "数量"),
        "評価額": _yen_or_blank(record),
    }


def _saimu_row(record: dict) -> dict:
    return {"内容": _val(record, "特定情報"), "金額": _yen_or_blank(record)}


def _classify(records: list[dict]) -> tuple[list, list, list, list]:
    """財産種別 → セクション（不動産／預貯金／有価証券その他／債務・葬儀費用）"""
    fudousan, yokin, sonota, saimu = [], [], [], []
    for r in records:
        t = _val(r, "財産種別")
        if t in NEGATIVE_TYPES:
            saimu.append(r)
        elif t.startswith("不動産"):
            fudousan.append(r)
        elif t == "預貯金":
            yokin.append(r)
        else:
            sonota.append(r)
    return fudousan, yokin, sonota, saimu


def _kijunbi_note(records: list[dict]) -> str:
    dates = sorted({_val(r, "評価基準日") for r in records if _val(r, "評価基準日")})
    return "、".join(to_wareki(date.fromisoformat(d)) for d in dates) or "－"


def _shutten_note(records: list[dict]) -> str:
    sources = dict.fromkeys(_val(r, "データ源") for r in records if _val(r, "データ源"))
    return "、".join(sources) or "－"


def build_zaisan_mokuroku_docx(records: list[dict], *,
                               decedent_name: str | None = None,
                               created: date | None = None) -> bytes:
    """財産行（App 財産のレコード）から財産目録 docx を組み立てる（kintone I/O なし）"""
    if not records:
        raise ZaisanMokurokuError(
            "財産行が0件です（App 財産に案件の財産が登録されていません）")
    ensure_valuations_confirmed(records)

    name = decedent_name or next(
        (_val(r, "被相続人名表示用") for r in records if _val(r, "被相続人名表示用")), "")
    office = get_office_info()
    author = "　".join(x for x in (
        office["名称"],
        f"弁護士　{office['弁護士名']}" if office["弁護士名"] else "",
    ) if x)

    fudousan, yokin, sonota, saimu = _classify(records)
    positive_total = sum(_amount(r) for r in fudousan + yokin + sonota)
    negative_total = sum(_amount(r) for r in saimu)

    template = resolve_template(UNIT, DOC_TYPE)
    scalars = {
        "{{被相続人名}}": name,
        "{{作成日}}": to_wareki(created or date.today()),
        "{{作成者}}": author,
        "{{積極財産合計}}": _yen(positive_total),
        "{{消極財産合計}}": _yen(negative_total),
        "{{純資産額}}": _yen(positive_total - negative_total),
        "{{評価基準日}}": _kijunbi_note(records),
        "{{出典資料}}": _shutten_note(records),
    }
    doc = Document(io.BytesIO(fill_template(str(template), scalars)))
    for marker, rows in (
        ("{{行:不動産}}", [_fudousan_row(r) for r in fudousan]),
        ("{{行:預貯金}}", [_yokin_row(r) for r in yokin]),
        ("{{行:有価証券}}", [_sonota_row(r) for r in sonota]),
        ("{{行:債務}}", [_saimu_row(r) for r in saimu]),
    ):
        fill_table_rows(doc, rows, row_marker=marker, empty_text="該当なし")
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


async def fetch_zaisan_records(case_app_id: str, case_record_id: str) -> list[dict]:
    """案件に紐づく有効な財産行を App 財産から取得する（env 未設定は安全側で拒否）"""
    if not APP_ZAISAN.app_id():
        raise ZaisanMokurokuError(
            "APP_ZAISAN が未設定です（App 財産の作成後に APP_ZAISAN / TOKEN_ZAISAN を"
            "環境変数に登録してください）")
    query = (f'案件アプリID = "{case_app_id}" and 案件レコードID = "{case_record_id}"'
             f' and 有効 in ("yes") order by レコード番号 asc')
    return await kintone.search_records(APP_ZAISAN, query)


async def generate_zaisan_mokuroku(case_app_id: str, case_record_id: str, *,
                                   decedent_name: str | None = None) -> bytes:
    """案件の財産行を取得して財産目録 docx を返す（保存・発送は呼び出し側の責務）"""
    records = await fetch_zaisan_records(case_app_id, case_record_id)
    return build_zaisan_mokuroku_docx(records, decedent_name=decedent_name)
