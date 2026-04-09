#!/usr/bin/env python3
"""
不動産登記JSONをkintone不動産管理アプリに登録するスクリプト

使い方:
    export KINTONE_SUBDOMAIN="your-subdomain"
    export KINTONE_FUDOSAN_APP_ID="123"
    export KINTONE_FUDOSAN_API_TOKEN="your-token"
    python registry_to_kintone.py <registry_json_file>

    例:
    python registry_to_kintone.py document_registry.json

フィールドコード対応:
    案件番号       ← 不動産番号
    種別           ← 種別（土地/建物/区分建物）
    所在           ← 表題部.所在
    地番           ← 表題部.地番
    地目           ← 表題部.地目
    地積           ← 表題部.地積_m2
    床面積1階      ← 表題部.床面積_m2 (1階部分)
    床面積2階      ← 表題部.床面積_m2 (2階部分)
    床面積3階      ← 表題部.床面積_m2 (3階部分)
    建物名         ← 表題部.種類
    部屋番号       ← 表題部.家屋番号
    専有面積       ← 区分建物の場合の専有部分床面積
    階数           ← 表題部.構造から階数を抽出
    持分割合       ← 甲区 最新所有者の持分
    固定資産税評価額 ← （登記情報に含まれない場合は空）
    担保抵当権     ← 乙区 有効な抵当権/根抵当権の有無
    担保内容       ← 乙区 有効な権利の詳細
    状況           ← 甲区 最新所有者氏名・住所
    備考           ← OCR品質メモ＋表題部原因
"""

import sys
import os
import json
import re
import urllib.request
import urllib.error


# ── ヘルパー関数 ──────────────────────────────────────────

def get_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        print(f"エラー: 環境変数 {name} が設定されていません")
        sys.exit(1)
    return val


def parse_floor_areas(floor_text: str | None) -> dict:
    """
    「1階 58.50、2階 62.60、3階 62.60」などから各階面積を抽出。
    戻り値: {"1": 58.5, "2": 62.6, "3": 62.6, ...}（float）
    """
    result = {}
    if not floor_text:
        return result
    # パターン: "N階 数値" または "N階部分 数値"
    for m in re.finditer(r'(\d+)階(?:部分)?\s*([\d.]+)', floor_text):
        try:
            result[m.group(1)] = float(m.group(2))
        except ValueError:
            pass
    return result


def extract_floor_count(structure: str | None) -> str:
    """「鉄骨造陸屋根3階建」→「3」"""
    if not structure:
        return ""
    m = re.search(r'(\d+)階建', structure)
    return m.group(1) if m else ""


def get_active_rights(rights_list: list) -> list:
    """乙区から有効な権利のみ返す。
    ・抹消フラグがFalse
    ・登記目的に「抹消」を含まない（抹消登記自体を除外）
    """
    return [
        r for r in rights_list
        if not r.get("抹消", False)
        and "抹消" not in str(r.get("登記目的", ""))
    ]


def get_latest_owner(kouku_list: list) -> dict | None:
    """
    甲区から最新の有効（抹消=false）所有権エントリを返す。
    「付記」エントリは除外。
    """
    candidates = [
        e for e in kouku_list
        if not e.get("抹消", False)
        and "付記" not in str(e.get("順位番号", ""))
        and e.get("所有者", {}).get("氏名_名称")
    ]
    return candidates[-1] if candidates else None


def extract_mochiwari(owner_name: str | None) -> str:
    """「下玉利栄太（持分2分の1）」→「2分の1」"""
    if not owner_name:
        return ""
    m = re.search(r'持分(.+?)[）)]', owner_name)
    return m.group(1) if m else ""


def build_mortgage_text(active_rights: list) -> str:
    """有効な乙区権利を人が読める形式にまとめる。"""
    lines = []
    for r in active_rights:
        parts = []
        if r.get("登記目的"):
            parts.append(r["登記目的"])
        if r.get("極度額_債権額"):
            parts.append(r["極度額_債権額"])
        if r.get("権利者_債権者"):
            parts.append(r["権利者_債権者"])
        if r.get("債務者"):
            parts.append(f"債務者: {r['債務者']}")
        if r.get("受付年月日"):
            parts.append(r["受付年月日"])
        lines.append(" / ".join(parts))
    return "\n".join(lines)


def property_to_kintone_record(prop: dict, ocr_note: str) -> dict:
    """
    property（1物件）をkintoneレコード形式（フィールドコード: {value: ...}）に変換。
    """
    title = prop.get("表題部", {})
    kouku = prop.get("甲区_所有権", [])
    otsuku = prop.get("乙区_その他権利", [])

    # 床面積パース
    floors = parse_floor_areas(title.get("床面積_m2"))

    # 最新所有者
    latest_owner = get_latest_owner(kouku)
    owner_name = latest_owner["所有者"]["氏名_名称"] if latest_owner else ""
    owner_addr = latest_owner["所有者"].get("住所", "") if latest_owner else ""
    mochiwari = extract_mochiwari(owner_name)
    owner_display = re.sub(r'（持分.+?）', '', owner_name or "").strip()

    # 乙区（有効）
    active_otsuku = get_active_rights(otsuku)
    has_mortgage = any(
        r.get("権利種別") in ("抵当権", "根抵当権")
        for r in active_otsuku
    )
    mortgage_text = build_mortgage_text(active_otsuku)

    # 4階以上の床面積を備考にまとめる
    upper_floors = {k: v for k, v in floors.items() if int(k) >= 4}
    if upper_floors:
        biko = "【床面積】" + "、".join(f"{k}階 {v}㎡" for k, v in sorted(upper_floors.items(), key=lambda x: int(x[0])))
    else:
        biko = ""

    shubetsu = prop.get("種別") or ""
    # 建物は地番なし（空欄）、区分建物のみ部屋番号に家屋番号を入れる
    chiban = title.get("地番") or ""
    heya_bango = title.get("家屋番号") or "" if shubetsu == "区分建物" else ""

    record = {
        "種別":           {"value": shubetsu},
        "所在":           {"value": title.get("所在") or ""},
        "地番":           {"value": chiban},
        "地目":           {"value": title.get("地目") or ""},
        "地積":           {"value": title.get("地積_m2") or ""},
        "床面積1階":      {"value": floors.get("1", None)},
        "床面積2階":      {"value": floors.get("2", None)},
        "床面積3階":      {"value": floors.get("3", None)},
        "建物名":         {"value": title.get("種類") or ""},
        "部屋番号":       {"value": heya_bango},
        "専有面積":       {"value": ""},          # 区分建物専有面積（今回データなし）
        "階数":           {"value": extract_floor_count(title.get("構造"))},
        "持分割合":       {"value": mochiwari},
        "固定資産税評価額": {"value": ""},        # 登記情報に含まれない
        "担保抵当権":     {"value": "有" if has_mortgage else "無"},
        "担保内容":       {"value": mortgage_text},
        "備考":           {"value": biko},
    }
    return record


# ── kintone API ────────────────────────────────────────────

def post_records(subdomain: str, app_id: str, api_token: str, records: list) -> dict:
    """
    kintone REST APIで複数レコードを一括登録（最大100件/リクエスト）。
    戻り値: {"ids": [...], "revisions": [...]}
    """
    url = f"https://{subdomain}.cybozu.com/k/v1/records.json"
    body = {"app": app_id, "records": records}
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-Cybozu-API-Token": api_token,
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post_records_chunked(subdomain: str, app_id: str, api_token: str,
                         records: list, chunk_size: int = 100) -> list:
    """100件を超える場合はチャンクに分けて登録。登録されたIDリストを返す。"""
    all_ids = []
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        result = post_records(subdomain, app_id, api_token, chunk)
        ids = result.get("ids", [])
        all_ids.extend(ids)
        print(f"  登録完了: {len(ids)} 件 (レコードID: {ids})")
    return all_ids


# ── メイン ─────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print(f"使い方: python {sys.argv[0]} <registry_json_file>")
        sys.exit(1)

    json_path = sys.argv[1]
    if not os.path.exists(json_path):
        print(f"エラー: ファイルが見つかりません: {json_path}")
        sys.exit(1)

    subdomain  = get_env("KINTONE_SUBDOMAIN")
    app_id     = get_env("KINTONE_FUDOSAN_APP_ID")
    api_token  = get_env("KINTONE_FUDOSAN_API_TOKEN")

    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    properties = data.get("properties", [])
    ocr_note   = data.get("OCR品質メモ", "")

    if not properties:
        print("エラー: JSONに properties が見つかりません")
        sys.exit(1)

    print(f"入力ファイル : {json_path}")
    print(f"物件数       : {len(properties)} 件")
    print(f"登録先アプリ : https://{subdomain}.cybozu.com/k/{app_id}/")
    print("-" * 60)

    # kintoneレコードに変換
    records = []
    for i, prop in enumerate(properties):
        rec = property_to_kintone_record(prop, ocr_note)
        records.append(rec)
        owner_entry = get_latest_owner(prop.get("甲区_所有権", []))
        owner_name = (owner_entry["所有者"]["氏名_名称"] if owner_entry else "不明")
        print(f"  [{i+1}] {prop.get('種別','?')} / "
              f"{prop.get('表題部',{}).get('所在','?')} "
              f"{prop.get('表題部',{}).get('地番') or prop.get('表題部',{}).get('家屋番号','?')} / "
              f"所有者: {owner_name}")

    print(f"\nkintoneに登録中...")

    try:
        ids = post_records_chunked(subdomain, app_id, api_token, records)
        print(f"\n完了: 合計 {len(ids)} 件を登録しました")
        print(f"登録レコードID: {ids}")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        print(f"\nHTTPエラー {e.code}:")
        try:
            err = json.loads(body)
            print(json.dumps(err, ensure_ascii=False, indent=2))
        except Exception:
            print(body)
        sys.exit(1)
    except Exception as e:
        print(f"\nエラー: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
