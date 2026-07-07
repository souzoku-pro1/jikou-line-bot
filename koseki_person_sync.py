"""人物レコード生成（R4-1）: App 33 の読解JSON → App 34（人物）起票

設計: docs/koseki-ocr/02 §2（実機完全形35）・2026-07-07 R4-1 裁定
- 入力は 読解状態 in (確認済, AI読解済) の App 33 レコード。案件参照が空の戸籍は
  人物化しない（宙に浮いた人物を作らない・起動経路が R4-0 の案件確定後続である理由）
- **名寄せ確定=未確定 で起票**（機械は名寄せを確定しない・確定は R4-2 の関所経由で人）。
  確認済み系フィールド（確認状態/確認者/確認日時/グラフ確定日時/相続人候補/相続資格/
  被相続人フラグ）には一切書かない（自動遷移コード禁止・既存原則）
- App 36（相続人）には一切書かない（相続人化は R4-3）
- 生年月日/死亡日（DATE 型・確定値）は**書かない**——和暦原文は身分事項サブテーブルの
  年月日列（文字列）に保持し、西暦への確定は人の確認時（02 §3 の原則）
- 親子エッジ（父人物ID/母人物ID）は**同一戸籍内で導出できる範囲の候補**として設定:
  筆頭者（氏名一致）＋配偶者（婚姻の相手方一致）＋子（続柄が○男/○女）。
  性別は続柄末尾から推定できた場合のみ 父/母 を割り当てる（確定扱いにしない＝
  名寄せ確定=未確定・確認状態=初期値のまま）
- 冪等: (戸籍レコードID, 氏名) の組で既存検索し、あれば再起票しない。
  **既存レコードの更新はしない**（マージは R4-2 の仕事・確定操作なしで App 34 は変化しない）
- 有効化フラグ: env KOSEKI_PERSON_SYNC_ENABLED=1 のときのみ動く（**既定は無効**。
  既存の *_DISABLED 慣行と逆だが、本番書き込みを明示的に有効化する安全側・
  2026-07-07 運用注意に従う）
"""

import json
import os
import re

from hub import kintone

APP_KOSEKI_BOOK = kintone.KintoneApp(
    "App 33 (戸籍読解)", "APP_KOSEKI_BOOK", "TOKEN_KOSEKI_BOOK")
APP_KOSEKI_PERSON = kintone.KintoneApp(
    "App 34 (人物)", "APP_KOSEKI_PERSON", "TOKEN_KOSEKI_PERSON")

UNIT = "相続一般"
READABLE_STATES = ("確認済", "AI読解済")

# 身分事項サブテーブルの 事項種別 許容値（02 §2）。読解JSONの種別のうち
# ここに無いもの（転籍/入籍/除籍/改製）は「その他」に写像し原文を記載原文に保持
_EVENT_TYPES = {"出生", "死亡", "婚姻", "離婚", "養子縁組", "離縁", "認知"}

_CHILD_ZOKUGARA = re.compile(r"^(長|二|三|四|五|六|七|八|九|十)?[男女]$")


def sync_enabled() -> bool:
    return os.environ.get("KOSEKI_PERSON_SYNC_ENABLED") == "1"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _norm(name: str) -> str:
    """名寄せ用の簡易正字化（第1版は空白除去のみ。旧字体正規化テーブルは後続タスク）"""
    return (name or "").replace(" ", "").replace("　", "")


def _gender(zokugara: str) -> str:
    z = (zokugara or "").strip()
    if z.endswith("男") or z == "夫":
        return "男"
    if z.endswith("女") or z == "妻":
        return "女"
    return "不明"


def _is_dead(person: dict) -> bool:
    """死亡記載がある場合のみ True（死亡記載なし＝生存とは推定しない・02 §2）"""
    if "死亡" in str(person.get("除籍事由") or ""):
        return True
    return any((e or {}).get("種別") == "死亡"
               for e in person.get("身分事項") or [])


def _subtable(rows: list[dict]) -> list[dict]:
    return [{"value": {k: {"value": v} for k, v in row.items()}} for row in rows]


def _identity_rows(person: dict) -> list[dict]:
    rows = []
    for e in person.get("身分事項") or []:
        if not isinstance(e, dict):
            continue
        kind = str(e.get("種別") or "")
        mapped = kind if kind in _EVENT_TYPES else "その他"
        original = str(e.get("備考") or "")
        if mapped == "その他" and kind:
            original = f"{kind} {original}".strip()
        rows.append({"事項種別": mapped,
                     "年月日": str(e.get("日付") or ""),   # 和暦原文のまま保持
                     "相手方": str(e.get("相手方") or ""),
                     "記載原文": original})
    return rows


def _classify(reading: dict) -> dict:
    """同一戸籍内の役割分類: 筆頭者（氏名一致）・配偶者（婚姻の相手方一致）・
    子（続柄が○男/○女）・その他在籍者"""
    head_name = _norm((reading.get("戸籍") or {}).get("筆頭者") or "")
    persons = [p for p in reading.get("人物") or [] if isinstance(p, dict)]

    head = next((p for p in persons if _norm(p.get("氏名")) == head_name), None)
    spouse = None
    if head is not None:
        for p in persons:
            if p is head:
                continue
            partners = [_norm(e.get("相手方")) for e in p.get("身分事項") or []
                        if isinstance(e, dict) and e.get("種別") == "婚姻"]
            if any(x and (x == head_name or x in head_name or head_name in x)
                   for x in partners):
                spouse = p
                break
    roles = {}
    for p in persons:
        if p is head:
            roles[id(p)] = "筆頭者"
        elif p is spouse:
            roles[id(p)] = "配偶者"
        elif _CHILD_ZOKUGARA.fullmatch(str(p.get("続柄") or "").strip()):
            roles[id(p)] = "子"
        else:
            roles[id(p)] = "その他在籍者"
    return {"persons": persons, "head": head, "spouse": spouse, "roles": roles}


def _person_fields(person: dict, reading: dict, koseki_record: dict,
                   koseki_id: str, role: str, parent_ids: dict) -> dict:
    """1人物分の App 34 フィールド（確認済み系・DATE 型には書かない）"""
    name = str(person.get("氏名") or "")
    birth_raw = str(person.get("生年月日") or "")
    fields = {
        "案件アプリID": _v(koseki_record, "案件アプリID"),
        "案件レコードID": _v(koseki_record, "案件レコードID"),
        "ユニット種別": UNIT,
        "氏名": name,
        "氏名_原文": name,
        "氏名_正字": _norm(name),
        "続柄メモ": str(person.get("続柄") or ""),
        "本籍最新": str((reading.get("戸籍") or {}).get("本籍") or ""),
        "名寄せキー": f"{_norm(name)}|{birth_raw}",
        "名寄せ確定": "未確定",  # 機械は名寄せを確定しない（確定は R4-2 の関所で人）
        "読解JSON断片": json.dumps(person, ensure_ascii=False),
        "身分事項": _subtable(_identity_rows(person)),
        "登場戸籍": _subtable([{
            "戸籍レコードID": koseki_id,
            "登場区分": role,
            "続柄原文": str(person.get("続柄") or ""),
            "在籍期間メモ": "",
        }]),
    }
    gender = _gender(str(person.get("続柄") or ""))
    if gender != "不明":
        fields["性別"] = gender
    if _is_dead(person):
        fields["生死区分"] = "死亡"  # 死亡記載なしは書かない（初期値=不明のまま）
    if role == "子":
        for pid_role, pid in parent_ids.items():
            fields[pid_role] = pid  # 父人物ID / 母人物ID（候補・確定扱いにしない）
    return {k: v for k, v in fields.items() if v not in ("", [], None)}


def _escape(value: str) -> str:
    return (value or "").replace('"', '\\"')


async def _find_existing(koseki_id: str, name: str) -> str:
    """冪等キー = (戸籍レコードID, 氏名)。登場戸籍サブテーブル内の一致で判定。

    ⚠ 戸籍レコードID は SUBTABLE（登場戸籍）内のフィールドのため、kintone クエリ
    仕様上 `=` 演算子が使えない（GAIA_IQ07・2026-07-07 実機で発生）——**`in` を使う**。
    トップレベルの 氏名 は従来どおり `=`。
    """
    records = await kintone.search_records(
        APP_KOSEKI_PERSON,
        f'戸籍レコードID in ("{_escape(koseki_id)}") and 氏名 = "{_escape(name)}"',
        fields=["$id"])
    return _v(records[0], "$id") if records else ""


async def _create_or_skip(person, reading, koseki_record, koseki_id, role,
                          parent_ids, results) -> str:
    name = str(person.get("氏名") or "")
    existing = await _find_existing(koseki_id, name)
    if existing:
        results["skipped"].append({"氏名": name, "person_record_id": existing})
        return existing
    fields = _person_fields(person, reading, koseki_record, koseki_id,
                            role, parent_ids)
    person_id = str(await kintone.create_record(APP_KOSEKI_PERSON, fields))
    results["created"].append({"氏名": name, "person_record_id": person_id,
                               "登場区分": role})
    return person_id


async def sync_persons_from_koseki(koseki_record_id: str) -> dict:
    """App 33 のレコード1件から人物レコードを起票する（R4-0 確定の後続処理）。

    Returns: {"status": "synced", "created": [...], "skipped": [...]} ／
             {"status": "skipped", "reason": ...}（env・状態・案件未紐付けの縮退）
    """
    if not (APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        return {"status": "skipped",
                "reason": "APP_KOSEKI_PERSON / TOKEN_KOSEKI_PERSON 未設定"}

    koseki_record = await kintone.get_record(APP_KOSEKI_BOOK, koseki_record_id)
    state = _v(koseki_record, "読解状態")
    if state not in READABLE_STATES:
        return {"status": "skipped",
                "reason": f"読解状態が{state or '空'}（確認済/AI読解済のみ対象）"}
    if not _v(koseki_record, "案件レコードID"):
        return {"status": "skipped",
                "reason": "案件未紐付け（宙に浮いた人物を作らない）"}

    try:
        reading = json.loads(_v(koseki_record, "読解JSON") or "{}")
    except json.JSONDecodeError:
        return {"status": "skipped", "reason": "読解JSONが不正"}
    if not reading.get("人物"):
        return {"status": "skipped", "reason": "読解JSONに人物がありません"}

    c = _classify(reading)
    results: dict = {"status": "synced", "koseki_record_id": koseki_record_id,
                     "created": [], "skipped": []}

    # 親（筆頭者・配偶者）を先に起票して子のエッジ候補に使う
    parent_ids: dict[str, str] = {}
    for p in (c["head"], c["spouse"]):
        if p is None:
            continue
        pid = await _create_or_skip(p, reading, koseki_record, koseki_record_id,
                                    c["roles"][id(p)], {}, results)
        role_key = {"男": "父人物ID", "女": "母人物ID"}.get(
            _gender(str(p.get("続柄") or "")))
        if role_key and pid:
            parent_ids[role_key] = pid

    for p in c["persons"]:
        if p is c["head"] or p is c["spouse"]:
            continue
        role = c["roles"][id(p)]
        await _create_or_skip(p, reading, koseki_record, koseki_record_id, role,
                              parent_ids if role == "子" else {}, results)

    print(f"[KOSEKI_PERSON_SYNC] koseki={koseki_record_id} "
          f"created={len(results['created'])} skipped={len(results['skipped'])}")
    return results


async def sync_missing_persons(limit: int = 20) -> list[dict]:
    """案件紐付け済みだが人物未生成の戸籍を拾って人物化する（回収用・恒久部品）。

    R3 の process_unread_records と同型: 対象を検索し、1件の失敗は他を止めない。
    起動は**コード内からの手動呼び出し専用**（自動結線しない。R4-0 経路の失敗時や
    フラグ無効期間の回収に使う。本番実行は別途の明示指示を待つ）。
    KOSEKI_PERSON_SYNC_ENABLED フラグには依存しない（手動呼び出し自体が明示承認）。
    """
    if not (APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        print("[KOSEKI_PERSON_SYNC] 回収スキップ（APP_KOSEKI_PERSON 未設定）")
        return []
    records = await kintone.search_records(
        APP_KOSEKI_BOOK,
        '読解状態 in ("確認済", "AI読解済") and 案件レコードID != ""'
        f' order by レコード番号 asc limit {int(limit)}',
        fields=["$id"])
    results = []
    for record in records:
        koseki_id = str((record.get("$id") or {}).get("value") or "")
        try:
            existing = await kintone.search_records(
                APP_KOSEKI_PERSON,
                # サブテーブル内フィールドは = 不可・in を使う（GAIA_IQ07）
                f'戸籍レコードID in ("{_escape(koseki_id)}")',
                fields=["$id"])
            if existing:
                results.append({"status": "skipped",
                                "koseki_record_id": koseki_id,
                                "reason": "人物生成済み"})
                continue
            results.append(await sync_persons_from_koseki(koseki_id))
        except Exception as e:
            print(f"[KOSEKI_PERSON_SYNC] 回収失敗（他の戸籍は継続）"
                  f" koseki={koseki_id}: {e}")
            results.append({"status": "error", "koseki_record_id": koseki_id,
                            "detail": str(e)[:200]})
    return results
