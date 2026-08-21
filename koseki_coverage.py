"""koseki_coverage — KOSEKI-CHECK-1: 戸籍不足チェック（決定的検査・read-only）

Q-KOSEKI-SURVEY 推奨案(a) の具体化。指定案件について
  (i)  被相続人の戸籍連続性（編製日/消除日・西暦の構造化値を時系列に並べ、
       被覆の切れ目を検出。生年月日_西暦があれば「出生〜最初の編製」も判定）
  (ii) 相続人ごとの「現在戸籍（戸籍種別=現行）」の有無
を機械判定する。**参考見立てであり不足の確定はしない**（機械は確定しない・
表示側で定型注記が必ず付く＝webapp_q の FLAG_NOTES）。

出力 grammar（閉集合・自由文なし・ocr_text は返さない・50KB 上限内の subset）:
  {"case_record_id": str,
   "estimate": true,                     # 参考見立てである旨の機械 marker
   "decedent": {"registered": bool, "birth_seireki": str|null},
   "chain": {"status": "ok"|"gaps_found"|"insufficient",
             "kosekis": [{"record_id", "shubetsu", "hensei", "shojo",
                          "belongs_to_decedent", "drive_file_id"}],
             "gaps": [{"kind": "birth_to_first"|"between",
                       "from": str|null, "to": str}],
             "insufficient_reasons": [closed set]},
   "heirs": {"status": "ok"|"missing_found"|"insufficient",
             "rows": [{"record_id", "name", "has_current_koseki"}],
             "insufficient_reasons": [closed set]}}

切れ目判定の規則（決定的）:
- 対象は「被相続人の戸籍」＝読解JSON の 人物[].氏名 または 戸籍.筆頭者 に
  被相続人名（App34 被相続人フラグ=yes・正規化一致）が現れる戸籍。
- 各戸籍の被覆区間 = [編製日, 消除日]（消除日なし＝現在まで被覆）。
- 編製日昇順に前線（frontier）を進め、次の編製日が前線より 1 日超 先なら
  切れ目（between）。消除日なしの戸籍以降は切れ目なし（無限被覆）。
- 生年月日_西暦があり最初の編製日より前なら birth_to_first の切れ目。

判定不能の分類（INSUFFICIENT_REASONS・「不足」と断定しない）:
- no_kosekis:              案件に（被相続人の）戸籍がない
- decedent_unknown:        被相続人を特定できない（App34 被相続人フラグなし）
- decedent_birth_unknown:  被相続人の生年月日_西暦がない（出生カバー判定不能）
- unparseable_dates:       編製日（西暦）の無い戸籍がある（西暦変換不能含む・
                           undated_koseki_ids に列挙）
- shubetsu_unset:          戸籍種別が未設定の戸籍がある（現行判定に使えない）
- heirs_unregistered:      App36 相続人行がない（判定不能を fail-closed で明示）

規律: read-only（P4 系 checker 適用・書込み API 呼出しなし）。App34/App36 の
読取は dashboard の filter 経由 loader を共用（reader manifest 閉包の維持）。
"""

import json
import unicodedata
from datetime import date, timedelta

from hub import kintone
from hub import webapp_souzoku_dashboard as souzoku_dash
from kinship_graph import APP_KOSEKI_BOOK

INSUFFICIENT_REASONS = ("no_kosekis", "decedent_unknown",
                        "decedent_birth_unknown", "unparseable_dates",
                        "shubetsu_unset", "heirs_unregistered")
CHAIN_STATUSES = ("ok", "gaps_found", "insufficient")
HEIRS_STATUSES = ("ok", "missing_found", "insufficient")

_KOSEKI_FIELDS = ["$id", "戸籍種別", "編製日", "消除日", "読解JSON",
                  "Drive_fileId"]
_FAR_FUTURE = date(9999, 12, 31)


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "")


def _norm(s) -> str:
    return (unicodedata.normalize("NFKC", str(s or ""))
            .replace(" ", "").replace("　", ""))


def _iso(s):
    try:
        y, m, d = (int(x) for x in str(s).split("-"))
        return date(y, m, d)
    except (ValueError, AttributeError):
        return None


def _names_in_reading(reading: dict) -> set:
    """読解JSON に現れる人物名の集合（人物[].氏名＋戸籍.筆頭者・正規化）。"""
    names = set()
    if not isinstance(reading, dict):
        return names
    koseki = reading.get("戸籍")
    if isinstance(koseki, dict):
        names.add(_norm(koseki.get("筆頭者")))
    for person in (reading.get("人物") or []):
        if isinstance(person, dict):
            names.add(_norm(person.get("氏名")))
    names.discard("")
    return names


def _birth_of(reading: dict, name_norm: str):
    """読解JSON から指定人物の 生年月日_西暦 を引く（無ければ None）。"""
    if not isinstance(reading, dict):
        return None
    for person in (reading.get("人物") or []):
        if isinstance(person, dict) and _norm(person.get("氏名")) == name_norm:
            b = _iso(person.get("生年月日_西暦"))
            if b is not None:
                return b
    return None


async def check_coverage(case_record_id: str) -> dict:
    rid = str(case_record_id or "")
    kosekis = await kintone.search_records(
        APP_KOSEKI_BOOK,
        f'案件レコードID = "{rid}" order by $id asc limit 100',
        fields=_KOSEKI_FIELDS)

    # ── 被相続人の特定（App34・filter 経由 loader 共用） ─────────────────────
    persons = await souzoku_dash._load_persons(rid)
    decedent_norm = ""
    for p in persons.get("records") or []:
        if _v(p, "被相続人フラグ") == "yes" and _v(p, "氏名"):
            decedent_norm = _norm(_v(p, "氏名"))
            break

    rows = []
    undated_decedent_ids = []
    shubetsu_unset = False
    decedent_birth = None
    intervals = []
    for rec in kosekis:
        try:
            reading = json.loads(_v(rec, "読解JSON") or "{}")
        except json.JSONDecodeError:
            reading = {}
        names = _names_in_reading(reading)
        belongs = bool(decedent_norm) and decedent_norm in names
        shubetsu = _v(rec, "戸籍種別")
        if not shubetsu:
            shubetsu_unset = True
        hensei = _iso(_v(rec, "編製日"))
        shojo = _iso(_v(rec, "消除日"))
        rows.append({"record_id": _v(rec, "$id"),
                     "shubetsu": shubetsu,
                     "hensei": _v(rec, "編製日") or None,
                     "shojo": _v(rec, "消除日") or None,
                     "belongs_to_decedent": belongs,
                     "drive_file_id": _v(rec, "Drive_fileId") or None,
                     "_names": names})
        if belongs:
            if decedent_birth is None:
                decedent_birth = _birth_of(reading, decedent_norm)
            if hensei is None:
                undated_decedent_ids.append(_v(rec, "$id"))
            else:
                intervals.append((hensei, shojo))

    # ── (i) 連続性判定 ───────────────────────────────────────────────────────
    chain_reasons = []
    gaps = []
    if not decedent_norm:
        chain_reasons.append("decedent_unknown")
    decedent_rows = [r for r in rows if r["belongs_to_decedent"]]
    if decedent_norm and not decedent_rows:
        chain_reasons.append("no_kosekis")
    if undated_decedent_ids:
        chain_reasons.append("unparseable_dates")
    if shubetsu_unset:
        chain_reasons.append("shubetsu_unset")
    if decedent_norm and decedent_rows and decedent_birth is None:
        chain_reasons.append("decedent_birth_unknown")

    intervals.sort(key=lambda t: t[0])
    if intervals:
        first_start = intervals[0][0]
        if decedent_birth is not None and first_start > decedent_birth:
            gaps.append({"kind": "birth_to_first",
                         "from": decedent_birth.isoformat(),
                         "to": first_start.isoformat()})
        frontier = None                      # 被覆前線（None=未開始）
        for start, end in intervals:
            if frontier is not None and frontier != _FAR_FUTURE \
                    and (start - frontier) > timedelta(days=1):
                gaps.append({"kind": "between",
                             "from": frontier.isoformat(),
                             "to": start.isoformat()})
            this_end = end if end is not None else _FAR_FUTURE
            frontier = this_end if frontier is None \
                else max(frontier, this_end)

    if not decedent_norm or not intervals:
        chain_status = "insufficient"
    elif gaps:
        chain_status = "gaps_found"
    else:
        chain_status = "ok"

    # ── (ii) 相続人の現在戸籍 ────────────────────────────────────────────────
    heirs_reasons = []
    heirs_data = await souzoku_dash._load_heirs(rid)
    heir_rows = []
    missing = False
    for h in heirs_data.get("records") or []:
        name_norm = _norm(_v(h, "氏名"))
        has_current = any(
            r["shubetsu"] == "現行" and name_norm and name_norm in r["_names"]
            for r in rows)
        if not has_current:
            missing = True
        heir_rows.append({"record_id": _v(h, "$id"),
                          "name": _v(h, "氏名"),
                          "has_current_koseki": has_current})
    if not heir_rows:
        heirs_reasons.append("heirs_unregistered")
        heirs_status = "insufficient"       # fail-closed の明示（断定しない）
    elif missing:
        heirs_status = "missing_found"
        if shubetsu_unset:
            heirs_reasons.append("shubetsu_unset")
    else:
        heirs_status = "ok"

    for r in rows:
        del r["_names"]                     # 応答 grammar 外の作業用キーを除去
    return {
        "case_record_id": rid,
        "estimate": True,
        "decedent": {"registered": bool(decedent_norm),
                     "birth_seireki": (decedent_birth.isoformat()
                                       if decedent_birth else None)},
        "chain": {"status": chain_status, "kosekis": rows, "gaps": gaps,
                  "undated_koseki_ids": undated_decedent_ids,
                  "insufficient_reasons": chain_reasons},
        "heirs": {"status": heirs_status, "rows": heir_rows,
                  "insufficient_reasons": heirs_reasons},
    }
