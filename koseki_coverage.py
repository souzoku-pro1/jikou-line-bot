"""koseki_coverage — KOSEKI-CHECK-1: 戸籍不足チェック（決定的検査・read-only）

Q-KOSEKI-SURVEY 推奨案(a) の具体化＋fix1（R-KOSEKI-CHECK-1 全5所見）の
fail-closed 完成形。指定案件について
  (i)  被相続人の戸籍連続性（編製日/消除日・西暦を時系列に並べ切れ目を検出）
  (ii) 相続人ごとの「現在戸籍（戸籍種別=現行）」の有無
を機械判定する。**参考見立てであり不足の確定はしない**（機械は確定しない・
表示側で定型注記が必ず付く＝webapp_q の FLAG_NOTES）。

出力 grammar（閉集合・自由文なし・ocr_text は返さない・50KB 上限内の subset）:
  {"case_record_id": str,
   "estimate": true,
   "decedent": {"registered": bool, "birth_seireki": str|null},
   "persons_consulted": [App34 record_id],   # fix1(03) 検査に使用した人物行
   "chain": {"status": "ok"|"gaps_found"|"insufficient",
             "kosekis": [{"record_id","shubetsu","hensei","shojo",
                          "belongs_to_decedent","drive_file_id"}],
             "gaps": [{"kind": "birth_to_first"|"between",
                       "from": str|null, "to": str}],
             "undated_koseki_ids": [...],
             "insufficient_reasons": [closed set]},
   "heirs": {"status": "ok"|"missing_found"|"insufficient",
             "rows": [{"record_id","name","has_current_koseki": bool|null}],
             "insufficient_reasons": [closed set]}}

切れ目判定の規則（決定的・被覆前線方式）:
- 対象は「被相続人の戸籍」＝読解JSON の 人物[].氏名/戸籍.筆頭者 に被相続人名
  （正規化一致）が現れる戸籍。
- 各戸籍の被覆区間 = [編製日, 消除日]（消除日 空=現在まで被覆）。
- 編製日昇順に前線を進め、次の編製日が前線より 1 日超 先なら between の切れ目。
- 生年月日_西暦があり最初の編製日より前なら birth_to_first。

fix1(01) status 連動（fail-closed）: 被覆判定の完全性を損なう理由
（unparseable_dates・inverted_interval・shubetsu_unset・fetch_incomplete・
decedent_unknown・decedent_ambiguous・no_kosekis）が 1 つでもあれば当該判定面
は insufficient とし、**gaps/missing を確定結果として返さない**（gaps=[]・
has_current_koseki=null）。例外（裁定）: decedent_birth_unknown **のみ**の
場合は birth_to_first 判定だけを不能とし、between の切れ目はデータが完全なら
返す（reasons で明示）。

判定不能の分類（INSUFFICIENT_REASONS・「不足」と断定しない・fix1 で拡張）:
- no_kosekis:              案件に（被相続人の）戸籍がない
- decedent_unknown:        被相続人を特定できない（フラグ=yes が 0 件）
- decedent_ambiguous:      fix1(04) 被相続人候補が複数（フラグ=yes 2件以上・
                           または正規化後同名の別人物がいて名寄せ不能）
- decedent_birth_unknown:  生年月日_西暦がない（birth_to_first のみ判定不能）
- unparseable_dates:       編製日が無い/非空でも西暦解釈不能の戸籍がある
                           （fix1(02)・undated_koseki_ids に列挙）
- inverted_interval:       fix1(02) 消除日<編製日 の逆転区間がある
- shubetsu_unset:          戸籍種別が未設定の戸籍がある
- heirs_unregistered:      App36 相続人行がない
- fetch_incomplete:        fix1(05) App33 の全件取得が完了しなかった
                           （cap 到達・カーソル異常＝部分データで判定しない）

fix1(05) App33 取得: $id 厳密単調増加カーソルで全件取得（PWA-02 の確立
パターン踏襲）。重複/逆行/非数字・ページ上限到達は fail-closed。

規律: read-only（P4 系 checker 適用）。App34/App36 は dashboard の filter 経由
loader を共用（reader manifest 閉包の維持）。
"""

import json
import unicodedata
from datetime import date, timedelta

from hub import kintone
from hub import webapp_souzoku_dashboard as souzoku_dash
from kinship_graph import APP_KOSEKI_BOOK

INSUFFICIENT_REASONS = ("no_kosekis", "decedent_unknown",
                        "decedent_ambiguous", "decedent_birth_unknown",
                        "unparseable_dates", "inverted_interval",
                        "shubetsu_unset", "heirs_unregistered",
                        "fetch_incomplete")
CHAIN_STATUSES = ("ok", "gaps_found", "insufficient")
HEIRS_STATUSES = ("ok", "missing_found", "insufficient")
# fix1(01): birth 不明**のみ**は between 判定を続行できる（裁定）。それ以外は
# 当該判定面を insufficient に固定する完全性阻害理由
_CHAIN_BLOCKING = frozenset(INSUFFICIENT_REASONS) - {"decedent_birth_unknown",
                                                     "heirs_unregistered"}

_KOSEKI_FIELDS = ["$id", "戸籍種別", "編製日", "消除日", "読解JSON",
                  "Drive_fileId"]
_FAR_FUTURE = date(9999, 12, 31)
_PAGE_LIMIT = 100
_MAX_PAGES = 100


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


async def _fetch_all_kosekis(rid: str):
    """fix1(05): $id 厳密単調増加カーソルで App33 全件取得。
    Returns (records, complete)。重複/逆行/非数字・cap 到達は complete=False
    （部分データで判定しない＝呼び出し側が fetch_incomplete へ倒す）。"""
    out = []
    cursor = 0
    for _ in range(_MAX_PAGES):
        page = await kintone.search_records(
            APP_KOSEKI_BOOK,
            f'案件レコードID = "{rid}" and $id > {cursor} '
            f"order by $id asc limit {_PAGE_LIMIT}",
            fields=_KOSEKI_FIELDS)
        if not page:
            return out, True
        for rec in page:
            rid_s = _v(rec, "$id")
            if not rid_s.isdigit() or int(rid_s) <= cursor:
                return out, False            # 非数字・重複・逆行 = fail-closed
            cursor = int(rid_s)
            out.append(rec)
        if len(page) < _PAGE_LIMIT:
            return out, True
    return out, False                        # ページ上限到達 = 完全性保証なし


def _names_in_reading(reading: dict) -> set:
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
    if not isinstance(reading, dict):
        return None
    for person in (reading.get("人物") or []):
        if isinstance(person, dict) and _norm(person.get("氏名")) == name_norm:
            b = _iso(person.get("生年月日_西暦"))
            if b is not None:
                return b
    return None


def _identify_decedent(person_rows: list):
    """fix1(04): 被相続人同定の fail-closed。
    Returns (decedent_norm, reason|None)。
    - フラグ=yes（氏名あり）0 件 → ("", "decedent_unknown")
    - 2 件以上 → ("", "decedent_ambiguous")
    - 1 件でも、正規化後同名の**別の**App34 人物が存在すれば名寄せ不能
      → ("", "decedent_ambiguous")（先頭採用はしない）"""
    candidates = [p for p in person_rows
                  if _v(p, "被相続人フラグ") == "yes" and _v(p, "氏名")]
    if not candidates:
        return "", "decedent_unknown"
    if len(candidates) > 1:
        return "", "decedent_ambiguous"
    decedent = candidates[0]
    dec_norm = _norm(_v(decedent, "氏名"))
    dec_id = _v(decedent, "$id")
    for p in person_rows:
        if _v(p, "$id") != dec_id and _norm(_v(p, "氏名")) == dec_norm:
            return "", "decedent_ambiguous"
    return dec_norm, None


async def check_coverage(case_record_id: str) -> dict:
    rid = str(case_record_id or "")
    kosekis, fetch_complete = await _fetch_all_kosekis(rid)

    # ── 被相続人の特定（App34・filter 経由 loader 共用・fix1(04)） ───────────
    persons = await souzoku_dash._load_persons(rid)
    person_rows = persons.get("records") or []
    persons_consulted = [_v(p, "$id") for p in person_rows if _v(p, "$id")]
    decedent_norm, decedent_reason = _identify_decedent(person_rows)

    rows = []
    undated_decedent_ids = []
    inverted = False
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
        hensei_raw = _v(rec, "編製日")
        shojo_raw = _v(rec, "消除日")
        hensei = _iso(hensei_raw) if hensei_raw else None
        # fix1(02) 消除日の三値化: 空=無限被覆／非空で解釈不能=判定不能／
        # 逆転（消除日<編製日）=判定不能
        shojo = _iso(shojo_raw) if shojo_raw else None
        rows.append({"record_id": _v(rec, "$id"),
                     "shubetsu": shubetsu,
                     "hensei": hensei_raw or None,
                     "shojo": shojo_raw or None,
                     "belongs_to_decedent": belongs,
                     "drive_file_id": _v(rec, "Drive_fileId") or None,
                     "_names": names})
        if belongs:
            if decedent_birth is None:
                decedent_birth = _birth_of(reading, decedent_norm)
            if hensei is None or (shojo_raw and shojo is None):
                undated_decedent_ids.append(_v(rec, "$id"))
            elif shojo is not None and shojo < hensei:
                inverted = True
            else:
                intervals.append((hensei, shojo))

    # ── (i) 連続性判定（fix1(01) status 連動） ───────────────────────────────
    chain_reasons = []
    if not fetch_complete:
        chain_reasons.append("fetch_incomplete")
    if decedent_reason:
        chain_reasons.append(decedent_reason)
    decedent_rows = [r for r in rows if r["belongs_to_decedent"]]
    if decedent_norm and not decedent_rows:
        chain_reasons.append("no_kosekis")
    if undated_decedent_ids:
        chain_reasons.append("unparseable_dates")
    if inverted:
        chain_reasons.append("inverted_interval")
    if shubetsu_unset:
        chain_reasons.append("shubetsu_unset")
    if decedent_norm and decedent_rows and decedent_birth is None:
        chain_reasons.append("decedent_birth_unknown")

    blocked = bool(set(chain_reasons) & _CHAIN_BLOCKING) or not intervals
    gaps = []
    if not blocked:
        intervals.sort(key=lambda t: t[0])
        first_start = intervals[0][0]
        if decedent_birth is not None and first_start > decedent_birth:
            gaps.append({"kind": "birth_to_first",
                         "from": decedent_birth.isoformat(),
                         "to": first_start.isoformat()})
        frontier = None
        for start, end in intervals:
            if frontier is not None and frontier != _FAR_FUTURE \
                    and (start - frontier) > timedelta(days=1):
                gaps.append({"kind": "between",
                             "from": frontier.isoformat(),
                             "to": start.isoformat()})
            this_end = end if end is not None else _FAR_FUTURE
            frontier = this_end if frontier is None \
                else max(frontier, this_end)

    if blocked:
        chain_status = "insufficient"        # 確定結果（gaps）は返さない
    elif gaps:
        chain_status = "gaps_found"
    else:
        chain_status = "ok"

    # ── (ii) 相続人の現在戸籍（fix1(01) 非断定化） ───────────────────────────
    heirs_reasons = []
    if not fetch_complete:
        heirs_reasons.append("fetch_incomplete")
    if shubetsu_unset:
        heirs_reasons.append("shubetsu_unset")
    heirs_data = await souzoku_dash._load_heirs(rid)
    heirs_blocked = bool(heirs_reasons)
    heir_rows = []
    missing = False
    for h in heirs_data.get("records") or []:
        name_norm = _norm(_v(h, "氏名"))
        if heirs_blocked:
            has_current = None               # 断定しない（判定不能）
        else:
            has_current = any(
                r["shubetsu"] == "現行" and name_norm
                and name_norm in r["_names"] for r in rows)
            if not has_current:
                missing = True
        heir_rows.append({"record_id": _v(h, "$id"),
                          "name": _v(h, "氏名"),
                          "has_current_koseki": has_current})
    if not heir_rows:
        heirs_reasons.append("heirs_unregistered")
        heirs_status = "insufficient"
    elif heirs_blocked:
        heirs_status = "insufficient"        # missing_found と断定しない
    elif missing:
        heirs_status = "missing_found"
    else:
        heirs_status = "ok"

    for r in rows:
        del r["_names"]
    return {
        "case_record_id": rid,
        "estimate": True,
        "decedent": {"registered": bool(decedent_norm),
                     "birth_seireki": (decedent_birth.isoformat()
                                       if decedent_birth else None)},
        "persons_consulted": persons_consulted,
        "chain": {"status": chain_status, "kosekis": rows, "gaps": gaps,
                  "undated_koseki_ids": undated_decedent_ids,
                  "insufficient_reasons": chain_reasons},
        "heirs": {"status": heirs_status, "rows": heir_rows,
                  "insufficient_reasons": heirs_reasons},
    }
