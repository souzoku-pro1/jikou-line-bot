"""KOSEKI-DATA-1(3): 既存 App33 レコードの再正規化・構造化 field 充足の移行

運用（[人]二段）:
  1. dry-run（既定）  : railway run python koseki_backfill.py
     → 書込みゼロで、各レコードの充足見込み・書込み予定 payload・手作業が
       必要な項目（案件レコードID 欠落等）を表示する
  2. 本適用           : railway run python koseki_backfill.py --apply
     → dry-run と同じ算出結果を実際に update_record する（1 件の失敗は他を
       止めない・失敗一覧を末尾に表示）

規律:
- 再読解はしない（既存 読解JSON からの決定的再正規化のみ＝API コストゼロ・
  AI 出力で人の修正を上書きしない）。既存キーは保持し、西暦キーの充足と
  人物[].生年月日_西暦 の追加のみ行う（koseki_reader.normalize_reading と同一の正）
- **読解状態には一切触れない**（遷移は人手確認フロー=R4 の専権）
- 構造化 field（戸籍種別・編製日・消除日）は厳密検証済みの値のみ・値の無い
  field はキーを含めない（既存値を消さない）
- 出力は $id・案件番号・日付・様式・キー名のみ（氏名・本籍等の値は出さない）
"""

import argparse
import asyncio
import json
import sys
import unicodedata

# Windows コンソール（cp932）でも UnicodeEncodeError で落とさない（表示専用）
try:
    sys.stdout.reconfigure(errors="replace")
except AttributeError:
    pass

try:                              # ローカル実行時の企業 TLS 対策（無ければ素通し）
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

from hub import kintone
from hub import webapp_souzoku_dashboard as souzoku_dash
from koseki_reader import APP_KOSEKI_BOOK, normalize_reading, structured_fields

_FIELDS = ["$id", "案件レコードID", "読解状態", "読解JSON",
           "戸籍種別", "編製日", "消除日"]


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "")


def _norm_name(s) -> str:
    return (unicodedata.normalize("NFKC", str(s or ""))
            .replace(" ", "").replace("　", ""))


async def _case_candidates(reading: dict) -> list[str]:
    """案件レコードID 欠落レコードの手作業支援: 読解JSON 内の筆頭者・人物名を
    各案件の人物（dashboard の filter 経由 loader を共用＝App34 の新規 reader を
    作らない流儀・統合済み無効の除外も同一の正）と正規化一致させ、候補の
    案件番号のみ返す（氏名は出力しない）。"""
    names = set()
    koseki = reading.get("戸籍")
    if isinstance(koseki, dict):
        names.add(_norm_name(koseki.get("筆頭者")))
    for person in (reading.get("人物") or []):
        if isinstance(person, dict):
            names.add(_norm_name(person.get("氏名")))
    names.discard("")
    if not names:
        return []
    case_rows = await kintone.search_records(
        souzoku_dash.APP_SOUZOKU_CASES, "order by $id asc limit 100",
        fields=["$id"])
    cases = set()
    for row in case_rows:
        case_id = _v(row, "$id")
        if not case_id:
            continue
        data = await souzoku_dash._load_persons(case_id)
        for p in data.get("records") or []:
            if _norm_name(_v(p, "氏名")) in names:
                cases.add(case_id)
                break
    return sorted(cases)


async def run(apply: bool) -> int:
    records = await kintone.search_records(
        APP_KOSEKI_BOOK, "order by $id asc limit 500", fields=_FIELDS)
    print(f"App33 対象 {len(records)} 件（mode={'APPLY' if apply else 'DRY-RUN'}）")
    manual: list[str] = []
    failures: list[str] = []
    planned = 0
    for record in records:
        rid = _v(record, "$id")
        case = _v(record, "案件レコードID")
        raw = _v(record, "読解JSON")
        try:
            reading = json.loads(raw or "{}")
        except json.JSONDecodeError:
            manual.append(f"$id={rid}: 読解JSON が解釈不能（再読解が必要）")
            continue
        if not isinstance(reading, dict) or not isinstance(
                reading.get("戸籍"), dict):
            manual.append(f"$id={rid}: 読解JSON に 戸籍 ブロックなし"
                          "（再読解が必要）")
            continue
        before = json.dumps(reading, ensure_ascii=False, sort_keys=True)
        normalized = normalize_reading(json.loads(before))
        after = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
        struct = structured_fields(normalized)
        koseki = normalized.get("戸籍") or {}
        persons = normalized.get("人物") or []
        births = sum(1 for p in persons if isinstance(p, dict)
                     and p.get("生年月日_西暦"))
        print(f"$id={rid} case={case or '欠落'} "
              f"様式={normalized.get('様式')!r} "
              f"編製日_西暦={koseki.get('編製日_西暦')} "
              f"消除日_西暦={koseki.get('消除日_西暦')} "
              f"生年月日_西暦={births}/{len(persons)}人 "
              f"JSON更新={'あり' if after != before else 'なし'} "
              f"kintone書込予定={sorted(struct.keys())}")
        if not case:
            candidates = await _case_candidates(normalized)
            manual.append(
                f"$id={rid}: 案件レコードID が空。kintone で設定要"
                f"（人物名一致からの候補案件: "
                f"{candidates if candidates else '機械推定不能'}）")
        fields = dict(struct)
        if after != before:
            fields["読解JSON"] = json.dumps(normalized, ensure_ascii=False)
        if not fields:
            continue
        planned += 1
        if apply:
            try:
                await kintone.update_record(APP_KOSEKI_BOOK, rid, fields)
            except Exception as e:                     # 1 件の失敗は他を止めない
                failures.append(f"$id={rid}: {type(e).__name__}: {str(e)[:120]}")
    print(f"書込み対象 {planned} 件"
          + ("（適用済み）" if apply else "（dry-run のため書込みゼロ）"))
    if manual:
        print("== 手作業が必要な項目（大野・kintone 上の入力） ==")
        for line in manual:
            print(" -", line)
    if failures:
        print("== 適用失敗（要確認） ==")
        for line in failures:
            print(" -", line)
        return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true",
                        help="実際に App33 へ書き込む（既定は dry-run）")
    args = parser.parse_args()
    sys.exit(asyncio.run(run(apply=args.apply)))
