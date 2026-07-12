#!/usr/bin/env python3
"""市区町村マスタ（App 31）初期データ投入スクリプト（T3-1）

データ源（設計 docs/architecture/02 §3）:
  総務省「全国地方公共団体コード」一覧（約1,741団体）
  https://www.soumu.go.jp/denshijiti/code.html から最新の一覧をダウンロードし、
  CSV（Excel の場合は CSV に保存し直す）で本スクリプトに渡す。
  必要な列: 団体コード / 都道府県名（漢字） / 市区町村名（漢字）
  ※ 市区町村名が空の行（都道府県そのもの）は投入対象外として自動スキップ

投入内容: 団体コード・都道府県・市区町村名・有効=yes のみ。
  住所・郵便番号・担当部署・手数料は「使った自治体から実測で登録」する運用
  （手数料の初期値 450/750/300/300 は App 31 のフィールド初期値が適用される）

実行（二段階）:
  1. dry-run（既定）:  railway run python import_city_master.py <csvファイル>
     → 件数検証・サンプル・既存との差分を表示するだけ。書き込みしない
  2. 本実行:           railway run python import_city_master.py <csvファイル> --execute
     → 既存の団体コードをスキップして一括登録（100件チャンク・再実行安全）
"""

import argparse
import asyncio
import csv
import io
import logging
import sys

from hub.redact import emit

logger = logging.getLogger("import_city_master")

# ── パース・検証（テスト対象の純粋関数） ─────────────────────────────────────

EXPECTED_MIN, EXPECTED_MAX = 1500, 2000  # 設計上の想定 約1,741団体


def read_csv_text(path: str) -> str:
    raw = open(path, "rb").read()
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    raise SystemExit("CSV の文字コードを判定できません（UTF-8 か CP932 で保存してください）")


def _find_col(header: list[str], *keywords: str) -> int:
    for i, name in enumerate(header):
        n = name.replace("\n", "").replace("（", "(").replace("）", ")")
        if all(k in n for k in keywords):
            return i
    raise SystemExit(f"CSV に列が見つかりません: {keywords}（ヘッダ: {header}）")


def parse_rows(csv_text: str) -> list[dict]:
    """CSV → [{団体コード, 都道府県, 市区町村名}]。都道府県行（市区町村名なし）は除外"""
    reader = csv.reader(io.StringIO(csv_text))
    header = next(reader)
    i_code = _find_col(header, "団体コード")
    i_pref = _find_col(header, "都道府県", "漢字")
    i_city = _find_col(header, "市区町村", "漢字")

    rows = []
    for r in reader:
        if len(r) <= max(i_code, i_pref, i_city):
            continue
        code = r[i_code].strip()
        pref = r[i_pref].strip()
        city = r[i_city].strip()
        if not city:            # 都道府県そのものの行は対象外
            continue
        rows.append({"団体コード": code, "都道府県": pref, "市区町村名": city})
    return rows


def validate_rows(rows: list[dict]) -> list[str]:
    """件数・コード形式・重複の検証。問題の一覧を返す（空=OK）"""
    problems = []
    if not (EXPECTED_MIN <= len(rows) <= EXPECTED_MAX):
        problems.append(
            f"件数が想定範囲外: {len(rows)}件（想定 {EXPECTED_MIN}〜{EXPECTED_MAX}・約1,741）")
    seen = set()
    for row in rows:
        code = row["団体コード"]
        if not (code.isdigit() and len(code) == 6):
            problems.append(f"団体コードが6桁数字でない: {code!r}（{row['市区町村名']}）")
        if code in seen:
            problems.append(f"団体コードが重複: {code}（{row['市区町村名']}）")
        seen.add(code)
        if not row["都道府県"]:
            problems.append(f"都道府県が空: {code} {row['市区町村名']}")
    return problems


def plan_insert(rows: list[dict], existing_codes: set[str]) -> tuple[list[dict], int]:
    """既存の団体コードを除いた投入対象を返す（再実行安全）"""
    to_insert = [r for r in rows if r["団体コード"] not in existing_codes]
    return to_insert, len(rows) - len(to_insert)


# ── kintone I/O（hub/kintone 経由） ──────────────────────────────────────────

async def fetch_existing_codes(app) -> set[str]:
    from hub import kintone
    codes: set[str] = set()
    offset = 0
    while True:
        records = await kintone.search_records(
            app, f"order by 団体コード asc limit 500 offset {offset}",
            fields=["団体コード"])
        codes.update(r.get("団体コード", {}).get("value", "") for r in records)
        if len(records) < 500:
            return codes
        offset += 500


async def insert_rows(app, rows: list[dict]) -> int:
    from hub import kintone
    payload = [{**r, "有効": "yes"} for r in rows]
    ids = await kintone.create_records(app, payload)
    return len(ids)


# ── メイン ───────────────────────────────────────────────────────────────────

async def amain() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("csv_path", help="総務省 全国地方公共団体コード一覧の CSV")
    ap.add_argument("--execute", action="store_true",
                    help="本実行（省略時は dry-run。dry-run の結果を確認・承認してから付ける）")
    args = ap.parse_args()

    from hub import kintone
    app = kintone.KintoneApp("App 31 (市区町村マスタ)", "APP_CITY_MASTER", "TOKEN_CITY_MASTER")

    rows = parse_rows(read_csv_text(args.csv_path))
    problems = validate_rows(rows)

    logger.info("パース結果      : %s件（市区町村。都道府県行は除外済み）",
                emit(len(rows), "count", "log", "operator"))
    if problems:
        logger.info("検証            : ★問題 %s件",
                    emit(len(problems), "count", "log", "operator"))
    else:
        logger.info("検証            : OK")
    for p in problems[:20]:
        logger.info("  - %s", emit(p, "freetext", "log", "operator"))
    if problems:
        logger.info("検証 NG のため中止します（CSV を確認してください）")
        return 1

    existing = await fetch_existing_codes(app)
    to_insert, skipped = plan_insert(rows, existing)
    logger.info("kintone 既存    : %s件 / 今回スキップ（登録済み）: %s件",
                emit(len(existing), "count", "log", "operator"),
                emit(skipped, "count", "log", "operator"))
    logger.info("投入対象        : %s件", emit(len(to_insert), "count", "log", "operator"))
    logger.info("サンプル（先頭5件）:")
    for r in to_insert[:5]:
        logger.info("  %s %s %s",
                    emit(r['団体コード'], "record_id", "log", "operator"),
                    emit(r['都道府県'], "freetext", "log", "operator"),
                    emit(r['市区町村名'], "freetext", "log", "operator"))

    if not args.execute:
        logger.info("\n[dry-run] 書き込みは行っていません。内容を確認のうえ --execute で本実行してください。")
        return 0

    if not to_insert:
        logger.info("投入対象がありません（すべて登録済み）")
        return 0
    n = await insert_rows(app, to_insert)
    logger.info("\n本実行完了: %s件を登録しました", emit(n, "count", "log", "operator"))
    return 0


if __name__ == "__main__":
    try:
        import truststore
        truststore.inject_into_ssl()
    except ImportError:
        pass
    sys.exit(asyncio.run(amain()))
