"""line_log_anonymize — LINE-LOG-1 準備票: 匿名化変換スクリプト（[人]ローカル実行用）。

正本: docs/design-drafts/DRAFT_LINE_QUALITY_IMPROVEMENT.md §4.1（allowlist 固定仕様・
fail-closed・標本設計）＋ DRAFT_LINE_LOG_1_PROCEDURE.md（手順書ドラフト・同票）。

## 運用上の絶対条件（§4.1）
- 実行者は[人]（大野）のみ。**PC-A は raw に接触しない**（本票では合成データ試験のみ）。
- raw export は[人]端末ローカルで処理し**転送しない**。保持は export から絶対上限7日。
- 出力は allowlist フィールドのみ（表に無いフィールドは出力しない）。
- **fail-closed**: 変換失敗・残存検査ヒット・判定不能のスレッドは**出力しない**
  （部分出力禁止・除外件数と理由のみ summary に記録）。

## 入力契約（kintone CSV export・列名固定）
- App28（チャットログ）CSV: 作成日時, line_user_id, role, message, category, auto_sent
- App29（承認キュー）CSV（任意・delivery 判定の突合用）: line_user_id, AI下書き, 送信済み

## delivery 判定（手順書 §5 の固定規則）
assistant 発話 text が (i) PENDING_REPLY 定型 → demoted (ii) 即時定型集合 → immediate
(iii) App29 の送信済み AI下書き と一致 → approved (iv) それ以外 → auto。
（fallback 層(c) は delivery からは判定不能＝手順書 §6 の障害記録突合で[人]が指定）
※定型文言は本スクリプトに複製して埋め込む（本番モジュール非 import）。
  複製の drift は test_line_log_anonymize の同期テストが検知する。
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timedelta

THREAD_GAP_HOURS = 72          # スレッド境界（§4.1 固定・最終発話から72h 無応答で分割）
MAIN_CAP = 10                  # 主要3群 各10本（§4.1）
RARE_MIN = 3                   # 希少3層 各3本（不足は合成で充足・別集計）
TEXT_MAX = 400                 # turn.text 最大長（allowlist 固定仕様）

# ── 定型文言の複製（本番モジュール非 import・同期テストで drift 検知） ──────
PENDING_REPLY_TEXT = (
    "ありがとうございます。内容を確認の上、改めてご連絡いたします。\n"
    "少々お時間をいただく場合がございますが、何卒よろしくお願いいたします。"
)
IMMEDIATE_TEXT_MARKERS = (      # 即時定型 5 種の識別マーカー（各定型に固有の句）
    "放置すると不利益が大きい",       # court_doc_request
    "最適な解決方法は異なります",     # churn_neutral
    "税金や個人の方からの借入れ",     # out_of_scope_debt
    "借金の問題には解決の道があります",  # crisis_support
    "至急、弁護士が内容を確認して",   # urgent_seizure_panic
)

_CATEGORIES = {
    "挨拶・雑談", "手続きの一般的な流れ", "必要書類の案内", "費用の定型案内",
    "進捗の事実回答", "営業案内・アクセス", "時効見立て_条件付き",
    "法的判断・見通し", "費用交渉・減額相談", "クレーム・不満", "解約・辞任関係",
    "緊急対応", "本人確認不能・第三者", "その他判断系",
}

# redaction 写像（許可外の値パターン→カテゴリトークン。禁止列挙は残存検査で二重化）
_REDACTIONS = [
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "<メール>"),
    (re.compile(r"0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}"), "<電話番号>"),
    (re.compile(r"〒?\d{3}-\d{4}"), "<郵便番号>"),
    (re.compile(r"[?&]token=[A-Za-z0-9%_\-]+"), "<token>"),
    (re.compile(r"\d{7,}"), "<数字列>"),
]
# 残存検査（変換後にヒットしたら fail-closed・§4.1 の二段のうち機械側）
_RESIDUE_RES = [
    re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}"),
    re.compile(r"\d{3}-\d{4}"),
    re.compile(r"\d{7,}"),
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+"),
    re.compile(r"(?i)(token|secret|key)\s*[:=]\s*\S{8,}"),
]

_DT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M:%S",
               "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M:%S")


def _parse_dt(raw: str) -> datetime:
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue
    raise ValueError("datetime 形式を解釈できません")


def redact_text(text: str) -> str:
    """パターン redaction（氏名等の非パターン PII は[人]の要旨化工程が担う・手順書 §4）。"""
    for rx, token in _REDACTIONS:
        text = rx.sub(token, text)
    return text


def residue_hits(text: str) -> list[str]:
    return [rx.pattern for rx in _RESIDUE_RES if rx.search(text)]


def split_threads(rows: list[dict]) -> dict[str, list[list[dict]]]:
    """user ごとに時系列へ並べ、72h 無応答で分割（§4.1 スレッド境界・決定的）。"""
    by_user: dict[str, list[dict]] = {}
    for r in rows:
        by_user.setdefault(r["line_user_id"], []).append(r)
    out: dict[str, list[list[dict]]] = {}
    gap = timedelta(hours=THREAD_GAP_HOURS)
    for user, items in by_user.items():
        items.sort(key=lambda r: r["_dt"])
        threads: list[list[dict]] = []
        for r in items:
            if threads and r["_dt"] - threads[-1][-1]["_dt"] < gap:
                threads[-1].append(r)
            else:
                threads.append([r])
        out[user] = threads
    return out


def classify_delivery(text: str, approved_drafts: set[str]) -> str:
    if text == PENDING_REPLY_TEXT:
        return "demoted"
    if any(m in text for m in IMMEDIATE_TEXT_MARKERS):
        return "immediate"
    if text in approved_drafts:
        return "approved"
    return "auto"


def classify_layer(turns: list[dict]) -> str:
    """層割当（§4.1 割当順序＋手順書 §7 の希少層重複計数規則＝優先順位で単一層）。

    希少層優先: (a) silent ＞ (b) immediate（(c) fallback は機械判定不能・手順書 §6 で
    [人]指定のため本関数では割当てない）。残りは主要3群（降格＞承認＞自動）。"""
    deliveries = [t["delivery"] for t in turns if t["role"] == "assistant"]
    if not deliveries:
        return "rare:silent"                     # (a) 無言（assistant 発話ゼロ）
    if "immediate" in deliveries:
        return "rare:immediate"                  # (b) 即時通知発火
    if "demoted" in deliveries:
        return "main:demoted"
    if "approved" in deliveries:
        return "main:approved"
    return "main:auto"


def convert(rows: list[dict], approved_drafts: set[str]) -> dict:
    """スレッド化→dedup（user 最新1本）→層割当→標本抽出→allowlist 変換→残存検査。

    Returns: {"threads": [...], "summary": {...}, "checklist": [...]}
    fail-closed: 除外スレッドは出力に一切含めない（理由と件数のみ summary へ）。"""
    threads_by_user = split_threads(rows)
    candidates = []
    for user, ths in sorted(threads_by_user.items()):
        latest = ths[-1]                         # 同一 user 最新スレッド1本（§4.1）
        candidates.append((user, latest))

    excluded: dict[str, int] = {}
    converted = []
    for user, raw_turns in candidates:
        turns = []
        fail = None
        for r in raw_turns:
            text = redact_text(r["message"])
            if len(text) > TEXT_MAX:
                fail = "text_over_400"           # 要旨化（[人]工程・手順書 §4）が必要
                break
            hits = residue_hits(text)
            if hits:
                fail = "residue_detected"
                break
            turn = {"role": r["role"], "text": text}
            if r["role"] == "assistant":
                if r["category"] not in _CATEGORIES:
                    fail = "category_out_of_enum"
                    break
                turn["category"] = r["category"]
                turn["delivery"] = classify_delivery(text, approved_drafts)
            turns.append(turn)
        if fail:
            excluded[fail] = excluded.get(fail, 0) + 1
            continue                             # fail-closed（部分出力もしない）
        converted.append({"turns": turns, "layer": classify_layer(turns)})

    # 標本抽出: 希少層優先割当→主要3群 cap（§4.1 割当順序）
    picked, main_counts = [], {"main:auto": 0, "main:approved": 0, "main:demoted": 0}
    rare_counts = {"rare:silent": 0, "rare:immediate": 0}
    for th in converted:
        layer = th["layer"]
        if layer.startswith("rare:"):
            rare_counts[layer] = rare_counts.get(layer, 0) + 1
            picked.append(th)
        elif main_counts[layer] < MAIN_CAP:
            main_counts[layer] += 1
            picked.append(th)
    for i, th in enumerate(picked, 1):
        th["thread_id"] = f"C{i:03d}"            # 連番仮名（対応表は生成しない）

    summary = {
        "input_rows": len(rows),
        "users": len(threads_by_user),
        "picked": len(picked),
        "main_counts": main_counts,
        "rare_counts": {**rare_counts,
                        "rare:fallback": "機械判定不能（手順書 §6・[人]指定）"},
        "rare_min_required": RARE_MIN,
        "synthetic_needed": {k: max(0, RARE_MIN - v)
                             for k, v in rare_counts.items()},
        "excluded_fail_closed": excluded,
    }
    checklist = [{"thread_id": th["thread_id"],
                  "残存PII目視確認": "", "確認者": "", "確認日": ""}
                 for th in picked]
    ordered = [{"thread_id": th["thread_id"], "layer": th["layer"],
                "turns": th["turns"]} for th in picked]
    return {"threads": ordered, "summary": summary, "checklist": checklist}


def load_app28_csv(path: str) -> list[dict]:
    rows = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            rows.append({"line_user_id": r["line_user_id"],
                         "role": r["role"], "message": r["message"],
                         "category": r.get("category", ""),
                         "auto_sent": r.get("auto_sent", ""),
                         "_dt": _parse_dt(r["作成日時"])})
    return rows


def load_app29_drafts(path: str | None) -> set[str]:
    if not path:
        return set()
    out = set()
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r.get("送信済み") == "yes":
                out.add(r.get("AI下書き", ""))
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="LINE-LOG-1 匿名化変換（[人]ローカル実行・手順書ドラフト参照）")
    ap.add_argument("--app28-csv", required=True)
    ap.add_argument("--app29-csv", default=None)
    ap.add_argument("--out", required=True, help="匿名化コーパス JSON の出力先")
    ap.add_argument("--checklist-out", required=True, help="目視チェックリスト CSV")
    args = ap.parse_args(argv)

    rows = load_app28_csv(args.app28_csv)
    result = convert(rows, load_app29_drafts(args.app29_csv))
    with open(args.out, "w", encoding="utf-8", newline="\n") as f:
        json.dump({"threads": result["threads"], "summary": result["summary"]},
                  f, ensure_ascii=False, indent=1)
    with open(args.checklist_out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["thread_id", "残存PII目視確認",
                                          "確認者", "確認日"])
        w.writeheader()
        w.writerows(result["checklist"])
    print(json.dumps(result["summary"], ensure_ascii=False))   # 件数のみ（本文なし）
    return 0


if __name__ == "__main__":
    sys.exit(main())
