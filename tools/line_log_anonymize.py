"""line_log_anonymize — LINE-LOG-1 準備票: 匿名化変換（[人]ローカル実行用・fix1）。

正本: docs/design-drafts/DRAFT_LINE_QUALITY_IMPROVEMENT.md §4.1（allowlist 固定仕様・
fail-closed・標本設計）＋ DRAFT_LINE_LOG_1_PROCEDURE.md（実行手順書）。

## 一方向工程（fix1 H04）
1. `convert`  : raw CSV → **中間ファイル**（corpus 本体）＋summary（別ファイル・
                運用メタデータ）。checklist はここでは生成しない。
2. [人]手修正 : 中間ファイル上で要旨化・氏名等の目視除去・fallback 補正 ID の確定。
3. `reverify` : 中間ファイルを**全件再検査**（schema／400字／enum／残存パターン／
                allowlist 外フィールド）。**PASS したときのみ**検証済み最終成果物＋
                checklist を出力（FAIL は非0終了・checklist 生成・引渡し不可）。

## 運用上の絶対条件（§4.1）
- 実行者は[人]のみ。PC-A は raw に接触しない（本票では合成データ試験のみ）。
- raw はローカルのみ・転送しない・export から絶対上限7日。
- corpus 本体は allowlist フィールドのみ（summary は**別ファイル**＝fix1 H02）。
- fail-closed: 変換失敗・enum 外・残存ヒットのスレッドは出力しない（理由別件数のみ）。

## delivery 判定（fix1 M01: 正規化後**全文一致**が一次判定）
実ログの定型は Bot 送信文そのものであるため、空白・改行を正規化した全文一致で
判定する（部分一致・marker 方式は廃止＝通常回答が定型句を含んでも誤判定しない）。
定型全文は本番実物の複製（非 import・同期テストで drift 検知）。

## fallback 補正（fix1 H03・構造化入力）
希少層 (c) は機械判定不能のため、[人]が障害記録と突合した thread_id 一覧を
補正ファイル（JSON list）として `reverify --fallback-ids` へ渡す。層の再適用は
優先順位 a(silent)>b(immediate)>c(fallback) を維持（silent/immediate は不変・
それ以外が rare:fallback へ）し、summary（rare_counts/main_counts/main_shortfall/
synthetic_needed）を再計算する（demoted との二重計上なし）。
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timedelta

THREAD_GAP_HOURS = 72          # スレッド境界（§4.1 固定）
MAIN_CAP = 10                  # 主要3群 各10本（§4.1）
RARE_MIN = 3                   # 希少3層 各3本（不足は合成で充足・別集計）
TEXT_MAX = 400                 # turn.text 最大長（allowlist 固定仕様）

ROLES = ("user", "assistant")
MAIN_LAYERS = ("main:auto", "main:approved", "main:demoted")
RARE_LAYERS = ("rare:silent", "rare:immediate", "rare:fallback")
LAYERS = MAIN_LAYERS + RARE_LAYERS
DELIVERIES = ("auto", "approved", "demoted", "immediate", "silent")

# ── 定型文言の複製（本番モジュール非 import・同期テストで drift 検知） ──────
PENDING_REPLY_TEXT = (
    "ありがとうございます。内容を確認の上、改めてご連絡いたします。\n"
    "少々お時間をいただく場合がございますが、何卒よろしくお願いいたします。"
)
IMMEDIATE_TEXTS = {
    "court_doc_request": (
        "裁判所からの書類は放置すると不利益が大きい場合があります。\n"
        "お手元の書類の全ページを写真に撮って、このLINEに送っていただけますか。\n"
        "優先して確認いたします。"),
    "churn_neutral": (
        "ご事情により最適な解決方法は異なります。"
        "よろしければ状況をもう少しお聞かせください。"),
    "out_of_scope_debt": (
        "税金や個人の方からの借入れについては、内容により対応が異なりますので、"
        "別途個別にご案内いたします。確認の上、改めてご連絡いたします。"),
    "crisis_support": (
        "お辛い状況の中、正直にお話しくださりありがとうございます。"
        "借金の問題には解決の道があります。"
        "弁護士が優先してご連絡しますので、少しだけお待ちください。"),
    "urgent_seizure_panic": (
        "ご不安な状況、承知いたしました。至急、弁護士が内容を確認してご連絡します。"
        "お手元に届いている書類があれば、全ページの写真をこのLINEにお送りください。"),
}

_CATEGORIES = {
    "挨拶・雑談", "手続きの一般的な流れ", "必要書類の案内", "費用の定型案内",
    "進捗の事実回答", "営業案内・アクセス", "時効見立て_条件付き",
    "法的判断・見通し", "費用交渉・減額相談", "クレーム・不満", "解約・辞任関係",
    "緊急対応", "本人確認不能・第三者", "その他判断系",
}

_REDACTIONS = [
    (re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"), "<メール>"),
    (re.compile(r"0\d{1,4}[-\s]?\d{1,4}[-\s]?\d{3,4}"), "<電話番号>"),
    (re.compile(r"〒?\d{3}-\d{4}"), "<郵便番号>"),
    (re.compile(r"[?&]token=[A-Za-z0-9%_\-]+"), "<token>"),
    (re.compile(r"\d{7,}"), "<数字列>"),
]
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


def normalize_text(text: str) -> str:
    """delivery 全文一致用の正規化（fix1 M01: 空白・改行の揺れのみ吸収）。"""
    return " ".join(text.split())


_PENDING_NORM = normalize_text(PENDING_REPLY_TEXT)
_IMMEDIATE_NORMS = {normalize_text(v) for v in IMMEDIATE_TEXTS.values()}


def redact_text(text: str) -> str:
    """パターン redaction（氏名等の非パターン PII は[人]の手修正工程が担う）。"""
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
    """fix1 M01: 正規化後**全文一致**が一次判定（marker 部分一致は廃止）。"""
    norm = normalize_text(text)
    if norm == _PENDING_NORM:
        return "demoted"
    if norm in _IMMEDIATE_NORMS:
        return "immediate"
    if norm in approved_drafts:
        return "approved"
    return "auto"


def classify_layer(turns: list[dict]) -> str:
    """機械の層割当（a>b は機械・c=fallback は補正入力でのみ確定＝H03）。"""
    deliveries = [t["delivery"] for t in turns if t["role"] == "assistant"]
    if not deliveries:
        return "rare:silent"
    if "immediate" in deliveries:
        return "rare:immediate"
    if "demoted" in deliveries:
        return "main:demoted"
    if "approved" in deliveries:
        return "main:approved"
    return "main:auto"


def apply_fallback_corrections(threads: list[dict],
                               fallback_ids: set[str]) -> list[str]:
    """fix1 H03: [人]補正（障害記録突合済み thread_id）を優先順位 a>b>c で再適用。

    rare:silent / rare:immediate は上位層のため不変（二重計上なし）。
    それ以外の補正対象は rare:fallback へ。戻り値=未知 thread_id（警告用）。"""
    known = {t["thread_id"] for t in threads}
    for t in threads:
        if t["thread_id"] in fallback_ids and t["layer"] not in (
                "rare:silent", "rare:immediate"):
            t["layer"] = "rare:fallback"
    return sorted(fallback_ids - known)


def compute_summary(threads: list[dict], excluded: dict,
                    extra: dict | None = None) -> dict:
    """層別件数・不足数の再計算（fix1 H03/M02・fallback 補正後にも同一関数で算出）。"""
    counts: dict[str, int] = {layer: 0 for layer in LAYERS}
    for t in threads:
        counts[t["layer"]] += 1
    main_counts = {k: counts[k] for k in MAIN_LAYERS}
    rare_counts = {k: counts[k] for k in RARE_LAYERS}
    summary = {
        "picked": len(threads),
        "main_counts": main_counts,
        # fix1 M02: 主要3群の不足数（>0 の群があれば G2 停止＝手順書 §2）
        "main_shortfall": {k: max(0, MAIN_CAP - v) for k, v in main_counts.items()},
        "rare_counts": rare_counts,
        "rare_min_required": RARE_MIN,
        "synthetic_needed": {k: max(0, RARE_MIN - v)
                             for k, v in rare_counts.items()},
        "excluded_fail_closed": excluded,
    }
    summary.update(extra or {})
    return summary


def convert(rows: list[dict], approved_drafts: set[str]) -> dict:
    """スレッド化→dedup→検証→層割当→標本抽出（中間成果物・checklist は出さない）。"""
    threads_by_user = split_threads(rows)
    excluded: dict[str, int] = {}
    converted = []
    for user, ths in sorted(threads_by_user.items()):
        raw_turns = ths[-1]                      # 同一 user 最新スレッド1本（§4.1）
        turns = []
        fail = None
        for r in raw_turns:
            if r["role"] not in ROLES:           # fix1 H01: role enum 検証
                fail = "role_out_of_enum"
                break
            text = redact_text(r["message"])
            if len(text) > TEXT_MAX:
                fail = "text_over_400"
                break
            if residue_hits(text):
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

    picked = []
    main_counts = {k: 0 for k in MAIN_LAYERS}
    for th in converted:
        layer = th["layer"]
        if layer.startswith("rare:"):
            picked.append(th)
        elif main_counts[layer] < MAIN_CAP:
            main_counts[layer] += 1
            picked.append(th)
    for i, th in enumerate(picked, 1):
        th["thread_id"] = f"C{i:03d}"            # 連番仮名（対応表は生成しない）

    ordered = [{"thread_id": th["thread_id"], "layer": th["layer"],
                "turns": th["turns"]} for th in picked]
    summary = compute_summary(ordered, excluded,
                              {"input_rows": len(rows),
                               "users": len(threads_by_user),
                               "stage": "convert(中間・fallback 補正前)"})
    return {"threads": ordered, "summary": summary}


# ── reverify（fix1 H04: [人]手修正後の全件再検査・PASS 時のみ最終成果物） ────

_THREAD_KEYS = {"thread_id", "layer", "turns"}
_USER_TURN_KEYS = {"role", "text"}
_ASSISTANT_TURN_KEYS = {"role", "text", "category", "delivery"}


def reverify(doc: dict, fallback_ids: set[str]) -> tuple[list[str], dict | None]:
    """中間ファイルの全件再検査。Returns (errors, final_result|None)。

    検査: allowlist 外フィールド／schema／enum（role/category/delivery/layer）／
    400字／残存パターン。1件でも違反があれば final は生成しない（fail-closed）。"""
    errors: list[str] = []
    threads = doc.get("threads")
    if not isinstance(threads, list):
        return ["corpus 形式不正: threads がない"], None
    if set(doc) - {"threads"}:
        errors.append(f"corpus 本体に allowlist 外の最上位キー: {sorted(set(doc) - {'threads'})}")
    seen_ids = set()
    for i, th in enumerate(threads):
        tid = th.get("thread_id", f"(index {i})")
        if set(th) != _THREAD_KEYS:
            errors.append(f"{tid}: thread フィールドが allowlist 外/不足")
            continue
        if tid in seen_ids:
            errors.append(f"{tid}: thread_id 重複")
        seen_ids.add(tid)
        if th["layer"] not in LAYERS:
            errors.append(f"{tid}: layer が enum 外")
        for j, turn in enumerate(th["turns"]):
            where = f"{tid}.turns[{j}]"
            role = turn.get("role")
            if role == "user":
                if set(turn) != _USER_TURN_KEYS:
                    errors.append(f"{where}: user turn フィールド不一致")
            elif role == "assistant":
                if set(turn) != _ASSISTANT_TURN_KEYS:
                    errors.append(f"{where}: assistant turn フィールド不一致")
                elif turn["category"] not in _CATEGORIES:
                    errors.append(f"{where}: category が enum 外")
                elif turn["delivery"] not in DELIVERIES:
                    errors.append(f"{where}: delivery が enum 外")
            else:
                errors.append(f"{where}: role が enum 外")
                continue
            text = turn.get("text", "")
            if not isinstance(text, str) or not text:
                errors.append(f"{where}: text 不正")
            elif len(text) > TEXT_MAX:
                errors.append(f"{where}: 400 字超")
            elif residue_hits(text):
                errors.append(f"{where}: 残存パターン検出")
    if errors:
        return errors, None
    unknown = apply_fallback_corrections(threads, fallback_ids)
    if unknown:
        return [f"fallback 補正の thread_id が corpus に無い: {unknown}"], None
    summary = compute_summary(threads, {}, {"stage": "reverify(検証済み最終)"})
    checklist = [{"thread_id": th["thread_id"],
                  "残存PII目視確認": "", "確認者": "", "確認日": ""}
                 for th in threads]
    return [], {"threads": threads, "summary": summary, "checklist": checklist}


# ── CLI ──────────────────────────────────────────────────────────────────────

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
                out.add(normalize_text(r.get("AI下書き", "")))
    return out


def _write_json(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        json.dump(obj, f, ensure_ascii=False, indent=1)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="LINE-LOG-1 匿名化変換（[人]ローカル実行・一方向工程）")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("convert", help="raw CSV→中間 corpus＋summary（別ファイル）")
    c.add_argument("--app28-csv", required=True)
    c.add_argument("--app29-csv", default=None)
    c.add_argument("--out", required=True, help="中間 corpus JSON（threads のみ）")
    c.add_argument("--summary-out", required=True, help="summary JSON（別ファイル）")
    v = sub.add_parser("reverify",
                       help="[人]手修正済み中間ファイルの全件再検査→最終成果物")
    v.add_argument("--in", dest="infile", required=True)
    v.add_argument("--fallback-ids", default=None,
                   help="fallback 補正 thread_id の JSON list（H03・[人]突合済み）")
    v.add_argument("--out", required=True, help="検証済み最終 corpus JSON")
    v.add_argument("--summary-out", required=True)
    v.add_argument("--checklist-out", required=True)
    args = ap.parse_args(argv)

    if args.cmd == "convert":
        rows = load_app28_csv(args.app28_csv)
        result = convert(rows, load_app29_drafts(args.app29_csv))
        _write_json(args.out, {"threads": result["threads"]})   # allowlist のみ（H02）
        _write_json(args.summary_out, result["summary"])        # 運用メタは別ファイル
        sys.stdout.write(
            json.dumps(result["summary"], ensure_ascii=False) + "\n")
        return 0

    with open(args.infile, encoding="utf-8") as f:
        doc = json.load(f)
    fallback_ids: set[str] = set()
    if args.fallback_ids:
        with open(args.fallback_ids, encoding="utf-8") as f:
            fallback_ids = set(json.load(f))
    errors, final = reverify(doc, fallback_ids)
    if errors:
        sys.stdout.write(json.dumps({"result": "FAIL", "errors": errors[:50]},
                                    ensure_ascii=False) + "\n")
        return 1                                 # PASS 前は checklist 生成・引渡し不可
    _write_json(args.out, {"threads": final["threads"]})
    _write_json(args.summary_out, final["summary"])
    with open(args.checklist_out, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["thread_id", "残存PII目視確認",
                                          "確認者", "確認日"])
        w.writeheader()
        w.writerows(final["checklist"])
    sys.stdout.write(json.dumps({"result": "PASS", **final["summary"]},
                                ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
