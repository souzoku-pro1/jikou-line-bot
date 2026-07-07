"""人物の名寄せ候補検出スコアラー＋候補起票（R4-2a）

設計: docs/koseki-ocr/02 §4・07 R4・2026-07-07 R4-2a 裁定
- App 34 全人物を対象に候補ペアを検出し、App 30 封筒（トップキー=person_merge）へ
  起票するまで。**統合実行・Bot語彙・敗者削除・監査JSONは R4-2b（本スコープ外）**
- シグナル5種:
  ①正規化氏名一致（NFKC 全半角統一＋空白除去後の完全一致）
  ②氏補完後一致（名のみ人物に「その戸籍在籍時の氏」＝登場戸籍の筆頭者由来の姓を
    補完した上での一致。在籍時の氏であり現氏の断定ではない）
  ③生年月日一致（和暦原文の正規化比較）
  ④婚姻相互リンク（**同一の相手方**に互換日付で婚姻している2レコード。
    互いを相手方とする2レコードは配偶者ペアであり同一人ではない——除外）
  ⑤従前戸籍チェーン（従前戸籍の本籍・筆頭者の連結。**注記シグナルのみ**＝
    成立条件には入らない）
- 自動候補の成立条件: ①+③ / ②+③ / ④ のいずれか。**単独一致は候補にしない**
- 案件参照が異なるペア = 統合保留（起票はするが保留フラグつき・自動候補にしない）
- 氏のみ筆頭者は人物としてスコアリング対象にしない（独立ノード化しない原則。
  参照解決は Z 系管轄で本スコープ外）

機械遷移の制約（裁定1・最重要）:
- 機械が書けるのは (a) 名寄せ確定の「未確定→自動候補」への遷移 と
  (b) App 30 封筒への候補ペア記録 の2つ**のみ**。
  「確定」への機械遷移はこのモジュールに存在しない（静的テストで固定）。
  確認済み系・生死区分・案件参照・氏名への書き込みはゼロ

実行形態: 手動起動関数 detect_merge_candidates() のみ（自動トリガー結線なし・
定期実行/ingest後フックは R4-2b 以降の裁定事項）。
env PERSON_MERGE_ENABLED=1 のときのみ動く（既定無効・無効時は検出も起票もしない）。
"""

import json
import os
import re
import unicodedata
from dataclasses import dataclass, field

from hub import kintone

APP_KOSEKI_PERSON = kintone.KintoneApp(
    "App 34 (人物)", "APP_KOSEKI_PERSON", "TOKEN_KOSEKI_PERSON")
APP_KOSEKI_BOOK = kintone.KintoneApp(
    "App 33 (戸籍読解)", "APP_KOSEKI_BOOK", "TOKEN_KOSEKI_BOOK")
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

UNIT = "相続一般"

SIGNAL_NAME = "①正規化氏名一致"
SIGNAL_NAME_COMPLETED = "②氏補完後一致"
SIGNAL_BIRTH = "③生年月日一致"
SIGNAL_MARRIAGE = "④婚姻相互リンク"
SIGNAL_CHAIN = "⑤従前戸籍チェーン"


def merge_enabled() -> bool:
    return os.environ.get("PERSON_MERGE_ENABLED") == "1"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _norm(text: str) -> str:
    """正規化: NFKC（全半角統一）＋空白（全角含む）除去"""
    s = unicodedata.normalize("NFKC", text or "")
    return re.sub(r"[\s　]+", "", s)


def _date_compatible(a: str, b: str) -> bool:
    """和暦日付の互換比較。正規化後の完全一致、または一方が日未満の粒度
    （「日」を含まない）でもう一方の前方部分に一致する場合に互換とみなす。
    例: 平成11年7月 ⇔ 平成11年7月19日 は互換。
    「日」を含む同士の前方一致（平成11年7月1日 vs 平成11年7月19日）は
    別日のため互換にしない（安全側）"""
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    short, long_ = (na, nb) if len(na) <= len(nb) else (nb, na)
    return "日" not in short and long_.startswith(short)


@dataclass
class PersonView:
    """スコアリング用の人物ビュー（App 34 レコードからの抽出・純データ）"""
    record_id: str
    name: str
    norm_name: str
    birth: str = ""                       # 出生行の和暦（正規化済み）
    case_id: str = ""
    meyose: str = ""
    koseki_ids: list[str] = field(default_factory=list)
    marriages: list[tuple[str, str]] = field(default_factory=list)  # (相手方norm, 日付raw)
    completed_names: list[str] = field(default_factory=list)  # ②氏補完後の候補名


def _koseki_info(koseki_records: list[dict]) -> dict:
    """App 33 → {id: {"筆頭者", "本籍", "従前本籍", "従前筆頭者"}}（読解JSONから）"""
    info = {}
    for record in koseki_records:
        try:
            reading = json.loads(_v(record, "読解JSON") or "{}")
        except json.JSONDecodeError:
            reading = {}
        koseki = reading.get("戸籍") or {}
        juzen = koseki.get("従前戸籍") or {}
        info[_v(record, "$id")] = {
            "筆頭者": str(koseki.get("筆頭者") or ""),
            "本籍": str(koseki.get("本籍") or ""),
            "従前本籍": str(juzen.get("本籍") or ""),
            "従前筆頭者": str(juzen.get("筆頭者") or ""),
        }
    return info


def _family_name_candidates(hittousha: str) -> list[str]:
    """戸籍の氏の候補（②氏補完用）: 筆頭者に空白があれば先頭トークン・
    なければ筆頭者全体（氏のみ記載のケース）"""
    h = (hittousha or "").strip()
    if not h:
        return []
    if re.search(r"[\s　]", h):
        # 空白あり = 氏名並記（例: 鈴木 誠）→ 先頭トークンが氏
        return [_norm(h.split()[0])]
    # 空白なし = 氏のみ記載（例: 鈴木）→ 全体を氏として扱う
    return [_norm(h)]


def build_views(person_records: list[dict],
                koseki_records: list[dict]) -> list[PersonView]:
    """App 34/33 レコード群 → PersonView（純関数）"""
    koseki_info = _koseki_info(koseki_records)
    views = []
    for record in person_records:
        name = _v(record, "氏名")
        if not _norm(name):
            continue  # 氏名なしはスコアリング対象外
        birth = ""
        marriages = []
        for row in (record.get("身分事項") or {}).get("value") or []:
            value = row.get("value") or {}
            kind = str((value.get("事項種別") or {}).get("value") or "")
            date = str((value.get("年月日") or {}).get("value") or "")
            partner = str((value.get("相手方") or {}).get("value") or "")
            if kind == "出生" and not birth:
                birth = _norm(date)
            if kind == "婚姻" and _norm(partner):
                marriages.append((_norm(partner), date))
        koseki_ids = [str((row.get("value") or {}).get("戸籍レコードID", {})
                          .get("value") or "")
                      for row in (record.get("登場戸籍") or {}).get("value") or []]
        completed = []
        for kid in koseki_ids:
            for family in _family_name_candidates(
                    (koseki_info.get(kid) or {}).get("筆頭者", "")):
                candidate = family + _norm(name)
                if family and candidate != _norm(name):
                    completed.append(candidate)
        views.append(PersonView(
            record_id=_v(record, "$id"), name=name, norm_name=_norm(name),
            birth=birth, case_id=_v(record, "案件レコードID"),
            meyose=_v(record, "名寄せ確定"),
            koseki_ids=[k for k in koseki_ids if k],
            marriages=marriages, completed_names=completed))
    return views


def score_pair(a: PersonView, b: PersonView,
               koseki_info: dict | None = None) -> dict:
    """候補ペアのスコアリング（純関数）。
    Returns: {"signals": [...], "qualified": bool, "pending": bool, "根拠": {...}}
    """
    signals: list[str] = []
    evidence: dict = {"氏名": [f"No.{a.record_id} {a.name}",
                               f"No.{b.record_id} {b.name}"]}

    # 同一戸籍に共起する2レコードは別人（戸籍内の同名は起票時に冪等排除済み）
    if set(a.koseki_ids) & set(b.koseki_ids):
        return {"signals": [], "qualified": False, "pending": False,
                "根拠": evidence}

    if a.norm_name and a.norm_name == b.norm_name:
        signals.append(SIGNAL_NAME)
    elif (b.norm_name in a.completed_names) or (a.norm_name in b.completed_names):
        # ②: 名のみ人物の氏補完後一致（例: 香音＋戸籍氏「鈴木」→ 鈴木香音）
        signals.append(SIGNAL_NAME_COMPLETED)

    if a.birth and a.birth == b.birth:
        signals.append(SIGNAL_BIRTH)
        evidence["生年月日"] = a.birth

    # ④ 同一の相手方に互換日付で婚姻（互いを相手方とする配偶者ペアは該当しない）
    for pa, da in a.marriages:
        for pb, db in b.marriages:
            if pa and pa == pb and _date_compatible(da, db):
                signals.append(SIGNAL_MARRIAGE)
                evidence["婚姻"] = f"相手方={pa} 日付={da}/{db}"
                break
        if SIGNAL_MARRIAGE in signals:
            break

    # ⑤ 従前戸籍チェーン（注記のみ・成立条件に入らない）
    if koseki_info:
        for ka in a.koseki_ids:
            for kb in b.koseki_ids:
                ia, ib = koseki_info.get(ka) or {}, koseki_info.get(kb) or {}
                juzen_a = _norm(ia.get("従前本籍", ""))
                juzen_b = _norm(ib.get("従前本籍", ""))
                if (juzen_a and juzen_a == _norm(ib.get("本籍", ""))) or (
                        juzen_b and juzen_b == _norm(ia.get("本籍", ""))):
                    signals.append(SIGNAL_CHAIN)
                    break
            if SIGNAL_CHAIN in signals:
                break

    qualified = ((SIGNAL_NAME in signals and SIGNAL_BIRTH in signals)
                 or (SIGNAL_NAME_COMPLETED in signals and SIGNAL_BIRTH in signals)
                 or SIGNAL_MARRIAGE in signals)
    pending = bool(a.case_id and b.case_id and a.case_id != b.case_id)
    return {"signals": signals, "qualified": qualified, "pending": pending,
            "根拠": evidence}


def _pair_key(a: str, b: str) -> str:
    lo, hi = sorted((a, b), key=int)
    return f"person_merge:{lo}-{hi}"


def reduce_chain_pairs(pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """3名以上の連鎖の縮約: (a,b) について、より小さい共通ノード m があり
    (m,a)・(m,b) がともに成立しているなら (a,b) を落とす
    （例: 6-9・6-19 があれば 9-19 は不要。統合先が最小番号 No.6 に一意化される）"""
    qualified = {tuple(sorted(p, key=int)) for p in pairs}
    nodes = {n for pair in qualified for n in pair}

    def key(x: str, y: str) -> tuple[str, str]:
        return tuple(sorted((x, y), key=int))

    result = []
    for a, b in sorted(qualified, key=lambda p: (int(p[0]), int(p[1]))):
        dominated = any(
            int(m) < int(a) and key(m, a) in qualified and key(m, b) in qualified
            for m in nodes if m not in (a, b))
        if not dominated:
            result.append((a, b))
    return result


async def _already_filed(pair_key: str) -> bool:
    """同一ペアの封筒が存在すれば重複起票しない（冪等）。

    状態を問わず照合する（R4-2b 改修）: 未処理（要確認）だけでなく、
    「別人」裁定でクローズ済みの封筒も対象に含める＝**棄却済みペアの再起票を
    恒久抑止**する。統合済みペアは敗者削除により再検出されないため無害
    """
    records = await kintone.search_records(
        APP_SHIPPING,
        f'チャネル固有データ like "{pair_key}"',
        fields=["$id"])
    return bool(records)


async def _file_candidate(a: PersonView, b: PersonView, score: dict) -> str:
    """App 30 封筒（トップキー=person_merge）への候補起票"""
    lo, hi = sorted((a, b), key=lambda v: int(v.record_id))
    detail = {
        "ペアキー": _pair_key(a.record_id, b.record_id),
        "勝者候補": lo.record_id,
        "敗者候補": hi.record_id,
        "シグナル": score["signals"],
        "保留": score["pending"],
        "保留理由": (f"案件参照が相違（No.{lo.record_id}={lo.case_id} / "
                     f"No.{hi.record_id}={hi.case_id}）" if score["pending"] else ""),
        "根拠": {**score["根拠"],
                 "生年月日実値": {lo.record_id: lo.birth, hi.record_id: hi.birth}},
    }
    fields = {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": UNIT,
        "件名": f"人物の名寄せ候補: No.{lo.record_id} {lo.name} ⇔ "
                f"No.{hi.record_id} {hi.name}",
        "エラー詳細": json.dumps(detail, ensure_ascii=False)[:500],
        "チャネル固有データ": json.dumps({"person_merge": detail},
                                         ensure_ascii=False),
        "実行済み": "no",
    }
    return str(await kintone.create_record(APP_SHIPPING, fields))


async def _mark_auto_candidate(view: PersonView) -> None:
    """機械遷移(a): 名寄せ確定 未確定→自動候補 のみ。それ以外の値からは遷移しない。
    書き込むフィールドはこの1つだけ（他フィールド不変・裁定1）"""
    if view.meyose != "未確定":
        return
    await kintone.update_record(APP_KOSEKI_PERSON, view.record_id,
                                {"名寄せ確定": "自動候補"})
    view.meyose = "自動候補"  # 同一人物が複数ペアに出ても二度書かない


async def detect_merge_candidates() -> dict:
    """手動起動の入口: App 34 全人物から候補ペアを検出→封筒起票→自動候補マーク。

    PERSON_MERGE_ENABLED=1 のときのみ動く（既定無効・無効時は検出も起票もしない）。
    """
    if not merge_enabled():
        return {"status": "disabled", "reason": "PERSON_MERGE_ENABLED が未設定"}
    for app in (APP_KOSEKI_PERSON, APP_KOSEKI_BOOK, APP_SHIPPING):
        if not (app.app_id() and app.token()):
            return {"status": "skipped",
                    "reason": f"{app.label} の env（{app.app_id_env}）が未設定"}

    person_records = await kintone.search_records(
        APP_KOSEKI_PERSON, "order by $id asc limit 500",
        fields=["$id", "氏名", "案件レコードID", "名寄せ確定", "身分事項", "登場戸籍"])
    koseki_records = await kintone.search_records(
        APP_KOSEKI_BOOK, "order by $id asc limit 500",
        fields=["$id", "読解JSON"])
    koseki_info = _koseki_info(koseki_records)
    views = build_views(person_records, koseki_records)
    by_id = {v.record_id: v for v in views}

    scored: dict[tuple[str, str], dict] = {}
    for i, a in enumerate(views):
        for b in views[i + 1:]:
            score = score_pair(a, b, koseki_info)
            if score["qualified"]:
                scored[tuple(sorted((a.record_id, b.record_id), key=int))] = score

    pairs = reduce_chain_pairs(list(scored.keys()))
    results = {"status": "ok", "candidates": [], "filed": 0,
               "skipped_duplicates": 0}
    for lo, hi in pairs:
        score = scored[(lo, hi)]
        key = _pair_key(lo, hi)
        entry = {"pair": key, "signals": score["signals"],
                 "保留": score["pending"]}
        if await _already_filed(key):
            results["skipped_duplicates"] += 1
            entry["filed"] = "skip（同ペアの封筒あり: 未処理または裁定済み）"
            results["candidates"].append(entry)
            continue
        review_id = await _file_candidate(by_id[lo], by_id[hi], score)
        results["filed"] += 1
        entry["review_record_id"] = review_id
        if not score["pending"]:  # 保留ペアは自動候補にしない（裁定）
            await _mark_auto_candidate(by_id[lo])
            await _mark_auto_candidate(by_id[hi])
        results["candidates"].append(entry)
        print(f"[PERSON_MERGE] candidate {key} signals={score['signals']} "
              f"保留={score['pending']} review={review_id}")
    return results
