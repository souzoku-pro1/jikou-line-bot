"""従前戸籍チェーンの照合部品（R4-3・D-4 裁定の独立部品）

- App 33 の読解済み戸籍の 従前戸籍（本籍・筆頭者）を、収集済み戸籍の本籍と
  照合してチェーンを辿る。表記差（大字・漢数字 vs 算用）は R5-1 の比較用
  カノニカライズ（koseki_second_opinion._canon）を流用して吸収する
- 出力は**収集見込み（弁護士確認前）の参考判定**に留める（01 §4・04 §1）。
  OCR誤読でリンクが切れる実例（「鹿浜三丁目12」vs「鹿浜三丁目1261番地」）が
  あるため、機械判定を戸籍収集完了の確定に使わない。
  相続順位エンジンは F5（兄弟姉妹相続の収集不足）の保留理由の提示に使う
- 読み取り専用（kintone 呼び出しなし・入力は読解JSONの列）
"""

import json

from koseki_second_opinion import _canon


def _reading(koseki_record: dict) -> dict:
    """App 33 レコード（GET形）または読解JSON dict のどちらも受ける"""
    if "読解JSON" in koseki_record:
        try:
            return json.loads(
                str((koseki_record["読解JSON"] or {}).get("value") or "{}"))
        except (json.JSONDecodeError, AttributeError):
            return {}
    return koseki_record


def chain_links(kosekis: list[dict]) -> list[dict]:
    """各戸籍の従前戸籍が収集済み戸籍のどれに当たるかを照合する。

    Returns: [{"本籍", "筆頭者", "従前": {...}, "リンク先": index|None,
               "筆頭者一致": bool|None}]
    照合キーは本籍（カノニカライズ一致）。筆頭者は参考（不一致でも警告扱い）
    """
    readings = [_reading(k) for k in kosekis]
    infos = []
    for r in readings:
        koseki = r.get("戸籍") or {}
        juzen = koseki.get("従前戸籍") or {}
        infos.append({
            "本籍": str(koseki.get("本籍") or ""),
            "筆頭者": str(koseki.get("筆頭者") or ""),
            "従前": {"本籍": str(juzen.get("本籍") or ""),
                     "筆頭者": str(juzen.get("筆頭者") or "")},
        })
    links = []
    for info in infos:
        juzen = info["従前"]
        link = {**info, "リンク先": None, "筆頭者一致": None}
        if _canon(juzen["本籍"]):
            for j, other in enumerate(infos):
                if other is info:
                    continue
                if _canon(other["本籍"]) == _canon(juzen["本籍"]):
                    link["リンク先"] = j
                    link["筆頭者一致"] = (
                        _canon(other["筆頭者"]) == _canon(juzen["筆頭者"])
                        if _canon(juzen["筆頭者"]) else None)
                    break
        links.append(link)
    return links


def assess_chain(kosekis: list[dict]) -> dict:
    """チェーン全体の評価。未収集 = 従前戸籍の記載があるのに収集済み戸籍に
    リンクできないもの（＝取得候補）の列挙"""
    links = chain_links(kosekis)
    missing = [link["従前"] for link in links
               if _canon(link["従前"]["本籍"]) and link["リンク先"] is None]
    return {"リンク": links, "未収集": missing,
            "注記": "収集見込み（弁護士確認前・OCR表記揺れでリンクが切れる"
                    "可能性があります）"}


def assess_for_rank(kosekis: list[dict], rank: int) -> dict:
    """順位別の必要範囲の参考判定。
    - 第1順位: 被相続人の出生〜死亡の連続（v1 はチェーン欠落の有無で近似）
    - 第3順位: 加えて父母それぞれの出生までの連続が必要（兄弟姉妹の網羅）
    いずれも「収集見込み（弁護士確認前）」の参考表示（F5 の保留理由の提示用）
    """
    assessment = assess_chain(kosekis)
    assessment["必要範囲"] = (
        "被相続人の出生から死亡までの連続した戸籍" if rank == 1
        else "被相続人の出生〜死亡に加え、父母それぞれの出生までの戸籍"
        if rank == 3 else "順位に応じた連続戸籍")
    return assessment
