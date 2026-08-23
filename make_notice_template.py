"""時効援用通知書テンプレートの収載スクリプト（JIKOU-NOTICE-1）

大野修正版の実物（Desktop/claude/時効援用通知書　黒田達人.docx・誤字・
信用情報文言修正済み=本文凍結の正）から、個人情報をプレースホルダ化した
テンプレートを docx_templates/jikou/時効援用通知書.docx へ生成する。
生成物の SHA-256 を notice_webhook.py の TEMPLATE_SHA256 に pin する
（契約書と同型）。

置換規則（値は**実物の記載ブロックから読み取る**＝本スクリプトに個人情報を
持たない）:
  - p00 日付行（全体）              → {{通知日付}}
  - 通知人氏名（p02/p11/p20 の 3 箇所）→ {{通知人氏名}}
  - p19 ふりがな                    → {{ふりがな}}
  - p21 生年月日                    → {{生年月日}}
  - p22 住所（記載ブロック）        → {{通知人住所}}
  - p22 の直後に「旧　住　所」行を同書式で追加 → {{旧住所}}
    （生成時: 旧住所が空なら段落ごと削除・非空なら差し込み）

凍結（大野裁定・改変しない）:
  - 宛先「債権者各位」
  - 冒頭の事務所住所ブロック（〒・住所・建物名・通知代理人 弁護士名・
    TEL/FAX＝p03〜p08）
  - 表題・本文全段落（信用情報機関への削除依頼・債務承認非該当の付言含む）

プレースホルダ段落は run を単一化（fill_template 互換）。凍結段落の run は
触らない。

実行: python make_notice_template.py
"""

import copy
import hashlib
import re
from pathlib import Path

from docx import Document

SOURCE = Path.home() / "Desktop" / "claude" / "時効援用通知書　黒田達人.docx"
OUT = Path("docx_templates") / "jikou" / "時効援用通知書.docx"

# 記載ブロックのラベル（値の抽出と、置換後の体裁維持に使う）
_LABELED = {
    19: ("ふりがな", "{{ふりがな}}"),
    20: ("債務者氏名", "{{通知人氏名}}"),
    21: ("生年月日", "{{生年月日}}"),
    22: ("住　　　所", "{{通知人住所}}"),
}


def _set_text(p, text: str) -> None:
    """段落テキストを差し替え（先頭 run の書式を維持・run を単一化）。"""
    if not p.runs:
        p.add_run(text)
        return
    p.runs[0].text = text
    for r in p.runs[1:]:
        r.text = ""


def _label_split(text: str, label: str) -> tuple[str, str]:
    """『ラベル+空白+値』を（ラベル+空白, 値）に分ける。"""
    m = re.match(rf"^({re.escape(label)}[\s　]*)(.*)$", text)
    if not m or not m.group(2):
        raise SystemExit(f"記載ブロックの形が想定と異なります: {label}")
    return m.group(1), m.group(2)


def main() -> None:
    if not SOURCE.exists():
        raise SystemExit(f"大野修正版が見つかりません: {SOURCE}")
    doc = Document(SOURCE)
    paras = doc.paragraphs
    if len(paras) != 24 or paras[1].text != "債権者各位":
        raise SystemExit("段落構成が想定（24 段落・宛先=債権者各位）と異なります")

    # 実物から値を読む（氏名は記載ブロックの債務者氏名を正とする）
    values = {}
    for idx, (label, key) in _LABELED.items():
        _prefix, value = _label_split(paras[idx].text, label)
        values[key] = value
    name = values["{{通知人氏名}}"]

    # 記載ブロック 4 行をプレースホルダ化（ラベル+元の空白は維持）
    for idx, (label, key) in _LABELED.items():
        prefix, _value = _label_split(paras[idx].text, label)
        _set_text(paras[idx], prefix + key)

    # 旧住所行を住所行の直後に同書式で追加（{{旧住所}}・空なら生成時に削除）
    addr_p = paras[22]
    new_el = copy.deepcopy(addr_p._element)
    addr_p._element.addnext(new_el)
    doc2_paras = doc.paragraphs  # 挿入後に取り直し
    old_addr_p = doc2_paras[23]
    prefix, _ = _label_split(addr_p.text, "住　　　所")
    _set_text(old_addr_p, prefix.replace("住　　　所", "旧　住　所") + "{{旧住所}}")

    # 日付行（全体）と通知人氏名（冒頭・本文）
    _set_text(doc2_paras[0], "{{通知日付}}")
    for idx in (2, 11):
        t = doc2_paras[idx].text
        if name not in t:
            raise SystemExit(f"p{idx:02d} に氏名が見つかりません")
        _set_text(doc2_paras[idx], t.replace(name, "{{通知人氏名}}"))

    # 残存個人情報の機械検査（氏名・ふりがな・生年月日・記載住所の値）
    all_text = "\n".join(p.text for p in doc.paragraphs)
    for key, value in values.items():
        for token in re.split(r"[\s　]+", value):
            if token and token in all_text:
                raise SystemExit(f"個人情報が残存しています: {key}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    doc.save(OUT)
    sha = hashlib.sha256(OUT.read_bytes()).hexdigest()
    print(f"saved: {OUT} paras={len(doc.paragraphs)}")
    print(f"TEMPLATE_SHA256 = {sha}")


if __name__ == "__main__":
    main()
