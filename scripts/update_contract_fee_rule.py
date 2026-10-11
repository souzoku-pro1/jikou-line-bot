# -*- coding: utf-8 -*-
"""委任契約書（時効援用）雛形の第2条（弁護士報酬）を新料金規則へ差し替える — JIKOU-FEE-RULE-1

大野裁定（2026-10-11）: 1社 44,000円（税込）・2社 88,000円・3社目以降 1社 22,000円（税込）。
手続完了後の追加依頼も同一委任者の通算社数で算定する。

add_contract_tokuyaku.py と同じ位置づけ（雛形変更を python-docx でスクリプト的に行い、
再現可能にする）。

処理（旧雛形 = JIKOU-CONTRACT-TOKUYAKU で収載した現物・SHA ead90bb8…）:
  見出し「第2条（弁護士報酬）」の直後の 3 段落（旧 1〜3 項）を次の 2 段落に差し替える:
    1　本件弁護士報酬（手数料）は、対象1社あたり44,000円（消費税込み）とする。ただし、
       同一の委任者について対象が3社以上となる場合、3社目以降は1社あたり22,000円
       （消費税込み）とし、本契約締結後に対象を追加する場合も通算の社数により算定する。
    2　報酬は前払いとし、分割払いはできない。
  - 旧 1 項の段落に新 1 項の文（裁定文言・単一 run・rPr/pPr は旧段落のまま）
  - 旧 2 項（「社数を乗じた額」）は新 1 項に吸収されるため段落ごと削除
  - 旧 3 項（前払い・分割不可）は番号を 2 に繰り上げて維持（裁定に言及がなく、
    費用固定文の「前払いのみ」と整合させるため残す）
  上記以外の文面・段落は一字も変えない。

再現性: zip を固定時刻で書き直し、同じ入力から常に同じバイト列（=同じ SHA-256）を
得る（test_contract_tokuyaku が fixture からの再現を pin）。入力 SHA が旧雛形と一致
しないとき（二重適用・別物）は ValueError。

使い方:
  python scripts/update_contract_fee_rule.py <旧雛形.docx> <出力.docx>
  成功時は無出力・終了 0（sink 方針: print を使わない＝redaction_sink_allowlist 不変）。
  出力の SHA-256 は `sha256sum <出力.docx>` で確認する（test_contract_tokuyaku が pin）。
  引数不正は使い方を stderr に出して終了 1・入力 SHA 不一致/構造不一致は ValueError。
"""

import hashlib
import io
import os
import sys

from docx import Document

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from add_contract_tokuyaku import _set_single_run_text, deterministic_zip  # noqa: E402

SOURCE_SHA256 = "ead90bb8154f64318cc80ee7d6a2dda129192936ca29e32746348ac6145856fd"
HEADING = "第2条（弁護士報酬）"
OLD_ITEMS = (
    "1　本件の弁護士報酬（手数料）は、対象債権者1社につき金44,000円（消費税込み）とする。",
    "2　対象債権者が複数の場合の報酬は、前項の金額に社数を乗じた額とする（割引は行わない。）。",
    "3　報酬は前払いとし、分割払いはできない。",
)
NEW_ITEMS = (
    "1　本件弁護士報酬（手数料）は、対象1社あたり44,000円（消費税込み）とする。ただし、"
    "同一の委任者について対象が3社以上となる場合、3社目以降は1社あたり22,000円"
    "（消費税込み）とし、本契約締結後に対象を追加する場合も通算の社数により算定する。",
    "2　報酬は前払いとし、分割払いはできない。",
)


def build_fee_rule_template(src_bytes: bytes) -> bytes:
    """旧雛形（ead90bb8…）バイト列 → 第2条を差し替えた新雛形バイト列（決定的）。"""
    if hashlib.sha256(src_bytes).hexdigest() != SOURCE_SHA256:
        raise ValueError("unexpected source template (sha256 mismatch)")
    doc = Document(io.BytesIO(src_bytes))
    paras = doc.paragraphs
    texts = [p.text.strip() for p in paras]
    try:
        i = texts.index(HEADING)
    except ValueError:
        raise ValueError("article 2 heading not found") from None
    if tuple(texts[i + 1:i + 4]) != OLD_ITEMS:
        raise ValueError("unexpected article 2 structure")
    p1, p2, p3 = paras[i + 1], paras[i + 2], paras[i + 3]
    if any(len(p.runs) != 1 for p in (p1, p2, p3)):
        raise ValueError("unexpected run structure in article 2")
    _set_single_run_text(p1._p, NEW_ITEMS[0])
    p2._p.getparent().remove(p2._p)
    _set_single_run_text(p3._p, NEW_ITEMS[1])
    out = io.BytesIO()
    doc.save(out)
    return deterministic_zip(out.getvalue())


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        raise SystemExit(__doc__)          # 使い方を stderr へ（終了コード 1・print 不使用）
    src, dst = argv[1], argv[2]
    data = build_fee_rule_template(open(src, "rb").read())
    open(dst, "wb").write(data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
