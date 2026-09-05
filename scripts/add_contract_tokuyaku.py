# -*- coding: utf-8 -*-
"""委任契約書（時効援用）雛形に「特約事項」の 2 段落を追加する — JIKOU-CONTRACT-TOKUYAKU

申述書の make_shinjutsu_template.py と同じ位置づけ（雛形変更を python-docx で
スクリプト的に行い、再現可能にする）。

処理（旧雛形 = CONTRACT-GEN-1 で収載した現物・SHA 7cc168a1…）:
  最後の条文（「第N条（…）」見出しの最後のもの）の本文段落の直後・締結文
  （「本契約の成立を証するため…」）の前に、次の 2 段落を挿入する:
    見出し段落: 特約事項          … 書式は直前の条見出し段落（pPr・rPr）の複製
    本文段落:   {{特約}}          … 書式は直前の条文本文段落（pPr・rPr）の複製
  {{特約}} は単一 run。表は使わない。上記以外の文面は一字も変えない。

再現性: python-docx の save は zip エントリの時刻を現在時刻にするため SHA が
実行ごとに変わる。本スクリプトは zip を固定時刻（1980-01-01）で書き直し、
同じ入力から常に同じバイト列（=同じ SHA-256）を得る（test_contract_tokuyaku
が旧雛形 fixture からの再現を pin）。

使い方:
  python scripts/add_contract_tokuyaku.py <旧雛形.docx> <出力.docx>
"""

import copy
import hashlib
import io
import re
import sys
import zipfile

from docx import Document

HEADING_TEXT = "特約事項"
BODY_TEXT = "{{特約}}"
_ARTICLE_RE = re.compile(r"^第\d+条（")
_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)


def _set_single_run_text(p_elem, text: str) -> None:
    """段落要素の run を先頭 1 つだけ残し（rPr 保持）、テキストを置く。"""
    ns = p_elem.nsmap.get("w")
    runs = p_elem.findall(f"{{{ns}}}r")
    for r in runs[1:]:
        p_elem.remove(r)
    first = runs[0]
    for t in first.findall(f"{{{ns}}}t"):
        first.remove(t)
    t = copy.deepcopy(first.makeelement(f"{{{ns}}}t", {}))
    t.text = text
    t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    first.append(t)


def deterministic_zip(docx_bytes: bytes) -> bytes:
    """zip エントリの時刻を固定し、同一入力→同一バイト列にする。"""
    src = zipfile.ZipFile(io.BytesIO(docx_bytes))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            zi = zipfile.ZipInfo(info.filename, date_time=_FIXED_ZIP_TIME)
            zi.compress_type = zipfile.ZIP_DEFLATED
            zi.external_attr = info.external_attr
            dst.writestr(zi, src.read(info.filename))
    return buf.getvalue()


def build_tokuyaku_template(src_bytes: bytes) -> bytes:
    """旧雛形バイト列 → 特約 2 段落を挿入した新雛形バイト列（決定的）。"""
    doc = Document(io.BytesIO(src_bytes))
    paras = doc.paragraphs
    if any(p.text == HEADING_TEXT for p in paras):
        raise ValueError("already contains tokuyaku heading")
    heading_idx = max(i for i, p in enumerate(paras)
                      if _ARTICLE_RE.match(p.text))
    body_idx = heading_idx + 1
    heading_p, body_p = paras[heading_idx], paras[body_idx]
    if not body_p.text.strip() or not body_p.runs:
        raise ValueError("unexpected template structure (last article body)")
    new_heading = copy.deepcopy(heading_p._p)
    _set_single_run_text(new_heading, HEADING_TEXT)
    new_body = copy.deepcopy(body_p._p)
    _set_single_run_text(new_body, BODY_TEXT)
    body_p._p.addnext(new_heading)
    new_heading.addnext(new_body)
    out = io.BytesIO()
    doc.save(out)
    return deterministic_zip(out.getvalue())


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__)
        return 2
    src, dst = argv[1], argv[2]
    data = build_tokuyaku_template(open(src, "rb").read())
    open(dst, "wb").write(data)
    print("written:", dst)
    print("sha256:", hashlib.sha256(data).hexdigest())
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
