# -*- coding: utf-8 -*-
"""遺産分割協議書（案）の雛形 docx を生成する — SOUZOKU-4（QUEUE-2026-10-17-H H-5）

成果物: docx_templates/souzoku/遺産分割協議書.docx
規約（票）: 全文明朝体・太字なし・表やテキストボックス不使用の **素の段落** のみ・「弁護士確認前の案」の透かし文言 1 行（先頭）。
プレースホルダは各段落 1 つ（単一 run）。複数行の値（相続人一覧・財産目録・取得条項・代償金・署名欄）は
hub/docx_builder.fill_template_multiline が改行を <w:br/> にする。zip の時刻を固定した決定的バイト列（同一定義→同一 SHA-256）。
使い方: python scripts/make_souzoku_kyogisho_template.py [<出力先>]
"""
import hashlib
import io
import os
import sys
import zipfile

from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.shared import Mm, Pt

_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)
FONT_JA = "ＭＳ 明朝"
OUT_DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docx_templates", "souzoku", "遺産分割協議書.docx")

# 段落の並び（プレースホルダは 1 段落 1 つ・単一 run）
PARAGRAPHS = [
    ("{{透かし}}", 9, "left"),
    ("{{表題}}", 14, "center"),
    ("", 10.5, "left"),
    ("{{前文}}", 10.5, "left"),
    ("", 10.5, "left"),
    ("{{相続人一覧}}", 10.5, "left"),
    ("", 10.5, "left"),
    ("{{取得条項}}", 10.5, "left"),
    ("", 10.5, "left"),
    ("{{代償金条項}}", 10.5, "left"),
    ("{{記載外条項}}", 10.5, "left"),
    ("{{注記}}", 9.5, "left"),
    ("", 10.5, "left"),
    ("{{後文}}", 10.5, "left"),
    ("", 10.5, "left"),
    ("{{作成日}}", 10.5, "left"),
    ("", 10.5, "left"),
    ("{{署名欄}}", 10.5, "left"),
]
PLACEHOLDERS = tuple(p for p, _, _ in PARAGRAPHS if p)


def _set_font(run, size: float) -> None:
    run.font.name = FONT_JA
    run.font.size = Pt(size)
    run.font.bold = False
    rpr = run._element.get_or_add_rPr()
    rfonts = rpr.find(qn("w:rFonts"))
    if rfonts is None:
        rfonts = rpr.makeelement(qn("w:rFonts"), {})
        rpr.append(rfonts)
    for attr in ("w:ascii", "w:hAnsi", "w:eastAsia", "w:cs"):
        rfonts.set(qn(attr), FONT_JA)


def build_docx() -> bytes:
    doc = Document()
    sec = doc.sections[0]
    sec.page_height, sec.page_width = Mm(297), Mm(210)
    sec.left_margin = sec.right_margin = Mm(25)
    sec.top_margin = sec.bottom_margin = Mm(25)
    normal = doc.styles["Normal"]
    normal.font.name = FONT_JA
    normal.font.bold = False
    normal.element.rPr.rFonts.set(qn("w:eastAsia"), FONT_JA)
    for text, size, align in PARAGRAPHS:
        para = doc.add_paragraph()
        para.alignment = {"center": WD_ALIGN_PARAGRAPH.CENTER}.get(align, WD_ALIGN_PARAGRAPH.LEFT)
        run = para.add_run(text)
        _set_font(run, size)
    buf = io.BytesIO()
    doc.save(buf)
    return buf.getvalue()


def deterministic_zip(docx_bytes: bytes) -> bytes:
    src = zipfile.ZipFile(io.BytesIO(docx_bytes))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            zi = zipfile.ZipInfo(info.filename, date_time=_FIXED_ZIP_TIME)
            zi.compress_type = zipfile.ZIP_DEFLATED
            zi.external_attr = info.external_attr
            dst.writestr(zi, src.read(info.filename))
    return buf.getvalue()


def main(argv) -> int:
    out = argv[1] if len(argv) > 1 else OUT_DEFAULT
    data = deterministic_zip(build_docx())
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "wb") as f:
        f.write(data)
    print(f"written bytes={len(data)} sha256={hashlib.sha256(data).hexdigest()}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
