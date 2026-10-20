"""代理人目録テンプレートの収載スクリプト（HOUKI-DAIRININ-1）

申述書テンプレ docx_templates/houki/相続放棄申述書.docx の用紙・余白・既定書式（sectPr・styles）を複製し、
本文を 表題 + 2 列の表（ラベル／値=プレースホルダ）+ 末尾文 に置き換えて docx_templates/houki/代理人目録.docx を
生成する。各 {{ }} は単一 run。生成物は決定的 zip（タイムスタンプ固定）で、SHA-256 を
hub/houki_dairinin.TEMPLATE_SHA256 に pin する（受理通知送付状と同型）。

実行: python scripts/make_dairinin_template.py
"""

import hashlib
import io
import sys
import zipfile
from pathlib import Path

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.shared import Pt

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from hub.houki_dairinin import FOOTER, ROWS, TEMPLATE_SHA256, TITLE, verify_structure  # noqa: E402

SOURCE = REPO / "docx_templates" / "houki" / "相続放棄申述書.docx"
OUT = REPO / "docx_templates" / "houki" / "代理人目録.docx"
_FIXED_ZIP_TIME = (1980, 1, 1, 0, 0, 0)


def deterministic_zip(docx_bytes: bytes) -> bytes:
    src = zipfile.ZipFile(io.BytesIO(docx_bytes))
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in sorted(src.infolist(), key=lambda i: i.filename):
            zi = zipfile.ZipInfo(info.filename, date_time=_FIXED_ZIP_TIME)
            zi.compress_type = zipfile.ZIP_DEFLATED
            zi.external_attr = 0
            dst.writestr(zi, src.read(info.filename))
    return out.getvalue()


def build() -> bytes:
    doc = Document(str(SOURCE))
    body = doc.element.body
    # 本文（段落・表）をすべて除き、sectPr（用紙・余白）だけ残す
    for child in list(body.iterchildren()):
        if not child.tag.endswith("}sectPr"):
            body.remove(child)
    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = title.add_run(TITLE)
    r.bold = True
    r.font.size = Pt(16)
    doc.add_paragraph("")
    table = doc.add_table(rows=len(ROWS), cols=2)
    table.style = "Table Grid"
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, (label, ph) in enumerate(ROWS):
        c0, c1 = table.rows[i].cells
        c0.paragraphs[0].add_run(label)
        c1.paragraphs[0].add_run(ph)            # 単一 run
    doc.add_paragraph("")
    doc.add_paragraph(FOOTER)
    buf = io.BytesIO()
    doc.save(buf)
    return deterministic_zip(buf.getvalue())


def verify(docx_bytes: bytes) -> None:
    doc = Document(io.BytesIO(docx_bytes))
    verify_structure(doc)
    for _label, ph in ROWS:
        hits = [r for t in doc.tables for row in t.rows for cell in row.cells
                for p in cell.paragraphs for r in p.runs if r.text == ph]
        if len(hits) != 1:
            raise SystemExit(f"収載中止: プレースホルダ {ph} が単一 run に 1 つ存在しない")


def main() -> None:
    out = build()
    verify(out)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_bytes(out)
    # sink 方針: 値（パス・SHA）は出力しない。SHA は hub/houki_dairinin.TEMPLATE_SHA256 と照合して確認する
    if hashlib.sha256(out).hexdigest() != TEMPLATE_SHA256:
        raise SystemExit("収載完了（SHA が hub/houki_dairinin.TEMPLATE_SHA256 と不一致＝pin を更新して再実行）")
    print("wrote docx_templates/houki/代理人目録.docx (sha256 matches hub/houki_dairinin.TEMPLATE_SHA256)")


if __name__ == "__main__":
    main()
