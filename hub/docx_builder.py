"""docx 生成の共通実装（hub/docx_builder）

設計: docs/architecture/03-common-components.md §6

- fill_template / to_wareki: document_webhook.py から移設（T0-3・実装変更なし）。
  既存の import 経路は document_webhook 側の re-export で維持される。
- resolve_template: docx_templates/<ユニットの template_dir>/<種別>.docx の規約解決
- validate_template: テンプレート内に必要プレースホルダ（{{...}}）が揃っているか検査
  （daily_healthcheck に登録し、人がテンプレートを編集して差込キーを消した事故を検知）
"""

import io
import re
from datetime import date
from pathlib import Path

from docx import Document

from config import UNIT_CONFIG

# テンプレート規約のルートディレクトリ（リポジトリ相対・既存の配置と同じ）
TEMPLATE_ROOT = "docx_templates"

_PLACEHOLDER_RE = re.compile(r"\{\{[^{}]+\}\}")


class TemplateNotFound(Exception):
    """テンプレートファイルが規約の場所に存在しない / ユニットが未定義"""


def to_wareki(d: date) -> str:
    """西暦→和暦表記（document_webhook から移設・実装不変）"""
    if d >= date(2019, 5, 1):
        n, era = d.year - 2018, "令和"
    elif d >= date(1989, 1, 8):
        n, era = d.year - 1988, "平成"
    else:
        return d.strftime("%Y年%m月%d日")
    y = "元" if n == 1 else str(n)
    return f"{era}{y}年{d.month}月{d.day}日"


def fill_template(template_path: str, data: dict) -> bytes:
    """テンプレートを差し込み置換して docx の bytes を返す
    （document_webhook から移設・実装不変。run 分割されたプレースホルダにも対応）"""
    doc = Document(template_path)

    def replace_in_paragraph(para):
        full = "".join(run.text for run in para.runs)
        if not any(k in full for k in data):
            return
        for k, v in data.items():
            full = full.replace(k, v)
        if para.runs:
            para.runs[0].text = full
            for run in para.runs[1:]:
                run.text = ""

    for para in doc.paragraphs:
        replace_in_paragraph(para)
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for para in cell.paragraphs:
                    replace_in_paragraph(para)

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.read()


def _replace_multiline_in_paragraph(para, data: dict) -> None:
    full = "".join(run.text for run in para.runs)
    if not any(k in full for k in data):
        return
    for k, v in data.items():
        full = full.replace(k, str(v))
    if not para.runs:
        return
    lines = full.split("\n")
    para.runs[0].text = lines[0]
    for run in para.runs[1:]:
        run.text = ""
    for line in lines[1:]:
        br = para.add_run()
        br.add_break()
        para.add_run(line)


def _apply_multiline(doc: Document, data: dict) -> None:
    for para in doc.paragraphs:
        _replace_multiline_in_paragraph(para, data)
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for para in cell.paragraphs:
                    _replace_multiline_in_paragraph(para, data)


def fill_template_multiline(template_path: str, data: dict) -> bytes:
    """fill_template の複数行対応版（T2-1 で追加。既存 fill_template は不変）。

    値に改行（\\n）を含むプレースホルダを Word の改行（<w:br/>）として差し込む。
    同封物一覧のような「複数行を1プレースホルダで渡す」用途（設計 07 §2）に使う。
    ※ 2行目以降の run は段落既定の書式になる（雛形は段落書式で整えること）
    """
    doc = Document(template_path)
    _apply_multiline(doc, data)
    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.read()


def fill_table_rows(doc: Document, rows: list[dict], marker_prefix: str = "{{行:") -> None:
    """{{行:列名}} を含むテンプレート行を rows の件数分複製して差し込む
    （souzoku-shorui 02 §2 の設計を前倒し実装・T2-2 テンプレ統合で使用）。

    - テンプレート行の書式・罫線は行の複製（deepcopy）で継承される
    - rows が空のときはテンプレート行を削除する（空の表は「該当なし」にしない）
    - 対象は最初に見つかったテンプレート行1つ（1テンプレート=1可変表の規約）
    """
    import copy as _copy

    from docx.table import _Row

    for table in doc.tables:
        for row in table.rows:
            if not any(marker_prefix in cell.text for cell in row.cells):
                continue
            template_tr = row._tr
            for item in rows:
                new_tr = _copy.deepcopy(template_tr)
                template_tr.addprevious(new_tr)
                new_row = _Row(new_tr, table)
                data = {marker_prefix + k + "}}": str(v) for k, v in item.items()}
                for cell in new_row.cells:
                    for para in cell.paragraphs:
                        _replace_multiline_in_paragraph(para, data)
            template_tr.getparent().remove(template_tr)
            return
    # テンプレート行が見つからない場合は何もしない（validate_template で検知する）


def fill_template_with_table(template_path: str, data: dict,
                             table_rows: list[dict],
                             marker_prefix: str = "{{行:") -> bytes:
    """スカラー差込（複数行対応）＋表の行複製をまとめて行う（送付案内の正式書式用）"""
    doc = Document(template_path)
    _apply_multiline(doc, data)
    fill_table_rows(doc, table_rows, marker_prefix=marker_prefix)
    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.read()


def resolve_template(unit: str, doc_type: str, base_dir: str = TEMPLATE_ROOT) -> Path:
    """規約ベースのテンプレート解決: <base_dir>/<UNIT_CONFIG[unit].template_dir>/<doc_type>.docx
    ユニット未定義・ファイル不存在は TemplateNotFound"""
    conf = UNIT_CONFIG.get(unit)
    if conf is None:
        raise TemplateNotFound(f"ユニット未定義: {unit}（config.UNIT_CONFIG に登録してください）")
    path = Path(base_dir) / conf["template_dir"] / f"{doc_type}.docx"
    if not path.is_file():
        raise TemplateNotFound(f"テンプレートがありません: {path}")
    return path


def _extract_text(path) -> str:
    doc = Document(str(path))
    parts = [p.text for p in doc.paragraphs]
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                parts.extend(p.text for p in cell.paragraphs)
    return "\n".join(parts)


def validate_template(path, required_keys: list[str]) -> list[str]:
    """テンプレートに必要プレースホルダが揃っているか検査し、欠けているキーの一覧を返す
    （空リスト = 問題なし）。ファイル不存在は TemplateNotFound。"""
    p = Path(path)
    if not p.is_file():
        raise TemplateNotFound(f"テンプレートがありません: {p}")
    text = _extract_text(p)
    return [k for k in required_keys if k not in text]


def list_placeholders(path) -> list[str]:
    """テンプレート内の {{...}} プレースホルダを列挙する（レジストリ整備の補助）"""
    return sorted(set(_PLACEHOLDER_RE.findall(_extract_text(path))))
