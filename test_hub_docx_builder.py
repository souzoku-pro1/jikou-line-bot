"""hub/docx_builder.py の単体テスト（T0-3）

検証: 差込（run 分割・表セル含む）・和暦変換・テンプレ規約解決・
プレースホルダ検査・re-export 互換・実テンプレートと検査レジストリの一致。
"""

import io
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from docx import Document

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from hub import docx_builder
from hub.docx_builder import (
    TemplateNotFound,
    fill_template,
    list_placeholders,
    resolve_template,
    to_wareki,
    validate_template,
)


def _make_docx(path: Path, paragraphs=(), table_cells=(), split_run=None):
    doc = Document()
    for text in paragraphs:
        doc.add_paragraph(text)
    if split_run:
        p = doc.add_paragraph()
        for part in split_run:
            p.add_run(part)
    if table_cells:
        table = doc.add_table(rows=1, cols=len(table_cells))
        for i, text in enumerate(table_cells):
            table.rows[0].cells[i].text = text
    doc.save(str(path))


def _all_text(docx_bytes: bytes) -> str:
    doc = Document(io.BytesIO(docx_bytes))
    parts = [p.text for p in doc.paragraphs]
    for t in doc.tables:
        for row in t.rows:
            for c in row.cells:
                parts.extend(p.text for p in c.paragraphs)
    return "\n".join(parts)


class TestFillTemplate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "t.docx"

    def test_replaces_in_paragraphs_and_tables(self):
        _make_docx(self.path,
                   paragraphs=["{{氏名}}様", "日付: {{日付}}"],
                   table_cells=["{{氏名}}", "固定文言"])
        out = fill_template(str(self.path), {"{{氏名}}": "山田太郎", "{{日付}}": "令和8年7月3日"})
        text = _all_text(out)
        self.assertIn("山田太郎様", text)
        self.assertIn("日付: 令和8年7月3日", text)
        self.assertIn("固定文言", text)
        self.assertNotIn("{{", text, "未置換のプレースホルダが残っていない")

    def test_replaces_placeholder_split_across_runs(self):
        """Word 編集で run 分割されたプレースホルダも置換できる（既存実装の要点）"""
        _make_docx(self.path, split_run=["{{氏", "名}}", "様"])
        out = fill_template(str(self.path), {"{{氏名}}": "山田"})
        self.assertIn("山田様", _all_text(out))

    def test_no_match_leaves_document_unchanged(self):
        _make_docx(self.path, paragraphs=["プレースホルダなし"])
        out = fill_template(str(self.path), {"{{氏名}}": "山田"})
        self.assertIn("プレースホルダなし", _all_text(out))

    def test_returns_valid_docx_bytes(self):
        _make_docx(self.path, paragraphs=["{{a}}"])
        out = fill_template(str(self.path), {"{{a}}": "b"})
        self.assertTrue(out.startswith(b"PK"))


class TestToWareki(unittest.TestCase):
    def test_reiwa_first_day(self):
        self.assertEqual(to_wareki(date(2019, 5, 1)), "令和元年5月1日")

    def test_reiwa(self):
        self.assertEqual(to_wareki(date(2026, 7, 3)), "令和8年7月3日")

    def test_heisei_last_day(self):
        self.assertEqual(to_wareki(date(2019, 4, 30)), "平成31年4月30日")

    def test_heisei_first_day(self):
        self.assertEqual(to_wareki(date(1989, 1, 8)), "平成元年1月8日")

    def test_before_heisei_falls_back_to_seireki(self):
        self.assertEqual(to_wareki(date(1989, 1, 7)), "1989年01月07日")


class TestResolveTemplate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        (self.base / "jikou").mkdir()
        _make_docx(self.base / "jikou" / "送付案内.docx", paragraphs=["x"])

    def test_resolves_by_convention(self):
        path = resolve_template("時効援用", "送付案内", base_dir=str(self.base))
        self.assertEqual(path, self.base / "jikou" / "送付案内.docx")

    def test_unknown_unit_raises(self):
        with self.assertRaises(TemplateNotFound):
            resolve_template("存在しないユニット", "送付案内", base_dir=str(self.base))

    def test_missing_file_raises(self):
        with self.assertRaises(TemplateNotFound):
            resolve_template("時効援用", "存在しない種別", base_dir=str(self.base))

    def test_new_unit_needs_only_config_entry(self):
        """拡張ポイント検証: UNIT_CONFIG のエントリ追加だけで新ユニットが解決できる"""
        (self.base / "houki").mkdir()
        _make_docx(self.base / "houki" / "申述書.docx", paragraphs=["y"])
        new_unit = {"相続放棄": {"template_dir": "houki"}}
        with patch.dict(docx_builder.UNIT_CONFIG, new_unit):
            path = resolve_template("相続放棄", "申述書", base_dir=str(self.base))
        self.assertEqual(path, self.base / "houki" / "申述書.docx")


class TestValidateTemplate(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "t.docx"

    def test_all_keys_present_returns_empty(self):
        _make_docx(self.path, paragraphs=["{{日付}}"], table_cells=["{{氏名}}"])
        self.assertEqual(validate_template(self.path, ["{{日付}}", "{{氏名}}"]), [])

    def test_missing_keys_are_reported(self):
        _make_docx(self.path, paragraphs=["{{日付}}"])
        missing = validate_template(self.path, ["{{日付}}", "{{氏名}}", "{{住所}}"])
        self.assertEqual(missing, ["{{氏名}}", "{{住所}}"])

    def test_missing_file_raises(self):
        with self.assertRaises(TemplateNotFound):
            validate_template(self.path / "nai.docx", ["{{x}}"])

    def test_list_placeholders(self):
        _make_docx(self.path, paragraphs=["{{b}} と {{a}}"], table_cells=["{{a}}"])
        self.assertEqual(list_placeholders(self.path), ["{{a}}", "{{b}}"])


class TestRealTemplateRegistry(unittest.TestCase):
    """config.EXPECTED_DOCX_TEMPLATES が実テンプレートと一致していること
    （日次死活監視が誤警報しないことの担保）"""

    def test_registered_templates_pass_validation(self):
        from config import EXPECTED_DOCX_TEMPLATES
        for path, keys in EXPECTED_DOCX_TEMPLATES.items():
            with self.subTest(template=path):
                self.assertEqual(validate_template(path, keys), [],
                                 f"{path} に登録済みプレースホルダが欠けている")

    def test_healthcheck_check_templates_is_clean(self):
        import daily_healthcheck
        self.assertEqual(daily_healthcheck.check_templates(), [])


class TestReExport(unittest.TestCase):
    def test_document_webhook_reexports(self):
        """既存の import 経路（document_webhook.fill_template / to_wareki）が生きていること"""
        import document_webhook
        self.assertIs(document_webhook.fill_template, fill_template)
        self.assertIs(document_webhook.to_wareki, to_wareki)


if __name__ == "__main__":
    unittest.main()
