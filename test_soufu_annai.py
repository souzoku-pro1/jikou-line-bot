"""channels/soufu_annai.py（M4 送付案内・T2-1）のテスト

T2-1 完了条件: ソート・フィルタ（無効除外/ユニット）・未定義キー・
同期検査異常系・複数行差込。
"""

import asyncio
import io
import os
import re
import unittest
import zipfile
from unittest.mock import AsyncMock, patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from docx import Document

from channels import soufu_annai
from channels.soufu_annai import (
    SoufuAnnaiError,
    build_enclosure_text,
    build_soufu_annai_docx,
    check_block_sync,
    fetch_blocks,
)
from hub import kintone
from hub.docx_builder import fill_template_multiline

_OFFICE_ENV = {
    "OFFICE_NAME": "大野法律事務所",
    "OFFICE_ZIP": "332-0000",
    "OFFICE_ADDRESS": "埼玉県川口市テスト町1-2-3",
    "OFFICE_TEL": "048-000-0000",
}


def block(key, order="1", units=("時効援用",), note="", name=None):
    return {
        "$id": {"value": "1"},
        "ブロックキー": {"value": key},
        "表示名": {"value": name or key},
        "案内文": {"value": note},
        "対象ユニット": {"value": list(units)},
        "返送要否": {"value": "不要"},
        "表示順": {"value": order},
    }


def run(coro):
    return asyncio.run(coro)


class TestFetchBlocks(unittest.TestCase):
    def test_sorted_by_display_order(self):
        records = [block("B", order="20"), block("A", order="10"), block("C", order="5")]
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=records)) as s:
            got = run(fetch_blocks("時効援用", ["A", "B", "C"]))
        keys = [b["ブロックキー"]["value"] for b in got]
        self.assertEqual(keys, ["C", "A", "B"])
        query = s.await_args.args[1]
        self.assertIn('有効 in ("yes")', query, "無効ブロックはクエリで除外")
        self.assertIn("order by 表示順", query)

    def test_only_selected_keys_are_returned(self):
        records = [block("A"), block("B"), block("C")]
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=records)):
            got = run(fetch_blocks("時効援用", ["B"]))
        self.assertEqual([b["ブロックキー"]["value"] for b in got], ["B"])

    def test_undefined_key_raises(self):
        """未定義キー（マスタに無い/無効化済み）の選択はエラー（→エラー遷移+警報）"""
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=[block("A")])):
            with self.assertRaises(SoufuAnnaiError) as ctx:
                run(fetch_blocks("時効援用", ["A", "存在しないキー"]))
        self.assertIn("存在しないキー", str(ctx.exception))

    def test_unit_mismatch_raises(self):
        records = [block("A", units=("相続放棄",))]
        with patch("hub.kintone.search_records", new=AsyncMock(return_value=records)):
            with self.assertRaises(SoufuAnnaiError) as ctx:
                run(fetch_blocks("時効援用", ["A"]))
        self.assertIn("時効援用", str(ctx.exception))

    def test_empty_selection_raises(self):
        with self.assertRaises(SoufuAnnaiError):
            run(fetch_blocks("時効援用", []))


class TestEnclosureText(unittest.TestCase):
    def test_lines_with_and_without_note(self):
        blocks = [block("委任契約書2通", note="ご署名のうえ1通ご返送ください"),
                  block("返信用封筒")]
        text = build_enclosure_text(blocks)
        self.assertEqual(text.split("\n"), [
            "■ 委任契約書2通",
            "　ご署名のうえ1通ご返送ください",
            "■ 返信用封筒",
        ])


class TestMultilineFill(unittest.TestCase):
    """fill_template_multiline: 改行入り値が Word の改行(<w:br/>)になる（複数行差込）"""

    def _docx_xml(self, docx_bytes):
        with zipfile.ZipFile(io.BytesIO(docx_bytes)) as z:
            return z.read("word/document.xml").decode("utf-8")

    def test_multiline_value_becomes_breaks(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.docx"
            d = Document()
            d.add_paragraph("{{一覧}}")
            d.save(str(path))
            out = fill_template_multiline(str(path), {"{{一覧}}": "1行目\n2行目\n3行目"})
        xml = self._docx_xml(out)
        self.assertEqual(len(re.findall(r"<w:br\s*/>", xml)), 2, "3行→改行2つ")
        for line in ("1行目", "2行目", "3行目"):
            self.assertIn(line, xml)

    def test_single_line_behaves_like_fill_template(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.docx"
            d = Document()
            d.add_paragraph("{{名前}}様")
            d.save(str(path))
            out = fill_template_multiline(str(path), {"{{名前}}": "山田"})
        doc = Document(io.BytesIO(out))
        self.assertIn("山田様", doc.paragraphs[0].text)


def make_shipping_record(**over):
    rec = {
        "$id": {"value": "9"},
        "ユニット種別": {"value": "時効援用"},
        "宛先名": {"value": "山田太郎"},
        "顧客名表示用": {"value": "山田太郎"},
        "件名": {"value": "委任契約書の送付"},
        "本文_特記事項": {"value": "同封の返信用封筒をご利用ください。"},
        "同封物選択": {"value": ["委任契約書2通", "返信用封筒"]},
    }
    rec.update(over)
    return rec


class TestBuildDocx(unittest.TestCase):
    def _run(self, record):
        records = [block("委任契約書2通", order="1", note="ご返送ください"),
                   block("返信用封筒", order="2")]
        with patch.dict("os.environ", _OFFICE_ENV), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=records)):
            return run(build_soufu_annai_docx(record))

    def test_generates_docx_with_all_content(self):
        out = self._run(make_shipping_record())
        self.assertTrue(out.startswith(b"PK"))
        doc = Document(io.BytesIO(out))
        text = "\n".join(p.text for p in doc.paragraphs)
        for expected in ("山田太郎", "委任契約書の送付", "■ 委任契約書2通",
                         "大野法律事務所", "令和", "同封の返信用封筒"):
            self.assertIn(expected, text)
        self.assertNotIn("{{", text, "未置換プレースホルダが残っていない")

    def test_missing_office_info_raises(self):
        empty = {"OFFICE_NAME": "", "OFFICE_ADDRESS": ""}
        records = [block("委任契約書2通"), block("返信用封筒")]
        with patch.dict("os.environ", {**_OFFICE_ENV, **empty}), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=records)):
            with self.assertRaises(SoufuAnnaiError) as ctx:
                run(build_soufu_annai_docx(make_shipping_record()))
        self.assertIn("OFFICE_NAME", str(ctx.exception))

    def test_missing_unit_raises(self):
        with self.assertRaises(SoufuAnnaiError):
            run(build_soufu_annai_docx(make_shipping_record(ユニット種別={"value": ""})))

    def test_adapter_prepare_returns_artifact(self):
        records = [block("委任契約書2通"), block("返信用封筒")]
        with patch.dict("os.environ", _OFFICE_ENV), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=records)):
            result = run(soufu_annai.SoufuAnnaiAdapter().prepare(make_shipping_record()))
        self.assertEqual(len(result.artifacts), 1)
        self.assertEqual(result.artifacts[0].filename, "送付案内.docx")
        self.assertTrue(result.artifacts[0].content.startswith(b"PK"))

    def test_adapter_is_not_registered_yet(self):
        """T2-2 まで CHANNEL_REGISTRY に登録されない（承認済 Webhook で誤起動しない）"""
        import channels
        self.assertIsNone(channels.get_adapter("送付案内"))


class TestBlockSync(unittest.TestCase):
    """App30/32 同期検査（監視項目D）の異常系"""

    _ENV = {"APP_ENCLOSURE": "32", "TOKEN_ENCLOSURE": "d",
            "APP_SHIPPING": "30", "TOKEN_SHIPPING": "d"}

    def _run(self, options, keys, fields_error=None):
        form = {"同封物選択": {"type": "CHECK_BOX",
                               "options": {o: {"label": o, "index": str(i)}
                                           for i, o in enumerate(options)}}}
        records = [{"ブロックキー": {"value": k}} for k in keys]
        get_form = AsyncMock(return_value=form)
        if fields_error:
            get_form = AsyncMock(side_effect=fields_error)
        with patch.dict("os.environ", self._ENV), \
             patch("hub.kintone.get_form_fields", new=get_form), \
             patch("hub.kintone.search_records", new=AsyncMock(return_value=records)):
            return run(check_block_sync())

    def test_key_missing_from_app30_options_is_reported(self):
        problems = self._run(options=["（未設定）"], keys=["委任契約書2通"])
        self.assertEqual(len(problems), 1)
        self.assertIn("委任契約書2通", problems[0])
        self.assertIn("App 30", problems[0])

    def test_synced_keys_pass(self):
        problems = self._run(options=["（未設定）", "委任契約書2通", "返信用封筒"],
                             keys=["委任契約書2通", "返信用封筒"])
        self.assertEqual(problems, [])

    def test_fetch_error_is_reported_as_problem(self):
        problems = self._run(options=[], keys=[],
                             fields_error=kintone.KintoneError(500, "X", "down"))
        self.assertEqual(len(problems), 1)
        self.assertIn("同期検査の実行に失敗", problems[0])

    def test_env_unset_skips(self):
        with patch.dict("os.environ", {"APP_ENCLOSURE": "", "APP_SHIPPING": ""}):
            problems = run(check_block_sync())
        self.assertEqual(problems, [])


if __name__ == "__main__":
    unittest.main()
