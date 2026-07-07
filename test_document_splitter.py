"""document_splitter.py（D1-1 書類分割部品）＋OCRバッチ化のテスト

検証:
区間検証の各分岐（連続違反・重複・被覆漏れ・型不正）・低確信度の分割不能シグナル・
単一区間/1ページの高速パス・5ページ超のバッチOCR（バッチ境界・結合順序・
結合版の契約不変）・断片化（実PDF・ページ数検証・範囲外拒否）・
toolスキーマのキー制約（AST 検査は自動対象化・直接検査は保険）。
Claude / Vision はモック・断片化は pymupdf の実挙動。
"""

import asyncio
import io
import json
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

_DUMMY_ANTHROPIC_KEY = "dummy_key_for_import_only"
os.environ.setdefault("ANTHROPIC_API_KEY", _DUMMY_ANTHROPIC_KEY)
os.environ.update({
    "LINE_CHANNEL_SECRET": "dummy_secret",
    "LINE_CHANNEL_ACCESS_TOKEN": "dummy_token",
    "KINTONE_SUBDOMAIN": "testsub",
    "KINTONE_APP_ID": "21",
    "KINTONE_API_TOKEN": "dummy",
    "SOUZOKU_KINTONE_APP_ID": "26",
    "SOUZOKU_KINTONE_API_TOKEN": "dummy",
    "CLOUDSIGN_CLIENT_ID": "dummy_client",
    "CLOUDSIGN_WEBHOOK_SECRET": "cs_secret",
    "KINTONE_WEBHOOK_TOKEN": "approve_token",
    "DOCUMENT_WEBHOOK_SECRET": "doc_secret",
    "APP_APPROVAL": "29",
    "TOKEN_APPROVAL": "dummy",
    "GOOGLE_VISION_API_KEY": "dummy_vision",
    "HEALTHCHECK_DISABLED": "1",
})

import document_splitter  # noqa: E402
from document_splitter import (  # noqa: E402
    DocumentSplitError,
    SPLIT_TOOL,
    analyze_segments,
    split_pdf,
    validate_segments,
)
import main  # noqa: E402

if os.environ.get("ANTHROPIC_API_KEY") == _DUMMY_ANTHROPIC_KEY:
    del os.environ["ANTHROPIC_API_KEY"]


def run(coro):
    return asyncio.run(coro)


def seg(start, end, doc_type="戸籍", conf=0.95):
    return {"start_page": start, "end_page": end, "doc_type": doc_type,
            "confidence": conf}


class TestValidateSegments(unittest.TestCase):
    def test_valid_multi_segments(self):
        self.assertEqual(validate_segments([seg(1, 3), seg(4, 5, "通帳")], 5), [])

    def test_violations(self):
        cases = [
            ([seg(2, 3), seg(4, 5)], 5, "先頭ページが覆われていません"),
            ([seg(1, 3), seg(3, 5)], 5, "区間の重複"),
            ([seg(1, 2), seg(4, 5)], 5, "区間の不連続"),
            ([seg(1, 3)], 5, "末尾ページが覆われていません"),
            ([seg(3, 2)], 3, "start_page 3 > end_page 2"),
            ([{"start_page": "1", "end_page": 2, "doc_type": "戸籍",
               "confidence": 0.9}], 2, "start_page が整数でない"),
            ([], 3, "segments が空でない配列でない"),
        ]
        for segments, total, expected in cases:
            with self.subTest(expected=expected):
                errors = validate_segments(segments, total)
                self.assertTrue(any(expected in e for e in errors), errors)


class _Block:
    type = "tool_use"
    name = "save_document_segments"

    def __init__(self, segments):
        self.input = {"segments": segments}


def _analyze(page_texts, segments):
    response = MagicMock(content=[_Block(segments)])
    with patch.object(document_splitter, "create_message_with_fallback",
                      new=AsyncMock(return_value=response)), \
            patch.object(document_splitter, "_get_client", new=MagicMock()):
        return run(analyze_segments(page_texts))


class TestAnalyzeSegments(unittest.TestCase):
    PAGES = ["1p 戸籍", "2p 戸籍", "3p 通帳"]

    def test_ok_multi_segments_needs_split(self):
        result = _analyze(self.PAGES, [seg(1, 2), seg(3, 3, "通帳")])
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["needs_split"])
        self.assertEqual([s["doc_type"] for s in result["segments"]],
                         ["戸籍", "通帳"])

    def test_single_segment_is_fast_path(self):
        """単一区間 = 分割不要の判定（needs_split=False・従来経路と同じ扱い）"""
        result = _analyze(self.PAGES, [seg(1, 3)])
        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["needs_split"])

    def test_one_page_skips_ai_call(self):
        """1ページはAI呼び出しなしの高速パス"""
        gateway = AsyncMock()
        with patch.object(document_splitter, "create_message_with_fallback",
                          new=gateway):
            result = run(analyze_segments(["1p"]))
        gateway.assert_not_awaited()
        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["needs_split"])

    def test_invalid_structure_is_unsplittable(self):
        """検証不合格 → 分割不能の明示シグナル（安全側=呼び出し元がaskへ）"""
        result = _analyze(self.PAGES, [seg(1, 1), seg(3, 3)])  # p2 の隙間
        self.assertEqual(result["status"], "unsplittable")
        self.assertIn("区間検証に不合格", result["reason"])
        self.assertIn("不連続", result["reason"])

    def test_low_confidence_is_unsplittable(self):
        result = _analyze(self.PAGES, [seg(1, 2), seg(3, 3, "通帳", conf=0.84)])
        self.assertEqual(result["status"], "unsplittable")
        self.assertIn("閾値 0.85 未満", result["reason"])
        self.assertIn("p3-3", result["reason"])

    def test_threshold_env_override(self):
        with patch.dict(os.environ, {"SORTATION_SPLIT_THRESHOLD": "0.5"}):
            result = _analyze(self.PAGES, [seg(1, 2, conf=0.6), seg(3, 3, conf=0.6)])
        self.assertEqual(result["status"], "ok")

    def test_schema_keys_are_ascii(self):
        import re
        pattern = re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")
        items = SPLIT_TOOL["input_schema"]["properties"]["segments"]["items"]
        for key in items["properties"]:
            self.assertRegex(key, pattern)


def _make_pdf(num_pages: int) -> bytes:
    import fitz
    doc = fitz.open()
    for i in range(num_pages):
        page = doc.new_page()
        page.insert_text((72, 72), f"page {i + 1}")
    return doc.tobytes()


class TestSplitPdf(unittest.TestCase):
    def test_fragments_have_expected_page_counts(self):
        import fitz
        pdf = _make_pdf(5)
        fragments = split_pdf(pdf, [seg(1, 3), seg(4, 5, "通帳")])
        self.assertEqual(len(fragments), 2)
        counts = []
        for data in fragments:
            with fitz.open(stream=data, filetype="pdf") as d:
                counts.append(d.page_count)
        self.assertEqual(counts, [3, 2])

    def test_out_of_range_segment_raises(self):
        pdf = _make_pdf(3)
        with self.assertRaises(DocumentSplitError) as ctx:
            split_pdf(pdf, [seg(1, 4)])
        self.assertIn("範囲外", str(ctx.exception))


class TestBatchedOcr(unittest.TestCase):
    """5ページ超のバッチOCR（Vision 同期APIの5ページ上限対応・D1-1）"""

    def _fake_urlopen(self, captured):
        class _Resp:
            def __init__(self, payload):
                self._payload = payload

            def read(self):
                return json.dumps(self._payload).encode()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake(req, *a, **k):
            body = json.loads(req.data)
            pages = body["requests"][0].get("pages")
            captured.append(pages)
            texts = [{"fullTextAnnotation": {"text": f"p{n}"}}
                     for n in (pages or [1, 2, 3, 4, 5])]
            return _Resp({"responses": [{"responses": texts}]})
        return fake

    def test_12_pages_batched_in_fives_with_order(self):
        captured = []
        with patch("main._pdf_page_count", new=MagicMock(return_value=12)), \
                patch("urllib.request.urlopen", new=self._fake_urlopen(captured)):
            pages = main._ocr_pdf_pages(b"%PDF", "key")
            joined = main._ocr_pdf_bytes(b"%PDF", "key")
        self.assertEqual(captured[:3], [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10],
                                        [11, 12]], "5ページずつのバッチ")
        self.assertEqual(pages, [f"p{n}" for n in range(1, 13)],
                         "バッチ境界をまたいでもページ順が保たれる")
        self.assertEqual(joined, "\n\n".join(f"p{n}" for n in range(1, 13)),
                         "結合版の契約（\\n\\n 結合の str）は不変")

    def test_three_pages_single_batch(self):
        captured = []
        with patch("main._pdf_page_count", new=MagicMock(return_value=3)), \
                patch("urllib.request.urlopen", new=self._fake_urlopen(captured)):
            pages = main._ocr_pdf_pages(b"%PDF", "key")
        self.assertEqual(captured, [[1, 2, 3]])
        self.assertEqual(pages, ["p1", "p2", "p3"])

    def test_page_count_unavailable_degrades_to_single_request(self):
        """PyMuPDF 不在等 → pages 未指定の単発リクエスト（従来動作へ縮退）"""
        captured = []
        with patch("main._pdf_page_count", new=MagicMock(return_value=None)), \
                patch("urllib.request.urlopen", new=self._fake_urlopen(captured)):
            pages = main._ocr_pdf_pages(b"%PDF", "key")
        self.assertEqual(captured, [None], "pages 未指定＝API既定（先頭5ページ）")
        self.assertEqual(len(pages), 5)


if __name__ == "__main__":
    unittest.main()
