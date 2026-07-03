"""hub/address_label.py（宛名ラベルエンジン）のテスト（T1-3）

- PDF 生成スモーク・ページサイズ（レターパック貼付 100×70mm / A4）
- grid モード（方眼あり/なしで内容が変わる）
- 印字オフセット（環境変数・座標計算と PDF 差分）
- 長文の自動縮小・返信用ラベル（事務所情報 env・未設定エラー）
- 面付けの複数ページ分割
"""

import os
import re
import unittest
from unittest.mock import patch

os.environ.setdefault("KINTONE_SUBDOMAIN", "testsub")

from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics

from hub import address_label
from hub.address_label import (
    LETTERPACK_LABEL_MM,
    TextAt,
    fit_font_size,
    font_status,
    render_label_sheet,
    render_letterpack_label,
    render_overlay,
    render_reply_label,
)

_OFFICE_ENV = {
    "OFFICE_NAME": "大野法律事務所",
    "OFFICE_ZIP": "332-0000",
    "OFFICE_ADDRESS": "埼玉県川口市テスト町1-2-3",
}


def _mediabox_mm(pdf: bytes) -> tuple[float, float]:
    m = re.search(rb"/MediaBox\s*\[\s*0\s+0\s+([\d.]+)\s+([\d.]+)\s*\]", pdf)
    assert m, "MediaBox が見つからない"
    return float(m.group(1)) / mm, float(m.group(2)) / mm


class TestFont(unittest.TestCase):
    def test_bundled_font_is_used(self):
        self.assertIn("ipaexg", font_status())

    def test_font_registration_is_idempotent(self):
        font_status()
        font_status()  # 2回呼んでも例外にならない


class TestRenderOverlay(unittest.TestCase):
    def test_generates_valid_pdf_with_page_size(self):
        pdf = render_overlay((100, 70), [TextAt(10, 10, "テスト")])
        self.assertTrue(pdf.startswith(b"%PDF"))
        w, h = _mediabox_mm(pdf)
        self.assertAlmostEqual(w, 100, delta=0.1)
        self.assertAlmostEqual(h, 70, delta=0.1)

    def test_grid_mode_changes_output(self):
        items = [TextAt(10, 10, "テスト")]
        plain = render_overlay((100, 70), items)
        grid = render_overlay((100, 70), items, grid=True)
        self.assertNotEqual(plain, grid)
        self.assertGreater(len(grid), len(plain), "方眼の描画分だけ大きくなる")

    def test_offset_env_changes_output(self):
        items = [TextAt(10, 10, "テスト")]
        base = render_overlay((100, 70), items)
        with patch.dict("os.environ", {"PRINT_OFFSET_X_MM": "3.5", "PRINT_OFFSET_Y_MM": "-2"}):
            shifted = render_overlay((100, 70), items)
        self.assertNotEqual(base, shifted)

    def test_offset_zero_equals_default(self):
        items = [TextAt(10, 10, "テスト")]
        base = render_overlay((100, 70), items)
        with patch.dict("os.environ", {"PRINT_OFFSET_X_MM": "0", "PRINT_OFFSET_Y_MM": "0"}):
            zero = render_overlay((100, 70), items)
        self.assertEqual(base, zero)

    def test_invalid_offset_falls_back_to_zero(self):
        items = [TextAt(10, 10, "テスト")]
        base = render_overlay((100, 70), items)
        with patch.dict("os.environ", {"PRINT_OFFSET_X_MM": "abc"}):
            fallback = render_overlay((100, 70), items)
        self.assertEqual(base, fallback)


class TestFitFontSize(unittest.TestCase):
    def test_short_text_keeps_size(self):
        font = address_label._ensure_font()
        self.assertEqual(fit_font_size("短い", font, 12, 88), 12)

    def test_long_text_shrinks_to_fit(self):
        font = address_label._ensure_font()
        long_text = "埼玉県川口市非常に長い住所表記のテスト用番地一丁目二番三号"  # 縮小で収まる長さ
        size = fit_font_size(long_text, font, 12, 88)
        self.assertLess(size, 12)
        self.assertLessEqual(pdfmetrics.stringWidth(long_text, font, size), 88 * mm)

    def test_extremely_long_text_stops_at_floor(self):
        """収まりきらない超長文でも下限（6pt）で止まり、無限に縮小しない"""
        font = address_label._ensure_font()
        long_text = "非常に長い住所" * 30
        size = fit_font_size(long_text, font, 12, 88)
        self.assertEqual(size, 6.0)


class TestLetterpackLabel(unittest.TestCase):
    def test_default_size_is_letterpack(self):
        pdf = render_letterpack_label("川口市長", "332-8601", "埼玉県川口市青木2-1-1")
        w, h = _mediabox_mm(pdf)
        self.assertAlmostEqual(w, LETTERPACK_LABEL_MM[0], delta=0.1)
        self.assertAlmostEqual(h, LETTERPACK_LABEL_MM[1], delta=0.1)

    def test_grid_variant(self):
        plain = render_letterpack_label("宛先", "100-0001", "住所")
        grid = render_letterpack_label("宛先", "100-0001", "住所", grid=True)
        self.assertNotEqual(plain, grid)


class TestReplyLabel(unittest.TestCase):
    def test_uses_office_info_from_env(self):
        with patch.dict("os.environ", _OFFICE_ENV):
            pdf = render_reply_label()
        self.assertTrue(pdf.startswith(b"%PDF"))

    def test_missing_office_info_raises(self):
        empty = {"OFFICE_NAME": "", "OFFICE_ZIP": "", "OFFICE_ADDRESS": ""}
        with patch.dict("os.environ", empty):
            with self.assertRaises(ValueError) as ctx:
                render_reply_label()
        self.assertIn("OFFICE_NAME", str(ctx.exception))


class TestLabelSheet(unittest.TestCase):
    def _addr(self, i):
        return {"宛先名": f"宛先{i}", "郵便番号": "100-0001", "住所": f"東京都テスト{i}"}

    def test_a4_page_size(self):
        pdf = render_label_sheet([self._addr(1)])
        w, h = _mediabox_mm(pdf)
        self.assertAlmostEqual(w, 210, delta=0.1)
        self.assertAlmostEqual(h, 297, delta=0.1)

    def test_multi_page_when_over_capacity(self):
        """A4_2x6 = 12面 → 13件で2ページに分割"""
        pdf = render_label_sheet([self._addr(i) for i in range(13)])
        m = re.search(rb"/Count\s+(\d+)", pdf)
        self.assertIsNotNone(m)
        self.assertEqual(int(m.group(1)), 2)

    def test_unknown_layout_raises(self):
        with self.assertRaises(ValueError):
            render_label_sheet([self._addr(1)], layout="A3_9x9")


class TestHealthIntegration(unittest.TestCase):
    def test_health_reports_reportlab(self):
        os.environ.update({
            "LINE_CHANNEL_SECRET": "d", "LINE_CHANNEL_ACCESS_TOKEN": "d",
            "KINTONE_APP_ID": "21", "KINTONE_API_TOKEN": "d",
            "CLOUDSIGN_CLIENT_ID": "d", "CLOUDSIGN_WEBHOOK_SECRET": "d",
            "HEALTHCHECK_DISABLED": "1",
        })
        os.environ.setdefault("ANTHROPIC_API_KEY", "dummy_key_for_import_only")
        from fastapi.testclient import TestClient

        import main
        if os.environ.get("ANTHROPIC_API_KEY") == "dummy_key_for_import_only":
            del os.environ["ANTHROPIC_API_KEY"]
        client = TestClient(main.app)
        deps = client.get("/health").json()["deps"]
        self.assertIn("reportlab", deps)
        self.assertTrue(deps["reportlab"].startswith("ok"),
                        f"reportlab 依存チェックが NG: {deps['reportlab']}")


if __name__ == "__main__":
    unittest.main()
