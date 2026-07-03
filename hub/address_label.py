"""宛名ラベル・座標印字エンジン（hub/address_label・T1-3）

設計: docs/architecture/03-common-components.md §7、04 §3.3（キャリブレーション）

- 座標印字エンジンのみを持つ（具体的な座標表・帳票知識はチャネル側の責務）
- 日本語フォント: IPAex ゴシックを assets/fonts/ に同梱（IPA Font License v1.0・
  ライセンス文書同梱）。Railway コンテナのインストール済みフォントに依存しない。
  フォントファイル欠損時は reportlab 内蔵の日本語 CID フォントへフォールバック
- キャリブレーション: 環境変数 PRINT_OFFSET_X_MM / PRINT_OFFSET_Y_MM で全体オフセット。
  grid=True で 5mm 方眼を重ね、試し刷りで用紙と合わせる（運用手順はハブ 04 §3.3）
"""

import io
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

from config import get_office_info

logger = logging.getLogger("hub.address_label")

FONT_NAME = "HubJP"
_FONT_PATH = Path(__file__).resolve().parent.parent / "assets" / "fonts" / "ipaexg.ttf"
_CID_FALLBACK = "HeiseiKakuGo-W5"

_font_source: str | None = None  # "ipaexg" | "cid" （/health 表示用）

# レターパック貼付ラベルの既定サイズ（お届け先欄に収まる 100×70mm）
LETTERPACK_LABEL_MM = (100.0, 70.0)


def _ensure_font() -> str:
    """フォントを登録し、登録済みフォント名を返す（初回のみ実登録・冪等）"""
    global _font_source
    if _font_source is not None:
        return FONT_NAME if _font_source == "ipaexg" else _CID_FALLBACK
    if _FONT_PATH.is_file():
        pdfmetrics.registerFont(TTFont(FONT_NAME, str(_FONT_PATH)))
        _font_source = "ipaexg"
        return FONT_NAME
    logger.warning("同梱フォントが見つからないため CID フォントにフォールバック: %s", _FONT_PATH)
    pdfmetrics.registerFont(UnicodeCIDFont(_CID_FALLBACK))
    _font_source = "cid"
    return _CID_FALLBACK


def font_status() -> str:
    """/health 用: フォントの利用可否と出所"""
    _ensure_font()
    return "ok (ipaexg.ttf 同梱)" if _font_source == "ipaexg" else f"ok (CIDフォールバック: {_CID_FALLBACK})"


def _offsets_mm() -> tuple[float, float]:
    """印字オフセット（プリンタ個体差のキャリブレーション・mm単位）"""
    try:
        return (float(os.environ.get("PRINT_OFFSET_X_MM", "0")),
                float(os.environ.get("PRINT_OFFSET_Y_MM", "0")))
    except ValueError:
        logger.warning("PRINT_OFFSET_*_MM が数値でないため 0 を使用")
        return (0.0, 0.0)


def fit_font_size(text: str, font_name: str, size: float, max_width_mm: float,
                  min_size: float = 6.0) -> float:
    """max_width_mm に収まるまでフォントサイズを縮小する（下限 min_size）"""
    while size > min_size and pdfmetrics.stringWidth(text, font_name, size) > max_width_mm * mm:
        size -= 0.5
    return size


@dataclass
class TextAt:
    """座標印字1項目（座標は用紙左下原点・mm）"""
    x_mm: float
    y_mm: float
    text: str
    font_size: float = 10.5
    max_width_mm: float | None = None  # 指定時、はみ出す長文は縮小


def _draw_grid(c: canvas.Canvas, w_mm: float, h_mm: float) -> None:
    """5mm 方眼＋10mm ごとの座標値（キャリブレーション用の試し刷りモード）"""
    c.saveState()
    c.setStrokeColorRGB(0.75, 0.75, 0.75)
    c.setFillColorRGB(0.55, 0.55, 0.55)
    c.setLineWidth(0.2)
    c.setFont(_ensure_font(), 4)
    x = 0.0
    while x <= w_mm:
        c.line(x * mm, 0, x * mm, h_mm * mm)
        if x % 10 == 0 and x > 0:
            c.drawString(x * mm + 0.5, 1 * mm, str(int(x)))
        x += 5
    y = 0.0
    while y <= h_mm:
        c.line(0, y * mm, w_mm * mm, y * mm)
        if y % 10 == 0 and y > 0:
            c.drawString(0.5, y * mm + 0.5, str(int(y)))
        y += 5
    c.restoreState()


def render_overlay(page_size_mm: tuple[float, float], items: list[TextAt],
                   *, grid: bool = False) -> bytes:
    """白紙 PDF に座標印字する汎用エンジン（重ね打ち・ラベルの下回り）"""
    font = _ensure_font()
    off_x, off_y = _offsets_mm()
    w_mm, h_mm = page_size_mm

    buf = io.BytesIO()
    # invariant=1: 生成時刻等を埋め込まない再現性モード（同一入力→同一バイト列。
    # テストの決定性と成果物の差分比較のため）
    c = canvas.Canvas(buf, pagesize=(w_mm * mm, h_mm * mm), invariant=1)
    if grid:
        _draw_grid(c, w_mm, h_mm)
    for item in items:
        size = item.font_size
        if item.max_width_mm is not None:
            size = fit_font_size(item.text, font, size, item.max_width_mm)
        c.setFont(font, size)
        c.drawString((item.x_mm + off_x) * mm, (item.y_mm + off_y) * mm, item.text)
    c.showPage()
    c.save()
    return buf.getvalue()


def _address_items(name: str, zip_code: str, address: str,
                   w_mm: float, h_mm: float, honorific: str = "様") -> list[TextAt]:
    """ラベル1面ぶんの項目（郵便番号・住所・宛名）を組み立てる"""
    margin = 6.0
    usable = w_mm - margin * 2
    zip_disp = zip_code if zip_code.startswith("〒") else f"〒{zip_code}"
    display_name = f"{name}　{honorific}" if honorific else name
    return [
        TextAt(margin, h_mm - 12, zip_disp, font_size=12),
        TextAt(margin, h_mm - 24, address, font_size=11, max_width_mm=usable),
        TextAt(margin, h_mm - 42, display_name, font_size=16, max_width_mm=usable),
    ]


def render_letterpack_label(to_name: str, to_zip: str, to_address: str,
                            *, size_mm: tuple[float, float] = LETTERPACK_LABEL_MM,
                            grid: bool = False) -> bytes:
    """レターパック「お届け先」欄への貼付サイズ（既定 100×70mm）の宛名ラベル PDF"""
    items = _address_items(to_name, to_zip, to_address, *size_mm)
    return render_overlay(size_mm, items, grid=grid)


def render_reply_label(*, size_mm: tuple[float, float] = LETTERPACK_LABEL_MM,
                       grid: bool = False) -> bytes:
    """返信用（事務所宛）ラベル PDF。宛先は config.get_office_info()（環境変数）から。
    事務所情報が未設定なら ValueError（誤った空ラベルの印刷を防ぐ）"""
    office = get_office_info()
    missing = [k for k in ("名称", "郵便番号", "住所") if not office.get(k)]
    if missing:
        raise ValueError(
            f"事務所情報が未設定です: {missing}（環境変数 OFFICE_NAME / OFFICE_ZIP / "
            "OFFICE_ADDRESS を設定してください）"
        )
    items = _address_items(office["名称"], office["郵便番号"], office["住所"],
                           *size_mm, honorific="行")
    return render_overlay(size_mm, items, grid=grid)


# A4 面付けレイアウト: (列数, 行数)。ラベルシール規格に合わせて追加可能
_SHEET_LAYOUTS = {"A4_2x6": (2, 6)}
_A4_MM = (210.0, 297.0)


def render_label_sheet(addresses: list[dict], layout: str = "A4_2x6",
                       *, grid: bool = False) -> bytes:
    """A4 ラベルシートへの面付け印字（M4 宛名ラベル用）。
    addresses: [{"宛先名": ..., "郵便番号": ..., "住所": ...}, ...]。面数超過は複数ページ"""
    if layout not in _SHEET_LAYOUTS:
        raise ValueError(f"未対応レイアウト: {layout}（対応: {sorted(_SHEET_LAYOUTS)}）")
    cols, rows = _SHEET_LAYOUTS[layout]
    font = _ensure_font()
    off_x, off_y = _offsets_mm()
    w_mm, h_mm = _A4_MM
    cell_w, cell_h = w_mm / cols, h_mm / rows
    per_page = cols * rows

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(w_mm * mm, h_mm * mm), invariant=1)
    for page_start in range(0, len(addresses), per_page):
        if grid:
            _draw_grid(c, w_mm, h_mm)
        for i, addr in enumerate(addresses[page_start:page_start + per_page]):
            col, row = i % cols, i // cols
            base_x = col * cell_w
            base_y = h_mm - (row + 1) * cell_h  # 左上の面から順に
            items = _address_items(addr.get("宛先名", ""), addr.get("郵便番号", ""),
                                   addr.get("住所", ""), cell_w, cell_h,
                                   honorific=addr.get("敬称", "様"))
            for item in items:
                size = item.font_size
                if item.max_width_mm is not None:
                    size = fit_font_size(item.text, font, size, item.max_width_mm)
                c.setFont(font, size)
                c.drawString((base_x + item.x_mm + off_x) * mm,
                             (base_y + item.y_mm + off_y) * mm, item.text)
        c.showPage()
    c.save()
    return buf.getvalue()
