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


def _label_offsets_mm(default: tuple[float, float] = (0.0, 0.0)) -> tuple[float, float]:
    """LABEL-PRINT-1 D7: ラベルシート専用の印字オフセット（LABEL_PRINT_OFFSET_X/Y_MM）。
    env 未設定ならレイアウトの既定値（31514 は大野の校正実測 2026-09-09 で +5.0/+1.0 mm・Y は上方向が正）。
    既存の PRINT_OFFSET_*_MM（帳票・レターパック・A4_2x6）とは分離する。"""
    try:
        return (float(os.environ.get("LABEL_PRINT_OFFSET_X_MM", str(default[0]))),
                float(os.environ.get("LABEL_PRINT_OFFSET_Y_MM", str(default[1]))))
    except ValueError:
        logger.warning("LABEL_PRINT_OFFSET_*_MM が数値でないためレイアウト既定値を使用")
        return default


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
                   w_mm: float, h_mm: float, honorific: str = "様",
                   omit_empty_zip: bool = False) -> list[TextAt]:
    """ラベル1面ぶんの項目（郵便番号・住所・宛名）を組み立てる。
    omit_empty_zip（LABEL-PRINT-1 D4・既定 False＝従来どおり）: 郵便番号が空なら 〒 行を出さない。"""
    margin = 6.0
    usable = w_mm - margin * 2
    zip_disp = zip_code if zip_code.startswith("〒") else f"〒{zip_code}"
    display_name = f"{name}　{honorific}" if honorific else name
    items = [
        TextAt(margin, h_mm - 12, zip_disp, font_size=12),
        TextAt(margin, h_mm - 24, address, font_size=11, max_width_mm=usable),
        TextAt(margin, h_mm - 42, display_name, font_size=16, max_width_mm=usable),
    ]
    if omit_empty_zip and not zip_code.strip():
        items = items[1:]
    return items


def render_letterpack_label(to_name: str, to_zip: str, to_address: str,
                            *, honorific: str = "様",
                            size_mm: tuple[float, float] = LETTERPACK_LABEL_MM,
                            grid: bool = False) -> bytes:
    """レターパック「お届け先」欄への貼付サイズ（既定 100×70mm）の宛名ラベル PDF"""
    items = _address_items(to_name, to_zip, to_address, *size_mm, honorific=honorific)
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


def render_letterpack_roundtrip(to_name: str, to_zip: str, to_address: str,
                                *, honorific: str = "様",
                                size_mm: tuple[float, float] = LETTERPACK_LABEL_MM,
                                grid: bool = False) -> bytes:
    """レターパック往復ラベル PDF（2ページ: 1p=宛先、2p=返信用・事務所宛「行」）。
    返信用の宛先は config.get_office_info()（環境変数）から。
    事務所情報が未設定なら ValueError（誤った空ラベルの印刷を防ぐ・render_reply_label と同じ）"""
    office = get_office_info()
    missing = [k for k in ("名称", "郵便番号", "住所") if not office.get(k)]
    if missing:
        raise ValueError(
            f"事務所情報が未設定です: {missing}（環境変数 OFFICE_NAME / OFFICE_ZIP / "
            "OFFICE_ADDRESS を設定してください）"
        )
    font = _ensure_font()
    off_x, off_y = _offsets_mm()
    w_mm, h_mm = size_mm

    pages = [
        _address_items(to_name, to_zip, to_address, w_mm, h_mm, honorific=honorific),
        _address_items(office["名称"], office["郵便番号"], office["住所"],
                       w_mm, h_mm, honorific="行"),
    ]
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(w_mm * mm, h_mm * mm), invariant=1)
    for items in pages:
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


_A4_MM = (210.0, 297.0)


@dataclass(frozen=True)
class LayoutSpec:
    """ラベルシートの面付け仕様（mm）。面順は左上起点・row-major（左→右、次の段）。
    2 つの表現を持つ:
      - 等分（grid なし・A4_2x6）: 面 (col,row) の左下 = (left + col*pitch_x,
        H - top - (row+1)*label_h - row*(pitch_y-label_h))。従来どおり。
      - 格子（grid_x_mm/grid_y_mm あり・31514）: 縦線 x 座標（左端から・昇順）と横線 y 座標（下端から・
        降順＝上段から）の共有線で、面 n（row-major 左上起点）は隣接する縦線 2 本・横線 2 本で囲まれる
        領域（隙間なし・重なりなし）。cols/rows・label_w/h・top/left/pitch は格子から導く（from_grid）。
    offset_env: "print"=既存 PRINT_OFFSET_*_MM／"label"=LABEL_PRINT_OFFSET_*_MM（D7・分離）。"""
    name: str
    cols: int
    rows: int
    label_w_mm: float
    label_h_mm: float
    top_mm: float
    left_mm: float
    pitch_x_mm: float
    pitch_y_mm: float
    corner_r_mm: float = 0.0
    offset_env: str = "label"
    omit_empty_zip: bool = False
    default_offset_mm: tuple[float, float] = (0.0, 0.0)   # env 未設定時のオフセット（label のみ）
    grid_x_mm: tuple[float, ...] | None = None            # 縦線 x（mm・左端から・昇順）
    grid_y_mm: tuple[float, ...] | None = None            # 横線 y（mm・下端から・降順）
    text_h_mm: float | None = None                        # 文字配置の基準高さ（None＝面の高さ）

    def __post_init__(self) -> None:
        if (self.grid_x_mm is None) != (self.grid_y_mm is None):
            raise ValueError("grid_x_mm と grid_y_mm は両方指定する")
        if self.grid_x_mm is not None:
            gx, gy = self.grid_x_mm, self.grid_y_mm
            if len(gx) != self.cols + 1 or len(gy) != self.rows + 1:
                raise ValueError("格子線の本数が cols+1 / rows+1 と一致しない")
            if any(b <= a for a, b in zip(gx, gx[1:])) or any(b >= a for a, b in zip(gy, gy[1:])):
                raise ValueError("縦線は昇順・横線は降順で指定する")

    @classmethod
    def from_grid(cls, name: str, grid_x_mm: tuple[float, ...], grid_y_mm: tuple[float, ...],
                  *, h_mm: float = _A4_MM[1], **kw) -> "LayoutSpec":
        """格子線から仕様を作る。label_w/h・top/left/pitch は面 1（左上）の領域から導く（表示・互換用）。"""
        w = grid_x_mm[1] - grid_x_mm[0]
        h = grid_y_mm[0] - grid_y_mm[1]
        return cls(name, len(grid_x_mm) - 1, len(grid_y_mm) - 1, w, h,
                   h_mm - grid_y_mm[0], grid_x_mm[0], w, h,
                   grid_x_mm=tuple(grid_x_mm), grid_y_mm=tuple(grid_y_mm), **kw)

    @property
    def per_page(self) -> int:
        return self.cols * self.rows

    @property
    def is_grid(self) -> bool:
        return self.grid_x_mm is not None

    def face_box(self, face: int, h_mm: float = _A4_MM[1]) -> tuple[float, float, float, float]:
        """面番号（1 始まり・row-major 左上起点）→ 面の領域 (x0, y0, w, h)（左下原点・mm）。"""
        if not 1 <= face <= self.per_page:
            raise ValueError(f"面番号は 1〜{self.per_page}: {face}")
        i = face - 1
        col, row = i % self.cols, i // self.cols
        if self.grid_x_mm is not None and self.grid_y_mm is not None:
            x0, x1 = self.grid_x_mm[col], self.grid_x_mm[col + 1]
            y1, y0 = self.grid_y_mm[row], self.grid_y_mm[row + 1]
            return x0, y0, x1 - x0, y1 - y0
        x = self.left_mm + col * self.pitch_x_mm
        y = h_mm - self.top_mm - (row + 1) * self.label_h_mm - row * (self.pitch_y_mm - self.label_h_mm)
        return x, y, self.label_w_mm, self.label_h_mm

    def face_origin(self, face: int, h_mm: float = _A4_MM[1]) -> tuple[float, float]:
        """面番号 → 面の左下座標 (x_mm, y_mm)。"""
        return self.face_box(face, h_mm)[:2]


# A4 面付けレイアウト表（ラベルシール規格に合わせて追加可能・既存 A4_2x6 は不変）:
#  - A4_2x6: T1-3 の等分レイアウト（余白 0・面 105×49.5・PRINT_OFFSET・従来どおり）
#  - A4_2x5_aone31514: エーワン 31514（A4・10 面・四辺余白付）。公式製品ページ
#    https://www.a-one.co.jp/product/search/detail.php?id=31514（ラベル 91×55mm・2 列×5 段・F10A4-1）と
#    公式テストプリント用紙 https://www.a-one.co.jp/img_pdf/ZT31514_A.pdf（左余白 14mm・上余白 11mm・
#    ギャップなし＝14+91+91+14=210 / 11+55×5+11=297）を 2026-09-09 に実測して pin。角丸は公式に記載なし（0）。
_SHEET_LAYOUTS: dict[str, LayoutSpec] = {
    "A4_2x6": LayoutSpec("A4_2x6", 2, 6, 105.0, 49.5, 0.0, 0.0, 105.0, 49.5,
                         corner_r_mm=0.0, offset_env="print", omit_empty_zip=False),
    # 大野の校正実測（2026-09-09・普通紙 100%・台紙と重ね）: 1 回目 +3/+3 → 2 回目 +5/+1 → 3 回目 列間 3 mm →
    # 4 回目 枠 93×57 → 5 回目 升目は隙間なく連続しているため、面ごとの個別枠をやめ共有線の格子で表す。
    # 6 回目 格子全体を左に 2 mm → 7 回目 さらに左に 1 mm・上に 1 mm → 8 回目 最下線 15 を固定し横線間隔 56
    # → 9 回目 格子全体を下に 2 mm → 10 回目 外周の線だけ 1 mm 外へ（内側の線は不変）
    # → 11 回目 面ごとの個別枠（上下 +1 mm）を試行 → 12 回目 共有線の格子に戻し横線間隔 57（最上線 294 固定）
    # → 13 回目（最終）格子全体を下に 1 mm。
    # 縦線 x=[12, 106, 200]・横線 y=[293, 236, 179, 122, 65, 8]（mm・左下原点）。各面 94×57・隙間なし・重なりなし。
    # 文字の配置基準は各面の左下から x+6・住所 y+31・宛名 y+13 で不変（text_h_mm=55 を基準高さに固定）。
    # 実測ずれ（旧既定オフセット +5/+1）は格子座標に織り込み済みなので既定オフセットは 0/0
    # （LABEL_PRINT_OFFSET_X/Y_MM で微調整は引き続き可能）。公式台紙の物理寸法（left 14・gap 0）ではなく、
    # 当該プリンタでの実測合致位置を正とする。
    "A4_2x5_aone31514": LayoutSpec.from_grid(
        "A4_2x5_aone31514", (12.0, 106.0, 200.0), (293.0, 236.0, 179.0, 122.0, 65.0, 8.0),
        corner_r_mm=0.0, offset_env="label", omit_empty_zip=True, default_offset_mm=(0.0, 0.0),
        text_h_mm=55.0),
}


def layout_spec(layout: str) -> LayoutSpec:
    if layout not in _SHEET_LAYOUTS:
        raise ValueError(f"未対応レイアウト: {layout}（対応: {sorted(_SHEET_LAYOUTS)}）")
    return _SHEET_LAYOUTS[layout]


def _spec_offsets(spec: LayoutSpec) -> tuple[float, float]:
    return _label_offsets_mm(spec.default_offset_mm) if spec.offset_env == "label" else _offsets_mm()


def _draw_face(c: canvas.Canvas, font: str, spec: LayoutSpec, face: int,
               addr: dict, off_x: float, off_y: float) -> None:
    """面 face の左下を基準に宛名を描く（文字位置は _address_items: 左下から x+6・住所 y+31・宛名 y+13 mm）。"""
    base_x, base_y, w, h = spec.face_box(face)
    if spec.text_h_mm is not None:
        h = spec.text_h_mm                                   # 面の高さを変えても文字位置（左下基準）は動かさない
    items = _address_items(addr.get("宛先名", ""), addr.get("郵便番号", ""),
                           addr.get("住所", ""), w, h,
                           honorific=addr.get("敬称", "様"), omit_empty_zip=spec.omit_empty_zip)
    for item in items:
        size = item.font_size
        if item.max_width_mm is not None:
            size = fit_font_size(item.text, font, size, item.max_width_mm)
        c.setFont(font, size)
        c.drawString((base_x + item.x_mm + off_x) * mm, (base_y + item.y_mm + off_y) * mm, item.text)


def render_label_sheet(addresses: list[dict], layout: str = "A4_2x6",
                       *, grid: bool = False, faces: list[int] | None = None) -> bytes:
    """A4 ラベルシートへの面付け印字（M4 宛名ラベル用）。
    addresses: [{"宛先名": ..., "郵便番号": ..., "住所": ...}, ...]。面数超過は複数ページ。
    faces（LABEL-PRINT-1・任意）: 印字する面番号（1 始まり・row-major 左上起点）を addresses と同じ順で
    与える。指定面以外は空白で 1 ページに収める（面番号は 1〜面数、重複不可）。省略時は従来どおり面 1 から詰める。"""
    spec = layout_spec(layout)
    font = _ensure_font()
    off_x, off_y = _spec_offsets(spec)
    w_mm, h_mm = _A4_MM

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(w_mm * mm, h_mm * mm), invariant=1)
    if faces is not None:
        if len(faces) != len(addresses):
            raise ValueError("faces と addresses の件数が一致しません")
        if len(set(faces)) != len(faces):
            raise ValueError("faces に重複があります")
        if grid:
            _draw_grid(c, w_mm, h_mm)
        for face, addr in zip(faces, addresses):
            _draw_face(c, font, spec, face, addr, off_x, off_y)
        c.showPage()
        c.save()
        return buf.getvalue()

    per_page = spec.per_page
    for page_start in range(0, len(addresses), per_page):
        if grid:
            _draw_grid(c, w_mm, h_mm)
        for i, addr in enumerate(addresses[page_start:page_start + per_page]):
            _draw_face(c, font, spec, i + 1, addr, off_x, off_y)    # 左上の面から順に
        c.showPage()
    c.save()
    return buf.getvalue()


def render_label_calibration(layout: str = "A4_2x5_aone31514") -> bytes:
    """LABEL-PRINT-1 校正ページ: 面の境界線と面番号だけを印字した 1 ページ PDF。
    格子レイアウトは共有線（縦線・横線を各 1 回・個別枠や二重線なし）、等分レイアウトは面ごとの枠線を描く。
    普通紙に 100% で印刷し、ラベル台紙に重ねてずれを確認する（ずれは LABEL_PRINT_OFFSET_*_MM で補正）。"""
    spec = layout_spec(layout)
    font = _ensure_font()
    off_x, off_y = _spec_offsets(spec)
    w_mm, h_mm = _A4_MM
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=(w_mm * mm, h_mm * mm), invariant=1)
    c.setLineWidth(0.3)
    c.setStrokeColorRGB(0.4, 0.4, 0.4)
    if spec.is_grid:
        gx, gy = spec.grid_x_mm, spec.grid_y_mm
        for x in gx:                                              # 縦線: 最下段の下辺〜最上段の上辺
            c.line((x + off_x) * mm, (gy[-1] + off_y) * mm, (x + off_x) * mm, (gy[0] + off_y) * mm)
        for y in gy:                                              # 横線: 左端〜右端
            c.line((gx[0] + off_x) * mm, (y + off_y) * mm, (gx[-1] + off_x) * mm, (y + off_y) * mm)
    else:
        for face in range(1, spec.per_page + 1):
            x, y, w, h = spec.face_box(face, h_mm)
            x, y = x + off_x, y + off_y
            if spec.corner_r_mm > 0:
                c.roundRect(x * mm, y * mm, w * mm, h * mm, spec.corner_r_mm * mm)
            else:
                c.rect(x * mm, y * mm, w * mm, h * mm)
    c.setFont(font, 10)
    for face in range(1, spec.per_page + 1):
        x, y, w, h = spec.face_box(face, h_mm)
        c.drawString((x + off_x + 3) * mm, (y + off_y + h - 6) * mm, f"面 {face}")
    c.setFont(font, 8)
    if spec.is_grid:
        xs = ",".join(f"{v:g}" for v in spec.grid_x_mm)
        ys = ",".join(f"{v:g}" for v in spec.grid_y_mm)
        ws = ",".join(f"{b - a:g}" for a, b in zip(spec.grid_x_mm, spec.grid_x_mm[1:]))
        hs = ",".join(f"{a - b:g}" for a, b in zip(spec.grid_y_mm, spec.grid_y_mm[1:]))
        c.drawString(3 * mm, 6.5 * mm,
                     f"{spec.name}  面幅 [{ws}] 面高 [{hs}]mm"
                     f"  offset ({off_x:+.1f},{off_y:+.1f})mm  実寸100%で印刷")
        c.drawString(3 * mm, 3 * mm, f"縦線 x[{xs}]  横線 y[{ys}]")
    else:
        gap_x = spec.pitch_x_mm - spec.label_w_mm
        gap_y = spec.pitch_y_mm - spec.label_h_mm
        c.drawString(3 * mm, 3 * mm,
                     f"{spec.name}  label {spec.label_w_mm}x{spec.label_h_mm}mm"
                     f"  top {spec.top_mm}  left {spec.left_mm}  gap ({gap_x:.1f},{gap_y:.1f})"
                     f"  offset ({off_x:+.1f},{off_y:+.1f})mm  実寸100%で印刷")
    c.showPage()
    c.save()
    return buf.getvalue()
