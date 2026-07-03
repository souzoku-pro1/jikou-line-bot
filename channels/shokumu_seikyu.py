"""M1 職務上請求チャネル（channels/shokumu_seikyu・T3-1 = 宛先解決・手数料計算・チェックリスト）

設計: docs/architecture/04-module-01-shokumu-seikyu.md §1-2・§5、02 §3

T3-1 の範囲:
  - チャネル固有データ（request_items / target / purpose）の検証
  - 宛先自治体の App 31（市区町村マスタ）からの引き当て
  - 請求書類種別 × 通数 × App 31 手数料 → 定額小為替の合計金額計算
  - 発送準備チェックリスト PDF の生成（成果物）
  - 住所・手数料が未登録の自治体宛は **エラーにせず「App 31 への登録依頼」警報**
    （PrepareDeferred → 下書き維持。2026-07-03 の指示で設計 04 §5 の「エラー遷移」から変更）

T3-2 で追加（2026-07-03）:
  - 日弁連統一用紙（複写式）への重ね打ち PDF（FORM_COORDS 座標表・§3）
    ※座標は設計上の初期値。**実用紙での実測キャリブレーションが未実施**（手順は
    docs/architecture/04a-shokumu-seikyu-calibration.md。方眼 PDF は本モジュールの CLI で生成）
  - レターパック往復ラベル（宛先=App 31 引き当て自治体・返信用=事務所宛「行」）
T3-3 で追加: CHANNEL_REGISTRY への登録・状態結線（登録されるまでディスパッチャからは呼ばれない）
"""

import json
import logging
import os

from channels.base import PDF_MIME, Artifact, ChannelAdapter, DispatchResult, PrepareDeferred, PrepareResult
from config import get_office_info
from hub import kintone
from hub.address_label import (
    TextAt,
    render_letterpack_label,
    render_letterpack_roundtrip,
    render_overlay,
)

logger = logging.getLogger("channels.shokumu_seikyu")

APP_CITY_MASTER = kintone.KintoneApp(
    "App 31 (市区町村マスタ)", "APP_CITY_MASTER", "TOKEN_CITY_MASTER"
)

# 請求書類種別 → App 31 の手数料フィールド（設計 04 §2）
FEE_FIELD_BY_TYPE = {
    "戸籍謄本": "手数料_戸籍謄本",
    "除籍謄本": "手数料_除籍改製原",
    "改製原戸籍": "手数料_除籍改製原",
    "戸籍の附票": "手数料_附票",
    "住民票": "手数料_住民票",
    "住民票の除票": "手数料_住民票",
}


class ShokumuSeikyuError(Exception):
    """入力データの不備（起票内容の誤り）。エラー遷移＋警報の対象"""


# ── 統一用紙 重ね打ち（設計 04 §3） ─────────────────────────────────────────
#
# 座標は用紙左下原点・mm（render_overlay の流儀）。
# ★下記は設計上の初期値であり、実用紙の実測で確定する（04a の手順で更新）。
# 用紙の版が変わったら FORM_VERSION を上げ、試し刷り確認をリリース手順に含める。

FORM_VERSION = "v0-初期値（実測未・04a のキャリブレーション手順で確定させる）"
FORM_SIZE_MM = (210.0, 297.0)  # A4
# チェック印は「レ」（JIS X 0208 内・IPAex/CID フォールバックの両方で確実に印字できる。
# U+2713 "✓" は同梱フォントに無くトーフ化しうるため使わない）
CHECK_MARK = "レ"

FORM_COORDS: dict[str, tuple[float, float]] = {
    # 請求者欄（弁護士・事務所固定情報）
    "請求者資格チェック_弁護士": (25.0, 272.0),
    "事務所名":       (55.0, 265.0),
    "弁護士氏名":     (55.0, 257.0),
    "弁護士登録番号": (150.0, 257.0),
    "事務所所在地":   (55.0, 249.0),
    "事務所電話":     (150.0, 249.0),
    "請求日":         (150.0, 280.0),
    # 対象者欄
    "対象者本籍":     (45.0, 228.0),
    "対象者筆頭者":   (45.0, 218.0),
    "対象者氏名":     (45.0, 208.0),
    "対象者生年月日": (130.0, 208.0),
    # 請求種別欄（チェック + 通数。左列=戸籍系・右列=附票/住民票系）
    "請求種別チェック_戸籍謄本":   (25.0, 185.0),
    "通数_戸籍謄本":               (85.0, 185.0),
    "請求種別チェック_除籍謄本":   (25.0, 177.0),
    "通数_除籍謄本":               (85.0, 177.0),
    "請求種別チェック_改製原戸籍": (25.0, 169.0),
    "通数_改製原戸籍":             (85.0, 169.0),
    "請求種別チェック_戸籍の附票": (110.0, 185.0),
    "通数_戸籍の附票":             (170.0, 185.0),
    "請求種別チェック_住民票":     (110.0, 177.0),
    "通数_住民票":                 (170.0, 177.0),
    "請求種別チェック_住民票の除票": (110.0, 169.0),
    "通数_住民票の除票":           (170.0, 169.0),
    # 利用目的欄（2行まで。長文は fit_font_size で縮小）
    "利用目的_1行目": (25.0, 140.0),
    "利用目的_2行目": (25.0, 132.0),
    # 依頼者欄
    "依頼者氏名":     (45.0, 115.0),
}

# 記入欄の許容幅（mm）。はみ出す長文は fit_font_size で縮小される
_FORM_MAX_WIDTH_MM = {
    "事務所名": 90, "弁護士氏名": 90, "事務所所在地": 90,
    "対象者本籍": 150, "対象者筆頭者": 100, "対象者氏名": 80,
    "利用目的_1行目": 170, "利用目的_2行目": 170, "依頼者氏名": 100,
}

_PURPOSE_WRAP = 42  # 利用目的の1行目に収める文字数（2行目に折り返し）


def _form_item(key: str, text: str, font_size: float = 10.5) -> TextAt:
    x, y = FORM_COORDS[key]
    return TextAt(x, y, text, font_size=font_size,
                  max_width_mm=_FORM_MAX_WIDTH_MM.get(key))


def build_form_items(record: dict, data: dict) -> list[TextAt]:
    """統一用紙の記入欄に配置する項目を組み立てる（値が空の欄は印字しない）。
    事務所固定情報は env（OFFICE_* / OFFICE_ATTORNEY_REG）から"""
    office = get_office_info()
    target = data.get("target", {})
    purpose = (data.get("purpose") or "").strip()
    items: list[TextAt] = [_form_item("請求者資格チェック_弁護士", CHECK_MARK, font_size=12)]

    plain = {
        "事務所名": office.get("名称", ""),
        "弁護士氏名": office.get("弁護士名", ""),
        "弁護士登録番号": os.environ.get("OFFICE_ATTORNEY_REG", ""),
        "事務所所在地": office.get("住所", ""),
        "事務所電話": office.get("電話", ""),
        "請求日": data.get("request_date", ""),  # 未指定なら空欄（窓口提出時に記入）
        "対象者本籍": target.get("本籍", "") or target.get("住所", ""),
        "対象者筆頭者": target.get("筆頭者", ""),
        "対象者氏名": target.get("対象者", ""),
        "対象者生年月日": target.get("生年月日", ""),
        "利用目的_1行目": purpose[:_PURPOSE_WRAP],
        "利用目的_2行目": purpose[_PURPOSE_WRAP:],
        "依頼者氏名": record.get("顧客名表示用", {}).get("value", ""),
    }
    items += [_form_item(k, v) for k, v in plain.items() if v]

    for item in data.get("request_items", []):
        t = item["type"]
        items.append(_form_item(f"請求種別チェック_{t}", CHECK_MARK, font_size=12))
        items.append(_form_item(f"通数_{t}", str(item["count"])))
    return items


def build_request_form_pdf(record: dict, data: dict, *, grid: bool = False) -> bytes:
    """職務上請求書（統一用紙）への重ね打ち PDF。複写式用紙に手差し1枚給紙で印刷する"""
    return render_overlay(FORM_SIZE_MM, build_form_items(record, data), grid=grid)


def build_calibration_pdf() -> bytes:
    """キャリブレーション用 PDF: 5mm 方眼 + 全座標キーをキー名で印字。
    白紙に実寸印刷し統一用紙と透かして FORM_COORDS を実測する（04a 手順1〜3）"""
    items = [TextAt(x, y, f"└{key}", font_size=7) for key, (x, y) in FORM_COORDS.items()]
    return render_overlay(FORM_SIZE_MM, items, grid=True)


def parse_channel_data(record: dict) -> dict:
    """チャネル固有データ（JSON）を検証して返す（設計 04 §2 のスキーマ）"""
    raw = record.get("チャネル固有データ", {}).get("value") or ""
    try:
        data = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as e:
        raise ShokumuSeikyuError(f"チャネル固有データが JSON として不正です: {e}")

    items = data.get("request_items") or []
    if not items:
        raise ShokumuSeikyuError(
            'チャネル固有データに request_items がありません（例: '
            '{"request_items": [{"type": "戸籍謄本", "count": 1}]}）')
    for item in items:
        t = item.get("type", "")
        if t not in FEE_FIELD_BY_TYPE:
            raise ShokumuSeikyuError(
                f"未対応の請求書類種別: {t!r}（対応: {sorted(set(FEE_FIELD_BY_TYPE))}）")
        count = item.get("count")
        if not isinstance(count, int) or count <= 0:
            raise ShokumuSeikyuError(f"通数が不正です: type={t} count={count!r}（1以上の整数）")
    return data


async def find_municipality(record: dict, data: dict) -> dict:
    """宛先自治体を App 31 から引き当てる。
    キー: チャネル固有データ municipality → 無ければ 宛先名。
    未登録は PrepareDeferred（登録依頼警報・状態は変えない）"""
    name = (data.get("municipality") or record.get("宛先名", {}).get("value") or "").strip()
    if not name:
        raise ShokumuSeikyuError("宛先の市区町村が未指定です（宛先名 か municipality を設定）")

    records = await kintone.search_records(
        APP_CITY_MASTER,
        f'市区町村名 = "{name}" and 有効 in ("yes")',
    )
    if not records:
        raise PrepareDeferred(
            f"市区町村マスタ（App 31）に「{name}」の有効なレコードがありません。"
            "レコードを追加（または市区町村名の表記を確認）してください。")
    muni = records[0]
    if not (muni.get("住所", {}).get("value") or "").strip():
        raise PrepareDeferred(
            f"市区町村マスタ（App 31）の「{name}」に住所が未登録です。"
            "郵便番号・住所（担当部署も分かれば）を登録してください。")
    return muni


def compute_kogawase(items: list[dict], muni: dict) -> tuple[int, list[str]]:
    """請求内訳 × App 31 手数料 → (定額小為替の合計額, 明細行)。
    手数料未登録の種別があれば PrepareDeferred（登録依頼警報）"""
    total = 0
    lines: list[str] = []
    missing: list[str] = []
    name = muni.get("市区町村名", {}).get("value", "")
    for item in items:
        t, count = item["type"], item["count"]
        fee_field = FEE_FIELD_BY_TYPE[t]
        raw = muni.get(fee_field, {}).get("value")
        if raw in (None, ""):
            missing.append(f"{fee_field}（{t}）")
            continue
        fee = int(float(raw))
        subtotal = fee * count
        total += subtotal
        lines.append(f"{t} {count}通 × {fee:,}円 = {subtotal:,}円")
    if missing:
        raise PrepareDeferred(
            f"市区町村マスタ（App 31）の「{name}」に手数料が未登録です: {', '.join(missing)}。"
            "自治体に確認のうえ登録してください。")
    return total, lines


def _build_checklist_pdf(record: dict, muni: dict, data: dict,
                         total: int, breakdown: list[str],
                         label_note: str = "") -> bytes:
    """発送準備チェックリスト PDF（事務員向け・A4。設計 04 §1 の成果物c）"""
    muni_name = muni.get("市区町村名", {}).get("value", "")
    dept = (muni.get("担当部署", {}).get("value") or "").strip()
    target = data.get("target", {})
    lines = [
        "職務上請求 発送準備チェックリスト",
        "",
        f"件名: {record.get('件名', {}).get('value', '')}",
        f"宛先: {muni_name} {dept}".rstrip(),
        f"　　　〒{muni.get('郵便番号', {}).get('value', '')} {muni.get('住所', {}).get('value', '')}",
        f"対象者: {target.get('対象者', '')}　本籍/住所: {target.get('本籍', '') or target.get('住所', '')}",
        f"利用目的: {data.get('purpose', '')}",
        "",
        "【請求内訳と定額小為替】",
        *[f"　・{line}" for line in breakdown],
        f"　小為替 合計: {total:,}円（郵便局で購入・発行手数料は別途）",
        "",
        "【同封物チェック】",
        "　□ 職務上請求書（統一用紙に「職務上請求書_重ね打ち.pdf」を手差し1枚給紙で印刷・内容確認）",
        f"　□ 定額小為替 {total:,}円分",
        "　□ 返信用レターパック（事務所宛。「レターパック往復ラベル.pdf」2ページ目を貼付）",
        *([f"　★{label_note}"] if label_note else []),
        "",
        f"備考（App 31）: {(muni.get('備考', {}).get('value') or 'なし')}",
    ]
    items = []
    y = 280.0
    for i, line in enumerate(lines):
        size = 14 if i == 0 else 10.5
        items.append(TextAt(15, y, line, font_size=size, max_width_mm=180))
        y -= 9 if i == 0 else 7
    return render_overlay((210, 297), items)


class ShokumuSeikyuAdapter(ChannelAdapter):
    """M1 職務上請求（T3-1 時点では CHANNEL_REGISTRY 未登録・登録は T3-3）"""

    channel_name = "職務上請求"
    needs_return = True   # 戸籍等が返送される（返送待ち・期限監視の対象）

    async def prepare(self, record: dict) -> PrepareResult:
        data = parse_channel_data(record)
        muni = await find_municipality(record, data)
        total, breakdown = compute_kogawase(data["request_items"], muni)

        # 宛先・小為替合計をレコードへ書き戻し（承認画面で人が確認できる状態にする）
        muni_name = muni.get("市区町村名", {}).get("value", "")
        dept = (muni.get("担当部署", {}).get("value") or "").strip()
        data["kogawase_total"] = total
        recipient = ((record.get("宛先名", {}).get("value") or "").strip()
                     or (f"{muni_name}　{dept}" if dept else muni_name))
        recipient_zip = muni.get("郵便番号", {}).get("value", "")
        recipient_addr = muni.get("住所", {}).get("value", "")
        fields = {
            "宛先名": recipient,
            "宛先郵便番号": recipient_zip,
            "宛先住所": recipient_addr,
            "チャネル固有データ": json.dumps(data, ensure_ascii=False),
        }

        # 統一用紙への重ね打ち PDF（T3-2。座標は FORM_COORDS・実測補正は 04a 手順）
        form_pdf = build_request_form_pdf(record, data)

        # レターパック往復ラベル（1p=宛先・2p=返信用事務所宛「行」）。
        # 事務所情報（OFFICE_*）未設定時は宛先面のみに縮退し、チェックリストに明記する
        # （prepare を止めるほどの事象ではなく、返信面は手書きで代替できるため）
        try:
            label_pdf = render_letterpack_roundtrip(recipient, recipient_zip,
                                                    recipient_addr, honorific="御中")
            label_name = "レターパック往復ラベル.pdf"
            label_note = ""
        except ValueError:
            label_pdf = render_letterpack_label(recipient, recipient_zip,
                                                recipient_addr, honorific="御中")
            label_name = "レターパック宛名ラベル.pdf"
            label_note = ("返信用ラベル未生成（環境変数 OFFICE_NAME/ZIP/ADDRESS 未設定）。"
                          "返信用レターパックの宛先は手書きしてください")
            logger.warning("office info unset: reply label skipped record=%s",
                           record.get("$id", {}).get("value", ""))

        checklist = _build_checklist_pdf(record, muni, data, total, breakdown, label_note)
        return PrepareResult(
            artifacts=[
                Artifact("発送準備チェックリスト.pdf", checklist, PDF_MIME),
                Artifact("職務上請求書_重ね打ち.pdf", form_pdf, PDF_MIME),
                Artifact(label_name, label_pdf, PDF_MIME),
            ],
            fields=fields,
        )

    async def dispatch(self, record: dict) -> DispatchResult:
        """物理郵送チャネル: 印刷指示のみ（投函・追跡番号入力・発送済への変更は事務員）"""
        return DispatchResult(manual_mailing=True)


# ── キャリブレーション用 CLI（04a 手順1で使用） ──────────────────────────────
#   python -m channels.shokumu_seikyu <出力フォルダ>
#   → 方眼＋座標キー名 / サンプル重ね打ち（方眼付き） / レターパック方眼 の3PDFを出力

_SAMPLE_RECORD = {"顧客名表示用": {"value": "山田太郎"}}
_SAMPLE_DATA = {
    "request_items": [{"type": "戸籍謄本", "count": 2}, {"type": "戸籍の附票", "count": 1}],
    "target": {"本籍": "埼玉県川口市青木○丁目○番", "筆頭者": "山田一郎",
               "対象者": "山田花子", "生年月日": "昭和25年3月15日"},
    "purpose": "受任事件（消滅時効援用）の通知書送付先調査のため、対象者の現在の住所を確認する必要があるため",
    "request_date": "令和8年7月3日",
}

if __name__ == "__main__":
    import sys
    from pathlib import Path

    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "校正1_方眼と座標キー.pdf": build_calibration_pdf(),
        "校正2_サンプル重ね打ち.pdf": build_request_form_pdf(_SAMPLE_RECORD, _SAMPLE_DATA, grid=True),
        "校正3_レターパック方眼.pdf": render_letterpack_label(
            "○○市　市民課", "100-0001", "東京都千代田区○○1-2-3", honorific="御中", grid=True),
    }
    for name, content in outputs.items():
        (out_dir / name).write_bytes(content)
        print(f"出力: {out_dir / name}")
    print(f"座標表: {len(FORM_COORDS)}項目 / 版: {FORM_VERSION}")
    print("次の手順: docs/architecture/04a-shokumu-seikyu-calibration.md")
