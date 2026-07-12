"""M1 職務上請求チャネル（channels/shokumu_seikyu・T3-1 = 宛先解決・手数料計算・チェックリスト）

設計: docs/architecture/04-module-01-shokumu-seikyu.md §1-2・§5、02 §3

T3-1 の範囲:
  - チャネル固有データ（request_items / target / purpose）の検証
  - 宛先自治体の App 31（市区町村マスタ）からの引き当て
  - 請求書類種別 × 通数 × App 31 手数料 → 定額小為替の合計金額計算
  - 発送準備チェックリスト PDF の生成（成果物）
  - 住所・手数料が未登録の自治体宛は **エラーにせず「App 31 への登録依頼」警報**
    （PrepareDeferred → 下書き維持。2026-07-03 の指示で設計 04 §5 の「エラー遷移」から変更）

T3-2 で追加（2026-07-03・同日の追加要件で2様式対応に改訂）:
  - 日弁連統一用紙（複写式）への重ね打ち PDF。**様式第1号（戸籍謄本等・戸籍法10条の2）と
    様式第2号（住民票の写し等・住基法12条の3）の2座標表**（FORM1_COORDS / FORM2_COORDS）を持ち、
    請求書類種別から様式を自動判定。両様式が必要な請求は2枚（以上）生成
    ※座標は設計上の初期値。**実用紙での実測キャリブレーションが未実施**（手順は
    docs/architecture/04a-shokumu-seikyu-calibration.md。方眼 PDF は本モジュールの CLI で生成）
  - 様式1は生年月日必須（欠損→エラー遷移「生年月日が必要です」）・様式2は任意（空欄は非印字）
  - 請求者欄の印字は様式1=既定OFF（印字済み在庫）・様式2=既定ON。print_requester で上書き可
  - レターパック往復ラベル（宛先=App 31 引き当て自治体・返信用=事務所宛「行」）
T3-3 で追加（2026-07-03）: CHANNEL_REGISTRY への登録（channels/__init__.py）・状態結線。
  App 30 で「チャネル=職務上請求」の起票が実際に動く:
    下書き→prepare→承認待ち →（人が承認）→ 発送処理中＋印刷投函指示 LINE
    →（人が投函・発送済に変更）→ 返送待ち＋返送期限自動設定（hub/dispatch._handle_shipped）
    →（M5 受領・将来の T4系が消込）→ 完了。期限超過は return_deadline_check が毎朝監視
"""

import json
import logging
import os
import re
from datetime import date

from channels.base import PDF_MIME, Artifact, ChannelAdapter, DispatchResult, PrepareDeferred, PrepareResult
from config import get_office_info
from hub import kintone
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式）
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


# ── 統一用紙 重ね打ち（設計 04 §3・2様式） ─────────────────────────────────
#
# 職務上請求書は2様式あり、請求書類種別から自動判定して様式別に生成する:
#   様式第1号: 戸籍謄本等職務上請求書（戸籍法10条の2第3項〜第5項）
#              対象: 戸籍謄本・除籍謄本・改製原戸籍。「いずれかに○」のため1枚につき1種別
#              （複数種別は種別ごとに1枚生成）。請求に係る者の生年月日は**必須**（欠損→エラー遷移）
#   様式第2号: 住民票の写し等職務上請求書（住基法12条の3第2項等）
#              対象: 住民票・住民票の除票・戸籍の附票。該当種別すべてに○（1枚に集約）。
#              生年月日は任意（空欄なら印字しないだけ）
#
# 座標は用紙左下原点・mm（render_overlay の流儀）。
# ★下記は設計上の初期値であり、実用紙の実測で確定する（04a の手順で更新）。
# 用紙の版が変わったら FORM_VERSION を上げ、試し刷り確認をリリース手順に含める。

FORM_VERSION = "v0-初期値（2様式・実測未・04a のキャリブレーション手順で確定させる）"
FORM_SIZE_MM = (210.0, 297.0)  # A4（両様式とも）

# チェック印は「レ」・選択丸は「○」（いずれも JIS X 0208 内・IPAex/CID フォールバックの
# 両方で確実に印字できる。U+2713 "✓" は同梱フォントに無くトーフ化しうるため使わない）
CHECK_MARK = "レ"
CIRCLE_MARK = "○"

FORM1 = "様式第1号"  # 戸籍謄本等
FORM2 = "様式第2号"  # 住民票の写し等

FORM_BY_TYPE = {
    "戸籍謄本": FORM1, "除籍謄本": FORM1, "改製原戸籍": FORM1,
    "戸籍の附票": FORM2, "住民票": FORM2, "住民票の除票": FORM2,
}

# 様式1: 種別→「いずれかに○」の丸位置（謄本/抄本の丸は別キー。現対応種別はすべて謄本）
FORM1_TYPE_CIRCLE = {
    "戸籍謄本": "種別丸_戸籍", "除籍謄本": "種別丸_除籍", "改製原戸籍": "種別丸_原戸籍",
}
# 様式2: 種別→(丸位置, 通数位置)
FORM2_TYPE_KEYS = {
    "住民票": ("種別丸_住民票の写し", "通数_住民票"),
    "住民票の除票": ("種別丸_除票の写し", "通数_除票"),
    "戸籍の附票": ("種別丸_附票の写し", "通数_附票"),
}

# 請求者欄の印字既定: 様式1の事務所在庫は請求者欄印字済みのため OFF・様式2は ON。
# レコード単位の上書き: チャネル固有データ print_requester: {"form1": true, "form2": false}
PRINT_REQUESTER_DEFAULT = {FORM1: False, FORM2: True}

# 様式2「基礎証明事項以外の事項」: チャネル固有データ extra_items の文言→チェック位置
_FORM2_EXTRA_KEYS = {
    "世帯主についてその旨": "基礎証明チェック_世帯主の旨",
    "世帯主の氏名及び続柄": "基礎証明チェック_世帯主氏名続柄",
    "本籍又は国籍・地域": "基礎証明チェック_本籍国籍",
    "その他": "基礎証明チェック_その他",
}

FORM1_COORDS: dict[str, tuple[float, float]] = {
    # 宛先・日付
    "宛先自治体名": (25.0, 277.0),     # 「○○長 殿」の空欄（自治体名のみ印字）
    "年月日": (135.0, 285.0),
    # 請求の種別（戸籍・除籍・原戸籍のいずれかに○ / 謄本・抄本のいずれかに○）・通数
    "種別丸_戸籍":   (28.0, 262.0),
    "種別丸_除籍":   (52.0, 262.0),
    "種別丸_原戸籍": (76.0, 262.0),
    "種別丸_謄本":   (112.0, 262.0),
    "種別丸_抄本":   (136.0, 262.0),
    "通数":          (172.0, 262.0),
    # 対象
    "本籍":         (40.0, 246.0),
    "筆頭者氏名":   (40.0, 234.0),
    "請求に係る者_フリガナ": (60.0, 224.0),
    "請求に係る者_氏名":     (60.0, 215.0),
    # 生年月日（元号 M T S H R のいずれかに○ ＋ 年月日）
    "元号丸_明治": (118.0, 215.0),
    "元号丸_大正": (126.0, 215.0),
    "元号丸_昭和": (134.0, 215.0),
    "元号丸_平成": (142.0, 215.0),
    "元号丸_令和": (150.0, 215.0),
    "生年月日_年月日": (158.0, 215.0),
    # 利用目的の種別（1〜3 の該当番号に○。3=受任事件・弁護士の職務上請求の通常形）
    "利用目的丸_1": (25.0, 196.0),
    "利用目的丸_2": (25.0, 188.0),
    "利用目的丸_3": (25.0, 180.0),
    # 種別3の場合の記載事項
    "業務の種類":   (60.0, 172.0),
    "依頼者氏名":   (60.0, 163.0),
    "該当号チェック_1号": (60.0, 154.0),   # 戸籍法10条の2第1項の該当号 □
    "該当号チェック_2号": (95.0, 154.0),
    "該当号チェック_3号": (130.0, 154.0),
    "具体的事由_1行目": (30.0, 143.0),
    "具体的事由_2行目": (30.0, 135.0),
    # 請求者欄（既定は印字OFF＝印字済み用紙在庫を使う）
    "請求者_弁護士会":       (40.0, 100.0),
    "請求者_事務所所在場所": (40.0, 92.0),
    "請求者_事務所名":       (40.0, 84.0),
    "請求者_氏名":           (40.0, 76.0),
    "請求者_電話番号":       (140.0, 84.0),
    "請求者_登録番号":       (140.0, 76.0),
    # 使者欄（窓口提出者。自動印字はしない＝校正用に座標のみ保持）
    "使者_氏名": (40.0, 45.0),
}

FORM2_COORDS: dict[str, tuple[float, float]] = {
    # 宛先・日付
    "宛先自治体名": (25.0, 277.0),
    "年月日": (135.0, 285.0),
    # 請求の種別（該当するものに○）・種別ごとの通数
    "種別丸_住民票の写し":           (28.0, 264.0),
    "種別丸_住民票記載事項証明書":   (75.0, 264.0),  # 現対応種別に該当なし（校正用に保持）
    "種別丸_附票の写し":             (28.0, 256.0),
    "種別丸_除票の写し":             (75.0, 256.0),
    "通数_住民票": (165.0, 264.0),
    "通数_附票":   (165.0, 256.0),
    "通数_除票":   (185.0, 256.0),
    # 対象（住民票系は住所・附票は本籍）
    "住所":             (40.0, 242.0),
    "本籍":             (40.0, 232.0),
    "世帯主筆頭者氏名": (40.0, 222.0),
    # 基礎証明事項以外の事項（□。チャネル固有データ extra_items で指定）
    "基礎証明チェック_世帯主の旨":     (28.0, 212.0),
    "基礎証明チェック_世帯主氏名続柄": (65.0, 212.0),
    "基礎証明チェック_本籍国籍":       (110.0, 212.0),
    "基礎証明チェック_その他":         (150.0, 212.0),
    # 請求に係る者（生年月日は任意）
    "請求に係る者_フリガナ": (60.0, 202.0),
    "請求に係る者_氏名":     (60.0, 193.0),
    "元号丸_明治": (118.0, 193.0),
    "元号丸_大正": (126.0, 193.0),
    "元号丸_昭和": (134.0, 193.0),
    "元号丸_平成": (142.0, 193.0),
    "元号丸_令和": (150.0, 193.0),
    "生年月日_年月日": (158.0, 193.0),
    # 利用目的（3択の□）と内容
    "利用目的チェック_1": (25.0, 178.0),
    "利用目的チェック_2": (25.0, 170.0),
    "利用目的チェック_3": (25.0, 162.0),
    "利用目的の内容_1行目": (30.0, 151.0),
    "利用目的の内容_2行目": (30.0, 143.0),
    "業務の種類": (60.0, 133.0),
    "依頼者氏名": (60.0, 124.0),
    # 請求者欄（既定は印字ON）
    "請求者_弁護士会":       (40.0, 100.0),
    "請求者_事務所所在場所": (40.0, 92.0),
    "請求者_事務所名":       (40.0, 84.0),
    "請求者_氏名":           (40.0, 76.0),
    "請求者_電話番号":       (140.0, 84.0),
    "請求者_登録番号":       (140.0, 76.0),
    "使者_氏名": (40.0, 45.0),
}

FORM_COORDS_BY_FORM = {FORM1: FORM1_COORDS, FORM2: FORM2_COORDS}

# 記入欄の許容幅（mm・キー名共通）。はみ出す長文は fit_font_size で縮小される
_FORM_MAX_WIDTH_MM = {
    "宛先自治体名": 60, "本籍": 140, "住所": 140,
    "筆頭者氏名": 100, "世帯主筆頭者氏名": 100,
    "請求に係る者_フリガナ": 55, "請求に係る者_氏名": 55, "生年月日_年月日": 45,
    "業務の種類": 100, "依頼者氏名": 100,
    "具体的事由_1行目": 170, "具体的事由_2行目": 170,
    "利用目的の内容_1行目": 170, "利用目的の内容_2行目": 170,
    "請求者_弁護士会": 80, "請求者_事務所所在場所": 110,
    "請求者_事務所名": 110, "請求者_氏名": 80,
}

_PURPOSE_WRAP = 42  # 利用目的/具体的事由の1行目に収める文字数（2行目に折り返し）

# 生年月日の元号判定（ISO は和暦へ変換してから元号を切り出す）
_ERA_NAMES = ("明治", "大正", "昭和", "平成", "令和")
_ERA_STARTS = (
    (date(2019, 5, 1), "令和", 2018),
    (date(1989, 1, 8), "平成", 1988),
    (date(1926, 12, 25), "昭和", 1925),
    (date(1912, 7, 30), "大正", 1911),
    (date(1868, 1, 25), "明治", 1867),
)


def _iso_to_wareki(s: str) -> str:
    d = date.fromisoformat(s)
    for start, era, base in _ERA_STARTS:
        if d >= start:
            n = d.year - base
            return f"{era}{'元' if n == 1 else n}年{d.month}月{d.day}日"
    return s


def _split_birthdate(birth: str) -> tuple[str, str]:
    """生年月日文字列 → (元号, 元号以降の年月日文字列)。
    「昭和25年3月15日」「1950-03-15」の両形式に対応。元号を判定できない場合は ("", 原文)"""
    s = (birth or "").strip()
    if not s:
        return "", ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            s = _iso_to_wareki(s)
        except ValueError:
            return "", s
    for era in _ERA_NAMES:
        if s.startswith(era):
            return era, s[len(era):]
    return "", s


def _item(coords: dict, key: str, text: str, font_size: float = 10.5) -> TextAt:
    x, y = coords[key]
    return TextAt(x, y, text, font_size=font_size,
                  max_width_mm=_FORM_MAX_WIDTH_MM.get(key))


def _purpose_kind(data: dict) -> int:
    """利用目的の種別（1〜3）。既定3=受任事件（弁護士の職務上請求の通常形）"""
    k = data.get("purpose_kind")
    return k if k in (1, 2, 3) else 3


def _legal_item(data: dict) -> int:
    """様式1・戸籍法10条の2第1項の該当号（1〜3）。既定1号=権利行使又は義務履行"""
    k = data.get("legal_item")
    return k if k in (1, 2, 3) else 1


def _print_requester(data: dict, form: str) -> bool:
    pr = data.get("print_requester") or {}
    v = pr.get("form1" if form == FORM1 else "form2")
    return PRINT_REQUESTER_DEFAULT[form] if v is None else bool(v)


def _common_items(coords: dict, data: dict, muni: dict) -> list[TextAt]:
    """宛先（○○長 殿の自治体名）と年月日（未指定なら空欄=窓口提出時に記入）"""
    items = []
    name = muni.get("市区町村名", {}).get("value", "")
    if name:
        items.append(_item(coords, "宛先自治体名", name, font_size=12))
    if data.get("request_date"):
        items.append(_item(coords, "年月日", data["request_date"]))
    return items


def _person_items(coords: dict, target: dict) -> list[TextAt]:
    """請求に係る者（フリガナ・氏名・元号丸＋生年月日）。空の項目は印字しない"""
    items = []
    if target.get("フリガナ"):
        items.append(_item(coords, "請求に係る者_フリガナ", target["フリガナ"], font_size=8))
    if target.get("対象者"):
        items.append(_item(coords, "請求に係る者_氏名", target["対象者"]))
    era, rest = _split_birthdate(target.get("生年月日", ""))
    if era:
        items.append(_item(coords, f"元号丸_{era}", CIRCLE_MARK, font_size=12))
    if rest:
        items.append(_item(coords, "生年月日_年月日", rest))
    return items


def _requester_items(coords: dict, data: dict, form: str) -> list[TextAt]:
    """請求者欄（弁護士会・事務所・氏名・登録番号・電話）。印字フラグOFFなら空"""
    if not _print_requester(data, form):
        return []
    office = get_office_info()
    values = {
        "請求者_弁護士会": os.environ.get("OFFICE_BAR_ASSOCIATION", ""),
        "請求者_事務所所在場所": office.get("住所", ""),
        "請求者_事務所名": office.get("名称", ""),
        "請求者_氏名": office.get("弁護士名", ""),
        "請求者_登録番号": os.environ.get("OFFICE_ATTORNEY_REG", ""),
        "請求者_電話番号": office.get("電話", ""),
    }
    return [_item(coords, k, v) for k, v in values.items() if v]


def _client_name(record: dict, data: dict) -> str:
    return data.get("client_name") or record.get("顧客名表示用", {}).get("value", "")


def build_form1_items(record: dict, data: dict, muni: dict, req_item: dict) -> list[TextAt]:
    """様式第1号（戸籍謄本等）1枚ぶんの配置。req_item は request_items の1要素
    （「いずれかに○」のため1枚につき1種別）。生年月日必須の検証は
    build_request_form_pdfs 側で行う（ここに来る時点で検証済み）"""
    c = FORM1_COORDS
    target = data.get("target", {})
    purpose = (data.get("purpose") or "").strip()

    items = _common_items(c, data, muni)
    items.append(_item(c, FORM1_TYPE_CIRCLE[req_item["type"]], CIRCLE_MARK, font_size=14))
    items.append(_item(c, "種別丸_謄本", CIRCLE_MARK, font_size=14))  # 現対応種別はすべて謄本
    items.append(_item(c, "通数", str(req_item["count"])))

    honseki = target.get("本籍", "") or target.get("住所", "")
    if honseki:
        items.append(_item(c, "本籍", honseki))
    if target.get("筆頭者"):
        items.append(_item(c, "筆頭者氏名", target["筆頭者"]))
    items += _person_items(c, target)

    kind = _purpose_kind(data)
    items.append(_item(c, f"利用目的丸_{kind}", CIRCLE_MARK, font_size=14))
    if kind == 3:  # 3の場合のみ: 業務の種類・依頼者・該当号・具体的事由
        items.append(_item(c, "業務の種類", data.get("business_kind", "受任事件の処理")))
        client = _client_name(record, data)
        if client:
            items.append(_item(c, "依頼者氏名", client))
        items.append(_item(c, f"該当号チェック_{_legal_item(data)}号", CHECK_MARK, font_size=12))
        if purpose:
            items.append(_item(c, "具体的事由_1行目", purpose[:_PURPOSE_WRAP]))
            if purpose[_PURPOSE_WRAP:]:
                items.append(_item(c, "具体的事由_2行目", purpose[_PURPOSE_WRAP:]))

    items += _requester_items(c, data, FORM1)
    return items


def build_form2_items(record: dict, data: dict, muni: dict, req_items: list[dict]) -> list[TextAt]:
    """様式第2号（住民票の写し等）1枚ぶんの配置。該当種別すべてに○（1枚に集約）。
    生年月日は任意（空欄なら印字しないだけ・エラーにしない）"""
    c = FORM2_COORDS
    target = data.get("target", {})
    purpose = (data.get("purpose") or "").strip()

    items = _common_items(c, data, muni)
    for req in req_items:
        circle_key, count_key = FORM2_TYPE_KEYS[req["type"]]
        items.append(_item(c, circle_key, CIRCLE_MARK, font_size=14))
        items.append(_item(c, count_key, str(req["count"])))

    if target.get("住所"):
        items.append(_item(c, "住所", target["住所"]))
    if target.get("本籍"):
        items.append(_item(c, "本籍", target["本籍"]))
    setainushi = target.get("世帯主", "") or target.get("筆頭者", "")
    if setainushi:
        items.append(_item(c, "世帯主筆頭者氏名", setainushi))
    for label in data.get("extra_items", []):
        key = _FORM2_EXTRA_KEYS.get(label)
        if key:
            items.append(_item(c, key, CHECK_MARK, font_size=12))
    items += _person_items(c, target)

    items.append(_item(c, f"利用目的チェック_{_purpose_kind(data)}", CHECK_MARK, font_size=12))
    if purpose:
        items.append(_item(c, "利用目的の内容_1行目", purpose[:_PURPOSE_WRAP]))
        if purpose[_PURPOSE_WRAP:]:
            items.append(_item(c, "利用目的の内容_2行目", purpose[_PURPOSE_WRAP:]))
    items.append(_item(c, "業務の種類", data.get("business_kind", "受任事件の処理")))
    client = _client_name(record, data)
    if client:
        items.append(_item(c, "依頼者氏名", client))

    items += _requester_items(c, data, FORM2)
    return items


def build_request_form_pdfs(record: dict, data: dict, muni: dict,
                            *, grid: bool = False) -> list[tuple[str, bytes]]:
    """請求内訳から様式を自動判定し、[(ファイル名, PDFバイト列), ...] を返す。
    - 様式1（戸籍・除籍・原戸籍）: 種別ごとに1枚。**生年月日欠損はエラー**（エラー遷移の対象）
    - 様式2（住民票・除票・附票）: 1枚に集約。生年月日は任意
    - 両様式が必要な請求（例: 戸籍＋附票）は2枚（以上）生成される"""
    req_items = data.get("request_items", [])
    form1_reqs = [r for r in req_items if FORM_BY_TYPE[r["type"]] == FORM1]
    form2_reqs = [r for r in req_items if FORM_BY_TYPE[r["type"]] == FORM2]

    if form1_reqs and not (data.get("target", {}).get("生年月日") or "").strip():
        types = "・".join(r["type"] for r in form1_reqs)
        raise ShokumuSeikyuError(
            f"様式第1号（{types}）には請求に係る者の生年月日が必要です。"
            "チャネル固有データの target.生年月日 を設定してください"
            "（例: \"昭和25年3月15日\" または \"1950-03-15\"）")

    out: list[tuple[str, bytes]] = []
    for req in form1_reqs:
        pdf = render_overlay(FORM_SIZE_MM, build_form1_items(record, data, muni, req), grid=grid)
        out.append((f"職務上請求書_様式1_{req['type']}.pdf", pdf))
    if form2_reqs:
        pdf = render_overlay(FORM_SIZE_MM, build_form2_items(record, data, muni, form2_reqs), grid=grid)
        out.append(("職務上請求書_様式2_住民票等.pdf", pdf))
    return out


def build_calibration_pdf(form: str = FORM1) -> bytes:
    """キャリブレーション用 PDF（様式別）: 5mm 方眼 + その様式の全座標キーをキー名で印字。
    白紙に実寸印刷し統一用紙と透かして座標を実測する（04a 手順1〜3）"""
    coords = FORM_COORDS_BY_FORM[form]
    items = [TextAt(x, y, f"└{key}", font_size=7) for key, (x, y) in coords.items()]
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


def municipality_office_name(name: str) -> str:
    """市区町村名 → 封筒宛先用の施設名。
    川口市→川口市役所 / 千代田区→千代田区役所 / 伊奈町→伊奈町役場 / ○○村→○○村役場。
    該当しない場合はそのまま返す。
    ※App 30 宛先名の自動入力とレターパック宛先ラベル・チェックリスト用。
    　統一用紙の「○○長 殿」（宛先自治体名）には使わない（そちらは自治体名のままが正しい）"""
    n = (name or "").strip()
    if n.endswith(("市", "区")):
        return n + "役所"
    if n.endswith(("町", "村")):
        return n + "役場"
    return n


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
        lines.append(f"{t} {count}通 × {fee:,}円 = {subtotal:,}円（{FORM_BY_TYPE[t]}）")
    if missing:
        raise PrepareDeferred(
            f"市区町村マスタ（App 31）の「{name}」に手数料が未登録です: {', '.join(missing)}。"
            "自治体に確認のうえ登録してください。")
    return total, lines


def _checklist_lines(record: dict, muni: dict, data: dict,
                     total: int, breakdown: list[str],
                     label_note: str = "",
                     form_names: list[str] | None = None) -> list[str]:
    """チェックリストの行組み立て（純関数・テスト対象）。宛先は施設名表記（〜市役所等）"""
    muni_name = muni.get("市区町村名", {}).get("value", "")
    dept = (muni.get("担当部署", {}).get("value") or "").strip()
    target = data.get("target", {})
    lines = [
        "職務上請求 発送準備チェックリスト",
        "",
        f"件名: {record.get('件名', {}).get('value', '')}",
        f"宛先: {municipality_office_name(muni_name)} {dept}".rstrip(),
        f"　　　〒{muni.get('郵便番号', {}).get('value', '')} {muni.get('住所', {}).get('value', '')}",
        f"対象者: {target.get('対象者', '')}　本籍/住所: {target.get('本籍', '') or target.get('住所', '')}",
        f"利用目的: {data.get('purpose', '')}",
        "",
        "【請求内訳と定額小為替】",
        *[f"　・{line}" for line in breakdown],
        f"　小為替 合計: {total:,}円（郵便局で購入・発行手数料は別途）",
        "",
        "【同封物チェック】",
        *[f"　□ {n}（**該当様式**の統一用紙に手差し1枚給紙で印刷・内容確認）"
          for n in (form_names or [])],
        f"　□ 定額小為替 {total:,}円分",
        "　□ 返信用レターパック（事務所宛。「レターパック往復ラベル.pdf」2ページ目を貼付）",
        *([f"　★{label_note}"] if label_note else []),
        "",
        f"備考（App 31）: {(muni.get('備考', {}).get('value') or 'なし')}",
    ]
    return lines


def _build_checklist_pdf(record: dict, muni: dict, data: dict,
                         total: int, breakdown: list[str],
                         label_note: str = "",
                         form_names: list[str] | None = None) -> bytes:
    """発送準備チェックリスト PDF（事務員向け・A4。設計 04 §1 の成果物c）"""
    lines = _checklist_lines(record, muni, data, total, breakdown, label_note, form_names)
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

        # 宛先・小為替合計をレコードへ書き戻し（承認画面で人が確認できる状態にする）。
        # 封筒宛先（宛先名・ラベル）は施設名表記（川口市→川口市役所）。手入力があれば優先
        muni_name = muni.get("市区町村名", {}).get("value", "")
        dept = (muni.get("担当部署", {}).get("value") or "").strip()
        data["kogawase_total"] = total
        office_name = municipality_office_name(muni_name)
        recipient = ((record.get("宛先名", {}).get("value") or "").strip()
                     or (f"{office_name}　{dept}" if dept else office_name))
        recipient_zip = muni.get("郵便番号", {}).get("value", "")
        recipient_addr = muni.get("住所", {}).get("value", "")
        fields = {
            "宛先名": recipient,
            "宛先郵便番号": recipient_zip,
            "宛先住所": recipient_addr,
            "チャネル固有データ": json.dumps(data, ensure_ascii=False),
        }

        # 統一用紙への重ね打ち PDF（T3-2。様式1/2を自動判定・両様式必要なら複数枚。
        # 様式1の生年月日欠損はここで ShokumuSeikyuError → エラー遷移＋警報）
        form_pdfs = build_request_form_pdfs(record, data, muni)

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
                           emit(record.get("$id", {}).get("value", ""),
                                "record_id", "log", "operator"))

        checklist = _build_checklist_pdf(record, muni, data, total, breakdown,
                                         label_note, [n for n, _ in form_pdfs])
        return PrepareResult(
            artifacts=[
                Artifact("発送準備チェックリスト.pdf", checklist, PDF_MIME),
                *[Artifact(name, pdf, PDF_MIME) for name, pdf in form_pdfs],
                Artifact(label_name, label_pdf, PDF_MIME),
            ],
            fields=fields,
        )

    async def dispatch(self, record: dict) -> DispatchResult:
        """物理郵送チャネル: 印刷指示のみ（投函・追跡番号入力・発送済への変更は事務員）。

        以降の流れ（T3-3 結線・hub/dispatch 側）:
        - 発送済（人が設定）→ _handle_shipped が 返送待ち＋返送期限 を自動設定
          （needs_return=True のため。期限=発送日＋ユニット既定日数）
        - 返送待ち→完了 の消込は M5 スキャン受領（将来の T4系）の接続点。
          戸籍等の受領文書をこのレコードに突合して完了させる（設計 08 §3・04 §4）。
          突合不能時は 要確認 → reprocess()（本アダプタでは未実装・基底の no-op）
        - 期限超過は return_deadline_check（毎日 8:00 JST）が警報（状態は変えない）"""
        return DispatchResult(manual_mailing=True)


# ── キャリブレーション用 CLI（04a 手順1で使用） ──────────────────────────────
#   python -m channels.shokumu_seikyu <出力フォルダ>
#   → 様式別の方眼＋座標キー名（2枚）/ 様式別サンプル重ね打ち（2枚・方眼付き）/
#     レターパック方眼 の計5PDFを出力

_SAMPLE_RECORD = {"顧客名表示用": {"value": "山田太郎"}}
_SAMPLE_MUNI = {"市区町村名": {"value": "川口市"}}
_SAMPLE_DATA = {
    # 戸籍謄本（様式1）＋戸籍の附票（様式2）→ 両様式のサンプルが出る
    "request_items": [{"type": "戸籍謄本", "count": 2}, {"type": "戸籍の附票", "count": 1}],
    "target": {"本籍": "埼玉県川口市青木○丁目○番", "住所": "埼玉県川口市青木○丁目○番",
               "筆頭者": "山田一郎", "世帯主": "山田一郎",
               "対象者": "山田花子", "フリガナ": "ヤマダ　ハナコ",
               "生年月日": "昭和25年3月15日"},
    "purpose": "受任事件（消滅時効援用）の通知書送付先調査のため、対象者の現在の住所を確認する必要があるため",
    "request_date": "令和8年7月3日",
    "print_requester": {"form1": True, "form2": True},  # 校正では請求者欄も座標確認する
}

if __name__ == "__main__":
    import sys
    from pathlib import Path

    out_dir = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
    out_dir.mkdir(parents=True, exist_ok=True)
    outputs = {
        "校正1a_様式1_方眼と座標キー.pdf": build_calibration_pdf(FORM1),
        "校正1b_様式2_方眼と座標キー.pdf": build_calibration_pdf(FORM2),
        "校正3_レターパック方眼.pdf": render_letterpack_label(
            "○○市　市民課", "100-0001", "東京都千代田区○○1-2-3", honorific="御中", grid=True),
    }
    for name, pdf in build_request_form_pdfs(_SAMPLE_RECORD, _SAMPLE_DATA, _SAMPLE_MUNI, grid=True):
        prefix = "校正2a_サンプル_" if "様式1" in name else "校正2b_サンプル_"
        outputs[prefix + name.removeprefix("職務上請求書_")] = pdf
    for name, content in outputs.items():
        (out_dir / name).write_bytes(content)
        logger.info("出力: %s", emit(str(out_dir / name), "freetext", "log", "operator"))
    logger.info("座標表: 様式1=%s項目 / 様式2=%s項目",
                emit(len(FORM1_COORDS), "count", "log", "operator"),
                emit(len(FORM2_COORDS), "count", "log", "operator"))
    logger.info("次の手順: docs/architecture/04a-shokumu-seikyu-calibration.md")
