"""M1 職務上請求チャネル（channels/shokumu_seikyu・T3-1 = 宛先解決・手数料計算・チェックリスト）

設計: docs/architecture/04-module-01-shokumu-seikyu.md §1-2・§5、02 §3

T3-1 の範囲:
  - チャネル固有データ（request_items / target / purpose）の検証
  - 宛先自治体の App 31（市区町村マスタ）からの引き当て
  - 請求書類種別 × 通数 × App 31 手数料 → 定額小為替の合計金額計算
  - 発送準備チェックリスト PDF の生成（成果物）
  - 住所・手数料が未登録の自治体宛は **エラーにせず「App 31 への登録依頼」警報**
    （PrepareDeferred → 下書き維持。2026-07-03 の指示で設計 04 §5 の「エラー遷移」から変更）

T3-2 で追加: 職務上請求書の複写式重ね打ち PDF・レターパック宛名ラベル（座標実測が前提）
T3-3 で追加: CHANNEL_REGISTRY への登録・状態結線（登録されるまでディスパッチャからは呼ばれない）
"""

import json
import logging

from channels.base import PDF_MIME, Artifact, ChannelAdapter, DispatchResult, PrepareDeferred, PrepareResult
from hub import kintone
from hub.address_label import TextAt, render_overlay

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
                         total: int, breakdown: list[str]) -> bytes:
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
        "　□ 職務上請求書（複写式・記入済み ※重ね打ち帳票は T3-2 実装後に自動生成）",
        f"　□ 定額小為替 {total:,}円分",
        "　□ 返信用レターパック（事務所宛）",
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
        fields = {
            "宛先名": (record.get("宛先名", {}).get("value") or "").strip()
                      or (f"{muni_name}　{dept}" if dept else muni_name),
            "宛先郵便番号": muni.get("郵便番号", {}).get("value", ""),
            "宛先住所": muni.get("住所", {}).get("value", ""),
            "チャネル固有データ": json.dumps(data, ensure_ascii=False),
        }

        checklist = _build_checklist_pdf(record, muni, data, total, breakdown)
        return PrepareResult(
            artifacts=[Artifact("発送準備チェックリスト.pdf", checklist, PDF_MIME)],
            fields=fields,
        )

    async def dispatch(self, record: dict) -> DispatchResult:
        """物理郵送チャネル: 印刷指示のみ（投函・追跡番号入力・発送済への変更は事務員）"""
        return DispatchResult(manual_mailing=True)
