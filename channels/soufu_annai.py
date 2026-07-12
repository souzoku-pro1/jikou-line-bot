"""M4 送付案内チャネル（channels/soufu_annai・T2-1 = prepare 前半）

設計: docs/architecture/07-module-04-soufu-annai.md §1-2・§5、02 §4

T2-1 の範囲:
  - App 32（同封物ブロックマスタ）からの有効ブロック取得（表示順・ユニット検証）
  - App 30 の 同封物選択 チェックとの突合（未定義キーはエラー = prepare 例外
    → ディスパッチャがエラー遷移＋LINE 警報にする・hub/dispatch §_to_error）
  - 送付案内 docx の生成（hub/docx_builder.fill_template_multiline 経由）
  - App 30/32 の同期検査（daily_healthcheck の監視項目D として登録）

T2-2 で追加（設計 07 §1・§3-4）:
  - AI 特記事項生成（compose_note・失敗時は空欄で続行＝AI は加飾で必須依存にしない）
  - 宛名ラベル PDF の同時出力（宛先面＋返信用の事務所宛面・hub/address_label）
  - dispatch（manual_mailing=印刷指示で停止・投函と発送済への変更は事務員）
  - CHANNEL_REGISTRY への登録（channels/__init__.py）
  - 返送要否は チャネル固有データ に記録（発送済後の返送待ち自動遷移は未実装・
    T1-2 の確定挙動〔発送済 Webhook は skip〕を変えないため。09 実装ノート参照）
"""

import json
import logging
import os
from datetime import date

import anthropic

from channels.base import DOCX_MIME, PDF_MIME, Artifact, ChannelAdapter, DispatchResult, PrepareResult
from claude_gateway import create_message_with_fallback
from config import get_office_info
from hub import kintone
from hub.address_label import render_label_sheet
from hub.docx_builder import fill_template_with_table, resolve_template, to_wareki

logger = logging.getLogger("channels.soufu_annai")

APP_ENCLOSURE = kintone.KintoneApp(
    "App 32 (同封物ブロックマスタ)", "APP_ENCLOSURE", "TOKEN_ENCLOSURE"
)
APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

DOC_TYPE = "送付案内"


class SoufuAnnaiError(Exception):
    """送付案内の組み立てに必要な入力・マスタの不備（エラー遷移＋警報の対象）"""


async def fetch_blocks(unit: str, selected_keys: list[str]) -> list[dict]:
    """App 32 から選択された有効ブロックを表示順で返す。

    - 有効=yes のみ（無効ブロックの選択は「未定義キー」として扱う）
    - 未定義キー（マスタに無い/無効）→ SoufuAnnaiError（02 §4.2 の同期規約違反）
    - 対象ユニット にユニットが含まれないブロック → SoufuAnnaiError（07 §5）
    """
    if not selected_keys:
        raise SoufuAnnaiError("同封物が選択されていません（同封物選択が空）")

    # fields 指定なし＝全フィールド取得（App 32 への列追加〔部数等〕に自動追従。
    # 存在しない列名を fields に並べると kintone がエラーになるため指定しない）
    records = await kintone.search_records(
        APP_ENCLOSURE, '有効 in ("yes") order by 表示順 asc')
    by_key = {r.get("ブロックキー", {}).get("value", ""): r for r in records}

    missing = [k for k in selected_keys if k not in by_key]
    if missing:
        raise SoufuAnnaiError(
            f"同封物選択に未定義のブロックキーがあります: {missing}"
            "（App 32 に有効なレコードがあるか・App 30 の選択肢と同期しているか確認。"
            "同期規約は docs/architecture/02 §4.2）"
        )

    unit_mismatch = [
        k for k in selected_keys
        if unit not in (by_key[k].get("対象ユニット", {}).get("value") or [])
    ]
    if unit_mismatch:
        raise SoufuAnnaiError(
            f"ユニット「{unit}」の対象でないブロックが選択されています: {unit_mismatch}"
        )

    selected = [by_key[k] for k in selected_keys]
    # サーバー側 order by に加えクライアント側でも安定ソート（表示順の同値は選択順）
    selected.sort(key=lambda b: float(b.get("表示順", {}).get("value") or 0))
    return selected


def build_enclosure_text(blocks: list[dict]) -> str:
    """同封物一覧の本文（■ 表示名＋案内文）を改行結合で組み立てる（07 §2）"""
    lines = []
    for b in blocks:
        lines.append(f"■ {b.get('表示名', {}).get('value', '')}")
        note = (b.get("案内文", {}).get("value") or "").strip()
        if note:
            lines.append(f"　{note}")
    return "\n".join(lines)


def _office_signature() -> str:
    """差出人ブロック（事務所書式の並び: 〒→住所→名称→弁護士名→TEL→FAX。env 駆動）"""
    office = get_office_info()
    missing = [k for k in ("名称", "住所") if not office.get(k)]
    if missing:
        raise SoufuAnnaiError(
            f"事務所情報が未設定です: {missing}（環境変数 OFFICE_NAME / OFFICE_ADDRESS 等を"
            "設定してください。対外文書に空の署名は出せません）"
        )
    lines = []
    if office.get("郵便番号"):
        lines.append(f"〒{office['郵便番号']}")
    lines.append(office["住所"])
    lines.append(office["名称"])
    if office.get("弁護士名"):
        lines.append(f"弁護士　{office['弁護士名']}")
    if office.get("電話"):
        lines.append(f"ＴＥＬ：{office['電話']}")
    if office.get("FAX"):
        lines.append(f"ＦＡＸ：{office['FAX']}")
    return "\n".join(lines)


def _build_body(record: dict, needs_return: bool) -> str:
    """本文可変部（{{本文}}）。書式の文体（〜お送りいたします）を踏襲した定型組み立て"""
    lines = ["さて、下記のとおり書類をお送りいたしますので、ご査収のほど"
             "よろしくお願い申し上げます。"]
    if needs_return:
        lines.append("　ご署名・ご押印等が必要な書類につきましては、下表の備考欄をご確認の"
                     "うえ、当事務所までご返送くださいますようお願い申し上げます。")
    # ご依頼者表示: 宛先が依頼者本人のとき・顧客名が空のときは印字しない（自然な書面にする）
    customer = (record.get("顧客名表示用", {}).get("value") or "").strip()
    recipient = (record.get("宛先名", {}).get("value") or "").strip()
    if customer and customer != recipient:
        lines.append(f"（ご依頼者：{customer}　様）")
    return "\n".join(lines)


def _build_table_rows(blocks: list[dict]) -> list[dict]:
    """書類表（No./書類名/部数/備考）の行データ。備考=案内文＋返送要否に応じた文言"""
    rows = []
    for i, b in enumerate(blocks, start=1):
        note = (b.get("案内文", {}).get("value") or "").strip()
        wants_return = (b.get("返送要否", {}).get("value") or "") == "要"
        remarks = [note] if note else []
        if wants_return and "返送" not in note:
            remarks.append("ご返送をお願いいたします")
        count = (b.get("部数", {}).get("value") or "").strip() if isinstance(
            b.get("部数", {}).get("value"), str) else b.get("部数", {}).get("value")
        rows.append({
            "No": str(i),
            "書類名": b.get("表示名", {}).get("value", ""),
            "部数": str(count or "1"),
            "備考": "　".join(remarks),
        })
    return rows


# ── AI 特記事項（07 §3。加飾であり必須依存にしない）──────────────────────────

_COMPOSE_NOTE_TOOL = {
    "name": "compose_note",
    "description": "送付案内に添える特記事項を1〜2文で作成する",
    "input_schema": {
        "type": "object",
        "properties": {
            "note": {"type": "string",
                     "description": "特記事項（敬体・50字×2文以内。不要なら空文字）"},
        },
        "required": ["note"],
    },
}

_NOTE_PROMPT = """\
法律事務所が書類を郵送する際の送付案内（カバーレター）に添える特記事項を作成してください。

【禁則（chat_responder と同じルール）】
- 法的判断・見通しの断定禁止
- 記録にない日付・金額・進捗の創作禁止
- 敬体。50字×2文以内。同封物の扱い方の案内（返送のお願い・記入箇所の案内等）に限る
- 書くべきことがなければ note は空文字にする

件名: {subject}
宛先: {recipient}
同封物:
{enclosures}"""


async def generate_tokki_note(record: dict, blocks: list[dict]) -> str:
    """特記事項の一文を Claude で生成する。**失敗時は空欄を返して続行**
    （対外文書の品質は承認ステップで担保・07 §3。警報も出さない）"""
    try:
        client = anthropic.AsyncAnthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
        response = await create_message_with_fallback(
            client,
            context="送付案内 特記事項生成",
            max_tokens=256,
            tools=[_COMPOSE_NOTE_TOOL],
            tool_choice={"type": "tool", "name": "compose_note"},
            messages=[{"role": "user", "content": _NOTE_PROMPT.format(
                subject=record.get("件名", {}).get("value", ""),
                recipient=record.get("宛先名", {}).get("value", ""),
                enclosures=build_enclosure_text(blocks),
            )}],
        )
        block_ = next((b for b in response.content if b.type == "tool_use"), None)
        note = (block_.input.get("note", "") if block_ else "").strip()
        return note[:120]
    except Exception:
        logger.exception("特記事項の生成に失敗（空欄で続行）")
        return ""


def _needs_return(blocks: list[dict]) -> bool:
    return any((b.get("返送要否", {}).get("value") or "") == "要" for b in blocks)


def _build_labels_pdf(record: dict) -> bytes:
    """宛名ラベル PDF（宛先面＋返信用の事務所宛面・07 §1）"""
    office = get_office_info()
    addresses = [{
        "宛先名": record.get("宛先名", {}).get("value", ""),
        "郵便番号": record.get("宛先郵便番号", {}).get("value", ""),
        "住所": record.get("宛先住所", {}).get("value", ""),
    }, {
        "宛先名": office.get("名称", ""),
        "郵便番号": office.get("郵便番号", ""),
        "住所": office.get("住所", ""),
        "敬称": "行",   # 返信用
    }]
    return render_label_sheet(addresses)


async def build_soufu_annai_docx(record: dict, blocks: list[dict] | None = None,
                                 tokki: str | None = None) -> bytes:
    """発送管理レコードから送付案内 docx を生成する（prepare の中核・事務所正式書式）"""
    unit = record.get("ユニット種別", {}).get("value", "")
    if not unit:
        raise SoufuAnnaiError("ユニット種別が未設定です")
    if blocks is None:
        selected = record.get("同封物選択", {}).get("value") or []
        blocks = await fetch_blocks(unit, list(selected))
    if tokki is None:
        tokki = record.get("本文_特記事項", {}).get("value", "")

    # 宛先ブロックは3行の縦積み: 〒（1行目）→ 住所（2行目）→ 宛名+様（3行目・テンプレ側）
    zip_code = (record.get("宛先郵便番号", {}).get("value") or "").strip()
    address = (record.get("宛先住所", {}).get("value") or "").strip()
    recipient_addr = f"〒{zip_code}\n{address}" if zip_code else address

    template = resolve_template(unit, DOC_TYPE)
    data = {
        "{{日付}}": to_wareki(date.today()),
        "{{依頼者住所}}": recipient_addr,
        "{{依頼者氏名}}": record.get("宛先名", {}).get("value", ""),
        "{{事務所署名ブロック}}": _office_signature(),
        "{{本文}}": _build_body(record, _needs_return(blocks)),
        "{{特記事項}}": tokki,
    }
    return fill_template_with_table(str(template), data, _build_table_rows(blocks))


class SoufuAnnaiAdapter(ChannelAdapter):
    """M4 送付案内（T2-2 完成形・channels/__init__.py で CHANNEL_REGISTRY に登録）"""

    channel_name = "送付案内"
    needs_return = False  # 物理郵送のため dispatch では返送遷移しない（下記 dispatch 参照）

    async def prepare(self, record: dict) -> PrepareResult:
        unit = record.get("ユニット種別", {}).get("value", "")
        if not unit:
            raise SoufuAnnaiError("ユニット種別が未設定です")
        selected = list(record.get("同封物選択", {}).get("value") or [])
        blocks = await fetch_blocks(unit, selected)

        # 特記事項: 人が書いた値を優先。空なら AI 下書き（失敗は空欄で続行）。
        # 生成結果は kintone にも書き戻し、承認前に弁護士が編集できるようにする（07 §1）
        fields: dict = {}
        tokki = (record.get("本文_特記事項", {}).get("value") or "").strip()
        ai_generated = False
        if not tokki:
            tokki = await generate_tokki_note(record, blocks)
            if tokki:
                fields["本文_特記事項"] = tokki
                ai_generated = True

        docx_bytes = await build_soufu_annai_docx(record, blocks=blocks, tokki=tokki)
        labels_pdf = _build_labels_pdf(record)

        fields["チャネル固有データ"] = json.dumps({
            "blocks": selected,                    # prepare 時点のスナップショット（07 §4）
            "needs_return": _needs_return(blocks),
            "ai_note": {"generated": ai_generated},
        }, ensure_ascii=False)

        return PrepareResult(
            artifacts=[Artifact("送付案内.docx", docx_bytes, DOCX_MIME),
                       Artifact("宛名ラベル.pdf", labels_pdf, PDF_MIME)],
            fields=fields,
        )

    async def dispatch(self, record: dict) -> DispatchResult:
        """物理郵送チャネル: 印刷指示のみ（発送処理中で停止し、印刷・封入・投函・
        発送済への変更は事務員。hub/dispatch の manual_mailing 経路）"""
        return DispatchResult(manual_mailing=True)


# ── App 30/32 同期検査（daily_healthcheck 監視項目D・02 §4.2）─────────────────

async def check_block_sync() -> list[str]:
    """App 32 の有効ブロックキー ⊆ App 30『同封物選択』の選択肢 を検証する。
    env 未設定時はスキップ（アプリ存在の検査は監視項目Bが担当）"""
    import os
    if not (os.environ.get("APP_ENCLOSURE") and os.environ.get("APP_SHIPPING")):
        logger.info("block sync check skipped (env unset)")
        return []
    problems: list[str] = []
    try:
        fields = await kintone.get_form_fields(APP_SHIPPING)
        options = set((fields.get("同封物選択", {}).get("options") or {}).keys())
        records = await kintone.search_records(
            APP_ENCLOSURE, '有効 in ("yes")', fields=["ブロックキー"])
    except kintone.KintoneError as e:
        return [f"App30/32 同期検査の実行に失敗（種別: {type(e).__name__}）"]

    for r in records:
        key = r.get("ブロックキー", {}).get("value", "")
        if key and key not in options:
            problems.append(
                f"App 32 のブロックキー「{key}」が App 30『同封物選択』の選択肢にありません"
                "（先に App 30 へ選択肢を追加してください・docs/architecture/02 §4.2）"
            )
    return problems
