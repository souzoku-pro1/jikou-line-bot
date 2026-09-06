"""相談カード（来所用・紙）のスキャン読み取り→App 40 転記 — HOUKI-CARD-READ

起点: App 40 に 相談カード（FILE）を添付し 相談カード読取=読取依頼 にする →
kintone Webhook → houki_card_webhook（POST /souzoku-houki/card/{token}）→
本モジュール run_card_read（専用入口関数・ChannelConfig ではない）。

状態遷移（相談カード読取・DROP_DOWN・fail-closed）:
  読取依頼 --（webhook が $revision CAS で claim）--> 読取中
    --（読取・検証・転記が終わり、読めなかった欄/検証落ち/未転記が 0）--> 読取済
    --（読めなかった欄あり／AI 失敗／ダウンロード失敗／版不一致／添付が読めない
        形式／予期しない例外／claim 後のレコード取得失敗）--> 要確認
        （try/finally: 成功確定前の離脱は必ず要確認）
  読取中（取り残し）--（fix1 HCR-01 reconcile: 更新日時 が STALE_MINUTES より古い
        読取中 への再配送 → 読取中→読取中 を CAS で claim し直す）--> 読取中 → 再実行
        （新しい 読取中 は処理中の可能性 = skip in_flight・作用 0。再実行は空欄のみ
        書込・同名債権者スキップ・実値判定の規律で二重転記しない）
  終端遷移（読取済/要確認）は fix1 HCR-02: 再取得した最新の 相談カード読取 が
        読取中 のときだけ CAS で行う。人が 要確認/未読取/読取依頼 に変えていた場合は
        作用 0（finalize_preempted・通知 1 行）。409 後の再取得でも同じ状態検査
  終端への CAS が失敗（409 以外/再試行超過）→ ログ+通知（houki_card_read_failure）
  claim 世代フェンス（fix2 HCRF1-01）: claim/reclaim のたびに in-memory の世代番号を
        +1 し読取本体へ所有権として渡す。転記直前・終端直前（_finish）・_finalize
        の再取得ごとに「自分の世代 == 現在の世代」を検査し、不一致（reclaim で
        失効した旧処理）は作用 0・通知なし（fenced）。終端成功で所有権を消す
  通知 kind: 読取済/要確認（読めた・検証落ち）= houki_card_read、
  失敗分類（AI/ダウンロード/読めない添付/版不一致/例外/終端失敗）= houki_card_read_failure
  再実行は人が 読取依頼 に戻す運用（空欄のみ書込なので二重実行しても上書きしない）

読み取り: 相談カード の添付を download_file で取得（MAX_FILES 超は 要確認・PDF は
document ブロック〔MAX_PDF_PAGES まで〕・jpeg/png は image・HEIC/その他/上限超は
要確認）→ claude_gateway.create_message_with_fallback（tool_choice 強制・凍結
system prompt〔sha256 pin〕・timeout 60 秒・max_retries 1）→ 閉集合スキーマの
サーバ側検証（キー集合の完全一致）→ 項目ごとの値検証（選択肢閉集合・日付の
完全一致/実在/非未来・電話は数字とハイフン・メールは @・文字数上限）。

転記（すべて既存関数経由・空欄のみ・confidence=high かつ検証通過の値のみ）:
  単一欄 → houki_case_store.apply_hearing_fields(user_id, raw_fields,
  existing=正本レコード)を 1 回（日付 3 欄の整合検証・選択肢閉集合・空欄のみ・
  CAS 収束は既存のまま。existing を必ず渡し create 経路に入らない）。
  債権者名（項目 12・high のみ）→ houki_case_store.append_creditors。
  転記後に正本を再取得し、欄ごとに「書けた/既に値があった/書けなかった」を実値で
  判定（HOUKI-IMG-2-fix2 と同じ規律）。
  書かない欄: 未成年後見関与・死亡日（確定）・起算日・知った日の確定欄・
  response_mode・status。

規律: 相談カードの値はログ・通知に出さない（分類語彙と欄コードのみ）。
"""

import base64
import datetime
import itertools
import logging
import os
import re

import anthropic

from claude_gateway import create_message_with_fallback
from hub import houki_card as hc
from hub import houki_case_store as store
from hub import kintone
from hub import notify
from hub.image_store import detect_format
from hub.redact import emit

logger = logging.getLogger("hub.houki_card_read")

FIELD_CARD = "相談カード"                 # App 40 FILE（form fields API 実測 2026-09-06）
FIELD_STATUS = "相談カード読取"           # App 40 DROP_DOWN（同上）
STATUS_UNREAD = "未読取"
STATUS_REQUESTED = "読取依頼"
STATUS_WORKING = "読取中"
STATUS_DONE = "読取済"
STATUS_REVIEW = "要確認"

FIELD_UPDATED = "更新日時"                # App 40 UPDATED_TIME（分解能 1 分・実測 2026-09-06）
STALE_MINUTES = 10                        # 読取中 の取り残し判定（更新日時 がこれより古い）

# 【単一 worker 前提（既存裁定）】image_intake._send_claims と同型の in-memory 状態。
# uvicorn workers=1（Procfile 実測・test_image_intake が pin）が前提。worker 複数化
# 票では永続 CAS/一意キーによる排他へ置換すること（既知の制約・司令塔裁定でスコープ外）
# _generations: record_id → 現在の所有世代（読取が進行中の間だけ存在）。削除条件
#   （fix4 HCRF3-01: _finish の finally 1 か所に集約）: 終端成功／preempted（担当者の
#   状態変更で終端しなかった）／終端失敗（5xx・通信例外・CAS 再試行超過）のいずれも
#   「自世代が現在の所有者」のときに削除。fenced（他世代が所有）は削除しない。
#   reclaim（claim）は entry の有無に依存せず常に新世代を採番して上書きするので、
#   entry が残っても消えても回収経路には影響しない。record_id ごとの int で肥大しない
# _generation_seq: プロセス全体の単一採番（itertools.count・単調増加・record_id 別
#   カウンタは持たない=削除後の再 claim が旧世代と衝突しない）
_generations: dict[str, int] = {}
_generation_seq = itertools.count(1)


def _next_generation(record_id: str) -> int:
    gen = next(_generation_seq)
    _generations[record_id] = gen
    return gen


def _owns(record_id: str, generation: int | None) -> bool:
    """自分の世代が現在の所有世代か。None は claim を経ない直接呼出（フェンスなし）。
    登録なし（終端済み）や不一致（reclaim 済み）は False。"""
    return generation is None or _generations.get(record_id) == generation
MAX_FILES = 5
MAX_AI_IMAGE_BYTES = 5 * 1024 * 1024      # 1 ファイル上限（API の画像上限と同値）
MAX_PDF_PAGES = 10                        # PDF のページ数上限（Anthropic 仕様 100 頁内・pin）
API_TIMEOUT_SEC = 60.0
API_MAX_RETRIES = 1

# 文字数上限（欄の型に合わせた定数）
MAX_TEXT_CHARS = 100          # 氏名/ふりがな/住所/本籍/電話/メール
MAX_FREE_CHARS = 500          # 自由記述
MAX_CREDITOR_NAME_CHARS = 40
_PHONE_RE = re.compile(r"^[0-9\-]{8,15}$")
_ISO_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_URL_MARKERS = ("http", "://", "www.")

# 書かない欄（転記対象から常に除外）
NEVER_WRITE = frozenset({"未成年後見関与", "死亡日", "起算日_確定",
                         "相続の開始を知った日", "response_mode", "status"})

# ── AI 側（凍結 system prompt・tool スキーマ閉集合） ────────────────────────────
SYSTEM_PROMPT = (
    "あなたは法律事務所の事務補助です。添付は来所時に相談者が手書きで記入した"
    "「相続放棄 相談カード v1」（A4・2 ページ）のスキャンです。項目番号 1〜26 の各項目を"
    "読み取り、read_card ツールで報告してください。\n"
    "- 用紙の版を確認してください。各ページ下部のフッタが「相続放棄 相談カード v1」で"
    "あれば version は v1、別の版なら other、確認できなければ unknown とします。\n"
    "- □ の選択肢は、✓ または ○ が付いたものを選択します（複数に印がある場合は"
    "判読不能として value を null・confidence を low にしてください）。\n"
    "- 判読できない項目は value を null・confidence を low にしてください。\n"
    "- 記入がない（空欄の）項目は value を null・confidence を high にしてください。\n"
    "- 日付は西暦の YYYY-MM-DD 形式にしてください。年月日のいずれかが読めない場合は"
    " null・low にしてください。\n"
    "- 項目 4 で「その他」が選ばれている場合、（　）内の記入を 4_other に入れてください。\n"
    "- 項目 12（債権者）は記入のある行だけを最大 3 件、項目 13（財産）は記入内容を"
    "預貯金・不動産・有価証券の 3 区分に振り分けてください（該当なしは null）。\n"
    "- 用紙全体が判読できない場合は legible を false にしてください。\n"
    "- 記入内容の解釈や法的判断はせず、読み取った内容だけを報告してください。"
)
USER_TEXT = "添付の相談カードを読み取り、read_card で報告してください。"

CONFIDENCES = ("high", "medium", "low")
VERSIONS = ("v1", "other", "unknown")
COURT_DOC = ("あり", "なし", "不明")
KOSEKI = ("あり", "なし", "不明")
MAX_CREDITORS = hc.CREDITOR_ROWS

_ITEM_BY_NUMBER = {it.number: it for it in hc.CARD_ITEMS}


def _value_schema(item: hc.CardItem) -> dict:
    if item.kind in ("text", "free"):
        return {"type": ["string", "null"]}
    if item.kind == "kana_text":
        return {"type": ["object", "null"],
                "properties": {"name": {"type": ["string", "null"]},
                               "kana": {"type": ["string", "null"]}},
                "required": ["name", "kana"], "additionalProperties": False}
    if item.kind == "date":
        return {"type": ["string", "null"], "description": "YYYY-MM-DD"}
    if item.kind in ("choice", "check_only"):
        return {"type": ["string", "null"], "enum": list(item.choices) + [None]}
    if item.kind == "creditors":
        return {"type": "array", "maxItems": MAX_CREDITORS,
                "items": {"type": "object",
                          "properties": {"name": {"type": ["string", "null"]},
                                         "contact": {"type": ["string", "null"]},
                                         "court_document": {"type": "string",
                                                            "enum": list(COURT_DOC)}},
                          "required": ["name", "contact", "court_document"],
                          "additionalProperties": False}}
    raise ValueError(item.kind)


def _item_schema(item: hc.CardItem) -> dict:
    if item.number == 13:
        value = {"type": ["object", "null"],
                 "properties": {"cash_deposit": {"type": ["string", "null"]},
                                "real_estate": {"type": ["string", "null"]},
                                "securities": {"type": ["string", "null"]}},
                 "required": ["cash_deposit", "real_estate", "securities"],
                 "additionalProperties": False}
    else:
        value = _value_schema(item)
    return {"type": "object",
            "properties": {"value": value,
                           "confidence": {"type": "string", "enum": list(CONFIDENCES)}},
            "required": ["value", "confidence"], "additionalProperties": False}


def _build_tool() -> dict:
    props = {str(it.number): _item_schema(it) for it in hc.CARD_ITEMS}
    props["4_other"] = {"type": "object",
                        "properties": {"value": {"type": ["string", "null"]},
                                       "confidence": {"type": "string",
                                                      "enum": list(CONFIDENCES)}},
                        "required": ["value", "confidence"],
                        "additionalProperties": False}
    return {
        "name": "read_card",
        "description": "相続放棄 相談カード v1 の読み取り結果を項目番号ごとに報告する",
        "input_schema": {
            "type": "object",
            "properties": {
                "version": {"type": "string", "enum": list(VERSIONS)},
                "legible": {"type": "boolean"},
                "items": {"type": "object", "properties": props,
                          "required": sorted(props, key=lambda k: (len(k), k)),
                          "additionalProperties": False},
            },
            "required": ["version", "legible", "items"],
            "additionalProperties": False,
        },
    }


READ_CARD_TOOL = _build_tool()
ITEM_KEYS = frozenset(READ_CARD_TOOL["input_schema"]["properties"]["items"]["properties"])


# ── 出力の検証（閉集合スキーマ・キー集合の完全一致。逸脱は None=ai_failed） ───────
def _is_opt_str(v) -> bool:
    return v is None or isinstance(v, str)


def parse_card_report(tool_input) -> dict | None:
    if not isinstance(tool_input, dict) or set(tool_input) != {"version", "legible", "items"}:
        return None
    if tool_input["version"] not in VERSIONS or not isinstance(tool_input["legible"], bool):
        return None
    items = tool_input["items"]
    if not isinstance(items, dict) or set(items) != ITEM_KEYS:
        return None
    out: dict = {}
    for key, entry in items.items():
        if not isinstance(entry, dict) or set(entry) != {"value", "confidence"}:
            return None
        if entry["confidence"] not in CONFIDENCES:
            return None
        v = entry["value"]
        if key == "4_other":
            if not _is_opt_str(v):
                return None
        else:
            item = _ITEM_BY_NUMBER[int(key)]
            if item.number == 13:
                if v is not None and (not isinstance(v, dict)
                                      or set(v) != {"cash_deposit", "real_estate",
                                                    "securities"}
                                      or not all(_is_opt_str(x) for x in v.values())):
                    return None
            elif item.kind == "kana_text":
                if v is not None and (not isinstance(v, dict) or set(v) != {"name", "kana"}
                                      or not all(_is_opt_str(x) for x in v.values())):
                    return None
            elif item.kind == "creditors":
                if not isinstance(v, list) or len(v) > MAX_CREDITORS:
                    return None
                for row in v:
                    if (not isinstance(row, dict)
                            or set(row) != {"name", "contact", "court_document"}
                            or not _is_opt_str(row["name"]) or not _is_opt_str(row["contact"])
                            or row["court_document"] not in COURT_DOC):
                        return None
            elif item.kind in ("choice", "check_only"):
                if v is not None and v not in item.choices:
                    return None
            else:
                if not _is_opt_str(v):
                    return None
        out[key] = {"value": v, "confidence": entry["confidence"]}
    return {"version": tool_input["version"], "legible": tool_input["legible"],
            "items": out}


# ── 値の検証（項目種別ごと） ───────────────────────────────────────────────────
def _clean(s) -> str:
    return str(s or "").strip()


def _valid_text(s: str, limit: int) -> bool:
    return 0 < len(s) <= limit and "\n" not in s and not any(m in s.lower()
                                                            for m in _URL_MARKERS)


def _valid_date(s: str, today: datetime.date | None = None) -> bool:
    if not _ISO_DATE_RE.match(s):
        return False
    try:
        d = datetime.date.fromisoformat(s)
    except ValueError:
        return False
    return d <= (today or datetime.date.today())


def _validate_field(code: str, value: str) -> bool:
    """欄コードごとの検証（閉集合・日付・電話・メール・文字数）。"""
    if code in store.HEARING_CHOICE_FIELDS:
        return value in store.HEARING_CHOICE_FIELDS[code]
    if code in ("死亡日_申告", "死亡を知った日_申告", "相続人と知った日_申告", "生年月日"):
        return _valid_date(value)
    if code == "電話番号":
        return bool(_PHONE_RE.match(value))
    if code == "メールアドレス":
        return "@" in value and _valid_text(value, MAX_TEXT_CHARS)
    if code in ("知った経緯", "日付申告メモ", "財産_負債", "財産_現金預貯金", "財産_不動産",
                "財産_有価証券", "他の相続人", "先順位相続人の状況", "先順位者の放棄状況"):
        return 0 < len(value) <= MAX_FREE_CHARS and not any(m in value.lower()
                                                            for m in _URL_MARKERS)
    return _valid_text(value, MAX_TEXT_CHARS)


def extract_fields(report: dict) -> dict:
    """検証結果: {"fields": {欄コード: 値}（high かつ検証通過）,
    "creditors": [名称…]（high・検証通過）, "court_docs": 件数,
    "low": [欄コード…]（low/medium=判読不能・自信不足。null/high は空欄=対象外）,
    "invalid": [欄コード…]（検証落ち）, "koseki": あり/なし/不明}。"""
    fields: dict = {}
    low: list[str] = []
    invalid: list[str] = []
    creditors: list[str] = []
    court_docs = 0
    koseki = None
    items = report["items"]

    def _put(code: str, value, confidence: str) -> None:
        if code in NEVER_WRITE:
            return
        if confidence != "high":
            low.append(code)             # 判読不能・自信不足（null/low を含む）
            return
        if value is None or _clean(value) == "":
            return                       # 空欄（null/high）= 記入なし・作用 0
        s = _clean(value)
        if _validate_field(code, s):
            fields[code] = s
        else:
            invalid.append(code)

    for it in hc.CARD_ITEMS:
        entry = items[str(it.number)]
        v, conf = entry["value"], entry["confidence"]
        if it.kind == "kana_text":
            name = (v or {}).get("name") if isinstance(v, dict) else None
            kana = (v or {}).get("kana") if isinstance(v, dict) else None
            _put(it.fields[0], name, conf)
            _put(it.fields[1], kana, conf)
        elif it.number == 13:
            parts = v if isinstance(v, dict) else {}
            _put("財産_現金預貯金", parts.get("cash_deposit"), conf)
            _put("財産_不動産", parts.get("real_estate"), conf)
            _put("財産_有価証券", parts.get("securities"), conf)
        elif it.kind == "creditors":
            for row in (v or []):
                name = _clean(row.get("name"))
                if row.get("court_document") == "あり":
                    court_docs += 1
                if conf != "high" or not name:
                    if name or row.get("contact"):
                        low.append("債権者一覧")
                    continue
                if _valid_text(name, MAX_CREDITOR_NAME_CHARS) and name not in creditors:
                    creditors.append(name)
                else:
                    invalid.append("債権者一覧")
        elif it.kind == "check_only":
            koseki = v if (conf == "high" and v in it.choices) else "不明"
        elif it.number == 4:
            _put("続柄", v, conf)
            other = items["4_other"]
            if fields.get("続柄") == "その他":
                _put("続柄その他", other["value"], other["confidence"])
        else:
            _put(it.fields[0], v, conf)
    return {"fields": fields, "creditors": creditors, "court_docs": court_docs,
            "low": sorted(set(low)), "invalid": sorted(set(invalid)),
            "koseki": koseki}


# ── 添付の取得と AI ブロック ───────────────────────────────────────────────────
def _pdf_page_count(data: bytes) -> int | None:
    try:
        import fitz
        with fitz.open(stream=data, filetype="pdf") as d:
            return d.page_count
    except Exception:
        return None


def content_block(data: bytes) -> dict | None:
    """jpeg/png=image・pdf=document（MAX_PDF_PAGES まで）。HEIC/その他/上限超/
    ページ数不明は None（読めない形式）。"""
    if len(data) > MAX_AI_IMAGE_BYTES:
        return None
    fmt = detect_format(data)
    if fmt is None:
        return None
    ext, mime = fmt
    if ext == "pdf":
        pages = _pdf_page_count(data)
        if pages is None or pages > MAX_PDF_PAGES:
            return None
        kind = "document"
    elif ext in ("jpg", "png"):
        kind = "image"
    else:
        return None
    return {"type": kind, "source": {"type": "base64", "media_type": mime,
                                     "data": base64.b64encode(data).decode()}}


async def _call_ai(blocks: list[dict]) -> dict | None:
    client = anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        timeout=API_TIMEOUT_SEC, max_retries=API_MAX_RETRIES)
    response = await create_message_with_fallback(
        client, context="相続放棄・相談カード読取", max_tokens=4096,
        system=SYSTEM_PROMPT, tools=[READ_CARD_TOOL],
        tool_choice={"type": "tool", "name": READ_CARD_TOOL["name"]},
        messages=[{"role": "user",
                   "content": list(blocks) + [{"type": "text", "text": USER_TEXT}]}])
    for block in response.content:
        if block.type == "tool_use" and block.name == READ_CARD_TOOL["name"]:
            return parse_card_report(block.input)
    return None


# ── 通知（申述書型の箇条書き・欄コードのみ・値なし） ──────────────────────────────
def build_notice(record_id: str, result: str, summary: dict) -> str:
    lines = [f"【相談カード読取】案件レコードNo.{record_id}（結果: {result}）"]
    if summary.get("reconciled"):
        lines.append("・取り残しを再実行しました")
    if summary.get("reason"):
        lines.append(f"・{summary['reason']}")
    if summary.get("written"):
        lines.append("・転記した欄: " + ", ".join(summary["written"]))
    if summary.get("preexisting"):
        lines.append("・既に値があり転記しなかった欄: " + ", ".join(summary["preexisting"]))
    if summary.get("low"):
        lines.append("・読めなかった/自信の低い欄: " + ", ".join(summary["low"]))
    if summary.get("invalid"):
        lines.append("・検証に落ちた欄: " + ", ".join(summary["invalid"]))
    if summary.get("unwritten"):
        lines.append("・書けなかった欄: " + ", ".join(summary["unwritten"]))
    if summary.get("creditors_total"):
        lines.append(f"・債権者: 転記 {summary['creditors_added']} 件"
                     f"（裁判所書類あり {summary['court_docs']} 件）")
    if summary.get("koseki"):
        lines.append(f"・戸籍謄本・住民票: {summary['koseki']}")
    if summary.get("version"):
        lines.append(f"・版: {summary['version']}")
    return "\n".join(lines)


async def _notify(text: str, key: str) -> None:
    try:
        await notify.notify_admin_line(text, throttle_key=key,
                                       throttle_on_success_only=True)
    except Exception:
        logger.error("[HOUKI_CARD] notify failed (fixed text)")


# ── 状態遷移（CAS） ───────────────────────────────────────────────────────────
def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def updated_at(record: dict) -> datetime.datetime | None:
    """更新日時（UPDATED_TIME・ISO 8601 UTC）を aware datetime に。不明は None。"""
    raw = store._v(record, FIELD_UPDATED)
    try:
        return datetime.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.timezone.utc)
    except ValueError:
        return None


def is_stale(record: dict, now: datetime.datetime | None = None) -> bool:
    """読取中 の取り残し判定: 更新日時 が STALE_MINUTES より古い。更新日時 が
    読めないときは False（処理中とみなす=fail-closed・自動再実行しない）。"""
    ts = updated_at(record)
    if ts is None:
        return False
    return ((now or _now()) - ts) > datetime.timedelta(minutes=STALE_MINUTES)


async def claim(record: dict) -> int | None:
    """相談カード読取 を 読取中 に（$revision CAS）。勝者は新しい claim 世代（int）・
    敗者は None。読取依頼→読取中（初回 claim）と 読取中→読取中（reconcile の claim
    取り直し。kintone は値が同じでも PUT で revision を進める）の両方に使う。
    世代の +1 は CAS 成功後の同期区間（await なし）で行う（fix2）。"""
    rid = store._v(record, "$id")
    rev = store._v(record, "$revision")
    try:
        await kintone.update_record(store.APP_HOUKI_CASE, rid,
                                    {FIELD_STATUS: STATUS_WORKING}, revision=rev or None)
    except kintone.KintoneConflict:
        return None
    return _next_generation(rid)


async def _finalize(record_id: str, status: str,
                    generation: int | None = None) -> str:
    """終端ステータスの書込。戻り値 done / preempted / fenced / failed。

    fix1 HCR-02: 再取得した最新の 相談カード読取 が 読取中 のときだけ CAS で
    読取済/要確認 へ遷移する。人が別の状態に変えていた場合は作用 0（preempted）。
    fix2: 加えて自分の claim 世代が現在の所有世代であること（不一致は fenced・
    作用 0）。409 は再取得して 1 回再試行し、再取得後も両方を再検査する。
    所有権の削除は行わない（fix4: _finish の finally が全経路で「自世代が所有者なら
    削除」を担う）。"""
    for _attempt in range(2):
        try:
            latest = await kintone.get_record(store.APP_HOUKI_CASE, record_id)
            if not _owns(record_id, generation):
                return "fenced"
            if store._v(latest, FIELD_STATUS) != STATUS_WORKING:
                return "preempted"
            await kintone.update_record(store.APP_HOUKI_CASE, record_id,
                                        {FIELD_STATUS: status},
                                        revision=store._v(latest, "$revision") or None)
            return "done"
        except kintone.KintoneConflict:
            continue
        except Exception:
            return "failed"
    return "failed"


def _log(outcome: str) -> None:
    if outcome == "done":
        logger.info("[HOUKI_CARD] read done")
    elif outcome == "review":
        logger.info("[HOUKI_CARD] read needs review")
    elif outcome == "ai_failed":
        logger.warning("[HOUKI_CARD] ai_failed")
    elif outcome == "download_failed":
        logger.warning("[HOUKI_CARD] download_failed")
    elif outcome == "unreadable_attachment":
        logger.warning("[HOUKI_CARD] unreadable attachment")
    elif outcome == "version_mismatch":
        logger.warning("[HOUKI_CARD] version mismatch")
    elif outcome == "finalize_failed":
        logger.error("[HOUKI_CARD] finalize failed (status may stay working)")
    elif outcome == "finalize_preempted":
        logger.info("[HOUKI_CARD] finalize preempted (status changed by operator)")
    elif outcome == "fenced":
        logger.info("[HOUKI_CARD] fenced (superseded by a newer claim; no effect)")
    else:
        logger.error("[HOUKI_CARD] failed (fixed reason)")


# ── 終端（状態検査つき CAS）と通知 ─────────────────────────────────────────────
async def _finish(record_id: str, outcome: str, summary: dict,
                  generation: int | None = None) -> None:
    try:
        await _finish_inner(record_id, outcome, summary, generation)
    finally:
        # fix4 HCRF3-01: _finish を抜けるすべての経路（終端成功/preempted/終端失敗/
        # 通知中の例外）で「自世代が所有者なら削除」を 1 か所に集約。他世代へ交代
        # していれば削除しない。generation=None（直接呼出）は所有権を持たない
        if generation is not None and _owns(record_id, generation):
            _generations.pop(record_id, None)


async def _finish_inner(record_id: str, outcome: str, summary: dict,
                        generation: int | None) -> None:
    # fix2: 終端直前の世代検査（転記中に reclaim されたケースを含む）。旧処理は
    # 黙って終わる（終端も通知もしない）
    if outcome == "fenced" or not _owns(record_id, generation):
        _log("fenced")
        return
    status = STATUS_DONE if outcome == "done" else STATUS_REVIEW
    result = await _finalize(record_id, status, generation)
    if result == "fenced":
        _log("fenced")
        return
    _log(outcome)
    if result == "preempted":
        _log("finalize_preempted")
        await _notify(
            f"【相談カード読取】案件レコードNo.{record_id}: 担当者の変更を検知したため"
            "終端遷移を行いませんでした。", f"houki_card_read:{record_id}")
        return
    if result == "failed":
        _log("finalize_failed")
        await _notify(
            f"【相談カード読取・要確認】案件レコードNo.{record_id} の読取結果の"
            f"ステータス更新（{status}）に失敗しました。kintone で 相談カード読取 を"
            "確認してください。", f"houki_card_read_failure:{record_id}")
    kind = "houki_card_read" if outcome in ("done", "review") else "houki_card_read_failure"
    await _notify(build_notice(record_id, status, summary), f"{kind}:{record_id}")


async def run_card_read_by_id(record_id: str, reconciled: bool = False,
                              generation: int | None = None) -> str:
    """claim 後の入口（BackgroundTasks から）。claim 直後の正本取得に失敗しても
    要確認へ倒す（fix1 HCR-01: 読取中 の取り残しを作らない）。generation は
    claim が返した世代（所有権・fix2）。"""
    try:
        record = await kintone.get_record(store.APP_HOUKI_CASE, record_id)
    except Exception:
        await _finish(record_id, "failed",
                      {"reason": "claim 後のレコード取得に失敗しました",
                       "reconciled": reconciled}, generation)
        return "failed"
    return await run_card_read(record, reconciled, generation)


# ── 本体（claim 後に呼ぶ・例外は外へ出さず finally で要確認へ） ─────────────────
async def run_card_read(record: dict, reconciled: bool = False,
                        generation: int | None = None) -> str:
    """claim（読取中）済みのレコードで読取→転記→終端。戻り値 done/review/fenced/
    失敗分類。reconciled=True は取り残しの再実行（通知に 1 行足す）。generation は
    claim 世代（None=直接呼出・フェンスなし）。"""
    record_id = store._v(record, "$id")
    user_id = store._v(record, "LINEユーザーID")
    summary: dict = {"reconciled": reconciled}
    outcome = "failed"
    try:
        # 1. 添付の取得と振り分け
        files = (record.get(FIELD_CARD) or {}).get("value") or []
        if len(files) > MAX_FILES:
            summary["reason"] = "添付が多すぎます（上限を超えています）"
            outcome = "unreadable_attachment"
            return outcome
        blocks: list[dict] = []
        for f in files:
            key = (f or {}).get("fileKey") if isinstance(f, dict) else None
            if not key:
                continue
            try:
                data = await kintone.download_file(store.APP_HOUKI_CASE, key)
            except Exception:
                summary["reason"] = "添付のダウンロードに失敗しました"
                outcome = "download_failed"
                return outcome
            block = content_block(data)
            if block is None:
                summary["reason"] = "添付が読めない形式・サイズ・ページ数です"
                outcome = "unreadable_attachment"
                return outcome
            blocks.append(block)
        if not blocks:
            summary["reason"] = "読み取れる添付がありません"
            outcome = "unreadable_attachment"
            return outcome

        # 2. AI
        try:
            report = await _call_ai(blocks)
        except Exception:
            report = None
        if report is None:
            summary["reason"] = "AI の読み取りに失敗しました"
            outcome = "ai_failed"
            return outcome
        summary["version"] = "v1" if report["version"] == "v1" else "不一致"
        if report["version"] != "v1" or not report["legible"]:
            summary["reason"] = ("用紙の版が相談カード v1 ではありません"
                                 if report["version"] != "v1" else "用紙が判読できません")
            outcome = "version_mismatch" if report["version"] != "v1" else "review"
            return outcome

        # 3. 値の検証と転記（既存関数経由・空欄のみ）
        ex = extract_fields(report)
        summary.update({"low": ex["low"], "invalid": ex["invalid"],
                        "court_docs": ex["court_docs"], "koseki": ex["koseki"],
                        "creditors_total": len(ex["creditors"]), "creditors_added": 0})
        preexisting = [c for c in ex["fields"] if store._v(record, c)]
        to_write = {c: v for c, v in ex["fields"].items() if c not in preexisting}
        problems: list[str] = []
        if not _owns(record_id, generation):          # fix2: 転記直前の世代検査（早期離脱）
            outcome = "fenced"
            return outcome
        # fix3 HCRF2-01: CAS 再試行中の所有権検査（各試行の前・409 後の再取得の前）
        def fence() -> bool:
            return _owns(record_id, generation)
        if to_write:
            _rid, problems, _choice = await store.apply_hearing_fields(
                user_id, to_write, record, fence=fence)
            if "fenced" in problems:
                outcome = "fenced"
                return outcome
        if ex["creditors"]:
            latest_for_cred = await kintone.get_record(store.APP_HOUKI_CASE, record_id)
            summary["creditors_added"] = await store.append_creditors(
                record_id, latest_for_cred, ex["creditors"], fence=fence)
            if not fence():
                outcome = "fenced"
                return outcome

        # 4. 実値で判定
        latest = await kintone.get_record(store.APP_HOUKI_CASE, record_id)
        written = [c for c, v in to_write.items() if store._v(latest, c) == v]
        unwritten = [c for c in to_write if c not in written]
        summary.update({"written": sorted(written), "preexisting": sorted(preexisting),
                        "unwritten": sorted(unwritten)})
        if problems:
            summary["reason"] = "日付の整合検証で日付欄を書き込みませんでした"
        clean = not (ex["low"] or ex["invalid"] or unwritten or problems)
        outcome = "done" if clean else "review"
        return outcome
    except Exception:
        summary["reason"] = "読み取り処理で予期しない失敗が起きました"
        outcome = "failed"
        return outcome
    finally:
        await _finish(record_id, outcome, summary, generation)
