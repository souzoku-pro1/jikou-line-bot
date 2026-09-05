"""時効 LINE の書類写真の AI 読解と 2 通目の自動送信 — JIKOU-IMG-2

起点: hub/image_intake.send_receipt_and_close が**時効チャネル**で受領返信の
送信+閉鎖に成功した直後（IMG-1 の受領返信の構造は不変・本票は別の 2 通目）。
相続放棄チャネル・診断フォーム経由の写真は本票の対象外（次票）。

流れ（analyze_and_reply）:
  claim（in-memory・同期区間・単一 worker 前提）
  → App 21 レコード（userId 一致）の 受信書類写真 fileKey 一覧から未解析
    （App 28 の 画像解析済:jikou:{fileKey} 行が無い）を新しい順に最大 MAX_FILES
  → hub.kintone.download_file で取得。jpeg/png=image ブロック・pdf=document
    ブロック（koseki_second_opinion と同型）。heic・その他・MAX_AI_IMAGE_BYTES
    超は AI に送らず「読めない写真」として数える（依存追加なし・縮小変換なし）
  → claude_gateway.create_message_with_fallback（tool_choice 強制・凍結 system
    prompt・timeout 60 秒・max_retries=1）
  → 出力の検証（閉集合スキーマ）→ 債権者行（confidence=high のみ・差し込み値の
    検証）+ 未回答の質問（既知項目台帳 build_known_items・_HEARING_PROMPT_FROZEN
    の ①〜④ 逐語）を凍結テンプレで組み立て
  → 送信直前に pause／停止リスト／人対応を判定 → push（時効チャネル）
  → 送信成功後にのみ App 28 へ解析マーカー（画像解析:jikou:{event_id}・
    message=送った本文）と fileKey ごとの解析済み行（画像解析済:jikou:{fileKey}）
  → 問い合わせ業者名 が空欄のときのみ high の債権者名を CAS で書く／
    court_document が 訴状・支払督促・判決 なら弁護士通知（種別とレコード番号のみ）

規律:
- 相談者へ送る文にモデル出力を差し込む箇所は**債権者名のみ**（検証つき）。
  AI 自由文は送らない。本文はサーバ組立の凍結テンプレ+検証済み差し込みのみで
  モデル自由文を含まないため、長文ゲート（reply_sanitizer.structure_violations）
  は通さない（全文上限 REPLY_MAX_CHARS で別途 fail-closed）
- 写真内容・氏名・金額はログ・通知に出さない（分類語彙+レコード番号のみ）
- App 28 は本モジュールが読む専用の小欄を持たない（line_user_id / role /
  message / category / auto_sent のみ・form fields API 実測 2026-09-05）ため、
  解析済み fileKey は category とは別の固定書式行を fileKey ごとに書く
- heal は実装しない: 再起動で in-flight が消えた場合は未解析のまま残り、次の
  束（次の受領返信成功時）で拾う
"""

import base64
import datetime
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Callable

import anthropic

from chat_responder import build_known_items
from claude_gateway import create_message_with_fallback
from hub import houki_case_store
from hub import kintone
from hub import notify
from hub.autoreply_stoplist import is_suppressed
from hub.image_store import APP_JIKOU_CASE, PHOTO_FIELD, detect_format
from hub.line_channel import HOUKI_CHANNEL, JIKOU_CHANNEL, push_text
from hub.redact import emit

logger = logging.getLogger("hub.image_analysis")

CHANNEL = "jikou"
MAX_FILES = 5                              # 1 回の解析で扱う未解析写真の上限
MAX_AI_IMAGE_BYTES = 5 * 1024 * 1024       # API の 1 枚上限（超過は読めない写真）
API_TIMEOUT_SEC = 60.0
API_MAX_RETRIES = 1
REPLY_MAX_CHARS = 600                      # 全文上限（超過は送らず要確認通知）
STORE_RETRIES = 1                          # 問い合わせ業者名 CAS の 409 再取得回数

_APP_CHATLOG = kintone.KintoneApp(
    "App 28 (チャットログ)", "APP_CHATLOG", "TOKEN_CHATLOG")
ANALYSIS_PREFIX = "画像解析:"        # 実カテゴリ= 画像解析:jikou:{event_id}
ANALYZED_PREFIX = "画像解析済:"      # 実カテゴリ= 画像解析済:jikou:{fileKey}
ANALYZED_MARKER_TEXT = "（画像解析済み）"
_ANALYZED_QUERY_LIMIT = 500

# claim: "jikou:{userId}:{event_id}"（確認→取得は await を挟まない同期区間。
# 単一 worker 前提=IMG-1 _send_claims と同型）
_claims: set[str] = set()

# ── AI 側（凍結 system prompt・tool スキーマ閉集合） ────────────────────────────
SYSTEM_PROMPT = (
    "あなたは法律事務所の事務補助です。督促状・請求書・訴状などの書類写真から、"
    "債権者（お金を請求している側）を特定し、report_creditors ツールで報告して"
    "ください。\n"
    "- 債権譲渡（譲受人が新しい債権者になる。「債権譲渡通知」「譲り受けました」等）"
    "と、代理受任・回収委託（弁護士・司法書士・債権回収会社が債権者の代理として"
    "連絡している。債権者は元のまま）を区別してください。\n"
    "- role は 原債権者／譲受人／代理人／サービサー／不明 のいずれかです。"
    "サービサーは債権回収会社が自ら債権者として請求している場合に使います。\n"
    "- 判読できない・確信が持てない場合は confidence を low にしてください。"
    "書類全体が判読できない場合は legible を false にしてください。\n"
    "- court_document は、裁判所から届いた書類（訴状・支払督促・判決）が写って"
    "いる場合のみその種別、裁判所の書類でなければ なし、判断できなければ 不明 と"
    "してください。\n"
    "- 債務者本人の氏名・住所・金額・口座など、ツールの項目にない情報は一切"
    "出力しないでください。"
)
USER_TEXT = "添付の書類写真を読み取り、report_creditors で報告してください。"

CREDITOR_ROLES = ("原債権者", "譲受人", "代理人", "サービサー", "不明")
CONFIDENCES = ("high", "medium", "low")
COURT_DOCUMENTS = ("訴状", "支払督促", "判決", "なし", "不明")
COURT_NOTIFY_KINDS = frozenset({"訴状", "支払督促", "判決"})
MAX_CREDITORS = 3

REPORT_TOOL = {
    "name": "report_creditors",
    "description": "書類写真から読み取った債権者と裁判所書類の種別を報告する",
    "input_schema": {
        "type": "object",
        "properties": {
            "creditors": {
                "type": "array",
                "maxItems": MAX_CREDITORS,
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string",
                                 "description": "債権者・譲受人・代理人の名称"},
                        "role": {"type": "string", "enum": list(CREDITOR_ROLES)},
                        "confidence": {"type": "string",
                                       "enum": list(CONFIDENCES)},
                    },
                    "required": ["name", "role", "confidence"],
                    "additionalProperties": False,
                },
            },
            "court_document": {"type": "string", "enum": list(COURT_DOCUMENTS)},
            "legible": {"type": "boolean"},
        },
        "required": ["creditors", "court_document", "legible"],
        "additionalProperties": False,
    },
}

# ── 相談者への返信（弁護士文言・凍結・sha256 pin） ────────────────────────────
# JIKOU-IMG-2-fix1/fix3: 弁護士決定の文言へ差し替え（逐語・記号・全角空白・改行・
# 「1．」の全角ピリオドを含めて一字も変えない）。「〇〇」の位置が債権者名の
# 差し込み位置で、鉤括弧「」はテンプレ側に残す。質問文は _HEARING_PROMPT_FROZEN
# の逐語ではなく IMG-2 固有の凍結文（test_image_analysis が sha256 pin）
IMG2_HEADER = "お写真をありがとうございます。"
CREDITOR_CONFIRM = "誤りがある場合は、正しい名称をお知らせください。"
QUESTIONS_LEAD = "あわせて、次の点について、分かる範囲で教えてください。"
IMG2_REPLY_TEMPLATE = (
    IMG2_HEADER + "\n"
    "{債権者行}\n"
    + CREDITOR_CONFIRM + "\n"
    + QUESTIONS_LEAD + "\n"
    "{質問}"
)
# (a) 債権者名が読み取れなかった場合（質問のみ版）: 2 行目を置き換え、
#     「あわせて、」の行は「次の点について、…」に置き換える（司令塔案）
NO_CREDITOR_LINE = "お写真からは、債権者名を確認できませんでした。"
QUESTIONS_LEAD_NO_CREDITOR = "次の点について、分かる範囲で教えてください。"
IMG2_REPLY_TEMPLATE_NO_CREDITOR = (
    IMG2_HEADER + "\n"
    + NO_CREDITOR_LINE + "\n"
    + QUESTIONS_LEAD_NO_CREDITOR + "\n"
    "{質問}"
)
# 債権者行（2 行目。3 行目 CREDITOR_CONFIRM は 4 文型共通）
CREDITOR_LINE_SINGLE = "お写真からは、債権者名が「{A}」と読み取れました。"
# (b) 複数（2〜3 社）: {LIST}=「A」と「B」（3 社は「A」と「B」と「C」）
CREDITOR_LINE_MULTI = "お写真からは、債権者名が{LIST}と読み取れました。"
# (c) 譲渡
CREDITOR_LINE_ASSIGNED = ("お写真からは、債権者名が「{譲受人}」（「{原債権者}」から"
                          "債権を譲り受けた会社）と読み取れました。")
# (d) 代理人
CREDITOR_LINE_AGENT = ("お写真からは、債権者名が「{債権者}」（ご連絡元の「{代理人}」"
                       "は債権者の代理人）と読み取れました。")

# 質問ブロック（弁護士文言・逐語。番号は振り直さない。①は司令塔案=債権者行が
# 無い場合のみ ②③④ の前に置く）
QUESTION_1 = ("① 債権者（業者）の名称\n"
              "債権回収会社や法律事務所から通知が届いている場合は、その名称も"
              "お知らせください。")
QUESTION_2 = ("② おおよその借入時期\n"
              "不明な場合は「不明」とお答えください。")
QUESTION_3 = ("③ おおよその最終返済日\n"
              "不明な場合は「不明」とお答えください。\n"
              "また、過去5年以内に返済したことがあるかどうかも教えてください。")
QUESTION_4 = ("④ 過去10年以内に、次のような書類が届いたことはありますか？\n"
              "1．訴状\n"
              "2．支払督促\n"
              "3．その他の督促状や通知書\n"
              "4．何も届いていない\n"
              "5．不明\n"
              "該当する番号でお答えください。")
# (既知項目台帳のキー, 質問文)。①は債権者行がある場合は省く
QUESTIONS = (("債権者名", QUESTION_1), ("借入時期", QUESTION_2),
             ("最終返済日", QUESTION_3), ("裁判所書類の有無", QUESTION_4))

# 差し込み値（債権者名）の検証: 1〜40 文字・改行/URL/記号なし（「株式会社」等の
# 文字・「（）」「・」「&」は許可）・数字のみ不可
_NAME_RE = re.compile(
    r"^[0-9A-Za-z぀-ゟ゠-ヿ㐀-䶿一-鿿"
    r"０-９Ａ-Ｚａ-ｚー・（）()&＆ 　]{1,40}$")
_URL_MARKERS = ("http", "://", "www.")


def valid_creditor_name(name) -> bool:
    if not isinstance(name, str):
        return False
    s = name.strip()
    if not s or "\n" in s or "\r" in s:
        return False
    if any(m in s.lower() for m in _URL_MARKERS):
        return False
    if not _NAME_RE.match(s):
        return False
    digits = re.sub(r"[ 　]", "", s)
    if digits.isdigit() or re.fullmatch(r"[0-9０-９]+", digits):
        return False
    return True


# ── 出力の検証（閉集合スキーマ。逸脱は None=ai_failed） ─────────────────────────
_TOP_KEYS = frozenset({"creditors", "court_document", "legible"})
_CREDITOR_KEYS = frozenset({"name", "role", "confidence"})


def parse_report(tool_input) -> dict | None:
    """閉集合スキーマの検証（fix2 I2-01: サーバ側検証が正）。None=ai_failed
    （質問のみ版）。拒否条件:
    - dict でない／トップレベルの未知キー／必須キー欠落
    - creditors が list でない／len > MAX_CREDITORS（切り捨て受理しない）
    - 各要素が dict でない／要素の未知キー／必須キー欠落／name が str でない／
      role・confidence が閉集合外
    - court_document が閉集合外／legible が bool でない"""
    if not isinstance(tool_input, dict):
        return None
    if set(tool_input) != _TOP_KEYS:
        return None
    creditors = tool_input["creditors"]
    court = tool_input["court_document"]
    legible = tool_input["legible"]
    if (not isinstance(creditors, list) or len(creditors) > MAX_CREDITORS
            or court not in COURT_DOCUMENTS or not isinstance(legible, bool)):
        return None
    out = []
    for c in creditors:
        if not isinstance(c, dict) or set(c) != _CREDITOR_KEYS:
            return None
        name, role, conf = c["name"], c["role"], c["confidence"]
        if (not isinstance(name, str) or role not in CREDITOR_ROLES
                or conf not in CONFIDENCES):
            return None
        out.append({"name": name.strip(), "role": role, "confidence": conf})
    return {"creditors": out, "court_document": court, "legible": legible}


# ── 債権者行の組み立て（high のみ・差し込み値検証・1 つでも落ちたら省略） ────────
def _join_names(names: list[str]) -> str:
    """(b) 複数の並び: 「A」と「B」（3 社は「A」と「B」と「C」・司令塔の補完）。"""
    return "と".join(f"「{n}」" for n in names)


def creditor_line(creditors: list[dict]) -> str | None:
    high = [c for c in creditors if c.get("confidence") == "high"]
    if not high:
        return None
    if not all(valid_creditor_name(c["name"]) for c in high):
        return None
    by_role: dict[str, list[str]] = {}
    for c in high:
        names = by_role.setdefault(c["role"], [])
        if c["name"] not in names:
            names.append(c["name"])
    assignees = by_role.get("譲受人", [])
    originals = by_role.get("原債権者", [])
    agents = by_role.get("代理人", [])
    principals = [n for role in ("原債権者", "譲受人", "サービサー", "不明")
                  for n in by_role.get(role, [])]
    if len(assignees) == 1 and len(originals) == 1 and not agents:
        return CREDITOR_LINE_ASSIGNED.replace("{譲受人}", assignees[0]) \
            .replace("{原債権者}", originals[0])
    if len(agents) == 1 and len(principals) == 1:
        return CREDITOR_LINE_AGENT.replace("{債権者}", principals[0]) \
            .replace("{代理人}", agents[0])
    if not principals:
        return None                          # 代理人のみ=債権者を確定できない
    names = principals[:MAX_CREDITORS]
    if len(names) == 1:
        return CREDITOR_LINE_SINGLE.replace("{A}", names[0])
    return CREDITOR_LINE_MULTI.replace("{LIST}", _join_names(names))


def high_creditor_names(creditors: list[dict]) -> list[str]:
    """問い合わせ業者名 への書込候補（high・代理人以外・検証済み・重複除去）。"""
    out: list[str] = []
    for c in creditors:
        if (c.get("confidence") == "high" and c.get("role") != "代理人"
                and valid_creditor_name(c["name"]) and c["name"] not in out):
            out.append(c["name"])
    return out


# ── 本文の組み立て（凍結テンプレ・モデル自由文なし） ───────────────────────────
def pending_questions(known: dict, has_creditor_line: bool) -> list[str]:
    out = []
    for key, text in QUESTIONS:
        if key == "債権者名" and has_creditor_line:
            continue
        if not str(known.get(key) or "").strip():
            out.append(text)
    return out


def compose_reply(line: str | None, questions: list[str]) -> str | None:
    """本文。債権者行も質問も無ければ None（送らない）。
    - 債権者行あり+質問あり: IMG2_REPLY_TEMPLATE（冒頭・債権者行・確認行・
      「あわせて、…」・質問）
    - 債権者行あり+質問なし: 「あわせて、…」以下を省く
    - 債権者行なし+質問あり: IMG2_REPLY_TEMPLATE_NO_CREDITOR（(a) 読み取れ
      なかった旨+「次の点について、…」+質問。①は pending_questions が先頭に置く）
    長文ゲートは通さない（docstring 冒頭の規律）。"""
    if not line and not questions:
        return None
    joined = "\n".join(questions)
    if not line:
        return IMG2_REPLY_TEMPLATE_NO_CREDITOR.replace("{質問}", joined)
    body = IMG2_REPLY_TEMPLATE.replace("{債権者行}", line)
    if not questions:
        return body.split("\n" + QUESTIONS_LEAD + "\n", 1)[0]
    return body.replace("{質問}", joined)


# ── App 28 マーカー ───────────────────────────────────────────────────────────
def analysis_category(event_id: str, channel: str = CHANNEL) -> str:
    return f"{ANALYSIS_PREFIX}{channel}:{event_id}"


def analyzed_category(file_key: str, channel: str = CHANNEL) -> str:
    return f"{ANALYZED_PREFIX}{channel}:{file_key}"


async def analyzed_file_keys(user_id: str, channel: str = CHANNEL) -> set[str]:
    """解析済み fileKey の集合（_latest_marker_row と同型: line_user_id +
    category 前方一致）。"""
    prefix = f"{ANALYZED_PREFIX}{channel}:"
    rows = await kintone.search_records(
        _APP_CHATLOG,
        f'line_user_id = "{user_id}" and '
        f'category like "{prefix}" '
        f"order by $id desc limit {_ANALYZED_QUERY_LIMIT}",
        fields=["$id", "category"])
    out = set()
    for r in rows:
        cat = str(((r.get("category") or {}).get("value")) or "")
        if cat.startswith(prefix):
            out.add(cat[len(prefix):])
    return out


async def _write_markers(user_id: str, event_id: str, text: str,
                         file_keys: list[str], channel: str = CHANNEL) -> None:
    """送信成功後にのみ呼ぶ。失敗はログのみ（次の束で再解析され得る=留意点）。"""
    try:
        await kintone.create_record(_APP_CHATLOG, {
            "line_user_id": user_id, "role": "assistant", "message": text,
            "category": analysis_category(event_id, channel), "auto_sent": "yes"})
        for key in file_keys:
            await kintone.create_record(_APP_CHATLOG, {
                "line_user_id": user_id, "role": "assistant",
                "message": ANALYZED_MARKER_TEXT,
                "category": analyzed_category(key, channel), "auto_sent": "no"})
    except Exception:
        logger.error("[IMAGE_ANALYSIS] marker save failed (may re-analyze)")


# ── App 21 ────────────────────────────────────────────────────────────────────
def _v(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "")


async def _fetch_record(user_id: str,
                        app: kintone.KintoneApp = APP_JIKOU_CASE) -> dict | None:
    rows = await kintone.search_records(
        app, f'LINEユーザーID = "{user_id}" order by $id desc limit 1')
    return rows[0] if rows else None


async def _refetch_record(user_id: str,
                          app: kintone.KintoneApp = APP_JIKOU_CASE) -> dict | None:
    """fix2 I2-02: 送信直前の再取得（fail-closed）。例外・0 件・複数件は None。"""
    try:
        rows = await kintone.search_records(
            app, f'LINEユーザーID = "{user_id}" order by $id desc limit 2')
    except Exception:
        return None
    if len(rows) != 1:
        return None
    return rows[0]


def _file_keys_newest_first(record: dict) -> list[str]:
    files = ((record or {}).get(PHOTO_FIELD) or {}).get("value") or []
    keys = [str((f or {}).get("fileKey") or "") for f in files
            if isinstance(f, dict)]
    return [k for k in reversed(keys) if k]


def _blocked(record: dict | None, user_id: str) -> bool:
    """送信直前の抑止判定（IMG-1 の受領返信と同じ判定関数・同じ env）。"""
    if os.environ.get("AUTOREPLY_PAUSED") == "1":
        return True
    if (_v(record, "response_mode") or "自動") == "人対応":
        return True
    return False


async def _store_creditor_names(record_id: str, names: list[str]) -> str:
    """問い合わせ業者名 が空欄のときのみ high の債権者名（「、」区切り）を
    $revision CAS で書く。noop=非空／stored／failed。"""
    value = "、".join(names)
    for attempt in range(STORE_RETRIES + 1):
        try:
            latest = await kintone.get_record(APP_JIKOU_CASE, record_id)
        except kintone.KintoneError:
            return "failed"
        if _v(latest, "問い合わせ業者名").strip():
            return "noop"
        try:
            await kintone.update_record(
                APP_JIKOU_CASE, record_id, {"問い合わせ業者名": value},
                revision=_v(latest, "$revision"))
            return "stored"
        except kintone.KintoneConflict:
            continue
        except kintone.KintoneError:
            return "failed"
    return "failed"


# ── AI 呼び出し ────────────────────────────────────────────────────────────────
def _content_block(data: bytes) -> dict | None:
    """jpeg/png=image・pdf=document。heic・その他・上限超は None（読めない写真）。"""
    if len(data) > MAX_AI_IMAGE_BYTES:
        return None
    fmt = detect_format(data)
    if fmt is None:
        return None
    ext, mime = fmt
    encoded = base64.b64encode(data).decode()
    if ext in ("jpg", "png"):
        return {"type": "image",
                "source": {"type": "base64", "media_type": mime, "data": encoded}}
    if ext == "pdf":
        return {"type": "document",
                "source": {"type": "base64", "media_type": mime, "data": encoded}}
    return None


async def _call_ai(blocks: list[dict], cfg: "ChannelConfig | None" = None
                   ) -> dict | None:
    """構造化出力（tool_choice 強制）。失敗は None（呼び出し側で ai_failed）。"""
    cfg = cfg or JIKOU
    client = anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        timeout=API_TIMEOUT_SEC, max_retries=API_MAX_RETRIES)
    response = await create_message_with_fallback(
        client,
        context=cfg.ai_context,
        max_tokens=1024,
        system=cfg.system_prompt,
        tools=[cfg.report_tool],
        tool_choice={"type": "tool", "name": cfg.report_tool["name"]},
        messages=[{"role": "user",
                   "content": list(blocks)
                   + [{"type": "text", "text": cfg.user_text}]}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == cfg.report_tool["name"]:
            return cfg.parse_fn(block.input)
    return None


# ── 通知（固定文言+レコード番号/種別のみ） ────────────────────────────────────
async def _notify(text: str, throttle_key: str) -> None:
    try:
        await notify.notify_admin_line(text, throttle_key=throttle_key,
                                       throttle_on_success_only=True)
    except Exception:
        logger.error("[IMAGE_ANALYSIS] notify failed (fixed text)")


def _log_result(outcome: str) -> None:
    """sink 規律: outcome は閉集合のため分岐で固定文言として出す。"""
    if outcome == "claimed":
        logger.info("[IMAGE_ANALYSIS] claim held elsewhere (skip)")
    elif outcome == "no_record":
        logger.info("[IMAGE_ANALYSIS] no case record (skip)")
    elif outcome == "no_files":
        logger.info("[IMAGE_ANALYSIS] no unanalyzed photos (skip)")
    elif outcome == "download_failed":
        logger.warning("[IMAGE_ANALYSIS] download failed (skip)")
    elif outcome == "ai_failed":
        logger.warning("[IMAGE_ANALYSIS] ai_failed (questions only)")
    elif outcome == "illegible":
        logger.info("[IMAGE_ANALYSIS] illegible (questions only)")
    elif outcome == "low_confidence":
        logger.info("[IMAGE_ANALYSIS] low_confidence (questions only)")
    elif outcome == "nothing_to_send":
        logger.info("[IMAGE_ANALYSIS] nothing to send")
    elif outcome == "too_long":
        logger.error("[IMAGE_ANALYSIS] reply too long (not sent)")
    elif outcome == "blocked":
        logger.info("[IMAGE_ANALYSIS] blocked (paused/stoplist/human)")
    elif outcome == "recheck_failed":
        logger.warning("[IMAGE_ANALYSIS] recheck_failed (no send, no marker)")
    elif outcome == "send_failed":
        logger.error("[IMAGE_ANALYSIS] send failed (no marker)")
    elif outcome == "sent":
        logger.info("[IMAGE_ANALYSIS] sent")
    else:
        logger.error("[IMAGE_ANALYSIS] failed (fixed reason)")


# ── チャネル設定（HOUKI-IMG-2: 時効は既定 cfg で従来と同一挙動） ───────────────
@dataclass
class Composed:
    """compose_fn の結果: 本文（None=送らない）・AI 状態（閉集合）・チャネル固有の
    後処理データ（store_fn/notify_fn が使う）。"""
    text: str | None
    ai_state: str
    data: dict


@dataclass(frozen=True)
class ChannelConfig:
    name: str                       # マーカー書式・claim key・ログのチャネル名
    app: kintone.KintoneApp         # 対象レコードの App（21 or 40）
    line_channel: Any               # push 先（JIKOU_CHANNEL / HOUKI_CHANNEL）
    report_tool: dict               # tool スキーマ（閉集合）
    system_prompt: str              # 凍結 system prompt
    user_text: str
    ai_context: str
    parse_fn: Callable[[Any], dict | None]
    compose_fn: Callable[[dict | None, dict], Composed]
    store_fn: Callable[..., Any]    # async (record_id, latest, composed) -> noop/stored/failed
    notify_fn: Callable[..., Any]   # async (report, record_id, user_id) -> None
    notify_timing: str              # "after_send"（時効）| "after_ai"（相続放棄）
    send_failure_kind: str
    store_kind: str
    too_long_text: str              # {record_id} を含む固定文言
    send_failure_text: str
    store_failure_text: str


# ── 本体 ─────────────────────────────────────────────────────────────────────
async def analyze_and_reply(user_id: str, event_id: str,
                            cfg: "ChannelConfig | None" = None) -> str:
    """受領返信成功後に呼ばれる 2 通目。戻り値は分類（閉集合）。
    例外は外へ出さない（受領返信の経路を道連れにしない）。cfg 省略=時効。"""
    cfg = cfg or JIKOU
    key = f"{cfg.name}:{user_id}:{event_id}"
    if key in _claims:                       # 確認→取得（同期区間・await なし）
        _log_result("claimed")
        return "claimed"
    _claims.add(key)
    try:
        outcome = await _analyze_and_reply(user_id, event_id, cfg)
    except Exception:
        outcome = "failed"
    finally:
        _claims.discard(key)
    _log_result(outcome)
    return outcome


async def _analyze_and_reply(user_id: str, event_id: str,
                             cfg: "ChannelConfig") -> str:
    record = await _fetch_record(user_id, cfg.app)
    if record is None:
        return "no_record"
    record_id = _v(record, "$id")
    keys = _file_keys_newest_first(record)
    if not keys:
        return "no_files"
    analyzed = await analyzed_file_keys(user_id, cfg.name)
    targets = [k for k in keys if k not in analyzed][:MAX_FILES]
    if not targets:
        return "no_files"

    blocks: list[dict] = []
    unreadable = 0
    for fk in targets:
        try:
            data = await kintone.download_file(cfg.app, fk)
        except Exception:
            return "download_failed"
        block = _content_block(data)
        if block is None:
            unreadable += 1
        else:
            blocks.append(block)
    logger.info("[IMAGE_ANALYSIS] photos=%s unreadable=%s",
                emit(len(targets), "count", "log", "operator"),
                emit(unreadable, "count", "log", "operator"))

    report = None
    if blocks:
        try:
            report = await _call_ai(blocks, cfg)
        except Exception:
            report = None
    if cfg.notify_timing == "after_ai":
        # 相続放棄: 弁護士通知は送信の成否・抑止と独立（レコード番号+固定文言）
        await cfg.notify_fn(report, record_id, user_id)

    # fix2 I2-02: 送信直前に対象 App を再取得し（fail-closed）、最新レコードで
    # (1) 人対応 (2) 再取得失敗 (3) 本文の組立（既知項目の再構成）(4) 転記の
    # 空欄判定（store_fn 内の get_record）を行う。AI 処理中の変化を反映する
    latest = await _refetch_record(user_id, cfg.app)
    if latest is None:
        return "recheck_failed"
    record_id = _v(latest, "$id") or record_id
    if _blocked(latest, user_id):
        return "blocked"
    composed = cfg.compose_fn(report, latest)
    if composed.ai_state != "ok":
        _log_result(composed.ai_state)
    text = composed.text
    if text is None:
        return "nothing_to_send"
    if len(text) > REPLY_MAX_CHARS:
        await _notify(cfg.too_long_text.replace("{record_id}", record_id),
                      f"{cfg.send_failure_kind}:{user_id}")
        return "too_long"

    # 送信直前の抑止判定（pause／停止リスト。人対応は上の再取得判定で済み）
    if os.environ.get("AUTOREPLY_PAUSED") == "1" or await is_suppressed(user_id):
        return "blocked"
    try:
        sent = await push_text(cfg.line_channel, user_id, text)
    except Exception:
        sent = False
    if sent is not True:
        await _notify(cfg.send_failure_text.replace("{record_id}", record_id),
                      f"{cfg.send_failure_kind}:{user_id}")
        return "send_failed"

    await _write_markers(user_id, event_id, text, targets, cfg.name)

    if record_id:
        stored = await cfg.store_fn(record_id, latest, composed)
        if stored == "failed":
            await _notify(cfg.store_failure_text.replace("{record_id}", record_id),
                          f"{cfg.store_kind}:{record_id}")
    if cfg.notify_timing == "after_send":
        await cfg.notify_fn(report, record_id, user_id)
    return "sent"


# ── 時効（既定 cfg・従来と同一挙動） ─────────────────────────────────────────────
def _jikou_compose(report: dict | None, latest: dict) -> Composed:
    if report is None:
        ai_state, creditors = "ai_failed", []
    elif not report["legible"]:
        ai_state, creditors = "illegible", []
    else:
        ai_state, creditors = "ok", report["creditors"]
    line = creditor_line(creditors)
    if ai_state == "ok" and line is None:
        ai_state = "low_confidence"
    known = build_known_items(latest, [])
    questions = pending_questions(known, has_creditor_line=line is not None)
    return Composed(compose_reply(line, questions), ai_state,
                    {"creditors": creditors, "line": line})


async def _jikou_store(record_id: str, latest: dict, composed: Composed) -> str:
    names = (high_creditor_names(composed.data["creditors"])
             if composed.data["line"] is not None else [])
    if not names:
        return "noop"
    return await _store_creditor_names(record_id, names)


async def _jikou_notify(report: dict | None, record_id: str, user_id: str) -> None:
    if report and report["legible"] \
            and report["court_document"] in COURT_NOTIFY_KINDS:
        await _notify(
            "【書類写真・裁判所書類】お写真に裁判所からの書類が写っています"
            f"（種別: {report['court_document']}・レコード番号: {record_id}）。"
            "優先してご確認ください。",
            f"image_analysis_court:{record_id}")


JIKOU = ChannelConfig(
    name=CHANNEL, app=APP_JIKOU_CASE, line_channel=JIKOU_CHANNEL,
    report_tool=REPORT_TOOL, system_prompt=SYSTEM_PROMPT, user_text=USER_TEXT,
    ai_context="書類写真の債権者読解",
    parse_fn=parse_report, compose_fn=_jikou_compose, store_fn=_jikou_store,
    notify_fn=_jikou_notify, notify_timing="after_send",
    send_failure_kind="image_analysis_send_failure",
    store_kind="image_analysis_store",
    too_long_text=("【書類写真・要確認】お写真への自動返信が文字数上限を超えたため"
                   "送信していません（レコード番号: {record_id}）。App 21 と App 28 を"
                   "確認し、必要なら手動でご返信ください。"),
    send_failure_text=("【書類写真・要確認】お写真への自動返信の送信に失敗しました"
                       "（レコード番号: {record_id}）。LINE アプリで受信をご確認"
                       "ください。"),
    store_failure_text=("【書類写真・要確認】読み取った債権者名を 問い合わせ業者名 へ"
                        "書き込めませんでした（レコード番号: {record_id}）。上書き"
                        "せず中止しています。"),
)


# ══════════════════════════════════════════════════════════════════════════════
# 相続放棄（HOUKI-IMG-2）: 書類写真の読解→読み取れた項目の確認+弁護士通知
# ══════════════════════════════════════════════════════════════════════════════
HOUKI_CHANNEL_NAME = "houki"
HOUKI_STORE_RETRIES = 1

# 凍結 system prompt（sha256 pin）。「初めて届いた通知か」「財産処分に当たるか」
# 「熟慮期間」は判定させない
HOUKI_SYSTEM_PROMPT = (
    "あなたは法律事務所の事務補助です。相続放棄のご相談者から届いた書類写真"
    "（督促状・請求書・裁判所からの書類・家庭裁判所や他の相続人からの通知など）"
    "を読み取り、report_documents ツールで報告してください。\n"
    "- creditors: 亡くなった方（被相続人）に対して請求している債権者を最大 3 件。"
    "role は 原債権者／譲受人（債権譲渡を受けた新しい債権者）／代理人・回収受託者"
    "（弁護士・司法書士・債権回収会社などが債権者の代理として連絡している場合。"
    "債権者は元のまま）／不明。kind は 民間債権／公租公課（税金・保険料など）／"
    "不明。判読できない・確信が持てない場合は confidence を low にしてください。\n"
    "- court_document: 裁判所から届いた書類が写っている場合のみその種別"
    "（訴状／支払督促／仮執行宣言付支払督促／判決／差押命令／競売開始決定／"
    "その他）。裁判所の書類でなければ なし、判断できなければ 不明。\n"
    "- death_date: 書類に亡くなった方の死亡日が明記されている場合のみ"
    " YYYY-MM-DD 形式で。明記がなければ null。death_date_confidence は死亡日の"
    "読み取りの確信度（明記がなければ low）。書類の作成日・通知日を死亡日として"
    "扱わないでください。\n"
    "- inheritance_document: 相続関係の書類（相続放棄申述受理通知書／相続放棄"
    "申述受理証明書／相続関係についての通知書／その他）が写っていればその種別と"
    "書面日付（YYYY-MM-DD・不明なら null）。knowledge_timing は、その書類が"
    "「ご相談者が自分が相続人だと知るきっかけになった通知」に当たり得るかの"
    "外形（該当／非該当／不明）のみ。\n"
    "- possible_disposition_document: 亡くなった方の財産の処分・解約・出金に"
    "関係する可能性のある書類（解約申込書・売買契約書・出金伝票など）が写って"
    "いれば あり、なければ なし、判断できなければ 不明。\n"
    "- 「初めて届いた通知かどうか」「財産処分に当たるかどうか」「熟慮期間"
    "（3 か月）の起算や経過」は判定しないでください。外形の報告に留めます。\n"
    "- 書類全体が判読できない場合は legible を false にしてください。\n"
    "- 被相続人・ご相談者の氏名・住所・金額・口座など、ツールの項目にない情報は"
    "一切出力しないでください。"
)
HOUKI_USER_TEXT = "添付の書類写真を読み取り、report_documents で報告してください。"

HOUKI_CREDITOR_ROLES = ("原債権者", "譲受人", "代理人・回収受託者", "不明")
HOUKI_CREDITOR_KINDS = ("民間債権", "公租公課", "不明")
HOUKI_COURT_DOCUMENTS = ("訴状", "支払督促", "仮執行宣言付支払督促", "判決",
                         "差押命令", "競売開始決定", "その他", "なし", "不明")
HOUKI_COURT_DISPLAY = ("訴状", "支払督促", "仮執行宣言付支払督促", "判決",
                       "差押命令", "競売開始決定")           # 表示は 6 種の固定語のみ
HOUKI_INHERITANCE_KINDS = ("相続放棄申述受理通知書", "相続放棄申述受理証明書",
                           "相続関係についての通知書", "その他", "なし", "不明")
HOUKI_KNOWLEDGE_TIMING = ("該当", "非該当", "不明")
HOUKI_DISPOSITION = ("あり", "なし", "不明")
_HOUKI_AGENT_ROLE = "代理人・回収受託者"

HOUKI_REPORT_TOOL = {
    "name": "report_documents",
    "description": "相続放棄のご相談者から届いた書類写真の読み取り結果を報告する",
    "input_schema": {
        "type": "object",
        "properties": {
            "creditors": {
                "type": "array", "maxItems": MAX_CREDITORS,
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "role": {"type": "string", "enum": list(HOUKI_CREDITOR_ROLES)},
                        "kind": {"type": "string", "enum": list(HOUKI_CREDITOR_KINDS)},
                        "confidence": {"type": "string", "enum": list(CONFIDENCES)},
                    },
                    "required": ["name", "role", "kind", "confidence"],
                    "additionalProperties": False,
                },
            },
            "court_document": {"type": "string", "enum": list(HOUKI_COURT_DOCUMENTS)},
            "death_date": {"type": ["string", "null"],
                           "description": "YYYY-MM-DD または null"},
            "death_date_confidence": {"type": "string", "enum": list(CONFIDENCES)},
            "inheritance_document": {
                "type": "object",
                "properties": {
                    "kind": {"type": "string", "enum": list(HOUKI_INHERITANCE_KINDS)},
                    "document_date": {"type": ["string", "null"]},
                    "knowledge_timing": {"type": "string",
                                         "enum": list(HOUKI_KNOWLEDGE_TIMING)},
                },
                "required": ["kind", "document_date", "knowledge_timing"],
                "additionalProperties": False,
            },
            "possible_disposition_document": {"type": "string",
                                              "enum": list(HOUKI_DISPOSITION)},
            "legible": {"type": "boolean"},
        },
        "required": ["creditors", "court_document", "death_date",
                     "death_date_confidence", "inheritance_document",
                     "possible_disposition_document", "legible"],
        "additionalProperties": False,
    },
}
_HOUKI_TOP_KEYS = frozenset(HOUKI_REPORT_TOOL["input_schema"]["required"])
_HOUKI_CREDITOR_KEYS = frozenset({"name", "role", "kind", "confidence"})
_HOUKI_INHERITANCE_KEYS = frozenset({"kind", "document_date", "knowledge_timing"})


def parse_houki_report(tool_input) -> dict | None:
    """閉集合スキーマの検証（サーバ側が正・キー集合の完全一致）。None=ai_failed。
    日付文字列は型のみ検査し、表示条件（形式・実在・未来日・confidence）は
    houki_display_items で判定する。"""
    if not isinstance(tool_input, dict) or set(tool_input) != _HOUKI_TOP_KEYS:
        return None
    creditors = tool_input["creditors"]
    if not isinstance(creditors, list) or len(creditors) > MAX_CREDITORS:
        return None
    out = []
    for c in creditors:
        if not isinstance(c, dict) or set(c) != _HOUKI_CREDITOR_KEYS:
            return None
        if (not isinstance(c["name"], str) or c["role"] not in HOUKI_CREDITOR_ROLES
                or c["kind"] not in HOUKI_CREDITOR_KINDS
                or c["confidence"] not in CONFIDENCES):
            return None
        out.append({"name": c["name"].strip(), "role": c["role"],
                    "kind": c["kind"], "confidence": c["confidence"]})
    if tool_input["court_document"] not in HOUKI_COURT_DOCUMENTS:
        return None
    dd = tool_input["death_date"]
    if dd is not None and not isinstance(dd, str):
        return None
    if tool_input["death_date_confidence"] not in CONFIDENCES:
        return None
    inh = tool_input["inheritance_document"]
    if not isinstance(inh, dict) or set(inh) != _HOUKI_INHERITANCE_KEYS:
        return None
    if (inh["kind"] not in HOUKI_INHERITANCE_KINDS
            or (inh["document_date"] is not None
                and not isinstance(inh["document_date"], str))
            or inh["knowledge_timing"] not in HOUKI_KNOWLEDGE_TIMING):
        return None
    if tool_input["possible_disposition_document"] not in HOUKI_DISPOSITION:
        return None
    if not isinstance(tool_input["legible"], bool):
        return None
    return {
        "creditors": out,
        "court_document": tool_input["court_document"],
        "death_date": dd,
        "death_date_confidence": tool_input["death_date_confidence"],
        "inheritance_document": {"kind": inh["kind"],
                                 "document_date": inh["document_date"],
                                 "knowledge_timing": inh["knowledge_timing"]},
        "possible_disposition_document": tool_input["possible_disposition_document"],
        "legible": tool_input["legible"],
    }


# ── 相談者への 2 通目（弁護士文言・凍結・sha256 pin） ────────────────────────────
HOUKI_REPLY_TEMPLATE = (
    "お写真をありがとうございます。\n"
    "お写真からは、次の内容が読み取れました。\n"
    "\n"
    "{項目}\n"
    "\n"
    "読み取りに誤りがある場合は、正しい内容をお知らせください。"
)
HOUKI_KEEP_ORIGINAL_LINE = "裁判所から届いた書類の原本は、そのまま保管してください。"
HOUKI_ITEM_CREDITOR = "・債権者名：{LIST}"                 # {LIST}=「A」と「B」
HOUKI_ITEM_COURT = "・裁判所から届いた書類：「{種別}」"
HOUKI_ITEM_DEATH = "・亡くなられた方の死亡日：「{日付}」"

_ISO_DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def parse_iso_date(value) -> datetime.date | None:
    """"YYYY-MM-DD" 完全一致かつ実在する暦日のみ date。それ以外は None。"""
    if not isinstance(value, str) or not _ISO_DATE_RE.match(value):
        return None
    try:
        return datetime.date.fromisoformat(value)
    except ValueError:
        return None


def format_date_ja(d: datetime.date) -> str:
    """「YYYY年M月D日」（ゼロ埋めなし・元号変換なし）。"""
    return f"{d.year}年{d.month}月{d.day}日"


def valid_death_date(value, confidence: str,
                     today: datetime.date | None = None) -> datetime.date | None:
    """表示・転記条件: 完全一致の実在日・未来日でない・confidence=high。"""
    if confidence != "high":
        return None
    d = parse_iso_date(value)
    if d is None:
        return None
    today = today or datetime.date.today()
    if d > today:
        return None
    return d


def houki_display_items(report: dict | None,
                        today: datetime.date | None = None) -> dict:
    """表示規則（凍結事項）で読み取れた項目だけを取り出す。
    creditor_names: high かつ名称検証通過（代理人・回収受託者は債権者でないため
    表示・転記とも除く）・重複除去・最大 3／court: 6 種の固定語のみ／
    death_date: valid_death_date を満たす date（書面日付は流用しない）。"""
    out = {"creditor_names": [], "court": None, "death_date": None}
    if not report or not report["legible"]:
        return out
    for c in report["creditors"]:
        if (c["confidence"] == "high" and c["role"] != _HOUKI_AGENT_ROLE
                and valid_creditor_name(c["name"])
                and c["name"] not in out["creditor_names"]):
            out["creditor_names"].append(c["name"])
    out["creditor_names"] = out["creditor_names"][:MAX_CREDITORS]
    if report["court_document"] in HOUKI_COURT_DISPLAY:
        out["court"] = report["court_document"]
    out["death_date"] = valid_death_date(report["death_date"],
                                         report["death_date_confidence"], today)
    return out


def compose_houki_reply(items: dict) -> str | None:
    """固定順（債権者名→裁判所書類→死亡日）。表示 0 件は None。原本保管文は
    裁判所書類の行があるときだけ。質問は付けない。長文ゲートは通さない。"""
    lines = []
    if items["creditor_names"]:
        lines.append(HOUKI_ITEM_CREDITOR.replace(
            "{LIST}", _join_names(items["creditor_names"])))
    if items["court"]:
        lines.append(HOUKI_ITEM_COURT.replace("{種別}", items["court"]))
    if items["death_date"]:
        lines.append(HOUKI_ITEM_DEATH.replace("{日付}",
                                              format_date_ja(items["death_date"])))
    if not lines:
        return None
    text = HOUKI_REPLY_TEMPLATE.replace("{項目}", "\n".join(lines))
    if items["court"]:
        text += "\n" + HOUKI_KEEP_ORIGINAL_LINE
    return text


def _houki_compose(report: dict | None, latest: dict) -> Composed:
    if report is None:
        ai_state = "ai_failed"
    elif not report["legible"]:
        ai_state = "illegible"
    else:
        ai_state = "ok"
    items = houki_display_items(report)
    text = compose_houki_reply(items)
    if ai_state == "ok" and text is None:
        ai_state = "low_confidence"
    return Composed(text, ai_state, items)


async def _houki_store_death_date(record_id: str, iso: str) -> str:
    """死亡日_申告 が空欄のときのみ $revision CAS で書く（noop/stored/failed）。
    死亡日（確定）・起算日_確定・知った日 3 欄には書かない。"""
    for _attempt in range(HOUKI_STORE_RETRIES + 1):
        try:
            latest = await kintone.get_record(houki_case_store.APP_HOUKI_CASE,
                                              record_id)
        except kintone.KintoneError:
            return "failed"
        if _v(latest, "死亡日_申告").strip():
            return "noop"
        try:
            await kintone.update_record(
                houki_case_store.APP_HOUKI_CASE, record_id,
                {"死亡日_申告": iso}, revision=_v(latest, "$revision"))
            return "stored"
        except kintone.KintoneConflict:
            continue
        except kintone.KintoneError:
            return "failed"
    return "failed"


async def _houki_store(record_id: str, latest: dict, composed: Composed) -> str:
    """App 40 への転記: 債権者名は houki_case_store.append_creditors（重複除去・
    CAS 収束・収束不能は同関数が要確認通知）・死亡日_申告 は空欄のみ CAS。
    訴訟督促有無・財産処分有無・財産_負債 は書かない。"""
    items = composed.data
    outcome = "noop"
    if items["creditor_names"]:
        try:
            added = await houki_case_store.append_creditors(
                record_id, latest, list(items["creditor_names"]))
            if added:
                outcome = "stored"
        except Exception:
            outcome = "failed"
    if items["death_date"] is not None:
        res = await _houki_store_death_date(record_id, items["death_date"].isoformat())
        if res == "failed":
            outcome = "failed"
        elif res == "stored" and outcome != "failed":
            outcome = "stored"
    return outcome


async def _houki_notify(report: dict | None, record_id: str, user_id: str) -> None:
    """弁護士通知（レコード番号+固定文言のみ・success_only throttle）。送信の
    成否・抑止と独立に AI 出力から判定する。"""
    if not report or not report["legible"]:
        return
    court = report["court_document"]
    if court not in ("なし", "不明"):
        await _notify(
            f"【相続放棄・書類写真】裁判所からの書類「{court}」が含まれています"
            f"（レコード番号 {record_id}）。内容を確認してください。",
            f"houki_image_analysis_court:{record_id}")
    inh = report["inheritance_document"]
    if inh["kind"] not in ("なし", "不明") or inh["knowledge_timing"] == "該当":
        d = parse_iso_date(inh["document_date"])
        date_text = format_date_ja(d) if d else "不明"
        await _notify(
            f"【相続放棄・書類写真】相続関係の書類「{inh['kind']}」が含まれています"
            f"（レコード番号 {record_id}・書面日付 {date_text}）。知った時期の"
            "検討材料として確認してください。",
            f"houki_image_analysis_notice:{record_id}")
    if report["possible_disposition_document"] == "あり":
        await _notify(
            "【相続放棄・書類写真】財産処分に関係する可能性のある書類が含まれて"
            f"います（レコード番号 {record_id}）。内容を確認してください。",
            f"houki_image_analysis_disposition:{record_id}")


HOUKI = ChannelConfig(
    name=HOUKI_CHANNEL_NAME, app=houki_case_store.APP_HOUKI_CASE,
    line_channel=HOUKI_CHANNEL,
    report_tool=HOUKI_REPORT_TOOL, system_prompt=HOUKI_SYSTEM_PROMPT,
    user_text=HOUKI_USER_TEXT, ai_context="相続放棄・書類写真の読解",
    parse_fn=parse_houki_report, compose_fn=_houki_compose,
    store_fn=_houki_store, notify_fn=_houki_notify, notify_timing="after_ai",
    send_failure_kind="houki_image_analysis_send_failure",
    store_kind="houki_image_analysis_store",
    too_long_text=("【相続放棄・書類写真・要確認】お写真への自動返信が文字数上限を"
                   "超えたため送信していません（レコード番号 {record_id}）。App 40 と"
                   " App 28 を確認し、必要なら手動でご返信ください。"),
    send_failure_text=("【相続放棄・書類写真・要確認】お写真への自動返信の送信に"
                       "失敗しました（レコード番号 {record_id}）。LINE アプリで受信を"
                       "ご確認ください。"),
    store_failure_text=("【相続放棄・書類写真・要確認】読み取った内容の App 40 への"
                        "転記を確定できませんでした（レコード番号 {record_id}）。"
                        "上書きせず中止しています。"),
)
