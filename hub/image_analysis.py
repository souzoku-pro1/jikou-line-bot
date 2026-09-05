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
import logging
import os
import re

import anthropic

from chat_responder import build_known_items
from claude_gateway import create_message_with_fallback
from hub import kintone
from hub import notify
from hub.autoreply_stoplist import is_suppressed
from hub.image_store import APP_JIKOU_CASE, PHOTO_FIELD, detect_format
from hub.line_channel import JIKOU_CHANNEL, push_text
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
# JIKOU-IMG-2-fix1: 弁護士決定の文言へ差し替え（逐語・記号・全角空白・改行・
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
NO_CREDITOR_LINE = "お写真からは、債権者名を読み取ることができませんでした。"
QUESTIONS_LEAD_NO_CREDITOR = "次の点について、分かる範囲で教えてください。"
IMG2_REPLY_TEMPLATE_NO_CREDITOR = (
    IMG2_HEADER + "\n"
    + NO_CREDITOR_LINE + "\n"
    + QUESTIONS_LEAD_NO_CREDITOR + "\n"
    "{質問}"
)
# 債権者行（2 行目。3 行目 CREDITOR_CONFIRM は 4 文型共通）
CREDITOR_LINE_SINGLE = "お写真からは、債権者名が「{A}」と読み取れました。"
# (b) 複数（2〜3 社）: {LIST}=「A」「B」（3 社は「A」「B」「C」）
CREDITOR_LINE_MULTI = "お写真からは、債権者名が{LIST}と読み取れました。"
# (c) 譲渡
CREDITOR_LINE_ASSIGNED = ("お写真からは、債権者名が「{譲受人}」（「{原債権者}」から"
                          "債権譲渡を受けたもの）と読み取れました。")
# (d) 代理人
CREDITOR_LINE_AGENT = ("お写真からは、債権者名が「{債権者}」（ご連絡元の「{代理人}」"
                       "はその代理人）と読み取れました。")

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
    """(b) 複数の並び: 「A」「B」（3 社は「A」「B」「C」）。"""
    return "".join(f"「{n}」" for n in names)


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
def analysis_category(event_id: str) -> str:
    return f"{ANALYSIS_PREFIX}{CHANNEL}:{event_id}"


def analyzed_category(file_key: str) -> str:
    return f"{ANALYZED_PREFIX}{CHANNEL}:{file_key}"


async def analyzed_file_keys(user_id: str) -> set[str]:
    """解析済み fileKey の集合（_latest_marker_row と同型: line_user_id +
    category 前方一致）。"""
    rows = await kintone.search_records(
        _APP_CHATLOG,
        f'line_user_id = "{user_id}" and '
        f'category like "{ANALYZED_PREFIX}{CHANNEL}:" '
        f"order by $id desc limit {_ANALYZED_QUERY_LIMIT}",
        fields=["$id", "category"])
    prefix = f"{ANALYZED_PREFIX}{CHANNEL}:"
    out = set()
    for r in rows:
        cat = str(((r.get("category") or {}).get("value")) or "")
        if cat.startswith(prefix):
            out.add(cat[len(prefix):])
    return out


async def _write_markers(user_id: str, event_id: str, text: str,
                         file_keys: list[str]) -> None:
    """送信成功後にのみ呼ぶ。失敗はログのみ（次の束で再解析され得る=留意点）。"""
    try:
        await kintone.create_record(_APP_CHATLOG, {
            "line_user_id": user_id, "role": "assistant", "message": text,
            "category": analysis_category(event_id), "auto_sent": "yes"})
        for key in file_keys:
            await kintone.create_record(_APP_CHATLOG, {
                "line_user_id": user_id, "role": "assistant",
                "message": ANALYZED_MARKER_TEXT,
                "category": analyzed_category(key), "auto_sent": "no"})
    except Exception:
        logger.error("[IMAGE_ANALYSIS] marker save failed (may re-analyze)")


# ── App 21 ────────────────────────────────────────────────────────────────────
def _v(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "")


async def _fetch_record(user_id: str) -> dict | None:
    rows = await kintone.search_records(
        APP_JIKOU_CASE,
        f'LINEユーザーID = "{user_id}" order by $id desc limit 1')
    return rows[0] if rows else None


async def _refetch_record(user_id: str) -> dict | None:
    """fix2 I2-02: 送信直前の再取得（fail-closed）。例外・0 件・複数件は None。"""
    try:
        rows = await kintone.search_records(
            APP_JIKOU_CASE,
            f'LINEユーザーID = "{user_id}" order by $id desc limit 2')
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


async def _call_ai(blocks: list[dict]) -> dict | None:
    """構造化出力（tool_choice 強制）。失敗は None（呼び出し側で ai_failed）。"""
    client = anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
        timeout=API_TIMEOUT_SEC, max_retries=API_MAX_RETRIES)
    response = await create_message_with_fallback(
        client,
        context="書類写真の債権者読解",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        tools=[REPORT_TOOL],
        tool_choice={"type": "tool", "name": REPORT_TOOL["name"]},
        messages=[{"role": "user",
                   "content": list(blocks) + [{"type": "text", "text": USER_TEXT}]}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == REPORT_TOOL["name"]:
            return parse_report(block.input)
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


# ── 本体 ─────────────────────────────────────────────────────────────────────
async def analyze_and_reply(user_id: str, event_id: str) -> str:
    """時効チャネルの受領返信成功後に呼ばれる 2 通目。戻り値は分類（閉集合）。
    例外は外へ出さない（受領返信の経路を道連れにしない）。"""
    key = f"{CHANNEL}:{user_id}:{event_id}"
    if key in _claims:                       # 確認→取得（同期区間・await なし）
        _log_result("claimed")
        return "claimed"
    _claims.add(key)
    try:
        outcome = await _analyze_and_reply(user_id, event_id)
    except Exception:
        outcome = "failed"
    finally:
        _claims.discard(key)
    _log_result(outcome)
    return outcome


async def _analyze_and_reply(user_id: str, event_id: str) -> str:
    record = await _fetch_record(user_id)
    if record is None:
        return "no_record"
    record_id = _v(record, "$id")
    keys = _file_keys_newest_first(record)
    if not keys:
        return "no_files"
    analyzed = await analyzed_file_keys(user_id)
    targets = [k for k in keys if k not in analyzed][:MAX_FILES]
    if not targets:
        return "no_files"

    blocks: list[dict] = []
    unreadable = 0
    for fk in targets:
        try:
            data = await kintone.download_file(APP_JIKOU_CASE, fk)
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
    ai_state = "ai_failed"
    if blocks:
        try:
            report = await _call_ai(blocks)
        except Exception:
            report = None
    if report is None:
        ai_state = "ai_failed"
    elif not report["legible"]:
        ai_state = "illegible"
    else:
        ai_state = "ok"
    creditors = report["creditors"] if (report and report["legible"]) else []
    line = creditor_line(creditors)
    if ai_state == "ok" and line is None:
        ai_state = "low_confidence"
    if ai_state != "ok":
        _log_result(ai_state)

    # fix2 I2-02: 送信直前に App 21 を再取得し（fail-closed）、最新レコードで
    # (1) 人対応 (2) 再取得失敗 (3) 既知項目台帳の再構成 (4) 業者名の空欄判定
    # （_store_creditor_names 内の get_record）を行う。AI 処理中の変化を反映する
    latest = await _refetch_record(user_id)
    if latest is None:
        return "recheck_failed"
    record_id = _v(latest, "$id") or record_id
    if _blocked(latest, user_id):
        return "blocked"
    known = build_known_items(latest, [])
    questions = pending_questions(known, has_creditor_line=line is not None)
    text = compose_reply(line, questions)
    if text is None:
        return "nothing_to_send"
    if len(text) > REPLY_MAX_CHARS:
        await _notify(
            "【書類写真・要確認】お写真への自動返信が文字数上限を超えたため"
            f"送信していません（レコード番号: {record_id}）。App 21 と App 28 を"
            "確認し、必要なら手動でご返信ください。",
            f"image_analysis_send_failure:{user_id}")
        return "too_long"

    # 送信直前の抑止判定（pause／停止リスト。人対応は上の再取得判定で済み）
    if os.environ.get("AUTOREPLY_PAUSED") == "1" or await is_suppressed(user_id):
        return "blocked"
    try:
        sent = await push_text(JIKOU_CHANNEL, user_id, text)
    except Exception:
        sent = False
    if sent is not True:
        await _notify(
            "【書類写真・要確認】お写真への自動返信の送信に失敗しました"
            f"（レコード番号: {record_id}）。LINE アプリで受信をご確認ください。",
            f"image_analysis_send_failure:{user_id}")
        return "send_failed"

    await _write_markers(user_id, event_id, text, targets)

    names = high_creditor_names(creditors) if line is not None else []
    if names and record_id:
        stored = await _store_creditor_names(record_id, names)
        if stored == "failed":
            await _notify(
                "【書類写真・要確認】読み取った債権者名を 問い合わせ業者名 へ"
                f"書き込めませんでした（レコード番号: {record_id}）。上書きせず"
                "中止しています。",
                f"image_analysis_store:{record_id}")
    if report and report["legible"] \
            and report["court_document"] in COURT_NOTIFY_KINDS:
        await _notify(
            "【書類写真・裁判所書類】お写真に裁判所からの書類が写っています"
            f"（種別: {report['court_document']}・レコード番号: {record_id}）。"
            "優先してご確認ください。",
            f"image_analysis_court:{record_id}")
    return "sent"
