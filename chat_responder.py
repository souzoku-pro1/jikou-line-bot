"""
顧客対応 Claude モジュール

ヒアリング完了後（status が「受付中」「問い合わせ」以外）の顧客メッセージに対し、
Claude API (tool use) で返信案を作成し、自動送信または承認キューへの保存を行う。

外部から使うインターフェース:
  - get_app21_record(user_id)          : App21 を LINEユーザーID で検索
  - classify_routing(status)            : ルーティング分類 ("hearing"|"pre"|"post")
  - handle_customer_message(...)        : 顧客対応Claudeのメインエントリ
  - get_approval_record(record_id)      : 承認キューレコード取得
  - mark_approval_sent(record_id)       : 承認キュー送信済みフラグ更新
  - send_line_push(to, text)            : LINE Push送信
  - save_to_chatlog(...)                : チャットログ保存
"""

import logging
import os
from typing import Callable, Optional

import anthropic
import httpx

logger = logging.getLogger("chat_responder")

# ── 環境変数 ──────────────────────────────────────────────────────────────────
_SUBDOMAIN     = os.environ.get("KINTONE_SUBDOMAIN", "")
_APP21_ID      = os.environ.get("KINTONE_APP_ID", "")
_APP21_TOKEN   = os.environ.get("KINTONE_API_TOKEN", "")
APP_CHATLOG    = os.environ.get("APP_CHATLOG", "")
TOKEN_CHATLOG  = os.environ.get("TOKEN_CHATLOG", "")
APP_APPROVAL   = os.environ.get("APP_APPROVAL", "")
TOKEN_APPROVAL = os.environ.get("TOKEN_APPROVAL", "")
_LINE_TOKEN    = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "")
_ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ATTORNEY_LINE_USER_ID = os.environ.get("ATTORNEY_LINE_USER_ID", "")

# ── ステータス分類 ─────────────────────────────────────────────────────────────
# ヒアリング未完了 → 既存フロー
HEARING_STATUSES         = {"", "受付中", "問い合わせ"}
# 受任後 → 顧客対応Claude（受任後モード）
POST_ENGAGEMENT_STATUSES = {"受任", "手続き中", "完了"}
# 受任前（決済完了・不受任など）→ 顧客対応Claude（受任前モード）
# PRE_ENGAGEMENT: 上記以外の値すべて（安全側フォールバック含む）

# ── カテゴリ定義 ───────────────────────────────────────────────────────────────
AUTO_SEND_CATEGORIES = {
    "挨拶・雑談",
    "手続きの一般的な流れ",
    "必要書類の案内",
    "費用の定型案内",
    "進捗の事実回答",
    "営業案内・アクセス",
}

# ── 定型文 ────────────────────────────────────────────────────────────────────
PENDING_REPLY = (
    "ありがとうございます。内容を確認の上、改めてご連絡いたします。\n"
    "少々お時間をいただく場合がございますが、何卒よろしくお願いいたします。"
)

# ── システムプロンプト ─────────────────────────────────────────────────────────
_SYSTEM_PROMPT_TMPL = """\
あなたは大野法律事務所のLINE応対担当アシスタントです。
顧客からのLINEメッセージに対し、弁護士（先生）が確認・送信する返信文の下書きを作成します。

【顧客情報】
- 顧客名: {customer_name}
- 案件ステータス: {status}（{phase}フェーズ）
- 対象業者: {business_name}

【返信ルール】
- 敬体（です・ます調）、1メッセージ400字以内を目安とする
- 用語: 「時効の更新」を使用（「時効の延長」は禁止）
- 個別事案の時効成否の断定・示唆は禁止。判断が必要な場合は「弁護士が確認いたします」の形にする
- 記録にない進捗・日付・金額の創作は禁止

【必ずauto_send=falseにするケース】
- 裁判所書類・差押えなど緊急連絡の場合
- 本人以外（家族・第三者）からの連絡の場合
- 不受任ステータスの顧客から新規受任の可否を問われた場合
- 判断に迷う場合（迷ったらfalse）

【カテゴリ選択肢】
自動送信OK: 挨拶・雑談 / 手続きの一般的な流れ / 必要書類の案内 / 費用の定型案内 / 進捗の事実回答 / 営業案内・アクセス
承認必須: 法的判断・見通し / 費用交渉・減額相談 / クレーム・不満 / 解約・辞任関係 / 緊急対応 / 本人確認不能・第三者 / その他判断系\
"""

# ── tool 定義 ─────────────────────────────────────────────────────────────────
_COMPOSE_REPLY_TOOL = {
    "name": "compose_reply",
    "description": "顧客への返信文を作成し、カテゴリと送信可否を判定する",
    "input_schema": {
        "type": "object",
        "properties": {
            "reply": {
                "type": "string",
                "description": "顧客への返信文（400字以内目安）",
            },
            "category": {
                "type": "string",
                "enum": [
                    "挨拶・雑談",
                    "手続きの一般的な流れ",
                    "必要書類の案内",
                    "費用の定型案内",
                    "進捗の事実回答",
                    "営業案内・アクセス",
                    "法的判断・見通し",
                    "費用交渉・減額相談",
                    "クレーム・不満",
                    "解約・辞任関係",
                    "緊急対応",
                    "本人確認不能・第三者",
                    "その他判断系",
                ],
                "description": "メッセージのカテゴリ",
            },
            "auto_send": {
                "type": "boolean",
                "description": "自動送信してよいか。trueでもサーバー側でカテゴリを検証する",
            },
            "reason": {
                "type": "string",
                "description": "弁護士向けの判断理由（1〜2文）",
            },
        },
        "required": ["reply", "category", "auto_send", "reason"],
    },
}


# ── ユーティリティ ─────────────────────────────────────────────────────────────

def _kintone_base() -> str:
    sub = _SUBDOMAIN.replace(".cybozu.com", "").strip()
    return f"https://{sub}.cybozu.com"


# ── ルーティング判定 ────────────────────────────────────────────────────────────

def classify_routing(status: str) -> str:
    """
    App 21 の status 値からルーティング先を返す。
      "hearing"        : ヒアリング未完了 → 既存フロー
      "post_engagement": 受任後 → 顧客対応Claude（受任後）
      "pre_engagement" : 受任前 → 顧客対応Claude（受任前）
                         ※ 想定外の値は安全側フォールバックで pre_engagement
    """
    if status in HEARING_STATUSES:
        return "hearing"
    if status in POST_ENGAGEMENT_STATUSES:
        return "post_engagement"
    return "pre_engagement"


# ── kintone App 21 ─────────────────────────────────────────────────────────────

async def get_app21_record(user_id: str) -> Optional[dict]:
    """App 21 から LINEユーザーID でレコードを検索して返す（なければ None）"""
    if not (_SUBDOMAIN and _APP21_TOKEN and _APP21_ID):
        logger.warning("App21 env vars not configured")
        return None
    url = f"{_kintone_base()}/k/v1/records.json"
    params = {
        "app": _APP21_ID,
        "query": f'LINEユーザーID = "{user_id}" order by $id desc limit 1',
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"X-Cybozu-API-Token": _APP21_TOKEN},
            params=params,
        )
    if not resp.is_success:
        logger.error("App21 search failed: %s %s", resp.status_code, resp.text)
        return None
    records = resp.json().get("records", [])
    return records[0] if records else None


# ── チャットログ（APP_CHATLOG 未設定時はスキップ） ─────────────────────────────

async def save_to_chatlog(
    user_id: str, role: str, message: str, category: str, auto_sent: str
) -> None:
    """チャットログアプリに1レコード保存。APP_CHATLOG 未設定時はスキップ。"""
    if not (APP_CHATLOG and TOKEN_CHATLOG):
        return
    url = f"{_kintone_base()}/k/v1/record.json"
    body = {
        "app": APP_CHATLOG,
        "record": {
            "line_user_id": {"value": user_id},
            "role":         {"value": role},
            "message":      {"value": message},
            "category":     {"value": category},
            "auto_sent":    {"value": auto_sent},
        },
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={
                "X-Cybozu-API-Token": TOKEN_CHATLOG,
                "Content-Type": "application/json",
            },
            json=body,
        )
    if not resp.is_success:
        logger.warning("chatlog save failed: %s %s", resp.status_code, resp.text)


async def get_recent_chat_history(user_id: str, limit: int = 10) -> list[dict]:
    """
    チャットログアプリから直近 limit 往復（最大 limit*2 件）のメッセージを取得し、
    Claude messages 形式（role/content）のリストで返す。
    APP_CHATLOG 未設定時は空リスト。
    """
    if not (APP_CHATLOG and TOKEN_CHATLOG):
        return []
    url = f"{_kintone_base()}/k/v1/records.json"
    params = {
        "app": APP_CHATLOG,
        "query": (
            f'line_user_id = "{user_id}" order by $id desc limit {limit * 2}'
        ),
        "fields[0]": "role",
        "fields[1]": "message",
    }
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"X-Cybozu-API-Token": TOKEN_CHATLOG},
            params=params,
        )
    if not resp.is_success:
        logger.warning("chatlog fetch failed: %s %s", resp.status_code, resp.text)
        return []
    records = resp.json().get("records", [])
    # desc で取得しているので reversed で古い順に並べ直す
    return [
        {"role": r["role"]["value"], "content": r["message"]["value"]}
        for r in reversed(records)
    ]


# ── 承認キュー（APP_APPROVAL 未設定時はスキップ） ──────────────────────────────

async def save_to_approval_queue(
    user_id: str,
    customer_name: str,
    customer_message: str,
    ai_draft: str,
    category: str,
    reason: str,
) -> Optional[str]:
    """承認キューアプリに下書きを保存し、レコードIDを返す。APP_APPROVAL 未設定時は None。"""
    if not (APP_APPROVAL and TOKEN_APPROVAL):
        logger.warning("APP_APPROVAL not configured, skipping approval queue")
        return None
    url = f"{_kintone_base()}/k/v1/record.json"
    body = {
        "app": APP_APPROVAL,
        "record": {
            "line_user_id":  {"value": user_id},
            "顧客名":        {"value": customer_name},
            "顧客メッセージ": {"value": customer_message},
            "AI下書き":      {"value": ai_draft},
            "カテゴリ":      {"value": category},
            "判断理由":      {"value": reason},
            "ステータス2":   {"value": "承認待ち"},
            "送信済み":      {"value": "no"},
        },
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={
                "X-Cybozu-API-Token": TOKEN_APPROVAL,
                "Content-Type": "application/json",
            },
            json=body,
        )
    if not resp.is_success:
        print(f"[APP29] save failed: {resp.status_code} {resp.text[:300]}")
        return None
    record_id = resp.json().get("id")
    print(f"[APP29] saved record_id={record_id}")
    return record_id


async def get_approval_record(record_id: str) -> Optional[dict]:
    """承認キューアプリから指定IDのレコードを取得する"""
    if not (APP_APPROVAL and TOKEN_APPROVAL):
        return None
    url = f"{_kintone_base()}/k/v1/record.json"
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"X-Cybozu-API-Token": TOKEN_APPROVAL},
            params={"app": APP_APPROVAL, "id": record_id},
        )
    if not resp.is_success:
        logger.error(
            "approval record fetch failed: %s %s", resp.status_code, resp.text
        )
        return None
    return resp.json().get("record")


async def mark_approval_sent(record_id: str) -> None:
    """承認キューの送信済み=yes に更新する（冪等）"""
    if not (APP_APPROVAL and TOKEN_APPROVAL):
        return
    url = f"{_kintone_base()}/k/v1/record.json"
    body = {
        "app": APP_APPROVAL,
        "id": record_id,
        "record": {"送信済み": {"value": "yes"}},
    }
    async with httpx.AsyncClient() as client:
        resp = await client.put(
            url,
            headers={
                "X-Cybozu-API-Token": TOKEN_APPROVAL,
                "Content-Type": "application/json",
            },
            json=body,
        )
    if not resp.is_success:
        logger.error(
            "mark_approval_sent failed: %s %s", resp.status_code, resp.text
        )


# ── LINE 送信 ──────────────────────────────────────────────────────────────────

async def send_line_push(to: str, text: str) -> None:
    """LINE Push API でメッセージを送信する"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {_LINE_TOKEN}",
                "Content-Type": "application/json",
            },
            json={"to": to, "messages": [{"type": "text", "text": text}]},
        )
    print(f"[LINE_PUSH] to={to} status={resp.status_code}")
    if not resp.is_success:
        print(f"[LINE_PUSH] ERROR: {resp.text[:300]}")


async def _notify_attorney(
    user_id: str, customer_name: str, approval_record_id: Optional[str], category: str
) -> None:
    """弁護士に承認依頼を LINE Push で通知する"""
    if not ATTORNEY_LINE_USER_ID:
        print("[ATTORNEY] ATTORNEY_LINE_USER_ID not set, skipping")
        return
    rid = approval_record_id or "（未取得）"
    print(f"[ATTORNEY] notifying to={ATTORNEY_LINE_USER_ID} approval_id={rid} category={category}")
    msg = (
        f"【承認依頼】\n"
        f"顧客: {customer_name or user_id}\n"
        f"カテゴリ: {category}\n"
        f"承認キューレコードNo: {rid}\n"
        f"kintone承認キューを確認し、ステータスを「承認済」に変更してください。"
    )
    await send_line_push(ATTORNEY_LINE_USER_ID, msg)


# ── Claude 呼び出し ────────────────────────────────────────────────────────────

async def _call_compose_reply(system_prompt: str, messages: list[dict]) -> dict:
    """Claude API (tool use / compose_reply 強制) を呼び出し結果 dict を返す"""
    client = anthropic.AsyncAnthropic(api_key=_ANTHROPIC_KEY)
    response = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=system_prompt,
        tools=[_COMPOSE_REPLY_TOOL],
        tool_choice={"type": "tool", "name": "compose_reply"},
        messages=messages,
    )
    block = next((b for b in response.content if b.type == "tool_use"), None)
    if not block:
        raise RuntimeError("compose_reply tool was not called by Claude")
    return block.input  # {"reply": ..., "category": ..., "auto_send": ..., "reason": ...}


# ── メインハンドラ ─────────────────────────────────────────────────────────────

async def handle_customer_message(
    user_id: str,
    user_message: str,
    reply_token: str,
    app21_record: dict,
    reply_func: Callable,
) -> None:
    """
    顧客対応 Claude のメインエントリーポイント。

    Parameters
    ----------
    user_id       : LINE ユーザーID
    user_message  : 顧客のメッセージ本文
    reply_token   : LINE Reply API トークン
    app21_record  : get_app21_record() で取得した kintone App21 レコード dict
    reply_func    : async (reply_token: str, text: str) -> None
                    LINE に返信するための呼び出し元提供の非同期関数
    """
    # App21 レコードから案件情報を取り出す
    status        = app21_record.get("status", {}).get("value", "")
    customer_name = app21_record.get("顧客名", {}).get("value", "") or "（未登録）"
    business_name = (
        app21_record.get("ルックアップ_0", {}).get("value", "")
        or app21_record.get("問い合わせ業者名", {}).get("value", "")
        or "（未登録）"
    )
    routing = classify_routing(status)
    phase   = "受任後" if routing == "post_engagement" else "受任前"

    system_prompt = _SYSTEM_PROMPT_TMPL.format(
        phase=phase,
        customer_name=customer_name,
        status=status,
        business_name=business_name,
    )

    # チャット履歴（直近10往復）を取得してメッセージに追加
    history = await get_recent_chat_history(user_id)
    history.append({"role": "user", "content": user_message})

    # Claude で返信案を作成
    try:
        result = await _call_compose_reply(system_prompt, history)
    except Exception:
        logger.exception("compose_reply failed for user_id=%s", user_id)
        # Claude 呼び出し失敗時は定型文を返して終了
        await reply_func(reply_token, PENDING_REPLY)
        return

    reply_text = result["reply"]
    category   = result["category"]
    auto_send  = result["auto_send"]
    reason     = result.get("reason", "")
    print(f"[COMPOSE_REPLY] user_id={user_id} category={category!r} auto_send={auto_send} reason={reason!r}")

    # サーバー側二重チェック: モデルの auto_send=true かつカテゴリが許可リストにある場合のみ自動送信
    can_auto_send = auto_send and (category in AUTO_SEND_CATEGORIES)

    # ユーザーメッセージをチャットログに保存
    await save_to_chatlog(user_id, "user", user_message, category, "no")

    if can_auto_send:
        await reply_func(reply_token, reply_text)
        await save_to_chatlog(user_id, "assistant", reply_text, category, "yes")
        print(f"[AUTO_SEND] user_id={user_id} category={category} len={len(reply_text)}")
    else:
        # 承認キューに下書きを保存
        approval_id = await save_to_approval_queue(
            user_id=user_id,
            customer_name=customer_name,
            customer_message=user_message,
            ai_draft=reply_text,
            category=category,
            reason=reason,
        )
        # 顧客への定型文を返信
        await reply_func(reply_token, PENDING_REPLY)
        await save_to_chatlog(user_id, "assistant", PENDING_REPLY, category, "yes")
        # 弁護士へ承認依頼通知
        await _notify_attorney(user_id, customer_name, approval_id, category)
        print(f"[APPROVAL] queued user_id={user_id} category={category} approval_id={approval_id}")
