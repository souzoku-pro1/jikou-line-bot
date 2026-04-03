import os
import re
import json
import hmac
import hashlib
import base64
import httpx
import anthropic
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
KINTONE_SUBDOMAIN = os.environ["KINTONE_SUBDOMAIN"]
KINTONE_APP_ID = os.environ["KINTONE_APP_ID"]
KINTONE_API_TOKEN = os.environ["KINTONE_API_TOKEN"]

REPLY_URL = "https://api.line.me/v2/bot/message/reply"

claude_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """【友達追加・最初のメッセージへの自動返信】
はじめまして。
大野法律事務所　時効援用専門窓口です。

借金の時効援用についてのご相談を
LINEで承っております。

時効の可能性を確認するため
以下の項目をご回答ください。

━━━━━━━━━━━━━━━
①債権者名（例：アコム、レイクなど）
※債権回収会社や法律事務所から通知や訴状、支払督促が来ている場合は、その名前

②おおよその借入時期
※不明な場合は「不明」とご記入ください

③おおよその最終返済日
（1）不明な場合は「不明」とご記入ください
（2）過去5年以内に返済しましたか？

④10年以内に裁判所から以下の書類は届きましたか？
・1 訴状が届いた
・2 支払督促が届いた
・3 その他の督促通知が届いた
・4 何も届いていない
※番号でお答えください

⑤お手元に通知書・訴状・支払督促などの
書類がございましたら
写真を送っていただくと
より正確に確認できます
━━━━━━━━━━━━━━━

ご不明な点はそのままお気軽にお送りください。

【回答に対するClaudeの判定ロジック】

④で1または2と答えた場合：
「裁判所からの書類が届いているとのことですね。
現在も訴訟・支払督促の手続きが
進行中かどうかによって対応が異なります。

まだ手続きが進行中の場合は
答弁書等で時効援用を主張できる可能性があります。

詳しくは担当弁護士が確認いたしますので
引き続き情報をお知らせください。」

④で4と答えた場合：追加質問
「承知しました。
今回の債務について
信用情報（CICやJICCなど）を
確認して知りましたか？

・はい
・いいえ」

【時効可能性ありの場合】
「ご回答ありがとうございます。
確認の結果、時効援用できる可能性があります。

正式にご依頼される場合は
追加で以下をお教えください。

━━━━━━━━━━━━━━━
①お名前
②ご住所
③生年月日
④今回の債務をどのように知りましたか？
・債権者からの通知書が届いた
・裁判所から訴状・支払督促が届いた
・信用情報を確認して知った
・その他
━━━━━━━━━━━━━━━」

【kintone登録について】
以下の5項目がすべて揃ったら、通常の返信メッセージの末尾に
以下の形式でデータを出力してください。ユーザーには見えません。

[KINTONE_RECORD]
{
  "問い合わせ業者名": "（債権者名の値）",
  "借入時期_テキスト": "（借入時期の値）",
  "最終返済日_テキスト": "（最終返済日の値）",
  "裁判所書類": "（裁判所からの書類の有無の値）",
  "信用情報確認": "（信用情報から知ったかどうかの値）"
}
[/KINTONE_RECORD]

5項目：債権者名・借入時期・最終返済日・裁判所からの書類の有無・信用情報から知ったかどうか"""

# ユーザーIDごとの会話履歴を保持
conversation_histories: dict[str, list] = {}


def verify_signature(body: bytes, signature: str) -> bool:
    hash = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256
    ).digest()
    expected = base64.b64encode(hash).decode("utf-8")
    return hmac.compare_digest(expected, signature)


async def post_to_kintone(record: dict) -> None:
    url = f"https://{KINTONE_SUBDOMAIN}.cybozu.com/k/v1/record.json"
    headers = {
        "X-Cybozu-API-Token": KINTONE_API_TOKEN,
        "Content-Type": "application/json",
    }
    fields = {key: {"value": value} for key, value in record.items()}
    body = {"app": KINTONE_APP_ID, "record": fields}

    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, json=body)
        response.raise_for_status()


def extract_kintone_record(text: str) -> tuple[str, dict | None]:
    """返答からkintoneデータを抽出し、マーカーを除去したテキストを返す"""
    pattern = r"\[KINTONE_RECORD\](.*?)\[/KINTONE_RECORD\]"
    match = re.search(pattern, text, re.DOTALL)
    if not match:
        return text, None

    clean_text = re.sub(pattern, "", text, flags=re.DOTALL).strip()
    try:
        record = json.loads(match.group(1).strip())
        return clean_text, record
    except json.JSONDecodeError:
        return clean_text, None


async def ask_claude(user_id: str, user_message: str) -> str:
    history = conversation_histories.setdefault(user_id, [])
    history.append({"role": "user", "content": user_message})

    response = await claude_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=history,
    )

    reply_text = response.content[0].text
    history.append({"role": "assistant", "content": reply_text})

    return reply_text


@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")

    if not verify_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    data = await request.json()

    for event in data.get("events", []):
        if event.get("type") != "message":
            continue
        if event["message"].get("type") != "text":
            continue

        reply_token = event["replyToken"]
        user_id = event["source"]["userId"]
        user_text = event["message"]["text"]

        claude_reply = await ask_claude(user_id, user_text)

        # kintoneデータが含まれていれば登録・除去
        clean_reply, kintone_record = extract_kintone_record(claude_reply)
        if kintone_record:
            kintone_record["LINEユーザーID"] = user_id
            kintone_record["ステータス"] = "問い合わせ"
            await post_to_kintone(kintone_record)

        async with httpx.AsyncClient() as client:
            await client.post(
                REPLY_URL,
                headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
                json={
                    "replyToken": reply_token,
                    "messages": [{"type": "text", "text": clean_reply}],
                },
            )

    return {"status": "ok"}
