import os
import stripe
import re
import json
import hmac
import hashlib
import base64
import httpx
import anthropic
from pydantic import BaseModel, Field, AliasChoices
from fastapi import FastAPI, Request, HTTPException, UploadFile, File, BackgroundTasks

app = FastAPI()

from cloudsign_webhook import router as cloudsign_router
from document_webhook import router as document_router
app.include_router(cloudsign_router)
app.include_router(document_router)

from chat_responder import (
    get_app21_record,
    classify_routing,
    handle_customer_message,
    get_approval_record,
    mark_approval_sent,
    send_line_push,
    save_to_chatlog,
)


@app.get("/health")
async def health():
    """起動確認・依存ライブラリのインポートチェック"""
    status = {}
    try:
        import fitz
        status["pymupdf"] = fitz.__version__
    except ImportError as e:
        status["pymupdf"] = f"NG: {e}"
    return {"status": "ok", "deps": status}


LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
KINTONE_SUBDOMAIN = os.environ["KINTONE_SUBDOMAIN"]
KINTONE_APP_ID = os.environ["KINTONE_APP_ID"]
KINTONE_API_TOKEN = os.environ["KINTONE_API_TOKEN"]

REPLY_URL = "https://api.line.me/v2/bot/message/reply"
PUSH_URL  = "https://api.line.me/v2/bot/message/push"


async def _line_reply_with_fallback(reply_token: str, user_id: str, text: str) -> None:
    """LINE Reply APIを試み、失敗（400等）したらPush APIにフォールバック"""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            REPLY_URL,
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"replyToken": reply_token, "messages": [{"type": "text", "text": text}]},
        )
    if resp.is_success:
        print(f"[LINE] reply OK user_id={user_id}")
        return
    print(f"[LINE] reply failed {resp.status_code} {resp.text[:200]}, trying push")
    async with httpx.AsyncClient() as client:
        push_resp = await client.post(
            PUSH_URL,
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
        )
    print(f"[LINE] push fallback status={push_resp.status_code}")
    if not push_resp.is_success:
        print(f"[LINE] push fallback error: {push_resp.text[:200]}")

# OCR固定資産エンドポイント用の環境変数（起動時ではなくリクエスト時にチェック）
GOOGLE_VISION_API_KEY        = os.environ.get("GOOGLE_VISION_API_KEY")
KINTONE_FUDOSAN_DOMAIN       = os.environ.get("KINTONE_DOMAIN", os.environ.get("KINTONE_SUBDOMAIN", ""))
KINTONE_FUDOSAN_APP_ID_OCR   = os.environ.get("KINTONE_FUDOSAN_APP_ID", "")
KINTONE_FUDOSAN_API_TOKEN_OCR = os.environ.get("KINTONE_FUDOSAN_API_TOKEN", "")
LINE_USER_ID                 = os.environ.get("LINE_USER_ID", "")

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
④電話番号
⑤メールアドレス
⑥今回の債務をどのように知りましたか？
・債権者からの通知書が届いた
・裁判所から訴状・支払督促が届いた
・信用情報を確認して知った
・その他
━━━━━━━━━━━━━━━」

【kintone登録について】

■第1段階：以下の5項目がすべて揃ったら、返信メッセージの末尾に出力してください。ユーザーには見えません。

[KINTONE_RECORD]
{
  "問い合わせ業者名": "（債権者名の値）",
  "借入時期_テキスト": "（借入時期の値）",
  "最終返済日_テキスト": "（最終返済日の値）",
  "裁判所書類": "（裁判所からの書類の有無の値）",
  "信用情報確認": "（信用情報から知ったかどうかの値）"
}
[/KINTONE_RECORD]

5項目：債権者名・借入時期・最終返済日・裁判所からの書類の有無・信用情報から知ったかどうか

■第2段階：お名前・ご住所・生年月日・電話番号・メールアドレスの5項目がすべて揃ったら、返信メッセージの末尾に出力してください。ユーザーには見えません。

[KINTONE_UPDATE]
{
  "顧客名": "（お名前の値）",
  "住所": "（ご住所の値）",
  "生年月日": "（生年月日の値）",
  "電話番号": "（電話番号の値）",
  "メールアドレス": "（メールアドレスの値）"
}
[/KINTONE_UPDATE]"""

# ユーザーIDごとの会話履歴を保持
conversation_histories: dict[str, list] = {}

# ユーザーIDごとのkintoneレコードIDを保持
kintone_record_ids: dict[str, str] = {}

# ユーザーIDごとの業者名を保持（第2段階更新で使用）
user_business_names: dict[str, str] = {}

# ヒアリング完了済みユーザーID（同セッション内で KINTONE_UPDATE を送出済み）
hearing_completed: set[str] = set()

KINTONE_WEBHOOK_TOKEN = os.environ.get("KINTONE_WEBHOOK_TOKEN", "")


def verify_signature(body: bytes, signature: str) -> bool:
    hash = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256
    ).digest()
    expected = base64.b64encode(hash).decode("utf-8")
    return hmac.compare_digest(expected, signature)


async def post_to_kintone(record: dict) -> str:
    """レコードを新規作成し、レコードIDを返す"""
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
        return response.json()["id"]


async def update_kintone_record(record_id: str, fields: dict) -> None:
    """既存レコードを更新する"""
    url = f"https://{KINTONE_SUBDOMAIN}.cybozu.com/k/v1/record.json"
    headers = {
        "X-Cybozu-API-Token": KINTONE_API_TOKEN,
        "Content-Type": "application/json",
    }
    record_fields = {key: {"value": value} for key, value in fields.items()}
    body = {"app": KINTONE_APP_ID, "id": record_id, "record": record_fields}

    print(f"[DEBUG] update url: {url}")
    print(f"[DEBUG] update record_id: {record_id!r}")
    print(f"[DEBUG] update fields keys: {list(fields.keys())}")
    print(f"[DEBUG] update body: {json.dumps(body, ensure_ascii=False)}")

    async with httpx.AsyncClient() as client:
        response = await client.put(url, headers=headers, json=body)
        print(f"[DEBUG] update status: {response.status_code}")
        print(f"[DEBUG] update response: {response.text}")
        if not response.is_success:
            try:
                err = response.json()
                print(f"[DEBUG] update error code: {err.get('code')}")
                print(f"[DEBUG] update error message: {err.get('message')}")
                print(f"[DEBUG] update error errors: {err.get('errors')}")
            except Exception:
                pass
        response.raise_for_status()


def extract_marker(text: str, tag: str) -> tuple[str, dict | None]:
    """指定タグのデータを抽出し、マーカーを除去したテキストを返す"""
    pattern = rf"\[{tag}\](.*?)\[/{tag}\]"
    match = re.search(pattern, text, re.DOTALL)
    if not match:
        return text, None

    clean_text = re.sub(pattern, "", text, flags=re.DOTALL).strip()
    try:
        data = json.loads(match.group(1).strip())
        return clean_text, data
    except json.JSONDecodeError:
        return clean_text, None


async def ask_claude(user_id: str, user_message: str) -> str:
    history = conversation_histories.setdefault(user_id, [])
    history.append({"role": "user", "content": user_message})

    response = await claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=history,
    )

    reply_text = response.content[0].text
    history.append({"role": "assistant", "content": reply_text})

    return reply_text


async def _process_line_event(reply_token: str, user_id: str, user_text: str) -> None:
    """LINEイベントの重い処理（BackgroundTasksで非同期実行）"""
    print(f"[PROCESS] start user_id={user_id} text={user_text[:30]!r}")
    try:
        # ── ルーティング判定 ──────────────────────────────────────────────
        in_hearing_session = (
            user_id in conversation_histories
            and user_id not in hearing_completed
        )

        if not in_hearing_session:
            app21_record = await get_app21_record(user_id)
            if app21_record is not None:
                status = app21_record.get("status", {}).get("value", "")
                routing = classify_routing(status)
                print(f"[ROUTING] user_id={user_id} App21 status={status!r} routing={routing}")
                if routing != "hearing":
                    async def _reply_func(token: str, text: str) -> None:
                        await _line_reply_with_fallback(token, user_id, text)
                    await handle_customer_message(
                        user_id=user_id,
                        user_message=user_text,
                        reply_token=reply_token,
                        app21_record=app21_record,
                        reply_func=_reply_func,
                    )
                    return
                print(f"[ROUTING] user_id={user_id} → hearing (status={status!r})")
            else:
                print(f"[ROUTING] user_id={user_id} → hearing (no App21 record)")
        else:
            print(f"[ROUTING] user_id={user_id} → hearing (in_session)")

        # ── 既存ヒアリングフロー ──────────────────────────────────────────
        claude_reply = await ask_claude(user_id, user_text)

        # 第1段階：レコード新規作成
        clean_reply, kintone_record = extract_marker(claude_reply, "KINTONE_RECORD")
        if kintone_record:
            kintone_record["LINEユーザーID"] = user_id
            kintone_record["status"] = "問い合わせ"
            user_business_names[user_id] = kintone_record.get("問い合わせ業者名", "")
            record_id = await post_to_kintone(kintone_record)
            kintone_record_ids[user_id] = record_id
            print(f"[KINTONE] RECORD created record_id={record_id}")
            claude_reply = clean_reply

        # 第2段階：既存レコードを更新
        clean_reply2, update_fields = extract_marker(claude_reply, "KINTONE_UPDATE")
        if update_fields:
            print(f"[DEBUG] KINTONE_UPDATE detected: {update_fields}")
            print(f"[DEBUG] stored record_id for user: {kintone_record_ids.get(user_id)!r}")
        if update_fields and user_id in kintone_record_ids:
            await update_kintone_record(kintone_record_ids[user_id], update_fields)
            claude_reply = clean_reply2
            hearing_completed.add(user_id)

        await _line_reply_with_fallback(reply_token, user_id, claude_reply)

    except Exception:
        import traceback
        print(f"[ERROR] _process_line_event failed user_id={user_id}:")
        print(traceback.format_exc())


@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")

    if not verify_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    data = json.loads(body)

    for event in data.get("events", []):
        if event.get("type") != "message":
            continue
        if event["message"].get("type") != "text":
            continue

        reply_token = event["replyToken"]
        user_id = event["source"]["userId"]
        user_text = event["message"]["text"]

        background_tasks.add_task(_process_line_event, reply_token, user_id, user_text)
        print(f"[WEBHOOK] queued user_id={user_id} text={user_text[:20]!r}")

    return {"status": "ok"}


@app.post("/webhook/kintone/approval")
async def kintone_approval_webhook(request: Request):
    """
    kintone 承認キューアプリの Webhook を受け取る。
    ステータス=承認済 かつ 送信済み=no のレコードに対して
    最新の AI下書き を LINE push し、送信済み=yes に更新する（冪等）。
    URL: /webhook/kintone/approval?token=<KINTONE_WEBHOOK_TOKEN>
    """
    token = request.query_params.get("token", "")
    if not KINTONE_WEBHOOK_TOKEN or not hmac.compare_digest(token, KINTONE_WEBHOOK_TOKEN):
        raise HTTPException(status_code=404, detail="not found")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    # レコード ID を取得
    try:
        record_id = body["record"]["$id"]["value"]
    except (KeyError, TypeError):
        record_id = body.get("recordId")
    if not record_id:
        return {"ok": True, "skip": "no_record_id"}
    record_id = str(record_id)

    # Webhook ボディで高速チェック（不要な API 呼び出しを減らす）
    try:
        webhook_status = body["record"]["ステータス2"]["value"]
        webhook_sent   = body["record"]["送信済み"]["value"]
    except (KeyError, TypeError):
        return {"ok": True, "skip": "missing_fields"}

    if webhook_status != "承認済" or webhook_sent != "no":
        return {"ok": True, "skip": "not_triggered"}

    # 最新レコードを取り直す（先生の修正を反映するため）
    record = await get_approval_record(record_id)
    if not record:
        return {"ok": True, "skip": "record_not_found"}

    current_status = record.get("ステータス2", {}).get("value", "")
    current_sent   = record.get("送信済み",   {}).get("value", "")

    if current_status != "承認済" or current_sent != "no":
        return {"ok": True, "skip": "already_sent_or_not_approved"}

    user_id  = record.get("line_user_id", {}).get("value", "")
    ai_draft = record.get("AI下書き",     {}).get("value", "")
    category = record.get("カテゴリ",     {}).get("value", "")

    if not user_id or not ai_draft:
        return {"ok": True, "skip": "missing_user_or_draft"}

    # LINE push 送信 → 送信済みフラグ更新 → チャットログ保存
    await send_line_push(user_id, ai_draft)
    await mark_approval_sent(record_id)
    await save_to_chatlog(user_id, "assistant", ai_draft, category, "yes")

    return {"ok": True, "record_id": record_id}


# ══════════════════════════════════════════════════════════════
# POST /ocr/fixed-asset  固定資産税評価額 OCR → kintone登録
# ══════════════════════════════════════════════════════════════

def _ocr_pdf_bytes(pdf_bytes: bytes, api_key: str) -> str:
    """PDFを Vision API の files:annotate に直接送ってOCRする（PyMuPDF不要）"""
    import urllib.request
    content = base64.b64encode(pdf_bytes).decode("utf-8")
    body = json.dumps({
        "requests": [{
            "inputConfig": {
                "content": content,
                "mimeType": "application/pdf",
            },
            "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
            "imageContext": {"languageHints": ["ja", "en"]},
        }]
    }).encode("utf-8")
    url = f"https://vision.googleapis.com/v1/files:annotate?key={api_key}"
    req = urllib.request.Request(url, data=body,
                                  headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Vision API {e.code}: {err_body}")

    # files:annotate のレスポンス構造:
    # result["responses"][0]["responses"] → ページごとの結果リスト
    pages_text = []
    for page_resp in result.get("responses", [{}])[0].get("responses", []):
        annotation = page_resp.get("fullTextAnnotation")
        if annotation:
            pages_text.append(annotation.get("text", ""))
    return "\n\n".join(pages_text)


async def _extract_fixed_asset(ocr_text: str) -> dict:
    """OCRテキストから固定資産税評価額・年度・所在地・地番を抽出して返す"""
    response = await claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": (
                "以下は固定資産税の課税明細書またはその関連書類のOCRテキストです。\n"
                "次の4項目をJSONで抽出してください。不明な場合は null にしてください。\n"
                "\n"
                "- 評価額: 円単位の整数（例: 12345678）。カンマや「円」は除去すること。\n"
                "- 年度: 西暦4桁の整数（例: 令和6年度→2024、令和7年度→2025）。\n"
                "- 所在地: 不動産の所在地（例: 埼玉県戸田市喜沢）。番地は含めず市区町村・大字まで。\n"
                "- 地番: 地番または家屋番号（例: 123-4）。\n"
                "\n"
                '出力形式: {"評価額": 12345678, "年度": 2024, "所在地": "埼玉県戸田市喜沢", "地番": "123-4"}\n'
                "JSONのみ出力してください。\n\n"
                f"=== OCRテキスト ===\n{ocr_text}\n=== END ==="
            ),
        }],
    )
    raw = response.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


# 丁目の数字変換テーブル（算用数字→漢数字）
_CHOME_KANJI = {
    1:'一', 2:'二', 3:'三', 4:'四', 5:'五',
    6:'六', 7:'七', 8:'八', 9:'九', 10:'十',
    11:'十一', 12:'十二', 13:'十三', 14:'十四', 15:'十五',
    16:'十六', 17:'十七', 18:'十八', 19:'十九', 20:'二十',
}

def _normalize_shozaichi(address: str) -> str:
    """OCR抽出の所在地をkintone検索用に正規化する

    変換内容:
    1. 都道府県名を削除（埼玉県川口市→川口市）
    2. 丁目前の算用数字を漢数字に変換（3丁目→三丁目）
    3. 番地表記を統一（32-6 / 32の6 → 32番地6）
    """
    if not address:
        return address
    # 1. 都道府県名を削除
    address = re.sub(r'^(北海道|東京都|大阪府|京都府|.{2,3}県)', '', address)
    # 2. 丁目前の算用数字→漢数字
    def _chome_to_kanji(m):
        n = int(m.group(1))
        return _CHOME_KANJI.get(n, str(n)) + '丁目'
    address = re.sub(r'(\d+)丁目', _chome_to_kanji, address)
    # 3. 番地表記を統一（ハイフン・「の」→「番地」）
    address = re.sub(r'(\d+)[－ー\-の](\d+)(番地?|号)?$', r'\1番地\2', address)
    return address.strip()


def _normalize_chiban(chiban: str) -> str:
    """地番の表記を統一する（32-6 / 32の6 → 32番地6）"""
    if not chiban:
        return chiban
    chiban = re.sub(r'(\d+)[－ー\-の](\d+)(番地?|号)?$', r'\1番地\2', chiban)
    return chiban.strip()


async def _search_kintone_record(shozaichi: str) -> str | None:
    """正規化済み所在地でkintoneレコードを部分一致検索してレコードIDを返す。見つからない場合はNone。"""
    import urllib.parse
    FIELD_SHOZAICHI = "所在"
    headers = {"X-Cybozu-API-Token": KINTONE_FUDOSAN_API_TOKEN_OCR}

    query = f'{FIELD_SHOZAICHI} like "{shozaichi}"'
    params = urllib.parse.urlencode({
        "app": KINTONE_FUDOSAN_APP_ID_OCR,
        "query": query,
        "fields[0]": "$id",
    })
    url = f"https://{KINTONE_FUDOSAN_DOMAIN}.cybozu.com/k/v1/records.json?{params}"
    print(f"[DEBUG] kintone search query: {query}")
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        print(f"[DEBUG] kintone search status: {resp.status_code} / response: {resp.text}")
        if not resp.is_success:
            raise Exception(f"kintone検索エラー {resp.status_code}: {resp.text}")
        records = resp.json().get("records", [])
        if not records:
            return None
        return records[0]["$id"]["value"]


async def _update_kintone_record(record_id: str, extracted: dict) -> None:
    """固定資産税評価額・年度を既存レコードに上書き更新する"""
    url = f"https://{KINTONE_FUDOSAN_DOMAIN}.cybozu.com/k/v1/record.json"
    headers = {
        "X-Cybozu-API-Token": KINTONE_FUDOSAN_API_TOKEN_OCR,
        "Content-Type": "application/json",
    }
    record = {
        "固定資産税評価額":   {"value": str(extracted["評価額"]) if extracted.get("評価額") is not None else ""},
        "固定資産税評価年度": {"value": str(extracted["年度"])   if extracted.get("年度")   is not None else ""},
    }
    body = {"app": KINTONE_FUDOSAN_APP_ID_OCR, "id": record_id, "record": record}
    print(f"[DEBUG] kintone PUT body: {json.dumps(body, ensure_ascii=False)}")
    async with httpx.AsyncClient() as client:
        resp = await client.put(url, headers=headers, json=body)
        print(f"[DEBUG] kintone PUT status: {resp.status_code}")
        print(f"[DEBUG] kintone PUT response: {resp.text}")
        if not resp.is_success:
            raise Exception(f"kintone更新エラー {resp.status_code}: {resp.text}")


async def _push_line_message(user_id: str, text: str) -> None:
    """LINE Push APIでメッセージを送る"""
    async with httpx.AsyncClient() as client:
        await client.post(
            PUSH_URL,
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
        )


@app.post("/ocr/fixed-asset")
async def ocr_fixed_asset(file: UploadFile = File(...)):
    """
    PDFをアップロードすると固定資産税評価額・年度をOCRで抽出し
    kintoneに登録してLINEで通知する。
    """
    # ─ 環境変数チェック ─
    missing = [k for k, v in {
        "GOOGLE_VISION_API_KEY": GOOGLE_VISION_API_KEY,
        "KINTONE_DOMAIN or KINTONE_SUBDOMAIN": KINTONE_FUDOSAN_DOMAIN,
        "KINTONE_FUDOSAN_APP_ID": KINTONE_FUDOSAN_APP_ID_OCR,
        "KINTONE_FUDOSAN_API_TOKEN": KINTONE_FUDOSAN_API_TOKEN_OCR,
    }.items() if not v]
    if missing:
        raise HTTPException(status_code=500,
                            detail=f"環境変数が未設定です: {', '.join(missing)}")

    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    # 1. PDFを読み込む
    pdf_bytes = await file.read()

    # 2. Google Cloud Vision API でOCR
    try:
        ocr_text = _ocr_pdf_bytes(pdf_bytes, GOOGLE_VISION_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRエラー: {e}")

    if not ocr_text.strip():
        raise HTTPException(status_code=422, detail="OCRでテキストを取得できませんでした")

    # 3. Claude APIで評価額・年度・所在地・地番を構造化抽出
    try:
        extracted = await _extract_fixed_asset(ocr_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Claude抽出エラー: {e}")

    print(f"[DEBUG] extracted: {extracted}")

    shozaichi_raw = extracted.get("所在地") or ""
    chiban        = extracted.get("地番")   or ""
    if not shozaichi_raw or not chiban:
        raise HTTPException(status_code=422,
                            detail=f"所在地または地番を抽出できませんでした: {extracted}")

    shozaichi = _normalize_shozaichi(shozaichi_raw)
    print(f"[DEBUG] 所在地: {shozaichi_raw!r} → {shozaichi!r}")

    # 4. kintoneで所在地・地番が一致するレコードを検索
    try:
        record_id = await _search_kintone_record(shozaichi)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintone検索エラー: {e}")

    if record_id is None:
        raise HTTPException(status_code=404,
                            detail=f"kintoneに一致するレコードが見つかりません（所在地: {shozaichi} / 地番: {chiban}）")

    # 5. 一致レコードの評価額・年度を更新
    try:
        await _update_kintone_record(record_id, extracted)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintone更新エラー: {e}")

    # 6. LINEで完了通知
    notify_text = (
        f"✅ 固定資産税評価額の更新が完了しました\n"
        f"━━━━━━━━━━━━━━━\n"
        f"所在地：{shozaichi}\n"
        f"地番：{chiban}\n"
        f"年度：{extracted.get('年度') or '不明'}\n"
        f"評価額：{extracted.get('評価額') or '不明'}\n"
        f"kintoneレコードID：{record_id}\n"
        f"━━━━━━━━━━━━━━━"
    )
    if LINE_USER_ID:
        try:
            await _push_line_message(LINE_USER_ID, notify_text)
        except Exception as e:
            print(f"[WARN] LINE通知失敗: {e}")
    else:
        print("[INFO] LINE_USER_ID未設定のためLINE通知をスキップ")

    return {
        "status": "ok",
        "kintone_record_id": record_id,
        "extracted": extracted,
    }


# ══════════════════════════════════════════════════════════════
# POST /scan  Google Drive PDF → OCR → Claude抽出 → kintone登録
# ══════════════════════════════════════════════════════════════

_SCAN_FOLDER_CONFIG = {
    "相談カード": {
        "app_id_env":  "SOUZOKU_KINTONE_APP_ID",
        "token_env":   "SOUZOKU_KINTONE_API_TOKEN",
        "prompt": (
            "以下は相続相談カードのOCRテキストです。\n"
            "次の11項目をJSONで抽出してください。不明な場合はnullにしてください。\n"
            "日付はすべてYYYY-MM-DD形式で出力してください（例: 1975-03-15）。\n"
            '出力形式: {{"氏名": "...", "生年月日": "YYYY-MM-DD", "住所": "...", '
            '"電話番号": "...", "メールアドレス": "...", "被相続人名": "...", "続柄": "...", '
            '"被相続人生年月日": "YYYY-MM-DD", "被相続人死亡日": "YYYY-MM-DD", '
            '"被相続人住所": "...", "被相続人本籍": "..."}}\n'
            "JSONのみ出力してください。\n\n"
            "=== OCRテキスト ===\n{ocr_text}\n=== END ==="
        ),
    },
    "戸籍謄本": {
        "app_id_env":  "KOSEKI_KINTONE_APP_ID",
        "token_env":   "KOSEKI_KINTONE_API_TOKEN",
        "prompt": (
            "以下は戸籍謄本のOCRテキストです。\n"
            "次の8項目をJSONで抽出してください。不明な場合はnullにしてください。\n"
            "生年月日・死亡日はYYYY-MM-DD形式で出力してください（例: 1950-03-15）。\n"
            "婚姻関係・養子縁組は該当する人名を列挙してください（複数いる場合は読点区切り）。\n"
            '出力形式: {{"氏名": "...", "生年月日": "YYYY-MM-DD", "死亡日": "YYYY-MM-DD", '
            '"続柄": "...", "婚姻関係": "...", "養子縁組": "...", "本籍": "...", "筆頭者": "..."}}\n'
            "JSONのみ出力してください。\n\n"
            "=== OCRテキスト ===\n{ocr_text}\n=== END ==="
        ),
    },
    "通帳": {
        "app_id_env":  "KINTONE_SCAN_APP_ID_TSUCHOU",
        "token_env":   "KINTONE_SCAN_API_TOKEN_TSUCHOU",
        "prompt": (
            "以下は通帳のOCRテキストです。\n"
            "次の4項目をJSONで抽出してください。不明な場合はnullにしてください。\n"
            "残高はページ末尾の最新残高を円単位の整数で抽出してください（カンマ・円記号は除去）。\n"
            '出力形式: {{"金融機関名": "...", "口座番号": "...", "名義人": "...", "残高": 12345}}\n'
            "JSONのみ出力してください。\n\n"
            "=== OCRテキスト ===\n{ocr_text}\n=== END ==="
        ),
    },
}


class ScanRequest(BaseModel):
    model_config = {"populate_by_name": True}

    pdf_base64: str = Field(..., validation_alias=AliasChoices("pdf_base64", "pdfBase64", "fileData"))
    folder_name: str = Field(..., validation_alias=AliasChoices("folder_name", "folderName"))
    file_name: str = Field("", validation_alias=AliasChoices("file_name", "fileName"))


async def _extract_by_folder(ocr_text: str, folder_name: str) -> dict:
    """foldernameに応じてOCRテキストからClaude APIで情報を抽出する"""
    prompt = _SCAN_FOLDER_CONFIG[folder_name]["prompt"].format(ocr_text=ocr_text)
    response = await claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


async def _post_scan_to_kintone(app_id: str, api_token: str, fields: dict) -> str:
    """スキャン抽出結果をkintoneに新規登録してレコードIDを返す"""
    url = f"https://{KINTONE_SUBDOMAIN}.cybozu.com/k/v1/record.json"
    headers = {
        "X-Cybozu-API-Token": api_token,
        "Content-Type": "application/json",
    }
    # None のフィールドは送信しない（DATE/DATETIME型フィールドで400エラーになるため）
    record = {k: {"value": str(v)} for k, v in fields.items() if v is not None}
    body = {"app": app_id, "record": record}
    print(f"[DEBUG] scan kintone POST body: {json.dumps(body, ensure_ascii=False)}")
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, json=body)
        print(f"[DEBUG] scan kintone POST status: {resp.status_code} / {resp.text}")
        resp.raise_for_status()
        return resp.json()["id"]


@app.post("/scan")
async def scan(req: ScanRequest):
    """
    GASからbase64エンコードされたPDFとフォルダ名を受け取り、
    OCR → Claude抽出 → kintone登録する。

    folder_name: 相談カード / 戸籍謄本 / 通帳
    """
    if req.folder_name not in _SCAN_FOLDER_CONFIG:
        raise HTTPException(
            status_code=400,
            detail=f"未対応のフォルダ名: {req.folder_name}。対応値: {list(_SCAN_FOLDER_CONFIG.keys())}",
        )

    config = _SCAN_FOLDER_CONFIG[req.folder_name]
    app_id   = os.environ.get(config["app_id_env"], "")
    api_token = os.environ.get(config["token_env"], "")

    missing = [k for k, v in {
        "GOOGLE_VISION_API_KEY": GOOGLE_VISION_API_KEY,
        config["app_id_env"]: app_id,
        config["token_env"]: api_token,
    }.items() if not v]
    if missing:
        raise HTTPException(status_code=500, detail=f"環境変数が未設定です: {', '.join(missing)}")

    # 1. base64デコード
    try:
        pdf_bytes = base64.b64decode(req.pdf_base64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"base64デコードエラー: {e}")

    # 2. Google Vision API でOCR
    try:
        ocr_text = _ocr_pdf_bytes(pdf_bytes, GOOGLE_VISION_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRエラー: {e}")

    if not ocr_text.strip():
        raise HTTPException(status_code=422, detail="OCRでテキストを取得できませんでした")

    # 3. Claude で情報抽出
    try:
        extracted = await _extract_by_folder(ocr_text, req.folder_name)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Claude抽出エラー: {e}")

    print(f"[DEBUG] scan extracted ({req.folder_name}): {extracted}")

    # 相談カード・戸籍謄本: ファイル名・登録日時を付加
    if req.folder_name in ("相談カード", "戸籍謄本"):
        from datetime import datetime, timezone
        extracted["ファイル名"] = req.file_name
        extracted["登録日時"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4. kintone に登録
    try:
        record_id = await _post_scan_to_kintone(app_id, api_token, extracted)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintone登録エラー: {e}")

    return {
        "status": "ok",
        "folder_name": req.folder_name,
        "kintone_record_id": record_id,
        "extracted": extracted,
    }
@app.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, webhook_secret
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        customer_name = session.get("customer_details", {}).get("name")
        customer_email = session.get("customer_details", {}).get("email")
        amount = session.get("amount_total")
        print(f"決済完了: {customer_name} / {customer_email} / {amount}円")
        # kintoneに新規レコード作成
        kintone_url = f"https://{os.environ.get('KINTONE_SUBDOMAIN')}.cybozu.com/k/v1/record.json"
        kintone_headers = {
            "X-Cybozu-API-Token": os.environ.get("KINTONE_API_TOKEN"),
            "Content-Type": "application/json"
        }
        kintone_data = {
            "app": 21,
            "record": {
                "顧客名": {"value": customer_name},
                "メールアドレス": {"value": customer_email},
                "Stripe決済ID": {"value": session.get("id")},
                "入金状況": {"value": "入金済み"},
                "ステータス": {"value": "決済完了"}
            }
        }
        async with httpx.AsyncClient() as client:
            await client.post(kintone_url, headers=kintone_headers, json=kintone_data)

    return {"status": "ok"}
