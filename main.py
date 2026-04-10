import os
import re
import json
import hmac
import hashlib
import base64
import httpx
import anthropic
import fitz  # PyMuPDF
from fastapi import FastAPI, Request, HTTPException, UploadFile, File

app = FastAPI()

LINE_CHANNEL_SECRET = os.environ["LINE_CHANNEL_SECRET"]
LINE_CHANNEL_ACCESS_TOKEN = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
KINTONE_SUBDOMAIN = os.environ["KINTONE_SUBDOMAIN"]
KINTONE_APP_ID = os.environ["KINTONE_APP_ID"]
KINTONE_API_TOKEN = os.environ["KINTONE_API_TOKEN"]

REPLY_URL = "https://api.line.me/v2/bot/message/reply"
PUSH_URL  = "https://api.line.me/v2/bot/message/push"

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

        # 第1段階：レコード新規作成
        clean_reply, kintone_record = extract_marker(claude_reply, "KINTONE_RECORD")
        if kintone_record:
            kintone_record["LINEユーザーID"] = user_id
            kintone_record["status"] = "問い合わせ"
            kintone_record["業者名"] = kintone_record.get("問い合わせ業者名", "")
            user_business_names[user_id] = kintone_record["業者名"]
            record_id = await post_to_kintone(kintone_record)
            kintone_record_ids[user_id] = record_id
            claude_reply = clean_reply

        # 第2段階：既存レコードを更新
        clean_reply2, update_fields = extract_marker(claude_reply, "KINTONE_UPDATE")
        if update_fields:
            print(f"[DEBUG] KINTONE_UPDATE detected: {update_fields}")
            print(f"[DEBUG] stored record_id for user: {kintone_record_ids.get(user_id)!r}")
        if update_fields and user_id in kintone_record_ids:
            update_fields["業者名"] = user_business_names.get(user_id, "")
            await update_kintone_record(kintone_record_ids[user_id], update_fields)
            claude_reply = clean_reply2

        async with httpx.AsyncClient() as client:
            await client.post(
                REPLY_URL,
                headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
                json={
                    "replyToken": reply_token,
                    "messages": [{"type": "text", "text": claude_reply}],
                },
            )

    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════
# POST /ocr/fixed-asset  固定資産税評価額 OCR → kintone登録
# ══════════════════════════════════════════════════════════════

def _ocr_page_bytes(image_bytes: bytes, api_key: str) -> str:
    """1ページ分の画像バイトを Vision API でOCRしてテキストを返す"""
    import urllib.request
    content = base64.b64encode(image_bytes).decode("utf-8")
    body = json.dumps({
        "requests": [{
            "image": {"content": content},
            "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
            "imageContext": {"languageHints": ["ja", "en"]},
        }]
    }).encode("utf-8")
    url = f"https://vision.googleapis.com/v1/images:annotate?key={api_key}"
    req = urllib.request.Request(url, data=body,
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    responses = result.get("responses", [])
    if not responses:
        return ""
    annotation = responses[0].get("fullTextAnnotation")
    if annotation:
        return annotation.get("text", "")
    texts = responses[0].get("textAnnotations", [])
    return texts[0].get("description", "") if texts else ""


def _ocr_pdf_bytes(pdf_bytes: bytes, api_key: str) -> str:
    """PDFバイトを PyMuPDF で画像化して全ページOCRし、結合テキストを返す"""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages_text = []
    for i, page in enumerate(doc):
        mat = fitz.Matrix(3, 3)          # ≈ 216dpi
        pix = page.get_pixmap(matrix=mat)
        text = _ocr_page_bytes(pix.tobytes("png"), api_key)
        pages_text.append(f"--- ページ {i + 1} ---\n{text}" if text else f"--- ページ {i + 1} ---\n(テキストなし)")
    doc.close()
    return "\n\n".join(pages_text)


async def _extract_fixed_asset(ocr_text: str) -> dict:
    """OCRテキストから固定資産税評価額と年度を抽出して返す"""
    response = await claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": (
                "以下は固定資産税の課税明細書またはその関連書類のOCRテキストです。\n"
                "次の2項目を数値のみでJSONに抽出してください。不明な場合は null にしてください。\n"
                "\n"
                "- 評価額: 円単位の整数（例: 12345678）。カンマや「円」は除去すること。\n"
                "- 年度: 西暦4桁の整数（例: 令和6年度→2024、令和7年度→2025）。\n"
                "\n"
                '出力形式: {"評価額": 12345678, "年度": 2024}\n'
                "JSONのみ出力してください。\n\n"
                f"=== OCRテキスト ===\n{ocr_text}\n=== END ==="
            ),
        }],
    )
    raw = response.content[0].text.strip()
    # コードブロックを除去
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


async def _post_fixed_asset_to_kintone(extracted: dict) -> str:
    """抽出データをkintone不動産管理アプリに登録してレコードIDを返す"""
    url = f"https://{KINTONE_FUDOSAN_DOMAIN}.cybozu.com/k/v1/record.json"
    headers = {
        "X-Cybozu-API-Token": KINTONE_FUDOSAN_API_TOKEN_OCR,
        "Content-Type": "application/json",
    }

    # kintone NUMBERフィールドは文字列として送信する
    record = {
        "固定資産税評価額": {"value": str(extracted["評価額"]) if extracted.get("評価額") is not None else ""},
        "固定資産税評価年度": {"value": str(extracted["年度"]) if extracted.get("年度") is not None else ""},
    }

    body = {"app": KINTONE_FUDOSAN_APP_ID_OCR, "record": record}
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, json=body)
        resp.raise_for_status()
        return resp.json()["id"]


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
        "LINE_USER_ID": LINE_USER_ID,
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

    # 3. Claude APIで評価額・年度を構造化抽出
    try:
        extracted = await _extract_fixed_asset(ocr_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Claude抽出エラー: {e}")

    # 4. kintoneに登録
    try:
        record_id = await _post_fixed_asset_to_kintone(extracted)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintone登録エラー: {e}")

    # 5. LINEで完了通知
    notify_text = (
        f"✅ 固定資産税評価額の登録が完了しました\n"
        f"━━━━━━━━━━━━━━━\n"
        f"年度：{extracted.get('年度') or '不明'}\n"
        f"評価額：{extracted.get('評価額') or '不明'}\n"
        f"kintoneレコードID：{record_id}\n"
        f"━━━━━━━━━━━━━━━"
    )
    try:
        await _push_line_message(LINE_USER_ID, notify_text)
    except Exception as e:
        # 通知失敗はログのみ（登録自体は成功しているため400系にしない）
        print(f"[WARN] LINE通知失敗: {e}")

    return {
        "status": "ok",
        "kintone_record_id": record_id,
        "extracted": extracted,
    }
