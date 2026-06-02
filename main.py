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
from fastapi import FastAPI, Request, HTTPException, UploadFile, File

app = FastAPI()

from cloudsign_webhook import router as cloudsign_router
from document_webhook import router as document_router
app.include_router(cloudsign_router)
app.include_router(document_router)


@app.get("/health")
async def health():
    """èµ·åç¢ºèªã»ä¾å­ã©ã¤ãã©ãªã®ã¤ã³ãã¼ããã§ãã¯"""
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

# OCRåºå®è³ç£ã¨ã³ããã¤ã³ãç¨ã®ç°å¢å¤æ°ï¼èµ·åæã§ã¯ãªããªã¯ã¨ã¹ãæã«ãã§ãã¯ï¼
GOOGLE_VISION_API_KEY        = os.environ.get("GOOGLE_VISION_API_KEY")
KINTONE_FUDOSAN_DOMAIN       = os.environ.get("KINTONE_DOMAIN", os.environ.get("KINTONE_SUBDOMAIN", ""))
KINTONE_FUDOSAN_APP_ID_OCR   = os.environ.get("KINTONE_FUDOSAN_APP_ID", "")
KINTONE_FUDOSAN_API_TOKEN_OCR = os.environ.get("KINTONE_FUDOSAN_API_TOKEN", "")
LINE_USER_ID                 = os.environ.get("LINE_USER_ID", "")

claude_client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """ãåéè¿½å ã»æåã®ã¡ãã»ã¼ã¸ã¸ã®èªåè¿ä¿¡ã
ã¯ããã¾ãã¦ã
å¤§éæ³å¾äºåæãæå¹æ´ç¨å°éçªå£ã§ãã

åéã®æå¹æ´ç¨ã«ã¤ãã¦ã®ãç¸è«ã
LINEã§æ¿ã£ã¦ããã¾ãã

æå¹ã®å¯è½æ§ãç¢ºèªãããã
ä»¥ä¸ã®é ç®ããåç­ãã ããã

âââââââââââââââ
â åµæ¨©èåï¼ä¾ï¼ã¢ã³ã ãã¬ã¤ã¯ãªã©ï¼
â»åµæ¨©ååä¼ç¤¾ãæ³å¾äºåæããéç¥ãè¨´ç¶ãæ¯æç£ä¿ãæ¥ã¦ããå ´åã¯ããã®åå

â¡ããããã®åå¥ææ
â»ä¸æãªå ´åã¯ãä¸æãã¨ãè¨å¥ãã ãã

â¢ããããã®æçµè¿æ¸æ¥
ï¼1ï¼ä¸æãªå ´åã¯ãä¸æãã¨ãè¨å¥ãã ãã
ï¼2ï¼éå»5å¹´ä»¥åã«è¿æ¸ãã¾ãããï¼

â£10å¹´ä»¥åã«è£å¤æããä»¥ä¸ã®æ¸é¡ã¯å±ãã¾ãããï¼
ã»1 è¨´ç¶ãå±ãã
ã»2 æ¯æç£ä¿ãå±ãã
ã»3 ãã®ä»ã®ç£ä¿éç¥ãå±ãã
ã»4 ä½ãå±ãã¦ããªã
â»çªå·ã§ãç­ããã ãã

â¤ãæåã«éç¥æ¸ã»è¨´ç¶ã»æ¯æç£ä¿ãªã©ã®
æ¸é¡ããããã¾ããã
åçãéã£ã¦ããã ãã¨
ããæ­£ç¢ºã«ç¢ºèªã§ãã¾ã
âââââââââââââââ

ãä¸æãªç¹ã¯ãã®ã¾ã¾ãæ°è»½ã«ãéããã ããã

ãåç­ã«å¯¾ããClaudeã®å¤å®ã­ã¸ãã¯ã

â£ã§1ã¾ãã¯2ã¨ç­ããå ´åï¼
ãè£å¤æããã®æ¸é¡ãå±ãã¦ããã¨ã®ãã¨ã§ãã­ã
ç¾å¨ãè¨´è¨ã»æ¯æç£ä¿ã®æç¶ãã
é²è¡ä¸­ãã©ããã«ãã£ã¦å¯¾å¿ãç°ãªãã¾ãã

ã¾ã æç¶ããé²è¡ä¸­ã®å ´åã¯
ç­å¼æ¸ç­ã§æå¹æ´ç¨ãä¸»å¼µã§ããå¯è½æ§ãããã¾ãã

è©³ããã¯æå½å¼è­·å£«ãç¢ºèªãããã¾ãã®ã§
å¼ãç¶ãæå ±ããç¥ãããã ãããã

â£ã§4ã¨ç­ããå ´åï¼è¿½å è³ªå
ãæ¿ç¥ãã¾ããã
ä»åã®åµåã«ã¤ãã¦
ä¿¡ç¨æå ±ï¼CICãJICCãªã©ï¼ã
ç¢ºèªãã¦ç¥ãã¾ãããï¼

ã»ã¯ã
ã»ãããã

ãæå¹å¯è½æ§ããã®å ´åã
ããåç­ãããã¨ããããã¾ãã
ç¢ºèªã®çµæãæå¹æ´ç¨ã§ããå¯è½æ§ãããã¾ãã

æ­£å¼ã«ãä¾é ¼ãããå ´åã¯
è¿½å ã§ä»¥ä¸ããæããã ããã

âââââââââââââââ
â ãåå
â¡ãä½æ
â¢çå¹´ææ¥
â£é»è©±çªå·
â¤ã¡ã¼ã«ã¢ãã¬ã¹
â¥ä»åã®åµåãã©ã®ããã«ç¥ãã¾ãããï¼
ã»åµæ¨©èããã®éç¥æ¸ãå±ãã
ã»è£å¤æããè¨´ç¶ã»æ¯æç£ä¿ãå±ãã
ã»ä¿¡ç¨æå ±ãç¢ºèªãã¦ç¥ã£ã
ã»ãã®ä»
âââââââââââââââã

ãkintoneç»é²ã«ã¤ãã¦ã

â ç¬¬1æ®µéï¼ä»¥ä¸ã®5é ç®ããã¹ã¦æã£ãããè¿ä¿¡ã¡ãã»ã¼ã¸ã®æ«å°¾ã«åºåãã¦ãã ãããã¦ã¼ã¶ã¼ã«ã¯è¦ãã¾ããã

[KINTONE_RECORD]
{
  "åãåããæ¥­èå": "ï¼åµæ¨©èåã®å¤ï¼",
  "åå¥ææ_ãã­ã¹ã": "ï¼åå¥ææã®å¤ï¼",
  "æçµè¿æ¸æ¥_ãã­ã¹ã": "ï¼æçµè¿æ¸æ¥ã®å¤ï¼",
  "è£å¤ææ¸é¡": "ï¼è£å¤æããã®æ¸é¡ã®æç¡ã®å¤ï¼",
  "ä¿¡ç¨æå ±ç¢ºèª": "ï¼ä¿¡ç¨æå ±ããç¥ã£ããã©ããã®å¤ï¼"
}
[/KINTONE_RECORD]

5é ç®ï¼åµæ¨©èåã»åå¥ææã»æçµè¿æ¸æ¥ã»è£å¤æããã®æ¸é¡ã®æç¡ã»ä¿¡ç¨æå ±ããç¥ã£ããã©ãã

â ç¬¬2æ®µéï¼ãååã»ãä½æã»çå¹´ææ¥ã»é»è©±çªå·ã»ã¡ã¼ã«ã¢ãã¬ã¹ã®5é ç®ããã¹ã¦æã£ãããè¿ä¿¡ã¡ãã»ã¼ã¸ã®æ«å°¾ã«åºåãã¦ãã ãããã¦ã¼ã¶ã¼ã«ã¯è¦ãã¾ããã

[KINTONE_UPDATE]
{
  "é¡§å®¢å": "ï¼ãååã®å¤ï¼",
  "ä½æ": "ï¼ãä½æã®å¤ï¼",
  "çå¹´ææ¥": "ï¼çå¹´ææ¥ã®å¤ï¼",
  "é»è©±çªå·": "ï¼é»è©±çªå·ã®å¤ï¼",
  "ã¡ã¼ã«ã¢ãã¬ã¹": "ï¼ã¡ã¼ã«ã¢ãã¬ã¹ã®å¤ï¼"
}
[/KINTONE_UPDATE]"""

# ã¦ã¼ã¶ã¼IDãã¨ã®ä¼è©±å±¥æ­´ãä¿æ
conversation_histories: dict[str, list] = {}

# ã¦ã¼ã¶ã¼IDãã¨ã®kintoneã¬ã³ã¼ãIDãä¿æ
kintone_record_ids: dict[str, str] = {}

# ã¦ã¼ã¶ã¼IDãã¨ã®æ¥­èåãä¿æï¼ç¬¬2æ®µéæ´æ°ã§ä½¿ç¨ï¼
user_business_names: dict[str, str] = {}


def verify_signature(body: bytes, signature: str) -> bool:
    hash = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"), body, hashlib.sha256
    ).digest()
    expected = base64.b64encode(hash).decode("utf-8")
    return hmac.compare_digest(expected, signature)


async def post_to_kintone(record: dict) -> str:
    """ã¬ã³ã¼ããæ°è¦ä½æããã¬ã³ã¼ãIDãè¿ã"""
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
    """æ¢å­ã¬ã³ã¼ããæ´æ°ãã"""
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
    """æå®ã¿ã°ã®ãã¼ã¿ãæ½åºãããã¼ã«ã¼ãé¤å»ãããã­ã¹ããè¿ã"""
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

        # ç¬¬1æ®µéï¼ã¬ã³ã¼ãæ°è¦ä½æ
        clean_reply, kintone_record = extract_marker(claude_reply, "KINTONE_RECORD")
        if kintone_record:
            kintone_record["LINEã¦ã¼ã¶ã¼ID"] = user_id
            kintone_record["status"] = "åãåãã"
            kintone_record["æ¥­èå"] = kintone_record.get("åãåããæ¥­èå", "")
            user_business_names[user_id] = kintone_record["æ¥­èå"]
            record_id = await post_to_kintone(kintone_record)
            kintone_record_ids[user_id] = record_id
            claude_reply = clean_reply

        # ç¬¬2æ®µéï¼æ¢å­ã¬ã³ã¼ããæ´æ°
        clean_reply2, update_fields = extract_marker(claude_reply, "KINTONE_UPDATE")
        if update_fields:
            print(f"[DEBUG] KINTONE_UPDATE detected: {update_fields}")
            print(f"[DEBUG] stored record_id for user: {kintone_record_ids.get(user_id)!r}")
        if update_fields and user_id in kintone_record_ids:
            update_fields["æ¥­èå"] = user_business_names.get(user_id, "")
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


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# POST /ocr/fixed-asset  åºå®è³ç£ç¨è©ä¾¡é¡ OCR â kintoneç»é²
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def _ocr_pdf_bytes(pdf_bytes: bytes, api_key: str) -> str:
    """PDFã Vision API ã® files:annotate ã«ç´æ¥éã£ã¦OCRããï¼PyMuPDFä¸è¦ï¼"""
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

    # files:annotate ã®ã¬ã¹ãã³ã¹æ§é :
    # result["responses"][0]["responses"] â ãã¼ã¸ãã¨ã®çµæãªã¹ã
    pages_text = []
    for page_resp in result.get("responses", [{}])[0].get("responses", []):
        annotation = page_resp.get("fullTextAnnotation")
        if annotation:
            pages_text.append(annotation.get("text", ""))
    return "\n\n".join(pages_text)


async def _extract_fixed_asset(ocr_text: str) -> dict:
    """OCRãã­ã¹ãããåºå®è³ç£ç¨è©ä¾¡é¡ã»å¹´åº¦ã»æå¨å°ã»å°çªãæ½åºãã¦è¿ã"""
    response = await claude_client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{
            "role": "user",
            "content": (
                "ä»¥ä¸ã¯åºå®è³ç£ç¨ã®èª²ç¨æç´°æ¸ã¾ãã¯ãã®é¢é£æ¸é¡ã®OCRãã­ã¹ãã§ãã\n"
                "æ¬¡ã®4é ç®ãJSONã§æ½åºãã¦ãã ãããä¸æãªå ´åã¯ null ã«ãã¦ãã ããã\n"
                "\n"
                "- è©ä¾¡é¡: ååä½ã®æ´æ°ï¼ä¾: 12345678ï¼ãã«ã³ãããåãã¯é¤å»ãããã¨ã\n"
                "- å¹´åº¦: è¥¿æ¦4æ¡ã®æ´æ°ï¼ä¾: ä»¤å6å¹´åº¦â2024ãä»¤å7å¹´åº¦â2025ï¼ã\n"
                "- æå¨å°: ä¸åç£ã®æå¨å°ï¼ä¾: å¼ççæ¸ç°å¸åæ²¢ï¼ãçªå°ã¯å«ããå¸åºçºæã»å¤§å­ã¾ã§ã\n"
                "- å°çª: å°çªã¾ãã¯å®¶å±çªå·ï¼ä¾: 123-4ï¼ã\n"
                "\n"
                'åºåå½¢å¼: {"è©ä¾¡é¡": 12345678, "å¹´åº¦": 2024, "æå¨å°": "å¼ççæ¸ç°å¸åæ²¢", "å°çª": "123-4"}\n'
                "JSONã®ã¿åºåãã¦ãã ããã\n\n"
                f"=== OCRãã­ã¹ã ===\n{ocr_text}\n=== END ==="
            ),
        }],
    )
    raw = response.content[0].text.strip()
    if "```" in raw:
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw.strip())


# ä¸ç®ã®æ°å­å¤æãã¼ãã«ï¼ç®ç¨æ°å­âæ¼¢æ°å­ï¼
_CHOME_KANJI = {
    1:'ä¸', 2:'äº', 3:'ä¸', 4:'å', 5:'äº',
    6:'å­', 7:'ä¸', 8:'å«', 9:'ä¹', 10:'å',
    11:'åä¸', 12:'åäº', 13:'åä¸', 14:'åå', 15:'åäº',
    16:'åå­', 17:'åä¸', 18:'åå«', 19:'åä¹', 20:'äºå',
}

def _normalize_shozaichi(address: str) -> str:
    """OCRæ½åºã®æå¨å°ãkintoneæ¤ç´¢ç¨ã«æ­£è¦åãã

    å¤æåå®¹:
    1. é½éåºçåãåé¤ï¼å¼ççå·å£å¸âå·å£å¸ï¼
    2. ä¸ç®åã®ç®ç¨æ°å­ãæ¼¢æ°å­ã«å¤æï¼3ä¸ç®âä¸ä¸ç®ï¼
    3. çªå°è¡¨è¨ãçµ±ä¸ï¼32-6 / 32ã®6 â 32çªå°6ï¼
    """
    if not address:
        return address
    # 1. é½éåºçåãåé¤
    address = re.sub(r'^(åæµ·é|æ±äº¬é½|å¤§éªåº|äº¬é½åº|.{2,3}ç)', '', address)
    # 2. ä¸ç®åã®ç®ç¨æ°å­âæ¼¢æ°å­
    def _chome_to_kanji(m):
        n = int(m.group(1))
        return _CHOME_KANJI.get(n, str(n)) + 'ä¸ç®'
    address = re.sub(r'(\d+)ä¸ç®', _chome_to_kanji, address)
    # 3. çªå°è¡¨è¨ãçµ±ä¸ï¼ãã¤ãã³ã»ãã®ãâãçªå°ãï¼
    address = re.sub(r'(\d+)[ï¼ã¼\-ã®](\d+)(çªå°?|å·)?$', r'\1çªå°\2', address)
    return address.strip()


def _normalize_chiban(chiban: str) -> str:
    """å°çªã®è¡¨è¨ãçµ±ä¸ããï¼32-6 / 32ã®6 â 32çªå°6ï¼"""
    if not chiban:
        return chiban
    chiban = re.sub(r'(\d+)[ï¼ã¼\-ã®](\d+)(çªå°?|å·)?$', r'\1çªå°\2', chiban)
    return chiban.strip()


async def _search_kintone_record(shozaichi: str) -> str | None:
    """æ­£è¦åæ¸ã¿æå¨å°ã§kintoneã¬ã³ã¼ããé¨åä¸è´æ¤ç´¢ãã¦ã¬ã³ã¼ãIDãè¿ããè¦ã¤ãããªãå ´åã¯Noneã"""
    import urllib.parse
    FIELD_SHOZAICHI = "æå¨"
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
            raise Exception(f"kintoneæ¤ç´¢ã¨ã©ã¼ {resp.status_code}: {resp.text}")
        records = resp.json().get("records", [])
        if not records:
            return None
        return records[0]["$id"]["value"]


async def _update_kintone_record(record_id: str, extracted: dict) -> None:
    """åºå®è³ç£ç¨è©ä¾¡é¡ã»å¹´åº¦ãæ¢å­ã¬ã³ã¼ãã«ä¸æ¸ãæ´æ°ãã"""
    url = f"https://{KINTONE_FUDOSAN_DOMAIN}.cybozu.com/k/v1/record.json"
    headers = {
        "X-Cybozu-API-Token": KINTONE_FUDOSAN_API_TOKEN_OCR,
        "Content-Type": "application/json",
    }
    record = {
        "åºå®è³ç£ç¨è©ä¾¡é¡":   {"value": str(extracted["è©ä¾¡é¡"]) if extracted.get("è©ä¾¡é¡") is not None else ""},
        "åºå®è³ç£ç¨è©ä¾¡å¹´åº¦": {"value": str(extracted["å¹´åº¦"])   if extracted.get("å¹´åº¦")   is not None else ""},
    }
    body = {"app": KINTONE_FUDOSAN_APP_ID_OCR, "id": record_id, "record": record}
    print(f"[DEBUG] kintone PUT body: {json.dumps(body, ensure_ascii=False)}")
    async with httpx.AsyncClient() as client:
        resp = await client.put(url, headers=headers, json=body)
        print(f"[DEBUG] kintone PUT status: {resp.status_code}")
        print(f"[DEBUG] kintone PUT response: {resp.text}")
        if not resp.is_success:
            raise Exception(f"kintoneæ´æ°ã¨ã©ã¼ {resp.status_code}: {resp.text}")


async def _push_line_message(user_id: str, text: str) -> None:
    """LINE Push APIã§ã¡ãã»ã¼ã¸ãéã"""
    async with httpx.AsyncClient() as client:
        await client.post(
            PUSH_URL,
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
        )


@app.post("/ocr/fixed-asset")
async def ocr_fixed_asset(file: UploadFile = File(...)):
    """
    PDFãã¢ããã­ã¼ãããã¨åºå®è³ç£ç¨è©ä¾¡é¡ã»å¹´åº¦ãOCRã§æ½åºã
    kintoneã«ç»é²ãã¦LINEã§éç¥ããã
    """
    # â ç°å¢å¤æ°ãã§ãã¯ â
    missing = [k for k, v in {
        "GOOGLE_VISION_API_KEY": GOOGLE_VISION_API_KEY,
        "KINTONE_DOMAIN or KINTONE_SUBDOMAIN": KINTONE_FUDOSAN_DOMAIN,
        "KINTONE_FUDOSAN_APP_ID": KINTONE_FUDOSAN_APP_ID_OCR,
        "KINTONE_FUDOSAN_API_TOKEN": KINTONE_FUDOSAN_API_TOKEN_OCR,
    }.items() if not v]
    if missing:
        raise HTTPException(status_code=500,
                            detail=f"ç°å¢å¤æ°ãæªè¨­å®ã§ã: {', '.join(missing)}")

    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFãã¡ã¤ã«ãéä¿¡ãã¦ãã ãã")

    # 1. PDFãèª­ã¿è¾¼ã
    pdf_bytes = await file.read()

    # 2. Google Cloud Vision API ã§OCR
    try:
        ocr_text = _ocr_pdf_bytes(pdf_bytes, GOOGLE_VISION_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRã¨ã©ã¼: {e}")

    if not ocr_text.strip():
        raise HTTPException(status_code=422, detail="OCRã§ãã­ã¹ããåå¾ã§ãã¾ããã§ãã")

    # 3. Claude APIã§è©ä¾¡é¡ã»å¹´åº¦ã»æå¨å°ã»å°çªãæ§é åæ½åº
    try:
        extracted = await _extract_fixed_asset(ocr_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Claudeæ½åºã¨ã©ã¼: {e}")

    print(f"[DEBUG] extracted: {extracted}")

    shozaichi_raw = extracted.get("æå¨å°") or ""
    chiban        = extracted.get("å°çª")   or ""
    if not shozaichi_raw or not chiban:
        raise HTTPException(status_code=422,
                            detail=f"æå¨å°ã¾ãã¯å°çªãæ½åºã§ãã¾ããã§ãã: {extracted}")

    shozaichi = _normalize_shozaichi(shozaichi_raw)
    print(f"[DEBUG] æå¨å°: {shozaichi_raw!r} â {shozaichi!r}")

    # 4. kintoneã§æå¨å°ã»å°çªãä¸è´ããã¬ã³ã¼ããæ¤ç´¢
    try:
        record_id = await _search_kintone_record(shozaichi)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintoneæ¤ç´¢ã¨ã©ã¼: {e}")

    if record_id is None:
        raise HTTPException(status_code=404,
                            detail=f"kintoneã«ä¸è´ããã¬ã³ã¼ããè¦ã¤ããã¾ããï¼æå¨å°: {shozaichi} / å°çª: {chiban}ï¼")

    # 5. ä¸è´ã¬ã³ã¼ãã®è©ä¾¡é¡ã»å¹´åº¦ãæ´æ°
    try:
        await _update_kintone_record(record_id, extracted)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintoneæ´æ°ã¨ã©ã¼: {e}")

    # 6. LINEã§å®äºéç¥
    notify_text = (
        f"â åºå®è³ç£ç¨è©ä¾¡é¡ã®æ´æ°ãå®äºãã¾ãã\n"
        f"âââââââââââââââ\n"
        f"æå¨å°ï¼{shozaichi}\n"
        f"å°çªï¼{chiban}\n"
        f"å¹´åº¦ï¼{extracted.get('å¹´åº¦') or 'ä¸æ'}\n"
        f"è©ä¾¡é¡ï¼{extracted.get('è©ä¾¡é¡') or 'ä¸æ'}\n"
        f"kintoneã¬ã³ã¼ãIDï¼{record_id}\n"
        f"âââââââââââââââ"
    )
    if LINE_USER_ID:
        try:
            await _push_line_message(LINE_USER_ID, notify_text)
        except Exception as e:
            print(f"[WARN] LINEéç¥å¤±æ: {e}")
    else:
        print("[INFO] LINE_USER_IDæªè¨­å®ã®ããLINEéç¥ãã¹ã­ãã")

    return {
        "status": "ok",
        "kintone_record_id": record_id,
        "extracted": extracted,
    }


# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
# POST /scan  Google Drive PDF â OCR â Claudeæ½åº â kintoneç»é²
# ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

_SCAN_FOLDER_CONFIG = {
    "ç¸è«ã«ã¼ã": {
        "app_id_env":  "SOUZOKU_KINTONE_APP_ID",
        "token_env":   "SOUZOKU_KINTONE_API_TOKEN",
        "prompt": (
            "ä»¥ä¸ã¯ç¸ç¶ç¸è«ã«ã¼ãã®OCRãã­ã¹ãã§ãã\n"
            "æ¬¡ã®11é ç®ãJSONã§æ½åºãã¦ãã ãããä¸æãªå ´åã¯nullã«ãã¦ãã ããã\n"
            "æ¥ä»ã¯ãã¹ã¦YYYY-MM-DDå½¢å¼ã§åºåãã¦ãã ããï¼ä¾: 1975-03-15ï¼ã\n"
            'åºåå½¢å¼: {{"æ°å": "...", "çå¹´ææ¥": "YYYY-MM-DD", "ä½æ": "...", '
            '"é»è©±çªå·": "...", "ã¡ã¼ã«ã¢ãã¬ã¹": "...", "è¢«ç¸ç¶äººå": "...", "ç¶æ": "...", '
            '"è¢«ç¸ç¶äººçå¹´ææ¥": "YYYY-MM-DD", "è¢«ç¸ç¶äººæ­»äº¡æ¥": "YYYY-MM-DD", '
            '"è¢«ç¸ç¶äººä½æ": "...", "è¢«ç¸ç¶äººæ¬ç±": "..."}}\n'
            "JSONã®ã¿åºåãã¦ãã ããã\n\n"
            "=== OCRãã­ã¹ã ===\n{ocr_text}\n=== END ==="
        ),
    },
    "æ¸ç±è¬æ¬": {
        "app_id_env":  "KOSEKI_KINTONE_APP_ID",
        "token_env":   "KOSEKI_KINTONE_API_TOKEN",
        "prompt": (
            "ä»¥ä¸ã¯æ¸ç±è¬æ¬ã®OCRãã­ã¹ãã§ãã\n"
            "æ¬¡ã®8é ç®ãJSONã§æ½åºãã¦ãã ãããä¸æãªå ´åã¯nullã«ãã¦ãã ããã\n"
            "çå¹´ææ¥ã»æ­»äº¡æ¥ã¯YYYY-MM-DDå½¢å¼ã§åºåãã¦ãã ããï¼ä¾: 1950-03-15ï¼ã\n"
            "å©å§»é¢ä¿ã»é¤å­ç¸çµã¯è©²å½ããäººåãåæãã¦ãã ããï¼è¤æ°ããå ´åã¯èª­ç¹åºåãï¼ã\n"
            'åºåå½¢å¼: {{"æ°å": "...", "çå¹´ææ¥": "YYYY-MM-DD", "æ­»äº¡æ¥": "YYYY-MM-DD", '
            '"ç¶æ": "...", "å©å§»é¢ä¿": "...", "é¤å­ç¸çµ": "...", "æ¬ç±": "...", "ç­é ­è": "..."}}\n'
            "JSONã®ã¿åºåãã¦ãã ããã\n\n"
            "=== OCRãã­ã¹ã ===\n{ocr_text}\n=== END ==="
        ),
    },
    "éå¸³": {
        "app_id_env":  "KINTONE_SCAN_APP_ID_TSUCHOU",
        "token_env":   "KINTONE_SCAN_API_TOKEN_TSUCHOU",
        "prompt": (
            "ä»¥ä¸ã¯éå¸³ã®OCRãã­ã¹ãã§ãã\n"
            "æ¬¡ã®4é ç®ãJSONã§æ½åºãã¦ãã ãããä¸æãªå ´åã¯nullã«ãã¦ãã ããã\n"
            "æ®é«ã¯ãã¼ã¸æ«å°¾ã®ææ°æ®é«ãååä½ã®æ´æ°ã§æ½åºãã¦ãã ããï¼ã«ã³ãã»åè¨å·ã¯é¤å»ï¼ã\n"
            'åºåå½¢å¼: {{"éèæ©é¢å": "...", "å£åº§çªå·": "...", "åç¾©äºº": "...", "æ®é«": 12345}}\n'
            "JSONã®ã¿åºåãã¦ãã ããã\n\n"
            "=== OCRãã­ã¹ã ===\n{ocr_text}\n=== END ==="
        ),
    },
}


class ScanRequest(BaseModel):
    model_config = {"populate_by_name": True}

    pdf_base64: str = Field(..., validation_alias=AliasChoices("pdf_base64", "pdfBase64", "fileData"))
    folder_name: str = Field(..., validation_alias=AliasChoices("folder_name", "folderName"))
    file_name: str = Field("", validation_alias=AliasChoices("file_name", "fileName"))


async def _extract_by_folder(ocr_text: str, folder_name: str) -> dict:
    """foldernameã«å¿ãã¦OCRãã­ã¹ãããClaude APIã§æå ±ãæ½åºãã"""
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
    """ã¹ã­ã£ã³æ½åºçµæãkintoneã«æ°è¦ç»é²ãã¦ã¬ã³ã¼ãIDãè¿ã"""
    url = f"https://{KINTONE_SUBDOMAIN}.cybozu.com/k/v1/record.json"
    headers = {
        "X-Cybozu-API-Token": api_token,
        "Content-Type": "application/json",
    }
    # None ã®ãã£ã¼ã«ãã¯éä¿¡ããªãï¼DATE/DATETIMEåãã£ã¼ã«ãã§400ã¨ã©ã¼ã«ãªãããï¼
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
    GASããbase64ã¨ã³ã³ã¼ããããPDFã¨ãã©ã«ãåãåãåãã
    OCR â Claudeæ½åº â kintoneç»é²ããã

    folder_name: ç¸è«ã«ã¼ã / æ¸ç±è¬æ¬ / éå¸³
    """
    if req.folder_name not in _SCAN_FOLDER_CONFIG:
        raise HTTPException(
            status_code=400,
            detail=f"æªå¯¾å¿ã®ãã©ã«ãå: {req.folder_name}ãå¯¾å¿å¤: {list(_SCAN_FOLDER_CONFIG.keys())}",
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
        raise HTTPException(status_code=500, detail=f"ç°å¢å¤æ°ãæªè¨­å®ã§ã: {', '.join(missing)}")

    # 1. base64ãã³ã¼ã
    try:
        pdf_bytes = base64.b64decode(req.pdf_base64)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"base64ãã³ã¼ãã¨ã©ã¼: {e}")

    # 2. Google Vision API ã§OCR
    try:
        ocr_text = _ocr_pdf_bytes(pdf_bytes, GOOGLE_VISION_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OCRã¨ã©ã¼: {e}")

    if not ocr_text.strip():
        raise HTTPException(status_code=422, detail="OCRã§ãã­ã¹ããåå¾ã§ãã¾ããã§ãã")

    # 3. Claude ã§æå ±æ½åº
    try:
        extracted = await _extract_by_folder(ocr_text, req.folder_name)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Claudeæ½åºã¨ã©ã¼: {e}")

    print(f"[DEBUG] scan extracted ({req.folder_name}): {extracted}")

    # ç¸è«ã«ã¼ãã»æ¸ç±è¬æ¬: ãã¡ã¤ã«åã»ç»é²æ¥æãä»å 
    if req.folder_name in ("ç¸è«ã«ã¼ã", "æ¸ç±è¬æ¬"):
        from datetime import datetime, timezone
        extracted["ãã¡ã¤ã«å"] = req.file_name
        extracted["ç»é²æ¥æ"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4. kintone ã«ç»é²
    try:
        record_id = await _post_scan_to_kintone(app_id, api_token, extracted)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"kintoneç»é²ã¨ã©ã¼: {e}")

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
        print(f"æ±ºæ¸å®äº: {customer_name} / {customer_email} / {amount}å")
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
