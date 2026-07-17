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
from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Form, BackgroundTasks

app = FastAPI()

from cloudsign_webhook import router as cloudsign_router
from document_webhook import router as document_router
from hub.dispatch import router as hub_dispatch_router
from dispatch_bot.router import router as dispatch_bot_router
from koseki_ingest import router as koseki_ingest_router
from registry_ingest import router as registry_ingest_router
from bank_ingest import router as bank_ingest_router
from sortation_ingest import router as sortation_ingest_router
from valuation_ingest import router as valuation_ingest_router
app.include_router(cloudsign_router)
app.include_router(document_router)
app.include_router(hub_dispatch_router)
app.include_router(dispatch_bot_router)
app.include_router(koseki_ingest_router)
app.include_router(registry_ingest_router)
app.include_router(bank_ingest_router)
app.include_router(sortation_ingest_router)
app.include_router(valuation_ingest_router)

from chat_responder import (
    get_app21_record,
    classify_routing,
    handle_customer_message,
    handle_claude_outage,
    get_approval_record,
    mark_approval_sent,
    send_line_push,
    save_to_chatlog,
    ATTORNEY_LINE_USER_ID,
)
from claude_gateway import (
    ClaudeUnavailableError,
    create_message_with_fallback,
    extract_text,
)
from daily_healthcheck import start_healthcheck_scheduler
from hub import kintone as hub_kintone
from hub import notify as hub_notify
from hub.webhook_auth import extract_record_id, verify_token
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式）

import logging
logger = logging.getLogger("main")


# ロガー出力配線は hub/logging_setup へ集約（PR-4b: CLI と共有）。従来名 alias で
# 既存参照（test_logging_wiring.py の main._configure_app_logging）を維持する。
from hub.logging_setup import configure_app_logging as _configure_app_logging  # noqa: E402

# uvicorn が `main:app` を import した時点で1回だけ配線する（起動経路で必ず通る）
_configure_app_logging()

# App 29（承認キュー）のハブ経由接続（T0-1。挙動は従来の get_approval_record と同等）
_APP_APPROVAL = hub_kintone.KintoneApp("App 29 (承認キュー)", "APP_APPROVAL", "TOKEN_APPROVAL")


@app.on_event("startup")
async def _on_startup():
    """定期ジョブの登録・起動（日次死活監視 7:00 / 返送期限監視 8:00 JST）"""
    # P1-114: service auth registry の起動時 fail-fast。dual-accept flag ON かつ
    # SERVICE_HMAC_KEY_REGISTRY が壊れ JSON/構造違反なら ServiceAuthConfigError を
    # そのまま送出して起動を失敗させる（署名リクエスト毎の沈黙 500 の排除）。
    # flag OFF は registry 非参照＝現行挙動不変。
    from hub.service_auth import (validate_registry_startup,
                                  validate_legacy_disabled_paths_startup)
    validate_registry_startup()
    # RV-04c H07: 旧 query 停止 path list の起動時 strict 検証（異常形は固定文言で起動停止）。
    validate_legacy_disabled_paths_startup()
    # RV-04c D2-M01: KINTONE_WEBHOOK_TOKEN_NEXT の残置を起動ログに固定文言で警告（値は出さない）。
    from daily_healthcheck import check_next_token_residual
    if check_next_token_residual():
        logger.warning("[RV04c] KINTONE_WEBHOOK_TOKEN_NEXT residual "
                       "(rotation cleanup pending; owner=大野)")
    from hub.return_deadline import register_return_deadline_job
    register_return_deadline_job()
    start_healthcheck_scheduler()
    # RV-05-13: flag ON のみ、放置 receipt の可視化 reconciliation を1回実行（再処理しない）。
    # M-06: flag OFF は hub.durable_inbound を import せず（env 直読み）一切実行しない。
    if os.environ.get("INBOUND_EVENT_DURABLE_ENABLED", "").strip().lower() \
            in ("1", "true", "on", "yes"):
        try:
            from hub.durable_inbound import reconcile_stale_seconds
            from hub.ingestion_receipt import reconcile_stale
            stats = await reconcile_stale(reconcile_stale_seconds())
            logger.info("[RV05] startup reconcile: to_pending_retry=%s to_unknown=%s",
                        emit(stats["to_pending_retry"], "count", "log", "operator"),
                        emit(stats["to_unknown"], "count", "log", "operator"))
        except Exception:
            logger.warning("[RV05] startup reconcile skipped (db not ready)")


@app.on_event("shutdown")
async def _on_shutdown():
    """DBエンジンの後片付け（P1-005a・P1-004申し送り①）。
    async文脈のため正規API await adispose_all() を使う（同期 dispose_all() は
    ループ内で明示例外になる・D6）。DBを一度も使っていなければ何もしない（lazy）"""
    from hub.db import adispose_all
    await adispose_all()


@app.get("/health")
async def health():
    """起動確認・依存ライブラリのインポートチェック

    ※ 旧実装は PyMuPDF(fitz) を確認していたが、OCR は Vision API の
      files:annotate に PDF を直接送る方式で PyMuPDF を使っておらず、
      requirements.txt にも含まれないため常に NG 表示だった。
      実際に本番が依存するライブラリの確認に置き換え（2026-07-03）。
    """
    status = {}
    try:
        import docx  # noqa: F401  document_webhook の送付状生成が依存
        status["python-docx"] = "ok"
    except ImportError as e:
        status["python-docx"] = f"NG: {e}"
    try:
        from hub.address_label import font_status  # reportlab + 同梱フォント（T1-3）
        status["reportlab"] = font_status()
    except Exception as e:
        status["reportlab"] = f"NG: {e}"
    # graphviz（Z2 関係図の dot 描画・nixpacks.toml で追加）。
    # 不在でも関係図の描画のみ不可＝他機能は落とさない（明示表示のみ）
    import shutil
    dot_path = shutil.which("dot")
    status["graphviz"] = f"ok ({dot_path})" if dot_path else \
        "NG: dot バイナリなし（関係図の描画のみ不可・他機能は正常）"
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
        logger.info("[LINE] reply OK user_id=%s",
                    emit(user_id, "external_ref", "log", "operator"))
        return
    logger.warning("[LINE] reply failed %s %s, trying push",
                   emit(resp.status_code, "count", "log", "operator"),
                   emit(resp.text[:200], "vendor_raw", "log", "operator"))
    async with httpx.AsyncClient() as client:
        push_resp = await client.post(
            PUSH_URL,
            headers={"Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}"},
            json={"to": user_id, "messages": [{"type": "text", "text": text}]},
        )
    logger.info("[LINE] push fallback status=%s",
                emit(push_resp.status_code, "count", "log", "operator"))
    if not push_resp.is_success:
        logger.error("[LINE] push fallback error: %s",
                     emit(push_resp.text[:200], "vendor_raw", "log", "operator"))

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

    logger.info("[DEBUG] update url: %s",
                emit(url, "freetext", "log", "operator"))
    logger.info("[DEBUG] update record_id: %s",
                emit(record_id, "record_id", "log", "operator"))
    logger.info("[DEBUG] update fields keys: %s",
                emit(list(fields.keys()), "freetext", "log", "operator"))
    logger.info("[DEBUG] update body (redacted)")

    async with httpx.AsyncClient() as client:
        response = await client.put(url, headers=headers, json=body)
        logger.info("[DEBUG] update status: %s",
                    emit(response.status_code, "count", "log", "operator"))
        logger.info("[DEBUG] update response: %s",
                    emit(response.text, "vendor_raw", "log", "operator"))
        if not response.is_success:
            try:
                err = response.json()
                logger.error("[DEBUG] update error code: %s",
                             emit(err.get('code'), "vendor_raw", "log", "operator"))
                logger.error("[DEBUG] update error message: %s",
                             emit(err.get('message'), "vendor_raw", "log", "operator"))
                logger.error("[DEBUG] update error errors: %s",
                             emit(err.get('errors'), "vendor_raw", "log", "operator"))
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

    response = await create_message_with_fallback(
        claude_client,
        context="ヒアリングフロー ask_claude",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=history,
    )

    reply_text = extract_text(response)
    history.append({"role": "assistant", "content": reply_text})

    return reply_text


async def _process_line_event(reply_token: str, user_id: str, user_text: str) -> None:
    """LINEイベントの重い処理（BackgroundTasksで非同期実行）"""
    logger.info("[PROCESS] start user_id=%s text=%s",
                emit(user_id, "external_ref", "log", "operator"),
                emit(user_text[:30], "freetext", "log", "operator"))
    try:
        # ── ルーティング判定 ──────────────────────────────────────────────
        in_hearing_session = (
            user_id in conversation_histories
            and user_id not in hearing_completed
        )

        if not in_hearing_session:
            app21_record = await get_app21_record(user_id)
            if app21_record is not None:
                # ── 対応モード「人対応」判定 ────────────────────────────────
                # App21参照後・既存ルーティング分岐の手前で早期return する。
                # 「人対応」の場合は顧客へ一切送信せず（自動応答・定型文・承認
                # キュー投入を含め完全無言）、App28へ受信ログ＋管理者へLINE通知
                # のみ行い、既存経路（受付ヒアリング／顧客対応Claude）には進めない。
                # フィールド無し・空は「自動」とみなす（後方互換）。
                response_mode = (
                    app21_record.get("response_mode", {}).get("value", "") or "自動"
                )
                if response_mode == "人対応":
                    display_name = (
                        app21_record.get("顧客名", {}).get("value", "") or user_id
                    )
                    logger.info(
                        "[HUMAN_MODE] user_id=%s mode=人対応 → silent early-return",
                        emit(user_id, "external_ref", "log", "operator"),
                    )
                    # (b) App28（チャットログ）に受信内容を記録（顧客へは送信しない）
                    #     方向=user / 本文=user_text / userId=user_id / timestamp はApp28側で自動付与。
                    #     category は App28 に新たな選択肢要件を持ち込まないよう空で記録する。
                    await save_to_chatlog(user_id, "user", user_text, "", "no")
                    # (c) 弁護士へ通知（P1-102・RV-10 S1）: 顧客Bot ではなく
                    #     業務チャネル（DISPATCHBOT）へ・氏名/本文は emit で redact
                    #     （既定=完全抑止）。弁護士は App28 で実体を確認する。
                    if ATTORNEY_LINE_USER_ID:
                        from hub.notify import notify_business
                        await notify_business(
                            ATTORNEY_LINE_USER_ID,
                            f"【人対応中】"
                            f"{emit(display_name, 'name', 'line_business', 'attorney')}"
                            f"：{emit(user_text, 'freetext', 'line_business', 'attorney')}",
                        )
                    else:
                        logger.info(
                            "[HUMAN_MODE] ATTORNEY_LINE_USER_ID not set, admin notify skipped"
                        )
                    return

                status = app21_record.get("status", {}).get("value", "")
                routing = classify_routing(status)
                logger.info("[ROUTING] user_id=%s App21 (status/routing redacted)",
                            emit(user_id, "external_ref", "log", "operator"))
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
                logger.info("[ROUTING] user_id=%s → hearing (status redacted)",
                            emit(user_id, "external_ref", "log", "operator"))
            else:
                logger.info("[ROUTING] user_id=%s → hearing (no App21 record)",
                            emit(user_id, "external_ref", "log", "operator"))
        else:
            logger.info("[ROUTING] user_id=%s → hearing (in_session)",
                        emit(user_id, "external_ref", "log", "operator"))

        # ── 既存ヒアリングフロー ──────────────────────────────────────────
        try:
            claude_reply = await ask_claude(user_id, user_text)
        except ClaudeUnavailableError as e:
            # PRIMARY / FALLBACK 両方失敗 → 確認中応答 + 承認キューに要対応レコード
            async def _reply_func(token: str, text: str) -> None:
                await _line_reply_with_fallback(token, user_id, text)
            await handle_claude_outage(
                user_id=user_id,
                user_message=user_text,
                reply_token=reply_token,
                reply_func=_reply_func,
                error=str(e),
            )
            # 履歴に応答なしの user メッセージが残らないようにする
            history = conversation_histories.get(user_id, [])
            if history and history[-1].get("role") == "user":
                history.pop()
            return

        # 第1段階：レコード新規作成
        clean_reply, kintone_record = extract_marker(claude_reply, "KINTONE_RECORD")
        if kintone_record:
            kintone_record["LINEユーザーID"] = user_id
            kintone_record["status"] = "問い合わせ"
            user_business_names[user_id] = kintone_record.get("問い合わせ業者名", "")
            record_id = await post_to_kintone(kintone_record)
            kintone_record_ids[user_id] = record_id
            logger.info("[KINTONE] RECORD created record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            claude_reply = clean_reply

        # 第2段階：既存レコードを更新
        clean_reply2, update_fields = extract_marker(claude_reply, "KINTONE_UPDATE")
        if update_fields:
            logger.info("[DEBUG] KINTONE_UPDATE detected (redacted)")
            logger.info("[DEBUG] stored record_id for user: %s",
                        emit(kintone_record_ids.get(user_id), "record_id", "log", "operator"))
        if update_fields and user_id in kintone_record_ids:
            await update_kintone_record(kintone_record_ids[user_id], update_fields)
            claude_reply = clean_reply2
            hearing_completed.add(user_id)

        await _line_reply_with_fallback(reply_token, user_id, claude_reply)

    except Exception:
        import traceback
        logger.error("[ERROR] _process_line_event failed user_id=%s:",
                     emit(user_id, "external_ref", "log", "operator"))
        logger.error("[ERROR] traceback: %s",
                     emit(traceback.format_exc(), "vendor_raw", "log", "operator"))


async def _process_line_event_durable(reply_token: str, user_id: str, user_text: str,
                                      webhook_event_id: str) -> None:
    """RV-05-13 Phase A: coarse observe（received→processing→completed/failed）で
    HOTFIX-01 型の背景タスク全滅を可視化する。**_process_line_event 本体は不変**（wrap のみ）。"""
    from hub.durable_inbound import (mark_line_processing, mark_line_completed,
                                     mark_line_failed)
    try:
        await mark_line_processing(webhook_event_id)
        await _process_line_event(reply_token, user_id, user_text)
        await mark_line_completed(webhook_event_id)
    except Exception as e:
        # 失敗を durable に可視化する（HOTFIX-01 型の沈黙を防ぐ）。記録後は握って背景
        # タスクを静かに終える（flag OFF の「例外がログに出るだけ」より観測性が高い）。
        try:
            await mark_line_failed(webhook_event_id, type(e).__name__)
        except Exception:
            pass


@app.post("/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    signature = request.headers.get("X-Line-Signature", "")

    if not verify_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid signature")

    data = json.loads(body)

    # RV-05-13: flag ON なら durable 記録（受理→200→既存 BackgroundTasks で処理）。
    # M-06: flag OFF は hub.durable_inbound を import せず（env 直読み）現行挙動と byte 同一。
    _durable = os.environ.get("INBOUND_EVENT_DURABLE_ENABLED", "").strip().lower() \
        in ("1", "true", "on", "yes")

    for event in data.get("events", []):
        if event.get("type") != "message":
            continue
        if event["message"].get("type") != "text":
            continue

        reply_token = event["replyToken"]
        user_id = event["source"]["userId"]
        user_text = event["message"]["text"]

        if _durable:
            from hub.durable_inbound import record_line_event
            webhook_event_id = event.get("webhookEventId") or (
                "evt-" + hashlib.sha256(
                    json.dumps(event, sort_keys=True, ensure_ascii=False).encode()
                ).hexdigest()[:32])
            try:
                # durable commit 前に 200 を返さない（G3）。DB 停止は 5xx（memory fallback 禁止）。
                outcome = await record_line_event(
                    webhook_event_id=webhook_event_id, user_id=user_id,
                    signature_result="verified", payload=body, event_type="message")
            except Exception:
                raise HTTPException(status_code=503, detail="event store unavailable")
            if outcome == "duplicate":
                # H-NEW-01: "duplicate" は **terminal 到達済み or 実行中 or 上限超** のみ。
                # 二重返信を遮断（BackgroundTasks 登録しない）。未終端の重複は
                # record_line_event が "reattempt" を返し、下で再処理を登録する
                # （INSERT 後クラッシュ／部分 insert 失敗の永久滞留を断つ）。
                continue
            background_tasks.add_task(_process_line_event_durable,
                                     reply_token, user_id, user_text, webhook_event_id)
        else:
            background_tasks.add_task(_process_line_event, reply_token, user_id, user_text)
        logger.info("[WEBHOOK] queued user_id=%s text=%s",
                    emit(user_id, "external_ref", "log", "operator"),
                    emit(user_text[:20], "freetext", "log", "operator"))

    return {"status": "ok"}


def _verify_kintone_token(token: str) -> bool:
    """RV-04c §5.2: rotation 期間の dual-accept。primary（KINTONE_WEBHOOK_TOKEN）または
    NEXT（KINTONE_WEBHOOK_TOKEN_NEXT・投入時のみ）のどちらかに一致すれば受理。
    NEXT 未設定なら従来どおり primary のみ（byte 同一）。"""
    if verify_token(token, "KINTONE_WEBHOOK_TOKEN"):
        return True
    if os.environ.get("KINTONE_WEBHOOK_TOKEN_NEXT"):
        return verify_token(token, "KINTONE_WEBHOOK_TOKEN_NEXT")
    return False


@app.post("/webhook/kintone/approval")
async def kintone_approval_webhook(request: Request):
    """
    kintone 承認キューアプリの Webhook を受け取る。
    ステータス=承認済 かつ 送信済み=no のレコードに対して
    最新の AI下書き を LINE push し、送信済み=yes に更新する（冪等）。
    URL: /webhook/kintone/approval?token=<KINTONE_WEBHOOK_TOKEN>

    RV-04c: flag KINTONE_EVENT_DEDUP_ENABLED ON のとき、webhook top-level `id` で
    inbound_event 冪等記録＋state 遷移（received/sending/done/failed・§4.2）。flag OFF は
    hub.kintone_lane を import せず現行挙動と byte 同一（env 直読みゲート・M-06 流儀）。
    """
    token = request.query_params.get("token", "")
    if not _verify_kintone_token(token):
        raise HTTPException(status_code=404, detail="not found")

    # RV-04c: flag ON は生 body を hash 用に確保してから parse する（flag OFF は request.json()）。
    _dedup = os.environ.get("KINTONE_EVENT_DEDUP_ENABLED", "").strip().lower() \
        in ("1", "true", "on", "yes")
    if _dedup:
        raw = await request.body()
        try:
            body = json.loads(raw)
        except Exception:
            raise HTTPException(status_code=400, detail="invalid json")
    else:
        try:
            body = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="invalid json")

    # RV-04c 冪等 claim（flag ON・§4.2）。_ev は claim 済み event_id（flag OFF は None）。
    _ev = None
    if _dedup:
        from hub.kintone_lane import (claim_event, extract_event_id, observe_xff,
                                      mark_noop_done, mark_sending, mark_done,
                                      mark_failed_preflight, observe_pre_claim_reject,
                                      is_record_not_found)
        observe_xff(request.headers.get("x-forwarded-for", ""))  # §4.1 observe-only
        event_id = extract_event_id(body)
        if not event_id:
            # H01: id 欠落/空/型不正は claim 前に拒否（LINE write 0・固定 reason の 400）。
            # 行を作らないため滞留監視ではなく専用計数で観測する。
            observe_pre_claim_reject()
            raise HTTPException(status_code=400, detail="invalid webhook id")
        try:
            outcome = await claim_event(
                event_id=event_id,
                caller_id=str((body.get("app") or {}).get("id") or "kintone"),
                event_type=body.get("type"), payload=raw)
        except Exception:
            # H04 fail-closed: dedup 不能（DB 障害等）は処理せず 5xx（喪失は §4.2b 観測）
            raise HTTPException(status_code=503, detail="event store unavailable")
        if outcome == "duplicate":
            return {"ok": True, "skip": "duplicate_delivery"}
        _ev = event_id

    # レコード ID を取得（hub/webhook_auth・従来と同一ロジック）
    record_id = extract_record_id(body)
    if not record_id:
        if _ev:
            await mark_noop_done(_ev, "skip_missing_fields")
        return {"ok": True, "skip": "no_record_id"}

    # Webhook ボディで高速チェック（不要な API 呼び出しを減らす）
    try:
        webhook_status = body["record"]["ステータス2"]["value"]
        webhook_sent   = body["record"]["送信済み"]["value"]
    except (KeyError, TypeError):
        if _ev:
            await mark_noop_done(_ev, "skip_missing_fields")
        return {"ok": True, "skip": "missing_fields"}

    if webhook_status != "承認済" or webhook_sent != "no":
        if _ev:
            await mark_noop_done(_ev, "skip_not_approved")
        return {"ok": True, "skip": "not_triggered"}

    # 最新レコードを取り直す（先生の修正を反映するため・hub 経由）
    # H02残: record 不存在の確定は **HTTP 404 かつ既知 record-not-found code（GAIA_RE01）** の
    # ときのみ no-op done。404×未知 code・code 欠落・非 JSON、および 404 以外（401/timeout/
    # 接続/5xx）は transient として mark_failed_preflight＋LINE write 0（done 化しない）。
    try:
        record = await hub_kintone.get_record(_APP_APPROVAL, record_id)
    except hub_kintone.KintoneError as e:
        _st, _cd = getattr(e, "status", 0), getattr(e, "code", "")
        if _ev and not is_record_not_found(_st, _cd):
            await mark_failed_preflight(_ev, f"get_record_error_{_st}_{_cd or 'nocode'}")
            return {"ok": True, "skip": "get_record_error"}   # LINE write 0・failed 記録
        record = None   # record 不存在確定（GAIA_RE01）or flag OFF → record_not_found
    if not record:
        if _ev:
            await mark_noop_done(_ev, "skip_record_not_found")
        return {"ok": True, "skip": "record_not_found"}

    current_status = record.get("ステータス2", {}).get("value", "")
    current_sent   = record.get("送信済み",   {}).get("value", "")

    if current_status != "承認済" or current_sent != "no":
        if _ev:
            await mark_noop_done(_ev, "skip_already_sent")
        return {"ok": True, "skip": "already_sent_or_not_approved"}

    user_id  = record.get("line_user_id", {}).get("value", "")
    ai_draft = record.get("AI下書き",     {}).get("value", "")
    category = record.get("カテゴリ",     {}).get("value", "")

    if not user_id or not ai_draft:
        if _ev:
            await mark_noop_done(_ev, "skip_missing_user_or_draft")
        return {"ok": True, "skip": "missing_user_or_draft"}

    # RV-04c §4.2 D3-H01: 送信着手 marker（received→sending）成功が LINE 送信の前提条件。
    if _ev:
        if not await mark_sending(_ev):
            # marker 失敗（並行操作等）→ LINE write 0（fail-closed・行は現状のまま滞留観測へ）
            logger.warning("[KINTONE] sending marker 失敗のため送信中止 record_id=%s",
                           emit(record_id, "record_id", "log", "operator"))
            return {"ok": True, "skip": "marker_not_acquired"}

    # LINE push 送信 → 送信済みフラグ更新 → チャットログ保存
    # RV-04c §4.2 phase 表: marker 後の失敗（例外・timeout）は sending 維持（failed 上書き禁止・
    # 不明は不明のまま §4.2 runbook へ）。例外は握らず伝播させ、state は sending に留める。
    await send_line_push(user_id, ai_draft)
    await mark_approval_sent(record_id)
    await save_to_chatlog(user_id, "assistant", ai_draft, category, "yes")
    if _ev:
        await mark_done(_ev)   # 全副作用完了後の terminal（sending→done・last_error=NULL）

    return {"ok": True, "record_id": record_id}


# ══════════════════════════════════════════════════════════════
# POST /ocr/fixed-asset  固定資産税評価額 OCR → kintone登録
# ══════════════════════════════════════════════════════════════

def _pdf_page_count(pdf_bytes: bytes) -> int | None:
    """PDF の総ページ数（PyMuPDF 不在・破損時は None = 従来動作へ縮退）"""
    try:
        import fitz  # PyMuPDF
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return doc.page_count
    except Exception as e:
        logger.warning("[OCR] ページ数の取得に失敗（従来の単発リクエストへ縮退）: %s: %s",
                       type(e).__name__,
                       emit(str(e), "vendor_raw", "log", "operator"))
        return None


def _vision_timeout_seconds() -> float:
    """Vision files:annotate の per-request timeout（env VISION_ANNOTATE_TIMEOUT_SECONDS・
    既定120秒）。5ページ/リクエストの同期 annotate は実測で数秒オーダー。120秒は通常の
    ~20-40× 余裕で遅い大判スキャンも吸収しつつ、明示 timeout の無い urlopen が socket
    ハング時に無限滞留する経路（M-NEW-01）を断つ。"""
    raw = os.environ.get("VISION_ANNOTATE_TIMEOUT_SECONDS", "").strip()
    try:
        v = float(raw)
    except ValueError:
        return 120.0
    return v if v > 0 else 120.0


def _vision_annotate(pdf_bytes: bytes, api_key: str,
                     pages: list[int] | None = None) -> list[str]:
    """Vision files:annotate を1回呼び、ページごとのテキストのリストを返す。
    pages 未指定は API 既定（先頭5ページ）＝従来動作"""
    import urllib.request
    content = base64.b64encode(pdf_bytes).decode("utf-8")
    request: dict = {
        "inputConfig": {"content": content, "mimeType": "application/pdf"},
        "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
        "imageContext": {"languageHints": ["ja", "en"]},
    }
    if pages:
        request["pages"] = pages
    body = json.dumps({"requests": [request]}).encode("utf-8")
    url = f"https://vision.googleapis.com/v1/files:annotate?key={api_key}"
    req = urllib.request.Request(url, data=body,
                                  headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=_vision_timeout_seconds()) as resp:
            result = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Vision API {e.code}: {err_body}")

    pages_text = []
    for page_resp in result.get("responses", [{}])[0].get("responses", []):
        annotation = page_resp.get("fullTextAnnotation")
        if annotation:
            pages_text.append(annotation.get("text", ""))
    return pages_text


def _ocr_pdf_pages(pdf_bytes: bytes, api_key: str) -> list[str]:
    """全ページを5ページずつのバッチで OCR し、ページ順のテキストリストを返す
    （D1-1・2026-07-07。Vision files:annotate（同期）は1リクエスト最大5ページの
    ため、従来実装は5ページ超の後半を取りこぼしていた）。
    ページ数不明（PyMuPDF 不在等）は従来の単発リクエストに縮退"""
    total = _pdf_page_count(pdf_bytes)
    if total is None:
        return _vision_annotate(pdf_bytes, api_key)
    pages_text: list[str] = []
    for start in range(1, total + 1, 5):
        batch = list(range(start, min(start + 4, total) + 1))
        pages_text.extend(_vision_annotate(pdf_bytes, api_key, pages=batch))
    return pages_text


def _ocr_pdf_bytes(pdf_bytes: bytes, api_key: str) -> str:
    """PDF 全ページの OCR テキスト（結合版・既存呼び出し元の契約は不変）"""
    return "\n\n".join(_ocr_pdf_pages(pdf_bytes, api_key))


async def _extract_fixed_asset(ocr_text: str) -> dict:
    """OCRテキストから固定資産税評価額・年度・所在地・地番を抽出して返す"""
    response = await create_message_with_fallback(
        claude_client,
        context="OCR固定資産税抽出",
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
    raw = extract_text(response).strip()
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
    logger.info("[DEBUG] kintone search query: 所在 like %s",
                emit(shozaichi, "address", "log", "operator"))
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers)
        logger.info("[DEBUG] kintone search status: %s / response: %s",
                    emit(resp.status_code, "count", "log", "operator"),
                    emit(resp.text, "vendor_raw", "log", "operator"))
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
    logger.info("[DEBUG] kintone PUT body (redacted)")
    async with httpx.AsyncClient() as client:
        resp = await client.put(url, headers=headers, json=body)
        logger.info("[DEBUG] kintone PUT status: %s",
                    emit(resp.status_code, "count", "log", "operator"))
        logger.info("[DEBUG] kintone PUT response: %s",
                    emit(resp.text, "vendor_raw", "log", "operator"))
        if not resp.is_success:
            raise Exception(f"kintone更新エラー {resp.status_code}: {resp.text}")


@app.post("/ocr/fixed-asset")
async def ocr_fixed_asset(file: UploadFile = File(...),
                          case_hint: str | None = Form(default=None)):
    """
    PDFをアップロードすると固定資産税評価額・年度をOCRで抽出し
    kintoneに登録してLINEで通知する。

    case_hint（省略可・S4）: 案件レコードID。App 財産への財産行 upsert
    （units/souzoku/zaisan_sync・souzoku-shorui/02 §3）の案件紐付けに使う。
    既存の呼び出し（file のみ）の動作は不変。
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
                            detail="サーバの環境変数が未設定です（管理者へ連絡してください）")

    if not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    # 1. PDFを読み込む
    pdf_bytes = await file.read()

    # 2. Google Cloud Vision API でOCR
    try:
        ocr_text = _ocr_pdf_bytes(pdf_bytes, GOOGLE_VISION_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=502, detail="OCRエラー（画像認識に失敗しました）")

    if not ocr_text.strip():
        raise HTTPException(status_code=422, detail="OCRでテキストを取得できませんでした")

    # 3. Claude APIで評価額・年度・所在地・地番を構造化抽出
    try:
        extracted = await _extract_fixed_asset(ocr_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail="Claude抽出エラー（項目抽出に失敗しました）")

    logger.info("[DEBUG] extracted (redacted)")

    shozaichi_raw = extracted.get("所在地") or ""
    chiban        = extracted.get("地番")   or ""
    if not shozaichi_raw or not chiban:
        raise HTTPException(status_code=422,
                            detail="所在地または地番を抽出できませんでした")

    shozaichi = _normalize_shozaichi(shozaichi_raw)
    logger.info("[DEBUG] 所在地: %s → %s",
                emit(shozaichi_raw, "address", "log", "operator"),
                emit(shozaichi, "address", "log", "operator"))

    # 4. kintoneで所在地・地番が一致するレコードを検索
    try:
        record_id = await _search_kintone_record(shozaichi)
    except Exception as e:
        raise HTTPException(status_code=502, detail="kintone検索エラー")

    if record_id is None:
        raise HTTPException(status_code=404,
                            detail="kintoneに一致するレコードが見つかりません")

    # 5. 一致レコードの評価額・年度を更新
    try:
        await _update_kintone_record(record_id, extracted)
    except Exception as e:
        raise HTTPException(status_code=502, detail="kintone更新エラー")

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
        # 受領通知は業務通知＝指示Botチャネルから（2026-07-07 裁定・
        # レガシー _push_line_message は廃止し hub/notify に一本化）
        ok = await hub_notify.push_line_message(
            LINE_USER_ID, notify_text, token_env=hub_notify.business_token_env())
        if not ok:
            logger.warning("[WARN] LINE通知失敗（push_line_message が False）")
    else:
        logger.info("[INFO] LINE_USER_ID未設定のためLINE通知をスキップ")

    # 7. S4 追記型拡張（souzoku-shorui/02 §3）: App 財産への財産行 upsert。
    #    既存処理（1〜6）成功後にのみ実行。無効時（ZAISAN_SYNC_DISABLED=1 /
    #    APP_ZAISAN 未設定）は None が返り、従来レスポンスをそのまま返す。
    #    追加処理の失敗は既存処理の成功応答を壊さない
    zaisan_sync = None
    try:
        from units.souzoku.zaisan_sync import sync_fixed_asset
        zaisan_sync = await sync_fixed_asset(
            fudosan_record_id=record_id, extracted=extracted,
            shozaichi=shozaichi, pdf_bytes=pdf_bytes,
            filename=file.filename, case_hint=case_hint)
    except Exception as e:
        logger.warning("[WARN] 財産行同期に失敗（既存処理は完了済み）: %s: %s",
                       type(e).__name__,
                       emit(str(e), "vendor_raw", "log", "operator"))
        zaisan_sync = {"status": "error", "detail": str(e)[:200]}

    response = {
        "status": "ok",
        "kintone_record_id": record_id,
        "extracted": extracted,
    }
    if zaisan_sync is not None:
        response["zaisan_sync"] = zaisan_sync
    return response


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
    response = await create_message_with_fallback(
        claude_client,
        context=f"スキャン抽出 {folder_name}",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = extract_text(response).strip()
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
    logger.info("[DEBUG] scan kintone POST body (redacted)")
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, json=body)
        logger.info("[DEBUG] scan kintone POST status: %s / %s",
                    emit(resp.status_code, "count", "log", "operator"),
                    emit(resp.text, "vendor_raw", "log", "operator"))
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
            detail="未対応のフォルダ名です（対応: 相談カード / 戸籍謄本 / 通帳）",
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
        raise HTTPException(status_code=500, detail="サーバの環境変数が未設定です（管理者へ連絡してください）")

    # 1. base64デコード
    try:
        pdf_bytes = base64.b64decode(req.pdf_base64)
    except Exception as e:
        raise HTTPException(status_code=400, detail="base64デコードエラー")

    # 2. Google Vision API でOCR
    try:
        ocr_text = _ocr_pdf_bytes(pdf_bytes, GOOGLE_VISION_API_KEY)
    except Exception as e:
        raise HTTPException(status_code=502, detail="OCRエラー（画像認識に失敗しました）")

    if not ocr_text.strip():
        raise HTTPException(status_code=422, detail="OCRでテキストを取得できませんでした")

    # 3. Claude で情報抽出
    try:
        extracted = await _extract_by_folder(ocr_text, req.folder_name)
    except Exception as e:
        raise HTTPException(status_code=502, detail="Claude抽出エラー（項目抽出に失敗しました）")

    logger.info("[DEBUG] scan extracted (redacted)")

    # 相談カード・戸籍謄本: ファイル名・登録日時を付加
    if req.folder_name in ("相談カード", "戸籍謄本"):
        from datetime import datetime, timezone
        extracted["ファイル名"] = req.file_name
        extracted["登録日時"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 4. kintone に登録
    try:
        record_id = await _post_scan_to_kintone(app_id, api_token, extracted)
    except Exception as e:
        raise HTTPException(status_code=502, detail="kintone登録エラー")

    return {
        "status": "ok",
        "folder_name": req.folder_name,
        "kintone_record_id": record_id,
        "extracted": extracted,
    }
def _stripe_journal_enabled() -> bool:
    """InboundEvent journal（P1-005a・D10）。既定OFF＝完全に従来挙動。
    ONへの切替は env STRIPE_EVENT_JOURNAL_ENABLED=1（大野が投入）"""
    return os.environ.get("STRIPE_EVENT_JOURNAL_ENABLED") == "1"


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
        # 未認証 caller（誰でも POST 可）へ内部詳細を返さない。署名検証の生の例外文言は
        # emit 契約経由で構造化 log へ（既定=完全抑止）・応答は固定メッセージ＋400 のみ。
        logger.warning("stripe webhook signature verification failed: %s",
                       emit(str(e), "vendor_raw", "log", "operator"))
        raise HTTPException(status_code=400, detail="署名の検証に失敗しました")

    # ── InboundEvent journal（P1-005a・D9: 入口の関所。業務ロジックは不変）──
    # DB到達不能時はここで例外→FastAPIが500を返し、Stripeの自動リトライに
    # 委ねる（D7: 成功ACKを返さない・memory fallback禁止）
    journal_pk = None
    outcome = None
    if _stripe_journal_enabled():
        from hub.inbound_event import record_stripe_event
        outcome, journal_pk = await record_stripe_event(event, payload)
        if outcome == "skipped_duplicate":
            logger.info("[STRIPE] duplicate delivery skipped (already done)")
            return {"status": "ok", "journal": "skipped_duplicate"}
        if outcome == "in_progress":
            # D14（P1-005c・H01）: 実行中(15分以内)の重複は 200 で飲まず 503。
            # 真に処理中→完了後の再送は done→200 skip。クラッシュ済み→再送が
            # 続き 15 分経過後の配送が stale 再claimで回収（回収の起動主体=
            # Stripe再送。指数バックオフで最大3日継続＝15分窓を確実に跨ぐ）
            raise HTTPException(status_code=503,
                                detail="event processing in progress")

    try:
        if outcome == "reprocess":
            # D15（P1-005c・H02）: 再処理経路は POST 前に App 21 を
            # Stripe決済ID で照合（§8.7「ACK不明は再実行より先にreconciliation」）。
            # 「kintone 500 だがレコード作成済み」の再送で二重起票しない
            session_id = str(((event.get("data") or {}).get("object") or {})
                             .get("id") or "")
            if await _stripe_session_already_filed(session_id):
                from hub.inbound_event import mark_done
                await mark_done(journal_pk)
                logger.info("[STRIPE] reconciled: record already filed")
                return {"status": "ok", "journal": "reconciled"}
        await _process_stripe_event(event)
    except HTTPException:
        raise
    except Exception as e:
        if journal_pk is not None:
            from hub.inbound_event import mark_failed
            await mark_failed(journal_pk, type(e).__name__)
        raise

    if journal_pk is not None:
        from hub.inbound_event import mark_done
        await mark_done(journal_pk)
    return {"status": "ok"}


async def _stripe_session_already_filed(session_id: str) -> bool:
    """App 21 に Stripe決済ID 一致のレコードが既にあるか（D15 reconciliation）。
    フィールドコード「Stripe決済ID」は _process_stripe_event の書き込みと同一。
    初回処理では呼ばない（存在し得ないため・レイテンシ増を避ける・D15）。
    kintone 到達不能はここで飲まず例外→failed→Stripe再送に委ねる"""
    if not session_id:
        return False
    url = f"https://{os.environ.get('KINTONE_SUBDOMAIN')}.cybozu.com/k/v1/records.json"
    headers = {"X-Cybozu-API-Token": os.environ.get("KINTONE_API_TOKEN")}
    params = {"app": 21,
              "query": f'Stripe決済ID = "{session_id}" limit 1'}
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, headers=headers, params=params)
    resp.raise_for_status()
    return bool(resp.json().get("records"))


async def _process_stripe_event(event: dict) -> None:
    """既存のStripe業務処理（P1-005aで関数化のみ・ロジック不変）"""
    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        customer_name = session.get("customer_details", {}).get("name")
        customer_email = session.get("customer_details", {}).get("email")
        amount = session.get("amount_total")
        logger.info("決済完了: %s / %s / %s円",
                    emit(customer_name, "name", "log", "operator"),
                    emit(customer_email, "email", "log", "operator"),
                    emit(amount, "count", "log", "operator"))
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
            resp = await client.post(kintone_url, headers=kintone_headers,
                                     json=kintone_data)
        # D11（P1-005b・M02）: kintone非2xxを業務失敗として例外化。
        # 「黙って成功扱い」を廃止する安全側一方向の変更（flag OFF時にも適用）。
        # 例外→handlerが5xx→Stripe再送→journal ONならfailed行の再claimで回復
        resp.raise_for_status()
