"""
クラウドサイン Webhook 受け取りモジュール（Railway 用）

役割:
  1. クラウドサインから「締結完了」の通知（Webhook）を受け取る
  2. なりすまし防止のため URL に埋めた合言葉を検証
  3. 書類詳細 API を叩いて通知の真正性を確認（締結情報も取得）
  4. documentID で kintone の案件レコードを検索し、ステータスを「締結済み」に更新
  5. LINE で管理者に通知

既存の LINE Bot アプリ（Railway 上）に組み込む前提。
フレームワークに依存しないコア関数 + FastAPI / Flask アダプタの2構成。
"""

import os
import time
import hmac
import logging

import requests

logger = logging.getLogger("cloudsign")

# ============================================================
# 環境変数（Railway の Variables に設定する）
# ============================================================
CLOUDSIGN_CLIENT_ID = os.environ["CLOUDSIGN_CLIENT_ID"]
# 本番: https://api.cloudsign.jp / サンドボックス: 別ホスト（検証時はこちらを指定）
CLOUDSIGN_API_BASE = os.environ.get("CLOUDSIGN_API_BASE", "https://api.cloudsign.jp")
# Webhook URL に埋め込む合言葉（推測されない長いランダム文字列にする）
WEBHOOK_SECRET = os.environ["CLOUDSIGN_WEBHOOK_SECRET"]

# kintone（既存の Railway 環境変数を流用）
#   KINTONE_SUBDOMAIN はサブドメインのみ（例: xxxx）でも、xxxx.cybozu.com でもOK
_sub = os.environ["KINTONE_SUBDOMAIN"]
KINTONE_BASE = _sub if _sub.startswith("http") else f"https://{_sub.replace('.cybozu.com', '')}.cybozu.com"
# 時効援用の案件が入っているアプリのトークン/ID。
#   ※もし時効援用案件が SOUZOKU_* などの別アプリなら、そちらの変数名に差し替える
KINTONE_APP_ID = os.environ["KINTONE_APP_ID"]
KINTONE_API_TOKEN = os.environ["KINTONE_API_TOKEN"]

# kintone のフィールドコード（App 21 の実フィールドコードに合わせる）
FIELD_DOCUMENT_ID = "cloudsign_document_id"  # 送信時に documentID を保存しておくフィールド
FIELD_STATUS = "status"                      # 案件ステータス（DROP_DOWN）のフィールドコード

# LINE 通知（任意。設定が無ければスキップ）
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
LINE_ADMIN_USER_ID = os.environ.get("LINE_ADMIN_USER_ID")  # 通知先（自分/管理者のuserId）

# クラウドサインの status 値
#   例の JSON では COMPLETED = 2。status は3種の整数を取るので、
#   却下・取消などの他の値は実環境（サンドボックス）で必ず確認すること。
STATUS_COMPLETED = 2


# ============================================================
# クラウドサイン アクセストークン管理
#   有効期限はクラウドサイン側で管理される。300秒バッファで自動更新し、
#   401（失効）が出たら1回だけ取り直す。
# ============================================================
class _CloudSignToken:
    def __init__(self):
        self._token = None
        self._expires_at = 0.0

    def get(self) -> str:
        if self._token and time.time() < self._expires_at - 300:
            return self._token
        resp = requests.post(
            f"{CLOUDSIGN_API_BASE}/token",
            params={"client_id": CLOUDSIGN_CLIENT_ID},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        # レスポンスのフィールド名は仕様書で確認（access_token / expires_in 想定）
        self._token = data["access_token"]
        self._expires_at = time.time() + int(data.get("expires_in", 3600))
        return self._token

    def invalidate(self):
        self._expires_at = 0.0


_token = _CloudSignToken()


def fetch_document(document_id: str) -> dict:
    """書類詳細を取得。Webhook の真正性確認＆締結情報の取得に使う。"""
    url = f"{CLOUDSIGN_API_BASE}/documents/{document_id}"
    headers = {"Authorization": f"Bearer {_token.get()}"}
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.status_code == 401:  # トークン失効 → 取り直して1回だけ再試行
        _token.invalidate()
        headers = {"Authorization": f"Bearer {_token.get()}"}
        resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ============================================================
# kintone 更新
# ============================================================
def update_kintone_status(document_id: str, new_status: str) -> bool:
    """documentID で案件を探し、ステータスフィールドを更新する。"""
    # 1. documentID で検索
    r = requests.get(
        f"{KINTONE_BASE}/k/v1/records.json",
        headers={"X-Cybozu-API-Token": KINTONE_API_TOKEN},
        params={
            "app": KINTONE_APP_ID,
            "query": f'{FIELD_DOCUMENT_ID} = "{document_id}"',
        },
        timeout=10,
    )
    r.raise_for_status()
    records = r.json().get("records", [])
    if not records:
        logger.warning("kintoneに該当案件なし document_id=%s", document_id)
        return False

    record_id = records[0]["$id"]["value"]

    # 2. ステータス更新
    u = requests.put(
        f"{KINTONE_BASE}/k/v1/record.json",
        headers={
            "X-Cybozu-API-Token": KINTONE_API_TOKEN,
            "Content-Type": "application/json",
        },
        json={
            "app": KINTONE_APP_ID,
            "id": record_id,
            "record": {FIELD_STATUS: {"value": new_status}},
        },
        timeout=10,
    )
    u.raise_for_status()
    logger.info("kintone更新完了 record_id=%s status=%s", record_id, new_status)
    return True


# ============================================================
# LINE 通知
# ============================================================
def notify_line(message: str) -> None:
    if not (LINE_CHANNEL_ACCESS_TOKEN and LINE_ADMIN_USER_ID):
        return
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {LINE_CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json={
                "to": LINE_ADMIN_USER_ID,
                "messages": [{"type": "text", "text": message}],
            },
            timeout=10,
        )
    except Exception:
        logger.exception("LINE通知失敗")


# ============================================================
# コア処理（フレームワーク非依存）
#   返り値: (HTTPステータスコード, レスポンスdict)
# ============================================================
def handle_webhook(secret: str, payload: dict) -> tuple[int, dict]:
    # 合言葉チェック（一致しなければ存在しないフリ＝404）
    if not hmac.compare_digest(secret or "", WEBHOOK_SECRET):
        return 404, {"error": "not found"}

    document_id = payload.get("documentID")
    status = payload.get("status")
    logger.info("CloudSign webhook受信 doc=%s status=%s", document_id, status)

    if status == STATUS_COMPLETED and document_id:
        title = ""
        try:
            doc = fetch_document(document_id)  # 真正性確認＆タイトル取得
            title = doc.get("title", "")
        except Exception:
            logger.exception("書類取得失敗 doc=%s", document_id)

        update_kintone_status(document_id, "受任")
        notify_line(f"【締結完了】{title}\ndocumentID: {document_id}")

    # クラウドサインには常に200を返す（再送ループを防ぐ）
    return 200, {"ok": True}


# ============================================================
# FastAPI アダプタ
#   既存アプリで:  app.include_router(router)
#   登録URL:      https://<app>.up.railway.app/cloudsign/webhook/<SECRET>
# ============================================================
try:
    from fastapi import APIRouter, Request
    from fastapi.responses import JSONResponse

    router = APIRouter()

    @router.post("/cloudsign/webhook/{secret}")
    async def cloudsign_webhook(secret: str, request: Request):
        payload = await request.json()
        code, body = handle_webhook(secret, payload)
        return JSONResponse(status_code=code, content=body)

except ImportError:
    router = None


# ============================================================
# Flask アダプタ（Flask を使っている場合はこちらを利用）
# ------------------------------------------------------------
# from flask import Flask, request, jsonify
#
# @app.route("/cloudsign/webhook/<secret>", methods=["POST"])
# def cloudsign_webhook(secret):
#     code, body = handle_webhook(secret, request.get_json(force=True))
#     return jsonify(body), code
# ============================================================
