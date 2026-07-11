"""
クラウドサイン Webhook 受け取りモジュール（Railway 用）

役割:
  1. クラウドサインから「締結完了」の通知（Webhook）を受け取る
  2. なりすまし防止のため URL に埋めた合言葉を検証
  3. 書類詳細 API を叩いて通知の真正性を確認（締結情報も取得）。
     確認できない場合は fail-closed（P0B-002 / R0A-B03）: 受任遷移も
     顧客向け通知も行わず、業務指示Botチャネルで要人手確認を警報する
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


def _classify_fetch_error(exc: Exception) -> str:
    """安全ログ用の失敗分類（識別子のみ。本文・PII・token・生レスポンスを含めない）"""
    if isinstance(exc, requests.HTTPError):
        code = getattr(getattr(exc, "response", None), "status_code", None)
        return f"api_http_{code}" if code else "api_http_error"
    if isinstance(exc, requests.Timeout):
        return "api_timeout"
    if isinstance(exc, requests.RequestException):
        return "api_request_error"
    return f"error_{type(exc).__name__}"


def verify_completed_document(document_id: str) -> tuple[dict | None, str]:
    """書類詳細 API で webhook の真正性を確認する（fail-closed 化・P0B-002）。

    戻り値: (書類dict, "") 成功 ／ (None, 失敗分類) 失敗。
    失敗と判定する条件（いずれも受任遷移させない）:
      - fetch_document の例外（HTTPエラー・タイムアウト・接続失敗ほか）
      - レスポンスが dict でない（想定外レスポンス）
      - レスポンスの id が webhook の documentID と一致しない（id キーがある場合）
      - レスポンスの status が STATUS_COMPLETED(2) でない
        ※書類詳細 API の status 値は webhook と同体系（:50-53 の注のとおり
          実環境/サンドボックスで要確認）。キー欠落も安全側で失敗扱いにする
    """
    try:
        doc = fetch_document(document_id)
    except Exception as exc:
        return None, _classify_fetch_error(exc)
    if not isinstance(doc, dict):
        return None, "unexpected_response_type"
    doc_id = doc.get("id")
    if doc_id is not None and str(doc_id) != str(document_id):
        return None, "document_id_mismatch"
    if doc.get("status") != STATUS_COMPLETED:
        return None, f"status_mismatch_{doc.get('status')}"
    return doc, ""


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


def notify_business_line(message: str) -> None:
    """業務通知（照合失敗の要人手確認）を業務指示Botチャネルで管理者へ送る。

    DISPATCHBOT_CHANNEL_ACCESS_TOKEN 未設定なら送信しない（警告ログのみ）。
    顧客Bot（LINE_CHANNEL_ACCESS_TOKEN）へのフォールバックは行わない
    （P0B-002 指示: 照合失敗の警報を顧客Botチャネルに乗せない。
    hub/notify.business_token_env のフォールバックはここでは使わない）。
    """
    token = os.environ.get("DISPATCHBOT_CHANNEL_ACCESS_TOKEN", "")
    to = (os.environ.get("LINE_ADMIN_USER_ID", "")
          or os.environ.get("ATTORNEY_LINE_USER_ID", ""))
    if not (token and to):
        logger.warning(
            "business LINE notify skipped (DISPATCHBOT token or admin id unset)")
        return
    try:
        requests.post(
            "https://api.line.me/v2/bot/message/push",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"to": to, "messages": [{"type": "text", "text": message[:4900]}]},
            timeout=10,
        )
    except Exception:
        logger.exception("business LINE通知失敗")


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
        doc, failure = verify_completed_document(document_id)
        if doc is None:
            # fail-closed（P0B-002 / R0A-B03）: 真正性を確認できないイベントでは
            # 業務 state（受任）を進めない・顧客チャネルの通知も出さない。
            # ログは correlation 用の documentID と失敗分類のみ
            # （本文・PII・token・ベンダー生レスポンスは出さない）
            logger.warning("CloudSign照合失敗のため受任へ遷移せず doc=%s reason=%s",
                           document_id, failure)
            notify_business_line(
                "【CloudSign照合失敗・要人手確認】\n"
                f"documentID: {document_id}\n"
                f"失敗分類: {failure}\n"
                "受任への自動遷移は行っていません。"
                "CloudSign管理画面で締結状況を確認してください。")
            # 再送方針: CloudSign が非2xx応答時に再送するか・何回で打ち切るか・
            # webhook を自動無効化するかは、リポジトリ内資料からは確定できない
            # （BLOCKED_NEEDS_HUMAN）。不用意に非2xxを返すと再送ループや配信停止を
            # 招き得るため、受理応答は従来どおり 200 を維持する。
            # 「業務 state を進めていない」ことは state=verification_failed と
            # 上記ログで判別できる（イベントの永続 journal 化は Phase 1）。
            return 200, {"ok": True, "state": "verification_failed"}

        title = doc.get("title", "")
        update_kintone_status(document_id, "受任")
        notify_line(f"【締結完了】{title}\ndocumentID: {document_id}")
        return 200, {"ok": True, "state": "processed"}

    # 締結完了以外のイベント（却下・取消等）は従来どおり何もしない
    # クラウドサインには常に200を返す（再送ループを防ぐ）
    return 200, {"ok": True, "state": "skipped"}


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
