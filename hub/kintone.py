"""kintone REST API 共通クライアント

設計: docs/architecture/03-common-components.md §3

設計上の決めごと:
  - fields は {"コード": 値} のフラット dict を受け、{"value": ...} への包みは
    内部で行う（既存 post_to_kintone と同じ流儀）
  - 書き込みはリトライしない。二重実行防止は上位（実行済みフラグ + revision）で担保。
    読み込み（GET）のみ 1 回リトライ（一時的なネットワーク断・5xx）
  - update_record(revision=...) は kintone の楽観ロックを透過させる
    （revision 不一致 = 他プロセスが先に更新 → KintoneConflict 例外）
  - 失敗は KintoneError(status, code, message) に正規化する。
    警報（LINE 通知）は呼び出し元の責務で、クライアント自身は警報を出さない
"""

import os
from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class KintoneApp:
    """アプリへの接続情報。環境変数名を保持し、値はリクエスト時に解決する"""

    label: str      # ログ・警報表示用（例: "App 29 (承認キュー)"）
    app_id_env: str  # 例: "APP_APPROVAL"
    token_env: str   # 例: "TOKEN_APPROVAL"

    def app_id(self) -> str:
        return os.environ.get(self.app_id_env, "")

    def token(self) -> str:
        return os.environ.get(self.token_env, "")


class KintoneError(Exception):
    """kintone API 呼び出しの失敗（HTTP エラー・通信エラーの正規化）"""

    def __init__(self, status: int, code: str = "", message: str = "",
                 errors: dict | None = None):
        self.status = status
        self.code = code
        self.message = message
        # kintone 検証エラー（CB_VA01）の欄別詳細 {"record.<code>.value": {"messages": [...]}}。
        # 呼び出し元が「どの欄の違反か」を閉集合で判定するために保持する
        # （JIKOU-FORM-1-fix1 01: 一意制約違反の確定判定）。str() には含めない
        self.errors: dict = errors if isinstance(errors, dict) else {}
        super().__init__(f"kintone error status={status} code={code} message={message}")


class KintoneConflict(KintoneError):
    """revision 楽観ロックの競合（他プロセスが先に更新した）"""


# GET 系のみ 1 回リトライ（書き込みはリトライしない）
_GET_RETRIES = 1


def _base_url() -> str:
    """KINTONE_SUBDOMAIN からベース URL を組み立てる。
    サブドメインのみ / xxx.cybozu.com / フル URL のいずれも受け付ける
    （既存 cloudsign_webhook / document_webhook の防御的挙動を統合）"""
    sub = os.environ.get("KINTONE_SUBDOMAIN", "").strip()
    if sub.startswith("http"):
        return sub.rstrip("/")
    return f"https://{sub.replace('.cybozu.com', '')}.cybozu.com"


def _raise_error(resp) -> None:
    """エラーレスポンスを KintoneError / KintoneConflict に正規化して送出する"""
    try:
        err = resp.json()
    except Exception:
        err = {}
    if not isinstance(err, dict):
        err = {}
    code = err.get("code", "")
    message = err.get("message", "") or getattr(resp, "text", "")[:200]
    errors = err.get("errors")
    # revision 不一致は HTTP 409（コード GAIA_CO02）
    if resp.status_code == 409 or code == "GAIA_CO02":
        raise KintoneConflict(resp.status_code, code, message, errors)
    raise KintoneError(resp.status_code, code, message, errors)


async def _get(url: str, app: KintoneApp, params: dict) -> httpx.Response:
    """GET 共通（1回だけリトライ: 5xx または通信エラー時）"""
    for attempt in range(_GET_RETRIES + 1):
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    url, headers={"X-Cybozu-API-Token": app.token()}, params=params
                )
        except httpx.TransportError as e:
            if attempt < _GET_RETRIES:
                continue
            raise KintoneError(0, "transport_error", str(e)) from e
        if resp.is_success:
            return resp
        if resp.status_code >= 500 and attempt < _GET_RETRIES:
            continue
        _raise_error(resp)
    _raise_error(resp)  # 保険（到達しない想定）


async def _write(method: str, url: str, app: KintoneApp, json_body: dict) -> httpx.Response:
    """書き込み共通（リトライしない）"""
    headers = {"X-Cybozu-API-Token": app.token(), "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.request(method, url, headers=headers, json=json_body)
    except httpx.TransportError as e:
        raise KintoneError(0, "transport_error", str(e)) from e
    if not resp.is_success:
        _raise_error(resp)
    return resp


def _wrap(fields: dict) -> dict:
    return {k: {"value": v} for k, v in fields.items()}


# ══════════════════════════════════════════════════════════════
# 公開 API（docs/architecture/03 §3 の関数群）
# ══════════════════════════════════════════════════════════════

async def get_record(app: KintoneApp, record_id: str) -> dict:
    resp = await _get(
        f"{_base_url()}/k/v1/record.json", app,
        params={"app": app.app_id(), "id": record_id},
    )
    return resp.json()["record"]


async def search_records(app: KintoneApp, query: str, fields: list[str] | None = None) -> list[dict]:
    params: dict = {"app": app.app_id(), "query": query}
    if fields:
        for i, f in enumerate(fields):
            params[f"fields[{i}]"] = f
    resp = await _get(f"{_base_url()}/k/v1/records.json", app, params=params)
    return resp.json().get("records", [])


async def create_record(app: KintoneApp, fields: dict) -> str:
    resp = await _write(
        "POST", f"{_base_url()}/k/v1/record.json", app,
        {"app": app.app_id(), "record": _wrap(fields)},
    )
    return resp.json()["id"]


async def create_records(app: KintoneApp, records: list[dict], chunk_size: int = 100) -> list[str]:
    """複数レコードの一括登録（kintone 上限 100 件/リクエストでチャンク分割）"""
    ids: list[str] = []
    for i in range(0, len(records), chunk_size):
        resp = await _write(
            "POST", f"{_base_url()}/k/v1/records.json", app,
            {"app": app.app_id(), "records": [_wrap(r) for r in records[i:i + chunk_size]]},
        )
        ids.extend(resp.json().get("ids", []))
    return ids


async def update_record(app: KintoneApp, record_id: str, fields: dict,
                        revision: str | None = None) -> None:
    body: dict = {"app": app.app_id(), "id": record_id, "record": _wrap(fields)}
    if revision is not None:
        body["revision"] = revision
    await _write("PUT", f"{_base_url()}/k/v1/record.json", app, body)


async def delete_record(app: KintoneApp, record_id: str) -> None:
    """レコードの物理削除（R4-2b: 名寄せ統合の敗者削除用）。
    削除はリトライしない（書き込みと同じ流儀）。呼び出し元は監査記録の保存成功を
    削除の前提条件とすること（person_merge_exec の順序固定）"""
    await _write(
        "DELETE", f"{_base_url()}/k/v1/records.json", app,
        {"app": app.app_id(), "ids": [record_id]},
    )


async def upload_file(app: KintoneApp, filename: str, content: bytes, mime: str) -> str:
    """ファイルアップロード（multipart）。fileKey を返す"""
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{_base_url()}/k/v1/file.json",
                headers={"X-Cybozu-API-Token": app.token()},
                files={"file": (filename, content, mime)},
            )
    except httpx.TransportError as e:
        raise KintoneError(0, "transport_error", str(e)) from e
    if not resp.is_success:
        _raise_error(resp)
    return resp.json()["fileKey"]


async def download_file(app: KintoneApp, file_key: str) -> bytes:
    resp = await _get(f"{_base_url()}/k/v1/file.json", app, params={"fileKey": file_key})
    return resp.content


async def get_form_fields(app: KintoneApp) -> dict:
    """フォーム設計の取得（死活監視用）。properties dict を返す"""
    resp = await _get(
        f"{_base_url()}/k/v1/app/form/fields.json", app, params={"app": app.app_id()}
    )
    return resp.json().get("properties", {})
