"""webapp_auth — P4-001: PWA（仮名称「案件管理」）の認証＋shell 配信

裁定（2026-07-27・[人]）: 認証=パスワード＋署名付き session cookie
（DRAFT_P4_PWA_INVENTORY §3 の推奨 (b) 採用）。司令塔既定:

- **env（hub/webhook_auth の既存命名に倣う・平文 env 禁止）**:
  - `WEBAPP_PASSWORD_HASH` … PBKDF2-HMAC-SHA256 の自己記述形式
    `pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>`（生成は本 module の
    `hash_password()` を[人]がローカルで実行して得る）。
  - `WEBAPP_SESSION_SECRET` … session 署名鍵（十分長いランダム文字列）。
    **鍵を差し替えると発行済み session は全て失効する**（それが失効手段。
    自動 rotation は作らない=司令塔既定）。
- **session 期限=7日**（`SESSION_TTL_SECONDS`）。cookie は HttpOnly・
  SameSite=Strict・Secure・path=/app。
- **検証は hmac.compare_digest の型**（hub/webhook_auth.verify_token に倣う）。
- **ログイン失敗時に入力値をログへ反射しない**——本 module は logging を
  **一切 import しない**（構造的に反射経路なし・テストで pin）。観測は
  HTTP 401/303 の Railway HTTP ログで足りる。
- **認証境界**: /app 配下は `/app/login`（GET/POST）**以外すべて session 必須**
  （未認証は 303→/app/login。manifest/sw も認証内=PWA インストールはログイン後）。
- **新規依存なし**（FastAPI/starlette 同梱のみ）。HTTPException は使わない
  （Response/RedirectResponse 直返し・sink 政策と整合）。
- 名称・アイコンは `webapp/manifest.json` **1ファイル差し替え**で変更可能な設計。
- env 未設定は fail-closed（ログイン不能・session 検証は常に否）。
"""

import hashlib
import hmac
import os
import secrets
import time
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import FileResponse, RedirectResponse, Response

router = APIRouter()

WEBAPP_ROOT = Path("webapp")
SESSION_TTL_SECONDS = 7 * 24 * 3600      # 司令塔既定: 7日
_COOKIE = "webapp_session"
_HASH_ENV = "WEBAPP_PASSWORD_HASH"
_SECRET_ENV = "WEBAPP_SESSION_SECRET"
_DEFAULT_ITERATIONS = 600_000


def hash_password(password: str, iterations: int = _DEFAULT_ITERATIONS) -> str:
    """[人]がローカルで env 値を生成するためのヘルパ（平文 env 禁止の実現手段）。"""
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                 bytes.fromhex(salt), iterations).hex()
    return f"pbkdf2_sha256${iterations}${salt}${digest}"


def _verify_password(password: str) -> bool:
    """env のハッシュと照合（compare_digest・env 未設定/形式不正は常に否）。"""
    stored = os.environ.get(_HASH_ENV, "")
    parts = stored.split("$")
    if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
        return False
    try:
        iterations = int(parts[1])
        salt = bytes.fromhex(parts[2])
        expected = parts[3]
    except ValueError:
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                 salt, iterations).hex()
    return hmac.compare_digest(digest, expected)


def _sign(payload: str) -> str | None:
    secret = os.environ.get(_SECRET_ENV, "")
    if not secret:
        return None                       # 鍵未設定=fail-closed（発行も検証も不能）
    return hmac.new(secret.encode("utf-8"), payload.encode("ascii"),
                    hashlib.sha256).hexdigest()


def issue_session(now: int | None = None) -> str | None:
    """署名付き session 値 "<exp_ts>.<sig>" を発行（鍵未設定なら None）。"""
    exp = str((now if now is not None else int(time.time())) + SESSION_TTL_SECONDS)
    sig = _sign(exp)
    return f"{exp}.{sig}" if sig else None


def verify_session(cookie_value: str | None, now: int | None = None) -> bool:
    """session cookie の検証（署名 compare_digest＋期限。鍵未設定は常に否）。"""
    if not cookie_value or "." not in cookie_value:
        return False
    exp_s, _, sig = cookie_value.partition(".")
    if not exp_s.isdigit():
        return False
    expected = _sign(exp_s)
    if expected is None or not hmac.compare_digest(sig, expected):
        return False
    return int(exp_s) > (now if now is not None else int(time.time()))


def _authed(request: Request) -> bool:
    return verify_session(request.cookies.get(_COOKIE))


def _login_redirect() -> RedirectResponse:
    return RedirectResponse("/app/login", status_code=303)


def _file(name: str, media_type: str) -> Response:
    path = WEBAPP_ROOT / name
    if not path.is_file():
        return Response(status_code=404)
    return FileResponse(path, media_type=media_type)


@router.get("/app/login")
async def login_page():
    """ログイン画面（/app 配下で唯一の非認証 GET）。"""
    return _file("login.html", "text/html; charset=utf-8")


@router.post("/app/login")
async def login(password: str = Form(default="")):
    """パスワード照合→session cookie 発行。失敗時は入力値をどこにも反射しない
    （応答は固定 303 のみ・本 module は logging 非使用）。"""
    if not _verify_password(password):
        return RedirectResponse("/app/login?e=1", status_code=303)
    value = issue_session()
    if value is None:                     # 署名鍵未設定=fail-closed
        return RedirectResponse("/app/login?e=1", status_code=303)
    resp = RedirectResponse("/app", status_code=303)
    resp.set_cookie(_COOKIE, value, max_age=SESSION_TTL_SECONDS, path="/app",
                    httponly=True, samesite="strict", secure=True)
    return resp


@router.get("/app")
async def app_shell(request: Request):
    if not _authed(request):
        return _login_redirect()
    return _file("index.html", "text/html; charset=utf-8")


@router.get("/app/app.js")
async def app_js(request: Request):
    if not _authed(request):
        return _login_redirect()
    return _file("app.js", "application/javascript")


@router.get("/app/manifest.json")
async def manifest(request: Request):
    if not _authed(request):
        return _login_redirect()
    return _file("manifest.json", "application/manifest+json")


@router.get("/app/sw.js")
async def sw_js(request: Request):
    if not _authed(request):
        return _login_redirect()
    return _file("sw.js", "application/javascript")
