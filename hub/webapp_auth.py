"""webapp_auth — P4-001: PWA（仮名称「案件管理」）の認証＋shell 配信

裁定（2026-07-27・[人]）: 認証=パスワード＋署名付き session cookie
（DRAFT_P4_PWA_INVENTORY §3 の推奨 (b) 採用）。司令塔既定:

- **env（hub/webhook_auth の既存命名に倣う・平文 env 禁止）**:
  - `WEBAPP_PASSWORD_HASH` … PBKDF2-HMAC-SHA256 の自己記述形式
    `pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>`（生成は本 module の
    `hash_password()` を[人]がローカルで実行して得る）。
  - `WEBAPP_SESSION_SECRET` … session 署名鍵（32byte 以上）。
    **鍵を差し替えると発行済み session は全て失効する**（それが失効手段。
    自動 rotation は作らない=司令塔既定）。
- **session 期限=7日**（`SESSION_TTL_SECONDS`）。cookie は HttpOnly・
  SameSite=Strict・Secure・path=/app。
- **検証は hmac.compare_digest の型**（hub/webhook_auth.verify_token に倣う）。
- **ログイン失敗時に入力値をログへ反射しない**——本 module は logging を
  **一切 import しない**（構造的に反射経路なし・テストで pin）。観測は
  HTTP 401/303 の Railway HTTP ログで足りる。
- **認証境界**: /app 配下は `/app/login`（GET/POST）**以外すべて session 必須**。
  保護 route は `_gate` 関所（単一実装）経由でのみ登録し、公開例外リスト
  （`PUBLIC_ROUTES`）以外の全 route が関所を持つことを機械検査テストで強制
  （fix1 M01）。未知 /app/* は catch-all（認証後 404）。
- **新規依存なし**（FastAPI/starlette 同梱のみ）。HTTPException は使わない
  （Response/RedirectResponse 直返し・sink 政策と整合）。
- 名称・アイコンは `webapp/manifest.json` **1ファイル差し替え**で変更可能な設計。

fail-closed の階層（fix1 H02→fix2 H01 で4象限化）:
- **両方未設定**=機能無効（ログイン不能・session 検証は常に否）。起動は妨げない
  ——本 app は LINE bot 本体と同居しており、PWA 未設定の環境（点火前の本番）で
  起動失敗させると bot 全体が落ちるため。「弱い鍵でログインが動いてしまう」
  状態は未設定では発生しない（動かないだけ）。
- **片方のみ設定**=設定ミスとみなし固定理由の WebAppConfigError で**起動停止**
  （fix2 H01。半端な設定を「未設定の意図」と解釈しない）。
- **両方設定**=強度検証へ（弱い/破損は固定理由で起動失敗。値は例外文言に
  含めない）。同じ下限は実行時にも強制する（validate をすり抜けた経路でも
  弱設定では動かない）。

ログイン防御（fix1 H03→fix2 H02）: password は PBKDF2 実行前に byte 上限で拒否。
プロセス内固定窓カウンタで試行制限。client 識別は X-Forwarded-For の**最終要素**
（単一信頼 proxy=Railway が付加した実クライアント）・XFF 不在時は client.host に
fallback。識別値は SHA-256 のみ保持（生 IP/生 XFF をログ・応答・内部状態の
いずれにも残さない）。制限超過は PBKDF2 に到達せず固定応答。応答は全失敗で同一
（/app/login?e=1・理由の区別も出さない）。

試行制限の前提と限界（fix2 H02・明記）:
- **単一信頼 proxy 前提**: Railway の edge proxy がクライアント自称の XFF の
  末尾に実アドレスを追加する配置を前提とする。この配置前提（多段 proxy 化・
  proxy なし直接公開等）が変わる場合は本識別方式の**設計改定が必要**。
- **複数 worker の限界**: カウンタはプロセス内共有のみ。worker を複数にすると
  制限はプロセス毎に独立し、総試行数は ATTEMPT_LIMIT × worker 数まで通り得る
  （現行 Railway 構成は単一 worker。worker 増設時は共有ストア化の別票）。
- **bucket 上限**: 上限到達時は期限切れ bucket の掃除→なお超過なら最古の
  非ロック bucket のみ退避。**ロックアウト中 bucket は退避しない**（新規 bucket
  作成で既存ロックが解除される抜け道を作らない・fix2 H02-ii）。
"""

import functools
import hashlib
import hmac
import os
import secrets
import time
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import (FileResponse, JSONResponse, RedirectResponse,
                               Response)

router = APIRouter()

# fix1 M03: 別 CWD 起動でも webapp/ を見失わないよう module 位置基準の絶対 path。
WEBAPP_ROOT = Path(__file__).resolve().parent.parent / "webapp"
SESSION_TTL_SECONDS = 7 * 24 * 3600      # 司令塔既定: 7日
_COOKIE = "webapp_session"
_HASH_ENV = "WEBAPP_PASSWORD_HASH"
_SECRET_ENV = "WEBAPP_SESSION_SECRET"
_DEFAULT_ITERATIONS = 600_000

# fix1 H02: 認証材料の強度下限/上限（validate_config と実行時の両方で強制）。
MIN_SECRET_BYTES = 32
MIN_ITERATIONS = 100_000
MAX_ITERATIONS = 10_000_000              # 桁誤り設定による起動後 DoS の遮断
MIN_SALT_BYTES = 16
_DIGEST_HEX_LEN = 64                     # SHA-256

# fix1 H03: ログイン防御の固定値。
MAX_PASSWORD_BYTES = 1024
ATTEMPT_WINDOW_SECONDS = 600
ATTEMPT_LIMIT = 10
MAX_BUCKETS = 10_000                     # fix2 H02-ii: bucket 数の上限
_attempts: dict[str, tuple[int, int]] = {}   # sha256(client) -> (window_start, count)

# fix1 M01: 公開例外リスト（これ以外の /app 配下 route は全て _gate 必須）。
PUBLIC_ROUTES = {("/app/login", "GET"), ("/app/login", "POST")}


class WebAppConfigError(RuntimeError):
    """認証材料の設定不正（固定理由のみ・env 値は文言に含めない）。"""


def _parse_password_hash(stored: str) -> tuple[int, bytes, str] | None:
    """自己記述形式を解析し強度下限も検査（不正・弱設定は None=fail-closed）。

    負・零・過大 iterations や非 hex をここで全て弾くことで、照合経路に
    未処理例外（pbkdf2_hmac の ValueError 等）を発生させない（fix1 H02）。
    """
    parts = stored.split("$")
    if len(parts) != 4 or parts[0] != "pbkdf2_sha256":
        return None
    try:
        iterations = int(parts[1])
        salt = bytes.fromhex(parts[2])
    except ValueError:
        return None
    digest = parts[3]
    if not (MIN_ITERATIONS <= iterations <= MAX_ITERATIONS):
        return None
    if len(salt) < MIN_SALT_BYTES:
        return None
    if len(digest) != _DIGEST_HEX_LEN or not all(
            c in "0123456789abcdef" for c in digest):
        return None
    return iterations, salt, digest


def validate_config() -> None:
    """起動時検証（fix1 H02→fix2 H01: 4象限）。

    両方未設定=機能無効として許容（docstring 冒頭の理由）／**片方のみ設定=
    固定理由で起動停止**（設定ミスを未設定の意図と解釈しない）／両方設定=
    強度検証（弱い/破損は起動失敗）。理由文言に env 値は含めない。
    """
    secret = os.environ.get(_SECRET_ENV)
    stored = os.environ.get(_HASH_ENV)
    if secret is None and stored is None:
        return                            # 両方未設定=機能無効（起動許容）
    if secret is None or stored is None:  # 片方のみ=設定ミス（fix2 H01）
        raise WebAppConfigError(
            f"{_HASH_ENV} / {_SECRET_ENV}: both must be set together "
            "(only one is set)")
    if len(secret.encode("utf-8")) < MIN_SECRET_BYTES:
        raise WebAppConfigError(
            f"{_SECRET_ENV}: too short (>= {MIN_SECRET_BYTES} bytes required)")
    if _parse_password_hash(stored) is None:
        raise WebAppConfigError(
            f"{_HASH_ENV}: malformed or below strength minimums")


@router.on_event("startup")
async def _startup_validate() -> None:
    validate_config()


def hash_password(password: str, iterations: int = _DEFAULT_ITERATIONS) -> str:
    """[人]がローカルで env 値を生成するためのヘルパ（平文 env 禁止の実現手段）。
    強度下限/上限外の iterations は生成段階で拒否（fix1 H02）。"""
    if not (MIN_ITERATIONS <= iterations <= MAX_ITERATIONS):
        raise WebAppConfigError("iterations out of allowed range")
    salt = secrets.token_hex(MIN_SALT_BYTES)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                 bytes.fromhex(salt), iterations).hex()
    return f"pbkdf2_sha256${iterations}${salt}${digest}"


def _verify_password(password: str) -> bool:
    """env のハッシュと照合（compare_digest・未設定/形式不正/弱設定は常に否）。"""
    parsed = _parse_password_hash(os.environ.get(_HASH_ENV, ""))
    if parsed is None:
        return False
    iterations, salt, expected = parsed
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"),
                                 salt, iterations).hex()
    return hmac.compare_digest(digest, expected)


def _sign(payload: str) -> str | None:
    secret = os.environ.get(_SECRET_ENV, "")
    if len(secret.encode("utf-8")) < MIN_SECRET_BYTES:
        return None     # 未設定・短い鍵=fail-closed（発行も検証も不能・fix1 H02）
    return hmac.new(secret.encode("utf-8"), payload.encode("ascii"),
                    hashlib.sha256).hexdigest()


def issue_session(now: int | None = None) -> str | None:
    """署名付き session 値 "<exp_ts>.<sig>" を発行（鍵が無効なら None）。"""
    exp = str((now if now is not None else int(time.time())) + SESSION_TTL_SECONDS)
    sig = _sign(exp)
    return f"{exp}.{sig}" if sig else None


def verify_session(cookie_value: str | None, now: int | None = None) -> bool:
    """session cookie の検証（署名 compare_digest＋期限。鍵が無効なら常に否）。"""
    if not cookie_value or "." not in cookie_value:
        return False
    exp_s, _, sig = cookie_value.partition(".")
    if not exp_s.isdigit():
        return False
    expected = _sign(exp_s)
    if expected is None or not hmac.compare_digest(sig, expected):
        return False
    return int(exp_s) > (now if now is not None else int(time.time()))


def _rate_key(request: Request) -> str:
    """試行制限 key（fix2 H02-i・[人]裁定済み）。

    X-Forwarded-For の**最終要素**を実クライアントとして採用する——単一信頼
    proxy（Railway）がクライアント自称の XFF 末尾に実アドレスを追加する配置を
    前提とする（この前提が変わる場合は設計改定要・module docstring 参照）。
    XFF 不在時（直接接続・テスト）は client.host に fallback。
    生 IP・生 XFF 値は保持せず SHA-256 のみ（偽装値も hash 化のみで非反射）。
    """
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        ip = xff.split(",")[-1].strip()
    else:
        ip = request.client.host if request.client else ""
    return hashlib.sha256(ip.encode("utf-8")).hexdigest()


def _locked(key: str, now: float) -> bool:
    entry = _attempts.get(key)
    if entry is None:
        return False
    window_start, count = entry
    if now - window_start >= ATTEMPT_WINDOW_SECONDS:
        del _attempts[key]                # 窓の満了で自然解除（固定窓）
        return False
    return count >= ATTEMPT_LIMIT


def _prune_buckets(now: float) -> None:
    """bucket 上限到達時の退避（fix2 H02-ii・_attempts.clear() の廃止）。

    手順: ①期限切れ bucket を掃除 → ②なお超過なら**最古の非ロック bucket のみ**
    退避。ロックアウト中（count >= ATTEMPT_LIMIT）の bucket は退避しない＝
    大量 bucket 作成で既存ロックアウトを解除させる抜け道を作らない。
    全 bucket がロック中で上限超過のままの場合は保全優先で退避しない
    （各ロックは窓満了で①により自然掃除される・メモリは有限窓で有界）。
    """
    for k in [k for k, (start, _) in _attempts.items()
              if now - start >= ATTEMPT_WINDOW_SECONDS]:
        del _attempts[k]
    while len(_attempts) > MAX_BUCKETS:
        unlocked = [(start, k) for k, (start, count) in _attempts.items()
                    if count < ATTEMPT_LIMIT]
        if not unlocked:
            break                         # 全部ロック中=ロックアウト保全を優先
        del _attempts[min(unlocked)[1]]


def _register_failure(key: str, now: float) -> None:
    entry = _attempts.get(key)
    if entry is None or now - entry[0] >= ATTEMPT_WINDOW_SECONDS:
        _attempts[key] = (int(now), 1)
    else:
        _attempts[key] = (entry[0], entry[1] + 1)
    if len(_attempts) > MAX_BUCKETS:
        _prune_buckets(now)


def _authed(request: Request) -> bool:
    return verify_session(request.cookies.get(_COOKIE))


# fix1 H01: PWA 保護領域の共通キャッシュ契約。顧客 PII を含み得る全応答を
# ブラウザ/中間キャッシュに残さない（`private` は共有キャッシュ禁止・`no-store` は
# 保存自体を禁止）。_gate 経由の全応答（成功・400・404・303 redirect）へ一律付与し、
# merge 済み P4-002 の案件 API（顧客 PII 含む）にも遡及適用される。login（公開）にも
# 付与する（secret は含まないが保存させないのが無難）。
_CACHE_CONTROL = "no-store, private"


def _no_store(resp: Response) -> Response:
    resp.headers["Cache-Control"] = _CACHE_CONTROL
    return resp


def _login_redirect() -> RedirectResponse:
    return _no_store(RedirectResponse("/app/login", status_code=303))


def _fail_redirect() -> RedirectResponse:
    """全失敗共通の固定応答（理由・入力値を一切区別しない/反射しない）。"""
    return _no_store(RedirectResponse("/app/login?e=1", status_code=303))


def _gate(fn):
    """認証関所（fix1 M01・単一実装）。保護 route は本 decorator 経由でのみ
    登録する。機械検査テストが「PUBLIC_ROUTES 以外の全 route が本関所を持つ」
    ことを assert する（将来 route 追加時の検査忘れ防波堤）。

    P4-004 fix1 H01: 保護 route の全応答に Cache-Control: no-store, private を
    一元付与する（共通契約）。case_views/approval_view の API は dict を返すため、
    dict/list は JSONResponse へ正規化してからヘッダを付ける（既存挙動＝FastAPI が
    dict を JSON 化するのと等価・content は不変）。"""
    @functools.wraps(fn)
    async def wrapper(request: Request):
        if not _authed(request):
            return _login_redirect()
        result = await fn(request)
        if not isinstance(result, Response):     # dict/list → JSONResponse 正規化
            result = JSONResponse(result)
        return _no_store(result)
    wrapper.__webapp_gate__ = True
    return wrapper


def _file(name: str, media_type: str) -> Response:
    path = WEBAPP_ROOT / name
    if not path.is_file():
        return Response(status_code=404)
    return FileResponse(path, media_type=media_type)


@router.get("/app/login")
async def login_page():
    """ログイン画面（/app 配下で唯一の非認証 route＝PUBLIC_ROUTES）。"""
    return _no_store(_file("login.html", "text/html; charset=utf-8"))


@router.post("/app/login")
async def login(request: Request, password: str = Form(default="")):
    """パスワード照合→session cookie 発行。失敗時は入力値をどこにも反射しない
    （応答は固定 303 のみ・本 module は logging 非使用）。

    fix1 H03: ①試行制限（固定窓・超過中は PBKDF2 非到達で固定応答）
    ②byte 上限（PBKDF2 実行前に検査・超過は固定応答）。
    """
    key = _rate_key(request)
    now = time.time()
    if _locked(key, now):
        return _fail_redirect()
    if len(password.encode("utf-8")) > MAX_PASSWORD_BYTES:
        _register_failure(key, now)
        return _fail_redirect()
    if not _verify_password(password):
        _register_failure(key, now)
        return _fail_redirect()
    value = issue_session()
    if value is None:                     # 署名鍵が無効=fail-closed
        return _fail_redirect()
    _attempts.pop(key, None)              # 成功でカウンタ解除
    resp = RedirectResponse("/app", status_code=303)
    resp.set_cookie(_COOKIE, value, max_age=SESSION_TTL_SECONDS, path="/app",
                    httponly=True, samesite="strict", secure=True)
    return _no_store(resp)


@router.get("/app")
@_gate
async def app_shell(request: Request):
    return _file("index.html", "text/html; charset=utf-8")


@router.get("/app/app.js")
@_gate
async def app_js(request: Request):
    return _file("app.js", "application/javascript")


@router.get("/app/manifest.json")
@_gate
async def manifest(request: Request):
    return _file("manifest.json", "application/manifest+json")


@router.get("/app/sw.js")
@_gate
async def sw_js(request: Request):
    return _file("sw.js", "application/javascript")


@router.get("/app/shell.js")
@_gate
async def shell_js(request: Request):
    """共通画面枠（ヘッダ・ナビ・ログアウト）の DOM 構築 JS（PWA-BATCH-1 A(iii)）。"""
    return _file("shell.js", "application/javascript")


@router.get("/app/icons/icon-192.png")
@_gate
async def icon_192(request: Request):
    """ホーム画面アイコン（PWA-BATCH-1 A(i)。webapp/icons/ 配下＝固定名のみ配信）。"""
    return _file("icons/icon-192.png", "image/png")


@router.get("/app/icons/icon-512.png")
@_gate
async def icon_512(request: Request):
    return _file("icons/icon-512.png", "image/png")


@router.post("/app/logout")
@_gate
async def logout(request: Request):
    """ログアウト（PWA-BATCH-1 A(iii)）。当該ブラウザの cookie 削除のみ——
    session は無状態の署名付き値のため、サーバ側の全失効手段は従来どおり
    WEBAPP_SESSION_SECRET の差し替え（P4-001 裁定のまま・自動 rotation なし）。"""
    resp = RedirectResponse("/app/login", status_code=303)
    resp.delete_cookie(_COOKIE, path="/app")
    return resp


@router.get("/app/{_rest:path}")
@_gate
async def app_unknown(request: Request):
    """未知 /app/* の catch-all（fix1 M02）。未認証は他と同じく 303→login・
    認証後は 404（path 値は応答へ反射しない・ファイル配信もしない）。"""
    return Response(status_code=404)
