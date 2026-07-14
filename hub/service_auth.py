"""service_auth — NM01 v1 HMAC 署名検証コア（本番モジュール・RV-04a）

PoC（`test_hmac_multipart_poc.py`）で硬化した検証器を本番品質へ移植したもの。
**RV-04a 時点では本番 router/ingest 群へ結線しない**（RV-04b で結線する）。

準拠: `docs/design-drafts/DRAFT_RV04_HMAC_MIGRATION.md`
- §2.1 canonical（length-prefix・ORDER 固定）… NM01 v1 FROZEN
- §2.3 検証順（8段・fail-closed・downgrade 防止）
- §6.1 status–reason table / §6.2 key lifecycle reason contract（単一の正）
- §2.4 nonce 一回性（案B=専用 DB 表・司令塔裁定 2026-07-14）

設計判断:
- **path は ASGI `scope["raw_path"]`（decode 前生バイト）基準**（H01）。decode 済み path は
  %2F 等で separator を smuggling されるため使わない。`raw_path` 欠落は fail-closed。
- **nonce は DB（`signature_nonce` 表）で一回性を担保**。process-memory 実装は禁止
  （再起動・多インスタンスで replay をすり抜けさせない）。DB 到達不能は fail-closed
  （握って受理しない）。
- **key registry は env（JSON）から読む**。secret 実体はコード/ログに出さない。
- retiring 鍵は受理しつつ warning を1回（可視は key_id・caller_id のみ）。
"""

import hashlib
import hmac
import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import sqlalchemy as sa
from fastapi import HTTPException, Request       # RV-04b 結線層で使用
from fastapi.routing import APIRoute             # RV-04b BodyCachingRoute
from sqlalchemy.exc import IntegrityError

from hub.db import session_scope
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式）
from hub.webhook_auth import verify_token  # RV-04b: dual-accept の旧 token 経路

logger = logging.getLogger("hub.service_auth")

_ORDER_TAG = "v1"
_VALID_STATUS = frozenset({"active", "retiring", "revoked"})
_DEFAULT_SKEW = 300
_REGISTRY_ENV = "SERVICE_HMAC_KEY_REGISTRY"
_SKEW_ENV = "SIG_MAX_SKEW_SEC"
# RV-04b dual-accept feature flag（既定 OFF＝完全に旧挙動。ON で署名経路併存）
_DUAL_ACCEPT_ENV = "SERVICE_AUTH_DUAL_ACCEPT_ENABLED"
_FLAG_TRUE = frozenset({"1", "true", "on", "yes"})

# §2.2: 署名経路で必須の 7 ヘッダ。欠落は第1段で missing_header（bad_sig 任せにしない）。
_REQUIRED_HEADERS = ("X-Sig-Version", "X-Sig-Key-Id", "X-Sig-Caller",
                     "X-Sig-Timestamp", "X-Sig-Nonce", "X-Sig-Content-SHA256",
                     "X-Sig-Signature")
_NONCE_RE = re.compile(r"[0-9a-fA-F]{32}")   # §2.2: nonce は 128bit hex 固定
_NONCE_HEX_LEN = 32


# ── nonce store（DB・案B）: signature_nonce 表 ──────────────────────────────
# app-state 専用 metadata（alembic env.py の target_metadata list に統合する）。
metadata = sa.MetaData()

signature_nonce = sa.Table(
    "signature_nonce", metadata,
    sa.Column("nonce", sa.Text, primary_key=True),   # UNIQUE(nonce) = replay 検知の実体
    sa.Column("key_id", sa.Text, nullable=False),
    sa.Column("caller", sa.Text, nullable=False),
    sa.Column("seen_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    # H-02: 128bit hex（32 文字）固定を DB でも保証（検証層の bad_nonce と二重の防御）
    sa.CheckConstraint("length(nonce) = 32", name="ck_signature_nonce_len"),
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── key registry（env・JSON） ────────────────────────────────────────────────

class ServiceAuthConfigError(RuntimeError):
    """key registry の env 値が不正（構造/型/status 違反・secret 不正 hex 等）。
    起動時検証で早期に落とすための明示例外（値の実体はメッセージに出さない）。"""


@dataclass(frozen=True)
class KeyEntry:
    key_id: str
    secret: bytes = field(repr=False)   # HMAC 鍵（H-01: repr/ログ/例外に出さない）
    caller: str = ""
    allowed_methods: frozenset = frozenset()   # {"POST"} 等（大文字化済み）
    allowed_paths: frozenset = frozenset()     # {"/koseki/ingest"} 等
    not_before: int = 0           # unix 秒
    expires_at: int = 0           # unix 秒
    status: str = "active"        # active / retiring / revoked
    # NB: secret に既定が無い（field(repr=False)）ため後続フィールドにも既定を付与して
    #     dataclass の "non-default after default なし" を満たす（構築は常に全指定）。

    def __repr__(self) -> str:
        # H-01: secret は値・hex を一切出さず <redacted> に固定。%r/f"{x!r}"/str も同じ。
        return (f"KeyEntry(key_id={self.key_id!r}, secret=<redacted>, "
                f"caller={self.caller!r}, allowed_methods={sorted(self.allowed_methods)!r}, "
                f"allowed_paths={sorted(self.allowed_paths)!r}, "
                f"not_before={self.not_before}, expires_at={self.expires_at}, "
                f"status={self.status!r})")


def _parse_entry(key_id: str, raw: dict) -> KeyEntry:
    """1 鍵分の構造/型を検証して KeyEntry を返す。secret は hex（>=32byte）。"""
    if not isinstance(raw, dict):
        raise ServiceAuthConfigError(f"key '{key_id}': entry must be an object")
    try:
        secret_hex = raw["secret"]
        caller = raw["caller"]
        methods = raw["allowed_methods"]
        paths = raw["allowed_paths"]
        status = raw["status"]
    except KeyError as e:
        raise ServiceAuthConfigError(f"key '{key_id}': missing field {e.args[0]}")
    if not isinstance(secret_hex, str):
        raise ServiceAuthConfigError(f"key '{key_id}': secret must be hex string")
    try:
        secret = bytes.fromhex(secret_hex)
    except ValueError:
        raise ServiceAuthConfigError(f"key '{key_id}': secret is not valid hex")
    if len(secret) < 32:
        raise ServiceAuthConfigError(f"key '{key_id}': secret must be >= 32 bytes")
    if not isinstance(caller, str) or not caller:
        raise ServiceAuthConfigError(f"key '{key_id}': caller must be non-empty string")
    if not isinstance(methods, list) or not all(isinstance(m, str) for m in methods):
        raise ServiceAuthConfigError(f"key '{key_id}': allowed_methods must be list[str]")
    if not isinstance(paths, list) or not all(isinstance(p, str) for p in paths):
        raise ServiceAuthConfigError(f"key '{key_id}': allowed_paths must be list[str]")
    if status not in _VALID_STATUS:
        raise ServiceAuthConfigError(f"key '{key_id}': status must be one of {sorted(_VALID_STATUS)}")
    try:
        not_before = int(raw.get("not_before", 0))
        expires_at = int(raw["expires_at"])
    except (KeyError, TypeError, ValueError):
        raise ServiceAuthConfigError(f"key '{key_id}': not_before/expires_at must be int (expires_at required)")
    return KeyEntry(
        key_id=key_id, secret=secret, caller=caller,
        allowed_methods=frozenset(m.upper() for m in methods),
        allowed_paths=frozenset(paths),
        not_before=not_before, expires_at=expires_at, status=status)


def parse_registry(raw_json: str) -> dict:
    """JSON 文字列 → {key_id: KeyEntry}。構造検証つき（起動時検証の実体）。"""
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as e:
        raise ServiceAuthConfigError(f"registry JSON parse error: {e.msg}")
    if not isinstance(data, dict):
        raise ServiceAuthConfigError("registry must be a JSON object of key_id -> entry")
    return {kid: _parse_entry(kid, entry) for kid, entry in data.items()}


def load_registry_from_env(*, env_var: str = _REGISTRY_ENV, required: bool = False) -> dict:
    """env から key registry を読み検証する。RV-04b の startup で呼ぶ想定。
    未設定時は required=False なら空 registry（未結線の間は無害）・True なら例外。"""
    raw = os.environ.get(env_var, "").strip()
    if not raw:
        if required:
            raise ServiceAuthConfigError(f"{env_var} is not set")
        return {}
    return parse_registry(raw)


def _max_skew() -> int:
    raw = os.environ.get(_SKEW_ENV, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return _DEFAULT_SKEW
    return v if v > 0 else _DEFAULT_SKEW


# ── canonical / 署名 / path 正規化（§2.1・PoC 移植） ─────────────────────────

def canonical_v1(key_id, caller_id, method, normalized_path,
                 timestamp_str, nonce_hex, content_sha256_hex) -> bytes:
    """§2.1 length-prefix: for each f in ORDER: ascii(len(utf8(f))) || ":" || utf8(f) || "\\n"."""
    order = [_ORDER_TAG, key_id, caller_id, method.upper(), normalized_path,
             timestamp_str, nonce_hex, content_sha256_hex]
    out = b""
    for f in order:
        u = f.encode("utf-8")
        out += str(len(u)).encode("ascii") + b":" + u + b"\n"
    return out


def sign_v1(secret: bytes, canonical: bytes) -> str:
    return hmac.new(secret, canonical, hashlib.sha256).hexdigest()


def normalize_path(raw_path):
    """§2.1 H02: decode 前 raw path 基準。正規化は末尾 slash 除去のみ。
    非 ASCII 生バイト / percent-encoding（`%` を含む）/ 連続 slash / dot segment を拒否
    （None＝400 相当）。raw_path は scope["raw_path"] を latin-1 で 1:1 復号した str を想定。"""
    if raw_path is None:
        return None
    if any(ord(c) > 127 for c in raw_path):
        return None
    if "%" in raw_path:
        return None
    if "//" in raw_path:
        return None
    if any(s in (".", "..") for s in raw_path.split("/")):
        return None
    if len(raw_path) > 1 and raw_path.endswith("/"):
        raw_path = raw_path.rstrip("/")
    return raw_path


def effective_signed_path(scope, prefix: str = "") -> str | None:
    """ASGI scope から署名対象の生 path を取り出す（H01）。raw_path 欠落は None＝fail-closed。
    prefix を与えると生バイトのまま除去する（mount 配下運用の吸収用・既定は無除去）。"""
    rp = scope.get("raw_path")
    if rp is None:
        return None
    s = rp.decode("latin-1")   # bytes→str（1:1・生バイト保持）
    if prefix and (s == prefix or s.startswith(prefix + "/")):
        return s[len(prefix):] or "/"
    return s


# ── 署名検証（§2.3 の 1〜7 段・同期・純粋関数） ──────────────────────────────

@dataclass(frozen=True)
class VerifyContext:
    key_id: str
    caller: str
    nonce: str
    expires_at: datetime   # nonce 保持期限（timestamp + skew）
    retiring: bool


def verify_signature(headers, raw_body: bytes, method: str, raw_path,
                     registry: dict, now: int, skew: int):
    """§2.3 の 1〜7 段（署名まで）を検証する純粋関数。
    Returns: (status:int, reason:str, ctx:VerifyContext|None)。
    200/"ok"（or "ok_retiring"）のとき ctx を返す（呼び出し側が 8 段 nonce を処理）。"""
    # 1. 署名ヘッダが存在すれば署名経路（token fallback 禁止）。
    if any(k.lower().startswith("x-sig-") for k in headers):
        # §2.2: 必須 7 ヘッダの欠落/空は第1段で missing_header（bad_sig 任せにしない・H-02）
        for h in _REQUIRED_HEADERS:
            if not headers.get(h):
                return 401, "missing_header", None
        # version!=v1（present だが値違反）は downgrade として bad_version
        if headers.get("X-Sig-Version") != "v1":
            return 401, "bad_version", None
        # nonce 形式（128bit hex 固定）。欠落は上で missing_header・形式違反は bad_nonce
        if not _NONCE_RE.fullmatch(headers.get("X-Sig-Nonce", "")):
            return 401, "bad_nonce", None
    else:
        return 401, "no_signature", None
    key_id = headers.get("X-Sig-Key-Id", "")
    caller = headers.get("X-Sig-Caller", "")
    ts = headers.get("X-Sig-Timestamp", "")
    nonce = headers.get("X-Sig-Nonce", "")
    csha = headers.get("X-Sig-Content-SHA256", "")
    sig = headers.get("X-Sig-Signature", "")
    # 2. key registry（§6.2 reason 分離: unknown/revoked/not_before/expired）
    key = registry.get(key_id)
    if key is None:
        return 401, "key_unknown", None
    if key.status == "revoked":
        return 401, "key_revoked", None
    if now < key.not_before:
        return 401, "key_not_yet_valid", None
    if now > key.expires_at:
        return 401, "key_expired", None
    if key.status not in ("active", "retiring"):
        return 401, "key_unknown", None      # 未定義 status は保守的に拒否
    retiring = key.status == "retiring"
    # 3. caller 一致
    if caller != key.caller:
        return 401, "caller_mismatch", None
    # 4. method / normalized_path 許可（raw_path 基準・client 指定 path ヘッダ非信用）
    npath = normalize_path(raw_path)
    if npath is None:
        return 400, "bad_path", None
    if method.upper() not in key.allowed_methods:
        return 403, "method_denied", None
    if npath not in key.allowed_paths:
        return 403, "path_denied", None
    # 5. timestamp SKEW
    try:
        ts_i = int(ts)
    except (TypeError, ValueError):
        return 401, "bad_ts", None
    if not (now - skew <= ts_i <= now + skew):
        return 401, "skew", None
    # 6. content_sha256 == 実 body hash（body 改変検知）
    if not hmac.compare_digest(csha, hashlib.sha256(raw_body).hexdigest()):
        return 401, "body_mismatch", None
    # 7. 署名再計算（compare_digest）
    expect = sign_v1(key.secret, canonical_v1(key_id, caller, method, npath,
                                              ts, nonce, csha))
    if not hmac.compare_digest(sig, expect):
        return 401, "bad_sig", None
    reason = "ok_retiring" if retiring else "ok"
    expires_dt = datetime.fromtimestamp(ts_i + skew, tz=timezone.utc)
    return 200, reason, VerifyContext(key_id=key_id, caller=caller, nonce=nonce,
                                      expires_at=expires_dt, retiring=retiring)


# ── nonce 一回性（§2.4・DB。8 段） ──────────────────────────────────────────

async def consume_nonce(nonce: str, key_id: str, caller: str,
                        expires_at: datetime, now: datetime | None = None) -> bool:
    """nonce を DB に一回だけ記録する。UNIQUE 衝突=replay→False。成功=True。
    保持期限切れ（expires_at < now）の行は検証時 lazy 削除する（§2.4）。now は検証と
    同一のクロックを渡す（未指定は実時刻）。DB 未設定/到達不能は例外を送出
    （fail-closed・memory fallback 禁止）。"""
    now = now if now is not None else _utcnow()
    # lazy cleanup（best-effort・独立 tx・検証と同一クロック基準）
    async with session_scope() as session:
        await session.execute(
            sa.delete(signature_nonce).where(signature_nonce.c.expires_at < now))
    # 一回性の記録（UNIQUE(nonce) 衝突で replay を検知）
    try:
        async with session_scope() as session:
            await session.execute(sa.insert(signature_nonce).values(
                nonce=nonce, key_id=key_id, caller=caller,
                seen_at=_utcnow(), expires_at=expires_at))
    except IntegrityError:
        return False
    return True


async def verify_request(headers, raw_body: bytes, method: str, raw_path, *,
                         registry: dict, now: int | None = None,
                         skew: int | None = None) -> tuple[int, str]:
    """§2.3 全 8 段。RV-04b はこの1本を ingest 経路の前段に差すだけでよい。
    nonce（8段）は DB で担保する。DB 到達不能は例外送出（受理しない）。"""
    now = now if now is not None else int(_utcnow().timestamp())
    skew = skew if skew is not None else _max_skew()
    status, reason, ctx = verify_signature(headers, raw_body, method, raw_path,
                                           registry, now, skew)
    if ctx is None:
        return status, reason
    # 8. nonce 一回性（検証と同一クロックで lazy cleanup する）
    now_dt = datetime.fromtimestamp(now, tz=timezone.utc)
    if not await consume_nonce(ctx.nonce, ctx.key_id, ctx.caller, ctx.expires_at, now=now_dt):
        return 409, "replay"
    # 全 8 段通過。retiring は受理 + warning 1 回（可視は key_id・caller のみ）。
    if ctx.retiring:
        logger.warning("service-auth: retiring key accepted key_id=%s caller=%s",
                       emit(ctx.key_id, "record_id", "log", "operator"),
                       emit(ctx.caller, "record_id", "log", "operator"))
    return 200, reason


# ── RV-04b: ingest 群への dual-accept 結線（薄い追加・verify_* 純関数は不変） ──
# ここから下は「結線用の薄い追加」。canonical/verify_signature/verify_request は変更しない。

def dual_accept_enabled() -> bool:
    """RV-04b feature flag。既定 OFF（未設定/0）＝完全に旧 query token 挙動。"""
    return os.environ.get(_DUAL_ACCEPT_ENV, "").strip().lower() in _FLAG_TRUE


def _has_signature_headers(headers) -> bool:
    return any(k.lower().startswith("x-sig-") for k in headers)


def _log_ingest_decision(headers, reason: str) -> None:
    """署名経路の判定結果を emit 契約でログ（key_id/caller_id/reason のみ可視・
    secret/署名値/顧客情報は出さない）。reason は固定コード（record_id 値域で素通し）。"""
    logger.info("service-auth ingest decision key_id=%s caller=%s reason=%s",
                emit(headers.get("X-Sig-Key-Id", ""), "record_id", "log", "operator"),
                emit(headers.get("X-Sig-Caller", ""), "record_id", "log", "operator"),
                emit(reason, "record_id", "log", "operator"))


class BodyCachingRoute(APIRoute):
    """flag ON かつ署名ヘッダ在時のみ、form parse 前に生 body を読み込みキャッシュする
    ルート。これにより署名検証（content_sha256）と後続の UploadFile/Form 受理が同一 body
    で共存できる（Starlette の body キャッシュ）。**flag OFF/署名ヘッダ皆無時は完全な
    passthrough**（生 body を読まない＝現行挙動と byte 同一）。適用は ingest 5 入口のみで、
    顧客 Bot（/webhook 等）には一切適用しない。"""

    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request):
            if dual_accept_enabled() and _has_signature_headers(request.headers):
                await request.body()   # form parse 前に _body をキャッシュ
            return await original(request)

        return handler


async def authorize_ingest(request: Request, *, token: str, token_env: str) -> None:
    """ingest 入口の dual-accept ゲート。受理なら None・拒否は HTTPException を送出。

    - flag OFF: 旧 query token のみ（署名ヘッダは無視＝現行挙動と完全同一）。
    - flag ON・署名ヘッダ在: **署名経路のみ**で判定（§2.3・token へ fallback しない
      ＝downgrade 防止）。§6.1 の status/reason をそのまま返す。
    - flag ON・署名ヘッダ皆無: 旧 query token（Phase A の併存）。
    """
    headers = request.headers
    if not dual_accept_enabled():
        if not verify_token(token, token_env):
            raise HTTPException(status_code=404, detail="Not Found")
        return
    if _has_signature_headers(headers):
        raw = await request.body()   # BodyCachingRoute がキャッシュ済み
        registry = load_registry_from_env()
        eff = effective_signed_path(request.scope)
        status, reason = await verify_request(headers, raw, request.method, eff,
                                              registry=registry)
        _log_ingest_decision(headers, reason)
        if status != 200:
            # 署名経路の失敗は token へ落とさない（downgrade 防止）。
            # status は §6.1 のとおり（401/403/400/409）。詳細 reason は上のログにのみ残し、
            # レスポンス body には固定文字列のみ（reason 素通しで攻撃者に分岐情報を与えない）。
            raise HTTPException(status_code=status, detail="signature verification rejected")
        return
    # 署名ヘッダ皆無 → 旧 query token（Phase A）
    if not verify_token(token, token_env):
        raise HTTPException(status_code=404, detail="Not Found")


def ingest_guard(token_env: str):
    """ingest エンドポイント用の依存を生成する（token_env ごと）。
    使い方: `_auth: None = Depends(ingest_guard("KOSEKI_INGEST_TOKEN"))`。"""

    async def _guard(request: Request, token: str = "") -> None:
        await authorize_ingest(request, token=token, token_env=token_env)

    return _guard
