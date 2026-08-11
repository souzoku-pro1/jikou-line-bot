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
# RV-04c §6/H07: 旧 query token を段階停止する path list（既定 未設定＝どこも停止しない）。
_LEGACY_DISABLED_ENV = "SERVICE_AUTH_LEGACY_DISABLED_PATHS"
_KNOWN_INGEST_PATHS = frozenset({
    "/koseki/ingest", "/registry/ingest", "/bank/ingest",
    "/sortation/ingest", "/valuation/ingest"})

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


# RP1114-M01: 起動境界の固定文言（key_id・フィールド名・registry 断片を含めない）。
# 詳細診断は既存 decision sink（redact 経由・request 時）のみに限定する。
_CONFIG_ERROR_FIXED_MSG = "service auth registry configuration invalid"


def _effective_key_count(registry: dict, now: int) -> int:
    """RP1114-H01: 実効鍵数＝status が active/retiring かつ expires_at 以内の鍵の数。
    revoked のみ・全鍵失効の registry は「鍵数>0 でも運用上ゼロ」なので fail-fast 対象。"""
    return sum(1 for k in registry.values()
               if k.status in ("active", "retiring") and now <= k.expires_at)


def load_registry_strict(*, env_var: str = _REGISTRY_ENV,
                         now: int | None = None) -> dict:
    """RP1114-H01: flag ON 用の registry 読込（4象限 fail-fast）。いずれも
    ServiceAuthConfigError を送出する:
      ① env 欠損・空文字（「空 registry による署名拒否」へ流さない）
      ② JSON 破損・非 object（parse_registry）
      ③ entry 型不正（非 dict・必須 field 欠落・型違い＝_parse_entry）
      ④ 実効鍵数 0（{}・全 revoked・全 expires_at 超過）
    正常時は {key_id: KeyEntry} を返す。"""
    raw = os.environ.get(env_var, "").strip()
    if not raw:
        raise ServiceAuthConfigError("registry is not configured")
    reg = parse_registry(raw)
    now = now if now is not None else int(_utcnow().timestamp())
    if _effective_key_count(reg, now) == 0:
        raise ServiceAuthConfigError("registry has no effective keys")
    return reg


def validate_registry_startup() -> int:
    """P1-114: 起動時 fail-fast。dual-accept flag ON のとき registry env を 4象限
    （欠損/空・JSON/構造不正・entry 型不正・実効鍵数0＝RP1114-H01）で検証し、不正なら
    ServiceAuthConfigError で**起動を止める**（署名リクエスト毎の沈黙 500 を排除する）。
    flag OFF は何もしない（registry 非参照＝現行挙動不変）。戻り値=実効鍵数（起動ログ用）。
    RP1114-M01: 送出する例外は**固定文言のみ**（`from None` で元例外の詳細メッセージ
    〔key_id・フィールド名・registry 断片〕を連鎖表示させない）。"""
    if not dual_accept_enabled():
        return 0
    try:
        return len(load_registry_strict())
    except ServiceAuthConfigError:
        raise ServiceAuthConfigError(_CONFIG_ERROR_FIXED_MSG) from None


def _parse_legacy_disabled_strict(raw: str) -> frozenset:
    """RV-04c H07: SERVICE_AUTH_LEGACY_DISABLED_PATHS を厳格集合検証。
    未設定/空は空集合。以下は ServiceAuthConfigError（固定文言で起動停止）:
    未知値・重複・末尾 slash・空要素・全角（非 ASCII）。実 routing raw path と同一
    （無正規化・完全一致）で照合するため、設定側で正しい形のみ受け付ける。"""
    if not raw.strip():
        return frozenset()
    items = raw.split(",")
    seen = set()
    for it in items:
        if it == "" or it != it.strip():
            raise ServiceAuthConfigError(_LEGACY_ERROR_FIXED_MSG)   # 空要素/前後空白
        if any(ord(c) > 127 for c in it):
            raise ServiceAuthConfigError(_LEGACY_ERROR_FIXED_MSG)   # 全角等
        if len(it) > 1 and it.endswith("/"):
            raise ServiceAuthConfigError(_LEGACY_ERROR_FIXED_MSG)   # 末尾 slash
        if it not in _KNOWN_INGEST_PATHS:
            raise ServiceAuthConfigError(_LEGACY_ERROR_FIXED_MSG)   # 未知値
        if it in seen:
            raise ServiceAuthConfigError(_LEGACY_ERROR_FIXED_MSG)   # 重複
        seen.add(it)
    return frozenset(seen)


_LEGACY_ERROR_FIXED_MSG = "legacy disabled paths configuration invalid"


def validate_legacy_disabled_paths_startup() -> frozenset:
    """RV-04c H07: 起動時 strict 検証。不正なら固定文言で起動停止（P1-114 方式合流）。
    戻り値=検証済み停止 path 集合（起動ログ用）。
    H03: dual-accept OFF のときは **検証しない**（停止 list は dual-accept ON 時のみ意味を
    持つため・OFF は env が inert＝現行 byte 不変）。"""
    if not dual_accept_enabled():
        return frozenset()
    raw = os.environ.get(_LEGACY_DISABLED_ENV, "")
    try:
        return _parse_legacy_disabled_strict(raw)
    except ServiceAuthConfigError:
        raise ServiceAuthConfigError(_LEGACY_ERROR_FIXED_MSG) from None


def legacy_disabled_paths() -> frozenset:
    """実行時アクセサ。H03: dual-accept OFF は空集合（旧経路に一切干渉しない）。
    ON 時は起動 strict 検証が通っている前提（parse 失敗は空集合＝500 storm を避ける保守側）。"""
    if not dual_accept_enabled():
        return frozenset()
    try:
        return _parse_legacy_disabled_strict(os.environ.get(_LEGACY_DISABLED_ENV, ""))
    except ServiceAuthConfigError:
        return frozenset()


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


async def _enforce_signed_request(request: Request) -> None:
    """署名経路の共通判定（RV-0102-PREP で authorize_ingest から逐語抽出・挙動不変）。
    通過は None・失敗は HTTPException を送出する。

    - RP1114-H01: 起動時と同じ 4象限（欠損/空・JSON 不正・entry 型不正・実効鍵0）
      の strict 読込。欠損・空が「空 registry による署名拒否（key_unknown 401）」へ
      流れる経路を排除し、設定不備はすべて registry_config_error の 503 に統一する。
    - P1-114: 壊れ registry を沈黙 500 にしない。起動時 fail-fast
      （validate_registry_startup）の請求時防衛。固定 reason で明示ログし
      明示 503 を返す（共通 raise に合流＝新規 sink を増やさない・台帳 61 維持。
      reason は固定コードのみ＝secret/値の実体は出ない）。
    - 失敗 status は §6.1 のとおり（401/403/400/409・registry 破損は 503）。詳細
      reason はログにのみ残し、レスポンス body には固定文字列のみ（reason 素通しで
      攻撃者に分岐情報を与えない）。
    """
    raw = await request.body()   # BodyCachingRoute がキャッシュ済み
    try:
        registry = load_registry_strict()
    except ServiceAuthConfigError:
        status, reason = 503, "registry_config_error"
    else:
        eff = effective_signed_path(request.scope)
        status, reason = await verify_request(request.headers, raw,
                                              request.method, eff,
                                              registry=registry)
    _log_ingest_decision(request.headers, reason)
    if status != 200:
        raise HTTPException(status_code=status,
                            detail="service auth configuration error"
                            if reason == "registry_config_error"
                            else "signature verification rejected")


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
        # RV-0102-PREP: 署名経路の判定・ログ・raise は _enforce_signed_request へ
        # 逐語共通化（挙動不変・opt-in 入口〔authorize_optionally_signed〕と共用）
        await _enforce_signed_request(request)
        return
    # 署名ヘッダ皆無 → 旧 query token（Phase A）。
    # RV-04c §6: 当該 path が停止 list（dual-accept ON 時のみ参照）にあれば token を検証せず
    # 404（存在しないフリの既存流儀）。停止 lane への旧 token 試行は下で計数（retirement §7）。
    disabled = legacy_disabled_paths()
    if disabled:
        eff = effective_signed_path(request.scope)
        if eff in disabled:
            _log_ingest_decision(headers, "legacy_blocked")
            raise HTTPException(status_code=404, detail="Not Found")
    if not verify_token(token, token_env):
        raise HTTPException(status_code=404, detail="Not Found")


def ingest_guard(token_env: str):
    """ingest エンドポイント用の依存を生成する（token_env ごと）。
    使い方: `_auth: None = Depends(ingest_guard("KOSEKI_INGEST_TOKEN"))`。"""

    async def _guard(request: Request, token: str = "") -> None:
        await authorize_ingest(request, token=token, token_env=token_env)

    return _guard


# ── RV-0102-PREP: 旧 token を持たない受信口（/scan・/ocr/fixed-asset）の
#    署名 opt-in 事前配線（薄い追加・verify_*/authorize_ingest は挙動不変） ──

async def authorize_optionally_signed(request: Request) -> None:
    """署名 opt-in 入口の dual-accept ゲート（RV-0102-PREP）。

    /scan・/ocr/fixed-asset は旧 query token を持たない（現行=無認証受理）ため、
    authorize_ingest（token fallback 前提）は使えない。本ゲートの分岐:
    - flag OFF: 何もしない（署名ヘッダが付いていても無視＝現行挙動と完全同一）
    - flag ON・署名ヘッダ在: 署名経路のみで判定（§2.3 全8段・token/無認証へ
      fallback しない＝downgrade 防止。authorize_ingest の署名分岐と同一実体
      〔_enforce_signed_request〕）
    - flag ON・署名ヘッダ皆無: 受理（従来どおり＝現行挙動不変）。非署名の遮断
      （強制化）と送信側の署名付与（GAS/watcher 点火）は[人]ゲートの別票
    """
    if not dual_accept_enabled():
        return
    if not _has_signature_headers(request.headers):
        return
    await _enforce_signed_request(request)


def optional_signature_guard():
    """署名 opt-in 入口用の依存を生成する（RV-0102-PREP）。
    使い方: `_auth: None = Depends(optional_signature_guard())`。"""

    async def _guard(request: Request) -> None:
        await authorize_optionally_signed(request)

    return _guard
