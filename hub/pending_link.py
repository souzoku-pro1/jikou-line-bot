"""pending_link — UX-PENDING-1 (1)(5): 業務 LINE 通知に載せる署名付き・期限付きリンク

- 種別 2 つだけ: `r`（その件の返信画面を開く）／`c`（「対応終了」の確認画面を開く）。リンクから顧客へ送信する経路は無い。
- 符号の形: `<種別>.<pending_id>.<有効期限 unix 秒>.<HMAC-SHA256 先頭 32 桁>`。鍵は PWA のセッションと同じ
  WEBAPP_SESSION_SECRET（hub.webapp_auth._sign を再利用・鍵が無効なら発行も検証も不能＝URL を出さない）。
- 識別は pending_id（DB の連番・署名付き。fix1 BU-06 裁定: 連番の露出を許容＝HMAC で改ざん不可・閲覧者は大野のみ・漏れるのは件数程度）。
  氏名・LINE userId・本文・会話参照は載せない（RV-10）。
- 期限 72 時間（大野裁定）。期限切れは「無効」ではなく expired として返し、呼び出し側はログイン後の一覧へ着地させる。
- 公開ホストは Railway 注入の RAILWAY_PUBLIC_DOMAIN のみ（hub.shindan_link.public_base_url と同じ・未設定なら URL 行を出さない）。
"""

import hmac
import re
import time

from hub.shindan_link import public_base_url
from hub.webapp_auth import _sign

KIND_REPLY = "r"
KIND_CLOSE = "c"
KINDS = (KIND_REPLY, KIND_CLOSE)
TTL_SECONDS = 72 * 3600
SIG_HEX = 32
PATH_PREFIX = "/app/go"
INBOX_PATH = "/app/inbox"

STATUS_OK = "ok"
STATUS_EXPIRED = "expired"
STATUS_INVALID = "invalid"

_TOKEN_RE = re.compile(r"^[rc]\.[0-9]{1,12}\.[0-9]{1,12}\.[0-9a-f]{32}$")


def _payload(kind: str, pending_id: int, exp: int) -> str:
    return f"{kind}.{int(pending_id)}.{int(exp)}"


def make_token(kind: str, pending_id: int, *, now: int | None = None) -> str | None:
    """符号を発行する（鍵が無効なら None）。"""
    if kind not in KINDS or int(pending_id) <= 0:
        return None
    exp = (now if now is not None else int(time.time())) + TTL_SECONDS
    sig = _sign(_payload(kind, pending_id, exp))
    if not sig:
        return None
    return f"{_payload(kind, pending_id, exp)}.{sig[:SIG_HEX]}"


def verify(token: str, *, now: int | None = None) -> tuple[str, str | None, int | None]:
    """(status, kind, pending_id)。status は ok / expired / invalid。形式 → 署名 → 期限 の順に見る。
    invalid は kind・pending_id を返さない（改ざん値を下流へ流さない）。"""
    if not _TOKEN_RE.fullmatch(token or ""):
        return STATUS_INVALID, None, None
    kind, pid_s, exp_s, sig = token.split(".")
    expected = _sign(_payload(kind, int(pid_s), int(exp_s)))
    if expected is None or not hmac.compare_digest(sig, expected[:SIG_HEX]):
        return STATUS_INVALID, None, None
    t = now if now is not None else int(time.time())
    if int(exp_s) <= t:
        return STATUS_EXPIRED, kind, int(pid_s)
    return STATUS_OK, kind, int(pid_s)


def inbox_url() -> str:
    base = public_base_url()
    return f"{base}{INBOX_PATH}" if base else ""


def link_url(kind: str, pending_id: int, *, now: int | None = None) -> str:
    """完全な URL（公開ホスト未設定・鍵無効なら ""＝呼び出し側は行を出さない）。"""
    base = public_base_url()
    token = make_token(kind, pending_id, now=now)
    if not base or not token:
        return ""
    return f"{base}{PATH_PREFIX}/{kind}/{token}"


def reply_url(pending_id: int, *, now: int | None = None) -> str:
    return link_url(KIND_REPLY, pending_id, now=now)


def close_url(pending_id: int, *, now: int | None = None) -> str:
    return link_url(KIND_CLOSE, pending_id, now=now)
