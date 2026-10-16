"""fax_adapter — JIKOU-FAX-2: FAX 送信 provider の抽象と DryRunProvider（実在サービスの実装はゼロ）

正本: docs/design/jikou-fax-1.md（JIKOU-FAX-1-DESIGN・§2 候補比較は大野の選定待ち）。QUEUE-2026-10-13-E E-3。
- Provider 契約: `await send(pdf_bytes, to_number, meta) -> 受付 ID（str）`。例外は呼出側（hub/jikou_fax）が
  unconfirmed に倒す。provider は PDF をローカルに保存しない・番号をログに出さない（下 4 桁のみ可）。
- 選択: env JIKOU_FAX_PROVIDER（未設定・"dryrun" → DryRunProvider。それ以外の値は本票では未実装＝UnknownProvider
  を送出して送らない＝fail-closed）。
- DryRunProvider: 外部へ何も送らない。受付 ID は "dryrun:" + PDF sha256 先頭 16 桁（決定的・再送の照合に使える）。
  受付の記録はメモリ上の件数のみ（PDF・番号は保持しない）。
"""

import hashlib
import logging
import os
from typing import Protocol

from hub.redact import emit

logger = logging.getLogger("hub.fax_adapter")

PROVIDER_ENV = "JIKOU_FAX_PROVIDER"
PROVIDER_DRYRUN = "dryrun"
PROVIDERS = (PROVIDER_DRYRUN,)          # 実在サービスは別票（大野の選定後）
ACCEPT_PREFIX_DRYRUN = "dryrun:"


class UnknownProvider(RuntimeError):
    """env の provider 名が閉集合外（送らない）。"""


class FaxProvider(Protocol):
    name: str

    async def send(self, pdf_bytes: bytes, to_number: str, meta: dict) -> str:
        ...


def pdf_sha256(pdf_bytes: bytes) -> str:
    return hashlib.sha256(pdf_bytes or b"").hexdigest()


def last4(number: str) -> str:
    digits = "".join(ch for ch in str(number or "") if ch.isdigit())
    return digits[-4:] if digits else ""


class DryRunProvider:
    """送らない provider。受付 ID は PDF ハッシュから決定的に作る。PDF・番号は保持しない。"""

    name = PROVIDER_DRYRUN

    def __init__(self):
        self.accepted = 0

    async def send(self, pdf_bytes: bytes, to_number: str, meta: dict) -> str:
        sha = pdf_sha256(pdf_bytes)
        self.accepted += 1
        logger.info("[FAX_DRYRUN] accepted last4=%s sha16=%s", emit(last4(to_number), "freetext", "log", "operator"),
                    emit(sha[:16], "freetext", "log", "operator"))
        return f"{ACCEPT_PREFIX_DRYRUN}{sha[:16]}"


def provider_name() -> str:
    return (os.environ.get(PROVIDER_ENV, "") or PROVIDER_DRYRUN).strip().lower()


def get_provider() -> FaxProvider:
    """env で選ぶ（未設定→dryrun・閉集合外→UnknownProvider＝送らない）。"""
    name = provider_name()
    if name == PROVIDER_DRYRUN:
        return DryRunProvider()
    raise UnknownProvider("fax provider not implemented")
