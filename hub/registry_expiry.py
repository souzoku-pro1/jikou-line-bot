"""registry_expiry — HMAC 鍵 registry（SERVICE_HMAC_KEY_REGISTRY）の失効前警告
（REGISTRY-EXPIRY-WARN-1・QUEUE-2026-10-20-J J-1）。

背景: 2026-10-17 00:10 JST のデプロイで registry の全鍵が expires_at 超過となり、
起動時 fail-fast（validate_registry_startup）で本番が 4 日間停止した。アプリ内の日次点検は
「失効後」には動かないため、**失効前**に鍵 ID と残日数だけを通知する。

契約:
- 秘密値（secret）は一切扱わない。通知・戻り値に載るのは key_id・status・残日数のみ。
- 警告閾値 ``REGISTRY_EXPIRY_WARN_DAYS``（既定 14 日・**要大野確認**）。
- 「高」: 有効鍵（status active/retiring かつ未失効）が 0 本、または有効鍵の全部が N 日以内に
  失効 → 毎日通知（run_healthcheck の problems に載せる）。
- 「中」: 1 本でも N 日超の余裕がある鍵が残っていて、N 日以内の鍵もある → 週 1 回
  （JST 月曜のみ problems に載せる）。
- registry が未設定: 署名基盤を使う設定（dual-accept ON または署名必須 path あり）のときだけ「高」。
- registry が JSON/構造不正: 「高」（起動停止・署名経路停止の原因）。
- 例外は呼出側（daily_healthcheck.run_healthcheck）が分類名のみで報告する。
"""

from __future__ import annotations

import datetime
import os
from dataclasses import dataclass

from hub.service_auth import (_REGISTRY_ENV, ServiceAuthConfigError, dual_accept_enabled,
                              parse_registry, signed_required_paths)

WARN_DAYS_ENV = "REGISTRY_EXPIRY_WARN_DAYS"
DEFAULT_WARN_DAYS = 14          # 要大野確認（QUEUE-2026-10-20-J J-1）
WEEKLY_NOTIFY_WEEKDAY = 0       # 「中」を載せる曜日（0=月曜・JST）

STATE_UNCONFIGURED = "unconfigured"
STATE_INVALID = "invalid"
STATE_OK = "ok"
STATE_WARN_ALL = "warn_all"      # 高: 有効鍵 0 本 or 全有効鍵が N 日以内
STATE_WARN_SOME = "warn_some"    # 中: N 日以内の鍵があるが余裕鍵も残る

_JST = datetime.timezone(datetime.timedelta(hours=9))
_EFFECTIVE_STATUSES = ("active", "retiring")


@dataclass(frozen=True)
class KeyExpiry:
    key_id: str
    status: str
    days_left: int          # 負＝失効済み（切り捨て日数）
    effective: bool         # status が active/retiring かつ未失効


@dataclass(frozen=True)
class Report:
    state: str
    keys: tuple[KeyExpiry, ...]
    warn_days: int
    registry_in_use: bool


def warn_days() -> int:
    """閾値（日）。未設定・不正・0 以下は既定値。"""
    raw = os.environ.get(WARN_DAYS_ENV, "").strip()
    try:
        v = int(raw)
    except ValueError:
        return DEFAULT_WARN_DAYS
    return v if v > 0 else DEFAULT_WARN_DAYS


def registry_in_use() -> bool:
    """署名基盤が有効か（起動時検証が走る条件と同じ: dual-accept ON または署名必須 path あり）。"""
    try:
        return dual_accept_enabled() or bool(signed_required_paths())
    except ServiceAuthConfigError:
        return True     # 署名必須 path の設定が壊れている＝基盤を使う意思はある（保守的に True）


def _days_left(expires_at: int, now: datetime.datetime) -> int:
    delta = expires_at - int(now.timestamp())
    return delta // 86400 if delta >= 0 else -((-delta + 86399) // 86400)


def inspect(now: datetime.datetime | None = None) -> Report:
    """registry を読み、鍵ごとの残日数と全体状態を返す（秘密値は含まない）。"""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    n = warn_days()
    in_use = registry_in_use()
    raw = os.environ.get(_REGISTRY_ENV, "").strip()
    if not raw:
        return Report(STATE_UNCONFIGURED, (), n, in_use)
    try:
        reg = parse_registry(raw)
    except ServiceAuthConfigError:
        return Report(STATE_INVALID, (), n, in_use)
    keys = []
    for kid, entry in reg.items():
        dl = _days_left(entry.expires_at, now)
        effective = entry.status in _EFFECTIVE_STATUSES and dl >= 0
        keys.append(KeyExpiry(kid, entry.status, dl, effective))
    keys_t = tuple(sorted(keys, key=lambda k: (k.days_left, k.key_id)))
    effective = [k for k in keys_t if k.effective]
    roomy = [k for k in effective if k.days_left > n]
    near = [k for k in effective if k.days_left <= n]
    if not effective or not roomy:
        state = STATE_WARN_ALL
    elif near:
        state = STATE_WARN_SOME
    else:
        state = STATE_OK
    return Report(state, keys_t, n, in_use)


def _fmt_key(k: KeyExpiry) -> str:
    if k.status not in _EFFECTIVE_STATUSES:
        return f"{k.key_id}（{k.status}）"
    if k.days_left < 0:
        return f"{k.key_id} 失効済み"
    return f"{k.key_id} 残{k.days_left}日"


def healthcheck_problems(now: datetime.datetime | None = None) -> list[str]:
    """daily_healthcheck.run_healthcheck に載せる問題文（鍵 ID・残日数のみ）。
    「高」は毎日・「中」は JST 月曜のみ返す。"""
    now = now or datetime.datetime.now(datetime.timezone.utc)
    rep = inspect(now)
    if rep.state == STATE_UNCONFIGURED:
        if not rep.registry_in_use:
            return []
        return ["【高】HMAC鍵registry（SERVICE_HMAC_KEY_REGISTRY）が未設定: "
                "署名経路の起動時検証が止まります（docs/credentials_expiry.md）"]
    if rep.state == STATE_INVALID:
        return ["【高】HMAC鍵registryがJSON/構造不正: 起動停止・署名経路停止の原因になります"
                "（scripts/registry_tool.py check で確認）"]
    listed = "・".join(_fmt_key(k) for k in rep.keys)
    if rep.state == STATE_WARN_ALL:
        return [f"【高】HMAC鍵registry: 有効鍵の全部が{rep.warn_days}日以内に失効（または失効済み・毎日通知）: "
                f"{listed}。期限延長/rotation=scripts/registry_tool.py extend|rebuild"]
    if rep.state == STATE_WARN_SOME:
        if now.astimezone(_JST).weekday() != WEEKLY_NOTIFY_WEEKDAY:
            return []
        near = "・".join(_fmt_key(k) for k in rep.keys if k.effective and k.days_left <= rep.warn_days)
        return [f"【中】HMAC鍵registry: {near} が{rep.warn_days}日以内に失効（余裕のある鍵あり・週1回通知）"]
    return []
