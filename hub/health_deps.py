"""health_deps — 依存サービス到達性の軽量確認（RCF-M14 監視拡張・Phase 2）

fix1（P2HC-H01）: **probe 実行と結果参照を分離**する。
- `probe_deps_once()`: Vision probe を実行し、結果を module 内キャッシュへ保存
  （timestamp 付き）。実行主体は内部ジョブ（daily_healthcheck からの結線は**別票**・
  本 module は関数の提供まで）。**公開 endpoint からは呼ばない**。
- `GET /health/deps`: **キャッシュされた直近結果のみ**を返す（外部呼出しゼロ）。
  キャッシュ未生成時は `status: unknown` を 200 で返す。
  ＝公開 GET を課金 API の実行器にしない（denial-of-wallet 経路の遮断）。

RCF-M14 の教訓は維持: 依存障害・タイムアウトでも probe 結果は分類のみ・
healthcheck 系は落とさない。露出禁止も維持: 応答・キャッシュに secret（API key）・
内部 URL・vendor 応答本文・例外本文を含めない（分類はクラス名/HTTP status のみ）。
HEALTH-MIN-1（R-P4-001-1 L01）: **env 名も応答へ出さない**——unconfigured は
固定文字列 status のみ・deps 名は抽象名（vision 等）に限定。
既存 `/health`（起動確認・import チェック）は無変更。
"""

import base64
import os
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter

router = APIRouter()

_TIMEOUT_ENV = "HEALTH_DEPS_TIMEOUT_SECONDS"
_DEFAULT_TIMEOUT = 5.0   # 外部呼出は短め（ジョブを長時間ブロックしない）

# 1x1 白 PNG（コスト最小の annotate probe 用・RCF-M14 §3 実装形 (a)）
_PNG_1PX_B64 = base64.b64encode(bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c626000000000ffff030000060005575ba1ec0000000049454e44ae426082"
)).decode("ascii")

# probe 結果キャッシュ（probe_deps_once のみが書き、GET は読むだけ）
_last_result: dict | None = None


def _timeout() -> float:
    raw = os.environ.get(_TIMEOUT_ENV, "").strip()
    try:
        v = float(raw)
    except ValueError:
        return _DEFAULT_TIMEOUT
    return v if v > 0 else _DEFAULT_TIMEOUT


async def _probe_vision() -> dict:
    """Vision API の到達可否・billing/権限エラーの検知（403 を早期可視化）。
    分類のみ返す: ok / timeout / error(reason=例外クラス名) / error(http_status)。
    例外本文・vendor 応答本文は結果に載せない。"""
    api_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not api_key:
        # HEALTH-MIN-1: 固定文字列のみ（env 名・内部識別子を応答へ出さない）
        return {"status": "unconfigured"}
    payload = {"requests": [{"image": {"content": _PNG_1PX_B64},
                             "features": [{"type": "TEXT_DETECTION", "maxResults": 1}]}]}
    try:
        async with httpx.AsyncClient(timeout=_timeout()) as client:
            resp = await client.post("https://vision.googleapis.com/v1/images:annotate",
                                     params={"key": api_key}, json=payload)
    except httpx.TimeoutException:
        return {"status": "timeout"}
    except Exception as e:  # 接続系含む一般例外も固定分類（クラス名のみ・本文非搭載）
        return {"status": "error", "reason": type(e).__name__}
    if resp.status_code == 200:
        return {"status": "ok"}
    # 403=billing/権限（RCF-M14 本体事象）。本文は載せない（status code のみ）
    return {"status": "error", "http_status": resp.status_code}


_DEPS = {"vision": _probe_vision}


async def probe_deps_once() -> dict:
    """依存 probe を 1 回実行し、結果をキャッシュへ保存して返す。
    呼出し主体は内部ジョブ（daily_healthcheck 結線は別票）・公開 GET からは呼ばない。"""
    global _last_result
    deps = {}
    for name, probe in _DEPS.items():
        try:
            deps[name] = await probe()
        except Exception as e:   # probe 自体の想定外も握る（分類のみ）
            deps[name] = {"status": "error", "reason": type(e).__name__}
    overall = "ok" if all(d.get("status") == "ok" for d in deps.values()) else "degraded"
    _last_result = {"status": overall, "deps": deps,
                    "checked_at": datetime.now(timezone.utc).isoformat()}
    return _last_result


@router.get("/health/deps")
async def health_deps():
    """直近 probe 結果の参照のみ（外部呼出しゼロ・常に HTTP 200）。
    未 probe なら status: unknown。"""
    if _last_result is None:
        return {"status": "unknown", "deps": {}, "checked_at": None}
    return _last_result
