"""health_deps — 依存サービス到達性の軽量確認（RCF-M14 監視拡張・Phase 2）

- `/health/deps`: Vision API 等の依存サービスへ軽量 probe を行い JSON で返す。
- RCF-M14 の教訓: **healthcheck 自体を落とさない** — 依存障害・タイムアウトでも
  HTTP 200 で `status: degraded` を返す（5xx にしない）。
- 露出禁止: 応答・ログに secret（API key）・内部 URL・vendor 応答本文を含めない
  （daily_healthcheck H02 と同流儀・例外はクラス名のみ、HTTP 失敗は status code のみ）。
- 既存 `/health`（起動確認・import チェック）は無変更（死活監視の互換維持）。
"""

import base64
import os

import httpx
from fastapi import APIRouter

router = APIRouter()

_TIMEOUT_ENV = "HEALTH_DEPS_TIMEOUT_SECONDS"
_DEFAULT_TIMEOUT = 5.0   # 外部呼出は短め（healthcheck を長時間ブロックしない）

# 1x1 白 PNG（コスト最小の annotate probe 用・RCF-M14 §3 実装形 (a)）
_PNG_1PX_B64 = base64.b64encode(bytes.fromhex(
    "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
    "0000000d49444154789c626000000000ffff030000060005575ba1ec0000000049454e44ae426082"
)).decode("ascii")


def _timeout() -> float:
    raw = os.environ.get(_TIMEOUT_ENV, "").strip()
    try:
        v = float(raw)
    except ValueError:
        return _DEFAULT_TIMEOUT
    return v if v > 0 else _DEFAULT_TIMEOUT


async def _probe_vision() -> dict:
    """Vision API の到達可否・billing/権限エラーの検知（403 を早期可視化）。"""
    api_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not api_key:
        return {"status": "unconfigured", "reason": "env GOOGLE_VISION_API_KEY unset"}
    payload = {"requests": [{"image": {"content": _PNG_1PX_B64},
                             "features": [{"type": "TEXT_DETECTION", "maxResults": 1}]}]}
    try:
        async with httpx.AsyncClient(timeout=_timeout()) as client:
            resp = await client.post("https://vision.googleapis.com/v1/images:annotate",
                                     params={"key": api_key}, json=payload)
    except httpx.TimeoutException:
        return {"status": "timeout"}
    except Exception as e:  # 到達不能等も degraded 側へ（healthcheck は落とさない）
        return {"status": "error", "reason": type(e).__name__}
    if resp.status_code == 200:
        return {"status": "ok"}
    # 403=billing/権限（RCF-M14 本体事象）。本文は載せない（status code のみ）
    return {"status": "error", "http_status": resp.status_code}


_DEPS = {"vision": _probe_vision}


@router.get("/health/deps")
async def health_deps():
    """依存サービスの到達可否。失敗しても HTTP 200 で status: degraded を返す。"""
    deps = {}
    for name, probe in _DEPS.items():
        try:
            deps[name] = await probe()
        except Exception as e:   # probe 自体の想定外も握る（healthcheck 無傷）
            deps[name] = {"status": "error", "reason": type(e).__name__}
    overall = "ok" if all(d.get("status") == "ok" for d in deps.values()) else "degraded"
    return {"status": overall, "deps": deps}
