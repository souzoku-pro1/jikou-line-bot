"""
日次死活監視ジョブ

監視項目:
  A. Anthropic Models API (GET /v1/models/{model_id}) で
     PRIMARY_MODEL / FALLBACK_MODEL の有効性を確認する
  B. kintone フォーム設計取得 API で、コードが依存する App 21/28/29 の
     フィールドコード・型・選択肢値が config.EXPECTED_KINTONE_SCHEMA と
     一致するか検証する

異常時のみ LINE Push で管理者に通知する。正常時はログのみ。

実行方式（既存構成 = Railway 単一 Web サービスに合わせる）:
  - main.py の FastAPI startup で start_healthcheck_scheduler() を呼び、
    毎日 HEALTHCHECK_HOUR_JST 時（デフォルト 7 時）にアプリ内で実行する
  - 手動実行 / Railway cron サービス化する場合は
      railway run python daily_healthcheck.py
    （異常があれば exit code 1）
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

import anthropic
import httpx

from claude_gateway import notify_admin_line
from config import EXPECTED_KINTONE_SCHEMA, FALLBACK_MODEL, PRIMARY_MODEL

logger = logging.getLogger("daily_healthcheck")

_JST = timezone(timedelta(hours=9))


# ══════════════════════════════════════════════════════════════
# 監視項目A: Anthropic Models API
# ══════════════════════════════════════════════════════════════

async def check_models() -> list[str]:
    """PRIMARY / FALLBACK モデルの有効性を確認し、問題のリストを返す"""
    problems: list[str] = []
    client = anthropic.AsyncAnthropic()
    for label, model_id in (("PRIMARY", PRIMARY_MODEL), ("FALLBACK", FALLBACK_MODEL)):
        try:
            info = await client.models.retrieve(model_id)
            logger.info("model OK: %s=%s (%s)", label, model_id, info.display_name)
        except anthropic.NotFoundError:
            problems.append(
                f"モデル {label}={model_id} が Models API に存在しません"
                "（廃止された可能性。config.py の更新が必要）"
            )
        except Exception as e:
            problems.append(f"モデル {label}={model_id} の確認に失敗: {str(e)[:150]}")
    return problems


# ══════════════════════════════════════════════════════════════
# 監視項目B: kintone フォーム設計の検証
# ══════════════════════════════════════════════════════════════

async def check_kintone_schema() -> list[str]:
    """App 21/28/29 のフォーム設計を取得し、コードの想定値と照合する"""
    problems: list[str] = []
    sub = os.environ.get("KINTONE_SUBDOMAIN", "").replace(".cybozu.com", "")
    if not sub:
        return ["KINTONE_SUBDOMAIN が未設定です"]

    for app_label, spec in EXPECTED_KINTONE_SCHEMA.items():
        app_id = os.environ.get(spec["app_id_env"], "")
        token = os.environ.get(spec["token_env"], "")
        if not (app_id and token):
            problems.append(
                f"{app_label}: 環境変数 {spec['app_id_env']} / {spec['token_env']} が未設定"
            )
            continue

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://{sub}.cybozu.com/k/v1/app/form/fields.json",
                    headers={"X-Cybozu-API-Token": token},
                    params={"app": app_id},
                )
            if not resp.is_success:
                problems.append(
                    f"{app_label}: フォーム設計取得失敗 "
                    f"{resp.status_code} {resp.text[:100]}"
                )
                continue
            actual_fields = resp.json().get("properties", {})
        except Exception as e:
            problems.append(f"{app_label}: フォーム設計取得エラー: {str(e)[:150]}")
            continue

        for code, expected in spec["fields"].items():
            actual = actual_fields.get(code)
            if actual is None:
                problems.append(
                    f"{app_label}: フィールド「{code}」が存在しません"
                    "（フィールドコードが変更・削除された可能性）"
                )
                continue
            if actual.get("type") != expected["type"]:
                problems.append(
                    f"{app_label}: フィールド「{code}」の型が想定と不一致 "
                    f"(想定={expected['type']} 実際={actual.get('type')})"
                )
                continue
            required = expected.get("required_options")
            if required:
                actual_options = set(actual.get("options", {}).keys())
                missing = [v for v in required if v not in actual_options]
                if missing:
                    problems.append(
                        f"{app_label}: フィールド「{code}」にコードが依存する選択肢 "
                        f"{missing} がありません (実際={sorted(actual_options)})"
                    )
        logger.info("kintone schema checked: %s", app_label)

    return problems


# ══════════════════════════════════════════════════════════════
# 実行本体
# ══════════════════════════════════════════════════════════════

async def run_healthcheck() -> list[str]:
    """全監視項目を実行し、問題リストを返す。異常時のみ LINE 通知。"""
    now = datetime.now(_JST).strftime("%Y-%m-%d %H:%M:%S JST")
    problems: list[str] = []

    try:
        problems += await check_models()
    except Exception as e:
        problems.append(f"モデル監視の実行自体が失敗: {str(e)[:150]}")
    try:
        problems += await check_kintone_schema()
    except Exception as e:
        problems.append(f"kintone監視の実行自体が失敗: {str(e)[:150]}")

    if problems:
        logger.error("healthcheck NG (%d problems): %s", len(problems), problems)
        body = "\n".join(f"・{p}" for p in problems)
        await notify_admin_line(
            "【日次死活監視: 異常検知】\n"
            f"時刻: {now}\n"
            f"{body}\n"
            "対応手順は README「日次死活監視」を参照してください。",
            throttle_key="",  # 日次実行なのでスロットルしない
        )
    else:
        logger.info("healthcheck OK (%s) models=%s/%s", now, PRIMARY_MODEL, FALLBACK_MODEL)
        print(f"[HEALTHCHECK] OK {now} models={PRIMARY_MODEL}/{FALLBACK_MODEL}")

    return problems


# ══════════════════════════════════════════════════════════════
# アプリ内スケジューラ（FastAPI startup から呼ぶ）
# ══════════════════════════════════════════════════════════════

def _seconds_until_next_run(hour_jst: int) -> float:
    now = datetime.now(_JST)
    next_run = now.replace(hour=hour_jst, minute=0, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(days=1)
    return (next_run - now).total_seconds()


async def _scheduler_loop() -> None:
    hour = int(os.environ.get("HEALTHCHECK_HOUR_JST", "7"))
    while True:
        wait = _seconds_until_next_run(hour)
        logger.info("healthcheck scheduled in %.0f sec (daily %02d:00 JST)", wait, hour)
        await asyncio.sleep(wait)
        try:
            await run_healthcheck()
        except Exception:
            logger.exception("healthcheck run failed")


def start_healthcheck_scheduler() -> None:
    """FastAPI startup から呼ぶ。HEALTHCHECK_DISABLED=1 で無効化できる。"""
    if os.environ.get("HEALTHCHECK_DISABLED", "") == "1":
        logger.info("healthcheck scheduler disabled by HEALTHCHECK_DISABLED=1")
        return
    asyncio.get_running_loop().create_task(_scheduler_loop())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = asyncio.run(run_healthcheck())
    raise SystemExit(1 if result else 0)
