"""
Claude API 呼び出しゲートウェイ（モデルフォールバック＋管理者警報）

全 Claude 呼び出しはこのモジュールの create_message_with_fallback() を経由する。

動作:
  1. config.PRIMARY_MODEL で messages.create を実行
  2. モデル起因エラー（404 / model_not_found / 廃止に伴う400系）を検知したら
     config.FALLBACK_MODEL で1回だけ自動リトライ
  3. フォールバック発動時、LINE Push で管理者に通知（時刻・エラー内容・使用モデル）
  4. フォールバックも失敗した場合は ClaudeUnavailableError を送出
     （呼び出し元が定型の「確認中」応答＋承認キュー登録を行う）

モデル起因でないエラー（429 / 529 / ネットワーク等）はフォールバックせず
そのまま送出する（SDK が自動リトライ済みのため）。

例外: クレジット残高系エラー（credit balance / billing）は、フォールバックせず
管理者に LINE 警報した上で送出する（2026-07-03 の無警報沈黙事象への対策。
同一アカウントのためフォールバックしても解消しない）。
"""

import logging
from datetime import datetime, timedelta, timezone

import anthropic

from config import (
    FALLBACK_EXTRA_PARAMS,
    FALLBACK_MODEL,
    PRIMARY_MODEL,
)

# notify_admin_line は T0-2 で hub/notify.py に移設。
# 既存の import 経路（from claude_gateway import notify_admin_line）互換のため re-export
from hub.notify import notify_admin_line  # noqa: F401

logger = logging.getLogger("claude_gateway")

_JST = timezone(timedelta(hours=9))


class ClaudeUnavailableError(Exception):
    """PRIMARY / FALLBACK の両方で Claude 応答が得られなかった"""


def _is_model_error(exc: Exception) -> bool:
    """モデル起因エラー（廃止・存在しないモデル名）かどうか"""
    if isinstance(exc, anthropic.NotFoundError):
        return True
    if isinstance(exc, anthropic.BadRequestError):
        text = str(exc).lower()
        return "model" in text  # 廃止に伴う 400 系は message にモデル名/modelを含む
    return False


def _is_billing_error(exc: Exception) -> bool:
    """残高・課金起因のエラー（クレジット不足等）かどうか。

    2026-07-03 にクレジット切れが無警報で沈黙する事象が発生。
    この 400 はメッセージに "model" を含まずモデル起因判定に乗らないため、
    フォールバック警報が発動せず、管理者は気づけなかった。
    """
    if isinstance(exc, (anthropic.BadRequestError, anthropic.PermissionDeniedError)):
        text = str(exc).lower()
        return "credit balance" in text or "billing" in text or "purchase credits" in text
    return False


async def _notify_billing_error(context: str, exc: Exception) -> None:
    """クレジット残高系エラーを管理者に LINE Push で警報する（スロットル付き）"""
    logger.error("Anthropic billing error: %s", exc)
    await notify_admin_line(
        "【Anthropicクレジット残高不足・要対応】\n"
        f"時刻: {_now_jst()}\n"
        f"呼び出し元: {context or '不明'}\n"
        f"エラー: {str(exc)[:300]}\n"
        "Claude API が全停止しています（フォールバックモデルも同一アカウントの"
        "ため復旧しません）。console.anthropic.com の Plans & Billing で"
        "クレジットを補充してください。\n"
        "復旧までの間、顧客には定型の「確認中」応答のみが返ります。",
        throttle_key="billing_error",
    )


def _now_jst() -> str:
    return datetime.now(_JST).strftime("%Y-%m-%d %H:%M:%S JST")


async def create_message_with_fallback(
    client: anthropic.AsyncAnthropic,
    *,
    context: str = "",
    **kwargs,
):
    """
    messages.create を PRIMARY_MODEL で実行し、モデル起因エラー時は
    FALLBACK_MODEL で1回だけリトライする。

    Parameters
    ----------
    client  : anthropic.AsyncAnthropic
    context : 通知メッセージに含める呼び出し元の説明（例: "ヒアリング"）
    kwargs  : messages.create に渡す引数（model は指定しないこと）
    """
    try:
        return await client.messages.create(model=PRIMARY_MODEL, **kwargs)
    except anthropic.APIError as primary_exc:
        if _is_billing_error(primary_exc):
            # クレジット不足はフォールバックしても解消しない（同一アカウント）。
            # 管理者に警報した上で従来どおり送出する（呼び出し元の挙動は不変）
            await _notify_billing_error(context, primary_exc)
            raise
        if not _is_model_error(primary_exc):
            raise
        logger.error(
            "PRIMARY model %s failed (%s), falling back to %s",
            PRIMARY_MODEL, primary_exc, FALLBACK_MODEL,
        )
        await notify_admin_line(
            "【Claudeフォールバック発動】\n"
            f"時刻: {_now_jst()}\n"
            f"呼び出し元: {context or '不明'}\n"
            f"失敗モデル: {PRIMARY_MODEL}\n"
            f"エラー: {str(primary_exc)[:300]}\n"
            f"→ {FALLBACK_MODEL} で自動リトライします。\n"
            "config.py の PRIMARY_MODEL 更新を検討してください（README参照）。",
            throttle_key="fallback_activated",
        )
        try:
            return await client.messages.create(
                model=FALLBACK_MODEL, **{**kwargs, **FALLBACK_EXTRA_PARAMS}
            )
        except Exception as fallback_exc:
            logger.exception("FALLBACK model %s also failed", FALLBACK_MODEL)
            await notify_admin_line(
                "【Claude応答不能・要対応】\n"
                f"時刻: {_now_jst()}\n"
                f"呼び出し元: {context or '不明'}\n"
                f"PRIMARY({PRIMARY_MODEL}): {str(primary_exc)[:200]}\n"
                f"FALLBACK({FALLBACK_MODEL}): {str(fallback_exc)[:200]}\n"
                "顧客には定型の「確認中」応答を返し、承認キュー(App 29)に"
                "要対応レコードを作成します。",
                throttle_key="fallback_failed",
            )
            raise ClaudeUnavailableError(
                f"primary={PRIMARY_MODEL}: {primary_exc} / "
                f"fallback={FALLBACK_MODEL}: {fallback_exc}"
            ) from fallback_exc


def extract_text(response) -> str:
    """レスポンスから最初の text ブロックを取り出す。

    （フォールバック先モデルによっては content 先頭が thinking ブロックに
    なり得るため、content[0].text の直接参照はしないこと）
    """
    for block in response.content:
        if block.type == "text":
            return block.text
    raise ValueError(f"no text block in response (stop_reason={response.stop_reason})")
