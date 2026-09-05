"""受信書類写真の永続化 — JIKOU-FORM-3（Part A: LINE 写真の取得+添付／
Part B: 診断フォーム写真の添付・共通層）

IMAGE-INTAKE-1 で保留した「LINE 画像の取得（LINE コンテンツ API）+ kintone FILE
欄への添付」を実装する。App 21（時効）/ App 40（相続放棄）とも「受信書類写真」
（FILE）が CU 済み（form fields API で 2026-09-05 実測）。

設計（票の逐語）:
- LINE コンテンツ API（api-data.line.me）からの取得はチャネル別 access token
  （hub/line_channel.LineChannelConfig.token）で行う。httpx は hub 側のみ
  （houki_bot には置かない=AST checker の閉集合維持）
- 取得はストリーミングで MAX_IMAGE_BYTES を超えた時点で即中断（Content-Length
  事前検査+実読込の二重）。形式はマジックバイトで判定（jpeg/png/pdf/heic）。
  超過・判定不能は添付せず要確認通知（受領返信は止めない=IMAGE-INTAKE-1 規律）
- 添付（attach_files）: hub.kintone.upload_file → fileKey → 対象レコードの現在の
  添付一覧を $revision 込みで取得 → 既存 fileKey を保持して末尾に追記 →
  revision 付き PUT（CAS）→ 409 は再取得・再構成（ATTACH_RETRIES 回）→ 収束不能
  は上書きせず要確認通知。plain 値契約（hub.kintone._wrap が {"value": [...]}
  へ包む。FILE 欄の value は [{"fileKey": ...}, ...] の list）
- レコード未存在（新規客で案件未作成）は添付を保留し、分類ログ「未添付
  （no_record）」のみ残す（後続の紐付け/作成時に拾う仕組みは本票外）
- 画像バイナリ・内容はログ・通知に出さない。通知はレコード番号+固定文言のみ
"""

import logging
import os

import httpx

from hub import kintone
from hub import notify
from hub.redact import emit

logger = logging.getLogger("hub.image_store")

PHOTO_FIELD = "受信書類写真"

# 1 枚あたり上限（実装判断・票報告に明記）: LINE の画像メッセージは最大 10MB
# （Messaging API 仕様）・診断フォームも同値に揃える。kintone の添付上限
# （1 ファイル 1GB）より十分小さい
MAX_IMAGE_BYTES = 10 * 1024 * 1024

# CAS 追記の再試行上限（409 → 再取得・再構成）。form_link.MERGE_RETRIES と同値
ATTACH_RETRIES = 3

_CONTENT_URL = "https://api-data.line.me/v2/bot/message/{message_id}/content"
_FETCH_TIMEOUT_SEC = 30.0

APP_JIKOU_CASE = kintone.KintoneApp(
    "App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")

# ── 形式判定（マジックバイト・閉集合） ──────────────────────────────────────────
# HEIC/HEIF は ISO BMFF の ftyp box（offset 4）+ brand（offset 8）で判定する
_HEIC_BRANDS = frozenset({b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1",
                          b"heim", b"heis"})


def detect_format(data: bytes) -> tuple[str, str] | None:
    """先頭バイトから (拡張子, MIME) を返す。判定不能は None（添付しない）。"""
    head = bytes(data[:12])
    if head[:3] == b"\xff\xd8\xff":
        return "jpg", "image/jpeg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "png", "image/png"
    if head[:5] == b"%PDF-":
        return "pdf", "application/pdf"
    if len(head) >= 12 and head[4:8] == b"ftyp" and head[8:12] in _HEIC_BRANDS:
        return "heic", "image/heic"
    return None


# ── LINE コンテンツ API（ストリーミング取得・上限で即中断） ─────────────────────
class ContentTooLarge(Exception):
    """取得中に MAX_IMAGE_BYTES を超えた（即中断・添付しない）。"""


class ContentFetchError(Exception):
    """LINE コンテンツ API が非 2xx を返した。"""


async def fetch_line_content(channel, message_id: str) -> bytes:
    """LINE コンテンツ API から画像本体を取得する（チャネル別 token）。
    Content-Length が上限超なら本体を読まずに中断・実読込でも上限超で即中断。"""
    url = _CONTENT_URL.format(message_id=message_id)
    headers = {"Authorization": f"Bearer {channel.token()}"}
    buf = bytearray()
    async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT_SEC) as client:
        async with client.stream("GET", url, headers=headers) as resp:
            if not resp.is_success:
                raise ContentFetchError(str(resp.status_code))
            declared = (resp.headers.get("content-length", "") or "").strip()
            if declared.isdigit() and int(declared) > MAX_IMAGE_BYTES:
                raise ContentTooLarge()
            async for chunk in resp.aiter_bytes():
                buf += chunk
                if len(buf) > MAX_IMAGE_BYTES:
                    raise ContentTooLarge()
    return bytes(buf)


# ── FILE 欄への CAS 追記（既存添付を保持） ────────────────────────────────────────
def _v(record: dict, code: str) -> str:
    return str(((record or {}).get(code) or {}).get("value") or "")


def _existing_file_keys(record: dict) -> list[dict]:
    """現在の添付一覧を PUT 用の [{"fileKey": ...}] へ写す（人の追加分も保持）。"""
    files = ((record or {}).get(PHOTO_FIELD) or {}).get("value") or []
    out = []
    for f in files:
        key = (f or {}).get("fileKey") if isinstance(f, dict) else None
        if key:
            out.append({"fileKey": key})
    return out


async def attach_files(app: kintone.KintoneApp, record_id: str,
                       files: list[tuple[str, bytes, str]]) -> str:
    """ファイル群を record_id の受信書類写真へ追記する。戻り値（閉集合）:
    attached=追記成立／unconverged=409 が再試行上限まで続いた（上書きせず中止）／
    failed=upload・取得・書込の確定失敗（書込 0）。
    既存の fileKey を保持したうえで末尾に追記する（人の編集を消さない）。"""
    keys: list[str] = []
    try:
        for name, content, mime in files:
            keys.append(await kintone.upload_file(app, name, content, mime))
    except kintone.KintoneError as e:
        logger.warning("[IMAGE_STORE] upload failed code=%s",
                       emit(e.code, "vendor_raw", "log", "operator"))
        return "failed"
    if not keys:
        return "failed"
    for _attempt in range(ATTACH_RETRIES + 1):
        try:
            latest = await kintone.get_record(app, record_id)
        except kintone.KintoneError as e:
            logger.warning("[IMAGE_STORE] refetch failed code=%s",
                           emit(e.code, "vendor_raw", "log", "operator"))
            return "failed"
        value = _existing_file_keys(latest) + [{"fileKey": k} for k in keys]
        try:
            await kintone.update_record(app, record_id, {PHOTO_FIELD: value},
                                        revision=_v(latest, "$revision"))
            return "attached"
        except kintone.KintoneConflict:
            logger.info("[IMAGE_STORE] attach cas conflict (retry)")
            continue
        except kintone.KintoneError as e:
            logger.warning("[IMAGE_STORE] attach update failed code=%s",
                           emit(e.code, "vendor_raw", "log", "operator"))
            return "failed"
    return "unconverged"


# ── 要確認通知（固定文言+レコード番号のみ・PII/画像内容なし） ────────────────────
_ISSUE_TEXT = {
    "too_large": "サイズ上限（10MB）を超えたため添付していません",
    "unknown_format": "形式を判定できなかったため添付していません",
    "fetch_failed": "LINE からの画像取得に失敗したため添付していません",
    "failed": "kintone への添付に失敗しました（書込なし）",
    "unconverged": "添付の更新競合が収束せず中止しました（上書きなし）",
}


async def _notify_issue(channel_name: str, record_id: str, outcome: str) -> None:
    what = _ISSUE_TEXT.get(outcome, "添付を確定できませんでした")
    try:
        if channel_name == "houki":
            await notify.notify_admin_line(
                "【相続放棄・要確認】書類写真の保存: " + what
                + "。LINE アプリで受信をご確認ください。\n"
                f"レコード番号: {record_id}",
                throttle_key=f"houki_image_attach:{record_id}",
                throttle_on_success_only=True,
            )
        else:
            attorney = os.environ.get("ATTORNEY_LINE_USER_ID", "")
            if attorney:
                await notify.notify_business(
                    attorney,
                    "【要確認】書類写真の保存: " + what
                    + "。LINE アプリで受信をご確認ください。"
                    f"（レコード番号: {record_id}）")
    except Exception:
        logger.error("[IMAGE_STORE] issue notify failed (fixed text)")


# ── LINE 受信画像の取得+添付（受領返信と独立・失敗しても返信は止めない） ─────────
OUTCOMES = frozenset({
    "attached", "no_message_id", "no_record", "fetch_failed", "too_large",
    "unknown_format", "failed", "unconverged"})


async def intake_line_image(channel_name: str, channel, app: kintone.KintoneApp,
                            user_id: str, message_id: str,
                            record_id: str) -> str:
    """LINE 受信画像を取得し、対象レコードの受信書類写真へ添付する。
    戻り値は OUTCOMES の閉集合。attached 以外の不成立は分類ログ（no_record・
    no_message_id は通知なし=保留・それ以外は要確認通知）。例外は外へ出さない。"""
    if not message_id:
        logger.info("[IMAGE_STORE] not attached (no_message_id)")
        return "no_message_id"
    if not record_id:
        logger.info("[IMAGE_STORE] not attached (no_record) userId=%s...",
                    emit(user_id[:10], "record_id", "log", "operator"))
        return "no_record"
    outcome = "failed"
    try:
        try:
            data = await fetch_line_content(channel, message_id)
        except ContentTooLarge:
            outcome = "too_large"
        except Exception:
            outcome = "fetch_failed"
        else:
            fmt = detect_format(data)
            if fmt is None:
                outcome = "unknown_format"
            else:
                ext, mime = fmt
                outcome = await attach_files(
                    app, record_id, [(f"line_{message_id}.{ext}", data, mime)])
    except Exception:
        logger.error("[IMAGE_STORE] intake failed (fixed reason)")
        outcome = "failed"
    _log_outcome(outcome, record_id)
    if outcome != "attached":
        await _notify_issue(channel_name, record_id, outcome)
    return outcome


def _log_outcome(outcome: str, record_id: str) -> None:
    """sink 規律: outcome は閉集合のため分岐で固定文言として出す（可変値は
    emit の直接呼び出しのみ）。"""
    if outcome == "attached":
        logger.info("[IMAGE_STORE] attached record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
    elif outcome == "too_large":
        logger.warning("[IMAGE_STORE] not attached (too_large) record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))
    elif outcome == "unknown_format":
        logger.warning("[IMAGE_STORE] not attached (unknown_format) "
                       "record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))
    elif outcome == "fetch_failed":
        logger.warning("[IMAGE_STORE] not attached (fetch_failed) record_id=%s",
                       emit(record_id, "record_id", "log", "operator"))
    elif outcome == "unconverged":
        logger.error("[IMAGE_STORE] not attached (unconverged) record_id=%s",
                     emit(record_id, "record_id", "log", "operator"))
    else:
        logger.error("[IMAGE_STORE] not attached (failed) record_id=%s",
                     emit(record_id, "record_id", "log", "operator"))
