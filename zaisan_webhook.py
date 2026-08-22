"""財産目録（相続）自動生成 Webhook — ZAISAN-GEN-1

CONTRACT-GEN で確立した構造の同型（App26=相談カード（相続）に載せる）:

状態機械（CAS＝$revision 楽観ロック・閉集合 4 値）:
  財産目録作成 --（CAS 勝者: 作成→作成中)--> 財産目録作成中
    --（生成+検証+upload+添付 PUT〔revision=claim+1〕）--> 財産目録作成済
  財産目録作成(前提未充足)  → 変更なし（固定文言のみ通知・作用は通知だけ）
  財産目録作成中 + 添付なし → 回収: CAS 再claim → 生成/添付 → 作成済
  財産目録作成中 + 添付あり → 自動上書きせず CAS で「要確認」+管理者通知
  財産目録作成済            → already_done skip（再配送の冪等化）
  要確認/空/他値            → stale_status skip（正本の完全一致検証）
  CAS 敗者（409）           → 作用 0 で skip（fix2 流儀: 409 のみ cas_lost・
                              障害系は 500 → kintone 再配送）

入口ガード: 本文 app.id が App26 実 ID と完全一致・本文ステータスが
「財産目録作成」のときのみ通過（自 update の echo はここで落ちる）。

fail-closed（前提未充足＝状態不変・値は通知に載せない）:
  - App 財産に有効な財産行が 0 件 → 生成拒否・明示
  - 生成 xlsx はサーバ側検証（verify_zaisan_xlsx: 金額セル int・小計/総合計
    再計算一致・行数整合・セル式不在・下書き表示の閉集合照合）を通してから
    添付

ZAISAN-GEN-2（大野裁定）: 評価確定=yes の全件要求は生成条件から外し、
未確定行があれば「下書き」（バナー+行注記+暫定表示つき）として生成する。
生成通知に「下書き（未確定 N 件）」か「完成版」かを明記する。
"""

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import kintone as hub_kintone
from hub.redact import emit
from hub.webhook_auth import extract_record_id, verify_token
from units.souzoku.zaisan_mokuroku import (
    ZaisanMokurokuError, fetch_zaisan_records)
from units.souzoku.zaisan_xlsx import (
    build_zaisan_xlsx, count_unconfirmed, verify_zaisan_xlsx)

logger = logging.getLogger("zaisan")

_APP = hub_kintone.KintoneApp(
    "相談カード (相続)", "SOUZOKU_KINTONE_APP_ID", "SOUZOKU_KINTONE_API_TOKEN")

FIELD_STATUS     = "財産目録ステータス"
STATUS_TRIGGER   = "財産目録作成"
STATUS_WORKING   = "財産目録作成中"
STATUS_DONE      = "財産目録作成済"
STATUS_REVIEW    = "要確認"
FIELD_ATTACHMENT = "財産目録"
FIELD_DECEDENT   = "被相続人名"
OUTPUT_FILENAME  = "財産目録.xlsx"
_XLSX_MIME = ("application/vnd.openxmlformats-officedocument"
              ".spreadsheetml.sheet")

router = APIRouter()


def _fv(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


async def _notify(text: str) -> None:
    """管理者 LINE 通知（best-effort・固定文言+レコード番号/件数のみ）。"""
    try:
        from hub.notify import notify_admin_line
        await notify_admin_line(text)
    except Exception:
        logger.error("[ZAISAN] admin notify failed (fixed text)")


async def _generate_and_attach(record_id: str, record: dict,
                               final_revision: str) -> None:
    """取得 → 生成 → 検証 → upload → 添付+作成済（revision CAS つき PUT）。"""
    records = await fetch_zaisan_records(str(_APP.app_id()), record_id)
    xlsx_bytes = build_zaisan_xlsx(
        records, decedent_name=_fv(record, FIELD_DECEDENT) or None)
    verify_zaisan_xlsx(xlsx_bytes, records)
    logger.info("[ZAISAN] generated record_id=%s bytes=%d rows=%d",
                emit(record_id, "record_id", "log", "operator"),
                len(xlsx_bytes), len(records))
    file_key = await hub_kintone.upload_file(
        _APP, OUTPUT_FILENAME, xlsx_bytes, _XLSX_MIME)
    await hub_kintone.update_record(_APP, record_id, {
        FIELD_ATTACHMENT: [{"fileKey": file_key}],
        FIELD_STATUS: STATUS_DONE,
    }, revision=final_revision)
    logger.info("[ZAISAN] attached record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    # ZAISAN-GEN-2: 下書き/完成版の別を明記して通知（件数のみ・PII 非搭載）
    unconfirmed = count_unconfirmed(records)
    if unconfirmed:
        await _notify(
            f"【財産目録】案件 No.{record_id} の財産目録を下書き"
            f"（評価未確定 {unconfirmed} 件）として生成・添付しました。"
            "評価確定の入力後、添付を削除してステータスを"
            f"「{STATUS_TRIGGER}」に設定し直すと完成版を再生成できます")
    else:
        await _notify(
            f"【財産目録】案件 No.{record_id} の財産目録を完成版として"
            "生成・添付しました")


async def _claim(record_id: str, revision: str, to_status: str) -> str | None:
    """CAS（fix2 流儀）: 409 競合のみ cas_lost（None）。障害系は再送出し
    外側で 500 → kintone 再配送へ。"""
    try:
        await hub_kintone.update_record(
            _APP, record_id, {FIELD_STATUS: to_status}, revision=revision)
    except hub_kintone.KintoneError as e:
        if getattr(e, "status", None) == 409:
            return None
        raise
    return str(int(revision) + 1)


async def _reconcile_working(record_id: str, record: dict, revision: str):
    """「作成中」で停止した行の回収（contract_webhook と同規則）。"""
    attachment = (record.get(FIELD_ATTACHMENT) or {}).get("value") or []
    if attachment:
        next_rev = await _claim(record_id, revision, STATUS_REVIEW)
        if next_rev is None:
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "cas_lost"})
        await _notify(
            f"【財産目録】案件 No.{record_id} は生成が中断した状態で既に"
            "添付ファイルが存在するため、自動では上書きせず"
            f"「{STATUS_REVIEW}」にしました。添付内容を確認のうえ、再生成する"
            f"場合は添付を削除してステータスを「{STATUS_TRIGGER}」に設定して"
            "ください")
        return JSONResponse(status_code=200, content={
            "ok": True, "skip": "needs_review"})
    next_rev = await _claim(record_id, revision, STATUS_WORKING)
    if next_rev is None:
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "cas_lost"})
    await _generate_and_attach(record_id, record, next_rev)
    return JSONResponse(status_code=200, content={
        "ok": True, "record_id": record_id, "recovered": True})


@router.post("/zaisan/{secret}")
async def zaisan_webhook(secret: str, request: Request):
    if not verify_token(secret or "", "DOCUMENT_WEBHOOK_SECRET"):
        return JSONResponse(status_code=403, content={"error": "forbidden"})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})

    app_in_body = str(((body.get("app") or {}) if isinstance(body, dict)
                       else {}).get("id") or "")
    if not app_in_body.isdigit() or app_in_body != str(_APP.app_id()):
        logger.warning("[ZAISAN] app mismatch in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "app_mismatch"})

    record_id = extract_record_id(body)
    if not record_id:
        logger.warning("[ZAISAN] record id missing in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "no_record_id"})

    try:
        status_in_webhook = body["record"][FIELD_STATUS]["value"]
    except (KeyError, TypeError):
        status_in_webhook = None
    if status_in_webhook != STATUS_TRIGGER:
        logger.info("[ZAISAN] not triggered record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "not_triggered"})

    try:
        record = await hub_kintone.get_record(_APP, record_id)
        current = _fv(record, FIELD_STATUS)
        revision = _fv(record, "$revision")

        if current == STATUS_DONE:
            logger.info("[ZAISAN] already done record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "already_done"})
        if current == STATUS_WORKING:
            return await _reconcile_working(record_id, record, revision)
        if current != STATUS_TRIGGER or not revision.isdigit():
            logger.info("[ZAISAN] stale status record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "stale_status"})

        # fail-closed: 財産行 0 件は生成しない（状態も動かさない）。
        # ZAISAN-GEN-2 裁定: 評価未確定は拒否せず下書きとして生成する。
        # ガード文言はレコード番号・件数のみ（PII 非搭載）。生成そのものは
        # CAS 勝者のみが行う（下の _generate_and_attach）
        try:
            records = await fetch_zaisan_records(
                str(_APP.app_id()), record_id)
            if not records:
                raise ZaisanMokurokuError(
                    "財産行が0件です（App 財産に案件の財産が登録されて"
                    "いません）。")
        except ZaisanMokurokuError as e:
            logger.info("[ZAISAN] not ready record_id=%s cls=%s",
                        emit(record_id, "record_id", "log", "operator"),
                        type(e).__name__)
            await _notify(
                f"【財産目録】案件 No.{record_id} は生成できません。{e} "
                f"解消後、財産目録ステータスを「{STATUS_TRIGGER}」に設定し直して"
                "ください")
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "not_ready"})

        next_rev = await _claim(record_id, revision, STATUS_WORKING)
        if next_rev is None:
            logger.info("[ZAISAN] cas lost record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "cas_lost"})
        await _generate_and_attach(record_id, record, next_rev)
    except Exception as e:
        logger.error("[ZAISAN] error record_id=%s cls=%s: %s",
                     emit(record_id, "record_id", "log", "operator"),
                     type(e).__name__,
                     emit(str(e), "vendor_raw", "log", "operator"))
        return JSONResponse(status_code=500,
                            content={"error": "internal_error"})

    return JSONResponse(status_code=200,
                        content={"ok": True, "record_id": record_id})
