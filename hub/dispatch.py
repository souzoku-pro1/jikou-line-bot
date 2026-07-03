"""発送管理（App 30）Webhook の受け口とチャネルディスパッチャ（hub/dispatch）

設計: docs/architecture/03-common-components.md §5.3

POST /hub/dispatch?token=<HUB_WEBHOOK_TOKEN>
  1. 合言葉検証（NG は 404 = 存在しないフリ・既存流儀）
  2. recordId 抽出 → BackgroundTasks で最新レコードを取り直して処理（即 200）
  3. 発送ステータスで分岐:
     - 下書き:   adapter.prepare → 成果物添付 → 承認待ちへ + 弁護士 LINE 通知
     - 承認済:   claim（冪等）→ 発送処理中 → adapter.dispatch → 発送済/返送待ち
                 （manual_mailing チャネルは発送処理中で停止し印刷指示を通知）
     - 要確認:   adapter.reprocess
     - その他:   skip（kintone Webhook はリトライしないため常に 200）

このモジュールは 発送ステータス を直接書かない（遷移は hub/approval.transition のみ）。
"""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

import channels
from hub import approval, kintone, notify
from hub.webhook_auth import extract_record_id, verify_token

logger = logging.getLogger("hub.dispatch")

router = APIRouter()

APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")


@router.post("/hub/dispatch")
async def hub_dispatch(request: Request, background_tasks: BackgroundTasks):
    token = request.query_params.get("token", "")
    if not verify_token(token, "HUB_WEBHOOK_TOKEN"):
        raise HTTPException(status_code=404, detail="not found")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid json")

    record_id = extract_record_id(body)
    if not record_id:
        return {"ok": True, "skip": "no_record_id"}

    background_tasks.add_task(process_dispatch, record_id)
    return {"ok": True, "queued": record_id}


async def process_dispatch(record_id: str) -> None:
    """最新レコードを取り直し、発送ステータスに応じて処理する（Webhook 二重配信に安全）"""
    try:
        try:
            record = await kintone.get_record(APP_SHIPPING, record_id)
        except kintone.KintoneError as e:
            logger.error("record fetch failed record=%s: %s", record_id, e)
            return

        status = record.get("発送ステータス", {}).get("value", "")
        if status == "下書き":
            await _handle_prepare(record)
        elif status == "承認済":
            await _handle_dispatch(record)
        elif status == "発送済":
            await _handle_shipped(record)
        elif status == "要確認":
            await _handle_reprocess(record)
        else:
            logger.info("skip record=%s status=%r (server 処理対象外)", record_id, status)
    except approval.TransitionError:
        pass  # transition() 内で警報済み
    except Exception as e:
        logger.exception("process_dispatch failed record=%s", record_id)
        await notify.notify_admin_line(
            "【発送管理: 処理エラー】\n"
            f"レコードNo: {record_id}\n"
            f"エラー: {str(e)[:300]}",
            throttle_key="hub_dispatch_error",
        )


def _rid(record: dict) -> str:
    return str(record["$id"]["value"])


def _summary(record: dict) -> str:
    return (f"件名: {record.get('件名', {}).get('value', '')} / "
            f"チャネル: {record.get('チャネル', {}).get('value', '')} / "
            f"顧客: {record.get('顧客名表示用', {}).get('value', '')}")


async def _adapter_for(record: dict):
    """チャネル値からアダプタを解決。未登録は警報のみ（状態は変えない=承認可能性を保全）"""
    channel = record.get("チャネル", {}).get("value", "")
    adapter = channels.get_adapter(channel)
    if adapter is None:
        await notify.notify_admin_line(
            "【発送管理: 未対応チャネル】\n"
            f"レコードNo: {_rid(record)} / チャネル: {channel!r}\n"
            "このチャネルのアダプタが未実装です（レコードは変更していません）。",
            throttle_key=f"no_adapter:{channel}",
        )
    return adapter


async def _to_error(record: dict, from_status: str, detail: str) -> None:
    record_id = _rid(record)
    await approval.transition(
        APP_SHIPPING, record_id, from_status, "エラー",
        extra_fields={"エラー詳細": detail[:1000]},
    )
    await notify.notify_admin_line(
        "【発送管理: エラー】\n"
        f"レコードNo: {record_id}\n{_summary(record)}\n"
        f"エラー: {detail[:300]}",
        throttle_key="",
    )


async def _handle_prepare(record: dict) -> None:
    """下書き → prepare（成果物生成・添付）→ 承認待ち + 弁護士通知"""
    adapter = await _adapter_for(record)
    if adapter is None:
        return
    record_id = _rid(record)
    try:
        result = await adapter.prepare(record)
        extra = dict(result.fields)
        if result.artifacts:
            file_keys = []
            for a in result.artifacts:
                file_keys.append(await kintone.upload_file(
                    APP_SHIPPING, a.filename, a.content, a.mime))
            extra["成果物"] = [{"fileKey": k} for k in file_keys]
    except channels.base.PrepareDeferred as e:
        # エラーではない中断（マスタ登録待ち等）: 状態を変えず登録依頼の警報のみ
        logger.info("prepare deferred record=%s: %s", record_id, e)
        await notify.notify_admin_line(
            "【発送管理: 対応依頼（エラーではありません）】\n"
            f"レコードNo: {record_id}\n{_summary(record)}\n"
            f"{e}\n"
            "対応後、このレコードを（下書きのまま）再保存すると自動で再処理されます。",
            throttle_key=f"prepare_deferred:{record_id}",
        )
        return
    except Exception as e:
        logger.exception("prepare failed record=%s", record_id)
        await _to_error(record, "下書き", f"prepare 失敗: {e}")
        return

    await approval.transition(APP_SHIPPING, record_id, "下書き", "承認待ち", extra)
    await notify.notify_attorney_approval(record)


async def _handle_dispatch(record: dict) -> None:
    """承認済 → claim（冪等）→ 発送処理中 → dispatch → 発送済（/返送待ち）"""
    adapter = await _adapter_for(record)
    if adapter is None:
        return
    record_id = _rid(record)

    if not await approval.claim_execution(APP_SHIPPING, record):
        logger.info("skip record=%s (already executed / claim conflict)", record_id)
        return

    await approval.transition(APP_SHIPPING, record_id, "承認済", "発送処理中")

    try:
        result = await adapter.dispatch(record)
    except Exception as e:
        logger.exception("dispatch failed record=%s", record_id)
        await _to_error(record, "発送処理中", f"dispatch 失敗: {e}")
        return

    extra = dict(result.fields)
    if result.manual_mailing:
        # 物理郵送: 発送処理中のまま停止（印刷・投函・発送済への変更は人）
        if extra:
            await kintone.update_record(APP_SHIPPING, record_id, extra)
        await notify.notify_admin_line(
            "【発送管理: 印刷・投函をお願いします】\n"
            f"レコードNo: {record_id}\n{_summary(record)}\n"
            "kintone の成果物を印刷・封入し、投函後に発送ステータスを「発送済」に変更してください。",
            throttle_key="",
        )
        return

    extra["発送日時"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    await approval.transition(APP_SHIPPING, record_id, "発送処理中", "発送済", extra)
    if adapter.needs_return:
        # 返送期限を自動設定（T1-4。日数はユニット設定・監視は return_deadline_check）
        from hub.return_deadline import compute_deadline
        unit = record.get("ユニット種別", {}).get("value", "")
        await approval.transition(
            APP_SHIPPING, record_id, "発送済", "返送待ち",
            extra_fields={"返送期限": compute_deadline(unit)},
        )


async def _handle_shipped(record: dict) -> None:
    """発送済（物理郵送チャネルでは人が投函後に設定→Webhook 再発火）→ 返送待ち/完了（T3-3）

    返送要否の判定（優先順）:
      1. アダプタの needs_return（チャネル全体の性質。M1 職務上請求 = True）
      2. チャネル固有データ の needs_return フラグ（レコード単位。M4 送付案内が
         prepare 時に「返送要否=要の同封物があるか」を記録する・T2-2 実装ノート参照）
    - 返送あり → 返送待ち へ遷移し 返送期限 を自動設定（発送日=今日 + ユニット既定日数。
      compute_deadline: UNIT_CONFIG.return_deadline_days・既定21日）
    - 返送なし → 完了 へ遷移（SERVER_TRANSITIONS「発送済→完了 返送想定なし」）

    ── M5 受領パイプライン（将来の T4系）との接続点 ─────────────────────────
    返送待ちの消込（返送待ち→完了）は**ここでは行わない**。スキャン受領（M5）が
    受領文書を発送管理レコードへ突合して 完了 に遷移させる（設計 08 §3・04 §4）。
    突合できず人の判断が要る場合、M5 は 要確認 に置き、_handle_reprocess が受ける。
    期限超過の監視は return_deadline_check（T1-4・毎日 8:00 JST）が「返送待ち」を
    チャネル横断で拾う（このチャネルのレコードも自動的に対象になる）。
    """
    adapter = await _adapter_for(record)
    if adapter is None:
        return
    record_id = _rid(record)

    needs_return = adapter.needs_return
    if not needs_return:
        try:
            data = json.loads(record.get("チャネル固有データ", {}).get("value") or "{}")
            needs_return = bool(data.get("needs_return"))
        except ValueError:
            needs_return = False  # prepare 通過済みで JSON が壊れているのは想定外。
                                  # フラグ不明は「フラグなし」と同じ扱い（返送なし→完了）

    if needs_return:
        from hub.return_deadline import compute_deadline
        unit = record.get("ユニット種別", {}).get("value", "")
        await approval.transition(
            APP_SHIPPING, record_id, "発送済", "返送待ち",
            extra_fields={"返送期限": compute_deadline(unit)},
        )
        logger.info("shipped record=%s -> 返送待ち (needs_return)", record_id)
    else:
        await approval.transition(APP_SHIPPING, record_id, "発送済", "完了")
        logger.info("shipped record=%s -> 完了 (返送想定なし)", record_id)


async def _handle_reprocess(record: dict) -> None:
    """要確認 → adapter.reprocess（M5 用・実装がなければ skip）"""
    adapter = await _adapter_for(record)
    if adapter is None:
        return
    await adapter.reprocess(record)
