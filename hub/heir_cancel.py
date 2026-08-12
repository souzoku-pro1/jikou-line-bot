"""heir_cancel — confirmed 済み projection の取消関所（P3-003C-CANCEL 実装票）

正本: `docs/design-drafts/DRAFT_P3_003C_CANCEL.md`（FROZEN・裁定①〜⑧全 RESOLVED）。

- **一本経路**（R1/R3）: 取消の開始は弁護士の明示操作のみ（ATTORNEY_ALLOWLIST・
  裁定④）。機械起点（検知・監査・エラーからの起案）なし・理由も記録しない
  （裁定⑦）。App36 の yes→no 書換え（正本 §3.4 逆遷移禁止の構造化例外）と
  `取消済み=yes` 化は本 module の `_rollback_row` のみが実行する。
- **取消可能条件**（§4.1a・裁定⑥）: 対象 run が head かつ有効 leaf が confirmed
  のみ。held/rejected leaf・decision なし・鎖破損・非 head は取消対象外。
- **2世代分割**（CANCEL-05・裁定⑧）: write-set（projection_log）が存在し
  WRITESET_SCHEMA_VERSION で解釈可能な confirmed のみ自動巻き戻し候補。
  legacy（write-set なし・旧 version）は **App36 write 0・取消台帳のみ追記・
  人手調査**。
- **照合**（§4.1a）: 対象行の現在値が postimage（write-set の fields_written）と
  完全一致する場合のみ自動巻き戻し候補。不一致は write 0 で要確認。
  update 行=preimage 復元／insert 行=無効化（戸籍確認済=no＋取消済み=yes の
  postimage 閉集合・行削除しない・CANCEL-06）。
- **phase 順序**（§4.4）: phase 1=読取専用の全件再検証 → 取消封筒
  （冪等キー heir_cancel:{case}:{run}・状態不問照合で二重取消抑止）→
  phase 2=台帳追記（create_cancel_decision・単一 txn）→ phase 3=App36 巻き戻し
  → 封筒クローズ。**phase 2 より前に App36 へ書かない**。部分失敗は封筒 open
  維持＝再実行時の phase 1 再検証が「取消記録済み・巻き戻し未了」を検出して
  phase 3 のみ再実行（resumed・毎回再照合＝盲目再適用しない）。
- **下流波及**（§4.3・裁定③）: クローズ済み封筒の再オープンはしない。取消は
  新規の取消封筒で記録。成果物の回収は機械はしない（封筒 detail に注記のみ）。
  正しい再確定は再導出→新 run→confirmed（既存原理）。
- flag `HEIR_CANCEL_ENABLED`（既定 OFF・語彙可視性連動＝P3-003-CMD の型）。
- PII 規律: 応答・ログ・封筒 detail は record ID・run id・件数・field code のみ。
"""

import json
import logging
import os

from hub import kintone
from hub.app36_validity import CANCELLED_FIELD, CANCELLED_YES
from hub.derivation_models import (ChainIntegrityError,
                                   DecisionChainCorruptionError,
                                   WRITESET_SCHEMA_VERSION,
                                   create_cancel_decision, get_current_head,
                                   get_leaf_decision, load_write_set)
from hub.heir_envelope import APP_SHIPPING
from hub.heir_projection import (APP_SOUZOKUNIN, _CASE_RECORD_ID_RE,
                                 attorney_allowlist)
from hub.redact import emit

logger = logging.getLogger("hub.heir_cancel")

_FLAG = "HEIR_CANCEL_ENABLED"
TOP_KEY = "heir_cancel"

MSG_CANCEL_DISABLED = "取消は現在無効です（HEIR_CANCEL_ENABLED 未設定）"


def heir_cancel_enabled() -> bool:
    """flag HEIR_CANCEL_ENABLED（既定 OFF・値集合は durable 系と同一流儀）。"""
    return os.environ.get(_FLAG, "").strip().lower() in ("1", "true", "on", "yes")


def cancel_idempotency_key(case_record_id: str, run_id) -> str:
    """二重取消の冪等キー（§4.4・run 単位で取消封筒を一意化）。"""
    return f"heir_cancel:{case_record_id}:{run_id}"


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


async def _alert_business(text: str) -> None:
    """業務チャネル警報（件数・record_id のみ・best-effort）。"""
    from hub import notify
    try:
        await notify.notify_admin_line(text, throttle_key="heir_cancel_alert")
    except Exception:
        logger.error("[HEIR-CANCEL] business alert failed "
                     "(fixed classification only)")


async def _find_cancel_envelope(idem_key: str) -> dict | None:
    """取消封筒の状態不問照合（§4.4・heir_envelope.find_existing 同型）。

    like（escape 済み）→ JSON パース → トップキー heir_cancel かつ冪等キー
    完全一致のみ採用。戻り値 {"record_id", "closed"} または None。"""
    esc = idem_key.replace("\\", "\\\\").replace('"', '\\"')
    records = await kintone.search_records(
        APP_SHIPPING, f'チャネル固有データ like "{esc}"',
        fields=["$id", "チャネル固有データ", "発送ステータス", "実行済み"])
    for rec in records:
        try:
            data = json.loads(_v(rec, "チャネル固有データ"))
        except (ValueError, TypeError):
            continue
        inner = data.get(TOP_KEY) if isinstance(data, dict) else None
        if isinstance(inner, dict) and inner.get("冪等キー") == idem_key:
            return {"record_id": _v(rec, "$id"),
                    "closed": _v(rec, "実行済み") == "yes"}
    return None


def _consolidate_write_set(rows: list[dict]) -> list[dict] | None:
    """write-set の App36 行単位への正規化（§4.1a・単一の正）。

    resumed 再確定により同一行へ複数 log（pending/completed）があり得る——
    - **op / preimage は最初の log**（真実・不変。CANCEL-IMPL-01 の先行保存に
      より preimage は常に初回書込み前の値）。
    - **postimage は全 log の fields_written を id 順に順次適用して合成**
      （CANCEL-IMPL-03。「最後の log で全面置換」は初回のみ書いた field
      〔案件アプリID・ユニット種別等〕が照合から漏れ、第三者変更を
      見逃すため廃止）。
    - app36_record_id 未確定（""）の pending（create の ACK 喪失で completed も
      再確定回収も無い迷子）は対象外＝巻き戻し不能（H11a 監査が最終網・§4.4）。
    schema version が 1 件でも解釈不能なら None（legacy 扱い・CANCEL-05）。"""
    by_rid: dict[str, dict] = {}
    for r in rows:   # id 昇順（load_write_set 契約）
        if r.get("schema_version") != WRITESET_SCHEMA_VERSION:
            return None
        rid = str(r["app36_record_id"])
        if not rid:
            continue
        if rid not in by_rid:
            by_rid[rid] = {"app36_record_id": rid, "op": r["op"],
                           "preimage": dict(r["preimage"]), "postimage": {}}
        by_rid[rid]["postimage"].update(r["fields_written"])   # 順次適用合成
    return list(by_rid.values())


async def _verify_rows(entries: list[dict]) -> tuple[list[dict], list[str]]:
    """三値判定つき照合（§4.1a・CANCEL-IMPL-02・読取専用・毎回実施＝盲目適用
    しない）。行ごとに:

    - **rollback 候補**: 現在値が postimage（順次適用合成）と完全一致。
    - **rollback 済み（完了扱い）**: 現在値が rollback 後の姿と完全一致——
      update 行=preimage の全 field 一致／insert 行=**無効化 postimage 閉集合
      （戸籍確認済=no ＋ 取消済み=yes）の完全一致**（取消済み=yes だけでは
      完了にしない＝無効化後に 戸籍確認済 だけ人手 yes 化された行は要確認へ）。
    - **それ以外＝第三者変更**: write 0 で要確認（機械は上書きしない）。
    完了行は再適用しない（再実行時の App36 再書込み 0・部分失敗の残行のみ回収）。
    取得不能も要確認（fail-closed）。"""
    candidates: list[dict] = []
    mismatched: list[str] = []
    for e in entries:
        rid = e["app36_record_id"]
        try:
            rec = await kintone.get_record(APP_SOUZOKUNIN, rid)
        except kintone.KintoneError:
            mismatched.append(rid)          # 取得不能=要確認（fail-closed）
            continue
        rolled_state = (dict(e["preimage"]) if e["op"] == "update"
                        else {"戸籍確認済": "no", CANCELLED_FIELD: CANCELLED_YES})
        if rolled_state and all(_v(rec, code) == str(val)
                                for code, val in rolled_state.items()):
            continue                        # rollback 済み＝完了扱い（再適用 0）
        if e["op"] == "insert" and _v(rec, CANCELLED_FIELD) == CANCELLED_YES:
            # 無効化マーカーは立つが閉集合が崩れている（例: 無効化後に
            # 戸籍確認済 だけ人手 yes 化）＝第三者変更・要確認（再無効化で
            # 人手変更を上書きしない・CANCEL-IMPL-02）
            mismatched.append(rid)
            continue
        if all(_v(rec, code) == str(val)
               for code, val in e["postimage"].items()):
            candidates.append({**e, "revision": _v(rec, "$revision") or None})
        else:
            # projection 後に人手編集・他 run の更新あり＝機械は上書きしない
            mismatched.append(rid)
    return candidates, mismatched


async def plan_cancel(case_record_id: str, decided_by: str) -> dict:
    """phase 1: 読取専用の全件検証＋復唱材料の収集（R1(iii)(iv)・§4.4）。

    App36/App30/DB への write は一切ない。戻り値 status:
      "plan"     — 取消可能。mode="auto"（巻き戻し候補あり）| "legacy"
                   （裁定⑧: 台帳のみ追記・App36 write 0）| "resumed"
                   （取消記録済み・巻き戻し未了の回収）。
      "aborted" / "disabled" — reason の固定文言（値非搭載）。
    """
    if not heir_cancel_enabled():
        return {"status": "disabled", "reason": MSG_CANCEL_DISABLED}
    if not _CASE_RECORD_ID_RE.fullmatch(case_record_id or ""):
        return {"status": "aborted",
                "reason": "案件レコードIDが数字列ではありません（書き込みなし）"}
    if decided_by not in attorney_allowlist():
        # 裁定④=(A): 確定と同一 allowlist・fail-closed（識別子の値は非表示）
        return {"status": "aborted",
                "reason": "取消権限がありません（ATTORNEY_ALLOWLIST 外・"
                          "書き込みなし）"}

    head = await get_current_head(case_record_id)
    if head is None:
        return {"status": "aborted",
                "reason": "この案件に導出 run がありません（書き込みなし）"}
    # 裁定⑥=(A): head のみ取消可（非 head の誤 projection は要確認=人手）
    try:
        leaf = await get_leaf_decision(head.id)
    except DecisionChainCorruptionError as e:
        await _alert_business(
            "【相続人取消: decision 鎖の破損検出】\n"
            f"案件 No.{case_record_id} / run #{e.run_id} / 有効 leaf {e.count} 件\n"
            "一本鎖でない decision 鎖を検出しました（書き込みなし・人手調査要）")
        return {"status": "aborted",
                "reason": "判断記録の整合が取れません（破損検出・書き込みなし）"}
    idem_key = cancel_idempotency_key(case_record_id, head.id)

    resumed = False
    if leaf is None or leaf.decision == "held":
        return {"status": "aborted",
                "reason": f"run #{head.id} は確定済みではありません"
                          "（取消対象外・書き込みなし）"}
    if leaf.decision == "rejected":
        # 取消記録済み（rejected が confirmed を supersede）か通常否認かは
        # 封筒の状態不問照合で判別する（§4.4: 二重取消は封筒一意化で抑止・
        # 取消済み run への再取消は leaf 再判定で中止）
        env = await _find_cancel_envelope(idem_key)
        if env is not None and not env["closed"]:
            resumed = True   # phase 2 済み・巻き戻し未了（ACK 喪失回収）
        elif env is not None:
            return {"status": "aborted",
                    "reason": f"run #{head.id} は取消済みです（書き込みなし）"}
        else:
            return {"status": "aborted",
                    "reason": f"run #{head.id} は否認済みです"
                              "（取消対象外・書き込みなし）"}

    # write-set の存在・schema version 確認（CANCEL-05・2世代分割）
    ws = await load_write_set(head.id)
    entries = _consolidate_write_set(ws)
    if not entries:
        # legacy confirmed（write-set 欠落・旧 version・解釈不能）: 自動巻き戻し
        # 禁止・App36 write 0・取消台帳のみ追記（裁定⑧）・実機修正は人手調査
        return {"status": "plan", "mode": "legacy", "case_record_id":
                case_record_id, "run_id": head.id, "idem_key": idem_key,
                "candidates": [], "mismatched": [], "record_ids": []}
    candidates, mismatched = await _verify_rows(entries)
    return {"status": "plan",
            "mode": "resumed" if resumed else "auto",
            "case_record_id": case_record_id, "run_id": head.id,
            "idem_key": idem_key, "candidates": candidates,
            "mismatched": mismatched,
            "record_ids": sorted((e["app36_record_id"] for e in entries),
                                 key=lambda x: int(x) if x.isdigit() else 0)}


async def _rollback_row(entry: dict, case_record_id: str) -> str:
    """1 行の巻き戻し（裁定②・一本経路の App36 write はここのみ）。

    - update 行 = preimage 復元（write-set の書込み field を書込み前の値へ）。
    - insert 行 = 無効化（戸籍確認済=no＋取消済み=yes の postimage 閉集合・
      CANCELLED-06。行削除はしない）。yes→no はこの経路のみの設計上の例外。
    - revision 楽観ロック競合＝他プロセスが先に更新＝当該行 held（要確認）。
    """
    fields = (dict(entry["preimage"]) if entry["op"] == "update"
              else {"戸籍確認済": "no", CANCELLED_FIELD: CANCELLED_YES})
    try:
        await kintone.update_record(APP_SOUZOKUNIN, entry["app36_record_id"],
                                    fields, revision=entry.get("revision"))
    except kintone.KintoneConflict:
        await _alert_business(
            "【相続人取消: revision 競合】\n"
            f"案件 No.{case_record_id} / App36 No.{entry['app36_record_id']}\n"
            "他プロセスが先に更新しました。当該行は書き込まず要確認としました")
        return "held"
    return "rolled_back"


async def _file_cancel_envelope(plan: dict) -> str:
    """取消封筒の起票（§4.3 裁定③: 元封筒は再オープンせず新規起票・監査可視性）。"""
    detail = {"冪等キー": plan["idem_key"],
              "case_record_id": plan["case_record_id"],
              "run_id": plan["run_id"], "mode": plan["mode"]}
    rid = await kintone.create_record(APP_SHIPPING, {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "案件レコードID": plan["case_record_id"],
        "実行済み": "no",
        "件名": f"相続人確定の取消: 案件 No.{plan['case_record_id']}"
                f"（run #{plan['run_id']}）",
        "チャネル固有データ": json.dumps({TOP_KEY: detail}, ensure_ascii=False),
    })
    return str(rid)


async def execute_cancel(case_record_id: str, decided_by: str,
                         decided_at) -> dict:
    """取消の実行（復唱 OK 後・§4.4 の phase 順序で完遂する一本経路）。

    再入を含む毎回、phase 1（plan_cancel）を素通りしない（write-set 照合・
    現在値照合を再実施＝盲目再適用しない）。"""
    plan = await plan_cancel(case_record_id, decided_by)
    if plan["status"] != "plan":
        return plan

    run_id = plan["run_id"]
    # ── 取消封筒（状態不問照合で一意化＝二重取消抑止・phase 2 より前だが
    #    App30 のみ=「phase 2 より前に App36 へ書かない」を満たす）──────────────
    env = await _find_cancel_envelope(plan["idem_key"])
    if env is not None and env["closed"]:
        return {"status": "aborted",
                "reason": f"run #{run_id} は取消済みです（書き込みなし）"}
    envelope_id = env["record_id"] if env else await _file_cancel_envelope(plan)

    # ── phase 2: 台帳追記（単一 txn・supersede rejected 型・裁定①）────────────
    try:
        outcome = await create_cancel_decision(
            case_record_id, run_id, decided_by=decided_by,
            decided_at=decided_at)
    except DecisionChainCorruptionError as e:
        await _alert_business(
            "【相続人取消: decision 鎖の破損検出】\n"
            f"案件 No.{case_record_id} / run #{e.run_id} / 有効 leaf {e.count} 件\n"
            "一本鎖でない decision 鎖を検出しました（人手調査要）")
        return {"status": "aborted",
                "reason": "判断記録の整合が取れません（破損検出・App36 への"
                          "書き込みなし）"}
    except ChainIntegrityError:
        return {"status": "aborted",
                "reason": "取消中に前提が変化しました（全体中止・App36 への"
                          "書き込みなし）。再指示してください"}

    # ── legacy（裁定⑧）: App36 write 0・台帳のみ追記して封筒クローズ ──────────
    if plan["mode"] == "legacy":
        await kintone.update_record(APP_SHIPPING, envelope_id, {
            "発送ステータス": "完了", "実行済み": "yes",
            "チャネル固有データ": json.dumps({TOP_KEY: {
                "冪等キー": plan["idem_key"], "case_record_id": case_record_id,
                "run_id": run_id, "mode": "legacy",
                "注記": "write-set なし（legacy confirmed）。App36 は変更して"
                        "いません。実機の修正は人手調査で行ってください"}},
                ensure_ascii=False),
        })
        logger.info("[HEIR-CANCEL] legacy cancelled case=%s run=%s "
                    "(app36 write 0)",
                    emit(case_record_id, "record_id", "log", "operator"),
                    emit(str(run_id), "record_id", "log", "operator"))
        return {"status": "cancelled", "mode": "legacy", "run_id": run_id,
                "envelope_id": envelope_id, "decision_outcome": outcome,
                "rolled_back": 0, "held": len(plan["mismatched"])}

    # ── phase 3: App36 巻き戻し（照合済み候補のみ・行単位継続）────────────────
    rolled = 0
    held: list[str] = list(plan["mismatched"])
    for entry in plan["candidates"]:
        result = await _rollback_row(entry, case_record_id)
        if result == "rolled_back":
            rolled += 1
        else:
            held.append(entry["app36_record_id"])
    closed = not held
    if closed:
        await kintone.update_record(APP_SHIPPING, envelope_id, {
            "発送ステータス": "完了", "実行済み": "yes",
        })
    else:
        # 部分失敗＝封筒 open 維持（reconcile 可視性・resumed 回収の目印）。
        # detail へ要確認 record ID（数字のみ・PII 非搭載）を追記
        await kintone.update_record(APP_SHIPPING, envelope_id, {
            "チャネル固有データ": json.dumps({TOP_KEY: {
                "冪等キー": plan["idem_key"], "case_record_id": case_record_id,
                "run_id": run_id, "mode": plan["mode"],
                "要確認行": sorted(held)}}, ensure_ascii=False),
        })
        await _alert_business(
            "【相続人取消: 巻き戻し未了行あり】\n"
            f"案件 No.{case_record_id} / run #{run_id} / 要確認 {len(held)} 件\n"
            "現在値が projection 結果と不一致の行は書き換えていません"
            "（取消封筒は要確認のまま・現物確認のうえ再指示で回収できます）")
    if plan["mismatched"]:
        logger.info("[HEIR-CANCEL] postimage mismatch rows=%s case=%s run=%s",
                    emit(len(plan["mismatched"]), "count", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"),
                    emit(str(run_id), "record_id", "log", "operator"))
    logger.info("[HEIR-CANCEL] cancelled case=%s run=%s rolled=%s held=%s "
                "closed=%s",
                emit(case_record_id, "record_id", "log", "operator"),
                emit(str(run_id), "record_id", "log", "operator"),
                emit(rolled, "count", "log", "operator"),
                emit(len(held), "count", "log", "operator"),
                emit(1 if closed else 0, "count", "log", "operator"))
    return {"status": "cancelled", "mode": plan["mode"], "run_id": run_id,
            "envelope_id": envelope_id, "decision_outcome": outcome,
            "rolled_back": rolled, "held": len(held),
            "envelope_closed": closed}
