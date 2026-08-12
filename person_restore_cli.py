"""person_restore_cli — 過去に物理削除された人物レコードの復元 tool（手動 CLI）

正本: DRAFT_RV08_SOFT_MERGE.md §3.2・裁定④=(A) 手動 CLI（R2）＋
R-RV08-IMPL-1 反映（RV08-IMPL-01/02: 決定的 operation_id・冪等復元・fail-closed）。

- 対象は **soft merge 移行前に物理削除された敗者**（封筒添付の監査JSONが原資）。
  soft merge 後の無効化行の復元（unmerge）は関所型の[人]操作でありスコープ外。
- kintone 仕様上、旧レコード番号での復元は不可能＝**新 ID での復元＋lineage 記録**。
- **決定的 restore operation_id（RV08-IMPL-01）**: 材料は閉集合4値
  （監査種別・封筒レコードID・旧敗者ID・元 merge operation_id〔旧監査は空〕）のみ
  から SHA-256 で決定的に構成する（uuid 不使用）。同一監査JSONは常に同一
  operation_id ＝連続 --execute・ACK 喪失後の再実行でも App34 create は合計 1 回。
- **復元操作ID の本体保存（RV08-IMPL-05・裁定=専用フィールド方式）**: create の
  payload に App34「復元操作ID」（SINGLE_LINE_TEXT・非PII=SHA-256 hex）として
  operation_id を保存する。ACK 喪失後の回収は
  `復元操作ID = "<operation_id>"` の完全一致検索＝**個体の決定的同定**
  （氏名検索は廃止——同氏名・同内容の既存人物への誤採用を構造で排除）。
  三値固定: 0件=create 続行／1件=採用（fingerprint 再検証つき）／
  複数件=write 0＋要確認（構造上起きないはずだが fail-closed）。
- **fail-closed＋段階回収（RV08-IMPL-02）**: create 前に preimage（pending）行を
  immutable 台帳へ先行保存する。台帳が使えない（DB 不可・migration 未適用）なら
  App34 へ一切書かない。create 後の各失敗点（ACK 喪失・親エッジ途中失敗・
  完了記録失敗）は再実行で**既存復元へ収束**する:
    完了行あり → 現況 fingerprint 一致なら create 0 で既存新 ID を返す・不一致は
    write 0 要確認 ／ pending のみ → 復元操作ID 検索（上記三値）／
    親エッジは現在値で per-edge 照合
    （勝者のまま=適用・新 ID=skip・第三者変更=write 0 で要人手）。
- 既定は **dry-run**（kintone 読み取りのみで計画を表示・台帳照合は --execute 時）。
  PERSON_MERGE_ENABLED には依存しない（手動起動自体が明示承認）。
- 出力は record ID・件数・field code・固定文言のみ（復元 payload の値・氏名等の
  PII を stdout に出さない——negative テストで固定）。

使い方:
    python person_restore_cli.py 監査ファイル.json            # dry-run
    python person_restore_cli.py 監査ファイル.json --execute  # 実書き込み
"""

import argparse
import asyncio
import hashlib
import json
import sys

from hub import kintone
from hub.person_merge_journal import (
    STAGE_PREIMAGE,
    STAGE_RESTORE,
    MergeJournalError,
    find_stages,
    fingerprint_with_updates,
    record_fingerprint,
    record_stage,
)
from hub.person_validity import RESTORE_OPERATION_FIELD
from person_merge import APP_KOSEKI_PERSON, _v
from person_merge_exec import (
    PARENT_EDGE_FIELDS,
    restore_payload_from_audit,
)


def restore_operation_id(audit: dict) -> str:
    """決定的 operation_id（RV08-IMPL-01）。材料は**閉集合4値のみ**:
    監査種別・封筒レコードID・旧敗者ID・元 merge operation_id（旧監査は空）。
    材料の追加/変更は本 CLI の設計改定と同時のみ（冪等キーの意味が変わるため）。"""
    material = [str(audit.get("監査種別") or ""),
                str(audit.get("封筒レコードID") or ""),
                str(audit.get("削除レコードID") or ""),
                str(audit.get("operation_id") or "")]
    digest = hashlib.sha256(
        json.dumps(material, ensure_ascii=False).encode("utf-8")).hexdigest()
    return f"restore-{digest}"


def _payload_fp(payload: dict) -> str:
    """復元 payload の fingerprint（空レコードへの仮適用形＝正規形）。"""
    return fingerprint_with_updates({}, payload)


def _subset_fp(record: dict, payload: dict) -> str:
    """既存レコードの payload 対象フィールドのみの fingerprint（現況照合用）。"""
    sub = {}
    for code, value in payload.items():
        cell = record.get(code)
        if not isinstance(cell, dict):
            cell = {"type": "SUBTABLE" if isinstance(value, list)
                    else "SINGLE_LINE_TEXT",
                    "value": [] if isinstance(value, list) else ""}
        sub[code] = cell
    return record_fingerprint(sub)


def _load_audit(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        audit = json.load(f)
    if audit.get("監査種別") != "person_merge":
        raise SystemExit("監査種別が person_merge ではありません（中止）")
    if audit.get("統合方式") == "soft_merge":
        raise SystemExit(
            "この監査JSONは soft merge（無効化・残置）の記録です。復元 CLI の"
            "対象は物理削除時代の監査のみ——無効化行の復元（unmerge）は"
            "[人]の関所型操作で行ってください（中止）")
    if not (audit.get("敗者レコード") or {}):
        raise SystemExit("監査JSONに 敗者レコード がありません（中止）")
    return audit


def _escape(value: str) -> str:
    return (value or "").replace('"', '\\"')


async def _classify_relink(plans: list, winner_id: str,
                           new_id: str | None) -> tuple[list, list]:
    """親エッジの per-edge 照合（盲目上書き禁止・§3.2a と同じ規律）。
    Returns: (適用すべき (record_id, fields) 一覧, スキップ注記〔ID/固定文言のみ〕)"""
    todo: list[tuple[str, list[str]]] = []
    notes: list[str] = []
    for plan in plans:
        rid = str(plan.get("person_record_id") or "")
        fields = [str(f) for f in plan.get("fields") or []
                  if f in PARENT_EDGE_FIELDS]
        if not (rid and fields):
            continue
        try:
            row = await kintone.get_record(APP_KOSEKI_PERSON, rid)
        except kintone.KintoneError:
            notes.append(f"No.{rid}（取得不能・要人手確認）")
            continue
        apply_fields = []
        for f in fields:
            current = _v(row, f)
            if current == winner_id:
                apply_fields.append(f)          # 未適用（当時勝者のまま）
            elif new_id is not None and current == new_id:
                continue                        # 適用済み（再実行の skip）
            else:
                notes.append(f"No.{rid}:{f}（第三者変更あり・要人手確認）")
        if apply_fields:
            todo.append((rid, apply_fields))
    return todo, notes


async def _find_restored_candidate(payload: dict, payload_fp: str,
                                   op_id: str) -> str | None:
    """ACK 喪失後の作成済みレコード探索（RV08-IMPL-05・専用フィールド方式）。
    `復元操作ID = "<operation_id>"` の完全一致検索＝個体の決定的同定。三値固定:
      0件 → None（create 続行＝回収規則どおり）
      1件 → 採用（payload fingerprint の再検証つき・不一致は要確認 write 0）
      複数件 → write 0＋要確認（構造上起きないはずだが fail-closed）
    同氏名・同内容の既存人物（復元操作ID なし）は検索に載らない＝誤採用しない。"""
    records = await kintone.search_records(
        APP_KOSEKI_PERSON,
        f'{RESTORE_OPERATION_FIELD} = "{_escape(op_id)}"'
        ' order by $id asc limit 10',
        fields=["$id"])
    if not records:
        return None
    if len(records) >= 2:
        ids = "・".join(f"No.{_v(r, '$id')}" for r in records)
        raise SystemExit(
            f"復元操作ID が複数レコードに存在します（{ids}・構造上想定外・"
            "要人手確認・書き込みなし）")
    rid = _v(records[0], "$id")
    try:
        rec = await kintone.get_record(APP_KOSEKI_PERSON, rid)
    except kintone.KintoneError:
        raise SystemExit(
            f"復元操作ID 一致の No.{rid} を取得できません"
            "（要人手確認・書き込みなし）")
    if _subset_fp(rec, payload) != payload_fp:
        raise SystemExit(
            f"復元操作ID 一致の No.{rid} は保存内容と一致しません"
            "（その後の編集あり・要人手確認・書き込みなし）")
    return rid


async def _restore(audit: dict, execute: bool) -> None:
    old_id = str(audit.get("削除レコードID") or "")
    winner_id = str(audit.get("統合先レコードID") or "")
    if not (APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        raise SystemExit("APP_KOSEKI_PERSON / TOKEN_KOSEKI_PERSON 未設定（中止）")

    op_id = restore_operation_id(audit)
    payload = restore_payload_from_audit(audit)
    payload_fp = _payload_fp(payload)
    pair_key = str(audit.get("ペアキー") or f"restore:{old_id}")
    repoint_plans = audit.get("参照付け替え") or []

    print(f"RESTORE_PLAN: 旧No.{old_id}（統合先=No.{winner_id}）を新レコードで復元")
    print(f"  operation_id = {op_id}")
    print(f"  復元フィールド数 = {len(payload)}")

    if not execute:
        todo, notes = await _classify_relink(repoint_plans, winner_id, None)
        print(f"  親エッジ再結線対象 = {len(todo)}件 "
              + " ".join(f"No.{rid}:{'/'.join(fs)}" for rid, fs in todo))
        for note in notes:
            print(f"  ⚠ 再結線スキップ: {note}")
        print("DRY_RUN: 書き込みは行っていません（台帳照合と実書き込みは --execute）")
        return

    # ── 台帳照合（三値・RV08-IMPL-01）。台帳不可＝App34 無書込（fail-closed） ──
    try:
        stages = await find_stages(op_id)
    except MergeJournalError as e:
        raise SystemExit(
            f"操作台帳(DB)を照会できないため復元を中止しました（書き込みなし）: {e}")
    done = stages.get(STAGE_RESTORE)
    pending = stages.get(STAGE_PREIMAGE)

    if done:
        # 復元済み: 保存内容と現況の一致確認のみ（create 0）
        new_id = str(done.get("restored_new_id") or "")
        try:
            rec = await kintone.get_record(APP_KOSEKI_PERSON, new_id)
        except kintone.KintoneError:
            raise SystemExit(
                f"復元済み記録の No.{new_id} を取得できません"
                "（要人手確認・書き込みなし）")
        if _subset_fp(rec, payload) != payload_fp:
            raise SystemExit(
                f"復元済みの No.{new_id} は保存内容と一致しません"
                "（その後の編集あり・要人手確認・書き込みなし）")
        todo, notes = await _classify_relink(repoint_plans, winner_id, new_id)
        for rid, fs in todo:      # 収束: 未適用の親エッジのみ適用（冪等）
            await kintone.update_record(APP_KOSEKI_PERSON, rid,
                                        {f: new_id for f in fs})
        for note in notes:
            print(f"  ⚠ 再結線スキップ: {note}")
        print(f"RESTORED(既存復元へ収束): 旧No.{old_id} → 新No.{new_id}・"
              f"親エッジ追適用 {len(todo)}件（create 0）")
        return

    new_id: str | None = None
    if pending:
        if str(pending.get("payload_fp") or "") != payload_fp:
            raise SystemExit(
                "復元途中の operation が残っていますが、監査JSONの内容が前回と"
                "一致しません（要人手確認・書き込みなし）")
        new_id = await _find_restored_candidate(payload, payload_fp, op_id)
    else:
        # ── preimage（pending）先行保存: create 前・RV08-IMPL-02 ─────────────
        try:
            await record_stage(
                operation_id=op_id, pair_key=pair_key,
                envelope_record_id=str(audit.get("封筒レコードID") or ""),
                winner_id=winner_id, loser_id=old_id,
                stage=STAGE_PREIMAGE,
                payload={"payload_fp": payload_fp, "old_id": old_id,
                         "relink_plan": repoint_plans})
        except MergeJournalError as e:
            raise SystemExit(
                f"操作台帳(DB)へ記録できないため復元を中止しました"
                f"（書き込みなし）: {e}")

    try:
        if new_id is None:
            # RV08-IMPL-05: 復元操作ID を本体保存（ACK 喪失回収の決定的同定キー）
            new_id = str(await kintone.create_record(
                APP_KOSEKI_PERSON,
                {**payload, RESTORE_OPERATION_FIELD: op_id}))
            print(f"  create: 新No.{new_id}")
        else:
            print(f"  create 0（作成済み 新No.{new_id} を再利用）")
        todo, notes = await _classify_relink(repoint_plans, winner_id, new_id)
        for rid, fs in todo:
            await kintone.update_record(APP_KOSEKI_PERSON, rid,
                                        {f: new_id for f in fs})
        for note in notes:
            print(f"  ⚠ 再結線スキップ: {note}")
    except (kintone.KintoneError, kintone.KintoneConflict) as e:
        raise SystemExit(
            "復元の途中で失敗しました。**再実行で回収できます**"
            "（決定的 operation_id の台帳照合により create は増えません）: "
            f"{type(e).__name__}")

    # ── 完了記録（stage=restore・lineage）。失敗しても App34 は完了済み＝
    #    再実行すると pending＋候補照合で「既存復元へ収束」する ────────────────
    try:
        await record_stage(
            operation_id=op_id, pair_key=pair_key,
            envelope_record_id=str(audit.get("封筒レコードID") or ""),
            winner_id=winner_id, loser_id=old_id,
            stage=STAGE_RESTORE,
            payload={"restored_new_id": new_id, "old_id": old_id,
                     "relinked": [{"id": rid, "fields": fs}
                                  for rid, fs in todo],
                     "relink_skipped": len(notes)})
    except MergeJournalError as e:
        raise SystemExit(
            "復元完了の台帳記録に失敗しました（App34 の復元・再結線は完了済み）。"
            f"**再実行すると既存復元へ収束します**: {e}")
    print(f"RESTORED: 旧No.{old_id} → 新No.{new_id}・"
          f"親エッジ再結線 {len(todo)}件・台帳記録済み")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description="過去物理削除分の人物レコード復元（RV-08 R2・手動 CLI）")
    parser.add_argument("audit_json", help="封筒からダウンロードした監査JSON")
    parser.add_argument("--execute", action="store_true",
                        help="実際に書き込む（省略時は dry-run）")
    args = parser.parse_args(argv)
    audit = _load_audit(args.audit_json)
    asyncio.run(_restore(audit, args.execute))


if __name__ == "__main__":
    main(sys.argv[1:])
