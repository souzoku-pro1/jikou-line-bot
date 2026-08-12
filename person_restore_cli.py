"""person_restore_cli — 過去に物理削除された人物レコードの復元 tool（手動 CLI）

正本: DRAFT_RV08_SOFT_MERGE.md §3.2・裁定④=(A) 手動 CLI（R2）。

- 対象は **soft merge 移行前に物理削除された敗者**（封筒添付の監査JSONが原資）。
  soft merge 後の無効化行の復元（unmerge）は関所型の[人]操作でありスコープ外。
- kintone 仕様上、旧レコード番号での復元は不可能＝**新 ID での復元＋lineage 記録**
  （§3.2）。lineage は操作台帳（person_merge_operation・stage=restore）へ追記する。
- 復元内容: 監査JSONの「敗者レコード」全フィールド＋サブテーブル全行
  （システムフィールドと RV-08 無効化3フィールドを除く＝有効行として作成）。
  「参照付け替え」計画の**逆適用**で親エッジを新 ID へ再結線する（当時勝者へ
  付け替えられた行のうち、現在も勝者を指しているフィールドのみ・第三者変更は
  上書きしない＝§3.2a の照合規律と同じ盲目上書き禁止）。
- 既定は **dry-run**（書き込みゼロで計画を表示）。書き込みは --execute 明示時のみ。
  PERSON_MERGE_ENABLED には依存しない（手動起動自体が明示承認・
  sync_missing_persons と同じ整理）。
- 出力は record ID・件数のみ（氏名等の PII を stdout に出さない）。

使い方:
    python person_restore_cli.py 監査ファイル.json            # dry-run
    python person_restore_cli.py 監査ファイル.json --execute  # 実書き込み
"""

import argparse
import asyncio
import json
import sys
import uuid

from hub import kintone
from hub.person_merge_journal import STAGE_RESTORE, record_stage
from hub.person_validity import is_active_person
from person_merge import APP_KOSEKI_PERSON, _v
from person_merge_exec import (
    PARENT_EDGE_FIELDS,
    restore_payload_from_audit,
)


def _load_audit(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        audit = json.load(f)
    if audit.get("監査種別") != "person_merge":
        raise SystemExit("監査種別が person_merge ではありません（中止）")
    if not (audit.get("敗者レコード") or {}):
        raise SystemExit("監査JSONに 敗者レコード がありません（中止）")
    return audit


async def _restore(audit: dict, execute: bool) -> None:
    old_id = str(audit.get("削除レコードID") or "")
    winner_id = str(audit.get("統合先レコードID") or "")
    if audit.get("統合方式") == "soft_merge":
        raise SystemExit(
            "この監査JSONは soft merge（無効化・残置）の記録です。復元 CLI の"
            "対象は物理削除時代の監査のみ——無効化行の復元（unmerge）は"
            "[人]の関所型操作で行ってください（中止）")
    if not (APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        raise SystemExit("APP_KOSEKI_PERSON / TOKEN_KOSEKI_PERSON 未設定（中止）")

    payload = restore_payload_from_audit(audit)
    repoint_plans = audit.get("参照付け替え") or []

    # 親エッジ逆適用の計画（現在値が勝者のままのフィールドのみ・盲目上書き禁止）
    relink: list[tuple[str, list[str]]] = []
    skipped: list[str] = []
    for plan in repoint_plans:
        rid = str(plan.get("person_record_id") or "")
        fields = [str(f) for f in plan.get("fields") or []
                  if f in PARENT_EDGE_FIELDS]
        if not (rid and fields):
            continue
        try:
            row = await kintone.get_record(APP_KOSEKI_PERSON, rid)
        except kintone.KintoneError:
            skipped.append(f"No.{rid}（取得不能）")
            continue
        todo = [f for f in fields if _v(row, f) == winner_id]
        changed = [f for f in fields if _v(row, f) not in (winner_id, "")]
        if changed:
            skipped.append(f"No.{rid}（第三者変更あり・要人手確認）")
            continue
        if todo:
            relink.append((rid, todo))

    print(f"RESTORE_PLAN: 旧No.{old_id}（統合先=No.{winner_id}）を新レコードで復元")
    print(f"  復元フィールド数 = {len(payload)}")
    print(f"  親エッジ再結線対象 = {len(relink)}件 "
          + " ".join(f"No.{rid}:{'/'.join(fs)}" for rid, fs in relink))
    for note in skipped:
        print(f"  ⚠ 再結線スキップ: {note}")

    if not execute:
        print("DRY_RUN: 書き込みは行っていません（--execute で実行）")
        return

    # 二重復元の抑止: 監査に勝者が残っていれば勝者の存在を確認（無効化行の
    # 勝者へは注意喚起のみ・復元自体は独立）
    if winner_id:
        try:
            winner = await kintone.get_record(APP_KOSEKI_PERSON, winner_id)
            if not is_active_person(winner):
                print(f"  ⚠ 統合先 No.{winner_id} は現在無効化行です（参考情報）")
        except kintone.KintoneError:
            print(f"  ⚠ 統合先 No.{winner_id} を取得できません（参考情報）")

    new_id = str(await kintone.create_record(APP_KOSEKI_PERSON, payload))
    for rid, fs in relink:
        await kintone.update_record(APP_KOSEKI_PERSON, rid,
                                    {f: new_id for f in fs})
    # lineage 記録（§3.2・PII なし: ID と件数のみ）
    await record_stage(
        operation_id=uuid.uuid4().hex,
        pair_key=str(audit.get("ペアキー") or f"restore:{old_id}"),
        envelope_record_id=str(audit.get("封筒レコードID") or ""),
        winner_id=winner_id, loser_id=old_id,
        stage=STAGE_RESTORE,
        payload={"restored_new_id": new_id, "old_id": old_id,
                 "relinked": [{"id": rid, "fields": fs} for rid, fs in relink],
                 "relink_skipped": len(skipped)})
    print(f"RESTORED: 旧No.{old_id} → 新No.{new_id}・"
          f"親エッジ再結線 {len(relink)}件・台帳記録済み")


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
