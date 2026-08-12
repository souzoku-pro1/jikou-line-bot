"""名寄せ統合の実行（R4-2b T1 → RV-08 soft merge 化）: 勝者マージ＋敗者無効化＋
監査JSON（preimage/postimage）＋操作台帳＋封筒クローズ

設計: 2026-07-07 R4-2b 裁定 → 2026-08-11 凍結 DRAFT_RV08_SOFT_MERGE.md（RV-08）
- person_merge.py（R4-2a スコアラー）とは**モジュールを分離**する:
  スコアラー側は「確定への機械遷移が存在しない」ことを AST 検査で固定しており、
  本モジュールの書き込みは全て**人の確定操作（Bot 二段確認の「OK」）の結果**として
  のみ起動される（自動トリガー結線なし・07 正本の注記と整合）
- 統合規則（裁定3・不変）:
  - 勝者=レコード番号小。勝者側が空・敗者側に値があるフィールドのみ転記
    （勝者の既存値は上書きしない）
  - サブテーブル（登場戸籍・身分事項）は和集合（重複行は排除）
  - 名寄せ確定は「確定」へ遷移（人の確定操作の結果。機械遷移禁止に抵触しない）
  - 確認済み系フィールドは触らない（統合しても未確認のまま）。
    **RV-08 追加: 無効化3フィールド（統合状態/統合先人物ID/統合日時）も転記対象外**
    （操作メタデータであり人物データではない。敗者が無効化済みの状態からの
    再実行で「統合済み無効」が勝者へ転記される事故を構造で防ぐ）
- **敗者は物理削除しない（RV-08 R1/R3・裁定①(B)）**: `PERSON_MERGE_ENABLED` の
  意味を soft merge へ置換（新 flag なし）。敗者は 統合状態=統合済み無効・
  統合先人物ID=勝者・統合日時 の**無効化 update で残置**（lineage 保持）。
  物理削除コードは完全除去（delete_record 呼出し不在は AST 検査で恒久 pin。
  flag×コード状態の全象限で物理削除への経路は存在しない・§4 全象限表）
- 順序固定は維持（§3.1）: ガード再読 → 監査JSON生成・封筒添付 → 参照付け替え →
  勝者更新 → **敗者無効化 update** → postimage 監査添付 → 封筒クローズ →
  postimage 台帳記録（＝**全処理完了マーク**・RV08-IMPL-04）。
  **監査添付が成功するまで App 34 に書かない**（既存規律の維持）
- **部分失敗の回収（§3.2a）**: 1 実行に operation_id を発番し、DB 操作台帳
  （hub/person_merge_journal・immutable 追記・裁定⑦(B)）へ preimage/postimage の
  fingerprint を記録する。再実行は preimage/operation_id と現在値を照合し、
  一致（未適用）→続行・適用済み→skip・**不一致（第三者変更の疑い）→ write 0 で
  要確認**（盲目再適用しない）。部分失敗時は封筒をクローズせず、detail へ
  operation_id・到達段を追記（再指示で回収可能）。台帳へ記録できない場合は
  kintone へ一切書かない（fail-closed）
- 「別人」裁定: 封筒クローズのみ（App34 不書込・再起票抑止は _already_filed）
- env フラグは R4-2a の PERSON_MERGE_ENABLED を共用（無効時は実行も不発）
"""

import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from hub import kintone
from hub.person_merge_journal import (
    STAGE_POSTIMAGE,
    STAGE_PREIMAGE,
    MergeJournalError,
    find_open_operation,
    fingerprint_with_updates,
    list_open_operations,
    record_fingerprint,
    record_stage,
)
from hub.person_validity import (
    MERGE_DATETIME_FIELD,
    MERGE_LINEAGE_FIELD,
    MERGE_STATE_FIELD,
    MERGE_STATE_MERGED,
    RESTORE_OPERATION_FIELD,
    is_active_person,
)
from hub.redact import emit
from person_merge import (
    APP_KOSEKI_PERSON,
    APP_SHIPPING,
    merge_enabled,
    _v,
)

logger = logging.getLogger("person_merge_exec")

STATUS_PENDING = "要確認"
STATUS_DONE = "完了"

# 確認済み系（自動遷移コード禁止・統合しても未確認のまま——既存原則）
FORBIDDEN_FIELDS = ("確認状態", "確認者", "確認日時", "グラフ確定日時",
                    "相続人候補", "相続資格", "被相続人フラグ")

# RV-08: 無効化3フィールド（操作メタデータ・転記/復元の対象外）
MERGE_ADMIN_FIELDS = (MERGE_STATE_FIELD, MERGE_LINEAGE_FIELD,
                      MERGE_DATETIME_FIELD)
# RV08-IMPL-05: 復元操作ID を含む操作メタデータ全4フィールド（転記/復元の対象外。
# 復元済みレコードが再び敗者→復元される場合も旧 operation_id を持ち越さない）
OPERATION_ADMIN_FIELDS = MERGE_ADMIN_FIELDS + (RESTORE_OPERATION_FIELD,)

# kintone GET レコードのシステムフィールド型（転記・復元の対象外）
SYSTEM_TYPES = {"RECORD_NUMBER", "__ID__", "__REVISION__", "CREATOR",
                "CREATED_TIME", "MODIFIER", "UPDATED_TIME", "STATUS",
                "STATUS_ASSIGNEE", "CATEGORY"}

# App 34 内で人物レコードを参照する親エッジ（実装調査の結果・上記 docstring）
PARENT_EDGE_FIELDS = ("父人物ID", "母人物ID", "養父人物ID", "養母人物ID")

# 和集合マージするサブテーブル
SUBTABLE_UNION_FIELDS = ("登場戸籍", "身分事項")

# RV08-IMPL-10: reconcile のカーソルページ幅（テストで縮小 patch 可能）
_RECONCILE_PAGE = 200

JST = timezone(timedelta(hours=9))


@dataclass(frozen=True)
class MergeCandidate:
    """未処理の person_merge 封筒1通（統合1ペア分）"""
    review_record_id: str
    pair_key: str
    winner_id: str
    loser_id: str
    winner_name: str = ""
    loser_name: str = ""
    signals: list[str] = field(default_factory=list)
    pending_case: bool = False   # 保留（案件参照の相違）
    pending_reason: str = ""

    def winner_label(self) -> str:
        return f"No.{self.winner_id} {self.winner_name}".strip()

    def loser_label(self) -> str:
        return f"No.{self.loser_id} {self.loser_name}".strip()


def _name_from_evidence(evidence: dict, record_id: str) -> str:
    """封筒の 根拠.氏名（例 "No.6 鈴木 誠"）から該当レコードの氏名を引く"""
    for entry in evidence.get("氏名") or []:
        s = str(entry)
        prefix = f"No.{record_id} "
        if s.startswith(prefix):
            return s[len(prefix):]
    return ""


async def list_merge_candidates() -> list[MergeCandidate]:
    """未処理（要確認・未実行）の person_merge 封筒を取得する。
    env 未設定は空リスト（上位=T2 が明示メッセージを出す）"""
    if not (APP_SHIPPING.app_id() and APP_SHIPPING.token()):
        logger.info("[PERSON_MERGE_EXEC] APP_SHIPPING 未設定のため候補を取得できません")
        return []
    records = await kintone.search_records(
        APP_SHIPPING,
        '発送ステータス in ("要確認") and 実行済み in ("no")'
        ' and チャネル固有データ like "person_merge"'
        ' order by $id asc limit 100',
        fields=["$id", "チャネル固有データ"])
    out = []
    for r in records:
        try:
            payload = json.loads(_v(r, "チャネル固有データ") or "")
        except json.JSONDecodeError:
            continue
        detail = payload.get("person_merge") if isinstance(payload, dict) else None
        if not isinstance(detail, dict):
            continue
        winner_id = str(detail.get("勝者候補") or "")
        loser_id = str(detail.get("敗者候補") or "")
        if not (winner_id and loser_id):
            continue
        evidence = detail.get("根拠") or {}
        out.append(MergeCandidate(
            review_record_id=_v(r, "$id"),
            pair_key=str(detail.get("ペアキー") or ""),
            winner_id=winner_id, loser_id=loser_id,
            winner_name=_name_from_evidence(evidence, winner_id),
            loser_name=_name_from_evidence(evidence, loser_id),
            signals=[str(s) for s in detail.get("シグナル") or []],
            pending_case=bool(detail.get("保留")),
            pending_reason=str(detail.get("保留理由") or "")))
    return out


def build_merge_payload(winner: dict, loser: dict) -> dict:
    """統合の転記規則（裁定3・純関数）:
    - 勝者側が空・敗者側に値があるフィールドのみ転記（勝者の既存値は不上書き）
    - 登場戸籍・身分事項は和集合（重複行排除）。追加行が無ければ書かない
    - 確認済み系・名寄せ確定・**無効化3フィールド（RV-08）**はここでは書かない
      （名寄せ確定=確定 は呼び出し元で付与）
    """
    payload: dict = {}
    for code, cell in loser.items():
        if not isinstance(cell, dict) or cell.get("type") in SYSTEM_TYPES:
            continue
        if code in FORBIDDEN_FIELDS or code == "名寄せ確定" \
                or code in OPERATION_ADMIN_FIELDS:
            continue
        if code in SUBTABLE_UNION_FIELDS:
            rows = _union_rows(winner.get(code), cell)
            if rows is not None:
                payload[code] = rows
            continue
        loser_value = cell.get("value")
        winner_value = (winner.get(code) or {}).get("value")
        if str(loser_value or "").strip() and not str(winner_value or "").strip():
            payload[code] = loser_value
    return payload


def _row_key(row: dict) -> tuple:
    return tuple(sorted((c, str((v or {}).get("value") or ""))
                        for c, v in (row.get("value") or {}).items()))


def _clean_row(row: dict) -> dict:
    """GET 形の行 → 更新payload形（type・行id を落とす）"""
    return {"value": {c: {"value": (v or {}).get("value")}
                      for c, v in (row.get("value") or {}).items()}}


def _union_rows(winner_cell: dict | None, loser_cell: dict) -> list | None:
    """サブテーブルの和集合（重複行排除・勝者行が先）。敗者由来の追加行が
    無ければ None（そのフィールドは更新しない）"""
    winner_rows = (winner_cell or {}).get("value") or []
    loser_rows = loser_cell.get("value") or []
    seen = set()
    rows = []
    for row in list(winner_rows) + list(loser_rows):
        key = _row_key(row)
        if key in seen:
            continue
        seen.add(key)
        rows.append(_clean_row(row))
    return rows if len(rows) > len(winner_rows) else None


def restore_payload_from_audit(audit: dict) -> dict:
    """監査JSON → create_record 用 payload（人手復元用・08 手順書と
    person_restore_cli.py から使う）。システムフィールドと無効化3フィールド
    （RV-08・復元行は有効行として作る）を除く全フィールド＋サブテーブル全行を
    復元する"""
    record = audit.get("敗者レコード") or {}
    payload: dict = {}
    for code, cell in record.items():
        if not isinstance(cell, dict) or cell.get("type") in SYSTEM_TYPES:
            continue
        if code in OPERATION_ADMIN_FIELDS:
            continue
        if cell.get("type") == "SUBTABLE":
            payload[code] = [_clean_row(row) for row in cell.get("value") or []]
        else:
            payload[code] = cell.get("value")
    return payload


async def _find_referrers(loser_id: str, winner_id: str) -> tuple[list[dict], list[dict]]:
    """敗者を親エッジで参照する人物レコードを洗い出す（読み取りのみ）。
    勝者・敗者自身は除外（同一人ペアで互いを親参照する状況は想定外＝残せば
    自己参照になるため付け替えない。監査JSONに敗者側のエッジは温存される）。
    Returns: (付け替え計画, 対象行の取得値〔preimage 監査用〕)"""
    query = " or ".join(f'{f} = "{loser_id}"' for f in PARENT_EDGE_FIELDS)
    records = await kintone.search_records(
        APP_KOSEKI_PERSON, f"({query}) order by $id asc limit 500",
        fields=["$id", *PARENT_EDGE_FIELDS])
    plans = []
    rows = []
    for r in records:
        rid = _v(r, "$id")
        if rid in (loser_id, winner_id):
            continue
        fields = [f for f in PARENT_EDGE_FIELDS if _v(r, f) == loser_id]
        if fields:
            plans.append({"person_record_id": rid, "fields": fields})
            rows.append(r)
    return plans, rows


def _edge_view(record: dict) -> dict:
    """親エッジ4フィールドのみの正規ビュー（付け替え対象行の fingerprint 基準。
    preimage 時は search（部分取得）・再実行時は get（全取得）でも同一形になる）"""
    return {f: {"type": "SINGLE_LINE_TEXT", "value": _v(record, f)}
            for f in PARENT_EDGE_FIELDS}


def _classify(current_fp: str, stored: dict) -> str:
    """§3.2a の照合: 適用済み→skip・一致（未適用）→続行・不一致→write 0"""
    if current_fp == str(stored.get("post") or ""):
        return "applied"
    if current_fp == str(stored.get("pre") or ""):
        return "unapplied"
    return "mismatch"


def _disable_updates(winner_id: str) -> dict:
    """敗者無効化 update の field 集合（固定・テストで pin）"""
    return {MERGE_STATE_FIELD: MERGE_STATE_MERGED,
            MERGE_LINEAGE_FIELD: winner_id,
            MERGE_DATETIME_FIELD: datetime.now(JST).isoformat(
                timespec="seconds")}


def _artifact_names(envelope: dict) -> set:
    return {str(f.get("name") or "")
            for f in (envelope.get("成果物") or {}).get("value") or []}


async def _attach_file(envelope: dict, review_record_id: str, filename: str,
                       content: bytes, extra_keys: list) -> None:
    """封筒 成果物 への添付（既存 fileKey 温存＋本実行で先に添付した分も温存）"""
    file_key = await kintone.upload_file(APP_SHIPPING, filename, content,
                                         "application/json")
    existing = [{"fileKey": f.get("fileKey")}
                for f in (envelope.get("成果物") or {}).get("value") or []
                if f.get("fileKey")]
    keys = existing + [{"fileKey": k} for k in extra_keys] \
        + [{"fileKey": file_key}]
    await kintone.update_record(APP_SHIPPING, review_record_id,
                                {"成果物": keys})
    extra_keys.append(file_key)


async def _note_partial(cand: MergeCandidate, operation_id: str,
                        reached: str) -> None:
    """部分失敗時の封筒 detail 追記（§3.2a・best-effort。封筒はクローズしない）"""
    try:
        env = await kintone.get_record(APP_SHIPPING, cand.review_record_id)
        payload = json.loads(_v(env, "チャネル固有データ") or "")
        detail = payload.get("person_merge") or {}
        if not isinstance(detail, dict):
            detail = {"ペアキー": cand.pair_key}
    except Exception:
        detail = {"ペアキー": cand.pair_key}
    detail["operation_id"] = operation_id
    detail["到達段"] = reached
    try:
        await kintone.update_record(APP_SHIPPING, cand.review_record_id, {
            "チャネル固有データ": json.dumps({"person_merge": detail},
                                             ensure_ascii=False)})
    except Exception:
        logger.error("[PERSON_MERGE_EXEC] 部分失敗の detail 追記に失敗"
                     "（固定文言のみ・封筒は要確認のまま）")


async def execute_merge(cand: MergeCandidate) -> dict:
    """1ペアの統合実行（RV-08 soft merge）。順序固定（部分成功設計・§3.1）:
    ガード再読 → 台帳照合/preimage 記録 → 監査JSON生成・封筒添付 →
    参照付け替え → 勝者更新 → 敗者無効化 → postimage 添付 → 封筒クローズ →
    postimage 台帳記録（全処理完了マーク・RV08-IMPL-04）。
    **監査添付が成功するまで App 34 に書かない**"""
    if not merge_enabled():
        return {"status": "unavailable",
                "reason": "PERSON_MERGE_ENABLED が未設定です"}
    for app in (APP_SHIPPING, APP_KOSEKI_PERSON):
        if not (app.app_id() and app.token()):
            return {"status": "unavailable",
                    "reason": f"{app.label} の env（{app.app_id_env}）が未設定です"}

    # ── ガード: 封筒・勝者・敗者の書き込み直前再読（関所と同じ意味論） ────────
    envelope = await kintone.get_record(APP_SHIPPING, cand.review_record_id)
    status, executed = _v(envelope, "発送ステータス"), _v(envelope, "実行済み")
    if status != STATUS_PENDING or executed != "no":
        return {"status": "aborted",
                "reason": f"封筒 No.{cand.review_record_id} が要確認ではなくなって"
                          f"います（発送ステータス={status}・実行済み={executed}）。"
                          "書き込みなしで中止しました"}
    try:
        winner = await kintone.get_record(APP_KOSEKI_PERSON, cand.winner_id)
        loser = await kintone.get_record(APP_KOSEKI_PERSON, cand.loser_id)
    except kintone.KintoneError as e:
        return {"status": "aborted",
                "reason": f"人物レコードの取得に失敗しました（No.{cand.winner_id}/"
                          f"No.{cand.loser_id}）: {e}。書き込みなしで中止しました"}

    # ── RV08-03: 直接 get の状態確認（無効化行なら要確認へ倒す） ─────────────
    if not is_active_person(winner):
        return {"status": "aborted",
                "reason": f"勝者 No.{cand.winner_id} が統合済み無効です"
                          "（無効化行への統合は不可・要確認・書き込みなし）"}
    loser_inactive = not is_active_person(loser)
    if loser_inactive and _v(loser, MERGE_LINEAGE_FIELD) != cand.winner_id:
        return {"status": "aborted",
                "reason": f"敗者 No.{cand.loser_id} は別の統合先へ無効化済みです"
                          "（要確認・書き込みなし）"}

    # ── 操作台帳: 未完了 operation の照合 or 新規発番（§3.2a・裁定⑦(B)） ─────
    try:
        prior = await find_open_operation(cand.review_record_id, cand.pair_key)
    except MergeJournalError as e:
        return {"status": "aborted",
                "reason": f"操作台帳(DB)を照会できないため統合を中止しました"
                          f"（書き込みなし）: {e}"}
    repoint_plans, referrer_rows = await _find_referrers(
        cand.loser_id, cand.winner_id)

    if prior is None:
        operation_id = uuid.uuid4().hex
        payload_updates = build_merge_payload(winner, loser)
        payload_updates["名寄せ確定"] = "確定"
        disable = _disable_updates(cand.winner_id)
        pre_payload = {
            "winner": {"id": cand.winner_id,
                       "pre": record_fingerprint(winner),
                       "post": fingerprint_with_updates(winner, payload_updates)},
            "loser": {"id": cand.loser_id,
                      "pre": record_fingerprint(loser),
                      "post": fingerprint_with_updates(loser, disable)},
            "repoint": [
                {"id": p["person_record_id"], "fields": p["fields"],
                 "pre": record_fingerprint(_edge_view(r)),
                 "post": fingerprint_with_updates(
                     _edge_view(r),
                     {f: cand.winner_id for f in p["fields"]})}
                for p, r in zip(repoint_plans, referrer_rows)],
        }
        try:
            await record_stage(
                operation_id=operation_id, pair_key=cand.pair_key,
                envelope_record_id=cand.review_record_id,
                winner_id=cand.winner_id, loser_id=cand.loser_id,
                stage=STAGE_PREIMAGE, payload=pre_payload)
        except MergeJournalError as e:
            return {"status": "aborted",
                    "reason": f"操作台帳(DB)へ記録できないため統合を中止しました"
                              f"（書き込みなし）: {e}"}
        do_winner = True
        do_loser = not loser_inactive   # 既に無効（統合先=勝者）なら再更新しない
        repoint_todo = list(repoint_plans)
    else:
        # ── 再実行の照合（一致=続行・適用済み=skip・不一致=write 0 要確認） ──
        operation_id = prior["operation_id"]
        stored = prior["payload"] or {}
        mismatches: list[str] = []
        cls_w = _classify(record_fingerprint(winner), stored.get("winner") or {})
        cls_l = _classify(record_fingerprint(loser), stored.get("loser") or {})
        if cls_w == "mismatch":
            mismatches.append(f"勝者 No.{cand.winner_id}")
        if cls_l == "mismatch":
            mismatches.append(f"敗者 No.{cand.loser_id}")
        repoint_todo = []
        for entry in stored.get("repoint") or []:
            rid = str(entry.get("id") or "")
            try:
                row = await kintone.get_record(APP_KOSEKI_PERSON, rid)
            except kintone.KintoneError:
                mismatches.append(f"付け替え対象 No.{rid}（取得不能）")
                continue
            cls_r = _classify(record_fingerprint(_edge_view(row)), entry)
            if cls_r == "mismatch":
                mismatches.append(f"付け替え対象 No.{rid}")
            elif cls_r == "unapplied":
                repoint_todo.append({"person_record_id": rid,
                                     "fields": list(entry.get("fields") or [])})
        if mismatches:
            return {"status": "aborted", "operation_id": operation_id,
                    "reason": "前回実行の preimage と現在値が一致しません"
                              f"（第三者変更の疑い: {'・'.join(mismatches)}）。"
                              "書き込みゼロで中止しました（要確認・盲目再適用"
                              "しません）"}
        do_winner = cls_w == "unapplied"
        do_loser = cls_l == "unapplied"
        payload_updates = build_merge_payload(winner, loser)
        payload_updates["名寄せ確定"] = "確定"
        disable = _disable_updates(cand.winner_id)

    # ── 監査JSON（削除前提時代とキー互換・preimage 拡張＋operation_id 貫通） ──
    audit = {
        "監査種別": "person_merge",
        "operation_id": operation_id,
        "ペアキー": cand.pair_key,
        "封筒レコードID": cand.review_record_id,
        "統合先レコードID": cand.winner_id,
        "削除レコードID": cand.loser_id,   # キーは旧監査と互換（実体は無効化）
        "統合方式": "soft_merge",           # RV-08 以降の判別子
        "統合日時": datetime.now(JST).isoformat(),
        "成立シグナル": cand.signals,
        "参照付け替え": repoint_plans,
        "参照付け替え前レコード": referrer_rows,   # preimage 拡張（§3.2a）
        "勝者レコード": winner,                     # preimage 拡張（§3.2a）
        "敗者レコード": loser,  # 全フィールド＋サブテーブル全行（GET 形そのまま）
    }
    audit_name = f"名寄せ統合監査_{cand.winner_id}-{cand.loser_id}.json"
    post_name = f"名寄せ統合監査_{cand.winner_id}-{cand.loser_id}_postimage.json"
    attached = _artifact_names(envelope)
    extra_keys: list = []

    # ── 監査添付（成功が無効化の前提条件。失敗したら App 34 に一切書かない） ──
    if audit_name not in attached:
        try:
            await _attach_file(
                envelope, cand.review_record_id, audit_name,
                json.dumps(audit, ensure_ascii=False, indent=1).encode("utf-8"),
                extra_keys)
        except Exception as e:
            return {"status": "aborted", "operation_id": operation_id,
                    "reason": f"監査JSONの保存に失敗したため統合を中止しました"
                              f"（無効化・更新なし）: {str(e)[:200]}"}

    # ── App34 書込み（順序固定・部分失敗は封筒 open 維持＋detail 追記） ───────
    reached = "監査添付"
    try:
        reached = "参照付け替え"
        for plan in repoint_todo:
            await kintone.update_record(
                APP_KOSEKI_PERSON, plan["person_record_id"],
                {f: cand.winner_id for f in plan["fields"]})
        reached = "勝者更新"
        if do_winner:
            await kintone.update_record(APP_KOSEKI_PERSON, cand.winner_id,
                                        payload_updates)
        reached = "敗者無効化"
        if do_loser:
            await kintone.update_record(APP_KOSEKI_PERSON, cand.loser_id,
                                        disable)
        # ── postimage 監査の封筒添付（値の監査・§3.2a）。二重添付防止の一次
        #    判定は封筒 成果物 の filename（server 側の真値）・部分失敗時の
        #    detail 追記（operation_id/到達段）が補助記録（RV08-IMPL-04） ────────
        reached = "postimage添付"
        post_winner = await kintone.get_record(APP_KOSEKI_PERSON, cand.winner_id)
        post_loser = await kintone.get_record(APP_KOSEKI_PERSON, cand.loser_id)
        if post_name not in attached:
            postimage = {
                "監査種別": "person_merge_postimage",
                "operation_id": operation_id,
                "ペアキー": cand.pair_key,
                "封筒レコードID": cand.review_record_id,
                "統合先レコードID": cand.winner_id,
                "無効化レコードID": cand.loser_id,
                "統合日時": datetime.now(JST).isoformat(),
                "勝者レコード": post_winner,
                "敗者レコード": post_loser,
            }
            await _attach_file(
                envelope, cand.review_record_id, post_name,
                json.dumps(postimage, ensure_ascii=False,
                           indent=1).encode("utf-8"),
                extra_keys)
        reached = "封筒クローズ"
        await kintone.update_record(APP_SHIPPING, cand.review_record_id, {
            "発送ステータス": STATUS_DONE,
            "実行済み": "yes",
        })
    except Exception as e:
        await _note_partial(cand, operation_id, reached)
        return {"status": "partial", "operation_id": operation_id,
                "reason": f"統合の途中で失敗しました（到達段={reached}・封筒は"
                          "要確認のまま・再指示で回収できます）: "
                          f"{str(e)[:200]}"}

    # ── postimage 台帳記録＝**全処理完了マーク**（RV08-IMPL-04: kintone 側の
    #    全書込み・添付・封筒クローズの後の最後尾。中間失敗は preimage のみの
    #    open operation として find_open_operation が同一 operation_id で回収）。
    #    ここでの失敗＝「封筒 closed×台帳 open」は reconcile_merge_operations が
    #    閉集合検証つきで完了化する（RV08-IMPL-07・検知でなく回収）────────────────
    result = {"status": "merged", "winner_id": cand.winner_id,
              "loser_id": cand.loser_id, "repointed": repoint_plans,
              "operation_id": operation_id,
              "review_record_id": cand.review_record_id}
    try:
        await record_stage(
            operation_id=operation_id, pair_key=cand.pair_key,
            envelope_record_id=cand.review_record_id,
            winner_id=cand.winner_id, loser_id=cand.loser_id,
            stage=STAGE_POSTIMAGE,
            payload={"winner": {"id": cand.winner_id,
                                "fp": record_fingerprint(post_winner)},
                     "loser": {"id": cand.loser_id,
                               "fp": record_fingerprint(post_loser)}})
    except MergeJournalError:
        logger.error("[PERSON_MERGE_EXEC] 完了マーク（postimage 台帳記録）に失敗"
                     "（封筒はクローズ済み・reconcile 経路が回収・固定分類のみ）")

    logger.info("[PERSON_MERGE_EXEC] merged winner=No.%s loser=No.%s(disabled) "
                "review=No.%s repointed=%s",
                emit(cand.winner_id, "record_id", "log", "operator"),
                emit(cand.loser_id, "record_id", "log", "operator"),
                emit(cand.review_record_id, "record_id", "log", "operator"),
                emit(len(repoint_plans), "count", "log", "operator"))
    return result


async def reject_pair(cand: MergeCandidate) -> dict:
    """「別人」裁定: 封筒クローズ＋裁定の記録。App 34 には一切書かない。
    再起票の恒久抑止は person_merge._already_filed（状態不問のペアキー照合）が担う"""
    if not merge_enabled():
        return {"status": "unavailable",
                "reason": "PERSON_MERGE_ENABLED が未設定です"}
    if not (APP_SHIPPING.app_id() and APP_SHIPPING.token()):
        return {"status": "unavailable",
                "reason": f"{APP_SHIPPING.label} の env が未設定です"}
    envelope = await kintone.get_record(APP_SHIPPING, cand.review_record_id)
    status, executed = _v(envelope, "発送ステータス"), _v(envelope, "実行済み")
    if status != STATUS_PENDING or executed != "no":
        return {"status": "aborted",
                "reason": f"封筒 No.{cand.review_record_id} が要確認ではなくなって"
                          f"います（発送ステータス={status}・実行済み={executed}）。"
                          "書き込みなしで中止しました"}
    try:
        payload = json.loads(_v(envelope, "チャネル固有データ") or "")
        detail = payload.get("person_merge") or {}
    except json.JSONDecodeError:
        detail = {"ペアキー": cand.pair_key}
    detail["裁定"] = "別人"
    detail["裁定日時"] = datetime.now(JST).isoformat()
    await kintone.update_record(APP_SHIPPING, cand.review_record_id, {
        "チャネル固有データ": json.dumps({"person_merge": detail},
                                         ensure_ascii=False),
        "発送ステータス": STATUS_DONE,
        "実行済み": "yes",
    })
    logger.info("[PERSON_MERGE_EXEC] rejected pair=%s review=No.%s（再起票を恒久抑止）",
                emit(cand.pair_key, "record_id", "log", "operator"),
                emit(cand.review_record_id, "record_id", "log", "operator"))
    return {"status": "rejected", "pair_key": cand.pair_key,
            "review_record_id": cand.review_record_id}


async def reconcile_merge_operations() -> dict:
    """「封筒 closed × 台帳 open」operation の回収（RV08-IMPL-07・裁定=reconcile
    追記方式）。postimage 台帳記録（全処理完了マーク・最後尾）だけが失敗した
    operation を、**閉集合検証の成立時のみ**同一 operation_id へ postimage stage を
    追記して完了化する（stage 閉集合は不変・App34/封筒への書き込みゼロ）。

    閉集合検証（3点・全成立が条件）:
      (i)   App34 全対象行（勝者・敗者・付け替え行）の現在 fingerprint が
            preimage 記録の expected-post と**完全一致**
      (ii)  監査JSON・postimage の両添付 filename が封筒 成果物 に存在
      (iii) 封筒が closed（発送ステータス=完了・実行済み=yes）
    検証不一致は**追記せず警報**（要確認・沈黙しない）。封筒が open のままの
    operation は通常の再実行（find_open_operation）の管轄＝本経路の対象外。

    起動タイミング: RV05 startup reconcile 流儀（main.py _on_startup・
    PERSON_MERGE_ENABLED=1 のときのみ・失敗しても起動を止めない）。
    手動起動も可（関数を直接呼ぶ）。

    RV08-IMPL-10: list_open_operations の **id カーソルページング**で全未完了
    operation を有限回で走査完了する（打ち切りなし——open 封筒の operation が
    多数滞留しても後方の「封筒 closed×台帳 open」へ必ず到達する）。統計は
    walked/still_open/reconciled/alerted。滞留数が閾値
    （env MERGE_OPEN_OPS_ALERT_THRESHOLD・既定 20）を超えたら業務警報を
    **1 回**発する（沈黙しない）。
    """
    stats = {"walked": 0, "reconciled": 0, "still_open": 0, "alerted": 0}
    if not merge_enabled():
        return stats
    if not (APP_SHIPPING.app_id() and APP_SHIPPING.token()
            and APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        return stats
    cursor = 0
    ops: list[dict] = []
    while True:
        page = await list_open_operations(after_id=cursor,
                                          limit=_RECONCILE_PAGE)
        if not page:
            break
        ops.extend(page)
        cursor = page[-1]["row_id"]   # 厳密単調増加＝有限回で必ず終端
    for op in ops:
        stats["walked"] += 1
        try:
            envelope = await kintone.get_record(APP_SHIPPING,
                                                op["envelope_record_id"])
        except kintone.KintoneError:
            await _alert_reconcile(op, "封筒を取得できません")
            stats["alerted"] += 1
            continue
        closed = _v(envelope, "発送ステータス") == STATUS_DONE \
            and _v(envelope, "実行済み") == "yes"
        if not closed:
            stats["still_open"] += 1   # 通常 resume の管轄（対象外）
            continue
        stored = op["payload"] or {}
        problems: list[str] = []
        # (i) App34 全対象行の postimage 完全一致
        fps = {}
        for side in ("winner", "loser"):
            info = stored.get(side) or {}
            rid = str(info.get("id") or "")
            try:
                rec = await kintone.get_record(APP_KOSEKI_PERSON, rid)
            except kintone.KintoneError:
                problems.append(f"{side} No.{rid} 取得不能")
                continue
            fps[side] = record_fingerprint(rec)
            if fps[side] != str(info.get("post") or ""):
                problems.append(f"{side} No.{rid} postimage 不一致")
        for entry in stored.get("repoint") or []:
            rid = str(entry.get("id") or "")
            try:
                row = await kintone.get_record(APP_KOSEKI_PERSON, rid)
            except kintone.KintoneError:
                problems.append(f"付け替え対象 No.{rid} 取得不能")
                continue
            if record_fingerprint(_edge_view(row)) != str(entry.get("post") or ""):
                problems.append(f"付け替え対象 No.{rid} postimage 不一致")
        # (ii) 両添付 filename の存在
        names = _artifact_names(envelope)
        audit_name = f"名寄せ統合監査_{op['winner_id']}-{op['loser_id']}.json"
        post_name = f"名寄せ統合監査_{op['winner_id']}-{op['loser_id']}_postimage.json"
        for fn, label in ((audit_name, "監査JSON"), (post_name, "postimage")):
            if fn not in names:
                problems.append(f"{label} 添付なし")
        if problems:
            await _alert_reconcile(op, "・".join(problems))
            stats["alerted"] += 1
            continue
        try:
            await record_stage(
                operation_id=op["operation_id"], pair_key=op["pair_key"],
                envelope_record_id=op["envelope_record_id"],
                winner_id=op["winner_id"], loser_id=op["loser_id"],
                stage=STAGE_POSTIMAGE,
                payload={"winner": {"id": op["winner_id"],
                                    "fp": fps.get("winner", "")},
                         "loser": {"id": op["loser_id"],
                                   "fp": fps.get("loser", "")},
                         "reconciled": True})
        except MergeJournalError:
            # 並行 reconcile / 台帳一時不調は次回 startup で再回収（沈黙させない）
            logger.error("[PERSON_MERGE_EXEC] reconcile の完了マーク追記に失敗"
                         "（次回 startup で再回収・固定分類のみ）")
            continue
        stats["reconciled"] += 1
        logger.info("[PERSON_MERGE_EXEC] reconciled operation（封筒 closed×台帳 "
                    "open の完了化） review=No.%s",
                    emit(op["envelope_record_id"], "record_id", "log",
                         "operator"))
    logger.info("[PERSON_MERGE_EXEC] reconcile stats walked=%s still_open=%s "
                "reconciled=%s alerted=%s",
                emit(stats["walked"], "count", "log", "operator"),
                emit(stats["still_open"], "count", "log", "operator"),
                emit(stats["reconciled"], "count", "log", "operator"),
                emit(stats["alerted"], "count", "log", "operator"))
    # RV08-IMPL-10: 異常な滞留数は業務警報を 1 回発する（沈黙しない・件数のみ）
    threshold = int(os.environ.get("MERGE_OPEN_OPS_ALERT_THRESHOLD", "20"))
    if stats["walked"] > threshold:
        from hub import notify
        try:
            await notify.notify_admin_line(
                "【名寄せ統合: 未完了 operation の滞留】\n"
                f"未完了 {stats['walked']} 件（閾値 {threshold} 超・"
                f"open封筒 {stats['still_open']} 件・要確認 {stats['alerted']} 件）\n"
                "封筒キューの未処理と要確認警報を確認してください",
                throttle_key="merge_reconcile_backlog")
        except Exception:
            logger.error("[PERSON_MERGE_EXEC] backlog alert failed "
                         "(fixed classification only)")
    return stats


async def _alert_reconcile(op: dict, reason: str) -> None:
    """reconcile 検証不一致の業務警報（要確認・沈黙しない・PII なし=ID/固定文言）"""
    from hub import notify
    try:
        await notify.notify_admin_line(
            "【名寄せ統合: 完了マーク未記録の operation を検証しましたが"
            "完了化できません（要確認）】\n"
            f"封筒 No.{op['envelope_record_id']} / 勝者 No.{op['winner_id']} / "
            f"敗者 No.{op['loser_id']}\n"
            f"不一致: {reason}\n"
            "台帳の open operation は残置しています（人手確認のうえ対処して"
            "ください）",
            throttle_key=f"merge_reconcile:{op['operation_id']}")
    except Exception:
        logger.error("[PERSON_MERGE_EXEC] reconcile alert failed "
                     "(fixed classification only)")
