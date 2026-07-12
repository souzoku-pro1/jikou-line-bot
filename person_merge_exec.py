"""名寄せ統合の実行（R4-2b T1）: 勝者マージ＋敗者物理削除＋監査JSON＋封筒クローズ

設計: 2026-07-07 R4-2b 裁定
- person_merge.py（R4-2a スコアラー）とは**モジュールを分離**する:
  スコアラー側は「確定への機械遷移が存在しない」ことを AST 検査で固定しており、
  本モジュールの書き込みは全て**人の確定操作（Bot 二段確認の「OK」）の結果**として
  のみ起動される（自動トリガー結線なし・07 正本の注記と整合）
- 統合規則（裁定3）:
  - 勝者=レコード番号小。勝者側が空・敗者側に値があるフィールドのみ転記
    （勝者の既存値は上書きしない）
  - サブテーブル（登場戸籍・身分事項）は和集合（重複行は排除）。
    身分事項も和集合とするのは実装判断: 敗者側の死亡・婚姻等の事項行は
    fill-if-empty では消えるが、相続判断に直結するため行単位で温存する
  - 名寄せ確定は「確定」へ遷移（人の確定操作の結果。機械遷移禁止に抵触しない）
  - 確認済み系フィールド（確認状態/確認者/確認日時/グラフ確定日時/相続人候補/
    相続資格/被相続人フラグ）は触らない（統合しても未確認のまま）
- 敗者=物理削除。**監査JSONの封筒添付の成功を削除の前提条件とする**（部分成功設計・
  順序固定: 監査添付 → 参照付け替え → 勝者更新 → 敗者削除 → 封筒クローズ。
  監査が保存できなければ App 34 への書き込みも削除も行わない）
- 参照構造（実装調査 2026-07-07）: App 34 人物レコードを参照するのは
  App 34 自身の親エッジ4フィールド（父人物ID/母人物ID/養父人物ID/養母人物ID）のみ。
  身分事項の相手方は氏名文字列（ID参照なし・kinship_graph が実行時に氏名照合）、
  App 33/36 に人物ID参照は存在しない。削除前に親エッジを勝者へ付け替える
- 「別人」裁定: 封筒クローズ（完了+実行済みyes）＋チャネル固有データに裁定を記録。
  再起票の恒久抑止は person_merge._already_filed の状態不問照合が担う（R4-2b 改修）
- env フラグは R4-2a の PERSON_MERGE_ENABLED を共用（無効時は実行も不発）
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from hub import kintone
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

# kintone GET レコードのシステムフィールド型（転記・復元の対象外）
SYSTEM_TYPES = {"RECORD_NUMBER", "__ID__", "__REVISION__", "CREATOR",
                "CREATED_TIME", "MODIFIER", "UPDATED_TIME", "STATUS",
                "STATUS_ASSIGNEE", "CATEGORY"}

# App 34 内で人物レコードを参照する親エッジ（実装調査の結果・上記 docstring）
PARENT_EDGE_FIELDS = ("父人物ID", "母人物ID", "養父人物ID", "養母人物ID")

# 和集合マージするサブテーブル
SUBTABLE_UNION_FIELDS = ("登場戸籍", "身分事項")

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
    - 確認済み系・名寄せ確定はここでは書かない（名寄せ確定=確定 は呼び出し元で付与）
    """
    payload: dict = {}
    for code, cell in loser.items():
        if not isinstance(cell, dict) or cell.get("type") in SYSTEM_TYPES:
            continue
        if code in FORBIDDEN_FIELDS or code == "名寄せ確定":
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
    """監査JSON → create_record 用 payload（人手復元用・08 手順書から使う）。
    システムフィールドを除く全フィールド＋サブテーブル全行を復元する"""
    record = audit.get("敗者レコード") or {}
    payload: dict = {}
    for code, cell in record.items():
        if not isinstance(cell, dict) or cell.get("type") in SYSTEM_TYPES:
            continue
        if cell.get("type") == "SUBTABLE":
            payload[code] = [_clean_row(row) for row in cell.get("value") or []]
        else:
            payload[code] = cell.get("value")
    return payload


async def _find_referrers(loser_id: str, winner_id: str) -> list[dict]:
    """敗者を親エッジで参照する人物レコードを洗い出す（読み取りのみ）。
    勝者・敗者自身は除外（同一人ペアで互いを親参照する状況は想定外＝残せば
    自己参照になるため付け替えない。監査JSONに敗者側のエッジは温存される）"""
    query = " or ".join(f'{f} = "{loser_id}"' for f in PARENT_EDGE_FIELDS)
    records = await kintone.search_records(
        APP_KOSEKI_PERSON, f"({query}) order by $id asc limit 500",
        fields=["$id", *PARENT_EDGE_FIELDS])
    plans = []
    for r in records:
        rid = _v(r, "$id")
        if rid in (loser_id, winner_id):
            continue
        fields = [f for f in PARENT_EDGE_FIELDS if _v(r, f) == loser_id]
        if fields:
            plans.append({"person_record_id": rid, "fields": fields})
    return plans


async def execute_merge(cand: MergeCandidate) -> dict:
    """1ペアの統合実行。順序固定（部分成功設計）:
    ガード再読 → 監査JSON生成・封筒添付 → 参照付け替え → 勝者更新 →
    敗者削除 → 封筒クローズ。**監査添付が成功するまで App 34 に書かない**"""
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

    # ── 監査JSON（削除前の敗者レコード全体を verbatim 保持） ─────────────────
    repoint_plans = await _find_referrers(cand.loser_id, cand.winner_id)
    audit = {
        "監査種別": "person_merge",
        "ペアキー": cand.pair_key,
        "封筒レコードID": cand.review_record_id,
        "統合先レコードID": cand.winner_id,
        "削除レコードID": cand.loser_id,
        "統合日時": datetime.now(JST).isoformat(),
        "成立シグナル": cand.signals,
        "参照付け替え": repoint_plans,
        "敗者レコード": loser,  # 全フィールド＋サブテーブル全行（GET 形そのまま）
    }
    audit_bytes = json.dumps(audit, ensure_ascii=False, indent=1).encode("utf-8")

    # ── 監査添付（成功が削除の前提条件。失敗したら App 34 に一切書かない） ────
    try:
        file_key = await kintone.upload_file(
            APP_SHIPPING,
            f"名寄せ統合監査_{cand.winner_id}-{cand.loser_id}.json",
            audit_bytes, "application/json")
        existing = [{"fileKey": f.get("fileKey")}
                    for f in (envelope.get("成果物") or {}).get("value") or []
                    if f.get("fileKey")]
        await kintone.update_record(APP_SHIPPING, cand.review_record_id,
                                    {"成果物": existing + [{"fileKey": file_key}]})
    except Exception as e:
        return {"status": "aborted",
                "reason": f"監査JSONの保存に失敗したため統合を中止しました"
                          f"（削除・更新なし）: {str(e)[:200]}"}

    # ── 参照付け替え（削除前・親エッジのみ） ─────────────────────────────────
    for plan in repoint_plans:
        await kintone.update_record(
            APP_KOSEKI_PERSON, plan["person_record_id"],
            {f: cand.winner_id for f in plan["fields"]})

    # ── 勝者更新（転記規則＋名寄せ確定=確定〔人の確定操作の結果〕） ──────────
    payload = build_merge_payload(winner, loser)
    payload["名寄せ確定"] = "確定"
    await kintone.update_record(APP_KOSEKI_PERSON, cand.winner_id, payload)

    # ── 敗者削除 → 封筒クローズ ─────────────────────────────────────────────
    await kintone.delete_record(APP_KOSEKI_PERSON, cand.loser_id)
    await kintone.update_record(APP_SHIPPING, cand.review_record_id, {
        "発送ステータス": STATUS_DONE,
        "実行済み": "yes",
    })
    logger.info("[PERSON_MERGE_EXEC] merged winner=No.%s loser=No.%s(deleted) "
                "review=No.%s repointed=%s",
                emit(cand.winner_id, "record_id", "log", "operator"),
                emit(cand.loser_id, "record_id", "log", "operator"),
                emit(cand.review_record_id, "record_id", "log", "operator"),
                emit(len(repoint_plans), "count", "log", "operator"))
    return {"status": "merged", "winner_id": cand.winner_id,
            "loser_id": cand.loser_id, "repointed": repoint_plans,
            "review_record_id": cand.review_record_id}


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
