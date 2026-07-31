"""heir_projection — App36 関所＋projection（P3-003b 実装票・confirmed handler side）

正本: `DRAFT_P3_003B_DESIGN`（設計凍結・D4 PASS・§8A 裁定4件）＋
`DRAFT_P3_003_ENVELOPE_FLOW` §3.2〜3.4（3 phase・H10/H11・stale ガード）。

- **書込み主体は confirmed handler の一本経路のみ**（P3-003B §4B fix3 H01・
  [人]裁定）: confirm を伴わない機械再導出は App36 へ **write 0**
  （insert/update/current 前進すべてなし）。本 module の kintone write は
  `_resolve_heir_derivation`（確定関所）の配下からのみ発行される。
- **3 phase**（ENVELOPE_FLOW §3.2・`_resolve_koseki` 型）:
  phase 1=読取専用の全件検証（封筒再読・run 実在/case 一致・二重確定 DB ガード・
  ATTORNEY_ALLOWLIST・stale・旧 run 判別・胎児停止・全行の写像/冪等キー分類）
  → phase 2=HeirConfirmationDecision INSERT（P3-001 正規経路・run は不改変）
  → phase 3=App36 upsert＋封筒クローズ。
  **phase 1 で 1 行でも要確認があれば全体中止（write 0・部分反映しない）**——
  stale/旧 run/胎児/行異常の検査は decision INSERT より前に置く（確定記録だけが
  残って projection されない宙吊り状態を作らない・安全側の順序）。
- **胎児停止**（裁定3・§2A・[人]明示承認済み）: 1 行でも fetus を含む案件は
  projection 全体停止＋要確認（業務警報は件数・record_id のみ・氏名/ラベル非出力）。
- **旧 run**（P3-001 改定 §3.2）: zokugara_code 欠落 payload は精密 projection
  不可＝要確認（粗い relation_key 写像に頼らない）。判別は
  `payload_has_zokugara_codes`（単一の正）。
- **重複収束**（§5 fix3 M01）: 冪等キー 2 件以上一致は書かず要確認。削除を伴う
  収束の提示は「両行の current が同一 head と確認できた場合の $id 最小 tiebreak」
  に限る。比較不能系は削除ゼロ。**機械は削除しない**（提示のみ・実施は[人]）。
- **PII 規律**: 応答・ログ・警報は件数・record_id・run id のみ
  （氏名・続柄値・payload 値を載せない＝P3-001 非露出契約と同じ規律）。
"""

import logging
import os
import re
from datetime import datetime, timezone

import sqlalchemy as sa

from hub import kintone
from hub.derivation_models import (_PERSON_ID_RE, _SHARE_RE, ZOKUGARA_CODES,
                                   DerivationRun, HeirConfirmationDecision,
                                   create_heir_decision, get_current_head,
                                   payload_has_zokugara_codes)
from hub.heir_envelope import APP_SHIPPING, _unit_for_case
from hub.redact import emit

logger = logging.getLogger("hub.heir_projection")

APP_SOUZOKUNIN = kintone.KintoneApp(
    "App 36 (相続人)", "APP_SOUZOKUNIN", "TOKEN_SOUZOKUNIN")

STATUS_PENDING = "要確認"
STATUS_DONE = "完了"

# ── 写像（P3-003B §3.1 表・凍結9値 enum → App36 続柄 dropdown〔実機10値〕）──
# 単一の正は hub.derivation_models.ZOKUGARA_CODES（独自定義禁止・発注 e）。
# fetus は写像しない（案件全体停止＝裁定3）。dropdown の「受遺者（相続人外）」
# 「その他」は機械写像の対象外（人手入力専用・§3.1 整合は import 時検査で固定）。
ZOKUGARA_CODE_TO_APP36 = {
    "spouse": "配偶者",
    "child": "子",
    "lineal_ascendant": "直系尊属",
    "sibling": "兄弟姉妹",
    "nephew_niece_rep": "甥姪（代襲）",
    "grandchild_rep": "孫（代襲）",
    "further_rep": "再代襲（曾孫等）",
    "successive": "数次承継",
}
if set(ZOKUGARA_CODE_TO_APP36) | {"fetus"} != set(ZOKUGARA_CODES):
    raise AssertionError("写像表が凍結9値 enum と乖離（拡張は正本改定と同時）")

# ── grammar（P3-003B §2 M02・保存層と逐語一致）─────────────────────────────
# current_derivation_run_id: 二段検査——(1) 正の整数・前ゼロなし・最大19桁
# (2) int 化して signed BigInt 上限以下（19桁は regex を通るが int64 超があり得る）
_RUN_ID_RE = re.compile(r"^[1-9][0-9]{0,18}$")
_INT64_MAX = 9223372036854775807
# 導出元人物ID: ^[0-9]{1,10}$ ＝保存層 person_id grammar（_PERSON_ID_RE）と
# 逐語一致（§2 M02・胎児合成 ID は本 field に入らない=胎児案件は停止）
_SOURCE_PERSON_ID_RE = _PERSON_ID_RE
_CASE_RECORD_ID_RE = re.compile(r"^[0-9]{1,10}$")


class ProjectionPolicyError(ValueError):
    """関所境界の検証違反（grammar 不一致・写像不能）。App36 への write は
    発生しない（fail-closed・文言に値を載せない）。"""


def validate_run_id_str(value) -> str:
    """current_derivation_run_id の二段検査（§2 M02）。合格した文字列を返す。"""
    if not isinstance(value, str) or not _RUN_ID_RE.fullmatch(value):
        raise ProjectionPolicyError(
            "current_derivation_run_id は正の整数（前ゼロなし・最大19桁）であること")
    if int(value) > _INT64_MAX:
        raise ProjectionPolicyError(
            "current_derivation_run_id は signed BigInt 上限以下であること"
            "（regex 通過でも数値比較を重ねる・fix2 M02）")
    return value


def share_to_display(share: str) -> str:
    """法定相続分の表記写像（§3.3）: "n/d" → "d分のn"。

    - grammar は保存層（derivation_models._SHARE_RE）をそのまま受ける（逐語一致）。
    - 単独相続 "1/1" → "1分の1"（裁定4・写像規則を分岐させない）。
    - 再約分・四捨五入はしない（保存層の既約分数をそのまま表記）。
    """
    if not isinstance(share, str) or not _SHARE_RE.fullmatch(share):
        raise ProjectionPolicyError(
            "share は保存層 grammar（n/d）に合致すること（値は非反射）")
    n, d = share.split("/")
    return f"{d}分の{n}"


def attorney_allowlist() -> frozenset[str]:
    """ATTORNEY_ALLOWLIST（env・カンマ区切りの弁護士識別集合）。
    未設定＝空集合＝全 confirmed 拒否（fail-closed・正本 §3.4 H11 防御側）。"""
    raw = os.environ.get("ATTORNEY_ALLOWLIST", "")
    return frozenset(x.strip() for x in raw.split(",") if x.strip())


def classify_duplicate_rows(rows: list[dict], head_run_id: str) -> dict:
    """冪等キー重複（2件以上一致）の人手収束分類（§5 fix3 M01・機械は削除しない）。

    - tiebreak: 全行の current_derivation_run_id が同値かつ head run と同一と
      確認できた場合のみ、$id 最小を残す提示（削除の実施は[人]）。
    - hold: それ以外（current が不正・空・別系列・比較不能）は削除・無効化ゼロで
      要確認のまま保持（誤って正しい行を消さない）。
    値は record_id のみ（氏名等は含まない）。
    """
    ids = []
    currents = set()
    for r in rows:
        ids.append(str((r.get("$id") or {}).get("value") or ""))
        currents.add(str((r.get("current_derivation_run_id") or {})
                         .get("value") or "").strip())
    same_head = (len(currents) == 1 and next(iter(currents)) == head_run_id)
    if same_head and all(i.isdigit() for i in ids):
        keep = min(ids, key=int)
        return {"action": "tiebreak", "keep": keep,
                "manual_delete_candidates": sorted(
                    (i for i in ids if i != keep), key=int)}
    return {"action": "hold", "record_ids": sorted(ids)}


async def _alert_business(text: str) -> None:
    """業務チャネル警報（件数・record_id のみ・best-effort）。"""
    from hub import notify
    try:
        await notify.notify_admin_line(text, throttle_key="heir_projection_alert")
    except Exception:
        logger.error("[HEIR-PROJ] business alert failed (fixed classification only)")


def _v(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


# ── DB 読取（read-only・SELECT のみ）────────────────────────────────────────

async def _load_run(run_id: int):
    from hub.db import session_scope
    t = DerivationRun.__table__
    async with session_scope() as s:
        return (await s.execute(
            sa.select(t).where(t.c.id == run_id))).one_or_none()


async def _has_root_decision(run_id: int) -> bool:
    from hub.db import session_scope
    t = HeirConfirmationDecision.__table__
    async with session_scope() as s:
        row = (await s.execute(
            sa.select(t.c.id).where(
                t.c.derivation_run_id == run_id,
                t.c.supersedes_decision_id.is_(None)).limit(1))).first()
        return row is not None


async def _ancestor_ids(run_row) -> set[int]:
    """run の supersedes 連鎖の祖先 id 集合（自身は含まない・read-only）。"""
    from hub.db import session_scope
    t = DerivationRun.__table__
    out: set[int] = set()
    cur = run_row.supersedes_run_id
    async with session_scope() as s:
        while cur is not None and cur not in out and len(out) < 10000:
            out.add(cur)
            row = (await s.execute(
                sa.select(t.c.supersedes_run_id)
                .where(t.c.id == cur))).one_or_none()
            cur = row.supersedes_run_id if row is not None else None
    return out


# ══════════════════════════════════════════════════════════════
# 確定関所（confirmed handler・review_resolve.RESOLVERS "heir_derivation"）
# ══════════════════════════════════════════════════════════════

async def _resolve_heir_derivation(group, case_record_id: str,
                                   decided_by: str = "") -> dict:
    """相続人導出封筒の確定（ENVELOPE_FLOW §3.2 の 3 phase）。

    phase 1（読取専用・1件でも要確認なら全体中止=write 0）→
    phase 2（HCD confirmed を 1 行 INSERT・DerivationRun は不改変）→
    phase 3（App36 upsert〔§4A の書込み表〕＋封筒クローズ）。
    """
    if not _CASE_RECORD_ID_RE.fullmatch(case_record_id or ""):
        return {"status": "aborted",
                "reason": "案件レコードIDが数字列ではありません（書き込みなし）"}
    if decided_by not in attorney_allowlist():
        # 正本 §3.4 H11 防御: allowlist 外の confirmed（=戸籍確認済 yes 遷移を伴う）
        # は拒否。識別子の値は文言に載せない
        return {"status": "aborted",
                "reason": "確定権限がありません（ATTORNEY_ALLOWLIST 外・書き込みなし）"}

    # ── phase 1: 読取専用の全件検証 ─────────────────────────────────────────
    plans = []      # (item, run_row, row_plans)
    for item in group.items:
        record = await kintone.get_record(APP_SHIPPING, item.record_id)
        status, executed = _v(record, "発送ステータス"), _v(record, "実行済み")
        if status != STATUS_PENDING or executed != "no":
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} が要確認ではなくなっています"
                              f"（発送ステータス={status}・実行済み={executed}）。"
                              "グループ全体を中止しました（書き込みなし）"}
        rid_raw = item.detail.get("derivation_run_id")
        try:
            rid_str = validate_run_id_str(str(rid_raw))
        except ProjectionPolicyError:
            return {"status": "aborted",
                    "reason": f"No.{item.record_id} の derivation_run_id が"
                              "不正です（grammar 外・書き込みなし）"}
        run = await _load_run(int(rid_str))
        if run is None or str(run.case_record_id) != case_record_id \
                or run.status not in ("derived", "held"):
            return {"status": "aborted",
                    "reason": f"run #{rid_str} が確定対象ではありません"
                              "（案件不一致または対象外 status・書き込みなし）"}
        # 二重確定ガード（DB 側・uq_heir_decision_single_root と重畳）
        if await _has_root_decision(run.id):
            return {"status": "aborted",
                    "reason": f"run #{run.id} は確定済みです（二重確定を防止・"
                              "書き込みなし）"}
        # stale ガード（§3.3・decision INSERT より前＝宙吊り確定を作らない）
        head = await get_current_head(case_record_id)
        if head is None or head.id != run.id:
            return {"status": "aborted",
                    "reason": f"run #{run.id} は最新ではありません（新しい導出が"
                              "あります）。新しい封筒から確定してください"
                              "（書き込みなし）"}
        payload = run.result_payload or {}
        heirs = payload.get("heirs") or []
        # 旧 run（zokugara_code 欠落）＝精密 projection 不可（P3-001 §3.2）
        if not payload_has_zokugara_codes(payload):
            return {"status": "aborted",
                    "reason": f"run #{run.id} は続柄区分コードを持たない旧形式の"
                              "ため精密反映できません（要確認・書き込みなし）。"
                              "再導出してから確定してください"}
        # 胎児停止（裁定3・§2A・[人]明示承認済み）: 1 行でも含めば全体停止
        fetus_rows = [h for h in heirs
                      if h.get("zokugara_code") == "fetus"
                      or str(h.get("person_id") or "").startswith("胎児:")]
        if fetus_rows:
            await _alert_business(
                "【相続人反映: 胎児案件のため停止】\n"
                f"案件 No.{case_record_id} / run #{run.id} / "
                f"胎児行 {len(fetus_rows)} 件\n"
                "App36 への反映を全体停止しました（民法886条・出生による実人物化"
                "後に再導出してください）")
            return {"status": "aborted",
                    "reason": f"胎児を含む案件のため App36 反映を全体停止しました"
                              f"（胎児行 {len(fetus_rows)} 件・部分反映はしません・"
                              "書き込みなし）"}
        # 行計画（写像・冪等キー検索・6状態分類。すべて読取専用）
        try:
            ancestors = await _ancestor_ids(run)
        except Exception:
            # 祖先確認中の DB 不達＝判定不能 → write 0・要確認（§5 状態表）
            return {"status": "aborted",
                    "reason": "run 系列の照会に失敗しました（判定不能のため"
                              "書き込みなし・再指示で再試行できます）"}
        row_plans = []
        for h in heirs:
            pid = str(h.get("person_id") or "")
            if not _SOURCE_PERSON_ID_RE.fullmatch(pid):
                return {"status": "aborted",
                        "reason": "導出元人物ID が grammar 外です"
                                  "（書き込みなし・値は表示しません）"}
            code = h.get("zokugara_code")
            zoku = ZOKUGARA_CODE_TO_APP36.get(code)
            if zoku is None:
                return {"status": "aborted",
                        "reason": "続柄区分コードが写像表にありません"
                                  "（書き込みなし・値は表示しません）"}
            share = h.get("share")
            try:
                share_disp = (share_to_display(share)
                              if share is not None else None)
            except ProjectionPolicyError:
                return {"status": "aborted",
                        "reason": "法定相続分が保存層 grammar 外です"
                                  "（書き込みなし・値は表示しません）"}
            rows = await kintone.search_records(
                APP_SOUZOKUNIN,
                f'案件レコードID = "{case_record_id}" and '
                f'導出元人物ID = "{pid}" order by $id asc limit 2',
                fields=["$id", "current_derivation_run_id", "戸籍確認済"])
            if len(rows) >= 2:
                cls = classify_duplicate_rows(rows, str(run.id))
                await _alert_business(
                    "【相続人反映: 冪等キー重複】\n"
                    f"案件 No.{case_record_id} / 対象行 {len(rows)} 件 / "
                    f"収束分類: {cls['action']}\n"
                    "書き込みは行っていません（収束は人手手順・機械は削除しません）")
                return {"status": "aborted",
                        "reason": f"App36 に重複行があります（{len(rows)} 件・"
                                  f"収束分類={cls['action']}・書き込みなし）。"
                                  "人手手順で収束後に再確定してください"}
            if not rows:
                row_plans.append(("insert", pid, zoku, share_disp, None, None))
                continue
            row = rows[0]
            app36_id = _v(row, "$id")
            cur = _v(row, "current_derivation_run_id")
            kakunin = _v(row, "戸籍確認済")
            if cur == str(run.id):
                row_plans.append(("update", pid, zoku, share_disp,
                                  app36_id, kakunin))       # 冪等ヒット＝§4A update
            elif not cur or not _RUN_ID_RE.fullmatch(cur) \
                    or int(cur) > _INT64_MAX:
                await _alert_business(
                    "【相続人反映: current 不正/空の既存行】\n"
                    f"案件 No.{case_record_id} / App36 No.{app36_id}\n"
                    "書き込みは行っていません（backfill 前提と接続・要確認）")
                return {"status": "aborted",
                        "reason": f"App36 No.{app36_id} の current が空または"
                                  "不正です（要確認・書き込みなし）"}
            elif int(cur) in ancestors:
                row_plans.append(("update", pid, zoku, share_disp,
                                  app36_id, kakunin))       # H10: 祖先→current 前進
            else:
                # 無関係 run（祖先でも自身でもない・別系列）。子孫 run は stale
                # ガードで本表前に弾かれている（run=head 確認済み）
                await _alert_business(
                    "【相続人反映: 別系列の run が既存】\n"
                    f"案件 No.{case_record_id} / App36 No.{app36_id}\n"
                    "書き込みは行っていません（要確認）")
                return {"status": "aborted",
                        "reason": f"App36 No.{app36_id} は別系列の導出に紐付いて"
                                  "います（要確認・書き込みなし）"}
        plans.append((item, run, row_plans))

    # ── phase 2: HCD confirmed の追記（P3-001 正規経路・run は不改変）──────────
    for item, run, _rp in plans:
        await create_heir_decision(
            derivation_run_id=run.id, decision="confirmed",
            decided_by=decided_by, decided_at=datetime.now(timezone.utc))

    # ── phase 3: App36 upsert（§4A 書込み表）＋封筒クローズ ────────────────────
    results = []
    for item, run, row_plans in plans:
        unit = _unit_for_case(str(run.case_app_id))
        inserted, updated = 0, 0
        for op, pid, zoku, share_disp, app36_id, kakunin in row_plans:
            fields = {
                "続柄": zoku,
                "データ源": "戸籍読解",
                "current_derivation_run_id": validate_run_id_str(str(run.id)),
                "導出元人物ID": pid,
            }
            if share_disp is not None:
                fields["法定相続分"] = share_disp   # share=None は書かない（§3.3）
            if op == "insert":
                fields.update({
                    "案件アプリID": str(run.case_app_id),
                    "案件レコードID": case_record_id,
                    "ユニット種別": unit,
                    "戸籍確認済": "yes",   # §4A: confirmed handler は yes を書ける
                })
                await kintone.create_record(APP_SOUZOKUNIN, fields)
                inserted += 1
            else:
                if kakunin == "no":
                    fields["戸籍確認済"] = "yes"   # no→yes のみ（yes→no は禁止・
                                                   # yes 既存時は field 自体含めない）
                await kintone.update_record(APP_SOUZOKUNIN, app36_id, fields)
                updated += 1
        await kintone.update_record(APP_SHIPPING, item.record_id, {
            "発送ステータス": STATUS_DONE,
            "実行済み": "yes",
        })
        results.append({"review_record_id": item.record_id,
                        "derivation_run_id": run.id,
                        "app36_inserted": inserted, "app36_updated": updated})
        logger.info("[HEIR-PROJ] projected review=No.%s run=%s ins=%s upd=%s "
                    "case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(str(run.id), "record_id", "log", "operator"),
                    emit(inserted, "count", "log", "operator"),
                    emit(updated, "count", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
    return {"status": "resolved", "case_record_id": case_record_id,
            "items": results}
