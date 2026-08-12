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
- **再開可能 projection（fix2 M02・設計改定 §9-v2）**: root decision 既存でも
  run が head かつ封筒未クローズなら中止せず、decision 作成をスキップして
  phase 3 のみ再実行（直前再検証＋same-run 冪等ヒットにより再実行安全）。
  封筒クローズは **held=0 の場合のみ**——held>0 は封筒を要確認のまま残し
  （耐久可視性は App30 キュー）、detail へ保留人物 record ID を追記。
  phase 2 はグループ全 item を単一 txn で一括 CAS＋一括 INSERT（fix2 H01-R2・
  途中失敗は全体 rollback＝decision 含む write 0・同一 run は 1 decision に排除）。
- **PII 規律**: 応答・ログ・警報は件数・record_id・run id のみ
  （氏名・続柄値・payload 値を載せない＝P3-001 非露出契約と同じ規律）。
"""

import json
import logging
import os
import re
from datetime import datetime, timezone

import sqlalchemy as sa

from hub import kintone
from hub.app36_validity import CANCELLED_FIELD, filter_active_heir_rows
from hub.derivation_models import (_PERSON_ID_RE, _SHARE_RE, DECISIONS,
                                   ZOKUGARA_CODES, ChainIntegrityError,
                                   DecisionBlockedError,
                                   DecisionChainCorruptionError, DerivationRun,
                                   HeirConfirmationDecision,
                                   append_projection_log,
                                   create_decisions_for_heads,
                                   get_current_head, get_leaf_decision,
                                   load_write_set,
                                   payload_has_zokugara_codes)
from hub.heir_envelope import (APP_SHIPPING, DETAIL_DECISION_KEY,
                               DETAIL_HELD_PERSONS_KEY, _unit_for_case)
from hub.person_validity import APP_KOSEKI_PERSON, is_active_person
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


# ── P3-003c: §3.2-v2 中止セルの固定応答（値非搭載）─────────────────────────
_BLOCKED_REASONS = {
    "already_rejected": "否認済みです。再導出してから確定してください（書き込みなし）",
    "already_confirmed": "確定済みです（取消は別途・書き込みなし）",
}


def _decision_note(decision: str, decided_at) -> dict:
    """封筒 detail の判断注記値（P3-003c §4）。decided_at は decision の保存値
    （再適用時も leaf の保存値＝時刻を上書きしない・§12 M01）。decided_by は
    載せない（PII 最小化）。

    ISO 表記は UTC へ正準化する——保存層（sqlite テスト）は naive（UTC）で
    復元されるため、初回適用（tz-aware）と再適用（DB 復元値）で文字列が
    揺れないよう単一形へ寄せる（冪等再適用の同値性・§4.1）。"""
    if hasattr(decided_at, "isoformat"):
        dt = decided_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)   # 保存層は UTC（naive 復元）
        iso = dt.astimezone(timezone.utc).isoformat()
    else:
        iso = str(decided_at or "")
    return {"decision": decision, "decided_at": iso}


def _live_detail_base(record: dict, item) -> dict:
    """detail 再構築の基底（P3-003c fix1 H01）。

    **phase 1 の App30 再読で取得した現在のチャネル固有データを基底**とし、
    pending 時の ReviewItem.detail スナップショットは再読に detail が無い場合
    （legacy/テスト封筒）の縮退のみに用いる。これにより pending 承認の間に
    他経路（row-held 追記・別 decision の注記等）が detail へ追記したキーを
    上書きで失わない——当該 decision が書くキーだけを基底へ重ねる規則。
    """
    raw = _v(record, "チャネル固有データ")
    if raw:
        try:
            inner = json.loads(raw).get("heir_derivation")
            if isinstance(inner, dict):
                return dict(inner)
        except (ValueError, TypeError, AttributeError):
            pass   # 解釈不能は縮退（snapshot 基底・既存挙動へ収束）
    return dict(item.detail)


def _decision_side_effect_fields(base_detail: dict, decision: str,
                                 info: dict) -> dict:
    """held/rejected の App30 単一 update に載せる field 集合（P3-003c §4.1・
    §12 M01・kintone 呼出しは行わない=一本経路 pin の配下は呼出し元）。

    判断注記（キー 判断）のみを **base_detail（=App30 再読の現在 detail・
    fix1 H01）** へ上書きし、他の既存キーは全保持。rejected のみ 完了/実行済み
    yes クローズ。held は封筒 open 維持（クローズ系 field を含めない）。冪等＝
    noop 時は leaf の保存 decided_at を用いるため内容も不変（§4.1）。
    """
    detail = dict(base_detail)   # 再読基底の既存キー（保留人物ID 等）全保持
    detail[DETAIL_DECISION_KEY] = _decision_note(
        decision, info.get("decided_at"))
    fields = {"チャネル固有データ": json.dumps({"heir_derivation": detail},
                                               ensure_ascii=False)}
    if decision == "rejected":
        fields["発送ステータス"] = STATUS_DONE
        fields["実行済み"] = "yes"
    return fields


async def _alert_chain_corruption(case_record_id: str, exc) -> None:
    """一本鎖破損の業務警報（fix1 M02・固定分類・件数と ID のみ・値非搭載）。

    正常系の並行 race（CAS 不一致・UNIQUE 競合の正規化＝素の
    ChainIntegrityError）では呼ばない——破損（データ異常）だけを可視化する。
    """
    await _alert_business(
        "【相続人判断: decision 鎖の破損検出】\n"
        f"案件 No.{case_record_id} / run #{exc.run_id} / "
        f"有効 leaf {exc.count} 件\n"
        "一本鎖でない decision 鎖を検出しました（書き込みなし・人手調査要）")
    logger.error("[HEIR-PROJ] decision-chain corruption detected "
                 "(fixed classification only)")


# ══════════════════════════════════════════════════════════════
# 確定関所（confirmed handler・review_resolve.RESOLVERS "heir_derivation"）
# ══════════════════════════════════════════════════════════════

async def _resolve_heir_derivation(group, case_record_id: str,
                                   decided_by: str = "",
                                   decision: str = "confirmed") -> dict:
    """相続人導出封筒の確定・保留・否認（ENVELOPE_FLOW §3.2＋P3-003c 凍結仕様）。

    confirmed: phase 1（読取専用・1件でも要確認なら全体中止=write 0）→
    phase 2（HCD confirmed を 1 行 INSERT・DerivationRun は不改変）→
    phase 3（App36 upsert〔§4A の書込み表〕＋封筒クローズ）。

    held/rejected（P3-003c §6・M01 の分岐位置）: gate 系検証（封筒再読・run 検証・
    head 確認・leaf 判定・allowlist）の後・**App36 row-plan 構築（search 含む）の前**
    に分岐＝App36 への照会は構造上ゼロ。decision 記録（§3.3-v2 leaf 判定 txn）→
    App30 の**単一 update 一括**で判断注記＋（rejected のみ）クローズ（§4.1・§12 M01）。
    projection 系検査（胎児・写像・旧 payload・冪等 search）は confirmed のみ。
    """
    if decision not in DECISIONS:
        return {"status": "aborted",
                "reason": "不明な判断種別です（書き込みなし）"}
    if not _CASE_RECORD_ID_RE.fullmatch(case_record_id or ""):
        return {"status": "aborted",
                "reason": "案件レコードIDが数字列ではありません（書き込みなし）"}
    if decided_by not in attorney_allowlist():
        # 正本 §3.4 H11 防御＋P3-003c 裁定①=(A): 3 decision 対称に allowlist 検証。
        # 識別子の値は文言に載せない
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
        # fix2 M02（設計改定 §9-v2）: root decision 既存はここでは中止しない——
        # run が依然 head（下の stale ガード）かつ封筒が未クローズ（上の再読で
        # 確認済み）なら**再開経路**（decision 作成をスキップして projection のみ
        # 再実行）。二重確定ガードの意味は「同一 run への新しい root decision の
        # 重複作成を防ぐ」に限定し、phase 2 の一括 CAS 関数内で強制する
        # stale ガード（§3.3・decision INSERT より前＝宙吊り確定を作らない）
        head = await get_current_head(case_record_id)
        if head is None or head.id != run.id:
            return {"status": "aborted",
                    "reason": f"run #{run.id} は最新ではありません（新しい導出が"
                              "あります）。新しい封筒から確定してください"
                              "（書き込みなし）"}
        # P3-003c §6: 有効 leaf の先行検査（gate 系・3 decision 共通・read-only）。
        # §3.2-v2 の中止セルを App36 row-plan 構築より**前**に遮断する（App36
        # 照会ゼロ・§7-3）。正式な判定と INSERT は phase 2 の単一 txn が再実施
        # （本検査は先行遮断・txn 側の CAS/leaf 判定が正）
        try:
            leaf = await get_leaf_decision(run.id)
        except DecisionChainCorruptionError as e:
            # fix1 M02: 破損（データ異常）は業務警報＋固定分類ログ。
            # 正常系の並行 race とは型で区別（race はここに来ない）
            await _alert_chain_corruption(case_record_id, e)
            return {"status": "aborted",
                    "reason": "判断記録の整合が取れません"
                              "（破損検出・書き込みなし）"}
        except ChainIntegrityError:
            return {"status": "aborted",
                    "reason": "判断記録の整合が取れません"
                              "（破損検出・書き込みなし）"}
        if leaf is not None:
            if leaf.decision == "rejected" and decision != "rejected":
                return {"status": "aborted",
                        "reason": _BLOCKED_REASONS["already_rejected"]}
            if leaf.decision == "confirmed" and decision != "confirmed":
                return {"status": "aborted",
                        "reason": _BLOCKED_REASONS["already_confirmed"]}
        # ── P3-003c §6 M01: held/rejected はここで分岐（gate 系のみ・以降の
        #    projection 系検査〔胎児・写像・旧 payload・App36 search〕へ進まない
        #    ＝App36 照会ゼロを分岐位置で構造保証）──────────────────────────────
        if decision != "confirmed":
            plans.append((item, run, None, None, _live_detail_base(record, item)))
            continue
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
                fields=["$id", "current_derivation_run_id", "戸籍確認済",
                        CANCELLED_FIELD])
            # P3-003C-CANCEL §4.2: 取消済み行は読み飛ばし（共通 filter・単一の正）
            rows = filter_active_heir_rows(rows)
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
                row_plans.append((pid, zoku, share_disp))
                continue
            row = rows[0]
            app36_id = _v(row, "$id")
            cur = _v(row, "current_derivation_run_id")
            if cur == str(run.id):
                row_plans.append((pid, zoku, share_disp))   # 冪等ヒット＝§4A update
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
                row_plans.append((pid, zoku, share_disp))   # H10: 祖先→current 前進
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
        plans.append((item, run, ancestors, row_plans,
                      _live_detail_base(record, item)))

    # ── phase 2: HCD decision の一括追記（fix2 H01-R2: グループ全 item を単一
    #    DB トランザクションで一括 CAS→一括 INSERT。途中失敗は全体 rollback＝
    #    decision 含む write 0。同一 run 参照の複数封筒は 1 decision に重複排除。
    #    P3-003c §3.3-v2: 有効 leaf 判定（0/1/複数の fail-closed）＋§3.2-v2 遷移表。
    #    leaf=confirmed×確定は "resumed"＝INSERT せず phase 3 のみ再実行）──────────
    try:
        decisions = await create_decisions_for_heads(
            case_record_id, [run.id for _i, run, _a, _rp, _d in plans],
            decision=decision, decided_by=decided_by,
            decided_at=datetime.now(timezone.utc))
    except DecisionBlockedError as e:
        # §3.2-v2 の中止セル（固定文言・値非搭載・全体 rollback 済み）
        return {"status": "aborted", "reason": _BLOCKED_REASONS[e.code]}
    except DecisionChainCorruptionError as e:
        # fix1 M02: txn 内検出の破損も警報（race 正規化とは型で区別・警報なし側は
        # 下の ChainIntegrityError 節）
        await _alert_chain_corruption(case_record_id, e)
        return {"status": "aborted",
                "reason": "判断記録の整合が取れません（破損検出・書き込みなし）"}
    except ChainIntegrityError:
        return {"status": "aborted",
                "reason": "確定中に前提が変化しました（supersede を検出・グループ"
                          "全体を中止・書き込みなし）。最新の封筒から確定し直して"
                          "ください"}

    # ── held/rejected: App30 side effect のみ（§4.1・§12 M01・App36 到達なし。
    #    封筒ごとに update_record **単一呼出し一括**・write は一本経路 pin の
    #    とおり本関数配下）─────────────────────────────────────────────────────
    if decision != "confirmed":
        results = []
        for item, run, _ancestors, _row_plans, live_detail in plans:
            info = decisions.get(run.id) or {}
            await kintone.update_record(
                APP_SHIPPING, item.record_id,
                _decision_side_effect_fields(live_detail, decision, info))
            results.append({"review_record_id": item.record_id,
                            "derivation_run_id": run.id,
                            "decision_outcome": info.get("outcome", "")})
            # decision は分岐確定済みのため format 文字列へリテラルで焼き込む
            # （sink 検査: logger 引数は emit() 経由のみ・変数直渡しをしない）
            if decision == "held":
                logger.info(
                    "[HEIR-PROJ] decision=held review=No.%s run=%s case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(str(run.id), "record_id", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
            else:
                logger.info(
                    "[HEIR-PROJ] decision=rejected review=No.%s run=%s case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(str(run.id), "record_id", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
        return {"status": "resolved", "decision": decision,
                "case_record_id": case_record_id, "items": results}

    # ── phase 3: App36 upsert（§4A 書込み表・fix1 H01: 各行 write 直前の再検証
    #    〔冪等キー再検索＋H10 は revision 楽観ロック〕・要確認行はスキップ継続）──
    #    fix2 M02（設計改定 §9-v2・fix1 の「held でもクローズ」は撤回）:
    #    封筒クローズは **held=0 かつ全行 projection 完了の場合のみ**。held>0 は
    #    封筒を要確認のまま残し（耐久可視性は App30 キュー）、封筒 detail へ
    #    保留行の人物 record ID（数字のみ・PII 非搭載）を追記。収束後に**同じ封筒を
    #    再確定**すると再開経路（resumed）で残り行だけが再反映される
    results = []
    for item, run, ancestors, row_plans, live_detail in plans:
        unit = _unit_for_case(str(run.case_app_id))
        counts = {"inserted": 0, "updated": 0, "held": 0}
        held_pids: list[str] = []
        for pid, zoku, share_disp in row_plans:
            outcome = await _project_row(run, case_record_id, unit, pid, zoku,
                                         share_disp, ancestors)
            counts[outcome] += 1
            if outcome == "held":
                held_pids.append(pid)
        closed = counts["held"] == 0
        # P3-003c §4 M03: held→confirmed の supersede 後は判断注記を confirmed へ
        # **更新**（除去しない・decided_at は decision の保存値）。注記が無い封筒
        # （held を経ない通常確定）には追記しない（既存挙動不変）。
        # fix1 H01: 基底は App30 再読の現在 detail（snapshot でなく）
        detail = dict(live_detail)
        annotate = DETAIL_DECISION_KEY in detail
        if annotate:
            info = decisions.get(run.id) or {}
            detail[DETAIL_DECISION_KEY] = _decision_note(
                "confirmed", info.get("decided_at"))
        if closed:
            # クローズ＋注記更新は単一 update の一括（§12 M01 と同型・呼出し 1 回）
            fields = {"発送ステータス": STATUS_DONE, "実行済み": "yes"}
            if annotate:
                fields["チャネル固有データ"] = json.dumps(
                    {"heir_derivation": detail}, ensure_ascii=False)
            await kintone.update_record(APP_SHIPPING, item.record_id, fields)
        else:
            # held の耐久可視性: 封筒は要確認のまま・detail へ保留人物 ID を追記
            # （操作者が対象行を特定できる・値は App34 person record ID の数字のみ。
            # 判断注記（confirmed 更新）とは別キー併存＝M03）
            detail[DETAIL_HELD_PERSONS_KEY] = held_pids
            await kintone.update_record(APP_SHIPPING, item.record_id, {
                "チャネル固有データ": json.dumps({"heir_derivation": detail},
                                                 ensure_ascii=False),
            })
        results.append({"review_record_id": item.record_id,
                        "derivation_run_id": run.id,
                        "app36_inserted": counts["inserted"],
                        "app36_updated": counts["updated"],
                        "app36_held": counts["held"],
                        "envelope_closed": closed})
        logger.info("[HEIR-PROJ] projected review=No.%s run=%s ins=%s upd=%s "
                    "held=%s closed=%s case=%s",
                    emit(item.record_id, "record_id", "log", "operator"),
                    emit(str(run.id), "record_id", "log", "operator"),
                    emit(counts["inserted"], "count", "log", "operator"),
                    emit(counts["updated"], "count", "log", "operator"),
                    emit(counts["held"], "count", "log", "operator"),
                    emit(1 if closed else 0, "count", "log", "operator"),
                    emit(case_record_id, "record_id", "log", "operator"))
    return {"status": "resolved", "case_record_id": case_record_id,
            "items": results}


async def _source_person_inactive(pid: str) -> bool:
    """RV-08 RV08-03「直接 get の状態確認」規約: App34 env 設定時のみ検査
    （optional・lazy 原則＝未設定環境では従来挙動不変）。無効化行
    （統合済み無効）と取得不能（レコード不在等）は True＝呼出し元が当該行を
    要確認へ倒す（fail-closed・盲目 projection しない）。"""
    if not (APP_KOSEKI_PERSON.app_id() and APP_KOSEKI_PERSON.token()):
        return False
    try:
        person = await kintone.get_record(APP_KOSEKI_PERSON, pid)
    except kintone.KintoneError:
        return True
    return not is_active_person(person)


async def _project_row(run, case_record_id: str, unit: str, pid: str,
                       zoku: str, share_disp, ancestors: set[int]) -> str:
    """1 行の App36 upsert（fix1 H01・R-P3-003B-IMPL-1: write 直前の再検証つき）。

    - **insert 前再検索**: 冪等キー完全一致を write 直前に再実行し、1件以上
      出現していれば盲目 insert しない（当該行 held＝要確認・警報）。
    - **H10 update は revision 楽観ロック**（App30 状態機械で確立済みの型・
      hub.kintone.update_record(revision=...)）。競合（KintoneConflict）＝
      他プロセスが先に更新＝当該行 held。
    - **残余（設計受容・§2.2）**: 再検索〜create の微小窓で並行 insert が成立
      し得る（kintone に条件付き create は無い）。この残余は既実装の重複収束
      （§5 fix3 M01: 冪等キー2件以上→書かず要確認・同一 head 限定 tiebreak
      〔$id 最小・提示のみ〕・比較不能系は削除ゼロ）が事後回収する。
    - 戻り値: "inserted" | "updated" | "held"（held＝当該行 write 0）。
    - **RV-08 §10.2(ii)**: 導出元人物（App34）が soft merge で無効化されていれば
      当該行は held（要確認・write 0）。resumed 経路の保留人物再検証もここを通る
      （封筒 detail の書き換えはしない＝履歴改変禁止・既存 held 機構のみ使用）。
    """
    if await _source_person_inactive(pid):
        await _alert_business(
            "【相続人反映: 導出元人物が無効化済み】\n"
            f"案件 No.{case_record_id} / 人物 No.{pid}\n"
            "当該行は書き込まず要確認としました（soft merge の無効化行・"
            "再導出→確定が正規経路です）")
        return "held"
    rows = await kintone.search_records(
        APP_SOUZOKUNIN,
        f'案件レコードID = "{case_record_id}" and '
        f'導出元人物ID = "{pid}" order by $id asc limit 2',
        # P3-003C-CANCEL §4.1a: 書込み対象 field の現在値も取得し preimage を
        # write-set へ保存する（$revision 楽観ロックと同一読取＝preimage の
        # 整合は revision が担保）。取消済み filter 用に CANCELLED_FIELD も取得
        fields=["$id", "current_derivation_run_id", "戸籍確認済", "$revision",
                "続柄", "データ源", "法定相続分", "導出元人物ID",
                CANCELLED_FIELD])
    # P3-003C-CANCEL §4.2: 取消済み行は読み飛ばし（共通 filter・単一の正）
    rows = filter_active_heir_rows(rows)
    fields = {
        "続柄": zoku,
        "データ源": "戸籍読解",
        "current_derivation_run_id": validate_run_id_str(str(run.id)),
        "導出元人物ID": pid,
    }
    if share_disp is not None:
        fields["法定相続分"] = share_disp       # share=None は書かない（§3.3）
    # CANCEL-IMPL-01: 当該行の既存 log（回収用・id 昇順）。record_id 一致に
    # 加え「app36_record_id 未確定の pending insert」（completed 欠落=ACK 喪失・
    # create 前クラッシュ）を 導出元人物ID で対応付ける
    run_logs = [l for l in await load_write_set(run.id)
                if l["fields_written"].get("導出元人物ID") == pid]
    if not rows:
        fields.update({
            "案件アプリID": str(run.case_app_id),
            "案件レコードID": case_record_id,
            "ユニット種別": unit,
            "戸籍確認済": "yes",       # §4A: confirmed handler は yes を書ける
        })
        # CANCEL-IMPL-01（先行保存）: App36 書込み**前**に pending を保存。
        # 保存失敗は伝播＝当該行 write 0（write-set の無い書込みを作らず、
        # 後から write-set を再構成＝誤生成もしない）。既存 pending（create 前
        # クラッシュの残置）は再利用＝二重追記しない
        if not any(l["op"] == "insert" and not l["app36_record_id"]
                   for l in run_logs):
            await append_projection_log(
                derivation_run_id=run.id, case_record_id=case_record_id,
                app36_record_id="", op="insert", stage="pending",
                fields_written=fields, preimage={})
        new_id = await kintone.create_record(APP_SOUZOKUNIN, fields)
        # 裁定⑤: 書込み成功後の完了追記。失敗（ACK 喪失）は pending が真実を
        # 保持＝resumed 再確定の回収経路が完了を追記する（§4.4）
        await append_projection_log(
            derivation_run_id=run.id, case_record_id=case_record_id,
            app36_record_id=str(new_id), op="insert", stage="completed",
            fields_written=fields, preimage={})
        return "inserted"
    if len(rows) >= 2:
        cls = classify_duplicate_rows(rows, str(run.id))
        await _alert_business(
            "【相続人反映: 直前再検証で冪等キー重複】\n"
            f"案件 No.{case_record_id} / 対象行 {len(rows)} 件 / "
            f"収束分類: {cls['action']}\n"
            "当該行は書き込まず要確認としました（収束は人手手順）")
        return "held"
    row = rows[0]
    app36_id = _v(row, "$id")
    cur = _v(row, "current_derivation_run_id")
    same = cur == str(run.id)
    ancestor = (not same and _RUN_ID_RE.fullmatch(cur or "") is not None
                and int(cur) <= _INT64_MAX and int(cur) in ancestors)
    if not (same or ancestor):
        # phase 1 検証後に行状態が変化（insert 競合・current 不正化・別系列化）
        await _alert_business(
            "【相続人反映: 直前再検証で行状態が変化】\n"
            f"案件 No.{case_record_id} / App36 No.{app36_id}\n"
            "当該行は書き込まず要確認としました")
        return "held"
    if _v(row, "戸籍確認済") == "no":
        fields["戸籍確認済"] = "yes"   # no→yes のみ（yes 既存時は field 自体含めない）
    # CANCEL-IMPL-01（回収・再構成しない）: 当該行の log が既にあれば元の
    # op/preimage をそのまま使う——再確定（resumed）時に「書込み後の現在値」を
    # preimage として誤保存しない（ACK 喪失＝completed 欠落でも pending の
    # 真正 preimage が正）。無ければ本 write が初回＝pending を先行保存
    orig = [l for l in run_logs
            if l["app36_record_id"] == app36_id or not l["app36_record_id"]]
    if orig:
        op_orig = orig[0]["op"]
        pre_orig = dict(orig[0]["preimage"])
    else:
        op_orig = "update"
        pre_orig = {code: _v(row, code) for code in fields}
        await append_projection_log(
            derivation_run_id=run.id, case_record_id=case_record_id,
            app36_record_id=app36_id, op="update", stage="pending",
            fields_written=fields, preimage=pre_orig)
    try:
        await kintone.update_record(APP_SOUZOKUNIN, app36_id, fields,
                                    revision=_v(row, "$revision") or None)
    except kintone.KintoneConflict:
        await _alert_business(
            "【相続人反映: revision 競合】\n"
            f"案件 No.{case_record_id} / App36 No.{app36_id}\n"
            "他プロセスが先に更新しました。当該行は書き込まず要確認としました")
        return "held"
    # 裁定⑤: 完了追記（op/preimage は最初の log の値＝真実を引き継ぐ。
    # revision 楽観ロック成功が「読取後に他更新なし」を担保）
    await append_projection_log(
        derivation_run_id=run.id, case_record_id=case_record_id,
        app36_record_id=app36_id, op=op_orig, stage="completed",
        fields_written=fields, preimage=pre_orig)
    return "updated"
