"""heir_envelope — DerivationRun→App30 要確認封筒の起票（P3-003a）

正本: `docs/design-drafts/DRAFT_P3_003_ENVELOPE_FLOW.md` §2（凍結・実装3分割の第1票）。

- 対象: 確定待ちの DerivationRun（status='derived' または 'held'）。'error' は
  起票しない（§2.1・導出失敗は人手経路）。
- 封筒: App30 に `発送ステータス:"要確認"`・`実行済み:"no"` で起票
  （person_merge._file_candidate 同型）。チャネル/方向は既存語彙
  「スキャン受領/受領」を踏襲——専用チャネル値の新設は kintone フィールド変更
  （[人]・BLOCKED）が要るため初版では行わない。
- 冪等キー: `heir_derivation:{case_record_id}:{input_hash}`（§2.2）。起票前に
  `チャネル固有データ like` 検索で二重封筒を遮断（app30_filer.find_existing 同型・
  第2層）。同一入力の再導出は既存封筒を再利用（新規起票しない）。
- **単票 API（create_record）必須**（§1.4 実機根拠: 一括 API は kintone
  「レコード追加」Webhook が発射されない）。
- detail は **run への参照のみ**（§2.1: result_payload 本体は封筒へ複製しない。
  PII 非混入を _DETAIL_KEYS の閉集合で構造的に担保）。
- flag `HEIR_DERIVATION_ENABLED`（既定 OFF）: OFF では一切起票しない。
- **結線点（§2.1）**: 導出コマンド（指示Bot語彙「相続人を導出して」）の完了直後に
  `file_heir_envelope(run)` を同期呼出しする。導出コマンド自体は**未実装**
  （別票・DRAFT §5 の 2026-07-27 追記参照）。
"""

import json
import logging
import os

from hub import kintone
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式）

logger = logging.getLogger("hub.heir_envelope")

APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

_UNIT = "相続一般"
_FLAG = "HEIR_DERIVATION_ENABLED"

# detail に載せてよいキーの閉集合（§2.1・run 参照のみ＝PII/payload 本体を持たない）
_DETAIL_KEYS = frozenset({
    "derivation_run_id", "case_record_id", "input_hash",
    "result_hash", "provisional", "lawyer_flags", "冪等キー",
})


class EnvelopeDetailPolicyError(ValueError):
    """detail が閉集合外のキーを持つ（PII/payload 混入の芽・保存前に拒否）。"""


def heir_derivation_enabled() -> bool:
    """flag HEIR_DERIVATION_ENABLED（既定 OFF・値集合は durable 系と同一流儀）。"""
    return os.environ.get(_FLAG, "").strip().lower() in ("1", "true", "on", "yes")


def idempotency_key(case_record_id: str, input_hash: str) -> str:
    """冪等キー（§2.2・detail に平文で保持し like 検索の対象にする）。"""
    return f"heir_derivation:{case_record_id}:{input_hash}"


async def find_existing(case_record_id: str, input_hash: str) -> str | None:
    """同一冪等キーの起票済み封筒を検索（二重封筒防止の第2層・app30_filer 同型）。"""
    key = idempotency_key(case_record_id, input_hash)
    records = await kintone.search_records(
        APP_SHIPPING, f'チャネル固有データ like "{key}"', fields=["$id"])
    if records:
        return str(records[0].get("$id", {}).get("value", ""))
    return None


def _build_detail(run) -> dict:
    detail = {
        "derivation_run_id": run.id,
        "case_record_id": run.case_record_id,
        "input_hash": run.input_hash,
        "result_hash": run.result_hash,
        "provisional": bool(run.provisional),
        "lawyer_flags": run.lawyer_flags,
        "冪等キー": idempotency_key(run.case_record_id, run.input_hash),
    }
    if set(detail) != set(_DETAIL_KEYS):   # 閉集合の構造ガード（拡張は正本改定と同時）
        raise EnvelopeDetailPolicyError(
            "envelope detail は _DETAIL_KEYS の閉集合のみ（PII/payload 非混入・§2.1）")
    return detail


async def file_heir_envelope(run) -> dict:
    """DerivationRun の要確認封筒を App30 へ起票する（導出完了直後の結線点・§2.1）。

    run: DerivationRun（ORM 行または同属性のオブジェクト。参照する属性は
         id / case_app_id / case_record_id / input_hash / result_hash /
         status / provisional / lawyer_flags のみ＝result_payload は読まない）。
    Returns: {"status": "filed"|"already_filed"|"disabled"|"not_target",
              "record_id": str|None}
    """
    if not heir_derivation_enabled():
        return {"status": "disabled", "record_id": None}
    if run.status not in ("derived", "held"):
        return {"status": "not_target", "record_id": None}

    existing = await find_existing(run.case_record_id, run.input_hash)
    if existing:
        logger.info("[HEIR-ENV] duplicate filing blocked run=%s -> No.%s",
                    emit(str(run.id), "record_id", "log", "operator"),
                    emit(existing, "record_id", "log", "operator"))
        return {"status": "already_filed", "record_id": existing}

    detail = _build_detail(run)
    fields = {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": _UNIT,
        "案件アプリID": run.case_app_id,
        "案件レコードID": run.case_record_id,
        "実行済み": "no",
        # 件名は record id 系のみ（氏名等の PII を入れない・§2.1）
        "件名": f"相続人導出の確認: 案件 No.{run.case_record_id}（run #{run.id}）",
        "チャネル固有データ": json.dumps({"heir_derivation": detail},
                                         ensure_ascii=False),
    }
    # ★単票 API（create_record）必須（§1.4: 一括 API は「レコード追加」Webhook 非発射）
    rid = str(await kintone.create_record(APP_SHIPPING, fields))
    logger.info("[HEIR-ENV] filed App30 No.%s run=%s",
                emit(rid, "record_id", "log", "operator"),
                emit(str(run.id), "record_id", "log", "operator"))
    return {"status": "filed", "record_id": rid}
