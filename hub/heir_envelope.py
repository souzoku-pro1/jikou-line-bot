"""heir_envelope — DerivationRun→App30 要確認封筒の起票（P3-003a・fix1）

正本: `docs/design-drafts/DRAFT_P3_003_ENVELOPE_FLOW.md` §2（凍結）＋§6 欠落補記。

- 対象: 確定待ちの DerivationRun（status='derived' または 'held'）。'error' は
  起票しない（§2.1・導出失敗は人手経路）。
- 封筒: App30 に `発送ステータス:"要確認"`・`実行済み:"no"` で起票
  （person_merge._file_candidate 同型）。チャネル/方向は既存語彙
  「スキャン受領/受領」を踏襲（専用チャネル値の新設は kintone 変更＝[人]・§6）。
- **ユニット種別は案件由来**（fix1 H03・凍結 §2.1 どおり）: 案件アプリ ID
  （run.case_app_id）→ ユニットの写像で解決。**解決不能なら起票せず異常扱い**
  （EnvelopePolicyError・§6 補記）。
- 冪等キー: `heir_derivation:{case_record_id}:{input_hash}`（§2.2）。
  **冪等照合は CloudSign 封筒経路の型**（fix1 H01）: query 値を escape した like で
  絞り込み → 各候補のチャネル固有データ JSON をコード側でパース →
  **トップキー heir_derivation かつ「冪等キー」完全一致**のレコードのみ再利用
  （部分一致・別トップキー・壊れ JSON は再利用しない）。
- **起票境界の検証**（fix1 H02/M01）: run 由来値を型・grammar 検証して snapshot 化
  してからのみ使用（案件 ID=数字列・input_hash/result_hash=SHA-256 小文字 hex 64・
  `:`/引用符等の曖昧値は構造で拒否）。lawyer_flags は既存 allowlist
  （hub.derivation_models.validate_lawyer_flags）を起票境界でも検証。
- **単票 API（create_record）必須**（§1.4: 一括 API は kintone Webhook 非発射）。
- flag `HEIR_DERIVATION_ENABLED`（既定 OFF）: OFF では一切起票しない。
- **結線点**: 導出コマンド（別票・§6）完了直後に `file_heir_envelope(run)` を呼ぶ。
"""

import json
import logging
import os
import re

from hub import kintone
from hub.derivation_models import validate_lawyer_flags
from hub.redact import emit  # RV-10: sink 出力は emit 契約経由（1形式）

logger = logging.getLogger("hub.heir_envelope")

APP_SHIPPING = kintone.KintoneApp("App 30 (発送管理)", "APP_SHIPPING", "TOKEN_SHIPPING")

_FLAG = "HEIR_DERIVATION_ENABLED"

# 起票境界の grammar（fix1 H02/M01・冪等キー構成要素の文法を固定）
_APP_ID_RE = re.compile(r"^[0-9]{1,10}$")          # kintone アプリ ID（数字列）
_CASE_RECORD_ID_RE = re.compile(r"^[0-9]{1,10}$")  # kintone `$id`（数字列・`:` 等を構造排除）
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")           # 正規化 SHA-256（§2.1 正本・小文字 hex）

# detail に載せてよいキーの閉集合（§2.1・run 参照のみ＝PII/payload 本体を持たない）
_DETAIL_KEYS = frozenset({
    "derivation_run_id", "case_record_id", "input_hash",
    "result_hash", "provisional", "lawyer_flags", "冪等キー",
})


class EnvelopePolicyError(ValueError):
    """起票境界の検証違反（grammar 不一致・ユニット解決不能・PII 防御）。
    起票せず異常扱い＝kintone への write は発生しない。"""


class EnvelopeDetailPolicyError(EnvelopePolicyError):
    """detail が閉集合外のキーを持つ（PII/payload 混入の芽・保存前に拒否）。"""


def heir_derivation_enabled() -> bool:
    """flag HEIR_DERIVATION_ENABLED（既定 OFF・値集合は durable 系と同一流儀）。"""
    return os.environ.get(_FLAG, "").strip().lower() in ("1", "true", "on", "yes")


def idempotency_key(case_record_id: str, input_hash: str) -> str:
    """冪等キー（§2.2・detail に平文で保持し like 検索の対象にする）。
    構成要素の文法は `_validated_snapshot` が起票境界で強制する（fix1 M01）。"""
    return f"heir_derivation:{case_record_id}:{input_hash}"


def _escape_kintone_query_value(value: str) -> str:
    """kintone クエリ文字列リテラル用エスケープ（fix1 H01・CloudSign M07 同型）。
    二重引用符・バックスラッシュを無害化し、like 構文の破壊・誤マッチを防ぐ。"""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _unit_for_case(case_app_id: str) -> str:
    """ユニット種別の案件由来解決（fix1 H03・凍結 §2.1）。

    案件アプリ ID → ユニットの写像（env で解決済みの既知案件アプリのみ）。
    解決不能（未知アプリ ID・env 未設定）は**起票せず異常扱い**（§6 補記）。"""
    mapping = {}
    jikou = os.environ.get("KINTONE_APP_ID", "").strip()
    souzoku = os.environ.get("SOUZOKU_KINTONE_APP_ID", "").strip()
    if jikou:
        mapping[jikou] = "時効援用"
    if souzoku:
        mapping[souzoku] = "相続一般"
    unit = mapping.get(case_app_id)
    if not unit:
        raise EnvelopePolicyError(
            "ユニット種別を案件から解決できません（未知の案件アプリ・起票中止）")
    return unit


def _validated_snapshot(run) -> dict:
    """run 由来値の型・grammar を起票境界で機械検証し、検証済み snapshot のみを
    以後の組立てに使う（fix1 H02: 値側 PII ガード／M01: 冪等キー構成要素の文法固定）。"""
    rid = getattr(run, "id", None)
    if not isinstance(rid, int) or rid <= 0:
        raise EnvelopePolicyError("run.id は正の整数であること")
    case_app_id = getattr(run, "case_app_id", None)
    if not isinstance(case_app_id, str) or not _APP_ID_RE.fullmatch(case_app_id):
        raise EnvelopePolicyError("case_app_id は数字列であること")
    case_record_id = getattr(run, "case_record_id", None)
    if not isinstance(case_record_id, str) \
            or not _CASE_RECORD_ID_RE.fullmatch(case_record_id):
        raise EnvelopePolicyError(
            "case_record_id は kintone $id（数字列）であること"
            "（`:`・引用符等の曖昧値は冪等キー構成要素として拒否・fix1 M01）")
    for name in ("input_hash", "result_hash"):
        v = getattr(run, name, None)
        if not isinstance(v, str) or not _HASH_RE.fullmatch(v):
            raise EnvelopePolicyError(
                f"{name} は正規化 SHA-256（小文字 hex 64 桁）であること（§2.1）")
    flags = getattr(run, "lawyer_flags", None)
    validate_lawyer_flags(flags)   # 既存 allowlist（enum 外＝PII 様値を保存前拒否・H02）
    return {
        "id": rid, "case_app_id": case_app_id, "case_record_id": case_record_id,
        "input_hash": run.input_hash, "result_hash": run.result_hash,
        "provisional": bool(getattr(run, "provisional", False)),
        "lawyer_flags": flags,
    }


async def find_existing(case_record_id: str, input_hash: str) -> str | None:
    """同一冪等キーの起票済み封筒を検索（fix1 H01・CloudSign 封筒経路の型）。

    like（escape 済み）で絞り込み → 候補のチャネル固有データ JSON をパースし、
    **トップキー heir_derivation かつ「冪等キー」完全一致**のみ採用。
    部分一致・別トップキー・壊れ JSON は再利用しない。"""
    key = idempotency_key(case_record_id, input_hash)
    esc = _escape_kintone_query_value(key)
    records = await kintone.search_records(
        APP_SHIPPING, f'チャネル固有データ like "{esc}"',
        fields=["$id", "チャネル固有データ"])
    for rec in records:
        raw = rec.get("チャネル固有データ", {}).get("value", "")
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue                     # 壊れ JSON は採用しない（H01）
        hd = data.get("heir_derivation") if isinstance(data, dict) else None
        if isinstance(hd, dict) and hd.get("冪等キー") == key:   # 完全一致のみ
            rid = rec.get("$id", {}).get("value")
            if rid is not None:
                return str(rid)
    return None


def _build_detail(snap: dict) -> dict:
    detail = {
        "derivation_run_id": snap["id"],
        "case_record_id": snap["case_record_id"],
        "input_hash": snap["input_hash"],
        "result_hash": snap["result_hash"],
        "provisional": snap["provisional"],
        "lawyer_flags": snap["lawyer_flags"],
        "冪等キー": idempotency_key(snap["case_record_id"], snap["input_hash"]),
    }
    if set(detail) != set(_DETAIL_KEYS):   # 閉集合の構造ガード（拡張は正本改定と同時）
        raise EnvelopeDetailPolicyError(
            "envelope detail は _DETAIL_KEYS の閉集合のみ（PII/payload 非混入・§2.1）")
    return detail


async def file_heir_envelope(run) -> dict:
    """DerivationRun の要確認封筒を App30 へ起票する（導出完了直後の結線点・§2.1）。

    ## 結線点の契約（裁定条件 (a)・fix1 M02 で失敗時挙動を拡充）

    - **入力**: DerivationRun への参照（ORM 行または同属性のオブジェクト）。
      参照する属性は id / case_app_id / case_record_id / input_hash / result_hash /
      status / provisional / lawyer_flags のみ＝**result_payload は読まない**。
      run の書換えは行わない（immutable 台帳と整合・読取専用）。
      値は `_validated_snapshot` の型・grammar 検証を通過したもののみ使用（fix1 H02）。
    - **冪等キーの生成規則**: `idempotency_key()`＝
      `heir_derivation:{case_record_id}:{input_hash}`（固定書式。構成要素の文法は
      起票境界で強制＝数字列＋hex64・fix1 M01）。照合は escape 済み like＋JSON
      完全一致（fix1 H01）。同一入力の再導出は既存封筒を再利用。
    - **戻り値**: {"status": "filed"（新規起票・record_id=新 App30 レコード番号）|
      "already_filed"（既存封筒あり・record_id=既存番号）|
      "disabled"（flag OFF・record_id=None）|
      "not_target"（status が derived/held 以外・record_id=None）}。
    - **失敗時挙動（fix1 M02・握り潰し禁止）**: **検索（search）・起票（create）・
      検証（policy）のいずれの失敗も例外で伝播**する。握り潰して正常戻り値を返す
      ことは禁止＝**新規起票を成功扱い（"filed"）にするのは create 成功時のみ**。
      policy 失敗（EnvelopePolicyError/PayloadPolicyError）は kintone I/O 前に送出
      （write ゼロ）。search 失敗時は create 未到達・create 失敗時も App30 への
      write は当該単票 create の 1 回以外に発生しない（部分状態なし）。
      例外時の再実行は冪等キーにより安全（成功済みなら already_filed）。
      **リトライ判断は呼出し元（導出コマンド票・別票）の責務**——この契約は
      導出コマンド票への申し送り事項として DRAFT §6 に固定
      （契約 pin テスト: test_p3_003a_heir_envelope.TestFailureBehaviorContract）。
    """
    if not heir_derivation_enabled():
        return {"status": "disabled", "record_id": None}
    if getattr(run, "status", None) not in ("derived", "held"):
        return {"status": "not_target", "record_id": None}

    snap = _validated_snapshot(run)          # H02/M01: kintone I/O 前に検証（write ゼロ）
    unit = _unit_for_case(snap["case_app_id"])   # H03: 案件由来・解決不能は起票中止

    existing = await find_existing(snap["case_record_id"], snap["input_hash"])
    if existing:
        logger.info("[HEIR-ENV] duplicate filing blocked run=%s -> No.%s",
                    emit(str(snap["id"]), "record_id", "log", "operator"),
                    emit(existing, "record_id", "log", "operator"))
        return {"status": "already_filed", "record_id": existing}

    detail = _build_detail(snap)
    fields = {
        "発送ステータス": "要確認",
        "方向": "受領",
        "チャネル": "スキャン受領",
        "ユニット種別": unit,
        "案件アプリID": snap["case_app_id"],
        "案件レコードID": snap["case_record_id"],
        "実行済み": "no",
        # 件名は record id 系のみ（氏名等の PII を入れない・§2.1）
        "件名": f"相続人導出の確認: 案件 No.{snap['case_record_id']}"
                f"（run #{snap['id']}）",
        "チャネル固有データ": json.dumps({"heir_derivation": detail},
                                         ensure_ascii=False),
    }
    # ★単票 API（create_record）必須（§1.4: 一括 API は「レコード追加」Webhook 非発射）
    rid = str(await kintone.create_record(APP_SHIPPING, fields))
    logger.info("[HEIR-ENV] filed App30 No.%s run=%s",
                emit(rid, "record_id", "log", "operator"),
                emit(str(snap["id"]), "record_id", "log", "operator"))
    return {"status": "filed", "record_id": rid}
