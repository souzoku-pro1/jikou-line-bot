"""出力 redaction の一点集約（RV-10 PR-1・redaction contract）

設計正本: docs/design-drafts/DRAFT_RV10_REDACTION_AND_NOTIFY.md §1（裁定済み）・
製品設計完全版v2.4 §13.1。R-P1-101 所見（全ACCEPT）反映。

要点:
- 出力は `emit(value, kind, sink, audience)` を通す。sink（出力先）× audience（受け手）で
  許可水準が変わる（単一 safe() ではなく policy）。
- **sink×audience 許可ペア行列（M04）**: `_ALLOWED_PAIRS` に無い組合せは行列外＝完全抑止。
- **fail-closed**: unknown kind / 構造化値（dict/list/bytes/オブジェクト）/ None / emit 内部例外 /
  行列外 pair は **原文を出さず固定文言へ縮退**する。
- §13.1 の禁止カテゴリ（contract/fax/qa/vendor_raw）と document_metadata / external_ref は
  log/exception/business へ完全抑止（line_customer 本人宛の正当ケースのみ PII 系を素通し）。
- **token/secret は全 sink で常時抑止**（本人宛でも出さない）。
- 通常 log の PII は **既定で完全抑止**（DRAFT §1.4・出し分け水準は OPEN=大野裁定待ちのため
  この既定を緩めない）。

契約の限界（M01・重要）:
- **audience は「受け手カテゴリ」の宣言にすぎず、実際の送信先が本人であることの検証
  （recipient 検証）の代替ではない**。`line_customer` × `customer` の素通しを使う結線
  （PR-2）では、呼び出し側が「その LINE 宛先＝当該顧客本人」を検証済みの context を
  持っていることが前提。emit 単体はその検証を行わない。

**このモジュールは PR-1 では誰からも呼ばれない**（新規追加のみ）。既存 sink の切替（S1〜S4）は
PR-2 以降。本 PR は契約実装＋テスト＋AST 土台のみで、本番挙動変更ゼロ。
"""

import re

# ── kind 分類（DRAFT §1.2 + R-P1-101 追加） ───────────────────────────────
# 素通し（非 PII・運用に必要。値域検証つき）
KINDS_PASSTHROUGH = frozenset({"record_id", "count"})
# PII（既定は完全抑止・本人宛のみ素通し）
KINDS_PII = frozenset({
    "name", "address", "phone", "email", "birthdate", "koseki", "asset",
    "freetext",
})
# §13.1 禁止カテゴリ（log/exception/business へ常に完全抑止）
KINDS_FORBIDDEN = frozenset({"contract", "fax", "qa", "vendor_raw"})
# 書類メタ（L02・既定=完全非表示・record ID のみで参照）
KINDS_DOCUMENT_METADATA = frozenset({"document_metadata"})
# 外部参照 ID（document ID / LINE user ID / 追跡番号 等・既定=完全抑止）
KINDS_EXTERNAL_REF = frozenset({"external_ref"})
# 認証情報（全 sink 常時抑止・本人宛でも出さない）
KINDS_SECRET = frozenset({"token", "secret"})

ALL_KINDS = (KINDS_PASSTHROUGH | KINDS_PII | KINDS_FORBIDDEN
             | KINDS_DOCUMENT_METADATA | KINDS_EXTERNAL_REF | KINDS_SECRET)

# ── sink / audience（DRAFT §1.1） ─────────────────────────────────────────
SINKS = frozenset({
    "log", "http_response", "line_business", "line_customer",
    "exception_detail",
})
AUDIENCES = frozenset({"operator", "caller", "attorney", "customer"})

# sink × audience 許可ペア行列（M04）。ここに無い組合せは行列外＝完全抑止。
_ALLOWED_PAIRS = frozenset({
    ("log", "operator"),
    ("http_response", "caller"),
    ("line_business", "attorney"),
    ("line_customer", "customer"),
    ("exception_detail", "caller"),
})

# ── 値域検証（R-P1-101） ──────────────────────────────────────────────────
# record_id は kintone 内部レコード番号（英数・_・- のみ・長さ上限）
_RECORD_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_DIGITS_RE = re.compile(r"^[0-9]{1,18}$")

# ── 固定文言（原文を含まない・縮退用） ────────────────────────────────────
_SUPPRESSED_GENERIC = "（非表示）"
_UNKNOWN_KIND = "（分類不明・非表示）"
_NONE_TEXT = "（値なし・非表示）"
_FAILURE = "（redaction失敗・非表示）"


def _suppressed(kind: str) -> str:
    """完全抑止の固定文言を返す（private・原文を含めないこと）。
    L01: このヘルパはモジュール内部専用。外部からは emit() のみを使う。"""
    return f"（{kind}・非表示）"


def _structured_text(value) -> str:
    """構造化値は中身を出さず要素数のみ（len が無理でも安全に縮退）"""
    try:
        n = len(value)  # 循環参照でも len は走査しないので安全
    except Exception:
        n = "?"
    return f"（構造化値・非表示・{n}要素）"


def _is_scalar(value) -> bool:
    """str/int/float/bool を scalar 扱い。bytes・dict・list・オブジェクトは構造化扱い。"""
    return isinstance(value, (str, int, float, bool))


def _valid_record_id(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value >= 0
    if isinstance(value, str):
        return bool(_RECORD_ID_RE.match(value))
    return False


def _valid_count(value) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value >= 0
    if isinstance(value, str):
        return bool(_DIGITS_RE.match(value))
    return False


def _emit(value, kind: str, sink: str, audience: str) -> str:
    # M04: sink×audience 許可ペア行列の外は完全抑止（未知 sink/audience もここで落ちる）
    if (sink, audience) not in _ALLOWED_PAIRS:
        return _SUPPRESSED_GENERIC
    # 未知の kind は素通しにしない（DRAFT §1.3）
    if kind not in ALL_KINDS:
        return _UNKNOWN_KIND
    # None は完全抑止
    if value is None:
        return _NONE_TEXT
    # 構造化値（dict/list/tuple/set/bytes/オブジェクト）は完全抑止（§1.3）
    if not _is_scalar(value):
        return _structured_text(value)

    # ── 以降は scalar（str/数値/bool） ──
    # token/secret は全 sink で常時抑止（本人宛でも出さない）
    if kind in KINDS_SECRET:
        return _suppressed(kind)

    # external_ref は既定＝完全抑止（全 sink・ID を素で出さない）
    if kind in KINDS_EXTERNAL_REF:
        return _suppressed(kind)

    # 素通し（record_id / count）は値域検証を通ったものだけ許容
    if kind == "record_id":
        return str(value) if _valid_record_id(value) else _suppressed(kind)
    if kind == "count":
        return str(value) if _valid_count(value) else _suppressed(kind)

    # 本人宛の正当ケース: line_customer×customer は PII/禁止/docmeta を本人情報として素通し
    #   （token/secret/external_ref は上で抑止済み・ここには来ない）
    if sink == "line_customer":  # 行列により audience==customer が保証されている
        return str(value)

    # http_response は record_id/count 以外は一切出さない（抽出内容禁止）
    if sink == "http_response":
        return _suppressed(kind)

    # 禁止カテゴリ・document_metadata は log/business/exception へ常に完全抑止
    if kind in KINDS_FORBIDDEN or kind in KINDS_DOCUMENT_METADATA:
        return _suppressed(kind)

    # PII: log / exception_detail は完全抑止（§1.4 既定・§1.1 exception は値禁止）
    if sink in ("log", "exception_detail"):
        return _suppressed(kind)

    # PII: line_business は「要約」だが、出し分け水準が OPEN（大野裁定待ち）のため
    # 既定＝完全抑止を守る（原文・部分マスクを出さない）。裁定後に緩める。
    if sink == "line_business":
        return _suppressed(kind)

    # ここに到達しないはず（全 sink を上で処理）。安全側で抑止。
    return _SUPPRESSED_GENERIC


def emit(value, kind: str, sink: str, audience: str) -> str:
    """redaction 契約の唯一の入口。原文を漏らさず必ず str を返す。

    emit 自身が例外を起こしても原文を出さず固定文言へ縮退する（fail-closed・§1.5）。

    注意（M01）: audience は受け手カテゴリの宣言であって recipient 検証の代替ではない。
    line_customer×customer の素通しを使う結線側は、宛先が当該本人であることを別途
    検証済みであること（PR-2 要件）。
    """
    try:
        return _emit(value, kind, sink, audience)
    except Exception:
        return _FAILURE
