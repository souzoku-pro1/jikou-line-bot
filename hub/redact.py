"""出力 redaction の一点集約（RV-10 PR-1・redaction contract）

設計正本: docs/design-drafts/DRAFT_RV10_REDACTION_AND_NOTIFY.md §1（裁定済み）・
製品設計完全版v2.4 §13.1。

要点:
- 出力は `emit(value, kind, sink, audience)` を通す。sink（出力先）× audience（受け手）で
  許可水準が変わる（単一 safe() ではなく policy）。
- **fail-closed**: unknown kind / 構造化値（dict/list/bytes/オブジェクト）/ None / emit 内部例外は
  すべて **原文を出さず固定文言へ縮退**する。
- §13.1 の禁止カテゴリ（contract/fax/qa/vendor_raw）と document_metadata は
  log/exception へ **常に完全抑止**（line_customer 本人宛の正当ケースのみ素通し）。
- 通常 log の PII は **既定で完全抑止**（DRAFT §1.4・出し分け水準は OPEN=大野裁定待ちのため
  この既定を緩めない）。

**このモジュールは PR-1 では誰からも呼ばれない**（新規追加のみ）。既存 sink の切替（S1〜S4）は
PR-2 以降。本 PR は契約実装＋テスト＋AST 土台のみで、本番挙動変更ゼロ。
"""

# ── kind 分類（DRAFT §1.2） ───────────────────────────────────────────────
# 素通し（非 PII・運用に必要）
KINDS_PASSTHROUGH = frozenset({"record_id", "count"})
# PII（既定は完全抑止・sink により要約）
KINDS_PII = frozenset({
    "name", "address", "phone", "email", "birthdate", "koseki", "asset",
    "freetext",
})
# §13.1 禁止カテゴリ（log/exception へ常に完全抑止）
KINDS_FORBIDDEN = frozenset({"contract", "fax", "qa", "vendor_raw"})
# 書類メタ（L02・既定=完全非表示・record ID のみで参照）
KINDS_DOCUMENT_METADATA = frozenset({"document_metadata"})

ALL_KINDS = (KINDS_PASSTHROUGH | KINDS_PII | KINDS_FORBIDDEN
             | KINDS_DOCUMENT_METADATA)

# ── sink / audience（DRAFT §1.1） ─────────────────────────────────────────
SINKS = frozenset({
    "log", "http_response", "line_business", "line_customer",
    "exception_detail",
})
AUDIENCES = frozenset({"operator", "caller", "attorney", "customer"})

# ── 固定文言（原文を含まない・縮退用） ────────────────────────────────────
_SUPPRESSED_GENERIC = "（非表示）"
_UNKNOWN_KIND = "（分類不明・非表示）"
_NONE_TEXT = "（値なし・非表示）"
_FAILURE = "（redaction失敗・非表示）"


def _suppressed(kind: str) -> str:
    return f"（{kind}・非表示）"


def _structured_text(value) -> str:
    """構造化値は中身を出さず要素数のみ（len が無理でも安全に縮退）"""
    try:
        n = len(value)  # 循環参照でも len は走査しないので安全
    except Exception:
        n = "?"
    return f"（構造化値・非表示・{n}要素）"


def _is_scalar(value) -> bool:
    """str/int/float/bool を scalar 扱い。bytes・dict・list・オブジェクトは構造化扱い。
    bool は int のサブクラスだが scalar として許容"""
    return isinstance(value, (str, int, float, bool))


def _emit(value, kind: str, sink: str, audience: str) -> str:
    # 未知の sink/audience は最も安全側（完全抑止）へ（fail-closed）
    if sink not in SINKS or audience not in AUDIENCES:
        return _SUPPRESSED_GENERIC
    # 未知の kind は素通しにしない（DRAFT §1.3）
    if kind not in ALL_KINDS:
        return _UNKNOWN_KIND
    # None は完全抑止
    if value is None:
        return _NONE_TEXT
    # 構造化値（dict/list/tuple/set/bytes/オブジェクト）は完全抑止
    # ——「json.dumps で丸ごと」を構造的に禁止（DRAFT §1.3）
    if not _is_scalar(value):
        return _structured_text(value)

    # ── 以降は scalar（str/数値/bool） ──
    # 素通し（record_id / count）は全 sink で許容
    if kind in KINDS_PASSTHROUGH:
        return str(value)

    # 本人宛の正当ケース: line_customer かつ audience=customer は原文可
    # （PII/document_metadata/禁止カテゴリいずれも本人の情報として返す）
    if sink == "line_customer":
        if audience == "customer":
            return str(value)
        return _suppressed(kind)  # 宛先不一致は fail-closed

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
    """
    try:
        return _emit(value, kind, sink, audience)
    except Exception:
        return _FAILURE
