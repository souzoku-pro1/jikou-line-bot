"""brain_link — BRAIN-A1-LEDGER-1: 案件の識別と紐付け（v3 §3・BD-01/BD-14）

案件キー = (案件アプリ ID, 案件レコード番号)。レコード番号単独では識別しない。
参照の信頼段階（3 段）:
- auto（自動採用可）: 正本上で確認された案件キー。A1 の出典は kintone のみで、
  App 40 レコード自身（案件そのもの）と、App 30 の案件参照欄が App 40 を指し
  参照先が実在し ユニット種別=相続放棄 の行。
- candidate（候補に限定）: 参照は形式的に App 40 を指すが整合検査に落ちた行の
  候補案件キー（自動確定しない・link_history の candidates に残す）。
- hold（保留＝紐付け待ち）: 参照先不在・数字でない・別アプリ・チャネル不一致。
  業務台帳には入れず source_ingest/link_history に置く（§4-6）。

R5（v3 §3 の例外として記録）: App 28 は「category が相続放棄の閉集合に属し、
かつ line_user_id が App 40 の LINEユーザーID（一意制約）に完全一致する行」
のみ自動採用可。それ以外は取り込まない（保留にもしない）。一意判定は台帳ではなく
**正本**（App 40 を LINEユーザーID で limit 2 検索・ちょうど 1 件のときだけ採用・
検索失敗／0 件／2 件以上は採用しない＝fail-closed・BA-04）。brain_sync 側で行う。

関連喪失（R10）: 自動採用可だった関連が失われた（参照消去・別アプリへの移動・
不正参照・R5 の一意不成立・category 閉集合外）ときは brain_ledger の共通処理
（_detach_source_tx）を通す。本 module は「変化の有無」を判定して link_change を
組み立てるだけ（純粋判定・kintone・logging を import しない）。
訂正（誤紐付け）は brain_ledger.relink_source＝履歴追加のみ（削除しない）。
"""

import re

from hub import brain_ledger as ledger

_DIGITS_RE = re.compile(r"^[0-9]{1,10}$")
HOUKI_UNIT = "相続放棄"
FIELD_CASE_APP = "案件アプリID"
FIELD_CASE_RECORD = "案件レコードID"
FIELD_UNIT = "ユニット種別"

REASON_AUTO = "ref_confirmed"
REASON_REF_CHANGED = "ref_changed"
REASON_OTHER_APP = "ref_other_app"
REASON_NOT_DIGITS = "ref_not_digits"
REASON_UNIT_MISMATCH = "unit_mismatch"
REASON_REF_MISSING = "ref_missing"
REASON_MANUAL_PINNED = "manual_pinned"       # 手動訂正が同 revision の間は優先
REASON_CATEGORY_OUT, REASON_LINE_NOT_UNIQUE = ledger.DETACH_REASONS   # R5 不成立
REASONS = (REASON_AUTO, REASON_REF_CHANGED, REASON_OTHER_APP, REASON_NOT_DIGITS,
           REASON_UNIT_MISMATCH, REASON_REF_MISSING, REASON_MANUAL_PINNED,
           REASON_CATEGORY_OUT, REASON_LINE_NOT_UNIQUE)


class LinkDecision:
    __slots__ = ("trust", "case_key", "reason", "candidates")

    def __init__(self, trust: str, case_key: tuple | None, reason: str,
                 candidates: list | None = None):
        if trust not in ledger.TRUST_LEVELS:
            raise ledger.LedgerError("trust_level_not_in_closed_set")
        self.trust = trust
        self.case_key = case_key
        self.reason = reason
        self.candidates = candidates or []

    @property
    def auto(self) -> bool:
        return self.trust == "auto" and self.case_key is not None

    def as_dict(self) -> dict:
        return {"trust": self.trust, "reason": self.reason,
                "case_key": list(self.case_key) if self.case_key else None,
                "candidates": self.candidates}


def _v(record: dict, code: str) -> str:
    return str(((record.get(code) or {}).get("value")) or "").strip()


def decide_app30_reference(record: dict, houki_app_id: str,
                           target_exists: bool | None) -> LinkDecision:
    """App 30 の 1 行の案件参照を判定する（純粋関数・kintone に触れない）。

    target_exists: 参照先 App 40 レコードの実在（呼び出し側が台帳→kintone で確認）。
    None は「確認できなかった」＝保留（取得失敗を値なしに変換しない・§5）。
    """
    ref_app = _v(record, FIELD_CASE_APP)
    ref_rec = _v(record, FIELD_CASE_RECORD)
    unit = _v(record, FIELD_UNIT)
    if not houki_app_id or ref_app != str(houki_app_id):
        return LinkDecision("hold", None, REASON_OTHER_APP)
    if not _DIGITS_RE.fullmatch(ref_rec):
        return LinkDecision("hold", None, REASON_NOT_DIGITS)
    candidate = {"case_app_id": ref_app, "case_record_id": ref_rec,
                 "trust": "candidate"}
    if unit != HOUKI_UNIT:
        return LinkDecision("hold", None, REASON_UNIT_MISMATCH, [candidate])
    if target_exists is not True:
        return LinkDecision("hold", None, REASON_REF_MISSING, [candidate])
    return LinkDecision("auto", (ref_app, ref_rec), REASON_AUTO)


def decide_app28_row(record: dict, matches: list) -> LinkDecision | None:
    """R5: line_user_id の完全一致で App 40 案件キーが**ちょうど 1 件**あるときのみ
    auto。0 件・複数件は None（取り込まない・保留にもしない）。
    matches は brain_sync が**正本**（App 40 を LINEユーザーID で検索・BA-04）から
    引いた案件キー列。検索失敗は呼び出し側で「採用しない」に倒す（fail-closed）。"""
    if len(matches) != 1:
        return None
    key = matches[0]
    return LinkDecision("auto", (str(key[0]), str(key[1])), REASON_AUTO)


def link_change_for(decision: LinkDecision | None, prev_case: tuple | None,
                    *, lost_reason: str | None = None,
                    ingest_state: str = "held") -> dict | None:
    """R10: 現在の紐付け prev_case と新しい判定から「関連の移動／喪失」を組み立てる。
    変化なし（未紐付けのまま／同じ案件のまま）は None。戻り値は
    brain_ledger._detach_source_tx のキーワード引数（new_case/reason/trust/
    candidates/ingest_state）。"""
    if prev_case is None:
        return None
    prev = (str(prev_case[0]), str(prev_case[1]))
    if decision is not None and decision.auto:
        new = (str(decision.case_key[0]), str(decision.case_key[1]))
        if new == prev:
            return None
        return {"new_case": new, "reason": REASON_REF_CHANGED, "trust": "auto",
                "candidates": None, "ingest_state": "held"}
    reason = lost_reason or (decision.reason if decision is not None else None)
    if reason is None:
        raise ledger.LedgerError("link_lost_reason_required")
    return {"new_case": None, "reason": reason,
            "trust": decision.trust if decision is not None else "hold",
            "candidates": (decision.candidates or None) if decision is not None else None,
            "ingest_state": ingest_state}


async def record_decision(source_app_id: str, source_record_id: str,
                          decision: LinkDecision, prev_case: tuple | None) -> int | None:
    """判定を link_history へ積む（同じ判定の繰り返しは積まない・履歴は削除しない）。
    prev_case が有り、auto の新案件が異なるときは ref_changed として旧→新を残す
    （通常は R10 の共通処理が同一トランザクションで積むため、ここに来るのは
    変化のない判定だけ）。"""
    if decision.auto and prev_case is not None and tuple(prev_case) != tuple(decision.case_key):
        return await ledger.add_link_history(
            source_app_id=source_app_id, source_record_id=source_record_id,
            prev_case=prev_case, new_case=decision.case_key, trust_level="auto",
            reason=REASON_REF_CHANGED)
    if decision.auto and prev_case is not None:
        return None                              # 変化なし＝積まない
    last = await ledger.latest_link(source_app_id, source_record_id)
    if (last is not None and last["trust_level"] == decision.trust
            and last["reason"] == decision.reason
            and last["new_case"] == (tuple(decision.case_key) if decision.case_key else None)):
        return None                              # 同じ判定の繰り返し＝積まない
    return await ledger.add_link_history(
        source_app_id=source_app_id, source_record_id=source_record_id,
        prev_case=prev_case, new_case=decision.case_key,
        trust_level=decision.trust, reason=decision.reason,
        candidates=decision.candidates or None)
