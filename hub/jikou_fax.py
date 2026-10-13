"""jikou_fax — JIKOU-FAX-2: 時効援用通知 PDF の FAX 送信ジョブの骨組み（承認必須・実送信なし・台帳が正）

正本: docs/design/jikou-fax-1.md（D-9）。QUEUE-2026-10-13-E E-3。
- 投入は「弁護士が /app（hub/webapp_fax_view）で明示承認した案件」のみ: `enqueue()` は署名付き Approval（
  approve() が WEBAPP_SESSION_SECRET で署名）以外を NotApproved で拒む。署名鍵が無ければ承認も投入もできない
  （fail-closed）。ジョブ・webhook・受信経路から enqueue を呼ぶ口は作らない。
- 宛先: kintone の業者マスタ（env APP_CREDITOR_MASTER／TOKEN_CREDITOR_MASTER・欄名は env で指定・要確認）から
  FAX 番号を読む。番号は復唱・ログ出力しない（下 4 桁のみ）。台帳の宛先はマスタの record_id（番号は置かない）。
  FAX送信可 欄が「可」以外・番号検証落ち・マスタ未設定・該当なし・複数該当は送らない（NotSendable・固定語彙）。
- 台帳: business=jikou・channel=fax・purpose=other・受信イベント ID=fax:{App 21 No.}:{slot}:{PDF sha256 先頭 16}
  （E-2 の purpose 表に行あり）。同一案件・同一宛先・同一 PDF は begin() の重複検出で 2 回目 skipped。台帳が使えなければ
  送らない。text_version は PDF sha256 を text として渡した hash（本文は持たない）。
- 送信: hub/fax_adapter.get_provider()（既定 DryRun）。例外・timeout は unconfirmed（自動再送なし・/app/send_ops）。
- kintone 刻印（写し・正は台帳）: App 21 の FAX送信状態_{slot}／FAX送信日時_{slot}／FAX送信ID_{slot}（欄が無ければ
  書かない・form fields 1 時間キャッシュ・失敗は「無い」側）。
- DryRun では PDF を保存しない（本 module も provider もファイル I/O なし）。台帳にはハッシュと下 4 桁（履歴 note）だけ。
- RV-10: ログ・例外・画面に番号（下 4 桁以外）・相談者の氏名を載せない。
"""

import datetime
import hashlib
import hmac
import logging
import os
import re
import time
from dataclasses import dataclass

from hub import fax_adapter, kintone, send_ledger
from hub.redact import emit
from hub.webapp_auth import _sign

logger = logging.getLogger("hub.jikou_fax")

BUSINESS = "jikou"
CHANNEL = "fax"
LEDGER_KIND = "fax"
APP_JIKOU = kintone.KintoneApp("App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")
APP_CREDITOR = kintone.KintoneApp("業者マスタ", "APP_CREDITOR_MASTER", "TOKEN_CREDITOR_MASTER")   # 要設定（大野）
MASTER_NAME_FIELD_ENV = "CREDITOR_MASTER_NAME_FIELD"
MASTER_FAX_FIELD_ENV = "CREDITOR_MASTER_FAX_FIELD"
MASTER_FAX_OK_FIELD_ENV = "CREDITOR_MASTER_FAX_OK_FIELD"
MASTER_NAME_FIELD_DEFAULT = "業者名"
MASTER_FAX_FIELD_DEFAULT = "FAX番号"
MASTER_FAX_OK_FIELD_DEFAULT = "FAX送信可"
FAX_OK_VALUE = "可"
CREDITOR_FIELDS = ("問い合わせ業者名", "対象債権者2", "対象債権者3")       # notice_webhook._CREDITOR_FIELDS と同じ（pin）
SLOTS = (1, 2, 3)
FIELD_ATTACHMENT = "notice_file"                                            # notice_webhook.FIELD_ATTACHMENT と同じ（pin）
PDF_NAME_FMT = "時効援用通知書_対象債権者{slot}.pdf"
STAMP_STATE_FMT = "FAX送信状態_{slot}"
STAMP_AT_FMT = "FAX送信日時_{slot}"
STAMP_ID_FMT = "FAX送信ID_{slot}"
STAMP_STATES = {"sent": "送達待ち", "unconfirmed": "結果不明", "failed": "失敗"}   # DryRun の sent は「受付済（送達待ち）」
CACHE_SECONDS = 3600.0
_JST = datetime.timezone(datetime.timedelta(hours=9))
_SIG_PREFIX = "fax-approval"
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_ID_RE = re.compile(r"^[0-9]{1,12}$")
_OP_ID_RE = re.compile(r"^[0-9a-f]{32}$")

# 結果（閉集合）
OUTCOME_SENT = "sent"                        # 受付（台帳 sent・applied）
OUTCOME_SKIPPED = "skipped"                  # 同一案件・同一宛先・同一 PDF の 2 回目
OUTCOME_UNCONFIRMED = "unconfirmed"
OUTCOME_FAILED = "failed"
OUTCOME_LEDGER_UNAVAILABLE = "ledger_unavailable"
OUTCOMES = (OUTCOME_SENT, OUTCOME_SKIPPED, OUTCOME_UNCONFIRMED, OUTCOME_FAILED, OUTCOME_LEDGER_UNAVAILABLE)

# 送れない理由（閉集合・固定語彙）
REASON_MASTER_UNSET = "master_unset"
REASON_NOT_FOUND = "not_found"
REASON_AMBIGUOUS = "ambiguous"
REASON_FAX_NOT_ALLOWED = "fax_not_allowed"
REASON_INVALID_NUMBER = "invalid_number"
REASON_PDF_NOT_FOUND = "pdf_not_found"
REASON_PDF_INVALID = "pdf_invalid"
REASON_KINTONE = "kintone"
REASONS = (REASON_MASTER_UNSET, REASON_NOT_FOUND, REASON_AMBIGUOUS, REASON_FAX_NOT_ALLOWED, REASON_INVALID_NUMBER,
           REASON_PDF_NOT_FOUND, REASON_PDF_INVALID, REASON_KINTONE)

_stamp_cache: dict = {}         # "fields" → (monotonic, frozenset(欄コード))


class NotApproved(RuntimeError):
    """承認（署名付き Approval）なしの投入。"""


class NotSendable(RuntimeError):
    """宛先・PDF が送れる状態にない（reason は閉集合）。"""

    def __init__(self, reason: str):
        super().__init__(reason if reason in REASONS else REASON_KINTONE)
        self.reason = reason if reason in REASONS else REASON_KINTONE


@dataclass(frozen=True)
class Destination:
    master_record_id: str
    number: str                 # E.164（復唱・ログ禁止）

    @property
    def last4(self) -> str:
        return fax_adapter.last4(self.number)


@dataclass(frozen=True)
class Approval:
    """弁護士の明示承認（approve() だけが正しい signature を付けられる）。"""
    record_id: str
    slot: int
    pdf_sha256: str
    master_record_id: str
    op_id: str
    signature: str

    def payload(self) -> str:
        return f"{_SIG_PREFIX}.{self.record_id}.{self.slot}.{self.pdf_sha256}.{self.master_record_id}.{self.op_id}"


# ── 純関数 ────────────────────────────────────────────────────────────────────
def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def normalize_number(raw: str) -> str | None:
    """数字・ハイフン・空白・全角を正規化し E.164 へ（国内 0 始まり → +81）。検証に落ちれば None。"""
    s = str(raw or "").translate(str.maketrans("０１２３４５６７８９－ー＋", "0123456789--+"))
    s = re.sub(r"[\s\-()（）]", "", s)
    if not s:
        return None
    if s.startswith("+"):
        digits = s[1:]
        if not digits.isdigit() or not (9 <= len(digits) <= 15):
            return None
        return "+" + digits
    if not s.isdigit():
        return None
    if s.startswith("0") and 10 <= len(s) <= 11:
        return "+81" + s[1:]
    return None


def ledger_key(record_id: str, slot: int, pdf_sha256: str) -> str:
    return f"{LEDGER_KIND}:{record_id}:{int(slot)}:{pdf_sha256[:16]}"


def creditor_name(record: dict, slot: int) -> str:
    if slot not in SLOTS:
        return ""
    code = CREDITOR_FIELDS[slot - 1]
    return str(((record or {}).get(code) or {}).get("value") or "").strip()


def pdf_file_for_slot(record: dict, slot: int) -> dict | None:
    """App 21 の添付欄から当該 slot の PDF（名前一致）を返す（無ければ None・docx は対象外）。"""
    files = ((record or {}).get(FIELD_ATTACHMENT) or {}).get("value") or []
    want = PDF_NAME_FMT.format(slot=int(slot))
    for f in files:
        if str((f or {}).get("name") or "") == want:
            return f
    return None


def approve(record_id: str, slot: int, pdf_sha256: str, master_record_id: str, op_id: str) -> Approval | None:
    """明示承認の印（署名）。鍵が無い・入力が閉集合外なら None（承認できない＝投入できない）。"""
    if not (_ID_RE.fullmatch(str(record_id)) and slot in SLOTS and _SHA_RE.fullmatch(str(pdf_sha256))
            and _ID_RE.fullmatch(str(master_record_id)) and _OP_ID_RE.fullmatch(str(op_id))):
        return None
    a = Approval(str(record_id), int(slot), str(pdf_sha256), str(master_record_id), str(op_id), "")
    sig = _sign(a.payload())
    if not sig:
        return None
    return Approval(a.record_id, a.slot, a.pdf_sha256, a.master_record_id, a.op_id, sig)


def is_approved(approval) -> bool:
    if not isinstance(approval, Approval) or not approval.signature:
        return False
    expected = _sign(approval.payload())
    return bool(expected) and hmac.compare_digest(str(approval.signature), expected)


# ── 宛先（業者マスタ・読取のみ） ───────────────────────────────────────────────
def _master_fields() -> tuple:
    return (os.environ.get(MASTER_NAME_FIELD_ENV, "") or MASTER_NAME_FIELD_DEFAULT,
            os.environ.get(MASTER_FAX_FIELD_ENV, "") or MASTER_FAX_FIELD_DEFAULT,
            os.environ.get(MASTER_FAX_OK_FIELD_ENV, "") or MASTER_FAX_OK_FIELD_DEFAULT)


def master_configured() -> bool:
    return bool(APP_CREDITOR.app_id() and APP_CREDITOR.token())


async def resolve_destination(name: str) -> Destination:
    """業者名 → マスタ 1 件 → FAX送信可=可 → 番号検証。送れなければ NotSendable。番号は返すだけ（ログに出さない）。"""
    name = str(name or "").strip()
    if not master_configured():
        raise NotSendable(REASON_MASTER_UNSET)
    if not name:
        raise NotSendable(REASON_NOT_FOUND)
    name_f, fax_f, ok_f = _master_fields()
    q = f'{name_f} = "{name.replace(chr(34), "")}"'
    try:
        rows = await kintone.search_records(APP_CREDITOR, q, fields=["$id", name_f, fax_f, ok_f])
    except Exception:
        logger.warning("[JIKOU_FAX] master lookup failed (fixed reason)")
        raise NotSendable(REASON_KINTONE)
    if not rows:
        raise NotSendable(REASON_NOT_FOUND)
    if len(rows) > 1:
        raise NotSendable(REASON_AMBIGUOUS)
    rec = rows[0]
    rid = str(((rec.get("$id") or {}).get("value")) or "")
    ok = str(((rec.get(ok_f) or {}).get("value")) or "").strip()
    if ok != FAX_OK_VALUE:
        raise NotSendable(REASON_FAX_NOT_ALLOWED)
    number = normalize_number(str(((rec.get(fax_f) or {}).get("value")) or ""))
    if not number or not _ID_RE.fullmatch(rid):
        raise NotSendable(REASON_INVALID_NUMBER)
    return Destination(rid, number)


async def fetch_pdf(record: dict, slot: int) -> bytes:
    """当該 slot の PDF を App 21 から取得（無い・PDF でない・取得失敗は NotSendable）。保存しない。"""
    f = pdf_file_for_slot(record, slot)
    if not f or not f.get("fileKey"):
        raise NotSendable(REASON_PDF_NOT_FOUND)
    try:
        data = await kintone.download_file(APP_JIKOU, str(f["fileKey"]))
    except Exception:
        logger.warning("[JIKOU_FAX] pdf download failed (fixed reason)")
        raise NotSendable(REASON_KINTONE)
    if not data or not data.startswith(b"%PDF"):
        raise NotSendable(REASON_PDF_INVALID)
    return data


# ── 投入（承認必須） ───────────────────────────────────────────────────────────
async def enqueue(approval, pdf_bytes: bytes, destination: Destination) -> dict:
    """承認済みの 1 件を台帳に記録して provider へ渡す。戻り値 {"outcome","accept_id","last4","op_id"}（閉集合）。
    承認なし・署名不一致・PDF/宛先が承認内容と違う → NotApproved（投入しない・台帳に書かない）。"""
    if not is_approved(approval):
        raise NotApproved("approval required")
    sha = fax_adapter.pdf_sha256(pdf_bytes)
    if sha != approval.pdf_sha256 or str(destination.master_record_id) != approval.master_record_id:
        raise NotApproved("approval does not match pdf/destination")
    provider = fax_adapter.get_provider()                 # 閉集合外は UnknownProvider（送らない）
    key = ledger_key(approval.record_id, approval.slot, sha)
    res = {"outcome": OUTCOME_LEDGER_UNAVAILABLE, "accept_id": "", "last4": destination.last4, "op_id": ""}
    token = send_ledger.bind_inbound(key)
    try:
        with send_ledger.purpose(send_ledger.PURPOSE_OTHER):
            op = await send_ledger.begin(BUSINESS, CHANNEL, destination.master_record_id, sha)
        if op is send_ledger.DUPLICATE_SENT:
            logger.info("[JIKOU_FAX] skipped (already accepted) record_id=%s", emit(approval.record_id, "record_id", "log", "operator"))
            res["outcome"] = OUTCOME_SKIPPED
            return res
        if op is send_ledger.DUPLICATE_UNCONFIRMED:
            res["outcome"] = OUTCOME_UNCONFIRMED
            return res
        if not isinstance(op, send_ledger.Operation):
            logger.warning("[JIKOU_FAX] not sent (ledger unavailable)")
            return res
        res["op_id"] = op.op_id
        try:
            accept_id = await send_ledger.guarded_http(
                op, provider.send(pdf_bytes, destination.number,
                                  {"record_id": approval.record_id, "slot": approval.slot, "sha256": sha}))
        except Exception:
            await send_ledger.finish(op, send_ledger.STATE_UNCONFIRMED)
            logger.warning("[JIKOU_FAX] provider result unknown (unconfirmed)")
            res["outcome"] = OUTCOME_UNCONFIRMED
            return res
        if not accept_id:
            await send_ledger.finish(op, send_ledger.STATE_FAILED)
            res["outcome"] = OUTCOME_FAILED
            return res
        applied = await send_ledger.finish(op, send_ledger.STATE_SENT)
        res["accept_id"] = str(accept_id)
        res["outcome"] = OUTCOME_SENT if applied == "applied" else OUTCOME_UNCONFIRMED
        logger.info("[JIKOU_FAX] accepted provider=%s last4=%s record_id=%s",
                    emit(provider.name, "freetext", "log", "operator"),
                    emit(destination.last4, "freetext", "log", "operator"),
                    emit(approval.record_id, "record_id", "log", "operator"))
        return res
    finally:
        send_ledger.unbind(token)


# ── kintone 刻印（写し） ───────────────────────────────────────────────────────
async def stamp_fields_available(*, now=None) -> frozenset:
    """App 21 に刻印欄があるか（1 時間キャッシュ・照会失敗は空＝書かない）。"""
    t = time.monotonic() if now is None else now
    hit = _stamp_cache.get("fields")
    if hit and t - hit[0] < CACHE_SECONDS:
        return hit[1]
    try:
        props = await kintone.get_form_fields(APP_JIKOU)
    except Exception:
        logger.info("[JIKOU_FAX] stamp field check failed (treated as unavailable)")
        return frozenset()
    have = frozenset(k for k in props.keys() if k.startswith(("FAX送信状態_", "FAX送信日時_", "FAX送信ID_")))
    _stamp_cache["fields"] = (t, have)
    return have


def clear_cache() -> None:
    _stamp_cache.clear()


def stamp_values(slot: int, outcome: str, accept_id: str, available: frozenset, now: datetime.datetime | None = None) -> dict:
    """刻印する欄と値（欄が無いものは落とす・純関数）。"""
    now = now or _now()
    cand = {STAMP_STATE_FMT.format(slot=slot): STAMP_STATES.get(outcome, "結果不明"),
            STAMP_AT_FMT.format(slot=slot): now.astimezone(_JST).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
            STAMP_ID_FMT.format(slot=slot): str(accept_id or "")[:64]}
    return {k: v for k, v in cand.items() if k in available}       # kintone.update_record が {"value"} に包む


async def stamp_kintone(record_id: str, slot: int, outcome: str, accept_id: str) -> str:
    """送信結果の写しを App 21 に書く（欄が無ければ skipped・失敗は failed・台帳が正）。"""
    if outcome not in (OUTCOME_SENT, OUTCOME_UNCONFIRMED, OUTCOME_FAILED):
        return "skipped"
    fields = stamp_values(slot, outcome, accept_id, await stamp_fields_available())
    if not fields:
        logger.info("[JIKOU_FAX] stamp skipped (fields unavailable)")
        return "skipped"
    try:
        await kintone.update_record(APP_JIKOU, str(record_id), fields)
    except Exception:
        logger.warning("[JIKOU_FAX] stamp failed (fixed reason) record_id=%s", emit(record_id, "record_id", "log", "operator"))
        return "failed"
    return "written"
