"""委任契約書（時効援用）自動生成 Webhook — CONTRACT-GEN-1（第1版）＋fix1

状態機械（fix1[02]・CAS＝$revision 楽観ロック）:
  契約書作成 --（CAS 勝者: 作成→作成中)--> 契約書作成中
    --（生成+検証+upload+添付 PUT〔revision=claim+1〕）--> 契約書作成済
  契約書作成(必須欠落)      → 変更なし（不足フィールド名のみ通知・作用は通知だけ）
  契約書作成中 + 添付なし   → 回収: CAS 再claim → 生成/添付 → 契約書作成済
  契約書作成中 + 添付あり   → 整合確認不能＝自動上書きせず CAS で「要確認」へ
                              ＋管理者通知（fix1[02] reconcile 規則）
  契約書作成済              → already_done skip（再配送・ACK 喪失の冪等化）
  要確認/空/他値            → stale_status skip（fix1[01] 正本の完全一致検証）
  CAS 敗者（409）           → 作用 0 で skip（並行 2 本でも生成/upload は 1 回）

入口ガード（fix1[01]）:
  - webhook 本文 app.id が App21 の実 app ID と完全一致（欠落・非数字・別 App
    は get_record 含め作用 0 で skip）
  - 本文ステータスが「契約書作成」のとき以外は skip（自 update の echo
    〔作成中/作成済〕もここで落ちる＝安価）

凍結文言の構造保証（fix1[03]）:
  - テンプレートは人承認済み現物の SHA-256 と完全一致（実行時+テスト両方）
  - 生成物の報酬条項（第2条全体）がテンプレートと正規化後逐語一致すること
    を実行時検証（不一致は添付せず 500＝差し込み事故の構造検知）

差し込み仕様（CONTRACT-GEN-1・8 キー=一意プレースホルダ数）:
  - {{依頼者氏名}}=顧客名（2 箇所）・{{依頼者住所}}=住所
  - {{対象債権者1}}=問い合わせ業者名・{{対象債権者2}}/{{対象債権者3}}=新設 field
  - 空き枠・契約年月日は全角空白（原本の体裁維持・契約日は締結時確定が既定）

スコープ外: CloudSign 送信 API 結線（第2版・P5）・PDF 自動変換。
"""

import hashlib
import io
import logging

from docx import Document
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from hub import kintone as hub_kintone
from hub.docx_builder import fill_template
from hub.redact import emit
from hub.webhook_auth import extract_record_id, verify_token

logger = logging.getLogger("contract")

_APP = hub_kintone.KintoneApp(
    "App 21 (案件)", "KINTONE_APP_ID", "KINTONE_API_TOKEN")

# ── 状態機械（fix1[02]・閉集合 4 値） ────────────────────────────────────────
FIELD_STATUS     = "契約書ステータス"
STATUS_TRIGGER   = "契約書作成"
STATUS_WORKING   = "契約書作成中"
STATUS_DONE      = "契約書作成済"
STATUS_REVIEW    = "要確認"
FIELD_ATTACHMENT = "委任契約書"
TEMPLATE_PATH    = "docx_templates/jikou/委任契約書.docx"
OUTPUT_FILENAME  = "委任契約書_時効援用.docx"
_BLANK           = "　"

# fix1[03]: 人承認済み現物テンプレートの SHA-256（2026-08-22 収載時実測）
TEMPLATE_SHA256 = (
    "7cc168a1bbce3ca183e9f4a3d46b6b8288c17d4d21954f07cdf038428c355334")
# fix1[03]: 報酬条項（第2条全体）の正規化済み逐語（弁護士凍結事項）
FROZEN_CLAUSE = (
    "第2条（弁護士報酬）",
    "1　本件の弁護士報酬（手数料）は、対象債権者1社につき金44,000円"
    "（消費税込み）とする。",
    "2　対象債権者が複数の場合の報酬は、前項の金額に社数を乗じた額とする"
    "（割引は行わない。）。",
    "3　報酬は前払いとし、分割払いはできない。",
)

_REQUIRED_NAME   = "顧客名"
_REQUIRED_ADDR   = "住所"
_CREDITOR_FIELDS = ("問い合わせ業者名", "対象債権者2", "対象債権者3")

router = APIRouter()


class ContractIntegrityError(RuntimeError):
    """fix1[03]: テンプレート/生成物の凍結文言検証に失敗（添付しない）。"""


def _fv(record: dict, code: str) -> str:
    return str((record.get(code) or {}).get("value") or "").strip()


def _missing_fields(record: dict) -> list[str]:
    missing = []
    if not _fv(record, _REQUIRED_NAME):
        missing.append(_REQUIRED_NAME)
    if not _fv(record, _REQUIRED_ADDR):
        missing.append(_REQUIRED_ADDR)
    if not any(_fv(record, c) for c in _CREDITOR_FIELDS):
        missing.append("債権者（問い合わせ業者名/対象債権者2/対象債権者3 の"
                       "いずれか1つ以上）")
    return missing


def build_fill_data(record: dict) -> dict:
    return {
        "{{依頼者氏名}}":  _fv(record, _REQUIRED_NAME),
        "{{依頼者住所}}":  _fv(record, _REQUIRED_ADDR),
        "{{対象債権者1}}": _fv(record, "問い合わせ業者名") or _BLANK,
        "{{対象債権者2}}": _fv(record, "対象債権者2") or _BLANK,
        "{{対象債権者3}}": _fv(record, "対象債権者3") or _BLANK,
        "{{契約年}}": _BLANK, "{{契約月}}": _BLANK, "{{契約日}}": _BLANK,
    }


def _clause_of(docx_bytes: bytes) -> tuple:
    """docx から第2条ブロック（見出し+3 項・正規化=前後空白除去）を抽出。"""
    doc = Document(io.BytesIO(docx_bytes))
    paras = [p.text.strip() for p in doc.paragraphs]
    try:
        i = paras.index(FROZEN_CLAUSE[0])
    except ValueError:
        return ()
    return tuple(paras[i:i + len(FROZEN_CLAUSE)])


def verify_template_integrity() -> None:
    """fix1[03]: 収載テンプレートが人承認済み現物（SHA-256 完全一致）である
    ことの実行時検証。不一致は生成しない。"""
    data = open(TEMPLATE_PATH, "rb").read()
    if hashlib.sha256(data).hexdigest() != TEMPLATE_SHA256:
        raise ContractIntegrityError("template hash mismatch")


def verify_frozen_clause(docx_bytes: bytes) -> None:
    """fix1[03]: 生成物の報酬条項（第2条全体）がテンプレートと逐語一致する
    ことの実行時検証。不一致は添付しない（差し込み事故の構造検知）。"""
    if _clause_of(docx_bytes) != FROZEN_CLAUSE:
        raise ContractIntegrityError("frozen clause mismatch")


async def _notify(text: str) -> None:
    """管理者 LINE 通知（best-effort・固定文言+レコード番号のみ）。"""
    try:
        from hub.notify import notify_admin_line
        await notify_admin_line(text)
    except Exception:
        logger.error("[CONTRACT] admin notify failed (fixed text)")


async def _generate_and_attach(record_id: str, record: dict,
                               final_revision: str) -> None:
    """生成 → 凍結文言検証 → upload → 添付+作成済（revision CAS つき PUT）。"""
    verify_template_integrity()
    docx_bytes = fill_template(TEMPLATE_PATH, build_fill_data(record))
    verify_frozen_clause(docx_bytes)
    logger.info("[CONTRACT] generated record_id=%s bytes=%d",
                emit(record_id, "record_id", "log", "operator"),
                len(docx_bytes))
    mime = ("application/vnd.openxmlformats-officedocument"
            ".wordprocessingml.document")
    file_key = await hub_kintone.upload_file(
        _APP, OUTPUT_FILENAME, docx_bytes, mime)
    await hub_kintone.update_record(_APP, record_id, {
        FIELD_ATTACHMENT: [{"fileKey": file_key}],
        FIELD_STATUS: STATUS_DONE,
    }, revision=final_revision)
    logger.info("[CONTRACT] attached record_id=%s",
                emit(record_id, "record_id", "log", "operator"))


async def _claim(record_id: str, revision: str, to_status: str) -> str | None:
    """CAS: $revision 一致時のみステータス遷移。勝者は次 revision（claim+1）を
    返す。

    fix2（CONTRACT-GEN-04）: cas_lost（None・HTTP 200）に落とすのは
    **revision 競合（409）のみ**。通信障害（transport_error）・認証障害
    （401/403）・5xx 等それ以外の KintoneError は再送出し、外側の except が
    HTTP 500 へ落として kintone Webhook の再配送に委ねる（沈黙させない）。
    本関数は「作成→作成中」claim・「作成中」再claim・「要確認」遷移の
    3 箇所すべてで共用される単一の正。"""
    try:
        await hub_kintone.update_record(
            _APP, record_id, {FIELD_STATUS: to_status}, revision=revision)
    except hub_kintone.KintoneError as e:
        if getattr(e, "status", None) == 409:
            return None                  # CAS 敗者（競合）のみ 200/作用 0
        raise                            # 障害系は外側で 500 → 再配送へ
    return str(int(revision) + 1)


async def _reconcile_working(record_id: str, record: dict,
                             revision: str):
    """fix1[02] reconcile: 「作成中」で停止した行の回収。
    添付なし=前回 run が upload/添付前に停止 → CAS 再claim して再生成（回収）。
    添付あり=起動内容との整合を機械確認できない → 自動上書きせず CAS で
    「要確認」へ倒し管理者通知。"""
    attachment = (record.get(FIELD_ATTACHMENT) or {}).get("value") or []
    if attachment:
        next_rev = await _claim(record_id, revision, STATUS_REVIEW)
        if next_rev is None:
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "cas_lost"})
        await _notify(
            f"【委任契約書】案件 No.{record_id} は生成が中断した状態で既に"
            "添付ファイルが存在するため、自動では上書きせず"
            f"「{STATUS_REVIEW}」にしました。添付内容を確認のうえ、再生成する"
            f"場合は添付を削除してステータスを「{STATUS_TRIGGER}」に設定して"
            "ください")
        return JSONResponse(status_code=200, content={
            "ok": True, "skip": "needs_review"})
    next_rev = await _claim(record_id, revision, STATUS_WORKING)
    if next_rev is None:
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "cas_lost"})
    await _generate_and_attach(record_id, record, next_rev)
    return JSONResponse(status_code=200, content={
        "ok": True, "record_id": record_id, "recovered": True})


@router.post("/contract/{secret}")
async def contract_webhook(secret: str, request: Request):
    if not verify_token(secret or "", "DOCUMENT_WEBHOOK_SECRET"):
        return JSONResponse(status_code=403, content={"error": "forbidden"})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid json"})

    # fix1[01]: app 同一性——App21 の実 app ID と完全一致（欠落・非数字・
    # 別 App は get_record 含め作用 0）
    app_in_body = str(((body.get("app") or {}) if isinstance(body, dict)
                       else {}).get("id") or "")
    if not app_in_body.isdigit() or app_in_body != str(_APP.app_id()):
        logger.warning("[CONTRACT] app mismatch in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "app_mismatch"})

    record_id = extract_record_id(body)
    if not record_id:
        logger.warning("[CONTRACT] record id missing in webhook body")
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "no_record_id"})

    # 本文ステータス gate（自 update の echo=作成中/作成済 もここで落ちる）
    try:
        status_in_webhook = body["record"][FIELD_STATUS]["value"]
    except (KeyError, TypeError):
        status_in_webhook = None
    if status_in_webhook != STATUS_TRIGGER:
        logger.info("[CONTRACT] not triggered record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "not_triggered"})

    try:
        record = await hub_kintone.get_record(_APP, record_id)
        current = _fv(record, FIELD_STATUS)
        revision = _fv(record, "$revision")

        # fix1[01]: 正本の完全一致検証（stale 本文は作用 0 で skip）
        if current == STATUS_DONE:
            logger.info("[CONTRACT] already done record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "already_done"})
        if current == STATUS_WORKING:
            return await _reconcile_working(record_id, record, revision)
        if current != STATUS_TRIGGER or not revision.isdigit():
            logger.info("[CONTRACT] stale status record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "stale_status"})

        # fail-closed: 必須欠落は生成しない（状態も動かさない）
        missing = _missing_fields(record)
        if missing:
            logger.info("[CONTRACT] missing required fields record_id=%s "
                        "count=%d",
                        emit(record_id, "record_id", "log", "operator"),
                        len(missing))
            await _notify(
                f"【委任契約書】案件 No.{record_id} は必須項目が未入力のため"
                f"生成しませんでした。不足: {'・'.join(missing)}。"
                "kintone で入力後、契約書ステータスを"
                f"「{STATUS_TRIGGER}」に設定し直してください")
            return JSONResponse(status_code=200, content={
                "ok": True, "skip": "missing_fields", "missing": missing})

        # fix1[02]: CAS（作成→作成中）勝者のみ生成（並行 2 本でも upload 1 回）
        next_rev = await _claim(record_id, revision, STATUS_WORKING)
        if next_rev is None:
            logger.info("[CONTRACT] cas lost record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "cas_lost"})
        await _generate_and_attach(record_id, record, next_rev)
    except Exception as e:
        logger.error("[CONTRACT] error record_id=%s cls=%s: %s",
                     emit(record_id, "record_id", "log", "operator"),
                     type(e).__name__,
                     emit(str(e), "vendor_raw", "log", "operator"))
        return JSONResponse(status_code=500,
                            content={"error": "internal_error"})

    return JSONResponse(status_code=200,
                        content={"ok": True, "record_id": record_id})
