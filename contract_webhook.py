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

第2版（CONTRACT-GEN-2・PDF 化+CloudSign 自動登録）:
  クラウドサイン登録 --（CAS: 登録→登録中）--> クラウドサイン登録中
    --（PDF 生成+凍結検証 → CloudSign 書類作成+PDF 添付+宛先追加
        → doc id 書き戻し PUT〔revision=claim+1〕）--> クラウドサイン登録済
  前提未充足（docx 未添付/メールアドレス欠落・形式不正/v1 必須欠落）
      → 変更なし（不足フィールド名のみ通知・値は非搭載）
  CloudSign 途中失敗 → 下書き削除（部分状態を残さない）＋掃除成功時のみ
      ステータス巻き戻し（登録中→登録）→ 500 で kintone 再配送=自動再試行。
      掃除失敗時は巻き戻さず 500 → 再配送は下記 reconcile で「要確認」へ
  クラウドサイン登録中（reconcile） → CloudSign 側に下書きが残り得る（外部
      状態）ため自動再実行はせず常に CAS で「要確認」+管理者通知（fail-closed・
      v1 の添付有無分岐と異なる点は二重下書き防止のため）
  クラウドサイン登録済 → already_done skip（冪等化）

CloudSign 連携の一線（裁定済み方針）:
  - 呼ぶのは 書類作成（POST /documents）・PDF 添付（POST .../files）・
    宛先追加（POST .../participants）・下書き削除（DELETE、掃除時のみ）。
  - 送信 API（PUT /documents/{id}）は呼ばない。送信操作は大野が CloudSign
    画面で行う（対外効果の一線）。テストが source 走査で PUT 不在を pin。

PDF 化の設計（CONTRACT-GEN-2 条件）:
  - テンプレ docx を単一の正とし、fill_template 済み docx の段落を
    contract_pdf が描画（本文をコードに二重管理しない）。
  - 生成 PDF の抽出テキストに対し第1版同水準の凍結検証を実行時に行う
    （verify_frozen_pdf: 第2条逐語一致〔全空白除去後の連続部分列〕+
    差し込みキー残存なし。不一致は登録せず 500）。

スコープ外: CloudSign 送信 API の呼び出し・締結後処理（既存
cloudsign_webhook が cloudsign_document_id で照合して担う）。
"""

import hashlib
import io
import logging
import re

from docx import Document
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import contract_pdf
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

# ── 第2版（CONTRACT-GEN-2）: CloudSign 登録の状態 3 値+フィールド ──────────
STATUS_CS_TRIGGER = "クラウドサイン登録"
STATUS_CS_WORKING = "クラウドサイン登録中"
STATUS_CS_DONE    = "クラウドサイン登録済"
FIELD_EMAIL       = "メールアドレス"
FIELD_CS_DOC_ID   = "cloudsign_document_id"   # cloudsign_webhook の照合キー
OUTPUT_PDF_NAME   = "委任契約書_時効援用.pdf"
# 簡易 grammar（fail-closed）: ASCII のローカル部@ドメイン.TLD のみ許可。
# 全角・空白・@ 二重などは形式不正として登録しない
_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

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


def verify_frozen_pdf(pdf_bytes: bytes) -> None:
    """CONTRACT-GEN-2: 生成 PDF の抽出テキストへの凍結検証（第1版同水準）。

    PDF は折返しで改行位置が変わるため、全空白（全角空白含む）除去後の
    連結文字列に対して第2条（見出し+3 項）が連続部分列として文字単位で
    逐語一致すること、および差し込みキー（{{ / }}）が残存しないことを検証。
    不一致は CloudSign へ登録しない（500）。"""
    flat = "".join(contract_pdf.pdf_text(pdf_bytes).split())
    frozen = "".join("".join(part.split()) for part in FROZEN_CLAUSE)
    if frozen not in flat:
        raise ContractIntegrityError("frozen clause missing in pdf")
    if "{{" in flat or "}}" in flat:
        raise ContractIntegrityError("unfilled key in pdf")


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


# ── CloudSign API（CONTRACT-GEN-2）───────────────────────────────────────────
# 呼ぶのは POST（作成/添付/宛先）と DELETE（掃除）のみ。送信 API（PUT）は
# 呼ばない＝送信操作は大野が CloudSign 画面で行う（テストが source pin）。


def _cs_request(method: str, path: str, **kwargs):
    """CloudSign API 呼び出し。token 管理は cloudsign_webhook._token（本番
    稼働中の単一の正）を共用し、401 は取り直して 1 回だけ再試行。"""
    import requests

    import cloudsign_webhook as cs
    url = f"{cs.CLOUDSIGN_API_BASE}{path}"
    headers = {"Authorization": f"Bearer {cs._token.get()}"}
    resp = requests.request(method, url, headers=headers, timeout=30, **kwargs)
    if resp.status_code == 401:
        cs._token.invalidate()
        headers = {"Authorization": f"Bearer {cs._token.get()}"}
        resp = requests.request(method, url, headers=headers, timeout=30,
                                **kwargs)
    resp.raise_for_status()
    return resp


def _cs_create_document(record_id: str) -> str:
    """書類作成（下書き）。タイトルは案件 No のみ（氏名等の PII は載せない）。"""
    resp = _cs_request("POST", "/documents",
                       data={"title": f"委任契約書_案件No.{record_id}"})
    doc_id = str((resp.json() or {}).get("id") or "")
    if not doc_id:
        raise ContractIntegrityError("cloudsign document id missing")
    return doc_id


def _cs_attach_pdf(doc_id: str, pdf_bytes: bytes) -> None:
    _cs_request("POST", f"/documents/{doc_id}/files",
                files={"uploadfile":
                       (OUTPUT_PDF_NAME, pdf_bytes, "application/pdf")},
                data={"name": OUTPUT_PDF_NAME})


def _cs_add_participant(doc_id: str, email: str, name: str) -> None:
    _cs_request("POST", f"/documents/{doc_id}/participants",
                data={"email": email, "name": name})


def _cs_delete_draft(doc_id: str) -> bool:
    """途中失敗時の下書き掃除（部分状態を残さない）。成功=True。失敗しても
    例外は伝播させない（元の失敗を 500 で報告するのが主）。"""
    try:
        _cs_request("DELETE", f"/documents/{doc_id}")
        return True
    except Exception:
        logger.error("[CONTRACT] cloudsign draft cleanup failed")
        return False


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


async def _cloudsign_flow(record_id: str, record: dict, revision: str):
    """CONTRACT-GEN-2: PDF 生成 → CloudSign 書類作成+PDF 添付+宛先追加 →
    doc id 書き戻し（登録済）。送信 API は呼ばない（対外効果の一線）。"""
    # fail-closed: 前提未充足は登録しない（状態も動かさない・値は非搭載）
    problems = _missing_fields(record)
    if not (record.get(FIELD_ATTACHMENT) or {}).get("value"):
        problems.append(f"{FIELD_ATTACHMENT}（docx 未添付＝先に"
                        f"「{STATUS_TRIGGER}」を実行してください）")
    email = _fv(record, FIELD_EMAIL)
    if not email:
        problems.append(FIELD_EMAIL)
    elif not _EMAIL_RE.fullmatch(email):
        problems.append(f"{FIELD_EMAIL}（形式不正）")
    if problems:
        logger.info("[CONTRACT] cloudsign preconditions unmet record_id=%s "
                    "count=%d",
                    emit(record_id, "record_id", "log", "operator"),
                    len(problems))
        await _notify(
            f"【委任契約書】案件 No.{record_id} はクラウドサイン登録の前提を"
            f"満たしていないため登録しませんでした。不足: "
            f"{'・'.join(problems)}。kintone で解消後、契約書ステータスを"
            f"「{STATUS_CS_TRIGGER}」に設定し直してください")
        return JSONResponse(status_code=200, content={
            "ok": True, "skip": "cs_preconditions", "missing": problems})

    # CAS（登録→登録中）勝者のみ実行（並行 2 本でも CloudSign 作成は 1 回）
    next_rev = await _claim(record_id, revision, STATUS_CS_WORKING)
    if next_rev is None:
        logger.info("[CONTRACT] cs cas lost record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "cas_lost"})

    # PDF 生成（テンプレ=単一の正）+ 凍結検証（docx 段・PDF 段の二層）。
    # 検証失敗はここで 500（系統的エラー→再配送が reconcile で「要確認」へ）
    verify_template_integrity()
    docx_bytes = fill_template(TEMPLATE_PATH, build_fill_data(record))
    verify_frozen_clause(docx_bytes)
    pdf_bytes = contract_pdf.docx_to_pdf_bytes(docx_bytes)
    verify_frozen_pdf(pdf_bytes)
    logger.info("[CONTRACT] pdf generated record_id=%s bytes=%d",
                emit(record_id, "record_id", "log", "operator"),
                len(pdf_bytes))

    # CloudSign 作成→添付→宛先。途中失敗は下書き削除（部分状態を残さない）
    # ＋掃除成功時のみ巻き戻し（登録中→登録）→ raise → 500 → 再配送で
    # 自動再試行。掃除失敗時は巻き戻さない（reconcile が「要確認」へ倒す）
    doc_id = None
    try:
        doc_id = _cs_create_document(record_id)
        _cs_attach_pdf(doc_id, pdf_bytes)
        _cs_add_participant(doc_id, email, _fv(record, _REQUIRED_NAME))
    except Exception:
        cleaned = _cs_delete_draft(doc_id) if doc_id else True
        if cleaned:
            try:
                await hub_kintone.update_record(
                    _APP, record_id, {FIELD_STATUS: STATUS_CS_TRIGGER},
                    revision=next_rev)
            except Exception:
                logger.error("[CONTRACT] cs status rollback failed "
                             "record_id=%s",
                             emit(record_id, "record_id", "log", "operator"))
        raise

    await hub_kintone.update_record(_APP, record_id, {
        FIELD_CS_DOC_ID: doc_id,
        FIELD_STATUS: STATUS_CS_DONE,
    }, revision=next_rev)
    logger.info("[CONTRACT] cloudsign registered record_id=%s",
                emit(record_id, "record_id", "log", "operator"))
    return JSONResponse(status_code=200, content={
        "ok": True, "record_id": record_id, "cloudsign": True})


async def _reconcile_cs_working(record_id: str, revision: str):
    """CONTRACT-GEN-2 reconcile: 「クラウドサイン登録中」で停止した行の回収。

    CloudSign 側に下書きが残っている可能性がある（外部状態・kintone からは
    機械確認できない）ため、v1 の回収と異なり自動再実行はせず、常に CAS で
    「要確認」へ倒して管理者通知（二重下書き防止の fail-closed）。"""
    next_rev = await _claim(record_id, revision, STATUS_REVIEW)
    if next_rev is None:
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "cas_lost"})
    await _notify(
        f"【委任契約書】案件 No.{record_id} はクラウドサイン登録が中断した"
        "状態のため「要確認」にしました。CloudSign 画面で下書きの有無を確認し"
        "（重複下書きがあれば削除）、再実行する場合は契約書ステータスを"
        f"「{STATUS_CS_TRIGGER}」に設定し直してください")
    return JSONResponse(status_code=200,
                        content={"ok": True, "skip": "cs_needs_review"})


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

    # 本文ステータス gate（自 update の echo=作成中/作成済/登録中/登録済 も
    # ここで落ちる）。通過はトリガ 2 値（契約書作成/クラウドサイン登録）のみ
    try:
        status_in_webhook = body["record"][FIELD_STATUS]["value"]
    except (KeyError, TypeError):
        status_in_webhook = None
    if status_in_webhook not in (STATUS_TRIGGER, STATUS_CS_TRIGGER):
        logger.info("[CONTRACT] not triggered record_id=%s",
                    emit(record_id, "record_id", "log", "operator"))
        return JSONResponse(status_code=200,
                            content={"ok": True, "skip": "not_triggered"})

    try:
        record = await hub_kintone.get_record(_APP, record_id)
        current = _fv(record, FIELD_STATUS)
        revision = _fv(record, "$revision")

        # fix1[01]: 正本の完全一致検証（stale 本文は作用 0 で skip）。
        # dispatch は本文でなく正本ステータスに対して行う
        if current in (STATUS_DONE, STATUS_CS_DONE):
            logger.info("[CONTRACT] already done record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "already_done"})
        if not revision.isdigit():
            logger.info("[CONTRACT] stale status record_id=%s",
                        emit(record_id, "record_id", "log", "operator"))
            return JSONResponse(status_code=200,
                                content={"ok": True, "skip": "stale_status"})
        if current == STATUS_WORKING:
            return await _reconcile_working(record_id, record, revision)
        if current == STATUS_CS_WORKING:
            return await _reconcile_cs_working(record_id, revision)
        if current == STATUS_CS_TRIGGER:
            return await _cloudsign_flow(record_id, record, revision)
        if current != STATUS_TRIGGER:
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
