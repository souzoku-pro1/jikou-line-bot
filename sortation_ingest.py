"""POST /sortation/ingest — 書類仕分け第1段 T2+T3（判定エンドポイント＋LINE照会通知）

設計: 2026-07-06 調査報告 §3 T2/T3（koseki_ingest 型を踏襲）
- 受領した PDF を OCR → 候補顧客リスト（customer_directory）を注入した
  Claude tool use 強制判定 → 仕分け先を応答する。**Drive は触らない**
  （移動・リネームは GAS 側の責務。T4 が folder_name / suggested_filename を使う）
- kintone への登録もしない（第1段の冪等は Railway ログ上の重複検知まで:
  プロセス内で見た drive_file_id を記憶し、再送は [SORTATION] duplicate を
  ログに出す。挙動は変えない＝同一入力には同一判定を返す）

応答契約:
  {"action": "auto"|"ask", "doc_type": str, "confidence": float,
   "customer": {"record_id", "name", "folder_name"} | null,
   "suggested_filename": str | null}
- action=auto: confidence >= SORTATION_AUTO_THRESHOLD（env・既定 0.85）かつ
  customer 確定時のみ。それ以外はすべて ask（安全側）
- customer.folder_name = "No{record_id}_{氏名}"・
  suggested_filename = "{氏名}_{doc_type}_{YYYYMMDD}.pdf"（JST）。
  customer 未確定時は suggested_filename も null（氏名が決まらないため）
- ask のとき ATTORNEY_LINE_USER_ID へ LINE 照会通知（スロットルなし）。
  通知失敗は応答を壊さない（縮退）
- OCR 失敗・Claude 全断は action=ask / doc_type=不明 の安全側縮退＋LINE 通知
  （env 欠落 GOOGLE_VISION_API_KEY は運用設定ミスのため 500 の明示エラー)

token 認証: ?token=（env SORTATION_INGEST_TOKEN）。不一致・env 未設定は
404 の「存在しないフリ」（koseki_ingest と同じ流儀・探信に file 必須 422 を返さない）
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import anthropic
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from claude_gateway import create_message_with_fallback
from customer_directory import Candidate, list_candidates
from hub import kintone
from hub.notify import push_line_message
from hub.redact import emit
from hub.service_auth import BodyCachingRoute, ingest_guard  # RV-04b dual-accept 結線

router = APIRouter(route_class=BodyCachingRoute)

logger = logging.getLogger("sortation_ingest")

# 仕分けログ（App 38・第2段②）。ask 判定の台帳＝Bot仕分け指示（照会中→確定）の対象
APP_SORTATION_LOG = kintone.KintoneApp(
    "App 38 (仕分けログ)", "APP_SORTATION_LOG", "TOKEN_SORTATION_LOG")

# doc_type は suggested_filename にそのまま入るため、ファイル名として自然な表記に
# 限る（区切りは「・」。/ や : 等ファイル名に使えない文字を入れない）
DOC_TYPES = ["戸籍", "住民票・戸籍附票", "評価証明・課税明細", "登記事項証明",
             "残高証明", "通帳", "保険", "契約書", "委任状", "印鑑証明書",
             "遺言書", "通知書・連絡文書", "請求書・領収書", "本人確認書類",
             "その他"]
DOC_TYPE_UNKNOWN = "不明"  # 判定不能の縮退時のみ（Claude には選ばせない）

_DEFAULT_THRESHOLD = 0.85
_JST = timezone(timedelta(hours=9))

# プロセス内の重複検知（第1段の冪等はログ可視化まで・再起動でリセットされてよい）
_seen_drive_file_ids: set[str] = set()

JUDGE_TOOL = {
    "name": "judge_sortation",
    "description": "書類OCRテキストの仕分け判定結果を保存する",
    "input_schema": {
        "type": "object",
        "properties": {
            "doc_type": {
                "type": "string", "enum": DOC_TYPES,
                "description": "書類の種類。判別できなければ「その他」。"
                               "どの候補にも明確に該当しない場合は「その他」を"
                               "選ぶこと。近そうな候補に寄せない",
            },
            "doc_type_confidence": {
                "type": "number",
                "description": "書類の種類（doc_type）の判定にどれだけ自信が"
                               "あるか（0〜1）。顧客帰属の自信（confidence）とは"
                               "別。様式・記載内容から種類が明確なら高く、"
                               "断片的・不鮮明なら低く",
            },
            "customer_record_id": {
                "type": ["string", "null"],
                "description": "候補リスト中で該当する顧客の record_id。"
                               "該当なし・確信が持てなければ null",
            },
            "confidence": {
                "type": "number",
                "description": "候補リスト中の特定顧客にこの書類が帰属すると"
                               "判断できる自信（0〜1）。候補に該当なし・"
                               "判別材料不足なら低く",
            },
            "reason": {
                "type": "string",
                "description": "判定根拠（帰属を決めた/決められなかった理由を簡潔に）",
            },
        },
        "required": ["doc_type", "doc_type_confidence", "customer_record_id",
                     "confidence", "reason"],
    },
}


def _threshold() -> float:
    return float(os.environ.get("SORTATION_AUTO_THRESHOLD", str(_DEFAULT_THRESHOLD)))


# ── 回送（S5-3 T1・2026-07-07 裁定）────────────────────────────────────────
# auto（顧客確定）かつ 対象種別 かつ doc_type_confidence ≥ 閾値 のときのみ、
# 読解ラインへ Railway 内部の関数呼び出しで回送する（pdf_bytes を流用・
# case_hint=顧客レコードID 付与＝案件紐付け不能の要確認が原理的に消える）。
# ask は回送しない（確定時回送は T3）・相談カードは対象外・OCR 2回は第1版許容。
# 下流の品質ゲート（戸籍=要再読解・登記=validate/確信度→要確認）が第2の網

_FORWARD_LINES = {"戸籍": "koseki", "登記事項証明": "registry",
                  "評価証明・課税明細": "valuation"}  # S4-M3（2026-07-07 裁定）


def _forward_enabled() -> bool:
    """既定無効（実機有効化は明示指示後・SORTATION_FORWARD_ENABLED=1）"""
    return os.environ.get("SORTATION_FORWARD_ENABLED") == "1"


def _split_enabled() -> bool:
    """書類分割層（D1-2）の有効化フラグ（既定無効）"""
    return os.environ.get("SORTATION_SPLIT_ENABLED") == "1"


async def _try_split_analysis(pdf_bytes: bytes, vision_key: str,
                              file_name: str) -> tuple[str | None, dict | None]:
    """D1-2: ページ別 OCR → 区間判定。失敗・分割不能は (結合テキスト or None, None)
    を返して従来経路へ縮退（安全側）。成功時は (結合テキスト, 分割結果)"""
    try:
        from main import _ocr_pdf_pages  # 実行時 import（循環回避）
        page_texts = _ocr_pdf_pages(pdf_bytes, vision_key)
    except Exception as e:
        logger.info("[SORTATION] ページ別OCRに失敗（従来経路へ） file=%s: %s %s",
                    emit(file_name, "external_ref", "log", "operator"),
                    type(e).__name__,
                    emit(e, "vendor_raw", "log", "operator"))
        return None, None
    ocr_text = "\n\n".join(page_texts)
    try:
        from document_splitter import analyze_segments  # 実行時 import（循環回避）
        result = await analyze_segments(page_texts)
    except Exception as e:
        logger.info("[SORTATION] 区間判定に失敗（従来経路へ） file=%s: %s %s",
                    emit(file_name, "external_ref", "log", "operator"),
                    type(e).__name__,
                    emit(e, "vendor_raw", "log", "operator"))
        return ocr_text, None
    if result.get("status") != "ok":
        # 分割不能の明示シグナル: 分割せず全体を従来経路へ（ask に落ちるかは
        # 従来判定に委ねる・裁定どおり）
        logger.info("[SORTATION] 分割不能（従来経路へ） file=%s: %s",
                    emit(file_name, "external_ref", "log", "operator"),
                    emit(result.get('reason'), "freetext", "log", "operator"))
        return ocr_text, None
    if not result.get("needs_split"):
        return ocr_text, None  # 単一区間 = 高速パス（従来経路そのまま）
    return ocr_text, result


async def _forward_fragments(segments: list[dict], pdf_bytes: bytes,
                             file_name: str, parent_fid: str,
                             customer: Candidate) -> list[dict]:
    """D1-2: 複数区間の各断片を種別ゲートに通して個別回送。
    断片1つの失敗（split/forward）は他の断片・auto 成功を壊さない縮退"""
    try:
        from document_splitter import split_pdf  # 実行時 import（循環回避）
        fragments = split_pdf(pdf_bytes, segments)
    except Exception as e:
        logger.info("[SORTATION] 断片化に失敗（回送なし・仕分け結果は不変） file=%s: %s %s",
                    emit(file_name, "external_ref", "log", "operator"),
                    type(e).__name__,
                    emit(e, "vendor_raw", "log", "operator"))
        return [{"status": "error", "error": f"断片化失敗: {str(e)[:150]}"}]
    forwarded: list[dict] = []
    for seg, fragment in zip(segments, fragments):
        pages = f"p{seg['start_page']}-{seg['end_page']}"
        result = await _forward_to_line(
            seg["doc_type"], float(seg.get("confidence") or 0.0),
            fragment, f"{file_name}#{pages}", f"{parent_fid}#{pages}", customer)
        if result is not None:  # ゲート不通過（対象外種別等）の断片は載せない
            forwarded.append({**result, "pages": pages,
                              "doc_type": seg["doc_type"]})
    return forwarded


def _forward_threshold() -> float:
    return float(os.environ.get("SORTATION_FORWARD_THRESHOLD", "0.85"))


async def _forward_to_line(doc_type: str, doc_type_conf: float,
                           pdf_bytes: bytes, file_name: str, fid: str,
                           customer: Candidate) -> dict | None:
    """ゲート通過時のみ読解ラインへ回送。回送失敗は auto 成功を壊さない
    （ログ＋forwarded.status=error の縮退）。ゲート不通過は None（キー自体なし）"""
    line = _FORWARD_LINES.get(doc_type)
    if line is None or doc_type_conf < _forward_threshold():
        return None
    try:
        if line == "koseki":
            from koseki_ingest import ingest_koseki_pdf
            result = await ingest_koseki_pdf(
                pdf_bytes, file_name,
                case_hint=customer.record_id,
                case_app_hint=os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
                drive_file_id=fid)  # 冪等キー貫通（専用フォルダ経由との二重防止）
        elif line == "valuation":  # S4-M3: case_hint/drive_file_id 貫通は T1 と同じ型
            from valuation_ingest import ingest_valuation_pdf
            result = await ingest_valuation_pdf(
                pdf_bytes, file_name,
                case_hint=customer.record_id,
                case_app_hint=os.environ.get("SOUZOKU_KINTONE_APP_ID", ""),
                drive_file_id=fid)
        else:
            from registry_ingest import ingest_registry_pdf
            result = await ingest_registry_pdf(
                pdf_bytes, file_name,
                case_hint=customer.record_id, drive_file_id=fid)
        forwarded = {"line": line, "status": result.get("status")}
        for key in ("kintone_record_id", "results"):
            if key in result:
                forwarded[key] = result[key]
        logger.info("[SORTATION] forwarded file=%s",
                    emit(file_name, "external_ref", "log", "operator"))
        return forwarded
    except Exception as e:
        logger.info("[SORTATION] 回送に失敗（仕分け結果は不変） file=%s: %s %s",
                    emit(file_name, "external_ref", "log", "operator"),
                    type(e).__name__,
                    emit(e, "vendor_raw", "log", "operator"))
        return {"line": line, "status": "error", "error": str(e)[:200]}


def _today_jst() -> str:
    return datetime.now(_JST).strftime("%Y%m%d")


def _ocr_pdf(pdf_bytes: bytes, api_key: str) -> str:
    """Vision files:annotate による OCR（既存 /ocr/fixed-asset・koseki_ingest と同一実装を共用）"""
    from main import _ocr_pdf_bytes  # 実行時 import（循環 import 回避）
    return _ocr_pdf_bytes(pdf_bytes, api_key)


def _get_client() -> anthropic.AsyncAnthropic:
    return anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY") or "unset")


def _build_prompt(ocr_text: str, candidates: list[Candidate]) -> str:
    lines = [c.label() + f" record_id={c.record_id}" for c in candidates]
    roster = "\n".join(f"- {line}" for line in lines) if lines else "（候補なし）"
    return (
        "あなたは法律事務所の書類仕分け係です。以下の書類OCRテキストを読み、"
        "書類の種類と、候補顧客リストの中の誰に帰属するかを判定してください。\n"
        "- customer_record_id は候補リストにある record_id のみ。リスト外の値を作らない\n"
        "- 候補に該当がない・判別材料が足りない場合は customer_record_id=null とし、"
        "confidence を低くする（無理に選ばない）\n\n"
        f"=== 候補顧客リスト ===\n{roster}\n\n"
        f"=== 書類OCRテキスト ===\n{ocr_text}\n=== END ==="
    )


async def _judge_with_claude(ocr_text: str, candidates: list[Candidate]) -> dict:
    """OCR テキストを仕分け判定する（tool use 強制・R3/D2 と同流儀）"""
    response = await create_message_with_fallback(
        _get_client(),
        context="書類仕分け判定",
        max_tokens=1024,
        tools=[JUDGE_TOOL],
        tool_choice={"type": "tool", "name": JUDGE_TOOL["name"]},
        messages=[{"role": "user", "content": _build_prompt(ocr_text, candidates)}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == JUDGE_TOOL["name"]:
            return dict(block.input)
    raise RuntimeError(f"tool_use ブロックがない応答（stop_reason={response.stop_reason}）")


def _top_candidates(candidates: list[Candidate], ocr_text: str,
                    suggested: Candidate | None) -> list[Candidate]:
    """LINE 照会に載せる候補上位3件: Claude の示唆 → 氏名が OCR に現れるもの →
    被相続人名が OCR に現れるもの の順（該当ゼロなら空＝「候補に該当なし」表記）"""
    ranked: list[Candidate] = [suggested] if suggested else []
    for key in ("customer_name", "decedent_name"):
        for c in candidates:
            name = getattr(c, key)
            if name and name in ocr_text and c not in ranked:
                ranked.append(c)
    return ranked[:3]


def _ask_recipient() -> str:
    """照会通知の宛先解決（2026-07-06 裁定の優先順）:
    SORTATION_ASK_TO → DISPATCHBOT_ALLOWED_USER_IDS の先頭 → ATTORNEY_LINE_USER_ID。
    指示Botチャネルで送るため、通常は指示Botで認可済みの userId に届く並び"""
    explicit = os.environ.get("SORTATION_ASK_TO", "").strip()
    if explicit:
        return explicit
    allowed = os.environ.get("DISPATCHBOT_ALLOWED_USER_IDS", "")
    first = next((p.strip() for p in allowed.split(",") if p.strip()), "")
    if first:
        return first
    return os.environ.get("ATTORNEY_LINE_USER_ID", "").strip()


async def _log_ask(file_name: str, drive_file_id: str, drive_file_url: str,
                   doc_type: str, confidence: float, reason: str,
                   top: list[Candidate]) -> str | None:
    """ask 判定を仕分けログ（App 38）に登録し、レコード URL を返す。
    env 未設定は登録スキップ（None）＝従来どおり LINE 通知のみの縮退。
    登録失敗も None（照会通知を壊さない）"""
    if not (APP_SORTATION_LOG.app_id() and APP_SORTATION_LOG.token()):
        logger.info("[SORTATION] 仕分けログ登録スキップ"
                    "（APP_SORTATION_LOG / TOKEN_SORTATION_LOG 未設定）")
        return None
    try:
        record_id = await kintone.create_record(APP_SORTATION_LOG, {
            "ファイル名": file_name,
            "Drive_fileId": drive_file_id,
            "Drive_URL": drive_file_url,
            "書類種類": doc_type,
            "確信度": str(confidence),
            "判定理由": reason,
            "候補一覧": "\n".join(c.label() for c in top),
            "状態": "照会中",
        })
    except Exception as e:
        logger.info("[SORTATION] 仕分けログ登録に失敗（照会通知は継続）: %s %s",
                    type(e).__name__,
                    emit(e, "vendor_raw", "log", "operator"))
        return None
    return (f"{kintone._base_url()}/k/{APP_SORTATION_LOG.app_id()}"
            f"/show#record={record_id}")


async def _notify_ask(file_name: str, drive_file_url: str, doc_type: str,
                      confidence: float, reason: str,
                      top: list[Candidate], log_url: str | None = None) -> None:
    """照会通知（T3）。業務指示Botチャネル名義で送る。
    宛先未解決・送信失敗は縮退（応答を壊さない）"""
    attorney_id = _ask_recipient()
    if not attorney_id:
        logger.info("[SORTATION] 照会通知スキップ（SORTATION_ASK_TO / "
                    "DISPATCHBOT_ALLOWED_USER_IDS / ATTORNEY_LINE_USER_ID すべて未設定）")
        return
    lines = [
        "【書類仕分け照会】自動仕分けできませんでした",
        f"ファイル: {file_name}",
        f"Drive: {drive_file_url or '（リンクなし）'}",
        f"書類種類: {doc_type} / 確信度: {confidence:.2f}",
        f"理由: {reason}",
    ]
    if top:
        lines.append("候補:")
        lines += [f"{i}. {c.label()}" for i, c in enumerate(top, 1)]
    else:
        lines.append("候補: 該当なし")
    lines.append("Drive で確認のうえ手動で仕分けしてください。")
    if log_url:
        lines.append(f"仕分けログ: {log_url}")
    try:
        await push_line_message(attorney_id, "\n".join(lines),
                                token_env="DISPATCHBOT_CHANNEL_ACCESS_TOKEN")
    except Exception as e:
        logger.info("[SORTATION] 照会通知に失敗（応答は継続）: %s %s",
                    type(e).__name__,
                    emit(e, "vendor_raw", "log", "operator"))


def _customer_payload(c: Candidate) -> dict:
    return {"record_id": c.record_id, "name": c.customer_name,
            "folder_name": f"No{c.record_id}_{c.customer_name}"}


@router.post("/sortation/ingest")
async def sortation_ingest(_auth: None = Depends(ingest_guard("SORTATION_INGEST_TOKEN")),
                           # file は意図的に optional: File(...) だと探信に 422 が
                           # 返り 404 偽装より先に存在が漏れる（koseki_ingest と同じ）
                           file: UploadFile | None = File(default=None),
                           drive_file_id: str | None = Form(default=None),
                           drive_file_url: str | None = Form(default=None)):
    """未整理フォルダの PDF を受領し、仕分け判定を返す（auto=GAS が移動 / ask=照会通知済み）。
    認証は前段の ingest_guard（RV-04b dual-accept）が担う。"""
    vision_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not vision_key:
        raise HTTPException(status_code=500,
                            detail="環境変数が未設定です: GOOGLE_VISION_API_KEY")

    if file is None or not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    pdf_bytes = await file.read()
    file_name = file.filename
    url = (drive_file_url or "").strip()

    fid = (drive_file_id or "").strip()
    # RV-05-13: flag ON は durable 台帳（ingestion_receipt）で冪等/可視化/fencing。
    # flag OFF は現行 process-memory（byte 同一）。台帳はレスポンス shape を変えない
    # （kintone upsert 冪等で再処理安全・shadow）。forward だけ claim で排他（二重forward回避）。
    from hub.durable_inbound import durable_enabled
    _receipt = None          # (receipt_id, epoch) or None
    _can_forward = True      # flag ON で claim できた request のみ True（fencing）
    if durable_enabled():
        from hub import ingestion_receipt as _ir
        _sha = hashlib.sha256(pdf_bytes).hexdigest()
        try:
            _rid = await _ir.upsert_receipt(
                ingest_type="sortation", caller_id="gas",
                source_file_id=fid or _sha, source_sha256=_sha,
                case_hint=(drive_file_url or None))
        except ValueError:
            raise HTTPException(status_code=400, detail="冪等キー要素が不正です")
        except _ir.ReceiptConflict:
            raise HTTPException(status_code=409, detail="duplicate_suspect")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=503, detail="event store unavailable")
        _epoch = await _ir.claim(_rid)
        if _epoch is None:
            _can_forward = False   # 並行/重複: 二重 forward を避ける（応答は返す）
        else:
            _receipt = (_rid, _epoch)
    elif fid:
        if fid in _seen_drive_file_ids:
            logger.info("[SORTATION] duplicate drive_file_id=%s（再判定して同一契約で応答）",
                        emit(fid, "external_ref", "log", "operator"))
        _seen_drive_file_ids.add(fid)

    # D1-2: 分割層（フラグ配下）。ページ別 OCR→区間判定。単一区間・分割不能・
    # 失敗は split_result=None（従来経路そのまま＝高速パス/安全側）
    split_result: dict | None = None
    pre_text: str | None = None
    if _split_enabled():
        pre_text, split_result = await _try_split_analysis(
            pdf_bytes, vision_key, file_name)

    # RV-05-13: flag ON は vendor 呼出前を durable marker（vendor_pre）。crash 復帰で
    # 「vendor 前を証明」できる（reconciliation が PENDING_RETRY 可視化・再処理は GAS 再送）。
    # 各遷移は epoch++（H-D4-01）のため、request は自身の最新 epoch を追跡する。
    if _receipt is not None:
        from hub import ingestion_receipt as _ir
        _ep = await _ir.mark_phase(_receipt[0], _receipt[1], _ir.ST_VENDOR_PRE)
        if _ep is not None:
            _receipt = (_receipt[0], _ep)

    # OCR → 候補注入 → Claude 判定。失敗はすべて ask の安全側縮退（doc_type=不明）
    # （複数区間でも顧客判定は親PDF全体で1回・裁定）
    judged: dict | None = None
    candidates: list[Candidate] = []
    ocr_text = ""
    failure = ""
    try:
        ocr_text = pre_text if pre_text is not None else \
            _ocr_pdf(pdf_bytes, vision_key)
        candidates = await list_candidates()
        judged = await _judge_with_claude(ocr_text, candidates)
    except Exception as e:
        failure = str(e)[:200]
        logger.info("[SORTATION] 判定不能のため照会へ縮退 file=%s: %s %s",
                    emit(file_name, "external_ref", "log", "operator"),
                    type(e).__name__,
                    emit(e, "vendor_raw", "log", "operator"))

    if judged is None:
        doc_type, confidence, doc_type_conf = DOC_TYPE_UNKNOWN, 0.0, 0.0
        reason = f"判定処理が実行できませんでした（{failure}）"
        customer = None
    else:
        doc_type = judged.get("doc_type") if judged.get("doc_type") in DOC_TYPES \
            else "その他"
        try:
            confidence = min(max(float(judged.get("confidence") or 0.0), 0.0), 1.0)
        except (TypeError, ValueError):
            confidence = 0.0
        try:
            doc_type_conf = min(max(
                float(judged.get("doc_type_confidence") or 0.0), 0.0), 1.0)
        except (TypeError, ValueError):
            doc_type_conf = 0.0
        reason = str(judged.get("reason") or "")
        # customer_record_id は候補リスト内のもののみ有効（リスト外の創作は棄却）
        suggested_id = judged.get("customer_record_id")
        customer = next((c for c in candidates
                         if suggested_id and c.record_id == str(suggested_id)), None)
        if suggested_id and customer is None:
            reason = f"候補リスト外の record_id={suggested_id} が返されたため棄却。" + reason
            confidence = 0.0

    # D1-2: 複数区間のとき応答の doc_type は主種別（最大ページ数の区間・同数は先頭）
    segments = (split_result or {}).get("segments")
    if segments:
        main_seg = max(segments, key=lambda s: (s["end_page"] - s["start_page"],
                                                -s["start_page"]))
        doc_type = main_seg["doc_type"]

    action = "auto" if customer is not None and confidence >= _threshold() else "ask"
    logger.info("[SORTATION] judged file=%s customer=%s",
                emit(file_name, "external_ref", "log", "operator"),
                emit(customer.record_id if customer else None, "record_id", "log", "operator"))

    # RV-05-13: flag ON は downstream（ask 保存/forward）を SENDING marker で囲み、
    # 失敗は PENDING_RETRY（成功 ACK にしない・RV-13）→ 例外を再送出（5xx・GAS 再送）。
    try:
        if _receipt is not None:
            from hub import ingestion_receipt as _ir
            _ep = await _ir.mark_phase(_receipt[0], _receipt[1], _ir.ST_SENDING)
            if _ep is not None:
                _receipt = (_receipt[0], _ep)

        if action == "ask":
            top = _top_candidates(candidates, ocr_text, customer)
            log_url = await _log_ask(file_name, fid, url, doc_type, confidence,
                                     reason, top)
            await _notify_ask(file_name, url, doc_type, confidence, reason, top,
                              log_url)

        # D1-2: 混在時の suggested_filename は「氏名_主種別ほかN件_日付」
        filename_doc = f"{doc_type}ほか{len(segments) - 1}件" if segments else doc_type
        response = {
            "action": action,
            "doc_type": doc_type,
            "confidence": confidence,
            "customer": _customer_payload(customer) if customer else None,
            "suggested_filename":
                f"{customer.customer_name}_{filename_doc}_{_today_jst()}.pdf"
                if customer else None,
        }
        # S5-3 T1/D1-2: auto（顧客確定）のみ読解ラインへ回送（既存キーは不変・追加キー
        # のみ＝GAS 無変更）。RV-05-13: flag ON の並行/重複（claim 不可）は二重 forward 回避で回送しない。
        if action == "auto" and _forward_enabled() and _can_forward:
            if segments:
                parent_fid = fid or f"sha256:{hashlib.sha256(pdf_bytes).hexdigest()}"
                forwarded_list = await _forward_fragments(
                    segments, pdf_bytes, file_name, parent_fid, customer)
                if forwarded_list:
                    response["forwarded"] = forwarded_list
            else:
                forwarded = await _forward_to_line(doc_type, doc_type_conf, pdf_bytes,
                                                   file_name, fid, customer)
                if forwarded is not None:
                    response["forwarded"] = forwarded
    except Exception:
        if _receipt is not None:
            from hub import ingestion_receipt as _ir
            try:
                await _ir.mark_pending_retry(_receipt[0], _receipt[1])
            except Exception:
                pass
        raise   # 成功 ACK にしない（5xx・GAS 再送）

    if _receipt is not None:
        from hub import ingestion_receipt as _ir
        await _ir.mark_terminal(_receipt[0], _receipt[1], _ir.ST_COMPLETED,
                                downstream_refs=(response.get("customer") or {}).get("record_id")
                                if isinstance(response.get("customer"), dict) else None)
    return response
