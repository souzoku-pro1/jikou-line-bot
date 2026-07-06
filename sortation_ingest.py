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

import json
import os
from datetime import datetime, timedelta, timezone

import anthropic
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from claude_gateway import create_message_with_fallback
from customer_directory import Candidate, list_candidates
from hub.notify import push_line_message
from hub.webhook_auth import verify_token

router = APIRouter()

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
        "required": ["doc_type", "customer_record_id", "confidence", "reason"],
    },
}


def _threshold() -> float:
    return float(os.environ.get("SORTATION_AUTO_THRESHOLD", str(_DEFAULT_THRESHOLD)))


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


async def _notify_ask(file_name: str, drive_file_url: str, doc_type: str,
                      confidence: float, reason: str,
                      top: list[Candidate]) -> None:
    """照会通知（T3）。宛先未設定・送信失敗は縮退（応答を壊さない）"""
    attorney_id = os.environ.get("ATTORNEY_LINE_USER_ID", "")
    if not attorney_id:
        print("[SORTATION] 照会通知スキップ（ATTORNEY_LINE_USER_ID 未設定）")
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
    try:
        await push_line_message(attorney_id, "\n".join(lines))
    except Exception as e:
        print(f"[SORTATION] 照会通知に失敗（応答は継続）: {e}")


def _customer_payload(c: Candidate) -> dict:
    return {"record_id": c.record_id, "name": c.customer_name,
            "folder_name": f"No{c.record_id}_{c.customer_name}"}


@router.post("/sortation/ingest")
async def sortation_ingest(token: str = "",
                           # file は意図的に optional: File(...) だと探信に 422 が
                           # 返り 404 偽装より先に存在が漏れる（koseki_ingest と同じ）
                           file: UploadFile | None = File(default=None),
                           drive_file_id: str | None = Form(default=None),
                           drive_file_url: str | None = Form(default=None)):
    """未整理フォルダの PDF を受領し、仕分け判定を返す（auto=GAS が移動 / ask=照会通知済み）"""
    if not verify_token(token, "SORTATION_INGEST_TOKEN"):
        raise HTTPException(status_code=404, detail="Not Found")

    vision_key = os.environ.get("GOOGLE_VISION_API_KEY", "")
    if not vision_key:
        raise HTTPException(status_code=500,
                            detail="環境変数が未設定です: GOOGLE_VISION_API_KEY")

    if file is None or not file.filename or not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDFファイルを送信してください")

    pdf_bytes = await file.read()
    file_name = file.filename
    url = (drive_file_url or "").strip()

    # 冪等（第1段）: プロセス内の重複検知をログに残す。挙動は変えない
    fid = (drive_file_id or "").strip()
    if fid:
        if fid in _seen_drive_file_ids:
            print(f"[SORTATION] duplicate drive_file_id={fid}（再判定して同一契約で応答）")
        _seen_drive_file_ids.add(fid)

    # OCR → 候補注入 → Claude 判定。失敗はすべて ask の安全側縮退（doc_type=不明）
    judged: dict | None = None
    candidates: list[Candidate] = []
    ocr_text = ""
    failure = ""
    try:
        ocr_text = _ocr_pdf(pdf_bytes, vision_key)
        candidates = await list_candidates()
        judged = await _judge_with_claude(ocr_text, candidates)
    except Exception as e:
        failure = str(e)[:200]
        print(f"[SORTATION] 判定不能のため照会へ縮退 file={file_name}: {e}")

    if judged is None:
        doc_type, confidence = DOC_TYPE_UNKNOWN, 0.0
        reason = f"判定処理が実行できませんでした（{failure}）"
        customer = None
    else:
        doc_type = judged.get("doc_type") if judged.get("doc_type") in DOC_TYPES \
            else "その他"
        try:
            confidence = min(max(float(judged.get("confidence") or 0.0), 0.0), 1.0)
        except (TypeError, ValueError):
            confidence = 0.0
        reason = str(judged.get("reason") or "")
        # customer_record_id は候補リスト内のもののみ有効（リスト外の創作は棄却）
        suggested_id = judged.get("customer_record_id")
        customer = next((c for c in candidates
                         if suggested_id and c.record_id == str(suggested_id)), None)
        if suggested_id and customer is None:
            reason = f"候補リスト外の record_id={suggested_id} が返されたため棄却。" + reason
            confidence = 0.0

    action = "auto" if customer is not None and confidence >= _threshold() else "ask"
    print(f"[SORTATION] judged file={file_name} action={action} doc_type={doc_type} "
          f"customer={customer.record_id if customer else None} conf={confidence}")

    if action == "ask":
        await _notify_ask(file_name, url, doc_type, confidence, reason,
                          _top_candidates(candidates, ocr_text, customer))

    return {
        "action": action,
        "doc_type": doc_type,
        "confidence": confidence,
        "customer": _customer_payload(customer) if customer else None,
        "suggested_filename":
            f"{customer.customer_name}_{doc_type}_{_today_jst()}.pdf"
            if customer else None,
    }
