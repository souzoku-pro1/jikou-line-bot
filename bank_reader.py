"""通帳・残高証明の構造化読解（S6-1）: OCR 生テキスト → 口座JSON

設計: 2026-07-07 S6-1 裁定
- 第1版スコープ: **残高証明と通帳見開きの2形態**の判別と口座単位の抽出まで。
  取引明細の構造化・異常検知は本タスク外（設計書のみ後日）
- 既存 /scan（通帳→App 27）の抽出知見（残高=ページ末尾の最新残高・円整数・
  カンマ/円除去）を継承し tool use 強制に置換。1枚に複数口座が載る残高証明に
  対応するため口座は配列で全抽出
- toolスキーマのプロパティキーは英数字のみ（Anthropic API 制約・AST 静的検査が
  自動対象化）。外部契約は日本語キー——to_japanese_bank が写像（_map_flat 転用）
- 日付（基準日）は和暦原文のまま保持し、西暦は 基準日_西暦 の別キーのみ
  （変換に自信がなければ null・R3/S5 と同一規約）
- 確信度3層: フィールド別 confidence マップ・overall_confidence・
  BANK_REREAD_THRESHOLD（env・既定 0.5。判定の利用は入口=bank_ingest の責務）
"""

import os
import statistics

import anthropic

from claude_gateway import create_message_with_fallback
from config import BANK_READER_PROMPTS

DOC_FORMS = ["残高証明", "通帳", "不明"]
ACCOUNT_TYPES = ["普通", "定期", "当座", "貯蓄", "その他"]

_CONFIDENCE_MAP = {
    "type": "object",
    "description": "フィールド名（このツールの英語プロパティ名）→確信度（0〜1）",
    "additionalProperties": {"type": "number"},
}

BANK_READING_TOOL = {
    "name": "save_bank_reading",
    "description": "残高証明書・通帳見開きOCRテキストの構造化読解結果を保存する",
    "input_schema": {
        "type": "object",
        "properties": {
            "doc_form": {"type": "string", "enum": DOC_FORMS,
                         "description": "書類形態。残高証明書なら「残高証明」・"
                                        "通帳（見開き）なら「通帳」・判別できなければ「不明」"},
            "accounts": {
                "type": "array",
                "description": "口座（残高証明に複数口座が載る場合は**すべて**抽出する）",
                "items": {
                    "type": "object",
                    "properties": {
                        "bank_name": {"type": "string",
                                      "description": "金融機関名。原文のまま"},
                        "branch_name": {"type": "string",
                                        "description": "支店名。原文のまま"},
                        "account_type": {"type": "string", "enum": ACCOUNT_TYPES,
                                         "description": "預金種別。判別できなければ「その他」"},
                        "account_number": {"type": "string",
                                           "description": "口座番号。原文のまま"},
                        "holder_name": {"type": "string",
                                        "description": "名義人。原文のまま"},
                        "balance": {"type": ["integer", "null"],
                                    "description": "残高。円単位の整数（カンマ・円は"
                                                   "除去）。通帳はページ末尾の最新残高。"
                                                   "読み取れなければ null"},
                        "basis_date": {"type": "string",
                                       "description": "基準日（残高証明の証明基準日・"
                                                      "通帳は最終記帳日）。和暦原文のまま"},
                        "basis_date_seireki": {"type": ["string", "null"],
                                               "description": "基準日の西暦 YYYY-MM-DD。"
                                                              "変換に自信がなければ null"},
                        "confidence": _CONFIDENCE_MAP,
                    },
                    "required": ["bank_name", "account_number"],
                },
            },
            "confidence": _CONFIDENCE_MAP,
        },
        "required": ["doc_form", "accounts"],
    },
}

# ── 写像層（対応表のみ翻訳・未知キー素通し・日本語キー入力には恒等・非補完） ──

_TOP_JA = {"doc_form": "書類形態", "accounts": "口座"}
_ACCOUNT_JA = {"bank_name": "金融機関名", "branch_name": "支店名",
               "account_type": "預金種別", "account_number": "口座番号",
               "holder_name": "名義人", "balance": "残高",
               "basis_date": "基準日", "basis_date_seireki": "基準日_西暦"}


def _map_flat(obj, key_map: dict):
    if not isinstance(obj, dict):
        return obj
    out = {}
    for k, v in obj.items():
        jk = key_map.get(k, k)
        if jk == "confidence" and isinstance(v, dict):
            v = {key_map.get(ck, ck): cv for ck, cv in v.items()}
        out[jk] = v
    return out


def to_japanese_bank(raw: dict) -> dict:
    """Claude 出力（BANK_READING_TOOL の英語キー）を日本語キーへ写像する"""
    if not isinstance(raw, dict):
        return raw
    mapped = _map_flat(raw, _TOP_JA)
    accounts = mapped.get("口座")
    if isinstance(accounts, list):
        mapped["口座"] = [_map_flat(a, _ACCOUNT_JA) for a in accounts]
    return mapped


class BankReaderError(Exception):
    """読解が実行できなかった（Claude 応答不正等）"""


def _is_conf(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) \
        and 0 <= value <= 1


def validate_reading(reading: dict) -> list[str]:
    """日本語キー口座JSONの検証。逸脱の一覧を返す（空リスト = 適合）"""
    errors: list[str] = []
    if reading.get("書類形態") not in DOC_FORMS:
        errors.append(f"書類形態 が許容値外: {reading.get('書類形態')!r}")
    accounts = reading.get("口座")
    if not isinstance(accounts, list) or not accounts:
        errors.append("口座 が空でない配列でない")
        return errors
    for i, account in enumerate(accounts):
        if not isinstance(account, dict):
            errors.append(f"口座[{i}] がオブジェクトでない")
            continue
        if not isinstance(account.get("金融機関名"), str) or \
                not account.get("金融機関名"):
            errors.append(f"口座[{i}].金融機関名 が空でない文字列でない")
        if not isinstance(account.get("口座番号"), str):
            errors.append(f"口座[{i}].口座番号 が文字列でない")
        balance = account.get("残高")
        if balance is not None and not (isinstance(balance, int) and
                                        not isinstance(balance, bool)):
            errors.append(f"口座[{i}].残高 が整数でも null でもない: {balance!r}")
    return errors


def _collect_confidences(reading: dict) -> list[float]:
    values = [float(v) for v in (reading.get("confidence") or {}).values()
              if _is_conf(v)]
    for account in reading.get("口座") or []:
        if isinstance(account, dict):
            values += [float(v) for v in (account.get("confidence") or {}).values()
                       if _is_conf(v)]
    return values


def overall_confidence(reading: dict) -> float:
    values = _collect_confidences(reading)
    return round(statistics.fmean(values), 3) if values else 0.0


def reread_threshold() -> float:
    return float(os.environ.get("BANK_REREAD_THRESHOLD", "0.5"))


def _get_client() -> anthropic.AsyncAnthropic:
    return anthropic.AsyncAnthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY") or "unset")


async def read_bank(ocr_text: str) -> dict:
    """OCR テキスト → 日本語キーの口座JSON（tool use 強制・写像層適用済み）"""
    prompt = BANK_READER_PROMPTS["共通"].format(ocr_text=ocr_text)
    response = await create_message_with_fallback(
        _get_client(),
        context="通帳・残高証明読解",
        max_tokens=4096,
        tools=[BANK_READING_TOOL],
        tool_choice={"type": "tool", "name": BANK_READING_TOOL["name"]},
        messages=[{"role": "user", "content": prompt}],
    )
    for block in response.content:
        if block.type == "tool_use" and block.name == BANK_READING_TOOL["name"]:
            return to_japanese_bank(dict(block.input))
    raise BankReaderError(
        f"tool_use ブロックがない応答（stop_reason={response.stop_reason}）")
