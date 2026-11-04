"""jikou_stage_texts — 時効案件進行アプリから依頼者へ送る固定文（JIKOU-STAGE-APP-1・QUEUE-2026-10-20-HI I-4'・**要承認**）

送信は kintone のプロセス管理ボタン（大野の「時効確認済にする」「不成立にする」＝承認）を起点に、承認台帳
（hub/approved_texts.APPROVED_SHA256）に本文 SHA が載っている固定文だけを既存の人による明示送信経路で送る。
台帳に無ければ送らず「要対応」。差し込みは {{URL}}（本人専用 URL・72 時間・PII なし）と {{債権者名}} のみ。
氏名・金額・日付は入れない（RV-10）。
"""

import hashlib
import json

TABLE_VERSION = "jikou-stage-texts-v1"
STATUS_NEEDS_APPROVAL = "needs_approval"
STATUS_APPROVED = "approved"

TEXTS: dict = {
    "R-1": {"label": "時効確認済（報告書あり）", "status": STATUS_NEEDS_APPROVAL,
            "text": ("{{債権者名}}に対する時効援用について、同社から時効が完成した旨の回答を受領しましたので、ご報告いたします。"
                     "完了報告書は次のリンクからご確認ください（72 時間有効・ご本人専用）。\n{{URL}}\n"
                     "ご不明な点があれば、この LINE でお知らせください。")},
    "R-2": {"label": "全件完了", "status": STATUS_NEEDS_APPROVAL,
            "text": ("ご依頼いただいた全ての債権者について、時効援用の手続きが完了しました。"
                     "これをもって本件のご依頼は終了となります。この度はご依頼いただき、ありがとうございました。"
                     "今後、債権者から連絡があった場合は、この LINE でご相談ください。")},
    "R-3": {"label": "不成立", "status": STATUS_NEEDS_APPROVAL,
            "text": ("{{債権者名}}に対する時効援用について、同社から時効の完成を認めない旨の回答がありました。"
                     "今後の進め方（支払いの要否・分割の相談など）は、弁護士から改めてご連絡いたします。"
                     "債権者から直接の連絡があっても、住所等をお伝えにならず、この LINE でご相談ください。")},
}
TEXT_IDS = tuple(TEXTS)
SLOT_URL = "{{URL}}"
SLOT_CREDITOR = "{{債権者名}}"


def table_sha256() -> str:
    return hashlib.sha256(json.dumps({"version": TABLE_VERSION, "texts": TEXTS}, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


TABLE_SHA256_PIN = "08c42e40c1a60eadcb9dd03f969f51db696becf5c3bef2a00c2777caed40dd29"


def verify_pin() -> bool:
    return table_sha256() == TABLE_SHA256_PIN


def text(text_id: str) -> str:
    if not verify_pin():
        return ""
    return str((TEXTS.get(text_id) or {}).get("text") or "")


def approval_table() -> list[dict]:
    return [{"id": i, "label": e["label"], "status": e["status"], "text": e["text"]} for i, e in TEXTS.items()]
