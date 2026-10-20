"""souzoku_intake_texts — SOUZOKU-2: 相続相談受付の相談者向け文言（**要承認**・別表・sha pin）

QUEUE-2026-10-16-R G-5。相談者向け文言は凍結コードに直書きせず本データ表に置く（hub/jikou_reply_texts.py と同じ流儀・
main には同 module が無いため相続一般用の別表を新設）。全文言は仮置き（status=needs_approval・大野承認待ち）。
承認後は text／status と PIN だけ差し替える（コードは触らない）。数値（金額・日数）は本文に書かない。
"""

import hashlib
import json

STATUS_NEEDS_APPROVAL = "needs_approval"
STATUS_APPROVED = "approved"
TABLE_VERSION = "souzoku-intake-1"

TEXTS: dict = {
    # 受付確認（画面に表示する 1 通分。LINE への自動送信はチャネルの裁定後＝docs/souzoku-1-design.md §10-1）
    "S-1": {"label": "受付確認", "status": STATUS_NEEDS_APPROVAL,
            "text": ("相続のご相談を受け付けました。ありがとうございます。いただいた内容を弁護士が確認のうえ、"
                     "この後のご案内（面談のご予約方法・ご用意いただく書類）を事務所からご連絡いたします。"
                     "お急ぎの事情（期限のある通知が届いている等）がある場合は、その旨をあわせてお知らせください。")},
    # 争いがある場合の終了文（当事務所では扱っていない旨）
    "S-2": {"label": "対象外（争いあり）", "status": STATUS_NEEDS_APPROVAL,
            "text": ("ご入力ありがとうございます。相続人の間で遺産の分け方に争いがある案件は、当事務所では取り扱っておりません。"
                     "お手数ですが、各地の弁護士会の法律相談センター等へご相談ください。"
                     "なお、争いが解消した場合や、争いの有無がはっきりしない場合は、改めてご相談ください。")},
    # 争いの有無が「わからない」の注記（受付確認に添える 1 文）
    "S-3": {"label": "争いの有無が不明のときの注記", "status": STATUS_NEEDS_APPROVAL,
            "text": "相続人の間の話し合いの状況は、弁護士が面談時に改めて確認いたします。"},
}
TEXT_IDS = tuple(TEXTS)


def table_sha256() -> str:
    return hashlib.sha256(json.dumps({"version": TABLE_VERSION, "texts": TEXTS}, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


TABLE_SHA256_PIN = "311d195164d4d5afe83b8db5da8c2cbd0806acbeb20bf9e78a7f022b6a654110"


def verify_pin() -> bool:
    return table_sha256() == TABLE_SHA256_PIN


def text(text_id: str) -> str:
    """pin 不一致なら空文字（fail-closed＝表示しない）。"""
    if not verify_pin():
        return ""
    return str((TEXTS.get(text_id) or {}).get("text") or "")


def all_approved() -> bool:
    """PRE-REVIEW-1 #307-2: flag ON でも要承認の文言が 1 件でも残っていれば受付画面を出さない（status の関所）。"""
    return verify_pin() and all(e.get("status") == STATUS_APPROVED for e in TEXTS.values())


def approval_table() -> list[dict]:
    return [{"id": i, "label": e["label"], "status": e["status"], "text": e["text"]} for i, e in TEXTS.items()]
