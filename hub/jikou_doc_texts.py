"""jikou_doc_texts — 時効援用完了報告書の文面（JIKOU-CERT-1・QUEUE-2026-10-20-HI I-1・**要承認**）

相談者向けの文書文言は凍結コードに直書きせず本データ表に置く（hub/jikou_reply_texts と同じ流儀）。全文言は仮置き
（status=needs_approval・大野承認待ち）。表示・生成の可否は **承認台帳 hub/approved_texts.APPROVED_SHA256**（本文 SHA）だけが
決める（TEXT-APPROVAL-GATE-1）＝台帳に載るまで報告書は生成できない。承認後は台帳に SHA を追加し、本表の status と PIN を差し替える。
差し込み値は {{…}}（宛名・債権者名・通知発送日・回答受領日・回答要旨・作成日・事務所情報）。金額・LINE ID・電話番号（相談者）は入れない。
"""

import hashlib
import json

TABLE_VERSION = "jikou-cert-texts-v1"
STATUS_NEEDS_APPROVAL = "needs_approval"
STATUS_APPROVED = "approved"

TEXTS: dict = {
    "C-1": {"label": "表題", "status": STATUS_NEEDS_APPROVAL, "text": "時効援用完了報告書"},
    "C-2": {"label": "宛名", "status": STATUS_NEEDS_APPROVAL, "text": "{{宛名}} 様"},
    "C-3": {"label": "本文", "status": STATUS_NEEDS_APPROVAL,
            "text": ("当事務所は、貴殿の代理人として、{{債権者名}}に対し、{{通知発送日}}付で消滅時効援用通知書を発送し、"
                     "同社から{{回答受領日}}付で、当該債権について時効が完成した旨の回答を受領いたしました。"
                     "よって、同社に対する貴殿の債務は消滅していることをご報告いたします。")},
    "C-4": {"label": "明細: 債権者名", "status": STATUS_NEEDS_APPROVAL, "text": "債権者名　　{{債権者名}}"},
    "C-5": {"label": "明細: 通知発送日", "status": STATUS_NEEDS_APPROVAL, "text": "通知発送日　{{通知発送日}}"},
    "C-6": {"label": "明細: 回答受領日", "status": STATUS_NEEDS_APPROVAL, "text": "回答受領日　{{回答受領日}}"},
    "C-7": {"label": "明細: 回答要旨", "status": STATUS_NEEDS_APPROVAL, "text": "回答要旨　　{{回答要旨}}"},
    "C-8": {"label": "作成日", "status": STATUS_NEEDS_APPROVAL, "text": "{{作成日}}"},
    "C-9": {"label": "事務所名", "status": STATUS_NEEDS_APPROVAL, "text": "{{事務所名}}"},
    "C-10": {"label": "事務所住所", "status": STATUS_NEEDS_APPROVAL, "text": "{{事務所住所}}"},
    "C-11": {"label": "事務所電話", "status": STATUS_NEEDS_APPROVAL, "text": "電話　{{電話}}"},
    "C-12": {"label": "弁護士署名", "status": STATUS_NEEDS_APPROVAL,
             "text": "弁護士　{{弁護士名}}（{{弁護士会}}・登録番号 {{登録番号}}）"},
}
TEXT_IDS = tuple(TEXTS)
SLOTS = ("{{宛名}}", "{{債権者名}}", "{{通知発送日}}", "{{回答受領日}}", "{{回答要旨}}", "{{作成日}}",
         "{{事務所名}}", "{{事務所住所}}", "{{電話}}", "{{弁護士名}}", "{{弁護士会}}", "{{登録番号}}")


def table_sha256() -> str:
    return hashlib.sha256(json.dumps({"version": TABLE_VERSION, "texts": TEXTS}, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


TABLE_SHA256_PIN = "ecafc4715f43c9a372d28246c2024c48c9bff6ef5def636d4dac184af666afd1"


def verify_pin() -> bool:
    return table_sha256() == TABLE_SHA256_PIN


def text(text_id: str) -> str:
    """pin 不一致なら空文字（fail-closed）。表示・生成の可否は approved_texts の台帳が決める（本関数は本文を返すだけ）。"""
    if not verify_pin():
        return ""
    return str((TEXTS.get(text_id) or {}).get("text") or "")


def approval_table() -> list[dict]:
    return [{"id": i, "label": e["label"], "status": e["status"], "text": e["text"]} for i, e in TEXTS.items()]
