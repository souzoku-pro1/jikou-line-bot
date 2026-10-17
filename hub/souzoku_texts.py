"""souzoku_texts — 遺産分割協議書（案）の条文（SOUZOKU-4・QUEUE-2026-10-17-H H-5）

全文言は **仮置き（status=needs_approval・大野承認待ち）**。承認後は text／status と PIN だけ差し替える（コードは触らない）。
協議書は「弁護士確認前の案」として生成され（透かし文言 K-0 を必ず先頭に置く）、App への添付はしない（画面からダウンロードのみ）。
条文は差し込みだけ（LLM は使わない・docs/souzoku-1-design.md §4）。差し込み値は {{…}}。氏名・住所は本票では差し込まず空欄線（弁護士が記入）。
"""
import hashlib
import json

TABLE_VERSION = "souzoku-kyogisho-texts-v1"
STATUS_NEEDS_APPROVAL = "needs_approval"
STATUS_APPROVED = "approved"

TEXTS: dict = {
    "K-0": {"label": "透かし（弁護士確認前）", "status": STATUS_NEEDS_APPROVAL,
            "text": "【弁護士確認前の案】本書は遺産分割協議書の案であり、弁護士が内容を確認するまで効力を持つ文書ではありません。"},
    "K-1": {"label": "表題", "status": STATUS_NEEDS_APPROVAL, "text": "遺産分割協議書（案）"},
    "K-2": {"label": "前文", "status": STATUS_NEEDS_APPROVAL,
            "text": "被相続人 {{被相続人}}（{{死亡日}}死亡）の遺産について、共同相続人全員は協議の結果、次のとおり遺産を分割することに合意した。"},
    "K-3": {"label": "相続人の表示（1 行）", "status": STATUS_NEEDS_APPROVAL,
            "text": "相続人（{{続柄}}）　住所 ＿＿＿＿＿＿＿＿＿＿＿＿＿＿　氏名 ＿＿＿＿＿＿＿＿＿＿　法定相続分 {{法定相続分}}"},
    "K-4": {"label": "取得条項（見出し）", "status": STATUS_NEEDS_APPROVAL,
            "text": "第{{条}}条　相続人（{{続柄}}）は、次の遺産を取得する。"},
    "K-5": {"label": "財産の表示（1 行）", "status": STATUS_NEEDS_APPROVAL, "text": "　（{{番号}}）{{財産}}"},
    "K-6": {"label": "代償金条項", "status": STATUS_NEEDS_APPROVAL,
            "text": "第{{条}}条　相続人（{{続柄}}）は、前記の遺産を取得する代償として、相続人（{{相手}}）に対し金{{金額}}円を{{支払期限}}までに支払う。"},
    "K-7": {"label": "記載外の遺産", "status": STATUS_NEEDS_APPROVAL,
            "text": "第{{条}}条　本協議書に記載のない遺産及び後日判明した遺産については、共同相続人全員が改めて協議して定める。"},
    "K-8": {"label": "後文", "status": STATUS_NEEDS_APPROVAL,
            "text": "以上のとおり協議が成立したことを証するため、本協議書を{{通数}}通作成し、各相続人が署名押印のうえ各 1 通を保有する。"},
    "K-9": {"label": "署名欄（1 人分）", "status": STATUS_NEEDS_APPROVAL,
            "text": "相続人（{{続柄}}）　住所 ＿＿＿＿＿＿＿＿＿＿＿＿＿＿　氏名 ＿＿＿＿＿＿＿＿＿＿　㊞"},
    "K-10": {"label": "未割付の注記", "status": STATUS_NEEDS_APPROVAL,
             "text": "（注）次の遺産は取得者が未定のため本案に取得条項がありません: {{財産}}"},
    "K-11": {"label": "評価未確定の注記", "status": STATUS_NEEDS_APPROVAL,
             "text": "（注）評価額が未確定の遺産があるため、法定相続分との差異は参考値です。"},
}
TEXT_IDS = tuple(TEXTS)


def table_sha256() -> str:
    return hashlib.sha256(json.dumps({"version": TABLE_VERSION, "texts": TEXTS}, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


TABLE_SHA256_PIN = "ebe3d120fff9266274cb0c1a6030847abe62db54e629f9bde1e960e361a1ab2f"


def verify_pin() -> bool:
    return table_sha256() == TABLE_SHA256_PIN


def text(text_id: str) -> str:
    """pin 不一致なら空文字（fail-closed＝生成側は空の条文を拒否する）。"""
    if not verify_pin():
        return ""
    return str((TEXTS.get(text_id) or {}).get("text") or "")


def all_approved() -> bool:
    return verify_pin() and all(e.get("status") == STATUS_APPROVED for e in TEXTS.values())


def approval_table() -> list[dict]:
    return [{"id": i, "label": e["label"], "status": e["status"], "text": e["text"]} for i, e in TEXTS.items()]
