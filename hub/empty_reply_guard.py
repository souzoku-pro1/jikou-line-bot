"""empty_reply_guard — JIKOU-REPLY-Q1c (b): 「確認のうえご連絡します」系の空返事の検出（純関数）

正本: 時効LINEボット_返信規則_v1.4.md R-J1（答えられない問いには相談者へ何も送らず、対応待ちを永続化し
大野に通知する。PENDING_REPLY・「申し伝えます」「改めてご案内」系は自動送信から外す）・§10-3 fail-closed
（PENDING_REPLY は送らない）。QUEUE-2026-10-12-C C-1 (b)。

- 対象は LLM が生成した返信文（顧客対応・ヒアリングの両経路）。閉集合の即時定型（court_doc_request 等）・
  J-0 は本検査の対象外（呼出側が固定文として扱う）。
- 語彙表は閉集合（EMPTY_REPLY_PATTERNS・ラベル+正規表現）。言い換え（確認の上／確認のうえ／確認して／
  担当者より／担当の者から／申し伝え／折り返し／追って／改めてご案内・ご連絡・ご返信）を吸収する。
- 判定は「空文字（空白のみ）」または「語彙表のいずれかに一致」。一致したラベルの列を返す（固定語彙＝ログ・
  降格理由にそのまま載せられる）。本文は返さない（RV-10）。
- 既存の PENDING_REPLY・PENDING_BY_CATEGORY の逐語も空返事として扱う（LLM が定型を写した場合）。
"""

import re

LABEL_EMPTY = "空の返信"
LABEL_PENDING_TEMPLATE = "確認中定型の写し"
LABEL_FOLLOW_UP_PROMISE = "確認のうえ連絡の約束"
LABEL_RELAY = "申し伝え"
LABEL_STAFF_FOLLOW_UP = "担当者からの連絡"
LABEL_CALLBACK = "折り返し・追って連絡"
LABEL_WAIT_REQUEST = "お時間をいただく旨のみ"
LABEL_CONFIRM_ONLY = "確認する旨のみ"

# 閉集合（ラベル, 正規表現）。順序は検出順＝ラベルの出力順。
EMPTY_REPLY_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    (LABEL_FOLLOW_UP_PROMISE,
     re.compile(r"(?:確認|検討|精査|調査)(?:の上|のうえ|し(?:て|た上で|たうえで|まして)|いたしまして|させていただ(?:き|いて)|致しまして)"
                r"[^。\n]{0,20}?(?:改めて|あらためて|追って|後ほど|のちほど|別途)?[^。\n]{0,12}?"
                r"(?:ご連絡|ご案内|ご返信|ご回答|ご報告|お返事|連絡)(?:を)?(?:いたします|します|させていただきます|申し上げます|差し上げます|致します|させて頂きます)")),
    (LABEL_RELAY, re.compile(r"申し伝え|お伝えしておきます|伝えておきます|共有しておきます|申し送り")),
    (LABEL_STAFF_FOLLOW_UP,
     re.compile(r"(?:担当者|担当の者|担当弁護士|担当|責任者|弁護士)(?:より|から|が|に確認|へ確認|に代わって)[^。\n]{0,20}?"
                r"(?:ご連絡|ご案内|ご返信|ご回答|お返事|連絡|確認)")),
    (LABEL_CALLBACK, re.compile(r"折り返し|追ってご(?:連絡|案内|返信)|改めてご(?:案内|連絡|返信)|あらためてご(?:案内|連絡|返信)")),
    (LABEL_WAIT_REQUEST, re.compile(r"(?:お時間|少々お時間|しばらく)(?:を)?(?:いただ|頂)[^。\n]{0,10}?(?:場合|こと)が(?:ございます|あります)")),
)


# 「確認いたします」だけで終わる返事（実質の回答・質問が無い）: 文単位で判定する
_SENTENCE_SPLIT_RE = re.compile(r"[。\n！!？?]+")
_GREETING_RE = re.compile(r"^(?:ご連絡|ご質問|お問い?合わせ|お問合せ|メッセージ)?(?:を)?(?:ありがとうございます|ありがとうございました|承知(?:いた)?しました|かしこまりました|"
                          r"お世話になっております|恐れ入ります|申し訳(?:ございません|ありません)|よろしくお願い(?:いた|致)?します|失礼(?:いた|致)?します)$")
_CONFIRM_ONLY_RE = re.compile(r"^[^、]{0,30}?(?:確認|検討|精査)(?:いたします|します|させていただきます|致します|させて頂きます|中です|中でございます|しております)$")


def _confirm_only(text: str) -> bool:
    sentences = [x.strip().strip("、 　") for x in _SENTENCE_SPLIT_RE.split(text)]
    rest = [x for x in sentences if x and not _GREETING_RE.match(x)]
    return bool(rest) and all(_CONFIRM_ONLY_RE.match(x) for x in rest)


def _pending_templates() -> tuple[str, ...]:
    """既存の確認中定型（逐語）。遅延 import（chat_responder → hub の循環を作らない）。"""
    try:
        import chat_responder as cr
    except Exception:
        return ()
    return (cr.PENDING_REPLY,) + tuple(cr.PENDING_BY_CATEGORY.values())


def empty_reply_labels(text: str) -> list[str]:
    """空返事ならラベルの列（固定語彙）、そうでなければ []。本文は返さない。"""
    s = str(text or "").strip()
    if not s:
        return [LABEL_EMPTY]
    labels: list[str] = []
    for tpl in _pending_templates():
        if tpl and tpl.strip() and tpl.strip() in s:
            labels.append(LABEL_PENDING_TEMPLATE)
            break
    for label, rx in EMPTY_REPLY_PATTERNS:
        if rx.search(s):
            labels.append(label)
    if _confirm_only(s):
        labels.append(LABEL_CONFIRM_ONLY)
    return labels


def is_empty_reply(text: str) -> bool:
    return bool(empty_reply_labels(text))
