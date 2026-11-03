"""wording_rules — WORDING-1（QUEUE-2026-10-14-G G-4）: 相談者向け文言の安全点検の語リスト（定数・報告のみ）

**要大野確認**: 本モジュールの語リスト・正規表現はすべて PC-A の仮置き（2026-10-14）。検出結果は報告
（pgv/wording_1_report.md）に載せるだけで、文言の修正・自動送信の可否・凍結文には一切影響しない（法的判断は大野専権）。
scripts/check_reply_wording.py と test_wording_check.py だけが参照する（本番経路からは import しない）。

観点（票の (a)〜(e)）:
 (a) 断定表現（「必ず」「確実に」「100%」「成立します」等）。否定の形（「絶対に大丈夫とは言えません」）は除外
 (b) 金額の表記ゆれ（正: 「44,000円」＝半角数字・3 桁区切り・円。44000円／4万4千円／４４，０００円／44,000 円 は揺れ）
 (c) 税込表記の欠落（金額を含む文（。または改行区切り）に「税込」「税別」「税抜」のいずれも無い）
 (d) 期日の曖昧表現（「すぐ」「近日」「早急」等）
 (e) 「電話」への誘導（通知先は LINE のみの方針に反する。「業者からの電話」のような言及は対象外）
"""

import re

RULE_ASSERTIVE = "a_assertive"
RULE_AMOUNT_FORMAT = "b_amount_format"
RULE_TAX_MISSING = "c_tax_missing"
RULE_VAGUE_DEADLINE = "d_vague_deadline"
RULE_PHONE = "e_phone_invite"
RULES = (RULE_ASSERTIVE, RULE_AMOUNT_FORMAT, RULE_TAX_MISSING, RULE_VAGUE_DEADLINE, RULE_PHONE)
RULE_LABELS = {
    RULE_ASSERTIVE: "(a) 断定表現",
    RULE_AMOUNT_FORMAT: "(b) 金額の表記ゆれ",
    RULE_TAX_MISSING: "(c) 税込表記の欠落",
    RULE_VAGUE_DEADLINE: "(d) 期日の曖昧表現",
    RULE_PHONE: "(e) 電話への誘導",
}

# (a) 要大野確認: 断定・保証の語（「確実」は「確実に」「確実です」「確実な時期」を含めて広く拾う）
ASSERTIVE_WORDS = ("必ず", "確実", "100%", "100％", "絶対", "間違いなく", "成立します", "保証", "問題ありません")
# 否定の形（顧客対応 prompt の例外 (2) と同じ考え方）: 語の直後 NEGATION_WINDOW 文字以内に否定があれば断定でない
ASSERTIVE_NEGATION_RE = re.compile(
    r"(とは|とまでは|と)(言えません|申し上げられません|限りません)|ではありません|ではございません|できません|いたしかねます|"
    r"しかねます|ございません|ありません")
NEGATION_WINDOW = 16

# (b) 正規の金額表記: 半角数字・3 桁ごとのカンマ・直後に「円」（例: 44,000円・110,000円・500円）
CANONICAL_AMOUNT_RE = re.compile(r"(?:[1-9]\d{0,2}(?:,\d{3})+|[1-9]\d{0,2}|0)円")
# 金額らしい表現を広く拾う（数字・カンマ・小数点・万／千・空白を挟んで「円」）
BROAD_AMOUNT_RE = re.compile(r"[0-9０-９][0-9０-９,，.．]*(?:\s*(?:万|億)\s*[0-9０-９,，]*)?(?:\s*千\s*[0-9０-９]*)?\s*円")

# (c) 税込の表記（いずれかが同じ文にあれば欠落でない）
TAX_WORDS = ("税込", "税別", "税抜", "消費税")
SENTENCE_SPLIT_RE = re.compile(r"[。\n]")

# (d) 要大野確認: 期日の曖昧表現
VAGUE_DEADLINE_WORDS = ("すぐ", "近日", "早急", "なるべく早く", "至急", "追って", "そのうち", "後日", "しばらく", "近いうち")

# (e) 要大野確認: 電話への誘導（「お電話ください」「電話でご連絡」「電話番号をお知らせ」等）。
#     「業者からの電話」「ご自宅への電話や郵送は行いません」のような言及・否定は誘導でない
PHONE_INVITE_RE = re.compile(
    r"(?:お電話|電話|TEL|ＴＥＬ|☎)(?:番号)?(?:を|で|にて|から)?"
    r"(?:ください|下さい|お願い|いただけ|頂け|お待ち|ご連絡|連絡|お問い合わせ|お問合せ|お知らせ|教えて|ご教示|おかけ|かけて)")
PHONE_NEGATION_RE = re.compile(r"行いません|いたしません|しません|ございません|ありません|不要")

SNIPPET_WIDTH = 18
# 報告の抜粋に電話番号様の数字列を載せない（RV-10 の趣旨・事務所情報も伏せる）
_PHONE_LIKE_RE = re.compile(r"0\d{1,4}[-ー－]\d{1,4}[-ー－]\d{3,4}")


def snippet(text: str, pos: int, width: int = SNIPPET_WIDTH) -> str:
    s = text[max(0, pos - width): pos + width].replace("\n", "⏎")
    return _PHONE_LIKE_RE.sub("0**-***-****", s)


def _hit(rule: str, match: str, text: str, pos: int) -> dict:
    return {"rule": rule, "match": match, "pos": pos, "snippet": snippet(text, pos)}


def find_assertive(text: str) -> list[dict]:
    hits = []
    for word in ASSERTIVE_WORDS:
        for m in re.finditer(re.escape(word), text):
            window = text[m.end(): m.end() + NEGATION_WINDOW]
            if ASSERTIVE_NEGATION_RE.search(window):
                continue
            hits.append(_hit(RULE_ASSERTIVE, word, text, m.start()))
    return sorted(hits, key=lambda h: h["pos"])


def find_amount_format(text: str) -> list[dict]:
    hits = []
    for m in BROAD_AMOUNT_RE.finditer(text):
        s = m.group(0)
        if not CANONICAL_AMOUNT_RE.fullmatch(s):
            hits.append(_hit(RULE_AMOUNT_FORMAT, s, text, m.start()))
    return hits


def find_tax_missing(text: str) -> list[dict]:
    hits = []
    offset = 0
    for sentence in SENTENCE_SPLIT_RE.split(text):
        amounts = list(BROAD_AMOUNT_RE.finditer(sentence))
        if amounts and not any(w in sentence for w in TAX_WORDS):
            for m in amounts:
                hits.append(_hit(RULE_TAX_MISSING, m.group(0), text, offset + m.start()))
        offset += len(sentence) + 1
    return hits


def find_vague_deadline(text: str) -> list[dict]:
    hits = []
    for word in VAGUE_DEADLINE_WORDS:
        for m in re.finditer(re.escape(word), text):
            hits.append(_hit(RULE_VAGUE_DEADLINE, word, text, m.start()))
    return sorted(hits, key=lambda h: h["pos"])


def find_phone_invite(text: str) -> list[dict]:
    hits = []
    for m in PHONE_INVITE_RE.finditer(text):
        tail = text[m.end(): m.end() + NEGATION_WINDOW]
        if PHONE_NEGATION_RE.search(tail):
            continue
        hits.append(_hit(RULE_PHONE, m.group(0), text, m.start()))
    return hits


FINDERS = {
    RULE_ASSERTIVE: find_assertive,
    RULE_AMOUNT_FORMAT: find_amount_format,
    RULE_TAX_MISSING: find_tax_missing,
    RULE_VAGUE_DEADLINE: find_vague_deadline,
    RULE_PHONE: find_phone_invite,
}


def check_text(text: str) -> list[dict]:
    """全観点の検出（rule・match・pos・snippet の列・rule 順→位置順）。例外は出さない（空文字は空）。"""
    out: list[dict] = []
    for rule in RULES:
        out.extend(FINDERS[rule](str(text or "")))
    return out
