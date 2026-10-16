"""pii_scan — 個人特定情報の伏せ字と残留検査（共通検査器・JIKOU-REPLY-Q1b-fix1 BR-04）

正本: 時効LINEボット_返信規則_v1.4.md §10-7（手本は匿名化済み文だけ・検査に落ちた文は手本にしない）。
Codex R-JIKOU-REPLY-Q1b-1 BR-04: hub/reply_exemplar（手本の匿名化）と tools/line_log_anonymize
（LINE-LOG-1 の [人]ローカル変換）が **同じ検査器** を使う。本 module は正規表現と純関数だけ
（env・DB・HTTP・ログに触れない）。

対象（伏せ字＝〔…〕の固定語彙）:
- 氏名: 呼び出し側が渡す登録氏名（App 21/29）の表記ゆれ（全体・空白除去・姓/名の各部）。
  1 文字の部分（「林」「一」）は単独では伏せない（誤伏せ防止）が、呼びかけの規則で拾う。
- 呼びかけ: 「○○様」「○○さん」「○○氏」の直前 1〜4 文字（人名辞書は使わない）。一般的な呼称
  （お客様・皆さん・ご担当者様・お母さん 等）は停止語で除外。「氏名」の語は対象外。
- 住所: 都道府県＋市区郡町村（番地なしでも）／都道府県なしの市区郡町村以下（丁目・番地・番・号・
  数字の連なりを伴うもの）。「千葉県信用金庫」のように市区郡町村が続かない業者名は触らない。
- 電話・メール・郵便番号・口座番号（ラベルは残す）・LINE userId（U＋英数 32 桁）・7 桁以上の数字列。
- 金額（「円」「万円」付き）は保持する（先に退避して最後に戻す）。業者名は触らない。

残留検査（residue_hits）: メール・電話・郵便番号・LINE userId・7 桁以上の数字列。
伏せ字後にこれらが残れば呼び出し側は保存しない（fail-closed）。
"""

import re

MASK_NAME = "〔氏名〕"
MASK_ADDRESS = "〔住所〕"
MASK_PHONE = "〔電話〕"
MASK_EMAIL = "〔メール〕"
MASK_ACCOUNT = "〔口座〕"
MASK_POSTAL = "〔郵便番号〕"
MASK_DIGITS = "〔数字列〕"
MASK_LINE_USER_ID = "〔LINE ID〕"
MASKS = (MASK_NAME, MASK_ADDRESS, MASK_PHONE, MASK_EMAIL, MASK_ACCOUNT, MASK_POSTAL,
         MASK_DIGITS, MASK_LINE_USER_ID)

_D = "0-9０-９"
# 金額は保持する（「12,345円」「3万円」「44,000 円」）。先に退避して最後に戻す
AMOUNT_RE = re.compile(rf"[{_D}][{_D},，]*\s?(?:万)?円")
_AMOUNT_SLOT = "\u0000AMT{}\u0000"
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
# 電話: 0 始まり・区切り（-/全角ハイフン/空白）あり・なし
PHONE_RE = re.compile(rf"(?<![{_D}])0\d{{1,4}}[-‐－\s]?\d{{1,4}}[-‐－\s]?\d{{3,4}}(?![{_D}])")
PHONE_STRICT_RE = re.compile(r"0\d{1,4}-\d{1,4}-\d{3,4}")            # 残留検査（区切りあり）
POSTAL_RE = re.compile(rf"〒?\s?(?<![{_D}-‐－])\d{{3}}[-‐－]\d{{4}}(?![{_D}-‐－])")
# 口座: 「口座番号 1234567」「普通 1234567」「口座:1234567」等（ラベルは残し番号だけ伏せる）
ACCOUNT_RE = re.compile(rf"(口座番号|口座|普通|当座)(\s*[:：]?\s*)[{_D}]{{4,8}}(?![{_D}])")
# LINE userId: U + 英数 32 桁（実機は 16 進だが、英字主体の 32 桁も検出＝Codex 例）
LINE_USER_ID_RE = re.compile(r"(?<![A-Za-z0-9])U[A-Za-z0-9]{32}(?![A-Za-z0-9])")
# 住所（都道府県あり）: 都道府県 + 市区郡町村 を含む連続（空白・句読点で終端）。
# 業者名（「〇〇県信用金庫」等）は市区郡町村が続かない限り一致しない
_SEP = r"\s、。「」（）〔〕"
ADDRESS_PREF_RE = re.compile(
    rf"(?:北海道|東京都|京都府|大阪府|[^{_SEP}]{{2,3}}県)[^{_SEP}]{{1,12}}?(?:市|区|郡|町|村)[^{_SEP}]{{0,40}}")
# 住所（都道府県なし）: 市区郡町村 の後ろに 丁目/番地/番/号、または数字の連なりが続くもの
ADDRESS_NOPREF_RE = re.compile(
    rf"[^{_SEP}{_D}]{{1,12}}?(?:市|区|郡|町|村)[^{_SEP}]{{0,30}}?"
    rf"(?:[{_D}]+(?:丁目|番地|番|号|[-‐－ー][{_D}])|丁目|番地)[^{_SEP}]{{0,30}}")
DIGITS_RE = re.compile(rf"(?<![{_D}])[{_D}]{{7,}}(?![{_D}])")
# 呼びかけ: 「様／さん／氏」の直前 1〜4 文字（区切り・伏せ字の括弧は含めない）。「氏名」は対象外
HONORIFIC_RE = re.compile(rf"([^{_SEP}{_D}A-Za-z]{{1,4}})(様|さん|氏)(?!名)")
# 一般的な呼称（人名ではない）: 直前の窓がこれと一致するときは伏せない
HONORIFIC_STOPWORDS = frozenset({
    "お客", "皆", "みな", "皆々", "各位", "担当", "担当者", "ご担当", "ご担当者", "弁護士", "先生",
    "業者", "相手", "相手方", "債権者", "奥", "旦那", "お父", "お母", "お姉", "お兄", "おじい",
    "おばあ", "お子", "患者", "裁判所", "事務所", "貴", "皆さ", "お疲れ", "ご本人", "本人",
})
_NAME_SPLIT_RE = re.compile(r"[\s　]+")

# 残留検査の順序＝報告の固定語彙（メール・電話・郵便番号・LINE userId・数字列）
RESIDUE_RES = (("email", EMAIL_RE), ("phone", PHONE_STRICT_RE), ("postal", POSTAL_RE),
               ("line_user_id", LINE_USER_ID_RE), ("digits", DIGITS_RE))


def name_variants(name: str) -> list[str]:
    """登録氏名の表記ゆれ（全体・空白除去・姓/名の各部。1 文字の部分は対象外＝誤伏せ防止。
    「（未登録）」のような括弧書きは氏名ではない）。長い順。"""
    name = (name or "").strip()
    if not name or name.startswith("（") or name.startswith("("):
        return []
    out = {name, name.replace(" ", "").replace("　", "")}
    out.update(p for p in _NAME_SPLIT_RE.split(name) if len(p) >= 2)
    return sorted((v for v in out if len(v) >= 2), key=len, reverse=True)


def _mask_honorific(m: "re.Match") -> str:
    window, hon = m.group(1), m.group(2)
    if window in HONORIFIC_STOPWORDS or window.endswith("〕"):
        return m.group(0)
    # 窓の中に停止語が末尾で一致するなら（「のお客様」）伏せない
    for sw in HONORIFIC_STOPWORDS:
        if window.endswith(sw):
            return m.group(0)
    return MASK_NAME + hon


def _redact_core(text: str, names) -> tuple[str, list[str]]:
    """金額を退避したまま伏せ字にする（残留検査は退避中の文に対して行う＝金額の数字列を誤検出しない）。"""
    s = str(text or "")
    amounts: list[str] = []

    def _stash(m):
        amounts.append(m.group(0))
        return _AMOUNT_SLOT.format(len(amounts) - 1)

    s = AMOUNT_RE.sub(_stash, s)
    s = LINE_USER_ID_RE.sub(MASK_LINE_USER_ID, s)
    for n in names or ():
        for v in name_variants(n):
            s = s.replace(v, MASK_NAME)
    s = EMAIL_RE.sub(MASK_EMAIL, s)
    s = ACCOUNT_RE.sub(lambda m: m.group(1) + m.group(2) + MASK_ACCOUNT, s)
    s = PHONE_RE.sub(MASK_PHONE, s)          # 電話を先に（郵便番号の 3-4 桁が電話の一部に一致しないよう）
    s = POSTAL_RE.sub(MASK_POSTAL, s)
    s = ADDRESS_PREF_RE.sub(MASK_ADDRESS, s)
    s = ADDRESS_NOPREF_RE.sub(MASK_ADDRESS, s)
    s = HONORIFIC_RE.sub(_mask_honorific, s)
    s = DIGITS_RE.sub(MASK_DIGITS, s)
    return s, amounts


def _restore(s: str, amounts: list[str]) -> str:
    for i, a in enumerate(amounts):
        s = s.replace(_AMOUNT_SLOT.format(i), a)
    return s


def redact(text: str, *, names: tuple | list = ()) -> str:
    """伏せ字にして返す（金額は保持・業者名は触らない）。names は登録氏名（表記ゆれは内部で展開）。"""
    s, amounts = _redact_core(text, names)
    return _restore(s, amounts)


def residue_hits(text: str) -> list[str]:
    """伏せ字後の残留検査。残っている種別名（固定語彙）のリスト。空なら合格。"""
    s = str(text or "")
    return [name for name, rx in RESIDUE_RES if rx.search(s)]


def anonymize(text: str, *, names: tuple | list = ()) -> tuple[str, bool]:
    """(伏せ字後の文, 残留検査に合格したか)。残留検査は金額を退避したままの文に対して行う。"""
    s, amounts = _redact_core(text, names)
    ok = not residue_hits(s)
    return _restore(s, amounts), ok
