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

残留検査（residue_hits）: メール・電話（区切りなしも）・郵便番号・LINE userId・6 桁以上の数字列・住所（都道府県／市区郡町村／
丁目・番・号の並び＝漢数字も）・続柄＋氏名（「家族の／妻の／夫の／息子の／娘の／母の／父の」＋漢字 2〜4 字）・氏名候補
（敬称なしの漢字 2〜4 字＋「に／へ／を／が／は」。一般語の停止語と語尾の漢字で除外）。
伏せ字後にこれらが残れば呼び出し側は保存しない（fail-closed）。fix2 BR-10: 誤検知は許容し安全側に倒す。
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
DIGITS_RE = re.compile(rf"(?<![{_D}])[{_D}]{{6,}}(?![{_D}])")       # fix2 BR-10: 6 桁以上（受付番号も含めて安全側）
# 呼びかけ: 「様／さん／氏」の直前 1〜4 文字（区切り・伏せ字の括弧は含めない）。「氏名」は対象外
HONORIFIC_RE = re.compile(rf"([^{_SEP}{_D}A-Za-z]{{1,4}})(様|さん|氏)(?!名)")
# 一般的な呼称（人名ではない）: 直前の窓がこれと一致するときは伏せない
HONORIFIC_STOPWORDS = frozenset({
    "お客", "皆", "みな", "皆々", "各位", "担当", "担当者", "ご担当", "ご担当者", "弁護士", "先生",
    "業者", "相手", "相手方", "債権者", "奥", "旦那", "お父", "お母", "お姉", "お兄", "おじい",
    "おばあ", "お子", "患者", "裁判所", "事務所", "貴", "皆さ", "お疲れ", "ご本人", "本人",
})
_NAME_SPLIT_RE = re.compile(r"[\s　]+")

# ── fix2 BR-10（Codex R-Q1b-fix1）: 残留検査の追加パターン（伏せ字はしない＝検出したら保存しない・安全側） ──
_KNUM = f"{_D}〇一二三四五六七八九十百千"
# 丁目・番地・番・号の並び（漢数字・全角を含む）: 「西新宿二丁目八番一号」「八番一号」「3丁目」
ADDRESS_BLOCK_RE = re.compile(rf"[{_KNUM}]+(?:丁目|番地|番|号)(?:[{_KNUM}]+(?:番地|番|号))*")
_KANJI = "一-龥々"
# 続柄＋氏名: 「家族の山田花子」「妻の花子」（漢字 2〜4 字が続く）
RELATION_NAME_RE = re.compile(
    rf"(?:家族|妻|夫|息子|娘|母|父|両親|親|兄|弟|姉|妹|祖父|祖母|子供|子)の([{_KANJI}]{{2,4}})(?![{_KANJI}])")
# 氏名候補: 敬称なしの漢字 2〜4 字＋格助詞（に／へ／を／が／は）。一般語は NAME_STOPWORDS と語尾の漢字（NAME_SAFE_SUFFIX）で除外
NAME_CANDIDATE_RE = re.compile(rf"(?<![{_KANJI}〔])([{_KANJI}]{{2,4}})(?=[にへをがは])")
NAME_SAFE_SUFFIX = frozenset(
    "者所書金日月年社円号先額数料費証票状局庁院行県市区町村部課係長省会店屋業務品物件情報度性的間点線式化系類値率量期限番"
    "件税法律条項号状形種別他等共通信用約束款")
NAME_STOPWORDS = frozenset({
    # 連絡・手続き
    "連絡", "連絡先", "返信", "返事", "回答", "質問", "確認", "相談", "説明", "案内", "対応", "手続", "手続き", "処理", "送付", "郵送",
    "発送", "提出", "申込", "申請", "受付", "受任", "委任", "着手", "完了", "終了", "開始", "進行", "進捗", "結果", "状況", "内容", "経過",
    "連携", "共有", "通知", "通達", "催告", "督促", "請求", "請求書", "書面", "書類", "資料", "写真", "画像", "添付", "記録", "記入", "署名",
    "押印", "捺印", "原本", "写し", "控え", "入力", "選択", "検討", "判断", "決定", "予定", "準備", "用意", "対象", "範囲", "関係", "事情",
    # 時効・債務
    "時効", "援用", "債務", "債権", "債権者", "業者", "会社", "金融", "貸金", "借入", "借金", "返済", "完済", "滞納", "延滞", "残高", "元金",
    "利息", "遅延", "損害", "和解", "分割", "一括", "支払", "入金", "振込", "振替", "決済", "費用", "料金", "金額", "報酬", "着手金", "実費",
    "消費税", "税込", "割引", "契約", "契約書", "条件", "特約", "解約", "辞任", "取消", "撤回", "更新", "中断", "猶予", "承認", "否認",
    "裁判", "裁判所", "判決", "訴状", "訴訟", "調停", "支払督促", "差押", "差押え", "強制執行", "異議", "答弁", "答弁書", "期日", "期限",
    "通知書", "内容証明", "配達", "到達", "受領", "開示", "信用", "情報", "記載", "住所", "氏名", "名前", "電話", "番号", "本人", "本人確認",
    # 一般・副詞的
    "今回", "前回", "次回", "今後", "以後", "以降", "以前", "以内", "以外", "以上", "以下", "事前", "事後", "直前", "直後", "午前", "午後",
    "当方", "弊所", "当所", "当事務所", "事務所", "弁護士", "先生", "担当", "担当者", "相手", "相手方", "先方", "家族", "親族", "本件",
    "当該", "該当", "全体", "一部", "個別", "具体", "詳細", "概要", "全部", "大半", "多数", "少数", "通常", "一般", "特別", "基本", "原則",
    "例外", "必要", "不要", "可能", "不可", "有無", "有効", "無効", "成立", "不成立", "成功", "失敗", "問題", "課題", "不安", "心配", "安心",
    "注意", "警告", "危険", "安全", "重要", "緊急", "至急", "早急", "順次", "随時", "適宜", "都度", "現在", "現時点", "現状", "最新",
    "最終", "最初", "最後", "途中", "最中", "同時", "同様", "同じ", "別途", "別件", "追加", "変更", "修正", "訂正", "削除", "取下", "再度",
    "改めて", "改善", "検索", "参照", "参考", "引用", "添削", "翻訳", "日本", "東京", "大阪", "名古屋", "福岡", "横浜", "埼玉", "千葉",
    "神奈川", "営業", "営業日", "平日", "休日", "祝日", "年末", "年始", "本日", "明日", "昨日", "来週", "今週", "先週", "来月", "今月",
    "先月", "時間", "時刻", "日程", "日時", "期間", "曜日", "電子", "郵便", "郵便局", "銀行", "口座", "名義", "窓口", "来所", "来訪", "面談",
    "予約", "訪問", "出張", "在宅", "外出", "不在", "留守", "携帯", "固定", "自宅", "勤務", "勤務先", "職場", "会社員", "無職", "収入",
    "生活", "家計", "病気", "入院", "通院", "高齢", "未成年", "成年", "後見", "代理", "代理人", "委任状", "身分", "身分証", "免許", "保険",
    "年金", "税金", "市役所", "区役所", "役所", "役場", "法務局", "公証", "謄本", "抄本", "戸籍", "住民票", "印鑑", "実印", "認印", "印鑑証明",
    "画面", "操作", "入力欄", "送信", "受信", "既読", "未読", "返答", "応答", "自動", "手動", "機械", "人間", "担当弁護士",
    "過去", "事例", "気軽", "万一", "万一時効", "分割", "分割払", "前払", "後払", "振込先", "口座番号",
})


def _has_name_candidate(text: str) -> bool:
    for m in NAME_CANDIDATE_RE.finditer(text):
        word = m.group(1)
        if word in NAME_STOPWORDS or word[-1] in NAME_SAFE_SUFFIX:
            continue
        return True
    return False


class _Matcher:
    """residue_hits 用の検査器（re.Pattern と同じく .search(text) を持つ・関数で判定）。"""

    def __init__(self, name: str, fn):
        self.pattern = name
        self._fn = fn

    def search(self, text: str):
        return self._fn(text)


def _search_address(text: str):
    return ADDRESS_PREF_RE.search(text) or ADDRESS_NOPREF_RE.search(text) or ADDRESS_BLOCK_RE.search(text)


NAME_CANDIDATE_MATCHER = _Matcher("name_candidate", _has_name_candidate)
ADDRESS_MATCHER = _Matcher("address", _search_address)

# 残留検査の順序＝報告の固定語彙（メール・電話・郵便番号・LINE userId・数字列・住所・続柄+氏名・氏名候補）
RESIDUE_RES = (("email", EMAIL_RE), ("phone", PHONE_RE), ("postal", POSTAL_RE),
               ("line_user_id", LINE_USER_ID_RE), ("digits", DIGITS_RE),
               ("address", ADDRESS_MATCHER), ("relation_name", RELATION_NAME_RE),
               ("name_candidate", NAME_CANDIDATE_MATCHER))


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


# fix2 BR-10: 氏名の発見的検査（続柄＋氏名・氏名候補）は **自由文の匿名化**（手本保存）にだけ使う。閉集合の凍結定型
# （大野承認済み・hub/jikou_reply_flow の送信前ガード）には誤検知が出る（例: 「過去」「事例」「気軽」）ため heuristics=False で外す。
RESIDUE_HEURISTIC_LABELS = ("relation_name", "name_candidate")


def residue_hits(text: str, *, heuristics: bool = True) -> list[str]:
    """伏せ字後の残留検査。残っている種別名（固定語彙）のリスト。空なら合格。
    heuristics=False は発見的な氏名検査（RESIDUE_HEURISTIC_LABELS）を外した確定パターンだけ（凍結定型の検査用）。"""
    s = str(text or "")
    return [name for name, rx in RESIDUE_RES if (heuristics or name not in RESIDUE_HEURISTIC_LABELS) and rx.search(s)]


def anonymize(text: str, *, names: tuple | list = ()) -> tuple[str, bool]:
    """(伏せ字後の文, 残留検査に合格したか)。残留検査は金額を退避したままの文に対して行う。"""
    s, amounts = _redact_core(text, names)
    ok = not residue_hits(s)
    return _restore(s, amounts), ok
