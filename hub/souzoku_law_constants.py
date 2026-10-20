"""souzoku_law_constants — 法定相続人の順位と法定相続分の判定表（SOUZOKU-3・QUEUE-2026-10-17-H H-4）

**全項目 要大野確認**（PC-A が民法 887〜890・900・901・939 条を基に仮置き。判定エンジン hub/souzoku_heirs.py は本表だけを参照し、
本表に無い組合せは「要弁護士判断」を返す）。画面・docx には「弁護士確認前」の印を出す（本表が確認されるまで）。
正本: docs/souzoku-1-design.md §2。数値は本ファイル 1 か所に置く。
"""
from fractions import Fraction

# ── 続柄・状態の閉集合 ──────────────────────────────────────────────────────────
LINE_SPOUSE = "spouse"            # 配偶者（民法 890 条・常に相続人）
LINE_CHILDREN = "children"        # 第 1 順位: 子（実子・養子・代襲者＝直系卑属・民法 887 条）
LINE_ASCENDANTS = "ascendants"    # 第 2 順位: 直系尊属（親等の近い者が先・民法 889 条 1 項 1 号）
LINE_SIBLINGS = "siblings"        # 第 3 順位: 兄弟姉妹（代襲は一代限り・民法 889 条 2 項）
LINES = (LINE_SPOUSE, LINE_CHILDREN, LINE_ASCENDANTS, LINE_SIBLINGS)
RANK_ORDER = (LINE_CHILDREN, LINE_ASCENDANTS, LINE_SIBLINGS)     # 血族相続人の順位（先順位がいれば後順位は相続人でない）

STATUS_ALIVE = "alive"                 # 生存（相続開始時）
STATUS_NONE = "none"                   # いない
STATUS_DECEASED = "deceased"           # 相続開始前に死亡（子・兄弟姉妹は代襲の原因・民法 887 条 2 項）
STATUS_RENOUNCED = "renounced"         # 相続放棄（初めから相続人でなかったものとみなす・民法 939 条。代襲は生じない）
STATUS_DISQUALIFIED = "disqualified"   # 欠格（民法 891 条・代襲の原因）
STATUS_DISINHERITED = "disinherited"   # 廃除（民法 892 条・代襲の原因）
STATUSES = (STATUS_ALIVE, STATUS_NONE, STATUS_DECEASED, STATUS_RENOUNCED, STATUS_DISQUALIFIED, STATUS_DISINHERITED)
REPRESENTATION_CAUSES = (STATUS_DECEASED, STATUS_DISQUALIFIED, STATUS_DISINHERITED)   # 代襲原因（放棄は含まない）

KIND_REAL = "real"                     # 実子（嫡出・非嫡出の区別は相続分に影響しない＝平成 25 年改正後・要大野確認）
KIND_ADOPTED = "adopted"               # 養子（実子と同じ・民法 809 条）
CHILD_KINDS = (KIND_REAL, KIND_ADOPTED)
BLOOD_FULL = "full"                    # 全血の兄弟姉妹
BLOOD_HALF = "half"                    # 半血の兄弟姉妹（父母の一方のみ同じ）
BLOODS = (BLOOD_FULL, BLOOD_HALF)

WILL_NONE = "none"
WILL_NOTARIAL = "notarial"             # 公正証書遺言
WILL_HOLOGRAPH = "holograph"           # 自筆証書遺言
WILL_UNKNOWN = "unknown"
WILLS = (WILL_NONE, WILL_NOTARIAL, WILL_HOLOGRAPH, WILL_UNKNOWN)

# ── 法定相続分（民法 900 条）: (配偶者の分, 血族相続人の群の分) ─────────────────── 要大野確認
SHARES_WITH_SPOUSE = {
    LINE_CHILDREN: (Fraction(1, 2), Fraction(1, 2)),      # 900 条 1 号
    LINE_ASCENDANTS: (Fraction(2, 3), Fraction(1, 3)),    # 900 条 2 号
    LINE_SIBLINGS: (Fraction(3, 4), Fraction(1, 4)),      # 900 条 3 号
}
SHARE_SPOUSE_ONLY = Fraction(1)        # 血族相続人がいなければ配偶者が全部（890 条）
SHARE_LINE_ONLY = Fraction(1)          # 配偶者がいなければその順位の群が全部
HALF_BLOOD_RATIO = Fraction(1, 2)      # 半血の兄弟姉妹は全血の 2 分の 1（900 条 4 号ただし書）
# 同順位の相続人の間では均等（900 条 4 号本文）。代襲者は被代襲者の分を均等に分ける（901 条）。
SIBLING_REPRESENTATION_DEPTH = 1       # 兄弟姉妹の代襲は一代（甥姪）まで（889 条 2 項は 887 条 2 項のみ準用）
CHILDREN_REPRESENTATION_DEPTH = None   # 子の代襲は再代襲あり（887 条 3 項）＝深さ制限なし

# ── 根拠条文（画面表示用・閉集合） ─────────────────────────────────────────────── 要大野確認
ARTICLES = {
    "spouse": "民法890条",
    "children": "民法887条1項",
    "representation_children": "民法887条2項・3項",
    "ascendants": "民法889条1項1号",
    "siblings": "民法889条1項2号",
    "representation_siblings": "民法889条2項（887条2項準用）",
    "shares": "民法900条",
    "half_blood": "民法900条4号ただし書",
    "per_stirpes": "民法901条",
    "renounced": "民法939条",
    "disqualified": "民法891条",
    "disinherited": "民法892条",
    "adopted": "民法809条",
}

# ── 本表で扱わない（要弁護士判断を返す）事由 ─────────────────────────────────── 要大野確認
UNSUPPORTED = {
    "successive": "数次相続（相続人がさらに死亡して相続が重なる）",
    "fetus": "胎児（民法886条）",
    "designated_shares": "遺言による相続分の指定・遺贈（民法902条）",
    "sibling_representation_depth": "兄弟姉妹の代襲者（甥姪）がさらに死亡している（再代襲なし＝相続人にならないが確認）",
    "no_heirs": "法定相続人がいない（相続財産清算人・特別縁故者の検討）",
    "renunciation_pending": "相続放棄の申述が未確定",
}
WILL_NOTE = "遺言あり: 指定相続分・遺贈・遺留分の確認が要る（法定相続分は参考値）"
REVIEW_MARK = "弁護士確認前"            # 画面・docx に出す印（本表が大野確認済みになるまで外さない）

# 画面の入力上限（SOUZOKU-3）
MAX_CHILD_ROWS = 8
MAX_SIBLING_ROWS = 8
MAX_DESCENDANTS_PER_ROW = 20
